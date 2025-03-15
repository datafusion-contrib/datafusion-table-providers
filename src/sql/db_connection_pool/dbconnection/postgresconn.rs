use std::any::Any;
use std::error::Error;
use std::sync::Arc;

use crate::sql::arrow_sql_gen::postgres::rows_to_arrow;
use crate::sql::arrow_sql_gen::postgres::schema::pg_data_type_to_arrow_type;
use crate::sql::arrow_sql_gen::postgres::schema::ParseContext;
use crate::util::handle_unsupported_type_error;
use crate::util::schema::SchemaValidator;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow_schema::DataType;
use async_stream::stream;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::PostgresConnectionManager;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::{dialect::ClickHouseDialect, tokenizer::Tokenizer};
use datafusion::sql::TableReference;
use futures::stream;
use futures::StreamExt;
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;

use crate::UnsupportedTypeAction;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

const SCHEMA_QUERY: &str = r"
WITH custom_type_details AS (
SELECT 
t.typname,
t.typtype,
CASE 
    WHEN t.typtype = 'e' THEN 
        jsonb_build_object(
            'type', 'enum',
            'values', (
                SELECT jsonb_agg(e.enumlabel ORDER BY e.enumsortorder)
                FROM pg_enum e 
                WHERE e.enumtypid = t.oid
            )
        )
    WHEN t.typtype = 'c' THEN
        jsonb_build_object(
            'type', 'composite',
            'attributes', (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'name', a.attname,
                        'type', pg_catalog.format_type(a.atttypid, a.atttypmod)
                    )
                    ORDER BY a.attnum
                )
                FROM pg_attribute a
                WHERE a.attrelid = t.typrelid 
                AND a.attnum > 0 
                AND NOT a.attisdropped
            )
        )
END as type_details
FROM pg_type t
WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
)
SELECT 
c.column_name,
CASE 
WHEN c.data_type = 'USER-DEFINED' THEN
    CASE 
        WHEN t.typtype = 'e' THEN 'enum'
        WHEN t.typtype = 'c' THEN 'composite'
        ELSE c.data_type
    END
WHEN c.data_type = 'ARRAY' THEN
    'array'
ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
END as data_type,
c.is_nullable,
CASE 
WHEN c.data_type = 'ARRAY' THEN
    jsonb_build_object(
        'type', 'array',
        'element_type', (
            SELECT pg_catalog.format_type(et.oid, a.atttypmod)
            FROM pg_type t
            JOIN pg_type et ON t.typelem = et.oid
            WHERE t.typname = c.udt_name
        )
    )
ELSE td.type_details
END as type_details
FROM 
information_schema.columns c
LEFT JOIN custom_type_details td ON td.typname = c.udt_name
LEFT JOIN pg_type t ON t.typname = c.udt_name
LEFT JOIN pg_attribute a ON 
a.attrelid = (
    SELECT oid 
    FROM pg_class 
    WHERE relname = c.table_name 
    AND relnamespace = (
        SELECT oid 
        FROM pg_namespace 
        WHERE nspname = c.table_schema
    )
)
AND a.attname = c.column_name
WHERE 
c.table_schema = $1
AND c.table_name = $2
ORDER BY 
c.ordinal_position;
";

const SCHEMAS_QUERY: &str = "
SELECT nspname AS schema_name
FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND nspname !~ '^pg_toast';
";

const TABLES_QUERY: &str = "
SELECT tablename
FROM pg_tables
WHERE schemaname = $1;
";

#[derive(Debug, Snafu)]
pub enum PostgresError {
    #[snafu(display(
        "Query execution failed.\n{source}\nFor details, refer to the PostgreSQL manual: https://www.postgresql.org/docs/17/index.html"
    ))]
    QueryError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("Failed to convert query result to Arrow.\n{source}\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    ConversionError {
        source: crate::sql::arrow_sql_gen::postgres::Error,
    },
}

pub struct PostgresConnection {
    pub conn: bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
    unsupported_type_action: UnsupportedTypeAction,
}

impl SchemaValidator for PostgresConnection {
    type Error = super::Error;

    fn is_data_type_supported(data_type: &DataType) -> bool {
        !matches!(data_type, DataType::Map(_, _))
    }

    fn unsupported_type_error(data_type: &DataType, field_name: &str) -> Self::Error {
        super::Error::UnsupportedDataType {
            data_type: data_type.to_string(),
            field_name: field_name.to_string(),
        }
    }
}

impl<'a>
    DbConnection<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'a (dyn ToSql + Sync),
    > for PostgresConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(
        &self,
    ) -> Option<
        &dyn AsyncDbConnection<
            bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
            &'a (dyn ToSql + Sync),
        >,
    > {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a>
    AsyncDbConnection<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'a (dyn ToSql + Sync),
    > for PostgresConnection
{
    fn new(
        conn: bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
    ) -> Self {
        PostgresConnection {
            conn,
            unsupported_type_action: UnsupportedTypeAction::default(),
        }
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, super::Error> {
        let rows = self
            .conn
            .query(TABLES_QUERY, &[&schema])
            .await
            .map_err(|e| super::Error::UnableToGetTables {
                source: Box::new(e),
            })?;

        Ok(rows.iter().map(|r| r.get::<usize, String>(0)).collect())
    }

    async fn schemas(&self) -> Result<Vec<String>, super::Error> {
        let rows = self.conn.query(SCHEMAS_QUERY, &[]).await.map_err(|e| {
            super::Error::UnableToGetSchemas {
                source: Box::new(e),
            }
        })?;

        Ok(rows.iter().map(|r| r.get::<usize, String>(0)).collect())
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let table_str = if is_table_function(table_reference) {
            let SCHEMA_QUERY_FN = r#"
            SELECT format_type(p.prorettype, null) as return_type
            FROM pg_proc p
            WHERE p.proname = $1
              AND p.pronamespace = current_schema::regnamespace
              AND p.prokind = 'f'
            "#;
            let rows = match self
                .conn
                .query(SCHEMA_QUERY_FN, &[&table_reference.to_string()])
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if let Some(error_source) = e.source() {
                        if let Some(pg_error) =
                            error_source.downcast_ref::<tokio_postgres::error::DbError>()
                        {
                            if pg_error.code() == &tokio_postgres::error::SqlState::UNDEFINED_TABLE
                            {
                                return Err(super::Error::UndefinedTable {
                                    source: Box::new(pg_error.clone()),
                                    table_name: table_reference.to_string(),
                                });
                            }
                        }
                    }
                    return Err(super::Error::UnableToGetSchema {
                        source: Box::new(e),
                    });
                }
            };
            rows.first()
                .map(|row| row.get::<usize, String>(0))
                .unwrap_or_default()
        } else {
            table_reference.to_quoted_string()
        };

        let schema_name = table_reference.schema().unwrap_or("public");

        let rows = match self
            .conn
            .query(SCHEMA_QUERY, &[&schema_name, &table_str])
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                if let Some(error_source) = e.source() {
                    if let Some(pg_error) =
                        error_source.downcast_ref::<tokio_postgres::error::DbError>()
                    {
                        if pg_error.code() == &tokio_postgres::error::SqlState::UNDEFINED_TABLE {
                            return Err(super::Error::UndefinedTable {
                                source: Box::new(pg_error.clone()),
                                table_name: table_reference.to_string(),
                            });
                        }
                    }
                }
                return Err(super::Error::UnableToGetSchema {
                    source: Box::new(e),
                });
            }
        };

        let mut fields = Vec::new();
        for row in rows {
            let column_name = row.get::<usize, String>(0);
            let pg_type = row.get::<usize, String>(1);
            let nullable_str = row.get::<usize, String>(2);
            let nullable = nullable_str == "YES";
            let type_details = row.get::<usize, Option<serde_json::Value>>(3);
            let mut context =
                ParseContext::new().with_unsupported_type_action(self.unsupported_type_action);

            if let Some(type_details) = type_details {
                context = context.with_type_details(type_details);
            };

            let Ok(arrow_type) = pg_data_type_to_arrow_type(&pg_type, &context) else {
                handle_unsupported_type_error(
                    self.unsupported_type_action,
                    super::Error::UnsupportedDataType {
                        data_type: pg_type.to_string(),
                        field_name: column_name.to_string(),
                    },
                )?;

                continue;
            };

            fields.push(Field::new(column_name, arrow_type, nullable));
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a (dyn ToSql + Sync)],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: We should have a way to detect if params have been passed
        // if they haven't we should use .copy_out instead, because it should be much faster
        let streamable = self
            .conn
            .query_raw(sql, params.iter().copied()) // use .query_raw to get access to the underlying RowStream
            .await
            .context(QuerySnafu)?;

        // chunk the stream into groups of rows
        let mut stream = streamable.chunks(4_000).boxed().map(move |rows| {
            let rows = rows
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(QuerySnafu)?;
            let rec = rows_to_arrow(rows.as_slice(), &projected_schema).context(ConversionSnafu)?;
            Ok::<_, PostgresError>(rec)
        });

        let Some(first_chunk) = stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let first_chunk = first_chunk?;
        let schema = first_chunk.schema(); // pull out the schema from the first chunk to use in the DataFusion Stream Adapter

        let output_stream = stream! {
           yield Ok(first_chunk);
           while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        yield Ok(batch); // we can yield the batch as-is because we've already converted to Arrow in the chunk map
                    }
                    Err(e) => {
                        yield Err(DataFusionError::Execution(format!("Failed to fetch batch: {e}")));
                    }
                }
           }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            output_stream,
        )))
    }

    async fn execute(&self, sql: &str, params: &[&'a (dyn ToSql + Sync)]) -> Result<u64> {
        Ok(self.conn.execute(sql, params).await?)
    }
}

impl PostgresConnection {
    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }
}

#[must_use]
pub fn is_table_function(table_reference: &TableReference) -> bool {
    let table_name = match table_reference {
        TableReference::Full { .. } | TableReference::Partial { .. } => return false,
        TableReference::Bare { table } => table,
    };

    let dialect = ClickHouseDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, table_name);
    let Ok(tokens) = tokenizer.tokenize() else {
        return false;
    };
    let Ok(tf) = Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_table_factor()
    else {
        return false;
    };

    let TableFactor::Table { args, .. } = tf else {
        return false;
    };

    args.is_some()
}
