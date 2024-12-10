use std::any::Any;
use std::error::Error;
use std::sync::Arc;

use crate::sql::arrow_sql_gen::postgres::rows_to_arrow;
use crate::sql::arrow_sql_gen::postgres::schema::pg_data_type_to_arrow_type;
use async_stream::stream;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::PostgresConnectionManager;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::stream;
use futures::StreamExt;
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

const SCHEMA_QUERY: &str = r#"
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
"#;

const SCHEMAS_QUERY: &str = r#"
SELECT nspname AS schema_name
FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND nspname !~ '^pg_toast';
"#;

const TABLES_QUERY: &str = r#"
SELECT tablename
FROM pg_tables
WHERE schemaname = $1;
"#;

#[derive(Debug, Snafu)]
pub enum PostgresError {
    #[snafu(display("{source}"))]
    QueryError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: crate::sql::arrow_sql_gen::postgres::Error,
    },

    #[snafu(display("{source}"))]
    InternalError {
        source: tokio_postgres::error::Error,
    },
}

pub struct PostgresConnection {
    pub conn: bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
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
        PostgresConnection { conn }
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
        let table_name = table_reference.table();
        let schema_name = table_reference.schema().unwrap_or("public");

        let rows = match self
            .conn
            .query(SCHEMA_QUERY, &[&schema_name, &table_name])
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
            let arrow_type = pg_data_type_to_arrow_type(&pg_type, type_details)
                .boxed()
                .context(super::UnableToGetSchemaSnafu)?;
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
