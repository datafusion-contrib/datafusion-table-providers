use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::TableReference;
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection, GenericError,
};
use datafusion_table_providers_common::util::{handle_unsupported_type_error, to_datafusion_error};
use datafusion_table_providers_common::{UnsupportedTypeAction, SOURCE_TYPE_METADATA_KEY};
use futures::{StreamExt, TryStreamExt};
use quack_protocol::{sql_literal, PooledClient, QuackError, Row, SqlParameter, Value};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("Quack query failed: {source}"))]
    Query { source: GenericError },

    #[snafu(display("Unable to render '{value}' as a SQL literal: {source}"))]
    InvalidLiteral { value: String, source: GenericError },

    #[snafu(display("Unexpected response from the Quack server: {message}"))]
    UnexpectedResponse { message: String },

    #[snafu(display("Column '{column}' has DuckDB type {duckdb_type}, which has no Arrow mapping ({reason}). Set the unsupported type action to 'warn' or 'ignore' to skip it."))]
    UnsupportedColumnType {
        column: String,
        duckdb_type: String,
        reason: String,
    },

    #[snafu(display("Query returned {actual} columns but the plan expects {expected}."))]
    ColumnCountMismatch { actual: usize, expected: usize },

    #[snafu(display("Cannot convert column '{column}' from {from} to {to}: {source}"))]
    CastColumn {
        column: String,
        from: DataType,
        to: DataType,
        source: ArrowError,
    },

    #[snafu(display("Query result does not match the plan schema: {source}"))]
    InvalidBatch { source: ArrowError },

    #[snafu(display(
        "Quack tables are read-only; statements that modify data are not supported."
    ))]
    ExecuteNotSupported,
}

/// A session leased from a [`QuackConnectionPool`](crate::quack::pool::QuackConnectionPool). It
/// returns to the pool when the connection built on it, and every result stream it
/// produced, is dropped.
pub struct QuackSession(PooledClient);

impl QuackSession {
    pub(crate) fn new(lease: PooledClient) -> Self {
        Self(lease)
    }
}

/// A connection on one Quack session.
pub struct QuackConnection {
    session: PooledClient,
    unsupported_type_action: UnsupportedTypeAction,
}

impl QuackConnection {
    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    async fn rows(&self, sql: &str) -> Result<Vec<Row>, Error> {
        let (_, rows) = self.query(sql).await?.into_rows();
        rows.try_collect().await.map_err(query_error)
    }

    async fn strings(&self, sql: &str) -> Result<Vec<String>, Error> {
        self.session
            .values(sql)
            .await
            .map_err(query_error)?
            .into_iter()
            .map(|value| match value {
                Value::String(s) => Ok(s),
                other => UnexpectedResponseSnafu {
                    message: format!("expected a string, got {other:?}"),
                }
                .fail(),
            })
            .collect()
    }

    async fn query(&self, sql: &str) -> Result<quack_protocol::QuackResultStream, Error> {
        self.session.query(sql, None).await.map_err(query_error)
    }

    /// The Arrow schema of a query's result, as the data path will produce it.
    async fn result_schema(&self, sql: &str) -> Result<SchemaRef, QuackError> {
        let (schema, _) = self.session.query(sql, None).await?.into_record_batches()?;
        Ok(schema)
    }

    async fn catalog_columns(
        &self,
        table_reference: &TableReference,
    ) -> Result<Vec<CatalogColumn>, Error> {
        let sql = format!(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE lower(table_catalog) = lower({catalog}) AND lower(table_schema) = lower({schema}) \
             AND lower(table_name) = lower({table}) ORDER BY ordinal_position",
            catalog = literal_or(table_reference.catalog(), "current_database()")?,
            schema = literal_or(table_reference.schema(), "current_schema()")?,
            table = literal(table_reference.table())?,
        );
        self.rows(&sql)
            .await?
            .into_iter()
            .map(|row| {
                let field = |name: &str| match row.get(name) {
                    Some(Value::String(s)) => Ok(s.clone()),
                    other => UnexpectedResponseSnafu {
                        message: format!("information_schema.columns.{name} is {other:?}"),
                    }
                    .fail(),
                };
                Ok(CatalogColumn {
                    name: field("column_name")?,
                    duckdb_type: field("data_type")?,
                    nullable: match field("is_nullable")?.as_str() {
                        "YES" => true,
                        "NO" => false,
                        other => {
                            return UnexpectedResponseSnafu {
                                message: format!(
                                    "information_schema.columns.is_nullable is '{other}'"
                                ),
                            }
                            .fail()
                        }
                    },
                })
            })
            .collect()
    }

    /// Resolves the Arrow type of each column, applying the unsupported type action to
    /// columns the client cannot decode. Returns `None` for a skipped column.
    async fn column_types(
        &self,
        table: &str,
        columns: &[CatalogColumn],
    ) -> Result<Vec<Option<Field>>, Error> {
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        match self.result_schema(&probe_sql(table, &names)).await {
            Ok(schema) => {
                return Ok(schema
                    .fields()
                    .iter()
                    .map(|f| Some(f.as_ref().clone()))
                    .collect())
            }
            Err(QuackError::UnsupportedType(_)) => {}
            Err(e) => return Err(query_error(e)),
        }

        // At least one column has no Arrow mapping: find which ones.
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            match self.result_schema(&probe_sql(table, &[&column.name])).await {
                Ok(schema) => fields.push(schema.fields().first().map(|f| f.as_ref().clone())),
                Err(QuackError::UnsupportedType(reason)) => {
                    handle_unsupported_type_error(
                        self.unsupported_type_action,
                        Error::UnsupportedColumnType {
                            column: column.name.clone(),
                            duckdb_type: column.duckdb_type.clone(),
                            reason,
                        },
                    )?;
                    fields.push(None);
                }
                Err(e) => return Err(query_error(e)),
            }
        }
        Ok(fields)
    }
}

struct CatalogColumn {
    name: String,
    duckdb_type: String,
    nullable: bool,
}

impl DbConnection<QuackSession, ()> for QuackConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<QuackSession, ()>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<QuackSession, ()> for QuackConnection {
    fn new(conn: QuackSession) -> Self {
        Self {
            session: conn.0,
            unsupported_type_action: UnsupportedTypeAction::default(),
        }
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        let sql = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = current_database() AND lower(table_schema) = lower({}) \
             ORDER BY table_name",
            literal(schema)
                .boxed()
                .context(dbconnection::UnableToGetTablesSnafu)?
        );
        self.strings(&sql)
            .await
            .boxed()
            .context(dbconnection::UnableToGetTablesSnafu)
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        self.strings(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE catalog_name = current_database() ORDER BY schema_name",
        )
        .await
        .boxed()
        .context(dbconnection::UnableToGetSchemasSnafu)
    }

    /// Resolves the table's schema from `information_schema.columns` and a `LIMIT 0` probe
    /// that goes through the same Arrow mapping as query results. Nullability comes from
    /// the catalog, and each field carries its DuckDB type under
    /// [`SOURCE_TYPE_METADATA_KEY`].
    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        let columns = self
            .catalog_columns(table_reference)
            .await
            .boxed()
            .context(dbconnection::UnableToGetSchemaSnafu)?;
        if columns.is_empty() {
            return Err(dbconnection::Error::UndefinedTable {
                table_name: table_reference.to_string(),
                source: "information_schema.columns lists no columns for it".into(),
            });
        }

        let table = quote_table_reference(table_reference);
        let types = self
            .column_types(&table, &columns)
            .await
            .boxed()
            .context(dbconnection::UnableToGetSchemaSnafu)?;

        let mut fields = Vec::with_capacity(columns.len());
        for (column, field) in columns.into_iter().zip(types) {
            let Some(field) = field else { continue };
            if field.name() != &column.name {
                return Err(dbconnection::Error::UnableToGetSchema {
                    source: Box::new(Error::UnexpectedResponse {
                        message: format!(
                            "query returned column '{}' where information_schema lists '{}'",
                            field.name(),
                            column.name
                        ),
                    }),
                });
            }
            fields.push(
                field
                    .with_nullable(column.nullable)
                    .with_metadata(HashMap::from([(
                        SOURCE_TYPE_METADATA_KEY.to_string(),
                        column.duckdb_type,
                    )])),
            );
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Runs a query and streams its result as `projected_schema` (or the result's own
    /// schema when none is given). Columns are matched by position and cast when their
    /// type differs. A value out of range for the target type fails the stream; other
    /// conversions (e.g. float to decimal, nanoseconds to microseconds) follow Arrow's cast.
    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[()],
        projected_schema: Option<SchemaRef>,
    ) -> dbconnection::Result<SendableRecordBatchStream> {
        let (result_schema, batches) = self
            .query(sql)
            .await?
            .into_record_batches()
            .map_err(query_error)?;
        let schema = projected_schema.unwrap_or(result_schema);
        let batch_schema = Arc::clone(&schema);
        let stream = batches.map(move |batch| {
            let batch = batch.map_err(|e| to_datafusion_error(query_error(e)))?;
            cast_batch(&batch, &batch_schema).map_err(to_datafusion_error)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn execute(&self, _sql: &str, _params: &[()]) -> dbconnection::Result<u64> {
        Err(Box::new(Error::ExecuteNotSupported))
    }
}

fn query_error(e: QuackError) -> Error {
    Error::Query {
        source: Box::new(e),
    }
}

fn literal(value: &str) -> Result<String, Error> {
    sql_literal(&SqlParameter::from(value)).map_err(|e| Error::InvalidLiteral {
        value: value.to_string(),
        source: Box::new(e),
    })
}

fn literal_or(value: Option<&str>, default: &str) -> Result<String, Error> {
    value.map_or_else(|| Ok(default.to_string()), literal)
}

fn quote_identifier(identifier: &str) -> String {
    Ident::with_quote('"', identifier).to_string()
}

fn quote_table_reference(table_reference: &TableReference) -> String {
    [
        table_reference.catalog(),
        table_reference.schema(),
        Some(table_reference.table()),
    ]
    .into_iter()
    .flatten()
    .map(quote_identifier)
    .collect::<Vec<_>>()
    .join(".")
}

fn probe_sql(table: &str, columns: &[&str]) -> String {
    let columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {columns} FROM {table} LIMIT 0")
}

/// Casts `batch` to `schema` column by column. Unlike Arrow's default cast, a value that
/// is out of range for the target type, or can't be parsed as it, is an error rather than
/// NULL. Other lossy conversions, such as float to decimal or nanoseconds to microseconds,
/// follow Arrow's cast.
///
/// Not datafusion-federation's `try_cast_to`: it turns out-of-range values into NULL, and
/// this crate's `quack` feature builds without federation.
pub(crate) fn cast_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, Error> {
    ensure!(
        batch.num_columns() == schema.fields().len(),
        ColumnCountMismatchSnafu {
            actual: batch.num_columns(),
            expected: schema.fields().len(),
        }
    );
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast_with_options(column, field.data_type(), &options).with_context(|_| {
                    CastColumnSnafu {
                        column: field.name().clone(),
                        from: column.data_type().clone(),
                        to: field.data_type().clone(),
                    }
                })
            }
        })
        .collect::<Result<Vec<ArrayRef>, Error>>()?;
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .context(InvalidBatchSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal256Array, Int32Array, Int64Array};
    use arrow::datatypes::i256;

    fn batch(field: Field, array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
    }

    #[test]
    fn cast_batch_widens_and_keeps_schema_metadata() {
        let input = batch(
            Field::new("1", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1, 2])),
        );
        let target = Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, false)
            .with_metadata(HashMap::from([("k".to_string(), "v".to_string())]))]));
        let out = cast_batch(&input, &target).unwrap();
        assert_eq!(out.schema(), target);
        assert_eq!(
            out.column(0).as_ref(),
            &Int64Array::from(vec![1, 2]) as &dyn arrow::array::Array
        );
    }

    #[test]
    fn cast_batch_errors_on_overflow_instead_of_null() {
        let big = i256::from_i128(i128::from(i64::MAX) + 1);
        let input = batch(
            Field::new("total", DataType::Decimal256(39, 0), true),
            Arc::new(
                Decimal256Array::from(vec![Some(big)])
                    .with_precision_and_scale(39, 0)
                    .unwrap(),
            ),
        );
        let target = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )]));
        let err = cast_batch(&input, &target).unwrap_err();
        let message = err.to_string();
        assert!(matches!(err, Error::CastColumn { .. }), "{message}");
        assert!(message.contains("'total'"), "{message}");
        assert!(message.contains("Decimal256(39, 0)"), "{message}");
    }

    #[test]
    fn cast_batch_rejects_nulls_in_non_nullable_field() {
        let input = batch(
            Field::new("id", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![Some(1), None])),
        );
        let target = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        assert!(matches!(
            cast_batch(&input, &target),
            Err(Error::InvalidBatch { .. })
        ));
    }

    #[test]
    fn cast_batch_rejects_column_count_mismatch() {
        let input = batch(
            Field::new("id", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1])),
        );
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
        ]));
        assert!(matches!(
            cast_batch(&input, &target),
            Err(Error::ColumnCountMismatch {
                actual: 1,
                expected: 2
            })
        ));
    }

    #[test]
    fn identifiers_are_quoted_and_escaped() {
        let table = TableReference::full("my db", "main", "we\"ird");
        assert_eq!(quote_table_reference(&table), r#""my db"."main"."we""ird""#);
        assert_eq!(
            probe_sql("\"t\"", &["a", "select"]),
            r#"SELECT "a", "select" FROM "t" LIMIT 0"#
        );
    }

    #[test]
    fn literals_escape_quotes() {
        assert_eq!(literal("o'brien").unwrap(), "'o''brien'");
        assert_eq!(
            literal_or(None, "current_schema()").unwrap(),
            "current_schema()"
        );
    }
}
