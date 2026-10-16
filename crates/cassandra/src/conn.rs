use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::value::{CqlValue, Row};
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection, GenericError, Result, TableSchemas,
};

use crate::types::{column_specs_to_schema, rows_to_record_batch};

/// Number of rows converted to Arrow at a time; Cassandra pages are 5000 rows by default.
const BATCH_SIZE: usize = 8192;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to query Cassandra: {source}"))]
    Query {
        source: scylla::errors::PagerExecutionError,
    },

    #[snafu(display("Failed to execute the Cassandra statement: {source}"))]
    Execute {
        source: scylla::errors::ExecutionError,
    },

    #[snafu(display("Failed to read rows from Cassandra: {source}"))]
    ReadRows {
        source: scylla::errors::NextRowError,
    },

    #[snafu(display("Unexpected Cassandra result: {source}"))]
    TypeCheck {
        source: scylla::deserialize::TypeCheckError,
    },

    #[snafu(display("Failed to convert the Cassandra result to Arrow: {source}"))]
    Conversion { source: crate::types::Error },
}

/// A connection to Cassandra.
///
/// The driver's [`Session`] manages its own connection pool, so a connection is a cheap handle
/// to a shared session.
pub struct CassandraConnection {
    pub session: Arc<Session>,
    schemas: Arc<TableSchemas>,
}

impl std::fmt::Debug for CassandraConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraConnection")
            .field("schemas", &self.schemas)
            .finish_non_exhaustive()
    }
}

impl CassandraConnection {
    /// Runs a CQL query that returns a single text column and collects its values.
    async fn query_strings(&self, cql: &str, params: Vec<CqlValue>) -> Result<Vec<String>, Error> {
        self.session
            .query_iter(cql, params)
            .await
            .context(QuerySnafu)?
            .rows_stream::<(String,)>()
            .context(TypeCheckSnafu)?
            .map_ok(|(value,)| value)
            .try_collect()
            .await
            .context(ReadRowsSnafu)
    }
}

impl DbConnection<Arc<Session>, CqlValue> for CassandraConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<Session>, CqlValue>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<Arc<Session>, CqlValue> for CassandraConnection {
    fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            schemas: Default::default(),
        }
    }

    fn new_with_schemas(session: Arc<Session>, schemas: Arc<TableSchemas>) -> Self
    where
        Self: Sized,
    {
        Self { session, schemas }
    }

    /// Returns the tables of the keyspace `schema`.
    async fn tables(&self, schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        self.query_strings(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
            vec![CqlValue::Text(schema.to_string())],
        )
        .await
        .boxed()
        .context(dbconnection::UnableToGetTablesSnafu)
    }

    /// Returns the keyspaces.
    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        self.query_strings("SELECT keyspace_name FROM system_schema.keyspaces", vec![])
            .await
            .boxed()
            .context(dbconnection::UnableToGetSchemasSnafu)
    }

    /// Returns the schema of a table, read from the result metadata of a prepared
    /// `SELECT *` statement so that no table data is read.
    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        if let Some(schema) = self.schemas.get(table_reference) {
            return Ok(Arc::clone(schema));
        }

        let cql = format!("SELECT * FROM {}", quote_table_reference(table_reference));
        let prepared = self.session.prepare(cql).await.map_err(|e| {
            let message = e.to_string();
            let source: GenericError = Box::new(e);
            if message.contains("unconfigured table") || message.contains("does not exist") {
                dbconnection::Error::UndefinedTable {
                    table_name: table_reference.to_string(),
                    source,
                }
            } else {
                dbconnection::Error::UnableToGetSchema { source }
            }
        })?;

        let col_specs = prepared.get_current_result_set_col_specs();
        column_specs_to_schema(col_specs.get().iter())
            .boxed()
            .context(dbconnection::UnableToGetSchemaSnafu)
    }

    /// Runs a CQL query and streams its result as Arrow record batches.
    ///
    /// The batches have `projected_schema` when it is given, otherwise the schema of the result
    /// set. The fields of `projected_schema` are matched to the result set columns by name.
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[CqlValue],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let pager = self
            .session
            .query_iter(sql, params.to_vec())
            .await
            .context(QuerySnafu)?;

        let column_names: Vec<String> = pager
            .column_specs()
            .iter()
            .map(|spec| spec.name().to_string())
            .collect();
        let schema = match projected_schema {
            Some(schema) => schema,
            None => column_specs_to_schema(pager.column_specs().iter()).context(ConversionSnafu)?,
        };

        let rows = pager.rows_stream::<Row>().context(TypeCheckSnafu)?;
        let stream_schema = Arc::clone(&schema);
        let batches = rows.chunks(BATCH_SIZE).map(move |chunk| {
            let rows = chunk
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .context(ReadRowsSnafu)
                .map_err(to_datafusion_error)?;
            rows_to_record_batch(&stream_schema, &column_names, &rows)
                .context(ConversionSnafu)
                .map_err(to_datafusion_error)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }

    /// Executes a CQL statement.
    ///
    /// Cassandra doesn't report the number of affected rows, so this always returns 0.
    async fn execute(&self, sql: &str, params: &[CqlValue]) -> Result<u64> {
        self.session
            .query_unpaged(sql, params.to_vec())
            .await
            .context(ExecuteSnafu)?;
        Ok(0)
    }
}

fn to_datafusion_error(e: Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

/// Quotes a CQL identifier, so that it is matched case-sensitively.
pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Quotes all parts of a table reference. The catalog part is ignored: Cassandra only has
/// keyspaces and tables.
pub(crate) fn quote_table_reference(table_reference: &TableReference) -> String {
    match table_reference.schema() {
        Some(keyspace) => format!(
            "{}.{}",
            quote_identifier(keyspace),
            quote_identifier(table_reference.table())
        ),
        None => quote_identifier(table_reference.table()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_identifier("users"), "\"users\"");
        assert_eq!(quote_identifier("MixedCase"), "\"MixedCase\"");
        assert_eq!(quote_identifier("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn quotes_table_references() {
        assert_eq!(
            quote_table_reference(&TableReference::bare("users")),
            "\"users\""
        );
        assert_eq!(
            quote_table_reference(&TableReference::partial("ks", "Users")),
            "\"ks\".\"Users\""
        );
        assert_eq!(
            quote_table_reference(&TableReference::full("catalog", "ks", "users")),
            "\"ks\".\"users\""
        );
    }
}
