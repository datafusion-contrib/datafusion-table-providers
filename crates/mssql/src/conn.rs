use std::any::Any;
use std::sync::Arc;

use async_stream::stream;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use snafu::prelude::*;
use tiberius::{ColumnData, Row, ToSql};
use tokio::sync::Mutex;

use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    self, AsyncDbConnection, DbConnection, TableSchemas,
};

use crate::arrow_sql_gen::{self, infer_schema, map_sql_server_type_to_arrow, rows_to_arrow};
use crate::dialect::rewrite_sql;

/// The type of the pooled SQL Server connections.
pub type MsSqlPooledConnection = bb8::PooledConnection<'static, bb8_tiberius::ConnectionManager>;

/// The type of the SQL parameters of [`MsSqlConnection`].
pub type MsSqlParameter = ColumnData<'static>;

/// The number of rows converted into one record batch.
const BATCH_SIZE: usize = 4_000;

/// SQL Server error 208: `Invalid object name`.
const INVALID_OBJECT_NAME: u32 = 208;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("SQL Server query failed: {source}"))]
    QueryError { source: tiberius::error::Error },

    #[snafu(display("Failed to convert the SQL Server query result to Arrow: {source}"))]
    ConversionError { source: arrow_sql_gen::Error },

    #[snafu(display("The table '{table_name}' has no columns or doesn't exist"))]
    TableNotFound { table_name: String },
}

/// A connection to Microsoft SQL Server.
pub struct MsSqlConnection {
    pub conn: Arc<Mutex<MsSqlPooledConnection>>,
    schemas: Arc<TableSchemas>,
}

impl MsSqlConnection {
    #[must_use]
    pub fn new_with_schemas(conn: MsSqlPooledConnection, schemas: Arc<TableSchemas>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            schemas,
        }
    }

    /// Runs a statement and returns the number of rows it affected.
    ///
    /// Unlike [`AsyncDbConnection::execute`], the SQL is sent as-is, without rewriting it to
    /// T-SQL, so it can contain T-SQL that the SQL parser doesn't support.
    pub async fn execute_raw(
        &self,
        sql: &str,
        params: &[MsSqlParameter],
    ) -> Result<u64, tiberius::error::Error> {
        let mut conn = self.conn.lock().await;
        execute_on(&mut conn, sql, params).await
    }
}

pub(crate) async fn execute_on(
    client: &mut bb8_tiberius::rt::Client,
    sql: &str,
    params: &[MsSqlParameter],
) -> Result<u64, tiberius::error::Error> {
    let params = params.iter().map(|p| p as &dyn ToSql).collect::<Vec<_>>();
    let result = client.execute(sql, &params).await?;
    Ok(result.total())
}

impl DbConnection<MsSqlPooledConnection, MsSqlParameter> for MsSqlConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<MsSqlPooledConnection, MsSqlParameter>> {
        Some(self)
    }
}

fn generic(e: impl std::error::Error + Send + Sync + 'static) -> dbconnection::GenericError {
    Box::new(e)
}

#[async_trait::async_trait]
impl AsyncDbConnection<MsSqlPooledConnection, MsSqlParameter> for MsSqlConnection {
    fn new(conn: MsSqlPooledConnection) -> Self {
        Self::new_with_schemas(conn, Arc::default())
    }

    fn new_with_schemas(conn: MsSqlPooledConnection, schemas: Arc<TableSchemas>) -> Self {
        MsSqlConnection::new_with_schemas(conn, schemas)
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        let mut conn = self.conn.lock().await;
        let rows = conn
            .query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @P1",
                &[&schema],
            )
            .await
            .map_err(generic)
            .context(dbconnection::UnableToGetTablesSnafu)?
            .into_first_result()
            .await
            .map_err(generic)
            .context(dbconnection::UnableToGetTablesSnafu)?;
        Ok(rows
            .iter()
            .filter_map(|row| row.get::<&str, _>(0).map(ToString::to_string))
            .collect())
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        let mut conn = self.conn.lock().await;
        let rows = conn
            .simple_query(
                "SELECT s.name FROM sys.schemas s \
                 WHERE s.principal_id IN (SELECT principal_id FROM sys.database_principals WHERE type IN ('S', 'U', 'G', 'E', 'X')) \
                 AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest') \
                 AND s.name NOT LIKE 'db[_]%'",
            )
            .await
            .map_err(generic)
            .context(dbconnection::UnableToGetSchemasSnafu)?
            .into_first_result()
            .await
            .map_err(generic)
            .context(dbconnection::UnableToGetSchemasSnafu)?;
        Ok(rows
            .iter()
            .filter_map(|row| row.get::<&str, _>(0).map(ToString::to_string))
            .collect())
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        if let Some(schema) = self.schemas.get(table_reference) {
            return Ok(Arc::clone(schema));
        }

        let object_name = crate::dialect::quote_table_reference(table_reference);
        let mut conn = self.conn.lock().await;
        let rows = conn
            .query(
                "SELECT c.name, t.name, c.precision, c.scale, c.is_nullable \
                 FROM sys.columns c \
                 JOIN sys.types t ON c.system_type_id = t.user_type_id \
                 WHERE c.object_id = OBJECT_ID(@P1) \
                 ORDER BY c.column_id",
                &[&object_name.as_str()],
            )
            .await
            .map_err(generic)
            .context(dbconnection::UnableToGetSchemaSnafu)?
            .into_first_result()
            .await
            .map_err(generic)
            .context(dbconnection::UnableToGetSchemaSnafu)?;

        if rows.is_empty() {
            return Err(dbconnection::Error::UndefinedTable {
                table_name: table_reference.to_string(),
                source: Box::new(Error::TableNotFound {
                    table_name: table_reference.to_string(),
                }),
            });
        }

        let mut fields = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: &str = row.get(0).unwrap_or_default();
            let sql_type: &str = row.get(1).unwrap_or_default();
            let precision: u8 = row.get(2).unwrap_or_default();
            let scale: u8 = row.get(3).unwrap_or_default();
            let nullable: bool = row.get(4).unwrap_or(true);
            let data_type = map_sql_server_type_to_arrow(name, sql_type, precision, scale)
                .map_err(generic)
                .context(dbconnection::UnableToGetSchemaSnafu)?;
            fields.push(Field::new(name, data_type, nullable));
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[MsSqlParameter],
        projected_schema: Option<SchemaRef>,
    ) -> dbconnection::Result<SendableRecordBatchStream> {
        let sql = rewrite_sql(sql);
        tracing::debug!("SQL Server query: {sql}");
        let params = params.to_vec();
        let conn = Arc::clone(&self.conn);

        let mut batches = Box::pin(stream! {
            let mut conn = conn.lock().await;
            let param_refs = params.iter().map(|p| p as &dyn ToSql).collect::<Vec<_>>();
            let query_stream = conn
                .query(sql.as_str(), &param_refs)
                .await
                .map_err(|e| match e {
                    tiberius::error::Error::Server(ref token) if token.code() == INVALID_OBJECT_NAME => {
                        DataFusionError::Plan(e.to_string())
                    }
                    _ => DataFusionError::External(Box::new(Error::QueryError { source: e })),
                })?;
            // only the rows of the first result set
            let mut rows = query_stream.into_row_stream().chunks(BATCH_SIZE);
            let mut schema = projected_schema;
            while let Some(chunk) = rows.next().await {
                let rows = chunk
                    .into_iter()
                    .collect::<Result<Vec<Row>, _>>()
                    .context(QuerySnafu)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let batch_schema = match &schema {
                    Some(schema) => Arc::clone(schema),
                    None => {
                        let inferred = infer_schema(&rows);
                        schema = Some(Arc::clone(&inferred));
                        inferred
                    }
                };
                let batch = rows_to_arrow(&rows, &batch_schema)
                    .context(ConversionSnafu)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                yield Ok::<_, DataFusionError>(batch);
            }
            if schema.is_none() {
                yield Ok(datafusion::arrow::array::RecordBatch::new_empty(Arc::new(Schema::empty())));
            }
        });

        let first = match batches.next().await {
            Some(first) => first.map_err(generic)?,
            None => {
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    futures::stream::empty(),
                )))
            }
        };
        let schema = first.schema();
        let rest = batches
            .filter(|batch| futures::future::ready(!matches!(batch, Ok(b) if b.num_rows() == 0)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(first) })
                .chain(rest)
                .into_stream(),
        )))
    }

    async fn execute(&self, sql: &str, params: &[MsSqlParameter]) -> dbconnection::Result<u64> {
        let sql = rewrite_sql(sql);
        Ok(self.execute_raw(&sql, params).await?)
    }
}
