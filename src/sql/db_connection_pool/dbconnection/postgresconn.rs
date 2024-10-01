use std::any::Any;
use std::error::Error;
use std::sync::Arc;

use crate::sql::arrow_sql_gen::postgres::rows_to_arrow;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::PostgresConnectionManager;
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

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let q = &format!("SELECT * FROM {table_reference} LIMIT 1");

        let rows = match self.conn.query(q.as_str(), &[]).await {
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

        let rec = match rows_to_arrow(rows.as_slice()) {
            Ok(rec) => rec,
            Err(e) => {
                return Err(super::Error::UnableToGetSchema {
                    source: Box::new(PostgresError::ConversionError { source: e }),
                })
            }
        };

        let schema = rec.schema();
        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a (dyn ToSql + Sync)],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: We should have a way to detect if params have been passed
        // if they haven't we should use .copy_out instead, because it should be much faster
        let streamable = self
            .conn
            .query_raw(sql, params.iter().copied()) // use .query_raw to get access to the underlying RowStream
            .await
            .context(QuerySnafu)?;

        // chunk the stream into groups of rows
        let mut stream = streamable.chunks(4_000).boxed().map(|rows| {
            let rows = rows
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(QuerySnafu)?;
            let rec = rows_to_arrow(rows.as_slice()).context(ConversionSnafu)?;
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
