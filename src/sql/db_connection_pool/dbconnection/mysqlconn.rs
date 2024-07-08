use std::{any::Any, sync::Arc};

use crate::sql::arrow_sql_gen::{self, mysql::rows_to_arrow};
use arrow::datatypes::{Schema, SchemaRef};
use async_stream::stream;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::lock::Mutex;
use futures::{stream, StreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{prelude::ToValue, Conn, Params, Row};
use snafu::prelude::*;

use super::{AsyncDbConnection, DbConnection};
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    QueryError { source: mysql_async::Error },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError { source: arrow_sql_gen::mysql::Error },

    #[snafu(display("Unable to get MySQL query result stream"))]
    QueryResultStreamError {},
}

pub struct MySQLConnection {
    pub conn: Arc<Mutex<Conn>>,
}

impl<'a> DbConnection<Conn, &'a (dyn ToValue + Sync)> for MySQLConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn super::AsyncDbConnection<Conn, &'a (dyn ToValue + Sync)>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Conn, &'a (dyn ToValue + Sync)> for MySQLConnection {
    fn new(conn: Conn) -> Self {
        MySQLConnection {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let rows: Vec<Row> = conn
            .exec(
                &format!(
                    "SELECT * FROM {} LIMIT 1",
                    table_reference.to_quoted_string()
                ),
                Params::Empty,
            )
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let rec = rows_to_arrow(&rows)
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Ok(rec.schema())
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'a (dyn ToValue + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let params_vec: Vec<_> = params.iter().map(|&p| p.to_value()).collect();
        let sql = sql.replace('"', "");
        let conn = Arc::clone(&self.conn);

        let mut stream = Box::pin(stream! {
            let mut conn = conn.lock().await;
            let mut exec_iter = conn
                .exec_iter(sql, Params::from(params_vec))
                .await
                .context(QuerySnafu)?;

            let Some(stream) = exec_iter.stream::<Row>().await.context(QuerySnafu)? else {
                yield Err(Error::QueryResultStreamError {});
                return;
            };

            let mut chunked_stream = stream.chunks(4_000).boxed();

            while let Some(chunk) = chunked_stream.next().await {
                let rows = chunk
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .context(QuerySnafu)?;

                let rec = rows_to_arrow(&rows).context(ConversionSnafu)?;
                yield Ok::<_, Error>(rec)
            }
        });

        let Some(first_chunk) = stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let first_chunk = first_chunk?;
        let schema = first_chunk.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, {
            stream! {
                yield Ok(first_chunk);
                while let Some(batch) = stream.next().await {
                    yield batch
                        .map_err(|e| DataFusionError::Execution(format!("Failed to fetch batch: {e}")))
                }
            }
        })))
    }

    async fn execute(&self, query: &str, params: &[&'a (dyn ToValue + Sync)]) -> Result<u64> {
        let mut conn = self.conn.lock().await;
        let conn = &mut *conn;
        let params_vec: Vec<_> = params.iter().map(|&p| p.to_value()).collect();
        let _: Vec<Row> = conn
            .exec(query, Params::from(params_vec))
            .await
            .context(QuerySnafu)?;
        return Ok(conn.affected_rows());
    }
}
