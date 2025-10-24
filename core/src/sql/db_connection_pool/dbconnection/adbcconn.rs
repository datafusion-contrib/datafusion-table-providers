// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use adbc_core::Statement;
use adbc_core::{Connection, Database};
use async_stream::stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use std::any::Any;
use std::cell::RefCell;

use std::sync::{Arc,};
use tokio::sync::mpsc::Sender;
use adbc_core::options::ObjectDepth;
use arrow::array::{AsArray, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use r2d2_adbc::AdbcConnectionManager;
use snafu::{prelude::*, ResultExt};
use std::marker::Send;
use std::marker::Sync;

use crate::sql::db_connection_pool::runtime::run_sync_with_tokio;

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ADBC Error: {source}"))]
    AdbcError { source: adbc_core::error::Error },

    #[snafu(display(
        "An unexpected error occurred.\n{message}\nVerify the configuration and try again"
    ))]
    ChannelError { message: String },
}

pub struct AdbcDbConnection<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Send + Sync,
{
    pub conn: RefCell<r2d2::PooledConnection<AdbcConnectionManager<D>>>,
}

impl<D> DbConnection<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>
    for AdbcDbConnection<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Send + Sync,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_sync(
        &self,
    ) -> Option<&dyn SyncDbConnection<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>>
    {
        Some(self)
    }
}

fn blocking_channel_send<T>(channel: &Sender<T>, item: T) -> Result<()> {
    match channel.blocking_send(item) {
        Ok(()) => Ok(()),
        Err(e) => Err(Error::ChannelError {
            message: format!("{e}"),
        }
        .into()),
    }
}

impl<D> SyncDbConnection<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>
    for AdbcDbConnection<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Send + Sync,
{
    fn new(conn: r2d2::PooledConnection<AdbcConnectionManager<D>>) -> Self {
        AdbcDbConnection {
            conn: RefCell::new(conn),
        }
    }

    fn tables(&self, schema: &str) -> Result<Vec<String>, super::Error> {
        let conn = self.conn.borrow();
        let mut result = conn
            .get_objects(ObjectDepth::Tables, None, Some(schema), None, None, None)
            .boxed()
            .context(super::UnableToGetTablesSnafu)?;

        let mut tables = vec![];
        while let Some(batch) = result.next() {
            // Process each batch to extract table names
            //
            // Schema is as follows:
            // 0: CATALOG_NAME
            // 1: list<DB_SCHEMA_SCHEMA>
            //
            // DB_SCHEMA_SCHEMA is as follows:
            // 0: SCHEMA_NAME
            // 1: list<TABLE_INFO>
            //
            // TABLE_INFO is as follows:
            // 0: TABLE_NAME
            // 1: TABLE_TYPE
            // 2: list<COLUMN_SCHEMA>
            // 3: list<CONSTRAINT_SCHEMA>
            //
            // so we need to drill down to the table names
            let b = batch.boxed().context(super::UnableToGetTablesSnafu)?;
            b.column(1).as_list::<i32>().iter().for_each(|value| {
                if let Some(db_schema_schema) = value {
                    db_schema_schema
                        .as_struct()
                        .column(1)
                        .as_list::<i32>()
                        .iter()
                        .for_each(|table_info| {
                            if let Some(table_struct) = table_info {
                                tables.extend(
                                    table_struct
                                        .as_struct()
                                        .column(1)
                                        .as_string::<i32>()
                                        .iter()
                                        .filter_map(|name| name)
                                        .map(|name| name.to_string()),
                                );
                            }
                        })
                }
            })
        }

        Ok(tables)
    }

    fn schemas(&self) -> Result<Vec<String>, super::Error> {
        let conn = self.conn.borrow();

        let mut result = conn
            .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let mut schemas = vec![];
        while let Some(batch) = result.next() {
            // Process each batch to extract schema names
            //
            // Schema is as follows:
            // 0: CATALOG_NAME
            // 1: list<DB_SCHEMA_SCHEMA>
            //
            // DB_SCHEMA_SCHEMA is as follows:
            // 0: SCHEMA_NAME
            // 1: list<TABLE_INFO>
            //
            // so we need to drill down to the schema names
            let b = batch.boxed().context(super::UnableToGetSchemaSnafu)?;
            b.column(1).as_list::<i32>().iter().for_each(|value| {
                if let Some(db_schema_schema) = value {
                    db_schema_schema
                        .as_struct()
                        .column(0)
                        .as_string::<i32>()
                        .iter()
                        .filter_map(|name| name)
                        .for_each(|name| schemas.push(name.to_string()));
                }
            });
        }
        Ok(schemas)
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, super::Error> {
        let conn = self.conn.borrow();

        let schema = conn
            .get_table_schema(
                table_reference.catalog(),
                table_reference.schema(),
                table_reference.table(),
            )
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Ok(Arc::new(schema))
    }

    fn query_arrow(
        &self,
        sql: &str,
        params: &[RecordBatch],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let create_stream = || -> Result<SendableRecordBatchStream> {
            let schema;
            {
                let mut conn = self.conn.borrow_mut();
                let mut stmt = conn
                    .new_statement()
                    .boxed()
                    .context(super::UnableToQueryArrowSnafu)?;
                stmt.set_sql_query(sql)?;

                schema = stmt
                    .execute_schema()
                    .boxed()
                    .context(super::UnableToQueryArrowSnafu)?;
            }

            let mut conn = self.conn.replace(
                // This is a temporary placeholder that will be immediately replaced
                unsafe { std::mem::zeroed() }
            );
            
            let sql_owned = sql.to_string();
            let params_owned = params.to_vec();
            
            let join_handle = tokio::task::spawn_blocking(move || {                
                let mut stmt = conn
                    .new_statement()
                    .boxed()
                    .context(super::UnableToQueryArrowSnafu)?;
                stmt.set_sql_query(&sql_owned)?;

                match params_owned.len() {
                    0 => {}
                    1 => stmt.bind(params_owned[0].clone())?,
                    _ => {
                        let param_schema = params_owned[0].schema();
                        let reader = RecordBatchIterator::new(
                            params_owned.into_iter().map(Ok),
                            param_schema,
                        );

                        stmt.bind_stream(Box::new(reader))?;
                    }
                }

                let results = stmt
                    .execute()
                    .boxed()
                    .context(super::UnableToQueryArrowSnafu)?;
                for batch in results {
                    let b = batch.boxed().context(super::UnableToQueryArrowSnafu)?;
                    blocking_channel_send(&batch_tx, b)?;
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(conn)
            });

            let output_stream = stream! {
                while let Some(batch) = batch_rx.recv().await {
                    yield Ok(batch);
                }

                match join_handle.await {
                    Ok(Ok(returned_conn)) => {
                        // Return the connection to the RefCell
                        // This is safe because we have exclusive access via self
                        self.conn.replace(returned_conn);
                    },
                    Ok(Err(task_error)) => {
                        yield Err(DataFusionError::Execution(format!(
                            "Failed to execute ADBC query: {task_error}"
                        )))
                    },
                    Err(join_error) => {
                        yield Err(DataFusionError::Execution(format!(
                            "Failed to execute ADBC query: {join_error}"
                        )))
                    },
                }
            };

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema.into(),
                output_stream,
            )))
        };

        run_sync_with_tokio(create_stream)
    }

    fn execute(&self, sql: &str, params: &[RecordBatch]) -> Result<u64> {
        Ok(0)
    }
}
