use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;
use crate::sql::arrow_sql_gen::trino::{
    self, arrow::rows_to_arrow, schema::trino_data_type_to_arrow_type,
};
use crate::UnsupportedTypeAction;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_stream::try_stream;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::Stream;
use futures::StreamExt;
use serde_json::Value;
use snafu::prelude::*;
use std::pin::Pin;
use std::time::Duration;
use std::{any::Any, sync::Arc};
use tokio::time::sleep;

pub type QueryStream = Pin<Box<dyn Stream<Item = Result<Vec<Vec<Value>>, Error>> + Send>>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Query execution failed.\n{source}\nFor details, refer to the Trino documentation: https://trino.io/docs/"))]
    QueryError { source: reqwest::Error },

    #[snafu(display("Failed to convert query result to Arrow.\n{source}\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    ConversionError { source: trino::Error },

    #[snafu(display("Authentication failed."))]
    AuthenticationFailedError,

    #[snafu(display("Trino server error: {status_code} - {message}"))]
    TrinoServerError { status_code: u16, message: String },

    #[snafu(display("Unsupported data type '{data_type}' for field '{column_name}'.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    UnsupportedDataTypeError {
        column_name: String,
        data_type: String,
    },

    #[snafu(display("Failed to find the field '{field}'.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    MissingFieldError { field: String },

    #[snafu(display("No schema was provide.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issuesd"))]
    NoSchema,
}

pub const DEFAULT_POLL_WAIT_TIME_MS: u64 = 50;

pub struct TrinoConnection {
    client: Arc<reqwest::Client>,
    base_url: String,
    unsupported_type_action: UnsupportedTypeAction,
    poll_wait_time: Duration,
    tz: Option<String>,
}

impl<'a> DbConnection<Arc<reqwest::Client>, &'a str> for TrinoConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<reqwest::Client>, &'a str>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Arc<reqwest::Client>, &'a str> for TrinoConnection {
    fn new(client: Arc<reqwest::Client>) -> Self {
        TrinoConnection {
            client,
            base_url: String::new(),
            unsupported_type_action: UnsupportedTypeAction::default(),
            poll_wait_time: Duration::from_millis(DEFAULT_POLL_WAIT_TIME_MS),
            tz: None,
        }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let sql = format!("DESCRIBE {table_reference}");
        let mut query_stream = self.execute_query(&sql);

        let mut fields = Vec::new();

        while let Some(batch_data) = query_stream.next().await {
            let batch_data = batch_data.map_err(|e| super::Error::UnableToGetSchema {
                source: Box::new(e),
            })?;

            for row_data in batch_data {
                if row_data.len() >= 2 {
                    let column_name =
                        row_data[0]
                            .as_str()
                            .ok_or_else(|| super::Error::UnableToGetSchema {
                                source: Box::new(Error::MissingFieldError {
                                    field: "column_name".to_string(),
                                }),
                            })?;

                    let data_type =
                        row_data[1]
                            .as_str()
                            .ok_or_else(|| super::Error::UnableToGetSchema {
                                source: Box::new(Error::MissingFieldError {
                                    field: "data_type".to_string(),
                                }),
                            })?;

                    let nullable = if row_data.len() > 2 {
                        row_data[2].as_str().unwrap_or("true") != "false"
                    } else {
                        true
                    };

                    let Ok(arrow_type) =
                        trino_data_type_to_arrow_type(data_type, self.tz.clone().as_deref())
                    else {
                        return Err(super::Error::UnsupportedDataType {
                            data_type: data_type.to_string(),
                            field_name: column_name.to_string(),
                        });
                    };

                    fields.push(Field::new(column_name, arrow_type, nullable));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[&'a str],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let schema_ref = projected_schema.ok_or(Error::NoSchema)?;

        let mut query_stream = self.execute_query(sql);

        let schema_for_stream = Arc::clone(&schema_ref);

        let mut arrow_stream = Box::pin(stream! {
            while let Some(batch_data) = query_stream.next().await {
                let batch_data = batch_data.map_err(|e| super::Error::UnableToQueryArrow {
                    source: Box::new(e),
                })?;

                if !batch_data.is_empty() {
                    let chunk_size = 4_000;
                    for chunk in batch_data.chunks(chunk_size) {
                        let rec = rows_to_arrow(chunk, Arc::clone(&schema_for_stream))
                            .map_err(|e| super::Error::UnableToQueryArrow {
                                source: Box::new(Error::ConversionError { source: e }),
                            })?;
                        yield Ok::<_, super::Error>(rec);
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, {
            stream! {
                while let Some(batch) = arrow_stream.next().await {
                    yield batch
                        .map_err(|e| DataFusionError::Execution(format!("Failed to fetch batch: {e}")))
                }
            }
        })))
    }

    async fn execute(&self, _query: &str, _params: &[&'a str]) -> Result<u64> {
        unimplemented!("Execute not implemented for Trino");
    }
}

impl TrinoConnection {
    pub fn new_with_config(
        client: Arc<reqwest::Client>,
        base_url: String,
        poll_wait_time: Duration,
        tz: Option<String>,
    ) -> Self {
        TrinoConnection {
            client,
            base_url,
            unsupported_type_action: UnsupportedTypeAction::default(),
            poll_wait_time,
            tz,
        }
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    fn execute_query(&self, sql: &str) -> QueryStream {
        let client = self.client.clone();
        let url = format!("{}/v1/statement", self.base_url);
        let poll_wait_time = self.poll_wait_time;
        let sql = sql.to_string();

        Box::pin(try_stream! {
            let mut result: Value = client
                .post(&url)
                .body(sql)
                .send()
                .await
                .context(QuerySnafu)?
                .json()
                .await
                .context(QuerySnafu)?;

            loop {
                let state = result["stats"]["state"].as_str().unwrap_or("");

                if state == "FAILED" {
                    Err(Error::TrinoServerError {
                        status_code: 500,
                        message: "Query failed".to_string(),
                    })?;
                } else if state == "CANCELED" {
                    Err(Error::TrinoServerError {
                        status_code: 499,
                        message: "Query was canceled".to_string(),
                    })?;
                }

                if let Some(data) = result.get("data").and_then(|d| d.as_array()) {
                    let batch_data = data.iter()
                        .filter_map(|row| row.as_array().cloned())
                        .collect::<Vec<_>>();

                    if !batch_data.is_empty() {
                        yield batch_data;
                    }
                }

                if state == "FINISHED" {
                    break;
                }

                if let Some(next_uri) = result.get("nextUri").and_then(|u| u.as_str()) {
                    sleep(poll_wait_time).await;

                    result = client
                        .get(next_uri)
                        .send()
                        .await
                        .context(QuerySnafu)?
                        .json()
                        .await
                        .context(QuerySnafu)?;
                } else {
                    if state != "FINISHED" {
                        Err(Error::TrinoServerError {
                            status_code: 500,
                            message: format!("Query stuck in state: {state}"),
                        })?;
                    }
                    break;
                }
            }
        })
    }
}
