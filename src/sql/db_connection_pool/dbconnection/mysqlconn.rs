use std::{any::Any, sync::Arc};

use crate::sql::arrow_sql_gen::mysql::map_column_to_data_type;
use crate::sql::arrow_sql_gen::{self, mysql::rows_to_arrow};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_stream::stream;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::lock::Mutex;
use futures::{stream, StreamExt};
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{prelude::ToValue, Conn, Params, Row};
use snafu::prelude::*;

use super::Result;
use super::{AsyncDbConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    QueryError { source: mysql_async::Error },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError { source: arrow_sql_gen::mysql::Error },

    #[snafu(display("Unable to get MySQL query result stream"))]
    QueryResultStreamError {},

    #[snafu(display("Unsupported column data type: {data_type}"))]
    UnsupportedDataTypeError { data_type: String },

    #[snafu(display("Unable to extract precision and scale from type: {data_type}"))]
    UnableToGetDecimalPrecisionAndScale { data_type: String },

    #[snafu(display("Field '{field}' is missing"))]
    MissingField { field: String },
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

        // we don't use SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' AND TABLE_SCHEMA = '{}'
        // as table_reference don't always have schema specified so we need to extract schema/db name from connection properties
        // to ensure we are querying information for correct table
        let columns_meta_query =
            format!("SHOW COLUMNS FROM {}", table_reference.to_quoted_string());

        let columns_meta: Vec<Row> = conn
            .exec(&columns_meta_query, Params::Empty)
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Ok(columns_meta_to_schema(columns_meta).context(super::UnableToGetSchemaSnafu)?)
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

fn columns_meta_to_schema(columns_meta: Vec<Row>) -> Result<SchemaRef> {
    let mut fields = Vec::new();

    for row in columns_meta.iter() {
        let column_name: String = row.get("Field").ok_or(Error::MissingField {
            field: "Field".to_string(),
        })?;

        let data_type: String = row.get("Type").ok_or(Error::MissingField {
            field: "Type".to_string(),
        })?;

        let column_type = map_str_type_to_column_type(&data_type)?;

        let arrow_data_type = match column_type {
            // map_column_to_data_type does not support decimal mapping and uses special logic to handle conversion based on actual value
            // so we handle it separately
            ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                let (_precision, scale) = extract_decimal_precision_and_scale(&data_type)
                    .context(super::UnableToGetSchemaSnafu)?;
                // rows_to_arrow uses hardcoded precision 38 for decimal so we use it here as well
                DataType::Decimal128(38, scale)
            }
            _ => map_column_to_data_type(column_type)
                .context(UnsupportedDataTypeSnafu { data_type })?,
        };
        fields.push(Field::new(&column_name, arrow_data_type, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn map_str_type_to_column_type(data_type: &str) -> Result<ColumnType> {
    let data_type = data_type.to_lowercase();
    let column_type = match data_type.as_str() {
        _ if data_type.starts_with("decimal") || data_type.starts_with("numeric") => {
            ColumnType::MYSQL_TYPE_DECIMAL
        }
        // most types can have addtional information: unsigned, size, etc so we use starts_with
        _ if data_type.starts_with("tinyint") => ColumnType::MYSQL_TYPE_TINY,
        _ if data_type.starts_with("smallint") => ColumnType::MYSQL_TYPE_SHORT,
        _ if data_type.starts_with("int") => ColumnType::MYSQL_TYPE_LONG,
        _ if data_type.starts_with("float") => ColumnType::MYSQL_TYPE_FLOAT,
        _ if data_type.starts_with("double") => ColumnType::MYSQL_TYPE_DOUBLE,
        _ if data_type.starts_with("bigint") => ColumnType::MYSQL_TYPE_LONGLONG,
        _ if data_type.starts_with("mediumint") => ColumnType::MYSQL_TYPE_INT24,
        _ if data_type.eq("null") => ColumnType::MYSQL_TYPE_NULL,
        _ if data_type.eq("timestamp2") => ColumnType::MYSQL_TYPE_TIMESTAMP2,
        _ if data_type.eq("timestamp") => ColumnType::MYSQL_TYPE_TIMESTAMP,
        _ if data_type.eq("datetime2") => ColumnType::MYSQL_TYPE_DATETIME2,
        _ if data_type.eq("datetime") => ColumnType::MYSQL_TYPE_DATETIME,
        _ if data_type.eq("time2") => ColumnType::MYSQL_TYPE_TIME2,
        _ if data_type.eq("time") => ColumnType::MYSQL_TYPE_TIME,
        _ if data_type.eq("date") => ColumnType::MYSQL_TYPE_DATE,
        _ if data_type.eq("year") => ColumnType::MYSQL_TYPE_YEAR,
        _ if data_type.eq("newdate") => ColumnType::MYSQL_TYPE_NEWDATE,
        _ if data_type.starts_with("varchar") => ColumnType::MYSQL_TYPE_VARCHAR,
        _ if data_type.starts_with("bit") => ColumnType::MYSQL_TYPE_BIT,
        _ if data_type.starts_with("array") => ColumnType::MYSQL_TYPE_TYPED_ARRAY,
        _ if data_type.starts_with("json") => ColumnType::MYSQL_TYPE_JSON,
        _ if data_type.starts_with("newdecimal") => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        _ if data_type.starts_with("enum") => ColumnType::MYSQL_TYPE_ENUM,
        _ if data_type.starts_with("set") => ColumnType::MYSQL_TYPE_SET,
        _ if data_type.starts_with("tinyblob") => ColumnType::MYSQL_TYPE_TINY_BLOB,
        _ if data_type.starts_with("mediumblob") => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
        _ if data_type.starts_with("longblob") => ColumnType::MYSQL_TYPE_LONG_BLOB,
        _ if data_type.starts_with("blob") => ColumnType::MYSQL_TYPE_BLOB,
        _ if data_type.starts_with("var_string") => ColumnType::MYSQL_TYPE_VAR_STRING,
        _ if data_type.starts_with("char") => ColumnType::MYSQL_TYPE_STRING,
        _ if data_type.starts_with("geometry") => ColumnType::MYSQL_TYPE_GEOMETRY,
        _ => UnsupportedDataTypeSnafu { data_type }.fail()?,
    };

    Ok(column_type)
}

fn extract_decimal_precision_and_scale(data_type: &str) -> Result<(u8, i8)> {
    let (start, end) = match (data_type.find('('), data_type.find(')')) {
        (Some(start), Some(end)) => (start, end),
        _ => UnableToGetDecimalPrecisionAndScaleSnafu { data_type }.fail()?,
    };
    let parts: Vec<&str> = data_type[start + 1..end].split(',').collect();
    if parts.len() != 2 {
        UnableToGetDecimalPrecisionAndScaleSnafu { data_type }.fail()?;
    }

    let precision =
        parts[0]
            .parse::<u8>()
            .map_err(|_| Error::UnableToGetDecimalPrecisionAndScale {
                data_type: data_type.to_string(),
            })?;
    let scale = parts[1]
        .parse::<i8>()
        .map_err(|_| Error::UnableToGetDecimalPrecisionAndScale {
            data_type: data_type.to_string(),
        })?;

    Ok((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_decimal_precision_and_scale() {
        let test_cases = vec![
            ("decimal(10,2)", 10, 2),
            ("DECIMAL(5,3)", 5, 3),
            ("numeric(12,4)", 12, 4),
            ("NUMERIC(8,6)", 8, 6),
            ("decimal(38,0)", 38, 0),
        ];

        for (data_type, expected_precision, expected_scale) in test_cases {
            let (precision, scale) = extract_decimal_precision_and_scale(data_type)
                .expect("Should extract precision and scale");
            assert_eq!(
                precision, expected_precision,
                "Incorrect precision for: {}",
                data_type
            );
            assert_eq!(scale, expected_scale, "Incorrect scale for: {}", data_type);
        }
    }
}
