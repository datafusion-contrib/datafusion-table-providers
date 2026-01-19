use async_trait::async_trait;
use bb8_oracle::OracleConnectionManager;
use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use std::{any::Any, sync::Arc};

use async_stream::stream;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::task;

use crate::sql::{
    arrow_sql_gen::oracle::rows_to_arrow,
    db_connection_pool::dbconnection::{
        AsyncDbConnection, DbConnection, Error, GenericError, Result,
    },
};

pub type OraclePooledConnection = bb8::PooledConnection<'static, OracleConnectionManager>;

pub struct OracleConnection {
    pub conn: OraclePooledConnection,
}

impl OracleConnection {
    pub fn new(conn: OraclePooledConnection) -> Self {
        Self { conn }
    }
}

impl DbConnection<OraclePooledConnection, oracle::sql_type::OracleType> for OracleConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(
        &self,
    ) -> Option<&dyn AsyncDbConnection<OraclePooledConnection, oracle::sql_type::OracleType>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<OraclePooledConnection, oracle::sql_type::OracleType> for OracleConnection {
    fn new(conn: OraclePooledConnection) -> Self {
        Self { conn }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> std::result::Result<SchemaRef, Error> {
        let table_name = table_reference.table().to_uppercase();
        let schema_name = table_reference.schema().map(|s| s.to_uppercase());

        let conn = self.conn.clone();

        let rows = task::spawn_blocking(move || {
            if let Some(schema) = schema_name {
                let rows = conn.query(
                    "SELECT column_name, data_type, data_precision, data_scale, nullable 
                                       FROM all_tab_columns 
                                       WHERE owner = :1 AND table_name = :2 
                                       ORDER BY column_id",
                    &[&schema, &table_name],
                )?;
                rows.collect::<std::result::Result<Vec<oracle::Row>, _>>()
            } else {
                let rows = conn.query(
                    "SELECT column_name, data_type, data_precision, data_scale, nullable 
                                       FROM all_tab_columns 
                                       WHERE table_name = :1 
                                       ORDER BY column_id",
                    &[&table_name],
                )?;
                rows.collect::<std::result::Result<Vec<oracle::Row>, _>>()
            }
        })
        .await
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToGetSchemaSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToGetSchemaSnafu)?;

        let mut fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();

        for row in rows {
            let column_name: String = row
                .get(0)
                .map_err(|e| Box::new(e) as GenericError)
                .context(super::UnableToGetSchemaSnafu)?;
            let data_type_str: String = row
                .get(1)
                .map_err(|e| Box::new(e) as GenericError)
                .context(super::UnableToGetSchemaSnafu)?;
            let precision: Option<i32> = row
                .get(2)
                .map_err(|e| Box::new(e) as GenericError)
                .context(super::UnableToGetSchemaSnafu)?;
            let scale: Option<i32> = row
                .get(3)
                .map_err(|e| Box::new(e) as GenericError)
                .context(super::UnableToGetSchemaSnafu)?;
            let nullable_str: String = row
                .get(4)
                .map_err(|e| Box::new(e) as GenericError)
                .context(super::UnableToGetSchemaSnafu)?;
            let nullable = nullable_str != "N";

            let arrow_type = map_oracle_type_to_arrow(&data_type_str, precision, scale);

            fields.push(datafusion::arrow::datatypes::Field::new(
                column_name, // Keep original case from Oracle
                arrow_type,
                nullable,
            ));
        }

        Ok(Arc::new(datafusion::arrow::datatypes::Schema::new(fields)))
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[oracle::sql_type::OracleType],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let sql = sql.to_string();
        let conn = self.conn.clone();
        let schema_clone = projected_schema.clone();

        let (tx, mut rx) = mpsc::channel(2);

        task::spawn_blocking(move || {
            let process = || -> std::result::Result<(), GenericError> {
                let mut stmt = conn
                    .statement(&sql)
                    .fetch_array_size(100_000)
                    .build()
                    .map_err(|e| Box::new(e) as GenericError)
                    .context(super::UnableToQueryArrowSnafu)?;

                let rows = stmt
                    .query(&[])
                    .map_err(|e| Box::new(e) as GenericError)
                    .context(super::UnableToQueryArrowSnafu)?;

                let mut chunk = Vec::with_capacity(4096);
                for row_result in rows {
                    let row = row_result
                        .map_err(|e| Box::new(e) as GenericError)
                        .context(super::UnableToQueryArrowSnafu)?;

                    chunk.push(row);
                    if chunk.len() >= 4096 {
                        let batch_res = rows_to_arrow(chunk, &schema_clone)
                            .map_err(|e| Box::new(e) as GenericError)
                            .context(super::UnableToQueryArrowSnafu)
                            .map_err(|e| Box::new(e) as GenericError);

                        if tx.blocking_send(batch_res).is_err() {
                            return Ok(());
                        }
                        chunk = Vec::with_capacity(4096);
                    }
                }
                if !chunk.is_empty() {
                    let batch_res = rows_to_arrow(chunk, &schema_clone)
                        .map_err(|e| Box::new(e) as GenericError)
                        .context(super::UnableToQueryArrowSnafu)
                        .map_err(|e| Box::new(e) as GenericError);
                    let _ = tx.blocking_send(batch_res);
                }
                Ok(())
            };

            if let Err(e) = process() {
                let _ = tx.blocking_send(Err(e));
            }
        });

        // Peek first batch to determine schema if needed
        let first_result = rx.recv().await;

        let Some(first_batch_res) = first_result else {
            // Stream empty
            let empty_schema = projected_schema
                .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                empty_schema,
                futures::stream::empty(),
            )));
        };

        let first_batch = first_batch_res?;
        let schema = first_batch.schema();

        let output_stream = stream! {
            yield Ok(first_batch);
            while let Some(result) = rx.recv().await {
                 match result {
                     Ok(batch) => yield Ok(batch),
                     Err(e) => yield Err(datafusion::error::DataFusionError::External(e)),
                 }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema.unwrap_or(schema),
            output_stream,
        )))
    }

    async fn execute(&self, sql: &str, _params: &[oracle::sql_type::OracleType]) -> Result<u64> {
        let sql = sql.to_string();
        let conn = self.conn.clone();

        let row_count = task::spawn_blocking(move || {
            let stmt = conn.execute(&sql, &[])?;
            stmt.row_count()
        })
        .await
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToQueryArrowSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToQueryArrowSnafu)?;

        Ok(row_count)
    }

    async fn tables(&self, schema: &str) -> std::result::Result<Vec<String>, Error> {
        let schema = schema.to_uppercase();
        let conn = self.conn.clone();

        let table_names = task::spawn_blocking(move || {
            let rows = conn.query(
                "SELECT table_name FROM all_tables WHERE owner = :1",
                &[&schema],
            )?;
            let mut result = Vec::new();
            for row in rows {
                let row = row?;
                let val: String = row.get(0)?;
                result.push(val);
            }
            Ok::<Vec<String>, oracle::Error>(result)
        })
        .await
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToGetTablesSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToGetTablesSnafu)?;

        Ok(table_names)
    }

    async fn schemas(&self) -> std::result::Result<Vec<String>, Error> {
        let conn = self.conn.clone();

        let schemas = task::spawn_blocking(move || {
            let rows = conn.query("SELECT username FROM all_users", &[])?;
            let mut result = Vec::new();
            for row in rows {
                let row = row?;
                let val: String = row.get(0)?;
                result.push(val);
            }
            Ok::<Vec<String>, oracle::Error>(result)
        })
        .await
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToGetSchemasSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(super::UnableToGetSchemasSnafu)?;

        Ok(schemas)
    }
}

/// Map Oracle data types to Arrow data types
fn map_oracle_type_to_arrow(
    oracle_type: &str,
    precision: Option<i32>,
    scale: Option<i32>,
) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;

    let type_upper = oracle_type.to_uppercase();

    // Handle types with parameters like VARCHAR2(100)
    let base_type = if let Some(paren_pos) = type_upper.find('(') {
        &type_upper[..paren_pos]
    } else {
        &type_upper
    };

    match base_type.trim() {
        // String types
        "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" => DataType::Utf8,
        "CLOB" | "NCLOB" | "LONG" => DataType::LargeUtf8,

        // Numeric types
        "NUMBER" | "NUMERIC" | "DECIMAL" | "DEC" => {
            let p = precision.unwrap_or(38) as u8;
            let s = scale.unwrap_or(0) as i8;
            // Int64 for integer types (scale = 0, precision ≤ 18)
            if s == 0 && p <= 18 {
                return DataType::Int64;
            }
            if p > 38 {
                DataType::Decimal256(p, s)
            } else {
                DataType::Decimal128(p, s)
            }
        }
        "INTEGER" | "INT" | "SMALLINT" => DataType::Int64,
        "FLOAT" => {
            // FLOAT precision in Oracle is binary: ≤24 → Float32, >24 → Float64
            match precision {
                Some(p) if p <= 24 => DataType::Float32,
                _ => DataType::Float64,
            }
        }
        "REAL" | "DOUBLE PRECISION" => DataType::Float64,
        "BINARY_FLOAT" => DataType::Float32,
        "BINARY_DOUBLE" => DataType::Float64,

        "BOOLEAN" => DataType::Boolean,

        // Date/Time types - Oracle DATE is conventionally a date, not a timestamp
        "DATE" => DataType::Date32,
        _ if type_upper.contains("TIMESTAMP") => {
            use datafusion::arrow::datatypes::TimeUnit;
            // Precision-aware timestamp: scale contains fractional seconds precision
            let fractional_precision = scale.unwrap_or(6);
            let time_unit = match fractional_precision {
                0 => TimeUnit::Second,
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                _ => TimeUnit::Nanosecond,
            };
            let tz = if type_upper.contains("WITH TIME ZONE")
                || type_upper.contains("WITH LOCAL TIME ZONE")
            {
                Some("UTC".into())
            } else {
                None
            };
            DataType::Timestamp(time_unit, tz)
        }

        // Interval types
        _ if type_upper.starts_with("INTERVAL YEAR") => {
            use datafusion::arrow::datatypes::IntervalUnit;
            DataType::Interval(IntervalUnit::YearMonth)
        }
        _ if type_upper.starts_with("INTERVAL DAY") => {
            use datafusion::arrow::datatypes::IntervalUnit;
            DataType::Interval(IntervalUnit::MonthDayNano)
        }

        // Binary types
        "RAW" => DataType::Binary,
        "BLOB" | "LONG RAW" => DataType::LargeBinary,

        // Other types - default to string
        _ => DataType::Utf8,
    }
}
