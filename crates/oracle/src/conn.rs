use async_stream::stream;
use async_trait::async_trait;
use bb8_oracle::OracleConnectionManager;
use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use std::{any::Any, sync::Arc};

use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::task;

use crate::arrow_sql_gen::{map_oracle_type_to_arrow_type, rows_to_arrow};
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error, GenericError, Result, UnableToGetSchemaSnafu,
    UnableToGetSchemasSnafu, UnableToGetTablesSnafu, UnableToQueryArrowSnafu,
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
                // In Oracle, the default schema is the user's schema that is used to connect
                // when no specific schema is provided in a SQL statement. Use SYS_CONTEXT to
                // scope the lookup to that schema, rather than every owner in
                // `all_tab_columns` (which would duplicate columns when the same table name
                // exists in several schemas).
                let rows = conn.query(
                    "SELECT column_name, data_type, data_precision, data_scale, nullable
                                       FROM all_tab_columns
                                       WHERE table_name = :1
                                         AND owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
                                       ORDER BY column_id",
                    &[&table_name],
                )?;
                rows.collect::<std::result::Result<Vec<oracle::Row>, _>>()
            }
        })
        .await
        .map_err(|e| Box::new(e) as GenericError)
        .context(UnableToGetSchemaSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(UnableToGetSchemaSnafu)?;

        let mut fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();

        for row in rows {
            let column_name: String = row
                .get(0)
                .map_err(|e| Box::new(e) as GenericError)
                .context(UnableToGetSchemaSnafu)?;
            let data_type_str: String = row
                .get(1)
                .map_err(|e| Box::new(e) as GenericError)
                .context(UnableToGetSchemaSnafu)?;
            let precision: Option<i32> = row
                .get(2)
                .map_err(|e| Box::new(e) as GenericError)
                .context(UnableToGetSchemaSnafu)?;
            let scale: Option<i32> = row
                .get(3)
                .map_err(|e| Box::new(e) as GenericError)
                .context(UnableToGetSchemaSnafu)?;
            let nullable_str: String = row
                .get(4)
                .map_err(|e| Box::new(e) as GenericError)
                .context(UnableToGetSchemaSnafu)?;
            let nullable = nullable_str != "N";

            let Some(arrow_type) = map_oracle_type_to_arrow_type(&data_type_str, precision, scale)
            else {
                // Unknown types have no lossless Arrow representation; skip the
                // column rather than silently coercing it to a string.
                tracing::warn!(
                    "Oracle column '{column_name}' of table {table_reference} has unsupported data type '{data_type_str}' and will be ignored"
                );
                continue;
            };

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
                    .context(UnableToQueryArrowSnafu)?;

                let rows = stmt
                    .query(&[])
                    .map_err(|e| Box::new(e) as GenericError)
                    .context(UnableToQueryArrowSnafu)?;

                let mut chunk = Vec::with_capacity(4096);
                for row_result in rows {
                    let row = row_result
                        .map_err(|e| Box::new(e) as GenericError)
                        .context(UnableToQueryArrowSnafu)?;

                    chunk.push(row);
                    if chunk.len() >= 4096 {
                        let batch_res = rows_to_arrow(chunk, &schema_clone)
                            .map_err(|e| Box::new(e) as GenericError)
                            .context(UnableToQueryArrowSnafu)
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
                        .context(UnableToQueryArrowSnafu)
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
        .context(UnableToQueryArrowSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(UnableToQueryArrowSnafu)?;

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
        .context(UnableToGetTablesSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(UnableToGetTablesSnafu)?;

        Ok(table_names)
    }

    async fn schemas(&self) -> std::result::Result<Vec<String>, Error> {
        let conn = self.conn.clone();

        let schemas = task::spawn_blocking(move || {
            let rows = conn.query(
                "SELECT username FROM all_users
                 WHERE username NOT IN (
                     'APPQOSSYS', 'AUDSYS', 'CTXSYS', 'DBSFWUSER', 'DIP', 'DVF',
                     'DVSYS', 'GGSYS', 'GSMADMIN_INTERNAL', 'GSMCATUSER', 'GSMUSER',
                     'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORACLE_ML_USER',
                     'ORDDATA', 'ORDPLUGINS', 'ORDSYS', 'OUTLN', 'REMOTE_SCHEDULER_AGENT',
                     'SYS', 'SYS$UMF', 'SYSBACKUP', 'SYSDG', 'SYSKM', 'SYSRAC',
                     'SYSTEM', 'WMSYS', 'XDB', 'XS$NULL'
                 )",
                &[],
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
        .context(UnableToGetSchemasSnafu)?
        .map_err(|e| Box::new(e) as GenericError)
        .context(UnableToGetSchemasSnafu)?;

        Ok(schemas)
    }
}
