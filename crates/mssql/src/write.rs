use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{dml::InsertOp, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::sql::unparser::Unparser;
use futures::StreamExt;
use snafu::prelude::*;

use datafusion_table_providers_common::util::{
    constraints,
    dml::{filters_to_sql, DeletionExec, DeletionSink, UpdateExec, UpdateSink},
    on_conflict::OnConflict,
    retriable_error::check_and_mark_retriable_error,
    to_datafusion_error,
};

use crate::dialect::{quote_identifier, rewrite_sql, MsSqlDialect};
use crate::MsSql;

/// A SQL Server table that supports `INSERT INTO`, `INSERT OVERWRITE`, `UPDATE` and `DELETE`.
#[derive(Debug, Clone)]
pub struct MsSqlTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    mssql: Arc<MsSql>,
    on_conflict: Option<OnConflict>,
}

impl MsSqlTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        mssql: MsSql,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            mssql: Arc::new(mssql),
            on_conflict,
        })
    }

    #[must_use]
    pub fn mssql(&self) -> Arc<MsSql> {
        Arc::clone(&self.mssql)
    }
}

#[async_trait]
impl TableProvider for MsSqlTableWriter {
    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(self.mssql.constraints())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.read_provider.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(MsSqlDataSink::new(
                Arc::clone(&self.mssql),
                op == InsertOp::Overwrite,
                self.on_conflict.clone(),
                self.schema(),
            )),
            None,
        )))
    }

    /// Deletes the rows the filters select, or all the rows when there are no filters.
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let sql_where = where_clause(&filters)?;
        Ok(Arc::new(DeletionExec::new(Arc::new(MsSqlDeletionSink {
            mssql: Arc::clone(&self.mssql),
            sql_where,
        }))))
    }

    /// Updates the rows the filters select, or all the rows when there are no filters.
    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if assignments.is_empty() {
            return Ok(Arc::new(UpdateExec::new(Arc::new(NoChangeSink))));
        }
        let set_clause = set_clause(&assignments)?;
        let sql_where = where_clause(&filters)?;
        Ok(Arc::new(UpdateExec::new(Arc::new(MsSqlUpdateSink {
            mssql: Arc::clone(&self.mssql),
            set_clause,
            sql_where,
        }))))
    }
}

fn where_clause(filters: &[Expr]) -> datafusion::error::Result<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }
    filters_to_sql(filters, &MsSqlDialect {}).map(Some)
}

fn set_clause(assignments: &[(String, Expr)]) -> datafusion::error::Result<String> {
    let dialect = MsSqlDialect {};
    let unparser = Unparser::new(&dialect);
    let parts = assignments
        .iter()
        .map(|(column, value)| {
            unparser
                .expr_to_sql(value)
                .map(|value| format!("{} = {value}", quote_identifier(column)))
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    Ok(parts.join(", "))
}

struct MsSqlDeletionSink {
    mssql: Arc<MsSql>,
    sql_where: Option<String>,
}

#[async_trait]
impl DeletionSink for MsSqlDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.mssql.quoted_table_name();
        let sql = match &self.sql_where {
            Some(filter) => format!("DELETE FROM {table} WHERE {filter}"),
            None => format!("DELETE FROM {table}"),
        };
        Ok(self.mssql.execute_statement(&rewrite_sql(&sql)).await?)
    }
}

struct MsSqlUpdateSink {
    mssql: Arc<MsSql>,
    set_clause: String,
    sql_where: Option<String>,
}

#[async_trait]
impl UpdateSink for MsSqlUpdateSink {
    async fn execute_update(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.mssql.quoted_table_name();
        let set = &self.set_clause;
        let sql = match &self.sql_where {
            Some(filter) => format!("UPDATE {table} SET {set} WHERE {filter}"),
            None => format!("UPDATE {table} SET {set}"),
        };
        Ok(self.mssql.execute_statement(&rewrite_sql(&sql)).await?)
    }
}

/// An update that assigns nothing: no rows are changed.
struct NoChangeSink;

#[async_trait]
impl UpdateSink for NoChangeSink {
    async fn execute_update(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(0)
    }
}

/// Writes the record batches of an `INSERT` into a SQL Server table in one transaction.
pub struct MsSqlDataSink {
    pub mssql: Arc<MsSql>,
    pub overwrite: bool,
    pub on_conflict: Option<OnConflict>,
    schema: SchemaRef,
}

impl MsSqlDataSink {
    #[must_use]
    pub fn new(
        mssql: Arc<MsSql>,
        overwrite: bool,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            mssql,
            overwrite,
            on_conflict,
            schema,
        }
    }
}

#[async_trait]
impl DataSink for MsSqlDataSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut db_conn = self.mssql.connect().await.map_err(to_datafusion_error)?;
        let conn = MsSql::mssql_conn(&mut db_conn).map_err(to_datafusion_error)?;
        let mut client = conn.conn.lock().await;

        crate::begin_transaction(&mut client)
            .await
            .map_err(to_datafusion_error)?;

        let result: datafusion::common::Result<u64> = async {
            if self.overwrite {
                self.mssql
                    .delete_all_table_data(&mut client)
                    .await
                    .map_err(to_datafusion_error)?;
            }

            let mut num_rows = 0u64;
            while let Some(batch) = data.next().await {
                let batch: RecordBatch = batch.map_err(check_and_mark_retriable_error)?;
                if batch.num_rows() == 0 {
                    continue;
                }
                num_rows += batch.num_rows() as u64;

                constraints::validate_batch_with_constraints(
                    std::slice::from_ref(&batch),
                    self.mssql.constraints(),
                )
                .await
                .context(crate::ConstraintViolationSnafu)
                .map_err(to_datafusion_error)?;

                self.mssql
                    .insert_batch(&mut client, &batch, self.on_conflict.as_ref())
                    .await
                    .map_err(to_datafusion_error)?;
            }
            Ok(num_rows)
        }
        .await;

        match result {
            Ok(num_rows) => {
                crate::commit_transaction(&mut client)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(num_rows)
            }
            Err(e) => {
                if let Err(rollback_error) = crate::rollback_transaction(&mut client).await {
                    tracing::warn!(
                        "Failed to roll back the SQL Server transaction: {rollback_error}"
                    );
                }
                Err(e)
            }
        }
    }
}

impl fmt::Debug for MsSqlDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MsSqlDataSink")
    }
}

impl DisplayAs for MsSqlDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MsSqlDataSink")
    }
}
