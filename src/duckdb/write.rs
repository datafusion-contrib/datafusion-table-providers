use std::{any::Any, fmt, sync::Arc};

use crate::duckdb::DuckDB;
use crate::util::{
    constraints, on_conflict::OnConflict, retriable_error::check_and_mark_retriable_error,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::common::Constraints;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        insert::{DataSink, DataSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use duckdb::Transaction;
use futures::StreamExt;
use snafu::prelude::*;

use super::to_datafusion_error;

pub struct DuckDBTableWriter {
    read_provider: Arc<dyn TableProvider>,
    duckdb: Arc<DuckDB>,
    on_conflict: Option<OnConflict>,
}

impl DuckDBTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        duckdb: DuckDB,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            duckdb: Arc::new(duckdb),
            on_conflict,
        })
    }

    pub fn duckdb(&self) -> Arc<DuckDB> {
        Arc::clone(&self.duckdb)
    }
}

#[async_trait]
impl TableProvider for DuckDBTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(self.duckdb.constraints())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(DuckDBDataSink::new(
                Arc::clone(&self.duckdb),
                overwrite,
                self.on_conflict.clone(),
            )),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
pub(crate) struct DuckDBDataSink {
    duckdb: Arc<DuckDB>,
    overwrite: bool,
    on_conflict: Option<OnConflict>,
}

#[async_trait]
impl DataSink for DuckDBDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut db_conn = self.duckdb.connect_sync().map_err(to_datafusion_error)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let data_batches_result = data
            .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
            .await;

        let data_batches: Vec<RecordBatch> = data_batches_result
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(check_and_mark_retriable_error)?;

        constraints::validate_batch_with_constraints(&data_batches, self.duckdb.constraints())
            .await
            .context(super::ConstraintViolationSnafu)
            .map_err(to_datafusion_error)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let num_rows = match **self.duckdb.constraints() {
            [] => self.try_write_all_no_constraints(&tx, &data_batches)?,
            _ => self.try_write_all_with_constraints(&tx, &data_batches)?,
        };

        tx.commit()
            .context(super::UnableToCommitTransactionSnafu)
            .map_err(to_datafusion_error)?;

        Ok(num_rows)
    }
}

impl DuckDBDataSink {
    pub(crate) fn new(
        duckdb: Arc<DuckDB>,
        overwrite: bool,
        on_conflict: Option<OnConflict>,
    ) -> Self {
        Self {
            duckdb,
            overwrite,
            on_conflict,
        }
    }

    /// If there are constraints on the `DuckDB` table, we need to create an empty copy of the target table, write to that table copy and then depending on
    /// if the mode is overwrite or not, insert into the target table or drop the target table and rename the current table.
    ///
    /// See: <https://duckdb.org/docs/sql/indexes#over-eager-unique-constraint-checking>
    fn try_write_all_with_constraints(
        &self,
        tx: &Transaction<'_>,
        data_batches: &Vec<RecordBatch>,
    ) -> datafusion::common::Result<u64> {
        // We want to clone the current table into our insert table
        let Some(ref orig_table_creator) = self.duckdb.table_creator else {
            return Err(DataFusionError::Execution(
                "Expected table with constraints to have a table creator".to_string(),
            ));
        };

        let mut num_rows = 0;

        for data_batch in data_batches {
            num_rows += u64::try_from(data_batch.num_rows()).map_err(|e| {
                DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
            })?;
        }

        let mut insert_table = orig_table_creator
            .create_empty_clone(tx)
            .map_err(to_datafusion_error)?;

        let Some(insert_table_creator) = insert_table.table_creator.take() else {
            unreachable!()
        };

        for (i, batch) in data_batches.iter().enumerate() {
            tracing::debug!(
                "Inserting batch #{i}/{} into cloned table.",
                data_batches.len()
            );
            insert_table
                .insert_batch_no_constraints(tx, batch)
                .map_err(to_datafusion_error)?;
        }

        if self.overwrite {
            insert_table_creator
                .replace_table(tx, orig_table_creator)
                .map_err(to_datafusion_error)?;
        } else {
            insert_table
                .insert_table_into(tx, &self.duckdb, self.on_conflict.as_ref())
                .map_err(to_datafusion_error)?;
            insert_table_creator
                .delete_table(tx)
                .map_err(to_datafusion_error)?;
        }

        Ok(num_rows)
    }

    /// If there are no constraints on the `DuckDB` table, we can do a simple single transaction write.
    fn try_write_all_no_constraints(
        &self,
        tx: &Transaction<'_>,
        data_batches: &Vec<RecordBatch>,
    ) -> datafusion::common::Result<u64> {
        let mut num_rows = 0;

        for data_batch in data_batches {
            num_rows += u64::try_from(data_batch.num_rows()).map_err(|e| {
                DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
            })?;
        }

        if self.overwrite {
            tracing::debug!("Deleting all data from table.");
            self.duckdb
                .delete_all_table_data(tx)
                .map_err(to_datafusion_error)?;
        }

        for (i, batch) in data_batches.iter().enumerate() {
            tracing::debug!("Inserting batch #{i}/{} into table.", data_batches.len());
            self.duckdb
                .insert_batch_no_constraints(tx, batch)
                .map_err(to_datafusion_error)?;
        }

        Ok(num_rows)
    }
}

impl std::fmt::Debug for DuckDBDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}

impl DisplayAs for DuckDBDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}
