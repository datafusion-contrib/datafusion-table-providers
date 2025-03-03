use std::{any::Any, fmt, sync::Arc};

use crate::duckdb::DuckDB;
use crate::util::{
    constraints,
    on_conflict::OnConflict,
    retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error},
};
use async_trait::async_trait;
use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
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
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use super::to_datafusion_error;

#[derive(Clone)]
pub struct DuckDBTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    duckdb: Arc<DuckDB>,
    on_conflict: Option<OnConflict>,
}

impl std::fmt::Debug for DuckDBTableWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DuckDBTableWriter")
    }
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
        state: &dyn Session,
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
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(DuckDBDataSink::new(
                Arc::clone(&self.duckdb),
                op == InsertOp::Overwrite,
                self.on_conflict.clone(),
                self.schema(),
            )),
            None,
        )) as _)
    }
}

#[derive(Clone)]
pub(crate) struct DuckDBDataSink {
    duckdb: Arc<DuckDB>,
    overwrite: bool,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
}

#[async_trait]
impl DataSink for DuckDBDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let duckdb = Arc::clone(&self.duckdb);
        let overwrite = self.overwrite;
        let on_conflict = self.on_conflict.clone();

        // Limit channel size to a maximum of 100 RecordBatches queued for cases when DuckDB is slower than the writer stream,
        // so that we don't significantly increase memory usage. After the maximum RecordBatches are queued, the writer stream will wait
        // until DuckDB is able to process more data.
        let (batch_tx, batch_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) = mpsc::channel(100);

        // Since the main task/stream can be dropped or fail, we use a oneshot channel to signal that all data is received and we should commit the transaction
        let (notify_commit_transaction, mut on_commit_transaction) =
            tokio::sync::oneshot::channel();

        let duckdb_write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                let mut db_conn = duckdb
                    .connect_sync()
                    .map_err(to_retriable_data_write_error)?;

                let duckdb_conn =
                    DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

                let tx = duckdb_conn
                    .conn
                    .transaction()
                    .context(super::UnableToBeginTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                let num_rows = match **duckdb.constraints() {
                    [] => try_write_all_no_constraints(duckdb, &tx, batch_rx, overwrite)?,
                    _ => try_write_all_with_constraints(
                        duckdb,
                        &tx,
                        batch_rx,
                        overwrite,
                        on_conflict,
                    )?,
                };

                on_commit_transaction
                    .try_recv()
                    .map_err(to_retriable_data_write_error)?;

                tx.commit()
                    .context(super::UnableToCommitTransactionSnafu)
                    .map_err(to_retriable_data_write_error)?;

                Ok(num_rows)
            });

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            constraints::validate_batch_with_constraints(
                &[batch.clone()],
                self.duckdb.constraints(),
            )
            .await
            .context(super::ConstraintViolationSnafu)
            .map_err(to_datafusion_error)?;

            if let Err(send_error) = batch_tx.send(batch).await {
                match duckdb_write_handle.await {
                    Err(join_error) => {
                        return Err(DataFusionError::Execution(format!(
                            "Error writing to DuckDB: {join_error}"
                        )));
                    }
                    Ok(Err(datafusion_error)) => {
                        return Err(datafusion_error);
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unable to send RecordBatch to DuckDB writer: {send_error}"
                        )))
                    }
                };
            }
        }

        if notify_commit_transaction.send(()).is_err() {
            return Err(DataFusionError::Execution(
                "Unable to send message to commit transaction to DuckDB writer.".to_string(),
            ));
        };

        // Drop the sender to signal the receiver that no more data is coming
        drop(batch_tx);

        match duckdb_write_handle.await {
            Ok(result) => result,
            Err(e) => Err(DataFusionError::Execution(format!(
                "Error writing to DuckDB: {e}"
            ))),
        }
    }
}

impl DuckDBDataSink {
    pub(crate) fn new(
        duckdb: Arc<DuckDB>,
        overwrite: bool,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            duckdb,
            overwrite,
            on_conflict,
            schema,
        }
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

/// If there are constraints on the `DuckDB` table, we need to create an empty copy of the target table, write to that table copy and then depending on
/// if the mode is overwrite or not, insert into the target table or drop the target table and rename the current table.
///
/// See: <https://duckdb.org/docs/sql/indexes#over-eager-unique-constraint-checking>
fn try_write_all_with_constraints(
    duckdb: Arc<DuckDB>,
    tx: &Transaction<'_>,
    mut data_batches: Receiver<RecordBatch>,
    overwrite: bool,
    on_conflict: Option<OnConflict>,
) -> datafusion::common::Result<u64> {
    // We want to clone the current table into our insert table
    let Some(ref orig_table_creator) = duckdb.table_creator else {
        return Err(DataFusionError::Execution(
            "Expected table with constraints to have a table creator".to_string(),
        ));
    };

    let mut num_rows = 0;

    let mut insert_table = orig_table_creator
        .create_empty_clone(tx)
        .map_err(to_datafusion_error)?;

    let Some(insert_table_creator) = insert_table.table_creator.take() else {
        unreachable!()
    };

    while let Some(batch) = data_batches.blocking_recv() {
        num_rows += u64::try_from(batch.num_rows()).map_err(|e| {
            DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
        })?;

        tracing::debug!("Inserting {} rows into cloned table.", batch.num_rows());
        insert_table
            .insert_batch_no_constraints(tx, &batch)
            .map_err(to_datafusion_error)?;
    }

    if overwrite {
        insert_table_creator
            .replace_table(tx, orig_table_creator)
            .map_err(to_datafusion_error)?;
    } else {
        insert_table
            .insert_table_into(tx, &duckdb, on_conflict.as_ref())
            .map_err(to_datafusion_error)?;
        insert_table_creator
            .delete_table(tx)
            .map_err(to_datafusion_error)?;
    }

    Ok(num_rows)
}

/// If there are no constraints on the `DuckDB` table, we can do a simple single transaction write.
fn try_write_all_no_constraints(
    duckdb: Arc<DuckDB>,
    tx: &Transaction<'_>,
    mut data_batches: Receiver<RecordBatch>,
    overwrite: bool,
) -> datafusion::common::Result<u64> {
    let mut num_rows = 0;

    if overwrite {
        tracing::debug!("Deleting all data from table.");
        duckdb
            .delete_all_table_data(tx)
            .map_err(to_datafusion_error)?;
    }

    while let Some(batch) = data_batches.blocking_recv() {
        num_rows += u64::try_from(batch.num_rows()).map_err(|e| {
            DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
        })?;

        tracing::debug!("Inserting {} rows into table.", batch.num_rows());

        duckdb
            .insert_batch_no_constraints(tx, &batch)
            .map_err(to_datafusion_error)?;
    }

    Ok(num_rows)
}
