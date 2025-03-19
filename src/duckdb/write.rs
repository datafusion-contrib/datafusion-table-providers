use std::{any::Any, fmt, sync::Arc};

use crate::duckdb::DuckDB;
use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use crate::util::{
    constraints,
    on_conflict::OnConflict,
    retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error},
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
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
use duckdb::{Error as DuckDBError, Transaction};
use futures::StreamExt;
use snafu::prelude::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use super::to_datafusion_error;

/// A transaction manager that ensures only a single transaction is active at a time
/// for a given DuckDB connection
pub struct DuckDbTransactionManager {
    /// The DuckDB connection
    conn: DuckDbConnection,
    /// The currently active transaction, if any
    transaction: Option<Transaction<'static>>,
}

impl<'a> DuckDbTransactionManager {
    /// Create a new connection manager with the given connection
    pub fn new(conn: DuckDbConnection) -> Self {
        Self {
            conn,
            transaction: None,
        }
    }

    /// Begin a new transaction if one doesn't already exist
    pub fn begin(&mut self) -> Result<(), DuckDBError> {
        if self.transaction.is_none() {
            let tx = self.conn.conn.transaction()?;
            // SAFETY: The transaction is tied to the lifetime of the connection - because Rust
            // doesn't support self-referential structs, we need to transmute the transaction
            // to a static lifetime. We never give out a reference to the transaction with the static lifetime,
            // so it's safe to transmute.
            self.transaction = Some(unsafe {
                std::mem::transmute::<duckdb::Transaction<'_>, duckdb::Transaction<'static>>(tx)
            });
        }
        Ok(())
    }

    /// Commit the current transaction if one exists and return success/failure
    pub fn commit(&mut self) -> Result<(), DuckDBError> {
        if let Some(tx) = self.transaction.take() {
            tx.commit()?;
        }
        Ok(())
    }

    /// Execute a database operation with the current transaction
    pub fn tx(&'a self) -> Option<&'a Transaction<'a>> {
        self.transaction.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct DuckDBTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
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
        overwrite: InsertOp,
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
    overwrite: InsertOp,
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
                let db_conn = Arc::clone(&duckdb)
                    .connect_sync_direct()
                    .map_err(to_retriable_data_write_error)?;

                let mut tx_manager = DuckDbTransactionManager::new(db_conn);

                tx_manager
                    .begin()
                    .context(super::UnableToBeginTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                let (num_rows, mut tx_manager) = match **duckdb.constraints() {
                    [] => try_write_all_no_constraints(duckdb, tx_manager, batch_rx, overwrite)?,
                    _ => try_write_all_with_constraints(
                        duckdb,
                        tx_manager,
                        batch_rx,
                        overwrite,
                        on_conflict,
                    )?,
                };

                on_commit_transaction
                    .try_recv()
                    .map_err(to_retriable_data_write_error)?;

                tx_manager
                    .commit()
                    .context(super::UnableToCommitTransactionSnafu)
                    .map_err(to_retriable_data_write_error)?;
                drop(tx_manager);

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
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
    ) -> Self {
        Self {
            duckdb,
            overwrite,
            on_conflict,
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
    mut tx_manager: DuckDbTransactionManager,
    mut data_batches: Receiver<RecordBatch>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
) -> datafusion::common::Result<(u64, DuckDbTransactionManager)> {
    // We want to clone the current table into our insert table
    let Some(ref orig_table_creator) = duckdb.table_creator else {
        return Err(DataFusionError::Execution(
            "Expected table with constraints to have a table creator".to_string(),
        ));
    };

    let mut num_rows = 0;

    let mut insert_table = orig_table_creator
        .create_empty_clone(tx_manager.tx().unwrap())
        .map_err(to_datafusion_error)?;

    let Some(insert_table_creator) = insert_table.table_creator.take() else {
        unreachable!()
    };

    // Auto-commit after processing this many rows
    const MAX_ROWS_PER_COMMIT: usize = 10_000_000;
    let mut rows_since_last_commit = 0;

    while let Some(batch) = data_batches.blocking_recv() {
        num_rows += u64::try_from(batch.num_rows()).map_err(|e| {
            DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
        })?;

        rows_since_last_commit += batch.num_rows();

        if rows_since_last_commit > MAX_ROWS_PER_COMMIT {
            tracing::info!("Committing DuckDB transaction after {rows_since_last_commit} rows.",);

            // Commit the current transaction
            tx_manager
                .commit()
                .context(super::UnableToCommitTransactionSnafu)
                .map_err(to_datafusion_error)?;

            // Create a new transaction
            tx_manager
                .begin()
                .context(super::UnableToBeginTransactionSnafu)
                .map_err(to_datafusion_error)?;

            rows_since_last_commit = 0;
        }

        tracing::debug!("Inserting {} rows into cloned table.", batch.num_rows());
        insert_table
            .insert_batch_no_constraints(tx_manager.tx().unwrap(), &batch)
            .map_err(to_datafusion_error)?;
    }

    if matches!(overwrite, InsertOp::Overwrite) {
        insert_table_creator
            .replace_table(tx_manager.tx().unwrap(), orig_table_creator)
            .map_err(to_datafusion_error)?;
    } else {
        insert_table
            .insert_table_into(tx_manager.tx().unwrap(), &duckdb, on_conflict.as_ref())
            .map_err(to_datafusion_error)?;
        insert_table_creator
            .delete_table(tx_manager.tx().unwrap())
            .map_err(to_datafusion_error)?;
    }

    Ok((num_rows, tx_manager))
}

/// Even if there are no constraints on the `DuckDB` table, we use the temp table approach
/// to be consistent with the constrained case and to help with performance.
fn try_write_all_no_constraints(
    duckdb: Arc<DuckDB>,
    mut tx_manager: DuckDbTransactionManager,
    mut data_batches: Receiver<RecordBatch>,
    overwrite: InsertOp,
) -> datafusion::common::Result<(u64, DuckDbTransactionManager)> {
    // We want to clone the current table into our insert table
    let Some(ref orig_table_creator) = duckdb.table_creator else {
        return Err(DataFusionError::Execution(
            "Expected table to have a table creator".to_string(),
        ));
    };

    let mut num_rows = 0;

    let mut insert_table = orig_table_creator
        .create_empty_clone(tx_manager.tx().unwrap())
        .map_err(to_datafusion_error)?;

    let Some(insert_table_creator) = insert_table.table_creator.take() else {
        unreachable!()
    };

    // Auto-commit after processing this many rows
    const MAX_ROWS_PER_COMMIT: usize = 10_000_000;
    let mut rows_since_last_commit = 0;

    while let Some(batch) = data_batches.blocking_recv() {
        num_rows += u64::try_from(batch.num_rows()).map_err(|e| {
            DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
        })?;

        rows_since_last_commit += batch.num_rows();

        if rows_since_last_commit > MAX_ROWS_PER_COMMIT {
            tracing::info!("Committing DuckDB transaction after {rows_since_last_commit} rows.",);

            // Commit the current transaction
            tx_manager
                .commit()
                .context(super::UnableToCommitTransactionSnafu)
                .map_err(to_datafusion_error)?;

            // Create a new transaction
            tx_manager
                .begin()
                .context(super::UnableToBeginTransactionSnafu)
                .map_err(to_datafusion_error)?;

            rows_since_last_commit = 0;
        }

        tracing::debug!("Inserting {} rows into cloned table.", batch.num_rows());
        insert_table
            .insert_batch_no_constraints(tx_manager.tx().unwrap(), &batch)
            .map_err(to_datafusion_error)?;
    }

    if matches!(overwrite, InsertOp::Overwrite) {
        insert_table_creator
            .replace_table(tx_manager.tx().unwrap(), orig_table_creator)
            .map_err(to_datafusion_error)?;
    } else {
        insert_table
            .insert_table_into(tx_manager.tx().unwrap(), &duckdb, None)
            .map_err(to_datafusion_error)?;
        insert_table_creator
            .delete_table(tx_manager.tx().unwrap())
            .map_err(to_datafusion_error)?;
    }

    Ok((num_rows, tx_manager))
}
