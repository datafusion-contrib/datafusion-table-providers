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
use duckdb::{Appender, Transaction};
use futures::StreamExt;
use snafu::prelude::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use super::to_datafusion_error;

/// An append manager that ensures only a single appender and transaction is active at a time
/// for a given DuckDB connection
pub struct DuckDbAppendManager {
    /// The DuckDB connection
    conn: DuckDbConnection,
    /// The currently active transaction, if any
    transaction: Option<Transaction<'static>>,
    /// The currently active appender, if any
    appender: Option<Appender<'static>>,
}

impl DuckDbAppendManager {
    /// Create a new connection manager with the given connection
    pub fn new(conn: DuckDbConnection) -> Self {
        Self {
            conn,
            transaction: None,
            appender: None,
        }
    }

    /// Begin a new transaction if one doesn't already exist
    pub fn begin_transaction(&mut self) -> Result<(), super::Error> {
        if self.transaction.is_some() {
            return Err(super::Error::TransactionAlreadyInProgress);
        }

        let tx = self
            .conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;
        // SAFETY: The transaction is tied to the lifetime of the connection - because Rust
        // doesn't support self-referential structs, we need to transmute the transaction
        // to a static lifetime. We never give out a reference to the transaction with the static lifetime,
        // so it's safe to transmute.
        let static_tx = unsafe {
            std::mem::transmute::<duckdb::Transaction<'_>, duckdb::Transaction<'static>>(tx)
        };

        self.transaction = Some(static_tx);
        Ok(())
    }

    pub fn begin_appender(&mut self, table_name: &str) -> Result<(), super::Error> {
        let Some(transaction) = self.transaction.take() else {
            return Err(super::Error::MissingTransaction);
        };

        let appender = transaction
            .appender(table_name)
            .context(super::UnableToGetAppenderToDuckDBTableSnafu)?;
        // SAFETY: The appender is tied to the lifetime of the transaction.
        self.appender = Some(unsafe {
            std::mem::transmute::<duckdb::Appender<'_>, duckdb::Appender<'static>>(appender)
        });
        self.transaction = Some(transaction);

        Ok(())
    }

    /// Commit the current transaction
    pub fn commit(&mut self) -> Result<(), super::Error> {
        if let Some(transaction) = self.transaction.take() {
            transaction
                .commit()
                .context(super::UnableToCommitTransactionSnafu)?;
        }
        self.appender = None;
        Ok(())
    }

    /// Execute a database operation with the current transaction
    pub fn tx(&self) -> Result<&Transaction<'_>, super::Error> {
        self.transaction
            .as_ref()
            .context(super::MissingTransactionSnafu)
    }

    pub fn appender(&self) -> Result<&Appender<'_>, super::Error> {
        self.appender.as_ref().context(super::MissingAppenderSnafu)
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn appender_mut<'a>(&'a mut self) -> Result<&'a mut Appender<'a>, super::Error> {
        Ok(unsafe {
            std::mem::transmute::<&mut duckdb::Appender<'_>, &mut duckdb::Appender<'a>>(
                self.appender
                    .as_mut()
                    .context(super::MissingAppenderSnafu)?,
            )
        })
    }

    pub fn appender_flush(&mut self) -> Result<(), super::Error> {
        if self.appender.is_none() {
            return Ok(());
        }

        self.appender_mut()?
            .flush()
            .context(super::UnableToFlushAppenderSnafu)
    }
}

#[derive(Debug, Clone)]
pub struct DuckDBTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    duckdb: Arc<DuckDB>,
    on_conflict: Option<OnConflict>,
    write_mode: DuckDBWriteMode,
}

impl DuckDBTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        duckdb: Arc<DuckDB>,
        on_conflict: Option<OnConflict>,
        write_mode: DuckDBWriteMode,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            duckdb,
            on_conflict,
            write_mode,
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
                self.write_mode,
            )),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DuckDBWriteMode {
    Standard,
    BatchedCommit,
}

impl TryFrom<&str> for DuckDBWriteMode {
    type Error = super::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "standard" => Ok(Self::Standard),
            "batch" => Ok(Self::BatchedCommit),
            _ => Err(super::Error::InvalidWriteMode {
                write_mode: value.to_string(),
            }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DuckDBDataSink {
    duckdb: Arc<DuckDB>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    write_mode: DuckDBWriteMode,
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

        let write_mode = self.write_mode;
        let duckdb_write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                let db_conn = Arc::clone(&duckdb)
                    .connect_sync_direct()
                    .map_err(to_retriable_data_write_error)?;

                let append_manager = DuckDbAppendManager::new(db_conn);

                let (num_rows, mut append_manager) =
                    match (duckdb.constraints().is_empty(), write_mode) {
                        // the only scenario where we can write directly to the table
                        (true, DuckDBWriteMode::Standard) => try_write_all_no_constraints(
                            &duckdb,
                            append_manager,
                            batch_rx,
                            overwrite,
                        )?,
                        _ =>
                        // all other scenarios where we need to create a temp table
                        {
                            try_write_all_with_temp_table(
                                &duckdb,
                                append_manager,
                                batch_rx,
                                overwrite,
                                on_conflict.as_ref(),
                                write_mode,
                            )?
                        }
                    };

                on_commit_transaction
                    .try_recv()
                    .map_err(to_retriable_data_write_error)?;

                append_manager
                    .commit()
                    .map_err(to_retriable_data_write_error)?;
                drop(append_manager);

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
        write_mode: DuckDBWriteMode,
    ) -> Self {
        Self {
            duckdb,
            overwrite,
            on_conflict,
            write_mode,
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
#[allow(dead_code)]
fn try_write_all_with_temp_table(
    duckdb: &Arc<DuckDB>,
    mut append_manager: DuckDbAppendManager,
    mut data_batches: Receiver<RecordBatch>,
    overwrite: InsertOp,
    on_conflict: Option<&OnConflict>,
    write_mode: DuckDBWriteMode,
) -> datafusion::common::Result<(u64, DuckDbAppendManager)> {
    // We want to clone the current table into our insert table
    let Some(ref orig_table_creator) = duckdb.table_creator else {
        return Err(DataFusionError::Execution(
            "Expected table with constraints to have a table creator".to_string(),
        ));
    };

    let mut num_rows = 0;

    append_manager
        .begin_transaction()
        .map_err(to_datafusion_error)?;

    let (insert_table, insert_table_constraints) = orig_table_creator
        .create_empty_clone(append_manager.tx().map_err(to_datafusion_error)?, true)
        .map_err(to_datafusion_error)?;

    append_manager
        .begin_appender(insert_table.table_name())
        .map_err(to_datafusion_error)?;

    // We don't need to apply constraints to the temporary table
    insert_table_constraints.mark_as_ignored();

    let mut insert_table = Arc::try_unwrap(insert_table)
        .ok()
        .context(super::UnexpectedReferenceCountOnDuckDBTableSnafu)
        .map_err(to_datafusion_error)?;

    let Some(insert_table_creator) = insert_table.table_creator.take() else {
        unreachable!()
    };

    // Auto-commit after processing this many rows
    const MAX_ROWS_PER_COMMIT: usize = 100_000_000;
    let mut rows_since_last_commit = 0;

    while let Some(batch) = data_batches.blocking_recv() {
        num_rows += u64::try_from(batch.num_rows()).map_err(|e| {
            DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
        })?;

        insert_table
            .insert_batch_no_constraints(
                append_manager.appender_mut().map_err(to_datafusion_error)?,
                &batch,
            )
            .map_err(to_datafusion_error)?;
        rows_since_last_commit += batch.num_rows();

        if matches!(write_mode, DuckDBWriteMode::BatchedCommit)
            && rows_since_last_commit > MAX_ROWS_PER_COMMIT
        {
            tracing::info!("Committing DuckDB transaction after {rows_since_last_commit} rows.",);

            append_manager
                .appender_flush()
                .map_err(to_datafusion_error)?;
            append_manager.commit().map_err(to_datafusion_error)?;
            append_manager
                .begin_transaction()
                .map_err(to_datafusion_error)?;
            append_manager
                .begin_appender(insert_table.table_name())
                .map_err(to_datafusion_error)?;

            rows_since_last_commit = 0;
        }
    }

    if rows_since_last_commit > 0 {
        tracing::debug!("Flushing appender and committing transaction.");
        append_manager
            .appender_flush()
            .map_err(to_datafusion_error)?;
        append_manager.commit().map_err(to_datafusion_error)?;
        append_manager
            .begin_transaction()
            .map_err(to_datafusion_error)?;
    }

    let (non_temp_table, non_temp_table_constraints) = orig_table_creator
        .create_empty_clone(append_manager.tx().map_err(to_datafusion_error)?, false)
        .map_err(to_datafusion_error)?;

    // We don't need to apply constraints to the intermediate table
    non_temp_table_constraints.mark_as_ignored();
    let mut non_temp_table = Arc::try_unwrap(non_temp_table)
        .ok()
        .context(super::UnexpectedReferenceCountOnDuckDBTableSnafu)
        .map_err(to_datafusion_error)?;

    let Some(non_temp_table_creator) = non_temp_table.table_creator.take() else {
        unreachable!()
    };

    insert_table
        .insert_table_into(
            append_manager.tx().map_err(to_datafusion_error)?,
            &non_temp_table,
            on_conflict,
        )
        .map_err(to_datafusion_error)?;
    insert_table_creator
        .delete_table(append_manager.tx().map_err(to_datafusion_error)?)
        .map_err(to_datafusion_error)?;

    tracing::debug!("Inserting into target table.");
    if matches!(overwrite, InsertOp::Overwrite) {
        non_temp_table_creator
            .replace_table(
                append_manager.tx().map_err(to_datafusion_error)?,
                orig_table_creator,
            )
            .map_err(to_datafusion_error)?;
    } else {
        non_temp_table
            .insert_table_into(
                append_manager.tx().map_err(to_datafusion_error)?,
                duckdb,
                on_conflict,
            )
            .map_err(to_datafusion_error)?;
        non_temp_table_creator
            .delete_table(append_manager.tx().map_err(to_datafusion_error)?)
            .map_err(to_datafusion_error)?;
    }

    Ok((num_rows, append_manager))
}

/// If there are no constraints on the `DuckDB` table, we can do a simple single transaction write.
fn try_write_all_no_constraints(
    duckdb: &Arc<DuckDB>,
    mut append_manager: DuckDbAppendManager,
    mut data_batches: Receiver<RecordBatch>,
    overwrite: InsertOp,
) -> datafusion::common::Result<(u64, DuckDbAppendManager)> {
    let mut num_rows = 0;

    append_manager
        .begin_transaction()
        .map_err(to_datafusion_error)?;
    append_manager
        .begin_appender(duckdb.table_name())
        .map_err(to_datafusion_error)?;

    if matches!(overwrite, InsertOp::Overwrite) {
        tracing::debug!("Deleting all data from table.");
        duckdb
            .delete_all_table_data(append_manager.tx().map_err(to_datafusion_error)?)
            .map_err(to_datafusion_error)?;
    }

    while let Some(batch) = data_batches.blocking_recv() {
        num_rows += u64::try_from(batch.num_rows()).map_err(|e| {
            DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
        })?;

        tracing::debug!("Inserting {} rows into table.", batch.num_rows());

        duckdb
            .insert_batch_no_constraints(
                append_manager.appender_mut().map_err(to_datafusion_error)?,
                &batch,
            )
            .map_err(to_datafusion_error)?;
    }

    append_manager
        .appender_flush()
        .map_err(to_datafusion_error)?;

    Ok((num_rows, append_manager))
}
