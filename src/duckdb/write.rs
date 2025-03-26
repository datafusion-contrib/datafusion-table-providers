use std::time::{SystemTime, UNIX_EPOCH};
use std::{any::Any, fmt, sync::Arc};

use crate::duckdb::DuckDB;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::util::{
    constraints,
    on_conflict::OnConflict,
    retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error},
};
use arrow::array::RecordBatchReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_schema::ArrowError;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, SchemaExt};
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

use super::creator::{TableCreator, TableDefinition};
use super::to_datafusion_error;

#[derive(Default)]
pub struct DuckDBTableWriterBuilder {
    read_provider: Option<Arc<dyn TableProvider>>,
    pool: Option<Arc<DuckDbConnectionPool>>,
    on_conflict: Option<OnConflict>,
    table_definition: Option<TableDefinition>,
}

impl DuckDBTableWriterBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_read_provider(mut self, read_provider: Arc<dyn TableProvider>) -> Self {
        self.read_provider = Some(read_provider);
        self
    }

    #[must_use]
    pub fn with_pool(mut self, pool: Arc<DuckDbConnectionPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    #[must_use]
    pub fn set_on_conflict(mut self, on_conflict: Option<OnConflict>) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    #[must_use]
    pub fn with_table_definition(mut self, table_definition: TableDefinition) -> Self {
        self.table_definition = Some(table_definition);
        self
    }

    /// Builds a `DuckDBTableWriter` from the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the required fields are missing:
    /// - `read_provider`
    /// - `pool`
    /// - `table_definition`
    pub fn build(self) -> super::Result<DuckDBTableWriter> {
        let Some(read_provider) = self.read_provider else {
            return Err(super::Error::MissingReadProvider);
        };

        let Some(pool) = self.pool else {
            return Err(super::Error::MissingPool);
        };

        let Some(table_definition) = self.table_definition else {
            return Err(super::Error::MissingTableDefinition);
        };

        Ok(DuckDBTableWriter {
            read_provider,
            on_conflict: self.on_conflict,
            table_definition: Arc::new(table_definition),
            pool,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DuckDBTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    on_conflict: Option<OnConflict>,
}

impl DuckDBTableWriter {
    #[must_use]
    pub fn pool(&self) -> Arc<DuckDbConnectionPool> {
        Arc::clone(&self.pool)
    }

    #[must_use]
    pub fn table_definition(&self) -> Arc<TableDefinition> {
        Arc::clone(&self.table_definition)
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
        self.table_definition.constraints()
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
                Arc::clone(&self.pool),
                Arc::clone(&self.table_definition),
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
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
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

    #[allow(clippy::too_many_lines)]
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let pool = Arc::clone(&self.pool);
        let table_definition = Arc::clone(&self.table_definition);
        let overwrite = self.overwrite;
        let on_conflict = self.on_conflict.clone();

        // Limit channel size to a maximum of 100 RecordBatches queued for cases when DuckDB is slower than the writer stream,
        // so that we don't significantly increase memory usage. After the maximum RecordBatches are queued, the writer stream will wait
        // until DuckDB is able to process more data.
        let (batch_tx, batch_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) = mpsc::channel(100);

        // Since the main task/stream can be dropped or fail, we use a oneshot channel to signal that all data is received and we should commit the transaction
        let (notify_commit_transaction, mut on_commit_transaction) =
            tokio::sync::oneshot::channel();

        let schema = data.schema();

        let duckdb_write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                let cloned_pool = Arc::clone(&pool);
                let mut db_conn = pool
                    .connect_sync()
                    .context(super::DbConnectionPoolSnafu)
                    .map_err(to_retriable_data_write_error)?;

                let duckdb_conn =
                    DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

                let tx = duckdb_conn
                    .conn
                    .transaction()
                    .context(super::UnableToBeginTransactionSnafu)
                    .map_err(to_retriable_data_write_error)?;

                let new_table = TableCreator::new(Arc::clone(&table_definition))
                    .map_err(to_retriable_data_write_error)?
                    .with_internal(true);

                new_table
                    .create_table(cloned_pool, &tx)
                    .map_err(to_retriable_data_write_error)?;

                tracing::debug!("Initial load for {}", new_table.table_name());
                let num_rows = write_to_table(&new_table, &tx, schema, batch_rx)
                    .map_err(to_retriable_data_write_error)?;

                let existing_tables = new_table
                    .list_other_internal_tables(&tx)
                    .map_err(to_retriable_data_write_error)?;
                let base_table = new_table
                    .base_table(&tx)
                    .map_err(to_retriable_data_write_error)?;
                let last_table = match (existing_tables.last(), base_table.as_ref()) {
                    (Some(internal_table), Some(base_table)) => {
                        return Err(DataFusionError::Execution(
                            format!("Failed to insert data for DuckDB - both an internal table and definition base table were found.\nManual table migration is required - delete the table '{internal_table}' or '{base_table}' and try again.",
                            internal_table = internal_table.0.table_name(),
                            base_table = base_table.table_name())));
                    }
                    (Some((table, _)), None) | (None, Some(table)) => Some(table),
                    (None, None) => None,
                };

                if matches!(overwrite, InsertOp::Append) && last_table.is_none() {
                    tracing::debug!(
                        "Append was requested, but no existing table was found to append from."
                    );
                }

                if let Some(last_table) = last_table {
                    // compare indexes and primary keys
                    let primary_keys_match = new_table
                        .verify_primary_keys_match(last_table, &tx)
                        .map_err(to_retriable_data_write_error)?;
                    let indexes_match = new_table
                        .verify_indexes_match(last_table, &tx)
                        .map_err(to_retriable_data_write_error)?;
                    let last_table_schema = last_table
                        .current_schema(&tx)
                        .map_err(to_retriable_data_write_error)?;
                    let new_table_schema = new_table
                        .current_schema(&tx)
                        .map_err(to_retriable_data_write_error)?;

                    if !new_table_schema.equivalent_names_and_types(&last_table_schema) {
                        return Err(DataFusionError::Execution(
                            "Schema does not match between the new table and the existing table."
                                .to_string(),
                        ));
                    }

                    if !primary_keys_match {
                        return Err(DataFusionError::Execution(
                            "Primary keys do not match between the new table and the existing table.".to_string(),
                        ));
                    }

                    if !indexes_match {
                        return Err(DataFusionError::Execution(
                            "Indexes do not match between the new table and the existing table."
                                .to_string(),
                        ));
                    }

                    if matches!(overwrite, InsertOp::Append) {
                        tracing::debug!(
                            "Inserting from {} into {}",
                            last_table.table_name(),
                            new_table.table_name()
                        );
                        last_table
                            .insert_into(&new_table, &tx, on_conflict.as_ref())
                            .map_err(to_retriable_data_write_error)?;
                    }
                }

                on_commit_transaction
                    .try_recv()
                    .map_err(to_retriable_data_write_error)?;

                if let Some(base_table) = base_table {
                    base_table
                        .delete_table(&tx)
                        .map_err(to_retriable_data_write_error)?;
                }

                new_table
                    .create_view(&tx)
                    .map_err(to_retriable_data_write_error)?;

                tx.commit()
                    .context(super::UnableToCommitTransactionSnafu)
                    .map_err(to_retriable_data_write_error)?;

                tracing::debug!(
                    "Load for table {table_name} complete, applying constraints and indexes.",
                    table_name = new_table.table_name()
                );

                let tx = duckdb_conn
                    .conn
                    .transaction()
                    .context(super::UnableToBeginTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                for (table, _) in existing_tables {
                    table
                        .delete_table(&tx)
                        .map_err(to_retriable_data_write_error)?;
                }

                // Apply constraints and indexes.
                new_table
                    .create_indexes(&tx)
                    .map_err(to_retriable_data_write_error)?;

                tx.commit()
                    .context(super::UnableToCommitTransactionSnafu)
                    .map_err(to_retriable_data_write_error)?;

                Ok(num_rows)
            });

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            if let Some(constraints) = self.table_definition.constraints() {
                constraints::validate_batch_with_constraints(&[batch.clone()], constraints)
                    .await
                    .context(super::ConstraintViolationSnafu)
                    .map_err(to_datafusion_error)?;
            }

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
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
    ) -> Self {
        Self {
            pool,
            table_definition,
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

#[allow(clippy::doc_markdown)]
/// Writes a stream of ``RecordBatch``es to a DuckDB table.
fn write_to_table(
    table: &TableCreator,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    data_batches: Receiver<RecordBatch>,
) -> datafusion::common::Result<u64> {
    let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchReaderFromStream::new(
        data_batches,
        schema,
    )));

    let current_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context(super::UnableToGetSystemTimeSnafu)
        .map_err(to_datafusion_error)?
        .as_millis();

    let view_name = format!("__scan_{}_{current_ts}", table.table_name());
    tx.register_arrow_scan_view(&view_name, &stream)
        .context(super::UnableToRegisterArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;

    let sql = format!(
        "INSERT INTO {} SELECT * FROM {view_name}",
        table.table_name()
    );
    let rows = tx
        .execute(&sql, [])
        .context(super::UnableToInsertToDuckDBTableSnafu)
        .map_err(to_datafusion_error)?;

    // Drop the view
    let drop_view_sql = format!("DROP VIEW IF EXISTS {view_name}");
    tx.execute(&drop_view_sql, [])
        .context(super::UnableToDropArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;

    Ok(rows as u64)
}

struct RecordBatchReaderFromStream {
    stream: Receiver<RecordBatch>,
    schema: SchemaRef,
}

impl RecordBatchReaderFromStream {
    fn new(stream: Receiver<RecordBatch>, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Iterator for RecordBatchReaderFromStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.blocking_recv().map(Ok)
    }
}

impl RecordBatchReader for RecordBatchReaderFromStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Int64Array, StringArray};
    use datafusion_physical_plan::memory::MemoryStream;

    use super::*;
    use crate::duckdb::creator::tests::{get_basic_table_definition, get_mem_duckdb, init_tracing};

    #[tokio::test]
    async fn test_write_to_table_overwrite() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn = pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");
        let mut internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 1);

        let table_name = internal_tables.pop().expect("should have a table").0;

        let rows = tx
            .query_row(&format!("SELECT COUNT(1) FROM {table_name}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("to get count");
        assert_eq!(rows, 2);

        // expect a view to be created with the table definition name
        let view_rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {view_name}",
                    view_name = table_definition.name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");

        assert_eq!(view_rows, 2);

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_to_table_append() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let cloned_pool = Arc::clone(&pool);
        let mut conn = cloned_pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let table_definition = get_basic_table_definition();

        // make an existing table to append from
        let append_table = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table")
            .with_internal(true);

        append_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        tx.execute(
            &format!(
                "INSERT INTO {table_name} VALUES (3, 'c')",
                table_name = append_table.table_name()
            ),
            [],
        )
        .expect("to insert");

        tx.commit().expect("to commit");

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Append,
            None,
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let mut internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 1);

        let table_name = internal_tables.pop().expect("should have a table").0;

        let rows = tx
            .query_row(&format!("SELECT COUNT(1) FROM {table_name}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("to get count");
        assert_eq!(rows, 3);

        // expect a view to be created with the table definition name
        let view_rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {view_name}",
                    view_name = table_definition.name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");

        assert_eq!(view_rows, 3);

        tx.rollback().expect("to rollback");
    }
}
