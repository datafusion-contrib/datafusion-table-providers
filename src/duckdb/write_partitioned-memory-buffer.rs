use core::num;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec;
use std::{any::Any, fmt, sync::Arc};

use crate::duckdb::DuckDB;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::util::constraints::UpsertOptions;
use crate::util::{
    on_conflict::OnConflict,
    retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error},
};
use arrow::array::RecordBatchReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow::compute::concat_batches;
use arrow_schema::ArrowError;
use async_trait::async_trait;
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType},
};
use duckdb::{Appender, Transaction};
use futures::StreamExt;
use futures::stream::TryStreamExt;
use snafu::prelude::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use futures::future::FutureExt;

use arrow::ipc::writer::{FileWriter as IpcWriter};
use tokio_stream::wrappers::ReceiverStream;

use super::creator::{TableDefinition, TableManager, ViewCreator};
use super::{to_datafusion_error, RelationName};

/// Helper struct to buffer batches per partition and write them to parquet files when thresholds are met
struct ParquetFilePartitionBuffer {
    sender: Sender<(String, PathBuf)>,
    schema: SchemaRef,
    partition_sinks: HashMap<String, PartSink>,
    row_counts: HashMap<String, usize>,
    rows_per_partition_threshold: usize,
    temp_dir: PathBuf,
}

impl ParquetFilePartitionBuffer {
    fn new(sender: Sender<(String, PathBuf)>, schema: SchemaRef, rows_per_partition_threshold: usize) -> datafusion::common::Result<Self> {
        let temp_dir = std::path::PathBuf::from("/Volumes/envoy_ssd/xdr/duckdb_tmp");
        
        
        // std::env::temp_dir().join(format!("spice_partitioned_write_{}", 
        //     SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()));


        std::fs::create_dir_all(&temp_dir)?;
        
        Ok(Self {
            sender,
            schema,
            partition_sinks: HashMap::new(),
            row_counts: HashMap::new(),
            rows_per_partition_threshold,
            temp_dir,
        })
    }

    /// Add a batch to the specified partition buffer. If threshold is reached, flush that partition.
    async fn process_batch(&mut self, partition_id: String, batch: RecordBatch) -> datafusion::common::Result<()> {
        let batch_row_count = batch.num_rows();
        
        // Get or create partition sink
        if !self.partition_sinks.contains_key(&partition_id) {
            let sink = PartSink::new(&self.temp_dir, &partition_id, &self.schema)?;
            self.partition_sinks.insert(partition_id.clone(), sink);
        }
        
        // Write batch to parquet file
        let sink = self.partition_sinks.get_mut(&partition_id).unwrap();
        sink.write_batch(&batch, &self.schema)?;
        
        // Update row count for this partition
        let current_rows = self.row_counts.entry(partition_id.clone())
            .or_insert(0);
        *current_rows += batch_row_count;
        
        // Check if we should flush this partition's buffer
        if *current_rows >= self.rows_per_partition_threshold {
            // Instead of removing the sink entirely, flush current file and get a new one
            let sink = self.partition_sinks.get_mut(&partition_id).unwrap();
            let file_path = sink.flush_current_file(&self.schema)?;
            
            tracing::debug!("Sending parquet file for partition {} with {} rows: {:?}",
                          partition_id, *current_rows, file_path);

            self.sender.send((partition_id.clone(), file_path)).await
                .map_err(|e| DataFusionError::Execution(format!(
                    "Unable to send parquet file path for partition {} to DuckDB writer: {}", 
                    partition_id, e
                )))?;
            
            // Reset row count for this partition since we flushed
            self.row_counts.insert(partition_id, 0);
        }
        
        Ok(())
    }

    /// Flush all buffered data for a specific partition
    async fn flush_partition(&mut self, partition_id: &str) -> datafusion::common::Result<()> {
        if let Some(sink) = self.partition_sinks.remove(partition_id) {
            let file_path = sink.close()?;
            
            let num_rows = self.row_counts.remove(partition_id).unwrap_or(0);
            
            tracing::debug!("Sending parquet file for partition {} with {} rows: {:?}",
                          partition_id, num_rows, file_path);

            self.sender.send((partition_id.to_string(), file_path)).await
                .map_err(|e| DataFusionError::Execution(format!(
                    "Unable to send parquet file path for partition {} to DuckDB writer: {}", 
                    partition_id, e
                )))?;
        }
        Ok(())
    }

    /// Flush all remaining buffered data for all partitions
    async fn flush_all(&mut self) -> datafusion::common::Result<()> {
        let partition_ids: Vec<String> = self.partition_sinks.keys().cloned().collect();
        for partition_id in partition_ids {
            self.flush_partition(&partition_id).await?;
        }
        Ok(())
    }
}

impl Drop for ParquetFilePartitionBuffer {
    fn drop(&mut self) {
        // Clean up temporary directory
        // if self.temp_dir.exists() {
        //     let _ = std::fs::remove_dir_all(&self.temp_dir);
        // }
    }
}

struct PartitionBuffer {
    sender: Sender<(String, Vec<RecordBatch>)>,
    schema: SchemaRef,
    buffers: HashMap<String, Vec<RecordBatch>>,
    row_counts: HashMap<String, usize>,
    rows_per_partition_threshold: usize,
}

impl PartitionBuffer {
    fn new(sender: Sender<(String, Vec<RecordBatch>)>, schema: SchemaRef, rows_per_partition_threshold: usize) -> Self {
        Self {
            sender,
            schema,
            buffers: HashMap::new(),
            row_counts: HashMap::new(),
            rows_per_partition_threshold,
        }
    }

    async fn send_partition_batches(&self, partition_id: String, batches: Vec<RecordBatch>) -> datafusion::common::Result<()> {
        self.sender.send((partition_id, batches)).await
            .map_err(|e| DataFusionError::Execution(format!(
                "Unable to send RecordBatch to DuckDB writer: {}", e
            )))
    }

    /// Add a batch to the specified partition buffer. If threshold is reached, flush that partition.
    async fn process_batch(&mut self, partition_id: String, batch: RecordBatch) -> datafusion::common::Result<()> {
        let batch_row_count = batch.num_rows();
        
        // Add batch to partition buffer
        self.buffers.entry(partition_id.clone())
            .or_insert_with(Vec::new)
            .push(batch);
        
        // Update row count for this partition
        let current_rows = self.row_counts.entry(partition_id.clone())
            .or_insert(0);
        *current_rows += batch_row_count;
        
        // Check if we should flush this partition's buffer
        if *current_rows >= self.rows_per_partition_threshold {
            self.flush_partition(&partition_id).await?;
        }
        
        Ok(())
    }

    /// Flush all buffered data for a specific partition
    async fn flush_partition(&mut self, partition_id: &str) -> datafusion::common::Result<()> {
        if let Some(partition_batches) = self.buffers.remove(partition_id) {
            if !partition_batches.is_empty() {
                // let combined_batch = concat_batches(&self.schema, &partition_batches)
                //     .map_err(|e| DataFusionError::Execution(format!("Failed to combine batches for partition {}: {e}", partition_id)))?;

                let num_rows: usize = partition_batches.iter().map(|b| b.num_rows()).sum();

                // tracing::debug!("Sending buffered batch for partition {} with {} rows",
                //               partition_id, num_rows);

                self.sender.send((partition_id.to_string(), partition_batches)).await
                    .map_err(|e| DataFusionError::Execution(format!(
                        "Unable to send combined RecordBatch for partition {} to DuckDB writer: {}", 
                        partition_id, e
                    )))?;
            }
            self.row_counts.remove(partition_id);
        }
        Ok(())
    }

    /// Flush all remaining buffered data for all partitions
    async fn flush_all(&mut self) -> datafusion::common::Result<()> {
        let partition_ids: Vec<String> = self.buffers.keys().cloned().collect();
        for partition_id in partition_ids {
            self.flush_partition(&partition_id).await?;
        }
        Ok(())
    }
}

pub trait BatchPartitioner: Send + Sync {
    /// Partition a RecordBatch into multiple batches based on partition keys
    /// Returns a HashMap where the key is the partition identifier and the value is the RecordBatch for that partition
    fn partition_batch(&self, batch: &RecordBatch) -> Result<HashMap<String, RecordBatch>, DataFusionError>;
}


#[derive(Clone)]
pub struct DuckDBPartitionedDataSink {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
    partitioner: Arc<dyn BatchPartitioner>,
}

#[async_trait]
impl DataSink for DuckDBPartitionedDataSink {
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
        let pool = Arc::clone(&self.pool);
        let table_definition = Arc::clone(&self.table_definition);
        let overwrite = self.overwrite;
        let on_conflict = self.on_conflict.clone();

        // Changed channel type to send file paths instead of batch vectors
        let (batch_tx, batch_rx): (Sender<(String, Vec<RecordBatch>)>, Receiver<(String, Vec<RecordBatch>)>) = mpsc::channel(5);

        // Since the main task/stream can be dropped or fail, we use a oneshot channel to signal that all data is received and we should commit the transaction
        let (notify_commit_transaction, on_commit_transaction) = tokio::sync::oneshot::channel();

        let schema = data.schema();
        let schema_for_blocking = Arc::clone(&schema);

        let duckdb_write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                let num_rows = match overwrite {
                    InsertOp::Overwrite => insert_overwrite(
                        pool,
                        &table_definition,
                        batch_rx,
                        on_conflict.as_ref(),
                        on_commit_transaction,
                        schema_for_blocking,
                    )?,
                    InsertOp::Append | InsertOp::Replace => 0,
                };

                Ok(num_rows)
            });

        // Buffer 128k rows per partition before sending to reduce write frequency
        //const ROWS_PER_PARTITION_BUFFER: usize = 128_000;
        // const ROWS_PER_PARTITION_BUFFER: usize = 5 * 122_880; // `137mb` per partition buffer

        const ROWS_PER_PARTITION_BUFFER: usize = 5 * 5 * 122_880;
        
        //let mut partition_buffer = ParquetFilePartitionBuffer::new(batch_tx, schema.clone(), ROWS_PER_PARTITION_BUFFER)?;

        let mut partition_buffer = PartitionBuffer::new(batch_tx, schema.clone(), ROWS_PER_PARTITION_BUFFER);

        let partitioner = Arc::clone(&self.partitioner);

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            let millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let partition_id = (millis as usize) % 4;
            let batches = vec![(partition_id.to_string(), batch)];

            for (partition_id, batch) in batches {
                if let Err(send_error) = partition_buffer.process_batch(partition_id, batch).await {
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
        }

        if let Err(send_error) = partition_buffer.flush_all().await {
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

        if notify_commit_transaction.send(()).is_err() {
            return Err(DataFusionError::Execution(
                "Unable to send message to commit transaction to DuckDB writer.".to_string(),
            ));
        };

        // Drop the sender to signal the receiver that no more data is coming
        drop(partition_buffer);
        //drop(batch_tx);

        match duckdb_write_handle.await {
            Ok(result) => result,
            Err(e) => Err(DataFusionError::Execution(format!(
                "Error writing to DuckDB: {e}"
            ))),
        }
    }
}

impl DuckDBPartitionedDataSink {
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
        partitioner: Arc<dyn BatchPartitioner>,
    ) -> Self {
        Self {
            pool,
            table_definition,
            overwrite,
            on_conflict,
            schema,
            partitioner,
        }
    }
}

impl std::fmt::Debug for DuckDBPartitionedDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBPartitionedDataSink")
    }
}

impl DisplayAs for DuckDBPartitionedDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBPartitionedDataSink")
    }
}

#[allow(clippy::too_many_lines)]
fn insert_overwrite(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<(String, Vec<RecordBatch>)>,
    on_conflict: Option<&OnConflict>,
    mut on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: SchemaRef,
) -> datafusion::common::Result<u64> {
    let cloned_pool = Arc::clone(&pool);
    let cloned_pool2 = Arc::clone(&pool);
    let mut db_conn = pool
        .connect_sync()
        .context(super::DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;

    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(super::UnableToBeginTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    let new_table = TableManager::new(Arc::clone(table_definition))
        .with_internal(true)
        .map_err(to_retriable_data_write_error)?;

    new_table
        .create_table(cloned_pool, &tx)
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

    if let Some(last_table) = last_table {
        let should_have_indexes = !last_table.indexes_vec().is_empty();
        let has_indexes = !last_table
            .current_indexes(&tx)
            .map_err(to_retriable_data_write_error)?
            .is_empty();
        let is_empty_table = last_table
            .get_row_count(&tx)
            .map_err(to_retriable_data_write_error)?
            == 0;
        let should_apply_indexes = should_have_indexes && !has_indexes && is_empty_table;

        let _last_table_schema = last_table
            .current_schema(&tx)
            .map_err(to_retriable_data_write_error)?;
        let _new_table_schema = new_table
            .current_schema(&tx)
            .map_err(to_retriable_data_write_error)?;

        if !should_apply_indexes {
            // compare indexes and primary keys
            let primary_keys_match = new_table
                .verify_primary_keys_match(last_table, &tx)
                .map_err(to_retriable_data_write_error)?;
            let indexes_match = new_table
                .verify_indexes_match(last_table, &tx)
                .map_err(to_retriable_data_write_error)?;

            if !primary_keys_match {
                return Err(DataFusionError::Execution(
                    "Primary keys do not match between the new table and the existing table.\nEnsure primary key configuration is the same as the existing table, or manually migrate the table."
                        .to_string(),
                ));
            }

            if !indexes_match {
                return Err(DataFusionError::Execution(
                    "Indexes do not match between the new table and the existing table.\nEnsure index configuration is the same as the existing table, or manually migrate the table.".to_string(),
                ));
            }
        }
    }

    tx.execute("SET allocator_background_threads = true", []).unwrap();
    // tx.execute("SET allocator_bulk_deallocation_flush_threshold='64MIB'", []).unwrap();
    // tx.execute("SET allocator_flush_threshold='64MIB'", []).unwrap();
    // tx.execute("SET write_buffer_row_group_count = 1", []).unwrap();
    tracing::debug!("Initial load for {}", new_table.table_name());
    let num_rows = write_to_partitioned_tables(&table_definition, &tx, schema, batch_rx, on_conflict, cloned_pool2)
        .map_err(to_retriable_data_write_error)?;

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
}

/// Writes parquet files to partitioned tables using file paths
fn write_parquet_files_to_partitioned_tables(
    table_definition: &Arc<TableDefinition>,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    mut data_files: Receiver<(String, PathBuf)>,
    on_conflict: Option<&OnConflict>,
    pool: Arc<DuckDbConnectionPool>,
) -> datafusion::common::Result<u64> {
    
    let mut total_rows = 0u64;
    let mut batch_count = 0usize;
    let start_main = SystemTime::now();
    
    // Track which partitions have already been created to avoid duplicate table creation
    let mut created_partitions: HashMap<String, Arc<TableManager>> = HashMap::new();

    tracing::info!("Starting parquet-based partitioned table writes for {}", table_definition.name());

    // Process parquet files sequentially
    while let Some((partition, file_path)) = data_files.blocking_recv() {
        let start = SystemTime::now();

        // Check if partition table already exists or create it
        let partition_table = if let Some(existing_table) = created_partitions.get(&partition) {
            Arc::clone(existing_table)
        } else {
            // Create new partition table
            let partition_table_name = format!("{}_partition_{}", table_definition.name(), partition);
            let partition_table_def = Arc::new(TableDefinition::new(
                RelationName::new(partition_table_name.clone()),
                schema.clone(),
            ));
            
            let partition_table = Arc::new(TableManager::new(partition_table_def)
                .with_internal(false)
                .map_err(to_datafusion_error)?);

            // Create the table in DuckDB
            partition_table.create_table(pool.clone(), tx)
                .map_err(to_datafusion_error)?;

            tracing::debug!("Created partition table {} for dynamic partitioning", partition_table.table_name());
            
            // Cache the created table
            created_partitions.insert(partition.clone(), Arc::clone(&partition_table));
            partition_table
        };

        // Insert data from parquet file directly into partition table
        let sql = format!(
            "INSERT INTO {} SELECT * FROM read_parquet('{}')",
            partition_table.table_name(),
            escape_single_quotes(file_path.to_string_lossy().as_ref())
        );
        
        let rows_written = tx.execute(&sql, [])
            .map_err(|e| DataFusionError::Execution(format!("Failed to insert from parquet: {}", e)))? as u64;

        total_rows += rows_written;
        batch_count += 1;

        // Clean up the temporary parquet file
        // if let Err(e) = std::fs::remove_file(&file_path) {
        //     tracing::warn!("Failed to clean up temporary parquet file {:?}: {}", file_path, e);
        // }

        let elapsed = start.elapsed().unwrap();
        let secs = elapsed.as_secs_f64();
        let rps = if secs > 0.0 { (rows_written as f64) / secs } else { rows_written as f64 };
        println!("Processed {rows_written} rows from parquet file in {elapsed:?} ({rps:.2} rows/s)");
    }

    let total_elapsed = start_main.elapsed().unwrap();

    tracing::info!("Completed parquet-based partitioned writes; created {} partition tables, processed {} files, total rows: {}, elapsed time: {:?}", 
                  created_partitions.len(), batch_count, total_rows, total_elapsed);

    Ok(total_rows)
}

#[allow(clippy::doc_markdown)]
/// Writes a stream of ``RecordBatch``es to a DuckDB table.
fn write_to_table(
    table: &TableManager,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    data_batches: Receiver<RecordBatch>,
    on_conflict: Option<&OnConflict>,
) -> datafusion::common::Result<u64> {
    let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchReaderFromStream::new(
        data_batches,
        schema,
    )));

    //partition_by_expressions

    let current_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context(super::UnableToGetSystemTimeSnafu)
        .map_err(to_datafusion_error)?
        .as_millis();

    let view_name = format!("__scan_{}_{current_ts}", table.table_name());
    tx.register_arrow_scan_view(&view_name, &stream)
        .context(super::UnableToRegisterArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;

    let view = ViewCreator::from_name(RelationName::new(view_name));
    let rows = view
        .insert_into(table, tx, on_conflict)
        .map_err(to_datafusion_error)?;
    // view.drop(tx).map_err(to_datafusion_error)?;

    Ok(rows as u64)
}

/// Writes batches sequentially to partitioned tables using bucket(100, account_id) partitioning
/// Each batch is split by account_id bucket and written to the appropriate partition table
fn write_to_partitioned_tables(
    table_definition: &Arc<TableDefinition>,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    mut data_batches: Receiver<(String, Vec<RecordBatch>)>,
    on_conflict: Option<&OnConflict>,
    pool: Arc<DuckDbConnectionPool>,
) -> datafusion::common::Result<u64> {
    
    let mut total_rows = 0u64;
    let mut batch_count = 0usize;

    let start_main = SystemTime::now();
    
    // Track which partitions have already been created to avoid duplicate table creation
    let mut created_partitions: HashMap<String, Arc<TableManager>> = HashMap::new();

    // Enable DuckDB profiling for detailed query analysis
    // tx.execute("PRAGMA enable_profiling", []).unwrap();
    // tx.execute("PRAGMA profiling_output='profiling_output.json'", []).unwrap();

    tracing::info!("Starting bucket-partitioned table writes for {}", table_definition.name());

    // Process batches sequentially
    while let Some((partition, batch)) = data_batches.blocking_recv() {


        // let num_rows_in_batches  = batch.iter().map(|b| b.num_rows()).sum::<usize>();
        let start = SystemTime::now();
        let batch_size_mb = batch.iter().map(|b| b.get_array_memory_size()).sum::<usize>() / (1024 * 1024);

        // Check if partition table already exists or create it
        let partition_table = if let Some(existing_table) = created_partitions.get(&partition) {
            Arc::clone(existing_table)
        } else {
            // Create new partition table
            let partition_table_name = format!("{}_partition_{}", table_definition.name(), partition);
            let partition_table_def = Arc::new(TableDefinition::new(
                RelationName::new(partition_table_name.clone()),
                schema.clone(),
            ));
            
            let partition_table = Arc::new(TableManager::new(partition_table_def)
                .with_internal(false)
                .map_err(to_datafusion_error)?);

            // Create the table in DuckDB
            partition_table.create_table(pool.clone(), tx)
                .map_err(to_datafusion_error)?;

            tracing::debug!("Created partition table {} for dynamic partitioning", partition_table.table_name());
            
            // Cache the created table
            created_partitions.insert(partition.clone(), Arc::clone(&partition_table));
            partition_table
        };

        // Write this partition's data
        let rows_written = write_single_batch_to_table(
            &partition_table,
            tx,
            schema.clone(),
            batch,
            on_conflict,
            batch_count,
        )?;

        total_rows += rows_written;
        batch_count += 1;

        let elapsed = start.elapsed().unwrap();
        let secs = elapsed.as_secs_f64();
        let rps = if secs > 0.0 { (rows_written as f64) / secs } else { rows_written as f64 };
        println!("Processed {rows_written} rows in {elapsed:?} ({rps:.2} rows/s, memory: {batch_size_mb:.2} MB)");
    }

    let total_elapsed = start_main.elapsed().unwrap();

    tracing::info!("Completed partitioned writes; created {} partition tables, processed {} batches, total rows: {}, elapsed time: {:?}", 
                  created_partitions.len(), batch_count, total_rows, total_elapsed);

    Ok(total_rows)
}

fn write_to_partitioned_tables_appender(
    table_definition: &Arc<TableDefinition>,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    mut data_batches: Receiver<(String, Vec<RecordBatch>)>,
    on_conflict: Option<&OnConflict>,
    pool: Arc<DuckDbConnectionPool>,
) -> datafusion::common::Result<u64> {
    
    let mut total_rows = 0u64;
    let mut batch_count = 0usize;
    
    // Track which partitions have already been created to avoid duplicate table creation
    let mut created_tables: HashMap<String, Arc<TableManager>> = HashMap::new();
    let mut partition_appenders: HashMap<String, duckdb::Appender> = HashMap::new();

    // Enable DuckDB profiling for detailed query analysis
    // tx.execute("PRAGMA enable_profiling", []).unwrap();
    // tx.execute("PRAGMA profiling_output='profiling_output.json'", []).unwrap();

    tracing::info!("Starting bucket-partitioned table writes for {}", table_definition.name());

    // Process batches sequentially
    while let Some((partition, batches)) = data_batches.blocking_recv() {

        // Check if partition table already exists or create it
        if !created_tables.contains_key(&partition) {
            // Create new partition table
            let partition_table_name = format!("{}_partition_{}", table_definition.name(), partition);
            let partition_table_def = Arc::new(TableDefinition::new(
                RelationName::new(partition_table_name.clone()),
                schema.clone(),
            ));
            
            let partition_table = Arc::new(TableManager::new(partition_table_def)
                .with_internal(false)
                .map_err(to_datafusion_error)?);

            // Create the table in DuckDB
            partition_table.create_table(pool.clone(), tx)
                .map_err(to_datafusion_error)?;

            tracing::debug!("Created partition table {} for dynamic partitioning", partition_table.table_name());
            
            // Cache the created table
            created_tables.insert(partition.clone(), Arc::clone(&partition_table));
        };

        // Get or create appender for this partition
        if !partition_appenders.contains_key(&partition) {
            let appender = tx.appender(format!("{}_partition_{}", table_definition.name(), partition).as_str())
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("Failed to create appender for partition {}: {}", partition, e)
                ))?;
            partition_appenders.insert(partition.clone(), appender);
        }

        let appender = partition_appenders.get_mut(&partition).unwrap();

        // Append all batches for this partition
        for batch in batches {
            let rows_in_batch = batch.num_rows();
            appender.append_record_batch(batch)
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("Failed to append batch to partition {}: {}", partition, e)
                ))?;
            total_rows += rows_in_batch as u64;
            batch_count += 1;
        }
    }

    // Flush all appenders at the end
    for (partition, mut appender) in partition_appenders {
        appender.flush()
            .map_err(|e| datafusion::error::DataFusionError::Execution(
                format!("Failed to flush appender for partition {}: {}", partition, e)
            ))?;
        tracing::debug!("Flushed appender for partition {}", partition);
    }

    tracing::info!("Completed partitioned writes; created {} partition tables, processed {} batches, total rows: {}", 
                  created_tables.len(), batch_count, total_rows);

    Ok(total_rows)
}


pub fn write_to_partitioned_tables_via_parquet(
    table_definition: &Arc<TableDefinition>,
    tx: &Transaction<'_>,                    // used only for the final ingest txn
    schema: SchemaRef,
    mut data_batches: Receiver<(String, Vec<RecordBatch>)>,
    _on_conflict: Option<&OnConflict>,       // handle conflicts in final INSERT if needed
    pool: Arc<DuckDbConnectionPool>,
) -> datafusion::common::Result<u64> {
    let base = std::path::PathBuf::from("/Volumes/envoy_ssd/xdr/duckdb_tmp");

    let mut total_rows: u64 = 0;
    let mut batch_count: usize = 0;

    // one Parquet writer per partition
    let mut sinks: HashMap<String, PartSink> = HashMap::new();

    // 1) STREAM → PARQUET files per partition
    while let Some((partition, batches)) = data_batches.blocking_recv() {
        let sink = sinks.entry(partition.clone())
            .or_insert_with(|| PartSink::new(&base, &partition, &schema).unwrap());

        for batch in batches {
            let rows = batch.num_rows();
            sink.write_batch(&batch, &schema).unwrap();
            total_rows += rows as u64;
            batch_count += 1;
        }
    }
    // close all parquet writers to ensure footers are flushed
    for (_, sink) in sinks.drain() {
        sink.close().unwrap();
    }

    // Optional: global knobs for faster ingest
    // tx.execute("INSTALL arrow FROM community", []).unwrap();
    // tx.execute("LOAD arrow", []).unwrap();

    // Ensure base table(s) exist. Your original code created per-partition *tables*.
    // We’ll mirror that behavior: `{base}_partition_{key}`
    for partition in list_partitions(&base)? {
        let part_table_name = format!("{}_partition_{}", table_definition.name(), partition);

        // Create if not exists with your schema (like your TableManager did)
        let partition_table_def = Arc::new(TableDefinition::new(
            RelationName::new(part_table_name.clone()),
            schema.clone(),
        ));
        let partition_table = Arc::new(TableManager::new(partition_table_def).with_internal(false)
            .map_err(to_datafusion_error)?);

        partition_table.create_table(pool.clone(), tx).map_err(to_datafusion_error)?;

        let start = SystemTime::now();

        // INSERT FROM staged Arrow
        // Files were written under: {base}/partition=<key>/part-*.parquet
        let glob = base.join(format!("partition={}/*.parquet", partition));
        let sql = format!(
            "INSERT INTO {tbl} SELECT * FROM read_parquet('{}', hive_partitioning=false)",
            escape_single_quotes(glob.to_string_lossy().as_ref()),
            tbl = part_table_name
        );
        tx.execute(sql.as_str(), []).unwrap();

        let elapsed = start.elapsed().unwrap();
        println!(
            "Inserted partition {} into table {} in {:?}",
            partition, part_table_name, elapsed
        );
    }

    // If you prefer one big table instead of many small ones:
    //   CREATE TABLE dst AS
    //   SELECT * FROM read_parquet('{base}/partition=*/*.parquet', hive_partitioning=true);

    // Commit happens outside this function if you manage `tx` yourself; if not, you can COMMIT here.
    // tx.commit().map_err(to_datafusion_error)?;

    tracing::info!(
        "Completed partitioned writes via Parquet staging; processed {} batches, total rows: {}",
        batch_count, total_rows
    );

    // TempDir auto-cleans on drop; delete eagerly if you want:
    // std::fs::remove_dir_all(&base).ok();

    Ok(total_rows)
}

fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

fn list_partitions(base: &PathBuf) -> datafusion::common::Result<Vec<String>> {
    let mut parts = Vec::new();
    for entry in std::fs::read_dir(base)? {
        let p = entry?.path();
        if p.is_dir() {
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if let Some(rest) = name.strip_prefix("partition=") {
                    parts.push(rest.to_string());
                }
            }
        }
    }
    Ok(parts)
}

struct PartSink {
    dir: PathBuf,
    file_idx: usize,
    rows: u64,
    writer: Option<ArrowWriter<File>>,
}

impl PartSink {
    fn new(base: &PathBuf, part_key: &str, schema: &SchemaRef) -> datafusion::common::Result<Self> {
        let dir = base.join(format!("partition_{}", part_key));
        std::fs::create_dir_all(&dir)?;
        let mut s = PartSink { 
            dir, 
            file_idx: 0, 
            rows: 0, 
            writer: None,
        };
        s.roll(schema)?; // open first file
        Ok(s)
    }
    
    fn roll(&mut self, schema: &SchemaRef) -> datafusion::common::Result<()> {
        // Close previous writer if it exists
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
        }

        self.file_idx += 1;
        
        // Create new file with incremented index
        let path = self.dir.join(format!("part-{:05}.parquet", self.file_idx));

        let f = File::create(&path)?;
        let props = WriterProperties::builder()
            .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
            .set_max_row_group_size(36864)
            .build();
        self.writer = Some(ArrowWriter::try_new(f, schema.clone(), Some(props))?);
        Ok(())
    }
    
    fn write_batch(&mut self, batch: &RecordBatch, _schema: &SchemaRef) -> datafusion::common::Result<()> {
        self.writer.as_mut().unwrap().write(batch)?;
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    
    /// Flush current file and start a new one, returning the path of the flushed file
    fn flush_current_file(&mut self, schema: &SchemaRef) -> datafusion::common::Result<PathBuf> {
        // Get the path of the file we're about to close
        let current_file_path = self.dir.join(format!("part-{:05}.parquet", self.file_idx));
        
        // Close current writer and start new one (this increments file_idx)
        self.roll(schema)?;
        
        Ok(current_file_path)
    }
    
    fn close(mut self) -> datafusion::common::Result<PathBuf> {
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
        }
        // Return the path of the last file written to
        let current_file_path = self.dir.join(format!("part-{:05}.parquet", self.file_idx));
        Ok(current_file_path)
    }
}

/// Writes a single RecordBatch to a table using the view creation approach
fn write_single_batch_to_table(
    table: &TableManager,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    on_conflict: Option<&OnConflict>,
    batch_id: usize,
) -> datafusion::common::Result<u64> {
    use std::time::Instant;
    
    let batch_start = Instant::now();

    // Step 1: Create Arrow structures
    let step1_start = Instant::now();
    let batch_reader = arrow::array::RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    );
    let stream = FFI_ArrowArrayStream::new(Box::new(batch_reader));
    let step1_duration = step1_start.elapsed();

    let view_name = "_spice_write_view";

    let step3_start = Instant::now();
    tx.register_arrow_scan_view(&view_name, &stream)
        .context(super::UnableToRegisterArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;
    let step3_duration = step3_start.elapsed();

    let step4_start = Instant::now();
    let view = ViewCreator::from_name(RelationName::new(view_name));
    let rows = view
        .insert_into(table, tx, on_conflict)
        .map_err(to_datafusion_error)?;
    let step4_duration = step4_start.elapsed();
    
    // view.drop(tx).map_err(to_datafusion_error)?;


    let total_duration = batch_start.elapsed();
    
    tracing::debug!(
        "Batch {} TIMING: total={}ms | arrow_prep={}μs | register_arrow_scan_view={}μs | insert={}μs | rows={}",
        batch_id,
        total_duration.as_millis(),
        step1_duration.as_micros(),
        step3_duration.as_micros(),
        step4_duration.as_micros(),
        rows
    );

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