use std::{any::Any, fmt, sync::Arc, time::Duration};

use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraints, SchemaExt},
    datasource::{
        sink::{DataSink, DataSinkExec},
        TableProvider, TableType,
    },
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr},
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan},
};
use futures::StreamExt;
use snafu::prelude::*;

use crate::util::{
    constraints, on_conflict::OnConflict, retriable_error::check_and_mark_retriable_error,
};

use crate::postgres::Postgres;

use super::to_datafusion_error;

#[derive(Debug, Clone)]
pub struct PostgresWriteConfig {
    pub batch_flush_interval: Duration,
    pub batch_size: usize,
    pub num_records_before_stop: Option<u64>,
}

impl Default for PostgresWriteConfig {
    fn default() -> Self {
        Self {
            batch_flush_interval: Duration::from_secs(1),
            batch_size: 100,
            num_records_before_stop: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostgresTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    postgres: Arc<Postgres>,
    on_conflict: Option<OnConflict>,
    pub write_config: PostgresWriteConfig,
}

impl PostgresTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        postgres: Postgres,
        on_conflict: Option<OnConflict>,
        write_config: PostgresWriteConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            postgres: Arc::new(postgres),
            on_conflict,
            write_config,
        })
    }

    pub fn postgres(&self) -> Arc<Postgres> {
        Arc::clone(&self.postgres)
    }
}

#[async_trait]
impl TableProvider for PostgresTableWriter {
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
        Some(self.postgres.constraints())
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
        let (batch_flush_interval, batch_size, num_records_before_stop) = (
            self.write_config.batch_flush_interval,
            self.write_config.batch_size,
            self.write_config.num_records_before_stop,
        );

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(PostgresDataSink::new(
                Arc::clone(&self.postgres),
                op,
                self.on_conflict.clone(),
                self.schema(),
                batch_flush_interval,
                batch_size,
                num_records_before_stop,
            )),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct PostgresDataSink {
    postgres: Arc<Postgres>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
    batch_flush_interval: Duration,
    batch_size: usize,
    num_records_before_stop: Option<u64>,
}

#[async_trait]
impl DataSink for PostgresDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    // @Goldsky: change the commit behavior to commit in batches
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut num_rows = 0u64;

        let mut db_conn = self.postgres.connect().await.map_err(to_datafusion_error)?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

        if matches!(self.overwrite, InsertOp::Overwrite) {
            let tx = postgres_conn
                .conn
                .transaction()
                .await
                .context(super::UnableToBeginTransactionSnafu)
                .map_err(to_datafusion_error)?;
            self.postgres
                .delete_all_table_data(&tx)
                .await
                .map_err(to_datafusion_error)?;
        }

        let postgres_fields = self
            .postgres
            .schema
            .fields
            .iter()
            .map(|f| {
                Arc::new(Field::new(
                    f.name(),
                    if f.data_type() == &DataType::LargeUtf8 {
                        DataType::Utf8
                    } else {
                        f.data_type().clone()
                    },
                    f.is_nullable(),
                ))
            })
            .collect::<Vec<_>>();

        let postgres_schema = Arc::new(Schema::new(postgres_fields));

        let batch_flush_size = self.batch_size;
        let flush_interval = self.batch_flush_interval;

        let mut batches_buffer = Vec::new();
        let mut buffer_row_count = 0;
        let mut last_flush_time = std::time::Instant::now();

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            // for the purposes of PostgreSQL, LargeUtf8 is equivalent to Utf8
            // because Postgres physically cannot store anything larger than 1Gb in text (VARCHAR)
            // normalize LargeUtf8 fields to Utf8 for both the incoming batch, and Postgres if it happens to specify any
            let batch_fields = batch
                .schema_ref()
                .fields()
                .iter()
                .map(|f| {
                    Arc::new(Field::new(
                        f.name(),
                        if f.data_type() == &DataType::LargeUtf8 {
                            DataType::Utf8
                        } else {
                            f.data_type().clone()
                        },
                        f.is_nullable(),
                    ))
                })
                .collect::<Vec<_>>();
            let batch_schema = Arc::new(Schema::new(batch_fields));

            if !Arc::clone(&postgres_schema).equivalent_names_and_types(&batch_schema) {
                return Err(to_datafusion_error(super::Error::SchemaValidationError {
                    table_name: self.postgres.table.to_string(),
                }));
            }

            let batch_num_rows = batch.num_rows();

            tracing::debug!(
                "Processing batch with {} rows. max_row_count: {:?}",
                batch_num_rows,
                self.num_records_before_stop
            );

            // Skip empty batches to avoid SQL syntax errors
            if batch_num_rows == 0 {
                tracing::debug!("Skipping empty batch");
                continue;
            }

            // Check if we've reached the record limit before processing this batch
            if let Some(max_records) = self.num_records_before_stop {
                if num_rows + batch_num_rows as u64 >= max_records {
                    // Truncate the batch to only include records up to the limit
                    let records_to_take = (max_records - num_rows) as usize;
                    tracing::debug!("Truncating batch to {} rows", records_to_take);
                    if records_to_take > 0 {
                        let truncated_batch = batch.slice(0, records_to_take);
                        batches_buffer.push(truncated_batch);
                        buffer_row_count += records_to_take;
                    }
                    // Force flush of remaining batches and exit
                    break;
                }
            }

            batches_buffer.push(batch);
            buffer_row_count += batch_num_rows;

            // Check if we need to flush based on size or time
            let should_flush =
                buffer_row_count >= batch_flush_size || last_flush_time.elapsed() >= flush_interval;

            if should_flush {
                let tx = postgres_conn
                    .conn
                    .transaction()
                    .await
                    .context(super::UnableToBeginTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                for batch in &batches_buffer {
                    // Skip empty batches
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    constraints::validate_batch_with_constraints(
                        &[batch.clone()],
                        self.postgres.constraints(),
                    )
                    .await
                    .context(super::ConstraintViolationSnafu)
                    .map_err(to_datafusion_error)?;

                    self.postgres
                        .insert_batch(&tx, batch.clone(), self.on_conflict.clone())
                        .await
                        .map_err(to_datafusion_error)?;
                }

                tx.commit()
                    .await
                    .context(super::UnableToCommitPostgresTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                num_rows += buffer_row_count as u64;
                batches_buffer.clear();
                buffer_row_count = 0;
                last_flush_time = std::time::Instant::now();

                // Check if we've reached the record limit after flushing
                if let Some(max_records) = self.num_records_before_stop {
                    if num_rows >= max_records {
                        break;
                    }
                }
            }
        }

        // Flush any remaining batches
        if !batches_buffer.is_empty() {
            let tx = postgres_conn
                .conn
                .transaction()
                .await
                .context(super::UnableToBeginTransactionSnafu)
                .map_err(to_datafusion_error)?;

            for batch in &batches_buffer {
                constraints::validate_batch_with_constraints(
                    &[batch.clone()],
                    self.postgres.constraints(),
                )
                .await
                .context(super::ConstraintViolationSnafu)
                .map_err(to_datafusion_error)?;

                self.postgres
                    .insert_batch(&tx, batch.clone(), self.on_conflict.clone())
                    .await
                    .map_err(to_datafusion_error)?;
            }

            tx.commit()
                .await
                .context(super::UnableToCommitPostgresTransactionSnafu)
                .map_err(to_datafusion_error)?;

            num_rows += buffer_row_count as u64;
            tracing::debug!("flushed final {} rows", num_rows);

            // Ensure we don't exceed the limit even in final flush
            if let Some(max_records) = self.num_records_before_stop {
                assert_eq!(num_rows, max_records);
            }
        }

        Ok(num_rows)
    }
}

impl PostgresDataSink {
    fn new(
        postgres: Arc<Postgres>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
        batch_flush_interval: Duration,
        batch_size: usize,
        num_records_before_stop: Option<u64>,
    ) -> Self {
        Self {
            postgres,
            overwrite,
            on_conflict,
            schema,
            batch_flush_interval,
            batch_size,
            num_records_before_stop,
        }
    }
}

impl std::fmt::Debug for PostgresDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresDataSink")
    }
}

impl DisplayAs for PostgresDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresDataSink")
    }
}
