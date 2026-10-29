use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::TableReference;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::{StreamExt, TryStreamExt};
use scylla::value::CqlValue;
use snafu::prelude::*;

use crate::conn::{quote_identifier, quote_table_reference};
use crate::pool::CassandraConnectionPool;
use crate::types::array_to_cql_values;

/// The maximum number of `INSERT` statements in flight at a time.
const MAX_CONCURRENT_INSERTS: usize = 64;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to prepare the Cassandra statement {cql}: {source}"))]
    Prepare {
        cql: String,
        source: scylla::errors::PrepareError,
    },

    #[snafu(display("Failed to write to the Cassandra table {table_reference}: {source}"))]
    Write {
        table_reference: TableReference,
        source: scylla::errors::ExecutionError,
    },

    #[snafu(display("Failed to convert the data for Cassandra: {source}"))]
    Conversion { source: crate::types::Error },
}

fn to_datafusion_error(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// A Cassandra table that can be written to with `INSERT INTO`.
///
/// Reads are delegated to `read_provider`. Every row is written with its own CQL `INSERT`, so,
/// as in Cassandra, inserting a row whose primary key exists overwrites that row (an upsert).
/// `INSERT OVERWRITE` truncates the table first.
///
/// Cassandra has no transactions: when a write fails, the rows written before the failure
/// are kept.
#[derive(Debug)]
pub struct CassandraTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    table_reference: TableReference,
    pool: Arc<CassandraConnectionPool>,
}

impl CassandraTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        table_reference: TableReference,
        pool: Arc<CassandraConnectionPool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            table_reference,
            pool,
        })
    }
}

#[async_trait]
impl TableProvider for CassandraTableWriter {
    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.read_provider.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(CassandraDataSink {
                table_reference: self.table_reference.clone(),
                pool: Arc::clone(&self.pool),
                op,
                schema: self.schema(),
            }),
            None,
        )) as _)
    }
}

struct CassandraDataSink {
    table_reference: TableReference,
    pool: Arc<CassandraConnectionPool>,
    op: InsertOp,
    schema: SchemaRef,
}

#[async_trait]
impl DataSink for CassandraDataSink {
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
    ) -> DataFusionResult<u64> {
        let session = self.pool.session();
        let table = quote_table_reference(&self.table_reference);

        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
            .collect::<Vec<_>>();
        let cql = format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            vec!["?"; columns.len()].join(", ")
        );
        let prepared = session
            .prepare(cql.as_str())
            .await
            .context(PrepareSnafu { cql })
            .map_err(to_datafusion_error)?;
        let column_types = prepared
            .get_variable_col_specs()
            .iter()
            .map(|spec| spec.typ().clone())
            .collect::<Vec<_>>();

        // Truncate only after the INSERT is prepared, so that an invalid INSERT (e.g. a
        // column that doesn't exist) leaves the table untouched
        if matches!(self.op, InsertOp::Overwrite) {
            session
                .query_unpaged(format!("TRUNCATE {table}"), ())
                .await
                .context(WriteSnafu {
                    table_reference: self.table_reference.clone(),
                })
                .map_err(to_datafusion_error)?;
        }

        let mut num_rows = 0;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }

            let mut columns = batch
                .columns()
                .iter()
                .zip(batch.schema().fields())
                .zip(&column_types)
                .map(|((array, field), typ)| {
                    array_to_cql_values(field.name(), array, typ).map(Vec::into_iter)
                })
                .collect::<Result<Vec<_>, _>>()
                .context(ConversionSnafu)
                .map_err(to_datafusion_error)?;
            let rows = (0..batch.num_rows()).map(|_| {
                columns
                    .iter_mut()
                    .map(|values| values.next().flatten())
                    .collect::<Vec<Option<CqlValue>>>()
            });

            futures::stream::iter(rows)
                .map(|row| session.execute_unpaged(&prepared, row))
                .buffer_unordered(MAX_CONCURRENT_INSERTS)
                .try_for_each(|_| async { Ok(()) })
                .await
                .context(WriteSnafu {
                    table_reference: self.table_reference.clone(),
                })
                .map_err(to_datafusion_error)?;

            num_rows += batch.num_rows() as u64;
        }

        Ok(num_rows)
    }
}

impl fmt::Debug for CassandraDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CassandraDataSink table={}", self.table_reference)
    }
}

impl DisplayAs for CassandraDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CassandraDataSink table={}", self.table_reference)
    }
}
