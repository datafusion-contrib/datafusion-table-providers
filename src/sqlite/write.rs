use std::{any::Any, fmt, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::Constraints,
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
use futures::StreamExt;
use snafu::prelude::*;
use tokio::runtime::Handle;

use crate::util::{
    constraints, on_conflict::OnConflict, retriable_error::check_and_mark_retriable_error,
};

use super::{to_datafusion_error, Sqlite};

#[derive(Clone)]
pub struct SqliteTableWriter {
    read_provider: Arc<dyn TableProvider>,
    sqlite: Arc<Sqlite>,
    on_conflict: Option<OnConflict>,
}

impl SqliteTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        sqlite: Sqlite,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            sqlite: Arc::new(sqlite),
            on_conflict,
        })
    }

    pub fn sqlite(&self) -> Arc<Sqlite> {
        Arc::clone(&self.sqlite)
    }
}

#[async_trait]
impl TableProvider for SqliteTableWriter {
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
        Some(self.sqlite.constraints())
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
            Arc::new(SqliteDataSink::new(
                Arc::clone(&self.sqlite),
                overwrite,
                self.on_conflict.clone(),
            )),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct SqliteDataSink {
    sqlite: Arc<Sqlite>,
    overwrite: bool,
    on_conflict: Option<OnConflict>,
}

#[async_trait]
impl DataSink for SqliteDataSink {
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
        let mut num_rows: u64 = 0;

        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(1);
        let mut db_conn = self.sqlite.connect().await.map_err(to_datafusion_error)?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let constraints = self.sqlite.constraints().clone();
        let mut data = data;
        let task = tokio::spawn(async move {
            while let Some(data_batch) = data.next().await {
                let data_batch = data_batch.map_err(check_and_mark_retriable_error)?;
                num_rows += u64::try_from(data_batch.num_rows()).map_err(|e| {
                    DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
                })?;

                constraints::validate_batch_with_constraints(&[data_batch.clone()], &constraints)
                    .await
                    .context(super::ConstraintViolationSnafu)
                    .map_err(to_datafusion_error)?;

                batch_tx.send(data_batch).await.map_err(|err| {
                    DataFusionError::Execution(format!("Error sending data batch: {err}"))
                })?;
            }

            Ok::<_, DataFusionError>(())
        });

        let overwrite = self.overwrite;
        let sqlite = Arc::clone(&self.sqlite);
        let on_conflict = self.on_conflict.clone();
        sqlite_conn
            .conn
            .call(move |conn| {
                let transaction = conn.transaction()?;

                if overwrite {
                    sqlite.delete_all_table_data(&transaction)?;
                }

                while let Some(data_batch) = batch_rx.blocking_recv() {
                    if data_batch.num_rows() > 0 {
                        sqlite.insert_batch(&transaction, data_batch, on_conflict.as_ref())?;
                    }
                }

                transaction.commit()?;

                Ok(())
            })
            .await
            .context(super::UnableToInsertIntoTableAsyncSnafu)
            .map_err(to_datafusion_error)?;

        task.await.map_err(|err| {
            DataFusionError::Execution(format!("Error sending data batch: {err}"))
        })??;

        Ok(num_rows)
    }
}

impl SqliteDataSink {
    fn new(sqlite: Arc<Sqlite>, overwrite: bool, on_conflict: Option<OnConflict>) -> Self {
        Self {
            sqlite,
            overwrite,
            on_conflict,
        }
    }
}

impl std::fmt::Debug for SqliteDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteDataSink")
    }
}

impl DisplayAs for SqliteDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteDataSink")
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Schema},
    };
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        datasource::provider::TableProviderFactory,
        execution::context::SessionContext,
        logical_expr::CreateExternalTable,
        physical_plan::{collect, test::exec::MockExec},
    };

    use crate::sqlite::SqliteTableProviderFactory;

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_round_trip_sqlite() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::default(),
        };
        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr1), Arc::new(arr3)])
            .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), false)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");
    }
}
