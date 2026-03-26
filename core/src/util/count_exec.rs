use std::{any::Any, fmt, sync::Arc};

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
};

pub fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

pub fn count_to_record_batch(
    schema: SchemaRef,
    count: u64,
) -> datafusion::error::Result<RecordBatch> {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    RecordBatch::try_new(schema, vec![array])
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Creates an `ExecutionPlan` that produces a single row with the given count value.
pub fn make_count_exec(count: u64) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    CountExec::try_from_count(count)
}

/// A minimal `ExecutionPlan` that yields a single pre-computed `RecordBatch`.
pub struct CountExec {
    batch: RecordBatch,
    properties: PlanProperties,
}

impl CountExec {
    pub fn new(schema: SchemaRef, batch: RecordBatch) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self { batch, properties }
    }

    /// Creates a `CountExec` that produces a single row with the given count value.
    pub fn try_from_count(count: u64) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![count])) as ArrayRef],
        )
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        Ok(Arc::new(Self::new(schema, batch)))
    }
}

impl fmt::Debug for CountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountExec").finish_non_exhaustive()
    }
}

impl DisplayAs for CountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountExec")
    }
}

impl ExecutionPlan for CountExec {
    fn name(&self) -> &'static str {
        "CountExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let batch = self.batch.clone();
        let schema = self.batch.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(batch) }),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_exec() {
        let exec = CountExec::try_from_count(42).expect("should create CountExec");
        let task_ctx = Arc::new(TaskContext::default());
        let batches = datafusion::physical_plan::collect(exec, task_ctx)
            .await
            .expect("collect should succeed");

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "count");

        let count_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("column should be UInt64Array");
        assert_eq!(count_array.value(0), 42);
    }
}
