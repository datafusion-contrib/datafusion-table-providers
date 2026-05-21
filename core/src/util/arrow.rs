use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;

/// Cast a `RecordBatch` to match the target schema, casting columns whose types differ.
/// Columns that already match are passed through unchanged.
pub fn cast_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .zip(target_schema.fields())
        .map(|(col, target_field)| {
            if col.data_type() == target_field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col, target_field.data_type()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to cast column '{}' from {:?} to {:?}: {e}",
                        target_field.name(),
                        col.data_type(),
                        target_field.data_type(),
                    ))
                })
            }
        })
        .collect::<Result<_, _>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns).map_err(|e| {
        DataFusionError::Execution(format!("Failed to create RecordBatch after cast: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_cast_timestamp_us_to_ns() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));

        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000, 3_000_000])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();

        let result = cast_batch_to_schema(&batch, &target_schema).unwrap();
        assert_eq!(result.schema(), target_schema);
        assert_eq!(
            result.column(1).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        // Values should be multiplied by 1000
        let ts_col = result
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1_000_000_000);
        assert_eq!(ts_col.value(1), 2_000_000_000);
        assert_eq!(ts_col.value(2), 3_000_000_000);
    }

    #[test]
    fn test_no_cast_when_types_match() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let result = cast_batch_to_schema(&batch, &schema).unwrap();
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_cast_incompatible_types_returns_error() {
        use arrow::array::StringArray;

        let source_schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));
        let target_schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec!["not_a_number"]))],
        )
        .unwrap();

        let result = cast_batch_to_schema(&batch, &target_schema);
        assert!(result.is_err());
    }
}
