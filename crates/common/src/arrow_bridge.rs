//! Bridge between arrow 60 and the arrow version used by DataFusion.
//!
//! The duckdb driver is built against arrow 60, while DataFusion uses a
//! different arrow release. Their types are distinct to the Rust compiler, so
//! values are moved across the boundary using the [Arrow C Data Interface],
//! which is ABI stable between arrow versions. Conversions are zero-copy: only
//! ownership of the underlying buffers is handed over.
//!
//! [Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html

use std::mem::{size_of, ManuallyDrop};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, RecordBatchReader};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

/// Re-export of arrow 60, so that crates using this bridge do not need their
/// own dependency on it.
pub use arrow60;

type Result<T> = std::result::Result<T, ArrowError>;

/// Moves a C Data Interface struct from one arrow version to the other.
///
/// # Safety
///
/// `A` and `B` must be the same `#[repr(C)]` struct defined by the Arrow C
/// Data Interface (e.g. `FFI_ArrowSchema` of two arrow versions).
unsafe fn move_ffi<A, B>(value: A) -> B {
    assert_eq!(size_of::<A>(), size_of::<B>());
    let value = ManuallyDrop::new(value);
    // SAFETY: both types have the same C layout; `value` is not dropped, so
    // the release callback is owned by the returned value only.
    unsafe { std::ptr::read(&*value as *const A as *const B) }
}

/// Wraps an arrow 60 error into an arrow error of the current version.
pub fn error_from_60(err: arrow60::error::ArrowError) -> ArrowError {
    ArrowError::ExternalError(Box::new(err))
}

/// Wraps an arrow error of the current version into an arrow 60 error.
pub fn error_to_60(err: ArrowError) -> arrow60::error::ArrowError {
    arrow60::error::ArrowError::ExternalError(Box::new(err))
}

fn schema_ffi_from_60(ffi: arrow60::ffi::FFI_ArrowSchema) -> FFI_ArrowSchema {
    // SAFETY: both are the C Data Interface `ArrowSchema` struct.
    unsafe { move_ffi(ffi) }
}

fn schema_ffi_to_60(ffi: FFI_ArrowSchema) -> arrow60::ffi::FFI_ArrowSchema {
    // SAFETY: both are the C Data Interface `ArrowSchema` struct.
    unsafe { move_ffi(ffi) }
}

pub fn data_type_from_60(data_type: &arrow60::datatypes::DataType) -> Result<DataType> {
    let ffi = arrow60::ffi::FFI_ArrowSchema::try_from(data_type).map_err(error_from_60)?;
    DataType::try_from(&schema_ffi_from_60(ffi))
}

pub fn data_type_to_60(data_type: &DataType) -> Result<arrow60::datatypes::DataType> {
    let ffi = FFI_ArrowSchema::try_from(data_type)?;
    arrow60::datatypes::DataType::try_from(&schema_ffi_to_60(ffi)).map_err(error_from_60)
}

pub fn field_from_60(field: &arrow60::datatypes::Field) -> Result<Field> {
    let ffi = arrow60::ffi::FFI_ArrowSchema::try_from(field).map_err(error_from_60)?;
    Field::try_from(&schema_ffi_from_60(ffi))
}

pub fn field_to_60(field: &Field) -> Result<arrow60::datatypes::Field> {
    let ffi = FFI_ArrowSchema::try_from(field)?;
    arrow60::datatypes::Field::try_from(&schema_ffi_to_60(ffi)).map_err(error_from_60)
}

pub fn schema_from_60(schema: &arrow60::datatypes::Schema) -> Result<Schema> {
    let ffi = arrow60::ffi::FFI_ArrowSchema::try_from(schema).map_err(error_from_60)?;
    Schema::try_from(&schema_ffi_from_60(ffi))
}

pub fn schema_to_60(schema: &Schema) -> Result<arrow60::datatypes::Schema> {
    let ffi = FFI_ArrowSchema::try_from(schema)?;
    arrow60::datatypes::Schema::try_from(&schema_ffi_to_60(ffi)).map_err(error_from_60)
}

pub fn schema_ref_from_60(schema: &arrow60::datatypes::SchemaRef) -> Result<SchemaRef> {
    schema_from_60(schema).map(Arc::new)
}

pub fn schema_ref_to_60(schema: &SchemaRef) -> Result<arrow60::datatypes::SchemaRef> {
    schema_to_60(schema).map(Arc::new)
}

pub fn array_from_60(array: &dyn arrow60::array::Array) -> Result<ArrayRef> {
    let (ffi_array, ffi_schema) = arrow60::ffi::to_ffi(&array.to_data()).map_err(error_from_60)?;
    // SAFETY: both pairs are C Data Interface `ArrowArray`/`ArrowSchema` structs,
    // freshly exported by arrow 60 and therefore valid.
    let data = unsafe {
        let ffi_array: FFI_ArrowArray = move_ffi(ffi_array);
        arrow::ffi::from_ffi(ffi_array, &schema_ffi_from_60(ffi_schema))?
    };
    Ok(arrow::array::make_array(data))
}

pub fn array_to_60(array: &dyn Array) -> Result<arrow60::array::ArrayRef> {
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array.to_data())?;
    // SAFETY: both pairs are C Data Interface `ArrowArray`/`ArrowSchema` structs,
    // freshly exported by the current arrow version and therefore valid.
    let data = unsafe {
        let ffi_array: arrow60::ffi::FFI_ArrowArray = move_ffi(ffi_array);
        arrow60::ffi::from_ffi(ffi_array, &schema_ffi_to_60(ffi_schema)).map_err(error_from_60)?
    };
    Ok(arrow60::array::make_array(data))
}

pub fn record_batch_from_60(batch: &arrow60::array::RecordBatch) -> Result<RecordBatch> {
    let schema = schema_ref_from_60(&batch.schema())?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| array_from_60(column.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options)
}

pub fn record_batch_to_60(batch: &RecordBatch) -> Result<arrow60::array::RecordBatch> {
    let schema = schema_ref_to_60(&batch.schema())?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| array_to_60(column.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let options = arrow60::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    arrow60::array::RecordBatch::try_new_with_options(schema, columns, &options)
        .map_err(error_from_60)
}

/// Exposes an arrow 60 record batch reader as a reader of the current arrow version.
pub fn reader_from_60(
    reader: Box<dyn arrow60::array::RecordBatchReader + Send>,
) -> Result<ArrowArrayStreamReader> {
    let stream = arrow60::ffi_stream::FFI_ArrowArrayStream::new(reader);
    // SAFETY: both are the C Data Interface `ArrowArrayStream` struct.
    let stream: FFI_ArrowArrayStream = unsafe { move_ffi(stream) };
    ArrowArrayStreamReader::try_new(stream)
}

/// Exposes a record batch reader of the current arrow version as an arrow 60 reader.
pub fn reader_to_60(
    reader: Box<dyn RecordBatchReader + Send>,
) -> Result<arrow60::ffi_stream::ArrowArrayStreamReader> {
    let stream = FFI_ArrowArrayStream::new(reader);
    // SAFETY: both are the C Data Interface `ArrowArrayStream` struct.
    let stream: arrow60::ffi_stream::FFI_ArrowArrayStream = unsafe { move_ffi(stream) };
    arrow60::ffi_stream::ArrowArrayStreamReader::try_new(stream).map_err(error_from_60)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::{Int32Type, TimeUnit};
    use std::collections::HashMap;

    fn sample_batch() -> RecordBatch {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
        ]);
        let strukt = StructArray::from(vec![(
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])) as ArrayRef,
        )]);
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("l", list.data_type().clone(), true),
            Field::new("st", strukt.data_type().clone(), true),
        ])
        .with_metadata(HashMap::from([("k".to_string(), "v".to_string())]));
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(list),
                Arc::new(strukt),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn record_batch_round_trip() {
        let batch = sample_batch();
        let batch60 = record_batch_to_60(&batch).expect("to 60");
        assert_eq!(batch60.num_rows(), 3);
        assert_eq!(batch60.schema().metadata()["k"], "v");
        let back = record_batch_from_60(&batch60).expect("from 60");
        assert_eq!(back, batch);
    }

    #[test]
    fn sliced_array_round_trip() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]).slice(1, 2);
        let back = array_from_60(array_to_60(&array).expect("to 60").as_ref()).expect("from 60");
        assert_eq!(back.as_ref(), &array as &dyn Array);
    }

    #[test]
    fn empty_record_batch_keeps_row_count() {
        let options = RecordBatchOptions::new().with_row_count(Some(5));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
            .expect("valid batch");
        let back =
            record_batch_from_60(&record_batch_to_60(&batch).expect("to 60")).expect("from 60");
        assert_eq!(back.num_rows(), 5);
    }

    #[test]
    fn data_type_round_trip() {
        let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let back =
            data_type_from_60(&data_type_to_60(&data_type).expect("to 60")).expect("from 60");
        assert_eq!(back, data_type);
    }

    #[test]
    fn reader_round_trip() {
        let batch = sample_batch();
        let reader =
            arrow::array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let reader60 = reader_to_60(Box::new(reader)).expect("to 60");
        let reader = reader_from_60(Box::new(reader60)).expect("from 60");
        assert_eq!(reader.schema(), batch.schema());
        let batches = reader.collect::<Result<Vec<_>>>().expect("read batches");
        assert_eq!(batches, vec![batch]);
    }
}
