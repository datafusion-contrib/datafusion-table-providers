//! Bridge between arrow 58 and the arrow version used by DataFusion.
//!
//! Some database drivers (duckdb, arrow-odbc, adbc_core) are still built against
//! arrow 58, while DataFusion uses a newer arrow release. Their types are
//! distinct to the Rust compiler, so values are moved across the boundary
//! using the [Arrow C Data Interface], which is ABI stable between arrow
//! versions. Conversions are zero-copy: only ownership of the underlying
//! buffers is handed over.
//!
//! [Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html

use std::mem::{size_of, ManuallyDrop};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, RecordBatchReader};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

/// Re-export of arrow 58, so that crates using this bridge do not need their
/// own dependency on it.
pub use arrow58;

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

/// Wraps an arrow 58 error into an arrow error of the current version.
pub fn error_from_58(err: arrow58::error::ArrowError) -> ArrowError {
    ArrowError::ExternalError(Box::new(err))
}

/// Wraps an arrow error of the current version into an arrow 58 error.
pub fn error_to_58(err: ArrowError) -> arrow58::error::ArrowError {
    arrow58::error::ArrowError::ExternalError(Box::new(err))
}

fn schema_ffi_from_58(ffi: arrow58::ffi::FFI_ArrowSchema) -> FFI_ArrowSchema {
    // SAFETY: both are the C Data Interface `ArrowSchema` struct.
    unsafe { move_ffi(ffi) }
}

fn schema_ffi_to_58(ffi: FFI_ArrowSchema) -> arrow58::ffi::FFI_ArrowSchema {
    // SAFETY: both are the C Data Interface `ArrowSchema` struct.
    unsafe { move_ffi(ffi) }
}

pub fn data_type_from_58(data_type: &arrow58::datatypes::DataType) -> Result<DataType> {
    let ffi = arrow58::ffi::FFI_ArrowSchema::try_from(data_type).map_err(error_from_58)?;
    DataType::try_from(&schema_ffi_from_58(ffi))
}

pub fn data_type_to_58(data_type: &DataType) -> Result<arrow58::datatypes::DataType> {
    let ffi = FFI_ArrowSchema::try_from(data_type)?;
    arrow58::datatypes::DataType::try_from(&schema_ffi_to_58(ffi)).map_err(error_from_58)
}

pub fn field_from_58(field: &arrow58::datatypes::Field) -> Result<Field> {
    let ffi = arrow58::ffi::FFI_ArrowSchema::try_from(field).map_err(error_from_58)?;
    Field::try_from(&schema_ffi_from_58(ffi))
}

pub fn field_to_58(field: &Field) -> Result<arrow58::datatypes::Field> {
    let ffi = FFI_ArrowSchema::try_from(field)?;
    arrow58::datatypes::Field::try_from(&schema_ffi_to_58(ffi)).map_err(error_from_58)
}

pub fn schema_from_58(schema: &arrow58::datatypes::Schema) -> Result<Schema> {
    let ffi = arrow58::ffi::FFI_ArrowSchema::try_from(schema).map_err(error_from_58)?;
    Schema::try_from(&schema_ffi_from_58(ffi))
}

pub fn schema_to_58(schema: &Schema) -> Result<arrow58::datatypes::Schema> {
    let ffi = FFI_ArrowSchema::try_from(schema)?;
    arrow58::datatypes::Schema::try_from(&schema_ffi_to_58(ffi)).map_err(error_from_58)
}

pub fn schema_ref_from_58(schema: &arrow58::datatypes::SchemaRef) -> Result<SchemaRef> {
    schema_from_58(schema).map(Arc::new)
}

pub fn schema_ref_to_58(schema: &SchemaRef) -> Result<arrow58::datatypes::SchemaRef> {
    schema_to_58(schema).map(Arc::new)
}

pub fn array_from_58(array: &dyn arrow58::array::Array) -> Result<ArrayRef> {
    let (ffi_array, ffi_schema) = arrow58::ffi::to_ffi(&array.to_data()).map_err(error_from_58)?;
    // SAFETY: both pairs are C Data Interface `ArrowArray`/`ArrowSchema` structs,
    // freshly exported by arrow 58 and therefore valid.
    let data = unsafe {
        let ffi_array: FFI_ArrowArray = move_ffi(ffi_array);
        arrow::ffi::from_ffi(ffi_array, &schema_ffi_from_58(ffi_schema))?
    };
    Ok(arrow::array::make_array(data))
}

pub fn array_to_58(array: &dyn Array) -> Result<arrow58::array::ArrayRef> {
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&array.to_data())?;
    // SAFETY: both pairs are C Data Interface `ArrowArray`/`ArrowSchema` structs,
    // freshly exported by the current arrow version and therefore valid.
    let data = unsafe {
        let ffi_array: arrow58::ffi::FFI_ArrowArray = move_ffi(ffi_array);
        arrow58::ffi::from_ffi(ffi_array, &schema_ffi_to_58(ffi_schema)).map_err(error_from_58)?
    };
    Ok(arrow58::array::make_array(data))
}

pub fn record_batch_from_58(batch: &arrow58::array::RecordBatch) -> Result<RecordBatch> {
    let schema = schema_ref_from_58(&batch.schema())?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| array_from_58(column.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options)
}

pub fn record_batch_to_58(batch: &RecordBatch) -> Result<arrow58::array::RecordBatch> {
    let schema = schema_ref_to_58(&batch.schema())?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| array_to_58(column.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let options = arrow58::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    arrow58::array::RecordBatch::try_new_with_options(schema, columns, &options)
        .map_err(error_from_58)
}

/// Exposes an arrow 58 record batch reader as a reader of the current arrow version.
pub fn reader_from_58(
    reader: Box<dyn arrow58::array::RecordBatchReader + Send>,
) -> Result<ArrowArrayStreamReader> {
    let stream = arrow58::ffi_stream::FFI_ArrowArrayStream::new(reader);
    // SAFETY: both are the C Data Interface `ArrowArrayStream` struct.
    let stream: FFI_ArrowArrayStream = unsafe { move_ffi(stream) };
    ArrowArrayStreamReader::try_new(stream)
}

/// Exposes a record batch reader of the current arrow version as an arrow 58 reader.
pub fn reader_to_58(
    reader: Box<dyn RecordBatchReader + Send>,
) -> Result<arrow58::ffi_stream::ArrowArrayStreamReader> {
    let stream = FFI_ArrowArrayStream::new(reader);
    // SAFETY: both are the C Data Interface `ArrowArrayStream` struct.
    let stream: arrow58::ffi_stream::FFI_ArrowArrayStream = unsafe { move_ffi(stream) };
    arrow58::ffi_stream::ArrowArrayStreamReader::try_new(stream).map_err(error_from_58)
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
        let batch58 = record_batch_to_58(&batch).expect("to 58");
        assert_eq!(batch58.num_rows(), 3);
        assert_eq!(batch58.schema().metadata()["k"], "v");
        let back = record_batch_from_58(&batch58).expect("from 58");
        assert_eq!(back, batch);
    }

    #[test]
    fn sliced_array_round_trip() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]).slice(1, 2);
        let back = array_from_58(array_to_58(&array).expect("to 58").as_ref()).expect("from 58");
        assert_eq!(back.as_ref(), &array as &dyn Array);
    }

    #[test]
    fn empty_record_batch_keeps_row_count() {
        let options = RecordBatchOptions::new().with_row_count(Some(5));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
            .expect("valid batch");
        let back =
            record_batch_from_58(&record_batch_to_58(&batch).expect("to 58")).expect("from 58");
        assert_eq!(back.num_rows(), 5);
    }

    #[test]
    fn data_type_round_trip() {
        let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let back =
            data_type_from_58(&data_type_to_58(&data_type).expect("to 58")).expect("from 58");
        assert_eq!(back, data_type);
    }

    #[test]
    fn reader_round_trip() {
        let batch = sample_batch();
        let reader =
            arrow::array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let reader58 = reader_to_58(Box::new(reader)).expect("to 58");
        let reader = reader_from_58(Box::new(reader58)).expect("from 58");
        assert_eq!(reader.schema(), batch.schema());
        let batches = reader.collect::<Result<Vec<_>>>().expect("read batches");
        assert_eq!(batches, vec![batch]);
    }
}
