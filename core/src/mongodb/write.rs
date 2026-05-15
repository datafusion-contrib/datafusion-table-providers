/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::mongodb::connection_pool::MongoDBConnectionPool;
use crate::mongodb::utils::expression::{combine_exprs_with_and, expr_to_mongo_filter};
use crate::util::{
    count_exec::make_count_exec,
    dml::{DeletionExec, DeletionSink, UpdateExec, UpdateSink},
};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::{
        sink::{DataSink, DataSinkExec},
        TableProvider, TableType,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr, TableProviderFilterPushDown},
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan},
    scalar::ScalarValue,
    sql::TableReference,
};
use futures::StreamExt;
use mongodb::bson::{doc, Bson, Document};
use std::{any::Any, fmt, sync::Arc};

/// Wraps a `MongoDBTable` read provider and adds INSERT, UPDATE, and DELETE support.
pub struct MongoDBTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    pool: Arc<MongoDBConnectionPool>,
    table_reference: TableReference,
}

impl fmt::Debug for MongoDBTableWriter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MongoDBTableWriter")
    }
}

impl MongoDBTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        pool: Arc<MongoDBConnectionPool>,
        table_reference: TableReference,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            pool,
            table_reference,
        })
    }
}

#[async_trait]
impl TableProvider for MongoDBTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        // Return Inexact instead of delegating Exact from the read provider.
        // If we let Exact propagate, DataFusion's optimizer removes the Filter node
        // from DELETE/UPDATE logical plans, making extract_dml_filters return empty
        // filters and causing unfiltered deletes/updates on all rows.
        // Inexact keeps the Filter node in the plan while still allowing pushdown.
        let read_pushdowns = self.read_provider.supports_filters_pushdown(filters)?;
        Ok(read_pushdowns
            .into_iter()
            .map(|p| match p {
                TableProviderFilterPushDown::Exact => TableProviderFilterPushDown::Inexact,
                other => other,
            })
            .collect())
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
            Arc::new(MongoDBDataSink {
                pool: Arc::clone(&self.pool),
                table_reference: self.table_reference.clone(),
                schema: self.schema(),
                overwrite: matches!(op, InsertOp::Overwrite),
            }),
            None,
        )))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let filter_doc = exprs_to_filter_doc(&filters)?;
        Ok(Arc::new(DeletionExec::new(Arc::new(MongoDBDeletionSink {
            pool: Arc::clone(&self.pool),
            table_reference: self.table_reference.clone(),
            filter_doc,
        }))))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if assignments.is_empty() {
            return make_count_exec(0);
        }

        let filter_doc = exprs_to_filter_doc(&filters)?;

        let mut set_doc = Document::new();
        for (col, expr) in &assignments {
            set_doc.insert(col.clone(), expr_to_bson(expr)?);
        }

        Ok(Arc::new(UpdateExec::new(Arc::new(MongoDBUpdateSink {
            pool: Arc::clone(&self.pool),
            table_reference: self.table_reference.clone(),
            filter_doc,
            set_doc,
        }))))
    }
}

fn exprs_to_filter_doc(filters: &[Expr]) -> DataFusionResult<Document> {
    if filters.is_empty() {
        return Ok(Document::new());
    }
    let combined = combine_exprs_with_and(filters);
    match combined.and_then(|e| expr_to_mongo_filter(&e)) {
        Some(doc) => Ok(doc),
        None => Err(DataFusionError::Execution(
            "Failed to convert filter expressions to a MongoDB filter document".to_string(),
        )),
    }
}

fn expr_to_bson(expr: &Expr) -> DataFusionResult<Bson> {
    match expr {
        Expr::Literal(scalar, _) => Ok(scalar_to_bson(scalar)),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported expression for MongoDB UPDATE assignment: {expr}"
        ))),
    }
}

fn scalar_to_bson(scalar: &ScalarValue) -> Bson {
    match scalar {
        ScalarValue::Boolean(Some(b)) => Bson::Boolean(*b),
        ScalarValue::Int8(Some(v)) => Bson::Int32(i32::from(*v)),
        ScalarValue::Int16(Some(v)) => Bson::Int32(i32::from(*v)),
        ScalarValue::Int32(Some(v)) => Bson::Int32(*v),
        ScalarValue::Int64(Some(v)) => Bson::Int64(*v),
        ScalarValue::UInt8(Some(v)) => Bson::Int32(i32::from(*v)),
        ScalarValue::UInt16(Some(v)) => Bson::Int32(i32::from(*v)),
        ScalarValue::UInt32(Some(v)) => Bson::Int64(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => Bson::Int64(*v as i64),
        ScalarValue::Float32(Some(v)) => Bson::Double(f64::from(*v)),
        ScalarValue::Float64(Some(v)) => Bson::Double(*v),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Bson::String(s.clone()),
        _ => Bson::Null,
    }
}

// -- DataSink for INSERT --

struct MongoDBDataSink {
    pool: Arc<MongoDBConnectionPool>,
    table_reference: TableReference,
    schema: SchemaRef,
    overwrite: bool,
}

impl fmt::Debug for MongoDBDataSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MongoDBDataSink")
    }
}

impl DisplayAs for MongoDBDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MongoDBDataSink")
    }
}

#[async_trait]
impl DataSink for MongoDBDataSink {
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
        let conn = self.pool.connect().await.map_err(to_datafusion_error)?;
        let collection = conn
            .client
            .database(&conn.db_name)
            .collection::<Document>(self.table_reference.table());

        if self.overwrite {
            collection
                .delete_many(Document::new())
                .await
                .map_err(to_datafusion_error)?;
        }

        let mut num_rows = 0u64;

        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let docs = record_batch_to_documents(&batch);
            num_rows += docs.len() as u64;
            collection
                .insert_many(docs)
                .await
                .map_err(to_datafusion_error)?;
        }

        Ok(num_rows)
    }
}

// -- DeletionSink for DELETE --

struct MongoDBDeletionSink {
    pool: Arc<MongoDBConnectionPool>,
    table_reference: TableReference,
    filter_doc: Document,
}

#[async_trait]
impl DeletionSink for MongoDBDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.connect().await?;
        let collection = conn
            .client
            .database(&conn.db_name)
            .collection::<Document>(self.table_reference.table());
        let result = collection.delete_many(self.filter_doc.clone()).await?;
        Ok(result.deleted_count)
    }
}

// -- UpdateSink for UPDATE --

struct MongoDBUpdateSink {
    pool: Arc<MongoDBConnectionPool>,
    table_reference: TableReference,
    filter_doc: Document,
    set_doc: Document,
}

#[async_trait]
impl UpdateSink for MongoDBUpdateSink {
    async fn execute_update(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.connect().await?;
        let collection = conn
            .client
            .database(&conn.db_name)
            .collection::<Document>(self.table_reference.table());
        let update = doc! { "$set": self.set_doc.clone() };
        let result = collection
            .update_many(self.filter_doc.clone(), update)
            .await?;
        Ok(result.modified_count)
    }
}

// -- Utilities --

fn to_datafusion_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
}

/// Converts an Arrow `RecordBatch` to a `Vec<Document>`.
/// Null `_id` values are omitted so MongoDB auto-generates the document ID.
fn record_batch_to_documents(batch: &RecordBatch) -> Vec<Document> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    let columns: Vec<Vec<Bson>> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(col_idx, array)| {
            let field = schema.field(col_idx);
            (0..num_rows)
                .map(|row_idx| arrow_value_to_bson(array.as_ref(), field.data_type(), row_idx))
                .collect()
        })
        .collect();

    (0..num_rows)
        .map(|row_idx| {
            let mut doc = Document::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let bson_val = columns[col_idx][row_idx].clone();
                if field.name() == "_id" && bson_val == Bson::Null {
                    continue;
                }
                doc.insert(field.name().clone(), bson_val);
            }
            doc
        })
        .collect()
}

fn arrow_value_to_bson(array: &dyn Array, data_type: &DataType, row_idx: usize) -> Bson {
    if array.is_null(row_idx) {
        return Bson::Null;
    }

    match data_type {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Bson::Boolean(arr.value(row_idx))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Bson::Int32(i32::from(arr.value(row_idx)))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Bson::Int32(i32::from(arr.value(row_idx)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Bson::Int32(arr.value(row_idx))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Bson::Int64(arr.value(row_idx))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Bson::Int32(i32::from(arr.value(row_idx)))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Bson::Int32(i32::from(arr.value(row_idx)))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Bson::Int64(i64::from(arr.value(row_idx)))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Bson::Int64(arr.value(row_idx) as i64)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Bson::Double(f64::from(arr.value(row_idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Bson::Double(arr.value(row_idx))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Bson::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Bson::String(arr.value(row_idx).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: arr.value(row_idx).to_vec(),
            })
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: arr.value(row_idx).to_vec(),
            })
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let millis = i64::from(arr.value(row_idx)) * 86_400 * 1_000;
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Bson::DateTime(mongodb::bson::DateTime::from_millis(arr.value(row_idx)))
        }
        DataType::Timestamp(unit, _) => {
            let millis = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    arr.value(row_idx) * 1_000
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    arr.value(row_idx)
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.value(row_idx) / 1_000
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    arr.value(row_idx) / 1_000_000
                }
            };
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        _ => Bson::Null,
    }
}
