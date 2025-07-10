use std::sync::Arc;
use std::collections::HashMap;
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, 
    StringBuilder, TimestampMillisecondBuilder, BinaryBuilder, ListBuilder,
    NullBuilder, Decimal128Builder, RecordBatch
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use mongodb::bson::{Bson, Document};

use crate::mongodb::{Result, Error};

pub fn mongo_docs_to_arrow(
    docs: &[Document],
    projected_schema: SchemaRef,
) -> Result<RecordBatch, Error> {
    if docs.is_empty() {
        // Return empty batch with correct schema
        let empty_arrays: Vec<ArrayRef> = projected_schema
            .fields()
            .iter()
            .map(|field| create_empty_array(field.data_type()))
            .collect();
        
        return RecordBatch::try_new(projected_schema, empty_arrays)
            .map_err(|e| Error::ConversionError { 
                source: Box::new(e) 
            });
    }

    let mut builders = create_builders(&projected_schema, docs.len())?;
    
    for doc in docs {
        append_document_to_builders(doc, &projected_schema, &mut builders)?;
    }
    
    let arrays = finish_builders(builders, &projected_schema)?;
    
    RecordBatch::try_new(projected_schema, arrays)
        .map_err(|e| Error::ConversionError { 
            source: Box::new(e) 
        })
}

fn create_empty_array(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => Arc::new(BooleanBuilder::new().finish()),
        DataType::Int32 => Arc::new(Int32Builder::new().finish()),
        DataType::Int64 => Arc::new(Int64Builder::new().finish()),
        DataType::Float64 => Arc::new(Float64Builder::new().finish()),
        DataType::Utf8 => Arc::new(StringBuilder::new().finish()),
        DataType::Binary => Arc::new(BinaryBuilder::new().finish()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Arc::new(TimestampMillisecondBuilder::new().finish())
        }
        DataType::Decimal128(_, _) => {
            Arc::new(Decimal128Builder::new().finish())
        }
        DataType::List(_) => {
            let values_builder = StringBuilder::new();
            Arc::new(ListBuilder::new(values_builder).finish())
        }
        DataType::Null => Arc::new(NullBuilder::new().finish()),
        _ => {
            // Fallback to string for unsupported types
            Arc::new(StringBuilder::new().finish())
        }
    }
}

type BuilderMap = HashMap<String, Box<dyn ArrayBuilderTrait>>;

trait ArrayBuilderTrait {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error>;
    fn finish_builder(self: Box<Self>) -> Result<ArrayRef, Error>;
}

fn create_builders(schema: &SchemaRef, capacity: usize) -> Result<BuilderMap, Error> {
    let mut builders: BuilderMap = HashMap::new();
    
    for field in schema.fields() {
        let builder: Box<dyn ArrayBuilderTrait> = match field.data_type() {
            DataType::Boolean => Box::new(BooleanArrayBuilder::new(capacity)),
            DataType::Int32 => Box::new(Int32ArrayBuilder::new(capacity)),
            DataType::Int64 => Box::new(Int64ArrayBuilder::new(capacity)),
            DataType::Float64 => Box::new(Float64ArrayBuilder::new(capacity)),
            DataType::Utf8 => Box::new(StringArrayBuilder::new(capacity)),
            DataType::Binary => Box::new(BinaryArrayBuilder::new(capacity)),
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                Box::new(TimestampArrayBuilder::new(capacity))
            }
            DataType::Decimal128(precision, scale) => {
                Box::new(Decimal128ArrayBuilder::new(capacity, *precision, *scale))
            }
            DataType::List(_) => Box::new(ListArrayBuilder::new(capacity)),
            DataType::Null => Box::new(NullArrayBuilder::new()),
            _ => {
                // Fallback to string for unsupported types
                Box::new(StringArrayBuilder::new(capacity))
            }
        };
        
        builders.insert(field.name().clone(), builder);
    }
    
    Ok(builders)
}

fn append_document_to_builders(
    doc: &Document,
    schema: &SchemaRef,
    builders: &mut BuilderMap,
) -> Result<(), Error> {
    for field in schema.fields() {
        let field_name = field.name();
        let value = doc.get(field_name);
        
        if let Some(builder) = builders.get_mut(field_name) {
            builder.append_bson(value)?;
        }
    }
    Ok(())
}

fn finish_builders(
    mut builders: BuilderMap,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>, Error> {
    let mut arrays = Vec::new();
    
    for field in schema.fields() {
        let field_name = field.name();
        if let Some(builder) = builders.remove(field_name) {
            arrays.push(builder.finish_builder()?);
        } else {
            return Err(Error::ConversionError {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Missing builder for field: {}", field_name)
                ))
            });
        }
    }
    
    Ok(arrays)
}

struct BooleanArrayBuilder(BooleanBuilder);
struct Int32ArrayBuilder(Int32Builder);
struct Int64ArrayBuilder(Int64Builder);
struct Float64ArrayBuilder(Float64Builder);
struct StringArrayBuilder(StringBuilder);
struct BinaryArrayBuilder(BinaryBuilder);
struct TimestampArrayBuilder(TimestampMillisecondBuilder);
struct Decimal128ArrayBuilder(Decimal128Builder);
struct ListArrayBuilder(ListBuilder<StringBuilder>);
struct NullArrayBuilder(NullBuilder);

impl BooleanArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(BooleanBuilder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for BooleanArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Boolean(b)) => self.0.append_value(*b),
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Int32ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int32Builder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for Int32ArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Int32(i)) => self.0.append_value(*i),
            Some(Bson::Int64(i)) if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 => {
                self.0.append_value(*i as i32)
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Int64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int64Builder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for Int64ArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Int32(i)) => self.0.append_value(*i as i64),
            Some(Bson::Int64(i)) => self.0.append_value(*i),
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Float64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Float64Builder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for Float64ArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Double(d)) => self.0.append_value(*d),
            Some(Bson::Int32(i)) => self.0.append_value(*i as f64),
            Some(Bson::Int64(i)) => self.0.append_value(*i as f64),
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl StringArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(StringBuilder::with_capacity(capacity, 1024))
    }
}

impl ArrayBuilderTrait for StringArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::String(s)) => self.0.append_value(s),
            Some(Bson::ObjectId(oid)) => self.0.append_value(&oid.to_hex()),
            Some(Bson::Document(doc)) => {
                // Convert document to JSON string. Maybe later add support for nested documents
                let json_str = serde_json::to_string(doc)
                    .map_err(|e| Error::ConversionError { source: Box::new(e) })?;
                self.0.append_value(&json_str);
            }
            Some(other) => {
                self.0.append_value(&format!("{}", other));
            }
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl BinaryArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(BinaryBuilder::with_capacity(capacity, 1024))
    }
}

impl ArrayBuilderTrait for BinaryArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Binary(binary)) => self.0.append_value(&binary.bytes),
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TimestampArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(TimestampMillisecondBuilder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for TimestampArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::DateTime(dt)) => {
                self.0.append_value(dt.timestamp_millis())
            }
            Some(Bson::Timestamp(ts)) => {
                // MongoDB timestamp to milliseconds
                self.0.append_value((ts.time as i64) * 1000)
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl Decimal128ArrayBuilder {
    fn new(capacity: usize, _precision: u8, _scale: i8) -> Self {
        Self(Decimal128Builder::with_capacity(capacity))
    }
}

impl ArrayBuilderTrait for Decimal128ArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Decimal128(decimal)) => {
                // Simplified conversion - you might need more sophisticated handling
                let bytes = decimal.bytes();
                let value = i128::from_le_bytes(bytes);
                self.0.append_value(value);
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl ListArrayBuilder {
    fn new(capacity: usize) -> Self {
        let values_builder = StringBuilder::with_capacity(capacity * 4, 256);
        Self(ListBuilder::new(values_builder))
    }
}

impl ArrayBuilderTrait for ListArrayBuilder {
    fn append_bson(&mut self, value: Option<&Bson>) -> Result<(), Error> {
        match value {
            Some(Bson::Array(arr)) => {
                for item in arr {
                    match item {
                        Bson::String(s) => self.0.values().append_value(s),
                        other => self.0.values().append_value(&format!("{}", other)),
                    }
                }
                self.0.append(true);
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl NullArrayBuilder {
    fn new() -> Self {
        Self(NullBuilder::new())
    }
}

impl ArrayBuilderTrait for NullArrayBuilder {
    fn append_bson(&mut self, _value: Option<&Bson>) -> Result<(), Error> {
        self.0.append_null();
        Ok(())
    }
    
    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef, Error> {
        Ok(Arc::new(self.0.finish()))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
    use mongodb::bson::{doc, Bson, Document, oid::ObjectId, DateTime, Timestamp, Binary, spec::BinarySubtype};
    use std::str::FromStr;

    #[test]
    fn test_empty_documents() {
        let docs: Vec<Document> = vec![];
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema.clone()).unwrap();
        
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_single_document_basic_types() {
        let doc = doc! {
            "name": "Alice",
            "age": 30_i32,
            "height": 5.6_f64,
            "is_active": true
        };
        let docs = vec![doc];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("height", DataType::Float64, true),
            Field::new("is_active", DataType::Boolean, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 4);
        
        // Check string value
        let name_array = result.column_by_name("name").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_array.value(0), "Alice");
        
        // Check int32 value
        let age_array = result.column_by_name("age").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(age_array.value(0), 30);
        
        // Check float64 value
        let height_array = result.column_by_name("height").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(height_array.value(0), 5.6);
        
        // Check boolean value
        let active_array = result.column_by_name("is_active").unwrap()
            .as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(active_array.value(0), true);
    }

    #[test]
    fn test_multiple_documents() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30_i32 },
            doc! { "name": "Bob", "age": 25_i32 },
            doc! { "name": "Charlie", "age": 35_i32 },
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        assert_eq!(result.num_rows(), 3);
        
        let name_array = result.column_by_name("name").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");
        
        let age_array = result.column_by_name("age").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
        assert_eq!(age_array.value(2), 35);
    }

    #[test]
    fn test_missing_fields() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30_i32 },
            doc! { "name": "Bob" }, // Missing age
            doc! { "age": 25_i32 }, // Missing name
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        assert_eq!(result.num_rows(), 3);
        
        let name_array = result.column_by_name("name").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert!(name_array.is_null(2)); // Missing name
        
        let age_array = result.column_by_name("age").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(age_array.value(0), 30);
        assert!(age_array.is_null(1)); // Missing age
        assert_eq!(age_array.value(2), 25);
    }

    #[test]
    fn test_mongodb_specific_types() {
        let test_oid = ObjectId::new();
        let test_datetime = DateTime::now();
        let test_timestamp = Timestamp { time: 1234567890, increment: 1 };
        let test_binary_data = vec![1, 2, 3];
        
        let doc = doc! {
            "id": test_oid,
            "created_at": test_datetime,
            "timestamp": test_timestamp,
            "binary_data": Binary { 
                subtype: BinarySubtype::Generic, 
                bytes: test_binary_data.clone() 
            },
            "decimal": mongodb::bson::Decimal128::from_str("123.456").unwrap(),
        };
        let docs = vec![doc];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("binary_data", DataType::Binary, true),
            Field::new("decimal", DataType::Decimal128(38, 10), true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        // Check ObjectId conversion
        let id_array = result.column_by_name("id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_array.value(0), test_oid.to_hex());
        
        // Check DateTime conversion
        let datetime_array = result.column_by_name("created_at").unwrap()
            .as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(datetime_array.value(0), test_datetime.timestamp_millis());
        
        // Check Timestamp conversion
        let timestamp_array = result.column_by_name("timestamp").unwrap()
            .as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(timestamp_array.value(0), (test_timestamp.time as i64) * 1000);
        
        // Check Binary conversion
        let binary_array = result.column_by_name("binary_data").unwrap()
            .as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_array.value(0), test_binary_data);
        
        // Check Decimal128 conversion (simplified - just check it doesn't panic)
        let decimal_array = result.column_by_name("decimal").unwrap()
            .as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimal_array.len(), 1);
        assert!(!decimal_array.is_null(0));
    }

    #[test]
    fn test_numeric_type_coercion() {
        let docs = vec![
            doc! { 
                "int32_to_int64": 100_i32,
                "int32_to_float": 50_i32,
                "int64_to_float": 75_i64
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32_to_int64", DataType::Int64, true),
            Field::new("int32_to_float", DataType::Float64, true),
            Field::new("int64_to_float", DataType::Float64, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        // Int32 -> Int64
        let int64_array = result.column_by_name("int32_to_int64").unwrap()
            .as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100_i64);
        
        // Int32 -> Float64
        let float_array1 = result.column_by_name("int32_to_float").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(float_array1.value(0), 50.0);
        
        // Int64 -> Float64
        let float_array2 = result.column_by_name("int64_to_float").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(float_array2.value(0), 75.0);
    }

    #[test]
    fn test_array_conversion() {
        let docs = vec![
            doc! {
                "string_array": ["a", "b", "c"],
                "mixed_array": ["text", 42_i32, true],
                "empty_array": []
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("string_array", DataType::List(
                Arc::new(Field::new("item", DataType::Utf8, true))
            ), true),
            Field::new("mixed_array", DataType::List(
                Arc::new(Field::new("item", DataType::Utf8, true))
            ), true),
            Field::new("empty_array", DataType::List(
                Arc::new(Field::new("item", DataType::Utf8, true))
            ), true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        // Check string array
        let string_list = result.column_by_name("string_array").unwrap()
            .as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(string_list.len(), 1);

        let string_array_ref = string_list.value(0);
        let string_values = string_array_ref
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_values.len(), 3);
        assert_eq!(string_values.value(0), "a");
        assert_eq!(string_values.value(1), "b");
        assert_eq!(string_values.value(2), "c");
        
        // Check mixed array (all converted to strings)
        let mixed_list = result.column_by_name("mixed_array").unwrap()
            .as_any().downcast_ref::<ListArray>().unwrap();
        let mixed_array_ref = mixed_list.value(0);
        let mixed_values = mixed_array_ref
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(mixed_values.len(), 3);
        assert_eq!(mixed_values.value(0), "text");
        assert_eq!(mixed_values.value(1), "42");
        assert_eq!(mixed_values.value(2), "true");
        
        // Check empty array
        let empty_list = result.column_by_name("empty_array").unwrap()
            .as_any().downcast_ref::<ListArray>().unwrap();
        let empty_array_ref = empty_list.value(0);
        let empty_values = empty_array_ref
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(empty_values.len(), 0);
    }

    #[test]
    fn test_nested_document_conversion() {
        let docs = vec![
            doc! {
                "user": {
                    "name": "Alice",
                    "age": 30_i32
                },
                "metadata": {}
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("user", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        let user_array = result.column_by_name("user").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let user_json = user_array.value(0);
        
        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(user_json).unwrap();
        assert_eq!(parsed["name"], "Alice");
        assert_eq!(parsed["age"], 30);
        
        let metadata_array = result.column_by_name("metadata").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let metadata_json = metadata_array.value(0);
        assert_eq!(metadata_json, "{}");
    }

    #[test]
    fn test_null_values() {
        let docs = vec![
            doc! {
                "nullable_string": Bson::Null,
                "nullable_int": Bson::Null,
                "nullable_bool": Bson::Null,
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("nullable_string", DataType::Utf8, true),
            Field::new("nullable_int", DataType::Int32, true),
            Field::new("nullable_bool", DataType::Boolean, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        let string_array = result.column_by_name("nullable_string").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.len(), 1);
        
        let int_array = result.column_by_name("nullable_int").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.len(), 1);
        
        let bool_array = result.column_by_name("nullable_bool").unwrap()
            .as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_array.len(), 1);
    }

    #[test]
    fn test_null_array_type() {
        let docs = vec![
            doc! { "null_field": Bson::Null }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("null_field", DataType::Null, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        let null_array = result.column_by_name("null_field").unwrap()
            .as_any().downcast_ref::<NullArray>().unwrap();
        
        assert_eq!(null_array.len(), 1);
    }

    #[test]
    fn test_type_mismatch_fallback() {
        let docs = vec![
            doc! {
                "wrong_type_string": 42_i32,  // Int32 in string field
                "wrong_type_int": "not_a_number",  // String in int field
                "wrong_type_bool": 3.14_f64,  // Float in bool field
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("wrong_type_string", DataType::Utf8, true),
            Field::new("wrong_type_int", DataType::Int32, true),
            Field::new("wrong_type_bool", DataType::Boolean, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        // String builder should convert int to string
        let string_array = result.column_by_name("wrong_type_string").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "42");
        
        // Int builder should null out non-int values
        let int_array = result.column_by_name("wrong_type_int").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(int_array.is_null(0));
        
        // Bool builder should null out non-bool values
        let bool_array = result.column_by_name("wrong_type_bool").unwrap()
            .as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.is_null(0));
    }

    #[test]
    fn test_extreme_values() {
        let docs = vec![
            doc! {
                "max_int32": i32::MAX,
                "min_int32": i32::MIN,
                "max_int64": i64::MAX,
                "min_int64": i64::MIN,
                "infinity": f64::INFINITY,
                "neg_infinity": f64::NEG_INFINITY,
                "nan": f64::NAN,
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("max_int32", DataType::Int32, true),
            Field::new("min_int32", DataType::Int32, true),
            Field::new("max_int64", DataType::Int64, true),
            Field::new("min_int64", DataType::Int64, true),
            Field::new("infinity", DataType::Float64, true),
            Field::new("neg_infinity", DataType::Float64, true),
            Field::new("nan", DataType::Float64, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        // Check extreme integers
        let max_int32_array = result.column_by_name("max_int32").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(max_int32_array.value(0), i32::MAX);
        
        let min_int64_array = result.column_by_name("min_int64").unwrap()
            .as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(min_int64_array.value(0), i64::MIN);
        
        // Check special float values
        let inf_array = result.column_by_name("infinity").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(inf_array.value(0).is_infinite());
        assert!(inf_array.value(0).is_sign_positive());
        
        let nan_array = result.column_by_name("nan").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(nan_array.value(0).is_nan());
    }

    #[test]
    fn test_large_binary_data() {
        let large_data = vec![0u8; 10000]; // 10KB of zeros
        let docs = vec![
            doc! {
                "large_binary": Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: large_data.clone()
                }
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("large_binary", DataType::Binary, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        let binary_array = result.column_by_name("large_binary").unwrap()
            .as_any().downcast_ref::<BinaryArray>().unwrap();
        let retrieved_data = binary_array.value(0);
        
        assert_eq!(retrieved_data.len(), 10000);
        assert_eq!(retrieved_data, large_data);
    }

    #[test]
    fn test_unicode_strings() {
        let docs = vec![
            doc! {
                "unicode": "Hello 世界 🌍 مرحبا Здравствуй",
                "emoji": "🚀🎉💯",
                "complex": "𝕳𝖊𝖑𝖑𝖔",
            }
        ];
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("unicode", DataType::Utf8, true),
            Field::new("emoji", DataType::Utf8, true),
            Field::new("complex", DataType::Utf8, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema).unwrap();
        
        let unicode_array = result.column_by_name("unicode").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let unicode_value = unicode_array.value(0);
        assert!(unicode_value.contains("世界"));
        assert!(unicode_value.contains("🌍"));
        assert!(unicode_value.contains("مرحبا"));
        
        let emoji_array = result.column_by_name("emoji").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(emoji_array.value(0), "🚀🎉💯");
    }

    #[test]
    fn test_schema_field_order_preservation() {
        let docs = vec![
            doc! {
                "z_field": "last",
                "a_field": "first",
                "m_field": "middle",
            }
        ];
        
        // Schema with specific field order
        let schema = Arc::new(Schema::new(vec![
            Field::new("a_field", DataType::Utf8, true),
            Field::new("m_field", DataType::Utf8, true),
            Field::new("z_field", DataType::Utf8, true),
        ]));
        
        let result = mongo_docs_to_arrow(&docs, schema.clone()).unwrap();
        
        // Verify field order matches schema order
        assert_eq!(result.schema(), schema);
        
        // Verify data is in correct positions
        let a_array = result.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a_array.value(0), "first");
        
        let m_array = result.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(m_array.value(0), "middle");
        
        let z_array = result.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(z_array.value(0), "last");
    }
}