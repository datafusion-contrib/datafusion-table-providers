use super::{Error, FailedToBuildRecordBatchSnafu, Result};
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, ListBuilder, NullBuilder, RecordBatch, StringBuilder,
        StructBuilder, Time32MillisecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    },
    datatypes::{i256, DataType, Date32Type, Field, Fields, TimeUnit},
};
use arrow_schema::{ArrowError, SchemaRef};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde_json::Value;
use snafu::ResultExt;
use std::any::Any;
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};

pub fn rows_to_arrow(rows: &[Vec<Value>], schema: SchemaRef) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
    }

    let mut builders = create_builders(&schema, rows.len())?;

    for row in rows {
        append_row_to_builders(row, &schema, &mut builders)?;
    }

    let arrays = finish_builders(builders, &schema)?;

    RecordBatch::try_new(Arc::clone(&schema), arrays).context(FailedToBuildRecordBatchSnafu)
}

type BuilderMap = HashMap<String, Box<dyn ArrayBuilder>>;

fn create_builders(schema: &SchemaRef, capacity: usize) -> Result<BuilderMap> {
    let mut builders: BuilderMap = HashMap::new();

    for field in schema.fields() {
        let builder: Box<dyn ArrayBuilder> = create_arrow_builder_for_field(field, capacity)?;
        builders.insert(field.name().clone(), builder);
    }

    Ok(builders)
}

fn create_arrow_builder_for_field(field: &Field, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
    match field.data_type() {
        DataType::Null => Ok(Box::new(NullBuilder::new())),
        DataType::Boolean => Ok(Box::new(BooleanBuilder::with_capacity(capacity))),
        DataType::Int8 => Ok(Box::new(Int8Builder::with_capacity(capacity))),
        DataType::Int16 => Ok(Box::new(Int16Builder::with_capacity(capacity))),
        DataType::Int32 => Ok(Box::new(Int32Builder::with_capacity(capacity))),
        DataType::Int64 => Ok(Box::new(Int64Builder::with_capacity(capacity))),
        DataType::Float32 => Ok(Box::new(Float32Builder::with_capacity(capacity))),
        DataType::Float64 => Ok(Box::new(Float64Builder::with_capacity(capacity))),
        DataType::Decimal128(precision, scale) => {
            let builder = Decimal128BuilderWrapper::new(capacity, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(builder))
        }
        DataType::Decimal256(precision, scale) => {
            let builder = Decimal256BuilderWrapper::new(capacity, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(builder))
        }
        DataType::Utf8 => Ok(Box::new(StringBuilder::with_capacity(capacity, 1024))),
        DataType::Binary => Ok(Box::new(BinaryBuilder::with_capacity(capacity, 1024))),
        DataType::Date32 => Ok(Box::new(Date32Builder::with_capacity(capacity))),
        DataType::Time32(TimeUnit::Millisecond) => {
            Ok(Box::new(Time32MillisecondBuilder::with_capacity(capacity)))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Ok(Box::new(Time64MicrosecondBuilder::with_capacity(capacity)))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Ok(Box::new(Time64NanosecondBuilder::with_capacity(capacity)))
        }
        DataType::Timestamp(time_unit, tz_opt) => match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => Ok(Box::new(
                TimestampMillisecondBuilder::with_capacity(capacity)
                    .with_timezone_opt(tz_opt.clone()),
            )),
            TimeUnit::Microsecond => Ok(Box::new(
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_timezone_opt(tz_opt.clone()),
            )),
            TimeUnit::Nanosecond => Ok(Box::new(
                TimestampNanosecondBuilder::with_capacity(capacity)
                    .with_timezone_opt(tz_opt.clone()),
            )),
        },
        DataType::List(field) => create_list_builder_for_field(field, capacity),
        DataType::Struct(fields) => {
            let mut field_builders = Vec::new();
            for field in fields {
                field_builders.push(create_arrow_builder_for_field(field, capacity)?);
            }
            Ok(Box::new(StructBuilder::new(fields.clone(), field_builders)))
        }
        arrow_type => Err(Error::UnsupportedArrowType {
            arrow_type: arrow_type.to_string(),
        }),
    }
}

struct Decimal128BuilderWrapper {
    inner: Box<Decimal128Builder>,
    precision: u8,
    scale: i8,
}

impl Decimal128BuilderWrapper {
    fn new(capacity: usize, precision: u8, scale: i8) -> std::result::Result<Self, ArrowError> {
        let inner = Decimal128Builder::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)?;

        Ok(Self {
            inner: Box::new(inner),
            precision,
            scale,
        })
    }

    fn append_value(&mut self, value: i128) {
        self.inner.append_value(value);
    }

    fn append_null(&mut self) {
        self.inner.append_null();
    }

    fn data_type(&self) -> DataType {
        DataType::Decimal128(self.precision, self.scale)
    }
}

impl ArrayBuilder for Decimal128BuilderWrapper {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.inner.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.inner.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct Decimal256BuilderWrapper {
    inner: Box<Decimal256Builder>,
    precision: u8,
    scale: i8,
}

impl Decimal256BuilderWrapper {
    fn new(capacity: usize, precision: u8, scale: i8) -> std::result::Result<Self, ArrowError> {
        let inner = Decimal256Builder::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)?;

        Ok(Self {
            inner: Box::new(inner),
            precision,
            scale,
        })
    }

    fn append_value(&mut self, value: i256) {
        self.inner.append_value(value);
    }

    fn append_null(&mut self) {
        self.inner.append_null();
    }

    fn data_type(&self) -> DataType {
        DataType::Decimal256(self.precision, self.scale)
    }
}

impl ArrayBuilder for Decimal256BuilderWrapper {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.inner.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.inner.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

fn create_list_builder_for_field(
    inner_field: &Field,
    capacity: usize,
) -> Result<Box<dyn ArrayBuilder>> {
    match inner_field.data_type() {
        DataType::Null => {
            let values_builder = NullBuilder::new();
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Boolean => {
            let values_builder: Box<dyn ArrayBuilder> =
                Box::new(BooleanBuilder::with_capacity(capacity * 4));
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int8 => {
            let values_builder: Box<dyn ArrayBuilder> =
                Box::new(Int8Builder::with_capacity(capacity * 4));
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int16 => {
            let values_builder = Int16Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int32 => {
            let values_builder = Int32Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int64 => {
            let values_builder = Int64Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Float32 => {
            let values_builder = Float32Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Float64 => {
            let values_builder = Float64Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Utf8 => {
            let values_builder = StringBuilder::with_capacity(capacity * 4, 1024);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Binary => {
            let values_builder = BinaryBuilder::with_capacity(capacity * 4, 1024);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Date32 => {
            let values_builder = Date32Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond) => {
            let values_builder = Time32MillisecondBuilder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let values_builder = Time64MicrosecondBuilder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let values_builder = Time64NanosecondBuilder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Timestamp(time_unit, tz_opt) => match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => {
                let values_builder = TimestampMillisecondBuilder::with_capacity(capacity * 4)
                    .with_timezone_opt(tz_opt.clone());
                Ok(Box::new(ListBuilder::new(values_builder)))
            }
            TimeUnit::Microsecond => {
                let values_builder = TimestampMicrosecondBuilder::with_capacity(capacity * 4)
                    .with_timezone_opt(tz_opt.clone());
                Ok(Box::new(ListBuilder::new(values_builder)))
            }
            TimeUnit::Nanosecond => {
                let values_builder = TimestampMicrosecondBuilder::with_capacity(capacity * 4)
                    .with_timezone_opt(tz_opt.clone());
                Ok(Box::new(ListBuilder::new(values_builder)))
            }
        },
        DataType::Decimal128(precision, scale) => {
            let values_builder = Decimal128BuilderWrapper::new(capacity * 4, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Decimal256(precision, scale) => {
            let values_builder = Decimal256BuilderWrapper::new(capacity * 4, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        arrow_type => Err(Error::UnsupportedArrowType {
            arrow_type: arrow_type.to_string(),
        }),
    }
}

fn append_row_to_builders(
    row: &[Value],
    schema: &SchemaRef,
    builders: &mut BuilderMap,
) -> Result<()> {
    for (field_idx, field) in schema.fields().iter().enumerate() {
        let field_name = field.name();
        let value = row.get(field_idx);

        if let Some(builder) = builders.get_mut(field_name) {
            append_value_to_builder(builder.as_mut(), value, field.data_type())?;
        }
    }
    Ok(())
}

fn append_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    value: Option<&Value>,
    data_type: &DataType,
) -> Result<()> {
    match data_type {
        DataType::Boolean => {
            let bool_builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BooleanBuilder".to_string(),
                })?;
            match value {
                Some(v) if v.is_null() => bool_builder.append_null(),
                Some(Value::Bool(b)) => bool_builder.append_value(*b),
                Some(_) => bool_builder.append_null(),
                None => bool_builder.append_null(),
            }
        }
        DataType::Int8 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int8Builder".to_string(),
                })?;
            append_int8_value(int_builder, value);
        }
        DataType::Int16 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int16Builder".to_string(),
                })?;
            append_int16_value(int_builder, value);
        }
        DataType::Int32 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int32Builder".to_string(),
                })?;
            append_int32_value(int_builder, value);
        }
        DataType::Int64 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int64Builder".to_string(),
                })?;
            append_int64_value(int_builder, value);
        }
        DataType::Float32 => {
            let float_builder = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float32Builder".to_string(),
                })?;
            append_float32_value(float_builder, value);
        }
        DataType::Float64 => {
            let float_builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float64Builder".to_string(),
                })?;
            append_float64_value(float_builder, value);
        }
        DataType::Utf8 => {
            let string_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder".to_string(),
                })?;
            append_string_value(string_builder, value);
        }
        DataType::Binary => {
            let binary_builder = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BinaryBuilder".to_string(),
                })?;
            append_binary_value(binary_builder, value)?;
        }
        DataType::Date32 => {
            let date_builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Date32Builder".to_string(),
                })?;
            append_date32_value(date_builder, value)?;
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let time_builder = builder
                .as_any_mut()
                .downcast_mut::<Time32MillisecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time32MillisecondBuilder".to_string(),
                })?;
            append_time32_millisecond_value(time_builder, value)?;
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let time_builder = builder
                .as_any_mut()
                .downcast_mut::<Time64MicrosecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time64MicrosecondBuilder".to_string(),
                })?;
            append_time64_microsecond_value(time_builder, value)?;
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let time_builder = builder
                .as_any_mut()
                .downcast_mut::<Time64NanosecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time64NanosecondBuilder".to_string(),
                })?;
            append_time64_nanosecond_value(time_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let timestamp_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampMillisecondBuilder".to_string(),
                })?;
            append_timestamp_millisecond_value(timestamp_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let timestamp_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampMicrosecondBuilder".to_string(),
                })?;
            append_timestamp_microsecond_value(timestamp_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let timestamp_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampNanosecondBuilder".to_string(),
                })?;
            append_timestamp_nanosecond_value(timestamp_builder, value)?;
        }
        DataType::Decimal128(_, _) => {
            let decimal_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal128BuilderWrapper>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal128BuilderWrapper".to_string(),
                })?;
            append_decimal128_value(decimal_builder, value)?;
        }
        DataType::Decimal256(_, _) => {
            let decimal_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal256BuilderWrapper>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal256BuilderWrapper".to_string(),
                })?;
            append_decimal256_value(decimal_builder, value)?;
        }
        DataType::List(_) => {
            append_list_value(builder, value)?;
        }
        DataType::Struct(fields) => {
            let struct_builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StructBuilder".to_string(),
                })?;
            append_struct_value(struct_builder, value, fields)?;
        }
        DataType::Null => {
            let null_builder = builder
                .as_any_mut()
                .downcast_mut::<NullBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "NullBuilder".to_string(),
                })?;
            null_builder.append_null();
        }
        arrow_type => {
            return Err(Error::UnsupportedArrowType {
                arrow_type: arrow_type.to_string(),
            });
        }
    }
    Ok(())
}

fn append_int8_value(builder: &mut Int8Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                    builder.append_value(i as i8);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_int16_value(builder: &mut Int16Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                    builder.append_value(i as i16);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_int32_value(builder: &mut Int32Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    builder.append_value(i as i32);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_int64_value(builder: &mut Int64Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                builder.append_value(i);
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_float32_value(builder: &mut Float32Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_f64() => {
            if let Some(f) = n.as_f64() {
                builder.append_value(f as f32);
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_float64_value(builder: &mut Float64Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_f64() => {
            if let Some(f) = n.as_f64() {
                builder.append_value(f);
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_string_value(builder: &mut StringBuilder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(s)) => builder.append_value(s),
        Some(other) => {
            let str_val = serde_json::to_string(other).unwrap_or_default();
            builder.append_value(&str_val);
        }
        None => builder.append_null(),
    }
}

fn append_binary_value(builder: &mut BinaryBuilder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(s)) => {
            if let Ok(bytes) = BASE64.decode(s) {
                builder.append_value(bytes);
            } else {
                builder.append_value(s.as_bytes());
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_date32_value(builder: &mut Date32Builder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(date_str)) => {
            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                builder.append_value(Date32Type::from_naive_date(date));
            } else {
                return Err(Error::InvalidDateValue {
                    value: date_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_time32_millisecond_value(
    builder: &mut Time32MillisecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(time_str)) => {
            if let Ok(time) = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
                let millis = i32::try_from(
                    i64::from(time.num_seconds_from_midnight()) * 1_000
                        + (time.nanosecond() / 1_000_000) as i64,
                )
                .map_err(|_| Error::InvalidTimeValue {
                    value: time_str.to_string(),
                })?;
                builder.append_value(millis);
            } else {
                return Err(Error::InvalidTimeValue {
                    value: time_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_time64_microsecond_value(
    builder: &mut Time64MicrosecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(time_str)) => {
            if let Ok(time) = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
                let micros = i64::from(time.num_seconds_from_midnight()) * 1_000_000
                    + (time.nanosecond() / 1_000) as i64;
                builder.append_value(micros);
            } else {
                return Err(Error::InvalidTimeValue {
                    value: time_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_time64_nanosecond_value(
    builder: &mut Time64NanosecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(time_str)) => {
            if let Ok(time) = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
                let nanos = i64::from(time.num_seconds_from_midnight()) * 1_000_000_000
                    + i64::from(time.nanosecond());
                builder.append_value(nanos);
            } else {
                return Err(Error::InvalidTimeValue {
                    value: time_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

pub fn append_timestamp_millisecond_value(
    builder: &mut TimestampMillisecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),

        Some(Value::String(timestamp_str)) => {
            let ts = timestamp_str.trim();

            if let Some(utc_dt) = parse_timestamp_to_utc_datetime(ts)? {
                builder.append_value(utc_dt.timestamp_millis());
            } else {
                return Err(Error::InvalidTimestampValue {
                    value: ts.to_string(),
                });
            }
        }

        Some(_) | None => builder.append_null(),
    }

    Ok(())
}

pub fn append_timestamp_microsecond_value(
    builder: &mut TimestampMicrosecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),

        Some(Value::String(timestamp_str)) => {
            let ts = timestamp_str.trim();

            if let Some(utc_dt) = parse_timestamp_to_utc_datetime(ts)? {
                builder.append_value(utc_dt.timestamp_micros());
            } else {
                return Err(Error::InvalidTimestampValue {
                    value: ts.to_string(),
                });
            }
        }

        Some(_) | None => builder.append_null(),
    }

    Ok(())
}

pub fn append_timestamp_nanosecond_value(
    builder: &mut TimestampNanosecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),

        Some(Value::String(timestamp_str)) => {
            let ts = timestamp_str.trim();

            if let Some(utc_dt) = parse_timestamp_to_utc_datetime(ts)? {
                let nanos =
                    utc_dt
                        .timestamp_nanos_opt()
                        .ok_or_else(|| Error::InvalidTimestampValue {
                            value: ts.to_string(),
                        })?;
                builder.append_value(nanos);
            } else {
                return Err(Error::InvalidTimestampValue {
                    value: ts.to_string(),
                });
            }
        }

        Some(_) | None => builder.append_null(),
    }

    Ok(())
}

fn parse_timestamp_to_utc_datetime(ts: &str) -> Result<Option<DateTime<Utc>>> {
    // 1. Try parsing with IANA timezone (e.g., "2023-12-25 15:30:00 America/New_York")
    if let Some((datetime_part, tz_part)) = ts.rsplit_once(' ') {
        if let Ok(tz) = Tz::from_str(tz_part) {
            // Parse the datetime part without timezone
            if let Ok(naive_dt) =
                NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f")
            {
                // Convert naive datetime to the specified timezone, then to UTC
                let dt_with_tz = tz.from_local_datetime(&naive_dt).single().ok_or_else(|| {
                    Error::InvalidTimestampValue {
                        value: ts.to_string(),
                    }
                })?;
                let utc_dt = dt_with_tz.with_timezone(&Utc);
                return Ok(Some(utc_dt));
            }
        }
    }

    // 3. Try parsing with numeric timezone offset (e.g., "+05:30", "-08:00")
    if let Ok(dt_with_tz) = DateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f %z") {
        let utc_dt = dt_with_tz.with_timezone(&Utc);
        return Ok(Some(utc_dt));
    }

    // 4. Try parsing with timezone abbreviation (e.g., "PST", "EST")
    if let Some((datetime_part, tz_part)) = ts.rsplit_once(' ') {
        if let Some(tz) = parse_timezone_abbreviation(tz_part) {
            if let Ok(naive_dt) =
                NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f")
            {
                let dt_with_tz = tz.from_local_datetime(&naive_dt).single().ok_or_else(|| {
                    Error::InvalidTimestampValue {
                        value: ts.to_string(),
                    }
                })?;
                let utc_dt = dt_with_tz.with_timezone(&Utc);
                return Ok(Some(utc_dt));
            }
        }
    }

    // 5. Fallback: naive datetime (assume UTC)
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f") {
        let utc_dt = Utc.from_utc_datetime(&naive_dt);
        return Ok(Some(utc_dt));
    }

    // 6. Try ISO 8601 format with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
        let utc_dt = dt.with_timezone(&Utc);
        return Ok(Some(utc_dt));
    }

    Ok(None)
}

fn parse_timezone_abbreviation(tz_abbr: &str) -> Option<Tz> {
    // Map common timezone abbreviations to IANA identifiers
    match tz_abbr {
        "PST" | "PDT" => Some(Tz::America__Los_Angeles),
        "MST" | "MDT" => Some(Tz::America__Denver),
        "CST" | "CDT" => Some(Tz::America__Chicago),
        "EST" | "EDT" => Some(Tz::America__New_York),
        "GMT" | "UTC" => Some(Tz::UTC),
        "JST" => Some(Tz::Asia__Tokyo),
        "CET" | "CEST" => Some(Tz::Europe__Berlin),
        "BST" => Some(Tz::Europe__London),
        "IST" => Some(Tz::Asia__Kolkata),
        "AEST" | "AEDT" => Some(Tz::Australia__Sydney),
        "HST" => Some(Tz::Pacific__Honolulu),
        _ => None,
    }
}

fn append_decimal128_value(
    builder: &mut Decimal128BuilderWrapper,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(decimal_str)) => {
            if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                if let DataType::Decimal128(_, scale) = builder.data_type() {
                    let scale_factor = BigDecimal::from(10_i128.pow(scale as u32));
                    let scaled_decimal = big_decimal * scale_factor;

                    if let Some(decimal_value) = scaled_decimal.to_i128() {
                        builder.append_value(decimal_value);
                    } else {
                        builder.append_null();
                    }
                } else if let Some(decimal_value) = big_decimal.to_i128() {
                    builder.append_value(decimal_value);
                } else {
                    builder.append_null();
                }
            } else {
                return Err(Error::FailedToParseDecimal {
                    value: decimal_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_decimal256_value(
    builder: &mut Decimal256BuilderWrapper,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(decimal_str)) => {
            if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                if let DataType::Decimal256(_, scale) = builder.data_type() {
                    let scale_factor = BigDecimal::from(10_i128.pow(scale as u32));
                    let scaled_decimal = big_decimal * scale_factor;
                    builder.append_value(to_decimal_256(&scaled_decimal));
                } else {
                    builder.append_value(to_decimal_256(&big_decimal));
                }
            } else {
                return Err(Error::FailedToParseDecimal {
                    value: decimal_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_list_value(builder: &mut dyn ArrayBuilder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => {
            append_null_to_list_builder(builder)?;
        }
        Some(Value::Array(arr)) => {
            append_array_to_list_builder(builder, arr)?;
        }
        Some(_) => {
            append_null_to_list_builder(builder)?;
        }
        None => {
            append_null_to_list_builder(builder)?;
        }
    }
    Ok(())
}

fn append_null_to_list_builder(builder: &mut dyn ArrayBuilder) -> Result<()> {
    if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<StringBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BooleanBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int8Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int16Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int32Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int64Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float32Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float64Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BinaryBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Date32Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time32MillisecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time64MicrosecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time64NanosecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampMillisecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampMicrosecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampNanosecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal128Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal256Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<NullBuilder>>()
    {
        list_builder.append_null();
    } else {
        return Err(Error::BuilderDowncastError {
            expected: "ListBuilder<T>".to_string(),
        });
    }
    Ok(())
}

fn append_array_to_list_builder(builder: &mut dyn ArrayBuilder, arr: &Vec<Value>) -> Result<()> {
    if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<StringBuilder>>()
    {
        for item in arr {
            append_string_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BooleanBuilder>>()
    {
        for item in arr {
            match item {
                Value::Bool(b) => list_builder.values().append_value(*b),
                Value::Null => list_builder.values().append_null(),
                _ => list_builder.values().append_null(),
            }
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int8Builder>>()
    {
        for item in arr {
            append_int8_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int16Builder>>()
    {
        for item in arr {
            append_int16_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int32Builder>>()
    {
        for item in arr {
            append_int32_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int64Builder>>()
    {
        for item in arr {
            append_int64_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float32Builder>>()
    {
        for item in arr {
            append_float32_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float64Builder>>()
    {
        for item in arr {
            append_float64_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BinaryBuilder>>()
    {
        for item in arr {
            append_binary_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Date32Builder>>()
    {
        for item in arr {
            append_date32_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time32MillisecondBuilder>>()
    {
        for item in arr {
            append_time32_millisecond_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time64MicrosecondBuilder>>()
    {
        for item in arr {
            append_time64_microsecond_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time64NanosecondBuilder>>()
    {
        for item in arr {
            append_time64_nanosecond_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampMillisecondBuilder>>()
    {
        for item in arr {
            append_timestamp_millisecond_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampMicrosecondBuilder>>()
    {
        for item in arr {
            append_timestamp_microsecond_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampNanosecondBuilder>>()
    {
        for item in arr {
            append_timestamp_nanosecond_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal128BuilderWrapper>>()
    {
        for item in arr {
            append_decimal128_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal256BuilderWrapper>>()
    {
        for item in arr {
            append_decimal256_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<NullBuilder>>()
    {
        for _ in arr {
            list_builder.values().append_null();
        }
        list_builder.append(true);
    } else {
        return Err(Error::BuilderDowncastError {
            expected: "ListBuilder<T>".to_string(),
        });
    }
    Ok(())
}

fn finish_builders(mut builders: BuilderMap, schema: &SchemaRef) -> Result<Vec<ArrayRef>> {
    let mut arrays = Vec::new();

    for field in schema.fields() {
        let field_name = field.name();
        if let Some(mut builder) = builders.remove(field_name) {
            arrays.push(builder.finish());
        } else {
            return Err(Error::FailedToFindFieldInSchema {
                column_name: field_name.to_string(),
            });
        }
    }

    Ok(arrays)
}

fn to_decimal_256(decimal: &BigDecimal) -> i256 {
    let (bigint_value, _) = decimal.as_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    };

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}

fn append_to_struct_field_builder(
    builder: &mut StructBuilder,
    field_index: usize,
    value: Option<&Value>,
    field_data_type: &DataType,
) -> Result<()> {
    match field_data_type {
        DataType::Boolean => {
            let field_builder = builder
                .field_builder::<BooleanBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BooleanBuilder".to_string(),
                })?;
            match value {
                Some(v) if v.is_null() => field_builder.append_null(),
                Some(Value::Bool(b)) => field_builder.append_value(*b),
                _ => field_builder.append_null(),
            }
        }
        DataType::Int8 => {
            let field_builder = builder
                .field_builder::<Int8Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int8Builder".to_string(),
                })?;
            append_int8_value(field_builder, value);
        }
        DataType::Int16 => {
            let field_builder = builder
                .field_builder::<Int16Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int16Builder".to_string(),
                })?;
            append_int16_value(field_builder, value);
        }
        DataType::Int32 => {
            let field_builder = builder
                .field_builder::<Int32Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int32Builder".to_string(),
                })?;
            append_int32_value(field_builder, value);
        }
        DataType::Int64 => {
            let field_builder = builder
                .field_builder::<Int64Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int64Builder".to_string(),
                })?;
            append_int64_value(field_builder, value);
        }
        DataType::Float32 => {
            let field_builder = builder
                .field_builder::<Float32Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float32Builder".to_string(),
                })?;
            append_float32_value(field_builder, value);
        }
        DataType::Float64 => {
            let field_builder = builder
                .field_builder::<Float64Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float64Builder".to_string(),
                })?;
            append_float64_value(field_builder, value);
        }
        DataType::Utf8 => {
            let field_builder = builder
                .field_builder::<StringBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder".to_string(),
                })?;
            append_string_value(field_builder, value);
        }
        DataType::Binary => {
            let field_builder = builder
                .field_builder::<BinaryBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BinaryBuilder".to_string(),
                })?;
            append_binary_value(field_builder, value)?;
        }
        DataType::Date32 => {
            let field_builder = builder
                .field_builder::<Date32Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Date32Builder".to_string(),
                })?;
            append_date32_value(field_builder, value)?;
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let field_builder = builder
                .field_builder::<Time32MillisecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time32MillisecondBuilder".to_string(),
                })?;
            append_time32_millisecond_value(field_builder, value)?;
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let field_builder = builder
                .field_builder::<Time64MicrosecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time64MicrosecondBuilder".to_string(),
                })?;
            append_time64_microsecond_value(field_builder, value)?;
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let field_builder = builder
                .field_builder::<Time64NanosecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time64NanosecondBuilder".to_string(),
                })?;
            append_time64_nanosecond_value(field_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let field_builder = builder
                .field_builder::<TimestampMillisecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampMillisecondBuilder".to_string(),
                })?;
            append_timestamp_millisecond_value(field_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let field_builder = builder
                .field_builder::<TimestampMicrosecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampMicrosecondBuilder".to_string(),
                })?;
            append_timestamp_microsecond_value(field_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let field_builder = builder
                .field_builder::<TimestampNanosecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampNanosecondBuilder".to_string(),
                })?;
            append_timestamp_nanosecond_value(field_builder, value)?;
        }
        DataType::Decimal128(_, _) => {
            let field_builder = builder
                .field_builder::<Decimal128BuilderWrapper>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal128BuilderWrapper".to_string(),
                })?;
            append_decimal128_value(field_builder, value)?;
        }
        DataType::Decimal256(_, _) => {
            let field_builder = builder
                .field_builder::<Decimal256BuilderWrapper>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal256BuilderWrapper".to_string(),
                })?;
            append_decimal256_value(field_builder, value)?;
        }
        DataType::Struct(nested_fields) => {
            let nested_struct_builder = builder
                .field_builder::<StructBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StructBuilder".to_string(),
                })?;
            append_struct_value(nested_struct_builder, value, nested_fields)?;
        }
        DataType::Null => {
            let field_builder = builder
                .field_builder::<NullBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "NullBuilder".to_string(),
                })?;
            field_builder.append_null();
        }
        _ => {
            let field_builder = builder
                .field_builder::<StringBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder (fallback) - 2".to_string(),
                })?;
            append_string_value(field_builder, value);
        }
    }
    Ok(())
}

fn append_struct_value(
    builder: &mut StructBuilder,
    value: Option<&Value>,
    fields: &Fields,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => {
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
        Some(Value::Object(obj)) => {
            for (i, field) in fields.iter().enumerate() {
                let field_value = obj.get(field.name());
                append_to_struct_field_builder(builder, i, field_value, field.data_type())?;
            }
            builder.append(true);
        }
        Some(Value::Array(arr)) => {
            for (i, field) in fields.iter().enumerate() {
                let field_value = arr.get(i);
                append_to_struct_field_builder(builder, i, field_value, field.data_type())?;
            }
            builder.append(true);
        }
        Some(_) => {
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
        None => {
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sql::arrow_sql_gen::trino::schema::trino_data_type_to_arrow_type;
    use arrow::array::*;
    use arrow::datatypes::Schema;
    use serde_json::{json, Value};

    fn create_test_schema(columns: Vec<(&str, &str)>) -> SchemaRef {
        let mut fields = Vec::new();
        for (name, data_type) in columns {
            let arrow_type = trino_data_type_to_arrow_type(data_type, None).unwrap();
            fields.push(Field::new(name, arrow_type, true));
        }

        Arc::new(Schema::new(fields))
    }

    fn assert_boolean_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<bool>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_int8_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i8>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_int16_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i16>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_int32_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i32>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_int64_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i64>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_float32_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<f32>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert!(
                (array.value(i) - expected_value).abs() < f32::EPSILON,
                "Mismatch at index {}: expected {}, got {}",
                i,
                expected_value,
                array.value(i)
            );
        }
    }

    fn assert_float64_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<f64>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert!(
                (array.value(i) - expected_value).abs() < f64::EPSILON,
                "Mismatch at index {}: expected {}, got {}",
                i,
                expected_value,
                array.value(i)
            );
        }
    }

    fn assert_string_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<&str>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_int32_array_with_nulls(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<Option<i32>>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(val) => {
                    assert!(!array.is_null(i), "Expected non-null at index {i}");
                    assert_eq!(array.value(i), *val, "Mismatch at index {i}");
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }

    fn assert_string_array_with_nulls(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<Option<&str>>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(val) => {
                    assert!(!array.is_null(i), "Expected non-null at index {i}");
                    assert_eq!(array.value(i), *val, "Mismatch at index {i}");
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }

    fn assert_date32_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i32>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_time32_millisecond_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i32>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_time64_microsecond_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i64>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_time64_nanosecond_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i64>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_timestamp_millisecond_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i64>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_timestamp_microsecond_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i64>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_timestamp_nanosecond_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i64>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_decimal128_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<i128>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_decimal256_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<arrow::datatypes::i256>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn decimal_to_scaled_int128(decimal_str: &str, scale: u8) -> i128 {
        let decimal = decimal_str.parse::<BigDecimal>().unwrap();
        let scale_factor = 10_i128.pow(scale as u32);
        (decimal * bigdecimal::BigDecimal::from(scale_factor))
            .to_i128()
            .unwrap()
    }

    fn decimal_to_scaled_int256(decimal_str: &str, scale: u8) -> i256 {
        let decimal = decimal_str.parse::<BigDecimal>().unwrap();
        let scale_factor = bigdecimal::BigDecimal::from(10_i128.pow(scale as u32));
        let scaled_decimal = decimal * scale_factor;

        let (bigint_value, _) = scaled_decimal.as_bigint_and_exponent();
        let mut bigint_bytes = bigint_value.to_signed_bytes_le();

        let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
        let fill_byte = if is_negative { 0xFF } else { 0x00 };

        if bigint_bytes.len() > 32 {
            bigint_bytes.truncate(32);
        } else {
            bigint_bytes.resize(32, fill_byte);
        };

        let mut array = [0u8; 32];
        array.copy_from_slice(&bigint_bytes);
        i256::from_le_bytes(array)
    }

    fn assert_binary_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<&[u8]>) {
        let column = record_batch.column(column_index);

        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {i}");
        }
    }

    fn assert_list_of_strings_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<Option<Vec<&str>>>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");

        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(expected_list) => {
                    assert!(!array.is_null(i), "Expected non-null at index {i}");
                    let list_array = array.value(i);
                    let string_array = list_array.as_any().downcast_ref::<StringArray>().unwrap();

                    assert_eq!(
                        string_array.len(),
                        expected_list.len(),
                        "List length mismatch at index {i}"
                    );

                    for (j, expected_item) in expected_list.iter().enumerate() {
                        assert_eq!(
                            string_array.value(j),
                            *expected_item,
                            "Mismatch at index {i} item {j}"
                        );
                    }
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }

    fn assert_list_of_integers_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<Option<Vec<i32>>>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");

        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(expected_list) => {
                    assert!(!array.is_null(i), "Expected non-null at index {i}");
                    let list_array = array.value(i);
                    let string_array = list_array.as_any().downcast_ref::<Int32Array>().unwrap();

                    assert_eq!(
                        string_array.len(),
                        expected_list.len(),
                        "List length mismatch at index {i}"
                    );

                    for (j, expected_item) in expected_list.iter().enumerate() {
                        assert_eq!(
                            string_array.value(j),
                            *expected_item,
                            "Mismatch at index {i} item {j}"
                        );
                    }
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }

    fn assert_struct_array(
        record_batch: &RecordBatch,
        column_index: usize,
        expected: Vec<Option<serde_json::Value>>,
    ) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");

        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(expected_struct) => {
                    assert!(!array.is_null(i), "Expected non-null at index {i}");

                    let mut actual_struct = serde_json::Map::new();

                    for (field_idx, field) in array.fields().iter().enumerate() {
                        let field_array = array.column(field_idx);
                        let field_name = field.name();

                        let field_value = match field.data_type() {
                            DataType::Utf8 => {
                                let string_array =
                                    field_array.as_any().downcast_ref::<StringArray>().unwrap();
                                if string_array.is_null(i) {
                                    Value::Null
                                } else {
                                    Value::String(string_array.value(i).to_string())
                                }
                            }
                            DataType::Int32 => {
                                let int_array =
                                    field_array.as_any().downcast_ref::<Int32Array>().unwrap();
                                if int_array.is_null(i) {
                                    Value::Null
                                } else {
                                    Value::Number(serde_json::Number::from(int_array.value(i)))
                                }
                            }
                            _ => Value::Null,
                        };

                        actual_struct.insert(field_name.clone(), field_value);
                    }

                    let actual_json = Value::Object(actual_struct);
                    assert_eq!(
                        actual_json, *expected_struct,
                        "Struct mismatch at index {i}"
                    );
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }

    #[test]
    fn test_empty_rows_empty_schema() {
        let rows: Vec<Vec<Value>> = vec![];
        let schema = create_test_schema(vec![]);

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 0);
    }

    #[test]
    fn test_empty_rows_with_columns() {
        let rows: Vec<Vec<Value>> = vec![];
        let schema = create_test_schema(vec![
            ("id", "bigint"),
            ("name", "varchar"),
            ("active", "boolean"),
        ]);

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 3);

        let schema = result.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "active");
    }

    #[test]
    fn test_basic_data_types() {
        let schema = create_test_schema(vec![
            ("bool_col", "boolean"),
            ("int8_col", "tinyint"),
            ("int16_col", "smallint"),
            ("int32_col", "integer"),
            ("int64_col", "bigint"),
            ("float32_col", "real"),
            ("float64_col", "double"),
            ("string_col", "varchar"),
        ]);

        let rows = vec![
            vec![
                json!(true),
                json!(127),
                json!(32767),
                json!(2147483647),
                json!(9223372036854775807i64),
                json!(3.14f32),
                json!(2.718281828),
                json!("hello"),
            ],
            vec![
                json!(false),
                json!(-128),
                json!(-32768),
                json!(-2147483648),
                json!(-9223372036854775808i64),
                json!(-1.23f32),
                json!(-9.876543210),
                json!("world"),
            ],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 8);

        assert_boolean_array(&result, 0, vec![true, false]);
        assert_int8_array(&result, 1, vec![127, -128]);
        assert_int16_array(&result, 2, vec![32767, -32768]);
        assert_int32_array(&result, 3, vec![2147483647, -2147483648]);
        assert_int64_array(
            &result,
            4,
            vec![9223372036854775807i64, -9223372036854775808i64],
        );
        assert_float32_array(&result, 5, vec![3.14f32, -1.23f32]);
        assert_float64_array(&result, 6, vec![2.718281828, -9.876543210]);
        assert_string_array(&result, 7, vec!["hello", "world"]);
    }

    #[test]
    fn test_null_values() {
        let schema = create_test_schema(vec![
            ("nullable_int", "integer"),
            ("nullable_string", "varchar"),
        ]);

        let rows = vec![
            vec![json!(42), json!("test")],
            vec![Value::Null, Value::Null],
            vec![json!(100), json!("another")],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 3);

        assert_int32_array_with_nulls(&result, 0, vec![Some(42), None, Some(100)]);
        assert_string_array_with_nulls(&result, 1, vec![Some("test"), None, Some("another")]);
    }

    #[test]
    fn test_time() {
        let schema = create_test_schema(vec![
            ("col0", "time(0)"),
            ("col1", "time(1)"),
            ("col2", "time(2)"),
            ("col3", "time(3)"),
            ("col4", "time(4)"),
            ("col5", "time(5)"),
            ("col6", "time(6)"),
            ("col7", "time(7)"),
            ("col8", "time(8)"),
            ("col9", "time(9)"),
        ]);

        let row = vec![
            json!("14:30:45"),
            json!("14:30:45.1"),
            json!("14:30:45.12"),
            json!("14:30:45.123"),
            json!("14:30:45.1234"),
            json!("14:30:45.12345"),
            json!("14:30:45.123456"),
            json!("14:30:45.1234567"),
            json!("14:30:45.12345678"),
            json!("14:30:45.123456789"),
        ];

        let result = rows_to_arrow(&[row], schema).unwrap();

        let t = |s| NaiveTime::parse_from_str(s, "%H:%M:%S%.f").unwrap();

        assert_time32_millisecond_array(
            &result,
            0,
            vec![t("14:30:45").num_seconds_from_midnight() as i32 * 1000],
        );
        assert_time32_millisecond_array(
            &result,
            1,
            vec![
                t("14:30:45.1").num_seconds_from_midnight() as i32 * 1000
                    + (t("14:30:45.1").nanosecond() / 1_000_000) as i32,
            ],
        );
        assert_time32_millisecond_array(
            &result,
            2,
            vec![
                t("14:30:45.12").num_seconds_from_midnight() as i32 * 1000
                    + (t("14:30:45.12").nanosecond() / 1_000_000) as i32,
            ],
        );
        assert_time32_millisecond_array(
            &result,
            3,
            vec![
                t("14:30:45.123").num_seconds_from_midnight() as i32 * 1000
                    + (t("14:30:45.123").nanosecond() / 1_000_000) as i32,
            ],
        );

        // Microsecond precision
        assert_time64_microsecond_array(
            &result,
            4,
            vec![
                t("14:30:45.1234").num_seconds_from_midnight() as i64 * 1_000_000
                    + (t("14:30:45.1234").nanosecond() / 1_000) as i64,
            ],
        );
        assert_time64_microsecond_array(
            &result,
            5,
            vec![
                t("14:30:45.12345").num_seconds_from_midnight() as i64 * 1_000_000
                    + (t("14:30:45.12345").nanosecond() / 1_000) as i64,
            ],
        );
        assert_time64_microsecond_array(
            &result,
            6,
            vec![
                t("14:30:45.123456").num_seconds_from_midnight() as i64 * 1_000_000
                    + (t("14:30:45.123456").nanosecond() / 1_000) as i64,
            ],
        );

        // Nanosecond precision
        assert_time64_nanosecond_array(
            &result,
            7,
            vec![
                t("14:30:45.1234567").num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t("14:30:45.1234567").nanosecond() as i64,
            ],
        );
        assert_time64_nanosecond_array(
            &result,
            8,
            vec![
                t("14:30:45.12345678").num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t("14:30:45.12345678").nanosecond() as i64,
            ],
        );
        assert_time64_nanosecond_array(
            &result,
            9,
            vec![
                t("14:30:45.123456789").num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t("14:30:45.123456789").nanosecond() as i64,
            ],
        );
    }

    #[test]
    fn test_timestamp() {
        let schema = create_test_schema(vec![
            ("col0", "timestamp(0)"),
            ("col1", "timestamp(1)"),
            ("col2", "timestamp(2)"),
            ("col3", "timestamp(3)"),
            ("col4", "timestamp(4)"),
            ("col5", "timestamp(5)"),
            ("col6", "timestamp(6)"),
            ("col7", "timestamp(7)"),
            ("col8", "timestamp(8)"),
            ("col9", "timestamp(9)"),
        ]);

        let row = vec![
            json!("2023-12-25 14:30:45"),
            json!("2023-12-25 14:30:45.1"),
            json!("2023-12-25 14:30:45.12"),
            json!("2023-12-25 14:30:45.123"),
            json!("2023-12-25 14:30:45.1234"),
            json!("2023-12-25 14:30:45.12345"),
            json!("2023-12-25 14:30:45.123456"),
            json!("2023-12-25 14:30:45.1234567"),
            json!("2023-12-25 14:30:45.12345678"),
            json!("2023-12-25 14:30:45.123456789"),
        ];

        let result = rows_to_arrow(&[row], schema).unwrap();

        let ts = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        };

        assert_timestamp_millisecond_array(
            &result,
            0,
            vec![ts("2023-12-25T14:30:45Z") / 1_000_000],
        );
        assert_timestamp_millisecond_array(
            &result,
            1,
            vec![ts("2023-12-25T14:30:45.1Z") / 1_000_000],
        );
        assert_timestamp_millisecond_array(
            &result,
            2,
            vec![ts("2023-12-25T14:30:45.12Z") / 1_000_000],
        );
        assert_timestamp_millisecond_array(
            &result,
            3,
            vec![ts("2023-12-25T14:30:45.123Z") / 1_000_000],
        );
        assert_timestamp_microsecond_array(
            &result,
            4,
            vec![ts("2023-12-25T14:30:45.1234Z") / 1_000],
        );
        assert_timestamp_microsecond_array(
            &result,
            5,
            vec![ts("2023-12-25T14:30:45.12345Z") / 1_000],
        );
        assert_timestamp_microsecond_array(
            &result,
            6,
            vec![ts("2023-12-25T14:30:45.123456Z") / 1_000],
        );
        assert_timestamp_nanosecond_array(&result, 7, vec![ts("2023-12-25T14:30:45.1234567Z")]);
        assert_timestamp_nanosecond_array(&result, 8, vec![ts("2023-12-25T14:30:45.12345678Z")]);
        assert_timestamp_nanosecond_array(&result, 9, vec![ts("2023-12-25T14:30:45.123456789Z")]);
    }

    #[test]
    fn test_timestamp_with_timezone() {
        let schema = create_test_schema(vec![
            ("col0", "timestamp(0) with time zone"),
            ("col1", "timestamp(1) with time zone"),
            ("col2", "timestamp(2) with time zone"),
            ("col3", "timestamp(3) with time zone"),
            ("col4", "timestamp(4) with time zone"),
            ("col5", "timestamp(5) with time zone"),
            ("col6", "timestamp(6) with time zone"),
            ("col7", "timestamp(7) with time zone"),
            ("col8", "timestamp(8) with time zone"),
            ("col9", "timestamp(9) with time zone"),
        ]);

        let row = vec![
            json!("2023-12-25 14:30:45 UTC"),
            json!("2023-12-25 14:30:45.1 UTC"),
            json!("2023-12-25 15:30:45.12 +01:00"),
            json!("2023-12-25 06:30:45.123 America/Los_Angeles"),
            json!("2023-12-25 14:30:45.1234 UTC"),
            json!("2023-12-25 16:30:45.12345 +02:00"),
            json!("2023-12-25 09:30:45.123456 America/New_York"),
            json!("2023-12-25 14:30:45.1234567 UTC"),
            json!("2023-12-25 11:30:45.12345678 -03:00"),
            json!("2023-12-25 15:30:45.123456789 Europe/Amsterdam"),
        ];

        let result = rows_to_arrow(&[row], schema).unwrap();

        let ts_millis = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_millis()
        };

        let ts_micros = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
                / 1_000
        };

        let ts_nanos = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        };

        assert_timestamp_millisecond_array(&result, 0, vec![ts_millis("2023-12-25T14:30:45Z")]);
        assert_timestamp_millisecond_array(&result, 1, vec![ts_millis("2023-12-25T14:30:45.1Z")]);
        assert_timestamp_millisecond_array(&result, 2, vec![ts_millis("2023-12-25T14:30:45.12Z")]);
        assert_timestamp_millisecond_array(&result, 3, vec![ts_millis("2023-12-25T14:30:45.123Z")]);
        assert_timestamp_microsecond_array(
            &result,
            4,
            vec![ts_micros("2023-12-25T14:30:45.1234Z")],
        );
        assert_timestamp_microsecond_array(
            &result,
            5,
            vec![ts_micros("2023-12-25T14:30:45.12345Z")],
        );
        assert_timestamp_microsecond_array(
            &result,
            6,
            vec![ts_micros("2023-12-25T14:30:45.123456Z")],
        );
        assert_timestamp_nanosecond_array(
            &result,
            7,
            vec![ts_nanos("2023-12-25T14:30:45.1234567Z")],
        );
        assert_timestamp_nanosecond_array(
            &result,
            8,
            vec![ts_nanos("2023-12-25T14:30:45.12345678Z")],
        );
        assert_timestamp_nanosecond_array(
            &result,
            9,
            vec![ts_nanos("2023-12-25T14:30:45.123456789Z")],
        );
    }

    #[test]
    fn test_date() {
        let schema = create_test_schema(vec![("date_col", "date")]);

        let rows = vec![vec![json!("2023-12-25")], vec![json!("1970-01-01")]];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 1);

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date1 = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .signed_duration_since(epoch)
            .num_days() as i32;
        let date2 = 0;

        assert_date32_array(&result, 0, vec![date1, date2]);
    }

    #[test]
    fn test_invalid_date_format() {
        let schema = create_test_schema(vec![("date_col", "date")]);
        let rows = vec![vec![json!("invalid-date")]];

        let result = rows_to_arrow(&rows, schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_decimal_types() {
        let schema = create_test_schema(vec![
            ("decimal128_col", "decimal(10,2)"),
            ("decimal256_col", "decimal(42,4)"),
        ]);

        let rows = vec![
            vec![json!("123.45"), json!("999999999999.9999")],
            vec![json!("0.00"), json!("0.0000")],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let decimal128_1 = decimal_to_scaled_int128("123.45", 2);
        let decimal128_2 = decimal_to_scaled_int128("0.00", 2);

        let decimal256_1 = decimal_to_scaled_int256("999999999999.9999", 4);
        let decimal256_2 = decimal_to_scaled_int256("0.0000", 4);

        assert_decimal128_array(&result, 0, vec![decimal128_1, decimal128_2]);
        assert_decimal256_array(&result, 1, vec![decimal256_1, decimal256_2]);
    }

    #[test]
    fn test_invalid_decimal_format() {
        let schema = create_test_schema(vec![("decimal_col", "decimal(10,2)")]);
        let rows = vec![vec![json!("not-a-number")]];

        let result = rows_to_arrow(&rows, schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_data() {
        let schema = create_test_schema(vec![("binary_col", "varbinary")]);

        let base64_data = BASE64.encode(b"hello world");
        let rows = vec![vec![json!(base64_data)], vec![json!("plain text")]];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_binary_array(&result, 0, vec![b"hello world", b"plain text"]);
    }

    #[test]
    fn test_list_of_strings() {
        let schema = create_test_schema(vec![("list_col", "array(varchar)")]);

        let rows = vec![
            vec![json!(["item1", "item2", "item3"])],
            vec![json!(["single"])],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_list_of_strings_array(
            &result,
            0,
            vec![
                Some(vec!["item1", "item2", "item3"]),
                Some(vec!["single"]),
                None,
            ],
        );
    }

    #[test]
    fn test_list_of_integers() {
        let schema = create_test_schema(vec![("list_col", "array(integer)")]);

        let rows = vec![
            vec![json!([100, 200, 300])],
            vec![json!([-10000000])],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_list_of_integers_array(
            &result,
            0,
            vec![Some(vec![100, 200, 300]), Some(vec![-10000000]), None],
        );
    }

    #[test]
    fn test_list_of_lists() {
        let schema = create_test_schema(vec![("list_col", "array(array(integer))")]);

        let rows = vec![
            vec![json!([[1, 2, 3], [4, 5], [6]])],
            vec![json!([[10, 20]])],
            vec![json!([])],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_list_of_strings_array(
            &result,
            0,
            vec![
                Some(vec!["[1,2,3]", "[4,5]", "[6]"]),
                Some(vec!["[10,20]"]),
                Some(vec![]),
                None,
            ],
        );
    }

    #[test]
    fn test_list_of_structs() {
        let schema =
            create_test_schema(vec![("list_col", "array(row(name varchar, age integer))")]);

        let rows = vec![
            vec![json!([{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}])],
            vec![json!([{"name": "Charlie", "age": 35}])],
            vec![json!([])],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_list_of_strings_array(
            &result,
            0,
            vec![
                Some(vec![
                    r#"{"age":30,"name":"Alice"}"#,
                    r#"{"age":25,"name":"Bob"}"#,
                ]),
                Some(vec![r#"{"age":35,"name":"Charlie"}"#]),
                Some(vec![]),
                None,
            ],
        );
    }

    #[test]
    fn test_struct() {
        let schema = create_test_schema(vec![("struct_col", "row(name varchar, age integer)")]);

        let rows = vec![
            vec![json!({"name": "Alice", "age": 30})],
            vec![json!(["Bob", 25])],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_struct_array(
            &result,
            0,
            vec![
                Some(json!({"name": "Alice", "age": 30})),
                Some(json!({"name": "Bob", "age": 25})),
                None,
            ],
        );
    }

    #[test]
    fn test_struct_with_list() {
        let schema = create_test_schema(vec![(
            "struct_col",
            "row(name varchar, tags array(varchar))",
        )]);

        let rows = vec![
            vec![json!({"name": "Alice", "tags": ["tag1", "tag2", "tag3"]})],
            vec![json!({"name": "Bob", "tags": ["single_tag"]})],
            vec![json!({"name": "Charlie", "tags": []})],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_struct_array(
            &result,
            0,
            vec![
                Some(json!({"name": "Alice", "tags": "[\"tag1\",\"tag2\",\"tag3\"]"})),
                Some(json!({"name": "Bob", "tags": "[\"single_tag\"]"})),
                Some(json!({"name": "Charlie", "tags": "[]"})),
                None,
            ],
        );
    }

    #[test]
    fn test_struct_with_nested_struct() {
        let schema = create_test_schema(vec![(
            "struct_col",
            "row(name varchar, address row(street varchar, city varchar))",
        )]);

        let rows = vec![
            vec![
                json!({"name": "Alice", "address": {"street": "123 Main St", "city": "New York"}}),
            ],
            vec![json!({"name": "Bob", "address": {"street": "456 Oak Ave", "city": "Boston"}})],
            vec![json!({"name": "Charlie", "address": {}})], // empty nested struct
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();

        assert_struct_array(
            &result,
            0,
            vec![
                Some(
                    json!({"name": "Alice", "address": "{\"city\":\"New York\",\"street\":\"123 Main St\"}"}),
                ),
                Some(
                    json!({"name": "Bob", "address": "{\"city\":\"Boston\",\"street\":\"456 Oak Ave\"}"}),
                ),
                Some(json!({"name": "Charlie", "address": "{}"})),
                None,
            ],
        );
    }

    #[test]
    fn test_integer_overflow_handling() {
        let schema = create_test_schema(vec![
            ("int8_col", "tinyint"),
            ("int16_col", "smallint"),
            ("int32_col", "integer"),
        ]);

        let rows = vec![vec![
            json!(1000),
            json!(100000),
            json!(9223372036854775807i64),
        ]];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let int8_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert!(int8_array.is_null(0));

        let int16_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert!(int16_array.is_null(0));

        let int32_array = result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(int32_array.is_null(0));
    }

    #[test]
    fn test_type_coercion_fallback() {
        let schema = create_test_schema(vec![
            ("bool_col", "boolean"),
            ("int_col", "integer"),
            ("string_col", "varchar"),
        ]);

        let rows = vec![vec![
            json!("not a boolean"),
            json!("not a number"),
            json!(42),
        ]];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 1);

        let bool_array = result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_array.is_null(0));

        let int_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(int_array.is_null(0));

        let string_array = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!string_array.is_null(0));
        assert_eq!(string_array.value(0), "42");
    }

    #[test]
    fn test_large_dataset() {
        let schema = create_test_schema(vec![("id", "bigint"), ("value", "varchar")]);

        let mut rows = Vec::new();
        for i in 0..1000 {
            rows.push(vec![json!(i), json!(format!("value_{}", i))]);
        }

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 1000);
        assert_eq!(result.num_columns(), 2);

        let id_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 0);
        assert_eq!(id_array.value(999), 999);

        let value_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value_array.value(0), "value_0");
        assert_eq!(value_array.value(999), "value_999");
    }

    #[test]
    fn test_mixed_null_and_valid_data() {
        let schema =
            create_test_schema(vec![("mixed_int", "integer"), ("mixed_string", "varchar")]);

        let rows = vec![
            vec![json!(1), json!("first")],
            vec![Value::Null, json!("second")],
            vec![json!(3), Value::Null],
            vec![Value::Null, Value::Null],
            vec![json!(5), json!("fifth")],
        ];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 5);

        let int_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert!(!int_array.is_null(0));
        assert!(int_array.is_null(1));
        assert!(!int_array.is_null(2));
        assert!(int_array.is_null(3));
        assert!(!int_array.is_null(4));

        assert_eq!(int_array.value(0), 1);
        assert_eq!(int_array.value(2), 3);
        assert_eq!(int_array.value(4), 5);
    }

    #[test]
    fn test_null_builder_type() {
        let schema = create_test_schema(vec![("null_col", "null")]);
        let rows = vec![vec![Value::Null], vec![Value::Null]];

        let result = rows_to_arrow(&rows, schema).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 1);

        let null_array = result
            .column(0)
            .as_any()
            .downcast_ref::<NullArray>()
            .unwrap();
        assert_eq!(null_array.len(), 2);
    }
}
