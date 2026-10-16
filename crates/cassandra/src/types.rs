//! Conversion between CQL and Arrow types and values.

use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, ListArray, MapArray,
    RecordBatch, RecordBatchOptions, StringArray, StructArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::array::{Array, AsArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Field, Fields, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, IntervalMonthDayNano, IntervalMonthDayNanoType, Schema, SchemaRef,
    Time64NanosecondType, TimeUnit, TimestampMillisecondType,
};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use scylla::frame::response::result::{CollectionType, ColumnSpec, ColumnType, NativeType};
use scylla::value::{
    CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint, Row,
};
use snafu::prelude::*;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported CQL type for column '{column_name}': {cql_type}"))]
    UnsupportedCqlType {
        column_name: String,
        cql_type: String,
    },

    #[snafu(display("Unsupported Arrow type {data_type} for a Cassandra column"))]
    UnsupportedArrowType { data_type: DataType },

    #[snafu(display("Cannot convert CQL value {value:?} to Arrow type {data_type}"))]
    IncompatibleValue { value: String, data_type: DataType },

    #[snafu(display("Column '{column_name}' is not part of the query result"))]
    MissingColumn { column_name: String },

    #[snafu(display("Failed to build Arrow data: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("Cannot write column '{column_name}' to Cassandra: {message}"))]
    InvalidValue {
        column_name: String,
        message: String,
    },

    #[snafu(display(
        "Cannot write column '{column_name}' to Cassandra: CQL collections can't contain nulls"
    ))]
    NullInCollection { column_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Number of days between -5877641-06-23 (the CQL `date` epoch) and the Unix epoch.
const CQL_DATE_UNIX_EPOCH_OFFSET: i64 = 1 << 31;

/// Builds the Arrow schema for the columns of a CQL result set.
///
/// Every field is nullable: Cassandra returns `null` for any regular column that has no value.
pub fn column_specs_to_schema<'a>(
    specs: impl IntoIterator<Item = &'a ColumnSpec<'a>>,
) -> Result<SchemaRef> {
    let fields = specs
        .into_iter()
        .map(|spec| {
            Ok(Field::new(
                spec.name(),
                map_column_type(spec.name(), spec.typ())?,
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Maps a CQL column type to its Arrow equivalent.
///
/// Types without an exact Arrow counterpart are mapped to their string representation:
/// `decimal` and `varint` (arbitrary precision), `uuid`, `timeuuid` and `inet`.
pub fn map_column_type(column_name: &str, typ: &ColumnType<'_>) -> Result<DataType> {
    let data_type = match typ {
        ColumnType::Native(native) => match native {
            NativeType::Ascii | NativeType::Text => DataType::Utf8,
            NativeType::Boolean => DataType::Boolean,
            NativeType::Blob => DataType::Binary,
            NativeType::Counter | NativeType::BigInt => DataType::Int64,
            NativeType::Date => DataType::Date32,
            NativeType::Decimal | NativeType::Varint => DataType::Utf8,
            NativeType::Double => DataType::Float64,
            NativeType::Duration => {
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
            }
            NativeType::Float => DataType::Float32,
            NativeType::Int => DataType::Int32,
            NativeType::SmallInt => DataType::Int16,
            NativeType::TinyInt => DataType::Int8,
            NativeType::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            NativeType::Time => DataType::Time64(TimeUnit::Nanosecond),
            NativeType::Uuid | NativeType::Timeuuid | NativeType::Inet => DataType::Utf8,
            _ => return unsupported(column_name, typ),
        },
        ColumnType::Collection {
            typ: collection, ..
        } => match collection {
            CollectionType::List(element) | CollectionType::Set(element) => {
                DataType::List(list_item_field(map_column_type(column_name, element)?))
            }
            CollectionType::Map(key, value) => map_data_type(
                map_column_type(column_name, key)?,
                map_column_type(column_name, value)?,
            ),
            _ => return unsupported(column_name, typ),
        },
        ColumnType::Vector {
            typ: element,
            dimensions,
        } => DataType::FixedSizeList(
            list_item_field(map_column_type(column_name, element)?),
            i32::from(*dimensions),
        ),
        ColumnType::UserDefinedType { definition, .. } => DataType::Struct(
            definition
                .field_types
                .iter()
                .map(|(name, typ)| {
                    Ok(Field::new(
                        name.as_ref(),
                        map_column_type(column_name, typ)?,
                        true,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ),
        ColumnType::Tuple(types) => DataType::Struct(
            types
                .iter()
                .enumerate()
                .map(|(i, typ)| {
                    Ok(Field::new(
                        i.to_string(),
                        map_column_type(column_name, typ)?,
                        true,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ),
        _ => return unsupported(column_name, typ),
    };
    Ok(data_type)
}

fn unsupported<T>(column_name: &str, typ: &ColumnType<'_>) -> Result<T> {
    UnsupportedCqlTypeSnafu {
        column_name,
        cql_type: format!("{typ:?}"),
    }
    .fail()
}

fn list_item_field(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

fn map_data_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", key, false),
                Field::new("value", value, true),
            ])),
            false,
        )),
        false,
    )
}

/// Converts CQL rows to a [`RecordBatch`] with the given `schema`.
///
/// `column_names` are the names of the result set columns, in the order of the values in each
/// row. Every field of `schema` is looked up by name among them, so `schema` may be a projection
/// of the result set. A `schema` without fields produces a batch that only carries the row count.
pub fn rows_to_record_batch(
    schema: &SchemaRef,
    column_names: &[String],
    rows: &[Row],
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let index = column_names
                .iter()
                .position(|name| name == field.name())
                .context(MissingColumnSnafu {
                    column_name: field.name(),
                })?;
            let values: Vec<Option<&CqlValue>> = rows
                .iter()
                .map(|row| row.columns.get(index).and_then(Option::as_ref))
                .collect();
            values_to_array(field.data_type(), &values)
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .context(ArrowSnafu)
}

/// Converts a column of CQL values to an Arrow array of the given type.
pub fn values_to_array(data_type: &DataType, values: &[Option<&CqlValue>]) -> Result<ArrayRef> {
    // CQL has a legacy "empty" value that is distinct from null; it carries no data.
    let values: Vec<Option<&CqlValue>> = values
        .iter()
        .map(|v| v.filter(|v| !matches!(v, CqlValue::Empty)))
        .collect();
    let values = values.as_slice();

    let array: ArrayRef = match data_type {
        DataType::Null => new_null_array(data_type, values.len()),
        DataType::Boolean => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, CqlValue::as_boolean))
                .collect::<Result<BooleanArray>>()?,
        ),
        DataType::Int8 => Arc::new(collect_ints::<i8, Int8Array>(data_type, values)?),
        DataType::Int16 => Arc::new(collect_ints::<i16, Int16Array>(data_type, values)?),
        DataType::Int32 => Arc::new(collect_ints::<i32, Int32Array>(data_type, values)?),
        DataType::Int64 => Arc::new(collect_ints::<i64, Int64Array>(data_type, values)?),
        DataType::Float32 => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, |v| v.as_float()))
                .collect::<Result<Float32Array>>()?,
        ),
        DataType::Float64 => Arc::new(
            values
                .iter()
                .map(|v| {
                    convert(*v, data_type, |v| {
                        v.as_double().or_else(|| v.as_float().map(f64::from))
                    })
                })
                .collect::<Result<Float64Array>>()?,
        ),
        DataType::Utf8 => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, value_to_string))
                .collect::<Result<StringArray>>()?,
        ),
        DataType::LargeUtf8 => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, value_to_string))
                .collect::<Result<LargeStringArray>>()?,
        ),
        DataType::Binary => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, |v| v.as_blob().cloned()))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect::<BinaryArray>(),
        ),
        DataType::LargeBinary => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, |v| v.as_blob().cloned()))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect::<LargeBinaryArray>(),
        ),
        DataType::Date32 => Arc::new(
            values
                .iter()
                .map(|v| {
                    convert(*v, data_type, |v| {
                        v.as_cql_date().and_then(|d| {
                            i32::try_from(i64::from(d.0) - CQL_DATE_UNIX_EPOCH_OFFSET).ok()
                        })
                    })
                })
                .collect::<Result<Date32Array>>()?,
        ),
        DataType::Time64(unit) => {
            let nanos = values
                .iter()
                .map(|v| convert(*v, data_type, |v| v.as_cql_time().map(|t| t.0)))
                .collect::<Result<Vec<_>>>()?;
            match unit {
                TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from(nanos)),
                TimeUnit::Microsecond => Arc::new(Time64MicrosecondArray::from(
                    nanos
                        .into_iter()
                        .map(|n| n.map(|n| n / 1_000))
                        .collect::<Vec<_>>(),
                )),
                _ => {
                    return UnsupportedArrowTypeSnafu {
                        data_type: data_type.clone(),
                    }
                    .fail()
                }
            }
        }
        DataType::Timestamp(unit, tz) => {
            let millis = values
                .iter()
                .map(|v| convert(*v, data_type, |v| v.as_cql_timestamp().map(|t| t.0)))
                .collect::<Result<Vec<_>>>()?;
            let scaled = |factor: i64, divisor: i64| -> Vec<Option<i64>> {
                millis
                    .iter()
                    .map(|m| m.map(|m| m.saturating_mul(factor) / divisor))
                    .collect()
            };
            let array: ArrayRef = match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(scaled(1, 1_000))),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(millis)),
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from(scaled(1_000, 1)))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from(scaled(1_000_000, 1)))
                }
            };
            match tz {
                Some(tz) => {
                    arrow::compute::cast(&array, &DataType::Timestamp(*unit, Some(Arc::clone(tz))))
                        .context(ArrowSnafu)?
                }
                None => array,
            }
        }
        DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => Arc::new(
            values
                .iter()
                .map(|v| {
                    convert(*v, data_type, |v| {
                        v.as_cql_duration()
                            .map(|d| IntervalMonthDayNano::new(d.months, d.days, d.nanoseconds))
                    })
                })
                .collect::<Result<IntervalMonthDayNanoArray>>()?,
        ),
        DataType::Decimal128(precision, scale) => Arc::new(
            values
                .iter()
                .map(|v| convert(*v, data_type, |v| value_to_i128_with_scale(v, *scale)))
                .collect::<Result<Decimal128Array>>()?
                .with_precision_and_scale(*precision, *scale)
                .context(ArrowSnafu)?,
        ),
        DataType::List(item) => {
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0i32);
            let mut children = Vec::new();
            for value in values {
                if let Some(value) = value {
                    let elements = collection_elements(value).context(IncompatibleValueSnafu {
                        value: format!("{value:?}"),
                        data_type: data_type.clone(),
                    })?;
                    children.extend(elements.iter().map(Some));
                }
                offsets.push(i32::try_from(children.len()).map_err(|_| {
                    Error::IncompatibleValue {
                        value: "list with more than i32::MAX elements".to_string(),
                        data_type: data_type.clone(),
                    }
                })?);
            }
            let child = values_to_array(item.data_type(), &children)?;
            Arc::new(
                ListArray::try_new(
                    Arc::clone(item),
                    OffsetBuffer::new(offsets.into()),
                    child,
                    nulls(values),
                )
                .context(ArrowSnafu)?,
            )
        }
        DataType::FixedSizeList(item, size) => {
            let size_usize = usize::try_from(*size).unwrap_or_default();
            let mut children = Vec::with_capacity(values.len() * size_usize);
            for value in values {
                match value {
                    Some(value) => {
                        let elements = collection_elements(value)
                            .filter(|elements| elements.len() == size_usize)
                            .context(IncompatibleValueSnafu {
                                value: format!("{value:?}"),
                                data_type: data_type.clone(),
                            })?;
                        children.extend(elements.iter().map(Some));
                    }
                    None => children.extend(std::iter::repeat_n(None, size_usize)),
                }
            }
            let child = values_to_array(item.data_type(), &children)?;
            Arc::new(
                FixedSizeListArray::try_new(Arc::clone(item), *size, child, nulls(values))
                    .context(ArrowSnafu)?,
            )
        }
        DataType::Struct(fields) => {
            let mut columns: Vec<Vec<Option<&CqlValue>>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            for value in values {
                for (i, field) in fields.iter().enumerate() {
                    let field_value = match value {
                        None => None,
                        Some(CqlValue::UserDefinedType {
                            fields: udt_fields, ..
                        }) => udt_fields
                            .iter()
                            .find(|(name, _)| name == field.name())
                            .and_then(|(_, v)| v.as_ref()),
                        Some(CqlValue::Tuple(elements)) => elements.get(i).and_then(Option::as_ref),
                        Some(value) => {
                            return IncompatibleValueSnafu {
                                value: format!("{value:?}"),
                                data_type: data_type.clone(),
                            }
                            .fail()
                        }
                    };
                    columns[i].push(field_value);
                }
            }
            let children = fields
                .iter()
                .zip(columns)
                .map(|(field, column)| values_to_array(field.data_type(), &column))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(
                StructArray::try_new(fields.clone(), children, nulls(values))
                    .context(ArrowSnafu)?,
            )
        }
        DataType::Map(entries_field, sorted) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return UnsupportedArrowTypeSnafu {
                    data_type: data_type.clone(),
                }
                .fail();
            };
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0i32);
            let mut keys = Vec::new();
            let mut items = Vec::new();
            for value in values {
                match value {
                    Some(CqlValue::Map(entries)) => {
                        for (k, v) in entries {
                            keys.push(Some(k));
                            items.push(Some(v));
                        }
                    }
                    Some(value) => {
                        return IncompatibleValueSnafu {
                            value: format!("{value:?}"),
                            data_type: data_type.clone(),
                        }
                        .fail()
                    }
                    None => {}
                }
                offsets.push(
                    i32::try_from(keys.len()).map_err(|_| Error::IncompatibleValue {
                        value: "map with more than i32::MAX entries".to_string(),
                        data_type: data_type.clone(),
                    })?,
                );
            }
            let entries = StructArray::try_new(
                entry_fields.clone(),
                vec![
                    values_to_array(entry_fields[0].data_type(), &keys)?,
                    values_to_array(entry_fields[1].data_type(), &items)?,
                ],
                None,
            )
            .context(ArrowSnafu)?;
            Arc::new(
                MapArray::try_new(
                    Arc::clone(entries_field),
                    OffsetBuffer::new(offsets.into()),
                    entries,
                    nulls(values),
                    *sorted,
                )
                .context(ArrowSnafu)?,
            )
        }
        _ => {
            return UnsupportedArrowTypeSnafu {
                data_type: data_type.clone(),
            }
            .fail()
        }
    };
    Ok(array)
}

/// Converts a non-null CQL value with `f`, failing if `f` cannot handle it.
fn convert<T>(
    value: Option<&CqlValue>,
    data_type: &DataType,
    f: impl FnOnce(&CqlValue) -> Option<T>,
) -> Result<Option<T>> {
    match value {
        None => Ok(None),
        Some(value) => f(value).map(Some).context(IncompatibleValueSnafu {
            value: format!("{value:?}"),
            data_type: data_type.clone(),
        }),
    }
}

/// Collects integer values into an array, widening or narrowing them to `N` when they fit.
fn collect_ints<N, A>(data_type: &DataType, values: &[Option<&CqlValue>]) -> Result<A>
where
    N: TryFrom<i64>,
    A: FromIterator<Option<N>>,
{
    values
        .iter()
        .map(|v| {
            convert(*v, data_type, |v| {
                value_to_i64(v).and_then(|i| N::try_from(i).ok())
            })
        })
        .collect()
}

fn value_to_i64(value: &CqlValue) -> Option<i64> {
    match value {
        CqlValue::TinyInt(v) => Some(i64::from(*v)),
        CqlValue::SmallInt(v) => Some(i64::from(*v)),
        CqlValue::Int(v) => Some(i64::from(*v)),
        CqlValue::BigInt(v) => Some(*v),
        CqlValue::Counter(c) => Some(c.0),
        CqlValue::Varint(v) => i64::try_from(BigInt::from(v.clone())).ok(),
        _ => None,
    }
}

/// Converts a numeric CQL value to the unscaled `i128` of a decimal with the given `scale`.
fn value_to_i128_with_scale(value: &CqlValue, scale: i8) -> Option<i128> {
    let decimal = match value {
        CqlValue::Decimal(d) => BigDecimal::from(d.clone()),
        CqlValue::Varint(v) => BigDecimal::from(BigInt::from(v.clone())),
        other => BigDecimal::from(value_to_i64(other)?),
    };
    let (unscaled, _) = decimal
        .with_scale(i64::from(scale))
        .into_bigint_and_exponent();
    i128::try_from(unscaled).ok()
}

/// Returns the string representation of a scalar CQL value.
fn value_to_string(value: &CqlValue) -> Option<String> {
    let s = match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => s.clone(),
        CqlValue::Uuid(u) => u.to_string(),
        CqlValue::Timeuuid(u) => u.to_string(),
        CqlValue::Inet(ip) => ip.to_string(),
        CqlValue::Decimal(d) => BigDecimal::from(d.clone()).to_string(),
        CqlValue::Varint(v) => BigInt::from(v.clone()).to_string(),
        CqlValue::Boolean(b) => b.to_string(),
        CqlValue::Double(d) => d.to_string(),
        CqlValue::Float(f) => f.to_string(),
        other => value_to_i64(other)?.to_string(),
    };
    Some(s)
}

fn collection_elements(value: &CqlValue) -> Option<&[CqlValue]> {
    match value {
        CqlValue::List(elements) | CqlValue::Set(elements) | CqlValue::Vector(elements) => {
            Some(elements)
        }
        _ => None,
    }
}

fn nulls(values: &[Option<&CqlValue>]) -> Option<NullBuffer> {
    if values.iter().all(Option::is_some) {
        None
    } else {
        Some(NullBuffer::from_iter(values.iter().map(Option::is_some)))
    }
}

/// Converts the values of `array` to CQL values of type `typ`, for binding them to a statement.
///
/// Returns one value per row, `None` for nulls. The array is first cast to the Arrow type that
/// [`map_column_type`] maps `typ` to, so any Arrow type that can be cast to it is accepted,
/// e.g. `Int32` for a `bigint` column or `Utf8` for a `timestamp` column. `decimal`, `varint`,
/// `uuid`, `timeuuid` and `inet` values are parsed from their string representation; `uuid` and
/// `timeuuid` values are also accepted as 16 bytes of `FixedSizeBinary(16)`.
///
/// # Errors
///
/// Returns an error if the array cannot be converted to `typ`, or if a collection contains a
/// null: CQL collections can't contain nulls.
pub fn array_to_cql_values(
    column_name: &str,
    array: &ArrayRef,
    typ: &ColumnType<'_>,
) -> Result<Vec<Option<CqlValue>>> {
    match typ {
        ColumnType::Native(native) => native_values(column_name, array, native, typ),
        ColumnType::Collection {
            typ: collection, ..
        } => match collection {
            CollectionType::List(element) => {
                list_values(column_name, array, element, typ, CqlValue::List)
            }
            CollectionType::Set(element) => {
                list_values(column_name, array, element, typ, CqlValue::Set)
            }
            CollectionType::Map(key, value) => map_values(column_name, array, key, value, typ),
            _ => unsupported(column_name, typ),
        },
        ColumnType::Vector { typ: element, .. } => {
            list_values(column_name, array, element, typ, CqlValue::Vector)
        }
        ColumnType::UserDefinedType { definition, .. } => {
            let struct_array = as_struct(column_name, array, typ)?;
            if let Some(field) = struct_array.fields().iter().find(|field| {
                !definition
                    .field_types
                    .iter()
                    .any(|(name, _)| name == field.name())
            }) {
                return InvalidValueSnafu {
                    column_name,
                    message: format!(
                        "field '{}' is not a field of the user-defined type {}.{}",
                        field.name(),
                        definition.keyspace,
                        definition.name
                    ),
                }
                .fail();
            }
            let fields = definition
                .field_types
                .iter()
                .map(|(name, typ)| match struct_array.column_by_name(name) {
                    Some(column) => array_to_cql_values(column_name, column, typ),
                    None => Ok(vec![None; struct_array.len()]),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((0..struct_array.len())
                .map(|row| {
                    struct_array
                        .is_valid(row)
                        .then(|| CqlValue::UserDefinedType {
                            keyspace: definition.keyspace.to_string(),
                            name: definition.name.to_string(),
                            fields: definition
                                .field_types
                                .iter()
                                .zip(&fields)
                                .map(|((name, _), values)| (name.to_string(), values[row].clone()))
                                .collect(),
                        })
                })
                .collect())
        }
        ColumnType::Tuple(types) => {
            let struct_array = as_struct(column_name, array, typ)?;
            ensure!(
                struct_array.num_columns() == types.len(),
                InvalidValueSnafu {
                    column_name,
                    message: format!(
                        "a tuple of {} elements can't be written from a struct with {} fields",
                        types.len(),
                        struct_array.num_columns()
                    ),
                }
            );
            let elements = struct_array
                .columns()
                .iter()
                .zip(types)
                .map(|(column, typ)| array_to_cql_values(column_name, column, typ))
                .collect::<Result<Vec<_>>>()?;
            Ok((0..struct_array.len())
                .map(|row| {
                    struct_array.is_valid(row).then(|| {
                        CqlValue::Tuple(elements.iter().map(|values| values[row].clone()).collect())
                    })
                })
                .collect())
        }
        _ => unsupported(column_name, typ),
    }
}

fn native_values(
    column_name: &str,
    array: &ArrayRef,
    native: &NativeType,
    typ: &ColumnType<'_>,
) -> Result<Vec<Option<CqlValue>>> {
    if matches!(native, NativeType::Uuid | NativeType::Timeuuid) {
        if let DataType::FixedSizeBinary(16) = array.data_type() {
            let bytes = array.as_fixed_size_binary();
            return each_value(array, |row| {
                let uuid = Uuid::from_slice(bytes.value(row)).map_err(|e| e.to_string())?;
                Ok(uuid_value(native, uuid))
            })
            .map_err(|message| invalid_value(column_name, message));
        }
    }

    let target_type = match (native, array.data_type()) {
        // Keep the time zone, so that the cast only changes the unit and not the instant
        (NativeType::Timestamp, DataType::Timestamp(_, tz)) => {
            DataType::Timestamp(TimeUnit::Millisecond, tz.clone())
        }
        (NativeType::Timestamp, _) => DataType::Timestamp(TimeUnit::Millisecond, None),
        (NativeType::Counter, _) => {
            return InvalidValueSnafu {
                column_name,
                message: "counter columns can't be written with INSERT, only with UPDATE",
            }
            .fail()
        }
        _ => map_column_type(column_name, typ)?,
    };
    let array = cast(array, &target_type).map_err(|e| {
        invalid_value(
            column_name,
            format!("can't convert {} to CQL {typ:?}: {e}", array.data_type()),
        )
    })?;

    let values = match native {
        NativeType::Ascii => {
            let strings = array.as_string::<i32>();
            each_value(&array, |row| {
                Ok(CqlValue::Ascii(strings.value(row).to_string()))
            })
        }
        NativeType::Text => {
            let strings = array.as_string::<i32>();
            each_value(&array, |row| {
                Ok(CqlValue::Text(strings.value(row).to_string()))
            })
        }
        NativeType::Boolean => {
            let booleans = array.as_boolean();
            each_value(&array, |row| Ok(CqlValue::Boolean(booleans.value(row))))
        }
        NativeType::Blob => {
            let blobs = array.as_binary::<i32>();
            each_value(&array, |row| Ok(CqlValue::Blob(blobs.value(row).to_vec())))
        }
        NativeType::BigInt => {
            let ints = array.as_primitive::<Int64Type>();
            each_value(&array, |row| Ok(CqlValue::BigInt(ints.value(row))))
        }
        NativeType::Int => {
            let ints = array.as_primitive::<Int32Type>();
            each_value(&array, |row| Ok(CqlValue::Int(ints.value(row))))
        }
        NativeType::SmallInt => {
            let ints = array.as_primitive::<Int16Type>();
            each_value(&array, |row| Ok(CqlValue::SmallInt(ints.value(row))))
        }
        NativeType::TinyInt => {
            let ints = array.as_primitive::<Int8Type>();
            each_value(&array, |row| Ok(CqlValue::TinyInt(ints.value(row))))
        }
        NativeType::Double => {
            let floats = array.as_primitive::<Float64Type>();
            each_value(&array, |row| Ok(CqlValue::Double(floats.value(row))))
        }
        NativeType::Float => {
            let floats = array.as_primitive::<Float32Type>();
            each_value(&array, |row| Ok(CqlValue::Float(floats.value(row))))
        }
        NativeType::Date => {
            let dates = array.as_primitive::<Date32Type>();
            each_value(&array, |row| {
                u32::try_from(i64::from(dates.value(row)) + CQL_DATE_UNIX_EPOCH_OFFSET)
                    .map(|days| CqlValue::Date(CqlDate(days)))
                    .map_err(|_| format!("date {} is out of range", dates.value(row)))
            })
        }
        NativeType::Timestamp => {
            let timestamps = array.as_primitive::<TimestampMillisecondType>();
            each_value(&array, |row| {
                Ok(CqlValue::Timestamp(CqlTimestamp(timestamps.value(row))))
            })
        }
        NativeType::Time => {
            let times = array.as_primitive::<Time64NanosecondType>();
            each_value(&array, |row| Ok(CqlValue::Time(CqlTime(times.value(row)))))
        }
        NativeType::Duration => {
            let intervals = array.as_primitive::<IntervalMonthDayNanoType>();
            each_value(&array, |row| {
                let interval = intervals.value(row);
                Ok(CqlValue::Duration(CqlDuration {
                    months: interval.months,
                    days: interval.days,
                    nanoseconds: interval.nanoseconds,
                }))
            })
        }
        NativeType::Decimal => {
            let strings = array.as_string::<i32>();
            each_value(&array, |row| {
                let decimal = BigDecimal::from_str(strings.value(row).trim())
                    .map_err(|e| format!("'{}' is not a decimal: {e}", strings.value(row)))?;
                CqlDecimal::try_from(decimal)
                    .map(CqlValue::Decimal)
                    .map_err(|_| format!("the scale of '{}' is too large", strings.value(row)))
            })
        }
        NativeType::Varint => {
            let strings = array.as_string::<i32>();
            each_value(&array, |row| {
                BigInt::from_str(strings.value(row).trim())
                    .map(|v| CqlValue::Varint(CqlVarint::from(v)))
                    .map_err(|e| format!("'{}' is not an integer: {e}", strings.value(row)))
            })
        }
        NativeType::Uuid | NativeType::Timeuuid => {
            let strings = array.as_string::<i32>();
            each_value(&array, |row| {
                Uuid::parse_str(strings.value(row).trim())
                    .map(|uuid| uuid_value(native, uuid))
                    .map_err(|e| format!("'{}' is not a UUID: {e}", strings.value(row)))
            })
        }
        NativeType::Inet => {
            let strings = array.as_string::<i32>();
            each_value(&array, |row| {
                IpAddr::from_str(strings.value(row).trim())
                    .map(CqlValue::Inet)
                    .map_err(|e| format!("'{}' is not an IP address: {e}", strings.value(row)))
            })
        }
        _ => return unsupported(column_name, typ),
    };
    values.map_err(|message| invalid_value(column_name, message))
}

fn invalid_value(column_name: &str, message: String) -> Error {
    Error::InvalidValue {
        column_name: column_name.to_string(),
        message,
    }
}

fn uuid_value(native: &NativeType, uuid: Uuid) -> CqlValue {
    match native {
        NativeType::Timeuuid => CqlValue::Timeuuid(CqlTimeuuid::from(uuid)),
        _ => CqlValue::Uuid(uuid),
    }
}

/// Converts the non-null values of `array` with `f`, keeping nulls as `None`.
fn each_value(
    array: &dyn Array,
    mut f: impl FnMut(usize) -> Result<CqlValue, String>,
) -> Result<Vec<Option<CqlValue>>, String> {
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                Ok(None)
            } else {
                f(row).map(Some)
            }
        })
        .collect()
}

/// Converts a list-like array (`List`, `LargeList` or `FixedSizeList`) to CQL collections built
/// by `collection`.
fn list_values(
    column_name: &str,
    array: &ArrayRef,
    element: &ColumnType<'_>,
    typ: &ColumnType<'_>,
    collection: fn(Vec<CqlValue>) -> CqlValue,
) -> Result<Vec<Option<CqlValue>>> {
    let (values, ranges): (&ArrayRef, Vec<(usize, usize)>) = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let ranges = list
                .value_offsets()
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect();
            (list.values(), ranges)
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let ranges = list
                .value_offsets()
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect();
            (list.values(), ranges)
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_fixed_size_list();
            let length = list.value_length() as usize;
            let ranges = (0..list.len())
                .map(|row| {
                    let start = list.value_offset(row) as usize;
                    (start, start + length)
                })
                .collect();
            (list.values(), ranges)
        }
        _ => return incompatible_array(column_name, array, typ),
    };

    let elements = array_to_cql_values(column_name, values, element)?;
    ranges
        .into_iter()
        .enumerate()
        .map(|(row, (start, end))| {
            if array.is_null(row) {
                return Ok(None);
            }
            elements[start..end]
                .iter()
                .map(|element| {
                    element
                        .clone()
                        .context(NullInCollectionSnafu { column_name })
                })
                .collect::<Result<Vec<_>>>()
                .map(|elements| Some(collection(elements)))
        })
        .collect()
}

fn map_values(
    column_name: &str,
    array: &ArrayRef,
    key: &ColumnType<'_>,
    value: &ColumnType<'_>,
    typ: &ColumnType<'_>,
) -> Result<Vec<Option<CqlValue>>> {
    let DataType::Map(_, _) = array.data_type() else {
        return incompatible_array(column_name, array, typ);
    };
    let map = array.as_map();
    let keys = array_to_cql_values(column_name, map.keys(), key)?;
    let values = array_to_cql_values(column_name, map.values(), value)?;
    map.value_offsets()
        .windows(2)
        .enumerate()
        .map(|(row, w)| {
            if map.is_null(row) {
                return Ok(None);
            }
            (w[0] as usize..w[1] as usize)
                .map(|i| {
                    let key = keys[i]
                        .clone()
                        .context(NullInCollectionSnafu { column_name })?;
                    let value = values[i]
                        .clone()
                        .context(NullInCollectionSnafu { column_name })?;
                    Ok((key, value))
                })
                .collect::<Result<Vec<_>>>()
                .map(|entries| Some(CqlValue::Map(entries)))
        })
        .collect()
}

fn as_struct<'a>(
    column_name: &str,
    array: &'a ArrayRef,
    typ: &ColumnType<'_>,
) -> Result<&'a StructArray> {
    match array.data_type() {
        DataType::Struct(_) => Ok(array.as_struct()),
        _ => incompatible_array(column_name, array, typ),
    }
}

fn incompatible_array<T>(column_name: &str, array: &ArrayRef, typ: &ColumnType<'_>) -> Result<T> {
    InvalidValueSnafu {
        column_name,
        message: format!("can't convert {} to CQL {typ:?}", array.data_type()),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Int32Type, IntervalUnit};
    use scylla::frame::response::result::{TableSpec, UserDefinedType};
    use scylla::value::{
        Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlVarint,
    };
    use std::borrow::Cow;

    fn native(native: NativeType) -> ColumnType<'static> {
        ColumnType::Native(native)
    }

    fn list_of(typ: ColumnType<'static>) -> ColumnType<'static> {
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(typ)),
        }
    }

    #[test]
    fn maps_native_types() {
        let cases = [
            (NativeType::Ascii, DataType::Utf8),
            (NativeType::Text, DataType::Utf8),
            (NativeType::Boolean, DataType::Boolean),
            (NativeType::Blob, DataType::Binary),
            (NativeType::Counter, DataType::Int64),
            (NativeType::BigInt, DataType::Int64),
            (NativeType::Int, DataType::Int32),
            (NativeType::SmallInt, DataType::Int16),
            (NativeType::TinyInt, DataType::Int8),
            (NativeType::Double, DataType::Float64),
            (NativeType::Float, DataType::Float32),
            (NativeType::Date, DataType::Date32),
            (NativeType::Decimal, DataType::Utf8),
            (NativeType::Varint, DataType::Utf8),
            (NativeType::Uuid, DataType::Utf8),
            (NativeType::Timeuuid, DataType::Utf8),
            (NativeType::Inet, DataType::Utf8),
            (NativeType::Time, DataType::Time64(TimeUnit::Nanosecond)),
            (
                NativeType::Timestamp,
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                NativeType::Duration,
                DataType::Interval(IntervalUnit::MonthDayNano),
            ),
        ];
        for (cql, arrow) in cases {
            assert_eq!(
                map_column_type("c", &native(cql.clone())).unwrap(),
                arrow,
                "{cql:?}"
            );
        }
    }

    #[test]
    fn maps_composite_types() {
        assert_eq!(
            map_column_type("c", &list_of(native(NativeType::Int))).unwrap(),
            DataType::List(list_item_field(DataType::Int32))
        );

        let set = ColumnType::Collection {
            frozen: true,
            typ: CollectionType::Set(Box::new(native(NativeType::Text))),
        };
        assert_eq!(
            map_column_type("c", &set).unwrap(),
            DataType::List(list_item_field(DataType::Utf8))
        );

        let map = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(native(NativeType::Text)),
                Box::new(native(NativeType::BigInt)),
            ),
        };
        assert_eq!(
            map_column_type("c", &map).unwrap(),
            map_data_type(DataType::Utf8, DataType::Int64)
        );

        let vector = ColumnType::Vector {
            typ: Box::new(native(NativeType::Float)),
            dimensions: 3,
        };
        assert_eq!(
            map_column_type("c", &vector).unwrap(),
            DataType::FixedSizeList(list_item_field(DataType::Float32), 3)
        );

        let tuple = ColumnType::Tuple(vec![native(NativeType::Int), native(NativeType::Text)]);
        assert_eq!(
            map_column_type("c", &tuple).unwrap(),
            DataType::Struct(Fields::from(vec![
                Field::new("0", DataType::Int32, true),
                Field::new("1", DataType::Utf8, true),
            ]))
        );

        let udt = ColumnType::UserDefinedType {
            frozen: true,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("address"),
                keyspace: Cow::Borrowed("ks"),
                field_types: vec![
                    (Cow::Borrowed("street"), native(NativeType::Text)),
                    (Cow::Borrowed("zip"), native(NativeType::Int)),
                ],
            }),
        };
        assert_eq!(
            map_column_type("c", &udt).unwrap(),
            DataType::Struct(Fields::from(vec![
                Field::new("street", DataType::Utf8, true),
                Field::new("zip", DataType::Int32, true),
            ]))
        );
    }

    #[test]
    fn builds_schema_from_column_specs() {
        let table = TableSpec::borrowed("ks", "t");
        let specs = [
            ColumnSpec::borrowed("id", native(NativeType::Int), table.clone()),
            ColumnSpec::borrowed("tags", list_of(native(NativeType::Text)), table),
        ];
        let schema = column_specs_to_schema(&specs).unwrap();
        assert_eq!(
            schema.as_ref(),
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new(
                    "tags",
                    DataType::List(list_item_field(DataType::Utf8)),
                    true
                ),
            ])
        );
    }

    #[test]
    fn converts_scalar_values() {
        let text = CqlValue::Text("hello".to_string());
        let array = values_to_array(&DataType::Utf8, &[Some(&text), None]).unwrap();
        let array = array.as_string::<i32>();
        assert_eq!(array.value(0), "hello");
        assert!(array.is_null(1));

        let values = [
            CqlValue::TinyInt(1),
            CqlValue::SmallInt(2),
            CqlValue::Int(3),
            CqlValue::BigInt(4),
            CqlValue::Counter(Counter(5)),
        ];
        let refs: Vec<_> = values.iter().map(Some).collect();
        let array = values_to_array(&DataType::Int64, &refs).unwrap();
        assert_eq!(
            array.as_primitive::<arrow::datatypes::Int64Type>().values(),
            &[1, 2, 3, 4, 5]
        );

        // a value that doesn't fit into the target type is an error, not a silent truncation
        let big = CqlValue::BigInt(i64::MAX);
        assert!(values_to_array(&DataType::Int32, &[Some(&big)]).is_err());

        // the CQL "empty" value is read as null
        let array = values_to_array(&DataType::Int32, &[Some(&CqlValue::Empty)]).unwrap();
        assert!(array.is_null(0));
    }

    #[test]
    fn converts_temporal_values() {
        // 1970-01-02 is one day after the Unix epoch
        let date = CqlValue::Date(CqlDate((1 << 31) + 1));
        let array = values_to_array(&DataType::Date32, &[Some(&date)]).unwrap();
        assert_eq!(
            array
                .as_primitive::<arrow::datatypes::Date32Type>()
                .value(0),
            1
        );

        let ts = CqlValue::Timestamp(CqlTimestamp(1_500));
        let utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let array = values_to_array(&utc, &[Some(&ts)]).unwrap();
        assert_eq!(array.data_type(), &utc);
        assert_eq!(
            array
                .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                .value(0),
            1_500
        );
        let array = values_to_array(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &[Some(&ts)],
        )
        .unwrap();
        assert_eq!(
            array
                .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                .value(0),
            1_500_000
        );

        let time = CqlValue::Time(CqlTime(42_000));
        let array =
            values_to_array(&DataType::Time64(TimeUnit::Nanosecond), &[Some(&time)]).unwrap();
        assert_eq!(
            array
                .as_primitive::<arrow::datatypes::Time64NanosecondType>()
                .value(0),
            42_000
        );

        let duration = CqlValue::Duration(CqlDuration {
            months: 1,
            days: 2,
            nanoseconds: 3,
        });
        let array = values_to_array(
            &DataType::Interval(IntervalUnit::MonthDayNano),
            &[Some(&duration)],
        )
        .unwrap();
        assert_eq!(
            array
                .as_primitive::<arrow::datatypes::IntervalMonthDayNanoType>()
                .value(0),
            IntervalMonthDayNano::new(1, 2, 3)
        );
    }

    #[test]
    fn converts_arbitrary_precision_numbers() {
        // 12345 * 10^-2
        let decimal = CqlValue::Decimal(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
            &12345i32.to_be_bytes(),
            2,
        ));
        let varint = CqlValue::Varint(CqlVarint::from_signed_bytes_be_slice(
            &(-42i64).to_be_bytes(),
        ));

        let array = values_to_array(&DataType::Utf8, &[Some(&decimal), Some(&varint)]).unwrap();
        let array = array.as_string::<i32>();
        assert_eq!(array.value(0), "123.45");
        assert_eq!(array.value(1), "-42");

        let array = values_to_array(
            &DataType::Decimal128(10, 3),
            &[Some(&decimal), Some(&varint)],
        )
        .unwrap();
        let array = array.as_primitive::<arrow::datatypes::Decimal128Type>();
        assert_eq!(array.value(0), 123_450);
        assert_eq!(array.value(1), -42_000);
    }

    #[test]
    fn converts_nested_values() {
        let list = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]);
        let empty = CqlValue::Set(vec![]);
        let data_type = DataType::List(list_item_field(DataType::Int32));
        let array = values_to_array(&data_type, &[Some(&list), None, Some(&empty)]).unwrap();
        let array = array.as_list::<i32>();
        assert_eq!(array.len(), 3);
        assert!(array.is_null(1));
        assert_eq!(array.value(0).as_primitive::<Int32Type>().values(), &[1, 2]);
        assert_eq!(array.value(2).len(), 0);

        let map = CqlValue::Map(vec![(CqlValue::Text("a".into()), CqlValue::BigInt(1))]);
        let array = values_to_array(
            &map_data_type(DataType::Utf8, DataType::Int64),
            &[Some(&map)],
        )
        .unwrap();
        let array = array.as_map();
        assert_eq!(array.keys().as_string::<i32>().value(0), "a");
        assert_eq!(
            array
                .values()
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0),
            1
        );

        let udt = CqlValue::UserDefinedType {
            keyspace: "ks".into(),
            name: "address".into(),
            fields: vec![
                ("street".into(), Some(CqlValue::Text("Main".into()))),
                ("zip".into(), None),
            ],
        };
        let fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("zip", DataType::Int32, true),
        ]);
        let array = values_to_array(&DataType::Struct(fields), &[Some(&udt), None]).unwrap();
        let array = array.as_struct();
        assert!(array.is_null(1));
        assert_eq!(array.column(0).as_string::<i32>().value(0), "Main");
        assert!(array.column(1).is_null(0));

        let tuple = CqlValue::Tuple(vec![Some(CqlValue::Int(7)), None]);
        let fields = Fields::from(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Utf8, true),
        ]);
        let array = values_to_array(&DataType::Struct(fields), &[Some(&tuple)]).unwrap();
        let array = array.as_struct();
        assert_eq!(array.column(0).as_primitive::<Int32Type>().value(0), 7);
        assert!(array.column(1).is_null(0));

        let vector = CqlValue::Vector(vec![CqlValue::Float(1.0), CqlValue::Float(2.0)]);
        let data_type = DataType::FixedSizeList(list_item_field(DataType::Float32), 2);
        let array = values_to_array(&data_type, &[Some(&vector), None]).unwrap();
        let array = array.as_fixed_size_list();
        assert!(array.is_null(1));
        assert_eq!(
            array
                .value(0)
                .as_primitive::<arrow::datatypes::Float32Type>()
                .values(),
            &[1.0, 2.0]
        );
    }

    #[test]
    fn builds_record_batch_by_column_name() {
        let rows = vec![
            Row {
                columns: vec![Some(CqlValue::Int(1)), Some(CqlValue::Text("a".into()))],
            },
            Row {
                columns: vec![Some(CqlValue::Int(2)), None],
            },
        ];
        let column_names = vec!["id".to_string(), "name".to_string()];

        // the schema may list the columns in a different order than the result set
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ]));
        let batch = rows_to_record_batch(&schema, &column_names, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "a");
        assert!(batch.column(0).is_null(1));
        assert_eq!(
            batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[1, 2]
        );

        // an empty schema keeps the row count, e.g. for `SELECT count(*)`
        let batch = rows_to_record_batch(&Arc::new(Schema::empty()), &column_names, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 0);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "missing",
            DataType::Int32,
            true,
        )]));
        assert!(rows_to_record_batch(&schema, &column_names, &rows).is_err());

        // a type that the values can't be converted to is an error
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Boolean,
            true,
        )]));
        assert!(rows_to_record_batch(&schema, &column_names, &rows).is_err());
    }

    fn write(array: ArrayRef, typ: &ColumnType<'_>) -> Result<Vec<Option<CqlValue>>> {
        array_to_cql_values("c", &array, typ)
    }

    fn map_of(key: ColumnType<'static>, value: ColumnType<'static>) -> ColumnType<'static> {
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(Box::new(key), Box::new(value)),
        }
    }

    fn address_udt() -> ColumnType<'static> {
        ColumnType::UserDefinedType {
            frozen: true,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("address"),
                keyspace: Cow::Borrowed("ks"),
                field_types: vec![
                    (Cow::Borrowed("street"), native(NativeType::Text)),
                    (Cow::Borrowed("zip"), native(NativeType::Int)),
                ],
            }),
        }
    }

    /// Converting CQL values to Arrow and back must give the original values.
    #[test]
    fn writes_what_it_reads() {
        let uuid = Uuid::parse_str("8f2bfa59-4e30-4b0a-8b9a-3c1e26f9a0b1").unwrap();
        let timeuuid = Uuid::parse_str("e9c0b2a0-6c1e-11ee-8c99-0242ac120002").unwrap();
        let cases = vec![
            (native(NativeType::Ascii), CqlValue::Ascii("abc".into())),
            (native(NativeType::Text), CqlValue::Text("héllo".into())),
            (native(NativeType::Boolean), CqlValue::Boolean(true)),
            (native(NativeType::Blob), CqlValue::Blob(vec![1, 2, 3])),
            (native(NativeType::BigInt), CqlValue::BigInt(i64::MIN)),
            (native(NativeType::Int), CqlValue::Int(-7)),
            (native(NativeType::SmallInt), CqlValue::SmallInt(300)),
            (native(NativeType::TinyInt), CqlValue::TinyInt(-3)),
            (native(NativeType::Double), CqlValue::Double(1.5)),
            (native(NativeType::Float), CqlValue::Float(2.5)),
            (
                native(NativeType::Date),
                CqlValue::Date(CqlDate((1 << 31) + 19_000)),
            ),
            (
                native(NativeType::Timestamp),
                CqlValue::Timestamp(CqlTimestamp(1_700_000_000_123)),
            ),
            (native(NativeType::Time), CqlValue::Time(CqlTime(42_000))),
            (
                native(NativeType::Duration),
                CqlValue::Duration(CqlDuration {
                    months: 1,
                    days: 2,
                    nanoseconds: 3,
                }),
            ),
            (
                native(NativeType::Decimal),
                CqlValue::Decimal(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
                    &[0x30, 0x39],
                    2,
                )),
            ),
            (
                native(NativeType::Varint),
                CqlValue::Varint(CqlVarint::from_signed_bytes_be_slice(&[0x01, 0x00, 0x00])),
            ),
            (native(NativeType::Uuid), CqlValue::Uuid(uuid)),
            (
                native(NativeType::Timeuuid),
                CqlValue::Timeuuid(CqlTimeuuid::from(timeuuid)),
            ),
            (
                native(NativeType::Inet),
                CqlValue::Inet("2001:db8::1".parse().unwrap()),
            ),
            (
                list_of(native(NativeType::Int)),
                CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]),
            ),
            (
                ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Set(Box::new(native(NativeType::Text))),
                },
                CqlValue::Set(vec![CqlValue::Text("a".into())]),
            ),
            (
                map_of(native(NativeType::Text), native(NativeType::BigInt)),
                CqlValue::Map(vec![(CqlValue::Text("a".into()), CqlValue::BigInt(1))]),
            ),
            (
                ColumnType::Vector {
                    typ: Box::new(native(NativeType::Float)),
                    dimensions: 2,
                },
                CqlValue::Vector(vec![CqlValue::Float(1.0), CqlValue::Float(2.0)]),
            ),
            (
                address_udt(),
                CqlValue::UserDefinedType {
                    keyspace: "ks".into(),
                    name: "address".into(),
                    fields: vec![
                        ("street".into(), Some(CqlValue::Text("Main".into()))),
                        ("zip".into(), None),
                    ],
                },
            ),
            (
                ColumnType::Tuple(vec![native(NativeType::Int), native(NativeType::Text)]),
                CqlValue::Tuple(vec![Some(CqlValue::Int(7)), None]),
            ),
        ];

        for (typ, value) in cases {
            let data_type = map_column_type("c", &typ).unwrap();
            let array = values_to_array(&data_type, &[Some(&value), None]).unwrap();
            let written = write(array, &typ).unwrap();
            assert_eq!(written, vec![Some(value), None], "CQL type {typ:?}");
        }
    }

    #[test]
    fn casts_to_the_column_type() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        assert_eq!(
            write(Arc::clone(&ints), &native(NativeType::BigInt)).unwrap(),
            vec![Some(CqlValue::BigInt(1)), None]
        );
        assert_eq!(
            write(ints, &native(NativeType::Varint)).unwrap(),
            vec![
                Some(CqlValue::Varint(CqlVarint::from(BigInt::from(1)))),
                None
            ]
        );

        let large_strings: ArrayRef = Arc::new(LargeStringArray::from(vec!["x"]));
        assert_eq!(
            write(large_strings, &native(NativeType::Text)).unwrap(),
            vec![Some(CqlValue::Text("x".into()))]
        );

        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![12345])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        assert_eq!(
            write(decimals, &native(NativeType::Decimal)).unwrap(),
            vec![Some(CqlValue::Decimal(
                CqlDecimal::from_signed_be_bytes_slice_and_exponent(&[0x30, 0x39], 2)
            ))]
        );
    }

    #[test]
    fn keeps_the_instant_of_timestamps() {
        let timestamps: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![1_500_000]).with_timezone("+02:00"));
        assert_eq!(
            write(timestamps, &native(NativeType::Timestamp)).unwrap(),
            vec![Some(CqlValue::Timestamp(CqlTimestamp(1_500)))]
        );
    }

    #[test]
    fn writes_uuids_from_bytes() {
        let uuid = Uuid::parse_str("8f2bfa59-4e30-4b0a-8b9a-3c1e26f9a0b1").unwrap();
        let bytes: ArrayRef = Arc::new(
            arrow::array::FixedSizeBinaryArray::try_from_iter(std::iter::once(uuid.as_bytes()))
                .unwrap(),
        );
        assert_eq!(
            write(bytes, &native(NativeType::Uuid)).unwrap(),
            vec![Some(CqlValue::Uuid(uuid))]
        );
    }

    #[test]
    fn fails_for_invalid_values() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["not a uuid"]));
        let err = write(strings, &native(NativeType::Uuid)).unwrap_err();
        assert!(err.to_string().contains("is not a UUID"), "{err}");

        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let err = write(ints, &native(NativeType::Counter)).unwrap_err();
        assert!(err.to_string().contains("counter"), "{err}");

        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let err = write(strings, &list_of(native(NativeType::Text))).unwrap_err();
        assert!(err.to_string().contains("can't convert Utf8"), "{err}");
    }

    #[test]
    fn fails_for_nulls_in_collections() {
        let list = values_to_array(
            &DataType::List(list_item_field(DataType::Int32)),
            &[Some(&CqlValue::List(vec![CqlValue::Int(1)]))],
        )
        .unwrap();
        // Replace the element with a null
        let list = list.as_list::<i32>();
        let list: ArrayRef = Arc::new(ListArray::new(
            list_item_field(DataType::Int32),
            list.offsets().clone(),
            Arc::new(Int32Array::from(vec![None::<i32>])),
            None,
        ));
        let err = write(list, &list_of(native(NativeType::Int))).unwrap_err();
        assert!(matches!(err, Error::NullInCollection { .. }), "{err}");
    }

    #[test]
    fn fails_for_struct_fields_missing_from_the_udt() {
        let array: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["Sofia"])) as ArrayRef,
        )]));
        let err = write(array, &address_udt()).unwrap_err();
        assert!(
            err.to_string()
                .contains("'city' is not a field of the user-defined type ks.address"),
            "{err}"
        );
    }
}
