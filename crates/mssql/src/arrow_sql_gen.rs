//! Conversions between SQL Server values (as decoded by `tiberius`) and Arrow.

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Decimal128Builder,
    Decimal256Builder, Decimal32Builder, Decimal64Builder, FixedSizeBinaryBuilder,
    LargeBinaryBuilder, LargeStringBuilder, NullArray, PrimitiveBuilder, RecordBatch,
    RecordBatchOptions, StringBuilder, StringViewBuilder,
};
use datafusion::arrow::datatypes::{
    i256, ArrowPrimitiveType, DataType, Date32Type, Date64Type, Field, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema, SchemaRef,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use snafu::prelude::*;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime2, DateTimeOffset, Time};
use tiberius::{ColumnData, ColumnType, Row};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Cannot convert the SQL Server value {value} of column '{column}' to the Arrow type {data_type}"
    ))]
    UnsupportedConversion {
        column: String,
        value: String,
        data_type: DataType,
    },

    #[snafu(display(
        "The value {value} of column '{column}' is out of range for the Arrow type {data_type}"
    ))]
    ValueOutOfRange {
        column: String,
        value: String,
        data_type: DataType,
    },

    #[snafu(display("The Arrow type {data_type} of column '{column}' is not supported by the SQL Server table provider"))]
    UnsupportedArrowType { column: String, data_type: DataType },

    #[snafu(display("The SQL Server type '{sql_type}' of column '{column}' is not supported"))]
    UnsupportedSqlServerType { column: String, sql_type: String },

    #[snafu(display("Failed to build an Arrow record batch: {source}"))]
    FailedToBuildRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Days from 0001-01-01 (the SQL Server `date` epoch) to 1970-01-01.
const DAYS_FROM_CE_TO_UNIX_EPOCH: i64 = 719_162;
/// Days from 1900-01-01 (the SQL Server `datetime` epoch) to 1970-01-01.
const DAYS_FROM_1900_TO_UNIX_EPOCH: i64 = 25_567;
const NANOS_PER_DAY: i128 = 86_400 * 1_000_000_000;
/// The scale of `time`, `datetime2` and `datetimeoffset` values sent to SQL Server.
const SQL_SERVER_TIME_SCALE: u8 = 7;
/// The scale of `money` and `smallmoney`.
const MONEY_SCALE: i8 = 4;

/// Maps a SQL Server column type, as reported by `sys.columns`, to an Arrow type.
pub fn map_sql_server_type_to_arrow(
    column: &str,
    sql_type: &str,
    precision: u8,
    scale: u8,
) -> Result<DataType> {
    let data_type = match sql_type.to_ascii_lowercase().as_str() {
        "bit" => DataType::Boolean,
        "tinyint" => DataType::UInt8,
        "smallint" => DataType::Int16,
        "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "float" => DataType::Float64,
        "decimal" | "numeric" => DataType::Decimal128(precision, scale as i8),
        "money" => DataType::Decimal128(19, MONEY_SCALE),
        "smallmoney" => DataType::Decimal128(10, MONEY_SCALE),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "sysname" | "xml"
        | "uniqueidentifier" => DataType::Utf8,
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => DataType::Binary,
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Nanosecond),
        // Microsecond precision keeps the whole 0001-9999 range of `datetime2` representable
        "datetime" | "smalldatetime" | "datetime2" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "datetimeoffset" => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
        _ => UnsupportedSqlServerTypeSnafu { column, sql_type }.fail()?,
    };
    Ok(data_type)
}

/// Maps an Arrow type to the SQL Server column type used to store it.
///
/// `key` must be `true` for columns that are part of a primary key or an index, because
/// SQL Server can't index `(n)varchar(max)`/`varbinary(max)` columns.
pub fn map_arrow_type_to_sql_server(
    column: &str,
    data_type: &DataType,
    key: bool,
) -> Result<String> {
    let sql_type = match data_type {
        DataType::Boolean => "BIT".to_string(),
        DataType::Int8 | DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt16 => "INT".to_string(),
        DataType::Int64 | DataType::UInt32 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT".to_string(),
        DataType::UInt64 => "DECIMAL(20, 0)".to_string(),
        DataType::Float16 | DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "FLOAT".to_string(),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s)
            if *p <= 38 && *s >= 0 =>
        {
            format!("DECIMAL({p}, {s})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => if key {
            "NVARCHAR(450)"
        } else {
            "NVARCHAR(MAX)"
        }
        .to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => if key {
            "VARBINARY(900)"
        } else {
            "VARBINARY(MAX)"
        }
        .to_string(),
        DataType::FixedSizeBinary(n) if *n > 0 && *n <= 8000 => format!("BINARY({n})"),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME(7)".to_string(),
        DataType::Timestamp(_, None) => "DATETIME2(7)".to_string(),
        DataType::Timestamp(_, Some(_)) => "DATETIMEOFFSET(7)".to_string(),
        _ => UnsupportedArrowTypeSnafu {
            column,
            data_type: data_type.clone(),
        }
        .fail()?,
    };
    Ok(sql_type)
}

/// Infers the Arrow type of a result column from the values in its first rows, or from the
/// column type if they are all `NULL`.
fn infer_arrow_type<'a>(
    column_type: ColumnType,
    mut values: impl Iterator<Item = &'a ColumnData<'static>>,
) -> DataType {
    let first = values.find(|v| !is_null(v));
    match first {
        Some(ColumnData::U8(_)) => DataType::UInt8,
        Some(ColumnData::I16(_)) => DataType::Int16,
        Some(ColumnData::I32(_)) => DataType::Int32,
        Some(ColumnData::I64(_)) => DataType::Int64,
        Some(ColumnData::F32(_)) => DataType::Float32,
        Some(ColumnData::F64(_))
            if matches!(column_type, ColumnType::Money | ColumnType::Money4) =>
        {
            DataType::Decimal128(19, MONEY_SCALE)
        }
        Some(ColumnData::F64(_)) => DataType::Float64,
        Some(ColumnData::Bit(_)) => DataType::Boolean,
        Some(ColumnData::String(_) | ColumnData::Guid(_) | ColumnData::Xml(_)) => DataType::Utf8,
        Some(ColumnData::Binary(_)) => DataType::Binary,
        Some(ColumnData::Numeric(Some(n))) => DataType::Decimal128(38, n.scale() as i8),
        Some(ColumnData::Date(_)) => DataType::Date32,
        Some(ColumnData::Time(_)) => DataType::Time64(TimeUnit::Nanosecond),
        Some(ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        Some(ColumnData::DateTimeOffset(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00")))
        }
        _ => infer_arrow_type_from_column_type(column_type),
    }
}

fn infer_arrow_type_from_column_type(column_type: ColumnType) -> DataType {
    match column_type {
        ColumnType::Bit | ColumnType::Bitn => DataType::Boolean,
        ColumnType::Int1 => DataType::UInt8,
        ColumnType::Int2 => DataType::Int16,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 | ColumnType::Intn => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 | ColumnType::Floatn => DataType::Float64,
        ColumnType::Money | ColumnType::Money4 => DataType::Decimal128(19, MONEY_SCALE),
        ColumnType::Decimaln | ColumnType::Numericn => DataType::Decimal128(38, 10),
        ColumnType::Daten => DataType::Date32,
        ColumnType::Timen => DataType::Time64(TimeUnit::Nanosecond),
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::DatetimeOffsetn => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00")))
        }
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => DataType::Binary,
        ColumnType::Null => DataType::Null,
        _ => DataType::Utf8,
    }
}

fn is_null(value: &ColumnData<'_>) -> bool {
    match value {
        ColumnData::U8(v) => v.is_none(),
        ColumnData::I16(v) => v.is_none(),
        ColumnData::I32(v) => v.is_none(),
        ColumnData::I64(v) => v.is_none(),
        ColumnData::F32(v) => v.is_none(),
        ColumnData::F64(v) => v.is_none(),
        ColumnData::Bit(v) => v.is_none(),
        ColumnData::String(v) => v.is_none(),
        ColumnData::Guid(v) => v.is_none(),
        ColumnData::Binary(v) => v.is_none(),
        ColumnData::Numeric(v) => v.is_none(),
        ColumnData::Xml(v) => v.is_none(),
        ColumnData::DateTime(v) => v.is_none(),
        ColumnData::SmallDateTime(v) => v.is_none(),
        ColumnData::Time(v) => v.is_none(),
        ColumnData::Date(v) => v.is_none(),
        ColumnData::DateTime2(v) => v.is_none(),
        ColumnData::DateTimeOffset(v) => v.is_none(),
    }
}

/// Maps the result columns to the fields of `schema`: by position when the names match,
/// otherwise by name.
fn map_columns(result_columns: &[&str], schema: &Schema) -> Vec<Option<usize>> {
    let same_count = result_columns.len() == schema.fields().len();
    let same_names = same_count
        && result_columns
            .iter()
            .zip(schema.fields())
            .all(|(c, f)| c.eq_ignore_ascii_case(f.name()));
    if same_names {
        return (0..result_columns.len()).map(Some).collect();
    }

    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            result_columns
                .iter()
                .position(|c| c == field.name())
                .or_else(|| {
                    result_columns
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(field.name()))
                })
                // e.g. unnamed expressions, or names changed by the federated query
                .or_else(|| same_count.then_some(i))
        })
        .collect()
}

/// Infers the schema of a result from its first rows.
pub fn infer_schema(rows: &[Row]) -> SchemaRef {
    let Some(first) = rows.first() else {
        return Arc::new(Schema::empty());
    };
    let fields = first
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let data_type = infer_arrow_type(
                column.column_type(),
                rows.iter().filter_map(|row| row.get_column_data(i).ok()),
            );
            Field::new(column.name(), data_type, true)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Converts the rows of a result to a record batch with the given schema.
pub fn rows_to_arrow(rows: &[Row], schema: &SchemaRef) -> Result<RecordBatch> {
    let Some(first) = rows.first() else {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    };
    let names = first.columns().iter().map(|c| c.name()).collect::<Vec<_>>();
    let mapping = map_columns(&names, schema);

    let mut arrays = Vec::with_capacity(schema.fields().len());
    for (field, index) in schema.fields().iter().zip(mapping) {
        let array = match index {
            Some(index) => {
                let values = rows
                    .iter()
                    .map(|row| row.get_column_data(index).ok())
                    .collect::<Vec<_>>();
                build_array(field.name(), field.data_type(), &values)?
            }
            // A column the query doesn't return can only be all NULL
            None => datafusion::arrow::array::new_null_array(field.data_type(), rows.len()),
        };
        arrays.push(array);
    }

    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .context(FailedToBuildRecordBatchSnafu)
}

fn unsupported(column: &str, value: &ColumnData<'_>, data_type: &DataType) -> Error {
    Error::UnsupportedConversion {
        column: column.to_string(),
        value: format!("{value:?}"),
        data_type: data_type.clone(),
    }
}

fn out_of_range(column: &str, value: impl std::fmt::Display, data_type: &DataType) -> Error {
    Error::ValueOutOfRange {
        column: column.to_string(),
        value: value.to_string(),
        data_type: data_type.clone(),
    }
}

fn to_i128(value: &ColumnData<'_>) -> Option<Option<i128>> {
    Some(match value {
        ColumnData::U8(v) => v.map(i128::from),
        ColumnData::I16(v) => v.map(i128::from),
        ColumnData::I32(v) => v.map(i128::from),
        ColumnData::I64(v) => v.map(i128::from),
        ColumnData::Bit(v) => v.map(i128::from),
        ColumnData::Numeric(v) => match v {
            Some(n) if n.scale() == 0 => Some(n.value()),
            Some(_) => return None,
            None => None,
        },
        _ if is_null(value) => None,
        _ => return None,
    })
}

#[allow(clippy::cast_precision_loss)]
fn to_f64(value: &ColumnData<'_>) -> Option<Option<f64>> {
    Some(match value {
        ColumnData::F32(v) => v.map(f64::from),
        ColumnData::F64(v) => *v,
        ColumnData::Numeric(v) => v.map(f64::from),
        _ => to_i128(value)?.map(|i| i as f64),
    })
}

fn pow10(exp: u32) -> Option<i128> {
    10_i128.checked_pow(exp)
}

/// Returns the value scaled by `10^scale`, rounding half away from zero.
#[allow(clippy::cast_possible_truncation)]
fn to_decimal(value: &ColumnData<'_>, scale: i8) -> Option<Option<i128>> {
    let rescale = |unscaled: i128, from: i8| -> Option<i128> {
        let diff = i32::from(scale) - i32::from(from);
        if diff >= 0 {
            unscaled.checked_mul(pow10(diff.unsigned_abs())?)
        } else {
            let divisor = pow10(diff.unsigned_abs())?;
            let quotient = unscaled / divisor;
            let remainder = unscaled % divisor;
            Some(if remainder.abs() * 2 >= divisor {
                quotient + unscaled.signum()
            } else {
                quotient
            })
        }
    };
    Some(match value {
        ColumnData::Numeric(Some(n)) => Some(rescale(n.value(), n.scale() as i8)?),
        // money and smallmoney are decoded as f64
        ColumnData::F64(Some(f)) => {
            let scaled = (f * 10_f64.powi(i32::from(scale))).round();
            if !scaled.is_finite() || scaled.abs() >= 1e38 {
                return None;
            }
            Some(scaled as i128)
        }
        ColumnData::F32(Some(f)) => {
            let scaled = (f64::from(*f) * 10_f64.powi(i32::from(scale))).round();
            if !scaled.is_finite() || scaled.abs() >= 1e38 {
                return None;
            }
            Some(scaled as i128)
        }
        _ => match to_i128(value)? {
            Some(i) => Some(rescale(i, 0)?),
            None => None,
        },
    })
}

fn time_nanos(time: Time) -> i64 {
    let factor = 10_i64.pow(9_u32.saturating_sub(u32::from(time.scale())));
    #[allow(clippy::cast_possible_wrap)]
    let increments = time.increments() as i64;
    increments * factor
}

fn date_epoch_days(date: Date) -> i64 {
    i64::from(date.days()) - DAYS_FROM_CE_TO_UNIX_EPOCH
}

fn datetime2_epoch_nanos(dt: DateTime2) -> i128 {
    i128::from(date_epoch_days(dt.date())) * NANOS_PER_DAY + i128::from(time_nanos(dt.time()))
}

/// The nanoseconds since the Unix epoch of a date/time value.
/// `datetimeoffset` values are converted to UTC.
fn to_epoch_nanos(value: &ColumnData<'_>) -> Option<Option<i128>> {
    Some(match value {
        ColumnData::DateTime2(v) => v.map(datetime2_epoch_nanos),
        // The date and time of a datetimeoffset are in UTC
        ColumnData::DateTimeOffset(v) => v.map(|dto| datetime2_epoch_nanos(dto.datetime2())),
        ColumnData::DateTime(v) => v.map(|dt| {
            let days = i128::from(dt.days()) - i128::from(DAYS_FROM_1900_TO_UNIX_EPOCH);
            // in 1/300 seconds
            let nanos = i128::from(dt.seconds_fragments()) * 1_000_000_000 / 300;
            days * NANOS_PER_DAY + nanos
        }),
        ColumnData::SmallDateTime(v) => v.map(|dt| {
            let days = i128::from(dt.days()) - i128::from(DAYS_FROM_1900_TO_UNIX_EPOCH);
            // in minutes
            let nanos = i128::from(dt.seconds_fragments()) * 60 * 1_000_000_000;
            days * NANOS_PER_DAY + nanos
        }),
        ColumnData::Date(v) => v.map(|d| i128::from(date_epoch_days(d)) * NANOS_PER_DAY),
        _ if is_null(value) => None,
        _ => return None,
    })
}

fn to_time_nanos(value: &ColumnData<'_>) -> Option<Option<i64>> {
    Some(match value {
        ColumnData::Time(v) => v.map(time_nanos),
        ColumnData::DateTime2(v) => v.map(|dt| time_nanos(dt.time())),
        _ if is_null(value) => None,
        _ => return None,
    })
}

fn to_string(value: &ColumnData<'_>) -> Option<Option<String>> {
    Some(match value {
        ColumnData::String(v) => v.as_ref().map(|s| s.to_string()),
        ColumnData::Guid(v) => v.map(|g| g.hyphenated().to_string().to_uppercase()),
        ColumnData::Xml(v) => v.as_ref().map(|x| x.to_string()),
        ColumnData::F32(v) => v.map(|f| f.to_string()),
        ColumnData::F64(v) => v.map(|f| f.to_string()),
        ColumnData::Numeric(v) => v.map(|n| n.to_string()),
        _ => to_i128(value)?.map(|i| i.to_string()),
    })
}

fn to_bytes<'a>(value: &'a ColumnData<'_>) -> Option<Option<&'a [u8]>> {
    Some(match value {
        ColumnData::Binary(v) => v.as_deref(),
        ColumnData::String(v) => v.as_deref().map(str::as_bytes),
        _ if is_null(value) => None,
        _ => return None,
    })
}

fn build_integers<T>(
    column: &str,
    data_type: &DataType,
    values: &[Option<&ColumnData<'static>>],
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i128>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for value in values {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        match to_i128(value).ok_or_else(|| unsupported(column, value, data_type))? {
            Some(i) => builder.append_value(
                T::Native::try_from(i).map_err(|_| out_of_range(column, i, data_type))?,
            ),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_primitives<T>(
    values: &[Option<&ColumnData<'static>>],
    mut convert: impl FnMut(&ColumnData<'static>) -> Result<Option<T::Native>>,
) -> Result<PrimitiveBuilder<T>>
where
    T: ArrowPrimitiveType,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => builder.append_option(convert(value)?),
            None => builder.append_null(),
        }
    }
    Ok(builder)
}

/// Builds an array of epoch-based values (timestamps or dates) in units of `nanos_per_unit`.
fn epoch_values<T>(
    column: &str,
    data_type: &DataType,
    values: &[Option<&ColumnData<'static>>],
    nanos_per_unit: i128,
) -> Result<PrimitiveBuilder<T>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i128>,
{
    build_primitives::<T>(values, |value| {
        match to_epoch_nanos(value).ok_or_else(|| unsupported(column, value, data_type))? {
            Some(nanos) => {
                let units = nanos.div_euclid(nanos_per_unit);
                T::Native::try_from(units)
                    .map(Some)
                    .map_err(|_| out_of_range(column, format!("{value:?}"), data_type))
            }
            None => Ok(None),
        }
    })
}

fn time_values<T>(
    column: &str,
    data_type: &DataType,
    values: &[Option<&ColumnData<'static>>],
    nanos_per_unit: i64,
) -> Result<PrimitiveBuilder<T>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i64>,
{
    build_primitives::<T>(values, |value| {
        match to_time_nanos(value).ok_or_else(|| unsupported(column, value, data_type))? {
            Some(nanos) => T::Native::try_from(nanos / nanos_per_unit)
                .map(Some)
                .map_err(|_| out_of_range(column, nanos, data_type)),
            None => Ok(None),
        }
    })
}

fn decimal_values(
    column: &str,
    data_type: &DataType,
    values: &[Option<&ColumnData<'static>>],
    scale: i8,
) -> Result<Vec<Option<i128>>> {
    values
        .iter()
        .map(|value| match value {
            Some(value) => to_decimal(value, scale).ok_or_else(|| {
                if is_null(value) {
                    unsupported(column, value, data_type)
                } else {
                    out_of_range(column, format!("{value:?}"), data_type)
                }
            }),
            None => Ok(None),
        })
        .collect()
}

fn check_decimal_precision(
    column: &str,
    data_type: &DataType,
    value: i128,
    precision: u8,
) -> Result<()> {
    let max = pow10(u32::from(precision)).unwrap_or(i128::MAX);
    if value.abs() >= max {
        return Err(out_of_range(column, value, data_type));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_array(
    column: &str,
    data_type: &DataType,
    values: &[Option<&ColumnData<'static>>],
) -> Result<ArrayRef> {
    let n = values.len();
    let array: ArrayRef = match data_type {
        DataType::Null => Arc::new(NullArray::new(n)),
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(n);
            for value in values {
                let Some(value) = value else {
                    builder.append_null();
                    continue;
                };
                match to_i128(value).ok_or_else(|| unsupported(column, value, data_type))? {
                    Some(i) => builder.append_value(i != 0),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int8 => build_integers::<Int8Type>(column, data_type, values)?,
        DataType::Int16 => build_integers::<Int16Type>(column, data_type, values)?,
        DataType::Int32 => build_integers::<Int32Type>(column, data_type, values)?,
        DataType::Int64 => build_integers::<Int64Type>(column, data_type, values)?,
        DataType::UInt8 => build_integers::<UInt8Type>(column, data_type, values)?,
        DataType::UInt16 => build_integers::<UInt16Type>(column, data_type, values)?,
        DataType::UInt32 => build_integers::<UInt32Type>(column, data_type, values)?,
        DataType::UInt64 => build_integers::<UInt64Type>(column, data_type, values)?,
        #[allow(clippy::cast_possible_truncation)]
        DataType::Float16 => Arc::new(
            build_primitives::<Float16Type>(values, |value| {
                to_f64(value)
                    .ok_or_else(|| unsupported(column, value, data_type))
                    .map(|v| v.map(<Float16Type as ArrowPrimitiveType>::Native::from_f64))
            })?
            .finish(),
        ),
        #[allow(clippy::cast_possible_truncation)]
        DataType::Float32 => Arc::new(
            build_primitives::<Float32Type>(values, |value| {
                to_f64(value)
                    .ok_or_else(|| unsupported(column, value, data_type))
                    .map(|v| v.map(|f| f as f32))
            })?
            .finish(),
        ),
        DataType::Float64 => Arc::new(
            build_primitives::<Float64Type>(values, |value| {
                to_f64(value).ok_or_else(|| unsupported(column, value, data_type))
            })?
            .finish(),
        ),
        DataType::Decimal32(p, s) => {
            let mut builder = Decimal32Builder::with_capacity(n);
            for value in decimal_values(column, data_type, values, *s)? {
                match value {
                    Some(v) => {
                        check_decimal_precision(column, data_type, v, *p)?;
                        builder.append_value(
                            i32::try_from(v).map_err(|_| out_of_range(column, v, data_type))?,
                        );
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*p, *s)
                    .context(FailedToBuildRecordBatchSnafu)?,
            )
        }
        DataType::Decimal64(p, s) => {
            let mut builder = Decimal64Builder::with_capacity(n);
            for value in decimal_values(column, data_type, values, *s)? {
                match value {
                    Some(v) => {
                        check_decimal_precision(column, data_type, v, *p)?;
                        builder.append_value(
                            i64::try_from(v).map_err(|_| out_of_range(column, v, data_type))?,
                        );
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*p, *s)
                    .context(FailedToBuildRecordBatchSnafu)?,
            )
        }
        DataType::Decimal128(p, s) => {
            let mut builder = Decimal128Builder::with_capacity(n);
            for value in decimal_values(column, data_type, values, *s)? {
                match value {
                    Some(v) => {
                        check_decimal_precision(column, data_type, v, *p)?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*p, *s)
                    .context(FailedToBuildRecordBatchSnafu)?,
            )
        }
        DataType::Decimal256(p, s) => {
            let mut builder = Decimal256Builder::with_capacity(n);
            for value in decimal_values(column, data_type, values, *s)? {
                builder.append_option(value.map(i256::from_i128));
            }
            Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*p, *s)
                    .context(FailedToBuildRecordBatchSnafu)?,
            )
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let strings = values
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        to_string(value).ok_or_else(|| unsupported(column, value, data_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            match data_type {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(n, n * 16);
                    strings
                        .iter()
                        .for_each(|s| builder.append_option(s.as_deref()));
                    Arc::new(builder.finish())
                }
                DataType::LargeUtf8 => {
                    let mut builder = LargeStringBuilder::with_capacity(n, n * 16);
                    strings
                        .iter()
                        .for_each(|s| builder.append_option(s.as_deref()));
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder = StringViewBuilder::with_capacity(n);
                    strings
                        .iter()
                        .for_each(|s| builder.append_option(s.as_deref()));
                    Arc::new(builder.finish())
                }
            }
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let bytes = values
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        to_bytes(value).ok_or_else(|| unsupported(column, value, data_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            match data_type {
                DataType::Binary => {
                    let mut builder = BinaryBuilder::with_capacity(n, n * 16);
                    bytes.iter().for_each(|b| builder.append_option(*b));
                    Arc::new(builder.finish())
                }
                DataType::LargeBinary => {
                    let mut builder = LargeBinaryBuilder::with_capacity(n, n * 16);
                    bytes.iter().for_each(|b| builder.append_option(*b));
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder = BinaryViewBuilder::with_capacity(n);
                    bytes.iter().for_each(|b| builder.append_option(*b));
                    Arc::new(builder.finish())
                }
            }
        }
        DataType::FixedSizeBinary(size) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(n, *size);
            for value in values {
                let bytes = match value {
                    Some(value) => {
                        to_bytes(value).ok_or_else(|| unsupported(column, value, data_type))?
                    }
                    None => None,
                };
                match bytes {
                    Some(b) => builder
                        .append_value(b)
                        .map_err(|_| out_of_range(column, format!("{b:?}"), data_type))?,
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Date32 => {
            Arc::new(epoch_values::<Date32Type>(column, data_type, values, NANOS_PER_DAY)?.finish())
        }
        DataType::Date64 => {
            Arc::new(epoch_values::<Date64Type>(column, data_type, values, 1_000_000)?.finish())
        }
        DataType::Time32(TimeUnit::Second) => Arc::new(
            time_values::<Time32SecondType>(column, data_type, values, 1_000_000_000)?.finish(),
        ),
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(
            time_values::<Time32MillisecondType>(column, data_type, values, 1_000_000)?.finish(),
        ),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(
            time_values::<Time64MicrosecondType>(column, data_type, values, 1_000)?.finish(),
        ),
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(time_values::<Time64NanosecondType>(column, data_type, values, 1)?.finish())
        }
        DataType::Timestamp(unit, tz) => match unit {
            TimeUnit::Second => Arc::new(
                epoch_values::<TimestampSecondType>(column, data_type, values, 1_000_000_000)?
                    .finish()
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Millisecond => Arc::new(
                epoch_values::<TimestampMillisecondType>(column, data_type, values, 1_000_000)?
                    .finish()
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Microsecond => Arc::new(
                epoch_values::<TimestampMicrosecondType>(column, data_type, values, 1_000)?
                    .finish()
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Nanosecond => Arc::new(
                epoch_values::<TimestampNanosecondType>(column, data_type, values, 1)?
                    .finish()
                    .with_timezone_opt(tz.clone()),
            ),
        },
        _ => UnsupportedArrowTypeSnafu {
            column,
            data_type: data_type.clone(),
        }
        .fail()?,
    };
    Ok(array)
}

fn nanos_per_unit(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

fn epoch_nanos_to_datetime2(column: &str, data_type: &DataType, nanos: i128) -> Result<DateTime2> {
    let days = nanos.div_euclid(NANOS_PER_DAY);
    let time_nanos = nanos.rem_euclid(NANOS_PER_DAY);
    let ce_days = days + i128::from(DAYS_FROM_CE_TO_UNIX_EPOCH);
    // 9999-12-31 is the last date SQL Server supports
    if !(0..3_652_059).contains(&ce_days) {
        return Err(out_of_range(column, nanos, data_type));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(DateTime2::new(
        Date::new(ce_days as u32),
        Time::new((time_nanos / 100) as u64, SQL_SERVER_TIME_SCALE),
    ))
}

fn epoch_days_to_date(column: &str, data_type: &DataType, days: i64) -> Result<Date> {
    let ce_days = days + DAYS_FROM_CE_TO_UNIX_EPOCH;
    if !(0..3_652_059).contains(&ce_days) {
        return Err(out_of_range(column, days, data_type));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(Date::new(ce_days as u32))
}

fn time_nanos_to_time(column: &str, data_type: &DataType, nanos: i64) -> Result<Time> {
    if !(0..86_400_000_000_000).contains(&nanos) {
        return Err(out_of_range(column, nanos, data_type));
    }
    #[allow(clippy::cast_sign_loss)]
    Ok(Time::new((nanos / 100) as u64, SQL_SERVER_TIME_SCALE))
}

fn numeric(column: &str, data_type: &DataType, value: i128, scale: i8) -> Result<Numeric> {
    let scale = u8::try_from(scale).map_err(|_| out_of_range(column, value, data_type))?;
    if scale > 37 {
        return Err(out_of_range(column, value, data_type));
    }
    Ok(Numeric::new_with_scale(value, scale))
}

/// Converts the value at `row` of an Arrow array to a SQL Server parameter value.
#[allow(clippy::too_many_lines)]
pub fn arrow_value_to_column_data<'a>(
    column: &str,
    array: &'a ArrayRef,
    row: usize,
) -> Result<ColumnData<'a>> {
    let data_type = array.data_type();
    let valid = array.is_valid(row);
    macro_rules! value {
        ($variant:ident, $value:expr) => {
            ColumnData::$variant(if valid { Some($value) } else { None })
        };
    }
    let value = match data_type {
        DataType::Null => ColumnData::I32(None),
        DataType::Boolean => value!(Bit, array.as_boolean().value(row)),
        DataType::Int8 => value!(I16, i16::from(array.as_primitive::<Int8Type>().value(row))),
        DataType::Int16 => value!(I16, array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => value!(I32, array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => value!(I64, array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => value!(U8, array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => value!(
            I32,
            i32::from(array.as_primitive::<UInt16Type>().value(row))
        ),
        DataType::UInt32 => value!(
            I64,
            i64::from(array.as_primitive::<UInt32Type>().value(row))
        ),
        DataType::UInt64 => value!(
            Numeric,
            Numeric::new_with_scale(i128::from(array.as_primitive::<UInt64Type>().value(row)), 0)
        ),
        DataType::Float16 => value!(F32, array.as_primitive::<Float16Type>().value(row).to_f32()),
        DataType::Float32 => value!(F32, array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => value!(F64, array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal32(_, s) => {
            let v = i128::from(
                array
                    .as_primitive::<datafusion::arrow::datatypes::Decimal32Type>()
                    .value(row),
            );
            value!(Numeric, numeric(column, data_type, v, *s)?)
        }
        DataType::Decimal64(_, s) => {
            let v = i128::from(
                array
                    .as_primitive::<datafusion::arrow::datatypes::Decimal64Type>()
                    .value(row),
            );
            value!(Numeric, numeric(column, data_type, v, *s)?)
        }
        DataType::Decimal128(_, s) => {
            let v = array
                .as_primitive::<datafusion::arrow::datatypes::Decimal128Type>()
                .value(row);
            value!(Numeric, numeric(column, data_type, v, *s)?)
        }
        DataType::Decimal256(_, s) => {
            if valid {
                let v = array
                    .as_primitive::<datafusion::arrow::datatypes::Decimal256Type>()
                    .value(row);
                let v = v
                    .to_i128()
                    .ok_or_else(|| out_of_range(column, v, data_type))?;
                ColumnData::Numeric(Some(numeric(column, data_type, v, *s)?))
            } else {
                ColumnData::Numeric(None)
            }
        }
        DataType::Utf8 => value!(String, Cow::Borrowed(array.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => value!(String, Cow::Borrowed(array.as_string::<i64>().value(row))),
        DataType::Utf8View => value!(String, Cow::Borrowed(array.as_string_view().value(row))),
        DataType::Binary => value!(Binary, Cow::Borrowed(array.as_binary::<i32>().value(row))),
        DataType::LargeBinary => value!(Binary, Cow::Borrowed(array.as_binary::<i64>().value(row))),
        DataType::BinaryView => value!(Binary, Cow::Borrowed(array.as_binary_view().value(row))),
        DataType::FixedSizeBinary(_) => {
            value!(
                Binary,
                Cow::Borrowed(array.as_fixed_size_binary().value(row))
            )
        }
        DataType::Date32 => {
            if valid {
                let days = i64::from(array.as_primitive::<Date32Type>().value(row));
                ColumnData::Date(Some(epoch_days_to_date(column, data_type, days)?))
            } else {
                ColumnData::Date(None)
            }
        }
        DataType::Date64 => {
            if valid {
                let ms = array.as_primitive::<Date64Type>().value(row);
                let days = ms.div_euclid(86_400_000);
                ColumnData::Date(Some(epoch_days_to_date(column, data_type, days)?))
            } else {
                ColumnData::Date(None)
            }
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            if valid {
                let nanos = match data_type {
                    DataType::Time32(TimeUnit::Second) => {
                        i64::from(array.as_primitive::<Time32SecondType>().value(row))
                            * 1_000_000_000
                    }
                    DataType::Time32(TimeUnit::Millisecond) => {
                        i64::from(array.as_primitive::<Time32MillisecondType>().value(row))
                            * 1_000_000
                    }
                    DataType::Time64(TimeUnit::Microsecond) => {
                        array.as_primitive::<Time64MicrosecondType>().value(row) * 1_000
                    }
                    DataType::Time64(TimeUnit::Nanosecond) => {
                        array.as_primitive::<Time64NanosecondType>().value(row)
                    }
                    _ => UnsupportedArrowTypeSnafu {
                        column,
                        data_type: data_type.clone(),
                    }
                    .fail()?,
                };
                ColumnData::Time(Some(time_nanos_to_time(column, data_type, nanos)?))
            } else {
                ColumnData::Time(None)
            }
        }
        DataType::Timestamp(unit, tz) => {
            let nanos = if valid {
                let v = match unit {
                    TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                    TimeUnit::Millisecond => {
                        array.as_primitive::<TimestampMillisecondType>().value(row)
                    }
                    TimeUnit::Microsecond => {
                        array.as_primitive::<TimestampMicrosecondType>().value(row)
                    }
                    TimeUnit::Nanosecond => {
                        array.as_primitive::<TimestampNanosecondType>().value(row)
                    }
                };
                Some(i128::from(v) * nanos_per_unit(*unit))
            } else {
                None
            };
            let datetime2 = nanos
                .map(|nanos| epoch_nanos_to_datetime2(column, data_type, nanos))
                .transpose()?;
            if tz.is_some() {
                // Arrow timestamps with a time zone are instants in UTC
                ColumnData::DateTimeOffset(datetime2.map(|dt| DateTimeOffset::new(dt, 0)))
            } else {
                ColumnData::DateTime2(datetime2)
            }
        }
        _ => UnsupportedArrowTypeSnafu {
            column,
            data_type: data_type.clone(),
        }
        .fail()?,
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Date32Array, Decimal128Array, Int32Array, StringArray, Time64NanosecondArray,
        TimestampMicrosecondArray,
    };

    fn roundtrip(array: ArrayRef) -> ArrayRef {
        let values = (0..array.len())
            .map(|row| {
                arrow_value_to_column_data("c", &array, row)
                    .map(|v| match v {
                        // the values decoded from SQL Server are owned
                        ColumnData::String(s) => {
                            ColumnData::String(s.map(|s| Cow::Owned(s.into_owned())))
                        }
                        ColumnData::Binary(b) => {
                            ColumnData::Binary(b.map(|b| Cow::Owned(b.into_owned())))
                        }
                        ColumnData::U8(v) => ColumnData::U8(v),
                        ColumnData::I16(v) => ColumnData::I16(v),
                        ColumnData::I32(v) => ColumnData::I32(v),
                        ColumnData::I64(v) => ColumnData::I64(v),
                        ColumnData::F32(v) => ColumnData::F32(v),
                        ColumnData::F64(v) => ColumnData::F64(v),
                        ColumnData::Bit(v) => ColumnData::Bit(v),
                        ColumnData::Numeric(v) => ColumnData::Numeric(v),
                        ColumnData::Date(v) => ColumnData::Date(v),
                        ColumnData::Time(v) => ColumnData::Time(v),
                        ColumnData::DateTime2(v) => ColumnData::DateTime2(v),
                        ColumnData::DateTimeOffset(v) => ColumnData::DateTimeOffset(v),
                        other => panic!("unexpected {other:?}"),
                    })
                    .expect("converted")
            })
            .collect::<Vec<ColumnData<'static>>>();
        let refs = values.iter().map(Some).collect::<Vec<_>>();
        build_array("c", array.data_type(), &refs).expect("built")
    }

    #[test]
    fn values_roundtrip() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(-3)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("資料")])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None, Some(-1)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(-719_162),
                Some(19_000),
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(0),
                Some(86_399_999_999_900),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_680_000_000_123_456),
                Some(-62_135_596_800_000_000),
                None,
            ])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_680_000_000_123_456)])
                    .with_timezone("+10:00"),
            ),
        ];
        for array in arrays {
            assert_eq!(roundtrip(Arc::clone(&array)).as_ref(), array.as_ref());
        }
    }

    #[test]
    fn decimal_rescale_and_money() {
        let numeric = ColumnData::Numeric(Some(Numeric::new_with_scale(12345, 3)));
        assert_eq!(to_decimal(&numeric, 2), Some(Some(1235)));
        assert_eq!(to_decimal(&numeric, 5), Some(Some(1_234_500)));
        let money = ColumnData::F64(Some(-12.3456));
        assert_eq!(to_decimal(&money, 4), Some(Some(-123_456)));
    }

    #[test]
    fn legacy_datetime() {
        // 1970-01-01 00:00:00.5 is 25567 days after 1900-01-01, plus 150/300 s
        let dt = ColumnData::DateTime(Some(tiberius::time::DateTime::new(25_567, 150)));
        assert_eq!(to_epoch_nanos(&dt), Some(Some(500_000_000)));
    }

    #[test]
    fn type_mapping() {
        assert_eq!(
            map_sql_server_type_to_arrow("c", "decimal", 10, 2).unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert!(map_sql_server_type_to_arrow("c", "geography", 0, 0).is_err());
        assert_eq!(
            map_arrow_type_to_sql_server("c", &DataType::Utf8, true).unwrap(),
            "NVARCHAR(450)"
        );
        assert!(map_arrow_type_to_sql_server("c", &DataType::Decimal256(76, 10), false).is_err());
    }

    #[test]
    fn column_mapping() {
        let schema = Schema::new(vec![
            Field::new("c", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]);
        assert_eq!(
            map_columns(&["a", "b", "c"], &schema),
            vec![Some(2), Some(0)]
        );
        assert_eq!(map_columns(&["C", "A"], &schema), vec![Some(0), Some(1)]);
        // duplicated names of joined tables are matched by position
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("id", DataType::Int32, true),
        ]);
        assert_eq!(map_columns(&["id", "id"], &schema), vec![Some(0), Some(1)]);
    }
}
