use crate::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder_optional;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder,
        NullBuilder, RecordBatch, RecordBatchOptions, Time64NanosecondBuilder,
        TimestampMillisecondBuilder, UInt64Builder,
    },
    datatypes::{DataType, Date32Type, Field, Schema, TimeUnit},
};
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use chrono::{NaiveDate, NaiveTime, Timelike};
use mysql_async::{consts::ColumnFlags, consts::ColumnType, FromValueError, Row, Value};
use snafu::{ResultExt, Snafu};
use std::{convert, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {:?}", mysql_type))]
    FailedToDowncastBuilder { mysql_type: String },

    #[snafu(display("Integer overflow when converting u64 to i64: {source}"))]
    FailedToConvertU64toI64 {
        source: <u64 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Integer overflow when converting u128 to i64: {source}"))]
    FailedToConvertU128toI64 {
        source: <u128 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Failed to get a row value for {:?}: {}", mysql_type, source))]
    FailedToGetRowValue {
        mysql_type: ColumnType,
        source: mysql_async::FromValueError,
    },

    #[snafu(display("Failed to parse raw Postgres Bytes as BigDecimal: {:?}", bytes))]
    FailedToParseBigDecimalFromPostgres { bytes: Vec<u8> },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to find field {column_name} in schema"))]
    FailedToFindFieldInSchema { column_name: String },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! handle_primitive_type {
    ($builder:expr, $type:expr, $builder_ty:ty, $value_ty:ty, $row:expr, $index:expr) => {{
        let Some(builder) = $builder else {
            return NoBuilderForIndexSnafu { index: $index }.fail();
        };
        let Some(builder) = builder.as_any_mut().downcast_mut::<$builder_ty>() else {
            return FailedToDowncastBuilderSnafu {
                mysql_type: format!("{:?}", $type),
            }
            .fail();
        };
        let v = handle_null_error($row.get_opt::<$value_ty, usize>($index).transpose())
            .context(FailedToGetRowValueSnafu { mysql_type: $type })?;

        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }};
}

/// Converts `MySQL` `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn rows_to_arrow(rows: &[Row]) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Option<Field>> = Vec::new();
    let mut arrow_columns_builders: Vec<Option<Box<dyn ArrayBuilder>>> = Vec::new();
    let mut mysql_types: Vec<ColumnType> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();
    let mut column_is_binary_stats: Vec<bool> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns().iter() {
            let column_name = column.name_str();
            let column_type = column.column_type();
            let column_is_binary = column.flags().contains(ColumnFlags::BINARY_FLAG);
            let column_decimal_precision: i8 = column.decimals().try_into().unwrap_or_default();

            let data_type = map_column_to_data_type(
                column_type,
                column_is_binary,
                Some(column_decimal_precision),
            );

            arrow_fields.push(
                data_type
                    .clone()
                    .map(|data_type| Field::new(column_name.clone(), data_type.clone(), true)),
            );
            arrow_columns_builders
                .push(map_data_type_to_array_builder_optional(data_type.as_ref()));
            mysql_types.push(column_type);
            column_names.push(column_name.to_string());
            column_is_binary_stats.push(column_is_binary);
        }
    }

    for row in rows {
        for (i, mysql_type) in mysql_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            match *mysql_type {
                ColumnType::MYSQL_TYPE_NULL => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    builder.append_null();
                }
                ColumnType::MYSQL_TYPE_BIT => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<UInt64Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let value = row.get_opt::<Value, usize>(i).transpose().context(
                        FailedToGetRowValueSnafu {
                            mysql_type: ColumnType::MYSQL_TYPE_BIT,
                        },
                    )?;
                    match value {
                        Some(Value::Bytes(mut bytes)) => {
                            while bytes.len() < 8 {
                                bytes.insert(0, 0);
                            }
                            let mut array = [0u8; 8];
                            array.copy_from_slice(&bytes);
                            builder.append_value(u64::from_be_bytes(array));
                        }
                        _ => builder.append_null(),
                    }
                }
                ColumnType::MYSQL_TYPE_TINY => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_TINY,
                        Int8Builder,
                        i8,
                        row,
                        i
                    );
                }
                column_type @ (ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR) => {
                    handle_primitive_type!(builder, column_type, Int16Builder, i16, row, i);
                }
                column_type @ (ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG) => {
                    handle_primitive_type!(builder, column_type, Int32Builder, i32, row, i);
                }
                ColumnType::MYSQL_TYPE_LONGLONG => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_LONGLONG,
                        Int64Builder,
                        i64,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_FLOAT => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_FLOAT,
                        Float32Builder,
                        f32,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    handle_primitive_type!(
                        builder,
                        ColumnType::MYSQL_TYPE_DOUBLE,
                        Float64Builder,
                        f64,
                        row,
                        i
                    );
                }
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };

                    let Some(builder) = builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };

                    let val = handle_null_error(row.get_opt::<BigDecimal, usize>(i).transpose())
                        .context(FailedToGetRowValueSnafu {
                            mysql_type: ColumnType::MYSQL_TYPE_DECIMAL,
                        })?;

                    let scale = match &val {
                        Some(val) => val.fractional_digit_count(),
                        None => 0,
                    };

                    let Some(val) = val else {
                        builder.append_null();
                        continue;
                    };

                    let Some(val) = to_decimal_128(&val, scale) else {
                        return FailedToConvertBigDecimalToI128Snafu { big_decimal: val }.fail();
                    };

                    builder.append_value(val);
                }
                column_type @ (ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_JSON
                | ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_ENUM) => {
                    handle_primitive_type!(
                        builder,
                        column_type,
                        LargeStringBuilder,
                        String,
                        row,
                        i
                    );
                }
                column_type @ (ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VAR_STRING) => {
                    if column_is_binary_stats[i] {
                        handle_primitive_type!(
                            builder,
                            column_type,
                            BinaryBuilder,
                            Vec<u8>,
                            row,
                            i
                        );
                    } else {
                        handle_primitive_type!(
                            builder,
                            column_type,
                            LargeStringBuilder,
                            String,
                            row,
                            i
                        );
                    }
                }
                ColumnType::MYSQL_TYPE_DATE => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let v = handle_null_error(row.get_opt::<NaiveDate, usize>(i).transpose())
                        .context(FailedToGetRowValueSnafu {
                            mysql_type: ColumnType::MYSQL_TYPE_DATE,
                        })?;

                    match v {
                        Some(v) => {
                            builder.append_value(Date32Type::from_naive_date(v));
                        }
                        None => builder.append_null(),
                    }
                }
                ColumnType::MYSQL_TYPE_TIME => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<Time64NanosecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let v = handle_null_error(row.get_opt::<NaiveTime, usize>(i).transpose())
                        .context(FailedToGetRowValueSnafu {
                            mysql_type: ColumnType::MYSQL_TYPE_TIME,
                        })?;

                    match v {
                        Some(value) => {
                            builder.append_value(
                                i64::from(value.num_seconds_from_midnight()) * 1_000_000_000
                                    + i64::from(value.nanosecond()),
                            );
                        }
                        None => builder.append_null(),
                    }
                }
                column_type @ (ColumnType::MYSQL_TYPE_TIMESTAMP
                | ColumnType::MYSQL_TYPE_DATETIME) => {
                    let Some(builder) = builder else {
                        return NoBuilderForIndexSnafu { index: i }.fail();
                    };
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            mysql_type: format!("{mysql_type:?}"),
                        }
                        .fail();
                    };
                    let v = handle_null_error(row.get_opt::<Value, usize>(i).transpose()).context(
                        FailedToGetRowValueSnafu {
                            mysql_type: column_type,
                        },
                    )?;

                    match v {
                        Some(v) => {
                            let timestamp = match v {
                                Value::Date(year, month, day, hour, minute, second, micros) => {
                                    let timestamp = chrono::NaiveDate::from_ymd_opt(
                                        i32::from(year),
                                        u32::from(month),
                                        u32::from(day),
                                    )
                                    .unwrap_or_default()
                                    .and_hms_micro_opt(
                                        u32::from(hour),
                                        u32::from(minute),
                                        u32::from(second),
                                        micros,
                                    )
                                    .unwrap_or_default()
                                    .and_utc();
                                    timestamp.timestamp() * 1000
                                }
                                Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
                                    let naive_time = chrono::NaiveTime::from_hms_micro_opt(
                                        u32::from(hours),
                                        u32::from(minutes),
                                        u32::from(seconds),
                                        micros,
                                    )
                                    .unwrap_or_default();

                                    let time: i64 = naive_time.num_seconds_from_midnight().into();

                                    let timestamp = i64::from(days) * 24 * 60 * 60 + time;

                                    if is_neg {
                                        -timestamp
                                    } else {
                                        timestamp
                                    }
                                }
                                _ => 0,
                            };
                            builder.append_value(timestamp);
                        }
                        None => builder.append_null(),
                    }
                }
                _ => unimplemented!("Unsupported column type {:?}", mysql_type),
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .filter_map(|builder| builder.map(|mut b| b.finish()))
        .collect::<Vec<ArrayRef>>();
    let arrow_fields = arrow_fields.into_iter().flatten().collect::<Vec<Field>>();
    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options)
        .map_err(|err| Error::FailedToBuildRecordBatch { source: err })
}

#[allow(clippy::unnecessary_wraps)]
pub fn map_column_to_data_type(
    column_type: ColumnType,
    column_is_binary: bool,
    column_decimal_precision: Option<i8>,
) -> Option<DataType> {
    match column_type {
        ColumnType::MYSQL_TYPE_NULL => Some(DataType::Null),
        ColumnType::MYSQL_TYPE_BIT => Some(DataType::UInt64),
        ColumnType::MYSQL_TYPE_TINY => Some(DataType::Int8),
        ColumnType::MYSQL_TYPE_YEAR | ColumnType::MYSQL_TYPE_SHORT => Some(DataType::Int16),
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => Some(DataType::Int32),
        ColumnType::MYSQL_TYPE_LONGLONG => Some(DataType::Int64),
        ColumnType::MYSQL_TYPE_FLOAT => Some(DataType::Float32),
        ColumnType::MYSQL_TYPE_DOUBLE => Some(DataType::Float64),
        // Decimal precision must be a value between 0x00 - 0x51, so it's safe to unwrap_or_default here
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => Some(DataType::Decimal128(38, column_decimal_precision.unwrap_or_default())),
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2  => {
            Some(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        ColumnType::MYSQL_TYPE_DATE => Some(DataType::Date32),
        ColumnType::MYSQL_TYPE_TIME => {
            Some(DataType::Time64(TimeUnit::Nanosecond))
        }
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => Some(DataType::LargeUtf8),
        ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING => {
            if column_is_binary {
                Some(DataType::Binary)
            } else {
                Some(DataType::LargeUtf8)
            }
        },
        // replication only
        ColumnType::MYSQL_TYPE_TYPED_ARRAY
        // internal
        | ColumnType::MYSQL_TYPE_NEWDATE
        // Unsupported yet
        | ColumnType::MYSQL_TYPE_UNKNOWN
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_TIME2
        | ColumnType::MYSQL_TYPE_GEOMETRY => {
            unimplemented!("Unsupported column type {:?}", column_type)
        }
    }
}

fn to_decimal_128(decimal: &BigDecimal, scale: i64) -> Option<i128> {
    (decimal * 10i128.pow(scale.try_into().unwrap_or_default())).to_i128()
}

fn handle_null_error<T>(
    result: Result<Option<T>, FromValueError>,
) -> Result<Option<T>, FromValueError> {
    match result {
        Ok(val) => Ok(val),
        Err(FromValueError(Value::NULL)) => Ok(None),
        err => err,
    }
}
