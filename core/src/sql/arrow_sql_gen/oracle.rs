use crate::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder_optional;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, Decimal128Builder, Decimal256Builder,
        LargeBinaryBuilder, LargeStringBuilder, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{i256, DataType, Field, Schema, SchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};

use bigdecimal::num_bigint;
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{TimeZone, Utc};
use oracle::Row;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for index {index}"))]
    FailedToDowncastBuilder { index: usize },

    #[snafu(display("Oracle error: {source}"))]
    OracleError { source: oracle::Error },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to parse BigDecimal from string '{value}': {source}"))]
    ParseBigDecimalError {
        value: String,
        source: bigdecimal::ParseBigDecimalError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn rows_to_arrow(rows: Vec<Row>, projected_schema: &Option<SchemaRef>) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(
            projected_schema
                .clone()
                .unwrap_or_else(|| Arc::new(Schema::empty())),
        ));
    }

    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut arrow_fields: Vec<Field> = Vec::new();

    let first_row = &rows[0];

    // Determine schema fields
    if let Some(schema) = projected_schema {
        for field in schema.fields() {
            arrow_fields.push((**field).clone());
            builders
                .push(map_data_type_to_array_builder_optional(Some(field.data_type())).unwrap());
        }
    } else {
        // Infer from first row - using ODPI-C metadata if available
        // In rust-oracle, Row doesn't directly expose column names easily without Statement metadata.
        // However, we usually have a projected_schema from get_schema which queries metadata.

        let column_count = first_row.column_info().len();
        for i in 0..column_count {
            let name = format!("col_{}", i);
            let data_type = DataType::Utf8;
            let field = Field::new(name, data_type.clone(), true);
            arrow_fields.push(field);
            builders.push(map_data_type_to_array_builder_optional(Some(&data_type)).unwrap());
        }
    }

    for row in rows {
        for (i, builder) in builders.iter_mut().enumerate() {
            let field = &arrow_fields[i];

            match field.data_type() {
                DataType::Utf8 => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<String> = row
                        .get::<_, Option<String>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                DataType::Float64 => {
                    use arrow::array::Float64Builder;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<f64> = row
                        .get::<_, Option<f64>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                DataType::Float32 => {
                    use arrow::array::Float32Builder;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<f32> = row
                        .get::<_, Option<f32>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                DataType::Int64 => {
                    use arrow::array::Int64Builder;
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<i64> = row
                        .get::<_, Option<i64>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                DataType::Decimal128(_p, s) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal128Builder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<String> = row
                        .get::<_, Option<String>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    if let Some(s_val) = val {
                        let big_dec = s_val.parse::<BigDecimal>().map_err(|e| {
                            Error::ParseBigDecimalError {
                                value: s_val.clone(),
                                source: e,
                            }
                        })?;
                        let i128_val = to_decimal_128(&big_dec, *s as i64).ok_or(
                            Error::FailedToConvertBigDecimalToI128 {
                                big_decimal: big_dec,
                            },
                        )?;
                        builder.append_value(i128_val);
                    } else {
                        builder.append_null();
                    }
                }
                DataType::Decimal256(_p, _s) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Decimal256Builder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<String> = row
                        .get::<_, Option<String>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    if let Some(s_val) = val {
                        let big_dec = s_val.parse::<BigDecimal>().map_err(|e| {
                            Error::ParseBigDecimalError {
                                value: s_val.clone(),
                                source: e,
                            }
                        })?;
                        let i256_val = to_decimal_256(&big_dec);
                        builder.append_value(i256_val);
                    } else {
                        builder.append_null();
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMicrosecondBuilder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<oracle::sql_type::Timestamp> = row
                        .get::<_, Option<oracle::sql_type::Timestamp>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    if let Some(ts) = val {
                        let chrono_ts = Utc
                            .with_ymd_and_hms(
                                ts.year(),
                                ts.month(),
                                ts.day(),
                                ts.hour(),
                                ts.minute(),
                                ts.second(),
                            )
                            .single();

                        if let Some(chrono_ts) = chrono_ts {
                            let micros =
                                chrono_ts.timestamp() * 1_000_000 + (ts.nanosecond() / 1000) as i64;
                            builder.append_value(micros);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                DataType::Binary => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<Vec<u8>> = row
                        .get::<_, Option<Vec<u8>>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                DataType::LargeBinary => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<LargeBinaryBuilder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<Vec<u8>> = row
                        .get::<_, Option<Vec<u8>>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                DataType::LargeUtf8 => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<LargeStringBuilder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<String> = row
                        .get::<_, Option<String>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
                _ => {
                    // Fallback: try to get as string
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .ok_or(Error::FailedToDowncastBuilder { index: i })?;
                    let val: Option<String> = row
                        .get::<_, Option<String>>(i)
                        .map_err(|e| Error::OracleError { source: e })?;
                    builder.append_option(val);
                }
            }
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    let schema = Arc::new(Schema::new(arrow_fields));

    RecordBatch::try_new(schema, arrays).context(FailedToBuildRecordBatchSnafu)
}

fn to_decimal_128(decimal: &BigDecimal, scale: i64) -> Option<i128> {
    (decimal * 10i128.pow(scale.try_into().unwrap_or_default())).to_i128()
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
