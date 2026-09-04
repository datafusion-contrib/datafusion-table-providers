use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Decimal256Builder, Float32Builder, Float64Builder, Int64Builder, LargeBinaryBuilder,
    LargeStringBuilder, StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{
    i256, DataType, Date32Type, Field, IntervalMonthDayNano, IntervalUnit, Schema, SchemaRef,
    TimeUnit,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use bigdecimal::num_bigint;
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, FixedOffset};
use oracle::sql_type::{IntervalDS, IntervalYM, OracleType};
use oracle::Row;
use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: ArrowError },

    #[snafu(display("Failed to downcast builder for column '{column}' of type '{native_type}'"))]
    FailedToDowncastBuilder { native_type: String, column: String },

    #[snafu(display("Oracle error: {source}"))]
    OracleError { source: oracle::Error },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to parse BigDecimal from string '{value}': {source}"))]
    ParseBigDecimalError {
        value: String,
        source: bigdecimal::ParseBigDecimalError,
    },

    #[snafu(display("Unsupported data type for column '{column}': {data_type}"))]
    UnsupportedType { data_type: String, column: String },

    // NaiveDateTime
    #[snafu(display("Failed to convert chrono::NaiveDateTime {v} to nanos timestamp"))]
    FailedToConvertNaiveDateTimeToNanos { v: chrono::NaiveDateTime },

    #[snafu(display("Failed to map column {name} to arrow type"))]
    FailedToMapColumnType { name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Maps an Oracle column type name (as reported by `ALL_TAB_COLUMNS.DATA_TYPE`,
/// possibly with a trailing `(precision)` suffix or `WITH TIME ZONE` clause) to
/// an Arrow data type.
///
/// Returns `None` for types that cannot be represented losslessly; callers
/// should skip such columns rather than silently coercing them to strings.
pub fn map_oracle_type_to_arrow_type(
    data_type: &str,
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<DataType> {
    let data_type = data_type.trim().to_uppercase();

    // Oracle's `DATE` stores century, year, month, day, hour, minute and second, so it is a
    // datetime with 1-second resolution rather than a date-only type. Mapping it to `Date32`
    // would make the time-of-day unrepresentable and silently truncate every value to
    // midnight, so it shares the mapping used for `TIMESTAMP(0)`.
    // See: https://github.com/spiceai/spiceai/issues/12096
    if data_type == "DATE" {
        return Some(DataType::Timestamp(TimeUnit::Second, None));
    }

    if data_type.starts_with("TIMESTAMP") {
        let time_unit = match scale.unwrap_or(6) {
            0 => TimeUnit::Second,
            _ => TimeUnit::Nanosecond,
        };
        let tz =
            if data_type.contains("WITH TIME ZONE") || data_type.contains("WITH LOCAL TIME ZONE") {
                Some(Arc::<str>::from("UTC"))
            } else {
                None
            };
        return Some(DataType::Timestamp(time_unit, tz));
    }

    // Strip any parameter suffix (e.g. `VARCHAR2(100)` -> `VARCHAR2`).
    let base_type = match data_type.find('(') {
        Some(paren_pos) => data_type[..paren_pos].trim(),
        None => data_type.as_str(),
    };

    match base_type {
        // String types (Oracle types below max size is 32767 bytes)
        "ROWID" | "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" | "LONG" => Some(DataType::Utf8),
        "CLOB" | "NCLOB" => Some(DataType::LargeUtf8),

        // Numeric types
        "NUMBER" | "NUMERIC" | "DECIMAL" | "DEC" => {
            // "The absence of precision and scale designators specifies the maximum range and
            // precision for an Oracle number."
            let p = precision.unwrap_or(38).clamp(1, 38) as u8;
            let s = scale.unwrap_or(20) as i8;

            // Integer types in Oracle are represented as NUMBER with 0 scale.
            // Prefer Int64 over Decimal128 for integer types as it is much more efficient
            // (including for accelerators).
            if s == 0 && p <= 18 {
                return Some(DataType::Int64);
            }

            Some(DataType::Decimal128(p, s))
        }
        "INTEGER" | "INT" | "SMALLINT" => Some(DataType::Int64),

        // A subtype of the NUMBER data type having precision p. The precision p can range
        // from 1 to 126 binary digits. If <= 24: Float32, > 24: Float64.
        "FLOAT" => match precision {
            Some(p) if p <= 24 => Some(DataType::Float32),
            _ => Some(DataType::Float64),
        },
        "REAL" | "DOUBLE PRECISION" => Some(DataType::Float64),
        "BINARY_FLOAT" => Some(DataType::Float32),
        "BINARY_DOUBLE" => Some(DataType::Float64),

        "BOOLEAN" => Some(DataType::Boolean),

        // Binary types
        // Up to 2 GB
        "RAW" | "LONG RAW" => Some(DataType::Binary),
        // Up to 4 GB
        "BLOB" => Some(DataType::LargeBinary),

        // Interval types
        "INTERVAL YEAR" => Some(DataType::Interval(IntervalUnit::YearMonth)),
        "INTERVAL DAY" => Some(DataType::Interval(IntervalUnit::MonthDayNano)),

        _ => None,
    }
}

fn map_field_to_builder(field: &Field, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
    let builder: Box<dyn ArrayBuilder> = match field.data_type() {
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 0)),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, 0)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Decimal128(_, _) => Box::new(Decimal128Builder::with_capacity(capacity)),
        DataType::Decimal256(_, _) => Box::new(Decimal256Builder::with_capacity(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 0)),
        DataType::LargeBinary => Box::new(LargeBinaryBuilder::with_capacity(capacity, 0)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(TimeUnit::Second, _) => {
            Box::new(TimestampSecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::YearMonth) => Box::new(
            arrow::array::IntervalYearMonthBuilder::with_capacity(capacity),
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => Box::new(
            arrow::array::IntervalMonthDayNanoBuilder::with_capacity(capacity),
        ),
        dt => {
            return Err(Error::UnsupportedType {
                data_type: dt.to_string(),
                column: field.name().clone(),
            })
        }
    };
    Ok(builder)
}

/// Appends one row's `index`-th column into `builder`, reading it with the
/// native type that matches the Arrow field's data type.
///
/// When the Arrow field carries a timezone (from Oracle's
/// `TIMESTAMP WITH [LOCAL] TIME ZONE`), read the value as
/// `DateTime<FixedOffset>` and normalize it to UTC; naive (timezone-less)
/// timestamps are read as `NaiveDateTime` and interpreted as UTC, matching the
/// UTC session timezone configured on the connection pool.
#[allow(clippy::too_many_arguments)]
fn append_value(
    builder: &mut Box<dyn ArrayBuilder>,
    field: &Field,
    row: &Row,
    index: usize,
) -> Result<()> {
    let native_type = row
        .column_info()
        .get(index)
        .map(|c| format!("{:?}", c.oracle_type()))
        .unwrap_or_else(|| "unknown".to_string());

    macro_rules! prim {
        ($builder_ty:ty, $value_ty:ty, $convert:expr) => {{
            let builder = builder.as_any_mut().downcast_mut::<$builder_ty>().context(
                FailedToDowncastBuilderSnafu {
                    native_type: native_type.clone(),
                    column: field.name().clone(),
                },
            )?;
            let val: Option<$value_ty> = row
                .get::<_, Option<$value_ty>>(index)
                .context(OracleSnafu)?;
            match val {
                Some(v) => builder.append_value($convert(v)?),
                None => builder.append_null(),
            }
            Ok(())
        }};
    }

    match field.data_type() {
        DataType::Utf8 => prim!(StringBuilder, String, Ok),
        DataType::LargeUtf8 => prim!(LargeStringBuilder, String, Ok),
        DataType::Int64 => prim!(Int64Builder, i64, Ok),
        DataType::Float32 => prim!(Float32Builder, f32, Ok),
        DataType::Float64 => prim!(Float64Builder, f64, Ok),
        DataType::Boolean => prim!(BooleanBuilder, bool, Ok),
        DataType::Binary => prim!(BinaryBuilder, Vec<u8>, Ok),
        DataType::LargeBinary => prim!(LargeBinaryBuilder, Vec<u8>, Ok),
        DataType::Decimal128(_, scale) => {
            let scale = *scale;
            prim!(Decimal128Builder, String, |v: String| {
                let decimal = v
                    .parse::<BigDecimal>()
                    .context(ParseBigDecimalSnafu { value: v.clone() })?;
                big_decimal_to_i128(&decimal, scale).context(FailedToConvertBigDecimalToI128Snafu {
                    big_decimal: decimal.clone(),
                })
            })
        }
        DataType::Decimal256(_, scale) => {
            let scale = *scale;
            prim!(Decimal256Builder, String, |v: String| {
                let decimal = v
                    .parse::<BigDecimal>()
                    .context(ParseBigDecimalSnafu { value: v.clone() })?;
                Ok::<_, Error>(big_decimal_to_i256(&decimal, scale))
            })
        }
        DataType::Date32 => prim!(
            Date32Builder,
            chrono::NaiveDate,
            |v: chrono::NaiveDate| Ok::<_, Error>(Date32Type::from_naive_date(v))
        ),
        // TIMESTAMP WITH [LOCAL] TIME ZONE: read as DateTime<FixedOffset> and normalize to UTC.
        DataType::Timestamp(TimeUnit::Second, Some(_)) => prim!(
            TimestampSecondBuilder,
            DateTime<FixedOffset>,
            |v: DateTime<FixedOffset>| Ok::<_, Error>(v.with_timezone(&chrono::Utc).timestamp())
        ),
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => prim!(
            TimestampMillisecondBuilder,
            DateTime<FixedOffset>,
            |v: DateTime<FixedOffset>| Ok::<_, Error>(
                v.with_timezone(&chrono::Utc).timestamp_millis()
            )
        ),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => prim!(
            TimestampMicrosecondBuilder,
            DateTime<FixedOffset>,
            |v: DateTime<FixedOffset>| Ok::<_, Error>(
                v.with_timezone(&chrono::Utc).timestamp_micros()
            )
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            prim!(
                TimestampNanosecondBuilder,
                DateTime<FixedOffset>,
                fixed_offset_to_nanos
            )
        }
        // TIMESTAMP WITHOUT TIME ZONE / Oracle DATE: the session runs in UTC
        // (see `SetTimezoneCustomizer`), so interpret naive values as UTC.
        DataType::Timestamp(TimeUnit::Second, None) => prim!(
            TimestampSecondBuilder,
            chrono::NaiveDateTime,
            |v: chrono::NaiveDateTime| Ok::<_, Error>(v.and_utc().timestamp())
        ),
        DataType::Timestamp(TimeUnit::Millisecond, None) => prim!(
            TimestampMillisecondBuilder,
            chrono::NaiveDateTime,
            |v: chrono::NaiveDateTime| Ok::<_, Error>(v.and_utc().timestamp_millis())
        ),
        DataType::Timestamp(TimeUnit::Microsecond, None) => prim!(
            TimestampMicrosecondBuilder,
            chrono::NaiveDateTime,
            |v: chrono::NaiveDateTime| Ok::<_, Error>(v.and_utc().timestamp_micros())
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => prim!(
            TimestampNanosecondBuilder,
            chrono::NaiveDateTime,
            |v: chrono::NaiveDateTime| {
                v.and_utc()
                    .timestamp_nanos_opt()
                    .context(FailedToConvertNaiveDateTimeToNanosSnafu { v })
            }
        ),
        DataType::Interval(IntervalUnit::YearMonth) => prim!(
            arrow::array::IntervalYearMonthBuilder,
            IntervalYM,
            |v: IntervalYM| Ok::<_, Error>(v.years() * 12 + v.months())
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => prim!(
            arrow::array::IntervalMonthDayNanoBuilder,
            IntervalDS,
            |v: IntervalDS| {
                Ok::<_, Error>(IntervalMonthDayNano::new(
                    0,
                    v.days(),
                    i64::from(v.hours()) * 3_600_000_000_000
                        + i64::from(v.minutes()) * 60_000_000_000
                        + i64::from(v.seconds()) * 1_000_000_000
                        + i64::from(v.nanoseconds()),
                ))
            }
        ),
        dt => Err(Error::UnsupportedType {
            data_type: dt.to_string(),
            column: field.name().clone(),
        }),
    }
}

/// Converts driver rows into an Arrow [`RecordBatch`].
///
/// When `projected_schema` is provided (the normal path — it comes from the
/// table schema resolved through `get_schema`), values are read using the
/// native types matching each Arrow field. When it is `None`, the schema is
/// inferred from the Oracle column metadata of the first row.
pub fn rows_to_arrow(rows: Vec<Row>, projected_schema: &Option<SchemaRef>) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(
            projected_schema
                .as_ref()
                .map(Arc::clone)
                .unwrap_or_else(|| Arc::new(Schema::empty())),
        ));
    }

    let mut arrow_fields: Vec<Field> = Vec::new();
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    match projected_schema {
        Some(schema) => {
            for field in schema.fields() {
                arrow_fields.push((**field).clone());
                builders.push(map_field_to_builder(field, rows.len())?);
            }
        }
        None => {
            // Infer from the ODPI-C column metadata of the first row.
            for info in rows[0].column_info() {
                let name = info.name().to_string();
                let Some(data_type) =
                    map_oracle_type_to_arrow_type(&info.oracle_type().to_string(), None, None)
                else {
                    return Err(Error::UnsupportedType {
                        data_type: info.oracle_type().to_string(),
                        column: name,
                    });
                };
                let field = Field::new(name, data_type, true);
                builders.push(map_field_to_builder(&field, rows.len())?);
                arrow_fields.push(field);
            }
        }
    }

    for row in &rows {
        for (index, builder) in builders.iter_mut().enumerate() {
            append_value(builder, &arrow_fields[index], row, index)?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    let schema = Arc::new(Schema::new(arrow_fields));

    RecordBatch::try_new(schema, arrays).context(FailedToBuildRecordBatchSnafu)
}

fn big_decimal_to_i128(decimal: &BigDecimal, scale: i8) -> Option<i128> {
    // `with_scale` rescales with exact integer arithmetic (no f32 rounding).
    decimal
        .with_scale(i64::from(scale))
        .as_bigint_and_exponent()
        .0
        .to_i128()
}

fn big_decimal_to_i256(decimal: &BigDecimal, scale: i8) -> i256 {
    let (bigint_value, _) = decimal
        .with_scale(i64::from(scale))
        .into_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    }

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}

/// Convert an Oracle `TIMESTAMP WITH TIME ZONE` value to nanoseconds since epoch (UTC).
/// Returns an error if the value is outside the i64 nanosecond range (~1677-2262).
/// Silently converting out-of-range timestamps to epoch 0 (1970-01-01 UTC) would corrupt
/// query results, so this surfaces the offending value instead.
fn fixed_offset_to_nanos(v: DateTime<FixedOffset>) -> Result<i64> {
    let utc_value = v.with_timezone(&chrono::Utc);
    utc_value
        .timestamp_nanos_opt()
        .context(FailedToConvertNaiveDateTimeToNanosSnafu {
            v: utc_value.naive_utc(),
        })
}

/// Convenience for downcasting errors keyed by `OracleType` debug strings in tests.
#[allow(dead_code)]
fn oracle_type_name(t: &OracleType) -> String {
    format!("{t:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_common_oracle_types_mappings() {
        // Test a typical Oracle table schema
        let columns_and_expected = vec![
            (("ID", "NUMBER", Some(10), Some(0)), DataType::Int64),
            (("NAME", "VARCHAR2", None, None), DataType::Utf8),
            (
                ("SALARY", "NUMBER", Some(10), Some(2)),
                DataType::Decimal128(10, 2),
            ),
            (
                ("HIRE_DATE", "DATE", None, None),
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                ("CREATED_AT", "TIMESTAMP", None, Some(6)),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                ("PROFILE_PICTURE", "BLOB", None, None),
                DataType::LargeBinary,
            ),
            // Decimal edge cases
            (
                ("BIG_DECIMAL", "NUMBER", Some(38), Some(10)),
                DataType::Decimal128(38, 10),
            ),
            (
                ("DEFAULT_DECIMAL", "NUMBER", None, None),
                DataType::Decimal128(38, 20),
            ),
            // Float
            (("FLOAT32", "FLOAT", Some(10), None), DataType::Float32),
            (("FLOAT64", "FLOAT", Some(30), None), DataType::Float64),
            (
                ("BINARY_FLOAT", "BINARY_FLOAT", None, None),
                DataType::Float32,
            ),
            (
                ("BINARY_DOUBLE", "BINARY_DOUBLE", None, None),
                DataType::Float64,
            ),
            // Timestamp with and without time zone
            (
                ("TS_NANO", "TIMESTAMP(9)", None, Some(9)),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                ("TS_SEC", "TIMESTAMP(0)", None, Some(0)),
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                ("TS_TZ", "TIMESTAMP(6) WITH TIME ZONE", None, Some(6)),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
            ),
            (
                (
                    "TS_LOCAL_TZ",
                    "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                    None,
                    Some(3),
                ),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
            ),
            // Interval types
            (
                ("YM", "INTERVAL YEAR(2) TO MONTH", None, None),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
            (
                ("DS", "INTERVAL DAY(2) TO SECOND(6)", None, None),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ),
        ];

        for ((name, oracle_type, precision, scale), expected) in columns_and_expected {
            let result = map_oracle_type_to_arrow_type(oracle_type, precision, scale);
            assert_eq!(
                result,
                Some(expected.clone()),
                "Failed mapping for column {name}: {oracle_type} -> {expected:?}",
            );
        }
    }

    /// Regression test for spiceai#12096.
    ///
    /// Oracle's `DATE` carries hour, minute and second, so it must not map to a date-only Arrow
    /// type: `Date32` cannot represent the time-of-day and reading it that way silently
    /// truncates every value to midnight.
    #[test]
    fn oracle_date_maps_to_a_type_that_can_hold_the_time_of_day() {
        let mapped =
            map_oracle_type_to_arrow_type("DATE", None, None).expect("DATE should be supported");

        assert_eq!(
            mapped,
            DataType::Timestamp(TimeUnit::Second, None),
            "Oracle DATE has 1-second resolution, so it should map to Timestamp(Second, None)"
        );

        assert!(
            !matches!(mapped, DataType::Date32 | DataType::Date64),
            "Oracle DATE must not map to a date-only Arrow type: {mapped} discards the time-of-day"
        );
    }

    #[test]
    fn unsupported_types_are_not_coerced_to_strings() {
        // Types with no lossless Arrow representation must return None (and be skipped by
        // schema resolution) rather than silently becoming Utf8.
        for ty in ["XMLTYPE", "UROWID", "FLOAT(200)", "ARRAY", "OBJECT"] {
            // FLOAT(200) maps to Float64 (precision > 24), so exclude it from the None list.
            if ty == "FLOAT(200)" {
                assert_eq!(
                    map_oracle_type_to_arrow_type(ty, Some(200), None),
                    Some(DataType::Float64)
                );
                continue;
            }
            assert_eq!(
                map_oracle_type_to_arrow_type(ty, None, None),
                None,
                "{ty} should be unsupported"
            );
        }
    }

    #[test]
    fn test_fixed_offset_to_nanos_in_range() {
        use chrono::TimeZone;
        let ts = chrono::Utc
            .with_ymd_and_hms(2024, 9, 12, 10, 0, 0)
            .unwrap()
            .with_timezone(&FixedOffset::east_opt(9 * 3600).unwrap());
        let nanos = fixed_offset_to_nanos(ts).expect("in range");
        assert_eq!(nanos, 1_726_135_200_000_000_000 - 0); // UTC epoch nanos of 10:00Z
    }

    #[test]
    fn test_fixed_offset_to_nanos_overflow_errors_not_silent() {
        use chrono::TimeZone;
        // Year 9999 is far outside the i64 nanosecond range.
        let far = chrono::Utc
            .with_ymd_and_hms(9999, 1, 1, 0, 0, 0)
            .unwrap()
            .with_timezone(&FixedOffset::east_opt(0).unwrap());
        let result = fixed_offset_to_nanos(far);
        assert!(
            matches!(
                result,
                Err(Error::FailedToConvertNaiveDateTimeToNanos { .. })
            ),
            "expected a FailedToConvertNaiveDateTimeToNanos error, got {result:?}"
        );
    }
}
