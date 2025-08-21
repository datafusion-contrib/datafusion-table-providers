use super::{Error, RegexSnafu, Result};
use arrow::datatypes::DataType;
use arrow_schema::{Field, Fields, TimeUnit};
use regex::Regex;
use snafu::ResultExt;
use std::sync::Arc;

pub(crate) fn trino_data_type_to_arrow_type(
    trino_type: &str,
    tz: Option<&str>,
) -> Result<DataType> {
    let normalized_type = trino_type.to_lowercase();

    if normalized_type.starts_with("time(") {
        let time_unit = time_unit_from_precision(extract_precision(&normalized_type, "time")?);

        return match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => Ok(DataType::Time32(TimeUnit::Millisecond)),
            time_unit => Ok(DataType::Time64(time_unit)),
        };
    }

    if normalized_type.contains("with time zone") && normalized_type.starts_with("timestamp(") {
        let time_unit = time_unit_from_precision(extract_precision(&normalized_type, "timestamp")?);
        return Ok(DataType::Timestamp(
            time_unit,
            Some(Arc::from(tz.unwrap_or("UTC"))),
        ));
    }

    if normalized_type.starts_with("timestamp(") {
        let time_unit = time_unit_from_precision(extract_precision(&normalized_type, "timestamp")?);
        return Ok(DataType::Timestamp(time_unit, None));
    }

    match normalized_type.as_str() {
        "null" => Ok(DataType::Null),
        "boolean" => Ok(DataType::Boolean),
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "varchar" | "char" => Ok(DataType::Utf8),
        "varbinary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        _ if normalized_type.starts_with("decimal") || normalized_type.starts_with("numeric") => {
            parse_decimal_type(&normalized_type)
        }
        _ if normalized_type.starts_with("varchar") => Ok(DataType::Utf8),
        _ if normalized_type.starts_with("char") => Ok(DataType::Utf8),
        _ if normalized_type.starts_with("varbinary") => Ok(DataType::Binary),
        _ if normalized_type.starts_with("array") => parse_array_type(&normalized_type, tz),
        _ if normalized_type.starts_with("row") => parse_row_type(&normalized_type, tz),
        _ => Err(Error::UnsupportedTrinoType {
            trino_type: trino_type.to_string(),
        }),
    }
}

pub fn extract_precision(s: &str, prefix: &str) -> Result<u32> {
    let pattern = format!(r"^{}(?:\((\d+)\))?", regex::escape(prefix));
    let re = Regex::new(&pattern).context(RegexSnafu)?;
    let caps = re.captures(s).ok_or_else(|| Error::InvalidPrecision {
        trino_type: s.to_string(),
    })?;

    let precision = match caps.get(1) {
        Some(m) => m
            .as_str()
            .parse::<u32>()
            .map_err(|_| Error::InvalidPrecision {
                trino_type: s.to_string(),
            })?,
        None => {
            return Err(Error::InvalidPrecision {
                trino_type: s.to_string(),
            })
        }
    };

    Ok(precision)
}

fn time_unit_from_precision(p: u32) -> TimeUnit {
    match p {
        0..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn parse_decimal_type(type_str: &str) -> Result<DataType> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let params = &type_str[start + 1..end];
            let parts: Vec<&str> = params.split(',').collect();

            let precision = parts[0].trim().parse::<u8>().unwrap_or(38);
            let scale = if parts.len() > 1 {
                parts[1].trim().parse::<i8>().unwrap_or(0)
            } else {
                0
            };

            if precision > 38 {
                Ok(DataType::Decimal256(precision, scale))
            } else {
                Ok(DataType::Decimal128(precision, scale))
            }
        } else {
            Ok(DataType::Decimal128(18, 6))
        }
    } else {
        Ok(DataType::Decimal128(18, 6))
    }
}

fn parse_array_type(type_str: &str, tz: Option<&str>) -> Result<DataType> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let element_type_str = &type_str[start + 1..end];
            return match trino_data_type_to_arrow_type(element_type_str, tz)? {
                DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => Ok(
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                ),
                inner_arrow_type => Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    inner_arrow_type,
                    true,
                )))),
            };
        }
    }

    Err(Error::UnsupportedTrinoType {
        trino_type: type_str.to_string(),
    })
}

fn parse_row_type(type_str: &str, tz: Option<&str>) -> Result<DataType> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let inner = &type_str[start + 1..end];
            let mut fields = Vec::new();

            let field_definitions = split_respecting_parentheses(inner, ',');

            for field_def in field_definitions {
                let field_def = field_def.trim();
                if let Some(space_pos) = field_def.find(' ') {
                    let field_name = field_def[..space_pos].trim();
                    let field_type = field_def[space_pos + 1..].trim();
                    let arrow_type = match trino_data_type_to_arrow_type(field_type, tz)? {
                        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                            DataType::Utf8
                        }
                        inner_arrow_type => inner_arrow_type,
                    };
                    fields.push(Field::new(field_name, arrow_type, true));
                }
            }

            return Ok(DataType::Struct(Fields::from(fields)));
        }
    }
    Err(Error::UnsupportedTrinoType {
        trino_type: type_str.to_string(),
    })
}

fn split_respecting_parentheses(s: &str, delimiter: char) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            ch if ch == delimiter && paren_depth == 0 => {
                result.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_basic_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("null", None).unwrap(),
            DataType::Null
        );
        assert_eq!(
            trino_data_type_to_arrow_type("boolean", None).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            trino_data_type_to_arrow_type("tinyint", None).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("smallint", None).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            trino_data_type_to_arrow_type("integer", None).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            trino_data_type_to_arrow_type("bigint", None).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            trino_data_type_to_arrow_type("real", None).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            trino_data_type_to_arrow_type("double", None).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("varchar", None).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("char", None).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("varbinary", None).unwrap(),
            DataType::Binary
        );

        assert_eq!(
            trino_data_type_to_arrow_type("varchar(255)", None).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("char(10)", None).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("varbinary(1000)", None).unwrap(),
            DataType::Binary
        );
    }

    #[test]
    fn test_date() {
        assert_eq!(
            trino_data_type_to_arrow_type("date", None).unwrap(),
            DataType::Date32
        );
    }

    #[test]
    fn test_time() {
        assert_eq!(
            trino_data_type_to_arrow_type("time(0)", None).unwrap(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(1)", None).unwrap(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(2)", None).unwrap(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(3)", None).unwrap(),
            DataType::Time32(TimeUnit::Millisecond)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("time(4)", None).unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(5)", None).unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(6)", None).unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("time(7)", None).unwrap(),
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(8)", None).unwrap(),
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time(9)", None).unwrap(),
            DataType::Time64(TimeUnit::Nanosecond)
        );
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(0)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(1)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(2)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(3)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(4)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(5)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(6)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(7)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(8)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(9)", None).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_timestamp_with_timezone() {
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(0) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(1) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(2) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(3) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );

        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(4) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(5) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(6) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );

        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(7) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(8) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp(9) with time zone", None).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(
            trino_data_type_to_arrow_type("BOOLEAN", None).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            trino_data_type_to_arrow_type("Boolean", None).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            trino_data_type_to_arrow_type("VARCHAR", None).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("decimal", None).unwrap(),
            DataType::Decimal128(18, 6)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(10)", None).unwrap(),
            DataType::Decimal128(10, 0)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(10,2)", None).unwrap(),
            DataType::Decimal128(10, 2)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(50,10)", None).unwrap(),
            DataType::Decimal256(50, 10)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("numeric(10,2)", None).unwrap(),
            DataType::Decimal128(10, 2)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(38,0)", None).unwrap(),
            DataType::Decimal128(38, 0)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(39,0)", None).unwrap(),
            DataType::Decimal256(39, 0)
        );
    }

    #[test]
    fn test_array_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("array(integer)", None).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );

        assert_eq!(
            trino_data_type_to_arrow_type("array(varchar)", None).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        assert_eq!(
            trino_data_type_to_arrow_type("array(decimal(10,2))", None).unwrap(),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(10, 2),
                true
            )))
        );
    }

    #[test]
    fn test_nested_array_types() {
        // Array of array becomes array of strings
        assert_eq!(
            trino_data_type_to_arrow_type("array(array(integer))", None).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        );

        // Array of maps becomes array of strings
        assert_eq!(
            trino_data_type_to_arrow_type("array(row(name varchar, age integer))", None).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        // Array of maps is not supported
        let res = trino_data_type_to_arrow_type("array(map(varchar, integer))", None);
        assert!(res.is_err());
    }

    #[test]
    fn test_map_types() {
        // Maps are not supported
        let res = trino_data_type_to_arrow_type("map(varchar, integer)", None);
        assert!(res.is_err())
    }

    #[test]
    fn test_row_type_simple() {
        let expected = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, age integer)", None).unwrap(),
            expected
        );
    }

    fn test_row_type_complex() {
        let expected_multi = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("salary", DataType::Decimal128(10, 2), true),
            Field::new("active", DataType::Boolean, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type(
                "row(id bigint, name varchar, salary decimal(10,2), active boolean)",
                None
            )
            .unwrap(),
            expected_multi
        );
    }

    #[test]
    fn test_row_type_with_array() {
        let expected_row_array = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("scores", DataType::Utf8, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, scores array(integer))", None)
                .unwrap(),
            expected_row_array
        );
    }

    #[test]
    fn test_row_type_with_map() {
        // row of maps is not supported
        let res =
            trino_data_type_to_arrow_type("row(name varchar, scores map(varchar, integer))", None);
        assert!(res.is_err());
    }

    #[test]
    fn test_row_type_with_nested_row() {
        let expected_row_array = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("scores", DataType::Utf8, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, scores row(value integer))", None)
                .unwrap(),
            expected_row_array
        );
    }

    #[test]
    fn test_unsupported_types() {
        let result = trino_data_type_to_arrow_type("unknown_type", None);
        assert!(result.is_err());
        if let Err(Error::UnsupportedTrinoType { trino_type }) = result {
            assert_eq!(trino_type, "unknown_type");
        }
    }

    #[test]
    fn test_extract_precision() {
        let result = extract_precision("time(3)", "time");
        assert_eq!(result.unwrap(), 3);

        let result = extract_precision("timestamp(6)", "timestamp");
        assert_eq!(result.unwrap(), 6);

        let result = extract_precision("timestamp(9) with time zone", "timestamp");
        assert_eq!(result.unwrap(), 9);

        let result = extract_precision("time", "time");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidPrecision { .. }
        ));

        let result = extract_precision("timestamp(x)", "timestamp");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidPrecision { .. }
        ));

        let result = extract_precision("row(x integer)", "timestamp");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidPrecision { .. }
        ));

        let result = extract_precision("array(timestamp(6))", "timestamp");
        assert!(result.is_err());
    }
}
