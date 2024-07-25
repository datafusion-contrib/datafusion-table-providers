use datafusion::{
    logical_expr::{Cast, Expr},
    scalar::ScalarValue,
};

pub const SECONDS_IN_DAY: i32 = 86_400;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Expression not supported {expr}"))]
    UnsupportedFilterExpr { expr: String },

    #[snafu(display("Engine {engine} not supported for expression {expr}"))]
    EngineNotSupportedForExpression { engine: String, expr: String },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug)]
pub enum Engine {
    Spark,
    SQLite,
    DuckDB,
    ODBC,
    Postgres,
}

pub fn to_sql_with_engine(expr: &Expr, engine: Option<Engine>) -> Result<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            let left = to_sql_with_engine(&binary_expr.left, engine)?;
            let right = to_sql_with_engine(&binary_expr.right, engine)?;

            if let Some(Engine::DuckDB) = engine {
                // TODO: DuckDB doesn't support comparison between timestamp_s /timestamp_ms with timestampz as of v1
                // Revisit in future DuckDB versions
                if right.starts_with("TO_TIMESTAMP") && !left.starts_with("TO_TIMESTAMP") {
                    return Ok(format!(
                        "TO_TIMESTAMP(EPOCH_MS({}) / 1000) {} {}",
                        left, binary_expr.op, right
                    ));
                }
            }

            Ok(format!("{} {} {}", left, binary_expr.op, right))
        }
        Expr::Column(name) => match engine {
            Some(Engine::Spark | Engine::ODBC) => Ok(format!("{name}")),
            _ => Ok(format!("\"{name}\"")),
        },
        Expr::Cast(cast) => handle_cast(cast, engine, expr),
        Expr::Literal(value) => match value {
            ScalarValue::Date32(Some(value)) => match engine {
                Some(Engine::SQLite) => {
                    Ok(format!("datetime({}, 'unixepoch')", value * SECONDS_IN_DAY))
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value * SECONDS_IN_DAY)),
            },
            ScalarValue::Date64(Some(value)) => match engine {
                Some(Engine::SQLite) => Ok(format!(
                    "datetime({}, 'unixepoch')",
                    value * i64::from(SECONDS_IN_DAY)
                )),
                _ => Ok(format!(
                    "TO_TIMESTAMP({})",
                    value * i64::from(SECONDS_IN_DAY)
                )),
            },
            ScalarValue::Null => Ok(value.to_string()),
            ScalarValue::Int16(Some(value)) => Ok(value.to_string()),
            ScalarValue::Int32(Some(value)) => Ok(value.to_string()),
            ScalarValue::Int64(Some(value)) => Ok(value.to_string()),
            ScalarValue::Boolean(Some(value)) => Ok(value.to_string()),
            ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                Ok(format!("'{value}'"))
            }
            ScalarValue::Float32(Some(value)) => Ok(value.to_string()),
            ScalarValue::Float64(Some(value)) => Ok(value.to_string()),
            ScalarValue::Int8(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt8(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt16(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt32(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt64(Some(value)) => Ok(value.to_string()),
            ScalarValue::TimestampNanosecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => {
                    Ok(format!("datetime({}, 'unixepoch')", value / 1_000_000_000))
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1_000_000_000)),
            },
            ScalarValue::TimestampMicrosecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({}, 'unixepoch')", value / 1_000_000)),
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1_000_000)),
            },
            ScalarValue::TimestampMillisecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({}, 'unixepoch')", value / 1000)),
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1000)),
            },
            ScalarValue::TimestampSecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({value}, 'unixepoch')")),
                _ => Ok(format!("TO_TIMESTAMP({value})")),
            },
            _ => Err(Error::UnsupportedFilterExpr {
                expr: format!("{expr}"),
            }),
        },
        Expr::Like(like_expr) => {
            if like_expr.escape_char.is_some() {
                // Escape char is not supported
                return Err(Error::UnsupportedFilterExpr {
                    expr: format!("{expr}"),
                });
            }
            let expr = to_sql_with_engine(&like_expr.expr, engine)?;
            let pattern = to_sql_with_engine(&like_expr.pattern, engine)?;

            let mut op_and_pattern = match (engine, like_expr.case_insensitive) {
                (Some(Engine::Postgres | Engine::DuckDB), true) => format!("ILIKE {pattern}"),
                (Some(Engine::Postgres | Engine::DuckDB), false) => format!("LIKE {pattern}"),
                (Some(Engine::SQLite), true) => format!("LIKE {pattern}"),
                (Some(Engine::SQLite), false) => format!("LIKE {pattern} COLLATE BINARY"),
                _ => {
                    return Err(Error::UnsupportedFilterExpr {
                        expr: expr.to_string(),
                    });
                }
            };

            if like_expr.negated {
                op_and_pattern = format!("NOT {}", op_and_pattern)
            };

            Ok(format!("{expr} {op_and_pattern}"))
        }
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}

pub fn to_sql(expr: &Expr) -> Result<String> {
    to_sql_with_engine(expr, None)
}

fn handle_cast(cast: &Cast, engine: Option<Engine>, expr: &Expr) -> Result<String> {
    match cast.data_type {
        arrow::datatypes::DataType::Timestamp(_, Some(_) | None) => match engine {
            Some(Engine::ODBC) => Ok(format!(
                "CAST({} AS TIMESTAMP)",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
            // This needs to match the timestamp conversion below
            Some(Engine::DuckDB) => Ok(format!(
                "TO_TIMESTAMP(EPOCH(CAST({} AS TIMESTAMP)))",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
            Some(Engine::SQLite) => Ok(format!(
                "datetime({}, 'subsec', 'utc')",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
            Some(Engine::Spark) => EngineNotSupportedForExpressionSnafu {
                engine: "Spark".to_string(),
                expr: format!("{expr}"),
            }
            .fail()?,
            _ => Ok(format!(
                "CAST({} AS TIMESTAMPTZ)",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
        },
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        logical_expr::{Expr, Like},
        prelude::col,
        scalar::ScalarValue,
    };

    #[test]
    fn test_like_expr_to_sql() -> Result<()> {
        for (engine, case_insensitive, negated, expected) in [
            (Engine::Postgres, false, false, "\"name\" LIKE '%John%'"),
            (Engine::Postgres, true, true, "\"name\" NOT ILIKE '%John%'"),
            (Engine::DuckDB, false, false, "\"name\" LIKE '%John%'"),
            (Engine::DuckDB, true, true, "\"name\" NOT ILIKE '%John%'"),
            (
                Engine::SQLite,
                false,
                false,
                "\"name\" LIKE '%John%' COLLATE BINARY",
            ),
            (Engine::SQLite, true, true, "\"name\" NOT LIKE '%John%'"),
        ] {
            let expr = Expr::Like(Like {
                expr: Box::new(col("name")),
                pattern: Box::new(Expr::Literal(ScalarValue::Utf8(Some("%John%".to_string())))),
                case_insensitive: case_insensitive,
                negated: negated,
                escape_char: None,
            });

            let sql = to_sql_with_engine(&expr, Some(engine))?;
            assert_eq!(sql, expected);
        }
        Ok(())
    }
}
