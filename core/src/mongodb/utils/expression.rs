use datafusion::{
    logical_expr::expr::{Cast, TryCast},
    logical_expr::{Expr, Operator},
    scalar::ScalarValue,
};
use mongodb::bson::{doc, Bson, Document, Regex as BsonRegex};

pub fn combine_exprs_with_and(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, e| acc.and(e.clone())))
}

/// Convert a comparison operator and extract (field, value) from either operand order.
/// Returns (field_name, bson_value) if one side is a column and the other a literal.
fn extract_comparison_operands(left: &Expr, right: &Expr) -> Option<(String, Bson)> {
    if let (Some(field), Some(value)) = (extract_column_name(left), extract_literal_value(right)) {
        Some((field, value))
    } else if let (Some(value), Some(field)) =
        (extract_literal_value(left), extract_column_name(right))
    {
        Some((field, value))
    } else {
        None
    }
}

/// For reversed operand order, flip the comparison operator.
fn needs_flip(left: &Expr, right: &Expr) -> bool {
    extract_column_name(left).is_none() && extract_column_name(right).is_some()
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::Lt => Operator::Gt,
        Operator::GtEq => Operator::LtEq,
        Operator::LtEq => Operator::GtEq,
        other => other, // Eq, NotEq are symmetric
    }
}

pub fn expr_to_mongo_filter(expr: &Expr) -> Option<Document> {
    match expr {
        Expr::BinaryExpr(binary) => {
            let op = if needs_flip(&binary.left, &binary.right) {
                flip_op(binary.op)
            } else {
                binary.op
            };

            match op {
                Operator::And => {
                    let l = expr_to_mongo_filter(&binary.left)?;
                    let r = expr_to_mongo_filter(&binary.right)?;
                    Some(doc! { "$and": [l, r] })
                }
                Operator::Or => {
                    let l = expr_to_mongo_filter(&binary.left)?;
                    let r = expr_to_mongo_filter(&binary.right)?;
                    Some(doc! { "$or": [l, r] })
                }
                Operator::Eq => {
                    let (field, value) = extract_comparison_operands(&binary.left, &binary.right)?;
                    Some(doc! { field: value })
                }
                Operator::Gt => {
                    let (field, value) = extract_comparison_operands(&binary.left, &binary.right)?;
                    Some(doc! { field: { "$gt": value } })
                }
                Operator::Lt => {
                    let (field, value) = extract_comparison_operands(&binary.left, &binary.right)?;
                    Some(doc! { field: { "$lt": value } })
                }
                Operator::GtEq => {
                    let (field, value) = extract_comparison_operands(&binary.left, &binary.right)?;
                    Some(doc! { field: { "$gte": value } })
                }
                Operator::LtEq => {
                    let (field, value) = extract_comparison_operands(&binary.left, &binary.right)?;
                    Some(doc! { field: { "$lte": value } })
                }
                Operator::NotEq => {
                    let (field, value) = extract_comparison_operands(&binary.left, &binary.right)?;
                    Some(doc! { field: { "$ne": value } })
                }
                _ => None,
            }
        }

        // IS NULL
        Expr::IsNull(inner) => {
            let field = extract_column_name(inner)?;
            Some(doc! { field: { "$eq": Bson::Null } })
        }

        // IS NOT NULL
        Expr::IsNotNull(inner) => {
            let field = extract_column_name(inner)?;
            Some(doc! { field: { "$ne": Bson::Null } })
        }

        // NOT expr
        Expr::Not(inner) => {
            match inner.as_ref() {
                // Optimize NOT (col = val) patterns directly
                Expr::BinaryExpr(binary) => match binary.op {
                    Operator::Eq => {
                        let (field, value) =
                            extract_comparison_operands(&binary.left, &binary.right)?;
                        Some(doc! { field: { "$ne": value } })
                    }
                    _ => {
                        let inner_doc = expr_to_mongo_filter(inner)?;
                        Some(doc! { "$nor": [inner_doc] })
                    }
                },
                _ => {
                    let inner_doc = expr_to_mongo_filter(inner)?;
                    Some(doc! { "$nor": [inner_doc] })
                }
            }
        }

        // IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
        Expr::IsTrue(inner) => {
            let field = extract_column_name(inner)?;
            Some(doc! { field: { "$eq": true } })
        }
        Expr::IsFalse(inner) => {
            let field = extract_column_name(inner)?;
            Some(doc! { field: { "$eq": false } })
        }
        Expr::IsNotTrue(inner) => {
            let field = extract_column_name(inner)?;
            Some(doc! { field: { "$ne": true } })
        }
        Expr::IsNotFalse(inner) => {
            let field = extract_column_name(inner)?;
            Some(doc! { field: { "$ne": false } })
        }

        // BETWEEN low AND high / NOT BETWEEN low AND high
        Expr::Between(between) => {
            let field = extract_column_name(&between.expr)?;
            let low = extract_literal_value(&between.low)?;
            let high = extract_literal_value(&between.high)?;
            if between.negated {
                // NOT BETWEEN: field < low OR field > high
                Some(doc! {
                    "$or": [
                        { &field: { "$lt": low } },
                        { &field: { "$gt": high } }
                    ]
                })
            } else {
                // BETWEEN: field >= low AND field <= high
                Some(doc! { field: { "$gte": low, "$lte": high } })
            }
        }

        // IN (list) / NOT IN (list)
        Expr::InList(in_list) => {
            let field = extract_column_name(&in_list.expr)?;
            let values: Option<Vec<Bson>> =
                in_list.list.iter().map(extract_literal_value).collect();
            let values = values?;
            if in_list.negated {
                Some(doc! { field: { "$nin": values } })
            } else {
                Some(doc! { field: { "$in": values } })
            }
        }

        // LIKE / NOT LIKE / ILIKE / NOT ILIKE
        Expr::Like(like) => {
            let field = extract_column_name(&like.expr)?;
            let pattern = extract_string_literal(&like.pattern)?;
            let regex_pattern = sql_like_to_regex(&pattern, like.escape_char);
            let options = if like.case_insensitive {
                "i".to_string()
            } else {
                String::new()
            };
            let regex = BsonRegex {
                pattern: regex_pattern,
                options,
            };
            if like.negated {
                Some(doc! { field: { "$not": Bson::RegularExpression(regex) } })
            } else {
                Some(doc! { field: Bson::RegularExpression(regex) })
            }
        }

        _ => None,
    }
}

/// Convert a SQL LIKE pattern to a MongoDB regex pattern.
/// `%` → `.*`, `_` → `.`, with proper escaping of regex metacharacters.
fn sql_like_to_regex(pattern: &str, escape_char: Option<char>) -> String {
    let mut regex = String::with_capacity(pattern.len() + 2);
    regex.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if Some(c) == escape_char {
            // Next character is literal
            if let Some(next) = chars.next() {
                regex.push_str(&regex_escape_char(next));
            }
        } else {
            match c {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                _ => regex.push_str(&regex_escape_char(c)),
            }
        }
    }

    regex.push('$');
    regex
}

/// Escape a single character for use in a regex pattern.
fn regex_escape_char(c: char) -> String {
    match c {
        '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '\\' | '^' | '$' | '|' => {
            format!("\\{c}")
        }
        _ => c.to_string(),
    }
}

fn extract_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        // Unwrap casts around columns (e.g., type coercion in comparisons)
        Expr::Cast(cast) => extract_column_name(&cast.expr),
        Expr::TryCast(cast) => extract_column_name(&cast.expr),
        _ => None,
    }
}

fn extract_literal_value(expr: &Expr) -> Option<Bson> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_bson(scalar),
        // Handle Cast(Literal(...), target_type) by evaluating the cast and converting the result.
        // Critical for timestamp/date filters: TIMESTAMP '2024-01-01' is parsed as
        // TimestampNanosecond but columns are often Timestamp(Millisecond, Some("UTC")),
        // and DataFusion wraps the literal in a Cast to reconcile.
        Expr::Cast(Cast { expr, data_type }) => {
            if let Expr::Literal(scalar, _) = expr.as_ref() {
                let casted = scalar.cast_to(data_type).ok()?;
                scalar_to_bson(&casted)
            } else {
                None
            }
        }
        Expr::TryCast(TryCast { expr, data_type }) => {
            if let Expr::Literal(scalar, _) = expr.as_ref() {
                let casted = scalar.cast_to(data_type).ok()?;
                scalar_to_bson(&casted)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn scalar_to_bson(scalar: &ScalarValue) -> Option<Bson> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Some(Bson::String(s.clone()))
        }
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => Some(Bson::Null),
        ScalarValue::Int32(Some(i)) => Some(Bson::Int32(*i)),
        ScalarValue::Int32(None) => Some(Bson::Null),
        ScalarValue::Int64(Some(i)) => Some(Bson::Int64(*i)),
        ScalarValue::Int64(None) => Some(Bson::Null),
        ScalarValue::Float32(Some(f)) => Some(Bson::Double(*f as f64)),
        ScalarValue::Float32(None) => Some(Bson::Null),
        ScalarValue::Float64(Some(f)) => Some(Bson::Double(*f)),
        ScalarValue::Float64(None) => Some(Bson::Null),
        ScalarValue::Boolean(Some(b)) => Some(Bson::Boolean(*b)),
        ScalarValue::Boolean(None) => Some(Bson::Null),

        ScalarValue::UInt8(Some(i)) => Some(Bson::Int32(*i as i32)),
        ScalarValue::UInt16(Some(i)) => Some(Bson::Int32(*i as i32)),
        ScalarValue::UInt32(Some(i)) => Some(Bson::Int64(*i as i64)),
        ScalarValue::UInt64(Some(i)) => {
            // Guard against overflow: u64 max > i64 max
            if *i <= i64::MAX as u64 {
                Some(Bson::Int64(*i as i64))
            } else {
                None
            }
        }
        ScalarValue::Int8(Some(i)) => Some(Bson::Int32(*i as i32)),
        ScalarValue::Int16(Some(i)) => Some(Bson::Int32(*i as i32)),

        // Timestamps → BSON DateTime (milliseconds since epoch)
        ScalarValue::TimestampSecond(Some(s), _) => Some(Bson::DateTime(
            mongodb::bson::DateTime::from_millis(*s * 1000),
        )),
        ScalarValue::TimestampMillisecond(Some(ms), _) => {
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(*ms)))
        }
        ScalarValue::TimestampMicrosecond(Some(us), _) => Some(Bson::DateTime(
            mongodb::bson::DateTime::from_millis(*us / 1000),
        )),
        ScalarValue::TimestampNanosecond(Some(ns), _) => Some(Bson::DateTime(
            mongodb::bson::DateTime::from_millis(*ns / 1_000_000),
        )),

        // Date32 → BSON DateTime (days since epoch → millis)
        ScalarValue::Date32(Some(days)) => {
            let millis = *days as i64 * 86_400_000;
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(millis)))
        }
        // Date64 → BSON DateTime (already millis)
        ScalarValue::Date64(Some(ms)) => {
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(*ms)))
        }

        // Decimal128 → BSON Decimal128
        ScalarValue::Decimal128(Some(val), _precision, scale) => {
            // Convert Arrow i128 representation to BSON Decimal128 via string
            let negative = *val < 0;
            let abs_val = val.unsigned_abs();
            let abs_scale = (*scale).unsigned_abs() as u32;
            let mut s = abs_val.to_string();
            if *scale > 0 {
                let scale_usize = abs_scale as usize;
                while s.len() <= scale_usize {
                    s.insert(0, '0');
                }
                let decimal_point = s.len() - scale_usize;
                s.insert(decimal_point, '.');
            }
            if negative {
                s.insert(0, '-');
            }
            match s.parse::<mongodb::bson::Decimal128>() {
                Ok(d) => Some(Bson::Decimal128(d)),
                Err(_) => None,
            }
        }

        ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::TimestampSecond(None, _)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::Date32(None)
        | ScalarValue::Date64(None)
        | ScalarValue::Decimal128(None, _, _) => Some(Bson::Null),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::BinaryExpr;
    use datafusion::prelude::{col, lit};

    #[test]
    fn test_combine_exprs_with_and() {
        let exprs = vec![col("age").gt(lit(30)), col("active").eq(lit(true))];

        let combined = combine_exprs_with_and(&exprs).unwrap();

        // Should produce (age > 30) AND (active = true)
        if let Expr::BinaryExpr(bin) = &combined {
            assert_eq!(bin.op, Operator::And);
        } else {
            panic!("Expected BinaryExpr with AND operator");
        }
    }

    #[test]
    fn test_simple_eq_filter() {
        let expr = col("name").eq(lit("Alice"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": "Alice" };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_gt_and_eq_filter() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("age").gt(lit(21))),
            op: Operator::And,
            right: Box::new(col("status").eq(lit("active"))),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$and": [
                { "age": { "$gt": 21 } },
                { "status": "active" }
            ]
        };

        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_eq_filter() {
        let expr = col("role").not_eq(lit("guest"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "role": { "$ne": "guest" } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_unsupported_expr_returns_none() {
        // Modulo operator is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("foo")),
            op: Operator::Modulo,
            right: Box::new(lit(2)),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_lt_filter() {
        let expr = col("age").lt(lit(65));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "age": { "$lt": 65 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_gte_filter() {
        let expr = col("score").gt_eq(lit(85));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "score": { "$gte": 85 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_lte_filter() {
        let expr = col("temperature").lt_eq(lit(100));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "temperature": { "$lte": 100 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_or_filter() {
        let expr = col("department")
            .eq(lit("sales"))
            .or(col("department").eq(lit("marketing")));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                { "department": "sales" },
                { "department": "marketing" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_complex_and_or_filter() {
        let age_and_status = col("age").gt(lit(25)).and(col("status").eq(lit("active")));
        let expr = age_and_status.or(col("priority").eq(lit("high")));

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                {
                    "$and": [
                        { "age": { "$gt": 25 } },
                        { "status": "active" }
                    ]
                },
                { "priority": "high" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_nested_and_filters() {
        let age_range = col("age").gt(lit(18)).and(col("age").lt(lit(65)));
        let expr = age_range.and(col("country").eq(lit("US")));

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$and": [
                {
                    "$and": [
                        { "age": { "$gt": 18 } },
                        { "age": { "$lt": 65 } }
                    ]
                },
                { "country": "US" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_boolean_filter() {
        let expr = col("is_verified").eq(lit(true));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "is_verified": true };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_float_filter() {
        let expr = col("price").gt(lit(99.99));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "price": { "$gt": 99.99 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_string_comparison_filters() {
        let expr = col("name").gt(lit("M"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": { "$gt": "M" } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_mixed_type_and_filter() {
        let expr = col("age").gt(lit(21)).and(col("name").eq(lit("John")));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$and": [
                { "age": { "$gt": 21 } },
                { "name": "John" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_single_expr_combine() {
        let exprs = vec![col("status").eq(lit("active"))];
        let combined = combine_exprs_with_and(&exprs).unwrap();

        if let Expr::BinaryExpr(bin) = &combined {
            assert_eq!(bin.op, Operator::Eq);
        } else {
            panic!("Expected BinaryExpr with EQ operator");
        }
    }

    #[test]
    fn test_empty_exprs_combine() {
        let exprs: Vec<Expr> = vec![];
        let result = combine_exprs_with_and(&exprs);
        assert!(result.is_none());
    }

    #[test]
    fn test_three_exprs_combine() {
        let exprs = vec![
            col("age").gt(lit(25)),
            col("status").eq(lit("active")),
            col("department").eq(lit("engineering")),
        ];
        let combined = combine_exprs_with_and(&exprs).unwrap();

        if let Expr::BinaryExpr(bin) = &combined {
            assert_eq!(bin.op, Operator::And);
            if let Expr::BinaryExpr(left_bin) = &*bin.left {
                assert_eq!(left_bin.op, Operator::And);
            } else {
                panic!("Expected nested AND on left side");
            }
        } else {
            panic!("Expected BinaryExpr with AND operator");
        }
    }

    #[test]
    fn test_null_literal_filter() {
        let expr = col("optional_field").eq(lit(ScalarValue::Utf8(None)));
        let filter = expr_to_mongo_filter(&expr);

        if let Some(doc) = filter {
            let expected = doc! { "optional_field": mongodb::bson::Bson::Null };
            assert_eq!(doc, expected);
        }
    }

    #[test]
    fn test_reversed_operand_order_eq() {
        use datafusion::logical_expr::Expr;

        // lit = col should be handled by flipping operands
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit("Alice")),
            op: Operator::Eq,
            right: Box::new(col("name")),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": "Alice" };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_reversed_operand_order_gt() {
        use datafusion::logical_expr::Expr;

        // 21 > col  →  col < 21
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(21)),
            op: Operator::Gt,
            right: Box::new(col("age")),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "age": { "$lt": 21 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_reversed_operand_order_lt() {
        use datafusion::logical_expr::Expr;

        // 10 < col  →  col > 10
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(10)),
            op: Operator::Lt,
            right: Box::new(col("age")),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "age": { "$gt": 10 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_multiple_or_conditions() {
        let expr = col("status")
            .eq(lit("active"))
            .or(col("status").eq(lit("pending")))
            .or(col("status").eq(lit("review")));

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                {
                    "$or": [
                        { "status": "active" },
                        { "status": "pending" }
                    ]
                },
                { "status": "review" }
            ]
        };
        assert_eq!(filter, expected);
    }

    // --- IS NULL / IS NOT NULL ---

    #[test]
    fn test_is_null_filter() {
        let expr = col("email").is_null();
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "email": { "$eq": Bson::Null } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_is_not_null_filter() {
        let expr = col("email").is_not_null();
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "email": { "$ne": Bson::Null } };
        assert_eq!(filter, expected);
    }

    // --- NOT ---

    #[test]
    fn test_not_eq_via_not() {
        use datafusion::logical_expr::Expr;
        // NOT(name = "Alice") → name != "Alice"
        let inner = col("name").eq(lit("Alice"));
        let expr = Expr::Not(Box::new(inner));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": { "$ne": "Alice" } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_gt_via_nor() {
        use datafusion::logical_expr::Expr;
        // NOT(age > 21) → $nor: [{age: {$gt: 21}}]
        let inner = col("age").gt(lit(21));
        let expr = Expr::Not(Box::new(inner));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "$nor": [{ "age": { "$gt": 21 } }] };
        assert_eq!(filter, expected);
    }

    // --- IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE ---

    #[test]
    fn test_is_true_filter() {
        let expr = col("active").is_true();
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "active": { "$eq": true } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_is_false_filter() {
        let expr = col("active").is_false();
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "active": { "$eq": false } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_is_not_true_filter() {
        let expr = col("active").is_not_true();
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "active": { "$ne": true } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_is_not_false_filter() {
        let expr = col("active").is_not_false();
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "active": { "$ne": false } };
        assert_eq!(filter, expected);
    }

    // --- BETWEEN ---

    #[test]
    fn test_between_filter() {
        let expr = col("age").between(lit(18), lit(65));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "age": { "$gte": 18, "$lte": 65 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_between_filter() {
        let expr = col("age").not_between(lit(18), lit(65));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                { "age": { "$lt": 18 } },
                { "age": { "$gt": 65 } }
            ]
        };
        assert_eq!(filter, expected);
    }

    // --- IN list ---

    #[test]
    fn test_in_list_filter() {
        let expr = col("status").in_list(vec![lit("active"), lit("pending")], false);
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "status": { "$in": ["active", "pending"] } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_in_list_filter() {
        let expr = col("status").in_list(vec![lit("deleted"), lit("banned")], true);
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "status": { "$nin": ["deleted", "banned"] } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_in_list_integers() {
        let expr = col("score").in_list(vec![lit(100), lit(200), lit(300)], false);
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "score": { "$in": [100, 200, 300] } };
        assert_eq!(filter, expected);
    }

    // --- LIKE ---

    #[test]
    fn test_like_starts_with() {
        let expr = col("name").like(lit("Al%"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "name": Bson::RegularExpression(BsonRegex { pattern: "^Al.*$".to_string(), options: String::new() }) }
        );
    }

    #[test]
    fn test_like_ends_with() {
        let expr = col("name").like(lit("%son"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "name": Bson::RegularExpression(BsonRegex { pattern: "^.*son$".to_string(), options: String::new() }) }
        );
    }

    #[test]
    fn test_like_contains() {
        let expr = col("name").like(lit("%ali%"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "name": Bson::RegularExpression(BsonRegex { pattern: "^.*ali.*$".to_string(), options: String::new() }) }
        );
    }

    #[test]
    fn test_like_single_char_wildcard() {
        let expr = col("code").like(lit("A_C"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "code": Bson::RegularExpression(BsonRegex { pattern: "^A.C$".to_string(), options: String::new() }) }
        );
    }

    #[test]
    fn test_not_like() {
        let expr = col("name").not_like(lit("%test%"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "name": { "$not": Bson::RegularExpression(BsonRegex { pattern: "^.*test.*$".to_string(), options: String::new() }) } }
        );
    }

    #[test]
    fn test_ilike_case_insensitive() {
        let expr = col("name").ilike(lit("%alice%"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "name": Bson::RegularExpression(BsonRegex { pattern: "^.*alice.*$".to_string(), options: "i".to_string() }) }
        );
    }

    #[test]
    fn test_not_ilike() {
        let expr = col("name").not_ilike(lit("admin%"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "name": { "$not": Bson::RegularExpression(BsonRegex { pattern: "^admin.*$".to_string(), options: "i".to_string() }) } }
        );
    }

    // --- sql_like_to_regex ---

    #[test]
    fn test_sql_like_to_regex_escapes_metacharacters() {
        let regex = sql_like_to_regex("price$100.00", None);
        assert_eq!(regex, r"^price\$100\.00$");
    }

    #[test]
    fn test_sql_like_to_regex_with_escape_char() {
        // Using \ as escape: \% is literal %, \_ is literal _
        let regex = sql_like_to_regex(r"100\%", Some('\\'));
        assert_eq!(regex, "^100%$");
    }

    // --- Edge cases ---

    #[test]
    fn test_two_literals_returns_none() {
        // Both sides are literals - should return None
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(1)),
            op: Operator::Eq,
            right: Box::new(lit(2)),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_two_columns_returns_none() {
        // Both sides are columns - should return None
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Eq,
            right: Box::new(col("b")),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_is_null_on_non_column_returns_none() {
        // IS NULL on a literal - should return None
        let expr = Expr::IsNull(Box::new(lit(42)));
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_is_not_null_on_non_column_returns_none() {
        let expr = Expr::IsNotNull(Box::new(lit(42)));
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_is_true_on_non_column_returns_none() {
        let expr = Expr::IsTrue(Box::new(lit(true)));
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_in_list_empty() {
        // IN () with empty list
        let expr = col("status").in_list(vec![], false);
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "status": { "$in": [] } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_in_list_empty() {
        let expr = col("status").in_list(vec![], true);
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "status": { "$nin": [] } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_in_list_single_element() {
        let expr = col("id").in_list(vec![lit(42)], false);
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "id": { "$in": [42] } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_in_list_with_unsupported_literal_returns_none() {
        // Binary is not in our scalar_to_bson mapping
        let expr = col("data").in_list(
            vec![Expr::Literal(
                ScalarValue::Binary(Some(vec![1, 2, 3])),
                None,
            )],
            false,
        );
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_in_list_on_non_column_returns_none() {
        // IN list where the expr is not a column
        let expr = Expr::InList(datafusion::logical_expr::expr::InList {
            expr: Box::new(lit(42)),
            list: vec![lit(1), lit(2)],
            negated: false,
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_between_on_non_column_returns_none() {
        let expr = Expr::Between(datafusion::logical_expr::expr::Between {
            expr: Box::new(lit(5)),
            negated: false,
            low: Box::new(lit(1)),
            high: Box::new(lit(10)),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_between_with_unsupported_bound_returns_none() {
        let expr = Expr::Between(datafusion::logical_expr::expr::Between {
            expr: Box::new(col("data")),
            negated: false,
            low: Box::new(Expr::Literal(ScalarValue::Binary(Some(vec![0])), None)),
            high: Box::new(Expr::Literal(ScalarValue::Binary(Some(vec![255])), None)),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_like_exact_match() {
        // LIKE with no wildcards - exact match regex
        let expr = col("code").like(lit("ABC"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "code": Bson::RegularExpression(BsonRegex { pattern: "^ABC$".to_string(), options: String::new() }) }
        );
    }

    #[test]
    fn test_like_on_non_column_returns_none() {
        let expr = Expr::Like(datafusion::logical_expr::expr::Like {
            negated: false,
            expr: Box::new(lit("hello")),
            pattern: Box::new(lit("%world%")),
            escape_char: None,
            case_insensitive: false,
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_like_with_non_string_pattern_returns_none() {
        let expr = Expr::Like(datafusion::logical_expr::expr::Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit(42)),
            escape_char: None,
            case_insensitive: false,
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_like_escape_underscore() {
        // Using # as escape char: #_ is literal underscore
        let regex = sql_like_to_regex("test#_value", Some('#'));
        assert_eq!(regex, "^test_value$");
    }

    #[test]
    fn test_like_pattern_with_regex_metacharacters() {
        // Pattern with dots and parens that need escaping
        let expr = col("email").like(lit("%.example.com"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        assert_eq!(
            filter,
            doc! { "email": Bson::RegularExpression(BsonRegex { pattern: r"^.*\.example\.com$".to_string(), options: String::new() }) }
        );
    }

    #[test]
    fn test_not_wrapping_unsupported_returns_none() {
        // NOT around something that can't be converted
        let modulo = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("x")),
            op: Operator::Modulo,
            right: Box::new(lit(2)),
        });
        let expr = Expr::Not(Box::new(modulo));
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_not_is_null() {
        // NOT(IS NULL) should work through $nor
        let inner = Expr::IsNull(Box::new(col("x")));
        let expr = Expr::Not(Box::new(inner));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "$nor": [{ "x": { "$eq": Bson::Null } }] };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_reversed_operand_gteq() {
        // 100 >= col  →  col <= 100
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(100)),
            op: Operator::GtEq,
            right: Box::new(col("score")),
        });
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "score": { "$lte": 100 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_reversed_operand_lteq() {
        // 10 <= col  →  col >= 10
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(10)),
            op: Operator::LtEq,
            right: Box::new(col("score")),
        });
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "score": { "$gte": 10 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_reversed_operand_neq() {
        // "admin" != col  →  col != "admin"
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit("admin")),
            op: Operator::NotEq,
            right: Box::new(col("role")),
        });
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "role": { "$ne": "admin" } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_deeply_nested_and_or() {
        // (a = 1 AND b = 2) OR (c = 3 AND d = 4)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c").eq(lit(3)).and(col("d").eq(lit(4)));
        let expr = left.or(right);

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                {
                    "$and": [
                        { "a": 1 },
                        { "b": 2 }
                    ]
                },
                {
                    "$and": [
                        { "c": 3 },
                        { "d": 4 }
                    ]
                }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_and_with_one_unsupported_child_returns_none() {
        // AND where one child can't be converted
        let supported = col("a").eq(lit(1));
        let unsupported = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("b")),
            op: Operator::Modulo,
            right: Box::new(lit(2)),
        });
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(supported),
            op: Operator::And,
            right: Box::new(unsupported),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_or_with_one_unsupported_child_returns_none() {
        let supported = col("a").eq(lit(1));
        let unsupported = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("b")),
            op: Operator::Modulo,
            right: Box::new(lit(2)),
        });
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(supported),
            op: Operator::Or,
            right: Box::new(unsupported),
        });
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_combine_exprs_preserves_order() {
        let exprs = vec![
            col("a").eq(lit(1)),
            col("b").eq(lit(2)),
            col("c").eq(lit(3)),
            col("d").eq(lit(4)),
        ];
        let combined = combine_exprs_with_and(&exprs).unwrap();
        let filter = expr_to_mongo_filter(&combined).unwrap();
        // Should be left-associative: ((a AND b) AND c) AND d
        assert!(filter.contains_key("$and"));
    }

    #[test]
    fn test_scalar_to_bson_all_integer_types() {
        assert_eq!(
            scalar_to_bson(&ScalarValue::Int8(Some(42))),
            Some(Bson::Int32(42))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::Int16(Some(1000))),
            Some(Bson::Int32(1000))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::Int32(Some(100_000))),
            Some(Bson::Int32(100_000))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::Int64(Some(1_000_000))),
            Some(Bson::Int64(1_000_000))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::UInt8(Some(255))),
            Some(Bson::Int32(255))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::UInt16(Some(65535))),
            Some(Bson::Int32(65535_i32))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::UInt32(Some(100_000))),
            Some(Bson::Int64(100_000))
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::UInt64(Some(1_000_000))),
            Some(Bson::Int64(1_000_000))
        );
    }

    #[test]
    fn test_scalar_to_bson_null_variants() {
        assert_eq!(scalar_to_bson(&ScalarValue::Utf8(None)), Some(Bson::Null));
        assert_eq!(
            scalar_to_bson(&ScalarValue::LargeUtf8(None)),
            Some(Bson::Null)
        );
        assert_eq!(scalar_to_bson(&ScalarValue::Int32(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::Int64(None)), Some(Bson::Null));
        assert_eq!(
            scalar_to_bson(&ScalarValue::Float32(None)),
            Some(Bson::Null)
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::Float64(None)),
            Some(Bson::Null)
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::Boolean(None)),
            Some(Bson::Null)
        );
        assert_eq!(scalar_to_bson(&ScalarValue::Int8(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::Int16(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::UInt8(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::UInt16(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::UInt32(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::UInt64(None)), Some(Bson::Null));
        assert_eq!(
            scalar_to_bson(&ScalarValue::TimestampSecond(None, None)),
            Some(Bson::Null)
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::TimestampMillisecond(None, None)),
            Some(Bson::Null)
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::TimestampMicrosecond(None, None)),
            Some(Bson::Null)
        );
        assert_eq!(
            scalar_to_bson(&ScalarValue::TimestampNanosecond(None, None)),
            Some(Bson::Null)
        );
        assert_eq!(scalar_to_bson(&ScalarValue::Date32(None)), Some(Bson::Null));
        assert_eq!(scalar_to_bson(&ScalarValue::Date64(None)), Some(Bson::Null));
        assert_eq!(
            scalar_to_bson(&ScalarValue::Decimal128(None, 18, 6)),
            Some(Bson::Null)
        );
    }

    #[test]
    fn test_scalar_to_bson_unsupported_returns_none() {
        // Binary and other exotic types remain unsupported
        assert!(scalar_to_bson(&ScalarValue::Binary(Some(vec![1, 2, 3]))).is_none());
    }

    #[test]
    fn test_scalar_to_bson_timestamp_types() {
        // TimestampSecond: 1_000_000 seconds since epoch → BSON DateTime at 1_000_000_000 ms
        let result = scalar_to_bson(&ScalarValue::TimestampSecond(Some(1_000_000), None));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                1_000_000_000
            )))
        );

        // TimestampMillisecond: 1_500_000_000 ms → same
        let result = scalar_to_bson(&ScalarValue::TimestampMillisecond(
            Some(1_500_000_000),
            None,
        ));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                1_500_000_000
            )))
        );

        // TimestampMicrosecond: 1_500_000_000_000 µs → 1_500_000_000 ms
        let result = scalar_to_bson(&ScalarValue::TimestampMicrosecond(
            Some(1_500_000_000_000),
            None,
        ));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                1_500_000_000
            )))
        );

        // TimestampNanosecond: 1_500_000_000_000_000 ns → 1_500_000_000 ms
        let result = scalar_to_bson(&ScalarValue::TimestampNanosecond(
            Some(1_500_000_000_000_000),
            None,
        ));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                1_500_000_000
            )))
        );

        // Timezone is ignored for BSON (always UTC)
        let tz = Some(std::sync::Arc::from("America/New_York"));
        let result = scalar_to_bson(&ScalarValue::TimestampMillisecond(Some(1000), tz));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(1000)))
        );
    }

    #[test]
    fn test_scalar_to_bson_date_types() {
        // Date32: 0 days = epoch
        let result = scalar_to_bson(&ScalarValue::Date32(Some(0)));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(0)))
        );

        // Date32: 1 day = 86_400_000 ms
        let result = scalar_to_bson(&ScalarValue::Date32(Some(1)));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                86_400_000
            )))
        );

        // Date32: negative days
        let result = scalar_to_bson(&ScalarValue::Date32(Some(-1)));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                -86_400_000
            )))
        );

        // Date64: already milliseconds
        let result = scalar_to_bson(&ScalarValue::Date64(Some(1_500_000_000_000)));
        assert_eq!(
            result,
            Some(Bson::DateTime(mongodb::bson::DateTime::from_millis(
                1_500_000_000_000
            )))
        );
    }

    #[test]
    fn test_scalar_to_bson_decimal128() {
        // 12345 with scale 2 → "123.45"
        let result = scalar_to_bson(&ScalarValue::Decimal128(Some(12345), 10, 2));
        if let Some(Bson::Decimal128(d)) = &result {
            assert_eq!(d.to_string(), "123.45");
        } else {
            panic!("Expected Bson::Decimal128, got: {result:?}");
        }

        // Negative value: -99999 with scale 3 → "-99.999"
        let result = scalar_to_bson(&ScalarValue::Decimal128(Some(-99999), 10, 3));
        if let Some(Bson::Decimal128(d)) = &result {
            assert_eq!(d.to_string(), "-99.999");
        } else {
            panic!("Expected Bson::Decimal128, got: {result:?}");
        }

        // Zero scale: 42 with scale 0 → "42"
        let result = scalar_to_bson(&ScalarValue::Decimal128(Some(42), 10, 0));
        if let Some(Bson::Decimal128(d)) = &result {
            assert_eq!(d.to_string(), "42");
        } else {
            panic!("Expected Bson::Decimal128, got: {result:?}");
        }
    }

    #[test]
    fn test_scalar_to_bson_uint64_overflow() {
        // u64::MAX > i64::MAX, should return None
        assert!(scalar_to_bson(&ScalarValue::UInt64(Some(u64::MAX))).is_none());
        // Just within i64 range should work
        assert_eq!(
            scalar_to_bson(&ScalarValue::UInt64(Some(i64::MAX as u64))),
            Some(Bson::Int64(i64::MAX))
        );
    }

    #[test]
    fn test_timestamp_filter_pushdown() {
        // Timestamp filter: created_at > TimestampMillisecond(1000)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("created_at")),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(1_700_000_000_000), None),
                None,
            )),
        });
        let filter = expr_to_mongo_filter(&expr);
        assert!(
            filter.is_some(),
            "Timestamp filters should now be pushed down"
        );
        let doc = filter.unwrap();
        assert!(
            doc.contains_key("created_at"),
            "Should filter on created_at"
        );
    }

    #[test]
    fn test_date32_filter_pushdown() {
        // Date32 filter: event_date = Date32(19000) (some date in 2022)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("event_date")),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Date32(Some(19000)), None)),
        });
        let filter = expr_to_mongo_filter(&expr);
        assert!(filter.is_some(), "Date32 filters should now be pushed down");
    }

    #[test]
    fn test_decimal128_filter_pushdown() {
        // Decimal128 filter: price > 99.99 (represented as 9999 with scale 2)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("price")),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(
                ScalarValue::Decimal128(Some(9999), 10, 2),
                None,
            )),
        });
        let filter = expr_to_mongo_filter(&expr);
        assert!(
            filter.is_some(),
            "Decimal128 filters should now be pushed down"
        );
    }

    #[test]
    fn test_large_utf8_literal() {
        let expr = col("name").eq(Expr::Literal(
            ScalarValue::LargeUtf8(Some("Alice".to_string())),
            None,
        ));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": "Alice" };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_sql_like_to_regex_empty_pattern() {
        let regex = sql_like_to_regex("", None);
        assert_eq!(regex, "^$");
    }

    #[test]
    fn test_sql_like_to_regex_only_percent() {
        let regex = sql_like_to_regex("%", None);
        assert_eq!(regex, "^.*$");
    }

    #[test]
    fn test_sql_like_to_regex_only_underscore() {
        let regex = sql_like_to_regex("_", None);
        assert_eq!(regex, "^.$");
    }

    #[test]
    fn test_sql_like_to_regex_escape_at_end() {
        // Escape char at the very end with nothing after it
        let regex = sql_like_to_regex(r"abc\", Some('\\'));
        assert_eq!(regex, "^abc$");
    }

    #[test]
    fn test_sql_like_to_regex_multiple_underscores() {
        let regex = sql_like_to_regex("A___Z", None);
        assert_eq!(regex, "^A...Z$");
    }

    // --- Cast / TryCast handling ---

    #[test]
    fn test_cast_timestamp_literal_pushdown() {
        use datafusion::arrow::datatypes::{DataType, TimeUnit};

        // Simulates DataFusion's output for:
        //   created_at >= TIMESTAMP '2024-06-01 00:00:00'
        // when the column is Timestamp(Millisecond, Some("UTC")) but the SQL literal
        // is parsed as Timestamp(Nanosecond, None) — DataFusion wraps the literal in a Cast.
        let ns_value = 1_717_200_000_000_000_000_i64; // 2024-06-01T00:00:00Z
        let cast_expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(ns_value), None),
                None,
            )),
            data_type: DataType::Timestamp(
                TimeUnit::Millisecond,
                Some(std::sync::Arc::from("UTC")),
            ),
        });

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("created_at")),
            op: Operator::GtEq,
            right: Box::new(cast_expr),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "created_at": { "$gte": mongodb::bson::DateTime::from_millis(1_717_200_000_000) } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_cast_int_literal_pushdown() {
        use datafusion::arrow::datatypes::DataType;

        let cast_expr = Expr::Cast(Cast {
            expr: Box::new(lit(42_i32)),
            data_type: DataType::Int64,
        });

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("count")),
            op: Operator::Gt,
            right: Box::new(cast_expr),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "count": { "$gt": 42_i64 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_try_cast_literal_pushdown() {
        use datafusion::arrow::datatypes::DataType;

        let try_cast_expr = Expr::TryCast(TryCast {
            expr: Box::new(lit(100_i32)),
            data_type: DataType::Int64,
        });

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("value")),
            op: Operator::Eq,
            right: Box::new(try_cast_expr),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "value": 100_i64 };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_cast_column_with_literal_pushdown() {
        use datafusion::arrow::datatypes::DataType;

        // Cast(col("age"), Int64) >= lit(30_i64) — column side wrapped in Cast
        let cast_col = Expr::Cast(Cast {
            expr: Box::new(col("age")),
            data_type: DataType::Int64,
        });

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(cast_col),
            op: Operator::GtEq,
            right: Box::new(lit(30_i64)),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "age": { "$gte": 30_i64 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_cast_timestamp_between_pushdown() {
        use datafusion::arrow::datatypes::{DataType, TimeUnit};

        let target_type =
            DataType::Timestamp(TimeUnit::Millisecond, Some(std::sync::Arc::from("UTC")));
        let low = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(1_704_067_200_000_000_000), None),
                None,
            )),
            data_type: target_type.clone(),
        });
        let high = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(1_735_689_600_000_000_000), None),
                None,
            )),
            data_type: target_type,
        });

        let expr = Expr::Between(datafusion::logical_expr::expr::Between {
            expr: Box::new(col("created_at")),
            negated: false,
            low: Box::new(low),
            high: Box::new(high),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "created_at": {
                "$gte": mongodb::bson::DateTime::from_millis(1_704_067_200_000),
                "$lte": mongodb::bson::DateTime::from_millis(1_735_689_600_000),
            }
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_cast_unsupported_target_type_returns_none() {
        use datafusion::arrow::datatypes::DataType;

        let cast_expr = Expr::Cast(Cast {
            expr: Box::new(lit("hello")),
            data_type: DataType::Binary,
        });
        assert!(extract_literal_value(&cast_expr).is_none());
    }

    #[test]
    fn test_cast_in_reversed_operand_order() {
        use datafusion::arrow::datatypes::{DataType, TimeUnit};

        let cast_expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(1_717_200_000_000_000_000), None),
                None,
            )),
            data_type: DataType::Timestamp(
                TimeUnit::Millisecond,
                Some(std::sync::Arc::from("UTC")),
            ),
        });

        // Cast(lit) > col  →  col < Cast(lit)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(cast_expr),
            op: Operator::Gt,
            right: Box::new(col("created_at")),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "created_at": { "$lt": mongodb::bson::DateTime::from_millis(1_717_200_000_000) } };
        assert_eq!(filter, expected);
    }
}
