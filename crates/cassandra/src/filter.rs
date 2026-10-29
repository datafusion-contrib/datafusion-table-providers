//! Pushing filters on key columns down to Cassandra.
//!
//! Without `ALLOW FILTERING`, which makes Cassandra scan the whole table, a CQL `WHERE` clause
//! may only restrict key columns, and only in these ways:
//!
//! * either every partition key column is restricted by `=` or `IN`, or none is;
//! * the clustering columns may only be restricted if the partition key is, and only in order:
//!   any number of them by `=`, then optionally the next one by a range (`<`, `<=`, `>`, `>=`),
//!   with at most one lower and one upper bound.
//!
//! [`select_pushed_down_filters`] picks the filters that make up such a `WHERE` clause. Only
//! comparisons of a key column with a literal are considered, and only for the CQL types whose
//! comparison in Cassandra gives the same result as in DataFusion, so that the pushed down
//! filters are exact. All the other filters are evaluated by DataFusion.

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::utils::split_binary;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

use crate::conn::quote_identifier;

/// The primary key of a Cassandra table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryKey {
    /// The partition key columns, in order.
    partition_key: Vec<KeyColumn>,
    /// The clustering columns, in order.
    clustering_key: Vec<KeyColumn>,
}

impl PrimaryKey {
    pub fn new(partition_key: Vec<KeyColumn>, clustering_key: Vec<KeyColumn>) -> Self {
        PrimaryKey {
            partition_key,
            clustering_key,
        }
    }
}

/// A primary key column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyColumn {
    name: String,
    /// The CQL type, as in `system_schema.columns`, e.g. `int` or `frozen<list<int>>`.
    cql_type: String,
}

impl KeyColumn {
    pub fn new(name: impl Into<String>, cql_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            cql_type: cql_type.into(),
        }
    }
}

/// A restriction of a key column, with its values as CQL literals.
#[derive(Debug, Clone, PartialEq)]
enum Restriction {
    Eq(String),
    In(Vec<String>),
    /// `>` (exclusive) or `>=`.
    Lower {
        value: String,
        inclusive: bool,
    },
    /// `<` (exclusive) or `<=`.
    Upper {
        value: String,
        inclusive: bool,
    },
}

impl Restriction {
    fn to_cql(&self, column: &str) -> String {
        let column = quote_identifier(column);
        match self {
            Self::Eq(value) => format!("{column} = {value}"),
            Self::In(values) => format!("{column} IN ({})", values.join(", ")),
            Self::Lower { value, inclusive } => {
                format!("{column} {} {value}", if *inclusive { ">=" } else { ">" })
            }
            Self::Upper { value, inclusive } => {
                format!("{column} {} {value}", if *inclusive { "<=" } else { "<" })
            }
        }
    }
}

/// The filters to push down to Cassandra.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct PushedDownFilters {
    /// For each filter, whether it is pushed down.
    pub pushed_down: Vec<bool>,
    /// The relations of the CQL `WHERE` clause, to be joined with `AND`.
    pub relations: Vec<String>,
}

/// Selects the filters that can be pushed down to Cassandra as a CQL `WHERE` clause, see the
/// module documentation.
pub(crate) fn select_pushed_down_filters(
    primary_key: &PrimaryKey,
    filters: &[&Expr],
) -> PushedDownFilters {
    let mut pushed_down = vec![false; filters.len()];
    let mut relations = vec![];
    if primary_key.partition_key.is_empty() {
        return PushedDownFilters {
            pushed_down,
            relations,
        };
    }

    let restrictions: Vec<Option<(&str, Restriction)>> = filters
        .iter()
        .map(|filter| key_column_restriction(primary_key, filter))
        .collect();
    // Finds the first filter restricting `column` with a restriction accepted by `accept`.
    // Further restrictions of the same kind are left to DataFusion: CQL rejects them.
    let find = |column: &str, accept: fn(&Restriction) -> bool| {
        restrictions.iter().position(|r| {
            r.as_ref()
                .is_some_and(|(name, restriction)| *name == column && accept(restriction))
        })
    };
    let mut push_down = |index: usize, column: &str| {
        pushed_down[index] = true;
        let (_, restriction) = restrictions[index].as_ref().expect("restriction exists");
        relations.push(restriction.to_cql(column));
    };

    let partition_key_filters: Option<Vec<usize>> = primary_key
        .partition_key
        .iter()
        .map(|column| {
            find(&column.name, |r| {
                matches!(r, Restriction::Eq(_) | Restriction::In(_))
            })
        })
        .collect();
    let Some(partition_key_filters) = partition_key_filters else {
        // CQL can't restrict only some of the partition key columns, nor the clustering
        // columns without the partition key
        return PushedDownFilters {
            pushed_down,
            relations,
        };
    };
    for (index, column) in partition_key_filters
        .into_iter()
        .zip(&primary_key.partition_key)
    {
        push_down(index, &column.name);
    }

    for column in &primary_key.clustering_key {
        if let Some(index) = find(&column.name, |r| matches!(r, Restriction::Eq(_))) {
            push_down(index, &column.name);
            continue;
        }
        // A range restriction ends the restricted clustering columns
        let lower = find(&column.name, |r| matches!(r, Restriction::Lower { .. }));
        let upper = find(&column.name, |r| matches!(r, Restriction::Upper { .. }));
        for index in [lower, upper].into_iter().flatten() {
            push_down(index, &column.name);
        }
        break;
    }

    PushedDownFilters {
        pushed_down,
        relations,
    }
}

/// Returns the key column `filter` restricts and the restriction, if CQL can evaluate it
/// exactly like DataFusion does.
fn key_column_restriction<'a>(
    primary_key: &'a PrimaryKey,
    filter: &Expr,
) -> Option<(&'a str, Restriction)> {
    let key_column = |column: &Column| {
        let is_partition_key = primary_key
            .partition_key
            .iter()
            .any(|c| c.name == column.name);
        primary_key
            .partition_key
            .iter()
            .chain(&primary_key.clustering_key)
            .find(|c| c.name == column.name)
            .map(|c| (c, is_partition_key))
    };

    match filter {
        // DataFusion rewrites short `IN` lists to `OR`s of `=`
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Or, ..
        }) => {
            let mut column = None;
            let mut literals = vec![];
            for disjunct in split_binary(filter, Operator::Or) {
                let (name, restriction) = key_column_restriction(primary_key, disjunct)?;
                if *column.get_or_insert(name) != name {
                    return None;
                }
                match restriction {
                    Restriction::Eq(literal) => literals.push(literal),
                    Restriction::In(values) => literals.extend(values),
                    _ => return None,
                }
            }
            let column = column?;
            let is_partition_key = primary_key.partition_key.iter().any(|c| c.name == column);
            is_partition_key.then_some((column, Restriction::In(literals)))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (column, op, value) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(value, _)) => (column, *op, value),
                (Expr::Literal(value, _), Expr::Column(column)) => (column, op.swap()?, value),
                _ => return None,
            };
            let (key_column, is_partition_key) = key_column(column)?;
            let literal = cql_literal(&key_column.cql_type, value, is_partition_key)?;
            if op != Operator::Eq && !supports_range(&key_column.cql_type) {
                return None;
            }
            let restriction = match op {
                Operator::Eq => Restriction::Eq(literal),
                Operator::Gt | Operator::GtEq => Restriction::Lower {
                    value: literal,
                    inclusive: op == Operator::GtEq,
                },
                Operator::Lt | Operator::LtEq => Restriction::Upper {
                    value: literal,
                    inclusive: op == Operator::LtEq,
                },
                _ => return None,
            };
            Some((&key_column.name, restriction))
        }
        Expr::InList(in_list) if !in_list.negated && !in_list.list.is_empty() => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return None;
            };
            let (key_column, is_partition_key) = key_column(column)?;
            let literals = in_list
                .list
                .iter()
                .map(|value| match value {
                    Expr::Literal(value, _) => {
                        cql_literal(&key_column.cql_type, value, is_partition_key)
                    }
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            // CQL supports `IN` on clustering columns too, but with more caveats, so only
            // the partition key is restricted with it
            is_partition_key.then_some((key_column.name.as_str(), Restriction::In(literals)))
        }
        _ => None,
    }
}

/// Whether ordering values of `cql_type` in Cassandra gives the same order as in DataFusion.
///
/// This isn't the case for UUIDs (`timeuuid`s are ordered by time, `uuid`s by version first),
/// whose values are strings in DataFusion.
fn supports_range(cql_type: &str) -> bool {
    !matches!(cql_type, "uuid" | "timeuuid")
}

/// Formats `value` as a CQL literal of `cql_type`.
///
/// Returns `None` if the value isn't one of the column, as DataFusion produces it, or if
/// Cassandra would reject the query or compare the values differently than DataFusion:
/// in these cases DataFusion must evaluate the filter.
fn cql_literal(cql_type: &str, value: &ScalarValue, is_partition_key: bool) -> Option<String> {
    let literal = match (cql_type, value) {
        ("tinyint", ScalarValue::Int8(Some(v))) => v.to_string(),
        ("smallint", ScalarValue::Int16(Some(v))) => v.to_string(),
        ("int", ScalarValue::Int32(Some(v))) => v.to_string(),
        ("bigint", ScalarValue::Int64(Some(v))) => v.to_string(),
        ("boolean", ScalarValue::Boolean(Some(v))) => v.to_string(),
        // Cassandra rejects empty partition keys, while DataFusion just finds no rows
        ("text" | "varchar", ScalarValue::Utf8(Some(s))) if !(is_partition_key && s.is_empty()) => {
            quote_string(s)
        }
        // Cassandra rejects non-ASCII values for `ascii` columns
        ("ascii", ScalarValue::Utf8(Some(s)))
            if s.is_ascii() && !(is_partition_key && s.is_empty()) =>
        {
            quote_string(s)
        }
        ("blob", ScalarValue::Binary(Some(bytes))) if !(is_partition_key && bytes.is_empty()) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("0x{hex}")
        }
        // An integer `date` literal is the number of days, with the epoch at 2^31
        ("date", ScalarValue::Date32(Some(days))) => (i64::from(*days) + (1 << 31)).to_string(),
        // An integer `timestamp` literal is the number of milliseconds since the epoch
        ("timestamp", ScalarValue::TimestampMillisecond(Some(millis), _)) => millis.to_string(),
        // An integer `time` literal is the number of nanoseconds since midnight
        ("time", ScalarValue::Time64Nanosecond(Some(nanos))) => nanos.to_string(),
        // DataFusion compares UUIDs as strings, so only the canonical form, in which Cassandra
        // returns them, matches the same rows. Cassandra rejects `timeuuid`s of other versions
        ("uuid" | "timeuuid", ScalarValue::Utf8(Some(s))) => {
            let uuid = uuid::Uuid::try_parse(s).ok()?;
            if uuid.to_string() != *s || (cql_type == "timeuuid" && uuid.get_version_num() != 1) {
                return None;
            }
            uuid.to_string()
        }
        _ => return None,
    };
    Some(literal)
}

fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    /// `PRIMARY KEY ((p1, p2), c1, c2)`
    fn primary_key() -> PrimaryKey {
        PrimaryKey {
            partition_key: vec![KeyColumn::new("p1", "int"), KeyColumn::new("p2", "text")],
            clustering_key: vec![
                KeyColumn::new("c1", "bigint"),
                KeyColumn::new("c2", "timeuuid"),
            ],
        }
    }

    fn select(filters: &[Expr]) -> (Vec<bool>, String) {
        let filters: Vec<&Expr> = filters.iter().collect();
        let selected = select_pushed_down_filters(&primary_key(), &filters);
        (selected.pushed_down, selected.relations.join(" AND "))
    }

    #[test]
    fn pushes_down_the_partition_key() {
        assert_eq!(
            select(&[
                col("p2").eq(lit("a'b")),
                col("v").eq(lit(1)),
                lit(1).eq(col("p1")),
            ]),
            (
                vec![true, false, true],
                r#""p1" = 1 AND "p2" = 'a''b'"#.to_string()
            )
        );
    }

    #[test]
    fn pushes_down_in_lists_on_the_partition_key() {
        assert_eq!(
            select(&[
                col("p1").in_list(vec![lit(1), lit(2)], false),
                col("p2").eq(lit("a"))
            ]),
            (
                vec![true, true],
                r#""p1" IN (1, 2) AND "p2" = 'a'"#.to_string()
            )
        );
        assert_eq!(
            select(&[
                col("p1").in_list(vec![lit(1)], true),
                col("p2").eq(lit("a"))
            ]),
            (vec![false, false], String::new())
        );
    }

    #[test]
    fn pushes_down_disjunctions_on_a_partition_key_column_as_in_lists() {
        assert_eq!(
            select(&[
                col("p1")
                    .eq(lit(1))
                    .or(lit(2).eq(col("p1")))
                    .or(col("p1").in_list(vec![lit(3)], false)),
                col("p2").eq(lit("a")),
            ]),
            (
                vec![true, true],
                r#""p1" IN (1, 2, 3) AND "p2" = 'a'"#.to_string()
            )
        );
        for filter in [
            col("p1").eq(lit(1)).or(col("p2").eq(lit("b"))),
            col("p1").eq(lit(1)).or(col("p1").gt(lit(2))),
            col("p1").eq(lit(1)).or(col("v").eq(lit(2))),
        ] {
            assert_eq!(
                select(&[filter.clone(), col("p2").eq(lit("a"))]),
                (vec![false, false], String::new()),
                "{filter}"
            );
        }
        // only the partition key is restricted with `IN`
        assert_eq!(
            select(&[
                col("p1").eq(lit(1)),
                col("p2").eq(lit("a")),
                col("c1").eq(lit(1i64)).or(col("c1").eq(lit(2i64))),
            ]),
            (
                vec![true, true, false],
                r#""p1" = 1 AND "p2" = 'a'"#.to_string()
            )
        );
    }

    #[test]
    fn keeps_partial_partition_keys() {
        assert_eq!(
            select(&[col("p1").eq(lit(1)), col("c1").eq(lit(1i64))]),
            (vec![false, false], String::new())
        );
    }

    #[test]
    fn keeps_unsupported_partition_key_filters() {
        for filter in [
            col("p1").gt(lit(1)),
            col("p1").not_eq(lit(1)),
            col("p1").eq(lit(1i64)), // not the type of the column
            col("p1").eq(lit(ScalarValue::Int32(None))),
            col("p1").in_list(vec![lit(1), col("p1")], false),
        ] {
            assert_eq!(
                select(&[filter.clone(), col("p2").eq(lit("a"))]),
                (vec![false, false], String::new()),
                "{filter}"
            );
        }
        assert_eq!(
            select(&[col("p1").eq(lit(1)), col("p2").eq(lit(""))]),
            (vec![false, false], String::new())
        );
    }

    #[test]
    fn pushes_down_clustering_columns_in_order() {
        assert_eq!(
            select(&[
                col("p1").eq(lit(1)),
                col("p2").eq(lit("a")),
                col("c1").eq(lit(2i64)),
                col("c2").eq(lit("50554d6e-29bb-11e5-b345-feff819cdc9f")),
            ]),
            (
                vec![true, true, true, true],
                r#""p1" = 1 AND "p2" = 'a' AND "c1" = 2 AND "c2" = 50554d6e-29bb-11e5-b345-feff819cdc9f"#
                    .to_string()
            )
        );
        // c2 can't be restricted without c1
        assert_eq!(
            select(&[
                col("p1").eq(lit(1)),
                col("p2").eq(lit("a")),
                col("c2").eq(lit("50554d6e-29bb-11e5-b345-feff819cdc9f")),
            ]),
            (
                vec![true, true, false],
                r#""p1" = 1 AND "p2" = 'a'"#.to_string()
            )
        );
    }

    #[test]
    fn pushes_down_one_range_on_a_clustering_column() {
        assert_eq!(
            select(&[
                col("p1").eq(lit(1)),
                col("p2").eq(lit("a")),
                col("c1").lt_eq(lit(9i64)),
                col("c1").gt(lit(2i64)),
                col("c1").gt_eq(lit(3i64)),
                col("c2").eq(lit("50554d6e-29bb-11e5-b345-feff819cdc9f")),
            ]),
            (
                vec![true, true, true, true, false, false],
                r#""p1" = 1 AND "p2" = 'a' AND "c1" > 2 AND "c1" <= 9"#.to_string()
            )
        );
        assert_eq!(
            select(&[
                col("p1").eq(lit(1)),
                col("p2").eq(lit("a")),
                lit(2i64).lt(col("c1")),
            ]),
            (
                vec![true, true, true],
                r#""p1" = 1 AND "p2" = 'a' AND "c1" > 2"#.to_string()
            )
        );
    }

    #[test]
    fn keeps_duplicate_restrictions() {
        assert_eq!(
            select(&[
                col("p1").eq(lit(1)),
                col("p1").eq(lit(2)),
                col("p2").eq(lit("a")),
                col("p2").in_list(vec![lit("a")], false),
            ]),
            (
                vec![true, false, true, false],
                r#""p1" = 1 AND "p2" = 'a'"#.to_string()
            )
        );
    }

    #[test]
    fn keeps_ranges_and_non_canonical_values_of_uuids() {
        let key = |c2| {
            vec![
                col("p1").eq(lit(1)),
                col("p2").eq(lit("a")),
                col("c1").eq(lit(1i64)),
                c2,
            ]
        };
        for c2 in [
            col("c2").gt(lit("50554d6e-29bb-11e5-b345-feff819cdc9f")),
            col("c2").eq(lit("50554D6E-29BB-11E5-B345-FEFF819CDC9F")),
            // a version 4 UUID
            col("c2").eq(lit("123e4567-e89b-42d3-a456-426614174000")),
        ] {
            assert_eq!(
                select(&key(c2.clone())).0,
                vec![true, true, true, false],
                "{c2}"
            );
        }
    }

    #[test]
    fn pushes_down_nothing_without_a_primary_key() {
        let filter = col("p1").eq(lit(1));
        assert_eq!(
            select_pushed_down_filters(&PrimaryKey::default(), &[&filter]),
            PushedDownFilters {
                pushed_down: vec![false],
                relations: vec![],
            }
        );
    }

    #[test]
    fn formats_literals() {
        let cases = [
            ("tinyint", ScalarValue::Int8(Some(-1)), "-1"),
            ("smallint", ScalarValue::Int16(Some(2)), "2"),
            ("boolean", ScalarValue::Boolean(Some(true)), "true"),
            ("ascii", ScalarValue::Utf8(Some("x".into())), "'x'"),
            ("varchar", ScalarValue::Utf8(Some("".into())), "''"),
            (
                "blob",
                ScalarValue::Binary(Some(vec![0xca, 0xfe])),
                "0xcafe",
            ),
            ("date", ScalarValue::Date32(Some(0)), "2147483648"),
            ("date", ScalarValue::Date32(Some(-1)), "2147483647"),
            (
                "timestamp",
                ScalarValue::TimestampMillisecond(Some(1_705_314_030_123), Some("UTC".into())),
                "1705314030123",
            ),
            ("time", ScalarValue::Time64Nanosecond(Some(5)), "5"),
            (
                "uuid",
                ScalarValue::Utf8(Some("123e4567-e89b-12d3-a456-426614174000".into())),
                "123e4567-e89b-12d3-a456-426614174000",
            ),
        ];
        for (cql_type, value, expected) in cases {
            assert_eq!(
                cql_literal(cql_type, &value, false).as_deref(),
                Some(expected),
                "{cql_type} {value}"
            );
        }

        for (cql_type, value) in [
            ("ascii", ScalarValue::Utf8(Some("é".into()))),
            ("double", ScalarValue::Float64(Some(1.0))),
            ("decimal", ScalarValue::Utf8(Some("1.0".into()))),
            ("inet", ScalarValue::Utf8(Some("127.0.0.1".into()))),
            ("frozen<list<int>>", ScalarValue::Int32(Some(1))),
            ("int", ScalarValue::Utf8(Some("1".into()))),
        ] {
            assert_eq!(
                cql_literal(cql_type, &value, false),
                None,
                "{cql_type} {value}"
            );
        }
        assert_eq!(
            cql_literal("blob", &ScalarValue::Binary(Some(vec![])), true),
            None
        );
    }
}
