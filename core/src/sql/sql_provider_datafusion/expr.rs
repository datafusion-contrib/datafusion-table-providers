use datafusion::{
    error::Result,
    logical_expr::{
        expr::{Alias, InList, WindowFunction},
        BinaryExpr, Case, Cast, Expr, GroupingSet, TryCast,
    },
};

pub(super) fn expr_contains_subquery(expr: &Expr) -> Result<bool> {
    match expr {
        Expr::InList(InList { expr, list, .. }) => {
            let list_expr = list
                .iter()
                .map(expr_contains_subquery)
                .collect::<Result<Vec<_>>>()?;
            Ok(list_expr.iter().any(|e| *e) || expr_contains_subquery(expr)?)
        }
        Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_) => Ok(true),
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            Ok(expr_contains_subquery(left)? || expr_contains_subquery(right)?)
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            if let Some(e) = expr {
                if expr_contains_subquery(e)? {
                    return Ok(true);
                }
            }
            for (when, then) in when_then_expr {
                if expr_contains_subquery(when)? || expr_contains_subquery(then)? {
                    return Ok(true);
                }
            }
            if let Some(e) = else_expr {
                if expr_contains_subquery(e)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
            expr_contains_subquery(expr)
        }
        Expr::Alias(Alias { expr, .. }) => expr_contains_subquery(expr),
        Expr::WindowFunction(WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        }) => {
            for arg in args {
                if expr_contains_subquery(arg)? {
                    return Ok(true);
                }
            }
            for part in partition_by {
                if expr_contains_subquery(part)? {
                    return Ok(true);
                }
            }
            for ord in order_by {
                if expr_contains_subquery(&ord.expr)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::AggregateFunction(agg) => {
            for arg in &agg.args {
                if expr_contains_subquery(arg)? {
                    return Ok(true);
                }
            }
            if let Some(filter) = &agg.filter {
                if expr_contains_subquery(filter)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(e)
        | Expr::Negative(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsNotTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => expr_contains_subquery(e),
        Expr::SimilarTo(like) | Expr::Like(like) => {
            if expr_contains_subquery(&like.expr)? || expr_contains_subquery(&like.pattern)? {
                return Ok(true);
            }
            Ok(false)
        }
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::GroupingSets(sets) => {
                for set in sets {
                    for e in set {
                        if expr_contains_subquery(e)? {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            GroupingSet::Cube(cube) | GroupingSet::Rollup(cube) => {
                for e in cube {
                    if expr_contains_subquery(e)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        },
        Expr::Placeholder(_)
        | Expr::ScalarFunction(_)
        | Expr::Column(_)
        | Expr::Literal(_)
        | Expr::Wildcard { .. }
        | Expr::ScalarVariable(_, _) => Ok(false),
        Expr::Between(between) => {
            if expr_contains_subquery(&between.expr)?
                || expr_contains_subquery(&between.low)?
                || expr_contains_subquery(&between.high)?
            {
                return Ok(true);
            }
            Ok(false)
        }
        Expr::OuterReferenceColumn(_, _) => Ok(false),
        Expr::Unnest(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{
        col, expr::InList, BinaryExpr, Expr, LogicalPlanBuilder, Operator,
    };
    use datafusion::prelude::scalar_subquery;

    #[test]
    fn test_literal_no_subquery() {
        let expr = Expr::Literal(ScalarValue::Int32(Some(1)));
        assert!(!expr_contains_subquery(&expr).expect("literal should not contain subquery"));
    }

    #[test]
    fn test_scalar_subquery() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let subquery = scalar_subquery(plan.into());
        assert!(expr_contains_subquery(&subquery).expect("subquery should contain subquery"));
    }

    #[test]
    fn test_binary_expr_with_subquery() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let left = scalar_subquery(plan.into());
        let right = Expr::Literal(ScalarValue::Int32(Some(2)));
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Eq,
            right: Box::new(right),
        });
        assert!(expr_contains_subquery(&expr).expect("binary expr should contain subquery"));
    }

    #[test]
    fn test_inlist_with_subquery() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let list = vec![
            Expr::Literal(ScalarValue::Int32(Some(1))),
            scalar_subquery(plan.into()),
        ];
        let expr = Expr::InList(InList {
            expr: Box::new(col("a")),
            list,
            negated: false,
        });
        assert!(expr_contains_subquery(&expr).expect("inlist should contain subquery"));
    }
}
