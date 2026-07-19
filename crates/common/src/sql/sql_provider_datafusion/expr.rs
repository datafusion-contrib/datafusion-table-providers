use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::Result,
    logical_expr::Expr,
};

pub(super) fn expr_contains_subquery(expr: &Expr) -> Result<bool> {
    let mut contains_subquery = false;
    expr.apply(|expr| match expr {
        Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_) => {
            contains_subquery = true;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(contains_subquery)
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
        let expr = Expr::Literal(ScalarValue::Int32(Some(1)), None);
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
        let right = Expr::Literal(ScalarValue::Int32(Some(2)), None);
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
            Expr::Literal(ScalarValue::Int32(Some(1)), None),
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
