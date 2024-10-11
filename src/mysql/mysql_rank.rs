use datafusion::sql::sqlparser::ast::{Expr, Function, Ident, VisitorMut, WindowType};
use datafusion_expr::sqlparser::ast::WindowFrameBound;
use std::ops::ControlFlow;

#[derive(Default)]
pub struct MySQLRankVisitor {}

impl VisitorMut for MySQLRankVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            // match only for RANK()
            if let Some(Ident { value, .. }) = func.name.0.first() {
                if value.to_uppercase() == "RANK" {
                    Self::replace_rank(func);
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl MySQLRankVisitor {
    pub fn replace_rank(func: &mut Function) {
        if let Some(WindowType::WindowSpec(spec)) = func.over.as_mut() {
            spec.window_frame = None; // frame (window) clauses are ignored for rank() in MySQL: https://dev.mysql.com/doc/refman/8.4/en/window-functions-frames.html

            if let Some(order_by) = spec.order_by.first_mut() {
                order_by.nulls_first = None; // nulls first/last are not supported in MySQL
            }
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion_expr::sqlparser::ast::{ObjectName, WindowFrame};

    use super::*;

    #[test]
    fn test_replace_rank() {
        let mut func = Function {
            name: ObjectName(vec![Ident {
                value: "RANK".to_string(),
                quote_style: None,
            }]),
            args: datafusion_expr::sqlparser::ast::FunctionArguments::None,
            over: Some(WindowType::WindowSpec(
                datafusion_expr::sqlparser::ast::WindowSpec {
                    window_name: None,
                    partition_by: vec![],
                    order_by: vec![datafusion_expr::sqlparser::ast::OrderByExpr {
                        expr: datafusion_expr::sqlparser::ast::Expr::Wildcard,
                        asc: None,
                        nulls_first: Some(true),
                        with_fill: None,
                    }],
                    window_frame: Some(WindowFrame {
                        units: datafusion_expr::sqlparser::ast::WindowFrameUnits::Rows,
                        start_bound: datafusion_expr::sqlparser::ast::WindowFrameBound::CurrentRow,
                        end_bound: None,
                    }),
                },
            )),
            parameters: datafusion_expr::sqlparser::ast::FunctionArguments::None,
            null_treatment: None,
            filter: None,
            within_group: vec![],
        };

        let expected = Some(WindowType::WindowSpec(
            datafusion_expr::sqlparser::ast::WindowSpec {
                window_name: None,
                partition_by: vec![],
                order_by: vec![datafusion_expr::sqlparser::ast::OrderByExpr {
                    expr: datafusion_expr::sqlparser::ast::Expr::Wildcard,
                    asc: None,
                    nulls_first: None,
                    with_fill: None,
                }],
                window_frame: None,
            },
        ));

        MySQLRankVisitor::replace_rank(&mut func);

        assert_eq!(func.over, expected);
    }
}
