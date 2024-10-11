use datafusion::sql::sqlparser::ast::{Expr, Function, Ident, VisitorMut, WindowType};
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
                    Self::replace_rank_window(func);
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl MySQLRankVisitor {
    pub fn replace_rank_window(func: &mut Function) {
        if let Some(WindowType::WindowSpec(spec)) = func.over.as_mut() {
            spec.window_frame = None; // frame (window) clauses are ignored in MySQL: https://dev.mysql.com/doc/refman/8.4/en/window-functions-frames.html

            if let Some(order_by) = spec.order_by.first_mut() {
                order_by.nulls_first = None; // nulls first/last are not supported in MySQL
            }
        }
    }
}
