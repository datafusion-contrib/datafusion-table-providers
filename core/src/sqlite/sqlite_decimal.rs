use arrow_schema::{DataType, SchemaRef};
use datafusion::sql::sqlparser::ast::{
    self, visit_expressions, Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, Ident,
    ObjectName, ObjectNamePart, VisitorMut,
};
use std::collections::HashSet;
use std::ops::ControlFlow;

pub struct SQLiteDecimalRewriter {
    decimal_idents: HashSet<String>,
}

impl SQLiteDecimalRewriter {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            decimal_idents: schema
                .fields
                .iter()
                .filter_map(|f| {
                    if let Some(_) = Self::decimal_precision_scale(f.data_type()) {
                        Some(f.name().clone())
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }

    fn decimal_precision_scale(data_type: &DataType) -> Option<(u8, i8)> {
        if let DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) = data_type
        {
            Some((*p, *s))
        } else {
            None
        }
    }

    fn is_sum_function(func: &ast::Function) -> bool {
        let ObjectName(ref parts) = func.name;
        parts
            .first()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.to_lowercase() == "sum")
            .unwrap_or(false)
    }

    fn function_has_decimal_ident(&self, func: &ast::Function) -> bool {
        let ident_found = visit_expressions(func, |inner_expr| match inner_expr {
            Expr::Identifier(ident) if self.decimal_idents.contains(&ident.value) => {
                ControlFlow::Break(true)
            }
            Expr::CompoundIdentifier(idents)
                if idents.last().is_some()
                    && self.decimal_idents.contains(&idents.last().unwrap().value) =>
            {
                ControlFlow::Break(true)
            }
            _ => ControlFlow::Continue(()),
        });

        ident_found.break_value().unwrap_or(false)
    }

    fn wrap_in_decimal(expr: Expr) -> Expr {
        Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal"))]),
            args: ast::FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))],
                clauses: Vec::new(),
            }),
            over: None,
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            filter: None,
            null_treatment: None,
            within_group: Vec::new(),
        })
    }

    fn wrap_in_decimal_literal(s: String) -> Expr {
        Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal"))]),
            args: ast::FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    ast::Value::SingleQuotedString(s).into(),
                )))],
                clauses: Vec::new(),
            }),
            over: None,
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            filter: None,
            null_treatment: None,
            within_group: Vec::new(),
        })
    }
}

impl VisitorMut for SQLiteDecimalRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            // push down decimal_sum()
            Expr::Function(func)
                if Self::is_sum_function(func) && self.function_has_decimal_ident(func) =>
            {
                func.name =
                    ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal_sum"))]);
            }
            // wrap decimal idents
            Expr::Identifier(ident) if self.decimal_idents.contains(&ident.value) => {
                *expr = Self::wrap_in_decimal(expr.clone());
            }
            Expr::CompoundIdentifier(idents) => {
                if let Some(last_ident) = idents.last() {
                    if self.decimal_idents.contains(&last_ident.value) {
                        *expr = Self::wrap_in_decimal(expr.clone());
                    }
                }
            }
            // wrap decimal literals
            Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(s, _),
                ..
            }) if s.contains(".") => *expr = Self::wrap_in_decimal_literal(s.clone()),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
