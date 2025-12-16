use arrow_schema::{DataType, SchemaRef};
use datafusion::logical_expr::sqlparser::ast::Query;
use datafusion::sql::sqlparser::ast::{
    self, visit_expressions, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, Ident, ObjectName, ObjectNamePart, SetExpr, VisitorMut,
};
use std::collections::HashSet;
use std::ops::ControlFlow;

pub struct SQLiteDecimalRewriter {
    decimal_idents: HashSet<String>,
    query_count: usize,
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
            query_count: 0,
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

    fn function_name_starts_with(func: &ast::Function, name: &str) -> bool {
        let ObjectName(ref parts) = func.name;
        parts
            .first()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.to_lowercase().starts_with(name))
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
        let cast_expr = Expr::Cast {
            expr: Box::new(expr),
            data_type: ast::DataType::Text,
            format: None,
            kind: ast::CastKind::Cast,
        };

        Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal"))]),
            args: ast::FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(cast_expr))],
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

    fn realias_decimal_functions(query: &mut Query) {
        match query.body.as_mut() {
            SetExpr::Select(select) => {
                let _ = visit_expressions_mut(&mut select.projection, |expr| {
                    let Expr::Function(func) = &expr else {
                        return ControlFlow::Continue(());
                    };

                    if !Self::function_name_starts_with(func, "decimal") {
                        return ControlFlow::Continue(());
                    }

                    if let Some(ident) = Self::extract_column_name_from_decimal(func) {
                        *expr = Expr::Named {
                            expr: Box::new(expr.clone()),
                            name: ident,
                        };
                    }

                    ControlFlow::<()>::Continue(())
                });
            }
            SetExpr::Query(inner) => Self::realias_decimal_functions(inner.as_mut()),
            _ => {}
        }
    }

    fn extract_column_name_from_decimal(func: &ast::Function) -> Option<Ident> {
        let maybe_ident = visit_expressions(func, |expr| match expr {
            Expr::Identifier(ident) => ControlFlow::Break(ident.clone()),
            Expr::CompoundIdentifier(idents) if !idents.is_empty() => {
                ControlFlow::Break(idents.last().unwrap().clone())
            }
            _ => ControlFlow::Continue(()),
        });

        maybe_ident.break_value()
    }
}

impl VisitorMut for SQLiteDecimalRewriter {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.query_count += 1;
        ControlFlow::Continue(())
    }

    /// After doing all rewrites, inspect the query selection for invocations of "decimal*" and
    /// percolate up an ident for them if they are nested
    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Lame, but have to count to leave the topmost select alone
        if self.query_count == 2 {
            Self::realias_decimal_functions(query);
        }

        self.query_count -= 1;

        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            // push down decimal_sum()
            Expr::Function(func)
                if Self::function_name_starts_with(func, "sum")
                    && self.function_has_decimal_ident(func) =>
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
