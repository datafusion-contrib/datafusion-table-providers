//! The T-SQL flavour of SQL that is sent to Microsoft SQL Server.
//!
//! DataFusion's unparser is driven by [`MsSqlDialect`], but some T-SQL differences can't be
//! expressed through an unparser dialect (e.g. T-SQL has no `LIMIT` and no boolean literals),
//! and the SQL sent to the server is also produced by string manipulation (filter, sort and
//! limit pushdown). So every statement is finally passed through [`rewrite_sql`], which parses
//! it and rewrites the remaining differences in its AST.

use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::TimeUnit;
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, CharacterLength, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, Offset, OffsetRows, OrderBy, OrderByKind, Query, Select, SetExpr, Statement,
    TableFactor, Top, TopQuantity, UnaryOperator, Value, VisitMut, VisitorMut, WindowType,
};
use datafusion::sql::sqlparser::dialect::MsSqlDialect as MsSqlParserDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::unparser::dialect::{CharacterLengthStyle, Dialect};

/// The DataFusion unparser [`Dialect`] for Microsoft SQL Server (T-SQL).
#[derive(Debug, Default, Clone, Copy)]
pub struct MsSqlDialect {}

impl Dialect for MsSqlDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('[')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        false
    }

    fn float64_ast_dtype(&self) -> DataType {
        DataType::Float(ast::ExactNumberInfo::None)
    }

    fn utf8_cast_dtype(&self) -> DataType {
        DataType::Nvarchar(Some(CharacterLength::Max))
    }

    fn large_utf8_cast_dtype(&self) -> DataType {
        DataType::Nvarchar(Some(CharacterLength::Max))
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        CharacterLengthStyle::Length
    }

    // T-SQL TINYINT is unsigned
    fn int8_cast_dtype(&self) -> DataType {
        DataType::SmallInt(None)
    }

    fn timestamp_cast_dtype(&self, _time_unit: &TimeUnit, tz: &Option<Arc<str>>) -> DataType {
        if tz.is_some() {
            custom_type("DATETIMEOFFSET")
        } else {
            custom_type("DATETIME2")
        }
    }

    fn requires_derived_table_alias(&self) -> bool {
        true
    }

    fn supports_qualify(&self) -> bool {
        false
    }

    /// SQL Server doesn't accept a window frame for ranking and offset functions.
    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        _start_bound: &ast::WindowFrameBound,
        _end_bound: &ast::WindowFrameBound,
    ) -> bool {
        !matches!(
            func_name.to_ascii_lowercase().as_str(),
            "row_number"
                | "rank"
                | "dense_rank"
                | "ntile"
                | "percent_rank"
                | "cume_dist"
                | "lag"
                | "lead"
        )
    }

    /// Non-ASCII strings must be national (`N'...'`) literals, or SQL Server replaces the
    /// characters that are not in the database's code page with `?`.
    fn string_literal_to_sql(&self, s: &str) -> Option<Expr> {
        if s.is_ascii() {
            return None;
        }
        // `Value::NationalStringLiteral` is displayed as-is, so the quotes are escaped here
        Some(Expr::value(Value::NationalStringLiteral(escape_quotes(s))))
    }
}

fn custom_type(name: &str) -> DataType {
    DataType::Custom(ObjectName::from(vec![Ident::new(name)]), vec![])
}

fn escape_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

/// Quotes an identifier for T-SQL, e.g. `my]table` becomes `[my]]table]`.
#[must_use]
pub fn quote_identifier(identifier: &str) -> String {
    format!("[{}]", identifier.replace(']', "]]"))
}

/// Quotes all parts of a table reference for T-SQL, e.g. `dbo.users` becomes `[dbo].[users]`.
#[must_use]
pub fn quote_table_reference(table: &datafusion::common::TableReference) -> String {
    [table.catalog(), table.schema(), Some(table.table())]
        .into_iter()
        .flatten()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

/// Rewrites a SQL statement into valid T-SQL, see [`rewrite_statement`].
///
/// SQL that sqlparser can't parse as a single statement is returned unchanged, so that T-SQL
/// sqlparser doesn't support can still be executed.
#[must_use]
pub fn rewrite_sql(sql: &str) -> String {
    let Ok(mut statements) = Parser::parse_sql(&MsSqlParserDialect {}, sql) else {
        return sql.to_string();
    };
    if statements.len() != 1 {
        return sql.to_string();
    }
    let statement = statements.remove(0);
    if !matches!(
        statement,
        Statement::Query(_) | Statement::Delete(_) | Statement::Update(_)
    ) {
        return sql.to_string();
    }
    // The parser un-escapes national string literals, but they are displayed as-is
    rewrite_statement(statement, true).to_string()
}

/// Rewrites the parts of a statement that are not valid T-SQL:
/// - `LIMIT n OFFSET m` becomes `TOP (n)` or `OFFSET m ROWS FETCH NEXT n ROWS ONLY`
/// - `NULLS FIRST` / `NULLS LAST` are dropped
/// - boolean literals become `1` / `0`, and boolean columns used as predicates become `col = 1`
/// - `a || b` becomes `CONCAT(a, b)` and `a ILIKE b` becomes `LOWER(a) LIKE LOWER(b)`
/// - `IS [NOT] DISTINCT FROM` is expanded into `NULL` checks
/// - functions whose names differ in T-SQL are renamed, e.g. `length` to `LEN`
///
/// `escape_national_strings` must be `true` when the statement was parsed from SQL text, and
/// `false` when it was produced by DataFusion's unparser with [`MsSqlDialect`], which escapes them.
#[must_use]
pub fn rewrite_statement(mut statement: Statement, escape_national_strings: bool) -> Statement {
    let mut rewriter = TSqlRewriter {
        escape_national_strings,
    };
    let _ = statement.visit(&mut rewriter);
    statement
}

struct TSqlRewriter {
    escape_national_strings: bool,
}

impl VisitorMut for TSqlRewriter {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        match statement {
            Statement::Delete(delete) => {
                if let Some(selection) = delete.selection.as_mut() {
                    fix_predicate(selection);
                }
            }
            Statement::Update(update) => {
                if let Some(selection) = update.selection.as_mut() {
                    fix_predicate(selection);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    // The predicates are fixed before their expressions are visited, because the expressions'
    // boolean literals are rewritten into numbers then.
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        fix_set_expr_predicates(&mut query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if let Some(order_by) = query.order_by.as_mut() {
            drop_nulls_ordering(order_by);
        }
        rewrite_limit(query);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Case { conditions, .. } = expr {
            for condition in conditions {
                fix_predicate(&mut condition.condition);
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        rewrite_expr(expr, self.escape_national_strings);
        ControlFlow::Continue(())
    }
}

fn fix_set_expr_predicates(set_expr: &mut SetExpr) {
    match set_expr {
        SetExpr::Select(select) => fix_select_predicates(select),
        SetExpr::SetOperation { left, right, .. } => {
            fix_set_expr_predicates(left);
            fix_set_expr_predicates(right);
        }
        // nested queries are fixed when they are visited
        _ => {}
    }
}

fn fix_select_predicates(select: &mut Select) {
    if let Some(selection) = select.selection.as_mut() {
        fix_predicate(selection);
    }
    if let Some(having) = select.having.as_mut() {
        fix_predicate(having);
    }
    for table in &mut select.from {
        fix_table_factor_predicates(&mut table.relation);
        for join in &mut table.joins {
            fix_table_factor_predicates(&mut join.relation);
            if let Some(JoinConstraint::On(on)) = join_constraint(&mut join.join_operator) {
                fix_predicate(on);
            }
        }
    }
}

fn fix_table_factor_predicates(table_factor: &mut TableFactor) {
    if let TableFactor::NestedJoin {
        table_with_joins, ..
    } = table_factor
    {
        fix_table_factor_predicates(&mut table_with_joins.relation);
        for join in &mut table_with_joins.joins {
            fix_table_factor_predicates(&mut join.relation);
            if let Some(JoinConstraint::On(on)) = join_constraint(&mut join.join_operator) {
                fix_predicate(on);
            }
        }
    }
}

fn join_constraint(join_operator: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match join_operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => Some(c),
        _ => None,
    }
}

/// T-SQL has no boolean type: `WHERE flag` must be written as `WHERE flag = 1`,
/// and `WHERE true` as `WHERE 1 = 1`.
fn fix_predicate(expr: &mut Expr) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And | BinaryOperator::Or,
            right,
        } => {
            fix_predicate(left);
            fix_predicate(right);
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        }
        | Expr::Nested(expr) => fix_predicate(expr),
        _ if is_column(expr) => {
            let column = std::mem::replace(expr, Expr::value(Value::Null));
            *expr = equals(column, 1);
        }
        _ => {
            if let Some(b) = boolean_literal(expr) {
                *expr = equals(number(1), i64::from(b));
            }
        }
    }
}

fn is_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => boolean_ident(ident).is_none(),
        Expr::CompoundIdentifier(_) => true,
        _ => false,
    }
}

/// sqlparser parses an unquoted `true`/`false` as an identifier in the T-SQL dialect.
/// As all identifiers are quoted, these are boolean literals.
fn boolean_ident(ident: &Ident) -> Option<bool> {
    if ident.quote_style.is_some() {
        return None;
    }
    match ident.value.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn boolean_literal(expr: &Expr) -> Option<bool> {
    match expr {
        Expr::Value(value) => match value.value {
            Value::Boolean(b) => Some(b),
            _ => None,
        },
        Expr::Identifier(ident) => boolean_ident(ident),
        _ => None,
    }
}

fn number(n: i64) -> Expr {
    Expr::value(Value::Number(n.to_string(), false))
}

fn equals(left: Expr, right: i64) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::Eq,
        right: Box::new(number(right)),
    }
}

fn function(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn rewrite_expr(expr: &mut Expr, escape_national_strings: bool) {
    let taken = std::mem::replace(expr, Expr::value(Value::Null));
    *expr = match taken {
        Expr::Value(mut value) => {
            match &mut value.value {
                Value::Boolean(b) => value.value = Value::Number(u8::from(*b).to_string(), false),
                Value::NationalStringLiteral(s) if escape_national_strings => {
                    *s = escape_quotes(s);
                }
                _ => {}
            }
            Expr::Value(value)
        }
        Expr::Identifier(ident) => match boolean_ident(&ident) {
            Some(b) => number(i64::from(b)),
            None => Expr::Identifier(ident),
        },
        Expr::IsTrue(inner) if is_column(&inner) => equals(*inner, 1),
        Expr::IsFalse(inner) if is_column(&inner) => equals(*inner, 0),
        Expr::IsNotTrue(inner) if is_column(&inner) => Expr::Nested(Box::new(Expr::BinaryOp {
            left: Box::new(equals((*inner).clone(), 0)),
            op: BinaryOperator::Or,
            right: Box::new(Expr::IsNull(inner)),
        })),
        Expr::IsNotFalse(inner) if is_column(&inner) => Expr::Nested(Box::new(Expr::BinaryOp {
            left: Box::new(equals((*inner).clone(), 1)),
            op: BinaryOperator::Or,
            right: Box::new(Expr::IsNull(inner)),
        })),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            right,
        } => function("CONCAT", vec![*left, *right]),
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated,
            any,
            expr: Box::new(function("LOWER", vec![*expr])),
            pattern: Box::new(function("LOWER", vec![*pattern])),
            escape_char,
        },
        Expr::IsDistinctFrom(left, right) => {
            Expr::Nested(Box::new(is_distinct_from(*left, *right)))
        }
        Expr::IsNotDistinctFrom(left, right) => Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Nested(Box::new(is_distinct_from(*left, *right)))),
        },
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => Expr::Cast {
            kind,
            expr,
            data_type: rewrite_data_type(data_type),
            array,
            format,
        },
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            // T-SQL has only SUBSTRING(expr, start, length)
            let from = substring_from.map_or_else(|| number(1), |f| *f);
            let length = substring_for.map_or_else(|| number(i64::from(i32::MAX)), |f| *f);
            Expr::Substring {
                expr,
                substring_from: Some(Box::new(from)),
                substring_for: Some(Box::new(length)),
                special: true,
                shorthand: false,
            }
        }
        Expr::Function(function) => Expr::Function(rewrite_function(function)),
        other => other,
    };
}

/// `a IS DISTINCT FROM b` = `(a <> b OR (a IS NULL AND b IS NOT NULL) OR (a IS NOT NULL AND b IS NULL))`
fn is_distinct_from(left: Expr, right: Expr) -> Expr {
    let and = |l: Expr, r: Expr| Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOperator::And,
        right: Box::new(r),
    };
    let or = |l: Expr, r: Expr| Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOperator::Or,
        right: Box::new(r),
    };
    let not_eq = Expr::BinaryOp {
        left: Box::new(left.clone()),
        op: BinaryOperator::NotEq,
        right: Box::new(right.clone()),
    };
    let left_null = and(
        Expr::IsNull(Box::new(left.clone())),
        Expr::IsNotNull(Box::new(right.clone())),
    );
    let right_null = and(
        Expr::IsNotNull(Box::new(left)),
        Expr::IsNull(Box::new(right)),
    );
    or(
        or(not_eq, Expr::Nested(Box::new(left_null))),
        Expr::Nested(Box::new(right_null)),
    )
}

fn rewrite_data_type(data_type: DataType) -> DataType {
    match data_type {
        DataType::Bool | DataType::Boolean => custom_type("BIT"),
        DataType::Text | DataType::String(_) | DataType::Varchar(None) => {
            DataType::Nvarchar(Some(CharacterLength::Max))
        }
        DataType::Double(_) | DataType::DoublePrecision | DataType::Float64 => {
            DataType::Float(ast::ExactNumberInfo::None)
        }
        DataType::Timestamp(_, ast::TimezoneInfo::None) | DataType::Datetime(_) => {
            custom_type("DATETIME2")
        }
        DataType::Timestamp(_, _) => custom_type("DATETIMEOFFSET"),
        DataType::Bytea | DataType::Blob(_) => DataType::Varbinary(Some(ast::BinaryLength::Max)),
        other => other,
    }
}

fn rewrite_function(mut function: Function) -> Function {
    if let Some(WindowType::WindowSpec(spec)) = function.over.as_mut() {
        for order_by in &mut spec.order_by {
            order_by.options.nulls_first = None;
        }
    }
    if function.name.0.len() != 1 {
        return function;
    }
    let name = function.name.to_string().to_ascii_lowercase();
    let renamed = match name.as_str() {
        "character_length" | "char_length" | "length" => Some("LEN"),
        "ceil" => Some("CEILING"),
        "ln" => Some("LOG"),
        "random" => Some("RAND"),
        "pow" => Some("POWER"),
        "btrim" => Some("TRIM"),
        "date_part" | "datepart" => Some("DATEPART"),
        _ => None,
    };
    let Some(renamed) = renamed else {
        return function;
    };
    function.name = ObjectName::from(vec![Ident::new(renamed)]);

    // DATEPART takes the date part as a keyword, e.g. DATEPART(year, d)
    if let (FunctionArguments::List(list), "DATEPART") = (&mut function.args, renamed) {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))) =
            list.args.first_mut()
        {
            if let Value::SingleQuotedString(part) = &value.value {
                let part = match part.to_ascii_lowercase().as_str() {
                    "dow" => "weekday".to_string(),
                    "doy" => "dayofyear".to_string(),
                    other => other.to_string(),
                };
                list.args[0] =
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(part))));
            }
        }
    }
    function
}

fn drop_nulls_ordering(order_by: &mut OrderBy) {
    if let OrderByKind::Expressions(exprs) = &mut order_by.kind {
        for expr in exprs {
            expr.options.nulls_first = None;
        }
    }
}

/// `ORDER BY (SELECT NULL)`, the ORDER BY required by `OFFSET ... FETCH` when the order of the
/// rows doesn't matter.
static NO_ORDER: LazyLock<Option<OrderBy>> = LazyLock::new(|| {
    let statements =
        Parser::parse_sql(&MsSqlParserDialect {}, "SELECT 1 ORDER BY (SELECT NULL)").ok()?;
    match statements.into_iter().next()? {
        Statement::Query(query) => query.order_by,
        _ => None,
    }
});

/// Rewrites `LIMIT n [OFFSET m]` into `TOP (n)` or `OFFSET m ROWS FETCH NEXT n ROWS ONLY`.
fn rewrite_limit(query: &mut Query) {
    let Some(LimitClause::LimitOffset {
        limit,
        offset,
        limit_by,
    }) = query.limit_clause.take()
    else {
        return;
    };
    if !limit_by.is_empty() {
        // not T-SQL, leave it to the server to report
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        });
        return;
    }

    // `TOP` only when the rows don't need to be sorted or skipped:
    // `TOP` and `ORDER BY` are evaluated differently in set operations
    if offset.is_none() && query.order_by.is_none() {
        if let (Some(limit), SetExpr::Select(select)) = (&limit, query.body.as_mut()) {
            if select.top.is_none() {
                select.top = Some(Top {
                    with_ties: false,
                    percent: false,
                    quantity: Some(top_quantity(limit.clone())),
                });
                select.top_before_distinct = false;
                return;
            }
        }
    }

    if limit.is_none() {
        // e.g. `OFFSET m ROWS [FETCH ...]`, which is already T-SQL
        if offset.is_some() {
            if query.order_by.is_none() {
                query.order_by = NO_ORDER.clone();
            }
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            });
        }
        return;
    }

    if query.order_by.is_none() {
        query.order_by = NO_ORDER.clone();
    }
    query.limit_clause = Some(LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: offset.map_or_else(|| number(0), |o| o.value),
            rows: OffsetRows::Rows,
        }),
        limit_by: vec![],
    });
    query.fetch = limit.map(|limit| ast::Fetch {
        with_ties: false,
        percent: false,
        quantity: Some(limit),
    });
}

fn top_quantity(limit: Expr) -> TopQuantity {
    if let Expr::Value(value) = &limit {
        if let Value::Number(n, _) = &value.value {
            if let Ok(n) = n.parse::<u64>() {
                return TopQuantity::Constant(n);
            }
        }
    }
    TopQuantity::Expr(limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::TableReference;

    #[test]
    fn limit_becomes_top() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE ([t].[a] > 1) LIMIT 10"),
            "SELECT TOP 10 [a] FROM [t] WHERE ([t].[a] > 1)"
        );
        assert_eq!(
            rewrite_sql("SELECT DISTINCT [a] FROM [t] LIMIT 3"),
            "SELECT DISTINCT TOP 3 [a] FROM [t]"
        );
    }

    #[test]
    fn limit_with_order_by_becomes_offset_fetch() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] ORDER BY [a] DESC NULLS LAST LIMIT 5"),
            "SELECT [a] FROM [t] ORDER BY [a] DESC OFFSET 0 ROWS FETCH FIRST 5 ROWS ONLY"
        );
    }

    #[test]
    fn offset_without_order_by() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] LIMIT 3 OFFSET 2"),
            "SELECT [a] FROM [t] ORDER BY (SELECT NULL) OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY"
        );
    }

    #[test]
    fn limit_of_set_operation() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t1] UNION ALL SELECT [a] FROM [t2] LIMIT 4"),
            "SELECT [a] FROM [t1] UNION ALL SELECT [a] FROM [t2] ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH FIRST 4 ROWS ONLY"
        );
    }

    #[test]
    fn limit_in_subquery() {
        assert_eq!(
            rewrite_sql("SELECT count(1) FROM (SELECT [a] FROM [t] LIMIT 7) AS [d]"),
            "SELECT count(1) FROM (SELECT TOP 7 [a] FROM [t]) AS [d]"
        );
    }

    #[test]
    fn offset_fetch_is_kept() {
        let sql = "SELECT [a] FROM [t] ORDER BY [a] ASC OFFSET 1 ROWS FETCH FIRST 5 ROWS ONLY";
        assert_eq!(rewrite_sql(sql), sql);
    }

    #[test]
    fn rewrite_is_idempotent() {
        let sql = "SELECT [a] FROM [t] WHERE [b] = N'it''s 資料' AND [flag] ORDER BY [a] ASC NULLS FIRST LIMIT 5 OFFSET 1";
        let once = rewrite_sql(sql);
        assert_eq!(rewrite_sql(&once), once);
    }

    #[test]
    fn boolean_literals_and_predicates() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE [t].[flag] = true AND NOT [t].[other]"),
            "SELECT [a] FROM [t] WHERE [t].[flag] = 1 AND NOT [t].[other] = 1"
        );
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE [flag] OR false"),
            "SELECT [a] FROM [t] WHERE [flag] = 1 OR 1 = 0"
        );
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE [flag] IS TRUE"),
            "SELECT [a] FROM [t] WHERE [flag] = 1"
        );
        assert_eq!(
            rewrite_sql("SELECT CASE WHEN [flag] THEN 1 ELSE 0 END FROM [t]"),
            "SELECT CASE WHEN [flag] = 1 THEN 1 ELSE 0 END FROM [t]"
        );
    }

    #[test]
    fn national_strings_stay_escaped() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE [b] = N'it''s 資料'"),
            "SELECT [a] FROM [t] WHERE [b] = N'it''s 資料'"
        );
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE [b] = 'it''s'"),
            "SELECT [a] FROM [t] WHERE [b] = 'it''s'"
        );
    }

    #[test]
    fn functions_and_operators() {
        assert_eq!(
            rewrite_sql("SELECT length([a]) || 'x', substr([a], 2) FROM [t] WHERE [a] ILIKE 'A%'"),
            "SELECT CONCAT(LEN([a]), 'x'), SUBSTRING([a], 2, 2147483647) FROM [t] WHERE LOWER([a]) LIKE LOWER('A%')"
        );
        assert_eq!(
            rewrite_sql("SELECT date_part('year', [d]) FROM [t]"),
            "SELECT DATEPART(year, [d]) FROM [t]"
        );
    }

    #[test]
    fn is_distinct_from() {
        assert_eq!(
            rewrite_sql("SELECT [a] FROM [t] WHERE [a] IS DISTINCT FROM [b]"),
            "SELECT [a] FROM [t] WHERE ([a] <> [b] OR ([a] IS NULL AND [b] IS NOT NULL) OR ([a] IS NOT NULL AND [b] IS NULL))"
        );
    }

    #[test]
    fn dml_predicates() {
        assert_eq!(
            rewrite_sql("DELETE FROM [t] WHERE [flag] AND [id] > 3"),
            "DELETE FROM [t] WHERE [flag] = 1 AND [id] > 3"
        );
        assert_eq!(
            rewrite_sql("UPDATE [t] SET [flag] = false WHERE [flag]"),
            "UPDATE [t] SET [flag] = 0 WHERE [flag] = 1"
        );
    }

    #[test]
    fn other_statements_are_unchanged() {
        let sql = "CREATE TABLE [t] ([flag] BIT)";
        assert_eq!(rewrite_sql(sql), sql);
        let sql = "this is not SQL";
        assert_eq!(rewrite_sql(sql), sql);
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_identifier("a]b"), "[a]]b]");
        assert_eq!(
            quote_table_reference(&TableReference::partial("dbo", "users")),
            "[dbo].[users]"
        );
    }

    #[test]
    fn unparsed_plan_is_t_sql() {
        use datafusion::arrow::datatypes::{DataType as ArrowType, Field, Schema};
        use datafusion::logical_expr::{col, lit, LogicalPlanBuilder, LogicalTableSource};
        use datafusion::sql::unparser::Unparser;

        let schema = Schema::new(vec![
            Field::new("id", ArrowType::Int32, false),
            Field::new("name", ArrowType::Utf8, true),
            Field::new("flag", ArrowType::Boolean, true),
        ]);
        let plan = LogicalPlanBuilder::scan_with_filters(
            "users",
            Arc::new(LogicalTableSource::new(Arc::new(schema))),
            None,
            vec![
                col("flag"),
                col("name").eq(lit("名前")),
                col("id").gt(lit(1)),
            ],
        )
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

        let statement = Unparser::new(&MsSqlDialect {}).plan_to_sql(&plan).unwrap();
        let sql = rewrite_statement(statement, false).to_string();
        assert_eq!(
            sql,
            "SELECT * FROM [users] WHERE (([users].[flag] = 1 AND ([users].[name] = N'名前')) AND ([users].[id] > 1)) ORDER BY [users].[id] ASC OFFSET 0 ROWS FETCH FIRST 10 ROWS ONLY"
        );
        // and it survives the rewrite of the executed SQL text
        assert_eq!(rewrite_sql(&sql), sql);
    }
}
