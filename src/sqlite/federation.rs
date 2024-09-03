use crate::sql::db_connection_pool::dbconnection::{get_schema, Error as DbError};
use crate::sql::sql_provider_datafusion::{get_stream, to_execution_error};
use arrow::datatypes::SchemaRef;
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, Ident,
    VisitMut, VisitorMut,
};
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use datafusion_federation_sql::{AstAnalyzer, SQLExecutor, SQLFederationProvider, SQLTableSource};
use futures::TryStreamExt;
use snafu::ResultExt;

use async_trait::async_trait;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;

use super::sql_table::SQLiteTable;
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

impl<T, P> SQLiteTable<T, P> {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = self.base_table.table_reference.to_quoted_string();
        let schema = Arc::clone(&Arc::clone(&self).base_table.schema());
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        )?))
    }

    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[derive(Default)]
struct SQLiteVisitor {}

#[derive(Default, Debug)]
struct IntervalParts {
    years: u64,
    months: u64,
    days: u64,
    hours: u64,
    minutes: u64,
    seconds: u64,
    nanos: u32,
}

type IntervalSetter = fn(IntervalParts, u64) -> IntervalParts;

impl IntervalParts {
    fn new() -> Self {
        Self::default()
    }

    fn with_years(mut self, years: u64) -> Self {
        self.years = years;
        self
    }

    fn with_months(mut self, months: u64) -> Self {
        self.months = months;
        self
    }

    fn with_days(mut self, days: u64) -> Self {
        self.days = days;
        self
    }

    fn with_hours(mut self, hours: u64) -> Self {
        self.hours = hours;
        self
    }

    fn with_minutes(mut self, minutes: u64) -> Self {
        self.minutes = minutes;
        self
    }

    fn with_seconds(mut self, seconds: u64) -> Self {
        self.seconds = seconds;
        self
    }

    fn with_nanos(mut self, nanos: u32) -> Self {
        self.nanos = nanos;
        self
    }
}

impl VisitorMut for SQLiteVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // for each INTERVAL, find the previous (or next, if the INTERVAL is first) expression or column name that is associated with it
        // e.g. `column_name + INTERVAL '1' DAY``, we should find the `column_name`
        // then replace the `INTERVAL` with e.g. `datetime(column_name, '+1 day')`
        // this should also apply to expressions though, like `CAST(column_name AS TEXT) + INTERVAL '1' DAY`
        // in this example, it would be replaced with `datetime(CAST(column_name AS TEXT), '+1 day')`

        // TODO: figure out nested BinaryOp, e.g. `column_name + INTERVAL '1' DAY + INTERVAL '1' DAY`
        if let Expr::BinaryOp {
            op: BinaryOperator::Plus,
            left,
            right,
        } = expr
        {
            let (target, interval) = self.normalize_interval_expr(left, right);

            if let Expr::Interval(_) = interval.as_ref() {
                // parse the INTERVAL and get the bits out of it
                // e.g. INTERVAL 0 YEARS 0 MONS 1 DAYS 0 HOURS 0 MINUTES 0.000000000 SECS -> IntervalParts { days: 1 }
                if let Ok(interval_parts) = self.parse_interval(interval) {
                    *expr = self.create_datetime_function(target, &interval_parts);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl SQLiteVisitor {
    // normalize the sides of the operation to make sure the INTERVAL is always on the right
    fn normalize_interval_expr<'a>(
        &self,
        left: &'a mut Box<Expr>,
        right: &'a mut Box<Expr>,
    ) -> (&'a mut Box<Expr>, &'a mut Box<Expr>) {
        if let Expr::Interval { .. } = left.as_ref() {
            (right, left)
        } else {
            (left, right)
        }
    }

    fn parse_interval(&self, interval: &Expr) -> Result<IntervalParts, DataFusionError> {
        if let Expr::Interval(interval_expr) = interval {
            if let Expr::Value(ast::Value::SingleQuotedString(value)) = interval_expr.value.as_ref()
            {
                return self.parse_interval_string(value);
            }
        }
        Err(DataFusionError::Plan(
            "Invalid interval expression".to_string(),
        ))
    }

    fn parse_interval_string(&self, value: &str) -> Result<IntervalParts, DataFusionError> {
        let mut parts = IntervalParts::new();
        let mut remaining = value;

        let components: [(_, IntervalSetter); 5] = [
            ("YEARS", IntervalParts::with_years),
            ("MONS", IntervalParts::with_months),
            ("DAYS", IntervalParts::with_days),
            ("HOURS", IntervalParts::with_hours),
            ("MINS", IntervalParts::with_minutes),
        ];

        for (unit, setter) in components.iter() {
            if let Some((value, rest)) = remaining.split_once(unit) {
                let parsed_value: u64 = self.parse_value(value.trim())?;
                parts = setter(parts, parsed_value);
                remaining = rest;
            }
        }

        // Parse seconds and nanoseconds separately
        if let Some((secs, _)) = remaining.split_once("SECS") {
            let (seconds, nanos) = self.parse_seconds_and_nanos(secs.trim())?;
            parts = parts.with_seconds(seconds).with_nanos(nanos);
        }

        Ok(parts)
    }

    fn parse_seconds_and_nanos(&self, value: &str) -> Result<(u64, u32), DataFusionError> {
        let parts: Vec<&str> = value.split('.').collect();
        let seconds = self.parse_value(parts[0])?;
        let nanos = if parts.len() > 1 {
            let nanos_str = format!("{:0<9}", parts[1]);
            nanos_str[..9].parse().map_err(|_| {
                DataFusionError::Plan(format!("Failed to parse nanoseconds: {}", parts[1]))
            })?
        } else {
            0
        };
        Ok((seconds, nanos))
    }

    fn parse_value<T: FromStr>(&self, value: &str) -> Result<T, DataFusionError> {
        value.parse().map_err(|_| {
            DataFusionError::Plan(format!("Failed to parse interval value: {}", value))
        })
    }

    fn create_datetime_function(&self, target: &Expr, interval: &IntervalParts) -> Expr {
        let function_args = vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(target.clone())),
            self.create_interval_arg("years", interval.years as i64),
            self.create_interval_arg("months", interval.months as i64),
            self.create_interval_arg("days", interval.days as i64),
            self.create_interval_arg("hours", interval.hours as i64),
            self.create_interval_arg("minutes", interval.minutes as i64),
            self.create_interval_arg_with_fraction(
                "seconds",
                interval.seconds as i64,
                interval.nanos,
            ),
        ];

        let datetime_function = Expr::Function(ast::Function {
            name: ast::ObjectName(vec![Ident::new("datetime")]),
            args: ast::FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: function_args,
                clauses: Vec::new(),
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: Vec::new(),
            parameters: ast::FunctionArguments::None,
        });

        Expr::Cast {
            expr: Box::new(datetime_function),
            data_type: ast::DataType::Text,
            format: None,
            kind: ast::CastKind::Cast,
        }
    }

    fn create_interval_arg(&self, unit: &str, value: i64) -> FunctionArg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            ast::Value::SingleQuotedString(format!("{:+} {}", value, unit)),
        )))
    }

    fn create_interval_arg_with_fraction(
        &self,
        unit: &str,
        value: i64,
        fraction: u32,
    ) -> FunctionArg {
        let fraction_str = if fraction > 0 {
            format!(".{:09}", fraction)
        } else {
            String::new()
        };
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            ast::Value::SingleQuotedString(format!("{:+}{}{}", value, fraction_str, unit)),
        )))
    }
}

#[allow(clippy::unnecessary_wraps)]
fn sqlite_ast_analyzer(ast: ast::Statement) -> Result<ast::Statement, DataFusionError> {
    match ast {
        ast::Statement::Query(query) => {
            let mut new_query = query.clone();

            // iterate over the query and find any INTERVAL statements
            // find the column they target, and replace the INTERVAL and column with e.g. datetime(column, '+1 day')
            let mut visitor = SQLiteVisitor::default();
            new_query.visit(&mut visitor);

            Ok(ast::Statement::Query(new_query))
        }
        _ => Ok(ast),
    }
}

#[async_trait]
impl<T, P> SQLExecutor for SQLiteTable<T, P> {
    fn name(&self) -> &str {
        self.base_table.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.base_table.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.base_table.dialect()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        Some(Box::new(sqlite_ast_analyzer))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fut = get_stream(
            self.base_table.clone_pool(),
            query.to_string(),
            Arc::clone(&schema),
        );

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        let conn = self
            .base_table
            .clone_pool()
            .connect()
            .await
            .map_err(to_execution_error)?;
        get_schema(conn, &TableReference::from(table_name))
            .await
            .boxed()
            .map_err(|e| DbError::UnableToGetSchema { source: e })
            .map_err(to_execution_error)
    }
}
