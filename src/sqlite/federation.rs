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
    seconds: f64,
}

impl VisitorMut for SQLiteVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // for each INTERVAL, find the previous (or next, if the INTERVAL is first) expression or column name that is associated with it
        // e.g. `column_name + INTERVAL '1' DAY``, we should find the `column_name`
        // then replace the `INTERVAL` with e.g. `datetime(column_name, '+1 day')`
        // this should also apply to expressions though, like `CAST(column_name AS TEXT) + INTERVAL '1' DAY`
        // in this example, it would be replaced with `datetime(CAST(column_name AS TEXT), '+1 day')`

        if let Expr::BinaryOp {
            op: BinaryOperator::Plus,
            left,
            right,
        } = expr
        {
            // normalize the sides of the operation to make sure the INTERVAL is always on the right
            let (target, interval) = if let Expr::Interval { .. } = left.as_ref() {
                (right, left)
            } else {
                (left, right)
            };

            // if it's not an INTERVAL, skip
            if let Expr::Interval(_) = interval.as_ref() {
            } else {
                return ControlFlow::Continue(());
            }

            println!("left: {target:?}");
            println!("right: {interval:?}");

            // TODO: figure out nested BinaryOp, e.g. `column_name + INTERVAL '1' DAY + INTERVAL '1' DAY`
            let mut interval_parts = IntervalParts::default();

            // parse the INTERVAL and get the bits out of it
            // e.g. INTERVAL 0 YEARS 0 MONS 1 DAYS 0 HOURS 0 MINUTES 0.000000000 SECS -> IntervalParts { days: 1 }
            if let Expr::Interval(interval_expr) = interval.as_ref() {
                if let Expr::Value(ast::Value::SingleQuotedString(value)) =
                    interval_expr.value.as_ref()
                {
                    // POC: replace this with a proper interval parser
                    let mut split = value.split_once("YEARS").unwrap_or(("0", value));
                    interval_parts.years = split.0.trim().parse().unwrap_or(0);

                    split = split.1.split_once("MONS").unwrap_or(("0", split.1));
                    interval_parts.months = split.0.trim().parse().unwrap_or(0);

                    split = split.1.split_once("DAYS").unwrap_or(("0", split.1));
                    interval_parts.days = split.0.trim().parse().unwrap_or(0);

                    split = split.1.split_once("HOURS").unwrap_or(("0", split.1));
                    interval_parts.hours = split.0.trim().parse().unwrap_or(0);

                    split = split.1.split_once("MINS").unwrap_or(("0", split.1));
                    interval_parts.minutes = split.0.trim().parse().unwrap_or(0);

                    split = split.1.split_once("SECS").unwrap_or(("0", split.1));
                    interval_parts.seconds = split.0.trim().parse().unwrap_or(0.0);

                    println!("interval_parts: {interval_parts:?}");
                }
            }

            let function_args = vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(*target.clone())),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString(format!("+{} years", interval_parts.years)),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString(format!("+{} months", interval_parts.months)),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString(format!("+{} days", interval_parts.days)),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString(format!("+{} hours", interval_parts.hours)),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString(format!("+{} minutes", interval_parts.minutes)),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString(format!("+{} seconds", interval_parts.seconds)),
                ))),
            ];

            let datetime_function = Expr::Function(ast::Function {
                name: ast::ObjectName(vec![Ident {
                    value: "datetime".to_string(),
                    quote_style: None,
                }]),
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

            let output_cast = Expr::Cast {
                expr: Box::new(datetime_function),
                data_type: ast::DataType::Text,
                format: None,
                kind: ast::CastKind::Cast,
            };

            println!("output_cast: {output_cast:?}");

            *expr = output_cast;
        }

        ControlFlow::Continue(())
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
