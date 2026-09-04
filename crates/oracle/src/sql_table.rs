use crate::pool::OracleConnectionPool;
use async_trait::async_trait;
use datafusion::catalog::Session;
use futures::TryStreamExt;
use std::fmt::Display;
use std::{fmt, sync::Arc};

use crate::conn::OraclePooledConnection;
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::utils::quote_identifier,
    config::ConfigOptions,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        filter_pushdown::{ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation},
        sort_pushdown::SortOrderPushdownResult,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
    sql::{
        sqlparser,
        unparser::{
            dialect::{CustomDialect, CustomDialectBuilder, Dialect},
            Unparser,
        },
        TableReference,
    },
};
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion::{
    self, get_stream, project_schema_safe, to_execution_error, Result as SqlResult, SqlExec,
    SqlTable,
};

type BaseSqlExec = SqlExec<OraclePooledConnection, oracle::sql_type::OracleType>;

pub struct OracleTable {
    pub(crate) pool: Arc<OracleConnectionPool>,
    pub(crate) base_table: SqlTable<OraclePooledConnection, oracle::sql_type::OracleType>,
}

impl std::fmt::Debug for OracleTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl OracleTable {
    pub async fn new(
        pool: &Arc<OracleConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let dyn_pool = Arc::clone(pool)
            as Arc<
                dyn DbConnectionPool<OraclePooledConnection, oracle::sql_type::OracleType>
                    + Send
                    + Sync,
            >;
        let base_table = SqlTable::new("oracle", &dyn_pool, table_reference)
            .await?
            .with_dialect(Arc::new(OracleTable::dialect()));

        Ok(Self {
            pool: Arc::clone(pool),
            base_table,
        })
    }

    pub(crate) fn dialect() -> CustomDialect {
        CustomDialectBuilder::new()
            .with_identifier_quote_style('"')
            // There is no 'DOUBLE' SQL type in Oracle: it can use 'FLOAT' for both single and double precision float values
            .with_float64_ast_dtype(sqlparser::ast::DataType::Float(
                sqlparser::ast::ExactNumberInfo::None,
            ))
            .build()
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema_safe(schema, projections)?;

        let columns = if projections.is_some_and(|p| p.is_empty()) {
            // The DataFusion unparser renders an empty projection as `SELECT 1`.
            "1".to_string()
        } else {
            projected_schema
                .fields()
                .iter()
                .map(|f| quote_identifier(f.name()))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let dialect = OracleTable::dialect();

        let where_expr = if filters.is_empty() {
            String::new()
        } else {
            let filter_expr = filters
                .iter()
                .map(|f| {
                    Unparser::new(&dialect)
                        .expr_to_sql(f)
                        .map(|e| format!("({e})"))
                })
                .collect::<DataFusionResult<Vec<String>>>()?
                .join(" AND ");
            format!("WHERE {filter_expr}")
        };

        let limit_expr = match limit {
            Some(limit) => format!("FETCH FIRST {limit} ROWS ONLY"),
            None => String::new(),
        };

        let table_reference = self.base_table.table_reference.to_quoted_string();
        let mut sql = format!("SELECT {columns} FROM {table_reference}");
        if !where_expr.is_empty() {
            sql.push_str(&format!(" {where_expr}"));
        }
        if !limit_expr.is_empty() {
            sql.push_str(&format!(" {limit_expr}"));
        }

        Ok(Arc::new(OracleSQLExec::from_base(BaseSqlExec::new(
            projections,
            schema,
            Arc::clone(&self.pool)
                as Arc<
                    dyn DbConnectionPool<OraclePooledConnection, oracle::sql_type::OracleType>
                        + Send
                        + Sync,
                >,
            sql,
            Arc::new(dialect),
        )?)))
    }

    /// Check if an expression contains datetime-related types that Oracle cannot handle
    /// in filter pushdown due to datetime literal format requirements.
    fn contains_datetime_expr(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                Self::is_datetime_type_expr(&binary_expr.left)
                    || Self::is_datetime_type_expr(&binary_expr.right)
                    || Self::contains_datetime_expr(&binary_expr.left)
                    || Self::contains_datetime_expr(&binary_expr.right)
            }
            Expr::Not(inner) => Self::contains_datetime_expr(inner),
            _ => Self::is_datetime_type_expr(expr),
        }
    }

    fn is_datetime_type_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Cast(cast) => matches!(
                cast.field.data_type(),
                DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
            ),
            Expr::Literal(literal, _) => matches!(
                literal.data_type(),
                DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
            ),
            _ => false,
        }
    }
}

#[async_trait]
impl TableProvider for OracleTable {
    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Oracle requires specific format for datetime literals that the expression
        // unparser cannot handle correctly, resulting in ORA-01843 errors.
        // We mark datetime-related filters as unsupported to prevent pushdown.
        let mut results = Vec::with_capacity(filters.len());
        for filter in filters {
            if Self::contains_datetime_expr(filter) {
                results.push(TableProviderFilterPushDown::Unsupported);
            } else {
                // For non-datetime filters, delegate to base table
                let base_result = self.base_table.supports_filters_pushdown(&[filter])?;
                results.extend(base_result);
            }
        }
        Ok(results)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

impl Display for OracleTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OracleTable {}", self.base_table.name())
    }
}

/// Remove a trailing `FETCH FIRST n ROWS ONLY` (any case) from the SQL and return
/// the remaining SQL plus the limit, if present.
fn split_trailing_fetch(sql: &str) -> (String, Option<usize>) {
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.rfind("FETCH FIRST ") {
        let tail = sql[pos..].trim();
        if tail.ends_with(" ROWS ONLY") {
            let num_part = tail["FETCH FIRST ".len()..tail.len() - " ROWS ONLY".len()].trim();
            if let Ok(n) = num_part.parse::<usize>() {
                return (sql[..pos].trim_end().to_string(), Some(n));
            }
        }
    }
    (sql.to_string(), None)
}

pub(crate) struct OracleSQLExec {
    base_exec: BaseSqlExec,
    dialect: Arc<dyn Dialect + Send + Sync>,
}

impl OracleSQLExec {
    fn from_base(base_exec: BaseSqlExec) -> Self {
        Self {
            base_exec,
            dialect: Arc::new(OracleTable::dialect()),
        }
    }

    fn sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }

    fn downcast_base(node: Arc<dyn ExecutionPlan>) -> DataFusionResult<BaseSqlExec> {
        node.downcast_ref::<BaseSqlExec>().cloned().ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast SqlExec in OracleSQLExec".to_string())
        })
    }

    /// Rebuild a node around `base_exec`, replacing its SQL.
    fn rebuild_with_sql(&self, base_exec: &BaseSqlExec, sql: String) -> DataFusionResult<Self> {
        Ok(Self::from_base(BaseSqlExec::new(
            None,
            &base_exec.schema(),
            base_exec.clone_pool(),
            sql,
            Arc::clone(&self.dialect),
        )?))
    }
}

impl std::fmt::Debug for OracleSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "OracleSQLExec sql={sql}")
    }
}

impl DisplayAs for OracleSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "OracleSQLExec sql={sql}")
    }
}

impl ExecutionPlan for OracleSQLExec {
    fn name(&self) -> &'static str {
        "OracleSQLExec"
    }

    fn schema(&self) -> SchemaRef {
        self.base_exec.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.base_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.base_exec.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DataFusionResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // Oracle has no `LIMIT`; a trailing `FETCH FIRST n ROWS ONLY` would end up
        // *after* the appended ORDER BY clause, so strip it before delegating and
        // re-append it once the ORDER BY has been inserted.
        let sql = self.sql().map_err(to_execution_error)?;
        let (stripped, limit) = split_trailing_fetch(&sql);

        if limit.is_none() {
            return match self.base_exec.try_pushdown_sort(order)? {
                SortOrderPushdownResult::Exact { inner } => Ok(SortOrderPushdownResult::Exact {
                    inner: Arc::new(OracleSQLExec::from_base(Self::downcast_base(inner)?)),
                }),
                SortOrderPushdownResult::Inexact { inner } => {
                    Ok(SortOrderPushdownResult::Inexact {
                        inner: Arc::new(OracleSQLExec::from_base(Self::downcast_base(inner)?)),
                    })
                }
                SortOrderPushdownResult::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
            };
        }

        let limit = limit.expect("checked above");

        // Rebuild the base node without the FETCH tail, push the sort down into it,
        // then re-apply the limit via Oracle syntax.
        let base_no_fetch = BaseSqlExec::new(
            None,
            &self.base_exec.schema(),
            self.base_exec.clone_pool(),
            stripped,
            Arc::clone(&self.dialect),
        )?;

        match base_no_fetch.try_pushdown_sort(order)? {
            SortOrderPushdownResult::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
            result => {
                let inner = match result {
                    SortOrderPushdownResult::Exact { inner }
                    | SortOrderPushdownResult::Inexact { inner } => inner,
                    SortOrderPushdownResult::Unsupported => unreachable!(),
                };
                let sorted = Self::downcast_base(inner)?;
                let sorted_sql = sorted.sql().map_err(to_execution_error)?;
                let new_exec = self.rebuild_with_sql(
                    &sorted,
                    format!("{sorted_sql} FETCH FIRST {limit} ROWS ONLY"),
                )?;
                Ok(SortOrderPushdownResult::Inexact {
                    inner: Arc::new(new_exec),
                })
            }
        }
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fetch(&self) -> Option<usize> {
        split_trailing_fetch(&self.sql().ok()?).1
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let limit = limit?;
        let sql = self.sql().ok()?;
        let (stripped, _) = split_trailing_fetch(&sql);
        let new_sql = format!("{stripped} FETCH FIRST {limit} ROWS ONLY");
        let new_exec = BaseSqlExec::new(
            None,
            &self.base_exec.schema(),
            self.base_exec.clone_pool(),
            new_sql,
            Arc::clone(&self.dialect),
        )
        .ok()?;
        Some(Arc::new(OracleSQLExec::from_base(new_exec)))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // Same reason as `try_pushdown_sort`: the WHERE clause insertion in the base
        // node looks for ` LIMIT `/` ORDER BY ` boundaries and would land *after*
        // `FETCH FIRST n ROWS ONLY`. Strip the tail, delegate, then re-append it.
        let sql = self.sql().map_err(to_execution_error)?;
        let (stripped, limit) = split_trailing_fetch(&sql);

        let result = if let Some(limit) = limit {
            let base_stripped = BaseSqlExec::new(
                None,
                &self.base_exec.schema(),
                self.base_exec.clone_pool(),
                stripped,
                Arc::clone(&self.dialect),
            )?;
            let mut result =
                base_stripped.handle_child_pushdown_result(phase, child_pushdown_result, config)?;
            if let Some(node) = result.updated_node.take() {
                let updated = Self::downcast_base(node)?;
                let updated_sql = updated.sql().map_err(to_execution_error)?;
                let new_exec = self.rebuild_with_sql(
                    &updated,
                    format!("{updated_sql} FETCH FIRST {limit} ROWS ONLY"),
                )?;
                result.updated_node = Some(Arc::new(new_exec) as Arc<dyn ExecutionPlan>);
            }
            result
        } else {
            self.base_exec
                .handle_child_pushdown_result(phase, child_pushdown_result, config)?
        };

        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node: result.updated_node,
        })
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("OracleSQLExec sql: {sql}");

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&self.schema()));

        let stream = futures::stream::once(fut).try_flatten();
        let schema = Arc::clone(&self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
