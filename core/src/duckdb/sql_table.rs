use crate::sql::db_connection_pool::DbConnectionPool;
use crate::sql::sql_provider_datafusion::expr::Engine;
use crate::sql::sql_provider_datafusion::{
    get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
};
use crate::util::column_reference::ColumnReference;
use crate::util::indexes::IndexType;
use crate::util::supported_functions::FunctionSupport;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::sql::unparser::dialect::Dialect;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
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
    sql::{unparser::dialect::DuckDBDialect, TableReference},
};
use datafusion_physical_expr::EquivalenceProperties;

pub struct DuckDBTable<T: 'static, P: 'static> {
    pub(crate) base_table: SqlTable<T, P>,

    /// A mapping of table/view names to `DuckDB` functions that can instantiate a table (e.g. "`read_parquet`('`my_file.parquet`')").
    pub(crate) table_functions: Option<HashMap<String, String>>,

    pub(crate) function_support: Option<FunctionSupport>,

    /// A list of indexes as expressed by columns reference in their index expressions.
    pub(crate) indexes: Vec<(ColumnReference, IndexType)>,
}

impl<T, P> std::fmt::Debug for DuckDBTable<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl<T, P> DuckDBTable<T, P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
        table_functions: Option<HashMap<String, String>>,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
        constraints: Option<Constraints>,
        function_support: Option<FunctionSupport>,
        indexes: Vec<(ColumnReference, IndexType)>,
    ) -> Self {
        let base_table = SqlTable::new_with_schema(
            "duckdb",
            pool,
            schema,
            table_reference,
            Some(Engine::DuckDB),
        )
        .with_dialect(dialect.unwrap_or(Arc::new(DuckDBDialect::new())))
        .with_constraints_opt(constraints)
        .with_function_support(function_support.clone());

        Self {
            base_table,
            table_functions,
            function_support,
            indexes,
        }
    }

    #[must_use]
    pub fn with_indexes(mut self, indexes: Vec<(ColumnReference, IndexType)>) -> Self {
        self.indexes = indexes;
        self
    }

    fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        sql: String,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DuckSqlExec::new(
            projection,
            schema,
            self.base_table.clone_pool(),
            sql,
            self.table_functions.clone(),
            self.base_table.dialect_arc(),
        )?))
    }
}

#[async_trait]
impl<T, P> TableProvider for DuckDBTable<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        self.base_table.supports_filters_pushdown(filters)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sql = self.base_table.scan_to_sql(projection, filters, limit)?;
        return self.create_physical_plan(projection, &self.schema(), sql);
    }
}

impl<T, P> Display for DuckDBTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckDBTable {}", self.base_table.name())
    }
}

pub struct DuckSqlExec<T, P> {
    base_exec: SqlExec<T, P>,
    table_functions: Option<HashMap<String, String>>,
    indexes: Vec<(ColumnReference, IndexType)>,
    optimized_sql: Option<String>,
    optimized_sql_schema: Option<SchemaRef>,
    optimized_sql_properties: Option<PlanProperties>,
}

impl<T, P> Clone for DuckSqlExec<T, P> {
    fn clone(&self) -> Self {
        DuckSqlExec {
            base_exec: self.base_exec.clone(),
            table_functions: self.table_functions.clone(),
            indexes: self.indexes.clone(),
            optimized_sql: self.optimized_sql.clone(),
            optimized_sql_schema: self.optimized_sql_schema.clone(),
            optimized_sql_properties: self.optimized_sql_properties.clone(),
        }
    }
}

impl<T: 'static, P: 'static> DuckSqlExec<T, P> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        sql: String,
        table_functions: Option<HashMap<String, String>>,
        dialect: Arc<dyn Dialect + Send + Sync>,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(projection, schema, pool, sql, dialect)?;

        Ok(Self {
            base_exec,
            table_functions,
            indexes: Vec::new(),
            optimized_sql: None,
            optimized_sql_schema: None,
            optimized_sql_properties: None,
        })
    }
}

impl<T: 'static, P: 'static> DuckSqlExec<T, P> {
    /// The SQL expression for this execution node. This may differ from `DuckSqlExec::base_sql`
    /// if rewritten by an optimization step.
    pub fn sql(&self) -> SqlResult<String> {
        if let Some(sql) = &self.optimized_sql {
            return Ok(sql.clone());
        }

        let sql = self.base_exec.sql()?;

        Ok(format!(
            "{cte_expr} {sql}",
            cte_expr = get_cte(&self.table_functions)
        ))
    }

    /// Indexes that may be bound for the SQL expression in this execution node
    #[must_use]
    pub fn indexes(&self) -> &[(ColumnReference, IndexType)] {
        &self.indexes
    }

    /// The unoptimized SQL expression for this execution node
    pub fn base_sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }

    /// Use this method to bind an optimized SQL expression from a `PhysicalOptimizerRule`. Provide
    /// a `new_schema` if changing the output schema of this node.
    #[must_use]
    pub fn with_optimized_sql(
        mut self,
        sql: impl Into<String>,
        new_schema: Option<SchemaRef>,
    ) -> Self {
        self.optimized_sql = Some(sql.into());
        self.optimized_sql_schema = new_schema;

        if let Some(schema) = self.optimized_sql_schema.as_ref() {
            let mut properties = self.base_exec.properties().clone();
            let eq_properties = EquivalenceProperties::new(Arc::clone(schema));
            properties.eq_properties = eq_properties;
            self.optimized_sql_properties = Some(properties);
        }

        self
    }
}

impl<T: 'static, P: 'static> std::fmt::Debug for DuckSqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "DuckSqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> DisplayAs for DuckSqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "DuckSqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for DuckSqlExec<T, P> {
    fn name(&self) -> &'static str {
        "DuckSqlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.optimized_sql_schema
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.base_exec.schema())
    }

    fn properties(&self) -> &PlanProperties {
        self.optimized_sql_properties
            .as_ref()
            .unwrap_or_else(|| self.base_exec.properties())
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
        match self.base_exec.try_pushdown_sort(order)? {
            SortOrderPushdownResult::Exact { inner } => {
                let base_exec = inner
                    .as_any()
                    .downcast_ref::<SqlExec<T, P>>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast SqlExec in sort pushdown".to_string(),
                        )
                    })?
                    .clone();
                Ok(SortOrderPushdownResult::Exact {
                    inner: Arc::new(DuckSqlExec {
                        base_exec,
                        table_functions: self.table_functions.clone(),
                        indexes: self.indexes.clone(),
                        optimized_sql: None,
                        optimized_sql_schema: None,
                        optimized_sql_properties: None,
                    }),
                })
            }
            SortOrderPushdownResult::Inexact { inner } => {
                let base_exec = inner
                    .as_any()
                    .downcast_ref::<SqlExec<T, P>>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast SqlExec in sort pushdown".to_string(),
                        )
                    })?
                    .clone();
                Ok(SortOrderPushdownResult::Inexact {
                    inner: Arc::new(DuckSqlExec {
                        base_exec,
                        table_functions: self.table_functions.clone(),
                        indexes: self.indexes.clone(),
                        optimized_sql: None,
                        optimized_sql_schema: None,
                        optimized_sql_properties: None,
                    }),
                })
            }
            SortOrderPushdownResult::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
        }
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fetch(&self) -> Option<usize> {
        self.base_exec.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let base_exec = self
            .base_exec
            .with_fetch(limit)?
            .as_any()
            .downcast_ref::<SqlExec<T, P>>()?
            .clone();
        Some(Arc::new(DuckSqlExec {
            base_exec,
            table_functions: self.table_functions.clone(),
            indexes: self.indexes.clone(),
            optimized_sql: None,
            optimized_sql_schema: None,
            optimized_sql_properties: None,
        }))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let result =
            self.base_exec
                .handle_child_pushdown_result(phase, child_pushdown_result, config)?;
        let updated_node = result
            .updated_node
            .map(|node| {
                let base_exec = node
                    .as_any()
                    .downcast_ref::<SqlExec<T, P>>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast SqlExec in filter pushdown".to_string(),
                        )
                    })?
                    .clone();
                Ok::<_, DataFusionError>(Arc::new(DuckSqlExec {
                    base_exec,
                    table_functions: self.table_functions.clone(),
                    indexes: self.indexes.clone(),
                    optimized_sql: None,
                    optimized_sql_schema: None,
                    optimized_sql_properties: None,
                }) as Arc<dyn ExecutionPlan>)
            })
            .transpose()?;
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node,
        })
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("DuckSqlExec sql: {sql}");

        let schema = self.schema();

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&schema));

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Create CTE expressions for all the table functions.
pub(crate) fn get_cte(table_functions: &Option<HashMap<String, String>>) -> String {
    if let Some(table_fn) = table_functions {
        let inner = table_fn
            .iter()
            .map(|(name, r#fn)| format!("{name} AS (SELECT * FROM {fn})"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("WITH {inner} ")
    } else {
        String::new()
    }
}
