use crate::sql::db_connection_pool::DbConnectionPool;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::sql::unparser::dialect::{Dialect, SqliteDialect};
use futures::TryStreamExt;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use crate::sql::sql_provider_datafusion::{
    get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
};
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
    sql::TableReference,
};

pub struct SQLiteTable<T: 'static, P: 'static> {
    pub(crate) base_table: SqlTable<T, P>,
}

impl<T, P> std::fmt::Debug for SQLiteTable<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SQLiteTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl<T, P> SQLiteTable<T, P> {
    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
    ) -> Self {
        let base_table = SqlTable::new_with_schema("sqlite", pool, schema, table_reference)
            .with_dialect(Arc::new(SqliteDialect {}));

        Self { base_table }
    }

    fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        sql: String,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SQLiteSqlExec::new(
            projection,
            schema,
            self.base_table.clone_pool(),
            sql,
            self.base_table.dialect_arc(),
        )?))
    }
}

#[async_trait]
impl<T, P> TableProvider for SQLiteTable<T, P> {
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

impl<T, P> Display for SQLiteTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQLiteTable {}", self.base_table.name())
    }
}

#[derive(Clone)]
struct SQLiteSqlExec<T, P> {
    base_exec: SqlExec<T, P>,
}

impl<T, P> SQLiteSqlExec<T, P> {
    fn new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        sql: String,
        dialect: Arc<dyn Dialect + Send + Sync>,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(projection, schema, pool, sql, dialect)?;

        Ok(Self { base_exec })
    }

    fn sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }
}

impl<T, P> std::fmt::Debug for SQLiteSqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SQLiteSqlExec sql={sql}")
    }
}

impl<T, P> DisplayAs for SQLiteSqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SQLiteSqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for SQLiteSqlExec<T, P> {
    fn name(&self) -> &'static str {
        "SQLiteSqlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
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
                    inner: Arc::new(SQLiteSqlExec { base_exec }),
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
                    inner: Arc::new(SQLiteSqlExec { base_exec }),
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
        Some(Arc::new(SQLiteSqlExec { base_exec }))
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
                Ok::<_, DataFusionError>(
                    Arc::new(SQLiteSqlExec { base_exec }) as Arc<dyn ExecutionPlan>
                )
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
        tracing::debug!("SQLiteSqlExec sql: {sql}");

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&self.schema()));

        let stream = futures::stream::once(fut).try_flatten();
        let schema = Arc::clone(&self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
