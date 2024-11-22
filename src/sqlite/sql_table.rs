use crate::sql::db_connection_pool::DbConnectionPool;
use crate::sql::sql_provider_datafusion::Engine;
use async_trait::async_trait;
use datafusion::catalog::Session;
use futures::TryStreamExt;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use crate::sql::sql_provider_datafusion::{
    get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
    sql::TableReference,
};

pub struct SQLiteTable<T: 'static, P: 'static> {
    pub(crate) base_table: SqlTable<T, P>,
}

impl<T, P> fmt::Debug for SQLiteTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQLiteTable {}", self.base_table.name())
    }
}

impl<T, P> SQLiteTable<T, P> {
    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
    ) -> Self {
        let base_table = SqlTable::new_with_schema(
            "sqlite",
            pool,
            schema,
            table_reference,
            Some(Engine::SQLite),
        );

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
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(projection, schema, pool, sql)?;

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
