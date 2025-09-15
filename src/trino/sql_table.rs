use crate::sql::db_connection_pool::trinodbpool::TrinoConnectionPool;
use crate::sql::db_connection_pool::DbConnectionPool;
use crate::sql::sql_provider_datafusion::expr::Engine;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::sql::unparser::dialect::DefaultDialect;
use futures::TryStreamExt;
use reqwest::Client;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use crate::sql::sql_provider_datafusion::{
    self, get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
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

pub struct TrinoTable {
    pool: Arc<TrinoConnectionPool>,
    pub(crate) base_table: SqlTable<Arc<Client>, &'static str>,
}

impl std::fmt::Debug for TrinoTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl TrinoTable {
    pub async fn new(
        pool: &Arc<TrinoConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let dyn_pool =
            Arc::clone(pool) as Arc<dyn DbConnectionPool<Arc<Client>, &'static str> + Send + Sync>;

        let base_table = SqlTable::new("schema", &dyn_pool, table_reference, Some(Engine::Trino))
            .await?
            .with_dialect(Arc::new(DefaultDialect {}));

        Ok(Self {
            pool: Arc::clone(pool),
            base_table,
        })
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TrinoSQLExec::new(
            projections,
            schema,
            &self.base_table.table_reference,
            Arc::clone(&self.pool),
            filters,
            limit,
        )?))
    }
}

#[async_trait]
impl TableProvider for TrinoTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection, &self.schema(), filters, limit)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_table.supports_filters_pushdown(filters)
    }
}

impl Display for TrinoTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrinoTable {}", self.base_table.name())
    }
}

struct TrinoSQLExec {
    base_exec: SqlExec<Arc<Client>, &'static str>,
}

impl TrinoSQLExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<TrinoConnectionPool>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(
            projections,
            schema,
            table_reference,
            pool,
            filters,
            limit,
            Some(Engine::Trino),
        )?;

        Ok(Self { base_exec })
    }

    fn sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }
}

impl std::fmt::Debug for TrinoSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "TrinoSQLExec sql={sql}")
    }
}

impl DisplayAs for TrinoSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "TrinoSQLExec sql={sql}")
    }
}

impl ExecutionPlan for TrinoSQLExec {
    fn name(&self) -> &'static str {
        "TrinoSQLExec"
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
        tracing::debug!("TrinoSQLExec sql: {sql}");

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&self.schema()));

        let stream = futures::stream::once(fut).try_flatten();
        let schema = Arc::clone(&self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
