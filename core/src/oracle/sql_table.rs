use crate::sql::db_connection_pool::oraclepool::OracleConnectionPool;

use async_trait::async_trait;
use datafusion::catalog::Session;
use futures::TryStreamExt;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use crate::sql::db_connection_pool::dbconnection::oracleconn::OraclePooledConnection;
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
};

pub struct OracleTable {
    pool: Arc<OracleConnectionPool>,
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
    pub fn new(
        pool: Arc<OracleConnectionPool>,
        base_table: SqlTable<OraclePooledConnection, oracle::sql_type::OracleType>,
    ) -> Self {
        Self { pool, base_table }
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sql = self.base_table.scan_to_sql(projections, filters, limit)?;
        Ok(Arc::new(OracleSQLExec::new(
            projections,
            schema,
            Arc::clone(&self.pool),
            sql,
        )?))
    }
}

#[async_trait]
impl TableProvider for OracleTable {
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
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

impl Display for OracleTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OracleTable {}", self.base_table.name())
    }
}

struct OracleSQLExec {
    base_exec: SqlExec<OraclePooledConnection, oracle::sql_type::OracleType>,
}

impl OracleSQLExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<OracleConnectionPool>,
        sql: String,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(projections, schema, pool, sql)?;

        Ok(Self { base_exec })
    }

    fn sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }
}

impl std::fmt::Debug for OracleSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "OracleSQLExec sql={sql}")
    }
}

impl DisplayAs for OracleSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "OracleSQLExec sql={sql}")
    }
}

impl ExecutionPlan for OracleSQLExec {
    fn name(&self) -> &'static str {
        "OracleSQLExec"
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
        tracing::debug!("OracleSQLExec sql: {sql}");

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&self.schema()));

        let stream = futures::stream::once(fut).try_flatten();
        let schema = Arc::clone(&self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
