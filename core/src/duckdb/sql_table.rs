use crate::sql::db_connection_pool::DbConnectionPool;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::sql::unparser::dialect::Dialect;
use futures::TryStreamExt;
use std::collections::HashMap;
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
    sql::{unparser::dialect::DuckDBDialect, TableReference},
};

pub struct DuckDBTable<T: 'static, P: 'static> {
    pub(crate) base_table: SqlTable<T, P>,

    /// A mapping of table/view names to `DuckDB` functions that can instantiate a table (e.g. "`read_parquet`('`my_file.parquet`')").
    pub(crate) table_functions: Option<HashMap<String, String>>,
}

impl<T, P> std::fmt::Debug for DuckDBTable<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl<T, P> DuckDBTable<T, P> {
    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
        table_functions: Option<HashMap<String, String>>,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Self {
        let base_table = SqlTable::new_with_schema("duckdb", pool, schema, table_reference)
            .with_dialect(dialect.unwrap_or(Arc::new(DuckDBDialect::new())));

        Self {
            base_table,
            table_functions,
        }
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

/// ExecutionPlan node that represents DuckDB's EXPLAIN output
#[derive(Clone)]
struct DuckDBExplainExec<T, P> {
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    explain_sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl<T, P> DuckDBExplainExec<T, P> {
    fn new(
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        sql: String,
    ) -> DataFusionResult<Self> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        use datafusion::physical_plan::Partitioning;

        let explain_sql = format!("EXPLAIN {}", sql);
        
        // DuckDB EXPLAIN returns a single column named 'explain_value' with VARCHAR type
        let schema = Arc::new(Schema::new(vec![
            Field::new("explain_key", DataType::Utf8, false),
            Field::new("explain_value", DataType::Utf8, false),
        ]));

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            pool,
            explain_sql,
            schema,
            properties,
        })
    }
}

impl<T, P> std::fmt::Debug for DuckDBExplainExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBExplainExec")
    }
}

impl<T, P> DisplayAs for DuckDBExplainExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBExplain")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for DuckDBExplainExec<T, P> {
    fn name(&self) -> &'static str {
        "DuckDBExplainExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
        let sql = self.explain_sql.clone();
        let schema = self.schema.clone();
        let pool = self.pool.clone();

        tracing::debug!("DuckDBExplainExec executing: {sql}");

        let fut = get_stream(pool, sql, schema.clone());
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Clone)]
struct DuckSqlExec<T, P> {
    base_exec: SqlExec<T, P>,
    table_functions: Option<HashMap<String, String>>,
    explain_child: Option<Arc<dyn ExecutionPlan>>,
}

impl<T: 'static, P: 'static> DuckSqlExec<T, P> {
    fn new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        sql: String,
        table_functions: Option<HashMap<String, String>>,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(projection, schema, pool.clone(), sql.clone())?;

        // Create the explain child node
        let full_sql = format!(
            "{cte_expr} {sql}",
            cte_expr = get_cte(&table_functions)
        );
        let explain_child: Option<Arc<dyn ExecutionPlan>> = DuckDBExplainExec::new(pool, full_sql)
            .ok()
            .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>);

        Ok(Self {
            base_exec,
            table_functions,
            explain_child,
        })
    }

    fn sql(&self) -> SqlResult<String> {
        let sql = self.base_exec.sql()?;

        Ok(format!(
            "{cte_expr} {sql}",
            cte_expr = get_cte(&self.table_functions)
        ))
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
        self.base_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.base_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        if let Some(ref explain_child) = self.explain_child {
            vec![explain_child]
        } else {
            vec![]
        }
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
