//! # SQL DataFusion TableProvider
//!
//! This module implements a SQL TableProvider for DataFusion.
//!
//! This is used as a fallback if the `datafusion-federation` optimizer is not enabled.

use crate::sql::db_connection_pool::{
    self,
    dbconnection::{get_schema, query_arrow},
    DbConnectionPool,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    sql::unparser::dialect::{DefaultDialect, Dialect},
};
use futures::TryStreamExt;
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};
use std::{
    fmt::{Display, Formatter},
    sync::LazyLock,
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{
        logical_plan::builder::LogicalTableSource, Expr, LogicalPlan, LogicalPlanBuilder,
        TableProviderFilterPushDown, TableType,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, PlanProperties, SendableRecordBatchStream,
    },
    sql::{unparser::Unparser, TableReference},
};

mod expr;
#[cfg(feature = "federation")]
pub mod federation;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get a DB connection from the pool: {source}"))]
    UnableToGetConnectionFromPool { source: db_connection_pool::Error },

    #[snafu(display("Unable to get schema: {source}"))]
    UnableToGetSchema {
        source: db_connection_pool::dbconnection::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: DataFusionError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SqlTable<T: 'static, P: 'static> {
    name: String,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    schema: SchemaRef,
    pub table_reference: TableReference,
    dialect: Option<Arc<dyn Dialect + Send + Sync>>,
}

impl<T, P> fmt::Debug for SqlTable<T, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlTable")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .field("table_reference", &self.table_reference)
            .finish()
    }
}

impl<T, P> SqlTable<T, P> {
    pub async fn new(
        name: &str,
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let conn = pool
            .connect()
            .await
            .context(UnableToGetConnectionFromPoolSnafu)?;

        let schema = get_schema(conn, &table_reference)
            .await
            .context(UnableToGetSchemaSnafu)?;

        Ok(Self::new_with_schema(name, pool, schema, table_reference))
    }

    pub fn new_with_schema(
        name: &str,
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            pool: Arc::clone(pool),
            schema: schema.into(),
            table_reference: table_reference.into(),
            dialect: None,
        }
    }

    pub fn scan_to_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<String> {
        let logical_plan = self.create_logical_plan(projection, filters, limit)?;
        let sql = Unparser::new(self.dialect())
            .plan_to_sql(&logical_plan)?
            .to_string();

        Ok(sql)
    }

    fn create_logical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<LogicalPlan> {
        let table_source = LogicalTableSource::new(self.schema());
        LogicalPlanBuilder::scan_with_filters(
            self.table_reference.clone(),
            Arc::new(table_source),
            projection.cloned(),
            filters.to_vec(),
        )?
        .limit(0, limit)?
        .build()
    }

    fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        sql: String,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SqlExec::new(
            projection,
            &self.schema(),
            Arc::clone(&self.pool),
            sql,
        )?))
    }

    #[must_use]
    pub fn with_dialect(self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        Self {
            dialect: Some(dialect),
            ..self
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn clone_pool(&self) -> Arc<dyn DbConnectionPool<T, P> + Send + Sync> {
        Arc::clone(&self.pool)
    }

    fn dialect(&self) -> &(dyn Dialect + Send + Sync) {
        match &self.dialect {
            Some(dialect) => dialect.as_ref(),
            None => &DefaultDialect {},
        }
    }
}

#[async_trait]
impl<T, P> TableProvider for SqlTable<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let filter_push_down = default_filter_pushdown(filters, self.dialect());
        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sql = self.scan_to_sql(projection, filters, limit)?;
        return self.create_physical_plan(projection, sql);
    }
}

pub fn default_filter_pushdown(
    filters: &[&Expr],
    dialect: &dyn Dialect,
) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|f| match Unparser::new(dialect).expr_to_sql(f) {
            // The DataFusion unparser currently does not correctly handle unparsing subquery expressions on TableScan filters.
            Ok(_) => match expr::expr_contains_subquery(f) {
                Ok(true) => TableProviderFilterPushDown::Unsupported,
                Ok(false) => TableProviderFilterPushDown::Exact,
                Err(_) => TableProviderFilterPushDown::Unsupported,
            },
            Err(_) => TableProviderFilterPushDown::Unsupported,
        })
        .collect()
}

impl<T, P> Display for SqlTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqlTable {}", self.name)
    }
}

static ONE_COLUMN_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, true)])));

pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                // If the projection is Some([]) then it gets unparsed as `SELECT 1`, so return a schema with a single Int64 column.
                //
                // See: <https://github.com/apache/datafusion/blob/83ce79c39412a4f150167d00e40ea05948c4870f/datafusion/sql/src/unparser/plan.rs#L998>
                Arc::clone(&ONE_COLUMN_SCHEMA)
            } else {
                Arc::new(schema.project(columns)?)
            }
        }
        None => Arc::clone(schema),
    };
    Ok(schema)
}

#[derive(Clone)]
pub struct SqlExec<T, P> {
    projected_schema: SchemaRef,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    sql: String,
    properties: PlanProperties,
}

impl<T, P> SqlExec<T, P> {
    pub fn new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        sql: String,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema_safe(schema, projection)?;

        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            pool,
            sql,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }

    #[must_use]
    pub fn clone_pool(&self) -> Arc<dyn DbConnectionPool<T, P> + Send + Sync> {
        Arc::clone(&self.pool)
    }

    pub fn sql(&self) -> Result<String> {
        Ok(self.sql.clone())
    }
}

impl<T, P> std::fmt::Debug for SqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlExec sql={sql}")
    }
}

impl<T, P> DisplayAs for SqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for SqlExec<T, P> {
    fn name(&self) -> &'static str {
        "SqlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
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
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("SqlExec sql: {sql}");

        let schema = self.schema();

        let fut = get_stream(Arc::clone(&self.pool), sql, Arc::clone(&schema));

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub async fn get_stream<T: 'static, P: 'static>(
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    sql: String,
    projected_schema: SchemaRef,
) -> DataFusionResult<SendableRecordBatchStream> {
    let conn = pool.connect().await.map_err(to_execution_error)?;

    query_arrow(conn, sql, Some(projected_schema))
        .await
        .map_err(to_execution_error)
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    use tracing::{level_filters::LevelFilter, subscriber::DefaultGuard, Dispatch};

    use crate::sql::sql_provider_datafusion::SqlTable;

    fn setup_tracing() -> DefaultGuard {
        let subscriber: tracing_subscriber::FmtSubscriber = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::DEBUG)
            .finish();

        let dispatch = Dispatch::new(subscriber);
        tracing::dispatcher::set_default(&dispatch)
    }

    mod sql_table_plan_to_sql_tests {
        use std::any::Any;

        use async_trait::async_trait;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::sql::unparser::dialect::{Dialect, SqliteDialect};
        use datafusion::{
            logical_expr::{col, lit},
            sql::TableReference,
        };

        use crate::sql::db_connection_pool::{
            dbconnection::DbConnection, DbConnectionPool, JoinPushDown,
        };

        use super::*;

        struct MockConn {}

        impl DbConnection<(), &'static dyn ToString> for MockConn {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        struct MockDBPool {}

        #[async_trait]
        impl DbConnectionPool<(), &'static dyn ToString> for MockDBPool {
            async fn connect(
                &self,
            ) -> Result<
                Box<dyn DbConnection<(), &'static dyn ToString>>,
                Box<dyn Error + Send + Sync>,
            > {
                Ok(Box::new(MockConn {}))
            }

            fn join_push_down(&self) -> JoinPushDown {
                JoinPushDown::Disallow
            }
        }

        fn new_sql_table(
            table_reference: &'static str,
            dialect: Option<Arc<dyn Dialect + Send + Sync>>,
        ) -> Result<SqlTable<(), &'static dyn ToString>, Box<dyn Error + Send + Sync>> {
            let fields = vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int16, false),
                Field::new(
                    "createdDate",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("userId", DataType::LargeUtf8, false),
                Field::new("active", DataType::Boolean, false),
                Field::new("5e48", DataType::LargeUtf8, false),
            ];
            let schema = Arc::new(Schema::new(fields));
            let pool = Arc::new(MockDBPool {})
                as Arc<dyn DbConnectionPool<(), &'static dyn ToString> + Send + Sync>;
            let table_ref = TableReference::parse_str(table_reference);

            let sql_table = SqlTable::new_with_schema(table_reference, &pool, schema, table_ref);
            if let Some(dialect) = dialect {
                Ok(sql_table.with_dialect(dialect))
            } else {
                Ok(sql_table)
            }
        }

        #[tokio::test]
        async fn test_sql_to_string() -> Result<(), Box<dyn Error + Send + Sync>> {
            let sql_table = new_sql_table("users", Some(Arc::new(SqliteDialect {})))?;
            let result = sql_table.scan_to_sql(Some(&vec![0]), &[], None)?;
            assert_eq!(result, r#"SELECT `users`.`name` FROM `users`"#);
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_filters_and_limit(
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let filters = vec![col("age").gt_eq(lit(30)).and(col("name").eq(lit("x")))];
            let sql_table = new_sql_table("users", Some(Arc::new(SqliteDialect {})))?;
            let result = sql_table.scan_to_sql(Some(&vec![0, 1]), &filters, Some(3))?;
            assert_eq!(
                result,
                r#"SELECT `users`.`name`, `users`.`age` FROM `users` WHERE ((`users`.`age` >= 30) AND (`users`.`name` = 'x')) LIMIT 3"#
            );
            Ok(())
        }
    }

    #[test]
    fn test_references() {
        let table_ref = TableReference::bare("test");
        assert_eq!(format!("{table_ref}"), "test");
    }

    // XXX move this to duckdb mod??
    #[cfg(feature = "duckdb")]
    mod duckdb_tests {
        use super::*;
        use crate::sql::db_connection_pool::dbconnection::duckdbconn::{
            DuckDBSyncParameter, DuckDbConnection,
        };
        use crate::sql::db_connection_pool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool};
        use duckdb::DuckdbConnectionManager;

        #[tokio::test]
        async fn test_duckdb_table() -> Result<(), Box<dyn Error + Send + Sync>> {
            let t = setup_tracing();
            let ctx = SessionContext::new();
            let pool: Arc<
                dyn DbConnectionPool<
                        r2d2::PooledConnection<DuckdbConnectionManager>,
                        Box<dyn DuckDBSyncParameter>,
                    > + Send
                    + Sync,
            > = Arc::new(DuckDbConnectionPool::new_memory()?)
                as Arc<
                    dyn DbConnectionPool<
                            r2d2::PooledConnection<DuckdbConnectionManager>,
                            Box<dyn DuckDBSyncParameter>,
                        > + Send
                        + Sync,
                >;
            let conn = pool.connect().await?;
            let db_conn = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .expect("Unable to downcast to DuckDbConnection");
            db_conn.conn.execute_batch(
                "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
            )?;
            let duckdb_table = SqlTable::new("duckdb", &pool, "test").await?;
            ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
            let sql = "SELECT * FROM test_datafusion limit 1";
            let df = ctx.sql(sql).await?;
            df.show().await?;
            drop(t);
            Ok(())
        }

        #[tokio::test]
        async fn test_duckdb_table_filter() -> Result<(), Box<dyn Error + Send + Sync>> {
            let t = setup_tracing();
            let ctx = SessionContext::new();
            let pool: Arc<
                dyn DbConnectionPool<
                        r2d2::PooledConnection<DuckdbConnectionManager>,
                        Box<dyn DuckDBSyncParameter>,
                    > + Send
                    + Sync,
            > = Arc::new(DuckDbConnectionPool::new_memory()?)
                as Arc<
                    dyn DbConnectionPool<
                            r2d2::PooledConnection<DuckdbConnectionManager>,
                            Box<dyn DuckDBSyncParameter>,
                        > + Send
                        + Sync,
                >;
            let conn = pool.connect().await?;
            let db_conn = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .expect("Unable to downcast to DuckDbConnection");
            db_conn.conn.execute_batch(
                "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
            )?;
            let duckdb_table = SqlTable::new("duckdb", &pool, "test").await?;
            ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
            let sql = "SELECT * FROM test_datafusion where a > 1 and b = 'bar' limit 1";
            let df = ctx.sql(sql).await?;
            df.show().await?;
            drop(t);
            Ok(())
        }
    }
}
