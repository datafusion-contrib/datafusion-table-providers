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
    sql::unparser::dialect::{
        DefaultDialect, Dialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
    },
};
use futures::TryStreamExt;
use snafu::prelude::*;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    },
    sql::{sqlparser::ast, unparser::Unparser, TableReference},
};

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

#[derive(Clone, Copy, Debug)]
pub enum Engine {
    Spark,
    SQLite,
    DuckDB,
    ODBC,
    Postgres,
    MySQL,
    Default,
}

impl Engine {
    /// Get the corresponding `Dialect` to use for unparsing
    pub fn dialect(&self) -> Arc<dyn Dialect + Send + Sync> {
        match self {
            Engine::SQLite => Arc::new(SqliteDialect {}),
            Engine::Postgres => Arc::new(PostgreSqlDialect {}),
            Engine::MySQL => Arc::new(MySqlDialect {}),
            Engine::Spark | Engine::DuckDB | Engine::ODBC | Engine::Default => {
                Arc::new(DefaultDialect {})
            }
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SqlTable<T: 'static, P: 'static> {
    name: &'static str,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    schema: SchemaRef,
    pub table_reference: TableReference,
    engine: Engine,
}

impl<T, P> SqlTable<T, P> {
    pub async fn new(
        name: &'static str,
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        table_reference: impl Into<TableReference>,
        engine: Option<Engine>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let conn = pool
            .connect()
            .await
            .context(UnableToGetConnectionFromPoolSnafu)?;

        let schema = get_schema(conn, &table_reference)
            .await
            .context(UnableToGetSchemaSnafu)?;

        Ok(Self::new_with_schema(
            name,
            pool,
            schema,
            table_reference,
            engine,
        ))
    }

    pub fn new_with_schema(
        name: &'static str,
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
        engine: Option<Engine>,
    ) -> Self {
        let engine = engine.unwrap_or(Engine::Default);
        Self {
            name,
            pool: Arc::clone(pool),
            schema: schema.into(),
            table_reference: table_reference.into(),
            engine,
        }
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SqlExec::new(
            projections,
            schema,
            &self.table_reference,
            Arc::clone(&self.pool),
            filters,
            limit,
            Some(self.engine),
        )?))
    }

    // Return the current memory location of the object as a unique identifier
    fn unique_id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub fn clone_pool(&self) -> Arc<dyn DbConnectionPool<T, P> + Send + Sync> {
        Arc::clone(&self.pool)
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
        let filter_push_down: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(
                |f| match Unparser::new(self.engine.dialect().as_ref()).expr_to_sql(f) {
                    Ok(_) => TableProviderFilterPushDown::Exact,
                    Err(_) => TableProviderFilterPushDown::Unsupported,
                },
            )
            .collect();

        Ok(filter_push_down)
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

impl<T, P> Display for SqlTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqlTable {}", self.name)
    }
}

#[derive(Clone)]
pub struct SqlExec<T, P> {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
    engine: Engine,
}

pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                Arc::clone(schema)
            } else {
                Arc::new(schema.project(columns)?)
            }
        }
        None => Arc::clone(schema),
    };
    Ok(schema)
}

impl<T, P> SqlExec<T, P> {
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        filters: &[Expr],
        limit: Option<usize>,
        engine: Option<Engine>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema_safe(schema, projections)?;
        let engine = engine.unwrap_or(Engine::Default);

        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            pool,
            filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            engine,
        })
    }

    #[must_use]
    pub fn clone_pool(&self) -> Arc<dyn DbConnectionPool<T, P> + Send + Sync> {
        Arc::clone(&self.pool)
    }

    pub fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| self.column_name_escaped(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match self.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(|f| {
                    Unparser::new(self.engine.dialect().as_ref())
                        .expr_to_sql(f)
                        .map(|e| e.to_string())
                })
                .collect::<DataFusionResult<Vec<String>>>()
                .context(UnableToGenerateSQLSnafu)?
                .join(" AND ");
            format!("WHERE {}", filter_expr)
        };

        Ok(format!(
            "SELECT {columns} FROM {table_reference} {where_expr} {limit_expr}",
            table_reference = self.table_name_escaped(),
        ))
    }

    fn table_name_escaped(&self) -> String {
        match &self.table_reference {
            TableReference::Bare { table } => self.ident_escaped(&table),
            TableReference::Partial { schema, table } => format!(
                "{}.{}",
                self.ident_escaped(&schema),
                self.ident_escaped(&table)
            ),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => format!(
                "{}.{}.{}",
                self.ident_escaped(&catalog),
                self.ident_escaped(&schema),
                self.ident_escaped(&table)
            ),
        }
    }

    fn column_name_escaped(&self, column_name: &str) -> String {
        self.ident_escaped(column_name)
    }

    fn ident_escaped(&self, ident: &str) -> String {
        let quote_style = self.engine.dialect().identifier_quote_style(ident);
        ast::Expr::Identifier(ast::Ident {
            value: ident.to_string(),
            quote_style,
        })
        .to_string()
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

    mod sql_exec_tests {
        use std::any::Any;

        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use async_trait::async_trait;
        use datafusion::{
            logical_expr::{col, lit, Expr},
            sql::TableReference,
        };

        use crate::sql::db_connection_pool::{
            dbconnection::DbConnection, DbConnectionPool, JoinPushDown,
        };

        use super::super::{Engine, SqlExec};
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

        fn new_sql_exec(
            projections: Option<&Vec<usize>>,
            table_reference: &str,
            filters: &[Expr],
            limit: Option<usize>,
            engine: Option<Engine>,
        ) -> Result<SqlExec<(), &'static dyn ToString>, Box<dyn Error + Send + Sync>> {
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

            Ok(SqlExec::new(
                projections,
                &schema,
                &table_ref,
                pool,
                filters,
                limit,
                engine,
            )?)
        }

        #[tokio::test]
        async fn test_sql_to_string() -> Result<(), Box<dyn Error + Send + Sync>> {
            let sql_exec = new_sql_exec(Some(&vec![0]), "users", &[], None, None)?;
            assert_eq!(sql_exec.sql()?, r#"SELECT "name" FROM users  "#);
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_limit() -> Result<(), Box<dyn Error + Send + Sync>> {
            let sql_exec = new_sql_exec(Some(&vec![0, 1]), "users", &[], Some(3), None)?;
            assert_eq!(sql_exec.sql()?, r#"SELECT "name", age FROM users  LIMIT 3"#);
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_filters() -> Result<(), Box<dyn Error + Send + Sync>> {
            let filters = vec![col("age").gt_eq(lit(30)).and(col("name").eq(lit("x")))];
            let sql_exec = new_sql_exec(Some(&vec![0, 1]), "users", &filters, None, None)?;
            assert_eq!(
                sql_exec.sql()?,
                r#"SELECT "name", age FROM users WHERE ((age >= 30) AND ("name" = 'x')) "#
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_filters_and_limit(
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let filters = vec![col("age").gt_eq(lit(30)).and(col("name").eq(lit("x")))];
            let sql_exec = new_sql_exec(Some(&vec![0, 1]), "users", &filters, Some(3), None)?;
            assert_eq!(
                sql_exec.sql()?,
                r#"SELECT "name", age FROM users WHERE ((age >= 30) AND ("name" = 'x')) LIMIT 3"#
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_engine() -> Result<(), Box<dyn Error + Send + Sync>> {
            let filters = vec![col("age").gt_eq(lit(30)).and(col("name").eq(lit("x")))];
            let sql_exec = new_sql_exec(
                Some(&vec![0, 1]),
                "users",
                &filters,
                Some(3),
                Some(Engine::DuckDB),
            )?;
            assert_eq!(
                sql_exec.sql()?,
                r#"SELECT "name", age FROM users WHERE ((age >= 30) AND ("name" = 'x')) LIMIT 3"#
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_not_reasonable_name(
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let filters = vec![col("5e48").eq(lit("test")).and(col("name").eq(lit("x")))];
            let sql_exec = new_sql_exec(Some(&vec![0, 1, 5]), "users", &filters, Some(3), None)?;
            assert_eq!(
                sql_exec.sql()?,
                r#"SELECT "name", age, "5e48" FROM users WHERE (("5e48" = 'test') AND ("name" = 'x')) LIMIT 3"#
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_sql_to_string_with_not_reasonable_name_mysql(
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let filters = vec![col("5e48").eq(lit("test")).and(col("name").eq(lit("x")))];
            let sql_exec = new_sql_exec(
                Some(&vec![0, 1, 5]),
                "users",
                &filters,
                Some(3),
                Some(Engine::MySQL),
            )?;
            assert_eq!(
                sql_exec.sql()?,
                r#"SELECT `name`, `age`, `5e48` FROM `users` WHERE ((`5e48` = 'test') AND (`name` = 'x')) LIMIT 3"#
            );
            Ok(())
        }

        // TODO cover more test cases with different Engines & Dialects
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
        use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
        use crate::sql::db_connection_pool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool};
        use duckdb::{DuckdbConnectionManager, ToSql};

        #[tokio::test]
        async fn test_duckdb_table() -> Result<(), Box<dyn Error + Send + Sync>> {
            let t = setup_tracing();
            let ctx = SessionContext::new();
            let pool: Arc<
                dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
                    + Send
                    + Sync,
            > = Arc::new(DuckDbConnectionPool::new_memory()?);
            let conn = pool.connect().await?;
            let db_conn = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .expect("Unable to downcast to DuckDbConnection");
            db_conn.conn.execute_batch(
                "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
            )?;
            let duckdb_table = SqlTable::new("duckdb", &pool, "test", None).await?;
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
                dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
                    + Send
                    + Sync,
            > = Arc::new(DuckDbConnectionPool::new_memory()?);
            let conn = pool.connect().await?;
            let db_conn = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .expect("Unable to downcast to DuckDbConnection");
            db_conn.conn.execute_batch(
                "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
            )?;
            let duckdb_table = SqlTable::new("duckdb", &pool, "test", None).await?;
            ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
            let sql = "SELECT * FROM test_datafusion where a > 1 and b = 'bar' limit 1";
            let df = ctx.sql(sql).await?;
            df.show().await?;
            drop(t);
            Ok(())
        }
    }
}
