use std::fmt::{self, Display};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::dialect::Dialect;

use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion::{self, SqlTable};

use crate::conn::{MsSqlParameter, MsSqlPooledConnection};
use crate::dialect::MsSqlDialect;
use crate::pool::MsSqlConnectionPool;

pub type DynMsSqlConnectionPool =
    dyn DbConnectionPool<MsSqlPooledConnection, MsSqlParameter> + Send + Sync;

/// A read-only table in SQL Server.
///
/// The SQL generated for scans uses `LIMIT`, `NULLS FIRST` and boolean literals, which are not
/// T-SQL: they are rewritten by the connection when the query is executed.
pub struct MsSqlTable {
    pub(crate) pool: Arc<MsSqlConnectionPool>,
    pub(crate) base_table: SqlTable<MsSqlPooledConnection, MsSqlParameter>,
}

impl std::fmt::Debug for MsSqlTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsSqlTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl MsSqlTable {
    /// Creates a table, inferring its schema from the database.
    pub async fn new(
        pool: &Arc<MsSqlConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let dyn_pool = Arc::clone(pool) as Arc<DynMsSqlConnectionPool>;
        let base_table = SqlTable::new("mssql", &dyn_pool, table_reference)
            .await?
            .with_dialect(Arc::new(MsSqlDialect {}));
        Ok(Self {
            pool: Arc::clone(pool),
            base_table,
        })
    }

    /// Creates a table with a known schema.
    #[must_use]
    pub fn new_with_schema(
        pool: &Arc<MsSqlConnectionPool>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
    ) -> Self {
        let dyn_pool = Arc::clone(pool) as Arc<DynMsSqlConnectionPool>;
        let base_table = SqlTable::new_with_schema("mssql", &dyn_pool, schema, table_reference)
            .with_dialect(Arc::new(MsSqlDialect {}));
        Self {
            pool: Arc::clone(pool),
            base_table,
        }
    }

    /// Declares the constraints (primary key, unique) that hold on the table.
    #[must_use]
    pub fn with_constraints(self, constraints: Constraints) -> Self {
        Self {
            pool: self.pool,
            base_table: self.base_table.with_constraints(constraints),
        }
    }

    #[must_use]
    pub fn table_reference(&self) -> &TableReference {
        &self.base_table.table_reference
    }

    #[must_use]
    pub fn dialect(&self) -> Arc<dyn Dialect + Send + Sync> {
        self.base_table.dialect_arc()
    }
}

#[async_trait]
impl TableProvider for MsSqlTable {
    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let plan = self
            .base_table
            .scan(state, projection, filters, limit)
            .await?;
        if projection.is_some_and(Vec::is_empty) {
            // An empty projection (e.g. for `COUNT(*)`) is unparsed as `SELECT 1`: drop its column
            let no_columns: Vec<ProjectionExpr> = vec![];
            return Ok(Arc::new(ProjectionExec::try_new(no_columns, plan)?));
        }
        Ok(plan)
    }
}

impl Display for MsSqlTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MsSqlTable {}", self.base_table.name())
    }
}
