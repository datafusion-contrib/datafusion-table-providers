use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{project_schema, TableReference};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::TryStreamExt;

use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::AsyncDbConnection;

use crate::conn::{quote_identifier, quote_table_reference};
use crate::pool::CassandraConnectionPool;

/// A Cassandra table.
///
/// Filters are not pushed down (see the crate documentation), so DataFusion applies them.
pub struct CassandraTable {
    table_reference: TableReference,
    pool: Arc<CassandraConnectionPool>,
    schema: SchemaRef,
}

impl fmt::Debug for CassandraTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraTable")
            .field("table_reference", &self.table_reference)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl CassandraTable {
    #[must_use]
    pub fn new(
        table_reference: TableReference,
        pool: Arc<CassandraConnectionPool>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            table_reference,
            pool,
            schema,
        }
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for CassandraTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CassandraExec::try_new(
            self.table_reference.clone(),
            Arc::clone(&self.pool),
            &self.schema,
            projection,
            limit,
        )?))
    }
}

/// Scans a Cassandra table with `SELECT <columns> FROM <table> [LIMIT n]`.
pub struct CassandraExec {
    table_reference: TableReference,
    pool: Arc<CassandraConnectionPool>,
    /// The columns to select from Cassandra.
    columns: Vec<String>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl CassandraExec {
    /// Creates a scan of the `projection` columns of `schema`.
    ///
    /// # Errors
    ///
    /// Returns an error if the projection doesn't match the schema.
    pub fn try_new(
        table_reference: TableReference,
        pool: Arc<CassandraConnectionPool>,
        schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projection)?;
        let columns = scan_columns(&table_reference, schema, &projected_schema)?;

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            table_reference,
            pool,
            columns,
            projected_schema,
            limit,
            properties,
        })
    }

    /// The CQL statement this scan runs.
    #[must_use]
    pub fn cql(&self) -> String {
        build_cql(&self.table_reference, &self.columns, self.limit)
    }
}

/// Returns the columns to select from Cassandra to produce `projected_schema`.
fn scan_columns(
    table_reference: &TableReference,
    schema: &SchemaRef,
    projected_schema: &SchemaRef,
) -> DataFusionResult<Vec<String>> {
    if projected_schema.fields().is_empty() {
        // CQL can't select zero columns. Queries like `SELECT count(*)` only need the row
        // count, so select the first column (a partition key column for `SELECT *`) and
        // drop it when converting the rows.
        let first = schema.fields().first().ok_or_else(|| {
            DataFusionError::Plan(format!("Cassandra table {table_reference} has no columns"))
        })?;
        Ok(vec![first.name().clone()])
    } else {
        Ok(projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect())
    }
}

fn build_cql(table_reference: &TableReference, columns: &[String], limit: Option<usize>) -> String {
    let columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let mut cql = format!(
        "SELECT {columns} FROM {}",
        quote_table_reference(table_reference)
    );
    // CQL requires a positive limit; `LIMIT 0` doesn't query Cassandra at all
    if let Some(limit) = limit.filter(|l| *l > 0) {
        cql.push_str(&format!(" LIMIT {limit}"));
    }
    cql
}

impl fmt::Debug for CassandraExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CassandraExec cql={}", self.cql())
    }
}

impl DisplayAs for CassandraExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CassandraExec cql={}", self.cql())
    }
}

impl ExecutionPlan for CassandraExec {
    fn name(&self) -> &'static str {
        "CassandraExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            table_reference: self.table_reference.clone(),
            pool: Arc::clone(&self.pool),
            columns: self.columns.clone(),
            projected_schema: Arc::clone(&self.projected_schema),
            limit,
            properties: Arc::clone(&self.properties),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.projected_schema);
        if self.limit == Some(0) {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )));
        }

        let cql = self.cql();
        let conn = self.pool.connect_direct();

        let stream_schema = Arc::clone(&schema);
        let stream = futures::stream::once(async move {
            conn.query_arrow(&cql, &[], Some(stream_schema))
                .await
                .map_err(DataFusionError::External)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("Name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]))
    }

    fn cql(
        table_reference: TableReference,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> String {
        let schema = schema();
        let projected_schema = project_schema(&schema, projection).unwrap();
        let columns = scan_columns(&table_reference, &schema, &projected_schema).unwrap();
        build_cql(&table_reference, &columns, limit)
    }

    #[test]
    fn selects_all_columns() {
        assert_eq!(
            cql(TableReference::partial("ks", "users"), None, None),
            "SELECT \"id\", \"Name\", \"age\" FROM \"ks\".\"users\""
        );
    }

    #[test]
    fn selects_projected_columns_with_limit() {
        assert_eq!(
            cql(TableReference::bare("users"), Some(&vec![2, 1]), Some(10)),
            "SELECT \"age\", \"Name\" FROM \"users\" LIMIT 10"
        );
    }

    #[test]
    fn selects_first_column_for_empty_projection() {
        assert_eq!(
            cql(TableReference::bare("users"), Some(&vec![]), None),
            "SELECT \"id\" FROM \"users\""
        );
    }

    #[test]
    fn omits_zero_limit() {
        assert_eq!(
            cql(TableReference::bare("users"), Some(&vec![0]), Some(0)),
            "SELECT \"id\" FROM \"users\""
        );
    }

    #[test]
    fn fails_for_table_without_columns() {
        let schema = Arc::new(Schema::empty());
        assert!(scan_columns(&TableReference::bare("t"), &schema, &schema).is_err());
    }
}
