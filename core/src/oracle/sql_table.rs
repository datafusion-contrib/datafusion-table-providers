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
    arrow::datatypes::{DataType, SchemaRef},
    common::utils::quote_identifier,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
    sql::{
        sqlparser,
        unparser::{
            dialect::{CustomDialect, CustomDialectBuilder},
            Unparser,
        },
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

    pub(crate) fn dialect() -> CustomDialect {
        CustomDialectBuilder::new()
            .with_identifier_quote_style('"')
            // There is no 'DOUBLE' SQL type in Oracle: it can use 'FLOAT' for both single and double precision float values
            .with_float64_ast_dtype(sqlparser::ast::DataType::Float(
                sqlparser::ast::ExactNumberInfo::None,
            ))
            .build()
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = if let Some(proj) = projections {
            Arc::new(schema.project(proj)?)
        } else {
            Arc::clone(schema)
        };

        let columns = projected_schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let dialect = Self::dialect();

        let where_expr = if filters.is_empty() {
            String::new()
        } else {
            let filter_expr = filters
                .iter()
                .map(|f| {
                    Unparser::new(&dialect)
                        .expr_to_sql(f)
                        .map(|e| e.to_string())
                })
                .collect::<DataFusionResult<Vec<String>>>()?
                .join(" AND ");
            format!("WHERE {filter_expr}")
        };

        let limit_expr = if let Some(limit) = limit {
            format!("FETCH FIRST {limit} ROWS ONLY")
        } else {
            String::new()
        };

        let table_reference = self.base_table.table_reference.to_quoted_string();
        let sql = format!("SELECT {columns} FROM {table_reference} {where_expr} {limit_expr}");

        Ok(Arc::new(OracleSQLExec::new(
            projections,
            schema,
            Arc::clone(&self.pool),
            sql,
        )?))
    }

    /// Check if an expression contains datetime-related types that Oracle cannot handle
    /// in filter pushdown due to datetime literal format requirements.
    fn contains_datetime_expr(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                Self::is_datetime_type_expr(&binary_expr.left)
                    || Self::is_datetime_type_expr(&binary_expr.right)
                    || Self::contains_datetime_expr(&binary_expr.left)
                    || Self::contains_datetime_expr(&binary_expr.right)
            }
            Expr::Not(inner) => Self::contains_datetime_expr(inner),
            _ => Self::is_datetime_type_expr(expr),
        }
    }

    fn is_datetime_type_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Cast(cast) => matches!(
                cast.data_type,
                DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
            ),
            Expr::Literal(literal, _) => matches!(
                literal.data_type(),
                DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
            ),
            _ => false,
        }
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
        // Oracle requires specific format for datetime literals that the expression
        // unparser cannot handle correctly, resulting in ORA-01843 errors.
        // We mark datetime-related filters as unsupported to prevent pushdown.
        let mut results = Vec::with_capacity(filters.len());
        for filter in filters {
            if Self::contains_datetime_expr(filter) {
                results.push(TableProviderFilterPushDown::Unsupported);
            } else {
                // For non-datetime filters, delegate to base table
                let base_result = self.base_table.supports_filters_pushdown(&[filter])?;
                results.extend(base_result);
            }
        }
        Ok(results)
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
