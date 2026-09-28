use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Constraints, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::sql::unparser::dialect::DuckDBDialect;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::TableReference;
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion::{self, SqlTable};
use datafusion_table_providers_common::SOURCE_TYPE_METADATA_KEY;
use futures::StreamExt;

use crate::conn::QuackSession;
use crate::pool::QuackConnectionPool;

/// A table on a Quack server, scanned through [`SqlTable`] with DuckDB's SQL dialect.
///
/// Filters are pushed down only where DuckDB is known to give the same answer as
/// DataFusion (see [`filter_is_exact`]); everything else is evaluated by DataFusion.
pub(crate) struct QuackTable {
    pub(crate) base_table: SqlTable<QuackSession, ()>,
}

impl fmt::Debug for QuackTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuackTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl QuackTable {
    /// Resolves the table's schema on the server.
    pub(crate) async fn new(
        pool: &Arc<QuackConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let pool = Arc::clone(pool) as Arc<dyn DbConnectionPool<QuackSession, ()> + Send + Sync>;
        let base_table = SqlTable::new("quack", &pool, table_reference)
            .await?
            .with_dialect(Arc::new(DuckDBDialect::new()));
        Ok(Self { base_table })
    }
}

#[async_trait]
impl TableProvider for QuackTable {
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
        let schema = self.schema();
        let dialect = DuckDBDialect::new();
        let unparser = Unparser::new(&dialect);
        Ok(filters
            .iter()
            .map(|filter| {
                if filter_is_exact(filter, &schema) && unparser.expr_to_sql(filter).is_ok() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
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
        Ok(Arc::new(QuackSqlExec::new(
            plan,
            projection.is_some_and(Vec::is_empty),
        )))
    }
}

/// Whether DuckDB evaluates `filter` exactly as DataFusion does, so DataFusion may drop its
/// own copy of it.
///
/// Allowed: a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`) or `IN` list between a column
/// whose DuckDB type [`has_duckdb_ordering`] and literals of the column's own Arrow type;
/// `IS [NOT] NULL` on any column; a BOOLEAN column on its own; and `AND`, `OR`, `NOT` over
/// those. Anything else,
/// including casts, arithmetic and functions, is not.
pub(crate) fn filter_is_exact(filter: &Expr, schema: &Schema) -> bool {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => {
                filter_is_exact(left, schema) && filter_is_exact(right, schema)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                column_compares_to_literals(left, [right.as_ref()], schema)
                    || column_compares_to_literals(right, [left.as_ref()], schema)
            }
            _ => false,
        },
        Expr::Not(inner) => filter_is_exact(inner, schema),
        Expr::Column(column) => schema.field_with_name(&column.name).is_ok_and(|field| {
            field
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str)
                == Some("BOOLEAN")
        }),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            matches!(inner.as_ref(), Expr::Column(c) if schema.field_with_name(&c.name).is_ok())
        }
        Expr::InList(InList { expr, list, .. }) => {
            column_compares_to_literals(expr, list.iter(), schema)
        }
        _ => false,
    }
}

fn column_compares_to_literals<'a>(
    column: &Expr,
    literals: impl IntoIterator<Item = &'a Expr>,
    schema: &Schema,
) -> bool {
    let Expr::Column(column) = column else {
        return false;
    };
    let Ok(field) = schema.field_with_name(&column.name) else {
        return false;
    };
    has_duckdb_ordering(field)
        && literals.into_iter().all(
            |literal| matches!(literal, Expr::Literal(value, _) if value.data_type() == *field.data_type()),
        )
}

/// Whether DuckDB compares and orders values of this column, against a literal the
/// DuckDB unparser renders, exactly as DataFusion does. Decided by the DuckDB type in the
/// field's source type metadata.
///
/// Left out, and why:
/// - VARCHAR and other strings: a column may carry a collation (inherited silently
///   through views, or from the server's `default_collation`) that the catalog doesn't
///   report.
/// - FLOAT, DOUBLE: NaN and signed zero semantics.
/// - HUGEINT, UHUGEINT: literals render as bare integers, which DuckDB types by magnitude
///   and turns into DOUBLE beyond the 128-bit range.
/// - TIMESTAMP_NS: literals render as a microsecond `TIMESTAMP`, dropping nanoseconds.
/// - TIMESTAMPTZ, TIME: depend on session settings or render inexactly.
/// - ENUM: ordered by position, not by label. UUID: compared as UUIDs, not strings.
pub(crate) fn has_duckdb_ordering(field: &Field) -> bool {
    let Some(duckdb_type) = field.metadata().get(SOURCE_TYPE_METADATA_KEY) else {
        return false;
    };
    matches!(
        duckdb_type.as_str(),
        "BOOLEAN"
            | "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "BIGINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
            | "UBIGINT"
            | "DATE"
            | "TIMESTAMP"
            | "TIMESTAMP_S"
            | "TIMESTAMP_MS"
    ) || duckdb_type.starts_with("DECIMAL(")
}

/// Wraps the scan's `SqlExec` so the plan can't push work to DuckDB that
/// [`QuackTable::supports_filters_pushdown`] would refuse.
///
/// `SqlExec` accepts any physical filter it can render into its SQL, which would let
/// DataFusion move a filter the table answered `Unsupported` (a float comparison, say) to
/// DuckDB and drop its own copy. This wrapper refuses those filters, and only lets a sort
/// through when every key has the same ordering in DuckDB and DataFusion, and only once:
/// `SqlExec` appends its `ORDER BY` to the SQL, so a second one would be invalid.
///
/// For an empty projection (`COUNT(*)`), `SqlExec` selects a constant `1`; the wrapper
/// presents the zero-column plan DataFusion asked for and emits batches with only a row
/// count.
#[derive(Debug)]
struct QuackSqlExec {
    inner: Arc<dyn ExecutionPlan>,
    /// Set for an empty projection: the plan's own, zero-column properties.
    no_columns: Option<Arc<PlanProperties>>,
    /// Whether the SQL already has a pushed-down `ORDER BY`.
    sorted: bool,
}

impl QuackSqlExec {
    fn new(inner: Arc<dyn ExecutionPlan>, empty_projection: bool) -> Self {
        let no_columns = empty_projection.then(|| {
            Arc::new(
                PlanProperties::clone(inner.properties())
                    .with_eq_properties(EquivalenceProperties::new(Arc::new(Schema::empty()))),
            )
        });
        Self {
            inner,
            no_columns,
            sorted: false,
        }
    }

    fn wrap(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            no_columns: self.no_columns.clone(),
            sorted: self.sorted,
        })
    }

    fn wrap_sorted(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            no_columns: self.no_columns.clone(),
            sorted: true,
        })
    }
}

impl DisplayAs for QuackSqlExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Quack")?;
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for QuackSqlExec {
    fn name(&self) -> &'static str {
        "QuackSqlExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.no_columns
            .as_ref()
            .unwrap_or_else(|| self.inner.properties())
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

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit).map(|inner| self.wrap(inner))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DataFusionResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let schema = self.schema();
        let sortable = order.iter().all(|sort| {
            sort.expr
                .downcast_ref::<Column>()
                .and_then(|column| schema.field_with_name(column.name()).ok())
                .is_some_and(has_duckdb_ordering)
        });
        if self.sorted || !sortable {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        Ok(match self.inner.try_pushdown_sort(order)? {
            SortOrderPushdownResult::Exact { inner } => SortOrderPushdownResult::Exact {
                inner: self.wrap_sorted(inner),
            },
            SortOrderPushdownResult::Inexact { inner } => SortOrderPushdownResult::Inexact {
                inner: self.wrap_sorted(inner),
            },
            SortOrderPushdownResult::Unsupported => SortOrderPushdownResult::Unsupported,
        })
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::all_unsupported(
            child_pushdown_result,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;
        if self.no_columns.is_none() {
            return Ok(stream);
        }
        let schema = self.schema();
        let batch_schema = Arc::clone(&schema);
        let row_counts = stream.map(move |batch| {
            let batch = batch?;
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(&batch_schema),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, row_counts)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        if self.no_columns.is_some() {
            return Ok(Arc::new(Statistics::new_unknown(&self.schema())));
        }
        self.inner.partition_statistics(partition)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::prelude::{cast, col, lit, not};
    use datafusion::scalar::ScalarValue;

    use super::*;

    fn field(name: &str, data_type: DataType, duckdb_type: &str) -> Field {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            SOURCE_TYPE_METADATA_KEY.to_string(),
            duckdb_type.to_string(),
        )]))
    }

    fn schema() -> Schema {
        Schema::new(vec![
            field("i", DataType::Int32, "INTEGER"),
            field("u", DataType::UInt64, "UBIGINT"),
            field("dec", DataType::Decimal128(10, 2), "DECIMAL(10,2)"),
            field("d", DataType::Date32, "DATE"),
            field(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "TIMESTAMP",
            ),
            field(
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, None),
                "TIMESTAMP_S",
            ),
            field(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "TIMESTAMP_NS",
            ),
            field(
                "tstz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "TIMESTAMP WITH TIME ZONE",
            ),
            field("b", DataType::Boolean, "BOOLEAN"),
            field("s", DataType::Utf8, "VARCHAR"),
            field("e", DataType::Utf8, "ENUM('x', 'y')"),
            field("uuid", DataType::Utf8, "UUID"),
            field("f", DataType::Float64, "DOUBLE"),
            field("h", DataType::Decimal256(39, 0), "HUGEINT"),
            Field::new("no_metadata", DataType::Int32, true),
        ])
    }

    fn exact(filter: Expr) -> bool {
        filter_is_exact(&filter, &schema())
    }

    #[test]
    fn comparisons_on_ordered_types_are_exact() {
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let filter = |c: &str, v: ScalarValue| {
                Expr::BinaryExpr(BinaryExpr::new(Box::new(col(c)), op, Box::new(lit(v))))
            };
            assert!(exact(filter("i", ScalarValue::Int32(Some(1)))), "{op}");
            assert!(exact(filter("u", ScalarValue::UInt64(Some(u64::MAX)))));
            assert!(exact(filter(
                "dec",
                ScalarValue::Decimal128(Some(12345), 10, 2)
            )));
            assert!(exact(filter("d", ScalarValue::Date32(Some(19723)))));
            assert!(exact(filter(
                "ts",
                ScalarValue::TimestampMicrosecond(Some(1), None)
            )));
            assert!(exact(filter(
                "ts_s",
                ScalarValue::TimestampSecond(Some(1), None)
            )));
            assert!(exact(filter("b", ScalarValue::Boolean(Some(true)))));
        }
        // Literal on the left.
        assert!(exact(lit(1i32).lt(col("i"))));
    }

    #[test]
    fn comparisons_on_other_types_are_not_exact() {
        assert!(!exact(col("s").eq(lit("x"))));
        assert!(!exact(col("e").lt(lit("y"))));
        assert!(!exact(col("uuid").eq(lit("x"))));
        assert!(!exact(col("f").gt(lit(1.0f64))));
        assert!(!exact(
            col("h").eq(lit(ScalarValue::Decimal256(None, 39, 0)))
        ));
        assert!(!exact(
            col("ts_ns").eq(lit(ScalarValue::TimestampNanosecond(Some(1), None)))
        ));
        assert!(!exact(col("tstz").gt(lit(
            ScalarValue::TimestampMicrosecond(Some(1), Some("UTC".into()))
        ))));
        assert!(!exact(col("no_metadata").eq(lit(1i32))));
    }

    #[test]
    fn literal_must_have_the_column_type() {
        assert!(!exact(col("i").eq(lit(1i64))));
        assert!(!exact(
            col("ts").eq(lit(ScalarValue::TimestampMillisecond(Some(1), None)))
        ));
    }

    #[test]
    fn only_column_to_literal_comparisons_are_exact() {
        assert!(!exact(col("i").eq(col("i"))));
        assert!(!exact(lit(1i32).eq(lit(1i32))));
        assert!(!exact((col("i") + lit(1i32)).gt(lit(2i32))));
        assert!(!exact(cast(col("i"), DataType::Int64).eq(lit(1i64))));
        assert!(!exact(col("s").like(lit("a%"))));
        assert!(!exact(col("missing").eq(lit(1i32))));
    }

    #[test]
    fn null_checks_are_exact_on_any_column() {
        for c in ["i", "s", "f", "tstz", "no_metadata"] {
            assert!(exact(col(c).is_null()), "{c}");
            assert!(exact(col(c).is_not_null()), "{c}");
        }
        assert!(!exact((col("i") + lit(1i32)).is_null()));
    }

    #[test]
    fn in_lists_follow_the_comparison_rules() {
        assert!(exact(col("i").in_list(vec![lit(1i32), lit(2i32)], false)));
        assert!(exact(col("i").in_list(vec![lit(1i32)], true)));
        assert!(!exact(col("i").in_list(vec![lit(1i32), col("i")], false)));
        assert!(!exact(col("s").in_list(vec![lit("a")], false)));
    }

    #[test]
    fn boolean_combinations_require_every_part() {
        let good = col("i").gt(lit(1i32));
        let bad = col("f").gt(lit(1.0f64));
        assert!(exact(good.clone().and(col("s").is_null())));
        assert!(exact(not(good.clone().or(col("b").eq(lit(false))))));
        assert!(!exact(good.clone().and(bad.clone())));
        assert!(!exact(good.or(bad.clone())));
        assert!(!exact(not(bad)));
        assert!(exact(col("b").or(not(col("b")))));
        assert!(!exact(col("i")));
    }

    #[test]
    fn exact_filters_render_with_the_duckdb_dialect() {
        let dialect = DuckDBDialect::new();
        let sql = Unparser::new(&dialect)
            .expr_to_sql(&col("d").eq(lit(ScalarValue::Date32(Some(19723)))))
            .unwrap()
            .to_string();
        assert_eq!(sql, r#"("d" = CAST('2024-01-01' AS DATE))"#);
    }

    mod exec {
        use datafusion::physical_expr::expressions::{
            lit as physical_lit, BinaryExpr as PhysicalBinaryExpr,
        };
        use datafusion::physical_plan::filter_pushdown::{ChildFilterPushdownResult, PushedDown};
        use datafusion::physical_plan::PhysicalExpr;
        use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::DbConnection;
        use datafusion_table_providers_common::sql::db_connection_pool::JoinPushDown;
        use datafusion_table_providers_common::sql::sql_provider_datafusion::SqlExec;

        use super::*;

        struct NoPool;

        #[async_trait]
        impl DbConnectionPool<QuackSession, ()> for NoPool {
            async fn connect(
                &self,
            ) -> Result<
                Box<dyn DbConnection<QuackSession, ()>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err("no server in unit tests".into())
            }

            fn join_push_down(&self) -> JoinPushDown {
                JoinPushDown::Disallow
            }
        }

        fn quack_exec() -> QuackSqlExec {
            let schema = Arc::new(schema());
            let sql = r#"SELECT "i", "f" FROM "t""#.to_string();
            let projection = vec![schema.index_of("i").unwrap(), schema.index_of("f").unwrap()];
            let inner = SqlExec::new(
                Some(&projection),
                &schema,
                Arc::new(NoPool),
                sql,
                Arc::new(DuckDBDialect::new()),
            )
            .unwrap();
            QuackSqlExec::new(Arc::new(inner), false)
        }

        fn sql(plan: &dyn ExecutionPlan) -> String {
            datafusion::physical_plan::displayable(plan)
                .one_line()
                .to_string()
        }

        fn sort_on(exec: &QuackSqlExec, name: &str) -> Vec<PhysicalSortExpr> {
            let index = exec.schema().index_of(name).unwrap();
            vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
                name, index,
            )))]
        }

        #[test]
        fn refuses_parent_filters_that_sql_exec_would_accept() {
            let exec = quack_exec();
            let filter: Arc<dyn PhysicalExpr> = Arc::new(PhysicalBinaryExpr::new(
                Arc::new(Column::new("f", 1)),
                Operator::Gt,
                physical_lit(1.0f64),
            ));
            let child = ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter: Arc::clone(&filter),
                    child_results: vec![],
                }],
                self_filters: vec![],
            };
            // The wrapped SqlExec would splice this float filter into its SQL.
            let accepted = exec
                .inner
                .handle_child_pushdown_result(
                    FilterPushdownPhase::Pre,
                    child.clone(),
                    &ConfigOptions::default(),
                )
                .unwrap();
            assert!(matches!(accepted.filters[..], [PushedDown::Yes]));

            let result = exec
                .handle_child_pushdown_result(
                    FilterPushdownPhase::Pre,
                    child,
                    &ConfigOptions::default(),
                )
                .unwrap();
            assert!(matches!(result.filters[..], [PushedDown::No]));
            assert!(result.updated_node.is_none());
        }

        #[test]
        fn pushes_sort_only_on_types_ordered_like_datafusion() {
            let exec = quack_exec();
            let SortOrderPushdownResult::Inexact { inner } =
                exec.try_pushdown_sort(&sort_on(&exec, "i")).unwrap()
            else {
                panic!("expected the INTEGER sort to be pushed down");
            };
            assert_eq!(inner.name(), "QuackSqlExec");
            let sorted_sql = sql(inner.as_ref());
            assert!(
                sorted_sql.contains(r#"ORDER BY "i" ASC NULLS FIRST"#),
                "{sorted_sql}"
            );

            assert!(matches!(
                exec.try_pushdown_sort(&sort_on(&exec, "f")).unwrap(),
                SortOrderPushdownResult::Unsupported
            ));

            // A second push would append another ORDER BY; it's refused, and a limit pushed
            // afterwards keeps the flag.
            assert!(matches!(
                inner.try_pushdown_sort(&sort_on(&exec, "i")).unwrap(),
                SortOrderPushdownResult::Unsupported
            ));
            let limited = inner.with_fetch(Some(2)).unwrap();
            assert!(matches!(
                limited.try_pushdown_sort(&sort_on(&exec, "i")).unwrap(),
                SortOrderPushdownResult::Unsupported
            ));
        }

        #[test]
        fn limit_pushdown_keeps_the_wrapper() {
            let exec = quack_exec();
            let limited = exec.with_fetch(Some(5)).unwrap();
            assert_eq!(limited.name(), "QuackSqlExec");
            assert_eq!(limited.fetch(), Some(5));
            let limited_sql = sql(limited.as_ref());
            assert!(limited_sql.trim_end().ends_with("LIMIT 5"), "{limited_sql}");
        }
    }
}
