use crate::mongodb::connection_pool::MongoDBConnectionPool;
use crate::mongodb::utils::expression::{combine_exprs_with_and, expr_to_mongo_filter};
use crate::mongodb::Error;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::project_schema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use mongodb::bson::Document;
use serde_json;
use std::{any::Any, fmt, sync::Arc};

#[derive(Debug)]
pub struct MongoDBTable {
    pool: Arc<MongoDBConnectionPool>,
    schema: SchemaRef,
    table_reference: Arc<TableReference>,
}

impl MongoDBTable {
    pub async fn new(
        pool: &Arc<MongoDBConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, Error> {
        let table_reference = table_reference.into();
        let schema = pool.connect().await?.get_schema(&table_reference).await?;

        Ok(Self {
            pool: Arc::clone(pool),
            schema,
            table_reference: Arc::new(table_reference),
        })
    }
}

#[async_trait]
impl TableProvider for MongoDBTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MongoDBExec::new(
            Arc::clone(&self.table_reference),
            Arc::clone(&self.pool),
            Arc::clone(&self.schema),
            projection,
            filters,
            limit,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        supports_filters_pushdown(filters)
    }
}

fn supports_filters_pushdown(
    filters: &[&Expr],
) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
    let filter_push_down: Vec<TableProviderFilterPushDown> = filters
        .iter()
        .map(|f| match expr_to_mongo_filter(f) {
            Some(_) => TableProviderFilterPushDown::Exact,
            None => TableProviderFilterPushDown::Unsupported,
        })
        .collect();

    Ok(filter_push_down)
}

#[derive(Debug)]
struct MongoDBExec {
    table_reference: Arc<TableReference>,
    pool: Arc<MongoDBConnectionPool>,
    projected_schema: SchemaRef,
    filters_doc: Document,
    sort_doc: Document,
    limit: Option<i32>,
    properties: PlanProperties,
}

impl MongoDBExec {
    pub fn new(
        table_reference: Arc<TableReference>,
        pool: Arc<MongoDBConnectionPool>,
        schema: SchemaRef,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let mut projected_schema = project_schema(&schema, projections)?;

        // If no columns are specified, use _id - otherwise mongo returns an error
        if projected_schema.fields.is_empty() {
            let idx = schema.index_of("_id")?;
            projected_schema = SchemaRef::from(schema.project(&[idx])?);
        }

        let limit = limit
            .map(|u| {
                let Ok(u) = u32::try_from(u) else {
                    return Err(DataFusionError::Execution(
                        "Value is too large to fit in a u32".to_string(),
                    ));
                };
                if let Ok(u) = i32::try_from(u) {
                    Ok(u)
                } else {
                    Err(DataFusionError::Execution(
                        "Value is too large to fit in an i32".to_string(),
                    ))
                }
            })
            .transpose()?;

        let combined_exprs = combine_exprs_with_and(filters);

        let mongo_filters_doc = match combined_exprs {
            Some(e) => expr_to_mongo_filter(&e).ok_or(DataFusionError::Execution(
                "Failed to convert expressions".to_string(),
            ))?,
            None => Document::new(),
        };

        Ok(Self {
            table_reference: Arc::clone(&table_reference),
            pool,
            projected_schema: Arc::clone(&projected_schema),
            filters_doc: mongo_filters_doc,
            sort_doc: Document::new(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        })
    }
}

impl DisplayAs for MongoDBExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();

        let filters = serde_json::to_string(&self.filters_doc).map_err(|_| fmt::Error)?;

        write!(
            f,
            "MongoDBExec projection=[{}] filters=[{}]",
            columns.join(", "),
            filters,
        )?;

        if !self.sort_doc.is_empty() {
            let sort = serde_json::to_string(&self.sort_doc).map_err(|_| fmt::Error)?;
            write!(f, " sort=[{sort}]")?;
        }

        if let Some(limit) = self.limit {
            write!(f, " limit=[{limit}]")?;
        }

        Ok(())
    }
}

impl ExecutionPlan for MongoDBExec {
    fn name(&self) -> &'static str {
        "MongoDBExec"
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

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fetch(&self) -> Option<usize> {
        self.limit.and_then(|l| usize::try_from(l).ok())
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new_limit = match limit {
            Some(u) => Some(i32::try_from(u).ok()?),
            None => None,
        };

        Some(Arc::new(MongoDBExec {
            table_reference: Arc::clone(&self.table_reference),
            pool: Arc::clone(&self.pool),
            projected_schema: Arc::clone(&self.projected_schema),
            filters_doc: self.filters_doc.clone(),
            sort_doc: self.sort_doc.clone(),
            limit: new_limit,
            properties: self.properties.clone(),
        }))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DataFusionResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        use datafusion::physical_expr::expressions::Column;

        let mut sort_doc = Document::new();
        for sort_expr in order {
            let Some(col) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
                // Can only push down simple column references
                return Ok(SortOrderPushdownResult::Unsupported);
            };
            let direction = if sort_expr.options.descending { -1 } else { 1 };
            sort_doc.insert(col.name().to_string(), direction);
        }

        let mut new_exec = MongoDBExec {
            table_reference: Arc::clone(&self.table_reference),
            pool: Arc::clone(&self.pool),
            projected_schema: Arc::clone(&self.projected_schema),
            filters_doc: self.filters_doc.clone(),
            sort_doc,
            limit: self.limit,
            properties: self.properties.clone(),
        };

        // Update equivalence properties to reflect the output ordering
        let eq_properties = EquivalenceProperties::new_with_orderings(
            Arc::clone(&self.projected_schema),
            vec![order.to_vec()],
        );
        new_exec.properties = new_exec.properties.with_eq_properties(eq_properties);

        // Return Inexact rather than Exact so DataFusion keeps the SortExec wrapper
        // above us. Exact would replace the SortExec with `inner`, which loses the
        // SortExec's embedded fetch (`ORDER BY ... LIMIT N` is represented as a
        // single SortExec with fetch=N). Keeping the SortExec preserves the fetch
        // as a TopK applied to our already-sorted MongoDB output — correct, though
        // currently missing the ability to push the fetch all the way down into the
        // MongoDB query itself. DataFusion 52 does not pass the fetch to
        // try_pushdown_sort; once that's fixed upstream, we can switch to Exact and
        // absorb the limit directly.
        Ok(SortOrderPushdownResult::Inexact {
            inner: Arc::new(new_exec),
        })
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = self.schema();

        let table_reference = Arc::clone(&self.table_reference);
        let pool = Arc::clone(&self.pool);
        let projected_schema = Arc::clone(&self.projected_schema);
        let filters_doc = self.filters_doc.clone();
        let sort_doc = self.sort_doc.clone();
        let limit = self.limit;

        let stream = futures::stream::once(async move {
            let conn = pool.connect().await.map_err(to_execution_error)?;

            conn.query_arrow(
                &table_reference,
                &projected_schema,
                &filters_doc,
                limit,
                &sort_doc,
            )
            .await
            .map_err(to_execution_error)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit, BinaryExpr, Expr, Operator};
    use mongodb::bson::doc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    /// Helper to get the DisplayAs output from a MongoDBExec.
    fn format_exec(exec: &MongoDBExec) -> String {
        struct Wrapper<'a>(&'a MongoDBExec);
        impl fmt::Display for Wrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_as(DisplayFormatType::Default, f)
            }
        }
        format!("{}", Wrapper(exec))
    }

    fn stub_pool() -> Arc<MongoDBConnectionPool> {
        Arc::new(MongoDBConnectionPool::new_stub())
    }

    // --- supports_filters_pushdown ---

    #[tokio::test]
    async fn test_supports_filters_pushdown_supported() {
        let expr = col("foo").eq(lit(42));
        let res = supports_filters_pushdown(&[&expr]).unwrap();
        assert_eq!(res, vec![TableProviderFilterPushDown::Exact]);
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_unsupported() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("foo")),
            op: Operator::Modulo,
            right: Box::new(lit("bar")),
        });
        let res = supports_filters_pushdown(&[&expr]).unwrap();
        assert_eq!(res, vec![TableProviderFilterPushDown::Unsupported]);
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_mixed() {
        let exprs = vec![
            col("foo").eq(lit(10)),
            col("bar").not_eq(lit("baz")),
            col("foo").is_null(),
        ];
        let refs: Vec<&Expr> = exprs.iter().collect();
        let res = supports_filters_pushdown(&refs).unwrap();
        assert_eq!(
            res,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_all_new_types() {
        let exprs = vec![
            col("x").is_null(),
            col("x").is_not_null(),
            col("x").is_true(),
            col("x").is_false(),
            col("x").is_not_true(),
            col("x").is_not_false(),
            col("x").between(lit(1), lit(10)),
            col("x").not_between(lit(1), lit(10)),
            col("x").in_list(vec![lit(1), lit(2)], false),
            col("x").in_list(vec![lit(1), lit(2)], true),
            col("x").like(lit("%foo%")),
            col("x").not_like(lit("bar%")),
            col("x").ilike(lit("%baz")),
            col("x").not_ilike(lit("qux%")),
            Expr::Not(Box::new(col("x").eq(lit(5)))),
        ];
        let refs: Vec<&Expr> = exprs.iter().collect();
        let res = supports_filters_pushdown(&refs).unwrap();
        assert!(
            res.iter().all(|r| *r == TableProviderFilterPushDown::Exact),
            "All new expression types should be Exact, got: {res:?}"
        );
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_empty() {
        let res = supports_filters_pushdown(&[]).unwrap();
        assert!(res.is_empty());
    }

    // --- DisplayAs / explain plan ---

    #[tokio::test]
    async fn test_display_no_filters_no_limit() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let display = format_exec(&exec);
        assert!(
            display.contains("MongoDBExec projection=[_id, name, age, active]"),
            "Should show all columns: {display}"
        );
        assert!(display.contains("filters=[{}]"), "No filters: {display}");
        assert!(
            !display.contains("sort="),
            "No sort shown when empty: {display}"
        );
        assert!(
            !display.contains("limit="),
            "No limit shown when None: {display}"
        );
    }

    #[tokio::test]
    async fn test_display_with_projection() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec =
            MongoDBExec::new(table_ref, stub_pool(), schema, Some(&vec![1, 2]), &[], None).unwrap();

        let display = format_exec(&exec);
        assert!(
            display.contains("projection=[name, age]"),
            "Should show projected columns: {display}"
        );
    }

    #[tokio::test]
    async fn test_display_with_filters() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![col("age").gt(lit(21))];
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &filters, None).unwrap();

        let display = format_exec(&exec);
        assert!(
            display.contains(r#""age":{"$gt":21}"#),
            "Should show filter doc: {display}"
        );
    }

    #[tokio::test]
    async fn test_display_with_limit() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], Some(100)).unwrap();

        let display = format_exec(&exec);
        assert!(
            display.contains("limit=[100]"),
            "Should show limit: {display}"
        );
    }

    #[tokio::test]
    async fn test_display_with_sort() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let mut exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();
        exec.sort_doc = doc! { "name": 1, "age": -1 };

        let display = format_exec(&exec);
        assert!(
            display.contains("sort=["),
            "Should show sort section: {display}"
        );
        assert!(
            display.contains(r#""name":1"#),
            "Should show sort fields: {display}"
        );
        assert!(
            display.contains(r#""age":-1"#),
            "Should show sort fields: {display}"
        );
    }

    #[tokio::test]
    async fn test_display_with_all_options() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![col("active").eq(lit(true))];
        let mut exec = MongoDBExec::new(
            table_ref,
            stub_pool(),
            schema,
            Some(&vec![1, 3]),
            &filters,
            Some(50),
        )
        .unwrap();
        exec.sort_doc = doc! { "name": 1 };

        let display = format_exec(&exec);
        assert!(
            display.contains("projection=[name, active]"),
            "projection: {display}"
        );
        assert!(display.contains(r#""active":true"#), "filter: {display}");
        assert!(display.contains("sort=["), "sort: {display}");
        assert!(display.contains("limit=[50]"), "limit: {display}");
    }

    #[tokio::test]
    async fn test_display_complex_filter() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![col("age").gt(lit(18)).and(col("name").eq(lit("Alice")))];
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &filters, None).unwrap();

        let display = format_exec(&exec);
        assert!(
            display.contains("$and"),
            "Should show AND filter: {display}"
        );
        assert!(
            display.contains(r#""age":{"$gt":18}"#),
            "Should show age filter: {display}"
        );
        assert!(
            display.contains(r#""name":"Alice""#),
            "Should show name filter: {display}"
        );
    }

    // --- MongoDBExec edge cases ---

    #[tokio::test]
    async fn test_exec_empty_projection_falls_back_to_id() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec =
            MongoDBExec::new(table_ref, stub_pool(), schema, Some(&vec![]), &[], None).unwrap();

        let display = format_exec(&exec);
        assert!(
            display.contains("projection=[_id]"),
            "Empty projection should fall back to _id: {display}"
        );
    }

    #[tokio::test]
    async fn test_exec_limit_too_large() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let result = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], Some(usize::MAX));
        assert!(result.is_err(), "Should fail for limit that exceeds i32");
    }

    #[tokio::test]
    async fn test_exec_no_filters_produces_empty_doc() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        assert!(
            exec.filters_doc.is_empty(),
            "No filters should produce empty doc"
        );
    }

    #[tokio::test]
    async fn test_exec_multiple_filters_combined() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![col("age").gt(lit(18)), col("active").eq(lit(true))];
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &filters, None).unwrap();

        assert!(
            exec.filters_doc.contains_key("$and"),
            "Multiple filters should be combined with $and: {:?}",
            exec.filters_doc
        );
    }

    #[tokio::test]
    async fn test_exec_properties() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        assert_eq!(exec.name(), "MongoDBExec");
        assert_eq!(exec.children().len(), 0);
        assert!(
            matches!(
                exec.properties().partitioning,
                Partitioning::UnknownPartitioning(1)
            ),
            "Expected UnknownPartitioning(1)"
        );
    }

    #[tokio::test]
    async fn test_exec_unconvertible_combined_filter_errors() {
        // Two filters where AND combines them, but the combined expr can't be converted
        // (e.g., one is a Modulo that passes combine_exprs_with_and but fails expr_to_mongo_filter)
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("age")),
            op: Operator::Modulo,
            right: Box::new(lit(2)),
        })];
        let result = MongoDBExec::new(table_ref, stub_pool(), schema, None, &filters, None);
        assert!(
            result.is_err(),
            "Should error when combined filter can't be converted"
        );
    }

    #[tokio::test]
    async fn test_exec_with_new_children_returns_self() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();
        let exec_arc: Arc<dyn ExecutionPlan> = Arc::new(exec);
        let result = exec_arc.clone().with_new_children(vec![]).unwrap();
        assert_eq!(result.name(), "MongoDBExec");
    }

    // --- try_pushdown_sort ---

    #[tokio::test]
    async fn test_sort_pushdown_single_column_asc() {
        use datafusion::physical_expr::expressions::Column as PhysColumn;

        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let sort_exprs = vec![PhysicalSortExpr::new(
            Arc::new(PhysColumn::new("name", 1)),
            datafusion::arrow::compute::SortOptions {
                descending: false,
                nulls_first: true,
            },
        )];

        let result = exec.try_pushdown_sort(&sort_exprs).unwrap();
        match result {
            SortOrderPushdownResult::Inexact { inner } => {
                let mongo_exec = inner.as_any().downcast_ref::<MongoDBExec>().unwrap();
                assert_eq!(mongo_exec.sort_doc, doc! { "name": 1 });
                let display = format_exec(mongo_exec);
                assert!(
                    display.contains("sort=["),
                    "Display should show sort: {display}"
                );
            }
            other => panic!("Expected Inexact, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_sort_pushdown_single_column_desc() {
        use datafusion::physical_expr::expressions::Column as PhysColumn;

        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let sort_exprs = vec![PhysicalSortExpr::new(
            Arc::new(PhysColumn::new("age", 2)),
            datafusion::arrow::compute::SortOptions {
                descending: true,
                nulls_first: false,
            },
        )];

        let result = exec.try_pushdown_sort(&sort_exprs).unwrap();
        match result {
            SortOrderPushdownResult::Inexact { inner } => {
                let mongo_exec = inner.as_any().downcast_ref::<MongoDBExec>().unwrap();
                assert_eq!(mongo_exec.sort_doc, doc! { "age": -1 });
            }
            other => panic!("Expected Inexact, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_sort_pushdown_multiple_columns() {
        use datafusion::physical_expr::expressions::Column as PhysColumn;

        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let sort_exprs = vec![
            PhysicalSortExpr::new(
                Arc::new(PhysColumn::new("name", 1)),
                datafusion::arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ),
            PhysicalSortExpr::new(
                Arc::new(PhysColumn::new("age", 2)),
                datafusion::arrow::compute::SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];

        let result = exec.try_pushdown_sort(&sort_exprs).unwrap();
        match result {
            SortOrderPushdownResult::Inexact { inner } => {
                let mongo_exec = inner.as_any().downcast_ref::<MongoDBExec>().unwrap();
                assert_eq!(mongo_exec.sort_doc, doc! { "name": 1, "age": -1 });
            }
            other => panic!("Expected Inexact, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_sort_pushdown_non_column_returns_unsupported() {
        use datafusion::physical_expr::expressions::Literal;
        use datafusion::scalar::ScalarValue;

        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let sort_exprs = vec![PhysicalSortExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            datafusion::arrow::compute::SortOptions::default(),
        )];

        let result = exec.try_pushdown_sort(&sort_exprs).unwrap();
        assert!(
            matches!(result, SortOrderPushdownResult::Unsupported),
            "Non-column sort should be Unsupported"
        );
    }

    #[tokio::test]
    async fn test_sort_pushdown_preserves_filters_and_limit() {
        use datafusion::physical_expr::expressions::Column as PhysColumn;

        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![col("age").gt(lit(21))];
        let exec =
            MongoDBExec::new(table_ref, stub_pool(), schema, None, &filters, Some(10)).unwrap();

        let sort_exprs = vec![PhysicalSortExpr::new(
            Arc::new(PhysColumn::new("name", 1)),
            datafusion::arrow::compute::SortOptions::default(),
        )];

        let result = exec.try_pushdown_sort(&sort_exprs).unwrap();
        match result {
            SortOrderPushdownResult::Inexact { inner } => {
                let mongo_exec = inner.as_any().downcast_ref::<MongoDBExec>().unwrap();
                assert!(
                    !mongo_exec.filters_doc.is_empty(),
                    "Filters should be preserved"
                );
                assert_eq!(mongo_exec.limit, Some(10), "Limit should be preserved");
                assert_eq!(mongo_exec.sort_doc, doc! { "name": 1 });
            }
            other => panic!("Expected Inexact, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_sort_pushdown_empty_order() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let result = exec.try_pushdown_sort(&[]).unwrap();
        match result {
            SortOrderPushdownResult::Inexact { inner } => {
                let mongo_exec = inner.as_any().downcast_ref::<MongoDBExec>().unwrap();
                assert!(
                    mongo_exec.sort_doc.is_empty(),
                    "Empty sort should produce empty doc"
                );
            }
            other => panic!("Expected Inexact, got: {other:?}"),
        }
    }

    // --- Limit pushdown (with_fetch) ---

    #[tokio::test]
    async fn test_supports_limit_pushdown() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();
        assert!(
            exec.supports_limit_pushdown(),
            "MongoDBExec should declare limit pushdown support"
        );
    }

    #[tokio::test]
    async fn test_fetch_reports_current_limit() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec_unlimited =
            MongoDBExec::new(Arc::clone(&table_ref), stub_pool(), Arc::clone(&schema), None, &[], None).unwrap();
        assert_eq!(exec_unlimited.fetch(), None);

        let exec_limited =
            MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], Some(42)).unwrap();
        assert_eq!(exec_limited.fetch(), Some(42));
    }

    #[tokio::test]
    async fn test_with_fetch_sets_limit() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        let with_fetch = exec.with_fetch(Some(5)).expect("should produce new plan");
        let mongo_exec = with_fetch.as_any().downcast_ref::<MongoDBExec>().unwrap();
        assert_eq!(mongo_exec.limit, Some(5));
        let display = format_exec(mongo_exec);
        assert!(
            display.contains("limit=[5]"),
            "Display should show pushed-down limit: {display}"
        );
    }

    #[tokio::test]
    async fn test_with_fetch_clears_limit() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], Some(100)).unwrap();

        let with_fetch = exec.with_fetch(None).expect("should produce new plan");
        let mongo_exec = with_fetch.as_any().downcast_ref::<MongoDBExec>().unwrap();
        assert_eq!(mongo_exec.limit, None);
    }

    #[tokio::test]
    async fn test_with_fetch_preserves_sort_and_filters() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let filters = vec![col("age").gt(lit(21))];
        let mut exec =
            MongoDBExec::new(table_ref, stub_pool(), schema, None, &filters, None).unwrap();
        exec.sort_doc = doc! { "name": 1 };

        let with_fetch = exec.with_fetch(Some(10)).expect("should produce new plan");
        let mongo_exec = with_fetch.as_any().downcast_ref::<MongoDBExec>().unwrap();
        assert_eq!(mongo_exec.limit, Some(10));
        assert!(!mongo_exec.filters_doc.is_empty(), "filters preserved");
        assert_eq!(mongo_exec.sort_doc, doc! { "name": 1 }, "sort preserved");
    }

    #[tokio::test]
    async fn test_with_fetch_too_large_returns_none() {
        let schema = test_schema();
        let table_ref = Arc::new(TableReference::bare("users"));
        let exec = MongoDBExec::new(table_ref, stub_pool(), schema, None, &[], None).unwrap();

        // usize::MAX won't fit in i32
        assert!(
            exec.with_fetch(Some(usize::MAX)).is_none(),
            "Limit exceeding i32::MAX should return None"
        );
    }
}
