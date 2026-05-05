//! Utility functions to enable federation support for scalar functions.
//!
//! Helpful for implementing [`SQLExecutor::can_execute_plan`], when federating Datafusion Scalar UDFs with a `SQLExecutor` that may not support them.

use std::sync::Arc;

use datafusion::{
    catalog::Session,
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::DataFusionError,
    logical_expr::{
        expr::{AggregateFunction, ScalarFunction},
        AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowFunctionDefinition, WindowUDF,
    },
};

/// Returns whether any [`ScalarFunction`], [`AggregateFunction`] or [`WindowFunction`]s in the [`LogicalPlan`] are unsupported.
///
/// # Arguments
/// * `plan` - The logical plan to check for scalar functions
/// * `supports` - The support policy (allow-list or deny-list)
///
/// # Returns
/// * `Ok(true)` if there are unsupported scalar functions in the plan
/// * `Ok(false)` if all scalar functions are supported
/// * `Err(DataFusionError)` if an error occurs during traversal
pub fn contains_unsupported_functions(
    plan: &LogicalPlan,
    sup: &FunctionSupport,
) -> Result<bool, DataFusionError> {
    let mut found_unsupported = false;
    plan.apply_with_subqueries(|plan| {
        for expr in plan.expressions() {
            expr.apply(|expr| {
                if sup.supports(expr) {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    found_unsupported = true;
                    Ok(TreeNodeRecursion::Stop)
                }
            })?;
            if found_unsupported {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found_unsupported)
}

#[derive(Clone, Debug)]
pub struct FunctionSupport {
    scalar: Option<FunctionRestriction>,
    window: Option<FunctionRestriction>,
    aggregate: Option<FunctionRestriction>,
}

impl FunctionSupport {
    pub fn new(
        scalar: Option<FunctionRestriction>,
        window: Option<FunctionRestriction>,
        aggregate: Option<FunctionRestriction>,
    ) -> Self {
        Self {
            scalar,
            window,
            aggregate,
        }
    }

    pub fn deny_all_from(sess: &dyn Session) -> Self {
        let scalar = Some(FunctionRestriction::Deny(
            sess.scalar_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let window = Some(FunctionRestriction::Deny(
            sess.window_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let aggregate = Some(FunctionRestriction::Deny(
            sess.aggregate_functions()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        ));

        FunctionSupport {
            scalar,
            window,
            aggregate,
        }
    }

    pub fn support_all_from(sess: &dyn Session) -> Self {
        let scalar = Some(FunctionRestriction::Allow(
            sess.scalar_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let window = Some(FunctionRestriction::Allow(
            sess.window_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let aggregate = Some(FunctionRestriction::Allow(
            sess.aggregate_functions()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        ));

        FunctionSupport {
            scalar,
            window,
            aggregate,
        }
    }

    pub fn supports(&self, expr: &Expr) -> bool {
        let mut supports = true;
        let _ = expr.apply(|e| {
            let support_child = match e {
                Expr::ScalarFunction(ScalarFunction { func, .. }) => self.supports_scalar(func),
                Expr::AggregateFunction(AggregateFunction { func, .. }) => {
                    self.supports_aggregate(func)
                }
                Expr::WindowFunction(wind) => match &wind.fun {
                    WindowFunctionDefinition::AggregateUDF(func) => self.supports_aggregate(func),
                    WindowFunctionDefinition::WindowUDF(func) => self.supports_window(func),
                },
                _ => true,
            };
            if !support_child {
                supports = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        supports
    }

    pub fn supports_window(&self, fnc: &Arc<WindowUDF>) -> bool {
        self.window
            .as_ref()
            .map(|restriction| restriction.supports(&fnc.name().to_string()))
            .unwrap_or(true)
    }
    pub fn supports_scalar(&self, fnc: &Arc<ScalarUDF>) -> bool {
        self.scalar
            .as_ref()
            .map(|restriction| restriction.supports(&fnc.name().to_string()))
            .unwrap_or(true)
    }
    pub fn supports_aggregate(&self, fnc: &Arc<AggregateUDF>) -> bool {
        self.aggregate
            .as_ref()
            .map(|restriction| restriction.supports(&fnc.name().to_string()))
            .unwrap_or(true)
    }
}

#[derive(Clone, Debug)]
pub enum FunctionRestriction {
    Allow(Vec<String>),
    Deny(Vec<String>),
}

impl FunctionRestriction {
    fn supports(&self, name: &String) -> bool {
        match self {
            Self::Allow(allowed) => allowed.contains(name),
            Self::Deny(denied) => !denied.contains(name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{create_udf, ColumnarValue, LogicalPlanBuilder, Subquery};
    use datafusion::prelude::col;
    use std::sync::Arc;

    fn stub_udf(name: &str) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    fn deny_support(names: &[&str]) -> FunctionSupport {
        FunctionSupport::new(
            Some(FunctionRestriction::Deny(
                names.iter().map(|s| s.to_string()).collect(),
            )),
            None,
            None,
        )
    }

    fn scan_plan(table: &str) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let source = Arc::new(LogicalTableSource::new(schema))
            as Arc<dyn datafusion::logical_expr::TableSource>;
        LogicalPlanBuilder::scan(table, source, None)
            .expect("scan")
            .build()
            .expect("build")
    }

    #[test]
    fn detects_denied_function_in_top_level_projection() {
        let udf = stub_udf("denied_fn");
        let plan = LogicalPlanBuilder::from(scan_plan("t"))
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![col("val")],
            ))])
            .expect("project")
            .build()
            .expect("build");

        let sup = deny_support(&["denied_fn"]);
        assert!(
            contains_unsupported_functions(&plan, &sup).expect("check"),
            "should detect denied function in top-level projection"
        );
    }

    #[test]
    fn allows_plan_without_denied_functions() {
        let udf = stub_udf("allowed_fn");
        let plan = LogicalPlanBuilder::from(scan_plan("t"))
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![col("val")],
            ))])
            .expect("project")
            .build()
            .expect("build");

        let sup = deny_support(&["denied_fn"]);
        assert!(
            !contains_unsupported_functions(&plan, &sup).expect("check"),
            "should allow plan with only non-denied functions"
        );
    }

    #[test]
    fn detects_denied_function_inside_in_subquery() {
        let udf = stub_udf("denied_fn");

        // Build subquery: SELECT denied_fn(val) FROM inner_t
        let subquery_plan = LogicalPlanBuilder::from(scan_plan("inner_t"))
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![col("val")],
            ))
            .alias("result")])
            .expect("project")
            .build()
            .expect("build");

        // Build outer: SELECT id FROM t WHERE id IN (subquery)
        let outer = LogicalPlanBuilder::from(scan_plan("t"))
            .filter(Expr::InSubquery(
                datafusion::logical_expr::expr::InSubquery::new(
                    Box::new(col("id")),
                    Subquery {
                        subquery: Arc::new(subquery_plan),
                        outer_ref_columns: vec![],
                        spans: Default::default(),
                    },
                    false,
                ),
            ))
            .expect("filter")
            .build()
            .expect("build");

        let sup = deny_support(&["denied_fn"]);
        assert!(
            contains_unsupported_functions(&outer, &sup).expect("check"),
            "should detect denied function inside IN subquery"
        );
    }

    #[test]
    fn detects_denied_function_inside_scalar_subquery() {
        let udf = stub_udf("denied_fn");

        // Build scalar subquery: SELECT denied_fn(val) FROM inner_t
        let subquery_plan = LogicalPlanBuilder::from(scan_plan("inner_t"))
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![col("val")],
            ))
            .alias("result")])
            .expect("project")
            .build()
            .expect("build");

        // Build outer: SELECT id FROM t WHERE id = (scalar subquery)
        let outer = LogicalPlanBuilder::from(scan_plan("t"))
            .filter(col("id").eq(Expr::ScalarSubquery(Subquery {
                subquery: Arc::new(subquery_plan),
                outer_ref_columns: vec![],
                spans: Default::default(),
            })))
            .expect("filter")
            .build()
            .expect("build");

        let sup = deny_support(&["denied_fn"]);
        assert!(
            contains_unsupported_functions(&outer, &sup).expect("check"),
            "should detect denied function inside scalar subquery"
        );
    }

    #[test]
    fn detects_denied_function_inside_exists_subquery() {
        let udf = stub_udf("denied_fn");

        // Build subquery: SELECT denied_fn(val) FROM inner_t
        let subquery_plan = LogicalPlanBuilder::from(scan_plan("inner_t"))
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![col("val")],
            ))
            .alias("result")])
            .expect("project")
            .build()
            .expect("build");

        // Build outer: SELECT id FROM t WHERE EXISTS (subquery)
        let outer = LogicalPlanBuilder::from(scan_plan("t"))
            .filter(Expr::Exists(datafusion::logical_expr::expr::Exists::new(
                Subquery {
                    subquery: Arc::new(subquery_plan),
                    outer_ref_columns: vec![],
                    spans: Default::default(),
                },
                false,
            )))
            .expect("filter")
            .build()
            .expect("build");

        let sup = deny_support(&["denied_fn"]);
        assert!(
            contains_unsupported_functions(&outer, &sup).expect("check"),
            "should detect denied function inside EXISTS subquery"
        );
    }
}
