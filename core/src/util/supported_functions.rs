//! Utility functions to enable federation support for scalar functions.
//!
//! Helpful for implementing [`SQLExecutor::can_execute_plan`], when federating Datafusion Scalar UDFs with a `SQLExecutor` that may not support them.

use std::sync::Arc;

use datafusion::{
    common::tree_node::TreeNode,
    error::DataFusionError,
    logical_expr::{expr::ScalarFunction, Expr, LogicalPlan, ScalarUDF},
    prelude::SessionContext,
};

/// Returns whether any [`ScalarFunction`]s in the [`LogicalPlan`] are unsupported.
///
/// # Arguments
/// * `plan` - The logical plan to check for scalar functions
/// * `supports` - The support policy (allow-list or deny-list)
///
/// # Returns
/// * `Ok(true)` if there are unsupported scalar functions in the plan
/// * `Ok(false)` if all scalar functions are supported
/// * `Err(DataFusionError)` if an error occurs during traversal
pub fn unsupported_scalar_functions(
    plan: &LogicalPlan,
    sup: &ScalarFunctionsSupport,
) -> Result<bool, DataFusionError> {
    plan.exists(|plan| {
        Ok(plan.expressions().into_iter().any(|expr| {
            expr.exists(|expr| {
                if let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr {
                    Ok(sup.supports(func))
                } else {
                    Ok(false)
                }
            })
            .unwrap_or(false)
        }))
    })
}

pub enum ScalarFunctionsSupport {
    Allow(Vec<String>),
    Deny(Vec<String>),
}

impl ScalarFunctionsSupport {
    pub fn support_all_from(ctx: &SessionContext) -> Self {
        let names = ctx
            .state()
            .scalar_functions()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ScalarFunctionsSupport::Allow(names)
    }
    fn supports(&self, func: &Arc<ScalarUDF>) -> bool {
        let func_name = func.name().to_string();
        match self {
            ScalarFunctionsSupport::Allow(allowed) => !allowed.contains(&func_name),
            ScalarFunctionsSupport::Deny(denied) => denied.contains(&func_name),
        }
    }
}
