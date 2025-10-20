use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::VisitMut;
use datafusion::sql::unparser::Unparser;
use std::fmt::Display;
use std::sync::Arc;
use std::{any::Any, fmt};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
};

use crate::sql::sql_provider_datafusion::{default_filter_pushdown, SqlExec};
use crate::util::table_arg_replace::TableArgReplace;

use super::{into_table_args, ClickHouseTable};

impl ClickHouseTable {
    fn create_logical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<LogicalPlan> {
        let table_source = LogicalTableSource::new(self.schema.clone());
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
            self.pool.clone(),
            sql,
        )?))
    }
}

#[async_trait]
impl TableProvider for ClickHouseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let filter_push_down = default_filter_pushdown(filters, &*self.dialect);
        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.create_logical_plan(projection, filters, limit)?;
        let mut sql = Unparser::new(&*self.dialect).plan_to_sql(&logical_plan)?;

        if let Some(args) = self.args.clone() {
            let args = into_table_args(args);
            let mut table_args = TableArgReplace::new(vec![(self.table_reference.clone(), args)]);
            let _ = sql.visit(&mut table_args);
        }

        let sql = sql.to_string();
        return self.create_physical_plan(projection, sql);
    }
}

impl Display for ClickHouseTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseTable {}", self.table_reference)
    }
}
