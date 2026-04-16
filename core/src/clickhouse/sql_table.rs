use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::dialect::Dialect;
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

use crate::sql::sql_provider_datafusion::{expr, project_schema_safe, SqlExec};

use super::ClickHouseTable;

impl ClickHouseTable {
    fn build_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<String> {
        let projected_schema = project_schema_safe(&self.schema, projection)?;

        let dialect: &dyn Dialect = &*self.dialect;

        let columns = projected_schema
            .fields()
            .iter()
            .map(|f| {
                let quote = dialect
                    .identifier_quote_style(f.name())
                    .unwrap_or_default()
                    .to_string();
                format!("{quote}{}{quote}", f.name())
            })
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if filters.is_empty() {
            String::new()
        } else {
            let unparser = Unparser::new(dialect);
            let filter_expr = filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|s| s.to_string()))
                .collect::<Result<Vec<String>, _>>()?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };

        // Use table_expr (which handles parameterized views) instead of table_reference
        let table_expr = &self.table_expr;

        Ok(format!(
            "SELECT {columns} FROM {table_expr} {where_expr} {limit_expr}"
        ))
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
        let filter_push_down: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(|f| match expr::to_sql_with_engine(f, None) {
                Ok(_) => TableProviderFilterPushDown::Exact,
                Err(_) => TableProviderFilterPushDown::Unsupported,
            })
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
        let sql = self.build_sql(projection, filters, limit)?;
        let exec = SqlExec::new(
            projection,
            &self.schema(),
            self.pool.clone(),
            sql,
            Arc::clone(&self.dialect),
        )?;

        Ok(Arc::new(exec))
    }
}

impl Display for ClickHouseTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseTable {}", self.table_reference)
    }
}
