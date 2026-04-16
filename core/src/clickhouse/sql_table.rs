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

        let quote_identifier = |identifier: &str| match dialect.identifier_quote_style(identifier)
        {
            Some(quote) => format!("{quote}{identifier}{quote}"),
            None => identifier.to_string(),
        };

        let columns = projected_schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::Arg;
    use crate::sql::db_connection_pool::{
        clickhousepool::ClickHouseConnectionPool, JoinPushDown,
    };
    use clickhouse::Client;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Constraints;
    use datafusion::sql::TableReference;

    fn new_clickhouse_table(args: Option<Vec<(String, Arg)>>) -> ClickHouseTable {
        let pool = Arc::new(ClickHouseConnectionPool {
            client: Client::default(),
            join_push_down: JoinPushDown::AllowedFor("test".to_string()),
        });

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("revenue", DataType::Decimal128(18, 2), false),
        ]));

        ClickHouseTable::new(
            TableReference::parse_str("companies_param_view"),
            args,
            pool,
            schema,
            Constraints::default(),
        )
    }

    #[test]
    fn test_build_sql_for_parameterized_view_has_no_nul_bytes() {
        let table = new_clickhouse_table(Some(vec![(
            "name".to_string(),
            Arg::String("Gizmo Corp.".to_string()),
        )]));

        let sql = table
            .build_sql(Some(&vec![0, 1, 2]), &[], None)
            .expect("build sql for parameterized view");

        assert!(
            !sql.contains('\0'),
            "ClickHouse SQL should not contain NUL bytes: {sql:?}"
        );
        assert!(
            sql.contains("FROM companies_param_view(name='Gizmo Corp.')"),
            "expected parameterized view expression in SQL: {sql}"
        );
    }

    #[test]
    fn test_build_sql_for_regular_table_has_no_nul_bytes() {
        let table = new_clickhouse_table(None);

        let sql = table
            .build_sql(Some(&vec![0, 2]), &[], Some(10))
            .expect("build sql for regular table");

        assert!(
            !sql.contains('\0'),
            "ClickHouse SQL should not contain NUL bytes: {sql:?}"
        );
        assert!(
            sql.contains("SELECT id") && sql.contains("revenue"),
            "expected projected columns in SQL: {sql}"
        );
    }
}
