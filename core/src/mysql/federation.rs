use crate::sql::db_connection_pool::dbconnection::{get_schema, Error as DbError};
use crate::sql::sql_provider_datafusion::{get_stream, to_execution_error};
use crate::util::supported_functions::unsupported_scalar_functions;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::sql::sqlparser::ast::{self, VisitMut};
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_expr::LogicalPlan;
use datafusion_federation::sql::{
    ast_analyzer::AstAnalyzer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::TryStreamExt;
use snafu::ResultExt;
use std::sync::Arc;

use super::mysql_window::MySQLWindowVisitor;
use super::sql_table::MySQLTable;
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

impl MySQLTable {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = self.base_table.table_reference.clone();
        let schema = Arc::clone(&Arc::clone(&self).base_table.schema());
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_name),
            schema,
        )))
    }

    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[allow(clippy::unnecessary_wraps)]
fn mysql_ast_analyzer(ast: ast::Statement) -> Result<ast::Statement, DataFusionError> {
    match ast {
        ast::Statement::Query(query) => {
            let mut new_query = query.clone();

            let mut window_visitor = MySQLWindowVisitor::default();
            let _ = new_query.visit(&mut window_visitor);

            Ok(ast::Statement::Query(new_query))
        }
        _ => Ok(ast),
    }
}

#[async_trait]
impl SQLExecutor for MySQLTable {
    fn name(&self) -> &str {
        self.base_table.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.base_table.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.base_table.dialect()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        Some(AstAnalyzer::new(vec![Box::new(mysql_ast_analyzer)]))
    }

    fn can_execute_plan(&self, plan: &LogicalPlan) -> bool {
        // Default to not federate if [`Self::scalar_udf_support`] provided, otherwise true.
        self.function_support
            .as_ref()
            .map(|supported_scalar_udfs| {
                !unsupported_scalar_functions(plan, supported_scalar_udfs).unwrap_or(false)
            })
            .unwrap_or(true)
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fut = get_stream(
            self.base_table.clone_pool(),
            query.to_string(),
            Arc::clone(&schema),
        );

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        let conn = self
            .base_table
            .clone_pool()
            .connect()
            .await
            .map_err(to_execution_error)?;
        get_schema(conn, &TableReference::from(table_name))
            .await
            .boxed()
            .map_err(|e| DbError::UnableToGetSchema { source: e })
            .map_err(to_execution_error)
    }
}
