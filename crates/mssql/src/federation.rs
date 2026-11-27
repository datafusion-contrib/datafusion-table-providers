use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{
    AstAnalyzer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::TryStreamExt;
use snafu::ResultExt;

use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    get_schema, Error as DbError,
};
use datafusion_table_providers_common::sql::db_connection_pool::{DbConnectionPool, JoinPushDown};
use datafusion_table_providers_common::sql::sql_provider_datafusion::{
    get_stream, to_execution_error,
};

use crate::dialect::rewrite_statement;
use crate::sql_table::MsSqlTable;

impl MsSqlTable {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_reference = self.base_table.table_reference.clone();
        let schema = TableProvider::schema(&self.base_table);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_reference),
            schema,
        )))
    }

    /// Wraps the table so that the queries of a federated session are pushed down to SQL Server.
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
fn mssql_ast_analyzer(statement: ast::Statement) -> DataFusionResult<ast::Statement> {
    Ok(rewrite_statement(statement, false))
}

#[async_trait]
impl SQLExecutor for MsSqlTable {
    fn name(&self) -> &str {
        self.base_table.name()
    }

    fn compute_context(&self) -> Option<String> {
        match self.pool.join_push_down() {
            JoinPushDown::AllowedFor(context) => Some(context),
            // A context that never matches another table's, see `SqlTable::compute_context`
            JoinPushDown::Disallow => Some(format!("{}", std::ptr::from_ref(self) as usize)),
        }
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.base_table.dialect_arc()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        Some(Box::new(mssql_ast_analyzer))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
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
