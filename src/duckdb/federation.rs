use crate::sql::db_connection_pool::dbconnection::{get_schema, Error as DbError};
use crate::sql::sql_provider_datafusion::{get_stream, to_execution_error};
use arrow::datatypes::SchemaRef;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use datafusion_federation_sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use futures::TryStreamExt;
use snafu::ResultExt;

use async_trait::async_trait;
use std::sync::Arc;

use super::sql_table::{get_cte, DuckDBTable};
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

impl<T, P> DuckDBTable<T, P> {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = self.base_table.table_reference.to_quoted_string();
        let schema = Arc::clone(&Arc::clone(&self).base_table.schema());
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        )?))
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

#[async_trait]
impl<T, P> SQLExecutor for DuckDBTable<T, P> {
    fn name(&self) -> &str {
        self.base_table.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.base_table.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.base_table.dialect()
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fut = get_stream(
            self.base_table.clone_pool(),
            format!("{cte} {query}", cte = get_cte(&self.table_functions)),
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
