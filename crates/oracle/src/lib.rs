use std::sync::Arc;

use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use secrecy::SecretString;
use snafu::prelude::*;
use std::collections::HashMap;

use crate::pool::OracleConnectionPool;
use crate::sql_table::OracleTable;

pub mod arrow_sql_gen;
pub mod conn;
#[cfg(feature = "federation")]
pub mod federation;
pub mod pool;
pub mod sql_table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source:
            datafusion_table_providers_common::sql::db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to create Oracle connection pool: {source}"))]
    UnableToCreateConnectionPool { source: pool::Error },

    #[snafu(display("Unable to create table provider: {source}"))]
    UnableToCreateTableProvider {
        source: datafusion_table_providers_common::sql::sql_provider_datafusion::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Factory for creating Oracle table providers from an existing connection pool.
pub struct OracleTableFactory {
    pool: Arc<OracleConnectionPool>,
}

impl OracleTableFactory {
    #[must_use]
    pub fn new(pool: Arc<OracleConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: impl Into<TableReference>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let table = Arc::new(
            OracleTable::new(&self.pool, table_reference)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        #[cfg(feature = "federation")]
        let table = Arc::new(
            table
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table)
    }
}

#[derive(Debug)]
pub struct OracleTableProviderFactory {}

impl OracleTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for OracleTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TableProviderFactory for OracleTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let options = &cmd.options;

        // Construct params from options
        let mut params: HashMap<String, SecretString> = HashMap::new();
        for (k, v) in options {
            params.insert(k.clone(), SecretString::from(v.clone()));
        }

        let pool = OracleConnectionPool::new(params)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let factory = OracleTableFactory::new(Arc::new(pool));

        let table = factory
            .table_provider(TableReference::from(name))
            .await
            .map_err(DataFusionError::External)?;

        Ok(table)
    }
}
