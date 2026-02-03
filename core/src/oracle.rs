use crate::sql::db_connection_pool::oraclepool::OracleConnectionPool;
use crate::sql::{db_connection_pool, sql_provider_datafusion::SqlTable};
use async_trait::async_trait;

use datafusion::error::DataFusionError;
use datafusion::{
    catalog::{Session, TableProviderFactory},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use secrecy::SecretString;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "oracle-federation")]
pub mod federation;
pub mod sql_table;
pub mod write;

use self::sql_table::OracleTable;
use crate::sql::db_connection_pool::dbconnection::oracleconn::OraclePooledConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to create Oracle connection pool: {source}"))]
    UnableToCreateConnectionPool {
        source: db_connection_pool::oraclepool::Error,
    },

    #[snafu(display("Unable to create table provider: {source}"))]
    UnableToCreateTableProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool = pool as Arc<
            dyn db_connection_pool::DbConnectionPool<
                    OraclePooledConnection,
                    oracle::sql_type::OracleType,
                > + Send
                + Sync
                + 'static,
        >;

        let table = SqlTable::new("oracle", &dyn_pool, table_reference)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let oracle_table = Arc::new(OracleTable::new(Arc::clone(&self.pool), table));

        #[cfg(feature = "oracle-federation")]
        let oracle_table = Arc::new(
            oracle_table
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(oracle_table)
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

#[async_trait]
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
