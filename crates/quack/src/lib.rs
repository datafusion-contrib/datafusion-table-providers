//! A read-only DataFusion table provider for a remote DuckDB 2.0 (or later) served over
//! DuckDB's Quack protocol.
//!
//! Build a [`pool::QuackConnectionPool`] for a server, then a [`QuackTableFactory`] on it
//! for each table, or register [`QuackTableProviderFactory`] to use
//! `CREATE EXTERNAL TABLE ... STORED AS QUACK`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use datafusion_table_providers_common::sql::sql_provider_datafusion;
use datafusion_table_providers_common::util::remove_prefix_from_hashmap_keys;
use datafusion_table_providers_common::util::secrets::to_secret_map;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use tokio::sync::Mutex;

use crate::pool::QuackConnectionPool;
use crate::sql_table::QuackTable;

pub mod conn;
#[cfg(feature = "federation")]
mod federation;
pub mod pool;
mod sql_table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create the Quack connection pool: {source}"))]
    UnableToCreateConnectionPool { source: pool::Error },

    #[snafu(display("Unable to create the Quack table provider: {source}"))]
    UnableToCreateTableProvider {
        source: sql_provider_datafusion::Error,
    },

    #[cfg(feature = "federation")]
    #[snafu(display("Unable to create the federated Quack table provider: {source}"))]
    UnableToCreateFederatedTableProvider { source: DataFusionError },

    #[snafu(display("Quack tables take their schema from the server. Remove the column list from CREATE EXTERNAL TABLE."))]
    DeclaredSchemaNotSupported,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates table providers for tables that already exist on one Quack server. Every table
/// shares the factory's pool.
///
/// With the `federation` feature (on by default) the providers take part in
/// datafusion-federation: under its optimizer, whole subplans over tables sharing a pool,
/// joins included, run on the server as one DuckDB query.
pub struct QuackTableFactory {
    pool: Arc<QuackConnectionPool>,
}

impl QuackTableFactory {
    #[must_use]
    pub fn new(pool: Arc<QuackConnectionPool>) -> Self {
        Self { pool }
    }

    /// Resolves the table's schema on the server and returns a provider for it.
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist, can't be reached, or has a column type
    /// the pool's unsupported type action rejects.
    pub async fn table_provider(
        &self,
        table_reference: impl Into<TableReference>,
    ) -> Result<Arc<dyn TableProvider + 'static>> {
        let table = Arc::new(
            QuackTable::new(&self.pool, table_reference)
                .await
                .context(UnableToCreateTableProviderSnafu)?,
        );

        #[cfg(feature = "federation")]
        let table = Arc::new(
            table
                .create_federated_table_provider()
                .context(UnableToCreateFederatedTableProviderSnafu)?,
        );

        Ok(table)
    }
}

/// Serves `CREATE EXTERNAL TABLE name STORED AS QUACK LOCATION '<table>' OPTIONS (...)`.
///
/// `LOCATION` names a table (optionally `schema.table` or `catalog.schema.table`) that must
/// already exist on the server; nothing is created there. `OPTIONS` are the
/// [`QuackConnectionPool::new`] parameters. Tables created with the same options share one
/// pool, which stays open for the life of the factory.
#[derive(Default)]
pub struct QuackTableProviderFactory {
    pools: Mutex<Vec<CachedPool>>,
}

/// A pool and the options, sorted by key, it was created with.
struct CachedPool {
    options: Vec<(String, SecretString)>,
    pool: Arc<QuackConnectionPool>,
}

impl std::fmt::Debug for QuackTableProviderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuackTableProviderFactory")
            .finish_non_exhaustive()
    }
}

impl QuackTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    async fn pool(&self, options: &HashMap<String, String>) -> Result<Arc<QuackConnectionPool>> {
        // DataFusion prefixes option keys it doesn't recognize with `format.`.
        let params = to_secret_map(remove_prefix_from_hashmap_keys(options.clone(), "format."));
        let mut key: Vec<(String, SecretString)> =
            params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        key.sort_by(|a, b| a.0.cmp(&b.0));

        let mut pools = self.pools.lock().await;
        let same_options = |other: &[(String, SecretString)]| {
            other.len() == key.len()
                && other.iter().zip(&key).all(|((ka, va), (kb, vb))| {
                    ka == kb && va.expose_secret() == vb.expose_secret()
                })
        };
        if let Some(cached) = pools.iter().find(|cached| same_options(&cached.options)) {
            return Ok(Arc::clone(&cached.pool));
        }
        let pool = Arc::new(
            QuackConnectionPool::new(params)
                .await
                .context(UnableToCreateConnectionPoolSnafu)?,
        );
        pools.push(CachedPool {
            options: key,
            pool: Arc::clone(&pool),
        });
        Ok(pool)
    }
}

#[async_trait::async_trait]
impl TableProviderFactory for QuackTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        if !cmd.schema.fields().is_empty() {
            return Err(DataFusionError::External(Box::new(
                Error::DeclaredSchemaNotSupported,
            )));
        }
        let pool = self
            .pool(&cmd.options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        QuackTableFactory::new(pool)
            .table_provider(TableReference::from(cmd.location.as_str()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
