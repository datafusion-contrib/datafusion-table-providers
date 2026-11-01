//! A read-only DataFusion table provider for a remote DuckDB served over DuckDB's Quack
//! protocol: v1 (DuckDB 1.5) or v3 (DuckDB 2.0). Enabled by the `quack` feature.
//!
//! Build a [`pool::QuackConnectionPool`] for a server, then a [`QuackTableFactory`] on it
//! for each table, or register [`QuackTableProviderFactory`] to use
//! `CREATE EXTERNAL TABLE ... STORED AS QUACK`.
//!
//! - Read-only: tables and views must already exist on the server; nothing is created there.
//! - Each open scan holds one pooled session until its stream ends, so size
//!   `connection_pool_size` for the scans a query runs at once. Dropping a stream releases
//!   its session but does not cancel the query on the server.
//! - Filters are pushed down only where DuckDB gives the same answer as DataFusion:
//!   comparisons and `IN` lists on integer (up to 64-bit, not HUGEINT), DECIMAL, DATE,
//!   BOOLEAN and second, millisecond or microsecond TIMESTAMP columns; `IS [NOT] NULL` on
//!   any column; and `AND`, `OR`, `NOT` over those. DataFusion evaluates the rest. Under
//!   the federation optimizer, federated subplans run entirely in DuckDB, with DuckDB's
//!   semantics (collations, NaN ordering, ...), and their results are cast to the types
//!   DataFusion planned: an out-of-range value is an error, while lossy conversions such as
//!   DOUBLE to DECIMAL follow Arrow's cast.
//! - Types map as `quack_protocol` maps them: HUGEINT and UHUGEINT arrive as
//!   `Decimal256(39, 0)`; ENUM, UUID and JSON as `Utf8`; BIT and GEOMETRY as `Binary`
//!   (DuckDB's bitstring bytes and WKB); VARIANT as DuckDB's shredded struct. TIMETZ, UNION
//!   and BIGNUM follow the pool's `UnsupportedTypeAction`.

use std::sync::Arc;

use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::{plan_err, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::CreateExternalTable;
use datafusion_table_providers_common::sql::sql_provider_datafusion;
use datafusion_table_providers_common::util::secrets::to_secret_map;
use datafusion_table_providers_common::util::{
    remove_prefix_from_hashmap_keys, to_datafusion_error,
};
use snafu::prelude::*;

use crate::quack::pool::QuackConnectionPool;
use crate::quack::sql_table::QuackTable;

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
    UnableToCreateFederatedTableProvider {
        source: datafusion::error::DataFusionError,
    },
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
/// already exist on the server; nothing is created there. The schema comes from the server,
/// and a column list in the statement is ignored. `OPTIONS` are the
/// [`QuackConnectionPool::new`] parameters; each table gets its own pool.
#[derive(Debug, Default)]
pub struct QuackTableProviderFactory {}

impl QuackTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TableProviderFactory for QuackTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let [location] = cmd.locations.as_slice() else {
            return plan_err!(
                "Quack table requires exactly one LOCATION, got {}",
                cmd.locations.len()
            );
        };
        // DataFusion prefixes option keys it doesn't recognize with `format.`.
        let params = to_secret_map(remove_prefix_from_hashmap_keys(
            cmd.options.clone(),
            "format.",
        ));
        let pool = QuackConnectionPool::new(params)
            .await
            .context(UnableToCreateConnectionPoolSnafu)
            .map_err(to_datafusion_error)?;
        QuackTableFactory::new(Arc::new(pool))
            .table_provider(TableReference::from(location.as_str()))
            .await
            .map_err(to_datafusion_error)
    }
}
