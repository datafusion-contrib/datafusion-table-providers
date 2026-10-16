//! A DataFusion [`TableProvider`] for Apache Cassandra, built on the
//! [ScyllaDB Rust driver](https://docs.rs/scylla), so it works with ScyllaDB too.
//!
//! CQL is far more restrictive than SQL: it has no joins or aggregations, and `WHERE` clauses
//! are limited to key columns. Scans therefore only push the projection and `LIMIT` down to
//! Cassandra. Filters, aggregations and joins are evaluated by DataFusion.
//!
//! Tables created with [`CassandraTableFactory::read_write_table_provider`] can also be written
//! to with `INSERT INTO`; see [`CassandraTableWriter`].

pub mod conn;
pub mod pool;
mod table;
pub mod types;
pub mod write;

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;

use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::AsyncDbConnection;

pub use crate::pool::CassandraConnectionPool;
pub use crate::table::{CassandraExec, CassandraTable};
pub use crate::write::CassandraTableWriter;

/// Creates [`TableProvider`]s for Cassandra tables.
#[derive(Debug)]
pub struct CassandraTableFactory {
    pool: Arc<CassandraConnectionPool>,
}

impl CassandraTableFactory {
    pub fn new(pool: impl Into<Arc<CassandraConnectionPool>>) -> Self {
        Self { pool: pool.into() }
    }

    /// Creates a table provider for `table_reference`, given as `table` (in the pool's
    /// keyspace) or `keyspace.table`. Names are case-sensitive.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema of the table cannot be read.
    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        let schema = self
            .pool
            .connect_direct()
            .get_schema(&table_reference)
            .await?;

        Ok(Arc::new(CassandraTable::new(
            table_reference,
            Arc::clone(&self.pool),
            schema,
        )))
    }

    /// Creates a table provider for `table_reference` that also supports `INSERT INTO`.
    ///
    /// See [`CassandraTableWriter`] for how the rows are written.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema of the table cannot be read.
    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        let read_provider = self.table_provider(table_reference.clone()).await?;
        Ok(CassandraTableWriter::create(
            read_provider,
            table_reference,
            Arc::clone(&self.pool),
        ))
    }
}
