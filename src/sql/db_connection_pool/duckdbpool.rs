use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager};
use snafu::{prelude::*, ResultExt};
use std::sync::Arc;

use super::{
    dbconnection::duckdbconn::{DuckDBAttachments, DuckDBParameter},
    DbConnectionPool, Mode, Result,
};
use crate::sql::db_connection_pool::{
    dbconnection::{duckdbconn::DuckDbConnection, DbConnection, SyncDbConnection},
    JoinPushDown,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display("Unable to connect to DuckDB: {source}"))]
    UnableToConnect { source: duckdb::Error },

    #[snafu(display("Unable to attach DuckDB database {path}: {source}"))]
    UnableToAttachDatabase {
        path: Arc<str>,
        source: std::io::Error,
    },

    #[snafu(display("Unable to extract database name from database file path"))]
    UnableToExtractDatabaseNameFromPath { path: Arc<str> },
}

#[derive(Clone)]
pub struct DuckDbConnectionPool {
    path: Arc<str>,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    join_push_down: JoinPushDown,
    attached_databases: Vec<Arc<str>>,
    mode: Mode,
}

impl std::fmt::Debug for DuckDbConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DuckDbConnectionPool {}", self.path)
    }
}

impl DuckDbConnectionPool {
    /// Create a new `DuckDbConnectionPool` from memory.
    ///
    /// # Arguments
    ///
    /// * `access_mode` - The access mode for the connection pool
    ///
    /// # Returns
    ///
    /// * A new `DuckDbConnectionPool`
    ///
    /// # Errors
    ///
    /// * `DuckDBSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    /// * `UnableToConnectSnafu` - If there is an error connecting to the database
    pub fn new_memory() -> Result<Self> {
        let config = get_config(&AccessMode::ReadWrite)?;
        let manager = DuckdbConnectionManager::memory_with_flags(config).context(DuckDBSnafu)?;
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            path: ":memory:".into(),
            pool,
            // There can't be any other tables that share the same context for an in-memory DuckDB.
            join_push_down: JoinPushDown::Disallow,
            attached_databases: Vec::new(),
            mode: Mode::Memory,
        })
    }

    /// Create a new `DuckDbConnectionPool` from a file.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file
    /// * `access_mode` - The access mode for the connection pool
    ///
    /// # Returns
    ///
    /// * A new `DuckDbConnectionPool`
    ///
    /// # Errors
    ///
    /// * `DuckDBSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    /// * `UnableToConnectSnafu` - If there is an error connecting to the database
    pub fn new_file(path: &str, access_mode: &AccessMode) -> Result<Self> {
        let config = get_config(access_mode)?;
        let manager =
            DuckdbConnectionManager::file_with_flags(path, config).context(DuckDBSnafu)?;
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            path: path.into(),
            pool,
            // Allow join-push down for any other instances that connect to the same underlying file.
            join_push_down: JoinPushDown::AllowedFor(path.to_string()),
            attached_databases: Vec::new(),
            mode: Mode::File,
        })
    }

    #[must_use]
    pub fn set_attached_databases(mut self, databases: &[Arc<str>]) -> Self {
        self.attached_databases = databases.to_vec();

        if !databases.is_empty() {
            let mut paths = self.attached_databases.clone();
            paths.push(Arc::clone(&self.path));
            paths.sort();
            let push_down_context = paths.join(";");
            self.join_push_down = JoinPushDown::AllowedFor(push_down_context);
        }

        self
    }

    /// Create a new `DuckDbConnectionPool` from a database URL.
    ///
    /// # Errors
    ///
    /// * `DuckDBSnafu` - If there is an error creating the connection pool
    pub fn connect_sync(
        self: Arc<Self>,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;

        let attachments = self.get_attachments()?;

        Ok(Box::new(
            DuckDbConnection::new(conn).with_attachments(attachments),
        ))
    }

    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn get_attachments(&self) -> Result<Option<Arc<DuckDBAttachments>>> {
        if self.attached_databases.is_empty() {
            Ok(None)
        } else {
            #[cfg(not(feature = "duckdb-federation"))]
            return Ok(None);

            #[cfg(feature = "duckdb-federation")]
            Ok(Some(Arc::new(DuckDBAttachments::new(
                &extract_db_name(Arc::clone(&self.path))?,
                &self.attached_databases,
            ))))
        }
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;

        let attachments = self.get_attachments()?;

        Ok(Box::new(
            DuckDbConnection::new(conn).with_attachments(attachments),
        ))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn test_connection(conn: &r2d2::PooledConnection<DuckdbConnectionManager>) -> Result<()> {
    conn.execute("SELECT 1", []).context(UnableToConnectSnafu)?;
    Ok(())
}

fn get_config(access_mode: &AccessMode) -> Result<duckdb::Config> {
    let config = duckdb::Config::default()
        .access_mode(match access_mode {
            AccessMode::ReadOnly => duckdb::AccessMode::ReadOnly,
            AccessMode::ReadWrite => duckdb::AccessMode::ReadWrite,
            AccessMode::Automatic => duckdb::AccessMode::Automatic,
        })
        .context(DuckDBSnafu)?;

    Ok(config)
}

// Helper function to extract the duckdb database name from the duckdb file path
fn extract_db_name(file_path: Arc<str>) -> Result<String> {
    let path = std::path::Path::new(file_path.as_ref());

    let db_name = match path.file_stem().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => {
            return Err(Box::new(Error::UnableToExtractDatabaseNameFromPath {
                path: file_path,
            }))
        }
    };

    Ok(db_name.to_string())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::sql::db_connection_pool::DbConnectionPool;

    #[tokio::test]
    async fn test_duckdb_connection_pool() {
        let pool =
            DuckDbConnectionPool::new_memory().expect("DuckDB connection pool to be created");
        let conn = pool
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn = conn
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        conn.execute("CREATE TABLE test (a INTEGER, b VARCHAR)", &[])
            .expect("Table should be created");
        conn.execute("INSERT INTO test VALUES (1, 'a')", &[])
            .expect("Data should be inserted");

        conn.query_arrow("SELECT * FROM test", &[], None)
            .expect("Query should be successful");
    }
}
