use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager};
use snafu::{prelude::*, ResultExt};
use std::{cmp::max, sync::Arc};

use super::{
    dbconnection::duckdbconn::{DuckDBAttachments, DuckDBParameter},
    DbConnectionPool, Mode, Result,
};
use crate::{
    sql::db_connection_pool::{
        dbconnection::{duckdbconn::DuckDbConnection, DbConnection, SyncDbConnection},
        JoinPushDown,
    },
    UnsupportedTypeAction,
};

const DEFAULT_MIN_IDLE_CONNECTIONS: u32 = 10;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDB connection failed.\n{source}\nFor details, refer to the DuckDB manual: https://duckdb.org/docs/"))]
    DuckDBConnectionError { source: duckdb::Error },

    #[snafu(display(
        "DuckDB connection failed.\n{source}\nAdjust the DuckDB connection pool parameters for sufficient capacity."
    ))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display(
        "Invalid DuckDB file path: {path}. Ensure it contains a valid database name."
    ))]
    UnableToExtractDatabaseNameFromPath { path: Arc<str> },
}

#[derive(Clone)]
pub struct DuckDbConnectionPool {
    path: Arc<str>,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    join_push_down: JoinPushDown,
    attached_databases: Vec<Arc<str>>,
    mode: Mode,
    unsupported_type_action: UnsupportedTypeAction,
}

impl std::fmt::Debug for DuckDbConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbConnectionPool")
            .field("path", &self.path)
            .field("join_push_down", &self.join_push_down)
            .field("attached_databases", &self.attached_databases)
            .field("mode", &self.mode)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .finish()
    }
}

impl DuckDbConnectionPool {
    /// Get the dataset path. Returns `:memory:` if the in memory database is used.
    pub fn db_path(&self) -> &str {
        self.path.as_ref()
    }

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
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    pub fn new_memory(connection_pool_size: Option<u32>) -> Result<Self> {
        let config = get_config(&AccessMode::ReadWrite)?;
        let manager =
            DuckdbConnectionManager::memory_with_flags(config).context(DuckDBConnectionSnafu)?;

        let mut pool_builder = r2d2::Pool::builder();
        if let Some(size) = connection_pool_size {
            let max_size = max(DEFAULT_MIN_IDLE_CONNECTIONS, size);
            pool_builder = pool_builder
                .max_size(max_size)
                .min_idle(Some(DEFAULT_MIN_IDLE_CONNECTIONS));
        }
        let pool = Arc::new(pool_builder.build(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBConnectionSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            path: ":memory:".into(),
            pool,
            join_push_down: JoinPushDown::AllowedFor(":memory:".to_string()),
            attached_databases: Vec::new(),
            mode: Mode::Memory,
            unsupported_type_action: UnsupportedTypeAction::Error,
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
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    pub fn new_file(
        path: &str,
        access_mode: &AccessMode,
        connection_pool_size: Option<u32>,
    ) -> Result<Self> {
        let config = get_config(access_mode)?;
        let manager = DuckdbConnectionManager::file_with_flags(path, config)
            .context(DuckDBConnectionSnafu)?;

        let mut pool_builder = r2d2::Pool::builder();
        if let Some(size) = connection_pool_size {
            let max_size = max(DEFAULT_MIN_IDLE_CONNECTIONS, size);
            pool_builder = pool_builder
                .max_size(max_size)
                .min_idle(Some(DEFAULT_MIN_IDLE_CONNECTIONS));
        }
        let pool = Arc::new(pool_builder.build(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBConnectionSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            path: path.into(),
            pool,
            // Allow join-push down for any other instances that connect to the same underlying file.
            join_push_down: JoinPushDown::AllowedFor(path.to_string()),
            attached_databases: Vec::new(),
            mode: Mode::File,
            unsupported_type_action: UnsupportedTypeAction::Error,
        })
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
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
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
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
            DuckDbConnection::new(conn)
                .with_attachments(attachments)
                .with_unsupported_type_action(self.unsupported_type_action),
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
            DuckDbConnection::new(conn)
                .with_attachments(attachments)
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn test_connection(conn: &r2d2::PooledConnection<DuckdbConnectionManager>) -> Result<()> {
    conn.execute("SELECT 1", [])
        .context(DuckDBConnectionSnafu)?;
    Ok(())
}

fn get_config(access_mode: &AccessMode) -> Result<duckdb::Config> {
    let config = duckdb::Config::default()
        .access_mode(match access_mode {
            AccessMode::ReadOnly => duckdb::AccessMode::ReadOnly,
            AccessMode::ReadWrite => duckdb::AccessMode::ReadWrite,
            AccessMode::Automatic => duckdb::AccessMode::Automatic,
        })
        .context(DuckDBConnectionSnafu)?;

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
    use rand::Rng;

    use super::*;
    use crate::sql::db_connection_pool::DbConnectionPool;
    use std::sync::Arc;

    fn random_db_name() -> String {
        let mut rng = rand::thread_rng();
        let mut name = String::new();

        for _ in 0..10 {
            name.push(rng.gen_range(b'a'..=b'z') as char);
        }

        format!("./{name}.duckdb")
    }

    #[tokio::test]
    async fn test_duckdb_connection_pool() {
        let pool =
            DuckDbConnectionPool::new_memory(None).expect("DuckDB connection pool to be created");
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

    #[tokio::test]
    #[cfg(feature = "duckdb-federation")]
    async fn test_duckdb_connection_pool_with_attached_databases() {
        let db_base_name = random_db_name();
        let db_attached_name = random_db_name();
        let pool = DuckDbConnectionPool::new_file(&db_base_name, &AccessMode::ReadWrite, None)
            .expect("DuckDB connection pool to be created")
            .set_attached_databases(&[Arc::from(db_attached_name.as_str())]);

        let pool_attached =
            DuckDbConnectionPool::new_file(&db_attached_name, &AccessMode::ReadWrite, None)
                .expect("DuckDB connection pool to be created")
                .set_attached_databases(&[Arc::from(db_base_name.as_str())]);

        let conn = pool
            .pool
            .get()
            .expect("DuckDB connection should be established");

        conn.execute("CREATE TABLE test_one (a INTEGER, b VARCHAR)", [])
            .expect("Table should be created");
        conn.execute("INSERT INTO test_one VALUES (1, 'a')", [])
            .expect("Data should be inserted");

        let conn_attached = pool_attached
            .pool
            .get()
            .expect("DuckDB connection should be established");

        conn_attached
            .execute("CREATE TABLE test_two (a INTEGER, b VARCHAR)", [])
            .expect("Table should be created");
        conn_attached
            .execute("INSERT INTO test_two VALUES (1, 'a')", [])
            .expect("Data should be inserted");

        let conn = pool
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn = conn
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        let conn_attached = pool_attached
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn_attached = conn_attached
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        // sleep to let writes clear
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        conn.query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn_attached
            .query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        conn_attached
            .query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn.query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        std::fs::remove_file(&db_base_name).expect("File should be removed");
        std::fs::remove_file(&db_attached_name).expect("File should be removed");
    }
}
