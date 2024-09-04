use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager};
use snafu::{prelude::*, ResultExt};
use std::sync::Arc;

use super::{dbconnection::duckdbconn::DuckDBParameter, DbConnectionPool, Result};
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
        Ok(Box::new(DuckDbConnection::new(conn)))
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

        #[cfg(feature = "duckdb-federation")]
        if !self.attached_databases.is_empty() {
            let mut db_ids = Vec::new();
            db_ids.push(extract_db_name(Arc::clone(&self.path))?);

            for (i, db) in self.attached_databases.iter().enumerate() {
                // check the db file exists
                std::fs::metadata(db.as_ref()).context(UnableToAttachDatabaseSnafu {
                    path: Arc::clone(db),
                })?;

                let db_id = format!("attachment_{i}");
                conn.execute(
                    &format!(
                        "ATTACH IF NOT EXISTS '{db}' AS {} (READ_ONLY)",
                        db_id.clone()
                    ),
                    [],
                )
                .context(DuckDBSnafu)?;
                db_ids.push(db_id);
            }
            conn.execute(&format!("SET search_path = \"{}\"", db_ids.join(",")), [])
                .context(DuckDBSnafu)?;
        }

        Ok(Box::new(DuckDbConnection::new(conn)))
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

        format!("./{name}.sqlite")
    }

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

    #[tokio::test]
    async fn test_duckdb_connection_pool_with_attached_databases() {
        let db_base_name = random_db_name();
        let db_attached_name = random_db_name();
        let pool = DuckDbConnectionPool::new_file(&db_base_name, &AccessMode::ReadWrite)
            .expect("DuckDB connection pool to be created")
            .set_attached_databases(&[Arc::from(db_attached_name.as_str())]);

        let pool_attached =
            DuckDbConnectionPool::new_file(&db_attached_name, &AccessMode::ReadWrite)
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

        conn.query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn_attached
            .query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        conn_attached
            .query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn.query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        std::fs::remove_file(&db_base_name).expect("File should be removed");
        std::fs::remove_file(&db_attached_name).expect("File should be removed");
    }
}
