use std::sync::Arc;

use arrow::array::AsArray;
use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager, ToSql};
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Result};
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
}

pub struct DuckDbConnectionPool {
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
            pool,
            // Allow join-push down for any other instances that connect to the same underlying file.
            join_push_down: JoinPushDown::AllowedFor(path.to_string()),
            attached_databases: Vec::new(),
        })
    }

    #[must_use]
    pub fn set_attached_databases(mut self, databases: &[Arc<str>]) -> Self {
        self.attached_databases = databases.to_vec();
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
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(DuckDbConnection::new(conn)))
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
    for DuckDbConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;

        println!("connect attached_databases: {:?}", self.attached_databases);

        if !self.attached_databases.is_empty() {
            for (i, db) in self.attached_databases.iter().enumerate() {
                // check the db file exists
                std::fs::metadata(db.as_ref()).context(UnableToAttachDatabaseSnafu {
                    path: Arc::clone(db),
                })?;

                let db_id = format!("attachment_{i}");
                conn.execute(
                    &format!("ATTACH IF NOT EXISTS '{db}' AS {db_id} (READ_ONLY)"),
                    [],
                )
                .context(DuckDBSnafu)?;
            }

            let mut stmt = conn.prepare("SHOW DATABASES").context(DuckDBSnafu)?;
            let results = stmt.query_arrow([]).context(DuckDBSnafu)?;
            let results = results.collect::<Vec<_>>();

            let db_ids = results
                .iter()
                .flat_map(|r| match r.column(0).data_type() {
                    arrow::datatypes::DataType::Utf8 => r
                        .column(0)
                        .as_string::<i32>()
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>(),
                    arrow::datatypes::DataType::LargeUtf8 => r
                        .column(0)
                        .as_string::<i64>()
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>(),
                    _ => {
                        unreachable!("Unexpected data type from SHOW DATABASES");
                    }
                })
                .collect::<Vec<_>>();

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
        let pool = DuckDbConnectionPool::new_memory().unwrap();
        let conn = pool.connect().await.unwrap();
        let conn = conn.as_sync().unwrap();

        conn.execute("CREATE TABLE test (a INTEGER, b VARCHAR)", &[])
            .unwrap();
        conn.execute("INSERT INTO test VALUES (1, 'a')", &[])
            .unwrap();

        conn.query_arrow("SELECT * FROM test", &[]).unwrap();
    }

    #[tokio::test]
    async fn test_duckdb_connection_pool_with_attached_databases() {
        let db_base_name = random_db_name();
        let db_attached_name = random_db_name();
        let pool = DuckDbConnectionPool::new_file(&db_base_name, &AccessMode::ReadWrite)
            .unwrap()
            .set_attached_databases(&[Arc::from(db_attached_name.as_str())]);

        let pool_attached =
            DuckDbConnectionPool::new_file(&db_attached_name, &AccessMode::ReadWrite)
                .unwrap()
                .set_attached_databases(&[Arc::from(db_base_name.as_str())]);

        let conn = pool.pool.get().unwrap();

        conn.execute("CREATE TABLE test_one (a INTEGER, b VARCHAR)", [])
            .unwrap();
        conn.execute("INSERT INTO test_one VALUES (1, 'a')", [])
            .unwrap();

        let conn_attached = pool_attached.pool.get().unwrap();

        conn_attached
            .execute("CREATE TABLE test_two (a INTEGER, b VARCHAR)", [])
            .unwrap();
        conn_attached
            .execute("INSERT INTO test_two VALUES (1, 'a')", [])
            .unwrap();

        let conn = pool.connect().await.unwrap();
        let conn = conn.as_sync().unwrap();

        let conn_attached = pool_attached.connect().await.unwrap();
        let conn_attached = conn_attached.as_sync().unwrap();

        conn.query_arrow("SELECT * FROM test_one", &[]).unwrap();

        conn_attached
            .query_arrow("SELECT * FROM test_two", &[])
            .unwrap();

        conn_attached
            .query_arrow("SELECT * FROM test_one", &[])
            .unwrap();

        conn.query_arrow("SELECT * FROM test_two", &[]).unwrap();

        std::fs::remove_file(&db_base_name).unwrap();
        std::fs::remove_file(&db_attached_name).unwrap();
    }
}
