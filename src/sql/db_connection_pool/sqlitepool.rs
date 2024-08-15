use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use snafu::{prelude::*, ResultExt};
use tokio_rusqlite::{Connection, ToSql};

use super::{DbConnectionPool, Result};
use crate::sql::db_connection_pool::{
    dbconnection::{sqliteconn::SqliteConnection, AsyncDbConnection, DbConnection},
    JoinPushDown, Mode,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: tokio_rusqlite::Error },

    #[snafu(display("No path provided for SQLite connection"))]
    NoPathError {},

    #[snafu(display("Database to attach does not exist: {path}"))]
    DatabaseDoesNotExist { path: String },
}

pub struct SqliteConnectionPoolFactory {
    path: Arc<str>,
    mode: Mode,
    attach_databases: Option<Vec<Arc<str>>>,
}

fn hash_string(val: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(val);
    hasher.finalize().iter().fold(String::new(), |mut hash, b| {
        hash.push_str(&format!("{b:02x}"));
        hash
    })
}

impl SqliteConnectionPoolFactory {
    pub fn new(path: &str, mode: Mode) -> Self {
        SqliteConnectionPoolFactory {
            path: path.into(),
            mode,
            attach_databases: None,
        }
    }

    #[must_use]
    pub fn with_databases(mut self, attach_databases: Option<Vec<Arc<str>>>) -> Self {
        self.attach_databases = attach_databases;
        self
    }

    pub async fn build(&self) -> Result<SqliteConnectionPool> {
        let join_push_down = match (self.mode, &self.attach_databases) {
            (Mode::File, Some(attach_databases)) => {
                let mut attach_databases = attach_databases.clone();

                for database in &attach_databases {
                    // check if the database file exists
                    if std::fs::metadata(database.as_ref()).is_err() {
                        return Err(Error::DatabaseDoesNotExist {
                            path: database.to_string(),
                        }
                        .into());
                    }
                }

                if !attach_databases.contains(&self.path) {
                    attach_databases.push(Arc::clone(&self.path));
                }

                attach_databases.sort();

                let attach_hash = hash_string(&attach_databases.join(";"));
                JoinPushDown::AllowedFor(attach_hash) // push down is allowed cross-database when they're attached together
                                                      // hash the list of databases to generate the comparison for push down
            }
            (Mode::File, None) => JoinPushDown::AllowedFor(self.path.to_string()),
            _ => JoinPushDown::Disallow,
        };

        let attach_databases = if let Some(attach_databases) = &self.attach_databases {
            attach_databases.clone()
        } else {
            vec![]
        };

        let pool =
            SqliteConnectionPool::new(&self.path, self.mode, join_push_down, attach_databases)
                .await?;

        pool.setup().await?;

        Ok(pool)
    }
}

pub struct SqliteConnectionPool {
    conn: Connection,
    join_push_down: JoinPushDown,
    mode: Mode,
    path: Arc<str>,
    attach_databases: Vec<Arc<str>>,
}

impl SqliteConnectionPool {
    /// Creates a new instance of `SqliteConnectionPool`.
    ///
    /// NOTE: The `SqliteConnectionPool` currently does no connection pooling, it simply creates a new connection
    /// and clones it on each call to `connect()`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        path: &str,
        mode: Mode,
        join_push_down: JoinPushDown,
        attach_databases: Vec<Arc<str>>,
    ) -> Result<Self> {
        let conn = match mode {
            Mode::Memory => Connection::open_in_memory()
                .await
                .context(ConnectionPoolSnafu)?,

            Mode::File => Connection::open(path.to_string())
                .await
                .context(ConnectionPoolSnafu)?,
        };

        Ok(SqliteConnectionPool {
            conn,
            join_push_down,
            mode,
            attach_databases,
            path: path.into(),
        })
    }

    /// Initializes an SQLite database on-disk without creating a connection pool.
    /// No-op if the database is in-memory.
    pub async fn init(path: &str, mode: Mode) -> Result<()> {
        if mode == Mode::File {
            Connection::open(path.to_string())
                .await
                .context(ConnectionPoolSnafu)?;
        }

        Ok(())
    }

    pub async fn setup(&self) -> Result<()> {
        let conn = self.conn.clone();

        // these configuration options are only applicable for file-mode databases
        if self.mode == Mode::File {
            // change transaction mode to Write-Ahead log instead of default atomic rollback journal: https://www.sqlite.org/wal.html
            // NOTE: This is a no-op if the database is in-memory, as only MEMORY or OFF are supported: https://www.sqlite.org/pragma.html#pragma_journal_mode
            conn.call(|conn| {
                conn.pragma_update(None, "journal_mode", "WAL")?;
                conn.pragma_update(None, "busy_timeout", "5000")?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                conn.pragma_update(None, "cache_size", "-20000")?;
                conn.pragma_update(None, "foreign_keys", "true")?;
                conn.pragma_update(None, "temp_store", "memory")?;
                // conn.set_transaction_behavior(TransactionBehavior::Immediate); introduced in rustqlite 0.32.1, but tokio-rusqlite is still on 0.31.0
                Ok(())
            })
            .await
            .context(ConnectionPoolSnafu)?;

            // database attachments are only supported for file-mode databases
            let attach_databases = self
                .attach_databases
                .iter()
                .enumerate()
                .map(|(i, db)| format!("ATTACH DATABASE '{db}' AS attachment_{i}"));

            for attachment in attach_databases {
                if attachment == *self.path {
                    continue;
                }

                conn.call(move |conn| {
                    conn.execute(&attachment, [])?;
                    Ok(())
                })
                .await
                .context(ConnectionPoolSnafu)?;
            }
        }

        Ok(())
    }

    #[must_use]
    pub fn connect_sync(&self) -> Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>> {
        Box::new(SqliteConnection::new(self.conn.clone()))
    }
}

#[async_trait]
impl DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> for SqliteConnectionPool {
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>> {
        let conn = self.conn.clone();

        Ok(Box::new(SqliteConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::db_connection_pool::Mode;

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory() {
        let factory = SqliteConnectionPoolFactory::new("./test1.sqlite", Mode::File);
        let pool = factory.build().await.unwrap();

        assert!(pool.join_push_down == JoinPushDown::AllowedFor("./test1.sqlite".to_string()));
        assert!(pool.mode == Mode::File);
        assert_eq!(pool.path, "./test1.sqlite".into());

        drop(pool);

        // cleanup
        std::fs::remove_file("./test1.sqlite").unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_with_attachments() {
        let factory = SqliteConnectionPoolFactory::new("./test2.sqlite", Mode::File)
            .with_databases(Some(vec!["./test3.sqlite".into(), "./test4.sqlite".into()]));

        SqliteConnectionPool::init("./test3.sqlite", Mode::File)
            .await
            .unwrap();
        SqliteConnectionPool::init("./test4.sqlite", Mode::File)
            .await
            .unwrap();

        let pool = factory.build().await.unwrap();

        let push_down_hash = hash_string("./test2.sqlite;./test3.sqlite;./test4.sqlite");

        assert!(pool.join_push_down == JoinPushDown::AllowedFor(push_down_hash));
        assert!(pool.mode == Mode::File);
        assert_eq!(pool.path, "./test2.sqlite".into());

        drop(pool);

        // cleanup
        std::fs::remove_file("./test2.sqlite").unwrap();
        std::fs::remove_file("./test3.sqlite").unwrap();
        std::fs::remove_file("./test4.sqlite").unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_memory_with_attachments() {
        let factory = SqliteConnectionPoolFactory::new("./test5.sqlite", Mode::Memory)
            .with_databases(Some(vec!["./test6.sqlite".into(), "./test7.sqlite".into()]));
        let pool = factory.build().await.unwrap();

        assert!(pool.join_push_down == JoinPushDown::Disallow);
        assert!(pool.mode == Mode::Memory);
        assert_eq!(pool.path, "./test5.sqlite".into());

        drop(pool);

        // in memory mode, attachments are not created and nothing happens
        assert!(std::fs::metadata("./test5.sqlite").is_err());
        assert!(std::fs::metadata("./test6.sqlite").is_err());
        assert!(std::fs::metadata("./test7.sqlite").is_err());
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_errors_with_missing_attachments() {
        let factory =
            SqliteConnectionPoolFactory::new("./test8.sqlite", Mode::File).with_databases(Some(
                vec!["./test9.sqlite".into(), "./test10.sqlite".into()],
            ));
        let pool = factory.build().await;

        assert!(pool.is_err());

        let err = pool.err().unwrap();
        assert!(err
            .to_string()
            .contains("Database to attach does not exist: ./test9.sqlite"));
    }
}
