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

    pub async fn build(self) -> Result<SqliteConnectionPool> {
        let join_push_down = match (self.mode, &self.attach_databases) {
            (Mode::File, Some(attach_databases)) => {
                let mut attach_databases = attach_databases.clone();

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

        let attach_databases = self.attach_databases.unwrap_or_default();
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
