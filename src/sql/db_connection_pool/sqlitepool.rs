use async_trait::async_trait;
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

pub struct SqliteConnectionPool {
    conn: Connection,
    join_push_down: JoinPushDown,
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
    pub async fn new(path: &str, mode: Mode) -> Result<Self> {
        let (conn, join_push_down) = match mode {
            Mode::Memory => (
                Connection::open_in_memory()
                    .await
                    .context(ConnectionPoolSnafu)?,
                JoinPushDown::Disallow,
            ),
            Mode::File => (
                Connection::open(path.to_string())
                    .await
                    .context(ConnectionPoolSnafu)?,
                JoinPushDown::AllowedFor(path.to_string()),
            ),
        };

        Ok(SqliteConnectionPool {
            conn,
            join_push_down,
        })
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
        Ok(Box::new(SqliteConnection::new(self.conn.clone())))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
