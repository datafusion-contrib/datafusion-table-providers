use async_trait::async_trait;
use dbconnection::DbConnection;

pub mod dbconnection;
#[cfg(feature = "duckdb")]
pub mod duckdbpool;
#[cfg(feature = "mysql")]
pub mod mysqlpool;
#[cfg(feature = "postgres")]
pub mod postgrespool;
#[cfg(feature = "sqlite")]
pub mod sqlitepool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

/// Controls whether join pushdown is allowed, and under what conditions
#[derive(Clone, Debug)]
pub enum JoinPushDown {
    /// This connection pool should not allow join push down. (i.e. we don't know under what conditions it is safe to send a join query to the database)
    Disallow,
    /// Allows join push down for other tables that share the same context.
    ///
    /// The context can be part of the connection string that uniquely identifies the server.
    AllowedFor(String),
}

#[async_trait]
pub trait DbConnectionPool<T, P: 'static> {
    async fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;

    fn join_push_down(&self) -> JoinPushDown;
}

#[derive(Default)]
pub enum Mode {
    #[default]
    Memory,
    File,
}

impl From<&str> for Mode {
    fn from(m: &str) -> Self {
        match m {
            "file" => Mode::File,
            "memory" => Mode::Memory,
            _ => Mode::default(),
        }
    }
}
