use async_trait::async_trait;
use dbconnection::DbConnection;
use secrecy::{ExposeSecret, SecretString};
use std::sync::Arc;

pub mod dbconnection;

#[cfg(feature = "adbc")]
pub mod adbcpool;
#[cfg(feature = "clickhouse")]
pub mod clickhousepool;
#[cfg(feature = "duckdb")]
pub mod duckdbpool;
#[cfg(feature = "mysql")]
pub mod mysqlpool;
#[cfg(feature = "odbc")]
pub mod odbcpool;
#[cfg(feature = "postgres")]
pub mod postgrespool;
pub mod runtime;
#[cfg(feature = "sqlite")]
pub mod sqlitepool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

/// A trait for providing passwords dynamically to database connection pools.
///
/// Implementations can fetch credentials from secret managers, IAM services,
/// or other dynamic sources. Called each time a new connection is created in the pool.
///
/// Implementations that cache or rate-limit credentials should use interior
/// mutability (e.g., `tokio::sync::RwLock`) since the trait requires `&self`.
#[async_trait]
pub trait PasswordProvider: Send + Sync {
    /// Returns the current password/token for authentication.
    /// Called each time a new connection is created in the pool.
    async fn get_password(&self) -> Result<SecretString>;
}

/// A password provider that always returns the same static password.
///
/// This is the default provider used by [`PostgresConnectionPool::new()`](crate::sql::db_connection_pool::postgrespool::PostgresConnectionPool::new)
/// when a `pass` parameter is supplied. It can also be used explicitly with
/// [`new_with_password_provider()`](crate::sql::db_connection_pool::postgrespool::PostgresConnectionPool::new_with_password_provider).
pub struct StaticPasswordProvider(SecretString);

impl StaticPasswordProvider {
    /// Creates a new `StaticPasswordProvider` with the given password.
    pub fn new(password: SecretString) -> Self {
        Self(password)
    }
}

#[async_trait]
impl PasswordProvider for StaticPasswordProvider {
    async fn get_password(&self) -> Result<SecretString> {
        Ok(SecretString::from(self.0.expose_secret().to_string()))
    }
}

/// Controls whether join pushdown is allowed, and under what conditions
#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
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

/// A key that uniquely identifies a database instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbInstanceKey {
    /// The database is a file on disk, with the given path.
    File(Arc<str>),
    /// The database is in memory.
    Memory,
}

impl DbInstanceKey {
    pub fn memory() -> Self {
        DbInstanceKey::Memory
    }

    pub fn file(path: Arc<str>) -> Self {
        DbInstanceKey::File(path)
    }
}
