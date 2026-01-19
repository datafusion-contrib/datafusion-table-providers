use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bb8::CustomizeConnection;
use bb8_oracle::OracleConnectionManager;
use oracle::{Connection, Connector};

use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;

use super::DbConnectionPool;

/// Default TCP port for Oracle Database
const DEFAULT_ORACLE_PORT: u16 = 1521;

/// Default service name for Oracle Database connections
static DEFAULT_SERVICE_NAME: &str = "ORCL";

/// Default maximum pool size
const DEFAULT_POOL_MAX_SIZE: u32 = 10;

/// Default timezone for Oracle sessions (UTC for consistent timestamp handling)
static DEFAULT_TIMEZONE: &str = "UTC";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Oracle connection failed: {source}"))]
    ConnectionError { source: oracle::Error },

    #[snafu(display("Unable to create Oracle connection pool: {source}"))]
    PoolCreationError { source: bb8_oracle::Error },

    #[snafu(display("Unable to get Oracle connection from pool: {source}"))]
    PoolRunError {
        source: bb8::RunError<bb8_oracle::Error>,
    },

    #[snafu(display("Missing required parameter: {param}"))]
    MissingParameter { param: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Customizer that sets session timezone on connection acquire.
/// This ensures consistent timestamp handling across all connections.
#[derive(Debug, Clone)]
pub struct SetTimezoneCustomizer {
    pub timezone: String,
}

impl CustomizeConnection<Arc<Connection>, bb8_oracle::Error> for SetTimezoneCustomizer {
    fn on_acquire<'a>(
        &'a self,
        conn: &'a mut Arc<Connection>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), bb8_oracle::Error>> + Send + 'a>> {
        let sql = format!("ALTER SESSION SET TIME_ZONE = '{}'", self.timezone);
        Box::pin(async move {
            // Execute the timezone setting synchronously
            // rust-oracle is synchronous, so this is safe in async context
            conn.execute(&sql, &[])
                .map_err(bb8_oracle::Error::Database)?;
            Ok(())
        })
    }
}

pub struct OracleConnectionPool {
    pool: Arc<bb8::Pool<OracleConnectionManager>>,
}

impl std::fmt::Debug for OracleConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleConnectionPool").finish()
    }
}

impl OracleConnectionPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let user = params
            .get("user")
            .ok_or(Error::MissingParameter {
                param: "user".to_string(),
            })?
            .expose_secret();

        let password = params
            .get("password")
            .ok_or(Error::MissingParameter {
                param: "password".to_string(),
            })?
            .expose_secret();

        let host = params
            .get("host")
            .ok_or(Error::MissingParameter {
                param: "host".to_string(),
            })?
            .expose_secret();

        let port = params
            .get("port")
            .map(|s| {
                s.expose_secret()
                    .parse::<u16>()
                    .unwrap_or(DEFAULT_ORACLE_PORT)
            })
            .unwrap_or(DEFAULT_ORACLE_PORT);

        let service_name = params
            .get("service_name")
            .or_else(|| params.get("sid"))
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string());

        let connector = Connector::new(
            user,
            password,
            format!("//{}:{}/{}", host, port, service_name),
        );

        let manager = OracleConnectionManager::from_connector(connector);

        // Get timezone setting (default to UTC for consistent timestamp handling)
        let timezone = params
            .get("timezone")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| DEFAULT_TIMEZONE.to_string());

        let pool = bb8::Pool::builder()
            .max_size(
                params
                    .get("pool_max")
                    .and_then(|s| s.expose_secret().parse().ok())
                    .unwrap_or(DEFAULT_POOL_MAX_SIZE),
            )
            .connection_customizer(Box::new(SetTimezoneCustomizer { timezone }))
            .build(manager)
            .await
            .map_err(|e| Error::PoolCreationError { source: e })?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    pub async fn connect_direct(
        &self,
    ) -> Result<super::dbconnection::oracleconn::OracleConnection> {
        let conn = Arc::clone(&self.pool)
            .get_owned()
            .await
            .map_err(|e| Error::PoolRunError { source: e })?;

        Ok(super::dbconnection::oracleconn::OracleConnection::new(conn))
    }
}

#[async_trait]
impl
    DbConnectionPool<
        bb8::PooledConnection<'static, OracleConnectionManager>,
        oracle::sql_type::OracleType,
    > for OracleConnectionPool
{
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<
            dyn super::dbconnection::DbConnection<
                bb8::PooledConnection<'static, OracleConnectionManager>,
                oracle::sql_type::OracleType,
            >,
        >,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let conn = Arc::clone(&self.pool).get_owned().await.map_err(|e| {
            Box::new(Error::PoolRunError { source: e }) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(
            super::dbconnection::oracleconn::OracleConnection::new(conn),
        ))
    }

    fn join_push_down(&self) -> super::JoinPushDown {
        super::JoinPushDown::Disallow
    }
}
