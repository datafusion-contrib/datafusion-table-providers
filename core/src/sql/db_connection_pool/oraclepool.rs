use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bb8_oracle::OracleConnectionManager;
use oracle::Connector;

use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;

use super::DbConnectionPool;

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

    #[snafu(display("Wallet configuration error: {message}"))]
    WalletConfigError { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
            .map(|s| s.expose_secret().parse::<u16>().unwrap_or(1521))
            .unwrap_or(1521);

        let service_name = params
            .get("service_name")
            .or_else(|| params.get("sid"))
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| "ORCL".to_string());

        let connector = Connector::new(
            user,
            password,
            format!("//{}:{}/{}", host, port, service_name),
        );

        // Apply wallet configuration if provided
        if let Some(_wallet_path) = params.get("wallet_path") {
            // Note: rust-oracle (ODPI-C) handles wallets via TNS_ADMIN or connect string
        }

        let manager = OracleConnectionManager::from_connector(connector);

        let pool = bb8::Pool::builder()
            .max_size(
                params
                    .get("pool_max")
                    .and_then(|s| s.expose_secret().parse().ok())
                    .unwrap_or(10),
            )
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
