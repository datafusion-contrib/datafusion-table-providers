use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datafusion_table_providers_common::sql::db_connection_pool::{
    dbconnection::{AsyncDbConnection, DbConnection, GenericError},
    DbConnectionPool, JoinPushDown,
};
use datafusion_table_providers_common::UnsupportedTypeAction;
use quack_protocol::{QuackClientOptions, QuackPool, QuackPoolOptions, DEFAULT_MAX_CONNECTIONS};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;

use crate::conn::{QuackConnection, QuackSession};

const ENDPOINT: &str = "endpoint";
const TOKEN: &str = "token";
const SSL: &str = "ssl";
const CONNECTION_POOL_SIZE: &str = "connection_pool_size";
const CONNECTION_POOL_ACQUIRE_TIMEOUT: &str = "connection_pool_acquire_timeout";
const PARAMETERS: [&str; 5] = [
    ENDPOINT,
    TOKEN,
    SSL,
    CONNECTION_POOL_SIZE,
    CONNECTION_POOL_ACQUIRE_TIMEOUT,
];

const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Quack protocol v3, spoken by DuckDB 2.0 and later. Older servers are refused.
const QUACK_PROTOCOL_VERSION: u64 = 3;

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter '{parameter}'. Provide the Quack server address, e.g. 'quack:localhost:9494'."))]
    MissingParameter { parameter: &'static str },

    #[snafu(display("Unknown parameter '{parameter}'. Valid parameters are: {}.", PARAMETERS.join(", ")))]
    UnknownParameter { parameter: String },

    #[snafu(display("Invalid value '{value}' for parameter '{parameter}'. Provide {expected}."))]
    InvalidParameter {
        parameter: &'static str,
        value: String,
        expected: &'static str,
    },

    #[snafu(display("Unable to connect to the Quack server at '{endpoint}': {source}. Check the endpoint and token, and that the server is DuckDB 2.0 (Quack protocol v3) running quack_serve."))]
    UnableToConnect {
        endpoint: String,
        source: GenericError,
    },

    #[snafu(display("Unable to get a connection from the Quack pool: {source}"))]
    UnableToAcquire { source: GenericError },

    #[snafu(display("Timed out after {}s waiting for a Quack connection; all {pool_size} are in use, likely by open scans. Raise '{CONNECTION_POOL_SIZE}' (or '{CONNECTION_POOL_ACQUIRE_TIMEOUT}').", timeout.as_secs()))]
    AcquireTimeout { pool_size: usize, timeout: Duration },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A pool of Quack sessions against one server, shared by every table built from it.
///
/// Each open scan holds one session until its stream is drained or dropped, so the pool
/// must be at least as large as the number of Quack scans a query runs at once. When the
/// pool is exhausted, [`DbConnectionPool::connect`] waits up to the acquire timeout and
/// then fails.
///
/// Joins are pushed down to the server only between tables that share one pool.
pub struct QuackConnectionPool {
    pool: QuackPool,
    acquire_timeout: Duration,
    join_context: String,
    unsupported_type_action: UnsupportedTypeAction,
}

impl std::fmt::Debug for QuackConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuackConnectionPool")
            .field("join_context", &self.join_context)
            .field("max_connections", &self.pool.max_connections())
            .field("acquire_timeout", &self.acquire_timeout)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .finish_non_exhaustive()
    }
}

impl QuackConnectionPool {
    /// Connects to a Quack server and opens a pool. One session is opened now, so a bad
    /// endpoint or token, or a server older than DuckDB 2.0 (Quack protocol v3), fails here.
    ///
    /// Parameters:
    /// - `endpoint` (required): server address, e.g. `quack:localhost:9494`, `localhost:9494`
    ///   or `http://localhost:9494`.
    /// - `token`: the server's auth token.
    /// - `ssl`: `true` or `false`; use HTTPS for addresses that don't name a scheme. The
    ///   certificate must be trusted by a CA; self-signed server certificates are not
    ///   supported.
    /// - `connection_pool_size`: sessions opened at most (default 4).
    /// - `connection_pool_acquire_timeout`: seconds to wait for a free session (default 30).
    ///
    /// # Errors
    ///
    /// Returns an error for a missing, unknown or invalid parameter, or if the first
    /// session can't be opened.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        if let Some(parameter) = params
            .keys()
            .find(|key| !PARAMETERS.contains(&key.as_str()))
        {
            return UnknownParameterSnafu {
                parameter: parameter.clone(),
            }
            .fail();
        }

        let endpoint = params
            .get(ENDPOINT)
            .context(MissingParameterSnafu {
                parameter: ENDPOINT,
            })?
            .expose_secret()
            .to_string();
        let ssl = parse_param(&params, SSL, "'true' or 'false'", |v| {
            v.parse::<bool>().ok()
        })?;
        let max_connections =
            parse_param(&params, CONNECTION_POOL_SIZE, "a positive integer", |v| {
                v.parse::<usize>().ok().filter(|n| *n > 0)
            })?
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let acquire_timeout = parse_param(
            &params,
            CONNECTION_POOL_ACQUIRE_TIMEOUT,
            "a positive number of seconds",
            |v| v.parse::<u64>().ok().filter(|n| *n > 0),
        )?
        .map_or(DEFAULT_ACQUIRE_TIMEOUT, Duration::from_secs);

        let options = QuackClientOptions {
            auth_token: params.get(TOKEN).map(|t| t.expose_secret().to_string()),
            ssl,
            min_supported_quack_version: Some(QUACK_PROTOCOL_VERSION),
            max_supported_quack_version: Some(QUACK_PROTOCOL_VERSION),
            ..Default::default()
        };
        let pool = QuackPool::connect(&endpoint, options, QuackPoolOptions { max_connections })
            .await
            .map_err(|e| Error::UnableToConnect {
                endpoint: endpoint.clone(),
                source: Box::new(e),
            })?;

        Ok(Self {
            pool,
            acquire_timeout,
            join_context: format!(
                "quack:pool-{}",
                NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed)
            ),
            unsupported_type_action: UnsupportedTypeAction::default(),
        })
    }

    /// Sets what schema discovery does with a column whose DuckDB type has no Arrow
    /// mapping. `String` is not supported and behaves like `Error`.
    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }
}

fn parse_param<T>(
    params: &HashMap<String, SecretString>,
    parameter: &'static str,
    expected: &'static str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Option<T>> {
    params
        .get(parameter)
        .map(|value| {
            let value = value.expose_secret();
            parse(value).context(InvalidParameterSnafu {
                parameter,
                value,
                expected,
            })
        })
        .transpose()
}

#[async_trait]
impl DbConnectionPool<QuackSession, ()> for QuackConnectionPool {
    async fn connect(&self) -> Result<Box<dyn DbConnection<QuackSession, ()>>, GenericError> {
        let lease = tokio::time::timeout(self.acquire_timeout, self.pool.acquire())
            .await
            .map_err(|_| Error::AcquireTimeout {
                pool_size: self.pool.max_connections(),
                timeout: self.acquire_timeout,
            })?
            .map_err(|e| Error::UnableToAcquire {
                source: Box::new(e),
            })?;

        Ok(Box::new(
            QuackConnection::new(QuackSession::new(lease))
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.join_context.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, SecretString> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), SecretString::from((*v).to_string())))
            .collect()
    }

    async fn new_error(pairs: &[(&str, &str)]) -> Error {
        match QuackConnectionPool::new(params(pairs)).await {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    #[tokio::test]
    async fn rejects_unknown_parameter() {
        let err = new_error(&[(ENDPOINT, "localhost:1"), ("tokn", "x")]).await;
        assert!(matches!(err, Error::UnknownParameter { ref parameter } if parameter == "tokn"));
        assert!(err.to_string().contains("connection_pool_size"));
    }

    #[tokio::test]
    async fn requires_endpoint() {
        let err = new_error(&[(TOKEN, "x")]).await;
        assert!(matches!(
            err,
            Error::MissingParameter {
                parameter: ENDPOINT
            }
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_values_instead_of_defaulting() {
        for (parameter, value) in [
            (SSL, "yes"),
            (CONNECTION_POOL_SIZE, "0"),
            (CONNECTION_POOL_SIZE, "-1"),
            (CONNECTION_POOL_ACQUIRE_TIMEOUT, "0"),
            (CONNECTION_POOL_ACQUIRE_TIMEOUT, "1.5"),
        ] {
            let err = new_error(&[(ENDPOINT, "localhost:1"), (parameter, value)]).await;
            assert!(
                matches!(err, Error::InvalidParameter { parameter: p, .. } if p == parameter),
                "{parameter}={value}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn connection_error_names_endpoint_but_not_token() {
        // Nothing listens on port 1, so the first session fails.
        let err = new_error(&[(ENDPOINT, "127.0.0.1:1"), (TOKEN, "super_secret")]).await;
        let message = err.to_string();
        assert!(matches!(err, Error::UnableToConnect { .. }), "{message}");
        assert!(message.contains("127.0.0.1:1"), "{message}");
        assert!(!message.contains("super_secret"), "{message}");
    }
}
