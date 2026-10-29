use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::CqlValue;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;

use crate::conn::CassandraConnection;
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::TableSchemas;
use datafusion_table_providers_common::sql::db_connection_pool::{
    dbconnection::{AsyncDbConnection, DbConnection},
    DbConnectionPool, JoinPushDown,
};

type PoolResult<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u16 = 9042;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to Cassandra: {source}"))]
    Connection {
        source: Box<scylla::errors::NewSessionError>,
    },

    #[snafu(display("Invalid value '{value}' for parameter '{parameter_name}'"))]
    InvalidParameter {
        parameter_name: String,
        value: String,
    },

    #[snafu(display(
        "Parameter '{parameter_name}' requires parameter '{required}' to be set too"
    ))]
    MissingParameter {
        parameter_name: String,
        required: String,
    },
}

/// The Cassandra connection settings, parsed from the pool parameters.
#[derive(Debug, PartialEq)]
struct CassandraConfig {
    /// Contact points as `host:port`.
    known_nodes: Vec<String>,
    keyspace: Option<String>,
    credentials: Option<(String, String)>,
    local_datacenter: Option<String>,
    connection_timeout: Option<Duration>,
}

impl CassandraConfig {
    fn from_params(params: &HashMap<String, SecretString>) -> Result<Self, Error> {
        let param = |name: &str| {
            params
                .get(name)
                .map(|v| v.expose_secret().trim().to_string())
                .filter(|v| !v.is_empty())
        };

        let port = match param("port") {
            Some(port) => port.parse::<u16>().map_err(|_| Error::InvalidParameter {
                parameter_name: "port".to_string(),
                value: port,
            })?,
            None => DEFAULT_PORT,
        };

        let hosts = param("hosts")
            .or_else(|| param("host"))
            .unwrap_or_else(|| DEFAULT_HOST.to_string());
        let known_nodes = hosts
            .split(',')
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .map(|host| with_default_port(host, port))
            .collect();

        let credentials = match (param("user"), param("password")) {
            (Some(user), Some(password)) => Some((user, password)),
            (None, None) => None,
            (Some(_), None) => {
                return MissingParameterSnafu {
                    parameter_name: "user",
                    required: "password",
                }
                .fail()
            }
            (None, Some(_)) => {
                return MissingParameterSnafu {
                    parameter_name: "password",
                    required: "user",
                }
                .fail()
            }
        };

        let connection_timeout = match param("connection_timeout_ms") {
            Some(timeout) => Some(Duration::from_millis(timeout.parse::<u64>().map_err(
                |_| Error::InvalidParameter {
                    parameter_name: "connection_timeout_ms".to_string(),
                    value: timeout,
                },
            )?)),
            None => None,
        };

        Ok(Self {
            known_nodes,
            keyspace: param("keyspace"),
            credentials,
            local_datacenter: param("local_datacenter"),
            connection_timeout,
        })
    }

    fn session_builder(&self) -> SessionBuilder {
        let mut builder = SessionBuilder::new().known_nodes(&self.known_nodes);
        if let Some(keyspace) = &self.keyspace {
            builder = builder.use_keyspace(keyspace, true);
        }
        if let Some((user, password)) = &self.credentials {
            builder = builder.user(user, password);
        }
        if let Some(datacenter) = &self.local_datacenter {
            builder = builder.prefer_datacenter(datacenter.clone());
        }
        if let Some(timeout) = self.connection_timeout {
            builder = builder.connection_timeout(timeout);
        }
        builder
    }
}

/// Appends `port` to `host` unless it already has one. IPv6 addresses must be given in
/// brackets, e.g. `[::1]:9042`, to carry a port.
fn with_default_port(host: &str, port: u16) -> String {
    let has_port = match host.rsplit_once(':') {
        Some((address, port)) => {
            port.parse::<u16>().is_ok() && (!address.contains(':') || address.ends_with(']'))
        }
        None => false,
    };
    if has_port {
        host.to_string()
    } else if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// A connection pool for Apache Cassandra (and other CQL-compatible databases).
///
/// The underlying driver [`Session`] maintains connections to every node of the cluster, so the
/// pool shares one session among all its connections.
#[derive(Clone)]
pub struct CassandraConnectionPool {
    session: Arc<Session>,
    join_push_down: JoinPushDown,
    schemas: Arc<TableSchemas>,
}

impl std::fmt::Debug for CassandraConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraConnectionPool")
            .field("join_push_down", &self.join_push_down)
            .field("schemas", &self.schemas)
            .finish_non_exhaustive()
    }
}

impl CassandraConnectionPool {
    /// Creates a new `CassandraConnectionPool`.
    ///
    /// # Arguments
    ///
    /// * `params` - The connection parameters:
    ///   * `hosts` - Comma-separated contact points, as `host` or `host:port`. Defaults to `localhost`.
    ///     `host` is accepted as an alias.
    ///   * `port` - The port of contact points that don't specify one. Defaults to `9042`.
    ///   * `keyspace` - The keyspace that unqualified table names are resolved in.
    ///   * `user` and `password` - Credentials for password authentication.
    ///   * `local_datacenter` - The datacenter to prefer when routing requests.
    ///   * `connection_timeout_ms` - The timeout for establishing connections.
    ///
    /// # Errors
    ///
    /// Returns an error if the parameters are invalid or the cluster cannot be reached.
    pub async fn new(params: HashMap<String, SecretString>) -> PoolResult<Self> {
        let config = CassandraConfig::from_params(&params)?;
        let session = config
            .session_builder()
            .build()
            .await
            .map_err(|e| Error::Connection {
                source: Box::new(e),
            })?;

        Ok(Self::from_session(Arc::new(session)))
    }

    /// Creates a pool from an existing driver session.
    #[must_use]
    pub fn from_session(session: Arc<Session>) -> Self {
        // Joins can't be pushed down: CQL has no joins.
        Self {
            session,
            join_push_down: JoinPushDown::Disallow,
            schemas: Default::default(),
        }
    }

    /// Use the provided `schema` for `table_reference` instead of inferring it from the database.
    ///
    /// Call it once per table to provide the schemas of several tables. Table references are
    /// matched exactly, e.g. `users` and `public.users` are different tables.
    #[must_use]
    pub fn with_schema(
        mut self,
        table_reference: impl Into<TableReference>,
        schema: SchemaRef,
    ) -> Self {
        Arc::make_mut(&mut self.schemas).insert(table_reference.into(), schema);
        self
    }

    /// Returns the driver session.
    #[must_use]
    pub fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    /// Returns a direct connection to the underlying database.
    #[must_use]
    pub fn connect_direct(&self) -> CassandraConnection {
        CassandraConnection::new_with_schemas(self.session(), Arc::clone(&self.schemas))
    }
}

#[async_trait]
impl DbConnectionPool<Arc<Session>, CqlValue> for CassandraConnectionPool {
    async fn connect(&self) -> PoolResult<Box<dyn DbConnection<Arc<Session>, CqlValue>>> {
        Ok(Box::new(self.connect_direct()))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, SecretString> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string())))
            .collect()
    }

    #[test]
    fn uses_defaults() {
        let config = CassandraConfig::from_params(&HashMap::new()).unwrap();
        assert_eq!(
            config,
            CassandraConfig {
                known_nodes: vec!["localhost:9042".to_string()],
                keyspace: None,
                credentials: None,
                local_datacenter: None,
                connection_timeout: None,
            }
        );
    }

    #[test]
    fn parses_all_params() {
        let config = CassandraConfig::from_params(&params(&[
            ("hosts", "node1, node2:9043,10.0.0.1"),
            ("port", "19042"),
            ("keyspace", "ks"),
            ("user", "cassandra"),
            ("password", "secret"),
            ("local_datacenter", "dc1"),
            ("connection_timeout_ms", "1500"),
        ]))
        .unwrap();
        assert_eq!(
            config,
            CassandraConfig {
                known_nodes: vec![
                    "node1:19042".to_string(),
                    "node2:9043".to_string(),
                    "10.0.0.1:19042".to_string(),
                ],
                keyspace: Some("ks".to_string()),
                credentials: Some(("cassandra".to_string(), "secret".to_string())),
                local_datacenter: Some("dc1".to_string()),
                connection_timeout: Some(Duration::from_millis(1500)),
            }
        );
    }

    #[test]
    fn accepts_host_alias() {
        let config = CassandraConfig::from_params(&params(&[("host", "db")])).unwrap();
        assert_eq!(config.known_nodes, vec!["db:9042".to_string()]);
    }

    #[test]
    fn rejects_invalid_params() {
        assert!(CassandraConfig::from_params(&params(&[("port", "abc")])).is_err());
        assert!(CassandraConfig::from_params(&params(&[("connection_timeout_ms", "-1")])).is_err());
        assert!(CassandraConfig::from_params(&params(&[("user", "cassandra")])).is_err());
        assert!(CassandraConfig::from_params(&params(&[("password", "secret")])).is_err());
    }

    #[test]
    fn adds_default_port() {
        assert_eq!(with_default_port("db", 9042), "db:9042");
        assert_eq!(with_default_port("db:9043", 9042), "db:9043");
        assert_eq!(with_default_port("::1", 9042), "[::1]:9042");
        assert_eq!(with_default_port("[::1]", 9042), "[::1]:9042");
        assert_eq!(with_default_port("[::1]:9043", 9042), "[::1]:9043");
    }
}
