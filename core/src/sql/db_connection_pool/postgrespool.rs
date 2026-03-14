use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use crate::{
    util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect},
    UnsupportedTypeAction,
};
use async_trait::async_trait;
use bb8::ErrorSink;
use bb8_postgres::tokio_postgres::{config::Host, types::ToSql, Config};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::{prelude::*, ResultExt};
use tokio::runtime::Handle;
use tokio_postgres;

use super::{
    runtime::run_async_with_tokio, DbConnectionPool, PasswordProvider, StaticPasswordProvider,
};
use crate::sql::db_connection_pool::{
    dbconnection::{postgresconn::PostgresConnection, AsyncDbConnection, DbConnection},
    JoinPushDown,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgreSQL connection failed.\n{source}\nFor details, refer to the PostgreSQL documentation: https://www.postgresql.org/docs/17/index.html"))]
    ConnectionPoolError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("PostgreSQL connection failed.\n{source}\nAdjust the connection pool parameters for sufficient capacity."))]
    ConnectionPoolRunError {
        source: bb8::RunError<bb8_postgres::tokio_postgres::Error>,
    },

    #[snafu(display(
        "Invalid parameter: {parameter_name}. Ensure the parameter name is correct."
    ))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Could not parse {parameter_name} into a valid integer. Ensure it is configured with a valid value."))]
    InvalidIntegerParameterError {
        parameter_name: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Cannot connect to PostgreSQL on {host}:{port}. Ensure the host and port are correct and reachable."))]
    InvalidHostOrPortError {
        source: crate::util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display(
        "Invalid root certificate path: {path}. Ensure it points to a valid root certificate."
    ))]
    InvalidRootCertPathError { path: String },

    #[snafu(display(
        "Failed to read certificate.\n{source}\nEnsure the root certificate path points to a valid certificate."
    ))]
    FailedToReadCertError { source: std::io::Error },

    #[snafu(display(
        "Certificate loading failed.\n{source}\nEnsure the root certificate path points to a valid certificate."
    ))]
    FailedToLoadCertError { source: native_tls::Error },

    #[snafu(display("TLS connector initialization failed.\n{source}\nVerify SSL mode and root certificate validity"))]
    FailedToBuildTlsConnectorError { source: native_tls::Error },

    #[snafu(display("PostgreSQL connection failed.\n{source}\nFor details, refer to the PostgreSQL documentation: https://www.postgresql.org/docs/17/index.html"))]
    PostgresConnectionError { source: tokio_postgres::Error },

    #[snafu(display("Authentication failed. Verify username and password."))]
    InvalidUsernameOrPassword { source: tokio_postgres::Error },

    #[snafu(display("Password provider error.\n{source}"))]
    PasswordProviderError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Task failed to execute on IO runtime.\n{source}"))]
    IoRuntimeError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for the connection manager, covering both Postgres and password provider errors.
#[derive(Debug)]
pub enum ConnectionManagerError {
    /// An error from the underlying Postgres connection.
    Postgres(tokio_postgres::Error),
    /// An error from the password provider.
    PasswordProvider(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for ConnectionManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres(e) => write!(f, "{e}"),
            Self::PasswordProvider(e) => write!(f, "password provider error: {e}"),
        }
    }
}

impl std::error::Error for ConnectionManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postgres(e) => Some(e),
            Self::PasswordProvider(e) => Some(e.as_ref()),
        }
    }
}

impl From<tokio_postgres::Error> for ConnectionManagerError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Postgres(e)
    }
}

/// A bb8 connection manager that supports dynamic password providers.
///
/// When a [`PasswordProvider`] is set, the manager calls it to get a fresh password
/// each time a new connection is created. This enables rotating credentials,
/// JWT-based auth, and cloud IAM authentication.
///
/// When no provider is set (passwordless auth), the manager connects using the
/// stored [`Config`] as-is.
pub struct ConnectionManager {
    config: Config,
    tls: MakeTlsConnector,
    password_provider: Option<Arc<dyn PasswordProvider>>,
}

impl ConnectionManager {
    fn new(config: Config, tls: MakeTlsConnector) -> Self {
        Self {
            config,
            tls,
            password_provider: None,
        }
    }

    fn with_password_provider(mut self, provider: Arc<dyn PasswordProvider>) -> Self {
        self.password_provider = Some(provider);
        self
    }
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = tokio_postgres::Client;
    type Error = ConnectionManagerError;

    async fn connect(&self) -> std::result::Result<tokio_postgres::Client, ConnectionManagerError> {
        let (client, connection) = if let Some(provider) = &self.password_provider {
            let password = provider
                .get_password()
                .await
                .map_err(ConnectionManagerError::PasswordProvider)?;
            let mut config = self.config.clone();
            config.password(password.expose_secret());
            config.connect(self.tls.clone()).await?
        } else {
            self.config.connect(self.tls.clone()).await?
        };
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!("postgres connection error: {e}");
            }
        });
        Ok(client)
    }

    async fn is_valid(
        &self,
        conn: &mut tokio_postgres::Client,
    ) -> std::result::Result<(), ConnectionManagerError> {
        conn.simple_query("").await.map(|_| ())?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut tokio_postgres::Client) -> bool {
        conn.is_closed()
    }
}

#[derive(Debug)]
pub struct PostgresConnectionPool {
    pool: Arc<bb8::Pool<ConnectionManager>>,
    join_push_down: JoinPushDown,
    unsupported_type_action: UnsupportedTypeAction,
    io_handle: Option<Handle>,
}

impl PostgresConnectionPool {
    /// Creates a new instance of `PostgresConnectionPool`.
    ///
    /// If a `pass` parameter is present, it is wrapped in a [`StaticPasswordProvider`]
    /// internally. For dynamic credentials, use [`new_with_password_provider`](Self::new_with_password_provider).
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        Self::new_inner(params, None).await
    }

    /// Creates a new instance of `PostgresConnectionPool` with a dynamic password provider.
    ///
    /// The password provider is called each time a new connection is created in the pool,
    /// enabling support for rotating credentials, JWT tokens, and cloud IAM authentication.
    ///
    /// Any `pass` parameter in `params` is ignored; the provider is used instead.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new_with_password_provider(
        params: HashMap<String, SecretString>,
        password_provider: Arc<dyn PasswordProvider>,
    ) -> Result<Self> {
        Self::new_inner(params, Some(password_provider)).await
    }

    async fn new_inner(
        params: HashMap<String, SecretString>,
        password_provider: Option<Arc<dyn PasswordProvider>>,
    ) -> Result<Self> {
        // Remove the "pg_" prefix from the keys to keep backward compatibility
        let params = util::remove_prefix_from_hashmap_keys(params, "pg_");

        let mut connection_string = String::new();
        let mut ssl_mode = "verify-full".to_string();
        let mut ssl_rootcert_path: Option<PathBuf> = None;
        let mut static_password: Option<SecretString> = None;

        if let Some(pg_connection_string) = params
            .get("connection_string")
            .map(SecretBox::expose_secret)
        {
            let (str, mode, cert_path, password) = parse_connection_string(pg_connection_string);
            connection_string = str;
            ssl_mode = mode;
            if password_provider.is_none() {
                static_password = password.map(SecretString::from);
            }
            if let Some(cert_path) = cert_path {
                let sslrootcert = cert_path.as_str();
                ensure!(
                    std::path::Path::new(sslrootcert).exists(),
                    InvalidRootCertPathSnafu { path: cert_path }
                );
                ssl_rootcert_path = Some(PathBuf::from(sslrootcert));
            }
        } else {
            if let Some(pg_host) = params.get("host").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("host={pg_host} ").as_str());
            }
            if let Some(pg_user) = params.get("user").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("user={pg_user} ").as_str());
            }
            if let Some(pg_db) = params.get("db").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("dbname={pg_db} ").as_str());
            }
            if password_provider.is_none() {
                if let Some(pg_pass) = params.get("pass") {
                    static_password = Some(pg_pass.clone());
                }
            }
            if let Some(pg_port) = params.get("port").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("port={pg_port} ").as_str());
            }
        }

        if let Some(pg_sslmode) = params.get("sslmode").map(SecretBox::expose_secret) {
            match pg_sslmode.to_lowercase().as_str() {
                "disable" | "require" | "prefer" | "verify-ca" | "verify-full" => {
                    ssl_mode = pg_sslmode.to_string();
                }
                _ => {
                    InvalidParameterSnafu {
                        parameter_name: "sslmode".to_string(),
                    }
                    .fail()?;
                }
            }
        }
        if let Some(pg_sslrootcert) = params.get("sslrootcert").map(SecretBox::expose_secret) {
            ensure!(
                std::path::Path::new(pg_sslrootcert).exists(),
                InvalidRootCertPathSnafu {
                    path: pg_sslrootcert,
                }
            );

            ssl_rootcert_path = Some(PathBuf::from(pg_sslrootcert));
        }

        let mode = match ssl_mode.as_str() {
            "disable" => "disable",
            "prefer" => "prefer",
            // tokio_postgres supports only disable, require and prefer
            _ => "require",
        };

        // Password is never included in the connection string — it flows
        // through the PasswordProvider on each connection instead.
        connection_string.push_str(format!("sslmode={mode} ").as_str());
        let mut config =
            Config::from_str(connection_string.as_str()).context(ConnectionPoolSnafu)?;

        if let Some(application_name) = params.get("application_name").map(SecretBox::expose_secret)
        {
            config.application_name(application_name);
        }

        verify_postgres_config(&config).await?;

        let mut certs: Option<Vec<Certificate>> = None;

        if let Some(path) = ssl_rootcert_path {
            let buf = tokio::fs::read(path).await.context(FailedToReadCertSnafu)?;
            certs = Some(parse_certs(&buf)?);
        }

        let tls_connector = get_tls_connector(ssl_mode.as_str(), certs)?;
        let connector = MakeTlsConnector::new(tls_connector);

        // Resolve the password provider: use the caller's, wrap the static password,
        // or leave as None for passwordless auth (trust, cert, etc.).
        let password_provider = password_provider.or_else(|| {
            static_password
                .map(|pw| Arc::new(StaticPasswordProvider::new(pw)) as Arc<dyn PasswordProvider>)
        });

        // Test the connection
        if let Some(ref provider) = password_provider {
            let password = provider
                .get_password()
                .await
                .map_err(|source| Error::PasswordProviderError { source })?;
            let mut test_config = config.clone();
            test_config.password(password.expose_secret());
            test_connection(&test_config, connector.clone()).await?;
        } else {
            test_connection(&config, connector.clone()).await?;
        }

        let join_push_down = get_join_context(&config);

        let mut manager = ConnectionManager::new(config, connector);
        if let Some(provider) = password_provider {
            manager = manager.with_password_provider(provider);
        }
        let error_sink = PostgresErrorSink::new();

        let mut connection_pool_size = 10; // The BB8 default is 10
        if let Some(pg_pool_size) = params
            .get("connection_pool_size")
            .map(SecretBox::expose_secret)
        {
            connection_pool_size = pg_pool_size.parse().context(InvalidIntegerParameterSnafu {
                parameter_name: "pool_size".to_string(),
            })?;
        }

        let pool = bb8::Pool::builder()
            .max_size(connection_pool_size)
            .error_sink(Box::new(error_sink))
            .build(manager)
            .await
            .map_err(map_pool_build_error)?;

        // Verify the pool by executing a simple query
        {
            let conn = pool.get().await.map_err(map_pool_run_error)?;
            conn.execute("SELECT 1", &[])
                .await
                .context(ConnectionPoolSnafu)?;
        }

        Ok(PostgresConnectionPool {
            pool: Arc::new(pool),
            join_push_down,
            unsupported_type_action: UnsupportedTypeAction::default(),
            io_handle: None,
        })
    }

    /// Specify the action to take when an invalid type is encountered.
    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    /// Route all Postgres connection background tasks to a dedicated IO runtime.
    #[must_use]
    pub fn with_io_runtime(mut self, handle: Handle) -> Self {
        self.io_handle = Some(handle);
        self
    }

    /// Returns a direct connection to the underlying database.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn connect_direct(&self) -> super::Result<PostgresConnection> {
        let pool = Arc::clone(&self.pool);
        let conn = if let Some(handle) = &self.io_handle {
            handle
                .spawn(async move { pool.get_owned().await.map_err(map_pool_run_error) })
                .await
                .context(IoRuntimeSnafu)??
        } else {
            pool.get_owned().await.map_err(map_pool_run_error)?
        };
        Ok(PostgresConnection::new(conn))
    }
}

/// Parses a connection string into components, extracting `sslmode`, `sslrootcert`,
/// and `password` separately so they can be handled by the caller.
fn parse_connection_string(
    pg_connection_string: &str,
) -> (String, String, Option<String>, Option<String>) {
    let mut connection_string = String::new();
    let mut ssl_mode = "verify-full".to_string();
    let mut ssl_rootcert_path: Option<String> = None;
    let mut password: Option<String> = None;

    let str_params: Vec<&str> = pg_connection_string.split_whitespace().collect();
    for param in str_params {
        let param = param.split('=').collect::<Vec<&str>>();
        if let (Some(&name), Some(&value)) = (param.first(), param.get(1)) {
            match name {
                "sslmode" => {
                    ssl_mode = value.to_string();
                }
                "sslrootcert" => {
                    ssl_rootcert_path = Some(value.to_string());
                }
                "password" => {
                    password = Some(value.to_string());
                }
                _ => {
                    connection_string.push_str(format!("{name}={value} ").as_str());
                }
            }
        }
    }

    (connection_string, ssl_mode, ssl_rootcert_path, password)
}

fn get_join_context(config: &Config) -> JoinPushDown {
    let mut join_push_context_str = String::new();
    for host in config.get_hosts() {
        join_push_context_str.push_str(&format!("host={host:?},"));
    }
    if !config.get_ports().is_empty() {
        join_push_context_str.push_str(&format!("port={port},", port = config.get_ports()[0]));
    }
    if let Some(dbname) = config.get_dbname() {
        join_push_context_str.push_str(&format!("db={dbname},"));
    }
    if let Some(user) = config.get_user() {
        join_push_context_str.push_str(&format!("user={user},"));
    }

    JoinPushDown::AllowedFor(join_push_context_str)
}

/// Classifies a connection error, returning a more specific error variant for
/// authentication failures.
fn classify_connection_error(err: tokio_postgres::Error) -> Error {
    if let Some(code) = err.code() {
        if *code == tokio_postgres::error::SqlState::INVALID_PASSWORD {
            return Error::InvalidUsernameOrPassword { source: err };
        }
    }
    Error::PostgresConnectionError { source: err }
}

async fn test_connection(config: &Config, connector: MakeTlsConnector) -> Result<()> {
    config
        .connect(connector)
        .await
        .map(|_| ())
        .map_err(classify_connection_error)
}

fn map_pool_build_error(e: ConnectionManagerError) -> Error {
    match e {
        ConnectionManagerError::Postgres(e) => Error::ConnectionPoolError { source: e },
        ConnectionManagerError::PasswordProvider(e) => Error::PasswordProviderError { source: e },
    }
}

fn map_pool_run_error(e: bb8::RunError<ConnectionManagerError>) -> Error {
    match e {
        bb8::RunError::User(ConnectionManagerError::Postgres(e)) => Error::ConnectionPoolRunError {
            source: bb8::RunError::User(e),
        },
        bb8::RunError::User(ConnectionManagerError::PasswordProvider(e)) => {
            Error::PasswordProviderError { source: e }
        }
        bb8::RunError::TimedOut => Error::ConnectionPoolRunError {
            source: bb8::RunError::TimedOut,
        },
    }
}

async fn verify_postgres_config(config: &Config) -> Result<()> {
    for host in config.get_hosts() {
        for port in config.get_ports() {
            if let Host::Tcp(host) = host {
                verify_ns_lookup_and_tcp_connect(host, *port)
                    .await
                    .context(InvalidHostOrPortSnafu { host, port: *port })?;
            }
        }
    }

    Ok(())
}

fn get_tls_connector(ssl_mode: &str, rootcerts: Option<Vec<Certificate>>) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    if ssl_mode == "disable" {
        return builder.build().context(FailedToBuildTlsConnectorSnafu);
    }

    if let Some(certs) = rootcerts {
        for cert in certs {
            builder.add_root_certificate(cert);
        }
    }

    builder
        .danger_accept_invalid_hostnames(ssl_mode != "verify-full")
        .danger_accept_invalid_certs(ssl_mode != "verify-full" && ssl_mode != "verify-ca")
        .build()
        .context(FailedToBuildTlsConnectorSnafu)
}

fn parse_certs(buf: &[u8]) -> Result<Vec<Certificate>> {
    Certificate::from_der(buf)
        .map(|x| vec![x])
        .or_else(|_| {
            pem::parse_many(buf)
                .unwrap_or_default()
                .iter()
                .map(pem::encode)
                .map(|s| Certificate::from_pem(s.as_bytes()))
                .collect()
        })
        .context(FailedToLoadCertSnafu)
}

#[derive(Debug, Clone, Copy)]
struct PostgresErrorSink {}

impl PostgresErrorSink {
    pub fn new() -> Self {
        PostgresErrorSink {}
    }
}

impl<E> ErrorSink<E> for PostgresErrorSink
where
    E: std::fmt::Debug,
    E: std::fmt::Display,
{
    fn sink(&self, error: E) {
        tracing::debug!("Postgres Pool Error: {}", error);
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<E>> {
        Box::new(*self)
    }
}

#[async_trait]
impl
    DbConnectionPool<bb8::PooledConnection<'static, ConnectionManager>, &'static (dyn ToSql + Sync)>
    for PostgresConnectionPool
{
    async fn connect(
        &self,
    ) -> super::Result<
        Box<
            dyn DbConnection<
                bb8::PooledConnection<'static, ConnectionManager>,
                &'static (dyn ToSql + Sync),
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = if let Some(handle) = &self.io_handle {
            handle
                .spawn(async move { pool.get_owned().await.map_err(map_pool_run_error) })
                .await
                .context(IoRuntimeSnafu)??
        } else {
            let get_conn = async || pool.get_owned().await.map_err(map_pool_run_error);
            run_async_with_tokio(get_conn).await?
        };
        Ok(Box::new(
            PostgresConnection::new(conn)
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[tokio::test]
    async fn static_password_provider_returns_password() {
        let provider = StaticPasswordProvider::new(SecretString::from("hunter2".to_string()));
        let password = provider.get_password().await.unwrap();
        assert_eq!(password.expose_secret(), "hunter2");
    }

    #[test]
    fn connection_manager_error_display() {
        let err = ConnectionManagerError::PasswordProvider("token expired".into());
        assert_eq!(err.to_string(), "password provider error: token expired");
    }

    #[test]
    fn connection_manager_error_implements_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(ConnectionManagerError::PasswordProvider("fail".into()));
        assert!(err.source().is_some());
    }

    #[test]
    fn parse_connection_string_extracts_password() {
        let (conn_str, ssl_mode, cert_path, password) = parse_connection_string(
            "host=localhost user=postgres password=secret dbname=mydb sslmode=disable",
        );
        assert_eq!(conn_str.trim(), "host=localhost user=postgres dbname=mydb");
        assert_eq!(ssl_mode, "disable");
        assert!(cert_path.is_none());
        assert_eq!(password.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_connection_string_without_password() {
        let (conn_str, _ssl_mode, _cert_path, password) =
            parse_connection_string("host=localhost user=postgres dbname=mydb");
        assert_eq!(conn_str.trim(), "host=localhost user=postgres dbname=mydb");
        assert!(password.is_none());
    }
}
