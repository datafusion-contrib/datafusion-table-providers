use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use mysql_async::{
    prelude::{Queryable, ToValue},
    DriverError, Opts, Params, Row, SslOpts,
};
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::{ResultExt, Snafu};

use crate::{
    sql::db_connection_pool::{
        dbconnection::{mysqlconn::MySQLConnection, AsyncDbConnection, DbConnection},
        JoinPushDown,
    },
    util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect},
};

use super::DbConnectionPool;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("MySQL connection error: {source}"))]
    MySQLConnectionError { source: mysql_async::Error },

    #[snafu(display("Invalid MySQL connection string: {source} "))]
    InvalidConnectionString { source: mysql_async::UrlError },

    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Invalid root cert path: {path}"))]
    InvalidRootCertPathError { path: String },

    #[snafu(display("Cannot connect to MySQL on {host}:{port}. Ensure that the host and port are correctly configured, and that the host is reachable."))]
    InvalidHostOrPortError {
        source: crate::util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display(
        "Authentication failed. Ensure that the username and password are correctly configured."
    ))]
    InvalidUsernameOrPassword,

    #[snafu(display("{message}"))]
    UnknownMySQLDatabase { message: String },
}

#[derive(Debug)]
pub struct MySQLConnectionPool {
    pool: Arc<mysql_async::Pool>,
    join_push_down: JoinPushDown,
}

impl MySQLConnectionPool {
    /// Creates a new instance of `MySQLConnectionPool`.
    ///
    /// # Arguments
    ///
    /// * `params` - A map of parameters to create the connection pool.
    ///   * `connection_string` - The connection string to use to connect to the MySQL database, or can be specified with the below individual parameters.
    ///   * `host` - The host of the MySQL database.
    ///   * `user` - The user to use when connecting to the MySQL database.
    ///   * `db` - The database to connect to.
    ///   * `pass` - The password to use when connecting to the MySQL database.
    ///   * `tcp_port` - The TCP port to use when connecting to the MySQL database.
    ///   * `sslmode` - The SSL mode to use when connecting to the MySQL database. Can be "disabled", "required", or "preferred".
    ///   * `sslrootcert` - The path to the root certificate to use when connecting to the MySQL database.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::unused_async)]
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        // Remove the "mysql_" prefix from the keys to keep backward compatibility
        let params = util::remove_prefix_from_hashmap_keys(params, "mysql_");

        let mut connection_string = mysql_async::OptsBuilder::default();
        let mut ssl_mode = "required";
        let mut ssl_rootcert_path: Option<PathBuf> = None;

        if let Some(mysql_connection_string) =
            params.get("connection_string").map(Secret::expose_secret)
        {
            connection_string = mysql_async::OptsBuilder::from_opts(
                mysql_async::Opts::from_url(mysql_connection_string.as_str())
                    .context(InvalidConnectionStringSnafu)?,
            );
        } else {
            if let Some(mysql_host) = params.get("host").map(Secret::expose_secret) {
                connection_string = connection_string.ip_or_hostname(mysql_host.as_str());
            }
            if let Some(mysql_user) = params.get("user").map(Secret::expose_secret) {
                connection_string = connection_string.user(Some(mysql_user));
            }
            if let Some(mysql_db) = params.get("db").map(Secret::expose_secret) {
                connection_string = connection_string.db_name(Some(mysql_db));
            }
            if let Some(mysql_pass) = params.get("pass").map(Secret::expose_secret) {
                connection_string = connection_string.pass(Some(mysql_pass));
            }
            if let Some(mysql_tcp_port) = params.get("tcp_port").map(Secret::expose_secret) {
                connection_string =
                    connection_string.tcp_port(mysql_tcp_port.parse::<u16>().unwrap_or(3306));
            }
        }

        if let Some(mysql_sslmode) = params.get("sslmode").map(Secret::expose_secret) {
            match mysql_sslmode.to_lowercase().as_str() {
                "disabled" | "required" | "preferred" => {
                    ssl_mode = mysql_sslmode.as_str();
                }
                _ => {
                    InvalidParameterSnafu {
                        parameter_name: "sslmode".to_string(),
                    }
                    .fail()?;
                }
            }
        }
        if let Some(mysql_sslrootcert) = params.get("sslrootcert").map(Secret::expose_secret) {
            if !std::path::Path::new(mysql_sslrootcert).exists() {
                InvalidRootCertPathSnafu {
                    path: mysql_sslrootcert,
                }
                .fail()?;
            }

            ssl_rootcert_path = Some(PathBuf::from(mysql_sslrootcert));
        }

        let ssl_opts = get_ssl_opts(ssl_mode, ssl_rootcert_path);

        connection_string = connection_string.ssl_opts(ssl_opts);

        let opts = mysql_async::Opts::from(connection_string);

        verify_mysql_opts(&opts).await?;

        let join_push_down = get_join_context(&opts);

        let pool = mysql_async::Pool::new(opts);

        // Test the connection
        let mut conn = pool.get_conn().await.map_err(|err| match err {
            // In case of an incorrect user name, the error `Unknown authentication plugin 'sha256_password'` is returned.
            // We override it with a more user-friendly error message.
            mysql_async::Error::Driver(DriverError::UnknownAuthPlugin { .. }) => {
                Error::InvalidUsernameOrPassword
            }
            mysql_async::Error::Server(server_error) => {
                match server_error.code {
                    // Code 1049: Server error: `ERROR 42000 (1049): Unknown database <database>
                    1049 => Error::UnknownMySQLDatabase {
                        message: server_error.message,
                    },
                    // Code 1045: Server error: ERROR 1045 (28000): Access denied for user <user> (using password: YES / NO)
                    1045 => Error::InvalidUsernameOrPassword,
                    _ => Error::MySQLConnectionError {
                        source: mysql_async::Error::Server(server_error),
                    },
                }
            }
            _ => Error::MySQLConnectionError { source: err },
        })?;

        let _rows: Vec<Row> = conn
            .exec("SELECT 1", Params::Empty)
            .await
            .context(MySQLConnectionSnafu)?;

        Ok(Self {
            pool: Arc::new(pool),
            join_push_down,
        })
    }

    /// Returns a direct connection to the underlying database.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn connect_direct(&self) -> super::Result<MySQLConnection> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_conn().await.context(MySQLConnectionSnafu)?;
        Ok(MySQLConnection::new(conn))
    }
}

async fn verify_mysql_opts(opts: &Opts) -> Result<()> {
    // Verify the host and port are correct
    let host = opts.ip_or_hostname();
    let port = opts.tcp_port();

    verify_ns_lookup_and_tcp_connect(host, port)
        .await
        .context(InvalidHostOrPortSnafu { host, port })?;

    Ok(())
}

fn get_join_context(opts: &mysql_async::Opts) -> JoinPushDown {
    let mut join_context = format!("host={},port={}", opts.ip_or_hostname(), opts.tcp_port());
    if let Some(db_name) = opts.db_name() {
        join_context.push_str(&format!(",db={db_name}"));
    }
    if let Some(user) = opts.user() {
        join_context.push_str(&format!(",user={user}"));
    }

    JoinPushDown::AllowedFor(join_context)
}

fn get_ssl_opts(ssl_mode: &str, rootcert_path: Option<PathBuf>) -> Option<SslOpts> {
    if ssl_mode == "disabled" {
        return None;
    }

    let mut opts = SslOpts::default();

    if let Some(rootcert_path) = rootcert_path {
        let path = rootcert_path;
        opts = opts.with_root_certs(vec![path.into()]);
    }

    // If ssl_mode is "preferred", we will accept invalid certs and skip domain validation
    // mysql_async does not have a "ssl_mode" https://github.com/blackbeam/mysql_async/issues/225#issuecomment-1409922237
    if ssl_mode == "preferred" {
        opts = opts
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }

    Some(opts)
}

#[async_trait]
impl DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> for MySQLConnectionPool {
    async fn connect(
        &self,
    ) -> super::Result<Box<dyn DbConnection<mysql_async::Conn, &'static (dyn ToValue + Sync)>>>
    {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_conn().await.context(MySQLConnectionSnafu)?;
        Ok(Box::new(MySQLConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
