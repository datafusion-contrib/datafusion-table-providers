use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use tiberius::{AuthMethod, Config, EncryptionLevel};

use datafusion_table_providers_common::sql::db_connection_pool::{
    dbconnection::{DbConnection, TableSchemas},
    DbConnectionPool, JoinPushDown,
};

use crate::conn::{MsSqlConnection, MsSqlParameter, MsSqlPooledConnection};

/// The default TCP port of SQL Server.
const DEFAULT_PORT: u16 = 1433;
const DEFAULT_POOL_MAX_SIZE: u32 = 10;
/// SQL Server error 18456: `Login failed for user`.
const LOGIN_FAILED: u32 = 18456;
/// SQL Server error 4060: `Cannot open database requested by the login`.
const CANNOT_OPEN_DATABASE: u32 = 4060;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid SQL Server connection string: {source}"))]
    InvalidConnectionString { source: tiberius::error::Error },

    #[snafu(display("Invalid value '{value}' for parameter '{parameter_name}'"))]
    InvalidParameter {
        parameter_name: String,
        value: String,
    },

    #[snafu(display("Missing required parameter: {parameter_name}"))]
    MissingParameter { parameter_name: String },

    #[snafu(display("Authentication failed. Verify the user name and password.\n{source}"))]
    InvalidUsernameOrPassword { source: bb8_tiberius::Error },

    #[snafu(display("Unable to open the SQL Server database. Ensure it exists and the user can access it.\n{source}"))]
    UnknownDatabase { source: bb8_tiberius::Error },

    #[snafu(display("Unable to connect to SQL Server: {source}"))]
    ConnectionError { source: bb8_tiberius::Error },

    #[snafu(display("Unable to get a SQL Server connection from the pool: {source}"))]
    PoolRunError {
        source: bb8::RunError<bb8_tiberius::Error>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A pool of connections to Microsoft SQL Server.
pub struct MsSqlConnectionPool {
    pool: Arc<bb8::Pool<bb8_tiberius::ConnectionManager>>,
    join_push_down: JoinPushDown,
    schemas: Arc<TableSchemas>,
}

impl std::fmt::Debug for MsSqlConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsSqlConnectionPool")
            .field("join_push_down", &self.join_push_down)
            .finish_non_exhaustive()
    }
}

fn param<'a>(params: &'a HashMap<String, SecretString>, name: &str) -> Option<&'a str> {
    params.get(name).map(ExposeSecret::expose_secret)
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" | "1" => Ok(true),
        "false" | "no" | "0" => Ok(false),
        _ => InvalidParameterSnafu {
            parameter_name: name,
            value,
        }
        .fail(),
    }
}

impl MsSqlConnectionPool {
    /// Creates a new pool of connections to SQL Server.
    ///
    /// # Arguments
    ///
    /// * `params` - The connection parameters. The `mssql_` prefix of the keys is optional.
    ///   * `connection_string` - An ADO.NET connection string, e.g.
    ///     `server=tcp:localhost,1433;user=sa;password=...;database=master;TrustServerCertificate=true`.
    ///     The following parameters override its values.
    ///   * `host` - The host of the server. Defaults to `localhost`.
    ///   * `port` - The TCP port of the server. Defaults to `1433`.
    ///   * `instance_name` - The name of a named instance, resolved through the SQL Browser.
    ///   * `database` (or `db`) - The database to connect to.
    ///   * `user` and `password` (or `pass`) - The SQL Server authentication credentials.
    ///   * `encrypt` - `true` (the default), `false` (only the login is encrypted), `off`
    ///     (nothing is encrypted) or `strict` (TDS 8.0).
    ///   * `trust_server_certificate` - Accept any server certificate. Defaults to `false`.
    ///   * `trust_cert_ca` - The path to a CA certificate to validate the server certificate with.
    ///   * `application_name` - The application name reported to the server.
    ///   * `pool_max` - The maximum number of connections in the pool. Defaults to `10`.
    ///   * `pool_min` - The number of idle connections the pool keeps open.
    ///
    /// # Errors
    ///
    /// Returns an error if the parameters are invalid or a connection can't be established.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let params = datafusion_table_providers_common::util::remove_prefix_from_hashmap_keys(
            params, "mssql_",
        );

        let mut database = None;
        let mut user = None;
        let mut config = match param(&params, "connection_string") {
            Some(connection_string) => {
                (database, user) = ado_database_and_user(connection_string);
                Config::from_ado_string(connection_string).context(InvalidConnectionStringSnafu)?
            }
            None => {
                let mut config = Config::new();
                config.host("localhost");
                config.port(DEFAULT_PORT);
                config.encryption(EncryptionLevel::Required);
                config
            }
        };

        if let Some(host) = param(&params, "host") {
            config.host(host);
        }
        if let Some(port) = param(&params, "port") {
            config.port(port.parse().map_err(|_| Error::InvalidParameter {
                parameter_name: "port".to_string(),
                value: port.to_string(),
            })?);
        }
        if let Some(instance_name) = param(&params, "instance_name") {
            config.instance_name(instance_name);
        }
        if let Some(db) = param(&params, "database").or_else(|| param(&params, "db")) {
            config.database(db);
            database = Some(db.to_string());
        }
        if let Some(application_name) = param(&params, "application_name") {
            config.application_name(application_name);
        }

        let password = param(&params, "password").or_else(|| param(&params, "pass"));
        match (param(&params, "user"), password) {
            (Some(u), Some(password)) => {
                config.authentication(AuthMethod::sql_server(u, password));
                user = Some(u.to_string());
            }
            (Some(_), None) => MissingParameterSnafu {
                parameter_name: "password",
            }
            .fail()?,
            (None, Some(_)) => MissingParameterSnafu {
                parameter_name: "user",
            }
            .fail()?,
            (None, None) => {}
        }

        if let Some(encrypt) = param(&params, "encrypt") {
            let level = match encrypt.to_ascii_lowercase().as_str() {
                "true" | "yes" | "required" | "mandatory" => EncryptionLevel::Required,
                "false" | "no" | "optional" => EncryptionLevel::Off,
                "off" | "disabled" | "not_supported" => EncryptionLevel::NotSupported,
                "strict" => EncryptionLevel::Strict,
                _ => InvalidParameterSnafu {
                    parameter_name: "encrypt",
                    value: encrypt,
                }
                .fail()?,
            };
            config.encryption(level);
        }
        if let Some(trust) = param(&params, "trust_server_certificate") {
            if parse_bool("trust_server_certificate", trust)? {
                config.trust_cert();
            }
        }
        if let Some(ca) = param(&params, "trust_cert_ca") {
            config.trust_cert_ca(ca);
        }

        let pool_max = match param(&params, "pool_max") {
            Some(max) => max.parse().map_err(|_| Error::InvalidParameter {
                parameter_name: "pool_max".to_string(),
                value: max.to_string(),
            })?,
            None => DEFAULT_POOL_MAX_SIZE,
        };
        let pool_min = match param(&params, "pool_min") {
            Some(min) => Some(min.parse::<u32>().map_err(|_| Error::InvalidParameter {
                parameter_name: "pool_min".to_string(),
                value: min.to_string(),
            })?),
            None => None,
        };

        let join_push_down = join_push_down_context(&config, database.as_deref(), user.as_deref());

        let mut manager = bb8_tiberius::ConnectionManager::new(config);
        if param(&params, "instance_name").is_some() {
            manager = manager.using_named_connection();
        }

        // Connect once to report a connection error now, with a helpful message
        let conn = bb8::ManageConnection::connect(&manager)
            .await
            .map_err(map_connection_error)?;
        drop(conn);

        let pool = bb8::Pool::builder()
            .max_size(pool_max)
            .min_idle(pool_min)
            .build(manager)
            .await
            .map_err(map_connection_error)?;

        Ok(Self {
            pool: Arc::new(pool),
            join_push_down,
            schemas: Arc::default(),
        })
    }

    /// Use the provided `schema` for `table_reference` instead of inferring it from the database.
    ///
    /// Call it once per table to provide the schemas of several tables. Table references are
    /// matched exactly, e.g. `users` and `dbo.users` are different tables.
    #[must_use]
    pub fn with_schema(
        mut self,
        table_reference: impl Into<TableReference>,
        schema: SchemaRef,
    ) -> Self {
        Arc::make_mut(&mut self.schemas).insert(table_reference.into(), schema);
        self
    }

    fn new_connection(&self, conn: MsSqlPooledConnection) -> MsSqlConnection {
        MsSqlConnection::new_with_schemas(conn, Arc::clone(&self.schemas))
    }

    /// Returns a connection to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if a connection can't be established.
    pub async fn connect_direct(&self) -> Result<MsSqlConnection> {
        let conn = self.pool.get_owned().await.context(PoolRunSnafu)?;
        Ok(self.new_connection(conn))
    }
}

fn map_connection_error(e: bb8_tiberius::Error) -> Error {
    if let bb8_tiberius::Error::Tiberius(tiberius::error::Error::Server(token)) = &e {
        match token.code() {
            LOGIN_FAILED => return Error::InvalidUsernameOrPassword { source: e },
            CANNOT_OPEN_DATABASE => return Error::UnknownDatabase { source: e },
            _ => {}
        }
    }
    Error::ConnectionError { source: e }
}

/// Joins can be pushed down between the tables of pools that connect to the same server and
/// database as the same user.
fn join_push_down_context(
    config: &Config,
    database: Option<&str>,
    user: Option<&str>,
) -> JoinPushDown {
    let mut context = format!("addr={}", config.get_addr());
    if let Some(database) = database {
        context.push_str(&format!(",db={database}"));
    }
    if let Some(user) = user {
        context.push_str(&format!(",user={user}"));
    }
    JoinPushDown::AllowedFor(context)
}

/// The database and the user of an ADO.NET connection string, which [`Config`] doesn't expose.
fn ado_database_and_user(connection_string: &str) -> (Option<String>, Option<String>) {
    let mut database = None;
    let mut user = None;
    for part in connection_string.split(';') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        let value = value
            .trim()
            .trim_matches(|c| c == '\'' || c == '"')
            .to_string();
        match key.trim().to_ascii_lowercase().as_str() {
            "database" | "initial catalog" => database = Some(value),
            "user" | "user id" | "uid" | "username" => user = Some(value),
            _ => {}
        }
    }
    (database, user)
}

#[async_trait]
impl DbConnectionPool<MsSqlPooledConnection, MsSqlParameter> for MsSqlConnectionPool {
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<dyn DbConnection<MsSqlPooledConnection, MsSqlParameter>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let conn = self.pool.get_owned().await.context(PoolRunSnafu)?;
        Ok(Box::new(self.new_connection(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_and_user_of_connection_string() {
        assert_eq!(
            ado_database_and_user(
                "server=tcp:localhost,1433;User Id=sa;Password=x;Initial Catalog='db1'"
            ),
            (Some("db1".to_string()), Some("sa".to_string()))
        );
        assert_eq!(ado_database_and_user("server=localhost"), (None, None));
    }
}
