use super::DbConnectionPool;
use crate::sql::db_connection_pool::dbconnection::trinoconn::DEFAULT_POLL_WAIT_TIME_MS;
use crate::{
    sql::db_connection_pool::{
        dbconnection::{trinoconn::TrinoConnection, DbConnection},
        JoinPushDown,
    },
    util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect},
    UnsupportedTypeAction,
};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Certificate, Client, ClientBuilder, Identity};
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
use std::path::PathBuf;
use std::{collections::HashMap, fs, sync::Arc, time::Duration};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Trino connection failed.\n{source}\nFor details, refer to the Trino documentation: https://trino.io/docs/"))]
    TrinoConnectionError { source: reqwest::Error },

    #[snafu(display("Could not parse {parameter_name} into a valid integer. Ensure it is configured with a valid value."))]
    InvalidIntegerParameterError {
        parameter_name: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Cannot connect to Trino on {host}:{port}. Ensure the host and port are correct and reachable."))]
    InvalidHostOrPortError {
        source: util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display("Authentication failed."))]
    AuthenticationFailedError,

    #[snafu(display("Invalid Trino URL: {url}. Ensure it starts with http:// or https://"))]
    InvalidTrinoUrl { url: String },

    #[snafu(display(
        "Invalid sslmode: {value}. Expected values are: required, preferred, disabled"
    ))]
    InvalidSSLModeParameter { value: String },

    #[snafu(display("Missing required parameter: {parameter_name}"))]
    MissingRequiredParameter { parameter_name: String },

    #[snafu(display("Failed to build HTTP client: {source}"))]
    FailedToBuildTrinoHttpClient { source: reqwest::Error },

    #[snafu(display("Trino server error: {status_code} - {message}"))]
    TrinoServerError { status_code: u16, message: String },

    #[snafu(display("Invalid Trino authentication configuration: {details}"))]
    InvalidAuthConfig { details: String },

    #[snafu(display("Failed to read identity PEM file at '{}': {}", path, source))]
    UnableToReadIdentityPem {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Invalid identity PEM at '{}': {}", path, source))]
    InvalidIdentityPem {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Failed to read root cert file at '{path}': {source}"))]
    UnableToReadRootCert {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Invalid root cert at '{path}': {source}"))]
    InvalidRootCert {
        path: String,
        source: reqwest::Error,
    },
}

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_SSL_MODE: &str = "required";

#[derive(Clone)]
pub struct TrinoConnectionPool {
    base_url: String,
    catalog: String,
    schema: String,
    client: Arc<Client>,
    join_push_down: JoinPushDown,
    unsupported_type_action: UnsupportedTypeAction,
    poll_wait_time: Duration,
    tz: Option<String>,
}

impl std::fmt::Debug for TrinoConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoConnectionPool")
            .field("base_url", &self.base_url)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("join_push_down", &self.join_push_down)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .finish()
    }
}

impl TrinoConnectionPool {
    /// Creates a new instance of `TrinoConnectionPool`.
    ///
    /// # Arguments
    ///
    /// * `params` - A map of parameters to create the connection pool.
    ///   * `host` - The Trino coordinator host (required)
    ///   * `port` - The Trino coordinator port (optional, defaults to 8080)
    ///   * `catalog` - The default catalog to use (required)
    ///   * `schema` - The default schema to use (optional, defaults to "default")
    ///   * `user` - The user to authenticate with (required)
    ///   * `password` - The password for authentication (optional)
    ///   * `timeout_ms` - Request timeout in ms (optional, defaults to 300)
    ///   * `sslmode` - TLS/SSL mode for the connection. Supported values: 'disabled', 'required', 'preferred'. Defaults to 'required'. 'preferred' allows invalid certificates/hostnames.
    ///   * `identity_pem_path` - Path to a PEM file containing both the client certificate and private key for mTLS authentication. (optional)
    ///   * `bearer_token` - Bearer token for authentication (optional)
    ///   * `poll_wait_time_ms` - Waiting time in ms between polling trino results (optional, defaults to 50)
    ///   * `time_zone` - The time zone to use for the MySQL connection (e.g., "+2:00", "UTC", etc.). Default is "+00:00" (UTC).
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let params = util::remove_prefix_from_hashmap_keys(params, "trino_");

        let (catalog, schema) = get_catalog_and_schema(&params)?;
        let (user, password) = get_user_and_password(&params);
        let bearer_token = params.get("bearer_token").cloned();

        validate_auth(&params, &user, &password)?;

        let headers = build_headers(&catalog, &schema, &user, &password, &bearer_token)?;

        let timeout_ms = parse_u64_param(&params, "timeout_ms", DEFAULT_TIMEOUT_MS)?;
        let poll_wait_time =
            parse_u64_param(&params, "poll_wait_time_ms", DEFAULT_POLL_WAIT_TIME_MS)?;

        let client_builder = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_millis(timeout_ms));

        let (mut client_builder, protocol) = configure_tls(client_builder, &params)?;

        if let Some(identity_path) = params.get("identity_pem_path") {
            let pem =
                fs::read(identity_path.expose_secret()).context(UnableToReadIdentityPemSnafu {
                    path: identity_path.expose_secret().to_string(),
                })?;

            let identity = Identity::from_pem(&pem).context(InvalidIdentityPemSnafu {
                path: identity_path.expose_secret().to_string(),
            })?;
            client_builder = client_builder.identity(identity);
        }

        let base_url = build_base_url(protocol, &params)?;

        let client = client_builder
            .build()
            .context(FailedToBuildTrinoHttpClientSnafu)?;

        Self::test_connection(&client, &base_url).await?;

        let join_push_down = Self::get_join_context(&base_url, &catalog, &schema, &user);

        Ok(Self {
            base_url,
            catalog,
            schema,
            client: Arc::new(client),
            join_push_down,
            unsupported_type_action: UnsupportedTypeAction::default(),
            poll_wait_time: Duration::from_millis(poll_wait_time),
            tz: params
                .get("time_zone")
                .map(|t| t.expose_secret().to_string()),
        })
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    async fn test_connection(client: &Client, base_url: &str) -> Result<()> {
        let url = format!("{base_url}/v1/info");

        let response = client
            .get(&url)
            .send()
            .await
            .context(TrinoConnectionSnafu)?;

        if response.status() == 401 {
            return Err(Error::AuthenticationFailedError);
        }

        if !response.status().is_success() {
            return Err(Error::TrinoServerError {
                status_code: response.status().as_u16(),
                message: format!("Connection test failed with HTTP {}", response.status()),
            });
        }

        Ok(())
    }

    fn get_join_context(
        base_url: &str,
        catalog: &str,
        schema: &str,
        user: &Option<String>,
    ) -> JoinPushDown {
        let mut join_context = format!("url={base_url},catalog={catalog},schema={schema}");
        if let Some(user) = user {
            join_context.push_str(&format!(",user={user}"));
        }

        JoinPushDown::AllowedFor(join_context)
    }
}

#[async_trait]
impl DbConnectionPool<Arc<Client>, &'static str> for TrinoConnectionPool {
    async fn connect(&self) -> super::Result<Box<dyn DbConnection<Arc<Client>, &'static str>>> {
        let connection = TrinoConnection::new_with_config(
            self.client.clone(),
            self.base_url.clone(),
            self.poll_wait_time,
            self.tz.clone(),
        )
        .with_unsupported_type_action(self.unsupported_type_action);

        Ok(Box::new(connection))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn build_base_url(protocol: String, params: &HashMap<String, SecretString>) -> Result<String> {
    let host = params
        .get("host")
        .map(ExposeSecret::expose_secret)
        .ok_or_else(|| Error::MissingRequiredParameter {
            parameter_name: "host".to_string(),
        })?;

    let port = parse_u16_param(params, "port", 8080)?;
    futures::executor::block_on(verify_ns_lookup_and_tcp_connect(host, port))
        .context(InvalidHostOrPortSnafu { host, port })?;

    Ok(format!("{protocol}://{host}:{port}"))
}

fn get_catalog_and_schema(params: &HashMap<String, SecretString>) -> Result<(String, String)> {
    let catalog = params
        .get("catalog")
        .map(ExposeSecret::expose_secret)
        .ok_or_else(|| Error::MissingRequiredParameter {
            parameter_name: "catalog".to_string(),
        })?
        .to_string();

    let schema = params
        .get("schema")
        .map(ExposeSecret::expose_secret)
        .unwrap_or("default")
        .to_string();

    Ok((catalog, schema))
}

fn get_user_and_password(
    params: &HashMap<String, SecretString>,
) -> (Option<String>, Option<SecretString>) {
    let user = params.get("user").map(|u| u.expose_secret().to_string());
    let password = params.get("password").cloned();
    (user, password)
}

fn validate_auth(
    params: &HashMap<String, SecretString>,
    user: &Option<String>,
    password: &Option<SecretString>,
) -> Result<()> {
    let has_user = user.is_some();
    let has_user_pass = user.is_some() && password.is_some();
    let has_identity = params.contains_key("identity_pem_path");
    let has_token = params.contains_key("bearer_token");

    if !has_user {
        return Err(Error::InvalidAuthConfig {
            details: "User is required".into(),
        });
    }

    let auth_count = [has_user_pass, has_identity, has_token]
        .into_iter()
        .filter(|x| *x)
        .count();

    if auth_count > 1 {
        return Err(Error::InvalidAuthConfig {
            details: "At most one authentication method must be provided: basic auth, mTLS, or bearer token".into(),
        });
    }
    Ok(())
}

fn configure_tls(
    client_builder: ClientBuilder,
    params: &HashMap<String, SecretString>,
) -> Result<(ClientBuilder, String)> {
    let ssl_mode = params
        .get("sslmode")
        .map(ExposeSecret::expose_secret)
        .unwrap_or(DEFAULT_SSL_MODE)
        .to_string()
        .to_lowercase();

    if ssl_mode == "disabled" {
        return Ok((client_builder, "http".to_string()));
    }

    match ssl_mode.as_str() {
        "disabled" | "required" | "preferred" => {}
        _ => {
            return Err(Error::InvalidSSLModeParameter {
                value: ssl_mode.to_string(),
            });
        }
    }

    let ssl_rootcert = if let Some(cert_path) = params.get("sslrootcert") {
        let path = PathBuf::from(cert_path.expose_secret());
        let ca_cert = fs::read(path).context(UnableToReadRootCertSnafu {
            path: cert_path.expose_secret().to_string(),
        })?;
        Some(
            Certificate::from_pem(&ca_cert).context(InvalidRootCertSnafu {
                path: cert_path.expose_secret().to_string(),
            })?,
        )
    } else {
        None
    };

    let client_builder = match (ssl_rootcert, ssl_mode.as_str()) {
        // Root cert + preferred
        (Some(cert), "preferred") => client_builder
            .add_root_certificate(cert)
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true),

        // Root cert + required
        (Some(cert), _) => client_builder.add_root_certificate(cert),

        // No root cert + preferred
        (None, "preferred") => client_builder
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true),

        // No root cert + required
        (None, _) => client_builder,
    };

    Ok((client_builder, "https".to_string()))
}

fn build_headers(
    catalog: &str,
    schema: &str,
    user: &Option<String>,
    password: &Option<SecretString>,
    bearer_token: &Option<SecretString>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert("X-Trino-Catalog", catalog.parse().unwrap());
    headers.insert("X-Trino-Schema", schema.parse().unwrap());

    if let Some(user) = user {
        headers.insert("X-Trino-User", user.parse().unwrap());
    }

    if let (Some(user), Some(password)) = (user, password) {
        let credentials = format!("{}:{}", user, password.expose_secret());
        let encoded = BASE64.encode(credentials);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
    } else if let Some(token) = bearer_token {
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.expose_secret())).unwrap(),
        );
    }

    Ok(headers)
}

fn parse_u64_param(params: &HashMap<String, SecretString>, key: &str, default: u64) -> Result<u64> {
    params
        .get(key)
        .map(ExposeSecret::expose_secret)
        .unwrap_or(&default.to_string())
        .parse::<u64>()
        .context(InvalidIntegerParameterSnafu {
            parameter_name: key,
        })
}

fn parse_u16_param(params: &HashMap<String, SecretString>, key: &str, default: u16) -> Result<u16> {
    params
        .get(key)
        .map(ExposeSecret::expose_secret)
        .unwrap_or(&default.to_string())
        .parse::<u16>()
        .context(InvalidIntegerParameterSnafu {
            parameter_name: key,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;
    use secrecy::SecretString;
    use std::collections::HashMap;

    use tempfile::NamedTempFile;

    fn create_basic_params() -> HashMap<String, SecretString> {
        let mut params = HashMap::new();
        params.insert(
            "catalog".to_string(),
            SecretString::new("test_catalog".into()),
        );
        params.insert(
            "schema".to_string(),
            SecretString::new("test_schema".into()),
        );
        params
    }

    fn create_mock_pem_file() -> NamedTempFile {
        let pem_content = r#"-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKLy2PtfxYqjANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJV
UzAeFw0yMzEwMDEwMDAwMDBaFw0yNDA5MzAyMzU5NTlaMA0xCzAJBgNVBAYTAlVT
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyJ3yfgDHc...
-----END CERTIFICATE-----
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDInfJ+AMdz...
-----END PRIVATE KEY-----"#;

        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        std::io::Write::write_all(&mut file, pem_content.as_bytes())
            .expect("Failed to write to temp file");
        file
    }

    #[tokio::test]
    async fn test_new_with_url_basic_auth() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/info")
            .with_status(200)
            .with_body(r#"{"nodeVersion":{"version":"1.0"}}"#)
            .create_async()
            .await;

        let mut params = create_basic_params();
        params.insert(
            "host".to_string(),
            SecretString::new(server.socket_address().ip().to_string().into()),
        );
        params.insert(
            "port".to_string(),
            SecretString::new(server.socket_address().port().to_string().into()),
        );
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert("user".to_string(), SecretString::new("testuser".into()));
        params.insert("password".to_string(), SecretString::new("testpass".into()));

        let pool = TrinoConnectionPool::new(params).await;
        assert!(pool.is_ok());

        let pool = pool.unwrap();
        assert_eq!(pool.base_url, server.url());
        assert_eq!(pool.catalog, "test_catalog");
        assert_eq!(pool.schema, "test_schema");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_new_with_bearer_token() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/info")
            .with_status(200)
            .with_header("Authorization", "Bearer test-token-123")
            .with_body(r#"{"nodeVersion":{"version":"1.0"}}"#)
            .create_async()
            .await;

        let mut params = create_basic_params();
        params.insert(
            "host".to_string(),
            SecretString::new(server.socket_address().ip().to_string().into()),
        );
        params.insert(
            "port".to_string(),
            SecretString::new(server.socket_address().port().to_string().into()),
        );
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert(
            "bearer_token".to_string(),
            SecretString::new("test-token-123".into()),
        );
        params.insert("user".to_string(), SecretString::new("testuser".into()));

        let pool = TrinoConnectionPool::new(params).await;
        assert!(pool.is_ok());

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_new_with_host_port() {
        let mut server = Server::new_async().await;
        let url = server.url();
        let url_parts: Vec<&str> = url.split(':').collect();
        let host = url_parts[1].trim_start_matches("//");
        let port: u16 = url_parts[2].parse().unwrap();

        let mock = server
            .mock("GET", "/v1/info")
            .with_status(200)
            .with_body(r#"{"nodeVersion":{"version":"1.0"}}"#)
            .create_async()
            .await;

        let mut params = create_basic_params();
        params.insert(
            "host".to_string(),
            SecretString::new(server.socket_address().ip().to_string().into()),
        );
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert(
            "port".to_string(),
            SecretString::new(port.to_string().into()),
        );
        params.insert("user".to_string(), SecretString::new("testuser".into()));

        let pool = TrinoConnectionPool::new(params)
            .await
            .expect("Failed to create TrinoConnectionPool");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_new_missing_catalog() {
        let mut params = HashMap::new();
        params.insert("host".to_string(), SecretString::new("localhost".into()));
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));

        let result = TrinoConnectionPool::new(params).await;
        assert!(result.is_err());

        if let Err(Error::MissingRequiredParameter { parameter_name }) = result {
            assert_eq!(parameter_name, "catalog");
        } else {
            panic!("Expected MissingRequiredParameter error for catalog");
        }
    }

    #[tokio::test]
    async fn test_new_missing_host() {
        let mut params = create_basic_params();
        params.insert("user".to_string(), SecretString::new("testuser".into()));

        let result = TrinoConnectionPool::new(params).await;
        assert!(result.is_err());

        if let Err(Error::MissingRequiredParameter { parameter_name }) = result {
            assert_eq!(parameter_name, "host");
        } else {
            panic!("Expected MissingRequiredParameter error for url or host");
        }
    }

    #[tokio::test]
    async fn test_authentication_failed() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/info")
            .with_status(401)
            .create_async()
            .await;

        let mut params = create_basic_params();
        params.insert(
            "host".to_string(),
            SecretString::new(server.socket_address().ip().to_string().into()),
        );
        params.insert(
            "port".to_string(),
            SecretString::new(server.socket_address().port().to_string().into()),
        );
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert("user".to_string(), SecretString::new("baduser".into()));
        params.insert("password".to_string(), SecretString::new("badpass".into()));

        let result = TrinoConnectionPool::new(params).await;
        assert!(result.is_err());

        if let Err(Error::AuthenticationFailedError) = result {
            // Expected
        } else {
            panic!("Expected AuthenticationFailedError");
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_server_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/info")
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await;

        let mut params = create_basic_params();
        params.insert(
            "host".to_string(),
            SecretString::new(server.socket_address().ip().to_string().into()),
        );
        params.insert(
            "port".to_string(),
            SecretString::new(server.socket_address().port().to_string().into()),
        );
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert("user".to_string(), SecretString::new("testuser".into()));

        let result = TrinoConnectionPool::new(params).await;
        assert!(result.is_err());

        if let Err(Error::TrinoServerError { status_code, .. }) = result {
            assert_eq!(status_code, 500);
        } else {
            panic!("Expected TrinoServerError");
        }

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_multiple_auth_methods_error() {
        let mut params = create_basic_params();
        params.insert(
            "url".to_string(),
            SecretString::new("http://localhost:8080".into()),
        );
        params.insert("host".to_string(), SecretString::new("localhost".into()));
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert("user".to_string(), SecretString::new("testuser".into()));
        params.insert("password".to_string(), SecretString::new("testpass".into()));
        params.insert(
            "bearer_token".to_string(),
            SecretString::new("token123".into()),
        );

        let result = TrinoConnectionPool::new(params).await;
        assert!(result.is_err());

        if let Err(Error::InvalidAuthConfig { details }) = result {
            assert!(details.contains("At most one authentication method"));
        } else {
            panic!("Expected InvalidAuthConfig error");
        }
    }

    #[tokio::test]
    async fn test_no_auth_method_allowed() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/info")
            .with_status(200)
            .with_body(r#"{"nodeVersion":{"version":"1.0"}}"#)
            .create_async()
            .await;

        let mut params = create_basic_params();
        params.insert(
            "host".to_string(),
            SecretString::new(server.socket_address().ip().to_string().into()),
        );
        params.insert(
            "port".to_string(),
            SecretString::new(server.socket_address().port().to_string().into()),
        );
        params.insert("sslmode".to_string(), SecretString::new("disabled".into()));
        params.insert("user".to_string(), SecretString::new("testuser".into()));

        let result = TrinoConnectionPool::new(params).await;
        assert!(result.is_ok());

        mock.assert_async().await;
    }

    #[test]
    fn test_build_headers_basic_auth() {
        let user = Some("testuser".to_string());
        let password = Some(SecretString::new("testpass".into()));
        let bearer_token = None;

        let headers = build_headers("catalog", "schema", &user, &password, &bearer_token).unwrap();

        assert_eq!(headers.get("X-Trino-Catalog").unwrap(), "catalog");
        assert_eq!(headers.get("X-Trino-Schema").unwrap(), "schema");
        assert_eq!(headers.get("X-Trino-User").unwrap(), "testuser");

        let auth_header = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth_header.starts_with("Basic "));

        // Decode and verify the basic auth
        let encoded = auth_header.strip_prefix("Basic ").unwrap();
        let decoded = String::from_utf8(BASE64.decode(encoded).unwrap()).unwrap();
        assert_eq!(decoded, "testuser:testpass");
    }

    #[test]
    fn test_build_headers_bearer_token() {
        let user = None;
        let password = None;
        let bearer_token = Some(SecretString::new("test-token-123".into()));

        let headers = build_headers("catalog", "schema", &user, &password, &bearer_token).unwrap();

        assert_eq!(headers.get("X-Trino-Catalog").unwrap(), "catalog");
        assert_eq!(headers.get("X-Trino-Schema").unwrap(), "schema");
        assert!(headers.get("X-Trino-User").is_none());

        let auth_header = headers.get("Authorization").unwrap().to_str().unwrap();
        assert_eq!(auth_header, "Bearer test-token-123");
    }

    #[test]
    fn test_build_headers_no_auth() {
        let user = None;
        let password = None;
        let bearer_token = None;

        let headers = build_headers("catalog", "schema", &user, &password, &bearer_token).unwrap();

        assert_eq!(headers.get("X-Trino-Catalog").unwrap(), "catalog");
        assert_eq!(headers.get("X-Trino-Schema").unwrap(), "schema");
        assert!(headers.get("X-Trino-User").is_none());
        assert!(headers.get("Authorization").is_none());
    }

    #[test]
    fn test_parse_parameters() {
        let mut params = HashMap::new();
        params.insert("timeout".to_string(), SecretString::new("120".into()));
        params.insert("port".to_string(), SecretString::new("9080".into()));
        params.insert(
            "ssl_verification".to_string(),
            SecretString::new("false".into()),
        );

        assert_eq!(parse_u64_param(&params, "timeout", 300).unwrap(), 120);
        assert_eq!(parse_u16_param(&params, "port", 8080).unwrap(), 9080);

        // Test defaults
        assert_eq!(parse_u64_param(&params, "nonexistent", 300).unwrap(), 300);
        assert_eq!(parse_u16_param(&params, "nonexistent", 8080).unwrap(), 8080);
    }

    #[test]
    fn test_parse_invalid_parameters() {
        let mut params = HashMap::new();
        params.insert("timeout".to_string(), SecretString::new("invalid".into()));
        params.insert("port".to_string(), SecretString::new("99999".into())); // Too large for u16

        assert!(parse_u64_param(&params, "timeout", 300).is_err());
        assert!(parse_u16_param(&params, "port", 8080).is_err());
    }

    #[test]
    fn test_validate_auth() {
        // Test valid cases
        let mut params = HashMap::new();
        let user = Some("user".to_string());
        let password = Some(SecretString::new("pass".into()));
        assert!(validate_auth(&params, &user, &password).is_ok());

        let password = None;
        params.insert(
            "bearer_token".to_string(),
            SecretString::new("token".into()),
        );
        assert!(validate_auth(&params, &user, &password).is_ok());

        // Test invalid case - multiple auth methods
        let user = Some("user".to_string());
        let password = Some(SecretString::new("pass".into()));
        assert!(validate_auth(&params, &user, &password).is_err());

        // User is required
        let user = None;
        let password = None;
        params.insert(
            "bearer_token".to_string(),
            SecretString::new("token".into()),
        );
        assert!(validate_auth(&params, &user, &password).is_err());
    }

    #[test]
    fn test_get_catalog_and_schema() {
        let mut params = HashMap::new();
        params.insert("catalog".to_string(), SecretString::new("test_cat".into()));
        params.insert(
            "schema".to_string(),
            SecretString::new("test_schema".into()),
        );

        let (catalog, schema) = get_catalog_and_schema(&params).unwrap();
        assert_eq!(catalog, "test_cat");
        assert_eq!(schema, "test_schema");

        params.remove("schema");
        let (catalog, schema) = get_catalog_and_schema(&params).unwrap();
        assert_eq!(catalog, "test_cat");
        assert_eq!(schema, "default");
    }

    #[test]
    fn test_get_user_and_password() {
        let mut params = HashMap::new();
        params.insert("user".to_string(), SecretString::new("testuser".into()));
        params.insert("password".to_string(), SecretString::new("testpass".into()));

        let (user, password) = get_user_and_password(&params);
        assert_eq!(user, Some("testuser".to_string()));
        assert!(password.is_some());
    }
}
