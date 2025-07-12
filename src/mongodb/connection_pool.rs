use std::{collections::HashMap, path::PathBuf, sync::Arc};
use mongodb::{
    bson::doc,
    options::{ClientOptions, Tls, TlsOptions},
    Client,
};
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::ResultExt;
use crate::mongodb::{connection::MongoDBConnection, ConnectionFailedSnafu, Error, InvalidUriSnafu, Result};

#[derive(Clone, Debug)]
pub struct MongoDBConnectionPool {
    client: Arc<Client>,
    db_name: String,
}

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: &str = "27017";
const DEFAULT_DATABASE : &str = "default";
const DEFAULT_MIN_POOL_SIZE: u32 = 10;
const DEFAULT_MAX_POOL_SIZE: u32 = 100;

impl MongoDBConnectionPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let params = crate::util::remove_prefix_from_hashmap_keys(params, "mongodb_");

        let uri = if let Some(uri) = params.get("connection_string") {
            uri.expose_secret().to_string()
        } else {
            let db_name = params
                .get("db")
                .map(SecretBox::expose_secret)
                .unwrap_or(DEFAULT_DATABASE);
            let host = params
                .get("host")
                .map(SecretBox::expose_secret)
                .unwrap_or(DEFAULT_HOST);
            let port = params
                .get("port")
                .map(SecretBox::expose_secret)
                .unwrap_or(DEFAULT_PORT);
            let user = params.get("user").map(SecretBox::expose_secret);
            let pass = params.get("pass").map(SecretBox::expose_secret);

            let auth = match (user, pass) {
                (Some(u), Some(p)) => format!("{}:{}@", u, p),
                _ => "".to_string(),
            };

            format!("mongodb://{}{}:{}/{}", auth, host, port, db_name)
        };

        let mut client_options = ClientOptions::parse(&uri)
            .await
            .context(InvalidUriSnafu)?;

        // Configure pool size
        let pool_min = params
            .get("pool_min")
            .map(SecretBox::expose_secret)
            .unwrap_or_default()
            .parse::<u32>()
            .unwrap_or(DEFAULT_MIN_POOL_SIZE);
        client_options.min_pool_size = Some(pool_min);

        let pool_max = params
            .get("pool_max")
            .map(SecretBox::expose_secret)
            .unwrap_or_default()
            .parse::<u32>()
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);
        client_options.min_pool_size = Some(pool_max);

        // Configure SSL + TLS
        let mut ssl_mode = "required";
        let mut ssl_rootcert_path: Option<PathBuf> = None;

        if let Some(mongo_sslmode) = params.get("sslmode").map(SecretBox::expose_secret) {
            match mongo_sslmode.to_lowercase().as_str() {
                "disabled" | "required" | "preferred" => {
                    ssl_mode = mongo_sslmode;
                }
                _ => {
                    return Err(Error::InvalidParameter {
                        parameter_name: "sslmode".to_string(),
                    });
                }
            }
        }

        if let Some(mongo_sslrootcert) = params.get("sslrootcert").map(SecretBox::expose_secret) {
            let path = PathBuf::from(mongo_sslrootcert);
            if !path.exists() {
                return Err(Error::InvalidRootCertPath {
                    path: mongo_sslrootcert.to_string(),
                });
            }
            ssl_rootcert_path = Some(path);
        }

        client_options.tls = get_tls_opts(ssl_mode, ssl_rootcert_path);

        let db_name = &client_options.default_database.as_ref().unwrap();

        let client = Client::with_options(client_options.clone()).context(ConnectionFailedSnafu)?;
        client
            .database(db_name)
            .run_command(doc! { "ping": 1 })
            .await
            .context(ConnectionFailedSnafu)?;

        Ok(Self {
            client: Arc::new(client),
            db_name: db_name.to_string(),
        })
    }

    pub async fn connect(&self) -> Result<Box<MongoDBConnection>> {
        Ok(Box::new(MongoDBConnection::new(
            Arc::clone(&self.client),
            self.db_name.clone(),
        )))
    }
}

fn get_tls_opts(ssl_mode: &str, rootcert_path: Option<PathBuf>) -> Option<Tls> {
    if ssl_mode == "disabled" {
        return Some(Tls::Disabled);
    }

    let tls_options = match (rootcert_path, ssl_mode) {
        // Root cert + preferred 
        (Some(path), "preferred") => TlsOptions::builder()
            .ca_file_path(Some(path))
            .allow_invalid_certificates(Some(true))
            .allow_invalid_hostnames(Some(true))
            .build(),
        
        // Root cert + required 
        (Some(path), _) => TlsOptions::builder()
            .ca_file_path(Some(path))
            .build(),
        
        // No root cert + preferred 
        (None, "preferred") => TlsOptions::builder()
            .allow_invalid_certificates(Some(true))
            .allow_invalid_hostnames(Some(true))
            .build(),
        
        // No root cert + required 
        (None, _) => TlsOptions::builder().build(),
    };

    Some(Tls::Enabled(tls_options))
}
