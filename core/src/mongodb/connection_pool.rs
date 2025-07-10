use std::{collections::HashMap, path::PathBuf, sync::Arc};

use mongodb::{
    bson::doc,
    options::{ClientOptions, ServerApi, ServerApiVersion, Tls, TlsOptions},
    Client, Database,
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

impl MongoDBConnectionPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let params = crate::util::remove_prefix_from_hashmap_keys(params, "mongodb_");

        // Build URI
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

        // Optional TLS
        if let Some(cert_path) = params.get("sslrootcert") {
            let path = PathBuf::from(cert_path.expose_secret());
            if !path.exists() {
                return Err(Error::InvalidRootCertPath {
                    path: cert_path.expose_secret().to_string(),
                });
            }

            let tls = Tls::Enabled(
                TlsOptions::builder()
                    .ca_file_path(Some(path))
                    .build(),
            );
            client_options.tls = Some(tls);
        }

        // Set ServerApi for compatibility with Atlas
        client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());

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

    pub fn client(&self) -> Arc<Client> {
        Arc::clone(&self.client)
    }

    pub fn database(&self) -> Database {
        self.client.database(&self.db_name)
    }

    pub async fn connect(&self) -> Result<Box<MongoDBConnection>> {
        Ok(Box::new(MongoDBConnection::new(
            Arc::clone(&self.client),
            self.db_name.clone(),
        )))
    }
}
