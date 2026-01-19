use datafusion_table_providers::sql::db_connection_pool::oraclepool::OracleConnectionPool;
use secrecy::SecretString;
use std::collections::HashMap;
use std::env;

const ORACLE_PASSWORD: &str = "password";
const ORACLE_USER: &str = "system";
const ORACLE_SERVICE: &str = "FREEPDB1";
const DEFAULT_ORACLE_PORT: u16 = 1521;

pub fn get_oracle_params() -> HashMap<String, SecretString> {
    let mut params = HashMap::new();

    // Default to strict env vars or defaults
    let host = env::var("ORACLE_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("ORACLE_PORT").unwrap_or_else(|_| DEFAULT_ORACLE_PORT.to_string());
    let user = env::var("ORACLE_USER").unwrap_or_else(|_| ORACLE_USER.to_string());
    let pass = env::var("ORACLE_PASSWORD").unwrap_or_else(|_| ORACLE_PASSWORD.to_string());
    let service = env::var("ORACLE_SERVICE").unwrap_or_else(|_| ORACLE_SERVICE.to_string());

    params.insert("host".to_string(), SecretString::from(host));
    params.insert("port".to_string(), SecretString::from(port));
    params.insert("user".to_string(), SecretString::from(user));
    params.insert("password".to_string(), SecretString::from(pass));
    params.insert("service_name".to_string(), SecretString::from(service));

    // Optional wallet params
    if let Ok(wallet) = env::var("ORACLE_WALLET_PATH") {
        params.insert("wallet_path".to_string(), SecretString::from(wallet));
    }
    if let Ok(wpass) = env::var("ORACLE_WALLET_PASSWORD") {
        params.insert("wallet_password".to_string(), SecretString::from(wpass));
    }

    params
}

pub async fn get_oracle_connection_pool() -> OracleConnectionPool {
    let params = get_oracle_params();
    OracleConnectionPool::new(params)
        .await
        .expect("Failed to create Oracle connection pool")
}
