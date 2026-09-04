use datafusion_table_providers::sql::db_connection_pool::oraclepool::OracleConnectionPool;
use secrecy::SecretString;

use std::collections::HashMap;
use std::env;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

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

/// Probes the configured Oracle listener with a short TCP connect.
///
/// Returns `false` when no Oracle server is listening, so tests can skip
/// instead of failing in environments (like most CI jobs) that have no
/// Oracle instance available.
async fn listener_reachable() -> bool {
    let host = env::var("ORACLE_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = env::var("ORACLE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_ORACLE_PORT);

    let addrs = match (host.as_str(), port).to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(_) => return false,
    };

    for addr in addrs {
        if tokio::time::timeout(Duration::from_secs(3), tokio::net::TcpStream::connect(addr))
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

/// Waits for the configured Oracle listener to accept TCP connections.
///
/// The CI job starts gvenzl/oracle-free as its own step (with its own
/// health-wait loop) before invoking the tests, so here we only wait. Keeping
/// container startup out of the test process means the slow Oracle boot runs
/// against the step's own budget instead of the first test's.
async fn wait_for_listener(timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if listener_reachable().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            eprintln!("Oracle listener not reachable after {timeout:?}; skipping Oracle integration tests");
            return false;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Creates the shared test connection pool, or `None` when no Oracle is
/// available (the Oracle suite then skips rather than fails).
///
/// The whole attempt is memoized: with `--test-threads=1` every test calls
/// this, and only the first may pay the availability wait.
pub async fn get_oracle_connection_pool() -> Option<Arc<OracleConnectionPool>> {
    static POOL: tokio::sync::OnceCell<Option<Arc<OracleConnectionPool>>> =
        tokio::sync::OnceCell::const_new();

    POOL.get_or_init(|| async {
        if !wait_for_listener(Duration::from_secs(900)).await {
            return None;
        }
        let params = get_oracle_params();
        Some(Arc::new(
            OracleConnectionPool::new(params)
                .await
                .expect("Failed to create Oracle connection pool"),
        ))
    })
    .await
    .clone()
}
