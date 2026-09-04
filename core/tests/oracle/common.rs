use bollard::models::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::oraclepool::OracleConnectionPool;
use secrecy::SecretString;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};
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

static ORACLE_CONTAINER: tokio::sync::OnceCell<Option<RunningContainer>> =
    tokio::sync::OnceCell::const_new();

/// Starts an Oracle Free container (mirroring `start_clickhouse_docker_container`
/// from the ClickHouse suite) so the integration tests have a database. The
/// container is started once per test binary and intentionally left running —
/// the CI runner tears it down with the job.
///
/// Returns `false` only when Docker itself is unavailable (e.g. running the
/// suite on a host without a Docker daemon), in which case the tests skip
/// instead of failing.
async fn ensure_oracle_container() -> bool {
    ORACLE_CONTAINER
        .get_or_init(|| async {
            match start_oracle_docker_container().await {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("Failed to start Oracle container: {e:#}");
                    None
                }
            }
        })
        .await
        .is_some()
}

async fn start_oracle_docker_container() -> Result<RunningContainer, anyhow::Error> {
    let container_name = "runtime-integration-test-oracle";

    // Container startup is opt-in via ORACLE_DOCKER_IMAGE (set by CI), so
    // running the suite locally doesn't silently pull a multi-GB image.
    let oracle_docker_image = std::env::var("ORACLE_DOCKER_IMAGE")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("ORACLE_DOCKER_IMAGE not set; not starting an Oracle container")
        })?;

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(oracle_docker_image)
        .add_port_binding(1521, 1521)
        .add_env_var("ORACLE_PASSWORD", ORACLE_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "${ORACLE_BASE}/healthcheck.sh >/dev/null || exit 1".to_string(),
            ]),
            // Oracle Free can take a few minutes to open its listener on first boot.
            interval: Some(5_000_000_000),      // 5s
            timeout: Some(30_000_000_000),      // 30s
            retries: Some(60),                  // up to ~5 min
            start_period: Some(60_000_000_000), // 60s grace
            start_interval: None,
        })
        .health_timeout(Duration::from_secs(600))
        .build()?
        .run()
        .await?;

    Ok(running_container)
}

/// Creates the shared test connection pool, or `None` when neither an Oracle
/// listener nor a Docker daemon is available (the Oracle suite then skips
/// rather than fails).
/// Creates the shared test connection pool, or `None` when no Oracle is
/// available (the Oracle suite then skips rather than fails).
///
/// The whole attempt is memoized: with `--test-threads=1` every test calls
/// this, and only the first may pay the container-start/wait cost.
pub async fn get_oracle_connection_pool() -> Option<Arc<OracleConnectionPool>> {
    static POOL: tokio::sync::OnceCell<Option<Arc<OracleConnectionPool>>> =
        tokio::sync::OnceCell::const_new();

    POOL.get_or_init(|| async {
        if !listener_reachable().await {
            if !ensure_oracle_container().await {
                eprintln!("No Oracle available (no listener, no Docker); skipping Oracle integration tests");
                return None;
            }
            // Wait for the freshly-started listener to accept connections.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
            while !listener_reachable().await {
                if tokio::time::Instant::now() >= deadline {
                    eprintln!("Oracle listener never came up; skipping Oracle integration tests");
                    return None;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
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
