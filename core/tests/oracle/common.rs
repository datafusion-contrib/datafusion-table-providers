use bollard::models::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::oraclepool::OracleConnectionPool;
use secrecy::SecretString;
use tracing::instrument;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const ORACLE_PASSWORD: &str = "integration-test-pw";
const ORACLE_USER: &str = "system";
const ORACLE_SERVICE: &str = "FREEPDB1";
const ORACLE_DOCKER_CONTAINER: &str = "runtime-integration-test-oracle";

pub(super) fn get_oracle_params(port: u16) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert("port".to_string(), SecretString::from(port.to_string()));
    params.insert(
        "user".to_string(),
        SecretString::from(ORACLE_USER.to_string()),
    );
    params.insert(
        "password".to_string(),
        SecretString::from(ORACLE_PASSWORD.to_string()),
    );
    params.insert(
        "service_name".to_string(),
        SecretString::from(ORACLE_SERVICE.to_string()),
    );
    params
}

#[instrument]
pub(super) async fn start_oracle_docker_container(
    port: u16,
) -> Result<RunningContainer, anyhow::Error> {
    let container_name = format!("{ORACLE_DOCKER_CONTAINER}-{port}");

    // gvenzl/oracle-free is not mirrored in the default (ECR public library)
    // registry, so it is pulled from Docker Hub unless overridden.
    let oracle_docker_image = std::env::var("ORACLE_DOCKER_IMAGE")
        .unwrap_or_else(|_| "gvenzl/oracle-free:latest".to_string());

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(oracle_docker_image)
        .add_port_binding(1521, port)
        .add_env_var("ORACLE_PASSWORD", ORACLE_PASSWORD)
        .healthcheck(HealthConfig {
            // The image ships `healthcheck.sh` (on its PATH) but no Docker
            // HEALTHCHECK; it succeeds once FREEPDB1 is open for READ WRITE.
            test: Some(vec!["CMD".to_string(), "healthcheck.sh".to_string()]),
            interval: Some(5_000_000_000), // 5s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(10),
            start_period: Some(30_000_000_000),  // 30s
            start_interval: Some(1_000_000_000), // 1s
        })
        // A cold boot can take several minutes on CI runners.
        .health_timeout(Duration::from_secs(900))
        .build()?
        .run()
        .await?;

    Ok(running_container)
}

/// Polls until a real login to the container succeeds, so tests never see a
/// database that is "healthy" but not yet accepting sessions (e.g. before the
/// listener has registered the PDB service).
///
/// Connects directly instead of through the bb8 pool, which only reports
/// `TimedOut` and hides the Oracle error; every failed attempt is printed so CI
/// logs show the real cause.
async fn wait_until_connectable(port: u16, timeout: Duration) {
    let connect_string = format!("//localhost:{port}/{ORACLE_SERVICE}");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let cs = connect_string.clone();
        let result = tokio::task::spawn_blocking(move || {
            oracle::Connection::connect(ORACLE_USER, ORACLE_PASSWORD, cs)
        })
        .await
        .expect("Oracle connect task panicked");
        match result {
            Ok(conn) => {
                let _ = conn.close();
                return;
            }
            Err(e) if tokio::time::Instant::now() < deadline => {
                eprintln!("Oracle at {connect_string} not accepting sessions yet: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => panic!("Oracle at {connect_string} never accepted a session: {e}"),
        }
    }
}

/// Starts the Oracle container shared by all Oracle tests on first use and
/// returns its host port once the database accepts logins.
async fn ensure_oracle_container() -> u16 {
    static PORT: tokio::sync::OnceCell<u16> = tokio::sync::OnceCell::const_new();

    *PORT
        .get_or_init(|| async {
            let port = super::container_port();
            let running_container = start_oracle_docker_container(port)
                .await
                .expect("Oracle Docker container to start");
            super::register_container(running_container);
            wait_until_connectable(port, Duration::from_secs(300)).await;
            port
        })
        .await
}

/// Returns a new connection pool to the Oracle container shared by all Oracle
/// tests, starting the container on first use.
///
/// Only the container is shared, never the pool: each `#[tokio::test]` runs on
/// its own runtime, and bb8 spawns its connection tasks on the runtime of the
/// test that triggered them. Those tasks die with that runtime, and the
/// connections they were opening stay counted as pending, so a pool shared
/// across tests can end up permanently timing out.
pub async fn get_oracle_connection_pool() -> Arc<OracleConnectionPool> {
    let port = ensure_oracle_container().await;

    Arc::new(
        OracleConnectionPool::new(get_oracle_params(port))
            .await
            .expect("Failed to create Oracle connection pool"),
    )
}
