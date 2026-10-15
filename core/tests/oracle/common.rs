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

/// Starts the Oracle container shared by all Oracle tests on first use and
/// returns its host port.
///
/// Memoized separately from the pool so that a failed pool creation does not
/// start (and leak) another container on the next test's retry.
async fn ensure_oracle_container() -> u16 {
    static PORT: tokio::sync::OnceCell<u16> = tokio::sync::OnceCell::const_new();

    *PORT
        .get_or_init(|| async {
            let port = super::container_port();
            let running_container = start_oracle_docker_container(port)
                .await
                .expect("Oracle Docker container to start");
            super::register_container(running_container);
            port
        })
        .await
}

/// Returns the connection pool shared by all Oracle tests, starting the
/// Oracle container on first use.
///
/// Each `#[tokio::test]` runs in its own runtime, so the pool and the
/// container are memoized across tests: only the first caller pays for the
/// (slow) Oracle boot, the others wait for it and reuse the same container.
pub async fn get_oracle_connection_pool() -> Arc<OracleConnectionPool> {
    static POOL: tokio::sync::OnceCell<Arc<OracleConnectionPool>> =
        tokio::sync::OnceCell::const_new();

    POOL.get_or_init(|| async {
        let port = ensure_oracle_container().await;

        Arc::new(
            OracleConnectionPool::new(get_oracle_params(port))
                .await
                .expect("Failed to create Oracle connection pool"),
        )
    })
    .await
    .clone()
}
