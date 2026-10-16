use bollard::models::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::cassandrapool::CassandraConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use std::collections::HashMap;
use tracing::instrument;

use crate::docker::{container_registry, ContainerRunnerBuilder, RunningContainer};

const CASSANDRA_DOCKER_CONTAINER: &str = "runtime-integration-test-cassandra";

pub(super) fn get_cassandra_params(port: u16) -> HashMap<String, String> {
    HashMap::from([
        ("hosts".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), port.to_string()),
    ])
}

#[instrument]
pub(super) async fn start_cassandra_docker_container(
    port: u16,
) -> Result<RunningContainer, anyhow::Error> {
    let cassandra_docker_image = std::env::var("CASSANDRA_DOCKER_IMAGE")
        .unwrap_or_else(|_| format!("{}cassandra:5.0", container_registry()));

    let running_container =
        ContainerRunnerBuilder::new(format!("{CASSANDRA_DOCKER_CONTAINER}-{port}"))
            .image(cassandra_docker_image)
            .add_port_binding(9042, port)
            // The driver connects to the address each node advertises in `system.local`, not to
            // the contact point. By default that is the container's internal IP, which, combined
            // with the published host port, isn't reachable from the host.
            .add_env_var("CASSANDRA_BROADCAST_RPC_ADDRESS", "127.0.0.1")
            // Keep the JVM small so that the node starts quickly in CI
            .add_env_var("MAX_HEAP_SIZE", "512M")
            .add_env_var("HEAP_NEWSIZE", "128M")
            .healthcheck(HealthConfig {
                test: Some(vec![
                    "CMD-SHELL".to_string(),
                    "cqlsh -e 'DESCRIBE KEYSPACES' || exit 1".to_string(),
                ]),
                interval: Some(2_000_000_000), // 2s
                timeout: Some(10_000_000_000), // 10s
                retries: Some(90),
                start_period: Some(20_000_000_000), // 20s
                start_interval: None,
            })
            // Cassandra takes much longer to boot than the other databases
            .health_timeout(std::time::Duration::from_secs(300))
            .build()?
            .run()
            .await?;

    Ok(running_container)
}

#[instrument]
pub(super) async fn get_cassandra_connection_pool(
    port: u16,
) -> Result<CassandraConnectionPool, anyhow::Error> {
    CassandraConnectionPool::new(to_secret_map(get_cassandra_params(port)))
        .await
        .map_err(|e| anyhow::anyhow!(e))
}
