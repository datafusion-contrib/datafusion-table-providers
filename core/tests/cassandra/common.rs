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

    // ScyllaDB is compatible with Cassandra, but its image is configured differently
    let is_scylla = cassandra_docker_image.contains("scylla");

    let builder = ContainerRunnerBuilder::new(format!("{CASSANDRA_DOCKER_CONTAINER}-{port}"))
        .image(cassandra_docker_image)
        .add_port_binding(9042, port);
    let builder = if is_scylla {
        configure_scylla(builder)
    } else {
        configure_cassandra(builder)
    };

    let running_container = builder
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

/// The `cassandra` image is configured with environment variables.
fn configure_cassandra(builder: ContainerRunnerBuilder<'_>) -> ContainerRunnerBuilder<'_> {
    builder
        // The driver connects to the address each node advertises in `system.local`, not to
        // the contact point. By default that is the container's internal IP, which, combined
        // with the published host port, isn't reachable from the host.
        .add_env_var("CASSANDRA_BROADCAST_RPC_ADDRESS", "127.0.0.1")
        // Single-node cluster: the node is its own seed (the image's default), so there is
        // no one to gossip with. Skip the gossip-settle polling and the 30s ring delay.
        .add_env_var(
            "JVM_EXTRA_OPTS",
            "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.ring_delay_ms=0",
        )
        .add_env_var(
            "CASSANDRA_NUM_TOKENS",
            &std::env::var("CASSANDRA_NUM_TOKENS").unwrap_or("1".to_string()),
        )
        .add_env_var("CASSANDRA_CLUSTER_NAME", "df-t-p-cassandra")
        // Keep the JVM small so that the node starts quickly in CI
        .add_env_var("MAX_HEAP_SIZE", "512M")
        .add_env_var("HEAP_NEWSIZE", "128M")
}

/// The `scylladb/scylla` image ignores environment variables: it is configured with the
/// arguments of its entrypoint.
fn configure_scylla(builder: ContainerRunnerBuilder<'_>) -> ContainerRunnerBuilder<'_> {
    builder.cmd(
        [
            // See `configure_cassandra`
            "--broadcast-rpc-address",
            "127.0.0.1",
            // By default Scylla uses all CPUs and most of the memory. With a single shard, the
            // driver doesn't need the (unpublished) shard-aware port either.
            "--smp",
            "1",
            "--memory",
            "1G",
            "--overprovisioned",
            "1",
            // Skips the checks of the host's I/O and kernel settings, which fail in Docker
            "--developer-mode",
            "1",
        ]
        .map(String::from)
        .to_vec(),
    )
}

#[instrument]
pub(super) async fn get_cassandra_connection_pool(
    port: u16,
) -> Result<CassandraConnectionPool, anyhow::Error> {
    CassandraConnectionPool::new(to_secret_map(get_cassandra_params(port)))
        .await
        .map_err(|e| anyhow::anyhow!(e))
}
