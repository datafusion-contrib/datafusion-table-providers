use bollard::secret::HealthConfig;
use std::collections::HashMap;
use tracing::instrument;

use crate::{
    container_registry,
    docker::{ContainerRunnerBuilder, RunningContainer},
};

const PG_PASSWORD: &str = "runtime-integration-test-pw";
const PG_DOCKER_CONTAINER: &str = "runtime-integration-test-postgres";

pub(super) fn get_pg_params(port: usize) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("pg_host".to_string(), "localhost".to_string());
    params.insert("pg_port".to_string(), port.to_string());
    params.insert("pg_user".to_string(), "postgres".to_string());
    params.insert("pg_pass".to_string(), PG_PASSWORD.to_string());
    params.insert("pg_db".to_string(), "postgres".to_string());
    params.insert("pg_sslmode".to_string(), "disable".to_string());
    params
}

#[instrument]
pub(super) async fn start_postgres_docker_container(
    port: usize,
) -> Result<RunningContainer, anyhow::Error> {
    let container_name = format!("{PG_DOCKER_CONTAINER}-{port}");
    let port = port.try_into().unwrap_or(15432);

    let pg_docker_image = std::env::var("PG_DOCKER_IMAGE")
        .unwrap_or_else(|_| format!("{}postgres:latest", container_registry()));

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(pg_docker_image)
        .add_port_binding(5432, port)
        .add_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "pg_isready -U postgres".to_string(),
            ]),
            interval: Some(1_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(100),
            start_period: Some(60_000_000_000),
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

