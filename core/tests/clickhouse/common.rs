use bollard::models::HealthConfig;
use datafusion_table_providers::util::secrets::to_secret_map;
use secrecy::SecretString;
use std::collections::HashMap;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const CLICKHOUSE_USER: &str = "user";
const CLICKHOUSE_PASSWORD: &str = "integration-test-pw";
const CLICKHOUSE_DOCKER_CONTAINER: &str = "runtime-integration-test-clickhouse";

pub(super) fn get_clickhouse_params(port: u16) -> HashMap<String, SecretString> {
    to_secret_map(HashMap::from([
        ("url".to_string(), format!("http://localhost:{port}")),
        ("user".to_string(), CLICKHOUSE_USER.to_string()),
        ("password".to_string(), CLICKHOUSE_PASSWORD.to_string()),
    ]))
}

#[instrument]
pub async fn start_clickhouse_docker_container(
    port: u16,
) -> Result<RunningContainer, anyhow::Error> {
    let clickhouse_docker_image = std::env::var("CLICKHOUSE_DOCKER_IMAGE")
        .unwrap_or_else(|_| format!("{}clickhouse:latest", "registry.hub.docker.com/library/"));

    let running_container =
        ContainerRunnerBuilder::new(format!("{CLICKHOUSE_DOCKER_CONTAINER}-{port}"))
            .image(clickhouse_docker_image)
            .add_port_binding(8123, port)
            .add_env_var("CLICKHOUSE_USER", CLICKHOUSE_USER)
            .add_env_var("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
            .healthcheck(HealthConfig {
                test: Some(vec![
                    "CMD-SHELL".to_string(),
                    "wget --no-verbose --tries=1 --spider http://localhost:8123/ping || exit 1"
                        .to_string(),
                ]),
                interval: Some(500_000_000), // 250ms
                timeout: Some(100_000_000),  // 100ms
                retries: Some(5),
                start_period: Some(500_000_000), // 100ms
                start_interval: None,
            })
            .build()?
            .run()
            .await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    Ok(running_container)
}
