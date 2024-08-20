use bollard::secret::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use rand::Rng;
use secrecy::SecretString;
use std::collections::HashMap;
use tracing::instrument;

use crate::{
    container_registry,
    docker::{ContainerRunnerBuilder, RunningContainer},
};

const MYSQL_ROOT_PASSWORD: &str = "integration-test-pw";
const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-mysql";

fn get_mysql_params(port: usize) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "mysql_host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert(
        "mysql_tcp_port".to_string(),
        SecretString::from(port.to_string()),
    );
    params.insert(
        "mysql_user".to_string(),
        SecretString::from("root".to_string()),
    );
    params.insert(
        "mysql_pass".to_string(),
        SecretString::from(MYSQL_ROOT_PASSWORD.to_string()),
    );
    params.insert(
        "mysql_db".to_string(),
        SecretString::from("mysqldb".to_string()),
    );
    params.insert(
        "mysql_sslmode".to_string(),
        SecretString::from("disabled".to_string()),
    );
    params
}

#[instrument]
pub async fn start_mysql_docker_container(
    port: usize,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{MYSQL_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());

    let port = if let Ok(port) = port.try_into() {
        port
    } else {
        15432
    };

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(format!("{}mysql:latest", container_registry()))
        .add_port_binding(3306, port)
        .add_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .add_env_var("MYSQL_DATABASE", "mysqldb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!("mysqladmin ping --password={MYSQL_ROOT_PASSWORD}"),
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(5),
            start_period: Some(500_000_000), // 100ms
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

#[instrument]
pub(super) async fn get_mysql_connection_pool(
    port: usize,
) -> Result<MySQLConnectionPool, anyhow::Error> {
    let mysql_pool = MySQLConnectionPool::new(get_mysql_params(port))
        .await
        .expect("Failed to create MySQL Connection Pool");

    Ok(mysql_pool)
}

pub(super) fn get_random_port() -> usize {
    rand::thread_rng().gen_range(15432..65535)
}
