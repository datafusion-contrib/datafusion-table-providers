use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bollard::models::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::mssqlpool::MsSqlConnectionPool;
use secrecy::SecretString;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub(super) const MSSQL_PASSWORD: &str = "Integration-Test-Pw1";
const MSSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-mssql";
pub(super) const MSSQL_DATABASE: &str = "it_db";

pub(super) fn get_mssql_params(port: u16, database: Option<&str>) -> HashMap<String, SecretString> {
    let mut params = HashMap::from([
        ("mssql_host".to_string(), "127.0.0.1".to_string()),
        ("mssql_port".to_string(), port.to_string()),
        ("mssql_user".to_string(), "sa".to_string()),
        ("mssql_password".to_string(), MSSQL_PASSWORD.to_string()),
        ("mssql_encrypt".to_string(), "true".to_string()),
        (
            "mssql_trust_server_certificate".to_string(),
            "true".to_string(),
        ),
        ("mssql_pool_max".to_string(), "5".to_string()),
    ]);
    if let Some(database) = database {
        params.insert("mssql_database".to_string(), database.to_string());
    }
    params
        .into_iter()
        .map(|(k, v)| (k, SecretString::from(v)))
        .collect()
}

#[instrument]
pub(super) async fn start_mssql_docker_container(
    port: u16,
) -> Result<RunningContainer, anyhow::Error> {
    let container_name = format!("{MSSQL_DOCKER_CONTAINER}-{port}");

    // Azure SQL Edge has arm64 and amd64 images, unlike mcr.microsoft.com/mssql/server
    let image = std::env::var("MSSQL_DOCKER_IMAGE")
        .unwrap_or_else(|_| "mcr.microsoft.com/azure-sql-edge:latest".to_string());

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(image)
        .add_port_binding(1433, port)
        .add_env_var("ACCEPT_EULA", "Y")
        .add_env_var("MSSQL_SA_PASSWORD", MSSQL_PASSWORD)
        // The image has no sqlcmd, so the server is healthy when it listens on its port
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "grep -q ':0599 ' /proc/net/tcp /proc/net/tcp6".to_string(),
            ]),
            interval: Some(1_000_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(120),
            start_period: Some(5_000_000_000),
            start_interval: None,
        })
        .health_timeout(Duration::from_secs(300))
        .build()?
        .run()
        .await?;

    Ok(running_container)
}

/// Polls until a login succeeds and creates the test database: the server listens before it
/// accepts logins.
async fn wait_until_connectable_and_create_database(port: u16, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match MsSqlConnectionPool::new(get_mssql_params(port, None)).await {
            Ok(pool) => {
                let conn = pool.connect_direct().await.expect("connection");
                conn.execute_raw(
                    &format!(
                        "IF DB_ID(N'{MSSQL_DATABASE}') IS NULL CREATE DATABASE [{MSSQL_DATABASE}]"
                    ),
                    &[],
                )
                .await
                .expect("test database should be created");
                return;
            }
            Err(e) if tokio::time::Instant::now() < deadline => {
                eprintln!("SQL Server on port {port} not accepting logins yet: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => panic!("SQL Server on port {port} never accepted a login: {e}"),
        }
    }
}

/// Starts the SQL Server container shared by all the tests on first use and returns its port.
pub(super) async fn ensure_mssql_container() -> u16 {
    static PORT: tokio::sync::OnceCell<u16> = tokio::sync::OnceCell::const_new();

    *PORT
        .get_or_init(|| async {
            let port = super::container_port();
            let running_container = start_mssql_docker_container(port)
                .await
                .expect("SQL Server Docker container to start");
            super::register_container(running_container);
            wait_until_connectable_and_create_database(port, Duration::from_secs(180)).await;
            port
        })
        .await
}

/// Returns a new pool to the test database, starting the container on first use.
///
/// The pool is never shared between tests: bb8 runs its tasks on the runtime of the test
/// that created them, and each test has its own runtime.
pub(super) async fn get_mssql_connection_pool() -> Arc<MsSqlConnectionPool> {
    let port = ensure_mssql_container().await;
    Arc::new(
        MsSqlConnectionPool::new(get_mssql_params(port, Some(MSSQL_DATABASE)))
            .await
            .expect("SQL Server connection pool should be created"),
    )
}
