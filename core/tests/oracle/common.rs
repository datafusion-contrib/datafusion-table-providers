use bollard::secret::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::oraclepool::OracleConnectionPool;
use secrecy::SecretString;
use std::collections::HashMap;
use std::env;

const DEFAULT_ORACLE_CONTAINER_NAME: &str = "runtime-integration-test-oracle";
const DEFAULT_ORACLE_IMAGE: &str = "gvenzl/oracle-free:latest";
const DEFAULT_ORACLE_PASSWORD: &str = "password";
const DEFAULT_ORACLE_USER: &str = "system";
const DEFAULT_ORACLE_SERVICE: &str = "FREEPDB1";

pub fn get_oracle_params() -> HashMap<String, SecretString> {
    let mut params = HashMap::new();

    // Default to strict env vars or defaults
    let host = env::var("ORACLE_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("ORACLE_PORT").unwrap_or_else(|_| "1521".to_string());
    let user = env::var("ORACLE_USER").unwrap_or_else(|_| DEFAULT_ORACLE_USER.to_string());
    let pass = env::var("ORACLE_PASSWORD").unwrap_or_else(|_| DEFAULT_ORACLE_PASSWORD.to_string());
    let service = env::var("ORACLE_SERVICE").unwrap_or_else(|_| DEFAULT_ORACLE_SERVICE.to_string());

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

pub async fn get_oracle_connection_pool() -> OracleConnectionPool {
    let params = get_oracle_params();
    OracleConnectionPool::new(params)
        .await
        .expect("Failed to create Oracle connection pool")
}

pub async fn start_oracle_docker_container(
) -> Result<crate::docker::RunningContainer, anyhow::Error> {
    let container_name = env::var("ORACLE_CONTAINER_NAME")
        .unwrap_or_else(|_| DEFAULT_ORACLE_CONTAINER_NAME.to_string());

    let oracle_docker_image = std::env::var("ORACLE_DOCKER_IMAGE")
        .unwrap_or_else(|_| "gvenzl/oracle-free:latest".to_string());

    let host_port = env::var("ORACLE_PORT")
        .unwrap_or_else(|_| "1521".to_string())
        .parse::<u16>()
        .unwrap_or(1521);

    let mut builder = crate::docker::ContainerRunnerBuilder::new(container_name)
        .image(oracle_docker_image)
        .add_port_binding(host_port, 1521)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "/usr/local/bin/checkHealth.sh".to_string(),
            ]),
            interval: Some(1_000_000_000),     // 1s
            timeout: Some(500_000_000),        // 500ms
            retries: Some(30),                 // Give it time to start
            start_period: Some(5_000_000_000), // 5s initial wait
            start_interval: None,
        });

    // Pass through all ORACLE_ environment variables from the host to the container
    for (key, value) in env::vars() {
        if key.starts_with("ORACLE_")
            && !["ORACLE_DOCKER_IMAGE", "ORACLE_PORT", "ORACLE_HOST"].contains(&key.as_str())
        {
            builder = builder.add_env_var(&key, &value);
        }
    }

    // Ensure we have a password set if not provided in environment, as Oracle images require it
    if env::var("ORACLE_PASSWORD").is_err() && env::var("ORACLE_RANDOM_PASSWORD").is_err() {
        builder = builder.add_env_var("ORACLE_PASSWORD", DEFAULT_ORACLE_PASSWORD);
    }

    // Support creating an app user if ORACLE_USER is set and not a system user
    if let Ok(user) = env::var("ORACLE_USER") {
        if user.to_uppercase() != "SYSTEM" && user.to_uppercase() != "SYS" {
            builder = builder.add_env_var("APP_USER", &user);

            // Prioritize APP_USER_PASSWORD, then ORACLE_PASSWORD, then empty string
            let pass = env::var("APP_USER_PASSWORD")
                .or_else(|_| env::var("ORACLE_PASSWORD"))
                .unwrap_or_else(|_| "".to_string());
            builder = builder.add_env_var("APP_USER_PASSWORD", &pass);
        }
    }

    let running_container = builder.build()?.run().await?;

    // Let the healthcheck handle readiness
    Ok(running_container)
}
