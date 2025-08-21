use crate::docker::{ContainerRunnerBuilder, RunningContainer};
use bollard::secret::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::trinodbpool::TrinoConnectionPool;
use reqwest::header::HeaderMap;
use reqwest::Client;
use secrecy::SecretString;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;

const TRINO_DOCKER_CONTAINER: &str = "runtime-integration-test-trino";

#[instrument]
pub async fn start_trino_docker_container(port: usize) -> Result<RunningContainer, anyhow::Error> {
    let container_name = format!("{TRINO_DOCKER_CONTAINER}-{port}");

    let port = port.try_into().unwrap_or(8080);

    let trino_docker_image =
        std::env::var("TRINO_DOCKER_IMAGE").unwrap_or("trinodb/trino:latest".to_string());

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(trino_docker_image)
        .add_port_binding(8080, port)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD".to_string(),
                "curl".to_string(),
                "-f".to_string(),
                format!("http://localhost:8080/v1/info"),
            ]),
            interval: Some(2_000_000_000), // 2 seconds
            timeout: Some(5_000_000_000),  // 5 seconds
            retries: Some(15),
            start_period: Some(30_000_000_000), // 30 seconds
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    Ok(running_container)
}

pub(super) fn get_trino_params(port: usize) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "trino_host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert(
        "trino_port".to_string(),
        SecretString::from(port.to_string()),
    );
    params.insert(
        "trino_catalog".to_string(),
        SecretString::from("tpch".to_string()),
    );
    params.insert(
        "trino_schema".to_string(),
        SecretString::from("tiny".to_string()),
    );
    params.insert(
        "trino_user".to_string(),
        SecretString::from("test".to_string()),
    );
    params.insert(
        "trino_sslmode".to_string(),
        SecretString::from("disabled".to_string()),
    );
    params
}

#[instrument]
pub(super) async fn get_trino_connection_pool(
    port: usize,
) -> Result<TrinoConnectionPool, anyhow::Error> {
    let trino_pool = TrinoConnectionPool::new(get_trino_params(port))
        .await
        .expect("Failed to create Trino Connection Pool");

    Ok(trino_pool)
}

pub struct TrinoClient {
    reqwest_client: Client,
    base_url: String,
}

impl TrinoClient {
    pub fn new(port: usize) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert("X-Trino-Catalog", "memory".parse().unwrap());
        headers.insert("X-Trino-Schema", "default".parse().unwrap());
        headers.insert("X-Trino-User", "test".parse().unwrap());

        let client = Client::builder().default_headers(headers).build().unwrap();

        Self {
            reqwest_client: client,
            base_url: format!("http://localhost:{port}"),
        }
    }

    pub async fn execute(&self, query: &str) -> Result<Vec<Vec<serde_json::Value>>, anyhow::Error> {
        let url = format!("{}/v1/statement", self.base_url);
        let response = self
            .reqwest_client
            .post(&url)
            .body(query.to_string())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to submit query: HTTP {}: {}",
                response.status(),
                response.text().await?
            ));
        }

        let mut result: Value = response.json().await?;
        let mut all_data = Vec::new();

        loop {
            let state = result["stats"]["state"].as_str().unwrap_or("");

            // Extract data rows
            if let Some(data) = result.get("data").and_then(|d| d.as_array()) {
                for row in data {
                    if let Some(row_array) = row.as_array() {
                        all_data.push(row_array.clone());
                    }
                }
            }

            // Check if query is finished
            if state == "FINISHED" {
                break;
            }

            if let Some(next_uri) = result.get("nextUri").and_then(|u| u.as_str()) {
                // Wait before polling
                sleep(Duration::from_millis(50)).await;

                let response = self.reqwest_client.clone().get(next_uri).send().await?;

                if !response.status().is_success() {
                    let status_code = response.status().as_u16();
                    let message = response.text().await.unwrap_or_default();
                    return Err(anyhow::anyhow!(
                        "Failed to submit query: HTTP {}: {}",
                        status_code,
                        message
                    ));
                }

                result = response.json().await?;
            } else {
                if state != "FINISHED" {
                    // No next URI but query not finished - this shouldn't happen
                    return Err(anyhow::anyhow!(
                        "Query not finished but no nextUri provided. State: {}",
                        state
                    ));
                }
                break;
            }
        }

        Ok(all_data)
    }
}

pub(super) async fn get_trino_client(port: usize) -> Result<TrinoClient, anyhow::Error> {
    let client = TrinoClient::new(port);

    // Test connection and setup memory catalog
    let mut retries = 15;
    let mut last_err = None;
    while retries > 0 {
        match client.execute("SELECT 1").await {
            Ok(_) => {
                println!("Trino client connected successfully");
                return Ok(client);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                retries -= 1;
                println!("Trino connection test failed, retrying...");
            }
        }
    }

    if let Some(err) = last_err {
        return Err(anyhow::anyhow!(
            "Failed to connect to Trino after retries: {}",
            err
        ));
    }

    Ok(client)
}
