use bollard::{
    models::{
        ContainerCreateBody, ContainerState, ContainerStateStatusEnum, Health, HealthConfig,
        HealthStatusEnum, HostConfig, PortBinding,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, InspectContainerOptions,
        ListContainersOptionsBuilder, ListImagesOptions, RemoveContainerOptionsBuilder,
        StartContainerOptions, StopContainerOptions,
    },
    Docker,
};
use futures::StreamExt;
use std::fmt::Debug;
use std::future::Future;
use std::{borrow::Cow, collections::HashMap, sync::Arc};

pub struct RunningContainer {
    name: Arc<str>,
    docker: Docker,
}

impl Debug for RunningContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningContainer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RunningContainer {
    pub async fn remove(&self) -> Result<(), anyhow::Error> {
        remove(&self.docker, &self.name).await
    }

    pub async fn stop(&self) -> Result<(), anyhow::Error> {
        stop(&self.docker, &self.name).await
    }
}

pub async fn remove(docker: &Docker, name: &str) -> Result<(), anyhow::Error> {
    Ok(docker
        .remove_container(
            name,
            Some(RemoveContainerOptionsBuilder::new().force(true).build()),
        )
        .await?)
}

pub async fn stop(docker: &Docker, name: &str) -> Result<(), anyhow::Error> {
    Ok(docker
        .stop_container(name, Option::<StopContainerOptions>::None)
        .await?)
}

pub struct ContainerRunnerBuilder<'a> {
    name: Cow<'a, str>,
    image: Option<String>,
    port_bindings: Vec<(u16, u16)>,
    env_vars: Vec<(String, String)>,
    healthcheck: Option<HealthConfig>,
    health_timeout: Option<std::time::Duration>,
}

impl<'a> ContainerRunnerBuilder<'a> {
    pub fn new(name: impl Into<Cow<'a, str>>) -> Self {
        ContainerRunnerBuilder {
            name: name.into(),
            image: None,
            port_bindings: Vec::new(),
            env_vars: Vec::new(),
            healthcheck: None,
            health_timeout: None,
        }
    }

    pub fn image(mut self, image: String) -> Self {
        self.image = Some(image);
        self
    }

    /// Maps `container_port` inside the container to `host_port` on the host.
    pub fn add_port_binding(mut self, container_port: u16, host_port: u16) -> Self {
        self.port_bindings.push((container_port, host_port));
        self
    }

    pub fn add_env_var(mut self, key: &str, value: &str) -> Self {
        self.env_vars.push((key.to_string(), value.to_string()));
        self
    }

    pub fn healthcheck(mut self, healthcheck: HealthConfig) -> Self {
        self.healthcheck = Some(healthcheck);
        self
    }

    /// Overrides the default 90s window allowed for a container to become healthy
    /// (slow-booting databases like Oracle need more).
    pub fn health_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.health_timeout = Some(timeout);
        self
    }

    pub fn build(self) -> Result<ContainerRunner<'a>, anyhow::Error> {
        let image = self
            .image
            .ok_or_else(|| anyhow::anyhow!("Image must be set"))?;
        Ok(ContainerRunner::<'a> {
            name: self.name,
            docker: Docker::connect_with_local_defaults()?,
            image,
            port_bindings: self.port_bindings,
            env_vars: self.env_vars,
            healthcheck: self.healthcheck,
            health_timeout: self.health_timeout,
        })
    }
}

pub struct ContainerRunner<'a> {
    name: Cow<'a, str>,
    docker: Docker,
    image: String,
    port_bindings: Vec<(u16, u16)>,
    env_vars: Vec<(String, String)>,
    healthcheck: Option<HealthConfig>,
    health_timeout: Option<std::time::Duration>,
}

impl ContainerRunner<'_> {
    pub async fn run(self) -> Result<RunningContainer, anyhow::Error> {
        if self.does_container_exists().await? {
            remove(&self.docker, &self.name).await?;
        }

        self.pull_image().await?;

        let options = CreateContainerOptionsBuilder::new()
            .name(self.name.as_ref())
            .build();

        let mut port_bindings_map = HashMap::new();
        for (container_port, host_port) in self.port_bindings {
            port_bindings_map.insert(
                format!("{container_port}/tcp"),
                Some(vec![PortBinding {
                    host_ip: Some("127.0.0.1".to_string()),
                    // Docker HostPort is the bare port number (e.g. "15432"), not "15432/tcp".
                    host_port: Some(format!("{host_port}")),
                }]),
            );
        }
        tracing::debug!("Port bindings: {port_bindings_map:?}");

        let port_bindings = if port_bindings_map.is_empty() {
            None
        } else {
            Some(port_bindings_map)
        };

        let host_config = Some(HostConfig {
            port_bindings,
            ..Default::default()
        });

        let env_vars: Vec<String> = self
            .env_vars
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();

        let config = ContainerCreateBody {
            image: Some(self.image.clone()),
            env: Some(env_vars),
            host_config,
            healthcheck: self.healthcheck,
            ..Default::default()
        };

        let _ = self.docker.create_container(Some(options), config).await?;

        self.docker
            .start_container(&self.name, Option::<StartContainerOptions>::None)
            .await?;

        let start_time = std::time::Instant::now();
        let timeout = self
            .health_timeout
            .unwrap_or(std::time::Duration::from_secs(90));
        loop {
            let inspect_container = self
                .docker
                .inspect_container(&self.name, Option::<InspectContainerOptions>::None)
                .await?;
            tracing::trace!("Container status: {:?}", inspect_container.state);

            if let Some(ContainerState {
                status: Some(ContainerStateStatusEnum::RUNNING),
                health:
                    Some(Health {
                        status: Some(HealthStatusEnum::HEALTHY),
                        ..
                    }),
                ..
            }) = inspect_container.state
            {
                tracing::info!("Container {} running & healthy", self.name);
                break;
            }

            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Container {} failed to become healthy within {:?}: {:?}",
                    self.name,
                    timeout,
                    inspect_container.state
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        Ok(RunningContainer {
            name: self.name.into(),
            docker: self.docker,
        })
    }

    async fn pull_image(&self) -> Result<(), anyhow::Error> {
        // Check if image is already pulled
        let images = self
            .docker
            .list_images(Option::<ListImagesOptions>::None)
            .await?;
        for image in images {
            if image.repo_tags.iter().any(|t| t == &self.image) {
                tracing::debug!("Docker image {} already pulled", self.image);
                return Ok(());
            }
        }

        let options = Some(
            CreateImageOptionsBuilder::new()
                .from_image(&self.image)
                .build(),
        );

        let mut pulling_stream = self.docker.create_image(options, None, None);
        while let Some(event) = pulling_stream.next().await {
            tracing::debug!("Pulling image: {:?}", event?);
        }

        Ok(())
    }

    async fn does_container_exists(&self) -> Result<bool, anyhow::Error> {
        let containers = self
            .docker
            .list_containers(Some(
                ListContainersOptionsBuilder::default().all(true).build(),
            ))
            .await?;
        for container in containers {
            let Some(names) = container.names else {
                continue;
            };
            if names.iter().any(|n| {
                tracing::debug!("Docker container: {n}");
                n == &self.name || n == &format!("/{}", self.name)
            }) {
                tracing::debug!("Docker container {} already exists", self.name);
                return Ok(true);
            }
        }

        Ok(false)
    }
}

pub struct ContainerManager {
    pub port: u16,
    pub claimed: bool,
    pub running_container: Option<RunningContainer>,
}

impl ContainerManager {
    pub async fn start_container<F, Fut>(&mut self, container_factory: F)
    where
        F: Fn(u16) -> Fut,
        Fut: Future<Output = Result<RunningContainer, anyhow::Error>> + Send + 'static,
    {
        if !self.claimed {
            self.claimed = true;
            let running_container = container_factory(self.port)
                .await
                .expect("Docker container to start");

            tracing::info!("Container {:?} started", &running_container);
            self.running_container = Some(running_container);
        }
    }
}

impl Drop for ContainerManager {
    fn drop(&mut self) {
        tracing::info!("ContainerManager dropped");
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(drop_container(self.running_container.take(), self.port));
    }
}

impl Default for ContainerManager {
    fn default() -> Self {
        ContainerManager {
            port: crate::get_random_port(),
            claimed: false,
            running_container: None,
        }
    }
}

async fn drop_container(running_container: Option<RunningContainer>, port: u16) {
    if let Some(running_container) = running_container {
        match std::env::var("DF_TABLE_PROVIDERS_DEBUG").ok() {
            Some(_) => {
                // Just stop the container, so the developer could re-start if needed
                tracing::info!("Stopping Docker container on port {port}");
                if let Err(e) = running_container.stop().await {
                    tracing::error!("Error stopping Docker container: {e}");
                }
            }
            None => {
                tracing::info!("Removing Docker container on port {port}");
                if let Err(e) = running_container.remove().await {
                    tracing::error!("Error removing Docker container: {e}");
                }
            }
        }
    }
}

pub fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library/".to_string())
}
