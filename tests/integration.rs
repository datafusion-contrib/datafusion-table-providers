use rand::Rng;

mod arrow_record_batch_gen;
mod docker;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library/".to_string())
}

fn get_random_port() -> usize {
    rand::thread_rng().gen_range(15432..65535)
}
