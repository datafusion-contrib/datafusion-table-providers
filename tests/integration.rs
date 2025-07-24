use rand::Rng;

mod docker;


mod postgres;

fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library/".to_string())
}

fn get_random_port() -> usize {
    rand::thread_rng().gen_range(15432..65535)
}
