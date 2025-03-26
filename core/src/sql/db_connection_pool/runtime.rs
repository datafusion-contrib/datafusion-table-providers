use std::sync::OnceLock;

use tokio::runtime::Runtime;

pub fn get_tokio_runtime() -> &'static Runtime {
    // TODO: this function is a repetition of python/src/utils.rs::get_tokio_runtime.
    // Think about how to refactor it
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}
