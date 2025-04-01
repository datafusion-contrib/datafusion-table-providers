// If calling directly from Rust, there is already tokio runtime so no
// additional work is needed. If calling from Python FFI, there's no existing
// tokio runtime, so we need to start a new one.
use std::{future::Future, sync::OnceLock};

use tokio::runtime::{Handle, Runtime};

pub fn get_tokio_runtime() -> &'static Runtime {
    // TODO: this function is a repetition of python/src/utils.rs::get_tokio_runtime.
    // Think about how to refactor it
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

pub async fn run_async_with_tokio<F, Fut, T, E>(f: F) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match Handle::try_current() {
        Ok(_) => f().await,
        Err(_) => get_tokio_runtime().block_on(f()),
    }
}

pub fn run_sync_with_tokio<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    match Handle::try_current() {
        Ok(_) => f(),
        Err(_) => get_tokio_runtime().block_on(async { f() }),
    }
}
