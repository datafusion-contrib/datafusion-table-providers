use std::{future::Future, sync::OnceLock};
use pyo3::{exceptions::PyException, prelude::*};

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[inline]
pub(crate) fn get_tokio_runtime() -> &'static TokioRuntime {
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| TokioRuntime(tokio::runtime::Runtime::new().unwrap()))
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime: &tokio::runtime::Runtime = &get_tokio_runtime().0;
    py.allow_threads(|| runtime.block_on(f))
}

pub fn to_pyerr<T: ToString>(err: T) -> PyErr {
    PyException::new_err(err.to_string())
}