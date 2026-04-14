use std::{
    ffi::CString,
    sync::{Arc, OnceLock},
};

use datafusion::catalog::TableProvider;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::{prelude::*, types::PyCapsule};

#[pyclass(module = "datafusion_table_providers._internal")]
struct RawTableProvider {
    pub(crate) table: Arc<dyn TableProvider + Send>,
    pub(crate) supports_pushdown_filters: bool,
}

#[inline]
pub(crate) fn get_tokio_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"))
}

#[pymethods]
impl RawTableProvider {
    #[pyo3(signature = (session=None))]
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_table_provider").unwrap();

        let runtime = if cfg!(feature = "clickhouse") {
            Some(get_tokio_runtime().handle().clone())
        } else {
            None
        };

        let task_ctx_provider = extract_task_ctx_provider(py, session)?;

        let provider = FFI_TableProvider::new(
            Arc::clone(&self.table),
            self.supports_pushdown_filters,
            runtime,
            task_ctx_provider,
            None,
        );

        PyCapsule::new(py, provider, Some(name.clone()))
    }
}

/// Extract an [`FFI_TaskContextProvider`] from a session object.
///
/// If a session is provided and has `__datafusion_task_context_provider__`,
/// use its capsule (backed by the session's long-lived `Arc<SessionContext>`).
/// Otherwise, create a minimal one from a new `SessionContext`.
fn extract_task_ctx_provider(
    _py: Python<'_>,
    session: Option<Bound<'_, PyAny>>,
) -> PyResult<FFI_TaskContextProvider> {
    if let Some(session) = session {
        if session.hasattr("__datafusion_task_context_provider__")? {
            let capsule = session
                .getattr("__datafusion_task_context_provider__")?
                .call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            let provider = unsafe { capsule.reference::<FFI_TaskContextProvider>() };
            return Ok(provider.clone());
        }
    }

    // Fallback for older datafusion-python versions: create our own.
    use datafusion::execution::TaskContextProvider;
    use datafusion::prelude::SessionContext;
    let ctx = Arc::new(SessionContext::new()) as Arc<dyn TaskContextProvider>;
    Ok(FFI_TaskContextProvider::from(&ctx))
}

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flight")]
pub mod flight;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod utils;

#[pymodule]
// module name need to match project name
fn _internal(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawTableProvider>()?;

    #[cfg(feature = "sqlite")]
    {
        let sqlite = PyModule::new(py, "sqlite")?;
        sqlite::init_module(&sqlite)?;
        m.add_submodule(&sqlite)?;
    }

    #[cfg(feature = "duckdb")]
    {
        let duckdb = PyModule::new(py, "duckdb")?;
        duckdb::init_module(&duckdb)?;
        m.add_submodule(&duckdb)?;
    }

    #[cfg(feature = "odbc")]
    {
        let odbc = PyModule::new(py, "odbc")?;
        odbc::init_module(&odbc)?;
        m.add_submodule(&odbc)?;
    }

    #[cfg(feature = "mysql")]
    {
        let mysql = PyModule::new(py, "mysql")?;
        mysql::init_module(&mysql)?;
        m.add_submodule(&mysql)?;
    }

    #[cfg(feature = "postgres")]
    {
        let postgres = PyModule::new(py, "postgres")?;
        postgres::init_module(&postgres)?;
        m.add_submodule(&postgres)?;
    }

    #[cfg(feature = "flight")]
    {
        let flight = PyModule::new(py, "flight")?;
        flight::init_module(&flight)?;
        m.add_submodule(&flight)?;
    }

    #[cfg(feature = "clickhouse")]
    {
        let clickhouse = PyModule::new(py, "clickhouse")?;
        clickhouse::init_module(&clickhouse)?;
        m.add_submodule(&clickhouse)?;
    }

    #[cfg(feature = "mongodb")]
    {
        let mongodb = PyModule::new(py, "mongodb")?;
        mongodb::init_module(&mongodb)?;
        m.add_submodule(&mongodb)?;
    }

    Ok(())
}
