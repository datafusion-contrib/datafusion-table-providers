use std::{
    ffi::CString,
    sync::{Arc, OnceLock},
};

use datafusion::{catalog::TableProvider, execution::TaskContextProvider, prelude::SessionContext};
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

/// Returns a static reference to a default SessionContext.
/// This ensures the Arc backing the Weak reference inside
/// FFI_TaskContextProvider is never dropped.
fn get_default_session_context() -> &'static Arc<SessionContext> {
    static CTX: OnceLock<Arc<SessionContext>> = OnceLock::new();
    CTX.get_or_init(|| Arc::new(SessionContext::default()))
}

#[pymethods]
impl RawTableProvider {
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        _session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_table_provider").unwrap();

        let runtime = if cfg!(feature = "clickhouse") {
            Some(get_tokio_runtime().handle().clone())
        } else {
            None
        };

        let ctx = Arc::clone(get_default_session_context()) as Arc<dyn TaskContextProvider>;
        let provider = FFI_TableProvider::new(
            Arc::clone(&self.table),
            self.supports_pushdown_filters,
            runtime,
            &ctx,
            None,
        );

        PyCapsule::new(py, provider, Some(name.clone()))
    }
}

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flight")]
pub mod flight;
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

    Ok(())
}
