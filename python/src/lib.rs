use std::{ffi::CString, sync::Arc};

use datafusion::catalog::TableProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::{prelude::*, types::PyCapsule};

#[pyclass(module = "datafusion_table_providers._internal")]
struct RawTableProvider {
    pub(crate) table: Arc<dyn TableProvider + Send>,
    pub(crate) supports_pushdown_filters: bool,
}

#[pymethods]
impl RawTableProvider {
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_table_provider").unwrap();

        let provider = FFI_TableProvider::new(
            Arc::clone(&self.table),
            self.supports_pushdown_filters,
            None,
        );

        PyCapsule::new(py, provider, Some(name.clone()))
    }
}

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

    Ok(())
}
