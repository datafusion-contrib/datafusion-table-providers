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

pub mod duckdb;
pub mod flight;
pub mod mysql;
pub mod odbc;
pub mod postgres;
pub mod sqlite;
pub mod utils;

#[pymodule]
// module name need to match project name
fn _internal(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawTableProvider>()?;

    let sqlite = PyModule::new(py, "sqlite")?;
    sqlite::init_module(&sqlite)?;
    m.add_submodule(&sqlite)?;

    let duckdb = PyModule::new(py, "duckdb")?;
    duckdb::init_module(&duckdb)?;
    m.add_submodule(&duckdb)?;

    let odbc = PyModule::new(py, "odbc")?;
    odbc::init_module(&odbc)?;
    m.add_submodule(&odbc)?;

    Ok(())
}
