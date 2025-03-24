use std::{collections::HashMap, sync::Arc};

use datafusion_table_providers::{
    odbc::ODBCTableFactory,
    sql::db_connection_pool::{odbcpool::ODBCPool, DbConnectionPool},
    util::secrets::to_secret_map,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

use crate::{
    utils::{to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.odbc")]
struct RawODBCTableFactory {
    pool: Arc<ODBCPool>,
    factory: ODBCTableFactory<'static>, // TODO: 'static lifetime might be wrong
}

#[pymethods]
impl RawODBCTableFactory {
    #[new]
    #[pyo3(signature = (params))]
    pub fn new(params: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut hashmap = HashMap::new();

        // Iterate over the tuple and treat it as pairs
        for (key, value) in params.iter() {
            let key: String = key.extract()?;
            let value: String = value.extract()?;
            hashmap.insert(key, value);
        }
        let hashmap = to_secret_map(hashmap);
        let pool = Arc::new(ODBCPool::new(hashmap).map_err(to_pyerr)?);
        Ok(Self {
            factory: ODBCTableFactory::new(pool.clone()),
            pool,
        })
    }

    pub fn tables(&self) -> PyResult<Vec<String>> {
        // This method is not supported yet because of unimplemented traints in odbcconn.
        unimplemented!();
    }

    pub fn get_table(&self, py: Python, table_reference: &str) -> PyResult<RawTableProvider> {
        // TODO: schema is optional
        let table = wait_for_future(
            py,
            self.factory.table_provider(table_reference.into(), None),
        )
        .map_err(to_pyerr)?;

        Ok(RawTableProvider {
            table,
            supports_pushdown_filters: true,
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawODBCTableFactory>()?;

    Ok(())
}
