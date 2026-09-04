use std::sync::Arc;

use datafusion_table_providers::{
    mongodb::{connection_pool::MongoDBConnectionPool, MongoDBTableFactory},
    util::secrets::to_secret_map,
};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    utils::{pydict_to_hashmap, to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.mongodb")]
struct RawMongoDBTableFactory {
    pool: Arc<MongoDBConnectionPool>,
    factory: MongoDBTableFactory,
}

#[pymethods]
impl RawMongoDBTableFactory {
    #[new]
    #[pyo3(signature = (params))]
    pub fn new(py: Python, params: &Bound<'_, PyDict>) -> PyResult<Self> {
        let params = to_secret_map(pydict_to_hashmap(params)?);
        let pool =
            Arc::new(wait_for_future(py, MongoDBConnectionPool::new(params)).map_err(to_pyerr)?);

        Ok(Self {
            factory: MongoDBTableFactory::new(Arc::clone(&pool)),
            pool,
        })
    }

    pub fn tables(&self, py: Python) -> PyResult<Vec<String>> {
        wait_for_future(py, async {
            let conn = self.pool.connect().await.map_err(to_pyerr)?;
            let tables = conn.tables().await.map_err(to_pyerr)?;
            Ok(tables)
        })
    }

    pub fn get_table(&self, py: Python, table_reference: &str) -> PyResult<RawTableProvider> {
        let table = wait_for_future(py, self.factory.table_provider(table_reference.into()))
            .map_err(to_pyerr)?;

        Ok(RawTableProvider {
            table,
            supports_pushdown_filters: true,
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawMongoDBTableFactory>()?;

    Ok(())
}
