// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::sql::{
    db_connection_pool::{self, adbcpool::ADBCPool, DbConnectionPool},
    sql_provider_datafusion::SqlTable,
};

use adbc_driver_manager::ManagedDatabase;
use arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::{datasource::TableProvider, sql::TableReference};
use r2d2_adbc::AdbcConnectionManager;
use snafu::prelude::*;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

type DynAdbcConnectionPool = dyn DbConnectionPool<r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>>, RecordBatch>
    + Send
    + Sync;

pub struct AdbcTableFactory {
    pool: Arc<ADBCPool>,
}

impl AdbcTableFactory {
    #[must_use]
    pub fn new(pool: Arc<ADBCPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool: Arc<DynAdbcConnectionPool> = pool;

        let table = SqlTable::new("adbc", &dyn_pool, table_reference)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let table_provider = Arc::new(table);

        Ok(table_provider)
    }
}
