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

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use r2d2_adbc::AdbcConnectionManager;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use snafu::{prelude::*, ResultExt};
use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::db_connection_pool::dbconnection::{
    adbcconn::AdbcDbConnection, DbConnection, SyncDbConnection,
};

use super::{DbConnectionPool, JoinPushDown, Result};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ADBC Database initialization failed.\n{source}"))]
    AdbcInitializationError { source: adbc_core::error::Error },

    #[snafu(display("ADBC Connection failed to execute.\n{source}"))]
    ConnectionExecutionError { source: adbc_core::error::Error },

    #[snafu(display(
        "ADBC connection failed.\n{source}\nAdjust the connection pool parameters or sufficient capacity."
    ))]
    ConnectionPoolError { source: r2d2::Error },
}

pub struct AdbcConnectionPoolBuilder {
    driver: String,
    db_options: Arc<HashMap<String, SecretString>>,
    conn_options: HashMap<String, String>,
    max_size: Option<u32>,
    min_idle: Option<u32>,
}

pub fn hash_db_options(driver: &str, db_options: &HashMap<String, SecretString>, conn_options: &HashMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(driver.as_bytes());
    db_options.iter().for_each(|(key, value)| {
        hasher.update(key.as_bytes());
        hasher.update(value.expose_secret().as_bytes());
    });
    conn_options.iter().for_each(|(key, value)| {
        hasher.update(key.as_bytes());
        hasher.update(value.as_bytes());
    });
    hasher.finalize().iter().fold(String::new(), |mut hash, b| {
        hash.push_str(&format!("{b:02x}"));
        hash
    })
}

impl AdbcConnectionPoolBuilder {
    pub fn new(driver: &str, db_options: HashMap<String, SecretString>) -> Self {
        Self {
            driver: driver.to_string(),
            db_options: db_options.into(),
            conn_options: HashMap::new(),
            max_size: None,
            min_idle: None,
        }
    }

    pub fn with_conn_option(mut self, key: String, value: String) -> Self {
        self.conn_options.insert(key, value);
        self
    }

    pub fn with_conn_options(mut self, options: HashMap<String, String>) -> Self {
        for (key, value) in options {
            self.conn_options.insert(key, value);
        }
        self
    }

    pub fn with_max_size(mut self, size: Option<u32>) -> Self {
        self.max_size = size;
        self
    }

    pub fn with_min_idle(mut self, size: Option<u32>) -> Self {
        self.min_idle = size;
        self
    }

    pub fn build(self) -> Result<ADBCPool> {
        let mut driver = ManagedDriver::load_from_name(
            &self.driver,
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .context(AdbcInitializationSnafu)?;

        let mut pool_builder = r2d2::Pool::builder();

        if let Some(size) = self.max_size {
            pool_builder = pool_builder.max_size(size);
        }
        if self.min_idle.is_some() {
            pool_builder = pool_builder.min_idle(self.min_idle);
        }

        // let opts = ;
        let database = driver
            .new_database_with_opts(self.db_options.iter().map(|(key, value)| {
                let value = value.expose_secret();
                match key.as_str() {
                    "uri" => (OptionDatabase::Uri, OptionValue::String(value.to_string())),
                    "username" => (
                        OptionDatabase::Username,
                        OptionValue::String(value.to_string()),
                    ),
                    "password" => (
                        OptionDatabase::Password,
                        OptionValue::String(value.to_string()),
                    ),
                    _ => (
                        OptionDatabase::Other(key.to_string()),
                        OptionValue::String(value.to_string()),
                    ),
                }
            }))
            .context(AdbcInitializationSnafu)?;

        let id = hash_db_options(&self.driver, &self.db_options, &self.conn_options);
        let manager = AdbcConnectionManager::with_options(
                database, self.conn_options.into_iter());
        let pool = Arc::new(pool_builder.build(manager).context(ConnectionPoolSnafu)?);
        Ok(ADBCPool {
            driver: self.driver,
            pool,
            id,
        })
    }
}

#[derive(Clone)]
pub struct ADBCPool {
    driver: String,
    pool: Arc<r2d2::Pool<AdbcConnectionManager<ManagedDatabase>>>,    
    id: String,
}

impl std::fmt::Debug for ADBCPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ADBCPool")
            .field("driver", &self.driver)            
            .finish()
    }
}

impl ADBCPool {
    pub fn new(
        driver: String,
        db_options: HashMap<String, SecretString>,
        conn_options: HashMap<String, String>,
    ) -> Result<Self> {
        let builder =
            AdbcConnectionPoolBuilder::new(&driver, db_options).with_conn_options(conn_options);
        builder.build()
    }

    #[must_use]
    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    #[must_use]
    pub fn driver(&self) -> &str {
        self.driver.as_ref()
    }

    pub fn connect_sync(
        self: Arc<Self>,
    ) -> Result<
        Box<
            dyn DbConnection<
                r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>>,
                RecordBatch,
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>> =
            pool.get().context(ConnectionPoolSnafu)?;

        Ok(Box::new(AdbcDbConnection::new(conn)))
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>>, RecordBatch>
    for ADBCPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<
            dyn DbConnection<
                r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>>,
                RecordBatch,
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>> =
            pool.get().context(ConnectionPoolSnafu)?;

        Ok(Box::new(AdbcDbConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.id.clone())
    }
}
