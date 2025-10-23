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

use crate::sql::db_connection_pool::dbconnection::adbcconn::ADBCConnectionManager;
use std::collections::HashMap;
use secrecy::{ExposeSecret, SecretString};
use adbc_core::{Database, Connection};
use adbc_core::options::AdbcVersion;
use adbc_driver_manager::ManagedDriver;

use super::{dbconnection::DbConnection, DbConnectionPool, JoinPushDown};

pub struct ADBCPool {
    pub db: Arc<Database>,
    pub join_push_down: JoinPushDown,
}

impl ADBCPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let mut driver = None;
        
        let mut options = HashMap::new();
        for (key, value) in &params {
            let value = value.expose_secret();
            match key.as_str() {
                "driver" => {
                    driver = ManagedDriver::load_dynamic_from_name(value, None, AdbcVersion::V110)?;
                }
                _ => {
                    options.insert(key.clone(), value.clone());
                }
            }
        }

        let db = driver.new_database_with_opts(options.into_iter())?;
        Ok(Self { db: Arc::new(db) })
    }
}


