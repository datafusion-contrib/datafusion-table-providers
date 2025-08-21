/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::sql::db_connection_pool::trinodbpool::TrinoConnectionPool;
use crate::sql::sql_provider_datafusion::{self};
use datafusion::{datasource::TableProvider, sql::TableReference};
use snafu::prelude::*;
use sql_table::TrinoTable;
use std::sync::Arc;

pub mod sql_table;

pub struct TrinoTableFactory {
    pool: Arc<TrinoConnectionPool>,
}

impl TrinoTableFactory {
    #[must_use]
    pub fn new(pool: Arc<TrinoConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = Arc::new(
            TrinoTable::new(&pool, table_reference)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
