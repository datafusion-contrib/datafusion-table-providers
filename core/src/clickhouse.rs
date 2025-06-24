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

use clickhouse::Client;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgOperator, Ident, Value,
};
use datafusion::sql::unparser;
use datafusion::{common::Constraints, sql::TableReference};
use std::sync::Arc;

use crate::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use crate::sql::db_connection_pool::dbconnection::AsyncDbConnection;

#[cfg(feature = "clickhouse-federation")]
pub mod federation;
pub mod sql_table;

pub struct ClickHouseTableFactory {
    pool: Arc<ClickHouseConnectionPool>,
}

impl ClickHouseTableFactory {
    pub fn new(pool: impl Into<Arc<ClickHouseConnectionPool>>) -> Self {
        Self { pool: pool.into() }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
        args: Option<Vec<(String, Arg)>>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        let client: &dyn AsyncDbConnection<Client, ()> = &self.pool.client();
        let schema = client.get_schema(&table_reference).await?;
        let table_provider = Arc::new(ClickHouseTable::new(
            table_reference,
            args,
            self.pool.clone(),
            schema,
            Constraints::empty(),
        ));

        #[cfg(feature = "clickhouse-federation")]
        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}

#[derive(Debug, Clone)]
pub enum Arg {
    Unsigned(u64),
    Signed(i64),
    String(String),
}

impl From<String> for Arg {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<u64> for Arg {
    fn from(value: u64) -> Self {
        Self::Unsigned(value)
    }
}

impl From<i64> for Arg {
    fn from(value: i64) -> Self {
        Self::Signed(value)
    }
}

impl From<Arg> for Expr {
    fn from(value: Arg) -> Self {
        Expr::value(match value {
            Arg::Unsigned(x) => Value::Number(x.to_string(), false),
            Arg::Signed(x) => Value::Number(x.to_string(), false),
            Arg::String(x) => Value::SingleQuotedString(x),
        })
    }
}

fn into_table_args(args: Vec<(String, Arg)>) -> Vec<FunctionArg> {
    args.into_iter()
        .map(|(name, value)| FunctionArg::Named {
            name: Ident::new(name),
            arg: FunctionArgExpr::Expr(value.into()),
            operator: FunctionArgOperator::Equals,
        })
        .collect()
}

pub struct ClickHouseTable {
    table_reference: TableReference,
    args: Option<Vec<(String, Arg)>>,
    pool: Arc<ClickHouseConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
    dialect: Arc<dyn unparser::dialect::Dialect>,
}

impl std::fmt::Debug for ClickHouseTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTable")
            .field("table_name", &self.table_reference)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl ClickHouseTable {
    pub fn new(
        table_reference: TableReference,
        args: Option<Vec<(String, Arg)>>,
        pool: Arc<ClickHouseConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_reference,
            args,
            pool,
            schema,
            constraints,
            dialect: Arc::new(unparser::dialect::DefaultDialect {}),
        }
    }
}
