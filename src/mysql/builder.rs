use std::sync::Arc;

use datafusion::sql::{unparser::dialect::Dialect, TableReference};
use mysql_async::prelude::ToValue;

use crate::sql::{
    db_connection_pool::{mysqlpool::MySQLConnectionPool, DbConnectionPool},
    sql_provider_datafusion::{self, SqlTable},
};

use super::{dialect::MySqlDialect, sql_table::MySQLTable};

pub struct MySQLTableBuilder {
    pool: Arc<MySQLConnectionPool>,
    table_reference: TableReference,
    dialect: Option<Arc<dyn Dialect>>,
}

impl MySQLTableBuilder {
    pub fn new(pool: Arc<MySQLConnectionPool>, table_reference: TableReference) -> Self {
        Self {
            pool,
            table_reference,
            dialect: None,
        }
    }

    pub fn with_dialect(mut self, dialect: Arc<dyn Dialect>) -> Self {
        self.dialect = Some(dialect);
        self
    }
}

impl MySQLTableBuilder {
    pub async fn build(self) -> Result<MySQLTable, sql_provider_datafusion::Error> {
        let dyn_pool = Arc::clone(&self.pool)
            as Arc<
                dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)>
                    + Send
                    + Sync,
            >;
        let base_table = SqlTable::new("mysql", &dyn_pool, self.table_reference, None)
            .await?
            .with_dialect(
                self.dialect
                    .unwrap_or_else(|| Arc::new(MySqlDialect::default())),
            );

        Ok(MySQLTable {
            pool: self.pool,
            base_table,
        })
    }
}
