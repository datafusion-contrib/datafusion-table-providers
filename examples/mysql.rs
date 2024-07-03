use std::{collections::HashMap, sync::Arc};

use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    mysql::MySQLTableFactory, sql::db_connection_pool::mysqlpool::MySQLConnectionPool,
    util::secrets::to_secret_map,
};

/// This example demonstrates how to register a table provider into DataFusion that
/// uses a MySQL table as its source.
///
/// Use docker to start a MySQL server this example can connect to:
///
/// ```bash
/// docker run --name mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=mysql_db -p 3306:3306 -d mysql:9.0
/// # Wait for the MySQL server to start
/// sleep 30
///
/// # Create a table in the MySQL server and insert some data
/// docker exec -i mysql mysql -uroot -ppassword mysql_db <<EOF
/// CREATE TABLE companies (
///    id INT PRIMARY KEY,
///   name VARCHAR(100)
/// );
///
/// INSERT INTO companies (id, name) VALUES (1, 'Acme Corporation');
/// EOF
/// ```
#[tokio::main]
async fn main() {
    let mysql_params = to_secret_map(HashMap::from([
        (
            "mysql_connection_string".to_string(),
            "mysql://root:password@localhost:3306/mysql_db".to_string(),
        ),
        ("mysql_sslmode".to_string(), "disabled".to_string()),
    ]));

    let mysql_pool = Arc::new(
        MySQLConnectionPool::new(Arc::new(mysql_params))
            .await
            .expect("unable to create MySQL connection pool"),
    );

    let table_factory = MySQLTableFactory::new(mysql_pool);

    let companies_table = table_factory
        .table_provider(TableReference::bare("companies"))
        .await
        .expect("to create table provider");

    let ctx = SessionContext::new();

    // It's not required that the name registed in DataFusion matches the table name in DuckDB.
    ctx.register_table("companies", companies_table)
        .expect("to register table");

    let df = ctx
        .sql("SELECT * FROM companies")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
