use std::{sync::Arc, time::Duration};

use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    sql::db_connection_pool::{sqlitepool::SqliteConnectionPoolFactory, Mode},
    sqlite::SqliteTableFactory,
};

/// This example demonstrates how to create a SqliteTableFactory and use it to create TableProviders
/// that can be registered with DataFusion.
#[tokio::main]
async fn main() {
    let sqlite_pool = Arc::new(
        SqliteConnectionPoolFactory::new(
            "examples/sqlite_example.db",
            Mode::File,
            Duration::from_millis(5000),
        )
        .build()
        .await
        .expect("unable to create Sqlite connection pool"),
    );

    let sqlite_table_factory = SqliteTableFactory::new(sqlite_pool);

    let companies_table = sqlite_table_factory
        .table_provider(TableReference::bare("companies"))
        .await
        .expect("to create table provider");

    let projects_table = sqlite_table_factory
        .table_provider(TableReference::bare("projects"))
        .await
        .expect("to create table provider");

    let ctx = SessionContext::new();

    // It's not required that the name registed in DataFusion matches the table name in DuckDB.
    ctx.register_table("companies", companies_table)
        .expect("to register table");

    ctx.register_table("projects", projects_table)
        .expect("to register table");

    let df = ctx
        .sql("SELECT * FROM companies")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM projects")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
