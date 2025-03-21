use std::sync::Arc;

use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    duckdb::DuckDBTableFactory, sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::AccessMode;

/// This example demonstrates how to create a DuckDBTableFactory and use it to create TableProviders
/// that can be registered with DataFusion.
#[tokio::main]
async fn main() {
    // Opening in ReadOnly mode allows multiple reader processes to access the database at the same time.
    let duckdb_pool = Arc::new(
        DuckDbConnectionPool::new_file("examples/duckdb_example.db", &AccessMode::ReadOnly, None)
            .expect("unable to create DuckDB connection pool"),
    );

    let duckdb_table_factory = DuckDBTableFactory::new(duckdb_pool);

    let companies_table = duckdb_table_factory
        .table_provider(TableReference::bare("companies"))
        .await
        .expect("to create table provider");

    let projects_table = duckdb_table_factory
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
