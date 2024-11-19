use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::AccessMode;

/// This example demonstrates how to create a DuckDBTableFactory and use it to create TableProviders
/// that can be registered with DataFusion.
#[tokio::main]
async fn main() {
    // Opening in ReadOnly mode allows multiple reader processes to access the database at the same time.
    let duckdb_pool = Arc::new(
        DuckDbConnectionPool::new_file("examples/duckdb_example.db", &AccessMode::ReadOnly)
            .expect("unable to create DuckDB connection pool"),
    );

    let catalog = DatabaseCatalogProvider::try_new(duckdb_pool).await.unwrap();

    let ctx = SessionContext::new();

    ctx.register_catalog("duckdb", Arc::new(catalog));

    let df = ctx
        .sql("SELECT * FROM duckdb.main.companies")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM duckdb.main.projects")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
