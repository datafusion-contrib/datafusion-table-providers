use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, duckdb::DuckDBTableFactory,
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::AccessMode;

/// This example demonstrates the DuckDB EXPLAIN integration with DataFusion.
/// When you run EXPLAIN on a query that uses DuckDB tables, the DataFusion
/// explain plan will include a DuckDBExplain child node that shows the DuckDB
/// query plan as well.
#[tokio::main]
async fn main() {
    // Create DuckDB connection pool
    let duckdb_pool = Arc::new(
        DuckDbConnectionPool::new_file("core/examples/duckdb_example.db", &AccessMode::ReadOnly)
            .expect("unable to create DuckDB connection pool"),
    );

    // Create DuckDB table provider factory
    let table_factory = DuckDBTableFactory::new(duckdb_pool.clone());

    // Create database catalog provider
    let catalog = DatabaseCatalogProvider::try_new(duckdb_pool).await.unwrap();

    // Create DataFusion session context
    let ctx = SessionContext::new();
    ctx.register_catalog("duckdb", Arc::new(catalog));

    // Register a table
    ctx.register_table(
        "companies",
        table_factory
            .table_provider(TableReference::bare("companies"))
            .await
            .expect("failed to register table provider"),
    )
    .expect("failed to register table");

    // Create a query
    let df = ctx
        .sql("SELECT * FROM datafusion.public.companies WHERE id < 5")
        .await
        .expect("select failed");

    // Show the explain plan - notice the DuckDBExplain child node
    println!("\n=== DataFusion Explain Plan ===");
    df.clone()
        .explain(false, false)
        .expect("explain failed")
        .show()
        .await
        .expect("show failed");

    // Also show the results
    println!("\n=== Query Results ===");
    df.show().await.expect("show failed");
}
