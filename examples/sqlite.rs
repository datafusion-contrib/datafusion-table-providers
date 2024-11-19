use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider,
    sql::db_connection_pool::{sqlitepool::SqliteConnectionPoolFactory, Mode},
};
use std::sync::Arc;
use std::time::Duration;

/// This example demonstrates how to create a SqliteTableFactory and use it to create TableProviders
/// that can be registered with DataFusion.
#[tokio::main]
async fn main() {
    let sqlite_pool = Arc::new(
        SqliteConnectionPoolFactory::new(
            "examples/sqlite_example.db",
            Mode::File,
            Duration::default(),
        )
        .build()
        .await
        .expect("unable to create Sqlite connection pool"),
    );

    let catalog = DatabaseCatalogProvider::try_new(sqlite_pool).await.unwrap();
    let ctx = SessionContext::new();

    ctx.register_catalog("sqlite", Arc::new(catalog));

    let df = ctx
        .sql("SELECT * FROM sqlite.main.companies")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM sqlite.main.projects")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
