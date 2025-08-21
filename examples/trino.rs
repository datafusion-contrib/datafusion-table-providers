use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::trinodbpool::TrinoConnectionPool;
use datafusion_table_providers::trino::TrinoTableFactory;
use datafusion_table_providers::util::secrets::to_secret_map;

/// This example demonstrates how to:
/// 1. Create a Trino connection pool
/// 2. Create and use TrinoTableFactory to generate TableProvider
/// 3. Use SQL queries to access Trino table data
///
/// Prerequisites:
/// Start a Trino server using Docker:
/// ```bash
/// docker run -d --name trino   -p 8080:8080   trinodb/trino:latest
/// # Wait for the Trino server to start
/// sleep 30
/// ```
#[tokio::main]
async fn main() {
    // Create Trino connection parameters
    let trino_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("port".to_string(), "8080".to_string()),
        ("catalog".to_string(), "tpch".to_string()),
        ("schema".to_string(), "tiny".to_string()),
        ("user".to_string(), "test".to_string()),
        ("sslmode".to_string(), "disabled".to_string()),
    ]));

    // Create Trino connection pool
    let trino_pool = Arc::new(
        TrinoConnectionPool::new(trino_params)
            .await
            .expect("unable to create Trino connection pool"),
    );

    // Create Trino table provider factory
    let table_factory = TrinoTableFactory::new(trino_pool.clone());

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Register the Trino "region" table as "region"
    ctx.register_table(
        "region",
        table_factory
            .table_provider(TableReference::bare("region"))
            .await
            .expect("failed to register table provider"),
    )
    .expect("failed to register table");

    let df = ctx
        .sql("SELECT * FROM region")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
