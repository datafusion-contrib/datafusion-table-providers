use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    mssql::MsSqlTableFactory, sql::db_connection_pool::mssqlpool::MsSqlConnectionPool,
    util::secrets::to_secret_map,
};

/// This example demonstrates how to:
/// 1. Create a SQL Server connection pool
/// 2. Create and use MsSqlTableFactory to generate TableProviders
/// 3. Register them with DataFusion, in a federated session that pushes the queries
///    down to SQL Server
/// 4. Read and write SQL Server tables with SQL
///
/// Prerequisites:
/// Start SQL Server (here Azure SQL Edge, which has arm64 and amd64 images) in Docker:
/// ```bash
/// docker run --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='Password-123' \
///     -p 1433:1433 -d mcr.microsoft.com/azure-sql-edge:latest
/// # Wait for the server to start
/// sleep 20
/// ```
#[tokio::main]
async fn main() {
    let params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("port".to_string(), "1433".to_string()),
        ("user".to_string(), "sa".to_string()),
        ("password".to_string(), "Password-123".to_string()),
        ("database".to_string(), "master".to_string()),
        // the server uses a self-signed certificate
        ("trust_server_certificate".to_string(), "true".to_string()),
    ]));

    let pool = Arc::new(
        MsSqlConnectionPool::new(params)
            .await
            .expect("unable to create the SQL Server connection pool"),
    );

    // Create the example table
    let conn = pool.connect_direct().await.expect("connection");
    conn.execute_raw("DROP TABLE IF EXISTS companies", &[])
        .await
        .expect("DROP TABLE");
    conn.execute_raw(
        "CREATE TABLE companies (id INT PRIMARY KEY, name NVARCHAR(100))",
        &[],
    )
    .await
    .expect("CREATE TABLE");
    conn.execute_raw("INSERT INTO companies VALUES (1, N'Acme Corporation')", &[])
        .await
        .expect("INSERT");

    let factory = MsSqlTableFactory::new(Arc::clone(&pool));

    // A federated session pushes down the joins, aggregations, sorts and limits of the
    // SQL Server tables of the same database
    let ctx = SessionContext::new_with_state(datafusion_federation::default_session_state());
    ctx.register_table(
        "companies",
        factory
            .read_write_table_provider(TableReference::bare("companies"))
            .await
            .expect("failed to create the table provider"),
    )
    .expect("failed to register the table");

    ctx.sql("INSERT INTO companies VALUES (2, 'Initech'), (3, 'Umbrella')")
        .await
        .expect("INSERT should be planned")
        .collect()
        .await
        .expect("INSERT should be executed");

    let df = ctx
        .sql("SELECT * FROM companies WHERE id > 1 ORDER BY name LIMIT 10")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
