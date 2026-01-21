use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    postgres::PostgresTableFactory,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    util::secrets::to_secret_map,
};
use std::collections::HashMap;
use std::sync::Arc;

/// NOTE: This example currently FAILS due to a bug with auto-generated columns (SERIAL).
/// When using ctx.sql() for INSERT, the returned batch has null for the auto-generated `id`
/// column, but the schema marks it as non-nullable, causing:
/// "Invalid batch column at '0' has null but schema specifies non-nullable"
///
/// WORKAROUND: If the table is created without the auto-generated `id` column (only `name`
/// and `email`), the example works correctly. See the alternative CREATE TABLE below.
///
/// This example demonstrates how to:
/// 1. Create a PostgreSQL connection pool
/// 2. Use read_write_table_provider to register a table
/// 3. Use ctx.sql() with INSERT statement to insert data
/// 4. Query the inserted data back
///
/// Prerequisites:
/// Start a PostgreSQL server using Docker:
/// ```bash
/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5433:5432 -d postgres:16-alpine
/// # Wait for the Postgres server to start
/// sleep 30
///
/// # Create the users table (this will FAIL due to SERIAL column)
/// docker exec -i postgres psql -U postgres postgres_db <<EOF
/// CREATE TABLE IF NOT EXISTS users (
///    id SERIAL PRIMARY KEY,
///    name VARCHAR(100),
///    email VARCHAR(100)
/// );
/// EOF
///
/// # Alternative: Create table WITHOUT auto-generated column (this WORKS)
/// docker exec -i postgres psql -U postgres postgres_db <<EOF
/// DROP TABLE IF EXISTS users;
/// CREATE TABLE users (
///    name VARCHAR(100),
///    email VARCHAR(100)
/// );
/// EOF
/// ```
///
/// Run with:
/// ```bash
/// cargo run -p datafusion-table-providers --example postgres_insert_sql --no-default-features --features postgres
/// ```
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create PostgreSQL connection parameters
    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "postgres".to_string()),
        ("db".to_string(), "postgres_db".to_string()),
        ("pass".to_string(), "password".to_string()),
        ("port".to_string(), "5433".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ]));

    // Create PostgreSQL connection pool
    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create PostgreSQL connection pool"),
    );

    // Create PostgreSQL table provider factory
    let table_factory = PostgresTableFactory::new(postgres_pool);

    // Get a read-write table provider for the "users" table
    let table_provider = table_factory
        .read_write_table_provider(TableReference::bare("users"))
        .await
        .expect("failed to create read_write_table_provider");

    // Create a DataFusion session context
    let ctx = SessionContext::new();

    // Register the table provider with DataFusion
    ctx.register_table("users", table_provider)?;

    // Use ctx.sql() with INSERT statement to insert data
    println!("Inserting data using ctx.sql()...");
    let insert_sql = "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com')";
    println!("SQL: {}", insert_sql);

    let df = ctx.sql(insert_sql).await?;

    // Collect the insert result
    let insert_result = df.collect().await?;
    println!("Insert completed. Result: {:?}", insert_result);

    // Query the data back to verify
    println!("\nQuerying inserted data...");
    let select_df = ctx.sql("SELECT * FROM users").await?;

    // Collect and display results
    let results = select_df.collect().await?;
    println!("\nQuery results:");
    for batch in &results {
        println!("{:?}", batch);
    }

    println!("\nInsert and query completed successfully!");

    Ok(())
}
