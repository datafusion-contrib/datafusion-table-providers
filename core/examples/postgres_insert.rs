use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    postgres::PostgresTableFactory,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    util::secrets::to_secret_map,
};
use std::collections::HashMap;
use std::sync::Arc;

/// This example demonstrates how to:
/// 1. Create a PostgreSQL connection pool
/// 2. Create a "users" table in PostgreSQL
/// 3. Use read_write_table_provider to insert data
/// 4. Query the inserted data back
///
/// Prerequisites:
/// Start a PostgreSQL server using Docker:
/// ```bash
/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
/// # Wait for the Postgres server to start
/// sleep 30
///
/// # Create the users table
/// docker exec -i postgres psql -U postgres postgres_db <<EOF
/// CREATE TABLE IF NOT EXISTS users (
///    name VARCHAR(100),
///    email VARCHAR(100)
/// );
/// EOF
/// ```
///
/// Run with:
/// ```bash
/// cargo run --example postgres_insert --features postgres
/// ```
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create PostgreSQL connection parameters
    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "postgres".to_string()),
        ("db".to_string(), "postgres_db".to_string()),
        ("pass".to_string(), "password".to_string()),
        ("port".to_string(), "5432".to_string()),
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

    // Create the schema for our data
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]));

    // Create the data to insert
    let name_array = StringArray::from(vec!["Alice", "Bob"]);
    let email_array = StringArray::from(vec!["alice@example.com", "bob@example.com"]);

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(name_array), Arc::new(email_array)],
    )?;

    println!("Data to insert:");
    println!("{:?}", record_batch);

    // Create a DataFusion session context
    let ctx = SessionContext::new();

    // Create a MemorySourceConfig as the input data source for the insert
    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![record_batch.clone()]],
        record_batch.schema(),
        None,
    )?;

    // Execute INSERT using insert_into
    println!("\nInserting data into PostgreSQL...");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
        .await?;

    // Run the insert and collect results
    let insert_result = collect(insert_plan, ctx.task_ctx()).await?;
    println!("Insert completed. Result: {:?}", insert_result);

    // Register the table and query it back to verify the insert worked
    ctx.register_table("users", table_provider)?;

    println!("\nQuerying inserted data...");
    let df = ctx.sql("SELECT * FROM users").await?;

    // Collect and display results
    let results = df.collect().await?;
    println!("\nQuery results:");
    for batch in &results {
        println!("{:?}", batch);
    }

    println!("\nInsert and query completed successfully!");

    Ok(())
}
