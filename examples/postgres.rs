use std::{collections::HashMap, sync::Arc};

use datafusion::{prelude::SessionContext, sql::TableReference};
use datafusion_table_providers::{
    postgres::PostgresTableFactory, sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    util::secrets::to_secret_map,
};

/// This example demonstrates how to register a table provider into DataFusion that
/// uses a Postgres table as its source.
///
/// Use docker to start a Postgres server this example can connect to:
///
/// ```bash
/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
/// # Wait for the Postgres server to start
/// sleep 30
///
/// # Create a table in the Postgres server and insert some data
/// docker exec -i postgres psql -U postgres -d postgres_db <<EOF
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
    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "postgres".to_string()),
        ("db".to_string(), "postgres".to_string()),
        ("pass".to_string(), "postgres".to_string()),
        ("port".to_string(), "5432".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ]));

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(postgres_params)
            .await
            .expect("unable to create Postgres connection pool"),
    );

    let table_factory = PostgresTableFactory::new(postgres_pool);

    let companies_table = table_factory
        .table_provider(TableReference::bare("companies"))
        .await
        .expect("to create table provider");

    let companies_view = table_factory
        .table_provider(TableReference::bare("companies_view"))
        .await
        .expect("to create table provider for view");

    let companies_materialized_view = table_factory
        .table_provider(TableReference::bare("companies_materialized_view"))
        .await
        .expect("to create table provider for materialized view");

    let ctx = SessionContext::new();

    // It's not required that the name registed in DataFusion matches the table name in DuckDB.
    ctx.register_table("companies", companies_table)
        .expect("to register table");
    ctx.register_table("companies_view", companies_view)
        .expect("to register view");
    ctx.register_table("companies_materialized_view", companies_materialized_view)
        .expect("to register materialized view");

    let df = ctx
        .sql("SELECT * FROM companies")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM companies_view")
        .await
        .expect("select from view failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM companies_materialized_view")
        .await
        .expect("select from materialized view failed");

    df.show().await.expect("show failed");
}
