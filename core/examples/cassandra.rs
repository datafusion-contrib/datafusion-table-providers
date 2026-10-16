use std::{collections::HashMap, sync::Arc};

use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    cassandra::CassandraTableFactory,
    sql::db_connection_pool::cassandrapool::CassandraConnectionPool, util::secrets::to_secret_map,
};

/// This example demonstrates how to:
/// 1. Create a Cassandra connection pool
/// 2. Create and use CassandraTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access Cassandra table data
/// 5. Write data to a Cassandra table with `INSERT INTO`
///
/// Prerequisites:
/// Start a Cassandra server using Docker:
/// ```bash
/// docker run --name cassandra \
///     -e CASSANDRA_BROADCAST_RPC_ADDRESS=127.0.0.1 \
///     -p 9042:9042 \
///     -d cassandra:5.0
/// # Wait for the Cassandra server to start
/// until docker exec cassandra cqlsh -e 'DESCRIBE KEYSPACES' >/dev/null 2>&1; do sleep 5; done
///
/// # Create a table and insert sample data
/// docker exec -i cassandra cqlsh <<EOF
/// CREATE KEYSPACE cassandra_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
///
/// CREATE TABLE cassandra_ks.companies (
///   id INT PRIMARY KEY,
///   name TEXT
/// );
///
/// INSERT INTO cassandra_ks.companies (id, name) VALUES (1, 'Acme Corporation');
/// EOF
/// ```
#[tokio::main]
async fn main() {
    // Create Cassandra connection parameters
    // Unqualified table names are resolved in the given keyspace
    let cassandra_params = to_secret_map(HashMap::from([
        ("hosts".to_string(), "localhost".to_string()),
        ("port".to_string(), "9042".to_string()),
        ("keyspace".to_string(), "cassandra_ks".to_string()),
    ]));

    // Create Cassandra connection pool
    let cassandra_pool = Arc::new(
        CassandraConnectionPool::new(cassandra_params)
            .await
            .expect("unable to create Cassandra connection pool"),
    );

    // Create Cassandra table provider factory
    // Used to generate TableProvider instances that can read and write Cassandra table data
    let table_factory = CassandraTableFactory::new(cassandra_pool);

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Register the Cassandra "companies" table as "companies"
    // `read_write_table_provider` also supports INSERT INTO; use `table_provider` for read-only access
    ctx.register_table(
        "companies",
        table_factory
            .read_write_table_provider(TableReference::bare("companies"))
            .await
            .expect("failed to create table provider"),
    )
    .expect("failed to register table");

    // Write Example: Insert rows into the table.
    // As in CQL, inserting a row with an existing primary key replaces that row.
    let df = ctx
        .sql("INSERT INTO companies (id, name) VALUES (2, 'Initech'), (3, 'Umbrella Corporation')")
        .await
        .expect("insert failed");
    df.show().await.expect("show failed");

    // Query Example 1: Read the whole table
    let df = ctx
        .sql("SELECT * FROM companies ORDER BY id")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");

    // Query Example 2: Filter on a non-key column.
    // CQL can't filter on it, so DataFusion evaluates the filter after the scan.
    let df = ctx
        .sql("SELECT name FROM companies WHERE name LIKE 'Acme%'")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
