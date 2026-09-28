use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    quack::QuackTableFactory, sql::db_connection_pool::quackpool::QuackConnectionPool,
    util::secrets::to_secret_map,
};

/// This example demonstrates how to:
/// 1. Create a Quack connection pool for a remote DuckDB
/// 2. Create table providers for two of its tables with QuackTableFactory
/// 3. Query them with DataFusion, with the join federated to DuckDB
///
/// Prerequisites: a DuckDB 2.0 (Quack protocol v3) server with the quack extension, holding the
/// example tables. Start one with the DuckDB CLI and leave it running:
/// ```bash
/// duckdb -cmd "
///   CREATE TABLE companies (id INTEGER PRIMARY KEY, name VARCHAR);
///   INSERT INTO companies VALUES (1, 'Acme Corporation'), (2, 'Globex');
///   CREATE TABLE projects (id INTEGER, company_id INTEGER, title VARCHAR);
///   INSERT INTO projects VALUES (1, 1, 'Rocket skates'), (2, 1, 'Giant magnet'), (3, 2, 'Doomsday device');
///   INSTALL quack; LOAD quack;
///   CALL quack_serve('quack:localhost:9494', token => 'demo_token');"
/// ```
///
/// `QUACK_SERVER_URI` and `QUACK_AUTH_TOKEN` override the address and token.
#[tokio::main]
async fn main() {
    let endpoint =
        std::env::var("QUACK_SERVER_URI").unwrap_or_else(|_| "quack:localhost:9494".to_string());
    let token = std::env::var("QUACK_AUTH_TOKEN").unwrap_or_else(|_| "demo_token".to_string());

    // One pool per server. Every table built from it shares its sessions, and joins
    // between those tables can run on the server as a single query.
    let pool = Arc::new(
        QuackConnectionPool::new(to_secret_map(HashMap::from([
            ("endpoint".to_string(), endpoint),
            ("token".to_string(), token),
        ])))
        .await
        .expect("unable to connect to the Quack server"),
    );
    let table_factory = QuackTableFactory::new(pool);

    // A federated session sends whole subplans over Quack tables to DuckDB.
    let ctx = SessionContext::new_with_state(datafusion_federation::default_session_state());
    for table in ["companies", "projects"] {
        ctx.register_table(
            table,
            table_factory
                .table_provider(TableReference::bare(table))
                .await
                .expect("unable to create the table provider"),
        )
        .expect("unable to register the table");
    }

    let df = ctx
        .sql(
            "SELECT c.name, count(*) AS projects \
             FROM companies c JOIN projects p ON p.company_id = c.id \
             GROUP BY c.name ORDER BY c.name",
        )
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
