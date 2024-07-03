use std::sync::Arc;

use datafusion::{
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_table_providers::duckdb::DuckDBTableProviderFactory;
use duckdb::AccessMode;

/// This example demonstrates how to register the DuckDBTableProviderFactory into DataFusion so that
/// DuckDB-backed tables can be created at runtime.
#[tokio::main]
async fn main() {
    let duckdb = Arc::new(DuckDBTableProviderFactory::new().access_mode(AccessMode::ReadWrite));

    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionState::new_with_config_rt(SessionConfig::new(), runtime);

    state
        .table_factories_mut()
        .insert("DUCKDB".to_string(), duckdb);

    let ctx = SessionContext::new_with_state(state);

    // TODO: We could rework the DuckDB table factory to also respect LOCATION
    ctx.sql(
        "CREATE EXTERNAL TABLE person (id INT, name STRING) 
         STORED AS duckdb 
         LOCATION 'not_used' 
         OPTIONS ('duckdb.mode' 'file', 'duckdb.open' 'examples/duckdb_external_table_person.db');",
    )
    .await
    .expect("create table failed");

    // Inserting works!
    ctx.sql("INSERT INTO person VALUES (1, 'Alice')")
        .await
        .expect("insert plan failed")
        .collect()
        .await
        .expect("insert failed");

    let df = ctx
        .sql("SELECT * FROM person LIMIT 10")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
