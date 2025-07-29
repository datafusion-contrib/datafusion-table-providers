use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    catalog::TableProviderFactory,
    common::{Constraints, ToDFSchema},
    logical_expr::CreateExternalTable,
    prelude::{SessionConfig, SessionContext},
    sql::TableReference,
};
use datafusion_table_providers::duckdb::attached_factory::AttachedDuckDBTableProviderFactory;
use tempfile::TempDir;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// This example demonstrates how to use the `AttachedDuckDBTableProviderFactory`
/// to query multiple DuckDB database files from a single in-memory instance.
#[tokio::main]
async fn main() -> Result<()> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG) // Set log level (TRACE, DEBUG, INFO, etc.)
    //     .init();

    // Create a new factory. This initializes a single in-memory DuckDB instance.
    let factory =
        Arc::new(AttachedDuckDBTableProviderFactory::new().expect("Failed to create factory"));

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    let state = ctx.state();

    let cmd = create_external_table("db1")?;
    let db1 = factory.create(&state, &cmd).await?;
    let cmd = create_external_table("db2")?;
    let db2 = factory.create(&state, &cmd).await?;

    ctx.register_table("db1", db1).expect("to register table");

    ctx.register_table("db2", db2).expect("to register table");

    ctx.sql("INSERT INTO db1 (i) VALUES (1), (2), (3)")
        .await
        .expect("db1 insert");
    ctx.sql("INSERT INTO db2 (i) VALUES (4), (5), (6)")
        .await
        .expect("db2 insert");

    let df = ctx.sql("SELECT * FROM db1").await.expect("select failed");

    df.show().await.expect("show failed");

    let df = ctx.sql("SELECT * FROM db2").await.expect("select failed");

    df.show().await.expect("show failed");
    Ok(())
}

fn create_external_table(name: &str) -> Result<CreateExternalTable> {
    static TEMPDIR: LazyLock<TempDir> = LazyLock::new(|| tempfile::tempdir().unwrap());
    static FILE_TYPE: &str = "duckdb";

    let schema = Schema::new(vec![Field::new("i", DataType::Int64, false)]);
    let schema = schema.to_dfschema_ref()?;

    let name = TableReference::bare(name);

    let location = TEMPDIR.path().join(format!("{name}.{FILE_TYPE}"));
    // let location = format!("/tmp/{name}.{FILE_TYPE}");

    Ok(CreateExternalTable {
        schema,
        name,
        location: location.to_string_lossy().to_string(),
        // location,
        file_type: FILE_TYPE.to_string(),
        table_partition_cols: vec![],
        if_not_exists: true,
        temporary: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: HashMap::new(),
        constraints: Constraints::empty(),
        column_defaults: HashMap::new(),
    })
}
