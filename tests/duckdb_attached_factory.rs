use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    catalog::TableProviderFactory,
    common::{Constraints, ToDFSchema},
    logical_expr::CreateExternalTable,
    prelude::{SessionConfig, SessionContext},
    sql::TableReference,
};
use datafusion_table_providers::duckdb::{
    attached_factory::AttachedDuckDBTableProviderFactory, DuckDBTableProviderFactory,
};
use duckdb::AccessMode;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};
use tempfile::TempDir;
use tokio::task;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_multiple_table_providers() -> Result<()> {
    let factory = DuckDBTableProviderFactory::new(AccessMode::ReadWrite);
    let factory = Arc::new(
        AttachedDuckDBTableProviderFactory::new(factory)
            .await
            .expect("Failed to create factory"),
    );

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    let state = ctx.state();

    const NUM_TABLES: usize = 10;
    const NUM_ROWS: usize = 1000;
    let mut tasks = vec![];

    for i in 0..NUM_TABLES {
        let factory = factory.clone();
        let state = state.clone();
        let ctx = ctx.clone();

        let task = task::spawn(async move {
            // Silly names
            let table_name = format!(
                "scalar[TimestampSecond][0]=1740787200&scalar[TimestampSecond][1]&exprs_hash={i}"
            );
            let cmd = create_external_table(&table_name)?;
            let table = factory.create(&state, &cmd).await?;

            ctx.register_table(&table_name, table)?;

            let mut insert_values = String::new();
            for j in 0..NUM_ROWS {
                if j > 0 {
                    insert_values.push_str(", ");
                }
                insert_values.push_str(&format!("({})", i * NUM_ROWS + j));
            }
            let insert_sql = format!("INSERT INTO \"{table_name}\" (i) VALUES {insert_values}");
            let df = ctx.sql(&insert_sql).await?;
            df.collect().await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}

fn create_external_table(name: &str) -> Result<CreateExternalTable> {
    static TEMPDIR: LazyLock<TempDir> = LazyLock::new(|| tempfile::tempdir().unwrap());
    static FILE_TYPE: &str = "duckdb";

    let schema = Schema::new(vec![Field::new("i", DataType::Int64, false)]);
    let schema = schema.to_dfschema_ref()?;

    let name = TableReference::bare(name);
    let location = TEMPDIR.path().join(format!("{name}.{FILE_TYPE}"));

    Ok(CreateExternalTable {
        schema,
        name,
        location: location.to_string_lossy().to_string(),
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
