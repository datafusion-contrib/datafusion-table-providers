use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::clickhouse::{Arg, ClickHouseTableFactory};

/// Example illustrates on how to use clickhouse client as a table factory
/// and create read only table providers which can be registered with datafusion session.
#[tokio::main]
async fn main() {
    let client = clickhouse::Client::default()
        .with_url("http://localhost:8123")
        .with_user("admin")
        .with_password("secret");

    // Create a Datafusion session.
    let ctx = SessionContext::new();

    // Create a Clickhouse table factory
    let table_factory = ClickHouseTableFactory::new(client);

    // Using table factory, we can create table provider that queries a clickhouse table
    let base_table = table_factory
        .table_provider(TableReference::bare("Reports"), None)
        .await
        .unwrap();

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the Clickhouse "Reports" table as "reports_v1"
    ctx.register_table("reports_v1", base_table).unwrap();

    // Using table factory, we can create table provider that queries a parameterized view in clickhouse with some arguments.
    let view_table = table_factory
        .table_provider(
            TableReference::bare("Users"),
            Some(vec![(
                "workspace_uid".to_string(),
                Arg::String("abc".to_string()),
            )]),
        )
        .await
        .unwrap();

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the "Users('abc')" view as "users"
    ctx.register_table("users", view_table).unwrap();

    let df = ctx
        .sql("SELECT * FROM datafusion.public.reports_v1")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");

    let df = ctx
        .sql("SELECT * FROM datafusion.public.users")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
