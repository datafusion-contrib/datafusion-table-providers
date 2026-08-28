use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::Constraints;
use datafusion::common::ToDFSchema;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use std::collections::HashMap;
use std::sync::Arc;

use crate::postgres::common;
use crate::postgres::PostgresTableProviderFactory;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use datafusion_table_providers::SOURCE_TYPE_METADATA_KEY;

const COMPLEX_TABLE_SQL: &str = include_str!("scripts/complex_table.sql");

fn get_schema() -> SchemaRef {
    let fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("large_id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int16, true),
        Field::new("height", DataType::Float64, true),
        Field::new("is_active", DataType::Boolean, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("data", DataType::Binary, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ];

    Arc::new(Schema::new(fields))
}

/// [`get_schema`] plus the source type `pg_catalog.format_type` reports for each
/// column of the table the factory creates from it, as field metadata.
fn expected_inferred_schema() -> SchemaRef {
    let source_types = [
        ("id", "integer"),
        ("large_id", "bigint"),
        ("name", "text"),
        ("age", "smallint"),
        ("height", "double precision"),
        ("is_active", "boolean"),
        ("created_at", "timestamp without time zone"),
        ("data", "bytea"),
        ("tags", "text[]"),
    ];

    let fields = get_schema()
        .fields()
        .iter()
        .zip(source_types)
        .map(|(field, (name, source_type))| {
            assert_eq!(field.name(), name, "column order drifted from get_schema()");
            field.as_ref().clone().with_metadata(HashMap::from([(
                SOURCE_TYPE_METADATA_KEY.to_string(),
                source_type.to_string(),
            )]))
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new(fields))
}

#[tokio::test]
async fn test_postgres_schema_inference() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(port)
        .await
        .expect("Postgres container to start");

    let factory = PostgresTableProviderFactory::new();
    let ctx = SessionContext::new();
    let table_name = "test_table";
    let schema = get_schema();

    let cmd = CreateExternalTable {
        schema: schema.to_dfschema_ref().expect("to df schema"),
        name: table_name.into(),
        location: "".to_string(),
        file_type: "".to_string(),
        table_partition_cols: vec![],
        if_not_exists: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: common::get_pg_params(port),
        constraints: Constraints::default(),
        column_defaults: HashMap::new(),
        temporary: false,
        or_replace: false,
    };
    let _ = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );
    let table_factory = PostgresTableFactory::new(postgres_pool);
    let table_provider = table_factory
        .table_provider(TableReference::bare(table_name))
        .await
        .expect("to create table provider");

    assert_eq!(table_provider.schema(), expected_inferred_schema());

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}

#[tokio::test]
async fn test_postgres_schema_inference_complex_types() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(port)
        .await
        .expect("Postgres container to start");

    let table_name = "example_table";

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );

    let pg_conn = postgres_pool
        .connect_direct()
        .await
        .expect("to connect to postgres");
    for cmd in COMPLEX_TABLE_SQL.split(";") {
        pg_conn
            .conn
            .execute(cmd, &[])
            .await
            .expect("to create table");
    }

    let table_factory = PostgresTableFactory::new(postgres_pool);
    let table_provider = table_factory
        .table_provider(TableReference::bare(table_name))
        .await
        .expect("to create table provider");

    // The Arrow mapping is lossy for each of these: precision and scale survive
    // Decimal128 but width, enum identity, and element type do not.
    let schema = table_provider.schema();
    for (column, source_type) in [
        ("numeric_col", "numeric(10,2)"),
        ("varchar_col", "character varying(255)"),
        ("mood_col", "mood"),
        ("text_array_col", "text[]"),
    ] {
        let field = schema.field_with_name(column).expect("column to exist");
        assert_eq!(
            field
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some(source_type),
            "unexpected source type metadata on {column}"
        );
    }

    let pretty_schema = format!("{:#?}", table_provider.schema());
    insta::assert_snapshot!(pretty_schema);

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}

#[tokio::test]
async fn test_postgres_view_schema_inference() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(port)
        .await
        .expect("Postgres container to start");

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );
    let pg_conn = postgres_pool
        .connect_direct()
        .await
        .expect("to connect to postgres");

    for cmd in COMPLEX_TABLE_SQL.split(";") {
        if cmd.trim().is_empty() {
            continue;
        }
        pg_conn
            .conn
            .execute(cmd, &[])
            .await
            .expect("executing SQL from complex_table.sql");
    }

    let table_factory = PostgresTableFactory::new(postgres_pool.clone());
    let table_provider = table_factory
        .table_provider(TableReference::bare("example_view"))
        .await
        .expect("to create table provider for view");

    let pretty_schema = format!("{:#?}", table_provider.schema());
    insta::assert_snapshot!(pretty_schema);

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}

#[tokio::test]
async fn test_postgres_materialized_view_schema_inference() {
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(port)
        .await
        .expect("Postgres container to start");

    let postgres_pool = Arc::new(
        PostgresConnectionPool::new(to_secret_map(common::get_pg_params(port)))
            .await
            .expect("unable to create Postgres connection pool"),
    );
    let pg_conn = postgres_pool
        .connect_direct()
        .await
        .expect("to connect to postgres");

    for cmd in COMPLEX_TABLE_SQL.split(";") {
        if cmd.trim().is_empty() {
            continue;
        }
        pg_conn
            .conn
            .execute(cmd, &[])
            .await
            .expect("executing SQL from complex_table.sql");
    }

    let table_factory = PostgresTableFactory::new(postgres_pool);
    let table_provider = table_factory
        .table_provider(TableReference::bare("example_materialized_view"))
        .await
        .expect("to create table provider for materialized view");

    let pretty_schema = format!("{:#?}", table_provider.schema());
    insta::assert_snapshot!(pretty_schema);

    // Tear down
    container
        .remove()
        .await
        .expect("to stop postgres container");
}
