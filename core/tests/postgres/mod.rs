use crate::{arrow_record_batch_gen::*, docker::RunningContainer};
use arrow::{
    array::{
        Array, Decimal128Array, Decimal128Builder, ListArray, ListBuilder, RecordBatch,
        StringArray, StructArray,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
use datafusion::{catalog::TableProviderFactory, logical_expr::dml::InsertOp};
use datafusion::{
    common::{Constraints, ToDFSchema},
    datasource::memory::MemorySourceConfig,
};
#[cfg(feature = "postgres-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;

use datafusion_table_providers::{
    postgres::{DynPostgresConnectionPool, PostgresTableProviderFactory},
    sql::sql_provider_datafusion::SqlTable,
    UnsupportedTypeAction,
};
use rstest::{fixture, rstest};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

mod common;
mod schema;

async fn arrow_postgres_round_trip(
    port: usize,
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let factory = PostgresTableProviderFactory::new();
    let ctx = SessionContext::new();
    let cmd = CreateExternalTable {
        schema: Arc::new(arrow_record.schema().to_dfschema().expect("to df schema")),
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
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();
    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
        .await
        .expect("insert plan created");

    let _ = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");
    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Postgres returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    #[cfg(feature = "postgres-federation")]
    let casted_result =
        try_cast_to(record_batch[0].clone(), source_schema).expect("Failed to cast record batch");

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    #[cfg(feature = "postgres-federation")]
    assert_eq!(arrow_record, casted_result);
}

struct ContainerManager {
    port: usize,
    claimed: bool,
    running_container: Option<RunningContainer>,
}

impl Drop for ContainerManager {
    fn drop(&mut self) {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(stop_container(self.running_container.take(), self.port));
    }
}

async fn stop_container(running_container: Option<RunningContainer>, port: usize) {
    println!("Stopping Postgres container on port {}", port);
    if let Some(running_container) = running_container {
        if let Err(e) = running_container.stop().await {
            tracing::error!("Error stopping container: {}", e);
        };
    }
}

#[fixture]
#[once]
fn container_manager() -> Mutex<ContainerManager> {
    Mutex::new(ContainerManager {
        port: crate::get_random_port(),
        claimed: false,
        running_container: None,
    })
}

async fn start_container(manager: &mut MutexGuard<'_, ContainerManager>) {
    let running_container =
        common::start_postgres_docker_container("postgres:latest", manager.port, None)
            .await
            .expect("Postgres container to start");

    manager.running_container = Some(running_container);

    tracing::debug!("Container started");
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[case::bytea_array(get_arrow_bytea_array_record_batch(), "bytea_array")]
#[test_log::test(tokio::test)]
async fn test_arrow_postgres_roundtrip(
    container_manager: &Mutex<ContainerManager>,
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&mut container_manager).await;
    }

    arrow_postgres_round_trip(
        container_manager.port,
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_arrow_postgres_one_way(container_manager: &Mutex<ContainerManager>) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&mut container_manager).await;
    }

    test_postgres_enum_type(container_manager.port).await;
    test_postgres_numeric_type(container_manager.port).await;
    test_postgres_numeric_array_type(container_manager.port).await;
    test_postgres_jsonb_type(container_manager.port).await;
    test_postgres_nullability_constraints(container_manager.port).await;
    test_postgres_json_type(container_manager.port).await;
    test_postgres_jsonb_list_struct_with_projected_schema(container_manager.port).await;
    test_postgres_json_list_struct_with_projected_schema(container_manager.port).await;
}

async fn test_postgres_enum_type(port: usize) {
    let extra_stmt = Some("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');");
    let create_table_stmt = "
    CREATE TABLE person_mood (
    mood_status mood NOT NULL
    );";

    let insert_table_stmt = "
    INSERT INTO person_mood (mood_status) VALUES ('happy'), ('sad'), ('neutral');
    ";

    // Create expected record with nullable=false to match NOT NULL constraint
    let mut builder = arrow::array::StringDictionaryBuilder::<arrow::datatypes::Int8Type>::new();
    builder.append_value("happy");
    builder.append_value("sad");
    builder.append_value("neutral");
    let array: arrow::array::DictionaryArray<arrow::datatypes::Int8Type> = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "mood_status",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false, // NOT NULL
    )]));

    let expected_record = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
        .expect("Failed to create arrow dictionary array record batch");

    arrow_postgres_one_way(
        port,
        "person_mood",
        create_table_stmt,
        insert_table_stmt,
        extra_stmt,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn test_postgres_numeric_type(port: usize) {
    let extra_stmt = None;
    let create_table_stmt = "
    CREATE TABLE numeric_values (
    first_column NUMERIC,  -- No precision specified
    second_column NUMERIC  -- No precision specified
);";

    let insert_table_stmt = "
    INSERT INTO numeric_values (first_column, second_column) VALUES
(1.0917217805754313, 0.00000000000000000000),
(0.97824560830666753739, 1220.9175000000000000),
(1.0917217805754313, 52.9533333333333333);
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("first_column", DataType::Decimal128(38, 20), true),
        Field::new("second_column", DataType::Decimal128(38, 20), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                Decimal128Array::from(vec![
                    109172178057543130000i128,
                    97824560830666753739i128,
                    109172178057543130000i128,
                ])
                .with_precision_and_scale(38, 20)
                .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![
                    0i128,
                    122091750000000000000000i128,
                    5295333333333333330000i128,
                ])
                .with_precision_and_scale(38, 20)
                .unwrap(),
            ),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_postgres_one_way(
        port,
        "numeric_values",
        create_table_stmt,
        insert_table_stmt,
        extra_stmt,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn test_postgres_numeric_array_type(port: usize) {
    let create_table_stmt = "
    CREATE TABLE numeric_array_values (
    numeric_values NUMERIC[]
);";

    let insert_table_stmt = "
    INSERT INTO numeric_array_values (numeric_values) VALUES
(ARRAY[1.2300::NUMERIC, 42::NUMERIC, NULL::NUMERIC, -0.0045::NUMERIC]),
(NULL),
(ARRAY[]::NUMERIC[]),
(ARRAY[100.1::NUMERIC]);
    ";

    let decimal_item_type = DataType::Decimal128(38, 20);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "numeric_values",
        DataType::List(Arc::new(Field::new(
            "item",
            decimal_item_type.clone(),
            true,
        ))),
        true,
    )]));

    let mut numeric_array_builder = ListBuilder::new(
        Decimal128Builder::new()
            .with_precision_and_scale(38, 20)
            .expect("Failed to create Decimal128Builder with expected precision and scale"),
    );

    numeric_array_builder
        .values()
        .append_value(123000000000000000000);
    numeric_array_builder
        .values()
        .append_value(4200000000000000000000);
    numeric_array_builder.values().append_null();
    numeric_array_builder
        .values()
        .append_value(-450000000000000000);
    numeric_array_builder.append(true);
    numeric_array_builder.append_null();
    numeric_array_builder.append(true);
    numeric_array_builder
        .values()
        .append_value(10010000000000000000000);
    numeric_array_builder.append(true);

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(numeric_array_builder.finish())],
    )
    .expect("Failed to create expected record batch for NUMERIC[]");

    arrow_postgres_one_way(
        port,
        "numeric_array_values",
        create_table_stmt,
        insert_table_stmt,
        None,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn test_postgres_jsonb_type(port: usize) {
    let create_table_stmt = "
    CREATE TABLE jsonb_values (
        id INT PRIMARY KEY,
        data JSONB
    );";

    let insert_table_stmt = r#"
    INSERT INTO jsonb_values (id, data) VALUES
    (1, '{"name": "John", "age": 30}'),
    (2, '{"name": "Jane", "age": 25}'),
    (3, '[1, 2, 3]'),
    (4, 'null'),
    (5, '{"nested": {"key": "value"}}');
    "#;

    let expected_values: Vec<Value> = vec![
        serde_json::from_str(r#"{"name":"John","age":30}"#).unwrap(),
        serde_json::from_str(r#"{"name":"Jane","age":25}"#).unwrap(),
        serde_json::from_str("[1,2,3]").unwrap(),
        serde_json::from_str("null").unwrap(),
        serde_json::from_str(r#"{"nested":{"key":"value"}}"#).unwrap(),
    ];
    let batches = query_postgres_one_way(
        port,
        "jsonb_values",
        create_table_stmt,
        insert_table_stmt,
        None,
        UnsupportedTypeAction::String,
        Some("SELECT data FROM jsonb_values ORDER BY id"),
    )
    .await;
    assert_eq!(batches.len(), 1);

    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column should be StringArray");

    assert_eq!(col.len(), expected_values.len());
    for (i, expected) in expected_values.iter().enumerate() {
        let actual: Value =
            serde_json::from_str(col.value(i)).expect("actual value should be valid JSON");
        assert_eq!(&actual, expected, "mismatch at row {i}");
    }
}

/// Guards that plain JSON columns (not JSONB) still round-trip as Utf8 through
/// `JsonbRawString` without the serde_json::Value intermediate.
async fn test_postgres_json_type(port: usize) {
    let create_table_stmt = "
    CREATE TABLE json_values (
        id INT PRIMARY KEY,
        data JSON
    );";

    let insert_table_stmt = r#"
    INSERT INTO json_values (id, data) VALUES
    (1, '{"name": "Alice"}'),
    (2, '[1, 2]'),
    (3, 'null');
    "#;

    let expected_values: Vec<Value> = vec![
        serde_json::from_str(r#"{"name":"Alice"}"#).unwrap(),
        serde_json::from_str("[1,2]").unwrap(),
        serde_json::from_str("null").unwrap(),
    ];
    let batches = query_postgres_one_way(
        port,
        "json_values",
        create_table_stmt,
        insert_table_stmt,
        None,
        UnsupportedTypeAction::String,
        Some("SELECT data FROM json_values ORDER BY id"),
    )
    .await;
    assert_eq!(batches.len(), 1);

    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column should be StringArray");

    assert_eq!(col.len(), expected_values.len());
    for (i, expected) in expected_values.iter().enumerate() {
        let actual: Value =
            serde_json::from_str(col.value(i)).expect("actual value should be valid JSON");
        assert_eq!(&actual, expected, "mismatch at row {i}");
    }
}

async fn test_postgres_json_list_struct_projected(port: usize, sql_type: &str) {
    let table_name = format!("{sql_type}_list_struct_values").to_lowercase();

    let create_table_stmt = format!(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            data {sql_type}
        );"
    );

    let insert_table_stmt = format!(
        r#"INSERT INTO {table_name} (id, data) VALUES
            (1, '[{{"id":"u1","email":"one@example.com"}},{{"id":"u2","email":"two@example.com"}}]'),
            (2, '[]'),
            (3, null);
        "#
    );

    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("Postgres connection pool should be created")
        .with_unsupported_type_action(UnsupportedTypeAction::String);

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    let _ = db_conn
        .conn
        .execute(&create_table_stmt, &[])
        .await
        .expect("Postgres table should be created");

    let _ = db_conn
        .conn
        .execute(&insert_table_stmt, &[])
        .await
        .expect("Postgres table data should be inserted");

    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new(
            "data",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Utf8, true),
                        Field::new("email", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        ),
    ]));

    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new_with_schema(
        "postgres",
        &sqltable_pool,
        Arc::clone(&projected_schema),
        &table_name,
        None,
    );
    ctx.register_table(table_name.as_str(), Arc::new(table))
        .expect("Table should be registered");

    let df = ctx
        .sql(&format!("SELECT id, data FROM {table_name} ORDER BY id"))
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), 3);

    let data_col = record_batch[0]
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("data should decode to ListArray");

    assert!(!data_col.is_null(0));
    assert_eq!(data_col.value_length(0), 2);
    assert!(!data_col.is_null(1));
    assert_eq!(data_col.value_length(1), 0);
    assert!(data_col.is_null(2));

    let row_one_values = data_col.value(0);
    let row_one_struct = row_one_values
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("row 1 values should be StructArray");
    let ids = row_one_struct
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("id field should be StringArray");
    let emails = row_one_struct
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("email field should be StringArray");

    assert_eq!(ids.value(0), "u1");
    assert_eq!(ids.value(1), "u2");
    assert_eq!(emails.value(0), "one@example.com");
    assert_eq!(emails.value(1), "two@example.com");
}

async fn test_postgres_jsonb_list_struct_with_projected_schema(port: usize) {
    test_postgres_json_list_struct_projected(port, "JSONB").await;
}

async fn test_postgres_json_list_struct_with_projected_schema(port: usize) {
    test_postgres_json_list_struct_projected(port, "JSON").await;
}

/// Validates that [`PostgresConnectionPool::new_with_password_provider`] produces
/// a working pool by creating a table, inserting, and querying through the provider path.
#[rstest]
#[test_log::test(tokio::test)]
async fn test_password_provider_pool(container_manager: &Mutex<ContainerManager>) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&mut container_manager).await;
    }

    let pool = common::get_postgres_pool_with_password_provider(container_manager.port)
        .await
        .expect("Pool with password provider should be created");

    // Verify pool works: get a connection, create a table, insert, query
    let conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    conn.conn
        .execute(
            "CREATE TABLE IF NOT EXISTS password_provider_test (id INT, name TEXT)",
            &[],
        )
        .await
        .expect("Table should be created");

    conn.conn
        .execute(
            "INSERT INTO password_provider_test VALUES (1, 'hello')",
            &[],
        )
        .await
        .expect("Insert should succeed");

    let rows = conn
        .conn
        .query("SELECT id, name FROM password_provider_test", &[])
        .await
        .expect("Query should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "hello");

    // Also verify it works through the SqlTable (DataFusion) path
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, "password_provider_test", None)
        .await
        .expect("SqlTable should be created");

    let ctx = SessionContext::new();
    ctx.register_table("password_provider_test", Arc::new(table))
        .expect("Table should be registered");

    let df = ctx
        .sql("SELECT * FROM password_provider_test")
        .await
        .expect("Query should execute");
    let batches = df.collect().await.expect("Results should be collected");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

async fn test_postgres_nullability_constraints(port: usize) {
    let create_table_stmt = "
        CREATE TABLE nullability_table (
            id INTEGER NOT NULL,
            name VARCHAR(50) NOT NULL,
            age INTEGER,
            email VARCHAR(100),
            score NUMERIC(5, 2) NOT NULL
        );
    ";
    let insert_table_stmt = "
        INSERT INTO nullability_table (id, name, age, email, score)
        VALUES
        (1, 'Alice', 30, 'alice@example.com', 95.50),
        (2, 'Bob', NULL, NULL, 87.25),
        (3, 'Charlie', 25, 'charlie@example.com', 92.00);
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("score", DataType::Decimal128(5, 2), false),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec![
                "Alice", "Bob", "Charlie",
            ])),
            Arc::new(arrow::array::Int32Array::from(vec![
                Some(30),
                None,
                Some(25),
            ])),
            Arc::new(arrow::array::StringArray::from(vec![
                Some("alice@example.com"),
                None,
                Some("charlie@example.com"),
            ])),
            Arc::new(
                Decimal128Array::from(vec![i128::from(9550), i128::from(8725), i128::from(9200)])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create expected arrow record batch");

    arrow_postgres_one_way(
        port,
        "nullability_table",
        create_table_stmt,
        insert_table_stmt,
        None,
        expected_record,
        UnsupportedTypeAction::default(),
    )
    .await;
}

async fn arrow_postgres_one_way(
    port: usize,
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    extra_stmt: Option<&str>,
    expected_record: RecordBatch,
    unsupported_type_action: UnsupportedTypeAction,
) {
    let record_batch = query_postgres_one_way(
        port,
        table_name,
        create_table_stmt,
        insert_table_stmt,
        extra_stmt,
        unsupported_type_action,
        None,
    )
    .await;

    assert_eq!(record_batch[0], expected_record);
}

async fn query_postgres_one_way(
    port: usize,
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    extra_stmt: Option<&str>,
    unsupported_type_action: UnsupportedTypeAction,
    query: Option<&str>,
) -> Vec<RecordBatch> {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("Postgres connection pool should be created")
        .with_unsupported_type_action(unsupported_type_action);

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    if let Some(extra_stmt) = extra_stmt {
        let _ = db_conn
            .conn
            .execute(extra_stmt, &[])
            .await
            .expect("Statement should be created");
    }

    let _ = db_conn
        .conn
        .execute(create_table_stmt, &[])
        .await
        .expect("Postgres table should be created");

    let _ = db_conn
        .conn
        .execute(insert_table_stmt, &[])
        .await
        .expect("Postgres table data should be inserted");

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, table_name, None)
        .await
        .expect("Table should be created");
    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");
    let sql = query
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("SELECT * FROM {table_name}"));
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    df.collect().await.expect("RecordBatch should be collected")
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_postgres_io_runtime_segregation(container_manager: &Mutex<ContainerManager>) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&mut container_manager).await;
    }

    // Create a separate IO runtime
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("IO runtime should be created");

    let pool = common::get_postgres_connection_pool(container_manager.port)
        .await
        .expect("pool created")
        .with_io_runtime(io_runtime.handle().clone());

    // Verify the pool works through the IO runtime
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let conn = sqltable_pool.connect().await.expect("connect should work");
    let async_conn = conn.as_async().expect("should be async connection");
    // Execute a simple query to confirm IO runtime is functional
    let stream = async_conn
        .query_arrow("SELECT 1 AS val", &[], None)
        .await
        .expect("query should work");
    let batches: Vec<_> = futures::StreamExt::collect(stream).await;
    assert!(!batches.is_empty(), "should return results via IO runtime");

    io_runtime.shutdown_background();
}

// ── DML integration tests (delete_from / update) ──────────────────────────────

use arrow::array::{Int32Array, UInt64Array};
use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::prelude::{col, lit};
use datafusion_table_providers::postgres::{write::PostgresTableWriter, Postgres};

/// Helper: create a test table, insert rows, return a `PostgresTableWriter`.
async fn setup_dml_test_table(
    port: usize,
    table_name: &str,
    rows: &[(i32, &str)],
) -> (Arc<dyn TableProvider>, SessionContext) {
    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("pool");

    let db_conn = pool.connect_direct().await.expect("connect");

    // Create table
    db_conn
        .conn
        .execute(
            &format!(r#"CREATE TABLE IF NOT EXISTS "{table_name}" (id INT, name TEXT)"#),
            &[],
        )
        .await
        .expect("create table");

    // Insert rows
    for (id, name) in rows {
        db_conn
            .conn
            .execute(
                &format!(r#"INSERT INTO "{table_name}" VALUES ({id}, '{name}')"#),
                &[],
            )
            .await
            .expect("insert row");
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let pool = Arc::new(pool);
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::clone(&pool) as _;
    let read_provider = Arc::new(
        SqlTable::new("postgres", &sqltable_pool, table_name, None)
            .await
            .expect("SqlTable"),
    );

    let postgres = Postgres::new(table_name.into(), pool, schema, Constraints::default());
    let writer = PostgresTableWriter::create(read_provider, postgres, None);

    let ctx = SessionContext::new();
    (writer as Arc<dyn TableProvider>, ctx)
}

/// Helper: execute a DML plan and extract the u64 count.
async fn extract_dml_count(plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> u64 {
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("UInt64Array")
        .value(0)
}

/// Helper: query a table and return (ids, names) sorted by id.
async fn query_dml_table(port: usize, table_name: &str) -> (Vec<i32>, Vec<String>) {
    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("pool");
    let db_conn = pool.connect_direct().await.expect("connect");
    let rows = db_conn
        .conn
        .query(
            &format!(r#"SELECT id, name FROM "{table_name}" ORDER BY id"#),
            &[],
        )
        .await
        .expect("query");

    let ids: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    let names: Vec<String> = rows.iter().map(|r| r.get(1)).collect();
    (ids, names)
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_postgres_delete_from_with_filter(container_manager: &Mutex<ContainerManager>) {
    let mut cm = container_manager.lock().await;
    if !cm.claimed {
        cm.claimed = true;
        start_container(&mut cm).await;
    }
    let port = cm.port;

    let (writer, ctx) =
        setup_dml_test_table(port, "dml_delete_filter", &[(1, "a"), (2, "b"), (3, "c")]).await;

    let plan = writer
        .delete_from(&ctx.state(), vec![col("id").eq(lit(2))])
        .await
        .expect("delete_from");
    let count = extract_dml_count(plan).await;
    assert_eq!(count, 1);

    let (ids, names) = query_dml_table(port, "dml_delete_filter").await;
    assert_eq!(ids, vec![1, 3]);
    assert_eq!(names, vec!["a", "c"]);
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_postgres_delete_from_empty_filters(container_manager: &Mutex<ContainerManager>) {
    let mut cm = container_manager.lock().await;
    if !cm.claimed {
        cm.claimed = true;
        start_container(&mut cm).await;
    }
    let port = cm.port;

    let (writer, ctx) =
        setup_dml_test_table(port, "dml_delete_empty", &[(1, "a"), (2, "b"), (3, "c")]).await;

    let plan = writer
        .delete_from(&ctx.state(), vec![])
        .await
        .expect("delete_from");
    let count = extract_dml_count(plan).await;
    assert_eq!(count, 0);

    let (ids, _) = query_dml_table(port, "dml_delete_empty").await;
    assert_eq!(ids, vec![1, 2, 3]);
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_postgres_update_with_filter(container_manager: &Mutex<ContainerManager>) {
    let mut cm = container_manager.lock().await;
    if !cm.claimed {
        cm.claimed = true;
        start_container(&mut cm).await;
    }
    let port = cm.port;

    let (writer, ctx) =
        setup_dml_test_table(port, "dml_update_filter", &[(1, "a"), (2, "b"), (3, "c")]).await;

    let plan = writer
        .update(
            &ctx.state(),
            vec![("name".to_string(), lit("updated"))],
            vec![col("id").eq(lit(2))],
        )
        .await
        .expect("update");
    let count = extract_dml_count(plan).await;
    assert_eq!(count, 1);

    let (ids, names) = query_dml_table(port, "dml_update_filter").await;
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(names, vec!["a", "updated", "c"]);
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_postgres_update_without_filter(container_manager: &Mutex<ContainerManager>) {
    let mut cm = container_manager.lock().await;
    if !cm.claimed {
        cm.claimed = true;
        start_container(&mut cm).await;
    }
    let port = cm.port;

    let (writer, ctx) = setup_dml_test_table(port, "dml_update_all", &[(1, "a"), (2, "b")]).await;

    let plan = writer
        .update(&ctx.state(), vec![("name".to_string(), lit("all"))], vec![])
        .await
        .expect("update");
    let count = extract_dml_count(plan).await;
    assert_eq!(count, 2);

    let (_, names) = query_dml_table(port, "dml_update_all").await;
    assert_eq!(names, vec!["all", "all"]);
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_postgres_partitioned_table_query(container_manager: &Mutex<ContainerManager>) {
    let mut cm = container_manager.lock().await;
    if !cm.claimed {
        cm.claimed = true;
        start_container(&mut cm).await;
    }
    let port = cm.port;

    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("Postgres connection pool should be created");

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    db_conn
        .conn
        .execute(
            "CREATE TABLE partitioned_orders (
                id INTEGER NOT NULL,
                order_date DATE NOT NULL,
                amount NUMERIC(10,2),
                description TEXT
            ) PARTITION BY RANGE (order_date)",
            &[],
        )
        .await
        .expect("Partitioned table should be created");

    db_conn
        .conn
        .execute(
            "CREATE TABLE partitioned_orders_2024 PARTITION OF partitioned_orders
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            &[],
        )
        .await
        .expect("2024 partition should be created");

    db_conn
        .conn
        .execute(
            "CREATE TABLE partitioned_orders_2025 PARTITION OF partitioned_orders
                FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
            &[],
        )
        .await
        .expect("2025 partition should be created");

    db_conn
        .conn
        .execute(
            "INSERT INTO partitioned_orders (id, order_date, amount, description) VALUES
                (1, '2024-06-15', 100.50, 'order A'),
                (2, '2025-03-20', 200.75, 'order B')",
            &[],
        )
        .await
        .expect("Data should be inserted");

    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, "partitioned_orders", None)
        .await
        .expect("Table should be created");
    ctx.register_table("partitioned_orders", Arc::new(table))
        .expect("Table should be registered");

    let df = ctx
        .sql("SELECT * FROM partitioned_orders ORDER BY id")
        .await
        .expect("DataFrame should be created");
    let batches = df.collect().await.expect("RecordBatch should be collected");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should return rows from both partitions");
}
