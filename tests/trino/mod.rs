use datafusion::{error::DataFusionError, execution::context::SessionContext};
use rstest::rstest;
use std::sync::Arc;

use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, TimeUnit},
};

use crate::docker::RunningContainer;

mod common;

async fn test_trino_datetime_types(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.datetime_table (
            timestamp_field TIMESTAMP,
            timestamp_with_tz TIMESTAMP WITH TIME ZONE,
            date_field DATE,
            time_field TIME
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.datetime_table VALUES
        (TIMESTAMP '2024-09-12 10:00:00', TIMESTAMP '2024-09-12 10:00:00 UTC', DATE '2024-09-12', TIME '10:00:00')
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_field",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "timestamp_with_tz",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("date_field", DataType::Date32, true),
        Field::new("time_field", DataType::Time32(TimeUnit::Millisecond), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_000])),
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_000]).with_timezone("UTC")),
            Arc::new(Date32Array::from(vec![19978])),
            Arc::new(Time32MillisecondArray::from(vec![36_000_000])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "datetime_table", expected_record).await;
}

async fn test_trino_numeric_types(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.numeric_table (
            int_field INTEGER,
            bigint_field BIGINT,
            double_field DOUBLE,
            decimal_field DECIMAL(18,6)
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.numeric_table VALUES
        (2147483647, 9223372036854775807, 3.14159265359, DECIMAL '123.456000')
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new("int_field", DataType::Int32, true),
        Field::new("bigint_field", DataType::Int64, true),
        Field::new("double_field", DataType::Float64, true),
        Field::new("decimal_field", DataType::Decimal128(18, 6), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2147483647i32])),
            Arc::new(Int64Array::from(vec![9223372036854775807i64])),
            Arc::new(Float64Array::from(vec![3.14159265359])),
            Arc::new(
                Decimal128Array::from(vec![Some(123456000i128)])
                    .with_precision_and_scale(18, 6)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "numeric_table", expected_record).await;
}

async fn test_trino_string_types(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.string_table (
            name VARCHAR,
            description VARCHAR,
            notes VARCHAR
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.string_table VALUES
        ('Alice', 'Software Engineer', NULL),
        ('Bob', 'Data Scientist', 'Likes Trino')
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("notes", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec![
                "Software Engineer",
                "Data Scientist",
            ])),
            Arc::new(StringArray::from(vec![None, Some("Likes Trino")])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "string_table", expected_record).await;
}

async fn test_trino_boolean_types(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.boolean_table (
            is_active BOOLEAN,
            is_verified BOOLEAN,
            is_premium BOOLEAN
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.boolean_table VALUES
        (true, false, NULL),
        (false, true, true)
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new("is_active", DataType::Boolean, true),
        Field::new("is_verified", DataType::Boolean, true),
        Field::new("is_premium", DataType::Boolean, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(BooleanArray::from(vec![false, true])),
            Arc::new(BooleanArray::from(vec![None, Some(true)])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "boolean_table", expected_record).await;
}

async fn test_trino_binary_types(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.binary_table (
            binary_data VARBINARY,
            file_content VARBINARY
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.binary_table VALUES
        (X'68656c6c6f20776f726c64', X'62696e6172792066696c6520636f6e74656e74')
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new("binary_data", DataType::Binary, true),
        Field::new("file_content", DataType::Binary, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"hello world"])),
            Arc::new(BinaryArray::from_vec(vec![b"binary file content"])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "binary_table", expected_record).await;
}

async fn test_trino_array_types(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.array_table (
            string_tags ARRAY(VARCHAR),
            int_numbers ARRAY(INTEGER),
            empty_array ARRAY(VARCHAR)
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.array_table VALUES
        (ARRAY['rust', 'trino', 'arrow'], ARRAY[1, 2, 3], ARRAY[]),
        (ARRAY['python', 'sql'], ARRAY[4, 5], ARRAY[])
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "string_tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "int_numbers",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "empty_array",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let string_tags_builder = ListBuilder::new(StringBuilder::new());
    let mut string_tags_list = string_tags_builder;

    string_tags_list.values().append_value("rust");
    string_tags_list.values().append_value("trino");
    string_tags_list.values().append_value("arrow");
    string_tags_list.append(true);

    string_tags_list.values().append_value("python");
    string_tags_list.values().append_value("sql");
    string_tags_list.append(true);

    let string_tags_array = Arc::new(string_tags_list.finish());

    let int_numbers_builder = ListBuilder::new(Int32Builder::new());
    let mut int_numbers_list = int_numbers_builder;

    int_numbers_list.values().append_value(1);
    int_numbers_list.values().append_value(2);
    int_numbers_list.values().append_value(3);
    int_numbers_list.append(true);

    int_numbers_list.values().append_value(4);
    int_numbers_list.values().append_value(5);
    int_numbers_list.append(true);

    let int_numbers_array = Arc::new(int_numbers_list.finish());

    let empty_array_builder = ListBuilder::new(StringBuilder::new());
    let mut empty_array_list = empty_array_builder;

    empty_array_list.append(true);
    empty_array_list.append(true);

    let empty_array_array = Arc::new(empty_array_list.finish());

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![string_tags_array, int_numbers_array, empty_array_array],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "array_table", expected_record).await;
}

async fn test_trino_null_and_missing_fields(port: usize) {
    let client = common::get_trino_client(port)
        .await
        .expect("Trino client should be created");

    let create_table_sql = r#"
        CREATE TABLE memory.default.null_fields_table (
            name VARCHAR,
            age INTEGER,
            email VARCHAR,
            phone VARCHAR
        )
    "#;

    client
        .execute(create_table_sql)
        .await
        .expect("Table should be created");

    let insert_sql = r#"
        INSERT INTO memory.default.null_fields_table VALUES
        ('Alice', 30, 'alice@example.com', NULL),
        ('Bob', NULL, NULL, '555-1234'),
        ('Charlie', 25, NULL, NULL)
    "#;

    client
        .execute(insert_sql)
        .await
        .expect("Data should be inserted");

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![Some(30), None, Some(25)])),
            Arc::new(StringArray::from(vec![
                Some("alice@example.com"),
                None,
                None,
            ])),
            Arc::new(StringArray::from(vec![None, Some("555-1234"), None])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_trino_one_way(port, "null_fields_table", expected_record).await;
}

async fn arrow_trino_one_way(
    port: usize,
    table_name: &str,
    expected_record: RecordBatch,
) -> Vec<RecordBatch> {
    tracing::debug!("Running Trino tests on {table_name}");

    let ctx = SessionContext::new();

    let trino_conn_pool = common::get_trino_connection_pool(port)
        .await
        .expect("Trino connection pool should be created");

    let table = TrinoTable::new(
        &Arc::new(trino_conn_pool),
        format!("memory.default.{table_name}"),
    )
    .await
    .expect("Table should be created");

    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");

    let schema_ref = expected_record.schema();
    let expected_fields: Vec<&str> = schema_ref
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let projection = expected_fields
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {projection} FROM {table_name}");

    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batches = df.collect().await.expect("RecordBatch should be collected");
    assert_eq!(record_batches.len(), 1);

    let actual_projected =
        project_record_batch(&record_batches[0], &expected_fields).expect("Project actual");
    let expected_projected =
        project_record_batch(&expected_record, &expected_fields).expect("Project expected");

    assert_eq!(actual_projected, expected_projected);

    record_batches
}

use datafusion::common::Result as DFResult;
use datafusion_table_providers::trino::sql_table::TrinoTable;

fn project_record_batch(batch: &RecordBatch, columns: &[&str]) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let indices: Vec<usize> = columns
        .iter()
        .map(|col| schema.index_of(col).expect("Column not found"))
        .collect();
    let arrays = indices.iter().map(|&i| batch.column(i).clone()).collect();
    let fields = indices
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect::<Vec<_>>();
    let projected_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(projected_schema, arrays).map_err(|e| DataFusionError::ArrowError(e, None))
}

async fn start_trino_container(port: usize) -> RunningContainer {
    let running_container = common::start_trino_docker_container(port)
        .await
        .expect("Trino container to start");

    tracing::debug!("Trino Container started");

    running_container
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_trino_arrow_oneway() {
    let port = crate::get_random_port();
    let trino_container = start_trino_container(port).await;

    test_trino_datetime_types(port).await;
    test_trino_numeric_types(port).await;
    test_trino_string_types(port).await;
    test_trino_boolean_types(port).await;
    test_trino_binary_types(port).await;
    test_trino_array_types(port).await;
    test_trino_null_and_missing_fields(port).await;

    trino_container.remove().await.expect("container to stop");
}
