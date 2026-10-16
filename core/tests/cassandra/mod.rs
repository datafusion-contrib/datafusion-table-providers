use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::cassandra::{CassandraConnectionPool, CassandraTableFactory};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use linktime::{ctor, dtor};

use crate::docker::ContainerManager;

mod common;

const KEYSPACE: &str = "it_ks";

/// The columns of the `all_types` table, which has a column of every supported CQL type.
const ALL_TYPES_COLUMNS: &str = "
    id int PRIMARY KEY,
    a_ascii ascii,
    a_bigint bigint,
    a_blob blob,
    a_boolean boolean,
    a_date date,
    a_decimal decimal,
    a_double double,
    a_duration duration,
    a_float float,
    a_inet inet,
    a_list list<int>,
    a_map map<text, int>,
    a_set set<text>,
    a_smallint smallint,
    a_text text,
    a_time time,
    a_timestamp timestamp,
    a_timeuuid timeuuid,
    a_tinyint tinyint,
    a_tuple tuple<int, text>,
    a_udt frozen<address>,
    a_uuid uuid,
    a_varint varint,
    a_vector vector<float, 3>
";

static CONTAINER_MANAGER_INSTANCE: Mutex<Option<ContainerManager>> = Mutex::new(None);

#[ctor(unsafe)]
fn global_setup() {
    let mut guard = CONTAINER_MANAGER_INSTANCE.lock().unwrap();
    *guard = Some(ContainerManager::default());
}

#[dtor(unsafe)]
fn global_teardown() {
    let mut guard = CONTAINER_MANAGER_INSTANCE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(container_manager) = guard.take() {
        drop(container_manager);
    }
}

/// All Cassandra tests share one container: Cassandra takes too long to start for one per test.
#[test_log::test(tokio::test)]
async fn test_cassandra() {
    let port = {
        let mut guard = CONTAINER_MANAGER_INSTANCE.lock().unwrap();
        let container_manager = guard.as_mut().unwrap();
        container_manager
            .start_container(common::start_cassandra_docker_container)
            .await;
        container_manager.port
    };

    let pool = Arc::new(
        common::get_cassandra_connection_pool(port)
            .await
            .expect("Cassandra connection pool should be created"),
    );
    create_test_data(&pool).await;

    test_cassandra_types(&pool).await;
    test_cassandra_projection_filter_and_limit(&pool).await;
    test_cassandra_count(&pool).await;
    test_cassandra_explain_shows_pushed_down_cql(&pool).await;
    test_cassandra_tables_and_schemas(&pool).await;
    test_cassandra_missing_table(&pool).await;
    test_cassandra_connection_pool_with_schema(port).await;
    test_cassandra_insert_all_types(&pool).await;
    test_cassandra_insert_values(&pool).await;
}

async fn execute(pool: &CassandraConnectionPool, cql: &str) {
    pool.connect_direct()
        .execute(cql, &[])
        .await
        .unwrap_or_else(|e| panic!("CQL should be executed: {cql}: {e}"));
}

async fn create_test_data(pool: &CassandraConnectionPool) {
    execute(
        pool,
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ),
    )
    .await;
    execute(
        pool,
        &format!("CREATE TYPE IF NOT EXISTS {KEYSPACE}.address (street text, zip int)"),
    )
    .await;
    for table in ["all_types", "all_types_written"] {
        execute(
            pool,
            &format!("CREATE TABLE IF NOT EXISTS {KEYSPACE}.{table} ({ALL_TYPES_COLUMNS})"),
        )
        .await;
    }
    execute(
        pool,
        &format!(
            "INSERT INTO {KEYSPACE}.all_types (
                id, a_ascii, a_bigint, a_blob, a_boolean, a_date, a_decimal, a_double, a_duration,
                a_float, a_inet, a_list, a_map, a_set, a_smallint, a_text, a_time, a_timestamp,
                a_timeuuid, a_tinyint, a_tuple, a_udt, a_uuid, a_varint, a_vector
            ) VALUES (
                1, 'ascii', 9000000000, 0xcafe, true, '2024-01-15', 123.45, 1.5, 1mo2d3ns,
                2.5, '127.0.0.1', [1, 2, 3], {{'k': 1}}, {{'a', 'b'}}, 32000, 'text',
                '13:14:15.123456789', '2024-01-15T10:20:30.123Z',
                50554d6e-29bb-11e5-b345-feff819cdc9f, 100, (7, 'seven'),
                {{street: 'Main', zip: 12345}}, 123e4567-e89b-12d3-a456-426614174000,
                123456789012345678901234567890, [1.0, 2.0, 3.0]
            )"
        ),
    )
    .await;
    // all columns but the key are null
    execute(
        pool,
        &format!("INSERT INTO {KEYSPACE}.all_types (id) VALUES (2)"),
    )
    .await;

    execute(
        pool,
        &format!(
            "CREATE TABLE IF NOT EXISTS {KEYSPACE}.users (id int PRIMARY KEY, name text, age int)"
        ),
    )
    .await;
    execute(
        pool,
        &format!(
            "CREATE TABLE IF NOT EXISTS {KEYSPACE}.users_written (id int PRIMARY KEY, name text, age int)"
        ),
    )
    .await;
    for (id, name, age) in [
        (1, "alice", 31),
        (2, "bob", 25),
        (3, "carol", 42),
        (4, "dave", 35),
        (5, "eve", 19),
    ] {
        execute(
            pool,
            &format!("INSERT INTO {KEYSPACE}.users (id, name, age) VALUES ({id}, '{name}', {age})"),
        )
        .await;
    }
}

async fn register_table(ctx: &SessionContext, pool: &Arc<CassandraConnectionPool>, table: &str) {
    let table_provider = CassandraTableFactory::new(Arc::clone(pool))
        .table_provider(TableReference::partial(KEYSPACE, table))
        .await
        .expect("table provider should be created");
    ctx.register_table(table, table_provider)
        .expect("table should be registered");
}

async fn register_writable_table(
    ctx: &SessionContext,
    pool: &Arc<CassandraConnectionPool>,
    table: &str,
) {
    let table_provider = CassandraTableFactory::new(Arc::clone(pool))
        .read_write_table_provider(TableReference::partial(KEYSPACE, table))
        .await
        .expect("table provider should be created");
    ctx.register_table(table, table_provider)
        .expect("table should be registered");
}

/// Runs an `INSERT` and returns the number of inserted rows that it reports.
async fn insert(ctx: &SessionContext, sql: &str) -> u64 {
    let batches = query(ctx, sql).await;
    array_value_to_string(batches[0].column(0), 0)
        .unwrap()
        .parse()
        .unwrap()
}

async fn query_to_string(ctx: &SessionContext, sql: &str) -> String {
    arrow::util::pretty::pretty_format_batches(&query(ctx, sql).await)
        .unwrap()
        .to_string()
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("query should be planned")
        .collect()
        .await
        .expect("query should be executed")
}

async fn test_cassandra_types(pool: &Arc<CassandraConnectionPool>) {
    let ctx = SessionContext::new();
    register_table(&ctx, pool, "all_types").await;

    let batches = query(&ctx, "SELECT * FROM all_types ORDER BY id").await;
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .expect("batches should be concatenated");
    assert_eq!(batch.num_rows(), 2);

    let item = |data_type| Arc::new(Field::new("item", data_type, true));
    let expected: Vec<(&str, DataType, &str)> = vec![
        ("id", DataType::Int32, "1"),
        ("a_ascii", DataType::Utf8, "ascii"),
        ("a_bigint", DataType::Int64, "9000000000"),
        ("a_blob", DataType::Binary, "cafe"),
        ("a_boolean", DataType::Boolean, "true"),
        ("a_date", DataType::Date32, "2024-01-15"),
        ("a_decimal", DataType::Utf8, "123.45"),
        ("a_double", DataType::Float64, "1.5"),
        (
            "a_duration",
            DataType::Interval(IntervalUnit::MonthDayNano),
            "1 mons 2 days 0.000000003 secs",
        ),
        ("a_float", DataType::Float32, "2.5"),
        ("a_inet", DataType::Utf8, "127.0.0.1"),
        ("a_list", DataType::List(item(DataType::Int32)), "[1, 2, 3]"),
        (
            "a_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            "{k: 1}",
        ),
        ("a_set", DataType::List(item(DataType::Utf8)), "[a, b]"),
        ("a_smallint", DataType::Int16, "32000"),
        ("a_text", DataType::Utf8, "text"),
        (
            "a_time",
            DataType::Time64(TimeUnit::Nanosecond),
            "13:14:15.123456789",
        ),
        (
            "a_timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            "2024-01-15T10:20:30.123Z",
        ),
        (
            "a_timeuuid",
            DataType::Utf8,
            "50554d6e-29bb-11e5-b345-feff819cdc9f",
        ),
        ("a_tinyint", DataType::Int8, "100"),
        (
            "a_tuple",
            DataType::Struct(
                vec![
                    Field::new("0", DataType::Int32, true),
                    Field::new("1", DataType::Utf8, true),
                ]
                .into(),
            ),
            "{0: 7, 1: seven}",
        ),
        (
            "a_udt",
            DataType::Struct(
                vec![
                    Field::new("street", DataType::Utf8, true),
                    Field::new("zip", DataType::Int32, true),
                ]
                .into(),
            ),
            "{street: Main, zip: 12345}",
        ),
        (
            "a_uuid",
            DataType::Utf8,
            "123e4567-e89b-12d3-a456-426614174000",
        ),
        ("a_varint", DataType::Utf8, "123456789012345678901234567890"),
        (
            "a_vector",
            DataType::FixedSizeList(item(DataType::Float32), 3),
            "[1.0, 2.0, 3.0]",
        ),
    ];

    let schema = batch.schema();
    assert_eq!(schema.fields().len(), expected.len());
    for (i, (name, data_type, value)) in expected.into_iter().enumerate() {
        let field = schema.field(i);
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &data_type, "type of {name}");
        assert!(field.is_nullable(), "{name} should be nullable");

        let column = batch.column(i);
        assert_eq!(
            array_value_to_string(column, 0).expect("value should be formatted"),
            value,
            "value of {name}"
        );
        if name != "id" {
            assert!(column.is_null(1), "{name} should be null in the 2nd row");
        }
    }
}

async fn test_cassandra_projection_filter_and_limit(pool: &Arc<CassandraConnectionPool>) {
    let ctx = SessionContext::new();
    register_table(&ctx, pool, "users").await;

    // the filter isn't valid CQL (age is not a key column), so DataFusion must apply it
    let batches = query(
        &ctx,
        "SELECT name, age FROM users WHERE age > 30 ORDER BY age DESC",
    )
    .await;
    let rows: Vec<(String, String)> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (
                    array_value_to_string(batch.column(0), row).unwrap(),
                    array_value_to_string(batch.column(1), row).unwrap(),
                )
            })
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("carol".to_string(), "42".to_string()),
            ("dave".to_string(), "35".to_string()),
            ("alice".to_string(), "31".to_string()),
        ]
    );

    let batches = query(&ctx, "SELECT id FROM users LIMIT 2").await;
    let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(num_rows, 2);
    assert_eq!(batches[0].schema().fields().len(), 1);

    let batches = query(&ctx, "SELECT id FROM users LIMIT 0").await;
    let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(num_rows, 0);
}

async fn test_cassandra_count(pool: &Arc<CassandraConnectionPool>) {
    let ctx = SessionContext::new();
    register_table(&ctx, pool, "users").await;

    let batches = query(&ctx, "SELECT count(*) FROM users").await;
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "5");

    let batches = query(&ctx, "SELECT count(*) FROM users WHERE age < 30").await;
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "2");
}

async fn test_cassandra_explain_shows_pushed_down_cql(pool: &Arc<CassandraConnectionPool>) {
    let ctx = SessionContext::new();
    register_table(&ctx, pool, "users").await;

    let batches = query(&ctx, "EXPLAIN SELECT name FROM users LIMIT 3").await;
    let plan = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        plan.contains(r#"CassandraExec cql=SELECT "name" FROM "it_ks"."users" LIMIT 3"#),
        "the projection and limit should be pushed down:\n{plan}"
    );
}

async fn test_cassandra_tables_and_schemas(pool: &Arc<CassandraConnectionPool>) {
    let conn = pool.connect().await.expect("connection should be created");
    let conn = conn.as_async().expect("connection should be async");

    let keyspaces = conn.schemas().await.expect("keyspaces should be listed");
    assert!(keyspaces.contains(&KEYSPACE.to_string()), "{keyspaces:?}");
    assert!(keyspaces.contains(&"system".to_string()), "{keyspaces:?}");

    let mut tables = conn
        .tables(KEYSPACE)
        .await
        .expect("tables should be listed");
    tables.sort();
    assert_eq!(
        tables,
        vec![
            "all_types".to_string(),
            "all_types_written".to_string(),
            "users".to_string(),
            "users_written".to_string(),
        ]
    );
}

async fn test_cassandra_missing_table(pool: &Arc<CassandraConnectionPool>) {
    let result = CassandraTableFactory::new(Arc::clone(pool))
        .table_provider(TableReference::partial(KEYSPACE, "does_not_exist"))
        .await;
    let error = result.expect_err("a missing table should be an error");
    assert!(
        error.to_string().contains("does_not_exist"),
        "the error should name the table: {error}"
    );
}

/// A pool created with `with_schema` must return the provided schema from `get_schema`
/// instead of inferring it from the database.
async fn test_cassandra_connection_pool_with_schema(port: u16) {
    let table_reference = TableReference::partial(KEYSPACE, "users");

    // the schema is inferred from the database when none is provided
    let pool = common::get_cassandra_connection_pool(port)
        .await
        .expect("Cassandra connection pool should be created");
    let inferred = pool
        .connect_direct()
        .get_schema(&table_reference)
        .await
        .expect("schema should be inferred");
    assert_eq!(
        inferred.as_ref(),
        &Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("age", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])
    );

    // the provided schema is used as-is, also to convert the values
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::LargeUtf8, true),
    ]));
    let pool = Arc::new(
        common::get_cassandra_connection_pool(port)
            .await
            .expect("Cassandra connection pool should be created")
            .with_schema(table_reference.clone(), Arc::clone(&schema)),
    );

    let actual = pool
        .connect_direct()
        .get_schema(&table_reference)
        .await
        .expect("schema should be returned");
    assert_eq!(actual, schema);

    let conn = pool.connect().await.expect("connection should be created");
    let actual = conn
        .as_async()
        .expect("connection should be async")
        .get_schema(&table_reference)
        .await
        .expect("schema should be returned");
    assert_eq!(actual, schema);

    let ctx = SessionContext::new();
    register_table(&ctx, &pool, "users").await;
    let batches = query(&ctx, "SELECT id, name FROM users WHERE id = 3").await;
    assert_eq!(batches[0].schema(), schema);
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "3");
    assert_eq!(
        array_value_to_string(batches[0].column(1), 0).unwrap(),
        "carol"
    );
}

/// Copying a table with every supported CQL type through `INSERT INTO ... SELECT` must give
/// back the same data.
async fn test_cassandra_insert_all_types(pool: &Arc<CassandraConnectionPool>) {
    let ctx = SessionContext::new();
    register_table(&ctx, pool, "all_types").await;
    register_writable_table(&ctx, pool, "all_types_written").await;

    let inserted = insert(
        &ctx,
        "INSERT INTO all_types_written SELECT * FROM all_types",
    )
    .await;
    assert_eq!(inserted, 2);

    let expected = query_to_string(&ctx, "SELECT * FROM all_types ORDER BY id").await;
    let actual = query_to_string(&ctx, "SELECT * FROM all_types_written ORDER BY id").await;
    assert_eq!(actual, expected);
}

async fn test_cassandra_insert_values(pool: &Arc<CassandraConnectionPool>) {
    let ctx = SessionContext::new();
    register_writable_table(&ctx, pool, "users_written").await;

    // Cassandra orders the columns as the primary key, then the other columns by name, so
    // the columns of users_written are (id, age, name)
    let inserted = insert(
        &ctx,
        "INSERT INTO users_written VALUES (1, 31, 'alice'), (2, NULL, 'bob')",
    )
    .await;
    assert_eq!(inserted, 2);
    // the columns that are not given are null
    let inserted = insert(
        &ctx,
        "INSERT INTO users_written (id, name) VALUES (3, 'carol')",
    )
    .await;
    assert_eq!(inserted, 1);
    assert_eq!(
        query_to_string(&ctx, "SELECT id, name, age FROM users_written ORDER BY id").await,
        "+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | alice | 31  |
| 2  | bob   |     |
| 3  | carol |     |
+----+-------+-----+"
    );

    // as in CQL, inserting an existing primary key replaces the row
    let inserted = insert(
        &ctx,
        "INSERT INTO users_written (id, name, age) VALUES (1, 'alicia', 32)",
    )
    .await;
    assert_eq!(inserted, 1);
    assert_eq!(
        query_to_string(&ctx, "SELECT id, name, age FROM users_written WHERE id = 1").await,
        "+----+--------+-----+
| id | name   | age |
+----+--------+-----+
| 1  | alicia | 32  |
+----+--------+-----+"
    );

    // INSERT OVERWRITE replaces all rows
    let inserted = insert(
        &ctx,
        "INSERT OVERWRITE users_written (id, name, age) VALUES (4, 'dave', 35)",
    )
    .await;
    assert_eq!(inserted, 1);
    assert_eq!(
        query_to_string(&ctx, "SELECT id, name, age FROM users_written").await,
        "+----+------+-----+
| id | name | age |
+----+------+-----+
| 4  | dave | 35  |
+----+------+-----+"
    );

    // Cassandra rejects a null primary key
    let error = ctx
        .sql("INSERT INTO users_written (id, name) VALUES (NULL, 'nobody')")
        .await
        .expect("query should be planned")
        .collect()
        .await
        .expect_err("a null primary key should be rejected");
    assert!(
        error.to_string().contains("users_written"),
        "the error should name the table: {error}"
    );
}
