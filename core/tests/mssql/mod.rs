use std::sync::{Arc, Mutex};

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraint, Constraints, TableReference, ToDFSchema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::mssql::{MsSqlTableFactory, MsSqlTableProviderFactory};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::mssqlconn::{
    MsSqlParameter, MsSqlPooledConnection,
};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection,
};
use datafusion_table_providers::sql::db_connection_pool::mssqlpool::MsSqlConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use linktime::{ctor, dtor};
use rstest::rstest;
use secrecy::ExposeSecret;

use crate::arrow_record_batch_gen::*;
use crate::docker::{ContainerManager, RunningContainer};

mod common;

/// Owns the SQL Server container shared by all the tests, removed when the test binary exits.
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

fn container_port() -> u16 {
    CONTAINER_MANAGER_INSTANCE
        .lock()
        .unwrap()
        .as_ref()
        .expect("ContainerManager to be initialized")
        .port
}

fn register_container(running_container: RunningContainer) {
    let mut guard = CONTAINER_MANAGER_INSTANCE.lock().unwrap();
    let container_manager = guard.as_mut().expect("ContainerManager to be initialized");
    container_manager.claimed = true;
    container_manager.running_container = Some(running_container);
}

async fn execute(pool: &MsSqlConnectionPool, sql: &str) {
    let conn = pool.connect_direct().await.expect("connection");
    conn.execute_raw(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql} should execute: {e}"));
}

/// Creates a table with `create_stmt` (after dropping it) and fills it with `insert_stmt`.
async fn create_table(
    pool: &MsSqlConnectionPool,
    table: &str,
    create_stmt: &str,
    insert_stmt: &str,
) {
    execute(pool, &format!("DROP TABLE IF EXISTS [{table}]")).await;
    execute(pool, create_stmt).await;
    if !insert_stmt.is_empty() {
        execute(pool, insert_stmt).await;
    }
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} should be planned: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql} should be executed: {e}"))
}

fn int_values(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
}

/// The values of the first column, as strings: SQL `VARCHAR` columns are `Utf8View`.
fn string_values(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let column = arrow::compute::cast(b.column(0), &DataType::Utf8).expect("cast");
            column
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn count(batches: &[RecordBatch]) -> u64 {
    batches[0]
        .column(0)
        .as_primitive::<arrow::datatypes::UInt64Type>()
        .value(0)
}

/// Reads a table created by SQL Server and compares it with the expected batch.
async fn arrow_mssql_one_way(
    table: &str,
    create_stmt: &str,
    insert_stmt: &str,
    expected: RecordBatch,
) {
    let pool = common::get_mssql_connection_pool().await;
    create_table(&pool, table, create_stmt, insert_stmt).await;

    let factory = MsSqlTableFactory::new(Arc::clone(&pool));
    let provider = factory
        .table_provider(TableReference::bare(table))
        .await
        .expect("table provider should be created");
    assert_eq!(provider.schema(), expected.schema(), "schema of {table}");

    let ctx = SessionContext::new();
    ctx.register_table(table, provider)
        .expect("table registered");
    let batches = query(&ctx, &format!("SELECT * FROM {table}")).await;
    assert_eq!(batches.len(), 1);
    for (i, field) in expected.schema().fields().iter().enumerate() {
        assert_eq!(
            batches[0].column(i).as_ref(),
            expected.column(i).as_ref(),
            "values of {table}.{}",
            field.name()
        );
    }
}

async fn test_mssql_integer_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("bit_col", DataType::Boolean, true),
        Field::new("tinyint_col", DataType::UInt8, true),
        Field::new("smallint_col", DataType::Int16, true),
        Field::new("int_col", DataType::Int32, false),
        Field::new("bigint_col", DataType::Int64, true),
    ]));
    let expected = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(UInt8Array::from(vec![Some(255), None])),
            Arc::new(Int16Array::from(vec![Some(-32_768), None])),
            Arc::new(Int32Array::from(vec![2_147_483_647, 0])),
            Arc::new(Int64Array::from(vec![
                Some(-9_223_372_036_854_775_808),
                None,
            ])),
        ],
    )
    .unwrap();
    arrow_mssql_one_way(
        "integer_types",
        "CREATE TABLE integer_types (bit_col BIT, tinyint_col TINYINT, smallint_col SMALLINT, \
         int_col INT NOT NULL, bigint_col BIGINT)",
        "INSERT INTO integer_types VALUES (1, 255, -32768, 2147483647, -9223372036854775808), \
         (NULL, NULL, NULL, 0, NULL)",
        expected,
    )
    .await;
}

async fn test_mssql_numeric_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("real_col", DataType::Float32, true),
        Field::new("float_col", DataType::Float64, true),
        Field::new("decimal_col", DataType::Decimal128(10, 3), true),
        Field::new("numeric_col", DataType::Decimal128(38, 0), true),
        Field::new("money_col", DataType::Decimal128(19, 4), true),
        Field::new("smallmoney_col", DataType::Decimal128(10, 4), true),
    ]));
    let expected = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Float32Array::from(vec![Some(1.5), None])),
            Arc::new(Float64Array::from(vec![Some(-2.25), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1_234_567), None])
                    .with_precision_and_scale(10, 3)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(99_999_999_999_999_999_999_999_999_999_999_999_999),
                    None,
                ])
                .with_precision_and_scale(38, 0)
                .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(-123_456_789), None])
                    .with_precision_and_scale(19, 4)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(12_345), None])
                    .with_precision_and_scale(10, 4)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    arrow_mssql_one_way(
        "numeric_types",
        "CREATE TABLE numeric_types (real_col REAL, float_col FLOAT, decimal_col DECIMAL(10, 3), \
         numeric_col NUMERIC(38, 0), money_col MONEY, smallmoney_col SMALLMONEY)",
        "INSERT INTO numeric_types VALUES (1.5, -2.25, 1234.567, 99999999999999999999999999999999999999, \
         -12345.6789, 1.2345), (NULL, NULL, NULL, NULL, NULL, NULL)",
        expected,
    )
    .await;
}

async fn test_mssql_string_and_binary_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("char_col", DataType::Utf8, true),
        Field::new("varchar_col", DataType::Utf8, true),
        Field::new("nvarchar_col", DataType::Utf8, true),
        Field::new("text_col", DataType::Utf8, true),
        Field::new("guid_col", DataType::Utf8, true),
        Field::new("binary_col", DataType::Binary, true),
        Field::new("varbinary_col", DataType::Binary, true),
    ]));
    let expected = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![Some("ab  "), None])),
            Arc::new(StringArray::from(vec![Some("hello"), None])),
            Arc::new(StringArray::from(vec![Some("資料融合 ✓"), None])),
            Arc::new(StringArray::from(vec![Some("a long text"), None])),
            Arc::new(StringArray::from(vec![
                Some("6F9619FF-8B86-D011-B42D-00C04FC964FF"),
                None,
            ])),
            Arc::new(BinaryArray::from(vec![Some(&[1_u8, 2, 0][..]), None])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0xde_u8, 0xad, 0xbe, 0xef][..]),
                None,
            ])),
        ],
    )
    .unwrap();
    arrow_mssql_one_way(
        "string_types",
        "CREATE TABLE string_types (char_col CHAR(4), varchar_col VARCHAR(20), \
         nvarchar_col NVARCHAR(MAX), text_col TEXT, guid_col UNIQUEIDENTIFIER, \
         binary_col BINARY(3), varbinary_col VARBINARY(MAX))",
        "INSERT INTO string_types VALUES ('ab', 'hello', N'資料融合 ✓', 'a long text', \
         '6F9619FF-8B86-D011-B42D-00C04FC964FF', 0x0102, 0xDEADBEEF), \
         (NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        expected,
    )
    .await;
}

async fn test_mssql_date_and_time_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("date_col", DataType::Date32, true),
        Field::new("time_col", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new(
            "datetime_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "smalldatetime_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "datetime2_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "datetimeoffset_col",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
            true,
        ),
    ]));
    let expected = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            // 2024-09-12, and 0001-01-01 is the first SQL Server date
            Arc::new(Date32Array::from(vec![Some(19_978), Some(-719_162), None])),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(36_000_123_456_700),
                Some(0),
                None,
            ])),
            // `datetime` is rounded to 1/300 s: .123 is stored as .1233333
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_726_135_200_123_333),
                Some(0),
                None,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_726_135_200_000_000),
                Some(0),
                None,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_726_135_200_123_456),
                Some(-62_135_596_800_000_000),
                None,
            ])),
            Arc::new(
                // 10:00 at +02:00 is 08:00 UTC
                TimestampMicrosecondArray::from(vec![Some(1_726_128_000_123_456), Some(0), None])
                    .with_timezone("+00:00"),
            ),
        ],
    )
    .unwrap();
    arrow_mssql_one_way(
        "datetime_types",
        "CREATE TABLE datetime_types (date_col DATE, time_col TIME(7), datetime_col DATETIME, \
         smalldatetime_col SMALLDATETIME, datetime2_col DATETIME2(7), \
         datetimeoffset_col DATETIMEOFFSET(7))",
        "INSERT INTO datetime_types VALUES \
         ('2024-09-12', '10:00:00.1234567', '2024-09-12 10:00:00.123', '2024-09-12 10:00:00', \
          '2024-09-12 10:00:00.123456', '2024-09-12 10:00:00.123456 +02:00'), \
         ('0001-01-01', '00:00:00', '1970-01-01', '1970-01-01', '0001-01-01', '1970-01-01 00:00:00 +00:00'), \
         (NULL, NULL, NULL, NULL, NULL, NULL)",
        expected,
    )
    .await;
}

/// The schema of a table in another schema of the database.
async fn test_mssql_schema_qualified_table() {
    let pool = common::get_mssql_connection_pool().await;
    execute(
        &pool,
        "IF SCHEMA_ID(N'sales') IS NULL EXEC('CREATE SCHEMA sales')",
    )
    .await;
    execute(&pool, "DROP TABLE IF EXISTS sales.orders").await;
    execute(
        &pool,
        "CREATE TABLE sales.orders (id INT, amount DECIMAL(10, 2))",
    )
    .await;
    execute(
        &pool,
        "INSERT INTO sales.orders VALUES (1, 10.5), (2, 20.25)",
    )
    .await;

    let factory = MsSqlTableFactory::new(Arc::clone(&pool));
    let provider = factory
        .table_provider(TableReference::partial("sales", "orders"))
        .await
        .expect("table provider should be created");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("table registered");
    let batches = query(&ctx, "SELECT id FROM orders WHERE amount > 15").await;
    assert_eq!(int_values(&batches), vec![2]);

    let conn = pool.connect_direct().await.expect("connection");
    let tables = conn.tables("sales").await.expect("tables");
    assert_eq!(tables, vec!["orders".to_string()]);
    let schemas = conn.schemas().await.expect("schemas");
    assert!(schemas.contains(&"sales".to_string()), "{schemas:?}");
    assert!(schemas.contains(&"dbo".to_string()), "{schemas:?}");
}

async fn test_mssql_missing_table() {
    let pool = common::get_mssql_connection_pool().await;
    let factory = MsSqlTableFactory::new(pool);
    let err = factory
        .table_provider(TableReference::bare("no_such_table"))
        .await
        .expect_err("a missing table must be reported");
    assert!(err.to_string().contains("no_such_table"), "{err}");
}

#[test_log::test(tokio::test)]
async fn test_mssql_arrow_oneway() {
    test_mssql_integer_types().await;
    test_mssql_numeric_types().await;
    test_mssql_string_and_binary_types().await;
    test_mssql_date_and_time_types().await;
    test_mssql_schema_qualified_table().await;
    test_mssql_missing_table().await;
}

/// Creates the table with `CREATE EXTERNAL TABLE`, inserts the batch and reads it back.
async fn arrow_mssql_round_trip(
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let port = common::ensure_mssql_container().await;
    let pool = common::get_mssql_connection_pool().await;
    execute(&pool, &format!("DROP TABLE IF EXISTS [{table_name}]")).await;

    let factory = MsSqlTableProviderFactory::new();
    let ctx = SessionContext::new();
    let cmd = CreateExternalTable::builder(
        table_name,
        "",
        "mssql",
        Arc::clone(&arrow_record.schema())
            .to_dfschema_ref()
            .expect("df schema"),
    )
    .with_options(
        common::get_mssql_params(port, Some(common::MSSQL_DATABASE))
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect(),
    )
    .build();
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Overwrite)
        .await
        .expect("insert plan created");
    collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");

    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");
    let batches = query(&ctx, &format!("SELECT * FROM {table_name}")).await;
    assert_eq!(batches.len(), 1);
    let casted =
        try_cast_to(batches[0].clone(), source_schema).expect("Failed to cast record batch");
    assert_eq!(arrow_record, casted);
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::timestamp_without_timezone(
    get_arrow_timestamp_record_batch_without_timezone(),
    "timestamp_without_timezone"
)]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[test_log::test(tokio::test)]
async fn test_arrow_mssql_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_mssql_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_roundtrip"),
    )
    .await;
}

/// SQL Server DECIMAL has at most 38 digits.
#[test_log::test(tokio::test)]
async fn test_arrow_mssql_decimal_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("decimal32", DataType::Decimal32(9, 2), false),
        Field::new("decimal64", DataType::Decimal64(18, 6), false),
        Field::new("decimal128", DataType::Decimal128(38, 10), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                Decimal32Array::from(vec![123, -222, 0])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal64Array::from(vec![123, 222_000_000, -1])
                    .with_precision_and_scale(18, 6)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(i128::from(i64::MAX) * 1_000), None, Some(-321)])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    arrow_mssql_round_trip(batch, schema, "decimal_roundtrip").await;
}

/// More rows than fit in one `INSERT` statement, and more parameters than one request takes.
#[test_log::test(tokio::test)]
async fn test_mssql_insert_many_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let rows = 5_000;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..rows).map(|i| format!("row {i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let port = common::ensure_mssql_container().await;
    let pool = common::get_mssql_connection_pool().await;
    execute(&pool, "DROP TABLE IF EXISTS many_rows").await;
    let ctx = SessionContext::new();
    ctx.state_ref().write().table_factories_mut().insert(
        "MSSQL".to_string(),
        Arc::new(MsSqlTableProviderFactory::new()),
    );
    let options = common::get_mssql_params(port, Some(common::MSSQL_DATABASE))
        .into_iter()
        .map(|(k, v)| format!("'{k}' '{}'", v.expose_secret()))
        .collect::<Vec<_>>()
        .join(", ");
    query(
        &ctx,
        &format!(
            "CREATE EXTERNAL TABLE many_rows (id INT NOT NULL, label VARCHAR) STORED AS MSSQL \
             LOCATION '' OPTIONS ({options})"
        ),
    )
    .await;
    ctx.register_batch("source", batch).expect("registered");

    let inserted = query(&ctx, "INSERT INTO many_rows SELECT * FROM source").await;
    assert_eq!(count(&inserted), 5_000);
    let total = query(&ctx, "SELECT COUNT(*) FROM many_rows").await;
    assert_eq!(
        total[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        5_000
    );
    let max = query(&ctx, "SELECT MAX(label) FROM many_rows").await;
    assert_eq!(string_values(&max), vec!["row 999"]);
}

/// The rows of a failed `INSERT` must be rolled back.
#[test_log::test(tokio::test)]
async fn test_mssql_insert_is_atomic() {
    let pool = common::get_mssql_connection_pool().await;
    create_table(
        &pool,
        "atomic_insert",
        "CREATE TABLE atomic_insert (id INT PRIMARY KEY)",
        "INSERT INTO atomic_insert VALUES (1)",
    )
    .await;
    let factory = MsSqlTableFactory::new(Arc::clone(&pool));
    let provider = factory
        .read_write_table_provider(TableReference::bare("atomic_insert"))
        .await
        .expect("provider");
    let ctx = SessionContext::new();
    ctx.register_table("atomic_insert", provider)
        .expect("registered");

    let result = ctx
        .sql("INSERT INTO atomic_insert VALUES (2), (1)")
        .await
        .expect("planned")
        .collect()
        .await;
    assert!(result.is_err(), "the duplicate key must fail the INSERT");
    let ids = query(&ctx, "SELECT id FROM atomic_insert").await;
    assert_eq!(int_values(&ids), vec![1]);
}

#[test_log::test(tokio::test)]
async fn test_mssql_on_conflict() {
    let port = common::ensure_mssql_container().await;
    let pool = common::get_mssql_connection_pool().await;
    execute(&pool, "DROP TABLE IF EXISTS upsert_test").await;
    execute(&pool, "DROP TABLE IF EXISTS do_nothing_test").await;

    let ctx = SessionContext::new();
    ctx.state_ref().write().table_factories_mut().insert(
        "MSSQL".to_string(),
        Arc::new(MsSqlTableProviderFactory::new()),
    );
    let options = common::get_mssql_params(port, Some(common::MSSQL_DATABASE))
        .into_iter()
        .map(|(k, v)| format!("'{k}' '{}'", v.expose_secret()))
        .collect::<Vec<_>>()
        .join(", ");

    query(
        &ctx,
        &format!(
            "CREATE EXTERNAL TABLE upsert_test (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id)) \
             STORED AS MSSQL LOCATION '' OPTIONS ({options}, 'on_conflict' 'upsert:id')"
        ),
    )
    .await;
    query(&ctx, "INSERT INTO upsert_test VALUES (1, 'a'), (2, 'b')").await;
    query(&ctx, "INSERT INTO upsert_test VALUES (2, 'B'), (3, 'C')").await;
    let batches = query(&ctx, "SELECT name FROM upsert_test ORDER BY id").await;
    assert_eq!(string_values(&batches), vec!["a", "B", "C"]);

    query(
        &ctx,
        &format!(
            "CREATE EXTERNAL TABLE do_nothing_test (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id)) \
             STORED AS MSSQL LOCATION '' OPTIONS ({options}, 'on_conflict' 'do_nothing_all', \
             'indexes' 'name:unique')"
        ),
    )
    .await;
    query(
        &ctx,
        "INSERT INTO do_nothing_test VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    query(
        &ctx,
        "INSERT INTO do_nothing_test VALUES (2, 'B'), (3, 'c')",
    )
    .await;
    let batches = query(&ctx, "SELECT name FROM do_nothing_test ORDER BY id").await;
    assert_eq!(string_values(&batches), vec!["a", "b", "c"]);

    // the unique index is created
    let result = ctx
        .sql("INSERT INTO do_nothing_test VALUES (4, 'a')")
        .await
        .expect("planned")
        .collect()
        .await;
    assert!(
        result.is_err(),
        "the unique index must reject a duplicate name"
    );
}

async fn seed(pool: &MsSqlConnectionPool, table: &str) {
    let values = (1..=20)
        .map(|i| format!("({i}, 'row-{i:02}', {})", i % 2))
        .collect::<Vec<_>>()
        .join(", ");
    create_table(
        pool,
        table,
        &format!("CREATE TABLE [{table}] (id INT NOT NULL, label NVARCHAR(32) NOT NULL, even BIT)"),
        &format!("INSERT INTO [{table}] VALUES {values}"),
    )
    .await;
}

async fn read_write_context(table: &str) -> SessionContext {
    let pool = common::get_mssql_connection_pool().await;
    seed(&pool, table).await;
    let factory = MsSqlTableFactory::new(pool);
    let provider = factory
        .read_write_table_provider(TableReference::bare(table))
        .await
        .expect("a read-write provider should be built");
    let ctx = SessionContext::new();
    ctx.register_table(table, provider).expect("registered");
    ctx
}

#[test_log::test(tokio::test)]
async fn test_mssql_delete_and_update() {
    let ctx = read_write_context("dml_test").await;

    let deleted = query(&ctx, "DELETE FROM dml_test WHERE id > 17").await;
    assert_eq!(count(&deleted), 3);
    let ids = query(&ctx, "SELECT id FROM dml_test ORDER BY id").await;
    assert_eq!(int_values(&ids), (1..=17).collect::<Vec<_>>());

    // a boolean column as a predicate, and a non-ASCII string literal
    let updated = query(
        &ctx,
        "UPDATE dml_test SET label = '変更', id = id + 100 WHERE even AND id < 5",
    )
    .await;
    assert_eq!(count(&updated), 2, "ids 1 and 3 are odd");
    let ids = query(
        &ctx,
        "SELECT id FROM dml_test WHERE label = '変更' ORDER BY id",
    )
    .await;
    assert_eq!(int_values(&ids), vec![101, 103]);

    let updated = query(&ctx, "UPDATE dml_test SET even = false").await;
    assert_eq!(count(&updated), 17);
    let deleted = query(&ctx, "DELETE FROM dml_test").await;
    assert_eq!(count(&deleted), 17);
    let ids = query(&ctx, "SELECT id FROM dml_test").await;
    assert!(int_values(&ids).is_empty());
}

#[test_log::test(tokio::test)]
async fn test_mssql_insert_overwrite() {
    let ctx = read_write_context("overwrite_test").await;
    query(
        &ctx,
        "INSERT OVERWRITE overwrite_test VALUES (42, 'only', true)",
    )
    .await;
    let ids = query(&ctx, "SELECT id FROM overwrite_test").await;
    assert_eq!(int_values(&ids), vec![42]);
}

/// LIMIT, ORDER BY and filters are pushed down to SQL Server, as T-SQL.
#[test_log::test(tokio::test)]
async fn test_mssql_sort_limit_and_filter_pushdown() {
    let ctx = read_write_context("pushdown_test").await;

    let df = ctx
        .sql("SELECT id FROM pushdown_test WHERE id > 2 AND even LIMIT 3")
        .await
        .expect("planned");
    let plan = df
        .clone()
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(!plan.contains("LimitExec"), "{plan}");
    assert!(!plan.contains("FilterExec"), "{plan}");
    assert!(plan.contains("LIMIT 3"), "{plan}");
    let batches = df.collect().await.expect("executed");
    assert_eq!(int_values(&batches).len(), 3);

    let batches = query(
        &ctx,
        "SELECT id FROM pushdown_test ORDER BY id DESC LIMIT 5",
    )
    .await;
    assert_eq!(int_values(&batches), vec![20, 19, 18, 17, 16]);
    let batches = query(
        &ctx,
        "SELECT id FROM pushdown_test WHERE label LIKE 'row-1%' ORDER BY id LIMIT 3 OFFSET 2",
    )
    .await;
    assert_eq!(int_values(&batches), vec![12, 13, 14]);
    let batches = query(
        &ctx,
        "SELECT id FROM pushdown_test WHERE even IS NOT TRUE AND id IN (1, 2, 3, 4) ORDER BY id",
    )
    .await;
    assert_eq!(int_values(&batches), vec![2, 4]);
}

/// In a federated session the joins and aggregations of tables in the same database are
/// executed by SQL Server.
#[test_log::test(tokio::test)]
async fn test_mssql_federation() {
    let pool = common::get_mssql_connection_pool().await;
    create_table(
        &pool,
        "fed_customers",
        "CREATE TABLE fed_customers (id INT PRIMARY KEY, name NVARCHAR(50), active BIT)",
        "INSERT INTO fed_customers VALUES (1, N'Ana', 1), (2, N'Bo', 0), (3, N'Chloé', 1)",
    )
    .await;
    create_table(
        &pool,
        "fed_orders",
        "CREATE TABLE fed_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10, 2))",
        "INSERT INTO fed_orders VALUES (1, 1, 10.00), (2, 1, 5.50), (3, 2, 7.00), (4, 3, 1.25)",
    )
    .await;

    let factory = MsSqlTableFactory::new(Arc::clone(&pool));
    let ctx = SessionContext::new_with_state(datafusion_federation::default_session_state());
    for table in ["fed_customers", "fed_orders"] {
        ctx.register_table(
            table,
            factory
                .table_provider(TableReference::bare(table))
                .await
                .expect("table provider"),
        )
        .expect("registered");
    }

    let sql = "SELECT c.name, SUM(o.amount) AS total, COUNT(*) AS orders \
               FROM fed_customers c JOIN fed_orders o ON c.id = o.customer_id \
               WHERE c.active AND c.name <> 'Zoë' \
               GROUP BY c.name ORDER BY total DESC NULLS LAST LIMIT 10";
    let df = ctx.sql(sql).await.expect("planned");
    let plan = df
        .clone()
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(plan.contains("VirtualExecutionPlan"), "{plan}");
    assert!(
        !plan.contains("HashJoinExec"),
        "the join must be pushed down: {plan}"
    );
    assert!(
        !plan.contains("AggregateExec"),
        "the aggregation must be pushed down: {plan}"
    );

    let batches = df.collect().await.expect("executed");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(string_values(&batches), vec!["Ana", "Chloé"]);

    let batches = query(&ctx, "SELECT COUNT(*) FROM fed_orders").await;
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        4
    );

    // a subquery and a window function
    let batches = query(
        &ctx,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount) AS rn \
         FROM fed_orders WHERE customer_id IN (SELECT id FROM fed_customers WHERE active = true) \
         ORDER BY id",
    )
    .await;
    assert_eq!(int_values(&batches), vec![1, 2, 4]);
}

/// A pool created with `with_schema` must return the provided schema instead of inferring it.
#[test_log::test(tokio::test)]
async fn test_mssql_connection_pool_with_schema() {
    let pool = common::get_mssql_connection_pool().await;
    create_table(
        &pool,
        "with_schema_table",
        "CREATE TABLE with_schema_table (id INT PRIMARY KEY, name NVARCHAR(50))",
        "INSERT INTO with_schema_table VALUES (1, NULL), (2, N'a')",
    )
    .await;
    let table_reference = TableReference::bare("with_schema_table");

    let conn = pool.connect_direct().await.expect("connection");
    let inferred = conn
        .get_schema(&table_reference)
        .await
        .expect("schema inferred");
    assert_eq!(
        inferred.as_ref(),
        &Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let other_table_reference = TableReference::bare("other_table");
    let other_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        true,
    )]));
    let port = common::ensure_mssql_container().await;
    let pool =
        MsSqlConnectionPool::new(common::get_mssql_params(port, Some(common::MSSQL_DATABASE)))
            .await
            .expect("pool")
            .with_schema(table_reference.clone(), Arc::clone(&schema))
            .with_schema(other_table_reference.clone(), Arc::clone(&other_schema));

    let conn: Box<dyn DbConnection<MsSqlPooledConnection, MsSqlParameter>> =
        pool.connect().await.expect("connection");
    let conn = conn.as_async().expect("async connection");
    assert_eq!(
        conn.get_schema(&table_reference).await.expect("schema"),
        schema
    );
    assert_eq!(
        conn.get_schema(&other_table_reference)
            .await
            .expect("schema"),
        other_schema
    );

    // the provided schema is used to read the table: the INT column is read as Int64
    let pool = Arc::new(pool);
    let factory = MsSqlTableFactory::new(Arc::clone(&pool));
    let ctx = SessionContext::new();
    ctx.register_table(
        "with_schema_table",
        factory
            .table_provider(table_reference)
            .await
            .expect("provider"),
    )
    .expect("registered");
    let batches = query(&ctx, "SELECT id FROM with_schema_table ORDER BY id").await;
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .values(),
        &[1, 2]
    );
}

/// The constraints of `CREATE EXTERNAL TABLE` are enforced on insert.
#[test_log::test(tokio::test)]
async fn test_mssql_constraints_are_validated() {
    let port = common::ensure_mssql_container().await;
    let pool = common::get_mssql_connection_pool().await;
    execute(&pool, "DROP TABLE IF EXISTS constrained").await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let cmd = CreateExternalTable::builder(
        "constrained",
        "",
        "mssql",
        Arc::clone(&schema).to_dfschema_ref().expect("df schema"),
    )
    .with_options(
        common::get_mssql_params(port, Some(common::MSSQL_DATABASE))
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect(),
    )
    .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
        vec![0],
    )]))
    .build();
    let ctx = SessionContext::new();
    let provider = MsSqlTableProviderFactory::new()
        .create(&ctx.state(), &cmd)
        .await
        .expect("provider");
    ctx.register_table("constrained", provider)
        .expect("registered");

    let result = ctx
        .sql("INSERT INTO constrained VALUES (1, 'a'), (1, 'b')")
        .await
        .expect("planned")
        .collect()
        .await;
    let err = result.expect_err("duplicate keys in a batch must be rejected");
    assert!(err.to_string().contains("onstraint"), "{err}");
}
