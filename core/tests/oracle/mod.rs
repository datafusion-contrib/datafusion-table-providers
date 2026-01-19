use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{
    i256, DataType, Field, IntervalMonthDayNano, IntervalUnit, Schema, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::oracle::OracleTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::oracleconn::OraclePooledConnection;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::DbConnection;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use futures::StreamExt;
use std::sync::Arc;

mod common;

#[tokio::test]
async fn test_oracle_connection_pool() {
    let pool = common::get_oracle_connection_pool().await;
    let conn = pool
        .connect_direct()
        .await
        .expect("Failed to get connection");

    let rows = conn
        .conn
        .query("SELECT 1 FROM DUAL", &[])
        .expect("Failed to execute query");
    let rows: Vec<oracle::Row> = rows
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("Failed to collect rows");
    assert!(!rows.is_empty());

    let first_row = &rows[0];
    let val_str: String = first_row.get(0).expect("Value should exist");
    assert_eq!(val_str, "1");
}

/// Test registering Oracle's DUAL table as a DataFusion table provider
#[tokio::test]
async fn test_oracle_table_provider_registration() {
    let pool = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool));

    let provider = factory
        .table_provider(TableReference::from("DUAL"))
        .await
        .expect("Failed to create table provider");

    let ctx = SessionContext::new();
    ctx.register_table("dual_test", provider)
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM dual_test")
        .await
        .expect("Failed to create dataframe");
    let _result = df.collect().await.expect("Failed to execute query");
}

/// Test querying data through DataFusion using ALL_TABLES system view.
/// Validates the full Provider -> DataFusion query path.
#[tokio::test]
async fn test_oracle_query_with_data() {
    let pool = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool));

    let table_name = "ALL_TABLES";
    let provider = factory
        .table_provider(TableReference::from(table_name))
        .await
        .expect("Failed to create table provider for ALL_TABLES");

    // Verify schema contains expected columns
    let schema = provider.schema();
    let fields: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert!(
        fields.contains(&"TABLE_NAME".to_string()) || fields.contains(&"table_name".to_string()),
        "Schema missing 'table_name' column: {:?}",
        fields
    );

    let ctx = SessionContext::new();
    ctx.register_table("system_tables", provider)
        .expect("Failed to register table");

    let sql = "SELECT \"TABLE_NAME\", \"OWNER\" FROM system_tables";
    let df = ctx.sql(sql).await.expect("Failed to build plan");

    let batches = df.collect().await.expect("Query execution failed");
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(row_count > 0, "Expected to read rows from ALL_TABLES");
}

/// Test schema inference for ALL_TABLES system view
#[tokio::test]
async fn test_oracle_explain_plan() {
    let pool = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool));

    let provider = factory
        .table_provider(TableReference::from("ALL_TABLES"))
        .await
        .expect("Failed to create table provider for ALL_TABLES");

    let schema = provider.schema();
    println!("\n=== ALL_TABLES Schema ===");
    for field in schema.fields() {
        println!(
            "  {} : {:?} (nullable: {})",
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    assert!(!schema.fields().is_empty(), "Expected schema with columns");

    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"TABLE_NAME"),
        "Expected TABLE_NAME column"
    );
    assert!(field_names.contains(&"OWNER"), "Expected OWNER column");
}

/// Test schema inference for ALL_TAB_COLUMNS system view
#[tokio::test]
async fn test_oracle_explain_verbose() {
    let pool = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool));

    let provider = factory
        .table_provider(TableReference::from("ALL_TAB_COLUMNS"))
        .await
        .expect("Failed to create table provider for ALL_TAB_COLUMNS");

    let schema = provider.schema();
    println!("\n=== ALL_TAB_COLUMNS Schema ===");
    for field in schema.fields() {
        println!("  {} : {:?}", field.name(), field.data_type());
    }

    assert!(!schema.fields().is_empty(), "Expected schema with columns");

    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"COLUMN_NAME"),
        "Expected COLUMN_NAME column"
    );
    assert!(
        field_names.contains(&"DATA_TYPE"),
        "Expected DATA_TYPE column"
    );
}

/// Row struct for insertion test
#[derive(Debug)]
struct Row {
    id: i64,
    name: String,
    age: i32,
    score: f64,
}

fn create_sample_rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
            score: 91.5,
        },
        Row {
            id: 2,
            name: "Bob".to_string(),
            age: 45,
            score: 85.2,
        },
    ]
}

/// Creates or recreates a test table with the given name
async fn create_test_table(
    conn: &oracle::Connection,
    table_name: &str,
) -> std::result::Result<(), oracle::Error> {
    let check_sql = format!(
        "SELECT count(*) FROM user_tables WHERE table_name = '{}'",
        table_name
    );
    let rows = conn.query(&check_sql, &[])?;
    let rows: Vec<oracle::Row> = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    let count: i64 = if !rows.is_empty() { rows[0].get(0)? } else { 0 };

    if count > 0 {
        let _ = conn.execute(&format!("DROP TABLE {}", table_name), &[]);
    }

    let sql = format!(
        "CREATE TABLE {} (
            id NUMBER,
            name VARCHAR2(100),
            age NUMBER, 
            score BINARY_DOUBLE
        )",
        table_name
    );

    conn.execute(&sql, &[])?;
    Ok(())
}

async fn insert_test_rows(
    conn: &oracle::Connection,
    table_name: &str,
    rows: Vec<Row>,
) -> std::result::Result<(), oracle::Error> {
    let sql = format!(
        "INSERT INTO {} (id, name, age, score) VALUES (:1, :2, :3, :4)",
        table_name
    );

    for row in rows {
        conn.execute(&sql, &[&row.id, &row.name, &row.age, &row.score])?;
    }

    conn.commit()?;
    Ok(())
}

/// Full integration test: Create table -> Insert data -> Read via DataFusion
#[tokio::test]
async fn test_oracle_insert_and_read() {
    let table_name = "TEST_EMPLOYEES";

    let pool = common::get_oracle_connection_pool().await;
    let conn = pool
        .connect_direct()
        .await
        .expect("Failed to get connection");

    create_test_table(&conn.conn, table_name)
        .await
        .expect("Create table failed");
    insert_test_rows(&conn.conn, table_name, create_sample_rows())
        .await
        .expect("Insert failed");

    drop(conn);
    drop(pool);

    let pool_query = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool_query));

    let ctx = SessionContext::new();
    let provider = factory
        .table_provider(TableReference::from(table_name))
        .await
        .expect("Provider creation failed");

    // Verify schema has expected columns (uppercase in Oracle)
    let schema = provider.schema();
    let fields: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert!(fields.contains(&"ID".to_string()));
    assert!(fields.contains(&"NAME".to_string()));
    assert!(fields.contains(&"SCORE".to_string()));

    ctx.register_table("employees", provider)
        .expect("Table register failed");

    // Note: Column names must be quoted for uppercase identifiers in DataFusion SQL
    let sql = "SELECT * FROM employees ORDER BY \"ID\"";
    let df = ctx.sql(sql).await.expect("Query failed");
    let batches = df.collect().await.expect("Collect failed");

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, 2);
}

#[tokio::test]
async fn test_oracle_number_types() {
    let create_table_stmt = "
        CREATE TABLE number_test_table (
            n1 NUMBER,
            n2 NUMBER(10),
            n3 NUMBER(10, 2),
            n4 NUMBER(38, 10),
            n5 NUMBER(38)
        )
    ";
    let insert_table_stmt = "
        INSERT INTO number_test_table (n1, n2, n3, n4, n5) 
        VALUES (
            123.456, 
            1234567890, 
            12345678.90, 
            123456789012345678.1234567890, 
            12345678901234567890123456789012345678
        )
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("N1", DataType::Decimal128(38, 0), true),
        Field::new("N2", DataType::Int64, true), // NUMBER(10,0) where p≤18 → Int64
        Field::new("N3", DataType::Decimal128(10, 2), true),
        Field::new("N4", DataType::Decimal128(38, 10), true),
        Field::new("N5", DataType::Decimal128(38, 0), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                Decimal128Array::from(vec![Some(123)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![1234567890])), // NUMBER(10,0) → Int64
            Arc::new(
                Decimal128Array::from(vec![Some(1234567890)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(1234567890123456781234567890)])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(12345678901234567890123456789012345678)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "NUMBER_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn arrow_oracle_one_way(
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    expected_record: RecordBatch,
) -> Vec<RecordBatch> {
    let pool = common::get_oracle_connection_pool().await;
    let conn = pool
        .connect_direct()
        .await
        .expect("Failed to get connection");

    // Cleanup and create table
    let _ = conn
        .conn
        .execute(&format!("DROP TABLE {}", table_name), &[]);
    conn.conn
        .execute(create_table_stmt, &[])
        .expect("Failed to create table");
    conn.conn
        .execute(insert_table_stmt, &[])
        .expect("Failed to insert data");
    conn.conn.commit().expect("Failed to commit");

    let sqltable_pool: Arc<
        dyn DbConnectionPool<OraclePooledConnection, oracle::sql_type::OracleType>
            + Send
            + Sync
            + 'static,
    > = Arc::new(pool);
    let table = SqlTable::new("oracle", &sqltable_pool, table_name)
        .await
        .expect("Table should be created");

    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");

    let sql = format!("SELECT * FROM {}", table_name);
    let df = ctx.sql(&sql).await.expect("Query failed");

    let record_batches = df.collect().await.expect("Collect failed");

    assert_eq!(record_batches.len(), 1);
    assert_eq!(record_batches[0].schema(), expected_record.schema());
    assert_eq!(record_batches[0], expected_record);

    record_batches
}

#[tokio::test]
async fn test_oracle_date_time_types() {
    let create_table_stmt = "
        CREATE TABLE date_time_test_table (
            d1 DATE,
            t1 TIMESTAMP,
            t2 TIMESTAMP(6),
            t3 TIMESTAMP WITH TIME ZONE
        )
    ";
    let insert_table_stmt = "
        INSERT INTO date_time_test_table (d1, t1, t2, t3) 
        VALUES (
            TO_DATE('2024-09-12', 'YYYY-MM-DD'),
            TO_TIMESTAMP('2024-09-12 10:00:00.123', 'YYYY-MM-DD HH24:MI:SS.FF3'),
            TO_TIMESTAMP('2024-09-12 10:00:00.123456', 'YYYY-MM-DD HH24:MI:SS.FF6'),
            TO_TIMESTAMP_TZ('2024-09-12 10:00:00.123 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM')
        )
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("D1", DataType::Date32, true),
        Field::new("T1", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("T2", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new(
            "T3",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![19978])), // 2024-09-12 as days since epoch
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_456])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_726_135_200_123_000]).with_timezone("UTC"),
            ),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "DATE_TIME_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

#[tokio::test]
async fn test_oracle_integer_types() {
    let create_table_stmt = "
        CREATE TABLE integer_test_table (
            i1 NUMBER(5, 0),
            i2 NUMBER(10, 0),
            i3 NUMBER(18, 0),
            i4 NUMBER(38, 0)
        )
    ";
    let insert_table_stmt = "
        INSERT INTO integer_test_table (i1, i2, i3, i4) 
        VALUES (12345, 1234567890, 123456789012345678, 12345678901234567890123456789012345678)
    ";

    // NUMBER(p, 0) where p <= 18 should map to Int64
    let schema = Arc::new(Schema::new(vec![
        Field::new("I1", DataType::Int64, true),
        Field::new("I2", DataType::Int64, true),
        Field::new("I3", DataType::Int64, true),
        Field::new("I4", DataType::Decimal128(38, 0), true), // p > 18, stays Decimal128
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![12345])),
            Arc::new(Int64Array::from(vec![1234567890])),
            Arc::new(Int64Array::from(vec![123456789012345678])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345678901234567890123456789012345678)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "INTEGER_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

#[tokio::test]
async fn test_oracle_precision_aware_timestamps() {
    let create_table_stmt = "
        CREATE TABLE timestamp_precision_table (
            t0 TIMESTAMP(0),
            t3 TIMESTAMP(3),
            t6 TIMESTAMP(6),
            t9 TIMESTAMP(9)
        )
    ";
    let insert_table_stmt = "
        INSERT INTO timestamp_precision_table (t0, t3, t6, t9) 
        VALUES (
            TO_TIMESTAMP('2024-09-12 10:00:00', 'YYYY-MM-DD HH24:MI:SS'),
            TO_TIMESTAMP('2024-09-12 10:00:00.123', 'YYYY-MM-DD HH24:MI:SS.FF3'),
            TO_TIMESTAMP('2024-09-12 10:00:00.123456', 'YYYY-MM-DD HH24:MI:SS.FF6'),
            TO_TIMESTAMP('2024-09-12 10:00:00.123456789', 'YYYY-MM-DD HH24:MI:SS.FF9')
        )
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("T0", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new("T3", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("T6", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("T9", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampSecondArray::from(vec![1_726_135_200])),
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_123])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_456])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1_726_135_200_123_456_789,
            ])),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "TIMESTAMP_PRECISION_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

#[tokio::test]
async fn test_oracle_interval_types() {
    let create_table_stmt = "
        CREATE TABLE interval_test_table (
            ym INTERVAL YEAR TO MONTH,
            ds INTERVAL DAY TO SECOND
        )
    ";
    let insert_table_stmt = "
        INSERT INTO interval_test_table (ym, ds) 
        VALUES (
            INTERVAL '2-6' YEAR TO MONTH,
            INTERVAL '3 12:30:45.123456' DAY TO SECOND
        )
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("YM", DataType::Interval(IntervalUnit::YearMonth), true),
        Field::new("DS", DataType::Interval(IntervalUnit::MonthDayNano), true),
    ]));

    // INTERVAL '2-6' YEAR TO MONTH = 2*12 + 6 = 30 months
    // INTERVAL '3 12:30:45.123456' DAY TO SECOND = 3 days + (12*3600 + 30*60 + 45)*1e9 + 123456000 nanos
    let ds_nanos = (12 * 3600 + 30 * 60 + 45) * 1_000_000_000 + 123_456_000;

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(IntervalYearMonthArray::from(vec![30])),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(0, 3, ds_nanos),
            ])),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "INTERVAL_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

#[tokio::test]
async fn test_oracle_binary_types() {
    let create_table_stmt = "
        CREATE TABLE binary_test_table (
            r1 RAW(10),
            r2 RAW(100)
        )
    ";
    let insert_table_stmt = "
        INSERT INTO binary_test_table (r1, r2) 
        VALUES (
            HEXTORAW('DEADBEEF'),
            HEXTORAW('ABCDEF0123456789')
        )
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("R1", DataType::Binary, true),
        Field::new("R2", DataType::Binary, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"\xDE\xAD\xBE\xEF"])),
            Arc::new(BinaryArray::from_vec(vec![
                b"\xAB\xCD\xEF\x01\x23\x45\x67\x89",
            ])),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "BINARY_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

#[tokio::test]
async fn test_oracle_lob_types() {
    let create_table_stmt = "
        CREATE TABLE lob_test_table (
            b1 BLOB,
            c1 CLOB
        )
    ";
    let insert_table_stmt = "
        INSERT INTO lob_test_table (b1, c1) 
        VALUES (
            HEXTORAW('0102030405'),
            'Large text content for CLOB'
        )
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("B1", DataType::LargeBinary, true),
        Field::new("C1", DataType::LargeUtf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(LargeBinaryArray::from_vec(vec![b"\x01\x02\x03\x04\x05"])),
            Arc::new(LargeStringArray::from(vec!["Large text content for CLOB"])),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "LOB_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

#[tokio::test]
async fn test_oracle_null_handling() {
    let create_table_stmt = "
        CREATE TABLE null_test_table (
            n1 NUMBER,
            d1 DATE,
            t1 TIMESTAMP,
            r1 RAW(10),
            b1 BLOB,
            c1 CLOB
        )
    ";
    let insert_table_stmt = "
        INSERT INTO null_test_table (n1, d1, t1, r1, b1, c1) 
        VALUES (NULL, NULL, NULL, NULL, NULL, NULL)
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("N1", DataType::Decimal128(38, 0), true),
        Field::new("D1", DataType::Date32, true),
        Field::new("T1", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("R1", DataType::Binary, true),
        Field::new("B1", DataType::LargeBinary, true),
        Field::new("C1", DataType::LargeUtf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                Decimal128Array::from(vec![Option::<i128>::None])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![Option::<i32>::None])),
            Arc::new(TimestampMicrosecondArray::from(vec![Option::<i64>::None])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(LargeBinaryArray::from_opt_vec(vec![None])),
            Arc::new(LargeStringArray::from(vec![Option::<&str>::None])),
        ],
    )
    .expect("Failed to create expected record batch");

    arrow_oracle_one_way(
        "NULL_TEST_TABLE",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}
#[tokio::test]
async fn test_oracle_sql_generation_limit() {
    let pool = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool));

    // reusing "ALL_TABLES" as it is guaranteed to exist
    let table_name = "ALL_TABLES";
    let provider = factory
        .table_provider(TableReference::from(table_name))
        .await
        .expect("Failed to create table provider");

    let ctx = SessionContext::new();
    ctx.register_table("system_tables", provider)
        .expect("Failed to register table");

    // Test 1: Verify FETCH FIRST syntax for LIMIT
    let sql_limit = "SELECT * FROM system_tables LIMIT 5";
    let df_limit = ctx.sql(sql_limit).await.expect("Failed to build plan");
    let plan_limit = df_limit
        .create_physical_plan()
        .await
        .expect("Failed to create physical plan");
    let display_limit = format!(
        "{}",
        datafusion::physical_plan::displayable(plan_limit.as_ref()).indent(true)
    );
    assert!(
        display_limit.contains("FETCH FIRST 5 ROWS ONLY"),
        "Plan did not contain FETCH FIRST syntax: {}",
        display_limit
    );
}

#[tokio::test]
async fn test_oracle_sql_generation_filter() {
    let pool = common::get_oracle_connection_pool().await;
    let factory = OracleTableFactory::new(Arc::new(pool));

    let table_name = "ALL_TABLES";
    let provider = factory
        .table_provider(TableReference::from(table_name))
        .await
        .expect("Failed to create table provider");

    let ctx = SessionContext::new();
    ctx.register_table("system_tables", provider)
        .expect("Failed to register table");

    // Test 2: Verify identifier quoting (using a filter)
    // "OWNER" column exists in ALL_TABLES
    let sql_filter = "SELECT * FROM system_tables WHERE \"OWNER\" = 'SYS'";
    let df_filter = ctx.sql(sql_filter).await.expect("Failed to build plan");
    let plan_filter = df_filter
        .create_physical_plan()
        .await
        .expect("Failed to create physical plan");
    let display_filter = format!(
        "{}",
        datafusion::physical_plan::displayable(plan_filter.as_ref()).indent(true)
    );

    // Verify that the custom dialect correctly quotes identifiers (e.g. "OWNER").
    // Note: The expression might be wrapped in parentheses like WHERE ("OWNER" = 'SYS')
    assert!(
        display_filter.contains(r#""OWNER""#),
        "Plan did not contain quoted identifier \"OWNER\": {}",
        display_filter
    );
}

#[tokio::test]
async fn test_oracle_large_query_streaming() {
    let pool = common::get_oracle_connection_pool().await;
    let conn = pool
        .connect_direct()
        .await
        .expect("Failed to get connection");
    let oracle_conn = conn.conn.clone();

    // Create table with test data
    let table_name = "LARGE_QUERY_TEST";
    let _ = oracle_conn.execute(&format!("DROP TABLE {}", table_name), &[]);
    oracle_conn
        .execute(
            &format!("CREATE TABLE {} (id NUMBER, val VARCHAR2(50))", table_name),
            &[],
        )
        .expect("Failed to create table");

    // Insert 5000 rows (chunk size is 4096, so this guarantees at least 2 chunks)
    let batch_size = 1000;
    for i in 0..5 {
        let mut ids = Vec::with_capacity(batch_size);
        let mut vals = Vec::with_capacity(batch_size);
        for j in 0..batch_size {
            let id = i * batch_size + j;
            ids.push(id as i64);
            vals.push(format!("value_{}", id));
        }

        let sql = format!("INSERT INTO {} (id, val) VALUES (:1, :2)", table_name);
        let mut stmt = oracle_conn.statement(&sql).build().expect("Prepare failed");

        // Execute batch insertion manually for simplicity in test
        for j in 0..batch_size {
            stmt.execute(&[&ids[j], &vals[j]]).expect("Insert failed");
        }
    }
    oracle_conn.commit().expect("Commit failed");

    // Query using query_arrow directly to verify chunks
    let sql = format!("SELECT * FROM {} ORDER BY id", table_name);
    let mut stream = conn
        .as_async()
        .expect("Should be async")
        .query_arrow(&sql, &[], None)
        .await
        .expect("Query failed");

    let mut total_rows = 0;
    let mut batch_count = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch should be Ok");
        total_rows += batch.num_rows();
        batch_count += 1;
        println!("Batch {}: {} rows", batch_count, batch.num_rows());
    }

    assert_eq!(total_rows, 5000, "Total rows mismatch");
    // Verify we got multiple batches (streaming active)
    assert!(
        batch_count > 1,
        "Expected streaming (more than 1 batch), got {}",
        batch_count
    );
}
