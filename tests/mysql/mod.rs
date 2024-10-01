use datafusion::execution::context::SessionContext;
use datafusion_table_providers::{
    mysql::MySQLConnectionPool, sql::sql_provider_datafusion::SqlTable,
};
use rstest::rstest;
use std::sync::Arc;

use arrow::{
    array::*,
    datatypes::{i256, DataType, Field, Schema, TimeUnit, UInt16Type},
};

use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;

use crate::docker::RunningContainer;

mod common;

async fn test_mysql_timestamp_types(port: usize) {
    let create_table_stmt = "
        CREATE TABLE timestamp_table (
    timestamp_no_fraction TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP, 
    timestamp_one_fraction TIMESTAMP(1),  
    timestamp_two_fraction TIMESTAMP(2), 
    timestamp_three_fraction TIMESTAMP(3),                   
    timestamp_four_fraction TIMESTAMP(4),
    timestamp_five_fraction TIMESTAMP(5),
    timestamp_six_fraction TIMESTAMP(6) 
);
        ";
    let insert_table_stmt = "
INSERT INTO timestamp_table (
    timestamp_no_fraction, 
    timestamp_one_fraction, 
    timestamp_two_fraction, 
    timestamp_three_fraction, 
    timestamp_four_fraction, 
    timestamp_five_fraction, 
    timestamp_six_fraction
) 
VALUES 
(
    '2024-09-12 10:00:00',             
    '2024-09-12 10:00:00.1',
    '2024-09-12 10:00:00.12',
    '2024-09-12 10:00:00.123',
    '2024-09-12 10:00:00.1234',        
    '2024-09-12 10:00:00.12345',       
    '2024-09-12 10:00:00.123456'
);
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_no_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_one_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_two_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_three_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_four_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_five_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_six_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_100_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_120_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_400])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_450])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_456])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "timestamp_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_datetime_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE datetime_table (
    dt0 DATETIME(0),  
    dt1 DATETIME(1), 
    dt2 DATETIME(2), 
    dt3 DATETIME(3), 
    dt4 DATETIME(4), 
    dt5 DATETIME(5), 
    dt6 DATETIME(6)  
);

        ";
    let insert_table_stmt = "
INSERT INTO datetime_table (dt0, dt1, dt2, dt3, dt4, dt5, dt6)
VALUES (
    '2024-09-12 10:00:00',
    '2024-09-12 10:00:00.1',
    '2024-09-12 10:00:00.12',
    '2024-09-12 10:00:00.123',
    '2024-09-12 10:00:00.1234',
    '2024-09-12 10:00:00.12345',
    '2024-09-12 10:00:00.123456'
);
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "dt0",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt1",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt2",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt3",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt4",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt5",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt6",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_100_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_120_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_400])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_450])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_456])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "datetime_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_time_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE time_table (
    t0 TIME(0),  
    t1 TIME(1), 
    t2 TIME(2), 
    t3 TIME(3), 
    t4 TIME(4), 
    t5 TIME(5), 
    t6 TIME(6)
);
        ";
    let insert_table_stmt = "
INSERT INTO time_table (t0, t1, t2, t3, t4, t5, t6)
VALUES 
    ('12:30:00', 
     '12:30:00.1', 
     '12:30:00.12', 
     '12:30:00.123', 
     '12:30:00.1234', 
     '12:30:00.12345', 
     '12:30:00.123456');
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("t0", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t1", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t2", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t3", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t4", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t5", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t6", DataType::Time64(TimeUnit::Nanosecond), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60 + 0) * 1_000_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 100_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 120_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_400_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_450_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_456_000,
            ])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "time_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_enum_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE enum_table (
    status ENUM('active', 'inactive', 'pending', 'suspended')
);
        ";
    let insert_table_stmt = "
INSERT INTO enum_table (status)
VALUES
(NULL),
('active'),
('inactive'),
('pending'),
('suspended'),
('inactive');
        ";

    let mut builder = StringDictionaryBuilder::<UInt16Type>::new();
    builder.append_null();
    builder.append_value("active");
    builder.append_value("inactive");
    builder.append_value("pending");
    builder.append_value("suspended");
    builder.append_value("inactive");

    let array: DictionaryArray<UInt16Type> = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
        true,
    )]));

    let expected_record = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
        .expect("Failed to created arrow dictionary array record batch");

    arrow_mysql_one_way(
        port,
        "enum_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_blob_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE blobs_table (
    tinyblob_col    TINYBLOB,
    tinytext_col    TINYTEXT,
    mediumblob_col  MEDIUMBLOB,
    mediumtext_col  MEDIUMTEXT,
    blob_col        BLOB,
    text_col        TEXT,
    longblob_col    LONGBLOB,
    longtext_col    LONGTEXT
);
        ";
    let insert_table_stmt = "
INSERT INTO blobs_table (
    tinyblob_col, tinytext_col, mediumblob_col, mediumtext_col, blob_col, text_col, longblob_col, longtext_col
)
VALUES
    (
        'small_blob', 'small_text',
        'medium_blob', 'medium_text',
        'larger_blob', 'larger_text',
        'very_large_blob', 'very_large_text'
    );
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("tinyblob_col", DataType::Binary, true),
        Field::new("tinytext_col", DataType::Utf8, true),
        Field::new("mediumblob_col", DataType::Binary, true),
        Field::new("mediumtext_col", DataType::Utf8, true),
        Field::new("blob_col", DataType::Binary, true),
        Field::new("text_col", DataType::Utf8, true),
        Field::new("longblob_col", DataType::LargeBinary, true),
        Field::new("longtext_col", DataType::LargeUtf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"small_blob"])),
            Arc::new(StringArray::from(vec!["small_text"])),
            Arc::new(BinaryArray::from_vec(vec![b"medium_blob"])),
            Arc::new(StringArray::from(vec!["medium_text"])),
            Arc::new(BinaryArray::from_vec(vec![b"larger_blob"])),
            Arc::new(StringArray::from(vec!["larger_text"])),
            Arc::new(LargeBinaryArray::from_vec(vec![b"very_large_blob"])),
            Arc::new(LargeStringArray::from(vec!["very_large_text"])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "blobs_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_string_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE string_table (
    name VARCHAR(255),
    data VARBINARY(255),
    fixed_name CHAR(10),
    fixed_data BINARY(10)
);
        ";
    let insert_table_stmt = "
INSERT INTO string_table (name, data, fixed_name, fixed_data)
VALUES 
('Alice', 'Alice', 'ALICE', 'abc'),
('Bob', 'Bob', 'BOB', 'bob1234567'),
('Charlie', 'Charlie', 'CHARLIE', '0123456789'),
('Dave', 'Dave', 'DAVE', 'dave000000');
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("fixed_name", DataType::Utf8, true),
        Field::new("fixed_data", DataType::Binary, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])),
            Arc::new(BinaryArray::from_vec(vec![
                b"Alice", b"Bob", b"Charlie", b"Dave",
            ])),
            Arc::new(StringArray::from(vec!["ALICE", "BOB", "CHARLIE", "DAVE"])),
            Arc::new(BinaryArray::from_vec(vec![
                b"abc\0\0\0\0\0\0\0",
                b"bob1234567",
                b"0123456789",
                b"dave000000",
            ])),
        ],
    )
    .expect("Failed to created arrow record batch");
    arrow_mysql_one_way(
        port,
        "string_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_decimal_types_to_decimal256(port: usize) {
    let create_table_stmt = "
CREATE TABLE high_precision_decimal (
    decimal_values DECIMAL(50, 10)
);
        ";
    let insert_table_stmt = "
INSERT INTO high_precision_decimal (decimal_values) VALUES
(NULL),
(1234567890123456789012345678901234567890.1234567890),
(-9876543210987654321098765432109876543210.9876543210),
(0.0000000001),
(-0.000000001),
(0);
        ";

    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_values",
        DataType::Decimal256(50, 10),
        true,
    )]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal256Array::from(vec![
                None,
                Some(
                    i256::from_string("12345678901234567890123456789012345678901234567890")
                        .unwrap(),
                ),
                Some(
                    i256::from_string("-98765432109876543210987654321098765432109876543210")
                        .unwrap(),
                ),
                Some(i256::from_string("1").unwrap()),
                Some(i256::from_string("-10").unwrap()),
                Some(i256::from_string("0").unwrap()),
            ])
            .with_precision_and_scale(50, 10)
            .expect("Failed to create decimal256 array"),
        )],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "high_precision_decimal",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn test_mysql_decimal_types_to_decimal128(port: usize) {
    let create_table_stmt = "
        CREATE TABLE IF NOT EXISTS decimal_table (decimal_col DECIMAL(10, 2));
        ";
    let insert_table_stmt = "
        INSERT INTO decimal_table (decimal_col) VALUES (NULL), (12);
        ";

    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_col",
        DataType::Decimal128(10, 2),
        true,
    )]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![None, Some(i128::from(1200))])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        )],
    )
    .expect("Failed to created arrow record batch");

    let _ = arrow_mysql_one_way(
        port,
        "decimal_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

async fn arrow_mysql_one_way(
    port: usize,
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    expected_record: RecordBatch,
) -> Vec<RecordBatch> {
    tracing::debug!("Running tests on {table_name}");

    let ctx = SessionContext::new();
    let pool = common::get_mysql_connection_pool(port)
        .await
        .expect("MySQL connection pool should be created");

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    // Create table and insert data into mysql test_table
    let _ = db_conn
        .execute(create_table_stmt, &[])
        .await
        .expect("MySQL table should be created");

    let _ = db_conn
        .execute(insert_table_stmt, &[])
        .await
        .expect("MySQL table data should be inserted");

    // Register datafusion table, test mysql row -> arrow conversion
    let sqltable_pool: Arc<MySQLConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("mysql", &sqltable_pool, table_name, None)
        .await
        .expect("Table should be created");

    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");

    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");
    tracing::debug!(
        "MySQL returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0], expected_record);

    record_batch
}

async fn start_mysql_container(port: usize) -> RunningContainer {
    let running_container = common::start_mysql_docker_container(port)
        .await
        .expect("MySQL container to start");

    tracing::debug!("Container started");

    running_container
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_mysql_arrow_oneway() {
    let port = crate::get_random_port();
    let mysql_container = start_mysql_container(port).await;

    test_mysql_timestamp_types(port).await;
    test_mysql_datetime_types(port).await;
    test_mysql_time_types(port).await;
    test_mysql_enum_types(port).await;
    test_mysql_blob_types(port).await;
    test_mysql_string_types(port).await;
    test_mysql_decimal_types_to_decimal128(port).await;
    test_mysql_decimal_types_to_decimal256(port).await;

    mysql_container.remove().await.expect("container to stop");
}
