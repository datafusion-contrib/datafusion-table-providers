use datafusion::execution::context::SessionContext;
use datafusion_table_providers::{
    mysql::MySQLConnectionPool, sql::sql_provider_datafusion::SqlTable,
};
use rstest::rstest;
use std::sync::Arc;

use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, TimeUnit},
};

use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;

use crate::docker::RunningContainer;

mod common;

async fn test_mysql_decimal_types(port: usize) {
    let table_name = "decimal_table";
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

    let decimal_record = arrow_mysql_one_way(
        port,
        "decimal_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
    )
    .await;
}

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

    return record_batch;
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

    test_mysql_decimal_types(port).await;
    test_mysql_timestamp_types(port).await;

    mysql_container.remove().await.expect("container to stop");
}
