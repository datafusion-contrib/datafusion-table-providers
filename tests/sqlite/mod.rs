use super::*;
use crate::arrow_record_batch_gen::*;
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, Mode};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use datafusion_table_providers::sqlite::DynSqliteConnectionPool;
use std::sync::Arc;

async fn arrow_sqlite_round_trip(arrow_record: RecordBatch, table_name: &str) -> () {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = SqliteConnectionPoolFactory::new(":memory:", Mode::Memory)
        .build()
        .await
        .expect("Sqlite connection pool to be created");

    let conn = pool
        .connect()
        .await
        .expect("Sqlite connection should be established");
    let conn = conn.as_async().unwrap();

    // Create sqlite table from arrow records and insert arrow records
    let schema = Arc::clone(&arrow_record.schema());
    let create_table_stmts = CreateTableBuilder::new(schema, table_name).build_sqlite();
    let insert_table_stmt = InsertBuilder::new(table_name, vec![arrow_record.clone()])
        .build_sqlite(None)
        .expect("SQLite insert statement should be constructed");

    // Test arrow -> Sqlite row coverage
    let _ = conn
        .execute(&create_table_stmts, &[])
        .await
        .expect("Sqlite table should be created");
    let _ = conn
        .execute(&insert_table_stmt, &[])
        .await
        .expect("Sqlite data should be inserted");

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynSqliteConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("sqlite", &sqltable_pool, table_name, None)
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

    // Print original arrow record batch and record batch converted from postgres row in terminal
    // Check if the values are the same
    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Postgres returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_binary() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_binary_record_batch(), "binary_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_int() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_int_record_batch(), "int_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_float() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_float_record_batch(), "float_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_utf8() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_utf8_record_batch(), "utf8_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_time() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_time_record_batch(), "time_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_timestamp() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_timestamp_record_batch(), "timestamp_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_date() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_date_record_batch(), "date_types").await;
}

#[ignore]
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_struct() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_struct_record_batch(), "struct_types").await;
}

#[ignore]
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_decimal() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_decimal_record_batch(), "decimal_types").await;
}

#[ignore]
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_interval() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_interval_record_batch(), "interval_types").await;
}

#[ignore]
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_duration() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_duration_record_batch(), "duration_types").await;
}

#[ignore]
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_list() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_list_record_batch(), "list_types").await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_arrow_sqlite_types_conversion_null() {
    let _tracing = init_tracing(Some("integration=debug,info"));
    arrow_sqlite_round_trip(get_arrow_null_record_batch(), "null_types").await;
}
