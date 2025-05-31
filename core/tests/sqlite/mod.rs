use crate::arrow_record_batch_gen::*;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::sql::TableReference;
#[cfg(feature = "sqlite-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::{
    SqliteConnectionPool, SqliteConnectionPoolFactory,
};
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, Mode};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use datafusion_table_providers::sqlite::DynSqliteConnectionPool;
use rstest::rstest;
use std::sync::Arc;

async fn init_db_tabel(arrow_record: &RecordBatch, table_name: &str) -> SqliteConnectionPool {
    tracing::debug!("Running tests on {table_name}");

    let pool = SqliteConnectionPoolFactory::new(
        ":memory:",
        Mode::Memory,
        std::time::Duration::from_millis(5000),
    )
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
    let insert_table_stmt = InsertBuilder::new(
        &TableReference::from(table_name),
        vec![arrow_record.clone()],
    )
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

    pool
}

async fn arrow_sqlite_round_trip(
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let pool = init_db_tabel(&arrow_record, table_name).await;

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynSqliteConnectionPool> = Arc::new(pool);
    let ctx = SessionContext::new();

    // Perform the test twice: first, simulate a request without a known schema;
    // then, test result conversion with a known projected schema (matching the test RecordBatch).
    for projected_schema in [None, Some(arrow_record.schema())] {
        if ctx
            .table_exist(table_name)
            .expect("should be able to check if table exists")
        {
            ctx.deregister_table(table_name)
                .expect("Table should be deregistered");
        }

        let table = match projected_schema {
            None => SqlTable::new("sqlite", &sqltable_pool, table_name)
                .await
                .expect("Table should be created"),
            Some(schema) => SqlTable::new_with_schema("sqlite", &sqltable_pool, schema, table_name),
        };

        ctx.register_table(table_name, Arc::new(table))
            .expect("Table should be registered");

        let sql = format!("SELECT * FROM {table_name}");
        let df = ctx
            .sql(&sql)
            .await
            .expect("DataFrame should be created from query");

        let record_batch = df.collect().await.expect("RecordBatch should be collected");

        #[cfg(feature = "sqlite-federation")]
        let casted_record =
            try_cast_to(record_batch[0].clone(), Arc::clone(&source_schema)).unwrap();

        tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
        tracing::debug!(
            "Sqlite returned Record Batch: {:?}",
            record_batch[0].columns()
        );

        // Check results
        assert_eq!(record_batch.len(), 1);
        assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
        assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
        #[cfg(feature = "sqlite-federation")]
        assert_eq!(casted_record, arrow_record);
    }
}

#[cfg(feature = "sqlite-federation")]
async fn arrow_sqlite_select_round_trip(
    remote_record: RecordBatch,
    orig_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
    select: &str,
) {
    let pool = init_db_tabel(&remote_record, table_name).await;

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynSqliteConnectionPool> = Arc::new(pool);
    let ctx = SessionContext::new();

    let table =
        SqlTable::new_with_schema("sqlite", &sqltable_pool, remote_record.schema(), table_name);

    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");

    let sql = format!("SELECT {} FROM {table_name}", select);
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");
    let casted_record = try_cast_to(record_batch[0].clone(), Arc::clone(&source_schema)).unwrap();

    tracing::debug!("Original Arrow Record Batch: {:?}", orig_record.columns());
    tracing::debug!(
        "Sqlite returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    assert_eq!(record_batch.len(), 1);
    assert_eq!(casted_record, orig_record);
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
#[ignore] // Requires a custom sqlite extension for decimal types
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore] // TODO: interval types are broken in SQLite - Interval is not available in Sqlite.
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[test_log::test(tokio::test)]
async fn test_arrow_sqlite_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_sqlite_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

#[rstest]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::timestamp_no_tz(get_arrow_timestamp_record_batch_without_timezone(), "timestamp_no_tz")]
#[test_log::test(tokio::test)]
#[cfg(feature = "sqlite-federation")]
async fn test_arrow_sqlite_cast_timestamp_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    let (orig_record, orig_schema) = arrow_result;

    // test timestamps stores as INT
    let insert_fields = orig_schema
        .fields
        .iter()
        .map(|f| Field::new(f.name(), DataType::Int64, f.is_nullable()));
    let insert_schema = Arc::new(Schema::new(insert_fields.collect::<Vec<_>>()));
    let insert_record = try_cast_to(orig_record.clone(), insert_schema).unwrap();

    let select = orig_schema
        .fields
        .iter()
        .map(|f| format!("arrow_cast({},  '{}')", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ");

    tracing::debug!("Testing {table_name} with arrow casts: {select}");

    arrow_sqlite_select_round_trip(
        insert_record,
        orig_record,
        orig_schema,
        &format!("{table_name}_types"),
        &select,
    )
    .await;
}
