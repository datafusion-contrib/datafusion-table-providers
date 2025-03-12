use crate::arrow_record_batch_gen::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::sql::TableReference;
#[cfg(feature = "sqlite-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, Mode};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use datafusion_table_providers::sqlite::DynSqliteConnectionPool;
use rstest::rstest;
use std::sync::Arc;

async fn arrow_sqlite_round_trip(
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

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

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynSqliteConnectionPool> = Arc::new(pool);

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
