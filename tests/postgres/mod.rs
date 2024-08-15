use super::*;
use crate::arrow_record_batch_gen::*;
use datafusion::execution::context::SessionContext;
#[cfg(feature = "postgres")]
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
#[cfg(feature = "postgres")]
use datafusion_table_providers::{
    postgres::DynPostgresConnectionPool, sql::sql_provider_datafusion::SqlTable,
};
use std::sync::Arc;
mod common;

async fn arrow_postgres_round_trip(
    port: usize,
    arrow_record: RecordBatch,
    table_name: &str,
) -> Result<(), String> {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .map_err(|e| format!("Failed to create postgres connection pool: {e}"))?;

    let db_conn = pool
        .connect_direct()
        .await
        .expect("connection can be established");

    // Create postgres table from arrow records and insert arrow records
    let schema = Arc::clone(&arrow_record.schema());
    let create_table_stmts = CreateTableBuilder::new(schema, table_name).build_postgres();
    let insert_table_stmt = InsertBuilder::new(table_name, vec![arrow_record.clone()])
        .build_postgres(None)
        .map_err(|e| format!("Unable to construct postgres insert statement: {e}"))?;

    // Test arrow -> Postgres row coverage
    for create_table_stmt in create_table_stmts {
        let _ = db_conn
            .conn
            .execute(&create_table_stmt, &[])
            .await
            .map_err(|e| format!("Postgres table cannot be created: {e}"));
    }
    let _ = db_conn
        .conn
        .execute(&insert_table_stmt, &[])
        .await
        .map_err(|e| format!("Postgres table cannot be created: {e}"));

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, table_name, None)
        .await
        .expect("table can be created");
    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame can't be created from query");

    let record_batch = df.collect().await.expect("RecordBatch can't be collected");

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

    Ok(())
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn test_arrow_postgres_types_conversion() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let port = common::get_random_port();
    let _running_container = common::start_postgres_docker_container(port)
        .await
        .map_err(|e| format!("Failed to create postgres container: {e}"))?;

    tracing::debug!("Container started");

    arrow_postgres_round_trip(port, get_arrow_binary_record_batch(), "binary_types")
        .await
        .unwrap();

    arrow_postgres_round_trip(port, get_arrow_int_recordbatch(), "int_types")
        .await
        .unwrap();

    arrow_postgres_round_trip(port, get_arrow_float_record_batch(), "float_types")
        .await
        .unwrap();

    arrow_postgres_round_trip(port, get_arrow_utf8_record_batch(), "utf8_types")
        .await
        .unwrap();

    // arrow_postgres_round_trip(port, get_arrow_time_record_batch(), "time_types")
    //     .await
    //     .unwrap();

    arrow_postgres_round_trip(port, get_arrow_timestamp_record_batch(), "timestamp_types")
        .await
        .unwrap();

    // arrow_postgres_round_trip(port, get_arrow_date_record_batch(), "date_types")
    //     .await
    //     .unwrap();

    arrow_postgres_round_trip(port, get_arrow_struct_record_batch(), "struct_types")
        .await
        .unwrap();

    // arrow_postgres_round_trip(port, get_arrow_decimal_record_batch(), "decimal_types")
    //     .await
    //     .unwrap();

    arrow_postgres_round_trip(port, get_arrow_interval_record_batch(), "interval_types")
        .await
        .unwrap();

    // arrow_postgres_round_trip(port, get_arrow_duration_record_batch(), "duration_types")
    //     .await
    //     .unwrap();

    // arrow_postgres_round_trip(port, get_arrow_list_record_batch(), "list_types")
    //     .await
    //     .unwrap();

    arrow_postgres_round_trip(port, get_arrow_null_record_batch(), "null_types")
        .await
        .unwrap();

    Ok(())
}
