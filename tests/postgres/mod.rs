use crate::arrow_record_batch_gen::*;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion_federation::schema_cast::record_convert::try_cast_to;

use datafusion_table_providers::{
    postgres::{DynPostgresConnectionPool, PostgresTableProviderFactory},
    sql::sql_provider_datafusion::SqlTable,
};
use rstest::{fixture, rstest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

mod common;

async fn arrow_postgres_round_trip(
    port: usize,
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let factory = PostgresTableProviderFactory::new();
    let ctx = SessionContext::new();
    let cmd = CreateExternalTable {
        schema: Arc::new(arrow_record.schema().to_dfschema().expect("to df schema")),
        name: table_name.into(),
        location: "".to_string(),
        file_type: "".to_string(),
        table_partition_cols: vec![],
        if_not_exists: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: common::get_pg_params(port),
        constraints: Constraints::empty(),
        column_defaults: HashMap::new(),
    };
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();
    let mem_exec = MemoryExec::try_new(&[vec![arrow_record.clone()]], arrow_record.schema(), None)
        .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), Arc::new(mem_exec), true)
        .await
        .expect("insert plan created");

    let _ = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");
    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Postgres returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    let casted_result =
        try_cast_to(record_batch[0].clone(), source_schema).expect("Failed to cast record batch");

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    assert_eq!(arrow_record, casted_result);
}

#[derive(Debug)]
struct ContainerManager {
    port: usize,
    claimed: bool,
}

#[fixture]
#[once]
fn container_manager() -> Mutex<ContainerManager> {
    Mutex::new(ContainerManager {
        port: crate::get_random_port(),
        claimed: false,
    })
}

async fn start_container(manager: &MutexGuard<'_, ContainerManager>) {
    let _ = common::start_postgres_docker_container(manager.port)
        .await
        .expect("Postgres container to start");

    tracing::debug!("Container started");
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
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[case::bytea_array(get_arrow_bytea_array_record_batch(), "bytea_array")]
#[test_log::test(tokio::test)]
async fn test_arrow_postgres_roundtrip(
    container_manager: &Mutex<ContainerManager>,
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&container_manager).await;
    }

    arrow_postgres_round_trip(
        container_manager.port,
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_arrow_postgres_one_way(container_manager: &Mutex<ContainerManager>) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&container_manager).await;
    }

    let extra_stmt = Some("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');");
    let create_table_stmt = "
    CREATE TABLE person_mood (
    mood_status mood NOT NULL
    );";

    let insert_table_stmt = "
    INSERT INTO person_mood (mood_status) VALUES ('happy'), ('sad'), ('neutral');
    ";

    let (expected_record, _) = get_arrow_dictionary_array_record_batch();

    arrow_postgres_one_way(
        container_manager.port,
        "person_mood",
        create_table_stmt,
        insert_table_stmt,
        extra_stmt,
        expected_record,
    )
    .await;
}

async fn arrow_postgres_one_way(
    port: usize,
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    extra_stmt: Option<&str>,
    expected_record: RecordBatch,
) {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = common::get_postgres_connection_pool(port)
        .await
        .expect("Postgres connection pool should be created");

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    if let Some(extra_stmt) = extra_stmt {
        let _ = db_conn
            .conn
            .execute(extra_stmt, &[])
            .await
            .expect("Statement should be created");
    }

    let _ = db_conn
        .conn
        .execute(create_table_stmt, &[])
        .await
        .expect("Postgres table should be created");

    let _ = db_conn
        .conn
        .execute(insert_table_stmt, &[])
        .await
        .expect("Postgres table data should be inserted");

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynPostgresConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("postgres", &sqltable_pool, table_name, None)
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

    assert_eq!(record_batch[0], expected_record);
}
