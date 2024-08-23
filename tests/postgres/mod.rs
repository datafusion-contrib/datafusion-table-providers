use crate::arrow_record_batch_gen::*;
use arrow::array::new_null_array;
use arrow::array::Array;
use arrow::{array::RecordBatch, compute::cast, datatypes::SchemaRef};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
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

    let mut result_ok = false;
    let mut result_bacth: Option<RecordBatch> = None;

    match try_cast_to(record_batch[0].clone(), source_schema) {
        Ok(records) => {
            result_bacth = Some(records);
        }
        Err(e) => {
            // Known type loss for interval round trip conversion through postgres - Postgres exported value is always in precision of month day millisecond
            if table_name == "interval_types" {
                result_ok = record_batch[0] == get_pg_interval_expected_result();
            }
        }
    };

    if let Some(result) = result_bacth {
        result_ok = result == arrow_record
    }

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    assert_eq!(result_ok, true);
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
