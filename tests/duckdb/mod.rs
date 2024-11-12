use crate::arrow_record_batch_gen::*;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::duckdb::DuckDBTableProviderFactory;
use rstest::rstest;
use std::collections::HashMap;
use std::sync::Arc;

async fn arrow_duckdb_round_trip(
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
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
        options: HashMap::new(),
        constraints: Constraints::empty(),
        column_defaults: HashMap::new(),
        temporary: false,
    };
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();

    let mem_exec = MemoryExec::try_new(&[vec![arrow_record.clone()]], arrow_record.schema(), None)
        .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), Arc::new(mem_exec), InsertOp::Append)
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
    let casted_record = try_cast_to(record_batch[0].clone(), source_schema).unwrap();

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Duckdb returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    assert_eq!(casted_record, arrow_record);
}

#[rstest]
#[ignore]
// Binder Error: Unsupported data type: FixedSizeBinary(2), please file an issue https://github.com/wangfenjin/duckdb-rs"
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[ignore] // Decimal256(76,10) is not yet supported for ArrowVTab
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore] // Interval(DayTime) is not yet supported for ArrowVTab
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[ignore] // Duration(NanoSecond) is not yet supported for ArrowVTab
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[test_log::test(tokio::test)]
async fn test_arrow_duckdb_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_duckdb_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}
