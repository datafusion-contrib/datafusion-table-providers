use crate::arrow_record_batch_gen::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
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
        or_replace: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: HashMap::new(),
        constraints: Constraints::default(),
        column_defaults: HashMap::new(),
        temporary: false,
    };
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();

    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
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
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[ignore] // DuckDB does not support Decimal256 / duckdb_arrow_scan failed to register view
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore]
// Interval(DayTime) is not supported: / "Conversion Error: Could not convert Interval to Microsecond"
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[ignore] // TimeUnit::Nanosecond is not correctly supported; written values are zeros
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[case::list_of_structs(get_arrow_list_of_structs_record_batch(), "list_of_structs")]
#[case::list_of_fixed_size_lists(
    get_arrow_list_of_fixed_size_lists_record_batch(),
    "list_of_fixed_size_lists"
)]
#[case::list_of_lists(get_arrow_list_of_lists_record_batch(), "list_of_lists")]
#[case::map(get_arrow_map_record_batch(), "map")]
#[case::dictionary(get_arrow_dictionary_array_record_batch(), "dictionary")]
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

mod sort_limit_pushdown {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    async fn setup_table(ctx: &SessionContext, name: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]));

        // 20 rows: id = 1..=20
        let ids: Vec<i32> = (1..=20).collect();
        let labels: Vec<String> = ids.iter().map(|i| format!("row-{i:02}")).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap();

        let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
        let cmd = CreateExternalTable {
            schema: Arc::new(batch.schema().to_dfschema().unwrap()),
            name: name.into(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: false,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };
        let table = factory.create(&ctx.state(), &cmd).await.unwrap();
        let mem =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let insert = table
            .insert_into(&ctx.state(), mem, InsertOp::Append)
            .await
            .unwrap();
        let _ = collect(insert, ctx.task_ctx()).await.unwrap();
        ctx.register_table(name, table).unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn order_by_limit_returns_exactly_n_rows() {
        let ctx = SessionContext::new();
        setup_table(&ctx, "sort_limit_test").await;

        let df = ctx
            .sql("SELECT id FROM sort_limit_test ORDER BY id DESC LIMIT 5")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "LIMIT 5 must return exactly 5 rows");

        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let got: Vec<i32> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(got, vec![20, 19, 18, 17, 16], "top-5 DESC rows");
    }

    #[test_log::test(tokio::test)]
    async fn order_by_limit_with_filter() {
        let ctx = SessionContext::new();
        setup_table(&ctx, "sort_limit_filter_test").await;

        let df = ctx
            .sql("SELECT id FROM sort_limit_filter_test WHERE id > 10 ORDER BY id ASC LIMIT 3")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);

        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let got: Vec<i32> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(got, vec![11, 12, 13]);
    }

    #[test_log::test(tokio::test)]
    async fn limit_without_order_by() {
        let ctx = SessionContext::new();
        setup_table(&ctx, "limit_only_test").await;

        let df = ctx
            .sql("SELECT id FROM limit_only_test LIMIT 7")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 7, "LIMIT without ORDER BY must still cap rows");
    }
}
