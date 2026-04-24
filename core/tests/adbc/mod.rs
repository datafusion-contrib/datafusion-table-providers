// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::arrow_record_batch_gen::*;
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::collect;
use datafusion::{catalog::memory::MemorySourceConfig, prelude::SessionContext};

#[cfg(feature = "adbc-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;

use datafusion_table_providers::{
    adbc::AdbcTableFactory, sql::db_connection_pool::adbcpool::ADBCPool,
};
use rstest::rstest;
use std::sync::Arc;

fn get_db(driver_name: &str) -> ManagedDatabase {
    let mut driver = ManagedDriver::load_from_name(
        driver_name,
        None,
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .unwrap();

    driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(":memory:".to_string()),
        )])
        .unwrap()
}

async fn arrow_adbc_round_trip(
    arrow_record: RecordBatch,
    _source_schema: SchemaRef,
    table_name: &str,
) {
    let adbc_pool =
        Arc::new(ADBCPool::new(get_db("sqlite"), None).expect("Failed to create ADBC pool"));

    let table_factory = AdbcTableFactory::new(adbc_pool.clone());
    let ctx = SessionContext::new();
    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("Failed to create memory source execution plan");

    let create_plan = table_factory
        .create_from(&ctx.state(), mem_exec, table_name.into())
        .await
        .expect("Failed to create table");

    let _ = collect(create_plan, ctx.task_ctx())
        .await
        .expect("Table creation failed");

    let table_provider = table_factory
        .table_provider(table_name.into(), None)
        .await
        .expect("Failed to register table provider");
    ctx.register_table(table_name, Arc::clone(&table_provider))
        .expect("Failed to register table");

    let select_sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&select_sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    #[cfg(feature = "adbc-federation")]
    let casted_record = try_cast_to(record_batch[0].clone(), _source_schema).unwrap();

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Adbc returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    #[cfg(feature = "adbc-federation")]
    assert_eq!(casted_record, arrow_record);
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[ignore] // sqlite does not support Time32 / Time64
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[ignore] // sqlite does not support Date32 / Date64
#[case::date(get_arrow_date_record_batch(), "date")]
#[ignore] // sqlite does not support Struct type
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[ignore] // sqlite does not support Decimal256
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore]
// Interval(DayTime) is not supported: / "Conversion Error: Could not convert Interval to Microsecond"
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[ignore] // TimeUnit::Nanosecond is not correctly supported; written values are zeros
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[ignore] // sqlite does not support List type
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[ignore] // sqlite does not support list type
#[case::list_of_structs(get_arrow_list_of_structs_record_batch(), "list_of_structs")]
#[ignore] // sqlite does not support list type
#[case::list_of_fixed_size_lists(
    get_arrow_list_of_fixed_size_lists_record_batch(),
    "list_of_fixed_size_lists"
)]
#[ignore] // sqlite does not support list type
#[case::list_of_lists(get_arrow_list_of_lists_record_batch(), "list_of_lists")]
#[ignore] // sqlite does not support map type
#[case::map(get_arrow_map_record_batch(), "map")]
#[case::dictionary(get_arrow_dictionary_array_record_batch(), "dictionary")]
#[test_log::test(tokio::test)]
async fn test_arrow_adbc_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_adbc_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

/// Tests for ADBC pushdown optimizations: projection, filter, limit, sort
mod pushdown_tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn try_get_db(driver_name: &str) -> Option<ManagedDatabase> {
        let mut driver = ManagedDriver::load_from_name(
            driver_name,
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .ok()?;

        driver
            .new_database_with_opts([(
                OptionDatabase::Uri,
                OptionValue::String(":memory:".to_string()),
            )])
            .ok()
    }

    /// Helper: create an ADBC-backed table with test data.
    /// Returns None if the ADBC driver is not available.
    async fn setup_adbc_table(
        ctx: &SessionContext,
        table_name: &str,
    ) -> Option<Arc<dyn datafusion::datasource::TableProvider>> {
        let db = try_get_db("sqlite")?;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "Diana", "Eve",
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![30, 25, 35, 28, 22])),
            ],
        )
        .unwrap();

        let adbc_pool = Arc::new(ADBCPool::new(db, None).expect("Failed to create ADBC pool"));
        let table_factory = AdbcTableFactory::new(adbc_pool);

        let mem_exec = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
            .expect("Failed to create memory source execution plan");

        let create_plan = table_factory
            .create_from(&ctx.state(), mem_exec, table_name.into())
            .await
            .expect("Failed to create table");

        let _ = collect(create_plan, ctx.task_ctx())
            .await
            .expect("Table creation failed");

        let table_provider = table_factory
            .table_provider(table_name.into(), None)
            .await
            .expect("Failed to get table provider");

        ctx.register_table(table_name, Arc::clone(&table_provider))
            .expect("Failed to register table");

        Some(table_provider)
    }

    macro_rules! adbc_test {
        ($ctx:ident, $table:literal) => {
            let $ctx = SessionContext::new();
            if setup_adbc_table(&$ctx, $table).await.is_none() {
                eprintln!("Skipping test: ADBC SQLite driver not found");
                return;
            }
        };
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_projection_pushdown() {
        adbc_test!(ctx, "proj_test");

        let df = ctx
            .sql("SELECT name FROM proj_test")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].num_rows(), 5);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_filter_pushdown() {
        adbc_test!(ctx, "filter_test");

        let df = ctx
            .sql("SELECT * FROM filter_test WHERE age > 27")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // Alice(30), Charlie(35), Diana(28)
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_limit_pushdown() {
        adbc_test!(ctx, "limit_test");

        let df = ctx
            .sql("SELECT * FROM limit_test LIMIT 2")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_filter_and_limit_pushdown() {
        adbc_test!(ctx, "filter_limit_test");

        let df = ctx
            .sql("SELECT * FROM filter_limit_test WHERE age >= 28 LIMIT 2")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_projection_and_filter_pushdown() {
        adbc_test!(ctx, "proj_filter_test");

        let df = ctx
            .sql("SELECT name, age FROM proj_filter_test WHERE age < 30")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        assert_eq!(batches[0].num_columns(), 2);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // Bob(25), Diana(28), Eve(22)
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_sort_pushdown() {
        adbc_test!(ctx, "sort_test");

        let df = ctx
            .sql("SELECT name, age FROM sort_test ORDER BY age ASC")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let age_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        let ages: Vec<i32> = (0..age_col.len()).map(|i| age_col.value(i)).collect();
        assert_eq!(ages, vec![22, 25, 28, 30, 35]);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_sort_desc_pushdown() {
        adbc_test!(ctx, "sort_desc_test");

        let df = ctx
            .sql("SELECT name, age FROM sort_desc_test ORDER BY age DESC")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let age_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        let ages: Vec<i32> = (0..age_col.len()).map(|i| age_col.value(i)).collect();
        assert_eq!(ages, vec![35, 30, 28, 25, 22]);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_sort_with_limit_pushdown() {
        adbc_test!(ctx, "sort_limit_test");

        // TopK pattern: ORDER BY + LIMIT
        let df = ctx
            .sql("SELECT name, age FROM sort_limit_test ORDER BY age DESC LIMIT 3")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let age_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        let ages: Vec<i32> = (0..age_col.len()).map(|i| age_col.value(i)).collect();
        assert_eq!(ages, vec![35, 30, 28]);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_multi_column_sort_pushdown() {
        adbc_test!(ctx, "multi_sort_test");

        let df = ctx
            .sql("SELECT * FROM multi_sort_test ORDER BY age ASC, name DESC")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test_log::test(tokio::test)]
    async fn test_adbc_all_pushdowns_combined() {
        adbc_test!(ctx, "combined_test");

        // projection + filter + sort + limit all at once
        let df = ctx
            .sql("SELECT name, age FROM combined_test WHERE age >= 25 ORDER BY age ASC LIMIT 3")
            .await
            .expect("Query should succeed");
        let batches = df.collect().await.expect("Should collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        assert_eq!(batches[0].num_columns(), 2);

        let age_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        let ages: Vec<i32> = (0..age_col.len()).map(|i| age_col.value(i)).collect();
        assert_eq!(ages, vec![25, 28, 30]);
    }
}
