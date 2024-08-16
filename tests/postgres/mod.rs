#[cfg(feature = "postgres")]
mod common;

#[cfg(feature = "postgres")]
mod test {
    use super::common;
    use crate::arrow_record_batch_gen::*;
    use arrow::array::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use datafusion_table_providers::sql::arrow_sql_gen::statement::{
        CreateTableBuilder, InsertBuilder,
    };
    use datafusion_table_providers::{
        postgres::DynPostgresConnectionPool, sql::sql_provider_datafusion::SqlTable,
    };
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use tokio::sync::{Mutex, MutexGuard};

    async fn arrow_postgres_round_trip(port: usize, arrow_record: RecordBatch, table_name: &str) {
        tracing::debug!("Running tests on {table_name}");
        let ctx = SessionContext::new();

        let pool = common::get_postgres_connection_pool(port)
            .await
            .expect("Postgres connection pool should be created");

        let db_conn = pool
            .connect_direct()
            .await
            .expect("Connection should be established");

        // Create postgres table from arrow records and insert arrow records
        let schema = Arc::clone(&arrow_record.schema());
        let create_table_stmts = CreateTableBuilder::new(schema, table_name).build_postgres();
        let insert_table_stmt = InsertBuilder::new(table_name, vec![arrow_record.clone()])
            .build_postgres(None)
            .expect("Postgres insert statement should be constructed");

        // Test arrow -> Postgres row coverage
        for create_table_stmt in create_table_stmts {
            let _ = db_conn
                .conn
                .execute(&create_table_stmt, &[])
                .await
                .expect("Postgres table should be created");
        }
        let _ = db_conn
            .conn
            .execute(&insert_table_stmt, &[])
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

    #[derive(Debug)]
    struct ContainerManager {
        port: usize,
        claimed: bool,
    }

    #[fixture]
    #[once]
    fn container_manager() -> Mutex<ContainerManager> {
        Mutex::new(ContainerManager {
            port: common::get_random_port(),
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
    #[ignore] // TODO: time types are broken in Postgres
    #[case::time(get_arrow_time_record_batch(), "time")]
    #[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
    #[case::date(get_arrow_date_record_batch(), "date")]
    #[case::struct_type(get_arrow_struct_record_batch(), "struct")]
    #[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
    #[case::interval(get_arrow_interval_record_batch(), "interval")]
    #[ignore] // TODO: duration types are broken in Postgres
    #[case::duration(get_arrow_duration_record_batch(), "duration")]
    #[ignore] // TODO: list types are broken in Postgres
    #[case::list(get_arrow_list_record_batch(), "list")]
    #[case::null(get_arrow_null_record_batch(), "null")]
    #[test_log::test(tokio::test)]
    async fn test_arrow_postgres_roundtrip(
        container_manager: &Mutex<ContainerManager>,
        #[case] arrow_record: RecordBatch,
        #[case] table_name: &str,
    ) {
        let mut container_manager = container_manager.lock().await;
        if !container_manager.claimed {
            container_manager.claimed = true;
            start_container(&container_manager).await;
        }

        arrow_postgres_round_trip(
            container_manager.port,
            arrow_record,
            &format!("{table_name}_types"),
        )
        .await;
    }
}
