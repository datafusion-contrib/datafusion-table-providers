use datafusion::execution::context::SessionContext;
use datafusion_table_providers::{
    mysql::MySQLConnectionPool, sql::sql_provider_datafusion::SqlTable,
};
use rstest::{fixture, rstest};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

use arrow::array::*;

use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;

mod common;

async fn arrow_mysql_one_way(port: usize) {
    let table_name = "test_table";
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = common::get_mysql_connection_pool(port)
        .await
        .expect("MySQL connection pool should be created");

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    // Create mysql table with decimal columns that contains null value
    let create_table_stmt = "
        CREATE TABLE IF NOT EXISTS test_table (id INT AUTO_INCREMENT PRIMARY KEY, salary DECIMAL(10, 2));
        ";
    let insert_table_stmt = "
        INSERT INTO test_table (salary) VALUES (NULL), (12);
        ";

    // Create table and insert data into mysql test_table
    let _ = db_conn
        .execute(create_table_stmt, &[])
        .await
        .expect("MySQL table should be created");

    let _ = db_conn
        .execute(insert_table_stmt, &[])
        .await
        .expect("MySQL table data should be inserted");

    // Register datafusion table, test mysql row -> arrow conversion
    let sqltable_pool: Arc<MySQLConnectionPool> = Arc::new(pool);
    let table = SqlTable::new("mysql", &sqltable_pool, table_name, None)
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
    tracing::debug!(
        "MySQL returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    let int32_array = Int32Array::from(vec![1, 2]);
    let decimal128_array = Decimal128Array::from(vec![None, Some(i128::from(1200))])
        .with_precision_and_scale(38, 2)
        .unwrap();

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(
        record_batch[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &int32_array
    );
    assert_eq!(
        record_batch[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap(),
        &decimal128_array
    );
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
    let _ = common::start_mysql_docker_container(manager.port)
        .await
        .expect("MySQL container to start");

    tracing::debug!("Container started");
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_mysql_arrow_oneway(container_manager: &Mutex<ContainerManager>) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_container(&container_manager).await;
    }

    arrow_mysql_one_way(container_manager.port).await;
}
