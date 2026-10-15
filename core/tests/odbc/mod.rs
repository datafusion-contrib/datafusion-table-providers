use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion_table_providers::sql::db_connection_pool::{odbcpool::ODBCPool, DbConnectionPool};
use datafusion_table_providers::util::secrets::to_secret_map;

/// The SQLite ODBC driver to use: a driver name registered in `odbcinst.ini`
/// or a path to the driver library.
fn sqlite_odbc_driver() -> String {
    std::env::var("ODBC_SQLITE_DRIVER").unwrap_or_else(|_| "SQLite3".to_string())
}

fn get_odbc_pool(db_path: &str) -> ODBCPool {
    let params = to_secret_map(HashMap::from([(
        "connection_string".to_string(),
        format!("driver={};database={db_path};", sqlite_odbc_driver()),
    )]));
    ODBCPool::new(params).expect("ODBC pool should be created")
}

/// A pool created with `with_schema` must return the schema provided for a table from
/// `get_schema` instead of inferring it from the database.
#[tokio::test]
async fn test_odbc_connection_pool_with_schema() {
    let dir = tempfile::tempdir().expect("temp dir should be created");
    let db_path = dir.path().join("with_schema.db");
    let db_path = db_path.to_str().expect("path should be valid UTF-8");
    let table_reference = TableReference::bare("with_schema_table");

    let pool = get_odbc_pool(db_path);
    let conn = match pool.connect().await {
        Ok(conn) => conn,
        Err(e) => {
            if std::env::var("CI").is_ok() {
                panic!("SQLite ODBC driver unavailable in CI: {e}");
            }
            eprintln!(
                "Skipping: unable to connect with ODBC driver '{}': {e}",
                sqlite_odbc_driver()
            );
            return;
        }
    };
    let conn = conn.as_async().expect("should be async connection");
    conn.execute(
        "CREATE TABLE with_schema_table (id INTEGER, name TEXT)",
        &[],
    )
    .await
    .expect("table should be created");
    conn.execute(
        "INSERT INTO with_schema_table VALUES (1, NULL), (2, 'a')",
        &[],
    )
    .await
    .expect("row should be inserted");

    // the schema is inferred from the database when none is provided
    let inferred = conn
        .get_schema(&table_reference)
        .await
        .expect("schema should be inferred");
    assert_eq!(
        inferred.fields.len(),
        2,
        "There should be 2 fields in the inferred table"
    );
    let field1 = inferred.field(0);
    assert_eq!(field1.name(), "id");
    assert_eq!(field1.data_type(), &DataType::Int32);
    assert_eq!(field1.is_nullable(), true);
    let field2 = inferred.field(1);
    assert_eq!(field2.name(), "name");
    assert_eq!(field2.data_type(), &DataType::Utf8); // ODBCConnection::get_schema() infers it correctly
    assert_eq!(field2.is_nullable(), true);

    // the provided schemas are used as-is, each one for its own table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int8, false),
        Field::new("name", DataType::LargeUtf8, true),
    ]));
    // not created in the database, so its schema can only come from the pool
    let other_table_reference = TableReference::bare("other_table");
    let other_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        true,
    )]));
    let pool = get_odbc_pool(db_path)
        .with_schema(table_reference.clone(), Arc::clone(&schema))
        .with_schema(other_table_reference.clone(), Arc::clone(&other_schema));
    let conn = pool.connect().await.expect("connect should work");
    let conn = conn.as_async().expect("should be async connection");
    let actual = conn
        .get_schema(&table_reference)
        .await
        .expect("schema should be returned");
    assert_eq!(actual, schema);
    let actual = conn
        .get_schema(&other_table_reference)
        .await
        .expect("schema should be returned");
    assert_eq!(actual, other_schema);

    // the schema of a table without a provided schema is still inferred
    let pool = get_odbc_pool(db_path).with_schema(other_table_reference, other_schema);
    let conn = pool.connect().await.expect("connect should work");
    let actual = conn
        .as_async()
        .expect("should be async connection")
        .get_schema(&table_reference)
        .await
        .expect("schema should be inferred");
    assert_eq!(actual, inferred);
}
