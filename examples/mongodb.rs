use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    mongodb::{connection_pool::MongoDBConnectionPool, MongoDBTableFactory}, util::secrets::to_secret_map,
};
use mongodb::{options::ClientOptions, Client};

async fn get_mongodb_client(port: usize) -> Result<Client, anyhow::Error> {
    let connection_string = format!("mongodb://root:password@localhost:{port}/mongo_db?authSource=admin");
    
    let client_options = ClientOptions::parse(connection_string)
        .await
        .expect("Failed to parse MongoDB connection string");
    
    let client = Client::with_options(client_options)
        .expect("Failed to create MongoDB client");
    
    // Test the connection
    let mut retries = 10;
    let mut last_err = None;
    while retries > 0 {
        match client
            .database("testdb")
            .run_command(mongodb::bson::doc! { "ping": 1 })
            .await
        {
            Ok(_) => {println!("Client created"); return Ok(client)},
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                retries -= 1;
                println!("Ping failed");
            }
        }
    }

    Ok(client)
}

/// This example demonstrates how to:
/// 1. Create a MySQL connection pool
/// 2. Create and use MySQLTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access MySQL table data
///
/// Prerequisites:
/// Start a MongoDB server using Docker:
/// ```bash
/// docker run --name mongodb \
/// -e MONGO_INITDB_ROOT_USERNAME=root \
/// -e MONGO_INITDB_ROOT_PASSWORD=password \
/// -e MONGO_INITDB_DATABASE=mongo_db \
/// -p 27017:27017 \
/// -d mongo:7.0
/// # Wait for the MongoDB server to start
/// sleep 30
///
/// # Create a table in the MongoDB server and insert some data
/// docker exec -i mongodb mongosh -u root -p password --authenticationDatabase admin <<EOF
/// use mongo_db;
///
/// db.companies.insertOne({
/// id: 1,
/// name: "Acme Corporation"
/// });
/// EOF
/// ```
#[tokio::main]
async fn main(){

    let c = get_mongodb_client(27017).await.unwrap();
    let n = c.database("mongo_db").list_collection_names().await.unwrap();
    println!("{:?}", n);

    
    // Create MongoDB connection parameters
    // Including connection string and SSL mode settings
    let mongodb_params = to_secret_map(HashMap::from([
        (
            "connection_string".to_string(),
            "mongodb://root:password@localhost:27017/mongo_db?authSource=admin".to_string(),
        ),
    ]));

    // Create MySQL connection pool
    let mongodb_pool = Arc::new(
        MongoDBConnectionPool::new(mongodb_params)
            .await
            .expect("unable to create MongoDB connection pool"),
    );

    // Create MongoDB table provider factory
    // Used to generate TableProvider instances that can read MongoDB table data
    let table_factory = MongoDBTableFactory::new(mongodb_pool.clone());

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the MongoDB "companies" table as "companies_v2"
    ctx.register_table(
        "companies_v2",
        table_factory
            .table_provider(TableReference::bare("companies"))
            .await
            .expect("failed to register table provider"),
    )
    .expect("failed to register table");

    // Query Example: Query the renamed table through default catalog
    let df = ctx
        .sql("SELECT * FROM datafusion.public.companies_v2")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}


