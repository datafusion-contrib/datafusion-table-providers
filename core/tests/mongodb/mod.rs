use std::time::SystemTime;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use datafusion::{error::DataFusionError, execution::context::SessionContext};
use datafusion_table_providers::mongodb::table::MongoDBTable;
use mongodb::bson::{doc, Document, Bson, DateTime as BsonDateTime};
use rstest::rstest;

use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, TimeUnit},
};

use crate::docker::RunningContainer;

mod common;

async fn test_mongodb_timestamp_types(port: usize) {
    let ts0 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00Z").unwrap().with_timezone(&Utc);
    let ts1 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00.1Z").unwrap().with_timezone(&Utc);
    let ts2 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00.12Z").unwrap().with_timezone(&Utc);
    let ts3 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00.123Z").unwrap().with_timezone(&Utc);

    let test_docs = vec![
        doc! {
            "timestamp_field": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts0))),
            "timestamp_one_fraction": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts1))),
            "timestamp_two_fraction": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts2))),
            "timestamp_three_fraction": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts3))),
        }
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_field", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("timestamp_one_fraction", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("timestamp_two_fraction", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("timestamp_three_fraction", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_000])),
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_100])),
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_120])),
            Arc::new(TimestampMillisecondArray::from(vec![1_726_135_200_123])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "timestamp_collection",
        test_docs,
        expected_record,
    )
    .await;
}

async fn test_mongodb_numeric_types(port: usize) {
    let test_docs = vec![
        doc! {
            "int32_field": 2147483647i32,
            "int64_field": 9223372036854775807i64,
            "double_field": 3.14159265359,
            // "decimal_field": Bson::Decimal128(mongodb::bson::Decimal128::from_bytes([0u8; 16])),
        }
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("int32_field", DataType::Int32, true),
        Field::new("int64_field", DataType::Int64, true),
        Field::new("double_field", DataType::Float64, true),
        // Field::new("decimal_field", DataType::Decimal128(38, 0), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2147483647i32])),
            Arc::new(Int64Array::from(vec![9223372036854775807i64])),
            Arc::new(Float64Array::from(vec![3.14159265359])),
            // Arc::new(
            //     Decimal128Array::from(vec![Some(0i128)])
            //         .with_precision_and_scale(38, 0)
            //         .unwrap(),
            // ),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "numeric_collection",
        test_docs,
        expected_record,
    )
    .await;
}

async fn test_mongodb_string_types(port: usize) {
    let test_docs = vec![
        doc! {
            "name": "Alice",
            "description": "Software Engineer",
            "notes": Bson::Null,
        },
        doc! {
            "name": "Bob",
            "description": "Data Scientist", 
            "notes": "Likes MongoDB",
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("notes", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["Software Engineer", "Data Scientist"])),
            Arc::new(StringArray::from(vec![None, Some("Likes MongoDB")])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "string_collection",
        test_docs,
        expected_record,
    )
    .await;
}

async fn test_mongodb_boolean_types(port: usize) {
    let test_docs = vec![
        doc! {
            "is_active": true,
            "is_verified": false,
            "is_premium": Bson::Null,
        },
        doc! {
            "is_active": false,
            "is_verified": true,
            "is_premium": true,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("is_active", DataType::Boolean, true),
        Field::new("is_verified", DataType::Boolean, true),
        Field::new("is_premium", DataType::Boolean, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(BooleanArray::from(vec![false, true])),
            Arc::new(BooleanArray::from(vec![None, Some(true)])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "boolean_collection",
        test_docs,
        expected_record,
    )
    .await;
}

async fn test_mongodb_binary_types(port: usize) {
    let test_docs = vec![
        doc! {
            "binary_data": Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: b"hello world".to_vec(),
            }),
            "file_content": Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: b"binary file content".to_vec(),
            }),
        }
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("binary_data", DataType::Binary, true),
        Field::new("file_content", DataType::Binary, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"hello world"])),
            Arc::new(BinaryArray::from_vec(vec![b"binary file content"])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "binary_collection",
        test_docs,
        expected_record,
    )
    .await;
}

async fn test_mongodb_object_id_types(port: usize) {
    let oid1 = mongodb::bson::oid::ObjectId::new();
    let oid2 = mongodb::bson::oid::ObjectId::new();
    
    let test_docs = vec![
        doc! {
            "_id": oid1,
            "ref_id": oid2,
        }
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, true), // ObjectId typically converted to string
        Field::new("ref_id", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![oid1.to_hex()])),
            Arc::new(StringArray::from(vec![oid2.to_hex()])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "objectid_collection",
        test_docs,
        expected_record,
    )
    .await;
}

// async fn test_mongodb_array_types(port: usize) {
//     let test_docs = vec![
//         doc! {
//             "tags": ["rust", "mongodb", "arrow"],
//             "scores": [85, 92, 78],
//             "flags": [true, false, true],
//         }
//     ];

//     let schema = Arc::new(Schema::new(vec![
//         Field::new(
//             "tags",
//             DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
//             true,
//         ),
//         Field::new(
//             "scores",
//             DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
//             true,
//         ),
//         Field::new(
//             "flags",
//             DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
//             true,
//         ),
//     ]));

//     // Create list arrays
//     let tags_values = StringArray::from(vec!["rust", "mongodb", "arrow"]);
//     let tags_list = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(0), Some(1), Some(2)])]);
    
//     let scores_values = Int32Array::from(vec![85, 92, 78]);
//     let scores_list = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(0), Some(1), Some(2)])]);
    
//     let flags_values = BooleanArray::from(vec![true, false, true]);
//     let flags_list = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(0), Some(1), Some(2)])]);

//     // Note: This is a simplified version. In reality, you'd need to properly construct ListArrays
//     // For the test, we'll use a simpler approach or mark as ignored if too complex
    
//     // Simplified version - treat arrays as JSON strings for now
//     let simplified_schema = Arc::new(Schema::new(vec![
//         Field::new("tags", DataType::Utf8, true),
//         Field::new("scores", DataType::Utf8, true),
//         Field::new("flags", DataType::Utf8, true),
//     ]));

//     let simplified_record = RecordBatch::try_new(
//         Arc::clone(&simplified_schema),
//         vec![
//             Arc::new(StringArray::from(vec!["[\"rust\",\"mongodb\",\"arrow\"]"])),
//             Arc::new(StringArray::from(vec!["[85,92,78]"])),
//             Arc::new(StringArray::from(vec!["[true,false,true]"])),
//         ],
//     )
//     .expect("Failed to create arrow record batch");

//     arrow_mongodb_one_way(
//         port,
//         "array_collection",
//         test_docs,
//         simplified_record,
//     )
//     .await;
// }


async fn test_mongodb_null_and_missing_fields(port: usize) {
    let test_docs = vec![
        doc! {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
        },
        doc! {
            "name": "Bob",
            "age": Bson::Null,
            "phone": "555-1234",
        },
        doc! {
            "name": "Charlie",
            "age": 25,
            // email and phone missing
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![Some(30), None, Some(25)])),
            Arc::new(StringArray::from(vec![Some("alice@example.com"), None, None])),
            Arc::new(StringArray::from(vec![None, Some("555-1234"), None])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "null_fields_collection",
        test_docs,
        expected_record,
    )
    .await;
}

async fn arrow_mongodb_one_way(
    port: usize,
    collection_name: &str,
    test_docs: Vec<Document>,
    expected_record: RecordBatch,
) -> Vec<RecordBatch> {
    tracing::debug!("Running MongoDB tests on {collection_name}");

    let ctx = SessionContext::new();
    let client = common::get_mongodb_client(port)
        .await
        .expect("MongoDB client should be created");

    // Insert test data into MongoDB collection
    let db = client.database("testdb");
    let collection = db.collection::<Document>(collection_name);
    
    // Drop collection if it exists
    let _ = collection.drop().await;

    // Insert test documents
    if !test_docs.is_empty() {
        collection
            .insert_many(test_docs)
            .await
            .expect("MongoDB documents should be inserted");
    }

    // Register DataFusion table
    let mongo_conn_pool = common::get_mongodb_connection_pool(port)
        .await
        .expect("MongoDB connection pool should be created");

    let table = MongoDBTable::new(&Arc::new(mongo_conn_pool), collection_name)
        .await
        .expect("Table should be created");

    ctx.register_table(collection_name, Arc::new(table))
        .expect("Table should be registered");

    // Extract expected columns (excluding _id)
    let schema_ref = expected_record.schema();
    let expected_fields: Vec<&str> = schema_ref
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| *name != "_id")
        .collect();

    // Build SELECT query with correct projection
    let projection = expected_fields
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {projection} FROM {collection_name}");

    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batches = df.collect().await.expect("RecordBatch should be collected");
    assert_eq!(record_batches.len(), 1);

    // Normalize actual and expected
    let actual_projected =
        project_record_batch(&record_batches[0], &expected_fields).expect("Project actual");
    let expected_projected =
        project_record_batch(&expected_record, &expected_fields).expect("Project expected");

    assert_eq!(actual_projected, expected_projected);

    record_batches
}

use datafusion::common::Result as DFResult;
fn project_record_batch(batch: &RecordBatch, columns: &[&str]) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let indices: Vec<usize> = columns
        .iter()
        .map(|col| schema.index_of(col).expect("Column not found"))
        .collect();
    let arrays = indices.iter().map(|&i| batch.column(i).clone()).collect();
    let fields = indices
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect::<Vec<_>>();
    let projected_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(projected_schema, arrays)
         .map_err(|e| DataFusionError::ArrowError(e, None))
}

async fn start_mongodb_container(port: usize) -> RunningContainer {
    let running_container = common::start_mongodb_docker_container(port)
        .await
        .expect("MongoDB container to start");

    tracing::debug!("MongoDB Container started");

    running_container
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_mongodb_arrow_oneway() {
    let port = crate::get_random_port();
    let mongodb_container = start_mongodb_container(port).await;

    test_mongodb_timestamp_types(port).await;
    test_mongodb_numeric_types(port).await;
    test_mongodb_string_types(port).await;
    test_mongodb_boolean_types(port).await;
    test_mongodb_binary_types(port).await;
    test_mongodb_object_id_types(port).await;
    // test_mongodb_array_types(port).await;
    // test_mongodb_nested_object_types(port).await;
    test_mongodb_null_and_missing_fields(port).await;

    mongodb_container.remove().await.expect("container to stop");
}