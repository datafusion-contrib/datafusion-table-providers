use async_stream::stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};
use snafu::prelude::*;
use std::sync::Arc;

use crate::mongodb::utils::arrow::mongo_docs_to_arrow;
use crate::mongodb::utils::schema::infer_arrow_schema_from_documents;
use crate::mongodb::utils::unnest::{unnest_bson_documents, UnnestBehavior, UnnestParameters};
use crate::mongodb::{Error, QuerySnafu, Result, UnableToGetSchemaSnafu, UnableToGetTablesSnafu};

pub struct MongoDBConnection {
    pub client: Arc<Client>,
    pub db_name: String,
    tz: Option<String>,
    unnest_parameters: UnnestParameters,
    num_documents_to_infer_schema: i64,
}

impl MongoDBConnection {
    pub fn new(
        client: Arc<Client>,
        db_name: String,
        tz: Option<String>,
        unnest_parameters: UnnestParameters,
        num_documents_to_infer_schema: i64,
    ) -> Self {
        MongoDBConnection {
            client,
            db_name,
            tz,
            unnest_parameters,
            num_documents_to_infer_schema,
        }
    }

    fn get_collection(&self, collection: &str) -> Collection<Document> {
        self.client.database(&self.db_name).collection(collection)
    }

    pub async fn tables(&self) -> Result<Vec<String>, Error> {
        let db = self.client.database(&self.db_name);
        let collections = db
            .list_collection_names()
            .await
            .boxed()
            .context(UnableToGetTablesSnafu)?;
        Ok(collections)
    }

    pub async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error> {
        let collection_name = table_reference.table();
        let coll = self.get_collection(collection_name);

        let sample = coll
            .find(doc! {})
            .limit(self.num_documents_to_infer_schema)
            .await
            .boxed()
            .context(UnableToGetSchemaSnafu)?;

        let docs: Vec<Document> = sample
            .try_collect()
            .await
            .boxed()
            .context(UnableToGetSchemaSnafu)?;

        let unnested_docs = match self.unnest_parameters.behavior {
            UnnestBehavior::Depth(0) => docs,
            _ => unnest_bson_documents(docs, &self.unnest_parameters)?,
        };

        infer_arrow_schema_from_documents(&unnested_docs, self.tz.clone().as_deref())
            .boxed()
            .context(UnableToGetSchemaSnafu)
    }

    pub async fn query_arrow(
        &self,
        table_reference: &Arc<TableReference>,
        projected_schema: &SchemaRef,
        filters_doc: &Document,
        limit: Option<i32>,
        sort_doc: &Document,
    ) -> Result<SendableRecordBatchStream> {
        let collection_name = table_reference.table();
        let coll = self.get_collection(collection_name);

        let mut find = coll
            .find(filters_doc.clone())
            .projection(schema_to_mongo_projection(projected_schema));

        if !sort_doc.is_empty() {
            find = find.sort(sort_doc.clone());
        }

        if let Some(l) = limit {
            find = find.limit(l.into());
        }

        let cursor = find.await.boxed().context(QuerySnafu)?;
        let chunked_stream = cursor.try_chunks(4_000);
        let projected_schema_clone = Arc::clone(projected_schema);
        let unnest_parameters = self.unnest_parameters.clone();

        let schema = Arc::clone(projected_schema);
        let batch_stream = stream! {
            for await chunk in chunked_stream {
                match chunk {
                    Ok(docs) => {
                        let unnested_docs = match unnest_parameters.behavior {
                            UnnestBehavior::Depth(0) => docs,
                            _ => {
                                unnest_bson_documents(docs, &unnest_parameters)?
                            }
                        };

                        let batch = mongo_docs_to_arrow(&unnested_docs, Arc::clone(&projected_schema_clone))?;
                        yield Ok(batch);
                    }
                    Err(e) => yield Err(Error::QueryError { source: Box::new(e) }),
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(batch_stream)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string())),
        )))
    }
}

pub fn schema_to_mongo_projection(projected_schema: &SchemaRef) -> Document {
    let mut projection = Document::new();

    if projected_schema.fields().is_empty() {
        return projection;
    }

    let has_id = projected_schema.fields().iter().any(|f| f.name() == "_id");

    for field in projected_schema.fields() {
        projection.insert(field.name(), 1);
    }

    // MongoDB always includes _id unless explicitly suppressed
    if !has_id {
        projection.insert("_id", 0);
    }

    projection
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use mongodb::bson::doc;

    #[test]
    fn test_projection_with_id() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let projection = schema_to_mongo_projection(&schema);
        assert_eq!(projection, doc! { "_id": 1, "name": 1 });
    }

    #[test]
    fn test_projection_without_id_suppresses() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        let projection = schema_to_mongo_projection(&schema);
        assert_eq!(projection, doc! { "name": 1, "age": 1, "_id": 0 });
    }

    #[test]
    fn test_projection_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let projection = schema_to_mongo_projection(&schema);
        assert!(projection.is_empty());
    }

    #[test]
    fn test_projection_single_field() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let projection = schema_to_mongo_projection(&schema);
        assert_eq!(projection, doc! { "email": 1, "_id": 0 });
    }
}
