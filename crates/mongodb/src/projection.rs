//! `RowShape` adapter for MongoDB BSON, letting the connector-agnostic
//! [`datafusion_table_providers_common::schema_projection`] core apply JSON
//! nesting to MongoDB documents on the scan path.

use datafusion_table_providers_common::schema_projection::{RowShape, SchemaProjection};
use mongodb::bson::{Bson, Document};

/// Local wrapper that permits implementing the common projection trait for BSON
/// without violating Rust's orphan rules.
struct BsonRow(Bson);

impl RowShape for BsonRow {
    fn into_object(self) -> Result<Vec<(String, Self)>, Self> {
        match self.0 {
            Bson::Document(doc) => Ok(doc
                .into_iter()
                .map(|(key, value)| (key, Self(value)))
                .collect()),
            other => Err(Self(other)),
        }
    }

    fn from_object(entries: Vec<(String, Self)>) -> Self {
        Self(Bson::Document(
            entries
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect(),
        ))
    }

    fn to_json(&self) -> serde_json::Value {
        // Relaxed Extended JSON keeps ordinary values (numbers, strings, bools)
        // as plain JSON while round-tripping MongoDB-specific types.
        self.0.clone().into_relaxed_extjson()
    }

    fn from_json_string(json: String) -> Self {
        Self(Bson::String(json))
    }
}

/// Reshape one BSON document through a [`SchemaProjection`], preserving the
/// `Document` type. Non-declared fields are folded into the catch-all column.
#[must_use]
pub fn project_bson_document(doc: Document, projection: &SchemaProjection) -> Document {
    match projection.project_row(BsonRow(Bson::Document(doc))).0 {
        Bson::Document(doc) => doc,
        _ => Document::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_table_providers_common::schema_projection::{ColumnSource, ProjectedColumn};
    use mongodb::bson::doc;

    fn nesting_projection() -> SchemaProjection {
        SchemaProjection::new(
            vec![
                ProjectedColumn {
                    output_name: "_id".to_string(),
                    source: ColumnSource::Field,
                    declared_type: None,
                    nullable: true,
                },
                ProjectedColumn {
                    output_name: "data".to_string(),
                    source: ColumnSource::JsonObject,
                    declared_type: None,
                    nullable: true,
                },
            ],
            &["_id".to_string()],
        )
        .expect("projection")
    }

    #[test]
    fn folds_non_declared_fields_into_catch_all() {
        let projection = nesting_projection();
        let doc = doc! { "_id": "row1", "zeta": 2, "alpha": 1 };
        let out = project_bson_document(doc, &projection);

        assert_eq!(out.get_str("_id").expect("_id"), "row1");
        // Non-declared fields, alphabetically sorted, in the catch-all string.
        assert_eq!(
            out.get_str("data").expect("data"),
            r#"{"alpha":1,"zeta":2}"#
        );
        assert!(!out.contains_key("zeta"));
    }

    #[test]
    fn identity_projection_passes_through() {
        // No catch-all → document unchanged.
        let projection = SchemaProjection::new(
            vec![ProjectedColumn {
                output_name: "_id".to_string(),
                source: ColumnSource::Field,
                declared_type: None,
                nullable: true,
            }],
            &[],
        )
        .expect("projection");
        let doc = doc! { "_id": "row1", "other": 5 };
        let out = project_bson_document(doc.clone(), &projection);
        assert_eq!(out, doc);
    }
}
