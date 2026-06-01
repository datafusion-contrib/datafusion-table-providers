use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::handle_unsupported_type_error;
use crate::UnsupportedTypeAction;
use arrow_schema::{DataType, Field, Schema, SchemaBuilder};
use datafusion::arrow::datatypes::SchemaRef;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = GenericError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn test_merge_schemas_no_declared_returns_inferred() {
        let inferred = schema(vec![
            ("id", DataType::Int32, true),
            ("name", DataType::Utf8, true),
        ]);
        let result = merge_inferred_and_declared_schemas(Arc::clone(&inferred), None);
        assert_eq!(result, inferred);
    }

    #[test]
    fn test_merge_schemas_declared_overrides_inferred_field() {
        // inferred has `id` as Int32; declared overrides it to Int64 and non-nullable
        let inferred = schema(vec![
            ("id", DataType::Int32, true),
            ("name", DataType::Utf8, true),
        ]);
        let declared = schema(vec![("id", DataType::Int64, false)]);
        let result = merge_inferred_and_declared_schemas(inferred, Some(&declared));

        let field = result.field_with_name("id").expect("field exists");
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable());
        // `name` is still present from inferred
        assert!(result.field_with_name("name").is_ok());
    }

    #[test]
    fn test_merge_schemas_declared_only_field_appended() {
        let inferred = schema(vec![("name", DataType::Utf8, true)]);
        let declared = schema(vec![("score", DataType::Float64, true)]);
        let result = merge_inferred_and_declared_schemas(inferred, Some(&declared));

        assert_eq!(result.fields().len(), 2);
        assert!(result.field_with_name("name").is_ok());
        assert!(result.field_with_name("score").is_ok());
    }

    #[test]
    fn test_merge_schemas_declared_only_fields_come_after_inferred() {
        let inferred = schema(vec![
            ("a", DataType::Utf8, true),
            ("b", DataType::Utf8, true),
        ]);
        let declared = schema(vec![("c", DataType::Utf8, true)]);
        let result = merge_inferred_and_declared_schemas(inferred, Some(&declared));

        let names: Vec<&str> = result.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_merge_schemas_empty_inferred() {
        let inferred = Arc::new(Schema::empty());
        let declared = schema(vec![("id", DataType::Int32, false)]);
        let result = merge_inferred_and_declared_schemas(inferred, Some(&declared));

        assert_eq!(result.fields().len(), 1);
        assert_eq!(
            result
                .field_with_name("id")
                .expect("field exists")
                .data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn test_merge_schemas_empty_declared() {
        let inferred = schema(vec![("id", DataType::Int32, true)]);
        let declared = Arc::new(Schema::empty());
        let result = merge_inferred_and_declared_schemas(Arc::clone(&inferred), Some(&declared));

        assert_eq!(result, inferred);
    }
}

/// Merge an inferred schema with a declared schema.
///
/// Declared fields override inferred fields with the same name; fields that
/// only appear in the inferred schema are kept; fields that only appear in
/// the declared schema are appended after the inferred ones.
#[must_use]
pub fn merge_inferred_and_declared_schemas(inferred: SchemaRef, declared: Option<&SchemaRef>) -> SchemaRef {
    let Some(declared) = declared else {
        return inferred;
    };

    let declared_by_name: HashMap<&str, &Field> = declared
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();
    let inferred_names: HashSet<&str> = inferred
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mut merged: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|f| {
            declared_by_name
                .get(f.name().as_str())
                .copied()
                .cloned()
                .unwrap_or_else(|| f.as_ref().clone())
        })
        .collect();

    for f in declared.fields() {
        if !inferred_names.contains(f.name().as_str()) {
            merged.push(f.as_ref().clone());
        }
    }

    Arc::new(Schema::new(merged))
}

pub trait SchemaValidator {
    type Error: std::error::Error + Send + Sync;

    #[must_use]
    fn is_field_supported(field: &Arc<Field>) -> bool {
        Self::is_data_type_supported(field.data_type())
    }

    #[must_use]
    fn is_schema_supported(schema: &SchemaRef) -> bool {
        schema.fields.iter().all(Self::is_field_supported)
    }

    fn is_data_type_supported(data_type: &DataType) -> bool;

    fn unsupported_type_error(data_type: &DataType, field_name: &str) -> Self::Error;

    /// For a given input schema, rebuild it according to the `UnsupportedTypeAction`.
    /// Implemented for a specific connection type, that connection determines which types it supports.
    /// If the `UnsupportedTypeAction` is `Error`/`String`, the function will return an error if the schema contains an unsupported data type.
    /// If the `UnsupportedTypeAction` is `Warn`, the function will log a warning if the schema contains an unsupported data type and remove the column.
    /// If the `UnsupportedTypeAction` is `Ignore`, the function will remove the column silently.
    ///
    /// Components that want to handle `UnsupportedTypeAction::String` via the component's own logic should re-implement this trait function.
    ///
    /// # Errors
    ///
    /// If the `UnsupportedTypeAction` is `Error` or `String` and the schema contains an unsupported data type, the function will return an error.
    fn handle_unsupported_schema(
        schema: &SchemaRef,
        unsupported_type_action: UnsupportedTypeAction,
    ) -> Result<SchemaRef, Self::Error> {
        let mut schema_builder = SchemaBuilder::new();
        for field in &schema.fields {
            if Self::is_field_supported(field) {
                schema_builder.push(Arc::clone(field));
            } else {
                handle_unsupported_type_error(
                    unsupported_type_action,
                    Self::unsupported_type_error(field.data_type(), field.name()),
                )?;
            }
        }

        Ok(Arc::new(schema_builder.finish()))
    }
}
