use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::handle_unsupported_type_error;
use crate::UnsupportedTypeAction;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = GenericError> = std::result::Result<T, E>;

/// Merge an inferred schema with a declared schema.
///
/// Declared fields override inferred fields with the same name; inferred-only
/// fields are kept, and declared-only fields are appended.
#[must_use]
pub fn merge_inferred_and_declared_schemas(
    inferred: SchemaRef,
    declared: Option<&SchemaRef>,
) -> SchemaRef {
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

    for field in declared.fields() {
        if !inferred_names.contains(field.name().as_str()) {
            merged.push(field.as_ref().clone());
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
