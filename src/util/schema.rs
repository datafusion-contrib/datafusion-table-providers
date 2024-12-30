use std::sync::Arc;

use super::handle_invalid_type_error;
use crate::InvalidTypeAction;
use arrow_schema::{DataType, Field, SchemaBuilder};
use datafusion::arrow::datatypes::SchemaRef;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = GenericError> = std::result::Result<T, E>;

pub trait SchemaValidator {
    type Error: std::error::Error + Send + Sync;

    #[must_use]
    fn is_field_valid(field: &Arc<Field>) -> bool {
        Self::is_data_type_valid(field.data_type())
    }

    #[must_use]
    fn is_schema_valid(schema: &SchemaRef) -> bool {
        schema.fields.iter().all(Self::is_field_valid)
    }

    fn is_data_type_valid(data_type: &DataType) -> bool;
    fn invalid_type_error(data_type: &DataType, field_name: &str) -> Self::Error;

    /// For a given input schema, rebuild it according to the `InvalidTypeAction`.
    /// Implemented for a specific connection type, that connection determines which types it supports.
    /// If the `InvalidTypeAction` is `Error`, the function will return an error if the schema contains an unsupported data type.
    /// If the `InvalidTypeAction` is `Warn`, the function will log a warning if the schema contains an unsupported data type and remove the column.
    /// If the `InvalidTypeAction` is `Ignore`, the function will remove the column silently.
    ///
    /// # Errors
    ///
    /// If the `InvalidTypeAction` is `Error` and the schema contains an unsupported data type, the function will return an error.
    fn handle_unsupported_schema(
        schema: &SchemaRef,
        invalid_type_action: InvalidTypeAction,
    ) -> Result<SchemaRef, Self::Error> {
        let mut schema_builder = SchemaBuilder::new();
        for field in &schema.fields {
            if Self::is_field_valid(field) {
                schema_builder.push(Arc::clone(field));
            } else {
                handle_invalid_type_error(
                    invalid_type_action,
                    Self::invalid_type_error(field.data_type(), field.name()),
                )?;
            }
        }

        Ok(Arc::new(schema_builder.finish()))
    }
}
