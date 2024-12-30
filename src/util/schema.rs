use std::sync::Arc;

use super::handle_invalid_type_error;
use crate::sql::db_connection_pool::dbconnection::Error as DbConnectionError;
use crate::InvalidTypeAction;
use arrow_schema::{DataType, Field, SchemaBuilder};
use datafusion::arrow::datatypes::SchemaRef;

#[cfg(feature = "sqlite")]
use crate::sql::db_connection_pool::dbconnection::sqliteconn::SqliteConnection;

#[cfg(feature = "duckdb")]
use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
#[cfg(feature = "duckdb")]
use duckdb::vtab::to_duckdb_type_id;

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

#[cfg(feature = "duckdb")]
impl SchemaValidator for DuckDbConnection {
    type Error = DbConnectionError;

    fn is_data_type_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::List(inner_field)
            | DataType::FixedSizeList(inner_field, _)
            | DataType::LargeList(inner_field) => {
                match inner_field.data_type() {
                    dt if dt.is_primitive() => true,
                    DataType::Utf8
                    | DataType::Binary
                    | DataType::Utf8View
                    | DataType::BinaryView
                    | DataType::Boolean => true,
                    _ => false, // nested lists don't support anything else yet
                }
            }
            DataType::Struct(inner_fields) => inner_fields
                .iter()
                .all(|field| Self::is_data_type_valid(field.data_type())),
            _ => true,
        }
    }

    fn is_field_valid(field: &Arc<Field>) -> bool {
        let duckdb_type_id = to_duckdb_type_id(field.data_type());
        Self::is_data_type_valid(field.data_type()) && duckdb_type_id.is_ok()
    }

    fn invalid_type_error(data_type: &DataType, field_name: &str) -> Self::Error {
        DbConnectionError::UnsupportedDataType {
            data_type: data_type.to_string(),
            field_name: field_name.to_string(),
        }
    }
}

#[cfg(feature = "sqlite")]
impl SchemaValidator for SqliteConnection {
    type Error = DbConnectionError;

    fn is_data_type_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Dictionary(_, _) | DataType::Interval(_) | DataType::Map(_, _) => false,
            DataType::List(inner_field)
            | DataType::FixedSizeList(inner_field, _)
            | DataType::LargeList(inner_field) => {
                match inner_field.data_type() {
                    dt if dt.is_primitive() => true,
                    DataType::Utf8
                    | DataType::Binary
                    | DataType::Utf8View
                    | DataType::BinaryView
                    | DataType::Boolean => true,
                    _ => false, // nested lists don't support anything else yet
                }
            }
            DataType::Struct(inner_fields) => inner_fields
                .iter()
                .all(|field| Self::is_data_type_valid(field.data_type())),
            _ => true,
        }
    }

    fn invalid_type_error(data_type: &DataType, field_name: &str) -> Self::Error {
        DbConnectionError::UnsupportedDataType {
            data_type: data_type.to_string(),
            field_name: field_name.to_string(),
        }
    }
}
