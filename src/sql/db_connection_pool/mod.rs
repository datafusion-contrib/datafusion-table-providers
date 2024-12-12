use arrow_schema::{DataType, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use dbconnection::{duckdbconn::data_type_is_unsupported, DbConnection};
use std::sync::Arc;

use crate::InvalidTypeAction;

pub mod dbconnection;
#[cfg(feature = "duckdb")]
pub mod duckdbpool;
#[cfg(feature = "mysql")]
pub mod mysqlpool;
#[cfg(feature = "postgres")]
pub mod postgrespool;
#[cfg(feature = "sqlite")]
pub mod sqlitepool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

/// Controls whether join pushdown is allowed, and under what conditions
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinPushDown {
    /// This connection pool should not allow join push down. (i.e. we don't know under what conditions it is safe to send a join query to the database)
    Disallow,
    /// Allows join push down for other tables that share the same context.
    ///
    /// The context can be part of the connection string that uniquely identifies the server.
    AllowedFor(String),
}

#[async_trait]
pub trait DbConnectionPool<T, P: 'static> {
    async fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;

    fn join_push_down(&self) -> JoinPushDown;
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    #[default]
    Memory,
    File,
}

impl From<&str> for Mode {
    fn from(m: &str) -> Self {
        match m {
            "file" => Mode::File,
            "memory" => Mode::Memory,
            _ => Mode::default(),
        }
    }
}

/// A key that uniquely identifies a database instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbInstanceKey {
    /// The database is a file on disk, with the given path.
    File(Arc<str>),
    /// The database is in memory.
    Memory,
}

impl DbInstanceKey {
    pub fn memory() -> Self {
        DbInstanceKey::Memory
    }

    pub fn file(path: Arc<str>) -> Self {
        DbInstanceKey::File(path)
    }
}

pub(crate) fn handle_unsupported_data_type(
    data_type: &str,
    field_name: &str,
    invalid_type_action: InvalidTypeAction,
) -> Result<(), dbconnection::Error> {
    let error = dbconnection::Error::UnsupportedDataType {
        data_type: data_type.to_string(),
        field_name: field_name.to_string(),
    };
    match invalid_type_action {
        InvalidTypeAction::Error => {
            return Err(error);
        }
        InvalidTypeAction::Warn => {
            tracing::warn!("{error}");
        }
        InvalidTypeAction::Ignore => {}
    }
    Ok(())
}

pub(crate) fn parse_schema_for_unsupported_types(
    schema: &SchemaRef,
    invalid_type_action: InvalidTypeAction,
    is_unsupported_fn: impl Fn(&DataType) -> bool,
) -> Result<SchemaRef, dbconnection::Error> {
    let mut schema_builder = SchemaBuilder::new();
    for field in &schema.fields {
        let unsupported = is_unsupported_fn(field.data_type());

        if unsupported {
            handle_unsupported_data_type(
                &field.data_type().to_string(),
                field.name(),
                invalid_type_action,
            )?;
        } else {
            schema_builder.push(Arc::clone(field));
        }
    }

    Ok(Arc::new(schema_builder.finish()))
}
