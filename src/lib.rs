use serde::{Deserialize, Serialize};
use snafu::prelude::*;

pub mod sql;
pub mod util;

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flight")]
pub mod flight;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "trino")]
pub mod trino;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The database file path is not within the current directory: {path}"))]
    FileNotInDirectory { path: String },
    #[snafu(display("The database file is a symlink: {path}"))]
    FileIsSymlink { path: String },
    #[snafu(display("Error reading file: {source}"))]
    FileReadError { source: std::io::Error },
}

#[derive(PartialEq, Eq, Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedTypeAction {
    /// Refuse to create the table if any unsupported types are found
    #[default]
    Error,
    /// Log a warning for any unsupported types
    Warn,
    /// Ignore any unsupported types (i.e. skip them)
    Ignore,
    /// Attempt to convert any unsupported types to a string
    String,
}
