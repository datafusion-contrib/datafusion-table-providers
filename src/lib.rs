use snafu::prelude::*;

pub mod sql;
pub mod util;

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flight")]
pub mod flight;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The database file path is not within the current directory: {path}"))]
    FileNotInDirectory { path: String },
    #[snafu(display("The database file is a symlink: {path}"))]
    FileIsSymlink { path: String },
    #[snafu(display("Error reading file: {source}"))]
    FileReadError { source: std::io::Error },
}
