use std::path::Path;

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

pub(crate) fn check_path_within_current_directory(
    filepath: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let path = Path::new(filepath);
    if path.canonicalize()?.starts_with(std::env::current_dir()?) {
        Ok(filepath.to_string())
    } else {
        Err("Path must be absolute or relative to the current directory".into())
    }
}
