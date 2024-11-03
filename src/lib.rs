use snafu::prelude::*;
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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The database file path is not within the current directory: {path}"))]
    FileNotInDirectory { path: String },
    #[snafu(display("The database file is a symlink: {path}"))]
    FileIsSymlink { path: String },
    #[snafu(display("Error reading file: {source}"))]
    FileReadError { source: std::io::Error },
}

pub(crate) fn check_path_within_current_directory(filepath: &str) -> Result<String, Error> {
    let path = Path::new(filepath);
    if path.is_symlink() {
        return Err(Error::FileIsSymlink {
            path: filepath.to_string(),
        });
    }

    if std::path::absolute(path)
        .context(FileReadSnafu)?
        .starts_with(std::env::current_dir().context(FileReadSnafu)?)
    {
        Ok(filepath.to_string())
    } else {
        Err(Error::FileNotInDirectory {
            path: filepath.to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_check_path_within_current_directory() {
        let path = check_path_within_current_directory("src/lib.rs");
        assert!(path.is_ok());
    }

    #[test]
    fn test_check_path_outside_of_current_directory() {
        let path = check_path_within_current_directory("/etc/passwd");
        assert!(path.is_err());
    }

    #[test]
    fn test_check_nonexistant_path_in_directory() {
        let path = check_path_within_current_directory("src/notreal.rs");
        assert!(path.is_ok());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_check_path_is_symlink() {
        std::os::unix::fs::symlink("src/lib.rs", "src/lib_symlink.rs")
            .expect("symlink should be created");
        let path = check_path_within_current_directory("src/lib_symlink.rs");
        assert!(path.is_err());
        std::fs::remove_file("src/lib_symlink.rs").expect("symlink should be removed");
    }
}
