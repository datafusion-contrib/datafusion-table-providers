pub mod delete;
pub mod sql;
pub mod util;

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
