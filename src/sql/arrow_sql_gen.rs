//! # Generate SQL statements from Arrow schemas
//!
//! This module provides a set of functions to generate SQL statements (i.e. DML) from Arrow schemas.
//!
//! The primary use case is for generating `CREATE TABLE`/`INSERT INTO` statements for SQL databases from Arrow schemas/values with given constraints.
//!
//! ## Example
//!
//! ### `CREATE TABLE` statement
//! ```rust
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow_sql_gen::statement::CreateTableBuilder;
//!
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("name", DataType::Utf8, false),
//!     Field::new("age", DataType::Int32, false),
//! ]);
//!
//! let sql = CreateTableBuilder::new(&schema, "my_table").build();
//!
//! assert_eq!("CREATE TABLE my_table (id INT32 NOT NULL, name UTF8 NOT NULL, age INT32 NOT NULL)", sql);
//! ```
//!
//! With primary key constraints:
//! ```rust
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow_sql_gen::statement::CreateTableBuilder;
//!
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("name", DataType::Utf8, false),
//!     Field::new("age", DataType::Int32, false),
//! ]);
//!
//! let sql = CreateTableBuilder::new(&schema, "my_table")
//!     .primary_keys(vec!["id"])
//!     .build();
//!
//! assert_eq!("CREATE TABLE my_table (id INT32 NOT NULL, name UTF8 NOT NULL, age INT32 NOT NULL, PRIMARY KEY (id))", sql);
//! ```

pub mod arrow;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod statement;
