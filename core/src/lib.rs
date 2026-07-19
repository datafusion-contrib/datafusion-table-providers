#![cfg_attr(docsrs, feature(doc_auto_cfg))]

//! DataFusion Table Providers — integration facade.
//!
//! Prefer depending on individual provider crates (e.g.
//! `datafusion-table-providers-duckdb`) when you only need one backend.
//! This crate re-exports those providers behind the historical feature flags
//! and module paths for backward compatibility.

pub use datafusion_table_providers_common::{
    common, util, Error, UnsupportedTypeAction, DESCRIPTION_METADATA_KEY,
};

pub mod sql {
    pub mod arrow_sql_gen {
        pub use datafusion_table_providers_common::sql::arrow_sql_gen::{arrow, statement};

        #[cfg(feature = "mysql")]
        pub use datafusion_table_providers_mysql::arrow_sql_gen as mysql;
        #[cfg(feature = "postgres")]
        pub use datafusion_table_providers_postgres::arrow_sql_gen as postgres;
        #[cfg(feature = "sqlite")]
        pub use datafusion_table_providers_sqlite::arrow_sql_gen as sqlite;
    }

    pub mod sql_provider_datafusion {
        pub use datafusion_table_providers_common::sql::sql_provider_datafusion::*;
    }

    pub mod db_connection_pool {
        pub use datafusion_table_providers_common::sql::db_connection_pool::{
            DbConnectionPool, DbInstanceKey, JoinPushDown, Mode, PasswordProvider,
            StaticPasswordProvider,
        };
        pub use datafusion_table_providers_common::sql::db_connection_pool::runtime;

        #[cfg(feature = "adbc")]
        pub use datafusion_table_providers_adbc::pool as adbcpool;
        #[cfg(feature = "clickhouse")]
        pub use datafusion_table_providers_clickhouse::pool as clickhousepool;
        #[cfg(feature = "duckdb")]
        pub use datafusion_table_providers_duckdb::pool as duckdbpool;
        #[cfg(feature = "mysql")]
        pub use datafusion_table_providers_mysql::pool as mysqlpool;
        #[cfg(feature = "odbc")]
        pub use datafusion_table_providers_odbc::pool as odbcpool;
        #[cfg(feature = "postgres")]
        pub use datafusion_table_providers_postgres::pool as postgrespool;
        #[cfg(feature = "sqlite")]
        pub use datafusion_table_providers_sqlite::pool as sqlitepool;

        pub mod dbconnection {
            pub use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::*;

            #[cfg(feature = "adbc")]
            pub use datafusion_table_providers_adbc::conn as adbcconn;
            #[cfg(feature = "clickhouse")]
            pub use datafusion_table_providers_clickhouse::conn as clickhouseconn;
            #[cfg(feature = "duckdb")]
            pub use datafusion_table_providers_duckdb::conn as duckdbconn;
            #[cfg(feature = "mysql")]
            pub use datafusion_table_providers_mysql::conn as mysqlconn;
            #[cfg(feature = "odbc")]
            pub use datafusion_table_providers_odbc::conn as odbcconn;
            #[cfg(feature = "postgres")]
            pub use datafusion_table_providers_postgres::conn as postgresconn;
            #[cfg(feature = "sqlite")]
            pub use datafusion_table_providers_sqlite::conn as sqliteconn;
        }
    }
}

#[cfg(feature = "adbc")]
pub use datafusion_table_providers_adbc as adbc;
#[cfg(feature = "clickhouse")]
pub use datafusion_table_providers_clickhouse as clickhouse;
#[cfg(feature = "duckdb")]
pub use datafusion_table_providers_duckdb as duckdb;
#[cfg(feature = "flight")]
pub use datafusion_table_providers_flight as flight;
#[cfg(feature = "mongodb")]
pub use datafusion_table_providers_mongodb as mongodb;
#[cfg(feature = "mysql")]
pub use datafusion_table_providers_mysql as mysql;
#[cfg(feature = "odbc")]
pub use datafusion_table_providers_odbc as odbc;
#[cfg(feature = "postgres")]
pub use datafusion_table_providers_postgres as postgres;
#[cfg(feature = "sqlite")]
pub use datafusion_table_providers_sqlite as sqlite;
