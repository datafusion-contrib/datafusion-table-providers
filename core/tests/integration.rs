use rand::RngExt;

#[cfg(feature = "adbc")]
mod adbc;
mod arrow_record_batch_gen;
#[cfg(feature = "cassandra")]
mod cassandra;
#[cfg(feature = "clickhouse")]
mod clickhouse;
#[cfg(any(
    feature = "cassandra",
    feature = "clickhouse",
    feature = "mongodb",
    feature = "mysql",
    feature = "oracle",
    feature = "postgres"
))]
mod docker;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "flight")]
mod flight;
#[cfg(feature = "mongodb")]
mod mongodb;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "odbc")]
mod odbc;
#[cfg(feature = "oracle")]
mod oracle;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

fn get_random_port() -> u16 {
    rand::rng().random_range(15432..65535)
}
