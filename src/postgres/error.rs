use datafusion::error::DataFusionError;
use snafu::prelude::*;

use crate::{
    sql::{
        arrow_sql_gen::statement::Error as SqlGenError,
        db_connection_pool::{self, postgrespool},
    },
    util::{column_reference, constraints, on_conflict},
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: postgrespool::Error },

    #[snafu(display("Unable to downcast DbConnection to PostgresConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to begin Postgres transaction: {source}"))]
    UnableToBeginTransaction {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create the Postgres table: {source}"))]
    UnableToCreatePostgresTable {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create an index for the Postgres table: {source}"))]
    UnableToCreateIndexForPostgresTable {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to commit the Postgres transaction: {source}"))]
    UnableToCommitPostgresTransaction {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: DataFusionError },

    #[snafu(display("Unable to delete all data from the Postgres table: {source}"))]
    UnableToDeleteAllTableData {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to delete data from the Postgres table: {source}"))]
    UnableToDeleteData {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to insert Arrow batch to Postgres table: {source}"))]
    UnableToInsertArrowBatch {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create insertion statement for Postgres table: {source}"))]
    UnableToCreateInsertStatement { source: SqlGenError },

    #[snafu(display("The table '{table_name}' doesn't exist in the Postgres server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },
}

pub(super) fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
