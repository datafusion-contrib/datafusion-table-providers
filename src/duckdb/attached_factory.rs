use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPoolBuilder;
use crate::{
    duckdb::{
        make_initial_table, to_datafusion_error, write::DuckDBTableWriterBuilder, DuckDBTable,
        RelationName, TableDefinition,
    },
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::{
    catalog::Session,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use snafu::prelude::*;
use std::{path::PathBuf, sync::Arc};

use super::DynDuckDbConnectionPool;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create DuckDB table: {source}"))]
    UnableToCreateTable { source: crate::duckdb::Error },

    #[snafu(display(
        "The table '{table_name}' must have a LOCATION set to the path of a DuckDB database file."
    ))]
    MissingLocation { table_name: String },
}

/// A `TableProviderFactory` that creates DuckDB tables by attaching database files
/// to a single, in-memory master DuckDB instance.
///
/// This factory maintains a single in-memory DuckDB database. When a new external
/// table is created using this factory, it `ATTACH`es the DuckDB database file
/// specified in the `LOCATION` clause to the in-memory instance.
///
/// The attached database is mounted as a schema, where the schema name is derived
/// from the filename of the attached database. For example, if you attach a database
/// at `/path/to/my_data.db`, you can query tables within that database using
/// `my_data.table_name`.
#[derive(Debug)]
pub struct AttachedDuckDBTableProviderFactory {
    pool: Arc<DuckDbConnectionPool>,
}

impl AttachedDuckDBTableProviderFactory {
    /// Creates a new `AttachedDuckDBTableProviderFactory`.
    ///
    /// This initializes a new in-memory DuckDB instance that will be used to
    /// attach all subsequent database files.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory DuckDB connection pool cannot be created.
    pub fn new() -> Result<Self, crate::duckdb::Error> {
        let pool = DuckDbConnectionPoolBuilder::memory()
            .build()
            .context(super::DbConnectionPoolSnafu)?;
        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

#[async_trait]
impl TableProviderFactory for AttachedDuckDBTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let location = if cmd.location.is_empty() {
            return Err(DataFusionError::External(Box::new(
                Error::MissingLocation {
                    table_name: cmd.name.to_string(),
                },
            )));
        } else {
            cmd.location.clone()
        };

        // Attach the database file to the in-memory instance.
        self.pool
            .attach_file(&location)
            .map_err(DataFusionError::External)?;

        // The name of the attached database (schema in DuckDB) is the file stem.
        let db_name = PathBuf::from(&location)
            .file_stem()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to derive database name from location: {location}",
                ))
            })?
            .to_string_lossy()
            .to_string();

        let table_ref_for_duckdb = TableReference::partial(db_name, cmd.name.table());

        let schema = Arc::new(cmd.schema.as_ref().into());

        let table_definition = TableDefinition::new(
            RelationName::from(table_ref_for_duckdb.clone()),
            Arc::clone(&schema),
        )
        .with_constraints(cmd.constraints.clone());

        eprintln!("{}", table_definition.name());

        make_initial_table(Arc::new(table_definition.clone()), &self.pool)?;

        let pool = Arc::clone(&self.pool);
        let pool: Arc<DynDuckDbConnectionPool> = pool;

        let read_provider = Arc::new(DuckDBTable::new_with_schema(
            &pool,
            Arc::clone(&schema),
            table_ref_for_duckdb,
            None,
            None,
        ));

        let table_writer_builder = DuckDBTableWriterBuilder::new()
            .with_read_provider(read_provider)
            .with_pool(Arc::clone(&self.pool))
            .with_table_definition(table_definition);

        Ok(Arc::new(
            table_writer_builder.build().map_err(to_datafusion_error)?,
        ))
    }
}
