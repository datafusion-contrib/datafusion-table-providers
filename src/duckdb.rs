use crate::sql::db_connection_pool::{
    self,
    dbconnection::{
        duckdbconn::{
            flatten_table_function_name, is_table_function, DuckDBParameter, DuckDbConnection,
        },
        get_schema, DbConnection,
    },
    duckdbpool::DuckDbConnectionPool,
    DbConnectionPool, DbInstanceKey, Mode,
};
use crate::sql::sql_provider_datafusion;
use crate::util::{
    self,
    column_reference::{self, ColumnReference},
    constraints,
    indexes::IndexType,
    on_conflict::{self, OnConflict},
};
use async_trait::async_trait;
use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::{Session, TableProviderFactory},
    common::Constraints,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use duckdb::{AccessMode, DuckdbConnectionManager, Transaction};
use itertools::Itertools;
use snafu::prelude::*;
use std::{cmp, collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use self::{creator::TableCreator, sql_table::DuckDBTable, write::DuckDBTableWriter};

#[cfg(feature = "duckdb-federation")]
mod federation;

mod creator;
mod sql_table;
pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to downcast DbConnection to DuckDbConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to drop duckdb table: {source}"))]
    UnableToDropDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to create duckdb table: {source}"))]
    UnableToCreateDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to create index on duckdb table: {source}"))]
    UnableToCreateIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to drop index on duckdb table: {source}"))]
    UnableToDropIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to rename duckdb table: {source}"))]
    UnableToRenameDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to insert into duckdb table: {source}"))]
    UnableToInsertToDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to get appender to duckdb table: {source}"))]
    UnableToGetAppenderToDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to delete data from the duckdb table: {source}"))]
    UnableToDeleteDuckdbData { source: duckdb::Error },

    #[snafu(display("Unable to query data from the duckdb table: {source}"))]
    UnableToQueryData { source: duckdb::Error },

    #[snafu(display("Unable to commit transaction: {source}"))]
    UnableToCommitTransaction { source: duckdb::Error },

    #[snafu(display("Unable to begin duckdb transaction: {source}"))]
    UnableToBeginTransaction { source: duckdb::Error },

    #[snafu(display("Unable to rollback transaction: {source}"))]
    UnableToRollbackTransaction { source: duckdb::Error },

    #[snafu(display("Unable to delete all data from the Postgres table: {source}"))]
    UnableToDeleteAllTableData { source: duckdb::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTableAsync { source: duckdb::Error },

    #[snafu(display("The table '{table_name}' doesn't exist in the DuckDB server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct DuckDBTableProviderFactory {
    access_mode: AccessMode,
    instances: Arc<Mutex<HashMap<DbInstanceKey, DuckDbConnectionPool>>>,
}

const DUCKDB_DB_PATH_PARAM: &str = "open";
const DUCKDB_DB_BASE_FOLDER_PARAM: &str = "data_directory";
const DUCKDB_ATTACH_DATABASES_PARAM: &str = "attach_databases";

impl DuckDBTableProviderFactory {
    #[must_use]
    pub fn new(access_mode: AccessMode) -> Self {
        Self {
            access_mode,
            instances: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub fn attach_databases(&self, options: &HashMap<String, String>) -> Vec<Arc<str>> {
        options
            .get(DUCKDB_ATTACH_DATABASES_PARAM)
            .map(|attach_databases| {
                attach_databases
                    .split(';')
                    .map(Arc::from)
                    .collect::<Vec<Arc<str>>>()
            })
            .unwrap_or_default()
    }

    #[must_use]
    pub fn duckdb_file_path(&self, name: &str, options: &mut HashMap<String, String>) -> String {
        let options = util::remove_prefix_from_hashmap_keys(options.clone(), "duckdb_");

        let db_base_folder = options
            .get(DUCKDB_DB_BASE_FOLDER_PARAM)
            .cloned()
            .unwrap_or(".".to_string()); // default to the current directory
        let default_filepath = format!("{db_base_folder}/{name}.db");

        options
            .get(DUCKDB_DB_PATH_PARAM)
            .cloned()
            .unwrap_or(default_filepath)
    }

    pub async fn get_or_init_memory_instance(&self) -> Result<DuckDbConnectionPool> {
        let key = DbInstanceKey::memory();
        let mut instances = self.instances.lock().await;

        if let Some(instance) = instances.get(&key) {
            return Ok(instance.clone());
        }

        let pool = DuckDbConnectionPool::new_memory().context(DbConnectionPoolSnafu)?;

        instances.insert(key, pool.clone());

        Ok(pool)
    }

    pub async fn get_or_init_file_instance(
        &self,
        db_path: impl Into<Arc<str>>,
    ) -> Result<DuckDbConnectionPool> {
        let db_path = db_path.into();
        let key = DbInstanceKey::file(Arc::clone(&db_path));
        let mut instances = self.instances.lock().await;

        if let Some(instance) = instances.get(&key) {
            return Ok(instance.clone());
        }

        let pool = DuckDbConnectionPool::new_file(&db_path, &self.access_mode)
            .context(DbConnectionPoolSnafu)?;

        instances.insert(key, pool.clone());

        Ok(pool)
    }
}

type DynDuckDbConnectionPool = dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    + Send
    + Sync;

#[async_trait]
impl TableProviderFactory for DuckDBTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let mode = remove_option(&mut options, "mode").unwrap_or_default();
        let mode: Mode = mode.as_str().into();

        let indexes_option_str = remove_option(&mut options, "indexes");
        let unparsed_indexes: HashMap<String, IndexType> = match indexes_option_str {
            Some(indexes_str) => util::hashmap_from_option_string(&indexes_str),
            None => HashMap::new(),
        };

        let unparsed_indexes = unparsed_indexes
            .into_iter()
            .map(|(key, value)| {
                let columns = ColumnReference::try_from(key.as_str())
                    .context(UnableToParseColumnReferenceSnafu)
                    .map_err(to_datafusion_error);
                (columns, value)
            })
            .collect::<Vec<(Result<ColumnReference, DataFusionError>, IndexType)>>();

        let mut indexes: Vec<(ColumnReference, IndexType)> = Vec::new();
        for (columns, index_type) in unparsed_indexes {
            let columns = columns?;
            indexes.push((columns, index_type));
        }

        let mut on_conflict: Option<OnConflict> = None;
        if let Some(on_conflict_str) = remove_option(&mut options, "on_conflict") {
            on_conflict = Some(
                OnConflict::try_from(on_conflict_str.as_str())
                    .context(UnableToParseOnConflictSnafu)
                    .map_err(to_datafusion_error)?,
            );
        }

        let pool: DuckDbConnectionPool = match &mode {
            Mode::File => {
                // open duckdb at given path or create a new one
                let db_path = self.duckdb_file_path(&name, &mut options);

                self.get_or_init_file_instance(db_path)
                    .await
                    .map_err(to_datafusion_error)?
            }
            Mode::Memory => self
                .get_or_init_memory_instance()
                .await
                .map_err(to_datafusion_error)?,
        };

        let read_pool = match &mode {
            Mode::File => {
                let read_pool = pool.clone();

                read_pool.set_attached_databases(&self.attach_databases(&options))
            }
            Mode::Memory => pool.clone(),
        };

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());

        let duckdb = TableCreator::new(name.clone(), Arc::clone(&schema), Arc::new(pool))
            .constraints(cmd.constraints.clone())
            .indexes(indexes)
            .create()
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynDuckDbConnectionPool> = Arc::new(read_pool);

        let read_provider = Arc::new(DuckDBTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            None,
        ));

        #[cfg(feature = "duckdb-federation")]
        let read_provider: Arc<dyn TableProvider> = if mode == Mode::File {
            // federation is disabled for in-memory mode until memory connections are updated to use the same database instance instead of separate instances
            Arc::new(read_provider.create_federated_table_provider()?)
        } else {
            read_provider
        };

        Ok(DuckDBTableWriter::create(
            read_provider,
            duckdb,
            on_conflict,
        ))
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub struct DuckDB {
    table_name: String,
    pool: Arc<DuckDbConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
    table_creator: Option<TableCreator>,
}

impl DuckDB {
    #[must_use]
    pub fn existing_table(
        table_name: String,
        pool: Arc<DuckDbConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            pool,
            schema,
            constraints,
            table_creator: None,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub fn connect_sync(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        Arc::clone(&self.pool)
            .connect_sync()
            .context(DbConnectionSnafu)
    }

    pub fn duckdb_conn(
        db_connection: &mut Box<
            dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>,
        >,
    ) -> Result<&mut DuckDbConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
    }

    const MAX_BATCH_SIZE: usize = 2048;

    fn split_batch(batch: &RecordBatch) -> Vec<RecordBatch> {
        let mut result = vec![];
        (0..=batch.num_rows())
            .step_by(Self::MAX_BATCH_SIZE)
            .for_each(|offset| {
                let length = cmp::min(Self::MAX_BATCH_SIZE, batch.num_rows() - offset);
                result.push(batch.slice(offset, length));
            });
        result
    }

    fn insert_table_into(
        &self,
        tx: &Transaction<'_>,
        table_to_insert_into: &DuckDB,
        on_conflict: Option<&OnConflict>,
    ) -> Result<()> {
        let mut insert_sql = format!(
            r#"INSERT INTO "{}" SELECT * FROM "{}""#,
            table_to_insert_into.table_name, self.table_name
        );

        if let Some(on_conflict) = on_conflict {
            let on_conflict_sql = on_conflict.build_on_conflict_statement(&self.schema);
            insert_sql.push_str(&format!(" {on_conflict_sql}"));
        }
        tracing::debug!("{insert_sql}");

        tx.execute(&insert_sql, [])
            .context(UnableToInsertToDuckDBTableSnafu)?;

        Ok(())
    }

    fn insert_batch_no_constraints(
        &self,
        transaction: &Transaction<'_>,
        batch: &RecordBatch,
    ) -> Result<()> {
        let mut appender = transaction
            .appender(&self.table_name)
            .context(UnableToGetAppenderToDuckDBTableSnafu)?;

        for batch in Self::split_batch(batch) {
            appender
                .append_record_batch(batch.clone())
                .context(UnableToInsertToDuckDBTableSnafu)?;
        }

        Ok(())
    }

    fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> Result<()> {
        transaction
            .execute(format!(r#"DELETE FROM "{}""#, self.table_name).as_str(), [])
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }
}

fn remove_option(options: &mut HashMap<String, String>, key: &str) -> Option<String> {
    options
        .remove(key)
        .or_else(|| options.remove(&format!("duckdb.{key}")))
}

pub struct DuckDBTableFactory {
    pool: Arc<DuckDbConnectionPool>,
}

impl DuckDBTableFactory {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let conn = Arc::clone(&pool).connect().await?;
        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

        let schema = get_schema(conn, &table_reference).await?;
        let (tbl_ref, cte) = if is_table_function(&table_reference) {
            let tbl_ref_view = create_table_function_view_name(&table_reference);
            (
                tbl_ref_view.clone(),
                Some(HashMap::from_iter(vec![(
                    tbl_ref_view.to_string(),
                    table_reference.table().to_string(),
                )])),
            )
        } else {
            (table_reference.clone(), None)
        };

        let table_provider = Arc::new(DuckDBTable::new_with_schema(
            &dyn_pool, schema, tbl_ref, cte,
        ));

        #[cfg(feature = "duckdb-federation")]
        let table_provider: Arc<dyn TableProvider> = if self.pool.mode() == Mode::File {
            // federation is disabled for in-memory mode until memory connections are updated to use the same database instance instead of separate instances
            Arc::new(table_provider.create_federated_table_provider()?)
        } else {
            table_provider
        };

        Ok(table_provider)
    }

    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Self::table_provider(self, table_reference.clone()).await?;
        let schema = read_provider.schema();

        let table_name = table_reference.to_string();
        let duckdb = DuckDB::existing_table(
            table_name,
            Arc::clone(&self.pool),
            schema,
            Constraints::empty(),
        );

        Ok(DuckDBTableWriter::create(read_provider, duckdb, None))
    }
}

/// For a [`TableReference`] that is a table function, create a name for a view on the original [`TableReference`]
///
/// ### Example
///
/// ```rust,ignore
/// use datafusion_table_providers::duckdb::create_table_function_view_name;
/// use datafusion::common::TableReference;
///
/// let table_reference = TableReference::from("catalog.schema.read_parquet('cleaned_sales_data.parquet')");
/// let view_name = create_table_function_view_name(&table_reference);
/// assert_eq!(view_name.to_string(), "catalog.schema.read_parquet_cleaned_sales_dataparquet__view");
/// ```
fn create_table_function_view_name(table_reference: &TableReference) -> TableReference {
    let tbl_ref_view = [
        table_reference.catalog(),
        table_reference.schema(),
        Some(&flatten_table_function_name(table_reference)),
    ]
    .iter()
    .flatten()
    .join(".");
    TableReference::from(&tbl_ref_view)
}
