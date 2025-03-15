use crate::sql::sql_provider_datafusion;
use crate::util::constraints::get_primary_keys_from_constraints;
use crate::util::{
    self,
    column_reference::{self, ColumnReference},
    constraints,
    indexes::IndexType,
    on_conflict::{self, OnConflict},
};
use crate::{
    sql::db_connection_pool::{
        self,
        dbconnection::{
            duckdbconn::{
                flatten_table_function_name, is_table_function, DuckDBParameter, DuckDbConnection,
            },
            get_schema, DbConnection,
        },
        duckdbpool::DuckDbConnectionPool,
        DbConnectionPool, DbInstanceKey, Mode,
    },
    UnsupportedTypeAction,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect};
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
use std::collections::HashSet;
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

    #[snafu(display("Unable to retrieve existing primary keys from DuckDB table: {source}"))]
    UnableToGetPrimaryKeysOnDuckDBTable { source: duckdb::Error },

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

    #[snafu(display("Unable to delete all data from the DuckDB table: {source}"))]
    UnableToDeleteAllTableData { source: duckdb::Error },

    #[snafu(display("Unable to insert data into the DuckDB table: {source}"))]
    UnableToInsertIntoTableAsync { source: duckdb::Error },

    #[snafu(display("The table '{table_name}' doesn't exist in the DuckDB server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },

    #[snafu(display(
        "Failed to create '{table_name}': creating a table with a schema is not supported"
    ))]
    TableWithSchemaCreationNotSupported { table_name: String },

    #[snafu(display("Failed to parse memory_limit value '{value}': {source}\nProvide a valid value, e.g. '2GB', '512MiB' (expected: KB, MB, GB, TB for 1000^i units or KiB, MiB, GiB, TiB for 1024^i units)"))]
    UnableToParseMemoryLimit {
        value: String,
        source: byte_unit::ParseError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBTableProviderFactory {
    access_mode: AccessMode,
    instances: Arc<Mutex<HashMap<DbInstanceKey, DuckDbConnectionPool>>>,
    unsupported_type_action: UnsupportedTypeAction,
    dialect: Arc<dyn Dialect>,
}

// Dialect trait does not implement Debug so we implement Debug manually
impl std::fmt::Debug for DuckDBTableProviderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTableProviderFactory")
            .field("access_mode", &self.access_mode)
            .field("instances", &self.instances)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .finish()
    }
}

const DUCKDB_DB_PATH_PARAM: &str = "open";
const DUCKDB_DB_BASE_FOLDER_PARAM: &str = "data_directory";
const DUCKDB_ATTACH_DATABASES_PARAM: &str = "attach_databases";
const DUCKDB_SETTING_MEMORY_LIMIT: &str = "memory_limit";

impl DuckDBTableProviderFactory {
    #[must_use]
    pub fn new(access_mode: AccessMode) -> Self {
        Self {
            access_mode,
            instances: Arc::new(Mutex::new(HashMap::new())),
            unsupported_type_action: UnsupportedTypeAction::Error,
            dialect: Arc::new(DuckDBDialect::new()),
        }
    }

    #[must_use]
    pub fn with_unsupported_type_action(
        mut self,
        unsupported_type_action: UnsupportedTypeAction,
    ) -> Self {
        self.unsupported_type_action = unsupported_type_action;
        self
    }

    #[must_use]
    pub fn with_dialect(mut self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        self.dialect = dialect;
        self
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

    /// Get the path to the DuckDB file database.
    ///
    /// ## Errors
    ///
    /// - If the path includes absolute sequences to escape the current directory, like `./`, `../`, or `/`.
    pub fn duckdb_file_path(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
    ) -> Result<String, Error> {
        let options = util::remove_prefix_from_hashmap_keys(options.clone(), "duckdb_");

        let db_base_folder = options
            .get(DUCKDB_DB_BASE_FOLDER_PARAM)
            .cloned()
            .unwrap_or(".".to_string()); // default to the current directory
        let default_filepath = &format!("{db_base_folder}/{name}.db");

        let filepath = options
            .get(DUCKDB_DB_PATH_PARAM)
            .unwrap_or(default_filepath);

        Ok(filepath.to_string())
    }

    pub async fn get_or_init_memory_instance(&self) -> Result<DuckDbConnectionPool> {
        let key = DbInstanceKey::memory();
        let mut instances = self.instances.lock().await;

        if let Some(instance) = instances.get(&key) {
            return Ok(instance.clone());
        }

        let pool = DuckDbConnectionPool::new_memory()
            .context(DbConnectionPoolSnafu)?
            .with_unsupported_type_action(self.unsupported_type_action);

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
            .context(DbConnectionPoolSnafu)?
            .with_unsupported_type_action(self.unsupported_type_action);

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
        if cmd.name.schema().is_some() {
            TableWithSchemaCreationNotSupportedSnafu {
                table_name: cmd.name.to_string(),
            }
            .fail()
            .map_err(to_datafusion_error)?;
        }

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
                let db_path = self
                    .duckdb_file_path(&name, &mut options)
                    .map_err(to_datafusion_error)?;

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

        // If the table is already created, we don't create it again and don't apply primary keys.
        // We need to verify that the table was created with the correct primary keys.
        if !duckdb
            .verify_primary_keys_match()
            .await
            .map_err(to_datafusion_error)?
        {
            tracing::warn!(
                "Schema mismatch detected:\nThe local table definition for '{table_name}' in database '{db_path}' does not match the expected configuration.\n\
                 To fix this, drop the existing local copy. A new table with the correct schema will be automatically created upon the next access.",
                db_path = duckdb.pool.db_path(),
                table_name = duckdb.table_name
            );
        }

        let dyn_pool: Arc<DynDuckDbConnectionPool> = Arc::new(read_pool);

        if let Some(memory_limit) = options.get("memory_limit") {
            apply_memory_limit(&dyn_pool, memory_limit).await?;
        }

        let read_provider = Arc::new(DuckDBTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            None,
            Some(self.dialect.clone()),
        ));

        #[cfg(feature = "duckdb-federation")]
        let read_provider: Arc<dyn TableProvider> =
            Arc::new(read_provider.create_federated_table_provider()?);

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

impl std::fmt::Debug for DuckDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDB")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
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

        appender.flush().context(UnableToInsertToDuckDBTableSnafu)?;

        Ok(())
    }

    fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> Result<()> {
        transaction
            .execute(format!(r#"DELETE FROM "{}""#, self.table_name).as_str(), [])
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }

    pub async fn verify_primary_keys_match(&self) -> Result<bool> {
        let expected_pk_keys_str_map: HashSet<String> =
            get_primary_keys_from_constraints(&self.constraints, &self.schema)
                .into_iter()
                .collect();

        let mut db_conn = self.connect_sync()?;

        let actual_pk_keys_str_map = TableCreator::get_existing_primary_keys(
            DuckDB::duckdb_conn(&mut db_conn)?,
            &self.table_name,
        )
        .await?;

        tracing::debug!(
            "Expected primary keys: {:?}\nActual primary keys: {:?}",
            expected_pk_keys_str_map,
            actual_pk_keys_str_map
        );

        let missing_in_actual = expected_pk_keys_str_map
            .difference(&actual_pk_keys_str_map)
            .collect::<Vec<_>>();
        let extra_in_actual = actual_pk_keys_str_map
            .difference(&expected_pk_keys_str_map)
            .collect::<Vec<_>>();

        if !missing_in_actual.is_empty() {
            tracing::warn!(
                "Missing primary key(s) detected for the table '{name}': {:?}.",
                missing_in_actual.iter().join(", "),
                name = self.table_name
            );
        }

        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "The table '{name}' has unexpected primary key(s) not defined in the configuration: {:?}.",
                extra_in_actual.iter().join(", "),
                name = self.table_name
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }
}

fn remove_option(options: &mut HashMap<String, String>, key: &str) -> Option<String> {
    options
        .remove(key)
        .or_else(|| options.remove(&format!("duckdb.{key}")))
}

pub struct DuckDBTableFactory {
    pool: Arc<DuckDbConnectionPool>,
    dialect: Arc<dyn Dialect>,
}

impl DuckDBTableFactory {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>) -> Self {
        Self {
            pool,
            dialect: Arc::new(DuckDBDialect::new()),
        }
    }

    #[must_use]
    pub fn with_dialect(mut self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        self.dialect = dialect;
        self
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
            &dyn_pool,
            schema,
            tbl_ref,
            cte,
            Some(self.dialect.clone()),
        ));

        #[cfg(feature = "duckdb-federation")]
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(table_provider.create_federated_table_provider()?);

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

async fn apply_memory_limit(
    pool: &Arc<DynDuckDbConnectionPool>,
    memory_limit: &str,
) -> DataFusionResult<()> {
    tracing::debug!("Setting DuckDB memory limit to {memory_limit}");

    if let Err(err) = byte_unit::Byte::parse_str(memory_limit, true) {
        return Err(to_datafusion_error(Error::UnableToParseMemoryLimit {
            value: memory_limit.to_string(),
            source: err,
        }));
    }

    let db_conn = pool.connect().await?;
    let Some(conn) = db_conn.as_sync() else {
        // should never happen
        return Err(to_datafusion_error(Error::DbConnectionError {
            source: "Failed to get sync DuckDbConnection using DbConnection".into(),
        }));
    };
    conn.execute(
        &format!("SET {DUCKDB_SETTING_MEMORY_LIMIT} = '{memory_limit}'"),
        &[],
    )?;
    Ok(())
}
