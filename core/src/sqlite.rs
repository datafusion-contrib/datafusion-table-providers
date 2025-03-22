use crate::sql::arrow_sql_gen::statement::{CreateTableBuilder, IndexBuilder, InsertBuilder};
use crate::sql::db_connection_pool::dbconnection::{self, get_schema, AsyncDbConnection};
use crate::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use crate::sql::db_connection_pool::DbInstanceKey;
use crate::sql::db_connection_pool::{
    self,
    dbconnection::{sqliteconn::SqliteConnection, DbConnection},
    sqlitepool::SqliteConnectionPool,
    DbConnectionPool, Mode,
};
use crate::sql::sql_provider_datafusion;
use crate::util::schema::SchemaValidator;
use crate::UnsupportedTypeAction;
use arrow::array::{Int64Array, StringArray};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::{
    catalog::TableProviderFactory,
    common::Constraints,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use futures::TryStreamExt;
use rusqlite::{ToSql, Transaction};
use snafu::prelude::*;
use sql_table::SQLiteTable;
use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_rusqlite::Connection;

use crate::util::{
    self,
    column_reference::{self, ColumnReference},
    constraints::{self, get_primary_keys_from_constraints},
    indexes::IndexType,
    on_conflict::{self, OnConflict},
};

use self::write::SqliteTableWriter;

#[cfg(feature = "sqlite-federation")]
pub mod federation;

#[cfg(feature = "sqlite-federation")]
pub mod sqlite_interval;

pub mod sql_table;
pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("Unable to downcast DbConnection to SqliteConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to construct SQLTable instance: {source}"))]
    UnableToConstuctSqlTableProvider {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to create table in Sqlite: {source}"))]
    UnableToCreateTable { source: tokio_rusqlite::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTable { source: rusqlite::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTableAsync { source: tokio_rusqlite::Error },

    #[snafu(display("Unable to insert data into the Sqlite table. The disk is full."))]
    DiskFull {},

    #[snafu(display("Unable to deleta all table data in Sqlite: {source}"))]
    UnableToDeleteAllTableData { source: rusqlite::Error },

    #[snafu(display("There is a dangling reference to the Sqlite struct in TableProviderFactory.create. This is a bug."))]
    DanglingReferenceToSqlite,

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },

    #[snafu(display("Unable to infer schema: {source}"))]
    UnableToInferSchema { source: dbconnection::Error },

    #[snafu(display("Invalid SQLite busy_timeout value"))]
    InvalidBusyTimeoutValue { value: String },

    #[snafu(display(
        "Unable to parse SQLite busy_timeout parameter, ensure it is a valid duration"
    ))]
    UnableToParseBusyTimeoutParameter { source: fundu::ParseError },

    #[snafu(display(
        "Failed to create '{table_name}': creating a table with a schema is not supported"
    ))]
    TableWithSchemaCreationNotSupported { table_name: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SqliteTableProviderFactory {
    instances: Arc<Mutex<HashMap<DbInstanceKey, SqliteConnectionPool>>>,
}

const SQLITE_DB_PATH_PARAM: &str = "file";
const SQLITE_DB_BASE_FOLDER_PARAM: &str = "data_directory";
const SQLITE_ATTACH_DATABASES_PARAM: &str = "attach_databases";
const SQLITE_BUSY_TIMEOUT_PARAM: &str = "busy_timeout";

impl SqliteTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            instances: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub fn attach_databases(&self, options: &HashMap<String, String>) -> Option<Vec<Arc<str>>> {
        options.get(SQLITE_ATTACH_DATABASES_PARAM).map(|databases| {
            databases
                .split(';')
                .map(Arc::from)
                .collect::<Vec<Arc<str>>>()
        })
    }

    /// Get the path to the SQLite file database.
    ///
    /// ## Errors
    ///
    /// - If the path includes absolute sequences to escape the current directory, like `./`, `../`, or `/`.
    pub fn sqlite_file_path(
        &self,
        name: &str,
        options: &HashMap<String, String>,
    ) -> Result<String, Error> {
        let options = util::remove_prefix_from_hashmap_keys(options.clone(), "sqlite_");

        let db_base_folder = options
            .get(SQLITE_DB_BASE_FOLDER_PARAM)
            .cloned()
            .unwrap_or(".".to_string()); // default to the current directory
        let default_filepath = &format!("{db_base_folder}/{name}_sqlite.db");

        let filepath = options
            .get(SQLITE_DB_PATH_PARAM)
            .unwrap_or(default_filepath);

        Ok(filepath.to_string())
    }

    pub fn sqlite_busy_timeout(&self, options: &HashMap<String, String>) -> Result<Duration> {
        let busy_timeout = options.get(SQLITE_BUSY_TIMEOUT_PARAM).cloned();
        match busy_timeout {
            Some(busy_timeout) => {
                let duration = fundu::parse_duration(&busy_timeout)
                    .context(UnableToParseBusyTimeoutParameterSnafu)?;
                Ok(duration)
            }
            None => Ok(Duration::from_millis(5000)),
        }
    }

    pub async fn get_or_init_instance(
        &self,
        db_path: impl Into<Arc<str>>,
        mode: Mode,
        busy_timeout: Duration,
    ) -> Result<SqliteConnectionPool> {
        let db_path = db_path.into();
        let key = match mode {
            Mode::Memory => DbInstanceKey::memory(),
            Mode::File => DbInstanceKey::file(Arc::clone(&db_path)),
        };
        let mut instances = self.instances.lock().await;

        if let Some(instance) = instances.get(&key) {
            return instance.try_clone().await.context(DbConnectionPoolSnafu);
        }

        let pool = SqliteConnectionPoolFactory::new(&db_path, mode, busy_timeout)
            .build()
            .await
            .context(DbConnectionPoolSnafu)?;

        instances.insert(key, pool.try_clone().await.context(DbConnectionPoolSnafu)?);

        Ok(pool)
    }
}

impl Default for SqliteTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

pub type DynSqliteConnectionPool =
    dyn DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> + Send + Sync;

#[async_trait]
impl TableProviderFactory for SqliteTableProviderFactory {
    #[allow(clippy::too_many_lines)]
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

        let name = cmd.name.clone();
        let mut options = cmd.options.clone();
        let mode = options.remove("mode").unwrap_or_default();
        let mode: Mode = mode.as_str().into();

        let indexes_option_str = options.remove("indexes");
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
        if let Some(on_conflict_str) = options.remove("on_conflict") {
            on_conflict = Some(
                OnConflict::try_from(on_conflict_str.as_str())
                    .context(UnableToParseOnConflictSnafu)
                    .map_err(to_datafusion_error)?,
            );
        }

        let busy_timeout = self
            .sqlite_busy_timeout(&cmd.options)
            .map_err(to_datafusion_error)?;
        let db_path: Arc<str> = self
            .sqlite_file_path(name.table(), &cmd.options)
            .map_err(to_datafusion_error)?
            .into();

        let pool: Arc<SqliteConnectionPool> = Arc::new(
            self.get_or_init_instance(Arc::clone(&db_path), mode, busy_timeout)
                .await
                .map_err(to_datafusion_error)?,
        );

        let read_pool = if mode == Mode::Memory {
            Arc::clone(&pool)
        } else {
            // use a separate pool instance from writing to allow for concurrent reads+writes
            // even though we setup SQLite to use WAL mode, the pool isn't really a pool so shares the same connection
            // and we can't have concurrent writes when sharing the same connection
            Arc::new(
                self.get_or_init_instance(Arc::clone(&db_path), mode, busy_timeout)
                    .await
                    .map_err(to_datafusion_error)?,
            )
        };

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());
        let schema: SchemaRef =
            SqliteConnection::handle_unsupported_schema(&schema, UnsupportedTypeAction::Error)
                .map_err(|e| DataFusionError::External(e.into()))?;

        let sqlite = Arc::new(Sqlite::new(
            name.clone(),
            Arc::clone(&schema),
            Arc::clone(&pool),
            cmd.constraints.clone(),
        ));

        let mut db_conn = sqlite.connect().await.map_err(to_datafusion_error)?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);

        let table_exists = sqlite.table_exists(sqlite_conn).await;
        if !table_exists {
            let sqlite_in_conn = Arc::clone(&sqlite);
            sqlite_conn
                .conn
                .call(move |conn| {
                    let transaction = conn.transaction()?;
                    sqlite_in_conn.create_table(&transaction, primary_keys)?;
                    for index in indexes {
                        sqlite_in_conn.create_index(
                            &transaction,
                            index.0.iter().collect(),
                            index.1 == IndexType::Unique,
                        )?;
                    }
                    transaction.commit()?;
                    Ok(())
                })
                .await
                .context(UnableToCreateTableSnafu)
                .map_err(to_datafusion_error)?;
        } else {
            let mut table_definition_matches = true;

            table_definition_matches &= sqlite.verify_indexes_match(sqlite_conn, &indexes).await?;
            table_definition_matches &= sqlite
                .verify_primary_keys_match(sqlite_conn, &primary_keys)
                .await?;

            if !table_definition_matches {
                tracing::warn!(
                "The local table definition at '{db_path}' for '{name}' does not match the expected configuration. To fix this, drop the existing local copy. A new table with the correct schema will be automatically created upon first access.",
                name = name
            );
            }
        }

        let dyn_pool: Arc<DynSqliteConnectionPool> = read_pool;

        let read_provider = Arc::new(SQLiteTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            name,
        ));

        let sqlite = Arc::into_inner(sqlite)
            .context(DanglingReferenceToSqliteSnafu)
            .map_err(to_datafusion_error)?;

        #[cfg(feature = "sqlite-federation")]
        let read_provider: Arc<dyn TableProvider> =
            Arc::new(read_provider.create_federated_table_provider()?);

        Ok(SqliteTableWriter::create(
            read_provider,
            sqlite,
            on_conflict,
        ))
    }
}

pub struct SqliteTableFactory {
    pool: Arc<SqliteConnectionPool>,
}

impl SqliteTableFactory {
    #[must_use]
    pub fn new(pool: Arc<SqliteConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);

        let conn = pool.connect().await.context(DbConnectionSnafu)?;
        let schema = get_schema(conn, &table_reference)
            .await
            .context(UnableToInferSchemaSnafu)?;

        let dyn_pool: Arc<DynSqliteConnectionPool> = pool;

        let read_provider = Arc::new(SQLiteTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            table_reference,
        ));

        Ok(read_provider)
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Clone)]
pub struct Sqlite {
    table: TableReference,
    schema: SchemaRef,
    pool: Arc<SqliteConnectionPool>,
    constraints: Constraints,
}

impl std::fmt::Debug for Sqlite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sqlite")
            .field("table_name", &self.table)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl Sqlite {
    #[must_use]
    pub fn new(
        table: TableReference,
        schema: SchemaRef,
        pool: Arc<SqliteConnectionPool>,
        constraints: Constraints,
    ) -> Self {
        Self {
            table,
            schema,
            pool,
            constraints,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table.table()
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>> {
        self.pool.connect().await.context(DbConnectionSnafu)
    }

    pub fn sqlite_conn<'a>(
        db_connection: &'a mut Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>,
    ) -> Result<&'a mut SqliteConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<SqliteConnection>()
            .ok_or_else(|| UnableToDowncastDbConnectionSnafu {}.build())
    }

    async fn table_exists(&self, sqlite_conn: &mut SqliteConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
          SELECT 1
          FROM sqlite_master
          WHERE type='table'
          AND name = '{name}'
        )"#,
            name = self.table
        );
        tracing::trace!("{sql}");

        sqlite_conn
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let exists = stmt.query_row([], |row| row.get(0))?;
                Ok(exists)
            })
            .await
            .unwrap_or(false)
    }

    fn insert_batch(
        &self,
        transaction: &Transaction<'_>,
        batch: RecordBatch,
        on_conflict: Option<&OnConflict>,
    ) -> rusqlite::Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.table, vec![batch]);

        let sea_query_on_conflict =
            on_conflict.map(|oc| oc.build_sea_query_on_conflict(&self.schema));

        let sql = insert_table_builder
            .build_sqlite(sea_query_on_conflict)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

        transaction.execute(&sql, [])?;

        Ok(())
    }

    fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> rusqlite::Result<()> {
        transaction.execute(
            format!(r#"DELETE FROM {}"#, self.table.to_quoted_string()).as_str(),
            [],
        )?;

        Ok(())
    }

    fn create_table(
        &self,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> rusqlite::Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(Arc::clone(&self.schema), self.table.table())
                .primary_keys(primary_keys);
        let sql = create_table_statement.build_sqlite();

        transaction.execute(&sql, [])?;

        Ok(())
    }

    fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> rusqlite::Result<()> {
        let mut index_builder = IndexBuilder::new(self.table.table(), columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_sqlite();

        transaction.execute(&sql, [])?;

        Ok(())
    }

    async fn get_indexes(
        &self,
        sqlite_conn: &mut SqliteConnection,
    ) -> DataFusionResult<HashSet<String>> {
        let query_result = sqlite_conn
            .query_arrow(
                format!("PRAGMA index_list({name})", name = self.table).as_str(),
                &[],
                None,
            )
            .await?;

        let mut indexes = HashSet::new();

        query_result
            .try_collect::<Vec<RecordBatch>>()
            .await
            .into_iter()
            .flatten()
            .for_each(|batch| {
                if let Some(name_array) = batch
                    .column_by_name("name")
                    .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                {
                    for index_name in name_array.iter().flatten() {
                        // Filter out SQLite's auto-generated indexes
                        if !index_name.starts_with("sqlite_autoindex_") {
                            indexes.insert(index_name.to_string());
                        }
                    }
                }
            });

        Ok(indexes)
    }

    async fn get_primary_keys(
        &self,
        sqlite_conn: &mut SqliteConnection,
    ) -> DataFusionResult<HashSet<String>> {
        let query_result = sqlite_conn
            .query_arrow(
                format!("PRAGMA table_info({name})", name = self.table).as_str(),
                &[],
                None,
            )
            .await?;

        let mut primary_keys = HashSet::new();

        query_result
            .try_collect::<Vec<RecordBatch>>()
            .await
            .into_iter()
            .flatten()
            .for_each(|batch| {
                if let (Some(name_array), Some(pk_array)) = (
                    batch
                        .column_by_name("name")
                        .and_then(|col| col.as_any().downcast_ref::<StringArray>()),
                    batch
                        .column_by_name("pk")
                        .and_then(|col| col.as_any().downcast_ref::<Int64Array>()),
                ) {
                    // name and pk fields can't be None so it is safe to flatten both
                    for (name, pk) in name_array.iter().flatten().zip(pk_array.iter().flatten()) {
                        if pk > 0 {
                            // pk > 0 indicates primary key
                            primary_keys.insert(name.to_string());
                        }
                    }
                }
            });

        Ok(primary_keys)
    }

    async fn verify_indexes_match(
        &self,
        sqlite_conn: &mut SqliteConnection,
        indexes: &[(ColumnReference, IndexType)],
    ) -> DataFusionResult<bool> {
        let expected_indexes_str_map: HashSet<String> = indexes
            .iter()
            .map(|(col, _)| {
                IndexBuilder::new(self.table.table(), col.iter().collect()).index_name()
            })
            .collect();

        let actual_indexes_str_map = self.get_indexes(sqlite_conn).await?;

        let missing_in_actual = expected_indexes_str_map
            .difference(&actual_indexes_str_map)
            .collect::<Vec<_>>();
        let extra_in_actual = actual_indexes_str_map
            .difference(&expected_indexes_str_map)
            .collect::<Vec<_>>();

        if !missing_in_actual.is_empty() {
            tracing::warn!(
                "Missing indexes detected for the table '{name}': {:?}.",
                missing_in_actual,
                name = self.table
            );
        }
        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "The table '{name}' contains unexpected indexes not presented in the configuration: {:?}.",
                extra_in_actual,
                name = self.table
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }

    async fn verify_primary_keys_match(
        &self,
        sqlite_conn: &mut SqliteConnection,
        primary_keys: &[String],
    ) -> DataFusionResult<bool> {
        let expected_pk_keys_str_map: HashSet<String> = primary_keys.iter().cloned().collect();

        let actual_pk_keys_str_map = self.get_primary_keys(sqlite_conn).await?;

        let missing_in_actual = expected_pk_keys_str_map
            .difference(&actual_pk_keys_str_map)
            .collect::<Vec<_>>();
        let extra_in_actual = actual_pk_keys_str_map
            .difference(&expected_pk_keys_str_map)
            .collect::<Vec<_>>();

        if !missing_in_actual.is_empty() {
            tracing::warn!(
                "Missing primary keys detected for the table '{name}': {:?}.",
                missing_in_actual,
                name = self.table
            );
        }
        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "The table '{name}' contains unexpected primary keys not presented in the configuration: {:?}.",
                extra_in_actual,
                name = self.table
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use arrow::datatypes::{DataType, Schema};
    use datafusion::{
        common::{Constraint, ToDFSchema},
        prelude::SessionContext,
    };

    use super::*;

    #[tokio::test]
    async fn test_sqlite_table_creation_with_indexes() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("first_name", DataType::Utf8, false),
            arrow::datatypes::Field::new("last_name", DataType::Utf8, false),
            arrow::datatypes::Field::new("id", DataType::Int64, false),
        ]));

        let options: HashMap<String, String> = [(
            "indexes".to_string(),
            "id:enabled;(first_name, last_name):unique".to_string(),
        )]
        .iter()
        .cloned()
        .collect();

        let expected_indexes: HashSet<String> = [
            "i_test_table_id".to_string(),
            "i_test_table_first_name_last_name".to_string(),
        ]
        .iter()
        .cloned()
        .collect();

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let primary_keys_constraints = {
            let schema = Arc::clone(&schema);
            let indices: Vec<usize> = ["id"]
                .iter()
                .filter_map(|&col_name| schema.column_with_name(col_name).map(|(index, _)| index))
                .collect();

            Constraints::new_unverified(vec![Constraint::PrimaryKey(indices)])
        };

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: primary_keys_constraints,
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        let sqlite = table
            .as_any()
            .downcast_ref::<SqliteTableWriter>()
            .expect("downcast to SqliteTableWriter")
            .sqlite();

        let mut db_conn = sqlite.connect().await.expect("should connect to db");
        let sqlite_conn =
            Sqlite::sqlite_conn(&mut db_conn).expect("should create sqlite connection");

        let retrieved_indexes = sqlite
            .get_indexes(sqlite_conn)
            .await
            .expect("should get indexes");

        assert_eq!(retrieved_indexes, expected_indexes);

        let retrieved_primary_keys = sqlite
            .get_primary_keys(sqlite_conn)
            .await
            .expect("should get primary keys");

        assert_eq!(
            retrieved_primary_keys,
            vec!["id".to_string()]
                .into_iter()
                .collect::<HashSet<String>>()
        );
    }
}
