use crate::sql::arrow_sql_gen::statement::{CreateTableBuilder, IndexBuilder, InsertBuilder};
use crate::sql::db_connection_pool::dbconnection::{self, get_schema};
use crate::sql::db_connection_pool::{
    self,
    dbconnection::{sqliteconn::SqliteConnection, DbConnection},
    sqlitepool::SqliteConnectionPool,
    DbConnectionPool, Mode,
};
use crate::sql::sql_provider_datafusion::{self, expr::Engine, SqlTable};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::Constraints,
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use rusqlite::{ToSql, Transaction};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio_rusqlite::Connection;

use crate::util::{
    self,
    column_reference::{self, ColumnReference},
    constraints::{self, get_primary_keys_from_constraints},
    indexes::IndexType,
    on_conflict::{self, OnConflict},
};

use self::write::SqliteTableWriter;

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
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqliteTableProviderFactory {
    db_path_param: String,
}

impl SqliteTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            db_path_param: "sqlite_file".to_string(),
        }
    }

    #[must_use]
    pub fn sqlite_file_path(&self, name: &str, options: &HashMap<String, String>) -> String {
        options
            .get(&self.db_path_param)
            .cloned()
            .unwrap_or_else(|| format!("{name}_sqlite.db"))
    }
}

impl Default for SqliteTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

type DynSqliteConnectionPool =
    dyn DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> + Send + Sync;

#[async_trait]
impl TableProviderFactory for SqliteTableProviderFactory {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
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

        let db_path = self.sqlite_file_path(&name, &cmd.options);

        // use a separate pool instance from writing to allow for concurrent reads+writes
        // even though we setup SQLite to use WAL mode, the pool isn't really a pool so shares the same connection
        // and we can't have concurrent writes when sharing the same connection
        let read_pool: Arc<SqliteConnectionPool> = Arc::new(
            SqliteConnectionPool::new(&db_path, mode.clone())
                .await
                .context(DbConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let pool: Arc<SqliteConnectionPool> = Arc::new(
            SqliteConnectionPool::new(&db_path, mode)
                .await
                .context(DbConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());
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
        }

        let dyn_pool: Arc<DynSqliteConnectionPool> = read_pool;

        let read_provider = Arc::new(SqlTable::new_with_schema(
            "sqlite",
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            Some(Engine::SQLite),
        ));

        let sqlite = Arc::into_inner(sqlite)
            .context(DanglingReferenceToSqliteSnafu)
            .map_err(to_datafusion_error)?;

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

        let read_provider = Arc::new(SqlTable::new_with_schema(
            "sqlite",
            &dyn_pool,
            Arc::clone(&schema),
            table_reference,
            Some(Engine::SQLite),
        ));

        Ok(read_provider)
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Clone)]
pub struct Sqlite {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<SqliteConnectionPool>,
    constraints: Constraints,
}

impl Sqlite {
    #[must_use]
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        pool: Arc<SqliteConnectionPool>,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            schema,
            pool,
            constraints,
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
            name = self.table_name
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
        let insert_table_builder = InsertBuilder::new(&self.table_name, vec![batch]);

        let sea_query_on_conflict =
            on_conflict.map(|oc| oc.build_sea_query_on_conflict(&self.schema));

        let sql = insert_table_builder
            .build_sqlite(sea_query_on_conflict)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

        transaction.execute(&sql, [])?;

        Ok(())
    }

    fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> rusqlite::Result<()> {
        transaction.execute(format!(r#"DELETE FROM "{}""#, self.table_name).as_str(), [])?;

        Ok(())
    }

    fn create_table(
        &self,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> rusqlite::Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(Arc::clone(&self.schema), &self.table_name)
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
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_sqlite();

        transaction.execute(&sql, [])?;

        Ok(())
    }
}
