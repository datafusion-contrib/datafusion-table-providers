use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::{
    duckdb::UnableToGetPrimaryKeysOnDuckDBTableSnafu, sql::arrow_sql_gen::statement::IndexBuilder,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::common::utils::quote_identifier;
use datafusion::common::Constraints;
use duckdb::{vtab::arrow_recordbatch_to_query_params, ToSql, Transaction};
use snafu::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use super::DuckDB;
use crate::util::{
    column_reference::ColumnReference, constraints::get_primary_keys_from_constraints,
    indexes::IndexType,
};

/// Responsible for creating a `DuckDB` table along with any constraints and indexes
pub(crate) struct TableCreator {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<DuckDbConnectionPool>,
    constraints: Option<Constraints>,
    indexes: Vec<(ColumnReference, IndexType)>,
    created: bool,
}

impl TableCreator {
    pub fn new(table_name: String, schema: SchemaRef, pool: Arc<DuckDbConnectionPool>) -> Self {
        Self {
            table_name,
            schema,
            pool,
            constraints: None,
            indexes: Vec::new(),
            created: false,
        }
    }

    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    pub fn indexes(mut self, indexes: Vec<(ColumnReference, IndexType)>) -> Self {
        self.indexes = indexes;
        self
    }

    fn indexes_vec(&self) -> Vec<(Vec<&str>, IndexType)> {
        self.indexes
            .iter()
            .map(|(key, ty)| (key.iter().collect(), *ty))
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_with_tx(mut self, tx: &Transaction<'_>) -> super::Result<DuckDB> {
        assert!(!self.created, "Table already created");
        let primary_keys = if let Some(constraints) = &self.constraints {
            get_primary_keys_from_constraints(constraints, &self.schema)
        } else {
            Vec::new()
        };

        self.create_table(tx, primary_keys)?;

        for index in self.indexes_vec() {
            self.create_index(tx, index.0, index.1 == IndexType::Unique)?;
        }

        let constraints = self.constraints.clone().unwrap_or(Constraints::empty());

        let mut duckdb = DuckDB::existing_table(
            self.table_name.clone(),
            Arc::clone(&self.pool),
            Arc::clone(&self.schema),
            constraints,
        );

        self.created = true;

        duckdb.table_creator = Some(self);

        Ok(duckdb)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create(self) -> super::Result<DuckDB> {
        assert!(!self.created, "Table already created");

        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .context(super::DbConnectionSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;

        let duckdb = self.create_with_tx(&tx)?;

        tx.commit().context(super::UnableToCommitTransactionSnafu)?;

        Ok(duckdb)
    }

    /// Creates a copy of the `DuckDB` table with the same schema and constraints
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_empty_clone(&self, tx: &Transaction<'_>) -> super::Result<DuckDB> {
        assert!(self.created, "Table must be created before cloning");

        let new_table_name = format!(
            "{}_spice_{}",
            self.table_name,
            &Uuid::new_v4().to_string()[..8]
        );
        tracing::debug!(
            "Creating empty table {} from {}",
            new_table_name,
            self.table_name,
        );

        let new_table_creator = TableCreator {
            table_name: new_table_name.clone(),
            schema: Arc::clone(&self.schema),
            pool: Arc::clone(&self.pool),
            constraints: self.constraints.clone(),
            indexes: self.indexes.clone(),
            created: false,
        };

        new_table_creator.create_with_tx(tx)
    }

    #[tracing::instrument(level = "debug", skip(conn))]
    pub async fn get_existing_primary_keys(
        conn: &mut DuckDbConnection,
        table_name: &str,
    ) -> super::Result<HashSet<String>> {
        // DuckDB provides convinient querable 'pragma_table_info' table function
        // Complex table name with schema as part of the name must be quoted as
        // '"<name>"', otherwise it will be parsed to schema and table name
        let sql = format!(
            r#"SELECT name FROM pragma_table_info('{table_name}') WHERE pk = true"#,
            table_name = quote_identifier(table_name)
        );
        tracing::debug!("{sql}");

        let mut stmt = conn
            .conn
            .prepare(&sql)
            .context(UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let primary_keys_iter = stmt
            .query_map([], |row| row.get::<usize, String>(0))
            .context(UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let mut primary_keys = HashSet::new();
        for pk in primary_keys_iter {
            primary_keys.insert(pk.context(UnableToGetPrimaryKeysOnDuckDBTableSnafu)?);
        }

        Ok(primary_keys)
    }

    #[tracing::instrument(level = "trace")]
    pub fn get_index_name(table_name: &str, index: &(ColumnReference, IndexType)) -> String {
        let mut index_builder = IndexBuilder::new(table_name, index.0.iter().collect());
        if matches!(index.1, IndexType::Unique) {
            index_builder = index_builder.unique();
        }
        index_builder.index_name()
    }

    #[tracing::instrument(level = "debug", skip(conn))]
    pub async fn get_existing_indexes(
        conn: &mut DuckDbConnection,
        table_name: &str,
    ) -> super::Result<HashSet<String>> {
        let sql = format!(
            r#"SELECT index_name FROM duckdb_indexes WHERE table_name = '{table_name}'"#,
            table_name = table_name
        );

        tracing::debug!("{sql}");

        let mut stmt = conn
            .conn
            .prepare(&sql)
            .context(UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let indexes_iter = stmt
            .query_map([], |row| row.get::<usize, String>(0))
            .context(UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let mut indexes = HashSet::new();
        for pk in indexes_iter {
            indexes.insert(pk.context(UnableToGetPrimaryKeysOnDuckDBTableSnafu)?);
        }

        Ok(indexes)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn delete_table(self, tx: &Transaction<'_>) -> super::Result<()> {
        assert!(self.created, "Table must be created before deleting");
        for index in self.indexes_vec() {
            self.drop_index(tx, index.0)?;
        }
        self.drop_table(tx)?;

        Ok(())
    }

    /// Consumes the current table and replaces `table_to_replace` with the current table's contents.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn replace_table(
        mut self,
        tx: &Transaction<'_>,
        table_to_replace: &TableCreator,
    ) -> super::Result<()> {
        assert!(
            self.created,
            "Table must be created before replacing another table"
        );

        // Drop indexes and table for the table we want to replace
        for index in table_to_replace.indexes_vec() {
            table_to_replace.drop_index(tx, index.0)?;
        }
        // Drop the old table with the name we want to claim
        table_to_replace.drop_table(tx)?;

        // DuckDB doesn't support renaming tables with existing indexes, so first drop them, rename the table and recreate them.
        for index in self.indexes_vec() {
            self.drop_index(tx, index.0)?;
        }
        // Rename our table to the target table name
        self.rename_table(tx, table_to_replace.table_name.as_str())?;
        // Update our table name to the target table name so the indexes are created correctly
        self.table_name.clone_from(&table_to_replace.table_name);
        // Recreate the indexes, now for our newly renamed table.
        for index in self.indexes_vec() {
            self.create_index(tx, index.0, index.1 == IndexType::Unique)?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn create_table(
        &self,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> super::Result<()> {
        let mut sql = self.get_table_create_statement()?;

        if !primary_keys.is_empty() && !sql.contains("PRIMARY KEY") {
            let primary_key_clause = format!(", PRIMARY KEY ({}));", primary_keys.join(", "));
            sql = sql.replace(");", &primary_key_clause);
        }
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    /// DuckDB CREATE TABLE statements aren't supported by sea-query - so we create a temporary table
    /// from an Arrow schema and ask DuckDB for the CREATE TABLE statement.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_table_create_statement(&self) -> super::Result<String> {
        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .context(super::DbConnectionSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;

        let empty_record = RecordBatch::new_empty(Arc::clone(&self.schema));

        let arrow_params = arrow_recordbatch_to_query_params(empty_record);
        let arrow_params_vec: Vec<&dyn ToSql> = arrow_params
            .iter()
            .map(|p| p as &dyn ToSql)
            .collect::<Vec<_>>();
        let arrow_params_ref: &[&dyn ToSql] = &arrow_params_vec;
        let sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{name}" AS SELECT * FROM arrow(?, ?)"#,
            name = self.table_name
        );
        tracing::debug!("{sql}");

        tx.execute(&sql, arrow_params_ref)
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        let create_stmt = tx
            .query_row(
                &format!(
                    "select sql from duckdb_tables() where table_name = '{}'",
                    self.table_name
                ),
                [],
                |r| r.get::<usize, String>(0),
            )
            .context(super::UnableToQueryDataSnafu)?;

        // DuckDB doesn't add IF NOT EXISTS to CREATE TABLE statements, so we add it here.
        let create_stmt = create_stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

        tx.rollback()
            .context(super::UnableToRollbackTransactionSnafu)?;

        Ok(create_stmt)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_table(&self, transaction: &Transaction<'_>) -> super::Result<()> {
        let sql = format!(r#"DROP TABLE IF EXISTS "{}""#, self.table_name);
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToDropDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn rename_table(
        &self,
        transaction: &Transaction<'_>,
        new_table_name: &str,
    ) -> super::Result<()> {
        let sql = format!(
            r#"ALTER TABLE "{}" RENAME TO "{new_table_name}""#,
            self.table_name
        );
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToRenameDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> super::Result<()> {
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToCreateIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn drop_index(&self, transaction: &Transaction<'_>, columns: Vec<&str>) -> super::Result<()> {
        let index_name = IndexBuilder::new(&self.table_name, columns).index_name();

        let sql = format!(r#"DROP INDEX IF EXISTS "{index_name}""#);
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToDropIndexOnDuckDBTableSnafu)?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::sql::db_connection_pool::{
        dbconnection::duckdbconn::DuckDbConnection, duckdbpool::DuckDbConnectionPool,
    };
    use datafusion::arrow::array::RecordBatch;
    use datafusion::{
        execution::{SendableRecordBatchStream, TaskContext},
        logical_expr::dml::InsertOp,
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
        physical_plan::{insert::DataSink, memory::MemoryStream},
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::{
        duckdb::write::DuckDBDataSink,
        util::constraints::tests::{get_pk_constraints, get_unique_constraints},
    };

    use super::*;

    fn get_mem_duckdb() -> Arc<DuckDbConnectionPool> {
        Arc::new(
            DuckDbConnectionPool::new_memory().expect("to get a memory duckdb connection pool"),
        )
    }

    async fn get_logs_batches() -> Vec<RecordBatch> {
        let parquet_bytes = reqwest::get("https://public-data.spiceai.org/eth.recent_logs.parquet")
            .await
            .expect("to get parquet file")
            .bytes()
            .await
            .expect("to get parquet bytes");

        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)
            .expect("to get parquet reader builder")
            .build()
            .expect("to build parquet reader");

        parquet_reader
            .collect::<Result<Vec<_>, datafusion::arrow::error::ArrowError>>()
            .expect("to get records")
    }

    fn get_stream_from_batches(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(MemoryStream::try_new(batches, schema, None).expect("to get stream"))
    }

    #[tokio::test]
    async fn test_table_creator() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();

        for overwrite in &[InsertOp::Append, InsertOp::Overwrite] {
            let pool = get_mem_duckdb();
            let constraints =
                get_unique_constraints(&["log_index", "transaction_hash"], Arc::clone(&schema));
            let created_table = TableCreator::new(
                "eth.logs".to_string(),
                Arc::clone(&schema),
                Arc::clone(&pool),
            )
            .constraints(constraints)
            .indexes(
                vec![
                    (
                        ColumnReference::try_from("block_number").expect("valid column ref"),
                        IndexType::Enabled,
                    ),
                    (
                        ColumnReference::try_from("(log_index, transaction_hash)")
                            .expect("valid column ref"),
                        IndexType::Unique,
                    ),
                ]
                .into_iter()
                .collect(),
            )
            .create()
            .expect("to create table");

            let arc_created_table = Arc::new(created_table);

            let duckdb_sink =
                DuckDBDataSink::new(arc_created_table, *overwrite, None, Arc::clone(&schema));
            let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);
            let rows_written = data_sink
                .write_all(
                    get_stream_from_batches(batches.clone()),
                    &Arc::new(TaskContext::default()),
                )
                .await
                .expect("to write all");

            let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
            let conn = pool_conn
                .as_any_mut()
                .downcast_mut::<DuckDbConnection>()
                .expect("to downcast to duckdb connection");
            let num_rows = conn
                .get_underlying_conn_mut()
                .query_row(r#"SELECT COUNT(1) FROM "eth.logs""#, [], |r| {
                    r.get::<usize, u64>(0)
                })
                .expect("to get count");

            assert_eq!(num_rows, rows_written);

            let primary_keys = TableCreator::get_existing_primary_keys(conn, "eth.logs")
                .await
                .expect("to get primary keys");

            assert_eq!(primary_keys, HashSet::<String>::new());

            let created_indexes_str_map = TableCreator::get_existing_indexes(conn, "eth.logs")
                .await
                .expect("to get indexes");

            assert_eq!(
                created_indexes_str_map,
                vec![
                    "i_eth.logs_block_number".to_string(),
                    "i_eth.logs_log_index_transaction_hash".to_string()
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
                "Indexes must match"
            );
        }
    }

    #[tokio::test]
    async fn test_table_creator_primary_key() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();

        for overwrite in &[InsertOp::Append, InsertOp::Overwrite] {
            let pool = get_mem_duckdb();
            let constraints =
                get_pk_constraints(&["log_index", "transaction_hash"], Arc::clone(&schema));
            let created_table = TableCreator::new(
                "eth.logs".to_string(),
                Arc::clone(&schema),
                Arc::clone(&pool),
            )
            .constraints(constraints)
            .indexes(
                vec![(
                    ColumnReference::try_from("block_number").expect("valid column ref"),
                    IndexType::Enabled,
                )]
                .into_iter()
                .collect(),
            )
            .create()
            .expect("to create table");

            let arc_created_table = Arc::new(created_table);

            let duckdb_sink =
                DuckDBDataSink::new(arc_created_table, *overwrite, None, Arc::clone(&schema));
            let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);
            let rows_written = data_sink
                .write_all(
                    get_stream_from_batches(batches.clone()),
                    &Arc::new(TaskContext::default()),
                )
                .await
                .expect("to write all");

            let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
            let conn = pool_conn
                .as_any_mut()
                .downcast_mut::<DuckDbConnection>()
                .expect("to downcast to duckdb connection");
            let num_rows = conn
                .get_underlying_conn_mut()
                .query_row(r#"SELECT COUNT(1) FROM "eth.logs""#, [], |r| {
                    r.get::<usize, u64>(0)
                })
                .expect("to get count");

            assert_eq!(num_rows, rows_written);

            let create_stmt = conn
                .get_underlying_conn_mut()
                .query_row(
                    "select sql from duckdb_tables() where table_name = 'eth.logs'",
                    [],
                    |r| r.get::<usize, String>(0),
                )
                .expect("to get create table statement");

            assert_eq!(
                create_stmt,
                r#"CREATE TABLE "eth.logs"(log_index BIGINT, transaction_hash VARCHAR, transaction_index BIGINT, address VARCHAR, "data" VARCHAR, topics VARCHAR[], block_timestamp BIGINT, block_hash VARCHAR, block_number BIGINT, PRIMARY KEY(log_index, transaction_hash));"#
            );

            let primary_keys = TableCreator::get_existing_primary_keys(conn, "eth.logs")
                .await
                .expect("to get primary keys");

            assert_eq!(
                primary_keys,
                vec!["log_index".to_string(), "transaction_hash".to_string()]
                    .into_iter()
                    .collect::<HashSet<_>>()
            );

            let created_indexes_str_map = TableCreator::get_existing_indexes(conn, "eth.logs")
                .await
                .expect("to get indexes");

            assert_eq!(
                created_indexes_str_map,
                vec!["i_eth.logs_block_number".to_string()]
                    .into_iter()
                    .collect::<HashSet<_>>(),
                "Indexes must match"
            );
        }
    }

    pub(crate) fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
        let filter = match default_level {
            Some(level) => EnvFilter::new(level),
            _ => EnvFilter::new("INFO,datafusion_table_providers=TRACE"),
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_ansi(true)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }
}
