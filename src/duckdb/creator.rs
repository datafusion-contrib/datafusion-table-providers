use crate::sql::arrow_sql_gen::statement::IndexBuilder;
use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::util::on_conflict::OnConflict;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::common::utils::quote_identifier;
use datafusion::common::Constraints;
use datafusion::sql::TableReference;
use duckdb::{vtab::arrow_recordbatch_to_query_params, ToSql, Transaction};
use snafu::prelude::*;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

use super::DuckDB;
use crate::util::{
    column_reference::ColumnReference, constraints::get_primary_keys_from_constraints,
    indexes::IndexType,
};

#[derive(Debug, Clone)]
pub struct TableDefinition {
    name: TableName,
    schema: SchemaRef,
    constraints: Option<Constraints>,
    indexes: Vec<(ColumnReference, IndexType)>,
}

#[derive(Clone)]
pub(crate) struct TableCreator {
    table_definition: Arc<TableDefinition>,
    internal_name: TableName,
    is_internal: bool,
}

impl TableDefinition {
    #[must_use]
    pub(crate) fn new(name: TableName, schema: SchemaRef) -> Self {
        Self {
            name,
            schema,
            constraints: None,
            indexes: Vec::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    #[must_use]
    pub(crate) fn with_indexes(mut self, indexes: Vec<(ColumnReference, IndexType)>) -> Self {
        self.indexes = indexes;
        self
    }

    pub(crate) fn generate_internal_name(&self) -> super::Result<TableName> {
        let unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context(super::UnableToGetSystemTimeSnafu)?
            .as_millis();
        Ok(TableName(format!(
            "__data_{table_name}_{unix_ms}",
            table_name = self.name,
        )))
    }

    pub(crate) fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    pub fn list_internal_tables(
        &self,
        tx: &Transaction<'_>,
    ) -> super::Result<Vec<(TableName, u64)>> {
        // list all related internal tables, based on the table definition name
        let sql = format!(
            "select table_name from duckdb_tables() where table_name LIKE '__data_{table_name}%'",
            table_name = self.name
        );
        let mut stmt = tx.prepare(&sql).context(super::UnableToQueryDataSnafu)?;
        let mut rows = stmt.query([]).context(super::UnableToQueryDataSnafu)?;

        let mut table_names = Vec::new();
        while let Some(row) = rows.next().context(super::UnableToQueryDataSnafu)? {
            let table_name = row
                .get::<usize, String>(0)
                .context(super::UnableToQueryDataSnafu)?;
            let Some(timestamp) = table_name.split('_').last() else {
                continue; // skip invalid table names
            };

            let timestamp = timestamp
                .parse::<u64>()
                .context(super::UnableToParseSystemTimeSnafu)?;

            table_names.push((table_name, timestamp));
        }

        table_names.sort_by(|a, b| a.1.cmp(&b.1));

        Ok(table_names
            .into_iter()
            .map(|(name, time_created)| (TableName(name), time_created))
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableName(String);

impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TableName {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl From<TableReference> for TableName {
    fn from(table_ref: TableReference) -> Self {
        TableName(table_ref.to_string())
    }
}

impl TableCreator {
    pub(crate) fn new(table_definition: Arc<TableDefinition>) -> super::Result<Self> {
        let internal_name: TableName = table_definition.generate_internal_name()?;
        Ok(Self {
            table_definition,
            internal_name,
            is_internal: false,
        })
    }

    pub(crate) fn with_internal(mut self, is_internal: bool) -> Self {
        self.is_internal = is_internal;
        self
    }

    pub(crate) fn definition_name(&self) -> &TableName {
        &self.table_definition.name
    }

    pub(crate) fn table_name(&self) -> &TableName {
        if self.is_internal {
            &self.internal_name
        } else {
            &self.table_definition.name
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn base_table(&self, tx: &Transaction<'_>) -> super::Result<Option<Self>> {
        let mut stmt = tx
            .prepare("SELECT 1 FROM duckdb_tables() WHERE table_name = ?")
            .context(super::UnableToQueryDataSnafu)?;
        let mut rows = stmt
            .query([self.definition_name().to_string()])
            .context(super::UnableToQueryDataSnafu)?;

        if rows
            .next()
            .context(super::UnableToQueryDataSnafu)?
            .is_some()
        {
            let mut base_table = self.clone();
            base_table.is_internal = false;
            Ok(Some(base_table))
        } else {
            Ok(None)
        }
    }

    fn indexes_vec(&self) -> Vec<(Vec<&str>, IndexType)> {
        self.table_definition
            .indexes
            .iter()
            .map(|(key, ty)| (key.iter().collect(), *ty))
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn create_table(
        &self,
        pool: Arc<DuckDbConnectionPool>,
        tx: &Transaction<'_>,
    ) -> super::Result<()> {
        let mut db_conn = pool.connect_sync().context(super::DbConnectionPoolSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        // create the table with the supplied table name, or a generated internal name
        let mut create_stmt = self.get_table_create_statement(duckdb_conn)?;
        tracing::debug!("{create_stmt}");

        let primary_keys = if let Some(constraints) = &self.table_definition.constraints {
            get_primary_keys_from_constraints(constraints, &self.table_definition.schema)
        } else {
            Vec::new()
        };

        if !primary_keys.is_empty() && !create_stmt.contains("PRIMARY KEY") {
            let primary_key_clause = format!(", PRIMARY KEY ({}));", primary_keys.join(", "));
            create_stmt = create_stmt.replace(");", &primary_key_clause);
        }

        tx.execute(&create_stmt, [])
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn delete_table(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop indexes first
        self.drop_indexes(tx)?;
        self.drop_table(tx)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_table(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop this table
        tx.execute(
            &format!(r#"DROP TABLE IF EXISTS "{}""#, self.table_name()),
            [],
        )
        .context(super::UnableToDropDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn insert_into(
        &self,
        table: &TableCreator,
        tx: &Transaction<'_>,
        on_conflict: Option<&OnConflict>,
    ) -> super::Result<()> {
        // insert from this table, into the target table
        let mut insert_sql = format!(
            r#"INSERT INTO "{}" SELECT * FROM "{}""#,
            table.table_name(),
            self.table_name()
        );

        if let Some(on_conflict) = on_conflict {
            let on_conflict_sql =
                on_conflict.build_on_conflict_statement(&self.table_definition.schema);
            insert_sql.push_str(&format!(" {on_conflict_sql}"));
        }
        tracing::debug!("{insert_sql}");

        tx.execute(&insert_sql, [])
            .context(super::UnableToInsertToDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn create_index(
        &self,
        tx: &Transaction<'_>,
        index: (Vec<&str>, IndexType),
    ) -> super::Result<()> {
        let table_name = self.table_name();

        let unique = index.1 == IndexType::Unique;
        let columns = index.0;
        let mut index_builder = IndexBuilder::new(&table_name.to_string(), columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();
        tracing::debug!("Creating index: {sql}");

        tx.execute(&sql, [])
            .context(super::UnableToCreateIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn create_indexes(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // create indexes on this table
        for index in self.indexes_vec() {
            self.create_index(tx, index)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_index(&self, tx: &Transaction<'_>, index: (Vec<&str>, IndexType)) -> super::Result<()> {
        let table_name = self.table_name();
        let columns = index.0;
        let index_name = IndexBuilder::new(&table_name.to_string(), columns).index_name();

        let sql = format!(r#"DROP INDEX IF EXISTS "{index_name}""#);
        tracing::debug!("{sql}");

        tx.execute(&sql, [])
            .context(super::UnableToDropIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    pub(crate) fn drop_indexes(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop indexes on this table
        for index in self.indexes_vec() {
            self.drop_index(tx, index)?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn get_table_create_statement(
        &self,
        duckdb_conn: &mut DuckDbConnection,
    ) -> super::Result<String> {
        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;
        let table_name = self.table_name();
        let empty_record = RecordBatch::new_empty(Arc::clone(&self.table_definition.schema));

        let arrow_params = arrow_recordbatch_to_query_params(empty_record);
        let arrow_params_vec: Vec<&dyn ToSql> = arrow_params
            .iter()
            .map(|p| p as &dyn ToSql)
            .collect::<Vec<_>>();
        let arrow_params_ref: &[&dyn ToSql] = &arrow_params_vec;
        let sql =
            format!(r#"CREATE TABLE IF NOT EXISTS "{table_name}" AS SELECT * FROM arrow(?, ?)"#,);
        tracing::debug!("{sql}");

        tx.execute(&sql, arrow_params_ref)
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        let create_stmt = tx
            .query_row(
                &format!("select sql from duckdb_tables() where table_name = '{table_name}'",),
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
    pub(crate) fn list_internal_tables(
        &self,
        tx: &Transaction<'_>,
    ) -> super::Result<Vec<(Self, u64)>> {
        let tables = self.table_definition.list_internal_tables(tx)?;

        Ok(tables
            .into_iter()
            .filter_map(|(name, time_created)| {
                if name == self.internal_name {
                    return None;
                }

                let internal_table = TableCreator {
                    table_definition: Arc::clone(&self.table_definition),
                    internal_name: name,
                    is_internal: true,
                };
                Some((internal_table, time_created))
            })
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn create_view(&self, tx: &Transaction<'_>) -> super::Result<()> {
        if !self.is_internal {
            return Ok(());
        }

        tx.execute(
            &format!(
                "CREATE OR REPLACE VIEW {base_table} AS SELECT * FROM {internal_table}",
                base_table = quote_identifier(&self.definition_name().to_string()),
                internal_table = quote_identifier(&self.table_name().to_string())
            ),
            [],
        )
        .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn current_primary_keys(
        &self,
        tx: &Transaction<'_>,
    ) -> super::Result<HashSet<String>> {
        // DuckDB provides convenient queryable 'pragma_table_info' table function
        // Complex table name with schema as part of the name must be quoted as
        // '"<name>"', otherwise it will be parsed to schema and table name
        let sql = format!(
            "SELECT name FROM pragma_table_info('{table_name}') WHERE pk = true",
            table_name = quote_identifier(&self.table_name().to_string())
        );
        tracing::debug!("{sql}");

        let mut stmt = tx
            .prepare(&sql)
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let primary_keys_iter = stmt
            .query_map([], |row| row.get::<usize, String>(0))
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let mut primary_keys = HashSet::new();
        for pk in primary_keys_iter {
            primary_keys.insert(pk.context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?);
        }

        Ok(primary_keys)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn current_indexes(&self, tx: &Transaction<'_>) -> super::Result<HashSet<String>> {
        let sql = format!(
            "SELECT index_name FROM duckdb_indexes WHERE table_name = '{table_name}'",
            table_name = &self.table_name().to_string()
        );

        tracing::debug!("{sql}");

        let mut stmt = tx
            .prepare(&sql)
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let indexes_iter = stmt
            .query_map([], |row| row.get::<usize, String>(0))
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let mut indexes = HashSet::new();
        for index in indexes_iter {
            indexes.insert(index.context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?);
        }

        Ok(indexes)
    }

    #[cfg(test)]
    fn from_table_name(table_definition: Arc<TableDefinition>, table_name: TableName) -> Self {
        Self {
            table_definition,
            internal_name: table_name,
            is_internal: true,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::sql::db_connection_pool::{
        dbconnection::duckdbconn::DuckDbConnection, duckdbpool::DuckDbConnectionPool,
    };
    use arrow::array::RecordBatch;
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
            .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
            .expect("to get records")
    }

    fn get_stream_from_batches(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(MemoryStream::try_new(batches, schema, None).expect("to get stream"))
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

    #[tokio::test]
    async fn test_table_creator() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();

        let table_definition = Arc::new(
            TableDefinition::new(TableName::new("eth.logs"), Arc::clone(&schema))
                .with_constraints(get_unique_constraints(
                    &["log_index", "transaction_hash"],
                    Arc::clone(&schema),
                ))
                .with_indexes(vec![
                    (
                        ColumnReference::try_from("block_number").expect("valid column ref"),
                        IndexType::Enabled,
                    ),
                    (
                        ColumnReference::try_from("(log_index, transaction_hash)")
                            .expect("valid column ref"),
                        IndexType::Unique,
                    ),
                ]),
        );

        for overwrite in &[InsertOp::Append, InsertOp::Overwrite] {
            let pool = get_mem_duckdb();

            let duckdb_sink = DuckDBDataSink::new(
                Arc::clone(&pool),
                Arc::clone(&table_definition),
                *overwrite,
                None,
            );
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

            let tx = conn
                .get_underlying_conn_mut()
                .transaction()
                .expect("should begin transaction");
            let internal_tables: Vec<(TableName, u64)> = table_definition
                .list_internal_tables(&tx)
                .expect("should list internal tables");
            assert_eq!(internal_tables.len(), 1);

            let internal_table = internal_tables.first().expect("to get internal table");
            let internal_table = internal_table.0.clone();
            let internal_table_creator = TableCreator::from_table_name(
                Arc::clone(&table_definition),
                internal_table.clone(),
            );

            let primary_keys = internal_table_creator
                .current_primary_keys(&tx)
                .expect("should get primary keys");

            assert_eq!(primary_keys, HashSet::<String>::new());

            let created_indexes_str_map = internal_table_creator
                .current_indexes(&tx)
                .expect("should get indexes");

            assert_eq!(
                created_indexes_str_map,
                vec![
                    format!("i_{internal_table}_block_number"),
                    format!("i_{internal_table}_log_index_transaction_hash")
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
                "Indexes must match"
            );

            tx.rollback().expect("should rollback transaction");
        }
    }

    #[tokio::test]
    async fn test_table_creator_primary_key() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();
        let table_definition = Arc::new(
            TableDefinition::new(TableName::new("eth.logs"), Arc::clone(&schema))
                .with_constraints(get_pk_constraints(
                    &["log_index", "transaction_hash"],
                    Arc::clone(&schema),
                ))
                .with_indexes(
                    vec![(
                        ColumnReference::try_from("block_number").expect("valid column ref"),
                        IndexType::Enabled,
                    )]
                    .into_iter()
                    .collect(),
                ),
        );

        for overwrite in &[InsertOp::Append, InsertOp::Overwrite] {
            let pool = get_mem_duckdb();
            let duckdb_sink = DuckDBDataSink::new(
                Arc::clone(&pool),
                Arc::clone(&table_definition),
                *overwrite,
                None,
            );
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

            let tx = conn
                .get_underlying_conn_mut()
                .transaction()
                .expect("should begin transaction");

            let internal_tables = table_definition
                .list_internal_tables(&tx)
                .expect("should list internal tables");
            assert_eq!(internal_tables.len(), 1);

            let internal_table = internal_tables.first().expect("should get internal table");
            let internal_table = internal_table.0.clone();
            let internal_table_creator = TableCreator::from_table_name(
                Arc::clone(&table_definition),
                internal_table.clone(),
            );

            let create_stmt = tx
                .query_row(
                    "select sql from duckdb_tables() where table_name = ?",
                    [internal_table.to_string()],
                    |r| r.get::<usize, String>(0),
                )
                .expect("to get create table statement");

            assert_eq!(
                create_stmt,
                format!(
                    r#"CREATE TABLE "{internal_table}"(log_index BIGINT, transaction_hash VARCHAR, transaction_index BIGINT, address VARCHAR, "data" VARCHAR, topics VARCHAR[], block_timestamp BIGINT, block_hash VARCHAR, block_number BIGINT, PRIMARY KEY(log_index, transaction_hash));"#,
                )
            );

            let primary_keys = internal_table_creator
                .current_primary_keys(&tx)
                .expect("should get primary keys");

            assert_eq!(
                primary_keys,
                vec!["log_index".to_string(), "transaction_hash".to_string()]
                    .into_iter()
                    .collect::<HashSet<_>>()
            );

            let created_indexes_str_map = internal_table_creator
                .current_indexes(&tx)
                .expect("should get indexes");

            assert_eq!(
                created_indexes_str_map,
                vec![format!("i_{internal_table}_block_number")]
                    .into_iter()
                    .collect::<HashSet<_>>(),
                "Indexes must match"
            );

            tx.rollback().expect("should rollback transaction");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_related_tables_from_definition() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_related_tables_from_creator() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_view() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_tables_ddl() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_into_tables() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_indexes() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_indexes_ddl() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn test_lists_base_table_from_definition() {
        todo!();
    }
}
