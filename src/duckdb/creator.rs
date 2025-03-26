use crate::sql::arrow_sql_gen::statement::IndexBuilder;
use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::util::on_conflict::OnConflict;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::common::utils::quote_identifier;
use datafusion::common::Constraints;
use datafusion::sql::TableReference;
use duckdb::{vtab::arrow_recordbatch_to_query_params, ToSql, Transaction};
use itertools::Itertools;
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
#[cfg_attr(test, derive(PartialEq))]
pub struct TableDefinition {
    name: TableName,
    schema: SchemaRef,
    constraints: Option<Constraints>,
    indexes: Vec<(ColumnReference, IndexType)>,
}

#[derive(Debug, Clone)]
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

    #[cfg(test)]
    pub(crate) fn name(&self) -> &TableName {
        &self.name
    }

    #[cfg(test)]
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

    fn get_index_name(table_name: &TableName, index: &(Vec<&str>, IndexType)) -> String {
        let index_builder = IndexBuilder::new(&table_name.to_string(), index.0.clone());
        index_builder.index_name()
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
        let index_name = TableCreator::get_index_name(table_name, &index);

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

    pub(crate) fn verify_primary_keys_match(
        &self,
        other_table: &TableCreator,
        tx: &Transaction<'_>,
    ) -> super::Result<bool> {
        let expected_pk_keys_str_map =
            if let Some(constraints) = self.table_definition.constraints.as_ref() {
                get_primary_keys_from_constraints(constraints, &self.table_definition.schema)
                    .into_iter()
                    .collect()
            } else {
                HashSet::new()
            };

        let actual_pk_keys_str_map = other_table.current_primary_keys(tx)?;

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
                name = self.table_name()
            );
        }

        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "The table '{name}' has unexpected primary key(s) not defined in the configuration: {:?}.",
                extra_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }

    pub(crate) fn verify_indexes_match(
        &self,
        other_table: &TableCreator,
        tx: &Transaction<'_>,
    ) -> super::Result<bool> {
        let expected_indexes_str_map: HashSet<String> = self
            .indexes_vec()
            .iter()
            .map(|index| TableCreator::get_index_name(self.table_name(), index))
            .collect();

        let actual_indexes_str_map = other_table.current_indexes(tx)?;

        // replace table names for each index with nothing, as table names could be internal and have unique timestamps
        let expected_indexes_str_map = expected_indexes_str_map
            .iter()
            .map(|index| index.replace(&self.table_name().to_string(), ""))
            .collect::<HashSet<_>>();

        let actual_indexes_str_map = actual_indexes_str_map
            .iter()
            .map(|index| index.replace(&other_table.table_name().to_string(), ""))
            .collect::<HashSet<_>>();

        tracing::debug!(
            "Expected indexes: {:?}\nActual indexes: {:?}",
            expected_indexes_str_map,
            actual_indexes_str_map
        );

        let missing_in_actual = expected_indexes_str_map
            .difference(&actual_indexes_str_map)
            .collect::<Vec<_>>();
        let extra_in_actual = actual_indexes_str_map
            .difference(&expected_indexes_str_map)
            .collect::<Vec<_>>();

        if !missing_in_actual.is_empty() {
            tracing::warn!(
                "Missing index(es) detected for the table '{name}': {:?}.",
                missing_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }
        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "Unexpected index(es) detected in table '{name}': {}.\nThese indexes are not defined in the configuration.",
                extra_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }

    pub(crate) fn current_schema(&self, tx: &Transaction<'_>) -> super::Result<SchemaRef> {
        let sql = format!(
            "SELECT * FROM {table_name} LIMIT 0",
            table_name = self.table_name()
        );
        let mut stmt = tx.prepare(&sql).context(super::UnableToQueryDataSnafu)?;
        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .context(super::UnableToQueryDataSnafu)?;
        Ok(result.get_schema())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::sql::db_connection_pool::{
        dbconnection::duckdbconn::DuckDbConnection, duckdbpool::DuckDbConnectionPool,
    };
    use arrow::array::RecordBatch;
    use datafusion::{
        common::SchemaExt,
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

    pub(crate) fn get_mem_duckdb() -> Arc<DuckDbConnectionPool> {
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

    pub(crate) fn get_basic_table_definition() -> Arc<TableDefinition> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        Arc::new(TableDefinition::new(TableName::new("test_table"), schema))
    }

    #[tokio::test]
    async fn test_list_related_tables_from_definition() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 3 internal tables
        for _ in 0..3 {
            TableCreator::new(Arc::clone(&table_definition))
                .expect("to create table creator")
                .with_internal(true)
                .create_table(Arc::clone(&pool), &tx)
                .expect("to create table");
        }

        // using the table definition, list the names of the internal tables
        let table_name = table_definition.name.clone();
        let internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(internal_tables.len(), 3);

        // validate the first table is the oldest, and the last table is the newest
        let first_table = internal_tables.first().expect("to get first table");
        let last_table = internal_tables.last().expect("to get last table");
        assert!(first_table.1 < last_table.1);

        // validate none of the internal tables are the same, they are not equal to the base table
        let mut seen_tables = vec![];
        for (internal_table, _) in internal_tables {
            let internal_name = internal_table.clone();
            assert_ne!(&internal_name, &table_name);
            assert!(!seen_tables.contains(&internal_name));
            seen_tables.push(internal_name);
        }

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_list_related_tables_from_creator() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 3 internal tables
        for _ in 0..3 {
            TableCreator::new(Arc::clone(&table_definition))
                .expect("to create table creator")
                .with_internal(true)
                .create_table(Arc::clone(&pool), &tx)
                .expect("to create table");
        }

        // instantiate a new table creator, make it, and list the internal tables
        let table_creator = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let internal_tables = table_creator
            .list_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(internal_tables.len(), 3);

        // validate none of the internal tables are the same, they are not equal to the base table, and they are not equal to the internal table that listed them
        let mut seen_tables = vec![];
        for (internal_table, _) in &internal_tables {
            let table_name = internal_table.table_name().clone();
            assert_ne!(&table_name, table_creator.definition_name());
            assert_ne!(&table_name, &table_creator.internal_name);
            assert!(!seen_tables.contains(&table_name));
            seen_tables.push(table_name);
        }

        // drop the internal tables except the last one
        for (internal_table, _) in internal_tables {
            internal_table.delete_table(&tx).expect("to delete table");
        }

        // list the internal tables again
        let internal_tables = table_creator
            .list_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(internal_tables.len(), 0);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_create_view() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make a table
        let table_creator = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // create a view from the internal table
        table_creator.create_view(&tx).expect("to create view");

        // check if the view exists
        let view_exists = tx
            .query_row(
                "from duckdb_views() select 1 where view_name = ? and not internal",
                [table_creator.definition_name().to_string()],
                |r| r.get::<usize, i32>(0),
            )
            .expect("to get view");

        assert_eq!(view_exists, 1);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_insert_into_tables() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make a base table
        let base_table = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(false);

        base_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // make an internal table
        let internal_table = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        internal_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // insert some rows directly into the base table
        let insert_stmt = format!(
            r#"INSERT INTO "{base_table}" VALUES (1, 'test'), (2, 'test2')"#,
            base_table = base_table.table_name()
        );

        tx.execute(&insert_stmt, [])
            .expect("to insert into base table");

        // insert from the base table into the internal table
        base_table
            .insert_into(&internal_table, &tx, None)
            .expect("to insert into internal table");

        // check if the rows were inserted
        let rows = tx
            .query_row(
                &format!(
                    r#"SELECT COUNT(1) FROM "{internal_table}""#,
                    internal_table = internal_table.table_name()
                ),
                [],
                |r| r.get::<usize, u64>(0),
            )
            .expect("to get count");

        assert_eq!(rows, 2);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_lists_base_table_from_definition() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make a base table
        let table_creator = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(false);

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // list the base table from another base table
        let internal_table = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(false);

        let base_table = internal_table.base_table(&tx).expect("to get base table");

        assert!(base_table.is_some());
        assert_eq!(
            base_table.expect("to be some").table_definition,
            table_creator.table_definition
        );

        // list the base table from an internal table
        let internal_table = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        let base_table = internal_table.base_table(&tx).expect("to get base table");

        assert!(base_table.is_some());
        assert_eq!(
            base_table.expect("to be some").table_definition,
            table_creator.table_definition
        );

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_primary_keys_match() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(TableName::new("test_table"), Arc::clone(&schema))
                .with_constraints(get_pk_constraints(&["id"], Arc::clone(&schema))),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 2 internal tables which should have the same indexes
        let table_creator = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let table_creator2 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator2
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let primary_keys_match = table_creator
            .verify_primary_keys_match(&table_creator2, &tx)
            .expect("to verify primary keys match");

        assert!(primary_keys_match);

        // make another table that does not match
        let table_definition = get_basic_table_definition();

        let table_creator3 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator3
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let primary_keys_match = table_creator
            .verify_primary_keys_match(&table_creator3, &tx)
            .expect("to verify primary keys match");

        assert!(!primary_keys_match);

        // validate that 2 empty tables return true
        let table_creator4 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator4
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let primary_keys_match = table_creator3
            .verify_primary_keys_match(&table_creator4, &tx)
            .expect("to verify primary keys match");

        assert!(primary_keys_match);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_indexes_match() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(TableName::new("test_table"), Arc::clone(&schema)).with_indexes(
                vec![(
                    ColumnReference::try_from("id").expect("valid column ref"),
                    IndexType::Enabled,
                )]
                .into_iter()
                .collect(),
            ),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 2 internal tables which should have the same indexes
        let table_creator = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator
            .create_indexes(&tx)
            .expect("to create indexes");

        let table_creator2 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator2
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator2
            .create_indexes(&tx)
            .expect("to create indexes");

        let indexes_match = table_creator
            .verify_indexes_match(&table_creator2, &tx)
            .expect("to verify indexes match");

        assert!(indexes_match);

        // make another table that does not match
        let table_definition = get_basic_table_definition();

        let table_creator3 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator3
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator3
            .create_indexes(&tx)
            .expect("to create indexes");

        let indexes_match = table_creator
            .verify_indexes_match(&table_creator3, &tx)
            .expect("to verify indexes match");

        assert!(!indexes_match);

        // validate that 2 empty tables return true
        let table_creator4 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator4
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator4
            .create_indexes(&tx)
            .expect("to create indexes");

        let indexes_match = table_creator3
            .verify_indexes_match(&table_creator4, &tx)
            .expect("to verify indexes match");

        assert!(indexes_match);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_current_schema() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        let table_creator = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let schema = table_creator
            .current_schema(&tx)
            .expect("to get current schema");

        assert!(schema.equivalent_names_and_types(&table_definition.schema));

        // schemas between different tables are equivalent
        let table_creator2 = TableCreator::new(Arc::clone(&table_definition))
            .expect("to create table creator")
            .with_internal(true);

        table_creator2
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let schema2 = table_creator2
            .current_schema(&tx)
            .expect("to get current schema");

        assert!(schema.equivalent_names_and_types(&schema2));

        tx.rollback().expect("should rollback transaction");
    }
}
