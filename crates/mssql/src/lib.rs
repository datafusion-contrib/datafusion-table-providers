//! A DataFusion table provider for Microsoft SQL Server (and Azure SQL), built on the
//! [`tiberius`](https://crates.io/crates/tiberius) TDS driver.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::{Constraint, Constraints, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::CreateExternalTable;
use snafu::prelude::*;
use tiberius::{ColumnData, ToSql};

use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    DbConnection, GenericError,
};
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion;
use datafusion_table_providers_common::util::{
    self, column_reference, column_reference::ColumnReference, constraints,
    constraints::get_primary_keys_from_constraints, indexes::IndexType, on_conflict,
    on_conflict::OnConflict, secrets::to_secret_map, to_datafusion_error,
};

use crate::arrow_sql_gen::{arrow_value_to_column_data, map_arrow_type_to_sql_server};
use crate::conn::{MsSqlConnection, MsSqlParameter, MsSqlPooledConnection};
use crate::dialect::{quote_identifier, quote_table_reference};
use crate::pool::MsSqlConnectionPool;
use crate::sql_table::MsSqlTable;
use crate::write::MsSqlTableWriter;

pub mod arrow_sql_gen;
pub mod conn;
pub mod dialect;
#[cfg(feature = "federation")]
pub mod federation;
pub mod pool;
pub mod sql_table;
pub mod write;

pub type DynMsSqlConnection = dyn DbConnection<MsSqlPooledConnection, MsSqlParameter>;

/// SQL Server accepts at most 2100 parameters per request.
const MAX_PARAMETERS: usize = 2_000;
/// A table value constructor (`VALUES`) can have at most 1000 rows.
const MAX_ROWS_PER_INSERT: usize = 1_000;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError { source: GenericError },

    #[snafu(display("Unable to create the SQL Server connection pool: {source}"))]
    UnableToCreateConnectionPool { source: pool::Error },

    #[snafu(display("Unable to construct the SQL table: {source}"))]
    UnableToConstructSqlTable {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to downcast DbConnection to MsSqlConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("The table '{table_name}' doesn't exist in SQL Server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Unable to begin the SQL Server transaction: {source}"))]
    UnableToBeginTransaction { source: tiberius::error::Error },

    #[snafu(display("Unable to commit the SQL Server transaction: {source}"))]
    UnableToCommitTransaction { source: tiberius::error::Error },

    #[snafu(display("Unable to roll back the SQL Server transaction: {source}"))]
    UnableToRollbackTransaction { source: tiberius::error::Error },

    #[snafu(display("Unable to create the SQL Server table: {source}"))]
    UnableToCreateTable { source: tiberius::error::Error },

    #[snafu(display("Unable to create an index for the SQL Server table: {source}"))]
    UnableToCreateIndex { source: tiberius::error::Error },

    #[snafu(display("Unable to delete all data from the SQL Server table: {source}"))]
    UnableToDeleteAllTableData { source: tiberius::error::Error },

    #[snafu(display("Unable to insert an Arrow batch into the SQL Server table: {source}"))]
    UnableToInsertArrowBatch { source: tiberius::error::Error },

    #[snafu(display("Unable to execute the statement: {source}"))]
    UnableToExecuteStatement { source: tiberius::error::Error },

    #[snafu(display("Unable to convert the Arrow data: {source}"))]
    ArrowConversion { source: arrow_sql_gen::Error },

    #[snafu(display(
        "`on_conflict` '{on_conflict}' requires the conflict columns, e.g. `do_nothing:id`, or a primary key"
    ))]
    MissingConflictColumns { on_conflict: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates SQL Server table providers from an existing connection pool.
pub struct MsSqlTableFactory {
    pool: Arc<MsSqlConnectionPool>,
}

impl MsSqlTableFactory {
    #[must_use]
    pub fn new(pool: Arc<MsSqlConnectionPool>) -> Self {
        Self { pool }
    }

    /// Creates a read-only table provider for an existing table or view.
    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let table = Arc::new(
            MsSqlTable::new(&self.pool, table_reference)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        #[cfg(feature = "federation")]
        let table = Arc::new(
            table
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table)
    }

    /// Creates a table provider for an existing table that also supports `INSERT INTO`,
    /// `INSERT OVERWRITE`, `UPDATE` and `DELETE`.
    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = self.table_provider(table_reference.clone()).await?;
        let schema = read_provider.schema();
        let mssql = MsSql::new(
            table_reference,
            Arc::clone(&self.pool),
            schema,
            Constraints::default(),
        );
        Ok(MsSqlTableWriter::create(read_provider, mssql, None))
    }
}

/// Creates SQL Server tables with `CREATE EXTERNAL TABLE ... STORED AS MSSQL`.
///
/// The table is created in SQL Server if it doesn't exist. The `OPTIONS` are the connection
/// parameters of [`MsSqlConnectionPool::new`], plus:
/// - `indexes`: the indexes to create, e.g. `'a:enabled;(b, c):unique'`
/// - `on_conflict`: how to insert the rows that conflict with existing ones, e.g. `'upsert:id'`
#[derive(Debug, Default)]
pub struct MsSqlTableProviderFactory {}

impl MsSqlTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProviderFactory for MsSqlTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let table_reference = cmd.name.clone();
        // SQL `CREATE EXTERNAL TABLE` prefixes the options with `format.`
        let mut options = util::remove_prefix_from_hashmap_keys(cmd.options.clone(), "format.");
        let schema: Schema = cmd.schema.as_ref().as_arrow().clone();
        let schema = Arc::new(schema);

        let mut indexes: Vec<(ColumnReference, IndexType)> = Vec::new();
        if let Some(indexes_str) = options.remove("indexes") {
            let unparsed: HashMap<String, IndexType> =
                util::hashmap_from_option_string(&indexes_str);
            for (columns, index_type) in unparsed {
                let columns = ColumnReference::try_from(columns.as_str())
                    .context(UnableToParseColumnReferenceSnafu)
                    .map_err(to_datafusion_error)?;
                indexes.push((columns, index_type));
            }
        }

        let on_conflict = options
            .remove("on_conflict")
            .map(|s| {
                OnConflict::try_from(s.as_str())
                    .context(UnableToParseOnConflictSnafu)
                    .map_err(to_datafusion_error)
            })
            .transpose()?;

        let pool = Arc::new(
            MsSqlConnectionPool::new(to_secret_map(options))
                .await
                .context(UnableToCreateConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let mssql = MsSql::new(
            table_reference.clone(),
            Arc::clone(&pool),
            Arc::clone(&schema),
            cmd.constraints.clone(),
        );

        let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);
        mssql
            .create_table_if_not_exists(&primary_keys, &indexes)
            .await
            .map_err(to_datafusion_error)?;

        let read_provider = Arc::new(
            MsSqlTable::new_with_schema(&pool, Arc::clone(&schema), table_reference)
                .with_constraints(cmd.constraints.clone()),
        );

        #[cfg(feature = "federation")]
        let read_provider = Arc::new(read_provider.create_federated_table_provider()?);

        Ok(MsSqlTableWriter::create(read_provider, mssql, on_conflict))
    }
}

/// The write side of a SQL Server table.
#[derive(Debug)]
pub struct MsSql {
    table: TableReference,
    pool: Arc<MsSqlConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl MsSql {
    #[must_use]
    pub fn new(
        table: TableReference,
        pool: Arc<MsSqlConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table,
            pool,
            schema,
            constraints,
        }
    }

    #[must_use]
    pub fn table_reference(&self) -> &TableReference {
        &self.table
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    #[must_use]
    pub fn quoted_table_name(&self) -> String {
        quote_table_reference(&self.table)
    }

    /// Returns a connection to the database, checking that the table exists.
    pub async fn connect(&self) -> Result<Box<DynMsSqlConnection>> {
        let mut conn = self.pool.connect().await.context(DbConnectionSnafu)?;
        let mssql_conn = Self::mssql_conn(&mut conn)?;
        if !self.table_exists(mssql_conn).await {
            TableDoesntExistSnafu {
                table_name: self.table.to_string(),
            }
            .fail()?;
        }
        Ok(conn)
    }

    pub fn mssql_conn(db_connection: &mut Box<DynMsSqlConnection>) -> Result<&mut MsSqlConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<MsSqlConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
    }

    async fn table_exists(&self, conn: &MsSqlConnection) -> bool {
        let name = self.quoted_table_name();
        let mut client = conn.conn.lock().await;
        let row = match client
            .query("SELECT OBJECT_ID(@P1)", &[&name.as_str()])
            .await
        {
            Ok(stream) => stream.into_row().await,
            Err(e) => Err(e),
        };
        match row {
            Ok(Some(row)) => row.get::<i32, _>(0).is_some(),
            Ok(None) => false,
            Err(e) => {
                tracing::warn!("Unable to check if the table {name} exists: {e}");
                false
            }
        }
    }

    /// Executes a single statement and returns the number of rows it affected.
    pub(crate) async fn execute_statement(&self, sql: &str) -> Result<u64> {
        let mut db_conn = self.connect().await?;
        let conn = Self::mssql_conn(&mut db_conn)?;
        tracing::debug!("SQL Server statement: {sql}");
        conn.execute_raw(sql, &[])
            .await
            .context(UnableToExecuteStatementSnafu)
    }

    pub(crate) async fn delete_all_table_data(
        &self,
        client: &mut bb8_tiberius::rt::Client,
    ) -> Result<()> {
        let sql = format!("DELETE FROM {}", self.quoted_table_name());
        execute_with(client, &sql, &[])
            .await
            .context(UnableToDeleteAllTableDataSnafu)?;
        Ok(())
    }

    /// Inserts the rows of a batch, with as many rows per statement as SQL Server allows.
    pub(crate) async fn insert_batch(
        &self,
        client: &mut bb8_tiberius::rt::Client,
        batch: &RecordBatch,
        on_conflict: Option<&OnConflict>,
    ) -> Result<()> {
        let schema = batch.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Ok(());
        }
        let conflict_columns = on_conflict
            .map(|oc| self.conflict_columns(oc))
            .transpose()?;

        let rows_per_statement = (MAX_PARAMETERS / columns.len()).clamp(1, MAX_ROWS_PER_INSERT);
        let mut start = 0;
        while start < batch.num_rows() {
            let end = (start + rows_per_statement).min(batch.num_rows());

            let mut params: Vec<ColumnData<'_>> = Vec::with_capacity((end - start) * columns.len());
            for row in start..end {
                for (array, column) in batch.columns().iter().zip(&columns) {
                    params.push(
                        arrow_value_to_column_data(column, array, row)
                            .context(ArrowConversionSnafu)?,
                    );
                }
            }
            let values = values_placeholders(end - start, columns.len());

            let sql = match (on_conflict, &conflict_columns) {
                (Some(on_conflict), Some(conflict_columns)) => merge_statement(
                    &self.quoted_table_name(),
                    &columns,
                    &values,
                    conflict_columns,
                    on_conflict,
                ),
                _ => format!(
                    "INSERT INTO {} ({}) VALUES {values}",
                    self.quoted_table_name(),
                    columns
                        .iter()
                        .map(|c| quote_identifier(c))
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            };

            execute_with(client, &sql, &params)
                .await
                .context(UnableToInsertArrowBatchSnafu)?;
            start = end;
        }
        Ok(())
    }

    fn conflict_columns(&self, on_conflict: &OnConflict) -> Result<Vec<String>> {
        let columns = match on_conflict {
            OnConflict::DoNothing(columns) | OnConflict::Upsert(columns) => {
                columns.iter().map(ToString::to_string).collect()
            }
            OnConflict::DoNothingAll => {
                get_primary_keys_from_constraints(&self.constraints, &self.schema)
            }
        };
        if columns.is_empty() {
            MissingConflictColumnsSnafu {
                on_conflict: on_conflict.to_string(),
            }
            .fail()?;
        }
        Ok(columns)
    }

    /// Creates the table, and its indexes, unless the table exists already.
    pub async fn create_table_if_not_exists(
        &self,
        primary_keys: &[String],
        indexes: &[(ColumnReference, IndexType)],
    ) -> Result<()> {
        let mut db_conn = self.pool.connect().await.context(DbConnectionSnafu)?;
        let conn = Self::mssql_conn(&mut db_conn)?;
        let mut client = conn.conn.lock().await;

        let mut key_columns = primary_keys.to_vec();
        for (columns, _) in indexes {
            key_columns.extend(columns.iter().map(ToString::to_string));
        }
        for constraint in self.constraints.iter() {
            if let Constraint::Unique(indices) = constraint {
                key_columns.extend(indices.iter().map(|i| self.schema.field(*i).name().clone()));
            }
        }

        let create_table = create_table_statement(
            &self.quoted_table_name(),
            &self.schema,
            primary_keys,
            &key_columns,
        )
        .context(ArrowConversionSnafu)?;
        let name = self.quoted_table_name();
        let sql = format!(
            "IF OBJECT_ID(N'{}', N'U') IS NULL {create_table}",
            name.replace('\'', "''")
        );
        tracing::debug!("{sql}");
        execute_with(&mut client, &sql, &[])
            .await
            .context(UnableToCreateTableSnafu)?;

        for (columns, index_type) in indexes {
            let columns = columns.iter().collect::<Vec<_>>();
            let sql =
                create_index_statement(&self.table, &columns, *index_type == IndexType::Unique);
            tracing::debug!("{sql}");
            execute_with(&mut client, &sql, &[])
                .await
                .context(UnableToCreateIndexSnafu)?;
        }

        for constraint in self.constraints.iter() {
            if let Constraint::Unique(indices) = constraint {
                let columns = indices
                    .iter()
                    .map(|i| self.schema.field(*i).name().as_str())
                    .collect::<Vec<_>>();
                let sql = create_index_statement(&self.table, &columns, true);
                execute_with(&mut client, &sql, &[])
                    .await
                    .context(UnableToCreateIndexSnafu)?;
            }
        }
        Ok(())
    }
}

async fn execute_with(
    client: &mut bb8_tiberius::rt::Client,
    sql: &str,
    params: &[ColumnData<'_>],
) -> Result<u64, tiberius::error::Error> {
    let params = params.iter().map(|p| p as &dyn ToSql).collect::<Vec<_>>();
    Ok(client.execute(sql, &params).await?.total())
}

async fn simple_statement(
    client: &mut bb8_tiberius::rt::Client,
    sql: &str,
) -> Result<(), tiberius::error::Error> {
    client.simple_query(sql).await?.into_results().await?;
    Ok(())
}

pub(crate) async fn begin_transaction(client: &mut bb8_tiberius::rt::Client) -> Result<()> {
    simple_statement(client, "BEGIN TRANSACTION")
        .await
        .context(UnableToBeginTransactionSnafu)
}

pub(crate) async fn commit_transaction(client: &mut bb8_tiberius::rt::Client) -> Result<()> {
    simple_statement(client, "COMMIT TRANSACTION")
        .await
        .context(UnableToCommitTransactionSnafu)
}

pub(crate) async fn rollback_transaction(client: &mut bb8_tiberius::rt::Client) -> Result<()> {
    simple_statement(client, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
        .await
        .context(UnableToRollbackTransactionSnafu)
}

/// `(@P1, @P2), (@P3, @P4), ...`
fn values_placeholders(rows: usize, columns: usize) -> String {
    (0..rows)
        .map(|row| {
            let params = (0..columns)
                .map(|column| format!("@P{}", row * columns + column + 1))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({params})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A `MERGE` that inserts the rows that don't conflict with existing ones, and updates the
/// conflicting rows for an upsert.
fn merge_statement(
    table: &str,
    columns: &[&str],
    values: &str,
    conflict_columns: &[String],
    on_conflict: &OnConflict,
) -> String {
    let quoted = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>();
    let on = conflict_columns
        .iter()
        .map(|c| {
            let c = quote_identifier(c);
            format!("[target].{c} = [source].{c}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let update_columns = columns
        .iter()
        .filter(|c| !conflict_columns.iter().any(|k| k == *c))
        .map(|c| {
            let c = quote_identifier(c);
            format!("[target].{c} = [source].{c}")
        })
        .collect::<Vec<_>>();
    let when_matched = match on_conflict {
        OnConflict::Upsert(_) if !update_columns.is_empty() => {
            format!(
                " WHEN MATCHED THEN UPDATE SET {}",
                update_columns.join(", ")
            )
        }
        _ => String::new(),
    };
    format!(
        "MERGE INTO {table} WITH (HOLDLOCK) AS [target] \
         USING (VALUES {values}) AS [source] ({columns}) \
         ON {on}{when_matched} \
         WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_columns});",
        columns = quoted.join(", "),
        source_columns = quoted
            .iter()
            .map(|c| format!("[source].{c}"))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

fn create_table_statement(
    table: &str,
    schema: &Schema,
    primary_keys: &[String],
    key_columns: &[String],
) -> arrow_sql_gen::Result<String> {
    let mut definitions = Vec::with_capacity(schema.fields().len() + 1);
    for field in schema.fields() {
        let is_key = key_columns.iter().any(|k| k == field.name());
        let sql_type = map_arrow_type_to_sql_server(field.name(), field.data_type(), is_key)?;
        let nullable = field.is_nullable() && !primary_keys.iter().any(|k| k == field.name());
        definitions.push(format!(
            "{} {sql_type} {}",
            quote_identifier(field.name()),
            if nullable { "NULL" } else { "NOT NULL" }
        ));
    }
    if !primary_keys.is_empty() {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            primary_keys
                .iter()
                .map(|k| quote_identifier(k))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(format!("CREATE TABLE {table} ({})", definitions.join(", ")))
}

fn create_index_statement(table: &TableReference, columns: &[&str], unique: bool) -> String {
    let index_name = format!("i_{}_{}", table.table(), columns.join("_"));
    let quoted_table = quote_table_reference(table);
    format!(
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{name}' AND object_id = OBJECT_ID(N'{table}')) \
         CREATE {unique}INDEX {index} ON {quoted_table} ({columns})",
        name = index_name.replace('\'', "''"),
        table = quoted_table.replace('\'', "''"),
        unique = if unique { "UNIQUE " } else { "" },
        index = quote_identifier(&index_name),
        columns = columns
            .iter()
            .map(|c| quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

    #[test]
    fn placeholders() {
        assert_eq!(values_placeholders(2, 2), "(@P1, @P2), (@P3, @P4)");
    }

    #[test]
    fn merge() {
        let sql = merge_statement(
            "[t]",
            &["id", "name"],
            "(@P1, @P2)",
            &["id".to_string()],
            &OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()])),
        );
        assert_eq!(
            sql,
            "MERGE INTO [t] WITH (HOLDLOCK) AS [target] USING (VALUES (@P1, @P2)) AS [source] ([id], [name]) \
             ON [target].[id] = [source].[id] WHEN MATCHED THEN UPDATE SET [target].[name] = [source].[name] \
             WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES ([source].[id], [source].[name]);"
        );
        let sql = merge_statement(
            "[t]",
            &["id", "name"],
            "(@P1, @P2)",
            &["id".to_string()],
            &OnConflict::DoNothing(ColumnReference::new(vec!["id".to_string()])),
        );
        assert!(!sql.contains("WHEN MATCHED"), "{sql}");
    }

    #[test]
    fn create_table() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("note", DataType::Utf8, false),
        ]);
        assert_eq!(
            create_table_statement(
                "[dbo].[t]",
                &schema,
                &["id".to_string()],
                &["id".to_string(), "name".to_string()]
            )
            .unwrap(),
            "CREATE TABLE [dbo].[t] ([id] BIGINT NOT NULL, [name] NVARCHAR(450) NULL, [note] NVARCHAR(MAX) NOT NULL, PRIMARY KEY ([id]))"
        );
    }

    #[test]
    fn create_index() {
        assert_eq!(
            create_index_statement(&TableReference::bare("t"), &["a", "b"], true),
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'i_t_a_b' AND object_id = OBJECT_ID(N'[t]')) \
             CREATE UNIQUE INDEX [i_t_a_b] ON [t] ([a], [b])"
        );
    }
}
