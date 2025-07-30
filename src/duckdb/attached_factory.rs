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
use duckdb::AccessMode;
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
            .with_access_mode(AccessMode::ReadWrite)
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

        let table_ref_for_duckdb = TableReference::full(db_name, "main", cmd.name.table());

        let schema = Arc::new(cmd.schema.as_ref().into());

        let table_definition = TableDefinition::new(
            RelationName::from(table_ref_for_duckdb.clone()),
            Arc::clone(&schema),
        )
        .with_constraints(cmd.constraints.clone());

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::arrow::array::{Array, Int32Array, RecordBatch};
    use datafusion::logical_expr::logical_plan::dml::InsertOp;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use duckdb::Connection;
    use insta::assert_snapshot;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_attached_db() -> DataFusionResult<()> {
        // 1. Create a DuckDB database file with a table
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = Connection::open(&db_path).expect("Failed to open DuckDB");
        conn.execute_batch(
            r#"
            CREATE TABLE test_table (a INTEGER);
            INSERT INTO test_table VALUES (7);
        "#,
        )
        .expect("Failed to create table and insert data");
        // Connection must be dropped, to release the write lock on the file.
        drop(conn);

        // 2. Setup factory and create command
        let factory = AttachedDuckDBTableProviderFactory::new().expect("failed to create factory");

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let df_schema = schema.try_into()?;

        let cmd = CreateExternalTable {
            schema: Arc::new(df_schema),
            name: TableReference::bare("test_table"),
            location: db_path.to_str().unwrap().to_string(),
            file_type: String::new(),
            table_partition_cols: Vec::new(),
            if_not_exists: false,
            definition: None,
            order_exprs: Vec::new(),
            unbounded: false,
            options: Default::default(),
            constraints: Default::default(),
            column_defaults: Default::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let state: &dyn Session = &ctx.state();

        // 3. Create the table provider
        let table_provider = factory
            .create(state, &cmd)
            .await
            .expect("Failed to create table provider");

        // 4. Verify by querying the data
        let exec = table_provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan failed");
        let batches = collect(exec, ctx.task_ctx()).await?;

        let formatted = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert_snapshot!(formatted);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_to_attached_db() -> DataFusionResult<()> {
        // 1. Setup
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_write.db");
        let factory = AttachedDuckDBTableProviderFactory::new().expect("failed to create factory");

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let df_schema = Arc::clone(&schema).try_into()?;

        let cmd = CreateExternalTable {
            schema: Arc::new(df_schema),
            name: TableReference::bare("write_table"),
            location: db_path.to_str().unwrap().to_string(),
            file_type: String::new(),
            table_partition_cols: Vec::new(),
            if_not_exists: false,
            definition: None,
            order_exprs: Vec::new(),
            unbounded: false,
            options: Default::default(),
            constraints: Default::default(),
            column_defaults: Default::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let state: &dyn Session = &ctx.state();

        // 2. Create the table provider
        let table_provider = factory
            .create(state, &cmd)
            .await
            .expect("Failed to create table provider");

        // 3. First write
        let data = vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>];
        let batch = RecordBatch::try_new(Arc::clone(&schema), data)?;
        let insert_plan = ctx.read_batch(batch)?.create_physical_plan().await?;
        let write_exec = table_provider
            .insert_into(state, insert_plan, InsertOp::Append)
            .await?;
        collect(write_exec, ctx.task_ctx()).await?;

        // 4. First read
        let read_exec = table_provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan failed");
        let batches = collect(read_exec, ctx.task_ctx()).await?;
        let formatted = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert_snapshot!(formatted);

        // 5. Second write (append)
        let data = vec![Arc::new(Int32Array::from(vec![4, 5])) as Arc<dyn Array>];
        let batch = RecordBatch::try_new(Arc::clone(&schema), data)?;
        let insert_plan = ctx.read_batch(batch)?.create_physical_plan().await?;
        let write_exec = table_provider
            .insert_into(state, insert_plan, InsertOp::Append)
            .await?;
        collect(write_exec, ctx.task_ctx()).await?;

        // 6. Second read
        let read_exec = table_provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan failed");
        let batches = collect(read_exec, ctx.task_ctx()).await?;
        let formatted = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert_snapshot!(formatted);

        Ok(())
    }
}
