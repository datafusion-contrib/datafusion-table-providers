use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Fields, SchemaBuilder};
use async_stream::stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::{dialect::DuckDbDialect, tokenizer::Tokenizer};
use datafusion::sql::TableReference;
use duckdb::vtab::to_duckdb_type_id;
use duckdb::ToSql;
use duckdb::{Connection, DuckdbConnectionManager};
use dyn_clone::DynClone;
use snafu::{prelude::*, ResultExt};
use tokio::sync::mpsc::Sender;

use crate::InvalidTypeAction;

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ChannelError: {message}"))]
    ChannelError { message: String },

    #[snafu(display("Unable to attach DuckDB database {path}: {source}"))]
    UnableToAttachDatabase {
        path: Arc<str>,
        source: std::io::Error,
    },

    #[snafu(display("Unable to extract database name from database file path"))]
    UnableToExtractDatabaseNameFromPath { path: Arc<str> },
}

pub trait DuckDBSyncParameter: ToSql + Sync + Send + DynClone {
    fn as_input_parameter(&self) -> &dyn ToSql;
}

impl<T: ToSql + Sync + Send + DynClone> DuckDBSyncParameter for T {
    fn as_input_parameter(&self) -> &dyn ToSql {
        self
    }
}
dyn_clone::clone_trait_object!(DuckDBSyncParameter);
pub type DuckDBParameter = Box<dyn DuckDBSyncParameter>;

#[derive(Debug)]
pub struct DuckDBAttachments {
    attachments: Vec<Arc<str>>,
    search_path: Arc<str>,
}

impl DuckDBAttachments {
    /// Creates a new instance of a `DuckDBAttachments`, which instructs DuckDB connections to attach other DuckDB databases for queries.
    #[must_use]
    pub fn new(id: &str, attachments: &[Arc<str>]) -> Self {
        let search_path = Self::get_search_path(id, attachments);
        Self {
            attachments: attachments.to_owned(),
            search_path,
        }
    }

    /// Returns the search path for the given database and attachments.
    /// The given database needs to be included separately, as search path by default do not include the main database.
    #[must_use]
    pub fn get_search_path(id: &str, attachments: &[Arc<str>]) -> Arc<str> {
        // search path includes the main database and all attached databases
        let mut search_path: Vec<Arc<str>> = vec![id.into()];

        search_path.extend(
            attachments
                .iter()
                .enumerate()
                .map(|(i, _)| format!("attachment_{i}").into()),
        );

        search_path.join(",").into()
    }

    /// Sets the search path for the given connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the search path cannot be set or the connection fails.
    pub fn set_search_path(&self, conn: &Connection) -> Result<()> {
        conn.execute(&format!("SET search_path ='{}'", self.search_path), [])
            .context(DuckDBSnafu)?;
        Ok(())
    }

    /// Resets the search path for the given connection to default.
    ///
    /// # Errors
    ///
    /// Returns an error if the search path cannot be set or the connection fails.
    pub fn reset_search_path(&self, conn: &Connection) -> Result<()> {
        conn.execute("RESET search_path", []).context(DuckDBSnafu)?;
        Ok(())
    }

    /// Attaches the databases to the given connection and sets the search path for the newly attached databases.
    ///
    /// # Errors
    ///
    /// Returns an error if a specific attachment is missing, cannot be attached, search path cannot be set or the connection fails.
    pub fn attach(&self, conn: &Connection) -> Result<()> {
        for (i, db) in self.attachments.iter().enumerate() {
            // check the db file exists
            std::fs::metadata(db.as_ref()).context(UnableToAttachDatabaseSnafu {
                path: Arc::clone(db),
            })?;

            conn.execute(
                &format!("ATTACH IF NOT EXISTS '{db}' AS attachment_{i} (READ_ONLY)"),
                [],
            )
            .context(DuckDBSnafu)?;
        }

        self.set_search_path(conn)?;

        Ok(())
    }

    /// Detaches the databases from the given connection and resets the search path to default.
    ///
    /// # Errors
    ///
    /// Returns an error if an attachment cannot be detached, search path cannot be set or the connection fails.
    pub fn detach(&self, conn: &Connection) -> Result<()> {
        for (i, _) in self.attachments.iter().enumerate() {
            conn.execute(&format!("DETACH attachment_{i}"), [])
                .context(DuckDBSnafu)?;
        }

        self.reset_search_path(conn)?;

        Ok(())
    }
}

pub struct DuckDbConnection {
    pub conn: r2d2::PooledConnection<DuckdbConnectionManager>,
    attachments: Option<Arc<DuckDBAttachments>>,
    invalid_type_action: InvalidTypeAction,
}

impl DuckDbConnection {
    pub fn get_underlying_conn_mut(
        &mut self,
    ) -> &mut r2d2::PooledConnection<DuckdbConnectionManager> {
        &mut self.conn
    }

    #[must_use]
    pub fn with_invalid_type_action(mut self, invalid_type_action: InvalidTypeAction) -> Self {
        self.invalid_type_action = invalid_type_action;
        self
    }

    #[must_use]
    pub fn with_attachments(mut self, attachments: Option<Arc<DuckDBAttachments>>) -> Self {
        self.attachments = attachments;
        self
    }

    /// Passthrough if Option is Some for `DuckDBAttachments::attach`
    ///
    /// # Errors
    ///
    /// See `DuckDBAttachments::attach` for more information.
    pub fn attach(conn: &Connection, attachments: &Option<Arc<DuckDBAttachments>>) -> Result<()> {
        if let Some(attachments) = attachments {
            attachments.attach(conn)?;
        }
        Ok(())
    }

    /// Passthrough if Option is Some for `DuckDBAttachments::detach`
    ///
    /// # Errors
    ///
    /// See `DuckDBAttachments::detach` for more information.
    pub fn detach(conn: &Connection, attachments: &Option<Arc<DuckDBAttachments>>) -> Result<()> {
        if let Some(attachments) = attachments {
            attachments.detach(conn)?;
        }
        Ok(())
    }

    /// For a given input schema, rebuild it according to the `InvalidTypeAction` set in the connection.
    /// If the `InvalidTypeAction` is `Error`, the function will return an error if the schema contains an unsupported data type.
    /// If the `InvalidTypeAction` is `Warn`, the function will log a warning if the schema contains an unsupported data type and remove the column.
    /// If the `InvalidTypeAction` is `Ignore`, the function will remove the column silently.
    ///
    /// # Errors
    ///
    /// If the `InvalidTypeAction` is `Error` and the schema contains an unsupported data type, the function will return an error.
    fn rebuild_schema(&self, input_schema: &SchemaRef) -> Result<SchemaRef, super::Error> {
        let mut schema_builder = SchemaBuilder::new();
        for field in &input_schema.fields {
            let duckdb_type_id = to_duckdb_type_id(field.data_type());
            let unsupported =
                data_type_is_unsupported(field.data_type()) || duckdb_type_id.is_err();

            if unsupported {
                let error = super::Error::UnsupportedDataType {
                    data_type: field.data_type().clone(),
                    field_name: field.name().clone(),
                };

                match self.invalid_type_action {
                    InvalidTypeAction::Error => return Err(error),
                    InvalidTypeAction::Warn => {
                        tracing::warn!("{error}");
                    }
                    InvalidTypeAction::Ignore => {}
                }
            } else {
                schema_builder.push(Arc::clone(field));
            }
        }

        Ok(Arc::new(schema_builder.finish()))
    }
}

impl DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(
        &self,
    ) -> Option<
        &dyn SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>,
    > {
        Some(self)
    }
}

fn struct_type_is_unsupported(fields: &Fields) -> bool {
    let mut any_unsupported = false;
    for field in fields {
        match field.data_type() {
            dt if dt.is_primitive() => continue,
            DataType::Utf8 | DataType::Binary => continue,
            DataType::List(inner_field)
            | DataType::LargeList(inner_field)
            | DataType::FixedSizeList(inner_field, _) => {
                any_unsupported = data_type_is_unsupported(inner_field.data_type());
                if any_unsupported {
                    break;
                }
            }
            DataType::Struct(inner_fields) => {
                any_unsupported = struct_type_is_unsupported(inner_fields);
                if any_unsupported {
                    break;
                }
            }
            _ => {
                any_unsupported = true;
                break;
            }
        }
    }
    any_unsupported
}

/// Returns true if the given `DataType` is not supported by the `DuckDB` `TableProvider`
fn data_type_is_unsupported(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(inner_field)
        | DataType::FixedSizeList(inner_field, _)
        | DataType::LargeList(inner_field) => {
            match inner_field.data_type() {
                dt if dt.is_primitive() => false,
                DataType::Utf8 | DataType::Binary | DataType::Boolean => false,
                _ => true, // nested lists don't support anything else yet
            }
        }
        DataType::Struct(inner_fields) => struct_type_is_unsupported(inner_fields),
        _ => false,
    }
}

impl SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnection
{
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        DuckDbConnection {
            conn,
            attachments: None,
            invalid_type_action: InvalidTypeAction::Error,
        }
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, super::Error> {
        let table_str = if is_table_function(table_reference) {
            table_reference.to_string()
        } else {
            table_reference.to_quoted_string()
        };
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_str} LIMIT 0"))
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        self.rebuild_schema(&result.get_schema())
    }

    fn query_arrow(
        &self,
        sql: &str,
        params: &[DuckDBParameter],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        Self::attach(&self.conn, &self.attachments)?;
        let fetch_schema_sql =
            format!("WITH fetch_schema AS ({sql}) SELECT * FROM fetch_schema LIMIT 0");
        let mut stmt = self
            .conn
            .prepare(&fetch_schema_sql)
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Self::detach(&self.conn, &self.attachments)?;

        let schema = result.get_schema();

        let params = params.iter().map(dyn_clone::clone).collect::<Vec<_>>();

        let conn = self.conn.try_clone()?; // try_clone creates a new connection to the same database
                                           // this creates a new connection session, requiring resetting the ATTACHments and search_path
        let sql = sql.to_string();

        let cloned_schema = schema.clone();
        let attachments = self.attachments.clone();

        let join_handle = tokio::task::spawn_blocking(move || {
            Self::attach(&conn, &attachments)?; // this attach could happen when we clone the connection, but we can't detach after the thread closes because the connection isn't thread safe
            let mut stmt = conn.prepare(&sql).context(DuckDBSnafu)?;
            let params: &[&dyn ToSql] = &params
                .iter()
                .map(|f| f.as_input_parameter())
                .collect::<Vec<_>>();
            let result: duckdb::ArrowStream<'_> = stmt
                .stream_arrow(params, cloned_schema)
                .context(DuckDBSnafu)?;
            for i in result {
                blocking_channel_send(&batch_tx, i)?;
            }

            Self::detach(&conn, &attachments)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });

        let output_stream = stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            match join_handle.await {
                Ok(Err(task_error)) => {
                    yield Err(DataFusionError::Execution(format!(
                        "Failed to execute DuckDB query: {task_error}"
                    )))
                },
                Err(join_error) => {
                    yield Err(DataFusionError::Execution(format!(
                        "Failed to execute DuckDB query: {join_error}"
                    )))
                },
                _ => {}
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            output_stream,
        )))
    }

    fn execute(&self, sql: &str, params: &[DuckDBParameter]) -> Result<u64> {
        let params: &[&dyn ToSql] = &params
            .iter()
            .map(|f| f.as_input_parameter())
            .collect::<Vec<_>>();

        let rows_modified = self.conn.execute(sql, params).context(DuckDBSnafu)?;
        Ok(rows_modified as u64)
    }
}

fn blocking_channel_send<T>(channel: &Sender<T>, item: T) -> Result<()> {
    match channel.blocking_send(item) {
        Ok(()) => Ok(()),
        Err(e) => Err(Error::ChannelError {
            message: format!("{e}"),
        }
        .into()),
    }
}

#[must_use]
pub fn flatten_table_function_name(table_reference: &TableReference) -> String {
    let table_name = table_reference.table();
    let filtered_name: String = table_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '(')
        .collect();
    let result = filtered_name.replace('(', "_");

    format!("{result}__view")
}

#[must_use]
pub fn is_table_function(table_reference: &TableReference) -> bool {
    let table_name = match table_reference {
        TableReference::Full { .. } | TableReference::Partial { .. } => return false,
        TableReference::Bare { table } => table,
    };

    let dialect = DuckDbDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, table_name);
    let Ok(tokens) = tokenizer.tokenize() else {
        return false;
    };
    let Ok(tf) = Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_table_factor()
    else {
        return false;
    };

    let TableFactor::Table { args, .. } = tf else {
        return false;
    };

    args.is_some()
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn test_is_table_function() {
        let tests = vec![
            ("table_name", false),
            ("table_name()", true),
            ("table_name(arg1, arg2)", true),
            ("read_parquet", false),
            ("read_parquet()", true),
            ("read_parquet('my_parquet_file.parquet')", true),
            ("read_csv_auto('my_csv_file.csv')", true),
        ];

        for (table_name, expected) in tests {
            let table_reference = TableReference::bare(table_name.to_string());
            assert_eq!(is_table_function(&table_reference), expected);
        }
    }

    #[test]
    fn test_field_is_unsupported() {
        // A list with a struct is not supported
        let field = Field::new(
            "list_struct",
            DataType::List(Arc::new(Field::new(
                "struct",
                DataType::Struct(vec![Field::new("field", DataType::Int64, false)].into()),
                false,
            ))),
            false,
        );

        assert!(
            data_type_is_unsupported(field.data_type()),
            "list with struct should be unsupported"
        );
    }

    #[test]
    fn test_fields_are_supported() {
        // test that the usual field types are supported, string, numbers, etc
        let fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
        ];

        for field in fields {
            assert!(
                !data_type_is_unsupported(field.data_type()),
                "field should be supported"
            );
        }
    }

    #[test]
    fn test_schema_rebuild_with_supported_fields() {
        let fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
        ];

        let schema = Arc::new(SchemaBuilder::from(Fields::from(fields)).finish());

        let conn = DuckDbConnection {
            conn: r2d2::Pool::new(
                DuckdbConnectionManager::memory().expect("should have a memory connection"),
            )
            .expect("should have pool")
            .get()
            .expect("should have connection"),
            attachments: None,
            invalid_type_action: InvalidTypeAction::Error,
        };

        let rebuilt_schema = conn
            .rebuild_schema(&schema)
            .expect("should rebuild schema successfully");

        assert_eq!(schema, rebuilt_schema);
    }

    #[test]
    fn test_schema_rebuild_with_unsupported_fields() {
        let fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
            Field::new(
                "list_struct",
                DataType::List(Arc::new(Field::new(
                    "struct",
                    DataType::Struct(vec![Field::new("field", DataType::Int64, false)].into()),
                    false,
                ))),
                false,
            ),
            Field::new("another_bool", DataType::Boolean, false),
            Field::new(
                "another_list_struct",
                DataType::List(Arc::new(Field::new(
                    "struct",
                    DataType::Struct(vec![Field::new("field", DataType::Int64, false)].into()),
                    false,
                ))),
                false,
            ),
            Field::new("another_float", DataType::Float32, false),
        ];

        let rebuilt_fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
            // this also tests that ordering is preserved when rebuilding the schema with removed fields
            Field::new("another_bool", DataType::Boolean, false),
            Field::new("another_float", DataType::Float32, false),
        ];

        let schema = Arc::new(SchemaBuilder::from(Fields::from(fields)).finish());
        let expected_rebuilt_schema =
            Arc::new(SchemaBuilder::from(Fields::from(rebuilt_fields)).finish());

        let conn = DuckDbConnection {
            conn: r2d2::Pool::new(
                DuckdbConnectionManager::memory().expect("should have a memory connection"),
            )
            .expect("should have pool")
            .get()
            .expect("should have connection"),
            attachments: None,
            invalid_type_action: InvalidTypeAction::Error,
        };

        assert!(conn.rebuild_schema(&schema).is_err());

        let conn = DuckDbConnection {
            conn: r2d2::Pool::new(
                DuckdbConnectionManager::memory().expect("should have a memory connection"),
            )
            .expect("should have pool")
            .get()
            .expect("should have connection"),
            attachments: None,
            invalid_type_action: InvalidTypeAction::Warn,
        };

        let rebuilt_schema = conn
            .rebuild_schema(&schema)
            .expect("should rebuild schema successfully");

        assert_eq!(rebuilt_schema, expected_rebuilt_schema);

        let conn = DuckDbConnection {
            conn: r2d2::Pool::new(
                DuckdbConnectionManager::memory().expect("should have a memory connection"),
            )
            .expect("should have pool")
            .get()
            .expect("should have connection"),
            attachments: None,
            invalid_type_action: InvalidTypeAction::Ignore,
        };

        let rebuilt_schema = conn
            .rebuild_schema(&schema)
            .expect("should rebuild schema successfully");

        assert_eq!(rebuilt_schema, expected_rebuilt_schema);
    }
}
