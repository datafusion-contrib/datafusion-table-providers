use std::any::Any;

use arrow::array::RecordBatch;
use async_stream::stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::{dialect::DuckDbDialect, tokenizer::Tokenizer};
use datafusion::sql::TableReference;
use duckdb::DuckdbConnectionManager;
use duckdb::ToSql;
use dyn_clone::DynClone;
use snafu::{prelude::*, ResultExt};
use tokio::sync::mpsc::Sender;

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ChannelError: {message}"))]
    ChannelError { message: String },
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

pub struct DuckDbConnection {
    pub conn: r2d2::PooledConnection<DuckdbConnectionManager>,
}

impl DuckDbConnection {
    pub fn get_underlying_conn_mut(
        &mut self,
    ) -> &mut r2d2::PooledConnection<DuckdbConnectionManager> {
        &mut self.conn
    }
}

impl<'a> DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
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

impl SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnection
{
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        DuckDbConnection { conn }
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

        Ok(result.get_schema())
    }

    fn query_arrow(
        &self,
        sql: &str,
        params: &[DuckDBParameter],
    ) -> Result<SendableRecordBatchStream> {
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let fetch_schema_sql = format!(
            "WITH fetch_schema_daith7owar AS ({sql}) SELECT * FROM fetch_schema_daith7owar LIMIT 0"
        );
        let mut stmt = self
            .conn
            .prepare(&fetch_schema_sql)
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let schema = result.get_schema();

        let params = params.iter().map(dyn_clone::clone).collect::<Vec<_>>();

        let conn = self.conn.try_clone()?;
        let sql = sql.to_string();

        let cloned_schema = schema.clone();

        let join_handle = tokio::task::spawn_blocking(move || {
            let mut stmt = conn.prepare(&sql).context(DuckDBSnafu)?;
            let params: &[&dyn ToSql] = &params
                .iter()
                .map(|f| f.as_input_parameter())
                .collect::<Vec<_>>();
            let result: duckdb::Arrow<'_> = stmt
                .stream_arrow(params, cloned_schema)
                .context(DuckDBSnafu)?;
            for i in result {
                blocking_channel_send(&batch_tx, i)?;
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });

        let output_stream = stream! {
            while let Some(batch) = batch_rx.recv().await {
                yield Ok(batch);
            }

            if let Err(e) = join_handle.await {
                yield Err(DataFusionError::Execution(format!(
                    "Failed to execute ODBC query: {e}"
                )))
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
}
