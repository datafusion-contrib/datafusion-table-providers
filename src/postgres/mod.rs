use crate::postgres::error::*;
use crate::sql::arrow_sql_gen::statement::{CreateTableBuilder, IndexBuilder, InsertBuilder};
use crate::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use crate::sql::db_connection_pool::{
    dbconnection::DbConnection, postgrespool::PostgresConnectionPool, DbConnectionPool,
};
use crate::util::on_conflict::OnConflict;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use bb8_postgres::PostgresConnectionManager;
use datafusion::common::Constraints;
use postgres_native_tls::MakeTlsConnector;
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use tokio_postgres::Transaction;

pub mod error;
pub mod schema;
pub mod table;
pub mod write;

pub type DynPostgresConnectionPool = dyn DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'static (dyn ToSql + Sync),
    > + Send
    + Sync;
pub type DynPostgresConnection = dyn DbConnection<
    bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
    &'static (dyn ToSql + Sync),
>;

#[derive(Clone)]
pub struct Postgres {
    table_name: String,
    pool: Arc<PostgresConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl Postgres {
    #[must_use]
    pub fn new(
        table_name: String,
        pool: Arc<PostgresConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            pool,
            schema,
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

    pub async fn connect(&self) -> Result<Box<DynPostgresConnection>> {
        let mut conn = self.pool.connect().await.context(DbConnectionSnafu)?;

        let pg_conn = Self::postgres_conn(&mut conn)?;

        if !self.table_exists(pg_conn).await {
            TableDoesntExistSnafu {
                table_name: self.table_name.clone(),
            }
            .fail()?;
        }

        Ok(conn)
    }

    pub fn postgres_conn(
        db_connection: &mut Box<DynPostgresConnection>,
    ) -> Result<&mut PostgresConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<PostgresConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
    }

    async fn table_exists(&self, postgres_conn: &PostgresConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = '{name}'
        )"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        let Ok(row) = postgres_conn.conn.query_one(&sql, &[]).await else {
            return false;
        };

        row.get(0)
    }

    async fn insert_batch(
        &self,
        transaction: &Transaction<'_>,
        batch: RecordBatch,
        on_conflict: Option<OnConflict>,
    ) -> Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.table_name, vec![batch]);

        let sea_query_on_conflict =
            on_conflict.map(|oc| oc.build_sea_query_on_conflict(&self.schema));

        let sql = insert_table_builder
            .build_postgres(sea_query_on_conflict)
            .context(UnableToCreateInsertStatementSnafu)?;

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToInsertArrowBatchSnafu)?;

        Ok(())
    }

    async fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> Result<()> {
        transaction
            .execute(
                format!(r#"DELETE FROM "{}""#, self.table_name).as_str(),
                &[],
            )
            .await
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }

    async fn create_table(
        &self,
        schema: SchemaRef,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(schema, &self.table_name).primary_keys(primary_keys);
        let create_stmts = create_table_statement.build_postgres();

        for create_stmt in create_stmts {
            transaction
                .execute(&create_stmt, &[])
                .await
                .context(UnableToCreatePostgresTableSnafu)?;
        }

        Ok(())
    }

    async fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> Result<()> {
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToCreateIndexForPostgresTableSnafu)?;

        Ok(())
    }
}
