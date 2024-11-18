use crate::sql::db_connection_pool::{postgrespool::PostgresConnectionPool, DbConnectionPool};

use crate::postgres::error::*;
use crate::sql::sql_provider_datafusion::{Engine, SqlTable};
use arrow::datatypes::Schema;
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
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

use crate::util::{
    self, column_reference::ColumnReference, constraints::get_primary_keys_from_constraints,
    indexes::IndexType, on_conflict::OnConflict, secrets::to_secret_map,
};

use super::DynPostgresConnectionPool;
use super::{write::PostgresTableWriter, Postgres};

pub struct PostgresTableFactory {
    pool: Arc<PostgresConnectionPool>,
}

impl PostgresTableFactory {
    #[must_use]
    pub fn new(pool: Arc<PostgresConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;

        let table_provider = Arc::new(
            SqlTable::new(
                "postgres",
                &dyn_pool,
                table_reference,
                Some(Engine::Postgres),
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        #[cfg(feature = "postgres-federation")]
        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }

    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Self::table_provider(self, table_reference.clone()).await?;
        let schema = read_provider.schema();

        let table_name = table_reference.to_string();
        let postgres = Postgres::new(
            table_name,
            Arc::clone(&self.pool),
            schema,
            Constraints::empty(),
        );

        Ok(PostgresTableWriter::create(read_provider, postgres, None))
    }
}

#[derive(Default)]
pub struct PostgresTableProviderFactory {}

impl PostgresTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProviderFactory for PostgresTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let schema: Schema = cmd.schema.as_ref().into();

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

        let params = to_secret_map(options);

        let pool = Arc::new(
            PostgresConnectionPool::new(params)
                .await
                .context(UnableToCreatePostgresConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let schema = Arc::new(schema);
        let postgres = Postgres::new(
            name.clone(),
            Arc::clone(&pool),
            Arc::clone(&schema),
            cmd.constraints.clone(),
        );

        let mut db_conn = pool
            .connect()
            .await
            .context(DbConnectionSnafu)
            .map_err(to_datafusion_error)?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let tx = postgres_conn
            .conn
            .transaction()
            .await
            .context(UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);

        postgres
            .create_table(Arc::clone(&schema), &tx, primary_keys)
            .await
            .map_err(to_datafusion_error)?;

        for index in indexes {
            postgres
                .create_index(&tx, index.0.iter().collect(), index.1 == IndexType::Unique)
                .await
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .await
            .context(UnableToCommitPostgresTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;

        let read_provider = Arc::new(SqlTable::new_with_schema(
            "postgres",
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            Some(Engine::Postgres),
        ));

        #[cfg(feature = "postgres-federation")]
        let read_provider = Arc::new(read_provider.create_federated_table_provider()?);

        Ok(PostgresTableWriter::create(
            read_provider,
            postgres,
            on_conflict,
        ))
    }
}
