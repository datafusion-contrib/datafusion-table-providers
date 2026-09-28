use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::displayable;
use datafusion_table_providers::sql::db_connection_pool::quackpool::QuackConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use futures::TryStreamExt;
use rand::RngExt;
use secrecy::SecretString;

/// A Quack server given by `QUACK_SERVER_URI` (and `QUACK_AUTH_TOKEN`).
pub struct Server {
    uri: String,
    token: Option<String>,
}

/// Returns the configured server, or `None` (after saying so) when the tests should skip.
pub fn server() -> Option<Server> {
    let Ok(uri) = std::env::var("QUACK_SERVER_URI") else {
        eprintln!("QUACK_SERVER_URI is not set; skipping the Quack integration test");
        return None;
    };
    Some(Server {
        uri,
        token: std::env::var("QUACK_AUTH_TOKEN").ok(),
    })
}

impl Server {
    pub fn options(&self, extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut options = HashMap::from([("endpoint".to_string(), self.uri.clone())]);
        if let Some(token) = &self.token {
            options.insert("token".to_string(), token.clone());
        }
        for (key, value) in extra {
            options.insert((*key).to_string(), (*value).to_string());
        }
        options
    }

    pub async fn pool(&self, extra: &[(&str, &str)]) -> QuackConnectionPool {
        let params = self
            .options(extra)
            .into_iter()
            .map(|(k, v)| (k, SecretString::from(v)))
            .collect();
        QuackConnectionPool::new(params)
            .await
            .expect("connect to the Quack server")
    }
}

/// Runs statements on the server, e.g. to create fixtures.
pub async fn run(pool: &QuackConnectionPool, sql: &str) {
    let conn = pool.connect().await.expect("connection");
    let conn = conn.as_async().expect("async connection");
    conn.query_arrow(sql, &[], None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A table name no other test run uses.
pub fn table_name(prefix: &str) -> String {
    format!("{prefix}_{}", rand::rng().random_range(0..u32::MAX))
}

pub async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

pub fn pretty(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).expect("format").to_string()
}

/// The optimized physical plan, one node per line.
pub async fn physical_plan(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .create_physical_plan()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let text = displayable(plan.as_ref()).indent(false).to_string();
    text
}

pub fn federated_context() -> SessionContext {
    SessionContext::new_with_state(datafusion_federation::default_session_state())
}

pub fn shared(pool: QuackConnectionPool) -> Arc<QuackConnectionPool> {
    Arc::new(pool)
}
