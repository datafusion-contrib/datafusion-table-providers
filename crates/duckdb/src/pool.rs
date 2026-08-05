use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager};
use snafu::{prelude::*, ResultExt};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};

use crate::conn::{
    file_identity, DuckDBAttachments, DuckDBParameter, DuckDbConnection, FileIdentity,
};
use datafusion_table_providers_common::{
    sql::db_connection_pool::{
        dbconnection::{DbConnection, SyncDbConnection},
        runtime::run_async_with_tokio,
        DbConnectionPool, JoinPushDown, Mode,
    },
    UnsupportedTypeAction,
};

type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDB connection failed.\n{source}\nFor details, refer to the DuckDB manual: https://duckdb.org/docs/"))]
    DuckDBConnectionError { source: duckdb::Error },

    #[snafu(display(
        "DuckDB connection failed.\n{source}\nAdjust the DuckDB connection pool parameters for sufficient capacity."
    ))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display(
        "Invalid DuckDB file path: {path}. Ensure it contains a valid database name."
    ))]
    UnableToExtractDatabaseNameFromPath { path: Arc<str> },

    #[snafu(display(
        "Cannot swap the database file of an in-memory DuckDB instance. Only file-backed DuckDB instances support file swapping."
    ))]
    FileSwapUnsupportedForMemory,
}

pub struct DuckDbConnectionPoolBuilder {
    path: String,
    max_size: Option<u32>,
    access_mode: AccessMode,
    min_idle: Option<u32>,
    mode: Mode,
    connection_setup_queries: Vec<Arc<str>>,
    instance_setup_queries: Vec<Arc<str>>,
}

impl DuckDbConnectionPoolBuilder {
    pub fn memory() -> Self {
        Self {
            path: String::default(),
            max_size: None,
            access_mode: AccessMode::ReadWrite,
            min_idle: None,
            mode: Mode::Memory,
            connection_setup_queries: Vec::new(),
            instance_setup_queries: Vec::new(),
        }
    }

    pub fn file(path: &str) -> Self {
        Self {
            path: path.to_string(),
            max_size: None,
            access_mode: AccessMode::ReadWrite,
            min_idle: None,
            mode: Mode::File,
            connection_setup_queries: Vec::new(),
            instance_setup_queries: Vec::new(),
        }
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn get_mode(&self) -> Mode {
        self.mode
    }

    pub fn with_max_size(mut self, size: Option<u32>) -> Self {
        self.max_size = size;
        self
    }

    pub fn with_access_mode(mut self, access_mode: AccessMode) -> Self {
        self.access_mode = access_mode;
        self
    }

    pub fn with_min_idle(mut self, min_idle: Option<u32>) -> Self {
        self.min_idle = min_idle;
        self
    }

    pub fn with_connection_setup_query(mut self, query: impl Into<Arc<str>>) -> Self {
        self.connection_setup_queries.push(query.into());
        self
    }

    /// Add a statement applied **once per DuckDB instance** (at pool build, and
    /// again on the replacement instance after a database file swap). Use this
    /// for instance-scoped settings (`PRAGMA enable_checkpoint_on_shutdown`,
    /// `PRAGMA checkpoint_threshold`, ...): applying those per connection would
    /// let a draining connection re-apply them to a retiring instance after a
    /// file swap has deliberately reconfigured it.
    pub fn with_instance_setup_query(mut self, query: impl Into<Arc<str>>) -> Self {
        self.instance_setup_queries.push(query.into());
        self
    }

    fn build_memory_pool(self) -> Result<DuckDbConnectionPool> {
        let config = get_config(&AccessMode::ReadWrite)?;
        let manager =
            DuckdbConnectionManager::memory_with_flags(config).context(DuckDBConnectionSnafu)?;

        tracing::debug!("Creating DuckDB connection pool for memory instance with max_size {:?} and min_idle {:?}", self.max_size, self.min_idle);

        let pool = build_r2d2_pool(manager, self.max_size, self.min_idle)?;
        initialize_instance(&pool, &self.instance_setup_queries)?;

        Ok(DuckDbConnectionPool {
            path: ":memory:".into(),
            state: Arc::new(RwLock::new(Arc::new(PoolState {
                pool,
                physical_path: ":memory:".into(),
                physical_identity: None,
            }))),
            write_gate: Arc::new(RwLock::new(())),
            instance_setup_queries: Arc::new(Mutex::new(self.instance_setup_queries)),
            rebuild: Arc::new(PoolRebuildConfig {
                max_size: self.max_size,
                min_idle: self.min_idle,
                access_mode: clone_access_mode(&self.access_mode),
            }),
            join_push_down: JoinPushDown::AllowedFor(":memory:".to_string()),
            attached_databases: Arc::new(OnceLock::new()),
            attachment_error_path: Arc::new(OnceLock::new()),
            mode: Mode::Memory,
            unsupported_type_action: UnsupportedTypeAction::Error,
            connection_setup_queries: self.connection_setup_queries,
        })
    }

    fn build_file_pool(self) -> Result<DuckDbConnectionPool> {
        let config = get_config(&self.access_mode)?;
        let manager = DuckdbConnectionManager::file_with_flags(&self.path, config)
            .context(DuckDBConnectionSnafu)?;

        tracing::debug!(
            "Creating DuckDB connection pool for path {} with max_size {:?} and min_idle {:?}",
            self.path,
            self.max_size,
            self.min_idle
        );

        let pool = build_r2d2_pool(manager, self.max_size, self.min_idle)?;
        initialize_instance(&pool, &self.instance_setup_queries)?;

        Ok(DuckDbConnectionPool {
            path: self.path.as_str().into(),
            state: Arc::new(RwLock::new(Arc::new(PoolState {
                pool,
                physical_identity: file_identity(&self.path),
                physical_path: self.path.as_str().into(),
            }))),
            write_gate: Arc::new(RwLock::new(())),
            instance_setup_queries: Arc::new(Mutex::new(self.instance_setup_queries)),
            rebuild: Arc::new(PoolRebuildConfig {
                max_size: self.max_size,
                min_idle: self.min_idle,
                access_mode: clone_access_mode(&self.access_mode),
            }),
            // Allow join-push down for any other instances that connect to the same underlying file.
            join_push_down: JoinPushDown::AllowedFor(self.path),
            attached_databases: Arc::new(OnceLock::new()),
            attachment_error_path: Arc::new(OnceLock::new()),
            mode: Mode::File,
            unsupported_type_action: UnsupportedTypeAction::Error,
            connection_setup_queries: self.connection_setup_queries,
        })
    }

    pub fn build(self) -> Result<DuckDbConnectionPool> {
        match self.mode {
            Mode::Memory => self.build_memory_pool(),
            Mode::File => self.build_file_pool(),
        }
    }
}

/// The swappable core of a [`DuckDbConnectionPool`]: the r2d2 pool over one
/// DuckDB database instance, plus the on-disk path that instance has open.
///
/// Shared behind `Arc<RwLock<Arc<..>>>` across every clone of the outer pool so
/// that a file swap performed through any clone is observed by all of them:
/// connections checked out after the swap come from the new instance, while
/// connections already checked out drain against the old instance (which stays
/// alive until the last of its pooled connections is dropped).
struct PoolState {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    physical_path: Arc<str>,
    /// `(device, inode)` of `physical_path` as it was when this instance opened
    /// it. A file swap compares this against the file currently at the path
    /// before unlinking it, so it can never destroy a file some *other*
    /// mechanism (a snapshot restore, an operator, a future replacement path)
    /// put there in the meantime. `None` on platforms without inode identity,
    /// where the check is skipped.
    physical_identity: Option<FileIdentity>,
}

/// The r2d2 settings needed to rebuild an equivalent pool over a new database
/// file during a file swap.
struct PoolRebuildConfig {
    max_size: Option<u32>,
    min_idle: Option<u32>,
    access_mode: AccessMode,
}

#[derive(Clone)]
pub struct DuckDbConnectionPool {
    path: Arc<str>,
    state: Arc<RwLock<Arc<PoolState>>>,
    /// Coordinates writers with database file swaps. All write paths hold the
    /// lock in shared mode for the duration of their transaction(s); a file
    /// swap holds it exclusively while it copies live data into the new file
    /// and replaces the pool. Readers do not take this lock.
    write_gate: Arc<RwLock<()>>,
    /// Instance-scoped statements (e.g. `SET memory_limit = ...`) applied to the
    /// current DuckDB instance, recorded so a file swap can replay them on the
    /// replacement instance. Instance-scoped settings do not carry over to a new
    /// instance on their own, unlike `connection_setup_queries` which are
    /// re-applied on every connection checkout.
    instance_setup_queries: Arc<Mutex<Vec<Arc<str>>>>,
    rebuild: Arc<PoolRebuildConfig>,
    join_push_down: JoinPushDown,
    /// Shared across clones. Initialized once, first set of attached databases wins.
    attached_databases: Arc<OnceLock<Arc<DuckDBAttachments>>>,
    /// Preserves the historical infallible setter while deferring an invalid
    /// attachment path error until the next connection attempt.
    attachment_error_path: Arc<OnceLock<Arc<str>>>,
    mode: Mode,
    unsupported_type_action: UnsupportedTypeAction,
    connection_setup_queries: Vec<Arc<str>>,
}

impl std::fmt::Debug for DuckDbConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbConnectionPool")
            .field("path", &self.path)
            .field("physical_path", &self.physical_path())
            .field("join_push_down", &self.join_push_down)
            .field("attached_databases", &self.attached_databases.get())
            .field("mode", &self.mode)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .finish()
    }
}

impl DuckDbConnectionPool {
    /// Get the dataset path. Returns `:memory:` if the in memory database is used.
    ///
    /// This is the *configured* path, which is stable across database file
    /// swaps; see [`Self::physical_path`] for the file currently backing the
    /// pool.
    pub fn db_path(&self) -> &str {
        self.path.as_ref()
    }

    /// The on-disk file currently backing this pool. Equal to [`Self::db_path`]
    /// except transiently during a database file swap, or after a swap could
    /// not restore the configured path (e.g. the previous file could not be
    /// removed on Windows); a restart normalizes the file back to the
    /// configured path.
    #[must_use]
    pub fn physical_path(&self) -> Arc<str> {
        Arc::clone(&self.load_state().physical_path)
    }

    /// Whether the file currently at this pool's physical path is still the same
    /// file the backing instance opened.
    ///
    /// Returns `false` only when the identity is known and has changed — i.e.
    /// something replaced the database file out-of-band. `true` when the pool is
    /// in-memory, when the platform has no inode identity, or when the path
    /// cannot be stat'ed, so callers treat "unknown" as "unchanged" and rely on
    /// their other safeguards.
    #[must_use]
    pub fn physical_file_unchanged(&self) -> bool {
        let state = self.load_state();
        let Some(expected) = state.physical_identity else {
            return true;
        };
        file_identity(&state.physical_path).is_none_or(|actual| actual == expected)
    }

    fn load_state(&self) -> Arc<PoolState> {
        Arc::clone(&self.state.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn r2d2_pool(&self) -> Arc<r2d2::Pool<DuckdbConnectionManager>> {
        Arc::clone(&self.load_state().pool)
    }

    /// The lock coordinating writers with database file swaps.
    ///
    /// Every code path that writes to this DuckDB instance outside of the
    /// built-in `DataSink`/DML paths (which take it internally) must hold the
    /// returned lock in shared (`read`) mode for the full duration of its
    /// write, acquiring it *before* checking out a connection. A database file
    /// swap holds the lock exclusively, so writes never race the swap's
    /// copy-and-replace and can never land in a database file that is about to
    /// be retired.
    #[must_use]
    pub fn write_gate(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.write_gate)
    }

    /// Record instance-scoped setup statements (e.g. `SET memory_limit = ...`)
    /// that were applied to the current DuckDB instance, so that a database
    /// file swap can replay them on the replacement instance.
    pub fn record_instance_setup_queries<I>(&self, statements: I)
    where
        I: IntoIterator<Item = Arc<str>>,
    {
        self.instance_setup_queries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(statements);
    }

    /// Returns the recorded instance-scoped setup statements.
    #[must_use]
    pub fn instance_setup_queries(&self) -> Vec<Arc<str>> {
        self.instance_setup_queries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The per-connection setup statements this pool clone applies on every
    /// checkout.
    #[must_use]
    pub fn connection_setup_queries(&self) -> &[Arc<str>] {
        &self.connection_setup_queries
    }

    /// Atomically replace the DuckDB instance backing this pool (and all of its
    /// clones) with a new instance opened on `new_path`.
    ///
    /// Returns the previously backing file path. Connections checked out before
    /// the swap keep operating against the old instance until they are dropped;
    /// the old instance closes once the last of them drains. The caller is
    /// responsible for holding the [`Self::write_gate`] exclusively across the
    /// swap and for removing the old file afterwards.
    ///
    /// # Errors
    ///
    /// Returns an error if this is an in-memory pool, or if the new instance
    /// cannot be opened and initialized. On error the pool is left unchanged.
    pub fn swap_database_file(&self, new_path: &str) -> Result<Arc<str>> {
        if self.mode != Mode::File {
            return Err(Box::new(Error::FileSwapUnsupportedForMemory));
        }

        let config = get_config(&clone_access_mode(&self.rebuild.access_mode))?;
        let manager = DuckdbConnectionManager::file_with_flags(new_path, config)
            .context(DuckDBConnectionSnafu)?;
        let pool = build_r2d2_pool(manager, self.rebuild.max_size, self.rebuild.min_idle)?;

        // Re-establish instance-scoped state on the fresh instance: the arrow
        // scan table function, this clone's connection setup queries (other
        // clones re-apply their own on checkout), and the recorded
        // instance-scoped settings.
        let mut setup = self.connection_setup_queries.clone();
        setup.extend(self.instance_setup_queries());
        initialize_instance(&pool, &setup)?;

        let new_state = Arc::new(PoolState {
            pool,
            physical_identity: file_identity(new_path),
            physical_path: new_path.into(),
        });

        let old_state = {
            let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
            std::mem::replace(&mut *state, new_state)
        };

        tracing::debug!(
            old_path = %old_state.physical_path,
            new_path,
            "Swapped DuckDB database file backing connection pool"
        );

        Ok(Arc::clone(&old_state.physical_path))
    }

    /// Create a new `DuckDbConnectionPool` from memory.
    ///
    /// # Arguments
    ///
    /// * `access_mode` - The access mode for the connection pool
    ///
    /// # Returns
    ///
    /// * A new `DuckDbConnectionPool`
    ///
    /// # Errors
    ///
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    pub fn new_memory() -> Result<Self> {
        DuckDbConnectionPoolBuilder::memory().build()
    }

    /// Create a new `DuckDbConnectionPool` from a file.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file
    /// * `access_mode` - The access mode for the connection pool
    ///
    /// # Returns
    ///
    /// * A new `DuckDbConnectionPool`
    ///
    /// # Errors
    ///
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    pub fn new_file(path: &str, access_mode: &AccessMode) -> Result<Self> {
        DuckDbConnectionPoolBuilder::file(path)
            .with_access_mode(clone_access_mode(access_mode))
            .build()
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    /// Sets the databases to attach for cross-database queries.
    /// Attachments are performed lazily on first query using `OnceLock`.
    ///
    /// If attachments are already configured with the same databases, this is a no-op.
    /// If attachments are already configured with different databases, a warning is logged
    /// and the existing attachments are preserved.
    ///
    pub fn set_attached_databases(mut self, databases: &[Arc<str>]) -> Self {
        if !databases.is_empty() {
            let mut paths: Vec<Arc<str>> = databases.to_vec();
            paths.push(Arc::clone(&self.path));
            paths.sort();
            let push_down_context = paths
                .iter()
                .map(|p| p.as_ref())
                .collect::<Vec<_>>()
                .join(";");
            self.join_push_down = JoinPushDown::AllowedFor(push_down_context);
        } else {
            return self;
        }

        let new_set: std::collections::HashSet<Arc<str>> = databases.iter().cloned().collect();
        let path = Arc::clone(&self.path);
        let db_name = match extract_db_name(Arc::clone(&path)) {
            Ok(db_name) => db_name,
            Err(_) => {
                let _ = self.attachment_error_path.set(path);
                return self;
            }
        };

        let existing = self.attached_databases.get_or_init(|| {
            tracing::debug!(
                "pool_path = {}, db_name = {}, databases = {:?}, set_attached_databases: creating new DuckDBAttachments",
                path, db_name, databases
            );
            Arc::new(DuckDBAttachments::new(&db_name, databases))
        });

        // Check if the existing attachments match what was requested
        let existing_set = existing.attachments();
        if *existing_set != new_set {
            tracing::warn!(
                "Unable to reconfigure DuckDB attachments for database {}: attachments are already configured with a different set of databases. \
                 Existing: {existing_set:?}, Requested: {new_set:?}. Keeping existing attachments.",
                self.path
            );
        }

        self
    }

    /// Returns the attachments configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured database path has no extractable
    /// database name.
    pub fn get_attachments(&self) -> Result<Option<Arc<DuckDBAttachments>>> {
        if let Some(path) = self.attachment_error_path.get() {
            return Err(Box::new(Error::UnableToExtractDatabaseNameFromPath {
                path: Arc::clone(path),
            }));
        }
        Ok(self.attached_databases.get().cloned())
    }

    #[must_use]
    pub fn with_connection_setup_queries(mut self, queries: Vec<Arc<str>>) -> Self {
        self.connection_setup_queries = queries;
        self
    }

    /// Create a new `DuckDbConnectionPool` from a database URL.
    ///
    /// # Errors
    ///
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    pub fn connect_sync(
        self: Arc<Self>,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        let pool = self.r2d2_pool();
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;

        for query in self.connection_setup_queries.iter() {
            tracing::debug!("DuckDB connection setup: {}", query);
            conn.execute(query, []).context(DuckDBConnectionSnafu)?;
        }

        let attachments = self.get_attachments()?;

        Ok(Box::new(
            DuckDbConnection::new(conn)
                .with_attachments(attachments)
                .with_connection_setup_queries(self.connection_setup_queries.clone())
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        // `r2d2::Pool::get()` (blocking checkout, up to the pool timeout) and the
        // per-connection setup queries are synchronous DuckDB calls. Run them on
        // the blocking pool so awaiting `connect()` doesn't pin a runtime worker.
        // `run_async_with_tokio` keeps this usable from non-Tokio executors/FFI.
        let pool = self.r2d2_pool();
        let setup_queries = self.connection_setup_queries.clone();
        let setup_queries_for_conn = self.connection_setup_queries.clone();
        let attachments = self.get_attachments()?;
        let unsupported_type_action = self.unsupported_type_action;

        let connect = async move || -> Result<
            Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
        > {
            let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
                tokio::task::spawn_blocking(move || -> Result<_> {
                    let conn = pool.get().context(ConnectionPoolSnafu)?;

                    for query in setup_queries.iter() {
                        tracing::debug!("DuckDB connection setup: {}", query);
                        conn.execute(query, []).context(DuckDBConnectionSnafu)?;
                    }

                    Ok(conn)
                })
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)??;

            Ok(Box::new(
                DuckDbConnection::new(conn)
                    .with_attachments(attachments)
                    .with_connection_setup_queries(setup_queries_for_conn)
                    .with_unsupported_type_action(unsupported_type_action),
            )
                as Box<
                    dyn DbConnection<
                        r2d2::PooledConnection<DuckdbConnectionManager>,
                        DuckDBParameter,
                    >,
                >)
        };
        run_async_with_tokio(connect).await
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn clone_access_mode(access_mode: &AccessMode) -> AccessMode {
    match access_mode {
        AccessMode::ReadOnly => AccessMode::ReadOnly,
        AccessMode::ReadWrite => AccessMode::ReadWrite,
        AccessMode::Automatic => AccessMode::Automatic,
    }
}

fn build_r2d2_pool(
    manager: DuckdbConnectionManager,
    max_size: Option<u32>,
    min_idle: Option<u32>,
) -> Result<Arc<r2d2::Pool<DuckdbConnectionManager>>> {
    let mut pool_builder = r2d2::Pool::builder();

    if let Some(size) = max_size {
        pool_builder = pool_builder.max_size(size);
    }
    if min_idle.is_some() {
        pool_builder = pool_builder.min_idle(min_idle);
    }

    Ok(Arc::new(
        pool_builder.build(manager).context(ConnectionPoolSnafu)?,
    ))
}

/// One-time initialization of a fresh DuckDB instance: registers the arrow
/// scan table function (instance-wide), applies the given setup statements,
/// and verifies the connection works.
fn initialize_instance(
    pool: &Arc<r2d2::Pool<DuckdbConnectionManager>>,
    setup_queries: &[Arc<str>],
) -> Result<()> {
    let conn = pool.get().context(ConnectionPoolSnafu)?;
    conn.register_table_function::<ArrowVTab>("arrow")
        .context(DuckDBConnectionSnafu)?;

    for query in setup_queries {
        tracing::debug!("DuckDB instance setup: {}", query);
        conn.execute(query, []).context(DuckDBConnectionSnafu)?;
    }

    test_connection(&conn)?;

    Ok(())
}

fn test_connection(conn: &r2d2::PooledConnection<DuckdbConnectionManager>) -> Result<()> {
    conn.execute("SELECT 1", [])
        .context(DuckDBConnectionSnafu)?;
    Ok(())
}

fn get_config(access_mode: &AccessMode) -> Result<duckdb::Config> {
    let config = duckdb::Config::default()
        .access_mode(match access_mode {
            AccessMode::ReadOnly => duckdb::AccessMode::ReadOnly,
            AccessMode::ReadWrite => duckdb::AccessMode::ReadWrite,
            AccessMode::Automatic => duckdb::AccessMode::Automatic,
        })
        .context(DuckDBConnectionSnafu)?;

    Ok(config)
}

// Helper function to extract the duckdb database name from the duckdb file path
fn extract_db_name(file_path: Arc<str>) -> Result<String> {
    let path = std::path::Path::new(file_path.as_ref());

    let db_name = match path.file_stem().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => {
            return Err(Box::new(Error::UnableToExtractDatabaseNameFromPath {
                path: file_path,
            }))
        }
    };

    Ok(db_name.to_string())
}

#[cfg(test)]
mod test {
    use rand::RngExt;

    use super::*;
    use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;

    fn random_db_name() -> String {
        let mut rng = rand::rng();
        let mut name = String::new();

        for _ in 0..10 {
            name.push(rng.random_range(b'a'..=b'z') as char);
        }

        format!("./{name}.duckdb")
    }

    #[tokio::test]
    async fn test_duckdb_connection_pool() {
        let pool =
            DuckDbConnectionPool::new_memory().expect("DuckDB connection pool to be created");
        let conn = pool
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn = conn
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        conn.execute("CREATE TABLE test (a INTEGER, b VARCHAR)", &[])
            .expect("Table should be created");
        conn.execute("INSERT INTO test VALUES (1, 'a')", &[])
            .expect("Data should be inserted");

        conn.query_arrow("SELECT * FROM test", &[], None)
            .expect("Query should be successful");
    }

    #[tokio::test]
    async fn test_swap_database_file_rejected_for_memory() {
        let pool =
            DuckDbConnectionPool::new_memory().expect("DuckDB connection pool to be created");
        let err = pool
            .swap_database_file("./never-created.duckdb")
            .expect_err("swap must be rejected for in-memory pools");
        assert!(
            err.to_string().contains("in-memory"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_swap_database_file_moves_new_connections() {
        let db_one = random_db_name();
        let db_two = random_db_name();

        let pool = Arc::new(
            DuckDbConnectionPool::new_file(&db_one, &AccessMode::ReadWrite)
                .expect("DuckDB connection pool to be created"),
        );
        // A clone taken before the swap, as a table provider would hold.
        let cloned = Arc::new(pool.as_ref().clone());

        {
            let conn = pool.r2d2_pool().get().expect("connection");
            conn.execute("CREATE TABLE t (v INTEGER)", [])
                .expect("create");
            conn.execute("INSERT INTO t VALUES (1)", [])
                .expect("insert");
        }

        // Build the second database file out-of-band.
        {
            let conn = duckdb::Connection::open(&db_two).expect("open second db");
            conn.execute("CREATE TABLE t (v INTEGER)", [])
                .expect("create");
            conn.execute("INSERT INTO t VALUES (2), (3)", [])
                .expect("insert");
        }

        // Hold a connection to the old instance across the swap.
        let old_conn = pool.r2d2_pool().get().expect("old connection");

        let old_path = pool
            .swap_database_file(&db_two)
            .expect("swap should succeed");
        assert_eq!(old_path.as_ref(), db_one.as_str());
        assert_eq!(pool.physical_path().as_ref(), db_two.as_str());
        // The configured path is stable across swaps.
        assert_eq!(pool.db_path(), db_one.as_str());

        // The held (pre-swap) connection still reads the old file.
        let old_count = old_conn
            .query_row("SELECT COUNT(1) FROM t", [], |r| r.get::<usize, i64>(0))
            .expect("old count");
        assert_eq!(old_count, 1);

        // New checkouts from BOTH the swapped pool and its pre-existing clone
        // observe the new file.
        for p in [&pool, &cloned] {
            let mut conn = Arc::clone(p).connect_sync().expect("connect");
            let duck = conn
                .as_any_mut()
                .downcast_mut::<DuckDbConnection>()
                .expect("duckdb connection");
            let count = duck
                .get_underlying_conn_mut()
                .query_row("SELECT COUNT(1) FROM t", [], |r| r.get::<usize, i64>(0))
                .expect("new count");
            assert_eq!(count, 2);
        }

        drop(old_conn);
        std::fs::remove_file(&db_one).expect("File should be removed");
        std::fs::remove_file(&db_two).expect("File should be removed");
    }

    #[tokio::test]
    #[cfg(feature = "federation")]
    async fn test_duckdb_connection_pool_with_attached_databases() {
        let db_base_name = random_db_name();
        let db_attached_name = random_db_name();
        let pool = DuckDbConnectionPool::new_file(&db_base_name, &AccessMode::ReadWrite)
            .expect("DuckDB connection pool to be created")
            .set_attached_databases(&[Arc::from(db_attached_name.as_str())]);

        let pool_attached =
            DuckDbConnectionPool::new_file(&db_attached_name, &AccessMode::ReadWrite)
                .expect("DuckDB connection pool to be created")
                .set_attached_databases(&[Arc::from(db_base_name.as_str())]);

        let conn = pool
            .r2d2_pool()
            .get()
            .expect("DuckDB connection should be established");

        conn.execute("CREATE TABLE test_one (a INTEGER, b VARCHAR)", [])
            .expect("Table should be created");
        conn.execute("INSERT INTO test_one VALUES (1, 'a')", [])
            .expect("Data should be inserted");

        let conn_attached = pool_attached
            .r2d2_pool()
            .get()
            .expect("DuckDB connection should be established");

        conn_attached
            .execute("CREATE TABLE test_two (a INTEGER, b VARCHAR)", [])
            .expect("Table should be created");
        conn_attached
            .execute("INSERT INTO test_two VALUES (1, 'a')", [])
            .expect("Data should be inserted");

        let conn = pool
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn = conn
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        let conn_attached = pool_attached
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn_attached = conn_attached
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        // sleep to let writes clear
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        conn.query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn_attached
            .query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        conn_attached
            .query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn.query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        std::fs::remove_file(&db_base_name).expect("File should be removed");
        std::fs::remove_file(&db_attached_name).expect("File should be removed");
    }
}
