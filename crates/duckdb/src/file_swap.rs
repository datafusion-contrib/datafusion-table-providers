//! Full-refresh database **file swap** for file-backed DuckDB instances.
//!
//! An `InsertOp::Overwrite` normally rewrites a dataset's backing table inside
//! the live database file. DuckDB only reclaims the space of dropped tables at
//! a CHECKPOINT, and bulk loads bypass the WAL, so the WAL-growth trigger for
//! automatic checkpoints never fires — the live file grows without bound.
//! Running `CHECKPOINT` on the live instance is not an option under load: a
//! standard checkpoint fails while other transactions are active, and `FORCE
//! CHECKPOINT` aborts them.
//!
//! The swap path instead performs the overwrite into a *fresh* database file
//! and atomically replaces the live file with it:
//!
//! 1. **Stage** — stream the refreshed data into a new staging file (a private
//!    DuckDB instance), using the same internal-table + view layout as an
//!    in-place overwrite. The live instance serves queries untouched.
//! 2. **Copy** — under the pool's exclusive write gate (writers are paused,
//!    readers are not), attach the staging file to the live instance and copy
//!    every *other* table, view, and index — including metadata tables and
//!    other datasets sharing the file — into it. The refreshed dataset's stale
//!    tables are deliberately left behind; that is where the space is
//!    reclaimed.
//! 3. **Checkpoint** — checkpoint and cleanly detach the staging file, so it is
//!    a compact, WAL-free, self-contained database.
//! 4. **Swap** — rename the staging file over the live path and atomically
//!    repoint the connection pool at it. In-flight readers drain against the
//!    old instance; new checkouts observe the new file.
//!
//! # Why the old instance can never corrupt the new file
//!
//! DuckDB removes a database's WAL *by path* when a checkpoint completes
//! (including the shutdown checkpoint), so an old draining instance could
//! delete the new instance's WAL if both used the same path. Two measures
//! prevent this:
//!
//! - Writers are excluded for the entire swap by the write gate, and every
//!   post-swap write goes to the new pool — the old instance never commits
//!   (and therefore never checkpoints) again after the swap begins.
//! - `PRAGMA disable_checkpoint_on_shutdown` is applied to the old instance
//!   before the swap, so its eventual close performs no checkpoint and never
//!   touches the WAL path.
//!
//! The old database file is unlinked immediately after the swap; the draining
//! instance keeps reading it through its open file descriptors (the inode
//! outlives the unlink). If the unlink is denied (e.g. Windows holds the file
//! open), the pool serves the generation-named file instead and a restart
//! normalizes the path via [`recover_database_file_generations`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::common::Result as DataFusionResult;
use duckdb::Connection;
use snafu::prelude::*;
use tokio::sync::mpsc::Receiver;

use super::creator::{TableDefinition, TableManager};
use super::write::{execute_analyze_sql, write_to_table, WriteContext};
use super::{to_datafusion_error, DuckDB};
use crate::pool::DuckDbConnectionPool;
use datafusion_table_providers_common::util::retriable_error::to_retriable_data_write_error;

/// Infix appended to the configured database path for swap generation files:
/// `{configured}.refresh.{unix_ms}-{seq}` (plus `.building` while the staging
/// file is still being produced).
const GENERATION_INFIX: &str = ".refresh.";
const BUILDING_SUFFIX: &str = ".building";
const WAL_SUFFIX: &str = ".wal";

/// Monotonic per-process sequence, combined with a millisecond timestamp so
/// generation file names never collide within a process lifetime — the naming
/// guarantee that keeps a retiring instance's WAL path disjoint from every
/// newer generation.
static GENERATION_SEQ: AtomicU64 = AtomicU64::new(0);

fn wal_path_of(db_path: &str) -> String {
    format!("{db_path}{WAL_SUFFIX}")
}

/// Always-quote an identifier for DuckDB SQL, escaping embedded quotes.
/// Unconditional quoting matters because generation catalogs and user table
/// names may contain dots or reserved words.
fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn escape_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Best-effort file removal: missing files are fine, anything else is reported
/// to the caller.
fn remove_file_if_exists(path: &str) -> std::io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// The outcome of boot-time generation recovery for a configured database path.
#[derive(Debug, Default)]
pub struct SwapFileRecovery {
    /// A completed generation file that was adopted (renamed to the configured
    /// path) because the configured file was missing.
    pub adopted: Option<PathBuf>,
    /// Leftover swap files that were removed.
    pub removed: Vec<PathBuf>,
}

/// Recover from an interrupted database file swap at process startup, before
/// any DuckDB pool has been created for `configured_path`.
///
/// Rules, in order:
/// - `*.refresh.*.building` files (and their WALs) are incomplete staging
///   output from a crashed swap and are always deleted.
/// - If the configured file exists, it is authoritative: every completed
///   generation file is deleted. (A generation that was fully built but whose
///   swap never finished holds a refresh that was never acknowledged — the
///   refresh re-runs against the configured file.)
/// - If the configured file is missing, the newest completed generation is
///   adopted: renamed to the configured path together with its WAL if one
///   exists. Older generations are deleted.
///
/// Callers must ensure no DuckDB instance has `configured_path` (or any of its
/// generation files) open, and must run this at most once per path per
/// process.
///
/// # Errors
///
/// Returns an error if the directory cannot be enumerated or the adoption
/// rename fails. Failures to delete stale files are logged and skipped.
pub fn recover_database_file_generations(
    configured_path: &str,
) -> std::io::Result<SwapFileRecovery> {
    let configured = Path::new(configured_path);
    let dir = match configured.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let Some(file_name) = configured.file_name().and_then(|n| n.to_str()) else {
        return Ok(SwapFileRecovery::default());
    };
    if !dir.exists() {
        return Ok(SwapFileRecovery::default());
    }

    let generation_prefix = format!("{file_name}{GENERATION_INFIX}");
    let mut generations: Vec<(u128, u64, PathBuf)> = Vec::new();
    let mut leftovers: Vec<PathBuf> = Vec::new();

    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name.strip_prefix(generation_prefix.as_str()) else {
            continue;
        };

        if let Some((ts, seq)) = parse_generation_suffix(suffix) {
            generations.push((ts, seq, entry.path()));
        } else {
            // `.building`, `.wal`, `.building.wal`, or unrecognized debris.
            leftovers.push(entry.path());
        }
    }

    let mut recovery = SwapFileRecovery::default();

    // Adopt the newest completed generation only when the configured file is
    // gone (the swap that produced it got past the point of unlinking the old
    // file, so the generation is the authoritative newest state).
    if !configured.exists() {
        generations.sort_by_key(|(ts, seq, _)| (*ts, *seq));
        if let Some((_, _, newest)) = generations.pop() {
            let newest_str = newest.to_string_lossy().to_string();
            let generation_wal = wal_path_of(&newest_str);
            std::fs::rename(&newest, configured)?;
            if Path::new(&generation_wal).exists() {
                std::fs::rename(&generation_wal, wal_path_of(configured_path))?;
            }
            tracing::warn!(
                "Recovered DuckDB database file {configured_path} from interrupted file swap generation {newest_str}"
            );
            recovery.adopted = Some(newest);
        }
    }

    for (_, _, stale) in generations {
        // A completed generation's WAL (if any) is enumerated separately as a
        // leftover, so removing the database file alone is sufficient here.
        match std::fs::remove_file(&stale) {
            Ok(()) => recovery.removed.push(stale),
            Err(e) => {
                tracing::warn!(
                    "Failed to remove stale DuckDB file swap generation {}: {e}",
                    stale.display()
                );
            }
        }
    }
    for leftover in leftovers {
        // Never remove the WAL belonging to the file just adopted.
        if let Some(adopted) = &recovery.adopted {
            let adopted_wal = wal_path_of(&adopted.to_string_lossy());
            if leftover.to_string_lossy() == adopted_wal.as_str() {
                continue;
            }
        }
        match std::fs::remove_file(&leftover) {
            Ok(()) => recovery.removed.push(leftover),
            Err(e) => {
                tracing::warn!(
                    "Failed to remove leftover DuckDB file swap artifact {}: {e}",
                    leftover.display()
                );
            }
        }
    }

    Ok(recovery)
}

fn parse_generation_suffix(suffix: &str) -> Option<(u128, u64)> {
    let (ts, seq) = suffix.split_once('-')?;
    let ts = ts.parse::<u128>().ok()?;
    let seq = seq.parse::<u64>().ok()?;
    Some((ts, seq))
}

/// Removes staging files on drop unless disarmed, so a failed swap never
/// leaves partially built database files behind.
struct StagingCleanup {
    building_path: String,
    armed: bool,
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for path in [self.building_path.clone(), wal_path_of(&self.building_path)] {
            if let Err(e) = remove_file_if_exists(&path) {
                tracing::warn!("Failed to clean up DuckDB file swap staging file {path}: {e}");
            }
        }
    }
}

/// `InsertOp::Overwrite` via database file swap. See the module docs for the
/// full protocol.
pub(super) fn insert_overwrite_swap(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<arrow::array::RecordBatch>,
    mut on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    context: &WriteContext<'_>,
) -> DataFusionResult<u64> {
    let configured_path = pool.db_path().to_string();

    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context(super::UnableToGetSystemTimeSnafu)
        .map_err(to_datafusion_error)?
        .as_millis();
    let seq = GENERATION_SEQ.fetch_add(1, Ordering::Relaxed);
    let generation_path = format!("{configured_path}{GENERATION_INFIX}{unix_ms}-{seq}");
    let building_path = format!("{generation_path}{BUILDING_SUFFIX}");

    let mut cleanup = StagingCleanup {
        building_path: building_path.clone(),
        armed: true,
    };

    // ---- Phase 1: stage the refreshed data into a private instance. ----
    // No locks are held: the live file keeps serving reads and unrelated
    // writes while the (potentially long) source stream loads.
    let num_rows =
        stage_refreshed_data(&pool, table_definition, &building_path, batch_rx, context)?;

    on_commit_transaction
        .try_recv()
        .map_err(to_retriable_data_write_error)?;

    // ---- Phase 2: copy live content and swap, writers excluded. ----
    let write_gate = pool.write_gate();
    let _exclusive = write_gate
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    copy_live_contents_into_staging(&pool, table_definition, &building_path)?;
    complete_swap(&pool, &building_path, &generation_path)?;

    cleanup.armed = false;
    Ok(num_rows)
}

/// Phase 1: build the staging database file with the refreshed dataset,
/// checkpointed and cleanly closed (no WAL left on disk).
fn stage_refreshed_data(
    pool: &Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    building_path: &str,
    batch_rx: Receiver<arrow::array::RecordBatch>,
    context: &WriteContext<'_>,
) -> DataFusionResult<u64> {
    for stale in [building_path.to_string(), wal_path_of(building_path)] {
        remove_file_if_exists(&stale)
            .context(super::UnableToPrepareFileSwapSnafu { path: stale })
            .map_err(to_retriable_data_write_error)?;
    }

    let mut staging_conn = open_staging_instance(pool, building_path)?;

    let new_table = TableManager::new(Arc::clone(table_definition))
        .with_internal(true)
        .map_err(to_retriable_data_write_error)?;

    let tx = staging_conn
        .transaction()
        .context(super::UnableToBeginTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    // The CREATE TABLE statement is derived from the Arrow schema on a live
    // pool connection (created and rolled back there); it executes on the
    // staging transaction.
    new_table
        .create_table(Arc::clone(pool), &tx)
        .map_err(to_retriable_data_write_error)?;

    tracing::debug!(
        "Staged overwrite load for {table_name} into {building_path}",
        table_name = new_table.table_name()
    );
    let num_rows = write_to_table(
        &new_table,
        &tx,
        Arc::clone(context.schema),
        batch_rx,
        context.on_conflict,
    )?;

    new_table
        .create_view(&tx)
        .map_err(to_retriable_data_write_error)?;

    if let Some(callback) = context.on_data_written {
        callback(&tx, &new_table, context.schema, num_rows)?;
    }

    // Mirrors the in-place overwrite: the completion handler may already have
    // created the configured indexes; `CREATE INDEX IF NOT EXISTS` makes this
    // a no-op in that case.
    new_table
        .create_indexes(&tx)
        .map_err(to_retriable_data_write_error)?;

    if context.settings.recompute_statistics_on_write {
        execute_analyze_sql(&tx, &new_table.table_name().to_string());
    }

    tx.commit()
        .context(super::UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    // Flush everything into the database file and close cleanly so no WAL
    // survives; a WAL here would ride along under the live path after the
    // rename and be replayed against the wrong database identity.
    staging_conn
        .execute("CHECKPOINT", [])
        .context(super::UnableToCheckpointSwapStagingSnafu {
            path: building_path.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;
    drop(staging_conn);

    let staging_wal = wal_path_of(building_path);
    if Path::new(&staging_wal).exists() {
        return Err(to_datafusion_error(super::Error::FileSwapWalPresent {
            path: staging_wal,
        }));
    }

    Ok(num_rows)
}

fn open_staging_instance(
    pool: &Arc<DuckDbConnectionPool>,
    building_path: &str,
) -> DataFusionResult<Connection> {
    let conn = Connection::open(building_path)
        .context(super::UnableToOpenSwapStagingSnafu {
            path: building_path.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    conn.register_table_function::<duckdb::vtab::arrow::ArrowVTab>("arrow")
        .context(super::UnableToOpenSwapStagingSnafu {
            path: building_path.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    // Mirror the live instance's configuration so the staging load runs under
    // the same limits (memory limit, temp directory, checkpoint threshold, …).
    let mut setup: Vec<Arc<str>> = pool.instance_setup_queries();
    setup.extend(pool.connection_setup_queries().iter().cloned());
    for statement in setup {
        if let Err(e) = conn.execute(&statement, []) {
            tracing::warn!(
                "Failed to apply setting to DuckDB file swap staging instance ({statement}): {e}"
            );
        }
    }

    Ok(conn)
}

/// A table/view/index copy plan entry.
struct LiveObject {
    schema_name: String,
    name: String,
    sql: String,
}

/// Phase 2a: copy every live object that does **not** belong to the refreshed
/// dataset into the staging file, through the live instance (so data that is
/// still only in the live WAL is included). Runs with the write gate held
/// exclusively; concurrent readers are unaffected.
fn copy_live_contents_into_staging(
    pool: &Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    building_path: &str,
) -> DataFusionResult<()> {
    // A private (non-pooled) session on the live instance: `USE` leaks session
    // state, so it must never run on a connection that returns to the pool.
    let mut pooled = Arc::clone(pool)
        .connect_sync()
        .context(super::DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;
    let live_conn = DuckDB::duckdb_conn(&mut pooled)
        .map_err(to_retriable_data_write_error)?
        .get_underlying_conn_mut()
        .try_clone()
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "clone live connection".to_string(),
        })
        .map_err(to_retriable_data_write_error)?;
    drop(pooled);

    let live_catalog: String = live_conn
        .query_row("SELECT current_database()", [], |r| r.get(0))
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "resolve live catalog".to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    let seq = GENERATION_SEQ.fetch_add(1, Ordering::Relaxed);
    let stage_alias = format!("__dftp_swap_stage_{seq}");

    let result = copy_with_attached_staging(
        &live_conn,
        table_definition,
        building_path,
        &live_catalog,
        &stage_alias,
    );

    // Whatever happened, restore the session catalog and detach the staging
    // file. The staging file must not remain attached past this point: renaming
    // a still-attached file into the live path would leave two instances
    // writing the same inode.
    let _ = live_conn.execute(&format!("USE {}", quote_ident(&live_catalog)), []);
    let detach = live_conn
        .execute(&format!("DETACH {}", quote_ident(&stage_alias)), [])
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "detach staging file".to_string(),
        })
        .map_err(to_retriable_data_write_error);

    // The copy error (if any) takes precedence; the detach was still attempted
    // above so a failed copy never leaks the attachment on the live instance.
    result?;
    detach?;

    // The staging file must be complete and WAL-free after the clean detach; a
    // WAL here would ride along under the live path after the rename and be
    // replayed against the wrong database identity.
    let staging_wal = wal_path_of(building_path);
    if Path::new(&staging_wal).exists() {
        return Err(to_datafusion_error(super::Error::FileSwapWalPresent {
            path: staging_wal,
        }));
    }

    // The retiring instance must never checkpoint at shutdown: DuckDB removes
    // the WAL *by path* when a checkpoint completes, and after the swap this
    // instance's WAL path belongs to the new live file. (The runtime passes
    // `PRAGMA enable_checkpoint_on_shutdown` as an *instance* setup query, so
    // no draining connection can re-enable it on this retiring instance.)
    live_conn
        .execute("PRAGMA disable_checkpoint_on_shutdown", [])
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "disable retiring instance shutdown checkpoint".to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    Ok(())
}

fn copy_with_attached_staging(
    live_conn: &Connection,
    table_definition: &Arc<TableDefinition>,
    building_path: &str,
    live_catalog: &str,
    stage_alias: &str,
) -> DataFusionResult<()> {
    live_conn
        .execute(
            &format!(
                "ATTACH '{}' AS {}",
                escape_string_literal(building_path),
                quote_ident(stage_alias)
            ),
            [],
        )
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: format!("attach staging file {building_path}"),
        })
        .map_err(to_retriable_data_write_error)?;

    let skip_tables = refreshed_dataset_tables(live_conn, table_definition, live_catalog)?;
    let base_name = table_definition.name().to_string();

    let tables = query_live_objects(
        live_conn,
        &format!(
            "SELECT schema_name, table_name, sql FROM duckdb_tables() \
             WHERE database_name = '{live}' AND NOT internal AND NOT temporary \
             ORDER BY schema_name, table_name",
            live = escape_string_literal(live_catalog)
        ),
        "enumerate live tables",
    )?;
    let views = query_live_objects(
        live_conn,
        &format!(
            "SELECT schema_name, view_name, sql FROM duckdb_views() \
             WHERE database_name = '{live}' AND NOT internal \
             ORDER BY schema_name, view_name",
            live = escape_string_literal(live_catalog)
        ),
        "enumerate live views",
    )?;
    let indexes = query_live_objects(
        live_conn,
        &format!(
            "SELECT schema_name, table_name, sql FROM duckdb_indexes() \
             WHERE database_name = '{live}' AND sql IS NOT NULL \
             ORDER BY schema_name, index_name",
            live = escape_string_literal(live_catalog)
        ),
        "enumerate live indexes",
    )?;

    warn_on_skipped_object_kinds(live_conn, live_catalog);

    let mut hnsw_prepared = false;
    let mut current_schema: Option<String> = None;

    let use_stage_schema =
        |schema_name: &str, current: &mut Option<String>| -> DataFusionResult<()> {
            if current.as_deref() == Some(schema_name) {
                return Ok(());
            }
            if schema_name != "main" {
                live_conn
                    .execute(
                        &format!(
                            "CREATE SCHEMA IF NOT EXISTS {}.{}",
                            quote_ident(stage_alias),
                            quote_ident(schema_name)
                        ),
                        [],
                    )
                    .context(super::UnableToCopyLiveDatabaseSnafu {
                        detail: format!("create schema {schema_name} in staging file"),
                    })
                    .map_err(to_retriable_data_write_error)?;
            }
            live_conn
                .execute(
                    &format!(
                        "USE {}.{}",
                        quote_ident(stage_alias),
                        quote_ident(schema_name)
                    ),
                    [],
                )
                .context(super::UnableToCopyLiveDatabaseSnafu {
                    detail: format!("switch to staging schema {schema_name}"),
                })
                .map_err(to_retriable_data_write_error)?;
            *current = Some(schema_name.to_string());
            Ok(())
        };

    for table in &tables {
        if skip_tables.contains(&table.name) {
            continue;
        }
        use_stage_schema(&table.schema_name, &mut current_schema)?;
        live_conn
            .execute(&table.sql, [])
            .context(super::UnableToCopyLiveDatabaseSnafu {
                detail: format!("create table {} in staging file", table.name),
            })
            .map_err(to_retriable_data_write_error)?;
        let insert = format!(
            "INSERT INTO {schema}.{table} SELECT * FROM {live}.{schema}.{table}",
            live = quote_ident(live_catalog),
            schema = quote_ident(&table.schema_name),
            table = quote_ident(&table.name),
        );
        live_conn
            .execute(&insert, [])
            .context(super::UnableToCopyLiveDatabaseSnafu {
                detail: format!("copy rows of table {} into staging file", table.name),
            })
            .map_err(to_retriable_data_write_error)?;
    }

    for index in &indexes {
        // `duckdb_indexes()` rows carry the owning table in `table_name`.
        if skip_tables.contains(&index.name) {
            continue;
        }
        if !hnsw_prepared && index.sql.to_uppercase().contains("HNSW") {
            prepare_hnsw_support(live_conn);
            hnsw_prepared = true;
        }
        use_stage_schema(&index.schema_name, &mut current_schema)?;
        live_conn
            .execute(&index.sql, [])
            .context(super::UnableToCopyLiveDatabaseSnafu {
                detail: format!("create index on table {} in staging file", index.name),
            })
            .map_err(to_retriable_data_write_error)?;
    }

    for view in &views {
        if view.name == base_name {
            // The refreshed dataset's view was already created in the staging
            // file, pointing at the freshly loaded internal table.
            continue;
        }
        use_stage_schema(&view.schema_name, &mut current_schema)?;
        live_conn
            .execute(&view.sql, [])
            .context(super::UnableToCopyLiveDatabaseSnafu {
                detail: format!("create view {} in staging file", view.name),
            })
            .map_err(to_retriable_data_write_error)?;
    }

    // Leave the staging catalog as the session default before checkpointing.
    live_conn
        .execute(&format!("USE {}", quote_ident(live_catalog)), [])
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "restore live catalog".to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    live_conn
        .execute(&format!("CHECKPOINT {}", quote_ident(stage_alias)), [])
        .context(super::UnableToCheckpointSwapStagingSnafu {
            path: building_path.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    Ok(())
}

/// The refreshed dataset's own relations in the live file: its base table (if
/// any) and every `__data_{name}_{ts}` internal table. These are exactly the
/// relations the swap leaves behind.
fn refreshed_dataset_tables(
    live_conn: &Connection,
    table_definition: &Arc<TableDefinition>,
    live_catalog: &str,
) -> DataFusionResult<std::collections::HashSet<String>> {
    let base_name = table_definition.name().to_string();
    let mut skip = std::collections::HashSet::from([base_name.clone()]);

    let internal_prefix = format!("__data_{base_name}_");
    let sql = format!(
        "SELECT table_name FROM duckdb_tables() \
         WHERE database_name = '{live}' AND table_name LIKE '{prefix}%'",
        live = escape_string_literal(live_catalog),
        prefix = escape_string_literal(&internal_prefix),
    );
    let mut stmt = live_conn
        .prepare(&sql)
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "enumerate refreshed dataset internal tables".to_string(),
        })
        .map_err(to_retriable_data_write_error)?;
    let names = stmt
        .query_map([], |row| row.get::<usize, String>(0))
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: "enumerate refreshed dataset internal tables".to_string(),
        })
        .map_err(to_retriable_data_write_error)?;
    for name in names {
        let name = name
            .context(super::UnableToCopyLiveDatabaseSnafu {
                detail: "enumerate refreshed dataset internal tables".to_string(),
            })
            .map_err(to_retriable_data_write_error)?;
        // Internal table names end in a millisecond timestamp; anything else
        // that merely shares the prefix belongs to another dataset.
        if name
            .strip_prefix(&internal_prefix)
            .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
        {
            skip.insert(name);
        }
    }

    Ok(skip)
}

fn query_live_objects(
    live_conn: &Connection,
    sql: &str,
    detail: &str,
) -> DataFusionResult<Vec<LiveObject>> {
    let mut stmt = live_conn
        .prepare(sql)
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: detail.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(LiveObject {
                schema_name: row.get(0)?,
                name: row.get(1)?,
                sql: row.get(2)?,
            })
        })
        .context(super::UnableToCopyLiveDatabaseSnafu {
            detail: detail.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    let mut objects = Vec::new();
    for row in rows {
        objects.push(
            row.context(super::UnableToCopyLiveDatabaseSnafu {
                detail: detail.to_string(),
            })
            .map_err(to_retriable_data_write_error)?,
        );
    }
    Ok(objects)
}

/// Sequences and macros are not part of the accelerator's table layout and are
/// not copied; surface them loudly if any exist so the omission is never
/// silent.
fn warn_on_skipped_object_kinds(live_conn: &Connection, live_catalog: &str) {
    let live = escape_string_literal(live_catalog);
    for (kind, sql) in [
        (
            "sequences",
            format!("SELECT COUNT(1) FROM duckdb_sequences() WHERE database_name = '{live}'"),
        ),
        (
            "macros",
            format!(
                "SELECT COUNT(1) FROM duckdb_functions() WHERE database_name = '{live}' AND NOT internal"
            ),
        ),
    ] {
        match live_conn.query_row(&sql, [], |r| r.get::<usize, i64>(0)) {
            Ok(count) if count > 0 => {
                tracing::warn!(
                    "DuckDB file swap does not copy {kind}; {count} {kind} in {live_catalog} will not be carried into the new database file"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Failed to count {kind} during DuckDB file swap: {e}");
            }
        }
    }
}

/// HNSW indexes need the `vss` extension and its persistence flag in the
/// copying session. Best-effort: if this fails, the subsequent CREATE INDEX
/// fails and aborts the swap with the real error (the live file is untouched).
fn prepare_hnsw_support(live_conn: &Connection) {
    for sql in [
        "INSTALL vss",
        "LOAD vss",
        "SET hnsw_enable_experimental_persistence = true",
    ] {
        if let Err(e) = live_conn.execute(sql, []) {
            tracing::warn!(
                "DuckDB file swap: '{sql}' failed while preparing to copy an HNSW index: {e}"
            );
        }
    }
}

/// Phase 2b: move the completed staging file into place and repoint the pool.
///
/// Ordering (all under the exclusive write gate):
/// 1. `rename(building, generation)` — marks the staging output complete;
///    recovery adopts completed generations, never `.building` files.
/// 2. Unlink the old database file and its WAL. From here the configured path
///    is free; a crash before step 3 is healed at boot by adopting the
///    generation.
/// 3. `rename(generation, configured)` — the new file takes the live name.
/// 4. Repoint the pool. If the old file could not be unlinked (Windows file
///    locking), serve the generation path instead; a restart normalizes it.
fn complete_swap(
    pool: &Arc<DuckDbConnectionPool>,
    building_path: &str,
    generation_path: &str,
) -> DataFusionResult<()> {
    let configured_path = pool.db_path().to_string();
    let old_physical = pool.physical_path().to_string();

    // Refuse to unlink a file this pool does not own. The swap is not the only
    // mechanism that can replace the database file — a snapshot restore does too,
    // out-of-band and on its own schedule — and unlinking whatever happens to sit
    // at the configured path would silently destroy the other mechanism's file,
    // leaving the pool and the path resolving to different data. Aborting is
    // retriable: the staging file is cleaned up and the refresh runs again
    // against whatever is now the live file.
    if !pool.physical_file_unchanged() {
        return Err(to_retriable_data_write_error(
            super::Error::FileSwapFileReplaced {
                path: old_physical.clone(),
            },
        ));
    }

    std::fs::rename(building_path, generation_path)
        .context(super::UnableToCompleteFileSwapSnafu {
            path: building_path.to_string(),
        })
        .map_err(to_retriable_data_write_error)?;

    // Free the configured path: remove the retiring file (which may already
    // live at a generation path if a previous swap could not reclaim the
    // configured name) and any stale file still holding the configured name.
    let mut configured_free = true;
    for stale in [old_physical.clone(), configured_path.clone()] {
        match remove_file_if_exists(&stale) {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    "Failed to remove retiring DuckDB database file {stale} during file swap: {e}"
                );
                if stale == configured_path {
                    configured_free = false;
                }
            }
        }
        match remove_file_if_exists(&wal_path_of(&stale)) {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    "Failed to remove retiring DuckDB WAL file {stale}.wal during file swap: {e}"
                );
                if stale == configured_path {
                    // A foreign WAL beside the configured path would be
                    // replayed against the new file — do not place it there.
                    configured_free = false;
                }
            }
        }
    }

    let target = if configured_free {
        match std::fs::rename(generation_path, &configured_path) {
            Ok(()) => configured_path.clone(),
            Err(e) => {
                tracing::warn!(
                    "Failed to restore configured DuckDB database path {configured_path} during file swap; serving {generation_path} until restart: {e}"
                );
                generation_path.to_string()
            }
        }
    } else {
        tracing::warn!(
            "Configured DuckDB database path {configured_path} is not reclaimable; serving {generation_path} until restart"
        );
        generation_path.to_string()
    };

    pool.swap_database_file(&target)
        .context(super::DbConnectionPoolSnafu)
        .map_err(to_datafusion_error)?;

    // Other DuckDB instances that `ATTACH`ed this path resolved it to the file
    // just retired, and `ATTACH IF NOT EXISTS` never re-resolves it. Those
    // instances notice the replacement themselves on their next checkout (see
    // `DuckDBAttachments::attach_once`, which compares the attached files'
    // identities), so cross-instance reads converge on the new file rather than
    // serving pre-swap data until the process restarts.

    tracing::info!(
        "Completed DuckDB database file swap for {configured_path}: refreshed data now served from {target}"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::DuckDbConnection;
    use crate::pool::DuckDbConnectionPoolBuilder;
    use crate::write::DuckDBDataSink;
    use crate::write_settings::DuckDBWriteSettings;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::sink::DataSink;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion_table_providers_common::util::column_reference::ColumnReference;
    use datafusion_table_providers_common::util::indexes::IndexType;
    use duckdb::AccessMode;

    fn swap_dataset_definition() -> Arc<TableDefinition> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        Arc::new(
            TableDefinition::new(super::super::RelationName::new("swap_ds"), schema).with_indexes(
                vec![(
                    ColumnReference::try_from("id").expect("valid column ref"),
                    IndexType::Enabled,
                )],
            ),
        )
    }

    fn rows_batch(
        definition: &Arc<TableDefinition>,
        rows: &[(i64, &str)],
    ) -> arrow::array::RecordBatch {
        arrow::array::RecordBatch::try_new(
            definition.schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, name)| *name).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("record batch")
    }

    async fn overwrite_with_swap(
        pool: &Arc<DuckDbConnectionPool>,
        definition: &Arc<TableDefinition>,
        rows: &[(i64, &str)],
    ) {
        let written = try_overwrite_with_swap(pool, definition, rows)
            .await
            .expect("overwrite with file swap to succeed");
        assert_eq!(written, rows.len() as u64);
    }

    async fn try_overwrite_with_swap(
        pool: &Arc<DuckDbConnectionPool>,
        definition: &Arc<TableDefinition>,
        rows: &[(i64, &str)],
    ) -> DataFusionResult<u64> {
        let sink = DuckDBDataSink::new(
            Arc::clone(pool),
            Arc::clone(definition),
            InsertOp::Overwrite,
            None,
            definition.schema(),
        )
        .with_write_settings(DuckDBWriteSettings::default().with_overwrite_file_swap(true));

        let stream = Box::pin(
            MemoryStream::try_new(
                vec![rows_batch(definition, rows)],
                definition.schema(),
                None,
            )
            .expect("stream"),
        );

        Arc::new(sink)
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get::<usize, i64>(0))
            .expect("count query")
    }

    fn pooled_raw_connection(pool: &Arc<DuckDbConnectionPool>) -> Connection {
        let mut conn = Arc::clone(pool).connect_sync().expect("connect");
        conn.as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("duckdb connection")
            .get_underlying_conn_mut()
            .try_clone()
            .expect("clone connection")
    }

    fn swap_artifacts_in(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains(GENERATION_INFIX) || name.ends_with(WAL_SUFFIX))
            .collect()
    }

    #[tokio::test]
    async fn test_overwrite_file_swap_end_to_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join("swap_test.db")
            .to_string_lossy()
            .to_string();

        let pool = Arc::new(
            DuckDbConnectionPoolBuilder::file(&db_path)
                .with_access_mode(AccessMode::ReadWrite)
                .build()
                .expect("pool"),
        );

        // A sibling dataset sharing the file: data table, index, view, and a
        // metadata table, all of which the swap must carry over.
        {
            let conn = pooled_raw_connection(&pool);
            conn.execute_batch(
                "CREATE TABLE other_data (id BIGINT, tag VARCHAR);
                 INSERT INTO other_data VALUES (1, 'a'), (2, 'b'), (3, 'c');
                 CREATE INDEX i_other_data_id ON other_data (id);
                 CREATE VIEW other_view AS SELECT * FROM other_data;
                 CREATE TABLE provider_dataset_checkpoint (dataset_name TEXT PRIMARY KEY, created_at TIMESTAMP);
                 INSERT INTO provider_dataset_checkpoint VALUES ('other', now());",
            )
            .expect("create sibling dataset");
        }

        let definition = swap_dataset_definition();

        overwrite_with_swap(&pool, &definition, &[(1, "one"), (2, "two")]).await;

        assert_eq!(pool.physical_path().as_ref(), db_path.as_str());
        assert!(Path::new(&db_path).exists());
        assert!(
            swap_artifacts_in(dir.path()).is_empty(),
            "no generation/WAL files may remain: {:?}",
            swap_artifacts_in(dir.path())
        );

        // Hold a pre-swap connection: it must keep serving the retired file.
        let old_conn = pooled_raw_connection(&pool);

        overwrite_with_swap(&pool, &definition, &[(1, "uno"), (2, "dos"), (3, "tres")]).await;

        // Wait: old_conn was checked out after the FIRST swap, so it sees the
        // first generation (2 rows) while the pool now serves the second (3).
        assert_eq!(count(&old_conn, "SELECT COUNT(1) FROM swap_ds"), 2);
        assert_eq!(count(&old_conn, "SELECT COUNT(1) FROM other_data"), 3);

        let conn = pooled_raw_connection(&pool);
        assert_eq!(count(&conn, "SELECT COUNT(1) FROM swap_ds"), 3);
        assert_eq!(count(&conn, "SELECT COUNT(1) FROM other_data"), 3);
        assert_eq!(count(&conn, "SELECT COUNT(1) FROM other_view"), 3);
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(1) FROM provider_dataset_checkpoint WHERE dataset_name = 'other'"
            ),
            1
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(1) FROM duckdb_indexes() WHERE index_name = 'i_other_data_id'"
            ),
            1,
            "sibling dataset's index must be carried into the new file"
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(1) FROM duckdb_indexes() WHERE table_name LIKE '__data_swap_ds_%'"
            ),
            1,
            "refreshed dataset's index must exist in the new file"
        );
        // Exactly one internal generation of the refreshed dataset exists: the
        // stale pre-swap tables were left behind in the retired file.
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(1) FROM duckdb_tables() WHERE table_name LIKE '__data_swap_ds_%'"
            ),
            1
        );

        drop(old_conn);
        drop(conn);

        assert!(
            swap_artifacts_in(dir.path()).is_empty(),
            "no generation/WAL files may remain after the second swap: {:?}",
            swap_artifacts_in(dir.path())
        );
    }

    #[tokio::test]
    async fn test_overwrite_file_swap_reclaims_space() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join("reclaim_test.db")
            .to_string_lossy()
            .to_string();

        let pool = Arc::new(
            DuckDbConnectionPoolBuilder::file(&db_path)
                .with_access_mode(AccessMode::ReadWrite)
                .build()
                .expect("pool"),
        );

        // Grow the live file with incompressible data that the refresh
        // replaces (md5 output defeats compression, so the size delta is
        // dominated by the replaced rows rather than block-count noise).
        {
            let conn = pooled_raw_connection(&pool);
            conn.execute_batch(
                "CREATE TABLE swap_ds AS
                 SELECT range AS id, md5(range::VARCHAR) AS name FROM range(500000);",
            )
            .expect("bulk load");
            conn.execute("CHECKPOINT", []).expect("checkpoint");
        }
        let size_before = std::fs::metadata(&db_path).expect("metadata").len();

        let definition = swap_dataset_definition();
        overwrite_with_swap(&pool, &definition, &[(1, "tiny")]).await;

        let size_after = std::fs::metadata(&db_path).expect("metadata").len();
        assert!(
            size_after < size_before,
            "swap must reclaim space: before={size_before} after={size_after}"
        );

        let conn = pooled_raw_connection(&pool);
        assert_eq!(count(&conn, "SELECT COUNT(1) FROM swap_ds"), 1);
    }

    /// The swap is not the only mechanism that replaces the database file — a
    /// snapshot restore does too, out-of-band. If something else has replaced the
    /// file since this pool opened it, the swap must abort rather than unlink the
    /// replacement and rename its own generation over it (which would leave the
    /// pool and the configured path resolving to different data).
    #[tokio::test]
    async fn test_overwrite_file_swap_aborts_when_file_replaced_out_of_band() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("replaced.db").to_string_lossy().to_string();

        let pool = Arc::new(
            DuckDbConnectionPoolBuilder::file(&db_path)
                .with_access_mode(AccessMode::ReadWrite)
                .build()
                .expect("pool"),
        );
        assert!(
            pool.physical_file_unchanged(),
            "a freshly opened pool owns its file"
        );

        // Something else replaces the configured path with a different file,
        // exactly as a snapshot restore would.
        let replacement = dir.path().join("restored.db");
        {
            let conn = Connection::open(&replacement).expect("open replacement");
            conn.execute_batch(
                "CREATE TABLE restored_marker (v INTEGER); INSERT INTO restored_marker VALUES (7);",
            )
            .expect("seed replacement");
        }
        std::fs::rename(&replacement, &db_path).expect("replace the live file");

        assert!(
            !pool.physical_file_unchanged(),
            "the pool must notice its file was replaced"
        );

        let definition = swap_dataset_definition();
        let err = try_overwrite_with_swap(&pool, &definition, &[(1, "one")])
            .await
            .expect_err("the swap must refuse to overwrite a replaced file");
        assert!(
            err.to_string().contains("replaced by another process"),
            "unexpected error: {err}"
        );

        // The replacement survived untouched, and no swap debris was left.
        let conn = Connection::open(&db_path).expect("open replaced file");
        assert_eq!(count(&conn, "SELECT COUNT(1) FROM restored_marker"), 1);
        drop(conn);
        assert!(
            swap_artifacts_in(dir.path()).is_empty(),
            "a refused swap must not leave debris: {:?}",
            swap_artifacts_in(dir.path())
        );
    }

    /// `ATTACH` resolves a path to a file once per instance and `ATTACH IF NOT
    /// EXISTS` never re-resolves it, so an instance that attached a file which
    /// was later replaced would keep reading the retired file until the process
    /// restarted. `attach_once` must notice the replacement and re-attach.
    #[tokio::test]
    async fn test_attach_once_reattaches_replaced_database_file() {
        use crate::conn::DuckDBAttachments;

        let dir = tempfile::tempdir().expect("tempdir");
        let main_path = dir.path().join("main.db").to_string_lossy().to_string();
        let peer_path = dir.path().join("peer.db").to_string_lossy().to_string();

        // The peer database that `main` attaches, seeded with one row.
        {
            let conn = Connection::open(&peer_path).expect("open peer");
            conn.execute_batch(
                "CREATE TABLE peer_data (v INTEGER); INSERT INTO peer_data VALUES (1);",
            )
            .expect("seed peer");
        }

        let main = Connection::open(&main_path).expect("open main");
        let attachments =
            DuckDBAttachments::new("main", &[Arc::from(peer_path.as_str()) as Arc<str>]);

        attachments.attach_once(&main).expect("initial attach");
        assert_eq!(count(&main, "SELECT COUNT(1) FROM peer_data"), 1);

        // Replace the peer file with a different file holding three rows —
        // the same rename-over-the-path shape a file swap produces.
        let replacement = dir.path().join("peer_new.db");
        {
            let conn = Connection::open(&replacement).expect("open replacement peer");
            conn.execute_batch(
                "CREATE TABLE peer_data (v INTEGER); INSERT INTO peer_data VALUES (1), (2), (3);",
            )
            .expect("seed replacement peer");
        }
        std::fs::rename(&replacement, &peer_path).expect("replace peer file");

        // Without re-resolution this would still read the retired file (1 row).
        attachments
            .attach_once(&main)
            .expect("re-attach after replacement");
        assert_eq!(
            count(&main, "SELECT COUNT(1) FROM peer_data"),
            3,
            "attachment must be re-resolved to the replacement file"
        );
    }

    #[test]
    fn test_parse_generation_suffix() {
        assert_eq!(
            parse_generation_suffix("1722200000000-3"),
            Some((1_722_200_000_000, 3))
        );
        assert_eq!(parse_generation_suffix("1722200000000"), None);
        assert_eq!(parse_generation_suffix("1722200000000-3.building"), None);
        assert_eq!(parse_generation_suffix("1722200000000-3.wal"), None);
        assert_eq!(parse_generation_suffix("abc-3"), None);
    }

    #[test]
    fn test_recover_prefers_configured_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let configured = dir.path().join("test.db");
        let gen1 = dir.path().join("test.db.refresh.100-0");
        let building = dir.path().join("test.db.refresh.200-1.building");
        std::fs::write(&configured, b"configured").expect("write configured");
        std::fs::write(&gen1, b"gen").expect("write gen");
        std::fs::write(&building, b"building").expect("write building");

        let recovery =
            recover_database_file_generations(&configured.to_string_lossy()).expect("recover");

        assert!(recovery.adopted.is_none());
        assert!(configured.exists());
        assert!(!gen1.exists());
        assert!(!building.exists());
        assert_eq!(recovery.removed.len(), 2);
    }

    #[test]
    fn test_recover_adopts_newest_generation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let configured = dir.path().join("test.db");
        let gen_old = dir.path().join("test.db.refresh.100-0");
        let gen_new = dir.path().join("test.db.refresh.200-1");
        let gen_new_wal = dir.path().join("test.db.refresh.200-1.wal");
        std::fs::write(&gen_old, b"old").expect("write old gen");
        std::fs::write(&gen_new, b"new").expect("write new gen");
        std::fs::write(&gen_new_wal, b"wal").expect("write new gen wal");

        let recovery =
            recover_database_file_generations(&configured.to_string_lossy()).expect("recover");

        assert_eq!(recovery.adopted, Some(gen_new.clone()));
        assert!(configured.exists());
        assert_eq!(
            std::fs::read(&configured).expect("read adopted"),
            b"new".to_vec()
        );
        let configured_wal = dir.path().join("test.db.wal");
        assert!(configured_wal.exists(), "generation WAL adopted alongside");
        assert!(!gen_old.exists());
        assert!(!gen_new.exists());
        assert!(!gen_new_wal.exists());
    }

    #[test]
    fn test_recover_noop_when_nothing_to_do() {
        let dir = tempfile::tempdir().expect("tempdir");
        let configured = dir.path().join("test.db");
        std::fs::write(&configured, b"configured").expect("write configured");

        let recovery =
            recover_database_file_generations(&configured.to_string_lossy()).expect("recover");
        assert!(recovery.adopted.is_none());
        assert!(recovery.removed.is_empty());
    }
}
