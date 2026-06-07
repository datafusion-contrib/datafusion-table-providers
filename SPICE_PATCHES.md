# Spice patches — `spiceai-53` line audit (v0.11.0)

This document records the rigorous per-patch audit of the Spice-specific changes
carried on the previous `spiceai-52` line, verified against the upstream
`v0.11.0` tag (`15d4d415611bb3f19db5f2cd705ed752a10923ab`), which is the base of
the new `spiceai-53` / `spiceai-53-patches` branches.

## TL;DR (audit verdict)

**The premise "all Spice patches were upstreamed into v0.11.0" is FALSE.**

`v0.11.0` (`15d4d41`) is a *pure upstream tag* — it is a first-parent ancestor of
`datafusion-contrib/main` and contains **zero** Spice-specific commits by SHA. Many
individual Spice fixes (Postgres `XID`/numeric/JSON decoding, DuckDB
`AccessMode`/creator, MongoDB provider, `ns_lookup` `TokioResolver`, the
`on_conflict` module, the `TableArgReplace` table-arg rewriter, predefined schemas,
connection-pool knobs) **were** genuinely upstreamed and are present in v0.11.0.

But a substantial, **co-dependent block of the Spice `SqlTable` programming model
was never upstreamed** and is **actively used by spice2**:

| Missing symbol (in v0.11.0) | Old location | spice2 consumers |
|---|---|---|
| `sql::sql_provider_datafusion::expr::Engine` enum | `expr.rs` (690→68 lines, gutted) | `data_components/odbc.rs` (`Engine::ODBC`), `spark_connect.rs`, `search/index/duckdb/sql.rs` |
| `SqlTable::new(.., engine)` / `new_with_schema(.., engine)` (4/5-arg) | `sql_provider_datafusion/mod.rs` | clickhouse, odbc, databricks, snowflake (×2), scylladb, runtime tests (×2) — **8 call sites** |
| `SqlTable::with_allow_physical_filter_pushdown` | `sql_provider_datafusion/mod.rs` | `data_components/scylladb/mod.rs` |
| `SqlTable::with_function_support` / `FunctionSupport` / `FunctionRestriction` (`util/supported_functions.rs`, 359 lines) | `util/supported_functions.rs` (file absent) | `runtime/datafusion/udf.rs`, `data_components/snowflake/mod.rs` — **5 imports** |
| `util::constraints::UpsertOptions` | `util/constraints.rs` | `cayenne/provider/table.rs`, `runtime/task_history`, `runtime/dataaccelerator/upsert_dedup.rs` — **4 imports** |
| `util::schema::merge_inferred_and_declared_schemas` | `util/schema.rs` (file present, fn absent) | `data_components/dynamodb/provider.rs` — **2 imports** |
| `arrow_sql_gen::mysql::MysqlZeroDateBehavior` | `arrow_sql_gen/mysql.rs` | `connector-mysql/src/lib.rs` |

**Consequence:** spice2 **cannot** move its `datafusion-table-providers` pin to bare
`v0.11.0` (`15d4d41`). It would fail to compile against the missing symbols above.
The `spiceai-53-patches` branch as it currently exists (a no-op copy of `15d4d41`)
does **not** yet carry these Spice patches and is therefore **not** a usable pin for
spice2. It requires the DF53 re-port of the Spice `SqlTable` model (the work in
progress on the spice2 DataFusion-53 upgrade branch), not a documentation-only
formalization.

> Note on a prior assumption: the earlier analysis stated that
> `merge_inferred_and_declared_schemas` had been "re-homed locally in spice2's
> `data_components/schema_discovery.rs`". That is **not** what the current spice2
> tree shows — spice2 still imports it from
> `datafusion_table_providers::util::schema` in
> `crates/data_components/src/dynamodb/provider.rs`. It is therefore a live missing
> dependency, not a closed item.

## Method (tip-tree content verification, not SHA reachability)

SHA-range comparison is misleading here: `git log v0.11.0..<old-spice-tip>` lists
~205 commits, but most are upstream PRs that reached the old line via a *different*
merge topology and are content-identical to code already in v0.11.0. Comparing by
SHA would both (a) over-report upstream PRs as "Spice patches" and (b) miss
silently-dropped Spice code. Instead, every Spice feature was verified by **symbol
and file content in the actual v0.11.0 tree** (`git grep <SHA>` / `git show <SHA>:path`),
and every MISSING result was cross-checked for spice2 relevance by grepping
`/Users/lukim/dev/spice2/crates` for imports from `datafusion_table_providers`.

### Branch topology (authoritative SHAs)

Authoritative remote is `datafusion-contrib/datafusion-table-providers` (`origin`).

| Ref | SHA | `git describe` |
|---|---|---|
| `origin/spiceai-53` | `15d4d415611bb3f19db5f2cd705ed752a10923ab` | `v0.11.0` |
| `origin/spiceai-53-patches` | `15d4d415611bb3f19db5f2cd705ed752a10923ab` | `v0.11.0` (no-op copy) |
| `origin/spiceai-52` (old base) | `4547bee1b5ab39b4b825f0c44c6026533c1f08c5` | `v0.10.1-218-g4547bee` |
| `origin/spiceai-52-patches` (old patches) | `759b567bcff9968648a9d0d3f7f535e9731dc331` | `v0.9.2-116-g759b567` |
| upstream base of `spiceai-52` | `20b4ae44bb91923fca2368bbf5ef10127da77eb0` | `v0.10.1-13-g20b4ae4` (= `v0.11.0~7`) |

Findings about the old line that matter for interpretation:

- `spiceai-52` (`4547bee`) is the actively-maintained old Spice tip; it sits on
  upstream ~v0.10.1 and carries the Spice patches.
- `spiceai-52-patches` (`759b567`) is **stale/abandoned**: it sits on upstream
  **v0.9.2** and is *not* an ancestor of `4547bee`. It is **not** a meaningful
  "base→patches" pair with `spiceai-52`; the real Spice delta lives on `4547bee`.
- **spice2's current pin is `4547bee` (spiceai-52)**, not `15d4d41`. There is no
  in-flight committed move to v0.11.0 in the spice2 tree.

## Per-patch verdicts

Legend: **UPSTREAMED** = present in v0.11.0 (upstream adopted it); **MISSING** =
absent from v0.11.0; **(spice2)** = spice2 imports/uses the symbol from
`datafusion_table_providers`, so the verdict is load-bearing for the pin.

### Spice `SqlTable` programming model — MISSING (spice2-relevant)

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `expr::Engine` enum (`Engine::DuckDB/Postgres/SQLite/MySQL/ODBC/...`) | **MISSING (spice2)** | v0.11.0 `expr.rs` is 68 lines (only `expr_contains_subquery`); 0 hits for `enum Engine` tree-wide. Upstream `expr.rs` never carried the Engine model (last touched by #370, DF48). spice2: `odbc.rs:90` `Some(Engine::ODBC)`. |
| `expr::to_sql` + `Expr`→SQL helpers (~690-line `expr.rs`) | **MISSING** | Gutted in v0.11.0 (see above). Upstream pushdown now uses `Unparser` + `default_filter_pushdown`. |
| `SqlTable` 4-arg `new` / 5-arg `new_with_schema` (the `engine` param) | **MISSING (spice2)** | v0.11.0 `SqlTable::new` is 3-arg, `new_with_schema` is 4-arg, no `engine` field. spice2: 8 call sites pass the engine arg. |
| `SqlTable::with_constraints` / `with_constraints_opt`, `constraints` field | **MISSING** | Not on v0.11.0 `SqlTable` (8 tree-wide `with_constraints` hits are on other types, e.g. `TableDefinition`). spice2 uses `with_constraints` on its own `arrow`/duckdb types, not on this `SqlTable`. |
| `SqlTable::with_function_support` (deny-list federation) | **MISSING** | Not on v0.11.0 `SqlTable`. spice2's `with_function_support` calls target spice2's own factories (`SnowflakeTableFactory`, etc.), so this builder itself is not a hard spice2 dependency — but the underlying `FunctionSupport` type is (see below). |
| `SqlTable::with_allow_physical_filter_pushdown` | **MISSING (spice2)** | Not on v0.11.0 `SqlTable`/`SqlExec`. spice2: `scylladb/mod.rs:211`. |

### `util/supported_functions.rs` — MISSING (spice2-relevant)

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `util::supported_functions` module (`FunctionSupport`, `FunctionRestriction`) | **MISSING (spice2)** | File `core/src/util/supported_functions.rs` (359 lines on `4547bee`) is **absent** in v0.11.0; 0 hits tree-wide. spice2 imports it in `runtime/datafusion/udf.rs:40` and `data_components/snowflake/mod.rs:38` (5 references). |

### `util` helpers

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `util::constraints::UpsertOptions` (cayenne) | **MISSING (spice2)** | 0 hits in v0.11.0 `util/constraints.rs` (which only exposes `Error` + `get_primary_keys_from_constraints`). spice2 imports it in 4 places incl. `cayenne/provider/table.rs:94`. Originated as old commit `69eb05e` "Add UpsertOptions struct for cayenne compatibility". |
| `util::schema::merge_inferred_and_declared_schemas` | **MISSING (spice2)** | File `util/schema.rs` present but fn absent; 0 hits tree-wide. spice2: `data_components/dynamodb/provider.rs:66,441`. |
| `util::table_arg_replace::TableArgReplace` (custom table-arg rewrite) | **UPSTREAMED** | `core/src/util/table_arg_replace.rs` present in v0.11.0 (`struct TableArgReplace`, `impl VisitorMut`). |
| `util::ns_lookup` `TokioResolver` + `verify_ns_lookup_and_tcp_connect` (hickory) | **UPSTREAMED** | `ns_lookup.rs:4` `use hickory_resolver::TokioResolver`; `:62` `verify_ns_lookup_and_tcp_connect`. (#494 migrated trust-dns→hickory.) |
| `util::on_conflict` module | **UPSTREAMED** | `core/src/util/on_conflict.rs` present. |

### MySQL

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `arrow_sql_gen::mysql::MysqlZeroDateBehavior` enum | **MISSING (spice2)** | 0 hits tree-wide. v0.11.0 `mysql.rs` *handles* `0000-00-00` inline (lines 471/539) but does **not** export the `MysqlZeroDateBehavior` enum. spice2: `connector-mysql/src/lib.rs:21`. |
| MySQL session charset variables (`character_set_*`) | **UPSTREAMED** | Originated #334; upstream MySQL connection setup carries it. |
| MySQL session time-zone override | **UPSTREAMED** | #387; present upstream. |

### PostgreSQL — UPSTREAMED

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `XidFromSql` / `Type::XID` decoding | **UPSTREAMED** | `arrow_sql_gen/postgres.rs:303,1352-1363`. |
| Numeric / `BigDecimal` / `Decimal128` decoding | **UPSTREAMED** | `arrow_sql_gen/postgres.rs:9,20,72-75` (`FailedToParseBigDecimalFromPostgres`). (#443.) |
| `json`/`jsonb` + JSON `List<Struct>` decoding (Utf8 consistency) | **UPSTREAMED** | `arrow_sql_gen/postgres.rs:19,91-96` + #317. |
| Postgres `Name` type (`Type::NAME`) | **UPSTREAMED** | Present in v0.11.0 postgres arrow gen. (#441.) |
| `connection_pool_min_idle` | **UPSTREAMED** | Present in v0.11.0 pool config (2 files). (#451.) |
| inline PEM `sslrootcert` | **UPSTREAMED** | Present in v0.11.0 `postgrespool.rs`. (#634.) |
| Redshift schema inference | **MISSING (not spice2-relevant)** | 0 hits tree-wide in v0.11.0. spice2 has **no** `datafusion_table_providers` reference to Redshift schema inference, so this is not a blocker. (Was old #409 / `cc38905`.) |

### DuckDB

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `AccessMode` + `DuckDBTableProviderFactory::new(AccessMode)` | **UPSTREAMED** | `duckdb.rs:35,180,200,308-310,709`. |
| `creator.rs` table creator | **UPSTREAMED** | `core/src/duckdb/creator.rs` present. |
| `connection_pool_size` | **UPSTREAMED** | Present in v0.11.0 `duckdbpool.rs`. (#473.) |
| predefined schemas / public `TableManager` | **UPSTREAMED** | #455; present upstream. |
| `recompute_statistics_on_write` | **MISSING (not spice2-relevant)** | 0 hits tree-wide. No `datafusion_table_providers`-scoped spice2 reference. (#475.) |
| `invalidate_instance` (snapshot cache invalidation) | **MISSING (not spice2-relevant)** | 0 hits tree-wide. No spice2 reference from table-providers. (#635.) |

### SQLite

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| Prepared-statement batch insert | **UPSTREAMED** | Present in v0.11.0 `sqlite/write.rs`. (#452.) |
| Decimal / Date32/64 round-trip, String-list→JSON, decimal extension decoding | **UPSTREAMED** | #504/#510/#517; present upstream. |
| `decimal_cmp` instead of `BETWEEN` (the old `between.rs`, 620 lines) | **MISSING (not spice2-relevant)** | `core/src/sqlite/between.rs` absent in v0.11.0; 0 `decimal_cmp` hits. No spice2 reference from table-providers. (#333.) |
| `rusqlite` v0.37 / `tokio-rusqlite` v0.7 | **UPSTREAMED** | #495; reflected in v0.11.0 deps. |

### MongoDB — UPSTREAMED

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| Read-only MongoDB provider (`MongoDBTableFactory`, `MongoDBTable`) | **UPSTREAMED** | `core/src/mongodb.rs:65`, `mongodb/table.rs:26`. |
| SRV connection support | **UPSTREAMED** | Present in v0.11.0 `mongodb/connection.rs`. |
| Schema sort/merge, utils (`expression`, `unnest`, `arrow`, `schema`) | **UPSTREAMED** | Full `mongodb/utils/*` tree present in v0.11.0. |

### Federation files — UPSTREAMED

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `*/federation.rs` (clickhouse, duckdb, mysql, sqlite, sql_provider_datafusion) | **UPSTREAMED** | All five `federation.rs` files present in v0.11.0. (spice2 additionally has its own `create_spice_federated_table_provider` helper in `data_components`, which is spice2-local and unaffected.) |
| ADBC federation | **UPSTREAMED** | Present in v0.11.0 adbc module. (#584.) |
| `default_filter_pushdown` (comprehensive pushdown #615) | **UPSTREAMED** | `sql_provider_datafusion/mod.rs:235`. |

## Action required (not a doc-only formalization)

1. The Spice `SqlTable` model + `supported_functions` + `UpsertOptions` +
   `merge_inferred_and_declared_schemas` + `MysqlZeroDateBehavior` patches must be
   **re-ported onto v0.11.0 (DF53)** and land on `spiceai-53-patches` before that
   branch can be used as a pin. This is DF53 adaptation work (the old patches are
   DF52-era), not a mechanical cherry-pick.
2. **spice2's pin must NOT move to bare `15d4d41`.** It should remain on `4547bee`
   (spiceai-52) until `spiceai-53-patches` carries the re-ported patches; the pin
   then moves to the new `spiceai-53-patches` tip (a real, patched SHA — **not**
   `15d4d41`).
