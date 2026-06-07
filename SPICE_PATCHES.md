# Spice patches — `spiceai-53` line audit (v0.11.0)

This document records the rigorous per-patch audit of the Spice-specific changes
carried on the previous `spiceai-52` line, verified against the upstream
`v0.11.0` tag (`15d4d415611bb3f19db5f2cd705ed752a10923ab`), which is the base of
the `spiceai-53` / `spiceai-53-patches` branches.

## TL;DR (audit verdict)

**`spiceai-53` == `spiceai-53-patches` == `v0.11.0` (`15d4d41`), and this is a
correct, sufficient pin for the consuming codebase's DataFusion-53 line.** No Spice
patch needs to be ported on top of v0.11.0.

`v0.11.0` is a *pure upstream tag* (first-parent ancestor of
`datafusion-contrib/main`; zero Spice-specific commits by SHA). Of the Spice
patches that lived on the old `spiceai-52` line, every one falls into one of three
buckets, none of which blocks the v0.11.0 pin:

1. **UPSTREAMED** — adopted by upstream and present in v0.11.0 (the large majority:
   Postgres `XID`/numeric/JSON decoding, DuckDB `AccessMode`/creator, MongoDB
   provider, `ns_lookup` hickory `TokioResolver`, `on_conflict`, `TableArgReplace`,
   connection-pool knobs, all `federation.rs` files, comprehensive pushdown, …).
2. **Re-homed in the consuming codebase** — upstream dropped the Spice helper, and
   the consumer now owns it locally (so it no longer depends on table-providers for
   it):
   - `util/supported_functions.rs` → consumer's own `function_support` module
     (`FunctionSupport` / `FunctionRestriction`).
   - `util::schema::merge_inferred_and_declared_schemas` → consumer's own
     `schema_discovery` module.
   - `util::constraints::UpsertOptions` → consumer's own acceleration types.
3. **Refactored away** — the Spice-only `SqlTable` programming model that upstream
   never adopted (`expr::Engine` enum, the `engine` constructor arg, the
   `with_allow_physical_filter_pushdown` builder, `MysqlZeroDateBehavior`). The
   consumer's DF53 line migrated its call sites to the upstream v0.11.0 API
   (3-arg `SqlTable::new`, Unparser/`default_filter_pushdown`-based pushdown,
   inline `0000-00-00` handling) and imports **none** of these symbols from
   table-providers anymore.

A full-tree sweep of the consuming codebase's DF53 branch confirms **zero**
`datafusion_table_providers::` imports of any symbol that v0.11.0 dropped.

> History note: the previous (DF52) line of the consuming codebase *did* import
> several of these symbols from table-providers (e.g. `expr::Engine`,
> `util::supported_functions`, `util::constraints::UpsertOptions`,
> `MysqlZeroDateBehavior`, 4-arg `SqlTable::new`). Those imports are what made the
> old pin (`spiceai-52`, `4547bee`) depend on the Spice `SqlTable` model. The DF53
> migration removed all of them, which is precisely why v0.11.0 is a clean pin.

## Method (tip-tree content verification, not SHA reachability)

SHA-range comparison is misleading here. `git log v0.11.0..<old-spice-tip>` lists
~205 commits, but most are upstream PRs that reached the old line via a *different*
merge topology and are content-identical to code already in v0.11.0. Comparing by
SHA would both (a) over-report upstream PRs as "Spice patches" and (b) risk missing
silently-dropped Spice code. Instead, every Spice feature was verified by **symbol
and file content in the actual v0.11.0 tree** (`git grep <SHA>` /
`git show <SHA>:path`), and every MISSING result was cross-checked against the
consuming codebase's *current* DF53 imports from `datafusion_table_providers`.

### Branch topology (authoritative SHAs)

Authoritative remote is `datafusion-contrib/datafusion-table-providers` (`origin`).

| Ref | SHA | `git describe` |
|---|---|---|
| `origin/spiceai-53` | `15d4d415611bb3f19db5f2cd705ed752a10923ab` | `v0.11.0` |
| `origin/spiceai-53-patches` | `15d4d415611bb3f19db5f2cd705ed752a10923ab` (+ this doc) | `v0.11.0` |
| `origin/spiceai-52` (old base) | `4547bee1b5ab39b4b825f0c44c6026533c1f08c5` | `v0.10.1-218-g4547bee` |
| `origin/spiceai-52-patches` (old patches) | `759b567bcff9968648a9d0d3f7f535e9731dc331` | `v0.9.2-116-g759b567` |
| upstream base of `spiceai-52` | `20b4ae44bb91923fca2368bbf5ef10127da77eb0` | `v0.10.1-13-g20b4ae4` (= `v0.11.0~7`) |

Topology notes that matter for interpretation:

- `spiceai-52` (`4547bee`) is the actively-maintained old Spice tip; it sits on
  upstream ~v0.10.1 and carried the Spice patches. The consuming codebase's DF53
  branch pins **`15d4d41`** (v0.11.0); its non-DF53 branches still pin `4547bee`.
- `spiceai-52-patches` (`759b567`) is **stale/abandoned**: it sits on upstream
  **v0.9.2** and is *not* an ancestor of `4547bee`. It is **not** a meaningful
  "base→patches" pair with `spiceai-52`; the real Spice delta lived on `4547bee`.
- `v0.11.0` (`15d4d41`) is a first-parent ancestor of upstream `main` — i.e. a
  clean upstream release point with no Spice commits.

## Per-patch verdicts

Legend: **UPSTREAMED** = present in v0.11.0; **MISSING** = absent from v0.11.0.
"Disposition" = how the consuming codebase's DF53 line handles a MISSING item
(re-homed locally / refactored to upstream API) — in all cases it no longer imports
the symbol from table-providers, so v0.11.0 is sufficient.

### Spice `SqlTable` programming model — MISSING, refactored away on DF53

| Patch / symbol | Verdict | Evidence / DF53 disposition |
|---|---|---|
| `expr::Engine` enum (`Engine::DuckDB/Postgres/.../ODBC`) | **MISSING** | v0.11.0 `expr.rs` is 68 lines (only `expr_contains_subquery`); 0 hits for `enum Engine`. Upstream `expr.rs` never carried the Engine model (last touched by #370, DF48). **DF53:** consumer no longer imports `expr::Engine` (0 refs); `SqlTable::new` called with 3 args. |
| `expr::to_sql` + `Expr`→SQL helpers (~690-line `expr.rs`) | **MISSING** | Gutted in v0.11.0. Upstream pushdown uses `Unparser` + `default_filter_pushdown` (`mod.rs:235`). **DF53:** consumer relies on the upstream pushdown path. |
| `SqlTable` 4-arg `new` / 5-arg `new_with_schema` (`engine` param) | **MISSING** | v0.11.0 `SqlTable::new` is 3-arg, `new_with_schema` is 4-arg, no `engine` field. **DF53:** all consumer call sites updated to the 3/4-arg upstream form. |
| `SqlTable::with_constraints` / `with_constraints_opt` | **MISSING** | Not on v0.11.0 `SqlTable`. **DF53:** consumer applies constraints via its own provider types, not this `SqlTable`. |
| `SqlTable::with_function_support` builder | **MISSING** | Not on v0.11.0 `SqlTable`. **DF53:** `with_function_support` exists only on the consumer's own factory types, backed by the consumer's local `function_support` module. |
| `SqlTable::with_allow_physical_filter_pushdown` | **MISSING** | Not on v0.11.0 `SqlTable`/`SqlExec`. **DF53:** 0 consumer references. |

### `util/supported_functions.rs` — MISSING, re-homed on DF53

| Patch / symbol | Verdict | Evidence / DF53 disposition |
|---|---|---|
| `util::supported_functions` (`FunctionSupport`, `FunctionRestriction`) | **MISSING** | File (359 lines on `4547bee`) absent in v0.11.0; 0 hits. **DF53:** re-homed to the consumer's own `data_components::function_support` module; no table-providers import remains. |

### `util` helpers

| Patch / symbol | Verdict | Evidence / DF53 disposition |
|---|---|---|
| `util::constraints::UpsertOptions` (cayenne) | **MISSING** | 0 hits in v0.11.0 `util/constraints.rs` (exposes only `Error` + `get_primary_keys_from_constraints`). Originated as old commit `69eb05e`. **DF53:** re-homed to the consumer's own `dataaccelerator::UpsertOptions`. |
| `util::schema::merge_inferred_and_declared_schemas` | **MISSING** | File `util/schema.rs` present but fn absent; 0 hits. **DF53:** re-homed to the consumer's own `data_components::schema_discovery`. |
| `util::table_arg_replace::TableArgReplace` | **UPSTREAMED** | `core/src/util/table_arg_replace.rs` present (`struct TableArgReplace`, `impl VisitorMut`). |
| `util::ns_lookup` `TokioResolver` + `verify_ns_lookup_and_tcp_connect` (hickory) | **UPSTREAMED** | `ns_lookup.rs:4` `use hickory_resolver::TokioResolver`; `:62` `verify_ns_lookup_and_tcp_connect`. (#494.) |
| `util::on_conflict` module | **UPSTREAMED** | `core/src/util/on_conflict.rs` present. |

### MySQL

| Patch / symbol | Verdict | Evidence / DF53 disposition |
|---|---|---|
| `arrow_sql_gen::mysql::MysqlZeroDateBehavior` enum | **MISSING** | 0 hits in v0.11.0. v0.11.0 `mysql.rs` handles `0000-00-00` inline (lines 471/539) but does not export the enum. **DF53:** 0 consumer references — the inline upstream handling suffices. |
| MySQL session charset variables (`character_set_*`) | **UPSTREAMED** | #334; present upstream. |
| MySQL session time-zone override | **UPSTREAMED** | #387; present upstream. |

### PostgreSQL — UPSTREAMED

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `XidFromSql` / `Type::XID` decoding | **UPSTREAMED** | `arrow_sql_gen/postgres.rs:303,1352-1363`. |
| Numeric / `BigDecimal` / `Decimal128` decoding | **UPSTREAMED** | `arrow_sql_gen/postgres.rs:9,20,72-75`. (#443.) |
| `json`/`jsonb` + JSON `List<Struct>` decoding (Utf8) | **UPSTREAMED** | `arrow_sql_gen/postgres.rs:19,91-96`. (#317.) |
| Postgres `Name` type (`Type::NAME`) | **UPSTREAMED** | Present in v0.11.0. (#441.) |
| `connection_pool_min_idle` | **UPSTREAMED** | Present (2 files). (#451.) |
| inline PEM `sslrootcert` | **UPSTREAMED** | Present in v0.11.0 `postgrespool.rs`. (#634.) |
| Redshift schema inference | **MISSING (not consumer-relevant)** | 0 hits in v0.11.0; consumer's DF53 line has no table-providers reference to it. (Old #409 / `cc38905`.) |

### DuckDB

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `AccessMode` + `DuckDBTableProviderFactory::new(AccessMode)` | **UPSTREAMED** | `duckdb.rs:35,180,200,308-310,709`. |
| `creator.rs` table creator | **UPSTREAMED** | `core/src/duckdb/creator.rs` present. |
| `connection_pool_size` | **UPSTREAMED** | Present. (#473.) |
| predefined schemas / public `TableManager` | **UPSTREAMED** | #455; present. |
| `recompute_statistics_on_write` | **MISSING (not consumer-relevant)** | 0 hits; no table-providers reference in the consumer. (#475.) |
| `invalidate_instance` (snapshot cache invalidation) | **MISSING (not consumer-relevant)** | 0 hits; no table-providers reference in the consumer. (#635.) |

### SQLite

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| Prepared-statement batch insert | **UPSTREAMED** | Present in v0.11.0 `sqlite/write.rs`. (#452.) |
| Decimal/Date32/64 round-trip, String-list→JSON, decimal-ext decoding | **UPSTREAMED** | #504/#510/#517; present. |
| `decimal_cmp` instead of `BETWEEN` (old `between.rs`, 620 lines) | **MISSING (not consumer-relevant)** | `between.rs` absent in v0.11.0; 0 `decimal_cmp` hits; no consumer reference. (#333.) |
| `rusqlite` v0.37 / `tokio-rusqlite` v0.7 | **UPSTREAMED** | #495; reflected in deps. |

### MongoDB — UPSTREAMED

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| Read-only MongoDB provider (`MongoDBTableFactory`, `MongoDBTable`) | **UPSTREAMED** | `core/src/mongodb.rs:65`, `mongodb/table.rs:26`. |
| SRV connection support | **UPSTREAMED** | Present in v0.11.0 `mongodb/connection.rs`. |
| Schema sort/merge, utils (`expression`, `unnest`, `arrow`, `schema`) | **UPSTREAMED** | Full `mongodb/utils/*` tree present. |

### Federation files — UPSTREAMED

| Patch / symbol | Verdict | Evidence |
|---|---|---|
| `*/federation.rs` (clickhouse, duckdb, mysql, sqlite, sql_provider_datafusion) | **UPSTREAMED** | All five present in v0.11.0. (The consumer additionally owns a local `create_spice_federated_table_provider` helper, unaffected.) |
| ADBC federation | **UPSTREAMED** | Present in v0.11.0 adbc module. (#584.) |
| `default_filter_pushdown` (comprehensive pushdown #615) | **UPSTREAMED** | `sql_provider_datafusion/mod.rs:235`. |

## Conclusion

`spiceai-53-patches` carries no code patches over `v0.11.0` — only this audit
document — and that is correct: there are no outstanding Spice patches that need to
ride on top of v0.11.0 for the DataFusion-53 line. The consuming codebase's DF53
pin to `15d4d41` is sufficient and does not require moving to a patched SHA.
