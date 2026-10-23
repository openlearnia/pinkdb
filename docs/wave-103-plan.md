# Wave 103 — the lake catalog becomes a database, DuckLake-style (plan)

Started 2026-10-23. Decided with the user after wave 96a: pink-lake's catalog
follows DuckLake. The catalog is always a database (a pinkdb `.pink`
database file by default, SQLite or Postgres), holding normalized tables
with `begin_snapshot`/`end_snapshot` on every row. A commit inserts its
delta; nothing is rewritten in full. The JSON catalog layout (`lake.json`,
`catalog.json`, `highest.json`, `snapshots/`, `commits/`, `replication/`
as objects) is deleted outright (no-legacy rule). Data files stay on local
disk or S3 exactly as today.

Reference: DuckLake's schema and commit loop in
`~/Desktop/projects/ducklake-vortex/ducklake/src/storage/ducklake_metadata_manager.cpp`
(tables, `INSERT INTO ducklake_snapshot` as the commit point,
`UPDATE … SET end_snapshot = {SNAPSHOT_ID} WHERE end_snapshot IS NULL`),
`metadata_manager/ducklake_metadata_manager_v1_1.cpp` (v1.1 columns) and
`storage/ducklake_transaction.cpp` (`RunCommitLoop`, `RetryOnError`).

## Why

Measured on TPC-H SF1 (wave 96a): a JSON catalog entry costs ~1.8 KB per
data file (16 columns of stats, path/table/rows repeated), the whole catalog
is written twice per commit (`catalog.json` + `snapshots/<v>.json`) and read
in full on every open. At 10,000 files that is ~18 MB rewritten for a
one-row insert. DuckLake's rows scale with the delta, and its `count(*)` is
one `SUM(record_count)` query.

## Decisions

1. **The catalog is a database, never objects.** Backends:
   - `pink:<path>` (and a bare path): a durable pinkdb `.pink` database —
     the DuckDB-file analogue. Cross-process writers already serialize on
     its write lease; `BEGIN..COMMIT` gives snapshot isolation and a
     retryable write conflict (README "Concurrency"). Local only, like a
     DuckDB file; multi-machine lakes use Postgres (or pinkdb-server, 96b).
   - `sqlite:<path>` and `postgres://…` (the 96a connection, URL/TLS,
     redaction and test-harness code carries over; the blob tables go).
   - Default data location when none is given: `<catalog path>.files/`
     (DuckLake's `DATA_PATH` default). The data location is stored in the
     catalog's metadata table; open needs only the catalog location.
2. **Schema = DuckLake v1.1's, exactly (decided with the user: interop).**
   The same `ducklake_*` table names, columns and semantics as DuckLake
   v1.1 (`ducklake_metadata`, `_snapshot`, `_snapshot_changes`, `_schema`,
   `_table`, `_column`, `_data_file`, `_delete_file`, `_file_column_stats`,
   `_table_stats`, `_table_column_stats`, `_partition_info`,
   `_partition_column`, `_file_partition_value`, `_inlined_data_tables` +
   the typed `ducklake_inlined_data_<table_id>_<schema_version>` tables,
   `_name_mapping`/`_column_mapping`, `_files_scheduled_for_deletion`, …),
   with DuckLake's id allocation (`next_catalog_id`, `next_file_id` on the
   snapshot row), `snapshot_changes.changes_made` strings, stats value
   encoding and relative-path rules — so DuckDB's `ducklake` extension can
   attach a pinkdb lake whose catalog is SQLite/Postgres (and a DuckLake
   created by DuckDB opens in pinkdb). pink-only facts (`.pink` footer
   offset/length/CRC, our file format id `pink`) go in columns/tables
   DuckLake does not read: `ducklake_data_file.footer_size` is DuckLake's
   own column, the rest in a `pinkdb_data_file` side table keyed by
   `data_file_id`. `file_format` is `parquet` / `vortex` / `pink`; DuckDB
   reads only the first two, so interop holds for Parquet (and Vortex with
   the extension) lakes. A conformance test opens a pinkdb-written SQLite
   catalog with DuckDB (`/tmp/duckdb-2.0.0/duckdb`, if present) and
   compares `count(*)`, a filtered query and a time-travel query; and opens
   a DuckDB-written DuckLake in pinkdb. Mapping from today's structs:
   `Catalog` → `ducklake_snapshot` + `_snapshot_changes`; `TableEntry` →
   `_table`/`_column`/`_table_stats`; `FileRef` + `StatsBlock` →
   `_data_file` + `_file_column_stats`; `DeleteRef` → `_delete_file`;
   `InlinedRow` → the typed inlined tables (their `begin`/`end` already are
   begin/end snapshots); `RenameEntry` → `_column_mapping`/`_name_mapping`;
   `CommitManifest`/`TableDelta` → `_snapshot_changes`. Where pink-lake's
   model differs from DuckLake's (e.g. its `row_id`/`next_row_id`
   assignment, delete-file format), DuckLake's rules win: the new catalog
   adopts them.
3. **One SQL layer, three dialects.** A `MetadataStore` trait with typed
   operations replaces the 96a object-path `CatalogStore`:
   `load(version) -> Catalog` (assembled by queries filtered on
   `begin_snapshot <= v AND (end_snapshot IS NULL OR end_snapshot > v)`),
   `commit(base, delta) -> Result<u64>`, `snapshots()`,
   `changes(from, to)`, `expire(..)`, `files_to_delete(..)`, replication
   state get/put, `table_stats(table)`. The SQL text is generated once
   with a small dialect enum (BLOB/BYTEA, upsert syntax, `BEGIN IMMEDIATE`
   vs `BEGIN` + advisory lock vs pinkdb `BEGIN`). The in-memory `Catalog`
   stays the engine-facing model, so attach/scan/stats code changes little.
4. **Commit = one database transaction, DuckLake's loop.** Inside it: read
   the latest snapshot id; if it moved past `base`, read
   `snapshot_changes` since `base` and run the existing conflict rules
   (rebase = re-plan the delta, or the existing `Conflict` error); insert
   snapshot `v+1` (primary key — the commit point; a PK/unique/"conflict"
   error retries, with DuckLake's retry count/backoff settings), insert new
   rows with `begin_snapshot = v+1`, set `end_snapshot = v+1` on removed
   rows, update `table_stats`. Data files are written before the
   transaction opens (an abandoned attempt leaves only orphans, which
   `delete_orphaned_files` removes).
5. **Reads.** Attach loads only the requested snapshot's rows; `count(*)`
   is `SUM(record_count) - deletes + live inlined rows` from the catalog
   (DuckLake's `GetNetDataFileRowCountForStats`); time travel is the same
   query at another snapshot; CDC reads `snapshot_changes` + the begin/end
   rows between two snapshots; `versions`/`snapshots` list the snapshot
   table.
6. **Maintenance.** `expire_snapshots` deletes snapshot rows and the rows
   whose `end_snapshot` is at or below the oldest kept snapshot, moving
   their files into `files_scheduled_for_deletion`;
   `cleanup_old_files` deletes scheduled files past retention;
   `delete_orphaned_files` lists the data location against `data_file` /
   `delete_file`. The `lake-data.json` ownership marker (96a) stays.
7. **Encryption like DuckLake.** Metadata rows are plaintext in the
   catalog database. Each data/delete file gets a random 256-bit key stored
   in its row's `encryption_key` column (DuckLake semantics: protect the
   catalog database itself). The passphrase-derived lake key, sealed
   manifests, `lake.json` identity binding and `highest.json` go away.
8. **Deleted** (in T2, after the new path passes the suites): the JSON
   catalog layout and everything that exists only for it — metadata
   families in `Storage`, `SplitStorage`, the 96a blob stores,
   `seal_manifest`/manifest crypto, `highest.json`/rollback marker,
   `lake.json` identity, degraded check-then-put fallback,
   `reconcile_snapshot`, S3 conditional writes for metadata (data files
   never needed them), `moved.json` and `.lake migrate-catalog`. Old lakes
   are not converted: re-save them (no-legacy rule). `Storage` shrinks to
   the data operations.
9. **CLI.** `.lake save <catalog> [data <dir|s3://…>]`,
   `.lake open|attach <catalog> [data <loc>]`; `<catalog>` is `pink:<path>`,
   a bare path, `sqlite:<path>` or `postgres://…`. Features: the `.pink`
   catalog needs nothing new (pinkdb is already a dependency);
   `catalog-sqlite`/`catalog-postgres` stay opt-in.
10. **Out of scope:** DuckLake features pink-lake does not have yet
    (RBAC/grants, macros, procedures, materialized views, variant stats,
    sort info) — their tables are created empty so DuckDB attaches cleanly;
    multi-lake catalogs; MySQL. Delete files: DuckLake's are Parquet files
    of `(file_path, pos)`; adopting that format is part of interop (T2).

## Tasks

- **Bench baseline (here, first):** before deleting the JSON layout, measure
  open, attach + `count(*)`, a one-row commit and expire at 1k / 10k
  files (local and R2 data), so the new catalog has numbers to beat.
- **T1 (delegated):** `MetadataStore` + schema + dialect layer; the `.pink`
  and SQLite backends; create/open/commit/rebase/time travel/attach/
  `count(*)`/CDC/versions on the new tables; CLI save/open/attach; the lake
  test suites ported to run against the `.pink` and SQLite catalogs.
- **T2 (delegated, after T1 lands):** Postgres on the new tables;
  maintenance (expire/cleanup/orphans), replication state, per-file
  encryption keys; delete everything in decision 8; CI matrix; docs
  (README lake section, website lake page, ROADMAP row, CHANGELOG +
  website regen).
- **Bench (here):** the baseline again + DuckLake on the same R2 data.

### Storage-growth targets (measured 2026-10-23, local disk)

1,000 one-row commits into one 16-column table, one data file per commit
(harness: `/tmp/w96a/scale/bench.py`; DuckLake run with DuckDB 2.0,
`data_inlining_row_limit = 0`, SQLite catalog):

| | catalog storage | per commit at 1,000 files | attach + `count(*)` |
|---|---|---|---|
| JSON catalog (before) | 3.77 MB `catalog.json` + 1,884 MB `snapshots/` | 93 ms (36 ms at 1 file) | 49 ms |
| DuckLake | 0.95 MB `cat.db` (1,000 data-file rows, 16,000 stats rows) | ~7.7 ms, flat | — |

Gate for T1a review: with a SQLite catalog, total catalog storage after
1,000 commits is **≤ 2 MB**, it grows linearly with files (never with
files × commits), and commit time at 1,000 files is within 1.5× of commit
time at 10 files.

T1a measured (same harness, SQLite catalog, checkpointed):

| | catalog storage | one-row commit, 10 → 1,000 | attach + `count(*)` |
|---|---|---|---|
| T1a, inlining off (one file per commit) | 1.37 MB | 10 → 11 ms | 56 ms |
| T1a, inlining on (default) | 0.67 MB | 1 → 7 ms | 9.8 ms |

Inlined rows live only in the typed `ducklake_inlined_data_*` tables.

**Carried into T1b:** with inlining on, a commit still scans every inlined
row of the table. `write_inlined` builds a set of all base rows to find the
new ones, and `end_inlined` walks all base rows to find the ended ones
(`metadata/sqlite.rs`). That is why commits go from 1 ms to 7 ms. Fix: new rows are the
ones with `row_id >= base.next_row_id` (the table's row-id high-water
mark), and ended rows come from the delta, not a diff of the full lists.
Gate: inlining-on commit time at 1,000 rows within 1.5× of 10 rows.

T1b measured (`/tmp/w96a/scale/bench4.py`, 1,000 one-row commits,
checkpointed; attach + `count(*)` is one process start, median of 5):

| catalog | inlining | one-row commit, 10 → 1,000 | catalog at 1,000 | attach + `count(*)`, 10 → 1,000 |
|---|---|---|---|---|
| `sqlite:` | on | 0–1 → 1 ms | 0.67 MB | 3.9 → 10.2 ms |
| `sqlite:` | off | 9 → 10 ms | 1.37 MB | 5.0 → 39.8 ms |
| `pink:` | on | 8 → 7 ms | 0.15 MB | 7.1 → 12.4 ms |
| `pink:` | off | 17–19 → 27 ms | 0.36 MB | 7.6 → 42.2 ms |

T1b also fixed a T1a bug: flush, merge, rewrite and `add_data_files` read
a table-less head on SQL catalogs and silently did nothing.

**Carried into T2:** with inlining off, a `pink:` commit grows ~10 ms per
1,000 files (1.42–1.59× across runs, at the 1.5× gate), while SQLite stays
flat. Some commit query is O(files) on pinkdb (likely a scan where SQLite
uses an index). Since T2 makes `.pink` the default catalog, gate it at
10,000 files: commit within 1.5× of the 10-file time.

## Gates

fmt; `cargo test --workspace`; pink-lake and CLI feature matrices (with
`catalog-sqlite`, `catalog-postgres` against a throwaway local cluster);
clippy on both toolchains; wasm32; default CLI never links SQLite or
Postgres; website changelog freshness.

## Rules for delegated agents

Same as wave 96a: do not commit/push/tag; mock S3 only; only a throwaway
local Postgres; `CARGO_INCREMENTAL=0`; no legacy format support; touch only
the crates and docs the task names.
