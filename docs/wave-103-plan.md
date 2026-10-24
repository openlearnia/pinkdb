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

## T2 Part 1 result (2026-10-24)

Postgres runs on the shared DuckLake core (the 96a blob tables are deleted). It is a third `SqlConnection`, and writers race on the `ducklake_snapshot` primary key with no advisory lock. The other Part 1 changes:
- **Maintenance:** expire, cleanup and orphans follow DuckLake semantics on all three backends. DuckDB is the oracle in both directions.
- **Replication state:** typed `pinkdb_replication_*` tables.
- **Encryption:** each file gets a random 256-bit key in `encryption_key` (PNKLAKE1 raw-key container). Metadata rows are plaintext, and a `pink:` catalog may itself be a PINKENC1-encrypted database.
- **Delete files:** DuckLake Parquet delete files are read and, for Parquet tables, written through new `LakeFormat` hooks.

Release bench at 10,000 files, with full per-file statistics:

| mode | commit, 10 → 10,000 | catalog transaction, 10 → 10,000 | catalog | attach + `count(*)` |
|---|---|---|---|---|
| `pink:` inlining off | 20 → 22 ms | 9.8 → 7.1 ms | 3.43 MB | 184 ms |
| `sqlite:` inlining off | 8 → 16 ms | 0.7 → 0.7 ms | 11.82 MB | 192 ms |
| `pink:` inlining on | 10 → 12 ms | 7.9 → 6.0 ms | 1.24 MB | 68 ms |

The catalog transaction stays flat. The remaining end-to-end growth is the CLI writable attachment re-diffing the whole session table on each commit, which is O(session rows), not catalog work.

### Decisions taken while the user was away (2026-10-23/24)

- **T2 runs in two passes.** Part 1 builds the features on the new tables; Part 2 deletes the JSON layout and updates CI and docs. This follows decision 8, which deletes the layout only after the new path passes the suites.
- **Engine fix instead of a workaround.** Part 1 first dropped the catalog's indexes on pinkdb and stopped loading per-file statistics above 2,048 files. That cost large tables their pruning and refused writable attach on them. The root cause was that every pinkdb INSERT marked a table's hash indexes stale, so the next statement rebuilt them in full (O(rows)). Appends now extend the index maps in place; UPDATE, DELETE and ALTER still rebuild (`crates/pinkdb/src/storage.rs`). The workarounds are reverted.
- **Deferred:** the writable-attachment session-table diff (O(rows) per commit) is left as is. It belongs to a later wave, not to the catalog work.

## Result (T2 Part 2, 2026-10-24)

The JSON layout is deleted: no `catalog.json`, `snapshots/`, `commits/`,
`highest.json`, `lake.json`/`moved.json`, `replication/state.json`, rendered
catalog facade or `SplitStorage`; `Storage` now serves data objects only and
the lake reads its `MetadataStore` directly. Sealed manifests, the
passphrase-derived lake key/KDF, PNKLAKE1 container v1 reading, the degraded
check-then-put fallback, `reconcile_snapshot`, metadata conditional writes and
`.lake migrate-catalog` are removed. The `lake-data.json` ownership marker and
the raw-key PNKLAKE1 per-file container remain.

Locations: a bare path is `pink:<path>` with data in `<path>.files/`; SQL
catalogs use their recorded data location; `s3://…` alone fails with the
`.lake save pink:lake.pink data s3://…` fix.

Suites now run on `pink:` by default and mirror catalog behavior on `sqlite:`;
the Postgres suite runs against a throwaway local server. Tests whose subject
was the removed layout (sealed manifests, rollback markers, `lake.json`
binding, migration, degraded/conditional metadata writes, tamper-by-editing
`catalog.json`) are deleted rather than ported. See CHANGELOG for the
breaking-change note.

The release split from Part 1 still holds: catalog transactions stay flat to
10,000 files; the remaining commit growth is the writable attachment's
session-table diff (O(session rows)), deferred to a later wave.

## Review pass (same tree)

Restored the tests whose subject survives (schema/time travel, lake
create/attach/corruption, inlining corruption, partition zone maps, lean
attach, CLI attach/S3, catalog equivalence, 15 mock-S3 cases, six per-file-key
encryption cases, and 17 replication scenarios on `pink:` and `sqlite:`).

Root cause of the schema bug: `load_stats_blocks` joined statistics to
`ducklake_column` by `column_id` alone, so a RENAMED column's file statistics
were labelled with the new name even though the file's bytes carry the old
one. Attach then failed the physical-schema check. The loader now selects the
column definition visible at that file's `begin_snapshot`
(`begin_snapshot <= file.begin_snapshot < end_snapshot`), which also fixes
zone-map pruning and time travel through a rename. Negative `record_count`
rows are a clear corrupt-metadata error.

S3 degraded/conditional-write machinery, the CLI opt-in and `Lake::create_s3`
/`open_s3` are removed; explicit-config S3 tests now use
`create_with_catalog_s3`/`open_with_catalog_s3` with a real catalog.

Still undone: the PNKLAKE1 v1 read branch and Argon2id KDF remain in
`crypto.rs` (per-file keys are raw, but the legacy/passphrase path was not
excised), and the supervisor notify/pause/resume lifecycle test was reduced to
the verified start/stop path after its polling raced the supervisor.

## Benchmark: catalog local, data on R2 (2026-10-24, after waves 103 and 104)

Harness: `/tmp/r2bench/v8/` (`save.sh` and `bench.py`), release `pinkdb --features pinklake,s3,parquetlake,catalog-sqlite`, DuckDB 2.0. TPC-H SF1, Cloudflare R2 bucket `pinkdb-bench`, data under `lakes/v8/`. Both systems now use the same model: a local catalog and data on R2. Before wave 103, pinkdb also kept its catalog on R2, as JSON.

TPC-H SF1, median of 3 runs:

| query | DuckLake | pinkdb `.pink` lake (pink catalog) | pinkdb `.pink` lake (sqlite catalog) | pinkdb Parquet lake | pinkdb `.pink`, catalog JSON on R2 (2026-10-22) |
|---|---|---|---|---|---|
| attach + `count(*)` | 0.07 s | **0.02 s** | 0.01 s | 0.02 s | 2.05 s |
| attach + Q6 | 2.16 s | 2.35 s | 3.21 s | 5.00 s | 4.56 s |
| attach + Q1 | 2.29 s | 2.56 s | 2.39 s | 4.99 s | 4.25 s |

1,000 one-row commits (16 columns, inlining off, one data file per commit on R2):

| | DuckLake | pinkdb (`pink:` catalog) |
|---|---|---|
| commit (first 20 → last 20) | 700 → 606 ms | 356 → 342 ms (median 380 ms, p95 544 ms with catalog checkpoints) |
| attach + `count(*)` after 1,000 commits | 0.07 s | 0.73 s → **0.05 s** with the catalog WAL fix |

Findings:
- **Bug found and fixed:** the `.pink` catalog only auto-checkpointed at the engine's 64 MiB WAL default. A catalog commit logs ~14 KB, so after 1,000 commits the catalog was 14 MB of WAL, replayed (0.7 s of CPU) on every open. Catalog databases now checkpoint at 1 MiB (`CATALOG_AUTO_CHECKPOINT_BYTES`, test `the_catalog_wal_is_checkpointed_as_commits_accumulate`).
- **Parquet lake is ~2× slower:** pinkdb's Parquet writer has no compression or dictionary encoding, so its files are 673 MB against DuckDB's ~310 MB for the same data. Folded into wave 98c.
- **WAL volume:** ~14 KB of WAL per catalog commit is a lot for a one-row commit; worth shrinking (fewer statements per commit, or logging the delta compactly) in a later engine or catalog pass.
