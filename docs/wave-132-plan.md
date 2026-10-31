# Wave 132 — DuckLake interop

Planned 2026-10-31, the second wave of the QuackLab track's stream B. QuackLab
keeps its warehouse in DuckLake: a Postgres catalog (Neon) and Parquet data in
R2, written by DuckDB. pinkdb has to open that lake as it is (the same catalog,
the same files, DuckDB's attach syntax and secrets) before it can replace
DuckDB under QuackLab, and DuckDB has to keep reading what pinkdb writes.

## What exists

- **pink-lake's catalog already uses DuckLake's tables** (wave 103,
  `crates/pink-lake/src/metadata/`), with pink-only facts in `pinkdb_*` side
  tables, and reads DuckDB-created catalogs through `Lake::open_with_catalog`
  (the checked-in fixture in `pink-lake-parquet/tests/ducklake_interop.rs`).
- **Stock DuckDB's catalogs do not open.** Probed 2026-10-31 with DuckDB
  v2.0.0-alpha46057 writing a SQLite catalog (inlined rows, a 5,000-row
  Parquet file, a `DELETE`, an `UPDATE`):
  1. it writes `ducklake_metadata.version = '1.1-dev1'`; pink-lake accepts
     only `1.1` (`metadata/store.rs` `has_ducklake_tables`) and reports
     "format_version 1 … written by an older pinkdb", which is wrong twice;
  2. with the version patched, file paths resolve as `<data>/data/main/t/…`:
     pink-lake assumes its own layout instead of DuckLake's rule (data path,
     then the schema's `path`, then the table's `path`, then the file's
     `path`, each joined only when its `path_is_relative` is true);
  3. DuckDB's delete files are Parquet (`file_path`, `pos`); pink-lake reads
     only its own `PINKDL1` binary delete files (`delete_file.rs`) and names
     a Parquet one unsupported.
- **Versions.** In the DuckLake source the port QuackLab runs
  (`~/Desktop/projects/ducklake-vortex/ducklake`,
  `common/ducklake_version.hpp`), `1.1`, `1.2` and `1.3` are that port's own
  versions: the `1.1-dev1` table shape plus its materialized-view and
  procedure tables. pinkdb's `DUCKLAKE_VERSION = "1.1"` therefore claims the
  port's version without its tables, and stock DuckDB rejects it
  ("Unsupported ducklake_version '1.1'"), which is why the interop gate has
  failed on this machine since 10-29.
- **SQL surface.** `ATTACH` takes `lake:` or `TYPE LAKE`; `ducklake:` is
  refused. Postgres catalogs are `postgres://` URLs; DuckDB writes libpq
  conninfo (`ducklake:postgres:dbname=… host=…`). There is no `CREATE SECRET`;
  S3 credentials come from `AWS_*` environment variables. DuckLake's
  functions exist only as CLI dot-commands (`crates/pinkdb-cli/src/lake.rs`:
  flush inlined data, table changes, list files, table info, options, …).
- **Schemas.** A lake database has one schema (wave 124); several DuckLake
  schemas per lake are stream A's wave 131d, which reworks
  `crates/pinkdb/src/lake_db.rs` and `pink-lake/src/database.rs`.

## Decisions

1. **Read every catalog with the `1.1-dev1` table shape**: versions
   `1.1-dev1`, `1.1`, `1.2`, `1.3`. Tables a version adds that pinkdb does not
   use (the port's materialized views and procedures until wave 137) are
   ignored on read. An unknown version is refused with the DuckLake version
   string in the message ("DuckLake catalog version '2.0' is not supported;
   this build reads 1.1-dev1 … 1.3"), never a pinkdb format number.
2. **New catalogs are written as `1.1-dev1`**, the version stock DuckDB reads,
   so DuckDB attaches what pinkdb creates. 137 moves pinkdb to the port's
   version when it adds that version's tables. No legacy reader for catalogs
   written by earlier pinkdb builds.
3. **DuckLake's path rule**, ported from `ducklake_metadata_manager.cpp`
   (`FromRelativePath` for schemas, tables, data and delete files): data path
   = the `DATA_PATH` option, else `ducklake_metadata.data_path`; each level is
   joined to its parent only when `path_is_relative`. pinkdb writes the same
   layout (`<data>/<schema>/<table>/ducklake-<uuid>.parquet`, relative
   paths).
4. **Parquet delete files.** Read DuckLake's `file_path`/`pos` Parquet delete
   files (positions within the named data file) in 132a. Writing them, and
   deleting the `PINKDL1` container, is wave 134 (which owns the write path).
   Deletion vectors (`write_deletion_vectors`, Puffin) are refused by name.
5. **`ATTACH 'ducklake:<metadata>' AS x (…)`** with DuckDB's metadata forms:
   `sqlite:<path>`, `postgres:<libpq conninfo>` (a port of the key=value
   grammar; a `password=` key is refused like a password in a URL) and
   `postgres://…`. A bare `<file>.ducklake` (a DuckDB database file) is
   refused: pinkdb does not read DuckDB's file format. Options: `DATA_PATH`,
   `READ_ONLY`, `SNAPSHOT_VERSION`, `SNAPSHOT_TIME`, `METADATA_SCHEMA`,
   `CREATE_IF_NOT_EXISTS`, `DATA_INLINING_ROW_LIMIT`; any other DuckLake option
   is refused by name, never ignored. It maps onto the wave-124 lake
   database (`LakeSpec`), so the WAL, checkpoint footer and `SHOW DATABASES`
   carry it unchanged.
6. **Secrets.** `CREATE [OR REPLACE] [PERSISTENT | TEMPORARY] SECRET [IF NOT
   EXISTS] [name] (TYPE s3 | r2 | gcs, KEY_ID, SECRET, SESSION_TOKEN, REGION,
   ENDPOINT, URL_STYLE, USE_SSL, ACCOUNT_ID, SCOPE)`, `DROP [PERSISTENT |
   TEMPORARY] SECRET [IF EXISTS] name`, and `duckdb_secrets()` with
   redacted values. `r2` derives the endpoint
   `<account_id>.r2.cloudflarestorage.com` with path-style URLs, like DuckDB.
   A `TEMPORARY` secret (the default, as in DuckDB) lives in the session; a
   `PERSISTENT` one lives in the store. Secret values never reach statement
   text: the WAL logs a redacted statement plus the values in a separate
   record section, `SHOW`/history/profiles/errors print `redacted`, and the
   checkpoint keeps them in their own footer section (encrypted when the
   store is). Lake I/O and `read_csv`/`read_parquet` pick a secret by
   longest `SCOPE` prefix match, then fall back to `AWS_*`.
7. **DuckLake functions as SQL**, over the attached lake by name:
   `ducklake_snapshots(db)`, `ducklake_current_snapshot(db)`,
   `ducklake_table_info(db)`, `ducklake_list_files(db, table [, schema])`,
   `ducklake_table_changes(db, schema, table, start, end)`,
   `ducklake_table_insertions` / `deletions`, `ducklake_options(db)`, and
   `CALL ducklake_flush_inlined_data(db [, schema_name, table_name])`,
   `CALL ducklake_expire_snapshots(…)`, `CALL ducklake_merge_adjacent_files(…)`,
   `CALL ducklake_cleanup_old_files(…)`. The implementations move from the
   CLI's dot-commands into pink-lake behind one host hook; the dot-commands
   that duplicate them go.
8. **Interop tests, both directions, on QuackLab's real shapes.** The DuckDB
   CLI writes a lake into a throwaway Postgres (port 55433) with data in mock
   S3 (`pink-lake`'s `mock_s3`, or RustFS in the bench): two schemas, inlined
   rows, Parquet files, deletes, an update, a `MERGE`, `ALTER TABLE ADD
   COLUMN`. pinkdb attaches it and every snapshot's rows equal DuckDB's. Then
   pinkdb writes and DuckDB reads. Tests skip with a printed note when the
   DuckDB binary or Postgres binaries are absent.

## Briefs (pi)

- **132a — read DuckDB's lakes** (pink-lake and pink-lake-parquet only):
  decisions 1–4 (reading side) and the read half of 8 at the Rust API. No
  engine or SQL changes, so it runs alongside stream A's 131.
- **132b — `ATTACH 'ducklake:…'` and secrets** (parser, binder, storage,
  lake opener, httpfs/S3 configuration): decisions 5–6. Touches the parser
  and `lake_db.rs` after stream A's 131b/131d land, with the parent
  coordinating name resolution.
- **132c — the DuckLake functions** (decision 7) and the write half of 8.
- After 131d: attach multi-schema QuackLab catalogs; the benchmark's lake path
  moves from "blocked by 132" to emitted.

## Measured

To fill in: the benchmark's lake path before and after (wave 130 baseline:
pinkdb's emitted lake path is blocked by 132; workaround lake numbers in
`docs/wave-130-plan.md`).
