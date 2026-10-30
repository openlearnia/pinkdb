# Wave 124 — pinkdb-server owns its data (databases with storage, ATTACH, a data directory)

Planned and started 2026-10-30. The user saw that pinkdb-server picks its
storage once at startup (memory, one `.pink` with `--file`, or one lake with
`--catalog` + `--data`): `CREATE DATABASE` is only a namespace inside that
one store, so the console can create databases but never a lakehouse, and
nothing can be attached at runtime. They chose a server that persists every
database itself, with lakes as one more kind of database storage. Decided
with the user: `--data-dir` is optional and defaults to `./pinkdb-data`;
`--in-memory` stays as an explicit throwaway mode; a local write that reads
a lake pins the lake version it read in the WAL; attaching foreign `.pink`
files, multi-schema lakes, vault-backed lake secrets and zero-copy clones are
later waves. Accounts and warehouses belong to the later pink-ocean product:
pinkdb-server stays DuckDB-class.

## What exists

- **One storage per process.** `pinkdb-server` opens memory, one `.pink`
  file (`--file`, with its WAL) or one lake (`--catalog` + `--data`,
  `src/catalog.rs` `CatalogStore`). In `--catalog` mode every engine object
  lives in one catalog image in the lake and every commit publishes the
  whole store through `PinkDb::set_commit_hook`; nothing else uses the hook.
- **Databases are namespaces.** `DbCatalog` (`storage.rs`) holds tables,
  views, macros, MVs, procedures and triggers. `CREATE DATABASE` only adds a
  namespace; there is no `ATTACH` (`serve.rs` says so).
- **Lakes reach the engine only through the CLI.** `.lake attach` registers a
  session-local, read-only `AttachedLake` (`attached.rs`); `.lake attach …
  write` materializes the lake into the store and `.lake commit` diffs it
  back (`pinkdb-cli/src/lake.rs`, a process-global `WRITABLES` map). A
  durable statement that writes and reads an attached table is refused
  (`refuse_logged_attached_reads`): its WAL text could never replay.
- **The state store** (wave 121e) lives next to `--file`, or at `--state`.
  Uploads stage under `$TMPDIR`. Nothing checkpoints on exit.

## Decisions

1. **A database has a storage.** `DbCatalog.storage` is `local` (rows in
   the store, as before) or `lake` (rows and table metadata in a lake, with
   the lake's own lease and version history). It is persisted per database
   in the checkpoint footer (footer version 3; version-2 files are refused as
   written by an older pinkdb).
2. **SQL, not dot-commands.** `CREATE DATABASE n (TYPE LAKE [, CATALOG '…']
   [, DATA '…'])` creates a lake (no `CATALOG` = a managed lake under the
   store's lake directory); `ATTACH '<location>' AS n [(TYPE LAKE,
   READ_ONLY, DATA '…', VERSION k)]` opens an existing one (`lake:` prefix
   or `TYPE LAKE`; a `.pink` file is a later wave); `DETACH [DATABASE]
   [IF EXISTS] n` forgets it and never deletes lake data. All three are
   WAL-logged store-level statements (refused inside a transaction, ADMIN
   only); replay opens the lake rather than creating it. `DROP DATABASE` on
   a lake database is refused (use `DETACH`). `ATTACH` reports
   `CreatedDatabase` and `DETACH` reports `DroppedDatabase`, so no wire,
   FFI or wasm result kind changes.
3. **One schema per lake in v1.** A lake's tables must all live in one
   schema (`main` for lakes this wave creates); that schema is exposed under
   the database's name. A lake with several schemas is refused.
4. **The engine stays free of pink-lake.** A process-global
   `DatabaseOpener` registry (like `register_global_table_source`) returns a
   `LakeDatabase`: its tables, version, redacted location, read-only flag,
   a read-only `AttachedLake` view, `publish` and `tables_at(version)`. Hosts
   register pink-lake's opener before opening a store, so replay resolves.
   Read-only lakes are served by the lazy, statistics-first `LakeAttachment`
   behind an alias that renames the lake schema to the database; writable
   lakes are materialized into the store (the `--catalog` model, one
   database at a time) and hold the lake's `EngineLease` with a heartbeat.
5. **Per-database image.** A writable lake keeps its engine-only metadata
   (constraints, defaults, index definitions, views, macros, MVs,
   procedures, triggers) in its own catalog through `set_engine_catalog`,
   from `to_database_image(db)` / `load_database_image`. No RBAC: users and
   grants stay in the server's store and name the database. The whole-store
   image and its loader go away with `--catalog`.
6. **Commit routing replaces the commit hook.** After each committed unit
   the engine finds the lake databases whose content or image changed since
   their last publish (table version stamps plus the image bytes) and
   publishes each; a lake-only unit is not appended to the local WAL. A
   unit that writes more than one storage is refused: at bind from the
   plan's writes (and cumulatively inside a transaction), and again after
   execution for writes a `CALL` or trigger hid, rolling the statement (or
   the transaction) back. A failed publish restores the database's last
   published state and fails the statement. `set_commit_hook` is removed.
7. **Pinned lake reads.** A logged local write that read lake databases
   records `(database, version)` pairs in its WAL record (inside the
   record's memo section, under a name no SQL function can have, so the
   payload format is unchanged). Replay swaps in each lake's tables at the
   pinned version while the record runs. A version the lake no longer has
   is a clear replay error. This replaces `refuse_logged_attached_reads` for
   lake databases (raw session attachments from the Rust API stay refused).
8. **Unavailable lakes don't block startup.** A lake that cannot be opened
   at open or replay is listed with `status = unavailable` and the reason;
   statements on it error, every other database works.
9. **Visibility.** `SHOW DATABASES` and `pinkdb_catalog()` gain `storage`,
   `location` (passwords redacted), `read_only`, `version` and `status`.
   A catalog URL with a password is refused outright (it would land in the
   WAL and the checkpoint): credentials come from `PGPASSWORD` / `AWS_*`.
10. **CLI.** `.lake attach`, `.lake detach`, `.lake attached`, `.lake commit`
    and `WRITABLES` go; SQL replaces them. The CLI registers the opener
    (feature `pinklake`); every other `.lake` command stays.
11. **Server data directory.** `--file`, `--catalog`, `--data` and `--state`
    are removed. `--data-dir DIR` (default `./pinkdb-data`, created on first
    start) holds `pinkdb.pink` (databases, users, roles, grants, the list of
    lake databases), `state.pink` (console worksheets, vault, agent, memory),
    `lakes/` (managed lakes) and `tmp/` (upload staging). `--in-memory`
    keeps everything in memory. `CatalogStore` is deleted.
12. **Lake location policy** (server, admin-only through RBAC): local lake
    paths resolve under `<data-dir>/lakes/` or a `--lake-root DIR`
    (repeatable); `postgres://` and `s3://` are allowed; passwords inside
    URLs are refused.
13. **Graceful shutdown.** SIGINT/SIGTERM stop accepting connections, wait
    briefly for running statements, checkpoint `pinkdb.pink` and
    `state.pink`, and release the lake leases.
14. **Console.** `Catalog.databases` becomes `Vec<DatabaseInfo { name,
    storage, location, read_only, version, status }>`. The Storage page
    lists databases with storage badges and offers **New database** (local
    or lake), **Attach lake** and **Detach** to administrators; the catalog
    tree shows a storage badge per database and the reason an unavailable
    lake is down.

## Tests

Engine: `ATTACH`/`DETACH` round trip with the schema alias; a writable lake
database publishes per commit and a second writer is refused by the lease; a
durable store reopens its lake databases; a cross-storage write is refused
(also through a trigger); `INSERT INTO local SELECT FROM lake` replays
identically after the lake moved on; an unavailable lake does not block the
open; a multi-schema lake is refused; footer version 2 is refused. Server
(binary-spawn): the default data directory in a temporary working directory;
local and lake databases survive SIGTERM and a restart; `--in-memory` writes
nothing; a lake path outside `--lake-root` is refused; a non-admin `ATTACH`
is refused; `tests/catalog.rs` and `tests/common/console.rs` rewritten for
lake databases. Console: `/api/catalog` storage fields; New database, Attach
lake and Detach from the UI; a live check in the preview browser. CLI:
`ATTACH`/`DETACH` replace the removed dot-commands. Full gate (format, API,
lake and console change), with the cli-lake/lake feature matrix and the
website changelog check.
