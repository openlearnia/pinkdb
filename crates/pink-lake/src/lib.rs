//! pink-lake — the minimal lake layer for pinkdb: decoupled metadata and
//! data storage on local disk, or in an S3 bucket/prefix with the
//! non-default `s3` feature (AWS S3, Cloudflare R2, any S3-compatible
//! endpoint).
//!
//! # What a lake is
//!
//! A lake is a data directory (or, with the `s3` feature, an
//! `s3://bucket/prefix` key prefix) plus a catalog on DuckLake v1.1 tables: a
//! `pink:<path>` or `sqlite:<path>` file (a bare path means `pink:`), or a
//! PostgreSQL URL, used as given:
//!
//! ```text
//! <lake>/
//!   lake-data.json              this catalog's data-ownership marker
//!   data/<table>/<uuid>.pink    immutable per-table data files (`PINKDB06`)
//!   data/<table>/<uuid>.pinkdel positional delete files (see delete_file)
//! ```
//!
//! Every published version is a snapshot row in the catalog; its manifest (the
//! [`catalog::Catalog`] type) has this shape:
//!
//! ```json
//! {
//!   "format_version": 7,
//!   "lake_id": "0123456789abcdef0123456789abcdef",
//!   "updated": "2026-10-14T12:34:56.789Z",
//!   "version": 2,
//!   "tables": {
//!     "main.t": {
//!       "files": [
//!         { "path": "data/main.t/3f2a….pink", "rows": 5 },
//!         { "path": "data/main.t/9c01….pink", "rows": 2 }
//!       ],
//!       "deletes": [
//!         { "path": "data/main.t/77aa….pinkdel", "file": "data/main.t/3f2a….pink",
//!           "count": 1 }
//!       ],
//!       "rows": 6,
//!       "format": "pink",
//!       "table_id": "0123456789abcdef0123456789abcdef"
//!     }
//!   }
//! }
//! ```
//!
//! Wave 89 adds the optional table identity `table_id` (a random 128-bit
//! hex string assigned on creation, preserved for the same key across later
//! writes and omitted while absent; see [`Lake::table_ids`]).
//!
//! A `format_version` 4 entry is **incremental**: every data file carries its
//! own exact row count, scans union the files in listed order, and each
//! `deletes` entry names one **positional delete file** (0-based positions
//! within the one data file it targets) whose rows the reader drops
//! (merge-on-read). `rows` is the logical count and must equal
//! `sum(files.rows) - sum(deletes.count)`. An entry's `format` is `"pink"`
//! (the engine's single-table `.pink` bytes) or any other id a registered
//! provider ([`crate::format`]) supplies; every data file carries a
//! CRC-protected statistics/zone-map block in its `files` object — the exact
//! facts its encode pass collected — so aggregate fast paths and **file
//! pruning** work without decoding anything.
//!
//! # Partitioning (wave 66)
//!
//! A table may declare a **partition specification** (DuckLake's
//! `CREATE TABLE … PARTITIONED BY` / `ALTER TABLE … SET PARTITIONED BY`,
//! surfaced as `.lake partition`): a list of identity columns and/or
//! `bucket(N, column)` transforms. A commit (or save) that writes rows to a
//! partitioned table splits them into **one data file per partition value
//! set** and records each file's values (`"partition": {"region": "east"}`,
//! SQL NULL as a JSON `null`); DuckLake splits its Parquet files the same
//! way, and the values land in its own metadata rows
//! (`ducklake_file_partition_value`). The spec changes only what *new writes*
//! do — files already published keep their recorded values (and stay
//! readable without them), exactly like DuckLake's versioned partition
//! records. [`Lake::set_partition_spec`]/[`Lake::clear_partition_spec`]
//! publish a spec change as one version, and `Commit::set_partition` can set
//! a spec in the same commit that creates a table.
//!
//! # Pruned scans
//!
//! Attachment builds a per-file **zone map** (from each file's block/footer)
//! and a scan whose `WHERE` carries simple `col op literal` / `IS [NOT] NULL`
//! conjuncts skips whole data files those maps prove cannot match (see the
//! `zone` module): a query filtered to one partition value of a partitioned
//! table decodes only the matching files, and a range predicate prunes files
//! by min/max on any column. Pruning is conservative — unknown statistics
//! never prune, and a file with positional deletes is still prunable
//! (deleting rows only shrinks its value set), while the table-level
//! `COUNT`/`MIN`/`MAX` fast paths keep their wave-63 honesty rule (any
//! delete file makes the per-column facts unknown).
//!
//! Only the current catalog (`format_version` 7) is read: a manifest with
//! any other `format_version`, an entry whose `format` names a removed or
//! unknown provider, a file object without a row count, or a table whose row
//! count does not add up is a clear
//! [`LakeError::Unsupported`]/[`LakeError::Corrupt`] error, never a guess.
//!
//! New writes are engine `.pink` single-table persistence files
//! ([`PinkDb::to_pink_bytes_with_stats`]) **plus a CRC-protected statistics
//! block per file** (the exact per-column facts the same encode pass
//! collected; see the `stats` module docs). A catalog row naming the removed
//! (wave 104) data format fails with a clear re-save error; there is no reader
//! and no converter for it.
//!
//! # Data-file format providers (wave 87)
//!
//! An entry's `format` string is a **format id**, resolved through a
//! process-global registry ([`crate::format`]). The crate registers `"pink"`
//! lazily in every build. An extension registers a [`format::LakeFormat`] — writer, reader,
//! optional footer statistics, extension, id — with
//! [`format::register_lake_format`], making its id usable in a catalog:
//!
//! ```text
//! pink_lake::format::register_lake_format(Arc::new(MyFormat))?;
//! let mut lake = pink_lake::Lake::open(dir)?;
//! lake.set_write_format("my-format")?;
//! lake.write_from_store(&store)?;
//! ```
//!
//! The writer contract is the `.pink` writer's own: [`format::LakeFormat::write`]
//! returns the file bytes and the exact per-column [`pinkdb::persist::SavedTableStats`]
//! facts, which the lake seals into the manifest's CRC-protected block (the
//! shared exactness gate in `stats` applies to every provider). Attach normally
//! reads that block; a projected read may also read the format's footer via
//! [`format::LakeFormat::footer_stats`] to map the file's physical schema.
//! Either way the decode goes through [`format::LakeFormat::read`]. Format ids
//! are recorded in the catalog: a registered id is accepted, while an id with
//! no registered provider is a clean [`LakeError::UnknownFormat`] naming the id
//! when that table is read or written — never a panic and never a silent
//! fallback (wave 88 makes that a per-table refusal: the rest of the lake
//! still opens). [`Lake::set_write_format`] selects a registered format for a
//! handle's subsequent writes; the default is `.pink`.
//!
//! # Per-table formats (wave 88)
//!
//! One lake may hold tables in different data-file formats. A handle chooses
//! one per table with [`Lake::set_table_format`] (validated against the
//! registry exactly like [`Lake::set_write_format`]); the choice is in memory
//! and applies to the handle's subsequent **full saves** and to tables it
//! creates.
//!
//! A table **keeps its format** on every save and append: changing it is
//! always explicit (wave 102 removed the wave 88 hybrid rule that migrated
//! default-written tables, together with every legacy catalog version). A
//! full save resolves each table's format in this order:
//!
//! 1. the handle's explicit per-table choice ([`Lake::set_table_format`]);
//! 2. else the format the table's previous entry already has;
//! 3. else (a new table) the handle's write format ([`Lake::set_write_format`],
//!    else the build default).
//!
//! Appends always use the entry's format: an existing entry has exactly one
//! format, so a commit/flush writes its new files in
//! that format (a per-table choice never makes an append mix formats inside
//! one entry). A table created by a commit uses the per-table choice (pinned)
//! else the handle's write format (pinned when explicitly chosen).
//! Converting a table is explicit maintenance — [`Lake::rewrite_table_format`]
//! (the `.lake rewrite <dir> <table> format <id>` command) rewrites every
//! file of the table into the requested format in one atomically published
//! version, materializing any positional deletes; old files are retired by
//! the ordinary expire/cleanup path. Ordinary compaction/rewrite keeps the
//! entry's format.
//!
//! **Unavailable formats are refused per table, not per lake.** An entry
//! whose format id has no registered provider ([`LakeError::UnknownFormat`])
//! does not refuse the whole lake: `open`/`attach` succeed,
//! [`Lake::unavailable_tables`] lists the table with its id and error,
//! [`Lake::load`] materializes the readable tables and skips the rest, and
//! `.lake attach` lists the table while any read of it fails with that error
//! naming the table and the id. A full save carries such an entry forward
//! verbatim when the store has no table under its key and refuses when it
//! does; a commit/append, rewrite or compaction targeting it fails, and
//! lake-wide maintenance skips it. The format id removed in wave 104
//! is the exception: it is refused up front, never treated as a missing
//! provider, because no build will ever read it again.
//!
//! `.lake open` (every build) and `.lake attach` (every build) are the two
//! ways in: `open` eagerly loads a version into the store, `attach` reads
//! only the catalog, the manifest, and the per-file `.pink` statistics blocks
//! (no data file is opened at attach) — and lazily materializes the one table
//! a query actually scans, with exact `COUNT`/`MIN`/`MAX` answered straight
//! from those statistics. Attached
//! lakes are read-only, session-local, and never part of a save, a
//! checkpoint, or the WAL; the write path is [`commit`](Lake::commit) (see
//! the `incremental` module), which appends immutable data files and
//! positional delete files and **never rewrites a published file**.
//!
//! # Snapshots and versioning
//!
//! [`Lake::write_from_store`] writes a full snapshot: every table of every
//! database of the store is written to a fresh data file (one file per table
//! per version — a full save never appends and never reuses), then the
//! catalog rows describing it are written in the same store transaction. [`Lake::commit`] (and
//! the higher-level [`Lake::commit_from_tables`]) publishes an
//! **incremental** version on top of one: appended files are added to the
//! table's file list, positional deletes become delete files, and **no file a
//! previous version references is rewritten, replaced, or reused for new
//! data** — the `incremental` module documents the delta format. Each commit
//! also records its author and message on the snapshot row. A version number
//! is `previous + 1`, computed with `checked_add` and capped at
//! [`MAX_VERSION`]; a lake whose next version would exceed the cap is a clear
//! [`LakeError::Overflow`], never a wrap. Every manifest's `version` must be
//! in `1..=MAX_VERSION` (a tampered `version: 0` or `version: u64::MAX` is
//! [`LakeError::Corrupt`]). An already-published version is never replaced
//! ([`LakeError::Conflict`]), so saving it again cannot clobber its snapshot.
//! Old snapshots and old data files are never touched, so any published
//! version stays loadable. [`Lake::load`] takes the latest version (the
//! catalog) or one specific snapshot.
//!
//! Every version also carries a **snapshot record** (wave 64; see the
//! `snapshot` module): `updated` is its publish time (millisecond ISO-8601
//! UTC, written since wave 34), and a save may add an `author`, a
//! `commit_message` ([`Lake::set_commit_message`], DuckLake's
//! `ducklake_set_commit_message`) and a `changes` summary diffed from the
//! previous manifest. [`Lake::snapshots`] reads those records back without
//! touching a data file, and `.lake snapshots` prints them. Reading a
//! version's *data* (rather than its record) is [`Lake::load`] or
//! [`Lake::attach`]; an attachment additionally serves per-query time travel
//! (`FROM t AT (VERSION => n)` / `AT (TIMESTAMP => ts)`) over the versions up
//! to the one it is pinned to (see the `attach` module).
//!
//! # Schema evolution (wave 65)
//!
//! A `format_version` 5 table entry carries a **schema record** (see the
//! `schema` module): `schema_version` (the table's schema epoch), `columns`
//! (every live column with its type, its `DEFAULT` and the epoch it appeared)
//! and `renames` (the rename history). Every data file records the epoch it
//! was written under, and every manifest carries the lake-wide
//! `schema_version` counter (DuckLake's `snapshot.schema_version`), bumped
//! once per published version that created, dropped, or altered a table.
//! `ALTER TABLE` (ADD COLUMN with or without a DEFAULT, DROP COLUMN, RENAME
//! COLUMN) through a writable attach publishes a new record with the version
//! **without rewriting any data file**: a column the file predates reads its
//! default on every row, a renamed column reads its physical predecessor, and
//! a dropped column's bytes simply stay unlisted (positions are unchanged, so
//! positional delete files stay valid). Each manifest embeds the record as of
//! its own version, so time travel reads each version with that version's
//! schema and names.
//!
//! Probed against DuckLake (duckdb `v2.0.0-alpha46057` + ducklake): creating a
//! table bumps `snapshot.schema_version`, data changes do not, and each
//! `ALTER TABLE` does; `ADD COLUMN … DEFAULT` sets both `initial_default` and
//! `default_value` and leaves the data file's `end_snapshot` NULL (no
//! rewrite); `DROP COLUMN` closes the `ducklake_column` row and leaves the
//! data file and its bytes untouched; a drop + re-add is a **fresh column id**
//! whose `initial_default` covers the old files; `RENAME COLUMN` keeps the
//! column id and closes/reopens its name row. `ALTER TABLE … RENAME TO` *is*
//! supported by DuckLake as metadata-only (`ducklake_table.table_name` gets an
//! `end_snapshot` + a new row, same table id), but pinkdb's commit format has
//! no per-version table-name mapping: a rename on a writable attach is
//! refused by [`WritableAttachment::commit`] with [`LakeError::NotRepresentable`]
//! pointing at `.lake save` (a full snapshot), never silently published as a
//! drop.
//!
//! DuckLake's `ducklake_default_version` setting is deliberately **not**
//! mirrored: it is not a per-query read default — it selects the storage
//! format version for *new catalogs*, and the probed DuckDB build rejects
//! every explicit value ("Unsupported ducklake_version 'n'"). A pinkdb
//! setting under the same name would mean something else, so there is none.
//!
//! # Maintenance (wave 67)
//!
//! [`Lake::merge_adjacent_files`] and [`Lake::rewrite_data_files`] consolidate
//! data files by publishing one new version that references fresh immutable
//! files — candidates must share partition values and schema epoch, merge
//! skips delete-bearing files (rewrite is the tool for those), and every
//! source file stays on disk byte-identical for the versions that reference
//! it. [`Lake::expire_snapshots`] removes old snapshot rows and their commit
//! deltas (never the current version; time travel to an expired version is a
//! clean error). [`Lake::cleanup_old_files`] /
//! [`Lake::delete_orphaned_files`] delete `data/` objects no retained
//! version references, behind a two-day age gate and an explicit
//! `dry-run`/`confirm`. [`Lake::list_files`], [`Lake::table_info`],
//! [`Lake::options`] and [`Lake::settings`] are the DuckLake
//! `ducklake_list_files` / `ducklake_table_info` / `ducklake_options` /
//! `ducklake_settings` introspection, docked to this catalog. The
//! `maintenance` module documents every rule, the live DuckLake probes
//! behind them, and the honest differences.
//!
//! # Replication (wave 89)
//!
//! [`replication`] is the **table-level** replication API: a writable
//! destination lake carries jobs (in its catalog) that copy the
//! tables of one source — another lake, or one database of the caller's live
//! [`PinkDb`] store — under the same `db.table` names. A job's tables are
//! ordinary lake tables: every destination write is an incremental
//! [`Lake::commit_from_tables`] version with only that table in
//! `base`/`current`, so time travel, attach, CDC and maintenance all apply.
//! Each table picks a strategy — **watermark** (append-only cursor on a
//! configured column), **snapshot** (lake sources; replay
//! [`Lake::table_changes`] and verify), **merge** (store sources with a
//! `PRIMARY KEY`; key-level diff) or **full** (copy) — and the state row
//! records the strategy and cursor. Table identity ([`Lake::table_ids`])
//! detects a drop + recreate on the source and forces a reseed; schema drift
//! is extended in place when additive and recreated otherwise. The state file
//! is written with conditional writes (`put_new`/`put_if_match`) and is
//! sealed like every other artifact when the destination is keyed.
//! [`replication::catch_up`] runs one cycle. [`replication::Supervisor`] runs
//! one background thread per destination: due `running`/`error` lake-source
//! jobs on their interval (a 250 ms floor), exponential backoff capped at
//! 60 s after a failed cycle, a condvar woken by stop/notify/pause/resume, and
//! a stop that aborts an in-flight cycle at the next table boundary. Cycles
//! never overlap. Store-source jobs are not supervised — a single-threaded
//! store runs them through `catch_up` from the process that owns it. The CLI
//! surface is `.lake replicate …` (create, catchup, start/stop, pause/resume/
//! resume-all, status, tables, drop).
//!
//! # Crash safety and durability
//!
//! A commit writes its immutable data and delete files first, then publishes
//! everything else in one catalog transaction (head check, snapshot record,
//! typed delta; see `metadata`). Each file write goes to a **unique** temp
//! sibling (`<name>.<uuid>.tmp`, created with `create_new(true)` so a planted
//! symlink or an older writer's temp file is never followed or truncated), is
//! fsynced, and is then renamed into place; the parent directory is fsynced
//! afterwards, best-effort. A crash anywhere can only leave *unreferenced*
//! leftovers: the catalog still names the last committed version, and stray
//! data files and `.tmp` files are ignored — [`Lake::versions`] lists only
//! the snapshots the catalog holds.
//!
//! This is **process-crash safety**: after a `kill -9`, the previous version
//! always loads. It is deliberately *not* a power-loss guarantee on APFS:
//! `File::sync_all`/`fsync(2)` may return before the drive's own cache is
//! flushed; only `F_FULLFSYNC` pushes that through, and this crate does not
//! add an `unsafe`/libc dependency for it (a future opt-in
//! `F_FULLFSYNC` mode could lift this).
//!
//! # Concurrent writers
//!
//! A lake is **one live writer handle per catalog** (no file locking).
//! Concurrent readers of one published version are fine. A handle that tries
//! to publish after another handle or process moved the catalog forward
//! fails with [`LakeError::Conflict`] ("another writer published version N;
//! reopen the lake") — and two writers racing for the same version cannot
//! both win: the store's head check admits exactly one. A **commit** whose
//! publish conflicts is retried under [`CommitSettings`] (DuckLake's
//! `ducklake_max_retry_count`/`ducklake_retry_backoff`/`ducklake_retry_wait_ms`,
//! defaults 10 / 1.5 / 100 ms), rebasing the already-written immutable files
//! onto the new head; a delta whose base files the head no longer contains
//! is refused, never silently re-applied. Over S3 data files are published
//! with conditional writes (`If-None-Match: *`), which AWS S3 / Cloudflare
//! R2 evaluate atomically with the write commit. An endpoint that rejects
//! conditionals refuses the write unless the
//! fallback is explicitly opted into — see the `s3` feature docs and the
//! README.
//!
//! # Naming and scope (lake decisions)
//!
//! - The catalog is **flat**: keys are `db.table`. Restoring splits on the
//!   first `.` and recreates the databases. A database name that contains a
//!   `.` cannot be represented and is refused at save time; a table name may
//!   contain dots. Views are session-local in the engine and are not saved.
//! - A lake records **tables**. A database with no tables (and the absence of
//!   the default `main`, which [`Lake::load`] always recreates) is not
//!   preserved — a lake limitation.
//! - The table part of a data path is percent-encoded from the catalog key,
//!   so table names that contain `/`, `.`, spaces, or non-ASCII characters
//!   can never escape the `data/` directory. [`validate_store`] refuses
//!   names that would not fit a filesystem name (and dotted database names)
//!   *before* a lake is created, so a store that `save_store`/`.lake save`
//!   rejects leaves nothing behind.
//! - Data files are **plaintext by default**. With the non-default
//!   `encryption` feature, a lake opened or created with a key
//!   ([`EncryptionKey`], or the CLI's `PINKDB_KEY`) seals every artifact —
//!   the `pink:` catalog file, data files, and positional delete files — into
//!   `PNKLAKE1` containers (AES-256-GCM, one Argon2id derivation per session;
//!   see the `crypto` module for the format). A keyed open
//!   of a plaintext lake is refused and a keyless open of an encrypted lake
//!   is refused, so a key is never silently ignored. The engine's own
//!   `.pink` containers (`PINKENC1`) are never stored in a lake. Plaintext
//!   data files carry **no checksums**: mutating a data file in place is
//!   undetected by design (an encrypted lake authenticates the whole file,
//!   and binds it to its catalog path). The checks that do run on load are
//!   structural: a `.pink` file's footer must name the catalog's table and
//!   its schema/row count must match — so a file swapped in from a
//!   *differently named* table is rejected. Swapping two *plaintext* files of the *same* table
//!   (same name, same schema, same row count) is not detectable (an
//!   encrypted file is bound to its catalog path), and a tampered plaintext
//!   file can still decode to different values or fail with a
//!   [`LakeError::Corrupt`] (a caught decoder panic included).
//! - **Single writer**: one live handle per lake directory; a stale handle's
//!   save fails with [`LakeError::Conflict`], it never silently forks the
//!   version chain.
//! - I/O goes through a private `Storage` trait: a local directory
//!   (`LocalStorage`, the wave-34/35 behavior unchanged) or, with the
//!   non-default `s3` feature, an S3 bucket and key prefix.
//!   `Lake::open_url("s3://bucket/prefix")` (and
//!   [`save_store_url`]) accept either; without the feature an `s3://`
//!   location is a clear [`LakeError::FeatureMissing`]. The catalog is the
//!   single-writer guard, so S3 holds only data objects and needs no
//!   conditional PUT support. Single-PUT objects only: no multipart, so one
//!   data file must be ≤ 5 GiB (see the README).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use pinkdb::{DataTable, DataType, PinkDb};

mod attach;
mod catalog;
mod catalog_store;
mod cdc;
mod crypto;
pub mod database;
mod delete_file;
pub mod engine;
pub mod format;
mod incremental;
mod inlined;
mod local;
mod maintenance;
mod metadata;
#[cfg(test)]
mod mock_s3;
mod ranged;
#[cfg(feature = "s3")]
mod s3;
mod schema;
mod snapshot;
mod stats;
mod storage;
mod time;
mod zone;

pub mod replication;

pub use attach::AttachmentReport;
use catalog::{Catalog, DeleteRef, FileFormat, FileRef, PartitionKey, TableEntry};
pub use cdc::{ChangeKind, ChangeRow};
pub use crypto::EncryptionKey;
pub use engine::{EngineLease, LEASE_PATH, LEASE_TIMEOUT, LeaseError};
pub use incremental::{
    AddFilesReport, Commit, CommitReport, CommitSettings, FlushReport, WritableAttachment,
};
use local::LocalStorage;
pub use maintenance::{
    CleanupReport, CleanupSettings, CompactionReport, DEFAULT_RETENTION_SECS, ExpireReport,
    ExpireSettings, LakeFileInfo, LakeOption, LakeSettings, MergeSettings, RewriteSettings,
    TableCompaction, TableInfo,
};
pub use snapshot::{ChangeSummary, SnapshotInfo};
use storage::{Head, Location, Storage};

#[cfg(feature = "s3")]
pub use s3::{DEFAULT_RETRIES, S3Config};

/// The catalog format version this build writes (and its only read).
///
/// Version 7 (wave 102): one entry per table with a **schema record**
/// (`schema_version`, `columns`, `renames`), a growing, never-rewritten file
/// list, positional delete files, inlined rows, per-file statistics/zone maps
/// and a catalog-recorded footer location. Every file of a table carries the
/// schema epoch it was written under, so `ALTER TABLE` publishes metadata-only
/// changes and old data files stay readable through the name mapping.
///
/// Wave 102 dropped every older catalog: pinkdb carries no legacy readers
/// while in early development, and an older lake is refused with "written by
/// an older pinkdb". Wave 104 removed the legacy columnar data format; a
/// catalog row naming it is refused with a re-save hint instead of read.
pub const FORMAT_VERSION: u32 = 7;

/// The oldest catalog format version this build reads: the current one
/// (wave 102 dropped every older catalog — pinkdb carries no legacy readers
/// while in early development; an older lake is refused with "written by an
/// older pinkdb").
pub const MIN_FORMAT_VERSION: u32 = 7;

/// The newest catalog format version this codebase knows. A manifest above
/// this (a future format) is [`LakeError::Unsupported`] in every build.
pub const MAX_FORMAT_VERSION: u32 = 7;

/// The tree-walk marker of a pre-103 lake directory: only the data-location
/// guard looks for it, so a new catalog never takes over an older build's
/// files. Nothing reads or writes it as a lake.
pub(crate) const LAKE_FILE: &str = "lake.json";

/// The data-root ownership marker of a split lake (wave 96a): `{"lake_id": …}`.
/// Written at create and verified only by the destructive maintenance paths,
/// so two catalogs pointed at one data location can never delete each other's
/// files. The default object-store layout never writes or reads it.
pub(crate) const LAKE_DATA_FILE: &str = "lake-data.json";

/// The largest manifest `version` this build reads or writes.
///
/// `version + 1` is computed with `checked_add`, so `u64::MAX` is provably
/// unpublishable; capping one below it keeps every version a writer can
/// publish loadable forever. A manifest above the cap (a tampered or
/// truncated `version: u64::MAX`) is [`LakeError::Corrupt`], not a wrap.
pub const MAX_VERSION: u64 = u64::MAX - 1;

/// DuckLake's `ducklake_default_data_inlining_row_limit`: a commit's appended
/// rows are stored in the manifest instead of a data file when the table's
/// limit admits them (at most this many rows, and more than zero), unless the
/// table records its own `data_inlining_row_limit`. 0 disables inlining.
pub const DEFAULT_INLINING_ROW_LIMIT: u64 = 10;

/// Result alias for lake operations.
pub type Result<T> = std::result::Result<T, LakeError>;

/// Everything that can go wrong while operating on a lake.
#[derive(Debug)]
pub enum LakeError {
    /// The location holds no lake catalog.
    NotALake { path: PathBuf },
    /// The lake's catalog was migrated away (wave 96a): the source carries a
    /// `moved.json` (object store) or a `pink_lake_meta.moved_to` row (SQL),
    /// naming the new location. Opening the old location is refused with this
    /// instead of `NotALake`, so a moved lake is never mistaken for a deleted
    /// one.
    CatalogMoved { path: PathBuf, moved_to: String },
    /// The location already holds a lake catalog; `create` refuses to
    /// overwrite a lake.
    AlreadyALake { path: PathBuf },
    /// Malformed lake metadata (a bad catalog row, a bad data-file
    /// reference).
    Corrupt { path: PathBuf, message: String },
    /// The metadata is well-formed but a newer/incompatible format.
    Unsupported { path: PathBuf, found: u32 },
    /// The requested snapshot version does not exist.
    VersionNotFound { path: PathBuf, version: u64 },
    /// The store's shape cannot be represented in the flat lake catalog.
    NotRepresentable { message: String },
    /// Another writer changed the lake since this handle read the catalog, or
    /// a snapshot version already exists and must not be replaced. Reopen the
    /// lake (one live writer handle per directory).
    Conflict { path: PathBuf, message: String },
    /// The lake is at [`MAX_VERSION`] (or `u64::MAX - 1`) and cannot publish
    /// a further version without wrapping.
    Overflow { path: PathBuf, version: u64 },
    /// The location string is not usable: a malformed `s3://` URL, a bad
    /// bucket/prefix/endpoint.
    BadLocation { location: String, message: String },
    /// This pink-lake build cannot do that: a non-default cargo feature is
    /// missing (an `s3://` location without the `s3` feature).
    FeatureMissing {
        feature: &'static str,
        message: String,
    },
    /// Object storage denied the request (HTTP 403): credentials, bucket
    /// permissions, or a wrong endpoint.
    Auth { path: PathBuf, message: String },
    /// Filesystem failure, with the operation that failed.
    Io {
        context: String,
        source: std::io::Error,
    },
    /// The engine rejected the data or the store shape.
    Engine {
        context: String,
        source: pinkdb::PinkDbError,
    },
    /// A catalog row names the data format removed in wave 104. There is no
    /// reader and no converter: the lake must be re-saved by an older build.
    RemovedFormat { message: String },
    /// An encrypted lake artifact was read without a key: the catalog
    /// database or a data file is a `PNKLAKE1` container (or this build
    /// cannot read it at all). A keyless open never guesses.
    Encrypted { path: PathBuf, message: String },
    /// A key was supplied but the artifact is plaintext. Refused so a key can
    /// never be silently ignored (`pinkdb::crypto` has the same rule for
    /// `.pink` files).
    NotEncrypted { path: PathBuf, message: String },
    /// The supplied key does not match: the container's key-check block did
    /// not authenticate (wrong key, wrong key *type*, or a tampered header —
    /// the three are indistinguishable in principle).
    WrongKey { path: PathBuf, message: String },
    /// An encrypted container failed authentication or does not belong here: a
    /// modified/truncated ciphertext, a missing terminal frame, or a container
    /// sealed for another domain/path. Never a partial read.
    Tampered { path: PathBuf, message: String },
    /// An encrypted container whose *version* this build does not understand:
    /// it was sealed by a newer pinkdb. Not corruption — upgrade to open it.
    UnsupportedContainer { path: PathBuf, found: u32 },
    /// A maintenance/introspection argument that cannot be honored — the
    /// DuckLake `InvalidInputException` family: `expire_snapshots` given both
    /// `versions` and `older_than`, a `delete_threshold` outside `0..=1`, a
    /// `min_file_size` not below `max_file_size`. The message is
    /// DuckLake-shaped where one exists.
    InvalidOption { message: String },
    /// A catalog entry names a data-file format no provider is registered
    /// for in this process (see [`crate::format`]). The id is named verbatim; the
    /// lake never falls back to another format.
    UnknownFormat { path: PathBuf, format: String },
    /// [`format::register_lake_format`] was called with an id that is already
    /// registered. A format id resolves to exactly one provider per process.
    DuplicateFormat { format: String },
    /// The key material itself is unusable (e.g. a non-UTF-8 `PINKDB_KEY`).
    BadKey { message: String },
    /// A bug in this crate (e.g. serializing its own structs).
    Internal { message: String },
}

impl fmt::Display for LakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LakeError::NotALake { path } => write!(
                f,
                "{} is not a pink lake (no catalog at that location; use create)",
                path.display()
            ),
            LakeError::CatalogMoved { path, moved_to } => write!(
                f,
                "this lake's catalog moved to {moved_to} (the source {} is retired; open the new \
                 location instead)",
                path.display()
            ),
            LakeError::AlreadyALake { path } => {
                write!(f, "{} is already a pink lake (use open)", path.display())
            }
            LakeError::Corrupt { path, message } => {
                write!(f, "corrupt lake metadata at {}: {message}", path.display())
            }
            LakeError::Unsupported { path, found } => {
                if *found > MAX_FORMAT_VERSION {
                    write!(
                        f,
                        "unsupported lake format_version {found} at {} (this build reads \
                         format_version {FORMAT_VERSION} only; the lake was written by a newer \
                         pinkdb — upgrade to open it)",
                        path.display()
                    )
                } else {
                    write!(
                        f,
                        "unsupported lake format_version {found} at {}: this lake was written by \
                         an older pinkdb; this build reads format_version {FORMAT_VERSION} only \
                         (no legacy readers while pinkdb is in early development)",
                        path.display()
                    )
                }
            }
            LakeError::VersionNotFound { path, version } => write!(
                f,
                "lake {} has no snapshot version {version}",
                path.display()
            ),
            LakeError::NotRepresentable { message } => f.write_str(message),
            LakeError::Conflict { path, message } => {
                write!(f, "write conflict at {}: {message}", path.display())
            }
            LakeError::Overflow { path, version } => write!(
                f,
                "lake version overflow at {}: version {version} cannot be incremented \
                 (this build publishes at most {MAX_VERSION})",
                path.display()
            ),
            LakeError::BadLocation { location, message } => {
                write!(f, "invalid lake location {location:?}: {message}")
            }
            LakeError::FeatureMissing { feature, message } => write!(
                f,
                "this pink-lake build does not include the `{feature}` feature: {message}"
            ),
            LakeError::Auth { path, message } => {
                write!(
                    f,
                    "object storage access error at {}: {message}",
                    path.display()
                )
            }
            LakeError::Io { context, source } => write!(f, "lake I/O error ({context}): {source}"),
            LakeError::Engine { context, source } => {
                write!(f, "lake data error ({context}): {source}")
            }
            LakeError::RemovedFormat { message } => f.write_str(message),
            LakeError::Encrypted { path, message } => {
                write!(f, "{}: the lake is encrypted; {message}", path.display())
            }
            LakeError::NotEncrypted { path, message } => {
                write!(
                    f,
                    "{}: not an encrypted pink-lake container; {message}",
                    path.display()
                )
            }
            LakeError::WrongKey { path, message } => {
                write!(f, "wrong key at {}: {message}", path.display())
            }
            LakeError::Tampered { path, message } => {
                write!(
                    f,
                    "corrupt or tampered encrypted lake file at {}: {message}",
                    path.display()
                )
            }
            LakeError::UnsupportedContainer { path, found } => write!(
                f,
                "unsupported encrypted-container version {found} at {} (written by a newer \
                 pinkdb — upgrade to open it)",
                path.display()
            ),
            LakeError::BadKey { message } => write!(f, "invalid encryption key: {message}"),
            LakeError::InvalidOption { message } => write!(f, "{message}"),
            LakeError::UnknownFormat { path, format } => write!(
                f,
                "corrupt lake metadata at {}: no lake format provider is registered for \
                 data-file format {format:?} (this build registers `pink`; extensions register \
                 one with pink_lake::format::register_lake_format)",
                path.display()
            ),
            LakeError::DuplicateFormat { format } => write!(
                f,
                "a lake format provider with id {format:?} is already registered (format ids \
                 are unique per process)"
            ),
            LakeError::Internal { message } => write!(f, "internal lake error: {message}"),
        }
    }
}

impl std::error::Error for LakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LakeError::Io { source, .. } => Some(source),
            LakeError::Engine { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// The row count an attached table this build cannot read advertises
/// (wave 88, rule 5): larger than `i64::MAX`, so the engine's statistics fast
/// paths (`COUNT`/`MIN`/`MAX`) cannot answer from the descriptor and fall
/// back to the table's loader, which returns the per-table format error.
/// Never a real lake row count.
pub const UNAVAILABLE_ROWS: u64 = u64::MAX;

/// One table of a lake whose data-file format this build cannot read (wave
/// 88, rule 5): the flat `db.table` catalog key, the entry's format id, and
/// the error every read or targeted write of the table returns.
///
/// A lake holding such a table still opens; [`Lake::unavailable_tables`]
/// lists it, [`Lake::load`] skips it, and `.lake attach` lists it with any
/// read failing. The error is the provider-resolution failure
/// ([`LakeError::UnknownFormat`]) and names the id.
#[derive(Debug)]
pub struct UnavailableTable {
    /// The flat `db.table` catalog key.
    pub table: String,
    /// The entry's data-file format id, verbatim from the catalog.
    pub format: String,
    /// The provider-resolution error this build would return for the table.
    pub error: LakeError,
}

impl UnavailableTable {
    /// The error's human-readable message (the id is named in it).
    pub fn message(&self) -> String {
        self.error.to_string()
    }
}

/// What one [`Lake::write_from_store`] published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaveReport {
    /// The newly published lake version (`previous + 1`).
    pub version: u64,
    /// Number of tables snapshotted across every database.
    pub tables: usize,
    /// Total rows across those tables.
    pub rows: u64,
    /// Bytes written: data files + snapshot manifest + catalog.
    pub bytes_written: u64,
}

/// A handle to a lake.
///
/// The handle caches the catalog's current version (single-writer contract:
/// one live handle per lake location); [`Lake::current_version`] reflects the
/// last open/create/save, not an out-of-band edit made by another process.
/// If another writer publishes in the meantime, the next save fails with
/// [`LakeError::Conflict`] — reopen the lake to continue.
#[derive(Debug)]
pub struct Lake {
    root: PathBuf,
    /// The catalog location errors name: the URL as given, password redacted
    /// (`pink:<path>`, `sqlite:<path>` or a Postgres URL).
    catalog: String,
    /// The data location (objects only; the catalog is `store`).
    storage: Arc<dyn Storage>,
    /// The catalog database (DuckLake tables).
    store: Arc<dyn crate::metadata::MetadataStore>,
    /// The lake's random 128-bit identity (hex).
    lake_id: String,
    format_version: u32,
    version: u64,
    /// The author/commit message [`Lake::set_commit_message`] recorded for the
    /// next published save (DuckLake's `ducklake_set_commit_message`
    /// transaction metadata). Kept across a failed save, cleared by a
    /// successful one.
    pending_commit: Option<(Option<String>, Option<String>)>,
    /// The engine catalog image [`Lake::set_engine_catalog`] staged for the
    /// next published commit (wave 96b). Kept across a failed save, cleared by
    /// a successful one — like `pending_commit`, so a retry publishes it once.
    pending_engine_catalog: Option<Arc<Vec<u8>>>,
    /// The handle's `data_inlining_row_limit` default, applied to a table
    /// that does not record its own (DuckLake's session setting
    /// `ducklake_default_data_inlining_row_limit`; this handle is the
    /// analogue of a session). Not persisted: every handle starts at
    /// [`DEFAULT_INLINING_ROW_LIMIT`] and a table's persisted
    /// [`TableEntry::inlined_limit`] wins over it.
    inline_row_limit: u64,
    /// The data-file format [`Lake::set_write_format`] selected for this
    /// handle's subsequent writes; `None` keeps the build default (`pink`).
    /// Not persisted — a reopened handle starts at the default again.
    write_format_choice: Option<FileFormat>,
    /// Per-table format choices ([`Lake::set_table_format`]), keyed by the
    /// flat `db.table` catalog key. A choice wins over the entry's pinned
    /// format for a full save and for a table the handle creates; an append
    /// to an existing table always keeps the entry's format (see the module
    /// docs). Not persisted.
    table_format_choices: BTreeMap<String, FileFormat>,
}

/// One inlined row version, as [`Lake::inlined_records`] returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct InlinedRecord {
    pub row_id: u64,
    pub begin: u64,
    pub end: Option<u64>,
    pub flushed: bool,
    pub columns: Vec<String>,
    pub values: Vec<Option<String>>,
}

impl Lake {
    /// Every inlined row version of one table (diagnostics; the typed
    /// DuckLake inlined tables are the only copy).
    pub fn inlined_records(&self, key: &str) -> Result<Vec<InlinedRecord>> {
        let Some(entry) = self.store.table(key, None)? else {
            return Ok(Vec::new());
        };
        Ok(entry
            .inlined
            .into_iter()
            .map(|row| InlinedRecord {
                row_id: row.row_id,
                begin: row.begin,
                end: row.end,
                flushed: row.flushed,
                columns: row.columns,
                values: row.values,
            })
            .collect())
    }
}

impl Lake {
    /// Create a lake at a bare path: a `pink:<path>` catalog database with
    /// its default data location `<path>.files/`.
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        Self::create_url(&dir.as_ref().to_string_lossy())
    }

    /// [`Lake::create`] with per-file-key encryption (and an engine-encrypted
    /// `pink:` catalog database).
    pub fn create_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Self> {
        Self::create_url_with_key(&dir.as_ref().to_string_lossy(), key)
    }

    /// Create a lake from a location: a bare path (a `pink:<path>` catalog),
    /// `pink:<path>`, `sqlite:<path>` or `postgres://…`. `pink:` and
    /// `sqlite:` default their data location to `<path>.files/`; Postgres
    /// needs an explicit data location (`Lake::create_with_catalog`). An
    /// `s3://` URL alone is not a lake: data on S3 needs a catalog database.
    pub fn create_url(location: &str) -> Result<Self> {
        let (catalog, data) = default_catalog_split(location, "create")?;
        Self::create_with_catalog_keyed(&catalog, &data, None)
    }

    /// [`Lake::create_url`] with per-file-key encryption.
    pub fn create_url_with_key(location: &str, key: EncryptionKey) -> Result<Self> {
        let (catalog, data) = default_catalog_split(location, "create")?;
        Self::create_with_catalog_keyed(&catalog, &data, Some(key))
    }

    /// Create a lake whose metadata lives in `catalog_url` and whose data
    /// files live at `data_location` (a local directory or `s3://…`). The
    /// data location is recorded in the catalog, so [`Lake::open_url`] needs
    /// only the catalog location afterwards.
    pub fn create_with_catalog(catalog_url: &str, data_location: &str) -> Result<Self> {
        Self::create_with_catalog_keyed(catalog_url, data_location, None)
    }

    /// [`Lake::create_with_catalog`] with per-file-key encryption.
    pub fn create_with_catalog_with_key(
        catalog_url: &str,
        data_location: &str,
        key: EncryptionKey,
    ) -> Result<Self> {
        Self::create_with_catalog_keyed(catalog_url, data_location, Some(key))
    }

    fn create_with_catalog_keyed(
        catalog_url: &str,
        data_location: &str,
        key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let catalog_location = Location::parse(catalog_url)?;
        if !matches!(
            catalog_location,
            Location::Pink(_)
                | Location::Sqlite(_)
                | Location::Postgres(_)
                | Location::CatalogS3 { .. }
        ) {
            return Err(LakeError::BadLocation {
                location: redact_location(catalog_url),
                message: "a lake catalog must be `pink:<path>`, `sqlite:<path>` or \
                          `postgres://…`; `s3://…` is a data location, not a catalog"
                    .to_string(),
            });
        }
        let data = data_storage(data_location)?;
        let Some(store) = catalog_store::open_metadata_store_keyed(
            catalog_url,
            &catalog_location,
            true,
            key.as_ref(),
        )?
        else {
            return Err(catalog_feature_missing(catalog_url, &catalog_location));
        };
        if crate::metadata::MetadataStore::head_version(&store)?.is_some() {
            return Err(LakeError::AlreadyALake {
                path: PathBuf::from(redact_location(catalog_url)),
            });
        }
        ensure_data_location_free(&*data, data_location)?;
        Self::create_sql_catalog(catalog_url, store, data_location, key.as_ref())
    }

    /// Create a catalog lake directly on the DuckLake tables.
    fn create_sql_catalog(
        catalog_url: &str,
        store: catalog_store::SqlMetadataStore,
        data_location: &str,
        key: Option<&EncryptionKey>,
    ) -> Result<Self> {
        let data = data_storage(data_location)?;
        Self::create_sql_catalog_data(catalog_url, store, data_location, data, key)
    }

    fn create_sql_catalog_data(
        catalog_url: &str,
        store: catalog_store::SqlMetadataStore,
        data_location: &str,
        data: Arc<dyn Storage>,
        key: Option<&EncryptionKey>,
    ) -> Result<Self> {
        let lake_id = fresh_lake_id()?;
        let manifest = Catalog {
            format_version: FORMAT_VERSION,
            lake_id: Some(lake_id.clone()),
            updated: time::iso8601_utc(SystemTime::now()),
            version: 1,
            tables: BTreeMap::new(),
            author: None,
            commit_message: None,
            changes: None,
            schema_version: 0,
        };
        crate::metadata::MetadataStore::create(
            &store,
            data_location,
            concat!("pinkdb ", env!("CARGO_PKG_VERSION")),
            &manifest,
        )?;
        crate::metadata::MetadataStore::set_encrypted(&store, key.is_some())?;
        let store = Arc::new(store);
        let data = keyed_data_storage(data, Arc::clone(&store))?;
        let root = data.abs("");
        let lake = Self {
            root,
            catalog: redact_location(catalog_url).to_string(),
            storage: data,
            store,
            lake_id,
            format_version: FORMAT_VERSION,
            version: 1,
            pending_commit: None,
            pending_engine_catalog: None,
            inline_row_limit: DEFAULT_INLINING_ROW_LIMIT,
            write_format_choice: None,
            table_format_choices: BTreeMap::new(),
        };
        lake.storage
            .put(LAKE_DATA_FILE, &data_ownership_json(&lake.lake_id)?)?;
        Ok(lake)
    }

    /// Open a lake at a bare path: a `pink:<path>` catalog.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_url(&dir.as_ref().to_string_lossy())
    }

    /// [`Lake::open`] with per-file-key encryption.
    pub fn open_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Self> {
        Self::open_url_with_key(&dir.as_ref().to_string_lossy(), key)
    }

    /// Open a lake by its catalog location. A bare path means `pink:<path>`;
    /// `s3://…` alone is not a lake.
    pub fn open_url(location: &str) -> Result<Self> {
        let catalog = open_catalog_location(location, "open")?;
        Self::open_with_catalog_keyed(&catalog, None, None)
    }

    /// [`Lake::open_url`] with per-file-key encryption.
    pub fn open_url_with_key(location: &str, key: EncryptionKey) -> Result<Self> {
        let catalog = open_catalog_location(location, "open")?;
        Self::open_with_catalog_keyed(&catalog, None, Some(key))
    }

    /// Open the lake whose metadata lives in `catalog_url`. The data location
    /// comes from the catalog unless `data_location` overrides it.
    pub fn open_with_catalog(catalog_url: &str, data_location: Option<&str>) -> Result<Self> {
        Self::open_with_catalog_keyed(catalog_url, data_location, None)
    }

    /// [`Lake::open_with_catalog`] with per-file-key encryption.
    pub fn open_with_catalog_with_key(
        catalog_url: &str,
        data_location: Option<&str>,
        key: EncryptionKey,
    ) -> Result<Self> {
        Self::open_with_catalog_keyed(catalog_url, data_location, Some(key))
    }

    fn open_with_catalog_keyed(
        catalog_url: &str,
        data_location: Option<&str>,
        key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let catalog_location = Location::parse(catalog_url)?;
        let Some(store) = catalog_store::open_metadata_store_keyed(
            catalog_url,
            &catalog_location,
            false,
            key.as_ref(),
        )?
        else {
            return Err(catalog_feature_missing(catalog_url, &catalog_location));
        };
        Self::open_sql_catalog(catalog_url, store, data_location, key.as_ref())
    }

    fn open_sql_catalog(
        catalog_url: &str,
        store: catalog_store::SqlMetadataStore,
        data_location: Option<&str>,
        key: Option<&EncryptionKey>,
    ) -> Result<Self> {
        let encrypted = crate::metadata::MetadataStore::is_encrypted(&store)?;
        if encrypted && key.is_none() {
            return Err(LakeError::Encrypted {
                path: PathBuf::from(redact_location(catalog_url)),
                message: "the catalog's data files are encrypted; open it with the lake's key"
                    .to_string(),
            });
        }
        if !encrypted && key.is_some() {
            return Err(LakeError::NotEncrypted {
                path: PathBuf::from(redact_location(catalog_url)),
                message: "the catalog's data files are plaintext; open it without a key"
                    .to_string(),
            });
        }
        let recorded = store.data_location()?;
        let Some(data_location) = data_location
            .map(str::to_string)
            .or(recorded)
            .or_else(|| default_data_for_catalog(catalog_url))
        else {
            return Err(LakeError::Corrupt {
                path: PathBuf::from(redact_location(catalog_url)),
                message: "the catalog has no recorded data location; pass one explicitly"
                    .to_string(),
            });
        };
        let data = data_storage(&data_location)?;
        Self::open_sql_catalog_data(catalog_url, store, &data_location, data, key)
    }

    fn open_sql_catalog_data(
        catalog_url: &str,
        store: catalog_store::SqlMetadataStore,
        data_location: &str,
        data: Arc<dyn Storage>,
        _key: Option<&EncryptionKey>,
    ) -> Result<Self> {
        let Some(mut manifest) = store.head_catalog()? else {
            return Err(LakeError::NotALake {
                path: PathBuf::from(redact_location(catalog_url)),
            });
        };
        let _ = data_location;
        let lake_id = match manifest.lake_id.clone() {
            Some(id) => id,
            None => {
                let id = fresh_lake_id()?;
                store.set_lake_id(&id)?;
                manifest.lake_id = Some(id.clone());
                id
            }
        };
        let store = Arc::new(store);
        let data = keyed_data_storage(data, Arc::clone(&store))?;
        data.tighten_existing()?;
        if let Ok(Location::Pink(path)) | Ok(Location::Sqlite(path)) = Location::parse(catalog_url)
        {
            crate::local::tighten_catalog(&path);
        }
        let root = data.abs("");
        Ok(Self {
            root,
            catalog: redact_location(catalog_url).to_string(),
            storage: data,
            store,
            lake_id,
            format_version: manifest.format_version,
            version: manifest.version,
            pending_commit: None,
            pending_engine_catalog: None,
            inline_row_limit: DEFAULT_INLINING_ROW_LIMIT,
            write_format_choice: None,
            table_format_choices: BTreeMap::new(),
        })
    }

    /// [`Lake::open`] when the directory already holds a lake, else
    /// [`Lake::create`]. The create→open retry covers a concurrent create
    /// that won the catalog link in between.
    pub fn open_or_create(dir: impl AsRef<Path>) -> Result<Self> {
        let root = dir.as_ref().to_path_buf();
        match Self::open(&root) {
            Ok(lake) => Ok(lake),
            Err(LakeError::NotALake { .. }) => match Self::create(&root) {
                Ok(lake) => Ok(lake),
                Err(LakeError::AlreadyALake { .. }) => Self::open(root),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// [`Lake::open_or_create`] with at-rest encryption: creates an encrypted
    /// lake at a fresh location, opens an existing encrypted one, and refuses
    /// a *plaintext* lake ([`LakeError::NotEncrypted`]) — a plaintext lake is
    /// never silently rewritten as encrypted (save the store to a new
    /// location to migrate).
    pub fn open_or_create_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Self> {
        let root = dir.as_ref().to_path_buf();
        match Self::open_with_key(&root, key.clone()) {
            Ok(lake) => Ok(lake),
            Err(LakeError::NotALake { .. }) => match Self::create_with_key(&root, key.clone()) {
                Ok(lake) => Ok(lake),
                Err(LakeError::AlreadyALake { .. }) => Self::open_with_key(root, key),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// [`Lake::open_url`] when the location already holds a lake, else
    /// [`Lake::create_url`].
    pub fn open_or_create_url(location: &str) -> Result<Self> {
        match Self::open_url(location) {
            Ok(lake) => Ok(lake),
            Err(LakeError::NotALake { .. }) => match Self::create_url(location) {
                Ok(lake) => Ok(lake),
                Err(LakeError::AlreadyALake { .. }) => Self::open_url(location),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// [`Lake::open_or_create_url`] with at-rest encryption (see
    /// [`Lake::open_or_create_with_key`] for the plaintext-lake refusal).
    pub fn open_or_create_url_with_key(location: &str, key: EncryptionKey) -> Result<Self> {
        match Self::open_url_with_key(location, key.clone()) {
            Ok(lake) => Ok(lake),
            Err(LakeError::NotALake { .. }) => {
                match Self::create_url_with_key(location, key.clone()) {
                    Ok(lake) => Ok(lake),
                    Err(LakeError::AlreadyALake { .. }) => Self::open_url_with_key(location, key),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// The data-file format id pinned for one table (`pink`, `parquet`), if
    /// the table exists.
    pub fn table_format_name(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .store
            .table(key, None)?
            .map(|entry| entry.format().id().to_string()))
    }

    /// The table's column record `(name, type, added-at epoch)`, when it has
    /// one. Diagnostics only.
    pub fn table_columns(&self, key: &str) -> Result<Option<Vec<(String, String, u64)>>> {
        Ok(self.store.table(key, None)?.and_then(|entry| {
            entry.columns.map(|columns| {
                columns
                    .into_iter()
                    .map(|c| (c.name, c.dtype, c.added))
                    .collect()
            })
        }))
    }

    /// Create a lake whose **data** is the configured S3 bucket/prefix and
    /// whose catalog is `catalog_url` (a `pink:`, `sqlite:` or Postgres
    /// catalog). This is the explicit-config counterpart of
    /// [`Lake::create_with_catalog`] (which reads credentials from the
    /// environment).
    #[cfg(feature = "s3")]
    pub fn create_with_catalog_s3(catalog_url: &str, config: S3Config) -> Result<Self> {
        let location = Location::parse(catalog_url)?;
        if !matches!(
            location,
            Location::Pink(_)
                | Location::Sqlite(_)
                | Location::Postgres(_)
                | Location::CatalogS3 { .. }
        ) {
            return Err(LakeError::BadLocation {
                location: redact_location(catalog_url),
                message: "the first argument is the lake catalog, not the data location"
                    .to_string(),
            });
        }
        let data_location = s3_location(&config);
        let data: Arc<dyn Storage> = Arc::new(s3::S3Storage::new(config)?);
        let store = catalog_store::open_metadata_store_keyed(catalog_url, &location, true, None)?
            .ok_or_else(|| catalog_feature_missing(catalog_url, &location))?;
        if crate::metadata::MetadataStore::head_version(&store)?.is_some() {
            return Err(LakeError::AlreadyALake {
                path: PathBuf::from(redact_location(catalog_url)),
            });
        }
        ensure_data_location_free(&*data, &data_location)?;
        Self::create_sql_catalog_data(catalog_url, store, &data_location, data, None)
    }

    /// Open a lake whose data is the configured S3 bucket/prefix and whose
    /// catalog is `catalog_url`.
    #[cfg(feature = "s3")]
    pub fn open_with_catalog_s3(catalog_url: &str, config: S3Config) -> Result<Self> {
        let location = Location::parse(catalog_url)?;
        let data_location = s3_location(&config);
        let data: Arc<dyn Storage> = Arc::new(s3::S3Storage::new(config)?);
        let store = catalog_store::open_metadata_store_keyed(catalog_url, &location, false, None)?
            .ok_or_else(|| catalog_feature_missing(catalog_url, &location))?;
        Self::open_sql_catalog_data(catalog_url, store, &data_location, data, None)
    }

    /// The lake location: a directory path or the `s3://bucket/prefix` URL.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The catalog location this handle's errors name: the URL it was opened
    /// with, password redacted (`pink:<path>`, `sqlite:<path>` or a Postgres
    /// URL).
    pub fn catalog_label(&self) -> &str {
        &self.catalog
    }

    /// [`Lake::catalog_label`] as a path, for the error-attribution helpers.
    fn catalog_path(&self) -> &Path {
        Path::new(&self.catalog)
    }

    /// The catalog `format_version` this handle last saw: the version the lake
    /// was opened with, or [`FORMAT_VERSION`] after a successful save.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// The current version (the catalog's head, cached after open/save).
    pub fn current_version(&self) -> u64 {
        self.version
    }

    /// The lake's random 128-bit identity (32 lowercase hex characters), as
    /// recorded in the catalog (M-7).
    pub fn lake_id(&self) -> &str {
        &self.lake_id
    }

    /// Whether this handle is a keyed (encrypted) lake session. A handle that
    /// opened or created a lake successfully with a key is always encrypted;
    /// one created without a key always writes plaintext.
    pub fn is_encrypted(&self) -> bool {
        self.storage.encrypted()
    }

    /// Record the author and commit message for the **next** published save,
    /// like DuckLake's `ducklake_set_commit_message(catalog, author, message)`:
    /// the metadata lands on the version that save publishes. `None` clears a
    /// field; the pair is cleared once a save succeeds and kept when one
    /// fails, so a retry still carries it.
    pub fn set_commit_message(&mut self, author: Option<String>, message: Option<String>) {
        self.pending_commit = Some((author, message));
    }

    /// Stage the engine catalog image (wave 96b) for the next published
    /// commit: the engine's metadata and this commit's table data become one
    /// version, so a reader never sees one without the other.
    pub fn set_engine_catalog(&mut self, image: Vec<u8>) {
        self.pending_engine_catalog = Some(Arc::new(image));
    }

    /// Every schema the lake has at the head version, in the catalog's
    /// spelling (DuckLake keeps an empty schema as a `ducklake_schema` row).
    pub fn schemas(&self) -> Result<Vec<String>> {
        self.store.schemas(None)
    }

    /// The engine catalog image at the head version, if the catalog carries
    /// one (`None` for a lake written by a `--file` server or the CLI).
    pub fn engine_catalog(&self) -> Result<Option<(u64, Vec<u8>)>> {
        self.store.engine_catalog()
    }

    /// Publish the engine catalog image on its own, without a data commit —
    /// the first image of a lake (whose tables are already published) or a
    /// resync after the engine was rebuilt. Takes over a staged image, so a
    /// later commit does not publish it a second time.
    pub fn publish_engine_catalog(&mut self, image: &[u8]) -> Result<()> {
        self.store.put_engine_catalog(self.version, image)?;
        self.pending_engine_catalog = None;
        Ok(())
    }

    /// The catalog's metadata store, for the engine lease
    /// ([`crate::EngineLease`]) and the opaque side objects it keeps there.
    pub(crate) fn store_handle(&self) -> Arc<dyn crate::metadata::MetadataStore> {
        Arc::clone(&self.store)
    }

    /// This handle's default `data_inlining_row_limit` (DuckLake's session
    /// setting `ducklake_default_data_inlining_row_limit`, 10 by default; 0
    /// disables inlining). Applied to a table that does not record its own
    /// limit ([`TableEntry::inlined_limit`], set through
    /// [`Lake::set_inlining_limit`]).
    pub fn data_inlining_row_limit(&self) -> u64 {
        self.inline_row_limit
    }

    /// Set this handle's default `data_inlining_row_limit` (in memory only,
    /// like DuckLake's per-session setting). A table that records its own
    /// limit is unaffected; a per-table limit is published by
    /// [`Lake::set_inlining_limit`].
    pub fn set_data_inlining_row_limit(&mut self, limit: u64) {
        self.inline_row_limit = limit;
    }

    /// The per-version records of every published version, ascending: the
    /// publish time (`updated`), author, commit message and change summary of
    /// each manifest, plus its table and row counts (see [`SnapshotInfo`]).
    ///
    /// The set is exactly [`Lake::versions`]: only manifests at or below the
    /// handle's current version, named canonically, that parse and declare
    /// their own version. Reading a snapshot that disappears between the
    /// listing and this read (an expired or deleted one) skips it, exactly
    /// like [`Lake::versions`].
    pub fn snapshots(&self) -> Result<Vec<SnapshotInfo>> {
        let mut out = Vec::new();
        let mut previous: Option<Catalog> = None;
        for version in self.versions()? {
            match manifest_for(&*self.store, self.catalog_path(), Some(version)) {
                Ok((mut manifest, path)) => {
                    if self.check_lake_id(&manifest, &path).is_err() {
                        continue;
                    }
                    // SQL catalogs persist the typed tables, not the audit
                    // summary; derive it from the adjacent manifests so the
                    // record matches a save-time summary.
                    if manifest.changes.is_none()
                        && let Some(previous) = &previous
                    {
                        manifest.changes = Some(snapshot::ChangeSummary::diff(
                            &previous.tables,
                            &manifest.tables,
                        ));
                    }
                    let time_micros = time::parse_iso8601_utc(&manifest.updated);
                    previous = Some(manifest.clone());
                    out.push(SnapshotInfo::of(&manifest, time_micros));
                }
                Err(LakeError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue;
                }
                Err(LakeError::VersionNotFound { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Published snapshot versions, ascending: every version the catalog
    /// holds at or below this handle's head.
    pub fn versions(&self) -> Result<Vec<u64>> {
        published_versions(&*self.store, self.version)
    }

    /// The current version's tables: `(db.table, rows)`, sorted by name.
    pub fn tables(&self) -> Result<Vec<(String, u64)>> {
        let (manifest, _) = self.manifest(None)?;
        Ok(manifest
            .tables
            .into_iter()
            .map(|(key, entry)| (key, entry.rows))
            .collect())
    }

    /// The current version's per-table identity (wave 89): `(db.table,
    /// table_id)`, sorted by name. `table_id` is the entry's random 128-bit
    /// hex string, `None` for an entry written by a build before wave 89
    /// (the next version this build publishes assigns one to every entry
    /// that lacks it).
    ///
    /// This is the read-only identity replication compares across cycles: a
    /// different id under the same key means the source table was dropped and
    /// recreated, so a replica must reseed instead of trusting its cursors.
    pub fn table_ids(&self) -> Result<Vec<(String, Option<String>)>> {
        let (manifest, _) = self.manifest(None)?;
        Ok(manifest
            .tables
            .into_iter()
            .map(|(key, entry)| (key, entry.table_id))
            .collect())
    }

    /// Rebuild a [`PinkDb`] from the latest version (`None`) or a specific
    /// snapshot. Views are session-local in the engine and never come back.
    ///
    /// A table whose data-file format this build cannot read is skipped
    /// (wave 88, rule 5): [`Lake::unavailable_tables`] lists it, and the
    /// readable tables load normally.
    pub fn load(&self, version: Option<u64>) -> Result<PinkDb> {
        let (manifest, manifest_path) = self.manifest(version)?;
        self.rebuild(&manifest, &manifest_path)
    }

    /// Statistics-only *attach* of the latest version (`None`) or a specific
    /// snapshot: read the manifest and the `.pink` files' published statistics
    /// blocks — no data file is opened, encrypted or not, and no column is
    /// decoded.
    ///
    /// The returned [`AttachmentReport`] carries the version-pinned
    /// [`pinkdb::AttachedLake`] handle; register it with
    /// [`pinkdb::PinkDb::attach_lake`] to make the tables queryable. Attach
    /// validates what `.lake open` would validate when it loads: the file's
    /// identity against its catalog key, the declared schema, and the row
    /// count against the manifest. It never mutates the lake.
    ///
    /// A multi-file table (the v4 incremental format) merges its files'
    /// statistics when every file carries a block and the table has no
    /// positional deletes; once a delete file exists the per-column facts can
    /// no longer be proven without reading data, so those columns fall back
    /// to materialization (the row count stays exact). A `.pink` entry
    /// without statistics blocks is refused with a clear error; `.lake open`
    /// remains the way to load it.
    ///
    /// A table whose data-file format this build cannot read (wave 88, rule
    /// 5) is **listed** — [`Lake::unavailable_tables`] names it — but every
    /// read of it, including the `COUNT`/`MIN`/`MAX` fast paths, fails with
    /// the provider error naming the table and the id.
    pub fn attach(&self, version: Option<u64>) -> Result<AttachmentReport> {
        let before = self.storage.stats();
        let (_, mut report) = attach::build(self, version)?;
        let after = self.storage.stats();
        report.requests = after.requests.saturating_sub(before.requests);
        report.bytes = after.bytes.saturating_sub(before.bytes);
        report.wait_nanos = after.wait_nanos.saturating_sub(before.wait_nanos);
        Ok(report)
    }

    /// This handle's transport totals since it was opened: request attempts,
    /// response bytes and wall time spent waiting on the network. Local lakes
    /// report zeros. Wave 98b diagnostics; a caller can snapshot before and
    /// after a query to see exactly what it read.
    pub fn transport_stats(&self) -> (u64, u64, u64) {
        let stats = self.storage.stats();
        (stats.requests, stats.bytes, stats.wait_nanos)
    }

    /// The tables of the current version this build cannot read (wave 88,
    /// rule 5): each entry's format id has no registered provider in this
    /// process ([`LakeError::UnknownFormat`]).
    ///
    /// Such a table no longer refuses the lake by itself: [`Lake::open`] and
    /// [`Lake::attach`] succeed, [`Lake::load`] skips it, and
    /// [`Lake::attach`] lists it while every read of it fails with the listed
    /// error. The result is sorted by catalog key (the manifest's
    /// `BTreeMap` order).
    pub fn unavailable_tables(&self) -> Result<Vec<UnavailableTable>> {
        self.unavailable_tables_at(None)
    }

    /// [`Lake::unavailable_tables`] for the latest version (`None`) or one
    /// snapshot.
    pub fn unavailable_tables_at(&self, version: Option<u64>) -> Result<Vec<UnavailableTable>> {
        let (manifest, path) = self.manifest(version)?;
        let mut out = Vec::new();
        for (key, entry) in &manifest.tables {
            let format = entry.format();
            if let Err(error) = crate::format::format_for(format.id(), &path) {
                out.push(UnavailableTable {
                    table: key.clone(),
                    format: format.id().to_string(),
                    error,
                });
            }
        }
        Ok(out)
    }

    /// The published commit-delta versions, ascending: every version at or
    /// below the current one whose delta the catalog kept (from version 2
    /// on). The deltas are the append-only audit trail of how each version was
    /// built (which data and delete files it added); the snapshot record
    /// stays authoritative for loading.
    pub fn commits(&self) -> Result<Vec<u64>> {
        Ok(self
            .store
            .snapshot_versions()?
            .into_iter()
            .filter(|version| *version > 1 && *version <= self.version)
            .collect())
    }

    /// Referenced data files that are missing (or not a valid `data/…` path):
    /// `(table key, path)`, sorted by table then path. This is the cheap
    /// integrity check behind `.lake tables`/`.lake info`: a table can only
    /// be advertised while every data file — and every positional delete
    /// file — it names is present. It does not read the files (that is what
    /// [`Lake::load`] does).
    pub fn missing_data_files(&self) -> Result<Vec<(String, PathBuf)>> {
        let (manifest, _) = self.manifest(None)?;
        let mut out = Vec::new();
        for (key, entry) in &manifest.tables {
            for rel in entry
                .files
                .iter()
                .map(FileRef::path)
                .chain(entry.deletes.iter().map(|d| d.path.as_str()))
            {
                if validate_rel_path(rel).is_err() || !matches!(self.storage.head(rel)?, Head::File)
                {
                    out.push((key.clone(), self.storage.abs(rel)));
                }
            }
        }
        Ok(out)
    }

    /// Publish a full snapshot of `db` and return the new version.
    pub fn write_from_store(&mut self, db: &PinkDb) -> Result<u64> {
        Ok(self.write_from_store_report(db)?.version)
    }

    /// [`Lake::write_from_store`] with the details the CLI reports.
    ///
    /// Fails before writing anything when the store cannot be represented
    /// ([`validate_store`]) or when the handle is stale ([`LakeError::Conflict`]);
    /// the catalog transaction re-checks the head, so a concurrent publication
    /// can never produce a catalog that disagrees with the snapshot of its
    /// version.
    pub fn write_from_store_report(&mut self, db: &PinkDb) -> Result<SaveReport> {
        validate_store(db)?;
        // Fail fast on a handle another writer has already outdated (before
        // any data file is written).
        self.ensure_catalog_unchanged()?;
        let version = self
            .version
            .checked_add(1)
            .filter(|v| *v <= MAX_VERSION)
            .ok_or_else(|| LakeError::Overflow {
                path: self.catalog_path().to_path_buf(),
                version: self.version,
            })?;
        // The previous version's manifest, read before the data loop: a full
        // save **preserves the partitioning** of a table that is saved again
        // under the same key (the store itself has no partition concept), so
        // re-saving a partitioned table re-splits its rows the same way. The
        // handle is at the head (`ensure_catalog_unchanged` above), so fall
        // back to the head manifest when the previous snapshot read fails: an
        // unreadable predecessor must not silently reset the lake-wide
        // monotonic `schema_version` (or drop a partition spec). If even the
        // head cannot be read, the save fails loudly.
        let previous = manifest_for(&*self.store, self.catalog_path(), Some(self.version))
            .ok()
            .map(|(manifest, _)| manifest);
        // The head catalog is the authoritative entry set. A table this build
        // cannot read is carried forward verbatim when the store has no table
        // under its key; a store table under that key would replace data this
        // build cannot read, so the save is refused (wave 88, rule 5).
        let head = manifest_for(&*self.store, self.catalog_path(), None)?.0;
        let head_path = self.catalog_path().to_path_buf();
        let mut store_keys: BTreeSet<String> = BTreeSet::new();
        for dbname in db.list_databases() {
            for name in db.list_tables_in(&dbname) {
                store_keys.insert(format!("{dbname}.{name}"));
            }
        }
        let mut carried: Vec<(String, TableEntry)> = Vec::new();
        for (key, entry) in &head.tables {
            // A malformed key is corrupt metadata in any entry, readable or
            // not; never carry it into a new catalog.
            split_key(key, &head_path)?;
            let format = entry.format();
            if crate::format::format_for(format.id(), &head_path).is_ok() {
                continue;
            }
            if store_keys.contains(key) {
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "table \"{key}\" uses data-file format \"{}\", which this build cannot \
                         read, but the store has a table with that key: saving would replace \
                         data this build cannot read (drop that store table, or save it under \
                         another name)",
                        format.id()
                    ),
                });
            }
            carried.push((key.clone(), entry.clone()));
        }
        let _has_carried = !carried.is_empty();
        let mut tables: BTreeMap<String, TableEntry> = BTreeMap::new();
        let mut deltas: BTreeMap<String, catalog::TableDelta> = BTreeMap::new();
        let mut bytes_written = 0u64;
        let mut rows = 0u64;
        for dbname in db.list_databases() {
            for name in db.list_tables_in(&dbname) {
                let src = table_in(db, &dbname, &name)?;
                let key = format!("{dbname}.{name}");
                let previous_entry = previous.as_ref().and_then(|p| p.tables.get(&key));
                let keys: Vec<PartitionKey> = previous_entry
                    .and_then(|e| e.partition_by.clone())
                    .unwrap_or_default();
                // Wave 102: a table **keeps its format** on every save (no
                // migration): the handle's per-table choice wins, else the
                // table's existing catalog format, else the handle's write
                // format for a table the save creates. One data file per
                // partition value set (exactly one for an unpartitioned or
                // empty table), written before anything references it (see
                // [`Lake::write_data_files`] for the encoders). Every file of
                // a recorded entry then carries the schema epoch it was
                // written under (1: a full save writes the current schema).
                let format = self.resolve_save_format(&key, previous_entry);
                let (files, written) = self.write_data_files(&key, format.clone(), &keys, src)?;
                bytes_written += written;
                rows += src.num_rows() as u64;
                // Wave 68: the save folds every live inlined row into the new
                // data files (the store's tables already contain them), so it
                // publishes fresh row ids for them — the counter continues
                // from the table's recorded one, never backwards. The inlined
                // *history* is dropped: the rows are file-backed from this
                // version on, and a change-data read of a range that includes
                // this full-save version is refused anyway (the delta marks
                // the table replaced).
                let mut next_row_id = previous_entry
                    .map(|e| {
                        e.next_row_id
                            .max(e.max_row_id().map_or(0, |m| m.saturating_add(1)))
                    })
                    .unwrap_or(0);
                let files: Vec<crate::catalog::FileRef> = files
                    .into_iter()
                    .map(|f| {
                        let start = f.rows().filter(|r| *r > 0).map(|r| {
                            let start = next_row_id;
                            next_row_id = next_row_id.saturating_add(r);
                            start
                        });
                        f.with_schema_version(Some(1)).with_row_id_start(start)
                    })
                    .collect();
                // The v5 schema record: the store table's live columns, with
                // their defaults, all at epoch 1 — a full save rewrites every
                // data file under the current schema, so no history is needed.
                let columns = src
                    .columns
                    .iter()
                    .map(|c| {
                        crate::catalog::ColumnEntry::new(&c.name, c.dtype, c.default.as_ref(), 1)
                    })
                    .collect::<Result<Vec<_>>>()?;
                deltas.insert(
                    key.clone(),
                    catalog::TableDelta {
                        runtime: Default::default(),
                        data_files: files.clone(),
                        delete_files: Vec::new(),
                        rows_added: src.num_rows() as u64,
                        rows_deleted: 0,
                        replaced: true,
                        partition_changed: false,
                        partition_by: None,
                        compacted_files: Vec::new(),
                        removed_delete_files: Vec::new(),
                        rows_inlined: 0,
                        inlined_ended: 0,
                        inlined_flushed: 0,
                        inlined_limit: None,
                        inlining_changed: false,
                    },
                );
                tables.insert(
                    key,
                    TableEntry {
                        files: files
                            .iter()
                            .map(|file| file.with_schema_version(Some(1)))
                            .collect(),
                        rows: src.num_rows() as u64,
                        // Wave 89: a save preserves the table identity of the
                        // same key (a dropped + recreated table was dropped by
                        // an earlier published version, so this lookup no
                        // longer finds the old entry); a fresh table is
                        // assigned one below.
                        table_id: previous_entry.and_then(|e| e.table_id.clone()),
                        format: Some(format.clone()),
                        stats: None,
                        deletes: Vec::new(),
                        partition_by: (!keys.is_empty()).then_some(keys),
                        schema_version: 1,
                        columns: Some(columns),
                        renames: Vec::new(),
                        inlined: Vec::new(),
                        // A table's inlining setting survives a save; the
                        // counter continues (see above).
                        inlined_limit: previous_entry.and_then(|e| e.inlined_limit),
                        next_row_id,
                    },
                );
            }
        }
        // Unavailable entries ride through the save verbatim: same files,
        // statistics, partition spec and schema record (the store
        // had no table under their keys, checked above). They are not a
        // change, so the delta manifest lists nothing for them.
        for (key, entry) in carried {
            rows = rows.saturating_add(entry.rows);
            tables.insert(key, entry);
        }
        // Wave 89: every entry this version publishes carries an identity —
        // a table this save created gets a fresh one, a pre-wave-89 entry
        // acquires one here.
        catalog::assign_missing_table_ids(&mut tables)?;
        // A SQL catalog applies the save as a typed delta: each table it
        // rewrote is replaced in place, and a table the store no longer has is
        // dropped. (An unreadable carried entry, if any, uses the full-catalog
        // compatibility path below.)
        for (key, entry) in &tables {
            if let Some(delta) = deltas.get_mut(key) {
                delta.runtime = catalog::TableRuntime {
                    meta: Some(catalog::TableMeta::from_entry(entry)),
                    created: !head.tables.contains_key(key),
                    dropped: false,
                    kind: catalog::DeltaKind::Save,
                    inlined_rows: Vec::new(),
                    inlined_ends: Vec::new(),
                };
            }
        }
        for key in head.tables.keys() {
            if tables.contains_key(key) {
                continue;
            }
            deltas
                .entry(key.clone())
                .or_insert_with(|| catalog::TableDelta {
                    runtime: catalog::TableRuntime {
                        meta: None,
                        created: false,
                        dropped: true,
                        kind: catalog::DeltaKind::Save,
                        inlined_rows: Vec::new(),
                        inlined_ends: Vec::new(),
                    },
                    data_files: Vec::new(),
                    delete_files: Vec::new(),
                    rows_added: 0,
                    rows_deleted: 0,
                    replaced: false,
                    partition_changed: false,
                    partition_by: None,
                    compacted_files: Vec::new(),
                    removed_delete_files: Vec::new(),
                    rows_inlined: 0,
                    inlined_ended: 0,
                    inlined_flushed: 0,
                    inlined_limit: None,
                    inlining_changed: false,
                });
        }
        let (author, commit_message) = match &self.pending_commit {
            Some((author, message)) => (author.clone(), message.clone()),
            None => (None, None),
        };
        let schema_version = next_schema_version(previous.as_ref(), &tables);
        let changes = previous
            .as_ref()
            .map(|previous| ChangeSummary::diff(&previous.tables, &tables));
        let manifest = Catalog {
            format_version: FORMAT_VERSION,
            lake_id: Some(self.lake_id.clone()),
            updated: time::iso8601_utc(SystemTime::now()),
            version,
            tables,
            author,
            commit_message,
            changes,
            schema_version,
        };
        let commit_manifest = catalog::CommitManifest {
            format_version: FORMAT_VERSION,
            version,
            base_version: version - 1,
            updated: manifest.updated.clone(),
            tables: deltas,
        };
        let table_count = manifest.tables.len();
        let change = crate::metadata::CatalogChange {
            catalog: None,
            manifest: commit_manifest,
            schema_version: manifest.schema_version,
            format_version: manifest.format_version,
            updated: manifest.updated.clone(),
            lake_id: manifest.lake_id.clone(),
            author: manifest.author.clone(),
            commit_message: manifest.commit_message.clone(),
            engine_catalog: self.pending_engine_catalog.clone(),
            schemas: None,
        };
        self.publish_sql_commit(change)?;
        self.pending_commit = None;
        self.pending_engine_catalog = None;
        Ok(SaveReport {
            version,
            tables: table_count,
            rows,
            bytes_written,
        })
    }

    /// Choose the registered data-file format this handle writes for every
    /// subsequent save/append that does not pin a format explicitly (see the
    /// [`crate::format`] module). The id must have a provider registered in
    /// this process — an unknown id is [`LakeError::UnknownFormat`]; nothing
    /// is silently substituted.
    ///
    /// The choice is per handle and not persisted: [`Lake::open`] and
    /// [`Lake::create`] always start with the build default (`.pink`). It
    /// applies to a table with no per-table choice
    /// ([`Lake::set_table_format`]) and no already-pinned catalog format — a
    /// full save preserves an existing table's format, and a commit appends
    /// in it. Set it on the handle performing the write; maintenance rewrites
    /// always re-encode under the entry's own pinned format (or the target of
    /// [`Lake::rewrite_table_format`]).
    pub fn set_write_format(&mut self, id: &str) -> Result<()> {
        crate::format::format_for(id, self.catalog_path())?;
        self.write_format_choice = Some(FileFormat::new(id));
        Ok(())
    }

    /// Choose the data-file format for one table on this handle, keyed by
    /// `db.table` (wave 88). The id is validated against the registry exactly
    /// like [`Lake::set_write_format`] — an id with no provider is
    /// [`LakeError::UnknownFormat`], never a silent substitution.
    ///
    /// The choice wins over the format the table's catalog entry already
    /// pins for a **full save** (so a per-table choice is how a save asks for
    /// a table to be re-encoded), and it selects the format of a table the
    /// handle creates through a commit. An **append** to an existing table
    /// always keeps that entry's pinned format — one catalog entry names one
    /// data-file format, so a commit or flush never mixes formats inside it.
    /// To convert an existing table use [`Lake::rewrite_table_format`].
    ///
    /// Like [`Lake::set_write_format`], the choice is in memory and applies
    /// to this handle only.
    pub fn set_table_format(&mut self, db: &str, table: &str, id: &str) -> Result<()> {
        if db.contains('.') {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "database name \"{db}\" contains \".\" and cannot key a per-table format \
                     choice (catalog keys are flat `db.table`)"
                ),
            });
        }
        crate::format::format_for(id, self.catalog_path())?;
        self.table_format_choices
            .insert(format!("{db}.{table}"), FileFormat::new(id));
        Ok(())
    }

    /// The data-file format this handle writes by default: the one
    /// [`Lake::set_write_format`] selected, else engine `.pink` bytes.
    fn write_format(&self) -> FileFormat {
        self.write_format_choice
            .clone()
            .unwrap_or_else(default_write_format)
    }

    /// The format a full save of `key` writes (wave 102 — **no migration**):
    ///
    /// 1. the handle's per-table choice ([`Lake::set_table_format`]) — that
    ///    format;
    /// 2. else the table's existing catalog format — it is kept, whatever it
    ///    is;
    /// 3. else (a table the save creates) the handle's write format.
    ///
    /// A table therefore **keeps its format on every save and append**;
    /// changing it is explicit only — [`Lake::set_table_format`],
    /// [`Lake::rewrite_table_format`], or the CLI's `.lake format … for …`
    /// / `.lake rewrite … format …`. The wave-88 hybrid migration rule (and
    /// the `format_pinned` flag it needed) is gone: a build never
    /// re-encodes a table into another format behind the user's back.
    fn resolve_save_format(&self, key: &str, previous: Option<&TableEntry>) -> FileFormat {
        if let Some(format) = self.table_format_choices.get(key) {
            return format.clone();
        }
        if let Some(entry) = previous {
            return entry.format();
        }
        self.write_format()
    }

    /// The format a commit/flush writes for `key` (wave 102): an existing
    /// entry's format always wins (an append never mixes formats inside one
    /// entry); a table the commit creates uses the handle's per-table choice,
    /// else its write format.
    fn resolve_append_format(&self, key: &str, entry: Option<&TableEntry>) -> FileFormat {
        match entry {
            Some(entry) => entry.format(),
            None => match self.table_format_choices.get(key) {
                Some(format) => format.clone(),
                None => self.write_format(),
            },
        }
    }

    /// Write one immutable data file for `key` holding `src`, returning its
    /// manifest reference (path, row count, and the CRC-protected
    /// statistics/zone-map block the writer published for it) and the bytes
    /// written. `format` is the caller's resolved write format (wave 88:
    /// [`Lake::resolve_save_format`] for a full save,
    /// [`Lake::resolve_append_format`] for a commit/flush, or a maintenance
    /// target).
    ///
    /// The encoding is the registered provider's
    /// ([`format::LakeFormat`](crate::format::LakeFormat)): it streams its
    /// bytes through `Storage::put_streamed` (a unique `create_new` temp
    /// sibling plus fsync + rename locally; one atomic PUT over S3; sealed as
    /// one container for an encrypted lake) and publishes the exact
    /// per-column facts as the file's catalog statistics block, so
    /// `.lake attach` answers `COUNT`/`MIN`/`MAX` and prunes by zone map
    /// without opening the file.
    fn write_data_file(
        &self,
        key: &str,
        format: FileFormat,
        src: &DataTable,
    ) -> Result<(FileRef, u64)> {
        let (dbname, table) = split_key(key, self.catalog_path())?;
        let provider = crate::format::format_for(format.id(), self.catalog_path())?;
        let dir = format!("data/{}", encode_key(key));
        let id = fresh_id();
        let rows = src.num_rows() as u64;
        let rel = format!("{dir}/{id}.{}", provider.extension());
        // The catalog's column ids, before the commit that creates them: a
        // DuckLake reader matches a nested column's children by `field_id`,
        // and a data file is written before its columns exist.
        let fields = self.store.field_ids(key, &src.schema())?;
        let mut saved = None;
        let written = self.storage.put_streamed(&rel, &mut |sink| {
            saved = Some(provider.write_streamed_with_fields(dbname, table, src, &fields, sink)?);
            Ok(())
        })?;
        let (saved, footer) = saved.ok_or_else(|| LakeError::Internal {
            message: format!(
                "the \"{}\" format provider returned no statistics for \"{key}\"",
                provider.id()
            ),
        })?;
        let block = stats::StatsBlock::from_saved(key, &rel, &saved)?;
        Ok((
            FileRef::Entry {
                path: rel,
                rows,
                schema_version: None,
                stats: Some(block),
                partition: None,
                row_id_start: None,
                // Wave 102: the file's footer location, so a reader finds
                // the footer without probing the file's trailer.
                footer_offset: footer.map(|f| f.offset),
                footer_len: footer.map(|f| f.length),
                footer_crc32c: footer.map(|f| f.crc32c),
                stats_sealed: true,
            },
            written,
        ))
    }

    /// Write `src` as one or more data files under the partition spec `keys`
    /// (wave 66): with keys, the rows are grouped by their partition value
    /// set — one file per distinct set, ordered deterministically by the
    /// label tuple — and every file records its own
    /// [`FileRef::partition`] values. An unpartitioned table, or an empty
    /// one, still gets exactly one file (a 0-row file carries no partition
    /// value). Returns the manifest references in file order and the bytes
    /// written.
    ///
    /// DuckLake semantics: inserting rows is what assigns partitions — each
    /// distinct value set lands in its own data file, and a later commit
    /// appends another file per value set (files are never merged here; that
    /// is wave 67's `merge_adjacent_files`). Unlike DuckLake, the file path
    /// stays a flat `data/<key>/<uuid>` — partition values live in the
    /// catalog, not the path.
    fn write_data_files(
        &self,
        key: &str,
        format: FileFormat,
        keys: &[PartitionKey],
        src: &DataTable,
    ) -> Result<(Vec<FileRef>, u64)> {
        if keys.is_empty() || src.num_rows() == 0 {
            let (file, written) = self.write_data_file(key, format, src)?;
            return Ok((vec![file], written));
        }
        let schema = src.schema();
        let mut plan: Vec<(&PartitionKey, usize, DataType)> = Vec::with_capacity(keys.len());
        for k in keys {
            let idx = zone::schema_column_index(&schema, &k.column).ok_or_else(|| {
                LakeError::NotRepresentable {
                    message: format!(
                        "table \"{key}\" partitions on column \"{}\", but its schema has no \
                         such column (columns: {})",
                        k.column,
                        schema
                            .iter()
                            .map(|(n, _)| n.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                }
            })?;
            plan.push((k, idx, schema[idx].1));
        }
        // Group flat row indices by the label tuple, preserving each group's
        // source order (ascending indices).
        let mut groups: BTreeMap<Vec<Option<String>>, Vec<usize>> = BTreeMap::new();
        let mut row = 0usize;
        for c in 0..src.num_chunks() {
            let chunk = src.scan_chunk(c).ok_or_else(|| LakeError::Internal {
                message: format!("chunk {c} of \"{key}\" disappeared while partitioning"),
            })?;
            for r in 0..chunk.num_rows {
                let mut label = Vec::with_capacity(plan.len());
                for (k, idx, dtype) in &plan {
                    let value = chunk.columns[*idx].get(r);
                    label.push(zone::partition_label(k, *dtype, &value).ok_or_else(|| {
                        LakeError::NotRepresentable {
                            message: format!(
                                "table \"{key}\": partition value {value:?} of column \"{}\" \
                                 cannot be recorded",
                                k.column
                            ),
                        }
                    })?);
                }
                groups.entry(label).or_default().push(row);
                row += 1;
            }
        }
        let mut files = Vec::with_capacity(groups.len());
        let mut written = 0u64;
        for (label, indices) in groups {
            let sub = crate::incremental::sub_table(src, &indices)?;
            let (file, bytes) = self.write_data_file(key, format.clone(), &sub)?;
            written += bytes;
            let partition: BTreeMap<String, Option<String>> = label
                .into_iter()
                .zip(keys.iter())
                .map(|(value, k)| (k.key_text(), value))
                .collect();
            files.push(file.with_partition(partition));
        }
        Ok((files, written))
    }

    /// Write one positional delete file for `key`'s `file` (see the
    /// `delete_file` module) and return its manifest reference plus the bytes
    /// written.
    fn write_delete_file(
        &self,
        key: &str,
        file: &str,
        positions: &[u64],
        format: &FileFormat,
    ) -> Result<(DeleteRef, u64)> {
        let (dbname, table) = split_key(key, self.catalog_path())?;
        validate_rel_path(file)?;
        let provider = crate::format::format_for(format.id(), self.catalog_path())?;
        let positions = delete_file::normalize(positions.to_vec());
        let dir = format!("data/{}", encode_key(key));
        let rel = format!("{dir}/{}.{}", fresh_id(), provider.delete_extension());
        let data_file_path = self.storage.abs(file).to_string_lossy().to_string();
        let bytes = provider.write_delete_file(dbname, table, file, &data_file_path, &positions)?;
        self.storage.put(&rel, &bytes)?;
        Ok((
            DeleteRef {
                path: rel,
                file: file.to_string(),
                count: positions.len() as u64,
                format: provider.delete_format().to_string(),
            },
            bytes.len() as u64,
        ))
    }

    /// The manifest for the latest version (`None`) or one snapshot, with the
    /// path it was read from (for error attribution). The lake identity is
    /// cross-checked here, so a snapshot substituted from another lake (same
    /// key) is refused even when it parses.
    fn manifest(&self, version: Option<u64>) -> Result<(Catalog, PathBuf)> {
        let (manifest, path) = manifest_for(&*self.store, self.catalog_path(), version)?;
        self.check_lake_id(&manifest, &path)?;
        Ok((manifest, path))
    }

    /// Enforce the v6 lake identity on one parsed manifest (M-7).
    fn check_lake_id(&self, manifest: &Catalog, path: &Path) -> Result<()> {
        if manifest.format_version >= 6
            && manifest.lake_id.as_deref() != Some(self.lake_id.as_str())
        {
            return Err(LakeError::Corrupt {
                path: path.to_path_buf(),
                message: format!(
                    "manifest declares lake {}, but this lake is {} (a manifest from another \
                     lake was substituted)",
                    manifest.lake_id.as_deref().unwrap_or("<none>"),
                    self.lake_id
                ),
            });
        }
        Ok(())
    }

    /// The catalog must still declare the version this handle was opened or
    /// last saved from; anything else means another writer published.
    fn ensure_catalog_unchanged(&self) -> Result<()> {
        let head = self.store.head_version()?;
        if head != Some(self.version) {
            return Err(LakeError::Conflict {
                path: self.catalog_path().to_path_buf(),
                message: format!(
                    "another writer published version {} (this handle is based on version \
                     {}); reopen the lake",
                    head.map(|v| v.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    self.version
                ),
            });
        }
        Ok(())
    }

    /// Build a [`PinkDb`] from a manifest by loading every referenced data
    /// file and applying every positional delete on top of it (merge-on-read,
    /// per file). Databases are recreated from the flat `db.table` keys;
    /// `main` always exists in the result (an empty `main` if it had no
    /// tables). `manifest_path` is the file the manifest came from (catalog
    /// or snapshot) and is what errors about the manifest point at.
    ///
    /// A table whose format this build cannot read is **skipped** (wave 88,
    /// rule 5): [`Lake::unavailable_tables`] lists it, and `.lake open` warns.
    fn rebuild(&self, manifest: &Catalog, manifest_path: &Path) -> Result<PinkDb> {
        let mut db = PinkDb::new();
        for (key, entry) in &manifest.tables {
            let (dbname, table) = split_key(key, manifest_path)?;
            if crate::format::format_for(entry.format().id(), manifest_path).is_err() {
                continue;
            }
            if !db.database_exists(dbname) {
                db.create_database(dbname).map_err(|e| LakeError::Engine {
                    context: format!("recreating database \"{dbname}\""),
                    source: e,
                })?;
            }
            let format = entry.format();
            let layout = self.entry_layout(entry, key, manifest_path)?;
            // The v5 schema record (wave 65), when this entry has one.
            let schema = crate::schema::TableSchema::from_entry(key, entry, manifest_path)?;
            let mut created = false;
            // A table whose rows are all inlined (or an empty committed
            // table) lists no data file: the schema record is the authority.
            if entry.files.is_empty() {
                let record = schema.as_ref().ok_or_else(|| LakeError::Corrupt {
                    path: manifest_path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" has no data files and no schema record; its schema is \
                         not recorded anywhere"
                    ),
                })?;
                db.create_table_in(dbname, table, record.schema_pairs())
                    .map_err(|e| LakeError::Engine {
                        context: format!("recreating table \"{key}\""),
                        source: e,
                    })?;
                let t = db
                    .table_mut_in(dbname, table)
                    .map_err(|e| LakeError::Engine {
                        context: format!("recreating table \"{key}\""),
                        source: e,
                    })?;
                for (col, record) in t.columns.iter_mut().zip(&record.columns) {
                    col.default = record.default.clone();
                }
                created = true;
            }
            for (i, (rel, file_rows, deleted)) in layout.iter().enumerate() {
                validate_rel_path(rel)?;
                let mini = self.open_data_file(rel, format.clone())?;
                let (file_db, file_table) = only_table(&mini, rel, &self.root)?;
                if !file_db.eq_ignore_ascii_case(dbname)
                    || !file_table.name.eq_ignore_ascii_case(table)
                {
                    return Err(LakeError::Corrupt {
                        path: self.storage.abs(rel),
                        message: format!(
                            "data file holds {file_db}.{}, but the catalog says \"{key}\"",
                            file_table.name
                        ),
                    });
                }
                if file_table.num_rows() as u64 != *file_rows {
                    return Err(LakeError::Corrupt {
                        path: self.storage.abs(rel),
                        message: format!(
                            "data file has {} rows, but the catalog says {file_rows}",
                            file_table.num_rows()
                        ),
                    });
                }
                let file_table = apply_file_deletes(file_table, deleted, key, rel)?;
                // Project the file onto the entry's logical schema: renamed
                // columns map to their current name, columns the file
                // predates materialize their default, dropped ones stay out.
                let file_table = match &schema {
                    Some(s) => {
                        let physical = file_table.schema();
                        let file_version = entry.files[i].schema_version().unwrap_or(s.version);
                        let mapping =
                            s.map_file(file_version, &physical, &self.storage.abs(rel))?;
                        let all_present = vec![true; physical.len()];
                        s.project_table(&mapping, &file_table, &all_present, table)?
                            .0
                    }
                    None => file_table,
                };
                if !created {
                    db.create_table_in(dbname, table, file_table.schema())
                        .map_err(|e| LakeError::Engine {
                            context: format!("recreating table \"{key}\""),
                            source: e,
                        })?;
                    if let Some(s) = &schema {
                        // The record's defaults become the rebuilt table's:
                        // a later INSERT that omits the column writes them,
                        // and a `.lake save` publishes them again.
                        let t = db
                            .table_mut_in(dbname, table)
                            .map_err(|e| LakeError::Engine {
                                context: format!("recreating table \"{key}\""),
                                source: e,
                            })?;
                        for (col, record) in t.columns.iter_mut().zip(&s.columns) {
                            col.default = record.default.clone();
                        }
                    }
                    created = true;
                }
                let dst = db
                    .table_mut_in(dbname, table)
                    .map_err(|e| LakeError::Engine {
                        context: format!("loading table \"{key}\""),
                        source: e,
                    })?;
                // Recorded entries project every file onto the same logical
                // schema; legacy entries keep the files-agree check.
                if schema.is_none() && i > 0 && dst.schema() != file_table.schema() {
                    return Err(LakeError::Corrupt {
                        path: self.storage.abs(rel),
                        message: format!("data files of table \"{key}\" disagree on the schema"),
                    });
                }
                copy_chunks(&file_table, dst, rel)?;
            }
            // The live inlined rows union after every data file (the
            // documented wave-68 scan order; see `crate::inlined`).
            if !entry.inlined.is_empty() {
                let record = schema.as_ref().ok_or_else(|| LakeError::Corrupt {
                    path: manifest_path.to_path_buf(),
                    message: format!("table \"{key}\" carries inlined rows but no schema record"),
                })?;
                let live: Vec<crate::catalog::InlinedRow> =
                    entry.live_inlined().into_iter().cloned().collect();
                let (inlined, _) = crate::inlined::materialize_live(
                    &live,
                    record,
                    key,
                    table,
                    None,
                    manifest_path,
                )?;
                let dst = db
                    .table_mut_in(dbname, table)
                    .map_err(|e| LakeError::Engine {
                        context: format!("loading table \"{key}\""),
                        source: e,
                    })?;
                if dst.schema() != inlined.schema() {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.to_path_buf(),
                        message: format!(
                            "table \"{key}\": the inlined rows materialize under a different \
                             schema than the table's data files"
                        ),
                    });
                }
                copy_chunks(&inlined, dst, "inlined rows")?;
            }
            let loaded = table_in(&db, dbname, table)?;
            if loaded.num_rows() as u64 != entry.rows {
                return Err(LakeError::Corrupt {
                    path: manifest_path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" has {} rows, but the catalog says {}",
                        loaded.num_rows(),
                        entry.rows
                    ),
                });
            }
        }
        Ok(db)
    }

    /// Decode one data file into its one-table store (the caller checks the
    /// file's identity, schema and row count against the manifest). The
    /// entry's format id is resolved through the provider registry — an id
    /// with no provider is a clean [`LakeError::UnknownFormat`].
    fn open_data_file(&self, rel: &str, format: FileFormat) -> Result<PinkDb> {
        let provider = crate::format::format_for(format.id(), self.catalog_path())?;
        let path = self.storage.abs(rel);
        let file = crate::format::DataFile::new(&path, rel, Arc::clone(&self.storage));
        provider.read(&file)
    }
}

/// The build's default data-file format: engine `.pink` bytes.
fn default_write_format() -> FileFormat {
    FileFormat::pink()
}

/// Snapshot the store into the lake at `dir`, creating the lake if `dir` is
/// not one yet. Returns the published version. The store is validated
/// *before* the lake directory is created, so a rejected store leaves
/// nothing behind.
pub fn save_store(db: &PinkDb, dir: impl AsRef<Path>) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create(dir)?.write_from_store(db)
}

/// [`save_store`] with at-rest encryption: creates an encrypted lake at a
/// fresh location and appends an encrypted version to an existing keyed one;
/// a *plaintext* lake at `dir` is refused ([`LakeError::NotEncrypted`]) — the
/// plaintext→encrypted migration is a save to a new location, never a silent
/// rewrite.
pub fn save_store_with_key(db: &PinkDb, dir: impl AsRef<Path>, key: EncryptionKey) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create_with_key(dir, key)?.write_from_store(db)
}

/// Snapshot the store into the lake at `location`: a local directory or an
/// `s3://bucket/prefix` URL (with the `s3` feature; credentials/endpoint come
/// from the environment exactly as [`Lake::open_url`] documents). The store
/// is validated *before* any lake is created, so a rejected store leaves
/// nothing behind.
pub fn save_store_url(db: &PinkDb, location: &str) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create_url(location)?.write_from_store(db)
}

/// [`save_store_url`] with at-rest encryption (local directory or
/// `s3://bucket/prefix`).
pub fn save_store_url_with_key(db: &PinkDb, location: &str, key: EncryptionKey) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create_url_with_key(location, key)?.write_from_store(db)
}

#[cfg(feature = "s3")]
/// [`save_store_url`] for an explicit S3 configuration: the lake's catalog
/// lives at `catalog_url` (`pink:`/`sqlite:`/`postgres://`), its data on the
/// bucket. The store is validated before any lake is created, so a rejected
/// store leaves nothing behind. There is no default catalog location: a
/// catalog in a temp directory would be lost metadata.
pub fn save_store_s3(db: &PinkDb, catalog_url: &str, config: S3Config) -> Result<u64> {
    validate_store(db)?;
    let mut lake = match Lake::open_with_catalog_s3(catalog_url, config.clone()) {
        Ok(lake) => lake,
        Err(LakeError::NotALake { .. }) => Lake::create_with_catalog_s3(catalog_url, config)?,
        Err(e) => return Err(e),
    };
    lake.write_from_store(db)
}

/// The storage backend for a parsed location: local always, S3 only with the
/// `s3` feature (a clear error otherwise).
fn storage_for(location: Location) -> Result<Arc<dyn Storage>> {
    match location {
        Location::Local(path) => Ok(Arc::new(LocalStorage::new(&path))),
        Location::S3 { bucket, prefix } => s3_storage(&bucket, &prefix),
        Location::Pink(path) => Err(LakeError::BadLocation {
            location: format!("pink:{}", path.display()),
            message: "a catalog location is not a data location; use Lake::create_with_catalog \
                      / Lake::open_with_catalog"
                .to_string(),
        }),
        Location::Sqlite(path) => Err(LakeError::BadLocation {
            location: format!("sqlite:{}", path.display()),
            message: "a catalog location is not a data location; use Lake::create_with_catalog \
                      / Lake::open_with_catalog"
                .to_string(),
        }),
        Location::Postgres(url) => Err(LakeError::BadLocation {
            location: redact_location(&url),
            message: "a catalog location is not a data location; use Lake::create_with_catalog \
                      / Lake::open_with_catalog"
                .to_string(),
        }),
        Location::CatalogS3 { bucket, prefix } => Err(LakeError::BadLocation {
            location: if prefix.is_empty() {
                format!("pink+s3://{bucket}")
            } else {
                format!("pink+s3://{bucket}/{prefix}")
            },
            message: "a catalog location is not a data location; use Lake::create_with_catalog \
                      / Lake::open_with_catalog"
                .to_string(),
        }),
    }
}

/// Whether a parsed location names a SQL catalog rather than a data location.
fn is_catalog_location(location: &Location) -> bool {
    matches!(
        location,
        Location::Pink(_)
            | Location::Sqlite(_)
            | Location::Postgres(_)
            | Location::CatalogS3 { .. }
    )
}

/// Redact the password out of a Postgres catalog URL for display (every other
/// location is returned unchanged). The library's errors and the CLI use this
/// so a credential can never reach a terminal or a log.
pub fn redact_location(location: &str) -> String {
    catalog_store::url::redact_location(location)
}

/// The data half of a split lake: a local directory or an `s3://` URL, never
/// another catalog.
fn data_storage(location: &str) -> Result<Arc<dyn Storage>> {
    let parsed = Location::parse(location)?;
    if is_catalog_location(&parsed) {
        return Err(LakeError::BadLocation {
            location: redact_location(location),
            message: "the data location must be a local directory or an `s3://` URL, not a \
                      catalog location"
                .to_string(),
        });
    }
    storage_for(parsed)
}

/// The default data location for a catalog path: DuckLake's `DATA_PATH`
/// default, a sibling `<catalog path>.files/` directory.
fn default_catalog_data_location(path: &Path) -> String {
    format!("{}.files", path.display())
}

/// Resolve a lake location to its catalog URL for an **open**: a bare path
/// becomes `pink:<path>`; an explicit catalog is used as given. The data
/// location comes from the catalog (its recorded path, else the default).
fn open_catalog_location(location: &str, verb: &str) -> Result<String> {
    match Location::parse(location)? {
        Location::Pink(_) | Location::Sqlite(_) | Location::CatalogS3 { .. } => {
            Ok(location.to_string())
        }
        Location::Postgres(_) => {
            #[cfg(not(feature = "catalog-postgres"))]
            {
                Err(catalog_feature_missing(
                    location,
                    &Location::Postgres(location.to_string()),
                ))
            }
            #[cfg(feature = "catalog-postgres")]
            {
                let _ = verb;
                Ok(location.to_string())
            }
        }
        Location::Local(path) => Ok(format!("pink:{}", path.display())),
        Location::S3 { .. } => Err(LakeError::BadLocation {
            location: redact_location(location),
            message: format!(
                "`s3://…` is a data location, not a lake catalog; {verb} a catalog and put the \
                 data there, e.g. `.lake save pink:lake.pink data {location}` (or use a \
                 `postgres://` catalog for multi-machine lakes)"
            ),
        }),
    }
}

/// Split a lake location into `(catalog URL, default data location)`. A bare
/// path becomes `pink:<path>`; `pink:`/`sqlite:` default data to
/// `<path>.files/`; Postgres has no filesystem path and needs an explicit
/// data location; `s3://` alone is data, not a catalog.
fn default_catalog_split(location: &str, verb: &str) -> Result<(String, String)> {
    match Location::parse(location)? {
        Location::Pink(path) => Ok((location.to_string(), default_catalog_data_location(&path))),
        Location::Sqlite(path) => Ok((location.to_string(), default_catalog_data_location(&path))),
        Location::Postgres(_) => {
            #[cfg(not(feature = "catalog-postgres"))]
            {
                Err(catalog_feature_missing(
                    location,
                    &Location::Postgres(location.to_string()),
                ))
            }
            #[cfg(feature = "catalog-postgres")]
            {
                Err(catalog_needs_data(location, verb))
            }
        }
        Location::CatalogS3 { bucket, prefix } => {
            let data = if prefix.is_empty() {
                format!("s3://{bucket}/data")
            } else {
                format!("s3://{bucket}/{prefix}/data")
            };
            Ok((location.to_string(), data))
        }
        Location::S3 { .. } => Err(LakeError::BadLocation {
            location: redact_location(location),
            message: format!(
                "`s3://…` is a data location, not a lake catalog; {verb} a catalog and put \
                 the data there, e.g. `.lake save pink:lake.pink data {location}` (or use a \
                 `postgres://` catalog for multi-machine lakes)"
            ),
        }),
        Location::Local(path) => Ok((
            format!("pink:{}", path.display()),
            default_catalog_data_location(&path),
        )),
    }
}

/// The default data location for an explicit catalog URL whose rows carry no
/// data path (a DuckDB-created catalog opened without one cannot be guessed
/// for Postgres).
fn default_data_for_catalog(catalog_url: &str) -> Option<String> {
    match Location::parse(catalog_url).ok()? {
        Location::Pink(path) | Location::Sqlite(path) => Some(default_catalog_data_location(&path)),
        Location::CatalogS3 { bucket, prefix } => Some(if prefix.is_empty() {
            format!("s3://{bucket}/data")
        } else {
            format!("s3://{bucket}/{prefix}/data")
        }),
        _ => None,
    }
}

fn catalog_feature_missing(catalog_url: &str, location: &Location) -> LakeError {
    let feature = match location {
        Location::Sqlite(_) => "catalog-sqlite",
        Location::Postgres(_) => "catalog-postgres",
        Location::CatalogS3 { .. } => "catalog-s3",
        _ => "catalog",
    };
    LakeError::FeatureMissing {
        feature,
        message: format!(
            "the location {} is a SQL catalog; rebuild with the `{feature}` feature",
            redact_location(catalog_url)
        ),
    }
}

#[cfg(feature = "s3")]
fn s3_location(config: &S3Config) -> String {
    if config.prefix.is_empty() {
        format!("s3://{}", config.bucket)
    } else {
        format!("s3://{}/{}", config.bucket, config.prefix)
    }
}

fn keyed_data_storage(
    data: Arc<dyn Storage>,
    store: Arc<catalog_store::SqlMetadataStore>,
) -> Result<Arc<dyn Storage>> {
    if !crate::metadata::MetadataStore::is_encrypted(&*store)? {
        return Ok(data);
    }
    let resolver_store = Arc::clone(&store);
    let resolver = Arc::new(move |rel: &str| {
        resolver_store
            .file_key(rel)
            .ok()
            .flatten()
            .and_then(|bytes| bytes.try_into().ok())
    });
    Ok(Arc::new(crate::storage::FileKeyStorage::new(
        data, resolver,
    )))
}

/// The clear "a SQL catalog needs a data location" error used by the existing
/// URL entry points (`create_url`, `open_or_create_url` on a missing catalog).
#[cfg_attr(not(feature = "catalog-postgres"), allow(dead_code))]
fn catalog_needs_data(location: &str, verb: &str) -> LakeError {
    let shown = redact_location(location);
    LakeError::BadLocation {
        location: shown.clone(),
        message: format!(
            "a SQL catalog needs a data location to {verb}; use Lake::create_with_catalog \
             (CLI: `.lake save {shown} data <dir|s3://…>`) or pass one on open \
             (`.lake open {shown} data <location>`)"
        ),
    }
}

/// Refuse a data location that already belongs to a lake: an existing
/// `lake.json`, another catalog's `lake-data.json` owner marker (a split lake
/// that has no data files yet), or any `data/` object (wave 96a ownership).
fn ensure_data_location_free(data: &dyn Storage, data_location: &str) -> Result<()> {
    if data.exists(LAKE_FILE)? {
        return Err(LakeError::AlreadyALake { path: data.abs("") });
    }
    if !data.list_recursive("data")?.is_empty() {
        return Err(LakeError::BadLocation {
            location: data_location.to_string(),
            message: "the data location already holds lake data files; a new catalog needs an \
                      empty data location (or a new prefix)"
                .to_string(),
        });
    }
    if data.exists(LAKE_DATA_FILE)? {
        return Err(LakeError::BadLocation {
            location: data_location.to_string(),
            message: format!(
                "the data location is owned by another catalog ({LAKE_DATA_FILE} is present); \
                 a new catalog needs an empty data location (or a new prefix)"
            ),
        });
    }
    Ok(())
}

#[cfg(feature = "s3")]
fn s3_storage(bucket: &str, prefix: &str) -> Result<Arc<dyn Storage>> {
    Ok(Arc::new(s3::S3Storage::from_url(bucket, prefix)?))
}

#[cfg(not(feature = "s3"))]
fn s3_storage(bucket: &str, prefix: &str) -> Result<Arc<dyn Storage>> {
    let location = if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    };
    Err(LakeError::FeatureMissing {
        feature: "s3",
        message: format!(
            "the location {location} needs object-storage support; rebuild with the `s3` \
             feature (for the CLI: `cargo build -p pinkdb-cli --features pinklake,s3`)"
        ),
    })
}

/// The largest encoded `data/<table>` directory segment a writer accepts,
/// so `<segment>/<uuid>.pink.<uuid>.tmp` (the data file plus a unique temp
/// sibling) fits the common 255-byte name limit. Also enforced by
/// [`Lake::write_from_store`].
const MAX_ENCODED_SEGMENT: usize = 200;

/// Check that `db` can be represented in a lake catalog: database names
/// must not contain `.` (keys are flat `db.table`), and every table's
/// percent-encoded `data/<key>` segment must fit the
/// `MAX_ENCODED_SEGMENT` (200-byte) directory-name limit.
///
/// [`save_store`] and the CLI's `.lake save` run this *before* touching the
/// filesystem, so a store that cannot be saved never leaves a fresh empty
/// lake behind. [`Lake::write_from_store`] runs it too (before writing any
/// data file).
pub fn validate_store(db: &PinkDb) -> Result<()> {
    for dbname in db.list_databases() {
        if dbname.contains('.') {
            return Err(LakeError::NotRepresentable {
                message: format!(
                    "database name \"{dbname}\" contains \".\" and cannot be stored in a \
                     flat lake catalog (keys are flat `db.table`); rename the database or \
                     save the store to a .pink file instead"
                ),
            });
        }
        for name in db.list_tables_in(&dbname) {
            let key = format!("{dbname}.{name}");
            let segment = encode_key(&key);
            if segment.len() > MAX_ENCODED_SEGMENT {
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "table key \"{key}\" needs a {}-byte `data/` directory name, over the \
                         {MAX_ENCODED_SEGMENT}-byte limit (filesystems allow ~255 bytes per \
                         name); shorten the table name or save the store to a .pink file \
                         instead",
                        segment.len()
                    ),
                });
            }
        }
    }
    Ok(())
}

/// A test-only hook for the crash-injection test: arm one failure right after
/// a commit's snapshot insert (inside the publish scope), so the test can
/// prove the transaction rolls back the delta, snapshot, catalog and highest
/// marker together. The arm names the lake id so parallel tests' publications
/// are untouched. Compiled only with `catalog-sqlite`.
#[doc(hidden)]
pub fn fail_next_publish_after_snapshot(lake_id: &str) {
    *FAIL_PUBLISH_AFTER_SNAPSHOT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(lake_id.to_string());
}

static FAIL_PUBLISH_AFTER_SNAPSHOT: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// The armed crash-injection point (a no-op unless a test armed it).
pub(crate) fn maybe_fail_publish_after_snapshot(lake_id: &str) -> Result<()> {
    {
        let mut armed = FAIL_PUBLISH_AFTER_SNAPSHOT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if armed.as_deref() == Some(lake_id) {
            *armed = None;
            return Err(LakeError::Internal {
                message: format!(
                    "injected catalog publish failure after the snapshot insert (lake {lake_id})"
                ),
            });
        }
    }
    Ok(())
}

/// Rows SQLite reported as changed by the most recent successful delta commit
/// in this process. Wave 103 tests use it to prove a late commit touches the
/// same number of rows as an early one; it is not part of the public API.
#[doc(hidden)]
#[cfg(feature = "catalog-sqlite")]
pub fn sqlite_last_commit_rows_touched() -> u64 {
    crate::metadata::store::last_commit_rows_touched()
}

/// Rows the catalog reported as changed by the most recent successful delta
/// commit in this process (used by the pink backend's scaling tests).
#[doc(hidden)]
pub fn last_commit_rows_touched() -> u64 {
    crate::metadata::store::last_commit_rows_touched()
}

/// Every per-file encryption key recorded in a SQL catalog (diagnostics and
/// tests: prove each file carries its own 256-bit key). `key` opens a `pink:`
/// catalog database that is itself engine-encrypted. Not part of the
/// supported API.
#[doc(hidden)]
pub fn catalog_encryption_keys(
    catalog_url: &str,
    key: Option<EncryptionKey>,
) -> Result<Vec<Vec<u8>>> {
    let location = Location::parse(catalog_url)?;
    let store =
        catalog_store::open_metadata_store_keyed(catalog_url, &location, false, key.as_ref())?
            .ok_or_else(|| LakeError::NotALake {
                path: PathBuf::from(redact_location(catalog_url)),
            })?;
    crate::metadata::MetadataStore::file_keys(&store)
}

/// Execute one SQL statement on a catalog database (tests only).
#[doc(hidden)]
pub fn catalog_execute(catalog_url: &str, sql: &str) -> Result<()> {
    let location = Location::parse(catalog_url)?;
    let store = catalog_store::open_metadata_store_keyed(catalog_url, &location, false, None)?
        .ok_or_else(|| LakeError::NotALake {
            path: PathBuf::from(redact_location(catalog_url)),
        })?;
    store.execute_sql(sql)
}

/// One catalog version's typed `Catalog` as JSON (diagnostics/tests): the
/// same model `MetadataStore::load(version)` returns, never a rendered object.
#[doc(hidden)]
pub fn catalog_snapshot_json(catalog_url: &str, version: u64) -> Result<Option<serde_json::Value>> {
    let location = Location::parse(catalog_url)?;
    let store = catalog_store::open_metadata_store_keyed(catalog_url, &location, false, None)?
        .ok_or_else(|| LakeError::NotALake {
            path: PathBuf::from(redact_location(catalog_url)),
        })?;
    match crate::metadata::MetadataStore::load(&store, Some(version))? {
        Some(catalog) => {
            serde_json::to_value(&catalog)
                .map(Some)
                .map_err(|e| LakeError::Internal {
                    message: format!("rendering catalog version {version}: {e}"),
                })
        }
        None => Ok(None),
    }
}

/// One table's typed catalog entry rendered as the engine model's JSON shape
/// (diagnostics/tests). Not part of the supported API.
#[doc(hidden)]
pub fn catalog_table_json(catalog_url: &str, key: &str) -> Result<Option<serde_json::Value>> {
    let location = Location::parse(catalog_url)?;
    let store = catalog_store::open_metadata_store_keyed(catalog_url, &location, false, None)?
        .ok_or_else(|| LakeError::NotALake {
            path: PathBuf::from(redact_location(catalog_url)),
        })?;
    match store.table(key, None)? {
        Some(entry) => serde_json::to_value(&entry)
            .map(Some)
            .map_err(|e| LakeError::Internal {
                message: format!("rendering table {key}: {e}"),
            }),
        None => Ok(None),
    }
}

/// Force one table's catalog `format` id (tests inject an unknown or another
/// provider: the catalog rows are the only source of truth now). Not part of
/// the supported API.
#[doc(hidden)]
pub fn catalog_set_table_format(catalog_url: &str, key: &str, format: &str) -> Result<()> {
    let location = Location::parse(catalog_url)?;
    let store = catalog_store::open_metadata_store_keyed(catalog_url, &location, false, None)?
        .ok_or_else(|| LakeError::NotALake {
            path: PathBuf::from(redact_location(catalog_url)),
        })?;
    store.set_table_format(key, format)
}

/// The manifest for the latest version (`None`) or one snapshot, with the
/// catalog location it was read from (for error attribution; `catalog` is the
/// lake's [`Lake::catalog_label`]). Free function so an attachment — which
/// holds only the storage, the data root and the label, not the [`Lake`]
/// handle — can read arbitrary versions for time travel.
pub(crate) fn manifest_for(
    store: &dyn crate::metadata::MetadataStore,
    catalog: &Path,
    version: Option<u64>,
) -> Result<(Catalog, PathBuf)> {
    let Some(manifest) = store.load(version)? else {
        return Err(match version {
            Some(version) => LakeError::VersionNotFound {
                path: catalog.to_path_buf(),
                version,
            },
            None => LakeError::NotALake {
                path: catalog.to_path_buf(),
            },
        });
    };
    // A catalog row naming the data format removed in wave 104 is refused
    // for every operation, not treated as a missing provider: no build will
    // ever read it again, and the lake must be re-saved by an older build.
    if manifest
        .tables
        .values()
        .any(|entry| crate::format::is_removed(entry.format().id()))
    {
        return Err(crate::format::removed_format_error());
    }
    let label = match version {
        None => catalog.to_path_buf(),
        Some(v) => PathBuf::from(format!("{} (snapshot {v})", catalog.display())),
    };
    Ok((manifest, label))
}

/// The published snapshot versions at or below `up_to`, ascending (see
/// [`Lake::versions`] for exactly what counts as published). Free function so
/// attachments can list versions without a [`Lake`] handle.
pub(crate) fn published_versions(
    store: &dyn crate::metadata::MetadataStore,
    up_to: u64,
) -> Result<Vec<u64>> {
    Ok(store
        .snapshot_versions()?
        .into_iter()
        .filter(|v| *v >= 1 && *v <= up_to)
        .collect())
}

pub(crate) fn is_lake_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A fresh 128-bit lake id from OS randomness (hex). Unlike [`fresh_id`]
/// (time/pid/stack derived, adequate for unique file names but predictable),
/// this is the identity an attacker must not be able to re-create or guess.
pub(crate) fn fresh_lake_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).map_err(|e| LakeError::Io {
        context: "obtaining OS randomness for the lake identity".to_string(),
        source: std::io::Error::other(e.to_string()),
    })?;
    let mut out = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    Ok(out)
}

fn data_ownership_json(lake_id: &str) -> Result<Vec<u8>> {
    #[derive(serde::Serialize)]
    struct DataOwnership<'a> {
        lake_id: &'a str,
    }
    let mut bytes =
        serde_json::to_vec_pretty(&DataOwnership { lake_id }).map_err(|e| LakeError::Internal {
            message: format!("serializing the data-location owner marker: {e}"),
        })?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Parse a `lake-data.json` owner marker. The path is for error attribution.
pub(crate) fn parse_data_owner(bytes: &[u8], path: &Path) -> Result<String> {
    #[derive(serde::Deserialize)]
    struct DataOwnership {
        lake_id: String,
    }
    let owner: DataOwnership = serde_json::from_slice(bytes).map_err(|e| LakeError::Corrupt {
        path: path.to_path_buf(),
        message: format!("{LAKE_DATA_FILE} does not parse: {e}"),
    })?;
    if !is_lake_id(&owner.lake_id) {
        return Err(LakeError::Corrupt {
            path: path.to_path_buf(),
            message: format!(
                "{LAKE_DATA_FILE} carries an invalid lake_id (expected 32 lowercase hex \
                 characters)"
            ),
        });
    }
    Ok(owner.lake_id)
}

/// Flat catalog key → `(database, table)`, splitting on the FIRST dot (a
/// table name may contain dots). `path` is the manifest the key came from.
fn split_key<'a>(key: &'a str, path: &Path) -> Result<(&'a str, &'a str)> {
    match key.split_once('.') {
        Some(parts) => Ok(parts),
        None => Err(LakeError::Corrupt {
            path: path.to_path_buf(),
            message: format!("table key \"{key}\" is not \"db.table\""),
        }),
    }
}

/// A catalog `files` entry must be a plain relative path under `data/`.
fn validate_rel_path(rel: &str) -> Result<()> {
    let path = Path::new(rel);
    let plain = path.components().all(|c| matches!(c, Component::Normal(_)));
    if path.is_absolute() || !plain || !rel.starts_with("data/") {
        return Err(LakeError::Corrupt {
            path: PathBuf::from(rel),
            message: "data file path must be relative and under data/".to_string(),
        });
    }
    Ok(())
}

/// Percent-encode a catalog key into one filesystem-safe path segment.
/// Everything outside `[A-Za-z0-9_-]` (and dots after the first byte) is
/// percent-encoded, so keys can never contain a path separator, a leading
/// dot, or `.`/`..` as a whole segment.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for (i, b) in key.bytes().enumerate() {
        let safe = b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || (b == b'.' && i > 0);
        if safe {
            out.push(b as char);
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
    }
    out
}

/// Percent-decode one `encode_key` segment back to a catalog key (the
/// inverse, for reporting only: `.lake files --all` names an unreferenced
/// object's table). `None` on a malformed escape or non-UTF-8 bytes — a
/// tampered name is reported as unknown, never guessed.
pub(crate) fn decode_key(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = char::from(*bytes.get(i + 1)?).to_digit(16)?;
            let lo = char::from(*bytes.get(i + 2)?).to_digit(16)?;
            out.push((hi * 16 + lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// A fresh data-file id: 32 hex chars in UUID shape from a splitmix64 mixer
/// over clock + pid + stack address + a process counter. Uniqueness (not
/// cryptographic randomness) is the requirement. Also used by
/// [`LocalStorage`] for unique temp-file names.
/// The bounded in-flight limit for parallel lake reads: the shared
/// `PINKDB_HTTPFS_PARALLELISM` (default 8, clamped to 16, 0/1 = serial) when
/// the S3/httpfs transport is built; 8 otherwise (local lakes still decode
/// files concurrently). One knob for every read path.
pub(crate) fn parallel_limit() -> usize {
    #[cfg(feature = "s3")]
    {
        pinkdb_httpfs::parallelism_from_env()
    }
    #[cfg(not(feature = "s3"))]
    {
        8
    }
}

/// Apply `f` to every item with at most `limit` concurrent workers, returning
/// results in input order. `limit <= 1` (or one item) runs inline. Results are
/// therefore deterministic even though the work runs concurrently; callers
/// pick the first error in order.
pub(crate) fn map_bounded<T, R, F>(items: &[T], limit: usize, f: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(&T) -> R + Sync,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    let workers = limit.clamp(1, items.len().max(1));
    if workers == 1 || items.len() <= 1 {
        return items.iter().map(&f).collect();
    }
    let slots: Vec<Option<R>> = (0..items.len()).map(|_| None).collect();
    let slots = std::sync::Mutex::new(slots);
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    let result = f(item);
                    slots.lock().unwrap()[index] = Some(result);
                }
            });
        }
    });
    slots
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|slot| slot.expect("every bounded-map item is claimed"))
        .collect()
}

pub(crate) fn fresh_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let stack = &n as *const u64 as u64;
    let seed = now.as_nanos() as u64
        ^ (now.subsec_nanos() as u64).rotate_left(32)
        ^ ((std::process::id() as u64) << 32)
        ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ stack;
    let a = splitmix64(seed);
    let b = splitmix64(seed ^ a);
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        a >> 32,
        (a >> 16) & 0xFFFF,
        a & 0xFFF,
        ((b >> 48) & 0x3FFF) | 0x8000,
        b & 0xFFFF_FFFF_FFFF
    )
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The lake-wide schema version for a new manifest: the previous version's
/// counter, plus one when any table was created, dropped, or had its schema
/// record (columns / defaults / renames) changed. DuckLake bumps
/// `snapshot.schema_version` once per schema-changing statement; pinkdb's
/// version granularity is the published version, so it bumps once.
pub(crate) fn next_schema_version(
    previous: Option<&Catalog>,
    tables: &BTreeMap<String, TableEntry>,
) -> u64 {
    let Some(previous) = previous else {
        return u64::from(!tables.is_empty());
    };
    let schema_changed = tables.len() != previous.tables.len()
        || tables.iter().any(|(key, entry)| {
            previous.tables.get(key).is_none_or(|before| {
                before.columns != entry.columns || before.renames != entry.renames
            })
        });
    previous.schema_version + u64::from(schema_changed)
}

/// The single database + single table a data file must hold.
pub(crate) fn only_table<'a>(
    mini: &'a PinkDb,
    rel: &str,
    root: &Path,
) -> Result<(String, &'a DataTable)> {
    let dbs = mini.list_databases();
    if dbs.len() != 1 {
        return Err(LakeError::Corrupt {
            path: root.join(rel),
            message: format!(
                "data file must hold exactly one database, has {}",
                dbs.len()
            ),
        });
    }
    let dbname = dbs.into_iter().next().expect("one database");
    let tables = mini.list_tables_in(&dbname);
    if tables.len() != 1 {
        return Err(LakeError::Corrupt {
            path: root.join(rel),
            message: format!(
                "data file must hold exactly one table, has {}",
                tables.len()
            ),
        });
    }
    let table = mini
        .table_in(&dbname, &tables[0])
        .map_err(|e| LakeError::Engine {
            context: format!("reading data file \"{rel}\""),
            source: e,
        })?;
    Ok((dbname, table))
}

/// Append every chunk of `src` into `dst`, sharing each `Arc<Vector>` with
/// the source (the decoded mini store is dropped right after) — bit-exact,
/// including NaN payloads and `-0.0` — with `append_chunk`'s chunking rules
/// (a ragged tail is merged, never left ragged). `context` names the source
/// for errors.
pub(crate) fn copy_chunks(src: &DataTable, dst: &mut DataTable, context: &str) -> Result<()> {
    for i in 0..src.num_chunks() {
        let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {i} of {context} disappeared while copying"),
        })?;
        dst.append_shared_chunk(&chunk.columns)
            .map_err(|e| LakeError::Engine {
                context: format!("appending chunk {i} of {context}"),
                source: e,
            })?;
    }
    Ok(())
}

/// Materialize one data file's rows with its positional deletes applied
/// (merge-on-read): `positions` are 0-based row positions within the decoded
/// file, ascending. Rows are deleted chunk by chunk (the engine's
/// `delete_from_chunk`), so values, chunking of untouched chunks, and NaN
/// payloads are untouched; a chunk left empty is dropped. The returned table
/// shares storage with `table` except in the chunks that lost rows.
pub(crate) fn apply_file_deletes(
    table: &DataTable,
    positions: &[u64],
    key: &str,
    rel: &str,
) -> Result<DataTable> {
    if positions.is_empty() {
        return Ok(table.clone());
    }
    let total = table.num_rows() as u64;
    if let Some(&bad) = positions.iter().find(|&&p| p >= total) {
        return Err(LakeError::Corrupt {
            path: PathBuf::from(rel),
            message: format!(
                "table \"{key}\": delete position {bad} is outside \"{rel}\" ({total} rows)"
            ),
        });
    }
    let mut by_chunk: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut start = 0usize;
    for c in 0..table.num_chunks() {
        let len = table.columns[0].chunks[c].len();
        for &p in positions {
            let p = p as usize;
            if p >= start && p < start + len {
                by_chunk.entry(c).or_default().push(p - start);
            }
        }
        start += len;
    }
    let mut out = table.clone();
    // Descending chunk order: `delete_from_chunk` may drop an emptied chunk,
    // which would shift the indices of every later one.
    for (chunk, drop) in by_chunk.into_iter().rev() {
        out.delete_from_chunk(chunk, &drop)
            .map_err(|e| LakeError::Engine {
                context: format!("applying deletes to \"{rel}\" (table \"{key}\")"),
                source: e,
            })?;
    }
    Ok(out)
}

/// A one-table [`PinkDb`] holding `src` (the `.pink` data-file encoder). The
/// mini store holds exactly the source's database and table (the default
/// `main` of a fresh store is dropped when the table lives elsewhere); its
/// chunks are shared with `src` (no deep copy) and appended with
/// `append_chunk`'s chunking rules, exactly what the loader's `copy_chunks`
/// reverses. The engine's `.pink` encoder is then byte-deterministic for a
/// given dense table.
fn single_table_store(dbname: &str, table: &str, src: &DataTable) -> Result<PinkDb> {
    let mut mini = empty_structure(dbname, table, src.schema(), "saving")?;
    let dst = mini
        .table_mut_in(dbname, table)
        .map_err(|e| LakeError::Engine {
            context: format!("preparing table \"{dbname}.{table}\" while saving"),
            source: e,
        })?;
    copy_chunks(src, dst, &format!("{dbname}.{table}"))?;
    Ok(mini)
}

/// The database/table skeleton the one-table encoders fill: `dbname.table`
/// created with `schema`, the default `main` of a fresh store dropped when
/// the table lives elsewhere. `what` reads "saving" or "loading" in errors.
fn empty_structure(
    dbname: &str,
    table: &str,
    schema: Vec<(String, DataType)>,
    what: &str,
) -> Result<PinkDb> {
    let mut mini = PinkDb::new();
    if !mini.database_exists(dbname) {
        mini.create_database(dbname)
            .map_err(|e| LakeError::Engine {
                context: format!("creating database \"{dbname}\" while {what}"),
                source: e,
            })?;
        mini.drop_database("main").map_err(|e| LakeError::Engine {
            context: format!("dropping the default main database while {what} \"{dbname}\""),
            source: e,
        })?;
    }
    mini.create_table_in(dbname, table, schema)
        .map_err(|e| LakeError::Engine {
            context: format!("creating table \"{dbname}.{table}\" while {what}"),
            source: e,
        })?;
    Ok(mini)
}

/// `&DataTable` with a lake error instead of an engine error.
fn table_in<'a>(db: &'a PinkDb, dbname: &str, table: &str) -> Result<&'a DataTable> {
    db.table_in(dbname, table).map_err(|e| LakeError::Engine {
        context: format!("reading table \"{dbname}.{table}\""),
        source: e,
    })
}

/// A path the engine's `&str` file APIs can use.
fn path_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| LakeError::Internal {
        message: format!("path is not valid UTF-8: {}", path.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_key_splits_on_the_first_dot() {
        let catalog = PathBuf::from("pink:/tmp/lake.pink");
        assert_eq!(split_key("main.t", &catalog).unwrap(), ("main", "t"));
        assert_eq!(
            split_key("analytics.a.b", &catalog).unwrap(),
            ("analytics", "a.b")
        );
        assert_eq!(split_key("main.", &catalog).unwrap(), ("main", ""));
        let err = split_key("nodot", &catalog).unwrap_err().to_string();
        assert!(err.contains("pink:/tmp/lake.pink"), "{err}");
        let snapshot = PathBuf::from("sqlite:/tmp/cat.db (snapshot 2)");
        let err = split_key("nodot", &snapshot).unwrap_err().to_string();
        assert!(err.contains("sqlite:/tmp/cat.db (snapshot 2)"), "{err}");
    }

    #[test]
    fn encode_key_never_escapes_a_directory() {
        assert_eq!(encode_key("main.t"), "main.t");
        assert_eq!(encode_key("analytics.u_1-x"), "analytics.u_1-x");
        // Leading dots cannot produce "." or "..".
        assert_eq!(encode_key("."), "%2E");
        assert_eq!(encode_key(".."), "%2E.");
        // Separators and spaces are encoded.
        assert_eq!(encode_key("a/b"), "a%2Fb");
        assert_eq!(encode_key("a b"), "a%20b");
        assert_eq!(encode_key("a\\b"), "a%5Cb");
        // Non-ASCII is encoded byte-wise.
        assert_eq!(encode_key("héllo"), "h%C3%A9llo");
        for key in [".", "..", "a/../../b", "weird name", "%2e"] {
            let encoded = encode_key(key);
            assert!(
                Path::new(&encoded).components().count() == 1,
                "{key:?} → {encoded:?} escapes its segment"
            );
            assert_ne!(encoded, ".");
            assert_ne!(encoded, "..");
        }
    }

    #[test]
    fn decode_key_inverts_encode_key() {
        for key in [
            "main.t",
            "analytics.u_1-x",
            "a/b",
            "a b",
            "héllo",
            "a.b.c",
            "%2e",
        ] {
            let encoded = encode_key(key);
            assert_eq!(decode_key(&encoded).as_deref(), Some(key), "{key:?}");
        }
        // Malformed escapes and non-UTF-8 are a clean None.
        assert_eq!(decode_key("%"), None);
        assert_eq!(decode_key("%2"), None);
        assert_eq!(decode_key("%zz"), None);
        assert_eq!(decode_key("%FF"), None);
    }

    #[test]
    fn fresh_ids_are_unique_and_uuid_shaped() {
        let a = fresh_id();
        let b = fresh_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        let parts: Vec<&str> = a.split('-').collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12]
        );
        assert!(a.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        assert!(parts[2].starts_with('4'));
        assert!(matches!(parts[3].as_bytes()[0], b'8'..=b'b'));
    }

    #[test]
    fn relative_path_validation_is_fail_closed() {
        assert!(validate_rel_path("data/main.t/x.pink").is_ok());
        assert!(validate_rel_path("data/x.pink").is_ok());
        assert!(validate_rel_path("../evil.pink").is_err());
        assert!(validate_rel_path("data/../../evil.pink").is_err());
        assert!(validate_rel_path("/etc/passwd").is_err());
        assert!(validate_rel_path("data").is_err());
        assert!(validate_rel_path("./data/x.pink").is_err());
    }
}
