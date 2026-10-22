//! pink-lake — the minimal lake layer for pinkdb: decoupled metadata and
//! data storage on local disk, or in an S3 bucket/prefix with the
//! non-default `s3` feature (AWS S3, Cloudflare R2, any S3-compatible
//! endpoint).
//!
//! # What a lake is
//!
//! A lake is a directory (or, with the `s3` feature, an `s3://bucket/prefix`
//! key prefix with exactly the same layout):
//!
//! ```text
//! <lake>/
//!   lake.json                   random 128-bit lake identity (never replaced)
//!   highest.json                authenticated highest published version
//!   catalog.json                current catalog (authoritative for "latest")
//!   snapshots/<version>.json    immutable manifests, one per published version
//!   commits/<version>.json      immutable per-commit delta manifests (audit)
//!   data/<table>/<uuid>.vortex  immutable per-table data files (v4, Vortex)
//!   data/<table>/<uuid>.pink    `.pink` data files (v1/v4 lean builds)
//!   data/<table>/<uuid>.pinkdel positional delete files (v4; see delete_file)
//!   replication/state.json      replication jobs + per-table cursor state (wave 89)
//! ```
//!
//! `catalog.json` and every snapshot manifest are JSON of the same shape:
//!
//! ```json
//! {
//!   "format_version": 6,
//!   "lake_id": "0123456789abcdef0123456789abcdef",
//!   "updated": "2026-10-14T12:34:56.789Z",
//!   "version": 2,
//!   "tables": {
//!     "main.t": {
//!       "files": [
//!         { "path": "data/main.t/3f2a….vortex", "rows": 5 },
//!         { "path": "data/main.t/9c01….vortex", "rows": 2 }
//!       ],
//!       "deletes": [
//!         { "path": "data/main.t/77aa….pinkdel", "file": "data/main.t/3f2a….vortex",
//!           "count": 1 }
//!       ],
//!       "rows": 6,
//!       "format": "vortex",
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
//! `sum(files.rows) - sum(deletes.count)`. A v4 entry's `format` is
//! `"vortex"` in a `vortexlake` build or `"pink"` (the engine's
//! single-table `.pink` bytes) in a lean one — or any other id a registered
//! provider ([`crate::format`]) supplies; every v4 data file carries a
//! CRC-protected statistics/zone-map block in its `files` object — for
//! `.pink` the exact facts its encode pass collected, for Vortex the same
//! facts published from the writer and cross-checked against the file's own
//! footer at attach — so aggregate fast paths and **file pruning** work
//! without decoding anything.
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
//! Versions 1–3 remain readable and are never written any more: a v1 entry
//! (the wave-34 lean format) is one `.pink` file as a plain string path plus
//! one entry-level `"stats"` block; versions 2 and 3 are one `.vortex` file
//! as a plain string path (`"format": "vortex"`), v3 with larger row blocks.
//! A v1–v3 entry that omits `format` (v2/v3) or declares the wrong one, a
//! manifest with any other `format_version`, a v4 file object without a row
//! count, or a v4 table whose row count does not add up is a clear
//! [`LakeError::Unsupported`]/[`LakeError::Corrupt`] error, never a guess.
//! Decoding a corrupt Vortex data file is the same clear [`LakeError::Corrupt`]
//! error: the whole decoder boundary is guarded, so a panic inside Vortex's
//! malformed-layout paths is caught and reported (the default panic hook may
//! still print one line to stderr first; the crate does not swap the global
//! hook).
//!
//! **With the `vortexlake` feature (not on by default; the full CLI builds
//! enable it) Vortex is the format for new writes**: every new data file is a
//! plain Vortex file holding one struct array — one field per column, row
//! order preserved, `-0.0` and NaN payloads bit-exact — plus a
//! `pinkdb.table` metadata segment carrying the table's `db`/`table` identity
//! and its pinkdb column schema (the `vortex_file` module docs have the
//! type mapping; DATE/TIMESTAMP are raw `i32`/`i64` storage because the native
//! `vortex.timestamp` extension cannot represent the full engine range).
//! **Without the feature** — the `pinklake` builds — new writes are
//! engine `.pink` single-table persistence files
//! ([`PinkDb::to_pink_bytes_with_stats`]) **plus a CRC-protected statistics
//! block per file** (the exact per-column facts the same encode pass
//! collected; see the `stats` module docs). A v4–v6 lake whose entries
//! include Vortex tables still **opens** in a lean build: those tables are
//! listed as unavailable (wave 88, rule 5) and every read or targeted write
//! of one is a clear [`LakeError::FeatureMissing`] naming the table — never a
//! corrupt-file error and never a wrong-format guess; [`Lake::load`] skips
//! them. A v2/v3 all-Vortex lake keeps the whole-lake refusal, and a v4
//! `.pink` lake stays readable in every build (its data files open
//! standalone with [`PinkDb::open`]).
//!
//! # Data-file format providers (wave 87)
//!
//! An entry's `format` string is a **format id**, resolved through a
//! process-global registry ([`crate::format`]). The crate registers the built-ins
//! lazily: `"pink"` in every build and `"vortex"` with the `vortexlake`
//! feature. An extension registers a [`format::LakeFormat`] — writer, reader,
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
//! shared exactness gate in `stats` applies to every provider). Attach reads
//! that block, or the format's own footer when it advertises one via
//! [`format::LakeFormat::footer_stats`] (Vortex), and either way the decode
//! goes through [`format::LakeFormat::read`]. Format ids are recorded in the
//! catalog: existing manifests stay byte-identical (`"pink"`/`"vortex"`, and
//! a v1 entry without `format` still means `"pink"`), while an id with no
//! registered provider is a clean [`LakeError::UnknownFormat`] naming the id
//! when that table is read or written — never a panic and never a silent
//! fallback (wave 88 makes that a per-table refusal: the rest of the lake
//! still opens). [`Lake::set_write_format`] selects a registered format for a
//! handle's subsequent writes; the default is unchanged (Vortex with
//! `vortexlake`, `.pink` otherwise).
//!
//! # Per-table formats (wave 88)
//!
//! One lake may hold tables in different data-file formats. A handle chooses
//! one per table with [`Lake::set_table_format`] (validated against the
//! registry exactly like [`Lake::set_write_format`]); the choice is in memory
//! and applies to the handle's subsequent **full saves** and to tables it
//! creates.
//!
//! **Pinning (the hybrid migration rule).** A format chosen explicitly — a
//! per-table choice, an explicit [`Lake::set_write_format`], or
//! [`Lake::rewrite_table_format`] — is recorded in the table's catalog entry
//! as `"format_pinned": true` (omitted when false, so catalogs written by
//! default stay byte-identical, and older builds ignore the field). An entry
//! written under the build default is **not** pinned and keeps the wave-45
//! behavior: a full save re-encodes it into the saving build's write format,
//! so a pinklake-written lake migrates to Vortex on its next save in a
//! vortexlake build. An entry whose format id is not a built-in default
//! (anything but `pink`/`vortex`, e.g. `parquet`) is always treated as
//! pinned.
//!
//! A full save resolves each table's format in this order:
//!
//! 1. the handle's explicit per-table choice ([`Lake::set_table_format`]) —
//!    that format, pinned;
//! 2. else the previous entry if it is pinned (or its format id is not a
//!    built-in default) — its format, pinned;
//! 3. else the handle's write format — pinned exactly when
//!    [`Lake::set_write_format`] was called explicitly, otherwise the build
//!    default and unpinned.
//!
//! Appends always use the entry's format and keep its pin flag: an existing
//! entry has exactly one format, so a commit/flush writes its new files in
//! that format (a per-table choice never makes an append mix formats inside
//! one entry). A table created by a commit uses the per-table choice (pinned)
//! else the handle's write format (pinned when explicitly chosen).
//! Converting a table is explicit maintenance — [`Lake::rewrite_table_format`]
//! (the `.lake rewrite <dir> <table> format <id>` command) rewrites every
//! file of the table into the requested format in one atomically published
//! version, materializing any positional deletes, and pins the result; old
//! files are retired by the ordinary expire/cleanup path. Ordinary
//! compaction/rewrite keeps the entry's format and pin flag.
//!
//! **Unavailable formats are refused per table, not per lake.** In a
//! format_version 4–6 catalog, an entry whose format id has no registered
//! provider ([`LakeError::UnknownFormat`], or [`LakeError::FeatureMissing`]
//! for `vortex` without the `vortexlake` feature) no longer refuses the whole
//! lake: `open`/`attach` succeed, [`Lake::unavailable_tables`] lists each such
//! table with its id and error, [`Lake::load`] materializes the readable
//! tables and skips the rest, and `.lake attach` lists the table while any
//! read of it fails with that error naming the table and the id. A full save
//! carries such an entry forward verbatim when the store has no table under
//! its key and refuses when it does; a commit/append, rewrite or compaction
//! targeting it fails, and lake-wide maintenance skips it. v2/v3 catalogs
//! (all-Vortex by definition) keep the whole-lake [`LakeError::FeatureMissing`]
//! refusal, and corrupt format/version combinations still refuse the lake.
//!
//! `.lake open` (every build) and `.lake attach` (every build) are the two
//! ways in: `open` eagerly loads a version into the store, `attach` reads
//! only the catalog, the manifest, and the statistics layer — a Vortex data
//! file's footer, metadata segment and stored statistics, or the per-file
//! `.pink` statistics blocks (no data file is opened at attach) — and lazily
//! materializes the one table a query actually scans, with exact
//! `COUNT`/`MIN`/`MAX` answered straight from those statistics. Attached
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
//! manifest is published as `snapshots/<version>.json`, then `catalog.json`
//! is replaced with the same bytes. [`Lake::commit`] (and the higher-level
//! [`Lake::commit_from_tables`]) publishes an **incremental** version on top
//! of one: appended files are added to the table's file list, positional
//! deletes become delete files, and **no file a previous version references
//! is rewritten, replaced, or reused for new data** — the `incremental`
//! module documents the delta format. Each commit also writes
//! `commits/<version>.json`, an append-only delta manifest (audit, never a
//! load-time input). A version number is `previous + 1`, computed with
//! `checked_add` and capped at [`MAX_VERSION`]; a lake whose next version
//! would exceed the cap is a clear [`LakeError::Overflow`], never a wrap.
//! Every manifest's `version` must be in `1..=MAX_VERSION` (a tampered
//! `version: 0` or `version: u64::MAX` is [`LakeError::Corrupt`]). Snapshot
//! manifests are *hard-linked* into place, never renamed over: a snapshot
//! file that already exists is never replaced ([`LakeError::Conflict`]), so
//! restoring an older `catalog.json` and re-saving cannot clobber an
//! already-published `snapshots/<v>.json`. Old snapshots and old data files
//! are never touched, so any published version stays loadable.
//! [`Lake::load`] takes the latest version (the catalog) or one specific
//! snapshot.
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
//! it. [`Lake::expire_snapshots`] removes old `snapshots/<v>.json` +
//! `commits/<v>.json` (never the current version; time travel to an expired
//! version is a clean error). [`Lake::cleanup_old_files`] /
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
//! destination lake carries jobs (`replication/state.json`) that copy the
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
//! Publication order is data and delete files → commit manifest
//! (`commits/<version>.json`) → snapshot manifest (`snapshots/<version>.json`)
//! → `catalog.json`. Each write goes to a **unique** temp sibling
//! (`<name>.<uuid>.tmp`, created with `create_new(true)` so a planted symlink
//! or an older writer's temp file is never followed or truncated), is
//! fsynced, and is then renamed (catalog, data files) or hard-linked
//! (snapshot manifests) into place; the parent directory is fsynced
//! afterwards, best-effort. A crash anywhere can only leave *unreferenced*
//! leftovers: the previous catalog still loads, and stray data files, stray
//! `.tmp` files, and snapshots above the catalog's version are ignored —
//! [`Lake::versions`] lists only manifests that parse and declare their own
//! canonical version. A stray manifest at exactly the *next* version cannot
//! be told apart from a published one, so a save or commit that lands on it
//! refuses with [`LakeError::Conflict`] instead of replacing it; delete the
//! unpublished stray (the catalog still points at the previous version) to
//! continue. A stray **commit delta** at the next version (never with its
//! snapshot) is self-healing: the retry that reaches that version overwrites
//! it, because the snapshot is the authoritative manifest.
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
//! A lake is **one live writer handle per directory** (no file locking).
//! Concurrent readers of one published version are fine. A handle that tries
//! to publish after another handle or process moved the catalog forward
//! fails with [`LakeError::Conflict`] ("another writer published version N;
//! reopen the lake") before its catalog replacement — and two writers racing
//! for the same version cannot both win: the snapshot link is atomic, so
//! exactly one publishes `snapshots/<v>.json`, and `catalog.json` is
//! byte-identical to the snapshot of its version. A **commit** whose publish
//! conflicts is retried under [`CommitSettings`] (DuckLake's
//! `ducklake_max_retry_count`/`ducklake_retry_backoff`/`ducklake_retry_wait_ms`,
//! defaults 10 / 1.5 / 100 ms), rebasing the already-written immutable files
//! onto the new head; a delta whose base files the head no longer contains
//! is refused, never silently re-applied. Over S3 both publications are
//! conditional writes (`If-None-Match: *`, `If-Match: <etag>`), which AWS
//! S3 / Cloudflare R2 evaluate atomically with the write commit. An endpoint
//! that rejects conditionals refuses the write
//! ([`LakeError::UnsupportedEndpoint`]) unless the single-writer degraded
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
//!   catalog, snapshots, commit deltas, data files, and positional delete
//!   files — into `PNKLAKE1` containers (AES-256-GCM, one Argon2id derivation
//!   per session; see the `crypto` module for the format and the
//!   `catalog.json == snapshots/<v>.json` byte-identity story). A keyed open
//!   of a plaintext lake is refused and a keyless open of an encrypted lake
//!   is refused, so a key is never silently ignored. The engine's own
//!   `.pink` containers (`PINKENC1`) are never stored in a lake. Plaintext
//!   data files carry **no checksums**: mutating a data file in place is
//!   undetected by design (an encrypted lake authenticates the whole file,
//!   and binds it to its catalog path). The checks that do run on load are
//!   structural: a v2 file's `pinkdb.table` metadata must name the catalog's
//!   table, agree with the Vortex field names/types, and the chunk and row
//!   counts must match — so a file swapped in from a *differently named*
//!   table is rejected. Swapping two *plaintext* files of the *same* table
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
//!   location is a clear [`LakeError::FeatureMissing`]. Over S3 the
//!   snapshot `put_new` is `If-None-Match: *` and the catalog replace is
//!   `If-Match: <etag>`, so AWS S3 / Cloudflare R2 evaluate conflict
//!   detection atomically with the write; an endpoint that rejects
//!   conditional writes refuses the write with
//!   [`LakeError::UnsupportedEndpoint`] unless the unsafe single-writer
//!   fallback is explicitly accepted (see `S3Config::allow_degraded`).
//!   Single-PUT objects only: no multipart, so one table
//!   file must be ≤ 5 GiB (see the README).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use pinkdb::{DataTable, DataType, PinkDb};

mod attach;
mod catalog;
mod cdc;
mod crypto;
mod delete_file;
pub mod format;
mod incremental;
mod inlined;
mod local;
mod maintenance;
#[cfg(test)]
mod mock_s3;
#[cfg(feature = "s3")]
mod s3;
mod schema;
mod snapshot;
mod stats;
mod storage;
mod time;
#[cfg(feature = "vortexlake")]
mod vortex_file;
mod zone;

pub mod replication;

pub use attach::AttachmentReport;
use catalog::{Catalog, DeleteRef, FileFormat, FileRef, PartitionKey, TableEntry};
pub use cdc::{ChangeKind, ChangeRow};
pub use crypto::EncryptionKey;
pub use incremental::{
    AddFilesReport, Commit, CommitReport, CommitSettings, FlushReport, WritableAttachment,
};
use local::{CATALOG_FILE, LocalStorage};
pub use maintenance::{
    CleanupReport, CleanupSettings, CompactionReport, DEFAULT_RETENTION_SECS, ExpireReport,
    ExpireSettings, LakeFileInfo, LakeOption, LakeSettings, MergeSettings, RewriteSettings,
    TableCompaction, TableInfo,
};
pub use snapshot::{ChangeSummary, SnapshotInfo};
use storage::{CryptoStorage, Head, Location, Storage, VersionTag};

#[cfg(feature = "s3")]
pub use s3::{DEFAULT_RETRIES, S3Config};

/// The catalog format version this build writes (and its newest read).
///
/// Version 6 (M-7): every manifest records the lake's random 128-bit
/// `lake_id`, and `lake.json` (identity) plus `highest.json` (the
/// monotonically increasing published version) anchor it. Manifests are
/// sealed under a `(lake_id, version)` binding; a catalog below
/// `highest.json`, a catalog/snapshot version mismatch, or a manifest from
/// another lake under the same key is refused at open. A v6 manifest without
/// a valid `lake_id` is corrupt; a pre-v6 manifest never has one. A keyed
/// pre-v6 lake is refused with a clear legacy-format error (its manifest
/// containers carry the empty binding and cannot be upgraded safely in
/// place); a plaintext pre-v6 lake opens and is upgraded on its next save.
///
/// Version 5 (wave 65): version 4 plus the per-table **schema record** —
/// `schema_version`, `columns` (every live column with its type, default and
/// the epoch it appeared) and `renames` — so `ALTER TABLE` publishes
/// metadata-only changes and old data files stay readable through the name
/// mapping; every data file records the schema epoch it was written under.
/// A v5 manifest may still carry *legacy entries* (no record): a table a
/// commit did not touch, and tables of a v4 lake that was never re-saved.
/// The lake also carries a lake-wide `schema_version` counter
/// (DuckLake's `snapshot.schema_version`).
///
/// Version 4 (wave 63): the incremental format — every data file carries its
/// own row count, a table lists a growing, never-rewritten file list, and
/// positional delete files (merge-on-read) express `DELETE`/`UPDATE`. Written
/// by every build: Vortex data files with `vortexlake`, engine `.pink`
/// files (with a per-file statistics block) without it.
///
/// Wave 66 extends v4 additively (no version bump): an entry may declare a
/// `partition_by` spec, a data file may record its `partition` values and a
/// zone map (`stats`) in every format; an older v4 reader ignores the fields
/// and reads the same files (it just cannot prune or interpret partitions).
///
/// Wave 68 extends v4/v5 additively as well: a table may carry **inlined
/// rows** (small commits stored in the manifest) with their change history
/// and a row-id counter, a data file may record its `row_id_start`, and the
/// per-commit delta gains the inlining audit fields. A wave-67 reader ignores
/// all of them except the row-count invariant (which already includes live
/// inlined rows in `rows`); this build refuses nothing a wave-67 writer
/// published.
///
/// Versions 1–3 stay readable and are never written any more. Version 3
/// (wave 45): Vortex data files with 8,192-row / ~1 MiB row blocks. Version
/// 2: Vortex data files with one block per engine chunk. Both list one
/// `.vortex` file as a plain string and declare `"format": "vortex"` on
/// every entry. Version 1 (wave 34): one engine `.pink` file per entry (no
/// `format` field) plus one entry-level statistics block.
///
/// A build without `vortexlake` — the `pinklake` builds — writes v6 with
/// `.pink` files and refuses a v2/v3 Vortex lake with a clear
/// [`LakeError::FeatureMissing`], never a corrupt-file error. A v4–v6 lake
/// whose entries include Vortex tables **opens**: those tables are listed by
/// [`Lake::unavailable_tables`], skipped by [`Lake::load`], and refused per
/// table on read/write (wave 88, rule 5).
pub const FORMAT_VERSION: u32 = 6;

/// The oldest catalog format version this build still reads.
pub const MIN_FORMAT_VERSION: u32 = 1;

/// The newest catalog format version this codebase knows: 6 (the schema
/// record plus lake identity/highest-version binding). A manifest above this
/// (a future format) is [`LakeError::Unsupported`] in every build; a v4/v5/v6
/// Vortex lake in a build without `vortexlake` is
/// [`LakeError::FeatureMissing`].
pub const MAX_FORMAT_VERSION: u32 = 6;

/// The `lake.json` identity format this build writes.
pub(crate) const LAKE_ID_FORMAT: u32 = 1;

/// The lake-relative name of the identity object (M-7). Created once, never
/// replaced.
pub(crate) const LAKE_FILE: &str = "lake.json";

/// The lake-relative name of the authenticated highest-published-version
/// marker (M-7). Rewritten after every successful catalog replacement.
pub(crate) const HIGHEST_FILE: &str = "highest.json";

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
    /// The directory is not a lake: `catalog.json` is missing.
    NotALake { path: PathBuf },
    /// `catalog.json` already exists; `create` refuses to overwrite a lake.
    AlreadyALake { path: PathBuf },
    /// Malformed lake metadata (bad JSON, bad shape, bad file references).
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
    /// The object-storage endpoint cannot support a safe write: it rejects
    /// the conditional writes the single-writer contract needs and the
    /// unsafe degraded fallback was not explicitly accepted. Writes are
    /// refused; reads still work. Opt in with `S3Config::allow_degraded(true)`
    /// (or `PINKDB_S3_ALLOW_DEGRADED=1` on the `s3://` URL path) to accept a
    /// single-writer guarantee with a small race window.
    UnsupportedEndpoint { path: PathBuf, message: String },
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
    /// The Vortex layer rejected data while encoding or decoding a data file
    /// (the error is a string because Vortex errors are not `std` error
    /// sources).
    Vortex { context: String, message: String },
    /// An encrypted lake artifact was read without a key: `catalog.json`, a
    /// snapshot, a commit delta, or a data file is a `PNKLAKE1` container (or
    /// this build cannot read it at all). A keyless open never guesses.
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
                "{} is not a pink-lake directory (no catalog.json; use create)",
                path.display()
            ),
            LakeError::AlreadyALake { path } => write!(
                f,
                "{} is already a pink-lake directory (use open)",
                path.display()
            ),
            LakeError::Corrupt { path, message } => {
                write!(f, "corrupt lake metadata at {}: {message}", path.display())
            }
            LakeError::Unsupported { path, found } => {
                if *found > MAX_FORMAT_VERSION {
                    write!(
                        f,
                        "unsupported lake format_version {found} at {} (this build reads \
                         format_version {MIN_FORMAT_VERSION}..={MAX_FORMAT_VERSION}; the lake \
                         was written by a newer pinkdb — upgrade to open it)",
                        path.display()
                    )
                } else if cfg!(feature = "vortexlake") {
                    write!(
                        f,
                        "unsupported lake format_version {found} at {} (this build reads \
                         format_version {MIN_FORMAT_VERSION}..={FORMAT_VERSION} including the \
                         v1 `.pink` form)",
                        path.display()
                    )
                } else {
                    write!(
                        f,
                        "unsupported lake format_version {found} at {} (this build reads/writes \
                         format_version {FORMAT_VERSION} with `.pink` data files; Vortex \
                         format_version 2/3/4/5 needs the `vortexlake` feature)",
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
            LakeError::UnsupportedEndpoint { path, message } => {
                write!(
                    f,
                    "unsupported object-storage endpoint at {}: {message}",
                    path.display()
                )
            }
            LakeError::Io { context, source } => write!(f, "lake I/O error ({context}): {source}"),
            LakeError::Engine { context, source } => {
                write!(f, "lake data error ({context}): {source}")
            }
            LakeError::Vortex { context, message } => {
                write!(f, "lake vortex error ({context}): {message}")
            }
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
                 data-file format {format:?} (this build registers `pink`{}; extensions register \
                 one with pink_lake::format::register_lake_format)",
                path.display(),
                if cfg!(feature = "vortexlake") {
                    " and `vortex`"
                } else {
                    "; `vortex` needs the `vortexlake` feature"
                }
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
/// A v4–v6 lake holding such a table still opens; [`Lake::unavailable_tables`]
/// lists it, [`Lake::load`] skips it, and `.lake attach` lists it with any
/// read failing. The error is the provider-resolution failure
/// ([`LakeError::UnknownFormat`], or [`LakeError::FeatureMissing`] for
/// `vortex` in a build without the `vortexlake` feature) and names the id.
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
    storage: Arc<dyn Storage>,
    /// The lake's random 128-bit identity (hex), recorded in every v6
    /// manifest and in `lake.json`/`highest.json` (M-7).
    lake_id: String,
    format_version: u32,
    version: u64,
    /// The author/commit message [`Lake::set_commit_message`] recorded for the
    /// next published save (DuckLake's `ducklake_set_commit_message`
    /// transaction metadata). Kept across a failed save, cleared by a
    /// successful one.
    pending_commit: Option<(Option<String>, Option<String>)>,
    /// The handle's `data_inlining_row_limit` default, applied to a table
    /// that does not record its own (DuckLake's session setting
    /// `ducklake_default_data_inlining_row_limit`; this handle is the
    /// analogue of a session). Not persisted: every handle starts at
    /// [`DEFAULT_INLINING_ROW_LIMIT`] and a table's persisted
    /// [`TableEntry::inlined_limit`] wins over it.
    inline_row_limit: u64,
    /// The data-file format [`Lake::set_write_format`] selected for this
    /// handle's subsequent writes; `None` keeps the build default (Vortex
    /// with `vortexlake`, else `pink`). Not persisted — a reopened handle
    /// starts at the default again.
    write_format_choice: Option<FileFormat>,
    /// Per-table format choices ([`Lake::set_table_format`]), keyed by the
    /// flat `db.table` catalog key. A choice wins over the entry's pinned
    /// format for a full save and for a table the handle creates; an append
    /// to an existing table always keeps the entry's format (see the module
    /// docs). Not persisted.
    table_format_choices: BTreeMap<String, FileFormat>,
}

impl Lake {
    /// Create a new lake at `dir`: the directory (and parents) plus an empty
    /// catalog at snapshot version 1 (a manifest of this build's
    /// [`FORMAT_VERSION`]) and its snapshot. Fails if `dir` already holds a
    /// lake.
    ///
    /// The empty version-1 snapshot is *linked* into place (never replaced).
    /// If a concurrent or crashed create already left a valid empty version-1
    /// snapshot, it is adopted instead of rewritten — both concurrent creates
    /// can only end with exactly one version-1 catalog, byte-identical to its
    /// snapshot, and the loser retries [`Lake::open`] via
    /// [`Lake::open_or_create`].
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        Self::create_with(Arc::new(LocalStorage::new(dir.as_ref())), None)
    }

    /// [`Lake::create`] with at-rest encryption: every artifact this lake
    /// ever publishes — catalog, snapshots, commit deltas, data files, delete
    /// files — is sealed into a `PNKLAKE1` container under `key` (see the
    /// `crypto` module docs for the format and the KDF policy; one Argon2id
    /// derivation per session). Reading the lake then requires the same key;
    /// a wrong key fails closed and a keyless open refuses.
    pub fn create_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Self> {
        Self::create_with(Arc::new(LocalStorage::new(dir.as_ref())), Some(key))
    }

    /// Create a lake at a location string: a local directory, or an
    /// `s3://bucket/prefix` URL when the `s3` feature is built (a clear
    /// [`LakeError::FeatureMissing`] otherwise). S3 credentials, endpoint and
    /// region come from the usual environment variables (`AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_ENDPOINT_URL`/
    /// `AWS_ENDPOINT_URL_S3`, `AWS_REGION`/`AWS_DEFAULT_REGION`,
    /// `AWS_S3_FORCE_PATH_STYLE`), and the safety opt-ins
    /// `PINKDB_S3_ALLOW_HTTP` (plain-http test endpoints) and
    /// `PINKDB_S3_ALLOW_DEGRADED` (single-writer fallback on endpoints
    /// without conditional writes); use `Lake::create_s3` (with the `s3`
    /// feature) for explicit configuration.
    pub fn create_url(location: &str) -> Result<Self> {
        Self::create_with(storage_for(Location::parse(location)?)?, None)
    }

    /// [`Lake::create_url`] with at-rest encryption (local directory or
    /// `s3://bucket/prefix`): a keyed create in a build without the
    /// `encryption` feature fails before touching any storage.
    pub fn create_url_with_key(location: &str, key: EncryptionKey) -> Result<Self> {
        Self::create_with(storage_for(Location::parse(location)?)?, Some(key))
    }

    fn create_with(inner: Arc<dyn Storage>, key: Option<EncryptionKey>) -> Result<Self> {
        let storage: Arc<dyn Storage> = Arc::new(CryptoStorage::new(inner, key)?);
        let root = storage.abs("");
        if storage.exists(CATALOG_FILE)? {
            return Err(LakeError::AlreadyALake { path: root });
        }
        storage.create_dir("")?;
        // M-7: one random 128-bit identity per lake, fixed for its lifetime.
        // An interrupted create may already have written it; adopt that id
        // instead of minting a second one.
        let lake_id = match read_identity(&*storage, &root)? {
            Some(id) => id,
            None => {
                let id = fresh_lake_id()?;
                storage.put_new(LAKE_FILE, &identity_json(&id)?)?;
                id
            }
        };
        storage.set_manifest_lake_id(Some(lake_id.clone()));
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
        let plain = manifest_json(&manifest, &root)?;
        // Seal the manifest **once**: the same bytes are published as the
        // snapshot and as the catalog (the byte-identity invariant of every
        // published version). A keyless create seals nothing.
        let bytes = storage.seal_manifest(&plain, &lake_id, 1)?;
        // Adopt (do not rewrite) an empty version-1 snapshot a concurrent or
        // interrupted create already put there: the catalog must stay
        // byte-identical to the snapshot of its version, so the *stored*
        // bytes are what gets republished (sealing the adopted plaintext
        // again would mint a different container).
        let bytes = if storage.put_new("snapshots/1.json", &bytes)? {
            bytes
        } else {
            let rel = "snapshots/1.json";
            let path = storage.abs(rel);
            let (existing, stored) =
                storage.read_with_stored(rel, &format!("reading {}", path.display()))?;
            let parsed = read_manifest(&path, &existing).map_err(|e| LakeError::Conflict {
                path: path.clone(),
                message: format!(
                    "snapshots/1.json exists but does not parse ({e}); refusing to adopt it"
                ),
            })?;
            if parsed.version != 1 || !parsed.tables.is_empty() {
                return Err(LakeError::Conflict {
                    path,
                    message: "snapshots/1.json exists but is not the empty version 1 snapshot; \
                              refusing to adopt it"
                        .to_string(),
                });
            }
            stored
        };
        if !storage.put_new(CATALOG_FILE, &bytes)? {
            return Err(LakeError::AlreadyALake { path: root });
        }
        // Publish the rollback marker *after* the catalog: a crash between
        // the two leaves a catalog without a marker (refused at open), never
        // a marker above the authoritative catalog (which would look like a
        // rollback).
        storage.put_highest(HIGHEST_FILE, &highest_json(&lake_id, 1)?, &lake_id, 1)?;
        Ok(Self {
            root,
            storage,
            lake_id,
            format_version: FORMAT_VERSION,
            version: 1,
            pending_commit: None,
            inline_row_limit: DEFAULT_INLINING_ROW_LIMIT,
            write_format_choice: None,
            table_format_choices: BTreeMap::new(),
        })
    }

    /// Open an existing lake: `dir/catalog.json` must exist and parse.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(Arc::new(LocalStorage::new(dir.as_ref())), None)
    }

    /// [`Lake::open`] with at-rest encryption: the catalog and every artifact
    /// read must be `PNKLAKE1` containers under `key`. A plaintext lake is
    /// refused ([`LakeError::NotEncrypted`], so a key can never be silently
    /// ignored), and a wrong key fails with [`LakeError::WrongKey`] at the
    /// catalog before anything else is read.
    pub fn open_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Self> {
        Self::open_with(Arc::new(LocalStorage::new(dir.as_ref())), Some(key))
    }

    /// Open a lake at a location string: a local directory, or an
    /// `s3://bucket/prefix` URL when the `s3` feature is built (see
    /// [`Lake::create_url`] for the environment variables). Use
    /// `Lake::open_s3` (with the `s3` feature) for explicit S3
    /// configuration.
    pub fn open_url(location: &str) -> Result<Self> {
        Self::open_with(storage_for(Location::parse(location)?)?, None)
    }

    /// [`Lake::open_url`] with at-rest encryption.
    pub fn open_url_with_key(location: &str, key: EncryptionKey) -> Result<Self> {
        Self::open_with(storage_for(Location::parse(location)?)?, Some(key))
    }

    fn open_with(inner: Arc<dyn Storage>, key: Option<EncryptionKey>) -> Result<Self> {
        let storage: Arc<dyn Storage> = Arc::new(CryptoStorage::new(inner, key)?);
        let root = storage.abs("");
        storage.tighten_existing()?;
        let identity = read_identity(&*storage, &root)?;
        if let Some(id) = &identity {
            storage.set_manifest_lake_id(Some(id.clone()));
        }
        let manifest = read_catalog(&*storage, &root)?;
        let lake_id = match manifest.lake_id.clone() {
            Some(id) => {
                match &identity {
                    Some(expected) if expected == &id => {}
                    Some(expected) => {
                        return Err(LakeError::Corrupt {
                            path: root.join(CATALOG_FILE),
                            message: format!(
                                "catalog.json declares lake {id}, but lake.json identifies \
                                 this lake as {expected} (a manifest from another lake was \
                                 substituted)"
                            ),
                        });
                    }
                    None => {
                        return Err(LakeError::Corrupt {
                            path: root.join(LAKE_FILE),
                            message: "catalog.json is a v6 manifest, but lake.json is missing \
                                      (the lake identity was removed)"
                                .to_string(),
                        });
                    }
                }
                id
            }
            None => {
                // A plaintext pre-v6 lake opens and is upgraded on its next
                // save. A keyed pre-v6 lake cannot be authenticated safely:
                // its manifest containers carry the empty binding.
                if storage.encrypted() {
                    return Err(LakeError::Corrupt {
                        path: root.join(CATALOG_FILE),
                        message: "this is a pre-v6 encrypted lake: its manifest containers \
                                  carry no lake-id/version binding and this build refuses to \
                                  open a weaker-bound format; create a new keyed lake (or \
                                  load and re-save it with a pre-M-7 build)"
                            .to_string(),
                    });
                }
                match identity {
                    Some(id) => id,
                    None => fresh_lake_id()?,
                }
            }
        };
        if manifest.format_version >= 6 {
            // The authenticated highest-version marker: a catalog below it is
            // a rollback. A catalog *above* it is the narrow crash window
            // between the catalog replace and the marker write; heal it.
            let highest = read_highest(&*storage, &lake_id)?;
            if manifest.version < highest {
                return Err(LakeError::Corrupt {
                    path: root.join(CATALOG_FILE),
                    message: format!(
                        "rollback detected: catalog.json declares version {}, but the \
                         authenticated highest.json records version {highest} (restore the \
                         catalog at version {highest} or later)",
                        manifest.version
                    ),
                });
            }
            if manifest.version > highest {
                storage.put_highest(
                    HIGHEST_FILE,
                    &highest_json(&lake_id, manifest.version)?,
                    &lake_id,
                    manifest.version,
                )?;
            }
            // Catalog and snapshot of the same version must agree on both the
            // version and the lake identity (M-7).
            let (snapshot, snapshot_path) = manifest_for(&*storage, &root, Some(manifest.version))?;
            if snapshot.version != manifest.version
                || snapshot.lake_id.as_deref() != Some(lake_id.as_str())
            {
                return Err(LakeError::Corrupt {
                    path: snapshot_path,
                    message: format!(
                        "catalog.json version {} and its snapshot disagree on the version or \
                         lake identity (expected {lake_id})",
                        manifest.version
                    ),
                });
            }
        }
        Ok(Self {
            root,
            storage,
            lake_id,
            format_version: manifest.format_version,
            version: manifest.version,
            pending_commit: None,
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

    /// Create a lake from explicit [`S3Config`] (endpoint, region,
    /// credentials, bucket/prefix) instead of the environment.
    #[cfg(feature = "s3")]
    pub fn create_s3(config: S3Config) -> Result<Self> {
        Self::create_with(Arc::new(s3::S3Storage::new(config)?), None)
    }

    /// [`Lake::create_s3`] with at-rest encryption.
    #[cfg(feature = "s3")]
    pub fn create_s3_with_key(config: S3Config, key: EncryptionKey) -> Result<Self> {
        Self::create_with(Arc::new(s3::S3Storage::new(config)?), Some(key))
    }

    /// Open a lake from explicit [`S3Config`].
    #[cfg(feature = "s3")]
    pub fn open_s3(config: S3Config) -> Result<Self> {
        Self::open_with(Arc::new(s3::S3Storage::new(config)?), None)
    }

    /// [`Lake::open_s3`] with at-rest encryption.
    #[cfg(feature = "s3")]
    pub fn open_s3_with_key(config: S3Config, key: EncryptionKey) -> Result<Self> {
        Self::open_with(Arc::new(s3::S3Storage::new(config)?), Some(key))
    }

    /// [`Lake::open_s3`] when the bucket/prefix already holds a lake, else
    /// [`Lake::create_s3`].
    #[cfg(feature = "s3")]
    pub fn open_or_create_s3(config: S3Config) -> Result<Self> {
        match Self::open_s3(config.clone()) {
            Ok(lake) => Ok(lake),
            Err(LakeError::NotALake { .. }) => match Self::create_s3(config.clone()) {
                Ok(lake) => Ok(lake),
                Err(LakeError::AlreadyALake { .. }) => Self::open_s3(config),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// [`Lake::open_or_create_s3`] with at-rest encryption.
    #[cfg(feature = "s3")]
    pub fn open_or_create_s3_with_key(config: S3Config, key: EncryptionKey) -> Result<Self> {
        match Self::open_s3_with_key(config.clone(), key.clone()) {
            Ok(lake) => Ok(lake),
            Err(LakeError::NotALake { .. }) => {
                match Self::create_s3_with_key(config.clone(), key.clone()) {
                    Ok(lake) => Ok(lake),
                    Err(LakeError::AlreadyALake { .. }) => Self::open_s3_with_key(config, key),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// The lake location: a directory path or the `s3://bucket/prefix` URL.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The catalog `format_version` this handle last saw: the version the lake
    /// was opened with, or [`FORMAT_VERSION`] after a successful save (a save
    /// from a v1 lake migrates it to v3 in a `vortexlake` build; a build
    /// without the feature always publishes v1 and keeps reporting 1).
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// The current version (from `catalog.json`, cached after open/save).
    pub fn current_version(&self) -> u64 {
        self.version
    }

    /// The lake's random 128-bit identity (32 lowercase hex characters), as
    /// recorded in v6 manifests, `lake.json` and `highest.json` (M-7).
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
        for version in self.versions()? {
            match manifest_for(&*self.storage, &self.root, Some(version)) {
                Ok((manifest, path)) => {
                    if self.check_lake_id(&manifest, &path).is_err() {
                        continue;
                    }
                    let time_micros = time::parse_iso8601_utc(&manifest.updated);
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

    /// Published snapshot versions, ascending: every `snapshots/<v>.json`
    /// that is named canonically (`<decimal>`, no leading zeros), declares
    /// its own version `v`, and parses. A leftover manifest from an
    /// interrupted publish (`v > current`) is ignored, as are corrupt
    /// manifests, `01.json`-style names, and non-numeric names — they are
    /// never loadable through this handle. A snapshot that cannot be *read*
    /// (permissions, a transient object-store error) is an error, never a
    /// silently missing version; one deleted between listing and reading is
    /// skipped.
    pub fn versions(&self) -> Result<Vec<u64>> {
        published_versions(&*self.storage, self.version)
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

    /// Footer-only *attach* of the latest version (`None`) or a specific
    /// snapshot: read the manifest, then the statistics attach needs — a
    /// Vortex data file's footer, metadata segment and stored statistics
    /// (`vortexlake` builds; no column data is read or decoded, unless the
    /// files live on object storage — whole-object reads, still no decode —
    /// or the lake is encrypted, where each container is unsealed whole
    /// first: the documented stage-1 cost, with footer-range reads over
    /// unsealed segments as the follow-up), or the `.pink` files' published
    /// statistics blocks (every build: the manifest is all attach reads;
    /// `data` files are never opened, encrypted or not).
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
        let (_, report) = attach::build(self, version)?;
        Ok(report)
    }

    /// The tables of the current version this build cannot read (wave 88,
    /// rule 5): each entry's format id has no registered provider in this
    /// process ([`LakeError::UnknownFormat`], or [`LakeError::FeatureMissing`]
    /// for `vortex` without the `vortexlake` feature).
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

    /// The published commit-delta manifest versions, ascending: every
    /// `commits/<v>.json` naming a version at or below the current one (and
    /// below this build's newest format). The delta manifests are the
    /// append-only audit trail of how each version was built (which data and
    /// delete files it added); the snapshot manifests stay authoritative for
    /// loading.
    pub fn commits(&self) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for name in self.storage.list("commits")? {
            let Some(v) = name
                .strip_suffix(".json")
                .and_then(canonical_version_number)
            else {
                continue;
            };
            if v < 1 || v > self.version {
                continue;
            }
            out.push(v);
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
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
    /// fails without replacing an existing `snapshots/<version>.json`, and
    /// re-checks the catalog right before replacing it, so a concurrent
    /// publication can never produce a catalog that disagrees with the
    /// snapshot of its version.
    pub fn write_from_store_report(&mut self, db: &PinkDb) -> Result<SaveReport> {
        validate_store(db)?;
        // Fail fast on a handle another writer has already outdated (before
        // any data file is written).
        self.ensure_catalog_unchanged()?;
        // M-7: a plaintext pre-v6 lake gets its identity here, before the
        // manifest that records it is built.
        self.ensure_identity()?;
        let version = self
            .version
            .checked_add(1)
            .filter(|v| *v <= MAX_VERSION)
            .ok_or_else(|| LakeError::Overflow {
                path: self.root.clone(),
                version: self.version,
            })?;
        let snapshot_rel = format!("snapshots/{version}.json");
        if self.storage.exists(&snapshot_rel)? {
            return Err(self.snapshot_exists_error(version));
        }
        // The previous version's manifest, read before the data loop: a full
        // save **preserves the partitioning** of a table that is saved again
        // under the same key (the store itself has no partition concept), so
        // re-saving a partitioned table re-splits its rows the same way. The
        // handle is at the head (`ensure_catalog_unchanged` above), and
        // `catalog.json` is byte-identical to a healthy snapshot, so fall back
        // to the catalog when the snapshot read fails: an unreadable
        // predecessor must not silently reset the lake-wide monotonic
        // `schema_version` (or drop a partition spec). If even the catalog
        // cannot be read, the save fails loudly.
        let previous = match manifest_for(&*self.storage, &self.root, Some(self.version)) {
            Ok((manifest, _)) => Some(manifest),
            Err(_) => Some(crate::read_catalog(&*self.storage, &self.root)?),
        };
        // The head catalog is the authoritative entry set. A table this build
        // cannot read is carried forward verbatim when the store has no table
        // under its key; a store table under that key would replace data this
        // build cannot read, so the save is refused (wave 88, rule 5).
        let head = crate::read_catalog(&*self.storage, &self.root)?;
        let head_path = self.storage.abs(CATALOG_FILE);
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
                // Wave 88: the per-table choice wins, else a previous entry
                // that is pinned (or names a non-builtin format) keeps its
                // format and is pinned, else the handle's write format (pinned
                // only when it was chosen explicitly — the build default keeps
                // the wave-45 migration rule). One data file per partition
                // value set (exactly one for an unpartitioned or empty table),
                // written before anything references it (see
                // [`Lake::write_data_files`] for the encoders). Every file of
                // a recorded entry then carries the schema epoch it was
                // written under (1: a full save writes the current schema).
                let (format, format_pinned) = self.resolve_save_format(&key, previous_entry);
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
                        files,
                        rows: src.num_rows() as u64,
                        // Wave 89: a save preserves the table identity of the
                        // same key (a dropped + recreated table was dropped by
                        // an earlier published version, so this lookup no
                        // longer finds the old entry); a fresh table is
                        // assigned one below.
                        table_id: previous_entry.and_then(|e| e.table_id.clone()),
                        format: Some(format.clone()),
                        format_pinned,
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
        // statistics, partition spec, schema record and pin flag (the store
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
        // The per-version record: the author/message [`Lake::set_commit_message`]
        // recorded for this save (kept pending until the save succeeds below),
        // and the change summary — the diff against the previous version's
        // manifest (read above). A previous manifest that was unavailable
        // means the summary is simply absent.
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
        let plain = manifest_json(&manifest, &self.root)?;
        // Seal the manifest **once**: the exact same bytes are published as
        // `snapshots/<version>.json` and as `catalog.json`, so the
        // byte-identity invariant survives encryption. A keyless save seals
        // nothing.
        let bytes = self.storage.seal_manifest(&plain, &self.lake_id, version)?;
        let commit_manifest = catalog::CommitManifest {
            format_version: FORMAT_VERSION,
            version,
            base_version: version - 1,
            updated: time::iso8601_utc(SystemTime::now()),
            tables: deltas,
        };
        let commit_bytes = commit_manifest_json(&commit_manifest)?;
        // Same publication order as a commit: the delta manifest, the
        // snapshot, then the catalog.
        let commit_rel = format!("commits/{version}.json");
        if !self.storage.put_new(&commit_rel, &commit_bytes)? {
            let snapshot_exists = self.storage.exists(&snapshot_rel)?;
            if snapshot_exists {
                return Err(self.snapshot_exists_error(version));
            }
            // A stray delta from a crashed attempt that never linked its
            // snapshot: the snapshot is authoritative, so heal it.
            self.storage.put(&commit_rel, &commit_bytes)?;
        }
        bytes_written += commit_bytes.len() as u64;
        // Create-only, never replace: an existing snapshot version is a
        // published version (or an unpublished stray the caller must remove).
        // Locally a hard link; over S3 `If-None-Match: *` — the condition
        // evaluates on the *sealed* bytes exactly as it would on plaintext.
        if !self.storage.put_new(&snapshot_rel, &bytes)? {
            return Err(self.snapshot_exists_error(version));
        }
        bytes_written += bytes.len() as u64;
        self.replace_catalog(version, &plain, &bytes, "save")?;
        bytes_written += bytes.len() as u64;
        Ok(SaveReport {
            version,
            tables: manifest.tables.len(),
            rows,
            bytes_written,
        })
    }

    /// Choose the registered data-file format this handle writes for every
    /// subsequent save/append that does not pin a format explicitly (see the
    /// [`crate::format`] module). The id must have a provider registered in this
    /// process — an unknown id is [`LakeError::UnknownFormat`], and `vortex`
    /// in a build without the `vortexlake` feature is
    /// [`LakeError::FeatureMissing`]; nothing is silently substituted.
    ///
    /// The choice is per handle and not persisted: [`Lake::open`] and
    /// [`Lake::create`] always start with the build default (Vortex with
    /// `vortexlake`, else `.pink`). It applies to a table with no per-table
    /// choice ([`Lake::set_table_format`]) and no already-pinned catalog
    /// format — a full save preserves an existing table's format, and a
    /// commit appends in it. Set it on the handle performing the write;
    /// maintenance rewrites always re-encode under the entry's own pinned
    /// format (or the target of [`Lake::rewrite_table_format`]).
    ///
    /// **The choice pins (wave 88):** a table this handle writes in the
    /// format selected here gets `"format_pinned": true` in its catalog
    /// entry, so later full saves keep it. An entry written under the build
    /// default (this method never called) stays unpinned and keeps the
    /// wave-45 migration behavior — a full save re-encodes it into the
    /// saving build's write format.
    pub fn set_write_format(&mut self, id: &str) -> Result<()> {
        crate::format::format_for(id, &self.root.join(CATALOG_FILE))?;
        self.write_format_choice = Some(FileFormat::new(id));
        Ok(())
    }

    /// Choose the data-file format for one table on this handle, keyed by
    /// `db.table` (wave 88). The id is validated against the registry exactly
    /// like [`Lake::set_write_format`] — an id with no provider is
    /// [`LakeError::UnknownFormat`], `vortex` without the `vortexlake`
    /// feature is [`LakeError::FeatureMissing`], never a silent substitution.
    ///
    /// The choice wins over the format the table's catalog entry already
    /// pins for a **full save** (so a per-table choice is how a save asks for
    /// a table to be re-encoded), and it selects the format of a table the
    /// handle creates through a commit. A table saved with this choice is
    /// recorded **pinned** (`"format_pinned": true`), so a later save keeps
    /// it even without the choice. An **append** to an existing table always
    /// keeps that entry's pinned format and pin flag — one catalog entry
    /// names one data-file format, so a commit or flush never mixes formats
    /// inside it. To convert an existing table use
    /// [`Lake::rewrite_table_format`] (which also pins the result).
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
        crate::format::format_for(id, &self.root.join(CATALOG_FILE))?;
        self.table_format_choices
            .insert(format!("{db}.{table}"), FileFormat::new(id));
        Ok(())
    }

    /// The data-file format this handle writes by default: the one
    /// [`Lake::set_write_format`] selected, else the build default — Vortex
    /// with `vortexlake`, engine `.pink` bytes without it.
    fn write_format(&self) -> FileFormat {
        self.write_format_choice
            .clone()
            .unwrap_or_else(default_write_format)
    }

    /// The format **and pin flag** a full save of `key` writes (wave 88):
    ///
    /// 1. the handle's per-table choice — that format, pinned;
    /// 2. else a previous entry whose format is pinned (or whose id is not a
    ///    built-in default) — its format, pinned;
    /// 3. else the handle's write format — pinned exactly when
    ///    [`Lake::set_write_format`] was called explicitly, so the build
    ///    default keeps the wave-45 migration behavior.
    fn resolve_save_format(&self, key: &str, previous: Option<&TableEntry>) -> (FileFormat, bool) {
        if let Some(format) = self.table_format_choices.get(key) {
            return (format.clone(), true);
        }
        if let Some(entry) = previous {
            let format = entry.format();
            if entry.format_pinned || !is_builtin_format(format.id()) {
                return (format, true);
            }
        }
        (self.write_format(), self.write_format_choice.is_some())
    }

    /// The format **and pin flag** a commit/flush writes for `key` (wave 88):
    /// an existing entry's format and pin flag always win (an append never
    /// mixes formats inside one entry); a table the commit creates uses the
    /// handle's per-table choice (pinned), else its write format (pinned when
    /// explicitly chosen).
    fn resolve_append_format(&self, key: &str, entry: Option<&TableEntry>) -> (FileFormat, bool) {
        match entry {
            Some(entry) => (entry.format(), entry.format_pinned),
            None => match self.table_format_choices.get(key) {
                Some(format) => (format.clone(), true),
                None => (self.write_format(), self.write_format_choice.is_some()),
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
    /// without opening the file. Vortex additionally carries the same facts in
    /// its footer (cross-checked at attach).
    fn write_data_file(
        &self,
        key: &str,
        format: FileFormat,
        src: &DataTable,
    ) -> Result<(FileRef, u64)> {
        let (dbname, table) = split_key(key, &self.root.join(CATALOG_FILE))?;
        let provider = crate::format::format_for(format.id(), &self.root.join(CATALOG_FILE))?;
        let dir = format!("data/{}", encode_key(key));
        let id = fresh_id();
        let rows = src.num_rows() as u64;
        let rel = format!("{dir}/{id}.{}", provider.extension());
        let mut saved = None;
        let written = self.storage.put_streamed(&rel, &mut |sink| {
            saved = Some(provider.write_streamed(dbname, table, src, sink)?);
            Ok(())
        })?;
        let saved = saved.ok_or_else(|| LakeError::Internal {
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
    ) -> Result<(DeleteRef, u64)> {
        split_key(key, &self.root.join(CATALOG_FILE))?;
        validate_rel_path(file)?;
        let positions = delete_file::normalize(positions.to_vec());
        let dir = format!("data/{}", encode_key(key));
        let rel = format!("{dir}/{}.pinkdel", fresh_id());
        let bytes = delete_file::encode(&positions);
        self.storage.put(&rel, &bytes)?;
        Ok((
            DeleteRef {
                path: rel,
                file: file.to_string(),
                count: positions.len() as u64,
            },
            bytes.len() as u64,
        ))
    }

    /// Replace `catalog.json` with the manifest of `version` after re-checking
    /// that the catalog still declares the version this handle is based on
    /// (so a concurrent publication can never produce a catalog that
    /// disagrees with the snapshot of its version), with the degraded
    /// (check-then-put) endpoint reconciliation. Updates this handle.
    ///
    /// `plain` is the manifest as written (`snapshots/<version>.json`'s
    /// logical content) and `sealed` the exact bytes published there; for a
    /// keyless lake they are the same bytes. The conditional replace sends
    /// the *sealed* bytes, while the degraded-path verification compares the
    /// decrypted catalog against `plain` (the bytes a reader sees).
    fn replace_catalog(
        &mut self,
        version: u64,
        plain: &[u8],
        sealed: &[u8],
        what: &str,
    ) -> Result<()> {
        // M-7: a plaintext pre-v6 lake has no identity object yet; create it
        // before the catalog that names it (the manifest bytes already carry
        // `lake_id`). Every publishing path funnels through here.
        self.ensure_identity()?;
        // Re-check that the catalog is still the version our snapshot was
        // taken from, right before replacing it, and replace it
        // conditionally: `If-Match: <etag>` over S3 (a 412 is the conflict
        // below), a file-identity re-check locally.
        let catalog_tag = self.ensure_catalog_unchanged()?;
        if !self
            .storage
            .put_if_match(CATALOG_FILE, sealed, &catalog_tag)?
        {
            return Err(LakeError::Conflict {
                path: self.storage.abs(CATALOG_FILE),
                message: format!(
                    "catalog.json changed while this {what} was in flight (this handle is \
                     based on version {}); reopen the lake",
                    self.version
                ),
            });
        }
        // A degraded (check-then-put) endpoint cannot make the replace
        // atomic, so verify what actually got published before reporting
        // success: read the catalog back, and if another writer's catalog is
        // authoritative now, report the conflict and restore the snapshot
        // pair from the winning bytes. If ours landed, repair
        // `snapshots/<version>.json` in case a racing writer replaced it
        // between its PUT and this replace. No-op (and no extra requests) on
        // local storage and conditional-write endpoints.
        if self.storage.uses_degraded_fallback() {
            let path = self.storage.abs(CATALOG_FILE);
            let (current, _) = self
                .storage
                .read_tagged(CATALOG_FILE, &format!("reading {}", path.display()))?;
            if current != plain {
                let winner = read_manifest(&path, &current)?;
                self.storage.reconcile_snapshot(winner.version, &current)?;
                return Err(LakeError::Conflict {
                    path,
                    message: format!(
                        "catalog.json changed while this {what} was in flight (this handle \
                         is based on version {}); reopen the lake",
                        self.version
                    ),
                });
            }
            self.storage.reconcile_snapshot(version, plain)?;
        }
        // M-7: publish the authenticated highest-version marker only after
        // the catalog replacement succeeded. A later open refuses a catalog
        // below it; a crash between the two is the narrow window healed at
        // open (catalog above marker, never the reverse).
        self.write_highest(version)?;
        self.version = version;
        // The commit record is published; a later save without a new
        // `set_commit_message` must not repeat it.
        self.pending_commit = None;
        // A publish writes this build's format (v4), so a handle that opened
        // an older-format lake reports the new format afterwards.
        self.format_version = FORMAT_VERSION;
        Ok(())
    }

    /// The manifest for the latest version (`None`) or one snapshot, with the
    /// path it was read from (for error attribution). The lake identity is
    /// cross-checked here, so a snapshot substituted from another lake (same
    /// key) is refused even when it parses.
    fn manifest(&self, version: Option<u64>) -> Result<(Catalog, PathBuf)> {
        let (manifest, path) = manifest_for(&*self.storage, &self.root, version)?;
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

    /// Write (or repair) `lake.json` before publishing a v6 manifest. A
    /// plaintext pre-v6 lake has no identity yet; its first save creates one
    /// here. A concurrent creator that won the create-only write is adopted.
    fn ensure_identity(&mut self) -> Result<()> {
        match read_identity(&*self.storage, &self.root)? {
            Some(id) => {
                if id != self.lake_id {
                    self.lake_id = id;
                }
                Ok(())
            }
            None => {
                let bytes = identity_json(&self.lake_id)?;
                if !self.storage.put_new(LAKE_FILE, &bytes)? {
                    let id = read_identity(&*self.storage, &self.root)?.ok_or_else(|| {
                        LakeError::Corrupt {
                            path: self.root.join(LAKE_FILE),
                            message: "lake.json appeared and disappeared while creating the \
                                      lake identity"
                                .to_string(),
                        }
                    })?;
                    self.lake_id = id;
                }
                Ok(())
            }
        }
    }

    /// Publish `highest.json` for `version`, refusing to move it backwards.
    fn write_highest(&self, version: u64) -> Result<()> {
        if self.storage.exists(HIGHEST_FILE)? {
            let current = read_highest(&*self.storage, &self.lake_id)?;
            if version < current {
                return Err(LakeError::Conflict {
                    path: self.root.join(HIGHEST_FILE),
                    message: format!(
                        "refusing to move the highest published version backwards ({} -> \
                         {version})",
                        current
                    ),
                });
            }
        }
        self.storage.put_highest(
            HIGHEST_FILE,
            &highest_json(&self.lake_id, version)?,
            &self.lake_id,
            version,
        )
    }

    /// A save-time conflict: `snapshots/<version>.json` is already there.
    fn snapshot_exists_error(&self, version: u64) -> LakeError {
        LakeError::Conflict {
            path: self.storage.abs(&format!("snapshots/{version}.json")),
            message: format!(
                "snapshot {version} already exists; refusing to replace a published version \
                 (if catalog.json still points at version {} it was never published — \
                 remove the stray snapshot and retry)",
                self.version
            ),
        }
    }

    /// The catalog must still declare the version this handle was opened or
    /// last saved from; anything else means another writer published. Returns
    /// the catalog's version tag for the conditional replace that follows.
    fn ensure_catalog_unchanged(&self) -> Result<VersionTag> {
        let path = self.storage.abs(CATALOG_FILE);
        // A directory-shaped catalog (local) still proceeds to the read and
        // its I/O error, exactly as before; only a missing file is the
        // conflict below.
        if matches!(self.storage.head(CATALOG_FILE)?, Head::Missing) {
            return Err(LakeError::Conflict {
                path,
                message: format!(
                    "catalog.json is missing (this handle is based on version {}); reopen the lake",
                    self.version
                ),
            });
        }
        let (bytes, tag) = self
            .storage
            .read_tagged(CATALOG_FILE, &format!("reading {}", path.display()))?;
        let current = read_manifest(&path, &bytes)?;
        if current.version != self.version {
            // A degraded endpoint's check-then-put window can let this save
            // overwrite a *published* snapshot (`snapshots/<current.version>`
            // written from the catalog of the writer it raced). The catalog
            // is authoritative and byte-identical to its snapshot by
            // construction, so repair the pair before reporting the
            // conflict. Local storage and conditional-write endpoints have
            // nothing to repair (the default implementation is a no-op).
            self.storage
                .reconcile_snapshot(current.version, &bytes)
                .map_err(|e| LakeError::Conflict {
                    path: path.clone(),
                    message: format!(
                        "another writer published version {} (this handle is based on version \
                         {}); reopen the lake — and the catalog/snapshot pair could not be \
                         reconciled afterwards ({e})",
                        current.version, self.version
                    ),
                })?;
            return Err(LakeError::Conflict {
                path,
                message: format!(
                    "another writer published version {} (this handle is based on version {}); \
                     reopen the lake",
                    current.version, self.version
                ),
            });
        }
        Ok(tag)
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
        let provider = crate::format::format_for(format.id(), &self.root.join(CATALOG_FILE))?;
        let path = self.storage.abs(rel);
        let file = crate::format::DataFile::new(&path, rel, &*self.storage);
        provider.read(&file)
    }
}

/// Whether a format id is one of the built-in defaults (`pink`, `vortex`) —
/// the formats an entry written under the build default may carry. An entry
/// naming anything else is always treated as pinned (wave 88, rule 2).
fn is_builtin_format(id: &str) -> bool {
    id == crate::format::PINK_FORMAT_ID || id == crate::format::VORTEX_FORMAT_ID
}

/// The build's default data-file format: Vortex with `vortexlake`, engine
/// `.pink` bytes without it.
fn default_write_format() -> FileFormat {
    #[cfg(feature = "vortexlake")]
    {
        FileFormat::new(crate::format::VORTEX_FORMAT_ID)
    }
    #[cfg(not(feature = "vortexlake"))]
    {
        FileFormat::pink()
    }
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

/// [`save_store_url`] with explicit [`S3Config`] (bucket/prefix/endpoint/
/// region/credentials) instead of the environment.
#[cfg(feature = "s3")]
pub fn save_store_s3(db: &PinkDb, config: S3Config) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create_s3(config)?.write_from_store(db)
}

/// [`save_store_s3`] with at-rest encryption.
#[cfg(feature = "s3")]
pub fn save_store_s3_with_key(db: &PinkDb, config: S3Config, key: EncryptionKey) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create_s3_with_key(config, key)?.write_from_store(db)
}

/// The storage backend for a parsed location: local always, S3 only with the
/// `s3` feature (a clear error otherwise).
fn storage_for(location: Location) -> Result<Arc<dyn Storage>> {
    match location {
        Location::Local(path) => Ok(Arc::new(LocalStorage::new(&path))),
        Location::S3 { bucket, prefix } => s3_storage(&bucket, &prefix),
    }
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
             feature (for the CLI: `cargo build -p pinkdb-cli --features vortexlake,s3`)"
        ),
    })
}

/// The largest encoded `data/<table>` directory segment a writer accepts,
/// so `<segment>/<uuid>.vortex.<uuid>.tmp` (the data file plus a unique temp
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

/// The manifest for the latest version (`None`) or one snapshot, with the path
/// it was read from (for error attribution). Free function so an attachment —
/// which holds only the storage and the root, not the [`Lake`] handle — can
/// read arbitrary versions for time travel.
pub(crate) fn manifest_for(
    storage: &dyn Storage,
    root: &Path,
    version: Option<u64>,
) -> Result<(Catalog, PathBuf)> {
    match version {
        None => {
            let manifest = read_catalog(storage, root)?;
            Ok((manifest, root.join(CATALOG_FILE)))
        }
        Some(v) => {
            let rel = format!("snapshots/{v}.json");
            let path = storage.abs(&rel);
            if !storage.exists(&rel)? {
                return Err(LakeError::VersionNotFound {
                    path: root.to_path_buf(),
                    version: v,
                });
            }
            let bytes = storage.read(&rel, &format!("reading {}", path.display()))?;
            let manifest = read_manifest(&path, &bytes)?;
            if manifest.version != v {
                return Err(LakeError::Corrupt {
                    path,
                    message: format!(
                        "snapshot file declares version {}, expected {v}",
                        manifest.version
                    ),
                });
            }
            Ok((manifest, path))
        }
    }
}

/// The published snapshot versions at or below `up_to`, ascending (see
/// [`Lake::versions`] for exactly what counts as published). Free function so
/// attachments can list versions without a [`Lake`] handle.
pub(crate) fn published_versions(storage: &dyn Storage, up_to: u64) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for name in storage.list("snapshots")? {
        let Some(v) = name
            .strip_suffix(".json")
            .and_then(canonical_version_number)
        else {
            continue;
        };
        if v < 1 || v > up_to {
            continue;
        }
        let rel = format!("snapshots/{name}");
        let path = storage.abs(&rel);
        let bytes = match storage.read(&rel, &format!("reading {}", path.display())) {
            Ok(bytes) => bytes,
            Err(LakeError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(e) => return Err(e),
        };
        match read_manifest(&path, &bytes) {
            Ok(manifest) if manifest.version == v => out.push(v),
            _ => continue,
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// The AEAD binding of a manifest (`catalog.json` / `snapshots/<v>.json`)
/// and of the `highest.json` marker: the lake identity and the published
/// version, not the path (the catalog and its snapshot are byte-identical,
/// so they must share one binding). M-7.
pub(crate) fn manifest_binding(lake_id: &str, version: u64) -> String {
    format!("lake:{lake_id}:{version}")
}

/// Parse `lake:<32 hex>:<version>`; `None` for the legacy empty binding or
/// any other shape.
pub(crate) fn parse_manifest_binding(binding: &str) -> Option<(&str, u64)> {
    let rest = binding.strip_prefix("lake:")?;
    let (id, version) = rest.rsplit_once(':')?;
    if !is_lake_id(id) {
        return None;
    }
    Some((id, version.parse().ok()?))
}

/// A lake id is exactly 32 lowercase hex characters (128 bits).
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

#[derive(serde::Serialize, serde::Deserialize)]
struct LakeIdentity {
    lake_format: u32,
    lake_id: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct HighestVersion {
    lake_format: u32,
    lake_id: String,
    version: u64,
}

fn identity_json(lake_id: &str) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(&LakeIdentity {
        lake_format: LAKE_ID_FORMAT,
        lake_id: lake_id.to_string(),
    })
    .map_err(|e| LakeError::Internal {
        message: format!("serializing lake identity: {e}"),
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn highest_json(lake_id: &str, version: u64) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(&HighestVersion {
        lake_format: LAKE_ID_FORMAT,
        lake_id: lake_id.to_string(),
        version,
    })
    .map_err(|e| LakeError::Internal {
        message: format!("serializing highest.json: {e}"),
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Read `lake.json`, or `None` when this is a pre-v6 lake (the file did not
/// exist before M-7).
fn read_identity(storage: &dyn Storage, _root: &Path) -> Result<Option<String>> {
    match storage.head(LAKE_FILE)? {
        Head::Missing => Ok(None),
        Head::NotAFile => Err(LakeError::Corrupt {
            path: storage.abs(LAKE_FILE),
            message: "lake.json is not a regular file".to_string(),
        }),
        Head::File => {
            let path = storage.abs(LAKE_FILE);
            let bytes = storage.read(LAKE_FILE, &format!("reading {}", path.display()))?;
            let identity: LakeIdentity =
                serde_json::from_slice(&bytes).map_err(|e| LakeError::Corrupt {
                    path: path.clone(),
                    message: format!("lake.json does not parse: {e}"),
                })?;
            if identity.lake_format != LAKE_ID_FORMAT {
                return Err(LakeError::Unsupported {
                    path,
                    found: identity.lake_format,
                });
            }
            if !is_lake_id(&identity.lake_id) {
                return Err(LakeError::Corrupt {
                    path,
                    message: "lake.json carries an invalid lake_id (expected 32 lowercase hex \
                              characters)"
                        .to_string(),
                });
            }
            Ok(Some(identity.lake_id))
        }
    }
}

/// Read and validate `highest.json` for `lake_id`. A missing or unreadable
/// marker is a hard error for a v6 lake: accepting the catalog without it
/// would silently restore the rollback surface M-7 closes.
fn read_highest(storage: &dyn Storage, lake_id: &str) -> Result<u64> {
    let path = storage.abs(HIGHEST_FILE);
    match storage.head(HIGHEST_FILE)? {
        Head::Missing => {
            return Err(LakeError::Corrupt {
                path,
                message: "highest.json is missing (the authenticated rollback marker was \
                          removed); refusing to open"
                    .to_string(),
            });
        }
        Head::NotAFile => {
            return Err(LakeError::Corrupt {
                path,
                message: "highest.json is not a regular file".to_string(),
            });
        }
        Head::File => {}
    }
    let bytes = storage.read_highest(
        HIGHEST_FILE,
        &format!("reading {}", path.display()),
        lake_id,
    )?;
    let highest: HighestVersion =
        serde_json::from_slice(&bytes).map_err(|e| LakeError::Corrupt {
            path: path.clone(),
            message: format!("highest.json does not parse: {e}"),
        })?;
    if highest.lake_format != LAKE_ID_FORMAT {
        return Err(LakeError::Unsupported {
            path,
            found: highest.lake_format,
        });
    }
    if highest.lake_id != lake_id {
        return Err(LakeError::Corrupt {
            path: path.clone(),
            message: format!(
                "highest.json belongs to lake {}, not to this lake ({lake_id})",
                highest.lake_id
            ),
        });
    }
    if highest.version < 1 || highest.version > MAX_VERSION {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "highest.json version {} is outside the supported range 1..={MAX_VERSION}",
                highest.version
            ),
        });
    }
    Ok(highest.version)
}

/// Read and validate `catalog.json`.
fn read_catalog(storage: &dyn Storage, root: &Path) -> Result<Catalog> {
    let path = storage.abs(CATALOG_FILE);
    match storage.head(CATALOG_FILE)? {
        Head::Missing => {
            return Err(LakeError::NotALake {
                path: root.to_path_buf(),
            });
        }
        Head::NotAFile => {
            return Err(LakeError::Corrupt {
                path,
                message: "catalog.json is not a regular file".to_string(),
            });
        }
        Head::File => {}
    }
    let bytes = storage.read(CATALOG_FILE, &format!("reading {}", path.display()))?;
    read_manifest(&path, &bytes)
}

/// Parse + validate one manifest (`catalog.json` or a snapshot). The version
/// must be in `1..=MAX_VERSION`: `0` is not a version this writer ever
/// publishes, and a value at the top wraps `version + 1` — both are corrupt
/// metadata, reported as a clean [`LakeError::Corrupt`] (this path is plain
/// JSON, no Vortex decode involved).
///
/// `format_version` must be in [`MIN_FORMAT_VERSION`]`..=`[`FORMAT_VERSION`]`.
/// Per-version shape rules (all violations are [`LakeError::Corrupt`], never
/// a silent downgrade):
///
/// - **v1**: entries are `.pink` files as plain string paths; an entry must
///   not declare `"format": "vortex"`, may carry one entry-level `stats`
///   block, and must not carry `deletes`.
/// - **v2/v3**: entries are `.vortex` files as plain string paths and must
///   declare `"format": "vortex"`; no `stats`, no `deletes`.
/// - **v4**: entries must declare `"format"` (`"pink"` or `"vortex"`), list
///   their files as objects with exact row counts, and may carry positional
///   delete files. `stats` lives per `.pink` file (never on the entry, never
///   on a Vortex file), every delete must target one of the entry's files,
///   and the logical row count must equal `sum(files.rows) -
///   sum(deletes.count)`.
///
/// A build without `vortexlake` refuses a **v2/v3** manifest (all-Vortex by
/// definition) with a clear [`LakeError::FeatureMissing`] at this very first
/// parse, before any data file is touched. A v4–v6 entry whose format id has
/// no provider — `vortex` without `vortexlake`, or an unknown id — does
/// **not** refuse the lake: the catalog parses, and the per-table door
/// ([`Lake::unavailable_tables`], [`Lake::load`], attach and the write paths)
/// refuses only that table (wave 88, rule 5). Corrupt format/version
/// combinations still refuse the lake here.
fn read_manifest(path: &Path, bytes: &[u8]) -> Result<Catalog> {
    let manifest: Catalog = serde_json::from_slice(bytes).map_err(|e| LakeError::Corrupt {
        path: path.to_path_buf(),
        message: e.to_string(),
    })?;
    if manifest.format_version < MIN_FORMAT_VERSION || manifest.format_version > MAX_FORMAT_VERSION
    {
        return Err(LakeError::Unsupported {
            path: path.to_path_buf(),
            found: manifest.format_version,
        });
    }
    if manifest.version < 1 || manifest.version > MAX_VERSION {
        return Err(LakeError::Corrupt {
            path: path.to_path_buf(),
            message: format!(
                "version {} is outside the supported range 1..={MAX_VERSION}",
                manifest.version
            ),
        });
    }
    // M-7: v6 manifests carry the lake identity; older manifests never do.
    if manifest.format_version >= 6 {
        match manifest.lake_id.as_deref() {
            Some(id) if is_lake_id(id) => {}
            Some(id) => {
                return Err(LakeError::Corrupt {
                    path: path.to_path_buf(),
                    message: format!(
                        "format_version 6 manifest carries an invalid lake_id {id:?} \
                         (expected 32 lowercase hex characters)"
                    ),
                });
            }
            None => {
                return Err(LakeError::Corrupt {
                    path: path.to_path_buf(),
                    message: "format_version 6 manifest has no lake_id (the lake identity is \
                              mandatory from v6)"
                        .to_string(),
                });
            }
        }
    }
    // An older manifest may carry the field (a hand-edited or future-additive
    // fixture); only v6 requires it. It is ignored below v6, where the lake is
    // upgraded on its next save.
    let v = manifest.format_version;
    let corrupt = |message: String| LakeError::Corrupt {
        path: path.to_path_buf(),
        message,
    };
    // A build without `vortexlake` refuses a v2/v3 manifest (all-Vortex by
    // definition) at this very first parse, before any data file is touched,
    // with a feature error — never a corrupt-file one. A v4–v6 entry naming
    // an unavailable format stays a per-table matter (wave 88, rule 5), so it
    // is not refused here.
    #[cfg(not(feature = "vortexlake"))]
    if (2..=3).contains(&v) {
        return Err(LakeError::FeatureMissing {
            feature: "vortexlake",
            message: format!(
                "{} is a format_version {v} lake (Vortex data files); this build was built \
                 without Vortex format support — rebuild with the `vortexlake` feature \
                 (for the CLI: `cargo build -p pinkdb-cli --features vortexlake`)",
                path.display(),
            ),
        });
    }
    for (key, entry) in &manifest.tables {
        if v != 1 && entry.stats.is_some() {
            return Err(corrupt(format!(
                "table \"{key}\" declares an entry-level statistics block in a format_version \
                 {v} manifest (statistics blocks belong to v1 `.pink` entries; v4 `.pink` \
                 files carry their own per-file block, and Vortex statistics live in the \
                 data file's footer)"
            )));
        }
        if v < 4 && !entry.deletes.is_empty() {
            return Err(corrupt(format!(
                "table \"{key}\" declares positional delete files in a format_version {v} \
                 manifest (deletes are part of the v4 incremental format)"
            )));
        }
        if v < 4 && entry.partition_by.is_some() {
            return Err(corrupt(format!(
                "table \"{key}\" declares a partition specification in a format_version {v} \
                 manifest (partitioning is part of the v4 incremental format)"
            )));
        }
        if v < 4 && (!entry.inlined.is_empty() || entry.inlined_limit.is_some()) {
            return Err(corrupt(format!(
                "table \"{key}\" declares inlined rows in a format_version {v} manifest \
                 (inlining is part of the v4 incremental format)"
            )));
        }
        if !entry.inlined.is_empty() && entry.columns.is_none() {
            return Err(corrupt(format!(
                "table \"{key}\" carries inlined rows but no schema record (inlined rows are \
                 read through the v5 `columns`/`renames` mapping)"
            )));
        }
        if let Some(keys) = &entry.partition_by {
            if keys.is_empty() {
                return Err(corrupt(format!(
                    "table \"{key}\" declares an empty partition specification (a partitioned \
                     table lists at least one partition key)"
                )));
            }
            for spec_key in keys {
                if !spec_key.is_valid() {
                    return Err(corrupt(format!(
                        "table \"{key}\" declares partition key column \"{}\" with an unknown \
                         transform \"{}\" (this build understands \"identity\" and \
                         \"bucket(N)\")",
                        spec_key.column, spec_key.transform
                    )));
                }
            }
            for (i, spec_key) in keys.iter().enumerate() {
                if keys[..i]
                    .iter()
                    .any(|k| k.key_text() == spec_key.key_text())
                {
                    return Err(corrupt(format!(
                        "table \"{key}\" partitions on \"{}\" twice (every partition key must \
                         be unique)",
                        spec_key.key_text()
                    )));
                }
            }
        }
        match (v, entry.format.as_ref().map(FileFormat::id)) {
            (1, Some(crate::format::VORTEX_FORMAT_ID)) => {
                return Err(corrupt(format!(
                    "table \"{key}\" declares a vortex data file in a format_version 1 \
                     manifest (version 1 data files are engine `.pink` bytes)"
                )));
            }
            (1, Some(id)) if id != crate::format::PINK_FORMAT_ID => {
                return Err(corrupt(format!(
                    "table \"{key}\" declares data-file format \"{id}\" in a format_version 1 \
                     manifest (version 1 data files are engine `.pink` bytes)"
                )));
            }
            (2 | 3, None) => {
                return Err(corrupt(format!(
                    "table \"{key}\" does not declare a data file format (a format_version \
                     {v} entry requires `\"format\": \"vortex\"`)"
                )));
            }
            (2 | 3, Some(crate::format::PINK_FORMAT_ID)) => {
                return Err(corrupt(format!(
                    "table \"{key}\" declares a pink data file in a format_version {v} \
                     manifest (Vortex-era writers emit Vortex only; `.pink` data files are \
                     format_version 1)"
                )));
            }
            (2 | 3, Some(id)) if id != crate::format::VORTEX_FORMAT_ID => {
                return Err(corrupt(format!(
                    "table \"{key}\" declares data-file format \"{id}\" in a format_version {v} \
                     manifest (Vortex-era writers emit Vortex only)"
                )));
            }
            (4..=6, None) => {
                return Err(corrupt(format!(
                    "table \"{key}\" does not declare a data file format (a format_version {v} \
                     entry requires `\"format\"`: \"pink\" or \"vortex\")"
                )));
            }
            // A v4–v6 id with no provider is no longer a whole-lake error:
            // open/attach list the table, `Lake::load` skips it, and every
            // read or targeted write of it fails with the provider error
            // (wave 88, rule 5). The id is deliberately resolved later, per
            // table, so this build can still carry the entry forward.
            (4..=6, Some(_)) => {}
            _ => {}
        }
    }
    for (key, entry) in &manifest.tables {
        for file in &entry.files {
            match (v, file) {
                (1..=3, FileRef::Entry { .. }) => {
                    return Err(corrupt(format!(
                        "table \"{key}\" lists a data file as an object in a format_version \
                         {v} manifest (only v4 entries carry per-file row counts)"
                    )));
                }
                (4..=6, FileRef::Path(_)) => {
                    return Err(corrupt(format!(
                        "table \"{key}\" lists a data file without its row count (a \
                         format_version {v} entry lists files as {{\"path\", \"rows\"}} \
                         objects)"
                    )));
                }
                (
                    4..=6,
                    FileRef::Entry {
                        path: rel,
                        stats: _stats,
                        partition,
                        ..
                    },
                ) => {
                    validate_rel_path(rel)?;
                    // A statistics block may ride on a `.pink` file (its
                    // per-file statistics) or on a Vortex file (the
                    // catalog-published zone map, cross-checked against the
                    // footer at attach). Files written before wave 66 (Vortex
                    // without a block) stay valid, as do `.pink` files whose
                    // block was never published (attach refuses those with the
                    // clear "no statistics" message).
                    if let Some(partition) = partition {
                        for pkey in partition.keys() {
                            if crate::catalog::PartitionKey::parse(pkey).is_err() {
                                return Err(corrupt(format!(
                                    "table \"{key}\": data file \"{rel}\" carries a partition \
                                     key \"{pkey}\" that is not a column name or bucket(N, column)"
                                )));
                            }
                        }
                    }
                }
                (1..=3, FileRef::Path(rel)) => validate_rel_path(rel)?,
                _ => {}
            }
        }
        if v >= 4 {
            if entry.files.is_empty() && entry.inlined.is_empty() {
                return Err(corrupt(format!(
                    "table \"{key}\" has no data files (a format_version {v} table always lists \
                     at least one, unless it holds inlined rows)"
                )));
            }
            // Wave 68: the inlined row history, validated structurally. Row
            // ids are unique and below the counter; every record carries the
            // version pair a change-data read consumes; values are positional
            // against the row's own column list. The value encoding itself is
            // decoded lazily, where a failure names the exact row.
            let mut seen_rows: BTreeMap<u64, u64> = BTreeMap::new();
            for row in &entry.inlined {
                if row.values.len() != row.columns.len() {
                    return Err(corrupt(format!(
                        "table \"{key}\": inlined row {} holds {} values for {} columns",
                        row.row_id,
                        row.values.len(),
                        row.columns.len()
                    )));
                }
                if row.schema_version < 1 {
                    return Err(corrupt(format!(
                        "table \"{key}\": inlined row {} declares schema version 0 (a recorded \
                         row is written under a schema epoch of at least 1)",
                        row.row_id
                    )));
                }
                if row.begin < 1 || row.begin > manifest.version {
                    return Err(corrupt(format!(
                        "table \"{key}\": inlined row {} begins at version {}, outside \
                         1..={}",
                        row.row_id, row.begin, manifest.version
                    )));
                }
                if let Some(end) = row.end
                    && (end < row.begin || end > manifest.version)
                {
                    return Err(corrupt(format!(
                        "table \"{key}\": inlined row {} ends at version {end}, outside \
                             {}..={}",
                        row.row_id, row.begin, manifest.version
                    )));
                }
                if row.flushed && row.end.is_none() {
                    return Err(corrupt(format!(
                        "table \"{key}\": inlined row {} is marked flushed but has no end \
                         version (a flush writes the end that materialized it)",
                        row.row_id
                    )));
                }
                if let Some(begin) = seen_rows.get(&row.row_id)
                    && *begin == row.begin
                {
                    return Err(corrupt(format!(
                        "table \"{key}\": inlined row {} appears twice for version {}",
                        row.row_id, row.begin
                    )));
                }
                seen_rows.insert(row.row_id, row.begin);
            }
            if entry.next_row_id > 0 {
                for row in &entry.inlined {
                    if row.row_id >= entry.next_row_id {
                        return Err(corrupt(format!(
                            "table \"{key}\": inlined row id {} is at or beyond the table's \
                             next_row_id {}",
                            row.row_id, entry.next_row_id
                        )));
                    }
                }
            }
            let data_rows = entry.data_rows().ok_or_else(|| {
                corrupt(format!(
                    "table \"{key}\" overflows u64 rows across its data files"
                ))
            })?;
            let deleted = entry.deleted_rows().ok_or_else(|| {
                corrupt(format!(
                    "table \"{key}\" overflows u64 rows across its delete files"
                ))
            })?;
            let live_inlined = entry.live_inlined_rows();
            if data_rows < deleted {
                return Err(corrupt(format!(
                    "table \"{key}\" declares {} rows, but its files hold {data_rows} rows and \
                     its delete files drop {deleted} (the manifest disagrees with itself)",
                    entry.rows
                )));
            }
            let implied = (data_rows - deleted).checked_add(live_inlined);
            if implied != Some(entry.rows) {
                return Err(if live_inlined == 0 {
                    corrupt(format!(
                        "table \"{key}\" declares {} rows, but its files hold {data_rows} rows \
                         and its delete files drop {deleted} (the manifest disagrees with \
                         itself)",
                        entry.rows
                    ))
                } else {
                    corrupt(format!(
                        "table \"{key}\" declares {} rows, but its files hold {data_rows} rows, \
                         its delete files drop {deleted} and {live_inlined} inlined row{} live \
                         (the manifest disagrees with itself)",
                        entry.rows,
                        if live_inlined == 1 { "" } else { "s" }
                    ))
                });
            }
            for d in &entry.deletes {
                validate_rel_path(&d.path)?;
                if !entry.files.iter().any(|f| f.path() == d.file) {
                    return Err(corrupt(format!(
                        "table \"{key}\": delete file \"{}\" targets \"{}\", which is not one \
                         of the table's data files",
                        d.path, d.file
                    )));
                }
            }
        }
    }
    Ok(manifest)
}

/// Serialize a manifest (pretty JSON + trailing newline).
fn manifest_json(manifest: &Catalog, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(manifest).map_err(|e| LakeError::Internal {
        message: format!("serializing {}: {e}", path.display()),
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Serialize a commit delta manifest (pretty JSON + trailing newline).
fn commit_manifest_json(manifest: &catalog::CommitManifest) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(manifest).map_err(|e| LakeError::Internal {
        message: format!("serializing a commit manifest: {e}"),
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Canonical snapshot file stem: a decimal `u64` with no sign, whitespace,
/// leading `+`, or leading zero (`0` itself is never a version). `01.json`,
/// `1e3.json`, ` 1.json` and friends are not versions of this lake.
fn canonical_version_number(stem: &str) -> Option<u64> {
    let digits = !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit());
    let canonical = digits && (stem.len() == 1 || !stem.starts_with('0'));
    if !canonical {
        return None;
    }
    stem.parse().ok()
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
/// given dense table. Every build needs it: a `vortexlake` build still
/// appends `.pink` files to a pinklake lake's v4 entries.
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

/// An empty one-table [`PinkDb`]: `dbname.table` created with `schema`, the
/// default `main` of a fresh store dropped when the table lives elsewhere.
/// The Vortex decoder builds one per data file, so a decoded file has
/// exactly one database and table — the shape a v1 `.pink` data file opens
/// as, which is what the loader's checks expect.
#[cfg(feature = "vortexlake")]
fn empty_single_table_store(
    dbname: &str,
    table: &str,
    schema: Vec<(String, DataType)>,
) -> Result<PinkDb> {
    empty_structure(dbname, table, schema, "loading")
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
        let catalog = PathBuf::from("catalog.json");
        assert_eq!(split_key("main.t", &catalog).unwrap(), ("main", "t"));
        assert_eq!(
            split_key("analytics.a.b", &catalog).unwrap(),
            ("analytics", "a.b")
        );
        assert_eq!(split_key("main.", &catalog).unwrap(), ("main", ""));
        let err = split_key("nodot", &catalog).unwrap_err().to_string();
        assert!(err.contains("catalog.json"), "{err}");
        let snapshot = PathBuf::from("snapshots/2.json");
        let err = split_key("nodot", &snapshot).unwrap_err().to_string();
        assert!(err.contains("snapshots/2.json"), "{err}");
    }

    #[test]
    fn a_bad_file_partition_key_names_the_table_not_the_key() {
        let catalog = serde_json::json!({
            "format_version": 5,
            "version": 2,
            "updated": "2026-01-01T00:00:00.000Z",
            "tables": {
                "main.t": {
                    "format": "pink",
                    "rows": 1,
                    "files": [{
                        "path": "data/main.t/f.pink",
                        "rows": 1,
                        "partition": { "year(ts)": "x" }
                    }]
                }
            }
        });
        let bytes = serde_json::to_vec(&catalog).unwrap();
        let err = read_manifest(Path::new("catalog.json"), &bytes)
            .unwrap_err()
            .to_string();
        assert!(err.contains("table \"main.t\""), "{err}");
        assert!(err.contains("year(ts)"), "{err}");
    }

    #[test]
    fn canonical_version_numbers_reject_lookalikes() {
        assert_eq!(canonical_version_number("1"), Some(1));
        assert_eq!(canonical_version_number("42"), Some(42));
        assert_eq!(
            canonical_version_number("18446744073709551615"),
            Some(u64::MAX)
        );
        for bad in ["", "0x1", "01", "007", "+1", "-1", " 1", "1 ", "1e3", "١"] {
            assert_eq!(canonical_version_number(bad), None, "{bad:?}");
        }
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
