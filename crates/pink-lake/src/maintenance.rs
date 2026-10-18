//! Lake maintenance and introspection (wave 67), DuckLake parity:
//! `merge_adjacent_files`, `rewrite_data_files`, `expire_snapshots`,
//! `cleanup_old_files` / `delete_orphaned_files`, and the `.lake files` /
//! `table_info` / `options` / `settings` introspection.
//!
//! # Compaction: `merge_adjacent_files` / `rewrite_data_files`
//!
//! A lake accumulates small immutable data files (one per commit, or one per
//! partition value set per commit). Compaction **publishes one new version
//! whose tables reference consolidated files**: every candidate file is
//! decoded, its rows are written into a fresh immutable data file (one per
//! group), and the new manifest lists that file where its sources were. A
//! published file is **never mutated or replaced** — the old files stay on
//! disk byte-identical (the tests pin bytes and mtimes) for every earlier
//! version, exactly like DuckLake, whose compaction marks source files with
//! an `end_snapshot` and writes new Parquet files.
//!
//! The safety rules are DuckLake's (probed live against
//! `v2.0.0-alpha46057` + ducklake, and read in
//! `ducklake_compaction_functions.cpp` / `ducklake_metadata_manager.cpp`):
//!
//! - **merge** considers only data files that are live in the head version,
//!   have **no positional delete file** (DuckLake skips delete-bearing
//!   files: `files_processed` 3 of 4 in the probe; data inlining, wave 68,
//!   does not exist here yet) and that pass the `min_file_size` (`>=`) /
//!   `max_file_size` (`<`; DuckLake defaults the upper bound to its
//!   `target_file_size` option, pinkdb has no such option and defaults to
//!   unbounded) filters. Files group by **(partition value map, schema
//!   epoch)** and a group needs **at least two files** to merge (DuckLake's
//!   `DuckLakeCompactionGroup` keys on partition id + values with `Value`
//!   equality; the probe confirms the 2-file floor and the delete-file
//!   skip);
//! - **rewrite** considers only files with a delete file whose delete ratio
//!   (`deleted positions / file rows`) is `>= delete_threshold` (DuckLake's
//!   default 0.95, its `rewrite_delete_threshold`). Each candidate is
//!   materialized (deletes applied), so its delete file is dropped from the
//!   new manifest (probed: a 200-row file with 150 deleted rows is untouched
//!   at the default threshold and rewritten with `delete_threshold => 0`,
//!   `files_processed 1 / files_created 1`, the delete file gone);
//! - both group by the same key. DuckLake lets a group of older-schema files
//!   merge into the *latest* schema when every field is preserved; pinkdb is
//!   stricter and merges only files of the **same schema epoch**, re-encoding
//!   them under exactly that epoch — a renamed/added/dropped column since
//!   then never changes which physical layout the merged file has, and the
//!   v5 record maps it forward on read like any other file (see `schema`);
//! - `max_compacted_files` caps how many groups one call processes
//!   (DuckLake's `max_compacted_files`, default unlimited).
//!
//! Row counts and zone maps stay **exact**: a merged file's rows are the
//! summed survivors of its sources (checked), its statistics/zone map come
//! from the same writer path as any other data file (`write_data_file`), and
//! the manifest's `rows = sum(files) - sum(deletes)` invariant is re-checked
//! before publishing. Stats-fast attach and filter pruning work on the
//! merged file unchanged (`vortexlake` footers, `.pink` blocks, the
//! catalog-published zone map); prune tests pin that the surviving decode
//! counters still fire.
//!
//! # Snapshots: `expire_snapshots`
//!
//! Removes `snapshots/<v>.json` **and its `commits/<v>.json`** for versions
//! selected by a rule — an explicit `versions` list, an `older_than` cutoff,
//! and/or pinkdb's `retain_last` (keep the newest N) — never the current
//! version (DuckLake filters `snapshot_id != MAX(snapshot_id)`, and an
//! explicit `versions` entry naming the current snapshot was probed to be a
//! no-op). `versions` and `older_than` together are refused with DuckLake's
//! message; no criterion at all is a silent no-op, like DuckLake without an
//! `expire_older_than` default. `dry_run` reports the victims and deletes
//! nothing. Time travel simply shrinks: `.lake open <dir> <v>` and
//! `AT (VERSION => v)` for an expired `v` are the ordinary clean "no
//! snapshot version" error; `Lake::versions()` and `.lake snapshots` list
//! only retained versions. Expiration deletes **only** the two metadata
//! objects — data files are `cleanup_old_files`' job (DuckLake's expire
//! likewise only removes metadata; file deletion is scheduled there and
//! performed by its cleanup functions).
//!
//! # Objects: `cleanup_old_files` / `delete_orphaned_files`
//!
//! Deletes `data/` objects that **no retained version references** (data
//! files and delete files of the catalog and of every retained snapshot):
//! files unreferenced by a merge/rewrite, files of dropped tables whose
//! snapshots were expired, and leftovers of crashed writers (a `.tmp`
//! sibling is referenced by nothing once its writer died). DuckLake keeps a
//! `ducklake_files_scheduled_for_deletion` queue for the first class and
//! compares the data path against all metadata for the second; pinkdb stores
//! no such queue, so **both functions run the same object scan** — the names
//! exist for DuckLake parity, and this is stated rather than faked.
//!
//! Two guards, mirroring DuckLake's:
//!
//! - the **age gate**: only objects older than the cutoff are deleted
//!   (`older_than`, or the default `now - 2 days` — DuckLake's
//!   `delete_older_than` default `"2 days"`); `all` (DuckLake's
//!   `cleanup_all`) skips the gate. An object whose age cannot be read (an
//!   endpoint that sends no `Last-Modified`) is **never** deleted unless
//!   `all` is set;
//! - the **explicit confirmation**: `dry_run` previews, `confirm` deletes; a
//!   call that would delete anything with neither is refused with the count
//!   and both ways out. Nothing to delete is a clean empty report either
//!   way, and running twice is a no-op the second time.
//!
//! # Introspection
//!
//! [`Lake::list_files`] is DuckLake's `ducklake_list_files` (one row per
//! data file × its delete file, `None` when it has none) plus pinkdb's own
//! columns (rows, partition values, schema epoch, `referenced`) and the
//! `all` mode that appends every `data/` object no retained version
//! references — exactly the cleanup scan, so a preview and a dry run agree.
//! [`Lake::table_info`] is `ducklake_table_info` docked to this catalog
//! (names instead of ids/UUIDs, plus rows/schema epoch/partition spec), and
//! [`Lake::options`] / [`Lake::settings`] are `ducklake_options` /
//! `ducklake_settings` for the catalog facts this format has (format
//! version, data path, data format, encryption, current version).
//!
//! # Concurrency
//!
//! A compaction publishes exactly like a commit: candidate files first,
//! `commits/<v>.json`, `snapshots/<v>.json`, then the conditional catalog
//! replace — so a crash leaves only unreferenced leftovers. A conflicting
//! publish retries under [`CommitSettings`] (DuckLake's retry knobs),
//! **re-planning and re-writing** the merged files against the new head; the
//! superseded attempt's files are deleted best-effort before the retry (they
//! were never published, so nothing references them — a crash in that window
//! leaves them for `cleanup_old_files`). Snapshot expiration and object
//! cleanup first verify the catalog still declares this handle's version
//! (single-writer contract) and then delete; a concurrent publisher racing
//! one of them is refused up front, and neither operation changes the
//! current version or `catalog.json`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use pinkdb::DataTable;

use crate::catalog::{
    Catalog, CommitManifest, DeleteRef, FileFormat, FileRef, TableDelta, TableEntry,
};
use crate::incremental::CommitSettings;
use crate::local::CATALOG_FILE;
use crate::snapshot::SnapshotInfo;
use crate::{
    FORMAT_VERSION, Lake, LakeError, Result, apply_file_deletes, copy_chunks, manifest_for,
    next_schema_version, only_table, split_key, validate_rel_path,
};

/// The default object-cleanup retention: `2 days`, exactly DuckLake's
/// `delete_older_than` default (`"2 days"` — see
/// `ducklake_cleanup_files.cpp`'s `GetConfigOption` call). An unreferenced
/// object younger than this is protected from a cleanup that does not pass
/// `cleanup_all`; the window keeps a just-superseded file out of a racing
/// cleanup's way.
pub const DEFAULT_RETENTION_SECS: i64 = 2 * 86_400;

/// [`Lake::merge_adjacent_files`] options, mirroring DuckLake's
/// `ducklake_merge_adjacent_files` keyword arguments this build can honor.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MergeSettings {
    /// Skip files smaller than this many bytes (DuckLake: `min_file_size`;
    /// default: no lower bound).
    pub min_file_size: Option<u64>,
    /// Skip files at or above this many bytes (DuckLake: `max_file_size`,
    /// defaulting to its `target_file_size` catalog option; pinkdb has no
    /// target-file-size option, so the default is no upper bound).
    pub max_file_size: Option<u64>,
    /// Process at most this many compaction groups (DuckLake:
    /// `max_compacted_files`, default unlimited). One group writes one data
    /// file.
    pub max_compacted_files: Option<u64>,
}

/// [`Lake::rewrite_data_files`] options, mirroring DuckLake's
/// `ducklake_rewrite_data_files` keyword arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct RewriteSettings {
    /// Rewrite a delete-bearing file when `deleted rows / file rows` is at
    /// least this ratio (DuckLake: `delete_threshold`, default `0.95` from
    /// `rewrite_delete_threshold`). `0.0` rewrites every file with any
    /// delete (DuckLake's `total_delete_count > 0` guard still applies).
    pub delete_threshold: f64,
    /// Process at most this many compaction groups (one group writes one
    /// data file); DuckLake's `max_compacted_files`, default unlimited.
    pub max_compacted_files: Option<u64>,
}

impl Default for RewriteSettings {
    fn default() -> Self {
        Self {
            delete_threshold: 0.95,
            max_compacted_files: None,
        }
    }
}

/// One table's line of a [`CompactionReport`] — DuckLake's
/// `(schema_name, table_name, files_processed, files_created)` row, with the
/// flat `db.table` key in the table column (`schema_name` is always `main`
/// in a pinkdb lake: keys are flat).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCompaction {
    pub table: String,
    pub files_processed: u64,
    pub files_created: u64,
}

/// What one [`Lake::merge_adjacent_files`] / [`Lake::rewrite_data_files`]
/// published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    /// The published version; the unchanged head when nothing was compacted.
    pub version: u64,
    /// True when a version was published; false when no group was eligible.
    pub committed: bool,
    /// Conflicting publishes retried (0 = the first attempt won).
    pub retries: u32,
    /// One row per compacted table (empty when nothing was compacted).
    pub tables: Vec<TableCompaction>,
    /// Bytes written: new data files + commit manifest + snapshot + catalog.
    pub bytes_written: u64,
}

impl CompactionReport {
    /// Total source files consumed across tables (DuckLake's summed
    /// `files_processed`).
    pub fn files_processed(&self) -> u64 {
        self.tables.iter().map(|t| t.files_processed).sum()
    }

    /// Total data files written across tables (DuckLake's summed
    /// `files_created`).
    pub fn files_created(&self) -> u64 {
        self.tables.iter().map(|t| t.files_created).sum()
    }

    fn empty(version: u64) -> Self {
        Self {
            version,
            committed: false,
            retries: 0,
            tables: Vec::new(),
            bytes_written: 0,
        }
    }
}

/// [`Lake::expire_snapshots`] rule. `versions` and `older_than_micros`
/// together are refused (DuckLake: "cannot specify both 'versions' and
/// 'older_than'"); with none of the three criteria set the call is a silent
/// no-op (DuckLake without an `expire_older_than` default).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExpireSettings {
    /// Expire exactly these versions (DuckLake's `versions => [2,3]`). The
    /// current version is never expired even when listed.
    pub versions: Option<Vec<u64>>,
    /// Expire versions published strictly before this time (microseconds
    /// since the Unix epoch; DuckLake's `older_than`). A version whose
    /// publish time does not parse is never expired by this rule.
    pub older_than_micros: Option<i64>,
    /// pinkdb extension: always keep the newest N versions (the current one
    /// included), whatever the other rules select.
    pub retain_last: Option<u64>,
    /// Report the victims without deleting anything.
    pub dry_run: bool,
}

/// What one [`Lake::expire_snapshots`] call selected (and, unless
/// `dry_run`, deleted): the per-version records DuckLake's
/// `ducklake_expire_snapshots` returns, plus the bytes freed.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpireReport {
    pub versions: Vec<SnapshotInfo>,
    pub dry_run: bool,
    /// Bytes of the removed `snapshots/<v>.json` + `commits/<v>.json`
    /// objects (stored sizes, so ciphertext for an encrypted lake).
    pub bytes: u64,
}

impl ExpireReport {
    /// The expired (or, for a dry run, would-expire) version numbers.
    pub fn version_numbers(&self) -> Vec<u64> {
        self.versions.iter().map(|s| s.version).collect()
    }
}

/// [`Lake::cleanup_old_files`] / [`Lake::delete_orphaned_files`] options.
///
/// Deletion needs `confirm` (real deletion) or `dry_run` (preview); with
/// neither and a non-empty plan the call is refused. The age gate is
/// `older_than_micros` when set, else `now - DEFAULT_RETENTION_SECS`; `all`
/// skips it entirely (DuckLake's `cleanup_all`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CleanupSettings {
    /// Delete only objects last modified strictly before this time
    /// (microseconds since the Unix epoch; DuckLake's `older_than`).
    pub older_than_micros: Option<i64>,
    /// Ignore the age gate (DuckLake's `cleanup_all`).
    pub all: bool,
    /// Preview without deleting.
    pub dry_run: bool,
    /// Delete for real.
    pub confirm: bool,
}

/// What one cleanup call found (and, when confirmed, deleted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupReport {
    /// The unreferenced object paths, sorted (`data/…`).
    pub paths: Vec<String>,
    /// Their summed stored bytes (ciphertext for an encrypted lake).
    pub bytes: u64,
    pub dry_run: bool,
    /// True when the objects were actually deleted (`!dry_run` and the plan
    /// was non-empty).
    pub deleted: bool,
}

/// One row of [`Lake::list_files`]: a data file (or, with `all`, an object
/// on disk) and its pairing with one delete file, mirroring
/// `ducklake_list_files`'s data/delete columns docked to this catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct LakeFileInfo {
    /// The data file's lake-relative path (`data/<table>/<file>`).
    pub path: String,
    /// The `db.table` key the path belongs to (decoded from the path for an
    /// unreferenced object; `None` when the segment does not decode).
    pub table: Option<String>,
    /// The file's exact row count (absent for an unreferenced object and
    /// for a v1–v3 entry's path-only file).
    pub rows: Option<u64>,
    /// The stored object's size in bytes (ciphertext for an encrypted lake).
    pub size_bytes: Option<u64>,
    /// The file's recorded partition values (keyed by
    /// [`crate::catalog::PartitionKey`] text); `None` for an unpartitioned
    /// or unreferenced object.
    pub partition: Option<BTreeMap<String, Option<String>>>,
    /// The schema epoch the file was written under (v5 entries).
    pub schema_version: Option<u64>,
    /// The delete file paired with this data file, if any (one row per
    /// delete file of the data file).
    pub delete_file: Option<String>,
    /// The paired delete file's exact position count.
    pub delete_file_rows: Option<u64>,
    /// Whether a retained version references this object (for `all`, the
    /// listed manifest's own paths are `true` and the extras are `false`).
    pub referenced: bool,
}

/// One table's line of [`Lake::table_info`] — `ducklake_table_info` docked
/// to this catalog (no numeric ids/UUIDs; rows, schema epoch and the
/// partition spec added).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// Flat `db.table` key.
    pub table: String,
    /// Logical row count.
    pub rows: u64,
    /// Data files in the head version.
    pub file_count: u64,
    /// Summed stored bytes of those files (`None` when a size could not be
    /// read).
    pub file_size_bytes: Option<u64>,
    /// Delete files in the head version.
    pub delete_file_count: u64,
    /// Summed stored bytes of those delete files.
    pub delete_file_size_bytes: Option<u64>,
    /// The table's schema epoch (0 = a legacy entry without a record).
    pub schema_version: u64,
    /// The partition keys as their canonical texts (`region`,
    /// `bucket(4, id)`); empty when unpartitioned.
    pub partition_by: Vec<String>,
    /// Wave 68: live inlined row versions in the head version (rows stored in
    /// the manifest instead of a data file).
    pub inlined_rows: u64,
    /// Wave 68: the table's full inlined history — every live, ended and
    /// flushed record (the change-data log `.lake flush` keeps).
    pub inlined_history: u64,
    /// Wave 68: the table's persisted `data_inlining_row_limit` (`None` =
    /// the handle default applies).
    pub inlined_limit: Option<u64>,
}

/// One catalog fact, as [`Lake::options`] lists it (`ducklake_options`'s
/// `option_name` / `description` / `value` trio; no scope columns — pinkdb
/// has no per-scope settings store, these values are read-only facts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeOption {
    pub name: String,
    pub description: String,
    pub value: String,
}

/// [`Lake::settings`]: DuckLake's `ducklake_settings` trio
/// (`catalog_type`, `extension_version`, `data_path`) docked to pinkdb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSettings {
    /// Where the catalog metadata lives: `"json"` (pinkdb's `catalog.json` +
    /// snapshot files; DuckLake reports its metadata backend, `duckdb`).
    pub catalog_type: String,
    /// The pink-lake crate version.
    pub extension_version: String,
    /// The lake location (a directory or an `s3://bucket/prefix` URL).
    pub data_path: String,
}

/// "Now" in microseconds since the Unix epoch (the cutoffs' unit).
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// One compaction candidate file (merge or rewrite).
#[derive(Debug, Clone)]
struct Candidate {
    /// Index in the entry's `files` list.
    index: usize,
    rel: String,
    rows: u64,
    /// The positions this file's delete files drop (validated by
    /// `entry_layout`).
    deleted: Vec<u64>,
    /// The file's recorded partition values (grouping key).
    partition: Option<BTreeMap<String, Option<String>>>,
    /// The schema epoch the file was written under (grouping key).
    epoch: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionKind {
    Merge,
    Rewrite,
}

/// The grouping key of a compaction: files merge only within one
/// **(partition value map, schema epoch)** pair (DuckLake's
/// `DuckLakeCompactionGroup` keys on partition id + values; pinkdb adds the
/// same-epoch rule — see the module docs).
type GroupKey = (Option<BTreeMap<String, Option<String>>>, Option<u64>);

/// The settings both compaction kinds read, flattened by the public entry
/// points.
#[derive(Debug, Clone, Copy)]
struct PlanOptions {
    min_file_size: Option<u64>,
    max_file_size: Option<u64>,
    max_compacted_files: Option<u64>,
    delete_threshold: f64,
}

/// One table's planned maintenance: the entry the new version publishes and
/// what the report/delta manifests say about it.
#[derive(Debug)]
struct PlannedTable {
    key: String,
    entry: TableEntry,
    /// The files this call wrote (the delta manifest's `data_files`).
    new_files: Vec<FileRef>,
    processed: u64,
    created: u64,
    compacted_files: Vec<String>,
    removed_delete_files: Vec<String>,
}

impl Lake {
    /// Merge adjacent data files, DuckLake's `ducklake_merge_adjacent_files`.
    /// `table = None` scans every table; else only that flat `db.table` key
    /// (a missing table, or a legacy v1–v3 entry, is a clean error). One
    /// version is published when at least one group is eligible, listing a
    /// consolidated file where each group's files were; every source file
    /// stays on disk byte-identical. See the module docs for the exact
    /// eligibility rules.
    pub fn merge_adjacent_files(
        &mut self,
        table: Option<&str>,
        settings: &MergeSettings,
    ) -> Result<CompactionReport> {
        if settings.min_file_size.is_some()
            && settings.max_file_size.is_some()
            && settings.min_file_size >= settings.max_file_size
        {
            return Err(LakeError::InvalidOption {
                message: "The min_file_size must be less than max_file_size.".to_string(),
            });
        }
        let plan = PlanOptions {
            min_file_size: settings.min_file_size,
            max_file_size: settings.max_file_size,
            max_compacted_files: settings.max_compacted_files,
            delete_threshold: 0.0,
        };
        self.compact(table, CompactionKind::Merge, plan)
    }

    /// Rewrite the files whose delete files drop at least `delete_threshold`
    /// of their rows, materializing the deletes (DuckLake's
    /// `ducklake_rewrite_data_files`). One version is published when at
    /// least one file is eligible; the rewritten files' delete files are
    /// dropped from the new manifest (their positions are baked into the new
    /// files), and the source files stay on disk for older versions.
    pub fn rewrite_data_files(
        &mut self,
        table: Option<&str>,
        settings: &RewriteSettings,
    ) -> Result<CompactionReport> {
        if !(0.0..=1.0).contains(&settings.delete_threshold) {
            return Err(LakeError::InvalidOption {
                message: "The delete_threshold option must be between 0 and 1".to_string(),
            });
        }
        let plan = PlanOptions {
            min_file_size: None,
            max_file_size: None,
            max_compacted_files: settings.max_compacted_files,
            delete_threshold: settings.delete_threshold,
        };
        self.compact(table, CompactionKind::Rewrite, plan)
    }

    /// Delete one object, ignoring "it is already gone" (S3 DELETE is
    /// idempotent; locally a missing file is a `NotFound` I/O error).
    fn delete_if_present(&self, rel: &str) -> Result<()> {
        match self.storage.delete(rel) {
            Ok(()) => Ok(()),
            Err(LakeError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Best-effort removal of an attempt's unpublished merged files (a
    /// retry or a failure before publication). Nothing ever references
    /// them; a crash here leaves them for `cleanup_old_files`.
    fn discard_unpublished(&self, written: &[String]) {
        for rel in written {
            let _ = self.storage.delete(rel);
        }
    }

    /// The shared compaction engine: plan against the head, write the
    /// consolidated files, publish one version, retry conflicts by
    /// re-planning (discarding the superseded attempt's files).
    fn compact(
        &mut self,
        table: Option<&str>,
        kind: CompactionKind,
        options: PlanOptions,
    ) -> Result<CompactionReport> {
        if let Some(table) = table {
            split_key(table, &self.root)?;
        }
        let retry = CommitSettings::default();
        let mut attempt: u32 = 0;
        loop {
            if attempt > 0 {
                std::thread::sleep(retry.backoff_for(attempt));
                self.rebase_catalog()?;
            }
            let head = self.read_catalog_for_commit()?;
            let version = head
                .version
                .checked_add(1)
                .filter(|v| *v <= crate::MAX_VERSION)
                .ok_or_else(|| LakeError::Overflow {
                    path: self.root.clone(),
                    version: head.version,
                })?;
            // Unpublished files this attempt wrote: deleted best-effort
            // before a retry or on failure — nothing ever references them.
            let mut written: Vec<String> = Vec::new();
            let (planned, files_bytes) =
                match self.plan_compaction(&head, table, kind, &options, &mut written) {
                    Ok(planned) => planned,
                    Err(e) => {
                        self.discard_unpublished(&written);
                        return Err(e);
                    }
                };
            if planned.is_empty() {
                self.discard_unpublished(&written);
                return Ok(CompactionReport::empty(head.version));
            }
            let mut tables = head.tables.clone();
            let mut deltas: BTreeMap<String, TableDelta> = BTreeMap::new();
            let mut report_tables = Vec::with_capacity(planned.len());
            for planned in planned {
                deltas.insert(
                    planned.key.clone(),
                    TableDelta {
                        data_files: planned.new_files.clone(),
                        delete_files: Vec::new(),
                        rows_added: 0,
                        rows_deleted: 0,
                        replaced: false,
                        partition_changed: false,
                        partition_by: None,
                        compacted_files: planned.compacted_files.clone(),
                        removed_delete_files: planned.removed_delete_files.clone(),
                        rows_inlined: 0,
                        inlined_ended: 0,
                        inlined_flushed: 0,
                        inlined_limit: None,
                        inlining_changed: false,
                    },
                );
                report_tables.push(TableCompaction {
                    table: planned.key.clone(),
                    files_processed: planned.processed,
                    files_created: planned.created,
                });
                tables.insert(planned.key, planned.entry);
            }
            let (author, commit_message) = match &self.pending_commit {
                Some((author, message)) => (author.clone(), message.clone()),
                None => (None, None),
            };
            let changes = manifest_for(&*self.storage, &self.root, Some(head.version))
                .ok()
                .map(|(previous, _)| {
                    crate::snapshot::ChangeSummary::diff(&previous.tables, &tables)
                });
            let schema_version = next_schema_version(Some(&head), &tables);
            let new_catalog = Catalog {
                format_version: FORMAT_VERSION,
                updated: crate::time::iso8601_utc(SystemTime::now()),
                version,
                tables,
                author,
                commit_message,
                changes,
                schema_version,
            };
            let snapshot_plain = crate::manifest_json(&new_catalog, &self.root)?;
            let commit_manifest = CommitManifest {
                format_version: FORMAT_VERSION,
                version,
                base_version: head.version,
                updated: crate::time::iso8601_utc(SystemTime::now()),
                tables: deltas,
            };
            let commit_bytes = crate::commit_manifest_json(&commit_manifest)?;
            let report = CompactionReport {
                version,
                committed: true,
                retries: attempt,
                tables: report_tables,
                bytes_written: files_bytes
                    + commit_bytes.len() as u64
                    + 2 * snapshot_plain.len() as u64,
            };
            match self.publish_commit_version(version, &commit_bytes, &snapshot_plain) {
                Ok(()) => {
                    self.pending_commit = None;
                    return Ok(report);
                }
                Err(LakeError::Conflict { .. }) if attempt < retry.max_retry_count => {
                    self.discard_unpublished(&written);
                    attempt += 1;
                }
                Err(e) => {
                    self.discard_unpublished(&written);
                    return Err(e);
                }
            }
        }
    }

    /// The planning half of a compaction: which tables change, their new
    /// entries, and the consolidated files (written as soon as a group is
    /// eligible, so `written` must be discarded on any later failure or
    /// retry). Returns the plans and the bytes written.
    fn plan_compaction(
        &self,
        head: &Catalog,
        table: Option<&str>,
        kind: CompactionKind,
        options: &PlanOptions,
        written: &mut Vec<String>,
    ) -> Result<(Vec<PlannedTable>, u64)> {
        let manifest_path = self.storage.abs(CATALOG_FILE);
        if let Some(selected) = table {
            // DuckLake resolves the named table up front and raises when it
            // does not exist; do the same (never a silent empty plan).
            if !head.tables.contains_key(selected) {
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "table \"{selected}\" does not exist in version {}",
                        head.version
                    ),
                });
            }
        }
        let mut planned = Vec::new();
        let mut bytes_written = 0u64;
        for (key, entry) in &head.tables {
            if let Some(selected) = table
                && key != selected
            {
                continue;
            }
            // Only v4/v5 entries: a legacy v1–v3 entry lists one data file
            // without per-file counts, so there is nothing to consolidate
            // (an explicit request is a clean error, a scan skips it).
            if entry.data_rows().is_none() {
                if table.is_some() {
                    return Err(LakeError::NotRepresentable {
                        message: format!(
                            "table \"{key}\" is a format_version < 4 entry (one data file, no \
                             per-file row counts); only v4/v5 incremental entries can be \
                             compacted — re-save the lake to rewrite it as a current entry"
                        ),
                    });
                }
                continue;
            }
            let layout =
                crate::incremental::entry_layout(&*self.storage, entry, key, &manifest_path)?;
            let mut candidates: Vec<Candidate> = Vec::new();
            for (i, (rel, rows, deleted)) in layout.iter().enumerate() {
                let file = &entry.files[i];
                match kind {
                    CompactionKind::Merge => {
                        // DuckLake: "Merge Adjacent Tables doesn't perform
                        // the merge if any deletes are present".
                        if !deleted.is_empty() {
                            continue;
                        }
                        let size = self.storage.stat(rel)?.map(|info| info.size);
                        if options
                            .min_file_size
                            .is_some_and(|min| !size.is_some_and(|s| s >= min))
                        {
                            continue;
                        }
                        if options
                            .max_file_size
                            .is_some_and(|max| size.is_some_and(|s| s >= max))
                        {
                            continue;
                        }
                    }
                    CompactionKind::Rewrite => {
                        // DuckLake: candidates are the files with active
                        // deletes whose delete ratio reaches the threshold.
                        if deleted.is_empty() || *rows == 0 {
                            continue;
                        }
                        let ratio = deleted.len() as f64 / *rows as f64;
                        if ratio < options.delete_threshold {
                            continue;
                        }
                    }
                }
                candidates.push(Candidate {
                    index: i,
                    rel: rel.clone(),
                    rows: *rows,
                    deleted: deleted.clone(),
                    partition: file.partition().cloned(),
                    epoch: file.schema_version(),
                });
            }
            if candidates.is_empty() {
                continue;
            }
            // Group by (partition values, schema epoch); deterministic order
            // by the group's first file index.
            let mut groups: BTreeMap<GroupKey, Vec<Candidate>> = BTreeMap::new();
            for candidate in candidates {
                groups
                    .entry((candidate.partition.clone(), candidate.epoch))
                    .or_default()
                    .push(candidate);
            }
            let mut group_list: Vec<Vec<Candidate>> = groups.into_values().collect();
            group_list.sort_by_key(|group| group.iter().map(|c| c.index).min().unwrap_or(0));

            let table_name = split_key(key, &manifest_path)?.1.to_string();
            let mut processed = 0u64;
            let mut created = 0u64;
            let mut groups_done = 0u64;
            let mut compacted_files: Vec<String> = Vec::new();
            let mut removed_delete_files: BTreeSet<String> = BTreeSet::new();
            let mut merged_at: BTreeMap<usize, FileRef> = BTreeMap::new();
            let mut member: BTreeSet<usize> = BTreeSet::new();
            for group in group_list {
                if options
                    .max_compacted_files
                    .is_some_and(|cap| groups_done >= cap)
                {
                    break;
                }
                // A merge needs at least two files (DuckLake skips the
                // single-file group); a rewrite materializes one file too.
                if kind == CompactionKind::Merge && group.len() < 2 {
                    continue;
                }
                let mut merged: Option<DataTable> = None;
                let mut source_rows = 0u64;
                for candidate in &group {
                    let physical = self.decode_physical_file(
                        key,
                        entry.format(),
                        &candidate.rel,
                        candidate.rows,
                        &candidate.deleted,
                    )?;
                    source_rows += candidate.rows - candidate.deleted.len() as u64;
                    match &mut merged {
                        None => {
                            let mut table = DataTable::new(table_name.clone(), physical.schema())
                                .map_err(|e| LakeError::Engine {
                                context: format!(
                                    "preparing the merged rows of \"{key}\" (from \
                                         \"{}\")",
                                    candidate.rel
                                ),
                                source: e,
                            })?;
                            copy_chunks(&physical, &mut table, &candidate.rel)?;
                            merged = Some(table);
                        }
                        Some(table) => {
                            if table.schema() != physical.schema() {
                                return Err(LakeError::Corrupt {
                                    path: self.storage.abs(&candidate.rel),
                                    message: format!(
                                        "table \"{key}\": data file \"{}\" has a different \
                                         physical schema than the file it would merge with; \
                                         refusing to merge them",
                                        candidate.rel
                                    ),
                                });
                            }
                            copy_chunks(&physical, table, &candidate.rel)?;
                        }
                    }
                }
                let merged = merged.expect("a processed group has at least one file");
                if merged.num_rows() as u64 != source_rows {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.clone(),
                        message: format!(
                            "table \"{key}\": the merge sources hold {source_rows} rows, but \
                             the merged table has {}",
                            merged.num_rows()
                        ),
                    });
                }
                let (file, bytes) = self.write_data_file(key, Some(entry.format()), &merged)?;
                bytes_written += bytes;
                written.push(file.path().to_string());
                let mut file = file.with_schema_version(group[0].epoch);
                if let Some(partition) = &group[0].partition {
                    file = file.with_partition(partition.clone());
                }
                merged_at.insert(group[0].index, file);
                for candidate in &group {
                    member.insert(candidate.index);
                    compacted_files.push(candidate.rel.clone());
                }
                for delete in &entry.deletes {
                    if group.iter().any(|c| c.rel == delete.file) {
                        removed_delete_files.insert(delete.path.clone());
                    }
                }
                processed += group.len() as u64;
                created += 1;
                groups_done += 1;
            }
            if merged_at.is_empty() {
                continue;
            }
            // The new file list: the merged file where each group started,
            // every other file (and every file of an unprocessed group)
            // unchanged in place.
            let mut new_files: Vec<FileRef> = Vec::with_capacity(entry.files.len());
            let mut new_refs: Vec<FileRef> = Vec::with_capacity(merged_at.len());
            for (i, file) in entry.files.iter().enumerate() {
                if let Some(merged) = merged_at.get(&i) {
                    new_files.push(merged.clone());
                    new_refs.push(merged.clone());
                } else if !member.contains(&i) {
                    new_files.push(file.clone());
                }
            }
            let removed: Vec<String> = removed_delete_files.into_iter().collect();
            let new_deletes: Vec<DeleteRef> = entry
                .deletes
                .iter()
                .filter(|d| !removed.contains(&d.path))
                .cloned()
                .collect();
            let mut new_entry = entry.clone();
            new_entry.files = new_files;
            new_entry.deletes = new_deletes;
            // The manifest's own rows invariant must survive compaction; the
            // live inlined rows are untouched by a merge or rewrite and stay
            // part of the logical count (wave 68).
            let data_rows = new_entry.data_rows().ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!("table \"{key}\" overflows u64 rows across its data files"),
            })?;
            let deleted_rows = new_entry.deleted_rows().ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!("table \"{key}\" overflows u64 rows across its delete files"),
            })?;
            let live_inlined = new_entry.live_inlined_rows();
            if data_rows < deleted_rows || data_rows - deleted_rows + live_inlined != entry.rows {
                return Err(LakeError::Corrupt {
                    path: manifest_path.clone(),
                    message: format!(
                        "table \"{key}\": compaction would leave {data_rows} data rows minus \
                         {deleted_rows} deleted positions plus {live_inlined} live inlined \
                         row(s), but the table declares {} rows; refusing to publish",
                        entry.rows
                    ),
                });
            }
            planned.push(PlannedTable {
                key: key.clone(),
                entry: new_entry,
                new_files: new_refs,
                processed,
                created,
                compacted_files,
                removed_delete_files: removed,
            });
        }
        Ok((planned, bytes_written))
    }

    /// Decode one data file for compaction: its physical table as written
    /// (same-epoch groups are re-encoded under that epoch, so no schema
    /// projection happens here), with the file's positional deletes applied
    /// for a rewrite. Identity and row count are checked exactly like the
    /// loader's.
    fn decode_physical_file(
        &self,
        key: &str,
        format: FileFormat,
        rel: &str,
        rows: u64,
        deleted: &[u64],
    ) -> Result<DataTable> {
        let (dbname, table) = split_key(key, &self.root.join(CATALOG_FILE))?;
        let mini = self.open_data_file(rel, format)?;
        let (file_db, file_table) = only_table(&mini, rel, &self.root)?;
        if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(table) {
            return Err(LakeError::Corrupt {
                path: self.storage.abs(rel),
                message: format!(
                    "data file holds {file_db}.{}, but the catalog says \"{key}\"",
                    file_table.name
                ),
            });
        }
        if file_table.num_rows() as u64 != rows {
            return Err(LakeError::Corrupt {
                path: self.storage.abs(rel),
                message: format!(
                    "data file has {} rows, but the catalog says {rows}",
                    file_table.num_rows()
                ),
            });
        }
        apply_file_deletes(file_table, deleted, key, rel)
    }

    /// Expire old snapshot manifests (and their commit deltas), DuckLake's
    /// `ducklake_expire_snapshots`. Deletes `snapshots/<v>.json` and
    /// `commits/<v>.json`; the current version is never touched, and time
    /// travel to an expired version becomes the ordinary clean
    /// `VersionNotFound`. See [`ExpireSettings`] for the selection rules.
    pub fn expire_snapshots(&mut self, settings: &ExpireSettings) -> Result<ExpireReport> {
        if settings.versions.is_some() && settings.older_than_micros.is_some() {
            return Err(LakeError::InvalidOption {
                message: "ducklake_expire_snapshots: cannot specify both 'versions' and \
                          'older_than' parameters at the same time. Please use only one \
                          criterion."
                    .to_string(),
            });
        }
        // An explicitly empty version set expires nothing (DuckLake).
        if settings.versions.as_ref().is_some_and(|v| v.is_empty())
            && settings.older_than_micros.is_none()
            && settings.retain_last.is_none()
        {
            return Ok(ExpireReport {
                versions: Vec::new(),
                dry_run: settings.dry_run,
                bytes: 0,
            });
        }
        // Single-writer guard: never expire while another writer owns the
        // catalog (this handle's version is the head it reasons about).
        self.ensure_catalog_unchanged()?;
        let current = self.version;
        let records = self.snapshots()?;
        let mut victims = Vec::new();
        for info in records {
            // Never the current version, whatever the rule says (DuckLake
            // filters `snapshot_id != MAX(snapshot_id)`).
            if info.version == current {
                continue;
            }
            // `retain_last` protects the newest N versions unconditionally.
            if settings
                .retain_last
                .is_some_and(|n| info.version.saturating_add(n) > current)
            {
                continue;
            }
            let selected = match (&settings.versions, settings.older_than_micros) {
                (Some(_), _) => settings
                    .versions
                    .as_ref()
                    .is_some_and(|versions| versions.contains(&info.version)),
                (None, Some(cutoff)) => info.time_micros.is_some_and(|time| time < cutoff),
                // `retain_last` alone expires everything it does not protect.
                (None, None) => settings.retain_last.is_some(),
            };
            if selected {
                victims.push(info);
            }
        }
        let mut bytes = 0u64;
        for info in &victims {
            let snapshot_rel = format!("snapshots/{}.json", info.version);
            let commit_rel = format!("commits/{}.json", info.version);
            bytes += self
                .storage
                .stat(&snapshot_rel)?
                .map(|i| i.size)
                .unwrap_or(0);
            bytes += self.storage.stat(&commit_rel)?.map(|i| i.size).unwrap_or(0);
        }
        if !settings.dry_run {
            for info in &victims {
                let snapshot_rel = format!("snapshots/{}.json", info.version);
                let commit_rel = format!("commits/{}.json", info.version);
                // Delta first, then the snapshot: a crash in between leaves
                // a snapshot without its audit delta (harmless — the
                // snapshot stays authoritative), never a delta pretending a
                // version exists its snapshot cannot load.
                self.delete_if_present(&commit_rel)?;
                self.delete_if_present(&snapshot_rel)?;
            }
        }
        Ok(ExpireReport {
            versions: victims,
            dry_run: settings.dry_run,
            bytes,
        })
    }

    /// Delete `data/` objects no retained version references, DuckLake's
    /// `ducklake_cleanup_old_files`. See [`CleanupSettings`] for the age
    /// gate and the confirmation rules; [`Lake::delete_orphaned_files`] runs
    /// the same scan (pinkdb has no scheduled-deletion queue; the module
    /// docs explain).
    pub fn cleanup_old_files(&mut self, settings: &CleanupSettings) -> Result<CleanupReport> {
        self.cleanup(settings)
    }

    /// DuckLake's `ducklake_delete_orphaned_files`: the same
    /// unreferenced-object scan as [`Lake::cleanup_old_files`] (pinkdb
    /// stores no deletion queue, so the two are one rule here — the module
    /// docs are explicit about it).
    pub fn delete_orphaned_files(&mut self, settings: &CleanupSettings) -> Result<CleanupReport> {
        self.cleanup(settings)
    }

    fn cleanup(&mut self, settings: &CleanupSettings) -> Result<CleanupReport> {
        // Single-writer guard before anything is deleted.
        self.ensure_catalog_unchanged()?;
        let referenced = self.referenced_paths()?;
        let cutoff = settings
            .older_than_micros
            .unwrap_or_else(|| now_micros() - DEFAULT_RETENTION_SECS * 1_000_000);
        let mut found: Vec<(String, u64)> = Vec::new();
        for rel in self.storage.list_recursive("data")? {
            let full = format!("data/{rel}");
            if validate_rel_path(&full).is_err() || referenced.contains(&full) {
                continue;
            }
            let info = self.storage.stat(&full)?;
            if !settings.all {
                // No readable age: protected (never deleted without `all`).
                let Some(modified) = info.and_then(|i| i.modified_micros) else {
                    continue;
                };
                if modified >= cutoff {
                    continue;
                }
            }
            found.push((full, info.map(|i| i.size).unwrap_or(0)));
        }
        found.sort();
        found.dedup();
        let paths: Vec<String> = found.iter().map(|(path, _)| path.clone()).collect();
        let bytes: u64 = found.iter().map(|(_, size)| *size).sum();
        if settings.dry_run || paths.is_empty() {
            return Ok(CleanupReport {
                paths,
                bytes,
                dry_run: settings.dry_run,
                deleted: false,
            });
        }
        if !settings.confirm {
            return Err(LakeError::NotRepresentable {
                message: format!(
                    "refusing to delete {} unreferenced lake object(s) without confirmation: \
                     run with dry_run to preview them, or confirm to delete them (the objects \
                     are older than the {} retention cutoff; pass all to skip the age gate)",
                    paths.len(),
                    if settings.older_than_micros.is_some() {
                        "given"
                    } else {
                        "default 2-day"
                    }
                ),
            });
        }
        for path in &paths {
            self.delete_if_present(path)?;
        }
        Ok(CleanupReport {
            paths,
            bytes,
            dry_run: false,
            deleted: true,
        })
    }

    /// Every `data/` path any retained version references: the catalog plus
    /// every retained snapshot (data files and delete files). Shared by
    /// cleanup and `list_files(all)`.
    fn referenced_paths(&self) -> Result<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        let mut collect = |manifest: &Catalog| {
            for entry in manifest.tables.values() {
                for file in &entry.files {
                    out.insert(file.path().to_string());
                }
                for delete in &entry.deletes {
                    out.insert(delete.path.clone());
                }
            }
        };
        let (catalog, _) = self.manifest(None)?;
        collect(&catalog);
        for version in self.versions()? {
            if let Ok((manifest, _)) = manifest_for(&*self.storage, &self.root, Some(version)) {
                collect(&manifest);
            }
        }
        Ok(out)
    }

    /// List one version's data files and their delete-file pairings
    /// (DuckLake's `ducklake_list_files`), or — with `all` — additionally
    /// every `data/` object **no retained version references**, which is
    /// exactly the cleanup scan. `version = None` lists the current catalog;
    /// `table = None` lists every table (a bare table name matches the flat
    /// key's table part too).
    pub fn list_files(
        &self,
        table: Option<&str>,
        version: Option<u64>,
        all: bool,
    ) -> Result<Vec<LakeFileInfo>> {
        let (manifest, _) = self.manifest(version)?;
        let matches = |key: &str| match table {
            None => true,
            Some(selected) => {
                key == selected
                    || key
                        .split_once('.')
                        .is_some_and(|(_, table_part)| table_part == selected)
            }
        };
        if let Some(selected) = table
            && !manifest.tables.keys().any(|key| matches(key))
        {
            return Err(LakeError::NotRepresentable {
                message: format!(
                    "table \"{selected}\" does not exist in the listed version ({})",
                    manifest.version
                ),
            });
        }
        let mut out = Vec::new();
        for (key, entry) in &manifest.tables {
            if !matches(key) {
                continue;
            }
            for file in &entry.files {
                let size = self.storage.stat(file.path())?.map(|i| i.size);
                let deletes: Vec<&DeleteRef> = entry
                    .deletes
                    .iter()
                    .filter(|d| d.file == file.path())
                    .collect();
                if deletes.is_empty() {
                    out.push(LakeFileInfo {
                        path: file.path().to_string(),
                        table: Some(key.clone()),
                        rows: file.rows(),
                        size_bytes: size,
                        partition: file.partition().cloned(),
                        schema_version: file.schema_version(),
                        delete_file: None,
                        delete_file_rows: None,
                        referenced: true,
                    });
                } else {
                    for delete in deletes {
                        out.push(LakeFileInfo {
                            path: file.path().to_string(),
                            table: Some(key.clone()),
                            rows: file.rows(),
                            size_bytes: size,
                            partition: file.partition().cloned(),
                            schema_version: file.schema_version(),
                            delete_file: Some(delete.path.clone()),
                            delete_file_rows: Some(delete.count),
                            referenced: true,
                        });
                    }
                }
            }
        }
        if all {
            let referenced = self.referenced_paths()?;
            for rel in self.storage.list_recursive("data")? {
                let full = format!("data/{rel}");
                if referenced.contains(&full) || out.iter().any(|info| info.path == full) {
                    continue;
                }
                let table = rel
                    .split_once('/')
                    .and_then(|(segment, _)| crate::decode_key(segment));
                let size = self.storage.stat(&full)?.map(|i| i.size);
                out.push(LakeFileInfo {
                    path: full,
                    table,
                    rows: None,
                    size_bytes: size,
                    partition: None,
                    schema_version: None,
                    delete_file: None,
                    delete_file_rows: None,
                    referenced: false,
                });
            }
        }
        Ok(out)
    }

    /// Per-table counts and sizes of the current version
    /// (`ducklake_table_info` docked to this catalog). Sizes come from the
    /// stored objects (`Storage::stat`), so they are ciphertext sizes for an
    /// encrypted lake.
    pub fn table_info(&self) -> Result<Vec<TableInfo>> {
        let (manifest, _) = self.manifest(None)?;
        let mut out = Vec::with_capacity(manifest.tables.len());
        for (key, entry) in &manifest.tables {
            let mut file_size: Option<u64> = Some(0);
            for file in &entry.files {
                file_size = add_opt(file_size, self.storage.stat(file.path())?.map(|i| i.size));
            }
            let mut delete_size: Option<u64> = Some(0);
            for delete in &entry.deletes {
                delete_size = add_opt(
                    delete_size,
                    self.storage.stat(&delete.path)?.map(|i| i.size),
                );
            }
            out.push(TableInfo {
                table: key.clone(),
                rows: entry.rows,
                file_count: entry.files.len() as u64,
                file_size_bytes: file_size,
                delete_file_count: entry.deletes.len() as u64,
                delete_file_size_bytes: delete_size,
                schema_version: entry.schema_version,
                partition_by: entry
                    .partition_keys()
                    .iter()
                    .map(|key| key.key_text())
                    .collect(),
                inlined_rows: entry.live_inlined_rows(),
                inlined_history: entry.inlined.len() as u64,
                inlined_limit: entry.inlined_limit,
            });
        }
        Ok(out)
    }

    /// The catalog's read-only facts (`ducklake_options`'s
    /// name/description/value trio; pinkdb has no settings store, so every
    /// value here is a fact of the current catalog, not a knob).
    pub fn options(&self) -> Result<Vec<LakeOption>> {
        let (manifest, _) = self.manifest(None)?;
        let option = |name: &str, description: &str, value: String| LakeOption {
            name: name.to_string(),
            description: description.to_string(),
            value,
        };
        Ok(vec![
            option(
                "created_by",
                "Tool that writes this lake",
                format!("pinkdb {}", env!("CARGO_PKG_VERSION")),
            ),
            option(
                "data_path",
                "Path to data files",
                self.storage.abs("").display().to_string(),
            ),
            option(
                "encrypted",
                "Whether lake objects are sealed at rest",
                self.storage.encrypted().to_string(),
            ),
            option(
                "version",
                "Catalog format_version of the current manifest",
                manifest.format_version.to_string(),
            ),
            option(
                "data_format",
                "Data-file format this build writes",
                match self.write_format() {
                    FileFormat::Pink => "pink".to_string(),
                    FileFormat::Vortex => "vortex".to_string(),
                },
            ),
            option(
                "current_version",
                "Current lake version (catalog.json)",
                manifest.version.to_string(),
            ),
            option(
                "schema_version",
                "Lake-wide schema version of the current manifest",
                manifest.schema_version.to_string(),
            ),
        ])
    }

    /// [`Lake::options`]'s three DuckLake counterparts as one record:
    /// `catalog_type`, `extension_version`, `data_path`.
    pub fn settings(&self) -> LakeSettings {
        LakeSettings {
            catalog_type: "json".to_string(),
            extension_version: env!("CARGO_PKG_VERSION").to_string(),
            data_path: self.storage.abs("").display().to_string(),
        }
    }
}

/// `Some(a) + Some(b)` with overflow reporting (`None` on overflow or when a
/// side is unavailable).
fn add_opt(acc: Option<u64>, value: Option<u64>) -> Option<u64> {
    match (acc, value) {
        (Some(a), Some(b)) => a.checked_add(b),
        _ => None,
    }
}
