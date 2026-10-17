//! `.lake attach`: a session-local, read-only, statistics-only view of one
//! lake.
//!
//! Attaching reads `catalog.json` + the chosen manifest (exactly what
//! [`Lake::load`] reads first). What it reads from the data files depends on
//! the entry's format:
//!
//! - a **Vortex** entry (`vortex-format` builds): each data file's footer,
//!   the `pinkdb.table` identity/schema segment and the stored statistics —
//!   no data segment is read and no column is decoded. Locally that is a
//!   positioned read of the postscript + footer per file; over object storage
//!   (which has no ranged reads in this build) the whole object is read into
//!   memory and opened in place, still skipping the decode.
//! - a **`.pink`** entry (every build): the per-file statistics blocks
//!   published in the manifest and nothing else — no data file is opened at
//!   attach. Each block carries a file's schema and exact per-column
//!   statistics and is CRC-protected against its key, path and row count (see
//!   [`crate::stats`]); a `.pink` file published without one is refused with
//!   a clear message.
//!
//! A v4 table may list several data files (incremental appends) and
//! positional delete files. Attach merges the files' statistics into the
//! table's: row counts and NULL counts add up, `MIN`/`MAX` fold, and every
//! fact stays exact — **unless the table has delete files**, in which case
//! the per-column facts can no longer be proven from metadata alone (a
//! deleted position may have held the minimum), so they become unknown and
//! the affected queries materialize; the row count stays exact and
//! `COUNT(*)` keeps its fast path.
//!
//! # Pruned scans (wave 66)
//!
//! Per **file**, attach also keeps the file's own zone map and partition
//! values. When a query's `WHERE` carries simple conjuncts (`col op literal`
//! / `IS [NOT] NULL`, extracted by the engine's plan analysis),
//! [`pinkdb::AttachedLake::load_filtered`] skips every data file the map
//! proves cannot match — no decode, no bytes read for that file — and
//! decodes the survivors (only the referenced columns, where the format
//! supports a projection). Execution still applies the filter; pruning only
//! removes files that provably contribute nothing, so answers are identical
//! to a full scan.
//!
//! A file-level zone map stays valid after deletes (deleting rows only
//! shrinks the value set): a pruned file could not have matched the
//! predicate *before* its deletes either. That is the opposite direction
//! from the table-level aggregates above, which fall back — both rules are
//! exact, and neither is an approximation. A pruned load is never merged
//! into the session cache (it is a different row set); a query whose
//! predicates cannot prune takes the ordinary cached path unchanged.
//!
//! # Counters
//!
//! [`pinkdb::AttachedLake::decode_stats`] reports `(loads, columns decoded,
//! files decoded)` over the attachment and its time-travel views — the
//! observability behind "a statistics-only query decodes nothing" and "a
//! pruned scan decodes only the matching files".
//!
//! The result is a [`pinkdb::AttachedLake`]: descriptors the engine can
//! answer exact `COUNT`/`MIN`/`MAX` from, and a per-table loader that decodes
//! a table on first use through the very same path [`Lake::load`] uses
//! (every data file in order, each with its positional deletes applied), so
//! materialized values are identical to `.lake open`.
//!
//! # Time travel
//!
//! `FROM t AT (VERSION => n)` / `AT (TIMESTAMP => ts)` on an attached table
//! resolves through the same machinery, one version at a time (wave 64): the
//! target version's manifest is read, then a *version attachment* is built
//! exactly like `.lake attach <dir> <version>` builds one — every footer /
//! statistics block validated, every data file of that version checked to
//! exist — and cached per version, so repeated queries pay the metadata reads
//! once. The target is capped at the version this attachment is pinned to:
//! `.lake attach dir 3` can travel to 1–3, never to a version published after
//! it attached. An unknown version, a timestamp before the first snapshot, or
//! a table that did not exist at the target version is a clean error from
//! [`pinkdb::AttachedLake::table_at`]; a data file missing from an old
//! version is the same clean error [`Lake::load`] would give.
//!
//! Everything else is an ordinary attachment, so the engine's statistics
//! fast paths work on a time-travel scan unchanged: `COUNT(*)`/`MIN`/`MAX`
//! over `t AT (VERSION => 1)` are answered from that version's published
//! statistics without decoding a data file.
//!
//! Every check `.lake open` performs at load time runs at attach time
//! instead, where it can: each file's identity matches its catalog key, every
//! file's declared row count matches what the file/metadata says, the schemas
//! agree, and the summed row counts (minus deleted positions) match the
//! manifest. A lake this build cannot attach (a `.pink` file without
//! statistics) is refused with a clear error instead of a guess.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pinkdb::attached::AttachedScan;
use pinkdb::{
    AtSpec, AttachedLake, AttachedTable, ColumnStats, DataTable, PinkDb, PinkDbError, Value,
};

use crate::catalog::{DeleteRef, FileFormat, PartitionKey, TableEntry};
use crate::incremental::entry_layout;
use crate::storage::{Head, Storage};
#[cfg(feature = "vortex-format")]
use crate::vortex_file;
use crate::zone::{self, FileFacts};
use crate::{
    apply_file_deletes, copy_chunks, manifest_for, only_table, path_str, published_versions,
    split_key, validate_rel_path, Lake, LakeError, Result,
};

/// What [`Lake::attach`] read and pinned.
#[derive(Debug)]
pub struct AttachmentReport {
    /// The manifest version the attachment is pinned to (the latest, or the
    /// one asked for).
    pub version: u64,
    /// Number of attached tables.
    pub tables: usize,
    /// The lake's catalog `format_version` (1–4 in this build).
    pub format_version: u32,
    /// True when Vortex data files live on object storage and attach read
    /// whole objects (no ranged reads in this build), still without
    /// decoding. Always false for `.pink` entries: attach reads no data file
    /// at all.
    pub remote: bool,
    /// True when every attached entry is a `.pink` entry, so attach read the
    /// manifests' CRC-protected statistics blocks and **no data file at
    /// all** (the "statistics-only" shape); false when any entry needed a
    /// Vortex footer read (the "footer-only" shape).
    pub statistics_only: bool,
    /// The read-only handle to register with the engine
    /// ([`pinkdb::PinkDb::attach_lake`]).
    pub lake: Arc<dyn AttachedLake>,
}

/// The data-file format one attached table materializes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttachedFormat {
    /// The engine's single-table `.pink` bytes (v1/v4 lean entries): decode
    /// via [`PinkDb::open`] — the same decode `.lake open` performs. No
    /// projected reads exist for this format, so a projection materializes
    /// the whole table and caches it.
    Pink,
    /// A Vortex file (v2/v3/v4 `vortex-format` entries): projected
    /// footer-only decodes.
    Vortex,
}

/// A `.pink` file's schema and gated statistics, as attach wants them.
type SchemaAndStats = (Vec<(String, pinkdb::DataType)>, Vec<ColumnStats>);

/// One data file of an attached table: its exact row count, the physical
/// positions its delete files drop (merge-on-read), its per-column zone map
/// (when any metadata proves one), and its recorded partition values.
#[derive(Debug, Clone)]
struct AttachedFile {
    rel: String,
    rows: u64,
    deletes: Vec<u64>,
    /// The file's per-column exact facts, in schema order. `None` when no
    /// metadata carries them (e.g. a `.pink` file published without a block):
    /// the file can never be pruned, but it always decodes correctly.
    ///
    /// Unlike the table-level statistics (`merge_file_stats`, which falls
    /// back to unknown once any delete file exists), a per-file zone map
    /// stays usable for pruning with deletes: deleting rows only shrinks the
    /// value set, so a file that provably could not match a predicate before
    /// deletes still cannot after them (see `crate::zone`).
    zone: Option<Vec<ColumnStats>>,
    /// The file's partition value map, keyed by
    /// [`crate::catalog::PartitionKey::key_text`]; not necessarily aligned
    /// with the table's current spec (a file written before the spec was set
    /// carries none; a spec change does not rewrite old files).
    partition: Option<std::collections::BTreeMap<String, Option<String>>>,
}

/// One attached table's data-file list plus how to materialize it.
#[derive(Debug, Clone)]
struct AttachedEntry {
    db: String,
    table: String,
    /// Column names/types in scan order (validated at attach time), the
    /// schema every file of the table agrees on.
    schema: Vec<(String, pinkdb::DataType)>,
    /// The table's partition specification at this version (possibly empty).
    keys: Vec<PartitionKey>,
    files: Vec<AttachedFile>,
    format: AttachedFormat,
}

impl AttachedEntry {
    /// The table's key for error messages.
    fn key(&self) -> String {
        format!("{}.{}", self.db, self.table)
    }
}

/// One attached lake, as the engine sees it.
#[derive(Debug)]
pub(crate) struct LakeAttachment {
    location: String,
    storage: Arc<dyn Storage>,
    root: std::path::PathBuf,
    /// lowercased `(db, table)` → the data files and how to decode them.
    files: HashMap<(String, String), AttachedEntry>,
    tables: Vec<AttachedTable>,
    /// Materialized columns per table, decoded lazily and cached.
    cache: Mutex<HashMap<(String, String), CachedTable>>,
    /// Decodes performed (tests: a stats-only query must not decode).
    loads: AtomicU64,
    /// Columns decoded in total (diagnostics and tests: projection pushdown
    /// must decode only the referenced columns).
    columns_decoded: AtomicU64,
    /// Data files decoded in total (diagnostics and tests: file pruning must
    /// decode only the files whose zone maps leave a predicate
    /// satisfiable).
    files_decoded: AtomicU64,
    /// The version this attachment is pinned to (the attach version). Time
    /// travel reaches `1..=pinned` (see the module docs).
    pinned: u64,
    /// Time-travel attachments built for one version each, cached.
    views: Mutex<HashMap<u64, Arc<LakeAttachment>>>,
    /// (version, parsed publish time) for every version this attachment
    /// may travel to, ascending — loaded once from the manifests, cached.
    index: Mutex<Option<SnapshotTimes>>,
}

/// One version's publish time as this attachment's index caches it: the
/// parsed `updated` value in microseconds, or `None` when the manifest's time
/// did not parse (a tampered manifest — excluded from timestamp resolution).
type SnapshotTimes = Arc<Vec<(u64, Option<i64>)>>;

/// One version's attachment as a time-travel read sees it: the pinned
/// attachment itself (no rebuild for `AT (VERSION => <pinned>)`) or a cached
/// per-version attachment.
#[derive(Debug)]
enum VersionView<'a> {
    Pinned(&'a LakeAttachment),
    Built(Arc<LakeAttachment>),
}

impl std::ops::Deref for VersionView<'_> {
    type Target = LakeAttachment;
    fn deref(&self) -> &LakeAttachment {
        match self {
            VersionView::Pinned(attachment) => attachment,
            VersionView::Built(attachment) => attachment,
        }
    }
}

/// One table's materialized state: the columns decoded so far, plus NULL
/// placeholders (same chunking) for the ones not decoded yet. Every column of
/// `table` is chunk-aligned with every other, so a column decoded later can
/// be swapped in without touching the rest.
#[derive(Debug)]
struct CachedTable {
    table: Arc<DataTable>,
    /// One flag per column: true = decoded (its chunks hold values).
    present: Vec<bool>,
}

impl LakeAttachment {
    #[cfg(test)]
    pub(crate) fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
    }

    #[cfg(all(test, feature = "vortex-format"))]
    pub(crate) fn columns_decoded(&self) -> u64 {
        self.columns_decoded.load(Ordering::Relaxed)
    }

    /// Data files decoded by this attachment's loads (tests: a pruned scan
    /// must decode only the files it could not prove empty of matches).
    #[cfg(test)]
    pub(crate) fn files_decoded(&self) -> u64 {
        self.files_decoded.load(Ordering::Relaxed)
    }

    /// The table descriptor for a lowercased `(db, table)` key.
    fn descriptor(&self, key: &(String, String)) -> Option<&AttachedTable> {
        let (db, table) = key;
        self.tables
            .iter()
            .find(|t| t.db.eq_ignore_ascii_case(db) && t.table.eq_ignore_ascii_case(table))
    }

    /// The columns a call must decode: `requested` marked per index. A
    /// `None`/out-of-range/unprovable projection marks every column (the
    /// always-correct fallback), and an empty one marks column 0 (the table's
    /// chunk shape has to come from somewhere).
    fn requested_columns(&self, key: &(String, String), columns: Option<&[usize]>) -> Vec<bool> {
        let n_cols = self.descriptor(key).map(|t| t.schema.len()).unwrap_or(0);
        let mut requested = match columns {
            Some(cols) if cols.iter().all(|&i| i < n_cols) => {
                let mut wanted = vec![false; n_cols];
                for &i in cols {
                    wanted[i] = true;
                }
                wanted
            }
            _ => vec![true; n_cols],
        };
        if requested.iter().all(|&w| !w) {
            if let Some(first) = requested.first_mut() {
                *first = true;
            }
        }
        requested
    }
}

impl AttachedLake for LakeAttachment {
    fn location(&self) -> &str {
        &self.location
    }

    fn tables(&self) -> &[AttachedTable] {
        &self.tables
    }

    fn load(&self, db: &str, table: &str) -> std::result::Result<Arc<DataTable>, PinkDbError> {
        Ok(self.load_projected(db, table, None)?.table)
    }

    fn load_projected(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
    ) -> std::result::Result<AttachedScan, PinkDbError> {
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        // One decode per table at a time: the lock is held across the decode
        // so concurrent readers of one store materialize each column once.
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        let Some(entry) = self.files.get(&key).cloned() else {
            return Err(PinkDbError::Catalog(format!(
                "lake {} has no table \"{db}.{table}\"",
                self.location
            )));
        };
        let requested = self.requested_columns(&key, columns);
        if requested.is_empty() {
            return Err(PinkDbError::Catalog(format!(
                "lake {} table \"{db}.{table}\" has no columns",
                self.location
            )));
        }
        match cache.get_mut(&key) {
            Some(cached) => {
                let missing: Vec<usize> = (0..requested.len())
                    .filter(|&i| requested[i] && !cached.present[i])
                    .collect();
                if missing.is_empty() {
                    return Ok(AttachedScan {
                        table: Arc::clone(&cached.table),
                        present: cached.present.clone(),
                    });
                }
                self.loads.fetch_add(1, Ordering::Relaxed);
                let decoded_count = if entry.format == AttachedFormat::Pink {
                    // A `.pink` projection decodes the whole table.
                    cached.present.len() as u64
                } else {
                    missing.len() as u64
                };
                self.columns_decoded
                    .fetch_add(decoded_count, Ordering::Relaxed);
                self.files_decoded
                    .fetch_add(entry.files.len() as u64, Ordering::Relaxed);
                let (fresh, _fresh_present) = self
                    .load_entry(&entry, Some(&missing))
                    .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                if entry.format == AttachedFormat::Pink {
                    // A `.pink` projection decodes the whole table: the fresh
                    // decode is the complete state, no column merge needed.
                    cached.present = vec![true; fresh.num_cols()];
                    cached.table = Arc::new(fresh);
                } else {
                    let merged = merge_projected(&cached.table, &fresh, &missing, &entry.table)
                        .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                    for &i in &missing {
                        cached.present[i] = true;
                    }
                    cached.table = Arc::new(merged);
                }
                Ok(AttachedScan {
                    table: Arc::clone(&cached.table),
                    present: cached.present.clone(),
                })
            }
            None => {
                self.loads.fetch_add(1, Ordering::Relaxed);
                let decoded_count = if entry.format == AttachedFormat::Pink {
                    // A `.pink` projection decodes the whole table (no
                    // projected reader); it counts every column.
                    requested.len() as u64
                } else {
                    requested.iter().filter(|&&w| w).count() as u64
                };
                self.columns_decoded
                    .fetch_add(decoded_count, Ordering::Relaxed);
                self.files_decoded
                    .fetch_add(entry.files.len() as u64, Ordering::Relaxed);
                let full = entry.format == AttachedFormat::Pink || requested.iter().all(|&w| w);
                let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
                let (table, present) = self
                    .load_entry(&entry, if full { None } else { Some(&decoded) })
                    .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                let table = Arc::new(table);
                cache.insert(
                    key,
                    CachedTable {
                        table: Arc::clone(&table),
                        present: present.clone(),
                    },
                );
                Ok(AttachedScan { table, present })
            }
        }
    }

    fn load_filtered(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
    ) -> std::result::Result<AttachedScan, PinkDbError> {
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        if predicates.is_empty() {
            return self.load_projected(db, table, columns);
        }
        let Some(entry) = self.files.get(&key).cloned() else {
            return Err(PinkDbError::Catalog(format!(
                "lake {} has no table \"{db}.{table}\"",
                self.location
            )));
        };
        let survivors: Vec<AttachedFile> = entry
            .files
            .iter()
            .filter(|file| {
                let facts = FileFacts {
                    rows: file.rows,
                    zone: file.zone.as_deref(),
                    partition: file.partition.as_ref(),
                };
                !zone::file_prunes(facts, &entry.keys, &entry.schema, predicates)
            })
            .cloned()
            .collect();
        if survivors.len() == entry.files.len() {
            // Nothing pruned: take the ordinary cached path, so a query whose
            // predicates cannot prune behaves exactly as before (and shares
            // the materialized columns).
            return self.load_projected(db, table, columns);
        }
        // A pruned load is a different row set than the full table, so it is
        // never merged into the session cache: it decodes its surviving files
        // (only the requested columns, where the format supports a
        // projection) and is returned as-is.
        let requested = self.requested_columns(&key, columns);
        self.loads.fetch_add(1, Ordering::Relaxed);
        self.files_decoded
            .fetch_add(survivors.len() as u64, Ordering::Relaxed);
        let decoded_count = if entry.format == AttachedFormat::Pink {
            requested.len() as u64
        } else {
            requested.iter().filter(|&&w| w).count() as u64
        };
        self.columns_decoded
            .fetch_add(decoded_count, Ordering::Relaxed);
        let full = entry.format == AttachedFormat::Pink || requested.iter().all(|&w| w);
        let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
        let (table, present) = self
            .load_some(&entry, &survivors, if full { None } else { Some(&decoded) })
            .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        Ok(AttachedScan {
            table: Arc::new(table),
            present,
        })
    }

    fn decode_stats(&self) -> (u64, u64, u64) {
        // Time-travel views decode through their own attachments; count them
        // in (they are cached inside this attachment, part of its cost).
        let mut loads = self.loads.load(Ordering::Relaxed);
        let mut columns = self.columns_decoded.load(Ordering::Relaxed);
        let mut files = self.files_decoded.load(Ordering::Relaxed);
        let views = self.views.lock().unwrap_or_else(|p| p.into_inner());
        for view in views.values() {
            loads += view.loads.load(Ordering::Relaxed);
            columns += view.columns_decoded.load(Ordering::Relaxed);
            files += view.files_decoded.load(Ordering::Relaxed);
        }
        (loads, columns, files)
    }

    fn table_at(
        &self,
        db: &str,
        table: &str,
        at: AtSpec,
    ) -> std::result::Result<AttachedTable, PinkDbError> {
        let version = self.resolve_version(at)?;
        let view = self.view(version)?;
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        view.descriptor(&key).cloned().ok_or_else(|| {
            PinkDbError::Catalog(format!(
                "table \"{db}.{table}\" does not exist at version {version} of lake {} \
                 (AT ({at}))",
                self.location
            ))
        })
    }

    fn load_at(
        &self,
        db: &str,
        table: &str,
        at: AtSpec,
    ) -> std::result::Result<Arc<DataTable>, PinkDbError> {
        Ok(self.load_projected_at(db, table, at, None)?.table)
    }

    fn load_projected_at(
        &self,
        db: &str,
        table: &str,
        at: AtSpec,
        columns: Option<&[usize]>,
    ) -> std::result::Result<AttachedScan, PinkDbError> {
        let version = self.resolve_version(at)?;
        let view = self.view(version)?;
        view.load_projected(db, table, columns)
    }

    fn load_filtered_at(
        &self,
        db: &str,
        table: &str,
        at: AtSpec,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
    ) -> std::result::Result<AttachedScan, PinkDbError> {
        let version = self.resolve_version(at)?;
        let view = self.view(version)?;
        view.load_filtered(db, table, columns, predicates)
    }
}

impl LakeAttachment {
    /// The published versions this attachment may travel to, ascending, with
    /// each manifest's parsed publish time. Loaded once (one small manifest
    /// read per version, capped at the pinned version); a version that
    /// disappears between the listing and the read (an expired snapshot) is
    /// skipped, exactly like [`Lake::versions`].
    fn snapshot_index(&self) -> std::result::Result<SnapshotTimes, PinkDbError> {
        let mut guard = self.index.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(index) = &*guard {
            return Ok(Arc::clone(index));
        }
        let versions = published_versions(&*self.storage, self.pinned).map_err(catalog_error)?;
        let mut index = Vec::with_capacity(versions.len());
        for version in versions {
            match manifest_for(&*self.storage, &self.root, Some(version)) {
                Ok((manifest, _)) => {
                    index.push((version, crate::time::parse_iso8601_utc(&manifest.updated)))
                }
                Err(LakeError::VersionNotFound { .. }) => continue,
                Err(LakeError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue
                }
                Err(e) => return Err(catalog_error(e)),
            }
        }
        let index = Arc::new(index);
        *guard = Some(Arc::clone(&index));
        Ok(index)
    }

    /// Resolve a time-travel target to a version this attachment serves:
    /// exactly `n` for a version, the newest snapshot at or before `ts` for a
    /// timestamp (DuckLake's inclusive `snapshot_time <= ts` rule). Beyond
    /// the pinned version, or before the first snapshot, is a clean
    /// "no snapshot found" error.
    fn resolve_version(&self, at: AtSpec) -> std::result::Result<u64, PinkDbError> {
        let index = self.snapshot_index()?;
        match at {
            AtSpec::Version(v) => index
                .iter()
                .find(|(version, _)| *version == v)
                .map(|(version, _)| *version)
                .ok_or_else(|| {
                    PinkDbError::Catalog(format!(
                        "no snapshot found at version {v} in lake {}",
                        self.location
                    ))
                }),
            AtSpec::Timestamp(us) => index
                .iter()
                .rev()
                .find(|(_, time)| time.is_some_and(|t| t <= us))
                .map(|(version, _)| *version)
                .ok_or_else(|| {
                    PinkDbError::Catalog(format!(
                        "no snapshot found at timestamp {} in lake {}",
                        pinkdb::types::format_timestamp(us),
                        self.location
                    ))
                }),
        }
    }

    /// One version's attachment: the pinned attachment itself, or a cached
    /// footer-only/statistics-only attachment of that version's manifest —
    /// exactly what `.lake attach <dir> <version>` would build, so every
    /// validation runs and the target version's data files are what queries
    /// materialize.
    fn view(&self, version: u64) -> std::result::Result<VersionView<'_>, PinkDbError> {
        if version == self.pinned {
            return Ok(VersionView::Pinned(self));
        }
        let mut views = self.views.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(view) = views.get(&version) {
            return Ok(VersionView::Built(Arc::clone(view)));
        }
        let (attachment, _) = build_from(&self.storage, &self.root, Some(version))
            .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        views.insert(version, Arc::clone(&attachment));
        Ok(VersionView::Built(attachment))
    }
    /// Materialize one entry: every data file in manifest order, each with
    /// its positional deletes applied, unioned into one table. `columns`
    /// selects a projection for the files that support one (Vortex); `.pink`
    /// files always decode in full. The returned `present` flags match the
    /// union's columns.
    fn load_entry(
        &self,
        entry: &AttachedEntry,
        columns: Option<&[usize]>,
    ) -> Result<(DataTable, Vec<bool>)> {
        self.load_some(entry, &entry.files, columns)
    }

    /// [`LakeAttachment::load_entry`] over a chosen subset of the entry's
    /// files (the survivors of file pruning). An empty subset decodes
    /// nothing and yields an empty table with the entry's full schema.
    fn load_some(
        &self,
        entry: &AttachedEntry,
        files: &[AttachedFile],
        columns: Option<&[usize]>,
    ) -> Result<(DataTable, Vec<bool>)> {
        if files.is_empty() {
            let table = DataTable::new(entry.table.clone(), entry.schema.clone()).map_err(|e| {
                LakeError::Engine {
                    context: format!("preparing the empty pruned scan of \"{}\"", entry.key()),
                    source: e,
                }
            })?;
            return Ok((table, vec![true; entry.schema.len()]));
        }
        let mut union: Option<(DataTable, Vec<bool>)> = None;
        for (i, file) in files.iter().enumerate() {
            let (table, present) = load_file(&*self.storage, &self.root, entry, file, columns)?;
            match &mut union {
                None => union = Some((table, present)),
                Some((dst, first_present)) => {
                    if i > 0 && *first_present != present {
                        return Err(LakeError::Internal {
                            message: format!(
                                "attached files of \"{}\" disagree on the decoded columns",
                                entry.key()
                            ),
                        });
                    }
                    copy_chunks(&table, dst, &file.rel)?;
                }
            }
        }
        union.ok_or_else(|| LakeError::Corrupt {
            path: self.root.join("catalog.json"),
            message: format!("table \"{}\" has no data files", entry.key()),
        })
    }
}

/// Decode one data file of `entry` and apply its positional deletes.
#[cfg_attr(not(feature = "vortex-format"), allow(unused_variables))]
fn load_file(
    storage: &dyn Storage,
    root: &std::path::Path,
    entry: &AttachedEntry,
    file: &AttachedFile,
    columns: Option<&[usize]>,
) -> Result<(DataTable, Vec<bool>)> {
    let key = entry.key();
    match entry.format {
        AttachedFormat::Pink => {
            // No projected `.pink` read exists: any missing column
            // materializes the whole table (one decode, the same `.lake open`
            // performs).
            let table = load_pink_file(storage, root, &file.rel, &entry.db, &entry.table)?;
            if table.num_rows() as u64 != file.rows {
                return Err(LakeError::Corrupt {
                    path: storage.abs(&file.rel),
                    message: format!(
                        "table \"{key}\": data file \"{}\" has {} rows, but the manifest \
                         says {}",
                        file.rel,
                        table.num_rows(),
                        file.rows
                    ),
                });
            }
            let table = apply_file_deletes(&table, &file.deletes, &key, &file.rel)?;
            let present = vec![true; table.num_cols()];
            Ok((table, present))
        }
        AttachedFormat::Vortex => {
            #[cfg(feature = "vortex-format")]
            {
                let (table, present) = match columns {
                    Some(cols) => load_projected_table(
                        storage,
                        root,
                        &file.rel,
                        &entry.db,
                        &entry.table,
                        cols,
                    )?,
                    None => {
                        let table = load_table(storage, root, &file.rel, &entry.db, &entry.table)?;
                        let present = vec![true; table.num_cols()];
                        (table, present)
                    }
                };
                if table.num_rows() as u64 != file.rows {
                    return Err(LakeError::Corrupt {
                        path: storage.abs(&file.rel),
                        message: format!(
                            "table \"{key}\": data file \"{}\" has {} rows, but the manifest \
                             says {}",
                            file.rel,
                            table.num_rows(),
                            file.rows
                        ),
                    });
                }
                let table = apply_file_deletes(&table, &file.deletes, &key, &file.rel)?;
                Ok((table, present))
            }
            #[cfg(not(feature = "vortex-format"))]
            {
                // Unreachable: `read_manifest` refuses a Vortex entry before
                // an attach could see one. A clear error, never a panic.
                Err(LakeError::FeatureMissing {
                    feature: "vortex-format",
                    message: format!(
                        "table \"{key}\" uses a Vortex data file (\"{}\"); this build reads \
                         `.pink` lakes only — rebuild with the `vortex-format` feature",
                        file.rel
                    ),
                })
            }
        }
    }
}

/// Wrap a lake metadata error as an engine catalog error (resolution and
/// listing failures are metadata problems, not query execution ones).
fn catalog_error(e: LakeError) -> PinkDbError {
    PinkDbError::Catalog(e.to_string())
}

/// [`Lake::load`]'s per-file decode for one Vortex table: read the file,
/// decode it into a one-table store, check the file's identity against its
/// catalog key, and hand the table back (chunks shared by `Arc`, never copied
/// twice).
#[cfg(feature = "vortex-format")]
fn load_table(
    storage: &dyn Storage,
    root: &std::path::Path,
    rel: &str,
    dbname: &str,
    tablename: &str,
) -> Result<DataTable> {
    let path = storage.abs(rel);
    let bytes = storage.read(rel, &format!("loading data file \"{rel}\""))?;
    let mini = vortex_file::decode_table_owned(&path, bytes)?;
    let (file_db, file_table) = only_table(&mini, rel, root)?;
    if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(tablename) {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{dbname}.{tablename}\"",
                file_table.name
            ),
        });
    }
    Ok(file_table.clone())
}

/// [`vortex_file::decode_projected`]'s per-file read for one table: decode
/// only `columns`, check the file's identity against its catalog key, and
/// hand the full-schema table (decoded columns + NULL placeholders) back.
#[cfg(feature = "vortex-format")]
fn load_projected_table(
    storage: &dyn Storage,
    root: &std::path::Path,
    rel: &str,
    dbname: &str,
    tablename: &str,
    columns: &[usize],
) -> Result<(DataTable, Vec<bool>)> {
    let path = storage.abs(rel);
    let (mini, present) = vortex_file::decode_projected(&path, storage, rel, columns)?;
    let (file_db, file_table) = only_table(&mini, rel, root)?;
    if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(tablename) {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{dbname}.{tablename}\"",
                file_table.name
            ),
        });
    }
    Ok((file_table.clone(), present))
}

/// Swap freshly decoded columns into an already materialized table. The two
/// tables' chunkings must agree exactly (both come from the same slicing of
/// the same file list, so they do); a mismatch is an internal error, never a
/// silently misaligned column.
fn merge_projected(
    existing: &DataTable,
    fresh: &DataTable,
    columns: &[usize],
    tablename: &str,
) -> Result<DataTable> {
    let rows: Vec<usize> = existing.columns[0].chunks.iter().map(|c| c.len()).collect();
    let mut merged = existing.columns.clone();
    for &i in columns {
        let lens: Vec<usize> = fresh.columns[i].chunks.iter().map(|c| c.len()).collect();
        if lens != rows {
            return Err(LakeError::Internal {
                message: format!(
                    "projected decode of \"{tablename}\" column {i} produced a different \
                     chunking ({} vs {} chunks)",
                    lens.len(),
                    rows.len()
                ),
            });
        }
        merged[i] = fresh.columns[i].clone();
    }
    Ok(DataTable::from_columns(
        existing.name.clone(),
        merged,
        existing.num_rows,
    ))
}

/// The `.pink` per-file read for one table: open the file with the very
/// decode `.lake open` performs and check its identity. `.pink` files have no
/// projected reader, so there is one full decode per file.
fn load_pink_file(
    storage: &dyn Storage,
    root: &std::path::Path,
    rel: &str,
    dbname: &str,
    tablename: &str,
) -> Result<DataTable> {
    let path = storage.abs(rel);
    let copy = storage.local_copy(rel, &format!("loading data file \"{rel}\""))?;
    let mini = PinkDb::open(path_str(copy.path())?).map_err(|e| LakeError::Engine {
        context: format!("loading data file \"{rel}\" for table \"{dbname}.{tablename}\""),
        source: e,
    })?;
    let (file_db, file_table) = only_table(&mini, rel, root)?;
    if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(tablename) {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{dbname}.{tablename}\"",
                file_table.name
            ),
        });
    }
    Ok(file_table.clone())
}

/// One `.pink` file's schema + gated statistics: its published per-file block
/// (v4), or the entry-level block (v1). Either way the block is CRC-verified
/// and cross-checked against the entry's key, the file's path and its row
/// count, so a block pasted under another file can never attach.
fn pink_file_stats(
    entry_key: &str,
    entry: &TableEntry,
    file: &crate::catalog::FileRef,
    manifest_path: &std::path::Path,
) -> Result<SchemaAndStats> {
    let rel = file.path();
    // v4: the block rides inside the file's manifest object. v1: the entry
    // carries one block for its single file.
    let (block, file_rows) = match file.stats() {
        Some(block) => (block, file.rows()),
        None => match entry.stats.as_ref() {
            Some(block) if matches!(file, crate::catalog::FileRef::Path(_)) => (block, None),
            _ => {
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "lake table \"{entry_key}\": `.pink` data file \"{rel}\" was published \
                         without a statistics block (statistics are published by saves from \
                         this build onwards); use .lake open, or save the store again to \
                         publish statistics"
                    ),
                })
            }
        },
    };
    let rows = file_rows.unwrap_or(entry.rows);
    block_schema_and_stats(entry_key, rel, rows, block, manifest_path)
}

/// Verify one statistics block against its entry and turn it into the
/// `(schema, gated stats)` attach needs: the CRC and block version
/// ([`crate::stats::StatsBlock::schema_and_stats`]), the block's
/// `table`/`file`/`rows` against the catalog key, the file's path and its
/// row count. Shared by the `.pink` per-file block and the wave-66
/// catalog-published Vortex zone map, so a block pasted under another file
/// can never attach in either format.
fn block_schema_and_stats(
    entry_key: &str,
    rel: &str,
    rows: u64,
    block: &crate::stats::StatsBlock,
    manifest_path: &std::path::Path,
) -> Result<SchemaAndStats> {
    let corrupt = |message: String| LakeError::Corrupt {
        path: manifest_path.to_path_buf(),
        message,
    };
    let (schema, stats) = block.schema_and_stats(manifest_path)?;
    if block.table != entry_key {
        return Err(corrupt(format!(
            "table \"{entry_key}\" carries the statistics block of \"{}\"",
            block.table
        )));
    }
    if block.file != rel {
        return Err(corrupt(format!(
            "table \"{entry_key}\": the statistics block names data file \"{}\", but the \
             entry names \"{rel}\"",
            block.file
        )));
    }
    if block.rows != rows {
        return Err(corrupt(format!(
            "table \"{entry_key}\": the statistics block of \"{rel}\" declares {} rows, but \
             the entry says {rows}",
            block.rows,
        )));
    }
    Ok((schema, stats))
}

/// The catalog-published zone map of one **Vortex** file: its block,
/// CRC-verified and cross-checked exactly like a `.pink` file's (the same
/// shared gate), so a block pasted under another file can never attach.
#[cfg(feature = "vortex-format")]
fn vortex_catalog_stats(
    entry_key: &str,
    file: &crate::catalog::FileRef,
    manifest_path: &std::path::Path,
) -> Result<Option<SchemaAndStats>> {
    let Some(block) = file.stats() else {
        return Ok(None);
    };
    let rows = file.rows().ok_or_else(|| LakeError::Corrupt {
        path: manifest_path.to_path_buf(),
        message: format!(
            "table \"{entry_key}\": data file \"{}\" has no row count",
            file.path()
        ),
    })?;
    Ok(Some(block_schema_and_stats(
        entry_key,
        file.path(),
        rows,
        block,
        manifest_path,
    )?))
}

/// Whether a catalog zone map is compatible with the file footer's own
/// statistics (the authority): every field the footer proves must match.
/// The block may be *more* informative than the footer (a truncated VARCHAR
/// statistic the footer writes `Inexact` is `None` there while the block can
/// still carry the exact value), so a footer-unknown field never
/// contradicts.
#[cfg(feature = "vortex-format")]
fn zone_stats_compatible(block: &ColumnStats, footer: &ColumnStats) -> bool {
    if let (Some(b), Some(f)) = (block.null_count, footer.null_count) {
        if b != f {
            return false;
        }
    }
    let field_ok = |b: &Option<Value>, f: &Option<Value>| match f {
        Some(f) => b
            .as_ref()
            .is_some_and(|b| pinkdb::plan::compare_values(b, f).is_eq()),
        None => true,
    };
    field_ok(&block.min, &footer.min) && field_ok(&block.max, &footer.max)
}

/// Fold per-file statistics into one table's: NULL counts add up,
/// `MIN`/`MAX` fold when **every** contributing file proves them, and any
/// file that cannot (or any positional delete, which could have removed the
/// extreme value) makes the fact unknown — never an approximation. `files`
/// is `(rows, stats)` per data file in manifest order.
fn merge_file_stats(files: &[(u64, Vec<ColumnStats>)], has_deletes: bool) -> Vec<ColumnStats> {
    let ncols = files.first().map(|(_, s)| s.len()).unwrap_or(0);
    let mut out = Vec::with_capacity(ncols);
    for col in 0..ncols {
        if has_deletes {
            // A deleted position may have held any value: no per-column fact
            // is provable without reading data.
            out.push(ColumnStats::unknown());
            continue;
        }
        let mut nulls = Some(0u64);
        let mut min: Option<Value> = None;
        let mut max: Option<Value> = None;
        let mut min_known = true;
        let mut max_known = true;
        let mut any_rows = false;
        for (rows, stats) in files {
            let s = &stats[col];
            match (nulls, s.null_count) {
                (Some(n), Some(m)) => nulls = n.checked_add(m),
                _ => nulls = None,
            }
            if *rows == 0 {
                continue;
            }
            any_rows = true;
            match &s.min {
                Some(v) if !v.is_null() => match &min {
                    None => min = Some(v.clone()),
                    Some(m) => {
                        if pinkdb::plan::compare_values(v, m).is_lt() {
                            min = Some(v.clone());
                        }
                    }
                },
                // A zero-row file contributes no min/max; a file with rows
                // whose min is unknown (a `-0.0`/NaN gate) makes the fold
                // unknown.
                Some(_) => {}
                None => min_known = false,
            }
            match &s.max {
                Some(v) if !v.is_null() => match &max {
                    None => max = Some(v.clone()),
                    Some(m) => {
                        if pinkdb::plan::compare_values(v, m).is_gt() {
                            max = Some(v.clone());
                        }
                    }
                },
                Some(_) => {}
                None => max_known = false,
            }
        }
        if !any_rows {
            // All files empty: the executor derives NULL min/max from the row
            // count; the exact 0-NULL count is still provable.
            out.push(ColumnStats {
                null_count: nulls,
                min: None,
                max: None,
            });
            continue;
        }
        out.push(ColumnStats {
            null_count: nulls,
            min: if min_known { min } else { None },
            max: if max_known { max } else { None },
        });
    }
    out
}

/// Footer-only / statistics-only open of one manifest's tables (see the
/// module docs). `version` is the caller's snapshot choice; the manifest (not
/// the catalog) is read so an attached version stays pinned however the
/// catalog moves.
pub(crate) fn build(
    lake: &Lake,
    version: Option<u64>,
) -> Result<(Arc<LakeAttachment>, AttachmentReport)> {
    build_from(&lake.storage, lake.path(), version)
}

/// [`build`] without a [`Lake`] handle: the storage root and the version to
/// pin. Time-travel views are built through this same function, so a version
/// query validates exactly like `.lake attach <dir> <version>`.
pub(crate) fn build_from(
    storage: &Arc<dyn Storage>,
    root: &std::path::Path,
    version: Option<u64>,
) -> Result<(Arc<LakeAttachment>, AttachmentReport)> {
    let (manifest, manifest_path) = manifest_for(&**storage, root, version)?;
    let root = root.to_path_buf();
    let mut tables: Vec<AttachedTable> = Vec::with_capacity(manifest.tables.len());
    let mut files: HashMap<(String, String), AttachedEntry> = HashMap::new();
    let mut statistics_only = true;
    #[cfg(feature = "vortex-format")]
    let mut remote = false;
    #[cfg(not(feature = "vortex-format"))]
    let remote = false;
    for (key, entry) in &manifest.tables {
        let (dbname, table) = split_key(key, &manifest_path)?;
        // Every file (and delete file) must be present before anything is
        // read from it; the layout read decodes and validates every delete
        // file against the entry.
        let layout = entry_layout(&**storage, entry, key, &manifest_path)?;
        for d in &entry.deletes {
            check_delete_file_present(&**storage, d, key)?;
        }
        for (rel, _, _) in &layout {
            validate_rel_path(rel)?;
            let path = storage.abs(rel);
            match storage.head(rel)? {
                Head::File => {}
                Head::Missing => {
                    return Err(LakeError::Io {
                        context: format!("loading data file \"{rel}\""),
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("{} is missing", path.display()),
                        ),
                    })
                }
                Head::NotAFile => {
                    return Err(LakeError::Corrupt {
                        path,
                        message: "data file is not a regular file".to_string(),
                    })
                }
            }
        }
        let format = entry.format();
        if format != FileFormat::Pink {
            statistics_only = false;
        }
        // Per-file (schema, stats), in manifest order. The stats are the
        // file's zone map (wave 66): the `.pink` block's gated column facts,
        // or for Vortex the footer's, cross-checked against a
        // catalog-published block when one rides on the file.
        let mut per_file: Vec<(u64, Vec<ColumnStats>)> = Vec::with_capacity(entry.files.len());
        let mut schema: Option<Vec<(String, pinkdb::DataType)>> = None;
        for file in &entry.files {
            // A v1–v3 entry's single file carries no per-file count; the
            // entry's logical count is that file's count.
            let rows = file.rows().unwrap_or(entry.rows);
            let (file_schema, file_stats) = match format {
                FileFormat::Pink => pink_file_stats(key, entry, file, &manifest_path)?,
                FileFormat::Vortex => {
                    #[cfg(feature = "vortex-format")]
                    {
                        let path = storage.abs(file.path());
                        let local = storage.local_path(file.path()).is_some();
                        remote |= !local;
                        let footer = vortex_file::read_footer(&path, &**storage, file.path())?;
                        if !footer.db.eq_ignore_ascii_case(dbname)
                            || !footer.table.eq_ignore_ascii_case(table)
                        {
                            return Err(LakeError::Corrupt {
                                path,
                                message: format!(
                                    "data file holds {}.{}, but the catalog says \"{key}\"",
                                    footer.db, footer.table
                                ),
                            });
                        }
                        if footer.rows != rows {
                            return Err(LakeError::Corrupt {
                                path: manifest_path.clone(),
                                message: format!(
                                    "table \"{key}\": data file \"{}\" has {} rows, but the \
                                     manifest says {rows}",
                                    file.path(),
                                    footer.rows
                                ),
                            });
                        }
                        // A catalog-published zone map must not contradict
                        // the file's own footer (the footer is the authority;
                        // the block may be the more informative of the two —
                        // a truncated VARCHAR statistic the footer refuses
                        // can still be exact in the block). Any field the
                        // footer proves must match.
                        let block_stats = match vortex_catalog_stats(key, file, &manifest_path)? {
                            Some((block_schema, block_stats)) => {
                                if block_schema != footer.schema {
                                    return Err(LakeError::Corrupt {
                                        path: manifest_path.clone(),
                                        message: format!(
                                            "table \"{key}\": the catalog zone map of \"{}\" \
                                             disagrees with the file's footer on the schema",
                                            file.path()
                                        ),
                                    });
                                }
                                for (i, (b, f)) in block_stats.iter().zip(&footer.stats).enumerate()
                                {
                                    if !zone_stats_compatible(b, f) {
                                        return Err(LakeError::Corrupt {
                                            path: manifest_path.clone(),
                                            message: format!(
                                                "table \"{key}\": the catalog zone map of \
                                                 \"{}\" disagrees with the file's footer on \
                                                 column {i}",
                                                file.path()
                                            ),
                                        });
                                    }
                                }
                                Some(block_stats)
                            }
                            None => None,
                        };
                        let stats = block_stats.unwrap_or(footer.stats);
                        (footer.schema, stats)
                    }
                    #[cfg(not(feature = "vortex-format"))]
                    {
                        // Unreachable: `read_manifest` refuses a Vortex entry
                        // before an attach could see one. A clear error,
                        // never a panic.
                        return Err(LakeError::FeatureMissing {
                            feature: "vortex-format",
                            message: format!(
                                "table \"{key}\" uses a Vortex data file (\"{}\"); this \
                                 build reads `.pink` lakes only — rebuild with the \
                                 `vortex-format` feature to attach it",
                                file.path()
                            ),
                        });
                    }
                }
            };
            match &schema {
                None => schema = Some(file_schema),
                Some(first) if *first != file_schema => {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.clone(),
                        message: format!("data files of table \"{key}\" disagree on the schema"),
                    });
                }
                Some(_) => {}
            }
            per_file.push((rows, file_stats));
        }
        let schema = schema.ok_or_else(|| LakeError::Corrupt {
            path: manifest_path.clone(),
            message: format!("table \"{key}\" has no data files"),
        })?;
        // The partition spec: every key column must exist, and every file's
        // recorded partition value must decode under that column's type and
        // agree with the file's zone map (a misplaced or tampered value must
        // never silently prune a matching file).
        let keys: Vec<PartitionKey> = entry.partition_keys().to_vec();
        for spec_key in &keys {
            if zone::schema_column_index(&schema, &spec_key.column).is_none() {
                return Err(LakeError::Corrupt {
                    path: manifest_path.clone(),
                    message: format!(
                        "table \"{key}\": partition key column \"{}\" does not exist in the \
                         data files' schema",
                        spec_key.column
                    ),
                });
            }
        }
        for (file, (_, file_stats)) in entry.files.iter().zip(&per_file) {
            let Some(partition) = file.partition() else {
                continue;
            };
            for (text, value) in partition {
                let parsed = match PartitionKey::parse(text) {
                    Ok(parsed) => parsed,
                    Err(_) => {
                        return Err(LakeError::Corrupt {
                            path: manifest_path.clone(),
                            message: format!(
                                "table \"{key}\": data file \"{}\" carries partition key \
                                 {text:?}, which is not a column name or bucket(N, column)",
                                file.path()
                            ),
                        })
                    }
                };
                if !parsed.is_identity() {
                    // A bucket value is the writer's own label; there is no
                    // column fact to cross-check it against (and this build
                    // never prunes by one).
                    continue;
                }
                let Some(idx) = zone::schema_column_index(&schema, &parsed.column) else {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.clone(),
                        message: format!(
                            "table \"{key}\": data file \"{}\" carries a partition value for \
                             unknown column \"{}\"",
                            file.path(),
                            parsed.column
                        ),
                    });
                };
                if let Err(message) = zone::cross_check_partition_value(
                    file.rows().unwrap_or(entry.rows),
                    file_stats.get(idx),
                    value,
                    schema[idx].1,
                ) {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.clone(),
                        message: format!(
                            "table \"{key}\": data file \"{}\" fails its partition check: \
                             {message}",
                            file.path()
                        ),
                    });
                }
            }
        }
        let stats = merge_file_stats(&per_file, !entry.deletes.is_empty());
        let attached = AttachedTable {
            db: dbname.to_string(),
            table: table.to_string(),
            rows: entry.rows,
            schema: schema.clone(),
            stats,
        };
        let file_list: Vec<AttachedFile> = layout
            .iter()
            .zip(entry.files.iter().zip(per_file.iter()))
            .map(|((rel, rows, deletes), (file, (_, stats)))| AttachedFile {
                rel: rel.clone(),
                rows: *rows,
                deletes: deletes.clone(),
                zone: Some(stats.clone()),
                partition: file.partition().cloned(),
            })
            .collect();
        let lkey = (dbname.to_ascii_lowercase(), table.to_ascii_lowercase());
        if files.contains_key(&lkey) {
            return Err(LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!(
                    "the manifest names \"{key}\" twice under one case-insensitive name; \
                     refusing to attach an ambiguous table"
                ),
            });
        }
        files.insert(
            lkey,
            AttachedEntry {
                db: dbname.to_string(),
                table: table.to_string(),
                schema,
                keys,
                files: file_list,
                format: if format == FileFormat::Pink {
                    AttachedFormat::Pink
                } else {
                    AttachedFormat::Vortex
                },
            },
        );
        tables.push(attached);
    }
    let attachment = Arc::new(LakeAttachment {
        location: root.display().to_string(),
        storage: Arc::clone(storage),
        root,
        files,
        tables,
        cache: Mutex::new(HashMap::new()),
        loads: AtomicU64::new(0),
        columns_decoded: AtomicU64::new(0),
        files_decoded: AtomicU64::new(0),
        pinned: manifest.version,
        views: Mutex::new(HashMap::new()),
        index: Mutex::new(None),
    });
    let report = AttachmentReport {
        version: manifest.version,
        tables: attachment.tables.len(),
        format_version: manifest.format_version,
        remote,
        statistics_only,
        lake: Arc::clone(&attachment) as Arc<dyn AttachedLake>,
    };
    Ok((attachment, report))
}

/// A referenced delete file must exist (present, and a regular file locally).
fn check_delete_file_present(storage: &dyn Storage, d: &DeleteRef, key: &str) -> Result<()> {
    validate_rel_path(&d.path)?;
    match storage.head(&d.path)? {
        Head::File => Ok(()),
        Head::Missing => Err(LakeError::Io {
            context: format!("loading delete file \"{}\"", d.path),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "{} (delete file of table \"{key}\" targeting \"{}\") is missing",
                    storage.abs(&d.path).display(),
                    d.file
                ),
            ),
        }),
        Head::NotAFile => Err(LakeError::Corrupt {
            path: storage.abs(&d.path),
            message: "delete file is not a regular file".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::{PinkDb, Value};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicU64;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_attach_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A store with an all-types table (NULLs included) plus an empty table.
    fn fixture_store() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE TABLE t (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, \
             dt DATE, ts TIMESTAMP);",
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            t.append_row(vec![
                Value::Boolean(true),
                Value::Integer(-5),
                Value::Bigint(9_999_999_999),
                Value::Double(-2.5),
                Value::Varchar("b".to_string()),
                Value::Date(0),
                Value::Timestamp(-1),
            ])
            .unwrap();
            t.append_row(vec![Value::Null; 7]).unwrap();
            t.append_row(vec![
                Value::Boolean(false),
                Value::Integer(9),
                Value::Bigint(i64::MIN),
                Value::Double(1.25),
                Value::Varchar("a".to_string()),
                Value::Date(19_000),
                Value::Timestamp(1_700_000_000_000_000),
            ])
            .unwrap();
        }
        db.execute("CREATE TABLE empty (x INTEGER);").unwrap();
        db
    }

    fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        let mut results = db.execute(sql).unwrap();
        match results.pop().unwrap() {
            pinkdb::QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Stats-only queries must not decode a data file; anything else must
    /// decode once and then cache.
    #[test]
    fn stats_queries_do_not_decode_and_others_decode_once() {
        let dir = scratch("stats");
        let lake_dir = dir.join("lake");
        crate::save_store(&fixture_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, report) = build(&lake, None).unwrap();
        assert_eq!(report.tables, 2);
        let db = Arc::clone(&attachment);

        let mut store = PinkDb::new();
        store
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        // COUNT(*) over the attached table: footer rows, no decode.
        assert_eq!(
            query(&mut store, "SELECT COUNT(*) FROM t;"),
            vec![vec![Value::Bigint(3)]]
        );
        assert_eq!(db.loads(), 0, "COUNT(*) must not decode the data file");
        // MIN/MAX over the integer column: exact stats, no decode.
        assert_eq!(
            query(&mut store, "SELECT MIN(i), MAX(i) FROM t;"),
            vec![vec![Value::Integer(-5), Value::Integer(9)]]
        );
        assert_eq!(db.loads(), 0, "MIN/MAX must not decode the data file");
        // COUNT(v) skips the one NULL row: exact NULL count, no decode.
        assert_eq!(
            query(&mut store, "SELECT COUNT(v) FROM t;"),
            vec![vec![Value::Bigint(2)]]
        );
        assert_eq!(db.loads(), 0, "COUNT(col) must not decode the data file");
        // A filtered scan materializes the table exactly once.
        let rows = query(&mut store, "SELECT COUNT(*) FROM t WHERE i > 0;");
        assert_eq!(db.loads(), 1);
        assert_eq!(db.files_decoded(), 1, "the one data file decoded");
        assert_eq!(rows, vec![vec![Value::Bigint(1)]]);
        let _ = query(&mut store, "SELECT SUM(i) FROM t;");
        assert_eq!(db.loads(), 1, "materialized tables are reused");
        assert_eq!(db.files_decoded(), 1, "no extra file decode");
        // The empty table answers counts and NULL min/max without decoding.
        assert_eq!(
            query(
                &mut store,
                "SELECT COUNT(*), COUNT(x), MIN(x), MAX(x) FROM empty;"
            ),
            vec![vec![
                Value::Bigint(0),
                Value::Bigint(0),
                Value::Null,
                Value::Null
            ]]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The descriptors' statistics are exactly what the decoded table would
    /// answer, for every column of an adversarial fixture.
    #[test]
    fn footer_stats_match_decoded_answers() {
        let dir = scratch("equiv");
        let lake_dir = dir.join("lake");
        crate::save_store(&fixture_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();
        let mut store = PinkDb::new();
        store
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        let tables = attachment.tables().to_vec();
        for table in tables {
            // Decode once (this is the ground truth), then compare every
            // provable statistic against the decoded table.
            let decoded = attachment.load(&table.db, &table.table).unwrap();
            assert_eq!(
                decoded.num_rows() as u64,
                table.rows,
                "{}.{}",
                table.db,
                table.table
            );
            for (i, (name, dtype)) in table.schema.iter().enumerate() {
                let stats = &table.stats[i];
                let mut nulls = 0u64;
                let mut min: Option<Value> = None;
                let mut max: Option<Value> = None;
                for row in 0..decoded.num_rows() {
                    let v = decoded.get(row, i).unwrap();
                    if v.is_null() {
                        nulls += 1;
                        continue;
                    }
                    let v = v.clone();
                    if min.as_ref().is_none_or(|m| compare(&v, m).is_lt()) {
                        min = Some(v.clone());
                    }
                    if max.as_ref().is_none_or(|m| compare(&v, m).is_gt()) {
                        max = Some(v);
                    }
                }
                if let Some(n) = stats.null_count {
                    assert_eq!(n, nulls, "{}.{name} NULL count", table.table);
                }
                if decoded.num_rows() == 0 || nulls as usize == decoded.num_rows() {
                    // The executor derives NULL without the statistic; a
                    // statistic must not disagree.
                    if let Some(v) = &stats.min {
                        assert!(v.is_null(), "{}.{name} min", table.table);
                    }
                    if let Some(v) = &stats.max {
                        assert!(v.is_null(), "{}.{name} max", table.table);
                    }
                } else {
                    if let Some(v) = &stats.min {
                        assert_eq!(
                            Some(v),
                            min.as_ref(),
                            "{}.{name} ({dtype}) min",
                            table.table
                        );
                    }
                    if let Some(v) = &stats.max {
                        assert_eq!(
                            Some(v),
                            max.as_ref(),
                            "{}.{name} ({dtype}) max",
                            table.table
                        );
                    }
                }
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
        pinkdb::plan::compare_values(a, b)
    }

    /// A v1 entry's block is bound to its catalog key, data file and row
    /// count: a valid-CRC block that names a different table, file, or row
    /// count is refused, never attached under the wrong entry.
    #[test]
    fn pink_files_cross_check_their_statistics_block() {
        use crate::catalog::{FileFormat, FileRef, TableEntry};
        use crate::stats::StatsBlock;

        let saved = pinkdb::persist::SavedTableStats {
            db: "main".to_string(),
            table: "t".to_string(),
            rows: 3,
            columns: vec![pinkdb::persist::SavedColumnStats {
                name: "i".to_string(),
                dtype: pinkdb::DataType::Integer,
                null_count: 0,
                min: Some(Value::Integer(-5)),
                max: Some(Value::Integer(9)),
                nan_count: 0,
            }],
        };
        // A v4 entry whose single file carries `block` and declares 3 rows.
        let entry = |block: Option<StatsBlock>| TableEntry {
            files: vec![FileRef::Entry {
                path: "data/main.t/x.pink".to_string(),
                rows: 3,
                stats: block,
                partition: None,
            }],
            rows: 3,
            format: Some(FileFormat::Pink),
            stats: None,
            deletes: Vec::new(),
            partition_by: None,
        };
        let path = std::path::Path::new("snapshots/2.json");
        let key = "main.t";

        // A consistent block attaches.
        let block = StatsBlock::from_saved(key, "data/main.t/x.pink", &saved).unwrap();
        let e = entry(Some(block));
        let (schema, stats) = pink_file_stats(key, &e, &e.files[0], path).unwrap();
        assert_eq!(schema[0].0, "i");
        assert_eq!(stats[0].min, Some(Value::Integer(-5)));

        // A block of another table (resealed: the CRC alone cannot tell).
        let mut block = StatsBlock::from_saved("main.other", "data/main.t/x.pink", &saved).unwrap();
        block.seal();
        let e = entry(Some(block));
        let err = pink_file_stats(key, &e, &e.files[0], path)
            .unwrap_err()
            .to_string();
        assert!(err.contains("statistics block of \"main.other\""), "{err}");

        // A block naming another data file.
        let mut block = StatsBlock::from_saved(key, "data/main.t/other.pink", &saved).unwrap();
        block.seal();
        let e = entry(Some(block));
        let err = pink_file_stats(key, &e, &e.files[0], path)
            .unwrap_err()
            .to_string();
        assert!(err.contains("names data file"), "{err}");

        // A row-count disagreement (a resealed truncated block): the entry
        // says 3 rows, the block says 2.
        let shorter = pinkdb::persist::SavedTableStats {
            rows: 2,
            ..saved.clone()
        };
        let mut block = StatsBlock::from_saved(key, "data/main.t/x.pink", &shorter).unwrap();
        block.seal();
        let e = entry(Some(block));
        let err = pink_file_stats(key, &e, &e.files[0], path)
            .unwrap_err()
            .to_string();
        assert!(err.contains("declares 2 rows"), "{err}");
        assert!(err.contains("entry says 3"), "{err}");

        // A file without a block is refused with the clear stats-less message
        // (contains the `.lake open` pointer).
        let e = entry(None);
        let err = pink_file_stats(key, &e, &e.files[0], path)
            .unwrap_err()
            .to_string();
        assert!(err.contains("without a statistics block"), "{err}");
        assert!(err.contains("use .lake open"), "{err}");

        // The v1 legacy shape: a plain path with the entry-level block.
        let block = StatsBlock::from_saved(key, "data/main.t/x.pink", &saved).unwrap();
        let v1 = TableEntry {
            files: vec![FileRef::Path("data/main.t/x.pink".to_string())],
            rows: 3,
            format: None,
            stats: Some(block),
            deletes: Vec::new(),
            partition_by: None,
        };
        let (schema, stats) = pink_file_stats(key, &v1, &v1.files[0], path).unwrap();
        assert_eq!(schema[0].0, "i");
        assert_eq!(stats[0].max, Some(Value::Integer(9)));
    }

    /// A multi-block, all-types table for the projection tests: 20,000 rows
    /// (ten engine chunks over several stored blocks, so decodes exercise
    /// ragged block tails and multi-block merges), NULLs, NaN payloads,
    /// `-0.0`, extremes and long strings.
    ///
    /// Projection pushdown exists for Vortex data files only: a v1 `.pink`
    /// entry has no projected reader, so those tests run in the
    /// `vortex-format` build (the lean `.pink` behavior is pinned by
    /// `tests/lake_lite.rs` and by the format-agnostic tests above).
    #[cfg(feature = "vortex-format")]
    fn projection_store() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, b BOOLEAN);")
            .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            let mut next = move || {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                state
            };
            for row in 0..20_000i64 {
                let r = next();
                let d = if row % 599 == 0 {
                    Value::Double(f64::from_bits(0x7ff8_0000_cafe_0001))
                } else if row == 3 {
                    Value::Double(-0.0)
                } else {
                    Value::Double((r % 4000) as f64 / 8.0 - 250.0)
                };
                t.append_row(vec![
                    if r % 23 == 0 {
                        Value::Null
                    } else {
                        Value::Integer((r % 500) as i32 - 250)
                    },
                    Value::Bigint((r as i64).wrapping_sub(i64::MAX / 2)),
                    d,
                    if r % 31 == 0 {
                        Value::Varchar("z".repeat((r % 500) as usize))
                    } else {
                        Value::Varchar(format!("k{}-{}", r % 100, row % 7))
                    },
                    Value::Boolean(r % 2 == 0),
                ])
                .unwrap();
            }
        }
        db
    }

    /// Row-for-row equality with doubles compared bit-exactly.
    #[cfg(feature = "vortex-format")]
    fn assert_same_rows(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
        assert_eq!(a.len(), b.len(), "{what}: row count");
        for (ra, rb) in a.iter().zip(b) {
            assert_eq!(ra.len(), rb.len(), "{what}: column count");
            for (va, vb) in ra.iter().zip(rb) {
                match (va, vb) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "{what}: {va:?} vs {vb:?}")
                    }
                    _ => assert_eq!(va, vb, "{what}"),
                }
            }
        }
    }

    /// A projected read decodes exactly the requested columns, is
    /// value-identical (doubles by bit pattern) to the full decode for those
    /// columns, keeps the full decode's chunking in every column, and shares
    /// already-decoded columns across later projections.
    #[cfg(feature = "vortex-format")]
    #[test]
    fn projected_reads_equal_full_reads_and_decode_only_requested_columns() {
        let dir = scratch("projection_values");
        let lake_dir = dir.join("lake");
        crate::save_store(&projection_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();

        // Ground truth: the full decode (what `.lake open` materializes).
        let full = attachment.load("main", "t").unwrap();
        assert_eq!(full.num_rows(), 20_000);
        assert!(full.num_chunks() > 1, "the fixture must be multi-chunk");

        // A sparse projection after the full load: nothing new to decode.
        let projected = attachment
            .load_projected("main", "t", Some(&[0, 3]))
            .unwrap();
        assert_eq!(attachment.loads(), 1, "the full load already decoded");
        assert_eq!(attachment.columns_decoded(), 5);
        assert_eq!(projected.present, vec![true; 5]);
        assert_eq!(projected.table.schema(), full.schema());
        assert_eq!(projected.table.num_rows(), full.num_rows());

        // A fresh attachment, so the first decode is the projection itself:
        // exactly columns 1 and 4, with every other column a NULL
        // placeholder of the same chunk lengths.
        let (solo, _) = build(&lake, None).unwrap();
        let sparse = solo.load_projected("main", "t", Some(&[1, 4])).unwrap();
        assert_eq!(sparse.present, vec![false, true, false, false, true]);
        assert_eq!(solo.loads(), 1);
        assert_eq!(solo.columns_decoded(), 2, "only columns 1 and 4 decoded");
        assert_eq!(sparse.table.num_rows(), full.num_rows());
        assert_eq!(sparse.table.num_chunks(), full.num_chunks());
        for j in 0..full.num_chunks() {
            for i in 0..full.num_cols() {
                assert_eq!(
                    sparse.table.columns[i].chunks[j].len(),
                    full.columns[i].chunks[j].len(),
                    "chunk {j} column {i} length"
                );
            }
        }
        for (i, decoded) in [
            (0usize, false),
            (1, true),
            (2, false),
            (3, false),
            (4, true),
        ] {
            for row in 0..full.num_rows() {
                let (a, b) = (sparse.table.get(row, i).unwrap(), full.get(row, i).unwrap());
                if decoded {
                    match (&a, &b) {
                        (Value::Double(x), Value::Double(y)) => assert_eq!(
                            x.to_bits(),
                            y.to_bits(),
                            "column {i} row {row}: {a:?} vs {b:?}"
                        ),
                        _ => assert_eq!(a, b, "column {i} row {row}"),
                    }
                } else {
                    assert!(a.is_null(), "an undecoded column is a NULL placeholder");
                }
            }
        }

        // Extending decodes only the missing columns and keeps the decoded
        // ones shared (no re-decode, no copy).
        let extended = solo
            .load_projected("main", "t", Some(&[0, 1, 2, 3, 4]))
            .unwrap();
        assert_eq!(solo.loads(), 2);
        assert_eq!(solo.columns_decoded(), 5);
        assert_eq!(extended.present, vec![true; 5]);
        for i in [1usize, 4] {
            for j in 0..full.num_chunks() {
                assert!(
                    Arc::ptr_eq(
                        &sparse.table.columns[i].chunks[j],
                        &extended.table.columns[i].chunks[j]
                    ),
                    "column {i} chunk {j} must be shared across projections"
                );
            }
        }
        // Every value now equals the full decode bit for bit.
        for i in 0..full.num_cols() {
            for row in 0..full.num_rows() {
                let (a, b) = (
                    extended.table.get(row, i).unwrap(),
                    full.get(row, i).unwrap(),
                );
                match (&a, &b) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "column {i} row {row}")
                    }
                    _ => assert_eq!(a, b, "column {i} row {row}"),
                }
            }
        }

        // An empty projection decodes column 0 only (the chunk shape has to
        // come from somewhere) and never panics.
        let (empty, _) = build(&lake, None).unwrap();
        let none = empty.load_projected("main", "t", Some(&[])).unwrap();
        assert_eq!(none.present, vec![true, false, false, false, false]);
        assert_eq!(empty.columns_decoded(), 1);
        assert_eq!(none.table.num_rows(), 20_000);
        assert_eq!(none.table.num_chunks(), full.num_chunks());

        // An out-of-range projection is unprovable: every column decodes.
        let (oob, _) = build(&lake, None).unwrap();
        let all = oob.load_projected("main", "t", Some(&[9])).unwrap();
        assert_eq!(all.present, vec![true; 5]);
        assert_eq!(oob.columns_decoded(), 5);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The engine's attached scan path drives `load_projected`: each query
    /// decodes its referenced columns only, extensions are incremental, the
    /// fast paths decode nothing at all, and every answer equals the
    /// in-memory execution over the same rows.
    #[cfg(feature = "vortex-format")]
    #[test]
    fn queries_decode_only_their_referenced_columns() {
        let dir = scratch("projection_queries");
        let lake_dir = dir.join("lake");
        crate::save_store(&projection_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();
        let mut store = PinkDb::new();
        store
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        // The same rows in an ordinary store: the ground-truth answers.
        let mut plain = projection_store();

        let mut check = |store: &mut PinkDb, sql: &str, decoded_after: u64| {
            let (attached, plain) = (query(store, sql), query(&mut plain, sql));
            assert_eq!(
                attachment.columns_decoded(),
                decoded_after,
                "{sql}: columns decoded"
            );
            assert_same_rows(&attached, &plain, sql);
        };

        // Fast paths: statistics only, zero columns decoded.
        check(&mut store, "SELECT COUNT(*) FROM t;", 0);
        check(&mut store, "SELECT MIN(i), MAX(i), COUNT(v) FROM t;", 0);
        check(
            &mut store,
            "SELECT MIN(b), MAX(b), MIN(g), MAX(g) FROM t;",
            0,
        );
        // `d` holds NaN payloads: its MIN/MAX cannot come from statistics,
        // so the whole query materializes — only `d`'s column.
        check(&mut store, "SELECT MIN(d), MAX(d) FROM t;", 1);
        // Each fallback decodes exactly the referenced column; repeats of an
        // already-decoded column decode nothing.
        check(&mut store, "SELECT SUM(i) FROM t;", 2);
        check(&mut store, "SELECT AVG(i) FROM t;", 2);
        // A filter decodes the predicate's column (already decoded here).
        check(&mut store, "SELECT COUNT(*) FROM t WHERE i > 0;", 2);
        // Order-by + limit: the sort key and the projected column.
        check(&mut store, "SELECT g FROM t ORDER BY g DESC LIMIT 3;", 3);
        check(&mut store, "SELECT v FROM t ORDER BY v LIMIT 5;", 4);
        // GROUP BY aggregate: the key.
        check(
            &mut store,
            "SELECT b, COUNT(*) FROM t GROUP BY b ORDER BY b;",
            5,
        );
        check(&mut store, "SELECT SUM(i), MIN(g), COUNT(v) FROM t;", 5);
        // A window over projected columns.
        check(
            &mut store,
            "SELECT i, ROW_NUMBER() OVER (PARTITION BY b ORDER BY i) FROM t LIMIT 5;",
            5,
        );
        // `SELECT *` and expressions over several columns: every column is
        // already in, so nothing decodes.
        check(&mut store, "SELECT * FROM t ORDER BY i LIMIT 2;", 5);
        check(&mut store, "SELECT i + g + d FROM t LIMIT 2;", 5);
        // The counter only ever grows; the decode count stops growing once
        // every column is materialized.
        assert_eq!(
            attachment.loads(),
            5,
            "one decode per newly referenced column set"
        );
        let loads = attachment.loads();
        check(&mut store, "SELECT SUM(i), SUM(d) FROM t;", 5);
        assert_eq!(
            attachment.loads(),
            loads,
            "no decode once every column is in"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A missing data file fails attach with a clean error.
    #[test]
    fn missing_data_file_fails_attach() {
        let dir = scratch("missing");
        let lake_dir = dir.join("lake");
        crate::save_store(&fixture_store(), &lake_dir).unwrap();
        // Remove every data file the manifest references.
        for entry in walk(&lake_dir.join("data")) {
            std::fs::remove_file(&entry).unwrap();
        }
        let lake = Lake::open(&lake_dir).unwrap();
        let err = build(&lake, None).unwrap_err().to_string();
        assert!(
            err.contains("is missing") || err.contains("loading data file"),
            "{err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
        out
    }
}
