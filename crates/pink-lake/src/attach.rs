//! `.lake attach`: a session-local, read-only, statistics-only view of one
//! lake.
//!
//! Attaching reads the catalog + the chosen manifest (exactly what
//! [`Lake::load`] reads first) and then the per-file statistics blocks the
//! manifest publishes — no data file is opened and no column is decoded, and
//! an encrypted lake reads nothing but the catalog database. Each block
//! carries a file's schema and exact per-column statistics and is
//! CRC-protected against its key, path and row count (see [`crate::stats`]);
//! a file published without one is refused with a clear message.
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
//! # Unavailable formats (wave 88, rule 5)
//!
//! An entry whose format id has no registered provider (an unknown id) does
//! not refuse the lake.
//! Attach lists the table in its descriptors — schema from the v5 record, or
//! from the manifest's statistics block, so the planner can bind it — but
//! every read of it (a scan, a projection, or any `COUNT`/`MIN`/`MAX` fast
//! path that falls back) returns the provider error naming the table and the
//! id. The descriptor advertises [`crate::UNAVAILABLE_ROWS`], a row count the
//! engine's statistics fast paths cannot represent, which forces those fast
//! paths onto the loader (and thus onto the error) instead of answering.
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
//! exact, and neither is an approximation. A pruned load returns its own row
//! set; nothing decoded is kept between queries (wave 133a removed the
//! session-wide materialized table), so a pruned and an unpruned query cost
//! what each one reads and nothing more.
//!
//! # Streamed scans (wave 133a)
//!
//! Every data scan streams. [`LakeAttachment::scan_streamed_io`] walks the
//! surviving files, has each format push one morsel (a row group, or the
//! pages a predicate leaves) at a time, and folds each morsel into the
//! engine's sink — so peak memory is a morsel, not the table. Surviving files
//! are read with at most [`crate::parallel_limit`] in flight (ordered scans
//! stay serial, because the sink must see file order then row order), and the
//! scan returns [`ScanIo`]: files, row groups and pages read versus pruned,
//! plus the transport requests and bytes it issued. A format that cannot
//! stream (or a file with positional deletes, whose positions address the
//! whole file) decodes as one morsel through the same path, so answers are
//! identical either way.
//!
//! # Counters
//!
//! [`pinkdb::AttachedLake::decode_stats`] reports `(loads, columns decoded,
//! files decoded)` over the attachment and its time-travel views — the
//! observability behind "a statistics-only query decodes nothing" and "a
//! pruned scan decodes only the matching files". Wave 98b adds
//! [`AttachmentReport::requests`] / [`AttachmentReport::bytes`] /
//! [`AttachmentReport::wait_nanos`] (what attach itself fetched) and
//! `Lake::transport_stats()` (the running totals of one handle), so the
//! request/byte pattern is visible without a packet capture.
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
    AtSpec, AttachedLake, AttachedTable, ColumnStats, DataChunk, DataTable, PinkDbError, Value,
};

use crate::catalog::{PartitionKey, TableEntry};
use crate::format::{DataFile, LakeFormat};
use crate::incremental::entry_layout;
use crate::inlined::InlinedFact;
use crate::storage::Storage;
use crate::zone::{self, FileFacts};
use crate::{
    Lake, LakeError, Result, apply_file_deletes, copy_chunks, manifest_for, only_table,
    published_versions, split_key, validate_rel_path,
};

/// What [`Lake::attach`] read and pinned.
#[derive(Debug)]
pub struct AttachmentReport {
    /// The manifest version the attachment is pinned to (the latest, or the
    /// one asked for).
    pub version: u64,
    /// Number of attached tables.
    pub tables: usize,
    /// The lake's catalog `format_version` (7 in this build).
    pub format_version: u32,
    /// True when the lake is encrypted at rest: the catalog and statistics
    /// blocks were unsealed for this attachment. No data file is read either
    /// way, so this is never a decode.
    pub encrypted: bool,
    /// Transport requests this attach issued (wave 98b diagnostics): the
    /// catalog/manifest reads. Local lakes report zero.
    pub requests: u64,
    /// Response bytes this attach received. Local lakes report 0.
    pub bytes: u64,
    /// Wall-clock nanoseconds this attach spent waiting on the transport.
    pub wait_nanos: u64,
    /// True when the backend supports positioned reads and the attachment
    /// used them (object storage, or a local plaintext lake). False for an
    /// encrypted lake (a sealed object has no plaintext ranges).
    pub ranged: bool,
    /// The bounded in-flight limit the attach used for concurrent reads
    /// (`PINKDB_HTTPFS_PARALLELISM`, 1 = serial).
    pub parallelism: usize,
    /// The read-only handle to register with the engine
    /// ([`pinkdb::PinkDb::attach_lake`]).
    pub lake: Arc<dyn AttachedLake>,
}

/// A `.pink` file's schema and gated statistics, as attach wants them.
type SchemaAndStats = (
    Vec<(String, pinkdb::DataType)>,
    Vec<ColumnStats>,
    Vec<Vec<crate::stats::StatsPath>>,
);

/// One data file of an attached table: its exact row count, the physical
/// positions its delete files drop (merge-on-read), its per-column zone map
/// (when any metadata proves one), and its recorded partition values.
#[derive(Debug, Clone)]
struct AttachedFile {
    rel: String,
    rows: u64,
    /// The table schema epoch this file was written under (v5 entries).
    schema_version: u64,
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
    /// The file's per-path VARIANT statistics (`v['a']` pruning); empty when
    /// no metadata carries them.
    paths: Vec<pinkdb::PathStats>,
    /// The file's partition value map, keyed by
    /// [`crate::catalog::PartitionKey::key_text`]; not necessarily aligned
    /// with the table's current spec (a file written before the spec was set
    /// carries none; a spec change does not rewrite old files).
    partition: Option<std::collections::BTreeMap<String, Option<String>>>,
    /// Where the file's footer is (wave 102, `.pink` data files): recorded by
    /// the catalog so a projected read fetches the footer without probing
    /// the trailer. `None` for formats the catalog does not locate this way.
    footer: Option<crate::format::FooterLocation>,
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
    /// The registered provider this entry's data files are read with (the
    /// catalog's format id resolved through [`crate::format`]).
    format: Arc<dyn LakeFormat>,
    /// Whether the provider decodes column projections. `false` means a
    /// projected request materializes the whole table.
    projected: bool,
    /// The v5 schema record of this version's entry (wave 65); `None` for a
    /// legacy entry, whose files carry the schema themselves and must agree.
    record: Option<crate::schema::TableSchema>,
    /// Wave 68: the **live** inlined row versions, in row-id order — unioned
    /// after every data file (see `crate::inlined` for the scan-order and
    /// decode rules). Empty for a table that never inlined.
    inlined: Vec<crate::catalog::InlinedRow>,
}

impl AttachedEntry {
    /// The table's key for error messages.
    fn key(&self) -> String {
        format!("{}.{}", self.db, self.table)
    }
}

/// How many file footers one attachment keeps: a projected scan decodes a
/// file's footer only when the catalog's schema record is older than the
/// file, so a hot table otherwise re-reads the same footer every scan.
const FOOTER_CACHE_MAX: usize = 64;

/// What a streamed file needs from its attachment: where the data lives and
/// the footer cache shared across the attachment's life.
struct FileIo<'a> {
    storage: &'a Arc<dyn Storage>,
    root: &'a std::path::Path,
    footers: &'a FooterCache,
}

/// Per-attachment footer cache, keyed by `(relative path, row count)`.
///
/// The row count stands in for the file's identity: a data file is
/// immutable, so `(path, rows)` names one file for the attachment's life
/// (a re-written path with different rows is a different file).
type FooterCache = Mutex<HashMap<(String, u64), Option<crate::format::FileFooter>>>;

/// Test-only gauge of concurrent file streams: a scan's in-flight bound is
/// asserted against this instead of sampling RSS, which is noisy and slow.
#[cfg(test)]
pub(crate) mod inflight {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) static LIVE: AtomicUsize = AtomicUsize::new(0);
    pub(crate) static PEAK: AtomicUsize = AtomicUsize::new(0);

    /// One file stream in flight for the guard's lifetime.
    pub(crate) struct InFlight;

    impl InFlight {
        pub(crate) fn enter() -> Self {
            let live = LIVE.fetch_add(1, Ordering::SeqCst) + 1;
            PEAK.fetch_max(live, Ordering::SeqCst);
            Self
        }

        pub(crate) fn reset() {
            PEAK.store(LIVE.load(Ordering::SeqCst), Ordering::SeqCst);
        }
    }

    impl Drop for InFlight {
        fn drop(&mut self) {
            LIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// What one streamed scan read and what it pruned — the accounting a
/// `EXPLAIN`-style surface reports (wave 133a).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanIo {
    /// Data files this scan opened and data files the catalog statistics
    /// proved cannot match.
    pub files: u64,
    pub files_pruned: u64,
    /// Row groups decoded and row groups whose statistics proved no match.
    pub row_groups: u64,
    pub row_groups_pruned: u64,
    /// Data pages read and pages whose statistics proved no match (a format
    /// without a page index reports zero).
    pub pages: u64,
    pub pages_pruned: u64,
    /// Transport requests and response bytes this scan issued, measured as
    /// the backend's counters advanced (0 for a local lake).
    pub requests: u64,
    pub bytes: u64,
}

impl ScanIo {
    fn add(&mut self, other: &crate::format::ScanCounters) {
        self.row_groups += other.row_groups;
        self.row_groups_pruned += other.row_groups_pruned;
        self.pages += other.pages;
        self.pages_pruned += other.pages_pruned;
    }
}

/// One attached lake, as the engine sees it.
#[derive(Debug)]
pub(crate) struct LakeAttachment {
    location: String,
    /// The catalog location this attachment's errors name (redacted URL).
    catalog: String,
    store: Arc<dyn crate::metadata::MetadataStore>,
    storage: Arc<dyn Storage>,
    root: std::path::PathBuf,
    /// The lake identity this attachment's manifests must carry (M-7).
    lake_id: Option<String>,
    /// lowercased `(db, table)` → the data files and how to decode them.
    files: HashMap<(String, String), AttachedEntry>,
    /// Wave 88, rule 5: lowercased `(db, table)` → the reason this build
    /// cannot read the table's data-file format. The table is still listed
    /// (with [`crate::UNAVAILABLE_ROWS`] as its advertised row count, so the
    /// engine's statistics fast paths fall back to the loader), but every
    /// load returns this message — naming the table and the format id —
    /// instead of touching a file.
    unavailable: HashMap<(String, String), String>,
    tables: Vec<AttachedTable>,
    /// File footers decoded by projected scans, bounded by
    /// [`FOOTER_CACHE_MAX`].
    footers: FooterCache,
    /// What the last [`LakeAttachment::scan_streamed_io`] call read.
    last_scan: Mutex<Option<ScanIo>>,
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

impl LakeAttachment {
    /// The catalog location, for error attribution.
    fn catalog_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.catalog)
    }

    #[cfg(test)]
    pub(crate) fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
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
        if requested.iter().all(|&w| !w)
            && let Some(first) = requested.first_mut()
        {
            *first = true;
        }
        requested
    }

    /// What this attachment's last [`LakeAttachment::scan_streamed_io`] read
    /// and pruned (`None` before the first streamed scan).
    /// What the most recent [`Self::scan_streamed_io`] call read and pruned
    /// (the surfaced form of [`ScanIo`]).
    #[allow(dead_code)] // wave 133b reads it; nothing in this crate does yet.
    pub fn last_scan(&self) -> Option<ScanIo> {
        *self.last_scan.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// [`AttachedLake::scan_streamed`] with ordering control and per-scan I/O
    /// accounting — the streamed path every lake scan takes (wave 133a).
    ///
    /// `ordered` pushes chunks in manifest order, file by file, so row order
    /// is stable; a caller asks for it when order is observable (a positional
    /// `LIMIT`, `EXCEPT`, or a sort key the planner cannot sort above). An
    /// unordered scan reads up to `crate::parallel_limit()` files at once and
    /// pushes each morsel as it decodes.
    ///
    /// Data files are streamed by the format provider one morsel at a time
    /// (a row group, or page range); a file the provider cannot stream
    /// (positional deletes, or no projected reader) is one morsel holding the
    /// whole file. Either way the scan holds only the in-flight morsels.
    /// [`pinkdb::AttachedLake::scan_streamed`] with the accounting and the
    /// ordering choice: `ordered` keeps file order then row order (serial,
    /// because the sink sees one file at a time); `false` reads surviving
    /// files with bounded parallelism and pushes morsels as they finish.
    /// Returns the rows pushed and what the scan read and pruned.
    pub fn scan_streamed_io(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
        ordered: bool,
        sink: &mut dyn FnMut(DataChunk) -> std::result::Result<(), PinkDbError>,
    ) -> std::result::Result<(u64, ScanIo), PinkDbError> {
        self.stream_scan(db, table, columns, predicates, ordered, sink)
    }

    /// The scan proper. A sink failure is the engine's own error and must come
    /// back unchanged (not wrapped as "lake data error"); everything else the
    /// scan raises keeps its lake context. Recording the sink's error here is
    /// what tells the two apart: any later failure is that failure's
    /// consequence, because the scan stops pushing as soon as it happens.
    fn stream_scan(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
        ordered: bool,
        sink: &mut dyn FnMut(DataChunk) -> std::result::Result<(), PinkDbError>,
    ) -> std::result::Result<(u64, ScanIo), PinkDbError> {
        let failed = std::cell::RefCell::new(None::<PinkDbError>);
        let mut record = |chunk: DataChunk| -> std::result::Result<(), PinkDbError> {
            match sink(chunk) {
                Ok(()) => Ok(()),
                Err(error) => {
                    failed.borrow_mut().get_or_insert(error);
                    Err(PinkDbError::Execution(
                        "the lake scan stopped pushing rows".to_string(),
                    ))
                }
            }
        };
        let scanned = self.stream_scan_inner(db, table, columns, predicates, ordered, &mut record);
        match scanned {
            Ok(ok) => Ok(ok),
            Err(_) if failed.borrow().is_some() => Err(failed.into_inner().expect("checked above")),
            Err(other) => Err(PinkDbError::Execution(other.to_string())),
        }
    }

    fn stream_scan_inner(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
        ordered: bool,
        sink: &mut dyn FnMut(DataChunk) -> std::result::Result<(), PinkDbError>,
    ) -> Result<(u64, ScanIo)> {
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        if let Some(message) = self.unavailable.get(&key) {
            return Err(LakeError::Engine {
                context: format!("lake {}", self.location),
                source: PinkDbError::Execution(message.clone()),
            });
        }
        let Some(entry) = self.files.get(&key).cloned() else {
            return Err(LakeError::Engine {
                context: format!("lake {}", self.location),
                source: PinkDbError::Catalog(format!(
                    "lake {} has no table \"{db}.{table}\"",
                    self.location
                )),
            });
        };
        let requested = self.requested_columns(&key, columns);
        if requested.is_empty() {
            return Err(LakeError::Engine {
                context: format!("lake {}", self.location),
                source: PinkDbError::Catalog(format!(
                    "lake {} table \"{db}.{table}\" has no columns",
                    self.location
                )),
            });
        }
        let survivors = file_survivors(&entry, predicates);
        let mut io = ScanIo {
            files: survivors.len() as u64,
            files_pruned: (entry.files.len() - survivors.len()) as u64,
            ..ScanIo::default()
        };
        self.loads.fetch_add(1, Ordering::Relaxed);
        self.files_decoded
            .fetch_add(survivors.len() as u64, Ordering::Relaxed);
        let decoded_count = if entry.projected {
            requested.iter().filter(|&&w| w).count() as u64
        } else {
            requested.len() as u64
        };
        self.columns_decoded
            .fetch_add(decoded_count, Ordering::Relaxed);
        let full = !entry.projected || requested.iter().all(|&w| w);
        let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
        let columns = if full { None } else { Some(decoded.as_slice()) };
        // The backend's counters, so this scan's share of requests and bytes
        // is measurable (a local lake reports zero).
        let before = self.storage.stats();
        let mut rows = 0u64;
        let files = FileIo {
            storage: &self.storage,
            root: &self.root,
            footers: &self.footers,
        };
        if ordered || survivors.len() <= 1 {
            for file in &survivors {
                let (file_rows, counters) =
                    stream_file(&files, &entry, file, columns, predicates, sink)?;
                rows += file_rows;
                io.add(&counters);
            }
        } else {
            let (file_rows, counters) =
                stream_files_parallel(&files, &entry, &survivors, columns, predicates, sink)?;
            rows += file_rows;
            io.add(&counters);
        }
        if !entry.inlined.is_empty() {
            let record = entry.record.as_ref().ok_or_else(|| LakeError::Corrupt {
                path: self.catalog_path().to_path_buf(),
                message: format!(
                    "table \"{}\" carries inlined rows but no schema record",
                    entry.key()
                ),
            })?;
            let wanted = requested_mask(entry.schema.len(), columns);
            let (table, _present) = crate::inlined::materialize_live(
                &entry.inlined,
                record,
                &entry.key(),
                &entry.table,
                Some(&wanted),
                self.catalog_path(),
            )?;
            for index in 0..table.num_chunks() {
                let chunk = table.scan_chunk(index).ok_or_else(|| LakeError::Internal {
                    message: format!("inlined chunk {index} of \"{}\" disappeared", entry.key()),
                })?;
                rows += chunk.num_rows() as u64;
                sink(chunk).map_err(|source| LakeError::Engine {
                    context: format!("scan of \"{}\"", entry.key()),
                    source,
                })?;
            }
        }
        let after = self.storage.stats();
        io.requests = after.requests.saturating_sub(before.requests);
        io.bytes = after.bytes.saturating_sub(before.bytes);
        *self.last_scan.lock().unwrap_or_else(|p| p.into_inner()) = Some(io);
        Ok((rows, io))
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
        if let Some(message) = self.unavailable.get(&key) {
            return Err(PinkDbError::Execution(message.clone()));
        }
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
        self.loads.fetch_add(1, Ordering::Relaxed);
        // A `.pink` projection decodes every column (the provider has no
        // projected reader); a projected provider decodes only these.
        let decoded_count = if entry.projected {
            requested.iter().filter(|&&w| w).count() as u64
        } else {
            requested.len() as u64
        };
        self.columns_decoded
            .fetch_add(decoded_count, Ordering::Relaxed);
        self.files_decoded
            .fetch_add(entry.files.len() as u64, Ordering::Relaxed);
        let full = !entry.projected || requested.iter().all(|&w| w);
        let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
        let (table, present) = self
            .load_entry(&entry, if full { None } else { Some(&decoded) })
            .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        Ok(AttachedScan {
            table: Arc::new(table),
            present,
        })
    }

    fn load_filtered(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
    ) -> std::result::Result<AttachedScan, PinkDbError> {
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        if let Some(message) = self.unavailable.get(&key) {
            return Err(PinkDbError::Execution(message.clone()));
        }
        if predicates.is_empty() {
            return self.load_projected(db, table, columns);
        }
        let Some(entry) = self.files.get(&key).cloned() else {
            return Err(PinkDbError::Catalog(format!(
                "lake {} has no table \"{db}.{table}\"",
                self.location
            )));
        };
        let survivors = file_survivors(&entry, predicates);
        if survivors.len() == entry.files.len() && !entry.format.prunes_row_groups() {
            // Nothing pruned and the provider cannot prune *inside* a file:
            // decode the table as the unfiltered load does.
            return self.load_projected(db, table, columns);
        }
        // Only the surviving files decode, and only the columns the query
        // references; nothing is kept for a later query.
        let requested = self.requested_columns(&key, columns);
        self.loads.fetch_add(1, Ordering::Relaxed);
        self.files_decoded
            .fetch_add(survivors.len() as u64, Ordering::Relaxed);
        let decoded_count = if !entry.projected {
            requested.len() as u64
        } else {
            requested.iter().filter(|&&w| w).count() as u64
        };
        self.columns_decoded
            .fetch_add(decoded_count, Ordering::Relaxed);
        let full = !entry.projected || requested.iter().all(|&w| w);
        let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
        let (table, present) = self
            .load_some(
                &entry,
                &survivors,
                if full { None } else { Some(&decoded) },
                predicates,
            )
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

    fn streams_scan(&self, db: &str, table: &str) -> bool {
        // Every scan streams: a statistics-only query decodes nothing, and a
        // data query folds one morsel per referenced column at a time, so a
        // table never has to fit in memory (wave 133a, replacing the wave 98c
        // materializing bound).
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        !self.unavailable.contains_key(&key) && self.files.contains_key(&key)
    }

    fn scan_streamed(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
        sink: &mut dyn FnMut(DataChunk) -> std::result::Result<(), PinkDbError>,
    ) -> std::result::Result<u64, PinkDbError> {
        Ok(self
            .scan_streamed_io(db, table, columns, predicates, false, sink)?
            .0)
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
        let versions = published_versions(&*self.store, self.pinned).map_err(catalog_error)?;
        let mut index = Vec::with_capacity(versions.len());
        for version in versions {
            match manifest_for(&*self.store, self.catalog_path(), Some(version)) {
                Ok((manifest, _))
                    if self.lake_id.is_none()
                        || manifest.format_version < 6
                        || manifest.lake_id.as_deref() == self.lake_id.as_deref() =>
                {
                    index.push((version, crate::time::parse_iso8601_utc(&manifest.updated)))
                }
                Ok(_) => continue,
                Err(LakeError::VersionNotFound { .. }) => continue,
                Err(LakeError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue;
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
    /// statistics-only attachment of that version's manifest — exactly what
    /// `.lake attach <dir> <version>` would build, so every
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
        let (attachment, _) = build_from(
            &self.store,
            &self.storage,
            &self.root,
            &self.catalog,
            Some(version),
            self.lake_id.as_deref(),
        )
        .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        views.insert(version, Arc::clone(&attachment));
        Ok(VersionView::Built(attachment))
    }
    /// Materialize one entry: every data file in manifest order, each with
    /// its positional deletes applied, unioned into one table. `columns`
    /// selects a projection for the files that support one; providers without
    /// a projected reader decode in full. The returned `present` flags match
    /// the union's columns.
    fn load_entry(
        &self,
        entry: &AttachedEntry,
        columns: Option<&[usize]>,
    ) -> Result<(DataTable, Vec<bool>)> {
        self.load_some(entry, &entry.files, columns, &[])
    }

    /// [`LakeAttachment::load_entry`] over a chosen subset of the entry's
    /// files (the survivors of file pruning). An empty subset decodes no data
    /// file and yields the live inlined rows (or an empty table with the
    /// entry's full schema when there are none): inlined rows are **never**
    /// pruned — file pruning only skips files.
    ///
    /// The inlined contribution's `present` mask always equals the files'
    /// (a full decode is all-present, a projected decode marks only the
    /// requested columns), so the chunks merge cleanly; a column the
    /// files did not decode is a NULL placeholder in the inlined block too.
    fn load_some(
        &self,
        entry: &AttachedEntry,
        files: &[AttachedFile],
        columns: Option<&[usize]>,
        filters: &[pinkdb::attached::PrunePredicate],
    ) -> Result<(DataTable, Vec<bool>)> {
        // Wave 98b: files of one table are independent, so decode them
        // concurrently (bounded by `PINKDB_HTTPFS_PARALLELISM`) and merge in
        // manifest order. `map_bounded` returns results in input order, so
        // the first error is the same one a serial loop would report.
        let decoded = crate::map_bounded(files, crate::parallel_limit(), |file| {
            load_file(
                &self.storage,
                &self.root,
                &self.footers,
                entry,
                file,
                columns,
                filters,
            )
        });
        let mut union: Option<(DataTable, Vec<bool>)> = None;
        for (i, result) in decoded.into_iter().enumerate() {
            let (table, present) = result?;
            let file = &files[i];
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
        if !entry.inlined.is_empty() {
            let record = entry.record.as_ref().ok_or_else(|| LakeError::Corrupt {
                path: self.catalog_path().to_path_buf(),
                message: format!(
                    "table \"{}\" carries inlined rows but no schema record",
                    entry.key()
                ),
            })?;
            let wanted: Vec<bool> = match &union {
                Some((_, present)) => present.clone(),
                None => requested_mask(entry.schema.len(), columns),
            };
            let (table, present) = crate::inlined::materialize_live(
                &entry.inlined,
                record,
                &entry.key(),
                &entry.table,
                Some(&wanted),
                self.catalog_path(),
            )?;
            match &mut union {
                None => union = Some((table, present)),
                Some((dst, first_present)) => {
                    if *first_present != present {
                        return Err(LakeError::Internal {
                            message: format!(
                                "attached inlined rows of \"{}\" disagree with the files on the \
                                 decoded columns",
                                entry.key()
                            ),
                        });
                    }
                    copy_chunks(&table, dst, "inlined rows")?;
                }
            }
        }
        match union {
            Some(union) => Ok(union),
            None => {
                let table =
                    DataTable::new(entry.table.clone(), entry.schema.clone()).map_err(|e| {
                        LakeError::Engine {
                            context: format!("preparing the empty scan of \"{}\"", entry.key()),
                            source: e,
                        }
                    })?;
                Ok((table, vec![true; entry.schema.len()]))
            }
        }
    }
}

/// The requested-column mask of one table load: `None`/unprovable = every
/// column; a projection marks exactly its indices (an empty one marks column
/// 0, the table's chunk shape). The same rule `requested_columns` applies.
/// The files of `entry` whose zone maps leave at least one predicate
/// satisfiable, in manifest order. Inlined rows are **never** pruned: file
/// pruning only skips data files.
fn file_survivors(
    entry: &AttachedEntry,
    predicates: &[pinkdb::PrunePredicate],
) -> Vec<AttachedFile> {
    entry
        .files
        .iter()
        .filter(|file| {
            let facts = FileFacts {
                rows: file.rows,
                zone: file.zone.as_deref(),
                partition: file.partition.as_ref(),
                paths: (!file.paths.is_empty()).then_some(file.paths.as_slice()),
            };
            !zone::file_prunes(facts, &entry.keys, &entry.schema, predicates)
        })
        .cloned()
        .collect()
}

/// One file's published path statistics as the engine-side values pruning
/// compares. A bound that does not decode is dropped rather than guessed:
/// the path then prunes less, never wrongly.
fn path_stats(paths: &[crate::stats::StatsPath]) -> Vec<pinkdb::PathStats> {
    let bound = |path: &crate::stats::StatsPath, text: &Option<String>| {
        text.as_deref()
            .and_then(|text| pinkdb::variant_shred::stats_bound(&path.dtype, text))
            .map(|value| pinkdb::Value::Variant(std::sync::Arc::new(value)))
    };
    paths
        .iter()
        .map(|path| pinkdb::PathStats {
            path: path.path.clone(),
            null_count: Some(path.nulls),
            min: bound(path, &path.min),
            max: bound(path, &path.max),
        })
        .collect()
}

fn requested_mask(n_cols: usize, columns: Option<&[usize]>) -> Vec<bool> {
    let mut wanted = match columns {
        Some(cols) if cols.iter().all(|&i| i < n_cols) => {
            let mut wanted = vec![false; n_cols];
            for &i in cols {
                wanted[i] = true;
            }
            wanted
        }
        _ => vec![true; n_cols],
    };
    if wanted.iter().all(|&w| !w)
        && let Some(first) = wanted.first_mut()
    {
        *first = true;
    }
    wanted
}

/// Decode one data file of `entry` and apply its positional deletes.
///
/// With a v5 schema record the file is *projected* onto the entry's logical
/// schema: a renamed column reads its physical predecessor, a column the file
/// predates becomes a constant (its default), a dropped column is skipped (its
/// bytes stay on disk — positions are unchanged, so delete files stay valid).
/// `columns` are logical column indices (`None` = every logical column); a
/// projected provider decodes only the physical columns a mapped logical
/// column needs, others decode whole and project in memory.
#[cfg_attr(not(test), allow(unused_variables))]
/// One data file's physical decode plan: what the file holds, how those
/// columns map onto the table's logical schema, which physical columns the
/// request must decode, and the pushed predicates in physical indices.
struct FilePlan {
    physical_schema: Vec<(String, pinkdb::DataType)>,
    mapping: crate::schema::FileMapping,
    decoded: Vec<usize>,
    filters: Vec<pinkdb::attached::PrunePredicate>,
}

/// Plan one data file's projected decode, or `None` when the file must be
/// decoded whole (a provider without a projected reader, or a schema the
/// catalog record and footer cannot prove).
fn file_plan(
    storage: &Arc<dyn Storage>,
    footers: &FooterCache,
    entry: &AttachedEntry,
    file: &AttachedFile,
    data_file: &DataFile<'_>,
    columns: Option<&[usize]>,
    filters: &[pinkdb::attached::PrunePredicate],
) -> Result<Option<FilePlan>> {
    if !entry.projected {
        // No projected reader exists for this provider (the engine `.pink`
        // bytes are the built-in case): any missing column materializes the
        // whole table through the provider's reader.
        return Ok(None);
    }
    // A projection-capable provider. With a v5 record whose epoch equals the
    // file's, the record already is the file's physical schema: no footer
    // read is needed to map columns (a projected scan still reads its footer
    // once to decode). Otherwise the footer is the authority; a provider
    // without one falls back to the always-correct full decode.
    let path = storage.abs(&file.rel);
    let need_footer = entry
        .record
        .as_ref()
        .is_none_or(|record| file.schema_version != record.version);
    let footer = if need_footer {
        footer_stats(entry, file, data_file, footers)?
    } else {
        None
    };
    let physical_schema: Vec<(String, pinkdb::DataType)> = match &footer {
        Some(footer) => footer.schema.clone(),
        None => match &entry.record {
            Some(record) if !need_footer => record.schema_pairs(),
            _ => return Ok(None),
        },
    };
    // Which physical columns the request needs. With a record, logical
    // indices are mapped through the file's schema epoch (a dropped column's
    // bytes are never read; a default-filled one needs no data).
    let mapping = match &entry.record {
        Some(schema) => schema.map_file(file.schema_version, &physical_schema, &path)?,
        None => crate::schema::FileMapping {
            physical: (0..physical_schema.len()).map(Some).collect(),
        },
    };
    let logical_width = mapping.physical.len();
    let wanted: Vec<bool> = match columns {
        Some(cols) => {
            let mut w = vec![false; logical_width];
            for &i in cols {
                if i < w.len() {
                    w[i] = true;
                }
            }
            w
        }
        None => vec![true; logical_width],
    };
    let mut physical: Vec<usize> = Vec::new();
    for (li, on) in wanted.iter().enumerate() {
        if *on
            && let Some(pi) = mapping.physical[li]
            && !physical.contains(&pi)
        {
            physical.push(pi);
        }
    }
    physical.sort_unstable();
    // The query's predicates use logical column indices; the provider's
    // ranged scan sees the file's physical schema. Translate (and drop a
    // predicate on a column this file does not physically carry) before
    // handing them over. Pruning is only ever an optimization, so dropping a
    // translation is safe; passing a wrong index is not.
    let filters: Vec<pinkdb::PrunePredicate> = filters
        .iter()
        .filter_map(|filter| {
            let physical = (*mapping.physical.get(filter.column)?)?;
            Some(pinkdb::PrunePredicate {
                column: physical,
                op: filter.op,
                value: filter.value.clone(),
                path: filter.path.clone(),
            })
        })
        .collect();
    // Decode at least one physical column: the row count has to come from
    // somewhere even when the request only needs default-filled columns.
    let decoded = if physical.is_empty() {
        vec![0]
    } else {
        physical
    };
    Ok(Some(FilePlan {
        physical_schema,
        mapping,
        decoded,
        filters,
    }))
}

/// The file's footer statistics, from the attachment's bounded cache when
/// this attachment has read them before.
fn footer_stats(
    entry: &AttachedEntry,
    file: &AttachedFile,
    data_file: &DataFile<'_>,
    footers: &FooterCache,
) -> Result<Option<crate::format::FileFooter>> {
    let cache_key = (file.rel.clone(), file.rows);
    let cached = footers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&cache_key)
        .cloned();
    if let Some(cached) = cached {
        return Ok(cached);
    }
    let footer = entry.format.footer_stats(data_file)?;
    let mut cache = footers.lock().unwrap_or_else(|p| p.into_inner());
    if cache.len() >= FOOTER_CACHE_MAX {
        cache.clear();
    }
    cache.insert(cache_key, footer.clone());
    Ok(footer)
}

/// Decode one data file whole through its provider, apply its positional
/// deletes and project it onto the table's logical schema.
fn load_whole_file(
    storage: &Arc<dyn Storage>,
    root: &std::path::Path,
    entry: &AttachedEntry,
    file: &AttachedFile,
    data_file: &DataFile<'_>,
) -> Result<(DataTable, Vec<bool>)> {
    let key = entry.key();
    let table = decode_via_provider(entry, data_file, root)?;
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
    match &entry.record {
        Some(schema) => {
            let mapping = schema.map_file(
                file.schema_version,
                &table.schema(),
                &storage.abs(&file.rel),
            )?;
            let all = vec![true; table.num_cols()];
            schema.project_table(&mapping, &table, &all, &entry.table)
        }
        None => {
            let present = vec![true; table.num_cols()];
            Ok((table, present))
        }
    }
}

fn load_file(
    storage: &Arc<dyn Storage>,
    root: &std::path::Path,
    footers: &FooterCache,
    entry: &AttachedEntry,
    file: &AttachedFile,
    columns: Option<&[usize]>,
    filters: &[pinkdb::attached::PrunePredicate],
) -> Result<(DataTable, Vec<bool>)> {
    let key = entry.key();
    let abs = storage.abs(&file.rel);
    // The catalog's footer location saves the trailer probe; the file's
    // positional deletes tell a row-group-pruning provider not to prune this
    // file (delete positions address the file's full row sequence).
    let mut data_file =
        DataFile::new(&abs, &file.rel, Arc::clone(storage)).with_footer(file.footer);
    if !file.deletes.is_empty() {
        data_file = data_file.with_deletes(file.deletes.clone());
    }
    let Some(plan) = file_plan(storage, footers, entry, file, &data_file, columns, filters)? else {
        return load_whole_file(storage, root, entry, file, &data_file);
    };
    let (file_table, file_present) =
        match decode_projected_via_provider(entry, &data_file, root, &plan.decoded, &plan.filters)?
        {
            Some(decoded) => decoded,
            None => return load_whole_file(storage, root, entry, file, &data_file),
        };
    // A provider that prunes row groups inside a file (wave 102, `.pink`)
    // returns only the row groups that can match the predicates — never for a
    // file with deletes (positions must stay aligned). The engine re-applies
    // the full predicate above the scan, and a pruned load is never cached, so
    // fewer rows is correct there; every other read must return exactly the
    // manifest's count.
    let may_prune_rows =
        entry.format.prunes_row_groups() && !plan.filters.is_empty() && file.deletes.is_empty();
    let rows_ok = if may_prune_rows {
        file_table.num_rows() as u64 <= file.rows
    } else {
        file_table.num_rows() as u64 == file.rows
    };
    if !rows_ok {
        return Err(LakeError::Corrupt {
            path: storage.abs(&file.rel),
            message: format!(
                "table \"{key}\": data file \"{}\" has {} rows, but the manifest says {}",
                file.rel,
                file_table.num_rows(),
                file.rows
            ),
        });
    }
    let file_table = apply_file_deletes(&file_table, &file.deletes, &key, &file.rel)?;
    match &entry.record {
        Some(schema) => {
            schema.project_table(&plan.mapping, &file_table, &file_present, &entry.table)
        }
        None => Ok((file_table, file_present)),
    }
}

/// Stream one data file's projected columns into `sink`, one morsel at a
/// time: the provider decodes and pushes a row group (or page range) while
/// the previous morsel is already folded, so the peak is one morsel's bytes
/// rather than the file's.
///
/// A file this path cannot stream — no projected reader, a schema nothing
/// proves, or positional deletes (their positions address the file's rows and
/// a pruned stream would shift them) — decodes whole, as one morsel.
fn stream_file<F>(
    io: &FileIo<'_>,
    entry: &AttachedEntry,
    file: &AttachedFile,
    columns: Option<&[usize]>,
    predicates: &[pinkdb::attached::PrunePredicate],
    sink: &mut F,
) -> Result<(u64, crate::format::ScanCounters)>
where
    F: FnMut(DataChunk) -> std::result::Result<(), PinkDbError> + ?Sized,
{
    #[cfg(test)]
    let _in_flight = inflight::InFlight::enter();
    let key = entry.key();
    let abs = io.storage.abs(&file.rel);
    let data_file = DataFile::new(&abs, &file.rel, Arc::clone(io.storage)).with_footer(file.footer);
    let plan = if file.deletes.is_empty() {
        file_plan(
            io.storage, io.footers, entry, file, &data_file, columns, predicates,
        )?
    } else {
        None
    };
    let Some(plan) = plan else {
        let (table, _present) = load_file(
            io.storage, io.root, io.footers, entry, file, columns, predicates,
        )?;
        let mut rows = 0u64;
        for index in 0..table.num_chunks() {
            let chunk = table.scan_chunk(index).ok_or_else(|| LakeError::Internal {
                message: format!("chunk {index} of \"{}\" disappeared", file.rel),
            })?;
            rows += chunk.num_rows() as u64;
            sink(chunk).map_err(|source| LakeError::Engine {
                context: format!("scan of \"{key}\""),
                source,
            })?;
        }
        return Ok((rows, crate::format::ScanCounters::default()));
    };
    let mut present = vec![false; plan.physical_schema.len()];
    for &index in &plan.decoded {
        present[index] = true;
    }
    let mut rows = 0u64;
    let (file_rows, counters) = entry.format.scan(
        &data_file,
        &plan.decoded,
        &plan.filters,
        &mut |_offset, chunk| {
            // The chunk has either the file's full physical width or just
            // the requested columns, in order, and a column it did not decode
            // is a zero-length placeholder (see `DataChunk::projected`). The
            // projection needs the full physical width.
            let width = plan.physical_schema.len();
            let full_width = chunk.columns.len() == width;
            if !full_width && chunk.columns.len() != plan.decoded.len() {
                return Err(LakeError::Internal {
                    message: format!(
                        "table \"{key}\": data file \"{}\" pushed {} of {width} columns",
                        file.rel,
                        chunk.columns.len()
                    ),
                });
            }
            let mut columns = Vec::with_capacity(width);
            for (index, (name, dtype)) in plan.physical_schema.iter().enumerate() {
                let held = if full_width {
                    Some(Arc::clone(&chunk.columns[index]))
                } else {
                    plan.decoded
                        .binary_search(&index)
                        .ok()
                        .map(|position| Arc::clone(&chunk.columns[position]))
                };
                let vector = match held {
                    Some(vector) if vector.len() == chunk.num_rows() => vector,
                    _ => {
                        let mut vector = pinkdb::Vector::with_capacity(*dtype, chunk.num_rows());
                        for _ in 0..chunk.num_rows() {
                            vector.push_null().map_err(|source| LakeError::Engine {
                                context: format!(
                                    "building the NULL placeholder of \"{key}\".\"{name}\""
                                ),
                                source,
                            })?;
                        }
                        Arc::new(vector)
                    }
                };
                columns.push(pinkdb::storage::Column {
                    name: name.clone(),
                    dtype: *dtype,
                    default: None,
                    chunks: vec![vector],
                });
            }
            let decoded = DataTable::from_columns(entry.table.clone(), columns, chunk.num_rows());
            let (table, _present) = match &entry.record {
                Some(schema) => {
                    schema.project_table(&plan.mapping, &decoded, &present, &entry.table)?
                }
                None => (decoded, present.clone()),
            };
            for index in 0..table.num_chunks() {
                let out = table.scan_chunk(index).ok_or_else(|| LakeError::Internal {
                    message: format!("chunk {index} of \"{}\" disappeared", file.rel),
                })?;
                rows += out.num_rows() as u64;
                sink(out).map_err(|source| LakeError::Engine {
                    context: format!("scan of \"{key}\""),
                    source,
                })?;
            }
            Ok(())
        },
    )?;
    // The engine re-applies the predicate above the scan, so a pruning
    // provider may push fewer rows than the manifest lists; without pruning
    // the count is exact.
    let may_prune_rows =
        entry.format.prunes_row_groups() && !plan.filters.is_empty() && file.deletes.is_empty();
    let rows_ok = if may_prune_rows {
        rows <= file.rows
    } else {
        rows == file.rows
    };
    if !rows_ok || rows != file_rows {
        return Err(LakeError::Corrupt {
            path: io.storage.abs(&file.rel),
            message: format!(
                "table \"{key}\": data file \"{}\" streamed {rows} rows, but the manifest says {}",
                file.rel, file.rows
            ),
        });
    }
    Ok((rows, counters))
}

/// Stream `entry`'s surviving files on at most `crate::parallel_limit()`
/// workers, pushing each morsel as it decodes. Only called when the caller
/// does not need row order.
#[allow(clippy::too_many_arguments)]
fn stream_files_parallel(
    io: &FileIo<'_>,
    entry: &AttachedEntry,
    files: &[AttachedFile],
    columns: Option<&[usize]>,
    predicates: &[pinkdb::attached::PrunePredicate],
    sink: &mut dyn FnMut(DataChunk) -> std::result::Result<(), PinkDbError>,
) -> Result<(u64, crate::format::ScanCounters)> {
    let workers = crate::parallel_limit().clamp(1, files.len().max(1));
    if workers == 1 {
        let mut rows = 0u64;
        let mut counters = crate::format::ScanCounters::default();
        for file in files {
            let (file_rows, file_counters) =
                stream_file(io, entry, file, columns, predicates, &mut *sink)?;
            rows += file_rows;
            counters.row_groups += file_counters.row_groups;
            counters.row_groups_pruned += file_counters.row_groups_pruned;
            counters.pages += file_counters.pages;
            counters.pages_pruned += file_counters.pages_pruned;
        }
        return Ok((rows, counters));
    }
    // The sink is not `Send` (it belongs to the query), so workers push their
    // morsels through a bounded channel and this thread folds them. The bound
    // keeps at most `workers` morsels queued, on top of the one each worker
    // holds mid-decode.
    let (sender, receiver) = std::sync::mpsc::sync_channel::<DataChunk>(workers);
    let failure = Mutex::new(None::<PinkDbError>);
    let totals = Mutex::new((0u64, crate::format::ScanCounters::default()));
    std::thread::scope(|scope| {
        for worker in 0..workers {
            let sender = sender.clone();
            let failure = &failure;
            let totals = &totals;
            scope.spawn(move || {
                let mut push = |chunk: DataChunk| -> std::result::Result<(), PinkDbError> {
                    sender.send(chunk).map_err(|_| {
                        PinkDbError::Execution("lake scan receiver stopped".to_string())
                    })
                };
                for file in files.iter().skip(worker).step_by(workers) {
                    if failure.lock().unwrap_or_else(|p| p.into_inner()).is_some() {
                        break;
                    }
                    match stream_file(io, entry, file, columns, predicates, &mut push) {
                        Ok((rows, counters)) => {
                            let mut totals = totals.lock().unwrap_or_else(|p| p.into_inner());
                            totals.0 += rows;
                            totals.1.row_groups += counters.row_groups;
                            totals.1.row_groups_pruned += counters.row_groups_pruned;
                            totals.1.pages += counters.pages;
                            totals.1.pages_pruned += counters.pages_pruned;
                        }
                        Err(LakeError::Engine { source, .. }) => {
                            *failure.lock().unwrap_or_else(|p| p.into_inner()) = Some(source);
                            break;
                        }
                        Err(other) => {
                            *failure.lock().unwrap_or_else(|p| p.into_inner()) =
                                Some(PinkDbError::Execution(other.to_string()));
                            break;
                        }
                    }
                }
            });
        }
        drop(sender);
        // Keep draining after a sink failure: a worker blocked on a full
        // channel would otherwise never reach its end.
        for chunk in receiver {
            if failure.lock().unwrap_or_else(|p| p.into_inner()).is_some() {
                continue;
            }
            if let Err(source) = sink(chunk) {
                *failure.lock().unwrap_or_else(|p| p.into_inner()) = Some(source);
            }
        }
    });
    let failure = failure.into_inner().unwrap_or_else(|p| p.into_inner());
    if let Some(source) = failure {
        return Err(LakeError::Engine {
            context: format!("scan of \"{}\"", entry.key()),
            source,
        });
    }
    let totals = totals.into_inner().unwrap_or_else(|p| p.into_inner());
    Ok(totals)
}

/// Decode one data file's projected physical columns through its provider,
/// checking the file's identity against the catalog key. `None` means the
/// provider has no projected reader (the caller falls back to a full decode).
fn decode_projected_via_provider(
    entry: &AttachedEntry,
    file: &DataFile<'_>,
    root: &std::path::Path,
    columns: &[usize],
    filters: &[pinkdb::attached::PrunePredicate],
) -> Result<Option<(DataTable, Vec<bool>)>> {
    let Some((mini, present)) = entry.format.read_projected(file, columns, filters)? else {
        return Ok(None);
    };
    let (file_db, file_table) = only_table(&mini, file.rel(), root)?;
    if !file_db.eq_ignore_ascii_case(&entry.db)
        || !file_table.name.eq_ignore_ascii_case(&entry.table)
    {
        return Err(LakeError::Corrupt {
            path: file.path().to_path_buf(),
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{}\"",
                file_table.name,
                entry.key()
            ),
        });
    }
    Ok(Some((file_table.clone(), present)))
}

/// Wrap a lake metadata error as an engine catalog error (resolution and
/// listing failures are metadata problems, not query execution ones).
fn catalog_error(e: LakeError) -> PinkDbError {
    PinkDbError::Catalog(e.to_string())
}

/// Decode one data file through its registered provider (a full decode),
/// check the file's identity against its catalog key, and hand the table back
/// (chunks shared by `Arc`, never copied twice).
fn decode_via_provider(
    entry: &AttachedEntry,
    file: &DataFile<'_>,
    root: &std::path::Path,
) -> Result<DataTable> {
    let mini = entry.format.read(file)?;
    let (file_db, file_table) = only_table(&mini, file.rel(), root)?;
    if !file_db.eq_ignore_ascii_case(&entry.db)
        || !file_table.name.eq_ignore_ascii_case(&entry.table)
    {
        return Err(LakeError::Corrupt {
            path: file.path().to_path_buf(),
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{}\"",
                file_table.name,
                entry.key()
            ),
        });
    }
    Ok(file_table.clone())
}

/// Swap freshly decoded columns into an already materialized table. The two
/// tables' chunkings must agree exactly (both come from the same slicing of
/// the same file list, so they do); a mismatch is an internal error, never a
/// One `.pink` file's schema + gated statistics: its published per-file block
/// (v4), or the entry-level block (v1). Either way the block is CRC-verified
/// and cross-checked against the entry's key, the file's path and its row
/// count, so a block pasted under another file can never attach.
#[cfg(test)]
fn pink_file_stats(
    entry_key: &str,
    entry: &TableEntry,
    file: &crate::catalog::FileRef,
    manifest_path: &std::path::Path,
) -> Result<SchemaAndStats> {
    block_file_stats(
        entry_key,
        entry,
        file,
        manifest_path,
        crate::format::PINK_FORMAT_ID,
    )
}

/// The manifest-published statistics of one data file whose format does not
/// carry its own (the built-in `.pink` bytes, and any extension provider that
/// returns no footer): the per-file block (v4) or the entry-level block (v1).
/// `format_id` names the format in the stats-less error.
fn block_file_stats(
    entry_key: &str,
    entry: &TableEntry,
    file: &crate::catalog::FileRef,
    manifest_path: &std::path::Path,
    format_id: &str,
) -> Result<SchemaAndStats> {
    let rel = file.path();
    // v4: the block rides inside the file's manifest object. v1: the entry
    // carries one block for its single file.
    let (block, file_rows) = match file.stats() {
        Some(block) => (block, file.rows()),
        None => match entry.stats.as_ref() {
            Some(block) if matches!(file, crate::catalog::FileRef::Path(_)) => (block, None),
            _ => {
                let what = if format_id == crate::format::PINK_FORMAT_ID {
                    "`.pink`".to_string()
                } else {
                    format!("`{format_id}`")
                };
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "lake table \"{entry_key}\": {what} data file \"{rel}\" was published \
                         without a statistics block (statistics are published by saves from \
                         this build onwards); use .lake open, or save the store again to \
                         publish statistics"
                    ),
                });
            }
        },
    };
    let rows = file_rows.unwrap_or(entry.rows);
    block_schema_and_stats(
        entry_key,
        rel,
        rows,
        block,
        manifest_path,
        !file.stats_sealed(),
    )
}

/// Verify one statistics block against its entry and turn it into the
/// `(schema, gated stats)` attach needs: the CRC and block version
/// ([`crate::stats::StatsBlock::schema_and_stats`]), the block's
/// `table`/`file`/`rows` against the catalog key, the file's path and its
/// row count, so a block pasted under another file can never attach.
fn block_schema_and_stats(
    entry_key: &str,
    rel: &str,
    rows: u64,
    block: &crate::stats::StatsBlock,
    manifest_path: &std::path::Path,
    check_crc: bool,
) -> Result<SchemaAndStats> {
    let corrupt = |message: String| LakeError::Corrupt {
        path: manifest_path.to_path_buf(),
        message,
    };
    let (schema, stats, paths) = if check_crc {
        block.schema_and_stats(manifest_path)?
    } else {
        block.schema_and_stats_with_crc_check(manifest_path, false)?
    };
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
    Ok((schema, stats, paths))
}

/// Fold per-file statistics into one table's: NULL counts add up,
/// `MIN`/`MAX` fold when **every** contributing file proves them, and any
/// file that cannot (or any positional delete over a physical column, which
/// could have removed the extreme value) makes the fact unknown — never an
/// approximation. `files` is `(surviving rows, stats)` per data file in
/// manifest order, already mapped onto the logical schema and gated (a
/// default-filled column's constant facts are exact even under deletes).
///
/// Wave 68: the live inlined rows ride along as `facts` (exact — the values
/// are stored with the row) and fold into the same result; an all-NULL
/// inlined contribution adds only NULL counts, and an inlined DOUBLE extreme
/// the gate refuses (`NaN`/zero) clears the fold to unknown, exactly like an
/// unproven file.
fn merge_file_stats(files: &[(u64, Vec<ColumnStats>)], facts: &[InlinedFact]) -> Vec<ColumnStats> {
    let ncols = files.first().map(|(_, s)| s.len()).unwrap_or(facts.len());
    let mut out = Vec::with_capacity(ncols);
    for col in 0..ncols {
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
        if let Some(fact) = facts.get(col) {
            any_rows |= fact.rows > 0;
            crate::inlined::apply_fact(
                fact,
                &mut nulls,
                &mut min,
                &mut min_known,
                &mut max,
                &mut max_known,
            );
        }
        if !any_rows {
            // No file and no inlined row holds a value: the executor derives
            // NULL min/max from the row count; the exact 0-NULL count is
            // still provable.
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

/// Statistics-only open of one manifest's tables (see the module docs).
/// `version` is the caller's snapshot choice; the manifest (not
/// the catalog) is read so an attached version stays pinned however the
/// catalog moves.
pub(crate) fn build(
    lake: &Lake,
    version: Option<u64>,
) -> Result<(Arc<LakeAttachment>, AttachmentReport)> {
    build_from(
        &lake.store,
        &lake.storage,
        lake.path(),
        lake.catalog_label(),
        version,
        Some(&lake.lake_id),
    )
}

/// [`build`] without a [`Lake`] handle: the storage root, the catalog
/// location errors name and the version to pin. Time-travel views are built
/// through this same function, so a version query validates exactly like
/// `.lake attach <dir> <version>`.
pub(crate) fn build_from(
    store: &Arc<dyn crate::metadata::MetadataStore>,
    storage: &Arc<dyn Storage>,
    root: &std::path::Path,
    catalog: &str,
    version: Option<u64>,
    lake_id: Option<&str>,
) -> Result<(Arc<LakeAttachment>, AttachmentReport)> {
    let stats_before = storage.stats();
    let (manifest, manifest_path) = manifest_for(&**store, std::path::Path::new(catalog), version)?;
    if let Some(expected) = lake_id
        && manifest.format_version >= 6
        && manifest.lake_id.as_deref() != Some(expected)
    {
        return Err(LakeError::Corrupt {
            path: manifest_path.clone(),
            message: format!(
                "manifest declares lake {}, but this lake is {expected} (a manifest from \
                 another lake was substituted)",
                manifest.lake_id.as_deref().unwrap_or("<none>")
            ),
        });
    }
    let root = root.to_path_buf();
    let mut tables: Vec<AttachedTable> = Vec::with_capacity(manifest.tables.len());
    let mut files: HashMap<(String, String), AttachedEntry> = HashMap::new();
    let mut unavailable: HashMap<(String, String), String> = HashMap::new();
    let encrypted = storage.encrypted();
    for (key, entry) in &manifest.tables {
        let (dbname, table) = split_key(key, &manifest_path)?;
        // Wave 88, rule 5: a table whose format this build cannot read is
        // still **listed**, but no file is touched for it and every read
        // fails with the per-table message. The descriptor's schema comes
        // from the v5 record, else from the published statistics block (both
        // live in the manifest; no data file is opened), else it is empty.
        let format = entry.format();
        let provider = match crate::format::format_for(format.id(), &manifest_path) {
            Ok(provider) => provider,
            Err(error @ LakeError::RemovedFormat { .. }) => return Err(error),
            Err(error) => {
                let record = crate::schema::TableSchema::from_entry(key, entry, &manifest_path)?;
                let schema = match &record {
                    Some(record) => record.schema_pairs(),
                    None => entry
                        .files
                        .first()
                        .and_then(|file| {
                            block_file_stats(key, entry, file, &manifest_path, format.id())
                                .ok()
                                .map(|(schema, _, _)| schema)
                        })
                        .unwrap_or_default(),
                };
                let lkey = (dbname.to_ascii_lowercase(), table.to_ascii_lowercase());
                if files.contains_key(&lkey) || unavailable.contains_key(&lkey) {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.clone(),
                        message: format!(
                            "the manifest names \"{key}\" twice under one case-insensitive \
                             name; refusing to attach an ambiguous table"
                        ),
                    });
                }
                unavailable.insert(
                    lkey,
                    format!(
                        "lake table \"{key}\" uses data-file format \"{}\", which this build \
                         cannot read: {error}",
                        format.id()
                    ),
                );
                tables.push(AttachedTable {
                    db: dbname.to_string(),
                    schema_name: "main".to_string(),
                    table: table.to_string(),
                    rows: crate::UNAVAILABLE_ROWS,
                    stats: vec![ColumnStats::unknown(); schema.len()],
                    schema,
                });
                continue;
            }
        };
        // Every referenced path is validated before anything is read. Data
        // files are NOT probed for existence (wave 96a, like DuckLake): a
        // statistics-only attach issues no request per data file, and a
        // missing file fails at its first read with the file and table named
        // (`load_file`). Delete files need no probe either — the layout read
        // below fetches and decodes every one, so a missing one fails here.
        for file in &entry.files {
            validate_rel_path(file.path())?;
        }
        for d in &entry.deletes {
            validate_rel_path(&d.path)?;
        }
        let layout = entry_layout(&**storage, entry, key, &manifest_path)?;
        // `entry.rows` backs the statistics-only `COUNT(*)`. The deduped
        // layout plus the live inlined rows is the truth: if two delete files
        // target the same data file with overlapping positions, the manifest's
        // `sum(count)` overstates the deletion and `entry.rows` would be
        // wrong. Reject the mismatch (as `Lake::load` does) instead of
        // answering with a wrong count.
        let live_inlined = entry.live_inlined_rows();
        let decoded_rows: Option<u64> =
            layout
                .iter()
                .try_fold(live_inlined, |acc, (_, rows, deleted)| {
                    let live = rows.checked_sub(deleted.len() as u64)?;
                    acc.checked_add(live)
                });
        if decoded_rows != Some(entry.rows) {
            return Err(LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!(
                    "table \"{key}\" declares {} rows, but its deduplicated file layout and \
                     live inlined rows hold {}",
                    entry.rows,
                    decoded_rows
                        .map_or_else(|| "an overflowing count".to_string(), |r| r.to_string())
                ),
            });
        }
        let projected = provider.supports_projection();
        // The v5 schema record (wave 65), when this entry has one.
        let record = crate::schema::TableSchema::from_entry(key, entry, &manifest_path)?;
        // Per-file (surviving rows, stats in logical-schema order), in
        // manifest order — the stats are the file's zone map (wave 66): the
        // manifest-published block's gated column facts.
        let mut per_file: Vec<(u64, Vec<ColumnStats>)> = Vec::with_capacity(entry.files.len());
        // Per-file zone maps (ungated by deletes: deleting rows only shrinks
        // a file's value set, so a file that provably could not match a
        // predicate before deletes still cannot after them — wave 66's
        // pruning rule). The `per_file` stats fed to the table-level merge
        // use wave 65's gating instead (a deleted position may have held any
        // value, so physical facts fall back; default-materialized constants
        // stay exact).
        let mut zones: Vec<Vec<ColumnStats>> = Vec::with_capacity(entry.files.len());
        // Each file's per-path VARIANT statistics, projected onto the logical
        // schema like `zones` (a default-filled column has none, and a
        // reordered one moves its paths with its column).
        let mut file_paths: Vec<Vec<pinkdb::PathStats>> = Vec::with_capacity(entry.files.len());
        let mut schema: Option<Vec<(String, pinkdb::DataType)>> = None;
        for (fi, file) in entry.files.iter().enumerate() {
            // A v1–v3 entry's single file carries no per-file count; the
            // entry's logical count is that file's count.
            let rows = file.rows().unwrap_or(entry.rows);
            let deleted = layout.get(fi).map(|(_, _, d)| d.len() as u64).unwrap_or(0);
            let surviving = rows.saturating_sub(deleted);
            let (file_schema, file_stats, block_paths) =
                block_file_stats(key, entry, file, &manifest_path, provider.id())?;
            let (zone_stats, stats, paths) = match &record {
                // Project the file's physical statistics onto the logical
                // schema: renamed columns map, old-file columns materialize
                // their default's constant facts, dropped ones vanish.
                Some(record) => {
                    let file_version = file.schema_version().unwrap_or(record.version);
                    let mapping =
                        record.map_file(file_version, &file_schema, &storage.abs(file.path()))?;
                    let mut zone_stats = Vec::with_capacity(record.columns.len());
                    let mut stats = Vec::with_capacity(record.columns.len());
                    let mut paths = Vec::new();
                    // Only the merged table-level facts lose a deleted file's
                    // per-column values (a deleted position may have held any
                    // value); the file's zone map keeps them.
                    let file_has_deletes = layout.get(fi).is_some_and(|(_, _, d)| !d.is_empty());
                    for (li, mapped) in mapping.physical.iter().enumerate() {
                        match mapped {
                            Some(pi) => {
                                zone_stats.push(file_stats[*pi].clone());
                                if file_has_deletes {
                                    stats.push(ColumnStats::unknown());
                                } else {
                                    stats.push(file_stats[*pi].clone());
                                }
                                paths.extend(path_stats(&block_paths[*pi]));
                            }
                            None => {
                                let constant = record.default_stats(li, surviving);
                                zone_stats.push(constant.clone());
                                stats.push(constant);
                            }
                        }
                    }
                    let logical = record.schema_pairs();
                    match &schema {
                        None => schema = Some(logical),
                        Some(first) if *first != logical => {
                            return Err(LakeError::Corrupt {
                                path: manifest_path.clone(),
                                message: format!(
                                    "the schema records of table \"{key}\" disagree across \
                                     versions"
                                ),
                            });
                        }
                        Some(_) => {}
                    }
                    (zone_stats, stats, paths)
                }
                None => {
                    match &schema {
                        None => schema = Some(file_schema),
                        Some(first) if *first != file_schema => {
                            return Err(LakeError::Corrupt {
                                path: manifest_path.clone(),
                                message: format!(
                                    "data files of table \"{key}\" disagree on the schema"
                                ),
                            });
                        }
                        Some(_) => {}
                    }
                    // A legacy entry has no schema record and no defaults:
                    // its merged facts simply fall back once deletes exist,
                    // while the file's zone map stays exact.
                    let file_has_deletes = layout.get(fi).is_some_and(|(_, _, d)| !d.is_empty());
                    let merged = if file_has_deletes {
                        file_stats.iter().map(|_| ColumnStats::unknown()).collect()
                    } else {
                        file_stats.clone()
                    };
                    let paths = block_paths
                        .iter()
                        .flat_map(|block| path_stats(block))
                        .collect();
                    (file_stats, merged, paths)
                }
            };
            per_file.push((surviving, stats));
            zones.push(zone_stats);
            file_paths.push(paths);
        }
        let schema = match schema {
            Some(schema) => schema,
            // A table whose rows are all inlined (or an empty committed
            // table) lists no data file: the schema record is the authority,
            // exactly as `Lake::rebuild` reads it.
            None if entry.files.is_empty() => match &record {
                Some(record) => record.schema_pairs(),
                None => {
                    return Err(LakeError::Corrupt {
                        path: manifest_path.clone(),
                        message: format!(
                            "table \"{key}\" has no data files and no schema record; its \
                             schema is not recorded anywhere"
                        ),
                    });
                }
            },
            None => {
                return Err(LakeError::Corrupt {
                    path: manifest_path.clone(),
                    message: format!("table \"{key}\" has no data files"),
                });
            }
        };
        // The partition spec: every key column must exist, and every file's
        // recorded partition value must decode under that column's type and
        // agree with the file's zone map (a misplaced or tampered value must
        // never silently prune a matching file). A stats entry gated to
        // unknown by positional deletes simply cannot cross-check a value —
        // the check passes rather than guessing.
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
                        });
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
        let stats = {
            // Wave 68: the live inlined rows contribute exact facts (they are
            // fully known); see `crate::inlined::apply_fact` for the fold and
            // the DOUBLE gate.
            let live_inlined: Vec<crate::catalog::InlinedRow> =
                entry.live_inlined().into_iter().cloned().collect();
            let facts = match &record {
                Some(record) if !live_inlined.is_empty() => {
                    crate::inlined::live_facts(&live_inlined, record, key, &manifest_path)?
                }
                _ => Vec::new(),
            };
            merge_file_stats(&per_file, &facts)
        };
        let attached = AttachedTable {
            db: dbname.to_string(),
            schema_name: "main".to_string(),
            table: table.to_string(),
            rows: entry.rows,
            schema: schema.clone(),
            stats,
        };
        let file_list: Vec<AttachedFile> = layout
            .iter()
            .zip(entry.files.iter().zip(zones.iter()))
            .zip(file_paths.iter())
            .map(
                |(((rel, rows, deletes), (file, zone)), paths)| AttachedFile {
                    rel: rel.clone(),
                    rows: *rows,
                    schema_version: file
                        .schema_version()
                        .or_else(|| record.as_ref().map(|r| r.version))
                        .unwrap_or(0),
                    deletes: deletes.clone(),
                    zone: Some(zone.clone()),
                    paths: paths.clone(),
                    partition: file.partition().cloned(),
                    footer: file.footer_location(),
                },
            )
            .collect();
        let lkey = (dbname.to_ascii_lowercase(), table.to_ascii_lowercase());
        if files.contains_key(&lkey) || unavailable.contains_key(&lkey) {
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
                format: provider,
                projected,
                record,
                inlined: entry.live_inlined().into_iter().cloned().collect(),
            },
        );
        tables.push(attached);
    }
    let attachment = Arc::new(LakeAttachment {
        store: Arc::clone(store),
        location: root.display().to_string(),
        catalog: catalog.to_string(),
        storage: Arc::clone(storage),
        root,
        lake_id: lake_id.map(str::to_string),
        files,
        unavailable,
        tables,
        footers: Mutex::new(HashMap::new()),
        last_scan: Mutex::new(None),
        loads: AtomicU64::new(0),
        columns_decoded: AtomicU64::new(0),
        files_decoded: AtomicU64::new(0),
        pinned: manifest.version,
        views: Mutex::new(HashMap::new()),
        index: Mutex::new(None),
    });
    let stats_after = storage.stats();
    let report = AttachmentReport {
        version: manifest.version,
        tables: attachment.tables.len(),
        format_version: manifest.format_version,
        encrypted,
        requests: stats_after.requests.saturating_sub(stats_before.requests),
        bytes: stats_after.bytes.saturating_sub(stats_before.bytes),
        wait_nanos: stats_after
            .wait_nanos
            .saturating_sub(stats_before.wait_nanos),
        ranged: storage.ranged_reads(),
        parallelism: crate::parallel_limit(),
        lake: Arc::clone(&attachment) as Arc<dyn AttachedLake>,
    };
    Ok((attachment, report))
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
        // A filtered scan of a `.pink` file takes the row-group-pruning path
        // (wave 102), and every later data query decodes afresh: the session
        // no longer keeps a materialized copy of the table (wave 133a).
        let rows = query(&mut store, "SELECT COUNT(*) FROM t WHERE i > 0;");
        assert_eq!(db.loads(), 1);
        assert_eq!(db.files_decoded(), 1, "the one data file decoded");
        assert_eq!(rows, vec![vec![Value::Bigint(1)]]);
        let _ = query(&mut store, "SELECT SUM(i) FROM t;");
        assert_eq!(db.loads(), 2, "a data query decodes");
        let _ = query(&mut store, "SELECT MAX(i) + SUM(i) FROM t;");
        assert_eq!(db.loads(), 3, "nothing is cached between queries");
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
            schema: "main".to_string(),
            table: "t".to_string(),
            rows: 3,
            columns: vec![pinkdb::persist::SavedColumnStats {
                name: "i".to_string(),
                dtype: pinkdb::DataType::Integer,
                null_count: 0,
                min: Some(Value::Integer(-5)),
                max: Some(Value::Integer(9)),
                nan_count: 0,
                paths: Vec::new(),
            }],
        };
        // A v4 entry whose single file carries `block` and declares 3 rows.
        let entry = |block: Option<StatsBlock>| TableEntry {
            files: vec![FileRef::Entry {
                path: "data/main.t/x.pink".to_string(),
                rows: 3,
                schema_version: None,
                stats: block,
                partition: None,
                row_id_start: None,
                footer_offset: None,
                footer_len: None,
                footer_crc32c: None,
                stats_sealed: false,
            }],
            rows: 3,
            table_id: None,
            format: Some(FileFormat::pink()),
            stats: None,
            deletes: Vec::new(),
            partition_by: None,
            schema_version: 0,
            columns: None,
            renames: Vec::new(),
            inlined: Vec::new(),
            inlined_limit: None,
            next_row_id: 0,
        };
        let path = std::path::Path::new("data/main.t/x.pink");
        let key = "main.t";

        // A consistent block attaches.
        let block = StatsBlock::from_saved(key, "data/main.t/x.pink", &saved).unwrap();
        let e = entry(Some(block));
        let (schema, stats, _) = pink_file_stats(key, &e, &e.files[0], path).unwrap();
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
            table_id: None,
            format: None,
            stats: Some(block),
            deletes: Vec::new(),
            partition_by: None,
            schema_version: 0,
            columns: None,
            renames: Vec::new(),
            inlined: Vec::new(),
            inlined_limit: None,
            next_row_id: 0,
        };
        let (schema, stats, _) = pink_file_stats(key, &v1, &v1.files[0], path).unwrap();
        assert_eq!(schema[0].0, "i");
        assert_eq!(stats[0].max, Some(Value::Integer(9)));
    }

    /// A multi-block, all-types table for the projection tests: 20,000 rows
    /// (ten engine chunks over several stored blocks, so decodes exercise
    /// ragged block tails and multi-block merges), NULLs, NaN payloads,
    /// `-0.0`, extremes and long strings.
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

        // A sparse projection after the full load decodes afresh: only the
        // requested columns hold values, the rest are NULL placeholders.
        let projected = attachment
            .load_projected("main", "t", Some(&[0, 3]))
            .unwrap();
        assert_eq!(attachment.loads(), 2, "every load decodes");
        assert_eq!(attachment.columns_decoded(), 7);
        assert_eq!(projected.present, vec![true, false, false, true, false]);
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

        // A wider projection decodes its columns afresh: nothing is shared
        // between loads (wave 133a).
        let extended = solo
            .load_projected("main", "t", Some(&[0, 1, 2, 3, 4]))
            .unwrap();
        assert_eq!(solo.loads(), 2);
        assert_eq!(solo.columns_decoded(), 7);
        assert_eq!(extended.present, vec![true; 5]);
        // Every value equals the full decode bit for bit.
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
    /// decodes its referenced columns only, the fast paths decode nothing at
    /// all, and every answer equals the in-memory execution over the same
    /// rows. Nothing is cached between queries (wave 133a), so a repeated
    /// column is decoded again — the check is per query, not cumulative.
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

        let mut total = 0u64;
        let mut check = |store: &mut PinkDb, sql: &str, decoded: u64| {
            let before = attachment.columns_decoded();
            let (attached, plain) = (query(store, sql), query(&mut plain, sql));
            assert_eq!(
                attachment.columns_decoded() - before,
                decoded,
                "{sql}: columns decoded"
            );
            total += decoded;
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
        check(&mut store, "SELECT SUM(i) FROM t;", 1);
        check(&mut store, "SELECT AVG(i) FROM t;", 1);
        // A filter decodes the predicate's column. A pruned row-group load
        // is deliberately uncached (its rows are a subset), so it decodes
        // that column again even though the table already has it.
        check(&mut store, "SELECT COUNT(*) FROM t WHERE i > 0;", 1);
        // Order-by + limit: the sort key and the projected column.
        check(&mut store, "SELECT g FROM t ORDER BY g DESC LIMIT 3;", 1);
        check(&mut store, "SELECT v FROM t ORDER BY v LIMIT 5;", 1);
        // GROUP BY aggregate: the key.
        check(
            &mut store,
            "SELECT b, COUNT(*) FROM t GROUP BY b ORDER BY b;",
            1,
        );
        check(&mut store, "SELECT SUM(i), MIN(g), COUNT(v) FROM t;", 3);
        // A window over projected columns.
        check(
            &mut store,
            "SELECT i, ROW_NUMBER() OVER (PARTITION BY b ORDER BY i) FROM t LIMIT 5;",
            2,
        );
        // `SELECT *` and expressions over several columns: every column is
        // already in, so nothing decodes.
        check(&mut store, "SELECT * FROM t ORDER BY i LIMIT 2;", 5);
        check(&mut store, "SELECT i + g + d FROM t LIMIT 2;", 3);
        // Every column of every query above, decoded afresh: the total is the
        // sum of the per-query deltas, i.e. no query decoded more than its
        // referenced columns.
        assert_eq!(attachment.columns_decoded(), 20);
        assert_eq!(total, 20);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Attach never probes data files (wave 96a, like DuckLake): with every
    /// data file removed a statistics-only attach still succeeds and answers
    /// `COUNT(*)` from the catalog, and the first query that has to read a
    /// file fails with a clean error naming it.
    #[test]
    fn missing_data_file_fails_at_first_read() {
        let dir = scratch("missing");
        let lake_dir = dir.join("lake");
        let mut writer = Lake::create(&lake_dir).unwrap();
        writer.set_write_format("pink").unwrap();
        writer.write_from_store(&fixture_store()).unwrap();
        let data_root = writer.path().to_path_buf();
        drop(writer);
        // Remove every data file the catalog references.
        let removed = walk(&data_root.join("data"));
        for entry in &removed {
            std::fs::remove_file(entry).unwrap();
        }
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();
        let mut db = PinkDb::new();
        db.attach_lake(attachment).unwrap();
        match db
            .execute("SELECT COUNT(*) FROM t;")
            .unwrap()
            .pop()
            .unwrap()
        {
            pinkdb::QueryResult::Selected { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Bigint(3)]]);
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let err = db.execute("SELECT * FROM t;").unwrap_err().to_string();
        let t_file = removed
            .iter()
            .find(|p| p.parent().is_some_and(|d| d.ends_with("main.t")))
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap()
            .to_string();
        assert!(err.contains(&t_file), "the error must name {t_file}: {err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Wave 133a: a streamed scan prunes files from the catalog statistics,
    /// reports what it read, reads surviving files with bounded parallelism,
    /// and only keeps file/row order when the caller asks for it.
    #[test]
    fn streamed_scans_prune_report_io_bound_parallelism_and_keep_order_on_request() {
        let dir = scratch("stream");
        let lake_dir = dir.join("lake");
        let mut base = PinkDb::new();
        base.execute("CREATE TABLE t (i INTEGER);").unwrap();
        {
            let t = base.table_mut("t").unwrap();
            for i in 0..4_000 {
                t.append_row(vec![Value::Integer(i)]).unwrap();
            }
        }
        crate::save_store(&base, &lake_dir).unwrap();
        let mut lake = Lake::open(&lake_dir).unwrap();
        lake.set_data_inlining_row_limit(0);
        for file in 1..4 {
            let mut rows = DataTable::new(
                "t".to_string(),
                vec![("i".to_string(), pinkdb::DataType::Integer)],
            )
            .unwrap();
            for i in 0..4_000 {
                rows.append_row(vec![Value::Integer(file * 4_000 + i)])
                    .unwrap();
            }
            let mut commit = crate::incremental::Commit::new();
            commit.append("main.t", Arc::new(rows));
            lake.commit(&commit).unwrap();
        }
        let (attachment, _) = build(&lake, None).unwrap();
        let table = attachment.tables()[0].clone();
        assert_eq!(table.rows, 16_000);

        let mut ordered = Vec::new();
        let (rows, io) = attachment
            .scan_streamed_io(&table.db, &table.table, None, &[], true, &mut |chunk| {
                for row in 0..chunk.num_rows() {
                    ordered.push(chunk.columns[0].get(row));
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 16_000);
        let expected: Vec<Value> = (0..16_000).map(Value::Integer).collect();
        assert_eq!(
            ordered, expected,
            "ordered output keeps file then row order"
        );
        assert_eq!(io.files, 4);
        assert_eq!(io.files_pruned, 0);
        assert_eq!(io.requests, 0, "a local lake issues no transport requests");
        assert_eq!(io.bytes, 0);
        assert_eq!(
            attachment.last_scan(),
            Some(io),
            "the last scan's accounting stays readable"
        );

        // Unordered: same rows, read with at most `parallel_limit()` files in
        // flight. Each worker holds several morsels and the channel is
        // bounded, so a slow sink keeps every worker in flight.
        super::inflight::InFlight::reset();
        let mut unordered = Vec::new();
        let (rows, io) = attachment
            .scan_streamed_io(&table.db, &table.table, None, &[], false, &mut |chunk| {
                std::thread::sleep(std::time::Duration::from_millis(2));
                for row in 0..chunk.num_rows() {
                    unordered.push(chunk.columns[0].get(row));
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 16_000);
        assert_eq!(io.files, 4);
        let limit = crate::parallel_limit();
        let peak = super::inflight::PEAK.load(Ordering::SeqCst);
        assert!(
            peak <= limit,
            "{peak} files were in flight, the limit is {limit}"
        );
        // How many reads overlap at the peak depends on thread scheduling (a
        // loaded machine reached 3 of 4), so only overlap itself is asserted.
        assert!(
            peak > 1,
            "an unordered scan of several files runs in parallel"
        );
        let mut sorted: Vec<i32> = unordered
            .iter()
            .map(|value| match value {
                Value::Integer(i) => *i,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..16_000).collect::<Vec<i32>>());

        // Catalog-side pruning: only the file whose zone map can match is read.
        let predicates = [pinkdb::attached::PrunePredicate {
            column: 0,
            op: pinkdb::attached::PruneOp::GtEq,
            value: Some(Value::Integer(12_000)),
            path: None,
        }];
        let mut pruned = Vec::new();
        let (rows, io) = attachment
            .scan_streamed_io(
                &table.db,
                &table.table,
                None,
                &predicates,
                true,
                &mut |chunk| {
                    for row in 0..chunk.num_rows() {
                        pruned.push(chunk.columns[0].get(row));
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(io.files, 1, "one file survives the zone map");
        assert_eq!(io.files_pruned, 3);
        assert_eq!(rows, 4_000);
        assert_eq!(
            pruned,
            (12_000..16_000).map(Value::Integer).collect::<Vec<Value>>()
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
