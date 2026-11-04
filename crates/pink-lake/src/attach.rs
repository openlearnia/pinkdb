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
//! [`pinkdb::AttachedLake::scan_source`] skips every data file the map
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
//! # Streamed scans (waves 133a–b)
//!
//! Every data scan streams. [`LakeAttachment::plan_scan`] settles from footer
//! and page metadata alone which files survive the predicates, which morsels
//! each decodes as — a Parquet row group or page range, or a whole file when
//! the provider cannot plan one (or when positional deletes make pruning
//! unsafe) — and the [`ScanIo`] the scan costs: files, row groups and pages
//! read versus pruned, plus the transport requests and bytes it issues. The
//! engine's pipeline hands those morsel indices to its workers, and each
//! worker decodes its own morsel, so a scan holds one morsel per worker rather
//! than the table, and no worker ever waits for another one to make progress.
//! Answers are identical either way: the engine re-applies the predicate above
//! the scan, so pruning only removes rows that provably cannot match.
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
    Lake, LakeError, Result, apply_file_deletes, manifest_for, only_table, published_versions,
    split_key, validate_rel_path,
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
    /// the header slots. `None` for formats the catalog does not locate this
    /// way.
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

/// What a planned file needs from its attachment: where the data lives and
/// the footer cache shared across the attachment's life.
struct FileIo<'a> {
    storage: &'a Arc<dyn Storage>,
    footers: &'a FooterCache,
}

/// Per-attachment footer cache, keyed by `(relative path, row count)`.
///
/// The row count stands in for the file's identity: a data file is
/// immutable, so `(path, rows)` names one file for the attachment's life
/// (a re-written path with different rows is a different file).
type FooterCache = Mutex<HashMap<(String, u64), Option<crate::format::FileFooter>>>;

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
    /// The scan in the engine's generic shape: one unit per kind of thing the
    /// source prunes (files, then row groups and pages when the format has
    /// them) and the object-storage totals. In-flight morsels are the
    /// pipeline's to measure.
    fn to_morsel(self) -> pinkdb::morsel::ScanIo {
        use pinkdb::morsel::{ScanIo as EngineIo, ScanUnit};
        let mut units = vec![ScanUnit {
            unit: "files",
            read: self.files,
            pruned: self.files_pruned,
        }];
        if self.row_groups + self.row_groups_pruned > 0 {
            units.push(ScanUnit {
                unit: "row groups",
                read: self.row_groups,
                pruned: self.row_groups_pruned,
            });
        }
        if self.pages + self.pages_pruned > 0 {
            units.push(ScanUnit {
                unit: "pages",
                read: self.pages,
                pruned: self.pages_pruned,
            });
        }
        EngineIo {
            units,
            requests: self.requests,
            bytes: self.bytes,
            in_flight_peak: 0,
        }
    }

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
    footers: Arc<FooterCache>,
    /// What the last scan of this attachment read and pruned (set when the
    /// scan's morsel stream reports its accounting). Shared with the plans the
    /// attachment builds, which record it when they report.
    last_scan: Arc<Mutex<Option<ScanIo>>>,
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

impl VersionView<'_> {
    /// The view as a shareable attachment (a scan source outlives the `AT`
    /// resolution): `None` when the view is the pinned attachment itself.
    fn shared(&self) -> Option<Arc<LakeAttachment>> {
        match self {
            VersionView::Pinned(_) => None,
            VersionView::Built(attachment) => Some(Arc::clone(attachment)),
        }
    }
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

    /// What the most recent scan of this attachment read and pruned (the
    /// surfaced form of [`ScanIo`]).
    #[cfg(test)]
    pub fn last_scan(&self) -> Option<ScanIo> {
        *self.last_scan.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Plan the morsel stream behind [`AttachedLake::scan_source`]: which
    /// files survive the predicates, which morsels each decodes as, and the
    /// I/O the scan will cost.
    ///
    /// Planning reads footer and page metadata only, so pruning — and the
    /// accounting it produces — is settled before any data page is read. The
    /// plan is what the pipeline's workers share: each claims a morsel index
    /// and decodes that morsel itself, reading only its own bytes, so no
    /// worker ever waits for another one to make progress.
    fn plan_scan(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
    ) -> std::result::Result<Arc<ScanPlan>, PinkDbError> {
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
        let full = !entry.projected || requested.iter().all(|&w| w);
        let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
        let columns = if full { None } else { Some(decoded) };
        let io = FileIo {
            storage: &self.storage,
            footers: &self.footers,
        };
        let survivors = file_survivors(&entry, predicates);
        let mut files = Vec::with_capacity(survivors.len());
        let mut morsels: Vec<Morsel> = Vec::new();
        let mut counts = crate::format::ScanCounters::default();
        for file in survivors {
            let index = files.len() as u32;
            let planned = planned_file(&io, &entry, &file, columns.as_deref(), predicates)
                .map_err(scan_error)?;
            match &planned.morsels {
                Some(plan) => {
                    counts.add(&plan.counters());
                    morsels.extend((0..plan.count() as u32).map(|morsel| Morsel::Group {
                        file: index,
                        morsel,
                    }));
                }
                None => morsels.push(Morsel::File { file: index }),
            }
            files.push(planned);
        }
        // The inlined rows come after every file, exactly as a materialized
        // whole-table scan appends them, so row order is unchanged.
        let inlined = if entry.inlined.is_empty() {
            None
        } else {
            let record = entry.record.as_ref().ok_or_else(|| {
                scan_error(LakeError::Corrupt {
                    path: self.catalog_path().to_path_buf(),
                    message: format!(
                        "table \"{}\" carries inlined rows but no schema record",
                        entry.key()
                    ),
                })
            })?;
            let wanted = requested_mask(entry.schema.len(), columns.as_deref());
            let (table, _present) = crate::inlined::materialize_live(
                &entry.inlined,
                record,
                &entry.key(),
                &entry.table,
                Some(&wanted),
                self.catalog_path(),
            )
            .map_err(scan_error)?;
            morsels.push(Morsel::Inlined);
            Some(table)
        };
        self.loads.fetch_add(1, Ordering::Relaxed);
        self.files_decoded
            .fetch_add(files.len() as u64, Ordering::Relaxed);
        let decoded_count = if entry.projected {
            requested.iter().filter(|&&w| w).count() as u64
        } else {
            requested.len() as u64
        };
        self.columns_decoded
            .fetch_add(decoded_count, Ordering::Relaxed);
        // The backend's counters, so this scan's share of requests and bytes is
        // measurable (a local lake reports zero).
        let before = self.storage.stats();
        let mut scan_io = ScanIo {
            files: files.len() as u64,
            files_pruned: (entry.files.len() - files.len()) as u64,
            ..ScanIo::default()
        };
        scan_io.add(&counts);
        let names = entry.schema.iter().map(|(name, _)| name.clone()).collect();
        let types = entry.schema.iter().map(|(_, dtype)| *dtype).collect();
        Ok(Arc::new(ScanPlan {
            label: format!("lake {} table \"{db}.{table}\"", self.location),
            names,
            types,
            entry,
            files,
            cursor: pinkdb::MorselCursor::new(morsels.len() as u64),
            morsels,
            inlined,
            columns,
            storage: Arc::clone(&self.storage),
            root: self.root.clone(),
            footers: Arc::clone(&self.footers),
            predicates: predicates.to_vec(),
            io: Mutex::new(scan_io),
            requests_before: before.requests,
            bytes_before: before.bytes,
            last_scan: Arc::clone(&self.last_scan),
        }))
    }
}

/// One morsel of a [`ScanPlan`]: the unit a pipeline worker claims and
/// decodes itself.
enum Morsel {
    /// One row group (or page range) of `files[file]`, as the provider's plan
    /// numbers them.
    Group { file: u32, morsel: u32 },
    /// A whole file: the provider cannot plan morsels for it, or it carries
    /// positional deletes, whose positions address the file's rows and would
    /// shift under pruning.
    File { file: u32 },
    /// The table's inlined rows, after every file.
    Inlined,
}

/// One surviving file, planned: its projected decode (mapping, physical
/// filters, row-count facts) and, when the provider can plan them, its
/// morsels.
struct PlannedFile {
    file: AttachedFile,
    plan: Option<FilePlan>,
    morsels: Option<Box<dyn crate::format::FileMorsels>>,
}

/// A planned lake scan: what the engine's pipeline shares between its workers.
///
/// The plan owns everything a decode needs (files, morsel plans, storage
/// handles), so it outlives the statement's `AttachedLake` borrow. Its
/// [`ScanIo`] is planned at construction and completed with the backend's
/// request and byte counters when the scan reports.
struct ScanPlan {
    label: String,
    names: Vec<String>,
    types: Vec<pinkdb::DataType>,
    entry: AttachedEntry,
    files: Vec<PlannedFile>,
    morsels: Vec<Morsel>,
    inlined: Option<DataTable>,
    /// `Some` when the scan decodes a projection; `None` decodes every column.
    columns: Option<Vec<usize>>,
    storage: Arc<dyn Storage>,
    root: std::path::PathBuf,
    footers: Arc<FooterCache>,
    predicates: Vec<pinkdb::attached::PrunePredicate>,
    /// Which morsel each worker claims next.
    cursor: pinkdb::MorselCursor,
    io: Mutex<ScanIo>,
    requests_before: u64,
    bytes_before: u64,
    last_scan: Arc<Mutex<Option<ScanIo>>>,
}

impl ScanPlan {
    /// Decode morsel `index`, handing `sink` its rows.
    ///
    /// Runs on the pipeline worker that claimed the morsel and reads only that
    /// morsel's bytes; nothing here waits on another worker.
    fn decode(
        &self,
        index: u64,
        sink: &mut dyn FnMut(DataChunk) -> pinkdb::Result<()>,
    ) -> pinkdb::Result<()> {
        let Some(morsel) = self.morsels.get(index as usize) else {
            return Err(PinkDbError::Execution(format!(
                "lake morsel {index} is out of range for a {} scan ({} morsels)",
                self.label,
                self.morsels.len()
            )));
        };
        match morsel {
            Morsel::Group { file, morsel } => {
                let planned = &self.files[*file as usize];
                let (Some(plan), Some(morsels)) = (planned.plan.as_ref(), planned.morsels.as_ref())
                else {
                    return Err(PinkDbError::Execution(format!(
                        "table \"{}\" planned morsel {morsel} of \"{}\" without a plan",
                        self.entry.key(),
                        planned.file.rel
                    )));
                };
                let mut rows = 0u64;
                morsels.decode(*morsel as usize, &mut |chunk| {
                    rows += push_physical(&self.entry, &planned.file, plan, chunk, sink)?;
                    Ok(())
                })?;
                // `rows` is what the plan would decode without a pushed
                // predicate; the decode emits the subset the predicate keeps.
                let expected = morsels.rows(*morsel as usize) - morsels.dropped(*morsel as usize);
                if rows != expected {
                    return Err(PinkDbError::Execution(format!(
                        "corrupt: {}: table \"{}\": data file \"{}\" streamed {rows} rows, \
                         but the manifest says {expected}",
                        self.storage.abs(&planned.file.rel).display(),
                        self.entry.key(),
                        planned.file.rel
                    )));
                }
                Ok(())
            }
            Morsel::File { file } => {
                let planned = &self.files[*file as usize];
                let (table, _present) = load_file(
                    &self.storage,
                    &self.root,
                    &self.footers,
                    &self.entry,
                    &planned.file,
                    self.columns.as_deref(),
                    &self.predicates,
                )
                .map_err(scan_error)?;
                for index in 0..table.num_chunks() {
                    let chunk = table.scan_chunk(index)?.ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "chunk {index} of \"{}\" disappeared",
                            planned.file.rel
                        ))
                    })?;
                    sink(chunk)?;
                }
                Ok(())
            }
            Morsel::Inlined => {
                let Some(table) = self.inlined.as_ref() else {
                    return Err(PinkDbError::Execution(format!(
                        "{} has no inlined rows",
                        self.label
                    )));
                };
                for index in 0..table.num_chunks() {
                    let chunk = table.scan_chunk(index)?.ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "inlined chunk {index} of \"{}\" disappeared",
                            self.entry.key()
                        ))
                    })?;
                    sink(chunk)?;
                }
                Ok(())
            }
        }
    }

    /// What the plan knows about logical column `column` in morsel `index`
    /// before decoding it: a row group's statistics from its file's morsel
    /// plan, or a whole file's zone map. `None` when unknown, or when the
    /// file stores the column as another type (its bounds would compare
    /// differently).
    fn morsel_stats(&self, index: u64, column: usize) -> Option<(pinkdb::ColumnStats, u64)> {
        match self.morsels.get(index as usize)? {
            Morsel::Group { file, morsel } => {
                let planned = self.files.get(*file as usize)?;
                let plan = planned.plan.as_ref()?;
                let physical = plan.mapping.physical.get(column).copied().flatten()?;
                if plan.physical_schema.get(physical)?.1 != *self.types.get(column)? {
                    return None;
                }
                planned.morsels.as_ref()?.stats(*morsel as usize, physical)
            }
            Morsel::File { file } => {
                let file = &self.files.get(*file as usize)?.file;
                let stats = file.zone.as_ref()?.get(column)?.clone();
                Some((stats, file.rows))
            }
            Morsel::Inlined => None,
        }
    }

    /// The scan's accounting: what the plan pruned, plus what the decodes read
    /// from the backend since the plan was built.
    fn io(&self) -> pinkdb::ScanIo {
        let mut io = *self.io.lock().unwrap_or_else(|p| p.into_inner());
        let stats = self.storage.stats();
        io.requests = stats.requests.saturating_sub(self.requests_before);
        io.bytes = stats.bytes.saturating_sub(self.bytes_before);
        *self.last_scan.lock().unwrap_or_else(|p| p.into_inner()) = Some(io);
        io.to_morsel()
    }
}

impl std::fmt::Debug for ScanPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanPlan")
            .field("label", &self.label)
            .field("files", &self.files.len())
            .field("morsels", &self.morsels.len())
            .finish_non_exhaustive()
    }
}

/// The engine's view of a [`ScanPlan`]: workers claim morsel indices and
/// decode them.
#[derive(Debug)]
struct PlanSource {
    plan: Arc<ScanPlan>,
}

impl pinkdb::MorselSource for PlanSource {
    fn names(&self) -> &[String] {
        &self.plan.names
    }

    fn types(&self) -> &[pinkdb::DataType] {
        &self.plan.types
    }

    fn label(&self) -> &str {
        &self.plan.label
    }

    fn next_morsel(&self) -> pinkdb::Result<Option<u64>> {
        Ok(self.plan.cursor.claim())
    }

    fn decode(
        &self,
        morsel: u64,
        sink: &mut dyn FnMut(DataChunk) -> pinkdb::Result<()>,
    ) -> pinkdb::Result<()> {
        self.plan.decode(morsel, sink)
    }

    fn morsel_stats(&self, morsel: u64, column: usize) -> Option<(pinkdb::ColumnStats, u64)> {
        self.plan.morsel_stats(morsel, column)
    }

    fn io(&self) -> pinkdb::ScanIo {
        self.plan.io()
    }
}

/// Plan one surviving file: its projected decode plan and, when the provider
/// can plan morsels over it, the morsel plan itself.
///
/// A file with positional deletes decodes whole: delete positions address the
/// file's row sequence, and a pruned morsel would shift them.
fn planned_file(
    io: &FileIo<'_>,
    entry: &AttachedEntry,
    file: &AttachedFile,
    columns: Option<&[usize]>,
    predicates: &[pinkdb::attached::PrunePredicate],
) -> Result<PlannedFile> {
    let abs = io.storage.abs(&file.rel);
    let data_file = DataFile::new(&abs, &file.rel, Arc::clone(io.storage)).with_footer(file.footer);
    let plan = if file.deletes.is_empty() {
        file_plan(
            io.storage, io.footers, entry, file, &data_file, columns, predicates,
        )?
    } else {
        None
    };
    let morsels = match &plan {
        Some(plan) => entry
            .format
            .morsels(&data_file, &plan.decoded, &plan.filters)?,
        None => None,
    };
    Ok(PlannedFile {
        file: file.clone(),
        plan,
        morsels,
    })
}

/// Project one decoded physical chunk onto the table's logical schema and hand
/// it to `sink`, returning the rows pushed.
///
/// The sink gets the engine's scan-chunk shape straight from the decoder: a
/// requested column is `num_rows` long (the decoded vector, shared), one the
/// file predates is its DEFAULT as a constant, and a column the projection did
/// not request is a zero-length placeholder. No `DataTable`, statistics pass or
/// re-split stands between the provider and the scan.
fn push_physical(
    entry: &AttachedEntry,
    file: &AttachedFile,
    plan: &FilePlan,
    chunk: DataChunk,
    sink: &mut dyn FnMut(DataChunk) -> std::result::Result<(), PinkDbError>,
) -> pinkdb::Result<u64> {
    let key = entry.key();
    let num_rows = chunk.num_rows();
    // The chunk has either the file's full physical width or just the requested
    // columns, in order, and a column it did not decode is a zero-length
    // placeholder (see `DataChunk::projected`).
    let width = plan.physical_schema.len();
    let full_width = chunk.columns.len() == width;
    if !full_width && chunk.columns.len() != plan.decoded.len() {
        return Err(PinkDbError::Execution(format!(
            "internal: table \"{key}\": data file \"{}\" pushed {} of {width} columns",
            file.rel,
            chunk.columns.len()
        )));
    }
    // The logical schema: the record's columns (type and DEFAULT), or, for an
    // entry without a schema record, the file's own columns one to one.
    let logical: Vec<(pinkdb::DataType, Option<&pinkdb::Value>)> = match &entry.record {
        Some(record) => record
            .columns
            .iter()
            .map(|column| (column.dtype, column.default.as_ref()))
            .collect(),
        None => plan
            .physical_schema
            .iter()
            .map(|(_, dtype)| (*dtype, None))
            .collect(),
    };
    if logical.len() != plan.wanted.len() {
        return Err(PinkDbError::Execution(format!(
            "internal: table \"{key}\": data file \"{}\" planned {} logical columns, the \
             schema has {}",
            file.rel,
            plan.wanted.len(),
            logical.len()
        )));
    }
    let mut columns = Vec::with_capacity(logical.len());
    for (index, (dtype, default)) in logical.into_iter().enumerate() {
        let vector = if !plan.wanted[index] {
            Arc::new(pinkdb::Vector::with_capacity(dtype, 0))
        } else if let Some(physical) = plan.mapping.physical[index] {
            let position = if full_width {
                physical
            } else {
                plan.decoded.binary_search(&physical).map_err(|_| {
                    PinkDbError::Execution(format!(
                        "internal: table \"{key}\": data file \"{}\" did not decode column \
                         {physical}",
                        file.rel
                    ))
                })?
            };
            let vector = chunk
                .columns
                .get(position)
                .filter(|vector| vector.len() == num_rows)
                .ok_or_else(|| {
                    PinkDbError::Execution(format!(
                        "internal: table \"{key}\": data file \"{}\" pushed column {physical} \
                         short of {num_rows} rows",
                        file.rel
                    ))
                })?;
            // The decoder's own vector: a projected chunk is read-only.
            Arc::clone(vector)
        } else {
            // The file predates the column: every row reads its DEFAULT.
            Arc::new(pinkdb::Vector::constant(
                dtype,
                default.unwrap_or(&pinkdb::Value::Null),
                num_rows,
            )?)
        };
        columns.push(vector);
    }
    sink(DataChunk::projected(columns, num_rows))?;
    Ok(num_rows as u64)
}

/// The engine's form of a lake error: its message carries the lake's own
/// context (`loading data file "…"`, `scan of "…"`), which a caller needs to
/// tell a missing file from a corrupt one.
fn scan_error(error: LakeError) -> PinkDbError {
    PinkDbError::Execution(error.to_string())
}

impl AttachedLake for LakeAttachment {
    fn location(&self) -> &str {
        &self.location
    }

    fn tables(&self) -> &[AttachedTable] {
        &self.tables
    }

    fn scan_source(
        self: Arc<Self>,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
    ) -> std::result::Result<Box<dyn pinkdb::MorselSource>, PinkDbError> {
        let plan = self.plan_scan(db, table, columns, predicates)?;
        Ok(Box::new(PlanSource { plan }))
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

    fn scan_source_at(
        self: Arc<Self>,
        db: &str,
        table: &str,
        at: AtSpec,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::PrunePredicate],
    ) -> std::result::Result<Box<dyn pinkdb::MorselSource>, PinkDbError> {
        let version = self.resolve_version(at)?;
        let view = self.view(version)?;
        match view.shared() {
            Some(view) => view.scan_source(db, table, columns, predicates),
            // The target is the pinned version: this very attachment.
            None => Arc::clone(&self).scan_source(db, table, columns, predicates),
        }
    }
}

/// One attached table decoded in full, for pink-lake's copy paths
/// (replication, incremental maintenance): a streamed scan drained into one
/// table.
pub(crate) fn load_table<L: AttachedLake + ?Sized>(
    lake: Arc<L>,
    db: &str,
    table: &str,
) -> std::result::Result<Arc<DataTable>, PinkDbError> {
    let source = lake.scan_source(db, table, None, &[])?;
    Ok(Arc::new(pinkdb::materialize(source.as_ref(), table)?))
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
/// columns map onto the table's logical schema, which logical columns the
/// request wants, which physical columns it must decode, and the pushed
/// predicates in physical indices.
struct FilePlan {
    physical_schema: Vec<(String, pinkdb::DataType)>,
    mapping: crate::schema::FileMapping,
    /// One flag per logical column: `columns` as the scan asked for it.
    wanted: Vec<bool>,
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
                values: filter.values.clone(),
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
        wanted,
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
    // The catalog's footer location saves the header probe; the file's
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
    // The lake's readers materialize whole tables (this path is the one that
    // runs for formats without a projected reader), and `from_columns` folds
    // statistics over the rows it is handed. The engine `.pink` reader keeps
    // its data encoded, so decode it here.
    file_table.decode_all().map_err(|e| LakeError::Engine {
        context: format!("decoding data file \"{}\"", file.rel()),
        source: e,
    })?;
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
        footers: Arc::new(Mutex::new(HashMap::new())),
        last_scan: Arc::new(Mutex::new(None)),
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
            let decoded =
                crate::attach::load_table(Arc::clone(&attachment), &table.db, &table.table)
                    .unwrap();
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
                    let v = decoded.get(row, i).unwrap().unwrap();
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

    /// One table's streamed scan, drained: the chunks a query's source
    /// produces, in order.
    fn drain(attachment: &Arc<LakeAttachment>, columns: Option<&[usize]>) -> Vec<DataChunk> {
        let source = Arc::clone(attachment)
            .scan_source("main", "t", columns, &[])
            .unwrap();
        let mut chunks = Vec::new();
        while let Some(morsel) = source.next_morsel().unwrap() {
            source
                .decode(morsel, &mut |chunk| {
                    chunks.push(chunk);
                    Ok(())
                })
                .unwrap();
        }
        chunks
    }

    /// A projected scan decodes only the requested columns, keeps the full
    /// decode's chunking, and leaves the other columns a NULL placeholder per
    /// row.
    #[test]
    fn projected_scans_equal_full_scans_and_decode_only_requested_columns() {
        let dir = scratch("projection_values");
        let lake_dir = dir.join("lake");
        crate::save_store(&projection_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();

        // Ground truth: the full decode.
        let full = drain(&attachment, None);
        let rows: usize = full.iter().map(|chunk| chunk.num_rows()).sum();
        assert_eq!(rows, 20_000);
        assert!(full.len() > 1, "the fixture must be multi-chunk");

        // A sparse projection after the full scan decodes afresh: only the
        // requested columns hold values.
        let projected = drain(&attachment, Some(&[0, 3]));
        assert_eq!(attachment.loads(), 2, "every scan decodes");
        assert_eq!(attachment.columns_decoded(), 7);
        assert_eq!(projected.len(), full.len());
        for (j, (sparse, dense)) in projected.iter().zip(&full).enumerate() {
            assert_eq!(sparse.num_rows(), dense.num_rows());
            for i in 0..dense.columns.len() {
                assert_eq!(
                    sparse.columns[i].len(),
                    dense.columns[i].len(),
                    "chunk {j} column {i} length"
                );
            }
            for row in 0..sparse.num_rows() {
                for i in [0usize, 3] {
                    assert_eq!(
                        sparse.columns[i].get(row),
                        dense.columns[i].get(row),
                        "column {i} row {row}"
                    );
                }
                for i in [1usize, 2, 4] {
                    assert!(
                        sparse.columns[i].get(row).is_null(),
                        "an undecoded column is NULL"
                    );
                }
            }
        }

        // A fresh attachment, so the first decode is the projection itself:
        // exactly columns 1 and 4.
        let (solo, _) = build(&lake, None).unwrap();
        let sparse = drain(&solo, Some(&[1, 4]));
        assert_eq!(solo.loads(), 1);
        assert_eq!(solo.columns_decoded(), 2, "only columns 1 and 4 decoded");
        assert_eq!(sparse.len(), full.len());
        for (sparse, dense) in sparse.iter().zip(&full) {
            for i in 0..dense.columns.len() {
                assert_eq!(sparse.columns[i].len(), dense.columns[i].len());
            }
            for row in 0..sparse.num_rows() {
                for i in [1usize, 4] {
                    assert_eq!(sparse.columns[i].get(row), dense.columns[i].get(row));
                }
                for i in [0usize, 2, 3] {
                    assert!(sparse.columns[i].get(row).is_null());
                }
            }
        }

        // A wider projection decodes its columns afresh: nothing is shared
        // between scans (wave 133a).
        let extended = drain(&solo, Some(&[0, 1, 2, 3, 4]));
        assert_eq!(solo.loads(), 2);
        assert_eq!(solo.columns_decoded(), 7);
        for (wide, dense) in extended.iter().zip(&full) {
            for i in 0..dense.columns.len() {
                for row in 0..wide.num_rows() {
                    let (a, b) = (wide.columns[i].get(row), dense.columns[i].get(row));
                    match (&a, &b) {
                        (Value::Double(x), Value::Double(y)) => {
                            assert_eq!(x.to_bits(), y.to_bits(), "column {i} row {row}")
                        }
                        _ => assert_eq!(a, b, "column {i} row {row}"),
                    }
                }
            }
        }

        // An empty projection decodes column 0 only (the chunk shape has to
        // come from somewhere) and never panics.
        let (empty, _) = build(&lake, None).unwrap();
        let none = drain(&empty, Some(&[]));
        assert_eq!(empty.columns_decoded(), 1);
        let none_rows: usize = none.iter().map(|chunk| chunk.num_rows()).sum();
        assert_eq!(none_rows, 20_000);
        assert_eq!(none.len(), full.len());
        for (sparse, dense) in none.iter().zip(&full) {
            for row in 0..sparse.num_rows() {
                assert_eq!(sparse.columns[0].get(row), dense.columns[0].get(row));
                for i in 1..sparse.columns.len() {
                    assert!(sparse.columns[i].get(row).is_null());
                }
            }
        }

        // An out-of-range projection is unprovable: every column decodes.
        let (oob, _) = build(&lake, None).unwrap();
        let all = drain(&oob, Some(&[9]));
        assert_eq!(oob.columns_decoded(), 5);
        assert_eq!(all.len(), full.len());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The engine's attached scan path drives `scan_source`: each query
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

    /// Drain one planned scan morsel by morsel, in claim order, and return the
    /// accounting it reported.
    fn drain_scan(
        attachment: &Arc<LakeAttachment>,
        table: &AttachedTable,
        columns: Option<&[usize]>,
        predicates: &[pinkdb::attached::PrunePredicate],
        sink: &mut dyn FnMut(DataChunk),
    ) -> ScanIo {
        let source = Arc::clone(attachment)
            .scan_source(&table.db, &table.table, columns, predicates)
            .unwrap();
        while let Some(morsel) = source.next_morsel().unwrap() {
            source
                .decode(morsel, &mut |chunk| {
                    sink(chunk);
                    Ok(())
                })
                .unwrap();
        }
        // The engine asks the source for its accounting once the scan ends;
        // the source publishes `last_scan` from there.
        source.io();
        attachment
            .last_scan()
            .expect("the scan reports its accounting")
    }

    /// Wave 133a–b: a planned scan prunes files from the catalog statistics,
    /// reports what it read, hands morsels out in claim order and keeps file
    /// then row order.
    #[test]
    fn planned_scans_prune_report_io_and_keep_order() {
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
        let io = drain_scan(&attachment, &table, None, &[], &mut |chunk| {
            for row in 0..chunk.num_rows() {
                ordered.push(chunk.columns[0].get(row));
            }
        });
        let expected: Vec<Value> = (0..16_000).map(Value::Integer).collect();
        assert_eq!(ordered, expected, "a scan keeps file then row order");
        assert_eq!(io.files, 4);
        assert_eq!(io.files_pruned, 0);
        assert_eq!(io.requests, 0, "a local lake issues no transport requests");
        assert_eq!(io.bytes, 0);

        // Catalog-side pruning: only the file whose zone map can match is read.
        let predicates = [pinkdb::attached::PrunePredicate {
            column: 0,
            op: pinkdb::attached::PruneOp::GtEq,
            value: Some(Value::Integer(12_000)),
            path: None,
            values: Vec::new(),
        }];
        let mut pruned = Vec::new();
        let io = drain_scan(&attachment, &table, None, &predicates, &mut |chunk| {
            for row in 0..chunk.num_rows() {
                pruned.push(chunk.columns[0].get(row));
            }
        });
        assert_eq!(io.files, 1, "one file survives the zone map");
        assert_eq!(io.files_pruned, 3);
        assert_eq!(
            pruned,
            (12_000..16_000).map(Value::Integer).collect::<Vec<Value>>()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Wave 133d: a pushed `IN` list prunes lake files (and store chunks)
    /// only when no list value can match; every answer equals the same
    /// predicate evaluated row by row in a projection, which the scan does
    /// not push.
    #[test]
    fn in_lists_prune_lakes_without_dropping_matching_rows() {
        let dir = scratch("in_pushdown");
        let lake_dir = dir.join("lake");
        let mut base = PinkDb::new();
        base.execute("CREATE TABLE t (i INTEGER, v VARCHAR, n BIGINT);")
            .unwrap();
        {
            let t = base.table_mut("t").unwrap();
            for r in 0..3000i32 {
                let v = if r % 11 == 0 {
                    Value::Null
                } else {
                    Value::Varchar(format!("{:02}", r % 40))
                };
                t.append_row(vec![Value::Integer(r), v, Value::Bigint(r as i64 * 3)])
                    .unwrap();
            }
        }
        crate::save_store(&base, &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();
        let mut lake_db = PinkDb::new();
        lake_db
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        let mut plain = base;
        let lists = [
            "v IN ('03', '07', '39')",
            "v IN ('03', NULL, '07')",
            "v IN ('00')",
            "v IN ('03', '07') AND i > 1000",
            "i IN (1, 2, 3, 2999)",
            "i IN (1, 2.5, 3)",
            "n IN (0, 3, -3, 8997)",
        ];
        for list in lists {
            let sql = |cond: &str| format!("SELECT i, v FROM t WHERE {cond} ORDER BY i;");
            let got = query(&mut lake_db, &sql(list));
            // The same predicate marked in a projection is evaluated per row
            // with no pushed predicate at all: the ground truth.
            let marked = query(
                &mut lake_db,
                &sql(&format!("(CASE WHEN {list} THEN 1 ELSE 0 END) = 1")),
            );
            assert_eq!(got, marked, "lake: {list}");
            assert_eq!(got, query(&mut plain, &sql(list)), "store: {list}");
        }
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
