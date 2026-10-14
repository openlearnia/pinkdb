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
//!   catalog.json                current catalog (authoritative for "latest")
//!   snapshots/<version>.json    immutable manifests, one per published version
//!   data/<table>/<uuid>.vortex  immutable per-table data files (v2, Vortex)
//!   data/<table>/<uuid>.pink    v1 data files (engine persistence bytes)
//! ```
//!
//! `catalog.json` and every snapshot manifest are JSON of the same shape:
//!
//! ```json
//! {
//!   "format_version": 2,
//!   "updated": "2026-10-14T12:34:56.789Z",
//!   "version": 2,
//!   "tables": {
//!     "main.t":      { "files": ["data/main.t/3f2a….vortex"], "rows": 5, "format": "vortex" },
//!     "analytics.u": { "files": ["data/analytics.u/9c01….vortex"], "rows": 1, "format": "vortex" }
//!   }
//! }
//! ```
//!
//! Schema and typing stay inside the data files. **Vortex is the default
//! format for new writes** ([`FORMAT_VERSION`] 2): every new data file is a
//! plain Vortex file holding one struct array — one field per column, chunk
//! boundaries preserved, `-0.0` and NaN payloads bit-exact — plus a
//! `pinkdb.table` metadata segment carrying the table's `db`/`table` identity
//! and its pinkdb column schema (the `vortex_file` module docs have the
//! type mapping; DATE/TIMESTAMP are raw `i32`/`i64` storage because the native
//! `vortex.timestamp` extension cannot represent the full engine range).
//! A `format_version` 1 lake — the wave-34 writer — stays readable: its
//! entries have no `format` field and its data files are the engine's own
//! `.pink` single-table persistence bytes ([`PinkDb::save`] / [`PinkDb::open`]),
//! which a data file still opens standalone. Opening an old lake and saving
//! again publishes a v2 snapshot: a full rewrite in Vortex (the migration
//! path). A manifest with any other `format_version` — or a v2 entry that
//! omits `format` or declares anything but `"vortex"` — is a clear
//! [`LakeError::Unsupported`]/[`LakeError::Corrupt`] error, never a guess.
//! Decoding a corrupt Vortex data file is the same clear [`LakeError::Corrupt`]
//! error: the whole decoder boundary is guarded, so a panic inside Vortex's
//! malformed-layout paths is caught and reported (the default panic hook may
//! still print one line to stderr first; the crate does not swap the global
//! hook).
//!
//! # Snapshots and versioning
//!
//! [`Lake::write_from_store`] writes a full snapshot: every table of every
//! database of the store is written to a fresh data file (one file per table
//! per version — a writer never appends and never reuses), then the manifest is
//! published as `snapshots/<version>.json`, then `catalog.json` is replaced
//! with the same bytes. A version number is `previous + 1`, computed with
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
//! # Crash safety and durability
//!
//! Publication order is data files → snapshot manifest → `catalog.json`. Each
//! write goes to a **unique** temp sibling (`<name>.<uuid>.tmp`, created with
//! `create_new(true)` so a planted symlink or an older writer's temp file is
//! never followed or truncated), is fsynced, and is then renamed (catalog,
//! data files) or hard-linked (snapshot manifests) into place; the parent
//! directory is fsynced afterwards, best-effort. A crash anywhere can only
//! leave *unreferenced* leftovers: the previous catalog still loads, and
//! stray data files, stray `.tmp` files, and snapshots above the catalog's
//! version are ignored — [`Lake::versions`] lists only manifests that parse
//! and declare their own canonical version. A stray manifest at exactly the
//! *next* version cannot be told apart from a published one, so a save that
//! lands on it refuses with [`LakeError::Conflict`] instead of replacing it;
//! delete the unpublished stray (the catalog still points at the previous
//! version) to continue.
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
//! byte-identical to the snapshot of its version.
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
//! - Data files are **plaintext** (both `.pink` and Vortex); the engine's
//!   `encryption` feature is not wired into a lake (the CLI keeps
//!   `PINKDB_KEY` for `.save`/`.open` across `.lake open`, but lake files
//!   themselves stay plaintext). Data files carry **no checksums**: mutating
//!   a data file in place is undetected by design. The checks that do run on
//!   load are structural: a v2 file's `pinkdb.table` metadata must name the
//!   catalog's table, agree with the Vortex field names/types, and the chunk
//!   and row counts must match — so a file swapped in from a *differently
//!   named* table is rejected. Swapping two files of the *same* table
//!   (same name, same schema, same row count) is not detectable, and a
//!   tampered file can still decode to different values or fail with a
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
//!   conditional writes degrades (once, noted) to a check-then-put with the
//!   documented window. Single-PUT objects only: no multipart, so one table
//!   file must be ≤ 5 GiB (see the README).

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use pinkdb::{DataTable, DataType, PinkDb, Vector};

mod catalog;
mod local;
#[cfg(test)]
mod mock_s3;
mod storage;
#[cfg(feature = "s3")]
mod s3;
mod time;
mod vortex_file;

use catalog::{Catalog, FileFormat, TableEntry};
use local::{LocalStorage, CATALOG_FILE};
use storage::{Head, Location, Storage, VersionTag};

#[cfg(feature = "s3")]
pub use s3::S3Config;

/// The catalog format version this build reads and writes.
///
/// Version 2 is the Vortex default: every table entry declares its data file
/// `format` (`"vortex"`). Version 1 (the wave-34 writer) is still read — its
/// entries carry no `format` and its data files are engine `.pink` bytes.
pub const FORMAT_VERSION: u32 = 2;

/// The oldest catalog format version this build still reads.
pub const MIN_FORMAT_VERSION: u32 = 1;

/// The largest manifest `version` this build reads or writes.
///
/// `version + 1` is computed with `checked_add`, so `u64::MAX` is provably
/// unpublishable; capping one below it keeps every version a writer can
/// publish loadable forever. A manifest above the cap (a tampered or
/// truncated `version: u64::MAX`) is [`LakeError::Corrupt`], not a wrap.
pub const MAX_VERSION: u64 = u64::MAX - 1;

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
    /// Filesystem failure, with the operation that failed.
    Io { context: String, source: std::io::Error },
    /// The engine rejected the data or the store shape.
    Engine { context: String, source: pinkdb::PinkDbError },
    /// The Vortex layer rejected data while encoding or decoding a data file
    /// (the error is a string because Vortex errors are not `std` error
    /// sources).
    Vortex { context: String, message: String },
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
            LakeError::Unsupported { path, found } => write!(
                f,
                "unsupported lake format_version {found} at {} (this build reads format_version \
                 {MIN_FORMAT_VERSION} in its v1 `.pink` form and reads/writes \
                 format_version {FORMAT_VERSION} in its Vortex form)",
                path.display()
            ),
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
                write!(f, "object storage access error at {}: {message}", path.display())
            }
            LakeError::Io { context, source } => write!(f, "lake I/O error ({context}): {source}"),
            LakeError::Engine { context, source } => {
                write!(f, "lake data error ({context}): {source}")
            }
            LakeError::Vortex { context, message } => {
                write!(f, "lake vortex error ({context}): {message}")
            }
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
    storage: Box<dyn Storage>,
    format_version: u32,
    version: u64,
}

impl Lake {
    /// Create a new lake at `dir`: the directory (and parents) plus an empty
    /// catalog at snapshot version 1 (a `format_version` 2 manifest) and its
    /// snapshot. Fails if `dir` already holds a lake.
    ///
    /// The empty version-1 snapshot is *linked* into place (never replaced).
    /// If a concurrent or crashed create already left a valid empty version-1
    /// snapshot, it is adopted instead of rewritten — both concurrent creates
    /// can only end with exactly one version-1 catalog, byte-identical to its
    /// snapshot, and the loser retries [`Lake::open`] via
    /// [`Lake::open_or_create`].
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        Self::create_with(Box::new(LocalStorage::new(dir.as_ref())))
    }

    /// Create a lake at a location string: a local directory, or an
    /// `s3://bucket/prefix` URL when the `s3` feature is built (a clear
    /// [`LakeError::FeatureMissing`] otherwise). S3 credentials, endpoint and
    /// region come from the usual environment variables (`AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_ENDPOINT_URL`/
    /// `AWS_ENDPOINT_URL_S3`, `AWS_REGION`/`AWS_DEFAULT_REGION`,
    /// `AWS_S3_FORCE_PATH_STYLE`); use `Lake::create_s3` (with the `s3`
    /// feature) for explicit configuration.
    pub fn create_url(location: &str) -> Result<Self> {
        Self::create_with(storage_for(Location::parse(location)?)?)
    }

    fn create_with(storage: Box<dyn Storage>) -> Result<Self> {
        let root = storage.abs("");
        if storage.exists(CATALOG_FILE)? {
            return Err(LakeError::AlreadyALake { path: root });
        }
        storage.create_dir("")?;
        let manifest = Catalog {
            format_version: FORMAT_VERSION,
            updated: time::iso8601_utc(SystemTime::now()),
            version: 1,
            tables: BTreeMap::new(),
        };
        let bytes = manifest_json(&manifest, &root)?;
        // Adopt (do not rewrite) an empty version-1 snapshot a concurrent or
        // interrupted create already put there: the catalog must stay
        // byte-identical to the snapshot of its version.
        let bytes = if storage.put_new("snapshots/1.json", &bytes)? {
            bytes
        } else {
            let rel = "snapshots/1.json";
            let path = storage.abs(rel);
            let existing = storage.read(rel, &format!("reading {}", path.display()))?;
            let parsed = read_manifest(&path, &existing).map_err(|e| LakeError::Conflict {
                path: path.clone(),
                message: format!("snapshots/1.json exists but does not parse ({e}); refusing to adopt it"),
            })?;
            if parsed.version != 1 || !parsed.tables.is_empty() {
                return Err(LakeError::Conflict {
                    path,
                    message: "snapshots/1.json exists but is not the empty version 1 snapshot; \
                              refusing to adopt it"
                        .to_string(),
                });
            }
            existing
        };
        if !storage.put_new(CATALOG_FILE, &bytes)? {
            return Err(LakeError::AlreadyALake { path: root });
        }
        Ok(Self {
            root,
            storage,
            format_version: FORMAT_VERSION,
            version: 1,
        })
    }

    /// Open an existing lake: `dir/catalog.json` must exist and parse.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(Box::new(LocalStorage::new(dir.as_ref())))
    }

    /// Open a lake at a location string: a local directory, or an
    /// `s3://bucket/prefix` URL when the `s3` feature is built (see
    /// [`Lake::create_url`] for the environment variables). Use
    /// `Lake::open_s3` (with the `s3` feature) for explicit S3
    /// configuration.
    pub fn open_url(location: &str) -> Result<Self> {
        Self::open_with(storage_for(Location::parse(location)?)?)
    }

    fn open_with(storage: Box<dyn Storage>) -> Result<Self> {
        let root = storage.abs("");
        let manifest = read_catalog(&*storage, &root)?;
        Ok(Self {
            root,
            storage,
            format_version: manifest.format_version,
            version: manifest.version,
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

    /// Create a lake from explicit [`S3Config`] (endpoint, region,
    /// credentials, bucket/prefix) instead of the environment.
    #[cfg(feature = "s3")]
    pub fn create_s3(config: S3Config) -> Result<Self> {
        Self::create_with(Box::new(s3::S3Storage::new(config)?))
    }

    /// Open a lake from explicit [`S3Config`].
    #[cfg(feature = "s3")]
    pub fn open_s3(config: S3Config) -> Result<Self> {
        Self::open_with(Box::new(s3::S3Storage::new(config)?))
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

    /// The lake location: a directory path or the `s3://bucket/prefix` URL.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The catalog `format_version` this handle last saw: the version the lake
    /// was opened with, or [`FORMAT_VERSION`] after a successful save (a save
    /// from a v1 lake migrates it to v2).
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// The current version (from `catalog.json`, cached after open/save).
    pub fn current_version(&self) -> u64 {
        self.version
    }

    /// Published snapshot versions, ascending: every `snapshots/<v>.json`
    /// that is named canonically (`<decimal>`, no leading zeros), declares
    /// its own version `v`, and parses. A leftover manifest from an
    /// interrupted publish (`v > current`) is ignored, as are corrupt
    /// manifests, `01.json`-style names, and non-numeric names — they are
    /// never loadable through this handle.
    pub fn versions(&self) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for name in self.storage.list("snapshots")? {
            let Some(v) = name
                .strip_suffix(".json")
                .and_then(canonical_version_number)
            else {
                continue;
            };
            if v < 1 || v > self.version {
                continue;
            }
            let rel = format!("snapshots/{name}");
            let path = self.storage.abs(&rel);
            let Ok(bytes) = self.storage.read(&rel, &format!("reading {}", path.display())) else {
                continue;
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

    /// The current version's tables: `(db.table, rows)`, sorted by name.
    pub fn tables(&self) -> Result<Vec<(String, u64)>> {
        let (manifest, _) = self.manifest(None)?;
        Ok(manifest
            .tables
            .into_iter()
            .map(|(key, entry)| (key, entry.rows))
            .collect())
    }

    /// Rebuild a [`PinkDb`] from the latest version (`None`) or a specific
    /// snapshot. Views are session-local in the engine and never come back.
    pub fn load(&self, version: Option<u64>) -> Result<PinkDb> {
        let (manifest, manifest_path) = self.manifest(version)?;
        self.rebuild(&manifest, &manifest_path)
    }

    /// Referenced data files that are missing (or not a valid `data/…` path):
    /// `(table key, path)`, sorted by table then path. This is the cheap
    /// integrity check behind `.lake tables`/`.lake info`: a table can only
    /// be advertised while every data file it names is present. It does not
    /// read the files (that is what [`Lake::load`] does).
    pub fn missing_data_files(&self) -> Result<Vec<(String, PathBuf)>> {
        let (manifest, _) = self.manifest(None)?;
        let mut out = Vec::new();
        for (key, entry) in &manifest.tables {
            for rel in &entry.files {
                if validate_rel_path(rel).is_err()
                    || !matches!(self.storage.head(rel)?, Head::File)
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
        let mut tables: BTreeMap<String, TableEntry> = BTreeMap::new();
        let mut bytes_written = 0u64;
        let mut rows = 0u64;
        for dbname in db.list_databases() {
            for name in db.list_tables_in(&dbname) {
                let src = table_in(db, &dbname, &name)?;
                let key = format!("{dbname}.{name}");
                let dir = format!("data/{}", encode_key(&key));
                let format = FileFormat::Vortex;
                let id = fresh_id();
                // The v2 writer emits exactly one Vortex file per table (the
                // loader is driven by the catalog's `format` field, never by
                // the extension).
                let rel_file = format!("{dir}/{id}.vortex");
                // v2 encodes the whole table into a Vortex file in memory,
                // then publishes it (locally: a unique `create_new` temp
                // sibling + fsync + rename; over S3: one atomic PUT). Nothing
                // is referenced by the catalog until the manifest is
                // published below.
                let bytes = vortex_file::encode_table(&dbname, &name, src)?;
                self.storage.put(&rel_file, &bytes)?;
                bytes_written += bytes.len() as u64;
                rows += src.num_rows() as u64;
                tables.insert(
                    key,
                    TableEntry {
                        files: vec![rel_file],
                        rows: src.num_rows() as u64,
                        format: Some(format),
                    },
                );
            }
        }
        let manifest = Catalog {
            format_version: FORMAT_VERSION,
            updated: time::iso8601_utc(SystemTime::now()),
            version,
            tables,
        };
        let bytes = manifest_json(&manifest, &self.root)?;
        // Create-only, never replace: an existing snapshot version is a
        // published version (or an unpublished stray the caller must remove).
        // Locally a hard link; over S3 `If-None-Match: *`.
        if !self.storage.put_new(&snapshot_rel, &bytes)? {
            return Err(self.snapshot_exists_error(version));
        }
        bytes_written += bytes.len() as u64;
        // Re-check that the catalog is still the version our snapshot was
        // taken from, right before replacing it, and replace it
        // conditionally: `If-Match: <etag>` over S3 (a 412 is the conflict
        // below), a file-identity re-check locally. A concurrent writer can
        // never end up with a catalog that disagrees with the snapshot of its
        // version.
        let catalog_tag = self.ensure_catalog_unchanged()?;
        if !self
            .storage
            .put_if_match(CATALOG_FILE, &bytes, &catalog_tag)?
        {
            return Err(LakeError::Conflict {
                path: self.storage.abs(CATALOG_FILE),
                message: format!(
                    "catalog.json changed while this save was in flight (this handle is based \
                     on version {}); reopen the lake",
                    self.version
                ),
            });
        }
        bytes_written += bytes.len() as u64;
        self.version = version;
        // A save always publishes a v2 (Vortex) manifest, so a handle that
        // opened a v1 lake reports v2 once it has migrated it.
        self.format_version = FORMAT_VERSION;
        Ok(SaveReport {
            version,
            tables: manifest.tables.len(),
            rows,
            bytes_written,
        })
    }

    /// The manifest for the latest version (`None`) or one snapshot, with the
    /// path it was read from (for error attribution).
    fn manifest(&self, version: Option<u64>) -> Result<(Catalog, PathBuf)> {
        match version {
            None => {
                let manifest = read_catalog(&*self.storage, &self.root)?;
                Ok((manifest, self.root.join(CATALOG_FILE)))
            }
            Some(v) => {
                let rel = format!("snapshots/{v}.json");
                let path = self.storage.abs(&rel);
                if !self.storage.exists(&rel)? {
                    return Err(LakeError::VersionNotFound {
                        path: self.root.clone(),
                        version: v,
                    });
                }
                let bytes = self
                    .storage
                    .read(&rel, &format!("reading {}", path.display()))?;
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
    /// file. Databases are recreated from the flat `db.table` keys; `main`
    /// always exists in the result (an empty `main` if it had no tables).
    /// `manifest_path` is the file the manifest came from (catalog or
    /// snapshot) and is what errors about the manifest point at.
    fn rebuild(&self, manifest: &Catalog, manifest_path: &Path) -> Result<PinkDb> {
        let mut db = PinkDb::new();
        for (key, entry) in &manifest.tables {
            let (dbname, table) = split_key(key, manifest_path)?;
            if !db.database_exists(dbname) {
                db.create_database(dbname).map_err(|e| LakeError::Engine {
                    context: format!("recreating database \"{dbname}\""),
                    source: e,
                })?;
            }
            if entry.files.is_empty() {
                return Err(LakeError::Corrupt {
                    path: manifest_path.to_path_buf(),
                    message: format!("table \"{key}\" has no data files"),
                });
            }
            let format = entry.format();
            let mut created = false;
            for (i, rel) in entry.files.iter().enumerate() {
                validate_rel_path(rel)?;
                let path = self.storage.abs(rel);
                let mini = match format {
                    // v1: the engine's own single-table persistence bytes,
                    // opened by path (over object storage this downloads a
                    // temp copy that is removed when dropped).
                    FileFormat::Pink => {
                        let copy = self
                            .storage
                            .local_copy(rel, &format!("loading data file \"{rel}\""))?;
                        PinkDb::open(path_str(copy.path())?).map_err(|e| LakeError::Engine {
                            context: format!("loading data file \"{rel}\" for table \"{key}\""),
                            source: e,
                        })?
                    }
                    // v2: a Vortex file that decodes into the same shape.
                    FileFormat::Vortex => {
                        let bytes = self
                            .storage
                            .read(rel, &format!("loading data file \"{rel}\""))?;
                        vortex_file::decode_table(&path, &bytes)?
                    }
                };
                let (file_db, file_table) = only_table(&mini, rel, &self.root)?;
                if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(table)
                {
                    return Err(LakeError::Corrupt {
                        path,
                        message: format!(
                            "data file holds {file_db}.{}, but the catalog says \"{key}\"",
                            file_table.name
                        ),
                    });
                }
                if !created {
                    db.create_table_in(dbname, table, file_table.schema())
                        .map_err(|e| LakeError::Engine {
                            context: format!("recreating table \"{key}\""),
                            source: e,
                        })?;
                    created = true;
                }
                let dst = db
                    .table_mut_in(dbname, table)
                    .map_err(|e| LakeError::Engine {
                        context: format!("loading table \"{key}\""),
                        source: e,
                    })?;
                if i > 0 && dst.schema() != file_table.schema() {
                    return Err(LakeError::Corrupt {
                        path,
                        message: format!("data files of table \"{key}\" disagree on the schema"),
                    });
                }
                copy_chunks(file_table, dst, rel)?;
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
}

/// Snapshot the store into the lake at `dir`, creating the lake if `dir` is
/// not one yet. Returns the published version. The store is validated
/// *before* the lake directory is created, so a rejected store leaves
/// nothing behind.
pub fn save_store(db: &PinkDb, dir: impl AsRef<Path>) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create(dir)?.write_from_store(db)
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

/// [`save_store_url`] with explicit [`S3Config`] (bucket/prefix/endpoint/
/// region/credentials) instead of the environment.
#[cfg(feature = "s3")]
pub fn save_store_s3(db: &PinkDb, config: S3Config) -> Result<u64> {
    validate_store(db)?;
    Lake::open_or_create_s3(config)?.write_from_store(db)
}

/// The storage backend for a parsed location: local always, S3 only with the
/// `s3` feature (a clear error otherwise).
fn storage_for(location: Location) -> Result<Box<dyn Storage>> {
    match location {
        Location::Local(path) => Ok(Box::new(LocalStorage::new(&path))),
        Location::S3 { bucket, prefix } => s3_storage(&bucket, &prefix),
    }
}

#[cfg(feature = "s3")]
fn s3_storage(bucket: &str, prefix: &str) -> Result<Box<dyn Storage>> {
    Ok(Box::new(s3::S3Storage::from_url(bucket, prefix)?))
}

#[cfg(not(feature = "s3"))]
fn s3_storage(bucket: &str, prefix: &str) -> Result<Box<dyn Storage>> {
    let location = if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    };
    Err(LakeError::FeatureMissing {
        feature: "s3",
        message: format!(
            "the location {location} needs object-storage support; rebuild with the `s3` \
             feature (for the CLI: `cargo build -p pinkdb-cli --features lake,s3`)"
        ),
    })
}

/// The largest encoded `data/<table>` directory segment a writer accepts,
/// so `<segment>/<uuid>.vortex.tmp` fits the common 255-byte name limit
/// (36-char uuid + `.vortex.tmp` = 47 bytes). Also enforced by
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

/// Read and validate `catalog.json`.
fn read_catalog(storage: &dyn Storage, root: &Path) -> Result<Catalog> {
    let path = storage.abs(CATALOG_FILE);
    match storage.head(CATALOG_FILE)? {
        Head::Missing => return Err(LakeError::NotALake { path: root.to_path_buf() }),
        Head::NotAFile => {
            return Err(LakeError::Corrupt {
                path,
                message: "catalog.json is not a regular file".to_string(),
            })
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
/// `format_version` must be in [`MIN_FORMAT_VERSION`]`..=`[`FORMAT_VERSION`].
/// A version 1 manifest is all-`.pink` by definition: an entry there must not
/// declare `"format": "vortex"`. A version 2 manifest must declare a `format`
/// on every entry, and that format must be `"vortex"` — the v2 writer emits
/// Vortex only, so a missing field or a `"pink"` entry is corrupt v2
/// metadata, not a silent `.pink` downgrade.
fn read_manifest(path: &Path, bytes: &[u8]) -> Result<Catalog> {
    let manifest: Catalog = serde_json::from_slice(bytes).map_err(|e| LakeError::Corrupt {
        path: path.to_path_buf(),
        message: e.to_string(),
    })?;
    if manifest.format_version < MIN_FORMAT_VERSION || manifest.format_version > FORMAT_VERSION {
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
    for (key, entry) in &manifest.tables {
        match (manifest.format_version, entry.format) {
            (1, Some(FileFormat::Vortex)) => {
                return Err(LakeError::Corrupt {
                    path: path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" declares a vortex data file in a format_version 1 \
                         manifest (version 1 data files are engine `.pink` bytes)"
                    ),
                });
            }
            (2, None) => {
                return Err(LakeError::Corrupt {
                    path: path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" does not declare a data file format (a format_version 2 \
                         entry requires `\"format\": \"vortex\"`)"
                    ),
                });
            }
            (2, Some(FileFormat::Pink)) => {
                return Err(LakeError::Corrupt {
                    path: path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" declares a pink data file in a format_version 2 \
                         manifest (the v2 writer emits Vortex only; `.pink` data files are \
                         format_version 1)"
                    ),
                });
            }
            _ => {}
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
    let plain = path
        .components()
        .all(|c| matches!(c, Component::Normal(_)));
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
        let safe = b.is_ascii_alphanumeric()
            || b == b'_'
            || b == b'-'
            || (b == b'.' && i > 0);
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

/// The single database + single table a data file must hold.
fn only_table<'a>(mini: &'a PinkDb, rel: &str, root: &Path) -> Result<(String, &'a DataTable)> {
    let dbs = mini.list_databases();
    if dbs.len() != 1 {
        return Err(LakeError::Corrupt {
            path: root.join(rel),
            message: format!("data file must hold exactly one database, has {}", dbs.len()),
        });
    }
    let dbname = dbs.into_iter().next().expect("one database");
    let tables = mini.list_tables_in(&dbname);
    if tables.len() != 1 {
        return Err(LakeError::Corrupt {
            path: root.join(rel),
            message: format!("data file must hold exactly one table, has {}", tables.len()),
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

/// Copy every chunk of `src` into `dst` (deep column clone; bit-exact,
/// including NaN payloads and `-0.0`). `context` names the source for errors.
fn copy_chunks(src: &DataTable, dst: &mut DataTable, context: &str) -> Result<()> {
    for i in 0..src.num_chunks() {
        let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {i} of {context} disappeared while copying"),
        })?;
        let cols: Vec<Vector> = chunk.columns.iter().map(|c| (**c).clone()).collect();
        dst.append_chunk(cols).map_err(|e| LakeError::Engine {
            context: format!("appending chunk {i} of {context}"),
            source: e,
        })?;
    }
    Ok(())
}

/// An empty one-table [`PinkDb`]: `dbname.table` created with `schema`, the
/// default `main` of a fresh store dropped when the table lives elsewhere.
/// The v2 Vortex decoder builds one per data file, so a decoded file has
/// exactly one database and table — the shape a v1 `.pink` data file opens
/// as, which is what the loader's checks expect.
fn empty_single_table_store(
    dbname: &str,
    table: &str,
    schema: Vec<(String, DataType)>,
) -> Result<PinkDb> {
    let mut mini = PinkDb::new();
    if !mini.database_exists(dbname) {
        mini.create_database(dbname).map_err(|e| LakeError::Engine {
            context: format!("creating database \"{dbname}\" while loading"),
            source: e,
        })?;
        mini.drop_database("main").map_err(|e| LakeError::Engine {
            context: format!("dropping the default main database while loading \"{dbname}\""),
            source: e,
        })?;
    }
    mini.create_table_in(dbname, table, schema)
        .map_err(|e| LakeError::Engine {
            context: format!("creating table \"{dbname}.{table}\" while loading"),
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
        let catalog = PathBuf::from("catalog.json");
        assert_eq!(split_key("main.t", &catalog).unwrap(), ("main", "t"));
        assert_eq!(split_key("analytics.a.b", &catalog).unwrap(), ("analytics", "a.b"));
        assert_eq!(split_key("main.", &catalog).unwrap(), ("main", ""));
        let err = split_key("nodot", &catalog).unwrap_err().to_string();
        assert!(err.contains("catalog.json"), "{err}");
        let snapshot = PathBuf::from("snapshots/2.json");
        let err = split_key("nodot", &snapshot).unwrap_err().to_string();
        assert!(err.contains("snapshots/2.json"), "{err}");
    }

    #[test]
    fn canonical_version_numbers_reject_lookalikes() {
        assert_eq!(canonical_version_number("1"), Some(1));
        assert_eq!(canonical_version_number("42"), Some(42));
        assert_eq!(canonical_version_number("18446744073709551615"), Some(u64::MAX));
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
    fn fresh_ids_are_unique_and_uuid_shaped() {
        let a = fresh_id();
        let b = fresh_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        let parts: Vec<&str> = a.split('-').collect();
        assert_eq!(parts.iter().map(|p| p.len()).collect::<Vec<_>>(), vec![8, 4, 4, 4, 12]);
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
