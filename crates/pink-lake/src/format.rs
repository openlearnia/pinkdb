//! Lake data-file format providers (wave 87).
//!
//! The catalog pins one format id per table entry (`"pink"`, `"parquet"`, …).
//! This module maps that id to a [`LakeFormat`] implementation — the seam
//! that lets the lake's catalog, save/commit, attach, maintenance, S3 and
//! encryption machinery work over any data-file encoding.
//!
//! # Registry
//!
//! The registry is process-global and keyed by the format id. The crate
//! registers [`PINK_FORMAT_ID`] lazily — the engine's single-table `.pink`
//! persistence bytes, available in every build.
//!
//! Extensions register additional formats with [`register_lake_format`]. An
//! id names exactly one provider for the life of the process: a **duplicate
//! id is refused** (the catalog's `format` string must resolve
//! deterministically, or one lake could be read differently by two
//! components in the same process).
//!
//! An id with no provider is [`LakeError::UnknownFormat`] whenever that
//! table is read or written; there is never a panic or a silent fallback to
//! `"pink"`. Wave 88 makes the refusal per-table: a lake still opens,
//! [`crate::Lake::unavailable_tables`] lists the affected tables, and only
//! reads/writes of those tables fail. The one exception is the format id
//! removed in wave 104: it is refused for the whole lake with a re-save
//! error, because no build will ever read it again.
//!
//! # The contract
//!
//! A provider owns three things:
//!
//! 1. **Writing** ([`LakeFormat::write`] / [`LakeFormat::write_streamed`]):
//!    encode one table and publish the exact per-column statistics the
//!    catalog block carries — the same [`SavedTableStats`] facts the `.pink`
//!    writer publishes. The shared exactness gate in the `stats` module is
//!    applied by the lake to every provider's output.
//! 2. **Reading** ([`LakeFormat::read`]): decode one data file into the
//!    one-database/one-table [`PinkDb`] shape every lake loader expects.
//! 3. **Optional self-description** ([`LakeFormat::footer_stats`]): formats
//!    whose files carry their own schema can answer a schema-mapping read;
//!    formats that do not return `None` and attach uses the manifest's
//!    CRC-protected statistics block.
//!
//! A provider may additionally advertise
//! [`LakeFormat::supports_projection`] to let attach decode only requested
//! columns; the lake falls back to a full decode otherwise. A projected
//! provider implements [`LakeFormat::read_projected`] (`.pink`: projected
//! column chunks; Parquet: the shared ranged scan core) and reports its
//! file's physical footer via [`LakeFormat::footer_stats`] so a schema record
//! can map logical columns onto it.
//!
//! # Ranged reads (wave 98b)
//!
//! [`DataFile`] exposes the lake storage's positional reader
//! ([`DataFile::read_range`], [`DataFile::range_source`]): object storage and
//! local plaintext files answer with real range reads, an encrypted lake
//! answers `None` (a sealed container must be unsealed whole). Providers use
//! [`RangedFile`] to fetch exactly the footer and projected column segments a
//! read needs.
//!
//! # Writing
//!
//! The lake always writes through [`LakeFormat::write_streamed`] (a unique
//! temp file plus fsync + rename locally, one PUT over S3, sealed as one
//! container for an encrypted lake). The default implementation calls
//! [`LakeFormat::write`] and writes its bytes into the sink, so a provider
//! only has to implement the in-memory form.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use pinkdb::persist::SavedTableStats;
use pinkdb::{ColumnStats, DataTable, DataType, PinkDb, Value};

use crate::storage::{LocalCopy, Storage};
use crate::{LakeError, Result};

/// The format id of the engine's `.pink` single-table persistence bytes — the
/// built-in format every build registers.
pub const PINK_FORMAT_ID: &str = "pink";

/// Where a data file's footer lives inside the file.
///
/// The lake's catalog records it for the formats that carry one (the
/// engine's `.pink` files, `format_version` 7), so a reader fetches the
/// footer **without** probing the file's 20-byte trailer: one ranged read
/// instead of two, and the footer is still checksum-verified before a
/// single byte of it is decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FooterLocation {
    /// The footer's offset in the file.
    pub offset: u64,
    /// The footer's length in bytes.
    pub length: u64,
    /// The footer's CRC32C (the trailer's checksum for it).
    pub crc32c: u32,
}

/// The output of one data-file write: the encoded bytes plus the exact
/// per-column statistics the catalog block publishes for them (the same
/// [`SavedTableStats`] facts the `.pink` writer collects).
#[derive(Debug, Clone)]
pub struct EncodedFile {
    /// The complete data-file bytes (what is stored under `data/…<uuid>.<ext>`).
    pub bytes: Vec<u8>,
    /// Exact per-column statistics, in schema order (the `stats` module's
    /// canonical encoding).
    pub stats: SavedTableStats,
    /// Where the file's footer is, when the format carries one (the engine's
    /// `.pink` files do). The catalog records it, so a reader needs no
    /// trailer probe to find the footer.
    pub footer: Option<FooterLocation>,
}

/// One published data file, as a provider's reader sees it.
///
/// The lake constructs this (there is no public constructor): it carries the
/// file's catalog-relative path, its absolute path for error messages, and
/// access to the storage backend for object-storage and encrypted lakes.
pub struct DataFile<'a> {
    path: &'a Path,
    rel: &'a str,
    storage: Arc<dyn Storage>,
    footer: Option<FooterLocation>,
    deletes: Option<Arc<Vec<u64>>>,
}

impl<'a> DataFile<'a> {
    /// Wrap one data-file reference for a provider (crate-internal: the lake
    /// owns the storage handle).
    pub(crate) fn new(path: &'a Path, rel: &'a str, storage: Arc<dyn Storage>) -> Self {
        Self {
            path,
            rel,
            storage,
            footer: None,
            deletes: None,
        }
    }

    /// Attach the catalog-recorded footer location (the lake knows it; a
    /// provider uses it to fetch the footer without a trailer probe).
    pub(crate) fn with_footer(mut self, footer: Option<FooterLocation>) -> Self {
        self.footer = footer;
        self
    }

    /// Attach the file's positional deletes (the row positions the catalog's
    /// delete files drop). A provider that **prunes** rows (row groups) must
    /// not prune a file with deletes: the delete positions address the file's
    /// rows, and pruning would move them.
    pub(crate) fn with_deletes(mut self, deletes: Vec<u64>) -> Self {
        self.deletes = Some(Arc::new(deletes));
        self
    }

    /// The file's catalog-recorded footer location, when the catalog has one.
    pub fn footer(&self) -> Option<FooterLocation> {
        self.footer
    }

    /// The file's positional deletes, when it has any.
    pub fn deletes(&self) -> Option<&[u64]> {
        self.deletes.as_ref().map(|deletes| deletes.as_slice())
    }

    /// The file's absolute path, for error messages (`s3://…` over object
    /// storage).
    pub fn path(&self) -> &Path {
        self.path
    }

    /// The file's lake-relative path, exactly as the manifest names it.
    pub fn rel(&self) -> &str {
        self.rel
    }

    /// The file's plaintext bytes: a local read, or a whole-object download /
    /// container unseal for object storage and encrypted lakes.
    pub fn read(&self) -> Result<Vec<u8>> {
        self.storage
            .read(self.rel, &format!("loading data file \"{}\"", self.rel))
    }

    /// The file's stored byte length (ciphertext length for an encrypted
    /// lake). Used to size a positional reader.
    pub fn len(&self) -> Result<u64> {
        match self.storage.stat(self.rel)? {
            Some(info) => Ok(info.size),
            None => Err(LakeError::Io {
                context: format!("loading data file \"{}\"", self.rel),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("{} is missing", self.path.display()),
                ),
            }),
        }
    }

    /// Whether the stored object is zero bytes (paired with
    /// [`DataFile::len`]).
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// A byte window of the file's plaintext, when the backend can read one
    /// without downloading/unsealing the whole object.
    pub fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.storage.read_range(
            self.rel,
            offset,
            len,
            &format!("loading data file \"{}\"", self.rel),
        )
    }

    /// Whether [`DataFile::read_range`] is a real positioned read.
    pub fn ranged(&self) -> bool {
        self.storage.ranged_reads()
    }

    /// A positional reader for this file, when the backend supports one and
    /// the object exists. `None` means "fall back to [`DataFile::read`]".
    pub fn range_source(&self) -> Option<Arc<dyn RangedFile>> {
        if !self.storage.ranged_reads() {
            return None;
        }
        let len = self.len().ok()?;
        Some(Arc::new(StorageRange {
            path: self.path.to_path_buf(),
            rel: self.rel.to_string(),
            storage: Arc::clone(&self.storage),
            len,
            parallelism: crate::parallel_limit(),
        }))
    }

    /// The real filesystem path when the backend is local **and** plaintext;
    /// `None` for object storage and encrypted lakes (the caller then falls
    /// back to [`DataFile::read`]).
    pub fn local_path(&self) -> Option<PathBuf> {
        self.storage.local_path(self.rel)
    }

    /// A local path holding the plaintext bytes: the storage's own file, or a
    /// temporary downloaded/decrypted copy removed when the guard drops.
    /// This is the engine `.pink` opener's entry point.
    pub(crate) fn local_copy(&self) -> Result<LocalCopy> {
        self.storage
            .local_copy(self.rel, &format!("loading data file \"{}\"", self.rel))
    }
}

/// A positional reader over one lake data file (wave 98b).
///
/// This is the seam between the lake's ranged [`Storage`] and a format
/// provider (parquet's remote scan, `.pink`'s projected chunk read): the provider
/// names byte windows and the lake fetches exactly them, with the storage's
/// retry policy. Implementations are `Send + Sync` so a scan may read ahead
/// from several threads.
pub trait RangedFile: fmt::Debug + Send + Sync {
    /// The file's absolute path (error messages).
    fn path(&self) -> &Path;
    /// The stored byte length.
    fn len(&self) -> u64;
    /// Whether the file is zero bytes (paired with [`RangedFile::len`]).
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// `len` bytes starting at `offset`.
    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>>;
    /// The bounded in-flight request limit (`PINKDB_HTTPFS_PARALLELISM`).
    fn parallelism(&self) -> usize;
}

/// [`RangedFile`] over the lake's [`Storage`]: one object key plus the shared
/// backend handle.
#[derive(Debug)]
struct StorageRange {
    path: PathBuf,
    rel: String,
    storage: Arc<dyn Storage>,
    len: u64,
    parallelism: usize,
}

impl RangedFile for StorageRange {
    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.storage.read_range(
            &self.rel,
            offset,
            len,
            &format!("loading data file \"{}\"", self.rel),
        )
    }

    fn parallelism(&self) -> usize {
        self.parallelism
    }
}

/// The schema and identity a data file carries itself, read without decoding
/// any column data (the `.pink` footer, a Parquet footer).
#[derive(Debug)]
pub struct FileFooter {
    /// The database the file's metadata names.
    pub db: String,
    /// The table the file's metadata names.
    pub table: String,
    /// The file's exact row count.
    pub rows: u64,
    /// The file's physical schema, in scan order.
    pub schema: Vec<(String, DataType)>,
    /// Exact per-column statistics, gated by the format's own rules.
    pub stats: Vec<ColumnStats>,
}

/// The catalog column ids a lake data file must record as parquet
/// `field_id`s, in table-column order.
///
/// DuckLake readers match a nested column's children by id, so the `element`
/// leaf of an ARRAY group needs its own `ducklake_column.column_id`; a
/// standalone file records none (DuckDB's `COPY … TO` does the same).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldIds {
    /// The column's `ducklake_column.column_id`.
    pub column: i64,
    /// The `element` child's `column_id`, for an ARRAY column.
    pub array_child: Option<i64>,
}

/// One lake data-file format: writer, reader, and optional self-description.
///
/// Providers are shared process-wide through the registry (see
/// [`register_lake_format`]); they must be safe to call concurrently.
pub trait LakeFormat: fmt::Debug + Send + Sync {
    /// The stable lowercase id written in the catalog's `format` field
    /// (`"pink"`, `"parquet"`, …). Ids are unique per process.
    fn id(&self) -> &str;

    /// The data-file name extension, without the dot (`"pink"`, `"parquet"`).
    /// The lake names a file `data/<table>/<uuid>.<extension>`.
    fn extension(&self) -> &str;

    /// Encode `src` as one complete data file and publish its statistics.
    ///
    /// `db`/`table` name the logical table the file will hold (the built-in
    /// writers embed this identity in the file, and the loaders check it
    /// against the catalog key).
    fn write(&self, db: &str, table: &str, src: &DataTable) -> Result<EncodedFile>;

    /// Encode `src` straight into `sink` and publish the same statistics.
    ///
    /// The default buffers through [`LakeFormat::write`]; a provider whose
    /// encoder can stream overrides this so a local save never materializes
    /// the whole file.
    ///
    /// Returns the published statistics and, when the format carries a
    /// footer, where it is in the written file (the catalog records it, so a
    /// reader finds the footer without probing the trailer).
    fn write_streamed(
        &self,
        db: &str,
        table: &str,
        src: &DataTable,
        sink: &mut (dyn Write + Send),
    ) -> Result<(SavedTableStats, Option<FooterLocation>)> {
        let encoded = self.write(db, table, src)?;
        sink.write_all(&encoded.bytes).map_err(|e| LakeError::Io {
            context: format!("writing the data file of \"{db}.{table}\""),
            source: e,
        })?;
        Ok((encoded.stats, encoded.footer))
    }

    /// [`LakeFormat::write_streamed`] with the catalog column ids the file
    /// must record as parquet `field_id`s.
    ///
    /// The lake passes them so a DuckLake reader can match a nested column's
    /// children (a parquet list has no name of its own); a format that has no
    /// place for them ignores the hint.
    fn write_streamed_with_fields(
        &self,
        db: &str,
        table: &str,
        src: &DataTable,
        fields: &[FieldIds],
        sink: &mut (dyn Write + Send),
    ) -> Result<(SavedTableStats, Option<FooterLocation>)> {
        let _ = fields;
        self.write_streamed(db, table, src, sink)
    }

    /// Decode one data file into the one-database/one-table [`PinkDb`] store
    /// the lake's loaders expect. The lake validates the file's identity,
    /// schema and row count against the catalog afterwards; a provider may
    /// (and the built-ins do) reject a malformed file with a clear
    /// [`LakeError::Corrupt`].
    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb>;

    /// The file's schema and identity when the format carries them in the
    /// file itself (the `.pink` footer, a Parquet footer), without decoding
    /// column data. `None` (the default) means the caller uses the schema the
    /// catalog already records.
    fn footer_stats(&self, _file: &DataFile<'_>) -> Result<Option<FileFooter>> {
        Ok(None)
    }

    /// Whether this format can decode a column projection (a subset of the
    /// physical columns). The default is `false`: a projection decodes the
    /// whole file. The `.pink` and Parquet providers opt in.
    fn supports_projection(&self) -> bool {
        false
    }

    /// The catalog's `ducklake_delete_file.format` value for this data
    /// format: `"pink"` for pink-lake's own positional container, `"parquet"`
    /// for DuckLake's `(file_path, pos)` files. `.pink` keeps pink's own
    /// format.
    fn delete_format(&self) -> &str {
        PINK_FORMAT_ID
    }

    /// The delete-file name extension (`pinkdel`, `parquet`).
    fn delete_extension(&self) -> &str {
        "pinkdel"
    }

    /// Decode one delete file. `data_file` is the catalog-relative data file
    /// the delete file belongs to; a format that records the data path per
    /// row (DuckLake's Parquet) uses it to filter. The default is pink-lake's
    /// `PINKDL1` container.
    fn read_delete_file(&self, bytes: &[u8], _data_file: &str, path: &Path) -> Result<Vec<u64>> {
        crate::delete_file::decode(bytes, path)
    }

    /// Encode one delete file. `data_file_path` is the data file's absolute
    /// (or `s3://`) path, which DuckLake's Parquet format stores per row.
    fn write_delete_file(
        &self,
        _db: &str,
        _table: &str,
        _data_file: &str,
        _data_file_path: &str,
        positions: &[u64],
    ) -> Result<Vec<u8>> {
        Ok(crate::delete_file::encode(positions))
    }

    /// Whether this format can prune **row groups** from a query's pushed-down
    /// predicates (Parquet's column-chunk statistics). The default is `false`.
    /// A provider that answers `true` makes [`LakeAttachment::load_filtered`]
    /// decode an uncached, pruned row-group set even when no whole file was
    /// pruned; the engine re-applies the full predicate, so the answer is
    /// unchanged.
    fn prunes_row_groups(&self) -> bool {
        false
    }

    /// Decode only `columns` (ascending physical schema indices) of one data
    /// file, returning the file's **full physical schema** with unprojected
    /// columns as NULL placeholders, plus the per-column decoded mask. The
    /// lake's attach layer maps that onto the logical schema, applies delete
    /// files and validates the row count, exactly as it does for a full read.
    ///
    /// `filters` are the query's pushed-down conjuncts: a provider may prune
    /// row groups with them, but must never let pruning change the answer
    /// (the engine re-applies the full predicate). `None` (the default) means
    /// "this provider has no projected reader"; the caller falls back to
    /// [`LakeFormat::read`].
    fn read_projected(
        &self,
        _file: &DataFile<'_>,
        _columns: &[usize],
        _filters: &[pinkdb::PrunePredicate],
    ) -> Result<Option<(PinkDb, Vec<bool>)>> {
        Ok(None)
    }
}

/// The process-global registry, seeded lazily with the built-in providers.
fn registry() -> &'static Mutex<BTreeMap<String, Arc<dyn LakeFormat>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<String, Arc<dyn LakeFormat>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut formats: BTreeMap<String, Arc<dyn LakeFormat>> = BTreeMap::new();
        formats.insert(PINK_FORMAT_ID.to_string(), Arc::new(PinkFormat));
        Mutex::new(formats)
    })
}

/// Register one data-file format provider, making its [`LakeFormat::id`]
/// usable in a catalog.
///
/// Registration is process-global. A duplicate id — including a built-in one
/// — is refused with [`LakeError::DuplicateFormat`], so two providers can
/// never race to interpret the same catalog string. The id and extension must
/// be stable lowercase names (`a-z`, `0-9`, `_`, `-`).
///
/// # Errors
///
/// [`LakeError::DuplicateFormat`] when the id is already registered, and
/// [`LakeError::InvalidOption`] when the id or extension is not a usable
/// data-file name component.
pub fn register_lake_format(format: Arc<dyn LakeFormat>) -> Result<()> {
    let id = format.id().to_string();
    if !is_name_component(&id) {
        return Err(LakeError::InvalidOption {
            message: format!(
                "lake format id {id:?} is not a stable lowercase id (use a-z, 0-9, '_' or '-')"
            ),
        });
    }
    let extension = format.extension().to_string();
    if !is_name_component(&extension) {
        return Err(LakeError::InvalidOption {
            message: format!(
                "lake format \"{id}\" declares extension {extension:?}, which is not a usable \
                 data-file name component (use a-z, 0-9, '_' or '-')"
            ),
        });
    }
    let mut formats = registry().lock().unwrap_or_else(|p| p.into_inner());
    if formats.contains_key(&id) {
        return Err(LakeError::DuplicateFormat { format: id });
    }
    formats.insert(id, format);
    Ok(())
}

/// The provider registered for `id`, or `None` when this build has none.
pub fn lake_format(id: &str) -> Option<Arc<dyn LakeFormat>> {
    registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(id)
        .cloned()
}

/// Resolve `id` for a lake operation, naming `path` in the failure.
///
/// A missing provider is [`LakeError::UnknownFormat`] — never a fallback.
/// The format id removed in wave 104 is refused with its own clear message:
/// old lakes are not converted, and no build registers a reader for them.
///
/// The id is spelled in two pieces on purpose: wave 104 removed every
/// reference to it from the tree, so it survives only as this comparison.
pub(crate) const REMOVED_FORMAT_ID: &str = concat!("vor", "tex");

/// Whether `id` is the format removed in wave 104.
pub(crate) fn is_removed(id: &str) -> bool {
    id == REMOVED_FORMAT_ID
}

/// The error a catalog row naming the removed format returns.
pub(crate) fn removed_format_error() -> LakeError {
    LakeError::RemovedFormat {
        message: concat!(
            "this lake has Vor",
            "tex data files, which pinkdb no longer reads; re-save the lake with an older build"
        )
        .to_string(),
    }
}

pub(crate) fn format_for(id: &str, path: &Path) -> Result<Arc<dyn LakeFormat>> {
    if is_removed(id) {
        return Err(removed_format_error());
    }
    if let Some(format) = lake_format(id) {
        return Ok(format);
    }
    Err(LakeError::UnknownFormat {
        path: path.to_path_buf(),
        format: id.to_string(),
    })
}

/// A format id / extension: lowercase ASCII letters, digits, `_` and `-`.
fn is_name_component(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// The built-in engine `.pink` format: one single-table store per file, with
/// the exact statistics its encode pass collected.
///
/// The file is a `PINKDB06` image (wave 102): a data region of column chunks
/// (row groups of 122,880 rows, one chunk per column per row group, each
/// with its own CRC32C and exact statistics) followed by a footer and a
/// 20-byte trailer. A reader therefore needs the footer and **only** the
/// projected column chunks of the row groups that survive the scan's
/// filters — read through the lake's positional reader, coalesced and
/// fetched with a bounded number of requests in flight.
#[derive(Debug)]
struct PinkFormat;

impl LakeFormat for PinkFormat {
    fn id(&self) -> &str {
        PINK_FORMAT_ID
    }

    fn extension(&self) -> &str {
        "pink"
    }

    fn write(&self, db: &str, table: &str, src: &DataTable) -> Result<EncodedFile> {
        let mini = crate::single_table_store(db, table, src)?;
        let (bytes, saved) = mini
            .to_pink_bytes_with_stats()
            .map_err(|e| LakeError::Engine {
                context: format!("saving table \"{db}.{table}\""),
                source: e,
            })?;
        let stats = saved
            .into_iter()
            .find(|s| s.db == db && s.table == table)
            .ok_or_else(|| LakeError::Internal {
                message: format!(
                    "the single-table encoder for \"{db}.{table}\" returned no statistics"
                ),
            })?;
        let (offset, length, crc32c) =
            pinkdb::persist::footer_location(&bytes).map_err(|e| LakeError::Engine {
                context: format!("reading the footer of the .pink file of \"{db}.{table}\""),
                source: e,
            })?;
        Ok(EncodedFile {
            bytes,
            stats,
            footer: Some(FooterLocation {
                offset,
                length,
                crc32c,
            }),
        })
    }

    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb> {
        let copy = file.local_copy()?;
        PinkDb::open(crate::path_str(copy.path())?).map_err(|e| LakeError::Engine {
            context: format!("loading data file \"{}\"", file.rel()),
            source: e,
        })
    }

    /// The file's schema, identity, row count and exact per-column
    /// statistics, read from its footer alone (no data segment).
    ///
    /// The footer is fetched through the lake's positional reader: one ranged
    /// read when the catalog records the footer's location, two (the trailer
    /// plus the footer) when it does not.
    fn footer_stats(&self, file: &DataFile<'_>) -> Result<Option<FileFooter>> {
        let Some(source) = file.range_source() else {
            // A sealed lake has no plaintext ranges: the caller falls back to
            // the whole-object read.
            return Ok(None);
        };
        let (bytes, crc) = read_footer(file, source.as_ref())?;
        let layout = pinkdb::persist::read_layout(&bytes, crc).map_err(|e| LakeError::Engine {
            context: format!("reading the footer of data file \"{}\"", file.rel()),
            source: e,
        })?;
        let Some(table) = layout.tables.first() else {
            return Err(LakeError::Corrupt {
                path: file.path().to_path_buf(),
                message: "the .pink data file holds no table".to_string(),
            });
        };
        let stats = table
            .schema
            .iter()
            .enumerate()
            .map(|(i, (_, dtype))| {
                // The footer's per-chunk statistics are per row group; the
                // file's own facts are their union (the exactness gate is
                // the shared one).
                let mut nulls = 0u64;
                let mut min = None;
                let mut max = None;
                let mut nans = 0u64;
                for group in &table.row_groups {
                    let Some(chunk) = group.columns.get(i) else {
                        continue;
                    };
                    nulls += chunk.null_count;
                    nans += chunk.nan_count;
                    min = tighter(min, chunk.min.clone());
                    max = tighter(max, chunk.max.clone());
                }
                crate::stats::serve_column_stats(*dtype, Some(nulls), min, max, Some(nans))
            })
            .collect();
        Ok(Some(FileFooter {
            db: table.db.clone(),
            table: table.table.clone(),
            rows: table.row_count,
            schema: table.schema.clone(),
            stats,
        }))
    }

    fn supports_projection(&self) -> bool {
        true
    }

    /// The `.pink` footer records **exact** per-column-chunk statistics, so
    /// a row group that cannot match the query's predicates is skipped
    /// without reading a byte of it.
    fn prunes_row_groups(&self) -> bool {
        true
    }

    fn read_projected(
        &self,
        file: &DataFile<'_>,
        columns: &[usize],
        filters: &[pinkdb::PrunePredicate],
    ) -> Result<Option<(PinkDb, Vec<bool>)>> {
        // An encrypted lake has no plaintext byte ranges (a sealed container
        // must be unsealed whole): the caller falls back to a full decode.
        let Some(source) = file.range_source() else {
            return Ok(None);
        };
        // 1. The footer: the row-group directory (where every column chunk
        //    is, and what it proves).
        let (bytes, crc) = read_footer(file, source.as_ref())?;
        let layout = pinkdb::persist::read_layout(&bytes, crc).map_err(|e| LakeError::Engine {
            context: format!("reading the footer of data file \"{}\"", file.rel()),
            source: e,
        })?;
        let Some(table) = layout.tables.first() else {
            return Err(LakeError::Corrupt {
                path: file.path().to_path_buf(),
                message: "the .pink data file holds no table".to_string(),
            });
        };
        // 2. The row groups that survive the scan's filters.
        let groups = surviving_row_groups(table, filters, file.deletes());
        // 3. The projected column chunks of those row groups — and only
        //    those bytes.
        let spans = chunk_spans(table, columns, &groups);
        // 4. Fetch them: coalesced, with a bounded number of requests in
        //    flight.
        let merged = crate::ranged::coalesce_spans(spans);
        let fetched = crate::ranged::fetch_spans(source.as_ref(), &merged, crate::parallel_limit());
        let mut windows = Vec::with_capacity(fetched.len());
        for (span, bytes) in merged.iter().zip(fetched) {
            windows.push(pinkdb::persist::PinkByteWindow {
                offset: span.start,
                bytes: bytes?,
            });
        }
        // 5. Decode (in parallel, in file order).
        let decoded = pinkdb::persist::decode_projected_columns(table, columns, &groups, &windows)
            .map_err(|e| LakeError::Engine {
                context: format!("decoding data file \"{}\"", file.rel()),
                source: e,
            })?;
        // 6. The one-database/one-table store the lake's loaders expect.
        let store = build_pink_store(file, &table.db, &table.table, decoded)?;
        let mut present = vec![false; table.schema.len()];
        for &column in columns {
            if column < present.len() {
                present[column] = true;
            }
        }
        Ok(Some((store, present)))
    }
}

/// The tighter of two statistics values: the smaller of two minimums, the
/// larger of two maximums (`None` is "no fact").
fn tighter(current: Option<Value>, other: Option<Value>) -> Option<Value> {
    match (current, other) {
        (Some(a), Some(b)) => Some(if pinkdb::plan::compare_values(&b, &a).is_lt() {
            b
        } else {
            a
        }),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// Read one `.pink` data file's footer through the lake's positional reader.
///
/// With the catalog-recorded location (wave 102, `format_version` 7) this is
/// **one** ranged read and the footer is still checksum-verified. Without it
/// the reader probes the file's 20-byte trailer first (two reads).
fn read_footer(file: &DataFile<'_>, source: &dyn RangedFile) -> Result<(Vec<u8>, Option<u32>)> {
    if let Some(location) = file.footer() {
        let bytes = source.read_range(location.offset, location.length)?;
        return Ok((bytes, Some(location.crc32c)));
    }
    let len = source.len();
    if len < pinkdb::persist::TRAILER_LEN as u64 {
        return Err(LakeError::Corrupt {
            path: file.path().to_path_buf(),
            message: "the .pink data file is shorter than its trailer".to_string(),
        });
    }
    let trailer = source.read_range(
        len - pinkdb::persist::TRAILER_LEN as u64,
        pinkdb::persist::TRAILER_LEN as u64,
    )?;
    let (offset, length, crc32c) =
        pinkdb::persist::parse_trailer(&trailer, len).map_err(|e| LakeError::Engine {
            context: format!("reading the trailer of data file \"{}\"", file.rel()),
            source: e,
        })?;
    let bytes = source.read_range(offset, length)?;
    Ok((bytes, Some(crc32c)))
}

/// The row groups of one `.pink` file that survive the scan's predicates.
///
/// The footer's per-chunk statistics are exact, so a row group whose
/// statistics prove that no row can match is skipped without reading it.
/// The same zone-map rules the lake prunes whole files with apply here
/// ([`crate::zone`]), and the same exactness gate applies to a DOUBLE's
/// min/max.
///
/// A file with **positional deletes** is never pruned: a delete position
/// addresses a row of the file, and dropping rows would move it.
fn surviving_row_groups(
    table: &pinkdb::persist::PinkTableLayout,
    filters: &[pinkdb::PrunePredicate],
    deletes: Option<&[u64]>,
) -> Vec<usize> {
    if filters.is_empty() || deletes.is_some_and(|d| !d.is_empty()) {
        return (0..table.row_groups.len()).collect();
    }
    table
        .row_groups
        .iter()
        .enumerate()
        .filter_map(|(index, group)| {
            let zone: Vec<pinkdb::ColumnStats> = table
                .schema
                .iter()
                .enumerate()
                .map(|(i, (_, dtype))| {
                    let Some(chunk) = group.columns.get(i) else {
                        return pinkdb::ColumnStats::unknown();
                    };
                    crate::stats::serve_column_stats(
                        *dtype,
                        Some(chunk.null_count),
                        chunk.min.clone(),
                        chunk.max.clone(),
                        Some(chunk.nan_count),
                    )
                })
                .collect();
            let facts = crate::zone::FileFacts {
                rows: group.rows,
                zone: Some(&zone),
                partition: None,
                paths: None,
            };
            if crate::zone::file_prunes(facts, &[], &table.schema, filters) {
                None
            } else {
                Some(index)
            }
        })
        .collect()
}

/// The byte ranges of the projected columns of the given row groups.
fn chunk_spans(
    table: &pinkdb::persist::PinkTableLayout,
    columns: &[usize],
    groups: &[usize],
) -> Vec<crate::ranged::ByteSpan> {
    let mut spans = Vec::new();
    for &group in groups {
        let Some(rg) = table.row_groups.get(group) else {
            continue;
        };
        for &column in columns {
            let Some(chunk) = rg.columns.get(column) else {
                continue;
            };
            if chunk.length == 0 {
                continue;
            }
            spans.push(crate::ranged::ByteSpan {
                start: chunk.offset,
                len: chunk.length,
            });
        }
    }
    spans
}

/// Build the one-database/one-table store the lake's loaders expect out of
/// a decoded `.pink` file.
fn build_pink_store(
    file: &DataFile<'_>,
    db: &str,
    table: &str,
    decoded: DataTable,
) -> Result<PinkDb> {
    let mut store = PinkDb::new();
    if !db.eq_ignore_ascii_case("main") {
        store.create_database(db).map_err(|e| LakeError::Engine {
            context: format!("creating database \"{db}\""),
            source: e,
        })?;
        store.drop_database("main").map_err(|e| LakeError::Engine {
            context: "dropping the default database".to_string(),
            source: e,
        })?;
    }
    store
        .create_table_in(db, table, decoded.schema())
        .map_err(|e| LakeError::Engine {
            context: format!("creating table \"{db}.{table}\""),
            source: e,
        })?;
    {
        let dst = store
            .table_mut_in(db, table)
            .map_err(|e| LakeError::Engine {
                context: format!("opening table \"{db}.{table}\""),
                source: e,
            })?;
        for index in 0..decoded.num_chunks() {
            let chunk = decoded
                .scan_chunk(index)
                .ok_or_else(|| LakeError::Internal {
                    message: format!(
                        "chunk {index} of \"{db}.{table}\" disappeared while decoding"
                    ),
                })?;
            dst.append_shared_chunk(&chunk.columns)
                .map_err(|e| LakeError::Engine {
                    context: format!("appending rows to \"{db}.{table}\""),
                    source: e,
                })?;
        }
    }
    let _ = file;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestProvider(&'static str);

    impl LakeFormat for TestProvider {
        fn id(&self) -> &str {
            self.0
        }

        fn extension(&self) -> &str {
            "test"
        }

        fn write(&self, _db: &str, _table: &str, _src: &DataTable) -> Result<EncodedFile> {
            unreachable!("the registry tests never write")
        }

        fn read(&self, _file: &DataFile<'_>) -> Result<PinkDb> {
            unreachable!("the registry tests never read")
        }
    }

    #[test]
    fn built_ins_are_registered_lazily() {
        assert!(lake_format(PINK_FORMAT_ID).is_some());
    }

    #[test]
    fn duplicate_and_malformed_ids_are_refused() {
        let id = format!("dup-{}", std::process::id());
        register_lake_format(Arc::new(TestProvider(Box::leak(
            id.clone().into_boxed_str(),
        ))))
        .unwrap();
        let err = register_lake_format(Arc::new(TestProvider(Box::leak(
            id.clone().into_boxed_str(),
        ))))
        .unwrap_err();
        assert!(matches!(err, LakeError::DuplicateFormat { .. }), "{err:?}");
        assert!(err.to_string().contains(&id), "{err}");

        let err = register_lake_format(Arc::new(TestProvider("Not Lowercase"))).unwrap_err();
        assert!(matches!(err, LakeError::InvalidOption { .. }), "{err:?}");
        assert!(register_lake_format(Arc::new(TestProvider(""))).is_err());
    }

    #[test]
    fn an_unknown_id_is_named() {
        let err = format_for("no-such-format", Path::new("catalog.json")).unwrap_err();
        assert!(matches!(err, LakeError::UnknownFormat { .. }), "{err:?}");
        let text = err.to_string();
        assert!(text.contains("no-such-format"), "{text}");
        assert!(text.contains("no lake format provider"), "{text}");
    }
}
