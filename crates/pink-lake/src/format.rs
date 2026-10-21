//! Lake data-file format providers (wave 87).
//!
//! The catalog pins one format id per table entry (`"pink"`, `"vortex"`, …).
//! This module maps that id to a [`LakeFormat`] implementation — the seam
//! that lets the lake's catalog, save/commit, attach, maintenance, S3 and
//! encryption machinery work over any data-file encoding.
//!
//! # Registry
//!
//! The registry is process-global and keyed by the format id. The crate
//! registers the built-ins lazily on first use:
//!
//! - [`PINK_FORMAT_ID`] — the engine's single-table `.pink` persistence bytes
//!   (every build);
//! - [`VORTEX_FORMAT_ID`] — Vortex data files (`vortexlake` builds only).
//!
//! Extensions register additional formats with [`register_lake_format`]. An
//! id names exactly one provider for the life of the process: a **duplicate
//! id is refused** (the catalog's `format` string must resolve
//! deterministically, or one lake could be read differently by two
//! components in the same process).
//!
//! An id with no provider is [`LakeError::UnknownFormat`] at open, attach,
//! read and maintenance alike; there is never a panic or a silent fallback
//! to `"pink"`. (A `vortex` entry in a build without the `vortexlake`
//! feature keeps its established [`LakeError::FeatureMissing`], because the
//! id is known but this build cannot provide it.)
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
//!    whose files carry their own schema/statistics (Vortex) can answer
//!    attach without the catalog block; formats that do not return `None`
//!    and attach uses the manifest's CRC-protected block.
//!
//! A provider may additionally advertise
//! [`LakeFormat::supports_projection`] to let attach decode only requested
//! columns; the lake falls back to a full decode otherwise.
//!
//! # Writing
//!
//! The lake always writes through [`LakeFormat::write_streamed`] (a unique
//! temp file plus fsync + rename locally, one PUT over S3, sealed as one
//! container for an encrypted lake). The default implementation calls
//! [`LakeFormat::write`] and writes its bytes into the sink, so a provider
//! only has to implement the in-memory form; the Vortex provider overrides
//! it to stream its writer's output directly.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use pinkdb::persist::SavedTableStats;
use pinkdb::{ColumnStats, DataTable, DataType, PinkDb};

use crate::storage::{LocalCopy, Storage};
use crate::{LakeError, Result};

/// The format id of the engine's `.pink` single-table persistence bytes — the
/// built-in format every build registers.
pub const PINK_FORMAT_ID: &str = "pink";

/// The format id of Vortex data files — registered by `vortexlake` builds.
pub const VORTEX_FORMAT_ID: &str = "vortex";

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
}

/// One published data file, as a provider's reader sees it.
///
/// The lake constructs this (there is no public constructor): it carries the
/// file's catalog-relative path, its absolute path for error messages, and
/// access to the storage backend for object-storage and encrypted lakes.
pub struct DataFile<'a> {
    path: &'a Path,
    rel: &'a str,
    storage: &'a dyn Storage,
}

impl<'a> DataFile<'a> {
    /// Wrap one data-file reference for a provider (crate-internal: the lake
    /// owns the storage handle).
    pub(crate) fn new(path: &'a Path, rel: &'a str, storage: &'a dyn Storage) -> Self {
        Self { path, rel, storage }
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

/// The schema, identity and exact statistics a data file carries itself,
/// without decoding any column data (Vortex's footer).
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

/// One lake data-file format: writer, reader, and optional self-description.
///
/// Providers are shared process-wide through the registry (see
/// [`register_lake_format`]); they must be safe to call concurrently.
pub trait LakeFormat: fmt::Debug + Send + Sync {
    /// The stable lowercase id written in the catalog's `format` field
    /// (`"pink"`, `"vortex"`, …). Ids are unique per process.
    fn id(&self) -> &str;

    /// The data-file name extension, without the dot (`"pink"`, `"vortex"`,
    /// `"parquet"`). The lake names a file `data/<table>/<uuid>.<extension>`.
    fn extension(&self) -> &str;

    /// Encode `src` as one complete data file and publish its statistics.
    ///
    /// `db`/`table` name the logical table the file will hold (the `.pink`
    /// and Vortex writers embed this identity in the file, and the loaders
    /// check it against the catalog key).
    fn write(&self, db: &str, table: &str, src: &DataTable) -> Result<EncodedFile>;

    /// Encode `src` straight into `sink` and publish the same statistics.
    ///
    /// The default buffers through [`LakeFormat::write`]; a provider whose
    /// encoder can stream overrides this (Vortex does) so a local save never
    /// materializes the whole file.
    fn write_streamed(
        &self,
        db: &str,
        table: &str,
        src: &DataTable,
        sink: &mut (dyn Write + Send),
    ) -> Result<SavedTableStats> {
        let encoded = self.write(db, table, src)?;
        sink.write_all(&encoded.bytes).map_err(|e| LakeError::Io {
            context: format!("writing the data file of \"{db}.{table}\""),
            source: e,
        })?;
        Ok(encoded.stats)
    }

    /// Decode one data file into the one-database/one-table [`PinkDb`] store
    /// the lake's loaders expect. The lake validates the file's identity,
    /// schema and row count against the catalog afterwards; a provider may
    /// (and the built-ins do) reject a malformed file with a clear
    /// [`LakeError::Corrupt`].
    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb>;

    /// The file's schema, identity and exact statistics when the format
    /// carries them in the file itself (Vortex's footer), without decoding
    /// column data. `None` (the default) means attach reads the
    /// catalog-published statistics block instead.
    fn footer_stats(&self, _file: &DataFile<'_>) -> Result<Option<FileFooter>> {
        Ok(None)
    }

    /// Whether this format's files carry their own footer statistics, so
    /// attach performs a footer read per file (and reports a footer-only
    /// attachment). The default is `false`: attach reads only the manifest.
    fn has_footer_stats(&self) -> bool {
        false
    }

    /// Whether this format can decode a column projection (a subset of the
    /// physical columns). The default is `false`: a projection decodes the
    /// whole file. Only the Vortex provider opts in.
    fn supports_projection(&self) -> bool {
        false
    }
}

/// The process-global registry, seeded lazily with the built-in providers.
fn registry() -> &'static Mutex<BTreeMap<String, Arc<dyn LakeFormat>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<String, Arc<dyn LakeFormat>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut formats: BTreeMap<String, Arc<dyn LakeFormat>> = BTreeMap::new();
        formats.insert(PINK_FORMAT_ID.to_string(), Arc::new(PinkFormat));
        #[cfg(feature = "vortexlake")]
        formats.insert(VORTEX_FORMAT_ID.to_string(), Arc::new(VortexFormat));
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
/// The built-in Vortex id keeps its established [`LakeError::FeatureMissing`]
/// in a build without `vortexlake` (the id is known, the feature is not
/// built).
pub(crate) fn format_for(id: &str, path: &Path) -> Result<Arc<dyn LakeFormat>> {
    if let Some(format) = lake_format(id) {
        return Ok(format);
    }
    #[cfg(not(feature = "vortexlake"))]
    if id == VORTEX_FORMAT_ID {
        return Err(LakeError::FeatureMissing {
            feature: "vortexlake",
            message: format!(
                "the lake uses Vortex data files (format \"{id}\"); this build was built \
                 without Vortex format support — rebuild with the `vortexlake` feature (for \
                 the CLI: `cargo build -p pinkdb-cli --features vortexlake`)"
            ),
        });
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
        Ok(EncodedFile { bytes, stats })
    }

    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb> {
        let copy = file.local_copy()?;
        PinkDb::open(crate::path_str(copy.path())?).map_err(|e| LakeError::Engine {
            context: format!("loading data file \"{}\"", file.rel()),
            source: e,
        })
    }
}

/// The built-in Vortex format (`vortexlake` builds): one Vortex file per
/// table, with the footer's schema and statistics.
#[cfg(feature = "vortexlake")]
#[derive(Debug)]
struct VortexFormat;

#[cfg(feature = "vortexlake")]
impl LakeFormat for VortexFormat {
    fn id(&self) -> &str {
        VORTEX_FORMAT_ID
    }

    fn extension(&self) -> &str {
        "vortex"
    }

    fn write(&self, db: &str, table: &str, src: &DataTable) -> Result<EncodedFile> {
        let mut bytes = Vec::new();
        crate::vortex_file::encode_table_into(db, table, src, &mut bytes)?;
        Ok(EncodedFile {
            bytes,
            stats: pinkdb::persist::table_stats(db, table, src),
        })
    }

    fn write_streamed(
        &self,
        db: &str,
        table: &str,
        src: &DataTable,
        sink: &mut (dyn Write + Send),
    ) -> Result<SavedTableStats> {
        crate::vortex_file::encode_table_into(db, table, src, sink)?;
        Ok(pinkdb::persist::table_stats(db, table, src))
    }

    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb> {
        let bytes = file.read()?;
        crate::vortex_file::decode_table_owned(file.path(), bytes)
    }

    fn footer_stats(&self, file: &DataFile<'_>) -> Result<Option<FileFooter>> {
        Ok(Some(crate::vortex_file::read_footer(file)?))
    }

    fn has_footer_stats(&self) -> bool {
        true
    }

    fn supports_projection(&self) -> bool {
        true
    }
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
        #[cfg(feature = "vortexlake")]
        assert!(lake_format(VORTEX_FORMAT_ID).is_some());
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
