//! pink-lake's **Parquet** data-file format provider (wave 87, task T2).
//!
//! This is the bridge between the lake's [`LakeFormat`] seam and the
//! `pinkdb-parquet` extension:
//!
//! - **write**: [`ParquetLakeFormat::write`] encodes one table with
//!   [`pinkdb_parquet::write::write_table_with_identity`] and publishes the
//!   engine's exact per-column facts from
//!   [`pinkdb::persist::table_stats`] — the same `SavedTableStats` the `.pink`
//!   and Vortex providers publish. The lake's shared exactness gate applies
//!   to the block uniformly.
//! - **read**: the whole file is decoded with the *existing* parquet reader
//!   (`pinkdb_parquet::source()` bound to `read_parquet('<path>')`) and
//!   rebuilt as the one-database/one-table [`PinkDb`] every lake loader
//!   expects. The database/table names come from the file's
//!   `pinkdb.db`/`pinkdb.table` footer keys (written by the bridge), so the
//!   lake's identity check sees the catalog's names.
//! - **no footer statistics**: [`LakeFormat::footer_stats`] keeps the
//!   default (`None`), so attach reads the CRC-protected statistics block
//!   from the manifest — parquet's own chunk statistics are not converted
//!   (and would need the same exactness gate as anyway).
//!
//! [`register`] installs the provider process-wide; it is idempotent for this
//! provider (a duplicate registration of another provider under `parquet`
//! still fails).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::format::{DataFile, EncodedFile, LakeFormat, register_lake_format};
use pink_lake::{LakeError, Result};
use pinkdb::{DataTable, PinkDb, PinkDbError, TableFunctionArg, TableFunctionCall, Value};

/// The catalog's format id for Parquet data files.
pub const FORMAT_ID: &str = "parquet";

/// The data-file name extension (the lake names a file
/// `data/<table>/<uuid>.parquet`).
pub const EXTENSION: &str = "parquet";

/// The lake provider: stateless, shared process-wide through the registry.
#[derive(Debug, Default, Clone, Copy)]
pub struct ParquetLakeFormat;

impl ParquetLakeFormat {
    pub fn new() -> Self {
        Self
    }
}

impl LakeFormat for ParquetLakeFormat {
    fn id(&self) -> &str {
        FORMAT_ID
    }

    fn extension(&self) -> &str {
        EXTENSION
    }

    fn write(&self, db: &str, table: &str, src: &DataTable) -> Result<EncodedFile> {
        let bytes = pinkdb_parquet::write::write_table_with_identity(src, db, table).map_err(
            |e| match e {
                pinkdb_parquet::write::WriteError::UnsupportedType { .. } => {
                    LakeError::NotRepresentable {
                        message: e.to_string(),
                    }
                }
                other => LakeError::Engine {
                    context: format!("encoding table \"{db}.{table}\" as parquet"),
                    source: PinkDbError::Execution(other.to_string()),
                },
            },
        )?;
        Ok(EncodedFile {
            bytes,
            stats: pinkdb::persist::table_stats(db, table, src),
        })
    }

    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb> {
        // The existing reader resolves a path (local file, glob, …). A local
        // plaintext file is read in place; object storage and encrypted lakes
        // (`local_path` is `None`) are staged as one plaintext copy, removed
        // when the guard drops.
        let (path, _staged) = match file.local_path() {
            Some(path) => (path, None),
            None => {
                let bytes = file.read()?;
                let staged = StagedFile::new(&bytes).map_err(|e| LakeError::Corrupt {
                    path: file.path().to_path_buf(),
                    message: format!("cannot stage the data file for decoding: {e}"),
                })?;
                (staged.path(), Some(staged))
            }
        };
        let path_text = path.display().to_string();
        let source = pinkdb_parquet::source();
        let call = TableFunctionCall::Function {
            name: "read_parquet".to_string(),
            args: vec![TableFunctionArg {
                name: None,
                value: Value::Varchar(path_text),
            }],
        };
        let corrupt = |message: String| LakeError::Corrupt {
            path: file.path().to_path_buf(),
            message,
        };
        let schema = source
            .bind(&call)
            .map_err(|e| corrupt(format!("cannot read the parquet footer: {e}")))?
            .ok_or_else(|| corrupt("the parquet source did not claim the data file".to_string()))?;
        let output = source
            .scan(&call, None, &[])
            .map_err(|e| corrupt(format!("cannot decode the parquet data: {e}")))?;
        let identity = pinkdb_parquet::write::read_identity(&path)
            .map_err(|e| corrupt(format!("cannot read the parquet metadata: {e}")))?;
        let (db, table) = identity.unwrap_or_else(|| ("main".to_string(), "data".to_string()));

        let mut store = PinkDb::new();
        if !db.eq_ignore_ascii_case("main") {
            store
                .create_database(&db)
                .map_err(|e| engine(format!("creating database \"{db}\""), e))?;
            store
                .drop_database("main")
                .map_err(|e| engine("dropping the default database".to_string(), e))?;
        }
        store
            .create_table_in(&db, &table, schema)
            .map_err(|e| engine(format!("creating table \"{db}.{table}\""), e))?;
        {
            let dst = store
                .table_mut_in(&db, &table)
                .map_err(|e| engine(format!("opening table \"{db}.{table}\""), e))?;
            for chunk in output.chunks {
                dst.append_shared_chunk(&chunk.columns)
                    .map_err(|e| engine(format!("appending rows to \"{db}.{table}\""), e))?;
            }
        }
        Ok(store)
    }
}

fn engine(context: String, source: PinkDbError) -> LakeError {
    LakeError::Engine { context, source }
}

/// Register the Parquet provider process-wide.
///
/// Idempotent: a duplicate-id error is ignored only when the duplicate is
/// this provider's id (`parquet`) — another provider already registered under
/// that id is indistinguishable through the registry, and refusing to start
/// on a second `register()` in the same process is not useful. Every other
/// error is returned.
pub fn register() -> Result<()> {
    match register_lake_format(Arc::new(ParquetLakeFormat)) {
        Ok(()) => Ok(()),
        Err(LakeError::DuplicateFormat { format }) if format == FORMAT_ID => Ok(()),
        Err(e) => Err(e),
    }
}

/// A staged plaintext copy of one data file, removed when dropped.
struct StagedFile {
    dir: PathBuf,
}

impl StagedFile {
    fn new(bytes: &[u8]) -> std::io::Result<Self> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_parquet_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir)?;
        if let Err(e) = std::fs::write(dir.join("data.parquet"), bytes) {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(e);
        }
        Ok(Self { dir })
    }

    fn path(&self) -> PathBuf {
        self.dir.join("data.parquet")
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
