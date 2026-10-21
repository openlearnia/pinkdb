//! `pinkdb-arrow` — the core **arrow** extension (wave 71): Arrow IPC
//! (Feather v2) file read/write over the arrow-rs crates.
//!
//! SQL surface (once a host registers [`ArrowSource`]/[`ArrowSink`]):
//!
//! - `FROM '<file>.arrow'` — a replacement scan resolved by file extension
//!   (`arrow`, `ipc`, `feather`, `arrows`);
//! - `FROM read_arrow('<file>.arrow')` — the same read, spelled as a table
//!   function (`read_ipc`/`read_feather` are aliases);
//! - `COPY <table> TO '<file>.arrow' [(FORMAT arrow)]` — write one table as
//!   an Arrow IPC **file** (Feather v2, the `ARROW1` magic); `FORMAT` may be
//!   omitted when the target's extension resolves (DuckDB's rule).
//!
//! ### Why an engine-adjacent crate (the integration decision)
//!
//! The scalar extension ABI (waves 29A/49) registers scalar functions only —
//! it has no way to express a replacement scan, a table function, or a COPY
//! format. The engine therefore exposes two host traits: the **table source**
//! ([`pinkdb::table_source::TableSource`]) for every file *read* surface, and
//! the **write sink** ([`pinkdb::FileFormat`]) for `COPY … (FORMAT <name>)`.
//! This crate implements both (depending on `pinkdb`, never the other way
//! around, exactly like `pink-lake` implements `AttachedLake`), and the host
//! registers them when it creates the store — or process-wide via
//! [`ensure_registered`], the bootstrap a durable store needs, or as an
//! **extension package** ([`package`]/[`register_package`], wave 86), which
//! activates per store through `LOAD EXTENSION 'arrow'`. The CLI wires it
//! behind the opt-in `arrow` cargo feature, so the default binary stays lean
//! — the same model as the `vortexlake` lake build. (Wave 70/71 unification:
//! both file-format crates now share this one read surface; see
//! `extensions/parquet`.)
//!
//! Alternatives considered and rejected:
//!
//! - **a scalar extension** (json/datetime/math style): cannot express table
//!   sources or write statements — the AST/planner surfaces simply do not
//!   exist behind the scalar ABI;
//! - **reusing the attached-lake path** (`.lake attach`'s session-local
//!   read-only catalog extension): it would have bent lake semantics around
//!   plain files (session-scoped registration, lake-specific refusal rules in
//!   durable stores, statistics descriptors, `.lake detach`) for no gain —
//!   the file surfaces needed their own AST nodes and a COPY plan anyway;
//! - **a `pinkdb`-internal arrow dependency**: would put arrow-rs in the core
//!   engine's dependency tree and violate the "default CLI stays lean"
//!   requirement.
//!
//! ### Semantics pinned in this wave
//!
//! - **Format**: Arrow IPC **file** format (`ARROW1` magic + footer) — the
//!   format DuckDB's arrow extension writes and reads. The IPC *stream*
//!   format is detected by its magic and refused with a message naming the
//!   difference (a documented follow-up, not a silent misread).
//! - **Batches**: our writer emits one record batch per engine chunk
//!   (≤ 2048 rows, DuckDB's vector size); a reader accepts any batch sizes
//!   and re-chunks the rows into engine chunks.
//! - **Reading is full-file**: a scan materializes the whole file in memory
//!   (no projection pushdown, no streaming, no predicate pushdown yet) and a
//!   file is decoded once per scan; a self-join on the same file decodes it
//!   once per side.
//! - **Writing is uncompressed** (pyarrow's and DuckDB's default); reading
//!   accepts uncompressed, LZ4- and ZSTD-compressed IPC files.
//! - **Types**: the physical mapping is in [`types`] — engine `INTEGER`
//!   ⇄ `Int32`, `BIGINT` ⇄ `Int64`, DECIMAL ⇄ `Decimal128`, INTERVAL ⇄
//!   `Interval(MonthDayNano)`, DATE ⇄ `Date32`, TIMESTAMP ⇄ naive
//!   `Timestamp(Microsecond)`, … Everything without an exact engine
//!   counterpart (BLOB, TIME, LIST, STRUCT, MAP, UNION, DICTIONARY,
//!   `Decimal256`, `Utf8View`… see the table) is refused at *bind* time — a
//!   clean error naming the column and type, never a silent coercion.
//! - **C Data Interface**: not part of this wave; see the pinned follow-up in
//!   this crate's README (`pinkdb-ffi` query-result export + Arrow-array scan
//!   input, zero-copy where the layout allows).
//! - **Remote objects** (`remote` feature, enabled by the CLI's `httpfs`
//!   feature): `read_arrow('https://…/x.arrow')`, `read_arrow('s3://…/x')`,
//!   and the same paths in the replacement scan. Claims are by extension
//!   regardless of scheme; plain `http://` is claimed but the shared
//!   transport refuses it unless `PINKDB_HTTPFS_ALLOW_HTTP=1`. IPC has no
//!   ranged footer protocol, so a remote read is a **whole-object
//!   `fetch()`**, cached for the bind→scan pair and re-fetched by the next
//!   statement (the re-read policy). `pinkdb-cli` prints the missing feature
//!   for a remote path in a lean build.
//!
//! ### Durability
//!
//! File access is not session-local state: a WAL-logged statement that reads
//! a file (`INSERT INTO t SELECT * FROM 'f.arrow'`) replays by re-reading
//! that file (DuckDB's behavior — see [`pinkdb::table_source`]); a deleted
//! file is a hard recovery error, a changed file replays its new content.
//! `COPY … TO` has no store effect and is never WAL-logged; like DuckDB, a
//! `COPY` inside a transaction is not rolled back (the file is external).

pub mod read;
pub mod remote;
pub mod types;
pub mod write;

use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "remote")]
use std::sync::Mutex;

use pinkdb::table_source::{TableFunctionCall, TableScanOutput, TableSource, TableSourceStats};
use pinkdb::types::Result;
use pinkdb::{
    AttachedScan, DataTable, DataType, ExtensionPackage, FileFormat, PinkDb, PrunePredicate,
};

/// The registered format's name: `FORMAT arrow`.
pub const EXTENSION_NAME: &str = "arrow";

/// The file extensions `FROM 'x.<ext>'` resolves through this format.
/// `arrows` (pyarrow's conventional *stream* suffix) is deliberately in the
/// list: it resolves here and is then refused by the reader with the precise
/// "this is an IPC stream" message instead of "no file format registered".
pub const EXTENSIONS: &[&str] = &["arrow", "ipc", "feather", "arrows"];

/// The `read_*` table functions this source answers to.
pub const READ_FUNCTIONS: &[&str] = &["read_arrow", "read_ipc", "read_feather"];

/// The Arrow IPC (Feather v2) file **read** surface: a
/// [`pinkdb::TableSource`].
///
/// Reads are full-file: a scan materializes the whole IPC file and returns it
/// in engine chunks (IPC has no row-group statistics, so the projection and
/// filter hints are ignored; every column decodes). A remote object (the
/// `remote` feature) is fetched whole — IPC has no ranged footer protocol —
/// once per bind→scan pair; the next statement re-fetches (the re-read
/// policy). The counters report one file and its column count.
#[derive(Debug, Default)]
pub struct ArrowSource {
    /// One-statement remote fetch cache: `bind` fetches the object, `scan`
    /// consumes it, a later statement re-fetches.
    #[cfg(feature = "remote")]
    cache: Mutex<Option<(String, Arc<Vec<u8>>)>>,
}

impl ArrowSource {
    pub fn new() -> Self {
        Self::default()
    }

    /// The scan shape for one materialized table (shared by local and
    /// remote reads).
    fn scan_table(table: pinkdb::DataTable) -> TableScanOutput {
        let columns = table.num_cols() as u64;
        let chunks = AttachedScan::complete(Arc::new(table)).chunks();
        TableScanOutput {
            chunks,
            files: 1,
            // The IPC file format has no row groups.
            row_groups: 0,
            row_groups_pruned: 0,
            columns_decoded: columns,
        }
    }
}

impl TableSource for ArrowSource {
    fn name(&self) -> &str {
        EXTENSION_NAME
    }

    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
        let Some(path) = self.claim(call)? else {
            return Ok(None);
        };
        if remote::is_remote(&path) {
            #[cfg(feature = "remote")]
            {
                let bytes = Arc::new(remote::fetch(&path)?);
                let schema = read::schema_bytes(&bytes, &path)?;
                let mut cache = self
                    .cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *cache = Some((path, bytes));
                return Ok(Some(schema));
            }
            #[cfg(not(feature = "remote"))]
            {
                return Err(remote::missing_feature(&path));
            }
        }
        Ok(Some(read::schema(Path::new(&path))?))
    }

    fn scan(
        &self,
        call: &TableFunctionCall,
        _projection: Option<&[usize]>,
        _filters: &[PrunePredicate],
    ) -> Result<TableScanOutput> {
        let path = self.claim(call)?.ok_or_else(|| {
            pinkdb::PinkDbError::Execution(format!("the arrow source does not serve {call:?}"))
        })?;
        if remote::is_remote(&path) {
            #[cfg(feature = "remote")]
            {
                let cached = {
                    let mut cache = self
                        .cache
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    match cache.as_ref() {
                        Some((key, _)) if key == &path => cache.take().map(|(_, bytes)| bytes),
                        _ => None,
                    }
                };
                let bytes = match cached {
                    Some(bytes) => bytes,
                    None => Arc::new(remote::fetch(&path)?),
                };
                let table = read::read_bytes(&bytes, &path, remote::table_name(&path))?;
                return Ok(Self::scan_table(table));
            }
            #[cfg(not(feature = "remote"))]
            {
                return Err(remote::missing_feature(&path));
            }
        }
        let table = read::read(Path::new(&path))?;
        Ok(Self::scan_table(table))
    }

    fn stats(&self) -> TableSourceStats {
        TableSourceStats {
            name: EXTENSION_NAME.to_string(),
            ..TableSourceStats::default()
        }
    }
}

impl ArrowSource {
    /// The path a call names when this source owns it, else `None`: a
    /// `read_arrow`/`read_ipc`/`read_feather(path)` call whose single
    /// argument is a string literal, or a bare path with one of
    /// [`EXTENSIONS`].
    fn claim(&self, call: &TableFunctionCall) -> Result<Option<String>> {
        match call {
            TableFunctionCall::Function { name, args } => {
                if !READ_FUNCTIONS.iter().any(|f| f.eq_ignore_ascii_case(name)) {
                    return Ok(None);
                }
                match args.as_slice() {
                    [arg] => match &arg.value {
                        pinkdb::Value::Varchar(path) => Ok(Some(path.clone())),
                        other => Err(pinkdb::PinkDbError::Type(format!(
                            "{name}: the path must be a string literal (got {other})"
                        ))),
                    },
                    _ => Err(pinkdb::PinkDbError::Type(format!(
                        "{name} takes exactly one path argument"
                    ))),
                }
            }
            TableFunctionCall::Path(path) => {
                // Extension-based regardless of scheme; the query/fragment of
                // a remote URL is ignored for the decision.
                let ext = Path::new(remote::path_only(path))
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_ascii_lowercase());
                Ok(ext
                    .filter(|e| EXTENSIONS.contains(&e.as_str()))
                    .map(|_| path.clone()))
            }
        }
    }
}

/// The Arrow IPC **write** sink: `COPY … (FORMAT arrow)`.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArrowSink;

impl ArrowSink {
    pub fn new() -> Self {
        Self
    }
}

impl FileFormat for ArrowSink {
    fn name(&self) -> &str {
        EXTENSION_NAME
    }

    fn extensions(&self) -> &[&str] {
        EXTENSIONS
    }

    fn write(&self, path: &Path, table: &DataTable) -> Result<()> {
        write::write(path, table)
    }
}

/// Register the Arrow IPC read source and write sink on one store (embedder
/// path). Idempotent: registering again replaces the previous instances.
pub fn register(db: &mut PinkDb) {
    db.register_table_source(Arc::new(ArrowSource::new()));
    db.register_file_format(Arc::new(ArrowSink));
}

/// Register both halves **process-wide**, for every store created afterwards
/// — the host-bootstrap step a durable store needs: recovery replays inside
/// `open_durable`, before the caller holds the store, so a WAL record that
/// reads a file (`INSERT … SELECT * FROM 'f.arrow'`) can only resolve if the
/// source was registered *before* the store was constructed. The CLI (built
/// with the `arrow` feature) calls this once at startup; an embedder that
/// opens stores before calling it must use [`register`] per store instead.
/// Idempotent.
pub fn ensure_registered() {
    pinkdb::table_source::register_global_table_source(Arc::new(ArrowSource::new()));
    pinkdb::filefmt::register_global_file_format(Arc::new(ArrowSink));
}

/// This extension as an [`ExtensionPackage`] (wave 86): [`ArrowSource`] as
/// the table source and [`ArrowSink`] as the file format, under the name
/// [`EXTENSION_NAME`] (`arrow`).
///
/// [`register_package`] publishes exactly this package; build one yourself
/// to bundle the Arrow surfaces with other sources, sinks or scalars.
pub fn package() -> ExtensionPackage {
    ExtensionPackage::new(EXTENSION_NAME)
        .with_table_source(Arc::new(ArrowSource::new()))
        .with_file_format(Arc::new(ArrowSink))
}

/// Publish the Arrow read source and write sink as an **extension package** —
/// the third registration model, next to [`register`] (one store) and
/// [`ensure_registered`] (every store, process-wide from startup).
///
/// Registration is process-global but activates **nothing**: a store gets the
/// Arrow reader and writer only after `LOAD EXTENSION 'arrow'` (or
/// [`PinkDb::load_extension_package`]), and only that store. Activation is
/// atomic — if a source or format name collides with a built-in, a
/// process-global registration or an earlier load, the whole package is
/// refused and the store is left unchanged.
///
/// A process must pick **one** model: if [`ensure_registered`] already owns
/// the `arrow` names globally (the CLI's bootstrap), a package load is refused
/// with that collision error rather than silently mixing models. For a durable
/// store, publish the package **before** the store is opened, so WAL replay
/// can resolve the `LOAD EXTENSION 'arrow'` prefixes it records. The package
/// is plain Rust (no `dlopen`), so it works on wasm32 like the linked model.
pub fn register_package() -> Result<()> {
    pinkdb::register_extension_package(package())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The read source claims the arrow/ipc/feather extensions and the three
    /// `read_*` spellings, and nothing else.
    #[test]
    fn registration_exposes_the_read_and_write_surfaces() {
        let mut db = PinkDb::new();
        assert!(db.table_source("arrow").is_none());
        assert!(db.file_format("arrow").is_none());
        register(&mut db);
        let source = db.table_source("ARROW").expect("case-insensitive lookup");
        assert_eq!(source.name(), "arrow");
        let sink = db.file_format("ARROW").expect("case-insensitive lookup");
        assert_eq!(sink.name(), "arrow");
        assert!(db.file_format_names().contains("arrow"));

        // The replacement scan claims every registered extension.
        for path in ["x.arrow", "x.IPC", "dir/x.feather", "x.arrows"] {
            let claimed = source
                .bind(&TableFunctionCall::Path(path.to_string()))
                .map_err(|e| e.to_string());
            assert!(
                matches!(claimed, Err(ref e) if e.contains("cannot open")),
                "{path}: {claimed:?}"
            );
        }
        // …and not another format's.
        assert_eq!(
            source
                .bind(&TableFunctionCall::Path("x.parquet".to_string()))
                .unwrap(),
            None
        );
        // Re-registration replaces instead of duplicating.
        register(&mut db);
        assert_eq!(db.file_format_names(), "arrow");
    }

    /// `read_*` name resolution, arity and argument-type refusals.
    #[test]
    fn read_functions_are_named_and_checked() {
        let source = ArrowSource::new();
        for name in READ_FUNCTIONS {
            let err = source
                .bind(&pinkdb::TableFunctionCall::Function {
                    name: name.to_string(),
                    args: vec![pinkdb::TableFunctionArg {
                        name: None,
                        value: pinkdb::Value::Varchar("x.arrow".to_string()),
                    }],
                })
                .unwrap_err()
                .to_string();
            assert!(err.contains("cannot open"), "{name}: {err}");
        }
        // Not this source's name.
        assert_eq!(
            source
                .bind(&pinkdb::TableFunctionCall::Function {
                    name: "read_parquet".to_string(),
                    args: vec![],
                })
                .unwrap(),
            None
        );
        // Arity / argument type are this source's clean errors.
        let err = source
            .bind(&pinkdb::TableFunctionCall::Function {
                name: "read_arrow".to_string(),
                args: vec![],
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactly one path"), "{err}");
        let err = source
            .bind(&pinkdb::TableFunctionCall::Function {
                name: "read_arrow".to_string(),
                args: vec![pinkdb::TableFunctionArg {
                    name: None,
                    value: pinkdb::Value::Integer(1),
                }],
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("string literal"), "{err}");
    }
}
