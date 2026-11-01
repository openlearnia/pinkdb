//! pink-lake's **Parquet** data-file format provider (wave 87; ranged and
//! projected in wave 98b).
//!
//! This is the bridge between the lake's [`LakeFormat`] seam and the
//! `pinkdb-parquet` extension:
//!
//! - **write**: [`ParquetLakeFormat::write`] encodes one table with
//!   [`pinkdb_parquet::write::write_table_with_identity`] and publishes the
//!   engine's exact per-column facts from
//!   [`pinkdb::persist::table_stats`] — the same `SavedTableStats` the `.pink`
//!   provider publishes. The lake's shared exactness gate applies to the
//!   block uniformly.
//! - **read**: the file's footer and the projected column-chunk ranges are
//!   read through the lake's positional [`pink_lake::format::RangedFile`] and
//!   the *existing* parquet reader's ranged scan core
//!   ([`pinkdb_parquet::scan_source`]): 98a's coalescing, bounded concurrent
//!   read-ahead and ordered assembly, with no whole-object download. A local
//!   plaintext file uses the same path (positioned reads); an encrypted lake
//!   (no plaintext ranges exist) falls back to the staged whole-object read.
//! - **projection**: [`LakeFormat::supports_projection`] is `true`;
//!   [`LakeFormat::read_projected`] decodes only the requested columns and
//!   passes the query's pushed-down filters to the parquet scan for row-group
//!   pruning. The database/table names come from the file's
//!   `pinkdb.db`/`pinkdb.table` footer keys (written by the bridge), so the
//!   lake's identity check sees the catalog's names.
//! - **no footer statistics for attach**: [`LakeFormat::has_footer_stats`]
//!   stays `false`, so attach reads the CRC-protected statistics block from
//!   the manifest and touches no data file. A projected query reads the
//!   footer once (ranged) to map the file's physical schema.
//!
//! [`register`] installs the provider process-wide; it is idempotent for this
//! provider (a duplicate registration of another provider under `parquet`
//! still fails).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::format::{
    DataFile, EncodedFile, FileFooter, LakeFormat, RangedFile, register_lake_format,
};
use pink_lake::{LakeError, Result};
use pinkdb::{
    ColumnStats, DataTable, DataType, PinkDb, PinkDbError, TableFunctionArg, TableFunctionCall,
    TableScanOutput, Value, Vector,
};

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

/// [`pinkdb_parquet::RangeSource`] over the lake's positional reader.
#[derive(Debug)]
struct LakeRange(Arc<dyn RangedFile>);

impl pinkdb_parquet::RangeSource for LakeRange {
    fn len(&self) -> u64 {
        self.0.len()
    }

    fn read_range(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, String> {
        self.0.read_range(offset, len).map_err(|e| e.to_string())
    }

    fn parallelism(&self) -> usize {
        self.0.parallelism()
    }
}

/// A [`RangeSource`](pinkdb_parquet::RangeSource) over one in-memory delete
/// file: a delete file is tiny, so it is read whole.
#[derive(Debug)]
struct BytesRange(Vec<u8>);

impl pinkdb_parquet::RangeSource for BytesRange {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }

    fn read_range(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, String> {
        let start = usize::try_from(offset).map_err(|_| "offset overflow".to_string())?;
        let end = start
            .checked_add(usize::try_from(len).map_err(|_| "length overflow".to_string())?)
            .ok_or_else(|| "range overflow".to_string())?;
        self.0
            .get(start..end)
            .map(|slice| slice.to_vec())
            .ok_or_else(|| format!("range {offset}+{len} is outside the file"))
    }

    fn parallelism(&self) -> usize {
        1
    }
}

impl LakeFormat for ParquetLakeFormat {
    fn id(&self) -> &str {
        FORMAT_ID
    }

    fn extension(&self) -> &str {
        EXTENSION
    }

    fn delete_format(&self) -> &str {
        // DuckLake's own delete files: a Parquet table of (file_path, pos).
        "parquet"
    }

    fn delete_extension(&self) -> &str {
        EXTENSION
    }

    /// Decode DuckLake's Parquet delete file. Each row names a data file and
    /// a 0-based position in it; rows for another data file are ignored (one
    /// catalog delete row binds one data file, but a rewritten DuckDB file
    /// may carry several).
    fn read_delete_file(
        &self,
        bytes: &[u8],
        data_file: &str,
        path: &std::path::Path,
    ) -> Result<Vec<u64>> {
        let source = Arc::new(BytesRange(bytes.to_vec()));
        let scan = pinkdb_parquet::scan_source("<delete>", source, None, &[]).map_err(|e| {
            corrupt_path(path, format!("cannot decode the parquet delete file: {e}"))
        })?;
        let wanted = data_file.rsplit('/').next().unwrap_or(data_file);
        let mut positions: Vec<u64> = Vec::new();
        for chunk in &scan.output.chunks {
            if chunk.columns.len() < 2 {
                return Err(corrupt_path(
                    path,
                    "a parquet delete file needs (file_path, pos)".to_string(),
                ));
            }
            for row in 0..chunk.num_rows {
                let file = match chunk.columns[0].get(row) {
                    Value::Varchar(v) => v,
                    other => {
                        return Err(corrupt_path(
                            path,
                            format!("parquet delete file_path is {other:?}, not VARCHAR"),
                        ));
                    }
                };
                if !(file == data_file
                    || file.ends_with(&format!("/{data_file}"))
                    || file.ends_with(&format!("/{wanted}"))
                    || file == wanted)
                {
                    continue;
                }
                let pos = match chunk.columns[1].get(row) {
                    Value::Bigint(v) if v >= 0 => v as u64,
                    Value::Integer(v) if v >= 0 => v as u64,
                    other => {
                        return Err(corrupt_path(
                            path,
                            format!("parquet delete pos is {other:?}, not a non-negative BIGINT"),
                        ));
                    }
                };
                positions.push(pos);
            }
        }
        positions.sort_unstable();
        positions.dedup();
        Ok(positions)
    }

    /// Encode DuckLake's Parquet `(file_path, pos)` delete file. `file_path`
    /// is the data file's full path (DuckDB compares it to the data path it
    /// resolves), so a DuckDB reader applies pinkdb's deletes.
    fn write_delete_file(
        &self,
        _db: &str,
        table: &str,
        _data_file: &str,
        data_file_path: &str,
        positions: &[u64],
    ) -> Result<Vec<u8>> {
        let mut rows = DataTable::new(
            table.to_string(),
            vec![
                ("file_path".to_string(), DataType::Varchar),
                ("pos".to_string(), DataType::Bigint),
            ],
        )
        .map_err(|e| engine(format!("creating the delete table of \"{table}\""), e))?;
        for position in positions {
            rows.append_row(vec![
                Value::Varchar(data_file_path.to_string()),
                Value::Bigint(*position as i64),
            ])
            .map_err(|e| engine(format!("appending a delete position of \"{table}\""), e))?;
        }
        pinkdb_parquet::write::write_table(&rows).map_err(|e| LakeError::Engine {
            context: format!("encoding the parquet delete file of \"{table}\""),
            source: PinkDbError::Execution(e.to_string()),
        })
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
            stats: pinkdb::persist::table_stats(db, "main", table, src),
            footer: None,
        })
    }

    /// The lake's own writer: the same parquet encoding, with the catalog's
    /// column ids stamped as `field_id`s so DuckLake readers find the
    /// children of an ARRAY column.
    fn write_streamed_with_fields(
        &self,
        db: &str,
        table: &str,
        src: &DataTable,
        fields: &[pink_lake::format::FieldIds],
        sink: &mut (dyn std::io::Write + Send),
    ) -> Result<(
        pinkdb::persist::SavedTableStats,
        Option<pink_lake::format::FooterLocation>,
    )> {
        let ids: Vec<(i32, i32)> = fields
            .iter()
            .map(|f| {
                (
                    i32::try_from(f.column).unwrap_or(0),
                    f.array_child
                        .and_then(|child| i32::try_from(child).ok())
                        .unwrap_or(0),
                )
            })
            .collect();
        let bytes =
            pinkdb_parquet::write::write_table_with_identity_and_fields(src, db, table, &ids)
                .map_err(|e| match e {
                    pinkdb_parquet::write::WriteError::UnsupportedType { .. } => {
                        LakeError::NotRepresentable {
                            message: e.to_string(),
                        }
                    }
                    other => LakeError::Engine {
                        context: format!("encoding table \"{db}.{table}\" as parquet"),
                        source: PinkDbError::Execution(other.to_string()),
                    },
                })?;
        sink.write_all(&bytes).map_err(|e| LakeError::Io {
            context: format!("writing the data file of \"{db}.{table}\""),
            source: e,
        })?;
        Ok((pinkdb::persist::table_stats(db, "main", table, src), None))
    }

    fn read(&self, file: &DataFile<'_>) -> Result<PinkDb> {
        // A local plaintext file keeps its path-based reader (one open, one
        // positioned read stream — the pre-98b behavior, no regression).
        if let Some(path) = file.local_path() {
            return read_local_path(file, &path);
        }
        if let Some(source) = file.range_source() {
            // Object storage: footer + every column chunk through the shared
            // ranged scan core, never a whole-object download.
            let path_text = file.path().display().to_string();
            let scan =
                pinkdb_parquet::scan_source(&path_text, Arc::new(LakeRange(source)), None, &[])
                    .map_err(|e| corrupt(file, format!("cannot decode the parquet data: {e}")))?;
            let schema = meta_schema(&scan.meta);
            let (db, table) = file_identity(file, &scan.meta);
            return build_store(file, &db, &table, schema, scan.output);
        }
        // Encrypted lakes have no plaintext byte ranges: stage one decrypted
        // copy and use the path-based reader.
        let bytes = file.read()?;
        let staged = StagedFile::new(&bytes).map_err(|e| LakeError::Corrupt {
            path: file.path().to_path_buf(),
            message: format!("cannot stage the data file for decoding: {e}"),
        })?;
        read_local_path(file, &staged.path())
    }

    /// The footer's schema/identity/row count, without decoding a column. Not
    /// advertised as attach statistics ([`LakeFormat::has_footer_stats`] is
    /// `false`): attach stays manifest-only. A projected query calls this to
    /// map the file's physical schema through a v5 record.
    fn footer_stats(&self, file: &DataFile<'_>) -> Result<Option<FileFooter>> {
        let Some(meta) = read_meta(file)? else {
            return Ok(None);
        };
        let (db, table) = file_identity(file, &meta);
        Ok(Some(FileFooter {
            db,
            table,
            rows: meta.rows,
            schema: meta_schema(&meta),
            // Parquet's own statistics are not gated to the lake's exactness
            // rules here; attach uses the manifest block instead.
            stats: vec![ColumnStats::unknown(); meta.columns.len()],
        }))
    }

    fn supports_projection(&self) -> bool {
        true
    }

    fn prunes_row_groups(&self) -> bool {
        true
    }

    fn scan(
        &self,
        file: &DataFile<'_>,
        columns: &[usize],
        filters: &[pinkdb::PrunePredicate],
        sink: &mut dyn FnMut(u64, pinkdb::DataChunk) -> Result<()>,
    ) -> Result<(u64, pink_lake::format::ScanCounters)> {
        let Some(source) = file.range_source() else {
            // Encrypted (or a backend without positioned reads): decode whole.
            return pink_lake::format::scan_whole_file(self, file, columns, filters, sink);
        };
        let path_text = file.path().display().to_string();
        let mut rows = 0u64;
        let mut offset = 0u64;
        let mut failure: Option<LakeError> = None;
        let scan = pinkdb_parquet::scan_source_streamed(
            &path_text,
            Arc::new(LakeRange(source)),
            Some(columns),
            filters,
            &mut |chunk| {
                let n = chunk.num_rows() as u64;
                match sink(offset, chunk) {
                    Ok(()) => {
                        offset += n;
                        rows += n;
                        Ok(())
                    }
                    Err(e) => {
                        failure = Some(e);
                        // The parquet core only propagates this; our error is
                        // returned below, keeping the lake error type.
                        Err(PinkDbError::Execution("lake scan sink failed".to_string()))
                    }
                }
            },
        );
        let streamed = match scan {
            Ok(streamed) => streamed,
            Err(e) => {
                // A failure the sink itself reported is the engine's error,
                // not a corrupt file.
                return match failure {
                    Some(error) => Err(error),
                    None => Err(corrupt(
                        file,
                        format!("cannot decode the parquet data: {e}"),
                    )),
                };
            }
        };
        Ok((
            rows,
            pink_lake::format::ScanCounters {
                row_groups: streamed.output.row_groups,
                row_groups_pruned: streamed.output.row_groups_pruned,
                pages: streamed.pages.pages,
                pages_pruned: streamed.pages.pages_pruned,
            },
        ))
    }

    fn read_projected(
        &self,
        file: &DataFile<'_>,
        columns: &[usize],
        filters: &[pinkdb::PrunePredicate],
    ) -> Result<Option<(PinkDb, Vec<bool>)>> {
        let Some(source) = file.range_source() else {
            // Encrypted (or a backend without positioned reads): the caller
            // falls back to a full decode.
            return Ok(None);
        };
        let path_text = file.path().display().to_string();
        let scan = pinkdb_parquet::scan_source(
            &path_text,
            Arc::new(LakeRange(source)),
            Some(columns),
            filters,
        )
        .map_err(|e| corrupt(file, format!("cannot decode the parquet data: {e}")))?;
        let schema = meta_schema(&scan.meta);
        let (db, table) = file_identity(file, &scan.meta);
        let mut present = vec![false; schema.len()];
        for &column in columns {
            if column < present.len() {
                present[column] = true;
            }
        }
        let output = fill_placeholders(scan.output, &schema, &present)?;
        let store = build_store(file, &db, &table, schema, output)?;
        Ok(Some((store, present)))
    }
}

/// Decode one parquet file through the path-based reader (a local plaintext
/// file, or a decrypted staged copy of an encrypted one). This is the
/// pre-98b path; object-storage files use the ranged scan core instead.
fn read_local_path(file: &DataFile<'_>, path: &std::path::Path) -> Result<PinkDb> {
    let path_text = path.display().to_string();
    let source = pinkdb_parquet::source();
    let call = TableFunctionCall::Function {
        name: "read_parquet".to_string(),
        args: vec![TableFunctionArg {
            name: None,
            value: Value::Varchar(path_text),
        }],
    };
    let schema = source
        .bind(&call)
        .map_err(|e| corrupt(file, format!("cannot read the parquet footer: {e}")))?
        .ok_or_else(|| {
            corrupt(
                file,
                "the parquet source did not claim the data file".to_string(),
            )
        })?;
    let output = source
        .scan(&call, None, &[])
        .map_err(|e| corrupt(file, format!("cannot decode the parquet data: {e}")))?;
    let identity = pinkdb_parquet::write::read_identity(path)
        .map_err(|e| corrupt(file, format!("cannot read the parquet metadata: {e}")))?;
    let (db, table) = identity
        .or_else(|| identity_from_rel(file.rel()))
        .unwrap_or_else(|| ("main".to_string(), "data".to_string()));
    build_store(file, &db, &table, schema, output)
}

/// Read one file's footer metadata through the positional source when there
/// is one, else the local path; `None` for a sealed lake (no plaintext
/// footer).
fn read_meta(file: &DataFile<'_>) -> Result<Option<pinkdb_parquet::FileMeta>> {
    if let Some(source) = file.range_source() {
        let path = file.path().display().to_string();
        return pinkdb_parquet::read_meta_source(&path, Arc::new(LakeRange(source)))
            .map(Some)
            .map_err(|e| corrupt(file, format!("cannot read the parquet footer: {e}")));
    }
    let Some(path) = file.local_path() else {
        return Ok(None);
    };
    let path_text = path.display().to_string();
    pinkdb_parquet::read_meta_path(&path_text)
        .map(Some)
        .map_err(|e| corrupt(file, format!("cannot read the parquet footer: {e}")))
}

fn meta_schema(meta: &pinkdb_parquet::FileMeta) -> Vec<(String, DataType)> {
    meta.columns
        .iter()
        .map(|column| (column.name.clone(), column.ty))
        .collect()
}

/// A file's declared database/table identity from its footer. `None` for a
/// file this lake did not write (for example a Parquet file from DuckDB's
/// `ducklake` extension), which carries no pink identity keys.
fn meta_identity(meta: &pinkdb_parquet::FileMeta) -> Option<(String, String)> {
    let mut db = None;
    let mut table = None;
    for (key, value) in &meta.kv {
        match key.as_str() {
            pinkdb_parquet::write::DB_METADATA_KEY => {
                db = String::from_utf8(value.clone()).ok();
            }
            pinkdb_parquet::write::TABLE_METADATA_KEY => {
                table = String::from_utf8(value.clone()).ok();
            }
            _ => {}
        }
    }
    Some((db?, table?))
}

/// The file's database/table identity: the footer's pink identity when the
/// file declares one, else the DuckLake layout it sits in
/// (`<data path>/<schema path>/<table path>/<file>`). The catalog is the
/// authority for a foreign file; deriving the identity from the path keeps
/// the lake's file-identity check meaningful instead of rejecting every file
/// DuckDB wrote.
fn file_identity(file: &DataFile<'_>, meta: &pinkdb_parquet::FileMeta) -> (String, String) {
    meta_identity(meta)
        .or_else(|| identity_from_rel(file.rel()))
        .unwrap_or_else(|| ("main".to_string(), "data".to_string()))
}

/// The schema-path/table-path pair a lake-relative file sits under. The
/// trailing component is the file name, so the two before it name the table
/// path and schema path.
fn identity_from_rel(rel: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = rel.split('/').filter(|part| !part.is_empty()).collect();
    if parts.len() < 3 {
        return None;
    }
    let table = parts[parts.len() - 2];
    let schema = parts[parts.len() - 3];
    Some((schema.to_string(), table.to_string()))
}

/// Replace a projected scan's zero-length placeholders with NULL vectors of
/// the chunk's row count, so the lake can merge the file into its full-schema
/// cache (every column chunk-aligned).
fn fill_placeholders(
    mut output: TableScanOutput,
    schema: &[(String, DataType)],
    present: &[bool],
) -> Result<TableScanOutput> {
    for chunk in &mut output.chunks {
        let rows = chunk.num_rows();
        for (index, (_, dtype)) in schema.iter().enumerate() {
            if present.get(index).copied().unwrap_or(false) {
                continue;
            }
            let mut vector = Vector::with_capacity(*dtype, rows);
            for _ in 0..rows {
                vector.push_null().map_err(|e| LakeError::Engine {
                    context: format!("building the NULL placeholder of column {index}"),
                    source: e,
                })?;
            }
            chunk.columns[index] = Arc::new(vector);
        }
    }
    Ok(output)
}

/// Build the one-database/one-table store the lake's loaders expect.
fn build_store(
    file: &DataFile<'_>,
    db: &str,
    table: &str,
    schema: Vec<(String, DataType)>,
    output: TableScanOutput,
) -> Result<PinkDb> {
    let mut store = PinkDb::new();
    if !db.eq_ignore_ascii_case("main") {
        store
            .create_database(db)
            .map_err(|e| engine(format!("creating database \"{db}\""), e))?;
        store
            .drop_database("main")
            .map_err(|e| engine("dropping the default database".to_string(), e))?;
    }
    store
        .create_table_in(db, table, schema)
        .map_err(|e| engine(format!("creating table \"{db}.{table}\""), e))?;
    {
        let dst = store
            .table_mut_in(db, table)
            .map_err(|e| engine(format!("opening table \"{db}.{table}\""), e))?;
        for chunk in output.chunks {
            dst.append_shared_chunk(&chunk.columns)
                .map_err(|e| engine(format!("appending rows to \"{db}.{table}\""), e))?;
        }
    }
    let _ = file;
    Ok(store)
}

fn corrupt(file: &DataFile<'_>, message: String) -> LakeError {
    LakeError::Corrupt {
        path: file.path().to_path_buf(),
        message,
    }
}

fn corrupt_path(path: &std::path::Path, message: String) -> LakeError {
    LakeError::Corrupt {
        path: path.to_path_buf(),
        message,
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
