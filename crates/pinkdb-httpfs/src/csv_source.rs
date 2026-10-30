//! The wave-75 `read_csv` table source: local files and `https://`/`s3://`
//! objects over the unified [`pinkdb::TableSource`] registry.
//!
//! # Claim rules (replacement scan)
//!
//! - A **local** path ending in `.csv` (case-insensitive) is claimed.
//! - An `https://` or `s3://` URL is claimed only when its path component
//!   (query/fragment stripped) ends in `.csv`.
//! - Any other scheme is never claimed, and a non-`.csv` remote path stays
//!   unclaimed, so `FROM 'x.parquet'` / a future `s3://…/x.parquet` goes to
//!   the parquet source when it is registered. `read_csv('…')` is explicit
//!   and accepts any local path or supported URL.
//!
//! # Semantics
//!
//! `read_csv` and `.import` share one parser and one option set
//! ([`CsvOptions`] in the engine): dialect, encoding, header handling, NULL
//! rules, inference and the error policy are the same code, so a `read_csv`
//! and an `.import` of one file agree by construction, locally and remotely.
//!
//! Projection pushdown is applied *after* parsing (the CSV parser decodes
//! every column; there is no column pruning in the format), and unprojected
//! columns become zero-length placeholders per the scan contract. `WHERE`
//! predicates are not pushed into the reader — the engine re-applies them
//! above the scan, exactly like every source that reports no statistics.
//!
//! # Durability
//!
//! A durable statement that reads a CSV replays by **re-reading** the file
//! or URL (the unified file-source policy), so the source must be registered
//! process-wide before the store is opened — [`ensure_registered`] is the
//! CLI/bootstrap step. A file deleted before replay is a hard recovery error;
//! a changed file replays its new content (documented, DuckDB's rule).
//!
//! # Options
//!
//! A named argument is applied through the engine's
//! [`CsvOptions::apply`], the same table `.import key=value` uses; the names
//! are DuckDB's (`delimiter`, `quote`, `escape`, `encoding`, `header`,
//! `nullstr`, `names`, `errors`, `allow_quoted_nulls`, `force_not_null`).
//! `errors=warn` skips a bad row instead of failing the scan (a table
//! function has no place to print the report `.import` prints).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pinkdb::attached::PrunePredicate;
pub use pinkdb::csv::{CsvOptions, CsvSchema, Encoding};
use pinkdb::csv::{parse_csv_chunks, sniff_csv, stream_csv_chunks};
use pinkdb::{
    DataChunk, DataType, PinkDb, PinkDbError, Result, TableFunctionArg, TableFunctionCall,
    TableScanOutput, TableSource, TableSourceStats, Vector,
};

use crate::{Fetcher, HttpfsConfig, open_with};

/// The registry name (`EXPLAIN` shows it; counters are attributed to it).
pub const SOURCE_NAME: &str = "httpfs";

/// One parsed `read_csv` / replacement-scan call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The local path or remote URL, as written.
    pub path: String,
    pub options: CsvOptions,
    /// Whether the file is gzip-compressed: `compression = 'gzip'`, or
    /// `'auto'` (the default) and a `.gz` path — DuckDB's rule.
    pub gzip: bool,
}

impl Call {
    fn new(path: String, options: CsvOptions, compression: Option<&str>) -> Result<Self> {
        let gzip = match compression.map(str::to_ascii_lowercase).as_deref() {
            None | Some("auto" | "infer") => is_gzip_path(&path),
            Some("gzip") => true,
            Some("none" | "uncompressed") => false,
            Some(other) => {
                return Err(PinkDbError::Parameter(format!(
                    "CSV compression `{other}` is not supported: use auto, gzip or none"
                )));
            }
        };
        Ok(Self {
            path,
            options,
            gzip,
        })
    }

    fn cache_key(&self) -> String {
        format!("{}|{:?}|{}", self.path, self.options, self.gzip)
    }
}

/// Whether a path names a gzip file (`.gz`, before any URL query).
fn is_gzip_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower
        .split(['?', '#'])
        .next()
        .unwrap_or(&lower)
        .ends_with(".gz")
}

/// The decompressed bytes of a whole gzip file (every member, like `zcat`).
fn gunzip(path: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(bytes)
        .read_to_end(&mut out)
        .map_err(|e| {
            PinkDbError::Execution(format!("cannot decompress \"{path}\" as gzip: {e}"))
        })?;
    Ok(out)
}

/// Whether the replacement scan claims `path` (see the module docs).
pub fn claims_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    if lower.starts_with("https://") || lower.starts_with("s3://") {
        let rest = lower
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(&lower);
        let path_part = rest.split(['?', '#']).next().unwrap_or(rest);
        return path_part.ends_with(".csv") || path_part.ends_with(".csv.gz");
    }
    if lower.contains("://") {
        return false;
    }
    lower.ends_with(".csv") || lower.ends_with(".csv.gz")
}

/// Parse a table-function call into a [`Call`], or `None` when this source
/// does not serve it. `read_csv` is matched case-insensitively; the
/// replacement scan follows [`claims_path`].
pub fn parse_call(call: &TableFunctionCall) -> Result<Option<Call>> {
    match call {
        TableFunctionCall::Path(path) => {
            if claims_path(path) {
                Ok(Some(Call::new(path.clone(), CsvOptions::default(), None)?))
            } else {
                Ok(None)
            }
        }
        TableFunctionCall::Function { name, args } if name.eq_ignore_ascii_case("read_csv") => {
            let path = positional_path(args)?;
            let options = parse_options(args)?;
            let compression = args
                .iter()
                .find(|a| a.name_lower().as_deref() == Some("compression"))
                .map(|a| option_text(a, "compression"))
                .transpose()?;
            Ok(Some(Call::new(path, options, compression.as_deref())?))
        }
        _ => Ok(None),
    }
}

fn positional_path(args: &[TableFunctionArg]) -> Result<String> {
    let mut path = None;
    for arg in args {
        if arg.name.is_some() {
            continue;
        }
        if path.is_some() {
            return Err(PinkDbError::Catalog(
                "read_csv takes exactly one path argument".to_string(),
            ));
        }
        match &arg.value {
            pinkdb::Value::Varchar(p) => path = Some(p.clone()),
            other => {
                return Err(PinkDbError::Type(format!(
                    "read_csv expects a string path, got {other:?}"
                )));
            }
        }
    }
    path.ok_or_else(|| PinkDbError::Catalog("read_csv takes exactly one path argument".to_string()))
}

/// Apply a call's named arguments through the engine's option table — one
/// parser for `.import` and `read_csv`.
fn parse_options(args: &[TableFunctionArg]) -> Result<CsvOptions> {
    let mut options = CsvOptions::default();
    for arg in args {
        let Some(name) = arg.name_lower() else {
            continue;
        };
        // The file's compression is the source's business, not the parser's.
        if name == "compression" {
            continue;
        }
        options.apply(&name, &option_text(arg, &name)?)?;
    }
    Ok(options)
}

/// One option value as text. DuckDB accepts numbers/booleans where the value
/// is a scalar (`header := 1`, `sample_size := 100`), so only the string-ish
/// kinds are converted and everything else is refused precisely.
fn option_text(arg: &TableFunctionArg, name: &str) -> Result<String> {
    Ok(match &arg.value {
        pinkdb::Value::Varchar(s) => s.clone(),
        pinkdb::Value::Integer(i) => i.to_string(),
        pinkdb::Value::Bigint(i) => i.to_string(),
        pinkdb::Value::Double(f) => f.to_string(),
        pinkdb::Value::Boolean(b) => b.to_string(),
        other => {
            return Err(PinkDbError::Type(format!(
                "read_csv option `{name}` expects a string, got {other:?}"
            )));
        }
    })
}

fn is_remote(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("https://") || lower.starts_with("http://") || lower.starts_with("s3://")
}

#[derive(Debug)]
struct Cached {
    key: String,
    schema: CsvSchema,
    body: Body,
}

/// What bind kept of the file for the scan.
#[derive(Debug)]
enum Body {
    /// The whole file, in memory.
    Whole(Vec<u8>),
    /// The file's beginning (the schema probe), for a file read in windows.
    Stream(Vec<u8>),
}

/// Bytes a remote file must exceed to be streamed instead of read whole. Up
/// to this much there is nothing to bound (it is the read-ahead window
/// anyway), so a small file costs one probe and is then cached for the scan.
const CACHE_MAX_BYTES: u64 = crate::prefetch::DEFAULT_WINDOW_BYTES;

/// The schema probe: one ranged read of this many bytes. A shorter answer is
/// the whole file. The same trade-off DuckDB's sniffer makes — a row past this
/// window that does not fit the inferred types fails the scan with a CSV type
/// error.
const SNIFF_BYTES: u64 = CACHE_MAX_BYTES;

/// The httpfs [`TableSource`]: one instance per store, with a one-statement
/// fetch cache and its own counters.
#[derive(Debug)]
pub struct CsvSource {
    config: HttpfsConfig,
    cache: Mutex<Option<Cached>>,
    scans: AtomicU64,
    rows: AtomicU64,
    files: AtomicU64,
    columns: AtomicU64,
}

impl CsvSource {
    pub fn new() -> Self {
        Self::with_config(HttpfsConfig::default())
    }

    pub fn with_config(config: HttpfsConfig) -> Self {
        Self {
            config,
            cache: Mutex::new(None),
            scans: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            files: AtomicU64::new(0),
            columns: AtomicU64::new(0),
        }
    }

    fn open(&self, path: &str) -> Result<Box<dyn Fetcher>> {
        open_with(path, &self.config)
            .map_err(|e| PinkDbError::Execution(format!("cannot open \"{path}\": {e}")))
    }

    /// The schema, plus the file's beginning: the whole file when one probe
    /// returned it, else the probe's bytes for the streaming scan. A local
    /// file is always read whole (there is no window to bound).
    fn load(&self, call: &Call) -> Result<(Body, CsvSchema)> {
        if !is_remote(&call.path) {
            let mut bytes = std::fs::read(&call.path).map_err(|e| {
                PinkDbError::Execution(format!("cannot read CSV \"{}\": {e}", call.path))
            })?;
            if call.gzip {
                bytes = gunzip(&call.path, &bytes)?;
            }
            let schema = sniff_csv(&bytes, &call.options)?;
            return Ok((Body::Whole(bytes), schema));
        }
        // A compressed object has no byte ranges to stream: fetch it whole.
        if call.gzip {
            let raw = self.open(&call.path)?.fetch().map_err(|e| {
                PinkDbError::Execution(format!("cannot read \"{}\": {e}", call.path))
            })?;
            let bytes = gunzip(&call.path, &raw)?;
            let schema = sniff_csv(&bytes, &call.options)?;
            return Ok((Body::Whole(bytes), schema));
        }
        let head = self
            .open(&call.path)?
            .fetch_range(0, SNIFF_BYTES)
            .map_err(|e| PinkDbError::Execution(format!("cannot read \"{}\": {e}", call.path)))?;
        let schema = sniff_csv(&head, &call.options)?;
        if (head.len() as u64) < SNIFF_BYTES {
            // The probe returned less than it asked for: that was the whole
            // file, so the scan re-parses it without another request.
            Ok((Body::Whole(head), schema))
        } else {
            Ok((Body::Stream(head), schema))
        }
    }

    /// Drop the columns `projection` did not ask for from one parsed chunk:
    /// absent columns become empty vectors, so every chunk keeps the bound
    /// schema's shape.
    fn project(chunk: DataChunk, selected: &[bool], types: &[DataType]) -> DataChunk {
        if selected.iter().all(|s| *s) {
            return chunk;
        }
        let columns: Vec<Arc<Vector>> = chunk
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if selected[i] {
                    Arc::clone(c)
                } else {
                    Arc::new(Vector::with_capacity(types[i], 0))
                }
            })
            .collect();
        DataChunk::projected(columns, chunk.num_rows)
    }

    /// Run one bound call's scan, handing each projected chunk to `sink` as it
    /// is parsed. Returns the rows and the decoded column count.
    fn scan_call(
        &self,
        call: &Call,
        projection: Option<&[usize]>,
        sink: &mut dyn FnMut(DataChunk) -> Result<()>,
    ) -> Result<(u64, u64)> {
        let cached = {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match cache.as_ref() {
                Some(entry) if entry.key == call.cache_key() => cache.take(),
                _ => None,
            }
        };
        let (body, schema) = match cached {
            Some(entry) => (entry.body, entry.schema),
            None => self.load(call)?,
        };
        let types = &schema.types;
        let selected: Vec<bool> = match projection {
            Some(projection) => (0..types.len()).map(|i| projection.contains(&i)).collect(),
            None => vec![true; types.len()],
        };
        let decoded = selected.iter().filter(|s| **s).count() as u64;
        let mut rows = 0u64;
        let mut emit = |chunk: DataChunk| {
            rows += chunk.num_rows as u64;
            sink(Self::project(chunk, &selected, types))
        };
        match body {
            Body::Whole(bytes) => {
                for chunk in parse_csv_chunks(&bytes, &schema)? {
                    emit(chunk)?;
                }
            }
            Body::Stream(head) => {
                let reader =
                    crate::prefetch::Prefetch::open_after_head(self.open(&call.path)?, head);
                stream_csv_chunks(reader, &schema, &mut emit)?;
            }
        }
        Ok((rows, decoded))
    }

    fn count(&self, rows: u64, decoded: u64) {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.files.fetch_add(1, Ordering::Relaxed);
        self.columns.fetch_add(decoded, Ordering::Relaxed);
    }
}

impl Default for CsvSource {
    fn default() -> Self {
        Self::new()
    }
}

impl TableSource for CsvSource {
    fn name(&self) -> &str {
        SOURCE_NAME
    }

    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
        let Some(call) = parse_call(call)? else {
            return Ok(None);
        };
        let (body, schema) = self.load(&call)?;
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *cache = Some(Cached {
            key: call.cache_key(),
            body,
            schema: schema.clone(),
        });
        Ok(Some(
            schema
                .names
                .iter()
                .cloned()
                .zip(schema.types.iter().copied())
                .collect(),
        ))
    }

    fn scan(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        _filters: &[PrunePredicate],
    ) -> Result<TableScanOutput> {
        let Some(call) = parse_call(call)? else {
            return Err(PinkDbError::Execution(format!(
                "the httpfs CSV source does not serve {call:?}"
            )));
        };
        let mut chunks = Vec::new();
        let (rows, decoded) = self.scan_call(&call, projection, &mut |chunk| {
            chunks.push(chunk);
            Ok(())
        })?;
        self.count(rows, decoded);
        Ok(TableScanOutput {
            chunks,
            files: 1,
            row_groups: 0,
            row_groups_pruned: 0,
            columns_decoded: decoded,
        })
    }

    fn scan_stream(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        _filters: &[PrunePredicate],
        sink: &mut dyn FnMut(DataChunk) -> Result<()>,
    ) -> Result<TableScanOutput> {
        let Some(call) = parse_call(call)? else {
            return Err(PinkDbError::Execution(format!(
                "the httpfs CSV source does not serve {call:?}"
            )));
        };
        let (rows, decoded) = self.scan_call(&call, projection, sink)?;
        self.count(rows, decoded);
        Ok(TableScanOutput {
            chunks: Vec::new(),
            files: 1,
            row_groups: 0,
            row_groups_pruned: 0,
            columns_decoded: decoded,
        })
    }

    /// Streams exactly when bind did not keep the file whole: a local file (or
    /// a remote one within the window) is parsed from memory anyway, and the
    /// materialized fold runs in parallel.
    fn streams_scan(&self, call: &TableFunctionCall) -> bool {
        let Ok(Some(call)) = parse_call(call) else {
            return false;
        };
        let cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.as_ref().is_some_and(|entry| {
            entry.key == call.cache_key() && matches!(entry.body, Body::Stream(_))
        })
    }

    fn stats(&self) -> TableSourceStats {
        TableSourceStats {
            name: SOURCE_NAME.to_string(),
            scans: self.scans.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            files: self.files.load(Ordering::Relaxed),
            row_groups: 0,
            row_groups_pruned: 0,
            columns_decoded: self.columns.load(Ordering::Relaxed),
        }
    }

    fn reset_stats(&self) {
        self.scans.store(0, Ordering::Relaxed);
        self.rows.store(0, Ordering::Relaxed);
        self.files.store(0, Ordering::Relaxed);
        self.columns.store(0, Ordering::Relaxed);
    }
}

/// The host's registration handle:
/// `db.register_table_source(pinkdb_httpfs::source())`.
pub fn source() -> Arc<dyn TableSource> {
    Arc::new(CsvSource::with_config(HttpfsConfig::from_env()))
}

/// [`source`] with explicit transport configuration (tests/local endpoints).
pub fn source_with(config: HttpfsConfig) -> Arc<dyn TableSource> {
    Arc::new(CsvSource::with_config(config))
}

/// The engine's `.import <url>` / `import_csv(url)` hook over this crate's
/// transport: the same scheme set the table source serves.
#[derive(Debug)]
pub struct UrlImportFetcher {
    config: HttpfsConfig,
}

impl pinkdb::csv::UrlFetcher for UrlImportFetcher {
    fn supports(&self, url: &str) -> bool {
        is_remote(url)
    }

    fn fetch(&self, url: &str) -> std::result::Result<Vec<u8>, String> {
        let fetcher: Box<dyn Fetcher> =
            open_with(url, &self.config).map_err(|e| format!("cannot open \"{url}\": {e}"))?;
        fetcher
            .fetch()
            .map_err(|e| format!("cannot read \"{url}\": {e}"))
    }
}

/// The URL-import hook instance (public for embedders that register their own
/// store-level pieces).
pub fn url_import_fetcher(config: HttpfsConfig) -> Arc<dyn pinkdb::csv::UrlFetcher> {
    Arc::new(UrlImportFetcher { config })
}

/// Register the source on one store (embedder path). Idempotent.
pub fn register(db: &mut PinkDb) {
    db.register_table_source(source());
}

/// Register the source and the URL-import hook **process-wide**, for every
/// store created afterwards — the host-bootstrap step a durable store needs
/// (recovery replays inside [`PinkDb::open_durable`], before the caller holds
/// the store). The CLI calls this once at startup; a `httpfs`-built binary
/// resolves `read_csv`, `FROM 'https://…/x.csv'` and `.import <url>` during
/// replay.
pub fn ensure_registered() {
    ensure_registered_with(HttpfsConfig::from_env());
}

/// [`ensure_registered`] with explicit transport configuration.
pub fn ensure_registered_with(config: HttpfsConfig) {
    pinkdb::table_source::register_global_table_source(source_with(config.clone()));
    pinkdb::csv::register_global_url_fetcher(url_import_fetcher(config));
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::csv::{ErrorPolicy, Header};
    use pinkdb::{QueryResult, Value};

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::AtomicU64;
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_httpfs_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Serializes the tests that install process-global hooks with
    /// conflicting transport configurations (the global registry is
    /// process-level by design).
    static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn global_test_lock() -> std::sync::MutexGuard<'static, ()> {
        GLOBAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        let mut out = db.execute(sql).expect("query");
        assert_eq!(out.len(), 1, "{out:?}");
        match out.pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn args(values: Vec<(&str, Value)>) -> Vec<TableFunctionArg> {
        values
            .into_iter()
            .map(|(name, value)| TableFunctionArg {
                name: if name.is_empty() {
                    None
                } else {
                    Some(name.to_string())
                },
                value,
            })
            .collect()
    }

    fn csv_call(path: &str) -> TableFunctionCall {
        TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(vec![("", Value::Varchar(path.to_string()))]),
        }
    }

    #[test]
    fn claim_rules_are_csv_only() {
        for claimed in [
            "x.csv",
            "dir/x.CSV",
            "https://host/a/b.csv",
            "https://host/a/b.csv?token=x",
            "s3://bucket/a/b.csv",
            "s3://bucket/a/b.csv#frag",
        ] {
            assert!(claims_path(claimed), "{claimed}");
        }
        for unclaimed in [
            "x.parquet",
            "x.csv.gz",
            "https://host/a/b.parquet",
            "s3://bucket/a/b.parquet",
            "https://host/a/b.csv.gz",
            "http://host/a/b.csv",
            "ftp://host/a/b.csv",
            "read_csv.csv.bak",
        ] {
            assert!(!claims_path(unclaimed), "{unclaimed}");
        }
        assert!(
            parse_call(&TableFunctionCall::Path("x.parquet".into()))
                .unwrap()
                .is_none()
        );
        assert!(parse_call(&csv_call("x.txt")).unwrap().is_some());
    }

    #[test]
    fn option_shape_defaults_and_refusals() {
        let call = parse_call(&csv_call("x.csv")).unwrap().unwrap();
        assert_eq!(call.options, CsvOptions::default());
        assert_eq!(call.options.delimiter, b',');
        assert_eq!(call.options.header, Header::Auto);

        // Wave-76 options parse through the engine's shared option table.
        let named = |values: Vec<(&str, Value)>| TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(values),
        };
        let call = parse_call(&named(vec![
            ("", Value::Varchar("x.csv".into())),
            ("sep", Value::Varchar("\t".into())),
            ("quote", Value::Varchar("'".into())),
            ("escape", Value::Varchar("\\".into())),
            ("encoding", Value::Varchar("latin-1".into())),
            ("header", Value::Boolean(false)),
            ("errors", Value::Varchar("warn".into())),
            ("nullstr", Value::Varchar("NA, N/A".into())),
            ("names", Value::Varchar("a,b".into())),
            ("allow_quoted_nulls", Value::Boolean(false)),
            ("force_not_null", Value::Varchar("b".into())),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(call.options.delimiter, b'\t');
        assert_eq!(call.options.quote, b'\'');
        assert_eq!(call.options.escape, Some(b'\\'));
        assert_eq!(call.options.encoding, pinkdb::csv::Encoding::Latin1);
        assert_eq!(call.options.header, Header::Absent);
        assert_eq!(call.options.errors, ErrorPolicy::Warn);
        assert_eq!(call.options.nullstr, vec!["NA", "N/A"]);
        assert_eq!(
            call.options.names,
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert!(!call.options.allow_quoted_nulls);
        assert_eq!(call.options.force_not_null, vec!["b"]);

        // The option set is part of the cache key, so two shapes of one path
        // never share a sniff.
        let plain = parse_call(&csv_call("x.csv")).unwrap().unwrap();
        assert_ne!(plain.cache_key(), call_of("x.csv", ";").cache_key());

        // Recognised but unimplemented stays precise, never silently ignored.
        let unsupported = named(vec![
            ("", Value::Varchar("x.csv".into())),
            ("all_varchar", Value::Boolean(true)),
        ]);
        let err = parse_call(&unsupported).unwrap_err().to_string();
        assert!(err.contains("all_varchar"), "{err}");
        assert!(err.contains("not supported yet"), "{err}");

        let unknown = named(vec![
            ("", Value::Varchar("x.csv".into())),
            ("bogus", Value::Integer(1)),
        ]);
        let err = parse_call(&unknown).unwrap_err().to_string();
        assert!(err.contains("unknown CSV option `bogus`"), "{err}");

        let bad_delimiter = named(vec![
            ("", Value::Varchar("x.csv".into())),
            ("delimiter", Value::Varchar(";;".into())),
        ]);
        let err = parse_call(&bad_delimiter).unwrap_err().to_string();
        assert!(err.contains("exactly one byte"), "{err}");

        let two = named(vec![
            ("", Value::Varchar("a.csv".into())),
            ("", Value::Varchar("b.csv".into())),
        ]);
        let err = parse_call(&two).unwrap_err().to_string();
        assert!(err.contains("exactly one path"), "{err}");

        let not_string = named(vec![("", Value::Integer(1))]);
        let err = parse_call(&not_string).unwrap_err().to_string();
        assert!(err.contains("string path"), "{err}");

        // Another source's function is not claimed.
        let other = TableFunctionCall::Function {
            name: "read_parquet".to_string(),
            args: args(vec![("", Value::Varchar("x.parquet".into()))]),
        };
        assert!(parse_call(&other).unwrap().is_none());
    }

    fn call_of(path: &str, delimiter: &str) -> Call {
        parse_call(&TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(vec![
                ("", Value::Varchar(path.to_string())),
                ("delimiter", Value::Varchar(delimiter.to_string())),
            ]),
        })
        .unwrap()
        .unwrap()
    }

    /// The same bytes with the same options give the same rows locally and
    /// remotely: options only change how the parser reads, never the
    /// transport split.
    #[test]
    fn options_read_the_same_local_and_remote() {
        let server = crate::test_http::TestHttpServer::start();
        // Latin-1 `é`, semicolon delimiter, CRLF, a quoted field holding `;`.
        let body: &[u8] = b"id;name\r\n1;Andr\xe9\r\n2;\"b;b\"\r\n";
        server.put("/o/opts.csv", body);
        let dir = scratch_dir("opts");
        let local = dir.join("opts.csv");
        std::fs::write(&local, body).unwrap();
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig {
            allow_http: true,
            ..Default::default()
        }));
        let sql = |path: &str| {
            format!(
                "SELECT id, name FROM read_csv('{path}', delimiter := ';', \
                 encoding := 'latin-1', header := true) ORDER BY id;"
            )
        };
        let local_rows = rows(&mut db, &sql(local.to_str().unwrap()));
        assert_eq!(
            local_rows,
            vec![
                vec![Value::Integer(1), Value::Varchar("André".into())],
                vec![Value::Integer(2), Value::Varchar("b;b".into())],
            ]
        );
        assert_eq!(rows(&mut db, &sql(&server.url("/o/opts.csv"))), local_rows);
    }

    /// `errors=warn` skips a malformed row in a scan; the default fails.
    #[test]
    fn warn_policy_skips_bad_rows_in_a_scan() {
        let dir = scratch_dir("warn");
        let path = dir.join("w.csv");
        std::fs::write(&path, "a,b\n1,2\n3,4,5\n6,7\n").unwrap();
        let path = path.to_str().unwrap();
        let mut db = PinkDb::new();
        db.register_table_source(source());
        let err = db
            .execute(&format!("SELECT COUNT(*) FROM read_csv('{path}');"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("3 fields"), "{err}");
        let got = rows(
            &mut db,
            &format!("SELECT SUM(a) FROM read_csv('{path}', errors := 'warn');"),
        );
        assert_eq!(got, vec![vec![Value::Bigint(7)]]);
        // DuckDB's name for the same thing, and the switch back to strict.
        let got = rows(
            &mut db,
            &format!("SELECT SUM(a) FROM read_csv('{path}', ignore_errors := true);"),
        );
        assert_eq!(got, vec![vec![Value::Bigint(7)]]);
        let err = db
            .execute(&format!(
                "SELECT COUNT(*) FROM read_csv('{path}', ignore_errors := false);"
            ))
            .unwrap_err()
            .to_string();
        assert!(err.contains("3 fields"), "{err}");
    }

    /// Timing aid, not a gate: a scan of a file larger than the sniff window
    /// over the test HTTP server (the streaming path), and the same file
    /// locally. Run with `cargo test -p pinkdb-httpfs --all-features
    /// --release -- --ignored --nocapture csv_scan_speed`.
    #[test]
    #[ignore]
    fn csv_scan_speed() {
        let mut body = String::with_capacity(4 << 20);
        body.push_str("id,name,note\n");
        for i in 0..1_200_000u32 {
            body.push_str(&format!("{i},n{i},\"a,b {i}\"\n"));
        }
        let body = body.into_bytes();
        let dir = scratch_dir("speed");
        let file = dir.join("big.csv");
        std::fs::write(&file, &body).unwrap();
        let server = crate::test_http::TestHttpServer::start();
        server.put("/r/big.csv", &body);
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig {
            allow_http: true,
            ..Default::default()
        }));
        let url = server.url("/r/big.csv");
        for (what, path) in [("local", file.display().to_string()), ("remote", url)] {
            let start = std::time::Instant::now();
            let got = rows(
                &mut db,
                &format!("SELECT COUNT(*), SUM(id) FROM read_csv('{path}');"),
            );
            eprintln!(
                "{what}: {} bytes, {:?} -> {got:?}",
                body.len(),
                start.elapsed()
            );
        }
    }

    #[test]
    fn local_scan_projection_filter_and_stats() {
        let dir = scratch_dir("local");
        let file = dir.join("sales.csv");
        std::fs::write(
            &file,
            "id,region,total,active\n1,west,10.5,true\n2,east,,FALSE\n3,west,7,True\n",
        )
        .unwrap();

        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig::default()));
        let sql = format!(
            "SELECT id, region FROM read_csv('{}') WHERE id > 1 ORDER BY id;",
            file.display()
        );
        assert_eq!(
            rows(&mut db, &sql),
            vec![
                vec![Value::Integer(2), Value::Varchar("east".into())],
                vec![Value::Integer(3), Value::Varchar("west".into())],
            ]
        );
        // Replacement scan sees the same file.
        let sql = format!("SELECT COUNT(*), SUM(total) FROM '{}';", file.display());
        assert_eq!(
            rows(&mut db, &sql),
            vec![vec![Value::Bigint(3), Value::Double(17.5)]]
        );
        // The inferred schema matches `.import`'s.
        let stats = db.table_source_stats();
        let csv = stats.iter().find(|s| s.name == SOURCE_NAME).unwrap();
        assert_eq!(csv.scans, 2);
        assert_eq!(csv.rows, 6);
        assert_eq!(csv.files, 2);
        assert!(csv.columns_decoded >= 3);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn local_errors_are_clear() {
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig::default()));
        let err = db
            .execute("SELECT * FROM read_csv('/no/such/file.csv');")
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read CSV"), "{err}");
        assert!(err.contains("/no/such/file.csv"), "{err}");
    }

    #[test]
    fn remote_https_scan_fetches_once_per_statement() {
        let server = crate::test_http::TestHttpServer::start();
        server.put(
            "/r/sales.csv",
            b"id,region,total\n1,west,10.5\n2,east,7\n3,west,9\n",
        );
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig {
            allow_http: true,
            ..Default::default()
        }));
        let url = server.url("/r/sales.csv");
        let got = rows(
            &mut db,
            &format!("SELECT SUM(total) FROM read_csv('{url}') WHERE region = 'west';"),
        );
        assert_eq!(got, vec![vec![Value::Double(19.5)]]);
        // bind fetched once and the scan consumed that fetch.
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.contains("/r/sales.csv"))
                .count(),
            1,
            "{:?}",
            server.requests()
        );

        // A second statement re-reads (DuckDB's rule): two more requests
        // (bind + scan for the second statement), and the edited content is
        // visible.
        server.put("/r/sales.csv", b"id,region,total\n1,west,100\n");
        let got = rows(
            &mut db,
            &format!("SELECT SUM(total) FROM read_csv('{url}');"),
        );
        assert_eq!(got, vec![vec![Value::Bigint(100)]]);
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.contains("/r/sales.csv"))
                .count(),
            2
        );
    }

    #[test]
    fn remote_errors_are_clear() {
        let server = crate::test_http::TestHttpServer::start();
        server.put_status("/denied.csv", 403, b"no");
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig {
            allow_http: true,
            ..Default::default()
        }));
        let err = db
            .execute(&format!(
                "SELECT * FROM read_csv('{}');",
                server.url("/missing.csv")
            ))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read") && err.contains("404"), "{err}");
        let err = db
            .execute(&format!(
                "SELECT * FROM read_csv('{}');",
                server.url("/denied.csv")
            ))
            .unwrap_err()
            .to_string();
        assert!(err.contains("403") || err.contains("AccessDenied"), "{err}");
    }

    /// `.import <url>` / `import_csv(url)` go through the same transport (the
    /// engine's URL hook), and the plain-http opt-in is enforced there too.
    #[test]
    fn import_url_reads_through_the_host_fetcher() {
        let _guard = global_test_lock();
        let server = crate::test_http::TestHttpServer::start();
        server.put("/i/sales.csv", b"id,region\n1,west\n2,east\n");
        let url = server.url("/i/sales.csv");

        ensure_registered_with(HttpfsConfig {
            allow_http: true,
            ..Default::default()
        });
        let mut db = PinkDb::new();
        assert_eq!(pinkdb::csv::import_csv(&mut db, &url, None).unwrap(), 2);
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM sales;"),
            vec![vec![Value::Bigint(2)]]
        );
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.contains("/i/sales.csv"))
                .count(),
            1
        );

        // Without the opt-in the same import is refused before any request.
        ensure_registered_with(HttpfsConfig::default());
        let before = server.requests().len();
        let err = pinkdb::csv::import_csv(&mut db, &url, Some("t2"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("PINKDB_HTTPFS_ALLOW_HTTP"), "{err}");
        assert_eq!(server.requests().len(), before, "no request went out");
    }

    /// `read_csv('s3://…')` end to end over the S3 mock: the source resolves
    /// the bucket from the URL and the region/endpoint/credentials from the
    /// same environment conventions as the lake (PINKDB_S3_ALLOW_HTTP for
    /// the mock's plain-http endpoint).
    /// Point the environment's S3 conventions at `mock` for this test.
    /// Dropping it restores nothing, so every caller takes `global_test_lock`.
    #[cfg(feature = "s3")]
    struct MockEnv;

    #[cfg(feature = "s3")]
    impl Drop for MockEnv {
        fn drop(&mut self) {
            // SAFETY: process-global environment mutation is serialized by
            // GLOBAL_TEST_LOCK (every caller holds it).
            unsafe {
                std::env::remove_var("AWS_ACCESS_KEY_ID");
                std::env::remove_var("AWS_SECRET_ACCESS_KEY");
                std::env::remove_var("AWS_REGION");
                std::env::remove_var("AWS_ENDPOINT_URL_S3");
                std::env::remove_var("PINKDB_S3_ALLOW_HTTP");
            }
        }
    }

    #[cfg(feature = "s3")]
    fn mock_env(mock: &crate::mock_s3::MockS3) -> MockEnv {
        // SAFETY: see `MockEnv`.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
            std::env::set_var("AWS_REGION", "auto");
            std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
            std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
        }
        MockEnv
    }

    /// A CSV body with `rows` rows and a deterministic sum.
    #[cfg(feature = "s3")]
    fn big_csv(rows: u32) -> (String, u64) {
        let mut csv = String::from("id,region,total\n");
        for i in 0..rows {
            csv.push_str(&format!("{i:07},r{},1.5\n", i % 7));
        }
        let sum = u64::from(rows) * u64::from(rows - 1) / 2;
        (csv, sum)
    }

    /// Every range the scan asked for, as `(start, end)`.
    #[cfg(feature = "s3")]
    fn spans(ranges: &[String]) -> Vec<(u64, u64)> {
        let mut spans: Vec<(u64, u64)> = ranges
            .iter()
            .filter_map(|range| {
                let (start, end) = range.strip_prefix("bytes=")?.split_once('-')?;
                Some((start.parse().ok()?, end.parse().ok()?))
            })
            .collect();
        spans.sort_unstable();
        spans
    }

    #[cfg(feature = "s3")]
    #[test]
    fn remote_s3_scan_reads_through_the_shared_client() {
        let _guard = global_test_lock();
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("data.csv", b"a,b\n1,x\n2,y\n");
        let _env = mock_env(&mock);

        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig::default()));
        let got = rows(
            &mut db,
            "SELECT SUM(a) FROM read_csv('s3://bucket/data.csv');",
        );
        assert_eq!(got, vec![vec![Value::Bigint(3)]]);
        assert!(mock.verified_requests() >= 1, "Signed requests only");
        assert!(
            mock.requests().iter().any(|r| r == "GET /bucket/data.csv"),
            "{:?}",
            mock.requests()
        );
    }

    /// A remote file larger than the read-ahead window is streamed: one probe
    /// for the schema, then sequential ranged GETs that each fetch their bytes
    /// exactly once — the parser never waits for the whole file, and memory is
    /// the window rather than the file.
    #[cfg(feature = "s3")]
    #[test]
    fn a_large_remote_csv_streams_in_windows() {
        let _guard = global_test_lock();
        let mock = crate::mock_s3::MockS3::start();
        let (csv, sum) = big_csv(800_000);
        assert!(csv.len() as u64 > crate::prefetch::DEFAULT_WINDOW_BYTES);
        mock.put_object("big.csv", csv.as_bytes());
        let _env = mock_env(&mock);
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig::default()));

        let got = rows(
            &mut db,
            "SELECT COUNT(*), SUM(id) FROM read_csv('s3://bucket/big.csv');",
        );
        assert_eq!(
            got,
            vec![vec![Value::Bigint(800_000), Value::Bigint(sum as i64)]]
        );

        let spans = spans(&mock.ranges());
        // The probe is the first window; the rest follows in chunks. Every
        // byte of the file is fetched exactly once, in order.
        assert_eq!(
            spans.first(),
            Some(&(0, crate::prefetch::DEFAULT_WINDOW_BYTES - 1))
        );
        assert_eq!(
            spans.last().map(|(_, end)| *end + 1),
            Some(csv.len() as u64)
        );
        for pair in spans.windows(2) {
            assert_eq!(pair[0].1 + 1, pair[1].0, "a gap or overlap: {spans:?}");
        }
        assert!(
            spans.len() as u64
                <= (csv.len() as u64).div_ceil(crate::prefetch::DEFAULT_CHUNK_BYTES) + 1,
            "{} requests for {} bytes",
            spans.len(),
            csv.len()
        );
    }

    /// With the external cache on, a second statement over the same file issues
    /// no ranged GETs at all: every window is a cache hit.
    /// A transient error mid-file is retried by the transport, and the
    /// read-ahead resumes at the same offset, so a large file still parses
    /// exactly once and completely.
    #[cfg(feature = "s3")]
    #[test]
    fn a_transient_error_mid_file_is_retried() {
        let _guard = global_test_lock();
        let mock = crate::mock_s3::MockS3::start();
        let (csv, sum) = big_csv(800_000);
        mock.put_object("flaky.csv", csv.as_bytes());
        // The first range GET after the probe fails once, mid-file.
        mock.inject_error("GET", "flaky.csv", 503, "SlowDown", "please retry", 1);
        let _env = mock_env(&mock);
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig::default()));

        let got = rows(
            &mut db,
            "SELECT COUNT(*), SUM(id) FROM read_csv('s3://bucket/flaky.csv');",
        );
        assert_eq!(
            got,
            vec![vec![Value::Bigint(800_000), Value::Bigint(sum as i64)]],
            "the retried scan lost or duplicated rows"
        );
        let mut spans = spans(&mock.ranges());
        let requests = spans.len();
        spans.sort_unstable();
        spans.dedup();
        assert_eq!(
            spans.last().map(|(_, end)| *end + 1),
            Some(csv.len() as u64)
        );
        for pair in spans.windows(2) {
            assert_eq!(pair[0].1 + 1, pair[1].0, "a gap or overlap: {spans:?}");
        }
        assert!(
            requests > spans.len(),
            "the injected failure was not retried: {} requests for {} ranges",
            requests,
            spans.len()
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn the_external_cache_serves_a_second_scan_without_requests() {
        let _guard = global_test_lock();
        let mock = crate::mock_s3::MockS3::start();
        let (csv, sum) = big_csv(800_000);
        mock.put_object("big.csv", csv.as_bytes());
        let _env = mock_env(&mock);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-csv-cache-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = HttpfsConfig {
            allow_http: false,
            cache: Some(crate::cache::CacheOptions {
                dir: dir.clone(),
                capacity: 64 << 20,
            }),
        };
        let mut db = PinkDb::new();
        db.register_table_source(source_with(config));
        let sql = "SELECT COUNT(*), SUM(id) FROM read_csv('s3://bucket/big.csv');";
        assert_eq!(
            rows(&mut db, sql),
            vec![vec![Value::Bigint(800_000), Value::Bigint(sum as i64)]]
        );
        // Only ranged GETs count: the size HEAD carries no Range.
        let after_first = spans(&mock.ranges()).len();
        assert!(after_first > 1, "{:?}", mock.ranges());
        assert_eq!(
            rows(&mut db, sql),
            vec![vec![Value::Bigint(800_000), Value::Bigint(sum as i64)]]
        );
        assert_eq!(
            spans(&mock.ranges()).len(),
            after_first,
            "the cached windows were fetched again"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepared_statements_re_execute_the_scan() {
        let dir = scratch_dir("prepared");
        let file = dir.join("p.csv");
        std::fs::write(&file, "a\n1\n2\n3\n").unwrap();
        let mut db = PinkDb::new();
        db.register_table_source(source_with(HttpfsConfig::default()));
        let stmt = db
            .prepare(&format!(
                "SELECT COUNT(*), SUM(a) FROM read_csv('{}') WHERE a > ?;",
                file.display()
            ))
            .unwrap();
        for (param, count, sum) in [(1, 2i64, 5i64), (2, 1, 3)] {
            let out = db
                .execute_prepared(&stmt, &[Value::Integer(param)])
                .unwrap();
            match out {
                QueryResult::Selected { rows, .. } => {
                    assert_eq!(rows, vec![vec![Value::Bigint(count), Value::Bigint(sum)]]);
                }
                other => panic!("expected rows, got {other:?}"),
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn durable_stores_replay_csv_reads_by_re_reading() {
        let _guard = global_test_lock();
        let dir = scratch_dir("durable");
        let file = dir.join("src.csv");
        let store = dir.join("store.pink");
        std::fs::write(&file, "a\n1\n2\n").unwrap();
        // Recovery replays inside open_durable, so the source must be global
        // before the store is constructed.
        ensure_registered_with(HttpfsConfig::default());
        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE dst (a INTEGER);").unwrap();
        db.execute(&format!(
            "INSERT INTO dst SELECT a FROM read_csv('{}');",
            file.display()
        ))
        .unwrap();
        assert_eq!(
            rows(&mut db, "SELECT a FROM dst ORDER BY a;"),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
        drop(db);

        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        assert_eq!(
            rows(&mut db, "SELECT a FROM dst ORDER BY a;"),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
        drop(db);

        // A vanished file is a hard recovery error, never a silent drop.
        std::fs::remove_file(&file).unwrap();
        let err = PinkDb::open_durable(store.to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("src.csv") || err.contains("cannot open"),
            "{err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
