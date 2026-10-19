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
//! # Semantics (wave 75 base surface)
//!
//! The dialect is the engine's existing importer exactly: comma-separated,
//! `"` quoting with `""` escapes, one header row, UTF-8, empty/unquoted cells
//! as NULL, and the same whole-file type inference (INTEGER → BIGINT →
//! DOUBLE, BOOLEAN, DATE, TIMESTAMP, else VARCHAR). `read_csv` therefore
//! returns the same schema and values as `.import` for the same bytes; a
//! `read_csv` and an `.import` of one file agree by construction.
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
//! [`CsvOptions`] is the extension point wave 76 fills in. Today every
//! option is at its default (DuckDB's default dialect); a named argument is
//! recognised and refused with a precise "not supported yet" message rather
//! than silently ignored.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pinkdb::attached::PrunePredicate;
use pinkdb::csv::{infer_csv_schema, parse_csv_chunks};
use pinkdb::{
    DataChunk, DataType, PinkDb, PinkDbError, Result, TableFunctionArg, TableFunctionCall,
    TableScanOutput, TableSource, TableSourceStats, Vector,
};

use crate::{Fetcher, HttpfsConfig, open_with};

/// The registry name (`EXPLAIN` shows it; counters are attributed to it).
pub const SOURCE_NAME: &str = "httpfs";

/// Text encodings wave 76 will add. UTF-8 is the only implemented one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    #[default]
    Utf8,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Utf8 => "utf-8",
        }
    }
}

/// CSV dialect and import options. Wave 75 pins the defaults; wave 76 adds
/// the delimiter/quote/encoding/header/nullstr surface additively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvOptions {
    /// Field separator (default `,`).
    pub delimiter: u8,
    /// Quote byte (default `"`).
    pub quote: u8,
    /// Escape byte inside a quoted field (`None` = doubled quote).
    pub escape: Option<u8>,
    /// Whether the first row is the header (default `true`).
    pub header: bool,
    /// Text encoding (default UTF-8).
    pub encoding: Encoding,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            escape: None,
            header: true,
            encoding: Encoding::Utf8,
        }
    }
}

/// One parsed `read_csv` / replacement-scan call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The local path or remote URL, as written.
    pub path: String,
    pub options: CsvOptions,
}

impl Call {
    fn cache_key(&self) -> String {
        format!(
            "{}|{}|{}|{:?}|{}|{}",
            self.path,
            self.options.delimiter,
            self.options.quote,
            self.options.escape,
            self.options.header,
            self.options.encoding.name()
        )
    }
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
        return path_part.ends_with(".csv");
    }
    if lower.contains("://") {
        return false;
    }
    lower.ends_with(".csv")
}

/// Parse a table-function call into a [`Call`], or `None` when this source
/// does not serve it. `read_csv` is matched case-insensitively; the
/// replacement scan follows [`claims_path`].
pub fn parse_call(call: &TableFunctionCall) -> Result<Option<Call>> {
    match call {
        TableFunctionCall::Path(path) => {
            if claims_path(path) {
                Ok(Some(Call {
                    path: path.clone(),
                    options: CsvOptions::default(),
                }))
            } else {
                Ok(None)
            }
        }
        TableFunctionCall::Function { name, args } if name.eq_ignore_ascii_case("read_csv") => {
            let path = positional_path(args)?;
            let options = parse_options(args)?;
            Ok(Some(Call { path, options }))
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

/// The option-parsing shape wave 76 extends. All wave-76 names are
/// recognised and refused precisely (never ignored).
fn parse_options(args: &[TableFunctionArg]) -> Result<CsvOptions> {
    let options = CsvOptions::default();
    for arg in args {
        let Some(name) = arg.name_lower() else {
            continue;
        };
        match name.as_str() {
            "delimiter" | "sep" | "quote" | "escape" | "encoding" | "header" | "auto_detect"
            | "all_varchar" | "nullstr" | "names" | "columns" | "types" | "sample_size"
            | "skip" | "ignore_errors" => {
                return Err(PinkDbError::Catalog(format!(
                    "read_csv option `{name}` is not supported yet: wave 75 reads DuckDB's \
                     default CSV dialect (comma-delimited, double-quote quoting, one header \
                     row, UTF-8); the option surface is the next file-import wave"
                )));
            }
            other => {
                return Err(PinkDbError::Catalog(format!(
                    "unknown read_csv option `{other}`"
                )));
            }
        }
    }
    Ok(options)
}

fn is_remote(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("https://") || lower.starts_with("http://") || lower.starts_with("s3://")
}

/// One CSV file's inferred output schema.
pub type CsvSchema = Vec<(String, DataType)>;

#[derive(Debug)]
struct Cached {
    key: String,
    bytes: Vec<u8>,
    schema: CsvSchema,
}

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

    fn load(&self, call: &Call) -> Result<(Vec<u8>, CsvSchema)> {
        let bytes = if is_remote(&call.path) {
            let fetcher: Box<dyn Fetcher> = open_with(&call.path, &self.config).map_err(|e| {
                PinkDbError::Execution(format!("cannot open \"{}\": {e}", call.path))
            })?;
            fetcher.fetch().map_err(|e| {
                PinkDbError::Execution(format!("cannot read \"{}\": {e}", call.path))
            })?
        } else {
            std::fs::read(&call.path).map_err(|e| {
                PinkDbError::Execution(format!("cannot read CSV \"{}\": {e}", call.path))
            })?
        };
        let schema = infer_csv_schema(&bytes)?;
        Ok((bytes, schema))
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
        let (bytes, schema) = self.load(&call)?;
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *cache = Some(Cached {
            key: call.cache_key(),
            bytes,
            schema: schema.clone(),
        });
        Ok(Some(schema))
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
        let key = call.cache_key();
        let cached = {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match cache.as_ref() {
                Some(entry) if entry.key == key => cache.take(),
                _ => None,
            }
        };
        let (bytes, schema) = match cached {
            Some(entry) => (entry.bytes, entry.schema),
            None => self.load(&call)?,
        };
        let types: Vec<DataType> = schema.iter().map(|(_, t)| *t).collect();
        let chunks = parse_csv_chunks(&bytes, &types)?;

        let selected: Vec<bool> = match projection {
            Some(projection) => (0..types.len()).map(|i| projection.contains(&i)).collect(),
            None => vec![true; types.len()],
        };
        let all = selected.iter().all(|s| *s);
        let mut out = Vec::with_capacity(chunks.len());
        let mut rows = 0u64;
        for chunk in chunks {
            rows += chunk.num_rows as u64;
            if all {
                out.push(chunk);
            } else {
                let columns: Vec<Arc<Vector>> = (0..types.len())
                    .map(|i| {
                        if selected[i] {
                            chunk.columns[i].clone()
                        } else {
                            Arc::new(Vector::with_capacity(types[i], 0))
                        }
                    })
                    .collect();
                out.push(DataChunk::projected(columns, chunk.num_rows));
            }
        }
        let decoded = selected.iter().filter(|s| **s).count() as u64;
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.files.fetch_add(1, Ordering::Relaxed);
        self.columns.fetch_add(decoded, Ordering::Relaxed);
        Ok(TableScanOutput {
            chunks: out,
            files: 1,
            row_groups: 0,
            row_groups_pruned: 0,
            columns_decoded: decoded,
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
        assert!(call.options.header);

        let bad = TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(vec![
                ("", Value::Varchar("x.csv".into())),
                ("delimiter", Value::Varchar(";".into())),
            ]),
        };
        let err = parse_call(&bad).unwrap_err().to_string();
        assert!(err.contains("delimiter"), "{err}");
        assert!(err.contains("not supported yet"), "{err}");

        let unknown = TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(vec![
                ("", Value::Varchar("x.csv".into())),
                ("bogus", Value::Integer(1)),
            ]),
        };
        let err = parse_call(&unknown).unwrap_err().to_string();
        assert!(err.contains("unknown read_csv option `bogus`"), "{err}");

        let two = TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(vec![
                ("", Value::Varchar("a.csv".into())),
                ("", Value::Varchar("b.csv".into())),
            ]),
        };
        let err = parse_call(&two).unwrap_err().to_string();
        assert!(err.contains("exactly one path"), "{err}");

        let not_string = TableFunctionCall::Function {
            name: "read_csv".to_string(),
            args: args(vec![("", Value::Integer(1))]),
        };
        let err = parse_call(&not_string).unwrap_err().to_string();
        assert!(err.contains("string path"), "{err}");

        // Another source's function is not claimed.
        let other = TableFunctionCall::Function {
            name: "read_parquet".to_string(),
            args: args(vec![("", Value::Varchar("x.parquet".into()))]),
        };
        assert!(parse_call(&other).unwrap().is_none());
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
        db.register_table_source(source_with(HttpfsConfig { allow_http: true }));
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
        db.register_table_source(source_with(HttpfsConfig { allow_http: true }));
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

        ensure_registered_with(HttpfsConfig { allow_http: true });
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
    #[cfg(feature = "s3")]
    #[test]
    fn remote_s3_scan_reads_through_the_shared_client() {
        let _guard = global_test_lock();
        struct EnvGuard;
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                // SAFETY: process-global environment mutation is serialized
                // by GLOBAL_TEST_LOCK; see the test body.
                unsafe {
                    std::env::remove_var("AWS_ACCESS_KEY_ID");
                    std::env::remove_var("AWS_SECRET_ACCESS_KEY");
                    std::env::remove_var("AWS_REGION");
                    std::env::remove_var("AWS_ENDPOINT_URL_S3");
                    std::env::remove_var("PINKDB_S3_ALLOW_HTTP");
                }
            }
        }
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("data.csv", b"a,b\n1,x\n2,y\n");
        // SAFETY: the global test lock serializes every env-mutating test in
        // this binary; no other test reads AWS_* concurrently.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
            std::env::set_var("AWS_REGION", "auto");
            std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
            std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
        }
        let _env = EnvGuard;

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
