//! pinkdb's **parquet** reader: `read_parquet` / `parquet_scan`, DuckDB's
//! replacement scan (`FROM 'file.parquet'`), and the four metadata functions
//! (`parquet_schema`, `parquet_metadata`, `parquet_file_metadata`,
//! `parquet_kv_metadata`). Wave 70 of the roadmap. Wave 87 added the
//! write half ([`write::write_table`]) that backs the lake's Parquet format
//! provider; wave 98c added the SQL write surface,
//! `COPY t TO 'x.parquet' (FORMAT parquet)` ([`ParquetSink`]).
//!
//! # Why this is a crate, not a loadable extension
//!
//! The extension ABI (v1, `crates/pinkdb-ext-loader/include/pinkdb_extension.h`)
//! registers **scalar functions**: one callback per row over ABI values. A
//! file reader is a different shape entirely — it needs a schema at *bind*
//! time, answers with whole chunks, wants a projection and pushdown
//! predicates, and is addressed by a FROM clause (`read_parquet('x')`,
//! `FROM 'x.parquet'`, a glob). None of that is expressible in ABI v1, and
//! stretching it (one dynamic scalar per row, a path resolved per call) would
//! read the file once per row and lose every pushdown.
//!
//! The engine therefore grew **one additive surface** for host-registered
//! data sources ([`pinkdb::table_source::TableSource`], wave 70), the same
//! shape it already uses for attached lakes (`pinkdb::AttachedLake`): a trait
//! object the host registers on the store, resolved by the parser/binder for
//! two new FROM forms, executed as a leaf scan that receives the statement's
//! projection and the pushable `WHERE` conjuncts. This crate implements that
//! trait; the engine keeps no parquet dependency, and the CLI links this
//! crate only behind its opt-in `parquet` feature — the default binary is
//! byte-for-byte unaffected.
//!
//! # Using it
//!
//! ```no_run
//! # fn main() -> Result<(), pinkdb::PinkDbError> {
//! let mut db = pinkdb::PinkDb::new();
//! db.register_table_source(pinkdb_parquet::source());
//! let rows = db.execute("SELECT name FROM read_parquet('data/*.parquet') WHERE id > 10;")?;
//! # Ok(())
//! # }
//! ```
//!
//! Registered sources are process-level like extensions and attached lakes:
//! never persisted, gone after a reopen, and a **durable** statement that
//! would be WAL-logged may not read one (the logged SQL text could not replay
//! the external read) — `SELECT` stays fine, a `CREATE TABLE AS SELECT` from
//! a file in a durable session is refused by the engine.
//!
//! # Semantics, pinned against `duckdb` v2.0.0-alpha46057
//!
//! - **Paths**: a literal file, a directory (every `*.parquet` under it,
//!   recursively) or a glob (`*`, `?`, `[abc]`, `[a-z]`, `[!abc]`, `**`).
//!   The *replacement scan* (`FROM 'x'`) claims `.parquet`/`.parq`/`.pq`
//!   paths and extension-less existing directories only, so a registered
//!   arrow source keeps `.arrow`/`.ipc`/`.feather` (a `read_parquet(...)`
//!   call is unambiguous and always claimed). Matches are sorted
//!   lexicographically; the first file's schema is the
//!   reference without `union_by_name`. One path argument: α refuses
//!   `read_parquet('a', 'b')` as well, and pinkdb has no list type for α's
//!   `paths ANY[]` form — use a glob.
//! - **Options**: `filename`, `hive_partitioning` (default `auto`),
//!   `union_by_name`, `file_row_number`, `allow_empty`, `hive_types_autocast`,
//!   `binary_as_string`. Everything else α takes (`hive_types`, `schema`,
//!   `column_statistics`, `compression`, …) is refused by name.
//! - **Hive partitions**: `key=value` path components become columns, the
//!   union of keys across files in alphabetical order, appended after the
//!   file columns; values autotype to BIGINT/DATE only when they round-trip
//!   (`2024` → BIGINT, `01`/`1.5`/`true` → VARCHAR).
//! - **Union schema**: without `union_by_name` every file must match the
//!   first file's names and types (α's error shape); with it, columns match
//!   by name, missing ones read NULL, numeric types widen
//!   (INTEGER→BIGINT→DOUBLE, DECIMAL widths), DATE+TIMESTAMP → TIMESTAMP, and
//!   anything else falls back to VARCHAR with the values rendered as text —
//!   all α's rules.
//! - **Types**: see `schema_map`'s table. TIMESTAMP(nanos), TIME, UUID,
//!   FLOAT16, BSON and nested schemas are refused with a clear column-named
//!   error rather than silently re-typed.
//! - **Pushdown**: the projection decides which columns are decoded; min/max
//!   and NULL-count statistics prune whole row groups; a predicate over a
//!   hive/`filename` column prunes whole files. Pruning is an optimization
//!   only — the engine re-applies every predicate above the scan — so a
//!   column whose statistics are missing or in a different order (unsigned
//!   integers) simply prunes nothing. `pinkdb-cli`'s `.parquet` prints the
//!   counters (files read, row groups read/pruned, columns decoded).
//! - **Compression**: snappy (DuckDB's default) and gzip are built in —
//!   both pure Rust, so the crate's default feature set is wasm-clean
//!   (`cargo check -p pinkdb-parquet --target wasm32-unknown-unknown`).
//!   zstd/brotli/lz4 are cargo features; `zstd` compiles the C library
//!   through `zstd-sys` (it built for wasm here, but a C toolchain that can
//!   target wasm is not guaranteed everywhere).
//! - **Remote objects** (`remote` feature, enabled by the CLI's `httpfs`
//!   feature): `read_parquet('https://…/x.parquet')`,
//!   `read_parquet('s3://bucket/x.parquet')`, and the same paths in the
//!   replacement scan. Claims are by extension regardless of scheme
//!   (`.parquet`/`.parq`/`.pq`); plain `http://` is claimed but the shared
//!   transport refuses it unless `PINKDB_HTTPFS_ALLOW_HTTP=1`. Remote paths
//!   are single objects (globs/directories are refused). Reads go through
//!   parquet's `ChunkReader` over [`pinkdb_httpfs::Fetcher`]: the footer and
//!   every projected column-chunk byte range are **range GETs**; wave 98a
//!   coalesces each row group's projected chunks into one request per run and
//!   reads ahead with a bounded pool (`PINKDB_HTTPFS_PARALLELISM`, 0/1 =
//!   serial 128 KiB blocks). `COUNT(*)` needs no data page at all. A
//!   whole-object GET happens only when the backend reports no size (HEAD
//!   without `Content-Length`, 405/501), in which case the object is fetched
//!   once and reused. Credentials/envs are the lake's (`AWS_*` /
//!   `PINKDB_S3_*`); the source's `remote_stats()` reports requests, bytes
//!   and network wait per session.
//! - **A reusable ranged scan core (wave 98b)**: that machinery is
//!   transport-agnostic. [`RangeSource`] is a positional reader, and
//!   [`scan_source`] runs the same footer reads, coalescing, bounded
//!   read-ahead, row-group pruning and ordered assembly over any
//!   implementation. pink-lake's Parquet provider uses it to read lake data
//!   files (`s3://` or local) with projection pushdown, so the lake and
//!   remote-parquet paths cannot drift.
//!
//! # Honest limits
//!
//! - No `COPY … FROM 'x.parquet'` (reading a file is `read_parquet`/`FROM
//!   'x.parquet'`); writing is `COPY t TO 'x.parquet' (FORMAT parquet)`.
//! - Flat schemas only (no LIST/MAP/STRUCT, no repeated fields).
//! - No TIME, TIMESTAMP(nanos), UUID, FLOAT16, BSON, VARIANT, GEOMETRY.
//! - Unannotated BINARY reads as VARCHAR (α's `binary_as_string=true`);
//!   invalid UTF-8 fails the read rather than becoming a BLOB.
//! - `filename`/row order follow the sorted file list; α's ordering matches.
//! - The footer is parsed once to bind and the resolved scan is cached for
//!   the statement's execution (a second `read_parquet` call re-resolves).
//! - No bloom-filter, page-index or dictionary-based pruning: row-group
//!   min/max/NULL-count statistics only.
//! - No `hive_types`/`schema` overrides, no pushdown of `LIKE`/`OR`
//!   predicates, no parallel column decode (row groups decode in parallel on
//!   the remote read-ahead path, but columns inside one row group are
//!   decoded one at a time).
//! - Remote: one object per call, no globs/directories, no HTTP auth beyond
//!   the transport (no custom CA), and no ranged `HEAD` cache across
//!   statements (bind and scan each read the footer again, cheaply).

mod file_meta;
mod glob;
mod hive;
mod introspect;
mod parse;
mod ranged;
mod remote;
mod resolve;
mod scan;
mod schema_map;
pub mod write;

use std::sync::Arc;

use pinkdb::attached::PrunePredicate;
use pinkdb::{
    DataChunk, DataTable, DataType, FileFormat, PinkDb, PinkDbError, Result, TableFunctionCall,
    TableScanOutput, TableSource,
};
use std::path::Path;

pub use file_meta::{ChunkInfo, ColStats, FileMeta, RowGroupMeta};
pub use ranged::{RangeSource, RemoteReadStats};
pub use resolve::{Call, Options, OutKind, Resolved};
pub use schema_map::FileColumn;

/// The registry name of the parquet source (`EXPLAIN` shows it; the CLI's
/// `.parquet` prints its counters).
pub const SOURCE_NAME: &str = "parquet";

/// The host's registration handle: `db.register_table_source(pinkdb_parquet::source())`.
pub fn source() -> Arc<dyn TableSource> {
    Arc::new(ParquetSource::new())
}

/// The parquet **write** sink: `COPY t TO 'x.parquet' (FORMAT parquet)`, and
/// the format an extension-less `.parquet`/`.parq`/`.pq` destination resolves
/// to. Writes through the same writer (and therefore the same Snappy +
/// dictionary settings) as a Parquet lake data file.
#[derive(Debug, Default, Clone, Copy)]
pub struct ParquetSink;

impl FileFormat for ParquetSink {
    fn name(&self) -> &str {
        SOURCE_NAME
    }

    fn extensions(&self) -> &[&str] {
        &["parquet", "parq", "pq"]
    }

    fn write(&self, path: &Path, table: &DataTable) -> Result<()> {
        let bytes = write::write_table(table).map_err(|e| {
            PinkDbError::Execution(format!("cannot write \"{}\": {e}", path.display()))
        })?;
        // M-3: never `File::create` at the client-named path (the helper
        // writes a 0600 temp sibling and renames it into place).
        pinkdb::write_atomic_private(path, |file| {
            use std::io::Write;
            file.write_all(&bytes).map_err(|e| {
                PinkDbError::Execution(format!("cannot write \"{}\": {e}", path.display()))
            })
        })
    }
}

/// The host's registration handle for the write half:
/// `db.register_file_format(pinkdb_parquet::sink())`.
pub fn sink() -> Arc<dyn FileFormat> {
    Arc::new(ParquetSink)
}

/// Register the parquet source and sink on one store (embedder path).
/// Idempotent.
pub fn register(db: &mut PinkDb) {
    db.register_table_source(source());
    db.register_file_format(sink());
}

/// Register the parquet source **process-wide**, for every store created
/// afterwards — the host-bootstrap step a durable store needs: recovery
/// replays inside [`PinkDb::open_durable`], before the caller holds the
/// store, so a WAL record that reads a file (`CREATE TABLE t AS SELECT …
/// FROM read_parquet('f')`) can only resolve if the source was registered
/// *before* the store was constructed. The CLI (built with the `parquet`
/// feature) calls this once at startup; an embedder that opens stores before
/// calling it must use [`register`] per store instead. Idempotent.
pub fn ensure_registered() {
    pinkdb::table_source::register_global_table_source(source());
    pinkdb::filefmt::register_global_file_format(sink());
}

/// The result of one ranged parquet scan: the engine's chunks/counters, the
/// file metadata the provider needs for its table identity, and the
/// transport totals (wave 98b).
#[derive(Debug)]
pub struct RangedScan {
    pub output: TableScanOutput,
    /// The footer metadata (identity KV pairs, schema, row groups).
    pub meta: FileMeta,
    /// Requests, bytes and wait time this scan spent.
    pub stats: RemoteReadStats,
}

/// Scan one parquet object through a positional [`RangeSource`] — the lake's
/// projected/ranged read path (wave 98b). The same coalescing, bounded
/// read-ahead, row-group pruning and ordered assembly as a remote
/// `read_parquet` apply; `projection` selects output columns and `filters`
/// prune row groups.
pub fn scan_source(
    path: &str,
    source: Arc<dyn RangeSource>,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
) -> Result<RangedScan> {
    let resolved = resolve::resolve_source(path, source)?;
    let (output, stats) = scan::scan(&resolved, projection, filters)?;
    let meta = resolved
        .files
        .into_iter()
        .next()
        .map(|file| file.meta)
        .ok_or_else(|| PinkDbError::Execution("the ranged parquet scan resolved no file".into()))?;
    Ok(RangedScan {
        output,
        meta,
        stats,
    })
}

/// Read one object's parquet metadata (footer only, no data pages) through a
/// positional [`RangeSource`]. This is the lake provider's footer read.
pub fn read_meta_source(path: &str, source: Arc<dyn RangeSource>) -> Result<FileMeta> {
    let spec = ranged::RemoteSpec::from_source(source);
    file_meta::read_file_meta_source(path, &spec)
}

/// Read one local parquet file's metadata (footer only, no data pages).
pub fn read_meta_path(path: &str) -> Result<FileMeta> {
    file_meta::read_file_meta(path)
}

/// The parquet [`TableSource`]: one instance per store, holding the
/// statement's resolved-scan cache and its own counters.
#[derive(Debug, Default)]
pub struct ParquetSource {
    cache: resolve::ResolutionCache,
    scans: std::sync::atomic::AtomicU64,
    rows: std::sync::atomic::AtomicU64,
    files: std::sync::atomic::AtomicU64,
    row_groups: std::sync::atomic::AtomicU64,
    pruned: std::sync::atomic::AtomicU64,
    columns: std::sync::atomic::AtomicU64,
    /// Remote transport counters (wave 98a): requests, bytes received and
    /// wall time spent in transport calls, summed over this source's scans.
    remote_requests: std::sync::atomic::AtomicU64,
    remote_bytes: std::sync::atomic::AtomicU64,
    remote_wait_nanos: std::sync::atomic::AtomicU64,
}

impl ParquetSource {
    pub fn new() -> Self {
        Self::default()
    }

    fn parse(&self, call: &TableFunctionCall) -> Result<Option<Call>> {
        resolve::parse_call(call)
    }

    /// The remote transport counters accumulated since the last reset (wave
    /// 98a). Local scans contribute nothing. This is per-source state, so it
    /// is also where `.parquet`-style tooling reads request/byte/wait totals.
    pub fn remote_stats(&self) -> RemoteReadStats {
        use std::sync::atomic::Ordering::Relaxed;
        RemoteReadStats {
            requests: self.remote_requests.load(Relaxed),
            bytes: self.remote_bytes.load(Relaxed),
            wait_nanos: self.remote_wait_nanos.load(Relaxed),
        }
    }

    /// The resolved scan for `call`, from the cache when `bind` just put it
    /// there, else freshly resolved (a scan without a bind).
    fn resolved(
        &self,
        call: &TableFunctionCall,
        options: &Options,
        path: &str,
    ) -> Result<Arc<Resolved>> {
        let key = resolve::cache_key(call);
        if let Some(hit) = self.cache.get(&key) {
            return Ok(hit);
        }
        let resolved = Arc::new(resolve::resolve(path, options)?);
        self.cache.put(key, Arc::clone(&resolved));
        Ok(resolved)
    }

    /// Book one scan's engine counters (the streaming scan passes the rows it
    /// pushed, since it keeps no chunks).
    fn record(&self, out: &TableScanOutput, rows: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.scans.fetch_add(1, Relaxed);
        self.rows.fetch_add(rows, Relaxed);
        self.files.fetch_add(out.files, Relaxed);
        self.row_groups.fetch_add(out.row_groups, Relaxed);
        self.pruned.fetch_add(out.row_groups_pruned, Relaxed);
        self.columns.fetch_add(out.columns_decoded, Relaxed);
    }

    /// Book a scan's remote transport counters.
    fn record_transport(&self, transport: &crate::ranged::RemoteReadStats) {
        use std::sync::atomic::Ordering::Relaxed;
        self.remote_requests.fetch_add(transport.requests, Relaxed);
        self.remote_bytes.fetch_add(transport.bytes, Relaxed);
        self.remote_wait_nanos
            .fetch_add(transport.wait_nanos, Relaxed);
    }
}

impl TableSource for ParquetSource {
    fn name(&self) -> &str {
        SOURCE_NAME
    }

    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
        let Some(parsed) = self.parse(call)? else {
            return Ok(None);
        };
        let schema = match parsed {
            Call::Scan { path, options } => {
                let resolved = Arc::new(resolve::resolve(&path, &options)?);
                let schema = resolved.schema.clone();
                self.cache.put(resolve::cache_key(call), resolved);
                schema
            }
            Call::Metadata { function, path } => {
                // Validate the path eagerly: α fails at bind time for a
                // missing file, and so does this.
                let _ = introspect::run(&function, &path)?;
                introspect::bind(&function)
            }
        };
        Ok(Some(schema))
    }

    fn scan(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        filters: &[PrunePredicate],
    ) -> Result<TableScanOutput> {
        let Some(parsed) = self.parse(call)? else {
            return Err(pinkdb::PinkDbError::Execution(format!(
                "the parquet source does not serve {call:?}"
            )));
        };
        let out = match parsed {
            Call::Scan { path, options } => {
                let resolved = self.resolved(call, &options, &path)?;
                let (out, transport) = scan::scan(&resolved, projection, filters)?;
                self.record_transport(&transport);
                out
            }
            Call::Metadata { function, path } => {
                let chunks = introspect::run(&function, &path)?;
                TableScanOutput {
                    chunks,
                    files: 0,
                    row_groups: 0,
                    row_groups_pruned: 0,
                    columns_decoded: 0,
                }
            }
        };
        // Counters: the source's own, read through `.parquet`/the engine API.
        let rows: u64 = out.chunks.iter().map(|c| c.num_rows() as u64).sum();
        self.record(&out, rows);
        Ok(out)
    }

    fn scan_stream(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        filters: &[PrunePredicate],
        sink: &mut dyn FnMut(DataChunk) -> Result<()>,
    ) -> Result<TableScanOutput> {
        let Some(parsed) = self.parse(call)? else {
            return Err(pinkdb::PinkDbError::Execution(format!(
                "the parquet source does not serve {call:?}"
            )));
        };
        let mut rows = 0u64;
        let out = {
            // The sink owns nothing: each chunk is folded by the caller and
            // dropped before the next row group decodes (wave 98c).
            let mut push = |chunk: DataChunk| {
                rows += chunk.num_rows() as u64;
                sink(chunk)
            };
            match parsed {
                Call::Scan { path, options } => {
                    let resolved = self.resolved(call, &options, &path)?;
                    let (out, transport) =
                        scan::scan_streamed(&resolved, projection, filters, &mut push)?;
                    self.record_transport(&transport);
                    out
                }
                Call::Metadata { function, path } => {
                    for chunk in introspect::run(&function, &path)? {
                        push(chunk)?;
                    }
                    TableScanOutput::default()
                }
            }
        };
        self.record(&out, rows);
        Ok(out)
    }

    fn streams_scan(&self, call: &TableFunctionCall) -> bool {
        // A remote object would land in memory whole; a local file is decoded
        // once either way and folded in parallel by the materialized path,
        // which is measurably faster than the serial fold (wave 98c).
        let Ok(Some(Call::Scan { path, options })) = self.parse(call) else {
            return false;
        };
        self.resolved(call, &options, &path)
            .is_ok_and(|resolved| resolved.files.iter().any(|file| file.remote.is_some()))
    }

    fn stats(&self) -> pinkdb::TableSourceStats {
        use std::sync::atomic::Ordering::Relaxed;
        pinkdb::TableSourceStats {
            name: SOURCE_NAME.to_string(),
            scans: self.scans.load(Relaxed),
            rows: self.rows.load(Relaxed),
            files: self.files.load(Relaxed),
            row_groups: self.row_groups.load(Relaxed),
            row_groups_pruned: self.pruned.load(Relaxed),
            columns_decoded: self.columns.load(Relaxed),
        }
    }

    fn reset_stats(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        self.scans.store(0, Relaxed);
        self.rows.store(0, Relaxed);
        self.files.store(0, Relaxed);
        self.row_groups.store(0, Relaxed);
        self.pruned.store(0, Relaxed);
        self.columns.store(0, Relaxed);
        self.remote_requests.store(0, Relaxed);
        self.remote_bytes.store(0, Relaxed);
        self.remote_wait_nanos.store(0, Relaxed);
    }
}
