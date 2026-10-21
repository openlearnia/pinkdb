//! pinkdb's **parquet** reader: `read_parquet` / `parquet_scan`, DuckDB's
//! replacement scan (`FROM 'file.parquet'`), and the four metadata functions
//! (`parquet_schema`, `parquet_metadata`, `parquet_file_metadata`,
//! `parquet_kv_metadata`). Wave 70 of the roadmap. Wave 87 added the
//! write half ([`write::write_table`]) that backs the lake's Parquet format
//! provider; the SQL `COPY … (FORMAT parquet)` surface is still a follow-up.
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
//!   every row-group/column-chunk read are **byte-range GETs**; a whole-object
//!   GET happens only when the backend reports no size (HEAD without
//!   `Content-Length`, 405/501), in which case the object is fetched once and
//!   reused. Credentials/envs are the lake's (`AWS_*` / `PINKDB_S3_*`).
//!
//! # Honest limits
//!
//! - No SQL `COPY … (FORMAT parquet)` yet (the writer behind it exists in
//!   [`write`]; the lake consumes that directly).
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
//!   predicates, no parallel column decode.
//! - Remote: one object per call, no globs/directories, no HTTP auth beyond
//!   the transport (no custom CA), and no ranged `HEAD` cache across
//!   statements (bind and scan each read the footer again, cheaply).

mod file_meta;
mod glob;
mod hive;
mod introspect;
mod parse;
mod remote;
mod resolve;
mod scan;
mod schema_map;
pub mod write;

use std::sync::Arc;

use pinkdb::attached::PrunePredicate;
use pinkdb::{DataType, PinkDb, Result, TableFunctionCall, TableScanOutput, TableSource};

pub use file_meta::{ChunkInfo, ColStats, FileMeta, RowGroupMeta};
pub use resolve::{Call, Options, OutKind, Resolved};
pub use schema_map::FileColumn;

/// The registry name of the parquet source (`EXPLAIN` shows it; the CLI's
/// `.parquet` prints its counters).
pub const SOURCE_NAME: &str = "parquet";

/// The host's registration handle: `db.register_table_source(pinkdb_parquet::source())`.
pub fn source() -> Arc<dyn TableSource> {
    Arc::new(ParquetSource::new())
}

/// Register the parquet source on one store (embedder path). Idempotent.
pub fn register(db: &mut PinkDb) {
    db.register_table_source(source());
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
}

impl ParquetSource {
    pub fn new() -> Self {
        Self::default()
    }

    fn parse(&self, call: &TableFunctionCall) -> Result<Option<Call>> {
        resolve::parse_call(call)
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
                scan::scan(&resolved, projection, filters)?
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
        self.scans
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let rows: u64 = out.chunks.iter().map(|c| c.num_rows() as u64).sum();
        self.rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
        self.files
            .fetch_add(out.files, std::sync::atomic::Ordering::Relaxed);
        self.row_groups
            .fetch_add(out.row_groups, std::sync::atomic::Ordering::Relaxed);
        self.pruned
            .fetch_add(out.row_groups_pruned, std::sync::atomic::Ordering::Relaxed);
        self.columns
            .fetch_add(out.columns_decoded, std::sync::atomic::Ordering::Relaxed);
        Ok(out)
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
    }
}
