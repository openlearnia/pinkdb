//! In-memory columnar storage + catalog.
//!
//! `DataTable` = one [`Vector`] chunk list per column (each ≤ [`CHUNK_SIZE`]
//! rows). [`PinkDb`] = map of table name → table. Single connection, single
//! thread. No persistence in v1.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(not(target_arch = "wasm32"))]
use pinkdb_ext_loader::{DynamicFunction, LoadedExtension};

use crate::crypto::EncryptionKey;
use crate::sql::ast::Select;
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};
use crate::wal::Wal;

/// Next store-unique table version stamp (see [`DataTable::version`]).
fn next_table_version() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// One column: its schema + its chunk list.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
    /// Reference-counted chunks: scans share storage instead of cloning
    /// megabytes per query. Mutations use copy-on-write (`Arc::make_mut`),
    /// which never actually clones — the executor never holds chunks
    /// across a `&mut` borrow.
    pub chunks: Vec<Arc<Vector>>,
}

/// Chunks above which a dropped column releases them in parallel.
const PARALLEL_DROP_CHUNKS: usize = 64;

impl Drop for Column {
    /// A large column frees one allocation per VARCHAR cell (plus every
    /// chunk buffer); dropping a 10M-row table serially took about as long
    /// as importing it. Release big columns' chunks on the rayon pool
    /// instead (chunks still shared by a snapshot just lose a reference).
    fn drop(&mut self) {
        if self.chunks.len() >= PARALLEL_DROP_CHUNKS {
            use rayon::prelude::*;
            std::mem::take(&mut self.chunks)
                .into_par_iter()
                .for_each(drop);
        }
    }
}

impl Column {
    fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            chunks: vec![Arc::new(Vector::new_empty(dtype))],
        }
    }
}

/// One table: a set of same-length columns.
#[derive(Debug, Clone)]
pub struct DataTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub num_rows: usize,
    /// Mutation stamp for optimistic-concurrency conflict detection. Drawn
    /// from a process-wide counter on creation and on every
    /// append/update/delete batch, so it is unique across tables: a table
    /// dropped and recreated under the same name never repeats an earlier
    /// stamp (a transaction based on the old table must conflict).
    /// Process-lifetime only (not persisted — save/load draws fresh stamps).
    pub version: u64,
}

impl DataTable {
    pub fn new(name: String, schema: Vec<(String, DataType)>) -> Result<Self> {
        if schema.is_empty() {
            return Err(PinkDbError::Catalog(
                "cannot create table with no columns".to_string(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for (col, _) in &schema {
            let key = col.to_ascii_lowercase();
            if !seen.insert(key) {
                return Err(PinkDbError::Catalog(format!(
                    "duplicate column name \"{col}\""
                )));
            }
        }
        Ok(Self {
            name,
            columns: schema
                .into_iter()
                .map(|(name, dtype)| Column::new(name, dtype))
                .collect(),
            num_rows: 0,
            version: next_table_version(),
        })
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Build a table directly from pre-built columns: every column must have
    /// the same per-chunk lengths, summing to `num_rows` (the projection-aware
    /// lake reader assembles partially materialized tables without going
    /// through `append`). Assigns a fresh version stamp like any constructor.
    pub fn from_columns(name: String, columns: Vec<Column>, num_rows: usize) -> Self {
        debug_assert!(
            !columns.is_empty(),
            "a table has at least one column (schemas are never empty)"
        );
        debug_assert!(
            {
                let chunks = columns[0].chunks.len();
                columns.iter().all(|c| {
                    c.chunks.len() == chunks
                        && c.chunks
                            .iter()
                            .zip(&columns[0].chunks)
                            .all(|(a, b)| a.len() == b.len())
                })
            },
            "from_columns needs chunk-aligned columns"
        );
        Self {
            name,
            columns,
            num_rows,
            version: next_table_version(),
        }
    }

    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    pub fn schema(&self) -> Vec<(String, DataType)> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.dtype))
            .collect()
    }

    /// Case-insensitive column lookup (SQL identifiers are case-insensitive).
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                PinkDbError::Catalog(format!(
                    "table \"{}\" has no column \"{}\"",
                    self.name, name
                ))
            })
    }

    /// Widening coercion for INSERT/CSV: INTEGER → BIGINT → DOUBLE.
    /// NULL passes through for any target.
    pub(crate) fn coerce(value: Value, target: DataType) -> Result<Value> {
        match (value, target) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Boolean(b), DataType::Boolean) => Ok(Value::Boolean(b)),
            (Value::Integer(i), DataType::Integer) => Ok(Value::Integer(i)),
            (Value::Integer(i), DataType::Bigint) => Ok(Value::Bigint(i as i64)),
            (Value::Integer(i), DataType::Double) => Ok(Value::Double(i as f64)),
            (Value::Bigint(i), DataType::Bigint) => Ok(Value::Bigint(i)),
            (Value::Bigint(i), DataType::Double) => Ok(Value::Double(i as f64)),
            (Value::Double(x), DataType::Double) => Ok(Value::Double(x)),
            (Value::Varchar(s), DataType::Varchar) => Ok(Value::Varchar(s)),
            (Value::Date(d), DataType::Date) => Ok(Value::Date(d)),
            (Value::Timestamp(us), DataType::Timestamp) => Ok(Value::Timestamp(us)),
            (v, t) => Err(PinkDbError::Type(format!("cannot coerce {v} to {t}"))),
        }
    }

    /// Append one row, opening a fresh chunk in every column when the tail
    /// chunk is full.
    pub fn append_row(&mut self, values: Vec<Value>) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(PinkDbError::Execution(format!(
                "row has {} values but table \"{}\" has {} columns",
                values.len(),
                self.name,
                self.columns.len()
            )));
        }
        // Open a new tail chunk everywhere if the tail is full, so columns
        // stay aligned even at chunk boundaries.
        let tail_full = self.columns[0]
            .chunks
            .last()
            .map(|c| c.len() >= CHUNK_SIZE)
            .unwrap_or(true);
        if tail_full {
            for col in &mut self.columns {
                col.chunks
                    .push(Arc::new(Vector::with_capacity(col.dtype, CHUNK_SIZE)));
            }
        }
        for (col, val) in self.columns.iter_mut().zip(values) {
            let coerced = Self::coerce(val, col.dtype)?;
            Arc::make_mut(col.chunks.last_mut().expect("tail chunk exists")).push_value(coerced)?;
        }
        self.num_rows += 1;
        self.version = next_table_version();
        Ok(())
    }

    /// Append a pre-built batch of typed columns (all the same length ≤
    /// [`CHUNK_SIZE`], matching this table's schema). Bulk path for ingestion
    /// (CSV import): one call per batch instead of one `append_row` per row.
    /// Splits across chunk boundaries as needed; one version bump per batch.
    pub fn append_chunk(&mut self, chunk: Vec<Vector>) -> Result<()> {
        let chunk: Vec<Arc<Vector>> = chunk.into_iter().map(Arc::new).collect();
        self.append_shared_chunk(&chunk)
    }

    /// [`DataTable::append_chunk`] for already-shared columns: appends the
    /// caller's `Arc<Vector>`s (shared, never copied) where the chunking
    /// rules allow. Validation, values, chunk boundaries and version bumps
    /// are exactly those of `append_chunk`; the only difference is that the
    /// caller's allocation is shared instead of moved into a fresh `Arc`.
    /// The lake loader uses this to hand decoded chunks straight to the
    /// destination table without deep-cloning megabytes of payload.
    pub fn append_shared_chunk(&mut self, chunk: &[Arc<Vector>]) -> Result<()> {
        if chunk.len() != self.columns.len() {
            return Err(PinkDbError::Execution(format!(
                "chunk has {} columns but table \"{}\" has {}",
                chunk.len(),
                self.name,
                self.columns.len()
            )));
        }
        let rows = chunk.first().map(|c| c.len()).unwrap_or(0);
        if rows > CHUNK_SIZE {
            return Err(PinkDbError::Execution(format!(
                "chunk has {rows} rows but CHUNK_SIZE is {CHUNK_SIZE}"
            )));
        }
        for (dst, src) in self.columns.iter().zip(chunk) {
            if src.len() != rows {
                return Err(PinkDbError::Execution(
                    "chunk columns must all have the same length".to_string(),
                ));
            }
            if src.dtype() != dst.dtype {
                return Err(PinkDbError::Type(format!(
                    "cannot append a {:?} chunk into column \"{}\" ({:?})",
                    src.dtype(),
                    dst.name,
                    dst.dtype
                )));
            }
        }
        if rows == 0 {
            return Ok(());
        }
        // Fast paths (rows ≤ CHUNK_SIZE by the check above): the batch fills
        // an empty tail chunk, or starts a new chunk after a full one — share
        // the built vectors in without copying. The copy path below would
        // produce exactly the same chunking; bulk loaders (CSV import, lake
        // load) append CHUNK_SIZE-row batches, so after the first batch every
        // append lands behind a full tail and takes the second fast path.
        let tail_len = self.columns[0].chunks.last().map(|c| c.len()).unwrap_or(0);
        if tail_len == 0 || tail_len >= CHUNK_SIZE {
            if tail_len == 0 && !self.columns[0].chunks.is_empty() {
                for (col, src) in self.columns.iter_mut().zip(chunk) {
                    *col.chunks.last_mut().expect("tail chunk exists") = Arc::clone(src);
                }
            } else {
                for (col, src) in self.columns.iter_mut().zip(chunk) {
                    col.chunks.push(Arc::clone(src));
                }
            }
            self.num_rows += rows;
            self.version = next_table_version();
            return Ok(());
        }
        // Partial tail: merge, splitting at chunk boundaries as needed.
        let mut offset = 0;
        while offset < rows {
            let tail_full = self.columns[0]
                .chunks
                .last()
                .map(|c| c.len() >= CHUNK_SIZE)
                .unwrap_or(true);
            if tail_full {
                for col in &mut self.columns {
                    col.chunks
                        .push(Arc::new(Vector::with_capacity(col.dtype, CHUNK_SIZE)));
                }
            }
            let take = {
                let tail_len = self.columns[0].chunks.last().map(|c| c.len()).unwrap_or(0);
                (CHUNK_SIZE - tail_len).min(rows - offset)
            };
            for (col, src) in self.columns.iter_mut().zip(chunk) {
                Arc::make_mut(col.chunks.last_mut().expect("tail chunk exists"))
                    .extend_from(src, offset, take);
            }
            offset += take;
            self.num_rows += take;
        }
        self.version = next_table_version();
        Ok(())
    }

    /// Whether values of type `from` can coerce to `to` (type-level check for
    /// UPDATE validation; mirrors [`DataTable::coerce`]).
    pub fn can_coerce(from: DataType, to: DataType) -> bool {
        match (from, to) {
            (a, b) if a == b => true,
            (DataType::Integer, DataType::Bigint)
            | (DataType::Integer, DataType::Double)
            | (DataType::Bigint, DataType::Double) => true,
            _ => false,
        }
    }

    /// Overwrite cells in one stored chunk: `updates` holds
    /// `(column, row-in-chunk, value)` triples. Values are coerced to the
    /// column type first, so a failed coercion leaves the chunk untouched
    /// (validate everything, then write).
    pub fn update_rows(&mut self, chunk: usize, updates: &[(usize, usize, Value)]) -> Result<()> {
        if chunk >= self.num_chunks() {
            return Err(PinkDbError::Execution(format!(
                "chunk {chunk} out of range"
            )));
        }
        // Validate + coerce up front for atomicity.
        let mut staged: Vec<(usize, usize, Value)> = Vec::with_capacity(updates.len());
        for (col, row, v) in updates {
            let dtype = self
                .columns
                .get(*col)
                .ok_or_else(|| PinkDbError::Execution(format!("column {col} out of range")))?
                .dtype;
            if *row >= self.columns[*col].chunks[chunk].len() {
                return Err(PinkDbError::Execution(format!(
                    "row {row} out of bounds in chunk {chunk}"
                )));
            }
            staged.push((*col, *row, Self::coerce(v.clone(), dtype)?));
        }
        for (col, row, v) in staged {
            Arc::make_mut(&mut self.columns[col].chunks[chunk]).set_value(row, v)?;
        }
        if !updates.is_empty() {
            self.version = next_table_version();
        }
        Ok(())
    }

    /// Remove rows (chunk-relative positions) from one stored chunk.
    /// Returns the removed count. Emptied chunks are dropped.
    pub fn delete_from_chunk(&mut self, chunk: usize, drop: &[usize]) -> Result<usize> {
        if chunk >= self.num_chunks() {
            return Err(PinkDbError::Execution(format!(
                "chunk {chunk} out of range"
            )));
        }
        if drop.is_empty() {
            return Ok(0);
        }
        let len = self.columns[0].chunks[chunk].len();
        let mut drop_sorted = drop.to_vec();
        drop_sorted.sort_unstable();
        drop_sorted.dedup();
        if drop_sorted.iter().any(|&r| r >= len) {
            return Err(PinkDbError::Execution(format!(
                "row out of bounds in chunk {chunk}"
            )));
        }
        let mut keep: Vec<usize> = Vec::with_capacity(len - drop_sorted.len());
        let mut d = 0;
        for r in 0..len {
            if d < drop_sorted.len() && drop_sorted[d] == r {
                d += 1;
            } else {
                keep.push(r);
            }
        }
        let removed = len - keep.len();
        for col in &mut self.columns {
            col.chunks[chunk] = Arc::new(col.chunks[chunk].gather(&keep));
        }
        // Drop chunks left empty (all columns share chunk boundaries).
        if self.columns[0].chunks[chunk].is_empty() {
            for col in &mut self.columns {
                col.chunks.remove(chunk);
            }
        }
        self.num_rows -= removed;
        if removed > 0 {
            self.version = next_table_version();
        }
        Ok(removed)
    }

    /// Number of stored chunks (all columns share the same count).
    pub fn num_chunks(&self) -> usize {
        self.columns.first().map(|c| c.chunks.len()).unwrap_or(0)
    }

    /// Read chunk `idx` as a `DataChunk` (columns shared by reference —
    /// scans never copy data).
    pub fn scan_chunk(&self, idx: usize) -> Option<DataChunk> {
        if idx >= self.num_chunks() {
            return None;
        }
        let cols = self.columns.iter().map(|c| c.chunks[idx].clone()).collect();
        DataChunk::from_arcs(cols)
    }

    /// Value at (row, col).
    pub fn get(&self, row: usize, col: usize) -> Option<Value> {
        if row >= self.num_rows || col >= self.columns.len() {
            return None;
        }
        let mut base = row;
        for chunk in &self.columns[col].chunks {
            if base < chunk.len() {
                return Some(chunk.get(base));
            }
            base -= chunk.len();
        }
        None
    }
}

/// A view: a stored query (rebound at each use, so drops/recreates of the
/// underlying tables surface as errors, never stale plans). Views are
/// session-local: `.save` does not persist them.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewDef {
    pub name: String,
    pub query: Select,
    pub cols: Option<Vec<String>>,
}

/// One database's catalog: its tables and its session-local views.
#[derive(Debug, Default, Clone)]
pub struct DbCatalog {
    /// Display name as created (the map key is the lowercased name).
    pub(crate) name: String,
    pub(crate) tables: HashMap<String, DataTable>,
    pub(crate) views: HashMap<String, ViewDef>,
}

/// A store of databases.
///
/// Holds an optional transaction snapshot (`BEGIN` clones the whole store;
/// `ROLLBACK` restores it, `COMMIT` drops it). Single connection, so
/// transactions give multi-statement atomicity, not isolation.
///
/// Databases are named, case-insensitive namespaces (default `main`).
/// `current_db` is the *embedded session's* default database: unqualified
/// names resolve against it. [`crate::Connection`]s carry their own session
/// database and never mutate this one.
///
/// Durable databases (opened via [`PinkDb::open_durable`]) additionally carry
/// a write-ahead log: committed statements are appended as records, and
/// `COMMIT` flushes the transaction's statements as one record. Clones share
/// the WAL handle (`Arc`), so transaction snapshots and multi-writer
/// `txn.work` copies all log to the same file. In-memory databases have
/// `wal == None` and are byte-for-byte unchanged.
#[derive(Debug)]
pub struct PinkDb {
    /// Key = lowercased database name.
    databases: HashMap<String, DbCatalog>,
    /// Loadable-extension scalar functions (ABI v1, wave 29A), keyed by
    /// lowercased name. Registered by [`PinkDb::load_extension`]; shared by
    /// every clone of the store. Empty for a stock engine, in which case the
    /// binder/executor behave exactly as before. Native-only: wasm32 links no
    /// extension loader (`LOAD EXTENSION` errors there).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) dynamic_functions: HashMap<String, DynamicFunction>,
    /// Every successfully loaded extension (ABI v1), keyed by lowercased
    /// declared name. Holds the library handle, so an extension that
    /// registered no functions still stays mapped; also pins duplicate loads
    /// by name. Native-only, like [`PinkDb::dynamic_functions`].
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) loaded_extensions: HashMap<String, LoadedExtension>,
    /// Lowercased name of the embedded session's current database.
    pub(crate) current_db: String,
    pub(crate) txn_snapshot: Option<Box<PinkDb>>,
    /// Durable WAL handle, shared by every clone of this store.
    pub(crate) wal: Option<Arc<Mutex<Wal>>>,
    /// The per-operation write lease of a durable store (wave 45m): the lock
    /// file fd, the in-process gate, the busy timeout, and the epoch cache.
    /// Shared by every clone; `None` in memory.
    pub(crate) lease: Option<Arc<crate::wal::Lease>>,
    /// The lease held for the whole span of an embedded durable transaction
    /// (`begin` .. `commit`/`rollback`). Never cloned into snapshots.
    pub(crate) txn_lease: Option<crate::wal::LeaseGuard>,
    /// SQL text of mutating statements executed since `BEGIN` of a durable
    /// transaction; flushed as one WAL record by [`PinkDb::commit`].
    wal_txn: Vec<String>,
    /// Session database before the first statement of the open transaction's
    /// WAL record (the record replays in that session).
    wal_txn_db: Option<String>,
    /// Extension paths touched by the buffered statements (dynamic functions
    /// they call), deduped, in first-use order. A record whose replay needs an
    /// extension the current WAL generation has not loaded is prefixed with
    /// `LOAD EXTENSION '<path>'` at append time.
    wal_txn_ext_paths: Vec<String>,
    /// Extension paths loaded by the buffered statements' own
    /// `LOAD EXTENSION` text (empty in practice: `LOAD EXTENSION` is refused
    /// inside a transaction; kept so a future relaxation still logs a
    /// self-contained record).
    wal_txn_ext_loaded: Vec<String>,
    /// At-rest key this store was opened/configured with. Checkpoints and
    /// snapshots of a keyed store are written encrypted (feature
    /// `encryption`); [`PinkDb::save`] stays a plaintext explicit API.
    pub(crate) encryption_key: Option<EncryptionKey>,
    /// Whether the SQL `LOAD EXTENSION` statement may run (default `true`:
    /// the embedded API is trusted). Network front-ends turn it off so a
    /// remote client cannot make the host process `dlopen` a library. WAL
    /// replay is unaffected: it runs at open, before a front-end can flip
    /// this, and the Rust [`PinkDb::load_extension`] API is never gated.
    pub(crate) allow_sql_load_extension: bool,
    /// Attached read-only lakes (`.lake attach`): session-local, additive to
    /// the visible catalog for reads only — never part of the store's own
    /// tables and never touched by `.save`, `.lake save`, or the WAL.
    pub(crate) attached_lakes: Vec<Arc<dyn crate::attached::AttachedLake>>,
}

impl Default for PinkDb {
    fn default() -> Self {
        Self::with_databases(["main"])
    }
}

impl Clone for PinkDb {
    /// Clone the store for a transaction snapshot / multi-writer copy. The
    /// copy shares the WAL handle and the lease *handle* (`Arc`s) but never
    /// inherits `txn_lease`: a held lease belongs to exactly the store that
    /// opened it, so dropping a snapshot can never release another store's
    /// flock.
    fn clone(&self) -> Self {
        Self {
            databases: self.databases.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            dynamic_functions: self.dynamic_functions.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            loaded_extensions: self.loaded_extensions.clone(),
            current_db: self.current_db.clone(),
            txn_snapshot: self.txn_snapshot.clone(),
            wal: self.wal.clone(),
            lease: self.lease.clone(),
            txn_lease: None,
            wal_txn: self.wal_txn.clone(),
            wal_txn_db: self.wal_txn_db.clone(),
            wal_txn_ext_paths: self.wal_txn_ext_paths.clone(),
            wal_txn_ext_loaded: self.wal_txn_ext_loaded.clone(),
            encryption_key: self.encryption_key.clone(),
            allow_sql_load_extension: self.allow_sql_load_extension,
            attached_lakes: self.attached_lakes.clone(),
        }
    }
}

impl PinkDb {
    pub fn new() -> Self {
        Self::default()
    }

    /// A store with exactly the named databases (used by `open` for v2
    /// files, which may legitimately lack `main` after it was dropped).
    pub(crate) fn with_databases<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut databases = HashMap::new();
        for name in names {
            databases.insert(
                Self::db_key(name.as_ref()),
                DbCatalog {
                    name: name.as_ref().to_string(),
                    ..DbCatalog::default()
                },
            );
        }
        Self {
            databases,
            #[cfg(not(target_arch = "wasm32"))]
            dynamic_functions: HashMap::new(),
            #[cfg(not(target_arch = "wasm32"))]
            loaded_extensions: HashMap::new(),
            current_db: "main".to_string(),
            txn_snapshot: None,
            wal: None,
            lease: None,
            txn_lease: None,
            wal_txn: Vec::new(),
            wal_txn_db: None,
            wal_txn_ext_paths: Vec::new(),
            wal_txn_ext_loaded: Vec::new(),
            encryption_key: None,
            allow_sql_load_extension: true,
            attached_lakes: Vec::new(),
        }
    }

    /// Allow or refuse the SQL `LOAD EXTENSION` statement on this store
    /// (see the field docs; the Rust load API stays available either way).
    pub fn set_allow_sql_load_extension(&mut self, allow: bool) {
        self.allow_sql_load_extension = allow;
    }

    /// Whether the SQL `LOAD EXTENSION` statement may run on this store.
    pub fn allow_sql_load_extension(&self) -> bool {
        self.allow_sql_load_extension
    }

    /// Configure the at-rest key used by checkpoints and snapshots of this
    /// store (`None` = plaintext). The keyed APIs set this automatically;
    /// this setter exists for stores that are configured before a file is
    /// opened (e.g. the REPL reading `PINKDB_KEY` at startup). With the
    /// `encryption` feature off, a keyed save fails clearly instead of
    /// silently writing plaintext. Changing the key (or its presence) on a
    /// store that already has a live WAL is refused at the next
    /// [`PinkDb::checkpoint`]: the `.pink` and the WAL generation must stay
    /// in one mode and under one key, so a mode/key change needs a reopen.
    pub fn set_encryption_key(&mut self, key: Option<EncryptionKey>) {
        self.encryption_key = key;
    }

    /// The at-rest key this store writes with, if any.
    pub fn encryption_key(&self) -> Option<&EncryptionKey> {
        self.encryption_key.as_ref()
    }

    /// The embedded session's current database (lowercased).
    pub fn current_db(&self) -> &str {
        &self.current_db
    }

    /// Set the embedded session's current database without validation
    /// (used by `open`, which restores the file's databases exactly, and by
    /// the session layer after `USE`/`DROP DATABASE`).
    pub(crate) fn set_current_db(&mut self, name: &str) {
        self.current_db = name.to_string();
    }

    /// Sorted display names of every database.
    pub fn list_databases(&self) -> Vec<String> {
        let mut names: Vec<String> = self.databases.values().map(|c| c.name.clone()).collect();
        names.sort();
        names
    }

    pub fn database_exists(&self, name: &str) -> bool {
        self.databases.contains_key(&Self::db_key(name))
    }

    /// Create a new, empty database. Case-insensitive; errors if it exists.
    pub fn create_database(&mut self, name: &str) -> Result<()> {
        let key = Self::db_key(name);
        if self.databases.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" already exists"
            )));
        }
        self.databases.insert(
            key,
            DbCatalog {
                name: name.to_string(),
                ..DbCatalog::default()
            },
        );
        Ok(())
    }

    /// Remove a database and everything in it. The last remaining database
    /// cannot be dropped. The *session* switch after dropping the current
    /// database is the caller's job (see [`PinkDb::fallback_db`]).
    pub fn drop_database(&mut self, name: &str) -> Result<()> {
        let key = Self::db_key(name);
        if !self.databases.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" not found"
            )));
        }
        if self.databases.len() == 1 {
            return Err(PinkDbError::Catalog(format!(
                "cannot drop the last database \"{name}\""
            )));
        }
        // Dropping a database whose tables are attached would silently
        // orphan the attachment (its qualified names could no longer
        // resolve); detach first.
        if self
            .attached_tables()
            .iter()
            .any(|t| t.db.eq_ignore_ascii_case(name))
        {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" has attached lake tables; .lake detach the lake first"
            )));
        }
        self.databases.remove(&key);
        Ok(())
    }

    /// Switch the embedded session to `name` (validated).
    pub fn use_database(&mut self, name: &str) -> Result<()> {
        if !self.database_exists(name) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" not found"
            )));
        }
        self.current_db = Self::db_key(name);
        Ok(())
    }

    /// The database a session should fall back to after dropping its current
    /// one: `main` when it exists, else the first database in sorted order.
    pub fn fallback_db(&self) -> String {
        if self.databases.contains_key("main") {
            return "main".to_string();
        }
        let mut names: Vec<&String> = self.databases.keys().collect();
        names.sort();
        names
            .first()
            .map(|s| (*s).clone())
            .unwrap_or_else(|| "main".to_string())
    }

    fn db_key(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn catalog(&self, db: &str) -> Result<&DbCatalog> {
        self.databases
            .get(&Self::db_key(db))
            .ok_or_else(|| PinkDbError::Catalog(format!("database \"{db}\" not found")))
    }

    fn catalog_mut(&mut self, db: &str) -> Result<&mut DbCatalog> {
        self.databases
            .get_mut(&Self::db_key(db))
            .ok_or_else(|| PinkDbError::Catalog(format!("database \"{db}\" not found")))
    }

    /// Current version of one table (for conflict detection).
    pub fn table_version(&self, name: &str) -> Result<u64> {
        let db = self.current_db.clone();
        self.table_version_in(&db, name)
    }

    pub fn table_version_in(&self, db: &str, name: &str) -> Result<u64> {
        Ok(self.table_in(db, name)?.version)
    }

    pub(crate) fn tables_remove_in(&mut self, db: &str, key: &str) -> Option<DataTable> {
        self.databases
            .get_mut(&Self::db_key(db))?
            .tables
            .remove(key)
    }

    pub(crate) fn tables_insert_in(
        &mut self,
        db: &str,
        key: String,
        table: DataTable,
    ) -> Result<()> {
        self.catalog_mut(db)?.tables.insert(key, table);
        Ok(())
    }

    pub(crate) fn views_remove_in(&mut self, db: &str, key: &str) -> Option<ViewDef> {
        self.databases.get_mut(&Self::db_key(db))?.views.remove(key)
    }

    pub(crate) fn views_insert_in(&mut self, db: &str, key: String, view: ViewDef) -> Result<()> {
        self.catalog_mut(db)?.views.insert(key, view);
        Ok(())
    }

    /// True while a `BEGIN..COMMIT/ROLLBACK` block is open.
    pub fn in_transaction(&self) -> bool {
        self.txn_snapshot.is_some()
    }

    /// True when this database logs commits to a WAL.
    pub fn is_durable(&self) -> bool {
        self.wal.is_some()
    }

    /// Fail-stop after a WAL write failure. The statement whose append failed
    /// was already applied in memory but is not durable; refusing every later
    /// statement (reads included) and every serialization keeps that state
    /// from ever being observed or saved. The database must be reopened,
    /// which recovers exactly the durable prefix.
    pub(crate) fn ensure_wal_healthy(&self) -> Result<()> {
        if let Some(wal) = self.durable_handle() {
            let guard = wal.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(msg) = guard.failure() {
                return Err(PinkDbError::Execution(format!(
                    "the database stopped after a WAL write failure ({msg}); \
                     reopen it to recover the durable state"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn durable_handle(&self) -> Option<Arc<Mutex<Wal>>> {
        self.wal.clone()
    }

    /// The durable write lease of this store, if durable.
    pub(crate) fn lease_handle(&self) -> Option<Arc<crate::wal::Lease>> {
        self.lease.clone()
    }

    /// Swap the catalog and data of `self` with `other` (a checkpoint loaded
    /// from disk), preserving every process-local facet of `self`: the WAL
    /// handle, the lease, loaded extensions, attached lakes, the encryption
    /// key, the SQL `LOAD EXTENSION` gate, the caller-managed session (the
    /// caller rewrites `current_db` right after, so this only adopts
    /// `other`'s checkpoint-default session as the base), and this process's
    /// session-local **views** (they live inside the catalogs but are never
    /// persisted, so the reloaded catalog is authoritative only for tables;
    /// design hazard H10). Used by the multi-process refresh's full rebuild.
    pub(crate) fn adopt(&mut self, other: PinkDb) {
        debug_assert!(
            self.txn_snapshot.is_none(),
            "a refreshed store is never inside an embedded transaction"
        );
        // Session-local views survive the swap; a replayed object colliding
        // with one is then a hard replay error (hazard H4), exactly like a
        // reopen.
        let local_views: Vec<(String, HashMap<String, ViewDef>)> = self
            .databases
            .iter()
            .filter(|(_, cat)| !cat.views.is_empty())
            .map(|(key, cat)| (key.clone(), cat.views.clone()))
            .collect();
        self.databases = other.databases;
        for (key, views) in local_views {
            if let Some(cat) = self.databases.get_mut(&key) {
                for (name, view) in views {
                    cat.views.entry(name).or_insert(view);
                }
            }
        }
        self.current_db = other.current_db;
    }

    /// WAL hook for one statement that has just committed successfully
    /// (autocommit) — or was executed inside an open transaction, in which
    /// case its text is buffered until `COMMIT`. Non-mutating statements
    /// (queries, view DDL) never reach the WAL.
    ///
    /// The record is made self-contained: when the WAL's replay session
    /// differs from the statement's session (after a checkpoint, or when
    /// multiple connections share the file) a `USE` prefix is emitted; and
    /// when the statement calls a dynamic function whose extension the
    /// current WAL generation has not loaded yet, a `LOAD EXTENSION '<path>'`
    /// prefix is emitted so replay can re-load it (see
    /// [`PinkDb::wal_append_session`]).
    ///
    /// `dynamic_functions_used` lists the dynamic-function names (lowercased
    /// registry keys) the statement's expressions reference, collected at
    /// bind time.
    pub(crate) fn wal_log_statement(
        &mut self,
        plan: &crate::plan::LogicalPlan,
        sql: &str,
        dynamic_functions_used: &[String],
    ) -> Result<()> {
        if !crate::plan::writes_tables(plan) {
            return Ok(());
        }
        let touched = self.dynamic_function_paths(dynamic_functions_used);
        let loaded = loaded_extension_path(plan);
        if self.in_transaction() {
            if self.wal_txn_db.is_none() {
                self.wal_txn_db = Some(self.current_db.clone());
            }
            self.wal_txn.push(sql.to_string());
            push_unique(&mut self.wal_txn_ext_paths, touched);
            push_unique(&mut self.wal_txn_ext_loaded, loaded);
            return Ok(());
        }
        // `USE` is self-contained: replaying it lands in the target session
        // from anywhere, so no prefix is needed (and none would help).
        if let crate::plan::LogicalPlan::Use { name } = plan {
            if let Some(wal) = self.durable_handle() {
                let mut guard = wal.lock().map_err(|_| {
                    PinkDbError::Execution("internal lock error (poisoned)".to_string())
                })?;
                guard.append(&[sql.to_string()])?;
                guard.set_session_db(&name.to_ascii_lowercase());
            }
            return Ok(());
        }
        let db = self.current_db.clone();
        self.wal_append_session(&db, &db, &[sql.to_string()], &touched, &loaded)
    }

    /// Append one committed unit executed in session database `db_before`,
    /// leaving the replay session at `db_after` (no-op in memory). When the
    /// WAL's replay session differs from `db_before`, a `USE db_before`
    /// statement is prepended so the record replays in the right database —
    /// used by the embedded path (after a checkpoint or recovery) and by
    /// [`crate::Connection`]s, whose session is not the store's.
    ///
    /// `touched_extensions` lists the extension paths whose functions the
    /// statements call; every path the current WAL generation has not loaded
    /// yet (and that the statements do not load themselves) is prefixed as
    /// `LOAD EXTENSION '<path>'`, so replay re-loads exactly the extensions
    /// the record needs. `loaded_extensions` lists paths loaded by the
    /// statements' own `LOAD EXTENSION` text: those are already in the record
    /// and are only marked covered. See [`crate::wal::Wal`] for the tracked
    /// set's lifecycle (seeded from replay, reset by a checkpoint).
    ///
    /// A `USE` prefix naming a database that no longer exists could never
    /// replay (the `DROP` that removed it is already earlier in the WAL), so
    /// the prefix is dropped in that case. Callers hold the store lock across
    /// execution and append, which guarantees the statements are then
    /// session-independent: a session-dependent statement against a vanished
    /// database would have failed before reaching the WAL.
    pub(crate) fn wal_append_session(
        &self,
        db_before: &str,
        db_after: &str,
        stmts: &[String],
        touched_extensions: &[String],
        loaded_extensions: &[String],
    ) -> Result<()> {
        if let Some(wal) = self.durable_handle() {
            let mut guard = wal.lock().map_err(|_| {
                PinkDbError::Execution("internal lock error (poisoned)".to_string())
            })?;
            let before = self.database_exists(db_before).then_some(db_before);
            guard.append_in_session(
                before,
                db_after,
                stmts,
                touched_extensions,
                loaded_extensions,
            )?;
        }
        Ok(())
    }

    /// Open a transaction: snapshot the whole store. Fails if one is open
    /// (flat transactions only — no savepoints in v1). A durable store
    /// acquires and holds the write lease from here to `commit`/`rollback`
    /// (design 4.7): writes by other processes and their refreshes wait for
    /// the transaction, and the transaction's view cannot go stale.
    pub fn begin(&mut self) -> Result<()> {
        if self.txn_snapshot.is_some() {
            return Err(PinkDbError::Execution(
                "already in a transaction".to_string(),
            ));
        }
        let guard = self.acquire_lease()?;
        if let Some(g) = &guard {
            // Refresh before snapshotting: the transaction runs against the
            // complete, current state.
            self.refresh_under_lease(g)?;
        }
        self.wal_txn.clear();
        self.wal_txn_db = None;
        self.wal_txn_ext_paths.clear();
        self.wal_txn_ext_loaded.clear();
        self.txn_snapshot = Some(Box::new(self.clone()));
        self.txn_lease = guard;
        Ok(())
    }

    /// Commit: make the transaction's statements durable (one WAL record),
    /// then drop the snapshot, keeping all changes. A WAL failure returns the
    /// error and leaves the transaction open (nothing was published) — and
    /// with it the write lease, so a retried COMMIT still serializes.
    pub fn commit(&mut self) -> Result<()> {
        if self.txn_snapshot.is_none() {
            return Err(PinkDbError::Execution(
                "no transaction in progress".to_string(),
            ));
        }
        if !self.wal_txn.is_empty() {
            let stmts = std::mem::take(&mut self.wal_txn);
            let touched = std::mem::take(&mut self.wal_txn_ext_paths);
            let loaded = std::mem::take(&mut self.wal_txn_ext_loaded);
            let before = self
                .wal_txn_db
                .take()
                .unwrap_or_else(|| self.current_db.clone());
            let after = self.current_db.clone();
            if let Err(e) = self.wal_append_session(&before, &after, &stmts, &touched, &loaded) {
                // Keep the statements: a retried COMMIT must never publish
                // changes that made it to memory but not to the WAL.
                self.wal_txn = stmts;
                self.wal_txn_db = Some(before);
                self.wal_txn_ext_paths = touched;
                self.wal_txn_ext_loaded = loaded;
                return Err(e);
            }
        }
        self.wal_txn.clear();
        self.wal_txn_db = None;
        self.wal_txn_ext_paths.clear();
        self.wal_txn_ext_loaded.clear();
        self.txn_snapshot = None;
        // Release the transaction's lease only after the record is durable.
        self.txn_lease = None;
        Ok(())
    }

    /// Roll back: restore the snapshot, discarding all changes since BEGIN
    /// (data and catalog alike); nothing is logged. The session's current
    /// database is *not* transactional — it keeps its live value.
    pub fn rollback(&mut self) -> Result<()> {
        match self.txn_snapshot.take() {
            Some(snap) => {
                let current = self.current_db.clone();
                *self = *snap;
                self.current_db = current;
                self.wal_txn.clear();
                self.wal_txn_db = None;
                self.wal_txn_ext_paths.clear();
                self.wal_txn_ext_loaded.clear();
                // The snapshot was cloned without the lease; dropping the old
                // `self` above already released the transaction's guard.
                self.txn_lease = None;
                Ok(())
            }
            None => Err(PinkDbError::Execution(
                "no transaction in progress".to_string(),
            )),
        }
    }

    fn key(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    pub fn create_table(&mut self, name: &str, schema: Vec<(String, DataType)>) -> Result<()> {
        let db = self.current_db.clone();
        self.create_table_in(&db, name, schema)
    }

    pub fn create_table_in(
        &mut self,
        db: &str,
        name: &str,
        schema: Vec<(String, DataType)>,
    ) -> Result<()> {
        if let Some(err) = self.attached_read_only(db, name) {
            return Err(err);
        }
        let key = Self::key(name);
        let cat = self.catalog_mut(db)?;
        if cat.tables.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "table \"{name}\" already exists"
            )));
        }
        if cat.views.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "a view named \"{name}\" already exists"
            )));
        }
        cat.tables
            .insert(key, DataTable::new(name.to_string(), schema)?);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let db = self.current_db.clone();
        self.drop_table_in(&db, name)
    }

    pub fn drop_table_in(&mut self, db: &str, name: &str) -> Result<()> {
        if let Some(err) = self.attached_read_only(db, name) {
            return Err(err);
        }
        self.catalog_mut(db)?
            .tables
            .remove(&Self::key(name))
            .map(|_| ())
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    pub fn table(&self, name: &str) -> Result<&DataTable> {
        self.table_in(&self.current_db, name)
    }

    pub fn table_in(&self, db: &str, name: &str) -> Result<&DataTable> {
        self.catalog(db)?
            .tables
            .get(&Self::key(name))
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    pub fn table_mut(&mut self, name: &str) -> Result<&mut DataTable> {
        let db = self.current_db.clone();
        self.table_mut_in(&db, name)
    }

    pub fn table_mut_in(&mut self, db: &str, name: &str) -> Result<&mut DataTable> {
        if let Some(err) = self.attached_read_only(db, name) {
            return Err(err);
        }
        self.catalog_mut(db)?
            .tables
            .get_mut(&Self::key(name))
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    /// Sorted table names (stable output for `.tables`).
    pub fn list_tables(&self) -> Vec<String> {
        self.list_tables_in(&self.current_db)
    }

    /// Sorted table display names in `db`.
    pub fn list_tables_in(&self, db: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .databases
            .get(&Self::db_key(db))
            .map(|c| c.tables.values().map(|t| t.name.clone()).collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Store a view definition (validated by the binder before calling).
    pub fn create_view(
        &mut self,
        name: &str,
        query: Select,
        cols: Option<Vec<String>>,
    ) -> Result<()> {
        let db = self.current_db.clone();
        self.create_view_in(&db, name, query, cols)
    }

    pub fn create_view_in(
        &mut self,
        db: &str,
        name: &str,
        query: Select,
        cols: Option<Vec<String>>,
    ) -> Result<()> {
        let key = Self::key(name);
        let cat = self.catalog_mut(db)?;
        if cat.tables.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "a table named \"{name}\" already exists"
            )));
        }
        if cat.views.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "view \"{name}\" already exists"
            )));
        }
        cat.views.insert(
            key,
            ViewDef {
                name: name.to_string(),
                query,
                cols,
            },
        );
        Ok(())
    }

    pub fn drop_view(&mut self, name: &str) -> Result<()> {
        let db = self.current_db.clone();
        self.drop_view_in(&db, name)
    }

    pub fn drop_view_in(&mut self, db: &str, name: &str) -> Result<()> {
        self.catalog_mut(db)?
            .views
            .remove(&Self::key(name))
            .map(|_| ())
            .ok_or_else(|| PinkDbError::Catalog(format!("view \"{name}\" not found")))
    }

    pub fn view(&self, name: &str) -> Result<&ViewDef> {
        self.view_in(&self.current_db, name)
    }

    pub fn view_in(&self, db: &str, name: &str) -> Result<&ViewDef> {
        self.catalog(db)?
            .views
            .get(&Self::key(name))
            .ok_or_else(|| PinkDbError::Catalog(format!("view \"{name}\" not found")))
    }

    /// Sorted view display names.
    pub fn list_views(&self) -> Vec<String> {
        self.list_views_in(&self.current_db)
    }

    /// Sorted view display names in `db`.
    pub fn list_views_in(&self, db: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .databases
            .get(&Self::db_key(db))
            .map(|c| c.views.values().map(|v| v.name.clone()).collect())
            .unwrap_or_default();
        names.sort();
        names
    }
}

/// The extension path a `LOAD EXTENSION` plan loads, if the plan is one.
/// Such a path is already covered by the record's own statement text, so it
/// is never prefixed — it is only marked as loaded for later records.
pub(crate) fn loaded_extension_path(plan: &crate::plan::LogicalPlan) -> Vec<String> {
    match plan {
        crate::plan::LogicalPlan::LoadExtension { path } => vec![path.clone()],
        _ => Vec::new(),
    }
}

/// Append `additions` to `out`, skipping entries already present (exact
/// string match, like the WAL's extension-path bookkeeping).
fn push_unique(out: &mut Vec<String>, additions: Vec<String>) {
    for a in additions {
        if !out.iter().any(|existing| existing == &a) {
            out.push(a);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{Expr, Projection};

    fn sample() -> DataTable {
        let mut t = DataTable::new(
            "t".to_string(),
            vec![
                ("id".to_string(), DataType::Bigint),
                ("name".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        t.append_row(vec![Value::Bigint(1), Value::Varchar("ann".to_string())])
            .unwrap();
        // Integer widens to Bigint.
        t.append_row(vec![Value::Integer(2), Value::Null]).unwrap();
        t
    }

    #[test]
    fn create_and_lookup_case_insensitive() {
        let mut db = PinkDb::new();
        db.create_table("Sales", vec![("id".to_string(), DataType::Integer)])
            .unwrap();
        assert!(db.table("sales").is_ok());
        assert!(db.table("SALES").is_ok());
        assert_eq!(db.list_tables(), vec!["Sales".to_string()]);
        assert!(db.create_table("sales", vec![]).is_err());
        assert!(db.table("missing").is_err());
    }

    #[test]
    fn append_and_get_with_coercion() {
        let t = sample();
        assert_eq!(t.num_rows(), 2);
        assert_eq!(t.get(0, 0), Some(Value::Bigint(1)));
        assert_eq!(t.get(1, 0), Some(Value::Bigint(2)));
        assert_eq!(t.get(1, 1), Some(Value::Null));
        assert_eq!(t.get(9, 0), None);
    }

    #[test]
    fn coercion_rejects_bad_types() {
        let mut t = sample();
        let err = t
            .append_row(vec![
                Value::Varchar("x".to_string()),
                Value::Varchar("y".to_string()),
            ])
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Type(_)));
        let err = t.append_row(vec![Value::Null]).unwrap_err();
        assert!(matches!(err, PinkDbError::Execution(_)));
    }

    #[test]
    fn chunk_boundary_stays_aligned() {
        let mut t = DataTable::new(
            "big".to_string(),
            vec![("a".to_string(), DataType::Integer)],
        )
        .unwrap();
        for i in 0..(CHUNK_SIZE + 5) {
            t.append_row(vec![Value::Integer(i as i32)]).unwrap();
        }
        assert_eq!(t.num_chunks(), 2);
        assert_eq!(t.scan_chunk(0).unwrap().num_rows(), CHUNK_SIZE);
        assert_eq!(t.scan_chunk(1).unwrap().num_rows(), 5);
        assert!(t.scan_chunk(2).is_none());
        assert_eq!(
            t.get(CHUNK_SIZE, 0),
            Some(Value::Integer(CHUNK_SIZE as i32))
        );
    }

    #[test]
    fn append_chunk_moves_and_merges() {
        let mut t = DataTable::new(
            "t".to_string(),
            vec![
                ("a".to_string(), DataType::Bigint),
                ("s".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();

        let chunk_rows = |n: i64, start: i64| -> Vec<Vector> {
            let mut a = Vector::with_capacity(DataType::Bigint, CHUNK_SIZE);
            let mut s = Vector::with_capacity(DataType::Varchar, CHUNK_SIZE);
            for i in start..start + n {
                a.push_valid(Value::Bigint(i)).unwrap();
                if i % 5 == 0 {
                    s.push_null().unwrap();
                } else {
                    s.push_str(&format!("v{i}")).unwrap();
                }
            }
            vec![a, s]
        };

        // Whole batch into an empty tail: moved in as one chunk.
        t.append_chunk(chunk_rows(2, 0)).unwrap();
        assert_eq!(t.num_rows(), 2);
        assert_eq!(t.num_chunks(), 1);

        // Batch spanning a full tail: splits at the chunk boundary.
        t.append_chunk(chunk_rows(CHUNK_SIZE as i64, 2)).unwrap();
        assert_eq!(t.num_rows(), 2 + CHUNK_SIZE);
        assert_eq!(t.num_chunks(), 2);
        assert_eq!(t.scan_chunk(0).unwrap().num_rows(), CHUNK_SIZE);
        assert_eq!(t.scan_chunk(1).unwrap().num_rows(), 2);
        assert_eq!(
            t.get(CHUNK_SIZE - 1, 0),
            Some(Value::Bigint(CHUNK_SIZE as i64 - 1))
        );
        assert_eq!(t.get(CHUNK_SIZE, 0), Some(Value::Bigint(CHUNK_SIZE as i64)));
        assert_eq!(
            t.get(CHUNK_SIZE + 1, 1),
            Some(Value::Varchar(format!("v{}", CHUNK_SIZE + 1)))
        );

        // Shape/type validation.
        let mut t2 =
            DataTable::new("u".to_string(), vec![("a".to_string(), DataType::Integer)]).unwrap();
        assert!(matches!(
            t2.append_chunk(vec![Vector::new_empty(DataType::Double)]),
            Err(PinkDbError::Type(_))
        ));
        assert!(matches!(
            t2.append_chunk(vec![]),
            Err(PinkDbError::Execution(_))
        ));
        assert_eq!(t2.num_rows(), 0);
    }

    #[test]
    fn append_shared_chunk_shares_arcs_and_matches_append_chunk() {
        let mk = || {
            let t = DataTable::new(
                "t".to_string(),
                vec![
                    ("a".to_string(), DataType::Bigint),
                    ("s".to_string(), DataType::Varchar),
                ],
            )
            .unwrap();
            let mut a = Vector::with_capacity(DataType::Bigint, CHUNK_SIZE);
            let mut s = Vector::with_capacity(DataType::Varchar, CHUNK_SIZE);
            for i in 0..CHUNK_SIZE {
                a.push_valid(Value::Bigint(i as i64)).unwrap();
                s.push_str(&format!("v{}", i % 7)).unwrap();
            }
            let chunk = vec![Arc::new(a), Arc::new(s)];
            // A partial tail first, so the next append must merge.
            let mut a2 = Vector::with_capacity(DataType::Bigint, CHUNK_SIZE);
            let mut s2 = Vector::with_capacity(DataType::Varchar, CHUNK_SIZE);
            for i in 0..3i64 {
                a2.push_valid(Value::Bigint(-i)).unwrap();
                s2.push_str(&format!("t{i}")).unwrap();
            }
            (t, chunk, vec![Arc::new(a2), Arc::new(s2)])
        };
        let (mut shared, full, partial) = mk();
        let (mut copied, _, _) = mk();

        // The partial tail path (merge) plus a full-batch append on both,
        // then the fast-path append, then compare chunk-for-chunk.
        shared.append_shared_chunk(&partial).unwrap();
        copied
            .append_chunk(partial.iter().map(|c| (**c).clone()).collect())
            .unwrap();
        shared.append_shared_chunk(&full).unwrap();
        copied
            .append_chunk(full.iter().map(|c| (**c).clone()).collect())
            .unwrap();

        assert_eq!(shared.num_rows(), copied.num_rows());
        assert_eq!(shared.num_chunks(), copied.num_chunks());
        for i in 0..shared.num_chunks() {
            let a = shared.scan_chunk(i).unwrap();
            let b = copied.scan_chunk(i).unwrap();
            assert_eq!(a.columns, b.columns, "chunk {i} differs");
            assert_eq!(a.num_rows(), b.num_rows());
        }
        // The CHUNK_SIZE-row batch after a partial 3-row tail merges, so the
        // source vectors are not shared anymore; a batch landing on the empty
        // initial chunk is shared, not copied.
        let (mut shared2, full2, _) = mk();
        shared2.append_shared_chunk(&full2).unwrap();
        assert_eq!(shared2.num_chunks(), 1); // empty tail is replaced
        assert!(Arc::ptr_eq(&shared2.columns[0].chunks[0], &full2[0]));
        assert!(Arc::ptr_eq(&shared2.columns[1].chunks[0], &full2[1]));
    }

    #[test]
    fn duplicate_columns_rejected() {
        let err = DataTable::new(
            "d".to_string(),
            vec![
                ("a".to_string(), DataType::Integer),
                ("A".to_string(), DataType::Integer),
            ],
        )
        .unwrap_err();
        assert!(matches!(err, PinkDbError::Catalog(_)));
    }

    #[test]
    fn databases_catalog_and_session_default() {
        let mut db = PinkDb::new();
        assert_eq!(db.current_db(), "main");
        assert_eq!(db.list_databases(), vec!["main".to_string()]);
        assert!(db.database_exists("Main"));
        assert!(!db.database_exists("other"));

        // Case-insensitive create/use; display name kept as written.
        db.create_database("Analytics").unwrap();
        assert!(db.create_database("ANALYTICS").is_err(), "duplicate");
        assert_eq!(
            db.list_databases(),
            vec!["Analytics".to_string(), "main".to_string()]
        );
        db.use_database("analytics").unwrap();
        assert_eq!(db.current_db(), "analytics");
        assert!(db.use_database("missing").is_err());

        // Tables resolve against the session's current database.
        db.create_table("t", vec![("a".to_string(), DataType::Integer)])
            .unwrap();
        assert!(db.table_in("analytics", "t").is_ok());
        assert!(db.table("t").is_ok());
        assert!(db.table_in("main", "t").is_err());
        // Explicit targets work regardless of the session.
        db.create_table_in("main", "m", vec![("a".to_string(), DataType::Integer)])
            .unwrap();
        assert_eq!(db.list_tables_in("main"), vec!["m".to_string()]);
        assert_eq!(db.list_tables(), vec!["t".to_string()]);

        // Drop rules: never the last database; fallback prefers main.
        assert!(db.drop_database("main").is_ok());
        assert_eq!(db.fallback_db(), "analytics");
        assert!(db.drop_database("analytics").is_err(), "last database");
        assert!(db.drop_database("nope").is_err());
        assert_eq!(db.list_databases(), vec!["Analytics".to_string()]);
    }

    #[test]
    fn views_are_per_database() {
        let mut db = PinkDb::new();
        db.create_database("db1").unwrap();
        let query = Select {
            with: Vec::new(),
            distinct: false,
            projections: vec![Projection::Expr {
                expr: Expr::Literal(Value::Integer(1)),
                alias: None,
            }],
            from: None,
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            unions: Vec::new(),
        };
        db.create_view("v", query.clone(), None).unwrap();
        assert!(db.view_in("main", "v").is_ok());
        assert!(db.view_in("db1", "v").is_err());
        db.create_view_in("db1", "v", query, None).unwrap();
        assert_eq!(db.list_views_in("db1"), vec!["v".to_string()]);
        assert_eq!(db.list_views(), vec!["v".to_string()]);
        db.drop_view_in("db1", "v").unwrap();
        assert!(db.view_in("db1", "v").is_err());
    }
}
