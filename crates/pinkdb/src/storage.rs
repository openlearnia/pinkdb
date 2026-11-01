//! In-memory columnar storage + catalog.
//!
//! `DataTable` = one [`Vector`] chunk list per column (each ≤ [`CHUNK_SIZE`]
//! rows). [`PinkDb`] = map of table name → table. Single connection, single
//! thread. No persistence in v1.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use pinkdb_ext_loader::{ExtFunction, ExtHandle};

use crate::crypto::EncryptionKey;
use crate::plan::ColumnDefault;
use crate::sql::ast::Select;
use crate::stats::{ColStats, Hll, TableStats, Zone};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector};
use crate::wal::Wal;

/// Next store-unique table version stamp (see [`DataTable::version`]).
fn next_table_version() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Full index-map rebuilds on this test thread. A test hook: appends must not
// increment it, mutations that move or remove rows may. Thread-local so
// parallel tests do not count each other's rebuilds.
#[cfg(test)]
thread_local! {
    static INDEX_REBUILDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn index_rebuild_count() -> u64 {
    INDEX_REBUILDS.with(|count| count.get())
}

#[cfg(test)]
pub(crate) fn reset_index_rebuild_count() {
    INDEX_REBUILDS.with(|count| count.set(0));
}

/// One column: its schema + its chunk list.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
    /// The column's DEFAULT (wave 65): the expression a later `INSERT` that
    /// omits the column evaluates, and the expression `ALTER TABLE … ADD
    /// COLUMN … DEFAULT` evaluated for every existing row. `None` = no
    /// default (an omitted `INSERT` column writes NULL). The `.pink` footer
    /// stores its text under the defaults flag and rebinds it at load.
    pub default: Option<ColumnDefault>,
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
            default: None,
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
    /// Declarative constraints (PRIMARY KEY / UNIQUE / FOREIGN KEY / NOT
    /// NULL): catalog metadata only, **never enforced** on INSERT/UPDATE.
    /// `DESCRIBE` reports them and the optimizer may trust them (see
    /// [`crate::optimizer`]).
    pub constraints: TableConstraints,
    /// In-memory hash indexes for equality lookups.
    pub indexes: Vec<Index>,
    /// Vector indexes (wave 107b), in creation order. A definition whose
    /// `index` is `None` is waiting for its provider extension to be loaded;
    /// `blob` holds the serialized graph until then. Mutating such a table is
    /// refused rather than silently leaving the graph behind.
    pub vector_indexes: Vec<VectorIndexDef>,
    /// Set by every mutation (INSERT/UPDATE/DELETE/bulk load/COPY) when the
    /// table has indexes: the maps describe a pre-mutation snapshot. The
    /// statement path rebuilds them lazily from the live chunks at the next
    /// statement boundary ([`PinkDb::rebuild_stale_indexes`]), and the
    /// optimizer refuses to use one while this flag is set, so a stale index
    /// is only ever a missed optimization — never a wrong answer. See
    /// [`DataTable::rebuild_indexes`].
    pub indexes_stale: bool,
    /// Cached occupant of every declared PRIMARY KEY / UNIQUE value (see
    /// [`crate::keys`]). `None` means "not built yet": the first keyed write
    /// pays one scan and leaves the map here, so later statements are O(writes)
    /// instead of O(rows). Every mutation that moves rows patches the map or
    /// drops it, so a present map is exact.
    pub(crate) key_index: Option<crate::keys::KeySpace>,
    /// Mutation stamp for optimistic-concurrency conflict detection. Drawn
    /// from a process-wide counter on creation and on every
    /// append/update/delete batch, so it is unique across tables: a table
    /// dropped and recreated under the same name never repeats an earlier
    /// stamp (a transaction based on the old table must conflict).
    /// Process-lifetime only (not persisted — save/load draws fresh stamps).
    pub version: u64,
    /// Catalog statistics (wave 122): row/NULL counts, min/max and a
    /// distinct-count sketch, maintained by every mutation and read by the
    /// cost model and `pinkdb_column_stats()`.
    pub(crate) stats: TableStats,
    /// Zone maps: `zones[column][chunk]`, aligned with each column's chunks.
    /// Exact per chunk when written, widened (never narrowed) by mutations.
    pub(crate) zones: Vec<Vec<Zone>>,
}

/// One vector index definition on a table. `index` is the live graph; the
/// engine only sees it through [`crate::vector_index::VectorIndex`]. The
/// graph is cloned on first write after a transaction snapshot shares the
/// `Arc` (copy-on-write), exactly like the column chunks.
#[derive(Debug, Clone)]
pub struct VectorIndexDef {
    pub name: String,
    pub column: usize,
    /// Registered provider name, lowercased.
    pub provider: String,
    /// `WITH (...)` pairs as written (provider-specific; the graph carries
    /// its own copy after deserialization).
    pub options: Vec<(String, String)>,
    pub index: Option<std::sync::Arc<dyn crate::vector_index::VectorIndex>>,
    /// Serialized graph read from the `.pink` footer, kept until the
    /// provider is loaded.
    pub blob: Option<std::sync::Arc<[u8]>>,
}

impl VectorIndexDef {
    pub(crate) fn pending_error(&self, table: &str) -> PinkDbError {
        PinkDbError::Execution(format!(
            "vector index \"{}\" on table \"{table}\" needs the \"{}\" extension; run \
             LOAD EXTENSION '{}' first",
            self.name, self.provider, self.provider
        ))
    }

    /// The graph as a mutable reference, deep-copying it first when a
    /// transaction snapshot still shares the `Arc`.
    fn writable(&mut self, table: &str) -> Result<&mut dyn crate::vector_index::VectorIndex> {
        if self.index.is_none() {
            return Err(self.pending_error(table));
        }
        let slot = self.index.as_mut().expect("checked above");
        if std::sync::Arc::get_mut(slot).is_none() {
            *slot = std::sync::Arc::from(slot.clone_box());
        }
        Ok(std::sync::Arc::get_mut(slot).expect("just made unique"))
    }
}

/// Append-buffer size for vector-index maintenance, and the count at which
/// an append switches from per-row `insert` to the provider's bulk build
/// (wave 107d). Small appends keep the exact sequential path; the buffer is
/// a multiple of the HNSW batch size, so flushing never re-segments a build.
const VECTOR_APPEND_BUFFER: usize = 1024;
const VECTOR_BATCH_MIN: usize = 32;

fn flush_vector_rows(
    def: &mut VectorIndexDef,
    table: &str,
    pending: &mut Vec<(u64, Vec<f32>)>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let index = def.writable(table)?;
    if pending.len() >= VECTOR_BATCH_MIN {
        let refs: Vec<(u64, &[f32])> = pending
            .iter()
            .map(|(row, vector)| (*row, vector.as_slice()))
            .collect();
        index.insert_batch(&refs)?;
    } else {
        for (row, vector) in pending.iter() {
            index.insert(*row, vector)?;
        }
    }
    pending.clear();
    Ok(())
}

/// A single-column hash index: maps a key value to the global row indices
/// (flattened across chunks) where it occurs. NULL keys are not stored
/// (NULL never matches `=`).
#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub column: usize,
    pub map: std::collections::HashMap<Vec<u8>, Vec<usize>>,
}

/// Build one hash-index map from a column's chunks: non-NULL key encoding →
/// global row indices (flattened across chunks, using each chunk's real
/// length). Shared by `CREATE INDEX`, the lazy rebuild and the `.pink`
/// loader (which rebuilds every persisted index from the decoded rows) so all
/// of them produce identical keys.
pub(crate) fn index_map_for(column: &Column) -> std::collections::HashMap<Vec<u8>, Vec<usize>> {
    let mut map = std::collections::HashMap::new();
    let mut offset = 0usize;
    for chunk in column.chunks.iter() {
        for row in 0..chunk.len() {
            let v = chunk.get(row);
            if matches!(v, Value::Null) {
                continue;
            }
            let mut buf = Vec::new();
            crate::exec::aggregate::encode_value(&mut buf, &v);
            map.entry(buf).or_insert_with(Vec::new).push(offset + row);
        }
        offset += chunk.len();
    }
    map
}

/// The kind of a declarative key constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Primary,
    Unique,
}

/// A `PRIMARY KEY` / `UNIQUE` constraint: an ordered column list.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyConstraint {
    /// `CONSTRAINT <name>` if written.
    pub name: Option<String>,
    pub kind: KeyKind,
    /// Column indices in declaration order (non-empty).
    pub columns: Vec<usize>,
}

/// A `FOREIGN KEY` constraint: this table's columns → a key of another table.
/// Validated at `CREATE TABLE` time (the referenced columns must be a
/// `PRIMARY KEY` or `UNIQUE` of the referenced table) and never enforced on
/// writes.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyConstraint {
    /// `CONSTRAINT <name>` if written.
    pub name: Option<String>,
    /// This table's column indices, in declaration order.
    pub columns: Vec<usize>,
    /// Lowercased referenced database name.
    pub ref_db: String,
    /// Lowercased referenced table name.
    pub ref_table: String,
    /// Referenced column indices, in declaration order (same length as
    /// `columns`; position `i` references `ref_columns[i]`).
    pub ref_columns: Vec<usize>,
}

/// Everything declarative a table carries. Default = an unconstrained table
/// (byte-compatible with a pre-constraint catalog).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableConstraints {
    /// `PRIMARY KEY` / `UNIQUE` constraints in declaration order.
    pub keys: Vec<KeyConstraint>,
    /// `FOREIGN KEY` constraints in declaration order.
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    /// Column indices declared `NOT NULL` (in declaration order). A PRIMARY
    /// KEY column is additionally NOT NULL *implied*; see
    /// [`TableConstraints::is_not_null`].
    pub not_null: Vec<usize>,
}

impl TableConstraints {
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.foreign_keys.is_empty() && self.not_null.is_empty()
    }

    /// The table's primary key, if declared.
    pub fn primary_key(&self) -> Option<&KeyConstraint> {
        self.keys.iter().find(|k| k.kind == KeyKind::Primary)
    }

    /// The `key` marker `DESCRIBE` prints for a column: `PRI` when the column
    /// is part of the primary key (which wins), else `UNI` when it is part of
    /// any UNIQUE constraint (DuckDB's precedence).
    pub fn column_key_kind(&self, col: usize) -> Option<KeyKind> {
        let mut uniq = false;
        for k in &self.keys {
            if k.columns.contains(&col) {
                match k.kind {
                    KeyKind::Primary => return Some(KeyKind::Primary),
                    KeyKind::Unique => uniq = true,
                }
            }
        }
        uniq.then_some(KeyKind::Unique)
    }

    /// Whether the column is NOT NULL: declared so, or implied by the PRIMARY
    /// KEY (DuckDB semantics).
    pub fn is_not_null(&self, col: usize) -> bool {
        self.not_null.contains(&col)
            || self
                .primary_key()
                .is_some_and(|pk| pk.columns.contains(&col))
    }

    /// The `PRIMARY KEY`/`UNIQUE` constraint whose columns are exactly `cols`
    /// (ordered match, DuckDB's foreign-key rule), if any.
    pub fn key_on(&self, cols: &[usize]) -> Option<&KeyConstraint> {
        self.keys.iter().find(|k| k.columns.as_slice() == cols)
    }
}

impl DataTable {
    pub fn new(name: String, schema: Vec<(String, DataType)>) -> Result<Self> {
        Self::new_with_constraints(name, schema, TableConstraints::default())
    }

    /// [`DataTable::new`] plus the declarative constraints of `CREATE TABLE`.
    pub fn new_with_constraints(
        name: String,
        schema: Vec<(String, DataType)>,
        constraints: TableConstraints,
    ) -> Result<Self> {
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
        let schema_len = schema.len();
        // Constraint indices address this schema (the binder and the `.pink`
        // reader both validate before constructing; a hand-built set that
        // does not is a programming error).
        debug_assert!(
            constraints
                .keys
                .iter()
                .all(|k| !k.columns.is_empty() && k.columns.iter().all(|c| *c < schema.len()))
                && constraints.foreign_keys.iter().all(|fk| {
                    !fk.columns.is_empty()
                        && fk.columns.len() == fk.ref_columns.len()
                        && fk.columns.iter().all(|c| *c < schema.len())
                })
                && constraints.not_null.iter().all(|c| *c < schema.len()),
            "constraint indices must address the table schema"
        );
        Ok(Self {
            name,
            columns: schema
                .into_iter()
                .map(|(name, dtype)| Column::new(name, dtype))
                .collect(),
            num_rows: 0,
            constraints,
            indexes: Vec::new(),
            vector_indexes: Vec::new(),
            indexes_stale: false,
            key_index: None,
            version: next_table_version(),
            stats: TableStats::new(schema_len),
            zones: (0..schema_len).map(|_| vec![Zone::empty()]).collect(),
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
        // Exact statistics in one pass: a constructor sees the data it is
        // handed, and no caller has a cheaper source (the `.pink` loader reads
        // its footer instead, via `set_loaded_stats`).
        let (stats, zones) = TableStats::scan(&columns, num_rows as u64);
        Self {
            name,
            columns,
            num_rows,
            constraints: TableConstraints::default(),
            indexes: Vec::new(),
            vector_indexes: Vec::new(),
            indexes_stale: false,
            key_index: None,
            version: next_table_version(),
            stats,
            zones,
        }
    }

    /// The table's catalog statistics.
    pub fn stats(&self) -> &TableStats {
        &self.stats
    }

    /// The zone maps: `zones[column][chunk]`, aligned with the column chunks.
    pub(crate) fn zones(&self) -> &[Vec<Zone>] {
        &self.zones
    }

    /// Adopt the statistics a `.pink` footer carries, without rescanning the
    /// rows: the reader appends its chunks with stats tracking off and installs
    /// the persisted (already exact-or-flagged) facts here.
    ///
    /// `zones` is only adopted when the caller knows the loaded chunks have
    /// exactly the writer's boundaries (`zones_aligned`); otherwise, and
    /// whenever the shapes disagree, the zones are rebuilt from the rows. A
    /// zone map applied to the wrong chunk would skip rows that match, so
    /// this is a runtime check, never a debug-only one.
    pub(crate) fn set_loaded_stats(
        &mut self,
        stats: TableStats,
        zones: Vec<Vec<Zone>>,
        zones_aligned: bool,
    ) {
        let shaped = stats.cols.len() == self.columns.len()
            && zones.len() == self.columns.len()
            && zones
                .iter()
                .zip(&self.columns)
                .all(|(z, c)| z.len() == c.chunks.len());
        if stats.cols.len() == self.columns.len() {
            self.stats = stats;
        } else {
            self.stats = TableStats::unknown(self.columns.len(), self.num_rows as u64);
        }
        self.zones = if zones_aligned && shaped {
            zones
        } else {
            self.rebuilt_zones()
        };
    }

    /// Zone maps recomputed from the rows, one per column chunk.
    fn rebuilt_zones(&self) -> Vec<Vec<Zone>> {
        self.columns
            .iter()
            .map(|c| c.chunks.iter().map(|v| Zone::of_vector(v)).collect())
            .collect()
    }

    /// Replace the statistics with an exact recomputation (`ANALYZE`): every
    /// zone rebuilt from the rows, every sketch emptied and refilled, `dirty`
    /// cleared. O(rows), which is the point — it is the repair path.
    pub(crate) fn analyze(&mut self) {
        let (stats, zones) = TableStats::scan(&self.columns, self.num_rows as u64);
        self.stats = stats;
        self.zones = zones;
    }

    /// Drop every row in place (materialized-view refresh): the column
    /// metadata and indexes survive, the statistics start over.
    pub(crate) fn clear_rows(&mut self) {
        self.num_rows = 0;
        for col in &mut self.columns {
            col.chunks = vec![Arc::new(Vector::new_empty(col.dtype))];
        }
        self.stats = TableStats::new(self.columns.len());
        self.zones = self.columns.iter().map(|_| vec![Zone::empty()]).collect();
    }

    /// The zone map of one column chunk (empty when the map is not built).
    pub(crate) fn zone(&self, col: usize, chunk: usize) -> &Zone {
        static EMPTY: Zone = Zone {
            null_count: 0,
            min: None,
            max: None,
            nan_count: 0,
        };
        self.zones
            .get(col)
            .and_then(|c| c.get(chunk))
            .unwrap_or(&EMPTY)
    }

    /// Recompute one column's table-level aggregates from its zone maps: the
    /// exact repair after a mutation touched the chunk a bound came from.
    /// Per-column `(zone, folded sketch)` for one appended batch. Bulk
    /// ingestion is the write hot path and its columns are independent, so the
    /// min/max/NULL pass and the sketch's hashing run on the worker pool; the
    /// sketch is cloned per column and installed by the caller, which keeps
    /// the fold free of mutable table access. Small batches stay serial
    /// (the pool costs more than a few thousand cells).
    fn fold_chunk_stats(&self, chunk: &[std::sync::Arc<Vector>]) -> Vec<(Zone, Option<Hll>)> {
        use rayon::prelude::*;
        let cells = chunk.first().map(|c| c.len()).unwrap_or(0) * chunk.len();
        let fold = |ci: usize, src: &Vector| -> (Zone, Option<Hll>) {
            let zone = Zone::of_vector(src);
            let hll = self.stats.cols[ci].hll.as_ref().map(|hll| {
                let mut hll = (**hll).clone();
                for row in 0..src.len() {
                    hll.insert_cell(src, row);
                }
                hll
            });
            (zone, hll)
        };
        if cells >= 4096 {
            chunk
                .par_iter()
                .enumerate()
                .map(|(ci, src)| fold(ci, src))
                .collect()
        } else {
            chunk
                .iter()
                .enumerate()
                .map(|(ci, src)| fold(ci, src))
                .collect()
        }
    }

    fn rebuild_column_stats(&mut self, col: usize) {
        let mut st = ColStats {
            null_count: 0,
            min: None,
            max: None,
            hll: self.stats.cols[col].hll.clone(),
            dirty: self.stats.cols[col].dirty,
        };
        for zone in &self.zones[col] {
            st.null_count += zone.null_count;
            if let Some(min) = &zone.min {
                st.widen(min);
            }
            if let Some(max) = &zone.max {
                st.widen(max);
            }
        }
        self.stats.cols[col] = st;
    }

    /// After a chunk of `col` changed in place: recompute its zone, keep the
    /// NULL count exact, and rebuild the column's bounds only when the old
    /// zone was where a bound came from (widening is otherwise enough — a
    /// deleted or overwritten extreme value is the one case that can shrink a
    /// bound).
    fn refresh_zone(&mut self, col: usize, chunk: usize, dirty: bool) {
        let new = Zone::of_vector(&self.columns[col].chunks[chunk]);
        let old = std::mem::replace(&mut self.zones[col][chunk], new.clone());
        let st = &mut self.stats.cols[col];
        let held = old.holds(&st.min, &st.max);
        st.null_count = st.null_count + new.null_count - old.null_count;
        if dirty {
            st.dirty = true;
        }
        if held {
            self.rebuild_column_stats(col);
        } else {
            let st = &mut self.stats.cols[col];
            if let Some(min) = &new.min {
                st.widen(min);
            }
            if let Some(max) = &new.max {
                st.widen(max);
            }
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

    /// `ALTER TABLE … ADD COLUMN` (wave 65): append a column at the end,
    /// filled with the per-row `values` evaluated from `default`. The values
    /// are materialized into the chunks (the engine's in-memory style; the
    /// lake's schema record is what keeps the operation metadata-only for old
    /// *data files*). `default` stays recorded on the [`Column`] so later
    /// `INSERT`s that omit the column evaluate it too.
    pub(crate) fn add_column(
        &mut self,
        name: String,
        dtype: DataType,
        default: Option<ColumnDefault>,
        values: &[Value],
    ) -> Result<()> {
        if self
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&name))
        {
            return Err(PinkDbError::Catalog(format!(
                "Column with name \"{name}\" already exists!"
            )));
        }
        let mut col = Column {
            name,
            dtype,
            default: default.clone(),
            chunks: Vec::new(),
        };
        let lens: Vec<usize> = self
            .columns
            .first()
            .map(|c| c.chunks.iter().map(|v| v.len()).collect())
            .unwrap_or_default();
        debug_assert_eq!(values.len(), lens.iter().sum::<usize>());
        let mut values = values.iter();
        for len in lens {
            let mut v = Vector::with_capacity(dtype, len);
            for _ in 0..len {
                match values.next() {
                    Some(d) if !d.is_null() => v.push_valid(d.clone())?,
                    _ => v.push_null()?,
                }
            }
            col.chunks.push(Arc::new(v));
        }
        // A new column's statistics are exact from birth: every existing row
        // just received the same DEFAULT (usually NULL).
        let mut stats = ColStats::new();
        let mut zones = Vec::with_capacity(col.chunks.len());
        for chunk in &col.chunks {
            let zone = Zone::of_vector(chunk);
            stats.observe_chunk(chunk, &zone);
            zones.push(zone);
        }
        self.columns.push(col);
        self.stats.cols.push(stats);
        self.zones.push(zones);
        Ok(())
    }

    /// `ALTER TABLE … DROP COLUMN`: remove the column at `idx` and reindex the
    /// declarative constraints that described the columns after it. The
    /// caller (binder or executor) has already refused a column a key or
    /// foreign-key constraint depends on; `NOT NULL` is reindexed (its flag
    /// on the dropped column simply goes away).
    pub(crate) fn drop_column_at(&mut self, idx: usize) -> Result<()> {
        if idx >= self.columns.len() {
            return Err(PinkDbError::Execution(format!(
                "drop column index {idx} out of range for \"{}\" ({} columns)",
                self.name,
                self.columns.len()
            )));
        }
        if self.columns.len() == 1 {
            return Err(PinkDbError::Catalog(
                "cannot drop the only column of a table".to_string(),
            ));
        }
        self.columns.remove(idx);
        self.stats.cols.remove(idx);
        self.zones.remove(idx);
        let reindex = |c: &mut usize| {
            if *c > idx {
                *c -= 1;
            }
        };
        for k in &mut self.constraints.keys {
            k.columns.iter_mut().for_each(reindex);
        }
        for fk in &mut self.constraints.foreign_keys {
            fk.columns.iter_mut().for_each(reindex);
        }
        self.constraints.not_null.retain(|&c| c != idx);
        self.constraints.not_null.iter_mut().for_each(reindex);
        // L-1: an index on the dropped column goes away with it, and every
        // index to its right shifts left with its column. Without the shift a
        // later `ADD COLUMN` could land on the stale ordinal and the optimizer
        // would use the wrong column's map.
        self.indexes.retain(|i| i.column != idx);
        for i in &mut self.indexes {
            reindex(&mut i.column);
        }
        self.vector_indexes.retain(|i| i.column != idx);
        for i in &mut self.vector_indexes {
            reindex(&mut i.column);
        }
        self.version = next_table_version();
        Ok(())
    }

    /// Mark the table's hash indexes stale after a mutation that may move or
    /// remove rows (UPDATE/DELETE/TRUNCATE/ALTER). Cheap (one flag);
    /// [`Self::rebuild_indexes`] rebuilds them lazily on the next indexed
    /// lookup.
    pub(crate) fn invalidate_indexes(&mut self) {
        if !self.indexes.is_empty() {
            self.indexes_stale = true;
        }
    }

    /// Record the appended rows `start..num_rows` in the key map, if one is
    /// built. Appends never move an earlier row, so the cache stays exact;
    /// only `start..` needs reading.
    fn extend_key_index(&mut self, start: usize) {
        if self.key_index.is_none() || self.constraints.keys.is_empty() {
            return;
        }
        let mut keys = self.key_index.take().expect("checked above");
        for global in start..self.num_rows {
            let id = crate::keys::RowId::new(global / CHUNK_SIZE, global % CHUNK_SIZE);
            if id.chunk >= self.num_chunks() {
                break;
            }
            let values: Vec<Value> = self
                .columns
                .iter()
                .map(|c| c.chunks[id.chunk].get(id.row))
                .collect();
            keys.reserve(&values, id);
        }
        self.key_index = Some(keys);
    }

    /// Extend every fresh index map with the rows appended in `start..num_rows`.
    ///
    /// An append-only mutation does not move earlier rows, so each map only
    /// needs the new positions. If an index was already stale (an UPDATE or
    /// DELETE left a map describing an older snapshot), leave the flag alone:
    /// [`Self::rebuild_indexes`] will rebuild it from the final rows. This is
    /// why a stale map is always a missed optimization, never a wrong answer.
    fn extend_indexes(&mut self, start: usize) -> Result<()> {
        self.extend_key_index(start);
        self.extend_vector_indexes(start)?;
        if self.indexes.is_empty() || self.indexes_stale || start >= self.num_rows {
            return Ok(());
        }
        let mut indexes = std::mem::take(&mut self.indexes);
        for index in &mut indexes {
            let Some(column) = self.columns.get(index.column) else {
                continue;
            };
            // Walk the chunks once from the one holding `start`: a per-row
            // global lookup would rescan the chunk list for every row, which
            // makes a bulk append into an indexed table O(rows × chunks).
            let mut offset = 0usize;
            for chunk in &column.chunks {
                let len = chunk.len();
                if offset + len > start {
                    for local in start.saturating_sub(offset)..len {
                        let value = chunk.get(local);
                        if matches!(value, Value::Null) {
                            continue;
                        }
                        let mut buf = Vec::new();
                        crate::exec::aggregate::encode_value(&mut buf, &value);
                        index
                            .map
                            .entry(buf)
                            .or_insert_with(Vec::new)
                            .push(offset + local);
                    }
                }
                offset += len;
            }
        }
        self.indexes = indexes;
        Ok(())
    }

    /// Keep every vector index in step with rows appended in `start..`.
    /// NULL rows (and rows with a NULL element) are not vectors and are not
    /// indexed; they are still visible through the exact path.
    fn extend_vector_indexes(&mut self, start: usize) -> Result<()> {
        if self.vector_indexes.is_empty() || start >= self.num_rows {
            return Ok(());
        }
        let mut defs = std::mem::take(&mut self.vector_indexes);
        let result = (|| -> Result<()> {
            for def in &mut defs {
                let Some(column) = self.columns.get(def.column) else {
                    continue;
                };
                let crate::types::DataType::Array { len: n, .. } = column.dtype else {
                    continue;
                };
                let n = n as usize;
                let table = self.name.clone();
                let mut pending: Vec<(u64, Vec<f32>)> = Vec::new();
                let mut offset = 0usize;
                for chunk in &column.chunks {
                    let len = chunk.len();
                    if offset + len > start {
                        for local in start.saturating_sub(offset)..len {
                            let global = (offset + local) as u64;
                            let Some(vector) = crate::vector_index::row_vector(chunk, local, n)
                            else {
                                continue;
                            };
                            pending.push((global, vector));
                            if pending.len() == VECTOR_APPEND_BUFFER {
                                flush_vector_rows(def, &table, &mut pending)?;
                            }
                        }
                    }
                    offset += len;
                }
                flush_vector_rows(def, &table, &mut pending)?;
            }
            Ok(())
        })();
        self.vector_indexes = defs;
        result
    }

    /// Refuse a mutation that would leave a not-yet-hydrated graph behind.
    fn require_vector_indexes_loaded(&self) -> Result<()> {
        for def in &self.vector_indexes {
            if def.index.is_none() {
                return Err(def.pending_error(&self.name));
            }
        }
        Ok(())
    }

    /// Rebuild every hash index on this table from the live chunks, clearing
    /// the stale flag. Called lazily by [`PinkDb::rebuild_stale_indexes`] at
    /// the next statement boundary, so a later indexed lookup sees a snapshot
    /// of the current rows.
    pub(crate) fn rebuild_indexes(&mut self) {
        #[cfg(test)]
        INDEX_REBUILDS.with(|count| count.set(count.get() + 1));
        // Drop an index whose column ordinal no longer exists (only possible
        // if a catalog bug skipped the ALTER reindex) rather than panicking.
        self.indexes.retain(|i| i.column < self.columns.len());
        let maps: Vec<_> = self
            .indexes
            .iter()
            .map(|i| index_map_for(&self.columns[i.column]))
            .collect();
        for (index, map) in self.indexes.iter_mut().zip(maps) {
            index.map = map;
        }
        self.indexes_stale = false;
    }

    /// `ALTER TABLE … RENAME COLUMN`: set the column's display name (the
    /// binder validated the new name is free; a case-only change is allowed).
    pub(crate) fn rename_column_at(&mut self, idx: usize, to: String) -> Result<()> {
        let Some(col) = self.columns.get_mut(idx) else {
            return Err(PinkDbError::Execution(format!(
                "rename column index {idx} out of range for \"{}\" ({} columns)",
                self.name,
                self.columns.len()
            )));
        };
        col.name = to;
        self.version = next_table_version();
        Ok(())
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
            // Any value can widen to VARIANT (the same matrix as CAST), and a
            // VARIANT value is stored as it is.
            (Value::Variant(v), DataType::Variant) => Ok(Value::Variant(v)),
            (other, DataType::Variant) => {
                crate::variant::from_value(&other).map(|v| Value::Variant(std::sync::Arc::new(v)))
            }
            // DECIMAL/INTERVAL columns accept numeric values and string cells
            // (parsed like an implicit cast), mirroring DuckDB's INSERT/CSV
            // behavior.
            (Value::Decimal(x, from), DataType::Decimal { precision, scale }) => {
                crate::types::rescale_decimal(x, from, scale)
                    .filter(|y| crate::types::decimal_fits(*y, precision))
                    .map(|y| Value::Decimal(y, scale))
                    .ok_or_else(|| {
                        PinkDbError::Type(format!(
                            "cannot coerce {} to DECIMAL({precision},{scale}): value out of range",
                            crate::types::format_decimal(x, from)
                        ))
                    })
            }
            (Value::Integer(i), DataType::Decimal { precision, scale }) => {
                crate::types::int_to_decimal(i as i128, precision, scale)
                    .map(|x| Value::Decimal(x, scale))
                    .ok_or_else(|| {
                        PinkDbError::Type(format!(
                            "cannot coerce {i} to DECIMAL({precision},{scale}): value out of range"
                        ))
                    })
            }
            (Value::Bigint(i), DataType::Decimal { precision, scale }) => {
                crate::types::int_to_decimal(i as i128, precision, scale)
                    .map(|x| Value::Decimal(x, scale))
                    .ok_or_else(|| {
                        PinkDbError::Type(format!(
                            "cannot coerce {i} to DECIMAL({precision},{scale}): value out of range"
                        ))
                    })
            }
            (Value::Double(x), DataType::Decimal { precision, scale }) => {
                crate::types::f64_to_decimal(x, precision, scale)
                    .map(|v| Value::Decimal(v, scale))
                    .ok_or_else(|| {
                        PinkDbError::Type(format!(
                            "cannot coerce {x} to DECIMAL({precision},{scale}): value out of range"
                        ))
                    })
            }
            (Value::Varchar(s), DataType::Decimal { precision, scale }) => {
                crate::types::parse_decimal_str(&s, precision, scale)
                    .map(|x| Value::Decimal(x, scale))
                    .map_err(PinkDbError::Type)
            }
            (
                Value::Interval {
                    months,
                    days,
                    micros,
                },
                DataType::Interval,
            ) => Ok(Value::Interval {
                months,
                days,
                micros,
            }),
            (Value::Varchar(s), DataType::Interval) => match crate::types::parse_interval(&s) {
                Some(iv) => Ok(Value::Interval {
                    months: iv.months,
                    days: iv.days,
                    micros: iv.micros,
                }),
                None => Err(PinkDbError::Type(format!(
                    "cannot parse \"{s}\" as INTERVAL"
                ))),
            },
            // UUID columns take the value as it is, and a string cell is
            // parsed the way the CAST matrix parses it.
            (Value::Uuid(bits), DataType::Uuid) => Ok(Value::Uuid(bits)),
            (Value::Varchar(s), DataType::Uuid) => crate::uuid::parse(&s)
                .map(Value::Uuid)
                .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{s}\" as UUID"))),
            // ARRAY columns follow the CAST matrix: same length, any element
            // tower, and the text/JSON spellings an INSERT or CSV gives.
            (v @ Value::Array(_), t @ DataType::Array { .. })
            | (v @ Value::Varchar(_), t @ DataType::Array { .. })
            | (v @ Value::Variant(_), t @ DataType::Array { .. }) => {
                crate::exec::expr::cast_value(v, t).map_err(|e| match e {
                    PinkDbError::Execution(m) => PinkDbError::Type(m),
                    e => e,
                })
            }
            // DuckDB's assignment casts (see the binder's `can_assign`): a
            // string parsed into a scalar column, a narrowing numeric cast.
            (
                v @ (Value::Varchar(_)
                | Value::Integer(_)
                | Value::Bigint(_)
                | Value::Double(_)
                | Value::Decimal(..)),
                t @ (DataType::Boolean
                | DataType::Integer
                | DataType::Bigint
                | DataType::Double
                | DataType::Date
                | DataType::Timestamp),
            ) if matches!(v, Value::Varchar(_))
                || !matches!(t, DataType::Boolean | DataType::Date | DataType::Timestamp) =>
            {
                crate::exec::expr::cast_value(v, t).map_err(|e| match e {
                    PinkDbError::Execution(m) => PinkDbError::Type(m),
                    e => e,
                })
            }
            (
                v @ (Value::Boolean(_)
                | Value::Integer(_)
                | Value::Bigint(_)
                | Value::Double(_)
                | Value::Decimal(..)
                | Value::Date(_)
                | Value::Timestamp(_)
                | Value::Interval { .. }
                | Value::Uuid(_)),
                DataType::Varchar,
            ) => crate::exec::expr::cast_value(v, DataType::Varchar),
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
        self.require_vector_indexes_loaded()?;
        // Open a new tail chunk everywhere if the tail is full, so columns
        // stay aligned even at chunk boundaries.
        let tail_full = self.columns[0]
            .chunks
            .last()
            .map(|c| c.len() >= CHUNK_SIZE)
            .unwrap_or(true);
        if tail_full {
            for (ci, col) in self.columns.iter_mut().enumerate() {
                col.chunks
                    .push(Arc::new(Vector::with_capacity(col.dtype, CHUNK_SIZE)));
                self.zones[ci].push(Zone::empty());
            }
        }
        let start = self.num_rows;
        let mut coerced = Vec::with_capacity(self.columns.len());
        for (col, val) in self.columns.iter_mut().zip(values) {
            let value = Self::coerce(val, col.dtype)?;
            Arc::make_mut(col.chunks.last_mut().expect("tail chunk exists"))
                .push_value(value.clone())?;
            coerced.push(value);
        }
        self.num_rows += 1;
        // Exact incremental statistics: one value, one zone, one hash.
        self.stats.rows += 1;
        let chunk = self.columns[0].chunks.len() - 1;
        for (ci, value) in coerced.iter().enumerate() {
            self.stats.cols[ci].observe(value);
            self.zones[ci][chunk].observe(value);
        }
        self.version = next_table_version();
        self.extend_indexes(start)?;
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

    /// [`DataTable::append_shared_chunk`] for the `.pink` loader: the footer
    /// carries this table's statistics, so folding the loaded rows in again
    /// would repeat a full pass for nothing. The caller installs them with
    /// [`DataTable::set_loaded_stats`].
    pub(crate) fn append_loaded_chunk(&mut self, chunk: &[Arc<Vector>]) -> Result<()> {
        self.append_shared_chunk_inner(chunk, false)
    }

    /// [`DataTable::append_chunk`] for already-shared columns: appends the
    /// caller's `Arc<Vector>`s (shared, never copied) where the chunking
    /// rules allow. Validation, values, chunk boundaries and version bumps
    /// are exactly those of `append_chunk`; the only difference is that the
    /// caller's allocation is shared instead of moved into a fresh `Arc`.
    /// The lake loader uses this to hand decoded chunks straight to the
    /// destination table without deep-cloning megabytes of payload.
    pub fn append_shared_chunk(&mut self, chunk: &[Arc<Vector>]) -> Result<()> {
        self.append_shared_chunk_inner(chunk, true)
    }

    fn append_shared_chunk_inner(
        &mut self,
        chunk: &[Arc<Vector>],
        track_stats: bool,
    ) -> Result<()> {
        if chunk.len() != self.columns.len() {
            return Err(PinkDbError::Execution(format!(
                "chunk has {} columns but table \"{}\" has {}",
                chunk.len(),
                self.name,
                self.columns.len()
            )));
        }
        self.require_vector_indexes_loaded()?;
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
        let start = self.num_rows;
        // Fast paths (rows ≤ CHUNK_SIZE by the check above): the batch fills
        // an empty tail chunk, or starts a new chunk after a full one — share
        // the built vectors in without copying. The copy path below would
        // produce exactly the same chunking; bulk loaders (CSV import, lake
        // load) append CHUNK_SIZE-row batches, so after the first batch every
        // append lands behind a full tail and takes the second fast path.
        let tail_len = self.columns[0].chunks.last().map(|c| c.len()).unwrap_or(0);
        if tail_len == 0 || tail_len >= CHUNK_SIZE {
            let replacing = tail_len == 0 && !self.columns[0].chunks.is_empty();
            if replacing {
                for (col, src) in self.columns.iter_mut().zip(chunk) {
                    *col.chunks.last_mut().expect("tail chunk exists") = Arc::clone(src);
                }
            } else {
                for (col, src) in self.columns.iter_mut().zip(chunk) {
                    col.chunks.push(Arc::clone(src));
                }
            }
            if track_stats {
                for (ci, (zone, hll)) in self.fold_chunk_stats(chunk).into_iter().enumerate() {
                    self.stats.cols[ci].absorb_zone(&zone);
                    self.stats.cols[ci].set_sketch(hll.map(std::sync::Arc::new));
                    if replacing {
                        *self.zones[ci].last_mut().expect("tail zone exists") = zone;
                    } else {
                        self.zones[ci].push(zone);
                    }
                }
            }
            self.num_rows += rows;
            self.stats.rows += rows as u64;
            self.version = next_table_version();
            self.extend_indexes(start)?;
            return Ok(());
        }
        // Partial tail: merge, splitting at chunk boundaries as needed.
        let first_chunk = self.columns[0].chunks.len().saturating_sub(1);
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
                if track_stats {
                    for zones in &mut self.zones {
                        zones.push(Zone::empty());
                    }
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
        self.stats.rows += rows as u64;
        if track_stats {
            // Only the touched chunks are re-derived, and only their appended
            // source rows are hashed (folded from the whole chunk instead
            // would count the existing rows twice).
            for (ci, src) in chunk.iter().enumerate() {
                for c in first_chunk..self.columns[0].chunks.len() {
                    self.refresh_zone(ci, c, false);
                }
                if let Some(hll) = &mut self.stats.cols[ci].hll {
                    let hll = Arc::make_mut(hll);
                    for row in 0..rows {
                        hll.insert_cell(src, row);
                    }
                }
            }
        }
        self.version = next_table_version();
        self.extend_indexes(start)?;
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
            // Widening into VARIANT; the other direction needs an explicit
            // CAST because it can fail per row.
            (_, DataType::Variant) => true,
            // ARRAY: same length, element tower free (see `coerce`).
            (DataType::Array { len: a, .. }, DataType::Array { len: b, .. }) => a == b,
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
        self.require_vector_indexes_loaded()?;
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
        for (col, row, v) in &staged {
            Arc::make_mut(&mut self.columns[*col].chunks[chunk]).set_value(*row, v.clone())?;
        }
        if !staged.is_empty() {
            // The new values join the sketch and the zone is re-derived from
            // the rewritten chunk; the replaced values stay in the sketch, so
            // the distinct estimate becomes an upper bound (`dirty`).
            let mut touched: Vec<usize> = staged.iter().map(|(c, _, _)| *c).collect();
            touched.sort_unstable();
            touched.dedup();
            for col in touched {
                if let Some(hll) = &mut self.stats.cols[col].hll {
                    let hll = Arc::make_mut(hll);
                    for (c, _, v) in &staged {
                        if *c == col {
                            hll.insert_value(v);
                        }
                    }
                }
                self.refresh_zone(col, chunk, true);
            }
        }
        if !updates.is_empty() {
            self.version = next_table_version();
            self.invalidate_indexes();
            if self.key_index.is_some()
                && updates.iter().any(|(col, _, _)| {
                    self.constraints
                        .keys
                        .iter()
                        .any(|k| k.columns.contains(col))
                })
            {
                self.key_index = None;
            }
            self.update_vector_indexes(chunk, &staged)?;
        }
        Ok(())
    }

    /// An UPDATE of a vector column is a delete + insert in every graph, so
    /// the new vector is searchable immediately.
    fn update_vector_indexes(
        &mut self,
        chunk: usize,
        staged: &[(usize, usize, Value)],
    ) -> Result<()> {
        if self.vector_indexes.is_empty() {
            return Ok(());
        }
        let chunk_start = self.chunk_start(chunk);
        let mut defs = std::mem::take(&mut self.vector_indexes);
        let result = (|| -> Result<()> {
            for def in &mut defs {
                let mut rows: Vec<usize> = staged
                    .iter()
                    .filter(|(col, _, _)| *col == def.column)
                    .map(|(_, row, _)| *row)
                    .collect();
                if rows.is_empty() {
                    continue;
                }
                rows.sort_unstable();
                rows.dedup();
                let Some(column) = self.columns.get(def.column) else {
                    continue;
                };
                let crate::types::DataType::Array { len: n, .. } = column.dtype else {
                    continue;
                };
                let n = n as usize;
                let table = self.name.clone();
                let index = def.writable(&table)?;
                for row in rows {
                    let global = chunk_start + row as u64;
                    index.delete(global);
                    if let Some(vector) =
                        crate::vector_index::row_vector(&column.chunks[chunk], row, n)
                    {
                        index.insert(global, &vector)?;
                    }
                }
            }
            Ok(())
        })();
        self.vector_indexes = defs;
        result
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
        self.require_vector_indexes_loaded()?;
        let len = self.columns[0].chunks[chunk].len();
        let chunk_start = self.chunk_start(chunk);
        let chunks_before = self.columns[0].chunks.len();
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
        let dropped_chunk = self.columns[0].chunks[chunk].is_empty();
        if dropped_chunk {
            for col in &mut self.columns {
                col.chunks.remove(chunk);
            }
        }
        if removed > 0 {
            // Deletes can only widen bounds (the sketch cannot subtract, so
            // every column's distinct estimate becomes an upper bound). A
            // dropped chunk is the one case that shrinks them: re-derive the
            // column from its remaining zones.
            for col in 0..self.columns.len() {
                if dropped_chunk {
                    self.zones[col].remove(chunk);
                    self.rebuild_column_stats(col);
                    self.stats.cols[col].dirty = true;
                } else {
                    self.refresh_zone(col, chunk, true);
                }
            }
        }
        self.num_rows -= removed;
        self.stats.rows -= removed as u64;
        self.key_index = None;
        if removed > 0 {
            self.version = next_table_version();
            self.invalidate_indexes();
            let shift = crate::vector_index::DeleteShift {
                chunk_start,
                chunk_len: len as u64,
                deleted: drop_sorted,
                chunk_dropped: self.columns[0].chunks.len() < chunks_before,
            };
            let table = self.name.clone();
            for def in &mut self.vector_indexes {
                def.writable(&table)?.remap_after_delete(&shift);
            }
        }
        Ok(removed)
    }

    /// Number of stored chunks (all columns share the same count).
    pub fn num_chunks(&self) -> usize {
        self.columns.first().map(|c| c.chunks.len()).unwrap_or(0)
    }

    /// Global row id of the first row in `chunk`. Deleting rows leaves
    /// interior chunks partially full, so this cannot assume full chunks
    /// before the tail.
    fn chunk_start(&self, chunk: usize) -> u64 {
        self.columns[0].chunks[..chunk]
            .iter()
            .map(|c| c.len() as u64)
            .sum()
    }

    /// Read chunk `idx` as a `DataChunk` (columns shared by reference —
    /// scans never copy data).
    pub fn scan_chunk(&self, idx: usize) -> Option<DataChunk> {
        if idx >= self.num_chunks() {
            return None;
        }
        let cols = self.columns.iter().map(|c| c.chunks[idx].clone()).collect();
        let mut chunk = DataChunk::from_arcs(cols)?;
        // Volatile expressions read the ordinal: a chunk read straight from
        // storage (an UPDATE, a trigger) is still the idx-th chunk in scan
        // order, which is what a plan-driven scan would stamp.
        chunk.ordinal = idx;
        Some(chunk)
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
/// underlying tables surface as errors, never stale plans). A `.pink` footer
/// does not carry views (they are session-local to `--file`), but the engine
/// catalog image does, from the statement text in `sql` (wave 96b).
#[derive(Debug, Clone, PartialEq)]
pub struct ViewDef {
    pub name: String,
    pub query: Select,
    pub cols: Option<Vec<String>>,
    /// The `CREATE VIEW` statement text, stamped by the executor. Empty for a
    /// view created through `create_view_in` without a statement (a test or an
    /// embedder); such a view cannot be written to a catalog image.
    pub sql: String,
}

/// A materialized view: a cached query result stored as a regular table, plus
/// the defining query used by `REFRESH MATERIALIZED VIEW` and the refresh
/// policy that schedules automatic recomputation (wave 80).
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedViewDef {
    pub name: String,
    /// The bound defining query, evaluated by every refresh. `None` when the
    /// definition could not be rebound (the file names a base table or view
    /// that does not exist at load time); a refresh then retries rebinding
    /// `query_sql` and reports the failure if it still cannot bind.
    pub query: Option<Box<crate::plan::LogicalPlan>>,
    /// The defining `CREATE MATERIALIZED VIEW ...` statement text, captured at
    /// parse time and persisted in `.pink` (a bound plan cannot be rendered
    /// back to SQL). Also the rebuild source when `query` is `None`.
    pub query_sql: String,
    /// When (and whether) the view recomputes itself at statement boundaries.
    pub policy: crate::types::RefreshPolicy,
    /// Unix epoch **milliseconds** of the last successful refresh. Creation
    /// counts as the initial computation.
    pub last_refreshed_ms: i64,
    /// Unix epoch milliseconds of the last refresh *attempt* (success or
    /// failure): an `EVERY` view retries at most once per interval, so a
    /// broken view cannot burn a full evaluation on every statement.
    pub last_attempt_ms: i64,
    /// Successful refreshes since creation (creation itself does not count).
    pub refresh_count: u64,
    /// The most recent *automatic* refresh failure, cleared by the next
    /// success. The manual `REFRESH` statement reports its error directly and
    /// leaves this field untouched.
    pub last_error: Option<String>,
}

/// One schema's catalog: the tables, views and other objects it holds.
///
/// Wave 131 added the level DuckDB calls a schema (`catalog.schema.object`):
/// two schemas of one database may each hold a table of the same name. The
/// design follows DuckDB's `SchemaCatalogEntry`
/// (`src/catalog/catalog_entry/duck_schema_entry.cpp`).
#[derive(Debug, Default, Clone)]
pub struct SchemaCatalog {
    /// Display name as created (the owning `DbCatalog::schemas` key is the
    /// lowercased name).
    pub(crate) name: String,
    pub(crate) tables: HashMap<String, DataTable>,
    pub(crate) views: HashMap<String, ViewDef>,
    pub(crate) materialized_views: HashMap<String, MaterializedViewDef>,
    /// Stored procedures, keyed by lowercased name.
    pub(crate) procedures: HashMap<String, crate::procedure::ProcedureDef>,
    /// SQL macros (wave 77), keyed by lowercased name. Macros live in their
    /// own namespace: a macro may share a name with a table or view, exactly
    /// like DuckDB (a scalar/table macro name is one namespace).
    pub(crate) macros: HashMap<String, crate::macros::MacroDef>,
    /// Triggers (wave 100) in **creation order**: that order is the firing
    /// order, so it must survive a save/open round trip and a WAL replay.
    /// The order is per schema, like every other object.
    pub(crate) triggers: Vec<crate::trigger::TriggerDef>,
}

impl SchemaCatalog {
    /// An empty schema catalog.
    pub(crate) fn empty(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Self::default()
        }
    }

    /// Trigger definitions are only ever replaced by `OR REPLACE` or removed
    /// by `DROP TRIGGER`: the list is short, so a linear scan is the lookup.
    fn trigger_index(&self, name: &str) -> Option<usize> {
        self.triggers
            .iter()
            .position(|t| t.name.eq_ignore_ascii_case(name))
    }

    /// Does the schema hold any object at all? (`DROP SCHEMA` without
    /// `CASCADE` refuses a non-empty schema.)
    fn is_empty(&self) -> bool {
        self.tables.is_empty()
            && self.views.is_empty()
            && self.materialized_views.is_empty()
            && self.procedures.is_empty()
            && self.macros.is_empty()
            && self.triggers.is_empty()
    }
}

/// A namespace's fully qualified name: `database.schema`, the first two of
/// DuckDB's three name parts (wave 131).
///
/// Both parts are stored lowercased: catalog names are case-insensitive and
/// the store's keys are lowercased. A bare database name means that database's
/// `main` schema, which is where every object of a version-3 store lives.
///
/// The struct is `Ord`/`Hash` so it can key maps and order checkpoints. Use
/// its `Display` for a message: identifiers are quoted only when they would
/// not re-parse as written (the `rbac::quote_ident` rule).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaPath {
    db: String,
    schema: String,
}

impl SchemaPath {
    /// The namespace `db.schema` (both parts lowercased).
    pub fn new(db: &str, schema: &str) -> Self {
        Self {
            db: db.to_ascii_lowercase(),
            schema: schema.to_ascii_lowercase(),
        }
    }

    /// The database's default schema, `db.main`.
    pub fn main(db: &str) -> Self {
        Self::new(db, "main")
    }

    /// The lowercased database name.
    pub fn db(&self) -> &str {
        &self.db
    }

    /// The lowercased schema name.
    pub fn schema(&self) -> &str {
        &self.schema
    }
}

impl std::fmt::Display for SchemaPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}",
            crate::rbac::quote_ident(&self.db),
            crate::rbac::quote_ident(&self.schema)
        )
    }
}

impl From<&str> for SchemaPath {
    fn from(db: &str) -> Self {
        Self::main(db)
    }
}

impl From<&String> for SchemaPath {
    fn from(db: &String) -> Self {
        Self::main(db)
    }
}

impl From<String> for SchemaPath {
    fn from(db: String) -> Self {
        Self::main(&db)
    }
}

impl From<&SchemaPath> for SchemaPath {
    fn from(ns: &SchemaPath) -> Self {
        ns.clone()
    }
}

/// A table or view addressed by its namespace and its lowercased name: the
/// form the plan's read/write sets, the materialized-view dirty set and the
/// in-process commit check carry.
pub(crate) type TableRef = (SchemaPath, String);

/// One database's catalog: its schemas, and where its rows live.
#[derive(Debug, Clone)]
pub struct DbCatalog {
    /// Display name as created (the map key is the lowercased name).
    pub(crate) name: String,
    /// The database's schemas, keyed by lowercased name. A database always
    /// holds `main`: [`DbCatalog::empty`] creates it and `drop_schema`
    /// refuses to remove it.
    pub(crate) schemas: BTreeMap<String, SchemaCatalog>,
    /// Where the database's rows live (wave 124): the store, or a lake.
    /// Persisted in the checkpoint footer.
    pub(crate) storage: crate::lake_db::DbStorage,
    /// A lake database's version as this catalog reflects it (the pinned or
    /// last published lake version); 0 for a local database. Process-local:
    /// a reopen reads the lake's own.
    pub(crate) lake_version: u64,
}

/// A database always holds schema `main`, so even the `..Default::default()`
/// construction paths (`create_database`, a reload into an existing store)
/// establish it.
impl Default for DbCatalog {
    fn default() -> Self {
        Self {
            name: String::new(),
            schemas: BTreeMap::from([("main".to_string(), SchemaCatalog::empty("main"))]),
            storage: crate::lake_db::DbStorage::Local,
            lake_version: 0,
        }
    }
}

impl DbCatalog {
    /// An empty database catalog: no objects, one schema `main`.
    pub(crate) fn empty(name: &str, storage: crate::lake_db::DbStorage) -> Self {
        let mut schemas = BTreeMap::new();
        schemas.insert("main".to_string(), SchemaCatalog::empty("main"));
        Self {
            name: name.to_string(),
            schemas,
            storage,
            lake_version: 0,
        }
    }
}

/// A store of databases.
///
/// Holds an optional transaction snapshot (`BEGIN` clones the whole store;
/// `ROLLBACK` restores it, `COMMIT` drops it). Single connection, so
/// transactions give multi-statement atomicity, not isolation.
///
/// Databases are named, case-insensitive namespaces (default `main`).
/// The `SET VARIABLE` state of one client.
///
/// A session is one client, not one store: the embedded [`PinkDb`] handle is a
/// single session, and every network [`crate::Connection`] owns its own map, so
/// one client's variables are invisible to — and cannot be overwritten by —
/// another's queries. Clones share the map (a transaction's private snapshot
/// runs in the session that opened it).
///
/// Variables are session state, not table data: they are not transactional
/// (a `ROLLBACK` keeps them, like DuckDB), never persisted, and never written
/// to the WAL. A statement the engine logs may not *read* one (see
/// `sql::bind_tracked_with`): replay re-binds the logged text against a fresh
/// session, so a reference there could not reproduce its value.
#[derive(Clone, Default)]
pub struct Session {
    /// Lowercased name -> value.
    vars: Arc<Mutex<BTreeMap<String, Value>>>,
    /// The authenticated user of this session (wave 48's login), lowercased:
    /// what `current_user()` answers and what `current_roles()` resolves.
    /// `None` is the trusted embedded session, whose implicit principal is
    /// [`IMPLICIT_PRINCIPAL`].
    principal: Arc<Mutex<Option<String>>>,
    /// The roles `current_roles()` answers when the identity was adopted
    /// from another store (wave 121e): a host's state store has no users, so
    /// its connection carries the caller's roles from the warehouse instead
    /// of resolving them in its own (empty) user catalog.
    roles: Arc<Mutex<Option<Vec<String>>>>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Values can hold anything a query can (a secret, a tenant filter);
        // never render them into a log line or an error. The principal is not
        // a secret and is what a `current_user()` bug report needs.
        f.debug_struct("Session")
            .field("variables", &self.len())
            .field("principal", &self.principal())
            .finish()
    }
}

impl Session {
    /// A session with no variables set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adopt `principal` as this session's user (`None` = trusted embedded).
    pub(crate) fn set_principal(&self, principal: Option<String>) {
        *self.principal.lock().unwrap_or_else(|e| e.into_inner()) = principal;
    }

    /// Adopt the role display names `current_roles()` answers (`None` =
    /// resolve them in this store's user catalog).
    pub(crate) fn set_roles(&self, roles: Option<Vec<String>>) {
        *self.roles.lock().unwrap_or_else(|e| e.into_inner()) = roles;
    }

    /// The adopted roles, if the identity came from another store.
    pub(crate) fn adopted_roles(&self) -> Option<Vec<String>> {
        self.roles.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The session's user, `None` on a trusted embedded session.
    pub(crate) fn principal(&self) -> Option<String> {
        self.principal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The value bound to `name` (case-insensitive), if the session set it.
    pub fn get(&self, name: &str) -> Option<Value> {
        self.lock().get(&name.to_ascii_lowercase()).cloned()
    }

    /// Bind `name` for the rest of the session; a later `SET` replaces it.
    pub fn set(&self, name: &str, value: Value) {
        self.lock().insert(name.to_ascii_lowercase(), value);
    }

    /// How many variables the session holds.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// `true` when the session set no variables.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Value>> {
        // A panic elsewhere must not turn every later statement into a panic.
        self.vars.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The volatile-function state of the WAL record being filled.
///
/// A record — an autocommit statement, a transaction, a procedure segment, a
/// trigger's refire, a view refresh — records one seed per statement that drew
/// a volatile value, in the order the statements ran. That order is the whole
/// key: a replay runs the record's statements in the same order (a procedure
/// body and a trigger action re-execute exactly as they did) and takes the
/// seeds back in it, so two identical statements of one record still draw
/// different values and nothing about statement positions has to survive the
/// WAL.
///
/// A mutex, not atomics, because the slots form a sequence and a bind only has
/// `&PinkDb`: the state needs interior mutability, and this is never on a hot
/// path.
#[derive(Debug, Default)]
pub(crate) struct RecordSeed {
    state: std::sync::Mutex<RecordState>,
}

#[derive(Debug, Default, Clone)]
struct RecordState {
    /// One slot per statement that could draw a volatile value, in execution
    /// order. It stays empty until the statement's first seeded call, so a
    /// statement that calls none consumes no recorded seed.
    slots: Vec<Option<u128>>,
    /// While replaying, the record's seeds in the order it recorded them; each
    /// logged statement takes the next one, so no statement position has to be
    /// carried across the WAL.
    replay: std::collections::VecDeque<u128>,
}

impl Clone for RecordSeed {
    fn clone(&self) -> Self {
        Self {
            state: std::sync::Mutex::new(self.lock().clone()),
        }
    }
}

impl RecordSeed {
    fn lock(&self) -> std::sync::MutexGuard<'_, RecordState> {
        // A panic elsewhere must not turn every later statement into a panic.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Start a new record: no statement, no seeds to replay.
    pub(crate) fn clear(&self) {
        *self.lock() = RecordState::default();
    }

    /// Load the seeds a record being replayed recorded, in order.
    fn set_replay(&self, seeds: impl IntoIterator<Item = u128>) {
        let mut state = self.lock();
        state.slots.clear();
        state.replay = seeds.into_iter().collect();
    }

    /// Open the slot of the statement about to execute.
    fn open_slot(&self) {
        self.lock().slots.push(None);
    }

    /// The seed for the value being evaluated: this statement's seed if it
    /// already drew one, else a fresh one — or, while replaying, the next seed
    /// its record recorded.
    fn seed(&self, replay: bool) -> Result<u128> {
        let mut state = self.lock();
        if state.slots.is_empty() {
            // A read-only statement has no record to replay: its values only
            // need to differ from each other.
            return crate::sql::binder::fresh_seed();
        }
        if let Some(seed) = state.slots.last().copied().flatten() {
            return Ok(seed);
        }
        let seed = if replay {
            match state.replay.pop_front() {
                Some(seed) => seed,
                // Fail closed: the store, its clients and any lake may hold
                // the values that seed produced, so a fresh draw would make
                // the replayed tables disagree with what was committed.
                None => {
                    return Err(PinkDbError::Execution(
                        "WAL replay failed: the record does not carry a seed for a statement that \
                         draws volatile values"
                            .to_string(),
                    ));
                }
            }
        } else {
            crate::sql::binder::fresh_seed()?
        };
        if let Some(slot) = state.slots.last_mut() {
            *slot = Some(seed);
        }
        Ok(seed)
    }

    /// Take the memo entries for the record that is being appended: one per
    /// statement that used a seeded function, in execution order. Replay reads
    /// them back in the same order, so the position is informational.
    pub(crate) fn take_memos(&self) -> Vec<crate::wal::ExtCall> {
        let slots = std::mem::take(&mut self.lock().slots);
        slots
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(position, seed)| crate::sql::binder::seed_memo(position, seed))
            .collect()
    }
}

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
    pub(crate) databases: HashMap<String, DbCatalog>,
    /// Loadable-extension scalar functions (ABI v1 `dlopen`, wave 29A; linked
    /// registry, wave 49), keyed by lowercased name. Registered by
    /// [`PinkDb::load_extension`] / [`PinkDb::load_linked_extension`]; shared
    /// by every clone of the store. Empty for a stock engine, in which case
    /// the binder/executor behave exactly as before. Native targets hold the
    /// `dlopen` variant, wasm targets the linked one; the field itself is
    /// target-independent.
    pub(crate) dynamic_functions: HashMap<String, ExtFunction>,
    /// Every successfully loaded extension, keyed by lowercased declared
    /// name. Holds the handle, so an extension that registered no functions
    /// still stays alive (natively the library stays mapped); also pins
    /// duplicate loads by name.
    pub(crate) loaded_extensions: HashMap<String, ExtHandle>,
    /// Lowercased name of the embedded session's current database.
    pub(crate) current_db: String,
    /// The embedded session's current schema (wave 131b). `current_db` above
    /// is kept as the database part so the pre-schema call sites stay cheap.
    pub(crate) current_ns: SchemaPath,
    /// The embedded handle's session (see [`Session`]): `SET VARIABLE` state
    /// for this client only. A network [`crate::Connection`] overrides it with
    /// its own session for every statement it runs.
    pub(crate) session: Session,
    pub(crate) txn_snapshot: Option<Box<PinkDb>>,
    /// Durable WAL handle, shared by every clone of this store.
    pub(crate) wal: Option<Arc<Mutex<Wal>>>,
    /// The per-operation write lease of a durable store (wave 45m): the lock
    /// file fd, the in-process gate, the busy timeout, and the epoch cache.
    /// Shared by every clone; `None` in memory.
    pub(crate) lease: Option<Arc<crate::wal::Lease>>,
    /// The optimistic base of an embedded durable transaction
    /// (`begin` .. `commit`/`rollback`), registered to keep the frame journal
    /// from being pruned below it. No lease is held while the transaction
    /// runs (wave 45o): `commit` takes it, validates against the frames
    /// committed after this base, and only then appends. Never cloned into
    /// snapshots.
    pub(crate) txn_base: Option<crate::wal::BaseToken>,
    /// Every catalog object the open transaction's statements read or wrote
    /// (plus the databases its `USE` statements switched into): what `commit`
    /// validates against the frames other writers committed.
    pub(crate) txn_touched: crate::wal::FrameSets,
    /// SQL text of mutating statements executed since `BEGIN` of a durable
    /// transaction; flushed as one WAL record by [`PinkDb::commit`].
    wal_txn: Vec<String>,
    /// Session database before the first statement of the open transaction's
    /// WAL record (the record replays in that session).
    wal_txn_db: Option<SchemaPath>,
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
    /// The buffered statements' touched-object set (wave 45o): stored in the
    /// transaction's one WAL frame so optimistic commits elsewhere can
    /// validate against it.
    wal_txn_sets: crate::wal::FrameSets,
    /// At-rest key this store was opened/configured with. Checkpoints and
    /// snapshots of a keyed store are written encrypted (feature
    /// `encryption`); [`PinkDb::save`] stays a plaintext explicit API.
    pub(crate) encryption_key: Option<EncryptionKey>,
    /// Whether the SQL `LOAD EXTENSION` statement may run (default
    /// **false**: loading an extension runs native code, so it is opt-in
    /// everywhere — the embedded API, the C ABI, the CLI and the server).
    /// The Rust [`PinkDb::load_extension`] API is a separate, explicit
    /// choice by the host and is not gated by this switch.
    pub(crate) allow_sql_load_extension: bool,
    /// Whether WAL replay may load an extension that the WAL records
    /// (default **false**). A durable store that used an extension no
    /// longer loads it on open: the recorded path is resolved against the
    /// rules in [`crate::ext`], and unless the extension is already loaded
    /// in this process (or the operator opted in *and* configured an
    /// extension root) the open fails with an actionable error. Never
    /// silently skipped, never auto-loaded.
    pub(crate) allow_extension_replay: bool,
    /// The directory every extension load is confined to, when set
    /// (`None` = no root configured; the loader's own path rules still
    /// apply). A path outside the root is refused.
    pub(crate) extension_root: Option<std::path::PathBuf>,
    /// True only while [`PinkDb::replay_frames`] executes recovered WAL
    /// records (hazard H5): a replayed `LOAD EXTENSION` prefix may name an
    /// extension this process already has loaded (its own earlier record, or
    /// a record the current generation loaded before a checkpoint rolled it
    /// away), and the WAL intent is "the extension must be available", so an
    /// exact path match is a no-op instead of the user-facing "already
    /// loaded" error. Normal SQL keeps the strict duplicate-load refusal.
    pub(crate) replaying: bool,
    /// The position of the record statement being replayed. A statement's
    /// volatile seed is keyed by it in the record's memo.
    /// The seed and statement slots of the WAL record being filled (see
    /// [`RecordSeed`]).
    pub(crate) volatile: RecordSeed,
    /// Recorded results of the `external_io` host scalars of the WAL record
    /// being replayed (wave 107c), keyed by (function, arguments) inside the
    /// record. `None` outside a replay: a live statement records instead.
    ext_replay: Option<std::sync::Arc<crate::wal::ReplayMemo>>,
    /// External-call memo the open transaction's buffered statements produced
    /// (wave 107c); flushed as part of the transaction's one WAL record.
    wal_txn_memo: Vec<crate::wal::ExtCall>,
    /// Whether this session may call `external_io` host scalars: the serve
    /// policy's `allow-files` switch (wave 107c). `true` for the embedded
    /// API and the CLI; the server and MCP set it per session so a read-only
    /// session cannot reach a paid service through a macro, view or trigger
    /// body that binds only after the statement starts.
    external_services: bool,
    /// Attached read-only lakes (`.lake attach`): session-local, additive to
    /// the visible catalog for reads only — never part of the store's own
    /// tables and never touched by `.save`, `.lake save`, or the WAL.
    pub(crate) attached_lakes: Vec<Arc<dyn crate::attached::AttachedLake>>,
    /// The host's state store (wave 121e): a separate store for the host's own
    /// bookkeeping — an extension's internal channel (the agent's transcript,
    /// the memory layers) and the console's worksheets and credentials. Never
    /// listed, read or written by a statement on this store. Process-level
    /// like attached lakes: never persisted.
    pub(crate) state_store: Option<crate::db::Database>,
    /// Host-registered table sources (wave 70; unified with the arrow
    /// extension's reads in wave 71): every file read surface — `read_parquet`
    /// / `read_arrow` / `FROM 'file.<ext>'`. Process-level like extensions and
    /// attached lakes — never persisted, and **seedable from a process-global
    /// registration** so a durable store's replay can resolve a file-reading
    /// record (replay re-reads the file, DuckDB's rule; see
    /// [`crate::table_source`]).
    pub(crate) table_sources: Vec<Arc<dyn crate::table_source::TableSource>>,
    /// `(database, table)` pairs written by *committed* units (autocommit
    /// statements and COMMITs) since the last statement-boundary refresh pass
    /// (wave 80): the ON COMMIT trigger's dirty set. Process-local, never
    /// persisted, cleared by the pass — see `crate::mv`.
    pub(crate) mv_pending: std::collections::HashSet<TableRef>,
    /// `(database, table)` pairs written by the open embedded transaction:
    /// moved into `mv_pending` at COMMIT, discarded at ROLLBACK (a rolled-back
    /// write must not trigger an ON COMMIT refresh).
    pub(crate) txn_writes: std::collections::HashSet<TableRef>,
    /// The `CALL` nesting depth of the statement currently executing (wave
    /// 44): 0 outside a call. Bounds recursion ([`crate::procedure::
    /// MAX_CALL_DEPTH`]) and decides where a call's statement-scoped frame
    /// bookkeeping begins/ends. Never persisted.
    pub(crate) proc_depth: usize,
    /// True while the statement runner executes a `CALL` with no enclosing
    /// transaction (wave 83): the only context where the outermost
    /// procedure body may `COMMIT`/`ROLLBACK`. Set and cleared around one
    /// statement by the runners; never persisted.
    pub(crate) standalone_call: bool,
    /// What the `CALL` currently executing actually touched (wave 44): every
    /// table its statements read or wrote, plus the writes alone (for the
    /// ON COMMIT trigger). Cleared at each statement boundary — the call's
    /// logging/conflict paths read it right after the call ran (see
    /// [`crate::procedure`]).
    pub(crate) proc_frame: crate::procedure::ProcFrame,
    /// The store's users, roles and grants (wave 48). Store-global (a table
    /// privilege names its database) and persisted in the `.pink` format's
    /// RBAC section + replayed from the WAL; an empty catalog is the
    /// documented "no users: every connection is trusted" state.
    pub(crate) rbac: crate::rbac::RbacCatalog,
    /// The row images and transition tables a trigger body sees (wave 100),
    /// installed by [`crate::trigger`] while the body runs and read back by
    /// the binder (as `NEW`/`OLD` variables and `FROM <alias>`). Never
    /// persisted, never serialized.
    pub(crate) trigger_scope: Option<crate::trigger::TriggerScope>,
    /// The rows an `AFTER STATEMENT` trigger's transition tables need, filled
    /// by the DML arms of the statement that is firing and drained before the
    /// trigger runs. `None` = no AFTER STATEMENT trigger is waiting, so a
    /// writing statement does not pay for the copy.
    pub(crate) trigger_capture: Option<Vec<(Vec<Value>, Vec<Value>)>>,
    /// Rows of the data-modifying CTEs of the statement currently executing
    /// (wave 95b). A CTE's writes must run before the query that reads them,
    /// but the row evaluator cannot write; the statement entry point runs them
    /// first and leaves their result sets here for the `Cte` plan node.
    pub(crate) dml_cte_rows:
        Option<std::sync::Arc<std::collections::HashMap<String, crate::exec::Output>>>,
    /// The trigger nesting depth of the statement currently executing (0
    /// outside a trigger): bounds recursion. Never persisted.
    pub(crate) trigger_depth: usize,
    /// The authenticated user of the statement currently executing (wave 100):
    /// a trigger body is checked against *this* principal — invoker rights, not
    /// the definer's. `None` is a trusted embedded session.
    pub(crate) session_principal: Option<String>,
    /// The store's lake databases (wave 124): the open lakes behind them, the
    /// managed-lake directory and the host's location policy. Shared by
    /// store clones and never persisted (the footer persists each database's
    /// [`crate::lake_db::DbStorage`]).
    pub(crate) lakes: crate::lake_db::LakeRuntime,
    /// The storages the open embedded transaction wrote so far (wave 124):
    /// a transaction writes the local store or one lake database.
    pub(crate) txn_storages: std::collections::BTreeSet<crate::lake_db::StorageKey>,
    /// While a trigger body (or anything it calls) runs: the principal every
    /// statement it binds is checked against — invoker rights, never a
    /// definer's. `None` outside a trigger body; an empty string is a trigger
    /// fired by a trusted embedded session, which skips the check.
    pub(crate) trigger_invoker: Option<String>,
    /// Registered `COPY … (FORMAT <name>)` write sinks (wave 71): a sink
    /// resolves by name or by the target path's extension. Process/store-level
    /// like loadable extensions — never persisted, shared by store clones,
    /// empty in a stock engine (COPY then fails with a clear "no file format
    /// …" error). Reads are the separate table-source registry above. See
    /// [`crate::filefmt`].
    pub(crate) file_formats: Vec<Arc<dyn crate::filefmt::FileFormat>>,
    /// Extension packages active on this store (wave 86), keyed by lowercased
    /// package name; the value is the declared name. Process-level like
    /// extensions: cloned with the store, never persisted, and a store clone
    /// (transaction snapshot) shares them. A second load of a key is refused.
    pub(crate) loaded_packages: HashMap<String, String>,
    /// The package that installed each active table source, keyed by the
    /// source's lowercased registry name. The binder records the package so
    /// a logged statement that reads the source is prefixed with its
    /// `LOAD EXTENSION` when the current WAL generation has not loaded it.
    pub(crate) package_table_sources: HashMap<String, String>,
    /// The package that installed each active file format, keyed by the
    /// format's lowercased name (the COPY-sink half of
    /// [`Self::package_table_sources`]).
    pub(crate) package_file_formats: HashMap<String, String>,
    /// The package that registered each dynamic scalar function, keyed by
    /// the function's lowercased name. A package's scalars register like a
    /// linked extension (whose source is the linked extension's own name), so
    /// this map keeps the WAL prefix pointing at the **package** — the name
    /// replay can resolve.
    pub(crate) package_dynamic_functions: HashMap<String, String>,
    /// The package that installed each active vector index provider, keyed
    /// by the provider's lowercased name. A `CREATE INDEX … USING <provider>`
    /// found it so its WAL record is prefixed with the package's
    /// `LOAD EXTENSION`, which replay needs before the provider exists.
    pub(crate) package_vector_providers: HashMap<String, String>,
    /// Vector index providers (wave 107b) registered through an extension
    /// package, in load order. Process-level like the host scalars: cloned
    /// with the store, never persisted.
    pub(crate) vector_providers: Vec<Arc<dyn crate::vector_index::VectorIndexProvider>>,
    /// Active host scalars and host statements (wave 108a), keyed by
    /// lowercased name. The package that installed each is inside the value.
    pub(crate) host_scalars: HashMap<String, crate::package::HostScalar>,
    pub(crate) host_statements: HashMap<String, crate::package::HostStatement>,
    /// Process-unique identity of this store, shared by its clones (a clone
    /// is a snapshot of the same store). An in-process cache — the fts
    /// extension's loaded index — keys on it so two open stores never share
    /// entries. Never persisted.
    pub(crate) store_id: u64,
    /// Where SQL `COPY … TO` may write (M-3). Embedded stores default to
    /// [`crate::CopyToPolicy::Unrestricted`]; a network server installs
    /// `Disabled` at bind time and the `--allow-copy-to --copy-root` opt-in
    /// installs `Confined(root)`.
    pub(crate) copy_to_policy: crate::CopyToPolicy,
    /// Rows the streaming scan→aggregate path (wave 98c) holds right now, and
    /// the peak that ever reached: the executor folds the chunk a scan hands
    /// it and drops it, so the peak stays at one chunk however large the input
    /// is. Counters only — nothing reads them for control flow — and shared by
    /// store clones like the scan counters they explain.
    pub(crate) stream_buffered_rows: std::sync::atomic::AtomicUsize,
    pub(crate) stream_peak_rows: std::sync::atomic::AtomicUsize,
    /// Rows folded through that path so far.
    pub(crate) stream_folded_rows: std::sync::atomic::AtomicU64,
}

impl Default for PinkDb {
    fn default() -> Self {
        Self::with_databases(["main"])
    }
}

/// A fresh store identity (see [`PinkDb::store_id`]).
fn next_store_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

impl Clone for PinkDb {
    /// Clone the store for a transaction snapshot / multi-writer copy. The
    /// copy shares the WAL handle and the lease *handle* (`Arc`s) but never
    /// inherits `txn_base`: an open transaction's base belongs to exactly the
    /// store that captured it, so dropping a snapshot can never unregister
    /// another store's base.
    fn clone(&self) -> Self {
        Self {
            databases: self.databases.clone(),
            dynamic_functions: self.dynamic_functions.clone(),
            loaded_extensions: self.loaded_extensions.clone(),
            current_db: self.current_db.clone(),
            current_ns: self.current_ns.clone(),
            // Clones share the session: a transaction's private snapshot runs
            // in the session that opened it.
            session: self.session.clone(),
            txn_snapshot: self.txn_snapshot.clone(),
            wal: self.wal.clone(),
            lease: self.lease.clone(),
            txn_base: None,
            txn_touched: crate::wal::FrameSets::default(),
            wal_txn: self.wal_txn.clone(),
            wal_txn_db: self.wal_txn_db.clone(),
            wal_txn_ext_paths: self.wal_txn_ext_paths.clone(),
            wal_txn_ext_loaded: self.wal_txn_ext_loaded.clone(),
            wal_txn_sets: self.wal_txn_sets.clone(),
            encryption_key: self.encryption_key.clone(),
            allow_sql_load_extension: self.allow_sql_load_extension,
            allow_extension_replay: self.allow_extension_replay,
            extension_root: self.extension_root.clone(),
            replaying: false,
            volatile: self.volatile.clone(),
            // A clone is a snapshot, never the record being replayed, and
            // the memo of an open transaction belongs to the store that
            // opened it (the committing clone flushes it).
            // A snapshot keeps the replay memo: a body's `ROLLBACK` restores
            // a snapshot taken *during* the replay, and the rest of the
            // record still needs the memo.
            ext_replay: self.ext_replay.clone(),
            wal_txn_memo: self.wal_txn_memo.clone(),
            external_services: self.external_services,
            attached_lakes: self.attached_lakes.clone(),
            state_store: self.state_store.clone(),
            table_sources: self.table_sources.clone(),
            mv_pending: self.mv_pending.clone(),
            txn_writes: HashSet::new(),
            // A clone is a snapshot for one statement/transaction: it keeps
            // the in-flight `CALL` depth (a call's own rollback restores
            // exactly the depth it ran at) and the statement-scoped procedure
            // frame (the caller's `proc_frame_reset` clears it at the next
            // boundary).
            proc_depth: self.proc_depth,
            standalone_call: self.standalone_call,
            proc_frame: self.proc_frame.clone(),
            // A trigger body owns these fields while it runs; a snapshot only
            // records the depth it was taken at.
            trigger_scope: self.trigger_scope.clone(),
            trigger_capture: self.trigger_capture.clone(),
            dml_cte_rows: self.dml_cte_rows.clone(),
            trigger_depth: self.trigger_depth,
            session_principal: self.session_principal.clone(),
            lakes: self.lakes.clone(),
            txn_storages: self.txn_storages.clone(),
            trigger_invoker: self.trigger_invoker.clone(),
            rbac: self.rbac.clone(),
            file_formats: self.file_formats.clone(),
            loaded_packages: self.loaded_packages.clone(),
            package_table_sources: self.package_table_sources.clone(),
            package_file_formats: self.package_file_formats.clone(),
            package_dynamic_functions: self.package_dynamic_functions.clone(),
            package_vector_providers: self.package_vector_providers.clone(),
            vector_providers: self.vector_providers.clone(),
            host_scalars: self.host_scalars.clone(),
            host_statements: self.host_statements.clone(),
            store_id: self.store_id,
            copy_to_policy: self.copy_to_policy.clone(),
            stream_buffered_rows: std::sync::atomic::AtomicUsize::new(
                self.stream_buffered_rows
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            stream_peak_rows: std::sync::atomic::AtomicUsize::new(
                self.stream_peak_rows
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            stream_folded_rows: std::sync::atomic::AtomicU64::new(
                self.stream_folded_rows
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
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
            dynamic_functions: HashMap::new(),
            loaded_extensions: HashMap::new(),
            current_db: "main".to_string(),
            current_ns: SchemaPath::main("main"),
            session: Session::default(),
            txn_snapshot: None,
            wal: None,
            lease: None,
            txn_base: None,
            txn_touched: crate::wal::FrameSets::default(),
            wal_txn: Vec::new(),
            wal_txn_db: None,
            wal_txn_ext_paths: Vec::new(),
            wal_txn_ext_loaded: Vec::new(),
            wal_txn_sets: crate::wal::FrameSets::default(),
            encryption_key: None,
            allow_sql_load_extension: false,
            allow_extension_replay: false,
            extension_root: None,
            replaying: false,
            volatile: RecordSeed::default(),
            ext_replay: None,
            wal_txn_memo: Vec::new(),
            external_services: true,
            attached_lakes: Vec::new(),
            state_store: None,
            // Process-global table sources (`table_source::
            // register_global_table_source`) seed every store constructed
            // after the registration; the store then owns its list (durable
            // replay resolves through it).
            table_sources: crate::table_source::seeded_global_table_sources(),
            mv_pending: HashSet::new(),
            txn_writes: HashSet::new(),
            proc_depth: 0,
            standalone_call: false,
            proc_frame: crate::procedure::ProcFrame::default(),
            trigger_scope: None,
            trigger_capture: None,
            dml_cte_rows: None,
            trigger_depth: 0,
            session_principal: None,
            lakes: crate::lake_db::LakeRuntime::default(),
            txn_storages: std::collections::BTreeSet::new(),
            trigger_invoker: None,
            rbac: crate::rbac::RbacCatalog::default(),
            // Process-global sinks (`filefmt::register_global_file_format`)
            // seed every store constructed after the registration.
            file_formats: crate::filefmt::seeded_global_formats(),
            // A package is process-global but its surfaces activate per
            // store, when `LOAD EXTENSION '<package>'` runs.
            loaded_packages: HashMap::new(),
            package_table_sources: HashMap::new(),
            package_file_formats: HashMap::new(),
            package_dynamic_functions: HashMap::new(),
            package_vector_providers: HashMap::new(),
            vector_providers: Vec::new(),
            host_scalars: HashMap::new(),
            host_statements: HashMap::new(),
            store_id: next_store_id(),
            copy_to_policy: crate::CopyToPolicy::Unrestricted,
            stream_buffered_rows: std::sync::atomic::AtomicUsize::new(0),
            stream_peak_rows: std::sync::atomic::AtomicUsize::new(0),
            stream_folded_rows: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Allow or refuse the SQL `LOAD EXTENSION` statement on this store
    /// (see the field docs; the Rust load API stays available either way).
    ///
    /// Off by default: the statement makes the host process load and run
    /// native code from a path the SQL text names, so it is opt-in.
    pub fn set_allow_sql_load_extension(&mut self, allow: bool) {
        self.allow_sql_load_extension = allow;
    }

    /// Whether the SQL `LOAD EXTENSION` statement may run on this store.
    pub fn allow_sql_load_extension(&self) -> bool {
        self.allow_sql_load_extension
    }

    /// Allow or refuse WAL replay to load an extension the WAL records
    /// (default: refused).
    ///
    /// With this on, a replayed `LOAD EXTENSION '<path>'` is only honoured
    /// when the path resolves inside the configured extension root (see
    /// [`Self::set_extension_root`]); without a root, nothing is loaded.
    /// Either way, an extension that is already loaded in this process is
    /// reused as it is — never loaded a second time.
    pub fn set_allow_extension_replay(&mut self, allow: bool) {
        self.allow_extension_replay = allow;
    }

    /// Whether WAL replay may load an extension the WAL records.
    pub fn allow_extension_replay(&self) -> bool {
        self.allow_extension_replay
    }

    /// Confine every extension load to `root` (the directory that holds the
    /// extension libraries). `None` removes the confinement; the loader's
    /// own path rules (no bare names, no `..`, no symlinked library, regular
    /// files only) always apply.
    pub fn set_extension_root(&mut self, root: Option<std::path::PathBuf>) {
        self.extension_root = root;
    }

    /// The extension root this store confines loads to, if any.
    pub fn extension_root(&self) -> Option<&std::path::Path> {
        self.extension_root.as_deref()
    }

    /// Set the `COPY … TO` destination policy (M-3). Embedded stores default
    /// to [`crate::CopyToPolicy::Unrestricted`]; a network server installs
    /// `Disabled` before accepting connections and `Confined(root)` only when
    /// the operator opts in.
    pub fn set_copy_to_policy(&mut self, policy: crate::CopyToPolicy) {
        self.copy_to_policy = policy;
    }

    /// The current `COPY … TO` destination policy.
    pub fn copy_to_policy(&self) -> &crate::CopyToPolicy {
        &self.copy_to_policy
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
        self.current_ns.db()
    }

    /// The embedded session's current schema (database and schema).
    pub fn current_ns(&self) -> &SchemaPath {
        &self.current_ns
    }

    /// Set the embedded session's current database without validation
    /// (used by `open`, which restores the file's databases exactly, and by
    /// the session layer after `USE`/`DROP DATABASE`).
    pub(crate) fn set_current_db(&mut self, name: &str) {
        self.current_db = name.to_string();
        self.current_ns = SchemaPath::main(name);
    }

    /// Set the embedded session's current schema without validation (the
    /// session layer has already checked it, or a dropped schema fell back).
    pub(crate) fn set_current_ns(&mut self, ns: SchemaPath) {
        self.current_db = ns.db().to_string();
        self.current_ns = ns;
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

    /// The as-created spelling of a database (for diagnostics), else the key
    /// itself.
    pub(crate) fn database_display_name(&self, key: &str) -> String {
        self.databases
            .get(&Self::db_key(key))
            .map(|c| c.name.clone())
            .unwrap_or_else(|| key.to_string())
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
        if matches!(
            self.databases.get(&key).map(|c| &c.storage),
            Some(crate::lake_db::DbStorage::Lake(_))
        ) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" is a lake database: DETACH it (the lake keeps its data)"
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
        self.current_ns = SchemaPath::main(&self.current_db);
        Ok(())
    }

    /// Switch the embedded session to `ns` (validated; wave 131b).
    pub fn use_schema(&mut self, ns: &SchemaPath) -> Result<()> {
        if !self.database_exists(ns.db()) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{}\" not found",
                ns.db()
            )));
        }
        if !self.schema_exists(ns) {
            return Err(PinkDbError::Catalog(format!(
                "schema \"{}\" not found",
                ns.schema()
            )));
        }
        self.set_current_ns(ns.clone());
        Ok(())
    }

    /// The schema a session should fall back to after dropping its current
    /// schema: `main` of the current database (or the fallback database when
    /// that one is gone too).
    pub fn fallback_ns(&self) -> SchemaPath {
        if self.database_exists(self.current_ns.db()) {
            return SchemaPath::main(self.current_ns.db());
        }
        SchemaPath::main(&self.fallback_db())
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

    /// The schema `ns` names. A missing database and a missing schema are
    /// different errors: `ns` may be fully qualified, so a typo in either part
    /// has to be reported as the part that is wrong.
    pub(crate) fn schema(&self, ns: &SchemaPath) -> Result<&SchemaCatalog> {
        self.catalog(ns.db())?
            .schemas
            .get(ns.schema())
            .ok_or_else(|| PinkDbError::Catalog(format!("schema \"{}\" not found", ns.schema())))
    }

    pub(crate) fn schema_mut(&mut self, ns: &SchemaPath) -> Result<&mut SchemaCatalog> {
        self.catalog_mut(ns.db())?
            .schemas
            .get_mut(ns.schema())
            .ok_or_else(|| PinkDbError::Catalog(format!("schema \"{}\" not found", ns.schema())))
    }

    /// Does `ns` name an existing schema?
    pub fn schema_exists(&self, ns: &SchemaPath) -> bool {
        self.databases
            .get(ns.db())
            .is_some_and(|c| c.schemas.contains_key(ns.schema()))
    }

    /// The display names of `db`'s schemas, sorted. Empty for an unknown
    /// database (like the other `list_*_in` helpers).
    pub fn list_schemas(&self, db: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .databases
            .get(&Self::db_key(db))
            .map(|c| c.schemas.values().map(|s| s.name.clone()).collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Every table of `db`, in schema-key then table-key order, as
    /// `(namespace, lowercased key, table)`: the shape the optimistic base and
    /// the "every table of every database" walks (statistics, index rebuilds,
    /// vector hydration, checkpointing) need.
    pub(crate) fn all_tables_of(&self, db: &str) -> Vec<(SchemaPath, String, &DataTable)> {
        let Some(cat) = self.databases.get(&Self::db_key(db)) else {
            return Vec::new();
        };
        cat.schemas
            .iter()
            .flat_map(|(schema, s)| {
                s.tables
                    .iter()
                    .map(move |(name, t)| (SchemaPath::new(db, schema), name.clone(), t))
            })
            .collect()
    }

    /// Every view of `db`, in schema-key then view-key order (see
    /// [`PinkDb::all_tables_of`]).
    pub(crate) fn all_views_of(&self, db: &str) -> Vec<(SchemaPath, String, &ViewDef)> {
        let Some(cat) = self.databases.get(&Self::db_key(db)) else {
            return Vec::new();
        };
        cat.schemas
            .iter()
            .flat_map(|(schema, s)| {
                s.views
                    .iter()
                    .map(move |(name, v)| (SchemaPath::new(db, schema), name.clone(), v))
            })
            .collect()
    }

    /// Create schema `ns.schema`. `if_not_exists` makes an existing schema a
    /// no-op instead of an error (SQL `CREATE SCHEMA IF NOT EXISTS`).
    pub fn create_schema(&mut self, ns: &SchemaPath, if_not_exists: bool) -> Result<()> {
        let cat = self.catalog_mut(ns.db())?;
        if cat.schemas.contains_key(ns.schema()) {
            if if_not_exists {
                return Ok(());
            }
            return Err(PinkDbError::Catalog(format!(
                "schema \"{}\" already exists",
                ns.schema()
            )));
        }
        // The SQL layer sets the written spelling after this call; the
        // catalog key and this fallback display name are lowercased.
        cat.schemas
            .insert(ns.schema().to_string(), SchemaCatalog::empty(ns.schema()));
        Ok(())
    }

    /// Set a schema's display name (the checkpoint restores the spelling it
    /// recorded; the catalog key stays the lowercased name).
    pub(crate) fn set_schema_display(&mut self, ns: &SchemaPath, name: &str) -> Result<()> {
        self.schema_mut(ns)?.name = name.to_string();
        Ok(())
    }

    /// Drop schema `ns.schema`. `main` can never be dropped (every database
    /// has it), and without `cascade` a non-empty schema is refused with
    /// DuckDB's wording. With `cascade` every object in the schema goes.
    pub fn drop_schema(&mut self, ns: &SchemaPath, if_exists: bool, cascade: bool) -> Result<()> {
        let cat = self.catalog_mut(ns.db())?;
        if !cat.schemas.contains_key(ns.schema()) {
            if if_exists {
                return Ok(());
            }
            return Err(PinkDbError::Catalog(format!(
                "schema \"{}\" not found",
                ns.schema()
            )));
        }
        if ns.schema() == "main" {
            return Err(PinkDbError::Catalog(
                "cannot drop schema \"main\": it is the database's default schema".to_string(),
            ));
        }
        if !cascade && cat.schemas.get(ns.schema()).is_some_and(|s| !s.is_empty()) {
            return Err(PinkDbError::Catalog(format!(
                "schema \"{}\" is not empty: use DROP SCHEMA ... CASCADE",
                ns.schema()
            )));
        }
        cat.schemas.remove(ns.schema());
        // Every grant on the schema and its tables goes with it, cascade or
        // not: a later schema of the same name must not inherit them. Inside
        // a transaction this edits the transaction's catalog copy, so a
        // rollback restores the grants exactly as it restores a dropped table
        // (see `sync_table_grants` in db.rs for the commit merge).
        self.rbac_mut().remove_schema(ns.db(), ns.schema());
        Ok(())
    }

    /// Current version of one table (for conflict detection).
    pub fn table_version(&self, name: &str) -> Result<u64> {
        let db = self.current_db.clone();
        self.table_version_in(&db, name)
    }

    /// Process-unique identity of this store, shared by its snapshots. An
    /// extension with an in-process cache keys on it (see the `store_id`
    /// field).
    pub fn store_id(&self) -> u64 {
        self.store_id
    }

    pub fn table_version_in(&self, ns: impl Into<SchemaPath>, name: &str) -> Result<u64> {
        Ok(self.table_in(ns, name)?.version)
    }

    pub(crate) fn tables_remove_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: &str,
    ) -> Option<DataTable> {
        self.schema_mut(&ns.into()).ok()?.tables.remove(key)
    }

    pub(crate) fn tables_insert_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: String,
        table: DataTable,
    ) -> Result<()> {
        self.schema_mut(&ns.into())?.tables.insert(key, table);
        Ok(())
    }

    pub(crate) fn views_remove_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: &str,
    ) -> Option<ViewDef> {
        self.schema_mut(&ns.into()).ok()?.views.remove(key)
    }

    pub(crate) fn views_insert_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: String,
        view: ViewDef,
    ) -> Result<()> {
        self.schema_mut(&ns.into())?.views.insert(key, view);
        Ok(())
    }

    pub(crate) fn materialized_views_remove_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: &str,
    ) -> Option<MaterializedViewDef> {
        self.schema_mut(&ns.into())
            .ok()?
            .materialized_views
            .remove(key)
    }

    pub(crate) fn materialized_views_insert_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: String,
        view: MaterializedViewDef,
    ) -> Result<()> {
        self.schema_mut(&ns.into())
            .map(|s| s.materialized_views.insert(key, view))?;
        Ok(())
    }

    /// A stored procedure of `ns` by name (case-insensitive).
    pub fn procedure_in(
        &self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Result<&crate::procedure::ProcedureDef> {
        let ns = ns.into();
        self.schema(&ns)?
            .procedures
            .get(&name.to_ascii_lowercase())
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("procedure \"{name}\" not found in schema {ns}"))
            })
    }

    /// Install (or replace) one procedure definition.
    pub(crate) fn procedures_insert_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: String,
        def: crate::procedure::ProcedureDef,
    ) -> Result<()> {
        self.schema_mut(&ns.into())?.procedures.insert(key, def);
        Ok(())
    }

    /// Remove one procedure (the caller checks existence for `IF EXISTS`).
    pub(crate) fn procedures_remove_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: &str,
    ) -> Option<crate::procedure::ProcedureDef> {
        self.schema_mut(&ns.into()).ok()?.procedures.remove(key)
    }

    /// Every procedure of `ns`, as `(lowercased key, definition)` pairs
    /// sorted by key — the deterministic order `.pink` writes them in.
    pub(crate) fn procedures_in(
        &self,
        ns: impl Into<SchemaPath>,
    ) -> Vec<(String, crate::procedure::ProcedureDef)> {
        let Ok(schema) = self.schema(&ns.into()) else {
            return Vec::new();
        };
        let mut out: Vec<(String, crate::procedure::ProcedureDef)> = schema
            .procedures
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Does any schema of any database hold a procedure? (Cheap gate for the
    /// `.pink` flags word.)
    pub(crate) fn has_procedures(&self) -> bool {
        self.databases
            .values()
            .any(|c| c.schemas.values().any(|s| !s.procedures.is_empty()))
    }

    /// Every trigger of `ns`, in creation order. A borrowed slice so the
    /// trigger-free write path pays no allocation at all.
    pub(crate) fn triggers_in(&self, ns: impl Into<SchemaPath>) -> &[crate::trigger::TriggerDef] {
        static EMPTY: &[crate::trigger::TriggerDef] = &[];
        match self.schema(&ns.into()) {
            Ok(schema) => &schema.triggers,
            Err(_) => EMPTY,
        }
    }

    /// A stored trigger of `ns` by name (case-insensitive).
    pub(crate) fn trigger_in(
        &self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Result<&crate::trigger::TriggerDef> {
        let ns = ns.into();
        self.schema(&ns)?
            .triggers
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("trigger \"{name}\" not found in schema {ns}"))
            })
    }

    /// Install (or replace) one trigger, keeping creation order: `OR REPLACE`
    /// overwrites in place, so a replaced trigger keeps its firing position.
    pub(crate) fn triggers_insert_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        def: crate::trigger::TriggerDef,
    ) -> Result<()> {
        let schema = self.schema_mut(&ns.into())?;
        match schema.trigger_index(&def.name) {
            Some(i) => schema.triggers[i] = def,
            None => schema.triggers.push(def),
        }
        Ok(())
    }

    /// Remove one trigger (the caller checks existence for `IF EXISTS`).
    pub(crate) fn triggers_remove_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Option<crate::trigger::TriggerDef> {
        let schema = self.schema_mut(&ns.into()).ok()?;
        let i = schema.trigger_index(name)?;
        Some(schema.triggers.remove(i))
    }

    /// Does any schema of any database hold a trigger? (Cheap gate for the
    /// `.pink` flags word.)
    pub(crate) fn has_triggers(&self) -> bool {
        self.databases
            .values()
            .any(|c| c.schemas.values().any(|s| !s.triggers.is_empty()))
    }

    /// A stored macro of `ns` by name (case-insensitive).
    pub fn macro_in(
        &self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Result<&crate::macros::MacroDef> {
        let ns = ns.into();
        self.schema(&ns)?
            .macros
            .get(&name.to_ascii_lowercase())
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("macro \"{name}\" not found in schema {ns}"))
            })
    }

    /// Install (or replace) one macro definition.
    pub(crate) fn macros_insert_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: String,
        def: crate::macros::MacroDef,
    ) -> Result<()> {
        self.schema_mut(&ns.into())?.macros.insert(key, def);
        Ok(())
    }

    /// Remove one macro (the caller checks existence for `IF EXISTS`).
    pub(crate) fn macros_remove_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        key: &str,
    ) -> Option<crate::macros::MacroDef> {
        self.schema_mut(&ns.into()).ok()?.macros.remove(key)
    }

    /// Every macro of `ns`, as `(lowercased key, definition)` pairs sorted by
    /// key — the deterministic order `.pink` writes them in.
    pub(crate) fn macros_in(
        &self,
        ns: impl Into<SchemaPath>,
    ) -> Vec<(String, crate::macros::MacroDef)> {
        let Ok(schema) = self.schema(&ns.into()) else {
            return Vec::new();
        };
        let mut out: Vec<(String, crate::macros::MacroDef)> = schema
            .macros
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Does any schema of any database hold a macro? (Cheap gate for the
    /// `.pink` flags word.)
    pub(crate) fn has_macros(&self) -> bool {
        self.databases
            .values()
            .any(|c| c.schemas.values().any(|s| !s.macros.is_empty()))
    }

    /// Does the store hold any RBAC data (a user, a role, a membership, a
    /// grant or an admin)? Cheap gate for the `.pink` flags word.
    pub(crate) fn has_rbac_data(&self) -> bool {
        !self.rbac.users.is_empty()
            || !self.rbac.roles.is_empty()
            || !self.rbac.memberships.is_empty()
            || !self.rbac.table_grants.is_empty()
            || !self.rbac.schema_grants.is_empty()
            || !self.rbac.database_grants.is_empty()
            || !self.rbac.admins.is_empty()
    }

    /// Clear the statement-scoped procedure frame (called at every statement
    /// boundary; see [`PinkDb::proc_frame`]).
    pub(crate) fn proc_frame_reset(&mut self) {
        self.proc_frame.clear();
    }

    pub fn materialized_view_in(
        &self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Result<&MaterializedViewDef> {
        let ns = ns.into();
        self.schema(&ns)?
            .materialized_views
            .get(&name.to_ascii_lowercase())
            .ok_or_else(|| PinkDbError::Catalog(format!("materialized view \"{name}\" not found")))
    }

    /// Mutable access to one materialized view's definition.
    pub(crate) fn materialized_view_mut_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Result<&mut MaterializedViewDef> {
        let ns = ns.into();
        let key = name.to_ascii_lowercase();
        self.schema_mut(&ns)?
            .materialized_views
            .get_mut(&key)
            .ok_or_else(|| PinkDbError::Catalog(format!("materialized view \"{name}\" not found")))
    }

    /// Every materialized view of `ns`, as `(lowercased key, definition)`
    /// pairs sorted by key (deterministic order for `SHOW MATERIALIZED VIEWS`,
    /// `REFRESH MATERIALIZED VIEWS`, and the boundary refresh pass).
    pub(crate) fn materialized_views_in(
        &self,
        ns: impl Into<SchemaPath>,
    ) -> Vec<(String, MaterializedViewDef)> {
        let Ok(schema) = self.schema(&ns.into()) else {
            return Vec::new();
        };
        let mut out: Vec<(String, MaterializedViewDef)> = schema
            .materialized_views
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Names of every materialized view of `ns`, sorted (display names).
    pub(crate) fn materialized_view_names_in(&self, ns: impl Into<SchemaPath>) -> Vec<String> {
        let mut out: Vec<String> = self
            .schema(&ns.into())
            .map(|s| {
                s.materialized_views
                    .values()
                    .map(|v| v.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }

    /// The base tables the refresh of the MV `(ns, name)` reads, as
    /// namespace-plus-name references. Empty for an unknown view or one whose
    /// query could not be rebound.
    pub(crate) fn materialized_view_deps(
        &self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> HashSet<TableRef> {
        let ns = ns.into();
        let mut out = HashSet::new();
        let Ok(mv) = self.materialized_view_in(&ns, name) else {
            return out;
        };
        if let Some(q) = &mv.query {
            crate::plan::collect_tables_read(q, &ns, &mut out);
        }
        out
    }

    /// Restore a `CALL`'s statement snapshot (wave 44): the call's partial
    /// effects are discarded, and everything else is exactly as it was when
    /// the snapshot was taken — an open transaction stays open with its
    /// buffered WAL text, its validation sets and (crucially) its registered
    /// optimistic base, which [`PinkDb::rollback`] would release. The
    /// session's current database is live, like `rollback`'s.
    pub(crate) fn proc_snapshot_restore(&mut self, snap: Box<PinkDb>) {
        let current = self.current_db.clone();
        let current_ns = self.current_ns.clone();
        let base = self.txn_base.take();
        *self = *snap;
        self.current_db = current;
        self.current_ns = current_ns;
        self.txn_base = base;
    }

    /// Restore a procedure savepoint (wave 83: an `EXCEPTION` handler's
    /// block, or a body `ROLLBACK`): the data and catalog go back to `snap`,
    /// while the call's own bookkeeping stays current — the touched set (so
    /// the failed statements' reads still count for conflict detection),
    /// the call depth, the session and the transaction base.
    pub(crate) fn savepoint_restore(&mut self, snap: Box<PinkDb>) {
        let frame = std::mem::take(&mut self.proc_frame);
        let depth = self.proc_depth;
        let standalone = self.standalone_call;
        // A trigger body is running *now*, so its scope, capture buffer and
        // depth belong to the live statement, not to the snapshot taken
        // before it started.
        let trigger_scope = self.trigger_scope.take();
        let trigger_capture = self.trigger_capture.take();
        let trigger_depth = self.trigger_depth;
        let principal = self.session_principal.clone();
        let invoker = self.trigger_invoker.take();
        self.proc_snapshot_restore(snap);
        self.proc_frame = frame;
        self.proc_depth = depth;
        self.standalone_call = standalone;
        self.trigger_scope = trigger_scope;
        self.trigger_capture = trigger_capture;
        self.trigger_depth = trigger_depth;
        self.session_principal = principal;
        self.trigger_invoker = invoker;
    }

    /// True while a `BEGIN..COMMIT/ROLLBACK` block is open.
    pub fn in_transaction(&self) -> bool {
        self.txn_snapshot.is_some()
    }

    /// True when this database logs commits to a WAL.
    pub fn is_durable(&self) -> bool {
        self.wal.is_some()
    }

    /// The checkpoint file behind a durable store, or `None` in memory.
    /// `duckdb_databases().path` reports it for local databases.
    pub fn store_path(&self) -> Option<String> {
        self.wal.as_ref().map(|wal| {
            let wal = wal.lock().unwrap_or_else(|e| e.into_inner());
            wal.checkpoint_path().display().to_string()
        })
    }

    /// Install (or clear) the replay memo of the WAL record being replayed,
    /// returning the previous one so the caller can restore it. While set,
    /// every `external_io` host scalar must be served from it.
    pub(crate) fn set_ext_replay(
        &mut self,
        memo: Option<std::sync::Arc<crate::wal::ReplayMemo>>,
    ) -> Option<std::sync::Arc<crate::wal::ReplayMemo>> {
        std::mem::replace(&mut self.ext_replay, memo)
    }

    /// The record being replayed, if any: `(function, args) -> result`.
    pub(crate) fn ext_replay(&self) -> Option<&crate::wal::ReplayMemo> {
        self.ext_replay.as_deref()
    }

    /// Start the record state for a statement that is about to execute.
    ///
    /// A record is one WAL unit: an autocommit statement, a transaction, a
    /// procedure segment, the statements two triggers fire. Its seed is drawn
    /// once, at the first statement that writes, and travels in the record's
    /// memo (see [`crate::sql::binder::seed_memo`]). Every statement of the
    /// record then advances an ordinal, so two identical statements in one
    /// record draw different values while a replay, which executes the same
    /// statements in the same order after `clear`, draws the same ones.
    ///
    /// Nested statements (a `CALL` body, a trigger action, a materialized-view
    /// refresh) belong to the unit being built, so they do not start a new one.
    /// Read-only statements never seed: they have no record to replay.
    pub(crate) fn begin_volatile_statement(&self, plan: &crate::plan::LogicalPlan) -> Result<()> {
        let in_replay = self.replaying || self.ext_replay.is_some();
        if !in_replay && !self.in_transaction() && self.proc_depth == 0 && self.trigger_depth == 0 {
            self.volatile.clear();
        }
        if self.logged_statement(plan) {
            self.begin_volatile_write();
        }
        Ok(())
    }

    /// Whether this statement needs a seed of its own in the record it logs.
    ///
    /// Every statement that can be logged gets one, and so does every nested
    /// statement that can draw a value: a trigger action, a `DO` block body
    /// and a `CALL` body all execute inside the statement that carries the
    /// record, so their seeds ride in that record. The rule must not depend on
    /// how a `CALL` happens to be logged (its text, or its body's statements
    /// one segment at a time), because a replay takes the seeds in order.
    fn logged_statement(&self, plan: &crate::plan::LogicalPlan) -> bool {
        matches!(plan, crate::plan::LogicalPlan::Call { .. })
            || self.proc_depth > 0
            || self.trigger_depth > 0
            || crate::plan::writes_tables(plan)
    }

    /// Open the statement's slot in the record being built.
    pub(crate) fn begin_volatile_write(&self) {
        self.volatile.open_slot();
    }

    /// Load the seeds a record being replayed recorded, in order.
    pub(crate) fn set_replay_seeds(&self, seeds: impl IntoIterator<Item = u128>) {
        self.volatile.set_replay(seeds);
    }

    /// The seed and nesting depth the statement being evaluated draws from.
    ///
    /// The statement's slot was opened when it started executing, so a
    /// statement that never calls a seeded function leaves no memo entry. A
    /// read-only statement has no slot and draws a fresh seed per evaluation:
    /// a read has no record to replay, so its values only need to differ from
    /// each other.
    pub(crate) fn volatile_evaluation(&self) -> Result<(u128, u64)> {
        // Depth keeps a trigger's action apart from the statement that fired
        // it, even where both draw from seeds of the same record.
        let nest = u64::from((self.proc_depth + self.trigger_depth) as u32) << 32;
        let replay = self.replaying || self.ext_replay.is_some();
        Ok((self.volatile.seed(replay)?, nest))
    }

    /// The clock reading a statement's record pins: from the statement's own
    /// dependencies, or from the statements a `CALL`/`DO` body or a trigger
    /// action ran (a `now()` inside a body is part of the same record).
    pub(crate) fn statement_clock_memo(
        &self,
        dependencies: &[String],
    ) -> Option<crate::wal::ExtCall> {
        let mut deps = dependencies.to_vec();
        deps.extend(self.proc_frame.memo_deps().iter().cloned());
        crate::sql::binder::clock_memo(&deps)
    }

    /// The memo entries a record needs for the volatile functions it used: the
    /// clock reading and the seed of every statement of the record that drew
    /// one.
    pub(crate) fn statement_memo(&self, dependencies: &[String]) -> Vec<crate::wal::ExtCall> {
        self.statement_clock_memo(dependencies)
            .into_iter()
            .chain(self.volatile.take_memos())
            .collect()
    }

    /// The result an earlier statement of the open transaction recorded for
    /// `name(args)` in the transaction's replay memo, if any.
    pub(crate) fn txn_memo_result(&self, name: &str, args: &[Value]) -> Option<&Value> {
        self.wal_txn_memo
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name) && c.args == args)
            .map(|c| &c.result)
    }

    /// Permission for this session to call `external_io` host scalars (the
    /// serve policy's `allow-files` switch). The default is allowed: the
    /// embedded API and the CLI are not a shared service. The server and MCP
    /// turn it off for a read-only session, and every statement that session
    /// runs carries the flag to the executor.
    pub fn set_external_services(&mut self, allowed: bool) {
        self.external_services = allowed;
    }

    /// Whether this session may call `external_io` host scalars.
    pub fn external_services(&self) -> bool {
        self.external_services
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
        let local_views: Vec<(SchemaPath, HashMap<String, ViewDef>)> = self
            .databases
            .iter()
            .flat_map(|(db, cat)| {
                cat.schemas
                    .iter()
                    .map(move |(schema, s)| (SchemaPath::new(db, schema), s.views.clone()))
            })
            .filter(|(_, views)| !views.is_empty())
            .collect();
        self.databases = other.databases;
        // The RBAC catalog is store-global metadata outside `databases`: a
        // rebuilt store (checkpoint + replayed WAL) is authoritative for it
        // exactly like it is for tables, so the swap carries users, roles and
        // grants too.
        self.rbac = other.rbac;
        for (ns, views) in local_views {
            if let Ok(schema) = self.schema_mut(&ns) {
                for (name, view) in views {
                    schema.views.entry(name).or_insert(view);
                }
            }
        }
        // Attached lakes are session-local and survive the swap, but
        // `attach_lake` also created the (empty) databases their tables live
        // in so qualified names and `USE` keep resolving. Re-create those the
        // reloaded catalog does not define; a real database with the same
        // name wins (store tables shadow attached ones in resolution, exactly
        // like `visible_table_schema`). Without this, a full rebuild silently
        // orphaned every attached `db.table` name.
        // Lake databases' readers are not session attachments: their
        // databases come back with the reloaded catalog (or a replayed
        // `ATTACH`), below.
        let attached_dbs: Vec<String> = self
            .attached_lakes
            .iter()
            .filter(|l| !self.is_lake_reader(l))
            .flat_map(|l| l.tables().iter().map(|t| t.db.clone()))
            .collect();
        for name in attached_dbs {
            self.databases
                .entry(Self::db_key(&name))
                .or_insert_with(|| DbCatalog {
                    name,
                    ..DbCatalog::default()
                });
        }
        self.current_db = other.current_db;
        self.current_ns = other.current_ns;
        // Wave 124: the reloaded checkpoint names its lake databases but holds
        // none of their content; this process's open lakes provide it.
        self.reinstall_lake_databases();
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
    /// `sets` is the statement's precomputed (canonical) touched set when the
    /// caller already has it (the optimistic autocommit path computes it for
    /// validation); `None` computes it here.
    pub(crate) fn wal_log_statement(
        &mut self,
        plan: &crate::plan::LogicalPlan,
        sql: &str,
        dynamic_functions_used: &[String],
        sets: Option<&crate::wal::FrameSets>,
        memo: &[crate::wal::ExtCall],
    ) -> Result<()> {
        // Replayed statements are already in the WAL and must never be logged
        // again — not even buffered into an open transaction's record: frame
        // replay runs *inside* `commit`'s validation span (the transaction is
        // still open, only its WAL handle is detached), and wave 80 made the
        // per-statement source text unconditional, which took the old
        // `is_durable()` shield away. Buffering here would append the frame's
        // own statements to this transaction's record and replay them twice.
        if self.replaying || !crate::plan::writes_tables(plan) {
            return Ok(());
        }
        // A statement that read the clock (`now()`) carries the reading in its
        // replay memo, so replay binds the same instant. The seeds ride with
        // the record itself: one per logged statement that drew a volatile
        // value, added where the record is appended (a transaction drains them
        // at commit, `PinkDb::commit`).
        let with_clock;
        let memo = match self.statement_clock_memo(dynamic_functions_used) {
            Some(clock) => {
                with_clock = memo.iter().cloned().chain([clock]).collect::<Vec<_>>();
                &with_clock[..]
            }
            None => memo,
        };
        // Wave 83: a CALL whose body used COMMIT/ROLLBACK already logged its
        // committed segments; what remains is the final segment (re-running
        // the CALL text would apply the committed part twice).
        if matches!(plan, crate::plan::LogicalPlan::Call { .. })
            && !self.in_transaction()
            && let Some((db, stmts)) = self.proc_frame.segment_log().cloned()
        {
            if stmts.is_empty() {
                return Ok(());
            }
            let sets = self.proc_frame.touched();
            let memo: Vec<_> = memo
                .iter()
                .cloned()
                .chain(self.volatile.take_memos())
                .collect();
            return self.wal_append_session(&db, &db, &stmts, &[], &[], Some(&sets), &memo);
        }
        // Wave 48: a `CREATE/ALTER USER` is logged in its `PASSWORD_HASH`
        // form — the plaintext of the statement text must never reach the
        // WAL (and the hash is exactly the one the catalog stored).
        let logged = crate::rbac::wal_statement_text(plan, sql);
        let sql: &str = &logged;
        let touched = self.dynamic_function_paths(dynamic_functions_used);
        let loaded = loaded_extension_path(plan);
        // The frame's touched set (wave 45o), recorded in the payload so
        // other writers can validate optimistic commits against this frame.
        let sets = match sets {
            Some(s) => s.clone(),
            None => {
                let ns = self.current_ns().clone();
                self.plan_sets_store(plan, &ns)
            }
        };
        if self.in_transaction() {
            if self.wal_txn_db.is_none() {
                self.wal_txn_db = Some(self.current_ns().clone());
            }
            self.wal_txn.push(sql.to_string());
            self.wal_txn_sets.extend(&sets);
            push_unique(&mut self.wal_txn_ext_paths, touched);
            push_unique(&mut self.wal_txn_ext_loaded, loaded);
            for call in memo {
                if !self
                    .wal_txn_memo
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(&call.name) && c.args == call.args)
                {
                    self.wal_txn_memo.push(call.clone());
                }
            }
            return Ok(());
        }
        // `USE` is self-contained: replaying it lands in the target session
        // from anywhere, so no prefix is needed (and none would help).
        if let crate::plan::LogicalPlan::Use { ns } = plan {
            if let Some(wal) = self.durable_handle() {
                let mut guard = wal.lock().map_err(|_| {
                    PinkDbError::Execution("internal lock error (poisoned)".to_string())
                })?;
                guard.append(&[sql.to_string()], Some(&sets), memo)?;
                guard.set_session_db(ns);
            }
            return Ok(());
        }
        let ns = self.current_ns().clone();
        let memo: Vec<_> = memo
            .iter()
            .cloned()
            .chain(self.volatile.take_memos())
            .collect();
        self.wal_append_session(
            &ns,
            &ns,
            &[sql.to_string()],
            &touched,
            &loaded,
            Some(&sets),
            &memo,
        )
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
    // The arguments are the record's fields, one per column of the payload.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn wal_append_session(
        &mut self,
        db_before: &SchemaPath,
        db_after: &SchemaPath,
        stmts: &[String],
        touched_extensions: &[String],
        loaded_extensions: &[String],
        sets: Option<&crate::wal::FrameSets>,
        memo: &[crate::wal::ExtCall],
    ) -> Result<()> {
        // Wave 124: a unit that changed lake databases commits there, and the
        // storage checks guarantee it wrote nothing local — so it is not
        // appended to the local WAL.
        if self.route_committed_unit()? {
            return Ok(());
        }
        self.wal_append_local(
            db_before,
            db_after,
            stmts,
            touched_extensions,
            loaded_extensions,
            sets,
            memo,
        )
    }

    /// [`PinkDb::wal_append_session`] without the lake routing: append the
    /// unit to the local WAL (no-op in memory). The lake versions the unit
    /// read ride in the record (see [`crate::lake_db`]). A transaction of a
    /// shared store logs its record before it merges its work, and routes
    /// after the merge.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn wal_append_local(
        &self,
        db_before: &SchemaPath,
        db_after: &SchemaPath,
        stmts: &[String],
        touched_extensions: &[String],
        loaded_extensions: &[String],
        sets: Option<&crate::wal::FrameSets>,
        memo: &[crate::wal::ExtCall],
    ) -> Result<()> {
        if let Some(wal) = self.durable_handle() {
            let pins = self.lake_pins(sets);
            let memo: std::borrow::Cow<'_, [crate::wal::ExtCall]> = if pins.is_empty() {
                std::borrow::Cow::Borrowed(memo)
            } else {
                let mut all = memo.to_vec();
                all.extend(pins);
                std::borrow::Cow::Owned(all)
            };
            let mut guard = wal.lock().map_err(|_| {
                PinkDbError::Execution("internal lock error (poisoned)".to_string())
            })?;
            let before = self.schema_exists(db_before).then_some(db_before);
            guard.append_in_session(
                before,
                db_after,
                stmts,
                touched_extensions,
                loaded_extensions,
                sets,
                &memo,
            )?;
        }
        Ok(())
    }

    /// Open a transaction: refresh (a short lease only when something was
    /// published), snapshot the whole store, and capture the optimistic base
    /// the commit will validate against. No lease is held while the
    /// transaction runs (wave 45o): another process's writes never wait for
    /// it; `commit` detects conflicts from the frames those writers published
    /// in the meantime. Fails if a transaction is already open (flat
    /// transactions only — no savepoints in v1).
    pub fn begin(&mut self) -> Result<()> {
        if self.txn_snapshot.is_some() {
            return Err(PinkDbError::Execution(
                "already in a transaction".to_string(),
            ));
        }
        // Refresh before snapshotting: the transaction runs against the
        // complete, current state (this takes the lease only if stale).
        self.refresh_if_stale()?;
        self.wal_txn.clear();
        self.wal_txn_db = None;
        self.wal_txn_ext_paths.clear();
        self.wal_txn_ext_loaded.clear();
        self.wal_txn_sets = crate::wal::FrameSets::default();
        self.wal_txn_memo.clear();
        // A transaction is a new record: it draws its own seed.
        self.volatile.clear();
        self.txn_touched = crate::wal::FrameSets::default();
        self.txn_storages.clear();
        // A new transaction starts with no writes; ROLLBACK must not leave
        // this transaction's writes in the ON COMMIT dirty set.
        self.txn_writes.clear();
        // Capture the base *before* taking the snapshot: a frame committed
        // between the two is past the base, so the commit validates it
        // (conservative), while a base captured after the snapshot could miss
        // a frame the snapshot already saw.
        self.txn_base = self.capture_base()?;
        self.txn_snapshot = Some(Box::new(self.clone()));
        Ok(())
    }

    /// Commit: take the write lease, validate the transaction against every
    /// frame another writer committed after its base (write-write and
    /// read-write, table granularity — a stale read or a lost update is a
    /// conflict, and write skew is prevented), apply the frames that arrived
    /// for other tables, then make the transaction's statements durable (one
    /// WAL record) and drop the snapshot.
    ///
    /// A conflict aborts the transaction — the snapshot is discarded and a
    /// retryable `write conflict` error is returned (re-running the
    /// transaction on the refreshed state is the intended retry). A WAL
    /// failure returns the error and leaves the transaction open (nothing was
    /// published) and the handle fail-stopped, exactly as before this wave.
    pub fn commit(&mut self) -> Result<()> {
        if self.txn_snapshot.is_none() {
            return Err(PinkDbError::Execution(
                "no transaction in progress".to_string(),
            ));
        }
        let lease = self.acquire_lease()?;
        let writes = !self.wal_txn.is_empty();
        // Cross-process validation first (durable stores with a captured
        // base): it either applies the frames that arrived for other tables
        // or reports the conflict. Nothing local is applied before the check
        // passed, so a conflict leaves the (aborted) transaction's changes
        // only in the snapshot being discarded. Read-only transactions (no
        // buffered record) log nothing and never conflict.
        if writes && let (Some(g), Some(base)) = (lease.as_ref(), self.txn_base.as_ref()) {
            let position = base.position();
            let mut touched = self.txn_touched.clone();
            touched.canonicalize();
            if let Some(conflict) = self.optimistic_commit_check(g, &position, &touched)? {
                self.rollback()?;
                return Err(PinkDbError::Execution(conflict.message("transaction")));
            }
        }
        // Wave 124: a transaction writes one storage. The per-statement checks
        // already refused the plans that named two; what a `CALL` or trigger
        // wrote is only known now. A lake transaction publishes instead of
        // logging; a failed publish rolls it back.
        let txn_storages = std::mem::take(&mut self.txn_storages);
        if let Err(e) = self.refuse_mixed_storages(&txn_storages, "transaction") {
            self.rollback()?;
            return Err(e);
        }
        let lake_routed = writes && !self.dirty_lake_databases()?.is_empty();
        if lake_routed && let Err(e) = self.route_committed_unit() {
            self.rollback()?;
            return Err(e);
        }
        if writes && !lake_routed {
            let stmts = std::mem::take(&mut self.wal_txn);
            let touched = std::mem::take(&mut self.wal_txn_ext_paths);
            let loaded = std::mem::take(&mut self.wal_txn_ext_loaded);
            let sets = std::mem::take(&mut self.wal_txn_sets);
            // One seed per logged statement of the record rides with the
            // transaction's single WAL record, exactly like an autocommit
            // statement's.
            let mut memo = std::mem::take(&mut self.wal_txn_memo);
            for call in self.volatile.take_memos() {
                // A retried COMMIT keeps the entries it already restored.
                if !memo
                    .iter()
                    .any(|c| c.name == call.name && c.args == call.args)
                {
                    memo.push(call);
                }
            }
            let before = self
                .wal_txn_db
                .take()
                .unwrap_or_else(|| self.current_ns().clone());
            let after = self.current_ns().clone();
            if let Err(e) = self.wal_append_local(
                &before,
                &after,
                &stmts,
                &touched,
                &loaded,
                Some(&sets),
                &memo,
            ) {
                // Keep the statements: a retried COMMIT must never publish
                // changes that made it to memory but not to the WAL.
                self.wal_txn = stmts;
                self.wal_txn_db = Some(before);
                self.wal_txn_ext_paths = touched;
                self.wal_txn_ext_loaded = loaded;
                self.wal_txn_sets = sets;
                self.wal_txn_memo = memo;
                return Err(e);
            }
        }
        self.wal_txn.clear();
        self.wal_txn_db = None;
        self.wal_txn_ext_paths.clear();
        self.wal_txn_ext_loaded.clear();
        self.wal_txn_sets = crate::wal::FrameSets::default();
        self.wal_txn_memo.clear();
        self.txn_touched = crate::wal::FrameSets::default();
        // The transaction's writes are now committed: they become the ON
        // COMMIT trigger's dirty set for the next statement boundary.
        self.mv_pending.extend(self.txn_writes.drain());
        self.txn_snapshot = None;
        // The base is released only after the record is durable.
        self.txn_base = None;
        Ok(())
    }

    /// Roll back: restore the snapshot, discarding all changes since BEGIN
    /// (data and catalog alike); nothing is logged. The session's current
    /// database is *not* transactional — it keeps its live value.
    pub fn rollback(&mut self) -> Result<()> {
        match self.txn_snapshot.take() {
            Some(snap) => {
                let current = self.current_db.clone();
                let current_ns = self.current_ns.clone();
                *self = *snap;
                self.current_db = current;
                self.current_ns = current_ns;
                self.wal_txn.clear();
                self.wal_txn_db = None;
                self.wal_txn_ext_paths.clear();
                self.wal_txn_ext_loaded.clear();
                self.wal_txn_sets = crate::wal::FrameSets::default();
                self.txn_touched = crate::wal::FrameSets::default();
                self.txn_storages.clear();
                // A rolled-back transaction's writes never happened: drop its
                // ON COMMIT dirty set (the snapshot restore above already
                // reverted the pre-`BEGIN` value).
                self.txn_writes.clear();
                // The snapshot was cloned without a base; dropping the old
                // `self` above already unregistered it.
                self.txn_base = None;
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
        self.create_table_in(SchemaPath::main(&db), name, schema)
    }

    pub fn create_table_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
        schema: Vec<(String, DataType)>,
    ) -> Result<()> {
        self.create_table_with_constraints_in(ns, name, schema, TableConstraints::default())
    }

    /// [`PinkDb::create_table_in`] with declarative constraints (metadata
    /// only; never enforced on writes). The binder has already validated them
    /// (foreign keys against the referenced table's keys).
    pub fn create_table_with_constraints_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
        schema: Vec<(String, DataType)>,
        constraints: TableConstraints,
    ) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, name) {
            return Err(err);
        }
        let key = Self::key(name);
        let cat = self.schema_mut(&ns)?;
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
        cat.tables.insert(
            key,
            DataTable::new_with_constraints(name.to_string(), schema, constraints)?,
        );
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let db = self.current_db.clone();
        self.drop_table_in(SchemaPath::main(&db), name)
    }

    pub fn drop_table_in(&mut self, ns: impl Into<SchemaPath>, name: &str) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, name) {
            return Err(err);
        }
        self.schema_mut(&ns)?
            .tables
            .remove(&Self::key(name))
            .map(|_| ())
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    /// Build a hash index on `table.column` (by index) and store it.
    pub fn create_index_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        table: &str,
        name: &str,
        column: usize,
    ) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, table) {
            return Err(err);
        }
        let t = self.table_mut_in(&ns, table)?;
        let Some(col) = t.columns.get(column) else {
            return Err(PinkDbError::Catalog(format!(
                "column {column} out of range for table \"{table}\""
            )));
        };
        let map = index_map_for(col);
        let first_index = t.indexes.is_empty();
        t.indexes.push(Index {
            name: name.to_string(),
            column,
            map,
        });
        // A new index is fresh, but it must not clear a stale flag left by an
        // *older* index on the same table: that one still needs its rebuild.
        if first_index {
            t.indexes_stale = false;
        }
        Ok(())
    }

    /// Build a vector index on `table.column` through a registered provider.
    /// The column must be `FLOAT[N]` or `DOUBLE[N]`; DOUBLE is narrowed to
    /// f32 for the graph (the exact path still uses f64). A row that is NULL
    /// or has a NULL element is not a vector and is skipped.
    pub fn create_vector_index_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        table: &str,
        name: &str,
        column: usize,
        provider_name: &str,
        options: &[(String, String)],
    ) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, table) {
            return Err(err);
        }
        let provider = self
            .vector_providers
            .iter()
            .find(|p| p.name().eq_ignore_ascii_case(provider_name))
            .cloned()
            .ok_or_else(|| {
                PinkDbError::Catalog(format!(
                    "vector index provider \"{provider_name}\" is not loaded; run LOAD \
                     EXTENSION '{provider_name}' first"
                ))
            })?;
        let dim = {
            let t = self.table_in(&ns, table)?;
            if t.indexes.iter().any(|i| i.name.eq_ignore_ascii_case(name))
                || t.vector_indexes
                    .iter()
                    .any(|i| i.name.eq_ignore_ascii_case(name))
            {
                return Err(PinkDbError::Catalog(format!(
                    "index \"{name}\" already exists"
                )));
            }
            let Some(col) = t.columns.get(column) else {
                return Err(PinkDbError::Catalog(format!(
                    "column {column} out of range for table \"{table}\""
                )));
            };
            if let Some(existing) = t.vector_indexes.iter().find(|i| i.column == column) {
                return Err(PinkDbError::Catalog(format!(
                    "column \"{}\" already has vector index \"{}\"",
                    col.name, existing.name
                )));
            }
            match col.dtype {
                crate::types::DataType::Array {
                    elem: crate::types::ArrayElem::Float | crate::types::ArrayElem::Double,
                    len,
                } if len > 0 => len as usize,
                other => {
                    return Err(PinkDbError::Catalog(format!(
                        "vector index \"{name}\" needs an ARRAY column of FLOAT or DOUBLE, but \
                         \"{}\" is {other}",
                        col.name
                    )));
                }
            }
        };
        let mut index = provider.create(dim, options)?;
        // Build before touching the catalog: a failed build leaves no index.
        // One bulk call per chunk (wave 107d): the provider parallelises
        // the build, and chunk-sized calls bound the copied vectors.
        {
            let t = self.table_in(&ns, table)?;
            let col = &t.columns[column];
            let mut offset = 0usize;
            for chunk in &col.chunks {
                let rows: Vec<(u64, Vec<f32>)> = (0..chunk.len())
                    .filter_map(|row| {
                        crate::vector_index::row_vector(chunk, row, dim)
                            .map(|vector| ((offset + row) as u64, vector))
                    })
                    .collect();
                let refs: Vec<(u64, &[f32])> = rows
                    .iter()
                    .map(|(row, vector)| (*row, vector.as_slice()))
                    .collect();
                index.insert_batch(&refs)?;
                offset += chunk.len();
            }
        }
        let t = self.table_mut_in(&ns, table)?;
        t.vector_indexes.push(VectorIndexDef {
            name: name.to_string(),
            column,
            provider: provider.name().to_ascii_lowercase(),
            options: options.to_vec(),
            index: Some(std::sync::Arc::from(index)),
            blob: None,
        });
        Ok(())
    }

    /// Drop tombstones from a vector index (`ALTER INDEX … COMPACT`).
    pub fn compact_vector_index_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        table: &str,
        name: &str,
    ) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, table) {
            return Err(err);
        }
        let t = self.table_mut_in(&ns, table)?;
        let table_name = t.name.clone();
        let def = t
            .vector_indexes
            .iter_mut()
            .find(|i| i.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("vector index \"{name}\" not found on \"{table}\""))
            })?;
        def.writable(&table_name)?.compact();
        t.version = next_table_version();
        Ok(())
    }

    /// Live/tombstone counts of a vector index (tests and diagnostics).
    pub fn vector_index_stats_in(
        &self,
        ns: impl Into<SchemaPath>,
        table: &str,
        name: &str,
    ) -> Result<crate::vector_index::VectorIndexStats> {
        let t = self.table_in(ns, table)?;
        let def = t
            .vector_indexes
            .iter()
            .find(|i| i.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("vector index \"{name}\" not found on \"{table}\""))
            })?;
        let index = def.index.as_ref().ok_or_else(|| def.pending_error(table))?;
        Ok(index.stats())
    }

    /// The provider registered for `name`, if it is loaded on this store.
    pub(crate) fn vector_provider(
        &self,
        name: &str,
    ) -> Option<Arc<dyn crate::vector_index::VectorIndexProvider>> {
        self.vector_providers
            .iter()
            .find(|p| p.name().eq_ignore_ascii_case(name))
            .cloned()
    }

    /// Attach every pending graph whose provider is already loaded. Used
    /// after a refresh adopts a checkpoint without reloading the package.
    pub(crate) fn hydrate_pending_vector_indexes(&mut self) -> Result<()> {
        for provider in self.vector_providers.clone() {
            let ready = self.take_hydrated_vector_indexes(provider.as_ref())?;
            self.install_hydrated_vector_indexes(ready);
        }
        Ok(())
    }

    /// Activate the registered package that owns each pending graph, so a
    /// checkpointed store can replay WAL writes that target the graph. Only
    /// packages already registered (linked into this process) are activated;
    /// an unknown provider stays pending and fails later with its own error.
    pub(crate) fn hydrate_registered_vector_packages(&mut self) -> Result<()> {
        loop {
            let pending = self
                .databases
                .values()
                .flat_map(|c| c.schemas.values())
                .flat_map(|s| s.tables.values())
                .flat_map(|t| t.vector_indexes.iter())
                .filter(|d| d.index.is_none() && d.blob.is_some())
                .map(|d| d.provider.clone())
                .find(|p| self.vector_provider(p).is_none());
            let Some(provider) = pending else {
                return Ok(());
            };
            let Some(package) = crate::package::registered_vector_provider_package(&provider)
            else {
                return Ok(());
            };
            self.load_extension_package(&package)?;
        }
    }

    /// Deserialize every pending graph for `provider` without installing it.
    /// Called before the package's other surfaces are installed, so a corrupt
    /// blob rejects the whole load atomically; the returned graphs are then
    /// installed without a second parse.
    pub(crate) fn take_hydrated_vector_indexes(
        &self,
        provider: &dyn crate::vector_index::VectorIndexProvider,
    ) -> Result<Vec<crate::vector_index::HydratedVectorIndex>> {
        let mut ready = Vec::new();
        for (db_key, cat) in &self.databases {
            for (schema_key, schema) in &cat.schemas {
                for (table_key, t) in &schema.tables {
                    for (i, def) in t.vector_indexes.iter().enumerate() {
                        if def.index.is_none()
                            && def.provider.eq_ignore_ascii_case(provider.name())
                            && let Some(blob) = &def.blob
                        {
                            ready.push((
                                SchemaPath::new(db_key, schema_key),
                                table_key.clone(),
                                i,
                                provider.deserialize(blob)?,
                            ));
                        }
                    }
                }
            }
        }
        Ok(ready)
    }

    /// Install graphs returned by [`Self::take_hydrated_vector_indexes`].
    /// The store is not mutated between the two calls, so the slots still
    /// exist; this step cannot fail.
    pub(crate) fn install_hydrated_vector_indexes(
        &mut self,
        ready: Vec<crate::vector_index::HydratedVectorIndex>,
    ) {
        for (ns, table_key, slot, index) in ready {
            let t = self
                .schema_mut(&ns)
                .ok()
                .and_then(|s| s.tables.get_mut(&table_key))
                .expect("table still present");
            t.vector_indexes[slot].index = Some(std::sync::Arc::from(index));
            t.vector_indexes[slot].blob = None;
        }
    }

    /// Rebuild one table's stale hash indexes from its live chunks. The
    /// optimizer calls this lazily, just before it rewrites an equality lookup
    /// into an `IndexScan` (see [`DataTable::invalidate_indexes`]).
    pub fn rebuild_indexes_in(&mut self, ns: impl Into<SchemaPath>, table: &str) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, table) {
            return Err(err);
        }
        self.table_mut_in(&ns, table)?.rebuild_indexes();
        Ok(())
    }

    /// Rebuild every stale in-memory hash index (one pass over the store's
    /// catalogs). The statement path calls this immediately before
    /// optimization: DML only sets a flag, so the first statement after a
    /// mutation pays one rebuild per indexed table instead of every mutation
    /// paying one maintenance update per row. The optimizer independently
    /// refuses to use an index whose flag is still set, so skipping this call
    /// only costs performance.
    pub fn rebuild_stale_indexes(&mut self) {
        for catalog in self.databases.values_mut() {
            for schema in catalog.schemas.values_mut() {
                for table in schema.tables.values_mut() {
                    if table.indexes_stale {
                        table.rebuild_indexes();
                    }
                }
            }
        }
    }

    pub fn drop_index_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        table: &str,
        name: &str,
    ) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, table) {
            return Err(err);
        }
        let t = self.table_mut_in(&ns, table)?;
        let before = t.indexes.len() + t.vector_indexes.len();
        t.indexes.retain(|i| !i.name.eq_ignore_ascii_case(name));
        t.vector_indexes
            .retain(|i| !i.name.eq_ignore_ascii_case(name));
        if t.indexes.len() + t.vector_indexes.len() == before {
            return Err(PinkDbError::Catalog(format!("index \"{name}\" not found")));
        }
        if t.indexes.is_empty() {
            t.indexes_stale = false;
        }
        Ok(())
    }

    /// `ALTER TABLE … RENAME TO` (wave 65): move the table to a new
    /// case-insensitive key, keeping its data (and its schema/constraints) —
    /// metadata only, like DuckDB. The new name must be free among the
    /// database's tables and views; a case-only rename updates the display
    /// name in place.
    pub fn rename_table_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, from) {
            return Err(err);
        }
        let cat = self.schema_mut(&ns)?;
        let key_from = Self::key(from);
        let key_to = Self::key(to);
        if key_to == key_from {
            let table = cat
                .tables
                .get_mut(&key_from)
                .ok_or_else(|| PinkDbError::Catalog(format!("table \"{from}\" not found")))?;
            table.name = to.to_string();
            table.version = next_table_version();
            return Ok(());
        }
        if cat.tables.contains_key(&key_to) || cat.views.contains_key(&key_to) {
            return Err(PinkDbError::Catalog(format!(
                "Could not rename \"{from}\" to \"{to}\": another entry with this name already \
                 exists!"
            )));
        }
        let mut table = cat
            .tables
            .remove(&key_from)
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{from}\" not found")))?;
        table.name = to.to_string();
        table.version = next_table_version();
        cat.tables.insert(key_to, table);
        Ok(())
    }

    pub fn table(&self, name: &str) -> Result<&DataTable> {
        self.table_in(SchemaPath::main(&self.current_db), name)
    }

    pub fn table_in(&self, ns: impl Into<SchemaPath>, name: &str) -> Result<&DataTable> {
        let ns = ns.into();
        self.schema(&ns)?
            .tables
            .get(&Self::key(name))
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    pub fn table_mut(&mut self, name: &str) -> Result<&mut DataTable> {
        let db = self.current_db.clone();
        self.table_mut_in(SchemaPath::main(&db), name)
    }

    pub fn table_mut_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
    ) -> Result<&mut DataTable> {
        let ns = ns.into();
        if let Some(err) = self.attached_read_only(&ns, name) {
            return Err(err);
        }
        self.schema_mut(&ns)?
            .tables
            .get_mut(&Self::key(name))
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    /// Sorted table names (stable output for `.tables`).
    pub fn list_tables(&self) -> Vec<String> {
        self.list_tables_in(SchemaPath::main(&self.current_db))
    }

    /// Sorted table display names in `ns`.
    pub fn list_tables_in(&self, ns: impl Into<SchemaPath>) -> Vec<String> {
        let mut names: Vec<String> = self
            .schema(&ns.into())
            .map(|s| s.tables.values().map(|t| t.name.clone()).collect())
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
        self.create_view_in(SchemaPath::main(&db), name, query, cols)
    }

    pub fn create_view_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
        query: Select,
        cols: Option<Vec<String>>,
    ) -> Result<()> {
        let key = Self::key(name);
        let cat = self.schema_mut(&ns.into())?;
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
                sql: String::new(),
            },
        );
        Ok(())
    }

    /// Stamp the statement text a view was created from, so the engine catalog
    /// image can rebuild it (wave 96b).
    pub fn set_view_source_in(
        &mut self,
        ns: impl Into<SchemaPath>,
        name: &str,
        sql: &str,
    ) -> Result<()> {
        let key = Self::key(name);
        match self.schema_mut(&ns.into())?.views.get_mut(&key) {
            Some(view) => {
                view.sql = sql.to_string();
                Ok(())
            }
            None => Err(PinkDbError::Catalog(format!("view \"{name}\" not found"))),
        }
    }

    pub fn drop_view(&mut self, name: &str) -> Result<()> {
        let db = self.current_db.clone();
        self.drop_view_in(SchemaPath::main(&db), name)
    }

    pub fn drop_view_in(&mut self, ns: impl Into<SchemaPath>, name: &str) -> Result<()> {
        self.schema_mut(&ns.into())?
            .views
            .remove(&Self::key(name))
            .map(|_| ())
            .ok_or_else(|| PinkDbError::Catalog(format!("view \"{name}\" not found")))
    }

    pub fn view(&self, name: &str) -> Result<&ViewDef> {
        self.view_in(SchemaPath::main(&self.current_db), name)
    }

    pub fn view_in(&self, ns: impl Into<SchemaPath>, name: &str) -> Result<&ViewDef> {
        self.schema(&ns.into())?
            .views
            .get(&Self::key(name))
            .ok_or_else(|| PinkDbError::Catalog(format!("view \"{name}\" not found")))
    }

    /// Sorted view display names.
    pub fn list_views(&self) -> Vec<String> {
        self.list_views_in(SchemaPath::main(&self.current_db))
    }

    /// Sorted view display names in `ns`.
    pub fn list_views_in(&self, ns: impl Into<SchemaPath>) -> Vec<String> {
        let mut names: Vec<String> = self
            .schema(&ns.into())
            .map(|s| s.views.values().map(|v| v.name.clone()).collect())
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
            distinct_on: None,
            projections: vec![Projection::Expr {
                expr: Expr::Literal(Value::Integer(1)),
                alias: None,
            }],
            from: None,
            where_clause: None,
            group_by: Vec::new(),
            group_sets: Vec::new(),
            having: None,
            qualify: None,
            windows: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
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

    fn assert_index_matches_table(db: &PinkDb) {
        let table = db.table("t").unwrap();
        for index in &table.indexes {
            assert_eq!(
                index.map,
                index_map_for(&table.columns[index.column]),
                "index {} must equal a full rebuild of the visible rows",
                index.name
            );
        }
    }

    /// Appends extend the maps in place: 10,000 single-row INSERTs must never
    /// trigger a full rebuild, and every map still equals a rebuild of the
    /// final rows.
    #[test]
    fn appends_extend_indexes_without_rebuilding() {
        reset_index_rebuild_count();
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id BIGINT, g BIGINT);").unwrap();
        db.execute("CREATE INDEX i_g ON t(g);").unwrap();
        for i in 0..10_000i64 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {});", i % 97))
                .unwrap();
            if db.table("t").unwrap().indexes_stale {
                panic!("index went stale at insert {i}");
            }
        }
        assert_eq!(
            index_rebuild_count(),
            0,
            "an append must not rebuild the whole index map"
        );
        assert_index_matches_table(&db);
    }

    /// Positions are global across chunk boundaries: a batch that fills one
    /// chunk and a second batch that opens the next one land at
    /// `CHUNK_SIZE..CHUNK_SIZE+n`.
    #[test]
    fn append_index_positions_cross_chunk_boundaries() {
        reset_index_rebuild_count();
        let mut db = PinkDb::new();
        db.create_table("t", vec![("g".to_string(), DataType::Integer)])
            .unwrap();
        db.create_index_in("main", "t", "i_g", 0).unwrap();
        let mut first = Vector::with_capacity(DataType::Integer, CHUNK_SIZE);
        for i in 0..CHUNK_SIZE {
            first.push_value(Value::Integer((i % 7) as i32)).unwrap();
        }
        db.table_mut("t")
            .unwrap()
            .append_chunk(vec![first])
            .unwrap();
        let mut second = Vector::with_capacity(DataType::Integer, 10);
        for i in 0..10 {
            second.push_value(Value::Integer(i)).unwrap();
        }
        db.table_mut("t")
            .unwrap()
            .append_chunk(vec![second])
            .unwrap();
        assert_eq!(index_rebuild_count(), 0);
        let table = db.table("t").unwrap();
        let map = &table.indexes[0].map;
        let zeros = map.get(&{
            let mut buf = Vec::new();
            crate::exec::aggregate::encode_value(&mut buf, &Value::Integer(0));
            buf
        });
        assert!(zeros.is_some_and(|positions| positions.contains(&0)));
        assert!(zeros.is_some_and(|positions| positions.contains(&CHUNK_SIZE)));
        assert!(zeros.is_some_and(|positions| positions.iter().all(|p| *p < table.num_rows)));
        assert_index_matches_table(&db);
    }

    /// A deterministic mix of inserts, updates, deletes and rolled-back
    /// transactions leaves every index exactly equal to a full rebuild of
    /// the final visible rows.
    #[test]
    fn mixed_mutations_and_rollbacks_leave_indexes_exact() {
        reset_index_rebuild_count();
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id BIGINT, g BIGINT);").unwrap();
        db.execute("CREATE INDEX i_g ON t(g);").unwrap();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for step in 0..600i64 {
            match next() % 7 {
                0..=2 => {
                    db.execute(&format!(
                        "INSERT INTO t VALUES ({step}, {});",
                        (next() % 23) as i64
                    ))
                    .unwrap();
                }
                3 => {
                    db.execute(&format!(
                        "UPDATE t SET g = {} WHERE id = {};",
                        (next() % 23) as i64,
                        (next() % (step as u64 + 1)) as i64
                    ))
                    .unwrap();
                }
                4 => {
                    db.execute(&format!(
                        "DELETE FROM t WHERE id = {};",
                        (next() % (step as u64 + 1)) as i64
                    ))
                    .unwrap();
                }
                5 => {
                    db.execute("BEGIN;").unwrap();
                    db.execute(&format!(
                        "INSERT INTO t VALUES ({}, {});",
                        step + 10_000,
                        (next() % 23) as i64
                    ))
                    .unwrap();
                    db.execute(&format!(
                        "UPDATE t SET g = 99 WHERE id = {};",
                        (next() % (step as u64 + 1)) as i64
                    ))
                    .unwrap();
                    db.execute("ROLLBACK;").unwrap();
                }
                _ => {
                    // A statement boundary rebuilds anything a moving
                    // mutation left stale; the result must be exact.
                    db.rebuild_stale_indexes();
                }
            }
            db.rebuild_stale_indexes();
            assert_index_matches_table(&db);
        }
    }

    /// Wave 131a: every database has a `main` schema, and a created schema is
    /// a catalog level of its own (objects never leak between schemas).
    #[test]
    fn schemas_are_created_listed_and_hold_their_own_objects() {
        let mut db = PinkDb::new();
        let main = SchemaPath::main("main");
        assert!(db.schema_exists(&main));
        assert_eq!(db.list_schemas("main"), vec!["main".to_string()]);
        // Keys are lowercased; the display name is what `list_schemas` sorts.
        let s = SchemaPath::new("main", "Sales");
        assert_eq!(s.schema(), "sales");
        // `Display` quotes an identifier only when it needs quoting.
        assert_eq!(s.to_string(), "main.sales");
        assert_eq!(SchemaPath::new("db", "we ird").to_string(), "db.\"we ird\"");
        db.create_schema(&s, false).unwrap();
        assert_eq!(
            db.list_schemas("main"),
            vec!["main".to_string(), "sales".to_string()]
        );
        // `IF NOT EXISTS` is a no-op; the plain form refuses a duplicate.
        db.create_schema(&s, true).unwrap();
        assert_eq!(
            db.create_schema(&s, false).unwrap_err().to_string(),
            "schema \"sales\" already exists"
        );
        db.create_table_in(&s, "t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        assert!(db.table_in(&s, "t").is_ok());
        assert!(db.table_in(&main, "t").is_err());
        assert!(db.table_in(SchemaPath::new("main", "other"), "t").is_err());
        // A schema needs a database to live in.
        assert_eq!(
            db.create_schema(&SchemaPath::new("nope", "s"), false)
                .unwrap_err()
                .to_string(),
            "database \"nope\" not found"
        );
        assert!(db.list_schemas("nope").is_empty());
    }

    /// Wave 131a: `main` cannot be dropped, a non-empty schema needs
    /// `CASCADE`, and `CASCADE` removes the schema's objects with it.
    #[test]
    fn drop_schema_refuses_main_and_non_empty_schemas() {
        let mut db = PinkDb::new();
        let err = db
            .drop_schema(&SchemaPath::main("main"), false, true)
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "cannot drop schema \"main\": it is the database's default schema"
        );
        let s = SchemaPath::new("main", "s");
        db.create_schema(&s, false).unwrap();
        db.create_table_in(&s, "t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        assert_eq!(
            db.drop_schema(&s, false, false).unwrap_err().to_string(),
            "schema \"s\" is not empty: use DROP SCHEMA ... CASCADE"
        );
        db.drop_schema(&s, false, true).unwrap();
        assert!(!db.schema_exists(&s));
        assert!(db.table_in(&s, "t").is_err());
        // A missing schema: refused, or a no-op with `IF EXISTS`.
        assert!(db.drop_schema(&s, true, false).is_ok());
        assert!(db.drop_schema(&s, false, false).is_err());
    }

    /// Review fix: dropping a schema drops its grants (schema and table), so
    /// a later schema of the same name starts clean; a non-empty schema is
    /// only dropped with `cascade`.
    #[test]
    fn drop_schema_removes_its_grants() {
        use crate::rbac::{GrantObject, Privilege};
        let mut db = PinkDb::new();
        let s = SchemaPath::new("main", "s");
        let schema_grant = GrantObject::Schema {
            db: "main".to_string(),
            schema: "s".to_string(),
        };
        let table_grant = GrantObject::Table {
            db: "main".to_string(),
            schema: "s".to_string(),
            table: "t".to_string(),
        };
        db.create_schema(&s, false).unwrap();
        db.rbac_mut()
            .grant(&schema_grant, "alice", &[Privilege::Select]);
        assert!(db.rbac().allowed("alice", &schema_grant, Privilege::Select));
        // An empty schema dropped without `cascade` still drops the grant.
        db.drop_schema(&s, false, false).unwrap();
        assert!(!db.rbac().allowed("alice", &schema_grant, Privilege::Select));
        db.create_schema(&s, false).unwrap();
        assert!(
            !db.rbac().allowed("alice", &schema_grant, Privilege::Select),
            "a re-created schema must not inherit the old grants"
        );
        // A table grant goes with the schema as well.
        db.create_table_in(&s, "t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.rbac_mut()
            .grant(&table_grant, "alice", &[Privilege::Select]);
        assert!(db.rbac().allowed("alice", &table_grant, Privilege::Select));
        db.drop_schema(&s, false, true).unwrap();
        assert!(!db.rbac().allowed("alice", &table_grant, Privilege::Select));
    }
}
