//! In-memory columnar storage + catalog.
//!
//! `DataTable` = one [`Vector`] chunk list per column (each ≤ [`CHUNK_SIZE`]
//! rows). [`PinkDb`] = map of table name → table. Single connection, single
//! thread. No persistence in v1.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use pinkdb_ext_loader::{ExtFunction, ExtHandle};

use crate::crypto::EncryptionKey;
use crate::sql::ast::Select;
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector};
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
    /// The column's DEFAULT (wave 65): the value a later `INSERT` that omits
    /// the column writes, and the value `ALTER TABLE … ADD COLUMN … DEFAULT`
    /// materialized into the rows that existed before it. `None` = no default
    /// (an omitted `INSERT` column writes NULL). Persisted in `.pink` v5 under
    /// the defaults flag; lake v5 manifests carry it in the table entry's
    /// `columns` record.
    pub default: Option<Value>,
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
    /// Set by every mutation (INSERT/UPDATE/DELETE/bulk load/COPY) when the
    /// table has indexes: the maps describe a pre-mutation snapshot. The
    /// statement path rebuilds them lazily from the live chunks at the next
    /// statement boundary ([`PinkDb::rebuild_stale_indexes`]), and the
    /// optimizer refuses to use one while this flag is set, so a stale index
    /// is only ever a missed optimization — never a wrong answer. See
    /// [`DataTable::rebuild_indexes`].
    pub indexes_stale: bool,
    /// Mutation stamp for optimistic-concurrency conflict detection. Drawn
    /// from a process-wide counter on creation and on every
    /// append/update/delete batch, so it is unique across tables: a table
    /// dropped and recreated under the same name never repeats an earlier
    /// stamp (a transaction based on the old table must conflict).
    /// Process-lifetime only (not persisted — save/load draws fresh stamps).
    pub version: u64,
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
            indexes_stale: false,
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
            constraints: TableConstraints::default(),
            indexes: Vec::new(),
            indexes_stale: false,
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

    /// `ALTER TABLE … ADD COLUMN` (wave 65): append a column at the end,
    /// filled with `default` for every existing row. The value is
    /// materialized into the chunks (the engine's in-memory style; the lake's
    /// schema record is what keeps the operation metadata-only for old *data
    /// files*). `default` stays recorded on the [`Column`] so later `INSERT`s
    /// that omit the column write it too.
    pub(crate) fn add_column(
        &mut self,
        name: String,
        dtype: DataType,
        default: Option<Value>,
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
        for len in lens {
            let mut v = Vector::with_capacity(dtype, len);
            for _ in 0..len {
                match &default {
                    Some(d) if !d.is_null() => v.push_valid(d.clone())?,
                    _ => v.push_null()?,
                }
            }
            col.chunks.push(Arc::new(v));
        }
        self.columns.push(col);
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
        self.version = next_table_version();
        Ok(())
    }

    /// Mark the table's hash indexes stale after a mutation. Cheap (one flag);
    /// [`Self::rebuild_indexes`] rebuilds them lazily on the next indexed
    /// lookup.
    pub(crate) fn invalidate_indexes(&mut self) {
        if !self.indexes.is_empty() {
            self.indexes_stale = true;
        }
    }

    /// Rebuild every hash index on this table from the live chunks, clearing
    /// the stale flag. Called lazily by [`PinkDb::rebuild_stale_indexes`] at
    /// the next statement boundary, so a later indexed lookup sees a snapshot
    /// of the current rows.
    pub(crate) fn rebuild_indexes(&mut self) {
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
        self.invalidate_indexes();
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
            self.invalidate_indexes();
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
        self.invalidate_indexes();
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
            self.invalidate_indexes();
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
            self.invalidate_indexes();
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

/// One database's catalog: its tables, its session-local views, and (wave 44)
/// its stored procedures.
#[derive(Debug, Default, Clone)]
pub struct DbCatalog {
    /// Display name as created (the map key is the lowercased name).
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
    /// Attached read-only lakes (`.lake attach`): session-local, additive to
    /// the visible catalog for reads only — never part of the store's own
    /// tables and never touched by `.save`, `.lake save`, or the WAL.
    pub(crate) attached_lakes: Vec<Arc<dyn crate::attached::AttachedLake>>,
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
    pub(crate) mv_pending: std::collections::HashSet<(String, String)>,
    /// `(database, table)` pairs written by the open embedded transaction:
    /// moved into `mv_pending` at COMMIT, discarded at ROLLBACK (a rolled-back
    /// write must not trigger an ON COMMIT refresh).
    pub(crate) txn_writes: std::collections::HashSet<(String, String)>,
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
    /// Where SQL `COPY … TO` may write (M-3). Embedded stores default to
    /// [`crate::CopyToPolicy::Unrestricted`]; a network server installs
    /// `Disabled` at bind time and the `--allow-copy-to --copy-root` opt-in
    /// installs `Confined(root)`.
    pub(crate) copy_to_policy: crate::CopyToPolicy,
}

impl Default for PinkDb {
    fn default() -> Self {
        Self::with_databases(["main"])
    }
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
            attached_lakes: self.attached_lakes.clone(),
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
            rbac: self.rbac.clone(),
            file_formats: self.file_formats.clone(),
            loaded_packages: self.loaded_packages.clone(),
            package_table_sources: self.package_table_sources.clone(),
            package_file_formats: self.package_file_formats.clone(),
            package_dynamic_functions: self.package_dynamic_functions.clone(),
            copy_to_policy: self.copy_to_policy.clone(),
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
            attached_lakes: Vec::new(),
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
            copy_to_policy: crate::CopyToPolicy::Unrestricted,
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

    pub(crate) fn materialized_views_remove_in(
        &mut self,
        db: &str,
        key: &str,
    ) -> Option<MaterializedViewDef> {
        self.databases
            .get_mut(&Self::db_key(db))?
            .materialized_views
            .remove(key)
    }

    pub(crate) fn materialized_views_insert_in(
        &mut self,
        db: &str,
        key: String,
        view: MaterializedViewDef,
    ) -> Result<()> {
        self.catalog_mut(db)?.materialized_views.insert(key, view);
        Ok(())
    }

    /// A stored procedure of `db` by name (case-insensitive).
    pub fn procedure_in(&self, db: &str, name: &str) -> Result<&crate::procedure::ProcedureDef> {
        self.catalog(db)?
            .procedures
            .get(&name.to_ascii_lowercase())
            .ok_or_else(|| {
                PinkDbError::Catalog(format!(
                    "procedure \"{name}\" not found in database \"{db}\""
                ))
            })
    }

    /// Install (or replace) one procedure definition.
    pub(crate) fn procedures_insert_in(
        &mut self,
        db: &str,
        key: String,
        def: crate::procedure::ProcedureDef,
    ) -> Result<()> {
        self.catalog_mut(db)?.procedures.insert(key, def);
        Ok(())
    }

    /// Remove one procedure (the caller checks existence for `IF EXISTS`).
    pub(crate) fn procedures_remove_in(
        &mut self,
        db: &str,
        key: &str,
    ) -> Option<crate::procedure::ProcedureDef> {
        self.databases
            .get_mut(&Self::db_key(db))?
            .procedures
            .remove(key)
    }

    /// Every procedure of `db`, as `(lowercased key, definition)` pairs
    /// sorted by key — the deterministic order `.pink` writes them in.
    pub(crate) fn procedures_in(&self, db: &str) -> Vec<(String, crate::procedure::ProcedureDef)> {
        let Some(cat) = self.databases.get(&Self::db_key(db)) else {
            return Vec::new();
        };
        let mut out: Vec<(String, crate::procedure::ProcedureDef)> = cat
            .procedures
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Does any database hold a procedure? (Cheap gate for the `.pink`
    /// flags word.)
    pub(crate) fn has_procedures(&self) -> bool {
        self.databases.values().any(|c| !c.procedures.is_empty())
    }

    /// A stored macro of `db` by name (case-insensitive).
    pub fn macro_in(&self, db: &str, name: &str) -> Result<&crate::macros::MacroDef> {
        self.catalog(db)?
            .macros
            .get(&name.to_ascii_lowercase())
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("macro \"{name}\" not found in database \"{db}\""))
            })
    }

    /// Install (or replace) one macro definition.
    pub(crate) fn macros_insert_in(
        &mut self,
        db: &str,
        key: String,
        def: crate::macros::MacroDef,
    ) -> Result<()> {
        self.catalog_mut(db)?.macros.insert(key, def);
        Ok(())
    }

    /// Remove one macro (the caller checks existence for `IF EXISTS`).
    pub(crate) fn macros_remove_in(
        &mut self,
        db: &str,
        key: &str,
    ) -> Option<crate::macros::MacroDef> {
        self.databases
            .get_mut(&Self::db_key(db))?
            .macros
            .remove(key)
    }

    /// Every macro of `db`, as `(lowercased key, definition)` pairs sorted by
    /// key — the deterministic order `.pink` writes them in.
    pub(crate) fn macros_in(&self, db: &str) -> Vec<(String, crate::macros::MacroDef)> {
        let Some(cat) = self.databases.get(&Self::db_key(db)) else {
            return Vec::new();
        };
        let mut out: Vec<(String, crate::macros::MacroDef)> = cat
            .macros
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Does any database hold a macro? (Cheap gate for the `.pink` flags
    /// word.)
    pub(crate) fn has_macros(&self) -> bool {
        self.databases.values().any(|c| !c.macros.is_empty())
    }

    /// Does the store hold any RBAC data (a user, a role, a membership, a
    /// grant or an admin)? Cheap gate for the `.pink` flags word.
    pub(crate) fn has_rbac_data(&self) -> bool {
        !self.rbac.users.is_empty()
            || !self.rbac.roles.is_empty()
            || !self.rbac.memberships.is_empty()
            || !self.rbac.table_grants.is_empty()
            || !self.rbac.database_grants.is_empty()
            || !self.rbac.admins.is_empty()
    }

    /// Clear the statement-scoped procedure frame (called at every statement
    /// boundary; see [`PinkDb::proc_frame`]).
    pub(crate) fn proc_frame_reset(&mut self) {
        self.proc_frame.clear();
    }

    pub fn materialized_view_in(&self, db: &str, name: &str) -> Result<&MaterializedViewDef> {
        self.catalog(db)?
            .materialized_views
            .get(&name.to_ascii_lowercase())
            .ok_or_else(|| PinkDbError::Catalog(format!("materialized view \"{name}\" not found")))
    }

    /// Mutable access to one materialized view's definition.
    pub(crate) fn materialized_view_mut_in(
        &mut self,
        db: &str,
        name: &str,
    ) -> Result<&mut MaterializedViewDef> {
        let key = name.to_ascii_lowercase();
        self.catalog_mut(db)?
            .materialized_views
            .get_mut(&key)
            .ok_or_else(|| PinkDbError::Catalog(format!("materialized view \"{name}\" not found")))
    }

    /// Every materialized view of `db`, as `(lowercased key, definition)`
    /// pairs sorted by key (deterministic order for `SHOW MATERIALIZED VIEWS`,
    /// `REFRESH MATERIALIZED VIEWS`, and the boundary refresh pass).
    pub(crate) fn materialized_views_in(&self, db: &str) -> Vec<(String, MaterializedViewDef)> {
        let Some(cat) = self.databases.get(&Self::db_key(db)) else {
            return Vec::new();
        };
        let mut out: Vec<(String, MaterializedViewDef)> = cat
            .materialized_views
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Names of every materialized view of `db`, sorted (display names).
    pub(crate) fn materialized_view_names_in(&self, db: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .databases
            .get(&Self::db_key(db))
            .map(|c| {
                c.materialized_views
                    .values()
                    .map(|v| v.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }

    /// The `(lowercased db, lowercased table)` pairs the refresh of the MV
    /// `(db, name)` reads: its defining query's base tables. Empty for an
    /// unknown view or one whose query could not be rebound.
    pub(crate) fn materialized_view_deps(&self, db: &str, name: &str) -> HashSet<(String, String)> {
        let mut out = HashSet::new();
        let Ok(mv) = self.materialized_view_in(db, name) else {
            return out;
        };
        if let Some(q) = &mv.query {
            crate::plan::collect_tables_read(q, db, &mut out);
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
        let base = self.txn_base.take();
        *self = *snap;
        self.current_db = current;
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
        self.proc_snapshot_restore(snap);
        self.proc_frame = frame;
        self.proc_depth = depth;
        self.standalone_call = standalone;
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
        // The RBAC catalog is store-global metadata outside `databases`: a
        // rebuilt store (checkpoint + replayed WAL) is authoritative for it
        // exactly like it is for tables, so the swap carries users, roles and
        // grants too.
        self.rbac = other.rbac;
        for (key, views) in local_views {
            if let Some(cat) = self.databases.get_mut(&key) {
                for (name, view) in views {
                    cat.views.entry(name).or_insert(view);
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
        let attached_dbs: Vec<String> = self
            .attached_lakes
            .iter()
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
            return self.wal_append_session(&db, &db, &stmts, &[], &[], Some(&sets));
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
            None => self.plan_sets_store(plan, &self.current_db.clone()),
        };
        if self.in_transaction() {
            if self.wal_txn_db.is_none() {
                self.wal_txn_db = Some(self.current_db.clone());
            }
            self.wal_txn.push(sql.to_string());
            self.wal_txn_sets.extend(&sets);
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
                guard.append(&[sql.to_string()], Some(&sets))?;
                guard.set_session_db(&name.to_ascii_lowercase());
            }
            return Ok(());
        }
        let db = self.current_db.clone();
        self.wal_append_session(&db, &db, &[sql.to_string()], &touched, &loaded, Some(&sets))
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
        sets: Option<&crate::wal::FrameSets>,
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
                sets,
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
        self.txn_touched = crate::wal::FrameSets::default();
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
        if writes {
            let stmts = std::mem::take(&mut self.wal_txn);
            let touched = std::mem::take(&mut self.wal_txn_ext_paths);
            let loaded = std::mem::take(&mut self.wal_txn_ext_loaded);
            let sets = std::mem::take(&mut self.wal_txn_sets);
            let before = self
                .wal_txn_db
                .take()
                .unwrap_or_else(|| self.current_db.clone());
            let after = self.current_db.clone();
            if let Err(e) =
                self.wal_append_session(&before, &after, &stmts, &touched, &loaded, Some(&sets))
            {
                // Keep the statements: a retried COMMIT must never publish
                // changes that made it to memory but not to the WAL.
                self.wal_txn = stmts;
                self.wal_txn_db = Some(before);
                self.wal_txn_ext_paths = touched;
                self.wal_txn_ext_loaded = loaded;
                self.wal_txn_sets = sets;
                return Err(e);
            }
        }
        self.wal_txn.clear();
        self.wal_txn_db = None;
        self.wal_txn_ext_paths.clear();
        self.wal_txn_ext_loaded.clear();
        self.wal_txn_sets = crate::wal::FrameSets::default();
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
                *self = *snap;
                self.current_db = current;
                self.wal_txn.clear();
                self.wal_txn_db = None;
                self.wal_txn_ext_paths.clear();
                self.wal_txn_ext_loaded.clear();
                self.wal_txn_sets = crate::wal::FrameSets::default();
                self.txn_touched = crate::wal::FrameSets::default();
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
        self.create_table_in(&db, name, schema)
    }

    pub fn create_table_in(
        &mut self,
        db: &str,
        name: &str,
        schema: Vec<(String, DataType)>,
    ) -> Result<()> {
        self.create_table_with_constraints_in(db, name, schema, TableConstraints::default())
    }

    /// [`PinkDb::create_table_in`] with declarative constraints (metadata
    /// only; never enforced on writes). The binder has already validated them
    /// (foreign keys against the referenced table's keys).
    pub fn create_table_with_constraints_in(
        &mut self,
        db: &str,
        name: &str,
        schema: Vec<(String, DataType)>,
        constraints: TableConstraints,
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
        cat.tables.insert(
            key,
            DataTable::new_with_constraints(name.to_string(), schema, constraints)?,
        );
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

    /// Build a hash index on `table.column` (by index) and store it.
    pub fn create_index_in(
        &mut self,
        db: &str,
        table: &str,
        name: &str,
        column: usize,
    ) -> Result<()> {
        if let Some(err) = self.attached_read_only(db, table) {
            return Err(err);
        }
        let t = self.table_mut_in(db, table)?;
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

    /// Rebuild one table's stale hash indexes from its live chunks. The
    /// optimizer calls this lazily, just before it rewrites an equality lookup
    /// into an `IndexScan` (see [`DataTable::invalidate_indexes`]).
    pub fn rebuild_indexes_in(&mut self, db: &str, table: &str) -> Result<()> {
        if let Some(err) = self.attached_read_only(db, table) {
            return Err(err);
        }
        self.table_mut_in(db, table)?.rebuild_indexes();
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
            for table in catalog.tables.values_mut() {
                if table.indexes_stale {
                    table.rebuild_indexes();
                }
            }
        }
    }

    pub fn drop_index_in(&mut self, db: &str, table: &str, name: &str) -> Result<()> {
        let t = self.table_mut_in(db, table)?;
        let before = t.indexes.len();
        t.indexes.retain(|i| i.name != name);
        if t.indexes.len() == before {
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
    pub fn rename_table_in(&mut self, db: &str, from: &str, to: &str) -> Result<()> {
        if let Some(err) = self.attached_read_only(db, from) {
            return Err(err);
        }
        let cat = self.catalog_mut(db)?;
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
}
