//! Incremental commits (catalog `format_version` 4): append-only data files,
//! positional delete files, and the merge-on-read readers that union them.
//!
//! # What a commit is
//!
//! [`Lake::commit`] publishes a new version whose tables are the previous
//! version's data files **plus** the files the commit adds — never a rewrite:
//!
//! - an **append** writes the added rows into one new immutable data file per
//!   table — or, for a partitioned table (wave 66), one file per partition
//!   value set, each carrying its values — and lists them (with exact row
//!   counts) after the table's existing files, so scans union the files in
//!   order;
//! - a **delete** writes one **positional delete file** per (table, data
//!   file) pair, listing the 0-based positions *within that data file*; the
//!   reader applies them on top of the decoded file (merge-on-read), and
//!   `UPDATE` is simply a delete of the old position plus an append;
//! - a **drop** removes the table's entry from the catalog (its files stay on
//!   disk: any earlier version stays loadable, exactly as with `.lake save`);
//! - a **repartition** ([`Commit::set_partition`]/[`Commit::clear_partition`],
//!   DuckLake's `ALTER TABLE … SET PARTITIONED BY`) records a new partition
//!   spec on the table's entry; existing files keep their recorded values,
//!   new appends split by the new spec.
//!
//! Every published version (including a full `.lake save`) also writes
//! `commits/<version>.json`, an append-only delta manifest listing the data
//! and delete files that commit added and the row movement per table — the
//! audit trail behind the authoritative snapshot, never a load-time input.
//!
//! [`Lake::commit_from_tables`] is the high-level form: it diffs the caller's
//! current tables against the logical content of a pinned version (the
//! content [`WritableAttachment`] materializes), turns row-level changes into
//! the appends/deletes above, and commits them. That is what the CLI's
//! writable attachment uses: `INSERT`/`UPDATE`/`DELETE` run against the
//! materialized tables as ordinary engine DML, and `.lake commit` publishes
//! exactly the difference.
//!
//! # Concurrency
//!
//! [`CommitSettings`] mirrors DuckLake's writer-retry controls — the settings
//! `ducklake_max_retry_count` (default 10), `ducklake_retry_backoff` (1.5)
//! and `ducklake_retry_wait_ms` (100). A conflicting publish (another writer
//! moved the catalog) is retried with exponentially increasing waits
//! (`retry_wait_ms * retry_backoff^(retry-1)`), rebasing the already-written
//! files onto the new head; when the budget is exhausted the commit fails
//! with a clear conflict error. A rebase is refused (fatal, never silent)
//! when the head no longer contains a data file the commit deletes from — the
//! delta would be wrong.
//!
//! # The diff
//!
//! [`commit_from_tables`](Lake::commit_from_tables) requires the base tables'
//! *logical* content (rows in scan order: data files in order, minus the
//! positions their delete files drop) and the current content. Rows are
//! matched exactly (a `DOUBLE` by bit pattern, so NaN payloads and `-0.0`
//! survive as themselves; strings and every other type by value): a base row
//! with no equal current row is deleted at its physical position, and a
//! current row with no equal base row is appended. The result is an exact
//! delta, whatever order the engine's mutations produced it in.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use pinkdb::{AttachedLake, DataTable, PinkDb, Value};

use crate::catalog::{CommitManifest, DeleteRef, FileRef, PartitionKey, TableDelta, TableEntry};
use crate::local::CATALOG_FILE;
use crate::{delete_file, split_key, Lake, LakeError, Result};

/// DuckLake-compatible retry controls for a lake commit (`ducklake_max_retry_count`,
/// `ducklake_retry_backoff`, `ducklake_retry_wait_ms`).
///
/// [`CommitSettings::default`] is exactly DuckLake's default configuration:
/// 10 retries, a 1.5 backoff factor, and a 100 ms base wait — the retry after
/// a conflict waits `retry_wait_ms`, the next `retry_wait_ms * retry_backoff`,
/// and so on.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitSettings {
    /// The maximum number of *retries* after a conflicting publish (DuckLake:
    /// `ducklake_max_retry_count`, default 10). One initial attempt plus this
    /// many retries are made before the commit fails.
    pub max_retry_count: u32,
    /// Exponential backoff factor between retries (DuckLake:
    /// `ducklake_retry_backoff`, default 1.5).
    pub retry_backoff: f64,
    /// Wait before the first retry, in milliseconds (DuckLake:
    /// `ducklake_retry_wait_ms`, default 100). Later retries multiply it by
    /// `retry_backoff`.
    pub retry_wait_ms: u64,
}

impl Default for CommitSettings {
    fn default() -> Self {
        Self {
            max_retry_count: 10,
            retry_backoff: 1.5,
            retry_wait_ms: 100,
        }
    }
}

impl CommitSettings {
    /// The wait before the `retry`-th retry (1-based): `retry_wait_ms *
    /// retry_backoff^(retry - 1)`, DuckLake's schedule.
    pub fn backoff_for(&self, retry: u32) -> Duration {
        let factor = if retry == 0 {
            1.0
        } else {
            self.retry_backoff.powi(retry as i32 - 1)
        };
        let ms = (self.retry_wait_ms as f64 * factor).max(0.0);
        Duration::from_millis(ms.min(u64::MAX as f64) as u64)
    }
}

/// One incremental commit: appended rows and/or positional deletes per
/// `db.table` key, plus whole-table drops.
///
/// Build one with [`Commit::append`]/[`Commit::delete_positions`]/
/// [`Commit::drop_table`] and publish it with [`Lake::commit`]. The files
/// themselves are written by the commit (that is what makes the
/// data-files-first publication order possible); the caller only describes
/// the change.
#[derive(Debug, Clone, Default)]
pub struct Commit {
    appends: BTreeMap<String, Arc<DataTable>>,
    deletes: BTreeMap<String, BTreeMap<String, Vec<u64>>>,
    drops: BTreeSet<String>,
    /// Per-table partition specification changes (wave 66): `Some(keys)` sets
    /// the spec, `None` clears it. Applied to the table's catalog entry when
    /// the commit publishes; the table must exist.
    partition_changes: BTreeMap<String, Option<Vec<PartitionKey>>>,
    /// Schema records this commit publishes (wave 65), per `db.table`: an
    /// added/dropped/renamed column's new [`TableSchema`](crate::schema::TableSchema).
    /// Set by [`Lake::commit_from_tables`]; a hand-built commit leaves it
    /// empty (no schema change).
    schemas: BTreeMap<String, crate::schema::TableSchema>,
    /// Table entries a row-level diff was computed against (`None` = the
    /// table must not exist), set by [`Lake::commit_from_tables`]. When one
    /// of these tables changed in the lake before the publish lands, the
    /// diff's positional mapping is no longer the whole story (a concurrent
    /// append would be silently dropped), so the commit refuses instead of
    /// rebasing: re-run it from a fresh base. A hand-built `Commit` has no
    /// expectations and rebases freely — appends are order-independent and
    /// positional deletes name their data file.
    expect: BTreeMap<String, Option<TableEntry>>,
}

impl Commit {
    /// An empty commit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `rows` (same schema as the target table) as one new data file.
    /// The table must exist in the lake unless the commit also creates it
    /// (the first append to a missing table creates it). At most one append
    /// per table per commit: a second call replaces the first's rows.
    pub fn append(&mut self, table: &str, rows: Arc<DataTable>) -> &mut Self {
        self.appends.insert(table.to_string(), rows);
        self
    }

    /// Delete the given 0-based positions of one existing data file
    /// (`file` is the lake-relative path as the manifest lists it). Positions
    /// are normalized (sorted, deduplicated); an empty list is a no-op.
    pub fn delete_positions(&mut self, table: &str, file: &str, positions: &[u64]) -> &mut Self {
        let positions = delete_file::normalize(positions.to_vec());
        if positions.is_empty() {
            if let Some(files) = self.deletes.get_mut(table) {
                files.remove(file);
                if files.is_empty() {
                    self.deletes.remove(table);
                }
            }
            return self;
        }
        self.deletes
            .entry(table.to_string())
            .or_default()
            .insert(file.to_string(), positions);
        self
    }

    /// Drop the whole table from the catalog (its old files stay on disk).
    pub fn drop_table(&mut self, table: &str) -> &mut Self {
        self.drops.insert(table.to_string());
        self
    }

    /// Set `table`'s partition specification (wave 66), mirroring DuckLake's
    /// `ALTER TABLE … SET PARTITIONED BY`: the published version records the
    /// spec, and the commit's own appends split into one data file per
    /// value set (later commits split the same way). Keys are the `.lake
    /// partition` texts — a column name (identity) or `bucket(N, column)`;
    /// any other transform is refused with DuckLake's own supported set in
    /// the message. The table must exist in the head when the commit
    /// publishes.
    pub fn set_partition<S: AsRef<str>>(&mut self, table: &str, keys: &[S]) -> Result<&mut Self> {
        let mut parsed = Vec::with_capacity(keys.len());
        for key in keys {
            let text = key.as_ref();
            let key = PartitionKey::parse(text).map_err(|_| LakeError::NotRepresentable {
                message: format!(
                    "unsupported partition key \"{text}\" for table \"{table}\": expected a \
                     column name or bucket(N, column) (DuckLake supports year, month, day, \
                     hour, epoch_year, epoch_month, epoch_day, epoch_hour, and bucket; this \
                     build implements identity columns and bucket)"
                ),
            })?;
            parsed.push(key);
        }
        if parsed.is_empty() {
            return Err(LakeError::NotRepresentable {
                message: format!(
                    "table \"{table}\": a partition specification needs at least one key"
                ),
            });
        }
        for (i, key) in parsed.iter().enumerate() {
            if parsed[..i].iter().any(|k| k.key_text() == key.key_text()) {
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "table \"{table}\": duplicate partition key \"{}\" (expression \
                         matches an earlier partition key)",
                        key.key_text()
                    ),
                });
            }
        }
        self.partition_changes
            .insert(table.to_string(), Some(parsed));
        Ok(self)
    }

    /// Clear `table`'s partition specification (existing files keep their
    /// recorded values; new writes are unpartitioned).
    pub fn clear_partition(&mut self, table: &str) -> &mut Self {
        self.partition_changes.insert(table.to_string(), None);
        self
    }

    /// True when the commit changes nothing.
    pub fn is_empty(&self) -> bool {
        self.appends.is_empty()
            && self.deletes.is_empty()
            && self.drops.is_empty()
            && self.partition_changes.is_empty()
            && self.schemas.is_empty()
    }

    /// Require `key`'s entry in the head to still equal `entry` (`None`: the
    /// table must not exist) when the commit publishes (crate-internal: set
    /// by [`Lake::commit_from_tables`]; see the `expect` field docs).
    pub(crate) fn expect_table(&mut self, key: &str, entry: Option<TableEntry>) {
        self.expect.insert(key.to_string(), entry);
    }

    /// Publish `key`'s new schema record with this commit (crate-internal:
    /// set by [`Lake::commit_from_tables`]; the record's epoch must be the
    /// base entry's + 1).
    pub(crate) fn set_schema(&mut self, key: &str, schema: crate::schema::TableSchema) {
        self.schemas.insert(key.to_string(), schema);
    }

    /// The appended sub-table for `table`, if any.
    pub fn appended(&self, table: &str) -> Option<&Arc<DataTable>> {
        self.appends.get(table)
    }

    /// The positions to delete from `file` of `table`, if any.
    pub fn deleted_positions(&self, table: &str, file: &str) -> Option<&[u64]> {
        self.deletes
            .get(table)
            .and_then(|f| f.get(file))
            .map(Vec::as_slice)
    }
}

/// What one [`Lake::commit`] published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReport {
    /// The published version; the unchanged head when nothing was committed.
    pub version: u64,
    /// The version the commit's change was computed against (the handle's
    /// version before the commit; a retry rebases but keeps this origin).
    pub base_version: u64,
    /// Conflicting publishes retried (0 = the first attempt won).
    pub retries: u32,
    /// True when a version was published; false for a no-op commit.
    pub committed: bool,
    /// Tables this commit changed (appends, deletes, or partition specs).
    pub tables_changed: usize,
    /// Tables this commit dropped.
    pub tables_dropped: usize,
    /// Tables whose partition specification this commit set, changed, or
    /// cleared.
    pub tables_repartitioned: usize,
    /// Tables whose schema record this commit published (wave 65: an
    /// `ADD`/`DROP`/`RENAME COLUMN` run against the writable session copy).
    pub schema_changes: usize,
    /// Data files written.
    pub data_files: usize,
    /// Delete files written.
    pub delete_files: usize,
    /// Bytes written: data files + delete files + commit manifest + snapshot
    /// + catalog.
    pub bytes_written: u64,
    /// Rows appended.
    pub rows_added: u64,
    /// Rows deleted by position.
    pub rows_deleted: u64,
}

impl CommitReport {
    /// The report of a commit that changed nothing.
    fn noop(version: u64) -> Self {
        Self {
            version,
            base_version: version,
            retries: 0,
            committed: false,
            tables_changed: 0,
            tables_dropped: 0,
            tables_repartitioned: 0,
            schema_changes: 0,
            data_files: 0,
            delete_files: 0,
            bytes_written: 0,
            rows_added: 0,
            rows_deleted: 0,
        }
    }
}

/// The row-level difference between a base table and its current content.
#[derive(Debug)]
pub(crate) struct TableDiff {
    /// `(data file, physical positions to delete)` pairs, in file order.
    pub(crate) deletes: Vec<(String, Vec<u64>)>,
    /// Indices of current rows to append (ascending, disjoint).
    pub(crate) appended: Vec<usize>,
}

impl TableDiff {
    pub(crate) fn is_empty(&self) -> bool {
        self.deletes.iter().all(|(_, p)| p.is_empty()) && self.appended.is_empty()
    }
}

/// Hash one row of `table` exactly (bit-exact for `DOUBLE`, value-exact
/// otherwise). `rows_identical` resolves any collision; the hash is only an
/// index. `(chunk, row-in-chunk)` addressing keeps the walk O(rows), never
/// O(rows × chunks): the diff runs over whole tables.
fn hash_row(table: &DataTable, chunk: usize, row: usize) -> u64 {
    // FNV-1a, 64-bit.
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for col in 0..table.num_cols() {
        mix_value(&mut h, &table.columns[col].chunks[chunk].get(row));
    }
    h
}

/// One current-schema column as a base-version row reads it (the write-side
/// projection of a schema delta, see [`crate::schema::DiffCol`]).
use crate::schema::DiffCol;

/// FNV-1a mixing of one value (shared by both sides of the diff so an equal
/// logical row hashes equal, whatever side it is read from).
fn mix_value(h: &mut u64, v: &Value) {
    fn mix(h: &mut u64, tag: u8, bytes: &[u8]) {
        const PRIME: u64 = 0x100_0000_01b3;
        *h ^= tag as u64;
        *h = h.wrapping_mul(PRIME);
        for &b in bytes {
            *h ^= b as u64;
            *h = h.wrapping_mul(PRIME);
        }
    }
    match v {
        Value::Null => mix(h, 0, &[]),
        Value::Boolean(b) => mix(h, 1, &[*b as u8]),
        Value::Integer(i) => mix(h, 2, &i.to_le_bytes()),
        Value::Bigint(i) => mix(h, 3, &i.to_le_bytes()),
        Value::Double(d) => mix(h, 4, &d.to_bits().to_le_bytes()),
        Value::Varchar(s) => mix(h, 5, s.as_bytes()),
        Value::Date(d) => mix(h, 6, &d.to_le_bytes()),
        Value::Timestamp(t) => mix(h, 7, &t.to_le_bytes()),
        Value::Decimal(v, scale) => {
            let mut b = [0u8; 17];
            b[..16].copy_from_slice(&v.to_le_bytes());
            b[16] = *scale;
            mix(h, 8, &b);
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            let mut b = [0u8; 16];
            b[..4].copy_from_slice(&months.to_le_bytes());
            b[4..8].copy_from_slice(&days.to_le_bytes());
            b[8..].copy_from_slice(&micros.to_le_bytes());
            mix(h, 9, &b);
        }
    }
}

/// [`hash_row`] for a base row read through a schema-delta view.
fn hash_row_view(base: &DataTable, chunk: usize, row: usize, view: &[DiffCol]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for col in view {
        match col {
            DiffCol::Base(index) => mix_value(&mut h, &base.columns[*index].chunks[chunk].get(row)),
            DiffCol::Added(v) => mix_value(&mut h, v.as_ref().unwrap_or(&Value::Null)),
        }
    }
    h
}

/// Bit-exact value equality (the diff's identity relation): `DOUBLE` by bit
/// pattern (NaN payloads and `-0.0` are themselves), everything else by
/// value.
fn values_identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

/// [`rows_identical`] for a base row read through a schema-delta view: `a` is
/// the base table, `b` the current one, and `view[i]` says where the current
/// column `i` comes from in `a` (a column, or a constant default).
fn rows_identical_view(
    a: &DataTable,
    ac: usize,
    ar: usize,
    b: &DataTable,
    bc: usize,
    br: usize,
    view: &[DiffCol],
) -> bool {
    view.len() == b.num_cols()
        && view.iter().enumerate().all(|(i, col)| {
            let bv = &b.columns[i].chunks[bc].get(br);
            match col {
                DiffCol::Base(bi) => values_identical(&a.columns[*bi].chunks[ac].get(ar), bv),
                DiffCol::Added(v) => values_identical(v.as_ref().unwrap_or(&Value::Null), bv),
            }
        })
}

/// The exact row-level diff between `base`'s logical rows (data files in
/// order minus their existing delete positions) and `current`.
///
/// `base_files` is the base version's physical layout; the caller has already
/// validated `base.num_rows()` against it. `view` projects a base row onto
/// the current schema (`view.len() == current.num_cols()`): an exact match,
/// a rename, or a constant for a column the base predates (wave 65). An
/// identity view (`DiffCol::Base(i)` per column) is the no-schema-change
/// case.
pub(crate) fn diff_table(
    base_files: &[(String, u64, Vec<u64>)],
    base: &DataTable,
    current: &DataTable,
    view: &[DiffCol],
) -> Result<TableDiff> {
    // Logical base row → (file index, physical position).
    let mut layout: Vec<(usize, u64)> = Vec::with_capacity(base.num_rows());
    for (fi, (_rel, rows, deleted)) in base_files.iter().enumerate() {
        let mut d = 0usize;
        for pos in 0..*rows {
            if d < deleted.len() && deleted[d] == pos {
                d += 1;
                continue;
            }
            layout.push((fi, pos));
        }
    }
    if layout.len() != base.num_rows() {
        return Err(LakeError::Internal {
            message: format!(
                "diff base disagrees with the version's file layout: {} rows vs {} logical rows",
                base.num_rows(),
                layout.len()
            ),
        });
    }

    // Index the current rows by hash: flat row → (chunk, row-in-chunk).
    let mut buckets: HashMap<u64, Vec<(usize, usize)>> = HashMap::new();
    let mut flat_of: Vec<usize> = Vec::with_capacity(current.num_chunks());
    {
        let mut flat = 0usize;
        for c in 0..current.num_chunks() {
            flat_of.push(flat);
            let len = current.columns[0].chunks[c].len();
            for r in 0..len {
                buckets
                    .entry(hash_row(current, c, r))
                    .or_default()
                    .push((c, r));
            }
            flat += len;
        }
    }
    let mut cursors: HashMap<u64, usize> = HashMap::new();
    let mut taken = vec![false; current.num_rows()];
    let mut deleted: Vec<Vec<u64>> = vec![Vec::new(); base_files.len()];

    let mut p = 0usize;
    for bc in 0..base.num_chunks() {
        let len = base.columns[0].chunks[bc].len();
        for br in 0..len {
            let h = hash_row_view(base, bc, br, view);
            let mut found: Option<(usize, usize)> = None;
            if let Some(list) = buckets.get(&h) {
                let cursor = cursors.entry(h).or_insert(0);
                while *cursor < list.len() && taken[flat_of[list[*cursor].0] + list[*cursor].1] {
                    *cursor += 1;
                }
                for &(cand_c, cand_r) in &list[*cursor..] {
                    if !taken[flat_of[cand_c] + cand_r]
                        && rows_identical_view(base, bc, br, current, cand_c, cand_r, view)
                    {
                        found = Some((cand_c, cand_r));
                        break;
                    }
                }
            }
            match found {
                Some((cand_c, cand_r)) => taken[flat_of[cand_c] + cand_r] = true,
                None => {
                    let (fi, pos) = layout[p];
                    deleted[fi].push(pos);
                }
            }
            p += 1;
        }
    }
    debug_assert_eq!(p, layout.len());

    let appended: Vec<usize> = (0..current.num_rows()).filter(|&r| !taken[r]).collect();
    let deletes = base_files
        .iter()
        .enumerate()
        .map(|(fi, (rel, _, _))| (rel.clone(), std::mem::take(&mut deleted[fi])))
        .collect();
    Ok(TableDiff { deletes, appended })
}

/// Build the sub-table of `src` holding the rows at `indices` (ascending),
/// with `src`'s schema. Contiguous runs share chunk storage where the
/// gather-per-chunk path allows.
pub(crate) fn sub_table(src: &DataTable, indices: &[usize]) -> Result<DataTable> {
    let mut out =
        DataTable::new(src.name.clone(), src.schema()).map_err(|e| LakeError::Engine {
            context: format!("preparing the appended rows of \"{}\"", src.name),
            source: e,
        })?;
    if indices.is_empty() {
        return Ok(out);
    }
    let full = indices.len() == src.num_rows() && indices[0] == 0;
    let mut seen = 0usize;
    let mut start = 0usize;
    for c in 0..src.num_chunks() {
        let chunk = src.scan_chunk(c).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {c} of \"{}\" disappeared", src.name),
        })?;
        let end = start + chunk.num_rows;
        // The indices that fall in this chunk, chunk-relative.
        let mut sel: Vec<usize> = Vec::new();
        while seen < indices.len() && indices[seen] < end {
            sel.push(indices[seen] - start);
            seen += 1;
        }
        if sel.is_empty() {
            start = end;
            continue;
        }
        if full {
            out.append_shared_chunk(&chunk.columns)
                .map_err(|e| LakeError::Engine {
                    context: format!("appending rows of \"{}\"", src.name),
                    source: e,
                })?;
        } else {
            let gathered = chunk.gather(&sel);
            out.append_shared_chunk(&gathered.columns)
                .map_err(|e| LakeError::Engine {
                    context: format!("appending rows of \"{}\"", src.name),
                    source: e,
                })?;
        }
        start = end;
    }
    Ok(out)
}

/// Project a base-version table onto the current schema through a schema
/// delta's view: `DiffCol::Base(i)` columns are shared (`Arc` clones, no
/// copy), `DiffCol::Added` columns become constants chunk-aligned with the
/// base. Output names/types come from `current` (the shape the result must
/// have).
fn project_base_table(
    base: &DataTable,
    current: &DataTable,
    view: &[DiffCol],
) -> Result<DataTable> {
    let mut columns = Vec::with_capacity(view.len());
    for (i, col) in view.iter().enumerate() {
        match col {
            DiffCol::Base(bi) => {
                let mut c = base.columns[*bi].clone();
                c.name = current.columns[i].name.clone();
                columns.push(c);
            }
            DiffCol::Added(v) => columns.push(crate::schema::constant_column(
                &current.columns[i].name,
                current.columns[i].dtype,
                v.as_ref(),
                base,
            )?),
        }
    }
    Ok(DataTable::from_columns(
        current.name.clone(),
        columns,
        base.num_rows(),
    ))
}

/// The base table's logical content after a diff lands: base rows minus the
/// deleted positions, plus the appended rows, in merge-on-read order (the
/// exact order the lake's readers produce), **in the current schema** (the
/// view projects renamed columns and materializes added columns' defaults).
/// Chunks are shared where possible: this is the reference a writable
/// attachment diffs against next, not a persisted artifact.
fn rebuild_logical(
    base: &DataTable,
    current: &DataTable,
    view: &[DiffCol],
    layout: &[(String, u64, Vec<u64>)],
    diff: &TableDiff,
) -> Result<DataTable> {
    let base = project_base_table(base, current, view)?;
    // Physical (file, position) → logical index, and the deleted logical
    // indices.
    let deleted: BTreeSet<(usize, u64)> = diff
        .deletes
        .iter()
        .enumerate()
        .flat_map(|(fi, (_, positions))| positions.iter().map(move |&p| (fi, p)))
        .collect();
    let mut deleted_logical: BTreeSet<usize> = BTreeSet::new();
    let mut index = 0usize;
    for (fi, (_rel, rows, base_deleted)) in layout.iter().enumerate() {
        let mut d = 0usize;
        for pos in 0..*rows {
            if d < base_deleted.len() && base_deleted[d] == pos {
                d += 1;
                continue;
            }
            if deleted.contains(&(fi, pos)) {
                deleted_logical.insert(index);
            }
            index += 1;
        }
    }
    let mut out =
        DataTable::new(base.name.clone(), base.schema()).map_err(|e| LakeError::Engine {
            context: format!("rebuilding the writable base of \"{}\"", base.name),
            source: e,
        })?;
    let mut start = 0usize;
    for c in 0..base.num_chunks() {
        let chunk = base.scan_chunk(c).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {c} of \"{}\" disappeared", base.name),
        })?;
        let end = start + chunk.num_rows;
        let keep: Vec<usize> = (start..end)
            .filter(|r| !deleted_logical.contains(r))
            .map(|r| r - start)
            .collect();
        if !keep.is_empty() {
            let gathered = chunk.gather(&keep);
            out.append_shared_chunk(&gathered.columns)
                .map_err(|e| LakeError::Engine {
                    context: format!("rebuilding the writable base of \"{}\"", base.name),
                    source: e,
                })?;
        }
        start = end;
    }
    if !diff.appended.is_empty() {
        let sub = sub_table(current, &diff.appended)?;
        crate::copy_chunks(&sub, &mut out, "the appended rows")?;
    }
    Ok(out)
}

/// Materialize a version's tables in lake (merge-on-read) order: the exact
/// logical content `commit_from_tables`' diff and the readers agree on.
fn materialize_tables(
    lake: &Lake,
    version: Option<u64>,
) -> Result<(u64, BTreeMap<String, Arc<DataTable>>)> {
    let (attachment, report) = crate::attach::build(lake, version)?;
    let mut base = BTreeMap::new();
    for t in attachment.tables() {
        let key = format!("{}.{}", t.db, t.table);
        let table = attachment
            .load(&t.db, &t.table)
            .map_err(|e| LakeError::Engine {
                context: format!("materializing table \"{key}\" for writing"),
                source: e,
            })?;
        base.insert(key, table);
    }
    Ok((report.version, base))
}

/// A writable attachment: one lake pinned at `base_version` with the logical
/// content of every table materialized in memory.
///
/// This is the write-side of `.lake attach <dir> write` (DuckLake's attach
/// with writes enabled, as far as this engine's attach route goes): the
/// caller materializes the base tables into its store, mutates them with
/// ordinary DML, and [`WritableAttachment::commit`] publishes exactly the
/// difference as one incremental lake version. Nothing else about the lake is
/// rewritten — a commit that appends a few rows writes only those rows.
///
/// The attachment is *not* registered with the engine (unlike the read-only
/// attach): its tables live in the caller's store, so the engine needs no
/// special path and a durable session's WAL logs the mutations exactly like
/// any other store write. Crash safety: the base is immutable, the commit is
/// an idempotent diff against it, so replaying a recovered store through
/// `attach` + `commit` always converges on the same lake state (nothing is
/// lost; a commit that already landed re-diffs to an empty change).
#[derive(Debug)]
pub struct WritableAttachment {
    lake: Lake,
    base_version: u64,
    /// `db.table` → logical content at `base_version`, in scan order.
    base: BTreeMap<String, Arc<DataTable>>,
}

impl WritableAttachment {
    /// Open the lake at `location` (a directory or `s3://bucket/prefix`) and
    /// materialize the pinned version's tables (the latest, or `version`).
    pub fn open(location: &str, version: Option<u64>) -> Result<Self> {
        Self::attach(Lake::open_url(location)?, version)
    }

    /// [`WritableAttachment::open`] with at-rest encryption: a keyed lake is
    /// opened and committed under `key` (a plaintext lake is refused — a key
    /// is never silently ignored).
    pub fn open_with_key(
        location: &str,
        version: Option<u64>,
        key: crate::EncryptionKey,
    ) -> Result<Self> {
        Self::attach(Lake::open_url_with_key(location, key)?, version)
    }

    /// [`WritableAttachment::open`] on an already-open [`Lake`].
    pub fn attach(lake: Lake, version: Option<u64>) -> Result<Self> {
        let (base_version, base) = materialize_tables(&lake, version)?;
        Ok(Self {
            lake,
            base_version,
            base,
        })
    }

    /// The lake location (as opened).
    pub fn location(&self) -> &std::path::Path {
        self.lake.path()
    }

    /// The pinned base version: what the materialized content corresponds to.
    pub fn base_version(&self) -> u64 {
        self.base_version
    }

    /// The attached tables as `(db.table, rows)` at the base version.
    pub fn tables(&self) -> Vec<(String, u64)> {
        self.base
            .iter()
            .map(|(key, table)| (key.clone(), table.num_rows() as u64))
            .collect()
    }

    /// One base table's materialized logical content.
    pub fn base_table(&self, key: &str) -> Option<&Arc<DataTable>> {
        self.base.get(key)
    }

    /// True when the materialized base holds a table under `key`.
    pub fn contains(&self, key: &str) -> bool {
        self.base.contains_key(key)
    }

    /// The tables of this attachment whose content in `db` differs from the
    /// base (sorted). Tables that vanished from the store count as changed
    /// (a commit would drop them).
    pub fn changed_tables(&self, db: &PinkDb) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for (key, base_table) in &self.base {
            let (dbname, table) = split_key(key, self.location())?;
            let changed = match db.table_in(dbname, table) {
                Ok(current) => {
                    let files = self.base_layout(key)?;
                    let entry = self.base_entry(key)?;
                    let delta = crate::schema::plan_schema_delta(key, &entry, base_table, current)?;
                    !diff_table(&files, base_table, current, &delta.view)?.is_empty()
                        || delta.record.is_some()
                }
                Err(_) => true,
            };
            if changed {
                out.push(key.clone());
            }
        }
        Ok(out)
    }

    /// Commit the difference between the materialized base and the same
    /// tables' current content in `db` as one incremental lake version.
    /// Tables of the base missing from `db` are dropped from the catalog (an
    /// explicit `DROP TABLE` propagates); store tables that are not part of
    /// the attachment are ignored (use `Lake::write_from_store` — `.lake
    /// save` — to publish new tables).
    ///
    /// A schema change (an `ADD COLUMN`/`DROP COLUMN`/`RENAME COLUMN` run
    /// against the session table, wave 65) publishes a new schema record with
    /// the version: old data files stay untouched and readable through the
    /// name/default mapping, appended rows are written under the new schema,
    /// and positional delete files keep their positions. A `RENAME TABLE` is
    /// **not** published through a commit (a base table that disappeared is
    /// treated as a drop); the CLI refuses the commit when it detects one.
    ///
    /// On success the pinned base advances to the published version with the
    /// tables' logical content **in the lake's merge-on-read order**, so the
    /// next diff's positional mapping stays exact.
    pub fn commit(&mut self, db: &PinkDb, settings: &CommitSettings) -> Result<CommitReport> {
        // A base table that vanished from the session while a same-shaped
        // sibling appeared is almost certainly a RENAME TABLE (the engine
        // supports it, but an incremental commit publishes data changes, not
        // renames — publishing it as a drop would silently lose the new
        // name's rows). Refuse with the way out instead.
        for (key, base_table) in &self.base {
            let (dbname, table) = split_key(key, self.location())?;
            if db.table_in(dbname, table).is_ok() {
                continue;
            }
            for other in db.list_tables_in(dbname) {
                let other_key = format!("{dbname}.{other}");
                if self.base.contains_key(&other_key) || other_key.eq_ignore_ascii_case(key) {
                    continue;
                }
                if db
                    .table_in(dbname, &other)
                    .is_ok_and(|t| t.schema() == base_table.schema())
                {
                    return Err(LakeError::NotRepresentable {
                        message: format!(
                            "table \"{key}\" is gone from the session while \"{other_key}\" \
                             (the same shape) appeared: a RENAME TABLE is not published \
                             through .lake commit (incremental commits move rows and schema, \
                             not names). Publish the store as a full snapshot with \
                             `.lake save <dir>`, or keep the name and commit the schema/row \
                             change instead"
                        ),
                    });
                }
            }
        }
        let base: Vec<(String, Arc<DataTable>)> = self
            .base
            .iter()
            .map(|(k, t)| (k.clone(), Arc::clone(t)))
            .collect();
        let mut current: Vec<(String, Arc<DataTable>)> = Vec::new();
        for (key, _) in &base {
            let (dbname, table) = split_key(key, self.location())?;
            if let Ok(t) = db.table_in(dbname, table) {
                current.push((key.clone(), Arc::new(t.clone())));
            }
        }
        let (commit, after) = self
            .lake
            .plan_tables_commit(self.base_version, &base, &current)?;
        let report = self.lake.commit_with_settings(&commit, settings)?;
        if report.committed {
            // The planned content is exact: `plan_tables_commit` marked every
            // table in scope as expected-unchanged, so a rebase can only have
            // landed on versions that touched tables outside this attachment
            // — the published content for these tables is `after`.
            self.base_version = report.version;
            self.base = after;
        }
        Ok(report)
    }

    /// Re-pin the base to the store's current content at `version`: used
    /// after a full `.lake save` of the same store published `version`, so
    /// the writable attachment continues from the new head with no pending
    /// changes. Tables of the attachment missing from the store are dropped
    /// from the pin (the save removed them from the lake too).
    pub fn repin_at(&mut self, version: u64, db: &PinkDb) -> Result<()> {
        let mut base = BTreeMap::new();
        for key in self.base.keys() {
            let (dbname, table) = split_key(key, self.location())?;
            if let Ok(t) = db.table_in(dbname, table) {
                base.insert(key.clone(), Arc::new(t.clone()));
            }
        }
        self.base = base;
        self.base_version = version;
        Ok(())
    }

    /// The base version's physical layout of one table: `(file, rows,
    /// existing delete positions)` for every data file, in manifest order.
    fn base_layout(&self, key: &str) -> Result<Vec<(String, u64, Vec<u64>)>> {
        let (manifest, manifest_path) = self.lake.manifest(Some(self.base_version))?;
        let entry = manifest.tables.get(key).ok_or_else(|| LakeError::Corrupt {
            path: manifest_path.clone(),
            message: format!(
                "the writable attachment holds table \"{key}\", but version {} does not",
                self.base_version
            ),
        })?;
        self.lake.entry_layout(entry, key, &manifest_path)
    }

    /// The base version's catalog entry of one table (for schema deltas).
    fn base_entry(&self, key: &str) -> Result<TableEntry> {
        let (manifest, manifest_path) = self.lake.manifest(Some(self.base_version))?;
        manifest
            .tables
            .get(key)
            .cloned()
            .ok_or_else(|| LakeError::Corrupt {
                path: manifest_path,
                message: format!(
                    "the writable attachment holds table \"{key}\", but version {} does not",
                    self.base_version
                ),
            })
    }
}

/// [`Lake::entry_layout`]'s body, storage-parameterized: the per-version
/// attach path holds no [`Lake`] handle, but a time-travel view must validate
/// data-file layouts and delete files exactly like a normal attach.
pub(crate) fn entry_layout(
    storage: &dyn crate::storage::Storage,
    entry: &TableEntry,
    key: &str,
    manifest_path: &std::path::Path,
) -> Result<Vec<(String, u64, Vec<u64>)>> {
    let mut out: Vec<(String, u64, Vec<u64>)> = Vec::with_capacity(entry.files.len());
    for file in &entry.files {
        let rows = match file.rows() {
            Some(rows) => rows,
            None if entry.files.len() == 1 => entry.rows,
            None => {
                return Err(LakeError::Corrupt {
                    path: manifest_path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" lists a data file without a row count (\"{}\") \
                         among several; incremental commits need a format_version 4 entry",
                        file.path()
                    ),
                })
            }
        };
        out.push((file.path().to_string(), rows, Vec::new()));
    }
    for d in &entry.deletes {
        let idx = out
            .iter()
            .position(|(rel, _, _)| rel == &d.file)
            .ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.to_path_buf(),
                message: format!(
                    "table \"{key}\": delete file \"{}\" targets \"{}\", which is not one \
                     of the table's data files",
                    d.path, d.file
                ),
            })?;
        let path = storage.abs(&d.path);
        let bytes = storage.read(&d.path, &format!("reading {}", path.display()))?;
        let positions = delete_file::decode(&bytes, &path)?;
        if positions.len() as u64 != d.count {
            return Err(LakeError::Corrupt {
                path,
                message: format!(
                    "table \"{key}\": delete file declares {} positions, holds {}",
                    d.count,
                    positions.len()
                ),
            });
        }
        delete_file::validate_bounds(&positions, out[idx].1, &d.file, &path)?;
        out[idx].2.extend(positions);
    }
    for (_, _, deleted) in &mut out {
        deleted.sort_unstable();
        deleted.dedup();
    }
    Ok(out)
}

impl Lake {
    /// Publish an incremental commit with the default [`CommitSettings`]
    /// (DuckLake's retry defaults).
    pub fn commit(&mut self, commit: &Commit) -> Result<CommitReport> {
        self.commit_with_settings(commit, &CommitSettings::default())
    }

    /// Publish an incremental commit: appended rows become new immutable data
    /// files, positional deletes become delete files, and the new version's
    /// manifest lists the previous files plus these — **no existing file is
    /// rewritten or replaced**. Publication order is data and delete files →
    /// `commits/<v>.json` → `snapshots/<v>.json` → `catalog.json`, the same
    /// crash-safe order a full save uses.
    ///
    /// A stale base (another writer moved the catalog) and a conflicting
    /// publisher are both retryable under `settings` — DuckLake's optimistic
    /// writer-retry semantics: the attempt waits `retry_wait_ms *
    /// retry_backoff^(retry-1)`, rebases the already-written immutable files
    /// onto the new head, and re-publishes. The commit fails with a clear
    /// conflict error when the retry budget is exhausted, or immediately on a
    /// wrong delta (a delete target the head no longer contains, positions
    /// already deleted).
    pub fn commit_with_settings(
        &mut self,
        commit: &Commit,
        settings: &CommitSettings,
    ) -> Result<CommitReport> {
        if commit.is_empty() {
            return Ok(CommitReport::noop(self.version));
        }
        for key in commit
            .appends
            .keys()
            .chain(commit.deletes.keys())
            .chain(commit.drops.iter())
        {
            split_key(key, &self.root)?;
        }
        let origin = self.version;
        let mut attempt: u32 = 0;
        let mut uploaded: Option<UploadedCommit> = None;
        loop {
            if attempt > 0 {
                // DuckLake's schedule: the retry after a conflict waits
                // `retry_wait_ms`, and each further retry multiplies by
                // `retry_backoff`.
                std::thread::sleep(settings.backoff_for(attempt));
                self.rebase_catalog()?;
            }
            if uploaded.is_none() {
                // The first attempt starts only from a base that is still the
                // head; anything else is the retryable conflict class (no file
                // is written before this passes).
                match self.ensure_catalog_unchanged() {
                    Ok(_) => {}
                    Err(e @ LakeError::Conflict { .. }) => {
                        if attempt >= settings.max_retry_count {
                            return Err(if attempt == 0 {
                                e
                            } else {
                                retry_exhausted(e, attempt)
                            });
                        }
                        attempt += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
                let specs = self.upload_partition_specs(commit)?;
                uploaded = Some(self.upload_commit(commit, &specs)?);
            }
            match self.try_publish_commit(
                commit,
                uploaded.as_ref().expect("uploaded before publishing"),
                origin,
                attempt,
            ) {
                Ok(report) => return Ok(report),
                Err(PublishFailure::Fatal(e)) => return Err(e),
                Err(PublishFailure::Retry(e)) => {
                    if attempt >= settings.max_retry_count {
                        return Err(retry_exhausted(e, attempt));
                    }
                    attempt += 1;
                }
            }
        }
    }

    /// Commit the difference between `base` (the logical content of
    /// `base_version`'s tables, materialized **in the base version's logical
    /// scan order**: data files in manifest order, minus deleted positions)
    /// and `current` (the caller's tables now) as one incremental version.
    ///
    /// `base` must hold exactly the tables the caller wants the commit to
    /// manage; each entry's row count must equal the base version's logical
    /// row count (a mismatch is [`LakeError::Corrupt`], never a silently
    /// wrong delta). Tables in `current` but not in `base` are created;
    /// tables in `base` but not in `current` are dropped. An empty
    /// difference publishes nothing.
    pub fn commit_from_tables(
        &mut self,
        base_version: u64,
        base: &[(String, Arc<DataTable>)],
        current: &[(String, Arc<DataTable>)],
        settings: &CommitSettings,
    ) -> Result<CommitReport> {
        let (commit, _after) = self.plan_tables_commit(base_version, base, current)?;
        self.commit_with_settings(&commit, settings)
    }

    /// Set one table's partition specification, publishing exactly one
    /// version — DuckLake's `ALTER TABLE … SET PARTITIONED BY`, with the
    /// pinkdb surface `.lake partition <dir> <table> by (…)`.
    ///
    /// Keys are parsed like [`Commit::set_partition`] (a column name, or
    /// `bucket(N, column)`); every identity key's column must exist in the
    /// table's schema (read the way `.lake attach` reads it: from the data
    /// files' statistics blocks or Vortex footers). Existing data files keep
    /// their recorded partition values — like DuckLake, the spec governs new
    /// writes, not files already published — and re-setting the identical
    /// spec is a no-op ([`CommitReport::committed`] `false`). A concurrent
    /// publish to the table makes the call fail with the usual conflict.
    pub fn set_partition_spec(
        &mut self,
        table: &str,
        keys: &[String],
        settings: &CommitSettings,
    ) -> Result<CommitReport> {
        split_key(table, &self.root)?;
        let (manifest, manifest_path) = self.manifest(None)?;
        let entry = manifest
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!(
                    "table \"{table}\" does not exist in the lake; create it (and commit it) \
                     before setting a partition specification"
                ),
            })?;
        let mut commit = Commit::new();
        commit.set_partition(table, keys)?;
        let parsed = commit
            .partition_changes
            .get(table)
            .and_then(|spec| spec.clone())
            .expect("set_partition recorded the spec");
        if entry.partition_by.as_ref() == Some(&parsed) {
            return Ok(CommitReport::noop(self.version));
        }
        // The table's schema lives in its data files; attach reads it without
        // materializing anything.
        let (tdb, ttable) = split_key(table, &self.root)?;
        let (attachment, _) = crate::attach::build_from(&self.storage, &self.root, None)?;
        let schema = attachment
            .tables()
            .iter()
            .find(|t| t.db.eq_ignore_ascii_case(tdb) && t.table.eq_ignore_ascii_case(ttable))
            .map(|t| t.schema.clone())
            .ok_or_else(|| LakeError::Internal {
                message: format!("the attachment is missing table \"{table}\""),
            })?;
        for key in &parsed {
            if crate::zone::schema_column_index(&schema, &key.column).is_none() {
                return Err(LakeError::NotRepresentable {
                    message: format!(
                        "table \"{table}\": partition key column \"{}\" does not exist \
                         (columns: {})",
                        key.column,
                        schema
                            .iter()
                            .map(|(n, _)| n.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
        }
        commit.expect_table(table, Some(entry));
        self.commit_with_settings(&commit, settings)
    }

    /// Clear one table's partition specification, publishing exactly one
    /// version: new writes are unpartitioned, existing files keep their
    /// recorded values (they are simply no longer interpreted). Clearing a
    /// table with no spec is a no-op.
    pub fn clear_partition_spec(
        &mut self,
        table: &str,
        settings: &CommitSettings,
    ) -> Result<CommitReport> {
        split_key(table, &self.root)?;
        let (manifest, manifest_path) = self.manifest(None)?;
        let entry = manifest
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!("table \"{table}\" does not exist in the lake"),
            })?;
        if entry.partition_by.is_none() {
            return Ok(CommitReport::noop(self.version));
        }
        let mut commit = Commit::new();
        commit.clear_partition(table);
        commit.expect_table(table, Some(entry));
        self.commit_with_settings(&commit, settings)
    }

    /// The planning half of [`Lake::commit_from_tables`]: the commit to
    /// publish **and** the tables' logical content after it lands (base order
    /// minus the deleted positions, plus the appended rows in commit order).
    /// A writable attachment keeps that as its next base so its diffs stay
    /// positionally accurate.
    pub(crate) fn plan_tables_commit(
        &self,
        base_version: u64,
        base: &[(String, Arc<DataTable>)],
        current: &[(String, Arc<DataTable>)],
    ) -> Result<(Commit, BTreeMap<String, Arc<DataTable>>)> {
        let (manifest, manifest_path) = self.manifest(Some(base_version))?;
        let base_map: BTreeMap<&str, &Arc<DataTable>> =
            base.iter().map(|(k, t)| (k.as_str(), t)).collect();
        let current_map: BTreeMap<&str, &Arc<DataTable>> =
            current.iter().map(|(k, t)| (k.as_str(), t)).collect();
        let mut commit = Commit::new();
        let mut after: BTreeMap<String, Arc<DataTable>> = BTreeMap::new();
        for (key, table) in &current_map {
            match base_map.get(key) {
                Some(base_table) => {
                    let entry = manifest
                        .tables
                        .get(*key)
                        .ok_or_else(|| LakeError::Corrupt {
                            path: manifest_path.clone(),
                            message: format!(
                                "table \"{key}\" is not part of base version {base_version}"
                            ),
                        })?
                        .clone();
                    commit.expect_table(key, Some(entry.clone()));
                    if base_table.num_rows() as u64 != entry.rows {
                        return Err(LakeError::Corrupt {
                            path: manifest_path.clone(),
                            message: format!(
                                "the base content of \"{key}\" has {} rows, but base version \
                                 {base_version} has {}",
                                base_table.num_rows(),
                                entry.rows
                            ),
                        });
                    }
                    // The schema delta (wave 65): a rename maps old files to
                    // the new name, an added column materializes its default
                    // on read, a dropped one hides. Published with the commit
                    // when it changed anything.
                    let delta = crate::schema::plan_schema_delta(key, &entry, base_table, table)?;
                    if let Some(record) = &delta.record {
                        commit.set_schema(key, record.clone());
                    }
                    let layout = self.entry_layout(&entry, key, &manifest_path)?;
                    let diff = diff_table(&layout, base_table, table, &delta.view)?;
                    if diff.is_empty() && delta.record.is_none() {
                        after.insert((*key).to_string(), Arc::clone(table));
                        continue;
                    }
                    for (file, positions) in &diff.deletes {
                        if !positions.is_empty() {
                            commit.delete_positions(key, file, positions);
                        }
                    }
                    if !diff.appended.is_empty() {
                        let sub = sub_table(table, &diff.appended)?;
                        commit.append(key, Arc::new(sub));
                    }
                    after.insert(
                        (*key).to_string(),
                        Arc::new(rebuild_logical(
                            base_table,
                            table,
                            &delta.view,
                            &layout,
                            &diff,
                        )?),
                    );
                }
                None => {
                    // A table the base version does not have: create it with
                    // its whole content as one data file — and refuse if
                    // another writer created it in between (the two contents
                    // are not the same change). It gets a schema record from
                    // the start (epoch 1).
                    commit.expect_table(key, None);
                    commit.append(key, Arc::clone(table));
                    let record = crate::schema::TableSchema {
                        version: 1,
                        columns: table
                            .columns
                            .iter()
                            .map(|c| crate::schema::SchemaColumn {
                                name: c.name.clone(),
                                dtype: c.dtype,
                                default: c.default.clone().filter(|v| !v.is_null()),
                                added: 1,
                            })
                            .collect(),
                        renames: Vec::new(),
                    };
                    commit.set_schema(key, record);
                    after.insert((*key).to_string(), Arc::clone(table));
                }
            }
        }
        for key in base_map.keys() {
            if !current_map.contains_key(key) {
                commit.expect_table(key, manifest.tables.get(*key).cloned());
                commit.drop_table(key);
            }
        }
        Ok((commit, after))
    }

    /// The physical layout of one entry: `(data file, rows, the positions its
    /// existing delete files drop)` per file, in manifest order. Reads every
    /// delete file (they are small) and validates it against the manifest.
    /// A v1–v3 entry's single file carries no per-file count; the entry's
    /// logical count *is* that file's count (the loader enforces it), so it
    /// is used. Delegates to the storage-parameterized free function below.
    pub(crate) fn entry_layout(
        &self,
        entry: &TableEntry,
        key: &str,
        manifest_path: &std::path::Path,
    ) -> Result<Vec<(String, u64, Vec<u64>)>> {
        entry_layout(&*self.storage, entry, key, manifest_path)
    }

    /// Write every data and delete file the commit adds (immutable, uniquely
    /// named), returning what the publish loop should reference. The appends
    /// follow the head's partition specification (or the spec this same
    /// commit sets for a table it creates): each distinct partition value set
    /// becomes its own data file carrying those values.
    fn upload_commit(
        &self,
        commit: &Commit,
        specs: &BTreeMap<String, Vec<PartitionKey>>,
    ) -> Result<UploadedCommit> {
        let mut uploaded = UploadedCommit::default();
        for (key, rows) in &commit.appends {
            let keys = specs.get(key).map(Vec::as_slice).unwrap_or(&[]);
            let (files, bytes) = self.write_data_files(key, None, keys, rows)?;
            uploaded.bytes += bytes;
            uploaded.appends.insert(key.clone(), files);
        }
        for (key, files) in &commit.deletes {
            for (file, positions) in files {
                let (dref, bytes) = self.write_delete_file(key, file, positions)?;
                uploaded.bytes += bytes;
                uploaded
                    .deletes
                    .entry(key.clone())
                    .or_default()
                    .insert(dref.file.clone(), dref);
            }
        }
        Ok(uploaded)
    }

    /// The partition specs the commit's appends must be split by: the head
    /// version's spec per appended table, or — for a table this commit
    /// creates (`partition_changes` without a head entry) — the spec being
    /// set. Read without touching this handle's cached version (the commit
    /// loop has already checked the catalog is still the head).
    fn upload_partition_specs(
        &self,
        commit: &Commit,
    ) -> Result<BTreeMap<String, Vec<PartitionKey>>> {
        let head = crate::read_catalog(&*self.storage, &self.root)?;
        let mut out = BTreeMap::new();
        for key in commit.appends.keys() {
            let spec = head
                .tables
                .get(key)
                .and_then(|entry| entry.partition_by.clone())
                .or_else(|| commit.partition_changes.get(key).cloned().flatten());
            if let Some(spec) = spec {
                out.insert(key.clone(), spec);
            }
        }
        Ok(out)
    }

    /// One publish attempt: build the new catalog from the current head plus
    /// the commit's files and try to publish it. `Retry` failures are the
    /// conflicting-publish class the retry loop rebases; `Fatal` failures are
    /// wrong deltas (never silently applied).
    fn try_publish_commit(
        &mut self,
        commit: &Commit,
        uploaded: &UploadedCommit,
        origin: u64,
        retries: u32,
    ) -> std::result::Result<CommitReport, PublishFailure> {
        let head = self
            .read_catalog_for_commit()
            .map_err(PublishFailure::Fatal)?;
        let version = self
            .version
            .checked_add(1)
            .filter(|v| *v <= crate::MAX_VERSION)
            .ok_or_else(|| {
                PublishFailure::Fatal(LakeError::Overflow {
                    path: self.root.clone(),
                    version: self.version,
                })
            })?;
        let mut tables = head.tables.clone();
        let mut deltas: BTreeMap<String, TableDelta> = BTreeMap::new();
        let mut rows_added = 0u64;
        let mut rows_deleted = 0u64;
        // A diff-based commit (commit_from_tables) requires the tables it
        // diffed to be untouched: anything else would silently drop another
        // writer's rows from the positional mapping. A hand-built commit has
        // no expectations and rebases freely.
        for (key, expected) in &commit.expect {
            if tables.get(key) != expected.as_ref() {
                return Err(PublishFailure::Fatal(LakeError::Conflict {
                    path: self.storage.abs(CATALOG_FILE),
                    message: format!(
                        "table \"{key}\" changed in the lake since this change was computed \
                         (another writer published to it); the stored difference cannot be \
                         applied on top — re-attach or recompute the change against the \
                         current version"
                    ),
                }));
            }
        }
        // Drops first: a dropped table's entry disappears; its files stay.
        for key in &commit.drops {
            if tables.remove(key).is_some() {
                deltas.insert(key.clone(), TableDelta::default());
            }
        }
        for (key, files) in &uploaded.appends {
            let appended = commit.appends.get(key).expect("uploaded append has rows");
            let added = appended.num_rows() as u64;
            rows_added += added;
            // A new file's schema epoch: the record the commit publishes for
            // this table, else the entry's current epoch (a recorded entry's
            // files always carry one), else none (legacy entry).
            let file_version = commit
                .schemas
                .get(key)
                .map(|s| s.version)
                .or_else(|| tables.get(key).map(|e| e.schema_version).filter(|v| *v > 0));
            let files: Vec<crate::catalog::FileRef> = files
                .iter()
                .map(|f| f.with_schema_version(file_version))
                .collect();
            match tables.get_mut(key) {
                Some(entry) => {
                    entry.files.extend(files.iter().cloned());
                    entry.rows = entry.rows.checked_add(added).ok_or_else(|| {
                        PublishFailure::Fatal(LakeError::Corrupt {
                            path: self.storage.abs(CATALOG_FILE),
                            message: format!("table \"{key}\" overflows u64 rows"),
                        })
                    })?;
                    let delta = deltas.entry(key.clone()).or_default();
                    delta.data_files.extend(files.iter().cloned());
                    delta.rows_added += added;
                }
                None => {
                    if commit.deletes.contains_key(key) || commit.drops.contains(key) {
                        return Err(PublishFailure::Fatal(LakeError::Corrupt {
                            path: self.storage.abs(CATALOG_FILE),
                            message: format!(
                                "table \"{key}\" does not exist; a commit cannot delete from or \
                                 drop it"
                            ),
                        }));
                    }
                    // A table this commit creates takes the partition spec the
                    // same commit sets for it, if any (its appends were
                    // already split by that spec — see
                    // `upload_partition_specs`).
                    let spec = commit.partition_changes.get(key).cloned().flatten();
                    tables.insert(
                        key.clone(),
                        TableEntry {
                            files: files.clone(),
                            rows: added,
                            format: Some(self.write_format()),
                            stats: None,
                            deletes: Vec::new(),
                            partition_by: spec.clone(),
                            schema_version: 0,
                            columns: None,
                            renames: Vec::new(),
                        },
                    );
                    let delta = deltas.entry(key.clone()).or_default();
                    delta.data_files.extend(files.iter().cloned());
                    delta.rows_added += added;
                    if spec.is_some() {
                        delta.partition_changed = true;
                        delta.partition_by = spec;
                    }
                }
            }
        }
        // The schema records this commit publishes (wave 65): the new epoch
        // and columns; every file that predates the record (or predates this
        // change) is stamped with the epoch it was written under.
        for (key, record) in &commit.schemas {
            let Some(entry) = tables.get_mut(key) else {
                return Err(PublishFailure::Fatal(LakeError::Corrupt {
                    path: self.storage.abs(CATALOG_FILE),
                    message: format!(
                        "the commit publishes a schema for table \"{key}\", but the head \
                         version does not have it"
                    ),
                }));
            };
            let previous = record.version.saturating_sub(1).max(1);
            for file in &mut entry.files {
                if file.schema_version().is_none() {
                    *file = file.with_schema_version(Some(previous));
                }
            }
            let (version, columns, renames) =
                record.to_entry_fields().map_err(PublishFailure::Fatal)?;
            entry.schema_version = version;
            entry.columns = Some(columns);
            entry.renames = renames;
        }
        for (key, files) in &uploaded.deletes {
            let entry = tables.get_mut(key).ok_or_else(|| {
                PublishFailure::Fatal(LakeError::Corrupt {
                    path: self.storage.abs(CATALOG_FILE),
                    message: format!("table \"{key}\" does not exist in the head version"),
                })
            })?;
            let positions = commit
                .deletes
                .get(key)
                .expect("uploaded deletes have positions");
            for (file, dref) in files {
                let pos: &[u64] = positions.get(file).expect("validated delete positions");
                let Some(idx) = entry.files.iter().position(|f| f.path() == dref.file) else {
                    return Err(PublishFailure::Fatal(LakeError::Conflict {
                        path: self.storage.abs(CATALOG_FILE),
                        message: format!(
                            "table \"{key}\": the commit deletes from \"{}\", which is no \
                             longer part of the table at version {} (another writer replaced \
                             it); re-run the commit from a fresh base",
                            dref.file, self.version
                        ),
                    }));
                };
                let file_rows = entry.files[idx].rows().ok_or_else(|| {
                    PublishFailure::Fatal(LakeError::Corrupt {
                        path: self.storage.abs(CATALOG_FILE),
                        message: format!(
                            "table \"{key}\": \"{}\" has no row count; a positional delete \
                             cannot be applied",
                            dref.file
                        ),
                    })
                })?;
                let path = self.storage.abs(&dref.path);
                delete_file::validate_bounds(pos, file_rows, &dref.file, &path)
                    .map_err(PublishFailure::Fatal)?; // Positions already deleted by the head's own delete files
                                                      // would double-drop rows (and break the row-count invariant):
                                                      // refuse, never publish a manifest that lies about its rows.
                for existing in entry.deletes.iter().filter(|d| d.file == dref.file) {
                    let epath = self.storage.abs(&existing.path);
                    let bytes = self
                        .storage
                        .read(&existing.path, &format!("reading {}", epath.display()))
                        .map_err(PublishFailure::Fatal)?;
                    let epos =
                        delete_file::decode(&bytes, &epath).map_err(PublishFailure::Fatal)?;
                    let overlap: Vec<u64> = pos
                        .iter()
                        .copied()
                        .filter(|p| epos.binary_search(p).is_ok())
                        .collect();
                    if !overlap.is_empty() {
                        return Err(PublishFailure::Fatal(LakeError::Conflict {
                            path: self.storage.abs(CATALOG_FILE),
                            message: format!(
                                "table \"{key}\": {} positions of \"{}\" (first {}) were \
                                 already deleted by another commit; recompute the change \
                                 against the current version",
                                overlap.len(),
                                dref.file,
                                overlap[0]
                            ),
                        }));
                    }
                }
                entry.deletes.push(dref.clone());
                entry.rows = entry.rows.checked_sub(pos.len() as u64).ok_or_else(|| {
                    PublishFailure::Fatal(LakeError::Corrupt {
                        path: self.storage.abs(CATALOG_FILE),
                        message: format!(
                            "table \"{key}\": deleting {} rows underflows its {} rows",
                            pos.len(),
                            entry.rows
                        ),
                    })
                })?;
                rows_deleted += pos.len() as u64;
                let delta = deltas.entry(key.clone()).or_default();
                delta.delete_files.push(dref.clone());
                delta.rows_deleted += pos.len() as u64;
            }
        }
        // Partition-spec changes last (a spec set on a table this commit
        // creates was applied at creation): the table must exist, and the
        // change is recorded in the delta manifest for the audit trail.
        for (key, spec) in &commit.partition_changes {
            let entry = tables.get_mut(key).ok_or_else(|| {
                PublishFailure::Fatal(LakeError::Corrupt {
                    path: self.storage.abs(CATALOG_FILE),
                    message: format!(
                        "table \"{key}\" does not exist; a commit cannot set or clear its \
                         partition specification"
                    ),
                })
            })?;
            if entry.partition_by == *spec {
                continue;
            }
            entry.partition_by = spec.clone();
            let delta = deltas.entry(key.clone()).or_default();
            delta.partition_changed = true;
            delta.partition_by = spec.clone();
        }
        // The per-version record, mirroring a save: the author/message kept
        // pending by `Lake::set_commit_message` and the change summary diffed
        // against the base manifest (absent when it is unavailable).
        let (author, commit_message) = match &self.pending_commit {
            Some((author, message)) => (author.clone(), message.clone()),
            None => (None, None),
        };
        let changes = crate::manifest_for(&*self.storage, &self.root, Some(version - 1))
            .ok()
            .map(|(previous, _)| crate::snapshot::ChangeSummary::diff(&previous.tables, &tables));
        let schema_version = crate::next_schema_version(Some(&head), &tables);
        let new_catalog = crate::catalog::Catalog {
            format_version: crate::FORMAT_VERSION,
            updated: crate::time::iso8601_utc(std::time::SystemTime::now()),
            version,
            tables,
            author,
            commit_message,
            changes,
            schema_version,
        };
        let snapshot_bytes =
            crate::manifest_json(&new_catalog, &self.root).map_err(PublishFailure::Fatal)?;
        let commit_manifest = CommitManifest {
            format_version: crate::FORMAT_VERSION,
            version,
            base_version: version - 1,
            updated: crate::time::iso8601_utc(std::time::SystemTime::now()),
            tables: deltas,
        };
        let commit_bytes =
            crate::commit_manifest_json(&commit_manifest).map_err(PublishFailure::Fatal)?;
        let tables_changed: BTreeSet<&String> = commit
            .appends
            .keys()
            .chain(commit.deletes.keys())
            .chain(commit.partition_changes.keys())
            .collect();
        let report = CommitReport {
            version,
            base_version: origin,
            retries,
            committed: true,
            tables_changed: tables_changed.len(),
            tables_dropped: commit
                .drops
                .iter()
                .filter(|k| head.tables.contains_key(*k))
                .count(),
            tables_repartitioned: commit
                .partition_changes
                .iter()
                .filter(|(k, spec)| {
                    head.tables
                        .get(*k)
                        .is_none_or(|entry| entry.partition_by.as_ref() != spec.as_ref())
                })
                .count(),
            schema_changes: commit.schemas.len(),
            data_files: uploaded.appends.values().map(Vec::len).sum(),
            delete_files: uploaded.deletes.values().map(BTreeMap::len).sum(),
            bytes_written: uploaded.bytes
                + commit_bytes.len() as u64
                + 2 * snapshot_bytes.len() as u64,
            rows_added,
            rows_deleted,
        };
        self.publish_commit_version(version, &commit_bytes, &snapshot_bytes)
            .map_err(PublishFailure::Retry)?;
        // The commit record is published; a later save without a new
        // `set_commit_message` must not repeat it.
        self.pending_commit = None;
        Ok(report)
    }

    /// The publish step of one commit attempt: `commits/<v>.json` (self-healing
    /// a stray from a crashed attempt), then the snapshot link (create-only),
    /// then the conditional catalog replace.
    ///
    /// `snapshot_plain` is the new manifest; it is sealed **once** here and
    /// the exact same bytes go to `snapshots/<v>.json` and to `catalog.json`,
    /// so the byte-identity invariant survives encryption (see
    /// `Storage::seal_manifest`).
    pub(crate) fn publish_commit_version(
        &mut self,
        version: u64,
        commit_bytes: &[u8],
        snapshot_plain: &[u8],
    ) -> Result<()> {
        let commit_rel = format!("commits/{version}.json");
        let snapshot_rel = format!("snapshots/{version}.json");
        let snapshot_bytes = self.storage.seal_manifest(snapshot_plain)?;
        if !self.storage.put_new(&commit_rel, commit_bytes)? {
            // Version `version` already published (another writer won the
            // race), or a stray delta manifest from a crashed attempt that
            // never reached the snapshot: rebase only in the former case.
            if self.storage.exists(&snapshot_rel)? {
                return Err(LakeError::Conflict {
                    path: self.storage.abs(&snapshot_rel),
                    message: format!("another writer published version {version}; rebasing"),
                });
            }
            self.storage.put(&commit_rel, commit_bytes)?;
        }
        if !self.storage.put_new(&snapshot_rel, &snapshot_bytes)? {
            return Err(LakeError::Conflict {
                path: self.storage.abs(&snapshot_rel),
                message: format!("another writer published version {version}; rebasing"),
            });
        }
        self.replace_catalog(version, snapshot_plain, &snapshot_bytes, "commit")
    }

    /// Read `catalog.json` for a commit attempt: whatever version it names
    /// becomes this handle's base (a rebase).
    pub(crate) fn read_catalog_for_commit(&mut self) -> Result<crate::catalog::Catalog> {
        let manifest = crate::read_catalog(&*self.storage, &self.root)?;
        self.version = manifest.version;
        self.format_version = manifest.format_version;
        Ok(manifest)
    }

    /// Re-read the catalog after a conflict: the new head becomes the base.
    pub(crate) fn rebase_catalog(&mut self) -> Result<()> {
        let manifest = crate::read_catalog(&*self.storage, &self.root)?;
        self.version = manifest.version;
        self.format_version = manifest.format_version;
        Ok(())
    }
}

/// The files one commit uploads before publishing.
#[derive(Debug, Default)]
struct UploadedCommit {
    /// `db.table` → the appended data files, one per partition value set
    /// (exactly one for an unpartitioned table).
    appends: BTreeMap<String, Vec<FileRef>>,
    /// `db.table` → data file → the delete file to add.
    deletes: BTreeMap<String, BTreeMap<String, DeleteRef>>,
    bytes: u64,
}

/// Why one publish attempt failed: `Retry` rebases (a conflicting writer);
/// `Fatal` is a wrong delta or unusable base and is never retried.
enum PublishFailure {
    Retry(LakeError),
    Fatal(LakeError),
}

/// Wrap an exhausted retry budget's last conflict with the attempt count.
fn retry_exhausted(e: LakeError, retries: u32) -> LakeError {
    LakeError::Conflict {
        path: match &e {
            LakeError::Conflict { path, .. } => path.clone(),
            _ => std::path::PathBuf::from("catalog.json"),
        },
        message: format!(
            "{} (after {retries} retr{}; another writer keeps publishing — raise \
             max_retry_count or retry later)",
            conflict_message(&e),
            if retries == 1 { "y" } else { "ies" },
        ),
    }
}
fn conflict_message(e: &LakeError) -> String {
    match e {
        LakeError::Conflict { message, .. } => message.clone(),
        other => other.to_string(),
    }
}
