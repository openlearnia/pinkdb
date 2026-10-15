//! Attached read-only lakes (`.lake attach`).
//!
//! An attached lake is a *session-local*, read-only extension of the
//! catalog the binder resolves queries against: the engine answers exact
//! `COUNT`/`MIN`/`MAX` aggregates from the lake's stored statistics when the
//! lake layer can prove the answer, and otherwise materializes the one table
//! a query touches through the same data-file decode `.lake open` uses.
//!
//! Nothing here touches the store's tables, the WAL, or any save/persist
//! path: an attached table is not part of `.save`, `.lake save`, or a durable
//! checkpoint, and every mutation statement (`INSERT`/`UPDATE`/`DELETE`,
//! `DROP TABLE`, `CREATE TABLE` over the name) is refused with a clear
//! read-only error. `.lake open` replaces the whole store and therefore drops
//! every attachment with it; `.lake detach` drops one explicitly.
//!
//! The lake side implements [`AttachedLake`]: [`AttachedLake::tables`]
//! returns the descriptors `.lake attach` read from the data files' footers
//! (no column data touched), and [`AttachedLake::load`] decodes one table on
//! first use and caches it for the rest of the session.

use std::fmt;
use std::sync::Arc;

use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector};

/// The exact statistics of one attached-table column, as far as the lake
/// layer could prove them.
///
/// Every field is `None` when the lake cannot answer *exactly* from stored
/// statistics — a truncated or inexact statistic, a NaN count it cannot
/// prove, an absent statistic. The executor falls back to materializing the
/// table for anything `None`; it never approximates.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// The exact number of NULLs in the column, when provable (a
    /// non-nullable column is always `Some(0)`). `COUNT(col)` is
    /// `rows - null_count`; unknown means the normal scan path.
    pub null_count: Option<u64>,
    /// The exact value `MIN(col)` must return, when provable. `None` also
    /// covers the cases where the answer is `NULL` (no rows, all rows NULL),
    /// which the executor derives from `rows`/`null_count` without touching
    /// the column.
    pub min: Option<Value>,
    /// The exact value `MAX(col)` must return, when provable.
    pub max: Option<Value>,
}

impl ColumnStats {
    /// No usable statistics: every fast path except `COUNT(*)` falls back.
    pub fn unknown() -> Self {
        Self {
            null_count: None,
            min: None,
            max: None,
        }
    }
}

/// One table an attached lake offers, as read from the data file's footer
/// (row count + schema + per-column statistics; no column data decoded).
#[derive(Debug, Clone, PartialEq)]
pub struct AttachedTable {
    /// Database name as the lake's catalog wrote it (display casing; lookups
    /// are case-insensitive like every catalog name).
    pub db: String,
    /// Table name as the lake's catalog wrote it.
    pub table: String,
    /// Exact row count of the table's single data file, cross-checked
    /// against the manifest at attach time.
    pub rows: u64,
    /// Column names and types, in scan order (the file's own `pinkdb.table`
    /// schema, validated against the Vortex dtypes at attach).
    pub schema: Vec<(String, DataType)>,
    /// Per-column statistics in `schema` order.
    pub stats: Vec<ColumnStats>,
}

/// One projected read of an attached table: a full-schema, chunk-aligned
/// table in which `present[i]` columns hold decoded values and the rest hold
/// NULL placeholders with the same chunk lengths (so row counts and chunk
/// boundaries are those of a full decode).
///
/// The engine converts this to scan chunks in which absent columns are
/// zero-length placeholders; every column reader goes through
/// `DataChunk::expect_column`, so a column the projection analysis missed
/// fails loudly instead of reading NULLs. A later query that references an
/// absent column materializes exactly that column (the lake caches decoded
/// columns per table), so nothing here is ever a durable wrong value.
#[derive(Debug, Clone)]
pub struct AttachedScan {
    /// The table, full schema, chunk-aligned: decoded columns and NULL
    /// placeholders share the table's chunk lengths.
    pub table: Arc<DataTable>,
    /// One flag per table column: true = decoded.
    pub present: Vec<bool>,
}

impl AttachedScan {
    /// A scan where every column is decoded (the pre-projection behavior).
    pub fn complete(table: Arc<DataTable>) -> Self {
        let present = vec![true; table.num_cols()];
        Self { table, present }
    }

    /// The scan as engine chunks, in table order: present columns as stored,
    /// absent columns as a shared zero-length placeholder of their dtype.
    pub fn chunks(&self) -> Vec<DataChunk> {
        let num_cols = self.table.columns.len();
        let rows: Vec<usize> = self
            .table
            .columns
            .first()
            .map(|c| c.chunks.iter().map(|c| c.len()).collect())
            .unwrap_or_default();
        // One shared placeholder per absent column; nothing reads it.
        let absent: Vec<Option<Arc<Vector>>> = (0..num_cols)
            .map(|i| {
                let dtype = self.table.columns[i].dtype;
                (!self.present.get(i).copied().unwrap_or(false))
                    .then(|| Arc::new(Vector::new_empty(dtype)))
            })
            .collect();
        rows.into_iter()
            .enumerate()
            .map(|(j, n)| {
                let cols: Vec<Arc<Vector>> = (0..num_cols)
                    .map(|i| match &absent[i] {
                        Some(placeholder) => Arc::clone(placeholder),
                        None => self.table.columns[i].chunks[j].clone(),
                    })
                    .collect();
                DataChunk::projected(cols, n)
            })
            .collect()
    }
}

/// One attached lake, as the engine sees it. Implemented by `pink-lake`
/// (the dependency direction stays engine ← lake).
pub trait AttachedLake: fmt::Debug + Send + Sync {
    /// The lake location as `.lake attach` was given it (for display and
    /// `.lake detach`).
    fn location(&self) -> &str;

    /// The tables this lake offers, pinned to the version attached.
    fn tables(&self) -> &[AttachedTable];

    /// Materialize one table: exactly the decode `.lake open` performs,
    /// cached by the implementation (a second call is cheap and returns the
    /// same data). `db`/`table` are matched case-insensitively.
    fn load(&self, db: &str, table: &str) -> Result<Arc<DataTable>>;

    /// Materialize only `columns` (table column indices; `None` = every
    /// column) of one table, reusing everything already decoded and caching
    /// the result per table — a later call naming other columns decodes just
    /// those. `columns` need not be sorted or deduplicated; an out-of-range
    /// index falls back to every column.
    ///
    /// The default implementation ignores the projection and materializes in
    /// full (always correct; the engine reads every column).
    fn load_projected(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
    ) -> Result<AttachedScan> {
        let _ = columns;
        Ok(AttachedScan::complete(self.load(db, table)?))
    }

    /// Data-file decode counters `(decodes, columns decoded)` for
    /// diagnostics: `COUNT(*)`/`MIN`/`MAX` fast paths leave both at zero,
    /// and a projected materialization counts only the columns it decoded.
    fn decode_stats(&self) -> (u64, u64) {
        (0, 0)
    }
}

impl PinkDb {
    /// Attach one read-only lake to this session, at the version its handle
    /// pinned. Refuses (before changing anything) when a table of the lake
    /// collides with a store table or a table another attached lake already
    /// offers, and creates the (empty) databases the lake's tables live in
    /// so qualified names and `USE` resolve.
    ///
    /// Returns the attached descriptors. Session-local: a transaction
    /// snapshot taken earlier keeps its own attachments, and `.lake open`
    /// (which replaces the store) drops them all.
    pub fn attach_lake(&mut self, lake: Arc<dyn AttachedLake>) -> Result<Vec<AttachedTable>> {
        let location = lake.location().to_string();
        if self.attached_lakes.iter().any(|l| l.location() == location) {
            return Err(PinkDbError::Catalog(format!(
                "lake {location} is already attached to this session; .lake detach it first"
            )));
        }
        let tables: Vec<AttachedTable> = lake.tables().to_vec();
        // Validate every collision before changing anything: an attach that
        // fails leaves the session exactly as it was.
        let mut seen = std::collections::HashSet::new();
        for t in &tables {
            if !seen.insert((t.db.to_ascii_lowercase(), t.table.to_ascii_lowercase())) {
                return Err(PinkDbError::Catalog(format!(
                    "lake {location} offers table \"{}.{}\" twice; refusing to attach",
                    t.db, t.table
                )));
            }
            if self.table_in(&t.db, &t.table).is_ok() {
                return Err(PinkDbError::Catalog(format!(
                    "cannot attach lake {location}: table \"{}.{}\" already exists in the \
                     session store; drop it or attach a different lake",
                    t.db, t.table
                )));
            }
            if let Some(other) = self.attached_location_in(&t.db, &t.table) {
                return Err(PinkDbError::Catalog(format!(
                    "cannot attach lake {location}: table \"{}.{}\" is already attached from \
                     lake {other}; .lake detach it first",
                    t.db, t.table
                )));
            }
        }
        for t in &tables {
            if !self.database_exists(&t.db) {
                self.create_database(&t.db)?;
            }
        }
        self.attached_lakes.push(lake);
        Ok(tables)
    }

    /// Detach the lake attached at exactly `location` (the string attach
    /// printed). True when a lake was detached.
    pub fn detach_lake(&mut self, location: &str) -> bool {
        let before = self.attached_lakes.len();
        self.attached_lakes.retain(|l| l.location() != location);
        self.attached_lakes.len() != before
    }

    /// The attached lakes and their tables, in attach order.
    pub fn attached_lakes(&self) -> Vec<(&str, Vec<AttachedTable>)> {
        self.attached_lakes
            .iter()
            .map(|l| (l.location(), l.tables().to_vec()))
            .collect()
    }

    /// Every attached table, in attach order.
    pub fn attached_tables(&self) -> Vec<AttachedTable> {
        self.attached_lakes
            .iter()
            .flat_map(|l| l.tables().iter().cloned())
            .collect()
    }

    /// Per attached lake, the location and its data-file decode counters
    /// `(decodes, columns decoded)` so far (see [`AttachedLake::decode_stats`]).
    pub fn attached_decode_stats(&self) -> Vec<(String, u64, u64)> {
        self.attached_lakes
            .iter()
            .map(|l| {
                let (loads, columns) = l.decode_stats();
                (l.location().to_string(), loads, columns)
            })
            .collect()
    }

    /// The location of the attached lake offering `db.table`, if any.
    pub fn attached_location_in(&self, db: &str, name: &str) -> Option<String> {
        self.attached_lookup(db, name)
            .map(|(_, lake)| lake.location().to_string())
    }

    /// One attached table descriptor, if `db.table` is attached.
    pub fn attached_table_in(&self, db: &str, name: &str) -> Option<AttachedTable> {
        self.attached_lookup(db, name).map(|(t, _)| t.clone())
    }

    /// The attached table and its lake, if `db.table` is attached.
    pub(crate) fn attached_lookup(
        &self,
        db: &str,
        name: &str,
    ) -> Option<(&AttachedTable, &Arc<dyn AttachedLake>)> {
        for lake in &self.attached_lakes {
            if let Some(t) = lake
                .tables()
                .iter()
                .find(|t| t.db.eq_ignore_ascii_case(db) && t.table.eq_ignore_ascii_case(name))
            {
                return Some((t, lake));
            }
        }
        None
    }

    /// The schema of a table visible to queries: a store table first, else an
    /// attached lake table. Anything else keeps the store's "not found"
    /// error unchanged.
    pub fn visible_table_schema(&self, db: &str, name: &str) -> Result<Vec<(String, DataType)>> {
        match self.table_in(db, name) {
            Ok(t) => Ok(t.schema()),
            Err(e) => match self.attached_lookup(db, name) {
                Some((t, _)) => Ok(t.schema.clone()),
                None => Err(e),
            },
        }
    }

    /// The read-only refusal for a mutation targeting an attached table,
    /// if it is one.
    pub(crate) fn attached_read_only(&self, db: &str, name: &str) -> Option<PinkDbError> {
        self.attached_location_in(db, name).map(|location| {
            PinkDbError::Execution(format!(
                "table \"{db}.{name}\" is attached read-only from lake {location} \
                     (attached lake tables are read-only); use .lake open to load a \
                     writable copy"
            ))
        })
    }

    /// Refuse a statement that is about to be WAL-logged and reads an
    /// attached lake table.
    ///
    /// Attached tables are session-local: they are not in the store, not in a
    /// checkpoint, and `.lake attach` is not a SQL statement, so the logged
    /// SQL text of a mutating statement that references one could never
    /// replay. Read-only statements are never logged and stay unaffected, and
    /// an in-memory store has no WAL to replay.
    pub(crate) fn refuse_logged_attached_reads(
        &self,
        plan: &crate::plan::LogicalPlan,
    ) -> Result<()> {
        if !self.is_durable() || self.attached_lakes.is_empty() {
            return Ok(());
        }
        let mut reads = std::collections::HashSet::new();
        crate::plan::collect_tables_read(plan, self.current_db(), &mut reads);
        for (db, table) in reads {
            if let Some(location) = self.attached_location_in(&db, &table) {
                return Err(PinkDbError::Execution(format!(
                    "this statement is durable and reads attached lake table \"{db}.{table}\" \
                     ({location}); attached tables are session-local and the statement could \
                     not replay after a reopen — run the write without the lake, or with \
                     .lake open"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PinkDbError;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    /// A fake attached lake over in-memory data: descriptors are given, loads
    /// are counted, and the loaded table is built from the descriptor's
    /// schema by appending the given rows.
    #[derive(Debug)]
    struct FakeLake {
        location: String,
        tables: Vec<AttachedTable>,
        rows: Vec<Vec<Value>>,
        loads: AtomicU64,
        loaded: Mutex<Option<Arc<DataTable>>>,
    }

    impl FakeLake {
        fn new(
            location: &str,
            db: &str,
            table: &str,
            schema: Vec<(String, DataType)>,
            rows: Vec<Vec<Value>>,
        ) -> Self {
            let stats = stats_for(&schema, &rows);
            Self::with_stats(location, db, table, schema, rows, stats)
        }

        /// A lake that carries no min/max statistics but knows each column's
        /// NULL count (a non-nullable column is always exact).
        fn without_stats(
            location: &str,
            db: &str,
            table: &str,
            schema: Vec<(String, DataType)>,
            rows: Vec<Vec<Value>>,
        ) -> Self {
            let stats = schema
                .iter()
                .map(|_| ColumnStats {
                    null_count: Some(0),
                    min: None,
                    max: None,
                })
                .collect();
            Self::with_stats(location, db, table, schema, rows, stats)
        }

        fn with_stats(
            location: &str,
            db: &str,
            table: &str,
            schema: Vec<(String, DataType)>,
            rows: Vec<Vec<Value>>,
            stats: Vec<ColumnStats>,
        ) -> Self {
            Self {
                location: location.to_string(),
                tables: vec![AttachedTable {
                    db: db.to_string(),
                    table: table.to_string(),
                    rows: rows.len() as u64,
                    schema,
                    stats,
                }],
                rows,
                loads: AtomicU64::new(0),
                loaded: Mutex::new(None),
            }
        }
    }

    /// The exact statistics a lake would read from a data file holding
    /// `rows`, with the same exactness gates `pink-lake` applies: a DOUBLE
    /// min/max with any NaN, or one that is zero (`-0.0 == 0.0`), is not
    /// servable.
    fn stats_for(schema: &[(String, DataType)], rows: &[Vec<Value>]) -> Vec<ColumnStats> {
        schema
            .iter()
            .enumerate()
            .map(|(i, (_, dtype))| {
                let mut nulls = 0u64;
                let mut min: Option<Value> = None;
                let mut max: Option<Value> = None;
                let mut any_nan = false;
                for row in rows {
                    let v = &row[i];
                    if v.is_null() {
                        nulls += 1;
                        continue;
                    }
                    if matches!(v, Value::Double(x) if x.is_nan()) {
                        any_nan = true;
                    }
                    if min
                        .as_ref()
                        .is_none_or(|m| crate::plan::compare_values(v, m).is_lt())
                    {
                        min = Some(v.clone());
                    }
                    if max
                        .as_ref()
                        .is_none_or(|m| crate::plan::compare_values(v, m).is_gt())
                    {
                        max = Some(v.clone());
                    }
                }
                let gate = |v: Option<Value>| match v {
                    Some(Value::Double(x)) if any_nan || x == 0.0 || x.is_nan() => None,
                    other => other,
                };
                let (min, max) = if *dtype == DataType::Double {
                    (gate(min), gate(max))
                } else {
                    (min, max)
                };
                ColumnStats {
                    null_count: Some(nulls),
                    min,
                    max,
                }
            })
            .collect()
    }

    impl AttachedLake for FakeLake {
        fn location(&self) -> &str {
            &self.location
        }
        fn tables(&self) -> &[AttachedTable] {
            &self.tables
        }
        fn load(&self, db: &str, table: &str) -> Result<Arc<DataTable>> {
            let t = self
                .tables
                .iter()
                .find(|t| t.db.eq_ignore_ascii_case(db) && t.table.eq_ignore_ascii_case(table))
                .ok_or_else(|| PinkDbError::Catalog(format!("no table {db}.{table}")))?;
            let mut loaded = self.loaded.lock().unwrap();
            if let Some(t) = &*loaded {
                return Ok(Arc::clone(t));
            }
            self.loads.fetch_add(1, Ordering::Relaxed);
            let mut table_data = DataTable::new(t.table.clone(), t.schema.clone())?;
            for row in &self.rows {
                table_data.append_row(row.clone())?;
            }
            let arc = Arc::new(table_data);
            *loaded = Some(Arc::clone(&arc));
            Ok(arc)
        }
    }

    fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        let mut results = db.execute(sql).unwrap();
        match results.pop().unwrap() {
            crate::QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Row-for-row equality with doubles compared bit-exactly (NaN payloads
    /// and `-0.0` included).
    fn assert_rows_same(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
        assert_eq!(a.len(), b.len(), "{what}: row count");
        for (ra, rb) in a.iter().zip(b) {
            assert_eq!(ra.len(), rb.len(), "{what}: column count");
            for (va, vb) in ra.iter().zip(rb) {
                match (va, vb) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "{what}: {va:?} vs {vb:?}")
                    }
                    _ => assert_eq!(va, vb, "{what}"),
                }
            }
        }
    }

    fn lake() -> Arc<FakeLake> {
        Arc::new(FakeLake::new(
            "/tmp/lake",
            "main",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
                ("c".to_string(), DataType::Double),
            ],
            vec![
                vec![
                    Value::Integer(1),
                    Value::Varchar("x".to_string()),
                    Value::Double(1.5),
                ],
                vec![Value::Integer(2), Value::Null, Value::Double(-2.0)],
            ],
        ))
    }

    /// A fake attached lake that *records* every projection it is asked for
    /// and materializes exactly those columns (like `pink-lake`): requested
    /// columns hold values, the rest are NULL placeholders of the same chunk
    /// lengths. The engine converts placeholders to absent columns and fails
    /// loudly on a read of one, so a query the analysis under-provisioned
    /// cannot pass silently; the recorded requests prove what it asked for.
    #[derive(Debug)]
    struct RecordingLake {
        location: String,
        tables: Vec<AttachedTable>,
        rows: Vec<Vec<Value>>,
        /// One entry per `load_projected` call: the analysis's bound (`None`
        /// = unbounded, every column).
        requests: Mutex<Vec<Option<Vec<usize>>>>,
    }

    impl RecordingLake {
        fn new(location: &str, schema: Vec<(String, DataType)>, rows: Vec<Vec<Value>>) -> Self {
            let stats = stats_for(&schema, &rows);
            Self {
                location: location.to_string(),
                tables: vec![AttachedTable {
                    db: "main".to_string(),
                    table: "t".to_string(),
                    rows: rows.len() as u64,
                    schema,
                    stats,
                }],
                rows,
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<Option<Vec<usize>>> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl AttachedLake for RecordingLake {
        fn location(&self) -> &str {
            &self.location
        }
        fn tables(&self) -> &[AttachedTable] {
            &self.tables
        }
        fn load(&self, db: &str, table: &str) -> Result<Arc<DataTable>> {
            self.load_projected(db, table, None).map(|scan| scan.table)
        }
        fn load_projected(
            &self,
            db: &str,
            table: &str,
            columns: Option<&[usize]>,
        ) -> Result<AttachedScan> {
            let t = self
                .tables
                .iter()
                .find(|t| t.db.eq_ignore_ascii_case(db) && t.table.eq_ignore_ascii_case(table))
                .ok_or_else(|| PinkDbError::Catalog(format!("no table {db}.{table}")))?;
            self.requests
                .lock()
                .unwrap()
                .push(columns.map(|c| c.to_vec()));
            // The full table (this fake's "decode").
            let mut full = DataTable::new(t.table.clone(), t.schema.clone())?;
            for row in &self.rows {
                full.append_row(row.clone())?;
            }
            let present = match columns {
                Some(cols) if cols.iter().all(|&i| i < full.num_cols()) => {
                    let mut p = vec![false; full.num_cols()];
                    for &i in cols {
                        p[i] = true;
                    }
                    // An empty projection still decodes column 0 (the shape).
                    if p.iter().all(|&b| !b) {
                        p[0] = true;
                    }
                    p
                }
                _ => vec![true; full.num_cols()],
            };
            // NULL placeholders for the columns not asked for, exactly like
            // `pink-lake`'s projected decode.
            let mut out = Vec::with_capacity(full.num_cols());
            for (i, col) in full.columns.iter().enumerate() {
                if present[i] {
                    out.push(col.clone());
                } else {
                    let chunks = col
                        .chunks
                        .iter()
                        .map(|c| {
                            let mut v = Vector::new_empty(col.dtype);
                            for _ in 0..c.len() {
                                v.push_null().expect("push_null");
                            }
                            Arc::new(v)
                        })
                        .collect();
                    out.push(crate::storage::Column {
                        name: col.name.clone(),
                        dtype: col.dtype,
                        chunks,
                    });
                }
            }
            let table = DataTable::from_columns(full.name.clone(), out, full.num_rows);
            Ok(AttachedScan {
                table: Arc::new(table),
                present,
            })
        }
    }

    /// A 5-column, 12-row corpus for the projection-analysis tests (every
    /// column type, NULLs included).
    fn analysis_corpus() -> (Vec<(String, DataType)>, Vec<Vec<Value>>) {
        let schema = vec![
            ("i".to_string(), DataType::Integer),
            ("g".to_string(), DataType::Bigint),
            ("d".to_string(), DataType::Double),
            ("s".to_string(), DataType::Varchar),
            ("b".to_string(), DataType::Boolean),
        ];
        let rows = (0..12i64)
            .map(|r| {
                vec![
                    if r % 5 == 0 {
                        Value::Null
                    } else {
                        Value::Integer(r as i32 - 6)
                    },
                    Value::Bigint(r * 1000 - 500),
                    Value::Double(r as f64 * 1.25 - 3.0),
                    if r % 4 == 0 {
                        Value::Null
                    } else {
                        Value::Varchar(format!("v{}", r % 3))
                    },
                    Value::Boolean(r % 2 == 0),
                ]
            })
            .collect();
        (schema, rows)
    }

    /// The referenced-columns analysis, query by query: exactly the columns
    /// the executor reads, `[]` when the scan only provides row shape,
    /// `None` for the unbounded shapes (joins), and no entry at all for the
    /// statistics fast paths. Every query also runs against the same rows in
    /// a plain store and must produce identical answers.
    #[test]
    fn scan_analysis_requests_exactly_the_referenced_columns() {
        // (SQL, one request per attached-lake load; `None` = every column).
        let cases: Vec<(&str, Vec<Option<Vec<usize>>>)> = vec![
            // Statistics fast paths: no data-file access at all.
            ("SELECT COUNT(*) FROM t;", vec![]),
            ("SELECT MIN(i), MAX(i), MAX(g) FROM t;", vec![]),
            ("SELECT COUNT(s), COUNT(*) FROM t;", vec![]),
            // One referenced column.
            ("SELECT SUM(d) FROM t;", vec![Some(vec![2])]),
            ("SELECT COUNT(*) FROM t WHERE d > 0;", vec![Some(vec![2])]),
            ("SELECT b, COUNT(*) FROM t GROUP BY b;", vec![Some(vec![4])]),
            ("SELECT COUNT(*) FROM t GROUP BY i;", vec![Some(vec![0])]),
            // Several: sort keys, projections, inner select lists.
            ("SELECT i FROM t ORDER BY s;", vec![Some(vec![0, 3])]),
            (
                "SELECT COUNT(*) FROM (SELECT d FROM t) x;",
                vec![Some(vec![2])],
            ),
            (
                "SELECT MAX(i) FROM (SELECT i, s FROM t) x;",
                vec![Some(vec![0, 3])],
            ),
            (
                "SELECT i, ROW_NUMBER() OVER (PARTITION BY b ORDER BY g) FROM t;",
                vec![Some(vec![0, 1, 4])],
            ),
            (
                "WITH x AS (SELECT i, s FROM t) SELECT MAX(i) FROM x;",
                vec![Some(vec![0, 3])],
            ),
            ("SELECT DISTINCT i, s FROM t;", vec![Some(vec![0, 3])]),
            // Nothing referenced: the scan still provides the row shape.
            ("SELECT 1 FROM t LIMIT 1;", vec![Some(vec![])]),
            // Subqueries analyze their own plans; the outer scan is bounded
            // separately.
            (
                "SELECT (SELECT SUM(d) FROM t) AS m FROM t;",
                vec![Some(vec![2]), Some(vec![])],
            ),
            (
                "SELECT (SELECT SUM(i) FROM t) AS a, (SELECT AVG(d) FROM t) AS b;",
                vec![Some(vec![0]), Some(vec![2])],
            ),
            // Unbounded shapes: a join assembles every column of both sides.
            (
                "SELECT COUNT(*) FROM t AS a JOIN t AS b ON a.i = b.i;",
                vec![None, None],
            ),
            ("SELECT * FROM t;", vec![Some(vec![0, 1, 2, 3, 4])]),
        ];

        for (sql, expected) in cases {
            let (schema, rows) = analysis_corpus();
            let fake = Arc::new(RecordingLake::new(
                "/tmp/lake-analysis",
                schema.clone(),
                rows.clone(),
            ));
            let mut attached = PinkDb::new();
            attached.attach_lake(fake.clone()).unwrap();
            let mut plain = PinkDb::new();
            let cols: Vec<String> = schema.iter().map(|(n, t)| format!("{n} {t}")).collect();
            plain
                .execute(&format!("CREATE TABLE t ({});", cols.join(", ")))
                .unwrap();
            for row in &rows {
                plain
                    .table_mut("t")
                    .unwrap()
                    .append_row(row.clone())
                    .unwrap();
            }

            assert_rows_same(&query(&mut attached, sql), &query(&mut plain, sql), sql);
            let mut got = fake.requests();
            got.sort();
            let mut want = expected;
            want.sort();
            assert_eq!(got, want, "{sql}");
        }
    }

    #[test]
    fn attach_lake_resolves_and_materializes() {
        let mut db = PinkDb::new();
        let fake = lake();
        let tables = db.attach_lake(fake.clone()).unwrap();
        assert_eq!(tables.len(), 1);
        // The binder sees the schema; the scan materializes on first use.
        let rows = query(&mut db, "SELECT a, b, c FROM t ORDER BY a;");
        assert_eq!(rows.len(), 2);
        assert_eq!(fake.loads.load(Ordering::Relaxed), 1);
        // A second query reuses the materialized table.
        let rows = query(&mut db, "SELECT COUNT(*) FROM t;");
        assert_eq!(rows, vec![vec![Value::Bigint(2)]]);
        assert_eq!(fake.loads.load(Ordering::Relaxed), 1);
        // The store itself has no such table: attached data is not persisted.
        assert!(db.table("t").is_err());
        assert_eq!(
            db.attached_location_in("main", "T").as_deref(),
            Some("/tmp/lake")
        );
        assert!(db.detach_lake("/tmp/lake"));
        assert!(db.execute("SELECT * FROM t;").is_err());
    }

    #[test]
    fn mutations_against_attached_tables_are_refused() {
        let mut db = PinkDb::new();
        db.attach_lake(lake()).unwrap();
        db.execute("CREATE TABLE other (x INTEGER);").unwrap();
        for (sql, needle) in [
            ("INSERT INTO t VALUES (1, 'a', 1.0);", "read-only"),
            ("UPDATE t SET a = 1;", "read-only"),
            ("DELETE FROM t;", "read-only"),
        ] {
            let err = db.execute(sql).unwrap_err().to_string();
            assert!(err.contains(needle), "{sql}: {err}");
        }
        // DROP TABLE of an attached name is refused with the same class of
        // error (the store never had the table).
        let err = db.execute("DROP TABLE t;").unwrap_err().to_string();
        assert!(err.contains("attached"), "DROP TABLE t: {err}");
        // CREATE TABLE over the name must not shadow the attachment.
        let err = db
            .execute("CREATE TABLE t (a INTEGER, b VARCHAR, c DOUBLE);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("attached"), "CREATE TABLE t: {err}");
    }

    #[test]
    fn collisions_and_duplicates_are_refused_atomically() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let err = db.attach_lake(lake()).unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
        // The failed attach changed nothing (no database was created, the
        // table is still the store's).
        assert_eq!(
            query(&mut db, "SELECT COUNT(*) FROM t;")[0][0],
            Value::Bigint(0)
        );
        // Two lakes offering the same name: the second is refused.
        db.execute("DROP TABLE t;").unwrap();
        db.attach_lake(lake()).unwrap();
        let second = Arc::new(FakeLake::new(
            "/tmp/lake2",
            "main",
            "t",
            vec![("a".to_string(), DataType::Integer)],
            vec![],
        ));
        let err = db.attach_lake(second).unwrap_err().to_string();
        assert!(err.contains("already attached"), "{err}");
        // Attaching the same lake twice is refused.
        let err = db.attach_lake(lake()).unwrap_err().to_string();
        assert!(err.contains("already attached"), "{err}");
    }

    #[test]
    fn attached_tables_are_not_persisted_or_checkpointed() {
        let mut db = PinkDb::new();
        db.attach_lake(lake()).unwrap();
        // `.save` (persist) writes only store tables: the attached table is
        // simply absent, never an empty table.
        let dir = std::env::temp_dir().join(format!("pinkdb_attach_save_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.pink");
        db.save(path.to_str().unwrap()).unwrap();
        let mut back = PinkDb::open(path.to_str().unwrap()).unwrap();
        assert!(back.table("t").is_err());
        assert!(back.execute("SELECT COUNT(*) FROM t;").is_err());
        assert!(back.attached_tables().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Every query shape that is provably answerable from the descriptor is
    /// answered without decoding; each probe gets a fresh store so a fallback
    /// cannot be masked by an earlier materialization.
    #[test]
    fn stats_fast_path_answers_without_decoding() {
        let answers = [
            ("SELECT COUNT(*) FROM t;", vec![vec![Value::Bigint(2)]]),
            ("SELECT COUNT(b) FROM t;", vec![vec![Value::Bigint(1)]]),
            (
                "SELECT MIN(a), MAX(a) FROM t;",
                vec![vec![Value::Integer(1), Value::Integer(2)]],
            ),
            (
                "SELECT MIN(c), MAX(c) FROM t;",
                vec![vec![Value::Double(-2.0), Value::Double(1.5)]],
            ),
            (
                "SELECT COUNT(*), COUNT(c), MIN(a), MAX(b) FROM t;",
                vec![vec![
                    Value::Bigint(2),
                    Value::Bigint(2),
                    Value::Integer(1),
                    Value::Varchar("x".to_string()),
                ]],
            ),
        ];
        for (sql, expected) in answers {
            let fake = lake();
            let mut db = PinkDb::new();
            db.attach_lake(fake.clone()).unwrap();
            assert_eq!(query(&mut db, sql), expected, "{sql}");
            assert_eq!(
                fake.loads.load(Ordering::Relaxed),
                0,
                "{sql} must be answered from statistics"
            );
        }
    }

    /// Anything outside the provable shapes falls back: the same answers, but
    /// the table is materialized (and reused).
    #[test]
    fn unsupported_shapes_fall_back_and_stay_correct() {
        for sql in [
            "SELECT SUM(a) FROM t;",
            "SELECT AVG(a) FROM t;",
            "SELECT MAX(a) FROM (SELECT a FROM t) s;",
            "SELECT COUNT(*) FROM t WHERE a > 1;",
            "SELECT a, COUNT(*) FROM t GROUP BY a;",
            "SELECT MIN(a + 1) FROM t;",
            "SELECT MIN(ABS(a)) FROM t;",
            "SELECT COUNT(*) FROM t JOIN t AS u ON t.a = u.a;",
            "SELECT a FROM t ORDER BY a LIMIT 1;",
        ] {
            let fake = lake();
            let mut db = PinkDb::new();
            db.attach_lake(fake.clone()).unwrap();
            // All of these are valid over the fixture; the point is that each
            // one materializes rather than claiming a statistics answer.
            let _ = query(&mut db, sql);
            assert_eq!(
                fake.loads.load(Ordering::Relaxed),
                1,
                "{sql} must materialize the table"
            );
        }
    }

    /// A lake with no statistics still answers COUNT(*) and COUNT over
    /// non-nullable columns (the descriptor carries an exact 0), and falls
    /// back for MIN/MAX.
    #[test]
    fn no_statistics_still_answers_counts() {
        let fake = Arc::new(FakeLake::without_stats(
            "/tmp/lake-nostats",
            "main",
            "t",
            vec![("a".to_string(), DataType::Integer)],
            vec![vec![Value::Integer(7)], vec![Value::Integer(8)]],
        ));
        let mut db = PinkDb::new();
        db.attach_lake(fake.clone()).unwrap();
        assert_eq!(
            query(&mut db, "SELECT COUNT(*) FROM t;"),
            vec![vec![Value::Bigint(2)]]
        );
        assert_eq!(
            query(&mut db, "SELECT COUNT(a) FROM t;"),
            vec![vec![Value::Bigint(2)]]
        );
        assert_eq!(fake.loads.load(Ordering::Relaxed), 0);
        // MIN/MAX cannot be proven, so the table materializes and the answer
        // comes from the data.
        assert_eq!(
            query(&mut db, "SELECT MIN(a), MAX(a) FROM t;"),
            vec![vec![Value::Integer(7), Value::Integer(8)]]
        );
        assert_eq!(fake.loads.load(Ordering::Relaxed), 1);
    }

    /// An empty table answers counts and NULL min/max without any statistics
    /// and without decoding.
    #[test]
    fn empty_attached_table_answers_nulls_and_zero() {
        let fake = Arc::new(FakeLake::new(
            "/tmp/lake-empty",
            "main",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
            vec![],
        ));
        let mut db = PinkDb::new();
        db.attach_lake(fake.clone()).unwrap();
        assert_eq!(
            query(
                &mut db,
                "SELECT COUNT(*), COUNT(a), COUNT(b), MIN(a), MAX(b) FROM t;"
            ),
            vec![vec![
                Value::Bigint(0),
                Value::Bigint(0),
                Value::Bigint(0),
                Value::Null,
                Value::Null
            ]]
        );
        assert_eq!(fake.loads.load(Ordering::Relaxed), 0);
    }

    /// An all-NULL column answers COUNT(col) = 0 and MIN/MAX = NULL from the
    /// exact NULL count, without decoding.
    #[test]
    fn all_null_column_answers_from_the_null_count() {
        let fake = Arc::new(FakeLake::new(
            "/tmp/lake-allnull",
            "main",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Bigint),
            ],
            vec![
                vec![Value::Integer(1), Value::Null],
                vec![Value::Integer(2), Value::Null],
            ],
        ));
        let mut db = PinkDb::new();
        db.attach_lake(fake.clone()).unwrap();
        assert_eq!(
            query(&mut db, "SELECT COUNT(b), MIN(b), MAX(b) FROM t;"),
            vec![vec![Value::Bigint(0), Value::Null, Value::Null]]
        );
        assert_eq!(fake.loads.load(Ordering::Relaxed), 0);
    }

    /// The gates are per aggregate and all-or-nothing: one unprovable
    /// aggregate sends the whole query down the materializing path.
    #[test]
    fn gated_aggregates_fall_back_together() {
        // `c` holds a NaN: its MIN/MAX can never come from statistics, but
        // COUNT(c) still can.
        let fake = Arc::new(FakeLake::new(
            "/tmp/lake-nan",
            "main",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("c".to_string(), DataType::Double),
            ],
            vec![
                vec![Value::Integer(1), Value::Double(f64::NAN)],
                vec![Value::Integer(2), Value::Double(3.0)],
            ],
        ));
        let db = &mut PinkDb::new();
        db.attach_lake(fake.clone()).unwrap();
        assert_eq!(
            query(db, "SELECT COUNT(c), MIN(a) FROM t;"),
            vec![vec![Value::Bigint(2), Value::Integer(1)]]
        );
        assert_eq!(
            fake.loads.load(Ordering::Relaxed),
            0,
            "COUNT/MIN are provable"
        );

        let fake = Arc::new(FakeLake::new(
            "/tmp/lake-nan2",
            "main",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("c".to_string(), DataType::Double),
            ],
            vec![
                vec![Value::Integer(1), Value::Double(3.0)],
                vec![Value::Integer(2), Value::Double(f64::NAN)],
            ],
        ));
        let db = &mut PinkDb::new();
        db.attach_lake(fake.clone()).unwrap();
        // The whole query materializes because MIN(c) is not provable; the
        // engine's first-seen comparator skips the later NaN and answers 3.0.
        assert_eq!(
            query(db, "SELECT COUNT(c), MIN(c) FROM t;"),
            vec![vec![Value::Bigint(2), Value::Double(3.0)]]
        );
        assert_eq!(fake.loads.load(Ordering::Relaxed), 1);
    }

    /// A durable store refuses mutating statements that *read* an attached
    /// lake table: the statement is logged as SQL text, and the attached
    /// table (session-local, not in any checkpoint) could never replay.
    /// Read-only statements and in-memory stores are unaffected.
    #[test]
    fn durable_stores_refuse_logged_attached_reads() {
        let dir = std::env::temp_dir().join(format!("pinkdb_attach_wal_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_lake = || {
            Arc::new(FakeLake::new(
                "/tmp/lake-wal",
                "main",
                "u",
                vec![("x".to_string(), DataType::Integer)],
                vec![vec![Value::Integer(5)], vec![Value::Integer(6)]],
            ))
        };

        // The embedded path.
        let path = dir.join("d.pink");
        let path = path.to_str().unwrap();
        let mut db = PinkDb::open_durable(path).unwrap();
        db.execute("CREATE TABLE t (a BIGINT);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.attach_lake(wal_lake()).unwrap();
        // Reading the attached table is fine (queries are never logged).
        assert_eq!(
            query(&mut db, "SELECT COUNT(*) FROM u;"),
            vec![vec![Value::Bigint(2)]]
        );
        // A *mutating* statement that reads it is refused before it runs: the
        // logged SQL could not replay.
        let err = db
            .execute("UPDATE t SET a = (SELECT COUNT(x) FROM u);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("could not replay"), "{err}");
        assert_eq!(
            query(&mut db, "SELECT a FROM t;"),
            vec![vec![Value::Bigint(1)]],
            "the refused statement changed nothing"
        );
        // The WAL is clean: the store reopens and replays.
        drop(db);
        let db = PinkDb::open_durable(path).unwrap();
        assert_eq!(db.table("t").unwrap().get(0, 0), Some(Value::Bigint(1)));

        // The multi-writer connection path takes the same guard.
        let path2 = dir.join("c.pink");
        let path2 = path2.to_str().unwrap();
        let mut store = PinkDb::open_durable(path2).unwrap();
        store.execute("CREATE TABLE t (a BIGINT);").unwrap();
        store.execute("INSERT INTO t VALUES (1);").unwrap();
        store.attach_lake(wal_lake()).unwrap();
        let database = crate::Database::from_store(store);
        let mut conn = database.connect();
        let err = conn
            .execute("UPDATE t SET a = (SELECT COUNT(x) FROM u);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("could not replay"), "{err}");
        // A read over the connection is fine, and the store still reopens.
        let mut reader = database.connect();
        let mut results = reader.execute("SELECT COUNT(*) FROM u;").unwrap();
        match results.pop().unwrap() {
            crate::QueryResult::Selected { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Bigint(2)]])
            }
            other => panic!("expected rows, got {other:?}"),
        }
        drop(reader);
        drop(conn);
        drop(database);
        let db = PinkDb::open_durable(path2).unwrap();
        assert_eq!(db.table("t").unwrap().get(0, 0), Some(Value::Bigint(1)));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Value equivalence over a randomized corpus, all types included: every
    /// stats-answerable query must equal the same query over the same rows in
    /// an ordinary store (the in-memory executor).
    #[test]
    fn fast_path_answers_equal_in_memory_answers() {
        let schema = vec![
            ("i".to_string(), DataType::Integer),
            ("g".to_string(), DataType::Bigint),
            ("d".to_string(), DataType::Double),
            ("v".to_string(), DataType::Varchar),
            ("b".to_string(), DataType::Boolean),
            ("dt".to_string(), DataType::Date),
            ("ts".to_string(), DataType::Timestamp),
            ("n".to_string(), DataType::Integer),
        ];
        let mut rows = Vec::new();
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };
        for i in 0..300i64 {
            let r = next();
            let nulls = r % 7;
            let d = if i % 97 == 0 {
                Value::Double(f64::NAN)
            } else if i == 3 {
                Value::Double(-0.0)
            } else if r % 11 == 0 {
                Value::Double(f64::from_bits(r))
            } else {
                Value::Double((r % 2000) as f64 / 8.0 - 100.0)
            };
            rows.push(vec![
                Value::Integer((r % 500) as i32 - 250),
                Value::Bigint((r as i64).wrapping_sub(i64::MAX / 2)),
                d,
                Value::Varchar(if r % 5 == 0 {
                    "x".repeat((r % 300) as usize) // long/outlined strings
                } else {
                    format!("k{}-{}", r % 100, i % 3)
                }),
                Value::Boolean(r % 2 == 0),
                Value::Date((r % 20000) as i32 - 10000),
                Value::Timestamp((r as i64).wrapping_sub(5000)),
                if nulls == 0 {
                    Value::Null
                } else {
                    Value::Integer(i as i32)
                },
            ]);
        }
        let rules = |v: Value, i: usize| -> Value {
            // Make `n` all-NULL and `i` never NULL: NULL and non-NULL shapes
            // for COUNT(col).
            if i == 7 {
                Value::Null
            } else {
                v
            }
        };
        let rows: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, v)| rules(v, i))
                    .collect()
            })
            .collect();

        let fake = Arc::new(FakeLake::new(
            "/tmp/lake-random",
            "main",
            "t",
            schema.clone(),
            rows.clone(),
        ));
        let mut attached = PinkDb::new();
        attached.attach_lake(fake.clone()).unwrap();
        let mut plain = PinkDb::new();
        let cols: Vec<String> = schema.iter().map(|(n, t)| format!("{n} {t}")).collect();
        plain
            .execute(&format!("CREATE TABLE t ({});", cols.join(", ")))
            .unwrap();
        for row in &rows {
            plain
                .table_mut("t")
                .unwrap()
                .append_row(row.clone())
                .unwrap();
        }

        let mut queries = vec!["SELECT COUNT(*) FROM t;".to_string()];
        for (n, _t) in &schema {
            queries.push(format!("SELECT COUNT({n}) FROM t;"));
            queries.push(format!("SELECT MIN({n}), MAX({n}) FROM t;"));
            queries.push(format!("SELECT COUNT(*), COUNT({n}) FROM t;"));
        }
        for sql in queries {
            assert_rows_same(&query(&mut attached, &sql), &query(&mut plain, &sql), &sql);
        }
        // The NaN/zero gates must have refused `d`'s MIN/MAX, so at least one
        // query materialized the table.
        assert!(
            fake.loads.load(Ordering::Relaxed) >= 1,
            "the corpus contains NaNs: some MIN/MAX must have fallen back"
        );
    }
}
