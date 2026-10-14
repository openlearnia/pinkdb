//! Operators + statement execution.
//!
//! Each operator consumes `Vec<DataChunk>` and produces `Vec<DataChunk>`;
//! all computation is vector-at-a-time over ≤ [`CHUNK_SIZE`]-row chunks.
//! Sort / Distinct / Aggregate are blocking (they see all input); the rest
//! stream chunk-by-chunk. (A Volcano `next()` pull interface can replace
//! these `run_*` fns later without touching expression or storage code.)

pub mod aggregate;
pub mod expr;
pub(crate) mod window;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use crate::types::{FastMap, FastSet};

use aggregate::{
    aggregate, emit_groups, local_index_capacity, merge_partials, Acc, Group, Partial,
    HIGH_CARDINALITY_KEYS,
};
use aggregate::{eval_agg_vec as eval_fused_vec, EvalVec as FusedVec};
use expr::{coerce_to, count_true, eval_expr, select_true};
use rayon::prelude::*;
use window::eval_window;

use crate::plan::{LogicalPlan, agg_output_schema, compare_values, win_output_schema};
use crate::sql::ast::{BinaryOp, JoinKind};
use crate::sql::binder::{AggFunc, BoundAgg, BoundExpr, ScalarFunc, common_numeric};
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, VectorData, CHUNK_SIZE};

/// Result of one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Created,
    Inserted { count: usize },
    Updated { count: usize },
    Deleted { count: usize },
    Began,
    Committed,
    RolledBack,
    CreatedView { name: String },
    DroppedTable { name: String },
    DroppedView { name: String },
    CreatedDatabase { name: String },
    DroppedDatabase { name: String },
    /// `USE <db>`: the session layer applies this to its current database.
    Used { db: String },
    /// `LOAD EXTENSION '<path>'`: `name` is the extension's declared name.
    LoadedExtension { name: String },
    Selected { names: Vec<String>, types: Vec<DataType>, rows: Vec<Vec<Value>> },
}

#[derive(Clone)]
pub(crate) struct Output {
    chunks: Vec<DataChunk>,
    names: Vec<String>,
    types: Vec<DataType>,
}

/// Per-statement execution context: the database, visible CTE outputs
/// (CTEs shadow catalog tables), and memoised subquery results keyed by
/// plan-node address (the tree is immutable during execution, so each
/// uncorrelated subquery evaluates once even when probed per chunk).
pub(crate) struct Cx<'a> {
    pub(crate) db: &'a PinkDb,
    pub(crate) ctes: HashMap<String, Output>,
    pub(crate) memo: std::sync::Arc<std::sync::Mutex<HashMap<usize, Memo>>>,
}

#[derive(Clone)]
pub(crate) enum Memo {
    Scalar(Value),
    Set(FastSet<Vec<u8>>, bool, Option<DataType>),
    Flag(bool),
}

impl<'a> Cx<'a> {
    pub(crate) fn new(db: &'a PinkDb) -> Self {
        Self { db, ctes: HashMap::new(), memo: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())) }
    }
}

/// The database a DDL/DML plan targets: its binder-resolved qualifier, else
/// the store's current database (the embedded session's).
fn resolve_db(db: &PinkDb, qdb: &Option<String>) -> String {
    qdb.clone().unwrap_or_else(|| db.current_db().to_string())
}

/// `USE` validation + result. Bind already checks existence; re-checking here
/// closes the window for the multi-writer `Connection` path (bind and run are
/// separate lock acquisitions) and keeps `run` self-contained.
fn use_result(db: &PinkDb, name: &str) -> Result<QueryResult> {
    if !db.database_exists(name) {
        return Err(PinkDbError::Catalog(format!(
            "database \"{name}\" not found"
        )));
    }
    Ok(QueryResult::Used { db: name.to_ascii_lowercase() })
}

/// Execute a bound plan.
pub fn run(db: &mut PinkDb, plan: &LogicalPlan) -> Result<QueryResult> {
    match plan {
        LogicalPlan::CreateTable { db: qdb, name, schema } => {
            let target = resolve_db(db, qdb);
            db.create_table_in(&target, name, schema.clone())?;
            Ok(QueryResult::Created)
        }
        LogicalPlan::Insert { db: qdb, table, rows } => {
            let target = resolve_db(db, qdb);
            // Evaluate constant cells against an empty single-row chunk, then
            // append (storage coerces INTEGER → BIGINT → DOUBLE).
            let single = DataChunk { columns: Vec::new(), num_rows: 1 };
            let mut staged = Vec::with_capacity(rows.len());
            {
                let cx = Cx::new(db);
                for row in rows {
                    let values: Vec<Value> = row
                        .iter()
                        .map(|e| {
                            let v = eval_expr(e, &single, &cx)?;
                            if v.len() != 1 {
                                return Err(PinkDbError::Execution(
                                    "INSERT cell did not evaluate to one value".to_string(),
                                ));
                            }
                            Ok(v.get(0))
                        })
                        .collect::<Result<_>>()?;
                    staged.push(values);
                }
            }
            let count = staged.len();
            // Validate every row before appending any: multi-row INSERT is atomic.
            let schema = db.table_in(&target, table)?.schema();
            for values in &staged {
                if values.len() != schema.len() {
                    return Err(PinkDbError::Execution(format!(
                        "row has {} values but table \"{table}\" has {} columns",
                        values.len(),
                        schema.len()
                    )));
                }
                for (v, (_, t)) in values.iter().zip(schema.iter()) {
                    DataTable::coerce(v.clone(), *t)?;
                }
            }
            let t = db.table_mut_in(&target, table)?;
            for values in staged {
                t.append_row(values)?;
            }
            Ok(QueryResult::Inserted { count })
        }
        LogicalPlan::Update { db: qdb, table, sets, predicate } => {
            let target = resolve_db(db, qdb);
            // Phase 1: evaluate predicates + SET exprs over current chunks
            // (subqueries see pre-mutation state). Phase 2: write back.
            // SET exprs evaluate against original values (simultaneous).
            let mut per_chunk: Vec<Vec<(usize, usize, Value)>>;
            let mut count = 0usize;
            {
                let cx = Cx::new(db);
                let t = cx.db.table_in(&target, table)?;
                per_chunk = vec![Vec::new(); t.num_chunks()];
                for (ci, writes) in per_chunk.iter_mut().enumerate() {
                    let chunk = t.scan_chunk(ci).ok_or_else(|| {
                        PinkDbError::Execution("storage changed during UPDATE".to_string())
                    })?;
                    let targets = match predicate {
                        Some(p) => select_true(&eval_expr(p, &chunk, &cx)?)?,
                        None => (0..chunk.num_rows()).collect(),
                    };
                    count += targets.len();
                    for (col, expr) in sets {
                        let vals = eval_expr(expr, &chunk, &cx)?;
                        for &r in &targets {
                            writes.push((*col, r, vals.get(r)));
                        }
                    }
                }
            }
            let t = db.table_mut_in(&target, table)?;
            for (ci, writes) in per_chunk.iter().enumerate() {
                t.update_rows(ci, writes)?;
            }
            Ok(QueryResult::Updated { count })
        }
        LogicalPlan::Delete { db: qdb, table, predicate } => {
            let target = resolve_db(db, qdb);
            // Phase 1: collect per-chunk drop lists. Phase 2: apply from the
            // highest chunk down (emptied chunks are removed, shifting lower
            // indices — descending order keeps them valid).
            let mut drops: Vec<Vec<usize>>;
            {
                let cx = Cx::new(db);
                let t = cx.db.table_in(&target, table)?;
                drops = Vec::with_capacity(t.num_chunks());
                for ci in 0..t.num_chunks() {
                    let chunk = t.scan_chunk(ci).ok_or_else(|| {
                        PinkDbError::Execution("storage changed during DELETE".to_string())
                    })?;
                    drops.push(match predicate {
                        // Predicate TRUE ⇒ row is deleted (NULL ⇒ kept).
                        Some(p) => select_true(&eval_expr(p, &chunk, &cx)?)?,
                        None => (0..chunk.num_rows()).collect(),
                    });
                }
            }
            let mut count = 0usize;
            let t = db.table_mut_in(&target, table)?;
            for ci in (0..drops.len()).rev() {
                count += t.delete_from_chunk(ci, &drops[ci])?;
            }
            Ok(QueryResult::Deleted { count })
        }
        LogicalPlan::Begin => {
            db.begin()?;
            Ok(QueryResult::Began)
        }        LogicalPlan::Commit => {
            db.commit()?;
            Ok(QueryResult::Committed)
        }
        LogicalPlan::Rollback => {
            db.rollback()?;
            Ok(QueryResult::RolledBack)
        }
        LogicalPlan::CreateView { db: qdb, name, cols, query } => {
            let target = resolve_db(db, qdb);
            // Validated at bind time; store the AST for per-use rebinding.
            db.create_view_in(&target, name, (**query).clone(), cols.clone())?;
            Ok(QueryResult::CreatedView { name: name.clone() })
        }
        LogicalPlan::DropTable { db: qdb, name } => {
            let target = resolve_db(db, qdb);
            db.drop_table_in(&target, name)?;
            Ok(QueryResult::DroppedTable { name: name.clone() })
        }
        LogicalPlan::DropView { db: qdb, name } => {
            let target = resolve_db(db, qdb);
            db.drop_view_in(&target, name)?;
            Ok(QueryResult::DroppedView { name: name.clone() })
        }
        LogicalPlan::CreateDatabase { name } => {
            db.create_database(name)?;
            Ok(QueryResult::CreatedDatabase { name: name.clone() })
        }
        LogicalPlan::DropDatabase { name } => {
            db.drop_database(name)?;
            Ok(QueryResult::DroppedDatabase { name: name.clone() })
        }
        LogicalPlan::Use { name } => use_result(db, name),
        LogicalPlan::LoadExtension { path } => {
            if !db.allow_sql_load_extension() {
                return Err(PinkDbError::Execution(
                    "LOAD EXTENSION is disabled on this connection (the server \
                     must be started with --allow-load-extension)"
                        .to_string(),
                ));
            }
            // Process-level side effect: load the cdylib and register its
            // functions (atomic; a failure registers nothing). The DB stays
            // usable afterwards either way.
            let name = db.load_extension(path)?;
            Ok(QueryResult::LoadedExtension { name })
        }
        _ => {
            let cx = Cx::new(db);
            run_read_with(&cx, plan)
        }
    }
}

/// Execute a read-only plan against a shared store (used under a read lock
/// by concurrent readers; also backs [`run`]'s query arm).
pub fn run_read(db: &PinkDb, plan: &LogicalPlan) -> Result<QueryResult> {
    debug_assert!(crate::plan::is_read_only(plan), "run_read needs a query plan");
    let cx = Cx::new(db);
    run_read_with(&cx, plan)
}

fn run_read_with(cx: &Cx, plan: &LogicalPlan) -> Result<QueryResult> {
    // `USE` is read-only for locking but is not a row-producing plan: it
    // returns the target database for the session layer to apply.
    if let LogicalPlan::Use { name } = plan {
        return use_result(cx.db, name);
    }
    let out = eval(cx, plan)?;
    let mut rows = Vec::new();
    for chunk in &out.chunks {
        for i in 0..chunk.num_rows() {
            rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
        }
    }
    Ok(QueryResult::Selected { names: out.names, types: out.types, rows })
}

fn eval(cx: &Cx, plan: &LogicalPlan) -> Result<Output> {
    match plan {
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::Use { .. } => Err(PinkDbError::Execution(
            "DDL/DML does not produce rows".to_string(),
        )),
        LogicalPlan::Scan { db: qdb, table } => {
            // Unqualified scans name CTEs (binder-resolved); qualified scans
            // are catalog tables in their database.
            if qdb.is_none() {
                if let Some(out) = cx.ctes.get(&table.to_ascii_lowercase()) {
                    return Ok(out.clone());
                }
            }
            let lookup = qdb.as_deref().unwrap_or_else(|| cx.db.current_db());
            let t = cx.db.table_in(lookup, table)?;
            let mut chunks = Vec::with_capacity(t.num_chunks());
            for i in 0..t.num_chunks() {
                if let Some(c) = t.scan_chunk(i) {
                    chunks.push(c);
                }
            }
            let (names, types) = t.schema().into_iter().unzip();
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::SingleRow => Ok(Output {
            // One row, zero columns: literals evaluate to 1-row vectors.
            chunks: vec![DataChunk { columns: Vec::new(), num_rows: 1 }],
            names: vec![],
            types: vec![],
        }),
        LogicalPlan::Filter { input, predicate } => {
            let inner = eval(cx, input)?;
            // Per-chunk predicates evaluate in parallel; order is preserved.
            let chunks: Vec<Option<DataChunk>> = inner
                .chunks
                .par_iter()
                .map(|chunk| {
                    let pred = eval_expr(predicate, chunk, cx)?;
                    let sel = select_true(&pred)?;
                    Ok(if sel.len() == chunk.num_rows() {
                        Some(chunk.clone())
                    } else if sel.is_empty() {
                        None
                    } else {
                        Some(chunk.gather(&sel))
                    })
                })
                .collect::<Result<_>>()?;
            let chunks = chunks.into_iter().flatten().collect();
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
        LogicalPlan::Join { kind, left, right, left_key, right_key, .. } => {
            let l = eval(cx, left)?;
            let r = eval(cx, right)?;
            let mut names = l.names.clone();
            names.extend(r.names.iter().cloned());
            let mut types = l.types.clone();
            types.extend(r.types.iter().copied());
            let chunks = hash_join(
                cx,
                JoinSide { chunks: &l.chunks, types: &l.types },
                JoinSide { chunks: &r.chunks, types: &r.types },
                *kind,
                left_key,
                right_key,
            )?;
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            // Fused join→aggregate: feed joined rows straight into per-group
            // accumulators without ever materializing them. All fusibility
            // checks are static, so a blocked shape falls through without
            // having evaluated anything.
            if let Some(out) = fused_join_aggregate(cx, input, keys, aggs)? {
                return Ok(out);
            }
            // Fast path: pure COUNT(*) with no keys counts rows — no data
            // touched at all. Over a Filter, count the predicate's TRUE rows
            // per chunk straight away: the filtered chunks are never
            // materialized (no selection vector, no gathered columns).
            if keys.is_empty()
                && !aggs.is_empty()
                && aggs.iter().all(|a| a.func == crate::sql::binder::AggFunc::Count && a.arg.is_none())
            {
                // The binder dedups identical aggregates, so the only
                // all-COUNT(*) shape reaching here is a single COUNT(*) —
                // and this path emits exactly one column.
                debug_assert_eq!(aggs.len(), 1);
                let total: i64 = match &**input {
                    LogicalPlan::Filter { input: fin, predicate } => {
                        let inner = eval(cx, fin)?;
                        inner
                            .chunks
                            .par_iter()
                            .map(|chunk| {
                                let pred = eval_expr(predicate, chunk, cx)?;
                                count_true(&pred)
                            })
                            .collect::<Result<Vec<i64>>>()?
                            .into_iter()
                            .sum()
                    }
                    _ => {
                        let inner = eval(cx, input)?;
                        inner.chunks.iter().map(|c| c.num_rows() as i64).sum()
                    }
                };
                let mut vec = Vector::new_empty(DataType::Bigint);
                vec.push_valid(Value::Bigint(total))?;
                let chunks = vec![DataChunk::new(vec![vec])?];
                let mut names = Vec::with_capacity(aggs.len());
                let mut types = Vec::with_capacity(aggs.len());
                for (i, a) in aggs.iter().enumerate() {
                    names.push(format!("__agg{i}"));
                    types.push(a.dtype);
                }
                return Ok(Output { chunks, names, types });
            }
            let inner = eval(cx, input)?;
            let chunks = aggregate(cx, &inner.chunks, keys, aggs)?;
            let (names, types) = agg_output_schema(keys, aggs);
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::Window { input, windows, .. } => {
            let inner = eval(cx, input)?;
            let chunks = eval_window(cx, &inner.chunks, windows)?;
            let (names, types) = win_output_schema(windows);
            let mut all_names = inner.names.clone();
            all_names.extend(names);
            let mut all_types = inner.types.clone();
            all_types.extend(types);
            Ok(Output { chunks, names: all_names, types: all_types })
        }
        LogicalPlan::Union { left, right, distinct: is_distinct, types } => {
            let l = eval(cx, left)?;
            let r = eval(cx, right)?;
            // Coerce each side to the common column types, then concatenate.
            // Output names come from the left branch.
            let mut chunks = Vec::with_capacity(l.chunks.len() + r.chunks.len());
            for chunk in l.chunks.iter().chain(r.chunks.iter()) {
                let mut cols = Vec::with_capacity(types.len());
                for (col, t) in chunk.columns.iter().zip(types.iter()) {
                    cols.push(coerce_to(col, *t)?);
                }
                chunks.push(DataChunk::new(cols)?);
            }
            let chunks = if *is_distinct { distinct(&chunks)? } else { chunks };
            Ok(Output { chunks, names: l.names.clone(), types: types.clone() })
        }
        LogicalPlan::Cte { name, query, body } => {
            let out = eval(cx, query)?;
            let mut ctes = cx.ctes.clone();
            ctes.insert(name.clone(), out);
            let child = Cx { db: cx.db, ctes, memo: cx.memo.clone() };
            eval(&child, body)
        }
        LogicalPlan::Project { input, exprs, names } => {
            let inner = eval(cx, input)?;
            let types: Vec<DataType> = exprs.iter().map(|e| e.dtype()).collect();
            // Project over zero input chunks → zero output chunks (0 rows).
            // (Aggregates always emit ≥1 chunk, so COUNT(*) over ∅ still works.)
            let chunks = inner
                .chunks
                .par_iter()
                .map(|chunk| -> Result<DataChunk> {
                    // Pass-through column references (and aggregate/window
                    // output columns) share the input chunk's `Arc<Vector>`
                    // instead of deep-copying the payload.
                    let cols: Vec<std::sync::Arc<Vector>> = exprs
                        .iter()
                        .map(|e| match e {
                            BoundExpr::Column { index, .. }
                            | BoundExpr::AggRef { index, .. }
                            | BoundExpr::WindowRef { index, .. } => Ok(chunk.columns[*index].clone()),
                            e => Ok(std::sync::Arc::new(eval_expr(e, chunk, cx)?)),
                        })
                        .collect::<Result<_>>()?;
                    DataChunk::from_arcs(cols).ok_or_else(|| {
                        PinkDbError::Execution("project column length mismatch".to_string())
                    })
                })
                .collect::<Result<_>>()?;
            Ok(Output { chunks, names: names.clone(), types })
        }
        LogicalPlan::Distinct { input } => {
            let inner = eval(cx, input)?;
            let chunks = distinct(&inner.chunks)?;
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
        LogicalPlan::Sort { input, keys } => {
            let inner = eval(cx, input)?;
            let chunks = sort(cx, &inner.chunks, keys)?;
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
        LogicalPlan::Limit { input, limit } => {
            // Top-N fast path: LIMIT directly over SORT selects the first
            // `limit` rows with bounded per-partition heaps instead of a full
            // sort. Only worth it when the limit is much smaller than the row
            // count; otherwise — or when a NaN float key disables the byte
            // encoding — fall back to the full sort + truncate.
            if let LogicalPlan::Sort { input: sort_in, keys } = &**input {
                if *limit > 0 {
                    // Evaluate the sort input once: the top-N path and the
                    // fallback full sort below both consume these chunks
                    // (re-evaluating `sort_in` here would run the whole
                    // subtree twice).
                    let inner = eval(cx, sort_in)?;
                    let n: usize = inner.chunks.iter().map(|c| c.num_rows()).sum();
                    let chunks = if limit.saturating_mul(16) <= n {
                        match top_n(cx, &inner.chunks, keys, *limit)? {
                            Some(chunks) => chunks,
                            // NaN float key: full sort with the legacy comparator.
                            None => sort(cx, &inner.chunks, keys)?,
                        }
                    } else {
                        // Limit too large for top-N: full sort + truncate.
                        sort(cx, &inner.chunks, keys)?
                    };
                    let out = Output { chunks, names: inner.names, types: inner.types };
                    return Ok(truncate_output(out, *limit));
                }
            }
            let inner = eval(cx, input)?;
            Ok(truncate_output(inner, *limit))
        }
    }
}

// ---------- blocking operators ----------

/// Sentinel ending a match chain in [`MatchTable`]'s arena.
const NIL: u32 = u32::MAX;

/// One arena node: (right chunk, right row, next node in the key's chain).
type MatchNode = (u32, u32, u32);

/// Key dtypes whose payload is a single integer that widens losslessly to
/// i64. The int path requires BOTH dtypes in the integer family, so
/// INTEGER=BIGINT is the only cross-type pairing (DATE and TIMESTAMP pair
/// only with themselves); `int_key_at` therefore never truncates a value.
fn is_int_key(t: DataType) -> bool {
    matches!(
        t,
        DataType::Integer | DataType::Bigint | DataType::Date | DataType::Timestamp
    )
}

/// Read one integer-payload key cell widened to i64 (the `i32` payloads —
/// INTEGER/DATE — cannot overflow the cast).
#[inline]
fn int_key_at(v: &Vector, i: usize) -> i64 {
    match v.data() {
        VectorData::Int32(d) => d[i] as i64,
        VectorData::Int64(d) => d[i],
        VectorData::Date32(d) => d[i] as i64,
        VectorData::TimestampMicros(d) => d[i],
        _ => unreachable!("int_key_at called on a non-integer-payload key vector"),
    }
}

/// Evaluate one side's join key over `chunk`. Integer-payload keys are read
/// directly through `int_key_at`, so they never go through `coerce_to` or the
/// byte encoding; byte-path keys are canonicalized to the common numeric type
/// when the two sides differ (the binder allows `1 = 1.0`). Same-type keys
/// skip the deep clone `coerce_to` would do.
fn eval_join_key(
    cx: &Cx,
    key: &BoundExpr,
    chunk: &DataChunk,
    common: Option<DataType>,
    ints: bool,
) -> Result<Vector> {
    let kv = eval_expr(key, chunk, cx)?;
    if ints {
        return Ok(kv);
    }
    match common {
        Some(t) if kv.dtype() != t => coerce_to(&kv, t),
        _ => Ok(kv),
    }
}

/// Build-side match table of a hash join: key → (head, tail) of that key's
/// match chain. The arena holds one `(right_chunk, right_row, next)` node per
/// match in right input order (sentinel [`NIL`] ends a chain); appending links
/// the new node after the tail. This replaces a small `Vec` allocation per
/// distinct key while keeping one hash lookup per row. (A `(start, len)` slice
/// into a flat arena cannot work in one pass: matches of distinct keys
/// interleave, so a key's rows are not contiguous.)
///
/// Two key representations, chosen by the bound key dtypes:
/// - `Ints`: INTEGER/BIGINT/DATE/TIMESTAMP payloads widened to i64 — no
///   encoding and no `coerce_to`;
/// - `Bytes`: the generic `Vector::encode_into` encoding for every other
///   dtype (VARCHAR, DOUBLE, BOOLEAN, mixed numeric).
///
/// Probe/assembly reads either variant through the same accessors, so the
/// future fused join→aggregate path can reuse the table as-is.
enum MatchTable {
    Ints {
        map: FastMap<i64, (u32, u32)>,
        arena: Vec<MatchNode>,
    },
    Bytes {
        map: FastMap<Vec<u8>, (u32, u32)>,
        arena: Vec<MatchNode>,
    },
}

impl MatchTable {
    /// Build over the right input. `reserve` is the caller's capacity policy
    /// (distinct keys, not rows: a skewed join must not preallocate one slot
    /// per right row; both containers grow on demand past the cap).
    fn build(
        cx: &Cx,
        right_chunks: &[DataChunk],
        right_key: &BoundExpr,
        common: Option<DataType>,
        ints: bool,
        reserve: usize,
    ) -> Result<MatchTable> {
        let mut table = if ints {
            MatchTable::Ints {
                map: FastMap::with_capacity_and_hasher(reserve, Default::default()),
                arena: Vec::with_capacity(reserve),
            }
        } else {
            MatchTable::Bytes {
                map: FastMap::with_capacity_and_hasher(reserve, Default::default()),
                arena: Vec::with_capacity(reserve),
            }
        };
        let mut buf = Vec::new();
        for (ci, chunk) in right_chunks.iter().enumerate() {
            let kv = eval_join_key(cx, right_key, chunk, common, ints)?;
            for i in 0..chunk.num_rows() {
                if kv.is_null(i) {
                    continue;
                }
                match &mut table {
                    MatchTable::Ints { map, arena } => {
                        let key = int_key_at(&kv, i);
                        let node = arena.len() as u32;
                        arena.push((ci as u32, i as u32, NIL));
                        match map.get_mut(&key) {
                            Some(slot) => {
                                arena[slot.1 as usize].2 = node;
                                slot.1 = node;
                            }
                            None => {
                                map.insert(key, (node, node));
                            }
                        }
                    }
                    MatchTable::Bytes { map, arena } => {
                        // Borrowed encoding, get-first: hits allocate nothing.
                        buf.clear();
                        kv.encode_into(&mut buf, i);
                        let node = arena.len() as u32;
                        arena.push((ci as u32, i as u32, NIL));
                        match map.get_mut(&buf) {
                            Some(slot) => {
                                arena[slot.1 as usize].2 = node;
                                slot.1 = node;
                            }
                            None => {
                                map.insert(buf.clone(), (node, node));
                            }
                        }
                    }
                }
            }
        }
        Ok(table)
    }

    /// Probe accessor: head node of `key`'s match chain, if any. The caller
    /// picks the accessor matching the representation the table was built
    /// with (`ints`).
    #[inline]
    fn head_int(&self, key: i64) -> Option<u32> {
        match self {
            MatchTable::Ints { map, .. } => map.get(&key).map(|&(head, _)| head),
            MatchTable::Bytes { .. } => unreachable!("int key probed against a byte match table"),
        }
    }

    /// Probe accessor for byte-encoded keys.
    #[inline]
    fn head_bytes(&self, key: &[u8]) -> Option<u32> {
        match self {
            MatchTable::Bytes { map, .. } => map.get(key).map(|&(head, _)| head),
            MatchTable::Ints { .. } => unreachable!("byte key probed against an int match table"),
        }
    }

    /// `(right_chunk, right_row, next)` of arena node `node`.
    #[inline]
    fn node(&self, node: u32) -> MatchNode {
        match self {
            MatchTable::Ints { arena, .. } | MatchTable::Bytes { arena, .. } => {
                arena[node as usize]
            }
        }
    }
}

/// One hash-join input: its chunks plus its output dtypes.
#[derive(Clone, Copy)]
struct JoinSide<'a> {
    chunks: &'a [DataChunk],
    types: &'a [DataType],
}

/// Hash join dispatcher. Output columns are always left ++ right.
/// - `Inner`: build on the right, probe with the left, emit matched pairs.
/// - `Left`: same build/probe, plus left columns + NULLs for every unmatched
///   left row.
/// - `Right`: mirrored — build on the left, probe with the right, plus NULLs
///   for the left columns + the right row for every unmatched right row.
///
/// NULL keys never match on either side, so they NULL-extend in outer joins.
fn hash_join(
    cx: &Cx,
    left: JoinSide<'_>,
    right: JoinSide<'_>,
    kind: JoinKind,
    left_key: &BoundExpr,
    right_key: &BoundExpr,
) -> Result<Vec<DataChunk>> {
    // The preserved side is the probing side: left for Inner/Left, right for
    // Right. `preserved_is_right` places it in the declared left ++ right
    // output order; `extend_unmatched` turns unmatched preserved rows into
    // NULL-extended rows (outer) instead of dropping them (inner).
    match kind {
        JoinKind::Inner => hash_join_probe(cx, left, right, left_key, right_key, false, false),
        JoinKind::Left => hash_join_probe(cx, left, right, left_key, right_key, false, true),
        JoinKind::Right => hash_join_probe(cx, right, left, right_key, left_key, true, true),
    }
}

/// Shared hash-join probe/assembly loop. `preserved` is the side whose rows
/// all appear in the output, in input order (left for Inner/Left, right for
/// Right); `build` feeds the match table. `preserved_is_right` says which
/// side of the declared `left ++ right` output the preserved side is.
/// `extend_unmatched` NULL-extends unmatched preserved rows (outer joins)
/// instead of dropping them (inner). NULL keys never match, so they
/// NULL-extend in outer joins and are skipped in an inner join.
///
/// Matched pairs emit build/probe columns in declared order (left ++ right);
/// an unmatched preserved row emits its own columns plus NULLs for the other
/// side. Probe order and per-key build-row order are deterministic (left-row
/// order for Inner/Left, right-row order for Right).
fn hash_join_probe(
    cx: &Cx,
    preserved: JoinSide<'_>,
    build: JoinSide<'_>,
    preserved_key: &BoundExpr,
    build_key: &BoundExpr,
    preserved_is_right: bool,
    extend_unmatched: bool,
) -> Result<Vec<DataChunk>> {
    // Integer-payload keys take the i64-specialized match table: no byte
    // encoding and no `coerce_to` (INTEGER=BIGINT widens per cell). Every
    // other dtype keeps the byte path, canonicalized to the common numeric
    // type first (binder allows `1 = 1.0`); same-type keys skip the deep
    // clone `coerce_to` would do. The int check and common-type resolution
    // are symmetric in the two key dtypes.
    let ints = is_int_key(preserved_key.dtype()) && is_int_key(build_key.dtype());
    let common = common_numeric(preserved_key.dtype(), build_key.dtype()).ok();
    let build_rows: usize = build.chunks.iter().map(|c| c.num_rows()).sum();
    if build_rows > u32::MAX as usize {
        return Err(PinkDbError::Execution(
            "hash join build side exceeds u32 rows".to_string(),
        ));
    }
    // Reserve for distinct keys, not rows: a skewed join would otherwise
    // allocate `build_rows` map/arena slots for a handful of keys. Both grow
    // on demand past the cap.
    let reserve = build_rows.min(1 << 20);
    let table = MatchTable::build(cx, build.chunks, build_key, common, ints, reserve)?;

    // Probe + assemble, parallel per preserved chunk: matches push straight
    // from the source chunks into per-partition output vectors (no
    // intermediate location tuples, no serial assembly pass). A partition can
    // exceed CHUNK_SIZE rows, so it emits chunks as they fill; the caller
    // concatenates partitions in order, preserving preserved-row order.
    let (left_types, right_types) = if preserved_is_right {
        (build.types, preserved.types)
    } else {
        (preserved.types, build.types)
    };
    let mut dtypes: Vec<DataType> = left_types.to_vec();
    dtypes.extend(right_types.iter().copied());
    let n_left = left_types.len();
    let partitions: Vec<Vec<DataChunk>> = preserved
        .chunks
        .par_iter()
        .map(|chunk| -> Result<Vec<DataChunk>> {
            let kv = eval_join_key(cx, preserved_key, chunk, common, ints)?;
            let mut finished = Vec::new();
            let mut cur: Vec<Vector> =
                dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
            let mut cur_rows = 0usize;
            let mut buf = Vec::new();
            for i in 0..chunk.num_rows() {
                let head = if kv.is_null(i) {
                    if !extend_unmatched {
                        continue;
                    }
                    None
                } else if ints {
                    table.head_int(int_key_at(&kv, i))
                } else {
                    buf.clear();
                    kv.encode_into(&mut buf, i);
                    table.head_bytes(&buf)
                };
                match head {
                    Some(mut node) => {
                        while node != NIL {
                            let (bci, bi, next) = table.node(node);
                            if cur_rows >= CHUNK_SIZE {
                                finished.push(DataChunk::new(std::mem::take(&mut cur))?);
                                cur = dtypes
                                    .iter()
                                    .map(|t| Vector::with_capacity(*t, CHUNK_SIZE))
                                    .collect();
                                cur_rows = 0;
                            }
                            let bchunk = &build.chunks[bci as usize];
                            if preserved_is_right {
                                for (dst, col) in
                                    cur.iter_mut().take(n_left).zip(bchunk.columns.iter())
                                {
                                    dst.push_from(col, bi as usize)?;
                                }
                                for (dst, col) in
                                    cur.iter_mut().skip(n_left).zip(chunk.columns.iter())
                                {
                                    dst.push_from(col, i)?;
                                }
                            } else {
                                for (dst, col) in
                                    cur.iter_mut().take(n_left).zip(chunk.columns.iter())
                                {
                                    dst.push_from(col, i)?;
                                }
                                for (dst, col) in
                                    cur.iter_mut().skip(n_left).zip(bchunk.columns.iter())
                                {
                                    dst.push_from(col, bi as usize)?;
                                }
                            }
                            cur_rows += 1;
                            node = next;
                        }
                    }
                    None => {
                        // Inner: no match, drop the row. Outer: unmatched (or
                        // NULL-keyed) preserved row — NULL-extend.
                        if !extend_unmatched {
                            continue;
                        }
                        if cur_rows >= CHUNK_SIZE {
                            finished.push(DataChunk::new(std::mem::take(&mut cur))?);
                            cur = dtypes
                                .iter()
                                .map(|t| Vector::with_capacity(*t, CHUNK_SIZE))
                                .collect();
                            cur_rows = 0;
                        }
                        if preserved_is_right {
                            for dst in cur.iter_mut().take(n_left) {
                                dst.push_null()?;
                            }
                            for (dst, col) in
                                cur.iter_mut().skip(n_left).zip(chunk.columns.iter())
                            {
                                dst.push_from(col, i)?;
                            }
                        } else {
                            for (dst, col) in
                                cur.iter_mut().take(n_left).zip(chunk.columns.iter())
                            {
                                dst.push_from(col, i)?;
                            }
                            for dst in cur.iter_mut().skip(n_left) {
                                dst.push_null()?;
                            }
                        }
                        cur_rows += 1;
                    }
                }
            }
            if cur_rows > 0 {
                finished.push(DataChunk::new(cur)?);
            }
            Ok(finished)
        })
        .collect::<Result<_>>()?;
    Ok(partitions.into_iter().flatten().collect())
}

// ---------- fused join → aggregate ----------

/// Which join input one rewritten aggregate expression reads from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    /// Every column reference is `< left_cols` (left input).
    Left,
    /// Every column reference is `>= left_cols` (right input).
    Right,
    /// No column references at all.
    Constant,
}

/// Combine the sides of a binary/vector expression: a constant operand
/// never conflicts; two different data sides do (not fusible).
fn merge_sides(a: Side, b: Side) -> Option<Side> {
    match (a, b) {
        (Side::Constant, s) | (s, Side::Constant) => Some(s),
        (a, b) if a == b => Some(a),
        _ => None,
    }
}

/// Side classification of a join-space expression: `Some(Left)`/`Some(Right)`
/// when every column reference lies entirely on one side, `Some(Constant)`
/// for column-free trees, `None` when the sides mix or the tree holds a
/// subquery / AggRef / WindowRef (none of which the fused path can evaluate
/// per side).
fn classify_side(expr: &BoundExpr, left_cols: usize) -> Option<Side> {
    match expr {
        BoundExpr::Literal(_) => Some(Side::Constant),
        BoundExpr::Column { index, .. } => {
            Some(if *index < left_cols { Side::Left } else { Side::Right })
        }
        BoundExpr::Binary { left, right, .. } => {
            merge_sides(classify_side(left, left_cols)?, classify_side(right, left_cols)?)
        }
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. }
        | BoundExpr::Cast { expr: e, .. } => classify_side(e, left_cols),
        BoundExpr::Scalar { args, .. } | BoundExpr::Coalesce { args, .. } => {
            let mut side = Side::Constant;
            for a in args {
                side = merge_sides(side, classify_side(a, left_cols)?)?;
            }
            Some(side)
        }
        BoundExpr::Case { branches, else_, .. } => {
            let mut side = Side::Constant;
            for (c, v) in branches {
                side = merge_sides(side, classify_side(c, left_cols)?)?;
                side = merge_sides(side, classify_side(v, left_cols)?)?;
            }
            if let Some(e) = else_ {
                side = merge_sides(side, classify_side(e, left_cols)?)?;
            }
            Some(side)
        }
        // Aggregate/window output columns, subqueries, and extension calls
        // (opaque: possibly impure and fallible) are not fusible.
        BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::InSubquery { .. }
        | BoundExpr::DynamicScalar { .. }
        | BoundExpr::Exists { .. } => None,
    }
}

/// Rewrite an aggregate expression from project-output space into join
/// space: every `BoundExpr::Column { index, .. }` becomes
/// `project.exprs[index]`. `None` (reject the fusion) when a subquery,
/// AggRef, or WindowRef occurs anywhere — including inside a substituted
/// project expression — or an index is out of range. `project` is `None`
/// when the aggregate's input is the join itself (expressions are already
/// join-space).
fn rewrite_through_project(
    expr: &BoundExpr,
    project: Option<&[BoundExpr]>,
) -> Option<BoundExpr> {
    match expr {
        BoundExpr::Literal(v) => Some(BoundExpr::Literal(v.clone())),
        BoundExpr::Column { index, dtype } => match project {
            None => Some(BoundExpr::Column { index: *index, dtype: *dtype }),
            Some(pexprs) => {
                let p = pexprs.get(*index)?;
                if p.has_subquery() {
                    return None;
                }
                Some(p.clone())
            }
        },
        BoundExpr::Binary { op, left, right, dtype } => Some(BoundExpr::Binary {
            op: *op,
            left: Box::new(rewrite_through_project(left, project)?),
            right: Box::new(rewrite_through_project(right, project)?),
            dtype: *dtype,
        }),
        BoundExpr::Not(e) => {
            Some(BoundExpr::Not(Box::new(rewrite_through_project(e, project)?)))
        }
        BoundExpr::Negate { expr, dtype } => Some(BoundExpr::Negate {
            expr: Box::new(rewrite_through_project(expr, project)?),
            dtype: *dtype,
        }),
        BoundExpr::IsNull { expr, negated } => Some(BoundExpr::IsNull {
            expr: Box::new(rewrite_through_project(expr, project)?),
            negated: *negated,
        }),
        BoundExpr::Scalar { func, args, dtype } => Some(BoundExpr::Scalar {
            func: *func,
            args: args
                .iter()
                .map(|a| rewrite_through_project(a, project))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        BoundExpr::Cast { expr, dtype } => Some(BoundExpr::Cast {
            expr: Box::new(rewrite_through_project(expr, project)?),
            dtype: *dtype,
        }),
        BoundExpr::Coalesce { args, dtype } => Some(BoundExpr::Coalesce {
            args: args
                .iter()
                .map(|a| rewrite_through_project(a, project))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        BoundExpr::Case { branches, else_, dtype } => Some(BoundExpr::Case {
            branches: branches
                .iter()
                .map(|(c, v)| {
                    Some((
                        rewrite_through_project(c, project)?,
                        rewrite_through_project(v, project)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
            else_: match else_ {
                Some(e) => Some(Box::new(rewrite_through_project(e, project)?)),
                None => None,
            },
            dtype: *dtype,
        }),
        // Aggregate/window output columns, subqueries, and extension calls
        // (opaque: possibly impure and fallible) are not fusible.
        BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::InSubquery { .. }
        | BoundExpr::DynamicScalar { .. }
        | BoundExpr::Exists { .. } => None,
    }
}

/// Shift a joined-space expression down by `left_cols`: column index `i`
/// becomes `i - left_cols`, so the expression evaluates against the right
/// input alone (the same rewrite the optimizer applies to right-side
/// predicates pushed below a join). Only right-side expressions are shifted,
/// so no index underflows. Rejected variants clone through, keeping the walk
/// total.
fn shift_join_columns(expr: &BoundExpr, left_cols: usize) -> BoundExpr {
    match expr {
        BoundExpr::Column { index, dtype } => {
            BoundExpr::Column { index: index - left_cols, dtype: *dtype }
        }
        BoundExpr::Literal(v) => BoundExpr::Literal(v.clone()),
        BoundExpr::Binary { op, left, right, dtype } => BoundExpr::Binary {
            op: *op,
            left: Box::new(shift_join_columns(left, left_cols)),
            right: Box::new(shift_join_columns(right, left_cols)),
            dtype: *dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(shift_join_columns(e, left_cols))),
        BoundExpr::Negate { expr, dtype } => BoundExpr::Negate {
            expr: Box::new(shift_join_columns(expr, left_cols)),
            dtype: *dtype,
        },
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(shift_join_columns(expr, left_cols)),
            negated: *negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func: *func,
            args: args.iter().map(|a| shift_join_columns(a, left_cols)).collect(),
            dtype: *dtype,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(shift_join_columns(expr, left_cols)),
            dtype: *dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args.iter().map(|a| shift_join_columns(a, left_cols)).collect(),
            dtype: *dtype,
        },
        BoundExpr::Case { branches, else_, dtype } => BoundExpr::Case {
            branches: branches
                .iter()
                .map(|(c, v)| {
                    (shift_join_columns(c, left_cols), shift_join_columns(v, left_cols))
                })
                .collect(),
            else_: else_.as_ref().map(|e| Box::new(shift_join_columns(e, left_cols))),
            dtype: *dtype,
        },
        BoundExpr::DynamicScalar { name, args, dtype } => BoundExpr::DynamicScalar {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| shift_join_columns(a, left_cols))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            dtype: *dtype,
        },
        BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::InSubquery { .. }
        | BoundExpr::Exists { .. } => expr.clone(),
    }
}

/// A rewritten key/argument expression plus the join side it reads from.
struct FusedExpr {
    expr: BoundExpr,
    side: Side,
}

/// Where one key/agg-arg gets its runtime values.
#[derive(Clone, Copy)]
enum ExprSource {
    /// Left-side or constant expression: index into the vectors evaluated
    /// once per left chunk.
    Left(usize),
    /// Right-side expression: index into the per-right-chunk vectors.
    Right(usize),
}

/// Rewrite + classify one aggregate key/argument. `None` ⇒ not fusible.
fn fusible_expr(
    expr: &BoundExpr,
    project: Option<&[BoundExpr]>,
    left_cols: usize,
) -> Option<FusedExpr> {
    let expr = rewrite_through_project(expr, project)?;
    // The fused path pre-evaluates this expression over the *whole* left or
    // right input, but the materialized path only evaluates it on matched
    // join rows. A value-dependent error on a never-matched row would then
    // surface only under fusion, so reject any expression that can error.
    if !expr_is_total(&expr) {
        return None;
    }
    let side = classify_side(&expr, left_cols)?;
    Some(FusedExpr { expr, side })
}

/// True when evaluating `e` on any valid value of its input type cannot
/// return an error (a panic-free, error-free evaluation). Used to keep the
/// fused join→aggregate path from observing value-dependent errors on rows
/// the materialized path never evaluates.
///
/// The rules mirror `exec::expr`'s evaluation:
/// - `Literal` / `Column` are pure reads;
/// - `Not` / `IsNull` only recurse; `Negate` is total only on DOUBLE
///   (integer negation of `MIN` is an overflow error);
/// - comparisons, `AND`/`OR`, `LIKE` never fail on well-typed operands;
///   `+`/`-`/`*` are total only when they produce DOUBLE (IEEE) or subtract
///   two DATEs (an i32 difference always fits BIGINT) — integer arithmetic
///   and DATE ± n raise overflow errors;
/// - `/` avoids the integer divide-by-zero rule only when both operands are
///   DOUBLE (pure IEEE); `%` is integer-only and errors on a zero divisor;
/// - `CAST` can fail per value (VARCHAR→numeric parses, date parses,
///   out-of-range timestamp widening);
/// - `COALESCE`/`CASE` evaluate every arm, so all arms must be total;
/// - only the scalar functions verified in `apply_scalar` to never return a
///   value-dependent `Err` on binder-typed arguments are whitelisted
///   (`substr` is left out with the rest; `date_trunc` errors on bad parts;
///   `abs` only on DOUBLE, since integer `abs(MIN)` overflows);
/// - subqueries / aggregate / window refs are rejected before this point.
fn expr_is_total(e: &BoundExpr) -> bool {
    match e {
        BoundExpr::Literal(_) | BoundExpr::Column { .. } => true,
        BoundExpr::Not(x) | BoundExpr::IsNull { expr: x, .. } => expr_is_total(x),
        BoundExpr::Negate { expr, dtype } => *dtype == DataType::Double && expr_is_total(expr),
        BoundExpr::Binary { op, left, right, dtype } => match op {
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply => {
                let date_diff = *op == BinaryOp::Subtract
                    && left.dtype() == DataType::Date
                    && right.dtype() == DataType::Date;
                (*dtype == DataType::Double || date_diff)
                    && expr_is_total(left)
                    && expr_is_total(right)
            }
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Like
            | BinaryOp::NotLike => expr_is_total(left) && expr_is_total(right),
            // int/int division errors on a zero divisor; only DOUBLE/DOUBLE
            // (both sides, so the int-div rule can never trigger) is safe.
            BinaryOp::Divide => {
                left.dtype() == DataType::Double
                    && right.dtype() == DataType::Double
                    && expr_is_total(left)
                    && expr_is_total(right)
            }
            BinaryOp::Modulo => false,
        },
        BoundExpr::Cast { .. } => false,
        BoundExpr::Coalesce { args, .. } => args.iter().all(expr_is_total),
        BoundExpr::Case { branches, else_, .. } => {
            branches.iter().all(|(c, v)| expr_is_total(c) && expr_is_total(v))
                && else_.as_ref().is_none_or(|e| expr_is_total(e))
        }
        BoundExpr::Scalar { func, args, .. } => {
            let abs_ok = *func != ScalarFunc::Abs
                || args.first().is_some_and(|a| a.dtype() == DataType::Double);
            abs_ok
                && matches!(
                func,
                ScalarFunc::Upper
                    | ScalarFunc::Lower
                    | ScalarFunc::Length
                    | ScalarFunc::Abs
                    | ScalarFunc::Round
                    | ScalarFunc::Year
                    | ScalarFunc::Month
                    | ScalarFunc::Day
                    | ScalarFunc::Hour
                    | ScalarFunc::Minute
                    | ScalarFunc::Second
            ) && args.iter().all(expr_is_total)
        }
        BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::InSubquery { .. }
        | BoundExpr::DynamicScalar { .. }
        | BoundExpr::Exists { .. } => false,
    }
}

/// Try to execute `Aggregate` over `Join` or `Project over Join` without
/// materializing the joined rows: each match feeds one row into its group's
/// accumulators directly. Returns `Ok(None)` — before evaluating anything —
/// when the shape or any aggregate key/argument is not fusible.
///
/// Row feed order is exactly the materialized pipeline's: left chunks in
/// order and, within a left row, matches in right-row order, so first-seen
/// group order and accumulation order match. Group-key encoding, key
/// `Value`s, and accumulator updates read the same source vectors the
/// materialized path sees, so integer/exact results are bit-identical
/// (float partials may differ at the ulp level — accepted).
fn fused_join_aggregate(
    cx: &Cx,
    input: &LogicalPlan,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Option<Output>> {
    // Shape: Aggregate over Join, or over Project over Join. A Filter /
    // Distinct / Window / Limit / Cte between the aggregate and the join
    // (or any other input) keeps the materialized path.
    let (kind, left, right, left_key, right_key, left_cols, project) = match input {
        LogicalPlan::Join { kind, left, right, left_key, right_key, left_cols } => {
            (*kind, &**left, &**right, left_key, right_key, *left_cols, None)
        }
        LogicalPlan::Project { input: inner, exprs, .. } => match &**inner {
            LogicalPlan::Join { kind, left, right, left_key, right_key, left_cols } => {
                (*kind, &**left, &**right, left_key, right_key, *left_cols, Some(&exprs[..]))
            }
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    // Outer joins must materialize their NULL-extended rows: fusion only
    // feeds matched pairs into the accumulators, so it would undercount.
    // Static kind check — nothing has been evaluated yet.
    if kind != JoinKind::Inner {
        return Ok(None);
    }

    // With a Project in the shape, fusion rewrites only the aggregate's
    // keys/args through it, so project exprs the aggregate does not reference
    // are never evaluated — while the materialized path evaluates all of
    // them. Evaluating a total, subquery-free expr has no observable effect,
    // so skipping an unused one is safe; a non-total or subquery-bearing
    // project expr must force the materialized path so it errors exactly as
    // before. (Side classification is irrelevant for exprs nothing reads.)
    if let Some(exprs) = project {
        if exprs.iter().any(|e| e.has_subquery() || !expr_is_total(e)) {
            return Ok(None);
        }
    }

    // Static fusibility: rewrite every key/arg into join space and classify
    // its side. Nothing is evaluated before all checks pass.
    let mut key_exprs: Vec<FusedExpr> = Vec::with_capacity(keys.len());
    for k in keys {
        match fusible_expr(k, project, left_cols) {
            Some(f) => key_exprs.push(f),
            None => return Ok(None),
        }
    }
    let mut arg_exprs: Vec<Option<FusedExpr>> = Vec::with_capacity(aggs.len());
    for a in aggs {
        let spec = match &a.arg {
            None => None, // COUNT(*)
            Some(e) => match fusible_expr(e, project, left_cols) {
                Some(f) => Some(f),
                None => return Ok(None),
            },
        };
        arg_exprs.push(spec);
    }
    // Right-side expressions evaluate over the right input alone, so their
    // joined-space column indices shift down by `left_cols`.
    for f in key_exprs.iter_mut().chain(arg_exprs.iter_mut().flatten()) {
        if f.side == Side::Right {
            f.expr = shift_join_columns(&f.expr, left_cols);
        }
    }
    // Left-side and constant expressions evaluate once per left chunk; right
    // expressions once per right chunk.
    let mut left_exprs: Vec<&BoundExpr> = Vec::new();
    let mut right_exprs: Vec<&BoundExpr> = Vec::new();
    let mut key_srcs: Vec<ExprSource> = Vec::with_capacity(key_exprs.len());
    for k in &key_exprs {
        key_srcs.push(match k.side {
            Side::Right => {
                right_exprs.push(&k.expr);
                ExprSource::Right(right_exprs.len() - 1)
            }
            Side::Left | Side::Constant => {
                left_exprs.push(&k.expr);
                ExprSource::Left(left_exprs.len() - 1)
            }
        });
    }
    let mut arg_srcs: Vec<Option<ExprSource>> = Vec::with_capacity(arg_exprs.len());
    for a in &arg_exprs {
        arg_srcs.push(a.as_ref().map(|f| match f.side {
            Side::Right => {
                right_exprs.push(&f.expr);
                ExprSource::Right(right_exprs.len() - 1)
            }
            Side::Left | Side::Constant => {
                left_exprs.push(&f.expr);
                ExprSource::Left(left_exprs.len() - 1)
            }
        }));
    }

    let l = eval(cx, left)?;
    let r = eval(cx, right)?;

    // Build the match table exactly like `hash_join`: same int
    // specialization, `common_numeric` canonicalization, u32 guard, and
    // capacity policy.
    let ints = is_int_key(left_key.dtype()) && is_int_key(right_key.dtype());
    let common = common_numeric(left_key.dtype(), right_key.dtype()).ok();
    let right_rows: usize = r.chunks.iter().map(|c| c.num_rows()).sum();
    if right_rows > u32::MAX as usize {
        return Err(PinkDbError::Execution(
            "hash join build side exceeds u32 rows".to_string(),
        ));
    }
    let reserve = right_rows.min(1 << 20);
    let table = MatchTable::build(cx, &r.chunks, right_key, common, ints, reserve)?;

    // Fast path: keys-free pure COUNT(*) counts matched pairs by walking the
    // match chains — no per-pair `Value` work, no group state.
    if keys.is_empty()
        && !aggs.is_empty()
        && aggs.iter().all(|a| a.func == AggFunc::Count && a.arg.is_none())
    {
        // Binder dedup leaves exactly one COUNT(*); this path emits one
        // column (and `agg_output_schema` below still reflects `aggs`).
        debug_assert_eq!(aggs.len(), 1);
        let total: i64 = l
            .chunks
            .par_iter()
            .map(|chunk| -> Result<i64> {
                let kv = eval_join_key(cx, left_key, chunk, common, ints)?;
                let mut count = 0i64;
                let mut buf = Vec::new();
                for li in 0..chunk.num_rows() {
                    if kv.is_null(li) {
                        continue;
                    }
                    let head = if ints {
                        table.head_int(int_key_at(&kv, li))
                    } else {
                        buf.clear();
                        kv.encode_into(&mut buf, li);
                        table.head_bytes(&buf)
                    };
                    let Some(mut node) = head else { continue };
                    while node != NIL {
                        count += 1;
                        node = table.node(node).2;
                    }
                }
                Ok(count)
            })
            .collect::<Result<Vec<i64>>>()?
            .into_iter()
            .sum();
        let mut vec = Vector::new_empty(DataType::Bigint);
        vec.push_valid(Value::Bigint(total))?;
        let chunks = vec![DataChunk::new(vec![vec])?];
        let (names, types) = agg_output_schema(keys, aggs);
        return Ok(Some(Output { chunks, names, types }));
    }

    // Pre-evaluate every right-side key/arg expression once per right chunk
    // (shared by all left chunks).
    let right_vecs: Vec<Vec<FusedVec>> = r
        .chunks
        .par_iter()
        .map(|chunk| {
            right_exprs
                .iter()
                .map(|e| eval_fused_vec(e, chunk, cx))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<_>>()?;

    // Probe + accumulate, parallel per left chunk: each match feeds one row
    // into its group's partial. A partition can cover arbitrarily many
    // joined rows, so groups are created on first sight exactly like
    // `aggregate()` does per chunk.
    let partials: Vec<Partial> = l
        .chunks
        .par_iter()
        .map(|chunk| -> Result<Partial> {
            let left_vecs: Vec<FusedVec> = left_exprs
                .iter()
                .map(|e| eval_fused_vec(e, chunk, cx))
                .collect::<Result<_>>()?;
            let kv = eval_join_key(cx, left_key, chunk, common, ints)?;
            let mut partial = Partial {
                groups: Vec::new(),
                index: FastMap::with_capacity_and_hasher(
                    local_index_capacity(chunk.num_rows()),
                    Default::default(),
                ),
            };
            let mut buf = Vec::new();
            let mut full_reserved = false;
            for li in 0..chunk.num_rows() {
                if kv.is_null(li) {
                    continue;
                }
                let head = if ints {
                    table.head_int(int_key_at(&kv, li))
                } else {
                    buf.clear();
                    kv.encode_into(&mut buf, li);
                    table.head_bytes(&buf)
                };
                let Some(mut node) = head else { continue };
                while node != NIL {
                    let (rci, ri, next) = table.node(node);
                    let (rci, ri) = (rci as usize, ri as usize);
                    // Group key encoding byte-identical to `aggregate()`:
                    // the same source vectors, indexed by the left row for
                    // left/constant keys and the right row for right keys.
                    buf.clear();
                    for src in &key_srcs {
                        match src {
                            ExprSource::Left(s) => {
                                left_vecs[*s].v().encode_into(&mut buf, li)
                            }
                            ExprSource::Right(s) => {
                                right_vecs[rci][*s].v().encode_into(&mut buf, ri)
                            }
                        }
                    }
                    let idx = match partial.index.get(&buf) {
                        Some(&idx) => idx,
                        None => {
                            let enc = buf.clone();
                            let idx = partial.groups.len();
                            if !full_reserved
                                && partial.index.len() >= HIGH_CARDINALITY_KEYS
                            {
                                partial
                                    .index
                                    .reserve(chunk.num_rows().saturating_sub(partial.index.len()));
                                full_reserved = true;
                            }
                            partial.index.insert(enc.clone(), idx);
                            partial.groups.push((
                                enc,
                                Group {
                                    keys: key_srcs
                                        .iter()
                                        .map(|src| match src {
                                            ExprSource::Left(s) => {
                                                left_vecs[*s].v().get(li)
                                            }
                                            ExprSource::Right(s) => {
                                                right_vecs[rci][*s].v().get(ri)
                                            }
                                        })
                                        .collect(),
                                    accs: aggs.iter().map(|a| Acc::new(a.func)).collect(),
                                },
                            ));
                            idx
                        }
                    };
                    let group = &mut partial.groups[idx].1;
                    for (acc, src) in group.accs.iter_mut().zip(arg_srcs.iter()) {
                        match src {
                            // COUNT(*): every row counts.
                            None => acc.count_star(),
                            // Typed read of the cell (NULL skips), the same
                            // accumulator update the materialized path makes.
                            Some(ExprSource::Left(s)) => {
                                acc.update_vec(left_vecs[*s].v(), li)
                            }
                            Some(ExprSource::Right(s)) => {
                                acc.update_vec(right_vecs[rci][*s].v(), ri)
                            }
                        }
                    }
                    node = next;
                }
            }
            Ok(partial)
        })
        .collect::<Result<_>>()?;

    // Merge in left-chunk order and emit through the shared post-partial
    // phase, exactly like `aggregate()`.
    let groups = merge_partials(partials, !keys.is_empty(), aggs);
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    let chunks = emit_groups(groups, &key_types, aggs)?;
    let (names, types) = agg_output_schema(keys, aggs);
    Ok(Some(Output { chunks, names, types }))
}

fn distinct(chunks: &[DataChunk]) -> Result<Vec<DataChunk>> {
    let mut seen = FastSet::default();
    let mut out: Vec<Vector> = Vec::new();
    let mut out_rows = 0usize;
    let mut finished: Vec<DataChunk> = Vec::new();
    let mut buf = Vec::new();
    for chunk in chunks.iter() {
        for i in 0..chunk.num_rows() {
            // Borrowed encoding, get-first: hits (dupes) allocate nothing.
            buf.clear();
            for col in chunk.columns.iter() {
                col.encode_into(&mut buf, i);
            }
            if seen.contains(&buf) {
                continue;
            }
            seen.insert(buf.clone());
            if out.is_empty() {
                out = chunk.columns.iter().map(|c| Vector::with_capacity(c.dtype(), CHUNK_SIZE)).collect();
            }
            if out_rows >= CHUNK_SIZE {
                finished.push(DataChunk::new(std::mem::take(&mut out))?);
                out = chunk.columns.iter().map(|c| Vector::with_capacity(c.dtype(), CHUNK_SIZE)).collect();
                out_rows = 0;
            }
            for (col, src) in out.iter_mut().zip(chunk.columns.iter()) {
                col.push_value(src.get(i))?;
            }
            out_rows += 1;
        }
    }
    if !out.is_empty() {
        finished.push(DataChunk::new(out)?);
    }
    Ok(finished)
}

// ---------- sort ----------

/// One sort's order-preserving key encoding: every input row's keys packed
/// into a single byte arena; `offsets[i]..offsets[i + 1]` is row `i`'s slice.
struct SortArena {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl SortArena {
    /// Row `row`'s encoded key bytes.
    #[inline]
    fn key(&self, row: u32) -> &[u8] {
        let row = row as usize;
        &self.bytes[self.offsets[row] as usize..self.offsets[row + 1] as usize]
    }
}

/// A top-N candidate: encoded key bytes + global row index. Row indices are
/// unique, so `(key, row)` is a *total* order — the heap keeps exactly the
/// right `limit` rows regardless of heap-internal tie shuffling.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Candidate<'a> {
    key: &'a [u8],
    row: u32,
}

impl Ord for Candidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(other.key).then(self.row.cmp(&other.row))
    }
}

impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Evaluate every sort key over every chunk in parallel, preserving chunk
/// order (the same per-chunk assembly the old `Row` build used). A bare
/// column key borrows its chunk's vector instead of deep-copying it.
fn eval_sort_keys<'a>(
    cx: &Cx,
    chunks: &'a [DataChunk],
    keys: &[(BoundExpr, bool)],
) -> Result<Vec<Vec<FusedVec<'a>>>> {
    chunks
        .par_iter()
        .map(|chunk| {
            keys.iter()
                .map(|(k, _)| eval_fused_vec(k, chunk, cx))
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

/// Push the low `width` bytes of `x` big-endian, each byte inverted when
/// `desc` (ascending byte order then means descending value order).
#[inline]
fn push_be(out: &mut Vec<u8>, x: u64, width: usize, desc: bool) {
    let be = x.to_be_bytes();
    let be = &be[8 - width..];
    if desc {
        out.extend(be.iter().map(|b| !b));
    } else {
        out.extend_from_slice(be);
    }
}

/// Append one key cell to `out` so ascending byte comparison of two rows'
/// concatenated encodings reproduces the `Value` comparator (reversed for
/// DESC keys), with NULLS LAST in both directions:
///
/// - NULL → single `0xFF` (after every non-null);
/// - otherwise tag `0x00` + payload; DESC inverts payload bytes only.
///
/// Payloads are order-preserving and, per key, prefix-free — a prefix
/// relation inside one key must never let the *next* key decide a comparison
/// the earlier key already settled:
/// - BOOLEAN: `0`/`1`;
/// - INTEGER/DATE, BIGINT/TIMESTAMP: big-endian two's complement with the
///   sign bit flipped (`x ^ MIN`), so unsigned byte order = signed order;
/// - DOUBLE: sign-magnitude → total-order bits (negatives inverted,
///   positives get the sign bit); −0.0 folds onto +0.0 because
///   `compare_values`'s `partial_cmp` calls them Equal;
/// - VARCHAR: each UTF-8 byte shifted up one (bytes are ≤ 0xF4, so `0x00`
///   is free) then a `0x00` terminator. The shift preserves order and the
///   terminator makes the encoding prefix-free ("ab" must beat "a" DESC).
///   DESC: inverted shifted bytes + inverted terminator `0xFF`.
///
/// Returns `false` for a NaN DOUBLE cell: `compare_values` treats NaN as
/// Equal to everything, which no byte order can express, so the caller
/// aborts encoding and sorts with the `Value` comparator instead.
fn encode_key_into(v: &Vector, idx: usize, desc: bool, out: &mut Vec<u8>) -> bool {
    if v.is_null(idx) {
        out.push(0xFF);
        return true;
    }
    out.push(0x00);
    match v.data() {
        VectorData::Boolean(d) => {
            let b = d[idx] as u8;
            out.push(if desc { !b } else { b });
        }
        VectorData::Int32(d) => push_be(out, ((d[idx] ^ i32::MIN) as u32) as u64, 4, desc),
        VectorData::Int64(d) => push_be(out, (d[idx] ^ i64::MIN) as u64, 8, desc),
        VectorData::Float64(d) => {
            let x = d[idx];
            if x.is_nan() {
                return false;
            }
            let x = if x == 0.0 { 0.0 } else { x }; // −0.0 == +0.0 in compare_values
            let b = x.to_bits();
            let b = if b & (1u64 << 63) != 0 { !b } else { b | (1u64 << 63) };
            push_be(out, b, 8, desc);
        }
        VectorData::VarChar(d) => {
            let s: &str = &d[idx];
            if desc {
                out.extend(s.bytes().map(|b| !(b + 1)));
                out.push(0xFF);
            } else {
                out.extend(s.bytes().map(|b| b + 1));
                out.push(0x00);
            }
        }
        VectorData::Date32(d) => push_be(out, ((d[idx] ^ i32::MIN) as u32) as u64, 4, desc),
        VectorData::TimestampMicros(d) => push_be(out, (d[idx] ^ i64::MIN) as u64, 8, desc),
    }
    true
}

/// Pack all rows' keys into one arena. `None` ⇒ the byte order cannot
/// reproduce `compare_values` exactly (NaN in a FLOAT key, dtypes differing
/// across chunks, or sizes past `u32`) and the caller must use the `Value`
/// comparator path.
fn encode_sort_keys(
    chunks: &[DataChunk],
    keyed: &[Vec<FusedVec>],
    keys: &[(BoundExpr, bool)],
) -> Option<SortArena> {
    let n: usize = chunks.iter().map(|c| c.num_rows()).sum();
    if n > u32::MAX as usize {
        return None;
    }
    // Key vectors are typed by binding, so one dtype per key across chunks;
    // anything else would make byte order diverge from `compare_values`.
    let mut dtypes: Vec<DataType> = Vec::with_capacity(keys.len());
    for ks in keyed {
        for (ki, v) in ks.iter().enumerate() {
            match dtypes.get(ki) {
                None => dtypes.push(v.v().dtype()),
                Some(t) if *t == v.v().dtype() => {}
                Some(_) => return None,
            }
        }
    }
    // Encode chunks in parallel into per-chunk buffers, then concatenate in
    // chunk order (offsets shifted by each chunk's base), so the arena stays
    // a single contiguous byte range.
    let encoded: Vec<Option<(Vec<u8>, Vec<u32>)>> = chunks
        .par_iter()
        .zip(keyed.par_iter())
        .map(|(chunk, ks)| {
            let mut bytes = Vec::new();
            let mut offsets = Vec::with_capacity(chunk.num_rows());
            for i in 0..chunk.num_rows() {
                if bytes.len() > u32::MAX as usize {
                    return None;
                }
                offsets.push(bytes.len() as u32);
                for (ki, (_, desc)) in keys.iter().enumerate() {
                    if !encode_key_into(ks[ki].v(), i, *desc, &mut bytes) {
                        return None;
                    }
                }
            }
            Some((bytes, offsets))
        })
        .collect();
    let mut arena = SortArena { bytes: Vec::new(), offsets: Vec::with_capacity(n + 1) };
    for part in encoded {
        let (bytes, offsets) = part?;
        if arena.bytes.len() + bytes.len() > u32::MAX as usize {
            return None;
        }
        let base = arena.bytes.len() as u32;
        arena.offsets.extend(offsets.iter().map(|o| o + base));
        arena.bytes.extend_from_slice(&bytes);
    }
    arena.offsets.push(arena.bytes.len() as u32);
    Some(arena)
}

/// Global index of each chunk's first row, plus the total row count at the
/// end (so `partition_point` maps a global row → its chunk).
fn chunk_starts(chunks: &[DataChunk]) -> Vec<usize> {
    let mut starts = Vec::with_capacity(chunks.len() + 1);
    starts.push(0);
    for c in chunks {
        starts.push(starts[starts.len() - 1] + c.num_rows());
    }
    starts
}

/// Rebuild output chunks from global row indices in sorted order, sharing
/// string `Arc`s via [`Vector::push_from`] (no per-cell `Value`).
fn rebuild_sorted(chunks: &[DataChunk], rows: &[u32]) -> Result<Vec<DataChunk>> {
    let dtypes: Vec<DataType> = chunks[0].columns.iter().map(|c| c.dtype()).collect();
    let starts = chunk_starts(chunks);
    // Sorted rows land in arbitrary chunks. For many output rows, one flat
    // row → chunk map beats a binary search per row; small top-N outputs
    // just search the (tiny) `starts` array.
    let total = starts[starts.len() - 1];
    let row_chunk: Option<Vec<u32>> = (rows.len() > 64 && rows.len() * 4 > total).then(|| {
        let mut map = Vec::with_capacity(total);
        for (ci, c) in chunks.iter().enumerate() {
            map.resize(map.len() + c.num_rows(), ci as u32);
        }
        map
    });
    let mut finished = Vec::new();
    let mut cur: Vec<Vector> = dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
    let mut cur_rows = 0;
    for &g in rows {
        if cur_rows >= CHUNK_SIZE {
            finished.push(DataChunk::new(std::mem::take(&mut cur))?);
            cur = dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
            cur_rows = 0;
        }
        let g = g as usize;
        let ci = match &row_chunk {
            Some(map) => map[g] as usize,
            None => starts.partition_point(|&s| s <= g) - 1,
        };
        let src = &chunks[ci];
        let r = g - starts[ci];
        for (dst, col) in cur.iter_mut().zip(src.columns.iter()) {
            dst.push_from(col, r)?;
        }
        cur_rows += 1;
    }
    if cur_rows > 0 {
        finished.push(DataChunk::new(cur)?);
    }
    Ok(finished)
}

fn sort(cx: &Cx, chunks: &[DataChunk], keys: &[(BoundExpr, bool)]) -> Result<Vec<DataChunk>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    // Encode each row's keys once into a byte arena, then sort row indices by
    // (arena bytes, row index). That comparator is a total order, so the
    // unstable parallel sort is deterministic; the row index is the old
    // `seq` tiebreak (input order), preserving tie stability.
    let keyed = eval_sort_keys(cx, chunks, keys)?;
    let Some(arena) = encode_sort_keys(chunks, &keyed, keys) else {
        return sort_legacy(chunks, keys, &keyed);
    };
    let n = arena.offsets.len() - 1;
    let mut rows: Vec<u32> = (0..n as u32).collect();
    rows.par_sort_unstable_by(|&a, &b| arena.key(a).cmp(arena.key(b)).then(a.cmp(&b)));
    rebuild_sorted(chunks, &rows)
}

/// Top-N selection for `LIMIT` directly over `SORT`: every rayon partition
/// keeps a bounded max-heap of its `limit` smallest rows under the byte
/// comparator; the ≤ partitions × limit candidates merge to the global first
/// `limit`. `Ok(None)` ⇒ encoding unavailable (NaN float key); the caller
/// then falls back to a full sort.
fn top_n(
    cx: &Cx,
    chunks: &[DataChunk],
    keys: &[(BoundExpr, bool)],
    limit: usize,
) -> Result<Option<Vec<DataChunk>>> {
    let keyed = eval_sort_keys(cx, chunks, keys)?;
    let Some(arena) = encode_sort_keys(chunks, &keyed, keys) else {
        return Ok(None);
    };
    let n = arena.offsets.len() - 1;
    debug_assert!(0 < limit && limit < n, "top-N needs a truncating limit");
    // Cap the partition count so the merged candidate list stays a small
    // fraction of n: each partition returns ≤ limit candidates, and the
    // caller only routes here when limit × 16 ≤ n.
    let parts = rayon::current_num_threads()
        .saturating_mul(8)
        .min((n / limit.saturating_mul(8)).max(1))
        .max(1);
    let part_size = n.div_ceil(parts);
    let indices: Vec<u32> = (0..n as u32).collect();
    let candidates: Vec<Vec<Candidate>> = indices
        .par_chunks(part_size)
        .map(|part| {
            let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(limit + 1);
            for &row in part {
                let cand = Candidate { key: arena.key(row), row };
                if heap.len() < limit {
                    heap.push(cand);
                } else if let Some(&max) = heap.peek() {
                    if cand < max {
                        heap.pop();
                        heap.push(cand);
                    }
                }
            }
            heap.into_vec()
        })
        .collect();
    let mut merged: Vec<Candidate> = candidates.into_iter().flatten().collect();
    merged.sort_unstable(); // (key, row) is total, so the merge is deterministic
    merged.truncate(limit);
    let rows: Vec<u32> = merged.iter().map(|c| c.row).collect();
    let mut out = rebuild_sorted(chunks, &rows)?;
    // The generic LIMIT shape gathers a straddling final chunk; do the same
    // so the top-N output equals full-sort-then-truncate exactly (validity
    // mask padding included).
    if let Some(last) = out.last_mut() {
        if last.num_rows() < CHUNK_SIZE {
            let sel: Vec<usize> = (0..last.num_rows()).collect();
            *last = last.gather(&sel);
        }
    }
    Ok(Some(out))
}

/// Legacy fallback: materialize `Value` keys and compare with
/// [`compare_values`] (NULLS LAST, `seq` tiebreak), exactly as before the
/// byte-arena path. Reached when a FLOAT key contains NaN.
fn sort_legacy(
    chunks: &[DataChunk],
    keys: &[(BoundExpr, bool)],
    keyed: &[Vec<FusedVec>],
) -> Result<Vec<DataChunk>> {
    struct Row {
        key: Vec<Value>,
        chunk: usize,
        row: usize,
        seq: usize,
    }
    let mut rows: Vec<Row> = Vec::new();
    for (ci, chunk) in chunks.iter().enumerate() {
        for i in 0..chunk.num_rows() {
            let seq = rows.len();
            rows.push(Row {
                key: keyed[ci].iter().map(|v| v.v().get(i)).collect(),
                chunk: ci,
                row: i,
                seq,
            });
        }
    }
    rows.par_sort_by(|a, b| {
        for ((ka, kb), (_, desc)) in a.key.iter().zip(b.key.iter()).zip(keys.iter()) {
            // NULLS LAST regardless of direction: only non-null values flip.
            let ord = match (ka, kb) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                _ => {
                    let c = compare_values(ka, kb);
                    if *desc { c.reverse() } else { c }
                }
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        // Stability tiebreak: input order (par_sort is not stable).
        a.seq.cmp(&b.seq)
    });
    // Rebuild columns in sorted order.
    let dtypes: Vec<DataType> = chunks[0].columns.iter().map(|c| c.dtype()).collect();
    let mut finished = Vec::new();
    let mut cur: Vec<Vector> = dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
    let mut cur_rows = 0;
    for r in rows {
        if cur_rows >= CHUNK_SIZE {
            finished.push(DataChunk::new(std::mem::take(&mut cur))?);
            cur = dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
            cur_rows = 0;
        }
        let src = &chunks[r.chunk];
        for (dst, col) in cur.iter_mut().zip(src.columns.iter()) {
            dst.push_value(col.get(r.row))?;
        }
        cur_rows += 1;
    }
    if cur_rows > 0 {
        finished.push(DataChunk::new(cur)?);
    }
    Ok(finished)
}

/// Keep only the first `limit` rows of `out`, chunk by chunk (whole chunks
/// pass through; a straddling chunk is gathered) — the generic LIMIT shape.
fn truncate_output(mut out: Output, limit: usize) -> Output {
    let mut chunks = Vec::new();
    let mut left = limit;
    for chunk in out.chunks.drain(..) {
        if left == 0 {
            break;
        }
        if chunk.num_rows() <= left {
            left -= chunk.num_rows();
            chunks.push(chunk);
        } else {
            let sel: Vec<usize> = (0..left).collect();
            chunks.push(chunk.gather(&sel));
            left = 0;
        }
    }
    Output { chunks, names: out.names, types: out.types }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value as V;

    // Shared-nothing columnar types must cross threads (rayon + RwLock).
    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn core_types_are_send_sync() {
        assert_send_sync::<PinkDb>();
        assert_send_sync::<DataTable>();
        assert_send_sync::<Vector>();
        assert_send_sync::<DataChunk>();
        assert_send_sync::<Value>();
        assert_send_sync::<QueryResult>();
    }

    fn sales_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(
            &mut db,
            "sales",
            "id,region,total\n1,west,10.5\n2,east,7.0\n3,west,3.25\n4,east,\n",
        )
        .unwrap();
        db
    }

    fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<V>> {
        let mut last = None;
        for r in db.execute(sql).unwrap() {
            if let QueryResult::Selected { rows, .. } = r {
                last = Some(rows);
            }
        }
        last.unwrap()
    }

    #[test]
    fn integer_overflow_errors_like_duckdb() {
        let mut db = PinkDb::new();
        let err = |db: &mut PinkDb, sql: &str| -> String {
            db.execute(sql).expect_err(sql).to_string()
        };
        assert!(err(&mut db, "SELECT 2147483647 + 1;").contains("Overflow in addition of INTEGER"));
        assert!(err(&mut db, "SELECT -2147483647 - 2;").contains("Overflow in subtraction"));
        assert!(err(&mut db, "SELECT 9223372036854775807 * 2;").contains("multiplication of BIGINT"));
        assert!(err(&mut db, "SELECT abs(CAST(-2147483648 AS INTEGER));").contains("Overflow in abs"));
        assert!(err(&mut db, "SELECT CAST(3000000000 AS INTEGER);").contains("out of range"));
        assert!(err(&mut db, "SELECT CAST(1e300 AS BIGINT);").contains("out of range"));
        assert!(err(&mut db, "SELECT DATE '2024-01-01' + 2147483647;").contains("date out of range"));
        // In range stays exact; float→int rounds half away from zero.
        assert_eq!(select(&mut db, "SELECT 2147483646 + 1;"), vec![vec![V::Integer(i32::MAX)]]);
        assert_eq!(
            select(&mut db, "SELECT CAST(2.5 AS INTEGER), CAST(-2.5 AS INTEGER), CAST(2.4 AS BIGINT);"),
            vec![vec![V::Integer(3), V::Integer(-3), V::Bigint(2)]]
        );
        // Column data takes the same checked kernels as literals.
        db.execute("CREATE TABLE o (x INTEGER, y BIGINT);").unwrap();
        db.execute("INSERT INTO o VALUES (2147483647, 9223372036854775807), (1, 1);").unwrap();
        assert!(err(&mut db, "SELECT x + 1 FROM o;").contains("Overflow in addition of INTEGER"));
        assert!(err(&mut db, "SELECT -x - 2 FROM o;").contains("Overflow"));
        // SUM accumulates wide: only a final result outside BIGINT errors…
        assert!(err(&mut db, "SELECT SUM(y) FROM o;").contains("Overflow in SUM"));
        // …and an intermediate excursion that comes back in range is exact.
        db.execute("INSERT INTO o VALUES (0, -9223372036854775807);").unwrap();
        assert_eq!(select(&mut db, "SELECT SUM(y) FROM o;"), vec![vec![V::Bigint(1)]]);
        // SUM(INTEGER) never overflows BIGINT at realistic row counts.
        assert_eq!(
            select(&mut db, "SELECT SUM(x) FROM o;"),
            vec![vec![V::Bigint(2147483648)]]
        );
    }

    #[test]
    fn filter_project_limit() {
        let mut db = sales_db();
        let rows = select(&mut db, "SELECT id FROM sales WHERE total > 5 LIMIT 10;");
        assert_eq!(rows, vec![vec![V::Integer(1)], vec![V::Integer(2)]]);
    }

    #[test]
    fn null_fails_where() {
        let mut db = sales_db();
        // id=4 has NULL total: `total > 0` is NULL → filtered; IS NULL keeps it.
        let rows = select(&mut db, "SELECT id FROM sales WHERE total > 0;");
        assert_eq!(rows.len(), 3);
        let rows = select(&mut db, "SELECT id FROM sales WHERE total IS NULL;");
        assert_eq!(rows, vec![vec![V::Integer(4)]]);
    }

    #[test]
    fn group_avg_having_order() {
        let mut db = sales_db();
        let rows = select(
            &mut db,
            "SELECT region, AVG(total) AS a FROM sales GROUP BY region HAVING COUNT(*) > 1 ORDER BY 2 DESC;",
        );
        assert_eq!(rows.len(), 2);
        // east avg = 7.0 (NULL skipped), west avg = (10.5+3.25)/2 = 6.875.
        assert_eq!(rows[0][0], V::Varchar("east".to_string()));
        assert!(matches!(rows[0][1], V::Double(_)));
        match rows[0][1] {
            V::Double(x) => assert!((x - 7.0).abs() < 1e-9),
            _ => panic!(),
        }
        match rows[1][1] {
            V::Double(x) => assert!((x - 6.875).abs() < 1e-9),
            _ => panic!(),
        }
    }

    /// Group output order is first-seen (no ORDER BY) and must survive the
    /// multi-chunk partial merge; NULL keys group into one group.
    #[test]
    fn group_first_seen_order_across_chunks_and_null_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE g (k INTEGER, v INTEGER);").unwrap();
        // 3000 rows = 2 chunks; key cycles 0,7,1,8,2,9,3,10,4,11,5,12,6.
        insert_rows(&mut db, "g", 3000, |n| format!("({}, {n})", (n * 7) % 13));
        let rows = select(&mut db, "SELECT k, COUNT(*) FROM g GROUP BY k;");
        let expected_order = [0, 7, 1, 8, 2, 9, 3, 10, 4, 11, 5, 12, 6];
        assert_eq!(rows.len(), expected_order.len());
        for (i, (row, key)) in rows.iter().zip(expected_order).enumerate() {
            assert_eq!(row[0], V::Integer(key), "group {i}");
            // 3000 = 13 × 230 + 10: the first 10 cycle keys occur 231×.
            assert_eq!(row[1], V::Bigint(if i < 10 { 231 } else { 230 }), "group {i}");
        }
        // NULL keys are one group, in first-seen position.
        db.execute("CREATE TABLE nn (k INTEGER);").unwrap();
        db.execute("INSERT INTO nn VALUES (1), (NULL), (2), (NULL);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT k, COUNT(*) FROM nn GROUP BY k;"),
            vec![
                vec![V::Integer(1), V::Bigint(1)],
                vec![V::Null, V::Bigint(2)],
                vec![V::Integer(2), V::Bigint(1)],
            ]
        );
    }

    #[test]
    fn count_star_over_empty() {
        let mut db = sales_db();
        let rows = select(&mut db, "SELECT COUNT(*) FROM sales WHERE false;");
        assert_eq!(rows, vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn distinct_and_order_nulls_last() {        let mut db = sales_db();
        let rows = select(&mut db, "SELECT DISTINCT region FROM sales ORDER BY region;");
        assert_eq!(
            rows,
            vec![vec![V::Varchar("east".to_string())], vec![V::Varchar("west".to_string())]]
        );
        let rows = select(&mut db, "SELECT total FROM sales ORDER BY total;");
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
        // NULLS LAST holds for DESC too.
        let rows = select(&mut db, "SELECT total FROM sales ORDER BY total DESC;");
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
        assert!(matches!(rows[0][0], V::Double(_)));
    }

    #[test]
    fn scalar_functions_and_coercion() {
        let mut db = sales_db();
        let rows = select(&mut db, "SELECT 1 / 2, upper(region), length(region) FROM sales LIMIT 1;");
        assert_eq!(rows[0][0], V::Double(0.5));
        assert_eq!(rows[0][1], V::Varchar("WEST".to_string()));
        assert_eq!(rows[0][2], V::Integer(4));
    }

    #[test]
    fn create_insert_select_roundtrip() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a BIGINT, b VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'yy');").unwrap();
        let rows = select(&mut db, "SELECT a, b FROM t ORDER BY a DESC;");
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(2), V::Varchar("yy".to_string())],
                vec![V::Bigint(1), V::Varchar("x".to_string())],
            ]
        );
    }

    #[test]
    fn select_without_from_returns_one_row() {
        let mut db = PinkDb::new();
        let rows = select(&mut db, "SELECT 1 + 2, upper('ab');");
        assert_eq!(
            rows,
            vec![vec![V::Integer(3), V::Varchar("AB".to_string())]]
        );
    }

    #[test]
    fn div_by_zero_errors() {
        let mut db = sales_db();
        assert!(db.execute("SELECT id / 0 FROM sales;").is_err());
    }

    // --- wave-4 review regressions ---

    #[test]
    fn constant_fragments_in_agg_queries() {
        // C1: non-literal constants must not panic the key rebasing.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
        assert_eq!(select(&mut db, "SELECT SUM(x) + (1+1) FROM t;"), vec![vec![V::Bigint(5)]]);
        assert_eq!(
            select(&mut db, "SELECT 1+1, SUM(x) FROM t;"),
            vec![vec![V::Integer(2), V::Bigint(3)]]
        );
        assert_eq!(select(&mut db, "SELECT SUM(x) + abs(1) FROM t;"), vec![vec![V::Bigint(4)]]);
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM t HAVING SUM(x) > 1+1;"), vec![vec![V::Bigint(3)]]);
    }

    #[test]
    fn alias_self_reference_resolves_to_column() {
        // C2: `SELECT x AS x` must not overflow the stack.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(select(&mut db, "SELECT x AS x FROM t;"), vec![vec![V::Integer(1)]]);
    }

    #[test]
    fn where_ignores_select_aliases() {
        // M1: WHERE binds table columns, never select aliases.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE u (a INTEGER, b INTEGER);").unwrap();
        db.execute("INSERT INTO u VALUES (1, 100), (2, 0);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT a AS b FROM u WHERE b > 50;"),
            vec![vec![V::Integer(1)]]
        );
    }

    #[test]
    fn sum_of_nothing_is_null() {
        // C3: SUM over zero/all-NULL rows is NULL, not 0.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE e (x INTEGER);").unwrap();
        db.execute("INSERT INTO e VALUES (NULL), (NULL);").unwrap();
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e WHERE false;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT SUM(x + 1) FROM e;"), vec![vec![V::Null]]);
        // Non-empty sums still work.
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e WHERE x IS NULL;"), vec![vec![V::Null]]);
    }

    /// Regression: a fresh table scans as one 0-row chunk, so with no GROUP
    /// BY every per-chunk partial has zero groups — the keys-empty merge fast
    /// path used to `expect` a group and panic. The single empty group must
    /// come from the fallback instead.
    #[test]
    fn global_aggregates_over_truly_empty_table() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE e (x INTEGER);").unwrap();
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT AVG(x) FROM e;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT MIN(x) FROM e;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT MAX(x) FROM e;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT COUNT(x) FROM e;"), vec![vec![V::Bigint(0)]]);
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM e;"), vec![vec![V::Bigint(0)]]);
        // Same path once the 0-row chunk comes from a derived table: the
        // inner grouped aggregate emits no groups over the empty table.
        assert_eq!(
            select(&mut db, "SELECT SUM(c) FROM (SELECT x, COUNT(*) AS c FROM e GROUP BY x) s;"),
            vec![vec![V::Null]]
        );
    }

    #[test]
    fn cross_type_join_matches() {
        // C4: INTEGER = BIGINT and INTEGER = DOUBLE join keys must match.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a (x INTEGER);").unwrap();
        db.execute("CREATE TABLE b (x BIGINT);").unwrap();
        db.execute("CREATE TABLE c (x DOUBLE);").unwrap();
        db.execute("INSERT INTO a VALUES (1), (2);").unwrap();
        db.execute("INSERT INTO b VALUES (1), (3);").unwrap();
        db.execute("INSERT INTO c VALUES (1.0), (4.0);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT * FROM a JOIN b ON a.x = b.x;"),
            vec![vec![V::Integer(1), V::Bigint(1)]]
        );
        assert_eq!(
            select(&mut db, "SELECT * FROM a JOIN c ON a.x = c.x;"),
            vec![vec![V::Integer(1), V::Double(1.0)]]
        );
    }

    /// Regression: FLOAT keys are encoded by `Vector::encode_into` for
    /// GROUP BY / DISTINCT / JOIN. Raw bits made `-0.0` and `0.0` distinct
    /// keys and each NaN payload its own key, disagreeing with `=`.
    #[test]
    fn float_key_encoding_matches_equality() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE d (v DOUBLE);").unwrap();
        db.execute("INSERT INTO d VALUES (-0.0), (0.0), (1.0);").unwrap();
        let rows = select(&mut db, "SELECT v, COUNT(*) FROM d GROUP BY v;");
        assert_eq!(rows.len(), 2);
        let zero = rows
            .iter()
            .find(|r| matches!(r[0], V::Double(x) if x == 0.0))
            .expect("zero group");
        assert_eq!(zero[1], V::Bigint(2));
        let one = rows
            .iter()
            .find(|r| matches!(r[0], V::Double(x) if x == 1.0))
            .expect("one group");
        assert_eq!(one[1], V::Bigint(1));
        assert_eq!(select(&mut db, "SELECT DISTINCT v FROM d;").len(), 2);
        // Self-join: each of the two zero rows matches both zero rows (2
        // matches per zero row), and 1.0 matches itself → 5 total.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM d a JOIN d b ON a.v = b.v;"),
            vec![vec![V::Bigint(5)]]
        );
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM d a JOIN d b ON a.v = b.v WHERE a.v = 0.0;"),
            vec![vec![V::Bigint(4)]]
        );
        // Different NaN payloads are one DISTINCT row.
        db.execute("CREATE TABLE n (v DOUBLE);").unwrap();
        db.execute("INSERT INTO n VALUES (0.0 / 0.0), (-0.0 / 0.0);").unwrap();
        assert_eq!(select(&mut db, "SELECT DISTINCT v FROM n;").len(), 1);
    }

    /// One key can expand past CHUNK_SIZE (here 5000 right rows across
    /// several right chunks); the fused probe/assembly path must emit
    /// multiple chunks and keep right-row order.
    #[test]
    fn join_match_expansion_spans_chunks_in_order() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t1 (id INTEGER);").unwrap();
        db.execute("CREATE TABLE t2 (id INTEGER, rn INTEGER);").unwrap();
        db.execute("INSERT INTO t1 VALUES (1);").unwrap();
        let mut sql = String::from("INSERT INTO t2 VALUES");
        for rn in 0..5000 {
            if rn > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(" (1, {rn})"));
        }
        sql.push(';');
        db.execute(&sql).unwrap();
        let rows = select(&mut db, "SELECT rn FROM t1 JOIN t2 ON t1.id = t2.id;");
        assert_eq!(rows.len(), 5000);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], V::Integer(i as i32), "row {i}");
        }
    }

    /// Duplicate keys interleaved with other keys on either side must match
    /// exactly and keep right-row order per key (flat-arena bookkeeping).
    #[test]
    fn join_interleaved_duplicate_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv VARCHAR);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv VARCHAR);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 'a'), (2, 'b'), (1, 'c');").unwrap();
        db.execute("INSERT INTO r VALUES (1, 'x'), (2, 'y'), (1, 'z');").unwrap();
        let rows = select(&mut db, "SELECT lv, rv FROM l JOIN r ON l.k = r.k;");
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("a".to_string()), V::Varchar("x".to_string())],
                vec![V::Varchar("a".to_string()), V::Varchar("z".to_string())],
                vec![V::Varchar("b".to_string()), V::Varchar("y".to_string())],
                vec![V::Varchar("c".to_string()), V::Varchar("x".to_string())],
                vec![V::Varchar("c".to_string()), V::Varchar("z".to_string())],
            ]
        );
    }

    /// Join key breadth: INTEGER vs BIGINT and INTEGER vs DOUBLE across
    /// chunks, string keys with duplicates, and NULL keys on both sides
    /// (never match).
    #[test]
    fn join_cross_type_string_and_null_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ig (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE bg (k BIGINT);").unwrap();
        db.execute("CREATE TABLE dg (k DOUBLE);").unwrap();
        db.execute("CREATE TABLE sv (k VARCHAR, rn INTEGER);").unwrap();
        db.execute("CREATE TABLE s (k VARCHAR);").unwrap();
        // 100 rows per side: k cycles 0..3 (25 rows each).
        insert_rows(&mut db, "ig", 100, |n| format!("({}, {n})", n % 4));
        insert_rows(&mut db, "bg", 100, |n| format!("({})", n % 4));
        insert_rows(&mut db, "dg", 100, |n| format!("({}.0)", n % 4));
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM ig JOIN bg ON ig.k = bg.k;"),
            vec![vec![V::Bigint(100 * 25)]]
        );
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM ig JOIN dg ON ig.k = dg.k;"),
            vec![vec![V::Bigint(100 * 25)]]
        );
        // String keys: duplicate matches preserve right-input order; NULLs
        // never match.
        db.execute("INSERT INTO sv VALUES ('x', 1), ('y', 2), (NULL, 3);").unwrap();
        db.execute("INSERT INTO s VALUES ('x'), ('x'), ('y'), (NULL);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT rn FROM sv JOIN s ON sv.k = s.k;"),
            vec![
                vec![V::Integer(1)],
                vec![V::Integer(1)],
                vec![V::Integer(2)],
            ]
        );
    }

    /// Multi-left-chunk expansion: 3000 left rows (2 chunks) × 3 right rows
    /// per key; output preserves left-row then right-row order across chunk
    /// boundaries.
    #[test]
    fn join_multi_left_chunk_expansion_preserves_order() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        insert_rows(&mut db, "l", 3000, |n| format!("({}, {n})", n % 2));
        // Right rows in input order per key: rv 0, 1, 2.
        insert_rows(&mut db, "r", 6, |n| format!("({}, {})", n % 2, n / 2));
        let rows = select(&mut db, "SELECT lv, rv FROM l JOIN r ON l.k = r.k;");
        assert_eq!(rows.len(), 9000);
        for (idx, row) in rows.iter().enumerate() {
            let lv = (idx / 3) as i32;
            let rv = (idx % 3) as i32;
            assert_eq!(row, &vec![V::Integer(lv), V::Integer(rv)], "row {idx}");
        }
    }

    // --- wave 19: integer-key specialization in the hash join ---

    /// BIGINT keys with i64 extremes, negatives, duplicates, and NULLs on
    /// both sides; NULL keys never match, duplicates keep right-row order.
    #[test]
    fn join_int_keys_extremes_nulls_and_duplicates() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k BIGINT, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k BIGINT, rv INTEGER);").unwrap();
        db.execute(
            "INSERT INTO l VALUES
             (-9223372036854775807 - 1, 0), (-5, 1), (0, 2), (7, 3), (7, 4),
             (9223372036854775807, 5), (NULL, 6);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO r VALUES
             (7, 10), (-5, 11), (NULL, 12), (9223372036854775807, 13),
             (-9223372036854775807 - 1, 14), (7, 15);",
        )
        .unwrap();
        let rows = select(&mut db, "SELECT lv, rv FROM l JOIN r ON l.k = r.k;");
        assert_eq!(
            rows,
            vec![
                vec![V::Integer(0), V::Integer(14)],
                vec![V::Integer(1), V::Integer(11)],
                vec![V::Integer(3), V::Integer(10)],
                vec![V::Integer(3), V::Integer(15)],
                vec![V::Integer(4), V::Integer(10)],
                vec![V::Integer(4), V::Integer(15)],
                vec![V::Integer(5), V::Integer(13)],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM l JOIN r ON l.k = r.k;"),
            vec![vec![V::Bigint(7)]]
        );
    }

    /// INTEGER = BIGINT keys widen i32 → i64 per cell: a BIGINT outside the
    /// i32 range must not collide with any INTEGER (2147483648 and 4294967297
    /// would truncate to -2147483648 / 1).
    #[test]
    fn join_int_bigint_cross_type_widening() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE b (k BIGINT, rv INTEGER);").unwrap();
        db.execute("INSERT INTO a VALUES (1, 1), (-1, 2), (NULL, 3), (2147483647, 4);")
            .unwrap();
        db.execute(
            "INSERT INTO b VALUES (1, 10), (-1, 11), (2147483648, 12),
             (4294967297, 13), (2147483647, 14);",
        )
        .unwrap();
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM a JOIN b ON a.k = b.k;"),
            vec![
                vec![V::Integer(1), V::Integer(10)],
                vec![V::Integer(2), V::Integer(11)],
                vec![V::Integer(4), V::Integer(14)],
            ]
        );
    }

    /// TIMESTAMP keys take the int path too (i64 payload): matching is exact
    /// at microsecond precision (including pre-epoch), NULLs skipped.
    #[test]
    fn join_timestamp_keys_match_micros() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (ts TIMESTAMP, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (ts TIMESTAMP, rv INTEGER);").unwrap();
        db.execute(
            "INSERT INTO l VALUES
             (TIMESTAMP '2024-01-15 10:00:00', 1),
             (TIMESTAMP '2024-01-15 10:00:00.000001', 2),
             (TIMESTAMP '1969-12-31 23:59:59.999999', 3),
             (NULL, 4);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO r VALUES
             (TIMESTAMP '2024-01-15 10:00:00.000001', 10),
             (TIMESTAMP '2024-01-15 10:00:00', 11),
             (TIMESTAMP '1969-12-31 23:59:59.999999', 12),
             (NULL, 13);",
        )
        .unwrap();
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l JOIN r ON l.ts = r.ts;"),
            vec![
                vec![V::Integer(1), V::Integer(11)],
                vec![V::Integer(2), V::Integer(10)],
                vec![V::Integer(3), V::Integer(12)],
            ]
        );
    }

    /// Empty right side: the match table builds over zero rows and every
    /// probe misses (INTEGER and VARCHAR keys alike).
    #[test]
    fn join_empty_right_side() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k BIGINT);").unwrap();
        db.execute("CREATE TABLE r (k BIGINT);").unwrap();
        db.execute("INSERT INTO l VALUES (1), (NULL), (2);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM l JOIN r ON l.k = r.k;"),
            vec![vec![V::Bigint(0)]]
        );
        db.execute("CREATE TABLE ls (k VARCHAR);").unwrap();
        db.execute("CREATE TABLE rs (k VARCHAR);").unwrap();
        db.execute("INSERT INTO ls VALUES ('a'), (NULL);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM ls JOIN rs ON ls.k = rs.k;"),
            vec![vec![V::Bigint(0)]]
        );
    }

    /// BIGINT expansion past CHUNK_SIZE: one left row matching 2500 right
    /// rows (interleaved with another key across right chunks) emits multiple
    /// output chunks in right-row order.
    #[test]
    fn join_bigint_expansion_spans_chunks() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k BIGINT);").unwrap();
        db.execute("CREATE TABLE r (k BIGINT, rn INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1);").unwrap();
        // Right input alternates keys 1/2: key 1 has 2500 matches (even rn).
        insert_rows(&mut db, "r", 5000, |n| format!("({}, {n})", 1 + n % 2));
        let rows = select(&mut db, "SELECT rn FROM l JOIN r ON l.k = r.k;");
        assert_eq!(rows.len(), 2500);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], V::Integer((2 * i) as i32), "row {i}");
        }
    }

    /// The int-specialized and byte-encoded match tables must produce
    /// identical results over the same data shape (INTEGER vs VARCHAR keys).
    #[test]
    fn join_int_and_byte_paths_agree() {
        fn run(key_type: &str, l: &str, r: &str) -> Vec<Vec<V>> {
            let mut db = PinkDb::new();
            db.execute(&format!("CREATE TABLE l (k {key_type}, lv INTEGER);")).unwrap();
            db.execute(&format!("CREATE TABLE r (k {key_type}, rv INTEGER);")).unwrap();
            db.execute(&format!("INSERT INTO l VALUES {l};")).unwrap();
            db.execute(&format!("INSERT INTO r VALUES {r};")).unwrap();
            select(&mut db, "SELECT lv, rv FROM l JOIN r ON l.k = r.k;")
        }
        let int_rows = run(
            "INTEGER",
            "(1, 0), (2, 1), (1, 2), (NULL, 3), (3, 4), (2, 5)",
            "(2, 10), (1, 11), (3, 12), (1, 13), (NULL, 14), (2, 15)",
        );
        let byte_rows = run(
            "VARCHAR",
            "('1', 0), ('2', 1), ('1', 2), (NULL, 3), ('3', 4), ('2', 5)",
            "('2', 10), ('1', 11), ('3', 12), ('1', 13), (NULL, 14), ('2', 15)",
        );
        assert_eq!(int_rows, byte_rows);
        // Shared structure: left order, right-input order per key, NULL keys
        // skipped on both sides.
        assert_eq!(
            int_rows,
            vec![
                vec![V::Integer(0), V::Integer(11)],
                vec![V::Integer(0), V::Integer(13)],
                vec![V::Integer(1), V::Integer(10)],
                vec![V::Integer(1), V::Integer(15)],
                vec![V::Integer(2), V::Integer(11)],
                vec![V::Integer(2), V::Integer(13)],
                vec![V::Integer(4), V::Integer(12)],
                vec![V::Integer(5), V::Integer(10)],
                vec![V::Integer(5), V::Integer(15)],
            ]
        );
    }

    // --- wave 20: LEFT / RIGHT outer joins ---

    /// Matched pairs, unmatched rows, and NULL keys on both sides. Row order
    /// is deterministic (left-row order for LEFT, right-row order for RIGHT)
    /// and matches DuckDB's output for this data.
    #[test]
    fn outer_join_matched_unmatched_and_null_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);").unwrap();
        db.execute("INSERT INTO r VALUES (1, 100), (2, NULL), (NULL, 300), (5, 500);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT l.k, lv, r.k, rv FROM l LEFT JOIN r ON l.k = r.k;"),
            vec![
                vec![V::Integer(1), V::Integer(10), V::Integer(1), V::Integer(100)],
                vec![V::Integer(2), V::Integer(20), V::Integer(2), V::Null],
                vec![V::Null, V::Integer(30), V::Null, V::Null],
                vec![V::Integer(4), V::Integer(40), V::Null, V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT l.k, lv, r.k, rv FROM l LEFT OUTER JOIN r ON l.k = r.k;"),
            select(&mut db, "SELECT l.k, lv, r.k, rv FROM l LEFT JOIN r ON l.k = r.k;")
        );
    }

    /// RIGHT JOIN output columns stay in DECLARED order (left ++ right) even
    /// though the right input is the preserved/probing side.
    #[test]
    fn right_join_keeps_declared_column_order() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);").unwrap();
        db.execute("INSERT INTO r VALUES (1, 100), (2, NULL), (NULL, 300), (5, 500);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT * FROM l RIGHT OUTER JOIN r ON l.k = r.k;"),
            vec![
                vec![V::Integer(1), V::Integer(10), V::Integer(1), V::Integer(100)],
                vec![V::Integer(2), V::Integer(20), V::Integer(2), V::Null],
                vec![V::Null, V::Null, V::Null, V::Integer(300)],
                vec![V::Null, V::Null, V::Integer(5), V::Integer(500)],
            ]
        );
    }

    /// Empty left / right / both sides in either direction.
    #[test]
    fn outer_join_empty_sides() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("CREATE TABLE e (k INTEGER, ev INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 10);").unwrap();
        db.execute("INSERT INTO r VALUES (1, 20);").unwrap();
        // Empty right: LEFT keeps its left row NULL-extended; RIGHT is empty.
        assert_eq!(
            select(&mut db, "SELECT lv, ev FROM l LEFT JOIN e ON l.k = e.k;"),
            vec![vec![V::Integer(10), V::Null]]
        );
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM l RIGHT JOIN e ON l.k = e.k;"),
            vec![vec![V::Bigint(0)]]
        );
        // Empty left: LEFT is empty; RIGHT keeps its right row NULL-extended.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM e LEFT JOIN r ON e.k = r.k;"),
            vec![vec![V::Bigint(0)]]
        );
        assert_eq!(
            select(&mut db, "SELECT ev, rv FROM e RIGHT JOIN r ON e.k = r.k;"),
            vec![vec![V::Null, V::Integer(20)]]
        );
        // Both empty.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM e LEFT JOIN e e2 ON e.k = e2.k;"),
            vec![vec![V::Bigint(0)]]
        );
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM e RIGHT JOIN e e2 ON e.k = e2.k;"),
            vec![vec![V::Bigint(0)]]
        );
    }

    /// Disjoint keys: every probe row is NULL-extended.
    #[test]
    fn outer_join_all_unmatched() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 10), (2, 20);").unwrap();
        db.execute("INSERT INTO r VALUES (3, 30), (4, 40);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k;"),
            vec![vec![V::Integer(10), V::Null], vec![V::Integer(20), V::Null]]
        );
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k;"),
            vec![vec![V::Null, V::Integer(30)], vec![V::Null, V::Integer(40)]]
        );
    }

    /// Multi-chunk inputs (3000 left rows, 3000 right rows = 2 chunks each):
    /// matches and NULL-extensions interleave across chunk boundaries.
    #[test]
    fn outer_join_multi_chunk_inputs() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        insert_rows(&mut db, "l", 3000, |i| format!("({i}, {})", i as i64 + 1));
        insert_rows(&mut db, "r", 3000, |i| format!("({}, {})", i as i64 + 1000, i as i64 + 100));
        // Keys overlap on 1000..3000: 2000 matches, 1000 NULL-extended left rows.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*), COUNT(r.rv) FROM l LEFT JOIN r ON l.k = r.k;"),
            vec![vec![V::Bigint(3000), V::Bigint(2000)]]
        );
        let rows = select(&mut db, "SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k;");
        assert_eq!(rows[0], vec![V::Integer(0), V::Null]);
        assert_eq!(rows[999], vec![V::Integer(999), V::Null]);
        assert_eq!(rows[1000], vec![V::Integer(1000), V::Integer(1000)]);
        assert_eq!(rows[2999], vec![V::Integer(2999), V::Integer(2999)]);
        // RIGHT mirrors: every right row survives, in right-row order.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*), COUNT(l.lv) FROM l RIGHT JOIN r ON l.k = r.k;"),
            vec![vec![V::Bigint(3000), V::Bigint(2000)]]
        );
        let rows = select(&mut db, "SELECT l.k, r.k FROM l RIGHT JOIN r ON l.k = r.k;");
        assert_eq!(rows[0], vec![V::Integer(1000), V::Integer(1000)]);
        assert_eq!(rows[1999], vec![V::Integer(2999), V::Integer(2999)]);
        assert_eq!(rows[2000], vec![V::Null, V::Integer(3000)]);
        assert_eq!(rows[2999], vec![V::Null, V::Integer(3999)]);
    }

    /// One key expanding past CHUNK_SIZE on the preserved side.
    #[test]
    fn outer_join_expansion_spans_chunks() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t1 (id INTEGER);").unwrap();
        db.execute("CREATE TABLE t2 (id INTEGER, rn INTEGER);").unwrap();
        db.execute("INSERT INTO t1 VALUES (1);").unwrap();
        insert_rows(&mut db, "t2", 5000, |i| format!("(1, {i})"));
        let rows = select(&mut db, "SELECT rn FROM t1 LEFT JOIN t2 ON t1.id = t2.id;");
        assert_eq!(rows.len(), 5000);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], V::Integer(i as i32), "row {i}");
        }
        // RIGHT: one right row matching 2500 left rows, left-row order.
        db.execute("CREATE TABLE t3 (id INTEGER, rn INTEGER);").unwrap();
        db.execute("CREATE TABLE t4 (id INTEGER);").unwrap();
        db.execute("INSERT INTO t4 VALUES (1);").unwrap();
        insert_rows(&mut db, "t3", 2500, |i| format!("(1, {i})"));
        let rows = select(&mut db, "SELECT t3.rn FROM t4 RIGHT JOIN t3 ON t4.id = t3.id;");
        assert_eq!(rows.len(), 2500);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], V::Integer(i as i32), "row {i}");
        }
    }

    /// Cross-type INTEGER = BIGINT keys match on both outer kinds.
    #[test]
    fn outer_join_cross_type_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a (x INTEGER, av INTEGER);").unwrap();
        db.execute("CREATE TABLE b (x BIGINT, bv INTEGER);").unwrap();
        db.execute("INSERT INTO a VALUES (1, 10), (2, 20);").unwrap();
        db.execute("INSERT INTO b VALUES (2, 200), (3, 300);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT * FROM a LEFT JOIN b ON a.x = b.x;"),
            vec![
                vec![V::Integer(1), V::Integer(10), V::Null, V::Null],
                vec![V::Integer(2), V::Integer(20), V::Bigint(2), V::Integer(200)],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT * FROM a RIGHT JOIN b ON a.x = b.x;"),
            vec![
                vec![V::Integer(2), V::Integer(20), V::Bigint(2), V::Integer(200)],
                vec![V::Null, V::Null, V::Bigint(3), V::Integer(300)],
            ]
        );
    }

    /// VARCHAR keys take the byte-encoded match table; NULL keys never match.
    #[test]
    fn outer_join_string_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ls (k VARCHAR, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE rs (k VARCHAR, rv INTEGER);").unwrap();
        db.execute("INSERT INTO ls VALUES ('a', 1), ('b', 2), (NULL, 3);").unwrap();
        db.execute("INSERT INTO rs VALUES ('b', 20), ('c', 30), (NULL, 40);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT ls.k, lv, rs.k, rv FROM ls LEFT JOIN rs ON ls.k = rs.k;"),
            vec![
                vec![V::Varchar("a".to_string()), V::Integer(1), V::Null, V::Null],
                vec![
                    V::Varchar("b".to_string()),
                    V::Integer(2),
                    V::Varchar("b".to_string()),
                    V::Integer(20),
                ],
                vec![V::Null, V::Integer(3), V::Null, V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT rv, lv FROM ls RIGHT JOIN rs ON ls.k = rs.k;"),
            vec![
                vec![V::Integer(20), V::Integer(2)],
                vec![V::Integer(30), V::Null],
                vec![V::Integer(40), V::Null],
            ]
        );
    }

    /// WHERE on a LEFT JOIN (values bag-equal to DuckDB; row order is
    /// pinkdb-deterministic as noted at the RIGHT case):
    /// - a right-side predicate stays above the join and drops NULL-extended
    ///   rows when it rejects NULL;
    /// - `r.rv IS NULL` keeps exactly the unmatched/NULL-valued rows;
    /// - a left-side predicate pushes into the preserved left input.
    #[test]
    fn left_join_where_predicates_match_duckdb() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);").unwrap();
        db.execute("INSERT INTO r VALUES (1, 100), (2, NULL), (NULL, 300), (5, 500);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k WHERE r.rv > 50;"),
            vec![vec![V::Integer(10), V::Integer(100)]]
        );
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k WHERE r.rv IS NULL;"),
            vec![
                vec![V::Integer(20), V::Null],
                vec![V::Integer(30), V::Null],
                vec![V::Integer(40), V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k WHERE l.lv > 15;"),
            vec![
                vec![V::Integer(20), V::Null],
                vec![V::Integer(30), V::Null],
                vec![V::Integer(40), V::Null],
            ]
        );
        // RIGHT mirrors: the right-side predicate pushes; a left-side one
        // stays above (NULL-extended left rows survive `l.lv IS NULL`).
        // Values below are bag-equal to DuckDB's, not order-identical:
        // pinkdb emits deterministic right-row order ([10,100], [NULL,300],
        // [NULL,500]) while DuckDB prints [10,100], [NULL,500], [NULL,300].
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k WHERE r.rv > 50;"),
            vec![
                vec![V::Integer(10), V::Integer(100)],
                vec![V::Null, V::Integer(300)],
                vec![V::Null, V::Integer(500)],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k WHERE l.lv IS NULL;"),
            vec![vec![V::Null, V::Integer(300)], vec![V::Null, V::Integer(500)]]
        );
    }

    /// NULL-extension covers every right-side dtype (not just INTEGER/VARCHAR):
    /// BOOLEAN/DOUBLE/DATE/TIMESTAMP columns come back as typed NULLs, and the
    /// `SELECT *` shape stays left ++ right.
    #[test]
    fn outer_join_null_extension_all_dtypes() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, b BOOLEAN, d DOUBLE, dt DATE, ts TIMESTAMP);")
            .unwrap();
        db.execute(
            "INSERT INTO l VALUES
             (1, true, 1.5, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00.000001');",
        )
        .unwrap();
        db.execute("CREATE TABLE e (k INTEGER, b BOOLEAN, d DOUBLE, dt DATE, ts TIMESTAMP);")
            .unwrap();
        let left = select(&mut db, "SELECT * FROM l;");
        let sql = "SELECT * FROM l LEFT JOIN e ON l.k = e.k;";
        let rows = select(&mut db, sql);
        assert_eq!(rows.len(), 1);
        // Left values survive; every right-side dtype NULL-extends.
        assert_eq!(&rows[0][..5], &left[0][..]);
        assert!(rows[0][5..].iter().all(|v| *v == V::Null));
        // The NULLs are actually invalid values, not sentinels: COUNT skips all.
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(e.b), COUNT(e.d), COUNT(e.dt), COUNT(e.ts)
                 FROM l LEFT JOIN e ON l.k = e.k;"
            ),
            vec![vec![V::Bigint(0), V::Bigint(0), V::Bigint(0), V::Bigint(0)]]
        );
        let (names, types) = select_meta(&mut db, sql);
        assert_eq!(names.len(), 10);
        assert_eq!(
            types,
            vec![
                DataType::Integer,
                DataType::Boolean,
                DataType::Double,
                DataType::Date,
                DataType::Timestamp,
                DataType::Integer,
                DataType::Boolean,
                DataType::Double,
                DataType::Date,
                DataType::Timestamp,
            ]
        );
        // RIGHT against an empty left mirrors: left dtypes NULL-extend first.
        let rows = select(&mut db, "SELECT * FROM e RIGHT JOIN l ON e.k = l.k;");
        assert_eq!(rows.len(), 1);
        assert!(rows[0][..5].iter().all(|v| *v == V::Null));
        assert_eq!(&rows[0][5..], &left[0][..]);
    }

    /// Outer joins on BOOLEAN keys (byte path), DATE / TIMESTAMP keys (int
    /// path), and mixed INTEGER = DOUBLE keys (byte path, canonicalized to
    /// DOUBLE); all values verified against DuckDB.
    #[test]
    fn outer_join_boolean_date_timestamp_and_double_keys() {
        let mut db = PinkDb::new();
        // BOOLEAN keys: RIGHT preserves right-row order, so the NULL-keyed
        // right row stays between true and false.
        db.execute("CREATE TABLE lb (k BOOLEAN, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE rb (k BOOLEAN, rv INTEGER);").unwrap();
        db.execute("INSERT INTO lb VALUES (true, 1), (false, 2), (NULL, 3);").unwrap();
        db.execute("INSERT INTO rb VALUES (true, 10), (NULL, 11), (false, 12);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT lb.k, lv, rb.k, rv FROM lb LEFT JOIN rb ON lb.k = rb.k;"),
            vec![
                vec![V::Boolean(true), V::Integer(1), V::Boolean(true), V::Integer(10)],
                vec![V::Boolean(false), V::Integer(2), V::Boolean(false), V::Integer(12)],
                vec![V::Null, V::Integer(3), V::Null, V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT lb.k, lv, rb.k, rv FROM lb RIGHT JOIN rb ON lb.k = rb.k;"),
            vec![
                vec![V::Boolean(true), V::Integer(1), V::Boolean(true), V::Integer(10)],
                vec![V::Null, V::Null, V::Null, V::Integer(11)],
                vec![V::Boolean(false), V::Integer(2), V::Boolean(false), V::Integer(12)],
            ]
        );
        // DATE keys.
        db.execute("CREATE TABLE ld (k DATE, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE rd (k DATE, rv INTEGER);").unwrap();
        db.execute(
            "INSERT INTO ld VALUES (DATE '2024-01-15', 1), (DATE '2024-02-20', 2), (NULL, 3);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO rd VALUES (DATE '2024-01-15', 10), (DATE '2024-03-01', 11), (NULL, 12);",
        )
        .unwrap();
        let jan15 = select(&mut db, "SELECT k FROM ld WHERE lv = 1;")[0][0].clone();
        let feb20 = select(&mut db, "SELECT k FROM ld WHERE lv = 2;")[0][0].clone();
        let mar01 = select(&mut db, "SELECT k FROM rd WHERE rv = 11;")[0][0].clone();
        assert_eq!(
            select(&mut db, "SELECT ld.k, lv, rd.k, rv FROM ld LEFT JOIN rd ON ld.k = rd.k;"),
            vec![
                vec![jan15.clone(), V::Integer(1), jan15.clone(), V::Integer(10)],
                vec![feb20.clone(), V::Integer(2), V::Null, V::Null],
                vec![V::Null, V::Integer(3), V::Null, V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT ld.k, lv, rd.k, rv FROM ld RIGHT JOIN rd ON ld.k = rd.k;"),
            vec![
                vec![jan15.clone(), V::Integer(1), jan15, V::Integer(10)],
                vec![V::Null, V::Null, mar01, V::Integer(11)],
                vec![V::Null, V::Null, V::Null, V::Integer(12)],
            ]
        );
        // TIMESTAMP keys match at microsecond precision.
        db.execute("CREATE TABLE lt (k TIMESTAMP, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE rt (k TIMESTAMP, rv INTEGER);").unwrap();
        db.execute(
            "INSERT INTO lt VALUES
             (TIMESTAMP '2024-01-15 10:00:00.000001', 1),
             (TIMESTAMP '2024-01-15 10:00:00', 2), (NULL, 3);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO rt VALUES
             (TIMESTAMP '2024-01-15 10:00:00', 10),
             (TIMESTAMP '2024-01-15 10:00:00.000001', 11), (NULL, 12);",
        )
        .unwrap();
        let us1 = select(&mut db, "SELECT k FROM lt WHERE lv = 1;")[0][0].clone();
        let sec = select(&mut db, "SELECT k FROM lt WHERE lv = 2;")[0][0].clone();
        assert_eq!(
            select(&mut db, "SELECT lt.k, lv, rt.k, rv FROM lt LEFT JOIN rt ON lt.k = rt.k;"),
            vec![
                vec![us1.clone(), V::Integer(1), us1.clone(), V::Integer(11)],
                vec![sec.clone(), V::Integer(2), sec.clone(), V::Integer(10)],
                vec![V::Null, V::Integer(3), V::Null, V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT lt.k, lv, rt.k, rv FROM lt RIGHT JOIN rt ON lt.k = rt.k;"),
            vec![
                vec![sec.clone(), V::Integer(2), sec, V::Integer(10)],
                vec![us1.clone(), V::Integer(1), us1, V::Integer(11)],
                vec![V::Null, V::Null, V::Null, V::Integer(12)],
            ]
        );
        // Mixed INTEGER = DOUBLE keys canonicalize to DOUBLE (1 matches 1.0).
        db.execute("CREATE TABLE li (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE rdd (k DOUBLE, rv INTEGER);").unwrap();
        db.execute("INSERT INTO li VALUES (1, 10), (2, 20), (NULL, 30);").unwrap();
        db.execute("INSERT INTO rdd VALUES (1.0, 100), (1.5, 150), (2.0, 200), (NULL, 300);")
            .unwrap();
        assert_eq!(
            select(&mut db, "SELECT li.k, lv, rdd.k, rv FROM li LEFT JOIN rdd ON li.k = rdd.k;"),
            vec![
                vec![V::Integer(1), V::Integer(10), V::Double(1.0), V::Integer(100)],
                vec![V::Integer(2), V::Integer(20), V::Double(2.0), V::Integer(200)],
                vec![V::Null, V::Integer(30), V::Null, V::Null],
            ]
        );
        assert_eq!(
            select(&mut db, "SELECT li.k, lv, rdd.k, rv FROM li RIGHT JOIN rdd ON li.k = rdd.k;"),
            vec![
                vec![V::Integer(1), V::Integer(10), V::Double(1.0), V::Integer(100)],
                vec![V::Null, V::Null, V::Double(1.5), V::Integer(150)],
                vec![V::Integer(2), V::Integer(20), V::Double(2.0), V::Integer(200)],
                vec![V::Null, V::Null, V::Null, V::Integer(300)],
            ]
        );
    }

    /// Probe rows interleave expanding matches, NULL keys, and unmatched keys,
    /// so matches and NULL-extensions alternate across output-chunk flushes
    /// (5000 probe rows → 6667 output rows, >3 flushes at CHUNK_SIZE 2048).
    #[test]
    fn outer_join_interleaved_probe_flushes() {
        const N: usize = 5000;
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        // Key 1 has two matches; 8/9 are decoys for the unmatched probe rows.
        db.execute("INSERT INTO r VALUES (1, 100), (8, 800), (1, 101), (9, 900);").unwrap();
        insert_rows(&mut db, "l", N, |i| match i % 3 {
            0 => format!("(1, {i})"),
            1 => format!("(NULL, {i})"),
            _ => format!("(7, {i})"),
        });
        let rows = select(&mut db, "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k;");
        let mut expected: Vec<Vec<V>> = Vec::new();
        for i in 0..N {
            if i % 3 == 0 {
                expected.push(vec![V::Integer(i as i32), V::Integer(100)]);
                expected.push(vec![V::Integer(i as i32), V::Integer(101)]);
            } else {
                expected.push(vec![V::Integer(i as i32), V::Null]);
            }
        }
        assert_eq!(expected.len(), 6667);
        assert_eq!(rows, expected);
    }

    /// RIGHT join determinism over a >CHUNK_SIZE preserved side with NULL
    /// keys: two runs are identical and follow fixed right-row order.
    #[test]
    fn outer_join_large_right_output_is_deterministic() {
        const N: usize = 5000;
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 100), (8, 800), (1, 101), (9, 900);").unwrap();
        insert_rows(&mut db, "r", N, |i| match i % 4 {
            0 => format!("(1, {i})"),
            1 => format!("(NULL, {i})"),
            2 => format!("(2, {i})"),
            _ => format!("(7, {i})"),
        });
        let sql = "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k;";
        let first = select(&mut db, sql);
        let second = select(&mut db, sql);
        assert_eq!(first, second);
        // Fixed right-row order: key 1 expands to two left rows; NULL keys and
        // unmatched keys (2, 7) NULL-extend the left side.
        let mut expected: Vec<Vec<V>> = Vec::new();
        for i in 0..N {
            if i % 4 == 0 {
                expected.push(vec![V::Integer(100), V::Integer(i as i32)]);
                expected.push(vec![V::Integer(101), V::Integer(i as i32)]);
            } else {
                expected.push(vec![V::Null, V::Integer(i as i32)]);
            }
        }
        assert_eq!(first, expected);
    }

    /// RIGHT join `SELECT *` with an empty left side: NULLs for the left
    /// columns in declared order, right values, schema left ++ right.
    #[test]
    fn right_join_select_star_empty_left() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE e (k INTEGER, ev VARCHAR);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv DOUBLE);").unwrap();
        db.execute("INSERT INTO r VALUES (1, 1.5), (2, 2.5);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT * FROM e RIGHT JOIN r ON e.k = r.k;"),
            vec![
                vec![V::Null, V::Null, V::Integer(1), V::Double(1.5)],
                vec![V::Null, V::Null, V::Integer(2), V::Double(2.5)],
            ]
        );
        let (names, types) = select_meta(&mut db, "SELECT * FROM e RIGHT JOIN r ON e.k = r.k;");
        assert_eq!(
            names,
            vec!["k".to_string(), "ev".to_string(), "k".to_string(), "rv".to_string()]
        );
        assert_eq!(
            types,
            vec![DataType::Integer, DataType::Varchar, DataType::Integer, DataType::Double]
        );
    }

    /// Wave-20 parser regression: LEFT/RIGHT/OUTER/FULL/CROSS are keywords,
    /// so bare `left`/`right` identifiers are rejected everywhere; quoting
    /// them keeps working as column, table, and alias names.
    #[test]
    fn outer_join_reserved_words_reject_bare_identifiers() {
        let mut db = PinkDb::new();
        // Bare keyword in column position.
        assert!(db.execute("CREATE TABLE t (left INTEGER);").is_err());
        assert!(db.execute("CREATE TABLE t (right INTEGER);").is_err());
        // Bare keyword in table position.
        assert!(db.execute("CREATE TABLE left (x INTEGER);").is_err());
        assert!(db.execute("SELECT * FROM left;").is_err());
        assert!(db.execute("SELECT * FROM right;").is_err());
        // Bare keyword as an alias.
        assert!(db.execute("SELECT 1 AS left;").is_err());
        assert!(db.execute("SELECT 1 AS right;").is_err());
        // Quoted identifiers work as column, table, and alias names.
        db.execute("CREATE TABLE \"left\" (\"right\" INTEGER, x VARCHAR);").unwrap();
        db.execute("INSERT INTO \"left\" VALUES (1, 'a'), (2, 'b');").unwrap();
        assert_eq!(
            select(&mut db, "SELECT \"right\" FROM \"left\" ORDER BY 1;"),
            vec![vec![V::Integer(1)], vec![V::Integer(2)]]
        );
        assert_eq!(
            select(&mut db, "SELECT x FROM \"left\" AS \"right\" ORDER BY x;"),
            vec![vec![V::Varchar("a".to_string())], vec![V::Varchar("b".to_string())]]
        );
        let (names, _) = select_meta(&mut db, "SELECT 1 AS \"left\";");
        assert_eq!(names, vec!["left".to_string()]);
    }

    /// Aggregates over a LEFT JOIN: NULL-extended rows count in `COUNT(*)`
    /// but are skipped by `COUNT(x)`/`SUM`/`AVG`/`MIN`/`MAX` (DuckDB values).
    #[test]
    fn left_join_aggregates_skip_null_extended_rows() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE l (k INTEGER, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE r (k INTEGER, rv INTEGER);").unwrap();
        db.execute("INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);").unwrap();
        db.execute("INSERT INTO r VALUES (1, 100), (1, 25), (2, NULL), (NULL, 300), (5, 500);")
            .unwrap();
        // 5 joined rows: two matches for l.k=1, one for l.k=2, two NULL-extended.
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), COUNT(r.rv), SUM(r.rv), AVG(r.rv), MIN(r.rv), MAX(r.rv)
                 FROM l LEFT JOIN r ON l.k = r.k;"
            ),
            vec![vec![
                V::Bigint(5),
                V::Bigint(2),
                V::Bigint(125),
                V::Double(62.5),
                V::Integer(25),
                V::Integer(100),
            ]]
        );
        // Grouped over the NULL-extended side: the unmatched left rows land in
        // a NULL group, and COUNT(r.rv) is 0 there. NULLS LAST on ORDER BY.
        assert_eq!(
            select(
                &mut db,
                "SELECT r.rv, COUNT(*), COUNT(l.lv)
                 FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.rv ORDER BY r.rv;"
            ),
            vec![
                vec![V::Integer(25), V::Bigint(1), V::Bigint(1)],
                vec![V::Integer(100), V::Bigint(1), V::Bigint(1)],
                vec![V::Null, V::Bigint(3), V::Bigint(3)],
            ]
        );
        // All-unmatched: COUNT(*) still counts the NULL-extended rows.
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), COUNT(r.rv), SUM(r.rv), MIN(r.rv), MAX(r.rv)
                 FROM l LEFT JOIN r ON l.k = r.k WHERE false;"
            ),
            vec![vec![V::Bigint(0), V::Bigint(0), V::Null, V::Null, V::Null]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), COUNT(r.rv), SUM(r.rv), MIN(r.rv), MAX(r.rv)
                 FROM (SELECT * FROM l WHERE k > 100) x LEFT JOIN r ON x.k = r.k;"
            ),
            vec![vec![V::Bigint(0), V::Bigint(0), V::Null, V::Null, V::Null]]
        );
    }

    /// The fused join→aggregate path feeds only matched pairs, so it must
    /// never fire for outer joins; the materialized path counts the
    /// NULL-extended rows instead.
    #[test]
    fn fusion_rejects_outer_joins() {
        let db = fused_db();
        let cx = Cx::new(&db);
        for sql in [
            "SELECT COUNT(*) FROM fa LEFT JOIN fb ON fa.id = fb.id;",
            "SELECT fa.region, COUNT(*), SUM(fb.total) FROM fa LEFT JOIN fb ON fa.id = fb.id GROUP BY fa.region;",
            "SELECT COUNT(*) FROM fa RIGHT JOIN fb ON fa.id = fb.id;",
        ] {
            let plan = bound_plan(&db, sql);
            let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
                unreachable!()
            };
            assert!(
                fused_join_aggregate(&cx, input, keys, aggs).unwrap().is_none(),
                "must not fuse: {sql}"
            );
        }
        // Materialized correctness: 5 left rows, 4 matched pairs for id=1,
        // 4 for id=2, one NULL-key left row NULL-extended → 9 rows.
        let mut db = fused_db();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM fa LEFT JOIN fb ON fa.id = fb.id;"),
            vec![vec![V::Bigint(9)]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(fb.total), SUM(fb.total) FROM fa LEFT JOIN fb ON fa.id = fb.id;"
            ),
            vec![vec![V::Bigint(6), V::Double(69.5)]]
        );
        // Same aggregate forced through the materialized path explicitly:
        // the fusion helper must agree with `eval` + `aggregate` (it returns
        // None, so the query above already ran materialized).
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM fa RIGHT JOIN fb ON fa.id = fb.id;"),
            vec![vec![V::Bigint(10)]]
        );
    }

    #[test]
    fn multirow_insert_is_atomic() {
        // M3: a bad row anywhere aborts the whole INSERT.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE p (a INTEGER);").unwrap();
        db.execute("INSERT INTO p VALUES (1);").unwrap();
        assert!(db.execute("INSERT INTO p VALUES (2), ('bad');").is_err());
        assert_eq!(select(&mut db, "SELECT * FROM p;"), vec![vec![V::Integer(1)]]);
    }

    // --- phase 7: dates + timestamps ---

    fn date_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ev (id INTEGER, d DATE, ts TIMESTAMP);").unwrap();
        db.execute(
            "INSERT INTO ev VALUES
             (1, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00'),
             (2, DATE '2024-02-20', TIMESTAMP '2024-02-20 23:59:59.5'),
             (3, NULL, NULL);",
        )
        .unwrap();
        db
    }

    #[test]
    fn dates_roundtrip_and_display() {
        let mut db = date_db();
        let rows = select(&mut db, "SELECT d, ts FROM ev ORDER BY id;");
        assert_eq!(rows[0][0].to_string(), "2024-01-15");
        assert_eq!(rows[0][1].to_string(), "2024-01-15 10:00:00");
        assert_eq!(rows[1][1].to_string(), "2024-02-20 23:59:59.500000");
        assert_eq!(rows[2], vec![V::Null, V::Null]);
    }

    #[test]
    fn date_arithmetic() {
        let mut db = date_db();
        let rows = select(&mut db, "SELECT d + 10, 1 + d, d - 5 FROM ev WHERE id = 1;");
        assert_eq!(rows[0][0].to_string(), "2024-01-25");
        assert_eq!(rows[0][1].to_string(), "2024-01-16");
        assert_eq!(rows[0][2].to_string(), "2024-01-10");
        let rows = select(&mut db, "SELECT DATE '2024-02-20' - DATE '2024-01-15';");
        assert_eq!(rows, vec![vec![V::Bigint(36)]]);
        // NULL propagates.
        let rows = select(&mut db, "SELECT d + 1 FROM ev WHERE id = 3;");
        assert_eq!(rows, vec![vec![V::Null]]);
        // Rejected: timestamp ± int, date ± date (non-minus), mixed compare.
        assert!(db.execute("SELECT ts + 1 FROM ev;").is_err());
        assert!(db.execute("SELECT d + d FROM ev;").is_err());
        assert!(db.execute("SELECT * FROM ev WHERE d = ts;").is_err());
        assert!(db.execute("SELECT * FROM ev WHERE d = '2024-01-15';").is_err());
    }

    /// Extreme DATE → TIMESTAMP casts must error, not wrap: `i32::MAX` days
    /// × 86_400_000_000 µs overflows i64.
    #[test]
    fn cast_extreme_date_to_timestamp_errors() {
        let mut db = PinkDb::new();
        let rows = select(&mut db, "SELECT DATE '1970-01-01' + 2147483647;");
        assert_eq!(rows[0][0], V::Date(i32::MAX));
        assert!(db
            .execute("SELECT CAST(DATE '1970-01-01' + 2147483647 AS TIMESTAMP);")
            .is_err());
    }

    #[test]
    fn date_functions() {
        let mut db = date_db();
        let rows = select(
            &mut db,
            "SELECT year(d), month(d), day(d), hour(ts), minute(ts), second(ts) FROM ev WHERE id = 2;",
        );
        assert_eq!(
            rows,
            vec![vec![
                V::Integer(2024),
                V::Integer(2),
                V::Integer(20),
                V::Integer(23),
                V::Integer(59),
                V::Integer(59)
            ]]
        );
        let rows = select(&mut db, "SELECT date_trunc('month', d), date_trunc('day', ts) FROM ev WHERE id = 1;");
        assert_eq!(rows[0][0].to_string(), "2024-01-01");
        assert_eq!(rows[0][1].to_string(), "2024-01-15 00:00:00");
        let rows = select(&mut db, "SELECT date_trunc('year', TIMESTAMP '2024-05-06 07:08:09');");
        assert_eq!(rows[0][0].to_string(), "2024-01-01 00:00:00");
        assert!(db.execute("SELECT hour(d) FROM ev;").is_err());
        assert!(db.execute("SELECT date_trunc('century', d) FROM ev;").is_err());
        assert!(db.execute("SELECT SUM(d) FROM ev;").is_err());
    }

    #[test]
    fn dates_group_order_join_minmax() {
        let mut db = date_db();
        let rows = select(&mut db, "SELECT MIN(d), MAX(d), COUNT(d) FROM ev;");
        assert_eq!(rows[0][0].to_string(), "2024-01-15");
        assert_eq!(rows[0][1].to_string(), "2024-02-20");
        assert_eq!(rows[0][2], V::Bigint(2));
        // date_trunc grouping (analytics workhorse).
        let rows = select(
            &mut db,
            "SELECT date_trunc('month', d) AS m, COUNT(*) FROM ev GROUP BY m ORDER BY m;",
        );
        assert_eq!(rows.len(), 3); // two months + NULL group
        assert_eq!(rows[2][0], V::Null);
        // Ordering + join on dates.
        db.execute("CREATE TABLE h (d DATE, note VARCHAR);").unwrap();
        db.execute("INSERT INTO h VALUES (DATE '2024-01-15', 'x');").unwrap();
        let rows = select(&mut db, "SELECT note FROM ev JOIN h ON ev.d = h.d ORDER BY id;");
        assert_eq!(rows, vec![vec![V::Varchar("x".to_string())]]);
        let rows = select(&mut db, "SELECT DISTINCT d FROM ev ORDER BY d;");
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
    }

    #[test]
    fn csv_inflects_dates() {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(
            &mut db,
            "c",
            "d,ts\n2024-01-15,2024-01-15 10:00:00\n,",
        )
        .unwrap();
        let t = db.table("c").unwrap();
        assert_eq!(
            t.schema(),
            vec![
                ("d".to_string(), DataType::Date),
                ("ts".to_string(), DataType::Timestamp),
            ]
        );
        assert_eq!(t.get(1, 0), Some(Value::Null));
        // Almost-dates stay VARCHAR.
        let mut db2 = PinkDb::new();
        crate::csv::import_csv_string(&mut db2, "c", "d\n2024-13-01\n2024-01-15\n").unwrap();
        assert_eq!(db2.table("c").unwrap().schema()[0].1, DataType::Varchar);
    }

    #[test]
    fn dates_persist() {
        let db = date_db();
        // Unique per process *and* per call: concurrent `cargo test`
        // invocations must not race on one temp path.
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pinkdb_dates_{}_{}.pink",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        db.save(path.to_str().unwrap()).unwrap();
        let back = PinkDb::open(path.to_str().unwrap()).unwrap();
        let (a, b) = (db.table("ev").unwrap(), back.table("ev").unwrap());
        assert_eq!(a.num_rows(), b.num_rows());
        for r in 0..a.num_rows() {
            for c in 0..3 {
                assert_eq!(a.get(r, c), b.get(r, c), "row {r} col {c}");
            }
        }
        std::fs::remove_file(&path).ok();
    }

    // --- phase 6: subqueries + CTEs ---

    fn sub_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "emp", "id,name,did\n1,ann,10\n2,bob,20\n3,cat,").unwrap();
        crate::csv::import_csv_string(&mut db, "dept", "did,dname\n10,eng\n20,ops\n").unwrap();
        db
    }

    #[test]
    fn cte_basic_and_chained() {
        let mut db = sub_db();
        let rows = select(
            &mut db,
            "WITH e AS (SELECT * FROM emp WHERE id > 1) SELECT name FROM e ORDER BY name;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Varchar("bob".to_string())], vec![V::Varchar("cat".to_string())]]
        );
        let rows = select(
            &mut db,
            "WITH e AS (SELECT * FROM emp WHERE id > 1),
                  n AS (SELECT name FROM e WHERE did IS NOT NULL)
             SELECT * FROM n;",
        );
        assert_eq!(rows, vec![vec![V::Varchar("bob".to_string())]]);
    }

    #[test]
    fn cte_with_agg_join_and_alias_cols() {
        let mut db = sub_db();
        let rows = select(
            &mut db,
            "WITH d(did, n) AS (SELECT did, COUNT(*) FROM emp GROUP BY did)
             SELECT did, n FROM d ORDER BY did;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Integer(10), V::Bigint(1)],
                vec![V::Integer(20), V::Bigint(1)],
                vec![V::Null, V::Bigint(1)],
            ]
        );
        let rows = select(
            &mut db,
            "WITH e AS (SELECT * FROM emp) SELECT dname FROM e JOIN dept ON e.did = dept.did ORDER BY 1;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Varchar("eng".to_string())], vec![V::Varchar("ops".to_string())]]
        );
    }

    #[test]
    fn derived_table_needs_alias() {
        let mut db = sub_db();
        assert!(db.execute("SELECT * FROM (SELECT * FROM emp);").is_err());
        let rows = select(&mut db, "SELECT name FROM (SELECT * FROM emp WHERE id = 2) AS s;");
        assert_eq!(rows, vec![vec![V::Varchar("bob".to_string())]]);
    }

    #[test]
    fn scalar_subquery() {
        let mut db = sub_db();
        let rows = select(&mut db, "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE dname = 'ops');");
        assert_eq!(rows, vec![vec![V::Varchar("bob".to_string())]]);
        // Empty → NULL → no rows.
        let rows = select(&mut db, "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE false);");
        assert!(rows.is_empty());
        // Multi-row → error.
        assert!(db.execute("SELECT (SELECT id FROM emp);").is_err());
        // Scalar over empty table in SELECT list.
        let rows = select(&mut db, "SELECT (SELECT COUNT(*) FROM emp WHERE false);");
        assert_eq!(rows, vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn in_subquery_and_list() {
        let mut db = sub_db();
        let rows = select(
            &mut db,
            "SELECT name FROM emp WHERE did IN (SELECT did FROM dept) ORDER BY name;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Varchar("ann".to_string())], vec![V::Varchar("bob".to_string())]]
        );
        let rows = select(
            &mut db,
            "SELECT name FROM emp WHERE did NOT IN (SELECT did FROM dept) ORDER BY name;",
        );
        // cat has NULL did: NOT IN with no NULLs in set → TRUE... set has no
        // NULLs (dept.did all non-null) → cat (NULL did) → NULL → filtered.
        assert!(rows.is_empty());
        // List form.
        let rows = select(&mut db, "SELECT name FROM emp WHERE id IN (1, 3) ORDER BY name;");
        assert_eq!(
            rows,
            vec![vec![V::Varchar("ann".to_string())], vec![V::Varchar("cat".to_string())]]
        );
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE id NOT IN (2);");
        assert_eq!(rows, vec![vec![V::Bigint(2)]]);
        // NULL in list poisons non-matches.
        let rows = select(&mut db, "SELECT name FROM emp WHERE id IN (1, NULL);");
        assert_eq!(rows, vec![vec![V::Varchar("ann".to_string())]]);
    }

    #[test]
    fn uncorrelated_exists() {
        let mut db = sub_db();
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept);");
        assert_eq!(rows, vec![vec![V::Bigint(3)]]);
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE NOT EXISTS (SELECT * FROM dept WHERE false);");
        assert_eq!(rows, vec![vec![V::Bigint(3)]]);
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept WHERE false);");
        assert_eq!(rows, vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn correlated_is_rejected() {
        let mut db = sub_db();
        assert!(db
            .execute("SELECT dname FROM dept WHERE EXISTS (SELECT * FROM emp WHERE emp.did = dept.did);")
            .is_err());
    }

    fn join_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "emp", "id,name,did\n1,ann,10\n2,bob,20\n3,cat,").unwrap();
        crate::csv::import_csv_string(&mut db, "dept", "did,dname\n10,eng\n20,ops\n30,hr\n").unwrap();
        db
    }

    #[test]
    fn inner_join_basic() {
        let mut db = join_db();
        let rows = select(
            &mut db,
            "SELECT name, dname FROM emp JOIN dept ON emp.did = dept.did ORDER BY name;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("ann".to_string()), V::Varchar("eng".to_string())],
                vec![V::Varchar("bob".to_string()), V::Varchar("ops".to_string())],
            ]
        );
    }

    #[test]
    fn join_null_keys_never_match() {
        let mut db = join_db();
        // cat has NULL did: inner join drops her.
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp JOIN dept ON emp.did = dept.did;");
        assert_eq!(rows, vec![vec![V::Bigint(2)]]);
    }

    #[test]
    fn join_with_pushed_filters_and_agg() {
        let mut db = join_db();
        let rows = select(
            &mut db,
            "SELECT dname, COUNT(*) FROM emp JOIN dept ON emp.did = dept.did \
             WHERE emp.id > 0 AND dept.did > 5 GROUP BY dname ORDER BY dname;",
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], V::Varchar("eng".to_string()));
        assert_eq!(rows[0][1], V::Bigint(1));
    }

    // --- phase 8: window functions ---

    fn win_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(
            &mut db,
            "sales",
            "region,amount\nwest,10\nwest,20\neast,5\neast,5\n",
        )
        .unwrap();
        db
    }

    #[test]
    fn row_number_rank_dense_rank() {
        let mut db = win_db();
        let rows = select(
            &mut db,
            "SELECT region, amount,
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn,
                    RANK() OVER (PARTITION BY region ORDER BY amount) AS r,
                    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS d
             FROM sales ORDER BY region, amount;",
        );
        // east: (5,5) peers → rn 1,2; r 1,1; d 1,1. west: 10,20 → 1,2; 1,2; 1,2.
        assert_eq!(
            rows,
            vec![
                vec![
                    V::Varchar("east".to_string()),
                    V::Integer(5),
                    V::Bigint(1),
                    V::Bigint(1),
                    V::Bigint(1)
                ],
                vec![
                    V::Varchar("east".to_string()),
                    V::Integer(5),
                    V::Bigint(2),
                    V::Bigint(1),
                    V::Bigint(1)
                ],
                vec![
                    V::Varchar("west".to_string()),
                    V::Integer(10),
                    V::Bigint(1),
                    V::Bigint(1),
                    V::Bigint(1)
                ],
                vec![
                    V::Varchar("west".to_string()),
                    V::Integer(20),
                    V::Bigint(2),
                    V::Bigint(2),
                    V::Bigint(2)
                ],
            ]
        );
    }

    #[test]
    fn window_agg_frames() {
        let mut db = win_db();
        // Default cumulative frame with ORDER BY.
        let rows = select(
            &mut db,
            "SELECT amount, SUM(amount) OVER (ORDER BY amount) AS run FROM sales ORDER BY amount;",
        );
        let runs: Vec<i64> = rows
            .iter()
            .map(|r| match r[1] {
                V::Bigint(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(runs, vec![5, 10, 20, 40]);
        // Whole-partition default without ORDER BY.
        let rows = select(&mut db, "SELECT COUNT(*) OVER () AS c FROM sales;");
        assert!(rows.iter().all(|r| r[0] == V::Bigint(4)));
        // Explicit ROWS BETWEEN 1 PRECEDING AND CURRENT ROW.
        let rows = select(
            &mut db,
            "SELECT amount, SUM(amount) OVER (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w
             FROM sales ORDER BY amount;",
        );
        let got: Vec<i64> = rows
            .iter()
            .map(|r| match r[1] {
                V::Bigint(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![5, 10, 15, 30]);
    }

    /// Regression: a `k PRECEDING` end bound that lands before the partition
    /// start is an EMPTY frame. It used to saturate to row 0 and wrongly
    /// include that row (the incremental UNBOUNDED PRECEDING path must treat
    /// the frame as empty until `hi` reaches row 0).
    #[test]
    fn window_preceding_end_before_partition_is_empty() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5);").unwrap();
        let rows = select(
            &mut db,
            "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM t;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(0)],
                vec![V::Bigint(0)],
                vec![V::Bigint(1)],
                vec![V::Bigint(2)],
                vec![V::Bigint(3)],
            ]
        );
        let rows = select(
            &mut db,
            "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM t;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(0)],
                vec![V::Bigint(1)],
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
            ]
        );
        let rows = select(
            &mut db,
            "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM t;",
        );
        assert_eq!(rows[0], vec![V::Null]);
        assert_eq!(rows[1], vec![V::Null]);
        assert_eq!(rows[2], vec![V::Bigint(1)]);
        assert_eq!(rows[3], vec![V::Bigint(3)]);
        assert_eq!(rows[4], vec![V::Bigint(6)]);
    }

    /// FOLLOWING / UNBOUNDED FOLLOWING end bounds, plus a float SUM/AVG
    /// window with exact values and MIN/MAX over the incremental
    /// (UNBOUNDED PRECEDING) frame — the borrowed `finalize_ref` path.
    #[test]
    fn window_following_bounds_and_float_minmax() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5);").unwrap();
        let rows = select(
            &mut db,
            "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
                vec![V::Bigint(1)],
            ]
        );
        let rows = select(
            &mut db,
            "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM t;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(4)],
                vec![V::Bigint(3)],
                vec![V::Bigint(2)],
                vec![V::Bigint(1)],
                vec![V::Bigint(0)],
            ]
        );
        let rows = select(
            &mut db,
            "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;",
        );
        assert!(rows.iter().all(|r| r[0] == V::Bigint(5)));
        // Exact float sums/avgs over a sliding frame.
        db.execute("CREATE TABLE f (d DOUBLE);").unwrap();
        db.execute("INSERT INTO f VALUES (0.5), (1.5), (2.0);").unwrap();
        let rows = select(
            &mut db,
            "SELECT SUM(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
                    AVG(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM f;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Double(0.5), V::Double(0.5)],
                vec![V::Double(2.0), V::Double(1.0)],
                vec![V::Double(3.5), V::Double(1.75)],
            ]
        );
        // MIN/MAX over the growing UNBOUNDED PRECEDING frame (finalize_ref).
        let rows = select(
            &mut db,
            "SELECT MIN(d) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                    MAX(d) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM f;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Double(0.5), V::Double(0.5)],
                vec![V::Double(0.5), V::Double(1.5)],
                vec![V::Double(0.5), V::Double(2.0)],
            ]
        );
    }

    /// Regression: frames that start at UNBOUNDED PRECEDING used to be
    /// recomputed from scratch per row, making a single-partition window
    /// O(n²) (20k rows took seconds-to-minutes in debug). They must be
    /// linear while producing identical results.
    #[test]
    fn window_agg_single_partition_is_linear() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE wt (id INTEGER, k INTEGER, x INTEGER);")
            .unwrap();
        // 20 batches × 1000 rows (one INSERT per batch keeps parsing cheap).
        for batch in 0..20 {
            let mut sql = String::from("INSERT INTO wt VALUES ");
            for j in 0..1000 {
                if j > 0 {
                    sql.push_str(", ");
                }
                let i = batch * 1000 + j + 1;
                sql.push_str(&format!("({i}, 1, {i})"));
            }
            sql.push(';');
            db.execute(&sql).unwrap();
        }
        // Default frame (no ORDER BY) = whole partition: every row sees 20k.
        let rows = select(
            &mut db,
            "SELECT MIN(c), MAX(c) FROM (SELECT COUNT(*) OVER (PARTITION BY k) AS c FROM wt) AS s;",
        );
        assert_eq!(rows, vec![vec![V::Bigint(20_000), V::Bigint(20_000)]]);
        // Cumulative frame: SUM(1..=20000) = 200_010_000; window output keeps
        // input row order, so the first/last rows are id 1 and id 20000.
        let rows = select(
            &mut db,
            "SELECT SUM(x) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s \
             FROM wt;",
        );
        assert_eq!(rows.len(), 20_000);
        assert_eq!(rows[0][0], V::Bigint(1));
        assert_eq!(rows[19_999][0], V::Bigint(200_010_000));
    }

    #[test]
    fn window_over_agg_and_rejections() {        let mut db = win_db();
        let rows = select(
            &mut db,
            "SELECT region, SUM(amount) AS s, RANK() OVER (ORDER BY SUM(amount) DESC) AS r
             FROM sales GROUP BY region ORDER BY r;",
        );
        assert_eq!(rows[0][0], V::Varchar("west".to_string()));
        assert_eq!(rows[0][2], V::Bigint(1));
        // Windows forbidden in WHERE / GROUP BY / HAVING.
        assert!(db.execute("SELECT * FROM sales WHERE RANK() OVER () > 1;").is_err());
        assert!(db.execute("SELECT * FROM sales GROUP BY RANK() OVER ();").is_err());
        assert!(db
            .execute("SELECT SUM(amount) FROM sales HAVING RANK() OVER () > 1;")
            .is_err());
        // Nested windows rejected; unknown window rejected.
        assert!(db
            .execute("SELECT RANK() OVER (ORDER BY SUM(amount) OVER ()) FROM sales;")
            .is_err());
        assert!(db.execute("SELECT MEDIAN(amount) OVER () FROM sales;").is_err());
    }

    // --- phase 9: UPDATE / DELETE ---

    fn ud_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, NULL);").unwrap();
        db
    }

    fn updated(db: &mut PinkDb, sql: &str) -> usize {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Updated { count } => count,
            QueryResult::Deleted { count } => count,
            r => panic!("not a mutation result: {r:?}"),
        }
    }

    #[test]
    fn update_basic_and_where() {
        let mut db = ud_db();
        assert_eq!(updated(&mut db, "UPDATE t SET a = a + 10 WHERE a > 1;"), 2);
        assert_eq!(
            select(&mut db, "SELECT a, b FROM t ORDER BY a;"),
            vec![
                vec![V::Integer(1), V::Varchar("x".to_string())],
                vec![V::Integer(12), V::Varchar("y".to_string())],
                vec![V::Integer(13), V::Null],
            ]
        );
        // No WHERE updates everything.
        assert_eq!(updated(&mut db, "UPDATE t SET b = 'z';"), 3);
        // NULL assignment + expression to NULL.
        assert_eq!(updated(&mut db, "UPDATE t SET b = NULL WHERE a = 1;"), 1);
        assert_eq!(select(&mut db, "SELECT b FROM t WHERE a = 1;"), vec![vec![V::Null]]);
    }

    #[test]
    fn update_sets_are_simultaneous() {
        let mut db = ud_db();
        // a=b, b=a swaps (both read pre-update values).
        db.execute("CREATE TABLE s (a INTEGER, b INTEGER);").unwrap();
        db.execute("INSERT INTO s VALUES (1, 2);").unwrap();
        assert_eq!(updated(&mut db, "UPDATE s SET a = b, b = a;"), 1);
        assert_eq!(select(&mut db, "SELECT a, b FROM s;"), vec![vec![V::Integer(2), V::Integer(1)]]);
    }

    #[test]
    fn update_type_rules() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE n (i INTEGER, g BIGINT, d DOUBLE);").unwrap();
        db.execute("INSERT INTO n VALUES (1, 1, 1.0);").unwrap();
        // Widening allowed.
        assert_eq!(updated(&mut db, "UPDATE n SET g = i, d = g;"), 1);
        // Narrowing / wrong types rejected, table untouched.
        assert!(db.execute("UPDATE n SET i = g;").is_err());
        assert!(db.execute("UPDATE n SET i = 'x';").is_err());
        assert!(db.execute("UPDATE n SET missing = 1;").is_err());
        assert!(db.execute("UPDATE n SET i = 1, i = 2;").is_err());
        assert!(db.execute("UPDATE n SET i = SUM(i);").is_err());
        assert_eq!(
            select(&mut db, "SELECT i, g, d FROM n;"),
            vec![vec![V::Integer(1), V::Bigint(1), V::Double(1.0)]]
        );
    }

    #[test]
    fn delete_basic_and_all() {
        let mut db = ud_db();
        assert_eq!(updated(&mut db, "DELETE FROM t WHERE a >= 2;"), 2);
        assert_eq!(select(&mut db, "SELECT a FROM t;"), vec![vec![V::Integer(1)]]);
        // Delete everything, then reinsert (empty chunks are dropped cleanly).
        assert_eq!(updated(&mut db, "DELETE FROM t;"), 1);
        assert!(select(&mut db, "SELECT * FROM t;").is_empty());
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(0)]]);
        db.execute("INSERT INTO t VALUES (9, 'q');").unwrap();
        assert_eq!(select(&mut db, "SELECT a FROM t;"), vec![vec![V::Integer(9)]]);
        // Unknown table / bad predicate.
        assert!(db.execute("DELETE FROM missing;").is_err());
        assert!(db.execute("DELETE FROM t WHERE a;").is_err());
    }

    #[test]
    fn update_delete_with_subquery() {        let mut db = ud_db();
        assert_eq!(
            updated(&mut db, "UPDATE t SET b = 'top' WHERE a = (SELECT MAX(a) FROM t);"),
            1
        );
        assert_eq!(
            select(&mut db, "SELECT b FROM t WHERE a = 3;"),
            vec![vec![V::Varchar("top".to_string())]]
        );
        // Fresh DB: row 3 still has NULL b, so the IN-set is {3}.
        let mut db = ud_db();
        assert_eq!(updated(&mut db, "DELETE FROM t WHERE a IN (SELECT a FROM t WHERE b IS NULL);"), 1);
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(2)]]);
    }

    // --- phase 10: transactions ---

    #[test]
    fn txn_commit_and_rollback() {
        let mut db = ud_db();
        assert!(!db.in_transaction());
        db.execute("BEGIN;").unwrap();
        assert!(db.in_transaction());
        db.execute("UPDATE t SET a = 100 WHERE a = 1;").unwrap();
        db.execute("DELETE FROM t WHERE a = 2;").unwrap();
        db.execute("CREATE TABLE tmp (x INTEGER);").unwrap();
        // Inside: changes visible.
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(2)]]);
        db.execute("ROLLBACK;").unwrap();
        assert!(!db.in_transaction());
        // Outside: everything reverted, data and catalog alike.
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(3)]]);
        assert_eq!(select(&mut db, "SELECT a FROM t WHERE a = 1;"), vec![vec![V::Integer(1)]]);
        assert!(db.table("tmp").is_err());

        db.execute("BEGIN;").unwrap();
        db.execute("DELETE FROM t;").unwrap();
        db.execute("COMMIT;").unwrap();
        assert!(!db.in_transaction());
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn txn_errors() {
        let mut db = ud_db();
        assert!(db.execute("COMMIT;").is_err());
        assert!(db.execute("ROLLBACK;").is_err());
        db.execute("BEGIN;").unwrap();
        assert!(db.execute("BEGIN;").is_err());
        assert!(db.execute("BEGIN TRANSACTION;").is_err());
        db.execute("ROLLBACK;").unwrap();
        // Reads and writes work fine outside transactions too.
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(3)]]);
    }

    // --- phase 11: UNION, views, CAST, COALESCE, LIKE, DROP ---

    fn uv_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a (x INTEGER, s VARCHAR);").unwrap();
        db.execute("CREATE TABLE b (y BIGINT, t VARCHAR);").unwrap();
        db.execute("INSERT INTO a VALUES (1, 'one'), (2, 'two'), (NULL, 'nil');").unwrap();
        db.execute("INSERT INTO b VALUES (2, 'TWO'), (3, 'three');").unwrap();
        db
    }

    #[test]
    fn union_all_and_distinct() {
        let mut db = uv_db();
        // Chain-level ORDER BY sorts the whole union (INTEGER unifies to BIGINT).
        let rows = select(
            &mut db,
            "SELECT x FROM a WHERE x IS NOT NULL UNION ALL SELECT y FROM b ORDER BY x;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(1)],
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
                vec![V::Bigint(3)],
            ]
        );
        // INTEGER unifies to BIGINT (left names win).
        let rows = select(&mut db, "SELECT x AS n FROM a WHERE x = 1 UNION SELECT y AS m FROM b WHERE y = 3;");
        assert_eq!(rows.len(), 2);
        // UNION (distinct) dedupes, NULLs dedupe together; NULLS LAST.
        let rows = select(&mut db, "SELECT x FROM a UNION SELECT y FROM b ORDER BY x;");
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(1)],
                vec![V::Bigint(2)],
                vec![V::Bigint(3)],
                vec![V::Null],
            ]
        );
        // Arity + type mismatches.
        assert!(db.execute("SELECT x FROM a UNION SELECT y, t FROM b;").is_err());
        assert!(db.execute("SELECT s FROM a UNION SELECT y FROM b;").is_err());
    }

    /// {1, 3, 5} + {2, 4}: odd/even split so concatenation order is visible.
    fn union_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a1 (x INTEGER);").unwrap();
        db.execute("INSERT INTO a1 VALUES (1), (3), (5);").unwrap();
        db.execute("CREATE TABLE a2 (x INTEGER);").unwrap();
        db.execute("INSERT INTO a2 VALUES (2), (4);").unwrap();
        db
    }

    fn int_rows(ns: &[i32]) -> Vec<Vec<V>> {
        ns.iter().map(|n| vec![V::Integer(*n)]).collect()
    }

    /// Column names + types of a SELECT (for schema-leak assertions).
    fn select_meta(db: &mut PinkDb, sql: &str) -> (Vec<String>, Vec<DataType>) {
        let mut last = None;
        for r in db.execute(sql).unwrap() {
            if let QueryResult::Selected { names, types, .. } = r {
                last = Some((names, types));
            }
        }
        last.unwrap()
    }

    #[test]
    fn union_chain_order_by_and_limit() {
        let mut db = union_db();
        // Trailing ORDER BY/LIMIT apply to the whole set operation (DuckDB: 5,4,3).
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x DESC LIMIT 3;"),
            int_rows(&[5, 4, 3])
        );
        // LIMIT without ORDER BY takes the first rows of the concatenation.
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 UNION ALL SELECT x FROM a2 LIMIT 2;"),
            int_rows(&[1, 3])
        );
        // UNION (distinct) + chain ORDER BY/LIMIT.
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 UNION SELECT x FROM a2 ORDER BY x DESC LIMIT 3;"),
            int_rows(&[5, 4, 3])
        );
        // Positional and head-alias forms bind against the set-op output.
        assert_eq!(
            select(&mut db, "SELECT x AS n FROM a1 UNION ALL SELECT x FROM a2 ORDER BY 1 DESC LIMIT 2;"),
            int_rows(&[5, 4])
        );
        assert_eq!(
            select(&mut db, "SELECT x AS n FROM a1 UNION ALL SELECT x FROM a2 ORDER BY n DESC LIMIT 2;"),
            int_rows(&[5, 4])
        );
        // Clauses before a UNION (head or middle branch) are rejected.
        assert!(db.execute("SELECT x FROM a1 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a2;").is_err());
        assert!(db
            .execute("SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a1;")
            .is_err());
    }

    #[test]
    fn union_in_derived_table_and_subqueries() {
        let mut db = union_db();
        // Every branch survives inside a derived table (was: only the first).
        assert_eq!(
            select(&mut db, "SELECT x FROM (SELECT x FROM a1 UNION ALL SELECT x FROM a2) s ORDER BY x;"),
            int_rows(&[1, 2, 3, 4, 5])
        );
        // Chain-level ORDER BY/LIMIT apply to the derived result.
        assert_eq!(
            select(
                &mut db,
                "SELECT x FROM (SELECT x FROM a1 UNION ALL SELECT x FROM a2) s ORDER BY x DESC LIMIT 3;",
            ),
            int_rows(&[5, 4, 3])
        );
        // Scalar / IN / EXISTS subqueries bind the whole chain.
        assert_eq!(
            select(&mut db, "SELECT (SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x DESC LIMIT 1);"),
            vec![vec![V::Integer(5)]]
        );
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 WHERE x IN (SELECT x FROM a1 UNION SELECT x FROM a2) ORDER BY x;"),
            int_rows(&[1, 3, 5])
        );
        // EXISTS must see the second branch (its only row lives there).
        assert_eq!(
            select(
                &mut db,
                "SELECT x FROM a1 WHERE EXISTS (SELECT x FROM a2 WHERE x = 99 UNION ALL SELECT x FROM a2 WHERE x = 4) ORDER BY x;",
            ),
            int_rows(&[1, 3, 5])
        );
    }

    #[test]
    fn union_in_cte_and_view() {
        let mut db = union_db();
        // CTE body with a UNION binds the whole chain.
        assert_eq!(
            select(
                &mut db,
                "WITH c AS (SELECT x FROM a1 UNION ALL SELECT x FROM a2) SELECT x FROM c ORDER BY x DESC LIMIT 3;",
            ),
            int_rows(&[5, 4, 3])
        );
        // The head's WITH is visible to every UNION branch (DuckDB semantics).
        assert_eq!(
            select(&mut db, "WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION ALL SELECT x FROM c;"),
            vec![vec![V::Integer(1)], vec![V::Integer(1)]]
        );
        // View definition validates + stores the full chain; view use binds it.
        db.execute("CREATE VIEW uv AS SELECT x FROM a1 UNION ALL SELECT x FROM a2;").unwrap();
        assert_eq!(select(&mut db, "SELECT x FROM uv ORDER BY x DESC LIMIT 3;"), int_rows(&[5, 4, 3]));
        // Chain clauses in the view definition apply per use.
        db.execute("CREATE VIEW uv2 AS SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x DESC LIMIT 3;")
            .unwrap();
        assert_eq!(select(&mut db, "SELECT x FROM uv2;"), int_rows(&[5, 4, 3]));
        // Clauses before a UNION are rejected at CREATE VIEW validation too.
        assert!(db
            .execute("CREATE VIEW bad AS SELECT x FROM a1 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a2;")
            .is_err());
    }

    #[test]
    fn union_type_unification_through_wrapper() {
        let mut db = union_db();
        db.execute("CREATE TABLE a3 (y BIGINT);").unwrap();
        db.execute("INSERT INTO a3 VALUES (2), (4);").unwrap();
        // INTEGER UNION ALL BIGINT unifies to BIGINT through a derived table.
        assert_eq!(
            select(&mut db, "SELECT * FROM (SELECT x FROM a1 UNION ALL SELECT y FROM a3) s ORDER BY x;"),
            vec![
                vec![V::Bigint(1)],
                vec![V::Bigint(2)],
                vec![V::Bigint(3)],
                vec![V::Bigint(4)],
                vec![V::Bigint(5)],
            ]
        );
    }

    /// ORDER BY expressions that are not projected: chain-level clauses bind
    /// over the whole set-op output, and the hidden key must not leak into the
    /// derived schema (names or types).
    #[test]
    fn union_chain_order_by_hidden_expressions() {
        let mut db = union_db();
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY -x LIMIT 2;"),
            int_rows(&[5, 4])
        );
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x + 1 DESC LIMIT 2;"),
            int_rows(&[5, 4])
        );
        let (names, types) = select_meta(
            &mut db,
            "SELECT * FROM (SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY -x) s;",
        );
        assert_eq!(names, vec!["x".to_string()]);
        assert_eq!(types, vec![DataType::Integer]);
    }

    /// Clauses written before a UNION (head or middle branch) are Parse errors
    /// with a message that says where the clause belongs.
    #[test]
    fn union_pre_clauses_are_parse_errors() {
        let mut db = union_db();
        let err = db
            .execute("SELECT x FROM a1 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a2;")
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Parse(_)), "{err:?}");
        assert!(err.to_string().contains("before UNION"), "{err}");

        let err = db
            .execute("SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a1;")
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Parse(_)), "{err:?}");
        assert!(err.to_string().contains("before UNION"), "{err}");
    }

    /// Only the left branch's output names are visible to chain-level ORDER BY.
    /// DuckDB resolves right-branch aliases here, but standard SQL exposes only
    /// the left branch, so we keep the error (a silently wrong result before
    /// wave 18 was worse).
    #[test]
    fn union_right_branch_alias_is_not_visible() {
        let mut db = union_db();
        let err = db
            .execute("SELECT x AS n FROM a1 UNION ALL SELECT x AS m FROM a2 ORDER BY m;")
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Catalog(_)), "{err:?}");
        assert!(err.to_string().contains("no column"), "{err}");
    }

    /// Filter pushdown distributes over a union in a derived table; surviving
    /// rows keep the unified (widened) type.
    #[test]
    fn union_filter_pushdown_through_derived_type_widening() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ti (x INTEGER);").unwrap();
        db.execute("INSERT INTO ti VALUES (1), (3);").unwrap();
        db.execute("CREATE TABLE td (x DOUBLE);").unwrap();
        db.execute("INSERT INTO td VALUES (2.0), (3.0);").unwrap();
        let rows = select(
            &mut db,
            "SELECT * FROM (SELECT x FROM ti UNION ALL SELECT x FROM td) s WHERE x = 3;",
        );
        assert_eq!(rows, vec![vec![V::Double(3.0)], vec![V::Double(3.0)]]);
        let (names, types) =
            select_meta(&mut db, "SELECT * FROM (SELECT x FROM ti UNION ALL SELECT x FROM td) s;");
        assert_eq!(names, vec!["x".to_string()]);
        assert_eq!(types, vec![DataType::Double]);
    }

    /// LIMIT-only hoisting over three branches, and a chain mixing UNION with
    /// UNION ALL (left-associative) plus trailing ORDER BY/LIMIT.
    #[test]
    fn union_three_branch_limit_and_mixed_setops() {
        let mut db = union_db();
        // LIMIT alone keeps the first rows of the concatenation.
        assert_eq!(
            select(
                &mut db,
                "SELECT x FROM a1 UNION ALL SELECT x FROM a2 UNION ALL SELECT x FROM a1 LIMIT 2;",
            ),
            int_rows(&[1, 3])
        );
        // (a1 UNION a2) UNION ALL a1 → {1,2,3,4,5} ∪ {1,3,5}; DESC top 3 has 5 twice.
        assert_eq!(
            select(
                &mut db,
                "SELECT x FROM a1 UNION SELECT x FROM a2 UNION ALL SELECT x FROM a1 ORDER BY x DESC LIMIT 3;",
            ),
            int_rows(&[5, 5, 4])
        );
    }

    /// UNION (distinct) dedupes across branches before chain ORDER BY/LIMIT.
    #[test]
    fn union_distinct_cross_branch_dedup_with_order() {
        let mut db = union_db();
        db.execute("CREATE TABLE a3 (x INTEGER);").unwrap();
        db.execute("INSERT INTO a3 VALUES (3), (5), (7);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT x FROM a1 UNION SELECT x FROM a3 ORDER BY x DESC LIMIT 2;"),
            int_rows(&[7, 5])
        );
    }

    /// A CTE column list renames the whole set operation's output; verified
    /// against DuckDB (5, 4).
    #[test]
    fn union_cte_column_list() {
        let mut db = union_db();
        assert_eq!(
            select(
                &mut db,
                "WITH c(a) AS (SELECT x FROM a1 UNION ALL SELECT x FROM a2) SELECT a FROM c ORDER BY a DESC LIMIT 2;",
            ),
            int_rows(&[5, 4])
        );
    }

    /// Branch arity mismatches fail the union bind (scalar subquery and CTE).
    #[test]
    fn union_arity_errors() {
        let mut db = union_db();
        let err = db
            .execute("SELECT (SELECT x FROM a1 UNION ALL SELECT x, x FROM a2);")
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Execution(_)), "{err:?}");
        let err = db
            .execute("WITH c AS (SELECT x FROM a1 UNION ALL SELECT x, x FROM a2) SELECT x FROM c;")
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Execution(_)), "{err:?}");
    }

    /// WITH inside WITH (unions at both levels) and a CTE referenced only by
    /// the middle branch of a three-branch chain.
    #[test]
    fn union_nested_with_and_middle_branch_cte() {
        let mut db = union_db();
        assert_eq!(
            select(
                &mut db,
                "WITH o AS (WITH i AS (SELECT x FROM a1 UNION ALL SELECT x FROM a2) SELECT x FROM i) SELECT x FROM o ORDER BY x;",
            ),
            int_rows(&[1, 2, 3, 4, 5])
        );
        // c (a1) is visible to the middle branch even though the head is a2.
        assert_eq!(
            select(
                &mut db,
                "WITH c AS (SELECT x FROM a1) SELECT x FROM a2 UNION ALL SELECT x FROM c UNION ALL SELECT x FROM a2 ORDER BY x;",
            ),
            int_rows(&[1, 2, 2, 3, 4, 4, 5])
        );
    }

    #[test]
    fn views_create_use_drop() {
        let mut db = uv_db();
        db.execute("CREATE VIEW evens AS SELECT x FROM a WHERE x % 2 = 0;").unwrap();
        assert_eq!(select(&mut db, "SELECT * FROM evens;"), vec![vec![V::Integer(2)]]);
        // Views compose with joins, CTEs, and aliases.
        let rows = select(
            &mut db,
            "WITH w AS (SELECT * FROM evens) SELECT x, y FROM w JOIN b ON w.x = b.y;",
        );
        assert_eq!(rows, vec![vec![V::Integer(2), V::Bigint(2)]]);
        // Column aliases.
        db.execute("CREATE VIEW named(n) AS SELECT x FROM a WHERE x = 1;").unwrap();
        assert_eq!(select(&mut db, "SELECT n FROM named;"), vec![vec![V::Integer(1)]]);
        // Name clashes + dupes rejected.
        assert!(db.execute("CREATE VIEW a AS SELECT 1;").is_err());
        assert!(db.execute("CREATE TABLE evens (x INTEGER);").is_err());
        assert!(db.execute("CREATE VIEW evens AS SELECT 2;").is_err());
        // Use-after-drop of the base table errors (rebound per use).
        db.execute("DROP TABLE b;").unwrap();
        assert!(db.execute("CREATE VIEW vb AS SELECT y FROM b;").is_err());
        db.execute("DROP VIEW evens;").unwrap();
        assert!(db.execute("SELECT * FROM evens;").is_err());
        assert!(db.execute("DROP VIEW evens;").is_err());
        assert!(db.execute("DROP TABLE missing;").is_err());
    }

    #[test]
    fn cast_coalesce_like() {
        let mut db = uv_db();
        let rows = select(
            &mut db,
            "SELECT CAST(x AS BIGINT), CAST(x AS DOUBLE) FROM a WHERE x = 1;",
        );
        assert_eq!(rows, vec![vec![V::Bigint(1), V::Double(1.0)]]);
        // 'one' does not parse as INTEGER.
        assert!(db.execute("SELECT CAST(s AS INTEGER) FROM a WHERE x = 1;").is_err());
        assert!(db.execute("SELECT CAST(x AS DATE) FROM a;").is_err());
        let rows = select(&mut db, "SELECT COALESCE(x, 99), COALESCE(s, 'dflt') FROM a ORDER BY 1;");
        assert_eq!(rows[0], vec![V::Integer(1), V::Varchar("one".to_string())]);
        assert_eq!(rows[2], vec![V::Integer(99), V::Varchar("nil".to_string())]);
        let rows = select(&mut db, "SELECT s FROM a WHERE s LIKE 'o%' ORDER BY s;");
        assert_eq!(
            rows,
            vec![vec![V::Varchar("one".to_string())]]
        );
        let rows = select(&mut db, "SELECT s FROM a WHERE s NOT LIKE '%o%' ORDER BY s;");
        assert_eq!(rows, vec![vec![V::Varchar("nil".to_string())]]);
        // LIKE is NULL-propagating; mixed types rejected.
        assert_eq!(select(&mut db, "SELECT CAST(NULL AS VARCHAR) LIKE 'a';"), vec![vec![V::Null]]);
        assert!(db.execute("SELECT x LIKE '1' FROM a;").is_err());
    }

    // --- phase 15: CASE ---

    fn case_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id INTEGER, name VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'ann'), (2, 'bob'), (3, NULL);").unwrap();
        db
    }

    #[test]
    fn case_searched_and_simple() {
        let mut db = case_db();
        let rows = select(
            &mut db,
            "SELECT id, CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END AS label
             FROM t ORDER BY id;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Integer(1), V::Varchar("one".to_string())],
                vec![V::Integer(2), V::Varchar("two".to_string())],
                vec![V::Integer(3), V::Varchar("other".to_string())],
            ]
        );
        let rows = select(&mut db, "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t ORDER BY id;");
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("one".to_string())],
                vec![V::Varchar("two".to_string())],
                vec![V::Null],
            ]
        );
    }

    #[test]
    fn case_nulls_and_nesting() {
        let mut db = case_db();
        // NULL conditions never match; NULL results pass through.
        let rows = select(
            &mut db,
            "SELECT CASE WHEN name IS NULL THEN 'anon' WHEN id > 100 THEN NULL ELSE name END FROM t ORDER BY id;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("ann".to_string())],
                vec![V::Varchar("bob".to_string())],
                vec![V::Varchar("anon".to_string())],
            ]
        );
        // Nested CASE + mixed numeric widths unify to DOUBLE.
        let rows = select(
            &mut db,
            "SELECT CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 2.5 ELSE 0 END FROM t ORDER BY id;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Double(1.0)], vec![V::Double(2.5)], vec![V::Double(0.0)]]
        );
        let rows = select(
            &mut db,
            "SELECT CASE WHEN id > 1 THEN CASE WHEN id > 2 THEN 'big' ELSE 'mid' END ELSE 'small' END FROM t ORDER BY id;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("small".to_string())],
                vec![V::Varchar("mid".to_string())],
                vec![V::Varchar("big".to_string())],
            ]
        );
        // Non-boolean conditions + incompatible results rejected.
        assert!(db.execute("SELECT CASE WHEN id THEN 1 END FROM t;").is_err());
        assert!(db.execute("SELECT CASE WHEN true THEN 1 ELSE 'x' END FROM t;").is_err());
    }

    #[test]
    fn case_in_clauses() {
        let mut db = case_db();
        // CASE in WHERE, GROUP BY, aggregates, and ORDER BY position.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM t WHERE CASE WHEN id = 1 THEN true ELSE false END;"),
            vec![vec![V::Bigint(1)]]
        );
        let rows = select(
            &mut db,
            "SELECT CASE WHEN id <= 2 THEN 'low' ELSE 'high' END AS grp, COUNT(*) FROM t GROUP BY grp ORDER BY grp;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("high".to_string()), V::Bigint(1)],
                vec![V::Varchar("low".to_string()), V::Bigint(2)],
            ]
        );
        let rows = select(
            &mut db,
            "SELECT SUM(CASE WHEN id = 3 THEN 10 ELSE 1 END) FROM t;",
        );
        assert_eq!(rows, vec![vec![V::Bigint(12)]]);
    }

    // --- wave 17: sort tuning (byte-arena keys + top-N over SORT) ---

    /// Insert `n` rows in batches (one statement per 1000 rows keeps parsing
    /// cheap). `row(i)` produces the SQL value list for insertion index `i`.
    fn insert_rows(db: &mut PinkDb, table: &str, n: usize, row: impl Fn(usize) -> String) {
        for batch in 0..n.div_ceil(1000) {
            let mut sql = format!("INSERT INTO {table} VALUES ");
            let lo = batch * 1000;
            let hi = (lo + 1000).min(n);
            for i in lo..hi {
                if i > lo {
                    sql.push(',');
                }
                sql.push_str(&row(i));
            }
            sql.push(';');
            db.execute(&sql).unwrap();
        }
    }

    /// `ORDER BY` + `LIMIT` over tie groups: 5000 rows (3 chunks), limit 2.
    /// The top-N result must be the full sort's first 2 rows, with ties
    /// broken by *input order* — the ids are permuted so an id-based
    /// tiebreak would pick different rows.
    #[test]
    fn order_limit_ties_match_full_sort_and_keep_input_order() {
        const N: usize = 5000;
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER, id INTEGER);").unwrap();
        insert_rows(&mut db, "t", N, |i| {
            let a = (i % 4) as i32;
            let b = ((i / 4) % 3) as i32;
            let c = ((i / 12) % 5) as i32;
            let id = ((i * 37) % N) as i32;
            format!("({a}, {b}, {c}, {id})")
        });
        // Independent expectation: smallest (a,b,c), then earliest insertion.
        let mut expected_all: Vec<(i32, i32, i32, usize)> = (0..N)
            .map(|i| ((i % 4) as i32, ((i / 4) % 3) as i32, ((i / 12) % 5) as i32, i))
            .collect();
        expected_all.sort_by_key(|&(a, b, c, i)| (a, b, c, i));
        let expected: Vec<Vec<V>> = expected_all[..2]
            .iter()
            .map(|&(a, b, c, i)| {
                vec![
                    V::Integer(a),
                    V::Integer(b),
                    V::Integer(c),
                    V::Integer(((i * 37) % N) as i32),
                ]
            })
            .collect();
        let topn = select(&mut db, "SELECT a, b, c, id FROM t ORDER BY a, b, c LIMIT 2;");
        assert_eq!(topn, expected);
        let full = select(&mut db, "SELECT a, b, c, id FROM t ORDER BY a, b, c;");
        assert_eq!(topn, full[..2].to_vec());
        // Winning group (0,0,0) first occurs at insertion indices 0 and 60;
        // the permuted ids prove the tiebreak is input order, not id order.
        assert_eq!(topn[0][3], V::Integer(0));
        assert_eq!(topn[1][3], V::Integer(((60 * 37) % N) as i32));
    }

    /// Multi-key mixed ASC/DESC top-N (byte arena) must equal the full sort
    /// truncated to the same limit, NULLS LAST in both directions.
    #[test]
    fn order_limit_mixed_directions_matches_full_sort() {
        const N: usize = 5000;
        const LIMIT: usize = 25; // 25 × 16 = 400 ≤ 5000 → top-N path
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b INTEGER, c VARCHAR, id INTEGER);").unwrap();
        insert_rows(&mut db, "t", N, |i| {
            let a = (i % 7) as i32;
            let b = ((i / 7) % 11) as i32;
            let c = match i % 3 {
                0 => "NULL".to_string(),
                1 => "'x'".to_string(),
                _ => "'yy'".to_string(),
            };
            format!("({a}, {b}, {c}, {i})")
        });
        let topn = select(
            &mut db,
            "SELECT a, b, c, id FROM t ORDER BY a ASC, b DESC, c ASC LIMIT 25;",
        );
        let full = select(&mut db, "SELECT a, b, c, id FROM t ORDER BY a ASC, b DESC, c ASC;");
        assert_eq!(topn.len(), LIMIT);
        assert_eq!(topn, full[..LIMIT].to_vec());
    }

    /// The `limit × 16 <= n` routing boundary: `n == limit*16` takes the
    /// top-N path, one row less takes the generic full-sort+truncate path;
    /// both must produce identical output (and the fallback must not
    /// re-evaluate the sort input).
    #[test]
    fn limit_over_sort_routing_boundary() {
        for n in [32usize, 31, 33] {
            let mut db = PinkDb::new();
            db.execute("CREATE TABLE t (a INTEGER, id INTEGER);").unwrap();
            insert_rows(&mut db, "t", n, |i| format!("({}, {i})", (i * 5) % 9));
            let limited = select(&mut db, "SELECT a, id FROM t ORDER BY a LIMIT 2;");
            let full = select(&mut db, "SELECT a, id FROM t ORDER BY a;");
            assert_eq!(limited, full[..2].to_vec(), "n={n}");
        }
    }

    /// DESC strings with NULLs and a prefix pair ('a' vs 'ab'): the top-N
    /// limit is smaller than every group, NULLs stay last, and a limit larger
    /// than the table (generic path) returns everything sorted.
    #[test]
    fn order_limit_desc_strings_and_oversized_limit() {
        const N: usize = 5000;
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE s (k VARCHAR, id INTEGER);").unwrap();
        insert_rows(&mut db, "s", N, |i| match i % 3 {
            0 => format!("(NULL, {i})"),
            1 => format!("('a', {i})"),
            _ => format!("('ab', {i})"),
        });
        let topn = select(&mut db, "SELECT k, id FROM s ORDER BY k DESC LIMIT 2;");
        assert_eq!(
            topn,
            vec![
                vec![V::Varchar("ab".to_string()), V::Integer(2)],
                vec![V::Varchar("ab".to_string()), V::Integer(5)],
            ]
        );
        let full = select(&mut db, "SELECT k, id FROM s ORDER BY k DESC;");
        assert_eq!(topn, full[..2].to_vec());
        // NULLS LAST, and every 'ab' beats every 'a' (prefix pair).
        assert!(full.last().unwrap()[0].is_null());
        let last_ab = full.iter().rposition(|r| r[0] == V::Varchar("ab".to_string())).unwrap();
        let first_a = full.iter().position(|r| r[0] == V::Varchar("a".to_string())).unwrap();
        assert!(last_ab < first_a);
        // Limit larger than the table: the generic full-sort path still
        // returns everything in sorted order.
        let big = select(&mut db, "SELECT k, id FROM s ORDER BY k DESC LIMIT 10000;");
        assert_eq!(big, full);
    }

    /// A NaN float key cannot be encoded (`compare_values` makes NaN Equal to
    /// everything), so sort/top-N must refuse the byte arena and fall back to
    /// the legacy `Value` comparator — same result as that path, no panic.
    #[test]
    fn order_nan_float_uses_legacy_comparator() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE f (d DOUBLE, id INTEGER);").unwrap();
        db.execute(
            "INSERT INTO f VALUES (3.0, 1), (1.0, 2), (NULL, 3), (2.0, 4), (1.5, 5), (0.0 / 0.0, 6);",
        )
        .unwrap();
        // Encoding (and top-N over it) must refuse the NaN key...
        let cx = Cx::new(&db);
        let t = db.table("f").unwrap();
        let chunks: Vec<DataChunk> = (0..t.num_chunks()).map(|i| t.scan_chunk(i).unwrap()).collect();
        let keys = vec![(BoundExpr::Column { index: 0, dtype: DataType::Double }, false)];
        let keyed = eval_sort_keys(&cx, &chunks, &keys).unwrap();
        assert!(encode_sort_keys(&chunks, &keyed, &keys).is_none());
        assert!(top_n(&cx, &chunks, &keys, 1).unwrap().is_none());
        // ... so ORDER BY runs the legacy comparator path unchanged.
        let rows = select(&mut db, "SELECT d, id FROM f ORDER BY d;");
        let legacy = sort_legacy(&chunks, &keys, &keyed).unwrap();
        let mut expected = Vec::new();
        for chunk in &legacy {
            for i in 0..chunk.num_rows() {
                expected.push(chunk.columns.iter().map(|c| c.get(i)).collect::<Vec<V>>());
            }
        }
        // `Value::Double(NaN) != Value::Double(NaN)`, so compare Debug forms.
        let debug_rows = |rows: &[Vec<V>]| rows.iter().map(|r| format!("{r:?}")).collect::<Vec<_>>();
        assert_eq!(debug_rows(&rows), debug_rows(&expected));
        assert_eq!(rows.len(), 6);
        assert!(rows.iter().any(|r| matches!(r[0], V::Double(x) if x.is_nan())));
    }

    /// End-to-end `LIMIT` over `ORDER BY` with a NaN key: top-N refuses the
    /// byte encoding, so `eval`'s fallback full-sort branch runs on the
    /// already-evaluated sort input and truncates to the same rows.
    #[test]
    fn order_nan_limit_end_to_end() {
        const N: usize = 100;
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE f (d DOUBLE, id INTEGER);").unwrap();
        insert_rows(&mut db, "f", N, |i| {
            if i == 7 {
                "(0.0 / 0.0, 7)".to_string()
            } else {
                format!("({}.0, {i})", i % 13)
            }
        });
        // 2 × 16 = 32 ≤ 100 → the top-N path is attempted and refuses.
        let limited = select(&mut db, "SELECT d, id FROM f ORDER BY d LIMIT 2;");
        let full = select(&mut db, "SELECT d, id FROM f ORDER BY d;");
        assert_eq!(limited.len(), 2);
        // `Value::Double(NaN) != Value::Double(NaN)`, so compare Debug forms.
        let debug_rows = |rows: &[Vec<V>]| rows.iter().map(|r| format!("{r:?}")).collect::<Vec<_>>();
        assert_eq!(debug_rows(&limited), debug_rows(&full[..2]));
    }

    /// Differential check: the byte-arena path must be byte-identical to the
    /// legacy `Value` comparator across dtypes, directions, NULLs, prefix
    /// strings ("" / "a" / "ab"), −0.0, and multi-chunk inputs — including
    /// the output chunking.
    #[test]
    fn arena_sort_matches_legacy_comparator_on_mixed_data() {
        const N: usize = 5000;
        let mut db = PinkDb::new();
        db.execute(
            "CREATE TABLE m (i INTEGER, g BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN, dt DATE, ts TIMESTAMP);",
        )
        .unwrap();
        insert_rows(&mut db, "m", N, |i| {
            let i_col = if i % 13 == 0 { "NULL".to_string() } else { format!("{}", (i % 5) as i32 - 2) };
            let g = format!("{}", ((i * 7) % 11) as i64 - 5);
            let d = if i % 17 == 0 { "-0.0".to_string() } else { format!("{:.1}", ((i % 9) as f64) - 4.0) };
            let s = match i % 5 {
                0 => "''",
                1 => "'a'",
                2 => "'ab'",
                3 => "NULL",
                _ => "'b'",
            };
            let b = if i % 3 == 0 { "true" } else { "false" };
            let dt = if i % 7 == 0 { "NULL".to_string() } else { format!("DATE '2024-01-01' + {}", (i % 6) as i32) };
            let ts = if i % 11 == 0 { "NULL".to_string() } else { format!("TIMESTAMP '2024-01-01 00:00:0{}.5'", i % 10) };
            format!("({i_col}, {g}, {d}, {s}, {b}, {dt}, {ts})")
        });
        let cx = Cx::new(&db);
        let t = db.table("m").unwrap();
        let chunks: Vec<DataChunk> = (0..t.num_chunks()).map(|i| t.scan_chunk(i).unwrap()).collect();
        let asc = |index: usize, dtype: DataType| (BoundExpr::Column { index, dtype }, false);
        let desc = |index: usize, dtype: DataType| (BoundExpr::Column { index, dtype }, true);
        let cases: Vec<Vec<(BoundExpr, bool)>> = vec![
            vec![asc(3, DataType::Varchar)],
            vec![desc(3, DataType::Varchar)],
            vec![asc(2, DataType::Double)],
            vec![desc(2, DataType::Double)],
            // Prefix string first: a naive raw-byte encoding would let the
            // second key decide comparisons the string key already settled.
            vec![asc(3, DataType::Varchar), asc(0, DataType::Integer)],
            vec![desc(3, DataType::Varchar), desc(1, DataType::Bigint)],
            vec![asc(4, DataType::Boolean), asc(3, DataType::Varchar)],
            vec![desc(0, DataType::Integer), desc(3, DataType::Varchar), asc(2, DataType::Double)],
            vec![asc(5, DataType::Date)],
            vec![desc(6, DataType::Timestamp)],
        ];
        for keys in cases {
            let keyed = eval_sort_keys(&cx, &chunks, &keys).unwrap();
            assert!(encode_sort_keys(&chunks, &keyed, &keys).is_some(), "{keys:?}");
            let arena = sort(&cx, &chunks, &keys).unwrap();
            let legacy = sort_legacy(&chunks, &keys, &keyed).unwrap();
            assert_eq!(arena, legacy, "{keys:?}");
        }
    }

    /// Byte-arena sort: VARCHAR keys with embedded NULs and multi-byte UTF-8
    /// (the shift+terminator encoding must match `str` order), plus an empty
    /// chunk in the middle of the input (chunk mapping and stability).
    #[test]
    fn sort_embedded_nul_high_bytes_and_empty_chunk() {
        let mk = |vals: &[Option<&str>]| {
            let mut v = Vector::new_empty(DataType::Varchar);
            for s in vals {
                match s {
                    Some(s) => v.push_valid(V::Varchar((*s).to_string())).unwrap(),
                    None => v.push_null().unwrap(),
                }
            }
            DataChunk::new(vec![v]).unwrap()
        };
        let chunks = vec![
            mk(&[Some("b\0x"), Some("a")]),
            mk(&[]),
            mk(&[Some("a\0"), Some("é"), None]),
            mk(&[Some("a\0x"), Some("z")]),
        ];
        let db = PinkDb::new();
        let cx = Cx::new(&db);
        let keys = vec![(BoundExpr::Column { index: 0, dtype: DataType::Varchar }, false)];
        let keyed = eval_sort_keys(&cx, &chunks, &keys).unwrap();
        let arena = sort(&cx, &chunks, &keys).unwrap();
        let legacy = sort_legacy(&chunks, &keys, &keyed).unwrap();
        assert_eq!(arena, legacy);
        let got: Vec<Option<String>> = arena
            .iter()
            .flat_map(|c| {
                (0..c.num_rows()).map(|i| match c.column(0).get(i) {
                    V::Varchar(s) => Some(s),
                    V::Null => None,
                    ref other => panic!("unexpected {other:?}"),
                })
            })
            .collect();
        assert_eq!(
            got,
            vec![
                Some("a".to_string()),
                Some("a\0".to_string()),
                Some("a\0x".to_string()),
                Some("b\0x".to_string()),
                Some("z".to_string()),
                Some("é".to_string()),
                None,
            ]
        );
    }

    /// NULLS LAST in the top-N path for both directions: 200 non-null values
    /// and 4800 NULLs, so a limit of 312 (still ≤ n/16) must include NULLs
    /// and place them after every value.
    #[test]
    fn order_limit_nulls_last_asc_and_desc() {
        const N: usize = 5000;
        const LIMIT: usize = 312; // 312 × 16 = 4992 ≤ 5000 → top-N path
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (v INTEGER, id INTEGER);").unwrap();
        insert_rows(&mut db, "t", N, |i| {
            if i % 25 == 0 {
                format!("({}, {i})", (i / 25) % 10)
            } else {
                format!("(NULL, {i})")
            }
        });
        for dir in ["", "DESC"] {
            let topn = select(&mut db, &format!("SELECT v, id FROM t ORDER BY v {dir} LIMIT {LIMIT};"));
            let full = select(&mut db, &format!("SELECT v, id FROM t ORDER BY v {dir};"));
            assert_eq!(topn.len(), LIMIT);
            assert_eq!(topn, full[..LIMIT].to_vec(), "dir={dir}");
            // 200 non-null rows sort first, the rest of the top-N is NULL.
            assert!(topn[..200].iter().all(|r| !r[0].is_null()), "dir={dir}");
            assert!(topn[200..].iter().all(|r| r[0].is_null()), "dir={dir}");
            let vals: Vec<i32> = topn[..200]
                .iter()
                .map(|r| match r[0] {
                    V::Integer(x) => x,
                    ref other => panic!("dir={dir}: expected INTEGER, got {other:?}"),
                })
                .collect();
            let ordered = vals.windows(2).all(|w| {
                if dir.is_empty() { w[0] <= w[1] } else { w[0] >= w[1] }
            });
            assert!(ordered, "dir={dir}: {vals:?}");
        }
    }

    /// Top-N chunking boundary: limits at/above CHUNK_SIZE must reproduce
    /// the full-sort-then-truncate chunking exactly (full chunks pass
    /// through, a straddling chunk is gathered).
    #[test]
    fn top_n_across_chunk_boundary_matches_truncated_sort() {
        // 40 chunks × 2048 rows built directly (no SQL parsing).
        let dtype = DataType::Integer;
        let mut chunks = Vec::new();
        for c in 0..40usize {
            let mut v = Vector::new_empty(dtype);
            for r in 0..CHUNK_SIZE {
                v.push_valid(V::Integer(((c * 37 + r * 11) % 1000) as i32)).unwrap();
            }
            chunks.push(DataChunk::new(vec![v]).unwrap());
        }
        let db = PinkDb::new();
        let cx = Cx::new(&db);
        let keys = vec![(BoundExpr::Column { index: 0, dtype }, false)];
        let full = sort(&cx, &chunks, &keys).unwrap();
        for limit in [1usize, 2, 4096, 5000] {
            assert!(limit * 16 <= 40 * CHUNK_SIZE, "limit={limit} must use top-N");
            let topn = top_n(&cx, &chunks, &keys, limit).unwrap().unwrap();
            let expected = truncate_output(
                Output { chunks: full.clone(), names: vec![], types: vec![] },
                limit,
            );
            assert_eq!(topn, expected.chunks, "limit={limit}");
        }
        // Exact multiples of CHUNK_SIZE stay full chunks.
        let topn = top_n(&cx, &chunks, &keys, 4096).unwrap().unwrap();
        assert_eq!(topn.len(), 2);
        assert!(topn.iter().all(|c| c.num_rows() == CHUNK_SIZE));
    }

    // --- wave 19 phase 2: fused join → aggregate ---

    /// Two small tables with duplicate keys (INTEGER = BIGINT), NULL join
    /// keys on both sides, NULL measures, and two group regions.
    fn fused_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE fa (id INTEGER, region VARCHAR, x INTEGER);").unwrap();
        db.execute("CREATE TABLE fb (id BIGINT, region VARCHAR, total DOUBLE);").unwrap();
        db.execute(
            "INSERT INTO fa VALUES
             (1, 'west', 3), (2, 'east', 5), (1, 'west', 7), (NULL, 'west', 11), (2, 'east', NULL);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO fb VALUES
             (1, 'W1', 10.5), (2, 'E1', NULL), (1, 'W2', 20.0), (3, 'X', 1.0),
             (NULL, 'N', 2.5), (2, 'E2', 4.25);",
        )
        .unwrap();
        db
    }

    /// Bind + optimize `sql` exactly like `PinkDb::execute` does.
    fn bound_plan(db: &PinkDb, sql: &str) -> LogicalPlan {
        let stmts = crate::sql::parse_statements(sql).unwrap();
        crate::optimizer::optimize(db, crate::sql::bind(db, &stmts[0], db.current_db()).unwrap())
    }

    /// Descend to the topmost `Aggregate` (through Project/Cte wrappers).
    fn top_aggregate(plan: &LogicalPlan) -> &LogicalPlan {
        match plan {
            LogicalPlan::Aggregate { .. } => plan,
            LogicalPlan::Project { input, .. } => top_aggregate(input),
            LogicalPlan::Cte { query, .. } => top_aggregate(query),
            other => panic!("no aggregate in plan: {other:?}"),
        }
    }

    /// Execute the top aggregate of `sql` through the fusion helper and
    /// assert it fused and produced exactly the materialized path's output
    /// (chunk for chunk, order included).
    fn assert_fused_matches_materialized(db: &PinkDb, sql: &str) {
        let plan = bound_plan(db, sql);
        let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
            unreachable!()
        };
        let cx = Cx::new(db);
        let fused = fused_join_aggregate(&cx, input, keys, aggs)
            .unwrap()
            .unwrap_or_else(|| panic!("expected a fusible plan: {sql}"));
        let inner = eval(&cx, input).unwrap();
        let materialized = aggregate(&cx, &inner.chunks, keys, aggs).unwrap();
        // Rows (in order) + chunk boundaries; validity-mask padding can
        // differ because the COUNT(*) fast path builds a tight vector while
        // `aggregate()` preallocates CHUNK_SIZE — same observable rows.
        let rows = |chunks: &[DataChunk]| -> Vec<Vec<Value>> {
            let mut rows = Vec::new();
            for c in chunks {
                for i in 0..c.num_rows() {
                    rows.push(c.columns.iter().map(|col| col.get(i)).collect());
                }
            }
            rows
        };
        // Fused partial boundaries are per left chunk, the materialized ones
        // per joined output chunk, so float SUM/AVG partials can sum in a
        // different order and differ by a few ulps (accepted behavior).
        // Compare Double cells with a small relative tolerance (absolute
        // floor 1e-12, NaN == NaN); every other type stays exact.
        let close = |a: &Value, b: &Value| -> bool {
            match (a, b) {
                (Value::Double(x), Value::Double(y)) => {
                    (x.is_nan() && y.is_nan())
                        || x == y
                        || (x - y).abs() <= 1e-12_f64.max(1e-9 * x.abs().max(y.abs()))
                }
                _ => a == b,
            }
        };
        let (fr, mr) = (rows(&fused.chunks), rows(&materialized));
        assert_eq!(fr.len(), mr.len(), "row count: {sql}");
        for (i, (a, b)) in fr.iter().zip(&mr).enumerate() {
            assert_eq!(a.len(), b.len(), "width row {i}: {sql}");
            for (j, (x, y)) in a.iter().zip(b).enumerate() {
                assert!(
                    close(x, y),
                    "row {i} col {j}: fused {x:?} vs materialized {y:?}: {sql}"
                );
            }
        }
        assert_eq!(
            fused.chunks.iter().map(|c| c.num_rows()).collect::<Vec<_>>(),
            materialized.iter().map(|c| c.num_rows()).collect::<Vec<_>>(),
            "{sql}"
        );
        assert_eq!(fused.names, agg_output_schema(keys, aggs).0, "{sql}");
        assert_eq!(fused.types, agg_output_schema(keys, aggs).1, "{sql}");
    }

    #[test]
    fn fused_count_star_over_join_counts_matches() {
        let mut db = fused_db();
        // Direct Aggregate-over-Join shape and the Project-over-Join shape
        // (derived table) both count the 8 matched pairs.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;"),
            vec![vec![V::Bigint(8)]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*) FROM (SELECT fa.region FROM fa JOIN fb ON fa.id = fb.id) s;"
            ),
            vec![vec![V::Bigint(8)]]
        );
        // Empty right side: zero matches through the fast path.
        db.execute("CREATE TABLE fbe (id BIGINT, total DOUBLE);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM fa JOIN fbe ON fa.id = fbe.id;"),
            vec![vec![V::Bigint(0)]]
        );
    }

    #[test]
    fn fused_group_by_matches_fallback_and_order() {
        let mut db = fused_db();
        let sql = "SELECT fa.region, COUNT(*), COUNT(fa.x), SUM(fa.x), AVG(fa.x), MIN(fa.x), MAX(fa.x)
                   FROM fa JOIN fb ON fa.id = fb.id";
        let fused = select(&mut db, &format!("{sql} GROUP BY fa.region;"));
        // Same semantics with a mixed-side predicate above the join: the
        // optimizer keeps it there, so the materialized path runs.
        let fallback = select(&mut db, &format!("{sql} WHERE fa.id = fb.id GROUP BY fa.region;"));
        assert_eq!(fused, fallback);
        // First-seen group order (west before east); NULL args skipped by
        // COUNT/SUM/AVG/MIN/MAX.
        assert_eq!(
            fused,
            vec![
                vec![
                    V::Varchar("west".to_string()),
                    V::Bigint(4),
                    V::Bigint(4),
                    V::Bigint(20),
                    V::Double(5.0),
                    V::Integer(3),
                    V::Integer(7),
                ],
                vec![
                    V::Varchar("east".to_string()),
                    V::Bigint(4),
                    V::Bigint(2),
                    V::Bigint(10),
                    V::Double(5.0),
                    V::Integer(5),
                    V::Integer(5),
                ],
            ]
        );
    }

    #[test]
    fn fused_compound_single_side_args() {
        let mut db = fused_db();
        let sql = "SELECT COUNT(*), SUM(fb.total * 2), SUM(fa.x * 3) FROM fa JOIN fb ON fa.id = fb.id";
        let fused = select(&mut db, &format!("{sql};"));
        let fallback = select(&mut db, &format!("{sql} WHERE fa.id = fb.id;"));
        assert_eq!(fused, fallback);
        // 8 matches; totals sum 69.5 → ×2 = 139.0; x sums 30 → ×3 = 90.
        assert_eq!(fused, vec![vec![V::Bigint(8), V::Double(139.0), V::Bigint(90)]]);
    }

    #[test]
    fn fused_project_reorders_drops_and_right_keys() {
        let mut db = fused_db();
        // Reordering project: region (left) grouped, t (right) summed.
        let fused = select(
            &mut db,
            "SELECT region, COUNT(*), SUM(t)
             FROM (SELECT fa.region AS region, fb.total AS t FROM fa JOIN fb ON fa.id = fb.id) s
             GROUP BY region;",
        );
        let fallback = select(
            &mut db,
            "SELECT region, COUNT(*), SUM(t)
             FROM (SELECT fa.region AS region, fb.total AS t FROM fa JOIN fb ON fa.id = fb.id
                   WHERE fa.id = fb.id) s
             GROUP BY region;",
        );
        assert_eq!(fused, fallback);
        assert_eq!(
            fused,
            vec![
                vec![V::Varchar("west".to_string()), V::Bigint(4), V::Double(61.0)],
                vec![V::Varchar("east".to_string()), V::Bigint(4), V::Double(8.5)],
            ]
        );
        // Dropping project: only the left key survives.
        assert_eq!(
            select(
                &mut db,
                "SELECT region, COUNT(*)
                 FROM (SELECT fa.region AS region FROM fa JOIN fb ON fa.id = fb.id) s
                 GROUP BY region;"
            ),
            vec![
                vec![V::Varchar("west".to_string()), V::Bigint(4)],
                vec![V::Varchar("east".to_string()), V::Bigint(4)],
            ]
        );
        // Right-side group keys: bare column and a shifted scalar function.
        assert_eq!(
            select(
                &mut db,
                "SELECT fb.region, COUNT(*), SUM(fa.x)
                 FROM fa JOIN fb ON fa.id = fb.id GROUP BY fb.region;"
            ),
            vec![
                vec![V::Varchar("W1".to_string()), V::Bigint(2), V::Bigint(10)],
                vec![V::Varchar("W2".to_string()), V::Bigint(2), V::Bigint(10)],
                vec![V::Varchar("E1".to_string()), V::Bigint(2), V::Bigint(5)],
                vec![V::Varchar("E2".to_string()), V::Bigint(2), V::Bigint(5)],
            ]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT upper(fb.region), COUNT(*)
                 FROM fa JOIN fb ON fa.id = fb.id GROUP BY upper(fb.region);"
            ),
            vec![
                vec![V::Varchar("W1".to_string()), V::Bigint(2)],
                vec![V::Varchar("W2".to_string()), V::Bigint(2)],
                vec![V::Varchar("E1".to_string()), V::Bigint(2)],
                vec![V::Varchar("E2".to_string()), V::Bigint(2)],
            ]
        );
    }

    #[test]
    fn fused_string_keys() {
        let mut db = fused_db();
        db.execute("CREATE TABLE fsa (k VARCHAR, v INTEGER);").unwrap();
        db.execute("CREATE TABLE fsb (k VARCHAR, w INTEGER);").unwrap();
        db.execute("INSERT INTO fsa VALUES ('a', 1), ('b', 2), ('a', 3), (NULL, 4);").unwrap();
        db.execute("INSERT INTO fsb VALUES ('a', 10), ('b', 20), ('a', 30), (NULL, 40);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM fsa JOIN fsb ON fsa.k = fsb.k;"),
            vec![vec![V::Bigint(5)]]
        );
        let fused = select(
            &mut db,
            "SELECT fsa.k, COUNT(*), SUM(fsb.w) FROM fsa JOIN fsb ON fsa.k = fsb.k GROUP BY fsa.k;",
        );
        let fallback = select(
            &mut db,
            "SELECT fsa.k, COUNT(*), SUM(fsb.w)
             FROM fsa JOIN fsb ON fsa.k = fsb.k WHERE fsa.k = fsb.k GROUP BY fsa.k;",
        );
        assert_eq!(fused, fallback);
        // 'a': 2 left × 2 right = 4 matches, w sum 80; 'b': 1 match, 20.
        assert_eq!(
            fused,
            vec![
                vec![V::Varchar("a".to_string()), V::Bigint(4), V::Bigint(80)],
                vec![V::Varchar("b".to_string()), V::Bigint(1), V::Bigint(20)],
            ]
        );
    }

    #[test]
    fn fused_timestamp_and_double_keys() {
        let mut db = PinkDb::new();
        // TIMESTAMP keys take the int path (i64 micros, NULLs skipped).
        db.execute("CREATE TABLE tl (ts TIMESTAMP, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE tr (ts TIMESTAMP, rv INTEGER);").unwrap();
        db.execute(
            "INSERT INTO tl VALUES
             (TIMESTAMP '2024-01-15 10:00:00', 1),
             (TIMESTAMP '2024-01-15 10:00:00.000001', 2), (NULL, 3);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO tr VALUES
             (TIMESTAMP '2024-01-15 10:00:00.000001', 10),
             (TIMESTAMP '2024-01-15 10:00:00', 11);",
        )
        .unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM tl JOIN tr ON tl.ts = tr.ts;"),
            vec![vec![V::Bigint(2)]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT tl.lv, COUNT(*), MIN(tr.rv)
                 FROM tl JOIN tr ON tl.ts = tr.ts GROUP BY tl.lv;"
            ),
            vec![
                vec![V::Integer(1), V::Bigint(1), V::Integer(11)],
                vec![V::Integer(2), V::Bigint(1), V::Integer(10)],
            ]
        );
        // DOUBLE keys take the byte path; -0.0 and 0.0 are one key.
        db.execute("CREATE TABLE dl (k DOUBLE, lv INTEGER);").unwrap();
        db.execute("CREATE TABLE dr (k DOUBLE, rv INTEGER);").unwrap();
        db.execute("INSERT INTO dl VALUES (1.5, 1), (2.5, 2), (-0.0, 3), (NULL, 4);").unwrap();
        db.execute("INSERT INTO dr VALUES (1.5, 10), (2.5, 20), (0.0, 30), (NULL, 40);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM dl JOIN dr ON dl.k = dr.k;"),
            vec![vec![V::Bigint(3)]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT dl.lv, COUNT(*) FROM dl JOIN dr ON dl.k = dr.k GROUP BY dl.lv;"
            ),
            vec![
                vec![V::Integer(1), V::Bigint(1)],
                vec![V::Integer(2), V::Bigint(1)],
                vec![V::Integer(3), V::Bigint(1)],
            ]
        );
    }

    #[test]
    fn fused_empty_right_side_single_empty_group() {
        let mut db = fused_db();
        db.execute("CREATE TABLE fbe (id BIGINT, total DOUBLE);").unwrap();
        // No keys: one row of zero/NULL results, like `aggregate()` over ∅.
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), COUNT(total), SUM(total), AVG(total), MIN(total), MAX(total)
                 FROM fa JOIN fbe ON fa.id = fbe.id;"
            ),
            vec![vec![
                V::Bigint(0),
                V::Bigint(0),
                V::Null,
                V::Null,
                V::Null,
                V::Null
            ]]
        );
        // With keys: zero rows (one empty chunk).
        assert_eq!(
            select(
                &mut db,
                "SELECT fa.region, COUNT(*) FROM fa JOIN fbe ON fa.id = fbe.id GROUP BY fa.region;"
            ),
            Vec::<Vec<V>>::new()
        );
        // Empty left side (`WHERE false` pushes below the join): same.
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM fa JOIN fb ON fa.id = fb.id WHERE false;"),
            vec![vec![V::Bigint(0)]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT fa.region, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id
                 WHERE false GROUP BY fa.region;"
            ),
            Vec::<Vec<V>>::new()
        );
    }

    #[test]
    fn mixed_side_group_key_uses_fallback() {
        let mut db = fused_db();
        // Mixed-side key: fusion rejects statically; the materialized path
        // groups fa.id + fb.id (BIGINT) in first-seen order.
        assert_eq!(
            select(
                &mut db,
                "SELECT fa.id + fb.id, COUNT(*)
                 FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.id + fb.id;"
            ),
            vec![vec![V::Bigint(2), V::Bigint(4)], vec![V::Bigint(4), V::Bigint(4)]]
        );
    }

    #[test]
    fn fused_agg_in_derived_table_and_cte() {
        let mut db = fused_db();
        // The grouped aggregate over the join sits inside the derived table.
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), SUM(c)
                 FROM (SELECT fa.region, COUNT(*) AS c
                       FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region) s;"
            ),
            vec![vec![V::Bigint(2), V::Bigint(8)]]
        );
        // Same shape inside a CTE.
        assert_eq!(
            select(
                &mut db,
                "WITH g AS (SELECT fa.region AS r, COUNT(*) AS c
                            FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region)
                 SELECT r, c FROM g;"
            ),
            vec![
                vec![V::Varchar("west".to_string()), V::Bigint(4)],
                vec![V::Varchar("east".to_string()), V::Bigint(4)],
            ]
        );
    }

    #[test]
    fn fused_multi_chunk_order_matches_fallback() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ml (k INTEGER, v INTEGER);").unwrap();
        db.execute("CREATE TABLE mr (k INTEGER, w INTEGER);").unwrap();
        // 3000 rows each = 2 chunks per side; k cycles 0..6 on the left and
        // 0..6 ×3 (coprime stride) on the right — duplicate keys interleave
        // across chunks on both sides.
        insert_rows(&mut db, "ml", 3000, |i| format!("({}, {i})", i % 7));
        insert_rows(&mut db, "mr", 3000, |i| format!("({}, {i})", (i * 3) % 7));
        let fused = select(
            &mut db,
            "SELECT ml.k, COUNT(*), SUM(ml.v), SUM(mr.w)
             FROM ml JOIN mr ON ml.k = mr.k GROUP BY ml.k;",
        );
        let fallback = select(
            &mut db,
            "SELECT ml.k, COUNT(*), SUM(ml.v), SUM(mr.w)
             FROM ml JOIN mr ON ml.k = mr.k WHERE ml.k = mr.k GROUP BY ml.k;",
        );
        assert_eq!(fused, fallback);
        assert_eq!(fused.len(), 7);
        // First-seen group order is 0, 1, ..., 6.
        for (i, row) in fused.iter().enumerate() {
            assert_eq!(row[0], V::Integer(i as i32), "group {i}");
        }
        // Independent count: left rows per key × right rows per key.
        for k in 0..7i32 {
            let l = (0..3000).filter(|i| i % 7 == k).count() as i64;
            let r = (0..3000).filter(|i| (i * 3) % 7 == k).count() as i64;
            assert_eq!(fused[k as usize][1], V::Bigint(l * r), "group {k}");
        }
    }

    #[test]
    fn fused_expansion_spans_right_chunks() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE xl (k INTEGER, v INTEGER);").unwrap();
        db.execute("CREATE TABLE xr (k INTEGER, w INTEGER);").unwrap();
        db.execute("INSERT INTO xl VALUES (1, 5);").unwrap();
        // 5000 right rows across 3 chunks; 2500 match key 1 (even i).
        insert_rows(&mut db, "xr", 5000, |i| format!("({}, {i})", 1 + i % 2));
        let fused = select(
            &mut db,
            "SELECT xl.k, COUNT(*), SUM(xr.w) FROM xl JOIN xr ON xl.k = xr.k GROUP BY xl.k;",
        );
        let fallback = select(
            &mut db,
            "SELECT xl.k, COUNT(*), SUM(xr.w)
             FROM xl JOIN xr ON xl.k = xr.k WHERE xl.k = xr.k GROUP BY xl.k;",
        );
        assert_eq!(fused, fallback);
        assert_eq!(fused, vec![vec![V::Integer(1), V::Bigint(2500), V::Bigint(6_247_500)]]);
    }

    #[test]
    fn fused_join_aggregate_equals_materialized() {
        let db = fused_db();
        // Direct Aggregate-over-Join shapes.
        assert_fused_matches_materialized(
            &db,
            "SELECT fa.region, COUNT(*), SUM(fa.x), AVG(fb.total), MIN(fb.total), MAX(fb.total)
             FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region;",
        );
        // Compound left key (including the NULL+1 group) and right scalar
        // key. DOUBLE arithmetic is total; integer `+` can overflow and is
        // covered by `fusion_rejects_value_erroring_shapes`.
        assert_fused_matches_materialized(
            &db,
            "SELECT fa.x + 1.5, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.x + 1.5;",
        );
        assert_fused_matches_materialized(
            &db,
            "SELECT upper(fb.region), COUNT(*), SUM(fb.total)
             FROM fa JOIN fb ON fa.id = fb.id GROUP BY upper(fb.region);",
        );
        // Project-over-Join: reorder + drop + right-side arg through the project.
        assert_fused_matches_materialized(
            &db,
            "SELECT region, COUNT(*), SUM(t)
             FROM (SELECT fa.region AS region, fb.total AS t
                   FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        );
        // Global aggregate with a compound right-side arg (keys-empty path).
        assert_fused_matches_materialized(
            &db,
            "SELECT COUNT(*), SUM(fb.total * 2) FROM fa JOIN fb ON fa.id = fb.id;",
        );
        // Keys-only aggregate (no accumulators).
        assert_fused_matches_materialized(
            &db,
            "SELECT fa.region FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region;",
        );
        // COUNT(*) fast path.
        assert_fused_matches_materialized(
            &db,
            "SELECT COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;",
        );
    }

    #[test]
    fn fused_agg_over_filter_pushed_below_join() {
        let mut db = fused_db();
        // A right-side WHERE is pushed below the join by the optimizer, so
        // the aggregate input is still Project-over-Join and fuses (the
        // filter is not *between* the aggregate and the join).
        let fused = select(
            &mut db,
            "SELECT COUNT(*), SUM(fb.total)
             FROM fa JOIN fb ON fa.id = fb.id WHERE fb.total > 5;",
        );
        // Adding a mixed-side no-op conjunct keeps a Filter above the join,
        // forcing the materialized path; semantics are identical.
        let fallback = select(
            &mut db,
            "SELECT COUNT(*), SUM(fb.total)
             FROM fa JOIN fb ON fa.id = fb.id WHERE fa.id = fb.id AND fb.total > 5;",
        );
        assert_eq!(fused, fallback);
        // Matches only the (id=1, total>5) right rows: 2 left × 2 right.
        assert_eq!(fused, vec![vec![V::Bigint(4), V::Double(61.0)]]);
    }

    #[test]
    fn fusion_rejects_blocked_shapes() {
        let db = fused_db();
        let cx = Cx::new(&db);
        let blocked = [
            // Mixed-side group key.
            "SELECT fa.id + fb.id, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.id + fb.id;",
            // Mixed-side aggregate argument.
            "SELECT SUM(fa.x + fb.total) FROM fa JOIN fb ON fa.id = fb.id;",
            // Filter between the aggregate and the join.
            "SELECT fa.region, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id
             WHERE fa.id = fb.id GROUP BY fa.region;",
            // Distinct between the aggregate and the join.
            "SELECT COUNT(*) FROM (SELECT DISTINCT fa.region FROM fa JOIN fb ON fa.id = fb.id) s;",
        ];
        for sql in blocked {
            let plan = bound_plan(&db, sql);
            let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
                unreachable!()
            };
            assert!(
                fused_join_aggregate(&cx, input, keys, aggs).unwrap().is_none(),
                "should not fuse: {sql}"
            );
        }
    }

    // --- wave 19 follow-up: value-dependent errors on unmatched rows ---

    #[test]
    fn fusion_falls_back_on_value_dependent_error() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t1 (k INTEGER, x INTEGER);").unwrap();
        db.execute("CREATE TABLE t2 (k INTEGER, q INTEGER);").unwrap();
        db.execute("INSERT INTO t1 VALUES (1, 10);").unwrap();
        db.execute("INSERT INTO t2 VALUES (1, 2), (2, 0);").unwrap();
        let sql = "SELECT SUM(100 / q) FROM t1 JOIN t2 ON t1.k = t2.k;";
        // `100 / q` pre-evaluated over the whole right input would divide by
        // zero on the never-matched q=0 row, so the helper must reject the
        // shape...
        let plan = bound_plan(&db, sql);
        let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
            unreachable!()
        };
        let cx = Cx::new(&db);
        assert!(fused_join_aggregate(&cx, input, keys, aggs).unwrap().is_none());
        // ...and the query must keep materialized semantics: only the matched
        // (q=2) row feeds the sum, so no error and 100/2 = 50.
        assert_eq!(select(&mut db, sql), vec![vec![V::Double(50.0)]]);
    }

    #[test]
    fn fusion_rejects_value_erroring_shapes() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE va (k INTEGER, x INTEGER);").unwrap();
        db.execute("CREATE TABLE vb (k INTEGER, qty INTEGER, s VARCHAR, a INTEGER, b INTEGER);")
            .unwrap();
        db.execute("INSERT INTO va VALUES (1, 10);").unwrap();
        // Key 2 rows never match; they hold the erroring values.
        db.execute("INSERT INTO vb VALUES (1, 2, '3', 4, 2), (2, 0, 'nope', 5, 0);").unwrap();
        let cx = Cx::new(&db);
        let blocked = [
            // Integer division by zero on an unmatched right row.
            "SELECT SUM(100 / vb.qty) FROM va JOIN vb ON va.k = vb.k;",
            // VARCHAR→INTEGER parse failure on an unmatched right row.
            "SELECT SUM(CAST(vb.s AS INTEGER)) FROM va JOIN vb ON va.k = vb.k;",
            // Modulo by zero on an unmatched group-key row.
            "SELECT vb.a % vb.b, COUNT(*) FROM va JOIN vb ON va.k = vb.k GROUP BY vb.a % vb.b;",
            // Integer arithmetic, negation and abs can overflow (DuckDB
            // semantics), so they are value-dependent errors too.
            "SELECT vb.a + 1, COUNT(*) FROM va JOIN vb ON va.k = vb.k GROUP BY vb.a + 1;",
            "SELECT SUM(-vb.a) FROM va JOIN vb ON va.k = vb.k;",
            "SELECT SUM(abs(vb.a)) FROM va JOIN vb ON va.k = vb.k;",
        ];
        for sql in blocked {
            let plan = bound_plan(&db, sql);
            let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
                unreachable!()
            };
            assert!(
                fused_join_aggregate(&cx, input, keys, aggs).unwrap().is_none(),
                "should not fuse: {sql}"
            );
        }
        // Same queries through the engine run the materialized path and only
        // see the matched (qty=2, s='3') row.
        assert_eq!(
            select(&mut db, "SELECT SUM(100 / vb.qty) FROM va JOIN vb ON va.k = vb.k;"),
            vec![vec![V::Double(50.0)]]
        );
        assert_eq!(
            select(&mut db, "SELECT SUM(CAST(vb.s AS INTEGER)) FROM va JOIN vb ON va.k = vb.k;"),
            vec![vec![V::Bigint(3)]]
        );
    }

    #[test]
    fn fusion_keeps_total_shapes() {
        let db = fused_db();
        // Each shape must still fuse (the helper asserts `Some`) and produce
        // exactly the materialized output.
        assert_fused_matches_materialized(
            &db,
            "SELECT COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;",
        );
        assert_fused_matches_materialized(
            &db,
            "SELECT SUM(fb.total * 2) FROM fa JOIN fb ON fa.id = fb.id;",
        );
        assert_fused_matches_materialized(
            &db,
            "SELECT SUM(abs(fb.total)) FROM fa JOIN fb ON fa.id = fb.id;",
        );
        // `upper(region)` reached through a Project over the join.
        assert_fused_matches_materialized(
            &db,
            "SELECT region, COUNT(*) FROM (SELECT upper(fa.region) AS region
             FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        );
    }

    // --- wave 19 review follow-up: error equivalence, key coverage, edges ---

    /// Unused erroring project expressions must force the materialized path
    /// so the engine errors exactly like the fallback; a total unused
    /// expression stays fusible and equivalent.
    #[test]
    fn fused_unused_project_expr_error_equivalence() {
        let mut db = fused_db();
        let erroring = [
            "SELECT region, COUNT(*) FROM (SELECT fa.region AS region,
                    CAST(fb.region AS INTEGER) AS junk
             FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
            "SELECT region, COUNT(*) FROM (SELECT fa.region AS region, 1/0 AS junk
             FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        ];
        // Static check: the fusion helper rejects the erroring shapes.
        let cx = Cx::new(&db);
        for sql in erroring {
            let plan = bound_plan(&db, sql);
            let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
                unreachable!()
            };
            assert!(
                fused_join_aggregate(&cx, input, keys, aggs).unwrap().is_none(),
                "erroring projection must not fuse: {sql}"
            );
        }
        // Engine check: both variants error, fused exactly like materialized.
        for sql in erroring {
            let fallback = sql.replace(
                "FROM fa JOIN fb ON fa.id = fb.id) s",
                "FROM fa JOIN fb ON fa.id = fb.id WHERE fa.id = fb.id) s",
            );
            assert_ne!(fallback, sql);
            assert!(db.execute(sql).is_err(), "fused variant must error: {sql}");
            assert!(
                db.execute(&fallback).is_err(),
                "materialized variant must error: {fallback}"
            );
        }
        // A total unused projection expression is safe to skip: fusion
        // proceeds and the engine matches the materialized variant.
        let sql = "SELECT region, COUNT(*) FROM (SELECT fa.region AS region,
                          upper(fb.region) AS junk
                   FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;";
        assert_fused_matches_materialized(&db, sql);
        let fused = select(&mut db, sql);
        let fallback = sql.replace(
            "FROM fa JOIN fb ON fa.id = fb.id) s",
            "FROM fa JOIN fb ON fa.id = fb.id WHERE fa.id = fb.id) s",
        );
        assert_eq!(fused, select(&mut db, &fallback));
        assert_eq!(
            fused,
            vec![
                vec![V::Varchar("west".to_string()), V::Bigint(4)],
                vec![V::Varchar("east".to_string()), V::Bigint(4)],
            ]
        );
    }

    /// Fused partial boundaries (per left chunk) differ from the
    /// materialized ones (per joined output chunk), so float `SUM`/`AVG` can
    /// differ by a few ulps; the differential helper's tolerance must absorb
    /// that on a multi-chunk join.
    #[test]
    fn fused_float_partial_boundaries_within_tolerance() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE fl (k INTEGER, d DOUBLE);").unwrap();
        db.execute("CREATE TABLE fr (k INTEGER);").unwrap();
        // 3000 left rows (2 chunks) × 3 right rows = 9000 matches on one
        // key; fractional doubles make the partial split visible.
        insert_rows(&mut db, "fl", 3000, |i| format!("(1, {:?})", (i % 10) as f64 / 7.0));
        insert_rows(&mut db, "fr", 3, |_| "(1)".to_string());
        assert_fused_matches_materialized(
            &db,
            "SELECT SUM(fl.d), AVG(fl.d) FROM fl JOIN fr ON fl.k = fr.k;",
        );
        // Both engine paths agree within the documented tolerance (the mixed
        // WHERE keeps the fallback on the materialized path).
        let fused = select(&mut db, "SELECT SUM(fl.d), AVG(fl.d) FROM fl JOIN fr ON fl.k = fr.k;");
        let materialized = select(
            &mut db,
            "SELECT SUM(fl.d), AVG(fl.d) FROM fl JOIN fr ON fl.k = fr.k WHERE fl.k = fr.k;",
        );
        for (a, b) in fused[0].iter().zip(&materialized[0]) {
            let (V::Double(x), V::Double(y)) = (a, b) else {
                panic!("expected doubles: {a:?} vs {b:?}")
            };
            assert!(
                (x - y).abs() <= 1e-12_f64.max(1e-9 * x.abs().max(y.abs())),
                "float drift: fused {x:?} vs materialized {y:?}"
            );
        }
    }

    /// Cross-type numeric join keys: INTEGER = DOUBLE and DOUBLE = INTEGER
    /// take the byte path (canonicalized to DOUBLE) and must group and count
    /// exactly like the materialized pipeline.
    #[test]
    fn fused_int_double_cross_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ija (k INTEGER, v INTEGER);").unwrap();
        db.execute("CREATE TABLE ijb (k DOUBLE, w INTEGER);").unwrap();
        db.execute("INSERT INTO ija VALUES (1, 10), (2, 20), (2, 21), (NULL, 30);").unwrap();
        db.execute("INSERT INTO ijb VALUES (2.0, 1), (1.0, 2), (2.0, 3), (3.0, 4);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM ija JOIN ijb ON ija.k = ijb.k;"),
            vec![vec![V::Bigint(5)]]
        );
        assert_fused_matches_materialized(
            &db,
            "SELECT ija.k, COUNT(*), SUM(ijb.w) FROM ija JOIN ijb ON ija.k = ijb.k GROUP BY ija.k;",
        );
        let fused = select(
            &mut db,
            "SELECT ija.k, COUNT(*), SUM(ijb.w) FROM ija JOIN ijb ON ija.k = ijb.k GROUP BY ija.k;",
        );
        let fallback = select(
            &mut db,
            "SELECT ija.k, COUNT(*), SUM(ijb.w)
             FROM ija JOIN ijb ON ija.k = ijb.k WHERE ija.k = ijb.k GROUP BY ija.k;",
        );
        assert_eq!(fused, fallback);
        // Key 1: one match (w=2); key 2: 2×2 matches (1+3+1+3).
        assert_eq!(
            fused,
            vec![
                vec![V::Integer(1), V::Bigint(1), V::Bigint(2)],
                vec![V::Integer(2), V::Bigint(4), V::Bigint(8)],
            ]
        );
        // Mirrored DOUBLE = INTEGER: groups keyed by the left (DOUBLE) side,
        // first-seen order 2.0 then 1.0.
        assert_fused_matches_materialized(
            &db,
            "SELECT ijb.k, COUNT(*), SUM(ija.v) FROM ijb JOIN ija ON ijb.k = ija.k GROUP BY ijb.k;",
        );
        let fused = select(
            &mut db,
            "SELECT ijb.k, COUNT(*), SUM(ija.v) FROM ijb JOIN ija ON ijb.k = ija.k GROUP BY ijb.k;",
        );
        assert_eq!(
            fused,
            vec![
                vec![V::Double(2.0), V::Bigint(4), V::Bigint(82)],
                vec![V::Double(1.0), V::Bigint(1), V::Bigint(10)],
            ]
        );
    }

    /// Extreme i64 keys (MIN/MAX, spelled as expressions because a literal
    /// cannot hold i64::MIN) and DATE keys (int path, raw day payloads) must
    /// group and count correctly through fusion.
    #[test]
    fn fused_extreme_and_date_keys() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ela (k BIGINT, v INTEGER);").unwrap();
        db.execute("CREATE TABLE elb (k BIGINT, w INTEGER);").unwrap();
        db.execute(
            "INSERT INTO ela VALUES
             ((-9223372036854775807 - 1), 1), (9223372036854775807, 2), (0, 3), (NULL, 4);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO elb VALUES
             (9223372036854775807, 10), ((-9223372036854775807 - 1), 20),
             (9223372036854775807, 30), (NULL, 40);",
        )
        .unwrap();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*) FROM ela JOIN elb ON ela.k = elb.k;"),
            vec![vec![V::Bigint(3)]]
        );
        assert_fused_matches_materialized(
            &db,
            "SELECT ela.k, COUNT(*), SUM(elb.w) FROM ela JOIN elb ON ela.k = elb.k GROUP BY ela.k;",
        );
        let fused = select(
            &mut db,
            "SELECT ela.k, COUNT(*), SUM(elb.w) FROM ela JOIN elb ON ela.k = elb.k GROUP BY ela.k;",
        );
        let fallback = select(
            &mut db,
            "SELECT ela.k, COUNT(*), SUM(elb.w)
             FROM ela JOIN elb ON ela.k = elb.k WHERE ela.k = elb.k GROUP BY ela.k;",
        );
        assert_eq!(fused, fallback);
        // First-seen: i64::MIN (1 match, w=20), then i64::MAX (2 matches, 40).
        assert_eq!(
            fused,
            vec![
                vec![V::Bigint(i64::MIN), V::Bigint(1), V::Bigint(20)],
                vec![V::Bigint(i64::MAX), V::Bigint(2), V::Bigint(40)],
            ]
        );
        // DATE keys ride the same int path (raw day payloads).
        db.execute("CREATE TABLE dta (d DATE, v INTEGER);").unwrap();
        db.execute("CREATE TABLE dtb (d DATE, w INTEGER);").unwrap();
        db.execute(
            "INSERT INTO dta VALUES
             (DATE '2024-01-15', 1), (DATE '1969-12-31', 2), (DATE '2024-01-15', 3), (NULL, 4);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO dtb VALUES
             (DATE '2024-01-15', 10), (DATE '2000-01-01', 20), (DATE '2024-01-15', 30);",
        )
        .unwrap();
        assert_fused_matches_materialized(
            &db,
            "SELECT dta.d, COUNT(*), SUM(dtb.w) FROM dta JOIN dtb ON dta.d = dtb.d GROUP BY dta.d;",
        );
        let fused = select(
            &mut db,
            "SELECT dta.d, COUNT(*), SUM(dtb.w) FROM dta JOIN dtb ON dta.d = dtb.d GROUP BY dta.d;",
        );
        // 2 left × 2 right matches on 2024-01-15 → 4 rows, w sum 80; the
        // unmatched 1969/2000 keys vanish.
        assert_eq!(fused.len(), 1);
        assert!(matches!(fused[0][0], V::Date(_)), "date key: {:?}", fused[0][0]);
        assert_eq!(fused[0][1], V::Bigint(4));
        assert_eq!(fused[0][2], V::Bigint(80));
        assert_eq!(
            select(
                &mut db,
                "SELECT dta.d, COUNT(*), SUM(dtb.w) FROM dta JOIN dtb ON dta.d = dtb.d
                 WHERE dta.d = dtb.d GROUP BY dta.d;"
            ),
            fused
        );
    }

    /// BOOLEAN, comparison, AND, CASE, COALESCE, LIKE, YEAR, and HOUR group
    /// keys all fuse and match the materialized pipeline.
    #[test]
    fn fused_scalar_group_keys_match_materialized() {
        let mut db = fused_db();
        let keys = [
            "fa.x > 4",
            "fb.total > 10",
            "fa.x > 4 AND fa.x < 10",
            "CASE WHEN fa.x > 4 THEN 'big' ELSE 'small' END",
            "COALESCE(fa.x, 0)",
            "fa.region LIKE 'w%'",
        ];
        for k in keys {
            let sql = format!(
                "SELECT {k}, COUNT(*), SUM(fa.x) FROM fa JOIN fb ON fa.id = fb.id GROUP BY {k};"
            );
            assert_fused_matches_materialized(&db, &sql);
            let fused = select(&mut db, &sql);
            let fallback = select(
                &mut db,
                &format!(
                    "SELECT {k}, COUNT(*), SUM(fa.x) FROM fa JOIN fb ON fa.id = fb.id
                     WHERE fa.id = fb.id GROUP BY {k};"
                ),
            );
            assert_eq!(fused, fallback, "key: {k}");
            assert!(!fused.is_empty(), "key produced no groups: {k}");
        }
        // YEAR/HOUR keys need DATE/TIMESTAMP inputs.
        db.execute("CREATE TABLE gka (d DATE, ts TIMESTAMP, k INTEGER, v INTEGER);").unwrap();
        db.execute("CREATE TABLE gkb (k INTEGER);").unwrap();
        db.execute(
            "INSERT INTO gka VALUES
             (DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00', 1, 1),
             (DATE '2024-02-20', TIMESTAMP '2024-01-15 11:30:00', 1, 2),
             (DATE '2023-12-31', TIMESTAMP '2024-01-16 10:00:00', 2, 3);",
        )
        .unwrap();
        db.execute("INSERT INTO gkb VALUES (1), (1), (2);").unwrap();
        for k in ["year(gka.d)", "hour(gka.ts)"] {
            let sql = format!(
                "SELECT {k}, COUNT(*), SUM(gka.v) FROM gka JOIN gkb ON gka.k = gkb.k GROUP BY {k};"
            );
            assert_fused_matches_materialized(&db, &sql);
            let fused = select(&mut db, &sql);
            let fallback = select(
                &mut db,
                &format!(
                    "SELECT {k}, COUNT(*), SUM(gka.v) FROM gka JOIN gkb ON gka.k = gkb.k
                     WHERE gka.k = gkb.k GROUP BY {k};"
                ),
            );
            assert_eq!(fused, fallback, "key: {k}");
            assert!(!fused.is_empty(), "key produced no groups: {k}");
        }
    }

    /// Both join sides empty through fusion: a global aggregate yields one
    /// zero/NULL row, a grouped aggregate zero rows, and the
    /// Project-over-Join shape stays equivalent to materialized execution.
    #[test]
    fn fused_both_sides_empty() {
        let mut db = fused_db();
        db.execute("CREATE TABLE lea (id INTEGER, x INTEGER);").unwrap();
        db.execute("CREATE TABLE leb (id BIGINT, total DOUBLE);").unwrap();
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), COUNT(lea.x), SUM(lea.x), AVG(lea.x),
                        MIN(leb.total), MAX(leb.total)
                 FROM lea JOIN leb ON lea.id = leb.id;"
            ),
            vec![vec![
                V::Bigint(0),
                V::Bigint(0),
                V::Null,
                V::Null,
                V::Null,
                V::Null
            ]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT lea.id, COUNT(*) FROM lea JOIN leb ON lea.id = leb.id GROUP BY lea.id;"
            ),
            Vec::<Vec<V>>::new()
        );
        assert_fused_matches_materialized(
            &db,
            "SELECT g, COUNT(*), SUM(t) FROM (SELECT lea.id AS g, leb.total AS t
             FROM lea JOIN leb ON lea.id = leb.id) s GROUP BY g;",
        );
    }

    /// Aggregate over Project over Join whose left side is itself a
    /// Project-over-Join: the outer join fuses, the deeper chain falls back
    /// to materialized execution, and results match the all-materialized
    /// variant.
    #[test]
    fn fused_nested_project_over_join_left() {
        let mut db = fused_db();
        let sql = "SELECT g, COUNT(*), SUM(v) FROM (SELECT x.id AS g, x.x AS v
                   FROM (SELECT fa.id AS id, fa.x AS x FROM fa JOIN fb ON fa.id = fb.id) x
                   JOIN fb f2 ON x.id = f2.id) s GROUP BY g;";
        let fallback = "SELECT g, COUNT(*), SUM(v) FROM (SELECT x.id AS g, x.x AS v
                        FROM (SELECT fa.id AS id, fa.x AS x FROM fa JOIN fb ON fa.id = fb.id) x
                        JOIN fb f2 ON x.id = f2.id WHERE x.id = f2.id) s GROUP BY g;";
        assert_fused_matches_materialized(&db, sql);
        assert_eq!(select(&mut db, sql), select(&mut db, fallback));
    }

    /// Binder dedup: `COUNT(*), COUNT(*)` reaches the fused fast path with
    /// one aggregate but must still emit the user-visible two columns;
    /// `COUNT(*), COUNT(1)` takes the generic fused path (two aggregates).
    #[test]
    fn fused_duplicate_count_columns() {
        let mut db = fused_db();
        assert_eq!(
            select(&mut db, "SELECT COUNT(*), COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;"),
            vec![vec![V::Bigint(8), V::Bigint(8)]]
        );
        assert_eq!(
            select(
                &mut db,
                "SELECT COUNT(*), COUNT(1), SUM(fa.x) FROM fa JOIN fb ON fa.id = fb.id;"
            ),
            vec![vec![V::Bigint(8), V::Bigint(8), V::Bigint(30)]]
        );
        // Fast-path shape: dedup leaves exactly one aggregate and the helper
        // emits exactly one column (the outer Project duplicates it).
        let sql = "SELECT COUNT(*), COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;";
        let plan = bound_plan(&db, sql);
        let LogicalPlan::Aggregate { input, keys, aggs } = top_aggregate(&plan) else {
            unreachable!()
        };
        assert_eq!(aggs.len(), 1);
        let cx = Cx::new(&db);
        let fused = fused_join_aggregate(&cx, input, keys, aggs).unwrap().unwrap();
        assert_eq!(fused.chunks.len(), 1);
        assert_eq!(fused.chunks[0].columns.len(), 1);
        assert_eq!(fused.names.len(), 1);
    }

    // ---------- multi-database (embedded session) ----------

    /// `USE` applies to every later statement in the same string; unqualified
    /// names resolve against the session's database.
    #[test]
    fn embedded_use_scopes_following_statements() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE DATABASE db1; \
             CREATE TABLE main.t (a INTEGER); \
             CREATE TABLE db1.t (a INTEGER);",
        )
        .unwrap();
        let results = db
            .execute("USE db1; INSERT INTO t VALUES (1); USE main; INSERT INTO t VALUES (2);")
            .unwrap();
        assert_eq!(results[0], QueryResult::Used { db: "db1".to_string() });
        assert_eq!(results[2], QueryResult::Used { db: "main".to_string() });
        assert_eq!(select(&mut db, "SELECT a FROM main.t;"), vec![vec![V::Integer(2)]]);
        assert_eq!(select(&mut db, "SELECT a FROM db1.t;"), vec![vec![V::Integer(1)]]);
        assert_eq!(db.current_db(), "main");
        // Case-insensitive database names.
        db.execute("USE DB1;").unwrap();
        assert_eq!(db.current_db(), "db1");
        assert_eq!(select(&mut db, "SELECT a FROM t;"), vec![vec![V::Integer(1)]]);
    }

    #[test]
    fn embedded_database_ddl_errors_and_session_fallback() {
        let mut db = PinkDb::new();
        // Last database / duplicate / missing.
        assert!(db.execute("DROP DATABASE main;").is_err());
        assert!(db.execute("CREATE DATABASE db1; CREATE DATABASE DB1;").is_err());
        assert!(db.execute("USE nope;").is_err());
        db.execute("CREATE DATABASE db2;").unwrap();
        // Dropping the current database switches to main.
        db.execute("USE db2; DROP DATABASE db2;").unwrap();
        assert_eq!(db.current_db(), "main");
        // Dropping main while db1 remains switches to the first remaining db.
        db.execute("DROP DATABASE main;").unwrap();
        assert_eq!(db.current_db(), "db1");
        assert_eq!(db.list_databases(), vec!["db1".to_string()]);
        // CREATE/DROP DATABASE inside a transaction is refused.
        db.execute("BEGIN;").unwrap();
        let err = db.execute("CREATE DATABASE x;").unwrap_err();
        assert!(err.to_string().contains("not allowed inside a transaction"), "{err}");
        // LOAD EXTENSION is process-level too; the refusal happens before the
        // path is even looked at.
        let err = db.execute("LOAD EXTENSION '/does/not/exist.dylib';").unwrap_err();
        assert_eq!(err.to_string(), "LOAD EXTENSION is not allowed inside a transaction");
        db.execute("ROLLBACK;").unwrap();
    }

    /// A view's body resolves in the database the view lives in, even when
    /// the querying session is elsewhere.
    #[test]
    fn embedded_views_are_per_database() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE DATABASE db1; \
             CREATE TABLE main.src (v INTEGER); \
             CREATE TABLE db1.src (v INTEGER); \
             INSERT INTO main.src VALUES (1); \
             INSERT INTO db1.src VALUES (10), (20); \
             CREATE VIEW db1.v AS SELECT v FROM src; \
             CREATE VIEW main.v AS SELECT v FROM src;",
        )
        .unwrap();
        assert_eq!(select(&mut db, "SELECT * FROM v;"), vec![vec![V::Integer(1)]]);
        assert_eq!(
            select(&mut db, "SELECT * FROM db1.v ORDER BY v;"),
            vec![vec![V::Integer(10)], vec![V::Integer(20)]]
        );
        // Views are not persisted by save: still session-local.
        assert_eq!(db.list_views(), vec!["v".to_string()]);
        assert_eq!(db.list_views_in("db1"), vec!["v".to_string()]);
    }

    /// DROP DATABASE leaves no views behind.
    #[test]
    fn drop_database_removes_its_views() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE DATABASE db1; CREATE TABLE db1.t (a INTEGER); \
             CREATE VIEW db1.v AS SELECT * FROM t;",
        )
        .unwrap();
        db.execute("DROP DATABASE db1;").unwrap();
        assert!(db.execute("SELECT * FROM db1.v;").is_err());
        assert!(db.execute("SELECT * FROM db1.t;").is_err());
    }
}
