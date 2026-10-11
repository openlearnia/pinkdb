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

use aggregate::aggregate;
use expr::{coerce_to, eval_expr, select_true};
use rayon::prelude::*;
use window::eval_window;

use crate::plan::{LogicalPlan, agg_output_schema, compare_values, win_output_schema};
use crate::sql::binder::{BoundExpr, common_numeric};
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

/// Execute a bound plan.
pub fn run(db: &mut PinkDb, plan: &LogicalPlan) -> Result<QueryResult> {
    match plan {
        LogicalPlan::CreateTable { name, schema } => {
            db.create_table(name, schema.clone())?;
            Ok(QueryResult::Created)
        }
        LogicalPlan::Insert { table, rows } => {
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
            let schema = db.table(table)?.schema();
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
            let t = db.table_mut(table)?;
            for values in staged {
                t.append_row(values)?;
            }
            Ok(QueryResult::Inserted { count })
        }
        LogicalPlan::Update { table, sets, predicate } => {
            // Phase 1: evaluate predicates + SET exprs over current chunks
            // (subqueries see pre-mutation state). Phase 2: write back.
            // SET exprs evaluate against original values (simultaneous).
            let mut per_chunk: Vec<Vec<(usize, usize, Value)>>;
            let mut count = 0usize;
            {
                let cx = Cx::new(db);
                let t = cx.db.table(table)?;
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
            let t = db.table_mut(table)?;
            for (ci, writes) in per_chunk.iter().enumerate() {
                t.update_rows(ci, writes)?;
            }
            Ok(QueryResult::Updated { count })
        }
        LogicalPlan::Delete { table, predicate } => {
            // Phase 1: collect per-chunk drop lists. Phase 2: apply from the
            // highest chunk down (emptied chunks are removed, shifting lower
            // indices — descending order keeps them valid).
            let mut drops: Vec<Vec<usize>>;
            {
                let cx = Cx::new(db);
                let t = cx.db.table(table)?;
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
            let t = db.table_mut(table)?;
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
        LogicalPlan::CreateView { name, cols, query } => {
            // Validated at bind time; store the AST for per-use rebinding.
            db.create_view(name, (**query).clone(), cols.clone())?;
            Ok(QueryResult::CreatedView { name: name.clone() })
        }
        LogicalPlan::DropTable { name } => {
            db.drop_table(name)?;
            Ok(QueryResult::DroppedTable { name: name.clone() })
        }
        LogicalPlan::DropView { name } => {
            db.drop_view(name)?;
            Ok(QueryResult::DroppedView { name: name.clone() })
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
        | LogicalPlan::DropView { .. } => Err(PinkDbError::Execution(
            "DDL/DML does not produce rows".to_string(),
        )),
        LogicalPlan::Scan { table } => {
            // CTEs shadow catalog tables (both keyed lowercased).
            if let Some(out) = cx.ctes.get(&table.to_ascii_lowercase()) {
                return Ok(out.clone());
            }
            let t = cx.db.table(table)?;
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
        LogicalPlan::Join { left, right, left_key, right_key, .. } => {
            let l = eval(cx, left)?;
            let r = eval(cx, right)?;
            let mut names = l.names.clone();
            names.extend(r.names.iter().cloned());
            let mut types = l.types.clone();
            types.extend(r.types.iter().copied());
            let chunks = hash_join(cx, &l.chunks, &r.chunks, left_key, right_key, &l.types, &r.types)?;
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            let inner = eval(cx, input)?;
            // Fast path: pure COUNT(*) with no keys counts chunk lengths —
            // no data touched at all.
            if keys.is_empty()
                && !aggs.is_empty()
                && aggs.iter().all(|a| a.func == crate::sql::binder::AggFunc::Count && a.arg.is_none())
            {
                let total: i64 = inner.chunks.iter().map(|c| c.num_rows() as i64).sum();
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
                .map(|chunk| {
                    let cols = exprs
                        .iter()
                        .map(|e| eval_expr(e, chunk, cx))
                        .collect::<Result<Vec<_>>>()?;
                    DataChunk::new(cols)
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

/// Inner hash join: build on the right, probe with the left (preserves
/// left-row order). NULL keys never match on either side.
fn hash_join(
    cx: &Cx,
    left_chunks: &[DataChunk],
    right_chunks: &[DataChunk],
    left_key: &BoundExpr,
    right_key: &BoundExpr,
    left_types: &[DataType],
    right_types: &[DataType],
) -> Result<Vec<DataChunk>> {
    // Build: encoded key → (head, tail) of that key's match chain. The arena
    // holds one `(right_chunk, right_row, next)` node per match in right
    // input order (sentinel `NIL` ends a chain); appending links the new node
    // after the tail. This replaces a small `Vec` allocation per distinct key
    // while keeping one hash lookup per row. (A `(start, len)` slice into a
    // flat arena cannot work in one pass: matches of distinct keys interleave,
    // so a key's rows are not contiguous.) Cross-type numeric keys are
    // canonicalized to their common type first (binder allows `1 = 1.0`);
    // same-type keys skip the deep clone `coerce_to` would do.
    const NIL: u32 = u32::MAX;
    let common = common_numeric(left_key.dtype(), right_key.dtype()).ok();
    let right_rows: usize = right_chunks.iter().map(|c| c.num_rows()).sum();
    if right_rows > u32::MAX as usize {
        return Err(PinkDbError::Execution(
            "hash join right side exceeds u32 rows".to_string(),
        ));
    }
    // Reserve for distinct keys, not rows: a skewed join would otherwise
    // allocate `right_rows` map/arena slots for a handful of keys. Both grow
    // on demand past the cap.
    let reserve = right_rows.min(1 << 20);
    let mut map: FastMap<Vec<u8>, (u32, u32)> =
        FastMap::with_capacity_and_hasher(reserve, Default::default());
    let mut arena: Vec<(u32, u32, u32)> = Vec::with_capacity(reserve);
    let mut buf = Vec::new();
    for (ci, chunk) in right_chunks.iter().enumerate() {
        let kv = eval_expr(right_key, chunk, cx)?;
        let kv = match common {
            Some(t) if kv.dtype() != t => coerce_to(&kv, t)?,
            _ => kv,
        };
        for i in 0..chunk.num_rows() {
            if kv.is_null(i) {
                continue;
            }
            // Borrowed encoding, get-first: hits allocate nothing.
            buf.clear();
            kv.encode_into(&mut buf, i);
            let node = arena.len() as u32;
            match map.get_mut(&buf) {
                Some(slot) => {
                    arena.push((ci as u32, i as u32, NIL));
                    arena[slot.1 as usize].2 = node;
                    slot.1 = node;
                }
                None => {
                    arena.push((ci as u32, i as u32, NIL));
                    map.insert(buf.clone(), (node, node));
                }
            }
        }
    }

    // Probe + assemble, parallel per left chunk: matches push straight from
    // the source chunks into per-partition output vectors (no intermediate
    // location tuples, no serial assembly pass). A partition can exceed
    // CHUNK_SIZE rows, so it emits chunks as they fill; the caller
    // concatenates partitions in order, preserving left-row order.
    let mut dtypes: Vec<DataType> = left_types.to_vec();
    dtypes.extend(right_types.iter().copied());
    let n_left = left_types.len();
    let partitions: Vec<Vec<DataChunk>> = left_chunks
        .par_iter()
        .map(|chunk| -> Result<Vec<DataChunk>> {
            let kv = eval_expr(left_key, chunk, cx)?;
            let kv = match common {
                Some(t) if kv.dtype() != t => coerce_to(&kv, t)?,
                _ => kv,
            };
            let mut finished = Vec::new();
            let mut cur: Vec<Vector> =
                dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
            let mut cur_rows = 0usize;
            let mut buf = Vec::new();
            for i in 0..chunk.num_rows() {
                if kv.is_null(i) {
                    continue;
                }
                buf.clear();
                kv.encode_into(&mut buf, i);
                if let Some(&(mut node, _)) = map.get(&buf) {
                    while node != NIL {
                        let (rci, ri, next) = arena[node as usize];
                        if cur_rows >= CHUNK_SIZE {
                            finished.push(DataChunk::new(std::mem::take(&mut cur))?);
                            cur = dtypes
                                .iter()
                                .map(|t| Vector::with_capacity(*t, CHUNK_SIZE))
                                .collect();
                            cur_rows = 0;
                        }
                        let rchunk = &right_chunks[rci as usize];
                        for (dst, col) in cur.iter_mut().take(n_left).zip(chunk.columns.iter()) {
                            dst.push_from(col, i)?;
                        }
                        for (dst, col) in cur.iter_mut().skip(n_left).zip(rchunk.columns.iter()) {
                            dst.push_from(col, ri as usize)?;
                        }
                        cur_rows += 1;
                        node = next;
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
/// order (the same per-chunk assembly the old `Row` build used).
fn eval_sort_keys(
    cx: &Cx,
    chunks: &[DataChunk],
    keys: &[(BoundExpr, bool)],
) -> Result<Vec<Vec<Vector>>> {
    chunks
        .par_iter()
        .map(|chunk| {
            keys.iter()
                .map(|(k, _)| eval_expr(k, chunk, cx))
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
    keyed: &[Vec<Vector>],
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
                None => dtypes.push(v.dtype()),
                Some(t) if *t == v.dtype() => {}
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
                    if !encode_key_into(&ks[ki], i, *desc, &mut bytes) {
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
    keyed: &[Vec<Vector>],
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
                key: keyed[ci].iter().map(|v| v.get(i)).collect(),
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
        let path = std::env::temp_dir().join("pinkdb_dates.pink");
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
        // NOTE: ORDER BY/LIMIT attach to their own UNION branch in v1, so
        // union tests sort client-side.
        let mut rows = select(&mut db, "SELECT x FROM a WHERE x IS NOT NULL UNION ALL SELECT y FROM b;");
        rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
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
        // UNION (distinct) dedupes, NULLs dedupe together.
        let mut rows = select(&mut db, "SELECT x FROM a UNION SELECT y FROM b;");
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (V::Null, V::Null) => std::cmp::Ordering::Equal,
            (V::Null, _) => std::cmp::Ordering::Greater,
            (_, V::Null) => std::cmp::Ordering::Less,
            (x, y) => x.to_string().cmp(&y.to_string()),
        });
        assert_eq!(rows.len(), 4); // 1, 2, 3, NULL
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
        // Arity + type mismatches.
        assert!(db.execute("SELECT x FROM a UNION SELECT y, t FROM b;").is_err());
        assert!(db.execute("SELECT s FROM a UNION SELECT y FROM b;").is_err());
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
}
