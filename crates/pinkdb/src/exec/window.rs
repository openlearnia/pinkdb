//! Window functions: partition rows, order within partitions, compute per row.
//!
//! Output preserves input row order (and input chunk boundaries) with one
//! appended column per window.
//! Ranking uses the ORDER BY's NULL placement (NULLS LAST by default, both
//! directions); NULL order keys are peers, matching GROUP BY semantics.
//! Aggregate windows share [`Acc`] with hash aggregation; an empty frame
//! yields COUNT → 0, others → NULL.
//!
//! Frames: `ROWS` counts physical rows; `RANGE` counts ORDER BY *value*
//! units (peers move together). The default frame with ORDER BY is DuckDB's
//! `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — peers share the
//! frame and get the same value.

use crate::types::FastMap;
use std::cmp::Ordering;

use super::aggregate::EvalVec as FusedVec;
use super::aggregate::{eval_agg_vec, Acc};
use super::Cx;
use crate::plan::compare_values;
use crate::sql::ast::{FrameMode, SortSpec};
use crate::sql::binder::{BoundWindow, FrameBound, WindowFunc};
use crate::types::{Result, Value};
use crate::vector::{DataChunk, Vector};
use rayon::prelude::*;

/// One input row's order key and argument (its global index is its position
/// in the row list; the partition key is encoded straight from the chunk's
/// evaluated key vectors, so no per-row partition `Value`s exist at all).
struct Row {
    ord: KeyVals,
    /// None = NULL input (COUNT(*) materializes a dummy Some).
    arg: Option<Value>,
}

/// The 0/1/N `Value`s of one window order key. Window keys are almost always
/// zero or one expression, so this keeps them inline instead of heap-
/// allocating a `Vec` per input row.
enum KeyVals {
    None,
    One(Value),
    Many(Vec<Value>),
}

impl KeyVals {
    fn as_slice(&self) -> &[Value] {
        match self {
            KeyVals::None => &[],
            KeyVals::One(v) => std::slice::from_ref(v),
            KeyVals::Many(v) => v,
        }
    }
}

/// Read one row of a window order key out of the evaluated vectors, without a
/// temporary `Vec` for the common 0/1-key shapes.
fn key_vals(vecs: &[FusedVec], i: usize) -> KeyVals {
    let mut it = vecs.iter();
    let Some(first) = it.next() else {
        return KeyVals::None;
    };
    match (it.next(), it.next()) {
        (None, _) => KeyVals::One(first.v().get(i)),
        (Some(b), None) => KeyVals::Many(vec![first.v().get(i), b.v().get(i)]),
        (Some(b), Some(c)) => {
            let mut v = Vec::with_capacity(4);
            v.push(first.v().get(i));
            v.push(b.v().get(i));
            v.push(c.v().get(i));
            v.extend(it.map(|v| v.v().get(i)));
            KeyVals::Many(v)
        }
    }
}

/// One input chunk's evaluated window key/order/arg vectors. Bare column
/// references borrow the chunk's vectors; anything else is an owned result.
struct ChunkVecs<'a> {
    part: Vec<FusedVec<'a>>,
    ord: Vec<FusedVec<'a>>,
    arg: Option<FusedVec<'a>>,
    /// lag/lead DEFAULT expression, when present.
    deflt: Option<FusedVec<'a>>,
}

/// Evaluate all windows, appending one column each (sequentially, so later
/// windows could in principle see earlier results — nesting is rejected at
/// bind time, so each window actually reads only input columns).
pub(crate) fn eval_window(
    cx: &Cx,
    chunks: &[DataChunk],
    windows: &[BoundWindow],
) -> Result<Vec<DataChunk>> {
    let mut out: Vec<DataChunk> = chunks.to_vec();
    for w in windows {
        out = eval_one_window(cx, &out, w)?;
    }
    Ok(out)
}

fn eval_one_window(cx: &Cx, chunks: &[DataChunk], w: &BoundWindow) -> Result<Vec<DataChunk>> {
    // 1. Evaluate the partition/order/arg expressions per chunk in parallel
    // (assembly preserves chunk order); rows stay as (chunk, row) indices.
    let per_chunk: Vec<ChunkVecs> = chunks
        .par_iter()
        .map(|chunk| {
            let part = w
                .partition
                .iter()
                .map(|e| eval_agg_vec(e, chunk, cx))
                .collect::<Result<Vec<_>>>()?;
            let ord = w
                .order
                .iter()
                .map(|(e, _)| eval_agg_vec(e, chunk, cx))
                .collect::<Result<Vec<_>>>()?;
            let arg = match &w.arg {
                Some(e) => Some(eval_agg_vec(e, chunk, cx)?),
                None => None,
            };
            let deflt = match &w.default {
                Some(e) => Some(eval_agg_vec(e, chunk, cx)?),
                None => None,
            };
            Ok(ChunkVecs {
                part,
                ord,
                arg,
                deflt,
            })
        })
        .collect::<Result<_>>()?;
    // Row records, built per chunk in parallel and concatenated in chunk
    // order (global row index = position in `rows`).
    let per_chunk_rows: Vec<Vec<Row>> = chunks
        .par_iter()
        .enumerate()
        .map(|(ci, chunk)| {
            let cv = &per_chunk[ci];
            (0..chunk.num_rows())
                .map(|i| Row {
                    ord: key_vals(&cv.ord, i),
                    arg: match &cv.arg {
                        None => Some(Value::Integer(1)), // COUNT(*): every row counts
                        Some(v) if v.v().is_null(i) => None,
                        Some(v) => Some(v.v().get(i)),
                    },
                })
                .collect()
        })
        .collect();
    let mut chunk_starts = Vec::with_capacity(chunks.len() + 1);
    let mut rows: Vec<Row> = Vec::with_capacity(chunks.iter().map(|c| c.num_rows()).sum());
    for part in per_chunk_rows {
        chunk_starts.push(rows.len());
        rows.extend(part);
    }
    chunk_starts.push(rows.len());
    let n = rows.len();
    // lag/lead DEFAULT values, one per global row (chunks in order). Only
    // built when the window carries a DEFAULT expression.
    let defaults: Option<Vec<Value>> = match &w.default {
        Some(_) if matches!(w.func, WindowFunc::Lag | WindowFunc::Lead) => {
            let mut d = Vec::with_capacity(n);
            for (ci, cv) in per_chunk.iter().enumerate() {
                let v = cv.deflt.as_ref().expect("default evaluated with the arg");
                for i in 0..chunks[ci].num_rows() {
                    d.push(v.v().get(i));
                }
            }
            Some(d)
        }
        _ => None,
    };

    // 2. Group global indices by partition encoding (NULL keys group together).
    // The partition key encodes straight from the evaluated vectors — byte
    // for byte the same encoding the old per-row `Value`s produced. Each
    // chunk groups its own rows in parallel; the partial maps merge in chunk
    // order, so every member list stays in ascending global row order.
    let partials: Vec<FastMap<Vec<u8>, Vec<usize>>> = (0..chunks.len())
        .into_par_iter()
        .map(|ci| {
            let mut local: FastMap<Vec<u8>, Vec<usize>> = FastMap::default();
            let mut buf = Vec::new();
            for (offset, g) in (chunk_starts[ci]..chunk_starts[ci + 1]).enumerate() {
                buf.clear();
                for v in &per_chunk[ci].part {
                    v.v().encode_into(&mut buf, offset);
                }
                match local.get_mut(&buf) {
                    Some(members) => members.push(g),
                    None => {
                        local.insert(buf.clone(), vec![g]);
                    }
                }
            }
            local
        })
        .collect();
    let mut groups: FastMap<Vec<u8>, Vec<usize>> = FastMap::default();
    for local in partials {
        for (key, members) in local {
            groups.entry(key).or_default().extend(members);
        }
    }

    // 3. Order within each group + compute (partitions in parallel), then
    // scatter back to global order. Sorted group keys keep the work
    // deterministic.
    let mut group_keys: Vec<&Vec<u8>> = groups.keys().collect();
    group_keys.sort();
    let computed: Vec<Vec<(usize, Value)>> = group_keys
        .par_iter()
        .map(|k| eval_partition(&groups[*k], &rows, w, defaults.as_deref()))
        .collect::<Result<_>>()?;
    let mut out_vals: Vec<Value> = vec![Value::Null; n];
    for part in computed {
        for (g, v) in part {
            out_vals[g] = v;
        }
    }

    // 4. Each output chunk is its input chunk's columns (shared, not copied)
    // plus the new column, in input order.
    chunks
        .par_iter()
        .enumerate()
        .map(|(ci, chunk)| {
            let mut col = Vector::with_capacity(w.dtype, chunk.num_rows());
            for v in &out_vals[chunk_starts[ci]..chunk_starts[ci + 1]] {
                col.push_value(v.clone())?;
            }
            let mut columns = chunk.columns.clone();
            columns.push(std::sync::Arc::new(col));
            Ok(DataChunk {
                columns,
                num_rows: chunk.num_rows(),
            })
        })
        .filter(|c: &Result<DataChunk>| c.as_ref().map_or(true, |c| c.num_rows > 0))
        .collect()
}

/// One partition's window values, as `(global row, value)` pairs.
/// `defaults` (lag/lead only) holds one DEFAULT value per global row.
fn eval_partition(
    members: &[usize],
    rows: &[Row],
    w: &BoundWindow,
    defaults: Option<&[Value]>,
) -> Result<Vec<(usize, Value)>> {
    let mut seq: Vec<usize> = members.to_vec();
    if !w.order.is_empty() {
        seq.sort_by(|&a, &b| order_rows(rows[a].ord.as_slice(), rows[b].ord.as_slice(), w));
    }
    let mut out = Vec::with_capacity(seq.len());
    let frame_value =
        |pos: usize, seq: &[usize], first: bool| match frame_range(&w.frame, pos, seq, rows, w) {
            Some((lo, hi)) => {
                let k = if first { lo } else { hi };
                rows[seq[k]].arg.clone().unwrap_or(Value::Null)
            }
            None => Value::Null,
        };
    match w.func {
        WindowFunc::RowNumber => {
            for (pos, g) in seq.iter().enumerate() {
                out.push((*g, Value::Bigint(pos as i64 + 1)));
            }
        }
        WindowFunc::Rank | WindowFunc::DenseRank => {
            let mut rank: i64 = 1;
            let mut dense: i64 = 1;
            for (pos, g) in seq.iter().enumerate() {
                if pos > 0 && !is_peer(rows[seq[pos - 1]].ord.as_slice(), rows[*g].ord.as_slice()) {
                    rank = pos as i64 + 1;
                    dense += 1;
                }
                out.push((
                    *g,
                    Value::Bigint(if w.func == WindowFunc::DenseRank {
                        dense
                    } else {
                        rank
                    }),
                ));
            }
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            // Frame-independent: lag/lead read the row `offset` before/after
            // the current one; at the edges the DEFAULT (else NULL) is used.
            // A negative offset flips the direction (DuckDB).
            let delta = if w.func == WindowFunc::Lag {
                w.offset
            } else {
                -w.offset
            };
            for (pos, g) in seq.iter().enumerate() {
                let target = pos as i128 - delta as i128;
                let v = if (0..seq.len() as i128).contains(&target) {
                    rows[seq[target as usize]]
                        .arg
                        .clone()
                        .unwrap_or(Value::Null)
                } else {
                    // The default expression is evaluated once per output
                    // row (DuckDB evaluates it per row, not per partition).
                    defaults
                        .and_then(|d| d.get(*g))
                        .cloned()
                        .unwrap_or(Value::Null)
                };
                out.push((*g, v));
            }
        }
        WindowFunc::NTile => {
            // Buckets as equal as possible, larger ones first: the first
            // `len % n` buckets take one extra row.
            let n = w.offset as usize; // binder guarantees > 0
            let (full, rem) = (seq.len() / n, seq.len() % n);
            for (pos, g) in seq.iter().enumerate() {
                let bucket = if pos < rem * (full + 1) {
                    pos / (full + 1)
                } else {
                    rem + (pos - rem * (full + 1)) / full
                };
                out.push((*g, Value::Bigint(bucket as i64 + 1)));
            }
        }
        WindowFunc::FirstValue | WindowFunc::LastValue => {
            let first = w.func == WindowFunc::FirstValue;
            for (pos, g) in seq.iter().enumerate() {
                out.push((*g, frame_value(pos, &seq, first)));
            }
        }
        WindowFunc::Agg(f) => {
            // `UnboundedPreceding` frames grow monotonically in `hi(pos)`
            // (every end bound saturates; RANGE end bounds are monotone in
            // `pos` too), so one running accumulator over `[0, hi]` gives
            // bit-identical results to recomputing each frame — O(n) instead of
            // O(n²). It never reads `lo`, so it is valid only while `lo` is
            // always 0: that fails for a RANGE frame with an offset, where a
            // NULL ordering key restricts the frame to the NULL peer group
            // (`lo > 0`). Those frames take the per-row recompute below.
            let range_offset = matches!(w.frame.mode, FrameMode::Range)
                && (matches!(
                    w.frame.start,
                    FrameBound::Preceding(_) | FrameBound::Following(_)
                ) || matches!(
                    w.frame.end,
                    FrameBound::Preceding(_) | FrameBound::Following(_)
                ));
            if w.frame.start == FrameBound::UnboundedPreceding && !range_offset {
                let mut acc = Acc::new(f, false);
                let mut next_to_add = 0usize;
                for (pos, g) in seq.iter().enumerate() {
                    if let Some((_, hi)) = frame_range(&w.frame, pos, &seq, rows, w) {
                        // Additions happen in the same order as the per-row
                        // recompute used to (same frame rows, same order ⇒
                        // identical float results too).
                        while next_to_add <= hi {
                            acc.update(rows[seq[next_to_add]].arg.as_ref());
                            next_to_add += 1;
                        }
                    }
                    out.push((*g, acc.finalize_ref(w.dtype)?));
                }
            } else {
                // Bounded start: frames are bounded by the spec, so a per-row
                // recompute is fine (and keeps the code simple).
                for (pos, g) in seq.iter().enumerate() {
                    let mut acc = Acc::new(f, false);
                    if let Some((lo, hi)) = frame_range(&w.frame, pos, &seq, rows, w) {
                        for k in lo..=hi {
                            acc.update(rows[seq[k]].arg.as_ref());
                        }
                    }
                    out.push((*g, acc.finalize(w.dtype)?));
                }
            }
        }
    }
    Ok(out)
}

/// Compare two order-key tuples under the window's ORDER BY: direction and
/// explicit/default NULL placement (NULLS LAST by default).
fn order_rows(a: &[Value], b: &[Value], w: &BoundWindow) -> Ordering {
    for ((ka, kb), (_, spec)) in a.iter().zip(b.iter()).zip(w.order.iter()) {
        let ord = seq_cmp(ka, kb, spec);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Compare two ORDER BY key values in *sequence order* (the order the sorted
/// partition is in): direction flips non-NULL values; NULL placement is the
/// spec's (first/last) independent of direction.
fn seq_cmp(a: &Value, b: &Value, spec: &SortSpec) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if spec.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if spec.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let c = compare_values(a, b);
            if spec.desc {
                c.reverse()
            } else {
                c
            }
        }
    }
}

/// Peer rows share a rank: equal order keys (NULLs are peers of NULLs).
/// With no ORDER BY every row is a peer (all ranks are 1).
fn is_peer(a: &[Value], b: &[Value]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(ka, kb)| match (ka, kb) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => compare_values(ka, kb) == Ordering::Equal,
    })
}

/// One frame's inclusive `[lo, hi]` sequence positions; `None` = empty frame.
fn frame_range(
    frame: &crate::sql::binder::BoundFrame,
    pos: usize,
    seq: &[usize],
    rows: &[Row],
    w: &BoundWindow,
) -> Option<(usize, usize)> {
    match frame.mode {
        FrameMode::Rows => rows_frame(frame, pos, seq.len()),
        FrameMode::Range => range_frame(frame, pos, seq, rows, w),
    }
}

/// ROWS frames: plain position arithmetic (the pre-RANGE behavior).
fn rows_frame(
    frame: &crate::sql::binder::BoundFrame,
    pos: usize,
    len: usize,
) -> Option<(usize, usize)> {
    use FrameBound::*;
    let lo = match frame.start {
        UnboundedPreceding => 0,
        Preceding(k) => pos.saturating_sub(k),
        CurrentRow => pos,
        Following(k) => pos.saturating_add(k),
        UnboundedFollowing => return None,
    };
    let hi = match frame.end {
        UnboundedPreceding => return None,
        // A `k PRECEDING` end bound before the partition start is an EMPTY
        // frame; saturating would wrongly clamp it to row 0 and include it.
        Preceding(k) => pos.checked_sub(k)?,
        CurrentRow => pos,
        Following(k) => pos.saturating_add(k).min(len - 1),
        UnboundedFollowing => len - 1,
    };
    if lo > hi {
        None
    } else {
        Some((lo, hi))
    }
}

/// RANGE frames: bounds are positions of ORDER BY *values*. Peers (equal
/// keys) always move together; `k PRECEDING`/`FOLLOWING` step `k` value
/// units in the ordering direction (so for DESC, PRECEDING reaches larger
/// values). Only the first ORDER BY key participates (the binder enforces at
/// most one for RANGE).
fn range_frame(
    frame: &crate::sql::binder::BoundFrame,
    pos: usize,
    seq: &[usize],
    rows: &[Row],
    w: &BoundWindow,
) -> Option<(usize, usize)> {
    use FrameBound::*;
    let len = seq.len();
    let Some((_, spec)) = w.order.first() else {
        // No ORDER BY: every row is a peer, so unbounded/current bounds
        // cover the whole partition. (Offsets without ORDER BY are rejected
        // at bind time.)
        let lo = match frame.start {
            UnboundedPreceding | CurrentRow => 0,
            _ => return None,
        };
        let hi = match frame.end {
            UnboundedFollowing | CurrentRow => len - 1,
            _ => return None,
        };
        return if lo > hi { None } else { Some((lo, hi)) };
    };
    let spec = *spec;
    let cur = key_of(&rows[seq[pos]]);
    let has_offset = matches!(frame.start, Preceding(_) | Following(_))
        || matches!(frame.end, Preceding(_) | Following(_));
    // An offset on a NULL ordering key cannot be evaluated as value
    // arithmetic: DuckDB restricts the frame to the NULL peer group.
    if has_offset && cur.is_null() {
        let mut lo = pos;
        while lo > 0 && key_of(&rows[seq[lo - 1]]).is_null() {
            lo -= 1;
        }
        let mut hi = pos;
        while hi + 1 < len && key_of(&rows[seq[hi + 1]]).is_null() {
            hi += 1;
        }
        return Some((lo, hi));
    }
    let lo = match frame.start {
        UnboundedPreceding => 0,
        CurrentRow => {
            // Peers are equal full ORDER BY key tuples (multi-key safe).
            let mut j = pos;
            while j > 0
                && is_peer(
                    rows[seq[j - 1]].ord.as_slice(),
                    rows[seq[pos]].ord.as_slice(),
                )
            {
                j -= 1;
            }
            j
        }
        Preceding(k) => {
            let t = shift_value(cur, k, !spec.desc)?;
            first_seq_ge(seq, rows, &t, &spec)
        }
        Following(k) => {
            let t = shift_value(cur, k, spec.desc)?;
            first_seq_ge(seq, rows, &t, &spec)
        }
        UnboundedFollowing => return None,
    };
    let hi = match frame.end {
        UnboundedPreceding => return None,
        Preceding(k) => {
            let t = shift_value(cur, k, !spec.desc)?;
            last_seq_le(seq, rows, &t, &spec)?
        }
        CurrentRow => {
            let mut j = pos;
            while j + 1 < len
                && is_peer(
                    rows[seq[j + 1]].ord.as_slice(),
                    rows[seq[pos]].ord.as_slice(),
                )
            {
                j += 1;
            }
            j
        }
        Following(k) => {
            let t = shift_value(cur, k, spec.desc)?;
            last_seq_le(seq, rows, &t, &spec)?
        }
        UnboundedFollowing => len - 1,
    };
    if lo > hi {
        None
    } else {
        Some((lo, hi))
    }
}

/// The window's first ORDER BY value for one row.
fn key_of(row: &Row) -> &Value {
    row.ord.as_slice().first().unwrap_or(&Value::Null)
}

/// `k` value units before (sequence order) the current key. `back` is true
/// when the target is sequence-earlier: ASC subtracts, DESC adds.
fn shift_value(v: &Value, k: usize, back: bool) -> Option<Value> {
    let k = k as i64;
    let delta = if back { k.saturating_neg() } else { k };
    Some(match v {
        Value::Integer(i) => Value::Bigint((*i as i64).saturating_add(delta)),
        Value::Bigint(i) => Value::Bigint(i.saturating_add(delta)),
        Value::Double(x) => Value::Double(x + delta as f64),
        Value::Date(d) => {
            Value::Date(d.saturating_add(delta.clamp(i32::MIN as i64, i32::MAX as i64) as i32))
        }
        // The binder only admits numeric/DATE keys for RANGE offsets.
        _ => return None,
    })
}

/// First sequence index whose key is `>= threshold` in sequence order.
fn first_seq_ge(seq: &[usize], rows: &[Row], threshold: &Value, spec: &SortSpec) -> usize {
    let mut lo = 0usize;
    let mut hi = seq.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if seq_cmp(key_of(&rows[seq[mid]]), threshold, spec) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Last sequence index whose key is `<= threshold` in sequence order
/// (`None` when no such index exists).
fn last_seq_le(seq: &[usize], rows: &[Row], threshold: &Value, spec: &SortSpec) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = seq.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if seq_cmp(key_of(&rows[seq[mid]]), threshold, spec) == Ordering::Greater {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo.checked_sub(1)
}
