//! Window functions: partition rows, order within partitions, compute per row.
//!
//! Output preserves input row order with one appended column per window.
//! Ranking uses NULLS-LAST ordering (both directions); NULL order keys are
//! peers, matching GROUP BY semantics. Aggregate windows share [`Acc`] with
//! hash aggregation; an empty frame yields COUNT → 0, others → NULL.

use std::cmp::Ordering;
use crate::types::FastMap;

use super::aggregate::{eval_agg_vec, Acc};
use super::aggregate::EvalVec as FusedVec;
use super::Cx;
use crate::plan::compare_values;
use crate::sql::binder::{BoundWindow, FrameBound, WindowFunc};
use crate::types::{DataType, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};
use rayon::prelude::*;

struct Row {
    /// Chunk index and row index of this input row (the partition key is
    /// encoded straight from the chunk's evaluated key vectors, so no
    /// per-row partition `Value`s exist at all).
    chunk: usize,
    row: usize,
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

fn eval_one_window(
    cx: &Cx,
    chunks: &[DataChunk],
    w: &BoundWindow,
) -> Result<Vec<DataChunk>> {
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
            Ok(ChunkVecs { part, ord, arg })
        })
        .collect::<Result<_>>()?;
    let mut rows: Vec<Row> = Vec::with_capacity(chunks.iter().map(|c| c.num_rows()).sum());
    for (ci, chunk) in chunks.iter().enumerate() {
        let cv = &per_chunk[ci];
        for i in 0..chunk.num_rows() {
            rows.push(Row {
                chunk: ci,
                row: i,
                ord: key_vals(&cv.ord, i),
                arg: match &cv.arg {
                    None => Some(Value::Integer(1)), // COUNT(*): every row counts
                    Some(v) if v.v().is_null(i) => None,
                    Some(v) => Some(v.v().get(i)),
                },
            });
        }
    }
    let n = rows.len();

    // 2. Group global indices by partition encoding (NULL keys group together).
    // The partition key encodes straight from the evaluated vectors — byte
    // for byte the same encoding the old per-row `Value`s produced.
    let mut groups: FastMap<Vec<u8>, Vec<usize>> = FastMap::default();
    let mut buf = Vec::new();
    for (g, row) in rows.iter().enumerate() {
        buf.clear();
        for v in &per_chunk[row.chunk].part {
            v.v().encode_into(&mut buf, row.row);
        }
        groups.entry(buf.clone()).or_default().push(g);
    }

    // 3. Order within each group + compute, scattering back to global order.
    let mut out_vals: Vec<Value> = vec![Value::Null; n];
    // Sort group keys for deterministic iteration (output is order-scattered
    // anyway, but determinism helps tests and debugging).
    let mut group_keys: Vec<Vec<u8>> = groups.keys().cloned().collect();
    group_keys.sort();
    for k in group_keys {
        let members = &groups[&k];
        let mut seq: Vec<usize> = members.clone();
        if !w.order.is_empty() {
            seq.sort_by(|&a, &b| order_rows(rows[a].ord.as_slice(), rows[b].ord.as_slice(), w));
        }
        match w.func {
            WindowFunc::RowNumber => {
                for (pos, g) in seq.iter().enumerate() {
                    out_vals[*g] = Value::Bigint(pos as i64 + 1);
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
                    out_vals[*g] = Value::Bigint(if w.func == WindowFunc::DenseRank {
                        dense
                    } else {
                        rank
                    });
                }
            }
            WindowFunc::Agg(f) => {
                // `UnboundedPreceding` frames only ever grow: `hi(pos)` from
                // `frame_range` is monotone non-decreasing (every end bound
                // saturates; a `k PRECEDING` end is empty — `None` — before
                // it reaches row 0), so one running accumulator extended row
                // by row gives bit-identical results to recomputing each
                // frame — but O(n) instead of O(n²) on whole-partition frames.
                if w.frame.start == FrameBound::UnboundedPreceding {
                    let mut acc = Acc::new(f);
                    let mut next_to_add = 0usize;
                    for (pos, g) in seq.iter().enumerate() {
                        if let Some((_, hi)) = frame_range(&w.frame, pos, seq.len()) {
                            // Additions happen in the same order as the
                            // per-row recompute used to (same frame rows,
                            // same order ⇒ identical float results too).
                            while next_to_add <= hi {
                                acc.update(rows[seq[next_to_add]].arg.as_ref());
                                next_to_add += 1;
                            }
                        }
                        out_vals[*g] = acc.finalize_ref(w.dtype);
                    }
                } else {
                    // Bounded start: frames are bounded by the spec, so a
                    // per-row recompute is fine (and keeps the code simple).
                    for (pos, g) in seq.iter().enumerate() {
                        let mut acc = Acc::new(f);
                        if let Some((lo, hi)) = frame_range(&w.frame, pos, seq.len()) {
                            for k in lo..=hi {
                                acc.update(rows[seq[k]].arg.as_ref());
                            }
                        }
                        out_vals[*g] = acc.finalize(w.dtype);
                    }
                }
            }
        }
    }

    // 4. Rebuild chunks in global order with the new column appended.
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut dtypes: Vec<DataType> =
        chunks[0].columns.iter().map(|c| c.dtype()).collect();
    dtypes.push(w.dtype);
    let mut finished = Vec::new();
    let mut cur: Vec<Vector> = dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
    let mut cur_rows = 0usize;
    let mut g = 0usize;
    for chunk in chunks {
        for i in 0..chunk.num_rows() {
            if cur_rows >= CHUNK_SIZE {
                finished.push(DataChunk::new(std::mem::take(&mut cur))?);
                cur = dtypes.iter().map(|t| Vector::with_capacity(*t, CHUNK_SIZE)).collect();
                cur_rows = 0;
            }
            for (dst, col) in cur.iter_mut().zip(chunk.columns.iter()) {
                dst.push_from(col, i)?;
            }
            cur.last_mut().expect("new column").push_value(out_vals[g].clone())?;
            cur_rows += 1;
            g += 1;
        }
    }
    if cur_rows > 0 {
        finished.push(DataChunk::new(cur)?);
    }
    Ok(finished)
}

/// Compare two order-key tuples with NULLS LAST in both directions.
fn order_rows(a: &[Value], b: &[Value], w: &BoundWindow) -> Ordering {
    for ((ka, kb), (_, desc)) in a.iter().zip(b.iter()).zip(w.order.iter()) {
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
    Ordering::Equal
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

/// Frame bounds as inclusive [lo, hi] group positions; None = empty frame.
fn frame_range(
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
