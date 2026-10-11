//! Window functions: partition rows, order within partitions, compute per row.
//!
//! Output preserves input row order with one appended column per window.
//! Ranking uses NULLS-LAST ordering (both directions); NULL order keys are
//! peers, matching GROUP BY semantics. Aggregate windows share [`Acc`] with
//! hash aggregation; an empty frame yields COUNT → 0, others → NULL.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::aggregate::{Acc, encode_row};
use super::expr::eval_expr;
use super::Cx;
use crate::plan::compare_values;
use crate::sql::binder::{BoundWindow, FrameBound, WindowFunc};
use crate::types::{DataType, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};

struct Row {
    part: Vec<Value>,
    ord: Vec<Value>,
    /// None = NULL input (COUNT(*) materializes a dummy Some).
    arg: Option<Value>,
}

/// Evaluate all windows, appending one column each (sequentially, so later
/// windows could in principle see earlier results — nesting is rejected at
/// bind time, so each window actually reads only input columns).
pub(crate) fn eval_window(
    cx: &mut Cx,
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
    cx: &mut Cx,
    chunks: &[DataChunk],
    w: &BoundWindow,
) -> Result<Vec<DataChunk>> {
    // 1. Materialize partition/order/arg values in global row order.
    let mut rows: Vec<Row> = Vec::new();
    for chunk in chunks {
        let parts: Vec<Vector> = w
            .partition
            .iter()
            .map(|e| eval_expr(e, chunk, cx))
            .collect::<Result<_>>()?;
        let ords: Vec<Vector> = w
            .order
            .iter()
            .map(|(e, _)| eval_expr(e, chunk, cx))
            .collect::<Result<_>>()?;
        let arg = match &w.arg {
            Some(e) => Some(eval_expr(e, chunk, cx)?),
            None => None,
        };
        for i in 0..chunk.num_rows() {
            rows.push(Row {
                part: parts.iter().map(|v| v.get(i)).collect(),
                ord: ords.iter().map(|v| v.get(i)).collect(),
                arg: match &arg {
                    None => Some(Value::Integer(1)), // COUNT(*): every row counts
                    Some(v) if v.is_null(i) => None,
                    Some(v) => Some(v.get(i)),
                },
            });
        }
    }
    let n = rows.len();

    // 2. Group global indices by partition encoding (NULL keys group together).
    let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    let mut buf = Vec::new();
    for (g, row) in rows.iter().enumerate() {
        buf.clear();
        encode_row(&mut buf, &row.part);
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
            seq.sort_by(|&a, &b| order_rows(&rows[a].ord, &rows[b].ord, w));
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
                    if pos > 0 && !is_peer(&rows[seq[pos - 1]].ord, &rows[*g].ord) {
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

    // 4. Rebuild chunks in global order with the new column appended.
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut dtypes: Vec<DataType> =
        chunks[0].columns.iter().map(|c| c.dtype()).collect();
    dtypes.push(w.dtype);
    let mut finished = Vec::new();
    let mut cur: Vec<Vector> = dtypes.iter().map(|t| Vector::new_empty(*t)).collect();
    let mut cur_rows = 0usize;
    let mut g = 0usize;
    for chunk in chunks {
        for i in 0..chunk.num_rows() {
            if cur_rows >= CHUNK_SIZE {
                finished.push(DataChunk::new(std::mem::take(&mut cur))?);
                cur = dtypes.iter().map(|t| Vector::new_empty(*t)).collect();
                cur_rows = 0;
            }
            for (dst, col) in cur.iter_mut().zip(chunk.columns.iter()) {
                dst.push_value(col.get(i))?;
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
        Preceding(k) => pos.saturating_sub(k),
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
