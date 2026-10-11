//! Hash aggregation: group rows by encoded keys, update per-group states.
//!
//! Keys encode as `Vec<u8>` → `HashMap` (NULL keys group together — the NULL
//! tag byte is the whole encoding). First-seen key `Value`s are kept for
//! output; SUM over integers accumulates in i64 (wraps on overflow, v1).

use crate::types::FastMap;

use super::expr::eval_expr;
use super::Cx;
use crate::plan::compare_values;
use rayon::prelude::*;
use crate::sql::binder::{AggFunc, BoundAgg, BoundExpr};
use crate::types::{DataType, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};

fn encode_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => buf.push(0),
        Value::Boolean(b) => {
            buf.push(1);
            buf.push(*b as u8);
        }
        Value::Integer(i) => {
            buf.push(2);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Bigint(i) => {
            buf.push(3);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Double(x) => {
            buf.push(4);
            // Normalize -0.0 → 0.0 and all NaNs to one encoding so equality
            // matches `=` (also keeps GROUP BY / DISTINCT / JOIN consistent).
            let norm = if x.is_nan() {
                f64::NAN
            } else if *x == 0.0 {
                0.0
            } else {
                *x
            };
            buf.extend_from_slice(&norm.to_bits().to_le_bytes());
        }
        Value::Varchar(s) => {
            buf.push(5);
            buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        // Dates encode as their raw payloads under distinct tags so same-type
        // equality (GROUP BY / DISTINCT / JOIN) never mixes them with ints.
        Value::Date(d) => {
            buf.push(6);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(us) => {
            buf.push(7);
            buf.extend_from_slice(&us.to_le_bytes());
        }
    }
}

pub(crate) fn encode_row(buf: &mut Vec<u8>, row: &[Value]) {
    for v in row {
        encode_value(buf, v);
    }
}

pub(crate) enum Acc {
    Count(i64),
    SumInt { sum: i64, non_null: bool },
    SumFloat { sum: f64, non_null: bool },
    Avg { sum: f64, count: i64 },
    MinMax { cur: Option<Value>, is_min: bool },
}

impl Acc {
    pub(crate) fn new(func: AggFunc) -> Acc {
        match func {
            AggFunc::Count => Acc::Count(0),
            AggFunc::Sum => Acc::SumInt { sum: 0, non_null: false },
            AggFunc::Avg => Acc::Avg { sum: 0.0, count: 0 },
            AggFunc::Min => Acc::MinMax { cur: None, is_min: true },
            AggFunc::Max => Acc::MinMax { cur: None, is_min: false },
        }
    }

    pub(crate) fn update(&mut self, arg: Option<&Value>) {
        match self {
            Acc::Count(n) => {
                // COUNT(*) passes Some(dummy) per row; COUNT(x) passes None for NULL.
                if arg.is_some() {
                    *n += 1;
                }
            }
            Acc::SumInt { sum, non_null } => match arg {
                Some(Value::Integer(i)) => {
                    *sum = sum.wrapping_add(*i as i64);
                    *non_null = true;
                }
                Some(Value::Bigint(i)) => {
                    *sum = sum.wrapping_add(*i);
                    *non_null = true;
                }
                Some(Value::Double(x)) => {
                    *self = Acc::SumFloat { sum: *sum as f64 + *x, non_null: true };
                }
                _ => {}
            },
            Acc::SumFloat { sum, non_null } => match arg {
                Some(Value::Integer(i)) => {
                    *sum += *i as f64;
                    *non_null = true;
                }
                Some(Value::Bigint(i)) => {
                    *sum += *i as f64;
                    *non_null = true;
                }
                Some(Value::Double(x)) => {
                    *sum += *x;
                    *non_null = true;
                }
                _ => {}
            },
            Acc::Avg { sum, count } => match arg {
                Some(Value::Integer(i)) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                Some(Value::Bigint(i)) => {
                    *sum += *i as f64;
                    *count += 1;
                }
                Some(Value::Double(x)) => {
                    *sum += *x;
                    *count += 1;
                }
                _ => {}
            },
            Acc::MinMax { cur, is_min } => {
                if let Some(v) = arg {
                    let better = match cur {
                        None => true,
                        Some(c) => {
                            let ord = compare_values(v, c);
                            (ord == std::cmp::Ordering::Less) == *is_min
                                && ord != std::cmp::Ordering::Equal
                        }
                    };
                    if better {
                        *cur = Some(v.clone());
                    }
                }
            }
        }
    }

    /// Merge another accumulator of the same aggregate into this one
    /// (for parallel aggregation: per-partition partials combine in order).
    pub(crate) fn merge(&mut self, other: Acc) {
        // Owned matching keeps every combination explicit and total.
        let mine = std::mem::replace(self, Acc::Count(0));
        *self = match (mine, other) {
            (Acc::Count(a), Acc::Count(b)) => Acc::Count(a + b),
            (
                Acc::SumInt { sum: a, non_null: an },
                Acc::SumInt { sum: b, non_null: bn },
            ) => Acc::SumInt { sum: a.wrapping_add(b), non_null: an || bn },
            (
                Acc::SumFloat { sum: a, non_null: an },
                Acc::SumFloat { sum: b, non_null: bn },
            ) => Acc::SumFloat { sum: a + b, non_null: an || bn },
            // Mixed int/float partials (one side saw a DOUBLE first): fold
            // the int side in as float. Either direction lands in SumFloat.
            (
                Acc::SumInt { sum: a, non_null: an },
                Acc::SumFloat { sum: b, non_null: bn },
            )
            | (
                Acc::SumFloat { sum: b, non_null: bn },
                Acc::SumInt { sum: a, non_null: an },
            ) => Acc::SumFloat { sum: a as f64 + b, non_null: an || bn },
            (
                Acc::Avg { sum: a, count: ac },
                Acc::Avg { sum: b, count: bc },
            ) => Acc::Avg { sum: a + b, count: ac + bc },
            (
                Acc::MinMax { cur: a, is_min },
                Acc::MinMax { cur: b, .. },
            ) => {
                let better = match (&a, &b) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some(c), Some(v)) => {
                        let ord = compare_values(v, c);
                        (ord == std::cmp::Ordering::Less) == is_min
                            && ord != std::cmp::Ordering::Equal
                    }
                };
                Acc::MinMax { cur: if better { b } else { a }, is_min }
            }
            // Different aggregates never meet (one Acc per agg slot).
            (mine, _) => mine,
        };
    }
    pub(crate) fn finalize(self, dtype: DataType) -> Value {
        match self {
            Acc::Count(n) => Value::Bigint(n),
            // SUM of zero/all-NULL inputs is NULL (like AVG/MIN/MAX).
            Acc::SumInt { sum, non_null } => {
                if !non_null {
                    Value::Null
                } else {
                    match dtype {
                        DataType::Double => Value::Double(sum as f64),
                        _ => Value::Bigint(sum),
                    }
                }
            }
            Acc::SumFloat { sum, non_null } => {
                if !non_null {
                    Value::Null
                } else {
                    Value::Double(sum)
                }
            }
            Acc::Avg { sum, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Double(sum / count as f64)
                }
            }
            Acc::MinMax { cur, .. } => cur.unwrap_or(Value::Null),
        }
    }
}

struct Group {
    keys: Vec<Value>,
    accs: Vec<Acc>,
}

/// Run hash aggregation over `inputs`. Output columns are
/// `[keys..., aggs...]`, chunked at [`CHUNK_SIZE`].
///
/// Chunks aggregate in parallel (rayon) into per-chunk partials, which merge
/// in chunk order — group order is deterministic (first-seen row wins).
pub(crate) fn aggregate(
    cx: &Cx,
    inputs: &[DataChunk],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    struct Partial {
        order: Vec<Vec<u8>>,
        groups: FastMap<Vec<u8>, Group>,
    }

    // Phase 1: per-chunk partials in parallel.
    let partials: Vec<Partial> = inputs
        .par_iter()
        .map(|chunk| {
            let key_vecs: Vec<Vector> = keys
                .iter()
                .map(|k| eval_expr(k, chunk, cx))
                .collect::<Result<_>>()?;
            let arg_vecs: Vec<Option<Vector>> = aggs
                .iter()
                .map(|a| match &a.arg {
                    Some(e) => eval_expr(e, chunk, cx).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<_>>()?;
            let mut partial = Partial { order: Vec::new(), groups: FastMap::default() };
            let mut buf = Vec::new();
            for i in 0..chunk.num_rows() {
                // Borrowed encoding + get-first lookup: hits (the common
                // case) allocate nothing; only misses materialize Values.
                buf.clear();
                for kv in &key_vecs {
                    kv.encode_into(&mut buf, i);
                }
                if !partial.groups.contains_key(&buf) {
                    let key_vals: Vec<Value> =
                        key_vecs.iter().map(|kv| kv.get(i)).collect();
                    partial.order.push(buf.clone());
                    partial.groups.insert(
                        buf.clone(),
                        Group {
                            keys: key_vals,
                            accs: aggs.iter().map(|a| Acc::new(a.func)).collect(),
                        },
                    );
                }
                let group = partial.groups.get_mut(&buf).expect("just inserted");
                for (acc, av) in group.accs.iter_mut().zip(arg_vecs.iter()) {
                    match av {
                        None => acc.update(Some(&Value::Integer(1))), // COUNT(*)
                        Some(v) => {
                            if v.is_null(i) {
                                acc.update(None);
                            } else {
                                acc.update(Some(&v.get(i)));
                            }
                        }
                    }
                }
            }
            Ok(partial)
        })
        .collect::<Result<_>>()?;

    // Phase 2: merge partials in chunk order (deterministic first-seen order).
    let mut table: FastMap<Vec<u8>, usize> = FastMap::default();
    let mut groups: Vec<Group> = Vec::new();
    // Fast path: no keys means a single global group — merge accumulators
    // directly instead of probing the table per row.
    if keys.is_empty() {
        let mut accs: Vec<Acc> = aggs.iter().map(|a| Acc::new(a.func)).collect();
        // Reuse the per-chunk partials computed above (each holds one group
        // exactly when keys are empty — merge them all in order).
        for partial in partials {
            let g = partial.groups.into_values().next().expect("global partial");
            for (a, b) in accs.iter_mut().zip(g.accs) {
                a.merge(b);
            }
        }
        groups.push(Group { keys: Vec::new(), accs });
    } else {
        for mut partial in partials {
            for enc in partial.order {
                let g = partial.groups.remove(&enc).expect("partial group");
                match table.get(&enc) {
                    Some(&idx) => {
                        let target = &mut groups[idx];
                        for (a, b) in target.accs.iter_mut().zip(g.accs) {
                            a.merge(b);
                        }
                    }
                    None => {
                        let idx = groups.len();
                        table.insert(enc, idx);
                        groups.push(g);
                    }
                }
            }
        }
    }
    // No input rows + no keys → single empty group (e.g. COUNT(*) over ∅).
    if groups.is_empty() && keys.is_empty() {
        groups.push(Group {
            keys: Vec::new(),
            accs: aggs.iter().map(|a| Acc::new(a.func)).collect(),
        });
    }

    // Emit output chunks.
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    let mut out_chunks = Vec::new();
    let fresh = || {
        let mut cols = Vec::with_capacity(key_types.len() + aggs.len());
        for t in &key_types {
            cols.push(Vector::with_capacity(*t, CHUNK_SIZE));
        }
        for a in aggs {
            cols.push(Vector::with_capacity(a.dtype, CHUNK_SIZE));
        }
        cols
    };
    let mut cur = fresh();
    let mut cur_rows = 0;
    for g in groups {
        if cur_rows >= CHUNK_SIZE {
            out_chunks.push(DataChunk::new(cur)?);
            cur = fresh();
            cur_rows = 0;
        }
        let finalized: Vec<Value> = g
            .accs
            .into_iter()
            .zip(aggs.iter())
            .map(|(acc, a)| acc.finalize(a.dtype))
            .collect();
        for (col, v) in cur.iter_mut().zip(g.keys.into_iter().chain(finalized)) {
            col.push_value(v)?;
        }
        cur_rows += 1;
    }
    if cur_rows > 0 || out_chunks.is_empty() {
        out_chunks.push(DataChunk::new(cur)?);
    }
    Ok(out_chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_nulls_group_together() {
        let mut a = Vec::new();
        encode_value(&mut a, &Value::Null);
        let mut b = Vec::new();
        encode_value(&mut b, &Value::Null);
        assert_eq!(a, b);
        let mut c = Vec::new();
        encode_value(&mut c, &Value::Integer(0));
        assert_ne!(a, c);
    }
}
