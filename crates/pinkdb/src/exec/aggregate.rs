//! Hash aggregation: group rows by encoded keys, update per-group states.
//!
//! Keys encode as `Vec<u8>` → `HashMap` (NULL keys group together — the NULL
//! tag byte is the whole encoding). First-seen key `Value`s are kept for
//! output; SUM over integers accumulates in i64 (wraps on overflow, v1).

use crate::types::FastMap;

use super::expr::eval_expr;
use super::Cx;
use crate::plan::{compare_values, num_cmp};
use crate::sql::binder::{AggFunc, BoundAgg, BoundExpr};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, VectorData, CHUNK_SIZE};
use rayon::prelude::*;

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

/// One evaluated aggregate key/argument vector: a bare column reference
/// borrows its chunk's vector (no deep clone), anything else owns the
/// evaluation result. Shared by the materialized `aggregate` loop and the
/// fused join→aggregate loop.
pub(crate) enum EvalVec<'a> {
    Col(&'a Vector),
    Owned(Vector),
}

/// Initial capacity for one chunk's local group table. Deliberately small:
/// the table is per chunk, and preallocating one slot per row (up to 2048)
/// makes every chunk allocate and initialize a multi-hundred-kilobyte table
/// even when the chunk holds four distinct keys. A chunk that outgrows this
/// is treated as high-cardinality and jumps to the full size in one rehash
/// (see [`HIGH_CARDINALITY_KEYS`]).
pub(crate) fn local_index_capacity(rows: usize) -> usize {
    rows.min(16)
}

/// A chunk whose local table reaches this many distinct keys is treated as
/// high-cardinality: the next insert reserves the whole per-chunk size in one
/// rehash (see `aggregate`).
pub(crate) const HIGH_CARDINALITY_KEYS: usize = 64;

impl EvalVec<'_> {
    #[inline]
    pub(crate) fn v(&self) -> &Vector {
        match self {
            EvalVec::Col(v) => v,
            EvalVec::Owned(v) => v,
        }
    }
}

/// Evaluate one aggregate key/argument expression over `chunk`, borrowing the
/// chunk column for a bare column reference (the common case) instead of
/// deep-copying it.
pub(crate) fn eval_agg_vec<'a>(
    expr: &BoundExpr,
    chunk: &'a DataChunk,
    cx: &Cx,
) -> Result<EvalVec<'a>> {
    match expr {
        BoundExpr::Column { index, .. } => Ok(EvalVec::Col(chunk.column(*index))),
        e => Ok(EvalVec::Owned(eval_expr(e, chunk, cx)?)),
    }
}

pub(crate) enum Acc {
    Count(i64),
    /// Integer SUM is exact like DuckDB's HUGEINT sum: the true total is
    /// `sum + wraps * 2^64` (`sum` wraps, `wraps` counts the signed
    /// wraparounds), so intermediate excursions past BIGINT are harmless and
    /// only a final value outside BIGINT is an overflow error. Two i64s keep
    /// the accumulator 8-byte aligned (an i128 would widen every `Acc`).
    SumInt {
        sum: i64,
        wraps: i64,
        non_null: bool,
    },
    SumFloat {
        sum: f64,
        non_null: bool,
    },
    Avg {
        sum: f64,
        count: i64,
    },
    MinMax {
        cur: Option<Value>,
        is_min: bool,
    },
}

/// Add `x` to a wrapping i64 sum, counting signed wraparounds so the exact
/// total stays `sum + wraps * 2^64` (see [`Acc::SumInt`]).
#[inline]
fn sum_add(sum: &mut i64, wraps: &mut i64, x: i64) {
    let (next, wrapped) = sum.overflowing_add(x);
    if wrapped {
        // Adding a positive value can only wrap upward, a negative downward.
        *wraps = wraps.wrapping_add(if x > 0 { 1 } else { -1 });
    }
    *sum = next;
}

/// The exact value of a [`sum_add`] accumulator.
fn exact_sum(sum: i64, wraps: i64) -> i128 {
    sum as i128 + ((wraps as i128) << 64)
}

impl Acc {
    pub(crate) fn new(func: AggFunc) -> Acc {
        match func {
            AggFunc::Count => Acc::Count(0),
            AggFunc::Sum => Acc::SumInt {
                sum: 0,
                wraps: 0,
                non_null: false,
            },
            AggFunc::Avg => Acc::Avg { sum: 0.0, count: 0 },
            AggFunc::Min => Acc::MinMax {
                cur: None,
                is_min: true,
            },
            AggFunc::Max => Acc::MinMax {
                cur: None,
                is_min: false,
            },
        }
    }

    /// COUNT(*) fast path: every row counts, no argument to look at and no
    /// `Value` to materialize. (`COUNT(*)` is the only aggregate the binder
    /// builds without an argument; anything else keeps the generic path.)
    #[inline]
    pub(crate) fn count_star(&mut self) {
        match self {
            Acc::Count(n) => *n += 1,
            other => other.update(Some(&Value::Integer(1))),
        }
    }

    /// Exactly `update(Some(&Value::Bigint(x)))` for a non-NULL INTEGER or
    /// BIGINT cell (an INTEGER widens to `i64` and then to `f64` exactly as
    /// it would from `i32`). MIN/MAX never come here: they keep the
    /// `Value`-typed comparison path.
    #[inline]
    fn add_int(&mut self, x: i64) {
        match self {
            Acc::Count(n) => *n += 1,
            Acc::SumInt {
                sum,
                wraps,
                non_null,
            } => {
                sum_add(sum, wraps, x);
                *non_null = true;
            }
            Acc::SumFloat { sum, non_null } => {
                *sum += x as f64;
                *non_null = true;
            }
            Acc::Avg { sum, count } => {
                *sum += x as f64;
                *count += 1;
            }
            Acc::MinMax { .. } => unreachable!("MIN/MAX use update_vec"),
        }
    }

    /// Exactly `update(Some(&Value::Double(x)))` for a non-NULL DOUBLE cell.
    #[inline]
    fn add_f64(&mut self, x: f64) {
        match self {
            Acc::Count(n) => *n += 1,
            Acc::SumInt { sum, wraps, .. } => {
                *self = Acc::SumFloat {
                    sum: exact_sum(*sum, *wraps) as f64 + x,
                    non_null: true,
                };
            }
            Acc::SumFloat { sum, non_null } => {
                *sum += x;
                *non_null = true;
            }
            Acc::Avg { sum, count } => {
                *sum += x;
                *count += 1;
            }
            Acc::MinMax { .. } => unreachable!("MIN/MAX use update_vec"),
        }
    }

    /// Typed per-row update: exactly `update(Some(&v.get(i)))` (or
    /// `update(None)` when the cell is NULL) with the cell read straight out
    /// of the vector payload — no `Value` materialization per row.
    #[inline]
    pub(crate) fn update_vec(&mut self, v: &Vector, i: usize) {
        if v.is_null(i) {
            return self.update(None);
        }
        match self {
            // `update(Some(_))` increments for every non-NULL payload.
            Acc::Count(n) => *n += 1,
            Acc::SumInt {
                sum,
                wraps,
                non_null,
            } => match v.data() {
                VectorData::Int32(d) => {
                    sum_add(sum, wraps, d[i] as i64);
                    *non_null = true;
                }
                VectorData::Int64(d) => {
                    sum_add(sum, wraps, d[i]);
                    *non_null = true;
                }
                // A DOUBLE cell flips the accumulator to float (same rule as
                // `update`).
                VectorData::Float64(d) => {
                    *self = Acc::SumFloat {
                        sum: exact_sum(*sum, *wraps) as f64 + d[i],
                        non_null: true,
                    };
                }
                // BOOLEAN/VARCHAR/DATE/TIMESTAMP cells are non-numeric and
                // ignored (`update`'s `_ => {}`).
                _ => {}
            },
            Acc::SumFloat { sum, non_null } => match v.data() {
                VectorData::Int32(d) => {
                    *sum += d[i] as f64;
                    *non_null = true;
                }
                VectorData::Int64(d) => {
                    *sum += d[i] as f64;
                    *non_null = true;
                }
                VectorData::Float64(d) => {
                    *sum += d[i];
                    *non_null = true;
                }
                _ => {}
            },
            Acc::Avg { sum, count } => match v.data() {
                VectorData::Int32(d) => {
                    *sum += d[i] as f64;
                    *count += 1;
                }
                VectorData::Int64(d) => {
                    *sum += d[i] as f64;
                    *count += 1;
                }
                VectorData::Float64(d) => {
                    *sum += d[i];
                    *count += 1;
                }
                _ => {}
            },
            Acc::MinMax { cur, is_min } => {
                let data = v.data();
                let wins = match cur {
                    None => true,
                    Some(c) => {
                        let ord = compare_cell(data, i, c);
                        (ord == std::cmp::Ordering::Less) == *is_min
                            && ord != std::cmp::Ordering::Equal
                    }
                };
                if wins {
                    // Only a winning cell materializes a `Value`.
                    *cur = Some(v.get(i));
                }
            }
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
            Acc::SumInt {
                sum,
                wraps,
                non_null,
            } => match arg {
                Some(Value::Integer(i)) => {
                    sum_add(sum, wraps, *i as i64);
                    *non_null = true;
                }
                Some(Value::Bigint(i)) => {
                    sum_add(sum, wraps, *i);
                    *non_null = true;
                }
                Some(Value::Double(x)) => {
                    *self = Acc::SumFloat {
                        sum: exact_sum(*sum, *wraps) as f64 + *x,
                        non_null: true,
                    };
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
                Acc::SumInt {
                    sum: a,
                    wraps: aw,
                    non_null: an,
                },
                Acc::SumInt {
                    sum: b,
                    wraps: bw,
                    non_null: bn,
                },
            ) => {
                let (mut sum, mut wraps) = (a, aw.wrapping_add(bw));
                sum_add(&mut sum, &mut wraps, b);
                Acc::SumInt {
                    sum,
                    wraps,
                    non_null: an || bn,
                }
            }
            (
                Acc::SumFloat {
                    sum: a,
                    non_null: an,
                },
                Acc::SumFloat {
                    sum: b,
                    non_null: bn,
                },
            ) => Acc::SumFloat {
                sum: a + b,
                non_null: an || bn,
            },
            // Mixed int/float partials (one side saw a DOUBLE first): fold
            // the int side in as float. Either direction lands in SumFloat.
            (
                Acc::SumInt {
                    sum: a,
                    wraps: aw,
                    non_null: an,
                },
                Acc::SumFloat {
                    sum: b,
                    non_null: bn,
                },
            )
            | (
                Acc::SumFloat {
                    sum: b,
                    non_null: bn,
                },
                Acc::SumInt {
                    sum: a,
                    wraps: aw,
                    non_null: an,
                },
            ) => Acc::SumFloat {
                sum: exact_sum(a, aw) as f64 + b,
                non_null: an || bn,
            },
            (Acc::Avg { sum: a, count: ac }, Acc::Avg { sum: b, count: bc }) => Acc::Avg {
                sum: a + b,
                count: ac + bc,
            },
            (Acc::MinMax { cur: a, is_min }, Acc::MinMax { cur: b, .. }) => {
                let better = match (&a, &b) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some(c), Some(v)) => {
                        let ord = compare_values(v, c);
                        (ord == std::cmp::Ordering::Less) == is_min
                            && ord != std::cmp::Ordering::Equal
                    }
                };
                Acc::MinMax {
                    cur: if better { b } else { a },
                    is_min,
                }
            }
            // Different aggregates never meet (one Acc per agg slot).
            (mine, _) => mine,
        };
    }
    /// Finalize by reference: identical results to [`Acc::finalize`], for
    /// callers (window frames) that must keep the accumulator live.
    pub(crate) fn finalize_ref(&self, dtype: DataType) -> Result<Value> {
        Ok(match self {
            Acc::Count(n) => Value::Bigint(*n),
            // SUM of zero/all-NULL inputs is NULL (like AVG/MIN/MAX).
            Acc::SumInt {
                sum,
                wraps,
                non_null,
            } => {
                if !*non_null {
                    Value::Null
                } else {
                    let exact = exact_sum(*sum, *wraps);
                    match dtype {
                        DataType::Double => Value::Double(exact as f64),
                        _ => Value::Bigint(i64::try_from(exact).map_err(|_| {
                            PinkDbError::Execution(format!(
                                "Overflow in SUM: {exact} is out of range for BIGINT"
                            ))
                        })?),
                    }
                }
            }
            Acc::SumFloat { sum, non_null } => {
                if !*non_null {
                    Value::Null
                } else {
                    Value::Double(*sum)
                }
            }
            Acc::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Double(*sum / *count as f64)
                }
            }
            Acc::MinMax { cur, .. } => cur.clone().unwrap_or(Value::Null),
        })
    }

    pub(crate) fn finalize(self, dtype: DataType) -> Result<Value> {
        self.finalize_ref(dtype)
    }
}

/// Compare one raw cell of `data` against a `Value`, exactly like
/// `compare_values(&Vector::get(i), c)` for that cell. Cross-layout pairings
/// (which the binder's one-dtype-per-slot rule cannot produce) fall back to
/// the generic comparator, so the result never depends on the fast path.
fn compare_cell(data: &VectorData, i: usize, c: &Value) -> std::cmp::Ordering {
    match (data, c) {
        (VectorData::Boolean(d), Value::Boolean(y)) => d[i].cmp(y),
        (VectorData::Int32(d), Value::Integer(y)) => d[i].cmp(y),
        (VectorData::Int64(d), Value::Bigint(y)) => d[i].cmp(y),
        (VectorData::Float64(d), Value::Double(y)) => num_cmp(d[i], *y),
        (VectorData::VarChar(d), Value::Varchar(y)) => d[i].as_ref().cmp(y.as_str()),
        (VectorData::Date32(d), Value::Date(y)) => d[i].cmp(y),
        (VectorData::TimestampMicros(d), Value::Timestamp(y)) => d[i].cmp(y),
        _ => compare_values(&cell_value(data, i), c),
    }
}

/// Materialize one raw cell as a `Value` (the cross-layout compare fallback).
fn cell_value(data: &VectorData, i: usize) -> Value {
    match data {
        VectorData::Boolean(d) => Value::Boolean(d[i]),
        VectorData::Int32(d) => Value::Integer(d[i]),
        VectorData::Int64(d) => Value::Bigint(d[i]),
        VectorData::Float64(d) => Value::Double(d[i]),
        VectorData::VarChar(d) => Value::Varchar(d[i].to_string()),
        VectorData::Date32(d) => Value::Date(d[i]),
        VectorData::TimestampMicros(d) => Value::Timestamp(d[i]),
    }
}

/// One group: first-seen key `Value`s plus one live accumulator per agg.
pub(crate) struct Group {
    pub(crate) keys: Vec<Value>,
    pub(crate) accs: Vec<Acc>,
}

/// One input chunk's (or one fused left chunk's) local groups: `groups` is
/// in first-seen order (entries are only ever pushed when a key first
/// appears), `index` maps each encoded key to its slot in `groups`.
pub(crate) struct Partial {
    pub(crate) groups: Vec<(Vec<u8>, Group)>,
    pub(crate) index: FastMap<Vec<u8>, usize>,
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
    // Phase 1: per-chunk partials in parallel.
    let partials: Vec<Partial> = inputs
        .par_iter()
        .map(|chunk| {
            let key_vecs: Vec<EvalVec> = keys
                .iter()
                .map(|k| eval_agg_vec(k, chunk, cx))
                .collect::<Result<_>>()?;
            let arg_vecs: Vec<Option<EvalVec>> = aggs
                .iter()
                .map(|a| match &a.arg {
                    Some(e) => eval_agg_vec(e, chunk, cx).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<_>>()?;
            let mut partial = Partial {
                groups: Vec::new(),
                // Start small: most analytic chunks hold a handful of
                // distinct keys, and preallocating one slot per row makes
                // every chunk allocate (and later free, on the merging
                // thread) a multi-hundred-kilobyte table. A chunk that
                // outgrows this is high-cardinality and pays one jump to the
                // full size instead (see the insert below).
                index: FastMap::with_capacity_and_hasher(
                    local_index_capacity(chunk.num_rows()),
                    Default::default(),
                ),
            };
            // Pass 1: one group id per row (first-seen order, as before).
            let gids = group_ids(&mut partial, &key_vecs, aggs, chunk.num_rows());
            // Pass 2: each aggregate runs as one typed loop over the chunk.
            // Accumulators are independent, so feeding agg by agg instead of
            // row by row gives every accumulator the same rows in the same
            // order — results are bit-identical.
            for (j, av) in arg_vecs.iter().enumerate() {
                accumulate(&mut partial.groups, &gids, j, av.as_ref().map(|v| v.v()));
            }
            Ok(partial)
        })
        .collect::<Result<_>>()?;

    // Phase 2 + emit, shared with the fused join→aggregate executor (which
    // builds the same `Partial`s from joined rows it never materializes).
    let groups = merge_partials(partials, !keys.is_empty(), aggs);
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    emit_groups(groups, &key_types, aggs)
}

/// Distinct keys a single-VARCHAR-key chunk resolves by linear scan before
/// switching to the hash map (a short scan of short strings beats hashing
/// plus an encoded-key compare for low-cardinality keys like `region`).
const SMALL_KEY_SET: usize = 16;

/// Register a new group for row `i` of `key_vecs` under encoded key `enc`.
/// `indexed` also records it in the encoded-key index (the hashed path and
/// the VARCHAR path, which may fall back to hashing mid-chunk); the typed
/// integer path never consults that index and skips the second insert.
fn new_group(
    partial: &mut Partial,
    key_vecs: &[EvalVec],
    aggs: &[BoundAgg],
    i: usize,
    enc: Vec<u8>,
    indexed: bool,
) -> usize {
    let idx = partial.groups.len();
    if indexed {
        partial.index.insert(enc.clone(), idx);
    }
    partial.groups.push((
        enc,
        Group {
            keys: key_vecs.iter().map(|kv| kv.v().get(i)).collect(),
            accs: aggs.iter().map(|a| Acc::new(a.func)).collect(),
        },
    ));
    idx
}

/// The group slot of every row of one chunk, creating groups in first-seen
/// row order. Single-column keys take typed paths (integers hash their
/// value; VARCHAR keys scan a small first-seen set); everything else hashes
/// the row's encoded key. Every group still records its encoded key, so the
/// cross-chunk merge is unchanged.
fn group_ids(
    partial: &mut Partial,
    key_vecs: &[EvalVec],
    aggs: &[BoundAgg],
    rows: usize,
) -> Vec<usize> {
    let mut gids = Vec::with_capacity(rows);
    let mut buf = Vec::new();
    let encode = |buf: &mut Vec<u8>, i: usize| {
        buf.clear();
        for kv in key_vecs {
            kv.v().encode_into(buf, i);
        }
    };
    let mut start = 0;
    if let [single] = key_vecs {
        let v = single.v();
        match v.data() {
            VectorData::Int32(_)
            | VectorData::Int64(_)
            | VectorData::Date32(_)
            | VectorData::TimestampMicros(_) => {
                let value = |i: usize| match v.data() {
                    VectorData::Int32(d) | VectorData::Date32(d) => d[i] as i64,
                    VectorData::Int64(d) | VectorData::TimestampMicros(d) => d[i],
                    _ => unreachable!("matched above"),
                };
                let mut map: FastMap<i64, usize> = FastMap::with_capacity_and_hasher(
                    local_index_capacity(rows),
                    Default::default(),
                );
                let mut null_gid = None;
                for i in 0..rows {
                    let gid = if v.is_null(i) {
                        match null_gid {
                            Some(g) => g,
                            None => {
                                encode(&mut buf, i);
                                let g = new_group(partial, key_vecs, aggs, i, buf.clone(), false);
                                null_gid = Some(g);
                                g
                            }
                        }
                    } else {
                        let x = value(i);
                        match map.get(&x) {
                            Some(&g) => g,
                            None => {
                                if map.len() >= HIGH_CARDINALITY_KEYS && map.capacity() < rows {
                                    map.reserve(rows - map.len());
                                }
                                encode(&mut buf, i);
                                let g = new_group(partial, key_vecs, aggs, i, buf.clone(), false);
                                map.insert(x, g);
                                g
                            }
                        }
                    };
                    gids.push(gid);
                }
                return gids;
            }
            VectorData::VarChar(d) => {
                let mut small: Vec<(&str, usize)> = Vec::new();
                let mut null_gid = None;
                while start < rows {
                    let i = start;
                    let gid = if v.is_null(i) {
                        match null_gid {
                            Some(g) => g,
                            None => {
                                encode(&mut buf, i);
                                let g = new_group(partial, key_vecs, aggs, i, buf.clone(), true);
                                null_gid = Some(g);
                                g
                            }
                        }
                    } else {
                        let key: &str = &d[i];
                        match small.iter().find(|(k, _)| *k == key) {
                            Some(&(_, g)) => g,
                            None => {
                                if small.len() >= SMALL_KEY_SET {
                                    // High cardinality: the rest of the
                                    // chunk takes the hashed path below
                                    // (every group so far is in the index).
                                    break;
                                }
                                encode(&mut buf, i);
                                let g = new_group(partial, key_vecs, aggs, i, buf.clone(), true);
                                small.push((key, g));
                                g
                            }
                        }
                    };
                    gids.push(gid);
                    start += 1;
                }
            }
            _ => {}
        }
    }
    let mut full_reserved = false;
    for i in start..rows {
        // Borrowed encoding + get-first lookup: hits (the common case)
        // allocate nothing and probe the map exactly once; only misses
        // materialize Values and insert.
        encode(&mut buf, i);
        let gid = match partial.index.get(&buf) {
            Some(&idx) => idx,
            None => {
                // Adaptive growth: once a chunk proves it holds many distinct
                // keys (> HIGH_CARDINALITY_KEYS), reserve the rest of the
                // chunk's slots in one rehash instead of growing doubling by
                // doubling.
                if !full_reserved && partial.index.len() >= HIGH_CARDINALITY_KEYS {
                    partial
                        .index
                        .reserve(rows.saturating_sub(partial.index.len()));
                    full_reserved = true;
                }
                new_group(partial, key_vecs, aggs, i, buf.clone(), true)
            }
        };
        gids.push(gid);
    }
    gids
}

/// Feed one aggregate's argument column (or COUNT(*)) into the per-row
/// group slots: the payload type is matched once per chunk, NULL cells are
/// skipped (`update(None)` is a no-op for every accumulator), and MIN/MAX
/// plus non-numeric cells keep the exact `update_vec` comparison path.
fn accumulate(groups: &mut [(Vec<u8>, Group)], gids: &[usize], j: usize, arg: Option<&Vector>) {
    let Some(v) = arg else {
        for &g in gids {
            groups[g].1.accs[j].count_star();
        }
        return;
    };
    let minmax = gids
        .first()
        .is_some_and(|&g| matches!(groups[g].1.accs[j], Acc::MinMax { .. }));
    match v.data() {
        VectorData::Int32(d) if !minmax => {
            for (i, &g) in gids.iter().enumerate() {
                if v.is_valid(i) {
                    groups[g].1.accs[j].add_int(d[i] as i64);
                }
            }
        }
        VectorData::Int64(d) if !minmax => {
            for (i, &g) in gids.iter().enumerate() {
                if v.is_valid(i) {
                    groups[g].1.accs[j].add_int(d[i]);
                }
            }
        }
        VectorData::Float64(d) if !minmax => {
            for (i, &g) in gids.iter().enumerate() {
                if v.is_valid(i) {
                    groups[g].1.accs[j].add_f64(d[i]);
                }
            }
        }
        _ => {
            for (i, &g) in gids.iter().enumerate() {
                groups[g].1.accs[j].update_vec(v, i);
            }
        }
    }
}

/// Merge per-chunk partials in chunk order (deterministic first-seen order).
/// Partial groups already sit in first-seen order, so each partial is walked
/// directly; the global table only maps encoded keys to slots for later
/// partials. Reserve the upper bound (Σ local groups) so neither the table
/// nor the output vec rehashes/regrows while it grows.
pub(crate) fn merge_partials(
    partials: Vec<Partial>,
    has_keys: bool,
    aggs: &[BoundAgg],
) -> Vec<Group> {
    let total_groups: usize = partials.iter().map(|p| p.groups.len()).sum();
    let mut table: FastMap<Vec<u8>, usize> =
        FastMap::with_capacity_and_hasher(total_groups, Default::default());
    let mut groups: Vec<Group> = Vec::with_capacity(total_groups);
    // Fast path: no keys means a single global group — merge accumulators
    // directly instead of probing the table per row.
    if !has_keys {
        let mut accs: Vec<Acc> = aggs.iter().map(|a| Acc::new(a.func)).collect();
        // Reuse the per-chunk partials computed above (each non-empty chunk
        // holds exactly one group; a fresh table scans as one 0-row chunk,
        // so an empty partial has no group to merge).
        for partial in partials {
            if let Some((_, g)) = partial.groups.into_iter().next() {
                for (a, b) in accs.iter_mut().zip(g.accs) {
                    a.merge(b);
                }
            }
        }
        groups.push(Group {
            keys: Vec::new(),
            accs,
        });
    } else {
        for partial in partials {
            for (enc, g) in partial.groups {
                // Entry API: one hash per group (a lookup + insert pair would
                // hash twice), which matters when the merged table is large
                // (one slot per distinct key across the whole input).
                match table.entry(enc) {
                    std::collections::hash_map::Entry::Occupied(slot) => {
                        let target = &mut groups[*slot.get()];
                        for (a, b) in target.accs.iter_mut().zip(g.accs) {
                            a.merge(b);
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        let idx = groups.len();
                        slot.insert(idx);
                        groups.push(g);
                    }
                }
            }
        }
    }
    // No input rows + no keys → single empty group (e.g. COUNT(*) over ∅).
    if groups.is_empty() && !has_keys {
        groups.push(Group {
            keys: Vec::new(),
            accs: aggs.iter().map(|a| Acc::new(a.func)).collect(),
        });
    }
    groups
}

/// Emit merged groups as output chunks: `[keys..., aggs...]`, chunked at
/// [`CHUNK_SIZE`]. `key_types` are the group keys' bound output dtypes.
pub(crate) fn emit_groups(
    groups: Vec<Group>,
    key_types: &[DataType],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    let mut out_chunks = Vec::new();
    let fresh = || {
        let mut cols = Vec::with_capacity(key_types.len() + aggs.len());
        for t in key_types {
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
            .collect::<Result<_>>()?;
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

    #[test]
    fn integer_sum_is_exact_across_many_wraps_and_merges() {
        let mut acc = Acc::new(AggFunc::Sum);
        for _ in 0..5 {
            acc.update(Some(&Value::Bigint(i64::MAX)));
        }
        let mut other = Acc::new(AggFunc::Sum);
        for _ in 0..5 {
            other.update(Some(&Value::Bigint(i64::MIN)));
        }
        other.update(Some(&Value::Integer(7)));
        // 5*MAX + 5*MIN + 7 = 7 - 5: exact despite multiple wraps both ways.
        acc.merge(other);
        assert_eq!(
            acc.finalize_ref(DataType::Bigint).unwrap(),
            Value::Bigint(2)
        );
        // Out of range at the end is an error, never a wrapped value.
        let mut big = Acc::new(AggFunc::Sum);
        big.update(Some(&Value::Bigint(i64::MAX)));
        big.update(Some(&Value::Bigint(1)));
        assert!(big.finalize_ref(DataType::Bigint).is_err());
        // Flipping to float keeps the exact integer prefix.
        big.update(Some(&Value::Double(0.5)));
        assert_eq!(
            big.finalize(DataType::Double).unwrap(),
            Value::Double(i64::MAX as f64 + 1.0 + 0.5)
        );
    }
}
