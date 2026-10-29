//! Hash aggregation: group rows by encoded keys, update per-group states.
//!
//! Keys encode as `Vec<u8>` → `HashMap` (NULL keys group together — the NULL
//! tag byte is the whole encoding). First-seen key `Value`s are kept for
//! output; SUM over integers accumulates exactly (i128) and **errors** on a
//! BIGINT overflow rather than wrapping (DuckDB's rule).

use crate::types::FastMap;
use crate::types::FastSet;

use super::Cx;
use super::expr::eval_expr;
use super::selection::Selected;
use crate::plan::{compare_values, num_cmp};
use crate::sql::ast::BinaryOp;
use crate::sql::binder::{AggFunc, BoundAgg, BoundExpr};
use crate::types::{DataType, PinkDbError, Result, Value, decimal_fits};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector, VectorData};
use rayon::prelude::*;

pub(crate) fn encode_value(buf: &mut Vec<u8>, v: &Value) {
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
        // DECIMAL keys carry their scale (same encoding as the vector path).
        Value::Decimal(v, scale) => {
            buf.push(8);
            buf.push(*scale);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            buf.push(9);
            // The comparison-normalized key (same as the vector path).
            let key = crate::types::interval_key(*months, *days, *micros);
            buf.extend_from_slice(&key.to_le_bytes());
        }
        // VARIANT: the canonical hash key (same as the vector path).
        Value::Variant(v) => {
            buf.push(10);
            crate::variant::hash_key(v, buf);
        }
        // ARRAY: the canonical key (same as the vector path).
        Value::Array(a) => {
            buf.push(11);
            a.hash_key(buf);
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
        BoundExpr::Column { index, .. } => Ok(EvalVec::Col(&**chunk.expect_column(*index)?)),
        e => Ok(EvalVec::Owned(eval_expr(e, chunk, cx)?)),
    }
}

/// The state of one AVG (wave 126). INTEGER/BIGINT/DECIMAL inputs are summed
/// exactly in an i128 at the input's scale and divided once in `finalize`,
/// as DuckDB's `IntegerAverageOperationHugeint` / decimal average do
/// (`extension/core_functions/aggregate/algebraic/avg.cpp`, after its
/// optimizer's `sum / count` rewrite): faster than a float add per row,
/// independent of the order rows arrive in, and the same double DuckDB
/// returns. A DOUBLE input switches to a float sum, kept in the
/// same i128 slot as its bits so `Acc` does not grow.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AvgState {
    sum: i128,
    count: i64,
    scale: u8,
    float: bool,
}

impl AvgState {
    #[inline]
    fn float_sum(&self) -> f64 {
        if self.float {
            f64::from_bits(self.sum as u64)
        } else {
            self.sum as f64 / crate::types::POW10_F64[self.scale as usize]
        }
    }

    #[inline]
    fn set_float(&mut self, x: f64) {
        self.sum = x.to_bits() as i128;
        self.float = true;
    }

    #[inline]
    pub(crate) fn add_int(&mut self, x: i128) {
        self.add_decimal(x, 0);
    }

    #[inline]
    pub(crate) fn add_decimal(&mut self, x: i128, scale: u8) {
        if self.float {
            let f = self.float_sum() + x as f64 / crate::types::POW10_F64[scale as usize];
            self.set_float(f);
        } else {
            if scale != self.scale {
                self.align(scale);
            }
            let x = if scale < self.scale {
                x * crate::types::POW10_I128[(self.scale - scale) as usize]
            } else {
                x
            };
            self.sum = self.sum.wrapping_add(x);
        }
        self.count += 1;
    }

    /// `n` decimal cells at once, `sum` being their exact sum at `scale`
    /// (exactly `n` calls of [`AvgState::add_decimal`]: the sum wraps the
    /// same way, and alignment only ever raises the scale).
    #[inline]
    fn add_decimal_n(&mut self, sum: i128, n: i64, scale: u8) {
        debug_assert!(!self.float);
        if scale != self.scale {
            self.align(scale);
        }
        let sum = if scale < self.scale {
            sum * crate::types::POW10_I128[(self.scale - scale) as usize]
        } else {
            sum
        };
        self.sum = self.sum.wrapping_add(sum);
        self.count += n;
    }

    /// Raise the exact sum's scale to `scale` (one column has one scale, so
    /// this happens at most once, on the first cell).
    fn align(&mut self, scale: u8) {
        if scale > self.scale {
            self.sum *= crate::types::POW10_I128[(scale - self.scale) as usize];
            self.scale = scale;
        }
    }

    #[inline]
    pub(crate) fn add_f64(&mut self, x: f64) {
        let f = if self.count == 0 && !self.float {
            0.0
        } else {
            self.float_sum()
        };
        self.set_float(f + x);
        self.count += 1;
    }

    pub(crate) fn merge(&mut self, other: AvgState) {
        if self.float || other.float {
            let f = self.float_sum() + other.float_sum();
            self.set_float(f);
        } else {
            let scale = self.scale.max(other.scale);
            self.align(scale);
            let b = other.sum * crate::types::POW10_I128[(scale - other.scale) as usize];
            self.sum = self.sum.wrapping_add(b);
        }
        self.count += other.count;
    }

    pub(crate) fn finalize(&self) -> Value {
        if self.count == 0 {
            return Value::Null;
        }
        if self.float {
            return Value::Double(self.float_sum() / self.count as f64);
        }
        // DuckDB's optimizer rewrites `avg(x)` to
        // `CAST(sum(x) AS DOUBLE) / CAST(count(x) AS DOUBLE)` (its plan shows
        // `sum_no_overflow`), so the exact sum becomes a double at its own
        // scale first and is then divided by the count. Dividing by
        // `count * 10^scale` in one step differs in the last bit.
        let sum = self.sum as f64 / crate::types::POW10_F64[self.scale as usize];
        Value::Double(sum / self.count as f64)
    }
}

#[derive(Clone)]
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
    /// DECIMAL SUM: exact i128 accumulation at the input's scale. The result
    /// type is DECIMAL(38, scale) (DuckDB); `overflow` records an i128
    /// excursion (the true total may still come back in range, so the error
    /// is only raised by `finalize` when the *value* cannot be represented).
    SumDecimal {
        sum: i128,
        scale: u8,
        overflow: bool,
        non_null: bool,
    },
    /// AVG: exact for INTEGER/BIGINT/DECIMAL like DuckDB (an i128 sum at the
    /// input's scale, divided once in `finalize`), a float sum for DOUBLE.
    Avg(AvgState),
    MinMax {
        cur: Option<Value>,
        is_min: bool,
    },
    /// A collecting aggregate: `COUNT(DISTINCT x)` or one of the ordered-set
    /// aggregates. The two share one `Acc` variant on purpose — an eighth
    /// variant measurably slows the per-group merge of every other aggregate.
    Collect(Collect),
}

/// The collecting accumulators. `COUNT(DISTINCT x)` keeps one encoded value
/// per distinct cell (the encoding GROUP BY/DISTINCT use, so equality
/// semantics — NULL skipped, −0.0/NaN canonical — are shared); the ordered-set
/// aggregates keep every non-NULL value and sort once in `finalize`.
#[derive(Clone)]
pub(crate) enum Collect {
    Distinct(FastSet<Vec<u8>>),
    /// `percentile_cont(fraction)`.
    Cont {
        vals: Vec<Value>,
        fraction: f64,
    },
    /// `percentile_disc(fraction)`.
    Disc {
        vals: Vec<Value>,
        fraction: f64,
    },
    /// `mode()`.
    Mode {
        vals: Vec<Value>,
    },
}

impl Collect {
    #[inline]
    fn push_vec(&mut self, v: &Vector, i: usize) {
        match self {
            Collect::Distinct(seen) => {
                let mut buf = Vec::new();
                v.encode_into(&mut buf, i);
                seen.insert(buf);
            }
            Collect::Cont { vals, .. } | Collect::Disc { vals, .. } | Collect::Mode { vals } => {
                vals.push(v.get(i))
            }
        }
    }

    #[inline]
    fn push(&mut self, arg: Option<&Value>) {
        match self {
            Collect::Distinct(seen) => {
                if let Some(v) = arg {
                    let mut buf = Vec::new();
                    encode_value(&mut buf, v);
                    seen.insert(buf);
                }
            }
            Collect::Cont { vals, .. } | Collect::Disc { vals, .. } | Collect::Mode { vals } => {
                if let Some(v) = arg {
                    vals.push(v.clone());
                }
            }
        }
    }

    /// Combine two partial accumulators of the same aggregate.
    fn merge(self, other: Collect) -> Collect {
        match (self, other) {
            (Collect::Distinct(mut seen), Collect::Distinct(more)) => {
                seen.extend(more);
                Collect::Distinct(seen)
            }
            (Collect::Cont { mut vals, fraction }, Collect::Cont { vals: more, .. }) => {
                vals.extend(more);
                Collect::Cont { vals, fraction }
            }
            (Collect::Disc { mut vals, fraction }, Collect::Disc { vals: more, .. }) => {
                vals.extend(more);
                Collect::Disc { vals, fraction }
            }
            (Collect::Mode { mut vals }, Collect::Mode { vals: more }) => {
                vals.extend(more);
                Collect::Mode { vals }
            }
            // DISTINCT and ordered-set accumulators never meet: one `Collect`
            // per aggregate slot.
            (mine, _) => mine,
        }
    }

    fn result(&self, dtype: DataType) -> Result<Value> {
        match self {
            Collect::Distinct(seen) => Ok(Value::Bigint(seen.len() as i64)),
            Collect::Cont { vals, fraction } => {
                ordered_set_result(vals, OrderedSet::PercentileCont(*fraction), dtype)
            }
            Collect::Disc { vals, fraction } => {
                ordered_set_result(vals, OrderedSet::PercentileDisc(*fraction), dtype)
            }
            Collect::Mode { vals } => ordered_set_result(vals, OrderedSet::Mode, dtype),
        }
    }
}

/// Which ordered-set aggregate an ordered [`Collect`] computes.
#[derive(Debug, Clone, Copy)]
pub(crate) enum OrderedSet {
    PercentileCont(f64),
    PercentileDisc(f64),
    Mode,
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
    /// `direct` is the ordered-set aggregate's constant fraction
    /// (`percentile_cont(0.5)`); every other function ignores it.
    #[inline]
    pub(crate) fn new(func: AggFunc, distinct: bool, direct: Option<f64>) -> Acc {
        if distinct {
            debug_assert_eq!(func, AggFunc::Count);
            return Acc::Collect(Collect::Distinct(FastSet::default()));
        }
        match func {
            AggFunc::Count => Acc::Count(0),
            AggFunc::Sum => Acc::SumInt {
                sum: 0,
                wraps: 0,
                non_null: false,
            },
            AggFunc::Avg => Acc::Avg(AvgState::default()),
            AggFunc::Min => Acc::MinMax {
                cur: None,
                is_min: true,
            },
            AggFunc::Max => Acc::MinMax {
                cur: None,
                is_min: false,
            },
            AggFunc::PercentileCont => Acc::Collect(Collect::Cont {
                vals: Vec::new(),
                fraction: direct.unwrap_or(0.0),
            }),
            AggFunc::PercentileDisc => Acc::Collect(Collect::Disc {
                vals: Vec::new(),
                fraction: direct.unwrap_or(0.0),
            }),
            AggFunc::Mode => Acc::Collect(Collect::Mode { vals: Vec::new() }),
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
            // A decimal SUM slot never sees an INTEGER cell (its argument
            // column is DECIMAL); route it through `update` for the same
            // conversion the generic path would apply.
            Acc::SumDecimal { .. } => self.update(Some(&Value::Bigint(x))),
            Acc::Avg(avg) => avg.add_int(x as i128),
            Acc::MinMax { .. } => unreachable!("MIN/MAX use update_vec"),
            // DISTINCT accumulators take the generic per-row path.
            Acc::Collect(_) => {
                unreachable!("COUNT(DISTINCT) and ordered-set aggregates use update_vec")
            }
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
            // A decimal SUM slot never sees a DOUBLE cell; route it through
            // `update` (which folds it like the generic path would).
            Acc::SumDecimal { .. } => self.update(Some(&Value::Double(x))),
            Acc::Avg(avg) => avg.add_f64(x),
            Acc::MinMax { .. } => unreachable!("MIN/MAX use update_vec"),
            Acc::Collect(_) => {
                unreachable!("COUNT(DISTINCT) and ordered-set aggregates use update_vec")
            }
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
        // DECIMAL cells feed SUM/AVG through the exact i128 path; MIN/MAX and
        // COUNT(DISTINCT) use the generic compare/encode arms below.
        if let VectorData::Decimal { values, scale, .. } = v.data()
            && matches!(
                self,
                Acc::SumInt { .. } | Acc::SumDecimal { .. } | Acc::Avg(_)
            )
        {
            return self.add_decimal(values[i], *scale);
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
            Acc::SumDecimal { .. } => {
                if let VectorData::Decimal { values, scale, .. } = v.data() {
                    self.add_decimal(values[i], *scale);
                }
            }
            Acc::Avg(avg) => match v.data() {
                VectorData::Int32(d) => avg.add_int(d[i] as i128),
                VectorData::Int64(d) => avg.add_int(d[i] as i128),
                VectorData::Float64(d) => avg.add_f64(d[i]),
                VectorData::Decimal { values, scale, .. } => avg.add_decimal(values[i], *scale),
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
            Acc::Collect(c) => c.push_vec(v, i),
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
            Acc::SumDecimal { .. } => {
                if let Some(Value::Decimal(x, scale)) = arg {
                    self.add_decimal(*x, *scale);
                }
            }
            Acc::Avg(avg) => match arg {
                Some(Value::Integer(i)) => avg.add_int(*i as i128),
                Some(Value::Bigint(i)) => avg.add_int(*i as i128),
                Some(Value::Double(x)) => avg.add_f64(*x),
                Some(Value::Decimal(x, scale)) => avg.add_decimal(*x, *scale),
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
            Acc::Collect(c) => c.push(arg),
        }
    }

    /// Merge another accumulator of the same aggregate into this one
    /// (for parallel aggregation: per-partition partials combine in order).
    /// DECIMAL SUM cell: exact i128 add at the accumulator's scale. A
    /// different-scale cell (which the binder never produces for one SUM
    /// slot) rescales first.
    /// Whether a chunk's cells of a `scale` DECIMAL column can reach this
    /// accumulator as one per-group sum ([`Acc::add_decimal_bulk`]) with the
    /// same result as cell by cell: SUM at the column's own scale (or still
    /// fresh) and an exact (non-float) AVG.
    #[inline]
    fn takes_decimal_bulk(&self, scale: u8) -> bool {
        match self {
            Acc::SumInt { .. } => true,
            Acc::SumDecimal { scale: s, .. } => *s == scale,
            Acc::Avg(a) => !a.float,
            _ => false,
        }
    }

    /// `n` cells of a `scale` DECIMAL column summing (exactly) to `sum`.
    #[inline]
    fn add_decimal_bulk(&mut self, sum: i128, n: i64, scale: u8) {
        match self {
            Acc::Avg(avg) => avg.add_decimal_n(sum, n, scale),
            _ => self.add_decimal(sum, scale),
        }
    }

    #[inline]
    fn add_decimal(&mut self, x: i128, scale: u8) {
        // A fresh SUM accumulator starts as `SumInt` (no dtype at `Acc::new`
        // time); the first DECIMAL cell converts it.
        if let Acc::SumInt {
            sum,
            wraps,
            non_null,
        } = self
        {
            let exact = exact_sum(*sum, *wraps);
            let mut overflow = false;
            let sum = match exact.checked_mul(crate::types::POW10_I128[scale as usize]) {
                Some(v) => v,
                None => {
                    overflow = true;
                    0
                }
            };
            let non_null = *non_null;
            *self = Acc::SumDecimal {
                sum,
                scale,
                overflow,
                non_null,
            };
        }
        match self {
            Acc::SumDecimal {
                sum,
                scale: acc_scale,
                overflow,
                non_null,
            } => {
                match crate::types::rescale_decimal(x, scale, *acc_scale) {
                    Some(y) => match sum.checked_add(y) {
                        Some(next) => *sum = next,
                        None => *overflow = true,
                    },
                    None => *overflow = true,
                }
                *non_null = true;
            }
            Acc::Avg(avg) => avg.add_decimal(x, scale),
            Acc::MinMax { .. } => unreachable!("MIN/MAX use update_vec"),
            Acc::Collect(_) => {
                unreachable!("COUNT(DISTINCT) and ordered-set aggregates use update_vec")
            }
            other => other.update(Some(&Value::Decimal(x, scale))),
        }
    }

    /// [`Acc::merge`] of a borrowed partial: the fixed-size states merge in
    /// place with the same arithmetic (the flat GROUP BY table merges
    /// millions of them); every other pair merges a clone.
    #[inline]
    pub(crate) fn merge_ref(&mut self, other: &Acc) {
        match (&mut *self, other) {
            (Acc::Count(a), Acc::Count(b)) => *a += b,
            (
                Acc::SumInt {
                    sum,
                    wraps,
                    non_null,
                },
                Acc::SumInt {
                    sum: b,
                    wraps: bw,
                    non_null: bn,
                },
            ) => {
                *wraps = wraps.wrapping_add(*bw);
                sum_add(sum, wraps, *b);
                *non_null |= *bn;
            }
            (
                Acc::SumDecimal {
                    sum,
                    scale,
                    overflow,
                    non_null,
                },
                Acc::SumDecimal {
                    sum: b,
                    scale: bscale,
                    overflow: bov,
                    non_null: bn,
                },
            ) if scale == bscale => {
                match sum.checked_add(*b) {
                    Some(next) => *sum = next,
                    None => *overflow = true,
                }
                *overflow |= *bov;
                *non_null |= *bn;
            }
            (Acc::Avg(a), Acc::Avg(b)) => a.merge(*b),
            _ => self.merge(other.clone()),
        }
    }

    #[inline(never)]
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
            (
                Acc::SumDecimal {
                    sum: a,
                    scale: ascale,
                    overflow: aov,
                    non_null: an,
                },
                Acc::SumDecimal {
                    sum: b,
                    scale: bscale,
                    overflow: bov,
                    non_null: bn,
                },
            ) => {
                // Both partials saw the same column, so the scales agree; a
                // disagreeing scale can only come from a test-built partial.
                let mut sum = a;
                let mut overflow = aov || bov;
                match crate::types::rescale_decimal(b, bscale, ascale) {
                    Some(y) => match sum.checked_add(y) {
                        Some(next) => sum = next,
                        None => overflow = true,
                    },
                    None => overflow = true,
                }
                Acc::SumDecimal {
                    sum,
                    scale: ascale,
                    overflow,
                    non_null: an || bn,
                }
            }
            // A fresh SUM partial (never fed a cell) converts to the other
            // side's scale; a fed one would have been SumDecimal already.
            (
                Acc::SumInt {
                    sum: a,
                    wraps: aw,
                    non_null: an,
                },
                Acc::SumDecimal {
                    sum: b,
                    scale,
                    overflow: bov,
                    non_null: bn,
                },
            )
            | (
                Acc::SumDecimal {
                    sum: b,
                    scale,
                    overflow: bov,
                    non_null: bn,
                },
                Acc::SumInt {
                    sum: a,
                    wraps: aw,
                    non_null: an,
                },
            ) => {
                let mut overflow = bov;
                let mut sum = b;
                match exact_sum(a, aw).checked_mul(crate::types::POW10_I128[scale as usize]) {
                    Some(y) => match sum.checked_add(y) {
                        Some(next) => sum = next,
                        None => overflow = true,
                    },
                    None => overflow = true,
                }
                Acc::SumDecimal {
                    sum,
                    scale,
                    overflow,
                    non_null: an || bn,
                }
            }
            (Acc::Avg(mut a), Acc::Avg(b)) => {
                a.merge(b);
                Acc::Avg(a)
            }
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
            (Acc::Collect(a), Acc::Collect(b)) => Acc::Collect(a.merge(b)),
            // Different aggregates never meet (one Acc per agg slot).
            (mine, _) => mine,
        };
    }
    /// Finalize by reference: identical results to [`Acc::finalize`], for
    /// callers (window frames) that must keep the accumulator live.
    #[inline]
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
            Acc::SumDecimal {
                sum,
                scale,
                overflow,
                non_null,
            } => {
                if !*non_null {
                    Value::Null
                } else if *overflow || !decimal_fits(*sum, crate::types::DECIMAL_MAX_PRECISION) {
                    return Err(PinkDbError::Execution(
                        "Overflow in SUM of DECIMAL: the result does not fit in DECIMAL(38)"
                            .to_string(),
                    ));
                } else {
                    Value::Decimal(*sum, *scale)
                }
            }
            Acc::Avg(avg) => avg.finalize(),
            Acc::MinMax { cur, .. } => cur.clone().unwrap_or(Value::Null),
            Acc::Collect(c) => c.result(dtype)?,
        })
    }

    pub(crate) fn finalize(self, dtype: DataType) -> Result<Value> {
        self.finalize_ref(dtype)
    }
}

/// Final value of an ordered-set aggregate. The collected non-NULL values are
/// sorted once here: `WITHIN GROUP (ORDER BY e)` only says *which* expression
/// orders them (the clause's direction and NULL placement cannot matter, and
/// DuckDB ignores them too), so a single ascending sort serves all three.
fn ordered_set_result(vals: &[Value], kind: OrderedSet, dtype: DataType) -> Result<Value> {
    if vals.is_empty() {
        return Ok(Value::Null);
    }
    // `finalize_ref` keeps the accumulator alive (window frames), so the sort
    // works on a copy.
    let mut vals = vals.to_vec();
    vals.sort_by(compare_values);
    let n = vals.len();
    Ok(match kind {
        OrderedSet::Mode => {
            // The longest run in sorted order; ties keep the smallest value
            // (DuckDB's `mode`).
            let (mut best, mut best_len) = (0usize, 0usize);
            let mut i = 0;
            while i < n {
                let mut j = i + 1;
                while j < n && compare_values(&vals[i], &vals[j]) == std::cmp::Ordering::Equal {
                    j += 1;
                }
                if j - i > best_len {
                    best = i;
                    best_len = j - i;
                }
                i = j;
            }
            vals[best].clone()
        }
        OrderedSet::PercentileDisc(p) => {
            let idx = ((p * n as f64).ceil() as usize)
                .saturating_sub(1)
                .min(n - 1);
            vals[idx].clone()
        }
        OrderedSet::PercentileCont(p) => {
            let pos = p * (n - 1) as f64;
            let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
            let (a, b) = (as_f64(&vals[lo]), as_f64(&vals[hi]));
            let x = a + (b - a) * (pos - lo as f64);
            match dtype {
                DataType::Decimal { precision, scale } => {
                    let mantissa =
                        crate::types::f64_to_decimal(x, precision, scale).ok_or_else(|| {
                            PinkDbError::Execution(format!(
                                "Overflow in percentile_cont: {x} does not fit in {dtype}"
                            ))
                        })?;
                    Value::Decimal(mantissa, scale)
                }
                _ => Value::Double(x),
            }
        }
    })
}

/// A numeric cell as `f64` for percentile interpolation.
fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => f64::from(*i),
        Value::Bigint(i) => *i as f64,
        Value::Double(x) => *x,
        Value::Decimal(m, s) => crate::types::decimal_to_f64(*m, *s),
        Value::Date(d) => f64::from(*d),
        Value::Timestamp(t) => *t as f64,
        _ => f64::NAN,
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
        (VectorData::VarChar(d), Value::Varchar(y)) => d[i].cmp(y.as_str()),
        (VectorData::Date32(d), Value::Date(y)) => d[i].cmp(y),
        (VectorData::TimestampMicros(d), Value::Timestamp(y)) => d[i].cmp(y),
        (VectorData::Decimal { values, scale, .. }, Value::Decimal(y, ys)) => {
            crate::types::decimal_cmp(values[i], *scale, *y, *ys)
        }
        (VectorData::Interval(d), Value::Interval { .. }) => {
            let y = match c {
                Value::Interval {
                    months,
                    days,
                    micros,
                } => crate::types::Interval {
                    months: *months,
                    days: *days,
                    micros: *micros,
                },
                _ => unreachable!(),
            };
            crate::types::interval_cmp(d[i], y)
        }
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
        VectorData::Decimal { values, scale, .. } => Value::Decimal(values[i], *scale),
        VectorData::Interval(d) => Value::Interval {
            months: d[i].months,
            days: d[i].days,
            micros: d[i].micros,
        },
        VectorData::Variant(d) => Value::Variant(std::sync::Arc::clone(&d[i])),
        VectorData::Array {
            elem,
            len,
            values,
            valid,
        } => {
            let n = *len as usize;
            Value::Array(std::sync::Arc::new(
                crate::array::cell_from_slice(*elem, values, valid.as_deref(), i * n, n)
                    .expect("ARRAY cell slice"),
            ))
        }
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

impl Partial {
    /// A fresh partial whose local-group table is sized for `rows` rows.
    ///
    /// Start small: most analytic chunks hold a handful of distinct keys, and
    /// preallocating one slot per row makes every chunk allocate (and later
    /// free, on the merging thread) a multi-hundred-kilobyte table. A chunk
    /// that outgrows this is high-cardinality and pays one jump to the full
    /// size instead (see the insert in [`group_ids`]).
    pub(crate) fn for_rows(rows: usize) -> Partial {
        Partial {
            groups: Vec::new(),
            index: FastMap::with_capacity_and_hasher(
                local_index_capacity(rows),
                Default::default(),
            ),
        }
    }
}

/// Runs a scan and hands each of its chunks to the sink it is given: the
/// producer half of [`fold_stream`].
type ChunkFeed<'a> = dyn FnMut(&mut dyn FnMut(DataChunk) -> Result<()>) -> Result<()> + 'a;

/// Fold a streamed scan into `workers` partials in parallel — phase 1 of
/// [`aggregate`] for a scan the executor never materializes (wave 98c).
///
/// `produce` runs the scan on the calling thread and blocks once `workers`
/// chunks are queued ([`crate::PinkDb::stream_fold_workers`] decides the
/// count); each worker applies `filters`, folds the chunk into its own
/// [`Partial`], and the partials merge exactly as [`aggregate`]'s do. The fold
/// owns at most `2 * workers` chunks — the queued ones plus the ones being
/// folded — so the memory bound follows the worker count and the source's
/// read-ahead window, never the file size.
///
/// Every aggregate the binder builds is order-insensitive (COUNT, SUM, AVG,
/// MIN, MAX, COUNT(DISTINCT)), so a chunk may land on any worker: a float
/// SUM/AVG can differ from the materialized path in its last bits, while
/// integer and decimal sums, counts and MIN/MAX stay exact. There is no
/// order-sensitive aggregate to keep on another path.
pub(crate) fn fold_stream(
    cx: &Cx,
    filters: &[&BoundExpr],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
    produce: &mut ChunkFeed<'_>,
) -> Result<Vec<Partial>> {
    let workers = crate::PinkDb::stream_fold_workers();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<DataChunk>(workers);
    let receiver = std::sync::Mutex::new(receiver);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                let receiver = &receiver;
                scope.spawn(move || -> Result<Partial> {
                    let mut partial = Partial::for_rows(CHUNK_SIZE);
                    let mut failed: Option<PinkDbError> = None;
                    loop {
                        let Ok(chunk) = receiver
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .recv()
                        else {
                            break;
                        };
                        let rows = chunk.num_rows();
                        if failed.is_some() {
                            // Keep draining, so a failed worker cannot block
                            // the producer on a full channel. The first error
                            // is reported once every worker has stopped.
                            cx.db.stream_release(rows);
                            continue;
                        }
                        let folded = filter_and_fold(cx, filters, keys, aggs, &mut partial, chunk);
                        cx.db.stream_release(rows);
                        if let Err(e) = folded {
                            failed = Some(e);
                        }
                    }
                    match failed {
                        Some(e) => Err(e),
                        None => Ok(partial),
                    }
                })
            })
            .collect();
        let produced = produce(&mut |chunk| {
            let rows = chunk.num_rows();
            cx.db.stream_hold(rows);
            if sender.send(chunk).is_err() {
                // Every worker has stopped; the join below reports why.
                cx.db.stream_release(rows);
            }
            Ok(())
        });
        drop(sender);
        let mut partials = Vec::with_capacity(workers);
        let mut failed: Option<PinkDbError> = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(partial)) => partials.push(partial),
                Ok(Err(e)) => failed = failed.or(Some(e)),
                Err(_) => {
                    failed = failed.or(Some(PinkDbError::Execution(
                        "a streaming aggregate worker panicked".to_owned(),
                    )))
                }
            }
        }
        match (failed, produced) {
            (Some(e), _) => Err(e),
            (None, produced) => produced.map(|()| partials),
        }
    })
}

/// The per-chunk phase the materializing path runs, for one streamed chunk.
fn filter_and_fold(
    cx: &Cx,
    filters: &[&BoundExpr],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
    partial: &mut Partial,
    mut chunk: DataChunk,
) -> Result<()> {
    for predicate in filters {
        match super::filter_chunk(cx, predicate, &chunk)? {
            Some(kept) => chunk = kept,
            None => return Ok(()),
        }
    }
    fold_chunk(cx, partial, &chunk, None, keys, aggs)
}

/// Fold one chunk into `partial` — phase 1 of [`aggregate`] for a single
/// chunk, shared with the streaming scan→aggregate executor (wave 98c), which
/// keeps **one** running partial and so holds one chunk at a time instead of
/// one partial per chunk of a materialized scan.
///
/// Rows are read through the same `eval_agg_vec` / `group_ids` / `accumulate`
/// calls the materialized path makes, in the same order, so groups and
/// accumulator updates are identical.
///
/// `sel` (ascending row indices) folds only those rows of `chunk`: key and
/// argument vectors are still evaluated over the whole chunk, which is what
/// keeps this path free of the filtered copy `filter_chunk` would otherwise
/// build, so the index is applied here and in [`accumulate`] last.
pub(crate) fn fold_chunk(
    cx: &Cx,
    partial: &mut Partial,
    chunk: &DataChunk,
    sel: Option<&[u32]>,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<()> {
    cx.check_deadline()?;
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
    // `FILTER (WHERE …)`: one keep mask per filtered aggregate, so the mask is
    // evaluated once for the chunk and `accumulate` just skips rows.
    let keeps: Vec<Option<Vec<bool>>> = aggs
        .iter()
        .map(|a| match &a.filter {
            Some(pred) => keep_mask(eval_agg_vec(pred, chunk, cx)?.v()).map(Some),
            None => Ok(None),
        })
        .collect::<Result<_>>()?;
    // Pass 1: one group id per row (first-seen order).
    let rows = sel.map_or(chunk.num_rows(), |s| s.len());
    let gids = group_ids(partial, &key_vecs, aggs, rows, sel);
    // Pass 2: each aggregate runs as one typed loop over the chunk.
    // Accumulators are independent, so feeding agg by agg instead of row by
    // row gives every accumulator the same rows in the same order — results
    // are bit-identical.
    for (j, av) in arg_vecs.iter().enumerate() {
        accumulate(
            partial.groups.as_mut_slice(),
            &gids,
            j,
            av.as_ref().map(|v| v.v()),
            keeps[j].as_deref(),
            sel,
        );
    }
    Ok(())
}

/// Output buffer of the integer-DISTINCT fast path: the current vector's
/// payload in the key's own layout (narrowed on flush for INTEGER/DATE) plus
/// the slot of the (at most one) NULL key. Buffers flush as `CHUNK_SIZE`-row
/// vectors, so no `Value` is ever built for a row.
struct DistinctOut {
    dtype: DataType,
    payload: Vec<i64>,
    null_at: Option<usize>,
    chunks: Vec<DataChunk>,
}

impl DistinctOut {
    fn new(dtype: DataType) -> DistinctOut {
        DistinctOut {
            dtype,
            payload: Vec::with_capacity(CHUNK_SIZE),
            null_at: None,
            chunks: Vec::new(),
        }
    }

    /// Record key `x` as the next output row; a full buffer flushes.
    fn push(&mut self, x: i64) -> Result<()> {
        self.payload.push(x);
        if self.payload.len() >= CHUNK_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    /// Record the NULL key's output row (the caller de-duplicates it).
    fn push_null(&mut self) -> Result<()> {
        debug_assert!(self.null_at.is_none());
        self.null_at = Some(self.payload.len());
        self.push(0)
    }

    /// Move the buffered payload into one vector. Only the NULL slot is
    /// invalid, so the mask is all-ones with that one bit cleared
    /// (`from_raw` clears the bits past the length).
    fn flush(&mut self) -> Result<()> {
        let rows = self.payload.len();
        if rows == 0 {
            return Ok(());
        }
        let payload = std::mem::take(&mut self.payload);
        let data = match self.dtype {
            DataType::Integer => VectorData::Int32(payload.iter().map(|&x| x as i32).collect()),
            DataType::Date => VectorData::Date32(payload.iter().map(|&x| x as i32).collect()),
            DataType::Bigint => VectorData::Int64(payload),
            DataType::Timestamp => VectorData::TimestampMicros(payload),
            // `aggregate` only routes the integer layouts here.
            _ => unreachable!("integer DISTINCT over a non-integer key"),
        };
        let mut validity = vec![u64::MAX; rows.div_ceil(64)];
        if let Some(at) = self.null_at.take() {
            validity[at / 64] &= !(1u64 << (at % 64));
        }
        self.payload = Vec::with_capacity(CHUNK_SIZE);
        self.chunks
            .push(DataChunk::new(vec![Vector::from_raw(data, validity)])?);
        Ok(())
    }
}

/// Fast path for DISTINCT over a single integer/date/timestamp column: avoids
/// the full group machinery (no per-group allocations, no encode/clone) and
/// writes distinct keys straight into typed output payloads.
fn fast_distinct_int(
    _cx: &Cx,
    inputs: &[DataChunk],
    idx: usize,
    dtype: DataType,
) -> Result<Vec<DataChunk>> {
    let total_rows: usize = inputs.iter().map(|c| c.num_rows()).sum();
    let mut seen: FastSet<i64> = FastSet::default();
    let mut reserved = false;
    let mut seen_null = false;
    let mut out = DistinctOut::new(dtype);
    // One row at a time: `Some(key)` inserts a key (emitting it on first
    // sight), `None` is the single NULL key.
    let mut feed = |x: Option<i64>| -> Result<()> {
        match x {
            Some(v) => {
                if seen.insert(v) {
                    // Like the general path: a set that proves high
                    // cardinality reserves the rest of the input in one
                    // rehash instead of growing by doubling.
                    if !reserved && seen.len() >= HIGH_CARDINALITY_KEYS {
                        seen.reserve(total_rows.saturating_sub(seen.len()).min(1 << 20));
                        reserved = true;
                    }
                    out.push(v)?;
                }
            }
            None => {
                if !seen_null {
                    seen_null = true;
                    out.push_null()?;
                }
            }
        }
        Ok(())
    };
    for chunk in inputs {
        let rows = chunk.num_rows();
        let col = &chunk.columns[idx];
        // INTEGER/DATE payloads are i32, BIGINT/TIMESTAMP i64.
        match col.data() {
            VectorData::Int32(d) | VectorData::Date32(d) => {
                for (i, &x) in d.iter().enumerate().take(rows) {
                    feed(if col.is_valid(i) {
                        Some(x as i64)
                    } else {
                        None
                    })?;
                }
            }
            VectorData::Int64(d) | VectorData::TimestampMicros(d) => {
                for (i, &x) in d.iter().enumerate().take(rows) {
                    feed(if col.is_valid(i) { Some(x) } else { None })?;
                }
            }
            other => {
                return Err(PinkDbError::Execution(format!(
                    "internal: integer DISTINCT over a {:?} column",
                    other.dtype()
                )));
            }
        }
    }
    out.flush()?;
    if out.chunks.is_empty() {
        // No input rows: one empty chunk carries the output schema, exactly
        // as the materialized emit did.
        out.chunks
            .push(DataChunk::new(vec![Vector::with_capacity(dtype, 0)])?);
    }
    Ok(out.chunks)
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
    // Fast path: a single integer/date/timestamp key with no aggregates is
    // just DISTINCT over that column. Use a FastSet directly instead of the
    // full group machinery (no per-group allocations, no encode/clone).
    if keys.len() == 1
        && aggs.is_empty()
        && let BoundExpr::Column { index, dtype } = &keys[0]
    {
        let idx = *index;
        let is_int_key = matches!(
            dtype,
            DataType::Integer | DataType::Bigint | DataType::Date | DataType::Timestamp
        );
        if is_int_key {
            return fast_distinct_int(cx, inputs, idx, *dtype);
        }
    }
    if let Some(out) = super::group_table::aggregate(cx, inputs, keys, aggs)? {
        return Ok(out);
    }
    // Phase 1: per-chunk partials in parallel.
    let partials: Vec<Partial> = inputs
        .par_iter()
        .map(|chunk| {
            let mut partial = Partial::for_rows(chunk.num_rows());
            fold_chunk(cx, &mut partial, chunk, None, keys, aggs)?;
            Ok(partial)
        })
        .collect::<Result<_>>()?;

    // Phase 2 + emit, shared with the fused join→aggregate executor (which
    // builds the same `Partial`s from joined rows it never materializes).
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    merge_and_emit(partials, &key_types, aggs)
}

/// [`aggregate`] over chunks that carry a selection: every partial folds only
/// the rows its filter kept, reading the key and argument columns through the
/// selection instead of from a materialized filtered copy.
pub(crate) fn aggregate_selected(
    cx: &Cx,
    inputs: &[Selected],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    debug_assert!(!aggs.is_empty(), "the DISTINCT path stays in `aggregate`");
    if let Some(out) = super::group_table::aggregate_selected(cx, inputs, keys, aggs)? {
        return Ok(out);
    }
    let partials: Vec<Partial> = inputs
        .par_iter()
        .map(|input| {
            let mut partial = Partial::for_rows(input.sel.len());
            match fold_chunk(cx, &mut partial, &input.chunk, Some(&input.sel), keys, aggs) {
                Ok(()) => Ok(partial),
                Err(_) => fold_kept_rows(cx, input, keys, aggs),
            }
        })
        .collect::<Result<_>>()?;
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    merge_and_emit(partials, &key_types, aggs)
}

/// The selection fold evaluates keys and arguments over the whole chunk, so
/// a row the filter dropped can raise an error (a decimal overflow, a
/// division by zero) that no kept row would. On an error, fold the kept rows
/// alone: the error stands only if one of them raises it, as in DuckDB.
/// `fold_chunk` evaluates everything before it touches the partial, so the
/// retry starts from a clean one. Kept out of line: it is the rare path, and
/// inlining a second `fold_chunk` into the per-chunk closure measurably slows
/// the common one.
#[cold]
#[inline(never)]
fn fold_kept_rows(
    cx: &Cx,
    input: &Selected,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Partial> {
    let mut partial = Partial::for_rows(input.sel.len());
    fold_chunk(
        cx,
        &mut partial,
        &input.chunk.take(&input.sel),
        None,
        keys,
        aggs,
    )?;
    Ok(partial)
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
            accs: aggs
                .iter()
                .map(|a| Acc::new(a.func, a.distinct, a.direct))
                .collect(),
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
    sel: Option<&[u32]>,
) -> Vec<usize> {
    // `rows` counts selection entries, not chunk rows: the key vectors are
    // still whole-chunk, so every index goes through the selection.
    let row = |k: usize| sel.map_or(k, |s| s[k] as usize);
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
                for k in 0..rows {
                    let i = row(k);
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
                    let i = row(start);
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
    if start == 0
        && (2..=3).contains(&key_vecs.len())
        && partial.groups.is_empty()
        && let Some(gids) = small_domain_gids(partial, key_vecs, aggs, rows, sel)
    {
        return gids;
    }
    let mut full_reserved = false;
    for k in start..rows {
        let i = row(k);
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

/// A string as a value that compares without a `memcmp` call when short:
/// up to eight bytes are one word plus the length (TPC-H Q1's one-letter
/// flags compared byte slices on every row of the small-domain scan).
#[derive(Clone, Copy, PartialEq)]
enum ShortStr<'a> {
    Word(u64, u8),
    Long(&'a str),
}

impl<'a> ShortStr<'a> {
    #[inline]
    fn of(s: &'a str) -> ShortStr<'a> {
        if s.len() <= 8 {
            // Byte by byte: a variable-length `copy_from_slice` is a
            // `memmove` call, the cost this type exists to avoid.
            let mut word = 0u64;
            for (k, &b) in s.as_bytes().iter().enumerate() {
                word |= u64::from(b) << (8 * k);
            }
            ShortStr::Word(word, s.len() as u8)
        } else {
            ShortStr::Long(s)
        }
    }
}

/// Per-row codes of one key column whose chunk holds at most
/// [`SMALL_KEY_SET`] distinct values (NULL counts as one): `None` past that,
/// or for a key type without a cheap equality here.
pub(crate) fn small_column_codes(
    v: &Vector,
    rows: usize,
    row: impl Fn(usize) -> usize,
) -> Option<(Vec<u16>, usize)> {
    fn codes_of<T: PartialEq + Copy>(
        v: &Vector,
        rows: usize,
        row: &impl Fn(usize) -> usize,
        cell: impl Fn(usize) -> T,
    ) -> Option<(Vec<u16>, usize)> {
        // Codes are handed out in first-seen order from one counter, so a
        // NULL and the values share one dense code space.
        let mut seen: Vec<(T, u16)> = Vec::new();
        let mut null_code: Option<u16> = None;
        let mut next: u16 = 0;
        let mut codes = Vec::with_capacity(rows);
        for k in 0..rows {
            let i = row(k);
            let code = if v.is_null(i) {
                match null_code {
                    Some(c) => c,
                    None => {
                        null_code = Some(next);
                        next += 1;
                        next - 1
                    }
                }
            } else {
                let x = cell(i);
                match seen.iter().find(|(y, _)| *y == x) {
                    Some(&(_, c)) => c,
                    None => {
                        seen.push((x, next));
                        next += 1;
                        next - 1
                    }
                }
            };
            if usize::from(next) > SMALL_KEY_SET {
                return None;
            }
            codes.push(code);
        }
        Some((codes, usize::from(next)))
    }
    match v.data() {
        VectorData::VarChar(d) => varchar_codes(v, d, rows, &row),
        VectorData::Int32(d) | VectorData::Date32(d) => codes_of(v, rows, &row, |i| d[i]),
        VectorData::Int64(d) | VectorData::TimestampMicros(d) => codes_of(v, rows, &row, |i| d[i]),
        VectorData::Boolean(d) => codes_of(v, rows, &row, |i| d[i]),
        _ => None,
    }
}

/// [`small_column_codes`] for a VARCHAR column. Rows are first matched by
/// their 16-byte string view ([`crate::strings::StrView::key`]): a short
/// string's view is the string, and a dictionary-encoded chunk's rows of one
/// value share one location — so most rows are coded without reading any
/// string bytes, and only a view not seen yet is compared by content.
fn varchar_codes(
    v: &Vector,
    d: &crate::strings::StrVec,
    rows: usize,
    row: &impl Fn(usize) -> usize,
) -> Option<(Vec<u16>, usize)> {
    const VIEWS: usize = 64;
    let mut views: Vec<(u128, u16)> = Vec::new();
    let mut seen: Vec<(ShortStr, u16)> = Vec::new();
    let mut null_code: Option<u16> = None;
    let mut next: u16 = 0;
    let mut codes = Vec::with_capacity(rows);
    for k in 0..rows {
        let i = row(k);
        let code = if v.is_null(i) {
            *null_code.get_or_insert_with(|| {
                next += 1;
                next - 1
            })
        } else {
            let p = d.view(i).key();
            match views.iter().find(|(q, _)| *q == p) {
                Some(&(_, c)) => c,
                None => {
                    let x = ShortStr::of(&d[i]);
                    let c = match seen.iter().find(|(y, _)| *y == x) {
                        Some(&(_, c)) => c,
                        None => {
                            seen.push((x, next));
                            next += 1;
                            next - 1
                        }
                    };
                    if views.len() < VIEWS {
                        views.push((p, c));
                    }
                    c
                }
            }
        };
        if usize::from(next) > SMALL_KEY_SET {
            return None;
        }
        codes.push(code);
    }
    Some((codes, usize::from(next)))
}

/// Group slots for a 2–3 column key whose columns each hold few distinct
/// values in the chunk (Q1's `l_returnflag, l_linestatus`): every column is
/// coded by a short linear scan and the combined code indexes a dense table,
/// so rows are neither encoded nor hashed — a perfect hash over the chunk's
/// own key domain (DuckDB's `PerfectHashAggregate` does this from column
/// statistics). Groups still record their encoded key for the merge, and
/// are created in first-seen row order. `None` hands the chunk back to the
/// hashed path (a column with too many distinct values).
fn small_domain_gids(
    partial: &mut Partial,
    key_vecs: &[EvalVec],
    aggs: &[BoundAgg],
    rows: usize,
    sel: Option<&[u32]>,
) -> Option<Vec<usize>> {
    let row = |k: usize| sel.map_or(k, |s| s[k] as usize);
    let mut columns = Vec::with_capacity(key_vecs.len());
    for kv in key_vecs {
        columns.push(small_column_codes(kv.v(), rows, row)?);
    }
    let size: usize = columns.iter().map(|(_, n)| (*n).max(1)).product();
    let mut slots = vec![usize::MAX; size];
    let mut gids = Vec::with_capacity(rows);
    let mut buf = Vec::new();
    for k in 0..rows {
        let mut code = 0usize;
        for (codes, n) in columns.iter().rev() {
            code = code * (*n).max(1) + codes[k] as usize;
        }
        let gid = match slots[code] {
            usize::MAX => {
                let i = row(k);
                buf.clear();
                for kv in key_vecs {
                    kv.v().encode_into(&mut buf, i);
                }
                let g = new_group(partial, key_vecs, aggs, i, buf.clone(), true);
                slots[code] = g;
                g
            }
            g => g,
        };
        gids.push(gid);
    }
    Some(gids)
}

/// Arithmetic subexpressions the aggregate arguments of one GROUP BY share
/// (TPC-H Q1: `l_extendedprice * (1 - l_discount)` is the argument of one
/// SUM and the left operand of the next), evaluated once per chunk into
/// extra columns that the rewritten arguments read — DuckDB's common
/// subexpression elimination (`src/optimizer/cse_optimizer.cpp`) for the
/// shape that matters here. Evaluating a subexpression once instead of twice
/// raises the same errors on the same rows.
pub(crate) struct SharedArgs {
    temps: Vec<BoundExpr>,
    args: Vec<Option<BoundExpr>>,
}

fn is_arith(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide
    )
}

/// The arithmetic nodes of `e`, outermost first, through casts.
fn arith_nodes<'a>(e: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
    match e {
        BoundExpr::Binary {
            op, left, right, ..
        } if is_arith(*op) => {
            out.push(e);
            arith_nodes(left, out);
            arith_nodes(right, out);
        }
        BoundExpr::Cast { expr, .. } => arith_nodes(expr, out),
        _ => {}
    }
}

/// `e` with every occurrence of `target` (inside arithmetic and casts)
/// replaced by `with`.
fn replace_node(e: BoundExpr, target: &BoundExpr, with: &BoundExpr) -> BoundExpr {
    if &e == target {
        return with.clone();
    }
    match e {
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } if is_arith(op) => BoundExpr::Binary {
            op,
            left: Box::new(replace_node(*left, target, with)),
            right: Box::new(replace_node(*right, target, with)),
            dtype,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(replace_node(*expr, target, with)),
            dtype,
        },
        other => other,
    }
}

impl SharedArgs {
    /// `None` when no arithmetic node appears twice. `width` is the input
    /// chunks' column count: shared node `t` becomes column `width + t`.
    pub(crate) fn new(aggs: &[BoundAgg], width: usize) -> Option<SharedArgs> {
        let mut args: Vec<Option<BoundExpr>> = aggs.iter().map(|a| a.arg.clone()).collect();
        let mut temps: Vec<BoundExpr> = Vec::new();
        while temps.len() < 8 {
            let mut nodes = Vec::new();
            for a in args.iter().flatten() {
                arith_nodes(a, &mut nodes);
            }
            // The first repeated node in outermost-first order is a largest
            // one: a node's enclosing nodes come before it.
            let Some(shared) = nodes
                .iter()
                .find(|n| nodes.iter().filter(|m| m == n).count() >= 2)
                .map(|n| (*n).clone())
            else {
                break;
            };
            let column = BoundExpr::Column {
                index: width + temps.len(),
                dtype: shared.dtype(),
            };
            args = args
                .into_iter()
                .map(|a| a.map(|e| replace_node(e, &shared, &column)))
                .collect();
            temps.push(shared);
        }
        (!temps.is_empty()).then_some(SharedArgs { temps, args })
    }

    /// `chunk` with the shared nodes appended as columns.
    pub(crate) fn extend(&self, chunk: &DataChunk, cx: &Cx) -> Result<DataChunk> {
        let mut ext = chunk.clone();
        for t in &self.temps {
            let v = eval_expr(t, &ext, cx)?;
            ext.columns.push(std::sync::Arc::new(v));
        }
        Ok(ext)
    }

    /// Aggregate `j`'s argument, reading the shared columns.
    pub(crate) fn arg(&self, j: usize) -> Option<&BoundExpr> {
        self.args[j].as_ref()
    }
}

/// The per-group accumulators [`accumulate`] feeds: a partial's group list,
/// or the flat accumulator array of a [`super::group_table`] partial.
pub(crate) trait AccSlots {
    fn group_count(&self) -> usize;
    fn acc(&mut self, g: usize, j: usize) -> &mut Acc;
    fn acc_ref(&self, g: usize, j: usize) -> &Acc;
}

impl AccSlots for [(Vec<u8>, Group)] {
    #[inline]
    fn group_count(&self) -> usize {
        self.len()
    }
    #[inline]
    fn acc(&mut self, g: usize, j: usize) -> &mut Acc {
        &mut self[g].1.accs[j]
    }
    #[inline]
    fn acc_ref(&self, g: usize, j: usize) -> &Acc {
        &self[g].1.accs[j]
    }
}

/// Feed one aggregate's argument column (or COUNT(*)) into the per-row
/// group slots: the payload type is matched once per chunk, NULL cells are
/// skipped (`update(None)` is a no-op for every accumulator), and MIN/MAX
/// plus non-numeric cells keep the exact `update_vec` comparison path.
/// `FILTER (WHERE pred)` as a per-row keep mask: only TRUE rows pass (a NULL
/// predicate is not TRUE, matching `WHERE`).
pub(crate) fn keep_mask(pred: &Vector) -> Result<Vec<bool>> {
    if pred.dtype() != DataType::Boolean {
        return Err(PinkDbError::Type(
            "FILTER needs a BOOLEAN expression".to_string(),
        ));
    }
    let mut keep = Vec::with_capacity(pred.len());
    let VectorData::Boolean(bits) = pred.data() else {
        unreachable!("BOOLEAN dtype implies BOOLEAN layout")
    };
    for (i, &is_true) in bits.iter().enumerate() {
        keep.push(is_true && pred.is_valid(i));
    }
    Ok(keep)
}

///
/// `sel` maps each selection entry back to its row of `arg` and of the
/// `FILTER` mask, which are both whole-chunk.
pub(crate) fn accumulate<S: AccSlots + ?Sized>(
    groups: &mut S,
    gids: &[usize],
    j: usize,
    arg: Option<&Vector>,
    keep: Option<&[bool]>,
    sel: Option<&[u32]>,
) {
    let row = |k: usize| sel.map_or(k, |s| s[k] as usize);
    // A filtered-out row contributes nothing to this aggregate: it must not
    // reach the accumulator at all, which is what `count(*) FILTER` needs.
    let keep = |i: usize| keep.is_none_or(|k| k[i]);
    let Some(v) = arg else {
        for (k, &g) in gids.iter().enumerate() {
            let i = row(k);
            if keep(i) {
                groups.acc(g, j).count_star();
            }
        }
        return;
    };
    let generic = gids
        .first()
        .is_some_and(|&g| matches!(groups.acc(g, j), Acc::MinMax { .. } | Acc::Collect(_)));
    match v.data() {
        VectorData::Int32(d) if !generic => {
            for (k, &g) in gids.iter().enumerate() {
                let i = row(k);
                if keep(i) && v.is_valid(i) {
                    groups.acc(g, j).add_int(d[i] as i64);
                }
            }
        }
        VectorData::Int64(d) if !generic => {
            for (k, &g) in gids.iter().enumerate() {
                let i = row(k);
                if keep(i) && v.is_valid(i) {
                    groups.acc(g, j).add_int(d[i]);
                }
            }
        }
        VectorData::Float64(d) if !generic => {
            for (k, &g) in gids.iter().enumerate() {
                let i = row(k);
                if keep(i) && v.is_valid(i) {
                    groups.acc(g, j).add_f64(d[i]);
                }
            }
        }
        // DECIMAL is `accumulate`'s other hot payload type (a decimal SUM or
        // AVG over a scan); `update_vec` would re-dispatch per row.
        // Bulk: sum each group's cells of the chunk in a local `i128` and
        // hand every accumulator one sum. Exact for DECIMAL(≤18): a chunk's
        // cells cannot overflow the local sum, and the accumulators' own
        // overflow rules see the same totals (see `takes_decimal_bulk`).
        VectorData::Decimal {
            values,
            scale,
            precision,
        } if !generic
            && *precision <= 18
            && groups.group_count() <= DECIMAL_BULK_GROUPS
            && (0..groups.group_count())
                .all(|g| groups.acc_ref(g, j).takes_decimal_bulk(*scale)) =>
        {
            let mut sums = vec![0i128; groups.group_count()];
            let mut counts = vec![0i64; groups.group_count()];
            for (k, &g) in gids.iter().enumerate() {
                let i = row(k);
                if keep(i) && v.is_valid(i) {
                    sums[g] += values[i];
                    counts[g] += 1;
                }
            }
            for (g, (&sum, &n)) in sums.iter().zip(&counts).enumerate() {
                if n > 0 {
                    groups.acc(g, j).add_decimal_bulk(sum, n, *scale);
                }
            }
        }
        VectorData::Decimal { values, scale, .. } if !generic => {
            for (k, &g) in gids.iter().enumerate() {
                let i = row(k);
                if keep(i) && v.is_valid(i) {
                    groups.acc(g, j).add_decimal(values[i], *scale);
                }
            }
        }
        _ => {
            for (k, &g) in gids.iter().enumerate() {
                let i = row(k);
                if keep(i) {
                    groups.acc(g, j).update_vec(v, i);
                }
            }
        }
    }
}

/// Most local groups the bulk DECIMAL accumulation sizes its per-group sums
/// for (a chunk has at most `CHUNK_SIZE` groups of its own).
const DECIMAL_BULK_GROUPS: usize = 2 * CHUNK_SIZE;

/// Above this many local groups, phase 2 partitions the keys by hash and
/// merges the partitions in parallel ([`merge_by_bucket`]).
pub(crate) const PARALLEL_MERGE_MIN: usize = 1 << 14;

/// Local groups per radix partition: large enough to amortize the per-bucket
/// table, small enough that a bucket's table stays in cache.
pub(crate) const MERGE_BUCKET_GROUPS: usize = 4096;

/// Cap on partitions: more tasks than this only adds per-bucket overhead.
pub(crate) const MERGE_BUCKETS_MAX: usize = 64;

/// Merge per-chunk partials in chunk order (deterministic first-seen order).
/// Partial groups already sit in first-seen order, so each partial is walked
/// directly; the global table only maps encoded keys to slots for later
/// partials. Reserve the upper bound (Σ local groups) so neither the table
/// nor the output vec rehashes/regrows while it grows.
///
/// A large merge (wave 111c) walks the partials per hash partition instead, in
/// parallel: the serial walk's cost is one cache miss per local group in a
/// table sized for *every* distinct key of the input, which dwarfed the fold
/// that produced them (TPC-H Q18: 6M local groups into a 1.5M-entry table).
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
        let mut accs: Vec<Acc> = aggs
            .iter()
            .map(|a| Acc::new(a.func, a.distinct, a.direct))
            .collect();
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
            accs: aggs
                .iter()
                .map(|a| Acc::new(a.func, a.distinct, a.direct))
                .collect(),
        });
    }
    groups
}

/// One group of a partial on its way to its merge partition: the partition,
/// its index in the partial (first-seen order), its encoded key, and the
/// group itself until the partition's merge takes it.
struct ScatteredGroup {
    bucket: u32,
    pos: u32,
    enc: Vec<u8>,
    group: Option<Group>,
}

/// Hash-partitioned phase-2 merge: every partition gets its own table of a
/// few thousand entries and the partitions merge in parallel; the first-seen
/// position of each merged group is recorded so the output comes back in the
/// order the serial walk would have produced.
///
/// Each partial is sorted by partition once, on the thread that produced it,
/// and each partition merges its contiguous run of every partial; only slice
/// borrows move between threads. Sizing follows DuckDB's radix-partitioned
/// aggregate (`radix_partitioned_hashtable.cpp`); the partitioning keeps the
/// probe working set cache-resident instead of one table per distinct key.
fn merge_by_bucket(partials: Vec<Partial>, total_groups: usize) -> MergedBuckets {
    let buckets = (total_groups / MERGE_BUCKET_GROUPS)
        .next_power_of_two()
        .clamp(1, MERGE_BUCKETS_MAX);
    // Any hash works as long as equal keys agree, but FNV's low bits barely
    // mix (each step only multiplies upward), so take the top bits of a
    // Fibonacci scramble instead of masking — masking put every key of the
    // same length into a handful of buckets, which cost all of the speedup.
    let shift = (64 - buckets.trailing_zeros()).min(63);
    let bucket_of = |enc: &[u8]| {
        let mut h = std::hash::BuildHasher::build_hasher(&crate::types::FnvBuild);
        std::hash::Hasher::write(&mut h, enc);
        let h = std::hash::Hasher::finish(&h);
        (h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> shift) as usize
    };
    // Scatter: each partial's groups are stably sorted by partition, so a
    // partition's share of the partial is one contiguous run (in first-seen
    // order) and the partial costs one allocation instead of one growing
    // list per partition. Each entry keeps its index in the partial — its
    // exact first-seen position there.
    let mut scattered: Vec<(Vec<ScatteredGroup>, Vec<usize>)> = partials
        .into_par_iter()
        .map(|partial| {
            let mut entries: Vec<ScatteredGroup> = partial
                .groups
                .into_iter()
                .enumerate()
                .map(|(pos, (enc, group))| ScatteredGroup {
                    bucket: bucket_of(&enc) as u32,
                    pos: pos as u32,
                    enc,
                    group: Some(group),
                })
                .collect();
            entries.sort_by_key(|e| e.bucket);
            let mut starts = vec![0usize; buckets + 1];
            for e in &entries {
                starts[e.bucket as usize + 1] += 1;
            }
            for b in 0..buckets {
                starts[b + 1] += starts[b];
            }
            (entries, starts)
        })
        .collect();
    // Hand each partition its run of every partial, in partial order.
    let mut work: Vec<Vec<(u32, &mut [ScatteredGroup])>> = (0..buckets)
        .map(|_| Vec::with_capacity(scattered.len()))
        .collect();
    for (partial, (entries, starts)) in scattered.iter_mut().enumerate() {
        let mut rest: &mut [ScatteredGroup] = entries;
        for b in 0..buckets {
            let (run, tail) = rest.split_at_mut(starts[b + 1] - starts[b]);
            rest = tail;
            if !run.is_empty() {
                work[b].push((partial as u32, run));
            }
        }
    }
    let merged: Vec<Vec<(u64, Group)>> = work
        .into_par_iter()
        .map(|runs| {
            let total: usize = runs.iter().map(|(_, r)| r.len()).sum();
            let mut table: FastMap<Vec<u8>, usize> =
                FastMap::with_capacity_and_hasher(total, Default::default());
            let mut out: Vec<(u64, Group)> = Vec::with_capacity(total);
            for (partial, run) in runs {
                for e in run.iter_mut() {
                    let group = e.group.take().expect("each group merges once");
                    match table.entry(std::mem::take(&mut e.enc)) {
                        std::collections::hash_map::Entry::Occupied(slot) => {
                            let target = &mut out[*slot.get()].1;
                            for (a, b) in target.accs.iter_mut().zip(group.accs) {
                                a.merge(b);
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(out.len());
                            // First-seen position: partial, then the group's
                            // index in it.
                            let seen = u64::from(partial) << 32 | u64::from(e.pos);
                            out.push((seen, group));
                        }
                    }
                }
            }
            out
        })
        .collect();
    // Every partition is already in first-seen order, so sorting the runs by
    // their head position restores the serial walk's global order.
    let mut order: Vec<(u64, u32, u32)> = Vec::with_capacity(total_groups);
    for (b, list) in merged.iter().enumerate() {
        for (i, (seen, _)) in list.iter().enumerate() {
            order.push((*seen, b as u32, i as u32));
        }
    }
    // Positions are unique, so an unstable (parallel) sort is exact.
    order.par_sort_unstable_by_key(|e| e.0);
    MergedBuckets { merged, order }
}

/// The partitions [`merge_by_bucket`] merged, plus every group's
/// `(first-seen position, partition, index)` sorted into the serial walk's
/// order. The groups stay where their partition put them; emitting reads
/// them in `order` by reference, so millions of groups are never moved into
/// one list just to be split into chunks again.
struct MergedBuckets {
    merged: Vec<Vec<(u64, Group)>>,
    order: Vec<(u64, u32, u32)>,
}

/// Merge per-chunk partials and emit the groups as output chunks: phase 2 and
/// the emit of every hash aggregate. A large keyed merge (at least
/// [`PARALLEL_MERGE_MIN`] partial groups) merges by partition in parallel and
/// builds each output chunk in parallel straight from the partitions; smaller
/// ones merge serially and go through [`emit_groups`]. Both produce the
/// groups in first-seen order.
pub(crate) fn merge_and_emit(
    partials: Vec<Partial>,
    key_types: &[DataType],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    let has_keys = !key_types.is_empty();
    let total_groups: usize = partials.iter().map(|p| p.groups.len()).sum();
    if !(has_keys && total_groups >= PARALLEL_MERGE_MIN) {
        return emit_groups(merge_partials(partials, has_keys, aggs), key_types, aggs);
    }
    let MergedBuckets { merged, order } = merge_by_bucket(partials, total_groups);
    let built: Vec<Result<DataChunk>> = order
        .par_chunks(CHUNK_SIZE)
        .map(|part| {
            let mut cols = Vec::with_capacity(key_types.len() + aggs.len());
            for t in key_types {
                cols.push(Vector::with_capacity(*t, part.len()));
            }
            for a in aggs {
                cols.push(Vector::with_capacity(a.dtype, part.len()));
            }
            for &(_, b, i) in part {
                let g = &merged[b as usize][i as usize].1;
                for (col, v) in cols.iter_mut().zip(g.keys.iter()) {
                    col.push_value(v.clone())?;
                }
                for (col, (acc, a)) in cols[key_types.len()..]
                    .iter_mut()
                    .zip(g.accs.iter().zip(aggs))
                {
                    col.push_value(acc.finalize_ref(a.dtype)?)?;
                }
            }
            DataChunk::new(cols)
        })
        .collect();
    built.into_iter().collect()
}

/// Emit merged groups as output chunks: `[keys..., aggs...]`, chunked at
/// [`CHUNK_SIZE`]. `key_types` are the group keys' bound output dtypes.
///
/// Each output chunk is finalized and built on its own (in parallel: a
/// high-cardinality `GROUP BY` emits millions of groups, and this was its
/// serial tail). Chunks stay in group order, and when several groups fail to
/// finalize (an out-of-range sum), the error reported is the first in that
/// order, as the serial loop reported it.
pub(crate) fn emit_groups(
    groups: Vec<Group>,
    key_types: &[DataType],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    let build = |part: Vec<Group>| -> Result<DataChunk> {
        let mut cols = Vec::with_capacity(key_types.len() + aggs.len());
        for t in key_types {
            cols.push(Vector::with_capacity(*t, part.len()));
        }
        for a in aggs {
            cols.push(Vector::with_capacity(a.dtype, part.len()));
        }
        for g in part {
            let finalized: Vec<Value> = g
                .accs
                .into_iter()
                .zip(aggs.iter())
                .map(|(acc, a)| acc.finalize(a.dtype))
                .collect::<Result<_>>()?;
            for (col, v) in cols.iter_mut().zip(g.keys.into_iter().chain(finalized)) {
                col.push_value(v)?;
            }
        }
        DataChunk::new(cols)
    };
    if groups.is_empty() {
        return Ok(vec![build(Vec::new())?]);
    }
    let built: Vec<Result<DataChunk>> = groups
        .into_par_iter()
        .chunks(CHUNK_SIZE)
        .map(build)
        .collect();
    built.into_iter().collect()
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
        let mut acc = Acc::new(AggFunc::Sum, false, None);
        for _ in 0..5 {
            acc.update(Some(&Value::Bigint(i64::MAX)));
        }
        let mut other = Acc::new(AggFunc::Sum, false, None);
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
        let mut big = Acc::new(AggFunc::Sum, false, None);
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

    /// xorshift64, so the differential case is repeatable.
    fn next_rand(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    fn test_chunk(rows: usize, state: &mut u64) -> DataChunk {
        let mut k = Vector::new_empty(DataType::Integer);
        let mut s = Vector::new_empty(DataType::Varchar);
        let mut d = Vector::new_empty(DataType::Decimal {
            precision: 15,
            scale: 2,
        });
        let mut b = Vector::new_empty(DataType::Bigint);
        let mut x = Vector::new_empty(DataType::Double);
        for i in 0..rows {
            let roll = next_rand(state) % 7;
            if i == 0 || roll == 0 {
                k.push_null().unwrap();
                s.push_null().unwrap();
                d.push_null().unwrap();
                b.push_null().unwrap();
                x.push_null().unwrap();
                continue;
            }
            k.push_valid(Value::Integer((next_rand(state) % 4) as i32))
                .unwrap();
            s.push_str(["", "apple", "banana", "cherry"][(next_rand(state) % 4) as usize])
                .unwrap();
            let mag = (next_rand(state) % 100_000) as i128;
            let value = if next_rand(state).is_multiple_of(2) {
                mag
            } else {
                -mag
            };
            d.push_valid(Value::Decimal(value, 2)).unwrap();
            b.push_valid(Value::Bigint((next_rand(state) % 2001) as i64 - 1000))
                .unwrap();
            x.push_valid(Value::Double((next_rand(state) % 2001) as f64 / 8.0 - 12.5))
                .unwrap();
        }
        DataChunk::new(vec![k, s, d, b, x]).unwrap()
    }

    fn aggs() -> Vec<BoundAgg> {
        let col = |index: usize, dtype: DataType| Some(BoundExpr::Column { index, dtype });
        let decimal = DataType::Decimal {
            precision: 15,
            scale: 2,
        };
        let mk = |func, arg, dtype, filter| BoundAgg {
            func,
            arg,
            dtype,
            distinct: false,
            filter,
            direct: None,
        };
        vec![
            mk(AggFunc::Count, None, DataType::Bigint, None),
            mk(AggFunc::Sum, col(2, decimal), decimal, None),
            mk(AggFunc::Avg, col(2, decimal), DataType::Double, None),
            mk(
                AggFunc::Min,
                col(3, DataType::Bigint),
                DataType::Bigint,
                None,
            ),
            mk(AggFunc::Max, col(2, decimal), decimal, None),
            mk(
                AggFunc::Sum,
                col(3, DataType::Bigint),
                DataType::Bigint,
                None,
            ),
            mk(
                AggFunc::Avg,
                col(4, DataType::Double),
                DataType::Double,
                None,
            ),
            // A FILTER under a selection: `keep` stays indexed by the chunk's
            // absolute row while the fold walks the selection.
            mk(
                AggFunc::Sum,
                col(3, DataType::Bigint),
                DataType::Bigint,
                Some(BoundExpr::Binary {
                    op: crate::sql::ast::BinaryOp::Gt,
                    left: Box::new(BoundExpr::Column {
                        index: 4,
                        dtype: DataType::Double,
                    }),
                    right: Box::new(BoundExpr::Literal(Value::Double(0.0))),
                    dtype: DataType::Boolean,
                }),
            ),
        ]
    }

    fn rows_of(chunks: &[DataChunk]) -> Vec<Vec<Value>> {
        chunks
            .iter()
            .flat_map(|c| (0..c.num_rows()).map(move |i| (c, i)))
            .map(|(c, i)| {
                (0..c.columns.len())
                    .map(|j| c.expect_column(j).unwrap().get(i))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn folding_through_a_selection_matches_folding_a_compacted_copy() {
        // The selection path indexes the key/argument vectors instead of
        // copying them; a compacted copy has to produce the same groups, in
        // the same first-seen order, with the same accumulators.
        let db = crate::storage::PinkDb::new();
        let cx = Cx::new(&db);
        let mut state = 0x1234_5678_9abc_def0u64;
        let chunk = test_chunk(300, &mut state);
        let keysets = [
            vec![BoundExpr::Column {
                index: 0,
                dtype: DataType::Integer,
            }],
            vec![BoundExpr::Column {
                index: 1,
                dtype: DataType::Varchar,
            }],
            vec![
                BoundExpr::Column {
                    index: 0,
                    dtype: DataType::Integer,
                },
                BoundExpr::Column {
                    index: 1,
                    dtype: DataType::Varchar,
                },
            ],
        ];
        for keys in &keysets {
            let aggs = aggs();
            // Everything selected: the two folds must agree with the plain
            // unfiltered aggregate.
            let all: Vec<u32> = (0..chunk.num_rows() as u32).collect();
            let plain = aggregate(&cx, std::slice::from_ref(&chunk), keys, &aggs).unwrap();
            let via_selection = aggregate_selected(
                &cx,
                &[Selected {
                    chunk: chunk.clone(),
                    sel: all.clone(),
                }],
                keys,
                &aggs,
            )
            .unwrap();
            assert_eq!(rows_of(&via_selection), rows_of(&plain));
            // A sparse selection, with holes and a NULL-key row dropped.
            let mut sel: Vec<u32> = all
                .iter()
                .copied()
                .filter(|&r| r % 3 == 0 || r % 7 == 0)
                .collect();
            sel.sort_unstable();
            let compacted = chunk.take(&sel);
            let want = aggregate(&cx, &[compacted], keys, &aggs).unwrap();
            let got = aggregate_selected(
                &cx,
                &[Selected {
                    chunk: chunk.clone(),
                    sel: sel.clone(),
                }],
                keys,
                &aggs,
            )
            .unwrap();
            assert_eq!(rows_of(&got), rows_of(&want), "keys {keys:?} sel {sel:?}");
        }
    }
}
