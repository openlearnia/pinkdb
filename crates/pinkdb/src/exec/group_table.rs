//! Flat GROUP BY tables for fixed-width keys (wave 123d).
//!
//! The general aggregate ([`super::aggregate`]) keeps every group as its own
//! heap objects: an encoded key, the first-seen key `Value`s and a list of
//! accumulators, so a chunk of 2,048 distinct keys allocates thousands of
//! times and the cross-chunk merge moves every one of them. When every key
//! is a fixed-width number (INTEGER, BIGINT, DATE, TIMESTAMP, BOOLEAN,
//! DECIMAL(≤18)) or a VARCHAR (wave 123e: its word is the string's hash and
//! the group keeps the shared string) and no aggregate collects values (`COUNT(DISTINCT)`, the
//! ordered-set aggregates), a group is instead one row of three flat arrays:
//! its key as `u64` words (plus one word of NULL bits), its hash, and its
//! accumulators. The layout follows DuckDB's `GroupedAggregateHashTable`
//! (`src/execution/aggregate_hashtable.cpp`): groups compared as fixed-width
//! rows, the hash stored beside them, and the merge partitioned by the hash's
//! radix bits (`RadixPartitionedHashTable`).
//!
//! Phase 1 still folds one chunk into one partial, and phase 2 merges the
//! partials in chunk order, so groups come out in first-seen order and every
//! accumulator merges in the same order as on the general path: results are
//! identical, float sums included.

use super::Cx;
use super::aggregate::{
    Acc, AccSlots, EvalVec, MERGE_BUCKET_GROUPS, MERGE_BUCKETS_MAX, PARALLEL_MERGE_MIN, SharedArgs,
    accumulate, eval_agg_vec, keep_mask, small_column_codes,
};
use super::selection::Selected;
use crate::sql::binder::{AggFunc, BoundAgg, BoundExpr};
use crate::types::{DataType, Result};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector, VectorData};
use rayon::prelude::*;
use std::sync::Arc;

/// How one key column's cells become a `u64` word, and back.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Bool,
    Int32,
    Date32,
    Int64,
    Timestamp,
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// VARCHAR: the word is the string's hash, and the group keeps the
    /// string itself (a shared `Arc<str>`, no copy) to compare and emit.
    Varchar,
}

impl KeyKind {
    fn of(dtype: DataType) -> Option<KeyKind> {
        Some(match dtype {
            DataType::Boolean => KeyKind::Bool,
            DataType::Integer => KeyKind::Int32,
            DataType::Date => KeyKind::Date32,
            DataType::Bigint => KeyKind::Int64,
            DataType::Timestamp => KeyKind::Timestamp,
            DataType::Varchar => KeyKind::Varchar,
            DataType::Decimal { precision, scale } if precision <= 18 => {
                KeyKind::Decimal { precision, scale }
            }
            _ => return None,
        })
    }

    /// The vector's payload is this kind's layout (an expression may produce
    /// a different one than the bound type promises; the general path then
    /// takes the query).
    fn matches(self, v: &Vector) -> bool {
        match (self, v.data()) {
            (KeyKind::Bool, VectorData::Boolean(_))
            | (KeyKind::Int32, VectorData::Int32(_))
            | (KeyKind::Date32, VectorData::Date32(_))
            | (KeyKind::Int64, VectorData::Int64(_))
            | (KeyKind::Timestamp, VectorData::TimestampMicros(_))
            | (KeyKind::Varchar, VectorData::VarChar(_)) => true,
            (
                KeyKind::Decimal { scale, .. },
                VectorData::Decimal {
                    precision: p,
                    scale: s,
                    ..
                },
            ) => *s == scale && *p <= 18,
            _ => false,
        }
    }
}

/// The key kinds of a GROUP BY the flat table handles, or `None`.
fn eligible(keys: &[BoundExpr], aggs: &[BoundAgg]) -> Option<Vec<KeyKind>> {
    if keys.is_empty() || keys.len() > 63 {
        return None;
    }
    let simple = aggs.iter().all(|a| {
        !a.distinct
            && matches!(
                a.func,
                AggFunc::Count | AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max
            )
    });
    if !simple {
        return None;
    }
    keys.iter().map(|k| KeyKind::of(k.dtype())).collect()
}

/// One chunk's groups: `keys` holds `width` words per group (one per key
/// column, then a word of NULL bits), `accs` one accumulator per aggregate.
/// Once folded, the groups are laid out by merge partition (`starts` holds
/// the partition boundaries) and `pos` keeps each one's first-seen index in
/// the chunk, so a partition's merge reads one contiguous run per partial.
struct FlatPartial {
    width: usize,
    naggs: usize,
    /// VARCHAR key columns: `strs` holds `nstr` strings per group (an empty
    /// one for a NULL cell, whose NULL bit tells it apart from `''`).
    nstr: usize,
    strs: Vec<Arc<str>>,
    keys: Vec<u64>,
    hashes: Vec<u64>,
    accs: Vec<Acc>,
    pos: Vec<u32>,
    starts: Vec<usize>,
}

/// What every chunk's fold shares: the GROUP BY, the accumulators a new
/// group starts with, and the number of merge partitions.
struct Spec<'a> {
    keys: &'a [BoundExpr],
    kinds: Vec<KeyKind>,
    /// The key columns that are VARCHAR, in order.
    str_cols: Vec<usize>,
    aggs: &'a [BoundAgg],
    fresh: Vec<Acc>,
    buckets: usize,
    /// Arithmetic the aggregate arguments share, evaluated once per chunk.
    shared: Option<SharedArgs>,
}

impl<'a> Spec<'a> {
    /// `None` when the GROUP BY is not one the flat table handles.
    fn new(
        keys: &'a [BoundExpr],
        aggs: &'a [BoundAgg],
        rows: usize,
        width: usize,
    ) -> Option<Spec<'a>> {
        let kinds = eligible(keys, aggs)?;
        Some(Spec {
            str_cols: (0..kinds.len())
                .filter(|&c| kinds[c] == KeyKind::Varchar)
                .collect(),
            kinds,
            keys,
            aggs,
            fresh: aggs
                .iter()
                .map(|a| Acc::new(a.func, a.distinct, a.direct))
                .collect(),
            buckets: partitions_for(rows),
            shared: SharedArgs::new(aggs, width),
        })
    }
}

impl FlatPartial {
    #[inline]
    fn key(&self, g: usize) -> &[u64] {
        &self.keys[g * self.width..(g + 1) * self.width]
    }

    #[inline]
    fn len(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    fn strs_of(&self, g: usize) -> &[Arc<str>] {
        &self.strs[g * self.nstr..(g + 1) * self.nstr]
    }
}

impl FlatPartial {
    /// Reorder the groups by merge partition (a stable counting sort, on the
    /// thread that folded them while they are still in cache).
    fn partition(&mut self, buckets: usize) {
        let n = self.len();
        if buckets == 1 {
            self.pos = (0..n as u32).collect();
            self.starts = vec![0, n];
            return;
        }
        let mut starts = vec![0usize; buckets + 1];
        for &h in &self.hashes {
            starts[bucket_of(h, buckets) + 1] += 1;
        }
        for b in 0..buckets {
            starts[b + 1] += starts[b];
        }
        let mut next = starts.clone();
        let mut perm = vec![0u32; n];
        for (g, &h) in self.hashes.iter().enumerate() {
            let b = bucket_of(h, buckets);
            perm[next[b]] = g as u32;
            next[b] += 1;
        }
        let (width, naggs) = (self.width, self.naggs);
        let mut keys = Vec::with_capacity(self.keys.len());
        let mut hashes = Vec::with_capacity(n);
        let mut accs = Vec::with_capacity(self.accs.len());
        let mut strs = Vec::with_capacity(self.strs.len());
        for &g in &perm {
            let g = g as usize;
            keys.extend_from_slice(&self.keys[g * width..(g + 1) * width]);
            strs.extend_from_slice(self.strs_of(g));
            hashes.push(self.hashes[g]);
            for acc in &mut self.accs[g * naggs..(g + 1) * naggs] {
                accs.push(std::mem::replace(acc, Acc::Count(0)));
            }
        }
        self.keys = keys;
        self.hashes = hashes;
        self.accs = accs;
        self.strs = strs;
        self.pos = perm;
        self.starts = starts;
    }
}

/// The merge partition of a group hash: its top bits.
#[inline]
fn bucket_of(h: u64, buckets: usize) -> usize {
    if buckets == 1 {
        0
    } else {
        (h >> (64 - buckets.trailing_zeros())) as usize
    }
}

/// Merge partitions for an aggregate over `rows` input rows: sized for the
/// most groups it could have, so every partial is laid out by partition
/// while it is folded.
fn partitions_for(rows: usize) -> usize {
    if rows < PARALLEL_MERGE_MIN {
        1
    } else {
        (rows / MERGE_BUCKET_GROUPS)
            .next_power_of_two()
            .clamp(1, MERGE_BUCKETS_MAX)
    }
}

impl AccSlots for FlatPartial {
    #[inline]
    fn group_count(&self) -> usize {
        self.hashes.len()
    }
    #[inline]
    fn acc(&mut self, g: usize, j: usize) -> &mut Acc {
        &mut self.accs[g * self.naggs + j]
    }
    #[inline]
    fn acc_ref(&self, g: usize, j: usize) -> &Acc {
        &self.accs[g * self.naggs + j]
    }
}

/// Hash of one key row: each word folded in with a multiply, then
/// MurmurHash3's finalizer (DuckDB's `CombineHash` + `MurmurHash64`), so
/// both the low bits (table slots) and the high bits (merge partitions) are
/// well mixed.
#[inline]
fn hash_words(words: &[u64]) -> u64 {
    let mut h = 0x9e37_79b9_7f4a_7c15u64;
    for &w in words {
        h = (h ^ w).wrapping_mul(0xbf58_476d_1ce4_e5b9).rotate_left(29);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// Open-addressing slot table over group indices (`u32::MAX` is empty),
/// sized for at least twice `groups` entries.
fn slot_table(groups: usize) -> (Vec<u32>, usize) {
    let cap = (groups * 2).next_power_of_two().max(16);
    (vec![u32::MAX; cap], cap - 1)
}

/// Write column `c` of every selected row into `words` (`width` words per
/// row), setting the row's NULL bit for a NULL cell.
fn fill_words(
    words: &mut [u64],
    width: usize,
    c: usize,
    v: &Vector,
    rows: usize,
    sel: Option<&[u32]>,
) {
    let null_word = width - 1;
    let nulls = v.has_nulls();
    let mut put = |cell: &dyn Fn(usize) -> u64| {
        for k in 0..rows {
            let i = sel.map_or(k, |s| s[k] as usize);
            if nulls && v.is_null(i) {
                words[k * width + null_word] |= 1 << c;
            } else {
                words[k * width + c] = cell(i);
            }
        }
    };
    match v.data() {
        VectorData::Boolean(d) => put(&|i| u64::from(d[i])),
        VectorData::Int32(d) | VectorData::Date32(d) => put(&|i| i64::from(d[i]) as u64),
        VectorData::Int64(d) | VectorData::TimestampMicros(d) => put(&|i| d[i] as u64),
        // DECIMAL(≤18) values fit an i64 (`KeyKind::matches`).
        VectorData::Decimal { values, .. } => put(&|i| values[i] as i64 as u64),
        VectorData::VarChar(d) => put(&|i| super::hash_bytes_key(d[i].as_bytes())),
        _ => unreachable!("KeyKind::matches admits only fixed-width payloads"),
    }
}

/// Fold one chunk (or the `sel` rows of it) into a fresh flat partial;
/// `Ok(None)` when a key column's payload is not the layout its type
/// promises.
fn fold(
    cx: &Cx,
    chunk: &DataChunk,
    sel: Option<&[u32]>,
    spec: &Spec,
) -> Result<Option<FlatPartial>> {
    cx.check_deadline()?;
    let (keys, aggs) = (spec.keys, spec.aggs);
    let key_vecs: Vec<EvalVec> = keys
        .iter()
        .map(|k| eval_agg_vec(k, chunk, cx))
        .collect::<Result<_>>()?;
    if !key_vecs
        .iter()
        .zip(&spec.kinds)
        .all(|(kv, kind)| kind.matches(kv.v()))
    {
        return Ok(None);
    }
    let extended;
    let (chunk, args): (&DataChunk, Vec<Option<&BoundExpr>>) = match &spec.shared {
        Some(shared) => {
            extended = shared.extend(chunk, cx)?;
            (&extended, (0..aggs.len()).map(|j| shared.arg(j)).collect())
        }
        None => (chunk, aggs.iter().map(|a| a.arg.as_ref()).collect()),
    };
    let arg_vecs: Vec<Option<EvalVec>> = args
        .iter()
        .map(|a| match a {
            Some(e) => eval_agg_vec(e, chunk, cx).map(Some),
            None => Ok(None),
        })
        .collect::<Result<_>>()?;
    let keeps: Vec<Option<Vec<bool>>> = aggs
        .iter()
        .map(|a| match &a.filter {
            Some(pred) => keep_mask(eval_agg_vec(pred, chunk, cx)?.v()).map(Some),
            None => Ok(None),
        })
        .collect::<Result<_>>()?;
    let rows = sel.map_or(chunk.num_rows(), |s| s.len());
    let width = keys.len() + 1;
    let nstr = spec.str_cols.len();
    // Row `k`'s string in VARCHAR key column `c` (`""` for NULL).
    let empty: Arc<str> = Arc::from("");
    let cell = |c: usize, k: usize| -> &Arc<str> {
        let v = key_vecs[c].v();
        let i = sel.map_or(k, |s| s[k] as usize);
        match v.data() {
            VectorData::VarChar(d) if v.is_valid(i) => &d[i],
            _ => &empty,
        }
    };
    let mut part = FlatPartial {
        width,
        naggs: aggs.len(),
        nstr,
        strs: Vec::new(),
        keys: Vec::new(),
        hashes: Vec::new(),
        accs: Vec::new(),
        pos: Vec::new(),
        starts: Vec::new(),
    };
    let new_group = |part: &mut FlatPartial, key: &[u64], h: u64, k: usize| -> u32 {
        let g = part.len() as u32;
        part.keys.extend_from_slice(key);
        part.hashes.push(h);
        part.accs.extend_from_slice(&spec.fresh);
        for &c in &spec.str_cols {
            part.strs.push(Arc::clone(cell(c, k)));
        }
        g
    };
    let mut gids = Vec::with_capacity(rows);
    // A chunk whose key columns each hold few values (TPC-H Q1's two flags)
    // codes every column by a short scan and indexes a dense table, so only
    // a group's first row is turned into words and hashed (DuckDB's
    // `PerfectHashAggregate`; the general path's `small_domain_gids`).
    let small = if key_vecs.len() <= 3 {
        key_vecs
            .iter()
            .map(|kv| small_column_codes(kv.v(), rows, |k| sel.map_or(k, |s| s[k] as usize)))
            .collect::<Option<Vec<_>>>()
    } else {
        None
    };
    if let Some(columns) = small {
        let size: usize = columns.iter().map(|(_, n)| (*n).max(1)).product();
        let mut slots = vec![u32::MAX; size];
        let mut one = vec![0u64; width];
        for k in 0..rows {
            let mut code = 0usize;
            for (codes, n) in columns.iter().rev() {
                code = code * (*n).max(1) + codes[k] as usize;
            }
            if slots[code] == u32::MAX {
                let row = [sel.map_or(k as u32, |s| s[k])];
                one.fill(0);
                for (c, kv) in key_vecs.iter().enumerate() {
                    fill_words(&mut one, width, c, kv.v(), 1, Some(&row));
                }
                let h = hash_words(&one);
                slots[code] = new_group(&mut part, &one, h, k);
            }
            gids.push(slots[code] as usize);
        }
    } else {
        let mut words = vec![0u64; rows * width];
        for (c, kv) in key_vecs.iter().enumerate() {
            fill_words(&mut words, width, c, kv.v(), rows, sel);
        }
        let (mut slots, mask) = slot_table(rows);
        for (k, key) in words.chunks_exact(width).enumerate() {
            let h = hash_words(key);
            let mut s = h as usize & mask;
            let gid = loop {
                let g = slots[s];
                if g == u32::MAX {
                    let g = new_group(&mut part, key, h, k);
                    slots[s] = g;
                    break g;
                }
                if part.hashes[g as usize] == h
                    && part.key(g as usize) == key
                    && spec
                        .str_cols
                        .iter()
                        .zip(part.strs_of(g as usize))
                        .all(|(&c, s)| **s == **cell(c, k))
                {
                    break g;
                }
                s = (s + 1) & mask;
            };
            gids.push(gid as usize);
        }
    }
    for (j, av) in arg_vecs.iter().enumerate() {
        accumulate(
            &mut part,
            &gids,
            j,
            av.as_ref().map(|v| v.v()),
            keeps[j].as_deref(),
            sel,
        );
    }
    part.partition(spec.buckets);
    Ok(Some(part))
}

/// One merge partition's groups, each with its first-seen position
/// (`partial << 32 | index in the partial`).
struct Merged {
    keys: Vec<u64>,
    strs: Vec<Arc<str>>,
    accs: Vec<Acc>,
    first: Vec<u64>,
}

/// Merge the partials (in chunk order, so first-seen order and the order
/// accumulators merge in match the general path) and emit the groups as
/// `[keys..., aggs...]` chunks in first-seen order.
fn merge_and_emit(
    partials: Vec<FlatPartial>,
    kinds: &[KeyKind],
    key_types: &[DataType],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    let width = kinds.len() + 1;
    let naggs = aggs.len();
    let nstr = kinds.iter().filter(|k| **k == KeyKind::Varchar).count();
    let total: usize = partials.iter().map(FlatPartial::len).sum();
    let buckets = partials.first().map_or(1, |p| p.starts.len() - 1);
    let merged: Vec<Merged> = (0..buckets)
        .into_par_iter()
        .map(|b| {
            let size: usize = partials.iter().map(|p| p.starts[b + 1] - p.starts[b]).sum();
            let (mut slots, mask) = slot_table(size);
            let mut out = Merged {
                keys: Vec::with_capacity(size * width),
                strs: Vec::with_capacity(size * nstr),
                accs: Vec::with_capacity(size * naggs),
                first: Vec::with_capacity(size),
            };
            let mut hashes: Vec<u64> = Vec::with_capacity(size);
            for (pi, p) in partials.iter().enumerate() {
                for g in p.starts[b]..p.starts[b + 1] {
                    let h = p.hashes[g];
                    let key = p.key(g);
                    let strs = p.strs_of(g);
                    let src = &p.accs[g * naggs..(g + 1) * naggs];
                    let mut s = h as usize & mask;
                    loop {
                        let t = slots[s];
                        if t == u32::MAX {
                            slots[s] = out.first.len() as u32;
                            out.keys.extend_from_slice(key);
                            out.strs.extend_from_slice(strs);
                            out.accs.extend_from_slice(src);
                            out.first.push((pi as u64) << 32 | u64::from(p.pos[g]));
                            hashes.push(h);
                            break;
                        }
                        let t = t as usize;
                        if hashes[t] == h
                            && out.keys[t * width..(t + 1) * width] == *key
                            && out.strs[t * nstr..(t + 1) * nstr] == *strs
                        {
                            for (acc, other) in
                                out.accs[t * naggs..(t + 1) * naggs].iter_mut().zip(src)
                            {
                                acc.merge_ref(other);
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
            out
        })
        .collect();
    drop(partials);
    // Every partition lists its groups in first-seen order; one sort by
    // position interleaves them into the global order.
    let mut order: Vec<(u64, u32, u32)> = Vec::with_capacity(total);
    for (b, m) in merged.iter().enumerate() {
        order.extend(
            m.first
                .iter()
                .enumerate()
                .map(|(i, &f)| (f, b as u32, i as u32)),
        );
    }
    if buckets > 1 {
        order.par_sort_unstable_by_key(|e| e.0);
    }
    let build = |part: &[(u64, u32, u32)]| -> Result<DataChunk> {
        let mut cols = Vec::with_capacity(kinds.len() + naggs);
        let mut slot = 0;
        for (c, kind) in kinds.iter().enumerate() {
            let word =
                |&(_, b, i): &(u64, u32, u32)| merged[b as usize].keys[i as usize * width + c];
            let data = match *kind {
                KeyKind::Bool => VectorData::Boolean(part.iter().map(|e| word(e) != 0).collect()),
                KeyKind::Int32 => {
                    VectorData::Int32(part.iter().map(|e| word(e) as i64 as i32).collect())
                }
                KeyKind::Date32 => {
                    VectorData::Date32(part.iter().map(|e| word(e) as i64 as i32).collect())
                }
                KeyKind::Int64 => VectorData::Int64(part.iter().map(|e| word(e) as i64).collect()),
                KeyKind::Timestamp => {
                    VectorData::TimestampMicros(part.iter().map(|e| word(e) as i64).collect())
                }
                KeyKind::Decimal { precision, scale } => VectorData::Decimal {
                    values: part.iter().map(|e| i128::from(word(e) as i64)).collect(),
                    precision,
                    scale,
                },
                KeyKind::Varchar => {
                    let at = slot;
                    slot += 1;
                    VectorData::VarChar(
                        part.iter()
                            .map(|&(_, b, i)| {
                                Arc::clone(&merged[b as usize].strs[i as usize * nstr + at])
                            })
                            .collect(),
                    )
                }
            };
            let mut validity = vec![u64::MAX; part.len().div_ceil(64)];
            for (r, &(_, b, i)) in part.iter().enumerate() {
                if merged[b as usize].keys[i as usize * width + width - 1] >> c & 1 == 1 {
                    validity[r / 64] &= !(1u64 << (r % 64));
                }
            }
            let v = Vector::from_raw(data, validity);
            debug_assert_eq!(v.dtype(), key_types[c]);
            cols.push(v);
        }
        for (j, a) in aggs.iter().enumerate() {
            let mut col = Vector::with_capacity(a.dtype, part.len());
            for &(_, b, i) in part {
                let acc = &merged[b as usize].accs[i as usize * naggs + j];
                col.push_value(acc.finalize_ref(a.dtype)?)?;
            }
            cols.push(col);
        }
        DataChunk::new(cols)
    };
    if order.is_empty() {
        return Ok(vec![build(&[])?]);
    }
    let built: Vec<Result<DataChunk>> = order.par_chunks(CHUNK_SIZE).map(build).collect();
    built.into_iter().collect()
}

/// [`super::aggregate::aggregate`] on flat tables, or `Ok(None)` when the
/// GROUP BY is not one they handle.
pub(crate) fn aggregate(
    cx: &Cx,
    inputs: &[DataChunk],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Option<Vec<DataChunk>>> {
    let rows = inputs.iter().map(DataChunk::num_rows).sum();
    let width = inputs.first().map_or(0, DataChunk::num_cols);
    let Some(spec) = Spec::new(keys, aggs, rows, width) else {
        return Ok(None);
    };
    let partials: Option<Vec<FlatPartial>> = inputs
        .par_iter()
        .map(|chunk| fold(cx, chunk, None, &spec))
        .collect::<Result<_>>()?;
    let Some(partials) = partials else {
        return Ok(None);
    };
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    merge_and_emit(partials, &spec.kinds, &key_types, aggs).map(Some)
}

/// [`super::aggregate::aggregate_selected`] on flat tables, or `Ok(None)`.
/// A fold error is retried on the kept rows alone, as there.
pub(crate) fn aggregate_selected(
    cx: &Cx,
    inputs: &[Selected],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Option<Vec<DataChunk>>> {
    let rows = inputs.iter().map(|i| i.sel.len()).sum();
    let width = inputs.first().map_or(0, |i| i.chunk.num_cols());
    let Some(spec) = Spec::new(keys, aggs, rows, width) else {
        return Ok(None);
    };
    let partials: Option<Vec<FlatPartial>> = inputs
        .par_iter()
        .map(
            |input| match fold(cx, &input.chunk, Some(&input.sel), &spec) {
                Ok(partial) => Ok(partial),
                Err(_) => fold_kept_rows(cx, input, &spec),
            },
        )
        .collect::<Result<_>>()?;
    let Some(partials) = partials else {
        return Ok(None);
    };
    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    merge_and_emit(partials, &spec.kinds, &key_types, aggs).map(Some)
}

/// The out-of-line retry of [`aggregate_selected`] (see
/// `aggregate::fold_kept_rows` for why it stays cold).
#[cold]
#[inline(never)]
fn fold_kept_rows(cx: &Cx, input: &Selected, spec: &Spec) -> Result<Option<FlatPartial>> {
    fold(cx, &input.chunk.take(&input.sel), None, spec)
}
