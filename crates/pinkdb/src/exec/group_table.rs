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
use crate::sql::binder::{AggFunc, BoundAgg, BoundExpr};
use crate::strings::StrVec;
use crate::types::{DataType, Result};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector, VectorData};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// string itself (a 16-byte view, wave 128a) to compare and emit.
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
/// The rows stay in first-seen order, which is the output order, and `ix`
/// lists them merge partition by merge partition, so a partition's merge
/// reads one run of indices per partial and no group's row is ever rewritten.
pub(crate) struct FlatPartial {
    width: usize,
    naggs: usize,
    /// One string per VARCHAR key column per group (an empty one for a NULL
    /// cell, whose NULL bit tells it apart from `''`).
    strs: StrVec,
    keys: Vec<u64>,
    hashes: Vec<u64>,
    accs: Vec<Acc>,
    /// The groups' first-seen indices, grouped by merge partition.
    ix: Vec<u32>,
    /// Where each merge partition's run of `ix` starts.
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
            fresh: aggs.iter().map(Acc::for_agg).collect(),
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
}

impl FlatPartial {
    /// Index the groups by merge partition (a stable counting sort, on the
    /// thread that folded them while their hashes are still in cache). The
    /// rows stay in first-seen order — the order the groups come out in — so
    /// only the indices move, not the keys and accumulators.
    fn index_buckets(&mut self, buckets: usize) {
        let n = self.len();
        if buckets == 1 {
            self.ix = (0..n as u32).collect();
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
        self.starts = starts;
        self.ix = perm;
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
        VectorData::VarChar(d) => put(&|i| crate::strings::view_hash(d, i)),
        _ => unreachable!("KeyKind::matches admits only fixed-width payloads"),
    }
}

impl Spec<'_> {
    /// Fold one chunk (or the `sel` rows of it) into a fresh flat partial;
    /// `Ok(None)` when a key column's payload is not the layout its type
    /// promises.
    fn fold(&self, cx: &Cx, chunk: &DataChunk, sel: Option<&[u32]>) -> Result<Option<FlatPartial>> {
        cx.check_deadline()?;
        let (keys, aggs) = (self.keys, self.aggs);
        let key_vecs: Vec<EvalVec> = keys
            .iter()
            .map(|k| eval_agg_vec(k, chunk, cx))
            .collect::<Result<_>>()?;
        if !key_vecs
            .iter()
            .zip(&self.kinds)
            .all(|(kv, kind)| kind.matches(kv.v()))
        {
            return Ok(None);
        }
        let extended;
        let (chunk, args): (&DataChunk, Vec<Option<&BoundExpr>>) = match &self.shared {
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
        let nstr = self.str_cols.len();
        // Row `k`'s string in VARCHAR key column `c` (`""` for NULL).
        let cell = |c: usize, k: usize| -> (&StrVec, usize, bool) {
            let v = key_vecs[c].v();
            let i = sel.map_or(k, |s| s[k] as usize);
            match v.data() {
                VectorData::VarChar(d) => (d, i, v.is_valid(i)),
                _ => unreachable!("KeyKind::Varchar admits only VARCHAR payloads"),
            }
        };
        let mut part = FlatPartial {
            width,
            naggs: aggs.len(),
            strs: StrVec::new(),
            keys: Vec::new(),
            hashes: Vec::new(),
            accs: Vec::new(),
            ix: Vec::new(),
            starts: Vec::new(),
        };
        let new_group = |part: &mut FlatPartial, key: &[u64], h: u64, k: usize| -> u32 {
            let g = part.len() as u32;
            part.keys.extend_from_slice(key);
            part.hashes.push(h);
            part.accs.extend_from_slice(&self.fresh);
            for &c in &self.str_cols {
                match cell(c, k) {
                    // Copied: a group's key outlives the chunk it came from, and
                    // a copy keeps the partial off the chunk's shared buffers.
                    (d, i, true) => part.strs.push(&d[i]),
                    _ => part.strs.push(""),
                }
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
                        && self.str_cols.iter().enumerate().all(|(slot, &c)| {
                            let (d, i, valid) = cell(c, k);
                            // A NULL's NULL bit already matched; its cell is "".
                            !valid
                                || crate::strings::views_eq(
                                    &part.strs,
                                    g as usize * nstr + slot,
                                    d,
                                    i,
                                )
                        })
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
        part.index_buckets(self.buckets);
        Ok(Some(part))
    }
}

/// One merge partition's groups: an accumulator row per group, and where the
/// group's first occurrence sits in the partials.
struct Merged {
    accs: Vec<Acc>,
    /// The group's first occurrence: the partial that folded it in the high
    /// word, its row there in the low one. Packed so the emit reads both in
    /// one access.
    at: Vec<u64>,
    /// Where each partial's new groups start in `accs`/`at`; one per partial
    /// plus a sentinel.
    starts: Vec<u32>,
}

/// Marks an input row whose group first appeared in an earlier partial, so it
/// has no row of its own in the merged tables.
const NO_GROUP: u64 = u64::MAX;

/// One key column's cells on the way out: the groups' words decoded into the
/// column's own payload and its validity word, filled in one pass over the
/// rows (a group's whole key row is one cache line, so reading it for every
/// column at once reads it once).
struct KeyBuild {
    nulls: Vec<u64>,
    data: KeyData,
}

enum KeyData {
    Bool(Vec<bool>),
    I32(Vec<i32>),
    Date(Vec<i32>),
    I64(Vec<i64>),
    Timestamp(Vec<i64>),
    Decimal {
        values: Vec<i128>,
        precision: u8,
        scale: u8,
    },
    /// String views are gathered after the pass: `picks` holds each group's
    /// partial and view index, `slot` the column's rank among the VARCHAR
    /// keys.
    Varchar {
        picks: Vec<(u32, u32)>,
        slot: usize,
    },
}

impl KeyBuild {
    fn new(kind: KeyKind, len: usize, slot: usize) -> KeyBuild {
        let capacity = len;
        let data = match kind {
            KeyKind::Bool => KeyData::Bool(Vec::with_capacity(capacity)),
            KeyKind::Int32 => KeyData::I32(Vec::with_capacity(capacity)),
            KeyKind::Date32 => KeyData::Date(Vec::with_capacity(capacity)),
            KeyKind::Int64 => KeyData::I64(Vec::with_capacity(capacity)),
            KeyKind::Timestamp => KeyData::Timestamp(Vec::with_capacity(capacity)),
            KeyKind::Decimal { precision, scale } => KeyData::Decimal {
                values: Vec::with_capacity(capacity),
                precision,
                scale,
            },
            KeyKind::Varchar => KeyData::Varchar {
                picks: Vec::with_capacity(capacity),
                slot,
            },
        };
        KeyBuild {
            nulls: vec![u64::MAX; len.div_ceil(64)],
            data,
        }
    }

    #[inline]
    fn push(&mut self, word: u64, source: u32, views: u32) {
        match &mut self.data {
            KeyData::Bool(d) => d.push(word != 0),
            KeyData::I32(d) | KeyData::Date(d) => d.push(word as i64 as i32),
            KeyData::I64(d) | KeyData::Timestamp(d) => d.push(word as i64),
            KeyData::Decimal { values, .. } => values.push(i128::from(word as i64)),
            KeyData::Varchar { picks, slot } => picks.push((source, views + *slot as u32)),
        }
    }

    #[inline]
    fn null(&mut self, r: usize) {
        self.nulls[r / 64] &= !(1u64 << (r % 64));
    }

    fn finish(self, sources: &[&StrVec]) -> (VectorData, Vec<u64>) {
        let data = match self.data {
            KeyData::Bool(d) => VectorData::Boolean(d),
            KeyData::I32(d) => VectorData::Int32(d),
            KeyData::Date(d) => VectorData::Date32(d),
            KeyData::I64(d) => VectorData::Int64(d),
            KeyData::Timestamp(d) => VectorData::TimestampMicros(d),
            KeyData::Decimal {
                values,
                precision,
                scale,
            } => VectorData::Decimal {
                values,
                precision,
                scale,
            },
            KeyData::Varchar { picks, .. } => {
                VectorData::VarChar(StrVec::gather(sources, &picks, u32::MAX))
            }
        };
        (data, self.nulls)
    }
}

/// Merge the partials (in chunk order, so the order accumulators merge in
/// matches the general path) and emit the groups as `[keys..., aggs...]`
/// chunks. With `ordered` they come out in first-seen order; otherwise each
/// merge partition's groups come out one partition after another, which is
/// all a set consumer needs.
fn merge_and_emit(
    partials: Vec<FlatPartial>,
    kinds: &[KeyKind],
    key_types: &[DataType],
    aggs: &[BoundAgg],
    ordered: bool,
) -> Result<Vec<DataChunk>> {
    let width = kinds.len() + 1;
    let naggs = aggs.len();
    let nstr = kinds.iter().filter(|k| **k == KeyKind::Varchar).count();
    let buckets = partials.first().map_or(1, |p| p.starts.len() - 1);
    let mut base: Vec<usize> = Vec::with_capacity(partials.len() + 1);
    let mut rows = 0;
    for p in &partials {
        base.push(rows);
        rows += p.len();
    }
    // The first occurrence of each group, per partial: its merged row (bucket
    // and row), or `NO_GROUP` for a row whose group was seen earlier. The
    // merge fills this in, so the emit needs neither a copy of the keys nor a
    // sort of the groups, and the partials' first-seen order is the output
    // order. Only the merge writes it (one slot per group, and the parallel
    // partitions own disjoint slots), and the emit reads it after that join,
    // so `Relaxed` is enough. Without `ordered` nothing reads it, so it stays
    // empty.
    let map: Vec<AtomicU64> = if ordered {
        (0..rows).map(|_| AtomicU64::new(NO_GROUP)).collect()
    } else {
        Vec::new()
    };
    let merged: Vec<Merged> = (0..buckets)
        .into_par_iter()
        .map(|b| {
            let size: usize = partials.iter().map(|p| p.starts[b + 1] - p.starts[b]).sum();
            let (mut slots, mask) = slot_table(size);
            let mut out = Merged {
                accs: Vec::with_capacity(size * naggs),
                at: Vec::with_capacity(size),
                starts: vec![0; partials.len() + 1],
            };
            let mut hashes: Vec<u64> = Vec::with_capacity(size);
            for (pi, p) in partials.iter().enumerate() {
                let from = base[pi];
                out.starts[pi] = out.at.len() as u32;
                for k in p.starts[b]..p.starts[b + 1] {
                    let g = p.ix[k] as usize;
                    let h = p.hashes[g];
                    let key = p.key(g);
                    let src = &p.accs[g * naggs..(g + 1) * naggs];
                    let mut s = h as usize & mask;
                    loop {
                        let t = slots[s];
                        if t == u32::MAX {
                            slots[s] = hashes.len() as u32;
                            let row = out.at.len() as u64;
                            if ordered {
                                map[from + g].store((b as u64) << 32 | row, Ordering::Relaxed);
                            }
                            out.accs.extend_from_slice(src);
                            out.at.push((pi as u64) << 32 | g as u64);
                            hashes.push(h);
                            break;
                        }
                        let t = t as usize;
                        let at = (out.at[t] & 0xffff_ffff) as usize;
                        let first = &partials[(out.at[t] >> 32) as usize];
                        if hashes[t] == h
                            && first.key(at) == key
                            && (0..nstr).all(|slot| {
                                crate::strings::views_eq(
                                    &first.strs,
                                    at * nstr + slot,
                                    &p.strs,
                                    g * nstr + slot,
                                )
                            })
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
            out.starts[partials.len()] = out.at.len() as u32;
            out
        })
        .collect();
    // Where each chunk of output rows starts: per partial when the order is
    // first-seen (a repeat's row was already emitted where it first appeared,
    // so a partial contributes only the groups it holds first), per merge
    // partition otherwise.
    let dbase: Vec<usize> = if ordered {
        let distinct: Vec<usize> = (0..partials.len())
            .into_par_iter()
            .map(|pi| {
                (0..buckets)
                    .map(|b| merged[b].starts[pi + 1] as usize - merged[b].starts[pi] as usize)
                    .sum()
            })
            .collect();
        let mut dbase: Vec<usize> = Vec::with_capacity(partials.len() + 1);
        let mut total = 0;
        for d in &distinct {
            dbase.push(total);
            total += d;
        }
        dbase.push(total);
        dbase
    } else {
        let mut dbase: Vec<usize> = Vec::with_capacity(buckets + 1);
        let mut total = 0;
        for m in &merged {
            dbase.push(total);
            total += m.at.len();
        }
        dbase.push(total);
        dbase
    };
    let total = dbase[dbase.len() - 1];
    let sources: Vec<&StrVec> = partials.iter().map(|p| &p.strs).collect();
    // Emit the groups a chunk holds, each given as its merge partition and its
    // row there. The key's row stays where the fold wrote it, in its first
    // partial.
    let emit = |pairs: &[(u32, u32)]| -> Result<DataChunk> {
        let len = pairs.len();
        let mut cols = Vec::with_capacity(kinds.len() + naggs);
        // A group's whole key row is one cache line, so one pass decodes every
        // key column at once instead of walking the row per column.
        let mut keys: Vec<KeyBuild> = Vec::with_capacity(kinds.len());
        let mut vslot = 0;
        for kind in kinds {
            keys.push(KeyBuild::new(*kind, len, vslot));
            if *kind == KeyKind::Varchar {
                vslot += 1;
            }
        }
        let mut agg_cols: Vec<Vector> = aggs
            .iter()
            .map(|a| Vector::with_capacity(a.dtype, len))
            .collect();
        for (out_row, &(b, t)) in pairs.iter().enumerate() {
            let m = &merged[b as usize];
            let t = t as usize;
            let at = (m.at[t] & 0xffff_ffff) as usize;
            let source = (m.at[t] >> 32) as u32;
            let p = &partials[source as usize];
            let key_row = p.key(at);
            let nulls = key_row[width - 1];
            let views = (at * nstr) as u32;
            for (c, kb) in keys.iter_mut().enumerate() {
                kb.push(key_row[c], source, views);
                if nulls >> c & 1 == 1 {
                    kb.null(out_row);
                }
            }
            let accs = &m.accs[t * naggs..(t + 1) * naggs];
            for (j, col) in agg_cols.iter_mut().enumerate() {
                col.push_value(accs[j].finalize_ref(aggs[j].dtype)?)?;
            }
        }
        for (c, kb) in keys.into_iter().enumerate() {
            let (data, validity) = kb.finish(&sources);
            let v = Vector::from_raw(data, validity);
            debug_assert_eq!(v.dtype(), key_types[c]);
            cols.push(v);
        }
        cols.extend(agg_cols);
        DataChunk::new(cols)
    };
    if total == 0 {
        return Ok(vec![emit(&[])?]);
    }
    let built: Vec<Result<DataChunk>> = (0..total.div_ceil(CHUNK_SIZE))
        .into_par_iter()
        .map(|k| {
            let lo = k * CHUNK_SIZE;
            let hi = (lo + CHUNK_SIZE).min(total);
            let mut pairs: Vec<(u32, u32)> = Vec::with_capacity(hi - lo);
            if ordered {
                let mut pi = dbase.partition_point(|&e| e <= lo) - 1;
                let mut pending = lo - dbase[pi];
                while pairs.len() < hi - lo {
                    let p = &partials[pi];
                    for g in 0..p.len() {
                        if pairs.len() == hi - lo {
                            break;
                        }
                        let e = map[base[pi] + g].load(Ordering::Relaxed);
                        if e == NO_GROUP {
                            continue;
                        }
                        if pending > 0 {
                            pending -= 1;
                            continue;
                        }
                        pairs.push(((e >> 32) as u32, (e & 0xffff_ffff) as u32));
                    }
                    pi += 1;
                }
            } else {
                let mut b = dbase.partition_point(|&e| e <= lo) - 1;
                let mut t = lo - dbase[b];
                while pairs.len() < hi - lo {
                    if t == dbase[b + 1] - dbase[b] {
                        b += 1;
                        t = 0;
                        continue;
                    }
                    pairs.push((b as u32, t as u32));
                    t += 1;
                }
            }
            emit(&pairs)
        })
        .collect();
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
    let Some(sink) = FlatSink::new(keys, aggs, rows, width) else {
        return Ok(None);
    };
    let partials: Option<Vec<FlatPartial>> = inputs
        .par_iter()
        .map(|chunk| sink.fold(cx, chunk, None))
        .collect::<Result<_>>()?;
    let Some(partials) = partials else {
        return Ok(None);
    };
    sink.merge(partials, !cx.unordered).map(Some)
}

/// The flat fold one chunk at a time, for the pipeline executor: it folds
/// morsels as they arrive and merges the partials once, in the order they
/// came. Every partial must come from the same sink (one [`Spec`]), which is
/// what makes the merge a single pass.
pub(crate) struct FlatSink<'a> {
    spec: Spec<'a>,
    key_types: Vec<DataType>,
}

impl<'a> FlatSink<'a> {
    /// `rows` is only the merge-bucket hint; it changes no result.
    pub(crate) fn new(
        keys: &'a [BoundExpr],
        aggs: &'a [BoundAgg],
        rows: usize,
        width: usize,
    ) -> Option<FlatSink<'a>> {
        let spec = Spec::new(keys, aggs, rows, width)?;
        Some(FlatSink {
            key_types: keys.iter().map(|k| k.dtype()).collect(),
            spec,
        })
    }

    pub(crate) fn fold(
        &self,
        cx: &Cx,
        chunk: &DataChunk,
        sel: Option<&[u32]>,
    ) -> Result<Option<FlatPartial>> {
        match self.spec.fold(cx, chunk, sel) {
            Ok(partial) => Ok(partial),
            // A row the selection dropped must not raise, the rule
            // `aggregate::fold_kept_rows` keeps: retry on the kept rows.
            Err(err) => match sel {
                None => Err(err),
                Some(sel) => match fold_kept_rows(cx, chunk, sel, &self.spec) {
                    Ok(partial) => Ok(partial),
                    Err(_) => Err(err),
                },
            },
        }
    }

    pub(crate) fn merge(self, partials: Vec<FlatPartial>, ordered: bool) -> Result<Vec<DataChunk>> {
        merge_and_emit(
            partials,
            &self.spec.kinds,
            &self.key_types,
            self.spec.aggs,
            ordered,
        )
    }
}

/// The out-of-line retry of [`FlatSink::fold`] (see
/// `aggregate::fold_kept_rows` for why it stays cold).
#[cold]
#[inline(never)]
fn fold_kept_rows(
    cx: &Cx,
    chunk: &DataChunk,
    sel: &[u32],
    spec: &Spec,
) -> Result<Option<FlatPartial>> {
    spec.fold(cx, &chunk.take(sel), None)
}
