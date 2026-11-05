//! Top-N building blocks: the order-preserving sort-key encoding, the
//! bounded [`TopNHeap`], `ORDER BY … LIMIT` selection, and the grouped Top-N
//! that answers `QUALIFY row_number() OVER (PARTITION BY … ORDER BY …) <= N`
//! without numbering every row.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use rayon::prelude::*;

use super::aggregate::{EvalVec as FusedVec, eval_agg_vec as eval_fused_vec};
use super::{Cx, chunks_approx_bytes, rebuild_sorted, sort};
use crate::plan::LogicalPlan;
use crate::sql::ast::{BinaryOp, SortSpec};
use crate::sql::binder::{BoundExpr, BoundWindow, WindowFunc};
use crate::types::{DataType, FastMap, Result, Value};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector, VectorData};

/// One sort's order-preserving key encoding: every input row's keys packed
/// into a single byte arena; `offsets[i]..offsets[i + 1]` is row `i`'s slice.
pub(super) struct SortArena {
    pub(super) bytes: Vec<u8>,
    pub(super) offsets: Vec<u32>,
}

impl SortArena {
    /// Row `row`'s encoded key bytes.
    #[inline]
    pub(super) fn key(&self, row: u32) -> &[u8] {
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
pub(super) fn eval_sort_keys<'a>(
    cx: &Cx,
    chunks: &'a [DataChunk],
    keys: &[(BoundExpr, SortSpec)],
) -> Result<Vec<Vec<FusedVec<'a>>>> {
    chunks
        .par_iter()
        .map(|chunk| {
            cx.check_deadline()?;
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
/// DESC keys):
///
/// - NULL → a single tag byte: `0x00` under NULLS FIRST (before every
///   non-null), `0xFF` under NULLS LAST (after every non-null) — the tag is
///   independent of the sort direction;
/// - otherwise tag `0x01` (NULLS FIRST) or `0x00` (NULLS LAST) + payload;
///   DESC inverts payload bytes only.
///
/// Payloads are order-preserving and, per key, prefix-free — a prefix
/// relation inside one key must never let the *next* key decide a comparison
/// the earlier key already settled:
/// - BOOLEAN: `0`/`1`;
/// - INTEGER/DATE, BIGINT/TIMESTAMP: big-endian two's complement with the
///   sign bit flipped (`x ^ MIN`), so unsigned byte order = signed order;
/// - DOUBLE: sign-magnitude → total-order bits (negatives inverted,
///   positives get the sign bit); −0.0 folds onto +0.0 and every NaN onto
///   all-ones (above `+inf`'s encoding), matching `num_cmp`'s DuckDB order;
/// - VARCHAR: each UTF-8 byte shifted up one (bytes are ≤ 0xF4, so `0x00`
///   is free) then a `0x00` terminator. The shift preserves order and the
///   terminator makes the encoding prefix-free ("ab" must beat "a" DESC).
///   DESC: inverted shifted bytes + inverted terminator `0xFF`.
pub(super) fn encode_key_into(v: &Vector, idx: usize, spec: SortSpec, out: &mut Vec<u8>) {
    let desc = spec.desc;
    if v.is_null(idx) {
        out.push(if spec.nulls_first { 0x00 } else { 0xFF });
        return;
    }
    out.push(if spec.nulls_first { 0x01 } else { 0x00 });
    match v.data() {
        VectorData::Boolean(d) => {
            let b = d[idx] as u8;
            out.push(if desc { !b } else { b });
        }
        VectorData::Int32(d) => push_be(out, ((d[idx] ^ i32::MIN) as u32) as u64, 4, desc),
        VectorData::Int64(d) => push_be(out, (d[idx] ^ i64::MIN) as u64, 8, desc),
        VectorData::Float64(d) => {
            let x = d[idx];
            let x = if x == 0.0 { 0.0 } else { x }; // −0.0 == +0.0 in num_cmp
            let b = x.to_bits();
            let b = if x.is_nan() {
                u64::MAX // NaN == NaN, above +inf (0xFFF0…) in num_cmp
            } else if b & (1u64 << 63) != 0 {
                !b
            } else {
                b | (1u64 << 63)
            };
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
        // DECIMAL: one scale per column, so plain i128 order works (sign bit
        // flipped like the integer encodings).
        VectorData::Decimal { values, .. } => push_be_i128(out, values[idx], desc),
        VectorData::Decimal64 { values, .. } => push_be_i128(out, i128::from(values[idx]), desc),
        // INTERVAL: encode the comparison key (months → 30 days, days →
        // micros), which is monotone in `interval_cmp`.
        VectorData::Interval(d) => {
            let iv = d[idx];
            let key = iv.months as i128 * 30 * 86_400_000_000i128
                + iv.days as i128 * 86_400_000_000i128
                + iv.micros as i128;
            push_be_i128(out, key, desc);
        }
        // UUID: unsigned 128-bit big-endian, the order `compare_values` uses.
        VectorData::Uuid(d) => {
            let be = d[idx].to_be_bytes();
            if desc {
                out.extend(be.iter().map(|b| !b));
            } else {
                out.extend_from_slice(&be);
            }
        }
        VectorData::Int128(d) => push_be_i128(out, d[idx], desc),
        VectorData::UInt128(d) => {
            let be = d[idx].to_be_bytes();
            if desc {
                out.extend(be.iter().map(|b| !b));
            } else {
                out.extend_from_slice(&be);
            }
        }
        // Unreachable: `encode_sort_keys` bails to the `Value` comparator for
        // VARIANT and ARRAY, whose byte order is not their sort order.
        VectorData::Variant(_) => unreachable!("VARIANT sort keys use the Value comparator"),
        VectorData::Array { .. } => unreachable!("ARRAY sort keys use the Value comparator"),
    }
}

/// Big-endian i128 with the sign bit flipped (ascending byte order); `desc`
/// inverts the bytes.
fn push_be_i128(out: &mut Vec<u8>, x: i128, desc: bool) {
    let be = (x ^ i128::MIN).to_be_bytes();
    if desc {
        out.extend(be.iter().map(|b| !b));
    } else {
        out.extend_from_slice(&be);
    }
}

/// Pack all rows' keys into one arena. `None` ⇒ the byte order cannot
/// reproduce `compare_values` exactly (dtypes differing across chunks, or
/// sizes past `u32`) and the caller must use the `Value` comparator path.
pub(super) fn encode_sort_keys(
    chunks: &[DataChunk],
    keyed: &[Vec<FusedVec>],
    keys: &[(BoundExpr, SortSpec)],
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
    // VARIANT sorts by the cross-kind total order and one chunk may mix kinds,
    // so no byte encoding reproduces `compare_values`; the same holds for
    // ARRAY (element-wise, NULL-element-last). The caller's `Value`
    // comparator (first-seen ties) takes over.
    if dtypes
        .iter()
        .any(|d| matches!(d, DataType::Variant | DataType::Array { .. }))
    {
        return None;
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
                for (ki, (_, spec)) in keys.iter().enumerate() {
                    encode_key_into(ks[ki].v(), i, *spec, &mut bytes);
                }
            }
            Some((bytes, offsets))
        })
        .collect();
    let mut arena = SortArena {
        bytes: Vec::new(),
        offsets: Vec::with_capacity(n + 1),
    };
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

/// The first `limit` rows of `chunks` under `keys`, in order, ties in input
/// order: the top-N heaps when the limit is small against the rows, a full
/// sort otherwise. What the `LIMIT` arm returns for the same chunks.
pub(super) fn select_top(
    cx: &Cx,
    chunks: &[DataChunk],
    keys: &[(BoundExpr, SortSpec)],
    limit: usize,
) -> Result<Vec<DataChunk>> {
    let n: usize = chunks.iter().map(DataChunk::num_rows).sum();
    let sorted = if n > limit && limit.saturating_mul(16) <= n {
        match top_n(cx, chunks, keys, limit)? {
            Some(chunks) => chunks,
            None => sort(cx, chunks, keys)?,
        }
    } else {
        sort(cx, chunks, keys)?
    };
    let mut out = Vec::new();
    let mut left = limit;
    for chunk in sorted {
        if left == 0 {
            break;
        }
        if chunk.num_rows() <= left {
            left -= chunk.num_rows();
            out.push(chunk);
        } else {
            let sel: Vec<usize> = (0..left).collect();
            out.push(chunk.gather(&sel));
            left = 0;
        }
    }
    Ok(out)
}

/// Top-N selection for `LIMIT` directly over `SORT`: every rayon partition
/// keeps a bounded max-heap of its `limit` smallest rows under the byte
/// comparator; the ≤ partitions × limit candidates merge to the global first
/// `limit`. `Ok(None)` ⇒ encoding unavailable (see [`encode_sort_keys`]);
/// the caller then falls back to a full sort.
pub(super) fn top_n(
    cx: &Cx,
    chunks: &[DataChunk],
    keys: &[(BoundExpr, SortSpec)],
    limit: usize,
) -> Result<Option<Vec<DataChunk>>> {
    let keyed = eval_sort_keys(cx, chunks, keys)?;
    let Some(arena) = encode_sort_keys(chunks, &keyed, keys) else {
        return Ok(None);
    };
    let n = arena.offsets.len() - 1;
    debug_assert!(0 < limit && limit < n, "top-N needs a truncating limit");
    // Wave 136a: the row indices and the candidate keys the heap partitions
    // hold, charged per statement; the output is at most `limit` rows.
    let mut held = cx
        .budget
        .memory()
        .reserve(n * 12 + chunks_approx_bytes(chunks))?;
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
                let cand = Candidate {
                    key: arena.key(row),
                    row,
                };
                if heap.len() < limit {
                    heap.push(cand);
                } else if let Some(&max) = heap.peek()
                    && cand < max
                {
                    heap.pop();
                    heap.push(cand);
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
    if let Some(last) = out.last_mut()
        && last.num_rows() < CHUNK_SIZE
    {
        let sel: Vec<usize> = (0..last.num_rows()).collect();
        *last = last.gather(&sel);
    }
    held.resize(chunks_approx_bytes(&out))?;
    Ok(Some(out))
}

// ---------- the bounded heap ----------

/// One entry of a [`TopNHeap`]: ordered by its encoded key bytes, then by its
/// input position, so two entries never compare equal and the kept set is
/// exactly determined whatever order the offers arrive in.
struct HeapEntry<P> {
    key: Box<[u8]>,
    pos: u64,
    payload: P,
}

impl<P> HeapEntry<P> {
    fn rank(&self, key: &[u8], pos: u64) -> Ordering {
        (&*self.key, self.pos).cmp(&(key, pos))
    }
}

impl<P> PartialEq for HeapEntry<P> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<P> Eq for HeapEntry<P> {}

impl<P> PartialOrd for HeapEntry<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P> Ord for HeapEntry<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank(&other.key, other.pos)
    }
}

/// What [`TopNHeap::offer`] did with an entry.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Offer<P> {
    /// Kept; the heap was not full yet.
    Kept,
    /// Kept in place of the heap's worst entry, whose payload this is.
    Evicted(P),
    /// Not kept: the heap is full of entries that all rank before it.
    Rejected(P),
}

/// The `limit` smallest entries offered to it by (key bytes, input
/// position): a bounded max-heap whose top is the entry the next better
/// offer evicts. Keys are any byte strings whose byte order is the wanted
/// order ([`encode_key_into`] builds them for sort keys); positions break
/// ties, so equal keys keep input order. The payload is the caller's: a row
/// reference, an aggregate argument, anything.
pub(crate) struct TopNHeap<P> {
    limit: usize,
    heap: BinaryHeap<HeapEntry<P>>,
}

impl<P> TopNHeap<P> {
    /// An empty heap keeping at most `limit` (at least 1) entries.
    pub(crate) fn new(limit: usize) -> Self {
        debug_assert!(limit > 0, "a Top-N heap keeps at least one entry");
        TopNHeap {
            limit: limit.max(1),
            heap: BinaryHeap::new(),
        }
    }

    /// Whether [`offer`](Self::offer) would keep an entry with this key and
    /// position, without building it.
    #[inline]
    pub(crate) fn would_keep(&self, key: &[u8], pos: u64) -> bool {
        self.heap.len() < self.limit
            || self
                .heap
                .peek()
                .is_some_and(|worst| worst.rank(key, pos) == Ordering::Greater)
    }

    /// Offer one entry; see [`Offer`] for what comes back.
    pub(crate) fn offer(&mut self, key: &[u8], pos: u64, payload: P) -> Offer<P> {
        if !self.would_keep(key, pos) {
            return Offer::Rejected(payload);
        }
        let entry = HeapEntry {
            key: key.into(),
            pos,
            payload,
        };
        if self.heap.len() < self.limit {
            self.heap.push(entry);
            return Offer::Kept;
        }
        let mut worst = self.heap.peek_mut().expect("a full heap has a top");
        let old = std::mem::replace(&mut *worst, entry);
        drop(worst); // sifts the new entry down
        Offer::Evicted(old.payload)
    }

    /// Fold `other` into this heap; `dropped` receives the payload of every
    /// entry of either heap that no longer makes the cut.
    pub(crate) fn merge(&mut self, other: TopNHeap<P>, mut dropped: impl FnMut(P)) {
        for entry in other.heap.into_vec() {
            if !self.would_keep(&entry.key, entry.pos) {
                dropped(entry.payload);
            } else if self.heap.len() < self.limit {
                self.heap.push(entry);
            } else {
                let mut worst = self.heap.peek_mut().expect("a full heap has a top");
                let old = std::mem::replace(&mut *worst, entry);
                drop(worst);
                dropped(old.payload);
            }
        }
    }

    /// The same entries with every payload mapped.
    pub(crate) fn map<Q>(self, mut f: impl FnMut(P) -> Q) -> TopNHeap<Q> {
        let entries: Vec<HeapEntry<Q>> = self
            .heap
            .into_vec()
            .into_iter()
            .map(|e| HeapEntry {
                key: e.key,
                pos: e.pos,
                payload: f(e.payload),
            })
            .collect();
        TopNHeap {
            limit: self.limit,
            // The keys and positions are unchanged, so the vector is still a
            // valid heap; `from` re-checks it in O(n).
            heap: BinaryHeap::from(entries),
        }
    }

    /// Every kept payload, in no particular order.
    pub(crate) fn payloads(&self) -> impl Iterator<Item = &P> {
        self.heap.iter().map(|e| &e.payload)
    }

    /// The kept entries as (key, position, payload), best first.
    pub(crate) fn into_sorted(self) -> Vec<(Box<[u8]>, u64, P)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| (e.key, e.pos, e.payload))
            .collect()
    }
}

// ---------- grouped Top-N: QUALIFY row_number() OVER (…) <= N ----------

/// `Filter(row_number <= N) → Window([row_number])`, the shape the grouped
/// Top-N answers: the N first rows of every partition, numbered, without
/// ordering (or even keeping) the rest.
pub(crate) struct GroupedShape<'a> {
    pub(crate) window: &'a BoundWindow,
    pub(crate) windows: &'a [BoundWindow],
    /// The window's input.
    pub(crate) input: &'a LogicalPlan,
    /// No row numbered past this passes the filter.
    pub(crate) limit: usize,
}

/// The grouped Top-N shape of a `Filter` over a `Window`, if it is one: one
/// `row_number()` window (no `FILTER`), byte-encodable order keys, and a
/// top-level conjunct `rn <= k`, `rn < k` or `rn = 1` (either side). The
/// filter's predicate is applied in full afterwards, so other conjuncts stay
/// correct.
pub(crate) fn grouped_shape(plan: &LogicalPlan) -> Option<GroupedShape<'_>> {
    let LogicalPlan::Filter { input, predicate } = plan else {
        return None;
    };
    let LogicalPlan::Window {
        input,
        windows,
        input_cols,
    } = &**input
    else {
        return None;
    };
    let [window] = windows.as_slice() else {
        return None;
    };
    if window.func != WindowFunc::RowNumber
        || window.filter.is_some()
        || window.dtype != DataType::Bigint
        || window
            .order
            .iter()
            .any(|(e, _)| matches!(e.dtype(), DataType::Variant | DataType::Array { .. }))
    {
        return None;
    }
    let mut parts = Vec::new();
    super::conjuncts(predicate, &mut parts);
    let limit = parts
        .iter()
        .filter_map(|c| row_number_bound(c, *input_cols))
        .min()?;
    Some(GroupedShape {
        window,
        windows,
        input,
        limit,
    })
}

/// The largest row number `expr` lets through, when it bounds column `col`
/// from above by a constant of at least 1.
fn row_number_bound(expr: &BoundExpr, col: usize) -> Option<usize> {
    let BoundExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let int = |v: &Value| match v {
        Value::Integer(x) => Some(i64::from(*x)),
        Value::Bigint(x) => Some(*x),
        _ => None,
    };
    // QUALIFY binds the number as a `WindowRef`; a Filter written over the
    // Window by hand sees it as a plain column.
    let is_number = |e: &BoundExpr| match e {
        BoundExpr::Column { index, .. } | BoundExpr::WindowRef { index, .. } => *index == col,
        _ => false,
    };
    let (op, k) = match (&**left, &**right) {
        (l, BoundExpr::Literal(v)) if is_number(l) => (*op, int(v)?),
        (BoundExpr::Literal(v), r) if is_number(r) => (
            match op {
                BinaryOp::GtEq => BinaryOp::LtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::Eq => BinaryOp::Eq,
                _ => return None,
            },
            int(v)?,
        ),
        _ => return None,
    };
    let bound = match op {
        BinaryOp::LtEq => k,
        BinaryOp::Lt => k - 1,
        BinaryOp::Eq if k == 1 => 1,
        _ => return None,
    };
    usize::try_from(bound).ok().filter(|&b| b >= 1)
}

/// Partitions spread over this many hash shards per worker, so the final
/// merge runs one shard per task instead of one map on one thread.
const SHARDS: usize = 64;

/// A worker compacts its kept rows once the chunks it holds them in carry
/// twice as many rows, or this many, whichever is more.
const GROUPED_COMPACT_ROWS: usize = 64 * 1024;

/// Partition key → that partition's heap of kept rows.
type Partitions<P> = FastMap<Box<[u8]>, TopNHeap<P>>;

/// One worker's partitions: a [`TopNHeap`] per partition key whose payload
/// is a row of the chunks the worker kept (`rows`, numbered across them).
pub(crate) struct GroupedWorker<'a> {
    window: &'a BoundWindow,
    limit: usize,
    shards: Vec<Partitions<u32>>,
    rows: Vec<DataChunk>,
    /// Rows in `rows`, live or not.
    held: usize,
    /// Rows some heap still names.
    live: usize,
    part_buf: Vec<u8>,
    key_buf: Vec<u8>,
}

impl<'a> GroupedWorker<'a> {
    pub(crate) fn new(shape: &GroupedShape<'a>) -> Self {
        GroupedWorker {
            window: shape.window,
            limit: shape.limit,
            shards: (0..SHARDS).map(|_| FastMap::default()).collect(),
            rows: Vec::new(),
            held: 0,
            live: 0,
            part_buf: Vec::new(),
            key_buf: Vec::new(),
        }
    }

    /// Offer every row of `chunk`; `pos(i)` is row `i`'s input position.
    /// `Ok(false)` ⇒ an order key came back typed unlike its expression, so
    /// byte order may not be the window's order: the caller takes the window
    /// path instead.
    pub(crate) fn push(
        &mut self,
        cx: &Cx,
        chunk: DataChunk,
        pos: impl Fn(usize) -> u64,
    ) -> Result<bool> {
        let n = chunk.num_rows();
        if n == 0 {
            return Ok(true);
        }
        if self.held + n > u32::MAX as usize {
            return Ok(false);
        }
        let w = self.window;
        let part: Vec<FusedVec> = w
            .partition
            .iter()
            .map(|e| eval_fused_vec(e, &chunk, cx))
            .collect::<Result<_>>()?;
        let ord: Vec<FusedVec> = w
            .order
            .iter()
            .map(|(e, _)| eval_fused_vec(e, &chunk, cx))
            .collect::<Result<_>>()?;
        if w.order
            .iter()
            .zip(&ord)
            .any(|((e, _), v)| v.v().dtype() != e.dtype())
        {
            return Ok(false);
        }
        let base = self.held as u32;
        let mut kept_any = false;
        for i in 0..n {
            self.part_buf.clear();
            for v in &part {
                v.v().encode_into(&mut self.part_buf, i);
            }
            self.key_buf.clear();
            for (v, (_, spec)) in ord.iter().zip(&w.order) {
                encode_key_into(v.v(), i, *spec, &mut self.key_buf);
            }
            let p = pos(i);
            let row = base + i as u32;
            let shard = &mut self.shards[shard_of(&self.part_buf)];
            match shard.get_mut(self.part_buf.as_slice()) {
                Some(heap) => match heap.offer(&self.key_buf, p, row) {
                    Offer::Kept => {
                        self.live += 1;
                        kept_any = true;
                    }
                    Offer::Evicted(_) => kept_any = true,
                    Offer::Rejected(_) => {}
                },
                None => {
                    let mut heap = TopNHeap::new(self.limit);
                    heap.offer(&self.key_buf, p, row);
                    shard.insert(self.part_buf.as_slice().into(), heap);
                    self.live += 1;
                    kept_any = true;
                }
            }
        }
        drop((part, ord));
        if kept_any {
            self.rows.push(chunk);
            self.held += n;
            if self.held > self.live.saturating_mul(2).max(GROUPED_COMPACT_ROWS) {
                self.compact()?;
            }
        }
        Ok(true)
    }

    /// Gather the live rows into fresh chunks and renumber the heaps.
    fn compact(&mut self) -> Result<()> {
        let mut live: Vec<u32> = self
            .shards
            .iter()
            .flat_map(|s| s.values())
            .flat_map(|h| h.payloads().copied())
            .collect();
        live.sort_unstable();
        self.rows = if live.is_empty() {
            Vec::new()
        } else {
            rebuild_sorted(&self.rows, &live)?
        };
        for shard in &mut self.shards {
            for heap in shard.values_mut() {
                let old = std::mem::replace(heap, TopNHeap::new(self.limit));
                *heap = old.map(|p| live.binary_search(&p).expect("a live row") as u32);
            }
        }
        self.held = live.len();
        self.live = live.len();
        Ok(())
    }
}

fn shard_of(key: &[u8]) -> usize {
    use std::hash::BuildHasher;
    // The finalized FNV hash mixes every bit; the middle bits stay clear of
    // the low bits the map buckets by and the top bits its control bytes use.
    (crate::types::FnvBuild.hash_one(key) >> 32) as usize % SHARDS
}

/// Offer `chunks` (a materialized input) to workers over contiguous runs of
/// chunks; positions are global row indices. `None` ⇒ a worker declined.
pub(crate) fn grouped_chunks<'a>(
    cx: &Cx,
    shape: &GroupedShape<'a>,
    chunks: &[DataChunk],
) -> Result<Option<Vec<GroupedWorker<'a>>>> {
    let starts = super::chunk_starts(chunks);
    let runs = rayon::current_num_threads().max(1).min(chunks.len().max(1));
    let per = chunks.len().div_ceil(runs).max(1);
    let workers: Vec<Option<GroupedWorker<'a>>> = (0..runs)
        .into_par_iter()
        .map(|r| {
            let mut worker = GroupedWorker::new(shape);
            for ci in (r * per)..((r + 1) * per).min(chunks.len()) {
                cx.check_deadline()?;
                let start = starts[ci] as u64;
                if !worker.push(cx, chunks[ci].clone(), |i| start + i as u64)? {
                    return Ok(None);
                }
            }
            Ok(Some(worker))
        })
        .collect::<Result<_>>()?;
    Ok(workers.into_iter().collect())
}

/// Merge the workers' partitions, number each partition's kept rows 1..=N in
/// (key, position) order, and return them in input order with the row number
/// appended: the rows and order the window path then the filter's
/// `rn <= N` conjunct would produce.
/// Grouped Top-N runs so far, so tests can tell the rewrite fired (equal
/// results alone cannot: the window path returns the same rows).
#[cfg(test)]
pub(crate) static GROUPED_RUNS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn grouped_finish(
    cx: &Cx,
    types: &[DataType],
    workers: Vec<GroupedWorker<'_>>,
) -> Result<Vec<DataChunk>> {
    #[cfg(test)]
    GROUPED_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let held: usize = workers.iter().map(|w| chunks_approx_bytes(&w.rows)).sum();
    let _held = cx.budget.memory().reserve(held)?;
    let mut by_shard: Vec<Vec<(u32, Partitions<u32>)>> = (0..SHARDS).map(|_| Vec::new()).collect();
    let mut rows: Vec<Vec<DataChunk>> = Vec::with_capacity(workers.len());
    for (wi, worker) in workers.into_iter().enumerate() {
        for (s, map) in worker.shards.into_iter().enumerate() {
            if !map.is_empty() {
                by_shard[s].push((wi as u32, map));
            }
        }
        rows.push(worker.rows);
    }
    let picked: Vec<Vec<(u64, u32, u32, i64)>> = by_shard
        .into_par_iter()
        .map(|maps| {
            let mut merged: Partitions<(u32, u32)> = FastMap::default();
            for (wi, map) in maps {
                for (key, heap) in map {
                    let heap = heap.map(|r| (wi, r));
                    match merged.get_mut(&key) {
                        Some(into) => into.merge(heap, |_| {}),
                        None => {
                            merged.insert(key, heap);
                        }
                    }
                }
            }
            let mut out = Vec::new();
            for heap in merged.into_values() {
                for (rn, (_, pos, (wi, r))) in heap.into_sorted().into_iter().enumerate() {
                    out.push((pos, wi, r, rn as i64 + 1));
                }
            }
            out
        })
        .collect();
    let mut picked: Vec<(u64, u32, u32, i64)> = picked.into_iter().flatten().collect();
    picked.par_sort_unstable_by_key(|p| p.0);
    let starts: Vec<Vec<usize>> = rows.iter().map(|r| super::chunk_starts(r)).collect();
    let gather = |part: &[(u64, u32, u32, i64)]| -> Result<DataChunk> {
        let mut cols: Vec<Vector> = types
            .iter()
            .map(|t| Vector::with_capacity(*t, part.len()))
            .collect();
        for &(_, wi, r, _) in part {
            let (st, kept) = (&starts[wi as usize], &rows[wi as usize]);
            let r = r as usize;
            let ci = st.partition_point(|&s| s <= r) - 1;
            let src = &kept[ci];
            for (dst, col) in cols.iter_mut().zip(src.columns.iter()) {
                // Absent placeholder columns (a projected lake scan) stay
                // absent, as `rebuild_sorted` leaves them.
                if col.len() == src.num_rows() {
                    dst.push_from(col, r - st[ci])?;
                }
            }
        }
        let mut columns: Vec<Arc<Vector>> = cols.into_iter().map(Arc::new).collect();
        columns.push(Arc::new(Vector::from_raw(
            VectorData::Int64(part.iter().map(|p| p.3).collect()),
            vec![u64::MAX; part.len().div_ceil(64)],
        )));
        Ok(DataChunk::projected(columns, part.len()))
    };
    picked.par_chunks(CHUNK_SIZE).map(gather).collect()
}

#[cfg(test)]
mod tests {
    use super::{Offer, TopNHeap};

    fn sorted(heap: TopNHeap<&'static str>) -> Vec<(Vec<u8>, u64, &'static str)> {
        heap.into_sorted()
            .into_iter()
            .map(|(k, p, v)| (k.into_vec(), p, v))
            .collect()
    }

    #[test]
    fn keeps_the_smallest_keys_and_breaks_ties_by_position() {
        let mut heap = TopNHeap::new(3);
        assert_eq!(heap.offer(b"m", 0, "a"), Offer::Kept);
        assert_eq!(heap.offer(b"c", 1, "b"), Offer::Kept);
        assert_eq!(heap.offer(b"m", 2, "c"), Offer::Kept);
        // Full: an equal key from later in the input ranks after both "m"s.
        assert!(!heap.would_keep(b"m", 3));
        assert_eq!(heap.offer(b"m", 3, "d"), Offer::Rejected("d"));
        // An equal key from earlier in the input evicts the later "m".
        assert_eq!(heap.offer(b"m", 1, "e"), Offer::Evicted("c"));
        // (m, 1) now ranks after (m, 0), so it is the one to go.
        assert_eq!(heap.offer(b"a", 9, "f"), Offer::Evicted("e"));
        assert_eq!(
            sorted(heap),
            vec![
                (b"a".to_vec(), 9, "f"),
                (b"c".to_vec(), 1, "b"),
                (b"m".to_vec(), 0, "a"),
            ]
        );
    }

    #[test]
    fn merging_keeps_the_same_set_as_one_heap_whatever_the_split() {
        let entries: Vec<(Vec<u8>, u64)> = (0..200u64)
            .map(|i| (vec![((i * 37) % 11) as u8, ((i * 13) % 3) as u8], i))
            .collect();
        let mut whole = TopNHeap::new(7);
        for (k, p) in &entries {
            whole.offer(k, *p, *p);
        }
        let want: Vec<u64> = whole.into_sorted().into_iter().map(|e| e.2).collect();
        for parts in [2usize, 3, 7, 50] {
            let mut heaps: Vec<TopNHeap<u64>> = (0..parts).map(|_| TopNHeap::new(7)).collect();
            for (i, (k, p)) in entries.iter().enumerate() {
                heaps[(i * 31) % parts].offer(k, *p, *p);
            }
            let mut merged = heaps.pop().unwrap();
            let mut dropped = 0;
            for heap in heaps {
                merged.merge(heap, |_| dropped += 1);
            }
            let got: Vec<u64> = merged.into_sorted().into_iter().map(|e| e.2).collect();
            assert_eq!(got, want, "{parts} parts");
        }
    }

    #[test]
    fn mapping_payloads_keeps_the_order() {
        let mut heap = TopNHeap::new(2);
        heap.offer(b"b", 0, 10u32);
        heap.offer(b"a", 1, 20u32);
        let mut heap = heap.map(|p| p + 1);
        assert_eq!(heap.offer(b"c", 2, 0), Offer::Rejected(0));
        let mut seen: Vec<u32> = heap.payloads().copied().collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![11, 21]);
        let order: Vec<u32> = heap.into_sorted().into_iter().map(|e| e.2).collect();
        assert_eq!(order, vec![21, 11]);
    }
}
