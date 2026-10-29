//! Column statistics, zone maps and the join filter's Bloom sketch (wave 122).
//!
//! The shape follows DuckDB's `src/storage/statistics/` (MIT): exact NULL
//! counts and min/max, an approximate distinct count from a HyperLogLog, and
//! conservative pruning — a skipped chunk or rejected row must be one the
//! filter could not have kept. Nothing here may change a query result.
//!
//! Per-chunk stats ([`Zone`]) are the zone maps: 2048-row segments, exact when
//! written and only widened by mutations, so a pruning decision is always an
//! upper bound on what the chunk holds. The table-level [`TableStats`] are
//! maintained from them in O(chunk) per write (see `DataTable`), never by a
//! rescan.
//!
//! Semantics kept from the `.pink` writer (the lake publishes the same facts):
//! NaN never takes part in min/max — a DOUBLE chunk with any NaN has no
//! prunable bounds for comparisons (the caller checks [`Zone::nan_count`]) —
//! and VARIANT/ARRAY have no bounds because their total order spans kinds.

use crate::attached::PruneOp;
use crate::plan::compare_values;
use crate::types::{DataType, Interval, Value};
use crate::vector::{Vector, VectorData};
use std::sync::Arc;

// ---------- hashing ----------

/// The final avalanche every hash in this module ends with: the HLL reads the
/// top bits and the Bloom the top 32, so a weak low-entropy input (row ids,
/// dense keys) must not survive.
fn mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// FNV-1a over `bytes`, then [`mix`].
pub(crate) fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(h)
}

/// The hash of one non-NULL cell of a key column: equal keys hash equally and
/// NULL is not a key (callers check `is_valid` first).
///
/// Both the hash that builds a bloom and the hash that probes it go through
/// this function, so the two can never disagree; the typed branches exist so a
/// probe scan does not materialize a `Value` per row.
pub(crate) fn key_hash(v: &Vector, row: usize) -> u64 {
    match v.data() {
        VectorData::Boolean(d) => mix(d[row] as u64 ^ 0x10),
        VectorData::Int32(d) => mix(d[row] as u64),
        VectorData::Int64(d) => mix(d[row] as u64 ^ 0x20),
        VectorData::Date32(d) => mix(d[row] as u64 ^ 0x30),
        VectorData::TimestampMicros(d) => mix(d[row] as u64 ^ 0x40),
        VectorData::Float64(d) => mix(double_bits(d[row])),
        VectorData::VarChar(d) => hash_bytes(d[row].as_bytes()),
        VectorData::Decimal { values, scale, .. } => {
            let x = values[row];
            mix(x as u64 ^ ((x >> 64) as u64).rotate_left(17) ^ ((*scale as u64) << 56))
        }
        VectorData::Interval(d) => {
            let iv = d[row];
            mix(crate::types::interval_key(iv.months, iv.days, iv.micros) as u64 ^ 0x50)
        }
        // The general encodings (they own the canonical form of these kinds).
        VectorData::Variant(_) | VectorData::Array { .. } => value_hash(&v.get(row)),
    }
}

/// The hash of one `Value`, agreeing with [`key_hash`] cell for cell.
pub(crate) fn value_hash(v: &Value) -> u64 {
    match v {
        // NULL is not a key; the HLL never sees it, and a bloom's caller
        // rejects the row first.
        Value::Null => 0,
        Value::Boolean(b) => mix(*b as u64 ^ 0x10),
        Value::Integer(i) => mix(*i as u64),
        Value::Bigint(i) => mix(*i as u64 ^ 0x20),
        Value::Date(d) => mix(*d as u64 ^ 0x30),
        Value::Timestamp(us) => mix(*us as u64 ^ 0x40),
        Value::Double(x) => mix(double_bits(*x)),
        Value::Varchar(s) => hash_bytes(s.as_bytes()),
        Value::Decimal(x, scale) => {
            mix(*x as u64 ^ ((*x >> 64) as u64).rotate_left(17) ^ ((*scale as u64) << 56))
        }
        Value::Interval {
            months,
            days,
            micros,
        } => mix(crate::types::interval_key(*months, *days, *micros) as u64 ^ 0x50),
        Value::Variant(_) | Value::Array(_) => {
            let mut buf = Vec::new();
            crate::exec::aggregate::encode_value(&mut buf, v);
            hash_bytes(&buf)
        }
    }
}

/// `-0.0` and every NaN collapse to one bit pattern, exactly as equality (and
/// the aggregate key encoding) treats them.
fn double_bits(x: f64) -> u64 {
    let norm = if x.is_nan() {
        f64::NAN
    } else if x == 0.0 {
        0.0
    } else {
        x
    };
    norm.to_bits()
}

// ---------- HyperLogLog ----------

/// `2^12 = 4096` one-byte registers: 1.63 % standard error, which is well
/// under the error the planner's selectivity estimate can use.
pub(crate) const HLL_PRECISION: u32 = 12;
pub(crate) const HLL_REGISTERS: usize = 1 << HLL_PRECISION;

/// The largest [`Hll::to_bytes`] output: every block occupied.
pub const HLL_MAX_BYTES: usize = 1 + (HLL_REGISTERS / HLL_BLOCK) * (1 + HLL_BLOCK);

/// Registers per serialized block (a sparse sketch stores whole blocks).
const HLL_BLOCK: usize = 32;

/// A fixed-precision HyperLogLog distinct-count sketch (no crate).
///
/// Registers only ever grow, so it cannot represent a remov**al**: a
/// DELETE/UPDATE leaves the estimate an upper bound and the column's
/// `dirty` flag records that (`ANALYZE` rebuilds it exactly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hll {
    regs: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Self::new()
    }
}

impl Hll {
    pub fn new() -> Self {
        Self {
            regs: vec![0; HLL_REGISTERS],
        }
    }

    /// Fold one already-hashed value in. `h`'s top bits choose the register
    /// and the rank of the first set bit in the rest is the observation.
    pub fn insert_hash(&mut self, h: u64) {
        let idx = (h >> (64 - HLL_PRECISION)) as usize;
        let rank = ((h << HLL_PRECISION).leading_zeros() + 1).min(64 - HLL_PRECISION + 1) as u8;
        let slot = &mut self.regs[idx];
        if rank > *slot {
            *slot = rank;
        }
    }

    /// Fold one cell of `vector` in; NULL cells are not values.
    pub(crate) fn insert_cell(&mut self, vector: &Vector, row: usize) {
        if vector.is_valid(row) {
            self.insert_hash(key_hash(vector, row));
        }
    }

    /// Fold one scalar in (the row-at-a-time write path), hashing exactly as
    /// [`Hll::insert_cell`] does.
    pub(crate) fn insert_value(&mut self, value: &Value) {
        if !value.is_null() {
            self.insert_hash(value_hash(value));
        }
    }

    pub fn merge(&mut self, other: &Hll) {
        for (a, b) in self.regs.iter_mut().zip(&other.regs) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// The cardinality estimate: linear counting while the sketch is sparse
    /// (the accurate regime for small counts), the raw estimate above it.
    pub fn estimate(&self) -> u64 {
        let m = HLL_REGISTERS as f64;
        let sum: f64 = self.regs.iter().map(|&r| f64::powi(2.0, -(r as i32))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        let zeros = self.regs.iter().filter(|&&r| r == 0).count();
        let est = if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        };
        est.round().max(0.0) as u64
    }

    /// Serialize sparsely: the index of every non-zero 32-register block,
    /// then those blocks' bytes. Registers of a small domain are mostly
    /// zero, so a tiny table's sketch stays a few hundred bytes instead of
    /// [`HLL_REGISTERS`]; a saturated one pays one index byte per block.
    /// (`HLLStorage` in DuckDB's `src/storage/statistics/distinct_statistics.cpp`
    /// keeps the same "only non-zero blocks" shape.)
    pub fn to_bytes(&self) -> Vec<u8> {
        let blocks: Vec<u8> = self
            .regs
            .chunks(HLL_BLOCK)
            .enumerate()
            .filter(|(_, block)| block.iter().any(|&r| r != 0))
            .map(|(i, _)| i as u8)
            .collect();
        let mut out = Vec::with_capacity(1 + blocks.len() * (1 + HLL_BLOCK));
        out.push(blocks.len() as u8);
        out.extend_from_slice(&blocks);
        for &b in &blocks {
            let start = b as usize * HLL_BLOCK;
            out.extend_from_slice(&self.regs[start..start + HLL_BLOCK]);
        }
        out
    }

    /// Rebuild from [`Hll::to_bytes`]; `None` when the encoding is not one
    /// this build writes (a file from another build is refused, never
    /// guessed at).
    pub fn from_bytes(bytes: &[u8]) -> Option<Hll> {
        let used = *bytes.first()? as usize;
        if bytes.len() != 1 + used * (1 + HLL_BLOCK) {
            return None;
        }
        let mut regs = vec![0u8; HLL_REGISTERS];
        let mut prev: Option<u8> = None;
        for i in 0..used {
            let block = bytes[1 + i];
            if block as usize >= HLL_REGISTERS / HLL_BLOCK || prev.is_some_and(|p| block <= p) {
                return None;
            }
            prev = Some(block);
            let src = &bytes[1 + used + i * HLL_BLOCK..];
            let dst = block as usize * HLL_BLOCK;
            regs[dst..dst + HLL_BLOCK].copy_from_slice(&src[..HLL_BLOCK]);
        }
        Some(Hll { regs })
    }
}

// ---------- per-chunk zone maps ----------

/// One column chunk's exact bounds: the zone map unit.
///
/// `min`/`max` cover the non-NULL, non-NaN values; `nan_count` records the
/// NaNs so a comparison can refuse to prune (NaN sorts above every double, so
/// a bound that ignored it is only a lower bound on the real maximum).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Zone {
    pub null_count: u64,
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub nan_count: u64,
}

impl Zone {
    pub(crate) fn empty() -> Self {
        Self {
            null_count: 0,
            min: None,
            max: None,
            nan_count: 0,
        }
    }

    /// Fold one value in (the incremental row-at-a-time path).
    pub(crate) fn observe(&mut self, v: &Value) {
        match v {
            Value::Null => self.null_count += 1,
            Value::Double(x) if x.is_nan() => self.nan_count += 1,
            v => {
                if self
                    .min
                    .as_ref()
                    .is_none_or(|m| compare_values(v, m).is_lt())
                {
                    self.min = Some(v.clone());
                }
                if self
                    .max
                    .as_ref()
                    .is_none_or(|m| compare_values(v, m).is_gt())
                {
                    self.max = Some(v.clone());
                }
            }
        }
    }

    /// Union with another zone (chunk → table merge).
    pub(crate) fn merge(&mut self, other: &Zone) {
        self.null_count += other.null_count;
        self.nan_count += other.nan_count;
        if let Some(v) = &other.min
            && self
                .min
                .as_ref()
                .is_none_or(|m| compare_values(v, m).is_lt())
        {
            self.min = Some(v.clone());
        }
        if let Some(v) = &other.max
            && self
                .max
                .as_ref()
                .is_none_or(|m| compare_values(v, m).is_gt())
        {
            self.max = Some(v.clone());
        }
    }

    /// Whether this zone *is where* a table-level bound came from — the cases
    /// a mutation has to re-derive from the other chunks.
    pub(crate) fn holds(&self, min: &Option<Value>, max: &Option<Value>) -> bool {
        (&self.min == min && min.is_some()) || (&self.max == max && max.is_some())
    }

    /// True when some row of a chunk with these bounds *could* satisfy
    /// `col op literal` (so the chunk must be read). `false` is a proof: a
    /// skipped chunk's rows would all have failed the filter.
    ///
    /// Conservative: an unknown shape (no bounds, a NaN-bound chunk for a
    /// comparison, a NULL test on a chunk with no NULL count) reads the chunk.
    pub(crate) fn may_match(&self, rows: u64, op: PruneOp, literal: Option<&Value>) -> bool {
        let all_null = self.null_count == rows;
        match op {
            PruneOp::IsNull => self.null_count > 0,
            PruneOp::IsNotNull => !all_null,
            _ => {
                let Some(literal) = literal else {
                    return true;
                };
                if all_null || self.nan_count > 0 {
                    // A NaN row compares as the maximum of the doubles, so a
                    // bound that excludes the literal proves nothing.
                    return !all_null;
                }
                let (Some(min), Some(max)) = (&self.min, &self.max) else {
                    return true;
                };
                // `x < lit` can only match when the chunk's minimum is
                // strictly below `lit`; `<=` allows equality. Getting these
                // two wrong by the boundary row would cost a chunk that does
                // match, so the strict forms are spelled out.
                let ge_min = !compare_values(literal, min).is_lt();
                let gt_min = compare_values(literal, min).is_gt();
                let le_max = !compare_values(literal, max).is_gt();
                let lt_max = compare_values(literal, max).is_lt();
                match op {
                    PruneOp::Eq => ge_min && le_max,
                    PruneOp::NotEq => {
                        !(compare_values(min, literal).is_eq()
                            && compare_values(max, literal).is_eq())
                    }
                    PruneOp::Lt => gt_min,
                    PruneOp::LtEq => ge_min,
                    PruneOp::Gt => lt_max,
                    PruneOp::GtEq => le_max,
                    PruneOp::IsNull | PruneOp::IsNotNull => unreachable!("handled above"),
                }
            }
        }
    }

    /// [`Zone::of_vector`]'s work, kept typed so no `Value` is allocated per
    /// row. Moved from the `.pink` writer (which now shares it through here).
    pub(crate) fn of_vector(v: &Vector) -> Zone {
        let null_count = v.null_count() as u64;
        let mut zone = Zone {
            null_count,
            min: None,
            max: None,
            nan_count: 0,
        };
        match v.data() {
            VectorData::Boolean(data) => {
                let (mut has_false, mut has_true) = (false, false);
                let mut see = |x: bool| {
                    if x {
                        has_true = true;
                    } else {
                        has_false = true;
                    }
                };
                if v.has_nulls() {
                    for (i, &x) in data.iter().enumerate() {
                        if v.is_valid(i) {
                            see(x);
                        }
                    }
                } else {
                    for &x in data {
                        see(x);
                    }
                }
                if has_false || has_true {
                    zone.min = Some(Value::Boolean(!has_false));
                    zone.max = Some(Value::Boolean(has_true));
                }
            }
            VectorData::Int32(data) => {
                let (min, max) = int_stats(v, data);
                zone.min = min.map(Value::Integer);
                zone.max = max.map(Value::Integer);
            }
            VectorData::Int64(data) => {
                let (min, max) = int_stats(v, data);
                zone.min = min.map(Value::Bigint);
                zone.max = max.map(Value::Bigint);
            }
            VectorData::Date32(data) => {
                let (min, max) = int_stats(v, data);
                zone.min = min.map(Value::Date);
                zone.max = max.map(Value::Date);
            }
            VectorData::TimestampMicros(data) => {
                let (min, max) = int_stats(v, data);
                zone.min = min.map(Value::Timestamp);
                zone.max = max.map(Value::Timestamp);
            }
            VectorData::Float64(data) => {
                let (mut min, mut max): (Option<f64>, Option<f64>) = (None, None);
                let mut see = |x: f64| {
                    if x.is_nan() {
                        zone.nan_count += 1;
                        return;
                    }
                    if min.is_none_or(|m| x < m) {
                        min = Some(x);
                    }
                    if max.is_none_or(|m| x > m) {
                        max = Some(x);
                    }
                };
                if v.has_nulls() {
                    for (i, &x) in data.iter().enumerate() {
                        if v.is_valid(i) {
                            see(x);
                        }
                    }
                } else {
                    for &x in data {
                        see(x);
                    }
                }
                zone.min = min.map(Value::Double);
                zone.max = max.map(Value::Double);
            }
            VectorData::VarChar(data) => {
                let (min, max) = str_stats(v, data);
                zone.min = min.map(|s| Value::Varchar(s.to_string()));
                zone.max = max.map(|s| Value::Varchar(s.to_string()));
            }
            VectorData::Decimal { values, scale, .. } => {
                let (min, max) = int_stats(v, values);
                zone.min = min.map(|x| Value::Decimal(x, *scale));
                zone.max = max.map(|x| Value::Decimal(x, *scale));
            }
            VectorData::Interval(data) => {
                let (mut min, mut max): (Option<Interval>, Option<Interval>) = (None, None);
                let mut see = |x: Interval| {
                    if min.is_none_or(|m| crate::types::interval_cmp(x, m).is_lt()) {
                        min = Some(x);
                    }
                    if max.is_none_or(|m| crate::types::interval_cmp(x, m).is_gt()) {
                        max = Some(x);
                    }
                };
                if v.has_nulls() {
                    for (i, &x) in data.iter().enumerate() {
                        if v.is_valid(i) {
                            see(x);
                        }
                    }
                } else {
                    for &x in data {
                        see(x);
                    }
                }
                zone.min = min.map(|iv| Value::Interval {
                    months: iv.months,
                    days: iv.days,
                    micros: iv.micros,
                });
                zone.max = max.map(|iv| Value::Interval {
                    months: iv.months,
                    days: iv.days,
                    micros: iv.micros,
                });
            }
            // No bounds: the total order spans kinds (see the module docs).
            VectorData::Variant(_) | VectorData::Array { .. } => {}
        }
        zone
    }
}

/// min/max over the valid elements of an integer chunk, first-seen on ties.
fn int_stats<T: Copy + Ord>(v: &Vector, data: &[T]) -> (Option<T>, Option<T>) {
    let (mut min, mut max): (Option<T>, Option<T>) = (None, None);
    let mut see = |x: T| {
        if min.is_none_or(|m| x < m) {
            min = Some(x);
        }
        if max.is_none_or(|m| x > m) {
            max = Some(x);
        }
    };
    if v.has_nulls() {
        for (i, &x) in data.iter().enumerate() {
            if v.is_valid(i) {
                see(x);
            }
        }
    } else {
        for &x in data {
            see(x);
        }
    }
    (min, max)
}

/// min/max over the valid elements of a string chunk, first-seen on ties
/// (byte order, exactly `Value::Varchar`'s `compare_values`).
fn str_stats<'a>(
    v: &Vector,
    data: &'a crate::strings::StrVec,
) -> (Option<&'a str>, Option<&'a str>) {
    let (mut min, mut max): (Option<&'a str>, Option<&'a str>) = (None, None);
    let mut see = |x: &'a str| {
        if min.is_none_or(|m| x < m) {
            min = Some(x);
        }
        if max.is_none_or(|m| x > m) {
            max = Some(x);
        }
    };
    if v.has_nulls() {
        for (i, s) in data.iter().enumerate() {
            if v.is_valid(i) {
                see(s);
            }
        }
    } else {
        for s in data.iter() {
            see(s);
        }
    }
    (min, max)
}

// ---------- table statistics ----------

/// One column's catalog statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct ColStats {
    pub null_count: u64,
    /// Min/max of the non-NULL values (NaN excluded, see the module docs).
    pub min: Option<Value>,
    pub max: Option<Value>,
    /// Distinct-count sketch. `Arc` so cloning a table (a transaction
    /// snapshot) shares it until a write; only a write pays the copy. `None`
    /// = no sketch (a catalog image without data, or a table loaded from one):
    /// the distinct count is then unknown, never guessed.
    pub hll: Option<Arc<Hll>>,
    /// The distinct estimate is an upper bound: a delete/update since the
    /// sketch was built. Cleared by `ANALYZE`.
    pub dirty: bool,
}

impl ColStats {
    pub(crate) fn new() -> Self {
        Self {
            null_count: 0,
            min: None,
            max: None,
            hll: Some(Arc::new(Hll::new())),
            dirty: false,
        }
    }

    /// Placeholder for a table whose rows are not in this image.
    pub(crate) fn unknown() -> Self {
        Self {
            null_count: 0,
            min: None,
            max: None,
            hll: None,
            dirty: true,
        }
    }

    /// Widen the bounds with an observed value.
    pub(crate) fn widen(&mut self, value: &Value) {
        if self
            .min
            .as_ref()
            .is_none_or(|m| compare_values(value, m).is_lt())
        {
            self.min = Some(value.clone());
        }
        if self
            .max
            .as_ref()
            .is_none_or(|m| compare_values(value, m).is_gt())
        {
            self.max = Some(value.clone());
        }
    }

    /// Fold one appended/updated value in (exact for appends; a substituted
    /// value leaves the sketch an over-count, flagged by `dirty`).
    pub(crate) fn observe(&mut self, v: &Value) {
        if v.is_null() {
            self.null_count += 1;
            return;
        }
        if let Some(hll) = &mut self.hll {
            Arc::make_mut(hll).insert_hash(value_hash(v));
        }
        if !matches!(v, Value::Double(x) if x.is_nan()) {
            self.widen(v);
        }
    }

    /// Fold a whole chunk in: the exact per-chunk zone plus one hash per
    /// non-NULL value.
    /// Merge a chunk's zone into the exact fields. The sketch is separate:
    /// a bulk append folds it per column on the worker pool and installs it
    /// with [`ColStats::set_sketch`], so the fold needs no mutable table.
    pub(crate) fn absorb_zone(&mut self, zone: &Zone) {
        self.null_count += zone.null_count;
        if let Some(min) = &zone.min {
            self.widen(min);
        }
        if let Some(max) = &zone.max {
            self.widen(max);
        }
    }

    /// Replace the sketch (see [`ColStats::absorb_zone`]); `None` keeps it
    /// unknown.
    pub(crate) fn set_sketch(&mut self, hll: Option<Arc<Hll>>) {
        if hll.is_some() {
            self.hll = hll;
        }
    }

    pub(crate) fn observe_chunk(&mut self, v: &Vector, zone: &Zone) {
        self.absorb_zone(zone);
        let Some(hll) = &mut self.hll else { return };
        let hll = Arc::make_mut(hll);
        if v.has_nulls() {
            for row in 0..v.len() {
                hll.insert_cell(v, row);
            }
        } else {
            for row in 0..v.len() {
                hll.insert_hash(key_hash(v, row));
            }
        }
    }

    /// The distinct-count estimate, or `None` when unknown.
    pub fn distinct_count(&self) -> Option<u64> {
        self.hll.as_ref().map(|h| h.estimate())
    }
}

/// The statistics of one table, one entry per column (schema order).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableStats {
    pub rows: u64,
    pub cols: Vec<ColStats>,
}

impl TableStats {
    pub(crate) fn new(ncols: usize) -> Self {
        Self {
            rows: 0,
            cols: (0..ncols).map(|_| ColStats::new()).collect(),
        }
    }

    /// A table whose rows this image does not carry (the catalog image of a
    /// stateless server): row count only, every column unknown.
    pub(crate) fn unknown(ncols: usize, rows: u64) -> Self {
        Self {
            rows,
            cols: (0..ncols).map(|_| ColStats::unknown()).collect(),
        }
    }

    /// Exact statistics of `columns` at load time: zones for every chunk and a
    /// sketch per column, one pass over the data.
    pub(crate) fn scan(columns: &[crate::storage::Column], rows: u64) -> (Self, Vec<Vec<Zone>>) {
        let mut zones: Vec<Vec<Zone>> = Vec::with_capacity(columns.len());
        let mut stats = Self::new(columns.len());
        stats.rows = rows;
        for (ci, col) in columns.iter().enumerate() {
            let mut col_zones = Vec::with_capacity(col.chunks.len());
            for chunk in &col.chunks {
                let zone = Zone::of_vector(chunk);
                stats.cols[ci].observe_chunk(chunk, &zone);
                col_zones.push(zone);
            }
            zones.push(col_zones);
        }
        (stats, zones)
    }

    /// A column's approximate distinct count: `None` when the sketch is
    /// unknown or stale (a delete/update since it was built).
    pub fn distinct(&self, col: usize) -> Option<u64> {
        let c = self.cols.get(col)?;
        if c.dirty {
            return None;
        }
        c.distinct_count()
    }

    pub fn col(&self, col: usize) -> Option<&ColStats> {
        self.cols.get(col)
    }
}

// ---------- join filter ----------

/// A blocked Bloom filter over a hash join's build keys: fixed 512-bit blocks,
/// two probes into one block, so one probe is one cache line (DuckDB's
/// `BlockedBloomFilter` shape).
///
/// The values are hashed with [`key_hash`], the same function the probe scan
/// uses, so a build key always passes. `contains == false` proves "not in the
/// build side"; a false positive only costs a row the hash join drops anyway.
#[derive(Debug, Clone)]
pub(crate) struct Bloom {
    words: Vec<u64>,
    blocks_mask: u64,
}

const BLOOM_BLOCK_WORDS: usize = 8;

impl Bloom {
    /// Size the filter for `rows` distinct keys at a 1 % false-positive rate
    /// (`m = -n ln(p) / ln(2)^2`), capped so a runaway build cannot allocate
    /// gigabytes.
    pub(crate) fn with_rows(rows: u64) -> Bloom {
        let n = rows.clamp(1, 1 << 26) as f64;
        let bits = (-n * 0.01f64.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2)).ceil();
        let blocks = (((bits as u64).div_ceil(512)).max(1)).next_power_of_two();
        Bloom {
            words: vec![0; blocks as usize * BLOOM_BLOCK_WORDS],
            blocks_mask: blocks - 1,
        }
    }

    fn slots(&self, h: u64) -> (usize, u64, u64) {
        let block = ((h >> 32) & self.blocks_mask) as usize;
        let second = mix(h ^ 0x9e37_79b9_7f4a_7c15);
        (
            block * BLOOM_BLOCK_WORDS,
            (h as usize & 511) as u64,
            (second as usize & 511) as u64,
        )
    }

    pub(crate) fn insert(&mut self, h: u64) {
        let (base, a, b) = self.slots(h);
        self.words[base + (a / 64) as usize] |= 1 << (a % 64);
        self.words[base + (b / 64) as usize] |= 1 << (b % 64);
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.words.len() * 8
    }

    pub(crate) fn contains(&self, h: u64) -> bool {
        let (base, a, b) = self.slots(h);
        (self.words[base + (a / 64) as usize] >> (a % 64)) & 1 == 1
            && (self.words[base + (b / 64) as usize] >> (b % 64)) & 1 == 1
    }
}

/// The key-side filter a hash join hands to its probe scans (wave 122).
#[derive(Debug, Clone)]
pub(crate) struct JoinFilter {
    /// The build keys' non-NULL bounds, compared with the probe column's own
    /// comparator (so a widened type on either side stays correct).
    pub min: Option<Value>,
    pub max: Option<Value>,
    /// The build keys' dtype: a bloom is only attached when the probe column
    /// has exactly this dtype, because [`key_hash`] is type-directed.
    pub dtype: DataType,
    pub bloom: Arc<Bloom>,
    /// `min`/`max` as integers when the keys are INTEGER, BIGINT, DATE or
    /// TIMESTAMP, so the probe compares payloads instead of `Value`s.
    int_range: Option<(i64, i64)>,
}

/// An integer-family key bound as `i64`.
fn int_bound(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(x) | Value::Date(x) => Some(i64::from(*x)),
        Value::Bigint(x) | Value::Timestamp(x) => Some(*x),
        _ => None,
    }
}

/// An integer-family probe cell as `i64`.
fn int_cell(v: &Vector, row: usize) -> Option<i64> {
    match v.data() {
        VectorData::Int32(d) | VectorData::Date32(d) => Some(i64::from(d[row])),
        VectorData::Int64(d) | VectorData::TimestampMicros(d) => Some(d[row]),
        _ => None,
    }
}

/// Accumulates a [`JoinFilter`] for one build key, chunk by chunk, without
/// collecting the key values first.
pub(crate) struct JoinFilterBuilder {
    dtype: DataType,
    zone: Zone,
    bloom: Bloom,
}

impl JoinFilterBuilder {
    /// `distinct` sizes the bloom: the catalog's exact distinct estimate when
    /// there is one, else the build's row count — an upper bound on the key
    /// count, so the false-positive rate stays at or below the target.
    pub(crate) fn new(dtype: DataType, distinct: Option<u64>, rows: u64) -> Self {
        Self {
            dtype,
            zone: Zone::empty(),
            bloom: Bloom::with_rows(distinct.unwrap_or(rows)),
        }
    }

    /// Fold one already-normalized build key column in.
    pub(crate) fn push(&mut self, v: &Vector) {
        self.zone.merge(&Zone::of_vector(v));
        for row in 0..v.len() {
            if v.is_valid(row) {
                self.bloom.insert(key_hash(v, row));
            }
        }
    }

    pub(crate) fn finish(self) -> JoinFilter {
        let int_range = match (&self.zone.min, &self.zone.max) {
            (Some(lo), Some(hi)) => int_bound(lo).zip(int_bound(hi)),
            _ => None,
        };
        JoinFilter {
            min: self.zone.min,
            max: self.zone.max,
            dtype: self.dtype,
            bloom: Arc::new(self.bloom),
            int_range,
        }
    }
}

impl JoinFilter {
    /// [`JoinFilter::may_match`] for every row of `v` at once into `pass`
    /// (`pass[i] &= …`): integer payloads run one typed loop — range test,
    /// [`key_hash`]'s hash and the bloom — instead of re-dispatching on the
    /// payload per row; other payloads take the per-row test.
    pub(crate) fn may_match_all(&self, v: &Vector, pass: &mut [bool]) {
        if v.dtype() != self.dtype {
            return;
        }
        #[inline(always)]
        fn run(
            f: &JoinFilter,
            v: &Vector,
            pass: &mut [bool],
            salt: u64,
            cell: impl Fn(usize) -> (i64, u64),
        ) {
            let (lo, hi) = f.int_range.unwrap_or((i64::MIN, i64::MAX));
            let nulls = v.has_nulls();
            for (i, p) in pass.iter_mut().enumerate() {
                // A NULL cell never matches, but the join decides that.
                if !*p || (nulls && !v.is_valid(i)) {
                    continue;
                }
                let (x, bits) = cell(i);
                *p = x >= lo && x <= hi && f.bloom.contains(mix(bits ^ salt));
            }
        }
        match v.data() {
            VectorData::Int32(d) => run(self, v, pass, 0, |i| (i64::from(d[i]), d[i] as u64)),
            VectorData::Int64(d) => run(self, v, pass, 0x20, |i| (d[i], d[i] as u64)),
            VectorData::Date32(d) => run(self, v, pass, 0x30, |i| (i64::from(d[i]), d[i] as u64)),
            VectorData::TimestampMicros(d) => run(self, v, pass, 0x40, |i| (d[i], d[i] as u64)),
            _ => {
                for (i, p) in pass.iter_mut().enumerate() {
                    if *p {
                        *p = self.may_match(v, i);
                    }
                }
            }
        }
    }

    /// Whether a probe cell could be a build key. `false` is a proof (the row
    /// cannot match), `true` only means "let the join decide".
    pub(crate) fn may_match(&self, v: &Vector, row: usize) -> bool {
        if !v.is_valid(row) {
            // NULL never matches in a hash join.
            return true;
        }
        if v.dtype() != self.dtype {
            return true;
        }
        // A NaN key matches itself in this engine, and no bound is meaningful
        // for it, so the range below would reject a row the join keeps.
        if let VectorData::Float64(d) = v.data()
            && d[row].is_nan()
        {
            return true;
        }
        // The range check reads the payload where it can: integers against
        // the precomputed range, strings borrowed (no `Value` per row).
        if let Some((lo, hi)) = self.int_range
            && let Some(x) = int_cell(v, row)
        {
            if x < lo || x > hi {
                return false;
            }
        } else if let VectorData::VarChar(d) = v.data() {
            let s: &str = &d[row];
            if let Some(Value::Varchar(min)) = &self.min
                && s < min.as_str()
            {
                return false;
            }
            if let Some(Value::Varchar(max)) = &self.max
                && s > max.as_str()
            {
                return false;
            }
        } else {
            if let Some(min) = &self.min {
                let value = v.get(row);
                if compare_values(&value, min).is_lt() {
                    return false;
                }
            }
            if let Some(max) = &self.max {
                let value = v.get(row);
                if compare_values(&value, max).is_gt() {
                    return false;
                }
            }
        }
        self.bloom.contains(key_hash(v, row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    fn int_vector(values: &[Option<i64>]) -> Vector {
        let mut v = Vector::with_capacity(DataType::Bigint, values.len());
        for x in values {
            match x {
                Some(x) => v.push_valid(Value::Bigint(*x)).expect("push"),
                None => v.push_null().expect("push"),
            }
        }
        v
    }

    /// The chunk-at-a-time test agrees with the per-row one on every integer
    /// payload, NULLs and out-of-range cells included, and leaves rows a
    /// previous filter already rejected rejected.
    #[test]
    fn may_match_all_agrees_with_may_match() {
        for dtype in [
            DataType::Integer,
            DataType::Bigint,
            DataType::Date,
            DataType::Timestamp,
            DataType::Varchar,
        ] {
            let cell = |i: i64| match dtype {
                DataType::Integer => Value::Integer(i as i32),
                DataType::Bigint => Value::Bigint(i),
                DataType::Date => Value::Date(i as i32),
                DataType::Timestamp => Value::Timestamp(i),
                _ => Value::Varchar(format!("k{i}")),
            };
            let mut build = Vector::with_capacity(dtype, 300);
            for i in (100..400).step_by(3) {
                build.push_valid(cell(i)).expect("push");
            }
            let mut builder = JoinFilterBuilder::new(dtype, None, 100);
            builder.push(&build);
            let filter = builder.finish();
            let mut probe = Vector::with_capacity(dtype, 600);
            for i in 0..600i64 {
                if i % 29 == 0 {
                    probe.push_null().expect("push");
                } else {
                    probe.push_valid(cell(i)).expect("push");
                }
            }
            let mut pass: Vec<bool> = (0..600).map(|i| i % 7 != 0).collect();
            filter.may_match_all(&probe, &mut pass);
            for (i, p) in pass.iter().enumerate() {
                let want = i % 7 != 0 && filter.may_match(&probe, i);
                assert_eq!(*p, want, "{dtype:?} row {i}");
            }
            // The build keys themselves always pass.
            assert!(
                (100..400)
                    .step_by(3)
                    .all(|i| pass[i] || i % 7 == 0 || i % 29 == 0)
            );
        }
    }

    #[test]
    fn hll_counts_distinct_within_its_error() {
        for n in [0usize, 1, 10, 100, 1_000, 10_000, 100_000] {
            let mut hll = Hll::new();
            for i in 0..n {
                hll.insert_hash(value_hash(&Value::Bigint(i as i64)));
            }
            let est = hll.estimate() as f64;
            let err = (est - n as f64).abs() / (n.max(1) as f64);
            assert!(err < 0.05, "n={n} estimate={est} error={err}");
        }
        // Duplicates do not raise the estimate.
        let mut hll = Hll::new();
        for _ in 0..1000 {
            hll.insert_hash(value_hash(&Value::Bigint(7)));
        }
        assert_eq!(hll.estimate(), 1);
    }

    #[test]
    fn hll_round_trips_through_bytes() {
        let mut hll = Hll::new();
        for i in 0..500 {
            hll.insert_hash(value_hash(&Value::Varchar(format!("k{i}"))));
        }
        let bytes = hll.to_bytes().to_vec();
        let back = Hll::from_bytes(&bytes).expect("same precision");
        assert_eq!(back, hll);
        assert_eq!(back.estimate(), hll.estimate());
    }

    #[test]
    fn hll_merge_is_a_union() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..1000 {
            a.insert_hash(value_hash(&Value::Bigint(i)));
        }
        for i in 500..1500 {
            b.insert_hash(value_hash(&Value::Bigint(i)));
        }
        a.merge(&b);
        let err = (a.estimate() as f64 - 1500.0).abs() / 1500.0;
        assert!(err < 0.05, "merged={} error={err}", a.estimate());
    }

    #[test]
    fn zone_bounds_ignore_null_and_nan_and_merge_widens() {
        let v = int_vector(&[Some(3), None, Some(-2), Some(9)]);
        let zone = Zone::of_vector(&v);
        assert_eq!(zone.null_count, 1);
        assert_eq!(zone.min, Some(Value::Bigint(-2)));
        assert_eq!(zone.max, Some(Value::Bigint(9)));
        let mut merged = Zone::empty();
        merged.merge(&zone);
        merged.merge(&Zone::of_vector(&int_vector(&[Some(100)])));
        assert_eq!(merged.min, Some(Value::Bigint(-2)));
        assert_eq!(merged.max, Some(Value::Bigint(100)));
        assert_eq!(merged.null_count, 1);

        let mut d = Vector::with_capacity(DataType::Double, 3);
        d.push_valid(Value::Double(1.0)).expect("push");
        d.push_valid(Value::Double(f64::NAN)).expect("push");
        d.push_valid(Value::Double(-1.0)).expect("push");
        let z = Zone::of_vector(&d);
        assert_eq!(z.nan_count, 1);
        assert_eq!(z.min, Some(Value::Double(-1.0)));
        assert_eq!(z.max, Some(Value::Double(1.0)));
        // A NaN anywhere disables comparison pruning (NaN sorts above all).
        assert!(z.may_match(3, PruneOp::Gt, Some(&Value::Double(100.0))));
    }

    #[test]
    fn zone_prunes_only_what_it_proves() {
        let zone = Zone {
            null_count: 0,
            min: Some(Value::Integer(10)),
            max: Some(Value::Integer(20)),
            nan_count: 0,
        };
        assert!(zone.may_match(4, PruneOp::Eq, Some(&Value::Integer(15))));
        assert!(!zone.may_match(4, PruneOp::Eq, Some(&Value::Integer(21))));
        assert!(!zone.may_match(4, PruneOp::Lt, Some(&Value::Integer(10))));
        assert!(zone.may_match(4, PruneOp::Lt, Some(&Value::Integer(11))));
        assert!(!zone.may_match(4, PruneOp::Gt, Some(&Value::Integer(20))));
        assert!(zone.may_match(4, PruneOp::NotEq, Some(&Value::Integer(15))));
        assert!(!zone.may_match(4, PruneOp::IsNull, None));
        assert!(zone.may_match(4, PruneOp::IsNotNull, None));
        // NULL-only chunk.
        let nulls = Zone {
            null_count: 4,
            min: None,
            max: None,
            nan_count: 0,
        };
        assert!(nulls.may_match(4, PruneOp::IsNull, None));
        assert!(!nulls.may_match(4, PruneOp::IsNotNull, None));
        assert!(!nulls.may_match(4, PruneOp::Eq, Some(&Value::Integer(1))));
        // Unknown bounds: always read.
        let unknown = Zone::empty();
        assert!(unknown.may_match(4, PruneOp::Eq, Some(&Value::Integer(1))));
    }

    #[test]
    fn bloom_has_no_false_negatives_and_a_small_false_positive_rate() {
        let n = 10_000u64;
        let mut bloom = Bloom::with_rows(n);
        for i in 0..n {
            bloom.insert(key_hash(&int_vector(&[Some(i as i64)]), 0));
        }
        for i in 0..n {
            let v = int_vector(&[Some(i as i64)]);
            assert!(bloom.contains(key_hash(&v, 0)), "key {i} missing");
        }
        let mut fp = 0;
        for i in n..(n + 10_000) {
            let v = int_vector(&[Some(i as i64)]);
            fp += usize::from(bloom.contains(key_hash(&v, 0)));
        }
        assert!(fp < 400, "false positives: {fp}/10000");
        assert!(bloom.bytes() <= 64 * 1024);
    }

    #[test]
    fn bloom_small_and_empty_builds_stay_correct() {
        let empty = Bloom::with_rows(0);
        assert!(!empty.contains(123));
        let mut one = Bloom::with_rows(1);
        one.insert(key_hash(&int_vector(&[Some(42)]), 0));
        assert!(one.contains(key_hash(&int_vector(&[Some(42)]), 0)));
    }

    #[test]
    fn value_hash_agrees_with_key_hash_for_every_kind() {
        let cases: Vec<(Vector, Vec<Value>)> = vec![
            (int_vector(&[Some(-5)]), vec![Value::Bigint(-5)]),
            (
                {
                    let mut v = Vector::with_capacity(DataType::Integer, 1);
                    v.push_valid(Value::Integer(7)).expect("push");
                    v
                },
                vec![Value::Integer(7)],
            ),
            (
                {
                    let mut v = Vector::with_capacity(DataType::Varchar, 1);
                    v.push_valid(Value::Varchar("abc".to_string()))
                        .expect("push");
                    v
                },
                vec![Value::Varchar("abc".to_string())],
            ),
            (
                {
                    let mut v = Vector::with_capacity(DataType::Double, 1);
                    v.push_valid(Value::Double(-0.0)).expect("push");
                    v
                },
                vec![Value::Double(0.0)],
            ),
            (
                {
                    let mut v = Vector::with_capacity(
                        DataType::Decimal {
                            precision: 10,
                            scale: 2,
                        },
                        1,
                    );
                    v.push_valid(Value::Decimal(12345, 2)).expect("push");
                    v
                },
                vec![Value::Decimal(12345, 2)],
            ),
        ];
        for (vector, values) in cases {
            for (row, value) in values.iter().enumerate() {
                assert_eq!(
                    key_hash(&vector, row),
                    value_hash(value),
                    "vector vs value hash of {value:?}"
                );
            }
        }
    }
}
