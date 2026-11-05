//! Columnar vectors, DuckDB-style.
//!
//! `Vector` = one typed column chunk + a validity bitmask (`Vec<u64>`,
//! bit set = valid, clear = NULL). `DataChunk` = a horizontal slice of
//! columns (all the same length, ≤ [`CHUNK_SIZE`] rows). Operators exchange
//! `DataChunk`s; `WHERE` produces a selection vector applied via `gather`
//! instead of copying rows eagerly.

use std::sync::Arc;

use crate::array::{ArrayElems, ArrayValue};
use crate::types::{ArrayElem, DataType, Interval, PinkDbError, Result, Value};

/// Max rows per vector / chunk. DuckDB uses 2048; so do we.
pub const CHUNK_SIZE: usize = 2048;

#[derive(Debug, Clone, PartialEq)]
pub enum VectorData {
    Boolean(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    VarChar(crate::strings::StrVec),
    Date32(Vec<i32>),
    TimestampMicros(Vec<i64>),
    /// DECIMAL: unscaled i128 values. Precision/scale ride with the payload
    /// so [`Vector::dtype`] reconstructs the column's `DECIMAL(p,s)` exactly.
    Decimal {
        values: Vec<i128>,
        precision: u8,
        scale: u8,
    },
    /// DECIMAL of precision 18 or less (wave 142): the same unscaled values
    /// in 64 bits, as DuckDB stores `DECIMAL(p ≤ 18)`. The precision alone
    /// picks the payload — `DECIMAL(p ≤ 18)` is always `Decimal64`, wider is
    /// always `Decimal` — so one type never has two payloads.
    Decimal64 {
        values: Vec<i64>,
        precision: u8,
        scale: u8,
    },
    /// INTERVAL: DuckDB's `(months, days, micros)` triples.
    Interval(Vec<Interval>),
    /// VARIANT: one shared semi-structured tree per row (see
    /// [`crate::variant`]).
    Variant(Vec<Arc<crate::variant::Variant>>),
    /// ARRAY: `rows * len` elements in one flat row-major buffer, plus an
    /// element validity bitmap that only exists when some element is NULL
    /// (see [`crate::array`]).
    Array {
        elem: ArrayElem,
        len: u32,
        values: ArrayElems,
        valid: Option<Vec<u8>>,
    },
    /// UUID: the unsigned 128-bit values.
    Uuid(Vec<u128>),
    /// Signed 128-bit integers (`HUGEINT`).
    Int128(Vec<i128>),
    /// Unsigned 128-bit integers (`UHUGEINT`).
    UInt128(Vec<u128>),
}

/// Bit `i` of an optional element bitmap; no bitmap means every element is
/// valid.
fn elem_bit(bits: Option<&[u8]>, i: usize) -> bool {
    bits.map(|m| (m[i / 8] >> (i % 8)) & 1 == 1).unwrap_or(true)
}

/// Append `n` element validity bits to a column's element bitmap, growing it
/// only when a NULL element appears. `start` is the element count before the
/// append; elements before it are all valid when the bitmap is absent.
fn array_bits_append(
    valid: &mut Option<Vec<u8>>,
    start: usize,
    bits: Option<&[u8]>,
    bit_off: usize,
    n: usize,
) {
    if n == 0 {
        return;
    }
    let any_null = (0..n).any(|i| !elem_bit(bits, bit_off + i));
    if valid.is_none() && !any_null {
        return;
    }
    let total = start + n;
    let need = total.div_ceil(8);
    let mask = match valid {
        Some(m) => {
            m.resize(need, 0);
            m
        }
        None => {
            let mut v = vec![0xFFu8; need];
            if !total.is_multiple_of(8) {
                v[need - 1] = (1u8 << (total % 8)) - 1;
            }
            valid.insert(v)
        }
    };
    for i in 0..n {
        let idx = start + i;
        if elem_bit(bits, bit_off + i) {
            mask[idx / 8] |= 1u8 << (idx % 8);
        } else {
            mask[idx / 8] &= !(1u8 << (idx % 8));
        }
    }
}

/// Append one ARRAY cell to a column chunk, checking the element kind and
/// length against the column's type.
fn array_push_cell(
    elem: ArrayElem,
    len: u32,
    values: &mut ArrayElems,
    valid: &mut Option<Vec<u8>>,
    cell: &ArrayValue,
) -> Result<()> {
    if cell.elem != elem || cell.len() != len as usize {
        return Err(PinkDbError::Type(format!(
            "type mismatch: cannot store {} in a {}[{len}] vector",
            cell.dtype(),
            elem.name()
        )));
    }
    let start = values.len();
    values.extend_from(elem, &cell.values)?;
    array_bits_append(valid, start, cell.valid.as_deref(), 0, cell.len());
    Ok(())
}

/// The widest DECIMAL precision stored in 64 bits (wave 142): 18 digits fit
/// an `i64`, as in DuckDB's physical-type choice.
pub const DECIMAL64_MAX_PRECISION: u8 = 18;

impl VectorData {
    /// A DECIMAL payload from unscaled 128-bit values, stored in the width its
    /// precision picks: `Decimal64` up to 18 digits (the values must fit,
    /// which a `DECIMAL(p ≤ 18)` value always does), `Decimal` beyond.
    pub fn decimal_from_i128(values: Vec<i128>, precision: u8, scale: u8) -> VectorData {
        if precision <= DECIMAL64_MAX_PRECISION {
            VectorData::Decimal64 {
                values: values
                    .into_iter()
                    .map(|v| {
                        debug_assert!(i64::try_from(v).is_ok(), "DECIMAL({precision}) value {v}");
                        v as i64
                    })
                    .collect(),
                precision,
                scale,
            }
        } else {
            VectorData::Decimal {
                values,
                precision,
                scale,
            }
        }
    }

    /// Row `i` of a DECIMAL payload as its unscaled 128-bit value, whatever
    /// its physical width (`None` for a non-DECIMAL payload).
    #[inline]
    pub fn decimal_at(&self, i: usize) -> Option<i128> {
        match self {
            VectorData::Decimal { values, .. } => Some(values[i]),
            VectorData::Decimal64 { values, .. } => Some(i128::from(values[i])),
            _ => None,
        }
    }

    /// A DECIMAL payload's unscaled values as 128-bit integers (a copy for
    /// the 64-bit form): the form the cold paths compute on.
    pub fn decimal_values_i128(&self) -> Option<std::borrow::Cow<'_, [i128]>> {
        match self {
            VectorData::Decimal { values, .. } => Some(std::borrow::Cow::Borrowed(values)),
            VectorData::Decimal64 { values, .. } => Some(std::borrow::Cow::Owned(
                values.iter().map(|&v| i128::from(v)).collect(),
            )),
            _ => None,
        }
    }

    /// The wave-142 invariant: `Decimal64` exactly for precision ≤ 18.
    pub(crate) fn decimal_width_ok(&self) -> bool {
        match self {
            VectorData::Decimal { precision, .. } => *precision > DECIMAL64_MAX_PRECISION,
            VectorData::Decimal64 { precision, .. } => *precision <= DECIMAL64_MAX_PRECISION,
            _ => true,
        }
    }

    pub(crate) fn dtype(&self) -> DataType {
        match self {
            VectorData::Boolean(_) => DataType::Boolean,
            VectorData::Int32(_) => DataType::Integer,
            VectorData::Int64(_) => DataType::Bigint,
            VectorData::Float64(_) => DataType::Double,
            VectorData::VarChar(_) => DataType::Varchar,
            VectorData::Date32(_) => DataType::Date,
            VectorData::TimestampMicros(_) => DataType::Timestamp,
            VectorData::Decimal {
                precision, scale, ..
            }
            | VectorData::Decimal64 {
                precision, scale, ..
            } => DataType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            VectorData::Interval(_) => DataType::Interval,
            VectorData::Variant(_) => DataType::Variant,
            VectorData::Array { elem, len, .. } => DataType::Array {
                elem: *elem,
                len: *len,
            },
            VectorData::Uuid(_) => DataType::Uuid,
            VectorData::Int128(_) => DataType::Hugeint,
            VectorData::UInt128(_) => DataType::Uhugeint,
        }
    }

    /// True when the payload holds no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of elements in the payload.
    pub fn len(&self) -> usize {
        match self {
            VectorData::Boolean(v) => v.len(),
            VectorData::Int32(v) => v.len(),
            VectorData::Int64(v) => v.len(),
            VectorData::Float64(v) => v.len(),
            VectorData::VarChar(v) => v.len(),
            VectorData::Date32(v) => v.len(),
            VectorData::TimestampMicros(v) => v.len(),
            VectorData::Decimal { values, .. } => values.len(),
            VectorData::Decimal64 { values, .. } => values.len(),
            VectorData::Interval(v) => v.len(),
            VectorData::Variant(v) => v.len(),
            // Rows, like every other variant; `len` is never zero.
            VectorData::Array { len, values, .. } => values.len() / *len as usize,
            VectorData::Uuid(v) => v.len(),
            VectorData::Int128(v) => v.len(),
            VectorData::UInt128(v) => v.len(),
        }
    }

    fn with_capacity(dtype: DataType, cap: usize) -> Self {
        match dtype {
            DataType::Boolean => VectorData::Boolean(Vec::with_capacity(cap)),
            DataType::Integer => VectorData::Int32(Vec::with_capacity(cap)),
            DataType::Bigint => VectorData::Int64(Vec::with_capacity(cap)),
            DataType::Double => VectorData::Float64(Vec::with_capacity(cap)),
            DataType::Varchar => VectorData::VarChar(crate::strings::StrVec::with_capacity(cap)),
            DataType::Date => VectorData::Date32(Vec::with_capacity(cap)),
            DataType::Timestamp => VectorData::TimestampMicros(Vec::with_capacity(cap)),
            DataType::Decimal { precision, scale } if precision <= DECIMAL64_MAX_PRECISION => {
                VectorData::Decimal64 {
                    values: Vec::with_capacity(cap),
                    precision,
                    scale,
                }
            }
            DataType::Decimal { precision, scale } => VectorData::Decimal {
                values: Vec::with_capacity(cap),
                precision,
                scale,
            },
            DataType::Interval => VectorData::Interval(Vec::with_capacity(cap)),
            DataType::Variant => VectorData::Variant(Vec::with_capacity(cap)),
            DataType::Array { elem, len } => VectorData::Array {
                elem,
                len,
                values: ArrayElems::with_capacity(elem, cap * len as usize),
                valid: None,
            },
            DataType::Uuid => VectorData::Uuid(Vec::with_capacity(cap)),
            DataType::Hugeint => VectorData::Int128(Vec::with_capacity(cap)),
            DataType::Uhugeint => VectorData::UInt128(Vec::with_capacity(cap)),
        }
    }

    /// Append `n` placeholder payload entries in one go — the bulk form of
    /// `Vector::push_placeholder`: only the validity mask marks them NULL.
    fn extend_placeholders(&mut self, n: usize) {
        match self {
            VectorData::Boolean(v) => v.resize(v.len() + n, false),
            VectorData::Int32(v) => v.resize(v.len() + n, 0),
            VectorData::Int64(v) => v.resize(v.len() + n, 0),
            VectorData::Float64(v) => v.resize(v.len() + n, 0.0),
            VectorData::VarChar(v) => {
                for _ in 0..n {
                    v.push("");
                }
            }
            VectorData::Date32(v) => v.resize(v.len() + n, 0),
            VectorData::TimestampMicros(v) => v.resize(v.len() + n, 0),
            VectorData::Decimal { values, .. } => values.resize(values.len() + n, 0),
            VectorData::Decimal64 { values, .. } => values.resize(values.len() + n, 0),
            VectorData::Interval(v) => v.resize(v.len() + n, Interval::default()),
            VectorData::Variant(v) => {
                v.resize(v.len() + n, Arc::new(crate::variant::Variant::Null));
            }
            VectorData::Array {
                len, values, valid, ..
            } => {
                for _ in 0..n {
                    for _ in 0..*len {
                        values.push_zero();
                    }
                }
                if let Some(mask) = valid {
                    mask.resize(values.len().div_ceil(8), 0);
                }
            }
            VectorData::Uuid(v) => v.resize(v.len() + n, 0),
            VectorData::Int128(v) => v.resize(v.len() + n, 0),
            VectorData::UInt128(v) => v.resize(v.len() + n, 0),
        }
    }
}

/// One typed column chunk with NULL bitmask.
#[derive(Debug, Clone, PartialEq)]
pub struct Vector {
    data: VectorData,
    /// Bit `i` set ⇒ row `i` is valid; clear ⇒ NULL. Length = ceil(len/64).
    validity: Vec<u64>,
    len: usize,
}

/// The source index of a [`Vector::gather_located`] pick that is NULL.
pub(crate) const GATHER_NULL: u32 = u32::MAX;

/// A row index type a selection may use (`u32` selection vectors, `usize`
/// gathers).
trait RowIndex: Copy {
    fn index(self) -> usize;
}

impl RowIndex for u32 {
    #[inline]
    fn index(self) -> usize {
        self as usize
    }
}

impl RowIndex for usize {
    #[inline]
    fn index(self) -> usize {
        self
    }
}

fn mask_len(len: usize) -> usize {
    len.div_ceil(64)
}

#[inline]
fn bit_get(mask: &[u64], idx: usize) -> bool {
    (mask[idx / 64] >> (idx % 64)) & 1 == 1
}

#[inline]
fn bit_set(mask: &mut [u64], idx: usize) {
    mask[idx / 64] |= 1u64 << (idx % 64);
}

#[inline]
fn bit_clear(mask: &mut [u64], idx: usize) {
    mask[idx / 64] &= !(1u64 << (idx % 64));
}

/// Error for a typed push into the wrong column layout (internal fast paths).
fn typed_push_error(what: &str, into: DataType) -> PinkDbError {
    PinkDbError::Type(format!(
        "type mismatch: cannot push {what} into {into:?} vector"
    ))
}

impl Vector {
    pub fn new_empty(dtype: DataType) -> Self {
        Self::with_capacity(dtype, 0)
    }

    pub fn with_capacity(dtype: DataType, cap: usize) -> Self {
        Self {
            data: VectorData::with_capacity(dtype, cap.min(CHUNK_SIZE)),
            validity: vec![0u64; mask_len(cap.min(CHUNK_SIZE))],
            len: 0,
        }
    }

    /// Build a constant vector of `n` identical rows without per-row `Value`
    /// materialization or string cloning. `NULL` ⇒ INTEGER-typed vector with
    /// every validity bit clear (matches the old `push_value(Value::Null)`
    /// loop); scalars ⇒ `n` valid rows; VARCHAR ⇒ one shared `Arc<str>`
    /// cloned `n` times.
    pub(crate) fn new_const(value: &Value, n: usize) -> Vector {
        debug_assert!(n <= CHUNK_SIZE, "constant vector exceeds CHUNK_SIZE");
        let (data, all_valid) = match value {
            Value::Null => (VectorData::Int32(vec![0; n]), false),
            Value::Boolean(b) => (VectorData::Boolean(vec![*b; n]), true),
            Value::Integer(i) => (VectorData::Int32(vec![*i; n]), true),
            Value::Bigint(i) => (VectorData::Int64(vec![*i; n]), true),
            Value::Double(x) => (VectorData::Float64(vec![*x; n]), true),
            Value::Varchar(s) => (
                VectorData::VarChar(crate::strings::StrVec::repeat(s, n)),
                true,
            ),
            Value::Date(d) => (VectorData::Date32(vec![*d; n]), true),
            Value::Timestamp(us) => (VectorData::TimestampMicros(vec![*us; n]), true),
            Value::Decimal {
                value: v,
                precision,
                scale,
            } => (
                VectorData::decimal_from_i128(vec![*v; n], *precision, *scale),
                true,
            ),
            Value::Interval {
                months,
                days,
                micros,
            } => (
                VectorData::Interval(vec![
                    Interval {
                        months: *months,
                        days: *days,
                        micros: *micros,
                    };
                    n
                ]),
                true,
            ),
            // A constant VARIANT shares one tree across the rows.
            Value::Variant(x) => (VectorData::Variant(vec![Arc::clone(x); n]), true),
            Value::Array(x) => {
                let len = x.len() as u32;
                let mut values = ArrayElems::with_capacity(x.elem, n * x.len());
                let mut valid: Option<Vec<u8>> = None;
                for _ in 0..n {
                    array_push_cell(x.elem, len, &mut values, &mut valid, x)
                        .expect("constant ARRAY vector");
                }
                (
                    VectorData::Array {
                        elem: x.elem,
                        len,
                        values,
                        valid,
                    },
                    true,
                )
            }
            Value::Uuid(bits) => (VectorData::Uuid(vec![*bits; n]), true),
            Value::Hugeint(x) => (VectorData::Int128(vec![*x; n]), true),
            Value::Uhugeint(x) => (VectorData::UInt128(vec![*x; n]), true),
        };
        let mut validity = vec![if all_valid { u64::MAX } else { 0 }; mask_len(n)];
        if all_valid && !n.is_multiple_of(64) {
            // Bits past `len` must read as NULL, like push-built masks do.
            validity[mask_len(n) - 1] = (1u64 << (n % 64)) - 1;
        }
        Vector::from_parts(data, validity, n)
    }

    /// Build `n` rows of `value` in one allocation: the constant column a scan
    /// materializes (a column added after the file was written, filled with its
    /// DEFAULT, or all NULL). NULLs are keyed on `dtype` — a NULL value carries
    /// no type of its own, and the column's readers expect its dtype.
    pub fn constant(dtype: DataType, value: &Value, n: usize) -> Result<Vector> {
        debug_assert!(n <= CHUNK_SIZE, "constant vector exceeds CHUNK_SIZE");
        if value.is_null() {
            let mut data = VectorData::with_capacity(dtype, n);
            data.extend_placeholders(n);
            return Ok(Vector::from_parts(data, vec![0u64; mask_len(n)], n));
        }
        let vector = Vector::new_const(value, n);
        if vector.dtype() == dtype {
            return Ok(vector);
        }
        // The value's own type differs from the column's (`DEFAULT 1.5` is a
        // DECIMAL(2,1) on a DECIMAL(10,2) column): one row-by-row push decides
        // the coercion, exactly as a per-row fill would, and its result repeats.
        let mut probe = Vector::with_capacity(dtype, 1);
        probe.push_valid(value.clone())?;
        let vector = Vector::new_const(&probe.get(0), n);
        if vector.dtype() != dtype {
            return Err(PinkDbError::Type(format!(
                "type mismatch: cannot fill {dtype:?} column with a {value} default"
            )));
        }
        Ok(vector)
    }

    /// Build a vector directly from raw parts (crate-internal fast paths):
    /// `data` must hold exactly `len` elements; `validity` is normalized to
    /// `mask_len(len)` words (extra words dropped, missing ones read NULL).
    pub(crate) fn from_parts(data: VectorData, mut validity: Vec<u64>, len: usize) -> Vector {
        debug_assert_eq!(data.len(), len);
        debug_assert!(
            data.decimal_width_ok(),
            "a DECIMAL payload of the wrong width"
        );
        validity.resize(mask_len(len), 0);
        Vector {
            data,
            validity,
            len,
        }
    }

    fn ensure_mask(&mut self) {
        let need = mask_len(self.len);
        if self.validity.len() < need {
            self.validity.resize(need, 0);
        }
    }

    fn push_placeholder(&mut self) {
        match &mut self.data {
            VectorData::Boolean(v) => v.push(false),
            VectorData::Int32(v) => v.push(0),
            VectorData::Int64(v) => v.push(0),
            VectorData::Float64(v) => v.push(0.0),
            VectorData::VarChar(v) => v.push(""),
            VectorData::Date32(v) => v.push(0),
            VectorData::TimestampMicros(v) => v.push(0),
            VectorData::Decimal { values, .. } => values.push(0),
            VectorData::Decimal64 { values, .. } => values.push(0),
            VectorData::Interval(v) => v.push(Interval::default()),
            VectorData::Variant(v) => v.push(Arc::new(crate::variant::Variant::Null)),
            VectorData::Array {
                len, values, valid, ..
            } => {
                for _ in 0..*len {
                    values.push_zero();
                }
                if let Some(mask) = valid {
                    let start = values.len() - *len as usize;
                    if start + *len as usize > mask.len() * 8 {
                        mask.resize((start + *len as usize).div_ceil(8), 0);
                    }
                }
            }
            VectorData::Uuid(v) => v.push(0),
            VectorData::Int128(v) => v.push(0),
            VectorData::UInt128(v) => v.push(0),
        }
    }

    fn check_room(&self) -> Result<()> {
        if self.len >= CHUNK_SIZE {
            return Err(PinkDbError::Execution(format!(
                "vector overflow: len {} >= CHUNK_SIZE {CHUNK_SIZE}",
                self.len
            )));
        }
        Ok(())
    }

    pub fn dtype(&self) -> DataType {
        self.data.dtype()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn data(&self) -> &VectorData {
        &self.data
    }

    /// Raw validity bitmask: bit `i` set ⇒ row `i` valid. May span more
    /// words than `mask_len(len)` for preallocated vectors; the extra words
    /// are always zero. Crate-internal fast paths only (`is_valid` is the
    /// checked per-row read).
    pub(crate) fn validity(&self) -> &[u64] {
        &self.validity
    }

    /// The validity bitmask as whole words (bit `i` of word `i / 64` set ⇒
    /// row `i` valid), trimmed to exactly `ceil(len / 64)` words. Bits past
    /// `len` in the last word are unspecified: mask them before use. For
    /// bulk consumers (file encoders) that would otherwise test every row.
    pub fn validity_words(&self) -> &[u64] {
        &self.validity[..mask_len(self.len).min(self.validity.len())]
    }

    /// True when any of the `len` rows is NULL — a word-wise test, not a
    /// per-row scan.
    pub fn has_nulls(&self) -> bool {
        let words = mask_len(self.len);
        for w in 0..words {
            let word = self.validity.get(w).copied().unwrap_or(0);
            let bits = if w + 1 == words && !self.len.is_multiple_of(64) {
                (1u64 << (self.len % 64)) - 1
            } else {
                u64::MAX
            };
            if word & bits != bits {
                return true;
            }
        }
        false
    }

    /// Build a vector from a payload and its validity words (bit `i` set ⇒
    /// row `i` valid) in one step, for bulk producers such as file decoders.
    /// The length is the payload's; missing words read as NULL and bits past
    /// the length are cleared.
    pub fn from_raw(data: VectorData, mut validity: Vec<u64>) -> Vector {
        debug_assert!(
            data.decimal_width_ok(),
            "a DECIMAL payload of the wrong width"
        );
        let len = data.len();
        validity.resize(mask_len(len), 0);
        if !len.is_multiple_of(64)
            && let Some(last) = validity.last_mut()
        {
            *last &= (1u64 << (len % 64)) - 1;
        }
        Vector {
            data,
            validity,
            len,
        }
    }

    /// The bytes this vector holds: payload capacity, validity bitmap, the
    /// buffers its payload points into, and the vector itself.
    ///
    /// Capacity, not length, because that is what is allocated: a decoded
    /// chunk reserves its full width before its rows arrive. A shared payload
    /// (VARCHAR and VARIANT strings, ARRAY element buffers) counts whatever it
    /// points at, so two vectors sharing one buffer each count it (see
    /// [`crate::strings::StrVec::memory_size`]).
    pub fn memory_size(&self) -> usize {
        let payload = match &self.data {
            // `Vec<bool>` is bit-packed: capacity is in bits.
            VectorData::Boolean(v) => v.capacity().div_ceil(8),
            VectorData::Int32(v) | VectorData::Date32(v) => v.capacity() * 4,
            VectorData::Int64(v) | VectorData::TimestampMicros(v) => v.capacity() * 8,
            VectorData::Float64(v) => v.capacity() * 8,
            VectorData::Decimal { values, .. } => values.capacity() * 16,
            VectorData::Decimal64 { values, .. } => values.capacity() * 8,
            VectorData::Interval(v) => v.capacity() * std::mem::size_of::<Interval>(),
            VectorData::Variant(v) => {
                v.capacity() * std::mem::size_of::<Arc<crate::variant::Variant>>()
            }
            VectorData::VarChar(s) => s.memory_size(),
            VectorData::Uuid(v) => v.capacity() * 16,
            VectorData::Int128(v) => v.capacity() * 16,
            VectorData::UInt128(v) => v.capacity() * 16,
            VectorData::Array { values, valid, .. } => {
                values.memory_size() + valid.as_ref().map_or(0, |v| v.capacity())
            }
        };
        payload + self.validity.capacity() * 8 + std::mem::size_of::<Vector>()
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        debug_assert!(idx < self.len);
        bit_get(&self.validity, idx)
    }

    pub fn is_null(&self, idx: usize) -> bool {
        !self.is_valid(idx)
    }

    /// Value at `idx` (clones strings).
    pub fn get(&self, idx: usize) -> Value {
        assert!(idx < self.len, "vector index out of bounds");
        if !self.is_valid(idx) {
            return Value::Null;
        }
        match &self.data {
            VectorData::Boolean(v) => Value::Boolean(v[idx]),
            VectorData::Int32(v) => Value::Integer(v[idx]),
            VectorData::Int64(v) => Value::Bigint(v[idx]),
            VectorData::Float64(v) => Value::Double(v[idx]),
            VectorData::VarChar(v) => Value::Varchar(v[idx].to_string()),
            VectorData::Date32(v) => Value::Date(v[idx]),
            VectorData::TimestampMicros(v) => Value::Timestamp(v[idx]),
            VectorData::Decimal {
                values,
                precision,
                scale,
            } => Value::Decimal {
                value: values[idx],
                precision: *precision,
                scale: *scale,
            },
            VectorData::Decimal64 {
                values,
                precision,
                scale,
            } => Value::Decimal {
                value: i128::from(values[idx]),
                precision: *precision,
                scale: *scale,
            },
            VectorData::Interval(v) => Value::Interval {
                months: v[idx].months,
                days: v[idx].days,
                micros: v[idx].micros,
            },
            VectorData::Variant(v) => Value::Variant(Arc::clone(&v[idx])),
            VectorData::Array {
                elem,
                len,
                values,
                valid,
            } => {
                let n = *len as usize;
                let cell =
                    crate::array::cell_from_slice(*elem, values, valid.as_deref(), idx * n, n)
                        .expect("ARRAY cell slice");
                Value::Array(Arc::new(cell))
            }
            VectorData::Uuid(v) => Value::Uuid(v[idx]),
            VectorData::Int128(v) => Value::Hugeint(v[idx]),
            VectorData::UInt128(v) => Value::Uhugeint(v[idx]),
        }
    }

    /// Borrow one ARRAY row (wave 107b): the element kind, the flat buffer
    /// and the optional element-validity bitmap. `None` for a NULL row or a
    /// non-ARRAY vector. The index build path uses this to avoid materialising
    /// an `ArrayValue` per row.
    pub(crate) fn array_row(&self, idx: usize) -> Option<(ArrayElem, &ArrayElems, Option<&[u8]>)> {
        if idx >= self.len || !self.is_valid(idx) {
            return None;
        }
        match &self.data {
            VectorData::Array {
                elem,
                len,
                values,
                valid,
            } => {
                let n = *len as usize;
                let _ = (idx, n);
                Some((*elem, values, valid.as_deref()))
            }
            _ => None,
        }
    }

    /// Append a NULL.
    pub fn push_null(&mut self) -> Result<()> {
        self.check_room()?;
        self.push_placeholder();
        self.len += 1;
        self.ensure_mask();
        bit_clear(&mut self.validity, self.len - 1);
        debug_assert_eq!(self.data.len(), self.len);
        Ok(())
    }

    /// Append a non-null value with exact type check.
    /// Widening coercion (int → bigint → double) is done by the caller.
    pub fn push_valid(&mut self, value: Value) -> Result<()> {
        if value.is_null() {
            return self.push_null();
        }
        self.check_room()?;
        match (&mut self.data, value) {
            (VectorData::Boolean(v), Value::Boolean(b)) => v.push(b),
            (VectorData::Int32(v), Value::Integer(i)) => v.push(i),
            (VectorData::Int64(v), Value::Bigint(i)) => v.push(i),
            (VectorData::Float64(v), Value::Double(x)) => v.push(x),
            (VectorData::VarChar(v), Value::Varchar(s)) => v.push(&s),
            (VectorData::Date32(v), Value::Date(d)) => v.push(d),
            (VectorData::TimestampMicros(v), Value::Timestamp(us)) => v.push(us),
            (
                VectorData::Decimal { values, scale, .. },
                Value::Decimal {
                    value: v, scale: s, ..
                },
            ) if s == *scale => {
                values.push(v);
            }
            (
                VectorData::Decimal64 { values, scale, .. },
                Value::Decimal {
                    value: v, scale: s, ..
                },
            ) if s == *scale && i64::try_from(v).is_ok() => {
                values.push(v as i64);
            }
            (
                VectorData::Interval(v),
                Value::Interval {
                    months,
                    days,
                    micros,
                },
            ) => v.push(Interval {
                months,
                days,
                micros,
            }),
            (VectorData::Variant(v), Value::Variant(x)) => v.push(x),
            (
                VectorData::Array {
                    elem,
                    len,
                    values,
                    valid,
                },
                Value::Array(x),
            ) => {
                array_push_cell(*elem, *len, values, valid, &x)?;
            }
            (VectorData::Uuid(v), Value::Uuid(bits)) => v.push(bits),
            (VectorData::Int128(v), Value::Hugeint(x)) => v.push(x),
            (VectorData::UInt128(v), Value::Uhugeint(x)) => v.push(x),
            (data, other) => {
                return Err(PinkDbError::Type(format!(
                    "type mismatch: cannot push {other} into {:?} vector",
                    data.dtype()
                )));
            }
        }
        self.len += 1;
        self.ensure_mask();
        bit_set(&mut self.validity, self.len - 1);
        Ok(())
    }

    /// Append a borrowed string to a VARCHAR vector: one `Arc` allocation
    /// copied directly from the slice (no intermediate `String`, unlike
    /// going through `push_valid(Value::Varchar(..))`).
    pub fn push_str(&mut self, s: &str) -> Result<()> {
        if self.dtype() != DataType::Varchar {
            return Err(PinkDbError::Type(format!(
                "type mismatch: cannot push a str into {:?} vector",
                self.dtype()
            )));
        }
        self.check_room()?;
        if let VectorData::VarChar(v) = &mut self.data {
            v.push(s);
        }
        self.len += 1;
        self.ensure_mask();
        bit_set(&mut self.validity, self.len - 1);
        Ok(())
    }

    /// Typed append helpers for crate-internal fast paths: no `Value`
    /// materialization, same dtype checks as [`Self::push_valid`].
    pub(crate) fn push_bool(&mut self, b: bool) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Boolean(v) => v.push(b),
            data => return Err(typed_push_error("a boolean", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    pub(crate) fn push_int32(&mut self, i: i32) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Int32(v) => v.push(i),
            data => return Err(typed_push_error("an integer", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    pub(crate) fn push_int64(&mut self, i: i64) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Int64(v) => v.push(i),
            data => return Err(typed_push_error("a bigint", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    pub(crate) fn push_date(&mut self, days: i32) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Date32(v) => v.push(days),
            data => return Err(typed_push_error("a date", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    pub(crate) fn push_timestamp(&mut self, micros: i64) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::TimestampMicros(v) => v.push(micros),
            data => return Err(typed_push_error("a timestamp", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    pub(crate) fn push_float64(&mut self, x: f64) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Float64(v) => v.push(x),
            data => return Err(typed_push_error("a double", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    /// Typed DECIMAL append: the value's scale must match the column's.
    pub(crate) fn push_decimal(&mut self, v: i128, scale: u8) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Decimal {
                values, scale: s, ..
            } if *s == scale => values.push(v),
            VectorData::Decimal64 {
                values, scale: s, ..
            } if *s == scale && i64::try_from(v).is_ok() => values.push(v as i64),
            data => return Err(typed_push_error("a decimal", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    /// Typed INTERVAL append.
    pub(crate) fn push_interval(&mut self, iv: Interval) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Interval(v) => v.push(iv),
            data => return Err(typed_push_error("an interval", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    /// Typed UUID append.
    pub(crate) fn push_uuid(&mut self, bits: u128) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Uuid(v) => v.push(bits),
            data => return Err(typed_push_error("a uuid", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    /// Typed HUGEINT append.
    pub(crate) fn push_i128(&mut self, x: i128) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Int128(v) => v.push(x),
            data => return Err(typed_push_error("a hugeint", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    /// Typed UHUGEINT append.
    pub(crate) fn push_u128(&mut self, x: u128) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::UInt128(v) => v.push(x),
            data => return Err(typed_push_error("a uhugeint", data.dtype())),
        }
        self.finish_typed_push();
        Ok(())
    }

    /// Shared tail of the typed push helpers: bump `len`, mark the new row valid.
    fn finish_typed_push(&mut self) {
        self.len += 1;
        self.ensure_mask();
        bit_set(&mut self.validity, self.len - 1);
    }

    /// Bulk-append `len` elements starting at `start` from `other` onto `self`
    /// (chunked loads). Types must match; `self` must have room; validity bits
    /// are copied. Infallible: callers validate (see `DataTable::append_chunk`).
    pub fn extend_from(&mut self, other: &Vector, start: usize, len: usize) {
        debug_assert_eq!(self.dtype(), other.dtype());
        debug_assert!(start + len <= other.len);
        debug_assert!(self.len + len <= CHUNK_SIZE);
        match (&mut self.data, &other.data) {
            (VectorData::Boolean(d), VectorData::Boolean(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Int32(d), VectorData::Int32(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Int64(d), VectorData::Int64(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Float64(d), VectorData::Float64(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::VarChar(d), VectorData::VarChar(s)) => {
                // Short strings copy their view; shared long ones keep
                // pointing into their buffer.
                d.extend_from(s, start, len);
            }
            (VectorData::Date32(d), VectorData::Date32(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::TimestampMicros(d), VectorData::TimestampMicros(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Decimal { values: d, .. }, VectorData::Decimal { values: s, .. }) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Decimal64 { values: d, .. }, VectorData::Decimal64 { values: s, .. }) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Interval(d), VectorData::Interval(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Variant(d), VectorData::Variant(s)) => {
                // Variants share their Arc value instead of copying.
                d.extend(s[start..start + len].iter().cloned());
            }
            (VectorData::Uuid(d), VectorData::Uuid(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::Int128(d), VectorData::Int128(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::UInt128(d), VectorData::UInt128(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (
                VectorData::Array {
                    elem,
                    len: elem_len,
                    values: d,
                    valid,
                },
                VectorData::Array {
                    values: s,
                    valid: svalid,
                    ..
                },
            ) => {
                let n = *elem_len as usize;
                let base = d.len();
                match (d, s) {
                    (ArrayElems::Ints(d), ArrayElems::Ints(s)) => {
                        d.extend_from_slice(&s[start * n..(start + len) * n]);
                    }
                    (ArrayElems::Floats(d), ArrayElems::Floats(s)) => {
                        d.extend_from_slice(&s[start * n..(start + len) * n]);
                    }
                    (ArrayElems::Floats32(d), ArrayElems::Floats32(s)) => {
                        d.extend_from_slice(&s[start * n..(start + len) * n]);
                    }
                    (_, _) => unreachable!("extend_from: element kind mismatch"),
                }
                array_bits_append(valid, base, svalid.as_deref(), start * n, len * n);
                let _ = elem;
            }
            _ => unreachable!("extend_from: vector dtype mismatch"),
        }
        let base = self.len;
        self.len += len;
        self.ensure_mask();
        for i in 0..len {
            if other.is_valid(start + i) {
                bit_set(&mut self.validity, base + i);
            } else {
                bit_clear(&mut self.validity, base + i);
            }
        }
    }

    /// Append either NULL or a typed value.
    pub fn push_value(&mut self, value: Value) -> Result<()> {
        if value.is_null() {
            self.push_null()
        } else {
            self.push_valid(value)
        }
    }

    /// Overwrite the value at `idx` (for UPDATE). Type-checked like push;
    /// NULL clears the validity bit, anything else sets it.
    pub fn set_value(&mut self, idx: usize, value: Value) -> Result<()> {
        if idx >= self.len {
            return Err(PinkDbError::Execution(format!(
                "vector index {idx} out of bounds (len {})",
                self.len
            )));
        }
        let is_null = value.is_null();
        match (&mut self.data, value) {
            (VectorData::Boolean(v), Value::Boolean(b)) => v[idx] = b,
            (VectorData::Int32(v), Value::Integer(i)) => v[idx] = i,
            (VectorData::Int64(v), Value::Bigint(i)) => v[idx] = i,
            (VectorData::Float64(v), Value::Double(x)) => v[idx] = x,
            (VectorData::VarChar(v), Value::Varchar(s)) => v.set(idx, &s),
            (VectorData::Date32(v), Value::Date(d)) => v[idx] = d,
            (VectorData::TimestampMicros(v), Value::Timestamp(us)) => v[idx] = us,
            (
                VectorData::Decimal { values, scale, .. },
                Value::Decimal {
                    value: v, scale: s, ..
                },
            ) if s == *scale => {
                values[idx] = v;
            }
            (
                VectorData::Decimal64 { values, scale, .. },
                Value::Decimal {
                    value: v, scale: s, ..
                },
            ) if s == *scale && i64::try_from(v).is_ok() => {
                values[idx] = v as i64;
            }
            (
                VectorData::Interval(v),
                Value::Interval {
                    months,
                    days,
                    micros,
                },
            ) => {
                v[idx] = Interval {
                    months,
                    days,
                    micros,
                }
            }
            (VectorData::Variant(v), Value::Variant(x)) => v[idx] = x,
            (VectorData::Uuid(v), Value::Uuid(bits)) => v[idx] = bits,
            (VectorData::Int128(v), Value::Hugeint(x)) => v[idx] = x,
            (VectorData::UInt128(v), Value::Uhugeint(x)) => v[idx] = x,
            (
                VectorData::Array {
                    elem,
                    len,
                    values,
                    valid,
                },
                Value::Array(x),
            ) => {
                let n = *len as usize;
                if x.elem != *elem || x.len() != n {
                    return Err(PinkDbError::Type(format!(
                        "type mismatch: cannot store {} in a {}[{n}] vector",
                        x.dtype(),
                        elem.name()
                    )));
                }
                match (values, &x.values) {
                    (ArrayElems::Ints(d), ArrayElems::Ints(s)) => {
                        d[idx * n..idx * n + n].copy_from_slice(s)
                    }
                    (ArrayElems::Floats(d), ArrayElems::Floats(s)) => {
                        d[idx * n..idx * n + n].copy_from_slice(s)
                    }
                    (ArrayElems::Floats32(d), ArrayElems::Floats32(s)) => {
                        d[idx * n..idx * n + n].copy_from_slice(s)
                    }
                    (_, _) => unreachable!("element kind checked above"),
                }
                if x.has_null_elems() || valid.is_some() {
                    let total = self.len * n;
                    let need = total.div_ceil(8);
                    let mask = valid.get_or_insert_with(|| {
                        let mut v = vec![0xFFu8; need];
                        if !total.is_multiple_of(8) {
                            v[need - 1] = (1u8 << (total % 8)) - 1;
                        }
                        v
                    });
                    for i in 0..n {
                        let at = idx * n + i;
                        if x.is_valid(i) {
                            mask[at / 8] |= 1u8 << (at % 8);
                        } else {
                            mask[at / 8] &= !(1u8 << (at % 8));
                        }
                    }
                }
            }
            (_, Value::Null) => {} // placeholder stays; bit cleared below
            (data, other) => {
                return Err(PinkDbError::Type(format!(
                    "type mismatch: cannot store {other} in {:?} vector",
                    data.dtype()
                )));
            }
        }
        if is_null {
            bit_clear(&mut self.validity, idx);
        } else {
            bit_set(&mut self.validity, idx);
        }
        Ok(())
    }

    /// Append this row's encoding to `buf` WITHOUT materializing a `Value`
    /// (hot path for GROUP BY / JOIN / DISTINCT keys). Byte-identical to
    /// encoding [`Value`]s, including the NULL tag.
    pub fn encode_into(&self, buf: &mut Vec<u8>, idx: usize) {
        debug_assert!(idx < self.len);
        if !self.is_valid(idx) {
            buf.push(0);
            return;
        }
        match &self.data {
            VectorData::Boolean(v) => {
                buf.push(1);
                buf.push(v[idx] as u8);
            }
            VectorData::Int32(v) => {
                buf.push(2);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
            VectorData::Int64(v) => {
                buf.push(3);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
            VectorData::Float64(v) => {
                buf.push(4);
                // Normalize -0.0 → 0.0 and all NaNs to one encoding (same as
                // `aggregate::encode_value`) so GROUP BY / DISTINCT / JOIN
                // agree with `=` and window partitioning.
                let x = v[idx];
                let norm = if x.is_nan() {
                    f64::NAN
                } else if x == 0.0 {
                    0.0
                } else {
                    x
                };
                buf.extend_from_slice(&norm.to_bits().to_le_bytes());
            }
            VectorData::VarChar(v) => {
                let s: &str = &v[idx];
                buf.push(5);
                buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            VectorData::Date32(v) => {
                buf.push(6);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
            VectorData::TimestampMicros(v) => {
                buf.push(7);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
            // DECIMAL keys encode the scale too: the tag byte keeps them out
            // of other domains, and the scale keeps two different types'
            // keys apart (mixed-scale keys are coerced to a common type
            // before hashing, so equality and grouping agree).
            VectorData::Decimal { values, scale, .. } => {
                buf.push(8);
                buf.push(*scale);
                buf.extend_from_slice(&values[idx].to_le_bytes());
            }
            // The 64-bit payload encodes as its 128-bit value: one key per
            // DECIMAL value, whatever its physical width (wave 142).
            VectorData::Decimal64 { values, scale, .. } => {
                buf.push(8);
                buf.push(*scale);
                buf.extend_from_slice(&i128::from(values[idx]).to_le_bytes());
            }
            VectorData::Interval(v) => {
                buf.push(9);
                // Encode the comparison-normalized key (months → 30 days, days
                // → micros) so GROUP BY / DISTINCT / JOIN agree with `=` and
                // the ordering, like the float -0.0/NaN path above.
                let iv = &v[idx];
                let key = crate::types::interval_key(iv.months, iv.days, iv.micros);
                buf.extend_from_slice(&key.to_le_bytes());
            }
            // VARIANT: the canonical hash key (see `variant::hash_key`), which
            // is byte-equal exactly when `compare_values` reports equality —
            // `1`, `1.0` and a DECIMAL 1.0 land in one bucket.
            VectorData::Variant(v) => {
                buf.push(10);
                crate::variant::hash_key(&v[idx], buf);
            }
            // ARRAY: element kind, length, NULL-element bitmap and the
            // normalized element values (see `array::encode_key`), so two
            // arrays hash together exactly when `compare_values` says equal.
            VectorData::Array {
                elem,
                len,
                values,
                valid,
            } => {
                buf.push(11);
                let n = *len as usize;
                crate::array::ArrayValue::encode_key_range(
                    *elem,
                    values,
                    valid.as_deref(),
                    idx * n,
                    n,
                    buf,
                );
            }
            VectorData::Uuid(v) => {
                buf.push(12);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
            VectorData::Int128(v) => {
                buf.push(13);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
            VectorData::UInt128(v) => {
                buf.push(14);
                buf.extend_from_slice(&v[idx].to_le_bytes());
            }
        }
    }
    /// Overwrite slot `dst[k]` with `src`'s row `src_rows[k]`, which must be
    /// valid, keeping `self`'s length: a result vector is patched where an
    /// earlier argument left it NULL (COALESCE) instead of being rebuilt.
    /// The two vectors must have the same layout.
    pub(crate) fn set_rows_from(
        &mut self,
        dst: &[usize],
        src: &Vector,
        src_rows: &[usize],
    ) -> Result<()> {
        debug_assert_eq!(dst.len(), src_rows.len());
        debug_assert!(
            src_rows.iter().all(|&s| src.is_valid(s)),
            "set_rows_from writes valid source rows"
        );
        if matches!(src.data(), VectorData::Array { .. }) {
            // A fixed-width ARRAY cell cannot be written in place; the rare
            // path pays a `Value` per cell.
            for (&d, &s) in dst.iter().zip(src_rows) {
                let value = src.get(s);
                self.set_value(d, value)?;
            }
            return Ok(());
        }
        macro_rules! copy {
            ($into:expr, $from:expr) => {
                for (&d, &s) in dst.iter().zip(src_rows) {
                    $into[d] = $from[s];
                }
            };
        }
        match (&mut self.data, src.data()) {
            (VectorData::Boolean(d), VectorData::Boolean(s)) => copy!(d, s),
            (VectorData::Int32(d), VectorData::Int32(s)) => copy!(d, s),
            (VectorData::Int64(d), VectorData::Int64(s)) => copy!(d, s),
            (VectorData::Float64(d), VectorData::Float64(s)) => copy!(d, s),
            (VectorData::Date32(d), VectorData::Date32(s)) => copy!(d, s),
            (VectorData::TimestampMicros(d), VectorData::TimestampMicros(s)) => copy!(d, s),
            (VectorData::Decimal { values: d, .. }, VectorData::Decimal { values: s, .. }) => {
                copy!(d, s)
            }
            (VectorData::Decimal64 { values: d, .. }, VectorData::Decimal64 { values: s, .. }) => {
                copy!(d, s)
            }
            (VectorData::Interval(d), VectorData::Interval(s)) => copy!(d, s),
            (VectorData::Uuid(d), VectorData::Uuid(s)) => copy!(d, s),
            (VectorData::Int128(d), VectorData::Int128(s)) => copy!(d, s),
            (VectorData::UInt128(d), VectorData::UInt128(s)) => copy!(d, s),
            (VectorData::VarChar(d), VectorData::VarChar(s)) => {
                for (&k, &j) in dst.iter().zip(src_rows) {
                    d.set(k, &s[j]);
                }
            }
            (VectorData::Variant(d), VectorData::Variant(s)) => {
                for (&k, &j) in dst.iter().zip(src_rows) {
                    d[k] = Arc::clone(&s[j]);
                }
            }
            _ => {
                return Err(PinkDbError::Execution(
                    "set_rows_from needs matching vector types".to_string(),
                ));
            }
        }
        for &d in dst {
            bit_set(&mut self.validity, d);
        }
        Ok(())
    }

    /// Copy element `idx` from `other` into `self` (for UPDATE-adjacent
    /// bulk moves: filter/project/sort/join/distinct outputs). Types must
    /// match; strings share the `Arc` (no allocation).
    pub fn push_from(&mut self, other: &Vector, idx: usize) -> Result<()> {
        if self.dtype() != other.dtype() {
            return Err(PinkDbError::Execution(
                "push_from needs matching vector types".to_string(),
            ));
        }
        if self.len >= CHUNK_SIZE {
            return Err(PinkDbError::Execution(format!(
                "vector overflow: len {} >= CHUNK_SIZE {CHUNK_SIZE}",
                self.len
            )));
        }
        match (&mut self.data, &other.data) {
            (VectorData::Boolean(d), VectorData::Boolean(s)) => d.push(s[idx]),
            (VectorData::Int32(d), VectorData::Int32(s)) => d.push(s[idx]),
            (VectorData::Int64(d), VectorData::Int64(s)) => d.push(s[idx]),
            (VectorData::Float64(d), VectorData::Float64(s)) => d.push(s[idx]),
            (VectorData::VarChar(d), VectorData::VarChar(s)) => d.push_from(s, idx),
            (VectorData::Date32(d), VectorData::Date32(s)) => d.push(s[idx]),
            (VectorData::TimestampMicros(d), VectorData::TimestampMicros(s)) => d.push(s[idx]),
            (VectorData::Decimal { values: d, .. }, VectorData::Decimal { values: s, .. }) => {
                d.push(s[idx])
            }
            (VectorData::Decimal64 { values: d, .. }, VectorData::Decimal64 { values: s, .. }) => {
                d.push(s[idx])
            }
            (VectorData::Interval(d), VectorData::Interval(s)) => d.push(s[idx]),
            (VectorData::Variant(d), VectorData::Variant(s)) => d.push(Arc::clone(&s[idx])),
            (VectorData::Uuid(d), VectorData::Uuid(s)) => d.push(s[idx]),
            (VectorData::Int128(d), VectorData::Int128(s)) => d.push(s[idx]),
            (VectorData::UInt128(d), VectorData::UInt128(s)) => d.push(s[idx]),
            (
                VectorData::Array {
                    elem,
                    len,
                    values,
                    valid,
                },
                VectorData::Array {
                    elem: selem,
                    len: slen,
                    values: svalues,
                    valid: svalid,
                },
            ) => {
                if elem != selem || len != slen {
                    return Err(PinkDbError::Execution(
                        "push_from needs matching vector types".to_string(),
                    ));
                }
                let n = *len as usize;
                let start = values.len();
                match (values, svalues) {
                    (ArrayElems::Ints(d), ArrayElems::Ints(s)) => {
                        d.extend_from_slice(&s[idx * n..idx * n + n]);
                    }
                    (ArrayElems::Floats(d), ArrayElems::Floats(s)) => {
                        d.extend_from_slice(&s[idx * n..idx * n + n]);
                    }
                    (ArrayElems::Floats32(d), ArrayElems::Floats32(s)) => {
                        d.extend_from_slice(&s[idx * n..idx * n + n]);
                    }
                    (_, _) => unreachable!("element kinds checked above"),
                }
                array_bits_append(valid, start, svalid.as_deref(), idx * n, n);
            }
            _ => {
                return Err(PinkDbError::Execution(
                    "push_from needs matching vector types".to_string(),
                ));
            }
        }
        self.len += 1;
        self.ensure_mask();
        if other.is_valid(idx) {
            bit_set(&mut self.validity, self.len - 1);
        } else {
            bit_clear(&mut self.validity, self.len - 1);
        }
        Ok(())
    }
    /// Build a new vector with rows in `selection` order. Used by Filter.
    pub fn gather(&self, selection: &[usize]) -> Vector {
        self.select_rows(selection)
    }

    /// Build a new vector with the rows of a selection (ascending `u32`
    /// indices into this vector, at most `CHUNK_SIZE` of them): the
    /// selection-vector form of [`Vector::gather`], for the boundaries that
    /// must materialize a filtered chunk.
    pub fn take(&self, selection: &[u32]) -> Vector {
        self.select_rows(selection)
    }

    /// One output vector gathered across several source vectors of type
    /// `dtype` (a hash join's build columns, one per build chunk): row `k` is
    /// `sources[c][r]` for `picks[k] = (c, r)`, or NULL when `c` is
    /// [`GATHER_NULL`] (an outer join's unmatched row). Copied payload by
    /// payload; ARRAY columns and an empty `sources` take the per-row path.
    pub(crate) fn gather_located(
        dtype: DataType,
        sources: &[&Vector],
        picks: &[(u32, u32)],
    ) -> Vector {
        let n = picks.len();
        let per_row = || {
            let mut out = Vector::with_capacity(dtype, n);
            for &(c, r) in picks {
                if c == GATHER_NULL {
                    out.push_null().expect("gather push_null cannot fail");
                } else {
                    out.push_from(sources[c as usize], r as usize)
                        .expect("gather push_from cannot fail");
                }
            }
            out
        };
        if sources.is_empty() {
            return per_row();
        }
        macro_rules! gather {
            ($variant:ident, $default:expr, $copy:expr) => {
                picks
                    .iter()
                    .map(|&(c, r)| {
                        if c == GATHER_NULL {
                            return $default;
                        }
                        match sources[c as usize].data() {
                            VectorData::$variant(v) => $copy(&v[r as usize]),
                            _ => unreachable!("gather sources of mixed types"),
                        }
                    })
                    .collect()
            };
        }
        let data = match sources[0].data() {
            VectorData::Boolean(_) => VectorData::Boolean(gather!(Boolean, false, |x: &bool| *x)),
            VectorData::Int32(_) => VectorData::Int32(gather!(Int32, 0, |x: &i32| *x)),
            VectorData::Int64(_) => VectorData::Int64(gather!(Int64, 0, |x: &i64| *x)),
            VectorData::Float64(_) => VectorData::Float64(gather!(Float64, 0.0, |x: &f64| *x)),
            VectorData::VarChar(_) => {
                let strs: Vec<&crate::strings::StrVec> = sources
                    .iter()
                    .map(|s| match s.data() {
                        VectorData::VarChar(v) => v,
                        _ => unreachable!("gather sources of mixed types"),
                    })
                    .collect();
                VectorData::VarChar(crate::strings::StrVec::gather(&strs, picks, GATHER_NULL))
            }
            VectorData::Date32(_) => VectorData::Date32(gather!(Date32, 0, |x: &i32| *x)),
            VectorData::TimestampMicros(_) => {
                VectorData::TimestampMicros(gather!(TimestampMicros, 0, |x: &i64| *x))
            }
            VectorData::Decimal {
                precision, scale, ..
            } => VectorData::Decimal {
                values: picks
                    .iter()
                    .map(|&(c, r)| {
                        if c == GATHER_NULL {
                            return 0;
                        }
                        match sources[c as usize].data() {
                            VectorData::Decimal { values, .. } => values[r as usize],
                            _ => unreachable!("gather sources of mixed types"),
                        }
                    })
                    .collect(),
                precision: *precision,
                scale: *scale,
            },
            VectorData::Decimal64 {
                precision, scale, ..
            } => VectorData::Decimal64 {
                values: picks
                    .iter()
                    .map(|&(c, r)| {
                        if c == GATHER_NULL {
                            return 0;
                        }
                        match sources[c as usize].data() {
                            VectorData::Decimal64 { values, .. } => values[r as usize],
                            _ => unreachable!("gather sources of mixed types"),
                        }
                    })
                    .collect(),
                precision: *precision,
                scale: *scale,
            },
            VectorData::Interval(_) => {
                VectorData::Interval(gather!(Interval, Interval::default(), |x: &Interval| *x))
            }
            VectorData::Variant(_) => VectorData::Variant(gather!(
                Variant,
                Arc::new(crate::variant::Variant::Null),
                |x: &Arc<crate::variant::Variant>| Arc::clone(x)
            )),
            VectorData::Uuid(_) => VectorData::Uuid(gather!(Uuid, 0, |x: &u128| *x)),
            VectorData::Int128(_) => VectorData::Int128(gather!(Int128, 0, |x: &i128| *x)),
            VectorData::UInt128(_) => VectorData::UInt128(gather!(UInt128, 0, |x: &u128| *x)),
            VectorData::Array { .. } => return per_row(),
        };
        let mut validity = vec![0u64; mask_len(n)];
        for (o, &(c, r)) in picks.iter().enumerate() {
            if c != GATHER_NULL && bit_get(&sources[c as usize].validity, r as usize) {
                validity[o / 64] |= 1 << (o % 64);
            }
        }
        Vector::from_parts(data, validity, n)
    }

    /// A hash join's build column prepared once per join: see
    /// [`GatherColumn`]. [`Vector::gather_located`] stays the one-off form.
    pub(crate) fn gather_column<'a>(sources: &[&'a Vector]) -> GatherColumn<'a> {
        GatherColumn::new(sources)
    }

    /// The rows at `selection` (any order, repeats allowed), copied payload
    /// by payload instead of one `push_from` per row; strings and variants
    /// share their `Arc`s. ARRAY columns, whose elements and element bitmap
    /// are row-major, keep the per-row path.
    fn select_rows<I: RowIndex>(&self, selection: &[I]) -> Vector {
        let n = selection.len();
        let pick = |i: &I| {
            let i = i.index();
            debug_assert!(i < self.len);
            i
        };
        let data = match &self.data {
            VectorData::Boolean(v) => {
                VectorData::Boolean(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::Int32(v) => {
                VectorData::Int32(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::Int64(v) => {
                VectorData::Int64(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::Float64(v) => {
                VectorData::Float64(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::VarChar(v) => VectorData::VarChar(v.take(selection.iter().map(pick))),
            VectorData::Date32(v) => {
                VectorData::Date32(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::TimestampMicros(v) => {
                VectorData::TimestampMicros(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::Decimal {
                values,
                precision,
                scale,
            } => VectorData::Decimal {
                values: selection.iter().map(|i| values[pick(i)]).collect(),
                precision: *precision,
                scale: *scale,
            },
            VectorData::Decimal64 {
                values,
                precision,
                scale,
            } => VectorData::Decimal64 {
                values: selection.iter().map(|i| values[pick(i)]).collect(),
                precision: *precision,
                scale: *scale,
            },
            VectorData::Interval(v) => {
                VectorData::Interval(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::Variant(v) => {
                VectorData::Variant(selection.iter().map(|i| Arc::clone(&v[pick(i)])).collect())
            }
            VectorData::Uuid(v) => VectorData::Uuid(selection.iter().map(|i| v[pick(i)]).collect()),
            VectorData::Int128(v) => {
                VectorData::Int128(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::UInt128(v) => {
                VectorData::UInt128(selection.iter().map(|i| v[pick(i)]).collect())
            }
            VectorData::Array { .. } => {
                let mut out = Vector::with_capacity(self.dtype(), n);
                for i in selection {
                    out.push_from(self, pick(i))
                        .expect("take push_from cannot fail");
                }
                return out;
            }
        };
        let mut validity = vec![0u64; mask_len(n)];
        for (o, i) in selection.iter().enumerate() {
            if bit_get(&self.validity, i.index()) {
                validity[o / 64] |= 1 << (o % 64);
            }
        }
        Vector::from_parts(data, validity, n)
    }

    /// Count of NULLs (linear scan; fine for ≤2048 rows).
    pub fn null_count(&self) -> usize {
        (0..self.len).filter(|&i| self.is_null(i)).count()
    }
}

/// A horizontal slice of columns, all of equal length.
///
/// [`DataChunk::projected`] builds a chunk whose columns may be *absent*: a
/// zero-length placeholder stands in for a column a projection pushdown did
/// not decode. Only projection-aware producers create such chunks; every
/// reader goes through [`DataChunk::expect_column`], which rejects a
/// placeholder instead of returning NULLs.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataChunk {
    /// Reference-counted columns: scans and pass-throughs share storage
    /// instead of cloning megabytes per query.
    pub columns: Vec<Arc<Vector>>,
    pub num_rows: usize,
    /// This chunk's position in the operator that produced it: `exec::eval`
    /// stamps it after every operator, so it is the position in the consumer's
    /// input order, never the worker that happened to process the chunk.
    /// Volatile functions (`uuid()`, `random()`) derive their per-row values
    /// from it, which is what makes them identical on WAL replay.
    pub ordinal: usize,
}

impl DataChunk {
    pub fn new(columns: Vec<Vector>) -> Result<Self> {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        for c in &columns {
            if c.len() != num_rows {
                return Err(PinkDbError::Execution(format!(
                    "DataChunk column length mismatch: expected {num_rows}, got {}",
                    c.len()
                )));
            }
        }
        Ok(Self {
            columns: columns.into_iter().map(Arc::new).collect(),
            num_rows,
            ordinal: 0,
        })
    }

    /// Build from pre-shared columns (zero-copy scan path).
    pub fn from_arcs(columns: Vec<Arc<Vector>>) -> Option<Self> {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        if columns.iter().any(|c| c.len() != num_rows) {
            return None;
        }
        Some(Self {
            columns,
            num_rows,
            ordinal: 0,
        })
    }

    /// Build a chunk that may carry *absent* columns: a zero-length
    /// placeholder (`len() == 0`) for a column a projection pushdown did not
    /// decode; `num_rows` is authoritative here. The projection-aware shapes
    /// only (`Filter`/`Sort`/`Limit` rebuilds and a projected scan — an
    /// attached lake's decode, a host table source's scan, or an in-memory
    /// table's `exec::project_scan_chunks`) create these; operators that read
    /// columns use `expect_column`.
    ///
    /// Public because it is the scan contract a
    /// [`TableSource`](crate::table_source::TableSource) returns: full-width
    /// columns in the bound schema's order, with the columns its projection
    /// did not decode as placeholders.
    pub fn projected(columns: Vec<Arc<Vector>>, num_rows: usize) -> Self {
        debug_assert!(
            columns.iter().all(|c| c.len() == num_rows || c.is_empty()),
            "projected chunks hold present columns (num_rows long) and \
             zero-length absent placeholders"
        );
        Self {
            columns,
            num_rows,
            ordinal: 0,
        }
    }

    /// True when column `index` is present (exactly `num_rows` values
    /// long). An absent placeholder column is zero-length.
    #[inline]
    pub(crate) fn is_present(&self, index: usize) -> bool {
        self.columns
            .get(index)
            .is_some_and(|c| c.len() == self.num_rows)
    }

    /// The column at `index`, for readers that must never silently see an
    /// absent projection placeholder. Reading a placeholder is an internal
    /// error (the referenced-columns analysis said nothing reads it), never
    /// a NULL value.
    #[inline]
    pub(crate) fn expect_column(&self, index: usize) -> Result<&Arc<Vector>> {
        let Some(col) = self.columns.get(index) else {
            return Err(PinkDbError::Execution(format!(
                "column {index} out of range (chunk has {} columns)",
                self.columns.len()
            )));
        };
        if col.len() == self.num_rows {
            Ok(col)
        } else {
            Err(PinkDbError::Execution(format!(
                "internal error: scan projection missed a reference to \
                 column {index}"
            )))
        }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            num_rows: 0,
            ordinal: 0,
        }
    }

    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn column(&self, idx: usize) -> &Vector {
        &self.columns[idx]
    }

    /// Apply a selection vector to every column. Absent placeholder columns
    /// are carried through unchanged (shared, still zero-length): nothing
    /// reads them, and gathering them would materialize NULLs for rows the
    /// query never reads.
    pub fn gather(&self, selection: &[usize]) -> DataChunk {
        let columns = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if self.is_present(i) {
                    Arc::new(c.gather(selection))
                } else {
                    Arc::clone(c)
                }
            })
            .collect();
        DataChunk {
            columns,
            num_rows: selection.len(),
            ordinal: self.ordinal,
        }
    }

    /// [`DataChunk::gather`] for a selection vector: materializes the rows
    /// `selection` names, placeholders carried through as in `gather`.
    pub fn take(&self, selection: &[u32]) -> DataChunk {
        let columns = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if self.is_present(i) {
                    Arc::new(c.take(selection))
                } else {
                    Arc::clone(c)
                }
            })
            .collect();
        DataChunk {
            columns,
            num_rows: selection.len(),
            ordinal: self.ordinal,
        }
    }
}

/// One hash-join build column, resolved once per join (wave 141a).
///
/// A join's output gathers a build column at `(chunk, row)` locations for
/// every match. [`Vector::gather_located`] does that per call, matching each
/// pick's source payload by type, which is a branch per row per column.
/// `GatherColumn` resolves every build chunk's payload slice, validity mask
/// and the column's NULL-freedom once, so a gather is an indexed copy over the
/// picks only — never a pass over all build chunks, which outnumber the
/// picks of one output chunk on a large build side.
pub(crate) struct GatherColumn<'a> {
    sources: Vec<&'a Vector>,
    typed: GatherSlices<'a>,
    masks: Vec<&'a [u64]>,
    has_nulls: bool,
}

enum GatherSlices<'a> {
    Boolean(Vec<&'a [bool]>),
    Int32(Vec<&'a [i32]>),
    Int64(Vec<&'a [i64]>),
    Float64(Vec<&'a [f64]>),
    Date32(Vec<&'a [i32]>),
    TimestampMicros(Vec<&'a [i64]>),
    Decimal(Vec<&'a [i128]>, u8, u8),
    Decimal64(Vec<&'a [i64]>, u8, u8),
    Interval(Vec<&'a [Interval]>),
    Uuid(Vec<&'a [u128]>),
    Int128(Vec<&'a [i128]>),
    UInt128(Vec<&'a [u128]>),
    VarChar(Vec<&'a crate::strings::StrVec>),
    /// VARIANT, ARRAY, or no sources: [`Vector::gather_located`]'s own path.
    Other,
}

impl<'a> GatherColumn<'a> {
    fn new(sources: &[&'a Vector]) -> Self {
        macro_rules! slices {
            ($variant:ident) => {
                sources
                    .iter()
                    .map(|v| match v.data() {
                        VectorData::$variant(x) => &x[..],
                        _ => unreachable!("gather sources of mixed types"),
                    })
                    .collect()
            };
        }
        let typed = match sources.first().map(|v| v.data()) {
            Some(VectorData::Boolean(_)) => GatherSlices::Boolean(slices!(Boolean)),
            Some(VectorData::Int32(_)) => GatherSlices::Int32(slices!(Int32)),
            Some(VectorData::Int64(_)) => GatherSlices::Int64(slices!(Int64)),
            Some(VectorData::Float64(_)) => GatherSlices::Float64(slices!(Float64)),
            Some(VectorData::Date32(_)) => GatherSlices::Date32(slices!(Date32)),
            Some(VectorData::TimestampMicros(_)) => {
                GatherSlices::TimestampMicros(slices!(TimestampMicros))
            }
            Some(VectorData::Decimal {
                precision, scale, ..
            }) => GatherSlices::Decimal(
                sources
                    .iter()
                    .map(|v| match v.data() {
                        VectorData::Decimal { values, .. } => &values[..],
                        _ => unreachable!("gather sources of mixed types"),
                    })
                    .collect(),
                *precision,
                *scale,
            ),
            Some(VectorData::Decimal64 {
                precision, scale, ..
            }) => GatherSlices::Decimal64(
                sources
                    .iter()
                    .map(|v| match v.data() {
                        VectorData::Decimal64 { values, .. } => &values[..],
                        _ => unreachable!("gather sources of mixed types"),
                    })
                    .collect(),
                *precision,
                *scale,
            ),
            Some(VectorData::Interval(_)) => GatherSlices::Interval(slices!(Interval)),
            Some(VectorData::Uuid(_)) => GatherSlices::Uuid(slices!(Uuid)),
            Some(VectorData::Int128(_)) => GatherSlices::Int128(slices!(Int128)),
            Some(VectorData::UInt128(_)) => GatherSlices::UInt128(slices!(UInt128)),
            Some(VectorData::VarChar(_)) => GatherSlices::VarChar(
                sources
                    .iter()
                    .map(|v| match v.data() {
                        VectorData::VarChar(x) => x,
                        _ => unreachable!("gather sources of mixed types"),
                    })
                    .collect(),
            ),
            _ => GatherSlices::Other,
        };
        GatherColumn {
            sources: sources.to_vec(),
            typed,
            masks: sources.iter().map(|v| &v.validity[..]).collect(),
            has_nulls: sources.iter().any(|v| v.has_nulls()),
        }
    }

    /// The build rows at `picks` (`(chunk, row)`, or [`GATHER_NULL`] for an
    /// outer join's unmatched row): the same vector
    /// [`Vector::gather_located`] returns.
    pub(crate) fn gather(&self, dtype: DataType, picks: &[(u32, u32)]) -> Vector {
        fn copy<T: Copy>(slices: &[&[T]], picks: &[(u32, u32)], default: T) -> Vec<T> {
            picks
                .iter()
                .map(|&(c, r)| {
                    if c == GATHER_NULL {
                        default
                    } else {
                        slices[c as usize][r as usize]
                    }
                })
                .collect()
        }
        let data = match &self.typed {
            GatherSlices::Boolean(s) => VectorData::Boolean(copy(s, picks, false)),
            GatherSlices::Int32(s) => VectorData::Int32(copy(s, picks, 0)),
            GatherSlices::Int64(s) => VectorData::Int64(copy(s, picks, 0)),
            GatherSlices::Float64(s) => VectorData::Float64(copy(s, picks, 0.0)),
            GatherSlices::Date32(s) => VectorData::Date32(copy(s, picks, 0)),
            GatherSlices::TimestampMicros(s) => VectorData::TimestampMicros(copy(s, picks, 0)),
            GatherSlices::Decimal(s, precision, scale) => VectorData::Decimal {
                values: copy(s, picks, 0),
                precision: *precision,
                scale: *scale,
            },
            GatherSlices::Decimal64(s, precision, scale) => VectorData::Decimal64 {
                values: copy(s, picks, 0),
                precision: *precision,
                scale: *scale,
            },
            GatherSlices::Interval(s) => VectorData::Interval(copy(s, picks, Interval::default())),
            GatherSlices::Uuid(s) => VectorData::Uuid(copy(s, picks, 0)),
            GatherSlices::Int128(s) => VectorData::Int128(copy(s, picks, 0)),
            GatherSlices::UInt128(s) => VectorData::UInt128(copy(s, picks, 0)),
            GatherSlices::VarChar(s) => {
                VectorData::VarChar(crate::strings::StrVec::gather(s, picks, GATHER_NULL))
            }
            GatherSlices::Other => return Vector::gather_located(dtype, &self.sources, picks),
        };
        let n = picks.len();
        if !self.has_nulls && !picks.iter().any(|&(c, _)| c == GATHER_NULL) {
            return Vector::from_parts(data, all_valid_mask(n), n);
        }
        let mut validity = vec![0u64; mask_len(n)];
        for (o, &(c, r)) in picks.iter().enumerate() {
            if c != GATHER_NULL && bit_get(self.masks[c as usize], r as usize) {
                validity[o / 64] |= 1 << (o % 64);
            }
        }
        Vector::from_parts(data, validity, n)
    }
}

/// A validity mask with the first `len` bits set and the tail bits clear.
fn all_valid_mask(len: usize) -> Vec<u64> {
    let mut mask = vec![u64::MAX; mask_len(len)];
    if !len.is_multiple_of(64)
        && let Some(last) = mask.last_mut()
    {
        *last = (1u64 << (len % 64)) - 1;
    }
    mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_size_counts_payload_validity_and_string_storage() {
        let base = std::mem::size_of::<Vector>();
        // Fixed width: the payload capacity, not the length, plus the validity
        // words the vector holds.
        let mut fixed = Vector::with_capacity(DataType::Bigint, 100);
        for i in 0..100 {
            fixed.push_valid(Value::Bigint(i)).unwrap();
        }
        assert_eq!(fixed.memory_size(), 100 * 8 + 2 * 8 + base);
        // VARCHAR: a view per row, validity, and — for strings too long to
        // inline — the buffer they share, counted once.
        let inline = Vector::from_raw(
            VectorData::VarChar(crate::strings::StrVec::repeat("abc", 10)),
            vec![u64::MAX; mask_len(10)],
        );
        assert_eq!(
            inline.memory_size(),
            10 * std::mem::size_of::<crate::strings::StrView>() + 8 + base
        );
        let heap = Vector::from_raw(
            VectorData::VarChar(crate::strings::StrVec::repeat("0123456789abcdefghij", 4)),
            vec![u64::MAX; mask_len(4)],
        );
        assert_eq!(
            heap.memory_size(),
            4 * std::mem::size_of::<crate::strings::StrView>()
                + std::mem::size_of::<Arc<str>>()
                + 20
                + 8
                + base
        );
    }

    #[test]
    fn push_get_and_nulls() {
        let mut v = Vector::new_empty(DataType::Integer);
        v.push_valid(Value::Integer(1)).unwrap();
        v.push_null().unwrap();
        v.push_value(Value::Null).unwrap();
        v.push_valid(Value::Integer(3)).unwrap();
        assert_eq!(v.len(), 4);
        assert_eq!(v.get(0), Value::Integer(1));
        assert_eq!(v.get(1), Value::Null);
        assert_eq!(v.get(2), Value::Null);
        assert_eq!(v.get(3), Value::Integer(3));
        assert!(v.is_null(1));
        assert!(v.is_valid(0));
        assert_eq!(v.null_count(), 2);
    }

    #[test]
    fn push_str_shares_one_allocation() {
        let mut v = Vector::new_empty(DataType::Varchar);
        v.push_str("a").unwrap();
        v.push_str("bb").unwrap();
        assert_eq!(v.get(0), Value::Varchar("a".to_string()));
        assert_eq!(v.get(1), Value::Varchar("bb".to_string()));
        assert!(Vector::new_empty(DataType::Integer).push_str("x").is_err());
    }

    #[test]
    fn extend_from_copies_ranges_and_validity() {
        let mut src = Vector::new_empty(DataType::Varchar);
        src.push_str("a").unwrap();
        src.push_null().unwrap();
        src.push_str("c").unwrap();
        let mut dst = Vector::new_empty(DataType::Varchar);
        dst.extend_from(&src, 1, 2);
        assert_eq!(dst.len(), 2);
        assert!(dst.is_null(0));
        assert_eq!(dst.get(1), Value::Varchar("c".to_string()));
        // Scalar range copy keeps nulls too.
        let mut ints = Vector::new_empty(DataType::Integer);
        ints.push_valid(Value::Integer(7)).unwrap();
        ints.push_null().unwrap();
        let mut copy = Vector::new_empty(DataType::Integer);
        copy.extend_from(&ints, 0, 2);
        assert_eq!(copy.get(0), Value::Integer(7));
        assert!(copy.is_null(1));
    }

    #[test]
    fn push_mismatch_errors() {
        let mut v = Vector::new_empty(DataType::Varchar);
        let err = v.push_valid(Value::Integer(1)).unwrap_err();
        assert!(matches!(err, PinkDbError::Type(_)));
    }

    #[test]
    fn hugeint_vectors_push_get_and_copy_all_128_bits() {
        let mut v = Vector::new_empty(DataType::Hugeint);
        v.push_i128(i128::MIN).unwrap();
        v.push_null().unwrap();
        v.push_valid(Value::Hugeint(1)).unwrap();
        assert_eq!(v.dtype(), DataType::Hugeint);
        assert_eq!(v.len(), 3);
        assert_eq!(v.get(0), Value::Hugeint(i128::MIN));
        assert_eq!(v.get(1), Value::Null);
        assert_eq!(v.get(2), Value::Hugeint(1));
        // Key bytes: tag 13 then the full little-endian payload.
        let mut buf = Vec::new();
        v.encode_into(&mut buf, 2);
        assert_eq!(buf, [13, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        // Copying must keep the high half, not just the low 64 bits.
        let mut copy = Vector::new_empty(DataType::Hugeint);
        copy.extend_from(&v, 0, 3);
        assert_eq!(copy.get(0), Value::Hugeint(i128::MIN));
        assert!(copy.is_null(1));
        assert_eq!(copy.gather(&[2, 0]).get(1), Value::Hugeint(i128::MIN));
        // A constant UHUGEINT vector carries the top bit too.
        let wide = Vector::new_const(&Value::Uhugeint(u128::MAX), 2);
        assert_eq!(wide.dtype(), DataType::Uhugeint);
        assert_eq!(wide.get(1), Value::Uhugeint(u128::MAX));
    }

    #[test]
    fn gather_selection() {
        let mut v = Vector::new_empty(DataType::Bigint);
        for i in 0..5 {
            v.push_valid(Value::Bigint(i)).unwrap();
        }
        let g = v.gather(&[4, 0, 2]);
        assert_eq!(g.len(), 3);
        assert_eq!(g.get(0), Value::Bigint(4));
        assert_eq!(g.get(1), Value::Bigint(0));
        assert_eq!(g.get(2), Value::Bigint(2));
    }

    #[test]
    fn gather_preserves_nulls() {
        let mut v = Vector::new_empty(DataType::Double);
        v.push_valid(Value::Double(1.5)).unwrap();
        v.push_null().unwrap();
        let g = v.gather(&[1, 0, 1]);
        assert_eq!(g.get(0), Value::Null);
        assert_eq!(g.get(1), Value::Double(1.5));
        assert_eq!(g.get(2), Value::Null);
    }

    #[test]
    fn chunk_roundtrip() {
        let mut a = Vector::new_empty(DataType::Integer);
        let mut b = Vector::new_empty(DataType::Varchar);
        for i in 0..3 {
            a.push_valid(Value::Integer(i)).unwrap();
            b.push_valid(Value::Varchar(format!("r{i}"))).unwrap();
        }
        let c = DataChunk::new(vec![a, b]).unwrap();
        let g = c.gather(&[2, 0]);
        assert_eq!(g.num_rows(), 2);
        assert_eq!(g.column(0).get(0), Value::Integer(2));
        assert_eq!(g.column(1).get(1), Value::Varchar("r0".to_string()));
    }

    #[test]
    fn chunk_mismatch_errors() {
        let mut a = Vector::new_empty(DataType::Integer);
        a.push_valid(Value::Integer(1)).unwrap();
        let b = Vector::new_empty(DataType::Integer);
        assert!(DataChunk::new(vec![a, b]).is_err());
    }

    /// A projected chunk's absent columns (zero-length placeholders) are
    /// rejected by every column reader — never read as NULLs — and carried
    /// through gathers without materializing.
    #[test]
    fn absent_columns_are_rejected_by_readers() {
        let mut present = Vector::new_empty(DataType::Integer);
        present.push_valid(Value::Integer(7)).unwrap();
        let absent = Vector::new_empty(DataType::Varchar);
        let chunk = DataChunk::projected(vec![Arc::new(present), Arc::new(absent)], 1);
        assert_eq!(chunk.num_rows(), 1);
        assert!(chunk.is_present(0));
        assert!(!chunk.is_present(1));
        assert_eq!(chunk.expect_column(0).unwrap().get(0), Value::Integer(7));
        let err = chunk.expect_column(1).unwrap_err().to_string();
        assert!(err.contains("projection missed a reference"), "{err}");
        assert!(chunk.expect_column(2).is_err(), "out of range");

        // Gather shares the absent column through unchanged and gathers the
        // present one.
        let out = chunk.gather(&[0]);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.expect_column(0).unwrap().get(0), Value::Integer(7));
        assert_eq!(out.column(1).len(), 0, "still a zero-length placeholder");
        assert!(out.expect_column(1).is_err());

        // A normal (non-projected) chunk is all-present.
        let normal = DataChunk::new(vec![
            {
                let mut v = Vector::new_empty(DataType::Integer);
                v.push_valid(Value::Integer(3)).unwrap();
                v
            },
            {
                let mut v = Vector::new_empty(DataType::Varchar);
                v.push_valid(Value::Varchar("x".to_string())).unwrap();
                v
            },
        ])
        .unwrap();
        assert!(normal.is_present(0) && normal.is_present(1));
        assert_eq!(
            normal.expect_column(1).unwrap().get(0),
            Value::Varchar("x".to_string())
        );
    }

    #[test]
    fn validity_bitmask_spans_words() {
        let mut v = Vector::new_empty(DataType::Boolean);
        for i in 0..130 {
            if i % 2 == 0 {
                v.push_valid(Value::Boolean(true)).unwrap();
            } else {
                v.push_null().unwrap();
            }
        }
        assert_eq!(v.len(), 130);
        assert_eq!(v.null_count(), 65);
        assert!(v.is_valid(64));
        assert!(v.is_null(65));
        assert!(v.is_valid(128));
        assert!(v.is_null(129));
    }

    /// A DEFAULT typed narrower than its column (`DECIMAL(10,2) DEFAULT 1.5`,
    /// the literal is DECIMAL(2,1) rescaled to scale 2) fills the column in
    /// the column's own type, as a row-by-row fill would.
    #[test]
    fn constant_coerces_a_default_to_the_column_type() {
        let column = DataType::Decimal {
            precision: 10,
            scale: 2,
        };
        let default = Value::Decimal {
            value: 150,
            precision: 3,
            scale: 2,
        };
        let v = Vector::constant(column, &default, 5).unwrap();
        assert_eq!(v.dtype(), column);
        assert_eq!(v.len(), 5);
        for row in 0..5 {
            assert_eq!(v.get(row).to_string(), "1.50");
        }
        let nulls = Vector::constant(column, &Value::Null, 3).unwrap();
        assert_eq!(nulls.dtype(), column);
        assert!((0..3).all(|row| nulls.get(row).is_null()));
        // A value the column cannot hold is still refused.
        assert!(Vector::constant(DataType::Integer, &Value::Varchar("x".into()), 2).is_err());
    }

    #[test]
    fn new_const_matches_push_loop() {
        // Dtypes, lengths, payloads, and validity must match a vector built
        // row-by-row with push_value, across the validity word boundary.
        let cases = [
            Value::Null,
            Value::Integer(7),
            Value::Varchar("pink".to_string()),
        ];
        for value in &cases {
            for n in [0usize, 1, 63, 64, 65, 130] {
                let dtype = value.data_type().unwrap_or(DataType::Integer);
                let mut pushed = Vector::new_empty(dtype);
                for _ in 0..n {
                    pushed.push_value(value.clone()).unwrap();
                }
                let packed = Vector::new_const(value, n);
                assert_eq!(packed.dtype(), pushed.dtype(), "value={value:?} n={n}");
                assert_eq!(packed.len(), pushed.len());
                assert_eq!(packed, pushed, "value={value:?} n={n}");
                for i in 0..n {
                    assert_eq!(packed.get(i), pushed.get(i));
                    assert_eq!(packed.is_valid(i), pushed.is_valid(i));
                }
            }
        }
    }

    #[test]
    fn new_const_varchar_shares_one_allocation() {
        let value = Value::Varchar("shared".to_string());
        let v = Vector::new_const(&value, 100);
        let VectorData::VarChar(strings) = v.data() else {
            panic!("expected VARCHAR data");
        };
        assert_eq!(strings.len(), 100);
        // One view repeated: no per-row copy of the bytes.
        assert!((0..100).all(|i| strings.view(i) == strings.view(0)));
        assert!(strings.iter().all(|s| s == "shared"));
    }

    /// GROUP BY / DISTINCT / JOIN keys use `encode_into`: FLOAT cells must
    /// normalize -0.0 → 0.0 and all NaNs to one encoding (like
    /// `aggregate::encode_value`), so the hash paths agree with `=`.
    #[test]
    fn encode_normalizes_float_zeros_and_nans() {
        let mut v = Vector::new_empty(DataType::Double);
        v.push_valid(Value::Double(-0.0)).unwrap();
        v.push_valid(Value::Double(0.0)).unwrap();
        // Two NaNs with different sign/payload bits.
        v.push_valid(Value::Double(f64::from_bits(0x7ff8_0000_0000_0001)))
            .unwrap();
        v.push_valid(Value::Double(f64::from_bits(0xfff8_0000_0000_0042)))
            .unwrap();
        let enc = |i: usize| {
            let mut b = Vec::new();
            v.encode_into(&mut b, i);
            b
        };
        assert_eq!(enc(0), enc(1), "-0.0 and 0.0 must encode the same");
        assert_eq!(enc(2), enc(3), "all NaNs must encode the same");
        assert_ne!(enc(0), enc(2));
    }
}
