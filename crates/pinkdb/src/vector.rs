//! Columnar vectors, DuckDB-style.
//!
//! `Vector` = one typed column chunk + a validity bitmask (`Vec<u64>`,
//! bit set = valid, clear = NULL). `DataChunk` = a horizontal slice of
//! columns (all the same length, ≤ [`CHUNK_SIZE`] rows). Operators exchange
//! `DataChunk`s; `WHERE` produces a selection vector applied via `gather`
//! instead of copying rows eagerly.

use std::sync::Arc;

use crate::types::{DataType, PinkDbError, Result, Value};

/// Max rows per vector / chunk. DuckDB uses 2048; so do we.
pub const CHUNK_SIZE: usize = 2048;

#[derive(Debug, Clone, PartialEq)]
pub enum VectorData {
    Boolean(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    VarChar(Vec<Arc<str>>),
    Date32(Vec<i32>),
    TimestampMicros(Vec<i64>),
}

impl VectorData {
    fn dtype(&self) -> DataType {
        match self {
            VectorData::Boolean(_) => DataType::Boolean,
            VectorData::Int32(_) => DataType::Integer,
            VectorData::Int64(_) => DataType::Bigint,
            VectorData::Float64(_) => DataType::Double,
            VectorData::VarChar(_) => DataType::Varchar,
            VectorData::Date32(_) => DataType::Date,
            VectorData::TimestampMicros(_) => DataType::Timestamp,
        }
    }

    fn len(&self) -> usize {
        match self {
            VectorData::Boolean(v) => v.len(),
            VectorData::Int32(v) => v.len(),
            VectorData::Int64(v) => v.len(),
            VectorData::Float64(v) => v.len(),
            VectorData::VarChar(v) => v.len(),
            VectorData::Date32(v) => v.len(),
            VectorData::TimestampMicros(v) => v.len(),
        }
    }

    fn with_capacity(dtype: DataType, cap: usize) -> Self {
        match dtype {
            DataType::Boolean => VectorData::Boolean(Vec::with_capacity(cap)),
            DataType::Integer => VectorData::Int32(Vec::with_capacity(cap)),
            DataType::Bigint => VectorData::Int64(Vec::with_capacity(cap)),
            DataType::Double => VectorData::Float64(Vec::with_capacity(cap)),
            DataType::Varchar => VectorData::VarChar(Vec::with_capacity(cap)),
            DataType::Date => VectorData::Date32(Vec::with_capacity(cap)),
            DataType::Timestamp => VectorData::TimestampMicros(Vec::with_capacity(cap)),
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
            Value::Varchar(s) => {
                let shared: Arc<str> = Arc::from(s.as_str());
                (VectorData::VarChar(vec![shared; n]), true)
            }
            Value::Date(d) => (VectorData::Date32(vec![*d; n]), true),
            Value::Timestamp(us) => (VectorData::TimestampMicros(vec![*us; n]), true),
        };
        let mut validity = vec![if all_valid { u64::MAX } else { 0 }; mask_len(n)];
        if all_valid && !n.is_multiple_of(64) {
            // Bits past `len` must read as NULL, like push-built masks do.
            validity[mask_len(n) - 1] = (1u64 << (n % 64)) - 1;
        }
        Vector::from_parts(data, validity, n)
    }

    /// Build a vector directly from raw parts (crate-internal fast paths):
    /// `data` must hold exactly `len` elements; `validity` is normalized to
    /// `mask_len(len)` words (extra words dropped, missing ones read NULL).
    pub(crate) fn from_parts(data: VectorData, mut validity: Vec<u64>, len: usize) -> Vector {
        debug_assert_eq!(data.len(), len);
        validity.resize(mask_len(len), 0);
        Vector { data, validity, len }
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
            VectorData::VarChar(v) => v.push(Arc::from("")),
            VectorData::Date32(v) => v.push(0),
            VectorData::TimestampMicros(v) => v.push(0),
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
            (VectorData::VarChar(v), Value::Varchar(s)) => v.push(s.into()),
            (VectorData::Date32(v), Value::Date(d)) => v.push(d),
            (VectorData::TimestampMicros(v), Value::Timestamp(us)) => v.push(us),
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
            v.push(Arc::from(s));
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

    pub(crate) fn push_float64(&mut self, x: f64) -> Result<()> {
        self.check_room()?;
        match &mut self.data {
            VectorData::Float64(v) => v.push(x),
            data => return Err(typed_push_error("a double", data.dtype())),
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
                // Strings share their Arc instead of copying bytes.
                d.extend(s[start..start + len].iter().cloned());
            }
            (VectorData::Date32(d), VectorData::Date32(s)) => {
                d.extend_from_slice(&s[start..start + len]);
            }
            (VectorData::TimestampMicros(d), VectorData::TimestampMicros(s)) => {
                d.extend_from_slice(&s[start..start + len]);
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
            (VectorData::VarChar(v), Value::Varchar(s)) => v[idx] = s.into(),
            (VectorData::Date32(v), Value::Date(d)) => v[idx] = d,
            (VectorData::TimestampMicros(v), Value::Timestamp(us)) => v[idx] = us,
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
        }
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
            (VectorData::VarChar(d), VectorData::VarChar(s)) => d.push(s[idx].clone()),
            (VectorData::Date32(d), VectorData::Date32(s)) => d.push(s[idx]),
            (VectorData::TimestampMicros(d), VectorData::TimestampMicros(s)) => {
                d.push(s[idx])
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
        let mut out = Vector::with_capacity(self.dtype(), selection.len());
        for &i in selection {
            debug_assert!(i < self.len);
            // push_from shares string Arcs instead of cloning.
            out.push_from(self, i).expect("gather push_from cannot fail");
        }
        out
    }

    /// Count of NULLs (linear scan; fine for ≤2048 rows).
    pub fn null_count(&self) -> usize {
        (0..self.len).filter(|&i| self.is_null(i)).count()
    }
}

/// A horizontal slice of columns, all of equal length.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataChunk {
    /// Reference-counted columns: scans and pass-throughs share storage
    /// instead of cloning megabytes per query.
    pub columns: Vec<Arc<Vector>>,
    pub num_rows: usize,
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
        })
    }

    /// Build from pre-shared columns (zero-copy scan path).
    pub fn from_arcs(columns: Vec<Arc<Vector>>) -> Option<Self> {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        if columns.iter().any(|c| c.len() != num_rows) {
            return None;
        }
        Some(Self { columns, num_rows })
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            num_rows: 0,
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

    /// Apply a selection vector to every column.
    pub fn gather(&self, selection: &[usize]) -> DataChunk {
        let columns = self.columns.iter().map(|c| Arc::new(c.gather(selection))).collect();
        DataChunk {
            columns,
            num_rows: selection.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(strings.iter().all(|s| Arc::ptr_eq(s, &strings[0])));
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
        v.push_valid(Value::Double(f64::from_bits(0x7ff8_0000_0000_0001))).unwrap();
        v.push_valid(Value::Double(f64::from_bits(0xfff8_0000_0000_0042))).unwrap();
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
