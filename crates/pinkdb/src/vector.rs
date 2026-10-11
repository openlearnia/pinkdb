//! Columnar vectors, DuckDB-style.
//!
//! `Vector` = one typed column chunk + a validity bitmask (`Vec<u64>`,
//! bit set = valid, clear = NULL). `DataChunk` = a horizontal slice of
//! columns (all the same length, ≤ [`CHUNK_SIZE`] rows). Operators exchange
//! `DataChunk`s; `WHERE` produces a selection vector applied via `gather`
//! instead of copying rows eagerly.

use crate::types::{DataType, PinkDbError, Result, Value};

/// Max rows per vector / chunk. DuckDB uses 2048; so do we.
pub const CHUNK_SIZE: usize = 2048;

#[derive(Debug, Clone, PartialEq)]
pub enum VectorData {
    Boolean(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    VarChar(Vec<String>),
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
            VectorData::VarChar(v) => v.push(String::new()),
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
            VectorData::VarChar(v) => Value::Varchar(v[idx].clone()),
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
            (VectorData::VarChar(v), Value::Varchar(s)) => v.push(s),
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
            (VectorData::VarChar(v), Value::Varchar(s)) => v[idx] = s,
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

    /// Build a new vector with rows in `selection` order. Used by Filter.
    pub fn gather(&self, selection: &[usize]) -> Vector {
        let mut out = Vector::with_capacity(self.dtype(), selection.len());
        for &i in selection {
            debug_assert!(i < self.len);
            // gather() is infallible for in-bounds indices; capacity was reserved.
            if self.is_null(i) {
                out.push_null().expect("gather push_null cannot fail");
            } else {
                out.push_valid(self.get(i))
                    .expect("gather push_valid cannot fail");
            }
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
    pub columns: Vec<Vector>,
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
        Ok(Self { columns, num_rows })
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
        let columns = self.columns.iter().map(|c| c.gather(selection)).collect();
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
}
