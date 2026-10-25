//! Fixed-size `ARRAY` values (wave 107a).
//!
//! One cell is a flat buffer of `len` elements plus an element validity
//! bitmap that only exists when some element is NULL — DuckDB allows NULL
//! elements, and they compare structurally (see `docs/wave-107-plan.md`).
//! A column stores `rows * len` elements in one buffer (`VectorData::Array`),
//! so a 768-d vector column is contiguous memory and never a per-row heap
//! allocation.

use std::cmp::Ordering;

use crate::types::{ArrayElem, DataType, PinkDbError, Result, Value};

/// Element payload: integers (`INTEGER` and `BIGINT` both stored as `i64`,
/// the element kind says which), doubles, or 32-bit floats (`FLOAT[N]`).
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayElems {
    Ints(Vec<i64>),
    Floats(Vec<f64>),
    Floats32(Vec<f32>),
}

impl ArrayElems {
    pub fn with_capacity(elem: ArrayElem, cap: usize) -> ArrayElems {
        match elem {
            ArrayElem::Integer | ArrayElem::Bigint => ArrayElems::Ints(Vec::with_capacity(cap)),
            ArrayElem::Double => ArrayElems::Floats(Vec::with_capacity(cap)),
            ArrayElem::Float => ArrayElems::Floats32(Vec::with_capacity(cap)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ArrayElems::Ints(v) => v.len(),
            ArrayElems::Floats(v) => v.len(),
            ArrayElems::Floats32(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append a zero element (a NULL row still occupies its `len` slots, so a
    /// row's elements are always at `row * len`).
    pub fn push_zero(&mut self) {
        match self {
            ArrayElems::Ints(v) => v.push(0),
            ArrayElems::Floats(v) => v.push(0.0),
            ArrayElems::Floats32(v) => v.push(0.0),
        }
    }

    /// Append one element; `None` is a NULL element and appends a zero.
    pub fn push_elem(&mut self, elem: ArrayElem, v: Option<&Value>) -> Result<()> {
        match (self, v) {
            (ArrayElems::Ints(out), None) => {
                out.push(0);
                return Ok(());
            }
            (ArrayElems::Ints(out), Some(Value::Integer(i))) => out.push(*i as i64),
            (ArrayElems::Ints(out), Some(Value::Bigint(i))) => out.push(*i),
            (ArrayElems::Floats(out), None) => {
                out.push(0.0);
                return Ok(());
            }
            // The literal's element kind is the widest of its elements
            // (`[1, 2.5]` is a `DOUBLE[2]`), so narrower ones widen here.
            (ArrayElems::Floats(out), Some(Value::Integer(i))) => out.push(f64::from(*i)),
            (ArrayElems::Floats(out), Some(Value::Bigint(i))) => out.push(*i as f64),
            (ArrayElems::Floats(out), Some(Value::Double(x))) => out.push(*x),
            (ArrayElems::Floats32(out), None) => {
                out.push(0.0);
                return Ok(());
            }
            (ArrayElems::Floats32(out), Some(Value::Integer(i))) => out.push(*i as f32),
            (ArrayElems::Floats32(out), Some(Value::Bigint(i))) => out.push(*i as f32),
            (ArrayElems::Floats32(out), Some(Value::Double(x))) => out.push(narrow_f32(*x)? as f32),
            (_, Some(other)) => {
                return Err(PinkDbError::Type(format!(
                    "type mismatch: cannot store {other} in a {} array",
                    elem.name()
                )));
            }
        }
        Ok(())
    }

    /// Append `other`'s elements as element kind `elem`.
    pub fn extend_from(&mut self, elem: ArrayElem, other: &ArrayElems) -> Result<()> {
        match (self, other) {
            (ArrayElems::Ints(d), ArrayElems::Ints(s)) => d.extend_from_slice(s),
            (ArrayElems::Floats(d), ArrayElems::Floats(s)) => d.extend_from_slice(s),
            (ArrayElems::Floats(d), ArrayElems::Ints(s)) => d.extend(s.iter().map(|v| *v as f64)),
            (ArrayElems::Floats32(d), ArrayElems::Floats32(s)) => d.extend_from_slice(s),
            (ArrayElems::Floats32(d), ArrayElems::Ints(s)) => d.extend(s.iter().map(|v| *v as f32)),
            (ArrayElems::Floats32(d), ArrayElems::Floats(s)) => {
                for v in s {
                    d.push(narrow_f32(*v)? as f32);
                }
            }
            (_, _) => {
                return Err(PinkDbError::Type(format!(
                    "type mismatch: cannot store these elements in a {} array",
                    elem.name()
                )));
            }
        }
        Ok(())
    }

    /// Copy elements `start..end` from `other` (same kind).
    fn copy_range(&mut self, other: &ArrayElems, start: usize, end: usize) -> Result<()> {
        match (self, other) {
            (ArrayElems::Ints(d), ArrayElems::Ints(s)) => {
                let slice = s.get(start..end).ok_or_else(short_payload)?;
                d.extend_from_slice(slice);
            }
            (ArrayElems::Floats(d), ArrayElems::Floats(s)) => {
                let slice = s.get(start..end).ok_or_else(short_payload)?;
                d.extend_from_slice(slice);
            }
            (ArrayElems::Floats32(d), ArrayElems::Floats32(s)) => {
                let slice = s.get(start..end).ok_or_else(short_payload)?;
                d.extend_from_slice(slice);
            }
            (_, _) => {
                return Err(PinkDbError::Execution(
                    "array element kind mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn short_payload() -> PinkDbError {
    PinkDbError::Execution("corrupt file: truncated ARRAY payload".to_string())
}

/// One row of a fixed-size array.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayValue {
    pub elem: ArrayElem,
    /// Exactly `len` elements; `len` is the type's, not the buffer's.
    pub values: ArrayElems,
    /// Bit `i` set ⇒ element `i` is not NULL. `None` ⇒ no NULL elements
    /// (the common case, and the only case `CAST` produces).
    pub valid: Option<Box<[u8]>>,
}

fn bitmap_len(n: usize) -> usize {
    n.div_ceil(8)
}

#[inline]
fn bit_get(mask: &[u8], idx: usize) -> bool {
    (mask[idx / 8] >> (idx % 8)) & 1 == 1
}

#[inline]
fn bit_set(mask: &mut [u8], idx: usize, valid: bool) {
    if valid {
        mask[idx / 8] |= 1u8 << (idx % 8);
    } else {
        mask[idx / 8] &= !(1u8 << (idx % 8));
    }
}

impl ArrayValue {
    /// Build a cell, checking the buffer holds exactly `len` elements.
    pub fn new(
        elem: ArrayElem,
        len: u32,
        values: ArrayElems,
        valid: Option<Box<[u8]>>,
    ) -> Result<Self> {
        if values.len() != len as usize {
            return Err(PinkDbError::Execution(format!(
                "array length mismatch: {} elements for {}[{}]",
                values.len(),
                elem.name(),
                len
            )));
        }
        if let Some(mask) = &valid
            && mask.len() != bitmap_len(len as usize)
        {
            return Err(PinkDbError::Execution(
                "array validity bitmap has the wrong size".to_string(),
            ));
        }
        Ok(ArrayValue {
            elem,
            values,
            valid,
        })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dtype(&self) -> DataType {
        DataType::Array {
            elem: self.elem,
            len: self.len() as u32,
        }
    }

    pub fn is_valid(&self, i: usize) -> bool {
        match &self.valid {
            None => true,
            Some(mask) => bit_get(mask, i),
        }
    }

    /// True when some element is NULL (a bitmap may exist and be all-ones).
    pub fn has_null_elems(&self) -> bool {
        match &self.valid {
            None => false,
            Some(mask) => (0..self.len()).any(|i| !bit_get(mask, i)),
        }
    }

    /// Element at the 1-based SQL index, NULL when out of range: DuckDB's
    /// subscript rule (`a[0]`, `a[4]`, `a[-1]` are all NULL).
    pub fn elem_at(&self, index: i64) -> Value {
        match usize::try_from(index) {
            Ok(i) if i >= 1 && i <= self.len() => self.elem_value(i - 1),
            _ => Value::Null,
        }
    }

    /// Element at the 1-based `index` of a columnar slot starting at `start`
    /// with `n` elements, NULL when out of range or masked out (the typed
    /// `a[i]` path — no [`ArrayValue`] is built).
    pub(crate) fn slot_elem(
        elem: ArrayElem,
        values: &ArrayElems,
        valid: Option<&[u8]>,
        start: usize,
        n: usize,
        index: i64,
    ) -> Value {
        let Ok(i) = usize::try_from(index) else {
            return Value::Null;
        };
        if i < 1 || i > n || valid.is_some_and(|m| !bit_get(m, start + i - 1)) {
            return Value::Null;
        }
        let k = start + i - 1;
        match values {
            ArrayElems::Ints(v) => match elem {
                ArrayElem::Integer => Value::Integer(v[k] as i32),
                _ => Value::Bigint(v[k]),
            },
            ArrayElems::Floats(v) => Value::Double(v[k]),
            ArrayElems::Floats32(v) => Value::Double(f64::from(v[k])),
        }
    }

    /// The raw `f32` elements of a FLOAT array, for a zero-copy hand-off.
    pub fn floats32(&self) -> Option<&[f32]> {
        match (&self.values, self.elem) {
            (ArrayElems::Floats32(v), _) => Some(v),
            _ => None,
        }
    }

    /// The raw `f64` elements of a DOUBLE array, for a zero-copy hand-off.
    pub fn floats(&self) -> Option<&[f64]> {
        match (&self.values, self.elem) {
            (ArrayElems::Floats(v), _) => Some(v),
            _ => None,
        }
    }

    /// Element `i`, or NULL.
    pub fn elem_value(&self, i: usize) -> Value {
        if !self.is_valid(i) {
            return Value::Null;
        }
        match &self.values {
            ArrayElems::Ints(v) => match self.elem {
                ArrayElem::Integer => Value::Integer(v[i] as i32),
                _ => Value::Bigint(v[i]),
            },
            ArrayElems::Floats(v) => Value::Double(v[i]),
            ArrayElems::Floats32(v) => Value::Double(f64::from(v[i])),
        }
    }

    /// `[1.0, 2.0, 3.0]` — the VARCHAR form and `CAST … AS VARCHAR` output.
    /// Doubles use the shortest round-tripping form so the text parses back to
    /// the same bits.
    pub fn text(&self) -> String {
        let mut out = String::with_capacity(self.len() * 8 + 2);
        out.push('[');
        for i in 0..self.len() {
            if i > 0 {
                out.push_str(", ");
            }
            if !self.is_valid(i) {
                out.push_str("NULL");
                continue;
            }
            match &self.values {
                ArrayElems::Ints(v) => out.push_str(&v[i].to_string()),
                ArrayElems::Floats(v) => out.push_str(&format_double(v[i])),
                ArrayElems::Floats32(v) => out.push_str(&format_double(f64::from(v[i]))),
            }
        }
        out.push(']');
        out
    }

    /// Build a cell from scalar values (literal items, VARIANT elements).
    /// Every non-NULL value must already have element kind `elem`.
    pub fn from_values(elem: ArrayElem, len: u32, items: &[Value]) -> Result<ArrayValue> {
        if items.len() != len as usize {
            return Err(PinkDbError::Execution(format!(
                "array length mismatch: {} elements for {}[{}]",
                items.len(),
                elem.name(),
                len
            )));
        }
        let mut values = ArrayElems::with_capacity(elem, items.len());
        let mut valid = vec![0u8; bitmap_len(items.len())];
        for (i, item) in items.iter().enumerate() {
            let v = if item.is_null() { None } else { Some(item) };
            values.push_elem(elem, v)?;
            bit_set(&mut valid, i, !item.is_null());
        }
        let valid = if valid.iter().all(|b| *b == 0xFF) || items.iter().all(|v| !v.is_null()) {
            None
        } else {
            Some(valid.into_boxed_slice())
        };
        ArrayValue::new(elem, len, values, valid)
    }

    /// Infer the element kind of a literal from its items: the numeric tower
    /// (all integers fit INTEGER, else BIGINT if all fit, else DOUBLE).
    /// DECIMAL literals widen to DOUBLE — pinkdb has no DECIMAL array kind.
    pub fn infer(items: &[Value]) -> Result<(ArrayElem, ArrayValue)> {
        if items.is_empty() {
            return Err(PinkDbError::Type(
                "an empty array literal has no element type (the engine has no LIST type; write CAST([] AS T[n]) once 107a2 lands)".to_string(),
            ));
        }
        let mut elem = ArrayElem::Integer;
        for v in items {
            let kind = match v {
                Value::Null => continue,
                Value::Integer(_) => ArrayElem::Integer,
                Value::Bigint(i) => {
                    if i32::try_from(*i).is_ok() {
                        ArrayElem::Integer
                    } else {
                        ArrayElem::Bigint
                    }
                }
                Value::Double(_) | Value::Decimal(..) => ArrayElem::Double,
                other => {
                    return Err(PinkDbError::Type(format!(
                        "an array element cannot be {other} (elements are numeric)"
                    )));
                }
            };
            elem = elem.max(kind);
        }
        // Re-encode through the target kind so a DECIMAL/BIGINT item widens.
        let items: Vec<Value> = items
            .iter()
            .map(|v| cast_elem_value(v, elem))
            .collect::<Result<Vec<_>>>()?;
        let len = items.len() as u32;
        let value = ArrayValue::from_values(elem, len, &items)?;
        Ok((elem, value))
    }

    /// Cast every element to `elem` (same length).
    pub fn cast_to(&self, elem: ArrayElem) -> Result<ArrayValue> {
        if elem == self.elem {
            return Ok(self.clone());
        }
        let mut values = ArrayElems::with_capacity(elem, self.len());
        for i in 0..self.len() {
            let v = self.elem_value(i);
            let v = if v.is_null() {
                Value::Null
            } else {
                cast_elem_value(&v, elem)?
            };
            values.push_elem(elem, if v.is_null() { None } else { Some(&v) })?;
        }
        let valid = self.valid.clone();
        ArrayValue::new(elem, self.len() as u32, values, valid)
    }

    /// Parse `[1.0, 2.0, …]` (also `[1, NULL]`) into exactly `len` elements.
    /// Whitespace around elements is ignored, matching the text form
    /// [`ArrayValue::text`] writes.
    pub fn parse(text: &str, elem: ArrayElem, len: u32) -> Result<ArrayValue> {
        let t = text.trim();
        let inner = t
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| {
                PinkDbError::Execution(format!(
                    "cannot CAST \"{text}\" AS {}: expected [element, …]",
                    elem.name()
                ))
            })?;
        let parts: Vec<&str> = if inner.trim().is_empty() {
            Vec::new()
        } else {
            inner.split(',').map(|s| s.trim()).collect()
        };
        if parts.len() != len as usize {
            return Err(PinkDbError::Execution(format!(
                "cannot CAST \"{text}\" AS {}[{len}]: found {} elements",
                elem.name(),
                parts.len()
            )));
        }
        let mut values = ArrayElems::with_capacity(elem, parts.len());
        let mut valid = vec![0u8; bitmap_len(parts.len())];
        for (i, part) in parts.iter().enumerate() {
            let v = if part.eq_ignore_ascii_case("null") {
                Value::Null
            } else {
                let v = crate::exec::expr::cast_value(
                    Value::Varchar((*part).to_string()),
                    elem.dtype(),
                )?;
                // DuckDB parses text straight to `f32`, so `'1e40'` becomes
                // `inf` instead of a range error (measured on 2.0).
                match (v, elem) {
                    (Value::Double(x), ArrayElem::Float) => Value::Double(f64::from(x as f32)),
                    (other, _) => other,
                }
            };
            values.push_elem(elem, if v.is_null() { None } else { Some(&v) })?;
            bit_set(&mut valid, i, !v.is_null());
        }
        let valid = if parts.iter().any(|p| p.eq_ignore_ascii_case("null")) {
            Some(valid.into_boxed_slice())
        } else {
            None
        };
        ArrayValue::new(elem, len, values, valid)
    }

    /// Total order: element-wise, NULL element last (greater than any value
    /// and equal to another NULL), then shorter first. `INTEGER`/`BIGINT`
    /// arrays against `DOUBLE` ones compare numerically, like the scalar
    /// numeric tower.
    pub fn compare(a: &ArrayValue, b: &ArrayValue) -> Ordering {
        let n = a.len().min(b.len());
        let ord = cmp_slot(
            &a.values,
            a.valid.as_deref(),
            0,
            &b.values,
            b.valid.as_deref(),
            0,
            n,
        );
        if ord != Ordering::Equal {
            return ord;
        }
        a.len().cmp(&b.len())
    }

    /// Append this value's grouping key: byte-equal exactly when
    /// [`ArrayValue::compare`] reports equality. The element validity flag is
    /// normalized away when no element is NULL, so a lazily allocated bitmap
    /// cannot split a group.
    pub fn hash_key(&self, out: &mut Vec<u8>) {
        encode_key(
            self.elem,
            &self.values,
            self.valid.as_deref(),
            0,
            self.len(),
            out,
        );
    }

    /// Append the same key from a columnar slice (`Vector::encode_into`).
    pub(crate) fn encode_key_range(
        elem: ArrayElem,
        values: &ArrayElems,
        valid: Option<&[u8]>,
        start: usize,
        len: usize,
        out: &mut Vec<u8>,
    ) {
        encode_key(elem, values, valid, start, len, out);
    }
}

/// The shared key encoding (grouping / DISTINCT / join): element kind, length,
/// then a NULL-element flag plus bitmap, then normalized values.
fn encode_key(
    elem: ArrayElem,
    values: &ArrayElems,
    valid: Option<&[u8]>,
    start: usize,
    len: usize,
    out: &mut Vec<u8>,
) {
    out.push(elem_key(elem));
    out.extend_from_slice(&(len as u32).to_le_bytes());
    let has_nulls = valid
        .map(|m| (0..len).any(|i| !bit_get(m, start + i)))
        .unwrap_or(false);
    out.push(has_nulls as u8);
    if has_nulls {
        let mask = valid.expect("has_nulls implies a bitmap");
        for i in 0..len {
            bit_set_block(out, i, bit_get(mask, start + i));
        }
    }
    match values {
        ArrayElems::Ints(v) => {
            for x in &v[start..start + len] {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        ArrayElems::Floats(v) => {
            for x in &v[start..start + len] {
                out.extend_from_slice(&norm_f64_bits(*x).to_le_bytes());
            }
        }
        ArrayElems::Floats32(v) => {
            for x in &v[start..start + len] {
                out.extend_from_slice(&norm_f32_bits(*x).to_le_bytes());
            }
        }
    }
}

/// Append bit `i` of a packed bitmap to `out` (the key form of a shifted
/// element bitmap).
fn bit_set_block(out: &mut Vec<u8>, i: usize, valid: bool) {
    if i.is_multiple_of(8) {
        out.push(0);
    }
    if valid && let Some(last) = out.last_mut() {
        *last |= 1u8 << (i % 8);
    }
}

fn elem_key(elem: ArrayElem) -> u8 {
    match elem {
        ArrayElem::Integer => 0,
        ArrayElem::Bigint => 1,
        ArrayElem::Double => 2,
        ArrayElem::Float => 3,
    }
}

fn elem_from_key(k: u8) -> Result<ArrayElem> {
    match k {
        0 => Ok(ArrayElem::Integer),
        1 => Ok(ArrayElem::Bigint),
        2 => Ok(ArrayElem::Double),
        3 => Ok(ArrayElem::Float),
        other => Err(PinkDbError::Execution(format!(
            "corrupt file: bad ARRAY element kind {other}"
        ))),
    }
}

#[inline]
/// Compare `len` elements of two columnar slots. A slot is `len` consecutive
/// elements of the flat buffer, so its offset is the row index times `len`
/// (for an [`ArrayValue`] the slot is the whole value at offset 0).
///
/// One total order for `=`, `ORDER BY`, `MIN`/`MAX` and `DISTINCT`: a NULL
/// element after every value and equal to another NULL — the order DuckDB
/// gives it in `ORDER BY` (and the only one a grouping key can honour).
pub(crate) fn cmp_slot(
    a: &ArrayElems,
    a_valid: Option<&[u8]>,
    a_start: usize,
    b: &ArrayElems,
    b_valid: Option<&[u8]>,
    b_start: usize,
    len: usize,
) -> Ordering {
    for j in 0..len {
        let ok_a = a_valid.is_none_or(|m| bit_get(m, a_start + j));
        let ok_b = b_valid.is_none_or(|m| bit_get(m, b_start + j));
        let ord = match (ok_a, ok_b) {
            (false, false) => Ordering::Equal,
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (true, true) => cmp_elems(a, a_start + j, b, b_start + j),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn cmp_elems(a: &ArrayElems, i: usize, b: &ArrayElems, k: usize) -> Ordering {
    match (a, b) {
        (ArrayElems::Ints(x), ArrayElems::Ints(y)) => x[i].cmp(&y[k]),
        (ArrayElems::Floats(x), ArrayElems::Floats(y)) => crate::plan::num_cmp(x[i], y[k]),
        (ArrayElems::Ints(x), ArrayElems::Floats(y)) => crate::plan::num_cmp(x[i] as f64, y[k]),
        (ArrayElems::Floats(x), ArrayElems::Ints(y)) => crate::plan::num_cmp(x[i], y[k] as f64),
        (ArrayElems::Floats32(x), ArrayElems::Floats32(y)) => {
            crate::plan::num_cmp(f64::from(x[i]), f64::from(y[k]))
        }
        (ArrayElems::Floats32(x), ArrayElems::Floats(y)) => {
            crate::plan::num_cmp(f64::from(x[i]), y[k])
        }
        (ArrayElems::Floats(x), ArrayElems::Floats32(y)) => {
            crate::plan::num_cmp(x[i], f64::from(y[k]))
        }
        (ArrayElems::Floats32(x), ArrayElems::Ints(y)) => {
            crate::plan::num_cmp(f64::from(x[i]), y[k] as f64)
        }
        (ArrayElems::Ints(x), ArrayElems::Floats32(y)) => {
            crate::plan::num_cmp(x[i] as f64, f64::from(y[k]))
        }
    }
}

/// Narrow a double to `f32` the way DuckDB's cast does: round to nearest,
/// and error only when a finite input becomes infinite (the range ends at the
/// midpoint above `f32::MAX`, which is where rounding overflows).
fn narrow_f32(x: f64) -> Result<f64> {
    let y = x as f32;
    if y.is_infinite() && x.is_finite() {
        return Err(PinkDbError::Type(format!(
            "value {x:?} is out of range for FLOAT"
        )));
    }
    Ok(f64::from(y))
}

/// The same normalization for a 32-bit float key.
fn norm_f32_bits(x: f32) -> u32 {
    if x.is_nan() {
        f32::NAN.to_bits()
    } else if x == 0.0 {
        0.0f32.to_bits()
    } else {
        x.to_bits()
    }
}

/// Normalize `-0.0` → `0.0` and every NaN to one bit pattern, the same
/// normalization the scalar DOUBLE key uses, so grouping and `=` agree.
fn norm_f64_bits(x: f64) -> u64 {
    if x.is_nan() {
        f64::NAN.to_bits()
    } else if x == 0.0 {
        0.0f64.to_bits()
    } else {
        x.to_bits()
    }
}

/// The shortest text form that parses back to the same double, always with a
/// decimal point or exponent (`1.0`, not `1`).
pub fn format_double(x: f64) -> String {
    format!("{x:?}")
}

/// Cast one element value to the element kind. Shares the scalar cast rules
/// (rounding, range checks) through `cast_value`.
fn cast_elem_value(v: &Value, elem: ArrayElem) -> Result<Value> {
    match (v, elem) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Integer(i), ArrayElem::Integer) => Ok(Value::Integer(*i)),
        (Value::Bigint(i), ArrayElem::Bigint) => Ok(Value::Bigint(*i)),
        (Value::Double(x), ArrayElem::Double) => Ok(Value::Double(*x)),
        // A `FLOAT` element keeps its narrowed value as a `Double`: the cell
        // form has no 32-bit float scalar to hand around.
        (Value::Double(x), ArrayElem::Float) => Ok(Value::Double(narrow_f32(*x)?)),
        (Value::Integer(i), ArrayElem::Float) => Ok(Value::Double(f64::from(*i as f32))),
        (Value::Bigint(i), ArrayElem::Float) => Ok(Value::Double(f64::from(*i as f32))),
        (v, _) => crate::exec::expr::cast_value(v.clone(), elem.dtype()),
    }
}

/// Encode `n` elements (`values` may hold more than `n` for a whole column
/// chunk) plus their validity bitmap: `u8 flag | [bitmap] | values`.
pub(crate) fn write_elems(
    out: &mut Vec<u8>,
    elem: ArrayElem,
    values: &ArrayElems,
    valid: Option<&[u8]>,
    n: usize,
) {
    let has_nulls = valid
        .map(|m| (0..n).any(|i| !bit_get(m, i)))
        .unwrap_or(false);
    out.push(has_nulls as u8);
    if has_nulls {
        out.extend_from_slice(&valid.expect("bitmap")[..bitmap_len(n)]);
    }
    match values {
        ArrayElems::Ints(v) => {
            for x in &v[..n] {
                match elem {
                    ArrayElem::Integer => out.extend_from_slice(&(*x as i32).to_le_bytes()),
                    _ => out.extend_from_slice(&x.to_le_bytes()),
                }
            }
        }
        ArrayElems::Floats(v) => {
            for x in &v[..n] {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        ArrayElems::Floats32(v) => {
            for x in &v[..n] {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
    }
}

/// The three parts of an encoded element run: the values, the bytes consumed
/// and the NULL-element bitmap.
pub(crate) type ElemRun = (ArrayElems, usize, Option<Box<[u8]>>);

/// Inverse of [`write_elems`]. `bytes` is the whole remaining payload.
pub(crate) fn read_elems(bytes: &[u8], elem: ArrayElem, n: usize) -> Result<ElemRun> {
    let width = elem.width();
    let mut pos = 0usize;
    let flag = *bytes.first().ok_or_else(short_payload)?;
    pos += 1;
    let valid = if flag == 0 {
        None
    } else if flag == 1 {
        let end = pos + bitmap_len(n);
        let bits = bytes.get(pos..end).ok_or_else(short_payload)?;
        pos = end;
        Some(bits.to_vec().into_boxed_slice())
    } else {
        return Err(PinkDbError::Execution(
            "corrupt file: bad ARRAY NULL-element flag".to_string(),
        ));
    };
    let need = n
        .checked_mul(width)
        .and_then(|b| pos.checked_add(b))
        .ok_or_else(short_payload)?;
    let body = bytes.get(pos..need).ok_or_else(short_payload)?;
    let values = match elem {
        ArrayElem::Integer => ArrayElems::Ints(
            body.as_chunks::<4>()
                .0
                .iter()
                .map(|c| i32::from_le_bytes(*c) as i64)
                .collect(),
        ),
        ArrayElem::Bigint => ArrayElems::Ints(
            body.as_chunks::<8>()
                .0
                .iter()
                .map(|c| i64::from_le_bytes(*c))
                .collect(),
        ),
        ArrayElem::Double => ArrayElems::Floats(
            body.as_chunks::<8>()
                .0
                .iter()
                .map(|c| f64::from_le_bytes(*c))
                .collect(),
        ),
        ArrayElem::Float => ArrayElems::Floats32(
            body.as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect(),
        ),
    };
    Ok((values, need, valid))
}

/// Decode the self-describing cell form used by the wire protocol and the
/// WAL literal: `u8 elem | u32 len | <write_elems>`.
pub fn encode_cell(v: &ArrayValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + v.len() * v.elem.width());
    out.push(elem_key(v.elem));
    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
    write_elems(&mut out, v.elem, &v.values, v.valid.as_deref(), v.len());
    out
}

/// Inverse of [`encode_cell`]; `bytes` is the whole cell payload.
pub fn decode_cell(bytes: &[u8]) -> Result<ArrayValue> {
    let elem = elem_from_key(*bytes.first().ok_or_else(short_payload)?)?;
    let len = bytes
        .get(1..5)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or_else(short_payload)?;
    let (values, used, valid) = read_elems(&bytes[5..], elem, len as usize)?;
    if 5 + used != bytes.len() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing bytes in ARRAY payload".to_string(),
        ));
    }
    ArrayValue::new(elem, len, values, valid)
}

/// Copy `len` elements of `src` starting at element `start` into a fresh cell.
pub(crate) fn cell_from_slice(
    elem: ArrayElem,
    values: &ArrayElems,
    valid: Option<&[u8]>,
    start: usize,
    len: usize,
) -> Result<ArrayValue> {
    let mut out = ArrayElems::with_capacity(elem, len);
    out.copy_range(values, start, start + len)?;
    // A slot with no NULL element keeps the "no bitmap" shape: the column
    // bitmap exists once *any* row has a NULL element.
    let mask = valid
        .filter(|m| (0..len).any(|i| !bit_get(m, start + i)))
        .map(|m| {
            let mut bits = vec![0u8; bitmap_len(len)];
            for i in 0..len {
                bit_set(&mut bits, i, bit_get(m, start + i));
            }
            bits.into_boxed_slice()
        });
    ArrayValue::new(elem, len as u32, out, mask)
}
