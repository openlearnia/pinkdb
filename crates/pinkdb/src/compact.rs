//! The decoded cache's compact form of a clean chunk (wave 136c-3).
//!
//! A cached chunk used to stay fully decoded: 8 bytes per `DECIMAL(15,2)` and
//! 16 per VARCHAR view, whatever the values. TPC-H Q1 at SF10 kept 4.3 GB
//! resident that way. The cache now keeps what is cheap to expand again:
//!
//! - integer-like columns (`INTEGER`, `DATE`, `BIGINT`, `TIMESTAMP`,
//!   `DECIMAL(p ≤ 18)`) as the chunk's minimum plus `u8`/`u16`/`u32` offsets
//!   (frame of reference at byte widths: expanding is one add per value);
//! - low-cardinality VARCHAR as `u8`/`u16` codes into the chunk's own
//!   dictionary, whose strings sit in one shared buffer (expanding copies
//!   views, never bytes).
//!
//! Anything else stays a decoded [`Vector`]. Expanding reproduces the vector
//! value for value, NULL slots included, so no reader can tell the forms
//! apart. DuckDB keeps compressed segments in its buffer pool and decompresses
//! per scan; this keeps a lighter form so a warm scan pays a widen, not a
//! decode.

use std::collections::HashMap;
use std::sync::Arc;

use crate::strings::StrVec;
use crate::vector::{Vector, VectorData};

/// Unsigned offsets at the narrowest byte width that holds them.
enum Codes {
    U8(Box<[u8]>),
    U16(Box<[u16]>),
    U32(Box<[u32]>),
}

impl Codes {
    /// Offsets `values[i] - base`, at the width `range` (the largest offset)
    /// needs; `None` past `limit` bytes per value.
    fn narrow(
        offsets: impl ExactSizeIterator<Item = u64> + Clone,
        range: u64,
        limit: usize,
    ) -> Option<Codes> {
        let width = match range {
            0..=0xFF => 1,
            0x100..=0xFFFF => 2,
            0x1_0000..=0xFFFF_FFFF => 4,
            _ => return None,
        };
        if width > limit {
            return None;
        }
        Some(match width {
            1 => Codes::U8(offsets.map(|o| o as u8).collect()),
            2 => Codes::U16(offsets.map(|o| o as u16).collect()),
            _ => Codes::U32(offsets.map(|o| o as u32).collect()),
        })
    }

    fn slice(&self) -> CodeSlice<'_> {
        match self {
            Codes::U8(c) => CodeSlice::U8(c),
            Codes::U16(c) => CodeSlice::U16(c),
            Codes::U32(c) => CodeSlice::U32(c),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Codes::U8(c) => c.len(),
            Codes::U16(c) => c.len() * 2,
            Codes::U32(c) => c.len() * 4,
        }
    }

    fn widen_i64(&self, base: i64) -> Vec<i64> {
        match self {
            Codes::U8(c) => c.iter().map(|&o| base.wrapping_add(o as i64)).collect(),
            Codes::U16(c) => c.iter().map(|&o| base.wrapping_add(o as i64)).collect(),
            Codes::U32(c) => c.iter().map(|&o| base.wrapping_add(o as i64)).collect(),
        }
    }

    fn widen_i32(&self, base: i32) -> Vec<i32> {
        match self {
            Codes::U8(c) => c.iter().map(|&o| base.wrapping_add(o as i32)).collect(),
            Codes::U16(c) => c.iter().map(|&o| base.wrapping_add(o as i32)).collect(),
            Codes::U32(c) => c.iter().map(|&o| base.wrapping_add(o as i32)).collect(),
        }
    }

    fn indices(&self) -> Vec<usize> {
        match self {
            Codes::U8(c) => c.iter().map(|&o| o as usize).collect(),
            Codes::U16(c) => c.iter().map(|&o| o as usize).collect(),
            Codes::U32(c) => c.iter().map(|&o| o as usize).collect(),
        }
    }

    fn at(&self, i: usize) -> u64 {
        match self {
            Codes::U8(c) => u64::from(c[i]),
            Codes::U16(c) => u64::from(c[i]),
            Codes::U32(c) => u64::from(c[i]),
        }
    }
}

/// A compact chunk's representation (see [`CompactChunk::form`]).
#[allow(dead_code)] // the dictionary GROUP BY wave is the first consumer
pub(crate) enum CompactForm<'a> {
    /// Value `i` is `base + codes[i]`, in the column's own payload type.
    Offsets { base: i64, codes: CodeSlice<'a> },
    /// Value `i` is `dict[codes[i]]`; the dictionary is this chunk's alone.
    Dictionary {
        dict: &'a StrVec,
        codes: CodeSlice<'a>,
    },
}

/// One code per row, at the width the chunk needed.
#[allow(dead_code)] // with `CompactForm`
pub(crate) enum CodeSlice<'a> {
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
}

/// Which payload an integer-like chunk expands back into.
#[derive(Clone, Copy)]
enum IntKind {
    Int32,
    Date32,
    Int64,
    Timestamp,
    Decimal64 { precision: u8, scale: u8 },
}

enum Data {
    Ints {
        kind: IntKind,
        base: i64,
        codes: Codes,
    },
    Strs {
        dict: StrVec,
        codes: Codes,
    },
}

/// A clean chunk in the cache's compact form.
pub(crate) struct CompactChunk {
    data: Data,
    validity: Box<[u64]>,
    len: usize,
}

/// A VARCHAR chunk with more distinct values than one in this many rows is
/// left decoded: its dictionary would hold most of its bytes anyway.
const DICT_MAX_SHARE: usize = 4;

impl CompactChunk {
    /// The compact form of `v`, when there is one and it is smaller.
    pub(crate) fn of(v: &Vector) -> Option<CompactChunk> {
        let len = v.len();
        if len == 0 {
            return None;
        }
        let data = match v.data() {
            VectorData::Int32(values) => ints32(IntKind::Int32, values)?,
            VectorData::Date32(values) => ints32(IntKind::Date32, values)?,
            VectorData::Int64(values) => ints64(IntKind::Int64, values)?,
            VectorData::TimestampMicros(values) => ints64(IntKind::Timestamp, values)?,
            VectorData::Decimal64 {
                values,
                precision,
                scale,
            } => ints64(
                IntKind::Decimal64 {
                    precision: *precision,
                    scale: *scale,
                },
                values,
            )?,
            VectorData::VarChar(strings) => dictionary(strings)?,
            _ => return None,
        };
        let compact = CompactChunk {
            data,
            validity: v.validity_words().into(),
            len,
        };
        (compact.memory_size() < v.memory_size()).then_some(compact)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The bytes this form holds (what the cache charges for it).
    pub(crate) fn memory_size(&self) -> usize {
        let data = match &self.data {
            Data::Ints { codes, .. } => codes.bytes(),
            Data::Strs { dict, codes } => codes.bytes() + dict.memory_size(),
        };
        std::mem::size_of::<Self>() + data + self.validity.len() * 8
    }

    /// The decoded vector, value for value what [`CompactChunk::of`] was given.
    pub(crate) fn expand(&self) -> Vector {
        let data = match &self.data {
            Data::Ints { kind, base, codes } => match *kind {
                IntKind::Int32 => VectorData::Int32(codes.widen_i32(*base as i32)),
                IntKind::Date32 => VectorData::Date32(codes.widen_i32(*base as i32)),
                IntKind::Int64 => VectorData::Int64(codes.widen_i64(*base)),
                IntKind::Timestamp => VectorData::TimestampMicros(codes.widen_i64(*base)),
                IntKind::Decimal64 { precision, scale } => VectorData::Decimal64 {
                    values: codes.widen_i64(*base),
                    precision,
                    scale,
                },
            },
            Data::Strs { dict, codes } => VectorData::VarChar(dict.take(codes.indices())),
        };
        Vector::from_parts(data, self.validity.to_vec(), self.len)
    }

    /// The representation itself, for consumers that work on codes without
    /// expanding: a GROUP BY over dictionary codes, a filter comparing
    /// offsets against a rebased constant. Expanding stays the default.
    #[allow(dead_code)] // the dictionary GROUP BY wave is the first consumer
    pub(crate) fn form(&self) -> CompactForm<'_> {
        match &self.data {
            Data::Ints { base, codes, .. } => CompactForm::Offsets {
                base: *base,
                codes: codes.slice(),
            },
            Data::Strs { dict, codes } => CompactForm::Dictionary {
                dict,
                codes: codes.slice(),
            },
        }
    }

    /// Bit `i` set ⇒ row `i` is valid, as in [`Vector::validity_words`].
    #[allow(dead_code)] // with `form`
    pub(crate) fn validity(&self) -> &[u64] {
        &self.validity
    }

    /// Whether row `i` is valid, and its value as the expanded vector would
    /// hold it: a point read without expanding the chunk.
    pub(crate) fn value(&self, i: usize) -> crate::types::Value {
        use crate::types::Value;
        if self
            .validity
            .get(i / 64)
            .is_none_or(|w| w >> (i % 64) & 1 == 0)
        {
            return Value::Null;
        }
        match &self.data {
            Data::Ints { kind, base, codes } => {
                let v = base.wrapping_add(codes.at(i) as i64);
                match *kind {
                    IntKind::Int32 => Value::Integer(v as i32),
                    IntKind::Date32 => Value::Date(v as i32),
                    IntKind::Int64 => Value::Bigint(v),
                    IntKind::Timestamp => Value::Timestamp(v),
                    IntKind::Decimal64 { precision, scale } => Value::Decimal {
                        value: i128::from(v),
                        precision,
                        scale,
                    },
                }
            }
            Data::Strs { dict, codes } => Value::Varchar(dict.get(codes.at(i) as usize).into()),
        }
    }
}

fn ints32(kind: IntKind, values: &[i32]) -> Option<Data> {
    let (&min, &max) = (values.iter().min()?, values.iter().max()?);
    let range = (i64::from(max) - i64::from(min)) as u64;
    let offsets = values
        .iter()
        .map(move |&v| (i64::from(v) - i64::from(min)) as u64);
    // Four bytes would only copy an `i32`.
    let codes = Codes::narrow(offsets, range, 2)?;
    Some(Data::Ints {
        kind,
        base: i64::from(min),
        codes,
    })
}

fn ints64(kind: IntKind, values: &[i64]) -> Option<Data> {
    let (&min, &max) = (values.iter().min()?, values.iter().max()?);
    let range = u64::try_from(i128::from(max) - i128::from(min)).ok()?;
    let offsets = values.iter().map(move |&v| v.wrapping_sub(min) as u64);
    let codes = Codes::narrow(offsets, range, 4)?;
    Some(Data::Ints {
        kind,
        base: min,
        codes,
    })
}

/// The chunk's distinct strings in one shared buffer, plus a code per row.
fn dictionary(strings: &StrVec) -> Option<Data> {
    let limit = (strings.len() / DICT_MAX_SHARE).clamp(1, 1 << 16);
    let mut index: HashMap<&str, u32> = HashMap::new();
    let mut distinct: Vec<&str> = Vec::new();
    let mut codes: Vec<u32> = Vec::with_capacity(strings.len());
    for s in strings.iter() {
        let next = distinct.len() as u32;
        let code = *index.entry(s).or_insert(next);
        if code == next {
            if distinct.len() == limit {
                return None;
            }
            distinct.push(s);
        }
        codes.push(code);
    }
    let mut buffer = Vec::with_capacity(distinct.iter().map(|s| s.len()).sum());
    let mut ranges = Vec::with_capacity(distinct.len());
    for s in &distinct {
        ranges.push((buffer.len(), s.len()));
        buffer.extend_from_slice(s.as_bytes());
    }
    let dict = StrVec::over_buffer(Arc::from(buffer), ranges.into_iter());
    let range = (distinct.len() - 1) as u64;
    let codes = Codes::narrow(codes.into_iter().map(u64::from), range, 2)?;
    Some(Data::Strs { dict, codes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Value};

    fn round_trip(v: &Vector) -> Option<usize> {
        let c = CompactChunk::of(v)?;
        let back = c.expand();
        assert_eq!(back.dtype(), v.dtype());
        assert_eq!(back.len(), v.len());
        for i in 0..v.len() {
            assert_eq!(back.get(i), v.get(i), "row {i}");
            assert_eq!(c.value(i), v.get(i), "point row {i}");
        }
        Some(c.memory_size())
    }

    fn vector(dtype: DataType, values: &[Value]) -> Vector {
        let mut v = Vector::new_empty(dtype);
        for value in values {
            v.push_value(value.clone()).unwrap();
        }
        v
    }

    #[test]
    fn integers_decimals_and_dates_round_trip_at_byte_widths() {
        let small: Vec<Value> = (0..2048).map(|i| Value::Integer(1000 + i % 200)).collect();
        assert!(round_trip(&vector(DataType::Integer, &small)).unwrap() < 2048 * 2);
        let dec: Vec<Value> = (0..2048)
            .map(|i| match i % 97 {
                0 => Value::Null,
                _ => Value::Decimal {
                    value: 9_000_000 + i128::from(i) * 4001,
                    precision: 15,
                    scale: 2,
                },
            })
            .collect();
        assert!(
            round_trip(&vector(
                DataType::Decimal {
                    precision: 15,
                    scale: 2
                },
                &dec
            ))
            .unwrap()
                < 2048 * 5
        );
        let dates: Vec<Value> = (0..2048).map(|i| Value::Date(8000 + i)).collect();
        round_trip(&vector(DataType::Date, &dates)).unwrap();
        // A full-width spread stays decoded.
        let wide = [
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Bigint(0),
        ];
        assert!(round_trip(&vector(DataType::Bigint, &wide)).is_none());
        let ts: Vec<Value> = (0..300)
            .map(|i| Value::Timestamp(-5 + i64::from(i) * 1_000_000))
            .collect();
        round_trip(&vector(DataType::Timestamp, &ts)).unwrap();
    }

    #[test]
    fn low_cardinality_strings_keep_codes_and_long_values() {
        let modes = [
            "TRUCK",
            "DELIVER IN PERSON",
            "COLLECT COD",
            "a string well past twelve bytes",
        ];
        let rows: Vec<Value> = (0..2048)
            .map(|i| match i % 11 {
                0 => Value::Null,
                k => Value::Varchar(modes[k % modes.len()].into()),
            })
            .collect();
        assert!(round_trip(&vector(DataType::Varchar, &rows)).unwrap() < 2048 * 2);
        // Mostly distinct strings stay decoded.
        let distinct: Vec<Value> = (0..2048)
            .map(|i| Value::Varchar(format!("comment {i}")))
            .collect();
        assert!(round_trip(&vector(DataType::Varchar, &distinct)).is_none());
    }
}
