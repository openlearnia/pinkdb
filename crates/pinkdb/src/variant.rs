//! `VARIANT`: a first-class semi-structured value (wave 101a).
//!
//! A VARIANT row may hold any shape — null, boolean, number, string, date,
//! timestamp, array or object — in a typed column, not as JSON text. The
//! value model, the total order, equality and the binary encoding are
//! specified in `docs/wave-101-plan.md`; this module is their implementation.
//!
//! Two facts drive most of the code here:
//!
//! - **Objects keep their keys sorted.** Every constructor sorts (and
//!   de-duplicates, keeping the last value) so the encoding is canonical: two
//!   structurally equal objects have byte-identical encodings regardless of
//!   the order the keys arrived in.
//! - **Numbers compare by value.** `1`, `1.0` and a `DECIMAL(3,1)` holding
//!   `1.0` are equal and land in one `GROUP BY` bucket, so equality, the
//!   total order and the hash key all go through one exact decimal form
//!   ([`Num`]) rather than the storage kinds.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::types::{
    self, DataType, PinkDbError, Result, Value, decimal_to_f64, format_date, format_decimal,
    format_timestamp, parse_date, parse_timestamp, rescale_decimal,
};

/// Deepest nesting a VARIANT may have. Bounds the recursive encoder, decoder,
/// comparator and JSON writer so a hostile document cannot smash the stack.
pub const MAX_DEPTH: usize = 128;

/// One semi-structured value.
#[derive(Debug, Clone)]
pub enum Variant {
    /// The JSON `null` *value*. SQL NULL is a different thing: it is the
    /// NULL bitmap bit, and `Value::Null` never holds a `Variant::Null`.
    Null,
    Bool(bool),
    /// One integer width: `i64` (pinkdb has no UINT/INT128 types).
    Int(i64),
    Double(f64),
    /// Unscaled `i128` plus scale, exactly like [`Value::Decimal`].
    Decimal(i128, u8),
    Str(Arc<str>),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since 1970-01-01 00:00:00.
    Timestamp(i64),
    Array(Arc<[Variant]>),
    /// Keys sorted by UTF-8 bytes, no duplicates.
    Object(Arc<[(Arc<str>, Variant)]>),
}

impl Variant {
    /// Build an array, refusing pathological nesting depth.
    pub fn array(items: Vec<Variant>) -> Result<Variant> {
        for it in &items {
            it.check_depth(1)?;
        }
        Ok(Variant::Array(items.into()))
    }

    /// Build an object: sorts the fields by key, keeps the last value for a
    /// repeated key (what JSON parsers do), then checks the nesting depth.
    pub fn object(fields: Vec<(Arc<str>, Variant)>) -> Result<Variant> {
        for (_, v) in &fields {
            v.check_depth(1)?;
        }
        let mut fields = fields;
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        fields.dedup_by(|later, earlier| {
            // `dedup_by(equal)` is called with (current, previous) and removes
            // the *current* element; sort is stable, so the last duplicate
            // is the surviving candidate. Copy it back over the earlier slot
            // so "last wins" holds.
            if later.0 == earlier.0 {
                earlier.1 = std::mem::replace(&mut later.1, Variant::Null);
                true
            } else {
                false
            }
        });
        Ok(Variant::Object(fields.into()))
    }

    /// The member called `key`, or `None` when this is not an object or has
    /// no such key (the caller turns that into SQL NULL).
    pub fn object_get(&self, key: &str) -> Option<&Variant> {
        match self {
            Variant::Object(fields) => fields
                .binary_search_by(|(k, _)| k.as_ref().cmp(key))
                .ok()
                .map(|i| &fields[i].1),
            _ => None,
        }
    }

    /// The element at a 0-based index, or `None` when this is not an array or
    /// the index is past the end.
    pub fn array_get(&self, index: usize) -> Option<&Variant> {
        match self {
            Variant::Array(items) => items.get(index),
            _ => None,
        }
    }

    fn check_depth(&self, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(too_deep());
        }
        match self {
            Variant::Array(items) => {
                for it in items.iter() {
                    it.check_depth(depth + 1)?;
                }
            }
            Variant::Object(fields) => {
                for (_, v) in fields.iter() {
                    v.check_depth(depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// `variant_type()`'s answer: pinkdb's own type names.
    pub fn type_name(&self) -> &'static str {
        match self {
            Variant::Null => "NULL",
            Variant::Bool(_) => "BOOL",
            Variant::Int(_) => "BIGINT",
            Variant::Double(_) => "DOUBLE",
            Variant::Decimal(..) => "DECIMAL",
            Variant::Str(_) => "VARCHAR",
            Variant::Date(_) => "DATE",
            Variant::Timestamp(_) => "TIMESTAMP",
            Variant::Array(_) => "ARRAY",
            Variant::Object(_) => "OBJECT",
        }
    }

    /// The JSON text of this value. Non-finite doubles print as
    /// `NaN`/`Infinity`/`-Infinity` and integral doubles keep a `.0`, both of
    /// which [`from_json_str`] reads back to the same kind.
    ///
    /// DATE and TIMESTAMP have no JSON form and print as ISO strings, so the
    /// text round trip demotes them to [`Variant::Str`]; [`Self::is_json_exact`]
    /// reports when that happens.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    /// True when [`Self::to_json`] reads back as the same value and kind.
    /// Every kind except DATE and TIMESTAMP (JSON has no such value).
    pub fn is_json_exact(&self) -> bool {
        match self {
            Variant::Date(_) | Variant::Timestamp(_) => false,
            Variant::Array(items) => items.iter().all(Variant::is_json_exact),
            Variant::Object(fields) => fields.iter().all(|(_, v)| v.is_json_exact()),
            _ => true,
        }
    }

    fn write_json(&self, out: &mut String) {
        match self {
            Variant::Null => out.push_str("null"),
            Variant::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Variant::Int(i) => out.push_str(&i.to_string()),
            Variant::Double(x) => write_json_double(*x, out),
            Variant::Decimal(v, scale) => out.push_str(&format_decimal(*v, *scale)),
            Variant::Str(s) => write_json_str(s, out),
            Variant::Date(d) => write_json_str(&format_date(*d), out),
            Variant::Timestamp(us) => write_json_str(&format_timestamp(*us), out),
            Variant::Array(items) => {
                out.push('[');
                for (i, it) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    it.write_json(out);
                }
                out.push(']');
            }
            Variant::Object(fields) => {
                out.push('{');
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_json_str(k, out);
                    out.push(':');
                    v.write_json(out);
                }
                out.push('}');
            }
        }
    }
}

/// `Display` is the JSON text — the same text `CAST(v AS VARCHAR)` yields, so
/// `VARCHAR → VARIANT → VARCHAR` is the identity for JSON's value model.
impl fmt::Display for Variant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_json())
    }
}

/// Structural equality with numeric cross-kind equality (`1` = `1.0`), i.e.
/// exactly what [`cmp`] reports. Kept in lockstep with [`hash_key`].
impl PartialEq for Variant {
    fn eq(&self, other: &Variant) -> bool {
        cmp(self, other) == Ordering::Equal
    }
}

fn too_deep() -> PinkDbError {
    PinkDbError::Execution(format!("VARIANT is nested deeper than {MAX_DEPTH} levels"))
}

fn write_json_double(x: f64, out: &mut String) {
    if x.is_nan() {
        out.push_str("NaN");
    } else if x == f64::INFINITY {
        out.push_str("Infinity");
    } else if x == f64::NEG_INFINITY {
        out.push_str("-Infinity");
    } else {
        // Rust's `{:?}` is the shortest form that round-trips and uses an
        // exponent for extreme magnitudes (`1e300` instead of the 301-digit
        // Display form); it is always a valid JSON number apart from the three
        // non-finite spellings handled above. An integral value prints as
        // `1.0`, so it reads back as a DOUBLE and not an integer.
        let s = format!("{x:?}");
        out.push_str(&s);
        if !s.contains(['.', 'e', 'E']) {
            out.push_str(".0");
        }
    }
}

fn write_json_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            // Everything else, including non-ASCII, is emitted as raw UTF-8
            // so a string survives the text round trip byte for byte.
            c => out.push(c),
        }
    }
    out.push('"');
}

// ---------------------------------------------------------------- ordering

/// The cross-kind rank of the total order (see the wave-101 plan). SQL NULL is
/// handled by the engine's existing NULL ordering, not here.
fn class(v: &Variant) -> u8 {
    match v {
        Variant::Bool(_) => 0,
        Variant::Int(_) | Variant::Double(_) | Variant::Decimal(..) => 1,
        Variant::Str(_) => 2,
        Variant::Date(_) => 3,
        Variant::Timestamp(_) => 4,
        Variant::Array(_) => 5,
        Variant::Object(_) => 6,
        // A variant null is a value, not SQL NULL, but it sorts after every
        // kind — DuckDB's ORDER BY puts it last too.
        Variant::Null => 7,
    }
}

/// The total order over VARIANTs. Numbers compare by value across kinds, so
/// `1` (BIGINT) and `1.0` (DECIMAL or DOUBLE) are `Equal` — the same relation
/// the canonical hash key encodes, which is what makes `=` agree with
/// `GROUP BY` and `DISTINCT`.
pub fn cmp(a: &Variant, b: &Variant) -> Ordering {
    let (ca, cb) = (class(a), class(b));
    if ca != cb {
        return ca.cmp(&cb);
    }
    match (a, b) {
        (Variant::Array(x), Variant::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let o = cmp(xi, yi);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        (Variant::Object(x), Variant::Object(y)) => {
            for ((xk, xv), (yk, yv)) in x.iter().zip(y.iter()) {
                let o = xk.cmp(yk);
                if o != Ordering::Equal {
                    return o;
                }
                let o = cmp(xv, yv);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        // Numbers: value first, kind second (a stable tie-break).
        (Variant::Int(_) | Variant::Double(_) | Variant::Decimal(..), _) => {
            let (na, nb) = (Num::of(a), Num::of(b));
            na.cmp(&nb)
        }
        (Variant::Bool(x), Variant::Bool(y)) => x.cmp(y),
        (Variant::Str(x), Variant::Str(y)) => x.cmp(y),
        (Variant::Date(x), Variant::Date(y)) => x.cmp(y),
        (Variant::Timestamp(x), Variant::Timestamp(y)) => x.cmp(y),
        // Same class ⇒ same constructor; the class check above covers it.
        _ => Ordering::Equal,
    }
}

/// Canonical hash key: byte-equal exactly when [`cmp`] says `Equal`, so
/// `GROUP BY`, `DISTINCT` and JOIN keys agree with `=`.
pub fn hash_key(v: &Variant, out: &mut Vec<u8>) {
    out.push(class(v));
    match v {
        Variant::Null => {}
        Variant::Bool(b) => out.push(*b as u8),
        Variant::Int(_) | Variant::Double(_) | Variant::Decimal(..) => Num::of(v).write_key(out),
        Variant::Str(s) => push_bytes(out, s.as_bytes()),
        Variant::Date(d) => out.extend_from_slice(&d.to_le_bytes()),
        Variant::Timestamp(us) => out.extend_from_slice(&us.to_le_bytes()),
        Variant::Array(items) => {
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for it in items.iter() {
                hash_key(it, out);
            }
        }
        Variant::Object(fields) => {
            out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for (k, v) in fields.iter() {
                push_bytes(out, k.as_bytes());
                hash_key(v, out);
            }
        }
    }
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// An exact decimal form of any number: `± digits × 10^-scale` with leading
/// and trailing zeros stripped, so the representation is unique per value.
struct Num {
    /// 0 finite, 1 +inf, 2 -inf, 3 NaN (ordered last, equal to itself —
    /// matching `Value`'s DOUBLE ordering).
    special: u8,
    neg: bool,
    digits: Vec<u8>,
    /// May be negative (`1e3` ⇒ digits `1`, scale `-3`).
    scale: i32,
}

impl Num {
    fn of(v: &Variant) -> Num {
        match v {
            Variant::Int(i) => Num::from_parts(*i < 0, i.unsigned_abs().to_string(), 0),
            Variant::Decimal(unscaled, scale) => Num::from_parts(
                *unscaled < 0,
                unscaled.unsigned_abs().to_string(),
                *scale as i32,
            ),
            Variant::Double(x) => Num::of_double(*x),
            _ => unreachable!("Num::of on a non-number"),
        }
    }

    fn of_double(x: f64) -> Num {
        if x.is_nan() {
            return Num::special(3);
        }
        if x == f64::INFINITY {
            return Num::special(1);
        }
        if x == f64::NEG_INFINITY {
            return Num::special(2);
        }
        // Shortest round-tripping decimal text is what makes `0.1` (DOUBLE)
        // equal `0.1` (DECIMAL) while a decimal no f64 round-trips stays
        // unequal to its double.
        let text = x.to_string();
        Num::from_text(&text)
    }

    /// Parse `[+-]digits[.digits][e[+-]digits]`; `f64`'s Display always emits
    /// this shape (`NaN`/`inf` are handled before the call).
    fn from_text(text: &str) -> Num {
        let (neg, body) = match text.as_bytes().first() {
            Some(b'-') => (true, &text[1..]),
            Some(b'+') => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exp) = match body.find(['e', 'E']) {
            Some(i) => (&body[..i], body[i + 1..].parse::<i32>().unwrap_or(0)),
            None => (body, 0),
        };
        let (int_part, frac_part) = match mantissa.find('.') {
            Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
            None => (mantissa, ""),
        };
        let digits: String = format!("{int_part}{frac_part}");
        let scale = frac_part.len() as i32 - exp;
        Num::from_parts(neg, digits, scale)
    }

    fn from_parts(neg: bool, digits: String, scale: i32) -> Num {
        let trimmed = digits.trim_start_matches('0');
        let mut scale = scale;
        let mut s = trimmed.trim_end_matches('0').to_string();
        if s.is_empty() {
            // Zero: one canonical form, signless.
            return Num {
                special: 0,
                neg: false,
                digits: Vec::new(),
                scale: 0,
            };
        }
        scale -= (trimmed.len() - s.len()) as i32;
        s.shrink_to_fit();
        Num {
            special: 0,
            neg,
            digits: s.into_bytes(),
            scale,
        }
    }

    fn special(kind: u8) -> Num {
        Num {
            special: kind,
            neg: false,
            digits: Vec::new(),
            scale: 0,
        }
    }

    fn is_zero(&self) -> bool {
        self.special == 0 && self.digits.is_empty()
    }

    /// Digits before the decimal point (may be ≤ 0 for values below 1).
    fn int_len(&self) -> i32 {
        self.digits.len() as i32 - self.scale
    }

    fn magnitude_cmp(&self, other: &Num) -> Ordering {
        if self.special != 0 || other.special != 0 {
            return self.special.cmp(&other.special);
        }
        if self.is_zero() && other.is_zero() {
            return Ordering::Equal;
        }
        if self.is_zero() {
            return Ordering::Less;
        }
        if other.is_zero() {
            return Ordering::Greater;
        }
        // Equal integer length ⇒ left-aligned digit comparison (missing
        // trailing digits are zeros).
        let (la, lb) = (self.int_len(), other.int_len());
        if la != lb {
            return la.cmp(&lb);
        }
        let n = self.digits.len().max(other.digits.len());
        for i in 0..n {
            let da = self.digits.get(i).copied().unwrap_or(b'0');
            let db = other.digits.get(i).copied().unwrap_or(b'0');
            match da.cmp(&db) {
                Ordering::Equal => {}
                o => return o,
            }
        }
        Ordering::Equal
    }

    fn cmp(&self, other: &Num) -> Ordering {
        // -0 and 0 are the same number, so compare magnitudes first and only
        // then let the signs decide.
        let mag = self.magnitude_cmp(other);
        if mag == Ordering::Equal {
            return Ordering::Equal;
        }
        if self.neg != other.neg {
            return if self.neg {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        if self.neg { mag.reverse() } else { mag }
    }

    fn write_key(&self, out: &mut Vec<u8>) {
        out.push(self.special);
        if self.special != 0 {
            return;
        }
        out.push(self.neg as u8);
        out.extend_from_slice(&self.int_len().to_le_bytes());
        out.extend_from_slice(&self.digits);
    }
}

// ---------------------------------------------------------------- encoding
//
// Little-endian, kind-tagged, self-describing per value — the same "type
// dictionary + value buffer" shape Parquet's VARIANT logical type uses, with
// the dictionary inlined per value until wave 101b hoists it per column.

const T_NULL: u8 = 0x00;
const T_BOOL: u8 = 0x01;
const T_INT: u8 = 0x02;
const T_DOUBLE: u8 = 0x03;
const T_DECIMAL: u8 = 0x04;
const T_STRING: u8 = 0x05;
const T_DATE: u8 = 0x06;
const T_TIMESTAMP: u8 = 0x07;
const T_OBJECT: u8 = 0x08;
const T_ARRAY: u8 = 0x09;

/// Append the binary encoding of `v` to `out`.
pub fn encode(v: &Variant, out: &mut Vec<u8>) {
    match v {
        Variant::Null => out.push(T_NULL),
        Variant::Bool(b) => {
            out.push(T_BOOL);
            out.push(*b as u8);
        }
        Variant::Int(i) => {
            out.push(T_INT);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Variant::Double(x) => {
            out.push(T_DOUBLE);
            out.extend_from_slice(&x.to_bits().to_le_bytes());
        }
        Variant::Decimal(unscaled, scale) => {
            out.push(T_DECIMAL);
            out.push(types::decimal_digits(*unscaled).min(255) as u8);
            out.push(*scale);
            out.extend_from_slice(&unscaled.to_le_bytes());
        }
        Variant::Str(s) => {
            out.push(T_STRING);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Variant::Date(d) => {
            out.push(T_DATE);
            out.extend_from_slice(&d.to_le_bytes());
        }
        Variant::Timestamp(us) => {
            out.push(T_TIMESTAMP);
            out.extend_from_slice(&us.to_le_bytes());
        }
        Variant::Object(fields) => {
            out.push(T_OBJECT);
            out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for (k, v) in fields.iter() {
                out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                out.extend_from_slice(k.as_bytes());
                encode(v, out);
            }
        }
        Variant::Array(items) => {
            out.push(T_ARRAY);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for it in items.iter() {
                encode(it, out);
            }
        }
    }
}

/// Decode one value from the front of `bytes`; returns it and the rest.
pub fn decode(bytes: &[u8]) -> Result<Variant> {
    let mut r = BinReader { bytes, pos: 0 };
    let v = decode_one(&mut r, 0)?;
    Ok(v)
}

struct BinReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BinReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(corrupt)?;
        if end > self.bytes.len() {
            return Err(corrupt());
        }
        let s = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i128(&mut self) -> Result<i128> {
        Ok(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<Arc<str>> {
        let n = self.u32()? as usize;
        let raw = self.take(n)?;
        std::str::from_utf8(raw)
            .map(Arc::from)
            .map_err(|_| PinkDbError::Execution("corrupt VARIANT: invalid UTF-8".to_string()))
    }
}

fn corrupt() -> PinkDbError {
    PinkDbError::Execution("corrupt VARIANT value".to_string())
}

fn decode_one(r: &mut BinReader<'_>, depth: usize) -> Result<Variant> {
    if depth > MAX_DEPTH {
        return Err(too_deep());
    }
    match r.u8()? {
        T_NULL => Ok(Variant::Null),
        T_BOOL => Ok(Variant::Bool(r.u8()? != 0)),
        T_INT => Ok(Variant::Int(r.i64()?)),
        T_DOUBLE => Ok(Variant::Double(f64::from_bits(u64::from_le_bytes(
            r.take(8)?.try_into().unwrap(),
        )))),
        T_DECIMAL => {
            // The digit count is informational (it rides the encoding so a
            // future DECIMAL type tag can be reconstructed); the scale and
            // the unscaled value are what the value needs.
            let _digits = r.u8()?;
            let scale = r.u8()?;
            Ok(Variant::Decimal(r.i128()?, scale))
        }
        T_STRING => Ok(Variant::Str(r.string()?)),
        T_DATE => Ok(Variant::Date(r.i32()?)),
        T_TIMESTAMP => Ok(Variant::Timestamp(r.i64()?)),
        T_OBJECT => {
            let n = r.u32()? as usize;
            // Each field costs ≥ 5 bytes, so the count cannot run away.
            if n > r.bytes.len() {
                return Err(corrupt());
            }
            let mut fields = Vec::with_capacity(n);
            let mut prev: Option<Arc<str>> = None;
            for _ in 0..n {
                let k = r.string()?;
                if let Some(p) = &prev
                    && p.as_ref() >= k.as_ref()
                {
                    return Err(PinkDbError::Execution(
                        "corrupt VARIANT: object keys are not sorted".to_string(),
                    ));
                }
                prev = Some(k.clone());
                fields.push((k, decode_one(r, depth + 1)?));
            }
            Ok(Variant::Object(fields.into()))
        }
        T_ARRAY => {
            let n = r.u32()? as usize;
            if n > r.bytes.len() {
                return Err(corrupt());
            }
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(decode_one(r, depth + 1)?);
            }
            Ok(Variant::Array(items.into()))
        }
        _ => Err(corrupt()),
    }
}

// ------------------------------------------------------------ json parsing

/// Parse JSON text into a VARIANT.
///
/// Deliberate differences from strict JSON, both so the text form is a total
/// round trip for every VARIANT: a bare `NaN` / `Infinity` / `-Infinity` is a
/// non-finite DOUBLE, and nesting deeper than [`MAX_DEPTH`] is an error rather
/// than a stack overflow.
///
/// Numbers: an integral literal that fits `i64` becomes `Int`; a larger
/// integral literal that fits 38 digits becomes `Decimal(q, 0)` (kept exact
/// rather than rounded through a double); everything else becomes `Double`.
pub fn from_json_str(text: &str) -> Result<Variant> {
    let mut p = JsonParser {
        bytes: text.as_bytes(),
        pos: 0,
    };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.pos != p.bytes.len() {
        return Err(bad_json(text, p.pos, "trailing characters"));
    }
    Ok(v)
}

fn bad_json(text: &str, pos: usize, why: &str) -> PinkDbError {
    let at = text.get(pos..).unwrap_or("");
    let shown = at.chars().take(20).collect::<String>();
    PinkDbError::Execution(format!("invalid JSON at byte {pos} ({why}): {shown:?}"))
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl JsonParser<'_> {
    fn ws(&mut self) {
        while matches!(self.bytes.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn eat(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, b: u8, text: &str) -> Result<()> {
        if self.eat(b) {
            Ok(())
        } else {
            Err(bad_json(
                text,
                self.pos,
                &format!("expected {:?}", b as char),
            ))
        }
    }

    fn literal(&mut self, word: &str, v: Variant) -> Result<Variant> {
        if self.bytes[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(v)
        } else {
            Err(bad_json(
                std::str::from_utf8(self.bytes).unwrap_or(""),
                self.pos,
                "unknown literal",
            ))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Variant> {
        if depth > MAX_DEPTH {
            return Err(too_deep());
        }
        let text = std::str::from_utf8(self.bytes).unwrap_or("");
        match self.peek() {
            None => Err(bad_json(text, self.pos, "unexpected end of input")),
            Some(b'n') => self.literal("null", Variant::Null),
            Some(b't') => self.literal("true", Variant::Bool(true)),
            Some(b'f') => self.literal("false", Variant::Bool(false)),
            Some(b'N') => self.literal("NaN", Variant::Double(f64::NAN)),
            Some(b'I') => self.literal("Infinity", Variant::Double(f64::INFINITY)),
            Some(b'"') => Ok(Variant::Str(self.string()?)),
            Some(b'[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.ws();
                if self.eat(b']') {
                    return Ok(Variant::Array(items.into()));
                }
                loop {
                    self.ws();
                    items.push(self.value(depth + 1)?);
                    self.ws();
                    if self.eat(b',') {
                        continue;
                    }
                    self.expect(b']', text)?;
                    break;
                }
                Ok(Variant::Array(items.into()))
            }
            Some(b'{') => {
                self.pos += 1;
                let mut fields: Vec<(Arc<str>, Variant)> = Vec::new();
                self.ws();
                if self.eat(b'}') {
                    return Ok(Variant::Object(fields.into()));
                }
                loop {
                    self.ws();
                    let k = self.string()?;
                    self.ws();
                    self.expect(b':', text)?;
                    self.ws();
                    let v = self.value(depth + 1)?;
                    fields.push((k, v));
                    self.ws();
                    if self.eat(b',') {
                        continue;
                    }
                    self.expect(b'}', text)?;
                    break;
                }
                // Sorts and de-duplicates (last key wins).
                Variant::object(fields)
            }
            Some(b'-') if self.bytes[self.pos..].starts_with(b"-Infinity") => {
                self.literal("-Infinity", Variant::Double(f64::NEG_INFINITY))
            }
            Some(_) => self.number(text),
        }
    }

    fn number(&mut self, text: &str) -> Result<Variant> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        let int_start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        let int_digits = &text[int_start..self.pos];
        if int_digits.is_empty() {
            return Err(bad_json(text, start, "not a number"));
        }
        let mut frac_digits = "";
        let mut fractional = false;
        if self.eat(b'.') {
            fractional = true;
            let s = self.pos;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
            frac_digits = &text[s..self.pos];
            if frac_digits.is_empty() {
                return Err(bad_json(text, start, "not a number"));
            }
        }
        let mut exponent = 0i32;
        if matches!(self.peek(), Some(b'e' | b'E')) {
            fractional = true;
            self.pos += 1;
            let sign = if self.eat(b'-') {
                -1
            } else {
                self.eat(b'+');
                1
            };
            let s = self.pos;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
            if s == self.pos {
                return Err(bad_json(text, start, "not a number"));
            }
            exponent = text[s..self.pos].parse::<i32>().unwrap_or(0) * sign;
        }
        let literal = &text[start..self.pos];
        if !fractional {
            if let Ok(i) = literal.parse::<i64>() {
                return Ok(Variant::Int(i));
            }
            // Keep a big integer exact while it fits DECIMAL(38, 0).
            if let Ok(u) = literal.parse::<i128>()
                && types::decimal_fits(u, 38)
            {
                return Ok(Variant::Decimal(u, 0));
            }
        } else {
            // A fraction with no exponent that fits DECIMAL(38, s) stays
            // exact, so a decimal written as JSON text keeps its scale.
            let digits = format!("{int_digits}{frac_digits}");
            let scale = frac_digits.len() as i32 - exponent;
            if (0..=38).contains(&scale)
                && digits.trim_start_matches('0').len() as i32 <= 38
                && let Ok(unscaled) = digits.parse::<i128>()
            {
                let unscaled = if literal.starts_with('-') {
                    -unscaled
                } else {
                    unscaled
                };
                if types::decimal_fits(unscaled, 38) {
                    return Ok(Variant::Decimal(unscaled, scale as u8));
                }
            }
        }
        literal
            .parse::<f64>()
            .map(Variant::Double)
            .map_err(|_| bad_json(text, start, "not a number"))
    }

    fn string(&mut self) -> Result<Arc<str>> {
        let text = std::str::from_utf8(self.bytes).unwrap_or("");
        self.expect(b'"', text)?;
        let mut out = String::new();
        loop {
            let Some(b) = self.peek() else {
                return Err(bad_json(text, self.pos, "unterminated string"));
            };
            self.pos += 1;
            match b {
                b'"' => break,
                // JSON forbids raw control bytes in a string; the writer
                // escapes them, so a strict reader keeps round trips exact.
                b if b < 0x20 => {
                    return Err(bad_json(text, self.pos - 1, "control character in string"));
                }
                b'\\' => {
                    let Some(e) = self.peek() else {
                        return Err(bad_json(text, self.pos, "unterminated escape"));
                    };
                    self.pos += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4(text)?;
                            let ch = if (0xD800..0xDC00).contains(&hi) {
                                // A high surrogate must be followed by a low
                                // one; anything else is malformed UTF-16.
                                if !(self.bytes[self.pos..].starts_with(b"\\u")) {
                                    return Err(bad_json(text, self.pos, "lone surrogate"));
                                }
                                self.pos += 2;
                                let lo = self.hex4(text)?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(bad_json(text, self.pos, "lone surrogate"));
                                }
                                let c =
                                    0x10000 + (((hi - 0xD800) as u32) << 10) + (lo - 0xDC00) as u32;
                                char::from_u32(c)
                                    .ok_or_else(|| bad_json(text, self.pos, "bad surrogate"))?
                            } else {
                                char::from_u32(hi as u32)
                                    .ok_or_else(|| bad_json(text, self.pos, "bad escape"))?
                            };
                            out.push(ch);
                        }
                        _ => return Err(bad_json(text, self.pos, "bad escape")),
                    }
                }
                _ => {
                    // Copy the raw UTF-8 bytes of one scalar.
                    let start = self.pos - 1;
                    let len = match b {
                        0x00..=0x7F => 0,
                        0xC0..=0xDF => 1,
                        0xE0..=0xEF => 2,
                        0xF0..=0xF7 => 3,
                        _ => return Err(bad_json(text, start, "invalid UTF-8")),
                    };
                    self.pos += len;
                    let raw = self
                        .bytes
                        .get(start..self.pos)
                        .ok_or_else(|| bad_json(text, start, "invalid UTF-8"))?;
                    out.push_str(
                        std::str::from_utf8(raw)
                            .map_err(|_| bad_json(text, start, "invalid UTF-8"))?,
                    );
                }
            }
        }
        Ok(out.into())
    }

    fn hex4(&mut self, text: &str) -> Result<u16> {
        let s = self
            .bytes
            .get(self.pos..self.pos + 4)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| u16::from_str_radix(s, 16).ok())
            .ok_or_else(|| bad_json(text, self.pos, "bad \\u escape"))?;
        self.pos += 4;
        Ok(s)
    }
}

// -------------------------------------------------------------------- paths

/// One step of a VARIANT path.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Key(Arc<str>),
    Index(usize),
}

/// The statistics key of a path, in the spelling DuckLake's
/// `ducklake_file_variant_stats.variant_path` uses: every key double-quoted,
/// joined by `.` (`"a"`, `"nested"."k"`), with an embedded `"` doubled. The
/// writer and the pruning reader spell a path the same way, so a predicate
/// and a statistic agree by construction.
///
/// Only object keys are spelled: the shredding policy is shallow, so no
/// published path crosses an array. `None` for a path no engine can name — an
/// empty key, or one holding a backslash (DuckDB's own struct-type spelling of
/// a shredded key cannot parse either, so such a key is never shredded) — and
/// those paths get no statistics.
pub fn stats_path_text(steps: &[Step]) -> Option<String> {
    let mut out = String::new();
    for step in steps {
        let Step::Key(key) = step else {
            return None;
        };
        if key.is_empty() || key.contains('\\') {
            return None;
        }
        if !out.is_empty() {
            out.push('.');
        }
        out.push('"');
        for ch in key.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
    }
    (!out.is_empty()).then_some(out)
}

/// Parse a path: `a`, `a.b[1]`, `$.a[0]`, `$['a b'][2]`.
///
/// DuckDB 2.0 spells extraction three ways (`v['a']`, `v.a`,
/// `variant_extract(v, 'a')` — a field name, not a path); pinkdb's grammar has
/// no subscript operator, so `variant_extract` accepts the JSONPath spelling
/// and this parser is its whole surface.
pub fn parse_path(path: &str) -> Result<Vec<Step>> {
    let mut rest = path.trim();
    if let Some(r) = rest.strip_prefix('$') {
        rest = r;
    }
    let mut steps = Vec::new();
    let bad = || PinkDbError::Execution(format!("invalid VARIANT path {path:?}"));
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            // `$."x y"` quotes a key, as DuckDB's path grammar does.
            if let Some(r) = r.strip_prefix('"') {
                let (key, r) = r.split_once('"').ok_or_else(bad)?;
                steps.push(Step::Key(Arc::from(key)));
                rest = r;
                continue;
            }
            let end = r.find(['.', '[']).unwrap_or(r.len());
            if end == 0 {
                return Err(bad());
            }
            steps.push(Step::Key(Arc::from(&r[..end])));
            rest = &r[end..];
        } else if let Some(r) = rest.strip_prefix('[') {
            if let Some(r) = r.strip_prefix('\'') {
                let (key, r) = r.split_once('\'').ok_or_else(bad)?;
                let r = r.strip_prefix(']').ok_or_else(bad)?;
                steps.push(Step::Key(Arc::from(key)));
                rest = r;
            } else {
                let (idx, r) = r.split_once(']').ok_or_else(bad)?;
                steps.push(Step::Index(idx.parse::<usize>().map_err(|_| bad())?));
                rest = r;
            }
        } else if steps.is_empty() {
            // A bare leading field name (`a.b[1]`).
            let end = rest.find(['.', '[']).unwrap_or(rest.len());
            steps.push(Step::Key(Arc::from(&rest[..end])));
            rest = &rest[end..];
        } else {
            return Err(bad());
        }
    }
    if steps.is_empty() {
        // `$` alone is the whole document; an empty path is a mistake.
        return if path.trim() == "$" {
            Ok(steps)
        } else {
            Err(bad())
        };
    }
    Ok(steps)
}

/// Walk `steps` down `v`. `None` means "no such path" (the caller yields SQL
/// NULL); a wrong kind part-way is also just a miss, like DuckDB's extract.
pub fn walk<'a>(v: &'a Variant, steps: &[Step]) -> Option<&'a Variant> {
    let mut cur = v;
    for step in steps {
        cur = match (step, cur) {
            (Step::Key(k), Variant::Object(fields)) => &fields.iter().find(|(f, _)| f == k)?.1,
            (Step::Index(i), Variant::Array(items)) => items.get(*i)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// How [`set_path`] treats a path that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetMode {
    /// `json_set`: assign, creating missing parents.
    Set,
    /// `json_insert`: assign only when the path is missing.
    Insert,
    /// `json_replace`: assign only when the path exists.
    Replace,
}

/// `json_set` / `json_insert` / `json_replace`: `root` with `value` at `steps`.
///
/// An empty `steps` is the document itself (`$`). A step into a scalar, or an
/// array index past the end, leaves the document unchanged — the silent no-op
/// DuckDB makes; only an existing (`Insert`) or missing (`Replace`) path
/// behaves differently.
pub fn set_path(root: &Variant, steps: &[Step], value: &Variant, mode: SetMode) -> Result<Variant> {
    assign(root, steps, value, mode)
}

fn assign(cur: &Variant, steps: &[Step], value: &Variant, mode: SetMode) -> Result<Variant> {
    let Some((step, rest)) = steps.split_first() else {
        return Ok(match mode {
            SetMode::Insert => cur.clone(),
            SetMode::Set | SetMode::Replace => value.clone(),
        });
    };
    match (step, cur) {
        (Step::Key(k), Variant::Object(fields)) => {
            let mut out: Vec<(Arc<str>, Variant)> =
                fields.iter().map(|(f, v)| (f.clone(), v.clone())).collect();
            match out.iter().position(|(f, _)| f == k) {
                Some(pos) => {
                    let child = out[pos].1.clone();
                    out[pos].1 = assign(&child, rest, value, mode)?;
                }
                None if mode == SetMode::Replace => return Ok(cur.clone()),
                None => out.push((k.clone(), build(rest, value)?)),
            }
            Variant::object(out)
        }
        (Step::Index(i), Variant::Array(items)) => {
            let mut out: Vec<Variant> = items.to_vec();
            match out.get(*i) {
                Some(_) => {
                    let child = out[*i].clone();
                    out[*i] = assign(&child, rest, value, mode)?;
                }
                None if *i == out.len() && mode != SetMode::Replace => {
                    out.push(build(rest, value)?)
                }
                None => return Ok(cur.clone()),
            }
            Variant::array(out)
        }
        _ => Ok(cur.clone()),
    }
}

/// The container chain for a path that does not exist yet: a key creates an
/// object, an index an array holding the value at slot 0.
fn build(steps: &[Step], value: &Variant) -> Result<Variant> {
    let Some((step, rest)) = steps.split_first() else {
        return Ok(value.clone());
    };
    match step {
        Step::Key(k) => Variant::object(vec![(k.clone(), build(rest, value)?)]),
        Step::Index(_) => Variant::array(vec![build(rest, value)?]),
    }
}

/// `json_remove`: `root` without the value at `steps`. A missing path leaves
/// it unchanged; an empty `steps` removes the document itself (`$`).
pub fn remove_path(root: &Variant, steps: &[Step]) -> Result<Variant> {
    let Some((step, rest)) = steps.split_first() else {
        return Ok(Variant::Null);
    };
    match (step, root) {
        (Step::Key(k), Variant::Object(fields)) => {
            let mut out: Vec<(Arc<str>, Variant)> = Vec::with_capacity(fields.len());
            for (f, v) in fields.iter() {
                if f == k {
                    if rest.is_empty() {
                        continue;
                    }
                    out.push((f.clone(), remove_path(v, rest)?));
                } else {
                    out.push((f.clone(), v.clone()));
                }
            }
            Variant::object(out)
        }
        (Step::Index(i), Variant::Array(items)) => {
            let mut out: Vec<Variant> = Vec::with_capacity(items.len());
            for (j, v) in items.iter().enumerate() {
                if j == *i {
                    if rest.is_empty() {
                        continue;
                    }
                    out.push(remove_path(v, rest)?);
                } else {
                    out.push(v.clone());
                }
            }
            Variant::array(out)
        }
        _ => Ok(root.clone()),
    }
}

/// `variant_keys`: the object's keys as a JSON array (pinkdb has no LIST
/// type). A non-object yields `[]`, as DuckDB does.
pub fn keys_json(v: &Variant) -> String {
    let mut out = String::from("[");
    if let Variant::Object(fields) = v {
        for (i, (k, _)) in fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_json_str(k, &mut out);
        }
    }
    out.push(']');
    out
}

// -------------------------------------------------------------------- casts

/// VARIANT → a stored type. `VARCHAR` is handled by the caller's generic
/// "everything formats to text" arm (Display is JSON), so it is not here.
pub fn cast(v: &Variant, target: DataType) -> Result<Value> {
    let err = |what: &str| {
        PinkDbError::Execution(format!(
            "cannot CAST VARIANT({}) {what} to {target}",
            v.type_name()
        ))
    };
    match target {
        DataType::Variant => Ok(Value::Variant(Arc::new(v.clone()))),
        // The text form is the JSON encoding (the only spelling a VARCHAR can
        // carry back through `from_value`), so the JSON null keeps its `null`
        // text: the text round trip stays lossless for JSON's value model.
        DataType::Varchar => Ok(Value::Varchar(v.to_json())),
        // Every other target reads a variant-null as SQL NULL, like DuckDB
        // (`json('null')::VARIANT IS NULL` is false; `::INTEGER` is NULL).
        _ if matches!(v, Variant::Null) => Ok(Value::Null),
        // A variant string converts like the VARCHAR it holds: `'"5"'`
        // reaches INTEGER as 5 (DuckDB does the same).
        DataType::Boolean => match v {
            Variant::Bool(b) => Ok(Value::Boolean(*b)),
            Variant::Str(s) => match s.to_ascii_lowercase().as_str() {
                "true" => Ok(Value::Boolean(true)),
                "false" => Ok(Value::Boolean(false)),
                _ => Err(err(&format!("value {v}"))),
            },
            _ => Err(err(&format!("value {v}"))),
        },
        DataType::Integer | DataType::Bigint => {
            let n = match v {
                Variant::Int(i) => *i,
                // A decimal keeps its integer part only when it has none.
                Variant::Decimal(unscaled, scale) => {
                    let rescaled = rescale_decimal(*unscaled, *scale, 0)
                        .ok_or_else(|| err(&format!("value {v} (not an exact integer)")))?;
                    i64::try_from(rescaled)
                        .map_err(|_| err(&format!("value {v} (out of range)")))?
                }
                Variant::Double(x) => {
                    let r = x.round();
                    if !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&r) {
                        return Err(err(&format!("value {v} (out of range)")));
                    }
                    r as i64
                }
                Variant::Str(s) => match s.trim().parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return Err(err(&format!("value {v}"))),
                },
                _ => return Err(err(&format!("value {v}"))),
            };
            if target == DataType::Integer {
                i32::try_from(n).map(Value::Integer).map_err(|_| {
                    PinkDbError::Execution(format!("cannot CAST VARIANT(BIGINT) {n} to INTEGER"))
                })
            } else {
                Ok(Value::Bigint(n))
            }
        }
        DataType::Double => match v {
            Variant::Int(i) => Ok(Value::Double(*i as f64)),
            Variant::Double(x) => Ok(Value::Double(*x)),
            Variant::Decimal(unscaled, scale) => {
                Ok(Value::Double(decimal_to_f64(*unscaled, *scale)))
            }
            Variant::Str(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Double)
                .map_err(|_| err(&format!("value {v}"))),
            _ => Err(err(&format!("value {v}"))),
        },
        DataType::Decimal { precision, scale } => {
            let unscaled = match v {
                Variant::Int(i) => types::int_to_decimal(*i as i128, precision, scale),
                Variant::Decimal(u, s) => {
                    rescale_decimal(*u, *s, scale).filter(|x| types::decimal_fits(*x, precision))
                }
                Variant::Double(x) => types::f64_to_decimal(*x, precision, scale),
                Variant::Str(s) => types::parse_decimal_str(s, precision, scale).ok(),
                _ => None,
            };
            unscaled.map(|u| Value::Decimal(u, scale)).ok_or_else(|| {
                PinkDbError::Execution(format!(
                    "cannot CAST VARIANT({}) value {v} to DECIMAL({precision},{scale})",
                    v.type_name()
                ))
            })
        }
        DataType::Date => match v {
            Variant::Date(d) => Ok(Value::Date(*d)),
            Variant::Str(s) => parse_date(s).map(Value::Date).ok_or_else(|| {
                PinkDbError::Execution(format!("cannot CAST VARIANT(VARCHAR) \"{s}\" to DATE"))
            }),
            _ => Err(err(&format!("value {v}"))),
        },
        DataType::Timestamp => match v {
            Variant::Timestamp(us) => Ok(Value::Timestamp(*us)),
            Variant::Date(d) => Ok(Value::Timestamp(*d as i64 * 86_400_000_000)),
            Variant::Str(s) => parse_timestamp(s).map(Value::Timestamp).ok_or_else(|| {
                PinkDbError::Execution(format!("cannot CAST VARIANT(VARCHAR) \"{s}\" to TIMESTAMP"))
            }),
            _ => Err(err(&format!("value {v}"))),
        },
        // INTERVAL and anything a later wave adds: no VARIANT kind exists for
        // it, so the cast has no answer.
        _ => Err(err(&format!("value {v}"))),
    }
}

/// Any engine value as a VARIANT (the `x → VARIANT` cast).
///
/// VARCHAR parses as JSON text — the one cast that is not a plain widening,
/// and the reason the text round trip is lossless for JSON's value model. A
/// string that is not JSON is an error (DuckDB would keep it as a string;
/// see the plan's difference table).
pub fn from_value(v: &Value) -> Result<Variant> {
    Ok(match v {
        // Callers drop NULL rows before casting; a bare NULL is JSON null.
        Value::Null => Variant::Null,
        Value::Boolean(b) => Variant::Bool(*b),
        Value::Integer(i) => Variant::Int(*i as i64),
        Value::Bigint(i) => Variant::Int(*i),
        Value::Double(x) => Variant::Double(*x),
        Value::Decimal(x, scale) => Variant::Decimal(*x, *scale),
        Value::Varchar(s) => {
            return from_json_str(s).map_err(|e| {
                PinkDbError::Execution(format!("cannot CAST VARCHAR to VARIANT: {e}"))
            });
        }
        Value::Date(d) => Variant::Date(*d),
        Value::Timestamp(us) => Variant::Timestamp(*us),
        // VARIANT has no interval kind; its canonical text is what DuckDB
        // reports for an interval in a variant too.
        Value::Interval {
            months,
            days,
            micros,
        } => Variant::Str(Arc::from(
            types::format_interval(*months, *days, *micros).as_str(),
        )),
        Value::Variant(x) => (**x).clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_paths_use_the_ducklake_spelling() {
        let key = |k: &str| Step::Key(Arc::from(k));
        assert_eq!(stats_path_text(&[key("a")]).as_deref(), Some("\"a\""));
        assert_eq!(
            stats_path_text(&[key("a b"), key("k")]).as_deref(),
            Some("\"a b\".\"k\"")
        );
        assert_eq!(
            stats_path_text(&[key("a\"b")]).as_deref(),
            Some("\"a\"\"b\"")
        );
        assert_eq!(stats_path_text(&[]), None);
        assert_eq!(stats_path_text(&[key("")]), None);
        assert_eq!(stats_path_text(&[key("a\\b")]), None);
        assert_eq!(stats_path_text(&[Step::Index(0)]), None);
    }

    fn round_trip(v: &Variant) {
        let mut bytes = Vec::new();
        encode(v, &mut bytes);
        let back = decode(&bytes).unwrap_or_else(|e| panic!("{v}: {e}"));
        let mut again = Vec::new();
        encode(&back, &mut again);
        assert_eq!(bytes, again, "encode(decode(x)) != encode(x) for {v}");
        assert_eq!(cmp(v, &back), Ordering::Equal, "{v} != {back}");
    }

    #[test]
    fn every_kind_round_trips_through_the_binary_encoding() {
        let s = |t: &str| Variant::Str(Arc::from(t));
        let values = [
            Variant::Null,
            Variant::Bool(true),
            Variant::Bool(false),
            Variant::Int(0),
            Variant::Int(-1),
            Variant::Int(i64::MIN),
            Variant::Int(i64::MAX),
            Variant::Double(0.0),
            Variant::Double(-1.5),
            Variant::Double(f64::NAN),
            Variant::Double(f64::INFINITY),
            Variant::Decimal(0, 0),
            Variant::Decimal(-12345, 2),
            Variant::Decimal(i128::MIN + 1, 38),
            s(""),
            s("héllo 世界 🚀"),
            Variant::Date(-1),
            Variant::Timestamp(i64::MAX),
            Variant::Array(Arc::from(vec![Variant::Int(1), Variant::Null])),
            Variant::Object(Arc::from(vec![
                (Arc::from("a"), Variant::Int(1)),
                (Arc::from("b"), Variant::Array(Arc::from(vec![]))),
            ])),
        ];
        for v in &values {
            round_trip(v);
        }
        // Deep-but-legal nesting survives; the reader's depth counter is the
        // same as the writer's.
        let mut deep = Variant::Int(1);
        for _ in 0..MAX_DEPTH {
            deep = Variant::Array(Arc::from(vec![deep]));
        }
        round_trip(&deep);
        let too_deep = Variant::Array(Arc::from(vec![deep]));
        let mut bytes = Vec::new();
        encode(&too_deep, &mut bytes);
        assert!(decode(&bytes).unwrap_err().to_string().contains("nested"));
    }

    #[test]
    fn corrupt_bytes_are_errors_not_panics() {
        assert_eq!(
            decode(&[0x7f]).unwrap_err().to_string(),
            "corrupt VARIANT value"
        );
        assert_eq!(
            decode(&[]).unwrap_err().to_string(),
            "corrupt VARIANT value"
        );
        // A T_INT with three bytes of payload.
        assert_eq!(
            decode(&[T_INT, 1, 2, 3]).unwrap_err().to_string(),
            "corrupt VARIANT value"
        );
        // An object whose key points past the buffer.
        let mut bytes = vec![T_OBJECT, 1, 0, 0, 0, 99, 0, 0, 0];
        bytes.extend_from_slice(&[T_NULL]);
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn keys_are_sorted_and_duplicates_keep_the_last_value() {
        let v = from_json_str(r#"{"b":1,"a":2,"b":3}"#).unwrap();
        assert_eq!(v.to_json(), r#"{"a":2,"b":3}"#);
        let Variant::Object(entries) = &v else {
            panic!("{v}");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(keys_json(&v), r#"["a","b"]"#);
        // Non-objects have no keys. (`variant_keys` answers SQL NULL for the
        // JSON null before it gets here, as DuckDB does.)
        assert_eq!(keys_json(&Variant::Int(1)), "[]");
        assert_eq!(keys_json(&Variant::Null), "[]");
        assert_eq!(keys_json(&Variant::Array(Arc::from(vec![]))), "[]");
    }

    #[test]
    fn numeric_equality_and_ordering_cross_kinds() {
        let one = from_json_str("1").unwrap();
        let one_dot = Variant::Double(1.0);
        let one_dec = Variant::Decimal(100, 2);
        assert_eq!(cmp(&one, &one_dot), Ordering::Equal);
        assert_eq!(cmp(&one, &one_dec), Ordering::Equal);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        hash_key(&one, &mut a);
        hash_key(&one_dot, &mut b);
        assert_eq!(a, b);
        // Kind order: Bool < number < string < date < timestamp < array <
        // object < JSON null.
        let ordered = [
            Variant::Bool(true),
            Variant::Int(1),
            Variant::Str(Arc::from("s")),
            Variant::Date(0),
            Variant::Timestamp(0),
            Variant::Array(Arc::from(vec![])),
            Variant::Object(Arc::from(vec![])),
            Variant::Null,
        ];
        for pair in ordered.windows(2) {
            assert_eq!(cmp(&pair[0], &pair[1]), Ordering::Less, "{:?}", pair);
        }
        // Strings compare bytewise, arrays lexicographically, objects by key
        // then value.
        let a = from_json_str(r#"["a",2]"#).unwrap();
        let b = from_json_str(r#"["a",10]"#).unwrap();
        assert_eq!(cmp(&a, &b), Ordering::Less);
        let a = from_json_str(r#"{"a":1}"#).unwrap();
        let b = from_json_str(r#"{"a":1,"b":0}"#).unwrap();
        assert_eq!(cmp(&a, &b), Ordering::Less);
    }

    #[test]
    fn paths_parse_in_both_spellings() {
        let steps = parse_path("a.b[1]").unwrap();
        let want = [
            Step::Key(Arc::from("a")),
            Step::Key(Arc::from("b")),
            Step::Index(1),
        ];
        assert_eq!(steps, want);
        assert_eq!(parse_path("$.a.b[1]").unwrap(), want);
        assert_eq!(parse_path("['a']").unwrap(), [Step::Key(Arc::from("a"))]);
        // Indices are 0-based (JSONPath), unlike DuckDB's 1-based `v[i]`.
        let v = from_json_str(r#"{"a":{"b":[10,20]}}"#).unwrap();
        assert_eq!(walk(&v, &steps).unwrap(), &Variant::Int(20));
        assert!(walk(&v, &parse_path("a.b[2]").unwrap()).is_none());
        assert!(walk(&v, &parse_path("a.c").unwrap()).is_none());
        // A bare `$` is the whole document: no steps, which the JSON mutators
        // read as "replace/remove the document itself".
        assert!(parse_path("$").unwrap().is_empty());
        for bad in ["", "a..b", "a[", "a[x]", "[1]x"] {
            assert!(parse_path(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn json_errors_are_positioned() {
        for (input, want) in [
            ("", "invalid JSON at byte 0 (unexpected end of input)"),
            ("nope", "invalid JSON at byte 0 (unknown literal)"),
            ("1 2", "invalid JSON at byte 2 (trailing characters)"),
            ("\"a", "invalid JSON at byte 2 (unterminated string)"),
            ("[1,]", "invalid JSON at byte 3 (not a number)"),
            ("{\"a\" 1}", "invalid JSON at byte 5 (expected ':')"),
        ] {
            let err = from_json_str(input).unwrap_err().to_string();
            assert!(err.starts_with(want), "{input}: {err}");
        }
        assert!(from_json_str("\"\u{1}\"").is_err());
    }

    #[test]
    fn only_json_values_are_a_lossless_text_round_trip() {
        assert!(
            from_json_str(r#"{"a":[1,2.5,"s",null,true]}"#)
                .unwrap()
                .is_json_exact()
        );
        // DATE and TIMESTAMP have no JSON kind: they print as ISO strings and
        // read back as VARCHAR.
        let dated = from_json_str(r#"{"d":"2020-01-02"}"#).unwrap();
        assert!(dated.is_json_exact());
        assert!(!Variant::Date(18263).is_json_exact());
        assert!(
            !Variant::Object(Arc::from(vec![(Arc::from("d"), Variant::Date(18263))]))
                .is_json_exact()
        );
        // The text form of a DATE parses back as the same string.
        assert_eq!(
            from_json_str(&Variant::Date(18263).to_json()).unwrap(),
            Variant::Str(Arc::from("2020-01-02"))
        );
    }
}
