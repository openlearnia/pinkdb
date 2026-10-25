//! Parquet's `Variant` binary encoding and shredding (wave 101b).
//!
//! `docs/wave-101-plan.md` asks for the Parquet `Variant` logical type: a
//! metadata dictionary plus value buffers, shredded into typed sub-columns.
//! The bytes here are the ones `parquet-format`'s `VariantEncoding.md`
//! specifies, because interop is the point: a DuckDB 2.0 file must read back
//! here, and a file written here must read back in DuckDB.
//!
//! # Why not `parquet-variant`
//!
//! The crate that implements this encoding depends on `arrow`. This module is
//! ~600 lines against [`Variant`], which is already the shape Parquet wants
//! (objects key-sorted, numbers one width), so the dependency would buy
//! nothing but a large compile unit in every build that reads parquet.
//!
//! # The two dictionaries
//!
//! [`crate::variant::encode`] — pinkdb's own `.pink`/WAL encoding — inlines a
//! key dictionary in every value. Parquet's encoding has the dictionary in a
//! *separate column*, shared by every row of a row group: one
//! [`encode_metadata`] blob per column, and object field ids that index into
//! it. That is the "shared per-column key dictionary" the plan asks for.
//!
//! # Shredding
//!
//! A shredded variant column is a group of `metadata` (REQUIRED), `value`
//! (OPTIONAL) and `typed_value` (OPTIONAL). `typed_value` mirrors the object
//! keys the policy shredded, one group per key, each with the spec's
//! `(value, typed_value)` pair. The validity invariants this module keeps are
//! the spec's:
//!
//! - a SQL NULL row has no `metadata` and neither buffer;
//! - at a shredded path *at most one* of `value`/`typed_value` is set;
//! - the residual `value` object never repeats a shredded key, and is omitted
//!   when nothing is left over.
//!
//! The writer keeps one rule of its own on top of the spec, so a round trip
//! is exact: a shredded field reaches its typed column only when the row
//! holds a non-NULL value of the policy's kind. A present-but-null field, or
//! one of another kind, stays in the residual `value` object. The spec's
//! reference shredder writes a typed `NULL` for both, which makes `{}` and
//! `{"a": null}` encode identically; putting the field back in the residual
//! keeps them apart and still satisfies the invariants above.

use std::sync::Arc;

use crate::types::{PinkDbError, Result};
use crate::variant::Variant;

/// The typed column kinds a shredded field may use: the primitive types the
/// spec lists as valid shredded targets, reduced to the kinds pinkdb has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShredKind {
    Bool,
    Int64,
    Double,
    Str,
    Date,
    Timestamp,
}

impl ShredKind {
    /// The kind a value can be shredded as, or `None` for values that must
    /// stay in the residual (`Variant::Null` included: see the module note).
    pub fn of(value: &Variant) -> Option<ShredKind> {
        match value {
            Variant::Bool(_) => Some(ShredKind::Bool),
            Variant::Int(_) => Some(ShredKind::Int64),
            Variant::Double(_) => Some(ShredKind::Double),
            Variant::Str(_) => Some(ShredKind::Str),
            Variant::Date(_) => Some(ShredKind::Date),
            Variant::Timestamp(_) => Some(ShredKind::Timestamp),
            Variant::Null | Variant::Decimal(..) | Variant::Array(_) | Variant::Object(_) => None,
        }
    }

    /// The kind name, for error messages and the documented policy.
    pub fn name(self) -> &'static str {
        match self {
            ShredKind::Bool => "BOOLEAN",
            ShredKind::Int64 => "BIGINT",
            ShredKind::Double => "DOUBLE",
            ShredKind::Str => "VARCHAR",
            ShredKind::Date => "DATE",
            ShredKind::Timestamp => "TIMESTAMP",
        }
    }

    /// The kind of a value read back from a typed column, which is exactly
    /// the inverse of [`Self::of`].
    pub fn value(self, value: &crate::types::Value) -> Option<Variant> {
        use crate::types::Value;
        Some(match (self, value) {
            (ShredKind::Bool, Value::Boolean(b)) => Variant::Bool(*b),
            (ShredKind::Int64, Value::Bigint(i)) => Variant::Int(*i),
            (ShredKind::Double, Value::Double(x)) => Variant::Double(*x),
            (ShredKind::Str, Value::Varchar(s)) => Variant::Str(Arc::from(s.as_str())),
            (ShredKind::Date, Value::Date(d)) => Variant::Date(*d),
            (ShredKind::Timestamp, Value::Timestamp(us)) => Variant::Timestamp(*us),
            _ => return None,
        })
    }

    /// The type name this kind's path statistics are published under, in the
    /// spelling DuckDB 2.0 uses for the three kinds it writes itself
    /// (`boolean`, `varchar`, `float64`); the other three are pinkdb's own
    /// names in the same style, and DuckDB ignores rows whose type it does not
    /// know.
    pub fn stats_type(self) -> &'static str {
        match self {
            ShredKind::Bool => "boolean",
            ShredKind::Int64 => "int64",
            ShredKind::Double => "float64",
            ShredKind::Str => "varchar",
            ShredKind::Date => "date",
            ShredKind::Timestamp => "timestamp",
        }
    }

    /// The kind a `shredded_type` name denotes, or `None` for a name this
    /// engine does not write (another engine's kind, or garbage).
    pub fn from_stats_type(name: &str) -> Option<ShredKind> {
        Some(match name {
            "boolean" => ShredKind::Bool,
            "int64" => ShredKind::Int64,
            "float64" => ShredKind::Double,
            "varchar" => ShredKind::Str,
            "date" => ShredKind::Date,
            "timestamp" => ShredKind::Timestamp,
            _ => return None,
        })
    }
}

/// One bound of a path's statistics as catalog text, matching the plain-text
/// values DuckDB 2.0 writes into `min_value`/`max_value` of
/// `ducklake_file_variant_stats` (`1`, `0.0`, `x`, `2020-01-01`).
///
/// `None` when the value is not of `kind`. Non-finite doubles spell out
/// (`NaN`, `Infinity`) so the exact bound survives the round trip.
pub fn stats_bound_text(kind: ShredKind, value: &Variant) -> Option<String> {
    if ShredKind::of(value)? != kind {
        return None;
    }
    Some(match value {
        Variant::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Variant::Int(i) => i.to_string(),
        Variant::Double(x) if x.is_nan() => "NaN".to_string(),
        Variant::Double(x) if x.is_infinite() && x.is_sign_negative() => "-Infinity".to_string(),
        Variant::Double(x) if x.is_infinite() => "Infinity".to_string(),
        Variant::Double(x) => x.to_string(),
        Variant::Str(s) => s.to_string(),
        Variant::Date(days) => crate::types::format_date(*days),
        Variant::Timestamp(us) => crate::types::format_timestamp(*us),
        _ => return None,
    })
}

/// The inverse of [`stats_bound_text`]: the bound a catalog row publishes, or
/// `None` when the text is not a value of that kind.
pub fn stats_bound(kind_name: &str, text: &str) -> Option<Variant> {
    let kind = ShredKind::from_stats_type(kind_name)?;
    let value = match kind {
        ShredKind::Bool => Variant::Bool(match text {
            "1" => true,
            "0" => false,
            _ => return None,
        }),
        ShredKind::Int64 => Variant::Int(text.parse().ok()?),
        ShredKind::Double => Variant::Double(text.parse().ok()?),
        ShredKind::Str => Variant::Str(Arc::from(text)),
        ShredKind::Date => Variant::Date(crate::types::parse_date(text)?),
        ShredKind::Timestamp => Variant::Timestamp(crate::types::parse_timestamp(text)?),
    };
    (ShredKind::of(&value) == Some(kind)).then_some(value)
}

/// One shredded top-level object key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShredField {
    pub name: Arc<str>,
    pub kind: ShredKind,
}

/// The shredding policy: which top-level object keys get a typed column.
///
/// The policy is inferred once per column over all of its rows
/// ([`infer_policy`]) and then fixed for the whole file — a Parquet schema
/// cannot change between row groups, and the path statistics are only sound
/// when no later row holds another kind at the same path. It is deliberately shallow: only
/// top-level object keys with primitive values are shredded, at most
/// [`MAX_SHRED_FIELDS`] of them. Nested objects, arrays and decimals stay in
/// the residual `value` buffer, which every reader understands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShredPolicy {
    pub fields: Vec<ShredField>,
}

/// A key must appear in at least this many rows to be worth a column.
pub const MIN_OCCURRENCES: usize = 32;

/// A key must appear in this share of the rows: a typed column for a rare key
/// costs a column chunk and saves nothing.
pub const MIN_RATE: f64 = 0.5;

/// Most typed columns one VARIANT column may add.
pub const MAX_SHRED_FIELDS: usize = 8;

impl ShredPolicy {
    /// The field named `name`, if the policy shreds it.
    pub fn field(&self, name: &str) -> Option<&ShredField> {
        self.fields.iter().find(|f| f.name.as_ref() == name)
    }

    /// True when nothing is shredded (the column writes only `metadata` and
    /// `value`, which is still a valid Variant column).
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Infer the policy from the column's rows.
///
/// A key is shredded when, among the rows:
///
/// - it is present in at least [`MIN_OCCURRENCES`] rows,
/// - it is present in at least [`MIN_RATE`] of them (a *common* path),
/// - every present value is non-NULL and of the same [`ShredKind`].
///
/// Up to [`MAX_SHRED_FIELDS`] keys qualify, most frequent first (ties by
/// name), so the schema is deterministic for the same rows.
pub fn infer_policy(rows: &[&Variant]) -> ShredPolicy {
    use std::collections::BTreeMap;

    let total = rows.len();
    if total == 0 {
        return ShredPolicy::default();
    }
    /// A key's tally over the rows.
    struct Tally {
        rows: usize,
        /// The kind every present value had, or `None` once two differ.
        kind: Option<ShredKind>,
    }
    let mut tallies: BTreeMap<Arc<str>, Tally> = BTreeMap::new();
    for value in rows {
        let Variant::Object(fields) = value else {
            continue;
        };
        for (name, field) in fields.iter() {
            let tally = tallies.entry(name.clone()).or_insert(Tally {
                rows: 0,
                kind: ShredKind::of(field),
            });
            tally.rows += 1;
            if tally.kind != ShredKind::of(field) {
                tally.kind = None;
            }
        }
    }
    let mut fields: Vec<ShredField> = tallies
        .into_iter()
        .filter_map(|(name, tally)| {
            let kind = tally.kind?;
            if tally.rows < MIN_OCCURRENCES || tally.rows as f64 / (total as f64) < MIN_RATE {
                return None;
            }
            Some(ShredField { name, kind })
        })
        .collect();
    // Most frequent first; `BTreeMap` gave the deterministic name order.
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    fields.truncate(MAX_SHRED_FIELDS);
    ShredPolicy { fields }
}

/// Split one row into its residual value and its typed fields.
///
/// `typed[i]` corresponds to `policy.fields[i]`; `None` means "this row's
/// value for that key is not a typed value", and the key stays in the
/// residual. Either buffer may be `None`: a fully shredded row needs no
/// residual, and a non-object row has no typed fields at all.
pub fn shred_row(value: &Variant, policy: &ShredPolicy) -> (Option<Variant>, Vec<Option<Variant>>) {
    let Variant::Object(fields) = value else {
        return (Some(value.clone()), vec![None; policy.fields.len()]);
    };
    if policy.is_empty() {
        return (Some(value.clone()), Vec::new());
    }
    // Keys go to a typed column only when they are present with a non-NULL
    // value of the policy's kind; everything else stays in the residual, in
    // the object's (already sorted) order.
    let mut leftover: Vec<(Arc<str>, Variant)> = Vec::with_capacity(fields.len());
    for (name, field) in fields.iter() {
        let shred = policy
            .field(name)
            .is_some_and(|want| ShredKind::of(field) == Some(want.kind));
        if !shred {
            leftover.push((name.clone(), field.clone()));
        }
    }
    let typed = policy
        .fields
        .iter()
        .map(|want| {
            fields
                .iter()
                .find(|(name, _)| name == &want.name)
                .map(|(_, field)| field)
                .filter(|field| ShredKind::of(field) == Some(want.kind))
                .cloned()
        })
        .collect();
    let residual = if leftover.is_empty() {
        None
    } else {
        Some(Variant::Object(leftover.into()))
    };
    (residual, typed)
}

/// Reconcile a row's residual value with its typed fields.
///
/// This is the reader half of [`shred_row`], and the exact inverse of it: the
/// typed fields are inserted into the residual object, in policy order. A
/// typed field that is `None` is a *missing* key and stays out of the result.
/// A residual object may not repeat a shredded key — that would make the
/// value ambiguous, and the spec rejects it.
pub fn unshred_row(
    residual: Option<&Variant>,
    typed: &[(&str, Option<Variant>)],
) -> Result<Variant> {
    let mut fields: Vec<(Arc<str>, Variant)> = match residual {
        None => Vec::new(),
        Some(Variant::Object(fields)) => fields.iter().cloned().collect(),
        // A shredded column only shreds object keys, so a scalar residual is
        // the whole value: it can only be a row the policy could not split.
        Some(other) if typed.iter().all(|(_, value)| value.is_none()) => return Ok(other.clone()),
        Some(_) => {
            return Err(PinkDbError::Execution(
                "a shredded VARIANT row has typed fields but a non-object residual value"
                    .to_string(),
            ));
        }
    };
    for (name, value) in typed {
        let Some(value) = value else {
            continue;
        };
        if fields.iter().any(|(k, _)| k.as_ref() == *name) {
            return Err(PinkDbError::Execution(format!(
                "field \"{name}\" appears in both typed_value and value"
            )));
        }
        fields.push((Arc::from(*name), value.clone()));
    }
    Variant::object(fields)
}

// ---------------------------------------------------------------------------
// Parquet's Variant metadata dictionary
// ---------------------------------------------------------------------------

/// Header byte: version (bits 3..0), sorted flag (bit 4), offset width minus
/// one (bits 7..6).
const METADATA_VERSION: u8 = 1;

/// Encode `keys` as a Variant metadata dictionary.
///
/// The dictionary is sorted and deduplicated, and the header advertises that,
/// so object field ids are ordered the way the spec requires. The offset
/// width follows the data length: 1 byte over the shared keys of a common
/// schema, 4 for a dictionary with megabytes of key text.
pub fn encode_metadata(keys: &[Arc<str>]) -> Vec<u8> {
    let mut keys: Vec<&str> = keys.iter().map(|k| k.as_ref()).collect();
    keys.sort_unstable();
    keys.dedup();
    let data_len: usize = keys.iter().map(|k| k.len()).sum();
    let offset_size = width_for(data_len);
    let mut out = Vec::with_capacity(1 + (keys.len() + 1) * offset_size as usize + data_len);
    out.push(METADATA_VERSION | (1 << 4) | ((offset_size - 1) << 6));
    push_uint(&mut out, keys.len() as u64, offset_size);
    let mut offset = 0usize;
    push_uint(&mut out, 0, offset_size);
    for key in &keys {
        offset += key.len();
        push_uint(&mut out, offset as u64, offset_size);
    }
    for key in &keys {
        out.extend_from_slice(key.as_bytes());
    }
    out
}

/// The keys of a metadata dictionary, in dictionary order.
pub fn decode_metadata(bytes: &[u8]) -> Result<Vec<Arc<str>>> {
    let header = *bytes.first().ok_or_else(|| bad("empty variant metadata"))?;
    if header & 0x0F != METADATA_VERSION {
        return Err(bad(&format!(
            "variant metadata version {} is not {METADATA_VERSION}",
            header & 0x0F
        )));
    }
    let offset_size = ((header >> 6) & 0x03) as usize + 1;
    let dictionary_size = read_uint(bytes, 1, offset_size)? as usize;
    // header byte, then the count, then the offsets, then the key bytes.
    let offsets_start = 1 + offset_size;
    let data_start = offsets_start + (dictionary_size + 1) * offset_size;
    let data = bytes
        .get(data_start..)
        .ok_or_else(|| bad("variant metadata offsets run past its bytes"))?;
    let mut keys = Vec::with_capacity(dictionary_size);
    let mut previous = 0usize;
    for index in 0..=dictionary_size {
        let offset = read_uint(bytes, offsets_start + index * offset_size, offset_size)? as usize;
        if index == 0 {
            if offset != 0 {
                return Err(bad("variant metadata's first offset is not zero"));
            }
            previous = 0;
            continue;
        }
        if offset < previous {
            return Err(bad("variant metadata offsets are not monotonic"));
        }
        let key = data
            .get(previous..offset)
            .ok_or_else(|| bad("variant metadata offset out of bounds"))?;
        let key =
            std::str::from_utf8(key).map_err(|_| bad("variant metadata key is not valid UTF-8"))?;
        // Collapse duplicate keys: the value encoding addresses fields by
        // dictionary index, and two indices for one key would be ambiguous.
        if keys.last().map(|k: &Arc<str>| k.as_ref()) != Some(key) {
            keys.push(Arc::from(key));
        }
        previous = offset;
    }
    Ok(keys)
}

// ---------------------------------------------------------------------------
// Parquet's Variant value encoding
// ---------------------------------------------------------------------------

/// The basic type of a value, in the header byte's low two bits.
const BASIC_PRIMITIVE: u8 = 0;
const BASIC_SHORT_STRING: u8 = 1;
const BASIC_OBJECT: u8 = 2;
const BASIC_ARRAY: u8 = 3;

/// Primitive type tags (`header >> 2`), in the spec's order.
const P_NULL: u8 = 0;
const P_TRUE: u8 = 1;
const P_FALSE: u8 = 2;
const P_INT8: u8 = 3;
const P_INT16: u8 = 4;
const P_INT32: u8 = 5;
const P_INT64: u8 = 6;
const P_DOUBLE: u8 = 7;
const P_DECIMAL4: u8 = 8;
const P_DECIMAL8: u8 = 9;
const P_DECIMAL16: u8 = 10;
const P_DATE: u8 = 11;
const P_TIMESTAMP_MICROS: u8 = 12;
const P_TIMESTAMP_NTZ_MICROS: u8 = 13;
const P_FLOAT: u8 = 14;
const P_STRING: u8 = 16;

/// Longest string the `ShortString` basic type can hold.
const SHORT_STRING_MAX: usize = 63;

/// Encode `value` with `keys` as its dictionary.
///
/// `keys` must be sorted (which [`encode_metadata`] guarantees), because
/// object field ids are written in the sorted-by-key order the spec requires.
pub fn encode_value(value: &Variant, keys: &[Arc<str>], out: &mut Vec<u8>) -> Result<()> {
    match value {
        Variant::Null => out.push(P_NULL << 2),
        Variant::Bool(true) => out.push(P_TRUE << 2),
        Variant::Bool(false) => out.push(P_FALSE << 2),
        Variant::Int(i) => {
            out.push(P_INT64 << 2);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Variant::Double(x) => {
            out.push(P_DOUBLE << 2);
            out.extend_from_slice(&x.to_le_bytes());
        }
        Variant::Decimal(unscaled, scale) => {
            // The narrowest decimal the value fits, exactly as the spec's
            // own encoders choose it.
            if let Ok(narrow) = i32::try_from(*unscaled) {
                out.push(P_DECIMAL4 << 2);
                out.push(*scale);
                out.extend_from_slice(&narrow.to_le_bytes());
            } else if let Ok(narrow) = i64::try_from(*unscaled) {
                out.push(P_DECIMAL8 << 2);
                out.push(*scale);
                out.extend_from_slice(&narrow.to_le_bytes());
            } else {
                out.push(P_DECIMAL16 << 2);
                out.push(*scale);
                out.extend_from_slice(&unscaled.to_le_bytes());
            }
        }
        Variant::Str(s) => {
            if s.len() <= SHORT_STRING_MAX {
                out.push(((s.len() as u8) << 2) | BASIC_SHORT_STRING);
            } else {
                out.push(P_STRING << 2);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            }
            out.extend_from_slice(s.as_bytes());
        }
        Variant::Date(days) => {
            out.push(P_DATE << 2);
            out.extend_from_slice(&days.to_le_bytes());
        }
        Variant::Timestamp(us) => {
            out.push(P_TIMESTAMP_MICROS << 2);
            out.extend_from_slice(&us.to_le_bytes());
        }
        Variant::Array(items) => {
            let mut encoded = Vec::with_capacity(items.len());
            for item in items.iter() {
                let mut bytes = Vec::new();
                encode_value(item, keys, &mut bytes)?;
                encoded.push(bytes);
            }
            let total: usize = encoded.iter().map(|b| b.len()).sum();
            let offset_size = width_for(total);
            let is_large = items.len() > u8::MAX as usize;
            let header = BASIC_ARRAY | ((offset_size - 1) << 2) | if is_large { 1 << 4 } else { 0 };
            out.push(header);
            if is_large {
                out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            } else {
                out.push(items.len() as u8);
            }
            let mut offset = 0usize;
            push_uint(out, 0, offset_size);
            for bytes in &encoded {
                offset += bytes.len();
                push_uint(out, offset as u64, offset_size);
            }
            for bytes in &encoded {
                out.extend_from_slice(bytes);
            }
        }
        Variant::Object(fields) => {
            // Field ids must be lexically ordered: `Variant`'s keys already
            // are, so the dictionary lookup of each key is non-decreasing.
            let mut ids = Vec::with_capacity(fields.len());
            for (name, _) in fields.iter() {
                let id = keys
                    .binary_search_by(|k| k.as_ref().cmp(name.as_ref()))
                    .map_err(|_| {
                        bad(&format!("variant key \"{name}\" is not in the dictionary"))
                    })?;
                ids.push(id as u64);
            }
            let mut encoded = Vec::with_capacity(fields.len());
            for (_, field) in fields.iter() {
                let mut bytes = Vec::new();
                encode_value(field, keys, &mut bytes)?;
                encoded.push(bytes);
            }
            let total: usize = encoded.iter().map(|b| b.len()).sum();
            let offset_size = width_for(total);
            let id_size = width_for(ids.iter().copied().max().unwrap_or(0) as usize);
            let is_large = fields.len() > u8::MAX as usize;
            let header = BASIC_OBJECT
                | ((offset_size - 1) << 2)
                | ((id_size - 1) << 4)
                | if is_large { 1 << 6 } else { 0 };
            out.push(header);
            if is_large {
                out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            } else {
                out.push(fields.len() as u8);
            }
            for id in ids {
                push_uint(out, id, id_size);
            }
            let mut offset = 0usize;
            push_uint(out, 0, offset_size);
            for bytes in &encoded {
                offset += bytes.len();
                push_uint(out, offset as u64, offset_size);
            }
            for bytes in &encoded {
                out.extend_from_slice(bytes);
            }
        }
    }
    Ok(())
}

/// Decode one value with `keys` as its dictionary.
pub fn decode_value(bytes: &[u8], keys: &[Arc<str>]) -> Result<Variant> {
    let mut cursor = 0usize;
    decode_at(bytes, &mut cursor, keys)
}

fn decode_at(bytes: &[u8], cursor: &mut usize, keys: &[Arc<str>]) -> Result<Variant> {
    let header = *bytes
        .get(*cursor)
        .ok_or_else(|| bad("variant value ended inside a value"))?;
    *cursor += 1;
    match header & 0x03 {
        BASIC_PRIMITIVE => decode_primitive(header >> 2, bytes, cursor),
        BASIC_SHORT_STRING => {
            let len = (header >> 2) as usize;
            let text = take(bytes, cursor, len)?;
            let text = std::str::from_utf8(text)
                .map_err(|_| bad("variant short string is not valid UTF-8"))?;
            Ok(Variant::Str(Arc::from(text)))
        }
        BASIC_ARRAY => {
            let value_header = header >> 2;
            let offset_size = (value_header & 0x03) as usize + 1;
            let is_large = value_header & 0x04 != 0;
            let count = if is_large {
                u32::from_le_bytes(take_array(bytes, cursor)?) as usize
            } else {
                take(bytes, cursor, 1)?[0] as usize
            };
            skip_offsets(bytes, cursor, count, offset_size)?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_at(bytes, cursor, keys)?);
            }
            Variant::array(items)
        }
        BASIC_OBJECT => {
            let value_header = header >> 2;
            let offset_size = (value_header & 0x03) as usize + 1;
            let id_size = ((value_header >> 2) & 0x03) as usize + 1;
            let is_large = value_header & 0x10 != 0;
            let count = if is_large {
                u32::from_le_bytes(take_array(bytes, cursor)?) as usize
            } else {
                take(bytes, cursor, 1)?[0] as usize
            };
            let mut names = Vec::with_capacity(count);
            for _ in 0..count {
                let id = read_uint(bytes, *cursor, id_size)? as usize;
                *cursor += id_size;
                let name = keys
                    .get(id)
                    .ok_or_else(|| bad("variant object field id is not in the dictionary"))?;
                names.push(name.clone());
            }
            skip_offsets(bytes, cursor, count, offset_size)?;
            let mut fields = Vec::with_capacity(count);
            for name in names {
                fields.push((name, decode_at(bytes, cursor, keys)?));
            }
            // `Variant::object` sorts (the field ids already are) and keeps
            // the last duplicate, so a malformed object cannot smuggle in two
            // values for one key.
            Variant::object(fields)
        }
        basic => Err(bad(&format!("unknown variant basic type {basic}"))),
    }
}

fn decode_primitive(tag: u8, bytes: &[u8], cursor: &mut usize) -> Result<Variant> {
    Ok(match tag {
        P_NULL => Variant::Null,
        P_TRUE => Variant::Bool(true),
        P_FALSE => Variant::Bool(false),
        P_INT8 => Variant::Int(i64::from(take(bytes, cursor, 1)?[0] as i8)),
        P_INT16 => Variant::Int(i64::from(i16::from_le_bytes(take_array(bytes, cursor)?))),
        P_INT32 => Variant::Int(i64::from(i32::from_le_bytes(take_array(bytes, cursor)?))),
        P_INT64 => Variant::Int(i64::from_le_bytes(take_array(bytes, cursor)?)),
        P_DOUBLE => Variant::Double(f64::from_le_bytes(take_array(bytes, cursor)?)),
        P_FLOAT => Variant::Double(f64::from(f32::from_le_bytes(take_array(bytes, cursor)?))),
        P_DECIMAL4 => {
            let scale = take(bytes, cursor, 1)?[0];
            Variant::Decimal(
                i128::from(i32::from_le_bytes(take_array(bytes, cursor)?)),
                scale,
            )
        }
        P_DECIMAL8 => {
            let scale = take(bytes, cursor, 1)?[0];
            Variant::Decimal(
                i128::from(i64::from_le_bytes(take_array(bytes, cursor)?)),
                scale,
            )
        }
        P_DECIMAL16 => {
            let scale = take(bytes, cursor, 1)?[0];
            Variant::Decimal(i128::from_le_bytes(take_array(bytes, cursor)?), scale)
        }
        P_DATE => Variant::Date(i32::from_le_bytes(take_array(bytes, cursor)?)),
        P_TIMESTAMP_MICROS | P_TIMESTAMP_NTZ_MICROS => {
            Variant::Timestamp(i64::from_le_bytes(take_array(bytes, cursor)?))
        }
        P_STRING => {
            let len = u32::from_le_bytes(take_array(bytes, cursor)?) as usize;
            let text = take(bytes, cursor, len)?;
            let text =
                std::str::from_utf8(text).map_err(|_| bad("variant string is not valid UTF-8"))?;
            Variant::Str(Arc::from(text))
        }
        other => {
            return Err(bad(&format!(
                "variant primitive type {other} is not representable in pinkdb"
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// Bytes
// ---------------------------------------------------------------------------

/// The fewest bytes that hold `value` (`1..=4`, little-endian).
fn width_for(value: usize) -> u8 {
    if value <= u8::MAX as usize {
        1
    } else if value <= u16::MAX as usize {
        2
    } else if value <= 0x00FF_FFFF {
        3
    } else {
        4
    }
}

fn push_uint(out: &mut Vec<u8>, value: u64, width: u8) {
    out.extend_from_slice(&value.to_le_bytes()[..width as usize]);
}

fn read_uint(bytes: &[u8], offset: usize, width: usize) -> Result<u64> {
    let bytes = bytes
        .get(offset..offset + width)
        .ok_or_else(|| bad("variant encoding ended inside an offset"))?;
    let mut buf = [0u8; 8];
    buf[..width].copy_from_slice(bytes);
    Ok(u64::from_le_bytes(buf))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let slice = bytes
        .get(*cursor..*cursor + len)
        .ok_or_else(|| bad("variant encoding ended inside a value"))?;
    *cursor += len;
    Ok(slice)
}

fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    let slice = take(bytes, cursor, N)?;
    let mut out = [0u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

/// Skip a `(count + 1)`-entry offset array; the values are decoded in order,
/// so the offsets are only a bounds check.
fn skip_offsets(bytes: &[u8], cursor: &mut usize, count: usize, width: usize) -> Result<()> {
    let widths = (count + 1)
        .checked_mul(width)
        .ok_or_else(|| bad("variant offset array is too large"))?;
    take(bytes, cursor, widths)?;
    Ok(())
}

fn bad(message: &str) -> PinkDbError {
    PinkDbError::Execution(format!("invalid parquet VARIANT: {message}"))
}

/// The exact statistics of one path of a VARIANT column: what a filter on
/// `v['a']` (or `variant_extract(v, '$.a')`) is pruned with.
#[derive(Debug, Clone, PartialEq)]
pub struct PathStats {
    /// The canonical path key ([`crate::variant::stats_path_text`]).
    pub path: String,
    /// The kind every value at this path has, and the type its bounds are
    /// published under.
    pub kind: ShredKind,
    /// Rows where the path is SQL NULL: the key is absent, or its value is
    /// `Variant::Null`. Exactly what `v['a'] IS NULL` tests.
    pub null_count: u64,
    /// Exact min/max of the non-NULL values, when the path has any.
    pub min: Option<Variant>,
    pub max: Option<Variant>,
}

/// Accumulates [`PathStats`] for a shredded column while one pass over its
/// rows runs.
///
/// Soundness rests on the policy: [`infer_policy`] only shreds a field every
/// occurrence of which has the field's kind, so the values folded into
/// min/max are *all* the non-NULL values of that path. A row whose value has
/// another kind (a file written by another engine, mixed shapes) therefore
/// must not reach [`PathStatsAcc::see`]; the callers check the kind.
#[derive(Debug, Clone)]
pub struct PathStatsAcc {
    fields: Vec<(Arc<str>, ShredKind)>,
    paths: Vec<String>,
    nulls: Vec<u64>,
    min: Vec<Option<Variant>>,
    max: Vec<Option<Variant>>,
    /// Cleared when a value of another kind appeared: min/max no longer bound
    /// every non-NULL value, so the path publishes no statistics at all.
    usable: Vec<bool>,
}

impl PathStatsAcc {
    /// One accumulator per policy field, skipping fields whose path cannot be
    /// spelled canonically (they get no statistics).
    pub fn new(policy: &ShredPolicy) -> Self {
        let mut fields = Vec::with_capacity(policy.fields.len());
        let mut paths = Vec::with_capacity(policy.fields.len());
        for field in &policy.fields {
            let key = [crate::variant::Step::Key(Arc::clone(&field.name))];
            let Some(path) = crate::variant::stats_path_text(&key) else {
                continue;
            };
            fields.push((Arc::clone(&field.name), field.kind));
            paths.push(path);
        }
        let n = fields.len();
        Self {
            fields,
            paths,
            nulls: vec![0; n],
            min: vec![None; n],
            max: vec![None; n],
            usable: vec![true; n],
        }
    }

    /// One non-NULL row value.
    pub fn see(&mut self, value: &Variant) {
        let Variant::Object(entries) = value else {
            self.null_all();
            return;
        };
        for (index, (name, kind)) in self.fields.iter().enumerate() {
            let Some((_, field)) = entries
                .iter()
                .find(|(key, _)| key.as_ref() == name.as_ref())
            else {
                self.nulls[index] += 1;
                continue;
            };
            if matches!(field, Variant::Null) {
                self.nulls[index] += 1;
                continue;
            }
            if Some(*kind) != ShredKind::of(field) {
                self.usable[index] = false;
                continue;
            }
            let (lo, hi) = (self.min[index].as_ref(), self.max[index].as_ref());
            if lo.is_none_or(|lo| crate::variant::cmp(field, lo).is_lt()) {
                self.min[index] = Some(field.clone());
            }
            if hi.is_none_or(|hi| crate::variant::cmp(field, hi).is_gt()) {
                self.max[index] = Some(field.clone());
            }
        }
    }

    /// One SQL NULL row.
    pub fn see_null(&mut self) {
        self.null_all();
    }

    fn null_all(&mut self) {
        for count in &mut self.nulls {
            *count += 1;
        }
    }

    /// The accumulated statistics, in policy order; a path that saw a
    /// foreign kind is omitted.
    pub fn finish(self) -> Vec<PathStats> {
        let mut out = Vec::with_capacity(self.paths.len());
        for (index, path) in self.paths.into_iter().enumerate() {
            if !self.usable[index] {
                continue;
            }
            out.push(PathStats {
                path,
                kind: self.fields[index].1,
                null_count: self.nulls[index],
                min: self.min[index].clone(),
                max: self.max[index].clone(),
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|n| Arc::from(*n)).collect()
    }

    fn round_trip(value: &Variant) -> Variant {
        let mut key_list: Vec<Arc<str>> = Vec::new();
        collect_keys(value, &mut key_list);
        let metadata = encode_metadata(&key_list);
        let keys = decode_metadata(&metadata).expect("metadata decodes");
        let mut bytes = Vec::new();
        encode_value(value, &keys, &mut bytes).expect("value encodes");
        decode_value(&bytes, &keys).expect("value decodes")
    }

    fn collect_keys(value: &Variant, out: &mut Vec<Arc<str>>) {
        match value {
            Variant::Array(items) => {
                for item in items.iter() {
                    collect_keys(item, out);
                }
            }
            Variant::Object(fields) => {
                for (name, field) in fields.iter() {
                    out.push(name.clone());
                    collect_keys(field, out);
                }
            }
            _ => {}
        }
    }

    fn parse(json: &str) -> Variant {
        crate::variant::from_json_str(json).expect("json parses")
    }

    #[test]
    fn metadata_matches_the_duckdb_bytes() {
        // DuckDB 2.0 wrote `01 02 00 01 02 61 62` for the dictionary
        // ["a","b"]; the first byte here is `0x11` because we sort the keys
        // and set the header's sorted flag (DuckDB does not).
        let encoded = encode_metadata(&keys(&["b", "a"]));
        assert_eq!(encoded, vec![0x11, 0x02, 0x00, 0x01, 0x02, b'a', b'b']);
        assert_eq!(decode_metadata(&encoded).unwrap(), keys(&["a", "b"]));
    }

    #[test]
    fn empty_metadata_is_the_canonical_three_bytes() {
        let encoded = encode_metadata(&[]);
        // `[1, 0, 0]` plus the sorted flag, which an empty dictionary may
        // advertise (it is trivially sorted).
        assert_eq!(encoded, vec![0x11, 0, 0]);
        assert!(decode_metadata(&encoded).unwrap().is_empty());
    }

    #[test]
    fn metadata_offsets_widen_with_the_dictionary() {
        let long = "k".repeat(300);
        let encoded = encode_metadata(&keys(&["a", long.as_str()]));
        // header says offset_size 2 (300 > u8::MAX).
        assert_eq!(encoded[0] >> 6, 1);
        assert_eq!(decode_metadata(&encoded).unwrap().len(), 2);
    }

    #[test]
    fn int64_matches_the_duckdb_bytes() {
        // DuckDB 2.0 wrote `18 01 00 00 00 00 00 00 00` for the typed value 1.
        let keys = keys(&[]);
        let mut bytes = Vec::new();
        encode_value(&Variant::Int(1), &keys, &mut bytes).unwrap();
        assert_eq!(bytes, vec![0x18, 1, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn values_round_trip_through_the_spec_encoding() {
        for json in [
            "null",
            "true",
            "false",
            "1",
            "-9223372036854775808",
            "1.5",
            "\"short\"",
            "\"a string longer than sixty three characters, which needs the long form\"",
            "{\"a\":1,\"b\":[1,2,{\"c\":\"x\"}]}",
            "[]",
            "{}",
            "[[1,[2,[3]]]]",
            "{\"z\":{\"y\":{\"x\":[null,true]}}}",
        ] {
            let value = parse(json);
            assert_eq!(round_trip(&value).to_json(), value.to_json(), "{json}");
        }
    }

    #[test]
    fn wide_objects_and_arrays_use_the_large_form() {
        let big = Variant::Object(Arc::from(
            (0..300)
                .map(|i| (Arc::from(format!("k{i:03}").as_str()), Variant::Int(i)))
                .collect::<Vec<_>>(),
        ));
        let round = round_trip(&big);
        assert_eq!(round.to_json(), big.to_json());
        let wide = Variant::array((0..300).map(Variant::Int).collect()).unwrap();
        assert_eq!(round_trip(&wide).to_json(), wide.to_json());
    }

    #[test]
    fn decimal_kinds_round_trip() {
        for value in [
            Variant::Decimal(1, 2),
            Variant::Decimal(i128::from(i32::MAX) + 1, 3),
            Variant::Decimal(i128::MAX, 4),
        ] {
            assert_eq!(round_trip(&value).to_json(), value.to_json());
        }
    }

    #[test]
    fn policy_shreds_common_typed_paths_only() {
        let mut sample = Vec::new();
        for i in 0..64 {
            sample.push(parse(&format!("{{\"a\":{i},\"b\":\"x{i}\",\"c\":[{i}]}}")));
        }
        let refs: Vec<&Variant> = sample.iter().collect();
        let policy = infer_policy(&refs);
        let names: Vec<&str> = policy.fields.iter().map(|f| f.name.as_ref()).collect();
        // `c` is an array, so it stays in the residual.
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(policy.fields[0].kind, ShredKind::Int64);
        assert_eq!(policy.fields[1].kind, ShredKind::Str);
    }

    #[test]
    fn policy_skips_rare_and_mixed_paths() {
        let mut sample: Vec<Variant> = (0..64).map(|i| parse(&format!("{{\"a\":{i}}}"))).collect();
        // One row with a string `a`, one with a second key: neither is
        // stable/frequent enough to shred.
        sample[1] = parse("{\"a\":\"x\"}");
        sample[2] = parse("{\"a\":1,\"b\":2}");
        let refs: Vec<&Variant> = sample.iter().collect();
        let policy = infer_policy(&refs);
        assert!(policy.is_empty(), "{policy:?}");
    }

    #[test]
    fn shredding_round_trips_exactly() {
        let mut sample: Vec<Variant> = Vec::new();
        for i in 0..64 {
            sample.push(parse(&format!("{{\"a\":{i},\"b\":\"x\",\"c\":[1]}}")));
        }
        let refs: Vec<&Variant> = sample.iter().collect();
        let policy = infer_policy(&refs);
        for value in [
            parse("{\"a\":1,\"b\":\"x\",\"c\":[1]}"),
            // Present but null: stays in the residual, so it reads back exact.
            parse("{\"a\":null,\"b\":\"x\"}"),
            // A different kind at a shredded path: also residual.
            parse("{\"a\":\"s\",\"b\":\"x\"}"),
            // Missing keys, and a non-object row.
            parse("{\"b\":\"x\"}"),
            parse("{}"),
            parse("[1,2]"),
            parse("null"),
        ] {
            let (residual, typed) = shred_row(&value, &policy);
            let fields: Vec<(&str, Option<Variant>)> = policy
                .fields
                .iter()
                .zip(typed)
                .map(|(f, v)| (f.name.as_ref(), v))
                .collect();
            let back = unshred_row(residual.as_ref(), &fields).unwrap();
            assert_eq!(back.to_json(), value.to_json());
        }
    }

    #[test]
    fn unshred_refuses_a_key_in_both_buffers() {
        let residual = parse("{\"a\":1}");
        let err = unshred_row(Some(&residual), &[("a", Some(Variant::Int(2)))]).unwrap_err();
        assert!(format!("{err}").contains("appears in both"));
    }

    #[test]
    fn a_truncated_value_is_an_error_not_a_panic() {
        let keys = keys(&["a"]);
        let mut bytes = Vec::new();
        encode_value(&parse("{\"a\":[1,2,3]}"), &keys, &mut bytes).unwrap();
        for cut in 0..bytes.len() {
            assert!(decode_value(&bytes[..cut], &keys).is_err(), "cut {cut}");
        }
    }
}
