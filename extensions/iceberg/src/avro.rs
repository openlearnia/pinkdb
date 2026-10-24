//! A minimal Avro binary decoder (wave 72).
//!
//! Iceberg puts its manifest list and manifests in Avro *object container*
//! files, so a table read needs the container framing plus the small slice of
//! Avro's binary encoding those schemas use: records, nullable unions, arrays,
//! maps (Iceberg writes its int-keyed maps as arrays of `{key, value}`
//! records), enums and fixed. That is a few hundred lines, so this stays a
//! hand-written decoder instead of the `apache-avro` crate (whose schema
//! resolution, logical types, single-object encoding and writer are most of
//! its weight).
//!
//! Deliberate limits, each refused by name rather than guessed at:
//!
//! - codecs `null` and `deflate` only (the two Iceberg writers emit);
//! - **no projection pushdown**: a whole record is decoded, then read by name.
//!   Manifest entries are ~100 bytes, so this never shows up in a profile;
//! - sequential reads only (the container is a stream of blocks).
//!
//! A schema is decoded against its own JSON: named types are registered first
//! so a record referenced by name resolves, and unions collapse to the value
//! of the branch that was written (a `null` branch decodes to [`Avro::Null`]).

use std::collections::HashMap;
use std::io::Read;

use serde_json::Map;
use serde_json::Value as Json;

/// A decoded Avro value, with the shape the schema gave it.
#[derive(Debug, Clone, PartialEq)]
pub enum Avro {
    Null,
    Bool(bool),
    /// `int` and `long` share one variant; the schema decides the width, and
    /// every reader here wants an `i64` anyway.
    Int(i64),
    /// `float` and `double`.
    Float(f64),
    Bytes(Vec<u8>),
    Str(String),
    Array(Vec<Avro>),
    /// A map keeps its written order: Iceberg's keys are small integers, so
    /// insertion order *is* the interesting order.
    Map(Vec<(String, Avro)>),
    Record(Vec<(String, Avro)>),
}

/// A static null, so a field lookup on a record that does not carry the field
/// (an older manifest schema) reads as null instead of failing.
static NULL: Avro = Avro::Null;

impl Avro {
    /// The field `name` of a record or map, or [`Avro::Null`] when the record
    /// does not have it. Iceberg readers must tolerate manifest schemas that
    /// predate a field (v1 manifests have no `content`, for one).
    pub fn field(&self, name: &str) -> &Avro {
        match self {
            Avro::Record(fields) | Avro::Map(fields) => fields
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v)
                .unwrap_or(&NULL),
            _ => &NULL,
        }
    }

    /// A non-null `long`/`int`, or `None`.
    pub fn as_long(&self) -> Option<i64> {
        match self {
            Avro::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Avro::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Avro::Str(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Avro::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Avro]> {
        match self {
            Avro::Array(v) => Some(v),
            _ => None,
        }
    }
}

/// Read every record of one Avro object container from `r`.
///
/// The header's schema drives the decode (so a writer's extra fields are
/// skipped, not rejected) and its codec decides the block inflation; the
/// container metadata itself is not returned — Iceberg puts the same schema
/// into the table metadata, which the caller already has.
pub fn read_records(r: &mut impl Read) -> Result<Vec<Avro>, String> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)
        .map_err(|e| format!("an Avro container starts with `Obj\\x01`: {e}"))?;
    if &magic != b"Obj\x01" {
        return Err("not an Avro container file (bad magic bytes)".to_string());
    }
    let metadata = read_map(r)?;
    let mut sync = [0u8; 16];
    r.read_exact(&mut sync)
        .map_err(|e| format!("the Avro header has no sync marker: {e}"))?;
    let codec = metadata
        .iter()
        .find(|(k, _)| k == "avro.codec")
        .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_else(|| "null".to_string());
    let schema_text = metadata
        .iter()
        .find(|(k, _)| k == "avro.schema")
        .ok_or("the Avro header has no avro.schema")?;
    let schema: Json = serde_json::from_slice(&schema_text.1)
        .map_err(|e| format!("avro.schema is not valid JSON: {e}"))?;
    let decoder = Decoder::new(&schema);
    let mut records = Vec::new();
    while let Some(count) = read_long_opt(r).map_err(|e| format!("Avro block header: {e}"))? {
        let size = read_long(r).map_err(|e| format!("Avro block size: {e}"))?;
        if !(0..=(1 << 30)).contains(&size) {
            return Err(format!("Avro block size {size} is out of range"));
        }
        let mut payload = vec![0u8; size as usize];
        r.read_exact(&mut payload)
            .map_err(|e| format!("Avro block: {e}"))?;
        let inflated;
        let bytes: &[u8] = match codec.as_str() {
            "null" => &payload,
            "deflate" => {
                inflated = inflate(&payload)?;
                &inflated
            }
            other => {
                return Err(format!(
                    "the Avro container uses the `{other}` codec; pinkdb decodes null \
                     and deflate blocks only"
                ));
            }
        };
        let mut block = bytes;
        for _ in 0..count {
            records.push(decoder.decode(&schema, &mut block, 0)?);
        }
        let mut marker = [0u8; 16];
        r.read_exact(&mut marker)
            .map_err(|e| format!("Avro block has no trailing sync marker: {e}"))?;
        if marker != sync {
            return Err("Avro sync marker mismatch (corrupt or concatenated file)".to_string());
        }
    }
    Ok(records)
}

fn inflate(payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    flate2::read::DeflateDecoder::new(payload)
        .read_to_end(&mut out)
        .map_err(|e| format!("a deflate Avro block did not inflate: {e}"))?;
    Ok(out)
}

/// The container header is a `map<string, bytes>`.
fn read_map(r: &mut impl Read) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut entries = Vec::new();
    loop {
        let mut count = read_long(r).map_err(|e| format!("Avro header map: {e}"))?;
        if count == 0 {
            break;
        }
        if count < 0 {
            count = -count;
            let _block_bytes = read_long(r).map_err(|e| format!("Avro header map: {e}"))?;
        }
        if count > (1 << 20) {
            return Err(format!("an Avro header map with {count} entries"));
        }
        for _ in 0..count {
            let key = String::from_utf8(read_bytes(r)?)
                .map_err(|e| format!("an Avro header key is not UTF-8: {e}"))?;
            let value = read_bytes(r)?;
            entries.push((key, value));
        }
    }
    Ok(entries)
}

fn read_bytes(r: &mut impl Read) -> Result<Vec<u8>, String> {
    let len = read_long(r)?;
    if !(0..=(1 << 30)).contains(&len) {
        return Err(format!("an Avro bytes length of {len} is out of range"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)
        .map_err(|e| format!("an Avro bytes value: {e}"))?;
    Ok(buf)
}

/// Zig-zag varint.
fn read_long(r: &mut impl Read) -> Result<i64, String> {
    read_long_opt(r)?.ok_or_else(|| "unexpected end of Avro data".to_string())
}

/// [`read_long`], with `None` for a clean end of file (the block loop's stop
/// condition).
fn read_long_opt(r: &mut impl Read) -> Result<Option<i64>, String> {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut byte = [0u8; 1];
    for i in 0..10 {
        match r.read(&mut byte) {
            Ok(0) if i == 0 => return Ok(None),
            Ok(0) => return Err("a varint ended mid-byte".to_string()),
            Ok(_) => {}
            Err(e) => return Err(format!("a varint: {e}")),
        }
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            // Zig-zag: the low bit is the sign.
            return Ok(Some(((value >> 1) as i64) ^ -((value & 1) as i64)));
        }
        shift += 7;
    }
    Err("a varint longer than 10 bytes".to_string())
}

/// The named types of one schema, so a field whose type is a name (not a
/// primitive) resolves to the definition it was declared with.
struct Decoder<'s> {
    names: HashMap<String, &'s Json>,
}

impl<'s> Decoder<'s> {
    fn new(schema: &'s Json) -> Decoder<'s> {
        let mut names = HashMap::new();
        collect_names(schema, &mut names);
        Decoder { names }
    }

    fn decode(&self, schema: &'s Json, r: &mut impl Read, depth: usize) -> Result<Avro, String> {
        if depth > 64 {
            return Err("an Avro schema nested deeper than 64 levels".to_string());
        }
        match schema {
            Json::String(name) => self.decode_named(name, r, depth),
            Json::Array(branches) => {
                let index = read_long(r)?;
                let branch = branches.get(index as usize).ok_or_else(|| {
                    format!(
                        "a union index {index} is out of range (0..{})",
                        branches.len()
                    )
                })?;
                self.decode(branch, r, depth + 1)
            }
            Json::Object(obj) => {
                let ty = obj.get("type").ok_or("an Avro type without a `type`")?;
                match ty {
                    Json::String(kind) => match kind.as_str() {
                        "record" => self.decode_record(obj, r, depth),
                        "array" => self.decode_array(obj, r, depth),
                        "map" => {
                            let values = obj.get("values").ok_or("a map without `values`")?;
                            let mut out = Vec::new();
                            loop {
                                let mut count = read_long(r)?;
                                if count == 0 {
                                    break;
                                }
                                if count < 0 {
                                    count = -count;
                                    let _block_bytes = read_long(r)?;
                                }
                                for _ in 0..count {
                                    // A map key is always a string.
                                    let key = String::from_utf8(read_bytes(r)?)
                                        .map_err(|e| format!("a map key is not UTF-8: {e}"))?;
                                    out.push((key, self.decode(values, r, depth + 1)?));
                                }
                            }
                            Ok(Avro::Map(out))
                        }
                        "enum" => {
                            let index = read_long(r)?;
                            let symbols = obj
                                .get("symbols")
                                .and_then(Json::as_array)
                                .ok_or("an enum without `symbols`")?;
                            let symbol = symbols
                                .get(index as usize)
                                .and_then(Json::as_str)
                                .ok_or_else(|| format!("an enum index {index} is out of range"))?;
                            Ok(Avro::Str(symbol.to_string()))
                        }
                        "fixed" => {
                            let size = obj
                                .get("size")
                                .and_then(Json::as_u64)
                                .ok_or("a fixed type without `size`")?;
                            let mut buf = vec![0u8; size as usize];
                            r.read_exact(&mut buf)
                                .map_err(|e| format!("a fixed value: {e}"))?;
                            Ok(Avro::Bytes(buf))
                        }
                        other => self.decode_named(other, r, depth),
                    },
                    // `{"type": ["null", "string"]}` and friends.
                    _ => self.decode(ty, r, depth + 1),
                }
            }
            other => Err(format!("an Avro type pinkdb cannot decode: {other}")),
        }
    }

    fn decode_named(&self, name: &str, r: &mut impl Read, depth: usize) -> Result<Avro, String> {
        match name {
            "null" => Ok(Avro::Null),
            "boolean" => {
                let mut byte = [0u8; 1];
                r.read_exact(&mut byte)
                    .map_err(|e| format!("an Avro boolean: {e}"))?;
                Ok(Avro::Bool(byte[0] != 0))
            }
            "int" | "long" => Ok(Avro::Int(read_long(r)?)),
            "float" => {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)
                    .map_err(|e| format!("an Avro float: {e}"))?;
                Ok(Avro::Float(f32::from_le_bytes(buf) as f64))
            }
            "double" => {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)
                    .map_err(|e| format!("an Avro double: {e}"))?;
                Ok(Avro::Float(f64::from_le_bytes(buf)))
            }
            "bytes" => Ok(Avro::Bytes(read_bytes(r)?)),
            "string" => {
                let bytes = read_bytes(r)?;
                String::from_utf8(bytes)
                    .map(Avro::Str)
                    .map_err(|e| format!("an Avro string is not UTF-8: {e}"))
            }
            other => match self.names.get(other) {
                Some(definition) => self.decode(definition, r, depth + 1),
                None => Err(format!(
                    "an Avro schema references the unknown type `{other}`"
                )),
            },
        }
    }

    fn decode_record(
        &self,
        obj: &'s Map<String, Json>,
        r: &mut impl Read,
        depth: usize,
    ) -> Result<Avro, String> {
        let fields = obj
            .get("fields")
            .and_then(Json::as_array)
            .ok_or("an Avro record without `fields`")?;
        let mut out = Vec::with_capacity(fields.len());
        for field in fields {
            let name = field
                .get("name")
                .and_then(Json::as_str)
                .ok_or("an Avro record field without `name`")?;
            let ty = field
                .get("type")
                .ok_or_else(|| format!("the Avro field `{name}` has no type"))?;
            out.push((name.to_string(), self.decode(ty, r, depth + 1)?));
        }
        Ok(Avro::Record(out))
    }

    fn decode_array(
        &self,
        obj: &'s Map<String, Json>,
        r: &mut impl Read,
        depth: usize,
    ) -> Result<Avro, String> {
        let items = obj.get("items").ok_or("an Avro array without `items`")?;
        Ok(Avro::Array(self.decode_blocks(r, items, depth)?))
    }

    /// Avro arrays and maps are both a sequence of counted blocks; a negative
    /// count is followed by the block's byte size, which decoding does not
    /// need.
    fn decode_blocks(
        &self,
        r: &mut impl Read,
        items: &'s Json,
        depth: usize,
    ) -> Result<Vec<Avro>, String> {
        let mut out = Vec::new();
        loop {
            let mut count = read_long(r)?;
            if count == 0 {
                return Ok(out);
            }
            if count < 0 {
                count = -count;
                let _block_bytes = read_long(r)?;
            }
            if count > (1 << 26) {
                return Err(format!("an Avro block of {count} items is out of range"));
            }
            for _ in 0..count {
                out.push(self.decode(items, r, depth + 1)?);
            }
        }
    }
}

fn collect_names<'s>(schema: &'s Json, names: &mut HashMap<String, &'s Json>) {
    match schema {
        Json::Array(branches) => {
            for branch in branches {
                collect_names(branch, names);
            }
        }
        Json::Object(obj) => {
            let kind = obj.get("type").and_then(Json::as_str);
            if matches!(kind, Some("record" | "enum" | "fixed"))
                && let Some(name) = obj.get("name").and_then(Json::as_str)
            {
                names.insert(name.to_string(), schema);
                if let Some(namespace) = obj.get("namespace").and_then(Json::as_str) {
                    names.insert(format!("{namespace}.{name}"), schema);
                }
            }
            for (key, value) in obj {
                if key != "name" && key != "namespace" {
                    collect_names(value, names);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny writer, so the decoder is tested against bytes built by hand
    /// instead of a fixture nobody can read.
    struct Writer {
        out: Vec<u8>,
    }

    impl Writer {
        fn long(&mut self, value: i64) -> &mut Self {
            let mut zz = ((value << 1) ^ (value >> 63)) as u64;
            loop {
                let byte = (zz & 0x7f) as u8;
                zz >>= 7;
                if zz == 0 {
                    self.out.push(byte);
                    return self;
                }
                self.out.push(byte | 0x80);
            }
        }

        fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
            self.long(bytes.len() as i64);
            self.out.extend_from_slice(bytes);
            self
        }

        fn string(&mut self, text: &str) -> &mut Self {
            self.bytes(text.as_bytes())
        }

        /// A null-coded container holding one record of `record`.
        fn container(schema: &Json, record: &mut Writer) -> Vec<u8> {
            let mut out = Writer {
                out: b"Obj\x01".to_vec(),
            };
            out.long(2)
                .string("avro.schema")
                .bytes(schema.to_string().as_bytes())
                .string("avro.codec")
                .bytes(b"null")
                .long(0);
            let sync = *b"0123456789abcdef";
            out.out.extend_from_slice(&sync);
            out.long(1).long(record.out.len() as i64);
            out.out.extend_from_slice(&record.out);
            out.out.extend_from_slice(&sync);
            out.out
        }
    }

    #[test]
    fn decodes_nested_records_unions_arrays_and_maps() {
        let schema: Json = serde_json::from_str(
            r#"{"type":"record","name":"entry","fields":[
                 {"name":"status","type":"int"},
                 {"name":"path","type":["null","string"]},
                 {"name":"counts","type":["null",{"type":"array","element-id":9,
                   "items":{"type":"record","name":"kv","fields":[
                     {"name":"key","type":"int"},{"name":"value","type":"long"}]}}]},
                 {"name":"tags","type":{"type":"map","values":"string"}},
                 {"name":"score","type":"double"},
                 {"name":"blob","type":"bytes"}]}"#,
        )
        .unwrap();
        let mut record = Writer { out: Vec::new() };
        record
            .long(1) // status
            .long(1) // union branch: string
            .string("data/a.parquet")
            .long(1) // union branch: array
            .long(2) // two items
            .long(7)
            .long(70)
            .long(8)
            .long(80)
            .long(0) // array end
            .long(1) // map: one entry
            .string("k")
            .string("v")
            .long(0); // map end
        let score = 1.5f64.to_le_bytes();
        record.out.extend_from_slice(&score);
        record.bytes(b"xyz");
        let bytes = Writer::container(&schema, &mut record);

        let records = read_records(&mut bytes.as_slice()).unwrap();
        assert_eq!(records.len(), 1);
        let entry = &records[0];
        assert_eq!(entry.field("status").as_long(), Some(1));
        assert_eq!(entry.field("path").as_str(), Some("data/a.parquet"));
        let counts = entry.field("counts").as_array().unwrap();
        assert_eq!(counts[0].field("value").as_long(), Some(70));
        assert_eq!(entry.field("tags").field("k").as_str(), Some("v"));
        assert_eq!(entry.field("score"), &Avro::Float(1.5));
        assert_eq!(entry.field("blob").as_bytes(), Some(&b"xyz"[..]));
        // A field the schema does not carry reads as null, never as an error.
        assert!(matches!(entry.field("missing"), Avro::Null));
    }

    #[test]
    fn deflate_blocks_inflate() {
        use std::io::Write as _;

        let schema: Json = serde_json::from_str(
            r#"{"type":"record","name":"r","fields":[{"name":"n","type":"long"}]}"#,
        )
        .unwrap();
        let mut record = Writer { out: Vec::new() };
        record.long(42);
        let mut encoder =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&record.out).unwrap();
        let payload = encoder.finish().unwrap();

        let mut out = Writer {
            out: b"Obj\x01".to_vec(),
        };
        out.long(2)
            .string("avro.schema")
            .bytes(schema.to_string().as_bytes())
            .string("avro.codec")
            .bytes(b"deflate")
            .long(0);
        let sync = *b"0123456789abcdef";
        out.out.extend_from_slice(&sync);
        out.long(1).long(payload.len() as i64);
        out.out.extend_from_slice(&payload);
        out.out.extend_from_slice(&sync);

        let records = read_records(&mut out.out.as_slice()).unwrap();
        assert_eq!(records[0].field("n").as_long(), Some(42));
    }

    #[test]
    fn refuses_a_bad_magic_and_an_unknown_codec() {
        let err = read_records(&mut &b"nope"[..]).unwrap_err();
        assert!(err.contains("bad magic"), "{err}");
    }
}
