//! LIST and STRUCT: variable-length lists and named-field records, interned
//! so [`DataType`] stays a small `Copy` value.
//!
//! A nested type is recursive (a LIST of STRUCTs of LISTs …), which an enum
//! that is `Copy` cannot hold directly. Instead `DataType::List(id)` and
//! `DataType::Struct(id)` carry a [`NestedId`]: an index into a process-wide,
//! append-only interner of [`NestedType`]s. Interning dedups, so two equal
//! types always get the same id and `DataType`'s derived `Eq`/`Hash` stay
//! structural. Ids live only in memory: every format that persists or sends a
//! type (the `.pink` footer, the WAL, the wire protocol, Parquet, the C ABI)
//! writes the structure and interns it again when it reads it back.
//!
//! The vector layout follows DuckDB's (`src/common/types/list_vector.cpp`,
//! `struct_vector.cpp`) with one change: a LIST vector keeps Arrow-style
//! offsets (`rows + 1` of them) over one child vector instead of DuckDB's
//! `(offset, length)` entries, so Arrow export and the Parquet writer use the
//! buffers as they are. A STRUCT vector keeps one child vector per field;
//! the field names live in the type.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};

use crate::types::{DataType, Value};

/// An interned nested type (see the module docs). Only [`intern`] makes one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NestedId(u32);

impl fmt::Debug for NestedId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The structure, not the process-local number.
        write!(f, "{}", get(*self).name())
    }
}

/// What a [`NestedId`] stands for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NestedType {
    /// A variable-length list of one element type.
    List(DataType),
    /// Named fields in order. Names are case-sensitive and unique.
    Struct(Arc<[(Arc<str>, DataType)]>),
}

impl NestedType {
    /// DuckDB's spelling: `INTEGER[]`, `STRUCT(a INTEGER, b VARCHAR[])`.
    pub fn name(&self) -> String {
        match self {
            NestedType::List(elem) => format!("{}[]", elem.name()),
            NestedType::Struct(fields) => {
                let fields: Vec<String> = fields
                    .iter()
                    .map(|(name, t)| format!("{} {}", quote_field(name), t.name()))
                    .collect();
                format!("STRUCT({})", fields.join(", "))
            }
        }
    }
}

/// A field name as DuckDB prints it in a type: bare when it is a plain
/// lowercase identifier, double-quoted otherwise.
fn quote_field(name: &str) -> String {
    let plain = !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[derive(Default)]
struct Interner {
    types: Vec<Arc<NestedType>>,
    ids: HashMap<Arc<NestedType>, u32>,
}

fn interner() -> &'static RwLock<Interner> {
    static INTERNER: OnceLock<RwLock<Interner>> = OnceLock::new();
    INTERNER.get_or_init(|| RwLock::new(Interner::default()))
}

/// The id of `t`, interning it the first time it is seen.
pub fn intern(t: NestedType) -> NestedId {
    if let Some(&id) = interner()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .ids
        .get(&t)
    {
        return NestedId(id);
    }
    let mut w = interner().write().unwrap_or_else(|e| e.into_inner());
    if let Some(&id) = w.ids.get(&t) {
        return NestedId(id);
    }
    let id = u32::try_from(w.types.len()).expect("fewer than 2^32 nested types");
    let t = Arc::new(t);
    w.types.push(Arc::clone(&t));
    w.ids.insert(t, id);
    NestedId(id)
}

/// The type an id stands for.
pub fn get(id: NestedId) -> Arc<NestedType> {
    Arc::clone(
        &interner()
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .types[id.0 as usize],
    )
}

/// `elem[]`.
pub fn list_type(elem: DataType) -> DataType {
    DataType::List(intern(NestedType::List(elem)))
}

/// `STRUCT(fields…)`. Field names must be unique (case-sensitive), which the
/// caller checks; an empty struct is allowed only as an internal type.
pub fn struct_type(fields: Vec<(String, DataType)>) -> DataType {
    DataType::Struct(intern(NestedType::Struct(
        fields
            .into_iter()
            .map(|(name, t)| (Arc::<str>::from(name), t))
            .collect(),
    )))
}

/// One LIST value: its LIST type and its elements (a NULL element is
/// `Value::Null`).
#[derive(Debug, Clone)]
pub struct ListValue {
    pub ty: DataType,
    pub items: Vec<Value>,
}

impl ListValue {
    /// The element type.
    pub fn elem(&self) -> DataType {
        self.ty
            .list_elem()
            .expect("a ListValue carries a LIST type")
    }
}

/// One STRUCT value: its STRUCT type and one value per field, in field order.
#[derive(Debug, Clone)]
pub struct StructValue {
    pub ty: DataType,
    pub fields: Vec<Value>,
}

impl StructValue {
    /// The field names and types.
    pub fn schema(&self) -> Arc<[(Arc<str>, DataType)]> {
        self.ty
            .struct_fields()
            .expect("a StructValue carries a STRUCT type")
    }
}

/// DuckDB's text for a LIST: `[1, 2, NULL]`, strings bare inside (`[a, b]`).
pub fn list_text(items: &[Value]) -> String {
    let mut out = String::from("[");
    for (i, v) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&nested_item_text(v));
    }
    out.push(']');
    out
}

/// DuckDB's text for a STRUCT: `{'a': 1, 'b': hello}`.
pub fn struct_text(fields: &[(Arc<str>, DataType)], values: &[Value]) -> String {
    let mut out = String::from("{");
    for (i, ((name, _), v)) in fields.iter().zip(values).enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('\'');
        out.push_str(&name.replace('\'', "''"));
        out.push_str("': ");
        out.push_str(&nested_item_text(v));
    }
    out.push('}');
    out
}

fn nested_item_text(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}
