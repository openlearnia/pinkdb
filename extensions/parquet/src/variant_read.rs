//! The **read** half of Parquet's `Variant` logical type (wave 101b).
//!
//! A shredded variant column is a group of `metadata` (REQUIRED), `value`
//! (OPTIONAL) and `typed_value` (OPTIONAL), where `typed_value` mirrors the
//! object keys the writer chose to type. This module parses that group into
//! [`VariantColumn`], reads each leaf of it, and reconciles the two buffers
//! back into one [`Variant`] per row with
//! [`pinkdb::variant_shred::unshred_row`].
//!
//! Three shapes are supported, which is what DuckDB 2.0 writes and every
//! conforming writer may write:
//!
//! - a typed primitive (`INT64`, `BYTE_ARRAY`, `DATE`, …),
//! - a struct of further shredded object keys,
//! - a LIST of shredded elements (a `value`/`typed_value` pair per element).
//!
//! Anything deeper is refused with an error naming the column: silently
//! dropping a nested field would corrupt values.

use std::sync::Arc;

use parquet::basic::{ConvertedType, LogicalType, Repetition};
use parquet::data_type::{BoolType, ByteArrayType, DoubleType, FloatType, Int32Type, Int64Type};
use parquet::file::reader::RowGroupReader;
use parquet::schema::types::Type;
use pinkdb::variant::Variant;
use pinkdb::variant_shred::{self, ShredKind, decode_metadata, decode_value};
use pinkdb::{CHUNK_SIZE, DataType, PinkDbError, Result, Value as EngineValue, Vector};

/// One `{ value, typed_value }` pair of the spec — a shredded object field or
/// a shredded array element.
#[derive(Debug, Clone, PartialEq)]
pub struct Shredded {
    /// The leaf holding this node's residual bytes (absent when the writer
    /// emitted no `value` sibling, which the spec allows).
    pub value_leaf: Option<usize>,
    /// What the `typed_value` child holds; [`Node::Absent`] when there is
    /// none, so the node is only ever its residual bytes.
    pub node: Node,
    /// The definition level at which this node exists at all (its parent
    /// wrote a NULL below it otherwise).
    pub def_present: i16,
}

/// The typed part of a shredded node.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// A typed primitive leaf.
    Primitive {
        leaf: usize,
        /// The leaf's parquet width, so an `INT32`/`FLOAT` leaf still reads
        /// back as the engine's wider `Variant::Int`/`Double`.
        physical: parquet::basic::Type,
        kind: ShredKind,
    },
    /// A struct of further shredded keys.
    Object(Vec<(Arc<str>, Shredded)>),
    /// A list of shredded elements.
    Array(Box<Shredded>),
    /// No typed child: the node's value is entirely in its `value` bytes.
    Absent,
}

/// One shredded VARIANT column, parsed from the file's schema.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantColumn {
    /// The `metadata` dictionary leaf.
    pub metadata_leaf: usize,
    /// The residual leaf of the row's outer object.
    pub value_leaf: Option<usize>,
    /// The shredded keys, in schema order.
    pub keys: Vec<(Arc<str>, Shredded)>,
    /// The definition level at which a row's variant group exists: a row
    /// below it is SQL NULL.
    pub def_present: i16,
    /// Leaves the whole group occupies.
    pub leaves: usize,
}

impl VariantColumn {
    /// The engine type this column reads as.
    pub fn data_type(&self) -> DataType {
        DataType::Variant
    }
}

/// Parse one `Variant`-annotated parquet group.
///
/// Leaf ordinals are *relative to the group* (0 is `metadata`); the caller
/// adds the group's first absolute leaf when it reads, so a parsed column is
/// position-independent.
pub fn parse_column(field: &Type) -> Result<VariantColumn> {
    let mut next = 0usize;
    let def_present = if field.get_basic_info().has_repetition()
        && field.get_basic_info().repetition() == Repetition::OPTIONAL
    {
        1
    } else {
        0
    };
    let mut children = field.get_fields().iter();
    let metadata = children
        .next()
        .ok_or_else(|| malformed(field, "no metadata leaf"))?;
    if metadata.is_group() {
        return Err(malformed(field, "metadata is not a leaf"));
    }
    let metadata_leaf = next;
    next += 1;
    let mut value_leaf = None;
    let mut keys = Vec::new();
    if let Some(child) = children.next() {
        if child.name() == "value" {
            value_leaf = Some(next);
            next += 1;
        } else {
            return Err(malformed(field, "expected a `value` leaf"));
        }
        if let Some(typed) = children.next() {
            if typed.name() != "typed_value" {
                return Err(malformed(field, "expected a `typed_value` group"));
            }
            if !typed.is_group() {
                return Err(malformed(field, "`typed_value` is not a group"));
            }
            for key in typed.get_fields() {
                let (node, after) = parse_shredded(key, next, def_present + 1)?;
                next = after;
                keys.push((Arc::from(key.name()), node));
            }
        }
    }
    Ok(VariantColumn {
        metadata_leaf,
        value_leaf,
        keys,
        def_present,
        leaves: next,
    })
}

/// Parse one `{ value, typed_value }` pair whose presence level is
/// `def_present`.
fn parse_shredded(field: &Type, first_leaf: usize, def_present: i16) -> Result<(Shredded, usize)> {
    let mut next = first_leaf;
    let mut children = field.get_fields().iter();
    let mut value_leaf = None;
    let mut node = Node::Absent;
    if let Some(child) = children.next() {
        if child.name() == "value" && !child.is_group() {
            value_leaf = Some(next);
            next += 1;
        } else {
            return Err(malformed(field, "expected a `value` leaf"));
        }
        if let Some(typed) = children.next() {
            let (parsed, after) = parse_typed(typed, next, def_present)?;
            node = parsed;
            next = after;
        }
    }
    if next == first_leaf {
        return Err(malformed(field, "a shredded node has no leaves"));
    }
    Ok((
        Shredded {
            value_leaf,
            node,
            def_present,
        },
        next,
    ))
}

/// Parse a `typed_value` child: a primitive leaf, a LIST, or a struct of
/// shredded keys.
fn parse_typed(field: &Type, first_leaf: usize, def_present: i16) -> Result<(Node, usize)> {
    if !field.is_group() {
        let kind = shred_kind(field)?;
        return Ok((
            Node::Primitive {
                leaf: first_leaf,
                physical: field.get_physical_type(),
                kind,
            },
            first_leaf + 1,
        ));
    }
    if is_list(field) {
        // LIST → repeated `list` group → `element`, or the legacy form where
        // the repeated field is the element itself.
        let repeated = field
            .get_fields()
            .iter()
            .find(|f| {
                f.get_basic_info().has_repetition()
                    && f.get_basic_info().repetition() == Repetition::REPEATED
            })
            .ok_or_else(|| malformed(field, "a LIST group has no repeated child"))?;
        let element = if repeated.is_group() && repeated.get_fields().len() == 1 {
            &repeated.get_fields()[0]
        } else {
            repeated
        };
        // A repeated node adds one definition level.
        let (parsed, after) = parse_shredded(element, first_leaf, def_present + 1)?;
        return Ok((Node::Array(Box::new(parsed)), after));
    }
    // A plain struct: each child is a shredded key.
    let mut keys = Vec::new();
    let mut next = first_leaf;
    for child in field.get_fields() {
        let (parsed, after) = parse_shredded(child, next, def_present + 1)?;
        next = after;
        keys.push((Arc::from(child.name()), parsed));
    }
    Ok((Node::Object(keys), next))
}

/// Whether a group is a Parquet LIST.
fn is_list(field: &Type) -> bool {
    let info = field.get_basic_info();
    matches!(info.converted_type(), ConvertedType::LIST)
        || matches!(info.logical_type_ref(), Some(LogicalType::List))
}

/// The shredded kind of one typed leaf.
fn shred_kind(field: &Type) -> Result<ShredKind> {
    use parquet::basic::Type as Physical;
    let info = field.get_basic_info();
    Ok(match field.get_physical_type() {
        Physical::BOOLEAN => ShredKind::Bool,
        Physical::INT32 => match info.logical_type_ref() {
            Some(LogicalType::Date) => ShredKind::Date,
            // The spec allows INT8/16/32 as shredded targets for an integer;
            // pinkdb widens them all into `Variant::Int`.
            _ => ShredKind::Int64,
        },
        Physical::INT64 => match info.logical_type_ref() {
            Some(LogicalType::Timestamp(_)) => ShredKind::Timestamp,
            _ => ShredKind::Int64,
        },
        Physical::DOUBLE | Physical::FLOAT => ShredKind::Double,
        Physical::BYTE_ARRAY => ShredKind::Str,
        other => {
            return Err(PinkDbError::Type(format!(
                "shredded VARIANT leaf \"{}\" has an unsupported type {other:?}",
                field.name()
            )));
        }
    })
}

fn malformed(field: &Type, why: &str) -> PinkDbError {
    PinkDbError::Type(format!(
        "column \"{}\" is a Parquet Variant group this reader cannot follow: {why}",
        field.name()
    ))
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// One leaf value, before it becomes a [`Variant`].
#[derive(Debug, Clone, PartialEq)]
enum LeafValue {
    Bool(bool),
    Int(i64),
    Double(f64),
    /// A range of the leaf's byte pool: a shredded column has a byte value in
    /// most rows, and one allocation per row for it would dominate the read.
    Bytes {
        start: u32,
        len: u32,
    },
}

impl LeafValue {
    fn into_variant(self, kind: ShredKind, bytes: Option<&[u8]>) -> Result<Variant> {
        Ok(match (kind, self) {
            (ShredKind::Bool, LeafValue::Bool(b)) => Variant::Bool(b),
            (ShredKind::Int64, LeafValue::Int(i)) => Variant::Int(i),
            (ShredKind::Date, LeafValue::Int(i)) => Variant::Date(i as i32),
            (ShredKind::Timestamp, LeafValue::Int(us)) => Variant::Timestamp(us),
            (ShredKind::Double, LeafValue::Double(x)) => Variant::Double(x),
            (ShredKind::Str, LeafValue::Bytes { .. }) => {
                let bytes = bytes.ok_or_else(|| {
                    PinkDbError::Execution("a shredded VARIANT string has no bytes".to_string())
                })?;
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    PinkDbError::Execution(
                        "a shredded VARIANT string is not valid UTF-8".to_string(),
                    )
                })?;
                Variant::Str(Arc::from(text))
            }
            (kind, _) => {
                return Err(PinkDbError::Execution(format!(
                    "a shredded VARIANT {} leaf holds another kind",
                    kind.name()
                )));
            }
        })
    }
}

/// One record of a leaf: its definition level and, when fully defined, the
/// value.
#[derive(Debug, Clone, PartialEq)]
struct Entry {
    def: i16,
    value: Option<LeafValue>,
}

/// One leaf's records, grouped by row.
///
/// Entries are stored flat with one offset per row: a shredded row owns a
/// record in every leaf of its column, so a `Vec` per row would allocate
/// millions of tiny vectors for a large file.
#[derive(Debug)]
struct LeafData {
    entries: Vec<Entry>,
    /// `offsets[row]` is the first entry of `row`; the last offset is the end.
    offsets: Vec<u32>,
    /// Every byte value of the leaf, back to back.
    bytes: Vec<u8>,
    max_def: i16,
}

impl LeafData {
    /// The bytes of a `LeafValue::Bytes` entry.
    fn value_bytes(&self, entry: &Entry) -> Option<&[u8]> {
        match &entry.value {
            Some(LeafValue::Bytes { start, len }) => {
                let start = *start as usize;
                self.bytes.get(start..start + *len as usize)
            }
            _ => None,
        }
    }

    fn row(&self, row: usize) -> &[Entry] {
        let start = self
            .offsets
            .get(row)
            .copied()
            .unwrap_or_default()
            .min(self.entries.len() as u32) as usize;
        let end = self
            .offsets
            .get(row + 1)
            .copied()
            .unwrap_or(self.entries.len() as u32) as usize;
        &self.entries[start..end.max(start)]
    }
}

/// Decode a shredded VARIANT column of one row group into engine vectors.
pub fn read_column(
    row_group: &dyn RowGroupReader,
    first_leaf: usize,
    column: &VariantColumn,
    rows: usize,
    name: &str,
) -> Result<Vec<Vector>> {
    let mut leaves = Vec::with_capacity(column.leaves);
    for offset in 0..column.leaves {
        leaves.push(read_leaf(row_group, first_leaf + offset, rows, name)?);
    }
    let mut out = Vec::with_capacity(rows.div_ceil(CHUNK_SIZE));
    let mut done = 0usize;
    let mut dictionary: Option<(Vec<u8>, Vec<Arc<str>>)> = None;
    while done < rows {
        let count = CHUNK_SIZE.min(rows - done);
        let mut vector = Vector::with_capacity(DataType::Variant, count);
        for row in done..done + count {
            let keys = dictionary_for(row, column, &leaves, &mut dictionary)?;
            let value = match keys {
                None => None,
                Some(keys) => Some(read_row(row, column, &leaves, keys, name)?),
            };

            match value {
                Some(value) => vector.push_value(EngineValue::Variant(Arc::new(value)))?,
                None => vector.push_null()?,
            }
        }
        out.push(vector);
        done += count;
    }
    Ok(out)
}

/// The row's key dictionary.
///
/// The spec describes one dictionary per column, and pinkdb writes one, but
/// DuckDB 2.0 writes a per-row dictionary holding only that row's keys (and a
/// row may label the same field id differently), so each row's buffer must be
/// decoded on its own. The last buffer and its keys are kept: identical
/// dictionaries — the common case — cost a comparison, not a decode.
fn dictionary_for<'a>(
    row: usize,
    column: &VariantColumn,
    leaves: &[LeafData],
    cache: &'a mut Option<(Vec<u8>, Vec<Arc<str>>)>,
) -> Result<Option<&'a [Arc<str>]>> {
    let leaf = &leaves[column.metadata_leaf];
    let Some(entry) = leaf.row(row).first() else {
        return Err(PinkDbError::Execution(
            "a VARIANT row has no metadata record".to_string(),
        ));
    };
    if entry.def < column.def_present {
        return Ok(None);
    }
    let Some(bytes) = leaf.value_bytes(entry) else {
        return Err(PinkDbError::Execution(
            "a VARIANT metadata record has no bytes".to_string(),
        ));
    };
    if cache
        .as_ref()
        .is_none_or(|(seen, _)| seen.as_slice() != bytes)
    {
        *cache = Some((bytes.to_vec(), decode_metadata(bytes)?));
    }
    Ok(cache.as_ref().map(|(_, keys)| keys.as_slice()))
}

/// Rebuild one row's value: the residual object reconciled with the shredded
/// keys.
fn read_row(
    row: usize,
    column: &VariantColumn,
    leaves: &[LeafData],
    keys: &[Arc<str>],
    name: &str,
) -> Result<Variant> {
    let residual = match column.value_leaf {
        Some(leaf) => leaf_entry(leaves, leaf, row, 0)
            .filter(|entry| entry.def == leaves[leaf].max_def)
            .and_then(|entry| leaves[leaf].value_bytes(entry))
            .map(|bytes| decode_value(bytes, keys))
            .transpose()?,
        None => None,
    };
    let mut typed: Vec<(&str, Option<Variant>)> = Vec::with_capacity(column.keys.len());
    for (key, node) in &column.keys {
        typed.push((key.as_ref(), node_value(node, row, 0, leaves, keys)?));
    }
    variant_shred::unshred_row(residual.as_ref(), &typed).map_err(|e| match e {
        PinkDbError::Execution(message) => {
            PinkDbError::Execution(format!("column \"{name}\" row {row}: {message}"))
        }
        other => other,
    })
}

/// The `position`-th record of one leaf at `row`.
fn leaf_entry(leaves: &[LeafData], leaf: usize, row: usize, position: usize) -> Option<&Entry> {
    leaves.get(leaf)?.row(row).get(position)
}

/// One shredded node's value for a row (and list position).
fn node_value(
    node: &Shredded,
    row: usize,
    position: usize,
    leaves: &[LeafData],
    keys: &[Arc<str>],
) -> Result<Option<Variant>> {
    // The node exists when its first leaf carries a definition level at or
    // above the node's own presence level.
    let first = node
        .value_leaf
        .or_else(|| node_first_leaf(&node.node))
        .ok_or_else(|| {
            PinkDbError::Execution("a shredded VARIANT node has no leaves".to_string())
        })?;
    let Some(entry) = leaf_entry(leaves, first, row, position) else {
        return Ok(None);
    };
    if entry.def < node.def_present {
        return Ok(None);
    }
    // The typed child wins (the spec forbids both being set).
    if let Some(value) = typed_value(&node.node, row, position, leaves, keys)? {
        return Ok(Some(value));
    }
    let Some(leaf) = node.value_leaf else {
        return Ok(None);
    };
    let Some(entry) = leaf_entry(leaves, leaf, row, position) else {
        return Ok(None);
    };
    if entry.def != leaves[leaf].max_def {
        return Ok(None);
    }
    match leaves[leaf].value_bytes(entry) {
        Some(bytes) => Ok(Some(decode_value(bytes, keys)?)),
        None => Ok(None),
    }
}

/// The value of a node's `typed_value` child.
fn typed_value(
    node: &Node,
    row: usize,
    position: usize,
    leaves: &[LeafData],
    keys: &[Arc<str>],
) -> Result<Option<Variant>> {
    match node {
        Node::Absent => Ok(None),
        Node::Primitive {
            leaf,
            physical: _,
            kind,
        } => {
            let Some(entry) = leaf_entry(leaves, *leaf, row, position) else {
                return Ok(None);
            };
            if entry.def != leaves[*leaf].max_def {
                return Ok(None);
            }
            match &entry.value {
                Some(value) => Ok(Some(
                    value
                        .clone()
                        .into_variant(*kind, leaves[*leaf].value_bytes(entry))?,
                )),
                None => Ok(None),
            }
        }
        Node::Object(fields) => {
            let mut typed: Vec<(&str, Option<Variant>)> = Vec::with_capacity(fields.len());
            let mut any = false;
            for (key, field) in fields {
                let value = node_value(field, row, position, leaves, keys)?;
                any |= value.is_some();
                typed.push((key.as_ref(), value));
            }
            if !any {
                return Ok(None);
            }
            Ok(Some(variant_shred::unshred_row(None, &typed)?))
        }
        Node::Array(element) => {
            let Some(first) = element
                .value_leaf
                .or_else(|| node_first_leaf(&element.node))
            else {
                return Ok(None);
            };
            let Some(entries) = leaves.get(first).map(|leaf| leaf.row(row)) else {
                return Ok(None);
            };
            let mut items = Vec::new();
            for index in 0..entries.len() {
                if let Some(value) = node_value(element, row, index, leaves, keys)? {
                    items.push(value);
                }
            }
            Variant::array(items).map(Some)
        }
    }
}

/// The first leaf of a node's subtree.
fn node_first_leaf(node: &Node) -> Option<usize> {
    match node {
        Node::Absent => None,
        Node::Primitive { leaf, .. } => Some(*leaf),
        Node::Object(fields) => fields
            .iter()
            .find_map(|(_, field)| field.value_leaf.or_else(|| node_first_leaf(&field.node))),
        Node::Array(element) => element
            .value_leaf
            .or_else(|| node_first_leaf(&element.node)),
    }
}

/// Read one leaf's records, grouped by row.
fn read_leaf(
    row_group: &dyn RowGroupReader,
    leaf: usize,
    rows: usize,
    column: &str,
) -> Result<LeafData> {
    let max_def = row_group
        .metadata()
        .column(leaf)
        .column_descr()
        .max_def_level();
    let reader = row_group.get_column_reader(leaf).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot read VARIANT column \"{column}\" (leaf {leaf}): {e}"
        ))
    })?;
    macro_rules! collect {
        ($reader:expr, $ty:ty, $wrap:expr) => {
            collect_leaf::<$ty>($reader, rows, max_def, leaf, column, $wrap)
        };
    }
    match reader {
        parquet::column::reader::ColumnReader::BoolColumnReader(r) => {
            collect!(r, BoolType, |v: &bool, _: &mut Vec<u8>| LeafValue::Bool(*v))
        }
        parquet::column::reader::ColumnReader::Int32ColumnReader(r) => {
            collect!(r, Int32Type, |v: &i32, _: &mut Vec<u8>| LeafValue::Int(
                i64::from(*v)
            ))
        }
        parquet::column::reader::ColumnReader::Int64ColumnReader(r) => {
            collect!(r, Int64Type, |v: &i64, _: &mut Vec<u8>| LeafValue::Int(*v))
        }
        parquet::column::reader::ColumnReader::FloatColumnReader(r) => {
            collect!(r, FloatType, |v: &f32, _: &mut Vec<u8>| LeafValue::Double(
                f64::from(*v)
            ))
        }
        parquet::column::reader::ColumnReader::DoubleColumnReader(r) => {
            collect!(r, DoubleType, |v: &f64, _: &mut Vec<u8>| LeafValue::Double(
                *v
            ))
        }
        parquet::column::reader::ColumnReader::ByteArrayColumnReader(r) => {
            collect!(
                r,
                ByteArrayType,
                |v: &parquet::data_type::ByteArray, pool: &mut Vec<u8>| {
                    let start = pool.len() as u32;
                    pool.extend_from_slice(v.data());
                    LeafValue::Bytes {
                        start,
                        len: v.data().len() as u32,
                    }
                }
            )
        }
        _ => Err(PinkDbError::Type(format!(
            "VARIANT column \"{column}\" (leaf {leaf}) has a leaf type pinkdb cannot \
             decode as a shredded value"
        ))),
    }
}

/// Records requested per `read_records` call. Any positive value works: the
/// split into rows ignores the chunking, and `read_records` never splits one
/// row's repeated records across calls in a way that loses them.
const READ_RECORDS: usize = 1024;

/// Read every record of one leaf, splitting rows at repetition level 0.
fn collect_leaf<T: parquet::data_type::DataType>(
    mut reader: parquet::column::reader::ColumnReaderImpl<T>,
    rows: usize,
    max_def: i16,
    leaf: usize,
    column: &str,
    wrap: impl Fn(&T::T, &mut Vec<u8>) -> LeafValue,
) -> Result<LeafData> {
    let mut out: Vec<Entry> = Vec::with_capacity(rows);
    let mut offsets: Vec<u32> = Vec::with_capacity(rows + 1);
    let mut bytes: Vec<u8> = Vec::new();
    let mut values: Vec<T::T> = Vec::new();
    let mut defs: Vec<i16> = Vec::new();
    let mut reps: Vec<i16> = Vec::new();
    loop {
        values.clear();
        defs.clear();
        reps.clear();
        let (records, _, _) = reader
            .read_records(READ_RECORDS, Some(&mut defs), Some(&mut reps), &mut values)
            .map_err(|e| {
                PinkDbError::Execution(format!(
                    "cannot decode VARIANT column \"{column}\" (leaf {leaf}): {e}"
                ))
            })?;
        if records == 0 {
            break;
        }
        // `records` counts complete records while `defs`/`reps` hold one entry
        // per *level*: a repeated leaf emits several levels for one record,
        // and a required leaf (`max_def == 0`) fills neither buffer.
        let levels = if !reps.is_empty() {
            reps.len()
        } else if !defs.is_empty() {
            defs.len()
        } else {
            records
        };
        let mut next = 0usize;
        for level in 0..levels {
            let def = defs.get(level).copied().unwrap_or(max_def);
            let rep = reps.get(level).copied().unwrap_or(0);
            // A level with repetition level 0 starts a new row; any other
            // level continues the row of the level before it.
            if rep == 0 || out.is_empty() {
                offsets.push(out.len() as u32);
            }
            let value = if def == max_def {
                let Some(value) = values.get(next) else {
                    return Err(PinkDbError::Execution(format!(
                        "VARIANT column \"{column}\" (leaf {leaf}): a definition level has no \
                         value"
                    )));
                };
                next += 1;
                Some(wrap(value, &mut bytes))
            } else {
                None
            };
            out.push(Entry { def, value });
        }
    }
    // Every row has at least one record per leaf (a low definition level for
    // an absent value), so a short leaf is a corrupt or misunderstood file.
    if offsets.len() != rows {
        return Err(PinkDbError::Execution(format!(
            "VARIANT column \"{column}\" (leaf {leaf}) ended after {} of {rows} rows",
            offsets.len()
        )));
    }
    // The offsets and byte ranges are `u32`; a single row group that large is
    // not a pinkdb file, so refuse it instead of truncating the offsets.
    if out.len() > u32::MAX as usize || bytes.len() > u32::MAX as usize {
        return Err(PinkDbError::Execution(format!(
            "VARIANT column \"{column}\" (leaf {leaf}) is too large to decode in one row group"
        )));
    }
    offsets.push(out.len() as u32);
    Ok(LeafData {
        entries: out,
        offsets,
        bytes,
        max_def,
    })
}
