//! Arrow C Data Interface (ABI 1.3).
//!
//! The C structs below are the standard `ArrowSchema`/`ArrowArray` layouts. A
//! schema or array owns everything it points at through `private_data`; its
//! release callback frees that private data and sets `release` to NULL. Child
//! arrays/schemas have their own private data, so a child moved out by hand
//! (with the parent slot set to NULL) remains valid after the parent is freed.

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::Arc;

use pinkdb::array::ArrayElems;
use pinkdb::{ArrayElem, DataChunk, DataType, Vector, VectorData};

use crate::{CLASS_INTERNAL, CLASS_INVALID_ARGUMENT, CLASS_TYPE, PinkResult, StmtData, stmt_at};

/// The Arrow C Data Interface schema struct.
#[repr(C)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

/// The Arrow C Data Interface array struct.
#[repr(C)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

impl ArrowSchema {
    /// A released/empty schema: `release == NULL`, every pointer NULL.
    pub const fn empty() -> ArrowSchema {
        ArrowSchema {
            format: ptr::null(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

impl ArrowArray {
    /// A released/empty array: `release == NULL`, every pointer NULL.
    pub const fn empty() -> ArrowArray {
        ArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: ptr::null_mut(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

/// An export failure with the public error class it should be reported under.
pub(crate) struct ExportError {
    class: &'static CStr,
    message: String,
}

impl ExportError {
    fn invalid(message: impl Into<String>) -> ExportError {
        ExportError {
            class: CLASS_INVALID_ARGUMENT,
            message: message.into(),
        }
    }

    fn type_(message: impl Into<String>) -> ExportError {
        ExportError {
            class: CLASS_TYPE,
            message: message.into(),
        }
    }

    pub(crate) fn report(self) {
        crate::set_error(self.class, &self.message);
    }
}

fn checked_len(len: usize) -> Result<i64, ExportError> {
    i64::try_from(len).map_err(|_| ExportError::invalid("row count does not fit in i64"))
}

// ----- schema -----

struct SchemaPrivate {
    format: CString,
    name: Option<CString>,
    metadata: Option<Vec<u8>>,
    children: Box<[*mut ArrowSchema]>,
}

struct SchemaNode {
    format: CString,
    name: Option<CString>,
    metadata: Option<Vec<u8>>,
    children: Vec<SchemaNode>,
}

impl SchemaNode {
    fn new(
        dtype: &DataType,
        name: Option<&str>,
        metadata: Option<Vec<u8>>,
    ) -> Result<Self, ExportError> {
        let format = CString::new(format_for(dtype)?)
            .map_err(|_| ExportError::type_("Arrow format contains NUL"))?;
        let name = name
            .map(CString::new)
            .transpose()
            .map_err(|_| ExportError::invalid("column name contains NUL"))?;
        let children = match dtype {
            DataType::Array { elem, .. } => vec![SchemaNode::leaf(
                array_elem_format(*elem),
                Some("item"),
                None,
            )],
            _ => Vec::new(),
        };
        Ok(SchemaNode {
            format,
            name,
            metadata,
            children,
        })
    }

    fn leaf(format: &str, name: Option<&str>, metadata: Option<Vec<u8>>) -> Self {
        SchemaNode {
            format: CString::new(format).expect("literal"),
            name: name.map(|name| CString::new(name).expect("literal")),
            metadata,
            children: Vec::new(),
        }
    }

    fn into_raw(self) -> *mut ArrowSchema {
        let children = self
            .children
            .into_iter()
            .map(SchemaNode::into_raw)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut private = Box::new(SchemaPrivate {
            format: self.format,
            name: self.name,
            metadata: self.metadata,
            children,
        });
        let n_children = private.children.len();
        let child_ptr = private.children.as_mut_ptr();
        let schema = ArrowSchema {
            format: private.format.as_ptr(),
            name: private
                .name
                .as_ref()
                .map_or(ptr::null(), |name| name.as_ptr()),
            metadata: private
                .metadata
                .as_ref()
                .map_or(ptr::null(), |metadata| metadata.as_ptr().cast()),
            flags: if private.name.is_some() { 2 } else { 0 },
            n_children: n_children as i64,
            children: if n_children == 0 {
                ptr::null_mut()
            } else {
                child_ptr
            },
            dictionary: ptr::null_mut(),
            release: Some(release_schema),
            private_data: Box::into_raw(private).cast(),
        };
        Box::into_raw(Box::new(schema))
    }
}

fn array_elem_format(elem: ArrayElem) -> &'static str {
    match elem {
        ArrayElem::Integer => "i",
        ArrayElem::Bigint => "l",
        ArrayElem::Float => "f",
        ArrayElem::Double => "g",
    }
}

fn format_for(dtype: &DataType) -> Result<String, ExportError> {
    Ok(match dtype {
        DataType::Boolean => "b".to_string(),
        DataType::Integer => "i".to_string(),
        DataType::Bigint => "l".to_string(),
        DataType::Double => "g".to_string(),
        DataType::Varchar => "u".to_string(),
        DataType::Date => "tdD".to_string(),
        DataType::Timestamp => "tsu:".to_string(),
        DataType::Decimal { precision, scale } => {
            if !(1..=38).contains(precision) || scale > precision {
                return Err(ExportError::type_(format!(
                    "DECIMAL({precision},{scale}) cannot be represented as Arrow decimal128"
                )));
            }
            format!("d:{precision},{scale}")
        }
        DataType::Interval => "tin".to_string(),
        DataType::Variant => "u".to_string(),
        DataType::Uuid => "w:16".to_string(),
        // decimal128 holds the full i128 bit pattern, but Arrow promises at
        // most 38 digits; a value past that fails the export (see
        // `fit_decimal38`) instead of the whole column turning into text.
        DataType::Hugeint | DataType::Uhugeint => "d:38,0".to_string(),
        DataType::Array { len, .. } => return Ok(format!("+w:{len}")),
    })
}

fn extension_metadata(name: &str) -> Vec<u8> {
    let key = b"ARROW:extension:name";
    let value = name.as_bytes();
    let mut out = Vec::with_capacity(4 + 4 + key.len() + 4 + value.len());
    out.extend_from_slice(&1i32.to_ne_bytes());
    out.extend_from_slice(&(key.len() as i32).to_ne_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&(value.len() as i32).to_ne_bytes());
    out.extend_from_slice(value);
    out
}

fn metadata_for(dtype: &DataType) -> Option<Vec<u8>> {
    match dtype {
        DataType::Uuid => Some(extension_metadata("arrow.uuid")),
        DataType::Variant => Some(extension_metadata("pinkdb.variant")),
        _ => None,
    }
}

fn child_name(name: &[u8]) -> Result<&str, ExportError> {
    let name = name.strip_suffix(&[0]).unwrap_or(name);
    std::str::from_utf8(name).map_err(|_| ExportError::invalid("column name is not UTF-8"))
}

pub(crate) fn result_schema(
    result: *const PinkResult,
    statement: usize,
    out: *mut ArrowSchema,
) -> Result<(), ExportError> {
    if out.is_null() {
        return Err(ExportError::invalid("null ArrowSchema output"));
    }
    // SAFETY: out is non-null and writable per the C contract.
    unsafe { ptr::write(out, ArrowSchema::empty()) };
    if result.is_null() {
        return Err(ExportError::invalid("null result"));
    }
    // SAFETY: non-null, valid for the call.
    let result_ref = unsafe { &*result };
    if let Some(message) = &result_ref.error {
        return Err(ExportError {
            class: result_ref.error_class.unwrap_or(CLASS_INTERNAL),
            message: message.to_string_lossy().into_owned(),
        });
    }
    let Some(StmtData::Sel(sel)) = stmt_at(result, statement) else {
        return Err(ExportError::invalid(format!(
            "statement {statement} is not a SELECT"
        )));
    };
    let mut children = Vec::with_capacity(sel.dtypes.len());
    for (column, dtype) in sel.dtypes.iter().enumerate() {
        let name = sel
            .names
            .get(column)
            .map(|name| child_name(name))
            .transpose()?
            .unwrap_or("");
        children.push(SchemaNode::new(dtype, Some(name), metadata_for(dtype))?);
    }
    let root = SchemaNode {
        format: CString::new("+s").expect("literal"),
        name: None,
        metadata: None,
        children,
    };
    let root = root.into_raw();
    // SAFETY: out is non-null and writable.
    unsafe { ptr::write(out, std::ptr::read(root)) };
    // SAFETY: root was just allocated by `into_raw` and its contents moved out.
    drop(unsafe { Box::from_raw(root) });
    Ok(())
}

unsafe extern "C" fn release_schema(schema: *mut ArrowSchema) {
    if schema.is_null() {
        return;
    }
    // SAFETY: non-null, owned by the caller.
    let schema = unsafe { &mut *schema };
    let private = schema.private_data.cast::<SchemaPrivate>();
    if !private.is_null() {
        // SAFETY: private_data was produced by Box::into_raw in `into_raw`.
        let mut private = unsafe { Box::from_raw(private) };
        for child in private.children.iter_mut() {
            let child = std::mem::replace(child, ptr::null_mut());
            // SAFETY: the child pointer came from this schema's private data.
            unsafe { release_child_schema(child) };
        }
        schema.private_data = ptr::null_mut();
    }
    schema.release = None;
    schema.format = ptr::null();
    schema.name = ptr::null();
    schema.metadata = ptr::null();
    schema.flags = 0;
    schema.n_children = 0;
    schema.children = ptr::null_mut();
    schema.dictionary = ptr::null_mut();
}

unsafe fn release_child_schema(child: *mut ArrowSchema) {
    if child.is_null() {
        return;
    }
    // SAFETY: a child pointer allocated by `SchemaNode::into_raw`.
    let release = unsafe { (*child).release };
    if let Some(release) = release {
        // SAFETY: the callback belongs to this child.
        unsafe { release(child) };
    }
    // SAFETY: the child struct itself was allocated with Box.
    drop(unsafe { Box::from_raw(child) });
}

// ----- arrays -----

#[repr(C)]
#[derive(Clone, Copy)]
struct IntervalNano {
    months: i32,
    days: i32,
    nanos: i64,
}

#[allow(dead_code)]
enum Payload {
    None,
    Boolean(Vec<u8>),
    Utf8 {
        offsets: Vec<i32>,
        data: Vec<u8>,
    },
    Interval(Vec<IntervalNano>),
    Uuid(Vec<[u8; 16]>),
    Int32(Vec<i32>),
    /// A 64-bit DECIMAL payload widened to Arrow's decimal128 (wave 142).
    Decimal128(Vec<i128>),
}

struct ArrayPrivate {
    _chunk: Option<DataChunk>,
    _vector: Option<Arc<Vector>>,
    _payload: Payload,
    buffers: Box<[*const c_void]>,
    children: Box<[*mut ArrowArray]>,
}

pub(crate) fn chunk_to_arrow(chunk: &DataChunk, out: *mut ArrowArray) -> Result<(), ExportError> {
    let length = checked_len(chunk.num_rows)?;
    let mut children = Vec::with_capacity(chunk.columns.len());
    for column in &chunk.columns {
        match build_column(column.clone()) {
            Ok(child) => children.push(child),
            Err(error) => {
                for child in children {
                    // SAFETY: the children were just allocated by this function.
                    unsafe { release_child_array(child) };
                }
                return Err(error);
            }
        }
    }
    let mut private = Box::new(ArrayPrivate {
        _chunk: Some(chunk.clone()),
        _vector: None,
        _payload: Payload::None,
        buffers: Box::new([]),
        children: children.into_boxed_slice(),
    });
    private.buffers = vec![ptr::null()].into_boxed_slice();
    let buffers = private.buffers.as_mut_ptr();
    let n_children = private.children.len();
    let child_ptr = private.children.as_mut_ptr();
    let array = ArrowArray {
        length,
        null_count: 0,
        offset: 0,
        n_buffers: 1,
        n_children: n_children as i64,
        buffers,
        children: if n_children == 0 {
            ptr::null_mut()
        } else {
            child_ptr
        },
        dictionary: ptr::null_mut(),
        release: Some(release_array),
        private_data: Box::into_raw(private).cast(),
    };
    // SAFETY: out is non-null and writable per the C contract.
    unsafe { ptr::write(out, array) };
    Ok(())
}

fn validity_ptr(vector: &Vector) -> *const c_void {
    if vector.has_nulls() {
        vector.validity_words().as_ptr().cast()
    } else {
        ptr::null()
    }
}

fn null_count(vector: &Vector) -> i64 {
    if vector.has_nulls() {
        vector.null_count() as i64
    } else {
        0
    }
}

fn build_column(vector: Arc<Vector>) -> Result<*mut ArrowArray, ExportError> {
    let dtype = vector.dtype();
    let validity = validity_ptr(&vector);
    let null_count = null_count(&vector);
    if let DataType::Array { elem, len } = dtype {
        let child = build_array_child(&vector, elem, len)?;
        return finish_column(
            vector,
            Payload::None,
            vec![validity].into_boxed_slice(),
            vec![child].into_boxed_slice(),
            null_count,
        );
    }
    let (payload, data) = build_payload(&vector)?;
    let buffers = match dtype {
        DataType::Boolean => vec![validity, data].into_boxed_slice(),
        DataType::Varchar | DataType::Variant => {
            let Payload::Utf8 { offsets, data } = &payload else {
                unreachable!("text payload")
            };
            vec![validity, offsets.as_ptr().cast(), data.as_ptr().cast()].into_boxed_slice()
        }
        _ => vec![validity, data].into_boxed_slice(),
    };
    finish_column(vector, payload, buffers, Box::new([]), null_count)
}

fn finish_column(
    vector: Arc<Vector>,
    payload: Payload,
    buffers: Box<[*const c_void]>,
    children: Box<[*mut ArrowArray]>,
    null_count: i64,
) -> Result<*mut ArrowArray, ExportError> {
    let length = checked_len(vector.len())?;
    let mut private = Box::new(ArrayPrivate {
        _chunk: None,
        _vector: Some(vector),
        _payload: payload,
        buffers: Box::new([]),
        children,
    });
    private.buffers = buffers;
    let n_buffers = private.buffers.len();
    let n_children = private.children.len();
    let buffers = private.buffers.as_mut_ptr();
    let children = private.children.as_mut_ptr();
    let array = ArrowArray {
        length,
        null_count,
        offset: 0,
        n_buffers: n_buffers as i64,
        n_children: n_children as i64,
        buffers,
        children: if n_children == 0 {
            ptr::null_mut()
        } else {
            children
        },
        dictionary: ptr::null_mut(),
        release: Some(release_array),
        private_data: Box::into_raw(private).cast(),
    };
    Ok(Box::into_raw(Box::new(array)))
}

/// Builds the payload for one non-nested column and returns its data-buffer
/// pointer. Zero-copy types point directly into the engine vector; the vector
/// is kept alive by the array's private data.
fn build_payload(vector: &Vector) -> Result<(Payload, *const c_void), ExportError> {
    let data = vector.data();
    Ok(match (vector.dtype(), data) {
        (DataType::Boolean, VectorData::Boolean(values)) => {
            let mut bits = vec![0u8; values.len().div_ceil(8)];
            for (row, value) in values.iter().enumerate() {
                if *value {
                    bits[row / 8] |= 1 << (row % 8);
                }
            }
            let ptr = bits.as_ptr().cast();
            (Payload::Boolean(bits), ptr)
        }
        (DataType::Integer, VectorData::Int32(values)) => (Payload::None, values.as_ptr().cast()),
        (DataType::Bigint, VectorData::Int64(values)) => (Payload::None, values.as_ptr().cast()),
        (DataType::Double, VectorData::Float64(values)) => (Payload::None, values.as_ptr().cast()),
        (DataType::Date, VectorData::Date32(values)) => (Payload::None, values.as_ptr().cast()),
        (DataType::Timestamp, VectorData::TimestampMicros(values)) => {
            (Payload::None, values.as_ptr().cast())
        }
        (
            DataType::Decimal { .. },
            VectorData::Decimal {
                values,
                precision,
                scale,
            },
        ) => {
            if !(1..=38).contains(precision) || scale > precision {
                return Err(ExportError::type_(format!(
                    "DECIMAL({precision},{scale}) cannot be represented as Arrow decimal128"
                )));
            }
            (Payload::None, values.as_ptr().cast())
        }
        (DataType::Decimal { .. }, VectorData::Decimal64 { values, .. }) => {
            let wide: Vec<i128> = values.iter().map(|&v| i128::from(v)).collect();
            let ptr = wide.as_ptr().cast();
            (Payload::Decimal128(wide), ptr)
        }
        (DataType::Interval, VectorData::Interval(values)) => {
            let mut converted = Vec::with_capacity(values.len());
            for (row, value) in values.iter().enumerate() {
                let nanos = if vector.is_valid(row) {
                    value
                        .micros
                        .checked_mul(1000)
                        .ok_or_else(|| ExportError::type_("INTERVAL microseconds overflow nanos"))?
                } else {
                    0
                };
                converted.push(IntervalNano {
                    months: value.months,
                    days: value.days,
                    nanos,
                });
            }
            let ptr = converted.as_ptr().cast();
            (Payload::Interval(converted), ptr)
        }
        (DataType::Uuid, VectorData::Uuid(values)) => {
            let mut bytes = Vec::with_capacity(values.len());
            for value in values {
                bytes.push(value.to_be_bytes());
            }
            let ptr = bytes.as_ptr().cast();
            (Payload::Uuid(bytes), ptr)
        }
        (DataType::Varchar, VectorData::VarChar(values)) => {
            let payload = text_payload(vector, |row, out| {
                out.extend_from_slice(values.get(row).as_bytes())
            })?;
            let ptr = utf8_data_ptr(&payload);
            (payload, ptr)
        }
        (DataType::Variant, VectorData::Variant(values)) => {
            let payload = text_payload(vector, |row, out| {
                out.extend_from_slice(values[row].to_json().as_bytes())
            })?;
            let ptr = utf8_data_ptr(&payload);
            (payload, ptr)
        }
        (DataType::Hugeint, VectorData::Int128(values)) => {
            fit_decimal38(vector, "HUGEINT", |row| values[row].unsigned_abs())?;
            (Payload::None, values.as_ptr().cast())
        }
        (DataType::Uhugeint, VectorData::UInt128(values)) => {
            // Below 10^38 a u128 and an i128 share their bit pattern.
            fit_decimal38(vector, "UHUGEINT", |row| values[row])?;
            (Payload::None, values.as_ptr().cast())
        }
        (dtype, data) => {
            return Err(ExportError::type_(format!(
                "cannot export {dtype:?} stored as {data:?}"
            )));
        }
    })
}

fn utf8_data_ptr(payload: &Payload) -> *const c_void {
    let Payload::Utf8 { data, .. } = payload else {
        unreachable!("utf8 payload")
    };
    data.as_ptr().cast()
}

/// Check that every valid 128-bit value of a HUGEINT or UHUGEINT column fits
/// Arrow's `decimal128(38,0)`: at most 38 digits.
fn fit_decimal38(
    vector: &Vector,
    what: &str,
    magnitude: impl Fn(usize) -> u128,
) -> Result<(), ExportError> {
    const LIMIT: u128 = 10u128.pow(38);
    for row in 0..vector.len() {
        if vector.is_valid(row) && magnitude(row) >= LIMIT {
            return Err(ExportError::type_(format!(
                "{what} value at row {row} has more than 38 digits and does not fit Arrow decimal128(38,0)"
            )));
        }
    }
    Ok(())
}

fn text_payload(
    vector: &Vector,
    mut text: impl FnMut(usize, &mut Vec<u8>),
) -> Result<Payload, ExportError> {
    let mut offsets = Vec::with_capacity(vector.len() + 1);
    offsets.push(0);
    let mut data = Vec::new();
    for row in 0..vector.len() {
        if vector.is_valid(row) {
            text(row, &mut data);
        }
        offsets.push(
            i32::try_from(data.len()).map_err(|_| {
                ExportError::type_("string column exceeds Arrow's 2 GiB offset limit")
            })?,
        );
    }
    Ok(Payload::Utf8 { offsets, data })
}

fn build_array_child(
    vector: &Arc<Vector>,
    elem: ArrayElem,
    width: u32,
) -> Result<*mut ArrowArray, ExportError> {
    let VectorData::Array { values, valid, .. } = vector.data() else {
        return Err(ExportError::type_("ARRAY column has non-array storage"));
    };
    let length = vector
        .len()
        .checked_mul(width as usize)
        .ok_or_else(|| ExportError::invalid("ARRAY element count overflows usize"))?;
    if values.len() != length {
        return Err(ExportError::invalid(format!(
            "ARRAY storage has {} elements, expected {length}",
            values.len()
        )));
    }
    let validity = valid
        .as_ref()
        .map_or(ptr::null(), |bits| bits.as_ptr().cast());
    let nulls = valid.as_ref().map_or(0, |bits| {
        (0..length)
            .filter(|row| bits[row / 8] & (1 << (row % 8)) == 0)
            .count() as i64
    });
    let (payload, data) = match (elem, values) {
        (ArrayElem::Integer, ArrayElems::Ints(values)) => {
            let narrowed = values
                .iter()
                .map(|value| {
                    i32::try_from(*value)
                        .map_err(|_| ExportError::type_("ARRAY INTEGER element does not fit i32"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let ptr = narrowed.as_ptr().cast();
            (Payload::Int32(narrowed), ptr)
        }
        (ArrayElem::Bigint, ArrayElems::Ints(values)) => (Payload::None, values.as_ptr().cast()),
        (ArrayElem::Double, ArrayElems::Floats(values)) => (Payload::None, values.as_ptr().cast()),
        (ArrayElem::Float, ArrayElems::Floats32(values)) => (Payload::None, values.as_ptr().cast()),
        (elem, values) => {
            return Err(ExportError::type_(format!(
                "ARRAY element {elem:?} has mismatched storage {values:?}"
            )));
        }
    };
    let mut private = Box::new(ArrayPrivate {
        _chunk: None,
        _vector: Some(vector.clone()),
        _payload: payload,
        buffers: Box::new([]),
        children: Box::new([]),
    });
    private.buffers = vec![validity, data].into_boxed_slice();
    let buffers = private.buffers.as_mut_ptr();
    let array = ArrowArray {
        length: checked_len(length)?,
        null_count: nulls,
        offset: 0,
        n_buffers: 2,
        n_children: 0,
        buffers,
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: Some(release_array),
        private_data: Box::into_raw(private).cast(),
    };
    Ok(Box::into_raw(Box::new(array)))
}

unsafe extern "C" fn release_array(array: *mut ArrowArray) {
    if array.is_null() {
        return;
    }
    // SAFETY: non-null, owned by the caller.
    let array = unsafe { &mut *array };
    let private = array.private_data.cast::<ArrayPrivate>();
    if !private.is_null() {
        // SAFETY: private_data was produced by Box::into_raw in this module.
        let mut private = unsafe { Box::from_raw(private) };
        for child in private.children.iter_mut() {
            let child = std::mem::replace(child, ptr::null_mut());
            // SAFETY: the child pointer came from this array's private data.
            unsafe { release_child_array(child) };
        }
        array.private_data = ptr::null_mut();
    }
    array.release = None;
    array.length = 0;
    array.null_count = 0;
    array.offset = 0;
    array.n_buffers = 0;
    array.n_children = 0;
    array.buffers = ptr::null_mut();
    array.children = ptr::null_mut();
    array.dictionary = ptr::null_mut();
}

unsafe fn release_child_array(child: *mut ArrowArray) {
    if child.is_null() {
        return;
    }
    // SAFETY: a child pointer allocated by this module.
    let release = unsafe { (*child).release };
    if let Some(release) = release {
        // SAFETY: the callback belongs to this child.
        unsafe { release(child) };
    }
    // SAFETY: the child struct itself was allocated with Box.
    drop(unsafe { Box::from_raw(child) });
}
