//! pinkdb C ABI v1.3 — `pinkdb-ffi`.
//!
//! Exposes the `pinkdb` embedding API over a C ABI (`cdylib` + `staticlib`)
//! for foreign bindings (Python `cffi`, Go `cgo`, ...). This crate is the
//! repo's sanctioned `unsafe` boundary: the core `pinkdb` crate stays
//! zero-unsafe; everything here is contained pointer marshalling with NULL
//! checks and `SAFETY` comments.
//!
//! Contract (also documented in `include/pinkdb.h`):
//! - every returned pointer is freed with the matching `pinkdb_*_free`;
//! - text pointers (`column_name`, `PinkValue.text_ptr`, `last_error`) are
//!   owned by their result/handle/thread and valid only until that owner is
//!   freed or the next FFI call on the same thread;
//! - handles are not thread-safe — use one handle per thread;
//! - SQL input is passed as (pointer, length) and must be valid UTF-8;
//!   embedded NUL bytes and multi-byte characters are fine.
//! - prepared statements (`pinkdb_prepare`) take one `pink_value` per `?`
//!   placeholder, in textual order; the value structs are read during the
//!   call only (VARCHAR payloads are copied).
//! - the SQL statement `LOAD EXTENSION '<path>'` loads and runs native code
//!   and is **disabled by default**: `pinkdb_execute` can only load native
//!   code after `pinkdb_set_allow_sql_load_extension` has enabled it.
//! - query profiling is opt-in per handle ([`pinkdb_set_profiling`], off by
//!   default). While it is on, the result of `pinkdb_execute` /
//!   `pinkdb_execute_prepared` carries one profile per statement, read with
//!   the `pinkdb_result_profile_*` accessors (same statement index as the
//!   other result accessors). When it is off those accessors report "no
//!   profile": 0 for numeric fields and NULL for the kind string; call
//!   [`pinkdb_result_profile_available`] to tell that apart from a real zero.

// Raw-pointer arguments are NULL-checked before every dereference and the
// pointer-taking signatures are dictated by the C ABI, so the lint that wants
// `unsafe fn` here does not apply to this crate.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::{RefCell, UnsafeCell};
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pinkdb::db::{Connection, Database};
use pinkdb::exec::ChunkedResult;
use pinkdb::wal::WalSyncMethod;
use pinkdb::{
    CancelFlag, DataChunk, DataType, EncryptionKey, PinkDbError, QueryProfile, QueryResult, Value,
    Vector, VectorData,
};

mod arrow;
pub use arrow::{ArrowArray, ArrowSchema};

/// ABI version reported by [`pinkdb_abi_version`]: `major << 16 | minor`.
///
/// Additions bump the minor; removals or layout changes bump the major.
pub const PINKDB_ABI_VERSION: u32 = (1 << 16) | 3;

// ----- statement kind tags (`pinkdb_result_kind`) -----
pub const PINKDB_STMT_SELECTED: i32 = 0;
pub const PINKDB_STMT_CREATED: i32 = 1;
pub const PINKDB_STMT_INSERTED: i32 = 2;
pub const PINKDB_STMT_UPDATED: i32 = 3;
pub const PINKDB_STMT_DELETED: i32 = 4;
pub const PINKDB_STMT_BEGAN: i32 = 5;
pub const PINKDB_STMT_COMMITTED: i32 = 6;
pub const PINKDB_STMT_ROLLEDBACK: i32 = 7;
pub const PINKDB_STMT_CREATED_VIEW: i32 = 8;
pub const PINKDB_STMT_DROPPED_TABLE: i32 = 9;
pub const PINKDB_STMT_DROPPED_VIEW: i32 = 10;
pub const PINKDB_STMT_CREATED_DATABASE: i32 = 11;
pub const PINKDB_STMT_DROPPED_DATABASE: i32 = 12;
pub const PINKDB_STMT_USED: i32 = 13;
/// `LOAD EXTENSION '<path>'`; `pinkdb_result_name` carries the extension's
/// declared name.
pub const PINKDB_STMT_LOADED_EXTENSION: i32 = 14;
/// `CHECKPOINT` (a no-op for in-memory databases).
pub const PINKDB_STMT_CHECKPOINTED: i32 = 15;
/// `ALTER TABLE`; `pinkdb_result_name` carries the table's name after the
/// statement (a `RENAME TO` reports the new name).
pub const PINKDB_STMT_ALTERED_TABLE: i32 = 16;
/// `CREATE USER` (wave 48); `pinkdb_result_name` carries the user.
pub const PINKDB_STMT_CREATED_USER: i32 = 17;
/// `ALTER USER … PASSWORD`; `pinkdb_result_name` carries the user.
pub const PINKDB_STMT_ALTERED_USER: i32 = 18;
/// `DROP USER`; `pinkdb_result_name` carries the user.
pub const PINKDB_STMT_DROPPED_USER: i32 = 19;
/// `CREATE ROLE`; `pinkdb_result_name` carries the role.
pub const PINKDB_STMT_CREATED_ROLE: i32 = 20;
/// `DROP ROLE`; `pinkdb_result_name` carries the role.
pub const PINKDB_STMT_DROPPED_ROLE: i32 = 21;
/// `GRANT …`; `pinkdb_result_name` carries the grant's description
/// (`SELECT ON TABLE main.main.t TO alice`).
pub const PINKDB_STMT_GRANTED: i32 = 22;
/// `REVOKE …`; `pinkdb_result_name` carries the revoked grant's description.
pub const PINKDB_STMT_REVOKED: i32 = 23;
/// `SET VARIABLE` (wave 95c); `pinkdb_result_name` is the variable.
pub const PINKDB_STMT_SET_VARIABLE: i32 = 24;
/// `CREATE SCHEMA` (wave 131b).
pub const PINKDB_STMT_CREATED_SCHEMA: i32 = 25;
/// `DROP SCHEMA` (wave 131b).
pub const PINKDB_STMT_DROPPED_SCHEMA: i32 = 26;

// ----- value / column type tags -----
pub const PINKDB_TYPE_NULL: i32 = 0;
pub const PINKDB_TYPE_BOOLEAN: i32 = 1;
pub const PINKDB_TYPE_INTEGER: i32 = 2;
pub const PINKDB_TYPE_BIGINT: i32 = 3;
pub const PINKDB_TYPE_DOUBLE: i32 = 4;
pub const PINKDB_TYPE_VARCHAR: i32 = 5;
pub const PINKDB_TYPE_DATE: i32 = 6;
pub const PINKDB_TYPE_TIMESTAMP: i32 = 7;
/// v1 additive: DECIMAL/INTERVAL are marshaled as text (canonical DuckDB
/// forms) in `text`/`text_len`; `i`/`d` stay zero.
pub const PINKDB_TYPE_DECIMAL: i32 = 8;
pub const PINKDB_TYPE_INTERVAL: i32 = 9;
/// v1 additive: a VARIANT renders into `text` as its JSON value. The DATE and
/// TIMESTAMP kinds inside it print as ISO strings there and come back as
/// VARCHAR from a C-side parse — the C ABI has no slot for those kinds, so
/// the extension ABI refuses VARIANT instead of pretending.
pub const PINKDB_TYPE_VARIANT: i32 = 10;
/// v1 additive: a `DOUBLE[N]` value carries its `N` in the tag itself —
/// `PINKDB_TYPE_ARRAY | (N << 8)` — and its elements as `N * 8` little-endian
/// `f64` bytes in `text`/`text_len`, with `int64` set to `N`. The length has to
/// travel with the tag because the C ABI has one `type_tag` slot and no room
/// for type parameters. INTEGER/BIGINT arrays and arrays holding NULL elements
/// are refused (there is no per-element NULL channel here).
pub const PINKDB_TYPE_ARRAY: i32 = 11;
/// v1 additive: a `FLOAT[N]` value, the 4-byte counterpart of
/// `PINKDB_TYPE_ARRAY`: the tag carries `N` (`PINKDB_TYPE_ARRAY_FLOAT |
/// (N << 8)`) and `text`/`text_len` hold `N * 4` little-endian `f32` bytes.
/// FLOAT is an array-only element kind here (the engine has no `FLOAT` scalar).
pub const PINKDB_TYPE_ARRAY_FLOAT: i32 = 12;
/// v1 additive: HUGEINT/UHUGEINT marshal as decimal text (like DECIMAL), so
/// `text`/`text_len` carry the exact digits and `i`/`d` stay zero.
pub const PINKDB_TYPE_HUGEINT: i32 = 13;
pub const PINKDB_TYPE_UHUGEINT: i32 = 14;
/// A UUID: 16 bytes in the chunk API (`pinkdb_uhugeint` layout); the v1 cell
/// API still renders it as canonical VARCHAR text.
pub const PINKDB_TYPE_UUID: i32 = 15;
/// A FLOAT array element, not a standalone column type. Appears only through
/// `pinkdb_result_column_array_element_type`.
pub const PINKDB_TYPE_FLOAT: i32 = 16;

// ----- thread-local error -----

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
    static LAST_ERROR_CLASS: std::cell::Cell<Option<&'static CStr>> =
        const { std::cell::Cell::new(None) };
}

// The class names are part of the ABI: bindings map them onto their own
// exception hierarchies, so they are stable, lower-case identifiers.
const CLASS_PARSE: &CStr = c"parse";
const CLASS_CATALOG: &CStr = c"catalog";
const CLASS_TYPE: &CStr = c"type";
const CLASS_PARAMETER: &CStr = c"parameter";
const CLASS_EXECUTION: &CStr = c"execution";
const CLASS_CONSTRAINT: &CStr = c"constraint";
const CLASS_IO: &CStr = c"io";
const CLASS_CSV: &CStr = c"csv";
const CLASS_INVALID_ARGUMENT: &CStr = c"invalid_argument";
const CLASS_INTERNAL: &CStr = c"internal";
const CLASS_PERMISSION: &CStr = c"permission";
const CLASS_INTERRUPTED: &CStr = c"interrupted";

fn set_error(class: &'static CStr, msg: &str) {
    // Interior NULs cannot live in a CString; escape them for display.
    let sanitized = msg.replace('\0', "\\0");
    LAST_ERROR.with(|e| *e.borrow_mut() = CString::new(sanitized).ok());
    LAST_ERROR_CLASS.with(|c| c.set(Some(class)));
}

/// Record an engine error, keeping the `PinkDbError` variant as the class.
/// The one enforced constraint (a NOT NULL column added to a non-empty table
/// without a default) is an `Execution` error with a fixed prefix; give it
/// its own class so drivers can raise an integrity error.
fn engine_error_class(e: &PinkDbError) -> &'static CStr {
    // Same stable kinds the CLI's JSON contract uses (`pinkdb::agent`), plus
    // "constraint" for the one enforced constraint and "interrupted" for the
    // cancellation flag's fixed message. Drivers map these onto their own
    // exception hierarchies.
    if matches!(e, PinkDbError::Permission(_)) {
        CLASS_PERMISSION
    } else if matches!(e, PinkDbError::Execution(msg)
        if msg == pinkdb::STATEMENT_CANCELLED)
    {
        CLASS_INTERRUPTED
    } else if matches!(e, PinkDbError::Execution(msg)
        if msg.starts_with("NOT NULL constraint failed"))
    {
        CLASS_CONSTRAINT
    } else {
        match pinkdb::agent::error_kind(e) {
            "parse" => CLASS_PARSE,
            "catalog" => CLASS_CATALOG,
            "type" => CLASS_TYPE,
            "parameter" => CLASS_PARAMETER,
            "execution" => CLASS_EXECUTION,
            "io" => CLASS_IO,
            "csv" => CLASS_CSV,
            _ => CLASS_INTERNAL,
        }
    }
}

fn set_engine_error(e: &PinkDbError) {
    set_error(engine_error_class(e), &e.to_string());
}

fn clear_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
    LAST_ERROR_CLASS.with(|c| c.set(None));
}

/// Last error on this thread (NULL when none). The pointer stays valid until
/// the next FFI call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_last_error() -> *const c_char {
    LAST_ERROR.with(|e| match &*e.borrow() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    })
}

/// Class of the last error on this thread (NULL when none): one of `parse`,
/// `catalog`, `type`, `parameter`, `execution`, `constraint`, `io`, `csv`,
/// `invalid_argument`, `internal`. Static string; never freed.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_last_error_class() -> *const c_char {
    LAST_ERROR_CLASS.with(|c| match c.get() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    })
}

/// ABI version of this library.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_abi_version() -> u32 {
    PINKDB_ABI_VERSION
}

// ----- value marshalling -----

/// One result value, returned by value. For `VARCHAR`, `text_ptr` points at
/// `text_len` bytes owned by the result and followed by a NUL byte; embedded
/// NULs are possible (always trust `text_len`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PinkValue {
    pub type_tag: i32,
    pub is_null: i32,
    pub int64: i64,
    pub double: f64,
    pub text_ptr: *const c_char,
    pub text_len: usize,
}

// L-10: the struct crosses the C ABI by value. Pin the layout at compile
// time so a field reorder or type change fails the build instead of silently
// reinterpreting every value in the Python and Go bindings. These offsets
// must match `include/pinkdb.h`'s `pink_value` (checked again by
// `tests/header_sync.rs`).
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(std::mem::offset_of!(PinkValue, type_tag) == 0);
    assert!(std::mem::offset_of!(PinkValue, is_null) == 4);
    assert!(std::mem::offset_of!(PinkValue, int64) == 8);
    assert!(std::mem::offset_of!(PinkValue, double) == 16);
    assert!(std::mem::offset_of!(PinkValue, text_ptr) == 24);
    assert!(std::mem::offset_of!(PinkValue, text_len) == 32);
    assert!(std::mem::size_of::<PinkValue>() == 40);
};
#[cfg(target_pointer_width = "32")]
const _: () = {
    assert!(std::mem::offset_of!(PinkValue, type_tag) == 0);
    assert!(std::mem::offset_of!(PinkValue, is_null) == 4);
    assert!(std::mem::offset_of!(PinkValue, int64) == 8);
    assert!(std::mem::offset_of!(PinkValue, double) == 16);
    assert!(std::mem::offset_of!(PinkValue, text_ptr) == 20);
    assert!(std::mem::offset_of!(PinkValue, text_len) == 24);
    assert!(std::mem::size_of::<PinkValue>() == 28);
};

impl PinkValue {
    fn null() -> Self {
        Self {
            type_tag: PINKDB_TYPE_NULL,
            is_null: 1,
            int64: 0,
            double: 0.0,
            text_ptr: ptr::null(),
            text_len: 0,
        }
    }
}

struct Cell {
    tag: i32,
    is_null: bool,
    i: i64,
    d: f64,
    /// Owned bytes with a trailing NUL; `PinkValue.text_len` excludes it.
    text: Option<Vec<u8>>,
}

impl Cell {
    fn from_value(v: &Value) -> std::result::Result<Cell, String> {
        Ok(match v {
            Value::Null => Cell {
                tag: PINKDB_TYPE_NULL,
                is_null: true,
                i: 0,
                d: 0.0,
                text: None,
            },
            Value::Boolean(b) => Cell {
                tag: PINKDB_TYPE_BOOLEAN,
                is_null: false,
                i: *b as i64,
                d: 0.0,
                text: None,
            },
            Value::Integer(i) => Cell {
                tag: PINKDB_TYPE_INTEGER,
                is_null: false,
                i: *i as i64,
                d: 0.0,
                text: None,
            },
            Value::Bigint(i) => Cell {
                tag: PINKDB_TYPE_BIGINT,
                is_null: false,
                i: *i,
                d: 0.0,
                text: None,
            },
            Value::Double(x) => Cell {
                tag: PINKDB_TYPE_DOUBLE,
                is_null: false,
                i: 0,
                d: *x,
                text: None,
            },
            Value::Varchar(s) => {
                let mut b = s.as_bytes().to_vec();
                b.push(0);
                Cell {
                    tag: PINKDB_TYPE_VARCHAR,
                    is_null: false,
                    i: 0,
                    d: 0.0,
                    text: Some(b),
                }
            }
            Value::Date(d) => Cell {
                tag: PINKDB_TYPE_DATE,
                is_null: false,
                i: *d as i64,
                d: 0.0,
                text: None,
            },
            Value::Timestamp(us) => Cell {
                tag: PINKDB_TYPE_TIMESTAMP,
                is_null: false,
                i: *us,
                d: 0.0,
                text: None,
            },
            Value::Decimal {
                value: x, scale, ..
            } => {
                let mut b = pinkdb::types::format_decimal(*x, *scale).into_bytes();
                b.push(0);
                Cell {
                    tag: PINKDB_TYPE_DECIMAL,
                    is_null: false,
                    i: 0,
                    d: 0.0,
                    text: Some(b),
                }
            }
            // UUID has no cell in the ABI, so it crosses as its text form
            // (and its column type tag is PINKDB_TYPE_VARCHAR).
            Value::Uuid(bits) => Cell {
                tag: PINKDB_TYPE_VARCHAR,
                is_null: false,
                i: 0,
                d: 0.0,
                text: Some(nul_terminated(&pinkdb::uuid::format(*bits))),
            },
            Value::Variant(v) => {
                let mut b = v.to_json().into_bytes();
                b.push(0);
                Cell {
                    tag: PINKDB_TYPE_VARIANT,
                    is_null: false,
                    i: 0,
                    d: 0.0,
                    text: Some(b),
                }
            }
            Value::Array(a) => {
                let single = match a.elem {
                    pinkdb::ArrayElem::Float => true,
                    pinkdb::ArrayElem::Double => false,
                    _ => {
                        return Err(format!(
                            "only FLOAT and DOUBLE arrays cross the C ABI; this {} column needs \
                             a CAST (e.g. SELECT CAST(v AS VARCHAR))",
                            a.dtype()
                        ));
                    }
                };
                if a.has_null_elems() {
                    return Err(format!(
                        "a {} value with a NULL element cannot cross the C ABI (there is no \
                         per-element NULL channel); replace it first, e.g. COALESCE(v[i], 0)",
                        a.dtype()
                    ));
                }
                let width = if single { 4 } else { 8 };
                let mut b = Vec::with_capacity(a.len() * width + 1);
                if single {
                    let Some(values) = a.floats32() else {
                        unreachable!("single is set only for FLOAT arrays")
                    };
                    for x in values {
                        b.extend_from_slice(&x.to_le_bytes());
                    }
                } else if let Some(values) = a.floats() {
                    for x in values {
                        b.extend_from_slice(&x.to_le_bytes());
                    }
                }
                b.push(0);
                Cell {
                    tag: if single {
                        float_array_tag(a.len() as u32)
                    } else {
                        array_tag(a.len() as u32)
                    },
                    is_null: false,
                    i: a.len() as i64,
                    d: 0.0,
                    text: Some(b),
                }
            }
            Value::Interval {
                months,
                days,
                micros,
            } => {
                let mut b = pinkdb::types::format_interval(*months, *days, *micros).into_bytes();
                b.push(0);
                Cell {
                    tag: PINKDB_TYPE_INTERVAL,
                    is_null: false,
                    i: 0,
                    d: 0.0,
                    text: Some(b),
                }
            }
            Value::Hugeint(x) => Cell {
                tag: PINKDB_TYPE_HUGEINT,
                is_null: false,
                i: 0,
                d: 0.0,
                text: Some(nul_terminated(&x.to_string())),
            },
            Value::Uhugeint(x) => Cell {
                tag: PINKDB_TYPE_UHUGEINT,
                is_null: false,
                i: 0,
                d: 0.0,
                text: Some(nul_terminated(&x.to_string())),
            },
        })
    }

    fn value(&self) -> PinkValue {
        let (p, len) = match &self.text {
            Some(b) => (b.as_ptr() as *const c_char, b.len().saturating_sub(1)),
            None => (ptr::null(), 0),
        };
        PinkValue {
            type_tag: self.tag,
            is_null: self.is_null as i32,
            int64: self.i,
            double: self.d,
            text_ptr: p,
            text_len: len,
        }
    }
}

/// `PINKDB_TYPE_ARRAY | (len << 8)`: the tag carries the length.
pub const fn array_tag(len: u32) -> i32 {
    PINKDB_TYPE_ARRAY | ((len as i32) << 8)
}

/// The element count packed into an ARRAY tag by [`array_tag`].
pub const fn array_tag_len(tag: i32) -> u32 {
    (tag as u32) >> 8
}

/// `PINKDB_TYPE_ARRAY_FLOAT | (len << 8)`, the `FLOAT[N]` counterpart.
pub const fn float_array_tag(len: u32) -> i32 {
    PINKDB_TYPE_ARRAY_FLOAT | ((len as i32) << 8)
}

fn dtype_tag(t: DataType) -> i32 {
    match t {
        DataType::Array { len, elem } => {
            if elem == pinkdb::ArrayElem::Float {
                float_array_tag(len)
            } else {
                array_tag(len)
            }
        }
        DataType::Boolean => PINKDB_TYPE_BOOLEAN,
        DataType::Integer => PINKDB_TYPE_INTEGER,
        DataType::Bigint => PINKDB_TYPE_BIGINT,
        DataType::Double => PINKDB_TYPE_DOUBLE,
        DataType::Varchar => PINKDB_TYPE_VARCHAR,
        DataType::Date => PINKDB_TYPE_DATE,
        DataType::Timestamp => PINKDB_TYPE_TIMESTAMP,
        DataType::Decimal { .. } => PINKDB_TYPE_DECIMAL,
        DataType::Interval => PINKDB_TYPE_INTERVAL,
        DataType::Variant => PINKDB_TYPE_VARIANT,
        DataType::Uuid => PINKDB_TYPE_UUID,
        DataType::Hugeint => PINKDB_TYPE_HUGEINT,
        DataType::Uhugeint => PINKDB_TYPE_UHUGEINT,
    }
}

fn nul_terminated(s: &str) -> Vec<u8> {
    let mut b = s.as_bytes().to_vec();
    b.push(0);
    b
}

/// A SELECT result: the schema, the executor's chunks, and (lazily) the v1
/// cell rows. Bindings on ABI 1.2 read the chunks; `pinkdb_result_value` still
/// answers the older cell API from the same data.
struct SelStmt {
    names: Vec<Vec<u8>>,
    types: Vec<i32>,
    dtypes: Vec<DataType>,
    chunks: Vec<DataChunk>,
    row_count: usize,
    /// The next chunk `pinkdb_result_fetch_chunk` hands out.
    next_chunk: usize,
    /// Built on the first v1 cell access; NULL-cell conversion is eager for
    /// the types that can fail (ARRAY), so this never loses an error.
    rows: RefCell<Option<Vec<Vec<Cell>>>>,
}

impl SelStmt {
    fn from_chunks(
        names: Vec<String>,
        dtypes: Vec<DataType>,
        chunks: Vec<DataChunk>,
    ) -> std::result::Result<SelStmt, String> {
        let row_count = chunks.iter().map(|c| c.num_rows).sum();
        let types = dtypes.iter().copied().map(dtype_tag).collect();
        // Array cells are the only cell conversion that can fail; validate
        // them now so a v1 caller sees the same error the row path produced.
        let rows = if dtypes.iter().any(|t| matches!(t, DataType::Array { .. })) {
            Some(cells_from_chunks(&chunks)?)
        } else {
            None
        };
        Ok(SelStmt {
            names: names.iter().map(|n| nul_terminated(n)).collect(),
            types,
            dtypes,
            chunks,
            row_count,
            next_chunk: 0,
            rows: RefCell::new(rows),
        })
    }

    fn from_rows(
        names: Vec<String>,
        dtypes: Vec<DataType>,
        rows: Vec<Vec<Value>>,
    ) -> std::result::Result<SelStmt, String> {
        let chunks = chunks_from_rows(&dtypes, &rows)?;
        SelStmt::from_chunks(names, dtypes, chunks)
    }

    fn rows(&self) -> std::cell::Ref<'_, Vec<Vec<Cell>>> {
        if self.rows.borrow().is_none() {
            // `from_chunks` already validated the only failing conversion.
            let rows = cells_from_chunks(&self.chunks).unwrap_or_default();
            *self.rows.borrow_mut() = Some(rows);
        }
        std::cell::Ref::map(self.rows.borrow(), |r| {
            r.as_ref().expect("rows were just initialized")
        })
    }
}

fn cells_from_chunks(chunks: &[DataChunk]) -> std::result::Result<Vec<Vec<Cell>>, String> {
    let mut rows = Vec::with_capacity(chunks.iter().map(|c| c.num_rows).sum());
    for chunk in chunks {
        for i in 0..chunk.num_rows {
            rows.push(
                chunk
                    .columns
                    .iter()
                    .map(|c| Cell::from_value(&c.get(i)))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(rows)
}

/// The row-shaped result paths (`RETURNING`, `CALL`, transactions) still hand
/// the C API rows; rebuild chunks so every SELECT serves chunk fetch.
fn chunks_from_rows(
    dtypes: &[DataType],
    rows: &[Vec<Value>],
) -> std::result::Result<Vec<DataChunk>, String> {
    let mut chunks = Vec::with_capacity(rows.len().div_ceil(pinkdb::CHUNK_SIZE));
    for window in rows.chunks(pinkdb::CHUNK_SIZE) {
        let mut columns = Vec::with_capacity(dtypes.len());
        for (i, dtype) in dtypes.iter().enumerate() {
            let mut vector = Vector::with_capacity(*dtype, window.len());
            for row in window {
                vector
                    .push_value(row.get(i).cloned().unwrap_or(Value::Null))
                    .map_err(|e| e.to_string())?;
            }
            columns.push(vector);
        }
        chunks.push(DataChunk::new(columns).map_err(|e| e.to_string())?);
    }
    Ok(chunks)
}

/// A `{const char *ptr; uint64_t len}` string element in a VARCHAR, BLOB or
/// VARIANT vector. `ptr` is NULL for a NULL row and is never NUL-terminated.
#[repr(C)]
pub struct PinkString {
    pub ptr: *const c_char,
    pub len: u64,
}

/// The little-endian `{lower, upper}` words C sees for a 128-bit value. On a
/// big-endian host the chunk builder fills one of these per row instead of
/// exposing the engine's native words.
#[repr(C)]
pub struct PinkU128 {
    pub lower: u64,
    pub upper: u64,
}

/// One vector's C view: a typed data pointer, an optional validity mask, and
/// the owned payloads those pointers borrow from.
pub struct PinkVector {
    data: *const c_void,
    validity: *const u64,
    len: usize,
    /// Owned string-element array (VARCHAR/BLOB/VARIANT).
    _strings: Vec<PinkString>,
    /// Owned JSON text backing `_strings` for VARIANT.
    _text: Vec<Vec<u8>>,
    /// Big-endian hosts only: owned `{lower, upper}` words for 128-bit types.
    _ints: Vec<PinkU128>,
}

fn wide_data<T>(
    values: &[T],
    ints: &mut Vec<PinkU128>,
    to_u128: impl Fn(&T) -> u128,
) -> *const c_void {
    if cfg!(target_endian = "little") {
        return values.as_ptr().cast();
    }
    ints.extend(values.iter().map(|v| {
        let x = to_u128(v);
        PinkU128 {
            lower: x as u64,
            upper: (x >> 64) as u64,
        }
    }));
    ints.as_ptr().cast()
}

impl PinkVector {
    fn new(vector: &Vector) -> PinkVector {
        let len = vector.len();
        let mut strings = Vec::new();
        let mut text = Vec::new();
        let mut ints = Vec::new();
        let data: *const c_void = match vector.data() {
            VectorData::Boolean(v) => v.as_ptr().cast(),
            VectorData::Int32(v) => v.as_ptr().cast(),
            VectorData::Int64(v) => v.as_ptr().cast(),
            VectorData::Float64(v) => v.as_ptr().cast(),
            VectorData::Date32(v) => v.as_ptr().cast(),
            VectorData::TimestampMicros(v) => v.as_ptr().cast(),
            VectorData::Decimal { values, .. } => wide_data(values, &mut ints, |v| *v as u128),
            VectorData::Interval(v) => v.as_ptr().cast(),
            VectorData::Int128(v) => wide_data(v, &mut ints, |v| *v as u128),
            VectorData::UInt128(v) => wide_data(v, &mut ints, |v| *v),
            VectorData::Uuid(v) => wide_data(v, &mut ints, |v| *v),
            VectorData::VarChar(v) => {
                strings.reserve(len);
                for i in 0..len {
                    if vector.is_valid(i) {
                        let s = v.get(i);
                        strings.push(PinkString {
                            ptr: s.as_ptr() as *const c_char,
                            len: s.len() as u64,
                        });
                    } else {
                        strings.push(PinkString {
                            ptr: ptr::null(),
                            len: 0,
                        });
                    }
                }
                strings.as_ptr().cast()
            }
            // Provisional: VARIANT crosses as its JSON text, not a nested
            // value. 112c's Arrow export is the full nested path.
            VectorData::Variant(v) => {
                text.reserve(len);
                strings.reserve(len);
                for (i, value) in v.iter().enumerate().take(len) {
                    if vector.is_valid(i) {
                        let bytes = value.to_json().into_bytes();
                        strings.push(PinkString {
                            ptr: bytes.as_ptr() as *const c_char,
                            len: bytes.len() as u64,
                        });
                        text.push(bytes);
                    } else {
                        strings.push(PinkString {
                            ptr: ptr::null(),
                            len: 0,
                        });
                    }
                }
                strings.as_ptr().cast()
            }
            // Provisional: ARRAY has no C child vector in 1.2; the data
            // pointer is NULL and callers use the v1 cell API.
            VectorData::Array { .. } => ptr::null(),
        };
        let validity = if vector.has_nulls() {
            vector.validity_words().as_ptr()
        } else {
            ptr::null()
        };
        PinkVector {
            data,
            validity,
            len,
            _strings: strings,
            _text: text,
            _ints: ints,
        }
    }
}

/// One chunk handed to C by [`pinkdb_result_fetch_chunk`]. It owns its
/// `DataChunk` (shared with the result's chunk list) and every C payload, so
/// it outlives the result.
pub struct PinkChunk {
    /// Keeps the `Arc<Vector>`s the typed pointers point into alive.
    _chunk: DataChunk,
    columns: Vec<PinkVector>,
}

impl PinkChunk {
    fn new(chunk: DataChunk) -> PinkChunk {
        let columns = chunk.columns.iter().map(|c| PinkVector::new(c)).collect();
        PinkChunk {
            _chunk: chunk,
            columns,
        }
    }
}

enum StmtData {
    Sel(SelStmt),
    Simple {
        kind: i32,
        count: usize,
        name: Option<Vec<u8>>,
    },
}

/// One profile, marshaled once at execution time. Durations are nanoseconds
/// and `kind` is NUL-terminated so the accessor can hand out a stable pointer.
struct ProfileData {
    parse: u64,
    bind: u64,
    optimize: u64,
    execute: u64,
    log: u64,
    total: u64,
    rows: u64,
    columns: u32,
    kind: Vec<u8>,
}

impl ProfileData {
    fn from_profile(p: &QueryProfile) -> ProfileData {
        let nanos = |d: std::time::Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        ProfileData {
            parse: nanos(p.parse),
            bind: nanos(p.bind),
            optimize: nanos(p.optimize),
            execute: nanos(p.execute),
            log: nanos(p.log),
            total: nanos(p.total),
            rows: p.rows,
            columns: p.columns,
            kind: nul_terminated(&p.kind),
        }
    }
}

/// Owns one `pinkdb_execute` outcome. Every pointer handed out by the
/// `pinkdb_result_*` accessors borrows from here.
pub struct PinkResult {
    stmts: Vec<StmtData>,
    /// One per statement, in the same order, when profiling was enabled for
    /// the execution that produced this result; empty otherwise.
    profiles: Vec<ProfileData>,
    /// The error this result carries, if any. Failed queries still return a
    /// result so callers read the error from it instead of a thread-local.
    error: Option<CString>,
    error_class: Option<&'static CStr>,
}

impl PinkResult {
    fn failed(class: &'static CStr, message: &str) -> PinkResult {
        PinkResult {
            stmts: Vec::new(),
            profiles: Vec::new(),
            error: CString::new(message.replace('\0', "\\0")).ok(),
            error_class: Some(class),
        }
    }
}

fn failed_result(class: &'static CStr, message: &str) -> *mut PinkResult {
    Box::into_raw(Box::new(PinkResult::failed(class, message)))
}

fn kind_of(r: &QueryResult) -> i32 {
    match r {
        QueryResult::Selected { .. } => PINKDB_STMT_SELECTED,
        QueryResult::Created => PINKDB_STMT_CREATED,
        QueryResult::Inserted { .. } => PINKDB_STMT_INSERTED,
        QueryResult::Updated { .. } => PINKDB_STMT_UPDATED,
        QueryResult::Deleted { .. } => PINKDB_STMT_DELETED,
        QueryResult::Began => PINKDB_STMT_BEGAN,
        QueryResult::Committed => PINKDB_STMT_COMMITTED,
        QueryResult::RolledBack => PINKDB_STMT_ROLLEDBACK,
        QueryResult::CreatedView { .. } => PINKDB_STMT_CREATED_VIEW,
        QueryResult::DroppedTable { .. } => PINKDB_STMT_DROPPED_TABLE,
        QueryResult::DroppedView { .. } => PINKDB_STMT_DROPPED_VIEW,
        QueryResult::CreatedDatabase { .. } => PINKDB_STMT_CREATED_DATABASE,
        QueryResult::DroppedDatabase { .. } => PINKDB_STMT_DROPPED_DATABASE,
        QueryResult::Used { .. } => PINKDB_STMT_USED,
        QueryResult::CreatedSchema { .. } => PINKDB_STMT_CREATED_SCHEMA,
        QueryResult::DroppedSchema { .. } => PINKDB_STMT_DROPPED_SCHEMA,
        QueryResult::SetVariable { .. } => PINKDB_STMT_SET_VARIABLE,
        QueryResult::LoadedExtension { .. } => PINKDB_STMT_LOADED_EXTENSION,
        QueryResult::Checkpointed => PINKDB_STMT_CHECKPOINTED,
        QueryResult::AlteredTable { .. } => PINKDB_STMT_ALTERED_TABLE,
        QueryResult::CreatedIndex { .. } => PINKDB_STMT_CREATED,
        QueryResult::DroppedIndex { .. } => PINKDB_STMT_DROPPED_TABLE,
        QueryResult::CreatedMaterializedView { .. } => PINKDB_STMT_CREATED,
        QueryResult::RefreshedMaterializedView { .. } => PINKDB_STMT_CREATED,
        QueryResult::RefreshedMaterializedViews { .. } => PINKDB_STMT_CREATED,
        QueryResult::DroppedMaterializedView { .. } => PINKDB_STMT_DROPPED_TABLE,
        QueryResult::CreatedProcedure { .. } => PINKDB_STMT_CREATED,
        QueryResult::DroppedProcedure { .. } => PINKDB_STMT_DROPPED_TABLE,
        QueryResult::CreatedTrigger { .. } => PINKDB_STMT_CREATED,
        QueryResult::DroppedTrigger { .. } => PINKDB_STMT_DROPPED_TABLE,
        QueryResult::CreatedMacro { .. } => PINKDB_STMT_CREATED,
        QueryResult::DroppedMacro { .. } => PINKDB_STMT_DROPPED_TABLE,
        QueryResult::Called { .. } => PINKDB_STMT_CREATED,
        QueryResult::CreatedUser { .. } => PINKDB_STMT_CREATED_USER,
        QueryResult::AlteredUser { .. } => PINKDB_STMT_ALTERED_USER,
        QueryResult::DroppedUser { .. } => PINKDB_STMT_DROPPED_USER,
        QueryResult::CreatedRole { .. } => PINKDB_STMT_CREATED_ROLE,
        QueryResult::DroppedRole { .. } => PINKDB_STMT_DROPPED_ROLE,
        QueryResult::Granted { .. } => PINKDB_STMT_GRANTED,
        QueryResult::Revoked { .. } => PINKDB_STMT_REVOKED,
    }
}

fn build_result(results: Vec<ChunkedResult>) -> std::result::Result<PinkResult, String> {
    let stmts = results
        .into_iter()
        .map(|r| {
            Ok(match r {
                ChunkedResult::Selected {
                    names,
                    types,
                    chunks,
                } => StmtData::Sel(SelStmt::from_chunks(names, types, chunks)?),
                // The row-shaped paths (a transaction, `RETURNING`, `CALL`)
                // still arrive as rows; rebuild chunks so fetch works there too.
                ChunkedResult::Other(QueryResult::Selected { names, types, rows }) => {
                    StmtData::Sel(SelStmt::from_rows(names, types, rows)?)
                }
                // `CALL` (wave 44): a call whose own result is a result set is
                // presented as that row set (the statement stream needs no new
                // C-ABI entry); any other call outcome is a simple statement whose
                // count is the number of body statements it ran.
                // A JavaScript body's `print` lines are not carried over the
                // C ABI yet (wave 81 follow-up).
                ChunkedResult::Other(QueryResult::Called {
                    name,
                    statements,
                    result: Some(inner),
                    ..
                }) => match *inner {
                    QueryResult::Selected { names, types, rows } => {
                        StmtData::Sel(SelStmt::from_rows(names, types, rows)?)
                    }
                    _ => StmtData::Simple {
                        kind: PINKDB_STMT_CREATED,
                        count: statements,
                        name: Some(nul_terminated(&name)),
                    },
                },
                ChunkedResult::Other(other) => {
                    let kind = kind_of(&other);
                    let (count, name) = match &other {
                        QueryResult::Inserted { count }
                        | QueryResult::Updated { count }
                        | QueryResult::Deleted { count } => (*count, None),
                        QueryResult::Called {
                            name, statements, ..
                        } => (*statements, Some(nul_terminated(name))),
                        QueryResult::CreatedView { name }
                        | QueryResult::DroppedTable { name }
                        | QueryResult::DroppedView { name }
                        | QueryResult::CreatedDatabase { name }
                        | QueryResult::DroppedDatabase { name }
                        | QueryResult::CreatedSchema { name }
                        | QueryResult::DroppedSchema { name }
                        | QueryResult::AlteredTable { name }
                        | QueryResult::CreatedProcedure { name }
                        | QueryResult::DroppedProcedure { name }
                        | QueryResult::CreatedTrigger { name }
                        | QueryResult::DroppedTrigger { name }
                        | QueryResult::CreatedMacro { name }
                        | QueryResult::DroppedMacro { name }
                        | QueryResult::CreatedUser { name }
                        | QueryResult::AlteredUser { name }
                        | QueryResult::DroppedUser { name }
                        | QueryResult::CreatedRole { name }
                        | QueryResult::DroppedRole { name } => (0, Some(nul_terminated(name))),
                        QueryResult::Granted { description }
                        | QueryResult::Revoked { description } => {
                            (0, Some(nul_terminated(description)))
                        }
                        QueryResult::Used { db, schema } => {
                            (0, Some(nul_terminated(&format!("{db}.{schema}"))))
                        }
                        QueryResult::SetVariable { name } => (0, Some(nul_terminated(name))),
                        QueryResult::LoadedExtension { name } => (0, Some(nul_terminated(name))),
                        _ => (0, None),
                    };
                    StmtData::Simple { kind, count, name }
                }
            })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    Ok(PinkResult {
        stmts,
        profiles: Vec::new(),
        error: None,
        error_class: None,
    })
}

/// Build a result that also owns one profile per statement. The engine
/// returns pairs in statement order, so the arrays line up with the
/// statement-index accessors.
fn build_profiled_result(
    pairs: Vec<(QueryResult, QueryProfile)>,
) -> std::result::Result<PinkResult, String> {
    let (results, profiles): (Vec<QueryResult>, Vec<QueryProfile>) = pairs.into_iter().unzip();
    let mut result = build_result(results.into_iter().map(ChunkedResult::Other).collect())?;
    result.profiles = profiles.iter().map(ProfileData::from_profile).collect();
    Ok(result)
}

// ----- database, connection and config objects -----

/// The store and the policy fixed when it was opened. Connections hold an
/// `Arc`, so closing the database handle stops new queries but does not pull
/// the store out from under a running connection.
struct DbInner {
    db: Database,
    read_only: bool,
    closed: AtomicBool,
    /// Applied to every new session. `threads` is a session setting; the WAL
    /// sync method is process-wide and only meaningful for durable stores.
    threads: Option<i64>,
    wal_sync_method: Option<&'static str>,
}

/// Opaque database handle (a store plus its open-time policy). May be created
/// on any thread and shared between threads; only connections are
/// single-threaded.
pub struct PinkDatabase {
    inner: Arc<DbInner>,
}

/// One connection's mutable engine session. `pinkdb_interrupt` reads only the
/// cancel flag next to this cell, never the session, so a shared reference is
/// enough there.
struct ConnState {
    conn: Connection,
    /// Per-connection session setting (`pinkdb_set_profiling`).
    profiling: bool,
}

/// Opaque connection handle. A connection is created from any thread but must
/// not be used from two threads at once, except for `pinkdb_interrupt`.
pub struct PinkConnection {
    inner: Arc<DbInner>,
    cancel: CancelFlag,
    state: UnsafeCell<ConnState>,
}

/// Builder for `pinkdb_open_ext`. Fields are validated by
/// `pinkdb_config_set`, so opening cannot fail on a bad setting.
#[derive(Default)]
pub struct PinkConfig {
    read_only: bool,
    memory_limit: Option<usize>,
    threads: Option<i64>,
    wal_sync_method: Option<WalSyncMethod>,
    encryption_key: Option<[u8; 32]>,
    allow_sql_load_extension: bool,
    /// Last `pinkdb_config_set` failure; borrowed until the next set/destroy.
    error: Option<CString>,
}

/// Opaque v1 handle: one database with one session, kept only so the existing
/// bindings compile until 112b moves them to the connection API.
pub struct PinkHandle {
    conn: Box<PinkConnection>,
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Some(true),
        "0" | "false" | "off" | "no" => Some(false),
        _ => None,
    }
}

fn c_text<'a>(value: *const c_char, what: &str) -> Result<&'a str, String> {
    if value.is_null() {
        return Err(format!("null {what}"));
    }
    // SAFETY: non-null and required by the API contract to be a NUL-terminated
    // C string.
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map_err(|_| format!("{what} is not valid UTF-8"))
}

fn config_set(config: &mut PinkConfig, name: &str, value: &str) -> Result<(), String> {
    match name.trim().to_ascii_lowercase().as_str() {
        "read_only" => {
            config.read_only =
                parse_bool(value).ok_or_else(|| format!("invalid boolean '{value}'"))?;
        }
        "memory_limit" => {
            config.memory_limit =
                Some(pinkdb::memory::parse_limit(value).map_err(|e| format!("memory_limit: {e}"))?);
        }
        "threads" => {
            let n: i64 = value
                .trim()
                .parse()
                .map_err(|_| format!("threads: '{value}' is not an integer"))?;
            if n <= 0 {
                return Err(format!("threads: {n} is not positive"));
            }
            config.threads = Some(n);
        }
        "wal_sync_method" => {
            config.wal_sync_method = Some(
                WalSyncMethod::parse(value)
                    .ok_or_else(|| format!("wal_sync_method: unknown method '{value}'"))?,
            );
        }
        "access_mode" => match value.trim().to_ascii_lowercase().as_str() {
            "automatic" | "read_write" => config.read_only = false,
            "read_only" => config.read_only = true,
            _ => return Err(format!("access_mode: unknown mode '{value}'")),
        },
        "allow_sql_load_extension" => {
            config.allow_sql_load_extension =
                parse_bool(value).ok_or_else(|| format!("invalid boolean '{value}'"))?;
        }
        _ => return Err(format!("unknown configuration option '{name}'")),
    }
    Ok(())
}

fn open_from_config(path: Option<&str>, config: &PinkConfig) -> Result<Database, String> {
    if config.encryption_key.is_some() && path.is_none() {
        return Err("an encryption key requires a durable path".to_string());
    }
    let db = match path {
        Some(path) => match config.encryption_key {
            // SAFETY: the 32-byte array is a valid raw key; the copy stays in
            // this stack frame and `EncryptionKey` zeroizes its own copy.
            Some(key) => Database::open_with_key(path, &EncryptionKey::from_raw(key))
                .map_err(|e| e.to_string())?,
            None => Database::open(path).map_err(|e| e.to_string())?,
        },
        None => Database::new(),
    };
    db.set_allow_sql_load_extension(config.allow_sql_load_extension);
    if let Some(bytes) = config.memory_limit {
        // The engine's limit is process-wide, like DuckDB's max_memory.
        pinkdb::memory::set_limit(bytes);
    }
    Ok(db)
}

fn new_connection(inner: Arc<DbInner>) -> Result<PinkConnection, String> {
    let mut conn = inner.db.connect();
    if let Some(n) = inner.threads {
        conn.execute(&format!("SET threads = {n}"))
            .map_err(|e| e.to_string())?;
    }
    if let Some(method) = inner.wal_sync_method
        && inner.db.is_durable()
    {
        conn.execute(&format!("SET wal_sync_method = '{method}'"))
            .map_err(|e| e.to_string())?;
    }
    let cancel = CancelFlag::new();
    conn.set_cancel_flag(Some(cancel.clone()));
    Ok(PinkConnection {
        cancel,
        state: UnsafeCell::new(ConnState {
            conn,
            profiling: false,
        }),
        inner,
    })
}

/// A query failure before a `PinkResult` exists.
enum QueryFailure {
    Engine(PinkDbError),
    Build(String),
}

fn failure_result(failure: QueryFailure) -> *mut PinkResult {
    match failure {
        QueryFailure::Engine(e) => Box::into_raw(Box::new(PinkResult::failed(
            engine_error_class(&e),
            &e.to_string(),
        ))),
        QueryFailure::Build(message) => failed_result(CLASS_TYPE, &message),
    }
}

fn read_only_violation(sql: &str) -> bool {
    let Ok(statements) = pinkdb::sql::parse_statements(sql) else {
        // The executor reports the parse error with its usual class.
        return false;
    };
    statements
        .iter()
        .any(|statement| !pinkdb::serve::statement_allowed(statement, false))
}

fn check_read_only(sql: &str) -> Option<*mut PinkResult> {
    if read_only_violation(sql) {
        return Some(failed_result(
            CLASS_PERMISSION,
            "read-only database: only queries are allowed",
        ));
    }
    None
}

fn executed(conn: &PinkConnection, text: &str) -> *mut PinkResult {
    if conn.inner.closed.load(Ordering::Acquire) {
        return failed_result(CLASS_INVALID_ARGUMENT, "database is closed");
    }
    if conn.inner.read_only
        && let Some(refused) = check_read_only(text)
    {
        return refused;
    }
    // A previous interrupt must not leak into this statement.
    conn.cancel.reset();
    // SAFETY: the connection is used from one thread at a time by contract;
    // `pinkdb_interrupt` never touches this cell.
    let state = unsafe { &mut *conn.state.get() };
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if state.profiling {
            match state.conn.execute_profiled(text) {
                Ok(pairs) => build_profiled_result(pairs).map_err(QueryFailure::Build),
                Err(e) => Err(QueryFailure::Engine(e)),
            }
        } else {
            match state.conn.execute_chunked(text) {
                Ok(results) => build_result(results).map_err(QueryFailure::Build),
                Err(e) => Err(QueryFailure::Engine(e)),
            }
        }
    }));
    match outcome {
        Ok(Ok(result)) => Box::into_raw(Box::new(result)),
        Ok(Err(failure)) => failure_result(failure),
        Err(_) => failed_result(CLASS_INTERNAL, "internal error: statement panicked"),
    }
}

fn bind_values(
    prepared: &PinkPrepared,
    values: *const PinkValue,
    value_count: usize,
) -> Result<Vec<Value>, PinkResult> {
    if values.is_null() && value_count > 0 {
        return Err(PinkResult::failed(
            CLASS_INVALID_ARGUMENT,
            "null values pointer with non-zero count",
        ));
    }
    // L-7: `value_count` arrives from the caller before the array can be
    // validated. A `Vec::with_capacity(2^62)` aborts the process, so check the
    // count against what the prepared statement actually takes (and a hard
    // ceiling far above any real statement) *before* touching the allocator.
    const MAX_BOUND_VALUES: usize = 65_536;
    let expected = prepared.stmt.parameter_count();
    if value_count > MAX_BOUND_VALUES {
        return Err(PinkResult::failed(
            CLASS_INVALID_ARGUMENT,
            &format!(
                "value_count {value_count} exceeds the maximum of {MAX_BOUND_VALUES} bound values"
            ),
        ));
    }
    if value_count != expected {
        // Keep the engine's established wording (the bindings assert it
        // verbatim); the only change is that the check now happens before
        // the allocation.
        let message = if value_count < expected {
            format!("values were not provided for prepared statement parameters: {expected}")
        } else {
            format!(
                "parameter count mismatch: the prepared statement takes {expected}, but values \
                 were provided for: {value_count}"
            )
        };
        return Err(PinkResult::failed(CLASS_PARAMETER, &message));
    }
    let mut bound: Vec<Value> = Vec::with_capacity(value_count);
    for i in 0..value_count {
        // SAFETY: non-null and valid for `value_count` entries per the API
        // contract.
        let raw = unsafe { &*values.add(i) };
        match value_from_pink(raw) {
            Ok(v) => bound.push(v),
            Err(msg) => {
                return Err(PinkResult::failed(
                    CLASS_INVALID_ARGUMENT,
                    &format!("value {i}: {msg}"),
                ));
            }
        }
    }
    Ok(bound)
}

fn prepare_on(conn: &PinkConnection, text: &str) -> Result<PinkPrepared, QueryFailure> {
    if conn.inner.closed.load(Ordering::Acquire) {
        return Err(QueryFailure::Build("database is closed".to_string()));
    }
    if conn.inner.read_only && read_only_violation(text) {
        return Err(QueryFailure::Engine(PinkDbError::Permission(
            "read-only database: only queries are allowed".to_string(),
        )));
    }
    // SAFETY: one thread uses the connection at a time.
    let state = unsafe { &mut *conn.state.get() };
    state
        .conn
        .prepare(text)
        .map(|stmt| PinkPrepared { stmt })
        .map_err(QueryFailure::Engine)
}

fn executed_prepared(
    conn: &PinkConnection,
    prepared: &PinkPrepared,
    bound: &[Value],
) -> *mut PinkResult {
    if conn.inner.closed.load(Ordering::Acquire) {
        return failed_result(CLASS_INVALID_ARGUMENT, "database is closed");
    }
    conn.cancel.reset();
    // SAFETY: one thread uses the connection at a time.
    let state = unsafe { &mut *conn.state.get() };
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let bound_stmt = state
            .conn
            .bind(&prepared.stmt, bound)
            .map_err(QueryFailure::Engine)?;
        if conn.inner.read_only && read_only_violation(bound_stmt.sql()) {
            return Err(QueryFailure::Engine(PinkDbError::Permission(
                "read-only database: only queries are allowed".to_string(),
            )));
        }
        if state.profiling {
            // The engine's profiled runner takes SQL, so run the same
            // substituted statement through it.
            match state.conn.execute_profiled(bound_stmt.sql()) {
                Ok(pairs) => build_profiled_result(pairs).map_err(QueryFailure::Build),
                Err(e) => Err(QueryFailure::Engine(e)),
            }
        } else {
            match state.conn.execute_bound_chunked(&bound_stmt) {
                Ok(result) => build_result(vec![result]).map_err(QueryFailure::Build),
                Err(e) => Err(QueryFailure::Engine(e)),
            }
        }
    }));
    match outcome {
        Ok(Ok(result)) => Box::into_raw(Box::new(result)),
        Ok(Err(failure)) => failure_result(failure),
        Err(_) => failed_result(CLASS_INTERNAL, "internal error: statement panicked"),
    }
}

/// Copy a failed result's error into the v1 thread-local and free it. Every
/// success passes through unchanged.
fn v1_result(result: *mut PinkResult) -> *mut PinkResult {
    if result.is_null() {
        return result;
    }
    // SAFETY: produced by this crate and not yet freed.
    let r = unsafe { &*result };
    if let Some(error) = &r.error {
        set_error(
            r.error_class.unwrap_or(CLASS_INTERNAL),
            &error.to_string_lossy(),
        );
        // SAFETY: produced by Box::into_raw above, freed once here.
        drop(unsafe { Box::from_raw(result) });
        return ptr::null_mut();
    }
    result
}

fn c_path(path: *const c_char) -> Result<String, &'static str> {
    if path.is_null() {
        return Err("null path");
    }
    // SAFETY: non-null and required by the API contract to be a NUL-terminated
    // C string. The pointer need not be aligned; CStr handles bytes.
    unsafe { CStr::from_ptr(path) }
        .to_str()
        .map(str::to_string)
        .map_err(|_| "path is not valid UTF-8")
}

fn write_errmsg(errmsg: *mut *mut c_char, message: Option<&str>) {
    if errmsg.is_null() {
        return;
    }
    // SAFETY: the caller passes a writable out-parameter.
    unsafe {
        *errmsg = match message {
            Some(text) => CString::new(text.replace('\0', "\\0"))
                .map(CString::into_raw)
                .unwrap_or(ptr::null_mut()),
            None => ptr::null_mut(),
        };
    }
}

/// Create an empty configuration. Free with [`pinkdb_destroy_config`].
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_create_config() -> *mut PinkConfig {
    Box::into_raw(Box::new(PinkConfig::default()))
}

/// Set one configuration option. Names and accepted values:
///
/// - `read_only`: `true`/`false`;
/// - `access_mode`: `automatic`, `read_only` or `read_write`;
/// - `memory_limit`: a DuckDB-style size (`1GB`, `512MB`, `-1` for no limit);
/// - `threads`: a positive integer;
/// - `wal_sync_method`: `full`, `fsync` or `fdatasync`;
/// - `allow_sql_load_extension`: `true`/`false`.
///
/// Returns 1 on success, 0 on an unknown name or invalid value; see
/// [`pinkdb_config_error`]. The strings are read only for this call.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_config_set(
    config: *mut PinkConfig,
    name: *const c_char,
    value: *const c_char,
) -> i32 {
    if config.is_null() {
        return 0;
    }
    // SAFETY: non-null (checked) and owned by the caller.
    let config = unsafe { &mut *config };
    let parsed = c_text(name, "configuration name")
        .and_then(|name| Ok((name, c_text(value, "configuration value")?)))
        .and_then(|(name, value)| config_set(config, name, value));
    match parsed {
        Ok(()) => {
            config.error = None;
            1
        }
        Err(message) => {
            config.error = CString::new(message.replace('\0', "\\0")).ok();
            0
        }
    }
}

/// Copy a 32-byte raw encryption key into the configuration. The key is never
/// logged, echoed, or included in an error message. Returns 0 for a NULL
/// pointer or a length other than 32; see [`pinkdb_config_error`].
///
/// Keyed stores need the library's `encryption` feature; without it the open
/// fails with a "not compiled in" error. In-memory databases cannot be keyed.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_config_set_encryption_key(
    config: *mut PinkConfig,
    key: *const u8,
    key_len: usize,
) -> i32 {
    if config.is_null() {
        return 0;
    }
    // SAFETY: non-null (checked) and owned by the caller.
    let config = unsafe { &mut *config };
    if key.is_null() || key_len != 32 {
        config.error =
            Some(CString::new("encryption key must be exactly 32 bytes").unwrap_or_default());
        return 0;
    }
    let mut raw = [0u8; 32];
    // SAFETY: non-null and valid for 32 bytes by the check above and the API
    // contract.
    raw.copy_from_slice(unsafe { std::slice::from_raw_parts(key, 32) });
    config.encryption_key = Some(raw);
    config.error = None;
    1
}

/// The last [`pinkdb_config_set`] / [`pinkdb_config_set_encryption_key`]
/// failure on this configuration, or NULL. The pointer is owned by the
/// configuration and valid until its next set call or
/// [`pinkdb_destroy_config`].
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_config_error(config: *const PinkConfig) -> *const c_char {
    if config.is_null() {
        return ptr::null();
    }
    // SAFETY: non-null, read-only access by the caller's ownership.
    match unsafe { &*config }.error.as_ref() {
        Some(message) => message.as_ptr(),
        None => ptr::null(),
    }
}

/// Destroy a configuration. NULL-safe. Does not touch any database opened
/// with it (the open copies every setting).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_destroy_config(config: *mut PinkConfig) {
    if config.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_create_config`, freed once.
    drop(unsafe { Box::from_raw(config) });
}

/// Open a database with an optional configuration.
///
/// `path` NULL or `":memory:"` opens an in-memory store; any other path opens
/// (or creates) a durable checkpoint + WAL. `config` may be NULL for defaults.
/// Returns 1 on success and 0 on failure. On failure `*out_database` is NULL
/// and `*errmsg` is a malloc'd string that the caller frees with
/// [`pinkdb_free`]; on success `*errmsg` is NULL. `out_database` and `errmsg`
/// must both be non-NULL.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_open_ext(
    path: *const c_char,
    config: *const PinkConfig,
    out_database: *mut *mut PinkDatabase,
    errmsg: *mut *mut c_char,
) -> i32 {
    if !out_database.is_null() {
        // SAFETY: the caller passes a writable out-parameter.
        unsafe { *out_database = ptr::null_mut() };
    }
    write_errmsg(errmsg, None);
    if out_database.is_null() || errmsg.is_null() {
        return 0;
    }
    let path = if path.is_null() {
        None
    } else {
        match c_path(path) {
            Ok(path) if path != ":memory:" => Some(path),
            Ok(_) => None,
            Err(message) => {
                write_errmsg(errmsg, Some(message));
                return 0;
            }
        }
    };
    let defaults = PinkConfig::default();
    // SAFETY: `config` is either NULL or a live configuration owned by the
    // caller for the duration of the call.
    let config = if config.is_null() {
        &defaults
    } else {
        unsafe { &*config }
    };
    match catch_unwind(AssertUnwindSafe(|| {
        open_from_config(path.as_deref(), config)
    })) {
        Ok(Ok(db)) => {
            let database = Box::new(PinkDatabase {
                inner: Arc::new(DbInner {
                    db,
                    read_only: config.read_only,
                    closed: AtomicBool::new(false),
                    threads: config.threads,
                    wal_sync_method: config.wal_sync_method.map(WalSyncMethod::as_str),
                }),
            });
            // SAFETY: non-null (checked) and writable.
            unsafe { *out_database = Box::into_raw(database) };
            1
        }
        Ok(Err(message)) => {
            write_errmsg(errmsg, Some(&message));
            0
        }
        Err(_) => {
            write_errmsg(
                errmsg,
                Some("internal error: panicked while opening the database"),
            );
            0
        }
    }
}

/// Close a database opened by [`pinkdb_open_ext`]. NULL-safe. Existing
/// connections keep the store memory alive until they are disconnected, but
/// every later query on them fails with `invalid_argument`.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_close_database(database: *mut PinkDatabase) {
    if database.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_open_ext`, freed once.
    let database = unsafe { Box::from_raw(database) };
    database.inner.closed.store(true, Ordering::Release);
}

/// Create a connection (session) on a database. Connections are independent:
/// each has its own transaction, temp tables and settings. Returns 1 on
/// success and 0 on failure (NULL/closed database or allocation failure).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_connect(
    database: *mut PinkDatabase,
    out_connection: *mut *mut PinkConnection,
) -> i32 {
    if out_connection.is_null() {
        return 0;
    }
    // SAFETY: the caller passes a writable out-parameter.
    unsafe { *out_connection = ptr::null_mut() };
    if database.is_null() {
        return 0;
    }
    // SAFETY: non-null and owned by the caller for the call.
    let database = unsafe { &*database };
    if database.inner.closed.load(Ordering::Acquire) {
        return 0;
    }
    match catch_unwind(AssertUnwindSafe(|| {
        new_connection(Arc::clone(&database.inner))
    })) {
        Ok(Ok(connection)) => {
            // SAFETY: non-null (checked) and writable.
            unsafe { *out_connection = Box::into_raw(Box::new(connection)) };
            1
        }
        _ => 0,
    }
}

/// Disconnect a connection. NULL-safe. A connection must not be used after
/// this call.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_disconnect(connection: *mut PinkConnection) {
    if connection.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_connect`, freed once.
    drop(unsafe { Box::from_raw(connection) });
}

/// Ask the statement running on `connection` to stop. May be called from
/// another thread while that statement runs, and from the same thread before
/// it starts (the next query clears the flag first, so pre-start interrupts
/// are ignored). The statement stops at the executor's next cancellation
/// checkpoint with the `interrupted` error class. NULL-safe.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_interrupt(connection: *mut PinkConnection) {
    if connection.is_null() {
        return;
    }
    // SAFETY: non-null and owned by the caller; only the cancel flag is
    // touched, which is why this call is allowed concurrently with a query.
    unsafe { &*connection }.cancel.cancel();
}

/// Execute SQL on a connection. Returns 1 when the statements ran and 0 when
/// they did not; either way `*out_result` receives a result owned by the
/// caller (free with [`pinkdb_result_free`]). Use
/// [`pinkdb_result_error`]/[`pinkdb_result_error_class`] to inspect a failed
/// result. `out_result` must be non-NULL.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_query(
    connection: *mut PinkConnection,
    sql: *const c_char,
    sql_len: usize,
    out_result: *mut *mut PinkResult,
) -> i32 {
    if out_result.is_null() {
        return 0;
    }
    let result = if connection.is_null() {
        failed_result(CLASS_INVALID_ARGUMENT, "null connection")
    } else {
        // SAFETY: the caller guarantees (ptr, len) validity for the call.
        match unsafe { sql_utf8_result(sql, sql_len) } {
            Ok(text) => {
                // SAFETY: non-null and one thread uses the connection at a
                // time by contract.
                executed(unsafe { &*connection }, text)
            }
            Err(message) => failed_result(CLASS_INVALID_ARGUMENT, message),
        }
    };
    // SAFETY: non-null (checked) and writable.
    unsafe { *out_result = result };
    // SAFETY: just stored above and not freed.
    if unsafe { &*result }.error.is_none() {
        1
    } else {
        0
    }
}

/// Parse one statement with `?` placeholders on a connection. Returns 1 on
/// success and 0 on failure. On failure `*out_prepared` is NULL and `*errmsg`
/// is freed with [`pinkdb_free`]; on success `*errmsg` is NULL. Both out
/// parameters must be non-NULL.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_prepare_ext(
    connection: *mut PinkConnection,
    sql: *const c_char,
    sql_len: usize,
    out_prepared: *mut *mut PinkPrepared,
    errmsg: *mut *mut c_char,
) -> i32 {
    if !out_prepared.is_null() {
        // SAFETY: the caller passes a writable out-parameter.
        unsafe { *out_prepared = ptr::null_mut() };
    }
    write_errmsg(errmsg, None);
    if out_prepared.is_null() || errmsg.is_null() {
        return 0;
    }
    if connection.is_null() {
        write_errmsg(errmsg, Some("null connection"));
        return 0;
    }
    // SAFETY: the caller guarantees (ptr, len) validity for the call.
    let text = match unsafe { sql_utf8_result(sql, sql_len) } {
        Ok(text) => text,
        Err(message) => {
            write_errmsg(errmsg, Some(message));
            return 0;
        }
    };
    // SAFETY: non-null and one thread uses the connection at a time.
    let connection = unsafe { &*connection };
    let outcome = catch_unwind(AssertUnwindSafe(|| prepare_on(connection, text)));
    match outcome {
        Ok(Ok(prepared)) => {
            // SAFETY: non-null (checked) and writable.
            unsafe { *out_prepared = Box::into_raw(Box::new(prepared)) };
            1
        }
        Ok(Err(QueryFailure::Engine(e))) => {
            write_errmsg(errmsg, Some(&e.to_string()));
            0
        }
        Ok(Err(QueryFailure::Build(message))) => {
            write_errmsg(errmsg, Some(&message));
            0
        }
        Err(_) => {
            write_errmsg(errmsg, Some("internal error: prepare panicked"));
            0
        }
    }
}

/// Execute a prepared statement with one `pink_value` per placeholder, in
/// textual order. Returns 1/0 exactly like [`pinkdb_query`]; `*out_result` is
/// always set and owned by the caller. Values are read only for the call.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_query_prepared(
    connection: *mut PinkConnection,
    prepared: *const PinkPrepared,
    values: *const PinkValue,
    value_count: usize,
    out_result: *mut *mut PinkResult,
) -> i32 {
    if out_result.is_null() {
        return 0;
    }
    let result = if connection.is_null() {
        failed_result(CLASS_INVALID_ARGUMENT, "null connection")
    } else if prepared.is_null() {
        failed_result(CLASS_INVALID_ARGUMENT, "null prepared statement")
    } else {
        // SAFETY: non-null (checked) and owned by the caller for the call.
        let conn = unsafe { &*connection };
        let prep = unsafe { &*prepared };
        match bind_values(prep, values, value_count) {
            Ok(bound) => executed_prepared(conn, prep, &bound),
            Err(result) => Box::into_raw(Box::new(result)),
        }
    };
    // SAFETY: non-null (checked) and writable.
    unsafe { *out_result = result };
    // SAFETY: just stored above and not freed.
    if unsafe { &*result }.error.is_none() {
        1
    } else {
        0
    }
}

/// Free a string returned through an `errmsg` out-parameter. NULL-safe. Do
/// not pass any other pointer: this is not a general-purpose `free`.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: every `errmsg` is produced by CString::into_raw, so this is the
    // matching deallocation.
    drop(unsafe { CString::from_raw(ptr as *mut c_char) });
}

/// The error carried by a result, or NULL when the result succeeded. The
/// pointer is owned by the result and valid until [`pinkdb_result_free`].
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_error(result: *const PinkResult) -> *const c_char {
    if result.is_null() {
        return ptr::null();
    }
    // SAFETY: non-null, read-only access by the caller's ownership.
    match unsafe { &*result }.error.as_ref() {
        Some(message) => message.as_ptr(),
        None => ptr::null(),
    }
}

/// The stable lower-case error class of [`pinkdb_result_error`], or NULL when
/// the result succeeded. The pointer is static: never free it.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_error_class(result: *const PinkResult) -> *const c_char {
    if result.is_null() {
        return ptr::null();
    }
    // SAFETY: non-null, read-only access by the caller's ownership.
    match unsafe { &*result }.error_class {
        Some(class) => class.as_ptr(),
        None => ptr::null(),
    }
}

/// Open a durable database at `path`. Returns NULL on error; see
/// [`pinkdb_last_error`]. Kept for the v1 bindings; new code uses
/// [`pinkdb_open_ext`].
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_open(path: *const c_char) -> *mut PinkHandle {
    let path = match c_path(path) {
        Ok(path) => path,
        Err(message) => {
            set_error(CLASS_INVALID_ARGUMENT, message);
            return ptr::null_mut();
        }
    };
    clear_error();
    let config = PinkConfig::default();
    let opened = catch_unwind(AssertUnwindSafe(|| {
        open_from_config(Some(&path), &config).and_then(|db| {
            let inner = Arc::new(DbInner {
                db,
                read_only: config.read_only,
                closed: AtomicBool::new(false),
                threads: config.threads,
                wal_sync_method: None,
            });
            new_connection(inner)
        })
    }));
    match opened {
        Ok(Ok(conn)) => Box::into_raw(Box::new(PinkHandle {
            conn: Box::new(conn),
        })),
        Ok(Err(message)) => {
            set_error(CLASS_EXECUTION, &message);
            ptr::null_mut()
        }
        Err(_) => {
            set_error(
                CLASS_INTERNAL,
                "internal error: panicked while opening the database",
            );
            ptr::null_mut()
        }
    }
}

/// Open a fresh in-memory database. Kept for the v1 bindings; new code uses
/// [`pinkdb_open_ext`] with a NULL path.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_open_memory() -> *mut PinkHandle {
    clear_error();
    let config = PinkConfig::default();
    match catch_unwind(AssertUnwindSafe(|| {
        let db = open_from_config(None, &config)?;
        let inner = Arc::new(DbInner {
            db,
            read_only: config.read_only,
            closed: AtomicBool::new(false),
            threads: config.threads,
            wal_sync_method: None,
        });
        new_connection(inner).map(|conn| PinkHandle {
            conn: Box::new(conn),
        })
    })) {
        Ok(Ok(handle)) => Box::into_raw(Box::new(handle)),
        Ok(Err(message)) => {
            set_error(CLASS_EXECUTION, &message);
            ptr::null_mut()
        }
        Err(_) => {
            set_error(
                CLASS_INTERNAL,
                "internal error: panicked while opening the database",
            );
            ptr::null_mut()
        }
    }
}

/// Close a handle opened by [`pinkdb_open`]/[`pinkdb_open_memory`]. NULL-safe.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_close(handle: *mut PinkHandle) {
    if handle.is_null() {
        return;
    }
    // SAFETY: the pointer came from Box::into_raw in an open function and is
    // consumed exactly once here (documented single-free contract).
    drop(unsafe { Box::from_raw(handle) });
}

/// Execute one or more SQL statements (UTF-8, `(ptr, len)`, embedded NULs
/// allowed). Returns NULL on error; free with [`pinkdb_result_free`].
///
/// With profiling enabled on the handle (see [`pinkdb_set_profiling`]) the
/// result also carries one profile per statement; otherwise this calls the
/// plain `Connection::execute` path (no clocks, no profile allocation).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_execute(
    handle: *mut PinkHandle,
    sql: *const c_char,
    sql_len: usize,
) -> *mut PinkResult {
    if handle.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null handle");
        return ptr::null_mut();
    }
    // SAFETY: the caller guarantees (ptr, len) validity for the call.
    let Some(text) = (unsafe { sql_utf8(sql, sql_len) }) else {
        return ptr::null_mut();
    };
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract; no other reference exists during the call.
    let h = unsafe { &*handle };
    v1_result(executed(&h.conn, text))
}

/// Enable (non-zero) or disable (zero) query profiling on this handle. Off by
/// default; the setting is per handle (per session) and applies to later
/// `pinkdb_execute` / `pinkdb_execute_prepared` calls. Returns 1 on success, 0
/// on failure (a NULL handle — see `pinkdb_last_error`).
///
/// Profiling only reads clocks: it never changes a result or a WAL byte, and
/// when it is off both execute functions take the plain engine path.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_set_profiling(handle: *mut PinkHandle, enabled: i32) -> i32 {
    if handle.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null handle");
        return 0;
    }
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract; no other reference exists during the call.
    let h = unsafe { &*handle };
    // SAFETY: one thread uses the v1 handle at a time.
    unsafe { &mut *h.conn.state.get() }.profiling = enabled != 0;
    1
}

/// Allow (non-zero) or refuse (zero) the SQL `LOAD EXTENSION` statement on
/// this handle. Returns 1 on success, 0 on failure (a NULL handle — see
/// `pinkdb_last_error`).
///
/// # Security
///
/// `LOAD EXTENSION '<path>'` makes the host process load and run a native
/// library from a path the SQL text names. It is **disabled by default** on
/// every handle: `pinkdb_execute` can only load native code after this
/// function has enabled it. Enable it only for a handle whose SQL text comes
/// from a trusted source — an application that forwards untrusted input to
/// `pinkdb_execute` inherits arbitrary native code execution otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_set_allow_sql_load_extension(handle: *mut PinkHandle, allow: i32) -> i32 {
    if handle.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null handle");
        return 0;
    }
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract; no other reference exists during the call.
    let h = unsafe { &*handle };
    h.conn.inner.db.set_allow_sql_load_extension(allow != 0);
    1
}

/// Read an `(ptr, len)` SQL argument as UTF-8. The new API reports failures
/// through its result/`errmsg`; the v1 wrapper below also copies them into
/// the thread-local.
///
/// # Safety
///
/// `sql` must be non-NULL whenever `sql_len > 0` and valid for `sql_len`
/// bytes for the duration of the returned borrow (the API contract for every
/// `(ptr, len)` entry point of this crate).
unsafe fn sql_utf8_result<'a>(sql: *const c_char, sql_len: usize) -> Result<&'a str, &'static str> {
    unsafe {
        if sql.is_null() && sql_len > 0 {
            return Err("null sql pointer with non-zero length");
        }
        let bytes: &[u8] = if sql_len == 0 {
            &[]
        } else {
            // SAFETY: non-null (checked above) and valid for `sql_len` bytes
            // per the API contract.
            std::slice::from_raw_parts(sql as *const u8, sql_len)
        };
        std::str::from_utf8(bytes).map_err(|_| "SQL is not valid UTF-8")
    }
}

/// v1 SQL reader: same as [`sql_utf8_result`] but sets the thread-local error.
///
/// # Safety
///
/// Same contract as [`sql_utf8_result`].
unsafe fn sql_utf8<'a>(sql: *const c_char, sql_len: usize) -> Option<&'a str> {
    match unsafe { sql_utf8_result(sql, sql_len) } {
        Ok(text) => Some(text),
        Err(message) => {
            set_error(CLASS_INVALID_ARGUMENT, message);
            None
        }
    }
}

// ----- prepared statements -----

/// Owns one prepared statement (see [`pinkdb_prepare`]). Not thread-safe,
/// like every other handle.
pub struct PinkPrepared {
    stmt: pinkdb::PreparedStatement,
}

/// Parse one SQL statement with `?` placeholders into a reusable prepared
/// statement; NULL on error (see [`pinkdb_last_error`]). A multi-statement
/// string, or a statement with no placeholders, is refused/allowed the same
/// way `Connection::prepare` is. Free with [`pinkdb_prepared_free`].
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_prepare(
    handle: *mut PinkHandle,
    sql: *const c_char,
    sql_len: usize,
) -> *mut PinkPrepared {
    if handle.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null handle");
        return ptr::null_mut();
    }
    // SAFETY: the caller guarantees (ptr, len) validity for the call.
    let Some(text) = (unsafe { sql_utf8(sql, sql_len) }) else {
        return ptr::null_mut();
    };
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract.
    let h = unsafe { &*handle };
    match catch_unwind(AssertUnwindSafe(|| prepare_on(&h.conn, text))) {
        Ok(Ok(stmt)) => Box::into_raw(Box::new(stmt)),
        Ok(Err(QueryFailure::Engine(e))) => {
            set_engine_error(&e);
            ptr::null_mut()
        }
        Ok(Err(QueryFailure::Build(message))) => {
            set_error(CLASS_TYPE, &message);
            ptr::null_mut()
        }
        Err(_) => {
            set_error(CLASS_INTERNAL, "internal error: prepare panicked");
            ptr::null_mut()
        }
    }
}

/// Number of `?` placeholders in a prepared statement: the exact number of
/// values [`pinkdb_execute_prepared`] takes. 0 for a NULL handle.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_prepared_parameter_count(prepared: *const PinkPrepared) -> usize {
    if prepared.is_null() {
        return 0;
    }
    // SAFETY: non-null, owned by the caller, read-only access.
    unsafe { &*prepared }.stmt.parameter_count()
}

/// Free a prepared statement. NULL-safe.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_prepared_free(prepared: *mut PinkPrepared) {
    if prepared.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_prepare`, freed once.
    drop(unsafe { Box::from_raw(prepared) });
}

/// Execute a prepared statement with one value per placeholder (in textual
/// order), like a normal statement of the handle's session. Returns NULL on
/// error; free with [`pinkdb_result_free`]. Values are read by value (for
/// VARCHAR, `text_ptr`/`text_len` must be valid for the call); a wrong count
/// or a value that cannot bind is a clear error.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_execute_prepared(
    handle: *mut PinkHandle,
    prepared: *const PinkPrepared,
    values: *const PinkValue,
    value_count: usize,
) -> *mut PinkResult {
    if handle.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null handle");
        return ptr::null_mut();
    }
    if prepared.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null prepared statement");
        return ptr::null_mut();
    }
    // SAFETY: non-null (checked) and owned by the caller for the call.
    let prep = unsafe { &*prepared };
    let bound = match bind_values(prep, values, value_count) {
        Ok(bound) => bound,
        Err(result) => {
            let message = result
                .error
                .as_ref()
                .map(|e| e.to_string_lossy().into_owned())
                .unwrap_or_default();
            set_error(result.error_class.unwrap_or(CLASS_INTERNAL), &message);
            return ptr::null_mut();
        }
    };
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract.
    let h = unsafe { &*handle };
    v1_result(executed_prepared(&h.conn, prep, &bound))
}

/// Convert one caller-provided `pink_value` into an engine value: the inverse
/// of [`Cell::value`]. `type_tag` decides the payload; `is_null` makes any
/// payload NULL. Out-of-range INTEGER/DATE payloads and invalid UTF-8 are
/// clear errors (never silent truncation).
fn value_from_pink(v: &PinkValue) -> Result<pinkdb::Value, String> {
    if v.is_null != 0 || v.type_tag == PINKDB_TYPE_NULL {
        return Ok(pinkdb::Value::Null);
    }
    Ok(match v.type_tag {
        PINKDB_TYPE_BOOLEAN => pinkdb::Value::Boolean(v.int64 != 0),
        PINKDB_TYPE_INTEGER => pinkdb::Value::Integer(
            i32::try_from(v.int64)
                .map_err(|_| format!("INTEGER payload {} does not fit in i32", v.int64))?,
        ),
        PINKDB_TYPE_BIGINT => pinkdb::Value::Bigint(v.int64),
        PINKDB_TYPE_DOUBLE => pinkdb::Value::Double(v.double),
        PINKDB_TYPE_VARCHAR => {
            if v.text_len == 0 {
                pinkdb::Value::Varchar(String::new())
            } else if v.text_ptr.is_null() {
                return Err("VARCHAR payload has a NULL text pointer".to_string());
            } else {
                // SAFETY: non-null and valid for `text_len` bytes per the API
                // contract.
                let bytes =
                    unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, v.text_len) };
                pinkdb::Value::Varchar(
                    std::str::from_utf8(bytes)
                        .map_err(|_| "VARCHAR payload is not valid UTF-8".to_string())?
                        .to_string(),
                )
            }
        }
        PINKDB_TYPE_DATE => pinkdb::Value::Date(
            i32::try_from(v.int64)
                .map_err(|_| format!("DATE payload {} does not fit in i32", v.int64))?,
        ),
        PINKDB_TYPE_TIMESTAMP => pinkdb::Value::Timestamp(v.int64),
        tag if tag & 0xff == PINKDB_TYPE_ARRAY_FLOAT => {
            let len = array_tag_len(tag);
            let want = len as usize * std::mem::size_of::<f32>();
            if v.text_len != want {
                return Err(format!(
                    "FLOAT[{len}] payload is {} bytes, not {want}",
                    v.text_len
                ));
            }
            if v.text_ptr.is_null() {
                return Err("ARRAY payload has a NULL text pointer".to_string());
            }
            // SAFETY: non-null and valid for `text_len` bytes per the API
            // contract.
            let bytes = unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, want) };
            let values = bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect::<Vec<f32>>();
            let array = pinkdb::array::ArrayValue::new(
                pinkdb::ArrayElem::Float,
                len,
                pinkdb::array::ArrayElems::Floats32(values),
                None,
            )
            .map_err(|e| e.to_string())?;
            pinkdb::Value::Array(std::sync::Arc::new(array))
        }
        tag if tag & 0xff == PINKDB_TYPE_ARRAY => {
            let len = array_tag_len(tag);
            let want = len as usize * std::mem::size_of::<f64>();
            if v.text_len != want {
                return Err(format!(
                    "DOUBLE[{len}] payload is {} bytes, not {want}",
                    v.text_len
                ));
            }
            if v.text_ptr.is_null() {
                return Err("ARRAY payload has a NULL text pointer".to_string());
            }
            // SAFETY: non-null and valid for `text_len` bytes per the API
            // contract.
            let bytes = unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, want) };
            let values = bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| f64::from_le_bytes(*c))
                .collect::<Vec<f64>>();
            // The payload always holds exactly `len` elements, so the
            // constructor cannot fail.
            let array = pinkdb::array::ArrayValue::new(
                pinkdb::ArrayElem::Double,
                len,
                pinkdb::array::ArrayElems::Floats(values),
                None,
            )
            .map_err(|e| e.to_string())?;
            pinkdb::Value::Array(std::sync::Arc::new(array))
        }
        // Results carry DECIMAL/INTERVAL as text cells; binding a *parameter*
        // back is not supported in ABI v1 (cast the text in SQL).
        PINKDB_TYPE_DECIMAL | PINKDB_TYPE_INTERVAL => {
            return Err(
                "DECIMAL/INTERVAL cannot be bound as parameters in ABI v1; pass the canonical \
                 text and CAST it in SQL"
                    .to_string(),
            );
        }
        other => return Err(format!("unknown value type tag {other}")),
    })
}

/// Free a result returned by [`pinkdb_execute`]. NULL-safe.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_free(result: *mut PinkResult) {
    if result.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_execute`, freed once.
    drop(unsafe { Box::from_raw(result) });
}

fn stmt_at_mut<'a>(result: *mut PinkResult, statement: usize) -> Option<&'a mut StmtData> {
    if result.is_null() {
        return None;
    }
    // SAFETY: non-null, owned by the caller, exclusive access for the fetch.
    let r = unsafe { &mut *result };
    r.stmts.get_mut(statement)
}

fn stmt_at<'a>(result: *const PinkResult, statement: usize) -> Option<&'a StmtData> {
    if result.is_null() {
        return None;
    }
    // SAFETY: non-null, owned by the caller, read-only access.
    let r = unsafe { &*result };
    r.stmts.get(statement)
}

/// Number of statements in the result.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_statement_count(result: *const PinkResult) -> usize {
    if result.is_null() {
        return 0;
    }
    // SAFETY: non-null, valid for the call.
    unsafe { &*result }.stmts.len()
}

/// Statement kind tag (e.g. [`PINKDB_STMT_SELECTED`]); -1 when out of range.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_kind(result: *const PinkResult, statement: usize) -> i32 {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(_)) => PINKDB_STMT_SELECTED,
        Some(StmtData::Simple { kind, .. }) => *kind,
        None => -1,
    }
}

/// For SELECT results: number of columns (0 for other kinds / out of range).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_count(result: *const PinkResult, statement: usize) -> usize {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s.names.len(),
        _ => 0,
    }
}

/// Column name (NUL-terminated, owned by the result) or NULL when out of range.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_name(
    result: *const PinkResult,
    statement: usize,
    column: usize,
) -> *const c_char {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => match s.names.get(column) {
            // SAFETY: the Vec bytes are heap-stable and NUL-terminated.
            Some(n) => n.as_ptr() as *const c_char,
            None => ptr::null(),
        },
        _ => ptr::null(),
    }
}

/// Column type tag (e.g. [`PINKDB_TYPE_INTEGER`]); -1 when out of range.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_type(
    result: *const PinkResult,
    statement: usize,
    column: usize,
) -> i32 {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s.types.get(column).copied().unwrap_or(-1),
        _ => -1,
    }
}

/// Rows in a SELECT result (0 for other kinds / out of range).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_row_count(result: *const PinkResult, statement: usize) -> usize {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s.row_count,
        _ => 0,
    }
}

/// Value at (statement, row, column) for a SELECT result. Out-of-range and
/// non-SELECT statements return a NULL `PinkValue`.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_value(
    result: *const PinkResult,
    statement: usize,
    row: usize,
    column: usize,
) -> PinkValue {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s
            .rows()
            .get(row)
            .and_then(|r| r.get(column))
            .map(Cell::value)
            .unwrap_or_else(PinkValue::null),
        _ => PinkValue::null(),
    }
}

/// DECIMAL precision, or -1 when the column is not DECIMAL.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_decimal_width(
    result: *const PinkResult,
    statement: usize,
    column: usize,
) -> i32 {
    match stmt_at(result, statement).and_then(|s| match s {
        StmtData::Sel(s) => s.dtypes.get(column),
        _ => None,
    }) {
        Some(DataType::Decimal { precision, .. }) => i32::from(*precision),
        _ => -1,
    }
}

/// DECIMAL scale, or -1 when the column is not DECIMAL.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_decimal_scale(
    result: *const PinkResult,
    statement: usize,
    column: usize,
) -> i32 {
    match stmt_at(result, statement).and_then(|s| match s {
        StmtData::Sel(s) => s.dtypes.get(column),
        _ => None,
    }) {
        Some(DataType::Decimal { scale, .. }) => i32::from(*scale),
        _ => -1,
    }
}

/// ARRAY element count, or 0 when the column is not an ARRAY.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_array_length(
    result: *const PinkResult,
    statement: usize,
    column: usize,
) -> u64 {
    match stmt_at(result, statement).and_then(|s| match s {
        StmtData::Sel(s) => s.dtypes.get(column),
        _ => None,
    }) {
        Some(DataType::Array { len, .. }) => u64::from(*len),
        _ => 0,
    }
}

/// An ARRAY's element type ([`PINKDB_TYPE_DOUBLE`] or
/// [`PINKDB_TYPE_FLOAT`]), or -1 when the column is not an ARRAY.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_column_array_element_type(
    result: *const PinkResult,
    statement: usize,
    column: usize,
) -> i32 {
    match stmt_at(result, statement).and_then(|s| match s {
        StmtData::Sel(s) => s.dtypes.get(column),
        _ => None,
    }) {
        Some(DataType::Array { elem, .. }) if *elem == pinkdb::ArrayElem::Float => {
            PINKDB_TYPE_FLOAT
        }
        Some(DataType::Array { .. }) => PINKDB_TYPE_DOUBLE,
        _ => -1,
    }
}

/// The next chunk of a SELECT result, or NULL at the end, for a failed result,
/// or for a non-SELECT statement. The caller owns the chunk and frees it with
/// [`pinkdb_destroy_chunk`]; a chunk stays valid after its result is freed.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_fetch_chunk(
    result: *mut PinkResult,
    statement: usize,
) -> *mut PinkChunk {
    let Some(StmtData::Sel(s)) = stmt_at_mut(result, statement) else {
        return ptr::null_mut();
    };
    let Some(chunk) = s.chunks.get(s.next_chunk).cloned() else {
        return ptr::null_mut();
    };
    s.next_chunk += 1;
    Box::into_raw(Box::new(PinkChunk::new(chunk)))
}

/// Free a chunk from [`pinkdb_result_fetch_chunk`]. NULL-safe.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_destroy_chunk(chunk: *mut PinkChunk) {
    if chunk.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_result_fetch_chunk`,
    // freed once by its owner.
    drop(unsafe { Box::from_raw(chunk) });
}

/// Fill a struct-typed Arrow schema for a SELECT result.
///
/// The schema has one child per result column and owns its format/name strings
/// and children; call its `release` callback when done. Returns 1 on success,
/// 0 on error (read the result's error for a failed result, otherwise
/// `pinkdb_last_error`).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_arrow_schema(
    result: *const PinkResult,
    statement: usize,
    out: *mut ArrowSchema,
) -> i32 {
    match arrow::result_schema(result, statement, out) {
        Ok(()) => 1,
        Err(error) => {
            error.report();
            0
        }
    }
}

/// Export one chunk as a struct-typed Arrow array.
///
/// The array owns an `Arc` of the chunk's vectors, so it stays valid after the
/// chunk and result are freed. Returns 1 on success, 0 on error (see
/// `pinkdb_last_error`).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_chunk_to_arrow(chunk: *const PinkChunk, out: *mut ArrowArray) -> i32 {
    if out.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null ArrowArray output");
        return 0;
    }
    // SAFETY: out is non-null and writable per the C contract.
    unsafe { ptr::write(out, ArrowArray::empty()) };
    if chunk.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null chunk");
        return 0;
    }
    // SAFETY: non-null, valid for the call.
    match arrow::chunk_to_arrow(&unsafe { &*chunk }._chunk, out) {
        Ok(()) => 1,
        Err(error) => {
            error.report();
            0
        }
    }
}

/// Fetch the next chunk of a SELECT result and export it as an Arrow array.
///
/// At the end of the result (and forever after) this leaves `out->release`
/// NULL and returns 1. Returns 0 on a null argument, a failed/non-SELECT
/// result, or an export error; the message is in the result's error for a
/// failed result, otherwise in `pinkdb_last_error`.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_fetch_arrow(
    result: *mut PinkResult,
    statement: usize,
    out: *mut ArrowArray,
) -> i32 {
    if out.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null ArrowArray output");
        return 0;
    }
    // SAFETY: out is non-null and writable per the C contract.
    unsafe { ptr::write(out, ArrowArray::empty()) };
    if result.is_null() {
        set_error(CLASS_INVALID_ARGUMENT, "null result");
        return 0;
    }
    // SAFETY: non-null, valid for the call.
    let result_ref = unsafe { &*result };
    if let Some(message) = &result_ref.error {
        set_error(
            result_ref.error_class.unwrap_or(CLASS_INTERNAL),
            &message.to_string_lossy(),
        );
        return 0;
    }
    let Some(StmtData::Sel(sel)) = stmt_at_mut(result, statement) else {
        set_error(
            CLASS_INVALID_ARGUMENT,
            &format!("statement {statement} is not a SELECT"),
        );
        return 0;
    };
    let Some(chunk) = sel.chunks.get(sel.next_chunk).cloned() else {
        return 1;
    };
    match arrow::chunk_to_arrow(&chunk, out) {
        Ok(()) => {
            sel.next_chunk += 1;
            1
        }
        Err(error) => {
            error.report();
            0
        }
    }
}

/// Rows in a chunk.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_chunk_size(chunk: *const PinkChunk) -> usize {
    if chunk.is_null() {
        return 0;
    }
    // SAFETY: non-null, valid for the call.
    unsafe { &*chunk }.columns.first().map_or(0, |c| c.len)
}

/// A vector borrowed from `chunk`; NULL when the column is out of range.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_chunk_column(chunk: *const PinkChunk, column: usize) -> *const PinkVector {
    if chunk.is_null() {
        return ptr::null();
    }
    // SAFETY: non-null, valid for the call; the vector lives as long as the
    // chunk.
    unsafe { &*chunk }
        .columns
        .get(column)
        .map_or(ptr::null(), |v| v as *const PinkVector)
}

/// The vector's typed payload (see the header for each type's layout), or
/// NULL for a type with no 1.2 payload (ARRAY).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_vector_data(vector: *const PinkVector) -> *const c_void {
    if vector.is_null() {
        return ptr::null();
    }
    // SAFETY: non-null, valid while its chunk lives.
    unsafe { &*vector }.data
}

/// The vector's validity words (bit i set = row i valid), or NULL when every
/// row is valid.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_vector_validity(vector: *const PinkVector) -> *const u64 {
    if vector.is_null() {
        return ptr::null();
    }
    // SAFETY: non-null, valid while its chunk lives.
    unsafe { &*vector }.validity
}

/// 1 when `row` is valid (or `validity` is NULL, meaning all rows are),
/// 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_validity_row_is_valid(validity: *const u64, row: usize) -> i32 {
    if validity.is_null() {
        return 1;
    }
    // SAFETY: the caller passes a mask covering `row`; bit i of word i/64.
    let word = unsafe { *validity.add(row / 64) };
    ((word >> (row % 64)) & 1) as i32
}

/// Affected-row count for INSERT/UPDATE/DELETE results (0 otherwise).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_affected(result: *const PinkResult, statement: usize) -> usize {
    match stmt_at(result, statement) {
        Some(StmtData::Simple { count, .. }) => *count,
        _ => 0,
    }
}

/// Object/session name for CREATE/DROP VIEW/TABLE/DATABASE and USE results
/// (NUL-terminated, owned by the result); NULL otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_name(result: *const PinkResult, statement: usize) -> *const c_char {
    match stmt_at(result, statement) {
        Some(StmtData::Simple { name: Some(n), .. }) => n.as_ptr() as *const c_char,
        _ => ptr::null(),
    }
}

// ----- per-statement query profiles (wave 84) -----

fn profile_at<'a>(result: *const PinkResult, statement: usize) -> Option<&'a ProfileData> {
    if result.is_null() {
        return None;
    }
    // SAFETY: non-null, owned by the caller, read-only access.
    let r = unsafe { &*result };
    r.profiles.get(statement)
}

/// 1 when the result carries a profile for `statement`, else 0 (profiling was
/// off for that execution, no profile exists, the result is NULL, or the
/// statement index is out of range). This is the status the numeric profile
/// accessors cannot express: they return 0 both for "no profile" and for a
/// real zero.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_available(
    result: *const PinkResult,
    statement: usize,
) -> i32 {
    i32::from(profile_at(result, statement).is_some())
}

/// Profile: parsing time in nanoseconds. 0 when
/// [`pinkdb_result_profile_available`] returns 0, or when a NULL result /
/// out-of-range index is passed.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_parse_ns(
    result: *const PinkResult,
    statement: usize,
) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.parse)
}

/// Profile: binding/planning time in nanoseconds (see
/// [`pinkdb_result_profile_parse_ns`] for the 0 conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_bind_ns(
    result: *const PinkResult,
    statement: usize,
) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.bind)
}

/// Profile: optimizer time in nanoseconds (see
/// [`pinkdb_result_profile_parse_ns`] for the 0 conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_optimize_ns(
    result: *const PinkResult,
    statement: usize,
) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.optimize)
}

/// Profile: execution time in nanoseconds (see
/// [`pinkdb_result_profile_parse_ns`] for the 0 conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_execute_ns(
    result: *const PinkResult,
    statement: usize,
) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.execute)
}

/// Profile: WAL logging/checkpoint time in nanoseconds (zero for reads and
/// in-memory stores; see [`pinkdb_result_profile_parse_ns`] for the 0
/// conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_log_ns(result: *const PinkResult, statement: usize) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.log)
}

/// Profile: total wall time in nanoseconds (see
/// [`pinkdb_result_profile_parse_ns`] for the 0 conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_total_ns(
    result: *const PinkResult,
    statement: usize,
) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.total)
}

/// Profile: result rows (or the affected count for INSERT/UPDATE/DELETE), 0
/// when there is no profile or the statement produces no rows (see
/// [`pinkdb_result_profile_parse_ns`] for the 0 conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_rows(result: *const PinkResult, statement: usize) -> u64 {
    profile_at(result, statement).map_or(0, |p| p.rows)
}

/// Profile: result columns (SELECT only), 0 when there is no profile or the
/// statement produces no result set (see
/// [`pinkdb_result_profile_parse_ns`] for the 0 conventions).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_columns(
    result: *const PinkResult,
    statement: usize,
) -> u32 {
    profile_at(result, statement).map_or(0, |p| p.columns)
}

/// Profile: statement kind as a NUL-terminated string (`"Selected"`,
/// `"Inserted"`, …), owned by the result and valid until it is freed; NULL
/// when there is no profile (profiling off, NULL result, or out-of-range
/// statement index).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_result_profile_kind(
    result: *const PinkResult,
    statement: usize,
) -> *const c_char {
    match profile_at(result, statement) {
        // SAFETY: the Vec bytes are heap-stable and NUL-terminated.
        Some(p) => p.kind.as_ptr() as *const c_char,
        None => ptr::null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec_ok(h: *mut PinkHandle, sql: &str) -> *mut PinkResult {
        let c = CString::new(sql).unwrap();
        let r = pinkdb_execute(h, c.as_ptr(), sql.len());
        assert!(!r.is_null(), "execute failed: {:?}", last_error_str());
        r
    }

    fn last_error_str() -> Option<String> {
        let p = pinkdb_last_error();
        if p.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
        }
    }

    fn text_of(v: &PinkValue) -> String {
        if v.text_ptr.is_null() {
            return String::new();
        }
        let bytes = unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, v.text_len) };
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn close(h: *mut PinkHandle) {
        pinkdb_close(h);
    }

    #[test]
    fn abi_version_is_one_three() {
        assert_eq!(pinkdb_abi_version(), (1 << 16) | 3);
    }

    #[test]
    fn execute_select_and_values() {
        let h = pinkdb_open_memory();
        assert!(!h.is_null());
        let r = exec_ok(
            h,
            "CREATE TABLE t (a INTEGER, b BIGINT, c DOUBLE, d VARCHAR, e DATE, f TIMESTAMP);
             INSERT INTO t VALUES (1, 2, 2.5, 'héllo', DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00');
             INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL, NULL);
             SELECT * FROM t ORDER BY a;",
        );
        assert_eq!(pinkdb_result_statement_count(r), 4);
        assert_eq!(pinkdb_result_kind(r, 0), PINKDB_STMT_CREATED);
        assert_eq!(pinkdb_result_kind(r, 1), PINKDB_STMT_INSERTED);
        assert_eq!(pinkdb_result_affected(r, 1), 1);
        assert_eq!(pinkdb_result_kind(r, 3), PINKDB_STMT_SELECTED);
        assert_eq!(pinkdb_result_column_count(r, 3), 6);
        assert_eq!(pinkdb_result_row_count(r, 3), 2);
        let name = unsafe { CStr::from_ptr(pinkdb_result_column_name(r, 3, 0)) };
        assert_eq!(name.to_str().unwrap(), "a");
        assert_eq!(pinkdb_result_column_type(r, 3, 0), PINKDB_TYPE_INTEGER);
        assert_eq!(pinkdb_result_column_type(r, 3, 3), PINKDB_TYPE_VARCHAR);

        let v = pinkdb_result_value(r, 3, 0, 0);
        assert_eq!(v.type_tag, PINKDB_TYPE_INTEGER);
        assert_eq!(v.is_null, 0);
        assert_eq!(v.int64, 1);
        let v = pinkdb_result_value(r, 3, 0, 1);
        assert_eq!(v.type_tag, PINKDB_TYPE_BIGINT);
        assert_eq!(v.int64, 2);
        let v = pinkdb_result_value(r, 3, 0, 2);
        assert_eq!(v.type_tag, PINKDB_TYPE_DOUBLE);
        assert!((v.double - 2.5).abs() < 1e-12);
        let v = pinkdb_result_value(r, 3, 0, 3);
        assert_eq!(v.type_tag, PINKDB_TYPE_VARCHAR);
        assert_eq!(text_of(&v), "héllo");
        let v = pinkdb_result_value(r, 3, 1, 0);
        assert_eq!(v.is_null, 1);
        assert_eq!(v.type_tag, PINKDB_TYPE_NULL);
        assert_eq!(pinkdb_result_kind(r, 99), -1);
        assert_eq!(pinkdb_result_row_count(r, 0), 0);
        assert_eq!(pinkdb_result_affected(r, 0), 0);
        assert!(pinkdb_result_name(r, 0).is_null());
        pinkdb_result_free(r);

        // DDL/DML name surface.
        let r = exec_ok(h, "CREATE VIEW v AS SELECT a FROM t; DROP VIEW v;");
        assert_eq!(pinkdb_result_kind(r, 0), PINKDB_STMT_CREATED_VIEW);
        let n = unsafe { CStr::from_ptr(pinkdb_result_name(r, 0)) };
        assert_eq!(n.to_str().unwrap(), "v");
        assert_eq!(pinkdb_result_kind(r, 1), PINKDB_STMT_DROPPED_VIEW);
        pinkdb_result_free(r);
        close(h);
    }

    /// Wave 101a: a VARIANT column reports `PINKDB_TYPE_VARIANT` and each cell
    /// carries its JSON text (the only exact spelling the C ABI has).
    #[test]
    fn variant_crosses_as_json_text() {
        let h = pinkdb_open_memory();
        let r = exec_ok(
            h,
            "SELECT CAST('{\"a\":[1,true,null]}' AS VARIANT) AS v,
                    CAST('null' AS VARIANT) AS n,
                    CAST(NULL AS VARIANT) AS m;",
        );
        assert_eq!(pinkdb_result_column_type(r, 0, 0), PINKDB_TYPE_VARIANT);
        let v = pinkdb_result_value(r, 0, 0, 0);
        assert_eq!((v.type_tag, v.is_null), (PINKDB_TYPE_VARIANT, 0));
        assert_eq!(text_of(&v), r#"{"a":[1,true,null]}"#);
        // A JSON null is a value, not a SQL NULL.
        let n = pinkdb_result_value(r, 0, 0, 1);
        assert_eq!((n.type_tag, n.is_null), (PINKDB_TYPE_VARIANT, 0));
        assert_eq!(text_of(&n), "null");
        let m = pinkdb_result_value(r, 0, 0, 2);
        assert_eq!((m.type_tag, m.is_null), (PINKDB_TYPE_NULL, 1));
        pinkdb_result_free(r);
        close(h);
    }

    #[test]
    fn multi_statement_kinds_and_use() {
        let h = pinkdb_open_memory();
        let r = exec_ok(
            h,
            "CREATE DATABASE db1; USE db1; BEGIN; COMMIT; BEGIN; ROLLBACK;",
        );
        let kinds: Vec<i32> = (0..6).map(|i| pinkdb_result_kind(r, i)).collect();
        assert_eq!(
            kinds,
            vec![
                PINKDB_STMT_CREATED_DATABASE,
                PINKDB_STMT_USED,
                PINKDB_STMT_BEGAN,
                PINKDB_STMT_COMMITTED,
                PINKDB_STMT_BEGAN,
                PINKDB_STMT_ROLLEDBACK
            ]
        );
        assert_eq!(pinkdb_result_kind(r, 6), -1, "out of range");
        pinkdb_result_free(r);
        close(h);
    }

    #[test]
    fn schema_statement_kinds_round_trip() {
        let h = pinkdb_open_memory();
        let r = exec_ok(h, "CREATE SCHEMA s; DROP SCHEMA s;");
        assert_eq!(pinkdb_result_kind(r, 0), PINKDB_STMT_CREATED_SCHEMA);
        assert_eq!(pinkdb_result_kind(r, 1), PINKDB_STMT_DROPPED_SCHEMA);
        for i in 0..2 {
            let name = pinkdb_result_name(r, i);
            assert!(!name.is_null());
            assert_eq!(
                unsafe { std::ffi::CStr::from_ptr(name) }.to_str().unwrap(),
                "s"
            );
        }
        pinkdb_result_free(r);
        close(h);
    }

    #[test]
    fn errors_round_trip_and_clear() {
        let h = pinkdb_open_memory();
        let sql = CString::new("SELECT * FROM missing;").unwrap();
        let r = pinkdb_execute(h, sql.as_ptr(), sql.as_bytes().len());
        assert!(r.is_null());
        let msg = last_error_str().unwrap();
        assert!(msg.contains("not found"), "unexpected: {msg}");
        // A successful call clears the thread-local error.
        let r = exec_ok(h, "SELECT 1;");
        assert!(pinkdb_last_error().is_null());
        pinkdb_result_free(r);
        close(h);
    }

    #[test]
    fn null_and_invalid_inputs_are_guarded() {
        assert!(pinkdb_execute(ptr::null_mut(), ptr::null(), 0).is_null());
        assert!(last_error_str().unwrap().contains("null handle"));
        assert!(pinkdb_open(ptr::null()).is_null());
        let h = pinkdb_open_memory();
        let bad = [0xffu8, 0xfe];
        let r = pinkdb_execute(h, bad.as_ptr() as *const c_char, bad.len());
        assert!(r.is_null());
        assert!(last_error_str().unwrap().contains("UTF-8"));
        // NULL handle guards on readers.
        assert_eq!(pinkdb_result_statement_count(ptr::null()), 0);
        assert_eq!(pinkdb_result_kind(ptr::null(), 0), -1);
        pinkdb_close(ptr::null_mut());
        close(h);
    }

    #[test]
    fn thread_local_errors_are_isolated() {
        let child = std::thread::spawn(|| {
            let h = pinkdb_open_memory();
            let sql = CString::new("SELECT * FROM missing;").unwrap();
            let r = pinkdb_execute(h, sql.as_ptr(), sql.as_bytes().len());
            assert!(r.is_null());
            let msg = {
                let p = pinkdb_last_error();
                assert!(!p.is_null());
                unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
            };
            pinkdb_close(h);
            msg
        });
        let msg = child.join().unwrap();
        assert!(msg.contains("not found"), "unexpected: {msg}");
        // The child's error never leaks into this thread.
        assert!(pinkdb_last_error().is_null());
    }

    #[test]
    fn free_loops_are_stable() {
        let h = pinkdb_open_memory();
        for _ in 0..100 {
            let r = exec_ok(h, "SELECT 1 + 1;");
            assert_eq!(pinkdb_result_value(r, 0, 0, 0).int64, 2);
            pinkdb_result_free(r);
        }
        close(h);
    }

    /// One `pink_value` cell for a bound parameter.
    fn cell(tag: i32, is_null: bool, i: i64, d: f64, text: Option<&[u8]>) -> (PinkValue, Vec<u8>) {
        let mut owned = Vec::new();
        let ptr = match text {
            Some(bytes) => {
                owned.extend_from_slice(bytes);
                owned.push(0);
                owned.as_ptr() as *const c_char
            }
            None => ptr::null(),
        };
        let len = owned.len().saturating_sub(1);
        (
            PinkValue {
                type_tag: tag,
                is_null: i32::from(is_null),
                int64: i,
                double: d,
                text_ptr: ptr,
                text_len: len,
            },
            owned,
        )
    }

    #[test]
    fn prepared_round_trip_through_the_c_abi() {
        let h = pinkdb_open_memory();
        let sql = CString::new("SELECT CAST(? AS INTEGER) + 1, ?;").unwrap();
        let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
        assert!(!prep.is_null(), "{:?}", last_error_str());
        assert_eq!(pinkdb_prepared_parameter_count(prep), 2);
        assert_eq!(pinkdb_prepared_parameter_count(ptr::null()), 0);

        let (first, _keep1) = cell(PINKDB_TYPE_VARCHAR, false, 0, 0.0, Some(b"41"));
        let (second, _keep2) = cell(PINKDB_TYPE_VARCHAR, false, 0, 0.0, Some("héllo".as_bytes()));
        let values = [first, second];
        let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 2);
        assert!(!r.is_null(), "{:?}", last_error_str());
        assert_eq!(pinkdb_result_kind(r, 0), PINKDB_STMT_SELECTED);
        let v = pinkdb_result_value(r, 0, 0, 0);
        assert_eq!((v.type_tag, v.int64), (PINKDB_TYPE_INTEGER, 42));
        let v = pinkdb_result_value(r, 0, 0, 1);
        assert_eq!(v.type_tag, PINKDB_TYPE_VARCHAR);
        assert_eq!(text_of(&v), "héllo");
        pinkdb_result_free(r);

        // Wrong arity is a clean error, and the prepared statement survives.
        let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 1);
        assert!(r.is_null());
        assert!(
            last_error_str()
                .unwrap()
                .contains("values were not provided for prepared statement parameters: 2"),
            "{:?}",
            last_error_str()
        );
        let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 2);
        assert!(!r.is_null(), "{:?}", last_error_str());
        pinkdb_result_free(r);

        pinkdb_prepared_free(prep);
        // NULL guards on the prepared accessors.
        assert!(pinkdb_prepare(ptr::null_mut(), sql.as_ptr(), sql.as_bytes().len()).is_null());
        pinkdb_prepared_free(ptr::null_mut());
        assert!(pinkdb_execute_prepared(h, ptr::null(), ptr::null(), 0).is_null());
        close(h);
    }

    #[test]
    fn prepared_bind_errors_and_mutations() {
        let h = pinkdb_open_memory();
        exec_ok(h, "CREATE TABLE t (i INTEGER, s VARCHAR);");
        let sql = CString::new("INSERT INTO t VALUES (?, ?);").unwrap();
        let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
        assert!(!prep.is_null(), "{:?}", last_error_str());
        let (i, _k1) = cell(PINKDB_TYPE_INTEGER, false, 7, 0.0, None);
        let (s, _k2) = cell(PINKDB_TYPE_VARCHAR, false, 0, 0.0, Some(b"seven"));
        let values = [i, s];
        let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 2);
        assert!(!r.is_null(), "{:?}", last_error_str());
        assert_eq!(pinkdb_result_kind(r, 0), PINKDB_STMT_INSERTED);
        assert_eq!(pinkdb_result_affected(r, 0), 1);
        pinkdb_result_free(r);

        // A non-numeric string for the INTEGER column fails at bind.
        let (bad, _k3) = cell(PINKDB_TYPE_VARCHAR, false, 0, 0.0, Some(b"nope"));
        let values = [bad, s];
        let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 2);
        assert!(r.is_null());
        assert!(
            last_error_str()
                .unwrap()
                .contains("cannot bind parameter 1"),
            "{:?}",
            last_error_str()
        );

        // An out-of-range INTEGER payload is refused, not truncated.
        let (huge, _k4) = cell(PINKDB_TYPE_INTEGER, false, i64::MAX, 0.0, None);
        let r = pinkdb_execute_prepared(h, prep, [huge, s].as_ptr(), 2);
        assert!(r.is_null());
        assert!(last_error_str().unwrap().contains("does not fit in i32"));

        // An unknown type tag is refused.
        let (unknown, _k5) = cell(99, false, 0, 0.0, None);
        let r = pinkdb_execute_prepared(h, prep, [unknown, s].as_ptr(), 2);
        assert!(r.is_null());
        assert!(
            last_error_str()
                .unwrap()
                .contains("unknown value type tag 99")
        );

        // NULL binds through the target column's type.
        let (null, _k6) = cell(PINKDB_TYPE_NULL, true, 0, 0.0, None);
        let r = pinkdb_execute_prepared(h, prep, [null, s].as_ptr(), 2);
        assert!(!r.is_null(), "{:?}", last_error_str());
        pinkdb_result_free(r);

        // A durable-free read-back shows both rows.
        let r = exec_ok(h, "SELECT r.i FROM t r ORDER BY r.i;");
        assert_eq!(pinkdb_result_row_count(r, 0), 2);
        pinkdb_result_free(r);
        pinkdb_prepared_free(prep);
        close(h);
    }

    #[test]
    fn hugeint_cells_carry_decimal_text() {
        let values = [Value::Hugeint(i128::MIN), Value::Uhugeint(u128::MAX)];
        for v in values {
            let cell = Cell::from_value(&v).expect("128-bit cell");
            let text = cell.text.as_deref().expect("text cell");
            assert_eq!(
                std::str::from_utf8(&text[..text.len() - 1]).unwrap(),
                v.to_string()
            );
            assert!(text.ends_with(&[0u8]));
        }
        assert_eq!(dtype_tag(DataType::Hugeint), PINKDB_TYPE_HUGEINT);
        assert_eq!(dtype_tag(DataType::Uhugeint), PINKDB_TYPE_UHUGEINT);
    }

    #[test]
    fn prepared_multi_statement_refused() {
        let h = pinkdb_open_memory();
        let sql = CString::new("SELECT 1; SELECT 2;").unwrap();
        assert!(pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len()).is_null());
        assert!(last_error_str().unwrap().contains("exactly one statement"));
        close(h);
    }
}
