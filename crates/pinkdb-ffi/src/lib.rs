//! pinkdb C ABI v1 — `pinkdb-ffi`.
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

// Raw-pointer arguments are NULL-checked before every dereference and the
// pointer-taking signatures are dictated by the C ABI, so the lint that wants
// `unsafe fn` here does not apply to this crate.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use pinkdb::{DataType, PinkDb, QueryResult, Value};

/// ABI version reported by [`pinkdb_abi_version`].
pub const PINKDB_ABI_VERSION: u32 = 1;

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

// ----- thread-local error -----

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(msg: &str) {
    // Interior NULs cannot live in a CString; escape them for display.
    let sanitized = msg.replace('\0', "\\0");
    LAST_ERROR.with(|e| *e.borrow_mut() = CString::new(sanitized).ok());
}

fn clear_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

/// Last error on this thread (NULL when none). The pointer stays valid until
/// the next FFI call on this thread.
#[no_mangle]
pub extern "C" fn pinkdb_last_error() -> *const c_char {
    LAST_ERROR.with(|e| match &*e.borrow() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    })
}

/// ABI version of this library.
#[no_mangle]
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
    fn from_value(v: &Value) -> Cell {
        match v {
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
            Value::Decimal(x, scale) => {
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
        }
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

fn dtype_tag(t: DataType) -> i32 {
    match t {
        DataType::Boolean => PINKDB_TYPE_BOOLEAN,
        DataType::Integer => PINKDB_TYPE_INTEGER,
        DataType::Bigint => PINKDB_TYPE_BIGINT,
        DataType::Double => PINKDB_TYPE_DOUBLE,
        DataType::Varchar => PINKDB_TYPE_VARCHAR,
        DataType::Date => PINKDB_TYPE_DATE,
        DataType::Timestamp => PINKDB_TYPE_TIMESTAMP,
        DataType::Decimal { .. } => PINKDB_TYPE_DECIMAL,
        DataType::Interval => PINKDB_TYPE_INTERVAL,
    }
}

fn nul_terminated(s: &str) -> Vec<u8> {
    let mut b = s.as_bytes().to_vec();
    b.push(0);
    b
}

struct SelStmt {
    names: Vec<Vec<u8>>,
    types: Vec<i32>,
    rows: Vec<Vec<Cell>>,
}

enum StmtData {
    Sel(SelStmt),
    Simple {
        kind: i32,
        count: usize,
        name: Option<Vec<u8>>,
    },
}

/// Owns one `pinkdb_execute` outcome. Every pointer handed out by the
/// `pinkdb_result_*` accessors borrows from here.
pub struct PinkResult {
    stmts: Vec<StmtData>,
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
        QueryResult::LoadedExtension { .. } => PINKDB_STMT_LOADED_EXTENSION,
        QueryResult::Checkpointed => PINKDB_STMT_CHECKPOINTED,
        QueryResult::AlteredTable { .. } => PINKDB_STMT_ALTERED_TABLE,
    }
}

fn build_result(results: Vec<QueryResult>) -> PinkResult {
    let stmts = results
        .into_iter()
        .map(|r| match r {
            QueryResult::Selected { names, types, rows } => StmtData::Sel(SelStmt {
                names: names.iter().map(|n| nul_terminated(n)).collect(),
                types: types.into_iter().map(dtype_tag).collect(),
                rows: rows
                    .iter()
                    .map(|row| row.iter().map(Cell::from_value).collect())
                    .collect(),
            }),
            other => {
                let kind = kind_of(&other);
                let (count, name) = match &other {
                    QueryResult::Inserted { count }
                    | QueryResult::Updated { count }
                    | QueryResult::Deleted { count } => (*count, None),
                    QueryResult::CreatedView { name }
                    | QueryResult::DroppedTable { name }
                    | QueryResult::DroppedView { name }
                    | QueryResult::CreatedDatabase { name }
                    | QueryResult::DroppedDatabase { name }
                    | QueryResult::AlteredTable { name } => (0, Some(nul_terminated(name))),
                    QueryResult::Used { db } => (0, Some(nul_terminated(db))),
                    QueryResult::LoadedExtension { name } => (0, Some(nul_terminated(name))),
                    _ => (0, None),
                };
                StmtData::Simple { kind, count, name }
            }
        })
        .collect();
    PinkResult { stmts }
}

// ----- handles -----

/// Opaque database handle. Not thread-safe: use one handle per thread.
pub struct PinkHandle {
    db: PinkDb,
}

fn c_path(path: *const c_char) -> Option<String> {
    if path.is_null() {
        set_error("null path");
        return None;
    }
    // SAFETY: non-null and required by the API contract to be a NUL-terminated
    // C string. The pointer need not be aligned; CStr handles bytes.
    match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => Some(s.to_string()),
        Err(_) => {
            set_error("path is not valid UTF-8");
            None
        }
    }
}

/// Open a durable database (checkpoint + WAL) at `path` (UTF-8, NUL-terminated
/// C string). Returns NULL on error; see [`pinkdb_last_error`].
#[no_mangle]
pub extern "C" fn pinkdb_open(path: *const c_char) -> *mut PinkHandle {
    let Some(path) = c_path(path) else {
        return ptr::null_mut();
    };
    clear_error();
    match catch_unwind(AssertUnwindSafe(|| PinkDb::open_durable(&path))) {
        Ok(Ok(db)) => Box::into_raw(Box::new(PinkHandle { db })),
        Ok(Err(e)) => {
            set_error(&e.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            set_error("internal error: panicked while opening the database");
            ptr::null_mut()
        }
    }
}

/// Open a fresh in-memory database.
#[no_mangle]
pub extern "C" fn pinkdb_open_memory() -> *mut PinkHandle {
    clear_error();
    // A panic must never unwind across the extern "C" boundary.
    match catch_unwind(AssertUnwindSafe(|| {
        Box::new(PinkHandle { db: PinkDb::new() })
    })) {
        Ok(handle) => Box::into_raw(handle),
        Err(_) => {
            set_error("internal error: panicked while opening the database");
            ptr::null_mut()
        }
    }
}

/// Close a handle opened by [`pinkdb_open`]/[`pinkdb_open_memory`]. NULL-safe.
#[no_mangle]
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
#[no_mangle]
pub extern "C" fn pinkdb_execute(
    handle: *mut PinkHandle,
    sql: *const c_char,
    sql_len: usize,
) -> *mut PinkResult {
    if handle.is_null() {
        set_error("null handle");
        return ptr::null_mut();
    }
    // SAFETY: the caller guarantees (ptr, len) validity for the call.
    let Some(text) = (unsafe { sql_utf8(sql, sql_len) }) else {
        return ptr::null_mut();
    };
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract; no other reference exists during the call.
    let h = unsafe { &mut *handle };
    match catch_unwind(AssertUnwindSafe(|| h.db.execute(text))) {
        Ok(Ok(results)) => match catch_unwind(AssertUnwindSafe(|| Box::new(build_result(results))))
        {
            Ok(result) => Box::into_raw(result),
            Err(_) => {
                set_error("internal error: building the result panicked");
                ptr::null_mut()
            }
        },
        Ok(Err(e)) => {
            set_error(&e.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            set_error("internal error: statement panicked");
            ptr::null_mut()
        }
    }
}

/// Read an `(ptr, len)` SQL argument as UTF-8, setting the thread-local error
/// (and returning `None`) on a NULL pointer with a non-zero length or invalid
/// UTF-8.
///
/// # Safety
///
/// `sql` must be non-NULL whenever `sql_len > 0` and valid for `sql_len`
/// bytes for the duration of the returned borrow (the API contract for every
/// `(ptr, len)` entry point of this crate).
unsafe fn sql_utf8<'a>(sql: *const c_char, sql_len: usize) -> Option<&'a str> {
    if sql.is_null() && sql_len > 0 {
        set_error("null sql pointer with non-zero length");
        return None;
    }
    let bytes: &[u8] = if sql_len == 0 {
        &[]
    } else {
        // SAFETY: non-null (checked above) and valid for `sql_len` bytes per
        // the API contract.
        std::slice::from_raw_parts(sql as *const u8, sql_len)
    };
    match std::str::from_utf8(bytes) {
        Ok(t) => Some(t),
        Err(_) => {
            set_error("SQL is not valid UTF-8");
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
/// way `PinkDb::prepare` is. Free with [`pinkdb_prepared_free`].
#[no_mangle]
pub extern "C" fn pinkdb_prepare(
    handle: *mut PinkHandle,
    sql: *const c_char,
    sql_len: usize,
) -> *mut PinkPrepared {
    if handle.is_null() {
        set_error("null handle");
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
    match catch_unwind(AssertUnwindSafe(|| h.db.prepare(text))) {
        Ok(Ok(stmt)) => Box::into_raw(Box::new(PinkPrepared { stmt })),
        Ok(Err(e)) => {
            set_error(&e.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            set_error("internal error: prepare panicked");
            ptr::null_mut()
        }
    }
}

/// Number of `?` placeholders in a prepared statement: the exact number of
/// values [`pinkdb_execute_prepared`] takes. 0 for a NULL handle.
#[no_mangle]
pub extern "C" fn pinkdb_prepared_parameter_count(prepared: *const PinkPrepared) -> usize {
    if prepared.is_null() {
        return 0;
    }
    // SAFETY: non-null, owned by the caller, read-only access.
    unsafe { &*prepared }.stmt.parameter_count()
}

/// Free a prepared statement. NULL-safe.
#[no_mangle]
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
#[no_mangle]
pub extern "C" fn pinkdb_execute_prepared(
    handle: *mut PinkHandle,
    prepared: *const PinkPrepared,
    values: *const PinkValue,
    value_count: usize,
) -> *mut PinkResult {
    if handle.is_null() {
        set_error("null handle");
        return ptr::null_mut();
    }
    if prepared.is_null() {
        set_error("null prepared statement");
        return ptr::null_mut();
    }
    if values.is_null() && value_count > 0 {
        set_error("null values pointer with non-zero count");
        return ptr::null_mut();
    }
    let mut bound: Vec<pinkdb::Value> = Vec::with_capacity(value_count);
    for i in 0..value_count {
        // SAFETY: non-null and valid for `value_count` entries per the API
        // contract.
        let raw = unsafe { &*values.add(i) };
        match value_from_pink(raw) {
            Ok(v) => bound.push(v),
            Err(msg) => {
                set_error(&format!("value {i}: {msg}"));
                return ptr::null_mut();
            }
        }
    }
    clear_error();
    // SAFETY: both non-null (checked) and uniquely owned per the
    // single-handle contract.
    let h = unsafe { &mut *handle };
    let stmt = unsafe { &*prepared };
    match catch_unwind(AssertUnwindSafe(|| {
        h.db.execute_prepared(&stmt.stmt, &bound)
    })) {
        Ok(Ok(result)) => {
            match catch_unwind(AssertUnwindSafe(|| Box::new(build_result(vec![result])))) {
                Ok(result) => Box::into_raw(result),
                Err(_) => {
                    set_error("internal error: building the result panicked");
                    ptr::null_mut()
                }
            }
        }
        Ok(Err(e)) => {
            set_error(&e.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            set_error("internal error: statement panicked");
            ptr::null_mut()
        }
    }
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
        // Results carry DECIMAL/INTERVAL as text cells; binding a *parameter*
        // back is not supported in ABI v1 (cast the text in SQL).
        PINKDB_TYPE_DECIMAL | PINKDB_TYPE_INTERVAL => {
            return Err(
                "DECIMAL/INTERVAL cannot be bound as parameters in ABI v1; pass the canonical \
                 text and CAST it in SQL"
                    .to_string(),
            )
        }
        other => return Err(format!("unknown value type tag {other}")),
    })
}

/// Free a result returned by [`pinkdb_execute`]. NULL-safe.
#[no_mangle]
pub extern "C" fn pinkdb_result_free(result: *mut PinkResult) {
    if result.is_null() {
        return;
    }
    // SAFETY: produced by Box::into_raw in `pinkdb_execute`, freed once.
    drop(unsafe { Box::from_raw(result) });
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
#[no_mangle]
pub extern "C" fn pinkdb_result_statement_count(result: *const PinkResult) -> usize {
    if result.is_null() {
        return 0;
    }
    // SAFETY: non-null, valid for the call.
    unsafe { &*result }.stmts.len()
}

/// Statement kind tag (e.g. [`PINKDB_STMT_SELECTED`]); -1 when out of range.
#[no_mangle]
pub extern "C" fn pinkdb_result_kind(result: *const PinkResult, statement: usize) -> i32 {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(_)) => PINKDB_STMT_SELECTED,
        Some(StmtData::Simple { kind, .. }) => *kind,
        None => -1,
    }
}

/// For SELECT results: number of columns (0 for other kinds / out of range).
#[no_mangle]
pub extern "C" fn pinkdb_result_column_count(result: *const PinkResult, statement: usize) -> usize {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s.names.len(),
        _ => 0,
    }
}

/// Column name (NUL-terminated, owned by the result) or NULL when out of range.
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
pub extern "C" fn pinkdb_result_row_count(result: *const PinkResult, statement: usize) -> usize {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s.rows.len(),
        _ => 0,
    }
}

/// Value at (statement, row, column) for a SELECT result. Out-of-range and
/// non-SELECT statements return a NULL `PinkValue`.
#[no_mangle]
pub extern "C" fn pinkdb_result_value(
    result: *const PinkResult,
    statement: usize,
    row: usize,
    column: usize,
) -> PinkValue {
    match stmt_at(result, statement) {
        Some(StmtData::Sel(s)) => s
            .rows
            .get(row)
            .and_then(|r| r.get(column))
            .map(Cell::value)
            .unwrap_or_else(PinkValue::null),
        _ => PinkValue::null(),
    }
}

/// Affected-row count for INSERT/UPDATE/DELETE results (0 otherwise).
#[no_mangle]
pub extern "C" fn pinkdb_result_affected(result: *const PinkResult, statement: usize) -> usize {
    match stmt_at(result, statement) {
        Some(StmtData::Simple { count, .. }) => *count,
        _ => 0,
    }
}

/// Object/session name for CREATE/DROP VIEW/TABLE/DATABASE and USE results
/// (NUL-terminated, owned by the result); NULL otherwise.
#[no_mangle]
pub extern "C" fn pinkdb_result_name(result: *const PinkResult, statement: usize) -> *const c_char {
    match stmt_at(result, statement) {
        Some(StmtData::Simple { name: Some(n), .. }) => n.as_ptr() as *const c_char,
        _ => ptr::null(),
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
    fn abi_version_is_one() {
        assert_eq!(pinkdb_abi_version(), 1);
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
        assert!(last_error_str()
            .unwrap()
            .contains("unknown value type tag 99"));

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
    fn prepared_multi_statement_refused() {
        let h = pinkdb_open_memory();
        let sql = CString::new("SELECT 1; SELECT 2;").unwrap();
        assert!(pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len()).is_null());
        assert!(last_error_str().unwrap().contains("exactly one statement"));
        close(h);
    }
}
