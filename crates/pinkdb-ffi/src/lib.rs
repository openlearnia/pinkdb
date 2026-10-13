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

// ----- value / column type tags -----
pub const PINKDB_TYPE_NULL: i32 = 0;
pub const PINKDB_TYPE_BOOLEAN: i32 = 1;
pub const PINKDB_TYPE_INTEGER: i32 = 2;
pub const PINKDB_TYPE_BIGINT: i32 = 3;
pub const PINKDB_TYPE_DOUBLE: i32 = 4;
pub const PINKDB_TYPE_VARCHAR: i32 = 5;
pub const PINKDB_TYPE_DATE: i32 = 6;
pub const PINKDB_TYPE_TIMESTAMP: i32 = 7;

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
            Value::Null => Cell { tag: PINKDB_TYPE_NULL, is_null: true, i: 0, d: 0.0, text: None },
            Value::Boolean(b) => Cell { tag: PINKDB_TYPE_BOOLEAN, is_null: false, i: *b as i64, d: 0.0, text: None },
            Value::Integer(i) => Cell { tag: PINKDB_TYPE_INTEGER, is_null: false, i: *i as i64, d: 0.0, text: None },
            Value::Bigint(i) => Cell { tag: PINKDB_TYPE_BIGINT, is_null: false, i: *i, d: 0.0, text: None },
            Value::Double(x) => Cell { tag: PINKDB_TYPE_DOUBLE, is_null: false, i: 0, d: *x, text: None },
            Value::Varchar(s) => {
                let mut b = s.as_bytes().to_vec();
                b.push(0);
                Cell { tag: PINKDB_TYPE_VARCHAR, is_null: false, i: 0, d: 0.0, text: Some(b) }
            }
            Value::Date(d) => Cell { tag: PINKDB_TYPE_DATE, is_null: false, i: *d as i64, d: 0.0, text: None },
            Value::Timestamp(us) => Cell { tag: PINKDB_TYPE_TIMESTAMP, is_null: false, i: *us, d: 0.0, text: None },
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
    Simple { kind: i32, count: usize, name: Option<Vec<u8>> },
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
                    | QueryResult::DroppedDatabase { name } => (0, Some(nul_terminated(name))),
                    QueryResult::Used { db } => (0, Some(nul_terminated(db))),
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
    Box::into_raw(Box::new(PinkHandle { db: PinkDb::new() }))
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
    if sql.is_null() && sql_len > 0 {
        set_error("null sql pointer with non-zero length");
        return ptr::null_mut();
    }
    let bytes: &[u8] = if sql_len == 0 {
        &[]
    } else {
        // SAFETY: non-null (checked above) and valid for `sql_len` bytes per
        // the API contract.
        unsafe { std::slice::from_raw_parts(sql as *const u8, sql_len) }
    };
    let text = match std::str::from_utf8(bytes) {
        Ok(t) => t,
        Err(_) => {
            set_error("SQL is not valid UTF-8");
            return ptr::null_mut();
        }
    };
    clear_error();
    // SAFETY: non-null (checked) and uniquely owned per the single-handle
    // contract; no other reference exists during the call.
    let h = unsafe { &mut *handle };
    match catch_unwind(AssertUnwindSafe(|| h.db.execute(text))) {
        Ok(Ok(results)) => Box::into_raw(Box::new(build_result(results))),
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
pub extern "C" fn pinkdb_result_column_count(
    result: *const PinkResult,
    statement: usize,
) -> usize {
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
pub extern "C" fn pinkdb_result_name(
    result: *const PinkResult,
    statement: usize,
) -> *const c_char {
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
        let r = exec_ok(h, "CREATE DATABASE db1; USE db1; BEGIN; COMMIT; BEGIN; ROLLBACK;");
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
}
