//! pinkdb browser ABI — `pinkdb-wasm`.
//!
//! A `cdylib` for `wasm32-unknown-unknown` with a tiny raw C ABI and no
//! wasm-bindgen. This crate is the wasm counterpart of `pinkdb-ffi`: the only
//! `unsafe` is pointer/length marshaling, contained here with NULL checks and
//! `SAFETY` comments; the `pinkdb` engine stays zero-unsafe.
//!
//! # Buffer contract
//!
//! All strings are UTF-8 byte ranges in wasm linear memory.
//!
//! - [`pw_alloc`] hands the caller `len` zeroed bytes to fill and pass into
//!   [`pw_exec`] / [`pw_restore`]; release it with `pw_free(ptr, len)`.
//! - Every buffer this module returns is `[u32 LE payload length][payload]`
//!   and is released with `pw_free(ptr, 4 + length)`.
//! - [`pw_snapshot`] returns `[u32 LE length][.pink bytes]`, or on error
//!   `[u32 0xFFFFFFFF][u32 length][UTF-8 message]` (release with
//!   `pw_free(ptr, 8 + length)`). The `0xFFFFFFFF` marker cannot collide
//!   with a real snapshot length on wasm32 (a >4 GiB image cannot exist).
//!
//! Handles are thread-local: one handle is valid only on the thread that
//! opened it (the JS glue always calls from one thread). A panic in any
//! export is caught and reported as `{"ok":false,"error":"internal error:
//! ..."}` (or `0` / a NULL pointer for the non-JSON returns); it never
//! unwinds across the ABI boundary.
//!
//! # JSON
//!
//! Results cross as hand-written JSON (no serde): `NULL` is `null`,
//! BOOLEAN is `true`/`false`, INTEGER is a number, BIGINT is a number only
//! when the exact value fits a JS safe integer (`|x| <= 2^53 - 1`) and a
//! string otherwise, DOUBLE is a number when finite and one of the strings
//! `"NaN"` / `"Infinity"` / `"-Infinity"` otherwise, VARCHAR is a string,
//! and DATE/TIMESTAMP/DECIMAL/INTERVAL cross as their canonical Display text.
//! Column `types` are the `DataType` Display strings.

// Raw-pointer arguments are NULL-checked before every dereference and the
// pointer-taking signatures are dictated by the ABI, so the lint that wants
// `unsafe fn` here does not apply (the same carve-out as `pinkdb-ffi`).
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use pinkdb::{PinkDb, QueryResult, Value};

/// The version [`pw_version`] reports: the engine and the glue crate ship
/// together, so this is the pinkdb package version.
pub const PINKDB_WASM_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Largest integer a JS number represents exactly (2^53 - 1).
const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

// ----- handles -----

struct State {
    /// Next handle to try; never 0 (0 means "open failed").
    next_handle: u32,
    handles: HashMap<u32, PinkDb>,
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State {
        next_handle: 1,
        handles: HashMap::new(),
    });
}

/// Run `f` on the database behind `handle`, or return `None` when there is no
/// such handle on this thread.
fn with_db<R>(handle: u32, f: impl FnOnce(&mut PinkDb) -> R) -> Option<R> {
    STATE.with(|state| state.borrow_mut().handles.get_mut(&handle).map(f))
}

// ----- JSON rendering -----

/// Append `s` as a JSON string literal, escaping quotes, backslashes and
/// every control character (including the ones with short escapes).
fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                // Writing to a String cannot fail.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Append one value with the ABI's JSON mapping (see the module docs).
fn push_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Integer(i) => {
            let _ = write!(out, "{i}");
        }
        Value::Bigint(i) => {
            if i.unsigned_abs() <= JS_MAX_SAFE_INTEGER {
                let _ = write!(out, "{i}");
            } else {
                // Beyond the exact range of a JS number: cross as text so no
                // consumer can silently round it.
                push_json_string(out, &i.to_string());
            }
        }
        Value::Double(x) => {
            if x.is_finite() {
                // Rust's shortest round-trip Display, always valid JSON.
                let _ = write!(out, "{x}");
            } else if x.is_nan() {
                push_json_string(out, "NaN");
            } else if x.is_sign_positive() {
                push_json_string(out, "Infinity");
            } else {
                push_json_string(out, "-Infinity");
            }
        }
        Value::Varchar(s) => push_json_string(out, s),
        // Canonical DuckDB-style display text.
        Value::Date(_) | Value::Timestamp(_) | Value::Decimal(..) | Value::Interval { .. } => {
            push_json_string(out, &v.to_string());
        }
    }
}

/// The `QueryResult` variant name (`"Selected"`, `"Inserted"`, …).
fn result_kind(r: &QueryResult) -> &'static str {
    match r {
        QueryResult::Created => "Created",
        QueryResult::Inserted { .. } => "Inserted",
        QueryResult::Updated { .. } => "Updated",
        QueryResult::Deleted { .. } => "Deleted",
        QueryResult::Began => "Began",
        QueryResult::Committed => "Committed",
        QueryResult::RolledBack => "RolledBack",
        QueryResult::CreatedView { .. } => "CreatedView",
        QueryResult::DroppedTable { .. } => "DroppedTable",
        QueryResult::CreatedIndex { .. } => "CreatedIndex",
        QueryResult::DroppedIndex { .. } => "DroppedIndex",
        QueryResult::CreatedMaterializedView { .. } => "CreatedMaterializedView",
        QueryResult::RefreshedMaterializedView { .. } => "RefreshedMaterializedView",
        QueryResult::RefreshedMaterializedViews { .. } => "RefreshedMaterializedViews",
        QueryResult::DroppedMaterializedView { .. } => "DroppedMaterializedView",
        QueryResult::DroppedView { .. } => "DroppedView",
        QueryResult::CreatedDatabase { .. } => "CreatedDatabase",
        QueryResult::DroppedDatabase { .. } => "DroppedDatabase",
        QueryResult::Used { .. } => "Used",
        QueryResult::LoadedExtension { .. } => "LoadedExtension",
        QueryResult::AlteredTable { .. } => "AlteredTable",
        QueryResult::Checkpointed => "Checkpointed",
        QueryResult::CreatedProcedure { .. } => "CreatedProcedure",
        QueryResult::DroppedProcedure { .. } => "DroppedProcedure",
        QueryResult::CreatedMacro { .. } => "CreatedMacro",
        QueryResult::DroppedMacro { .. } => "DroppedMacro",
        QueryResult::Called { .. } => "Called",
        QueryResult::CreatedUser { .. } => "CreatedUser",
        QueryResult::AlteredUser { .. } => "AlteredUser",
        QueryResult::DroppedUser { .. } => "DroppedUser",
        QueryResult::CreatedRole { .. } => "CreatedRole",
        QueryResult::DroppedRole { .. } => "DroppedRole",
        QueryResult::Granted { .. } => "Granted",
        QueryResult::Revoked { .. } => "Revoked",
        QueryResult::Selected { .. } => "Selected",
    }
}

/// The CLI's one-line text for a statement kind (mirrors
/// `pinkdb-cli`'s renderer); the kind name itself when there is none.
fn result_message(r: &QueryResult) -> String {
    match r {
        QueryResult::Created => "Table created.".to_string(),
        QueryResult::Began => "Transaction started.".to_string(),
        QueryResult::Committed => "Transaction committed.".to_string(),
        QueryResult::RolledBack => "Transaction rolled back.".to_string(),
        QueryResult::CreatedView { name } => format!("View {name} created."),
        QueryResult::DroppedTable { name } => format!("Table {name} dropped."),
        QueryResult::CreatedIndex { name } => format!("Index {name} created."),
        QueryResult::DroppedIndex { name } => format!("Index {name} dropped."),
        QueryResult::CreatedMaterializedView { name } => {
            format!("Materialized view {name} created.")
        }
        QueryResult::RefreshedMaterializedView { name } => {
            format!("Materialized view {name} refreshed.")
        }
        QueryResult::RefreshedMaterializedViews { names } => {
            format!("Refreshed {} materialized view(s).", names.len())
        }
        QueryResult::DroppedMaterializedView { name } => {
            format!("Materialized view {name} dropped.")
        }
        QueryResult::DroppedView { name } => format!("View {name} dropped."),
        QueryResult::CreatedDatabase { name } => format!("Database {name} created."),
        QueryResult::DroppedDatabase { name } => format!("Database {name} dropped."),
        QueryResult::Used { db } => format!("Using database {db}."),
        QueryResult::LoadedExtension { name } => format!("Extension {name} loaded."),
        QueryResult::Checkpointed => "Checkpoint complete.".to_string(),
        QueryResult::AlteredTable { name } => format!("Table {name} altered."),
        QueryResult::CreatedProcedure { name } => format!("Procedure {name} created."),
        QueryResult::DroppedProcedure { name } => format!("Procedure {name} dropped."),
        QueryResult::CreatedMacro { name } => format!("Macro {name} created."),
        QueryResult::DroppedMacro { name } => format!("Macro {name} dropped."),
        QueryResult::CreatedUser { name } => format!("User {name} created."),
        QueryResult::AlteredUser { name } => format!("User {name} altered."),
        QueryResult::DroppedUser { name } => format!("User {name} dropped."),
        QueryResult::CreatedRole { name } => format!("Role {name} created."),
        QueryResult::DroppedRole { name } => format!("Role {name} dropped."),
        QueryResult::Granted { description } => format!("Granted {description}."),
        QueryResult::Revoked { description } => format!("Revoked {description}."),
        QueryResult::Called {
            name, statements, ..
        } => format!(
            "Procedure {name} called ({statements} statement{}).",
            if *statements == 1 { "" } else { "s" }
        ),
        // Selected/Inserted/Updated/Deleted carry their own shape; they
        // never reach this arm in push_result, but keeping the match total
        // documents the choice.
        other => result_kind(other).to_string(),
    }
}

/// Append one statement result in the ABI's JSON shape.
fn push_result(out: &mut String, r: &QueryResult) {
    match r {
        QueryResult::Selected { names, types, rows } => {
            out.push_str("{\"kind\":\"Selected\",\"names\":[");
            for (i, name) in names.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_string(out, name);
            }
            out.push_str("],\"types\":[");
            for (i, t) in types.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_string(out, &t.name());
            }
            out.push_str("],\"rows\":[");
            for (i, row) in rows.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('[');
                for (j, value) in row.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    push_value(out, value);
                }
                out.push(']');
            }
            out.push_str("]}");
        }
        QueryResult::Inserted { count }
        | QueryResult::Updated { count }
        | QueryResult::Deleted { count } => {
            out.push_str("{\"kind\":");
            push_json_string(out, result_kind(r));
            let _ = write!(out, ",\"count\":{count}}}");
        }
        other => {
            out.push_str("{\"kind\":");
            push_json_string(out, result_kind(other));
            out.push_str(",\"message\":");
            push_json_string(out, &result_message(other));
            out.push('}');
        }
    }
}

/// `{"ok":true,"results":[...]}` for a successful execution.
fn results_json(results: &[QueryResult]) -> String {
    let mut out = String::from("{\"ok\":true,\"results\":[");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_result(&mut out, r);
    }
    out.push_str("]}");
    out
}

/// `{"ok":false,"error":"..."}`.
fn error_json(msg: &str) -> String {
    let mut out = String::from("{\"ok\":false,\"error\":");
    push_json_string(&mut out, msg);
    out.push('}');
    out
}

// ----- raw buffers -----

/// Copy `payload` into a fresh `[u32 LE length][payload]` buffer and leak it.
fn into_buffer(payload: &[u8]) -> *mut u8 {
    let mut buf = Vec::with_capacity(4 + payload.len());
    // On wasm32 an addressable payload is always < 4 GiB.
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    Box::into_raw(buf.into_boxed_slice()) as *mut u8
}

/// `[u32 0xFFFFFFFF][u32 length][message]` for a failed snapshot.
fn snapshot_error_buffer(msg: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + msg.len());
    buf.extend_from_slice(&u32::MAX.to_le_bytes());
    buf.extend_from_slice(&(msg.len() as u32).to_le_bytes());
    buf.extend_from_slice(msg.as_bytes());
    buf
}

/// Read a caller-provided byte range.
///
/// # Safety
///
/// `ptr` must be non-NULL whenever `len > 0` and valid for `len` bytes for
/// the duration of the returned borrow (the ABI contract for every
/// `(ptr, len)` entry point of this crate).
unsafe fn read_bytes<'a>(ptr: *const u8, len: u32) -> Result<&'a [u8], String> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err("null pointer with non-zero length".to_string());
    }
    // SAFETY: non-null (checked above) and valid for `len` bytes per this
    // function's contract.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len as usize) })
}

// ----- exports -----

/// Allocate `len` zeroed bytes for the caller to fill (a SQL statement or a
/// snapshot image). Free with `pw_free(ptr, len)`. NULL on allocation panic.
#[unsafe(no_mangle)]
pub extern "C" fn pw_alloc(len: u32) -> *mut u8 {
    match catch_unwind(AssertUnwindSafe(|| {
        Box::into_raw(vec![0u8; len as usize].into_boxed_slice()) as *mut u8
    })) {
        Ok(p) => p,
        Err(_) => ptr::null_mut(),
    }
}

/// Free a buffer from [`pw_alloc`] or any buffer this module returned, with
/// the exact byte length the caller was given (`4 + length` for JSON and
/// snapshot rows, `8 + length` for a failed snapshot). NULL-safe.
#[unsafe(no_mangle)]
pub extern "C" fn pw_free(ptr: *mut u8, len: u32) {
    if ptr.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the pointer came from a `Box<[u8]>` leaked by this crate
        // (`pw_alloc`/`into_buffer`/`pw_snapshot`) and `len` is that box's
        // exact length per the ABI contract; the box is consumed once.
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                ptr,
                len as usize,
            )));
        }
    }));
}

/// Open a new in-memory database. Returns a handle > 0, or 0 if even the
/// empty store could not be built.
#[unsafe(no_mangle)]
pub extern "C" fn pw_open() -> u32 {
    catch_unwind(AssertUnwindSafe(|| {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            let mut id = state.next_handle.max(1);
            while state.handles.contains_key(&id) {
                id = id.wrapping_add(1).max(1);
            }
            state.next_handle = id.wrapping_add(1).max(1);
            state.handles.insert(id, PinkDb::new());
            id
        })
    }))
    .unwrap_or(0)
}

/// Close a handle from [`pw_open`]. Unknown handles are ignored. NULL-safe
/// in the only sense this ABI has: 0 is never a valid handle.
#[unsafe(no_mangle)]
pub extern "C" fn pw_close(handle: u32) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        STATE.with(|state| {
            state.borrow_mut().handles.remove(&handle);
        });
    }));
}

/// Run every statement in `sql` (`(ptr, len)`, UTF-8) on `handle`.
///
/// Returns `{"ok":true,"results":[...]}` or `{"ok":false,"error":"..."}`.
/// Like [`PinkDb::execute`], statements before the first failure keep their
/// effects but their results are not reported.
#[unsafe(no_mangle)]
pub extern "C" fn pw_exec(handle: u32, sql: *const u8, len: u32) -> *mut u8 {
    let json = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller guarantees (ptr, len) validity for the call.
        let bytes = match unsafe { read_bytes(sql, len) } {
            Ok(b) => b,
            Err(e) => return error_json(&e),
        };
        let Ok(sql) = std::str::from_utf8(bytes) else {
            return error_json("SQL is not valid UTF-8");
        };
        match with_db(handle, |db| db.execute(sql)) {
            None => error_json(&format!("unknown handle {handle}")),
            Some(Ok(results)) => results_json(&results),
            Some(Err(e)) => error_json(&e.to_string()),
        }
    }))
    .unwrap_or_else(|_| error_json("internal error: statement panicked"));
    into_buffer(json.as_bytes())
}

/// Serialize `handle`'s database to `.pink` bytes: `[u32 LE length][bytes]`,
/// or `[u32 0xFFFFFFFF][u32 length][message]` on error.
#[unsafe(no_mangle)]
pub extern "C" fn pw_snapshot(handle: u32) -> *mut u8 {
    let buf = catch_unwind(AssertUnwindSafe(|| {
        match with_db(handle, |db| db.to_pink_bytes()) {
            None => snapshot_error_buffer(&format!("unknown handle {handle}")),
            Some(Ok(bytes)) => {
                let mut buf = Vec::with_capacity(4 + bytes.len());
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
                buf
            }
            Some(Err(e)) => snapshot_error_buffer(&e.to_string()),
        }
    }))
    .unwrap_or_else(|_| snapshot_error_buffer("internal error: snapshot panicked"));
    Box::into_raw(buf.into_boxed_slice()) as *mut u8
}

/// Replace `handle`'s database with a snapshot image (the bytes
/// [`pw_snapshot`] returned). The current database is kept when the image is
/// refused. Returns `{"ok":true}` or `{"ok":false,"error":"..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn pw_restore(handle: u32, bytes: *const u8, len: u32) -> *mut u8 {
    let json = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller guarantees (ptr, len) validity for the call.
        let bytes = match unsafe { read_bytes(bytes, len) } {
            Ok(b) => b,
            Err(e) => return error_json(&e),
        };
        match PinkDb::from_pink_bytes(bytes) {
            Ok(db) => STATE.with(|state| {
                let mut state = state.borrow_mut();
                match state.handles.get_mut(&handle) {
                    Some(slot) => {
                        *slot = db;
                        "{\"ok\":true}".to_string()
                    }
                    None => error_json(&format!("unknown handle {handle}")),
                }
            }),
            Err(e) => error_json(&e.to_string()),
        }
    }))
    .unwrap_or_else(|_| error_json("internal error: restore panicked"));
    into_buffer(json.as_bytes())
}

/// The engine version as JSON: `{"version":"..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn pw_version() -> *mut u8 {
    let json = catch_unwind(AssertUnwindSafe(|| {
        let mut out = String::from("{\"version\":");
        push_json_string(&mut out, PINKDB_WASM_VERSION);
        out.push('}');
        out
    }))
    .unwrap_or_else(|_| error_json("internal error: version panicked"));
    into_buffer(json.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::DataType;

    fn json_of(v: &Value) -> String {
        let mut out = String::new();
        push_value(&mut out, v);
        out
    }

    #[test]
    fn json_strings_escape_quotes_backslashes_and_controls() {
        let mut out = String::new();
        push_json_string(&mut out, "a\"b\\c\n\r\t\u{08}\u{0c}\u{01}\u{1f}é");
        assert_eq!(
            out, "\"a\\\"b\\\\c\\n\\r\\t\\b\\f\\u0001\\u001fé\"",
            "JSON escaping must cover quotes, backslashes and every control char"
        );
    }

    #[test]
    fn bigints_cross_as_numbers_only_within_the_js_safe_range() {
        // 2^53 - 1 is the last exactly representable integer.
        assert_eq!(
            json_of(&Value::Bigint(9_007_199_254_740_991)),
            "9007199254740991"
        );
        assert_eq!(
            json_of(&Value::Bigint(-9_007_199_254_740_991)),
            "-9007199254740991"
        );
        // One past it (2^53) and the first odd one past it are strings.
        assert_eq!(
            json_of(&Value::Bigint(9_007_199_254_740_992)),
            "\"9007199254740992\""
        );
        assert_eq!(
            json_of(&Value::Bigint(9_007_199_254_740_993)),
            "\"9007199254740993\""
        );
        assert_eq!(
            json_of(&Value::Bigint(-9_007_199_254_740_993)),
            "\"-9007199254740993\""
        );
        assert_eq!(
            json_of(&Value::Bigint(i64::MIN)),
            format!("\"{}\"", i64::MIN)
        );
        assert_eq!(json_of(&Value::Integer(-7)), "-7");
    }

    #[test]
    fn non_finite_doubles_cross_as_strings() {
        assert_eq!(json_of(&Value::Double(f64::NAN)), "\"NaN\"");
        assert_eq!(json_of(&Value::Double(f64::INFINITY)), "\"Infinity\"");
        assert_eq!(json_of(&Value::Double(f64::NEG_INFINITY)), "\"-Infinity\"");
        assert_eq!(json_of(&Value::Double(2.5)), "2.5");
        assert_eq!(json_of(&Value::Double(-0.0)), "-0");
    }

    #[test]
    fn temporal_and_exact_values_cross_as_display_text() {
        assert_eq!(json_of(&Value::Date(19_737)), "\"2024-01-15\"");
        assert_eq!(
            json_of(&Value::Decimal(12_345, 2)),
            "\"123.45\"",
            "DECIMAL crosses as its Display text"
        );
        assert_eq!(json_of(&Value::Null), "null");
        assert_eq!(json_of(&Value::Boolean(true)), "true");
        assert_eq!(
            json_of(&Value::Varchar("hé\"llo".to_string())),
            "\"hé\\\"llo\""
        );
    }

    #[test]
    fn selected_result_has_the_abi_shape() {
        let result = QueryResult::Selected {
            names: vec!["big".to_string()],
            types: vec![DataType::Bigint],
            rows: vec![vec![Value::Bigint(9_007_199_254_740_993)]],
        };
        assert_eq!(
            results_json(std::slice::from_ref(&result)),
            "{\"ok\":true,\"results\":[{\"kind\":\"Selected\",\"names\":[\"big\"],\
             \"types\":[\"BIGINT\"],\"rows\":[[\"9007199254740993\"]]}]}"
        );
    }

    #[test]
    fn simple_results_and_errors_have_the_abi_shape() {
        assert_eq!(
            results_json(&[QueryResult::Inserted { count: 3 }]),
            "{\"ok\":true,\"results\":[{\"kind\":\"Inserted\",\"count\":3}]}"
        );
        assert_eq!(
            results_json(&[QueryResult::Created]),
            "{\"ok\":true,\"results\":[{\"kind\":\"Created\",\"message\":\"Table created.\"}]}"
        );
        assert_eq!(
            error_json("bad \"thing\""),
            "{\"ok\":false,\"error\":\"bad \\\"thing\\\"\"}"
        );
    }

    #[test]
    fn handles_are_thread_local_and_round_trip_through_the_abi() {
        let h = pw_open();
        assert!(h > 0);
        let sql = b"CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);";
        let ptr = pw_exec(h, sql.as_ptr(), sql.len() as u32);
        assert!(!ptr.is_null());
        // SAFETY: the buffer is `[u32 len][payload]`, just returned.
        let len = unsafe { std::slice::from_raw_parts(ptr, 4) };
        let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
        // SAFETY: `ptr` is valid for `4 + len` bytes.
        let json = unsafe { std::slice::from_raw_parts(ptr.add(4), len) };
        let json = std::str::from_utf8(json).unwrap();
        assert!(json.starts_with("{\"ok\":true,"), "{json}");
        assert!(json.contains("\"kind\":\"Inserted\",\"count\":2"), "{json}");
        pw_free(ptr, (4 + len) as u32);
        // Snapshot -> restore into a second handle.
        let snap = pw_snapshot(h);
        assert!(!snap.is_null());
        // SAFETY: `snap` is a length-prefixed buffer.
        let len = unsafe { std::slice::from_raw_parts(snap, 4) };
        let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
        // SAFETY: `snap` is valid for `4 + len` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(snap.add(4), len) };
        assert!(bytes.starts_with(b"PINKDB05"));
        let h2 = pw_open();
        assert!(h2 > 0);
        let restore = pw_restore(h2, bytes.as_ptr(), bytes.len() as u32);
        // SAFETY: `restore` is a length-prefixed buffer.
        let rlen = unsafe { std::slice::from_raw_parts(restore, 4) };
        let rlen = u32::from_le_bytes([rlen[0], rlen[1], rlen[2], rlen[3]]) as usize;
        // SAFETY: `restore` is valid for `4 + rlen` bytes.
        let rjson = unsafe { std::slice::from_raw_parts(restore.add(4), rlen) };
        assert_eq!(std::str::from_utf8(rjson).unwrap(), "{\"ok\":true}");
        pw_free(restore, (4 + rlen) as u32);
        pw_free(snap, (4 + len) as u32);
        pw_close(h);
        pw_close(h2);
        pw_close(h2); // idempotent
    }
}
