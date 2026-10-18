//! Integration tests for the C ABI: durable open through a real file, full
//! value fidelity, and ownership loops.

use std::ffi::{CStr, CString, c_char};
use std::ptr;

use pinkdb_ffi::*;

fn last_error() -> String {
    let p = pinkdb_last_error();
    if p.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

fn exec(h: *mut PinkHandle, sql: &str) -> *mut PinkResult {
    let c = CString::new(sql).unwrap();
    let r = pinkdb_execute(h, c.as_ptr(), sql.len());
    assert!(!r.is_null(), "execute failed: {}", last_error());
    r
}

fn text(v: &PinkValue) -> String {
    let bytes = unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, v.text_len) };
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn durable_roundtrip_through_the_c_abi() {
    let dir = std::env::temp_dir().join(format!("pinkdb_ffi_it_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.pink");
    let cpath = CString::new(path.to_str().unwrap()).unwrap();

    let h = pinkdb_open(cpath.as_ptr());
    assert!(!h.is_null(), "open failed: {}", last_error());
    let r = exec(
        h,
        "CREATE TABLE t (i INTEGER, b BIGINT, d DOUBLE, s VARCHAR, dt DATE, ts TIMESTAMP);
         INSERT INTO t VALUES
           (-2147483647 - 1, 9223372036854775807, -0.0, '', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00.000001'),
           (NULL, NULL, NULL, NULL, NULL, NULL);",
    );
    assert_eq!(pinkdb_result_kind(r, 1), PINKDB_STMT_INSERTED);
    assert_eq!(pinkdb_result_affected(r, 1), 2);
    pinkdb_result_free(r);
    pinkdb_close(h);

    // Reopen the durable store (checkpoint + WAL) and read everything back.
    let h = pinkdb_open(cpath.as_ptr());
    assert!(!h.is_null(), "reopen failed: {}", last_error());
    let r = exec(h, "SELECT i, b, d, s, dt, ts FROM t ORDER BY b;");
    assert_eq!(pinkdb_result_statement_count(r), 1);
    assert_eq!(pinkdb_result_row_count(r, 0), 2);
    let v = pinkdb_result_value(r, 0, 0, 0);
    assert_eq!(
        (v.type_tag, v.is_null, v.int64),
        (PINKDB_TYPE_INTEGER, 0, i64::from(i32::MIN))
    );
    let v = pinkdb_result_value(r, 0, 0, 1);
    assert_eq!(v.int64, i64::MAX);
    let v = pinkdb_result_value(r, 0, 0, 2);
    assert_eq!(
        v.double.to_bits(),
        (-0.0f64).to_bits(),
        "-0.0 must round-trip"
    );
    let v = pinkdb_result_value(r, 0, 0, 3);
    assert_eq!((v.type_tag, text(&v).len()), (PINKDB_TYPE_VARCHAR, 0));
    let v = pinkdb_result_value(r, 0, 0, 4);
    assert_eq!((v.type_tag, v.int64), (PINKDB_TYPE_DATE, 0));
    let v = pinkdb_result_value(r, 0, 0, 5);
    assert_eq!((v.type_tag, v.int64), (PINKDB_TYPE_TIMESTAMP, 1));
    for col in 0..6 {
        let v = pinkdb_result_value(r, 0, 1, col);
        assert_eq!(v.is_null, 1, "column {col} should be NULL");
    }
    assert_eq!(pinkdb_result_column_type(r, 0, 5), PINKDB_TYPE_TIMESTAMP);
    pinkdb_result_free(r);
    pinkdb_close(h);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn errors_and_guards_through_the_c_abi() {
    // NULL handle.
    assert!(pinkdb_execute(ptr::null_mut(), ptr::null(), 0).is_null());
    assert!(last_error().contains("null handle"));
    // Invalid UTF-8 SQL.
    let h = pinkdb_open_memory();
    let bad = [0xff_u8, 0xfe];
    assert!(pinkdb_execute(h, bad.as_ptr() as *const c_char, bad.len()).is_null());
    assert!(last_error().contains("UTF-8"));
    // Bad SQL keeps the handle usable and clears the error on success.
    let r = pinkdb_execute(h, CString::new("SELECT * FROM nope;").unwrap().as_ptr(), 18);
    assert!(r.is_null());
    assert!(last_error().contains("not found"));
    let r = exec(h, "SELECT 42;");
    assert!(pinkdb_last_error().is_null());
    assert_eq!(pinkdb_result_value(r, 0, 0, 0).int64, 42);
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn ownership_loops_do_not_leak_or_corrupt() {
    let h = pinkdb_open_memory();
    for i in 0..200 {
        let sql = format!("SELECT {i} AS n, 'v{i}' AS s;");
        let r = exec(h, &sql);
        assert_eq!(pinkdb_result_value(r, 0, 0, 0).int64, i);
        assert_eq!(text(&pinkdb_result_value(r, 0, 0, 1)), format!("v{i}"));
        pinkdb_result_free(r);
    }
    pinkdb_close(h);
}

/// A prepared INSERT on a durable database: the WAL carries the substituted
/// SQL, and reopening replays it to the same rows.
#[test]
fn prepared_durable_round_trip_through_the_c_abi() {
    let dir = std::env::temp_dir().join(format!("pinkdb_ffi_prep_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("p.pink");
    let cpath = CString::new(path.to_str().unwrap()).unwrap();

    let text_cell = |s: &str| -> (PinkValue, Vec<u8>) {
        let mut owned = s.as_bytes().to_vec();
        owned.push(0);
        let ptr = owned.as_ptr() as *const c_char;
        let len = owned.len() - 1;
        (
            PinkValue {
                type_tag: PINKDB_TYPE_VARCHAR,
                is_null: 0,
                int64: 0,
                double: 0.0,
                text_ptr: ptr,
                text_len: len,
            },
            owned,
        )
    };

    let h = pinkdb_open(cpath.as_ptr());
    assert!(!h.is_null(), "open failed: {}", last_error());
    let r = exec(h, "CREATE TABLE t (i INTEGER, s VARCHAR);");
    pinkdb_result_free(r);
    let sql = CString::new("INSERT INTO t VALUES (?, ?);").unwrap();
    let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
    assert!(!prep.is_null(), "{:?}", last_error());
    assert_eq!(pinkdb_prepared_parameter_count(prep), 2);
    for (i, s) in [(1, "it's"), (2, "two")] {
        let (s_cell, _keep) = text_cell(s);
        let i_cell = PinkValue {
            type_tag: PINKDB_TYPE_INTEGER,
            is_null: 0,
            int64: i,
            double: 0.0,
            text_ptr: ptr::null(),
            text_len: 0,
        };
        let values = [i_cell, s_cell];
        let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 2);
        assert!(!r.is_null(), "{:?}", last_error());
        assert_eq!(pinkdb_result_affected(r, 0), 1);
        pinkdb_result_free(r);
    }
    pinkdb_prepared_free(prep);
    pinkdb_close(h);

    // The WAL records the substituted SQL (quotes doubled), never `?`.
    let wal = std::fs::read(format!("{}.wal", path.to_str().unwrap())).unwrap_or_default();
    let wal_text = String::from_utf8_lossy(&wal);
    assert!(
        wal_text.contains("INSERT INTO t VALUES (1, 'it''s')"),
        "unexpected WAL: {wal_text}"
    );
    assert!(!wal_text.contains("VALUES (?, ?)"), "{wal_text}");

    // Reopen: recovery replays the WAL to the same state.
    let h = pinkdb_open(cpath.as_ptr());
    assert!(!h.is_null(), "reopen failed: {}", last_error());
    let r = exec(h, "SELECT i, s FROM t ORDER BY i;");
    assert_eq!(pinkdb_result_row_count(r, 0), 2);
    let v = pinkdb_result_value(r, 0, 0, 1);
    assert_eq!(text(&v), "it's");
    assert_eq!(pinkdb_result_value(r, 0, 1, 0).int64, 2);
    pinkdb_result_free(r);
    pinkdb_close(h);

    std::fs::remove_dir_all(&dir).ok();
}
