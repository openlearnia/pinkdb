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

fn last_error_class() -> String {
    let p = pinkdb_last_error_class();
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
        "CREATE TABLE t (i INTEGER, b BIGINT, d DOUBLE, s VARCHAR, dt DATE, ts TIMESTAMP, va VARIANT);
         INSERT INTO t VALUES
           (-2147483647 - 1, 9223372036854775807, -0.0e0, '', DATE '1970-01-01',
            TIMESTAMP '1970-01-01 00:00:00.000001',
            CAST('{\"a\":[1,2.5,\"s\",null,true]}' AS VARIANT)),
           (NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
    );
    assert_eq!(pinkdb_result_kind(r, 1), PINKDB_STMT_INSERTED);
    assert_eq!(pinkdb_result_affected(r, 1), 2);
    pinkdb_result_free(r);
    pinkdb_close(h);

    // Reopen the durable store (checkpoint + WAL) and read everything back.
    let h = pinkdb_open(cpath.as_ptr());
    assert!(!h.is_null(), "reopen failed: {}", last_error());
    let r = exec(h, "SELECT i, b, d, s, dt, ts, va FROM t ORDER BY b;");
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
    // A VARIANT crosses as its JSON text (wave 101a); DATE/TIMESTAMP kinds
    // would print ISO strings and cannot be carried back exactly.
    let v = pinkdb_result_value(r, 0, 0, 6);
    assert_eq!(v.type_tag, PINKDB_TYPE_VARIANT);
    assert_eq!(
        text(&v),
        r#"{"a":[1,2.5,"s",null,true]}"#,
        "VARIANT crosses as JSON text"
    );
    for col in 0..7 {
        let v = pinkdb_result_value(r, 0, 1, col);
        assert_eq!(v.is_null, 1, "column {col} should be NULL");
    }
    assert_eq!(pinkdb_result_column_type(r, 0, 5), PINKDB_TYPE_TIMESTAMP);
    assert_eq!(pinkdb_result_column_type(r, 0, 6), PINKDB_TYPE_VARIANT);
    pinkdb_result_free(r);
    pinkdb_close(h);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn uuid_columns_report_the_uuid_tag() {
    let h = pinkdb_open_memory();
    let r = exec(
        h,
        "CREATE TABLE u (id UUID, b BIGINT);
         INSERT INTO u VALUES ('550E8400-E29B-41D4-A716-446655440000', 1), (NULL, 2);
         SELECT id FROM u ORDER BY b;",
    );
    // 1.2 reports a real UUID tag; the v1 cell still carries the lowercase
    // text form for callers that have not moved to chunk fetch.
    // Statements: 0 CREATE, 1 INSERT, 2 SELECT.
    assert_eq!(pinkdb_result_column_type(r, 2, 0), PINKDB_TYPE_UUID);
    assert_eq!(
        text(&pinkdb_result_value(r, 2, 0, 0)),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(pinkdb_result_value(r, 2, 1, 0).is_null, 1);
    pinkdb_result_free(r);
    pinkdb_close(h);
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
fn error_classes_are_stable_across_the_c_abi() {
    // Argument errors.
    assert!(pinkdb_execute(ptr::null_mut(), ptr::null(), 0).is_null());
    assert_eq!(last_error_class(), "invalid_argument");
    let h = pinkdb_open_memory();
    let bad = [0xff_u8, 0xfe];
    assert!(pinkdb_execute(h, bad.as_ptr() as *const c_char, bad.len()).is_null());
    assert_eq!(last_error_class(), "invalid_argument");

    // Engine errors keep their `PinkDbError` variant as the class.
    let cases = [
        ("SELEC 1", "parse"),
        ("SELECT * FROM no_such_table", "catalog"),
        ("SELECT 1 + 'x'", "type"),
        ("SELECT 1 / 0", "execution"),
    ];
    for (sql, class) in cases {
        let c = CString::new(sql).unwrap();
        let r = pinkdb_execute(h, c.as_ptr(), sql.len());
        assert!(r.is_null(), "{sql} should fail");
        assert_eq!(last_error_class(), class, "{sql}: {}", last_error());
    }

    // The one enforced constraint gets its own class.
    let sql = CString::new(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);
         ALTER TABLE t ADD COLUMN b INTEGER NOT NULL;",
    )
    .unwrap();
    let r = pinkdb_execute(h, sql.as_ptr(), sql.as_bytes().len());
    assert!(r.is_null(), "the NOT NULL column must be refused");
    assert_eq!(last_error_class(), "constraint", "{}", last_error());
    assert!(last_error().starts_with("NOT NULL constraint failed"));

    // A wrong bound-value count is a parameter error.
    let sql = CString::new("SELECT ? + 1").unwrap();
    let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
    assert!(!prep.is_null(), "{}", last_error());
    let r = pinkdb_execute_prepared(h, prep, ptr::null(), 0);
    assert!(r.is_null());
    assert_eq!(last_error_class(), "parameter");
    pinkdb_prepared_free(prep);

    // Success clears message and class.
    let r = exec(h, "SELECT 1");
    assert!(pinkdb_last_error().is_null());
    assert!(pinkdb_last_error_class().is_null());
    pinkdb_result_free(r);
    pinkdb_close(h);
}

/// C-2: `LOAD EXTENSION` is refused by default through the C ABI, and
/// `pinkdb_set_allow_sql_load_extension` flips the behavior.
#[test]
fn the_load_extension_switch_flips_the_behavior() {
    let h = pinkdb_open_memory();
    assert!(!h.is_null());

    // Off by default: the statement is refused before anything is opened.
    let sql = CString::new("LOAD EXTENSION '/definitely/not/a/lib.dylib';").unwrap();
    let (ptr, len) = (sql.as_ptr(), sql.as_bytes().len());
    let r = pinkdb_execute(h, ptr, len);
    assert!(r.is_null(), "the statement must be refused by default");
    let message = last_error();
    assert!(message.contains("LOAD EXTENSION is disabled"), "{message}");

    // The switch turns it on: the loader's own error proves the gate is no
    // longer the blocker.
    assert_eq!(pinkdb_set_allow_sql_load_extension(h, 1), 1);
    let r = pinkdb_execute(h, ptr, len);
    assert!(r.is_null());
    let message = last_error();
    assert!(!message.contains("LOAD EXTENSION is disabled"), "{message}");
    assert!(message.contains("cannot load extension"), "{message}");

    // And it turns it off again.
    assert_eq!(pinkdb_set_allow_sql_load_extension(h, 0), 1);
    let r = pinkdb_execute(h, ptr, len);
    assert!(r.is_null());
    assert!(last_error().contains("LOAD EXTENSION is disabled"));

    // A NULL handle is refused, not a crash.
    assert_eq!(pinkdb_set_allow_sql_load_extension(ptr::null_mut(), 1), 0);
    assert!(last_error().contains("null handle"));

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

/// L-7: `value_count` is validated before the host allocates its binding
/// vector. An absurd count must be a clean error, never a capacity abort.
#[test]
fn absurd_prepared_value_count_is_rejected_before_allocation() {
    let h = pinkdb_open_memory();
    assert!(!h.is_null());
    let sql = CString::new("SELECT ?;").unwrap();
    let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
    assert!(!prep.is_null(), "{:?}", last_error());
    assert_eq!(pinkdb_prepared_parameter_count(prep), 1);

    let dummy = PinkValue {
        type_tag: PINKDB_TYPE_INTEGER,
        is_null: 0,
        int64: 1,
        double: 0.0,
        text_ptr: ptr::null(),
        text_len: 0,
    };
    let r = pinkdb_execute_prepared(h, prep, &dummy, 1usize << 62);
    assert!(r.is_null(), "an absurd value_count must be refused");
    let err = last_error();
    assert!(err.contains("exceeds the maximum"), "{err}");

    // A plain wrong count is a normal user error, not an abort either.
    let r = pinkdb_execute_prepared(h, prep, &dummy, 2);
    assert!(r.is_null());
    let err = last_error();
    assert!(
        err.contains("prepared statement takes 1, but values were provided for: 2"),
        "{err}"
    );

    // The handle still works normally.
    let r = pinkdb_execute_prepared(h, prep, &dummy, 1);
    assert!(!r.is_null(), "{:?}", last_error());
    pinkdb_result_free(r);
    pinkdb_prepared_free(prep);
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

fn profile_kind(r: *const PinkResult, statement: usize) -> Option<String> {
    let p = pinkdb_result_profile_kind(r, statement);
    if p.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }
}

#[test]
fn profiling_is_off_by_default_and_reports_no_profile() {
    let h = pinkdb_open_memory();
    assert!(!h.is_null());
    let r = exec(h, "SELECT 1 AS n;");
    assert_eq!(pinkdb_result_profile_available(r, 0), 0);
    assert_eq!(pinkdb_result_profile_parse_ns(r, 0), 0);
    assert_eq!(pinkdb_result_profile_bind_ns(r, 0), 0);
    assert_eq!(pinkdb_result_profile_optimize_ns(r, 0), 0);
    assert_eq!(pinkdb_result_profile_execute_ns(r, 0), 0);
    assert_eq!(pinkdb_result_profile_log_ns(r, 0), 0);
    assert_eq!(pinkdb_result_profile_total_ns(r, 0), 0);
    assert_eq!(pinkdb_result_profile_rows(r, 0), 0);
    assert_eq!(pinkdb_result_profile_columns(r, 0), 0);
    assert!(pinkdb_result_profile_kind(r, 0).is_null());

    // Out-of-range indices and a NULL result are "no profile", never a crash.
    assert_eq!(pinkdb_result_profile_available(r, 7), 0);
    assert_eq!(pinkdb_result_profile_parse_ns(r, 7), 0);
    assert_eq!(pinkdb_result_profile_bind_ns(r, 7), 0);
    assert_eq!(pinkdb_result_profile_optimize_ns(r, 7), 0);
    assert_eq!(pinkdb_result_profile_execute_ns(r, 7), 0);
    assert_eq!(pinkdb_result_profile_log_ns(r, 7), 0);
    assert_eq!(pinkdb_result_profile_total_ns(r, 7), 0);
    assert_eq!(pinkdb_result_profile_rows(r, 7), 0);
    assert_eq!(pinkdb_result_profile_columns(r, 7), 0);
    assert!(pinkdb_result_profile_kind(r, 7).is_null());
    assert_eq!(pinkdb_result_profile_available(ptr::null(), 0), 0);
    assert_eq!(pinkdb_result_profile_total_ns(ptr::null(), 0), 0);
    assert!(pinkdb_result_profile_kind(ptr::null(), 0).is_null());

    // A NULL handle is a documented error.
    assert_eq!(pinkdb_set_profiling(ptr::null_mut(), 1), 0);
    assert!(last_error().contains("null handle"));

    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn profiling_reports_every_statement_of_a_batch() {
    let h = pinkdb_open_memory();
    assert_eq!(pinkdb_set_profiling(h, 1), 1);
    assert!(last_error().is_empty());
    let r = exec(
        h,
        "CREATE TABLE t (i INTEGER, s VARCHAR);
         INSERT INTO t VALUES (1, 'a'), (2, 'b');
         SELECT i, s FROM t ORDER BY i;",
    );
    assert_eq!(pinkdb_result_statement_count(r), 3);
    for statement in 0..3 {
        assert_eq!(
            pinkdb_result_profile_available(r, statement),
            1,
            "statement {statement} has no profile"
        );
    }

    assert_eq!(profile_kind(r, 0).as_deref(), Some("Created"));
    assert_eq!(pinkdb_result_profile_rows(r, 0), 0);
    assert_eq!(pinkdb_result_profile_columns(r, 0), 0);

    assert_eq!(profile_kind(r, 1).as_deref(), Some("Inserted"));
    assert_eq!(pinkdb_result_profile_rows(r, 1), 2);
    assert_eq!(pinkdb_result_profile_columns(r, 1), 0);

    assert_eq!(profile_kind(r, 2).as_deref(), Some("Selected"));
    assert_eq!(pinkdb_result_profile_rows(r, 2), 2);
    assert_eq!(pinkdb_result_profile_columns(r, 2), 2);

    // Timings are nanoseconds; total covers bind..log plus (for the first
    // statement only) its share of the batch's parse time.
    for statement in 0..3 {
        let phases = pinkdb_result_profile_bind_ns(r, statement)
            + pinkdb_result_profile_optimize_ns(r, statement)
            + pinkdb_result_profile_execute_ns(r, statement)
            + pinkdb_result_profile_log_ns(r, statement);
        assert!(
            pinkdb_result_profile_total_ns(r, statement) >= phases,
            "statement {statement}: total is less than its phases"
        );
        if statement == 0 {
            assert!(
                pinkdb_result_profile_total_ns(r, statement)
                    >= phases + pinkdb_result_profile_parse_ns(r, statement),
                "the first statement's total must include parse"
            );
        }
    }

    // Out-of-range on a profiled result is still "no profile".
    assert_eq!(pinkdb_result_profile_available(r, 3), 0);
    assert_eq!(pinkdb_result_profile_total_ns(r, 3), 0);
    assert!(pinkdb_result_profile_kind(r, 3).is_null());

    pinkdb_result_free(r);

    // Turning profiling off means later results carry no profile.
    assert_eq!(pinkdb_set_profiling(h, 0), 1);
    let r = exec(h, "SELECT 1;");
    assert_eq!(pinkdb_result_profile_available(r, 0), 0);
    assert!(pinkdb_result_profile_kind(r, 0).is_null());
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn profiling_covers_prepared_statements() {
    let h = pinkdb_open_memory();
    assert_eq!(pinkdb_set_profiling(h, 1), 1);
    let r = exec(h, "CREATE TABLE t (i INTEGER, s VARCHAR);");
    pinkdb_result_free(r);

    let insert = CString::new("INSERT INTO t VALUES (?, ?);").unwrap();
    let prep = pinkdb_prepare(h, insert.as_ptr(), insert.as_bytes().len());
    assert!(!prep.is_null(), "{:?}", last_error());
    let i = PinkValue {
        type_tag: PINKDB_TYPE_INTEGER,
        is_null: 0,
        int64: 7,
        double: 0.0,
        text_ptr: ptr::null(),
        text_len: 0,
    };
    let mut text = b"seven".to_vec();
    text.push(0);
    let s = PinkValue {
        type_tag: PINKDB_TYPE_VARCHAR,
        is_null: 0,
        int64: 0,
        double: 0.0,
        text_ptr: text.as_ptr() as *const c_char,
        text_len: text.len() - 1,
    };
    let values = [i, s];
    let r = pinkdb_execute_prepared(h, prep, values.as_ptr(), 2);
    assert!(!r.is_null(), "{:?}", last_error());
    assert_eq!(pinkdb_result_profile_available(r, 0), 1);
    assert_eq!(profile_kind(r, 0).as_deref(), Some("Inserted"));
    assert_eq!(pinkdb_result_profile_rows(r, 0), 1);
    assert_eq!(pinkdb_result_profile_columns(r, 0), 0);
    pinkdb_result_free(r);
    pinkdb_prepared_free(prep);

    let select = CString::new("SELECT i, s FROM t;").unwrap();
    let prep = pinkdb_prepare(h, select.as_ptr(), select.as_bytes().len());
    assert!(!prep.is_null(), "{:?}", last_error());
    let r = pinkdb_execute_prepared(h, prep, ptr::null(), 0);
    assert!(!r.is_null(), "{:?}", last_error());
    assert_eq!(profile_kind(r, 0).as_deref(), Some("Selected"));
    assert_eq!(pinkdb_result_profile_rows(r, 0), 1);
    assert_eq!(pinkdb_result_profile_columns(r, 0), 2);
    pinkdb_result_free(r);
    pinkdb_prepared_free(prep);
    pinkdb_close(h);
}

#[test]
fn profiling_is_per_handle() {
    let profiled = pinkdb_open_memory();
    let plain = pinkdb_open_memory();
    assert_eq!(pinkdb_set_profiling(profiled, 1), 1);

    let r = exec(profiled, "SELECT 1;");
    assert_eq!(pinkdb_result_profile_available(r, 0), 1);
    pinkdb_result_free(r);

    let r = exec(plain, "SELECT 1;");
    assert_eq!(pinkdb_result_profile_available(r, 0), 0);
    pinkdb_result_free(r);

    pinkdb_close(profiled);
    pinkdb_close(plain);
}

/// Run a query whose v1 cell conversion is expected to fail. A streaming
/// result defers that conversion until the cell is read.
fn cell_err(h: *mut PinkHandle, sql: &str) -> (String, String) {
    let c = CString::new(sql).unwrap();
    let r = pinkdb_execute(h, c.as_ptr(), sql.len());
    assert!(!r.is_null(), "expected a result for {sql:?}");
    let value = pinkdb_result_value(r, 0, 0, 0);
    assert_eq!(value.type_tag, PINKDB_TYPE_NULL, "{sql:?}");
    let msg = unsafe { CStr::from_ptr(pinkdb_result_error(r)) }
        .to_string_lossy()
        .into_owned();
    let class = unsafe { CStr::from_ptr(pinkdb_result_error_class(r)) }
        .to_string_lossy()
        .into_owned();
    pinkdb_result_free(r);
    (msg, class)
}

/// The `f64` elements of an ARRAY cell.
fn floats(v: &PinkValue) -> Vec<f64> {
    assert_eq!(
        v.type_tag & 0xff,
        PINKDB_TYPE_ARRAY,
        "not an ARRAY cell: {}",
        v.type_tag
    );
    let bytes = unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, v.text_len) };
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| f64::from_le_bytes(*c))
        .collect()
}

fn floats32(v: &PinkValue) -> Vec<f32> {
    assert_eq!(
        v.type_tag & 0xff,
        PINKDB_TYPE_ARRAY_FLOAT,
        "not a FLOAT ARRAY cell: {}",
        v.type_tag
    );
    let bytes = unsafe { std::slice::from_raw_parts(v.text_ptr as *const u8, v.text_len) };
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
        .collect()
}

/// A `FLOAT[N]` cell is the 4-byte counterpart: tag 12 carries the length and
/// the payload is N little-endian `f32`s — half the bytes of a DOUBLE array.
#[test]
fn float_arrays_cross_the_c_abi_as_f32_payloads() {
    let h = pinkdb_open_memory();
    assert!(!h.is_null());
    pinkdb_result_free(exec(h, "CREATE TABLE t(id INTEGER, v FLOAT[2]);"));
    pinkdb_result_free(exec(
        h,
        "INSERT INTO t VALUES (1, [1.5, -0.25]), (2, NULL);",
    ));
    let r = exec(
        h,
        "SELECT v, v[1], len(v), CAST('[0.1]' AS FLOAT[1])[1] FROM t ORDER BY id;",
    );
    assert!(!r.is_null());
    assert_eq!(pinkdb_result_column_type(r, 0, 0), float_array_tag(2));
    assert_eq!(array_tag_len(pinkdb_result_column_type(r, 0, 0)), 2);
    let cell = pinkdb_result_value(r, 0, 0, 0);
    assert_eq!(cell.type_tag, float_array_tag(2));
    assert_eq!(cell.int64, 2, "the element count rides int64");
    assert_eq!(cell.text_len, 8, "two 4-byte elements");
    assert_eq!(floats32(&cell), vec![1.5f32, -0.25]);
    // A scalar element still widens to DOUBLE, exactly.
    let element = pinkdb_result_value(r, 0, 0, 1);
    assert_eq!(element.type_tag, PINKDB_TYPE_DOUBLE);
    assert_eq!(element.double, 1.5);
    // The f32 literal keeps its narrowed value.
    let rounded = pinkdb_result_value(r, 0, 0, 3);
    assert_eq!(rounded.double, f64::from(0.1f32));
    // A NULL array carries no payload and no type.
    let null = pinkdb_result_value(r, 0, 1, 0);
    assert_eq!(null.type_tag, PINKDB_TYPE_NULL);
    assert_eq!(null.is_null, 1);
    // A bound FLOAT parameter round-trips through its payload.
    let insert = CString::new("INSERT INTO t VALUES (?, ?);").unwrap();
    let stmt = pinkdb_prepare(h, insert.as_ptr(), insert.as_bytes().len());
    assert!(!stmt.is_null(), "{:?}", last_error());
    assert_eq!(pinkdb_prepared_parameter_count(stmt), 2);
    let raw: Vec<u8> = [1.0f32, 2.0].iter().flat_map(|x| x.to_le_bytes()).collect();
    let id = PinkValue {
        type_tag: PINKDB_TYPE_INTEGER,
        is_null: 0,
        int64: 3,
        double: 0.0,
        text_ptr: ptr::null(),
        text_len: 0,
    };
    let param = PinkValue {
        type_tag: float_array_tag(2),
        is_null: 0,
        int64: 2,
        double: 0.0,
        text_ptr: raw.as_ptr() as *const c_char,
        text_len: raw.len(),
    };
    let values = [id, param];
    let ok = pinkdb_execute_prepared(h, stmt, values.as_ptr(), 2);
    assert!(!ok.is_null(), "bound FLOAT[2] parameter: {}", last_error());
    pinkdb_result_free(ok);
    let r = exec(h, "SELECT v[2] FROM t WHERE id = 3;");
    assert_eq!(pinkdb_result_value(r, 0, 0, 0).double, 2.0);
    // A payload of the wrong width is refused.
    let short = PinkValue {
        type_tag: float_array_tag(2),
        is_null: 0,
        int64: 2,
        double: 0.0,
        text_ptr: raw.as_ptr() as *const c_char,
        text_len: 4,
    };
    let values = [id, short];
    let bad = pinkdb_execute_prepared(h, stmt, values.as_ptr(), 2);
    assert!(bad.is_null());
    assert!(last_error().contains("payload"), "{}", last_error());
    pinkdb_result_free(r);
    pinkdb_close(h);
}

/// ARRAY values cross as one contiguous payload: the length rides in the tag
/// and the elements are little-endian `f64` bytes, so no per-element boxing.
#[test]
fn arrays_cross_the_c_abi_as_double_payloads() {
    let h = pinkdb_open_memory();
    assert!(!h.is_null());
    pinkdb_result_free(exec(h, "CREATE TABLE t(id INTEGER, v DOUBLE[3]);"));
    pinkdb_result_free(exec(
        h,
        "INSERT INTO t VALUES (1, [1.5, -0.0e0, 2.0]), (2, NULL), (3, [4.0, 5.0, 6.0]);",
    ));

    let r = exec(h, "SELECT id, v, v[2], len(v) FROM t ORDER BY id;");
    let column = pinkdb_result_column_type(r, 0, 1);
    assert_eq!(column, array_tag(3));
    assert_eq!(array_tag_len(column), 3);

    let v = pinkdb_result_value(r, 0, 0, 1);
    assert_eq!((v.type_tag, v.int64), (array_tag(3), 3));
    let got = floats(&v);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0], 1.5);
    assert_eq!(got[1].to_bits(), (-0.0f64).to_bits(), "-0.0 keeps its bits");
    assert_eq!(got[2], 2.0);
    // Element access and length are ordinary scalars next to the array.
    let v = pinkdb_result_value(r, 0, 0, 2);
    assert_eq!(
        (v.type_tag, v.double.to_bits()),
        (PINKDB_TYPE_DOUBLE, (-0.0f64).to_bits())
    );
    let v = pinkdb_result_value(r, 0, 0, 3);
    assert_eq!((v.type_tag, v.int64), (PINKDB_TYPE_INTEGER, 3));

    // A NULL cell has no value tag of its own (the column type does), and a
    // NULL array carries no payload.
    let v = pinkdb_result_value(r, 0, 1, 1);
    assert_eq!(
        (v.type_tag, v.is_null, v.text_len),
        (PINKDB_TYPE_NULL, 1, 0)
    );
    let v = pinkdb_result_value(r, 0, 1, 3);
    assert_eq!((v.type_tag, v.is_null), (PINKDB_TYPE_NULL, 1));

    // A NULL element has no channel, and an INTEGER array has no payload: both
    // are refused with a type error instead of being guessed at.
    let (msg, class) = cell_err(h, "SELECT [1, 2, 3];");
    assert_eq!(class, "type", "{msg}");
    assert!(msg.contains("only FLOAT and DOUBLE arrays"), "{msg}");
    let (msg, class) = cell_err(h, "SELECT CAST('[1.0, NULL, 3.0]' AS DOUBLE[3]);");
    assert_eq!(class, "type", "{msg}");
    assert!(msg.contains("NULL element"), "{msg}");
    pinkdb_result_free(r);

    // A bound parameter rebuilds the value from the packed tag alone.
    let sql = CString::new("INSERT INTO t VALUES (4, ?);").unwrap();
    let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
    assert!(!prep.is_null(), "{:?}", last_error());
    let bytes: Vec<u8> = [7.0f64, 8.0, 9.0]
        .iter()
        .flat_map(|x| x.to_le_bytes())
        .collect();
    let param = PinkValue {
        type_tag: array_tag(3),
        is_null: 0,
        int64: 3,
        double: 0.0,
        text_ptr: bytes.as_ptr() as *const c_char,
        text_len: bytes.len(),
    };
    let r = pinkdb_execute_prepared(h, prep, &param, 1);
    assert!(!r.is_null(), "{:?}", last_error());
    pinkdb_result_free(r);
    pinkdb_prepared_free(prep);

    // A short payload is refused, not read past its end.
    let prep = pinkdb_prepare(h, sql.as_ptr(), sql.as_bytes().len());
    let short = PinkValue {
        text_len: 16,
        ..param
    };
    let r = pinkdb_execute_prepared(h, prep, &short, 1);
    assert!(r.is_null(), "a short ARRAY payload must be refused");
    assert!(last_error().contains("payload"), "{}", last_error());
    pinkdb_prepared_free(prep);

    let r = exec(h, "SELECT v FROM t WHERE id = 4;");
    assert_eq!(
        floats(&pinkdb_result_value(r, 0, 0, 0)),
        vec![7.0, 8.0, 9.0]
    );
    pinkdb_result_free(r);
    pinkdb_close(h);
}
