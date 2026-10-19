//! End-to-end tests for the core datetime extension through the engine
//! (wave 56): the linked registry → `LOAD EXTENSION 'datetime'` → binder →
//! executor → WAL.
//!
//! Everything here goes through SQL — the same path a user takes — so the
//! tests pin the *observable* semantics (pinned against DuckDB
//! v2.0.0-alpha46057) rather than the pure functions (`src/lib.rs`'s unit
//! tests cover those).

use pinkdb::{DataType, PinkDb, QueryResult, Value};

/// The registry is process-global and one-shot: register once for the whole
/// integration test binary, then `LOAD EXTENSION 'datetime'` per store.
fn registered() {
    pinkdb_datetime::ensure_registered().expect("datetime registers");
}

fn db_with_datetime() -> PinkDb {
    registered();
    let mut db = PinkDb::new();
    db.load_linked_extension("datetime")
        .expect("datetime loads");
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn one(db: &mut PinkDb, sql: &str) -> Value {
    let rows = rows(db, sql);
    assert_eq!(rows.len(), 1, "{sql} -> {rows:?}");
    rows[0][0].clone()
}

fn types(db: &mut PinkDb, sql: &str) -> Vec<DataType> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { types, .. } => types,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn text(db: &mut PinkDb, sql: &str) -> Option<String> {
    match one(db, sql) {
        Value::Varchar(s) => Some(s),
        Value::Null => None,
        other => panic!("expected VARCHAR from {sql}, got {other:?}"),
    }
}

fn bigint(db: &mut PinkDb, sql: &str) -> Option<i64> {
    match one(db, sql) {
        Value::Bigint(i) => Some(i),
        Value::Null => None,
        other => panic!("expected BIGINT from {sql}, got {other:?}"),
    }
}

fn date(db: &mut PinkDb, sql: &str) -> Option<i32> {
    match one(db, sql) {
        Value::Date(d) => Some(d),
        Value::Null => None,
        other => panic!("expected DATE from {sql}, got {other:?}"),
    }
}

fn interval(db: &mut PinkDb, sql: &str) -> Option<(i32, i32, i64)> {
    match one(db, sql) {
        Value::Interval {
            months,
            days,
            micros,
        } => Some((months, days, micros)),
        Value::Null => None,
        other => panic!("expected INTERVAL from {sql}, got {other:?}"),
    }
}

fn error(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err("statement should fail")
        .to_string()
}

// ---------- registration ----------

#[test]
fn load_extension_by_name_registers_the_ten_functions() {
    let mut db = db_with_datetime();
    // All ten are reachable and case-insensitive like every function name.
    assert_eq!(
        bigint(&mut db, "SELECT DAYOFWEEK(DATE '2024-01-07'::TIMESTAMP);"),
        Some(0)
    );
    assert_eq!(
        one(&mut db, "SELECT EPOCH(TIMESTAMP '1970-01-01 00:00:01');"),
        Value::Double(1.0)
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT STRFTIME(DATE '2024-01-07'::TIMESTAMP, '%Y');"
        )
        .as_deref(),
        Some("2024")
    );

    // Loading a second time is the documented duplicate-load error.
    let err = db
        .load_linked_extension("datetime")
        .unwrap_err()
        .to_string();
    assert!(err.contains("already loaded"), "{err}");
}

#[test]
fn the_sql_load_statement_resolves_the_registry_name() {
    let mut db = PinkDb::new();
    let err = error(&mut db, "SELECT epoch_ms(DATE '1970-01-01'::TIMESTAMP);");
    assert!(err.contains("unknown function"), "{err}");

    registered();
    let result = db.execute("LOAD EXTENSION 'datetime';").unwrap();
    assert!(
        matches!(&result[0], QueryResult::LoadedExtension { name } if name == "datetime"),
        "{result:?}"
    );
    assert_eq!(
        bigint(
            &mut db,
            "SELECT epoch_ms(TIMESTAMP '1970-01-01 00:00:00.123');"
        ),
        Some(123)
    );
}

// ---------- the ten functions through SQL ----------

#[test]
fn epoch_and_epoch_ms_match_the_pinned_build() {
    let mut db = db_with_datetime();
    // Fractional seconds; pre-1970 values are negative and rounded tightly.
    assert_eq!(
        one(&mut db, "SELECT epoch(TIMESTAMP '1969-12-31 23:59:59.5');"),
        Value::Double(-0.5)
    );
    assert_eq!(
        one(&mut db, "SELECT epoch(TIMESTAMP '2024-01-01 12:00:00');"),
        Value::Double(1_704_110_400.0)
    );
    assert_eq!(
        bigint(
            &mut db,
            "SELECT epoch_ms(TIMESTAMP '1969-12-31 23:59:59.999999');"
        ),
        Some(0)
    );
    assert_eq!(
        bigint(
            &mut db,
            "SELECT epoch_ms(TIMESTAMP '1969-12-31 23:59:59.9995');"
        ),
        Some(-1)
    );
    assert_eq!(
        bigint(
            &mut db,
            "SELECT epoch_ms(TIMESTAMP '1970-01-01 00:00:00.000500');"
        ),
        Some(1)
    );
    assert_eq!(
        bigint(
            &mut db,
            "SELECT epoch_ms(TIMESTAMP '2024-01-01 00:00:00.000001');"
        ),
        Some(1_704_067_200_000)
    );
    assert_eq!(
        one(&mut db, "SELECT epoch(DATE '2024-01-01'::TIMESTAMP);"),
        Value::Double(1_704_067_200.0)
    );
}

#[test]
fn calendar_fields_match_the_pinned_build() {
    let mut db = db_with_datetime();
    // 0 = Sunday; a DATE goes through the explicit cast the ABI requires.
    assert_eq!(
        bigint(&mut db, "SELECT dayofweek(DATE '2024-01-07'::TIMESTAMP);"),
        Some(0)
    );
    assert_eq!(
        bigint(&mut db, "SELECT dayofweek(DATE '2024-01-08'::TIMESTAMP);"),
        Some(1)
    );
    assert_eq!(
        bigint(
            &mut db,
            "SELECT dayofweek(TIMESTAMP '2024-01-07 12:00:00');"
        ),
        Some(0)
    );
    assert_eq!(
        bigint(&mut db, "SELECT dayofyear(DATE '2024-03-01'::TIMESTAMP);"),
        Some(61)
    );
    assert_eq!(
        bigint(&mut db, "SELECT dayofyear(DATE '2023-03-01'::TIMESTAMP);"),
        Some(60)
    );
    // ISO weeks: 2021-01-01 is 2020-W53, 2024-12-31 is 2025-W01.
    assert_eq!(
        bigint(&mut db, "SELECT week(DATE '2021-01-01'::TIMESTAMP);"),
        Some(53)
    );
    assert_eq!(
        bigint(&mut db, "SELECT week(DATE '2021-01-04'::TIMESTAMP);"),
        Some(1)
    );
    assert_eq!(
        bigint(&mut db, "SELECT week(DATE '2024-12-31'::TIMESTAMP);"),
        Some(1)
    );
}

#[test]
fn last_day_and_make_date_return_dates() {
    let mut db = db_with_datetime();
    assert_eq!(
        types(&mut db, "SELECT last_day(DATE '2024-02-01'::TIMESTAMP);"),
        vec![DataType::Date]
    );
    assert_eq!(
        date(&mut db, "SELECT last_day(TIMESTAMP '2024-02-01 13:14:15');"),
        Some(19_782)
    );
    assert_eq!(
        date(&mut db, "SELECT last_day(DATE '2023-02-01'::TIMESTAMP);"),
        Some(19_416)
    );
    assert_eq!(
        date(&mut db, "SELECT last_day(DATE '2024-01-01'::TIMESTAMP);"),
        Some(19_753)
    );
    assert_eq!(
        date(&mut db, "SELECT make_date(2024, 2, 29);"),
        Some(19_782)
    );
    assert_eq!(
        types(&mut db, "SELECT make_date(2024, 2, 29);"),
        vec![DataType::Date]
    );
    // Out-of-range fields error like DuckDB (nothing was inserted).
    let err = error(&mut db, "SELECT make_date(2023, 2, 29);");
    assert!(err.contains("date out of range: 2023-2-29"), "{err}");
}

#[test]
fn strftime_renders_and_strptime_parses() {
    let mut db = db_with_datetime();
    assert_eq!(
        text(
            &mut db,
            "SELECT strftime(TIMESTAMP '2024-03-05 14:07:09.123456', '%Y-%m-%d %H:%M:%S.%f');"
        )
        .as_deref(),
        Some("2024-03-05 14:07:09.123456")
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT strftime(TIMESTAMP '2024-03-05 14:07:09', '%a %b %d %T %G %V %j');"
        )
        .as_deref(),
        Some("Tue Mar 05 14:07:09 2024 10 065")
    );
    assert_eq!(
        one(
            &mut db,
            "SELECT strptime('2024-03-05 14:07:09.123456', '%Y-%m-%d %H:%M:%S.%f');"
        ),
        Value::Timestamp(1_709_647_629_123_456)
    );
    assert_eq!(
        types(&mut db, "SELECT strptime('2024', '%Y');"),
        vec![DataType::Timestamp]
    );
    // The pinned partial-format defaults: 1900-01-01 00:00:00.
    assert_eq!(
        text(&mut db, "SELECT strptime('07', '%d')::VARCHAR;").as_deref(),
        Some("1900-01-07 00:00:00")
    );
    // Round trip through SQL.
    assert_eq!(
        one(
            &mut db,
            "SELECT (strptime(strftime(TIMESTAMP '2024-03-05 14:07:09.123456', \
             '%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f') \
             = TIMESTAMP '2024-03-05 14:07:09.123456')::INTEGER;"
        ),
        Value::Integer(1)
    );
    // `%e` is not in the supported set; the error names it and the session
    // stays usable.
    let err = error(
        &mut db,
        "SELECT strftime(DATE '2024-01-01'::TIMESTAMP, '%e');",
    );
    assert!(err.contains("unsupported format specifier %e"), "{err}");
    let err = error(&mut db, "SELECT strptime('2024-13-05', '%Y-%m-%d');");
    assert!(err.contains("month 13 out of range"), "{err}");
    assert_eq!(
        bigint(&mut db, "SELECT dayofyear(DATE '2024-03-01'::TIMESTAMP);"),
        Some(61)
    );
}

#[test]
fn age_returns_an_interval_end_to_end() {
    let mut db = db_with_datetime();
    assert_eq!(
        types(
            &mut db,
            "SELECT age(DATE '2024-03-01'::TIMESTAMP, DATE '2023-01-15'::TIMESTAMP);"
        ),
        vec![DataType::Interval]
    );
    assert_eq!(
        interval(
            &mut db,
            "SELECT age(DATE '2024-03-01'::TIMESTAMP, DATE '2023-01-15'::TIMESTAMP);"
        ),
        Some((13, 17, 0))
    );
    // The engine's interval display (wave 62) renders the decomposition.
    assert_eq!(
        text(
            &mut db,
            "SELECT age(DATE '2024-03-01'::TIMESTAMP, DATE '2023-01-15'::TIMESTAMP)::VARCHAR;"
        )
        .as_deref(),
        Some("1 year 1 month 17 days")
    );
    // Time parts borrow a day from the calendar fields.
    assert_eq!(
        interval(
            &mut db,
            "SELECT age(TIMESTAMP '2024-03-01 06:00:00', TIMESTAMP '2024-01-31 18:00:00');"
        ),
        Some((1, 0, 43_200_000_000))
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT age(TIMESTAMP '2024-01-31 18:00:00', TIMESTAMP '2024-03-01 06:00:00')::VARCHAR;"
        )
        .as_deref(),
        Some("-1 month -12:00:00")
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT age(DATE '2024-03-01'::TIMESTAMP, DATE '2024-03-01'::TIMESTAMP)::VARCHAR;"
        )
        .as_deref(),
        Some("00:00:00")
    );
}

// ---------- columns, WHERE, GROUP BY ----------

#[test]
fn functions_work_over_columns_where_and_group_by() {
    let mut db = db_with_datetime();
    db.execute(
        "CREATE TABLE events (id BIGINT, at TIMESTAMP);
         INSERT INTO events VALUES
           (1, TIMESTAMP '2024-01-07 09:00:00'),
           (2, TIMESTAMP '2024-01-08 10:30:00'),
           (3, TIMESTAMP '2024-03-05 14:07:09'),
           (4, TIMESTAMP '2024-03-12 00:00:00');",
    )
    .unwrap();

    // WHERE on a function result.
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM events WHERE week(at) >= 10 ORDER BY id;"
        ),
        vec![vec![Value::Bigint(3)], vec![Value::Bigint(4)]]
    );
    // GROUP BY a rendered bucket, ORDER BY the group key.
    assert_eq!(
        rows(
            &mut db,
            "SELECT strftime(at, '%Y-%m') AS month, COUNT(*) FROM events \
             GROUP BY month ORDER BY month;"
        ),
        vec![
            vec![Value::Varchar("2024-01".to_string()), Value::Bigint(2)],
            vec![Value::Varchar("2024-03".to_string()), Value::Bigint(2)],
        ]
    );
    // GROUP BY a calendar field, filtered (March rows are past day 61).
    assert_eq!(
        rows(
            &mut db,
            "SELECT dayofweek(at) AS dow, COUNT(*) FROM events \
             WHERE dayofyear(at) <= 61 GROUP BY dow ORDER BY dow;"
        ),
        vec![
            vec![Value::Bigint(0), Value::Bigint(1)],
            vec![Value::Bigint(1), Value::Bigint(1)],
        ]
    );
    // make_date feeding a comparison, and last_day in a projection.
    assert_eq!(
        rows(
            &mut db,
            "SELECT id, last_day(at) FROM events WHERE at >= make_date(2024, 3, 1)::TIMESTAMP \
             ORDER BY id;"
        ),
        vec![
            vec![Value::Bigint(3), Value::Date(19_813)],
            vec![Value::Bigint(4), Value::Date(19_813)],
        ]
    );
}

// ---------- NULL matrix ----------

#[test]
fn nulls_flow_through_every_function() {
    let mut db = db_with_datetime();
    let null_ts = "NULL";
    let null_str = "NULL";
    let null_int = "NULL";
    for sql in [
        format!("SELECT epoch({null_ts});"),
        format!("SELECT epoch_ms({null_ts});"),
        format!("SELECT dayofweek({null_ts});"),
        format!("SELECT dayofyear({null_ts});"),
        format!("SELECT week({null_ts});"),
        format!("SELECT last_day({null_ts});"),
        format!("SELECT strftime({null_ts}, '%Y');"),
        format!("SELECT strftime(DATE '2024-01-01'::TIMESTAMP, {null_str});"),
        format!("SELECT strptime({null_str}, '%Y');"),
        format!("SELECT strptime('2024', {null_str});"),
        format!("SELECT age({null_ts}, DATE '2024-01-01'::TIMESTAMP);"),
        format!("SELECT age(DATE '2024-01-01'::TIMESTAMP, {null_ts});"),
        format!("SELECT make_date({null_int}, 1, 1);"),
        format!("SELECT make_date(2024, {null_int}, 1);"),
        format!("SELECT make_date(2024, 1, {null_int});"),
    ] {
        assert_eq!(one(&mut db, &sql), Value::Null, "{sql}");
    }
    // A bare NULL literal binds (the ABI passes the declared type).
    assert_eq!(one(&mut db, "SELECT epoch_ms(NULL);"), Value::Null);
    // NULLs in a column, over rows.
    db.execute(
        "CREATE TABLE t (id BIGINT, at TIMESTAMP);
         INSERT INTO t VALUES (1, DATE '2024-01-01'::TIMESTAMP), (2, NULL);",
    )
    .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT id, dayofyear(at) FROM t ORDER BY id;"),
        vec![
            vec![Value::Bigint(1), Value::Bigint(1)],
            vec![Value::Bigint(2), Value::Null],
        ]
    );
    // A NULL timestamp is NULL before the format is even validated.
    assert_eq!(one(&mut db, "SELECT strftime(NULL, '%e');"), Value::Null);
}

// ---------- prepared statements ----------

#[test]
fn prepared_statements_bind_timestamps_formats_and_dates() {
    let mut db = db_with_datetime();

    let stmt = db
        .prepare("SELECT epoch_ms(?), dayofweek(?), week(?);")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 3);
    let result = db
        .execute_prepared(
            &stmt,
            &[
                Value::Timestamp(1_709_647_629_123_456),
                Value::Timestamp(19_729 * 86_400_000_000),
                Value::Timestamp(19_723 * 86_400_000_000),
            ],
        )
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(
            rows,
            vec![vec![
                Value::Bigint(1_709_647_629_123),
                Value::Bigint(0),
                Value::Bigint(1)
            ]]
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    let stmt = db
        .prepare("SELECT strftime(?, ?), strptime(?, ?);")
        .unwrap();
    let result = db
        .execute_prepared(
            &stmt,
            &[
                Value::Timestamp(0),
                Value::Varchar("%Y-%m-%d %H:%M:%S".to_string()),
                Value::Varchar("2024-03-05 14:07:09".to_string()),
                Value::Varchar("%Y-%m-%d %H:%M:%S".to_string()),
            ],
        )
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(
            rows,
            vec![vec![
                Value::Varchar("1970-01-01 00:00:00".to_string()),
                Value::Timestamp(1_709_647_629_000_000),
            ]]
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    let stmt = db.prepare("SELECT age(?, ?), make_date(?, ?, ?);").unwrap();
    let result = db
        .execute_prepared(
            &stmt,
            &[
                Value::Timestamp(19_783 * 86_400_000_000),
                Value::Timestamp(19_372 * 86_400_000_000),
                Value::Integer(2024),
                Value::Integer(2),
                Value::Integer(29),
            ],
        )
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(
            rows,
            vec![vec![
                Value::Interval {
                    months: 13,
                    days: 17,
                    micros: 0
                },
                Value::Date(19_782),
            ]]
        ),
        other => panic!("expected rows, got {other:?}"),
    }
}

// ---------- errors ----------

#[test]
fn failing_calls_leave_the_session_usable() {
    let mut db = db_with_datetime();
    for (sql, needle) in [
        (
            "SELECT strftime(DATE '2024-01-01'::TIMESTAMP, '%z');",
            "no time zones",
        ),
        (
            "SELECT strftime(DATE '2024-01-01'::TIMESTAMP, 'x%');",
            "trailing '%'",
        ),
        (
            "SELECT strptime('2024-13-05', '%Y-%m-%d');",
            "month 13 out of range",
        ),
        ("SELECT make_date(2023, 2, 29);", "date out of range"),
        ("SELECT make_date(6000000, 1, 1);", "date out of range"),
    ] {
        let err = error(&mut db, sql);
        assert!(err.contains(needle), "{sql}: {err}");
    }
    // The session still works.
    assert_eq!(
        bigint(&mut db, "SELECT epoch_ms(DATE '1970-01-01'::TIMESTAMP);"),
        Some(0)
    );
}

// ---------- durability ----------

/// A durable record that calls a datetime function is prefixed with
/// `LOAD EXTENSION 'datetime'`; reopening replays it through the registry
/// (the extension is registered in this process — the linked model) and the
/// rows come back.
#[test]
fn durable_records_touching_datetime_replay() {
    registered();
    let dir = std::env::temp_dir().join(format!("pinkdb_datetime_wal_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();

    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.load_linked_extension("datetime").unwrap();
        db.execute("CREATE TABLE t (id BIGINT, at TIMESTAMP);")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES (1, TIMESTAMP '2024-01-01 00:00:00.123456'), \
             (2, TIMESTAMP '1969-12-31 23:59:59.9995');",
        )
        .unwrap();
        // A record that calls the extension (needs the WAL prefix), in a
        // statement the engine logs: deterministic functions are allowed.
        db.execute("UPDATE t SET id = id + dayofyear(at);").unwrap();
        db.checkpoint().unwrap();
        // After the checkpoint the next touching record is prefixed again.
        db.execute("INSERT INTO t VALUES (epoch_ms(TIMESTAMP '1970-01-01 00:00:01'), TIMESTAMP '2024-03-01 00:00:00');")
            .unwrap();
        db.flush_wal().unwrap();
    }

    {
        // Reopen with the extension only published (not loaded in the store):
        // replay resolves `LOAD EXTENSION 'datetime'` through the registry.
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        assert_eq!(
            rows(&mut db, "SELECT id FROM t ORDER BY id;"),
            vec![
                vec![Value::Bigint(2)],
                vec![Value::Bigint(367)],
                vec![Value::Bigint(1_000)],
            ]
        );
        assert_eq!(
            text(&mut db, "SELECT strftime(at, '%Y-%m') FROM t WHERE id = 2;").as_deref(),
            Some("2024-01")
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
