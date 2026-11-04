//! The buffered appender flushes through the INSERT executor, so these tests
//! pin that it inherits its guarantees: coercion, keys, triggers, indexes,
//! transactions, and WAL durability. The buffer is columnar and borrows no
//! connection, so a query may run between appends.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::exec::ChunkedResult;
use pinkdb::{Connection, DataType, Database, QueryResult, Value, Vector};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_appender_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("db.pink");
        Self { dir, file }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn ok(conn: &mut Connection, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn count(conn: &mut Connection, sql: &str) -> i64 {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => match &rows[0][0] {
            Value::Bigint(n) => *n,
            Value::Integer(n) => *n as i64,
            Value::Hugeint(n) => i64::try_from(*n).unwrap(),
            other => panic!("expected an integer, got {other:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

fn varchar(s: &str) -> Value {
    Value::Varchar(s.to_string())
}

#[test]
fn appends_rows_and_chunks_with_coercion() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(&mut conn, "CREATE TABLE t (a INTEGER, b VARCHAR, c DOUBLE)");
    let mut app = conn.appender(None, "t").unwrap();
    assert_eq!(app.column_count(), 3);
    app.append_row(
        &mut conn,
        vec![Value::Bigint(1), varchar("one"), Value::Double(1.5)],
    )
    .unwrap();
    app.append_row(
        &mut conn,
        vec![Value::Integer(2), Value::Null, Value::Bigint(2)],
    )
    .unwrap();
    app.flush(&mut conn).unwrap();

    // A fetched chunk feeds `append_chunk` directly.
    let chunks = match conn
        .execute_chunked("SELECT a, b, c FROM t ORDER BY a")
        .unwrap()
        .pop()
        .unwrap()
    {
        ChunkedResult::Selected { chunks, .. } => chunks,
        _ => panic!("expected chunks"),
    };
    ok(
        &mut conn,
        "CREATE TABLE copy (a INTEGER, b VARCHAR, c DOUBLE)",
    );
    let mut app = conn.appender(None, "copy").unwrap();
    app.append_chunk(
        &mut conn,
        chunks[0]
            .columns
            .iter()
            .map(|column| (**column).clone())
            .collect(),
    )
    .unwrap();
    app.close(&mut conn).unwrap();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM copy"), 2);
    let rows = match conn
        .execute("SELECT a, b, c FROM copy ORDER BY a")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(
        rows[0],
        vec![Value::Integer(1), varchar("one"), Value::Double(1.5)]
    );
    assert_eq!(
        rows[1],
        vec![Value::Integer(2), Value::Null, Value::Double(2.0)]
    );
}

#[test]
fn a_rejected_flush_keeps_none_of_its_rows() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(
        &mut conn,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
    );
    ok(&mut conn, "INSERT INTO t VALUES (1, 10)");
    let mut app = conn.appender(None, "t").unwrap();
    app.set_auto_flush(100);
    app.append_row(&mut conn, vec![Value::Integer(2), Value::Integer(20)])
        .unwrap();
    app.append_row(&mut conn, vec![Value::Integer(1), Value::Integer(30)])
        .unwrap();
    let err = format!("{}", app.flush(&mut conn).unwrap_err());
    assert!(err.to_ascii_lowercase().contains("duplicate key"), "{err}");
    drop(app);
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 1);
    let mut app = conn.appender(None, "t").unwrap();
    app.set_auto_flush(100);

    // The rejected statement is atomic: a UNIQUE violation late in the batch
    // keeps none of the earlier rows either.
    app.append_row(&mut conn, vec![Value::Integer(3), Value::Integer(40)])
        .unwrap();
    app.append_row(&mut conn, vec![Value::Integer(4), Value::Integer(40)])
        .unwrap();
    let err = format!("{}", app.flush(&mut conn).unwrap_err());
    assert!(err.to_ascii_lowercase().contains("duplicate key"), "{err}");
    drop(app);
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 1);
    let mut app = conn.appender(None, "t").unwrap();
    app.set_auto_flush(100);

    // PRIMARY KEY implies NOT NULL on this path.
    app.append_row(&mut conn, vec![Value::Null, Value::Integer(50)])
        .unwrap();
    let err = format!("{}", app.flush(&mut conn).unwrap_err());
    assert!(err.contains("NOT NULL"), "{err}");
    drop(app);
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 1);

    // A fresh appender works after rejected flushes.
    let mut app = conn.appender(None, "t").unwrap();
    app.append_row(&mut conn, vec![Value::Integer(5), Value::Null])
        .unwrap();
    app.close(&mut conn).unwrap();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 2);
}

#[test]
fn appends_maintain_indexes_and_fire_triggers() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(
        &mut conn,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR)",
    );
    ok(&mut conn, "CREATE INDEX t_v ON t (v)");
    ok(&mut conn, "CREATE TABLE log (id INTEGER)");
    ok(
        &mut conn,
        "CREATE TRIGGER t_row AFTER INSERT ON t FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES (NEW.id)",
    );
    let mut app = conn.appender(None, "t").unwrap();
    app.set_auto_flush(2);
    for i in 1..=3 {
        app.append_row(
            &mut conn,
            vec![Value::Integer(i), varchar(&format!("v{i}"))],
        )
        .unwrap();
    }
    app.close(&mut conn).unwrap();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 3);
    assert_eq!(count(&mut conn, "SELECT count(*) FROM log"), 3);
    let rows = match conn
        .execute("SELECT id FROM t WHERE v = 'v2'")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn auto_flush_commits_at_its_boundary() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(&mut conn, "CREATE TABLE t (id INTEGER)");
    {
        let mut app = conn.appender(None, "t").unwrap();
        app.set_auto_flush(2);
        for i in 0..5 {
            app.append_row(&mut conn, vec![Value::Integer(i)]).unwrap();
        }
        assert_eq!(app.pending_rows(), 1);
        app.flush(&mut conn).unwrap();
        assert_eq!(app.pending_rows(), 0);
    }
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 5);
}

#[test]
fn wrong_widths_are_errors_and_buffer_nothing() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(&mut conn, "CREATE TABLE t (a INTEGER, b INTEGER)");
    let mut app = conn.appender(None, "t").unwrap();
    let err = format!(
        "{}",
        app.append_row(&mut conn, vec![Value::Integer(1)])
            .unwrap_err()
    );
    assert!(err.contains("has 2 columns"), "{err}");
    let err = format!(
        "{}",
        app.append_chunk(
            &mut conn,
            vec![pinkdb::Vector::constant(DataType::Integer, &Value::Integer(1), 3,).unwrap()]
        )
        .unwrap_err()
    );
    assert!(err.contains("has 2 columns"), "{err}");
    assert_eq!(app.pending_rows(), 0);
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 0);
}

#[test]
fn appends_join_and_roll_back_with_the_open_transaction() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(&mut conn, "CREATE TABLE t (id INTEGER)");
    ok(&mut conn, "BEGIN");
    {
        let mut app = conn.appender(None, "t").unwrap();
        app.append_row(&mut conn, vec![Value::Integer(1)]).unwrap();
        app.append_row(&mut conn, vec![Value::Integer(2)]).unwrap();
        app.close(&mut conn).unwrap();
    }
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 2);
    ok(&mut conn, "ROLLBACK");
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 0);

    ok(&mut conn, "BEGIN");
    {
        let mut app = conn.appender(None, "t").unwrap();
        app.append_row(&mut conn, vec![Value::Integer(3)]).unwrap();
        app.close(&mut conn).unwrap();
    }
    ok(&mut conn, "COMMIT");
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 1);
}

#[test]
fn durable_appends_survive_replay() {
    let scratch = Scratch::new("durable");
    let path = scratch.file.to_str().unwrap();
    {
        let db = Database::open(path).unwrap();
        let mut conn = db.connect();
        ok(
            &mut conn,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR)",
        );
        {
            let mut app = conn.appender(None, "t").unwrap();
            app.append_row(&mut conn, vec![Value::Integer(1), varchar("one")])
                .unwrap();
            app.append_row(&mut conn, vec![Value::Integer(2), varchar("two")])
                .unwrap();
            app.close(&mut conn).unwrap();
        }
    }
    let db = Database::open(path).unwrap();
    let mut conn = db.connect();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 2);
    let rows = match conn
        .execute("SELECT v FROM t ORDER BY id")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(rows, vec![vec![varchar("one")], vec![varchar("two")]]);
}

#[test]
fn queries_between_appends_see_only_flushed_rows() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(&mut conn, "CREATE TABLE t (id INTEGER)");
    let mut app = conn.appender(None, "t").unwrap();
    app.set_auto_flush(100);
    app.append_row(&mut conn, vec![Value::Integer(1)]).unwrap();
    // The buffered row is not part of the store yet.
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 0);
    app.flush(&mut conn).unwrap();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 1);
    // The appender stays usable after a query on the same connection.
    app.append_row(&mut conn, vec![Value::Integer(2)]).unwrap();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 1);
    app.close(&mut conn).unwrap();
    assert_eq!(count(&mut conn, "SELECT count(*) FROM t"), 2);
}

fn all_type_wanted() -> Vec<Value> {
    vec![
        Value::Boolean(true),
        Value::Integer(-7),
        Value::Bigint(9_000_000_000),
        Value::Double(1.5),
        varchar("hé"),
        Value::Date(19_000),
        Value::Timestamp(1_700_000_000_000_000),
        Value::Interval {
            months: 1,
            days: 2,
            micros: 3,
        },
        Value::Hugeint(1i128 << 100),
        Value::Uhugeint(u128::MAX - 1),
        Value::Decimal {
            value: 12_345,
            precision: 10,
            scale: 2,
        },
        Value::Uuid(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
    ]
}

fn all_type_columns() -> Vec<Vector> {
    let wanted = all_type_wanted();
    let constant = |dtype, value| Vector::constant(dtype, &value, 2).unwrap();
    vec![
        constant(DataType::Boolean, wanted[0].clone()),
        constant(DataType::Integer, wanted[1].clone()),
        constant(DataType::Bigint, wanted[2].clone()),
        constant(DataType::Double, wanted[3].clone()),
        constant(DataType::Varchar, wanted[4].clone()),
        constant(DataType::Date, wanted[5].clone()),
        constant(DataType::Timestamp, wanted[6].clone()),
        constant(DataType::Interval, wanted[7].clone()),
        constant(DataType::Hugeint, wanted[8].clone()),
        constant(DataType::Uhugeint, wanted[9].clone()),
        constant(
            DataType::Decimal {
                precision: 10,
                scale: 2,
            },
            wanted[10].clone(),
        ),
        constant(DataType::Uuid, wanted[11].clone()),
    ]
}

fn all_type_rows(conn: &mut Connection) -> Vec<Vec<Value>> {
    match conn
        .execute("SELECT * FROM all_types ORDER BY i")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn append_chunk_carries_every_scalar_type() {
    let db = Database::new();
    let mut conn = db.connect();
    ok(
        &mut conn,
        "CREATE TABLE all_types (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, \
         v VARCHAR, dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT, \
         uh UHUGEINT, dec DECIMAL(10,2), u UUID)",
    );
    let mut app = conn.appender(None, "all_types").unwrap();
    app.append_chunk(&mut conn, all_type_columns()).unwrap();
    app.close(&mut conn).unwrap();
    let wanted = all_type_wanted();
    let rows = all_type_rows(&mut conn);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], wanted);
    assert_eq!(rows[1], wanted);
}

#[test]
fn durable_chunk_appends_replay_every_scalar_type() {
    let scratch = Scratch::new("durable_all_types");
    {
        let db = Database::open(scratch.file.to_str().unwrap()).unwrap();
        let mut conn = db.connect();
        ok(
            &mut conn,
            "CREATE TABLE all_types (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, \
             v VARCHAR, dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT, \
             uh UHUGEINT, dec DECIMAL(10,2), u UUID)",
        );
        let mut app = conn.appender(None, "all_types").unwrap();
        app.append_chunk(&mut conn, all_type_columns()).unwrap();
        app.close(&mut conn).unwrap();
    }
    // Reopening replays the WAL text rendered for the chunk flush.
    let db = Database::open(scratch.file.to_str().unwrap()).unwrap();
    let mut conn = db.connect();
    let wanted = all_type_wanted();
    let rows = all_type_rows(&mut conn);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], wanted);
}
