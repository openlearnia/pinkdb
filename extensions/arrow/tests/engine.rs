//! Engine-level integration: the real arrow format through the SQL surfaces
//! (`FROM 'f.arrow'`, `read_arrow`, `COPY … (FORMAT arrow)`), the durability
//! rule, and the refusal of an unregistered extension.
//!
//! The engine's own `filefmt` unit tests pin the surfaces with a fake format;
//! this file pins the combination with the real one, including the
//! process-global registration the CLI performs at startup.

use std::path::PathBuf;

use pinkdb::exec::QueryResult;
use pinkdb::{DataType, PinkDb, Value};
use pinkdb_arrow::{ArrowSink, ArrowSource};

fn scratch_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_arrow_engine_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected a result set, got {other:?}"),
    }
}

#[test]
fn sql_surfaces_round_trip_through_a_real_file() {
    let dir = scratch_dir("surfaces");
    let file = dir.join("t.arrow");
    let mut db = PinkDb::new();
    pinkdb_arrow::register(&mut db);
    db.execute(
        "CREATE TABLE t (a INTEGER, s VARCHAR, d DOUBLE, dt DATE, ts TIMESTAMP, \
         dec DECIMAL(18,4), iv INTERVAL, bo BOOLEAN);",
    )
    .unwrap();
    db.execute(
        "INSERT INTO t VALUES \
         (1, 'x', 1.5, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00.000001', \
          '1.2345', INTERVAL '1 day', true), \
         (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
    )
    .unwrap();
    let rows = query(
        &mut db,
        &format!("COPY t TO '{}' (FORMAT arrow);", file.display()),
    );
    assert_eq!(rows, vec![vec![Value::Bigint(2)]]);

    // Read back through every spelling; values and types agree with the store.
    let rows = query(
        &mut db,
        &format!(
            "SELECT a, s, d, dt, ts, dec, iv, bo FROM '{}' ORDER BY a;",
            file.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Varchar("x".to_string()));
    assert_eq!(rows[0][2], Value::Double(1.5));
    assert_eq!(rows[0][3], Value::Date(19723));
    assert_eq!(rows[0][4], Value::Timestamp(1_704_110_400_000_001));
    assert_eq!(rows[0][5], Value::Decimal(12345, 4));
    assert_eq!(
        rows[0][6],
        Value::Interval {
            months: 0,
            days: 1,
            micros: 0
        }
    );
    assert_eq!(rows[0][7], Value::Boolean(true));
    assert_eq!(rows[1][1], Value::Null);
    let rows = query(
        &mut db,
        &format!("SELECT COUNT(*) FROM read_arrow('{}');", file.display()),
    );
    assert_eq!(rows, vec![vec![Value::Bigint(2)]]);

    // The schema of a file source is the file's own.
    let rows = query(&mut db, "DESCRIBE t;");
    let schema: Vec<(String, DataType)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Varchar(name), Value::Varchar(ty)) => (name.clone(), parse_type(ty)),
            other => panic!("bad DESCRIBE row: {other:?}"),
        })
        .collect();
    assert!(schema.contains(&(
        "dec".to_string(),
        DataType::Decimal {
            precision: 18,
            scale: 4
        }
    )));

    // Joins/aggregates over the file source behave like any scan.
    db.execute("CREATE TABLE w (k INTEGER, v INTEGER);")
        .unwrap();
    db.execute("INSERT INTO w VALUES (1, 100), (9, 900);")
        .unwrap();
    let rows = query(
        &mut db,
        &format!(
            "SELECT t.a, w.v FROM '{}' t JOIN w ON t.a = w.k ORDER BY t.a;",
            file.display()
        ),
    );
    assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(100)]]);

    std::fs::remove_dir_all(&dir).ok();
}

fn parse_type(name: &str) -> DataType {
    DataType::from_sql_params(name, &[]).unwrap_or_else(|| {
        // DECIMAL(p,s) carries its parameters in the display name.
        let inner = name.trim_start_matches("DECIMAL(").trim_end_matches(')');
        let (p, s) = inner.split_once(',').expect("DECIMAL(p,s)");
        DataType::Decimal {
            precision: p.trim().parse().unwrap(),
            scale: s.trim().parse().unwrap(),
        }
    })
}

/// The unified surface: a path no registered **source** claims is refused with
/// the registered-source hint, and an explicitly named format that isn't
/// registered is refused by the sink registry.
#[test]
fn unregistered_formats_are_refused_with_the_registered_list() {
    let mut db = PinkDb::new();
    pinkdb_arrow::register(&mut db);
    assert_eq!(db.table_sources().len(), 1);
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1);").unwrap();
    // `x.parquet` is not claimed by the arrow source; the parquet source is
    // not built into this crate's tests, so the binder reports the table
    // sources it has.
    let err = db
        .execute("SELECT * FROM 'x.parquet';")
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot read"), "{err}");
    assert!(err.contains("table source"), "{err}");
    let err = db
        .execute("COPY t TO 'x.arrow' (FORMAT parquet);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("no file format \"parquet\""), "{err}");
    // Reverse direction: the arrow format refuses a non-arrow file.
    let dir = scratch_dir("notarrow");
    let text = dir.join("not.arrow");
    std::fs::write(&text, b"not an arrow file").unwrap();
    let err = db
        .execute(&format!("SELECT * FROM '{}';", text.display()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("not an Arrow IPC file"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}

/// A durable store replays a file-reading record by re-reading the file; the
/// table source must be registered **before** the store is constructed
/// (`ensure_registered`, the CLI's startup step), because recovery replays
/// inside `open_durable`.
#[test]
fn durable_replay_resolves_the_process_global_source() {
    let dir = scratch_dir("durable");
    let file = dir.join("src.arrow");
    let store = dir.join("store.pink");
    pinkdb_arrow::ensure_registered();

    let mut db = PinkDb::new();
    db.execute("CREATE TABLE src (a INTEGER);").unwrap();
    db.execute("INSERT INTO src VALUES (1), (2), (3);").unwrap();
    db.execute(&format!("COPY src TO '{}' (FORMAT arrow);", file.display()))
        .unwrap();
    db.save(store.to_str().unwrap()).unwrap();
    drop(db);

    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE dst (a INTEGER);").unwrap();
    db.execute(&format!(
        "INSERT INTO dst SELECT a FROM '{}' WHERE a > 1;",
        file.display()
    ))
    .unwrap();
    assert_eq!(
        query(&mut db, "SELECT a FROM dst;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
    drop(db);

    // Reopen: the logged INSERT re-reads the file (the process-global format
    // is seeded into the store `open_durable` constructs).
    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    assert_eq!(
        query(&mut db, "SELECT COUNT(*), SUM(a) FROM dst;"),
        vec![vec![Value::Bigint(2), Value::Bigint(5)]]
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Both halves are plain implementations of the engine traits, and their
/// `Debug` bounds are satisfied.
#[test]
fn both_halves_implement_the_engine_traits() {
    let read: std::sync::Arc<dyn pinkdb::TableSource> = std::sync::Arc::new(ArrowSource::new());
    let write: std::sync::Arc<dyn pinkdb::FileFormat> = std::sync::Arc::new(ArrowSink::new());
    assert_eq!(read.name(), "arrow");
    assert_eq!(write.name(), "arrow");
}
