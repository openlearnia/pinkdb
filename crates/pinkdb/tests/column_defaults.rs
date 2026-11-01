//! Wave 139a: a column `DEFAULT` is an expression, evaluated for every row
//! that takes it.
//!
//! `CREATE TABLE` binds the expression (and refuses columns, subqueries,
//! aggregates and window functions, like DuckDB) but never runs it: a failing
//! conversion fails when a row takes the default. Values that depend on the
//! statement (`now()`, `CURRENT_TIMESTAMP`) read the statement's one clock
//! instant, but each `INSERT`/`UPDATE`/`MERGE`/`ADD COLUMN` evaluates them
//! again.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_column_defaults_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("db.pink");
        Self { dir, file }
    }

    fn path(&self) -> &str {
        self.file.to_str().unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn open(scratch: &Scratch) -> PinkDb {
    PinkDb::open_durable_with(scratch.path(), SyncPolicy::Sync)
        .unwrap()
        .0
}

fn run(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).map(|_| ()).unwrap_err().to_string()
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn timestamp(value: &Value) -> i64 {
    match value {
        Value::Timestamp(t) => *t,
        other => panic!("expected TIMESTAMP, got {other:?}"),
    }
}

fn cell_text(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

#[test]
fn a_clock_default_reads_the_statements_instant_once_per_statement() {
    let mut db = PinkDb::new();
    run(
        &mut db,
        "CREATE TABLE t (a INTEGER, ts TIMESTAMP DEFAULT now());",
    );
    run(&mut db, "INSERT INTO t (a) VALUES (1), (2), (3);");
    let first = rows(&mut db, "SELECT ts FROM t ORDER BY a;");
    assert_eq!(first.len(), 3);
    let instant = timestamp(&first[0][0]);
    assert!(
        first.iter().all(|r| timestamp(&r[0]) == instant),
        "one statement must see one clock instant: {first:?}"
    );

    std::thread::sleep(std::time::Duration::from_millis(3));
    run(&mut db, "INSERT INTO t (a) VALUES (4);");
    let later = timestamp(&rows(&mut db, "SELECT ts FROM t WHERE a = 4;")[0][0]);
    assert!(later > instant, "{instant} -> {later}");

    // CURRENT_TIMESTAMP is the same clock reading.
    run(
        &mut db,
        "CREATE TABLE c (ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
    );
    run(&mut db, "INSERT INTO c DEFAULT VALUES;");
    run(&mut db, "INSERT INTO c DEFAULT VALUES;");
    let two = rows(&mut db, "SELECT ts FROM c ORDER BY ts;");
    assert!(timestamp(&two[1][0]) > timestamp(&two[0][0]), "{two:?}");
}

#[test]
fn a_default_that_cannot_convert_is_accepted_at_create_and_fails_at_insert() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE q (b TIMESTAMP DEFAULT 'now()');");
    let describe = rows(&mut db, "DESCRIBE q;");
    assert_eq!(cell_text(&describe[0][4]), "'now()'");
    let e = err(&mut db, "INSERT INTO q DEFAULT VALUES;");
    assert!(e.contains("now()"), "{e}");
    // The conversion error names the column type, like any failing CAST.
    assert!(e.to_ascii_lowercase().contains("timestamp"), "{e}");
}

#[test]
fn duckdb_refusals_for_default_expressions() {
    let mut db = PinkDb::new();
    let cases = [
        (
            "CREATE TABLE r (a INTEGER DEFAULT (SELECT 1));",
            "DEFAULT value cannot contain subqueries",
        ),
        (
            "CREATE TABLE s (a INTEGER, b INTEGER DEFAULT a);",
            "DEFAULT value cannot contain column names",
        ),
        (
            "CREATE TABLE g (a INTEGER DEFAULT sum(1));",
            "DEFAULT value cannot contain aggregates!",
        ),
        (
            "CREATE TABLE w (a INTEGER DEFAULT row_number() OVER ());",
            "DEFAULT value cannot contain window functions!",
        ),
    ];
    for (sql, want) in cases {
        let e = err(&mut db, sql);
        assert_eq!(e, want, "{sql}");
    }
    // DuckDB's precedence: a subquery is reported before the column it wraps.
    let e = err(
        &mut db,
        "CREATE TABLE p (a INTEGER DEFAULT ((SELECT 1) + a));",
    );
    assert_eq!(e, "DEFAULT value cannot contain subqueries");

    // The same refusals apply to ALTER TABLE … ADD COLUMN.
    run(&mut db, "CREATE TABLE o (a INTEGER);");
    let e = err(&mut db, "ALTER TABLE o ADD COLUMN b INTEGER DEFAULT a + 1;");
    assert_eq!(e, "DEFAULT value cannot contain column names");
}

#[test]
fn a_volatile_default_is_evaluated_per_row() {
    let mut db = PinkDb::new();
    // Since wave 139b the statement's seed rides in the WAL memo, so a
    // volatile DEFAULT is legal in a logged write and each row gets its own
    // value.
    run(
        &mut db,
        "CREATE TABLE t (id UUID DEFAULT uuid(), a INTEGER);",
    );
    run(&mut db, "INSERT INTO t (a) VALUES (1), (2), (3);");
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*), count(DISTINCT id), typeof(min(id)) FROM t;"
        ),
        vec![vec![
            Value::Bigint(3),
            Value::Bigint(3),
            Value::Varchar("UUID".to_string())
        ]]
    );
    // A read-only use stays fine.
    assert_eq!(
        rows(&mut db, "SELECT length(uuid()::VARCHAR);"),
        vec![vec![Value::Integer(36)]]
    );
}

#[test]
fn the_default_keyword_in_values_and_update() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (a INTEGER DEFAULT 5, b INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (DEFAULT, DEFAULT);");
    run(&mut db, "INSERT INTO t (b) VALUES (DEFAULT);");
    assert_eq!(
        rows(&mut db, "SELECT a, b FROM t ORDER BY b NULLS FIRST;"),
        vec![
            vec![Value::Integer(5), Value::Null],
            vec![Value::Integer(5), Value::Null],
        ]
    );
    run(&mut db, "INSERT INTO t VALUES (1, 2);");
    run(&mut db, "INSERT INTO t VALUES (3, DEFAULT), (DEFAULT, 4);");
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM t WHERE a = 5 AND b IS NULL;"),
        vec![vec![Value::Bigint(2)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT a FROM t WHERE b = 4;"),
        vec![vec![Value::Integer(5)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT a FROM t WHERE b = 2;"),
        vec![vec![Value::Integer(1)]]
    );

    run(&mut db, "UPDATE t SET a = DEFAULT WHERE b = 2;");
    assert_eq!(
        rows(&mut db, "SELECT a FROM t WHERE b = 2;"),
        vec![vec![Value::Integer(5)]]
    );
    // A column without a default takes NULL from DEFAULT.
    run(&mut db, "CREATE TABLE n (a INTEGER, b INTEGER);");
    run(&mut db, "INSERT INTO n VALUES (1, 2);");
    run(&mut db, "UPDATE n SET b = DEFAULT;");
    assert_eq!(rows(&mut db, "SELECT b FROM n;"), vec![vec![Value::Null]]);
    // DuckDB's refusal for the nonsensical combination.
    let e = err(&mut db, "INSERT INTO t (a) DEFAULT VALUES;");
    assert!(e.contains("column list and DEFAULT VALUES"), "{e}");
    // DEFAULT is not a general expression: outside a VALUES row or SET value
    // it stays an identifier and does not resolve.
    let e = err(&mut db, "SELECT * FROM t WHERE a = DEFAULT;");
    assert!(e.contains("DEFAULT"), "{e}");
}

#[test]
fn default_values_inserts_one_row_of_defaults() {
    let mut db = PinkDb::new();
    run(
        &mut db,
        "CREATE TABLE u (i INTEGER DEFAULT 0, d DECIMAL(10,2) DEFAULT 1.5, \
         s VARCHAR DEFAULT 'x''y');",
    );
    run(&mut db, "INSERT INTO u DEFAULT VALUES;");
    run(&mut db, "INSERT INTO u DEFAULT VALUES;");
    let out = rows(&mut db, "SELECT i, d, s FROM u;");
    assert_eq!(
        out,
        vec![
            vec![
                Value::Integer(0),
                Value::decimal(150, 2),
                Value::Varchar("x'y".into())
            ],
            vec![
                Value::Integer(0),
                Value::decimal(150, 2),
                Value::Varchar("x'y".into())
            ],
        ]
    );
}

#[test]
fn merge_insert_and_insert_select_take_omitted_defaults() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE m (a INTEGER, b INTEGER DEFAULT 5);");
    run(&mut db, "CREATE TABLE src (a INTEGER);");
    run(&mut db, "INSERT INTO src VALUES (1), (2);");
    run(
        &mut db,
        "MERGE INTO m USING src ON m.a = src.a \
         WHEN NOT MATCHED THEN INSERT (a) VALUES (src.a);",
    );
    assert_eq!(
        rows(&mut db, "SELECT a, b FROM m ORDER BY a;"),
        vec![
            vec![Value::Integer(1), Value::Integer(5)],
            vec![Value::Integer(2), Value::Integer(5)],
        ]
    );
    run(
        &mut db,
        "MERGE INTO m USING src ON m.a = src.a \
         WHEN MATCHED THEN UPDATE SET b = 9;",
    );
    run(
        &mut db,
        "MERGE INTO m USING src ON m.a = src.a \
         WHEN NOT MATCHED THEN INSERT VALUES (src.a, DEFAULT);",
    );
    assert_eq!(
        rows(&mut db, "SELECT DISTINCT b FROM m ORDER BY b;"),
        vec![vec![Value::Integer(9)]]
    );

    // INSERT … SELECT with a column list evaluates a clock default per
    // statement, not per row.
    run(
        &mut db,
        "CREATE TABLE d (a INTEGER, ts TIMESTAMP DEFAULT now());",
    );
    run(&mut db, "INSERT INTO d (a) SELECT a FROM src;");
    let ts = rows(&mut db, "SELECT ts FROM d;");
    assert_eq!(ts.len(), 2);
    assert_eq!(timestamp(&ts[0][0]), timestamp(&ts[1][0]), "{ts:?}");

    // The default applies only to columns the list omits.
    run(&mut db, "UPDATE d SET ts = '2020-01-02 03:04:05';");
    run(
        &mut db,
        "INSERT INTO d (a, ts) SELECT 3, '2021-01-01 00:00:00';",
    );
    assert_eq!(
        rows(&mut db, "SELECT a, ts FROM d WHERE a = 3;"),
        vec![vec![
            Value::Integer(3),
            Value::Timestamp(pinkdb::types::parse_timestamp("2021-01-01 00:00:00").unwrap())
        ]]
    );
}

#[test]
fn add_column_evaluates_the_default_for_every_existing_row() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (i INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (1), (2), (3);");
    run(&mut db, "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1 + 2;");
    assert_eq!(
        rows(&mut db, "SELECT n FROM t;"),
        vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(3)],
            vec![Value::Integer(3)]
        ]
    );
    run(
        &mut db,
        "ALTER TABLE t ADD COLUMN ts TIMESTAMP DEFAULT now();",
    );
    let ts = rows(&mut db, "SELECT ts FROM t;");
    assert!(ts.iter().all(|r| timestamp(&r[0]) == timestamp(&ts[0][0])));
    let first = timestamp(&ts[0][0]);
    std::thread::sleep(std::time::Duration::from_millis(3));
    run(&mut db, "INSERT INTO t (i) VALUES (4);");
    let later = timestamp(&rows(&mut db, "SELECT ts FROM t WHERE i = 4;")[0][0]);
    assert!(later > first, "{first} -> {later}");
}

#[test]
fn defaults_and_values_survive_checkpoint_and_wal_replay() {
    // Checkpoint path: the footer stores the written text and rebinds it.
    let scratch = Scratch::new("checkpoint");
    let instant;
    {
        let mut db = open(&scratch);
        run(
            &mut db,
            "CREATE TABLE p (a INTEGER DEFAULT 7, ts TIMESTAMP DEFAULT now(), \
             s VARCHAR DEFAULT 'x''y');",
        );
        run(&mut db, "INSERT INTO p DEFAULT VALUES;");
        let out = rows(&mut db, "SELECT ts FROM p;");
        instant = timestamp(&out[0][0]);
        db.checkpoint().unwrap();
    }
    {
        let mut db = open(&scratch);
        assert_eq!(
            rows(&mut db, "SELECT a, ts, s FROM p;"),
            vec![vec![
                Value::Integer(7),
                Value::Timestamp(instant),
                Value::Varchar("x'y".into())
            ]]
        );
        let describe = rows(&mut db, "DESCRIBE p;");
        assert_eq!(cell_text(&describe[0][4]), "7");
        assert_eq!(cell_text(&describe[1][4]), "now()");
        assert_eq!(cell_text(&describe[2][4]), "'x''y'");
        // The reopened table still evaluates its defaults per statement.
        std::thread::sleep(std::time::Duration::from_millis(3));
        run(&mut db, "INSERT INTO p DEFAULT VALUES;");
        let out = rows(&mut db, "SELECT a, ts, s FROM p ORDER BY ts;");
        assert_eq!(out.len(), 2);
        assert!(timestamp(&out[1][1]) > instant);
    }

    // Crash path: dropping without a checkpoint replays the WAL, including the
    // clock instant the INSERT used.
    let scratch = Scratch::new("replay");
    let instant;
    {
        let mut db = open(&scratch);
        run(
            &mut db,
            "CREATE TABLE p (a INTEGER DEFAULT 7, ts TIMESTAMP DEFAULT now());",
        );
        run(&mut db, "INSERT INTO p DEFAULT VALUES;");
        instant = timestamp(&rows(&mut db, "SELECT ts FROM p;")[0][0]);
        // No checkpoint: drop models a crash.
    }
    let mut db = open(&scratch);
    assert_eq!(
        rows(&mut db, "SELECT a, ts FROM p;"),
        vec![vec![Value::Integer(7), Value::Timestamp(instant)]],
        "WAL replay must reproduce the statement's clock reading"
    );
}

#[test]
fn a_prepared_add_column_default_is_stored_as_its_value() {
    let scratch = Scratch::new("prepared");
    {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE t (i INTEGER);");
        run(&mut db, "INSERT INTO t VALUES (1), (2);");
        let stmt = db
            .prepare("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT ?;")
            .unwrap();
        db.execute_prepared(&stmt, &[Value::Integer(7)]).unwrap();
    }
    // The footer stores a rebindable literal, not `?`.
    let mut db = open(&scratch);
    assert_eq!(cell_text(&rows(&mut db, "DESCRIBE t;")[1][4]), "7");
    run(&mut db, "INSERT INTO t (i) VALUES (3);");
    assert_eq!(
        rows(&mut db, "SELECT n FROM t ORDER BY i;"),
        vec![
            vec![Value::Integer(7)],
            vec![Value::Integer(7)],
            vec![Value::Integer(7)],
        ]
    );
}

#[test]
fn a_version_four_footer_is_refused() {
    let scratch = Scratch::new("v4");
    {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE t (a INTEGER DEFAULT 1);");
        db.checkpoint().unwrap();
    }
    relabel_footer_version(Path::new(scratch.path()), 4);
    let e = PinkDb::open_durable_with(scratch.path(), SyncPolicy::Sync)
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(e.contains("footer version 4"), "{e}");
    assert!(e.contains("reads footer version 6 only"), "{e}");
}

/// Rewrite a `.pink` image's footer version word (and its trailer CRC), so the
/// loader sees a file an older build wrote.
fn relabel_footer_version(path: &Path, version: u32) {
    let mut bytes = std::fs::read(path).unwrap();
    let (offset, len) = pinkdb::persist::footer_span(&bytes).unwrap();
    let (offset, len) = (offset as usize, len as usize);
    bytes[offset..offset + 4].copy_from_slice(&version.to_le_bytes());
    let crc = pinkdb::persist::crc32c(&bytes[offset..offset + len]);
    bytes[offset + len..offset + len + 4].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn introspection_prints_the_written_default_text() {
    let mut db = PinkDb::new();
    run(
        &mut db,
        "CREATE TABLE z (a INTEGER DEFAULT 1+2, b TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
         c VARCHAR DEFAULT 'x''y', d DECIMAL(10,2) DEFAULT 1.5, e DOUBLE DEFAULT 1.5, \
         f VARCHAR DEFAULT uuid(), g TIMESTAMP DEFAULT now());",
    );
    let describe = rows(&mut db, "DESCRIBE z;");
    let defaults: Vec<String> = describe.iter().map(|r| cell_text(&r[4])).collect();
    // Simple defaults print exactly as DuckDB prints them. A compound
    // expression keeps the written text (`1+2`); DuckDB would normalize it to
    // `(1 + 2)` — the footer stores what the user wrote, so that is what every
    // catalog shows.
    assert_eq!(
        defaults,
        vec![
            "1+2",
            "CURRENT_TIMESTAMP",
            "'x''y'",
            "1.5",
            "1.5",
            "uuid()",
            "now()",
        ]
    );
    let duck = rows(
        &mut db,
        "SELECT column_default FROM duckdb_columns() ORDER BY column_index;",
    );
    assert_eq!(
        duck.iter().map(|r| cell_text(&r[0])).collect::<Vec<_>>(),
        defaults
    );
    let info = rows(
        &mut db,
        "SELECT column_default FROM information_schema.columns ORDER BY ordinal_position;",
    );
    assert_eq!(
        info.iter().map(|r| cell_text(&r[0])).collect::<Vec<_>>(),
        defaults
    );
    let sql = cell_text(&rows(&mut db, "SELECT sql FROM duckdb_tables();")[0][0]);
    assert!(sql.contains("DEFAULT(1+2)"), "{sql}");
    assert!(sql.contains("DEFAULT(uuid())"), "{sql}");

    // Deterministic defaults evaluate to their type: DECIMAL keeps its scale.
    run(
        &mut db,
        "CREATE TABLE w (c VARCHAR DEFAULT 'x''y', d DECIMAL(10,2) DEFAULT 1.5, \
         e DOUBLE DEFAULT 1.5);",
    );
    run(&mut db, "INSERT INTO w DEFAULT VALUES;");
    assert_eq!(
        rows(&mut db, "SELECT c, d, e FROM w;"),
        vec![vec![
            Value::Varchar("x'y".into()),
            Value::decimal(150, 2),
            Value::Double(1.5)
        ]]
    );
    assert_eq!(
        rows(&mut db, "SELECT typeof(d) FROM w;"),
        vec![vec![Value::Varchar("DECIMAL(10,2)".into())]]
    );
}
