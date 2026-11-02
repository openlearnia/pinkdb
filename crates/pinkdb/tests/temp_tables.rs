//! Wave 139c: session-local temporary tables (`CREATE TEMP TABLE`).
//!
//! Every session owns a `temp` catalog with one schema (`main`). It is never
//! written to the checkpoint, the WAL or a lake image, never visible to
//! another session, and dies with the session. An unqualified table name
//! resolves in `temp.main` first, then the current schema — DuckDB's search
//! path — while `CREATE TABLE` stays in the current schema unless the name
//! says `temp`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{
    AttachedLake, Connection, DataTable, DataType, Database, DatabaseOpener, LakeContent,
    LakeDatabase, LakeOpenRequest, PinkDb, QueryResult, SyncPolicy, Value,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_temp_tables_{tag}_{}_{}",
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
    PinkDb::open_durable_with(scratch.path(), SyncPolicy::Batch)
        .unwrap()
        .0
}

fn run(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn ints(db: &mut PinkDb, sql: &str) -> Vec<i32> {
    rows(db, sql)
        .into_iter()
        .map(|row| match row.into_iter().next().unwrap() {
            Value::Integer(i) => i,
            other => panic!("{sql}: {other:?}"),
        })
        .collect()
}

fn count(db: &mut PinkDb, sql: &str) -> i64 {
    match rows(db, sql).pop().unwrap().pop().unwrap() {
        Value::Bigint(n) => n,
        Value::Integer(n) => i64::from(n),
        other => panic!("{sql}: {other:?}"),
    }
}

fn strings(db: &mut PinkDb, sql: &str) -> Vec<String> {
    rows(db, sql)
        .into_iter()
        .map(|row| match row.into_iter().next().unwrap() {
            Value::Varchar(s) => s,
            other => panic!("{sql}: {other:?}"),
        })
        .collect()
}

fn conn_rows(conn: &mut Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn conn_count(conn: &mut Connection, sql: &str) -> i64 {
    match conn_rows(conn, sql).pop().unwrap().pop().unwrap() {
        Value::Bigint(n) => n,
        Value::Integer(n) => i64::from(n),
        other => panic!("{sql}: {other:?}"),
    }
}

fn conn_err(conn: &mut Connection, sql: &str) -> String {
    conn.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

/// The DuckDB cases: shadowing, the three spellings of the temp name, `DROP`
/// order, `USE` a schema, `IF NOT EXISTS` / `OR REPLACE`, and the parser
/// rules for the `temp` catalog.
#[test]
fn resolution_follows_duckdbs_search_path() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (a INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (1);");
    run(&mut db, "CREATE TEMP TABLE t (a INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (2);");
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![2]);
    assert_eq!(ints(&mut db, "SELECT a FROM temp.t;"), vec![2]);
    assert_eq!(ints(&mut db, "SELECT a FROM temp.main.t;"), vec![2]);
    // A bare `main.` prefix is DuckDB's `memory.main` in the search path, so
    // temp.main still wins; `main.main.t` names the persistent one.
    assert_eq!(ints(&mut db, "SELECT a FROM main.t;"), vec![2]);
    assert_eq!(ints(&mut db, "SELECT a FROM main.main.t;"), vec![1]);
    // The shadowed DML follows the same order.
    run(&mut db, "UPDATE t SET a = 3;");
    assert_eq!(ints(&mut db, "SELECT a FROM temp.t;"), vec![3]);
    assert_eq!(ints(&mut db, "SELECT a FROM main.main.t;"), vec![1]);
    run(&mut db, "DELETE FROM t WHERE a = 3;");
    assert_eq!(count(&mut db, "SELECT count(*) FROM temp.t;"), 0);
    run(&mut db, "DROP TABLE t;");
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);

    // `CREATE TABLE` stays in the current schema even when a temp table of
    // the same name exists (DuckDB).
    run(&mut db, "CREATE TEMP TABLE p (a INTEGER);");
    run(&mut db, "INSERT INTO p VALUES (7);");
    run(&mut db, "CREATE TABLE p (a INTEGER);");
    run(&mut db, "INSERT INTO main.main.p VALUES (8);");
    assert_eq!(ints(&mut db, "SELECT a FROM temp.p;"), vec![7]);
    assert_eq!(ints(&mut db, "SELECT a FROM main.p;"), vec![7]);
    assert_eq!(ints(&mut db, "SELECT a FROM main.main.p;"), vec![8]);

    // A temp table is found from another schema too: `temp.main` comes
    // before the current schema.
    run(&mut db, "CREATE SCHEMA s;");
    run(&mut db, "USE s;");
    assert_eq!(ints(&mut db, "SELECT a FROM p;"), vec![7]);
    assert_eq!(ints(&mut db, "SELECT a FROM main.p;"), vec![7]);
    assert_eq!(ints(&mut db, "SELECT a FROM main.main.p;"), vec![8]);
    run(&mut db, "USE main;");
}

#[test]
fn create_temp_grammar_and_qualifier_rules() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TEMPORARY TABLE a (x INTEGER);");
    run(&mut db, "CREATE TEMP TABLE b (x INTEGER);");
    run(&mut db, "CREATE TEMP TABLE temp.c (x INTEGER);");
    run(&mut db, "CREATE TEMP TABLE temp.main.d (x INTEGER);");
    // `main` is tolerated as the temp schema name, as in DuckDB.
    run(&mut db, "CREATE TEMP TABLE main.e (x INTEGER);");
    for name in ["a", "b", "c", "d", "e"] {
        assert_eq!(
            strings(
                &mut db,
                &format!(
                    "SELECT table_name FROM duckdb_tables() \
                 WHERE database_name = 'temp' AND table_name = '{name}'"
                )
            ),
            vec![name.to_string()],
            "{name}"
        );
    }
    let e = err(&mut db, "CREATE TABLE temp.f (x INTEGER);");
    assert!(
        e.contains("Only TEMPORARY table names can use the \"temp\" catalog"),
        "{e}"
    );
    let e = err(&mut db, "CREATE TEMP TABLE memory.main.f (x INTEGER);");
    assert!(
        e.contains("TEMPORARY table names can *only* use the \"temp\" catalog"),
        "{e}"
    );
    let e = err(&mut db, "CREATE TEMP TABLE temp.s.f (x INTEGER);");
    assert!(
        e.contains("TEMPORARY tables can only live in the \"temp\" schema"),
        "{e}"
    );
    let e = err(&mut db, "CREATE TEMP TABLE s.f (x INTEGER);");
    assert!(
        e.contains("TEMPORARY tables can only live in the \"temp\" schema"),
        "{e}"
    );

    // IF NOT EXISTS is a no-op, OR REPLACE replaces only the temp table.
    run(&mut db, "CREATE TABLE g (x INTEGER);");
    run(&mut db, "INSERT INTO g VALUES (1);");
    run(
        &mut db,
        "CREATE TEMPORARY TABLE IF NOT EXISTS h (x INTEGER);",
    );
    run(&mut db, "INSERT INTO h VALUES (2);");
    run(
        &mut db,
        "CREATE TEMPORARY TABLE IF NOT EXISTS h (x INTEGER);",
    );
    assert_eq!(count(&mut db, "SELECT count(*) FROM h;"), 1);
    run(&mut db, "CREATE OR REPLACE TEMP TABLE g (x INTEGER);");
    run(&mut db, "INSERT INTO g VALUES (3);");
    assert_eq!(ints(&mut db, "SELECT x FROM temp.g;"), vec![3]);
    assert_eq!(ints(&mut db, "SELECT x FROM main.main.g;"), vec![1]);

    let e = err(&mut db, "USE temp;");
    assert!(e.contains("internal database \"temp\""), "{e}");
}

#[test]
fn catalog_functions_list_the_sessions_temp_tables() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TEMP TABLE z (a INTEGER, b VARCHAR);");
    run(&mut db, "INSERT INTO z VALUES (1, 'x');");
    assert_eq!(
        strings(
            &mut db,
            "SELECT database_name FROM duckdb_databases() WHERE internal ORDER BY 1"
        ),
        vec!["system".to_string(), "temp".to_string()]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT schema_name FROM duckdb_schemas() \
             WHERE database_name = 'temp' AND internal"
        ),
        vec!["main".to_string()]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT schema_name, temporary FROM duckdb_tables() WHERE database_name = 'temp'"
        ),
        vec![vec![
            Value::Varchar("main".to_string()),
            Value::Boolean(true),
        ]]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_type FROM information_schema.tables WHERE table_catalog = 'temp'"
        ),
        vec!["LOCAL TEMPORARY".to_string()]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT column_name FROM information_schema.columns \
             WHERE table_catalog = 'temp' ORDER BY column_name"
        ),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT column_name FROM duckdb_columns() WHERE database_name = 'temp' \
             ORDER BY column_index"
        ),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(strings(&mut db, "SHOW TABLES;"), vec!["z".to_string()]);
    let all = rows(&mut db, "SHOW ALL TABLES;");
    let temp_rows: Vec<_> = all
        .into_iter()
        .filter(|row| matches!(&row[0], Value::Varchar(db) if db == "temp"))
        .collect();
    assert_eq!(temp_rows.len(), 1, "{temp_rows:?}");
    assert_eq!(temp_rows[0][0], Value::Varchar("temp".to_string()));
    assert_eq!(temp_rows[0][1], Value::Varchar("main".to_string()));
    assert_eq!(temp_rows[0][2], Value::Varchar("z".to_string()));
    assert_eq!(temp_rows[0][5], Value::Boolean(true));
    // The temp table's DDL is rendered with the `TEMP` keyword, like DuckDB.
    let sql = strings(
        &mut db,
        "SELECT sql FROM duckdb_tables() WHERE database_name = 'temp'",
    );
    assert!(sql[0].starts_with("CREATE TEMP TABLE z"), "{sql:?}");
}

#[test]
fn indexes_and_alter_work_on_a_temp_table() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TEMP TABLE t (a INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (2), (1);");
    run(&mut db, "CREATE INDEX i ON t (a);");
    run(&mut db, "ALTER TABLE t ADD COLUMN b VARCHAR;");
    run(&mut db, "UPDATE t SET b = 'x';");
    assert_eq!(
        rows(&mut db, "SELECT a, b FROM t ORDER BY a;"),
        vec![
            vec![Value::Integer(1), Value::Varchar("x".to_string())],
            vec![Value::Integer(2), Value::Varchar("x".to_string())],
        ]
    );
    run(&mut db, "DESCRIBE t;");
    run(&mut db, "DROP INDEX i;");
}

#[test]
fn each_connection_sees_only_its_own_temp_tables() {
    let shared = Database::from_store(PinkDb::new());
    let mut a = shared.connect();
    let mut b = shared.connect();
    a.execute("CREATE TEMP TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();
    b.execute("CREATE TEMP TABLE t (a INTEGER); INSERT INTO t VALUES (2);")
        .unwrap();
    assert_eq!(
        conn_rows(&mut a, "SELECT a FROM t;"),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        conn_rows(&mut b, "SELECT a FROM t;"),
        vec![vec![Value::Integer(2)]]
    );
    // A table that exists only in b's session is not found in a's.
    b.execute("CREATE TEMP TABLE only_b (a INTEGER);").unwrap();
    let e = conn_err(&mut a, "SELECT * FROM only_b;");
    assert!(e.contains("not found"), "{e}");
    // The embedded handle has its own catalog too.
    let mut embedded = shared.connect();
    let e = conn_err(&mut embedded, "SELECT * FROM t;");
    assert!(e.contains("not found"), "{e}");
    // Dropping a connection drops its temp catalog; a new session starts
    // empty and the other session is untouched.
    drop(a);
    let mut c = shared.connect();
    let e = conn_err(&mut c, "SELECT * FROM t;");
    assert!(e.contains("not found"), "{e}");
    assert_eq!(
        conn_rows(&mut b, "SELECT a FROM t;"),
        vec![vec![Value::Integer(2)]]
    );
    assert_eq!(
        conn_count(
            &mut b,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'temp';"
        ),
        2
    );
}

#[test]
fn temp_tables_follow_transactions() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TEMP TABLE t (a INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (1);");
    run(&mut db, "BEGIN;");
    run(&mut db, "INSERT INTO t VALUES (2);");
    run(&mut db, "ROLLBACK;");
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
    run(&mut db, "BEGIN;");
    run(&mut db, "CREATE TEMP TABLE u (a INTEGER);");
    run(&mut db, "ROLLBACK;");
    let e = err(&mut db, "SELECT * FROM u;");
    assert!(e.contains("not found"), "{e}");
    run(&mut db, "BEGIN;");
    run(&mut db, "CREATE TEMP TABLE v (a INTEGER);");
    run(&mut db, "COMMIT;");
    run(&mut db, "INSERT INTO v VALUES (3);");
    assert_eq!(ints(&mut db, "SELECT a FROM v;"), vec![3]);
}

#[test]
fn durable_stores_never_write_temp_tables() {
    let scratch = Scratch::new("durable");
    {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE keep (a INTEGER);");
        run(&mut db, "CREATE TEMP TABLE stage AS SELECT 1 AS a;");
        run(&mut db, "INSERT INTO stage VALUES (2);");
        run(&mut db, "CREATE TEMP TABLE t (a INTEGER);");
        run(&mut db, "DROP TABLE t;");
        run(&mut db, "CHECKPOINT;");
    }
    // The WAL (and the checkpoint) never saw the temp table's name.
    let wal = std::fs::read_to_string(format!("{}.wal", scratch.path())).unwrap_or_default();
    assert!(
        !wal.contains("stage"),
        "the WAL carries a temp table: {wal}"
    );
    let image = std::fs::read(scratch.path()).unwrap();
    assert!(
        !image.windows(5).any(|w| w == b"stage"),
        "the checkpoint carries a temp table"
    );
    let mut db = open(&scratch);
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'temp'"
        ),
        0
    );
    let e = err(&mut db, "SELECT * FROM stage;");
    assert!(e.contains("not found"), "{e}");
    run(&mut db, "INSERT INTO keep VALUES (1);");
    assert_eq!(ints(&mut db, "SELECT a FROM keep;"), vec![1]);
}

#[test]
fn a_durable_local_write_may_not_read_a_temp_table() {
    let scratch = Scratch::new("refusal");
    let mut db = open(&scratch);
    run(&mut db, "CREATE TABLE p (a INTEGER);");
    run(&mut db, "CREATE TEMP TABLE stage AS SELECT 1 AS a;");
    let e = err(&mut db, "INSERT INTO p SELECT a FROM stage;");
    assert!(e.contains("session-local temporary table \"stage\""), "{e}");
    assert!(e.contains("write the rows to a real table first"), "{e}");
    let e = err(&mut db, "CREATE TABLE q AS SELECT a FROM stage;");
    assert!(e.contains("session-local temporary table \"stage\""), "{e}");
    let e = err(&mut db, "UPDATE p SET a = (SELECT max(a) FROM stage);");
    assert!(e.contains("session-local temporary table \"stage\""), "{e}");
    let e = err(&mut db, "DELETE FROM p USING stage WHERE p.a = stage.a;");
    assert!(e.contains("session-local temporary table \"stage\""), "{e}");
    // Writing the temp table itself, or reading it, is fine.
    run(&mut db, "INSERT INTO stage VALUES (2);");
    run(&mut db, "CREATE TEMP TABLE copy AS SELECT * FROM p;");
    assert_eq!(count(&mut db, "SELECT count(*) FROM stage;"), 2);
    // In-memory stores allow everything (there is no WAL to replay).
    let mut memory = PinkDb::new();
    run(&mut memory, "CREATE TABLE p (a INTEGER);");
    run(&mut memory, "CREATE TEMP TABLE stage AS SELECT 1 AS a;");
    run(&mut memory, "INSERT INTO p SELECT a FROM stage;");
    assert_eq!(ints(&mut memory, "SELECT a FROM p;"), vec![1]);
}

/// A writable lake database with `master.fact`, served by a fake opener (the
/// lake write is the record; the local WAL is not involved).
#[derive(Debug)]
struct FakeLake {
    location: String,
}

impl LakeDatabase for FakeLake {
    fn location(&self) -> String {
        self.location.clone()
    }
    fn read_only(&self) -> bool {
        false
    }
    fn version(&self) -> u64 {
        1
    }
    fn schemas(&self) -> pinkdb::Result<Vec<String>> {
        Ok(vec!["main".to_string(), "master".to_string()])
    }
    fn reader(&self) -> pinkdb::Result<Arc<dyn AttachedLake>> {
        Err(pinkdb::PinkDbError::Execution(
            "the fake lake is writable".to_string(),
        ))
    }
    fn content(&self) -> pinkdb::Result<LakeContent> {
        let mut fact = DataTable::new(
            "fact".to_string(),
            vec![("id".to_string(), DataType::Integer)],
        )
        .expect("the fake table's schema");
        fact.append_row(vec![Value::Integer(1)])
            .expect("the fake table's row");
        Ok(LakeContent {
            image: None,
            tables: vec![("master".to_string(), "fact".to_string(), Arc::new(fact))],
        })
    }
    fn publish(
        &self,
        _image: &[u8],
        _schemas: &[String],
        _tables: &[(String, String, Arc<DataTable>)],
    ) -> pinkdb::Result<u64> {
        Ok(2)
    }
    fn tables_at(&self, _version: u64) -> pinkdb::Result<Vec<(String, String, Arc<DataTable>)>> {
        Ok(Vec::new())
    }
}

struct FakeOpener;

impl DatabaseOpener for FakeOpener {
    fn open(&self, request: &LakeOpenRequest) -> pinkdb::Result<Arc<dyn LakeDatabase>> {
        Ok(Arc::new(FakeLake {
            location: request.catalog.clone(),
        }))
    }
}

#[test]
fn a_lake_database_may_be_fed_from_a_temp_table() {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| pinkdb::register_database_opener(Arc::new(FakeOpener)));
    let scratch = Scratch::new("lake");
    let mut db = open(&scratch);
    run(
        &mut db,
        "CREATE DATABASE lake (TYPE LAKE, CATALOG 'fake:lake');",
    );
    run(&mut db, "CREATE TEMP TABLE stage AS SELECT 2 AS id;");
    run(
        &mut db,
        "INSERT INTO lake.master.fact SELECT id FROM stage;",
    );
    // The lake commit is the record: the local WAL never names the temp
    // table, so a reopen replays cleanly.
    let wal = std::fs::read_to_string(format!("{}.wal", scratch.path())).unwrap_or_default();
    assert!(
        !wal.contains("stage"),
        "the WAL carries a temp table: {wal}"
    );
    drop(db);
    let mut db = open(&scratch);
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_name FROM duckdb_tables() \
             WHERE database_name = 'lake' AND schema_name = 'master'"
        ),
        vec!["fact".to_string()]
    );
}

#[test]
fn a_restricted_user_gets_a_temp_catalog_without_grants() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE secret (a INTEGER);");
    run(&mut db, "CREATE USER alice PASSWORD 'alice-secret';");
    let shared = Database::from_store(db);
    let mut alice = shared.connect();
    alice.login("alice", "alice-secret").unwrap();
    // No grants at all: the temp catalog is still usable.
    alice
        .execute("CREATE TEMP TABLE mine (a INTEGER); INSERT INTO mine VALUES (1);")
        .unwrap();
    assert_eq!(
        conn_rows(&mut alice, "SELECT a FROM mine;"),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        conn_count(
            &mut alice,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'temp';"
        ),
        1
    );
    // Persistent objects are still denied without a grant.
    let e = conn_err(&mut alice, "SELECT * FROM secret;");
    assert!(e.contains("privilege"), "{e}");
    let e = conn_err(&mut alice, "GRANT SELECT ON TABLE mine TO alice;");
    assert!(e.contains("temporary tables"), "{e}");
}
