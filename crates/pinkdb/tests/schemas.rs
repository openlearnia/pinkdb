//! Wave 131b: the SQL surface for schemas — three-part names, `USE`,
//! `CREATE`/`DROP SCHEMA`, object bodies in their own schema, durable replay
//! and schema-level grants.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{Connection, Database, PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_schemas_{tag}_{}_{}",
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

fn result(db: &mut PinkDb, sql: &str) -> QueryResult {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .unwrap()
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match result(db, sql) {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    rows(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i as i64,
            Value::Bigint(i) => *i,
            other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

fn strings(db: &mut PinkDb, sql: &str) -> Vec<String> {
    rows(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

fn conn_ints(conn: &mut Connection, sql: &str) -> Vec<i64> {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i as i64,
                Value::Bigint(i) => *i,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn conn_err(conn: &mut Connection, sql: &str) -> String {
    conn.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

#[test]
fn quacklab_three_part_shape_works() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE DATABASE r2_cat;");
    run(&mut db, "USE r2_cat;");
    run(&mut db, "CREATE SCHEMA \"master\";");
    run(
        &mut db,
        "CREATE TABLE \"master\".\"FACT\" (id INTEGER, v VARCHAR);",
    );
    run(
        &mut db,
        "INSERT INTO r2_cat.\"master\".\"FACT\" VALUES (1, 'a'), (2, 'b');",
    );
    assert_eq!(
        ints(
            &mut db,
            "SELECT FACT.id FROM \"master\".\"FACT\" ORDER BY 1;"
        ),
        vec![1, 2]
    );
    assert_eq!(
        ints(
            &mut db,
            "SELECT f.id FROM \"master\".\"FACT\" f JOIN r2_cat.master.fact g \
             ON f.id = g.id ORDER BY 1;"
        ),
        vec![1, 2]
    );
    assert_eq!(
        ints(
            &mut db,
            "SELECT r2_cat.master.fact.id FROM r2_cat.master.fact ORDER BY 1;"
        ),
        vec![1, 2]
    );
    run(&mut db, "USE r2_cat.master;");
    assert_eq!(
        result(&mut db, "SHOW TABLES;"),
        QueryResult::Selected {
            names: vec!["name".to_string()],
            types: vec![pinkdb::DataType::Varchar],
            rows: vec![vec![Value::Varchar("FACT".to_string())]],
        }
    );
}

#[test]
fn two_part_names_are_ambiguous_but_main_t_is_exempt() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE DATABASE other;");
    run(&mut db, "CREATE SCHEMA other;");
    // `other.main.t` is the other database's table; `main.other.t` is the
    // schema in the current database.
    run(&mut db, "CREATE TABLE other.main.t (x INTEGER);");
    run(&mut db, "INSERT INTO other.main.t VALUES (1);");
    run(&mut db, "CREATE TABLE main.other.t (x INTEGER);");
    run(&mut db, "INSERT INTO main.other.t VALUES (2);");
    let e = err(&mut db, "SELECT * FROM other.t;");
    assert!(e.contains("ambiguous"), "{e}");
    assert!(
        e.contains("other.main.t") && e.contains("main.other.t"),
        "{e}"
    );
    // `main` is the current database's schema, so `main.t` reads one table.
    run(&mut db, "CREATE TABLE main.t (x INTEGER);");
    run(&mut db, "INSERT INTO main.t VALUES (9);");
    assert_eq!(ints(&mut db, "SELECT main.t.x FROM main.t;"), vec![9]);
}

#[test]
fn use_both_forms_and_the_drop_fallback() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE DATABASE d;");
    run(&mut db, "CREATE SCHEMA d.s;");
    run(&mut db, "CREATE TABLE d.s.t (x INTEGER);");
    run(&mut db, "INSERT INTO d.s.t VALUES (1);");
    run(&mut db, "USE d.s;");
    assert_eq!(strings(&mut db, "SELECT current_database();"), vec!["d"]);
    assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["s"]);
    assert_eq!(ints(&mut db, "SELECT * FROM t;"), vec![1]);
    assert_eq!(
        strings(&mut db, "SELECT current_schemas(false);"),
        vec!["[s]"]
    );
    // One part picks the database first (DuckDB's order).
    run(&mut db, "USE d;");
    assert_eq!(strings(&mut db, "SELECT current_database();"), vec!["d"]);
    assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["main"]);
    run(&mut db, "USE d.s;");
    run(&mut db, "DROP SCHEMA d.s CASCADE;");
    assert_eq!(strings(&mut db, "SELECT current_database();"), vec!["d"]);
    assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["main"]);
}

#[test]
fn drop_schema_restrict_cascade_if_exists_and_main() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE SCHEMA s;");
    run(&mut db, "CREATE TABLE s.t (x INTEGER);");
    let e = err(&mut db, "DROP SCHEMA s;");
    assert!(
        e.contains("not empty") || e.contains("RESTRICT") || e.contains("CASCADE"),
        "{e}"
    );
    run(&mut db, "DROP SCHEMA s CASCADE;");
    assert!(db.execute("SELECT * FROM s.t;").is_err());
    run(&mut db, "DROP SCHEMA IF EXISTS s;");
    assert!(err(&mut db, "DROP SCHEMA s;").contains("schema \"s\" not found"));
    let e = err(&mut db, "DROP SCHEMA main CASCADE;");
    assert!(e.contains("main"), "{e}");
}

#[test]
fn object_bodies_bind_in_their_own_schema() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (x INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (1);");
    run(&mut db, "CREATE SCHEMA s;");
    run(&mut db, "CREATE TABLE s.t (x INTEGER);");
    run(&mut db, "INSERT INTO s.t VALUES (2);");
    // Every object is created from `main` with a qualified name; its body
    // still binds against `s`, while `main.t` also exists.
    run(&mut db, "CREATE VIEW s.v AS SELECT x FROM t;");
    assert_eq!(ints(&mut db, "SELECT x FROM s.v;"), vec![2]);
    run(
        &mut db,
        "CREATE MACRO s.count_t() AS TABLE SELECT COUNT(*) AS c FROM t;",
    );
    // A macro is spliced into the caller: from `main` it counts `main.t`.
    assert_eq!(ints(&mut db, "SELECT c FROM s.count_t();"), vec![1]);
    run(&mut db, "CREATE MATERIALIZED VIEW s.mv AS SELECT x FROM t;");
    assert_eq!(ints(&mut db, "SELECT x FROM s.mv;"), vec![2]);
    run(
        &mut db,
        "CREATE PROCEDURE s.put() AS $$ BEGIN INSERT INTO t VALUES (3); END $$;",
    );
    run(&mut db, "CALL s.put();");
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM s.t;"), vec![2]);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM t;"), vec![1]);
    run(&mut db, "CREATE TABLE s.log (v INTEGER);");
    run(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON s.t EXECUTE INSERT INTO log VALUES (4);",
    );
    run(&mut db, "INSERT INTO s.t VALUES (5);");
    assert_eq!(ints(&mut db, "SELECT v FROM s.log;"), vec![4]);
}

#[test]
fn macro_bodies_bind_in_the_callers_schema() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (x INTEGER);");
    run(&mut db, "INSERT INTO t VALUES (1);");
    run(&mut db, "CREATE SCHEMA s;");
    run(&mut db, "CREATE TABLE s.t (x INTEGER);");
    run(&mut db, "INSERT INTO s.t VALUES (2), (3);");
    run(
        &mut db,
        "CREATE MACRO s.plus(a) AS a + (SELECT max(x) FROM t);",
    );
    // DuckDB's values: the call site decides which `t` the body reads.
    assert_eq!(ints(&mut db, "SELECT s.plus(1);"), vec![2]);
    run(&mut db, "USE s;");
    assert_eq!(ints(&mut db, "SELECT plus(1);"), vec![4]);

    run(&mut db, "CREATE SCHEMA empty;");
    assert!(err(&mut db, "SELECT empty.nope(1);").contains("function \"empty.nope\" not found"));
    assert!(
        err(&mut db, "SELECT * FROM empty.nope();").contains("function \"empty.nope\" not found")
    );
}

#[test]
fn cross_schema_foreign_keys_are_refused() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE SCHEMA a; CREATE SCHEMA b;");
    run(&mut db, "CREATE TABLE a.p (id INTEGER PRIMARY KEY);");
    let e = err(
        &mut db,
        "CREATE TABLE b.c (pid INTEGER REFERENCES a.p(id));",
    );
    assert!(
        e.contains("Creating foreign keys across different schemas or catalogs is not supported"),
        "{e}"
    );
}

#[test]
fn describe_analyze_and_copy_take_three_part_names() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE SCHEMA s;");
    run(&mut db, "CREATE TABLE s.t (x INTEGER);");
    assert!(!rows(&mut db, "DESCRIBE s.t;").is_empty());
    assert_eq!(ints(&mut db, "ANALYZE s.t; SELECT 1;"), vec![1]);
    // The default build registers no file formats; reaching the format error
    // proves `s.t` bound (a missing table fails before it).
    let copy = std::env::temp_dir().join("pinkdb_schemas_copy.csv");
    let e = err(
        &mut db,
        &format!("COPY s.t TO '{}' (FORMAT CSV);", copy.display()),
    );
    assert!(e.contains("no file format"), "{e}");
}

#[test]
fn durable_replay_and_checkpoint_keep_quoted_schema_names() {
    let scratch = Scratch::new("durable");
    {
        let mut db = open(&scratch);
        run(&mut db, "CREATE DATABASE cat;");
        run(&mut db, "CREATE SCHEMA cat.\"a b\";");
        run(&mut db, "CREATE SCHEMA cat.\"x\"\"y\";");
        run(&mut db, "CREATE TABLE cat.\"a b\".t (v INTEGER);");
        run(&mut db, "CREATE TABLE cat.\"x\"\"y\".t (v INTEGER);");
        run(&mut db, "USE cat.\"a b\";");
        run(&mut db, "INSERT INTO t VALUES (1), (2);");
        run(&mut db, "USE cat.\"x\"\"y\";");
        run(&mut db, "INSERT INTO t VALUES (3);");
        // No checkpoint: every statement replays from the WAL.
    }
    {
        let mut db = open(&scratch);
        // A reopen starts on the store's default database: `USE` is session
        // state, so the quoted schema is re-entered by name.
        assert_eq!(strings(&mut db, "SELECT current_database();"), vec!["main"]);
        assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["main"]);
        run(&mut db, "USE cat.\"a b\";");
        assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["a b"]);
        assert_eq!(ints(&mut db, "SELECT * FROM t ORDER BY 1;"), vec![1, 2]);
        run(&mut db, "USE cat.\"x\"\"y\";");
        assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["x\"y"]);
        assert_eq!(
            ints(&mut db, "SELECT * FROM cat.\"a b\".t ORDER BY 1;"),
            vec![1, 2]
        );
        assert_eq!(ints(&mut db, "SELECT * FROM cat.\"x\"\"y\".t;"), vec![3]);
        run(&mut db, "DROP SCHEMA cat.\"x\"\"y\" CASCADE;");
        run(&mut db, "CREATE SCHEMA cat.\"x\"\"y\";");
        run(&mut db, "CREATE TABLE cat.\"x\"\"y\".t (v INTEGER);");
        run(&mut db, "INSERT INTO cat.\"x\"\"y\".t VALUES (4);");
        db.checkpoint().unwrap();
    }
    {
        let mut db = open(&scratch);
        assert_eq!(
            ints(&mut db, "SELECT * FROM cat.\"a b\".t ORDER BY 1;"),
            vec![1, 2]
        );
        assert_eq!(ints(&mut db, "SELECT * FROM cat.\"x\"\"y\".t;"), vec![4]);
    }
}

#[test]
fn durable_replay_of_qualified_macro_calls() {
    let scratch = Scratch::new("macro-durable");
    {
        let mut db = open(&scratch);
        run(&mut db, "CREATE DATABASE cat;");
        run(&mut db, "CREATE SCHEMA cat.s;");
        run(&mut db, "CREATE MACRO cat.s.m(a) AS a + 1;");
        run(&mut db, "CREATE MACRO cat.s.tm() AS TABLE SELECT 7 AS v;");
        run(&mut db, "USE cat;");
        assert_eq!(ints(&mut db, "SELECT s.m(41);"), vec![42]);
        assert_eq!(ints(&mut db, "SELECT cat.s.m(41);"), vec![42]);
        assert_eq!(ints(&mut db, "SELECT v FROM s.tm();"), vec![7]);
        assert_eq!(ints(&mut db, "SELECT v FROM cat.s.tm();"), vec![7]);
        // No checkpoint: every statement replays from the WAL.
    }
    {
        let mut db = open(&scratch);
        assert_eq!(strings(&mut db, "SELECT current_database();"), vec!["main"]);
        assert_eq!(ints(&mut db, "SELECT cat.s.m(41);"), vec![42]);
        assert_eq!(ints(&mut db, "SELECT v FROM cat.s.tm();"), vec![7]);
        // `USE` is session state: the reopened session enters `cat` again.
        run(&mut db, "USE cat;");
        assert_eq!(ints(&mut db, "SELECT s.m(41);"), vec![42]);
        assert_eq!(ints(&mut db, "SELECT v FROM s.tm();"), vec![7]);
        db.checkpoint().unwrap();
    }
    {
        let mut db = open(&scratch);
        run(&mut db, "USE cat;");
        assert_eq!(ints(&mut db, "SELECT s.m(41);"), vec![42]);
        assert_eq!(ints(&mut db, "SELECT cat.s.m(41);"), vec![42]);
        assert_eq!(ints(&mut db, "SELECT v FROM s.tm();"), vec![7]);
        assert_eq!(ints(&mut db, "SELECT v FROM cat.s.tm();"), vec![7]);
    }
}

#[test]
fn schema_grants_cover_later_tables_and_only_that_schema() {
    let shared = Database::from_store(PinkDb::new());
    let mut admin = shared.connect();
    admin
        .execute("CREATE USER alice PASSWORD 'alice-secret';")
        .unwrap();
    admin.execute("CREATE SCHEMA s1;").unwrap();
    admin.execute("CREATE SCHEMA s2;").unwrap();
    admin.execute("CREATE TABLE s1.t (x INTEGER);").unwrap();
    admin.execute("INSERT INTO s1.t VALUES (1);").unwrap();
    admin.execute("CREATE TABLE s2.t (x INTEGER);").unwrap();
    admin.execute("INSERT INTO s2.t VALUES (2);").unwrap();
    admin
        .execute("GRANT SELECT, CREATE ON SCHEMA s1 TO alice;")
        .unwrap();
    let mut alice = shared.connect();
    alice.login("alice", "alice-secret").unwrap();
    assert_eq!(conn_ints(&mut alice, "SELECT x FROM s1.t;"), vec![1]);
    let e = conn_err(&mut alice, "SELECT x FROM s2.t;");
    assert!(e.contains("permission denied"), "{e}");
    // The grant was made before this table existed.
    admin.execute("CREATE TABLE s1.later (x INTEGER);").unwrap();
    admin.execute("INSERT INTO s1.later VALUES (3);").unwrap();
    assert_eq!(conn_ints(&mut alice, "SELECT x FROM s1.later;"), vec![3]);
    alice.execute("CREATE TABLE s1.mine (x INTEGER);").unwrap();
    let e = conn_err(&mut alice, "CREATE TABLE s2.mine (x INTEGER);");
    assert!(e.contains("permission denied"), "{e}");
}

#[test]
fn create_schema_needs_create_on_the_database_and_works_once_granted() {
    let shared = Database::from_store(PinkDb::new());
    let mut admin = shared.connect();
    admin
        .execute("CREATE USER bob PASSWORD 'bob-secret';")
        .unwrap();
    let mut bob = shared.connect();
    bob.login("bob", "bob-secret").unwrap();
    let e = conn_err(&mut bob, "CREATE SCHEMA s;");
    assert!(e.contains("permission denied"), "{e}");
    admin.execute("CREATE DATABASE d;").unwrap();
    admin.execute("GRANT CREATE ON DATABASE d TO bob;").unwrap();
    bob.execute("CREATE SCHEMA d.s;").unwrap();
    bob.execute("CREATE TABLE d.s.t (x INTEGER);").unwrap();
}

#[test]
fn reserved_schema_names_are_refused() {
    let mut db = PinkDb::new();
    let e = err(&mut db, "CREATE SCHEMA information_schema;");
    assert!(e.contains("reserved"), "{e}");
}

#[test]
fn created_schema_keeps_its_quoted_case() {
    let mut db = PinkDb::new();
    match result(&mut db, "CREATE SCHEMA \"Master\";") {
        QueryResult::CreatedSchema { name } => assert_eq!(name, "Master"),
        other => panic!("{other:?}"),
    }
    run(&mut db, "USE \"Master\";");
    assert_eq!(strings(&mut db, "SELECT current_schema();"), vec!["Master"]);
    // Quoted case is display-only: the catalog key is case-insensitive.
    run(&mut db, "CREATE TABLE master.t (v INTEGER);");
    run(&mut db, "INSERT INTO \"Master\".t VALUES (1);");
    assert_eq!(ints(&mut db, "SELECT * FROM master.t;"), vec![1]);
}
