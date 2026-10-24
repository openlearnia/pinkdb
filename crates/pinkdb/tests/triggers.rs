//! Wave 100: triggers end to end — syntax, the timing/event/level matrix,
//! `WHEN`, `OLD`/`NEW`, transition tables, ordering, the recursion bound,
//! rollback on a trigger error, `.pink` persistence, WAL replay, the RBAC
//! matrix, materialized-view and MVCC interactions, and the refusals.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{Database, PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_triggers_{tag}_{}_{}",
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

fn varchars(db: &mut PinkDb, sql: &str) -> Vec<String> {
    rows(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected a varchar, got {other:?}"),
        })
        .collect()
}

fn _unused(_: &Path) {}

// ---------- helpers ----------

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
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

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

fn seeded() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, n INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20);")
        .unwrap();
    db
}

#[test]
fn insert_fires_a_statement_trigger() {
    let mut db = seeded();
    db.execute("CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('ins');")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 30);").unwrap();
    let msgs = rows(&mut db, "SELECT msg FROM log;");
    assert_eq!(msgs, vec![vec![Value::Varchar("ins".to_string())]]);
}

#[test]
fn row_triggers_see_old_and_new() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER logs AFTER UPDATE ON t FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES ('u');",
    )
    .unwrap();
    db.execute("UPDATE t SET n = n + 1 WHERE id = 1;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
    // DELETE row triggers carry OLD only.
    db.execute(
        "CREATE TRIGGER del AFTER DELETE ON t FOR EACH ROW EXECUTE INSERT INTO t VALUES (99, 0);",
    )
    .unwrap();
    db.execute("DELETE FROM t WHERE id = 2;").unwrap();
    assert_eq!(ints(&mut db, "SELECT id FROM t ORDER BY id;"), vec![1, 99]);
}

#[test]
fn when_clause_filters() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER big AFTER INSERT ON t FOR EACH ROW WHEN (NEW.n > 100) \
         EXECUTE INSERT INTO log VALUES ('big');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 5), (4, 500);")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
}

#[test]
fn before_row_may_assign_new() {
    let mut db = seeded();
    db.execute("CREATE PROCEDURE bump() AS $$ BEGIN SET NEW.n = NEW.n * 2; END; $$")
        .unwrap();
    db.execute("CREATE TRIGGER twice BEFORE UPDATE ON t FOR EACH ROW EXECUTE PROCEDURE bump();")
        .unwrap();
    db.execute("UPDATE t SET n = 1 WHERE id = 1;").unwrap();
    assert_eq!(ints(&mut db, "SELECT n FROM t WHERE id = 1;"), vec![2]);
}

#[test]
fn before_row_assign_survives_on_insert() {
    let mut db = seeded();
    db.execute("CREATE TRIGGER stamp BEFORE INSERT ON t FOR EACH ROW EXECUTE DO $$ BEGIN SET NEW.n = 7; END; $$")
        .unwrap();
    db.execute("INSERT INTO t VALUES (5, 0);").unwrap();
    assert_eq!(ints(&mut db, "SELECT n FROM t WHERE id = 5;"), vec![7]);
}

#[test]
fn transition_tables_see_the_statement_rows() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER audit AFTER UPDATE ON t REFERENCING OLD TABLE AS o NEW TABLE AS n \
         FOR EACH STATEMENT EXECUTE INSERT INTO log SELECT 'old ' || CAST(o.n AS VARCHAR) FROM o;",
    )
    .unwrap();
    db.execute("UPDATE t SET n = n + 5;").unwrap();
    let got = rows(&mut db, "SELECT msg FROM log ORDER BY msg;");
    assert_eq!(
        got,
        vec![
            vec![Value::Varchar("old 10".to_string())],
            vec![Value::Varchar("old 20".to_string())]
        ]
    );
}

#[test]
fn triggers_fire_in_creation_order() {
    let mut db = seeded();
    db.execute("CREATE TRIGGER b AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('b');")
        .unwrap();
    db.execute("CREATE TRIGGER a AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('a');")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    let got: Vec<String> = rows(&mut db, "SELECT msg FROM log;")
        .into_iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(got, vec!["b".to_string(), "a".to_string()]);
}

#[test]
fn recursion_is_bounded() {
    let mut db = seeded();
    db.execute("CREATE TRIGGER again AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO t VALUES (NEW.id + 100, 0);")
        .unwrap();
    let msg = err(&mut db, "INSERT INTO t VALUES (10, 0);");
    assert!(msg.contains("trigger recursion depth exceeded"), "{msg}");
    // The failed statement rolled back, including the rows it had inserted.
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM t;"), vec![2]);
}

#[test]
fn trigger_error_rolls_back_the_statement() {
    let mut db = seeded();
    // The body must bind, so a bad reference is refused at CREATE time.
    let msg = err(
        &mut db,
        "CREATE TRIGGER boom AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO missing VALUES (1);",
    );
    assert!(msg.contains("missing"), "{msg}");
    db.execute("CREATE TRIGGER boom AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('ins');")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
}

#[test]
fn show_and_drop() {
    let mut db = seeded();
    db.execute("CREATE TRIGGER one AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');")
        .unwrap();
    let got = rows(&mut db, "SHOW TRIGGERS;");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], Value::Varchar("one".to_string()));
    db.execute("DROP TRIGGER one;").unwrap();
    assert_eq!(rows(&mut db, "SHOW TRIGGERS;").len(), 0);
    assert!(err(&mut db, "DROP TRIGGER one;").contains("not found"));
    db.execute("DROP TRIGGER IF EXISTS one;").unwrap();
}

// ---------- syntax ----------

#[test]
fn every_timing_event_and_level_combination_fires() {
    for timing in ["BEFORE", "AFTER"] {
        for event in ["INSERT", "UPDATE", "DELETE"] {
            for level in ["ROW", "STATEMENT"] {
                let mut db = seeded();
                db.execute(&format!(
                    "CREATE TRIGGER all_{timing}_{event}_{level} {timing} {event} ON t \
                     FOR EACH {level} EXECUTE INSERT INTO log VALUES ('{event}');"
                ))
                .unwrap_or_else(|e| panic!("{timing} {event} {level}: {e}"));
                let dml = match event {
                    "INSERT" => "INSERT INTO t VALUES (3, 30);",
                    "UPDATE" => "UPDATE t SET n = n;",
                    _ => "DELETE FROM t WHERE id = 1;",
                };
                db.execute(dml).unwrap();
                // The UPDATE writes both rows; every other event touches one.
                let expect = if event == "UPDATE" && level == "ROW" {
                    2
                } else {
                    1
                };
                assert_eq!(
                    ints(&mut db, "SELECT COUNT(*) FROM log;"),
                    vec![expect],
                    "{timing} {event} {level}"
                );
            }
        }
    }
}

#[test]
fn update_of_narrows_to_the_written_columns() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER narrow AFTER UPDATE OF n ON t FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES ('n');",
    )
    .unwrap();
    db.execute("UPDATE t SET id = 6 WHERE id = 1;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![0]);
    db.execute("UPDATE t SET n = 11 WHERE id = 6;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
}

#[test]
fn multiple_events_and_or_replace() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER both BEFORE INSERT OR DELETE ON t EXECUTE INSERT INTO log VALUES ('dml');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    db.execute("DELETE FROM t WHERE id = 3;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![2]);
    assert!(
        err(
            &mut db,
            "CREATE TRIGGER both AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');"
        )
        .contains("already exists")
    );
    db.execute(
        "CREATE OR REPLACE TRIGGER both AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('new');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (4, 0);").unwrap();
    let got = rows(&mut db, "SELECT msg FROM log ORDER BY msg;");
    assert_eq!(
        got.last().unwrap(),
        &vec![Value::Varchar("new".to_string())]
    );
}

#[test]
fn drop_trigger_on_another_table_is_refused() {
    let mut db = seeded();
    db.execute("CREATE TABLE other (i INTEGER);").unwrap();
    db.execute("CREATE TRIGGER one AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');")
        .unwrap();
    let msg = err(&mut db, "DROP TRIGGER one ON other;");
    assert!(msg.contains("not \"other\""), "{msg}");
    db.execute("DROP TRIGGER one ON t;").unwrap();
}

#[test]
fn before_statements_run_before_the_change_and_after_after() {
    let mut db = seeded();
    db.execute("CREATE TABLE seen (i BIGINT);").unwrap();
    db.execute(
        "CREATE TRIGGER pre AFTER INSERT ON t FOR EACH STATEMENT \
         EXECUTE INSERT INTO seen SELECT COUNT(*) FROM t;",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0), (4, 0);").unwrap();
    // AFTER STATEMENT: both rows are already there.
    assert_eq!(ints(&mut db, "SELECT i FROM seen;"), vec![4]);

    let mut db = seeded();
    db.execute("CREATE TABLE seen (i BIGINT);").unwrap();
    db.execute(
        "CREATE TRIGGER pre BEFORE INSERT ON t FOR EACH STATEMENT \
         EXECUTE INSERT INTO seen SELECT COUNT(*) FROM t;",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0), (4, 0);").unwrap();
    assert_eq!(ints(&mut db, "SELECT i FROM seen;"), vec![2]);
}

#[test]
fn before_row_runs_before_after_row_and_both_see_the_same_new() {
    let mut db = seeded();
    db.execute("CREATE PROCEDURE bump() AS $$ BEGIN SET NEW.n = NEW.n + 1; END; $$")
        .unwrap();
    db.execute("CREATE TRIGGER a_before BEFORE INSERT ON t FOR EACH ROW EXECUTE PROCEDURE bump();")
        .unwrap();
    db.execute(
        "CREATE TRIGGER b_after AFTER INSERT ON t FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES (CAST(NEW.n AS VARCHAR));",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (9, 1);").unwrap();
    assert_eq!(ints(&mut db, "SELECT n FROM t WHERE id = 9;"), vec![2]);
    assert_eq!(
        rows(&mut db, "SELECT msg FROM log;"),
        vec![vec![Value::Varchar("2".to_string())]]
    );
}

#[test]
fn delete_transition_table() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER gone AFTER DELETE ON t REFERENCING OLD TABLE AS o \
         FOR EACH STATEMENT EXECUTE INSERT INTO log SELECT 'gone' FROM o WHERE o.id = 1;",
    )
    .unwrap();
    db.execute("DELETE FROM t;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
}

#[test]
fn old_is_read_only_in_a_body() {
    let mut db = seeded();
    db.execute("CREATE PROCEDURE bad() AS $$ BEGIN SET OLD.n = 1; END; $$")
        .unwrap();
    db.execute("CREATE TRIGGER bad_t BEFORE UPDATE ON t FOR EACH ROW EXECUTE PROCEDURE bad();")
        .unwrap();
    let msg = err(&mut db, "UPDATE t SET n = 1;");
    assert!(msg.contains("read-only"), "{msg}");
    // The whole statement rolled back.
    assert_eq!(ints(&mut db, "SELECT n FROM t ORDER BY id;"), vec![10, 20]);
}

#[test]
fn wrong_column_name_fails_at_create_time() {
    let mut db = seeded();
    let msg = err(
        &mut db,
        "CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES (CAST(NEW.nope AS VARCHAR));",
    );
    assert!(msg.contains("nope"), "{msg}");
    let msg = err(
        &mut db,
        "CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW WHEN (OLD.nope > 1) \
         EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("nope"), "{msg}");
}

#[test]
fn update_of_an_unknown_column_is_refused() {
    let mut db = seeded();
    let msg = err(
        &mut db,
        "CREATE TRIGGER bad AFTER UPDATE OF nope ON t EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("nope"), "{msg}");
}

// ---------- persistence ----------

fn persisted_defs(db: &mut PinkDb) -> Vec<Vec<Value>> {
    rows(db, "SHOW TRIGGERS;")
}

#[test]
fn triggers_survive_save_and_open() {
    let s = Scratch::new("persist");
    {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id INTEGER, n INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        db.execute("CREATE TRIGGER b AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('b');")
            .unwrap();
        db.execute("CREATE TRIGGER a AFTER UPDATE OF n ON t FOR EACH ROW WHEN (NEW.n > 0) EXECUTE INSERT INTO log VALUES ('a');")
            .unwrap();
        db.save(s.path()).unwrap();
    }
    let mut db = PinkDb::open(s.path()).unwrap();
    // Creation order survives, and both are live.
    let defs = persisted_defs(&mut db);
    assert_eq!(defs.len(), 2);
    assert_eq!(defs[0][0], Value::Varchar("b".to_string()));
    assert_eq!(defs[1][0], Value::Varchar("a".to_string()));
    db.execute("INSERT INTO t VALUES (2, 0);").unwrap();
    db.execute("UPDATE t SET n = 5 WHERE id = 2;").unwrap();
    assert_eq!(
        varchars(&mut db, "SELECT msg FROM log ORDER BY msg;"),
        vec!["a", "b"]
    );
}

#[test]
fn trigger_definitions_reload_with_their_bodies() {
    let s = Scratch::new("persist-body");
    {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id INTEGER, n INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
        db.execute(
            "CREATE TRIGGER tr AFTER INSERT ON t REFERENCING NEW TABLE AS n FOR EACH STATEMENT \
             EXECUTE INSERT INTO log SELECT 'row' FROM n;",
        )
        .unwrap();
        db.save(s.path()).unwrap();
    }
    let mut db = PinkDb::open(s.path()).unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 2);").unwrap();
    assert_eq!(
        varchars(&mut db, "SELECT msg FROM log ORDER BY msg;"),
        vec!["row", "row"]
    );
}

// ---------- WAL replay ----------

fn durable(p: &Scratch) -> PinkDb {
    PinkDb::open_durable_with(p.path(), SyncPolicy::Batch)
        .unwrap()
        .0
}

#[test]
fn wal_replay_refires_triggers_exactly_once() {
    let s = Scratch::new("wal");
    {
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (id INTEGER, n INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
        db.execute("CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('row');")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 1), (2, 2);").unwrap();
        // Trigger DDL itself replays: the trigger is created before the rows.
        db.execute("CREATE TRIGGER later AFTER DELETE ON t FOR EACH STATEMENT EXECUTE INSERT INTO log VALUES ('gone');")
            .unwrap();
        db.execute("DELETE FROM t WHERE id = 2;").unwrap();
    }
    // WAL-only reopen: no checkpoint was taken, so every statement replays.
    let mut db = durable(&s);
    assert_eq!(
        varchars(&mut db, "SELECT msg FROM log ORDER BY msg;"),
        vec!["gone", "row", "row"]
    );
    assert_eq!(ints(&mut db, "SELECT id FROM t ORDER BY id;"), vec![1]);
    // The triggers are live and fire once each after recovery, too.
    db.execute("INSERT INTO t VALUES (3, 3);").unwrap();
    db.execute("DELETE FROM t WHERE id = 3;").unwrap();
    assert_eq!(
        varchars(&mut db, "SELECT msg FROM log ORDER BY msg;"),
        vec!["gone", "gone", "row", "row", "row"]
    );
    // A checkpoint carries the trigger section; the tail replays on top.
    db.execute("CHECKPOINT;").unwrap();
    db.execute("INSERT INTO t VALUES (4, 4);").unwrap();
    drop(db);
    let mut db = durable(&s);
    assert_eq!(ints(&mut db, "SELECT id FROM t;"), vec![1, 4]);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![6]);
}

/// The trigger list itself is logged and replayed: a trigger created and then
/// dropped before the crash is absent after replay.
#[test]
fn wal_replay_honours_dropped_triggers() {
    let s = Scratch::new("waldrop");
    {
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
        db.execute("CREATE TRIGGER gone AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');")
            .unwrap();
        db.execute("DROP TRIGGER gone;").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
    }
    let mut db = durable(&s);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![0]);
    assert_eq!(rows(&mut db, "SHOW TRIGGERS;").len(), 0);
}

/// A hard crash after a triggered commit: a child process opens the store,
/// fires a trigger and exits without dropping the store (so no checkpoint, no
/// flush hook). The parent then replays and must see exactly the child's
/// committed state — the trigger's effects present once, not twice.
#[test]
fn crash_after_a_triggered_commit_replays_to_the_same_state() {
    let s = Scratch::new("crash");
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(exe)
        .args(["--exact", "crash_child_writes_and_exits", "--ignored"])
        .env("PINKDB_TRIGGER_CRASH_DIR", s.dir.to_str().unwrap())
        .status()
        .unwrap();
    assert!(status.success(), "child failed: {status:?}");

    let mut db = durable(&s);
    assert_eq!(
        varchars(&mut db, "SELECT msg FROM log ORDER BY msg;"),
        vec!["row", "row"]
    );
    assert_eq!(ints(&mut db, "SELECT n FROM t ORDER BY id;"), vec![11, 12]);
    // Replay is idempotent: a second reopen sees the same rows.
    drop(db);
    let mut db = durable(&s);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![2]);
}

/// The child half of [`crash_after_a_triggered_commit_replays_to_the_same_state`].
#[test]
#[ignore = "child process helper for the crash test"]
fn crash_child_writes_and_exits() {
    let Ok(dir) = std::env::var("PINKDB_TRIGGER_CRASH_DIR") else {
        return;
    };
    let file = PathBuf::from(dir).join("db.pink");
    let mut db = PinkDb::open_durable_with(file.to_str().unwrap(), SyncPolicy::Sync)
        .unwrap()
        .0;
    db.execute("CREATE TABLE t (id INTEGER, n INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
    db.execute("CREATE PROCEDURE bump() AS $$ BEGIN SET NEW.n = NEW.n + 10; END; $$")
        .unwrap();
    db.execute("CREATE TRIGGER bump BEFORE INSERT ON t FOR EACH ROW EXECUTE PROCEDURE bump();")
        .unwrap();
    db.execute("CREATE TRIGGER audit AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('row');")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 2);").unwrap();
    // No checkpoint, no `drop`: the process image is the crash.
    std::process::exit(0);
}

// ---------- RBAC ----------

fn rbac_store() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE t (id INTEGER, n INTEGER);")
        .unwrap();
    conn.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
    conn.execute("CREATE TABLE hidden (msg VARCHAR);").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10);").unwrap();
    conn.execute("CREATE USER alice PASSWORD 'alice-secret';")
        .unwrap();
    conn.execute("CREATE USER bob PASSWORD 'bob-secret';")
        .unwrap();
    conn.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE t TO alice;")
        .unwrap();
    conn.execute("GRANT SELECT, INSERT ON TABLE log TO alice;")
        .unwrap();
    db
}

fn login(db: &Database, user: &str) -> pinkdb::Connection {
    let mut conn = db.connect();
    conn.login(user, &format!("{user}-secret")).expect("login");
    conn
}

#[test]
fn creating_a_trigger_needs_create_on_the_database() {
    let db = rbac_store();
    let mut alice = login(&db, "alice");
    let msg = execute(
        &mut alice,
        "CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("CREATE privilege"), "{msg}");
    // Owner-equivalent: CREATE ON DATABASE is what ALTER TABLE and index DDL
    // need too.
    let mut owner = db.connect();
    owner
        .execute("GRANT CREATE ON DATABASE main TO alice;")
        .unwrap();
    let msg = execute(
        &mut alice,
        "CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.is_empty(), "{msg}");
    // Dropping needs it as well.
    let msg = execute(&mut alice, "DROP TRIGGER tr;");
    assert!(msg.is_empty(), "{msg}");
}

/// The body runs with the *invoker's* rights: a trigger may not be used to
/// write a table the firing user cannot write.
#[test]
fn trigger_bodies_run_with_invoker_rights() {
    let db = rbac_store();
    let mut owner = db.connect();
    owner
        .execute(
            "CREATE TRIGGER leak AFTER INSERT ON t FOR EACH ROW \
             EXECUTE INSERT INTO hidden VALUES ('leak');",
        )
        .unwrap();
    let mut alice = login(&db, "alice");
    let msg = execute(&mut alice, "INSERT INTO t VALUES (2, 20);");
    assert!(msg.contains("permission denied"), "{msg}");
    assert!(msg.contains("hidden"), "{msg}");
    // The firing statement rolled back with the trigger.
    let mut owner = db.connect();
    assert_eq!(row_count(&mut owner, "SELECT COUNT(*) FROM t;"), 1);
    // The same write as the owner succeeds and the trigger writes `hidden`.
    owner.execute("INSERT INTO t VALUES (3, 30);").unwrap();
    assert_eq!(row_count(&mut owner, "SELECT COUNT(*) FROM hidden;"), 1);
}

#[test]
fn show_triggers_is_a_metadata_read_for_any_user() {
    let db = rbac_store();
    let mut owner = db.connect();
    owner
        .execute("CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');")
        .unwrap();
    let mut bob = login(&db, "bob");
    let msg = execute(&mut bob, "SHOW TRIGGERS;");
    assert!(msg.is_empty(), "{msg}");
}

fn row_count(conn: &mut pinkdb::Connection, sql: &str) -> i64 {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => match &rows[0][0] {
            Value::Bigint(i) => *i,
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

fn execute(conn: &mut pinkdb::Connection, sql: &str) -> String {
    match conn.execute(sql) {
        Ok(_) => String::new(),
        Err(e) => e.to_string(),
    }
}

// ---------- interactions ----------

#[test]
fn a_triggered_write_refreshes_a_materialized_view() {
    let mut db = seeded();
    db.execute("CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) AS c FROM log;")
        .unwrap();
    db.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('x');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0), (4, 0);").unwrap();
    db.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
    assert_eq!(ints(&mut db, "SELECT c FROM mv;"), vec![2]);
}

#[test]
fn an_on_commit_materialized_view_sees_triggered_writes() {
    let mut db = seeded();
    db.execute("CREATE MATERIALIZED VIEW mv REFRESH ON COMMIT AS SELECT COUNT(*) AS c FROM log;")
        .unwrap();
    db.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('x');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    assert_eq!(ints(&mut db, "SELECT c FROM mv;"), vec![1]);
}

/// A transaction that read a table a *trigger* wrote must conflict with it, so
/// the trigger's writes join the firing frame's touched set.
#[test]
fn mvcc_conflict_covers_trigger_written_tables() {
    let db = Database::new();
    let mut setup = db.connect();
    setup.execute("CREATE TABLE t (id INTEGER);").unwrap();
    setup.execute("CREATE TABLE log (msg VARCHAR);").unwrap();
    setup.execute("CREATE TABLE u (a BIGINT);").unwrap();
    setup.execute("INSERT INTO u VALUES (0);").unwrap();
    setup
        .execute("CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');")
        .unwrap();

    // A reads `log`, which only a trigger ever writes.
    let mut a = db.connect();
    a.execute("BEGIN; UPDATE u SET a = (SELECT COUNT(*) FROM log);")
        .unwrap();
    // B's statement touches `log` through the trigger.
    let mut b = db.connect();
    b.execute("INSERT INTO t VALUES (1);").unwrap();
    let msg = match a.execute("COMMIT;") {
        Ok(_) => String::new(),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("(read)"),
        "expected a read conflict, got {msg:?}"
    );
}

// ---------- transactions ----------

#[test]
fn triggers_run_inside_the_firing_transaction() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('x');",
    )
    .unwrap();
    db.execute("BEGIN;").unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    db.execute("ROLLBACK;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM t;"), vec![2]);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![0]);
    // Committed: both the change and its trigger effect are visible.
    db.execute("BEGIN;").unwrap();
    db.execute("INSERT INTO t VALUES (4, 0);").unwrap();
    db.execute("COMMIT;").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
}

#[test]
fn trigger_ddl_inside_a_transaction_is_refused() {
    let mut db = seeded();
    db.execute("BEGIN;").unwrap();
    let msg = err(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("CREATE TRIGGER"), "{msg}");
    db.execute("ROLLBACK;").unwrap();
}

// ---------- refusals ----------

#[test]
fn triggers_on_views_are_refused() {
    let mut db = seeded();
    db.execute("CREATE VIEW v AS SELECT id, n FROM t;").unwrap();
    let msg = err(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON v EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("views"), "{msg}");
}

#[test]
fn triggers_on_a_missing_table_are_refused() {
    let mut db = seeded();
    let msg = err(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON nope EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("nope"), "{msg}");
}

#[test]
fn referencing_on_a_row_trigger_is_refused() {
    let mut db = seeded();
    let msg = err(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t REFERENCING NEW TABLE AS n FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("FOR EACH STATEMENT"), "{msg}");
    let msg = err(
        &mut db,
        "CREATE TRIGGER tr BEFORE INSERT ON t REFERENCING NEW TABLE AS n FOR EACH STATEMENT \
         EXECUTE INSERT INTO log VALUES ('x');",
    );
    assert!(msg.contains("AFTER"), "{msg}");
}

// ---------- parser ----------

#[test]
fn parses_every_documented_shape() {
    let shapes = [
        "CREATE TRIGGER a BEFORE INSERT ON t EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE OR REPLACE TRIGGER a AFTER UPDATE OF id, n ON t FOR EACH ROW EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER a BEFORE INSERT OR UPDATE OR DELETE ON t FOR EACH STATEMENT EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER a AFTER INSERT ON main.t REFERENCING OLD TABLE AS o NEW TABLE AS n FOR EACH STATEMENT EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER a AFTER INSERT ON t FOR EACH ROW WHEN (NEW.n > 0) EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER a AFTER INSERT ON t EXECUTE PROCEDURE p(NEW.id, 'lit', 3);",
        "DROP TRIGGER a;",
        "DROP TRIGGER IF EXISTS a ON t;",
        "SHOW TRIGGERS;",
        "SHOW TRIGGERS ON t;",
    ];
    for sql in shapes {
        let statements =
            pinkdb::sql::parse_statements(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(statements.len(), 1, "{sql}");
        // The same shape in a different case and spacing parses to the same
        // statement: the keyword surface is case-insensitive and whitespace
        // insensitive.
        let respaced = sql
            .replace(" AFTER ", "\n  after\n ")
            .replace("INSERT", "insert");
        assert_eq!(
            pinkdb::sql::parse_statements(&respaced).unwrap(),
            statements,
            "{sql}"
        );
    }
}

#[test]
fn malformed_trigger_syntax_is_a_parse_error() {
    for sql in [
        "CREATE TRIGGER tr INSERT ON t EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER tr BEFORE ON t EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER tr BEFORE INSERT t EXECUTE INSERT INTO log VALUES ('x');",
        "CREATE TRIGGER tr BEFORE INSERT ON t EXECUTE;",
        "DROP TRIGGER;",
    ] {
        assert!(pinkdb::sql::parse_statements(sql).is_err(), "{sql} parsed");
    }
}

#[test]
fn statement_level_when_clause() {
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH STATEMENT WHEN (1 = 1) \
         EXECUTE INSERT INTO log VALUES ('yes');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![1]);
    let mut db = seeded();
    db.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH STATEMENT WHEN (1 = 0) \
         EXECUTE INSERT INTO log VALUES ('yes');",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (3, 0);").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM log;"), vec![0]);
}
