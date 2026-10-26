//! Wave 94: three-part column references (`db.table.column`, `db.table.*`).
//!
//! Covers every clause position, cross-database joins (three-part
//! disambiguates, two-part is ambiguous), aliases, the current database,
//! CTEs/derived tables, correlated subqueries, GROUP BY/ORDER BY/HAVING
//! equivalence, UPDATE/DELETE, quoting/case, four-part refusal, rendering,
//! views/macros (including a durable replay) and prepared statements.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

// ---------- scratch durable files ----------

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_three_part_{tag}_{}_{}",
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

// ---------- helpers ----------

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn names(db: &mut PinkDb, sql: &str) -> Vec<String> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { names, .. } => names,
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

fn strings(db: &mut PinkDb, sql: &str) -> Vec<Option<String>> {
    rows(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => Some(s.clone()),
            Value::Null => None,
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

fn explain(db: &mut PinkDb, sql: &str) -> String {
    rows(db, &format!("EXPLAIN {sql};"))
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected EXPLAIN text, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn seeded() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE DATABASE db1; CREATE DATABASE db2;")
        .unwrap();
    db.execute("CREATE TABLE main.t (a INTEGER, b VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO main.t VALUES (1, 'main'), (9, 'nine');")
        .unwrap();
    db.execute("CREATE TABLE db1.t (a INTEGER, b VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO db1.t VALUES (1, 'one'), (2, 'two'), (3, NULL);")
        .unwrap();
    db.execute("CREATE TABLE db2.t (a INTEGER, b VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO db2.t VALUES (2, 'deux'), (3, 'trois'), (4, 'quatre');")
        .unwrap();
    db.execute("CREATE TABLE db1.u (a INTEGER, c VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO db1.u VALUES (1, 'x'), (2, 'y');")
        .unwrap();
    db
}

// ---------- every clause position (rule 1) ----------

#[test]
fn three_part_references_work_in_every_clause_position() {
    let mut db = seeded();

    // SELECT list + WHERE + ORDER BY.
    assert_eq!(
        ints(
            &mut db,
            "SELECT db1.t.a FROM db1.t WHERE db1.t.a > 1 ORDER BY db1.t.a DESC;"
        ),
        vec![3, 2]
    );
    // Function arguments.
    assert_eq!(
        strings(
            &mut db,
            "SELECT upper(db1.t.b) FROM db1.t WHERE db1.t.a = 1;"
        ),
        vec![Some("ONE".to_string())]
    );
    // JOIN ON.
    assert_eq!(
        strings(
            &mut db,
            "SELECT db1.u.c FROM db1.t JOIN db1.u ON db1.t.a = db1.u.a \
             ORDER BY db1.t.a;"
        ),
        vec![Some("x".to_string()), Some("y".to_string())]
    );
    // GROUP BY + HAVING (HAVING takes the same aggregate forms as today).
    assert_eq!(
        ints(
            &mut db,
            "SELECT db1.t.a FROM db1.t GROUP BY db1.t.a \
             HAVING max(db1.t.a) >= 2 ORDER BY db1.t.a;"
        ),
        vec![2, 3]
    );
    // Window PARTITION BY / ORDER BY.
    assert_eq!(
        ints(
            &mut db,
            "SELECT row_number() OVER (PARTITION BY db1.t.b ORDER BY db1.t.a) \
             FROM db1.t ORDER BY db1.t.a;"
        ),
        vec![1, 1, 1]
    );
    // Uncorrelated subquery (inner body).
    assert_eq!(
        ints(
            &mut db,
            "SELECT a FROM db1.u WHERE a IN (SELECT db1.t.a FROM db1.t) ORDER BY a;"
        ),
        vec![1, 2]
    );
    // CTE body.
    assert_eq!(
        ints(
            &mut db,
            "WITH c AS (SELECT db1.t.a AS x FROM db1.t WHERE db1.t.a < 3) \
             SELECT x FROM c ORDER BY x;"
        ),
        vec![1, 2]
    );
    // `db.table.*` alone and next to other columns.
    assert_eq!(names(&mut db, "SELECT db1.t.* FROM db1.t;"), vec!["a", "b"]);
    assert_eq!(
        names(
            &mut db,
            "SELECT db1.u.c, db1.t.* FROM db1.t JOIN db1.u ON db1.t.a = db1.u.a \
             ORDER BY db1.t.a;"
        ),
        vec!["db1.u.c", "a", "b"]
    );
    // `db.table.*` where every column is explicitly qualified.
    assert_eq!(
        rows(&mut db, "SELECT db1.t.* FROM db1.t ORDER BY db1.t.a;")
            .into_iter()
            .map(|r| r[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
    );
}

// ---------- resolution, current database, aliases (rules 2 + 3) ----------

#[test]
fn unqualified_from_belongs_to_the_current_database() {
    let mut db = seeded();
    // `t` is main.t while main is current.
    assert_eq!(
        ints(&mut db, "SELECT main.t.a FROM t ORDER BY 1;"),
        vec![1, 9]
    );
    db.execute("USE db1;").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT db1.t.a FROM t ORDER BY 1;"),
        vec![1, 2, 3]
    );
    let e = err(&mut db, "SELECT main.t.a FROM t;");
    assert!(
        e.contains("no table \"main.t\" in this query's FROM clause"),
        "{e}"
    );
    db.execute("USE main;").unwrap();
}

#[test]
fn aliases_hide_the_original_name_and_the_workaround_still_works() {
    let mut db = seeded();
    // The alias rule names the alias and the workaround.
    let e = err(&mut db, "SELECT db1.t.a FROM db1.t AS x;");
    assert!(
        e.contains("table \"db1.t\" is aliased as \"x\"")
            && e.contains("refer to its columns as x.a"),
        "{e}"
    );
    // `db.table.*` follows the same rule.
    let e = err(&mut db, "SELECT db1.t.* FROM db1.t AS x;");
    assert!(
        e.contains("table \"db1.t\" is aliased as \"x\"")
            && e.contains("refer to its columns as x.*"),
        "{e}"
    );
    // The workaround is unchanged.
    assert_eq!(
        ints(&mut db, "SELECT x.a FROM db1.t AS x ORDER BY x.a;"),
        vec![1, 2, 3]
    );
    // The existing two-part rule is unchanged too: an alias hides the name.
    let e = err(&mut db, "SELECT t.a FROM db1.t AS x;");
    assert!(e.contains("no column \"t.a\""), "{e}");
    // An unaliased table of the same name can still be referenced.
    assert_eq!(
        ints(
            &mut db,
            "SELECT db1.t.a FROM db1.t JOIN db1.t AS x ON db1.t.a = x.a ORDER BY 1;"
        ),
        vec![1, 2, 3]
    );
}

// ---------- cross-database joins and ambiguity (rule 4) ----------

#[test]
fn same_table_name_in_two_databases_is_disambiguated_by_three_parts() {
    let mut db = seeded();
    assert_eq!(
        rows(
            &mut db,
            "SELECT db1.t.a, db2.t.b FROM db1.t JOIN db2.t ON db1.t.a = db2.t.a \
             ORDER BY db1.t.a;"
        ),
        vec![
            vec![Value::Integer(2), Value::Varchar("deux".to_string())],
            vec![Value::Integer(3), Value::Varchar("trois".to_string())],
        ]
    );
    // Bare `*` expands both tables even though they share a name.
    assert_eq!(
        names(
            &mut db,
            "SELECT * FROM db1.t JOIN db2.t ON db1.t.a = db2.t.a;"
        ),
        vec!["a", "b", "a", "b"]
    );
    // A two-part column reference cannot choose between db1.t and db2.t.
    let e = err(
        &mut db,
        "SELECT t.a FROM db1.t JOIN db2.t ON db1.t.a = db2.t.a;",
    );
    assert!(e.contains("ambiguous"), "{e}");
    assert!(e.contains("db1.t") && e.contains("db2.t"), "{e}");
    assert!(e.contains("db.t.a") || e.contains("three-part"), "{e}");
    // Neither can `t.*`.
    let e = err(
        &mut db,
        "SELECT t.* FROM db1.t JOIN db2.t ON db1.t.a = db2.t.a;",
    );
    assert!(e.contains("ambiguous") && e.contains("db.t.*"), "{e}");
    // A bare column keeps today's ambiguity rule.
    let e = err(
        &mut db,
        "SELECT a FROM db1.t JOIN db2.t ON db1.t.a = db2.t.a;",
    );
    assert!(e.contains("column \"a\" is ambiguous"), "{e}");
    // Aliases resolve it too.
    assert_eq!(
        ints(
            &mut db,
            "SELECT x.a FROM db1.t AS x JOIN db2.t AS y ON x.a = y.a ORDER BY 1;"
        ),
        vec![2, 3]
    );
}

#[test]
fn self_join_with_aliases_is_unchanged() {
    let mut db = seeded();
    db.execute("CREATE TABLE db1.s (a INTEGER, b VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO db1.s VALUES (1, 'x'), (2, 'x'), (3, 'y');")
        .unwrap();
    assert_eq!(
        rows(
            &mut db,
            "SELECT x.a, y.a FROM db1.s AS x JOIN db1.s AS y ON x.b = y.b \
             WHERE x.a < y.a ORDER BY x.a, y.a;"
        ),
        vec![vec![Value::Integer(1), Value::Integer(2)]]
    );
}

// ---------- CTEs / derived tables have no database (rule 2) ----------

#[test]
fn ctes_and_derived_tables_never_match_a_three_part_reference() {
    let mut db = seeded();
    let e = err(&mut db, "WITH c AS (SELECT 1 AS a) SELECT main.c.a FROM c;");
    assert!(
        e.contains("no table \"main.c\" in this query's FROM clause"),
        "{e}"
    );
    let e = err(&mut db, "SELECT main.d.a FROM (SELECT 1 AS a) AS d;");
    assert!(
        e.contains("no table \"main.d\" in this query's FROM clause"),
        "{e}"
    );
}

// ---------- correlated subqueries ----------

/// The engine still refuses correlated subqueries (a documented v1 limit —
/// SEE the README "Known v1 limits"): an inner query binds in a fresh scope,
/// so an outer `db.t.c` is not in the inner FROM clause. A three-part
/// reference gets the same clean, full-reference error as any other outer
/// column. This test pins that boundary for wave 94.
#[test]
fn correlated_outer_three_part_reference_resolves_like_two_part() {
    let mut db = seeded();
    // The subquery names `db1.t`, which its own FROM lacks but the enclosing
    // block has: that is an outer reference, and the two-part spelling must
    // resolve to the same row.
    let three = ints(
        &mut db,
        "SELECT a FROM db1.t WHERE EXISTS \
         (SELECT 1 FROM db1.u WHERE db1.t.a = db1.u.a) ORDER BY a;",
    );
    let two = ints(
        &mut db,
        "SELECT a FROM db1.t WHERE EXISTS \
         (SELECT 1 FROM db1.u WHERE t.a = u.a) ORDER BY a;",
    );
    assert_eq!(three, two);
    assert_eq!(three, vec![1, 2]);
}

// ---------- grouping / ordering equivalence (rule 7) ----------

#[test]
fn three_part_group_and_order_references_bind_like_two_part_forms() {
    let mut db = seeded();
    let three = rows(
        &mut db,
        "SELECT db1.t.a, COUNT(*) FROM db1.t GROUP BY db1.t.a ORDER BY db1.t.a;",
    );
    let two = rows(
        &mut db,
        "SELECT db1.t.a, COUNT(*) FROM db1.t GROUP BY t.a ORDER BY t.a;",
    );
    let bare = rows(
        &mut db,
        "SELECT db1.t.a, COUNT(*) FROM db1.t GROUP BY a ORDER BY a;",
    );
    assert_eq!(three, two);
    assert_eq!(three, bare);

    let having_three = rows(
        &mut db,
        "SELECT db1.t.a, COUNT(*) FROM db1.t GROUP BY db1.t.a \
         HAVING max(db1.t.a) >= 2 ORDER BY db1.t.a;",
    );
    let having_two = rows(
        &mut db,
        "SELECT db1.t.a, COUNT(*) FROM db1.t GROUP BY t.a \
         HAVING max(t.a) >= 2 ORDER BY t.a;",
    );
    assert_eq!(having_three, having_two);

    let order_three = ints(&mut db, "SELECT db1.t.a FROM db1.t ORDER BY db1.t.a DESC;");
    let order_two = ints(&mut db, "SELECT db1.t.a FROM db1.t ORDER BY t.a DESC;");
    assert_eq!(order_three, order_two);

    // A select alias still wins in ORDER BY, next to the three-part form.
    assert_eq!(
        ints(&mut db, "SELECT db1.t.a AS x FROM db1.t ORDER BY x DESC;"),
        order_three
    );
}

// ---------- UPDATE / DELETE (rule 6) ----------

#[test]
fn update_set_targets_accept_all_three_qualified_forms() {
    let mut db = seeded();
    db.execute("UPDATE db1.t SET db1.t.b = 'ONE' WHERE db1.t.a = 1;")
        .unwrap();
    db.execute("UPDATE db1.t SET t.b = 'TWO' WHERE t.a = 2;")
        .unwrap();
    db.execute("UPDATE db1.t SET b = db1.t.b || '!' WHERE db1.t.a = 3;")
        .unwrap();
    assert_eq!(
        strings(&mut db, "SELECT db1.t.b FROM db1.t ORDER BY db1.t.a;"),
        vec![Some("ONE".to_string()), Some("TWO".to_string()), None,]
    );
    // The right-hand side sees the target's table through three parts.
    db.execute("UPDATE db1.t SET db1.t.b = upper(db1.t.b) WHERE db1.t.a = 1;")
        .unwrap();
    assert_eq!(
        strings(&mut db, "SELECT db1.t.b FROM db1.t WHERE db1.t.a = 1;"),
        vec![Some("ONE".to_string())]
    );

    // A mismatched database names both sides.
    let e = err(&mut db, "UPDATE db1.t SET db2.t.b = 'x';");
    assert!(e.contains("db2.t.b") && e.contains("db1.t"), "{e}");
    // A mismatched table names both sides.
    let e = err(&mut db, "UPDATE db1.t SET u.b = 'x';");
    assert!(e.contains("u.b") && e.contains("db1.t"), "{e}");
    let e = err(&mut db, "UPDATE db1.t SET db1.t.nope = 1;");
    assert!(e.contains("no column \"nope\""), "{e}");
    // The plain form is unchanged.
    db.execute("UPDATE db1.t SET b = 'plain' WHERE a = 1;")
        .unwrap();
}

#[test]
fn delete_where_resolves_three_part_references() {
    let mut db = seeded();
    db.execute("DELETE FROM db1.t WHERE db1.t.a = 3;").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT db1.t.a FROM db1.t ORDER BY 1;"),
        vec![1, 2]
    );
    let e = err(&mut db, "DELETE FROM db1.t WHERE db2.t.a = 1;");
    assert!(
        e.contains("no table \"db2.t\" in this query's FROM clause"),
        "{e}"
    );
}

// ---------- identifiers, case, quoting, four-part refusal ----------

#[test]
fn quoted_multi_word_and_case_insensitive_parts_work() {
    let mut db = PinkDb::new();
    db.execute("CREATE DATABASE \"My Db\";").unwrap();
    db.execute("CREATE TABLE \"My Db\".\"t 1\" (\"a b\" INTEGER, c INTEGER);")
        .unwrap();
    db.execute("INSERT INTO \"My Db\".\"t 1\" VALUES (7, 8);")
        .unwrap();
    assert_eq!(
        ints(
            &mut db,
            "SELECT \"My Db\".\"t 1\".\"a b\" FROM \"My Db\".\"t 1\";"
        ),
        vec![7]
    );
    assert_eq!(
        ints(
            &mut db,
            "SELECT \"MY DB\".\"T 1\".\"A B\" FROM \"MY DB\".\"T 1\";"
        ),
        vec![7]
    );
    // Unquoted identifiers stay case-insensitive in every part.
    db.execute("CREATE DATABASE MyDb;").unwrap();
    db.execute("CREATE TABLE MyDb.T (A INTEGER);").unwrap();
    db.execute("INSERT INTO MyDb.T VALUES (8);").unwrap();
    assert_eq!(ints(&mut db, "SELECT mydb.t.a FROM mydb.t;"), vec![8]);
    assert_eq!(ints(&mut db, "SELECT MYDB.T.A FROM MYDB.T;"), vec![8]);
    // Four or more parts are refused clearly.
    let e = db
        .execute("SELECT a.b.c.d FROM a;")
        .expect_err("four parts")
        .to_string();
    assert!(e.contains("at most three parts"), "{e}");
    assert!(e.contains("got \"a.b.c.d\""), "{e}");
}

// ---------- full-reference errors (rule 5) ----------

#[test]
fn every_three_part_error_names_the_full_reference() {
    let mut db = seeded();
    let e = err(&mut db, "SELECT nope.t.a FROM db1.t;");
    assert!(e.contains("database \"nope\" not found"), "{e}");
    let e = err(&mut db, "SELECT db1.nope.a FROM db1.t;");
    assert!(
        e.contains("no table \"db1.nope\" in this query's FROM clause"),
        "{e}"
    );
    let e = err(&mut db, "SELECT db1.t.nope FROM db1.t;");
    assert!(e.contains("no column \"db1.t.nope\""), "{e}");
    // A table that is present in another database is not in scope here.
    let e = err(&mut db, "SELECT db2.t.a FROM db1.t;");
    assert!(
        e.contains("no table \"db2.t\" in this query's FROM clause"),
        "{e}"
    );
    let e = err(&mut db, "SELECT db1.nope.* FROM db1.t;");
    assert!(
        e.contains("no table \"db1.nope\" in this query's FROM clause"),
        "{e}"
    );
}

// ---------- rendering (rule 10) ----------

#[test]
fn three_part_references_render_faithfully() {
    let mut db = seeded();
    // Default output names keep the written reference (the expression text).
    assert_eq!(
        names(&mut db, "SELECT db1.t.a, db1.t.* FROM db1.t;"),
        vec!["db1.t.a", "a", "b"]
    );
    // EXPLAIN renders the bound plan without losing the reference's
    // resolution (the column is the same as the two-part form's).
    let text = explain(&mut db, "SELECT db1.t.a FROM db1.t WHERE db1.t.a = 1");
    assert!(!text.is_empty(), "EXPLAIN produced no text");
    assert!(text.contains("db1.t.a"), "{text}");
    let two = explain(&mut db, "SELECT t.a FROM db1.t WHERE t.a = 1");
    assert!(two.contains("t.a"), "{two}");
    // DESCRIBE and SHOW are unaffected.
    assert!(!rows(&mut db, "DESCRIBE db1.t;").is_empty());
}

// ---------- views, macros, procedures ----------

#[test]
fn views_and_macros_use_three_part_references() {
    let mut db = seeded();
    db.execute(
        "CREATE VIEW db1.v AS \
         SELECT db1.t.a AS a, upper(db1.t.b) AS b FROM db1.t WHERE db1.t.a > 1;",
    )
    .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a, b FROM db1.v ORDER BY a;"),
        vec![
            vec![Value::Integer(2), Value::Varchar("TWO".to_string())],
            vec![Value::Integer(3), Value::Null],
        ]
    );
    // A view whose body hides the table behind an alias gets the alias error.
    let e = err(
        &mut db,
        "CREATE VIEW db1.v2 AS SELECT db1.t.a FROM db1.t AS x;",
    );
    assert!(e.contains("table \"db1.t\" is aliased as \"x\""), "{e}");

    // A scalar macro body may carry a three-part reference; only bare
    // parameter names are substituted (rule 8).
    db.execute("CREATE MACRO m(x) AS x + db1.t.a;").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT m(10) FROM db1.t ORDER BY db1.t.a;"),
        vec![11, 12, 13]
    );
    db.execute("CREATE MACRO shadow(db1) AS db1.t.a + db1;")
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT shadow(5) FROM db1.t ORDER BY db1.t.a;"),
        vec![6, 7, 8]
    );
    // Table macro body.
    db.execute(
        "CREATE MACRO pick(x) AS TABLE \
         SELECT db1.t.a AS a FROM db1.t WHERE db1.t.a > x;",
    )
    .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT a FROM pick(1) ORDER BY a;"),
        vec![2, 3]
    );

    // Procedure variables still shadow only unqualified names.
    db.execute(
        "CREATE PROCEDURE p(v INTEGER) AS $$ BEGIN \
           INSERT INTO db1.u VALUES (v, upper(db1.t.b)); \
         END $$;",
    )
    .unwrap();
    db.execute("CALL p(1);").unwrap_err(); // db1.t.b is not in the INSERT's scope
    db.execute(
        "CREATE PROCEDURE q(v INTEGER) AS $$ BEGIN \
           INSERT INTO db1.u VALUES (v, 'from-proc'); \
           UPDATE db1.u SET c = 'seen' WHERE db1.u.a = v; \
         END $$;",
    )
    .unwrap();
    db.execute("CALL q(7);").unwrap();
    assert_eq!(
        strings(&mut db, "SELECT c FROM db1.u WHERE a = 7;"),
        vec![Some("seen".to_string())]
    );
}

#[test]
fn durable_replay_keeps_three_part_materialized_view_and_macro_definitions() {
    let s = Scratch::new("durable");
    {
        let mut db = PinkDb::open_durable_with(s.path(), SyncPolicy::Batch)
            .unwrap()
            .0;
        db.execute("CREATE DATABASE db1;").unwrap();
        db.execute("CREATE TABLE db1.t (a INTEGER);").unwrap();
        db.execute("INSERT INTO db1.t VALUES (1), (2);").unwrap();
        // Views are session-local (never persisted, see the README limit); a
        // materialized view's defining query IS persisted and replayed.
        db.execute(
            "CREATE MATERIALIZED VIEW db1.mv AS \
             SELECT db1.t.a AS a FROM db1.t WHERE db1.t.a > 1;",
        )
        .unwrap();
        db.execute("CREATE MACRO db1.m(x) AS x + db1.t.a;").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM db1.mv;"), vec![2]);
    }
    // Reopen replays the logged definitions (MV definitions and macros).
    let mut db = PinkDb::open_durable_with(s.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    assert_eq!(ints(&mut db, "SELECT a FROM db1.mv;"), vec![2]);
    db.execute("USE db1;").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT m(10) FROM db1.t ORDER BY db1.t.a;"),
        vec![11, 12]
    );
}

// ---------- prepared statements ----------

#[test]
fn prepared_statements_bind_three_part_references() {
    let mut db = seeded();
    let stmt = db
        .prepare("SELECT db1.t.a FROM db1.t WHERE db1.t.a = ?;")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 1);
    match db.execute_prepared(&stmt, &[Value::Integer(2)]).unwrap() {
        QueryResult::Selected { names, rows, .. } => {
            assert_eq!(names, vec!["db1.t.a".to_string()]);
            assert_eq!(rows, vec![vec![Value::Integer(2)]]);
        }
        other => panic!("{other:?}"),
    }
    // A three-part SET target through the prepared path.
    let update = db
        .prepare("UPDATE db1.t SET db1.t.b = ? WHERE db1.t.a = ?;")
        .unwrap();
    db.execute_prepared(
        &update,
        &[Value::Varchar("prepared".to_string()), Value::Integer(1)],
    )
    .unwrap();
    assert_eq!(
        strings(&mut db, "SELECT db1.t.b FROM db1.t WHERE db1.t.a = 1;"),
        vec![Some("prepared".to_string())]
    );
}

// ---------- RBAC is unchanged (rule 9) ----------

#[test]
fn three_part_reference_still_needs_table_access() {
    use pinkdb::Database;
    let shared = Database::new();
    let mut admin = shared.connect();
    admin
        .execute(
            "CREATE DATABASE db1; \
             CREATE TABLE db1.secret (a INTEGER); \
             INSERT INTO db1.secret VALUES (42);",
        )
        .unwrap();
    admin
        .execute("CREATE USER alice PASSWORD 'pw'; CREATE ROLE reader;")
        .unwrap();
    admin
        .execute("GRANT SELECT ON TABLE db1.secret TO reader;")
        .unwrap();
    admin.execute("GRANT reader TO alice;").unwrap();
    let mut alice = shared.connect();
    alice.login("alice", "pw").expect("login");
    assert_eq!(
        match alice
            .execute("SELECT db1.secret.a FROM db1.secret;")
            .unwrap()
            .pop()
            .unwrap()
        {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("{other:?}"),
        },
        vec![vec![Value::Integer(42)]]
    );
    let e = alice
        .execute("SELECT db1.other.a FROM db1.other;")
        .unwrap_err()
        .to_string();
    assert!(e.contains("not found") || e.contains("no table"), "{e}");
}
