//! Wave 77: SQL macros end to end — syntax, defaults and named arguments,
//! scalar/table expansion, nested use, views / materialized views /
//! procedures, recursion refusal, introspection, the RBAC matrix,
//! `.pink` persistence (flags bit 6) and durable WAL replay.

use std::path::PathBuf;
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
            "pinkdb_macros_{tag}_{}_{}",
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
    db.execute("CREATE TABLE emp (id INTEGER, name VARCHAR, salary DOUBLE);")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1, 'ann', 100.0), (2, 'bob', 50.0), (3, 'cat', 200.0);")
        .unwrap();
    db
}

// ---------- syntax, expansion, defaults ----------

#[test]
fn scalar_macros_expand_in_every_expression_position() {
    let mut db = seeded();
    db.execute("CREATE MACRO add(a, b) AS a + b;").unwrap();
    assert_eq!(ints(&mut db, "SELECT add(2, 3);"), vec![5]);
    assert_eq!(
        ints(
            &mut db,
            "SELECT add(id, 10) FROM emp WHERE add(id, 0) > 1 ORDER BY id;"
        ),
        vec![12, 13]
    );
    assert_eq!(ints(&mut db, "SELECT SUM(add(id, id)) FROM emp;"), vec![12]);
    // A macro body may itself be an aggregate; it binds in the caller's
    // aggregate-allowed context.
    db.execute("CREATE MACRO count_all() AS COUNT(*);").unwrap();
    assert_eq!(ints(&mut db, "SELECT count_all() FROM emp;"), vec![3]);
    // Argument expressions are spliced, evaluated at the call site.
    assert_eq!(
        ints(&mut db, "SELECT add(id * 2, 1) FROM emp WHERE id = 2;"),
        vec![5]
    );
    // Macros may be used in ORDER BY / GROUP BY expressions.
    assert_eq!(
        ints(
            &mut db,
            "SELECT add(id, 0) AS k FROM emp GROUP BY add(id, 0) ORDER BY add(id, 0);"
        ),
        vec![1, 2, 3]
    );
    // Prepared statements keep their own `?` slot through expansion.
    let stmt = db.prepare("SELECT add(?, 1);").unwrap();
    match db.execute_prepared(&stmt, &[Value::Integer(41)]).unwrap() {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![Value::Integer(42)]]),
        other => panic!("{other:?}"),
    }
    // DML positions expand exactly like SELECT expressions.
    db.execute("UPDATE emp SET salary = salary + add(1, 1) WHERE id = 1;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT salary FROM emp WHERE id = 1;"),
        vec![vec![Value::Double(102.0)]]
    );
    db.execute("DELETE FROM emp WHERE id = add(1, 1);").unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM emp;"), vec![2]);
    // Quoted macro names keep their spelling as the display name and resolve
    // case-insensitively, like every catalog object.
    db.execute("CREATE MACRO \"Weird Name\"(x) AS x + 1;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT \"Weird Name\"(2);"), vec![3]);
    assert_eq!(
        rows(&mut db, "SHOW MACROS;")
            .into_iter()
            .map(|r| r[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Varchar("add".to_string()),
            Value::Varchar("count_all".to_string()),
            Value::Varchar("Weird Name".to_string())
        ]
    );
    db.execute("DROP MACRO \"Weird Name\";").unwrap();
}

#[test]
fn macros_expand_inside_windows_and_ctes() {
    let mut db = seeded();
    db.execute("CREATE MACRO rn() AS ROW_NUMBER() OVER (ORDER BY id);")
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT rn() FROM emp ORDER BY id;"),
        vec![1, 2, 3]
    );
    db.execute("CREATE MACRO over_x(x) AS x + 1;").unwrap();
    assert_eq!(
        ints(
            &mut db,
            "SELECT ROW_NUMBER() OVER (ORDER BY over_x(id) DESC) FROM emp ORDER BY id;"
        ),
        vec![3, 2, 1]
    );
    // A table macro call inside a CTE, with the parameter in the CTE body.
    db.execute("CREATE MACRO many(min_id) AS TABLE SELECT * FROM emp WHERE id >= min_id;")
        .unwrap();
    assert_eq!(
        ints(
            &mut db,
            "WITH m AS (SELECT id FROM many(2)) SELECT COUNT(*) FROM m;"
        ),
        vec![2]
    );
}

#[test]
fn defaults_and_named_arguments_follow_duckdb_shape() {
    let mut db = PinkDb::new();
    db.execute("CREATE MACRO def(a, b := 10) AS a + b;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT def(1);"), vec![11]);
    assert_eq!(ints(&mut db, "SELECT def(1, 2);"), vec![3]);
    // Named arguments bind by name, in any order, and combine with defaults.
    assert_eq!(ints(&mut db, "SELECT def(b := 5, a := 1);"), vec![6]);
    assert_eq!(ints(&mut db, "SELECT def(a := 1);"), vec![11]);
    // `=>` is accepted too (DuckDB's other spelling).
    assert_eq!(ints(&mut db, "SELECT def(a => 2, b => 3);"), vec![5]);
}

#[test]
fn nested_macros_expand_during_one_bind() {
    let mut db = seeded();
    db.execute("CREATE MACRO inc(x) AS x + 1;").unwrap();
    db.execute("CREATE MACRO inc2(x) AS inc(inc(x));").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT inc2(id) FROM emp ORDER BY id;"),
        vec![3, 4, 5]
    );
    // A macro can shadow a built-in of the same name.
    db.execute("CREATE MACRO abs(x) AS 42;").unwrap();
    assert_eq!(ints(&mut db, "SELECT abs(-1);"), vec![42]);
}

#[test]
fn table_macros_expand_in_from_and_join_positions() {
    let mut db = seeded();
    db.execute("CREATE MACRO high(s) AS TABLE SELECT * FROM emp WHERE salary > s;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT name FROM high(60) ORDER BY name;"),
        vec![
            vec![Value::Varchar("ann".to_string())],
            vec![Value::Varchar("cat".to_string())]
        ]
    );
    // Aliases, joins and the derived-table machinery work unchanged.
    assert_eq!(
        ints(
            &mut db,
            "SELECT COUNT(*) FROM high(0) h JOIN emp e ON h.id = e.id;"
        ),
        vec![3]
    );
    db.execute("CREATE MACRO everyone() AS TABLE SELECT * FROM emp;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM everyone();"), vec![3]);
    // A table macro may call another table macro and use parameters inside
    // its body's subqueries/CTEs.
    db.execute(
        "CREATE MACRO top(n) AS TABLE \
           WITH ranked AS (SELECT * FROM everyone() WHERE salary IS NOT NULL) \
           SELECT id, name FROM ranked WHERE salary >= n ORDER BY salary DESC;",
    )
    .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT name FROM top(100) ORDER BY name;"),
        vec![
            vec![Value::Varchar("ann".to_string())],
            vec![Value::Varchar("cat".to_string())]
        ]
    );
    // Defaults on a table macro.
    db.execute("CREATE MACRO atleast(x := 100) AS TABLE SELECT * FROM emp WHERE salary >= x;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM atleast();"), vec![2]);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM atleast(40);"), vec![3]);
}

#[test]
fn default_expressions_are_constant_and_spliced() {
    let mut db = PinkDb::new();
    db.execute("CREATE MACRO f(x := 1 + 2) AS x * 2;").unwrap();
    assert_eq!(ints(&mut db, "SELECT f();"), vec![6]);
    // A default may call a scalar function.
    db.execute("CREATE MACRO g(x := upper('ab')) AS length(x);")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT g();"), vec![2]);
}

// ---------- errors and recursion ----------

#[test]
fn definition_errors_are_clean() {
    let mut db = PinkDb::new();
    let e = err(&mut db, "CREATE MACRO f(a, a) AS a;");
    assert!(e.contains("declared twice"), "{e}");
    let e = err(&mut db, "CREATE MACRO f(a := 1, b) AS a + b;");
    assert!(e.contains("without a default follows"), "{e}");
    let e = err(&mut db, "CREATE MACRO f(a INTEGER) AS a;");
    assert!(e.contains("untyped macro parameters"), "{e}");
    let e = err(&mut db, "CREATE MACRO f(x := a) AS x;");
    assert!(
        e.contains("constant expression") || e.contains("no column"),
        "{e}"
    );
    let e = err(&mut db, "CREATE MACRO f(x := (SELECT 1)) AS x;");
    assert!(e.contains("subqueries"), "{e}");
    let e = err(&mut db, "CREATE MACRO f(x := COUNT(*)) AS x;");
    assert!(e.contains("aggregate"), "{e}");
    let e = err(&mut db, "CREATE MACRO f(x) AS (SELECT ?);");
    assert!(
        e.contains("parameters are not supported in CREATE MACRO"),
        "{e}"
    );
    // The body is a template: a typo is a first-use error, not a definition
    // error (documented divergence from DuckDB, which binds at CREATE).
    db.execute("CREATE MACRO bad() AS nope(1);").unwrap();
    let e = err(&mut db, "SELECT bad();");
    assert!(e.contains("unknown function \"nope\""), "{e}");
    db.execute("CREATE MACRO bad_table() AS TABLE SELECT * FROM nope;")
        .unwrap();
    let e = err(&mut db, "SELECT * FROM bad_table();");
    assert!(e.contains("nope"), "{e}");
}

#[test]
fn call_errors_are_clean() {
    let mut db = seeded();
    db.execute("CREATE MACRO add(a, b) AS a + b;").unwrap();
    db.execute("CREATE MACRO tbl(x) AS TABLE SELECT * FROM emp WHERE id = x;")
        .unwrap();
    let e = err(&mut db, "SELECT add(1);");
    assert!(e.contains("does not support 1 argument"), "{e}");
    let e = err(&mut db, "SELECT add(1, 2, 3);");
    assert!(e.contains("does not support 3 argument"), "{e}");
    let e = err(&mut db, "SELECT add(nope := 1, b := 2);");
    assert!(e.contains("has no parameter \"nope\""), "{e}");
    let e = err(&mut db, "SELECT add(1, a := 2);");
    assert!(e.contains("supplied twice"), "{e}");
    let e = err(&mut db, "SELECT tbl(1);");
    assert!(e.contains("is a table macro"), "{e}");
    let e = err(&mut db, "SELECT * FROM add(1, 2);");
    assert!(e.contains("is a scalar macro"), "{e}");
    // Named arguments are macro-only.
    let e = err(&mut db, "SELECT abs(x := 1);");
    assert!(e.contains("named arguments are only supported"), "{e}");
    let e = err(&mut db, "SELECT add(DISTINCT 1, 2);");
    assert!(
        e.contains("DISTINCT is not supported in a macro call"),
        "{e}"
    );
    let e = err(&mut db, "DROP MACRO nope;");
    assert!(e.contains("macro \"nope\" not found"), "{e}");
}

#[test]
fn direct_and_mutual_recursion_are_refused_at_create() {
    let mut db = PinkDb::new();
    let e = err(&mut db, "CREATE MACRO fact(n) AS n * fact(n - 1);");
    assert!(e.contains("recursive"), "{e}");
    let e = err(&mut db, "CREATE MACRO a(n) AS TABLE SELECT * FROM a(n);");
    assert!(e.contains("recursive"), "{e}");
    // Mutual recursion, whether the second definition closes the cycle or the
    // first one names a missing macro (the call graph still reaches itself).
    db.execute("CREATE MACRO b(n) AS n + 1;").unwrap();
    let e = err(&mut db, "CREATE MACRO a(n) AS b(a(n));");
    assert!(e.contains("recursive"), "{e}");
    let e = err(&mut db, "CREATE OR REPLACE MACRO b(n) AS b(n);");
    assert!(e.contains("recursive"), "{e}");
    // A forward-referenced pair cannot sneak a cycle in: the second
    // definition closes it and is refused.
    db.execute("CREATE MACRO fw_a(n) AS fw_b(n);").unwrap();
    let e = err(&mut db, "CREATE MACRO fw_b(n) AS fw_a(n);");
    assert!(e.contains("recursive"), "{e}");
    // A forward reference that stays acyclic resolves at first use.
    db.execute("CREATE MACRO uses_later(n) AS later(n) + 1;")
        .unwrap();
    db.execute("CREATE MACRO later(n) AS n * 2;").unwrap();
    assert_eq!(ints(&mut db, "SELECT uses_later(3);"), vec![7]);
}

// ---------- views, materialized views, procedures ----------

#[test]
fn views_rebind_macros_at_every_use() {
    let mut db = seeded();
    db.execute("CREATE MACRO twice(x) AS x * 2;").unwrap();
    db.execute("CREATE VIEW v AS SELECT id, twice(id) AS y FROM emp;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT y FROM v ORDER BY y;"), vec![2, 4, 6]);
    // Redefining the macro changes the next bind of the view.
    db.execute("CREATE OR REPLACE MACRO twice(x) AS x * 10;")
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT y FROM v ORDER BY y;"),
        vec![10, 20, 30]
    );
    // Dropping it makes the view's next bind a clear "not found" error, and
    // recreating it heals the view.
    db.execute("DROP MACRO twice;").unwrap();
    let e = err(&mut db, "SELECT * FROM v;");
    assert!(e.contains("unknown function \"twice\""), "{e}");
    db.execute("CREATE MACRO twice(x) AS x * 2;").unwrap();
    assert_eq!(ints(&mut db, "SELECT y FROM v ORDER BY y;"), vec![2, 4, 6]);

    // A view defined before the macro is not supported: `CREATE VIEW` binds
    // its body immediately, so the macro must exist first. A table macro
    // inside a view binds and executes like any other FROM source.
    db.execute("DROP VIEW v;").unwrap();
    db.execute("CREATE MACRO everyone() AS TABLE SELECT * FROM emp;")
        .unwrap();
    db.execute("CREATE VIEW v2 AS SELECT COUNT(*) AS n FROM everyone();")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT n FROM v2;"), vec![3]);
}

#[test]
fn macro_in_view_cycles_fail_cleanly() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("CREATE MACRO f(x) AS x + 1;").unwrap();
    db.execute("CREATE VIEW v AS SELECT f(a) AS y FROM t;")
        .unwrap();
    // Define the cycle through OR REPLACE: `f` now reads `v`, which calls `f`.
    db.execute("CREATE OR REPLACE MACRO f(x) AS (SELECT COUNT(*) FROM v) + x;")
        .unwrap();
    let e = err(&mut db, "SELECT * FROM v;");
    assert!(
        e.contains("macro expansion depth limit") || e.contains("recursive"),
        "{e}"
    );
}

#[test]
fn materialized_views_refresh_through_macros() {
    let s = Scratch::new("mv");
    let mut db = PinkDb::open_durable_with(s.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    db.execute("CREATE MACRO bump(x) AS x + 100;").unwrap();
    db.execute("CREATE MATERIALIZED VIEW mv AS SELECT bump(a) AS v FROM t;")
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT v FROM mv ORDER BY v;"),
        vec![101, 102]
    );
    // An MV stores its *bound* query: a plain REFRESH in this session reuses
    // that plan, so the redefined macro is not observed until the persisted
    // definition rebinds (at load, below).
    db.execute("CREATE OR REPLACE MACRO bump(x) AS x + 200;")
        .unwrap();
    db.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT v FROM mv ORDER BY v;"),
        vec![101, 102]
    );
    // Save/reopen: the macro section loads before the MV section, so the MV
    //'s persisted definition rebinds successfully at load (against the
    // current macro), and the next refresh uses it.
    db.execute("CHECKPOINT;").unwrap();
    drop(db);
    let mut db = PinkDb::open(s.path()).unwrap();
    assert!(db.macro_in("main", "bump").is_ok());
    assert_eq!(
        ints(&mut db, "SELECT v FROM mv ORDER BY v;"),
        vec![101, 102]
    );
    db.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT v FROM mv ORDER BY v;"),
        vec![201, 202]
    );
}

#[test]
fn procedures_may_use_macros_but_not_define_them() {
    let mut db = seeded();
    db.execute("CREATE MACRO inc(x) AS x + 1;").unwrap();
    db.execute(
        "CREATE PROCEDURE p(v INTEGER) AS $$ BEGIN \
           INSERT INTO emp VALUES (inc(v), 'proc', 1.0); \
         END $$;",
    )
    .unwrap();
    db.execute("CALL p(3);").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT id FROM emp WHERE name = 'proc';"),
        vec![4]
    );
    // Macro DDL is store metadata and is refused inside a body.
    let e = err(
        &mut db,
        "CREATE PROCEDURE q() AS $$ BEGIN CREATE MACRO x(a) AS a; END $$;",
    );
    assert!(
        e.contains("CREATE MACRO") && e.contains("not allowed"),
        "{e}"
    );
    let e = err(
        &mut db,
        "CREATE PROCEDURE r() AS $$ BEGIN DROP MACRO inc; END $$;",
    );
    assert!(e.contains("DROP MACRO") && e.contains("not allowed"), "{e}");
}

// ---------- introspection ----------

#[test]
fn show_and_describe_macros() {
    let mut db = PinkDb::new();
    db.execute("CREATE MACRO add(a, b := 10) AS a + b;")
        .unwrap();
    db.execute("CREATE MACRO pick(x) AS TABLE SELECT x AS v;")
        .unwrap();
    let show = rows(&mut db, "SHOW MACROS;");
    assert_eq!(
        show,
        vec![
            vec![
                Value::Varchar("add".to_string()),
                Value::Varchar("scalar".to_string()),
                Value::Varchar("a, b".to_string()),
            ],
            vec![
                Value::Varchar("pick".to_string()),
                Value::Varchar("table".to_string()),
                Value::Varchar("x".to_string()),
            ],
        ]
    );
    let d = rows(&mut db, "DESCRIBE MACRO add;");
    assert_eq!(d.len(), 1);
    assert_eq!(d[0][0], Value::Varchar("add".to_string()));
    assert_eq!(d[0][1], Value::Varchar("macro".to_string()));
    assert_eq!(d[0][2], Value::Varchar("a, b".to_string()));
    assert_eq!(d[0][4], Value::Null);
    assert!(
        matches!(&d[0][5], Value::Varchar(s) if s.contains("CREATE MACRO add")),
        "{d:?}"
    );
    let e = err(&mut db, "DESCRIBE MACRO nope;");
    assert!(e.contains("not found"), "{e}");
}

// ---------- RBAC ----------

#[test]
fn macro_invocation_cannot_bypass_table_grants() {
    let database = Database::new();
    {
        let mut admin = database.connect();
        admin.execute("CREATE TABLE secret (v INTEGER);").unwrap();
        admin.execute("INSERT INTO secret VALUES (7);").unwrap();
        admin
            .execute("CREATE MACRO read_secret() AS (SELECT MAX(v) FROM secret);")
            .unwrap();
        admin
            .execute("CREATE MACRO secret_rows() AS TABLE SELECT * FROM secret;")
            .unwrap();
        admin
            .execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
    }
    let mut alice = database.connect();
    alice.login("alice", "alice-secret").unwrap();
    // Neither invocation form bypasses the base table's SELECT grant.
    let e = alice
        .execute("SELECT read_secret();")
        .expect_err("must be denied")
        .to_string();
    assert!(
        e.contains("permission denied") && e.contains("main.secret"),
        "{e}"
    );
    let e = alice
        .execute("SELECT * FROM secret_rows();")
        .expect_err("must be denied")
        .to_string();
    assert!(
        e.contains("permission denied") && e.contains("main.secret"),
        "{e}"
    );
    // A macro that touches no table needs no grant.
    let mut admin = database.connect();
    admin.execute("CREATE MACRO answer() AS 42;").unwrap();
    assert_eq!(
        match alice.execute("SELECT answer();").unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("{other:?}"),
        },
        vec![vec![Value::Integer(42)]]
    );
    // Granting SELECT on the table is what unlocks the macro (invoker rights;
    // there is no macro-level grant).
    admin
        .execute("GRANT SELECT ON TABLE secret TO alice;")
        .unwrap();
    assert_eq!(
        match alice
            .execute("SELECT read_secret();")
            .unwrap()
            .pop()
            .unwrap()
        {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("{other:?}"),
        },
        vec![vec![Value::Integer(7)]]
    );
}

#[test]
fn creating_and_dropping_macros_need_database_create() {
    let database = Database::new();
    {
        let mut admin = database.connect();
        admin.execute("CREATE TABLE t (a INTEGER);").unwrap();
        admin
            .execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
        admin.execute("GRANT SELECT ON TABLE t TO alice;").unwrap();
    }
    let mut alice = database.connect();
    alice.login("alice", "alice-secret").unwrap();
    let e = alice
        .execute("CREATE MACRO f(x) AS x + 1;")
        .expect_err("must be denied")
        .to_string();
    assert!(
        e.contains("permission denied") && e.contains("CREATE privilege on database \"main\""),
        "{e}"
    );
    let e = alice
        .execute("DROP MACRO nope;")
        .expect_err("must be denied")
        .to_string();
    assert!(e.contains("permission denied"), "{e}");
    // CREATE on the database is the documented macro-DDL privilege.
    let mut admin = database.connect();
    admin.execute("CREATE MACRO owned(x) AS x + 1;").unwrap();
    admin
        .execute("GRANT CREATE ON DATABASE main TO alice;")
        .unwrap();
    alice.execute("CREATE MACRO mine(x) AS x * 2;").unwrap();
    assert_eq!(
        match alice.execute("SELECT mine(4);").unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("{other:?}"),
        },
        vec![vec![Value::Integer(8)]]
    );
    alice.execute("DROP MACRO owned;").unwrap();
}

// ---------- persistence ----------

#[test]
fn persistence_round_trips_macros_and_sets_the_flag() {
    let s = Scratch::new("save");
    let mut db = seeded();
    db.execute("CREATE MACRO add(a, b := 10) AS a + b;")
        .unwrap();
    db.execute("CREATE MACRO high(s) AS TABLE SELECT * FROM emp WHERE salary > s;")
        .unwrap();
    db.execute("CREATE DATABASE other;").unwrap();
    db.execute("CREATE MACRO other.one() AS 1;").unwrap();
    db.save(s.path()).unwrap();

    let bytes = std::fs::read(s.path()).unwrap();
    let flags = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    assert_eq!(flags & 0x40, 0x40, "macros set flags bit 6: {flags:#x}");
    // A macro-free store keeps the bit clear.
    let plain = Scratch::new("plain");
    let mut empty = PinkDb::new();
    empty.execute("CREATE TABLE x (a INTEGER);").unwrap();
    empty.save(plain.path()).unwrap();
    let bytes = std::fs::read(plain.path()).unwrap();
    let flags = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    assert_eq!(flags & 0x40, 0, "no macros, no macro section: {flags:#x}");
    // Dropping the last macro clears the bit again (the section is recomputed
    // from the live catalog, never left behind).
    let dropped = Scratch::new("dropped");
    let mut db2 = PinkDb::new();
    db2.execute("CREATE MACRO gone() AS 1;").unwrap();
    db2.execute("DROP MACRO gone;").unwrap();
    db2.save(dropped.path()).unwrap();
    let bytes = std::fs::read(dropped.path()).unwrap();
    let flags = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    assert_eq!(flags & 0x40, 0, "no live macros: {flags:#x}");

    let mut loaded = PinkDb::open(s.path()).unwrap();
    assert_eq!(ints(&mut loaded, "SELECT add(1);"), vec![11]);
    assert_eq!(ints(&mut loaded, "SELECT add(1, 2);"), vec![3]);
    assert_eq!(
        rows(&mut loaded, "SELECT name FROM high(60) ORDER BY name;"),
        vec![
            vec![Value::Varchar("ann".to_string())],
            vec![Value::Varchar("cat".to_string())]
        ]
    );
    assert_eq!(loaded.macro_in("other", "one").unwrap().kind(), "scalar");
    assert_eq!(ints(&mut loaded, "USE other; SELECT one();"), vec![1]);
}

#[test]
fn newer_formats_and_unknown_flags_are_refused() {
    let s = Scratch::new("newer");
    let mut db = PinkDb::new();
    db.execute("CREATE MACRO f(x) AS x;").unwrap();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.save(s.path()).unwrap();
    let good = std::fs::read(s.path()).unwrap();

    let mut newer = good.clone();
    newer[..8].copy_from_slice(b"PINKDB06");
    std::fs::write(s.path(), &newer).unwrap();
    let e = PinkDb::open(s.path())
        .expect_err("newer format")
        .to_string();
    assert!(e.contains("newer pinkdb") && e.contains("format v6"), "{e}");

    // An as-yet-unknown flag bit (bit 7) is refused even with a valid CRC.
    let mut flagged = good;
    let flags = u32::from_le_bytes([flagged[8], flagged[9], flagged[10], flagged[11]]) | 0x80;
    flagged[8..12].copy_from_slice(&flags.to_le_bytes());
    let body_len = flagged.len() - 4;
    let crc = pinkdb::wal::crc32(&flagged[..body_len]);
    flagged[body_len..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(s.path(), &flagged).unwrap();
    let e = PinkDb::open(s.path())
        .expect_err("unknown flags")
        .to_string();
    assert!(e.contains("unsupported .pink v5 flags"), "{e}");
}

// ---------- WAL replay ----------

#[test]
fn wal_replay_reproduces_macro_ddl_and_its_uses() {
    let s = Scratch::new("wal");
    let mut db = PinkDb::open_durable_with(s.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("CREATE MACRO add(a, b) AS a + b;").unwrap();
    db.execute("CREATE MACRO src(x) AS TABLE SELECT a FROM t WHERE a > x;")
        .unwrap();
    db.execute("INSERT INTO t VALUES (add(1, 1)), (add(2, 2));")
        .unwrap();
    db.execute("INSERT INTO t SELECT add(a, 10) FROM src(0);")
        .unwrap();
    // A macro dropped before the next write: replay must observe the drop.
    db.execute("DROP MACRO add;").unwrap();
    db.execute("INSERT INTO t VALUES (99);").unwrap();
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![2, 4, 12, 14, 99]
    );
    drop(db);

    // WAL-only reopen (no checkpoint): every macro statement replays in order.
    let mut db = PinkDb::open_durable_with(s.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![2, 4, 12, 14, 99]
    );
    assert!(db.macro_in("main", "add").is_err());
    assert!(db.macro_in("main", "src").is_ok());
    // The surviving macro is live and usable after recovery.
    db.execute("INSERT INTO t SELECT a + 1 FROM src(10);")
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![2, 4, 12, 13, 14, 15, 99, 100]
    );

    // Checkpoint + tail: the checkpoint carries the macro section, the tail
    // replays on top.
    db.execute("CHECKPOINT;").unwrap();
    db.execute("CREATE MACRO plus10(x) AS x + 10;").unwrap();
    db.execute("INSERT INTO t VALUES (plus10(1));").unwrap();
    drop(db);
    let mut db = PinkDb::open_durable_with(s.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![2, 4, 11, 12, 13, 14, 15, 99, 100]
    );
    assert!(db.macro_in("main", "plus10").is_ok());
}

#[test]
fn macro_ddl_is_refused_inside_a_transaction() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("BEGIN;").unwrap();
    let e = err(&mut db, "CREATE MACRO f(x) AS x;");
    assert!(
        e.contains("CREATE MACRO") && e.contains("not allowed inside a transaction"),
        "{e}"
    );
    // Using a macro inside a transaction is ordinary SQL and is allowed.
    db.execute("CREATE MACRO g(x) AS x + 1;").ok();
    db.execute("ROLLBACK;").unwrap();
    db.execute("CREATE MACRO g(x) AS x + 1;").unwrap();
    db.execute("BEGIN;").unwrap();
    db.execute("INSERT INTO t VALUES (g(1));").unwrap();
    db.execute("COMMIT;").unwrap();
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![2]);
    db.execute("DROP MACRO g;").unwrap();
}

// ---------- name and catalog semantics ----------

#[test]
fn macros_live_in_their_own_namespace_and_are_per_database() {
    let mut db = seeded();
    // A macro may share a name with a table (DuckDB's separate namespace).
    db.execute("CREATE TABLE add (x INTEGER);").unwrap();
    db.execute("CREATE MACRO add(a, b) AS a + b;").unwrap();
    assert_eq!(ints(&mut db, "SELECT add(1, 2);"), vec![3]);
    assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM add;"), vec![0]);

    db.execute("CREATE DATABASE other; USE other;").unwrap();
    let e = err(&mut db, "SELECT add(1, 2);");
    assert!(e.contains("unknown function \"add\""), "{e}");
    db.execute("CREATE MACRO add(a, b) AS a * b;").unwrap();
    assert_eq!(ints(&mut db, "SELECT add(1, 2);"), vec![2]);
    db.execute("USE main;").unwrap();
    assert_eq!(ints(&mut db, "SELECT add(1, 2);"), vec![3]);

    // IF NOT EXISTS / OR REPLACE / DROP IF EXISTS.
    db.execute("CREATE MACRO add(a, b) AS a - b;").unwrap_err();
    db.execute("CREATE MACRO IF NOT EXISTS add(a, b) AS a - b;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT add(3, 2);"), vec![5]);
    db.execute("CREATE OR REPLACE MACRO add(a, b) AS a - b;")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT add(3, 2);"), vec![1]);
    db.execute("DROP MACRO IF EXISTS add;").unwrap();
    db.execute("DROP MACRO IF EXISTS add;").unwrap();
    let e = err(&mut db, "DROP MACRO add;");
    assert!(e.contains("not found"), "{e}");
}
