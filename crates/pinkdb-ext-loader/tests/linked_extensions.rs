//! End-to-end tests for the **linked** extension model (wave 49): an
//! extension is published in the process-global registry
//! (`pinkdb_ext_loader::linked::register`) and loaded by name, then driven
//! through SQL — binder, optimizer, executor, WAL.
//!
//! The linked model is the wasm target's only loading model; native targets
//! share every line of the path (registration, registry, binder, executor,
//! WAL prefixing), so this file exercises it natively. The C ABI's `dlopen`
//! path stays covered by `extensions.rs`.
//!
//! The registry is process-global, so every test publishes its demo extension
//! under its **own** name (`wave49_<tag>`); function names may repeat because
//! each store registers at most one extension at a time.

use pinkdb::{PinkDb, QueryResult, Value};
use pinkdb_ext_loader::linked::{self, LinkedExtension, LinkedFunction};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

// ---------- helpers ----------

/// The demo extension's functions: one per ABI v1 value kind plus the failure
/// paths, so a single registration covers every branch the executor takes.
fn demo(name: &str) -> LinkedExtension {
    LinkedExtension::new(
        name,
        vec![
            LinkedFunction::new(
                "lnk_zero",
                vec![],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Integer(7)),
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_double_it",
                vec![ExtType::Integer],
                ExtType::Integer,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Integer(i) => Ok(ExtValue::Integer(i.wrapping_mul(2))),
                    ExtArg::Null(_) => Ok(ExtValue::Null),
                    ref other => Err(format!("unexpected argument {other:?}")),
                },
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_shout",
                vec![ExtType::Varchar],
                ExtType::Varchar,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Varchar(bytes) => Ok(ExtValue::Varchar(
                        bytes.iter().map(u8::to_ascii_uppercase).collect(),
                    )),
                    ExtArg::Null(_) => Ok(ExtValue::Null),
                    ref other => Err(format!("unexpected argument {other:?}")),
                },
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_concat2",
                vec![ExtType::Varchar, ExtType::Varchar],
                ExtType::Varchar,
                |args: &[ExtArg<'_>]| match (args[0], args[1]) {
                    (ExtArg::Varchar(a), ExtArg::Varchar(b)) => {
                        Ok(ExtValue::Varchar([a, b].concat()))
                    }
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_halve",
                vec![ExtType::Double],
                ExtType::Double,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Double(x) => Ok(ExtValue::Double(x / 2.0)),
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_is_even",
                vec![ExtType::Integer],
                ExtType::Boolean,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Integer(i) => Ok(ExtValue::Boolean(i % 2 == 0)),
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_plus_day",
                vec![ExtType::Date],
                ExtType::Date,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Date(d) => Ok(ExtValue::Date(d + 1)),
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_micros",
                vec![ExtType::Timestamp],
                ExtType::Bigint,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Timestamp(us) => Ok(ExtValue::Bigint(us)),
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
            // Failure paths: an error, a panic, and a contract violation.
            LinkedFunction::new(
                "lnk_fail",
                vec![ExtType::Integer],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Err("bad input".to_string()),
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_panic",
                vec![ExtType::Integer],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| panic!("wave 49 test panic"),
            )
            .unwrap(),
            LinkedFunction::new(
                "lnk_wrong",
                vec![ExtType::Integer],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Varchar(b"nope".to_vec())),
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

/// Publish the demo extension under `wave49_<tag>` and load it into a fresh
/// in-memory store, the way a wasm embedder would (register at bootstrap,
/// load by name).
fn store(tag: &str) -> PinkDb {
    let name = format!("wave49_{tag}");
    linked::register(demo(&name)).unwrap();
    let mut db = PinkDb::new();
    assert_eq!(db.load_linked_extension(&name).unwrap(), name);
    db
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Error message of a failing `sql`.
fn error(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(results) => panic!("expected an error for {sql:?}, got {results:?}"),
        Err(e) => e.to_string(),
    }
}

/// A fresh scratch directory for one durable WAL test.
fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("linked_ext_wal_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------- loading and calling ----------

#[test]
fn registered_extension_runs_through_sql() {
    let mut db = store("runs");

    assert_eq!(
        rows(&mut db, "SELECT lnk_zero();"),
        vec![vec![Value::Integer(7)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(21);"),
        vec![vec![Value::Integer(42)]]
    );
    // Case-insensitive, like built-in functions and C ABI functions.
    assert_eq!(
        rows(&mut db, "SELECT LNK_Double_It(21);"),
        vec![vec![Value::Integer(42)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_shout('hey');"),
        vec![vec![Value::Varchar("HEY".to_string())]]
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_concat2('sun', 'day');"),
        vec![vec![Value::Varchar("sunday".to_string())]]
    );

    // Every value tag crosses the boundary: DOUBLE, BOOLEAN, DATE, TIMESTAMP.
    assert_eq!(
        rows(&mut db, "SELECT lnk_halve(5.0);"),
        vec![vec![Value::Double(2.5)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_is_even(4);"),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT lnk_plus_day(DATE '2024-01-31') = DATE '2024-02-01';"
        ),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT lnk_micros(TIMESTAMP '2024-01-01 00:00:00.5') = 1704067200500000;"
        ),
        vec![vec![Value::Boolean(true)]]
    );

    // The static path is untouched and composes with the dynamic one.
    assert_eq!(
        rows(&mut db, "SELECT upper(lnk_shout('hey'));"),
        rows(&mut db, "SELECT 'HEY';")
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(21) + 1;"),
        vec![vec![Value::Integer(43)]]
    );
}

#[test]
fn columns_nulls_and_aggregates_pass_through() {
    let mut db = store("columns");
    db.execute(
        "CREATE TABLE t (a INTEGER, s VARCHAR); \
         INSERT INTO t VALUES (1, 'x'), (NULL, NULL), (3, 'z');",
    )
    .unwrap();

    // NULL ordering: ORDER BY a puts NULL last.
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(a) FROM t ORDER BY a;"),
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(6)],
            vec![Value::Null],
        ]
    );
    // A NULL cell arrives as ExtArg::Null and the callback chooses NULL; a
    // bare NULL literal works for any registered argument type.
    assert_eq!(
        rows(&mut db, "SELECT lnk_shout(s) FROM t ORDER BY a;"),
        vec![
            vec![Value::Varchar("X".to_string())],
            vec![Value::Varchar("Z".to_string())],
            vec![Value::Null],
        ]
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(NULL);"),
        vec![vec![Value::Null]]
    );

    // It participates in WHERE, GROUP BY and aggregation like any expression.
    assert_eq!(
        rows(
            &mut db,
            "SELECT COUNT(*) FROM t WHERE lnk_double_it(a) > 4;"
        ),
        vec![vec![Value::Bigint(1)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT SUM(lnk_double_it(a)) FROM t;"),
        vec![vec![Value::Bigint(8)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_is_even(a) FROM t ORDER BY a;"),
        vec![
            vec![Value::Boolean(false)],
            vec![Value::Boolean(false)],
            vec![Value::Null],
        ]
    );
}

// ---------- the SQL statement ----------

#[test]
fn load_extension_sql_resolves_registered_names() {
    let name = "wave49_sql_stmt";
    linked::register(demo(name)).unwrap();
    let mut db = PinkDb::new();
    // The statement is off by default (it loads and runs native code).
    db.set_allow_sql_load_extension(true);

    let sql = format!("LOAD EXTENSION '{name}';");
    match db.execute(&sql).unwrap().pop().unwrap() {
        QueryResult::LoadedExtension { name: loaded } => assert_eq!(loaded, name),
        other => panic!("expected LoadedExtension, got {other:?}"),
    }
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(21);"),
        vec![vec![Value::Integer(42)]]
    );

    // Names are matched case-insensitively, and a second load is refused
    // exactly like a native one.
    let name = "WAVE49_SQL_STMT";
    let e = error(&mut db, &format!("LOAD EXTENSION '{name}';"));
    assert!(e.contains("is already loaded"), "{e}");

    // A string that is neither a loadable library nor a registered name keeps
    // the load error — the registry fallback never hides it. A bare name (no
    // directory) is refused outright: the dynamic loader would resolve it
    // through its own search path.
    let e = error(&mut db, "LOAD EXTENSION 'wave49_absent_extension.dylib';");
    assert!(e.contains("is not a loadable path"), "{e}");
    assert!(
        e.contains("wave49_absent_extension.dylib"),
        "the path must be named: {e}"
    );

    // Unknown functions still report unknown (the registry is per store).
    let mut fresh = PinkDb::new();
    let e = error(&mut fresh, "SELECT lnk_double_it(1);");
    assert_eq!(e, "unknown function \"lnk_double_it\"");
}

#[test]
fn unregistered_names_fail_with_the_registry_error() {
    let mut db = PinkDb::new();
    let e = db
        .load_linked_extension("wave49_never_registered")
        .unwrap_err()
        .to_string();
    assert!(e.contains("is not registered"), "{e}");
    assert!(e.contains("linked::register"), "{e}");
    // The failed load left the store usable and nothing registered (the
    // function stays unknown).
    assert_eq!(rows(&mut db, "SELECT 1;"), vec![vec![Value::Integer(1)]]);
}

// ---------- failures ----------

#[test]
fn failure_paths_are_clean_errors() {
    let mut db = store("failures");

    // A callback Err.
    let e = error(&mut db, "SELECT lnk_fail(1);");
    assert_eq!(e, "extension function lnk_fail() failed: bad input");

    // A panic: caught at the boundary (the linked model's stand-in for the C
    // ABI's "must not unwind" rule). The default panic hook still prints the
    // panic itself — that line in the test output is expected.
    let e = error(&mut db, "SELECT lnk_panic(1);");
    assert_eq!(
        e,
        "extension function lnk_panic() panicked: wave 49 test panic"
    );

    // A result that violates the declared type.
    let e = error(&mut db, "SELECT lnk_wrong(1);");
    assert_eq!(
        e,
        "extension function lnk_wrong() returned an invalid result: \
         returned VARCHAR, registered as INTEGER"
    );

    // Arity and argument types are checked at bind time, like the C ABI.
    let e = error(&mut db, "SELECT lnk_double_it(1, 2);");
    assert!(e.contains("lnk_double_it takes 1 argument(s)"), "{e}");
    let e = error(&mut db, "SELECT lnk_zero(1);");
    assert!(e.contains("lnk_zero takes 0 argument(s)"), "{e}");
    let e = error(&mut db, "SELECT lnk_double_it('x');");
    assert!(
        e.contains("lnk_double_it argument 1 must be INTEGER, got VARCHAR"),
        "{e}"
    );
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(CAST('21' AS INTEGER));"),
        vec![vec![Value::Integer(42)]]
    );

    // A failing row aborts the statement; a later statement is unaffected.
    db.execute("CREATE TABLE u (x INTEGER); INSERT INTO u VALUES (1), (2);")
        .unwrap();
    let e = error(&mut db, "SELECT lnk_fail(x) FROM u;");
    assert!(e.contains("failed: bad input"), "{e}");
    assert_eq!(
        rows(&mut db, "SELECT lnk_double_it(2);"),
        vec![vec![Value::Integer(4)]]
    );
}

// ---------- registration rules ----------

#[test]
fn registration_conflicts_are_atomic() {
    // A function that collides with a built-in fails the whole extension:
    // nothing half-registers.
    let name = "wave49_conflict_builtin";
    let ext = LinkedExtension::new(
        name,
        vec![
            LinkedFunction::new(
                "lnk_innocent",
                vec![],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Integer(1)),
            )
            .unwrap(),
            LinkedFunction::new(
                "upper",
                vec![],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Integer(1)),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    linked::register(ext).unwrap();
    let mut db = PinkDb::new();
    let e = db.load_linked_extension(name).unwrap_err().to_string();
    assert!(
        e.contains("function \"upper\" conflicts with a built-in function"),
        "{e}"
    );
    // Atomicity: the innocent sibling is not callable either.
    let e = error(&mut db, "SELECT lnk_innocent();");
    assert_eq!(e, "unknown function \"lnk_innocent\"");

    // Two extensions claiming the same function name.
    linked::register(demo("wave49_a")).unwrap();
    linked::register(demo("wave49_b")).unwrap();
    let mut db = PinkDb::new();
    db.load_linked_extension("wave49_a").unwrap();
    let e = db
        .load_linked_extension("wave49_b")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("function \"lnk_zero\" is already registered"),
        "{e}"
    );

    // The same extension name twice in the registry is a host bootstrap bug.
    let e = linked::register(demo("wave49_a")).unwrap_err().to_string();
    assert!(e.contains("is already registered"), "{e}");

    // Metadata mistakes are rejected before a handle exists at all.
    let e = LinkedExtension::new(
        "wave49_dupes",
        vec![
            LinkedFunction::new(
                "same",
                vec![],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Null),
            )
            .unwrap(),
            LinkedFunction::new(
                "SAME",
                vec![],
                ExtType::Integer,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Null),
            )
            .unwrap(),
        ],
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("is declared twice"), "{e}");
}

// ---------- durability ----------

/// A durable record that calls a linked function is prefixed with
/// `LOAD EXTENSION '<name>'` at append time; reopening replays that prefix
/// through the registry (no file is ever opened) and the rows come back.
#[test]
fn durable_records_touching_linked_functions_replay() {
    let name = "wave49_wal";
    linked::register(demo(name)).unwrap();
    let dir = scratch("replay");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();

    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        // The statement is off by default (it loads and runs native code).
        db.set_allow_sql_load_extension(true);
        let sql = format!(
            "LOAD EXTENSION '{name}'; CREATE TABLE t (a INTEGER); \
             INSERT INTO t VALUES (1), (2), (3);"
        );
        db.execute(&sql).unwrap();
        // Rows computed by the linked function (v1 INSERT takes VALUES only),
        // so the records that follow call it and need the prefix.
        let doubled: Vec<String> = match db
            .execute("SELECT lnk_double_it(a) FROM t;")
            .unwrap()
            .pop()
            .unwrap()
        {
            QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match r[0] {
                    Value::Integer(i) => format!("({i})"),
                    ref other => panic!("expected INTEGER, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        };
        db.execute(&format!("INSERT INTO t VALUES {};", doubled.join(", ")))
            .unwrap();
        // A checkpoint rolls the LOAD record away; the next record touching
        // the extension must be prefixed again.
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (lnk_double_it(10));")
            .unwrap();
        db.flush_wal().unwrap();
    }

    // Reopen in the same process (the registry is process-global, the wasm
    // analogue of the library file still being on disk): replay resolves the
    // name, re-runs the records, and every row is present exactly once.
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        assert_eq!(
            rows(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(2)],
                vec![Value::Integer(3)],
                vec![Value::Integer(4)],
                vec![Value::Integer(6)],
                vec![Value::Integer(20)],
            ]
        );
        assert_eq!(
            rows(&mut db, "SELECT SUM(lnk_double_it(a)) FROM t;"),
            vec![vec![Value::Bigint(76)]]
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------- registry surface ----------

#[test]
fn registry_introspection_is_case_insensitive_and_ordered() {
    // Other tests share the process-global registry and run in parallel, so
    // nothing here asserts on the *whole* list; the relative order of these
    // two registrations is stable (each register is atomic under the
    // registry lock).
    linked::register(demo("wave49_first")).unwrap();
    linked::register(demo("wave49_second")).unwrap();

    let names = linked::names();
    let first = names.iter().position(|n| n == "wave49_first").unwrap();
    let second = names.iter().position(|n| n == "wave49_second").unwrap();
    // Registration order, not hash order.
    assert!(first < second);

    assert!(linked::is_registered("WAVE49_FIRST"));
    assert_eq!(
        linked::get("Wave49_Second").map(|e| e.name().to_string()),
        Some("wave49_second".to_string())
    );
    let ext = linked::get("wave49_second").unwrap();
    let f = &ext.functions()[1];
    assert_eq!(f.name(), "lnk_double_it");
    assert_eq!(f.n_args(), 1);
    assert_eq!(f.arg_types(), &[ExtType::Integer]);
    assert_eq!(f.ret_type(), ExtType::Integer);
    assert!(linked::get("wave49_absent").is_none());
}
