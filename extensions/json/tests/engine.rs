//! End-to-end tests for the core json extension through the engine (wave 52):
//! the linked registry → `LOAD EXTENSION 'json'` → binder → executor → WAL.
//!
//! Everything here goes through SQL — the same path a user takes — so the
//! tests pin the *observable* semantics (including the error wording the
//! engine produces for a failing callback) rather than the pure functions
//! (which `src/lib.rs`'s unit tests cover).

use pinkdb::{PinkDb, QueryResult, Value};

/// The registry is process-global and one-shot: register once for the whole
/// integration test binary, then `LOAD EXTENSION 'json'` per store.
fn registered() {
    pinkdb_json::ensure_registered().expect("json registers");
}

fn db_with_json() -> PinkDb {
    registered();
    let mut db = PinkDb::new();
    db.load_linked_extension("json").expect("json loads");
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

fn text(db: &mut PinkDb, sql: &str) -> Option<String> {
    match one(db, sql) {
        Value::Varchar(s) => Some(s),
        Value::Null => None,
        other => panic!("expected VARCHAR from {sql}, got {other:?}"),
    }
}

fn error(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err("statement should fail")
        .to_string()
}

// ---------- registration ----------

#[test]
fn load_extension_by_name_registers_the_six_functions() {
    let mut db = db_with_json();
    // All six are reachable and case-insensitive like every function name.
    assert_eq!(
        one(&mut db, "SELECT json_valid('{}');"),
        Value::Boolean(true)
    );
    assert_eq!(
        one(&mut db, "SELECT JSON_VALID('{');"),
        Value::Boolean(false)
    );
    assert_eq!(
        one(&mut db, "SELECT json_array_length('[1,2,3]');"),
        Value::Bigint(3)
    );
}

#[test]
fn the_sql_load_statement_resolves_the_registry_name() {
    let mut db = PinkDb::new();
    let err = error(&mut db, "SELECT json_valid('{}');");
    assert!(err.contains("unknown function"), "{err}");

    registered();
    let result = db.execute("LOAD EXTENSION 'json';").unwrap();
    assert!(
        matches!(&result[0], QueryResult::LoadedExtension { name } if name == "json"),
        "{result:?}"
    );
    assert_eq!(
        one(&mut db, "SELECT json_valid('{}');"),
        Value::Boolean(true)
    );

    // Loading twice is the documented duplicate-load error, not a silent
    // second registration.
    let err = error(&mut db, "LOAD EXTENSION 'json';");
    assert!(err.contains("already loaded"), "{err}");
    // The engine agrees the extension is loaded under its declared name.
    assert!(db.is_extension_loaded("json"));
    assert!(db.is_extension_loaded("JSON"));
    assert!(!db.is_extension_loaded("nope"));
}

/// A second `load_linked_extension` call on the same store is refused
/// atomically: the store keeps exactly the one registration.
#[test]
fn a_second_load_is_refused_and_changes_nothing() {
    let mut db = db_with_json();
    let err = db.load_linked_extension("json").unwrap_err().to_string();
    assert!(err.contains("is already loaded"), "{err}");
    assert_eq!(
        text(&mut db, "SELECT json_extract('{\"a\":1}', '$.a');").as_deref(),
        Some("1")
    );
}

// ---------- the six functions ----------

#[test]
fn extract_and_value_through_sql() {
    let mut db = db_with_json();
    assert_eq!(
        text(
            &mut db,
            r#"SELECT json_extract('{"a": {"b" : [1,  2] }}', '$.a');"#
        )
        .as_deref(),
        Some(r#"{"b":[1,2]}"#)
    );
    assert_eq!(
        text(
            &mut db,
            r#"SELECT json_extract('{"a":"he said \"hi\""}', '$.a');"#
        )
        .as_deref(),
        Some(r#""he said \"hi\"""#)
    );
    assert_eq!(
        text(&mut db, r#"SELECT json_extract('[10,20,30]', '$[1]');"#).as_deref(),
        Some("20")
    );
    assert_eq!(
        text(
            &mut db,
            r#"SELECT json_extract('{"a":[{"b":true}]}', '$.a[0].b');"#
        )
        .as_deref(),
        Some("true")
    );
    assert_eq!(
        text(&mut db, r#"SELECT json_extract('{"a":1}', '$."missing"');"#),
        None
    );
    assert_eq!(text(&mut db, r#"SELECT json_extract(NULL, '$.a');"#), None);
    assert_eq!(
        text(&mut db, r#"SELECT json_extract('{"a":1}', NULL);"#),
        None
    );

    // json_value: scalars as text, arrays/objects/null as SQL NULL.
    assert_eq!(
        text(&mut db, r#"SELECT json_value('{"a":1.50}', '$.a');"#).as_deref(),
        Some("1.5")
    );
    assert_eq!(
        text(&mut db, r#"SELECT json_value('{"a":{"b":1}}', '$.a');"#),
        None
    );
    assert_eq!(
        text(&mut db, r#"SELECT json_value('{"a":[1]}', '$.a');"#),
        None
    );
    assert_eq!(
        text(&mut db, r#"SELECT json_value('{"a":null}', '$.a');"#),
        None
    );

    // json_valid: boolean, NULL-propagating, and lenient like DuckDB.
    assert_eq!(
        one(&mut db, "SELECT json_valid('[1,]');"),
        Value::Boolean(true)
    );
    assert_eq!(
        one(&mut db, "SELECT json_valid('nan');"),
        Value::Boolean(true)
    );
    assert_eq!(
        one(&mut db, "SELECT json_valid('01');"),
        Value::Boolean(false)
    );
    assert_eq!(one(&mut db, "SELECT json_valid(NULL);"), Value::Null);
}

#[test]
fn array_length_and_builders_through_sql() {
    let mut db = db_with_json();
    assert_eq!(
        one(&mut db, "SELECT json_array_length('[]');"),
        Value::Bigint(0)
    );
    assert_eq!(
        one(
            &mut db,
            r#"SELECT json_array_length('{"a":[1,2]}', '$.a');"#
        ),
        Value::Bigint(2)
    );
    assert_eq!(
        one(&mut db, "SELECT json_array_length('{}');"),
        Value::Bigint(0)
    );
    assert_eq!(
        one(&mut db, r#"SELECT json_array_length('{"a":1}', '$.a');"#),
        Value::Bigint(0)
    );
    assert_eq!(
        one(&mut db, r#"SELECT json_array_length('[]', '$.missing');"#),
        Value::Null
    );
    assert_eq!(one(&mut db, "SELECT json_array_length(NULL);"), Value::Null);
    assert_eq!(
        one(&mut db, "SELECT json_array_length('[]', NULL);"),
        Value::Null
    );

    assert_eq!(
        text(&mut db, "SELECT json_object();").as_deref(),
        Some("{}")
    );
    assert_eq!(
        text(&mut db, "SELECT json_object('a', '1', 'b', 'x');").as_deref(),
        Some(r#"{"a":"1","b":"x"}"#)
    );
    assert_eq!(
        text(&mut db, "SELECT json_object('a', NULL);").as_deref(),
        Some(r#"{"a":null}"#)
    );
    assert_eq!(text(&mut db, "SELECT json_array();").as_deref(), Some("[]"));
    assert_eq!(
        text(&mut db, "SELECT json_array('x', NULL, 'y');").as_deref(),
        Some(r#"["x",null,"y"]"#)
    );
}

#[test]
fn invalidity_and_nulls_follow_the_documented_matrix() {
    let mut db = db_with_json();
    // Invalid JSON errors the value-producing functions…
    for sql in [
        "SELECT json_extract('{', '$');",
        "SELECT json_value('nope', '$');",
        "SELECT json_array_length('nope');",
        "SELECT json_extract('{\"a\":1}', '$.a.');",
        "SELECT json_extract('{\"a\":1}', '$.a[*]');",
    ] {
        let err = error(&mut db, sql);
        assert!(err.contains("extension function json_"), "{sql}: {err}");
    }
    // …with a message that names the function and the reason.
    let err = error(&mut db, "SELECT json_extract('{', '$');");
    assert!(err.contains("malformed JSON"), "{err}");
    let err = error(&mut db, "SELECT json_extract('{\"a\":1}', '$.a.');");
    assert!(err.contains("JSON path error"), "{err}");
    // NULL keys are refused, an odd argument count too.
    let err = error(&mut db, "SELECT json_object(CAST(NULL AS VARCHAR), 'v');");
    assert!(err.contains("JSON key cannot be NULL"), "{err}");
    let err = error(&mut db, "SELECT json_object('a');");
    assert!(err.contains("even number of arguments"), "{err}");
    // A bare NULL literal is accepted for any argument (ABI v1's typeless
    // NULL rule) and propagates.
    assert_eq!(text(&mut db, "SELECT json_extract(NULL, NULL);"), None);
}

/// ABI v1 is strictly typed: no implicit casts, so `json_array(1)` is a bind
/// error telling the user to CAST — the documented divergence from DuckDB.
#[test]
fn typed_arguments_are_refused_with_a_cast_hint() {
    let mut db = db_with_json();
    let err = error(&mut db, "SELECT json_array(1);");
    assert!(err.contains("must be VARCHAR"), "{err}");
    assert!(err.contains("use CAST"), "{err}");
    assert_eq!(
        text(&mut db, "SELECT json_array(CAST(1 AS VARCHAR));").as_deref(),
        Some(r#"["1"]"#)
    );
    // Arity errors name the registered range.
    let err = error(&mut db, "SELECT json_object('a');");
    assert!(err.contains("even number"), "{err}");
    let err = error(&mut db, "SELECT json_array_length();");
    assert!(err.contains("between 1 and 2"), "{err}");
    let err = error(&mut db, "SELECT json_extract('{}');");
    assert!(err.contains("takes 2 argument(s)"), "{err}");
}

// ---------- composition ----------

#[test]
fn functions_compose_over_columns_and_in_queries() {
    let mut db = db_with_json();
    db.execute(
        "CREATE TABLE docs (id INTEGER, doc VARCHAR); \
         INSERT INTO docs VALUES \
           (1, '{\"name\": \"ann\", \"tags\": [\"a\", \"b\"], \"age\": 30}'), \
           (2, '{\"name\": \"bob\", \"tags\": [], \"age\": null}'), \
           (3, '{\"name\": \"cat\", \"tags\": [\"c\"], \"age\": 40}');",
    )
    .unwrap();

    // WHERE with a JSON predicate.
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM docs WHERE json_valid(doc) ORDER BY id;"
        )
        .len(),
        3
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM docs WHERE json_array_length(doc, '$.tags') > 1;"
        ),
        vec![vec![Value::Integer(1)]]
    );
    // Projection and GROUP BY over extracted values.
    assert_eq!(
        rows(
            &mut db,
            "SELECT json_value(doc, '$.name'), json_array_length(doc, '$.tags') FROM docs ORDER BY id;"
        ),
        vec![
            vec![Value::Varchar("\"ann\"".to_string()), Value::Bigint(2)],
            vec![Value::Varchar("\"bob\"".to_string()), Value::Bigint(0)],
            vec![Value::Varchar("\"cat\"".to_string()), Value::Bigint(1)],
        ]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT json_array_length(doc, '$.tags') AS n, COUNT(*) FROM docs GROUP BY n ORDER BY n;"
        ),
        vec![
            vec![Value::Bigint(0), Value::Bigint(1)],
            vec![Value::Bigint(1), Value::Bigint(1)],
            vec![Value::Bigint(2), Value::Bigint(1)],
        ]
    );
    // Builders compose with extraction in both directions (as strings —
    // v1's documented builder limitation).
    assert_eq!(
        text(
            &mut db,
            "SELECT json_array(json_extract('{\"a\":[1,2]}', '$.a'));"
        )
        .as_deref(),
        Some(r#"["[1,2]"]"#)
    );
    // `json_value` keeps a JSON string quoted (DuckDB's VARCHAR-returning
    // `json_value` does the same), so the builder embeds the quoted text as a
    // JSON string — the documented v1 composition limit.
    assert_eq!(
        text(
            &mut db,
            "SELECT json_object('n', json_value(doc, '$.name')) FROM docs WHERE id = 1;"
        )
        .as_deref(),
        Some(r#"{"n":"\"ann\""}"#)
    );
    assert_eq!(
        text(&mut db, "SELECT json_object('n', 'ann');").as_deref(),
        Some(r#"{"n":"ann"}"#)
    );
}

// ---------- prepared statements ----------

#[test]
fn prepared_statements_bind_and_return_json_text() {
    let mut db = db_with_json();
    let stmt = db.prepare("SELECT json_extract(?, ?);").unwrap();
    assert_eq!(stmt.parameter_count(), 2);
    let result = db
        .execute_prepared(
            &stmt,
            &[
                Value::Varchar(r#"{"a": [1, 2]}"#.to_string()),
                Value::Varchar("$.a[1]".to_string()),
            ],
        )
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Varchar("2".to_string())]])
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // The variadic builders bind a varying number of parameters per call.
    let stmt = db
        .prepare("SELECT json_array(?, CAST(? AS VARCHAR), ?);")
        .unwrap();
    let result = db
        .execute_prepared(
            &stmt,
            &[
                Value::Varchar("x".to_string()),
                Value::Null,
                Value::Varchar("y".to_string()),
            ],
        )
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Varchar(r#"["x",null,"y"]"#.to_string())]]
            )
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

// ---------- durability ----------

/// A durable record that calls a json function is prefixed with
/// `LOAD EXTENSION 'json'`; reopening replays it through the registry (the
/// extension is registered in this process — the wasm/native linked model)
/// and the rows come back.
#[test]
fn durable_records_touching_json_replay() {
    registered();
    let dir = std::env::temp_dir().join(format!("pinkdb_json_wal_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();

    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.load_linked_extension("json").unwrap();
        db.execute("CREATE TABLE t (id BIGINT, doc VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, '{\"a\": [1, 2]}'), (2, '{\"a\": []}');")
            .unwrap();
        // A record that calls the extension (needs the WAL prefix).
        db.execute("UPDATE t SET id = id + json_array_length(doc, '$.a');")
            .unwrap();
        db.checkpoint().unwrap();
        // After the checkpoint the next touching record is prefixed again.
        db.execute("INSERT INTO t VALUES (json_array_length('[1,2,3]'), '{\"a\": []}');")
            .unwrap();
        db.flush_wal().unwrap();
    }

    {
        // Reopen with the extension only published (not loaded in the store):
        // replay resolves `LOAD EXTENSION 'json'` through the registry.
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        assert_eq!(
            rows(&mut db, "SELECT id FROM t ORDER BY id;"),
            vec![
                vec![Value::Bigint(2)],
                vec![Value::Bigint(3)],
                vec![Value::Bigint(3)],
            ]
        );
        assert_eq!(
            text(
                &mut db,
                "SELECT json_extract(doc, '$.a') FROM t \
                 WHERE id = 3 AND json_array_length(doc, '$.a') = 2;"
            )
            .as_deref(),
            Some("[1,2]")
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
