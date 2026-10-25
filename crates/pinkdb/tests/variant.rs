//! `VARIANT` (wave 101a) end to end: the type in every stored shape, the
//! casts in both directions, the JSON text round trip, `typeof` /
//! `variant_type` / `variant_keys` / `variant_extract`, equality/ordering/
//! grouping, and `.pink` + WAL persistence.
//!
//! The last test pins pinkdb's answers next to DuckDB 2.0.0's, measured with
//! `/tmp/duckdb-2.0.0/duckdb -no-agent`; `docs/wave-101-plan.md` lists where
//! the two deliberately differ.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{DataType, PinkDb, QueryResult, Value, variant::Variant};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_variant_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

/// The single text cell of `sql`.
fn text(db: &mut PinkDb, sql: &str) -> Option<String> {
    match &rows(db, sql)[0][0] {
        Value::Null => None,
        Value::Varchar(s) => Some(s.clone()),
        other => panic!("expected text from {sql}, got {other:?}"),
    }
}

fn variant(json: &str) -> Value {
    Value::Variant(Arc::new(
        pinkdb::variant::from_json_str(json).unwrap_or_else(|e| panic!("{json}: {e}")),
    ))
}

// ------------------------------------------------------------------- casts

#[test]
fn every_stored_type_casts_into_variant() {
    let mut db = PinkDb::new();
    let got = rows(
        &mut db,
        "SELECT typeof(CAST(1 AS VARIANT)),
                variant_type(CAST(true AS VARIANT)),
                variant_type(CAST(1 AS VARIANT)),
                variant_type(CAST(1.5 AS VARIANT)),
                variant_type(CAST(CAST(1.25 AS DECIMAL(10,2)) AS VARIANT)),
                variant_type(CAST('\"s\"' AS VARIANT)),
                variant_type(CAST(DATE '2020-01-02' AS VARIANT)),
                variant_type(CAST(TIMESTAMP '2020-01-02 03:04:05' AS VARIANT)),
                variant_type(CAST(INTERVAL '1 day' AS VARIANT)),
                variant_type(CAST('[1,2]' AS VARIANT)),
                variant_type(CAST('{\"a\":1}' AS VARIANT)),
                variant_type(CAST(NULL AS VARIANT)) IS NULL;",
    );
    let text = |t: &str| Value::Varchar(t.to_string());
    assert_eq!(
        got,
        vec![vec![
            text("VARIANT"),
            text("BOOL"),
            text("BIGINT"),
            text("DOUBLE"),
            text("DECIMAL"),
            text("VARCHAR"),
            text("DATE"),
            text("TIMESTAMP"),
            // VARIANT has no interval kind: it keeps the interval's text.
            text("VARCHAR"),
            text("ARRAY"),
            text("OBJECT"),
            Value::Boolean(true),
        ]]
    );
}

#[test]
fn variant_casts_back_to_every_scalar() {
    let mut db = PinkDb::new();
    let got = rows(
        &mut db,
        "SELECT CAST(CAST('true' AS VARIANT) AS BOOLEAN),
                CAST(CAST('42' AS VARIANT) AS INTEGER),
                CAST(CAST('42' AS VARIANT) AS BIGINT),
                CAST(CAST('1.5' AS VARIANT) AS DOUBLE),
                CAST(CAST('1.25' AS VARIANT) AS DECIMAL(10,2)),
                CAST(CAST('\"hi\"' AS VARIANT) AS VARCHAR),
                CAST(CAST('\"2020-01-02\"' AS VARIANT) AS DATE),
                CAST(CAST('\"2020-01-02 03:04:05\"' AS VARIANT) AS TIMESTAMP);",
    );
    assert_eq!(
        got,
        vec![vec![
            Value::Boolean(true),
            Value::Integer(42),
            Value::Bigint(42),
            Value::Double(1.5),
            Value::Decimal(125, 2),
            // VARIANT -> VARCHAR is the JSON text (quoted), unlike DuckDB's
            // scalar text extraction; the JSON round trip needs the quotes.
            Value::Varchar("\"hi\"".into()),
            Value::Date(18263),
            Value::Timestamp(1577934245000000),
        ]]
    );

    // A variant string converts like the VARCHAR it holds (DuckDB).
    let nested = rows(
        &mut db,
        "SELECT CAST(CAST('\"5\"' AS VARIANT) AS INTEGER),
                CAST(CAST('\"1e3\"' AS VARIANT) AS DOUBLE),
                CAST(CAST('\"false\"' AS VARIANT) AS BOOLEAN),
                CAST(CAST('\"123\"' AS VARIANT) AS DECIMAL(10,2)),
                CAST(CAST('1.9' AS VARIANT) AS INTEGER),
                CAST(CAST('-1.9' AS VARIANT) AS INTEGER);",
    );
    assert_eq!(
        nested,
        vec![vec![
            Value::Integer(5),
            Value::Double(1000.0),
            Value::Boolean(false),
            Value::Decimal(12300, 2),
            // pinkdb rounds a DOUBLE like DuckDB does.
            Value::Integer(2),
            Value::Integer(-2),
        ]]
    );

    // A variant-null is SQL NULL for every other type, like DuckDB.
    let nulls = rows(
        &mut db,
        "SELECT CAST(CAST('null' AS VARIANT) AS BOOLEAN) IS NULL,
                CAST(CAST('null' AS VARIANT) AS INTEGER) IS NULL,
                CAST(CAST('null' AS VARIANT) AS DOUBLE) IS NULL,
                CAST(CAST('null' AS VARIANT) AS DECIMAL(10,2)) IS NULL,
                CAST(CAST('null' AS VARIANT) AS VARCHAR) = 'null',
                CAST(CAST('null' AS VARIANT) AS DATE) IS NULL,
                CAST(CAST('null' AS VARIANT) AS TIMESTAMP) IS NULL;",
    );
    assert_eq!(nulls, vec![vec![Value::Boolean(true); 7]]);
}

#[test]
fn variant_cast_errors_name_the_kind_and_the_value() {
    let mut db = PinkDb::new();
    assert_eq!(
        err(
            &mut db,
            "SELECT CAST(CAST('{\"a\":1}' AS VARIANT) AS INTEGER);"
        ),
        "cannot CAST VARIANT(OBJECT) value {\"a\":1} to INTEGER"
    );
    assert_eq!(
        err(
            &mut db,
            "SELECT CAST(CAST('\"hi\"' AS VARIANT) AS INTEGER);"
        ),
        "cannot CAST VARIANT(VARCHAR) value \"hi\" to INTEGER"
    );
    assert_eq!(
        err(
            &mut db,
            "SELECT CAST(CAST('9223372036854775808' AS VARIANT) AS BIGINT);"
        ),
        "cannot CAST VARIANT(DECIMAL) value 9223372036854775808 (out of range) to BIGINT"
    );
    assert_eq!(
        err(&mut db, "SELECT CAST(CAST('1e300' AS VARIANT) AS INTEGER);"),
        "cannot CAST VARIANT(DOUBLE) value 1e300 (out of range) to INTEGER"
    );
    assert_eq!(
        err(&mut db, "SELECT CAST(CAST('1' AS VARIANT) AS DATE);"),
        "cannot CAST VARIANT(BIGINT) value 1 to DATE"
    );
    assert_eq!(
        err(&mut db, "SELECT CAST(CAST('true' AS VARIANT) AS DOUBLE);"),
        "cannot CAST VARIANT(BOOL) value true to DOUBLE"
    );
    // A non-JSON VARCHAR is not a VARIANT.
    assert_eq!(
        err(&mut db, "SELECT CAST('nope' AS VARIANT);"),
        "cannot CAST VARCHAR to VARIANT: invalid JSON at byte 0 (unknown literal): \"nope\""
    );
}

// -------------------------------------------------------------- JSON text

#[test]
fn json_text_round_trips_through_variant() {
    let mut db = PinkDb::new();
    // Row 1: canonical text; row 2: the same JSON spelled with padding and
    // unsorted keys. Both must reach the byte-identical VARIANT.
    let cases = [
        ("{\"b\":1,\"a\":2}", "{\"a\":2,\"b\":1}"),
        (
            "{ \"a\" : [ 1 , 2 , { \"c\" : null } ] }",
            "{\"a\":[1,2,{\"c\":null}]}",
        ),
        ("\"héllo 世界 🚀\"", "\"héllo 世界 🚀\""),
        ("9223372036854775807", "9223372036854775807"),
        ("-9223372036854775808", "-9223372036854775808"),
        ("1.0", "1.0"),
        ("-1234567890123.456789", "-1234567890123.456789"),
    ];
    for (input, want) in &cases {
        let got = text(
            &mut db,
            &format!("SELECT CAST(CAST('{input}' AS VARIANT) AS VARCHAR);"),
        )
        .unwrap_or_else(|| panic!("{input} did not round-trip"));
        assert_eq!(got, *want, "input {input}");
        // And the text parses back to a VARIANT equal to the original.
        let same = rows(
            &mut db,
            &format!("SELECT CAST('{input}' AS VARIANT) = CAST('{want}' AS VARIANT);"),
        );
        assert_eq!(same, vec![vec![Value::Boolean(true)]], "input {input}");
    }
    // 1 and 1.0 stay different kinds but compare equal; the text keeps both.
    let kinds = rows(
        &mut db,
        "SELECT variant_type(CAST('1' AS VARIANT)), variant_type(CAST('1.0' AS VARIANT)),
                CAST('1' AS VARIANT) = CAST('1.0' AS VARIANT);",
    );
    assert_eq!(
        kinds,
        vec![vec![
            Value::Varchar("BIGINT".into()),
            // Fractional JSON numbers stay exact as DECIMAL (see the plan).
            Value::Varchar("DECIMAL".into()),
            Value::Boolean(true)
        ]]
    );
    // NaN/infinity have no JSON form but survive the text round trip.
    let non_finite = rows(
        &mut db,
        "SELECT CAST(CAST('NaN' AS VARIANT) AS VARCHAR),
                CAST(CAST('Infinity' AS VARIANT) AS VARCHAR),
                CAST(CAST('-Infinity' AS VARIANT) AS VARCHAR);",
    );
    assert_eq!(
        non_finite,
        vec![vec![
            Value::Varchar("NaN".into()),
            Value::Varchar("Infinity".into()),
            Value::Varchar("-Infinity".into())
        ]]
    );
    // A duplicate key keeps the last value (a JSON reader's rule).
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(CAST('{\"a\":1,\"a\":2}' AS VARIANT) AS VARCHAR);"
        ),
        Some("{\"a\":2}".to_string())
    );
}

// --------------------------------------------------------------- functions

#[test]
fn variant_functions_read_the_shape() {
    let mut db = PinkDb::new();
    let obj = "CAST('{\"b\":1,\"a\":{\"z\":[10,20]}}' AS VARIANT)";
    let got = rows(
        &mut db,
        &format!(
            "SELECT variant_type({obj}), variant_keys({obj}),
                    variant_keys(variant_extract({obj}, 'a')),
                    variant_type(CAST('[1,2]' AS VARIANT)),
                    variant_keys(CAST('[1,2]' AS VARIANT)),
                    variant_keys(CAST(1 AS VARIANT)),
                    variant_type(CAST(NULL AS VARIANT)) IS NULL,
                    variant_keys(CAST(NULL AS VARIANT)) IS NULL,
                    variant_type(CAST('null' AS VARIANT)) IS NULL,
                    variant_keys(CAST('null' AS VARIANT)) IS NULL;"
        ),
    );
    let t = |s: &str| Value::Varchar(s.to_string());
    assert_eq!(
        got,
        vec![vec![
            t("OBJECT"),
            t("[\"a\",\"b\"]"),
            t("[\"z\"]"),
            t("ARRAY"),
            t("[]"),
            t("[]"),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
}

#[test]
fn variant_extract_walks_a_zero_based_jsonpath() {
    let mut db = PinkDb::new();
    let v = "CAST('{\"a\":{\"b\":[1,2.5,\"s\",null,true]},\"k\":{\"x\":1},\"n\":null}' AS VARIANT)";
    let got = rows(
        &mut db,
        &format!(
            "SELECT CAST(variant_extract({v}, 'a.b[0]') AS INTEGER),
                    CAST(variant_extract({v}, 'a.b[1]') AS DOUBLE),
                    CAST(variant_extract({v}, '$.a.b[2]') AS VARCHAR),
                    variant_type(variant_extract({v}, 'a.b[3]')) IS NULL,
                    CAST(variant_extract({v}, 'a.b[4]') AS BOOLEAN),
                    CAST(variant_extract({v}, 'k') AS VARCHAR),
                    variant_extract({v}, 'a.b[9]') IS NULL,
                    variant_extract({v}, 'missing') IS NULL,
                    -- extracting the JSON null keeps it a VARIANT null, not SQL NULL
                    variant_extract({v}, 'n') IS NULL,
                    variant_type(variant_extract({v}, 'n')) IS NULL,
                    variant_extract(NULL, 'a') IS NULL;"
        ),
    );
    assert_eq!(
        got,
        vec![vec![
            Value::Integer(1),
            Value::Double(2.5),
            Value::Varchar("\"s\"".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Varchar("{\"x\":1}".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            // 'n' holds the JSON null: the extract is a VARIANT null.
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
    // An array path needs a bracket; a bare field name misses.
    let arrays = rows(
        &mut db,
        "SELECT variant_extract(CAST('[10,20,30]' AS VARIANT), '[1]'),
                variant_extract(CAST('[10,20,30]' AS VARIANT), '1') IS NULL,
                variant_extract(CAST('[10,20,30]' AS VARIANT), '$[2]');",
    );
    assert_eq!(
        arrays,
        vec![vec![variant("20"), Value::Boolean(true), variant("30")]]
    );
    // A field of a scalar is a miss, not an error (DuckDB:
    // `(json('1')::VARIANT)['a']` is NULL).
    assert_eq!(
        rows(
            &mut db,
            "SELECT variant_extract(CAST('1' AS VARIANT), 'a') IS NULL;"
        ),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        err(&mut db, "SELECT variant_extract(1, 'a');"),
        "variant_extract needs a VARIANT argument, got INTEGER"
    );
    assert_eq!(
        err(&mut db, "SELECT variant_extract(CAST('1' AS VARIANT), 1);"),
        "variant_extract's path must be VARCHAR"
    );
    // A malformed path is an error, not a miss.
    let malformed = err(
        &mut db,
        "SELECT variant_extract(CAST('{\"a\":1}' AS VARIANT), 'a[');",
    );
    assert_eq!(malformed, "invalid VARIANT path \"a[\"");
}

// --------------------------------------------------------------- semantics

#[test]
fn equality_folds_numbers_and_keeps_kinds_apart() {
    let mut db = PinkDb::new();
    let got = rows(
        &mut db,
        "SELECT CAST('1' AS VARIANT) = CAST('1.0' AS VARIANT),
                CAST('1' AS VARIANT) = CAST('1.00' AS VARIANT),
                CAST('1' AS VARIANT) = CAST('\"1\"' AS VARIANT),
                CAST('1' AS VARIANT) = CAST('true' AS VARIANT),
                CAST('1' AS VARIANT) < CAST('2' AS VARIANT),
                CAST('1' AS VARIANT) < CAST('1.5' AS VARIANT),
                CAST('\"a\"' AS VARIANT) < CAST('\"b\"' AS VARIANT),
                CAST('1' AS VARIANT) <> CAST('2' AS VARIANT);",
    );
    assert_eq!(
        got,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
    // A variant-null is not SQL NULL, but they group together (see below).
    let nulls = rows(
        &mut db,
        "SELECT CAST('null' AS VARIANT) IS NULL,
                CAST('null' AS VARIANT) = CAST('null' AS VARIANT);",
    );
    assert_eq!(
        nulls,
        vec![vec![Value::Boolean(false), Value::Boolean(true)]]
    );
}

#[test]
fn ordering_is_total_across_kinds() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE o (v VARIANT);").unwrap();
    db.execute(
        "INSERT INTO o VALUES
           (CAST('{\"a\":1}' AS VARIANT)), (CAST('[1]' AS VARIANT)),
           (CAST('\"s\"' AS VARIANT)), (CAST(DATE '2020-01-02' AS VARIANT)),
           (CAST(TIMESTAMP '2020-01-02 00:00:00' AS VARIANT)),
           (CAST('1' AS VARIANT)), (CAST('true' AS VARIANT)),
           (CAST('null' AS VARIANT)), (NULL);",
    )
    .unwrap();
    let order: Vec<String> = rows(&mut db, "SELECT CAST(v AS VARCHAR) FROM o ORDER BY v;")
        .into_iter()
        .map(|r| match &r[0] {
            Value::Null => "<sql null>".to_string(),
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        order,
        vec![
            "true",
            "1",
            "\"s\"",
            "\"2020-01-02\"",
            "\"2020-01-02 00:00:00\"",
            "[1]",
            "{\"a\":1}",
            "null",
            "<sql null>",
        ]
    );
    // DESC reverses the kinds but keeps first-seen order among equal keys.
    let desc: Vec<String> = rows(&mut db, "SELECT CAST(v AS VARCHAR) FROM o ORDER BY v DESC;")
        .into_iter()
        .map(|r| match &r[0] {
            Value::Null => "<sql null>".to_string(),
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        desc,
        vec![
            "null",
            "{\"a\":1}",
            "[1]",
            "\"2020-01-02 00:00:00\"",
            "\"2020-01-02\"",
            "\"s\"",
            "1",
            "true",
            "<sql null>",
        ]
    );
    // MIN/MAX follow the same order.
    let extrema = rows(
        &mut db,
        "SELECT CAST(MIN(v) AS VARCHAR), CAST(MAX(v) AS VARCHAR) FROM o;",
    );
    assert_eq!(
        extrema,
        vec![vec![
            Value::Varchar("true".into()),
            // A variant-null sorts after every other kind (SQL NULLs are
            // skipped by MIN/MAX).
            Value::Varchar("null".into())
        ]]
    );
}

#[test]
fn grouping_and_distinct_use_the_hash_key() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE g (v VARIANT);").unwrap();
    db.execute(
        "INSERT INTO g VALUES
           (CAST('1' AS VARIANT)), (CAST('1.0' AS VARIANT)), (CAST('1.00' AS VARIANT)),
           (CAST('\"1\"' AS VARIANT)), (CAST('true' AS VARIANT)),
           (CAST('null' AS VARIANT)), (NULL), (NULL);",
    )
    .unwrap();
    let grouped = rows(&mut db, "SELECT v, count(*) AS n FROM g GROUP BY v;");
    let mut flat: Vec<(String, i64)> = grouped
        .into_iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Null, Value::Bigint(n)) => ("<sql null>".to_string(), *n),
            (Value::Variant(v), Value::Bigint(n)) => (v.to_json(), *n),
            other => panic!("{other:?}"),
        })
        .collect();
    flat.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    assert_eq!(
        flat,
        vec![
            ("1".to_string(), 3),          // 1, 1.0, 1.00
            ("<sql null>".to_string(), 2), // the two SQL NULLs
            ("\"1\"".to_string(), 1),
            ("null".to_string(), 1), // a variant-null is its own value
            ("true".to_string(), 1),
        ]
    );
    let distinct = rows(&mut db, "SELECT count(DISTINCT v) FROM g;");
    // count(DISTINCT) skips SQL NULLs but counts the variant-null: four
    // distinct values, five DISTINCT rows.
    assert_eq!(distinct, vec![vec![Value::Bigint(4)]]);
    let distinct_rows = rows(&mut db, "SELECT DISTINCT v FROM g;");
    assert_eq!(distinct_rows.len(), 5);
}

// ------------------------------------------------------------- persistence

#[test]
fn variant_columns_round_trip_through_pink_bytes() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE v (id INTEGER, x VARIANT);")
        .unwrap();
    db.execute(
        "INSERT INTO v VALUES
           (1, CAST('{\"a\":[1,2.5,\"s\",null,true],\"b\":{\"c\":-3}}' AS VARIANT)),
           (2, CAST('1' AS VARIANT)),
           (3, CAST('null' AS VARIANT)),
           (4, NULL),
           (5, CAST('\"héllo 世界 🚀\"' AS VARIANT)),
           (6, CAST('9223372036854775807' AS VARIANT)),
           (7, CAST(DATE '2020-01-02' AS VARIANT)),
           (8, CAST(TIMESTAMP '2020-01-02 03:04:05.000001' AS VARIANT)),
           (9, CAST(CAST(1.25 AS DECIMAL(10,2)) AS VARIANT));",
    )
    .unwrap();
    let before = rows(&mut db, "SELECT id, x FROM v ORDER BY id;");
    let described = rows(&mut db, "DESCRIBE v;");
    assert_eq!(described[1][1], Value::Varchar("VARIANT".into()));

    let bytes = db.to_pink_bytes().unwrap();
    let mut back = PinkDb::from_pink_bytes(&bytes).unwrap();
    assert_eq!(rows(&mut back, "SELECT id, x FROM v ORDER BY id;"), before);

    // A saved file (the same encoding, through the file API).
    let dir = scratch("bytes");
    let path = dir.join("db.pink");
    db.save(path.to_str().unwrap()).unwrap();
    let mut file_db = PinkDb::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        rows(&mut file_db, "SELECT id, x FROM v ORDER BY id;"),
        before
    );

    // Kinds survive even where the text cannot: a DATE stays a DATE.
    assert_eq!(
        rows(&mut file_db, "SELECT variant_type(x) FROM v WHERE id = 7;"),
        vec![vec![Value::Varchar("DATE".into())]]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn variant_default_values_persist() {
    let mut db = PinkDb::new();
    // A column DEFAULT is stored as a VARIANT default value in the file.
    db.execute("CREATE TABLE d (id INTEGER);").unwrap();
    db.execute("ALTER TABLE d ADD COLUMN x VARIANT DEFAULT CAST('{\"a\":1}' AS VARIANT);")
        .unwrap();
    db.execute("INSERT INTO d (id) VALUES (1);").unwrap();
    let before = rows(&mut db, "SELECT x FROM d;");
    assert_eq!(before, vec![vec![variant("{\"a\":1}")]]);
    let bytes = db.to_pink_bytes().unwrap();
    let mut back = PinkDb::from_pink_bytes(&bytes).unwrap();
    back.execute("INSERT INTO d (id) VALUES (2);").unwrap();
    assert_eq!(
        rows(&mut back, "SELECT x FROM d;"),
        vec![before[0].clone(); 2]
    );
}

#[test]
fn variant_survives_wal_replay_and_checkpoint() {
    let dir = scratch("wal");
    let path = dir.join("db.pink");
    {
        let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE v (id INTEGER, x VARIANT);")
            .unwrap();
        db.execute("INSERT INTO v VALUES (1, CAST('{\"a\":[1,2]}' AS VARIANT));")
            .unwrap();
        // Flush the first row into the checkpoint, then log a second one in
        // the WAL only.
        db.checkpoint().unwrap();
        db.execute("INSERT INTO v VALUES (2, CAST('[true,\"x\",null]' AS VARIANT)), (3, NULL);")
            .unwrap();
    }
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    let got = rows(&mut db, "SELECT id, CAST(x AS VARCHAR) FROM v ORDER BY id;");
    assert_eq!(
        got,
        vec![
            vec![Value::Integer(1), Value::Varchar("{\"a\":[1,2]}".into())],
            vec![
                Value::Integer(2),
                Value::Varchar("[true,\"x\",null]".into())
            ],
            vec![Value::Integer(3), Value::Null],
        ]
    );
    // And the replayed database saves a checkpoint that reads back the same.
    db.checkpoint().unwrap();
    drop(db);
    let mut again = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(
        rows(
            &mut again,
            "SELECT id, CAST(x AS VARCHAR) FROM v ORDER BY id;"
        ),
        got
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn variant_parameters_and_updates_work() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE p (id INTEGER, x VARIANT);")
        .unwrap();
    let insert = db.prepare("INSERT INTO p VALUES (?, ?);").unwrap();
    db.execute_prepared(&insert, &[Value::Integer(1), variant("{\"a\":1}")])
        .unwrap();
    // A prepared statement substitutes a VARIANT literal: it must re-bind.
    assert_eq!(
        rows(&mut db, "SELECT CAST(x AS VARCHAR) FROM p;"),
        vec![vec![Value::Varchar("{\"a\":1}".into())]]
    );
    let select = db
        .prepare("SELECT id FROM p WHERE x = CAST('{\"a\":1}' AS VARIANT);")
        .unwrap();
    assert_eq!(
        db.execute_prepared(&select, &[]).unwrap(),
        db.execute("SELECT id FROM p WHERE x = CAST('{\"a\":1}' AS VARIANT);")
            .unwrap()
            .pop()
            .unwrap()
    );
    // A DATE inside a VARIANT has no SQL literal form (JSON text cannot carry
    // the kind), so a prepared parameter refuses it instead of silently
    // demoting it to a string.
    let dated = Value::Variant(Arc::new(Variant::Date(18263)));
    let e = db
        .execute_prepared(&insert, &[Value::Integer(2), dated])
        .unwrap_err()
        .to_string();
    assert!(e.contains("cannot be written as a SQL literal"), "{e}");
    // UPDATE and a widening INSERT both accept a VARIANT value.
    db.execute("UPDATE p SET x = CAST('[1]' AS VARIANT) WHERE id = 1;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT variant_type(x) FROM p;"),
        vec![vec![Value::Varchar("ARRAY".into())]]
    );
}

// ----------------------------------------------------------- DuckDB parity

/// Where DuckDB 2.0.0 and pinkdb agree, pinned so drift shows up. The
/// measured DuckDB answers are quoted beside each case.
#[test]
fn duckdb_parity_on_shared_behaviour() {
    let mut db = PinkDb::new();
    let cases: &[(&str, &str, &str)] = &[
        // duckdb: json('{"a":1}')::VARIANT -> OBJECT
        ("CAST('{\"a\":1}' AS VARIANT)", "variant_type", "OBJECT"),
        // duckdb: json('true')::VARIANT -> BOOL_TRUE (one BOOL kind here)
        ("CAST('true' AS VARIANT)", "variant_type", "BOOL"),
        // duckdb: json('1')::VARIANT -> UINT64 (BIGINT here: no unsigned kind)
        ("CAST('1' AS VARIANT)", "variant_type", "BIGINT"),
        // duckdb: json('1.5')::VARIANT -> DOUBLE; pinkdb keeps a fractional
        // JSON number exact as DECIMAL (documented difference)
        ("CAST('1.5' AS VARIANT)", "variant_type", "DECIMAL"),
        // duckdb: json('"s"')::VARIANT -> VARCHAR
        ("CAST('\"s\"' AS VARIANT)", "variant_type", "VARCHAR"),
        // duckdb: json('[1]')::VARIANT -> ARRAY
        ("CAST('[1]' AS VARIANT)", "variant_type", "ARRAY"),
        // duckdb: json('null')::VARIANT -> variant_type NULL
        ("CAST('null' AS VARIANT)", "variant_type", "NULL"),
        // duckdb: variant_keys({'a':1,'b':2}) -> [a, b]; pinkdb spells the
        // array as JSON text (it has no LIST type)
        (
            "CAST('{\"b\":2,\"a\":1}' AS VARIANT)",
            "variant_keys",
            "[\"a\",\"b\"]",
        ),
        // duckdb: variant_keys(1::VARIANT) -> []
        ("CAST('1' AS VARIANT)", "variant_keys", "[]"),
        // duckdb: json('1.9')::VARIANT::INTEGER -> 2
        ("CAST('1.9' AS VARIANT)", "cast_integer", "2"),
        // duckdb: json('"5"')::VARIANT::INTEGER -> 5
        ("CAST('\"5\"' AS VARIANT)", "cast_integer", "5"),
        // duckdb: json('123')::VARIANT::DECIMAL(10,2) -> 123.00
        ("CAST('123' AS VARIANT)", "cast_decimal", "123.00"),
        // duckdb: json('null')::VARIANT::INTEGER IS NULL -> true
        ("CAST('null' AS VARIANT)", "cast_integer", "NULL"),
        // duckdb: (json('{"a":1}')::VARIANT)::VARCHAR -> {'a': 1} (struct
        // text); pinkdb prints JSON, the lossless form
        ("CAST('{\"a\":1}' AS VARIANT)", "text", "{\"a\":1}"),
    ];
    for (expr, how, want) in cases {
        let sql = match *how {
            "variant_type" => format!("SELECT variant_type({expr});"),
            "variant_keys" => format!("SELECT variant_keys({expr});"),
            "cast_integer" => format!("SELECT CAST(CAST({expr} AS INTEGER) AS VARCHAR);"),
            "cast_decimal" => {
                format!("SELECT CAST(CAST(CAST({expr} AS DECIMAL(10,2)) AS VARIANT) AS VARCHAR);")
            }
            "text" => format!("SELECT CAST({expr} AS VARCHAR);"),
            other => panic!("unknown case {other}"),
        };
        let got = text(&mut db, &sql);
        let want = if *want == "NULL" {
            None
        } else {
            Some((*want).to_string())
        };
        assert_eq!(got, want, "{sql}");
    }
    // duckdb: 1::VARIANT = 1.0::VARIANT -> true; '1' is a different kind
    assert_eq!(
        rows(
            &mut db,
            "SELECT CAST('1' AS VARIANT) = CAST('1.0' AS VARIANT),
                    CAST('1' AS VARIANT) = CAST('\"1\"' AS VARIANT);"
        ),
        vec![vec![Value::Boolean(true), Value::Boolean(false)]]
    );
    // duckdb: ORDER BY crosses kinds as BOOL < NUMBER < VARCHAR < DATE <
    // TIMESTAMP < ARRAY < OBJECT, with a variant JSON null after them
    // (`ordering_is_total_across_kinds` pins the same sequence).
    let _ = DataType::Variant;
}

// ------------------------------------------------- implicit coercion (101b)

/// A comparison between a VARIANT path and a scalar coerces the scalar instead
/// of refusing it (`comparison_ok` has no VARIANT/scalar rule), so DuckDB's
/// `variant_extract(v, 'a') = 5` spelling works. A VARCHAR literal becomes a
/// VARIANT *string*, not JSON: `CAST(VARCHAR AS VARIANT)` parses JSON, so
/// coercing through it would reject `'buy'`.
#[test]
fn comparisons_coerce_the_scalar_side_to_the_variant_kind() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t(v VARIANT);").unwrap();
    db.execute(
        "INSERT INTO t VALUES (CAST('{\"a\": 5, \"kind\": \"buy\", \"ok\": true}' AS VARIANT));",
    )
    .unwrap();
    let got = rows(
        &mut db,
        "SELECT variant_extract(v, '$.a') = 5,
                v['a'] = 5,
                variant_extract(v, '$.kind') = 'buy',
                v['kind'] = 'buy',
                variant_extract(v, '$.kind') <> 'view',
                variant_extract(v, '$.ok') = true,
                5 = variant_extract(v, '$.a'),
                'buy' = variant_extract(v, '$.kind'),
                v['missing'] = 5
         FROM t;",
    );
    assert_eq!(
        got,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Null,
        ]]
    );
    // A non-literal VARCHAR still needs an explicit cast: there is no way to
    // build a VARIANT string from a runtime string without one.
    let message = err(
        &mut db,
        "SELECT count(*) FROM t WHERE v['kind'] = upper('buy');",
    );
    assert!(
        message.contains("VARIANT") && message.contains("VARCHAR"),
        "unexpected error: {message}"
    );
}
