//! `ARRAY` (wave 107a) end to end: the fixed-size type in the parser and
//! `typeof`, the literal and `array_value` constructors, casts in every
//! direction, element access, `len` / `array_length` / `array_extract`,
//! equality/ordering/grouping/DISTINCT with NULL arrays and NULL elements,
//! `.pink` + WAL + checkpoint persistence, a 100k x 768 vector table, and the
//! same answers DuckDB 2.0.0 gives (measured with
//! `/tmp/duckdb-2.0.0/duckdb -no-agent`).
//!
//! `docs/wave-107-plan.md` records where pinkdb deliberately differs.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use pinkdb::{ArrayElem, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_array_{tag}_{}_{}",
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

/// The single cell of `sql`, printed.
fn cell(db: &mut PinkDb, sql: &str) -> String {
    rows(db, sql)[0][0].to_string()
}

fn count(db: &mut PinkDb, sql: &str) -> i64 {
    match &rows(db, sql)[0][0] {
        Value::Integer(v) => i64::from(*v),
        Value::Bigint(v) => *v,
        other => panic!("expected a count from {sql}, got {other:?}"),
    }
}

/// A `t(id INTEGER, ...)` table holding exactly `rows`.
fn table(db: &mut PinkDb, columns: &str) {
    db.execute(&format!("CREATE TABLE t (id INTEGER, {columns});"))
        .unwrap();
}

// ------------------------------------------------- 32-bit FLOAT elements

#[test]
fn float_arrays_store_four_byte_elements() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (f FLOAT[3], d DOUBLE[3]);")
        .unwrap();
    // The element kind is part of the type and survives the round trip.
    assert_eq!(
        text(&mut db, "SELECT typeof(CAST([1,2,3] AS FLOAT[3]));"),
        Some("FLOAT[3]".into())
    );
    // DuckDB keeps the element type (`typeof(v[1])` is FLOAT); pinkdb has no
    // FLOAT scalar, so an element widens to DOUBLE — exact, and the one
    // documented difference (plan, "Differences from DuckDB").
    assert_eq!(
        text(
            &mut db,
            "SELECT typeof(CAST([1.5,2.5,3.5] AS FLOAT[3])[2]);"
        ),
        Some("DOUBLE".into())
    );
    assert_eq!(
        cell(&mut db, "SELECT CAST([1.5,2.5,3.5] AS FLOAT[3])[2];"),
        "2.5"
    );
    // DOUBLE -> FLOAT rounds to nearest (DuckDB: 16777217 becomes 16777216).
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST([16777217.0, 0.1, 1e-50] AS DOUBLE[3])::FLOAT[3][1];"
        ),
        "16777216"
    );
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST([0.1] AS DOUBLE[1]) AS FLOAT[1])[1];"
        ),
        "0.10000000149011612"
    );
    // Underflow is silent; infinity and NaN pass through.
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST([CAST('inf' AS DOUBLE), CAST('nan' AS DOUBLE)] AS DOUBLE[2]) AS FLOAT[2])[1];"
        ),
        "inf"
    );
    // A finite value that rounds to infinity is out of range (measured on
    // DuckDB 2.0: a Conversion Error, not a silent inf).
    let message = err(&mut db, "SELECT CAST([1e40] AS DOUBLE[1])::FLOAT[1];");
    assert!(message.contains("out of range for FLOAT"), "{message}");
    // The text path parses straight to f32 in DuckDB, so '1e40' is inf.
    assert_eq!(
        cell(&mut db, "SELECT CAST('[1e40]' AS FLOAT[1])[1];"),
        "inf"
    );
    // FLOAT -> DOUBLE is exact, INTEGER -> FLOAT is exact up to 2^24.
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST([0.1] AS FLOAT[1]) AS DOUBLE[1])[1];"
        ),
        "0.10000000149011612"
    );
    assert_eq!(cell(&mut db, "SELECT CAST([7] AS FLOAT[1])[1];"), "7");
    // A FLOAT column and a DOUBLE column compare by value.
    db.execute("INSERT INTO t VALUES ([1.5, 2.5, 3.5], [1.5, 2.5, 3.5]);")
        .unwrap();
    assert_eq!(
        text(&mut db, "SELECT CAST(f = d AS VARCHAR) FROM t;"),
        Some("true".into())
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(f = CAST([1.0, 2.0, 3.0] AS FLOAT[3]) AS VARCHAR) FROM t;"
        ),
        Some("false".into())
    );
    // Inserting a DOUBLE literal narrows it; a mismatch is an error.
    db.execute("INSERT INTO t (f) VALUES ([1.0, 2.0, 3.0]);")
        .unwrap();
    // A scalar DOUBLE prints without the fraction (`3`, not `3.0`).
    assert_eq!(cell(&mut db, "SELECT f[3] FROM t WHERE d IS NULL;"), "3");
    let message = err(&mut db, "INSERT INTO t (f) VALUES ([1.0, 2.0]);");
    assert!(message.contains("cannot CAST"), "{message}");
    // ARRAY <-> VARCHAR and ARRAY <-> VARIANT keep the kind.
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(CAST('[1.5, NULL]' AS FLOAT[2]) AS VARCHAR);"
        ),
        Some("[1.5, NULL]".into())
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT typeof(CAST(CAST([1.5] AS FLOAT[1]) AS VARIANT)::FLOAT[1]);"
        ),
        Some("FLOAT[1]".into())
    );
    // Grouping and DISTINCT key on the narrowed bits.
    db.execute("INSERT INTO t (f) VALUES ([0.1, 0.2, 0.3]), ([0.1, 0.2, 0.3]);")
        .unwrap();
    // [1.5,2.5,3.5], [1.0,2.0,3.0] and the two equal [0.1,0.2,0.3] rows.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(DISTINCT f) FROM t WHERE f IS NOT NULL;"
        ),
        3
    );
}

#[test]
fn float_arrays_survive_save_open_and_replay() {
    let dir = scratch("float_persist");
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, f FLOAT[3]);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, [0.1, 0.2, 0.3]), (2, NULL), (3, [1.0, NULL, 3.0]);")
        .unwrap();
    db.save(path).unwrap();
    let mut db = PinkDb::open(path).unwrap();
    assert_eq!(
        text(&mut db, "SELECT typeof(f) FROM t LIMIT 1;"),
        Some("FLOAT[3]".into())
    );
    // 0.1 as f32 is 0.10000000149011612 — the stored bits, not the literal.
    assert_eq!(
        cell(&mut db, "SELECT f[1] FROM t WHERE id = 1;"),
        "0.10000000149011612"
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(f[2] IS NULL AS VARCHAR) FROM t WHERE id = 3;"
        ),
        Some("true".into())
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The 4-byte counterpart of the DOUBLE measurement: an incompressible
/// `FLOAT[768]` table must store ~4 bytes per element.
#[test]
fn a_float_vector_table_stores_four_bytes_per_element() {
    const ROWS: usize = 100_000;
    const DIM: u32 = 768;
    let dir = scratch("float_vectors");
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();

    let mut db = PinkDb::new();
    db.execute(&format!("CREATE TABLE vec (id INTEGER, e FLOAT[{DIM}]);"))
        .unwrap();
    let started = Instant::now();
    let mut id = 0;
    while id < ROWS {
        let n = (ROWS - id).min(pinkdb::CHUNK_SIZE);
        let mut ids = pinkdb::Vector::with_capacity(DataType::Integer, n);
        let mut values = pinkdb::Vector::with_capacity(
            DataType::Array {
                elem: ArrayElem::Float,
                len: DIM,
            },
            n,
        );
        for row in 0..n {
            let elements = (0..DIM)
                .map(|k| Value::Double(splitmix(id + row, k)))
                .collect::<Vec<_>>();
            ids.push_value(Value::Integer((id + row) as i32)).unwrap();
            values
                .push_value(Value::Array(std::sync::Arc::new(
                    pinkdb::array::ArrayValue::from_values(ArrayElem::Float, DIM, &elements)
                        .unwrap(),
                )))
                .unwrap();
        }
        db.table_mut_in("main", "vec")
            .unwrap()
            .append_chunk(vec![ids, values])
            .unwrap();
        id += n;
    }
    let inserted = started.elapsed();
    assert_eq!(count(&mut db, "SELECT count(*) FROM vec;"), ROWS as i64);
    assert_eq!(
        cell(&mut db, "SELECT len(e) FROM vec WHERE id = 99999;"),
        DIM.to_string()
    );
    let scanned = started.elapsed();
    db.save(path).unwrap();
    let bytes = std::fs::metadata(path).unwrap().len() as usize;
    let per_element = bytes as f64 / (ROWS * DIM as usize) as f64;
    println!(
        "{ROWS} x {DIM} FLOAT rows: insert {inserted:?}, scan {scanned:?}, file {} MiB \
         ({per_element:.2} bytes/element)",
        bytes / (1024 * 1024)
    );
    assert!(
        (3.9..4.5).contains(&per_element),
        "a FLOAT[768] table stores {per_element:.2} bytes per element, not ~4"
    );
    // The values come back as f32, not the f64 the insert built.
    let mut db = PinkDb::open(path).unwrap();
    let first: f64 = splitmix(12345, 0);
    let want = format!("{}", f64::from(first as f32));
    assert_eq!(
        cell(&mut db, "SELECT e[1] FROM vec WHERE id = 12345;"),
        want
    );
    std::fs::remove_dir_all(&dir).ok();
}

// ------------------------------------------------------- types and parsing

#[test]
fn every_numeric_element_type_and_length_parses() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (
             f3 FLOAT[3], d2 DOUBLE[2], i4 INTEGER[4], b2 BIGINT[2],
             lower int[1], real2 REAL[2]
         );",
    )
    .unwrap();
    let described = rows(&mut db, "DESCRIBE t;");
    let types: Vec<String> = described
        .iter()
        .map(|row| row[1].to_string())
        .collect::<Vec<_>>();
    // FLOAT/REAL in an ARRAY name the 4-byte element kind (a *scalar*
    // FLOAT is still DOUBLE), and the element count is part of the name.
    assert_eq!(
        types,
        vec![
            "FLOAT[3]",
            "DOUBLE[2]",
            "INTEGER[4]",
            "BIGINT[2]",
            "INTEGER[1]",
            "FLOAT[2]"
        ]
    );

    // The type name round-trips through `typeof` and through the parser's
    // own text form, including through the lake catalog's text spelling.
    assert_eq!(
        text(&mut db, "SELECT typeof(CAST([1,2,3] AS INTEGER[3]));"),
        Some("INTEGER[3]".to_string())
    );
    assert_eq!(
        DataType::from_sql("DOUBLE[768]"),
        Some(DataType::Array {
            elem: ArrayElem::Double,
            len: 768
        })
    );
    assert_eq!(
        DataType::from_sql("FLOAT[768]"),
        Some(DataType::Array {
            elem: ArrayElem::Float,
            len: 768
        })
    );
    assert_eq!(
        DataType::from_sql("REAL[3]"),
        Some(DataType::Array {
            elem: ArrayElem::Float,
            len: 3
        })
    );
    // A scalar FLOAT is still DOUBLE: only the array spelling is 32-bit.
    assert_eq!(DataType::from_sql("FLOAT"), Some(DataType::Double));
    assert_eq!(DataType::from_sql("DOUBLE[0]"), None);
    assert_eq!(DataType::from_sql("VARCHAR[3]"), None);
}

#[test]
fn malformed_array_types_are_refused() {
    let mut db = PinkDb::new();
    for (sql, want) in [
        ("CREATE TABLE t (v DOUBLE[0]);", "array length"),
        ("CREATE TABLE t (v DOUBLE[-1]);", "parse error"),
        ("CREATE TABLE t (v VARCHAR[3]);", "ARRAY elements must be"),
        ("CREATE TABLE t (v DOUBLE[999999999999]);", "length"),
        (
            "CREATE TABLE t (v DOUBLE[]);",
            "variable-length LIST; pinkdb has fixed-size ARRAY only (wave 107a2)",
        ),
    ] {
        let message = err(&mut db, sql);
        assert!(
            message.contains(want),
            "{sql}: expected {want:?} in {message:?}"
        );
    }
}

// -------------------------------------------------------------- constructors

#[test]
fn literals_and_array_value_build_the_standard_numeric_tower() {
    let mut db = PinkDb::new();
    // A literal with no target is typed by its elements.
    assert_eq!(
        text(&mut db, "SELECT typeof([1,2,3]);"),
        Some("INTEGER[3]".into())
    );
    assert_eq!(
        text(&mut db, "SELECT typeof([1,2,3.5]);"),
        Some("DOUBLE[3]".into())
    );
    assert_eq!(
        text(&mut db, "SELECT typeof([1, 20000000000]);"),
        Some("BIGINT[2]".into())
    );
    assert_eq!(cell(&mut db, "SELECT [1,2,3];"), "[1, 2, 3]");
    assert_eq!(
        text(&mut db, "SELECT CAST([1.0, 2.5] AS VARCHAR);"),
        Some("[1.0, 2.5]".into())
    );

    // `array_value` is the same constructor spelled as a function.
    assert_eq!(
        text(&mut db, "SELECT typeof(array_value(1, 2, 3));"),
        Some("INTEGER[3]".into())
    );
    assert_eq!(cell(&mut db, "SELECT array_value(1.5, 2.5);"), "[1.5, 2.5]");

    // A CAST pins the element type and length.
    assert_eq!(
        text(&mut db, "SELECT typeof(CAST([1,2,3] AS DOUBLE[3]));"),
        Some("DOUBLE[3]".into())
    );
    assert_eq!(
        cell(&mut db, "SELECT CAST([1,2,3] AS DOUBLE[3]);"),
        "[1.0, 2.0, 3.0]"
    );
    assert_eq!(
        cell(&mut db, "SELECT CAST([1,2,3] AS BIGINT[3]);"),
        "[1, 2, 3]"
    );

    // NULL elements are values in their own right.
    assert_eq!(
        cell(&mut db, "SELECT CAST('[1, NULL, 3]' AS INTEGER[3]);"),
        "[1, NULL, 3]"
    );

    // Constructors are per row: they take columns, not just constants.
    table(&mut db, "a INTEGER, b INTEGER");
    db.execute("INSERT INTO t VALUES (1, 2, 3), (4, 5, 6);")
        .unwrap();
    let got = rows(
        &mut db,
        "SELECT array_value(a, b), [a, b, id] FROM t ORDER BY id;",
    );
    assert_eq!(got[0][0].to_string(), "[2, 3]");
    assert_eq!(got[0][1].to_string(), "[2, 3, 1]");
    assert_eq!(got[1][0].to_string(), "[5, 6]");

    // Errors.
    let empty = err(&mut db, "SELECT [];");
    assert!(empty.contains("needs at least one element"), "{empty}");
    let nulls = err(&mut db, "SELECT [NULL, NULL];");
    assert!(nulls.contains("all-NULL ARRAY"), "{nulls}");
    let strings = err(&mut db, "SELECT ['a', 'b'];");
    assert!(strings.contains("ARRAY elements must be"), "{strings}");
}

// ------------------------------------------------------------------- casts

#[test]
fn casts_run_in_every_direction() {
    let mut db = PinkDb::new();

    // ARRAY -> ARRAY: the length is part of the type, the element kind converts.
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST([1,2,3] AS INTEGER[3]) AS DOUBLE[3]);"
        ),
        "[1.0, 2.0, 3.0]"
    );
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST('[1.5, 2.5]' AS VARCHAR) AS DOUBLE[2]);"
        ),
        "[1.5, 2.5]"
    );

    // ARRAY <-> VARCHAR.
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(CAST([1.5, NULL] AS DOUBLE[2]) AS VARCHAR);"
        ),
        Some("[1.5, NULL]".into())
    );
    assert_eq!(
        cell(&mut db, "SELECT CAST('[4, 5, 6]' AS BIGINT[3]);"),
        "[4, 5, 6]"
    );

    // ARRAY <-> VARIANT and JSON.
    assert_eq!(
        text(&mut db, "SELECT CAST(CAST([1,2,3] AS VARIANT) AS VARCHAR);"),
        Some("[1,2,3]".into())
    );
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST('[1,2,3]' AS VARIANT) AS INTEGER[3]);"
        ),
        "[1, 2, 3]"
    );
    assert_eq!(
        cell(
            &mut db,
            "SELECT CAST(CAST(CAST([1.5, NULL] AS DOUBLE[2]) AS VARIANT) AS DOUBLE[2]);"
        ),
        "[1.5, NULL]"
    );

    // NULL casts to any ARRAY type.
    assert_eq!(text(&mut db, "SELECT CAST(NULL AS DOUBLE[3]);"), None);

    // A length mismatch names the sizes.
    let length = err(&mut db, "SELECT CAST([1,2] AS DOUBLE[3]);");
    assert!(
        length.contains("array size is part of the type"),
        "{length}"
    );
    let wide = err(
        &mut db,
        "SELECT CAST(CAST([1,2,3] AS INTEGER[3]) AS DOUBLE[4]);",
    );
    assert!(wide.contains("array size is part of the type"), "{wide}");

    // A malformed text payload names what is wrong.
    let short = err(&mut db, "SELECT CAST('[1,2]' AS DOUBLE[3]);");
    assert!(short.contains("found 2 elements"), "{short}");
    let bad = err(&mut db, "SELECT CAST('[1,2,x]' AS DOUBLE[3]);");
    assert!(bad.contains('x'), "{bad}");
    let variant_short = err(
        &mut db,
        "SELECT CAST(CAST('[1,2]' AS VARIANT) AS DOUBLE[3]);",
    );
    assert!(variant_short.contains("array"), "{variant_short}");

    // TRY_CAST turns the failures into NULL.
    assert_eq!(
        text(&mut db, "SELECT TRY_CAST('[1,2]' AS DOUBLE[3]);"),
        None
    );
    assert_eq!(
        cell(&mut db, "SELECT TRY_CAST('[1,2,3]' AS DOUBLE[3]);"),
        "[1.0, 2.0, 3.0]"
    );
}

// --------------------------------------------------------- element access

#[test]
fn element_access_is_one_based_and_forgiving() {
    let mut db = PinkDb::new();
    table(&mut db, "v DOUBLE[3]");
    db.execute("INSERT INTO t VALUES (1, [1.0, 2.0, 3.0]), (2, NULL), (3, [4.0, NULL, 6.0]);")
        .unwrap();

    assert_eq!(cell(&mut db, "SELECT v[1] FROM t WHERE id = 1;"), "1");
    assert_eq!(cell(&mut db, "SELECT v[3] FROM t WHERE id = 1;"), "3");
    // Out of range, zero, negative and NULL indexes are NULL, like DuckDB.
    for index in ["0", "4", "-1", "NULL"] {
        assert_eq!(
            text(&mut db, &format!("SELECT v[{index}] FROM t WHERE id = 1;")),
            None,
            "v[{index}]"
        );
    }
    // A NULL array and a NULL element both read as NULL.
    assert_eq!(text(&mut db, "SELECT v[2] FROM t WHERE id = 2;"), None);
    assert_eq!(text(&mut db, "SELECT v[2] FROM t WHERE id = 3;"), None);
    assert_eq!(cell(&mut db, "SELECT v[3] FROM t WHERE id = 3;"), "6");

    // `array_extract` is the function spelling and takes a Bigint index.
    assert_eq!(
        cell(&mut db, "SELECT array_extract(v, 2) FROM t WHERE id = 3;"),
        "NULL"
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(array_extract(v, CAST(3 AS BIGINT)) AS VARCHAR) FROM t WHERE id = 1;"
        ),
        Some("3".into())
    );
    assert_eq!(
        text(&mut db, "SELECT array_extract(v, 9) FROM t WHERE id = 1;"),
        None
    );

    // Indexes must be integers.
    let frac = err(&mut db, "SELECT array_extract(v, 1.5) FROM t;");
    assert!(frac.contains("integer"), "{frac}");
    let letter = err(&mut db, "SELECT v['x'] FROM t;");
    assert!(letter.contains("integer"), "{letter}");
}

#[test]
fn len_and_array_length_report_the_declared_size() {
    let mut db = PinkDb::new();
    table(&mut db, "v DOUBLE[3], s VARCHAR");
    db.execute("INSERT INTO t VALUES (1, [1.0, 2.0, 3.0], 'abcd'), (2, NULL, NULL);")
        .unwrap();
    assert_eq!(count(&mut db, "SELECT len(v) FROM t WHERE id = 1;"), 3);
    assert_eq!(
        count(&mut db, "SELECT array_length(v) FROM t WHERE id = 1;"),
        3
    );
    // A NULL array has no length; a VARCHAR keeps its own `len`.
    assert_eq!(text(&mut db, "SELECT len(v) FROM t WHERE id = 2;"), None);
    assert_eq!(count(&mut db, "SELECT len(s) FROM t WHERE id = 1;"), 4);
    let wrong = err(&mut db, "SELECT array_length(id) FROM t;");
    assert!(wrong.contains("ARRAY"), "{wrong}");
}

// ------------------------------------------------ equality and ordering

#[test]
fn equality_ordering_grouping_and_distinct() {
    let mut db = PinkDb::new();
    table(&mut db, "v DOUBLE[3]");
    db.execute(
        "INSERT INTO t VALUES
             (1, [1.0, NULL, 3.0]), (2, [1.0, NULL, 3.0]), (3, [1.0, 2.0, 3.0]),
             (4, NULL), (5, [0.5, 2.0, 3.0]);",
    )
    .unwrap();

    // NULL elements compare structurally (not three-valued), so two rows with
    // the same NULL in the same slot are equal.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE v = [1.0, NULL, 3.0];"
        ),
        2
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE v = CAST('[1.0, NULL, 3.0]' AS DOUBLE[3]);"
        ),
        2
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE v <> [0.5, 2.0, 3.0];"
        ),
        3
    );
    // A NULL array is unknown, like any SQL NULL.
    assert_eq!(count(&mut db, "SELECT count(*) FROM t WHERE v = v;"), 4);

    // Lexicographic order, a NULL element after every value, NULL array last.
    let ordered = rows(&mut db, "SELECT id FROM t ORDER BY v;");
    let ids: Vec<String> = ordered.iter().map(|r| r[0].to_string()).collect();
    assert_eq!(ids, vec!["5", "3", "1", "2", "4"]);

    // GROUP BY and DISTINCT treat equal arrays as one key.
    let groups = rows(&mut db, "SELECT count(*) FROM t GROUP BY v ORDER BY 1;");
    let counts: Vec<String> = groups.iter().map(|r| r[0].to_string()).collect();
    assert_eq!(counts, vec!["1", "1", "1", "2"]);
    assert_eq!(count(&mut db, "SELECT count(DISTINCT v) FROM t;"), 3);
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM (SELECT DISTINCT v FROM t) x;"
        ),
        4
    );

    // MIN/MAX use the same order.
    assert_eq!(cell(&mut db, "SELECT min(v) FROM t;"), "[0.5, 2.0, 3.0]");
    assert_eq!(cell(&mut db, "SELECT max(v) FROM t;"), "[1.0, NULL, 3.0]");

    // Element kinds compare by value.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE v = CAST('[1, 2, 3]' AS INTEGER[3]);"
        ),
        1
    );

    // A size mismatch is an error, never a comparison of the shared prefix.
    let sizes = err(
        &mut db,
        "SELECT count(*) FROM t WHERE v = CAST('[1,2]' AS DOUBLE[2]);",
    );
    assert!(sizes.contains("array sizes differ"), "{sizes}");
    let cast_sizes = err(&mut db, "SELECT CAST([1,2] AS DOUBLE[3]) FROM t;");
    assert!(cast_sizes.contains("array size"), "{cast_sizes}");
}

#[test]
fn joins_and_set_operations_key_on_the_whole_array() {
    let mut db = PinkDb::new();
    table(&mut db, "v DOUBLE[2]");
    db.execute("INSERT INTO t VALUES (1, [1.0, 2.0]), (2, [1.0, NULL]);")
        .unwrap();
    db.execute("CREATE TABLE u (id INTEGER, v DOUBLE[2]);")
        .unwrap();
    db.execute("INSERT INTO u VALUES (10, [1.0, 2.0]);")
        .unwrap();
    assert_eq!(
        count(&mut db, "SELECT count(*) FROM t JOIN u ON t.v = u.v;"),
        1
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM (SELECT v FROM t UNION SELECT v FROM u) x;"
        ),
        2
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM (SELECT v FROM t INTERSECT SELECT v FROM u) x;"
        ),
        1
    );
}

// ------------------------------------------------------------- persistence

#[test]
fn save_open_round_trips_both_columns() {
    let dir = scratch("save");
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, v DOUBLE[3], vi INTEGER[2]);")
        .unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, [1.5, 2.5, 3.5], [1, 2]),
                            (2, [1.0, NULL, 3.0], [NULL, -7]), (3, NULL, NULL);",
    )
    .unwrap();
    db.execute("CREATE TABLE e (id INTEGER, v DOUBLE[2]);")
        .unwrap();
    db.save(path).unwrap();

    let mut db = PinkDb::open(path).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT id, v, vi FROM t ORDER BY id;")
            .iter()
            .map(|r| r
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join("|"))
            .collect::<Vec<_>>(),
        vec![
            "1|[1.5, 2.5, 3.5]|[1, 2]",
            "2|[1.0, NULL, 3.0]|[NULL, -7]",
            "3|NULL|NULL",
        ]
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(v) FROM t LIMIT 1;"),
        Some("DOUBLE[3]".into())
    );
    assert_eq!(count(&mut db, "SELECT count(*) FROM e;"), 0);

    // The decoded schema keeps accepting rows, and a second save/open works.
    db.execute("INSERT INTO t VALUES (4, [9.0, 9.0, 9.0], [9, 9]);")
        .unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) FROM t;"), 4);
    db.save(path).unwrap();
    drop(db);
    let mut db = PinkDb::open(path).unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) FROM t;"), 4);
    assert_eq!(
        cell(&mut db, "SELECT v FROM t WHERE id = 2;"),
        "[1.0, NULL, 3.0]"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn wal_replay_and_checkpoint_keep_arrays() {
    let dir = scratch("wal");
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER, v DOUBLE[2]);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, [1.5, NULL]);")
            .unwrap();
        db.checkpoint().unwrap();
        // Not checkpointed: this row only exists in the WAL when the handle is
        // dropped without a flush, which is what replay must reproduce.
        db.execute("INSERT INTO t VALUES (2, [2.5, 3.5]);").unwrap();
    }
    let mut db = PinkDb::open_durable(path).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT id, v FROM t ORDER BY id;")
            .iter()
            .map(|r| r
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join("|"))
            .collect::<Vec<_>>(),
        vec!["1|[1.5, NULL]", "2|[2.5, 3.5]"]
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(v) FROM t LIMIT 1;"),
        Some("DOUBLE[2]".into())
    );
    db.checkpoint().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// A deterministic pseudo-random double: an embedding-like table has no
/// structure for the page encoder to exploit, so the stored size is honest.
fn splitmix(row: usize, k: u32) -> f64 {
    let mut x = (row as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(u64::from(k).wrapping_mul(1442695040888963407))
        .wrapping_add(0x9e3779b97f4a7c15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

#[test]
fn a_vector_table_of_100k_by_768_round_trips() {
    const ROWS: usize = 100_000;
    const DIM: u32 = 768;
    let dir = scratch("vectors");
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();

    let mut db = PinkDb::new();
    db.execute(&format!("CREATE TABLE vec (id INTEGER, e DOUBLE[{DIM}]);"))
        .unwrap();
    let started = Instant::now();
    let mut id = 0;
    while id < ROWS {
        let n = (ROWS - id).min(pinkdb::CHUNK_SIZE);
        let mut ids = pinkdb::Vector::with_capacity(DataType::Integer, n);
        let mut values = pinkdb::Vector::with_capacity(
            DataType::Array {
                elem: ArrayElem::Double,
                len: DIM,
            },
            n,
        );
        for row in 0..n {
            let elements = (0..DIM)
                .map(|k| Value::Double(splitmix(id + row, k)))
                .collect::<Vec<_>>();
            ids.push_value(Value::Integer((id + row) as i32)).unwrap();
            values
                .push_value(Value::Array(std::sync::Arc::new(
                    pinkdb::array::ArrayValue::from_values(ArrayElem::Double, DIM, &elements)
                        .unwrap(),
                )))
                .unwrap();
        }
        db.table_mut_in("main", "vec")
            .unwrap()
            .append_chunk(vec![ids, values])
            .unwrap();
        id += n;
    }
    let inserted = started.elapsed();
    assert_eq!(count(&mut db, "SELECT count(*) FROM vec;"), ROWS as i64);
    // A full scan touches every element.
    assert_eq!(
        cell(&mut db, "SELECT e[1] FROM vec WHERE id = 12345;"),
        format!("{}", splitmix(12345, 0))
    );
    assert_eq!(
        cell(&mut db, "SELECT len(e) FROM vec WHERE id = 99999;"),
        DIM.to_string()
    );
    let scanned = started.elapsed();

    db.save(path).unwrap();
    let bytes = std::fs::metadata(path).unwrap().len();
    println!(
        "{ROWS} x {DIM} DOUBLE rows: insert {inserted:?}, scan {scanned:?}, file {} MiB \
         ({} bytes/row, {} bytes/element)",
        bytes / (1024 * 1024),
        bytes as usize / ROWS,
        bytes as usize / (ROWS * DIM as usize)
    );
    // The values are pseudo-random (an embedding-like table does not compress),
    // so the page must not blow up much past the 8 raw bytes per element.
    assert!(
        (bytes as usize) < ROWS * DIM as usize * 8 + 32 * 1024 * 1024,
        "stored size {bytes} far above raw"
    );

    let mut db = PinkDb::open(path).unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) FROM vec;"), ROWS as i64);
    assert_eq!(
        cell(&mut db, "SELECT len(e) FROM vec WHERE id = 50000;"),
        DIM.to_string()
    );
    std::fs::remove_dir_all(&dir).ok();
}

// ------------------------------------------------------------- duckdb parity

/// DuckDB 2.0.0's answers for the behaviours both engines define. Skipped when
/// the reference binary is absent, like the Parquet interop tests.
#[test]
fn matches_duckdb_where_behaviour_is_defined() {
    let binary = std::env::var_os("DUCKDB_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/duckdb-2.0.0/duckdb"));
    if !binary.is_file() {
        println!("skip: {} is not present (set DUCKDB_BIN)", binary.display());
        return;
    }
    let script = "
        SELECT CAST([1.0, 2.5]::DOUBLE[2] AS VARCHAR);
        SELECT ([1,2,3]::INTEGER[3])[2];
        SELECT ([1,2,3]::INTEGER[3])[0] IS NULL, ([1,2,3]::INTEGER[3])[4] IS NULL;
        SELECT len([1,2,3]::INTEGER[3]), array_length([1,2,3]::INTEGER[3]);
        SELECT [1.0,NULL,3.0]::DOUBLE[3] = [1.0,NULL,3.0]::DOUBLE[3];
        SELECT array_extract([1,2,3]::INTEGER[3], 2);
    ";
    let out = std::process::Command::new(&binary)
        .args(["-no-agent", "-list", "-noheader", "-c", script])
        .output()
        .expect("run duckdb");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let duckdb = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();

    let mut db = PinkDb::new();
    let ours = [
        cell(
            &mut db,
            "SELECT CAST(CAST([1.0, 2.5] AS DOUBLE[2]) AS VARCHAR);",
        ),
        cell(&mut db, "SELECT CAST([1,2,3] AS INTEGER[3])[2];"),
        format!(
            "{}|{}",
            cell(&mut db, "SELECT CAST([1,2,3] AS INTEGER[3])[0] IS NULL;"),
            cell(&mut db, "SELECT CAST([1,2,3] AS INTEGER[3])[4] IS NULL;")
        ),
        format!(
            "{}|{}",
            cell(&mut db, "SELECT len(CAST([1,2,3] AS INTEGER[3]));"),
            cell(&mut db, "SELECT array_length(CAST([1,2,3] AS INTEGER[3]));")
        ),
        cell(
            &mut db,
            "SELECT CAST('[1.0, NULL, 3.0]' AS DOUBLE[3]) = CAST('[1.0, NULL, 3.0]' AS DOUBLE[3]);",
        ),
        cell(
            &mut db,
            "SELECT array_extract(CAST([1,2,3] AS INTEGER[3]), 2);",
        ),
    ];
    assert_eq!(ours.to_vec(), duckdb, "pinkdb vs DuckDB 2.0.0");
}
