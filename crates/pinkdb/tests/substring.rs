//! Wave 126b: `substr`/`substring` with DuckDB's semantics. Expected values
//! were read from DuckDB 2.0 (the reference CLI) over the same grid.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn one(db: &mut PinkDb, sql: &str) -> String {
    rows(db, sql)[0][0].to_string()
}

const STRINGS: [&str; 4] = ["hello", "héllo wörld", "", "日本語"];
const OFFSETS: [i64; 9] = [-7, -3, -1, 0, 1, 2, 4, 6, 12];
const LENGTHS: [i64; 7] = [-4, -1, 0, 1, 2, 3, 20];

/// Per string, in DuckDB's `ORDER BY o, l NULLS FIRST` order: for each
/// offset, `substr(s, o)` and then `substr(s, o, l)` for each length.
const EXPECTED: [&str; 4] = [
    "hello||||||h|hello|llo|he|e||l|ll|llo|llo|o|hell|l||o|o|o|o|hello|||||h|he|hello|hello||||h|he|hel|hello|ello|h|h||e|el|ell|ello|lo|hel|l||l|lo|lo|lo||ello|o|||||||||||||",
    "o wörld|héll|l||o|o |o w|o wörld|rld|o wö|ö||r|rl|rld|rld|d|wörl|l||d|d|d|d|héllo wörld|||||h|hé|héllo wörld|héllo wörld||||h|hé|hél|héllo wörld|éllo wörld|h|h||é|él|éll|éllo wörld|lo wörld|hél|l||l|lo|lo |lo wörld| wörld|éllo|o|| | w| wö| wörld||örld|d|||||",
    "|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||",
    "日本語|||||||日本語|日本語||||日|日本|日本語|日本語|語|日本|本||語|語|語|語|日本語|||||日|日本|日本語|日本語||||日|日本|日本語|日本語|本語|日|日||本|本語|本語|本語||日本語|語|||||||本語||||||||||||||",
];

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    let strs: Vec<String> = STRINGS
        .iter()
        .enumerate()
        .map(|(k, s)| format!("({}, '{s}')", k + 1))
        .collect();
    let offs: Vec<String> = OFFSETS.iter().map(|o| format!("({o})")).collect();
    let lens: Vec<String> = LENGTHS.iter().map(|l| format!("({l})")).collect();
    for sql in [
        "CREATE TABLE strs (k INTEGER, s VARCHAR);".to_string(),
        format!("INSERT INTO strs VALUES {};", strs.join(",")),
        "CREATE TABLE offs (o BIGINT);".to_string(),
        format!("INSERT INTO offs VALUES {};", offs.join(",")),
        "CREATE TABLE lens (l INTEGER);".to_string(),
        format!("INSERT INTO lens VALUES {};", lens.join(",")),
    ] {
        db.execute(&sql).unwrap();
    }
    db
}

#[test]
fn substr_matches_duckdb_over_a_grid() {
    let mut db = store();
    // Column arguments: the vector kernel, row by row.
    let two = rows(
        &mut db,
        "SELECT substr(s, o) FROM strs, offs ORDER BY k, o;",
    );
    let three = rows(
        &mut db,
        "SELECT substr(s, o, l) FROM strs, offs, lens ORDER BY k, o, l;",
    );
    for (k, expected) in EXPECTED.iter().enumerate() {
        let mut got = Vec::new();
        for i in 0..OFFSETS.len() {
            got.push(two[k * OFFSETS.len() + i][0].to_string());
            for j in 0..LENGTHS.len() {
                let r = (k * OFFSETS.len() + i) * LENGTHS.len() + j;
                got.push(three[r][0].to_string());
            }
        }
        assert_eq!(got.join("|"), *expected, "string {:?}", STRINGS[k]);
    }
    // Literal arguments (the row path) agree with the kernel.
    for (k, s) in STRINGS.iter().enumerate() {
        let expected: Vec<&str> = EXPECTED[k].split('|').collect();
        for (i, o) in OFFSETS.iter().enumerate() {
            for (j, l) in LENGTHS.iter().enumerate() {
                let got = one(&mut db, &format!("SELECT substr('{s}', {o}, {l});"));
                let want = expected[i * (LENGTHS.len() + 1) + 1 + j];
                assert_eq!(got, want, "substr({s:?}, {o}, {l})");
            }
        }
    }
}

#[test]
fn substring_keyword_forms_and_alias() {
    let mut db = PinkDb::new();
    let got = rows(
        &mut db,
        "SELECT substring('hello' FROM 2 FOR 3), substring('hello' FROM 2), \
         SUBSTRING('hello' FOR 2), substring('hello', 2, 2), substr(NULL, 1), substr('hello', NULL);",
    );
    let got: Vec<String> = got[0].iter().map(|v| v.to_string()).collect();
    assert_eq!(got, ["ell", "ello", "he", "el", "NULL", "NULL"]);
    // `substring` stays usable as a column name.
    db.execute("CREATE TABLE t (substring VARCHAR);").unwrap();
    db.execute("INSERT INTO t VALUES ('x');").unwrap();
    assert_eq!(one(&mut db, "SELECT substring FROM t;"), "x");
}

#[test]
fn substr_range_and_type_errors_match_duckdb() {
    let mut db = PinkDb::new();
    let e = db
        .execute("SELECT substr('hello', 5000000000);")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("Substring offset outside of supported range (> 4294967295)"),
        "{e}"
    );
    let e = db
        .execute("SELECT substr('hello', 1, -5000000000);")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("Substring length outside of supported range (< -4294967296)"),
        "{e}"
    );
    // DuckDB has no substring(VARCHAR, DOUBLE): a fractional start is an error.
    let e = db
        .execute("SELECT substr('hello', 1.5);")
        .unwrap_err()
        .to_string();
    assert!(e.contains("must be integers"), "{e}");
}
