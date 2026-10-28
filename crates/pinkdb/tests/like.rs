//! Wave 126c: `LIKE` compiled once per pattern (DuckDB's `LikeMatcher` for
//! `%`-only patterns, an in-place wildcard matcher otherwise). Expected
//! results were read from DuckDB 2.0 over the same grid.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

const STRINGS: [&str; 15] = [
    "",
    "a",
    "aa",
    "abc",
    "abcabc",
    "special requests",
    "xx special yy requests zz",
    "requests special",
    "héllo wörld",
    "日本語テキスト",
    "a%b",
    "a_b",
    "aXb",
    "banana",
    "abab",
];

const PATTERNS: [&str; 28] = [
    "",
    "%",
    "%%",
    "a",
    "a%",
    "%a",
    "%a%",
    "a%a",
    "ab%ab",
    "%special%requests%",
    "abc",
    "%abc",
    "abc%",
    "_",
    "__",
    "_b%",
    "%_",
    "a_b",
    "h_llo%",
    "%ö%",
    "日%",
    "%テキ_ト",
    "b%n%a",
    "%ana",
    "ab%",
    "%ab%ab%",
    "a%%b",
    "\\%",
];

/// Per pattern, one bit per string in order: `1` when `s LIKE pattern`.
const EXPECTED: [&str; 28] = [
    "100000000000000", // ""
    "111111111111111", // "%"
    "111111111111111", // "%%"
    "010000000000000", // "a"
    "011110000011101", // "a%"
    "011000000000010", // "%a"
    "011111110011111", // "%a%"
    "001000000000000", // "a%a"
    "000000000000001", // "ab%ab"
    "000001100000000", // "%special%requests%"
    "000100000000000", // "abc"
    "000110000000000", // "%abc"
    "000110000000000", // "abc%"
    "010000000000000", // "_"
    "001000000000000", // "__"
    "000110000000001", // "_b%"
    "011111111111111", // "%_"
    "000000000011100", // "a_b"
    "000000001000000", // "h_llo%"
    "000000001000000", // "%ö%"
    "000000000100000", // "日%"
    "000000000100000", // "%テキ_ト"
    "000000000000010", // "b%n%a"
    "000000000000010", // "%ana"
    "000110000000001", // "ab%"
    "000010000000001", // "%ab%ab%"
    "000000000011101", // "a%%b"
    "000000000000000", // "\\%"
];

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    let s: Vec<String> = STRINGS
        .iter()
        .enumerate()
        .map(|(k, v)| format!("({k}, {})", quote(v)))
        .collect();
    let p: Vec<String> = PATTERNS
        .iter()
        .enumerate()
        .map(|(j, v)| format!("({j}, {})", quote(v)))
        .collect();
    for sql in [
        "CREATE TABLE s (k INTEGER, v VARCHAR);".to_string(),
        format!("INSERT INTO s VALUES {};", s.join(",")),
        "CREATE TABLE p (j INTEGER, pat VARCHAR);".to_string(),
        format!("INSERT INTO p VALUES {};", p.join(",")),
    ] {
        db.execute(&sql).unwrap();
    }
    db
}

fn bits(rows: &[Vec<Value>]) -> String {
    rows.iter()
        .map(|r| match &r[0] {
            Value::Boolean(true) => '1',
            Value::Boolean(false) => '0',
            v => panic!("LIKE gave {v:?}"),
        })
        .collect()
}

#[test]
fn like_matches_duckdb_over_a_grid() {
    let mut db = store();
    // A pattern column: compiled per row.
    let all = rows(&mut db, "SELECT v LIKE pat FROM p, s ORDER BY j, k;");
    for (j, chunk) in all.chunks(STRINGS.len()).enumerate() {
        assert_eq!(bits(chunk), EXPECTED[j], "pattern column {:?}", PATTERNS[j]);
    }
    for (j, pat) in PATTERNS.iter().enumerate() {
        // A constant pattern: compiled once.
        let got = rows(
            &mut db,
            &format!("SELECT v LIKE {} FROM s ORDER BY k;", quote(pat)),
        );
        assert_eq!(bits(&got), EXPECTED[j], "constant {pat:?}");
        // NOT LIKE is the exact negation for non-NULL inputs.
        let not = rows(
            &mut db,
            &format!("SELECT v NOT LIKE {} FROM s ORDER BY k;", quote(pat)),
        );
        let flipped: String = EXPECTED[j]
            .chars()
            .map(|c| if c == '1' { '0' } else { '1' })
            .collect();
        assert_eq!(bits(&not), flipped, "NOT LIKE {pat:?}");
    }
}

#[test]
fn like_nulls_and_escape() {
    let mut db = PinkDb::new();
    let got = rows(
        &mut db,
        "SELECT NULL LIKE 'a%', 'abc' LIKE NULL, 'abc' NOT LIKE NULL, \
         like_escape('a%c', 'a\\%c', '\\'), like_escape('abc', 'a\\%c', '\\'), \
         like_escape('a_c', 'a\\_c', '\\'), like_escape('abc', 'a\\_c', '\\');",
    );
    let got: Vec<String> = got[0].iter().map(|v| v.to_string()).collect();
    assert_eq!(
        got,
        ["NULL", "NULL", "NULL", "true", "false", "true", "false"]
    );
}

#[test]
fn ascii_segments_in_non_ascii_strings() {
    // The segment search scans for the needle's rarest byte (`b` here) and
    // starts at its offset in the needle, which can fall inside a
    // multi-byte character of the string.
    let mut db = PinkDb::new();
    let got = rows(
        &mut db,
        "SELECT 'éab' LIKE '%ab%', 'ééxab' LIKE '%ab', 'é' LIKE '%ab%';",
    );
    let got: Vec<String> = got[0].iter().map(|v| v.to_string()).collect();
    assert_eq!(got, ["true", "true", "false"]);
}
