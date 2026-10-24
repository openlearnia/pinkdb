//! SQL dialect conformance, batch 1 (wave 95a).
//!
//! Every query form this wave added runs over one shared probe corpus and is
//! pinned against DuckDB 2.0.0. `docs/sql-conformance.md` is the full matrix;
//! the cases here are the ones DuckDB and pinkdb answer with the same SQL.
//!
//! [`batch_one_query_forms_match_duckdb`] asserts the pinned rows, and — when
//! `/tmp/duckdb-2.0.0/duckdb` is present — re-measures them on DuckDB so a pin
//! cannot drift. Forms whose *spelling* differs (VARIANT subscripts: DuckDB
//! needs `json(...)` to build a non-string VARIANT) are pinned without the
//! DuckDB re-check and covered by the pinkdb-only tests at the end.

use std::process::{Command, Stdio};

use pinkdb::{PinkDb, QueryResult, Value};

/// The DuckDB reference binary (see the standing rules); tests skip the
/// re-measurement when it is absent.
const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";

const SETUP: &str = "CREATE TABLE t(a INT, b INT, g VARCHAR); \
     INSERT INTO t VALUES (1,10,'p'),(2,20,'p'),(3,30,'q'),(4,NULL,'q'),(5,50,'q');";

/// `(label, sql, expected rows)`. Rows are tab-joined cells, `NULL` spelled
/// out — the shape DuckDB's `-list -separator '\t'` CLI mode prints.
const CASES: &[(&str, &str, &[&str])] = &[
    (
        "qualify_row_number",
        "SELECT a FROM t QUALIFY row_number() OVER (PARTITION BY g ORDER BY a) = 1 ORDER BY a",
        &["1", "3"],
    ),
    (
        "qualify_named_window_alias",
        "SELECT g, sum(a) OVER w AS s FROM t WINDOW w AS (PARTITION BY g) QUALIFY s > 10 ORDER BY g",
        &["q\t12", "q\t12", "q\t12"],
    ),
    (
        "try_cast",
        "SELECT coalesce(CAST(try_cast('x' AS INT) AS VARCHAR), '<null>'), \
         CAST(try_cast('5' AS INT) AS VARCHAR)",
        &["<null>\t5"],
    ),
    (
        "string_subscript",
        "SELECT concat('[', ('hello')[2], ']'), concat('[', ('hello')[2:4], ']'), \
         concat('[', ('hello')[-1], ']'), concat('[', ('hello')[10], ']'), \
         concat('[', ('hello')[2:1], ']')",
        &["[e]\t[ell]\t[o]\t[]\t[]"],
    ),
    (
        "is_distinct_from",
        "SELECT CAST(1 IS DISTINCT FROM NULL AS VARCHAR), \
         CAST(NULL IS NOT DISTINCT FROM NULL AS VARCHAR), \
         CAST(1 IS NOT DISTINCT FROM 2 AS VARCHAR)",
        &["true\ttrue\tfalse"],
    ),
    (
        "values_in_from",
        "SELECT x, y FROM (VALUES (1,'a'),(2,'b')) t(x,y) ORDER BY x",
        &["1\ta", "2\tb"],
    ),
    (
        "values_join_source_aliases",
        "SELECT x, y, z FROM (VALUES (1,'a'),(2,'b')) t(x,y) CROSS JOIN (VALUES (9)) u(z) ORDER BY x",
        &["1\ta\t9", "2\tb\t9"],
    ),
    (
        "values_standalone",
        "VALUES (1,'a'),(2,'b')",
        &["1\ta", "2\tb"],
    ),
    (
        "table_t",
        "TABLE t ORDER BY a",
        &["1\t10\tp", "2\t20\tp", "3\t30\tq", "4\tNULL\tq", "5\t50\tq"],
    ),
    (
        "fetch_first",
        "SELECT a FROM t ORDER BY a OFFSET 1 ROW FETCH FIRST 2 ROWS ONLY",
        &["2", "3"],
    ),
    (
        "like_escape",
        r"SELECT CAST('a%b' LIKE 'a\%b' ESCAPE '\' AS VARCHAR),
          CAST('axb' LIKE 'a\%b' ESCAPE '\' AS VARCHAR),
          CAST('50%' LIKE '50#%' ESCAPE '#' AS VARCHAR)",
        &["true\tfalse\ttrue"],
    ),
    (
        "similar_to",
        "SELECT CAST('abc' SIMILAR TO 'a.c' AS VARCHAR), \
         CAST('abd' SIMILAR TO 'a.c' AS VARCHAR), \
         CAST('cat' SIMILAR TO 'c(at|ow)' AS VARCHAR), \
         CAST('abbb' SIMILAR TO 'ab+' AS VARCHAR), \
         CAST('abc' SIMILAR TO 'a{2}' AS VARCHAR)",
        &["true\tfalse\ttrue\ttrue\tfalse"],
    ),
    (
        "distinct_on",
        "SELECT DISTINCT ON (g) g, a FROM t ORDER BY g, a DESC",
        &["p\t2", "q\t5"],
    ),
    (
        "distinct_on_key_not_selected",
        "SELECT DISTINCT ON (g) a FROM t ORDER BY a",
        &["1", "3"],
    ),
    (
        "aggregate_filter",
        "SELECT g, CAST(sum(a) FILTER (WHERE a > 1) AS VARCHAR), \
         CAST(count(*) FILTER (WHERE b IS NULL) AS VARCHAR) FROM t GROUP BY g ORDER BY g",
        &["p\t2\t0", "q\t12\t1"],
    ),
    (
        "aggregate_filter_distinct",
        "SELECT CAST(count(DISTINCT a) FILTER (WHERE a > 1) AS VARCHAR) FROM t",
        &["4"],
    ),
    (
        "named_window",
        "SELECT a, CAST(sum(a) OVER w AS VARCHAR) FROM t \
         WINDOW w AS (PARTITION BY g ORDER BY a) ORDER BY a",
        &["1\t1", "2\t3", "3\t3", "4\t7", "5\t12"],
    ),
    (
        "rollup_with_grouping",
        "SELECT coalesce(g,'<n>'), coalesce(CAST(b AS VARCHAR),'<n>'), sum(a), \
         CAST(grouping(g) AS VARCHAR), CAST(grouping(b) AS VARCHAR), \
         CAST(grouping(g,b) AS VARCHAR) FROM t GROUP BY ROLLUP(g,b) ORDER BY 1,2,3",
        &[
            "<n>\t<n>\t15\t1\t1\t3",
            "p\t10\t1\t0\t0\t0",
            "p\t20\t2\t0\t0\t0",
            "p\t<n>\t3\t0\t1\t1",
            "q\t30\t3\t0\t0\t0",
            "q\t50\t5\t0\t0\t0",
            "q\t<n>\t4\t0\t0\t0",
            "q\t<n>\t12\t0\t1\t1",
        ],
    ),
    (
        "cube",
        "SELECT coalesce(g,'<n>'), coalesce(CAST(b AS VARCHAR),'<n>'), sum(a) \
         FROM t GROUP BY CUBE(g,b) ORDER BY 1,2,3",
        &[
            "<n>\t10\t1",
            "<n>\t20\t2",
            "<n>\t30\t3",
            "<n>\t50\t5",
            "<n>\t<n>\t4",
            "<n>\t<n>\t15",
            "p\t10\t1",
            "p\t20\t2",
            "p\t<n>\t3",
            "q\t30\t3",
            "q\t50\t5",
            "q\t<n>\t4",
            "q\t<n>\t12",
        ],
    ),
    (
        "grouping_sets",
        "SELECT coalesce(g,'<n>'), coalesce(CAST(b AS VARCHAR),'<n>'), sum(a) \
         FROM t GROUP BY GROUPING SETS ((g),(b),()) ORDER BY 1,2,3",
        &[
            "<n>\t10\t1",
            "<n>\t20\t2",
            "<n>\t30\t3",
            "<n>\t50\t5",
            "<n>\t<n>\t4",
            "<n>\t<n>\t15",
            "p\t<n>\t3",
            "q\t<n>\t12",
        ],
    ),
    (
        "grouping_sets_with_plain_key",
        "SELECT coalesce(g,'<n>'), coalesce(CAST(b AS VARCHAR),'<n>'), sum(a) \
         FROM t GROUP BY g, GROUPING SETS ((b),()) ORDER BY 1,2,3",
        &[
            "p\t10\t1",
            "p\t20\t2",
            "p\t<n>\t3",
            "q\t30\t3",
            "q\t50\t5",
            "q\t<n>\t4",
            "q\t<n>\t12",
        ],
    ),
    (
        "recursive_cte",
        "WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM r WHERE i < 5) \
         SELECT i FROM r ORDER BY i",
        &["1", "2", "3", "4", "5"],
    ),
    (
        "intersect_all",
        "SELECT * FROM (VALUES (1),(1),(2)) a(x) INTERSECT ALL \
         SELECT * FROM (VALUES (1),(1),(1)) b(y) ORDER BY 1",
        &["1", "1"],
    ),
    (
        "except_all",
        "SELECT * FROM (VALUES (1),(1),(2)) a(x) EXCEPT ALL \
         SELECT * FROM (VALUES (1)) b(y) ORDER BY 1",
        &["1", "2"],
    ),
    (
        "union_all",
        "SELECT * FROM (VALUES (1),(1)) a(x) UNION ALL SELECT * FROM (VALUES (2)) b(y) ORDER BY 1",
        &["1", "1", "2"],
    ),
];

fn cell(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Bigint(i) => i.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Varchar(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn db() -> PinkDb {
    let mut db = PinkDb::new();
    for stmt in SETUP.split(';') {
        if !stmt.trim().is_empty() {
            db.execute(stmt).unwrap();
        }
    }
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn tabs(db: &mut PinkDb, sql: &str) -> Vec<String> {
    rows(db, sql)
        .iter()
        .map(|r| {
            r.iter()
                .map(cell)
                .collect::<Vec<_>>()
                .join("\t")
                .trim_end()
                .to_string()
        })
        .collect()
}

/// `Some(rows)` when the DuckDB reference binary exists, else `None`.
fn duck(sql: &str) -> Option<Vec<String>> {
    if !std::path::Path::new(DUCKDB).exists() {
        return None;
    }
    let mut child = Command::new(DUCKDB)
        .args(["-no-agent", "-noheader", "-list", "-separator", "\t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().unwrap();
        writeln!(stdin, "{SETUP}\n{sql};").unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "duckdb refused {sql}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Some(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.trim_end().to_string())
            .collect(),
    )
}

#[test]
fn batch_one_query_forms_match_duckdb() {
    let mut db = db();
    for (label, sql, want) in CASES {
        let got = tabs(&mut db, sql);
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want, "{label}: {sql}");
        if let Some(measured) = duck(sql) {
            assert_eq!(measured, want, "{label}: duckdb drifted from the pin");
        }
    }
}

/// `EXPLAIN` names the operator each new form introduces, so a plan can be
/// read without guessing.
#[test]
fn explain_shows_the_new_operators() {
    let mut db = db();
    let plan = |db: &mut PinkDb, sql: &str| tabs(db, &format!("EXPLAIN {sql}")).join("\n");
    let qualify = plan(
        &mut db,
        "SELECT a FROM t QUALIFY row_number() OVER (PARTITION BY g ORDER BY a) = 1",
    );
    assert!(qualify.contains("Window"), "{qualify}");
    let dist = plan(&mut db, "SELECT DISTINCT ON (g) a FROM t ORDER BY a");
    assert!(dist.contains("Distinct"), "{dist}");
    let vals = plan(&mut db, "SELECT x FROM (VALUES (1),(2)) t(x)");
    assert!(vals.contains("Values"), "{vals}");
    let rec = plan(
        &mut db,
        "WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM r WHERE i < 2) \
         SELECT i FROM r",
    );
    assert!(rec.contains("Cte"), "{rec}");
    assert!(rec.contains("Recursive step (UNION ALL)"), "{rec}");
}

/// Forms the roadmap lists for later waves fail with the wave, not a parse
/// error that reads like a typo.
#[test]
fn later_waves_are_named_in_the_error() {
    let mut db = db();
    let cases = [
        (
            "INSERT INTO t VALUES (1,2,'x') ON CONFLICT DO NOTHING",
            "95b",
        ),
        ("INSERT OR REPLACE INTO t VALUES (1,2,'x')", "95b"),
        ("REPLACE INTO t VALUES (1,2,'x')", "95b"),
        ("UPDATE t SET a = 1 FROM t AS s", "95b"),
        ("DELETE FROM t USING t AS s", "95b"),
        ("INSERT INTO t VALUES (1,2,'x') RETURNING a", "95b"),
        ("MERGE INTO t USING t AS s ON t.a = s.a", "95b"),
        (
            "WITH x AS (INSERT INTO t VALUES (1,2,'x') SELECT 1) SELECT * FROM x",
            "95b",
        ),
        ("SELECT * FROM LATERAL (SELECT 1) s", "95c"),
        ("SELECT * FROM t TABLESAMPLE 10 PERCENT", "95c"),
        ("SELECT a FROM t FOR UPDATE", "95c"),
        (
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) FROM t",
            "95c",
        ),
        (
            "SELECT a FROM t WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY a)",
            "95c",
        ),
        ("SELECT ts AT TIME ZONE 'UTC' FROM t", "95c"),
        ("SELECT (a, b) FROM t", "95c"),
        ("SELECT [1,2,3]", "95c"),
        ("SELECT sum(a) FILTER (WHERE a > 1) OVER () FROM t", "95c"),
        ("SET VARIABLE x = 1", "95c"),
        ("SELECT $x", "95c"),
        ("SELECT json_set('{}', '$.a', 1)", "unknown function"),
    ];
    for (sql, wave) in cases {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(
            err.contains(wave),
            "{sql}: expected {wave} in the error, got {err}"
        );
    }
}

// ------------------------------------------------------- pinkdb-only shapes

/// VARIANT subscripts: field names, 1-based indices, missing keys as NULL, and
/// the nested `v.a['b']` chain. DuckDB spells the same query `json(...)`, and
/// numbers its ARRAY subscripts the same way (1-based, `[0]` an error).
#[test]
fn variant_subscripts_follow_duckdb_indexing() {
    let mut db = db();
    let q = |db: &mut PinkDb, sql: &str| cell(&rows(db, sql)[0][0]);
    assert_eq!(
        q(
            &mut db,
            r#"SELECT CAST(('{"a":1}'::VARIANT)['a'] AS VARCHAR) AS x"#
        ),
        "1"
    );
    assert_eq!(
        q(
            &mut db,
            r#"SELECT CAST(('{"a":[10,20]}'::VARIANT)['a'][2] AS VARCHAR) AS x"#
        ),
        "20"
    );
    assert_eq!(
        q(
            &mut db,
            r#"SELECT CAST(('{"a":{"b":2}}'::VARIANT).a['b'] AS VARCHAR) AS x"#
        ),
        "2"
    );
    // A missing key is SQL NULL, not an error.
    assert_eq!(
        q(
            &mut db,
            r#"SELECT coalesce(CAST(('{"a":1}'::VARIANT)['nope'] AS VARCHAR), '<null>') AS x"#
        ),
        "<null>"
    );
    // 1-based, like DuckDB: index 0 is refused with DuckDB's wording.
    let err = db
        .execute(r#"SELECT ('[10,20]'::VARIANT)[0]"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("1-based"), "{err}");
    // Out of range is NULL.
    assert_eq!(
        q(
            &mut db,
            r#"SELECT coalesce(CAST(('[10]'::VARIANT)[7] AS VARCHAR), '<null>') AS x"#
        ),
        "<null>"
    );
}

#[test]
fn grouping_masks_and_rollup_shapes() {
    let mut db = db();
    // ROLLUP(g,b) visits {g,b}, {g}, {} in that order; masks are big-endian.
    let got = tabs(
        &mut db,
        "SELECT coalesce(g,'<n>'), coalesce(CAST(b AS VARCHAR),'<n>'), \
         CAST(grouping(g,b) AS VARCHAR) FROM t GROUP BY ROLLUP(g,b)",
    );
    assert_eq!(
        got,
        vec![
            "p\t10\t0",
            "p\t20\t0",
            "q\t30\t0",
            "q\t<n>\t0",
            "q\t50\t0",
            "p\t<n>\t1",
            "q\t<n>\t1",
            "<n>\t<n>\t3",
        ]
    );
    // `GROUPING()` with no arguments marks the grand total only.
    let got = tabs(
        &mut db,
        "SELECT CAST(grouping() AS VARCHAR) FROM t GROUP BY GROUPING SETS ((g),()) ORDER BY 1",
    );
    assert_eq!(got, vec!["0", "0", "1"]);
}

#[test]
fn recursive_cte_terminates_and_reports_a_runaway() {
    let mut db = db();
    // `UNION` (not ALL) stops when the step adds nothing new.
    let got = tabs(
        &mut db,
        "WITH RECURSIVE r(i) AS (SELECT 1 UNION SELECT (i % 3) + 1 FROM r) SELECT i FROM r ORDER BY i",
    );
    assert_eq!(got, vec!["1", "2", "3"]);
    // Duplicates the step produces in one round collapse under `UNION`, exactly
    // like rows already produced (DuckDB: 1/1, 2/1, 3/1).
    let got = tabs(
        &mut db,
        "WITH RECURSIVE r(i) AS (SELECT 1 UNION SELECT i + 1 FROM r CROSS JOIN \
         (VALUES (1),(1)) v(x) WHERE i < 3) SELECT i, count(*) FROM r GROUP BY i ORDER BY i",
    );
    assert_eq!(got, vec!["1\t1", "2\t1", "3\t1"]);
    let err = db
        .execute("WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r) SELECT * FROM r")
        .unwrap_err()
        .to_string();
    assert!(err.contains("100000 iterations"), "{err}");
}

#[test]
fn unsupported_variants_have_precise_errors() {
    let mut db = db();
    let cases = [
        ("SELECT 'ab' LIKE 'a%' ESCAPE 'xy'", "one character"),
        (
            "SELECT sum(a) FILTER (WHERE a > 1) OVER (PARTITION BY g) FROM t",
            "wave 95c",
        ),
        ("SELECT a[1] FROM t", "only VARCHAR and VARIANT"),
        ("SELECT DISTINCT ON (a), DISTINCT b FROM t", "parse error"),
        ("SELECT a FROM t LIMIT 1 FETCH FIRST 2 ROWS ONLY", "FETCH"),
        (
            "SELECT ('[1]'::VARIANT)[1:2]",
            "slicing a VARIANT is not supported",
        ),
        ("SELECT ('abc' SIMILAR TO 'a{1,2000}')", "too complex"),
    ];
    for (sql, want) in cases {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(err.contains(want), "{sql}: expected {want:?}, got {err}");
    }
}
