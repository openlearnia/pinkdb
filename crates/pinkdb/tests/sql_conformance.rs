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
    let sample = plan(
        &mut db,
        "SELECT a FROM t USING SAMPLE reservoir(2 ROWS) REPEATABLE(7)",
    );
    assert!(
        sample.contains("Sample reservoir(2 ROWS) REPEATABLE(7)"),
        "{sample}"
    );
    let ordered = plan(
        &mut db,
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) FROM t",
    );
    assert!(
        ordered.contains("percentile_cont(0.5) WITHIN GROUP (ORDER BY #0)"),
        "{ordered}"
    );
    let set = plan(&mut db, "SET VARIABLE x = 41");
    assert!(set.contains("SetVariable x = 41"), "{set}");
}

/// Forms this engine refuses keep a refusal that names what is missing,
/// rather than a parse error that reads like a typo.
#[test]
fn refusals_name_the_missing_feature() {
    let mut db = db();
    let cases = [
        ("SELECT a FROM t FOR UPDATE", "no row locks"),
        (
            "SELECT ts AT TIME ZONE 'Europe/Paris' FROM t",
            "time zone database",
        ),
        ("SELECT (a, b) FROM t", "no STRUCT type"),
        // A fixed-size ARRAY literal parses (wave 107a); the variable-length
        // form still names the follow-up.
        ("CREATE TABLE listy (v DOUBLE[])", "variable-length LIST"),
        ("SELECT $nope", "Variable nope does not exist"),
        (
            "SELECT * FROM t, LATERAL (SELECT t.a) s",
            "LATERAL subquery cannot reference",
        ),
        (
            "SELECT * FROM t, LATERAL foo(1) r",
            "LATERAL table function",
        ),
        (
            "SELECT (1,2) IN (SELECT * FROM (VALUES (1,2)) v(x,y))",
            "row constructor in IN (SELECT",
        ),
        (
            "SELECT json_remove('{\"a\":1}', '$.a', '$.b')",
            "takes exactly 2 arguments",
        ),
        (
            "SELECT percentile_cont(a) WITHIN GROUP (ORDER BY a) FROM t",
            "constant between 0 and 1",
        ),
        (
            "SELECT percentile_cont(0.5) OVER () FROM t",
            "cannot be used as a window function",
        ),
        (
            "SELECT first_value(a) FILTER (WHERE a > 1) OVER () FROM t",
            "FILTER is not implemented",
        ),
        (
            "SELECT a FROM t TABLESAMPLE BERNOULLI(10)",
            "discrete sample count",
        ),
        (
            "SELECT sum(a) OVER (w) FROM t WINDOW w AS (ORDER BY a ROWS 1 PRECEDING)",
            "cannot copy window",
        ),
        (
            "SELECT sum(a) OVER (w ORDER BY a) FROM t WINDOW w AS (ORDER BY a)",
            "Cannot override ORDER BY",
        ),
    ];
    for (sql, want) in cases {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(
            err.contains(want),
            "{sql}: expected {want:?} in the error, got {err}"
        );
    }
}

// ------------------------------------- batch 3: query forms (wave 95c)

/// Forms whose *spelling* is the same in DuckDB, so a pin can be re-measured
/// there. `SET VARIABLE`, `AT TIME ZONE` and the JSON mutators spell
/// differently (DuckDB has a TIMESTAMPTZ and a JSON type) and are pinned in
/// the pinkdb-only tests below.
const CASES_95C: &[(&str, &str, &[&str])] = &[
    (
        "window_filter",
        "SELECT a, CAST(sum(a) FILTER (WHERE a > 1) OVER (ORDER BY a) AS VARCHAR) FROM t ORDER BY a",
        &["1\tNULL", "2\t2", "3\t5", "4\t9", "5\t14"],
    ),
    (
        "window_filter_count_star",
        "SELECT a, CAST(count(*) FILTER (WHERE b IS NULL) OVER () AS VARCHAR) FROM t ORDER BY a",
        &["1\t1", "2\t1", "3\t1", "4\t1", "5\t1"],
    ),
    (
        "row_comparison",
        "SELECT CAST((1,2) = (1,2) AS VARCHAR), CAST((1,NULL) = (1,NULL) AS VARCHAR), \
         CAST((1,2) < (1,3) AS VARCHAR), CAST((1,NULL) > (1,3) AS VARCHAR), \
         CAST(ROW(1,2) = ROW(1,2) AS VARCHAR), CAST((1,NULL) <> (1,2) AS VARCHAR), \
         CAST((NULL,1) < (2,0) AS VARCHAR)",
        &["true\ttrue\ttrue\ttrue\ttrue\ttrue\tfalse"],
    ),
    (
        "row_in_list",
        "SELECT CAST((1,2) IN ((1,2),(3,4)) AS VARCHAR), \
         CAST((1,NULL) IN ((1,2),(3,4)) AS VARCHAR), \
         CAST((1,2) IN ((3,4),(5,6)) AS VARCHAR)",
        &["true\tfalse\tfalse"],
    ),
    (
        "sample_rows",
        "SELECT count(*) FROM t USING SAMPLE 3 ROWS",
        &["3"],
    ),
    (
        "sample_rows_more_than_input",
        "SELECT count(*) FROM t USING SAMPLE 10 ROWS",
        &["5"],
    ),
    (
        "sample_reservoir_rows",
        "SELECT count(*) FROM t TABLESAMPLE reservoir(2 ROWS)",
        &["2"],
    ),
    (
        "sample_bernoulli_all",
        "SELECT count(*) FROM t TABLESAMPLE BERNOULLI(100 PERCENT)",
        &["5"],
    ),
    (
        "sample_bernoulli_none",
        "SELECT count(*) FROM t TABLESAMPLE BERNOULLI(0 PERCENT)",
        &["0"],
    ),
    (
        "sample_system_all",
        "SELECT count(*) FROM t USING SAMPLE system(100 PERCENT)",
        &["5"],
    ),
    (
        "sample_reservoir_percent_all",
        "SELECT count(*) FROM t USING SAMPLE reservoir(100 PERCENT)",
        &["5"],
    ),
    (
        "sample_of_derived_table",
        "SELECT count(*) FROM (SELECT * FROM t) s USING SAMPLE 2 ROWS",
        &["2"],
    ),
    (
        "ordered_set_percentile_cont",
        "SELECT CAST(percentile_cont(0.3) WITHIN GROUP (ORDER BY a) AS VARCHAR), \
         CAST(percentile_cont(0.6) WITHIN GROUP (ORDER BY a) AS VARCHAR), \
         CAST(percentile_cont(0) WITHIN GROUP (ORDER BY a) AS INTEGER), \
         CAST(percentile_cont(1) WITHIN GROUP (ORDER BY a) AS INTEGER) FROM t",
        &["2.2\t3.4\t1\t5"],
    ),
    (
        "ordered_set_percentile_disc",
        "SELECT CAST(percentile_disc(0.5) WITHIN GROUP (ORDER BY a) AS VARCHAR), \
         CAST(percentile_disc(0) WITHIN GROUP (ORDER BY a) AS VARCHAR), \
         CAST(percentile_disc(1) WITHIN GROUP (ORDER BY a) AS VARCHAR) FROM t",
        &["3\t1\t5"],
    ),
    (
        "ordered_set_mode",
        "SELECT CAST(mode() WITHIN GROUP (ORDER BY g) AS VARCHAR), \
         CAST(mode() WITHIN GROUP (ORDER BY a) AS VARCHAR) FROM t",
        &["q\t1"],
    ),
    (
        "ordered_set_filter",
        "SELECT CAST(percentile_cont(0.25) WITHIN GROUP (ORDER BY a) \
         FILTER (WHERE a > 2) AS VARCHAR) FROM t",
        &["3.5"],
    ),
    (
        "lateral_derived",
        "SELECT CAST(sum(d) AS VARCHAR) FROM t, LATERAL (SELECT 5 AS d) s",
        &["25"],
    ),
    (
        "lateral_cross_join",
        "SELECT CAST(count(*) AS VARCHAR) FROM t CROSS JOIN LATERAL (SELECT 5 AS d) s",
        &["5"],
    ),
    (
        "lateral_left_join",
        "SELECT CAST(sum(d) AS VARCHAR) FROM t LEFT JOIN LATERAL (SELECT 7 AS d) s ON true",
        &["35"],
    ),
    (
        "lateral_head_source",
        "SELECT * FROM LATERAL (SELECT 1 AS a) s",
        &["1"],
    ),
    (
        "comma_from_is_cross_join",
        "SELECT CAST(count(*) AS VARCHAR) FROM t, t AS t2",
        &["25"],
    ),
    (
        "comma_from_on_sees_left",
        "SELECT CAST(count(*) AS VARCHAR) FROM (SELECT 1 AS x) a, \
         (SELECT 2 AS y) b JOIN (SELECT 1 AS z) c ON a.x = c.z",
        &["1"],
    ),
    (
        "ordered_set_empty_input",
        "SELECT CAST(percentile_cont(0.5) WITHIN GROUP (ORDER BY a) AS VARCHAR) \
         FROM t WHERE a > 100",
        &["NULL"],
    ),
    (
        "window_on_window_partition",
        "SELECT a, CAST(sum(a) OVER (w ORDER BY a) AS VARCHAR) FROM t \
         WINDOW w AS (PARTITION BY g) ORDER BY a",
        &["1\t1", "2\t3", "3\t3", "4\t7", "5\t12"],
    ),
    (
        "window_on_window_frame",
        "SELECT a, CAST(sum(a) OVER (w ROWS UNBOUNDED PRECEDING) AS VARCHAR) FROM t \
         WINDOW w AS (ORDER BY a) ORDER BY a",
        &["1\t1", "2\t3", "3\t6", "4\t10", "5\t15"],
    ),
    (
        "window_on_window_chain",
        "SELECT a, CAST(sum(a) OVER v AS VARCHAR) FROM t \
         WINDOW w AS (ORDER BY a), v AS (w ROWS 1 PRECEDING) ORDER BY a",
        &["1\t1", "2\t3", "3\t5", "4\t7", "5\t9"],
    ),
];

#[test]
fn batch_three_query_forms_match_duckdb() {
    let mut db = db();
    for (label, sql, want) in CASES_95C {
        let got = tabs(&mut db, sql);
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want, "{label}: {sql}");
        if let Some(measured) = duck(sql) {
            assert_eq!(measured, want, "{label}: duckdb drifted from the pin");
        }
    }
}

/// `REPEATABLE (seed)` makes a sample reproducible; two runs of the same
/// statement return the same rows, and a different seed a different sample
/// (the corpus is wide enough that the chance of a collision is 1 in 100).
#[test]
fn seeded_samples_are_reproducible() {
    let mut db = db();
    let setup = "CREATE TABLE s AS WITH RECURSIVE r(i) AS \
         (SELECT 1 UNION ALL SELECT i+1 FROM r WHERE i < 100) SELECT i FROM r;";
    for stmt in setup.split(';') {
        if !stmt.trim().is_empty() {
            db.execute(stmt).unwrap();
        }
    }
    let sql = |seed: i64| {
        format!("SELECT i FROM s USING SAMPLE reservoir(5 ROWS) REPEATABLE({seed}) ORDER BY i")
    };
    let first = tabs(&mut db, &sql(42));
    assert_eq!(first.len(), 5, "{first:?}");
    assert_eq!(tabs(&mut db, &sql(42)), first, "same seed, same rows");
    assert_ne!(tabs(&mut db, &sql(43)), first, "different seed, other rows");
    // A percent sample is seeded the same way.
    let pct = "SELECT count(*) FROM s USING SAMPLE bernoulli(50 PERCENT) REPEATABLE(1)";
    assert_eq!(tabs(&mut db, pct), tabs(&mut db, pct));
}

/// Forms whose *spelling* differs in DuckDB (no TIMESTAMPTZ, no `SET
/// VARIABLE`, a JSON type instead of JSON text), pinned without the DuckDB
/// re-check; `bench/duckdiff.py` measures the JSON mutators on both engines
/// with the cast spellings both accept.
const CASES_95C_PINKDB_ONLY: &[(&str, &str, &[&str])] = &[
    (
        "json_mutators_on_text",
        "SELECT json_set('{\"a\":1}', '$.b', 2), json_insert('{\"a\":1}', '$.a', 9), \
         json_replace('{\"a\":1}', '$.a', 9), json_remove('{\"a\":1,\"b\":2}', '$.a')",
        &["{\"a\":1,\"b\":2}\t{\"a\":1}\t{\"a\":9}\t{\"b\":2}"],
    ),
    (
        "json_mutator_paths",
        "SELECT json_set('{}', '$.a.b.c', 1), json_set('[1]', '$[5]', 9), \
         json_set('[1]', '$[1]', 9), json_insert('[1,2]', '$[2]', 3), \
         json_set('{\"a\":1}', '$', 9), CAST(json_remove('{\"a\":1}', '$') AS VARCHAR)",
        &["{\"a\":{\"b\":{\"c\":1}}}\t[1]\t[1,9]\t[1,2,3]\t9\tNULL"],
    ),
    (
        "json_mutators_are_strict",
        "SELECT CAST(json_set(NULL, '$.b', 1) AS VARCHAR), \
         CAST(json_set('{\"a\":1}', NULL, 1) AS VARCHAR), \
         CAST(json_set('{\"a\":1}', '$.b', NULL) AS VARCHAR)",
        &["NULL\tNULL\tNULL"],
    ),
    (
        "json_mutators_on_variant",
        "SELECT CAST(json_set('{\"a\":1}'::VARIANT, '$.b', 2) \
                   = '{\"a\":1,\"b\":2}'::VARIANT AS VARCHAR), \
         typeof(json_set('{\"a\":1}'::VARIANT, '$.b', 2)), \
         CAST(json_remove('{\"a\":1,\"b\":2}'::VARIANT, '$.a') AS VARCHAR)",
        &["true\tVARIANT\t{\"b\":2}"],
    ),
    (
        "at_time_zone_utc_is_the_identity",
        "SELECT CAST(TIMESTAMP '2024-01-01 12:00:00' AT TIME ZONE 'UTC' AS VARCHAR), \
         CAST(timezone('GMT', DATE '2024-01-01') AS VARCHAR), \
         CAST(TIMESTAMP '2024-01-01 12:00:00' AT TIME ZONE '+00:00' AS VARCHAR)",
        &["2024-01-01 12:00:00\t2024-01-01 00:00:00\t2024-01-01 12:00:00"],
    ),
];

#[test]
fn batch_three_pinkdb_only_forms() {
    let mut db = db();
    for (label, sql, want) in CASES_95C_PINKDB_ONLY {
        let got = tabs(&mut db, sql);
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want, "{label}: {sql}");
    }
}

/// `SET VARIABLE` stores a session value that `getvariable('name')` and the
/// `$name` shorthand read; the value is spliced at bind time as a literal.
#[test]
fn session_variables_are_visible_to_later_statements() {
    let mut db = db();
    assert_eq!(
        script_rows(
            &mut db,
            "SET VARIABLE x = 41; SELECT getvariable('x') + 1, $x + 1"
        ),
        vec!["42\t42"]
    );
    assert_eq!(
        script_rows(
            &mut db,
            "SET VARIABLE g = 'q'; SELECT count(*) FROM t WHERE g = $g"
        ),
        vec!["3"]
    );
    let err = db.execute("SELECT $nope").unwrap_err().to_string();
    assert!(err.contains("Variable nope does not exist"), "{err}");
    // DuckDB returns NULL for an unset name through `getvariable`; only the
    // `$name` shorthand errors.
    assert_eq!(
        script_rows(&mut db, "SELECT getvariable('nope') IS NULL"),
        vec!["true"]
    );
}

// ---------------------------------------------------- batch 2: DML (wave 95b)

/// A script's statements, split on `;` (the corpus has no `;` inside a
/// literal or a trigger body).
fn script_rows(db: &mut PinkDb, script: &str) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in script.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        match db.execute(stmt) {
            Ok(results) => {
                for result in results {
                    if let QueryResult::Selected { rows, .. } = result {
                        out.extend(
                            rows.iter()
                                .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t")),
                        );
                    }
                }
            }
            Err(e) => out.push(format!("ERROR: {e}")),
        }
    }
    out
}

/// Run `script` on the DuckDB reference (statement output only), or `None`.
fn duck_script(script: &str) -> Option<Vec<String>> {
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
        writeln!(stdin, "{script};").unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "duckdb refused the script: {}",
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

const DML_SETUP: &str = "CREATE TABLE t(a INT PRIMARY KEY, b INT, g VARCHAR); \
     INSERT INTO t VALUES (1,10,'p'),(2,20,'p'),(3,30,'q');";
const DML_SOURCE: &str = "CREATE TABLE s(a INT, b INT); INSERT INTO s VALUES (2,200),(4,40);";

/// `(label, script, every selected row in statement order)`. The scripts are
/// the pinned probe corpus; each is re-measured on DuckDB when it is present.
const DML_CASES: &[(&str, &str, &[&str])] = &[
    (
        "on_conflict_do_nothing",
        "INSERT INTO t VALUES (2,999,'x') ON CONFLICT DO NOTHING; \
         INSERT INTO t VALUES (4,40,'r') ON CONFLICT (a) DO NOTHING; \
         SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t20\tp", "3\t30\tq", "4\t40\tr"],
    ),
    (
        "on_conflict_do_update_excluded",
        "INSERT INTO t VALUES (2,200,'p2') ON CONFLICT (a) \
           DO UPDATE SET b = excluded.b, g = excluded.g; \
         SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t200\tp2", "3\t30\tq"],
    ),
    (
        "on_conflict_do_update_reads_target",
        "INSERT INTO t VALUES (2,20,'zz') ON CONFLICT (a) \
           DO UPDATE SET b = t.b + 1; \
         SELECT a,b FROM t WHERE a = 2",
        &["2\t21"],
    ),
    (
        "on_conflict_do_update_where",
        "INSERT INTO t VALUES (1,1000,'x'),(2,1000,'x') ON CONFLICT (a) \
           DO UPDATE SET b = excluded.b WHERE t.b < 100; \
         SELECT a,b FROM t ORDER BY a",
        &["1\t1000", "2\t1000", "3\t30"],
    ),
    (
        "insert_or_replace",
        "INSERT OR REPLACE INTO t VALUES (2,222,'r'),(5,50,'n'); \
         SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t222\tr", "3\t30\tq", "5\t50\tn"],
    ),
    (
        "insert_or_replace_partial_columns",
        "INSERT OR REPLACE INTO t (a) VALUES (2); SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t20\tp", "3\t30\tq"],
    ),
    (
        "insert_or_ignore",
        "INSERT OR IGNORE INTO t VALUES (2,999,'x'),(6,60,'y'); \
         SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t20\tp", "3\t30\tq", "6\t60\ty"],
    ),
    (
        "returning_insert",
        "INSERT INTO t VALUES (4,40,'r'),(5,50,'z') RETURNING a, b * 2 AS dbl",
        &["4\t80", "5\t100"],
    ),
    (
        "returning_update",
        "UPDATE t SET b = b + 1 WHERE a <= 2 RETURNING a AS k, b",
        &["1\t11", "2\t21"],
    ),
    (
        "returning_delete",
        "DELETE FROM t WHERE a = 3 RETURNING a, b, g",
        &["3\t30\tq"],
    ),
    (
        "merge_matched_and_not_matched",
        "MERGE INTO t USING s ON t.a = s.a \
           WHEN MATCHED THEN UPDATE SET b = s.b \
           WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b, 'm'); \
         SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t200\tp", "3\t30\tq", "4\t40\tm"],
    ),
    (
        "merge_delete_and_by_source",
        "MERGE INTO t USING s ON t.a = s.a \
           WHEN MATCHED AND s.b > 150 THEN DELETE \
           WHEN MATCHED THEN UPDATE SET b = s.b \
           WHEN NOT MATCHED BY SOURCE THEN DELETE \
           WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b, 'm'); \
         SELECT a,b,g FROM t ORDER BY a",
        &["4\t40\tm"],
    ),
    (
        "merge_first_matching_clause_wins",
        "MERGE INTO t USING s ON t.a = s.a \
           WHEN MATCHED AND s.b > 150 THEN UPDATE SET b = -1 \
           WHEN MATCHED THEN UPDATE SET b = s.b; \
         SELECT a,b FROM t ORDER BY a",
        &["1\t10", "2\t-1", "3\t30"],
    ),
    (
        "merge_insert_column_list",
        "MERGE INTO t USING s ON t.a = s.a \
           WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.b); \
         SELECT a,b,g FROM t ORDER BY a",
        &["1\t10\tp", "2\t20\tp", "3\t30\tq", "4\t40\tNULL"],
    ),
    (
        "merge_returning",
        "MERGE INTO t USING s ON t.a = s.a \
           WHEN MATCHED THEN UPDATE SET b = s.b \
           WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b, 'm') RETURNING a, b",
        &["2\t200", "4\t40"],
    ),
    (
        "dml_cte_insert",
        "WITH ins AS (INSERT INTO t VALUES (9,90,'c') RETURNING a) SELECT * FROM ins",
        &["9"],
    ),
    (
        "dml_cte_update",
        "WITH up AS (UPDATE t SET b = b + 1 WHERE a = 1 RETURNING b) SELECT * FROM up",
        &["11"],
    ),
    (
        "dml_cte_delete",
        "WITH del AS (DELETE FROM t WHERE a = 3 RETURNING a) SELECT * FROM del",
        &["3"],
    ),
    (
        "dml_cte_runs_once",
        "WITH ins AS (INSERT INTO t VALUES (7,70,'c') RETURNING a) \
         SELECT (SELECT count(*) FROM ins) AS n, (SELECT count(*) FROM ins) AS m",
        &["1\t1"],
    ),
    (
        "dml_cte_then_select",
        "WITH ins AS (INSERT INTO t VALUES (8,80,'c') RETURNING a) \
         SELECT * FROM t WHERE a >= 8 ORDER BY a",
        &["8\t80\tc"],
    ),
];

/// Source-scan forms: the script also creates `s` (and `dup`).
const DML_SOURCE_CASES: &[(&str, &str, &[&str])] = &[
    (
        "on_conflict_from_select",
        "INSERT INTO t (a, b) SELECT * FROM s ON CONFLICT (a) DO UPDATE SET b = excluded.b; \
         SELECT a,b FROM t ORDER BY a",
        &["1\t10", "2\t200", "3\t30", "4\t40"],
    ),
    (
        "update_from",
        "UPDATE t SET b = s.b FROM s WHERE t.a = s.a; SELECT a,b FROM t ORDER BY a",
        &["1\t10", "2\t200", "3\t30"],
    ),
    (
        "update_from_last_source_wins",
        "CREATE TABLE dup(a INT, b INT); INSERT INTO dup VALUES (2,100),(2,200); \
         UPDATE t SET b = dup.b FROM dup WHERE t.a = dup.a; \
         SELECT a,b FROM t WHERE a = 2",
        &["2\t200"],
    ),
    (
        "update_from_returning",
        "UPDATE t SET b = s.b FROM s WHERE t.a = s.a RETURNING a, b AS nb",
        &["2\t200"],
    ),
    (
        "delete_using",
        "DELETE FROM t USING s WHERE t.a = s.a; SELECT a FROM t ORDER BY a",
        &["1", "3"],
    ),
    (
        "delete_using_returning",
        "DELETE FROM t USING s WHERE t.a = s.a RETURNING a, g",
        &["2\tp"],
    ),
];

/// Batch 2: every DML form runs over the shared probe corpus and matches the
/// pin, and DuckDB re-measures the pin when its binary is present.
#[test]
fn batch_two_dml_forms_match_duckdb() {
    for (label, script, want) in DML_CASES.iter().chain(DML_SOURCE_CASES) {
        let mut db = PinkDb::new();
        db.execute(DML_SETUP).unwrap();
        db.execute(DML_SOURCE).unwrap();
        let got = script_rows(&mut db, script);
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want, "{label}: {script}");
        if let Some(measured) = duck_script(&format!("{DML_SETUP}\n{DML_SOURCE}\n{script}")) {
            assert_eq!(measured, want, "{label}: duckdb drifted from the pin");
        }
    }
}

/// Every wave-95b error keeps its wording, and DuckDB rejects the same
/// statement (the words differ, the refusal does not).
#[test]
fn dml_errors_are_precise() {
    let setup = format!("{DML_SETUP}\n{DML_SOURCE}");
    let cases: &[(&str, &str)] = &[
        ("INSERT INTO t VALUES (2,1,'x')", "constraint"),
        (
            "INSERT INTO t VALUES (2,1,'x') ON CONFLICT (b) DO NOTHING",
            "conflict target",
        ),
        ("INSERT INTO t VALUES (NULL,1,'x')", "NOT NULL"),
        (
            "MERGE INTO t USING (SELECT 2,1 UNION ALL SELECT 2,2) m(a,b) ON t.a = m.a \
             WHEN MATCHED THEN UPDATE SET b = m.b",
            "cannot affect the same target row more than once",
        ),
        (
            "SELECT * FROM (WITH x AS (INSERT INTO t VALUES (1,1,'x') RETURNING a) SELECT * FROM x) q",
            "must be at the top level",
        ),
    ];
    for (sql, want) in cases {
        let mut db = PinkDb::new();
        for stmt in setup.split(';') {
            if !stmt.trim().is_empty() {
                db.execute(stmt).unwrap();
            }
        }
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(err.contains(want), "{sql}: expected {want} in {err}");
        if std::path::Path::new(DUCKDB).exists() {
            let mut child = Command::new(DUCKDB)
                .args(["-no-agent", "-batch"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            {
                use std::io::Write;
                writeln!(child.stdin.as_mut().unwrap(), "{setup}\n{sql};").unwrap();
            }
            let out = child.wait_with_output().unwrap();
            assert!(
                !out.status.success(),
                "{sql}: duckdb accepted a statement pinkdb refuses"
            );
        }
    }
}

/// `RETURNING` reports the final image: a `BEFORE` trigger's rewrite of `NEW`
/// is what comes back (DuckDB has no triggers; this is pinkdb's contract).
#[test]
fn returning_reports_the_post_trigger_row() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER);")
        .unwrap();
    db.execute(
        "CREATE TRIGGER t_before BEFORE INSERT ON t FOR EACH ROW \
         EXECUTE DO $$ BEGIN SET NEW.b = NEW.b * 10; END; $$",
    )
    .unwrap();
    let got = tabs(&mut db, "INSERT INTO t VALUES (1, 5) RETURNING a, b");
    assert_eq!(got, vec!["1\t50".to_string()]);
    let got = tabs(&mut db, "UPDATE t SET b = b + 1 RETURNING a, b");
    assert_eq!(got, vec!["1\t51".to_string()]);
}

/// Wave 95b enforces what a conflict target rests on: a PRIMARY KEY or a
/// UNIQUE constraint. `ON CONFLICT` over an unenforced constraint would be
/// incoherent, so a plain INSERT gets the same check.
#[test]
fn primary_and_unique_keys_are_enforced() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE k (a INTEGER PRIMARY KEY, b INTEGER UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO k VALUES (1, 10), (2, NULL), (3, NULL)")
        .unwrap();

    let err = db.execute("INSERT INTO k VALUES (1, 99)").unwrap_err();
    assert!(err.to_string().contains("constraint"), "{err}");
    let err = db.execute("INSERT INTO k VALUES (9, 10)").unwrap_err();
    assert!(err.to_string().contains("unique constraint"), "{err}");
    let err = db.execute("INSERT INTO k VALUES (NULL, 5)").unwrap_err();
    assert!(err.to_string().contains("NOT NULL"), "{err}");
    // A rejected multi-row statement writes none of its rows.
    assert!(db.execute("INSERT INTO k VALUES (4, 11), (1, 12)").is_err());
    assert_eq!(tabs(&mut db, "SELECT count(*) FROM k"), ["3".to_string()]);
    // UPDATE observes the same keys and leaves the row alone when it refuses.
    let err = db.execute("UPDATE k SET b = 10 WHERE a = 2").unwrap_err();
    assert!(err.to_string().contains("unique constraint"), "{err}");
    assert_eq!(tabs(&mut db, "SELECT b FROM k WHERE a = 2"), ["NULL"]);

    // The conflict forms handle what a plain INSERT rejects.
    db.execute("INSERT INTO k VALUES (1, 77) ON CONFLICT (a) DO UPDATE SET b = 77")
        .unwrap();
    db.execute("INSERT OR IGNORE INTO k VALUES (1, 88)")
        .unwrap();
    db.execute("INSERT OR REPLACE INTO k VALUES (1, 55)")
        .unwrap();
    assert_eq!(
        tabs(&mut db, "SELECT a, b FROM k WHERE a = 1"),
        ["1\t55".to_string()]
    );
}

/// `EXPLAIN` renders a DML CTE's plan, including the conflict action.
#[test]
fn explain_names_the_dml_cte_forms() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE k (a INTEGER PRIMARY KEY, b INTEGER)")
        .unwrap();
    let plan = tabs(
        &mut db,
        "EXPLAIN WITH x AS (INSERT INTO k VALUES (1,1) \
           ON CONFLICT (a) DO UPDATE SET b = 1 RETURNING a) SELECT * FROM x",
    )
    .join("\n");
    assert!(plan.contains("Insert"), "{plan}");
    assert!(plan.contains("on conflict do update"), "{plan}");
    assert!(plan.contains("Cte"), "{plan}");
    // Every DML form can be explained directly.
    let cases = [
        (
            "EXPLAIN INSERT INTO k VALUES (1,2) ON CONFLICT DO NOTHING RETURNING *",
            "on conflict do nothing",
        ),
        (
            "EXPLAIN UPDATE k SET b = 3 FROM (VALUES (1)) s(a) WHERE k.a = s.a",
            "Update",
        ),
        (
            "EXPLAIN DELETE FROM k USING (VALUES (1)) s(a) WHERE k.a = s.a",
            "Delete",
        ),
        (
            "EXPLAIN MERGE INTO k USING (VALUES (1,2)) s(a,b) ON k.a = s.a \
               WHEN MATCHED THEN UPDATE SET b = s.b \
               WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)",
            "Merge",
        ),
    ];
    for (sql, want) in cases {
        let plan = tabs(&mut db, sql).join("\n");
        assert!(plan.contains(want), "{sql}: {plan}");
    }
    // DDL has no plan worth showing and would dodge its own statement gate.
    let err = db
        .execute("EXPLAIN CREATE TABLE nope (a INTEGER)")
        .expect_err("EXPLAIN of DDL");
    assert!(
        err.to_string()
            .contains("EXPLAIN supports a query or a DML statement"),
        "{err}"
    );
}
