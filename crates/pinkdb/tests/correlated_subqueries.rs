//! Correlated subqueries (wave 120).
//!
//! A subquery may read columns of any enclosing block, at any depth. The
//! planner unnest the common shapes into joins (`semi`/`anti`/`mark` for
//! `EXISTS`/`IN`, a grouped left join for scalars — see
//! `docs/wave-120-plan.md`); the rest keep the executor's per-row path. Both
//! are pinned here against DuckDB 2.0, whose answers are also re-measured when
//! `~/.local/bin/duckdb` is present, so a pin cannot drift.
//!
//! The classic COUNT bug of naive decorrelation — `count(*)` over no rows is
//! 0, while a left join NULL-extends the missing group — has its own case
//! (`scalar_count_star`), and so do SQL's three-valued `IN`/`NOT IN` rules.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// The DuckDB reference binary: `HOME/.local/bin/duckdb`, or `PINKDB_DUCKDB`.
fn duckdb_bin() -> String {
    std::env::var("PINKDB_DUCKDB").unwrap_or_else(|_| {
        format!(
            "{}/.local/bin/duckdb",
            std::env::var("HOME").unwrap_or_default()
        )
    })
}

/// `oc` is the outer block, `ic` an inner table with duplicate and NULL keys,
/// `mc` a two-column correlation target, `ec` a table whose `x` holds a NULL,
/// `qc` an ordered set for the `ANY`/`ALL` quantifiers, and `xc`/`xci`/`yc` the
/// correlated `IN` shapes: an operand that is *not* the correlation column
/// (`xc`), a body group that mixes a NULL with values (`xci.oid = 5`), and an
/// operand column that sits before the correlation column (`yc`), and `uc` a
/// foreign-key table whose key holds a NULL.
const SETUP: &str = "CREATE TABLE oc (k INT, v INT); \
     INSERT INTO oc VALUES (1,10),(2,20),(3,NULL),(4,40),(4,40); \
     CREATE TABLE ic (k INT, x INT); \
     INSERT INTO ic VALUES (1,100),(1,200),(2,300),(NULL,400); \
     CREATE TABLE mc (k INT, j INT, x INT); \
     INSERT INTO mc VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40),(3,1,50); \
     CREATE TABLE ec (k INT, x INT); \
     INSERT INTO ec VALUES (1,1),(1,NULL),(2,2); \
     CREATE TABLE qc (x INT); \
     INSERT INTO qc VALUES (5),(15),(25); \
     CREATE TABLE xc (id INT, g INT); \
     INSERT INTO xc VALUES (1,10),(2,20),(3,NULL),(4,40),(5,50); \
     CREATE TABLE xci (oid INT, v INT); \
     INSERT INTO xci VALUES (1,5),(1,7),(2,NULL),(4,40),(5,NULL),(5,50); \
     CREATE TABLE yc (g INT, k INT); \
     INSERT INTO yc VALUES (10,1),(20,2),(NULL,3),(40,4),(50,5); \
     CREATE TABLE dc (k INT, v INT); \
     INSERT INTO dc VALUES (1,10),(2,20),(3,NULL),(4,40); \
     CREATE TABLE di (k INT, j INT, x INT); \
     INSERT INTO di VALUES (1,1,5),(1,2,6),(2,1,NULL),(4,2,40); \
     CREATE TABLE gc (a INT, b INT); \
     INSERT INTO gc VALUES (10,1),(20,2),(30,3),(40,4); \
     CREATE TABLE uc (g INT, w INT); \
     INSERT INTO uc VALUES (1,10),(2,20),(NULL,30);";

fn db() -> PinkDb {
    let mut db = PinkDb::new();
    for stmt in SETUP.split(';') {
        if !stmt.trim().is_empty() {
            db.execute(stmt).unwrap();
        }
    }
    db
}

fn cell(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Bigint(i) => i.to_string(),
        Value::Hugeint(i) => i.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Varchar(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn tabs(db: &mut PinkDb, sql: &str) -> Vec<String> {
    let out = db.execute(sql).unwrap().pop().unwrap();
    let QueryResult::Selected { rows, .. } = out else {
        panic!("expected rows from {sql}, got {out:?}");
    };
    rows.iter()
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
    let bin = duckdb_bin();
    if !std::path::Path::new(&bin).exists() {
        return None;
    }
    let mut child = Command::new(bin)
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

/// `(label, sql, expected rows)`.
const CASES: &[(&str, &str, &[&str])] = &[
    // ---- EXISTS / NOT EXISTS ------------------------------------------------
    (
        "exists_basic",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k) ORDER BY k",
        &["1", "2"],
    ),
    (
        "not_exists_basic",
        "SELECT k FROM oc WHERE NOT EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k) ORDER BY k",
        &["3", "4", "4"],
    ),
    // A NULL on either side is never equal, so the body sees no row.
    (
        "exists_null_outer",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.v) ORDER BY k",
        &[],
    ),
    (
        "exists_null_inner_key",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k AND ic.k = 1) ORDER BY k",
        &["1"],
    ),
    (
        "exists_empty_inner",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE 1=0) ORDER BY k",
        &[],
    ),
    (
        "not_exists_empty_inner",
        "SELECT k FROM oc WHERE NOT EXISTS (SELECT 1 FROM ic WHERE 1=0) ORDER BY k",
        &["1", "2", "3", "4", "4"],
    ),
    // ---- IN / NOT IN: three-valued logic ------------------------------------
    (
        "in_basic",
        "SELECT k FROM oc WHERE k IN (SELECT k FROM ic) ORDER BY k",
        &["1", "2"],
    ),
    (
        "not_in_basic",
        "SELECT k FROM oc WHERE k NOT IN (SELECT k FROM ec) ORDER BY k",
        &["3", "4", "4"],
    ),
    // A NULL in the inner column makes every non-match NULL, not TRUE.
    (
        "not_in_null_inner",
        "SELECT k FROM oc WHERE k NOT IN (SELECT x FROM ec) ORDER BY k",
        &[],
    ),
    // A NULL left operand makes the result NULL for every row.
    (
        "in_null_lhs",
        "SELECT v FROM oc WHERE v IN (SELECT x FROM ec) ORDER BY v",
        &[],
    ),
    // An empty inner makes IN FALSE and NOT IN TRUE.
    (
        "in_empty",
        "SELECT k FROM oc WHERE k IN (SELECT k FROM ic WHERE k > 100) ORDER BY k",
        &[],
    ),
    (
        "not_in_empty",
        "SELECT k FROM oc WHERE k NOT IN (SELECT k FROM ic WHERE k > 100) ORDER BY k",
        &["1", "2", "3", "4", "4"],
    ),
    // A correlated IN whose operand is a *different* column than the one that
    // drives the correlation: the mark join's value key must stay in body
    // space while the correlation keys move (reviewer-reported shape).
    (
        "in_operand_distinct_from_correlation",
        "SELECT id FROM xc WHERE g NOT IN (SELECT v FROM xci WHERE xci.oid = xc.id) ORDER BY id",
        &["1", "3"],
    ),
    // The empty-group rule: x IN (no rows) is FALSE even when x is NULL, so
    // row 3 (g NULL, no xci rows) survives `NOT IN` and row 2 (20 against a
    // group holding only NULL) does not.
    (
        "in_operand_forms_side_by_side",
        "SELECT id, g IN (SELECT v FROM xci WHERE xci.oid = xc.id), \
                g NOT IN (SELECT v FROM xci WHERE xci.oid = xc.id), \
                g = ANY (SELECT v FROM xci WHERE xci.oid = xc.id), \
                g <> ALL (SELECT v FROM xci WHERE xci.oid = xc.id), \
                g > ANY (SELECT v FROM xci WHERE xci.oid = xc.id) \
         FROM xc ORDER BY id",
        &[
            "1\tfalse\ttrue\tfalse\ttrue\ttrue",
            "2\tNULL\tNULL\tNULL\tNULL\tNULL",
            "3\tfalse\ttrue\tfalse\ttrue\tfalse",
            "4\ttrue\tfalse\ttrue\tfalse\tfalse",
            "5\ttrue\tfalse\ttrue\tfalse\tNULL",
        ],
    ),
    (
        "in_operand_is_the_scalar_not_the_key",
        "SELECT id FROM xc \
         WHERE (SELECT v FROM xci WHERE xci.oid = xc.id AND xci.v > 6) = g ORDER BY id",
        &["4", "5"],
    ),
    (
        "in_operand_before_the_correlation_column",
        "SELECT k FROM yc WHERE g NOT IN (SELECT v FROM xci WHERE xci.oid = yc.k) ORDER BY k",
        &["1", "3"],
    ),
    (
        "in_mark_in_select_and_where",
        "SELECT id, g IN (SELECT v FROM xci WHERE xci.oid = xc.id) \
         FROM xc WHERE g NOT IN (SELECT v FROM xci WHERE xci.oid = xc.id) ORDER BY id",
        &["1\tfalse", "3\tfalse"],
    ),
    (
        "any_in_where",
        "SELECT id FROM xc WHERE g = ANY (SELECT v FROM xci WHERE xci.oid = xc.id) ORDER BY id",
        &["4", "5"],
    ),
    (
        "all_in_where",
        "SELECT id FROM xc WHERE g <> ALL (SELECT v FROM xci WHERE xci.oid = xc.id) ORDER BY id",
        &["1", "3"],
    ),
    (
        "ordered_any_in_where",
        "SELECT id FROM xc WHERE g > ANY (SELECT v FROM xci WHERE xci.oid = xc.id) ORDER BY id",
        &["1"],
    ),
    // ---- scalar subqueries --------------------------------------------------
    (
        "scalar_max",
        "SELECT k, (SELECT max(x) FROM ic WHERE ic.k = oc.k) AS m FROM oc ORDER BY k, m",
        &["1\t200", "2\t300", "3\tNULL", "4\tNULL", "4\tNULL"],
    ),
    // The COUNT bug: no matching row is 0 matching rows for count, and NULL
    // for every other aggregate.
    (
        "scalar_count_star",
        "SELECT k, (SELECT count(*) FROM ic WHERE ic.k = oc.k) AS c FROM oc ORDER BY k",
        &["1\t2", "2\t1", "3\t0", "4\t0", "4\t0"],
    ),
    (
        "scalar_count_col",
        "SELECT k, (SELECT count(x) FROM ic WHERE ic.k = oc.k) AS c FROM oc ORDER BY k",
        &["1\t2", "2\t1", "3\t0", "4\t0", "4\t0"],
    ),
    (
        "scalar_sum",
        "SELECT k, (SELECT sum(x) FROM ic WHERE ic.k = oc.k) AS s FROM oc ORDER BY k",
        &["1\t300", "2\t300", "3\tNULL", "4\tNULL", "4\tNULL"],
    ),
    (
        "scalar_no_aggregate",
        "SELECT k, (SELECT x FROM ic WHERE ic.k = oc.k AND ic.x = 100) AS s FROM oc ORDER BY k",
        &["1\t100", "2\tNULL", "3\tNULL", "4\tNULL", "4\tNULL"],
    ),
    (
        "scalar_in_where",
        "SELECT k FROM oc WHERE v > (SELECT avg(x) FROM ic WHERE ic.k = oc.k) ORDER BY k",
        &[],
    ),
    (
        "scalar_arithmetic",
        "SELECT k, CAST((SELECT 0.2 * avg(x) FROM ic WHERE ic.k = oc.k) AS INT) AS a FROM oc ORDER BY k",
        &["1\t30", "2\t60", "3\tNULL", "4\tNULL", "4\tNULL"],
    ),
    // ---- multi-column correlation -------------------------------------------
    (
        "multi_column_exists",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM mc WHERE mc.k = oc.k AND mc.j = oc.k) ORDER BY k",
        &["1", "2"],
    ),
    (
        "multi_column_scalar",
        "SELECT k, j, (SELECT max(x) FROM mc WHERE mc.k = oc.k AND mc.j = oc.k) AS m \
         FROM oc, (SELECT 1 AS j) t ORDER BY k, j",
        &[
            "1\t1\t10",
            "2\t1\t40",
            "3\t1\tNULL",
            "4\t1\tNULL",
            "4\t1\tNULL",
        ],
    ),
    // ---- correlation at depth and in derived tables -------------------------
    (
        "two_levels_deep",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k \
         AND EXISTS (SELECT 1 FROM mc WHERE mc.k = oc.k)) ORDER BY k",
        &["1", "2"],
    ),
    (
        "derived_table_in_body",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM (SELECT ic.k AS ik FROM ic) z \
         WHERE z.ik = oc.k) ORDER BY k",
        &["1", "2"],
    ),
    (
        "inside_case",
        "SELECT k, CASE WHEN EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k) THEN 1 ELSE 0 END AS e \
         FROM oc ORDER BY k",
        &["1\t1", "2\t1", "3\t0", "4\t0", "4\t0"],
    ),
    (
        "inside_case_scalar",
        "SELECT k, CASE WHEN (SELECT max(x) FROM ic WHERE ic.k = oc.k) > 150 THEN 'a' ELSE 'b' END AS e \
         FROM oc ORDER BY k",
        &["1\ta", "2\ta", "3\tb", "4\tb", "4\tb"],
    ),
    // ---- non-equality correlation -------------------------------------------
    (
        "non_equality_exists",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.x > oc.v) ORDER BY k",
        &["1", "2", "4", "4"],
    ),
    (
        "between_correlation",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k BETWEEN oc.k AND oc.k + 1) \
         ORDER BY k",
        &["1", "2"],
    ),
    // ---- other clauses ------------------------------------------------------
    (
        "having_exists",
        "SELECT k, count(*) AS c FROM oc GROUP BY k \
         HAVING count(*) > (SELECT 1) AND EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k) ORDER BY k",
        &[],
    ),
    (
        "having_scalar",
        "SELECT k FROM oc GROUP BY k \
         HAVING count(*) >= (SELECT count(*) FROM ic WHERE ic.k = oc.k) ORDER BY k",
        &["2", "3", "4"],
    ),
    (
        "on_clause",
        "SELECT oc.k FROM oc JOIN ic ON ic.k = oc.k \
         AND EXISTS (SELECT 1 FROM mc WHERE mc.k = oc.k) ORDER BY oc.k",
        &["1", "1", "2"],
    ),
    (
        "select_duplicates",
        "SELECT (SELECT count(*) FROM ic WHERE ic.k = oc.k) AS c FROM oc ORDER BY c",
        &["0", "0", "0", "1", "2"],
    ),
    (
        "joined_outer_block",
        "SELECT oc.k, ic.x FROM oc, ic WHERE ic.k = oc.k \
         AND EXISTS (SELECT 1 FROM ec WHERE ec.k = oc.k) ORDER BY oc.k, ic.x",
        &["1\t100", "1\t200", "2\t300"],
    ),
    (
        "exists_and_scalar",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k) \
         AND v > (SELECT min(x) FROM ic WHERE ic.k = oc.k) ORDER BY k",
        &[],
    ),
    (
        "any_less",
        "SELECT k, v < ANY (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	false", "4	false"],
    ),
    (
        "any_less_eq",
        "SELECT k, v <= ANY (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	false", "4	false"],
    ),
    (
        "any_greater",
        "SELECT k, v > ANY (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	true", "4	true"],
    ),
    (
        "any_not_eq",
        "SELECT k, v <> ANY (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	true", "4	true"],
    ),
    (
        "all_less",
        "SELECT k, v < ALL (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	false", "2	false", "3	NULL", "4	false", "4	false"],
    ),
    (
        "all_greater",
        "SELECT k, v > ALL (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	false", "2	false", "3	NULL", "4	true", "4	true"],
    ),
    (
        "all_not_eq",
        "SELECT k, v <> ALL (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	true", "4	true"],
    ),
    (
        "all_less_eq",
        "SELECT k, v <= ALL (SELECT x FROM qc) AS a FROM oc ORDER BY k",
        &["1	false", "2	false", "3	NULL", "4	false", "4	false"],
    ),
    (
        "any_eq",
        "SELECT k, v = ANY (SELECT 10) AS a FROM oc ORDER BY k",
        &["1	true", "2	false", "3	NULL", "4	false", "4	false"],
    ),
    (
        "all_eq",
        "SELECT k, v = ALL (SELECT 10) AS a FROM oc ORDER BY k",
        &["1	true", "2	false", "3	NULL", "4	false", "4	false"],
    ),
    (
        "any_with_null",
        "SELECT k, v > ANY (SELECT x FROM qc UNION ALL SELECT NULL) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	true", "4	true"],
    ),
    (
        "all_with_null",
        "SELECT k, v > ALL (SELECT x FROM qc UNION ALL SELECT NULL) AS a FROM oc ORDER BY k",
        &["1	false", "2	false", "3	NULL", "4	NULL", "4	NULL"],
    ),
    (
        "all_only_null",
        "SELECT k, v >= ALL (SELECT NULL) AS a FROM oc ORDER BY k",
        &["1	NULL", "2	NULL", "3	NULL", "4	NULL", "4	NULL"],
    ),
    (
        "any_empty",
        "SELECT k, v < ANY (SELECT x FROM qc WHERE x < 0) AS a FROM oc ORDER BY k",
        &["1	false", "2	false", "3	false", "4	false", "4	false"],
    ),
    (
        "all_empty",
        "SELECT k, v < ALL (SELECT x FROM qc WHERE x < 0) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	true", "4	true", "4	true"],
    ),
    (
        "any_empty_null_lhs",
        "SELECT k, v < ANY (SELECT x FROM qc WHERE x < 0) AS a FROM oc WHERE v IS NULL ORDER BY k",
        &["3	false"],
    ),
    (
        "all_empty_null_lhs",
        "SELECT k, v < ALL (SELECT x FROM qc WHERE x < 0) AS a FROM oc WHERE v IS NULL ORDER BY k",
        &["3	true"],
    ),
    (
        "any_correlated",
        "SELECT k, v < ANY (SELECT x FROM qc WHERE qc.x > oc.k) AS a FROM oc ORDER BY k",
        &["1	true", "2	true", "3	NULL", "4	false", "4	false"],
    ),
    (
        "all_correlated",
        "SELECT k, v < ALL (SELECT x FROM qc WHERE qc.x > oc.k) AS a FROM oc ORDER BY k",
        &["1	false", "2	false", "3	NULL", "4	false", "4	false"],
    ),
    (
        "any_in_where",
        "SELECT k FROM oc WHERE v < ANY (SELECT x FROM qc) ORDER BY k",
        &["1", "2"],
    ),
    (
        "all_in_where",
        "SELECT k FROM oc WHERE v < ALL (SELECT x FROM qc) ORDER BY k",
        &[],
    ),
    (
        "any_of_inner_block",
        "SELECT x FROM ic WHERE x > ANY (SELECT v FROM oc WHERE oc.k = ic.k) ORDER BY x",
        &["100", "200", "300"],
    ),
    // ---- grouped blocks -----------------------------------------------------
    // A correlated aggregate in the select list of a grouped block joins the
    // aggregate's *output*, so the key positions have to be rebased; a group
    // with no body rows keeps NULL (`sum`) or the empty-group value (`count`).
    (
        "grouped_scalar_in_select",
        "SELECT dc.k, (SELECT sum(di.x) FROM di WHERE di.k = dc.k) AS s \
         FROM dc GROUP BY dc.k ORDER BY dc.k",
        &["1\t11", "2\tNULL", "3\tNULL", "4\t40"],
    ),
    (
        "grouped_scalar_in_having",
        "SELECT dc.k, count(*) AS n FROM dc GROUP BY dc.k \
         HAVING count(*) <= (SELECT count(*) FROM di WHERE di.k = dc.k) ORDER BY dc.k",
        &["1\t1", "2\t1", "4\t1"],
    ),
    // The correlation column is not the first output column of the aggregate.
    (
        "grouped_key_not_first_column",
        "SELECT gc.b, (SELECT max(di.x) FROM di WHERE di.k = gc.b) AS m, count(*) AS n \
         FROM gc GROUP BY gc.b ORDER BY gc.b",
        &["1\t6\t1", "2\tNULL\t1", "3\tNULL\t1", "4\t40\t1"],
    ),
    // ---- derived tables inside the body --------------------------------------
    // The derived table reads the enclosing row, so the body's census has to
    // include references made one block in.
    (
        "derived_table_reads_outer_row",
        "SELECT k FROM dc WHERE EXISTS (SELECT 1 FROM \
         (SELECT di.k AS dk FROM di WHERE di.j = dc.k) z WHERE z.dk = dc.k) ORDER BY k",
        &["1"],
    ),
    (
        "derived_table_aggregated_in_body",
        "SELECT k, (SELECT count(*) FROM (SELECT di.x AS y FROM di WHERE di.j = dc.k) z) AS c \
         FROM dc ORDER BY k",
        &["1\t2", "2\t2", "3\t0", "4\t0"],
    ),
    // ---- the staging foreign-key check (wave 140) ---------------------------
    // A derived table that is a UNION used to run once per outer row: filter
    // pushdown clones the correlated conjunct into both branches, below their
    // projections, and the lift has to carry it back up over the set operation.
    (
        "union_not_exists",
        "SELECT k FROM oc WHERE NOT EXISTS \
         (SELECT 1 FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = oc.k) ORDER BY k",
        &["4", "4"],
    ),
    (
        "union_exists",
        "SELECT k FROM oc WHERE EXISTS \
         (SELECT 1 FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = oc.k) ORDER BY k",
        &["1", "2", "3"],
    ),
    (
        "union_in",
        "SELECT k FROM oc WHERE k IN \
         (SELECT k FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = oc.k) ORDER BY k",
        &["1", "2", "3"],
    ),
    (
        "union_scalar_count",
        "SELECT k, (SELECT count(*) FROM (SELECT k FROM ic UNION ALL SELECT k FROM mc) p \
         WHERE p.k = oc.k) AS c FROM oc ORDER BY k",
        &["1\t4", "2\t3", "3\t1", "4\t0", "4\t0"],
    ),
    (
        "union_multi_column",
        "SELECT k, j FROM mc WHERE EXISTS (SELECT 1 FROM \
         (SELECT k, j FROM mc WHERE x > 15 UNION SELECT k, j FROM di WHERE x > 10) p \
         WHERE p.k = mc.k AND p.j = mc.j) ORDER BY k, j",
        &["1\t2", "2\t1", "2\t2", "3\t1"],
    ),
    (
        "union_multi_column_not_exists",
        "SELECT k, j FROM mc WHERE NOT EXISTS (SELECT 1 FROM \
         (SELECT k, j FROM mc WHERE x > 15 UNION SELECT k, j FROM di WHERE x > 10) p \
         WHERE p.k = mc.k AND p.j = mc.j) ORDER BY k, j",
        &["1\t1"],
    ),
    // The derived table renames one column and computes the other, so the
    // conjunct arrives below the projection as `k` / `x + 1` and has to be
    // rewritten over the projection's outputs.
    (
        "derived_renamed_column",
        "SELECT k FROM dc WHERE EXISTS (SELECT 1 FROM \
         (SELECT di.k AS dk, di.x + 1 AS y FROM di) z WHERE z.dk = dc.k) ORDER BY k",
        &["1", "2", "4"],
    ),
    (
        "derived_computed_column",
        "SELECT k FROM dc WHERE EXISTS (SELECT 1 FROM \
         (SELECT di.k AS dk, di.x + 1 AS y FROM di) z WHERE z.y = dc.v) ORDER BY k",
        &[],
    ),
    (
        "derived_renamed_and_computed",
        "SELECT k FROM dc WHERE EXISTS (SELECT 1 FROM \
         (SELECT di.k AS dk, di.x + 1 AS y FROM di) z \
         WHERE z.dk = dc.k AND z.y = dc.v) ORDER BY k",
        &[],
    ),
    (
        "derived_distinct",
        "SELECT k FROM oc WHERE EXISTS \
         (SELECT 1 FROM (SELECT DISTINCT k FROM ic) z WHERE z.k = oc.k) ORDER BY k",
        &["1", "2"],
    ),
    // Grouped derived table: the correlation on the group key rides out on the
    // key, the one on the aggregate's output on the projection above it.
    (
        "derived_grouped_on_key",
        "SELECT k, (SELECT sum(s) FROM (SELECT k, sum(x) AS s FROM ic GROUP BY k) g \
         WHERE g.k = oc.k) AS s FROM oc ORDER BY k",
        &["1\t300", "2\t300", "3\tNULL", "4\tNULL", "4\tNULL"],
    ),
    (
        "derived_grouped_on_aggregate_output",
        "SELECT k, (SELECT sum(s) FROM (SELECT k, sum(x) AS s FROM ic GROUP BY k) g \
         WHERE g.s = oc.v) AS s FROM oc ORDER BY k",
        &["1\tNULL", "2\tNULL", "3\tNULL", "4\tNULL", "4\tNULL"],
    ),
    // A NULL key on both sides: the inner set holds a NULL and the outer row
    // holds one too, and neither ever equals anything.
    (
        "union_null_keys_not_exists",
        "SELECT g FROM uc WHERE NOT EXISTS \
         (SELECT 1 FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = uc.g) ORDER BY g",
        &["NULL"],
    ),
    (
        "union_null_keys_not_in",
        "SELECT g FROM uc WHERE g NOT IN \
         (SELECT k FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = uc.g) ORDER BY g",
        &["NULL"],
    ),
    // ---- shapes the lift cannot reach, still answered per row --------------
    (
        "correlation_under_limit",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k LIMIT 1) ORDER BY k",
        &["1", "2"],
    ),
    (
        "correlation_under_except",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM \
         (SELECT k FROM mc WHERE mc.k = oc.k EXCEPT SELECT k FROM ic) p) ORDER BY k",
        &["3"],
    ),
    (
        "correlation_in_window_input",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM (SELECT k, \
         row_number() OVER (ORDER BY k) AS rn FROM ic WHERE ic.x > oc.v) z \
         WHERE z.k = oc.k) ORDER BY k",
        &["1", "2"],
    ),
    (
        "correlation_on_window_output",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM (SELECT k, \
         row_number() OVER (ORDER BY k) AS rn FROM ic) z WHERE z.rn = oc.k) ORDER BY k",
        &["1", "2", "3", "4", "4"],
    ),
    // A `HAVING` correlation lifted out of a grouped derived table names the
    // aggregate's output by `AggRef`, which only the projection that exposes
    // it can turn into a column: a filter between that projection and the
    // aggregate leaves it unaddressable, so the lift must decline.
    (
        "correlation_above_grouped_derived_table",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM (SELECT ic.k FROM ic \
         GROUP BY ic.k HAVING count(*) >= oc.k) z WHERE z.k IS NOT NULL) ORDER BY k",
        &["1", "2"],
    ),
    // A `HAVING` conjunct names the aggregate's output by `AggRef`, which only
    // the node directly above the aggregate can evaluate, so lifting it out of
    // the aggregate would strand it. (The one above rides out on the enclosing
    // `Project` instead.)
    (
        "correlation_in_having",
        "SELECT k, v FROM oc WHERE EXISTS (SELECT 1 FROM ic GROUP BY ic.k \
         HAVING sum(ic.x) > oc.v AND ic.k = oc.k) ORDER BY 1",
        &["1\t10", "2\t20"],
    ),
    // The derived table's projection reads the enclosing row itself, so it
    // cannot be lifted: the base would carry a read of a row that is not there.
    (
        "correlation_in_projection",
        "SELECT k, v FROM oc WHERE EXISTS (SELECT 1 FROM \
         (SELECT mc.k, oc.v AS vv FROM mc) p WHERE p.k = oc.k AND p.vv > 15) ORDER BY 1",
        &["2\t20"],
    ),
];

#[test]
fn correlated_forms_match_duckdb() {
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

/// A scalar subquery returning more than one row is a runtime error, not a
/// silently chosen row.
#[test]
fn scalar_subquery_multiple_rows_errors() {
    let mut db = db();
    let sql = "SELECT k, (SELECT x FROM ic WHERE ic.k = oc.k) FROM oc";
    let err = db.execute(sql).unwrap_err().to_string();
    assert!(err.contains("more than one row"), "unexpected error: {err}");
    // Both engines refuse with the same *kind*. DuckDB places this check in the
    // plan only for some shapes (`a < (subquery)` silently keeps the first
    // row), so the randomized test stays on aggregate scalars.
    let bin = duckdb_bin();
    if Path::new(&bin).exists() {
        let mut child = Command::new(bin)
            .args(["-no-agent", "-noheader", "-list"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        {
            use std::io::Write;
            writeln!(child.stdin.as_mut().unwrap(), "{SETUP}\n{sql};").unwrap();
        }
        let out = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
        .to_lowercase();
        assert!(
            !out.status.success() && text.contains("more than one row"),
            "duckdb disagreed on the error kind: {text}"
        );
    }
}

/// A correlated subquery may only read outer columns the grouping carries:
/// a group has no single value for the others, and DuckDB rejects the shape
/// rather than picking an arbitrary member.
#[test]
fn correlated_outer_column_must_be_grouped() {
    let mut db = db();
    for sql in [
        "SELECT dc.v, count(*) FROM dc GROUP BY dc.k HAVING count(*) < \
         (SELECT sum(di.x) FROM di WHERE di.k = dc.v)",
        "SELECT dc.k, (SELECT max(di.x) FROM di WHERE di.k = dc.v) AS m \
         FROM dc GROUP BY dc.k",
        "SELECT dc.k, count(*) FROM dc GROUP BY dc.k HAVING EXISTS \
         (SELECT 1 FROM di WHERE di.j = dc.v)",
    ] {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(err.contains("GROUP BY"), "{sql}: unexpected error {err}");
    }
    // The group key itself is fine.
    tabs(
        &mut db,
        "SELECT dc.k, count(*) FROM dc GROUP BY dc.k \
         HAVING count(*) <= (SELECT count(*) FROM di WHERE di.k = dc.k)",
    );
}

/// The planner turns the common shapes into joins: `EXISTS` into a semi join,
/// `NOT EXISTS` into an anti join, `IN` into a mark join, and a correlated
/// scalar aggregate into a grouped left join. All four keep working after
/// optimization (filter pushdown and the comma-join hash conversion).
#[test]
fn explain_shows_the_decorrelated_plans() {
    let mut db = db();
    let plan = |db: &mut PinkDb, sql: &str| tabs(db, &format!("EXPLAIN {sql}")).join("\n");
    let semi = plan(
        &mut db,
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k)",
    );
    assert!(semi.contains("Join semi"), "{semi}");
    let anti = plan(
        &mut db,
        "SELECT k FROM oc WHERE NOT EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k)",
    );
    assert!(anti.contains("Join anti"), "{anti}");
    let mark = plan(
        &mut db,
        "SELECT k FROM oc WHERE k IN (SELECT x FROM ic WHERE ic.k = oc.k)",
    );
    assert!(mark.contains("Join mark"), "{mark}");
    let left = plan(
        &mut db,
        "SELECT k, (SELECT count(*) FROM ic WHERE ic.k = oc.k) FROM oc",
    );
    assert!(left.contains("Join left"), "{left}");
    assert!(left.contains("Aggregate keys="), "{left}");
}

/// A body whose correlation sits below a derived table's projection, a
/// `DISTINCT`, a grouped aggregate or a `UNION` lifts out of the whole shape
/// (wave 140): the subquery runs once, not once per outer row.
#[test]
fn derived_tables_and_unions_decorrelate() {
    let mut db = db();
    let plan = |db: &mut PinkDb, sql: &str| tabs(db, &format!("EXPLAIN {sql}")).join("\n");
    for (want, sql) in [
        (
            "Join anti",
            "SELECT k FROM oc WHERE NOT EXISTS \
             (SELECT 1 FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = oc.k)",
        ),
        (
            "Join semi",
            "SELECT k FROM oc WHERE EXISTS \
             (SELECT 1 FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = oc.k)",
        ),
        (
            "Join mark",
            "SELECT k FROM oc WHERE k IN \
             (SELECT k FROM (SELECT k FROM ic UNION SELECT k FROM mc) p WHERE p.k = oc.k)",
        ),
        (
            "Join left",
            "SELECT k, (SELECT count(*) FROM (SELECT k FROM ic UNION ALL SELECT k FROM mc) p \
             WHERE p.k = oc.k) FROM oc",
        ),
        (
            "Join semi",
            "SELECT k FROM dc WHERE EXISTS (SELECT 1 FROM \
             (SELECT di.k AS dk, di.x + 1 AS y FROM di) z WHERE z.dk = dc.k AND z.y = dc.v)",
        ),
        (
            "Join semi",
            "SELECT k FROM oc WHERE EXISTS \
             (SELECT 1 FROM (SELECT DISTINCT k FROM ic) z WHERE z.k = oc.k)",
        ),
        (
            "Join left",
            "SELECT k, (SELECT sum(s) FROM (SELECT k, sum(x) AS s FROM ic GROUP BY k) g \
             WHERE g.k = oc.k) FROM oc",
        ),
    ] {
        let text = plan(&mut db, sql);
        assert!(text.contains(want), "{sql}: {text}");
        // The per-row path keeps the subquery as an expression, where its
        // filter reads `EXISTS (subquery)` / `#0 IN (subquery)`.
        assert!(!text.contains("EXISTS (subquery)"), "{sql}: {text}");
        assert!(!text.contains("IN (subquery)"), "{sql}: {text}");
    }
}

/// What the lift refuses — a correlation under a `LIMIT`, inside an `EXCEPT`
/// branch, or in the input of a window — keeps the executor's per-row path.
/// The answers are pinned in [`CASES`].
#[test]
fn shapes_the_lift_cannot_reach_keep_the_per_row_path() {
    let mut db = db();
    for sql in [
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM ic WHERE ic.k = oc.k LIMIT 1)",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM \
         (SELECT k FROM mc WHERE mc.k = oc.k EXCEPT SELECT k FROM ic) p)",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM (SELECT k, \
         row_number() OVER (ORDER BY k) AS rn FROM ic WHERE ic.x > oc.v) z \
         WHERE z.k = oc.k)",
        "SELECT k FROM oc WHERE EXISTS (SELECT 1 FROM (SELECT ic.k FROM ic \
         GROUP BY ic.k HAVING count(*) >= oc.k) z WHERE z.k IS NOT NULL)",
        "SELECT k, v FROM oc WHERE EXISTS (SELECT 1 FROM ic GROUP BY ic.k \
         HAVING sum(ic.x) > oc.v AND ic.k = oc.k)",
        "SELECT k, v FROM oc WHERE EXISTS (SELECT 1 FROM \
         (SELECT mc.k, oc.v AS vv FROM mc) p WHERE p.k = oc.k AND p.vv > 15)",
    ] {
        let text = tabs(&mut db, &format!("EXPLAIN {sql}")).join("\n");
        assert!(text.contains("EXISTS (subquery)"), "{sql}: {text}");
    }
}

/// A correlated subquery in `UPDATE … WHERE` / `DELETE … WHERE` decides each
/// row against that row, so it must log and replay as the same statement the
/// WAL already carries — the per-row path is what executes during replay.
#[test]
fn correlated_dml_replays_from_the_wal() {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_corr_dml_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    let file: PathBuf = dir.join("db.pink");
    let path = file.to_str().unwrap().to_string();
    let open = || {
        PinkDb::open_durable_with(&path, SyncPolicy::Batch)
            .unwrap()
            .0
    };

    let mut db = open();
    let run = |db: &mut PinkDb, sql: &str| {
        db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    };
    run(
        &mut db,
        "CREATE TABLE oc (k INT, v INT); INSERT INTO oc VALUES (1,10),(2,20),(3,30),(4,40);",
    );
    run(
        &mut db,
        "CREATE TABLE ic (k INT, x INT); INSERT INTO ic VALUES (1,100),(2,200),(2,300);",
    );
    // EXISTS decides the row by a body that reads it.
    run(
        &mut db,
        "UPDATE ic SET x = x + 1 WHERE EXISTS (SELECT 1 FROM oc WHERE oc.k = ic.k);",
    );
    // A correlated set in DELETE: each outer row probes its own set.
    run(
        &mut db,
        "DELETE FROM oc WHERE k IN (SELECT ic.k FROM ic WHERE ic.x > oc.v);",
    );
    // THE COUNT BUG: no `ic` row for k means count(*) = 0, so `v + 0`.
    run(
        &mut db,
        "UPDATE oc SET v = v + (SELECT CAST(count(*) AS INT) FROM ic WHERE ic.k = oc.k);",
    );
    // A missing group is NULL, not 0, for every other aggregate.
    run(
        &mut db,
        "UPDATE oc SET v = v + (SELECT max(x) FROM ic WHERE ic.k = oc.k) WHERE k = 3;",
    );

    let rows = |db: &mut PinkDb| tabs(db, "SELECT k, v FROM oc ORDER BY k");
    let ic_rows = |db: &mut PinkDb| tabs(db, "SELECT k, x FROM ic ORDER BY k, x");
    let before = (rows(&mut db), ic_rows(&mut db));
    assert_eq!(before.0, vec!["3\tNULL", "4\t40"]);
    assert_eq!(before.1, vec!["1\t101", "2\t201", "2\t301"]);

    let wal = PathBuf::from(format!("{path}.wal"));
    let bytes = std::fs::read(wal).unwrap_or_default();
    let find = |needle: &str| bytes.windows(needle.len()).any(|w| w == needle.as_bytes());
    assert!(
        find("CAST(count(*) AS INT) FROM ic WHERE ic.k = oc.k"),
        "the correlated statement itself must be logged"
    );
    drop(db);

    // Crash: no checkpoint, so both statements replay in WAL order.
    let mut db = open();
    assert_eq!((rows(&mut db), ic_rows(&mut db)), before, "replay drifted");
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
