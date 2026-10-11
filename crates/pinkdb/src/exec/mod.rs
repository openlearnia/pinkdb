//! Operators + statement execution.
//!
//! Each operator consumes `Vec<DataChunk>` and produces `Vec<DataChunk>`;
//! all computation is vector-at-a-time over ≤ [`CHUNK_SIZE`]-row chunks.
//! Sort / Distinct / Aggregate are blocking (they see all input); the rest
//! stream chunk-by-chunk. (A Volcano `next()` pull interface can replace
//! these `run_*` fns later without touching expression or storage code.)

pub mod aggregate;
pub mod expr;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use aggregate::{aggregate, encode_row};
use expr::{coerce_to, eval_expr, select_true};

use crate::plan::{LogicalPlan, agg_output_schema, compare_values};
use crate::sql::binder::{BoundExpr, common_numeric};
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};

/// Result of one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Created,
    Inserted { count: usize },
    Selected { names: Vec<String>, types: Vec<DataType>, rows: Vec<Vec<Value>> },
}

#[derive(Clone)]
pub(crate) struct Output {
    chunks: Vec<DataChunk>,
    names: Vec<String>,
    types: Vec<DataType>,
}

/// Per-statement execution context: the database, visible CTE outputs
/// (CTEs shadow catalog tables), and memoised subquery results keyed by
/// plan-node address (the tree is immutable during execution, so each
/// uncorrelated subquery evaluates once even when probed per chunk).
pub(crate) struct Cx<'a> {
    pub(crate) db: &'a PinkDb,
    pub(crate) ctes: HashMap<String, Output>,
    pub(crate) memo: HashMap<usize, Memo>,
}

#[derive(Clone)]
pub(crate) enum Memo {
    Scalar(Value),
    Set(HashSet<Vec<u8>>, bool, Option<DataType>),
    Flag(bool),
}

impl<'a> Cx<'a> {
    pub(crate) fn new(db: &'a PinkDb) -> Self {
        Self { db, ctes: HashMap::new(), memo: HashMap::new() }
    }
}

/// Execute a bound plan.
pub fn run(db: &mut PinkDb, plan: &LogicalPlan) -> Result<QueryResult> {
    match plan {
        LogicalPlan::CreateTable { name, schema } => {
            db.create_table(name, schema.clone())?;
            Ok(QueryResult::Created)
        }
        LogicalPlan::Insert { table, rows } => {
            // Evaluate constant cells against an empty single-row chunk, then
            // append (storage coerces INTEGER → BIGINT → DOUBLE).
            let single = DataChunk { columns: Vec::new(), num_rows: 1 };
            let mut staged = Vec::with_capacity(rows.len());
            {
                let mut cx = Cx::new(db);
                for row in rows {
                    let values: Vec<Value> = row
                        .iter()
                        .map(|e| {
                            let v = eval_expr(e, &single, &mut cx)?;
                            if v.len() != 1 {
                                return Err(PinkDbError::Execution(
                                    "INSERT cell did not evaluate to one value".to_string(),
                                ));
                            }
                            Ok(v.get(0))
                        })
                        .collect::<Result<_>>()?;
                    staged.push(values);
                }
            }
            let count = staged.len();
            // Validate every row before appending any: multi-row INSERT is atomic.
            let schema = db.table(table)?.schema();
            for values in &staged {
                if values.len() != schema.len() {
                    return Err(PinkDbError::Execution(format!(
                        "row has {} values but table \"{table}\" has {} columns",
                        values.len(),
                        schema.len()
                    )));
                }
                for (v, (_, t)) in values.iter().zip(schema.iter()) {
                    DataTable::coerce(v.clone(), *t)?;
                }
            }
            let t = db.table_mut(table)?;
            for values in staged {
                t.append_row(values)?;
            }
            Ok(QueryResult::Inserted { count })
        }
        _ => {
            let mut cx = Cx::new(db);
            let out = eval(&mut cx, plan)?;
            let mut rows = Vec::new();
            for chunk in &out.chunks {
                for i in 0..chunk.num_rows() {
                    rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
                }
            }
            Ok(QueryResult::Selected { names: out.names, types: out.types, rows })
        }
    }
}

fn eval(cx: &mut Cx, plan: &LogicalPlan) -> Result<Output> {
    match plan {
        LogicalPlan::CreateTable { .. } | LogicalPlan::Insert { .. } => Err(
            PinkDbError::Execution("DDL/DML does not produce rows".to_string()),
        ),
        LogicalPlan::Scan { table } => {
            // CTEs shadow catalog tables (both keyed lowercased).
            if let Some(out) = cx.ctes.get(&table.to_ascii_lowercase()) {
                return Ok(out.clone());
            }
            let t = cx.db.table(table)?;
            let mut chunks = Vec::with_capacity(t.num_chunks());
            for i in 0..t.num_chunks() {
                if let Some(c) = t.scan_chunk(i) {
                    chunks.push(c);
                }
            }
            let (names, types) = t.schema().into_iter().unzip();
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::SingleRow => Ok(Output {
            // One row, zero columns: literals evaluate to 1-row vectors.
            chunks: vec![DataChunk { columns: Vec::new(), num_rows: 1 }],
            names: vec![],
            types: vec![],
        }),
        LogicalPlan::Filter { input, predicate } => {
            let mut inner = eval(cx, input)?;
            let mut chunks = Vec::with_capacity(inner.chunks.len());
            for chunk in inner.chunks.drain(..) {
                let pred = eval_expr(predicate, &chunk, cx)?;
                let sel = select_true(&pred)?;
                if sel.len() == chunk.num_rows() {
                    chunks.push(chunk);
                } else if !sel.is_empty() {
                    chunks.push(chunk.gather(&sel));
                }
                // Empty selection → drop the chunk.
            }
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
        LogicalPlan::Join { left, right, left_key, right_key, .. } => {
            let l = eval(cx, left)?;
            let r = eval(cx, right)?;
            let mut names = l.names.clone();
            names.extend(r.names.iter().cloned());
            let mut types = l.types.clone();
            types.extend(r.types.iter().copied());
            let chunks = hash_join(cx, &l.chunks, &r.chunks, left_key, right_key, &l.types, &r.types)?;
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            let inner = eval(cx, input)?;
            let chunks = aggregate(cx, &inner.chunks, keys, aggs)?;
            let (names, types) = agg_output_schema(keys, aggs);
            Ok(Output { chunks, names, types })
        }
        LogicalPlan::Cte { name, query, body } => {
            let out = eval(cx, query)?;
            let old = cx.ctes.insert(name.clone(), out);
            let body_out = eval(cx, body);
            // Restore any shadowed entry (nested same-name CTEs).
            match old {
                Some(o) => {
                    cx.ctes.insert(name.clone(), o);
                }
                None => {
                    cx.ctes.remove(name);
                }
            }
            body_out
        }
        LogicalPlan::Project { input, exprs, names } => {
            let mut inner = eval(cx, input)?;
            let types: Vec<DataType> = exprs.iter().map(|e| e.dtype()).collect();
            // Project over zero input chunks → zero output chunks (0 rows).
            // (Aggregates always emit ≥1 chunk, so COUNT(*) over ∅ still works.)
            let mut chunks = Vec::with_capacity(inner.chunks.len());
            for chunk in inner.chunks.drain(..) {
                let cols = exprs
                    .iter()
                    .map(|e| eval_expr(e, &chunk, cx))
                    .collect::<Result<Vec<_>>>()?;
                chunks.push(DataChunk::new(cols)?);
            }
            Ok(Output { chunks, names: names.clone(), types })
        }
        LogicalPlan::Distinct { input } => {
            let inner = eval(cx, input)?;
            let chunks = distinct(&inner.chunks)?;
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
        LogicalPlan::Sort { input, keys } => {
            let inner = eval(cx, input)?;
            let chunks = sort(cx, &inner.chunks, keys)?;
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
        LogicalPlan::Limit { input, limit } => {
            let mut inner = eval(cx, input)?;
            let mut chunks = Vec::new();
            let mut left = *limit;
            for chunk in inner.chunks.drain(..) {
                if left == 0 {
                    break;
                }
                if chunk.num_rows() <= left {
                    left -= chunk.num_rows();
                    chunks.push(chunk);
                } else {
                    let sel: Vec<usize> = (0..left).collect();
                    chunks.push(chunk.gather(&sel));
                    left = 0;
                }
            }
            Ok(Output { chunks, names: inner.names, types: inner.types })
        }
    }
}

// ---------- blocking operators ----------

/// Inner hash join: build on the right, probe with the left (preserves
/// left-row order). NULL keys never match on either side.
fn hash_join(
    cx: &mut Cx,
    left_chunks: &[DataChunk],
    right_chunks: &[DataChunk],
    left_key: &BoundExpr,
    right_key: &BoundExpr,
    left_types: &[DataType],
    right_types: &[DataType],
) -> Result<Vec<DataChunk>> {
    // Build: encoding → right row locations. Cross-type numeric keys are
    // canonicalized to their common type first (binder allows `1 = 1.0`).
    let common = common_numeric(left_key.dtype(), right_key.dtype()).ok();
    let mut map: HashMap<Vec<u8>, Vec<(usize, usize)>> = HashMap::new();
    let mut buf = Vec::new();
    for (ci, chunk) in right_chunks.iter().enumerate() {
        let kv = eval_expr(right_key, chunk, cx)?;
        let kv = match common {
            Some(t) => coerce_to(&kv, t)?,
            None => kv,
        };
        for i in 0..chunk.num_rows() {
            if kv.is_null(i) {
                continue;
            }
            buf.clear();
            encode_row(&mut buf, &[kv.get(i)]);
            map.entry(buf.clone()).or_default().push((ci, i));
        }
    }

    // Probe.
    let mut dtypes: Vec<DataType> = left_types.to_vec();
    dtypes.extend(right_types.iter().copied());
    let mut finished = Vec::new();
    let mut cur: Vec<Vector> = dtypes.iter().map(|t| Vector::new_empty(*t)).collect();
    let mut cur_rows = 0usize;
    for chunk in left_chunks.iter() {
        let kv = eval_expr(left_key, chunk, cx)?;
        let kv = match common {
            Some(t) => coerce_to(&kv, t)?,
            None => kv,
        };
        for i in 0..chunk.num_rows() {
            if kv.is_null(i) {
                continue;
            }
            buf.clear();
            encode_row(&mut buf, &[kv.get(i)]);
            if let Some(matches) = map.get(&buf) {
                for &(rci, ri) in matches {
                    if cur_rows >= CHUNK_SIZE {
                        finished.push(DataChunk::new(std::mem::take(&mut cur))?);
                        cur = dtypes.iter().map(|t| Vector::new_empty(*t)).collect();
                        cur_rows = 0;
                    }
                    let rchunk = &right_chunks[rci];
                    for (dst, col) in cur.iter_mut().zip(chunk.columns.iter()) {
                        dst.push_value(col.get(i))?;
                    }
                    for (dst, col) in cur.iter_mut().skip(chunk.columns.len()).zip(rchunk.columns.iter()) {
                        dst.push_value(col.get(ri))?;
                    }
                    cur_rows += 1;
                }
            }
        }
    }
    if cur_rows > 0 {
        finished.push(DataChunk::new(cur)?);
    }
    Ok(finished)
}

fn distinct(chunks: &[DataChunk]) -> Result<Vec<DataChunk>> {    let mut seen = HashSet::new();
    let mut out: Vec<Vector> = Vec::new();
    let mut out_rows = 0usize;
    let mut finished: Vec<DataChunk> = Vec::new();
    let mut buf = Vec::new();
    for chunk in chunks.iter() {
        for i in 0..chunk.num_rows() {
            buf.clear();
            encode_row(&mut buf, &chunk.columns.iter().map(|c| c.get(i)).collect::<Vec<_>>());
            if !seen.insert(buf.clone()) {
                continue;
            }
            if out.is_empty() {
                out = chunk.columns.iter().map(|c| Vector::new_empty(c.dtype())).collect();
            }
            if out_rows >= CHUNK_SIZE {
                finished.push(DataChunk::new(std::mem::take(&mut out))?);
                out = chunk.columns.iter().map(|c| Vector::new_empty(c.dtype())).collect();
                out_rows = 0;
            }
            for (col, src) in out.iter_mut().zip(chunk.columns.iter()) {
                col.push_value(src.get(i))?;
            }
            out_rows += 1;
        }
    }
    if !out.is_empty() {
        finished.push(DataChunk::new(out)?);
    }
    Ok(finished)
}

fn sort(cx: &mut Cx, chunks: &[DataChunk], keys: &[(BoundExpr, bool)]) -> Result<Vec<DataChunk>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    // Evaluate key vectors per chunk, then sort row locations.
    struct Row {
        key: Vec<Value>,
        chunk: usize,
        row: usize,
    }
    let mut rows: Vec<Row> = Vec::new();
    for (ci, chunk) in chunks.iter().enumerate() {
        let key_vecs: Vec<Vector> = keys
            .iter()
            .map(|(k, _)| eval_expr(k, chunk, cx))
            .collect::<Result<_>>()?;
        for i in 0..chunk.num_rows() {
            rows.push(Row {
                key: key_vecs.iter().map(|v| v.get(i)).collect(),
                chunk: ci,
                row: i,
            });
        }
    }
    rows.sort_by(|a, b| {
        for ((ka, kb), (_, desc)) in a.key.iter().zip(b.key.iter()).zip(keys.iter()) {
            // NULLS LAST regardless of direction: only non-null values flip.
            let ord = match (ka, kb) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                _ => {
                    let c = compare_values(ka, kb);
                    if *desc { c.reverse() } else { c }
                }
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    // Rebuild columns in sorted order.
    let dtypes: Vec<DataType> = chunks[0].columns.iter().map(|c| c.dtype()).collect();
    let mut finished = Vec::new();
    let mut cur: Vec<Vector> = dtypes.iter().map(|t| Vector::new_empty(*t)).collect();
    let mut cur_rows = 0;
    for r in rows {
        if cur_rows >= CHUNK_SIZE {
            finished.push(DataChunk::new(std::mem::take(&mut cur))?);
            cur = dtypes.iter().map(|t| Vector::new_empty(*t)).collect();
            cur_rows = 0;
        }
        let src = &chunks[r.chunk];
        for (dst, col) in cur.iter_mut().zip(src.columns.iter()) {
            dst.push_value(col.get(r.row))?;
        }
        cur_rows += 1;
    }
    if cur_rows > 0 {
        finished.push(DataChunk::new(cur)?);
    }
    Ok(finished)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value as V;

    fn sales_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(
            &mut db,
            "sales",
            "id,region,total\n1,west,10.5\n2,east,7.0\n3,west,3.25\n4,east,\n",
        )
        .unwrap();
        db
    }

    fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<V>> {
        let mut last = None;
        for r in db.execute(sql).unwrap() {
            if let QueryResult::Selected { rows, .. } = r {
                last = Some(rows);
            }
        }
        last.unwrap()
    }

    #[test]
    fn filter_project_limit() {
        let mut db = sales_db();
        let rows = select(&mut db, "SELECT id FROM sales WHERE total > 5 LIMIT 10;");
        assert_eq!(rows, vec![vec![V::Integer(1)], vec![V::Integer(2)]]);
    }

    #[test]
    fn null_fails_where() {
        let mut db = sales_db();
        // id=4 has NULL total: `total > 0` is NULL → filtered; IS NULL keeps it.
        let rows = select(&mut db, "SELECT id FROM sales WHERE total > 0;");
        assert_eq!(rows.len(), 3);
        let rows = select(&mut db, "SELECT id FROM sales WHERE total IS NULL;");
        assert_eq!(rows, vec![vec![V::Integer(4)]]);
    }

    #[test]
    fn group_avg_having_order() {
        let mut db = sales_db();
        let rows = select(
            &mut db,
            "SELECT region, AVG(total) AS a FROM sales GROUP BY region HAVING COUNT(*) > 1 ORDER BY 2 DESC;",
        );
        assert_eq!(rows.len(), 2);
        // east avg = 7.0 (NULL skipped), west avg = (10.5+3.25)/2 = 6.875.
        assert_eq!(rows[0][0], V::Varchar("east".to_string()));
        assert!(matches!(rows[0][1], V::Double(_)));
        match rows[0][1] {
            V::Double(x) => assert!((x - 7.0).abs() < 1e-9),
            _ => panic!(),
        }
        match rows[1][1] {
            V::Double(x) => assert!((x - 6.875).abs() < 1e-9),
            _ => panic!(),
        }
    }

    #[test]
    fn count_star_over_empty() {
        let mut db = sales_db();
        let rows = select(&mut db, "SELECT COUNT(*) FROM sales WHERE false;");
        assert_eq!(rows, vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn distinct_and_order_nulls_last() {        let mut db = sales_db();
        let rows = select(&mut db, "SELECT DISTINCT region FROM sales ORDER BY region;");
        assert_eq!(
            rows,
            vec![vec![V::Varchar("east".to_string())], vec![V::Varchar("west".to_string())]]
        );
        let rows = select(&mut db, "SELECT total FROM sales ORDER BY total;");
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
        // NULLS LAST holds for DESC too.
        let rows = select(&mut db, "SELECT total FROM sales ORDER BY total DESC;");
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
        assert!(matches!(rows[0][0], V::Double(_)));
    }

    #[test]
    fn scalar_functions_and_coercion() {
        let mut db = sales_db();
        let rows = select(&mut db, "SELECT 1 / 2, upper(region), length(region) FROM sales LIMIT 1;");
        assert_eq!(rows[0][0], V::Double(0.5));
        assert_eq!(rows[0][1], V::Varchar("WEST".to_string()));
        assert_eq!(rows[0][2], V::Integer(4));
    }

    #[test]
    fn create_insert_select_roundtrip() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a BIGINT, b VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'yy');").unwrap();
        let rows = select(&mut db, "SELECT a, b FROM t ORDER BY a DESC;");
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(2), V::Varchar("yy".to_string())],
                vec![V::Bigint(1), V::Varchar("x".to_string())],
            ]
        );
    }

    #[test]
    fn select_without_from_returns_one_row() {
        let mut db = PinkDb::new();
        let rows = select(&mut db, "SELECT 1 + 2, upper('ab');");
        assert_eq!(
            rows,
            vec![vec![V::Integer(3), V::Varchar("AB".to_string())]]
        );
    }

    #[test]
    fn div_by_zero_errors() {
        let mut db = sales_db();
        assert!(db.execute("SELECT id / 0 FROM sales;").is_err());
    }

    // --- wave-4 review regressions ---

    #[test]
    fn constant_fragments_in_agg_queries() {
        // C1: non-literal constants must not panic the key rebasing.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
        assert_eq!(select(&mut db, "SELECT SUM(x) + (1+1) FROM t;"), vec![vec![V::Bigint(5)]]);
        assert_eq!(
            select(&mut db, "SELECT 1+1, SUM(x) FROM t;"),
            vec![vec![V::Integer(2), V::Bigint(3)]]
        );
        assert_eq!(select(&mut db, "SELECT SUM(x) + abs(1) FROM t;"), vec![vec![V::Bigint(4)]]);
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM t HAVING SUM(x) > 1+1;"), vec![vec![V::Bigint(3)]]);
    }

    #[test]
    fn alias_self_reference_resolves_to_column() {
        // C2: `SELECT x AS x` must not overflow the stack.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(select(&mut db, "SELECT x AS x FROM t;"), vec![vec![V::Integer(1)]]);
    }

    #[test]
    fn where_ignores_select_aliases() {
        // M1: WHERE binds table columns, never select aliases.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE u (a INTEGER, b INTEGER);").unwrap();
        db.execute("INSERT INTO u VALUES (1, 100), (2, 0);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT a AS b FROM u WHERE b > 50;"),
            vec![vec![V::Integer(1)]]
        );
    }

    #[test]
    fn sum_of_nothing_is_null() {
        // C3: SUM over zero/all-NULL rows is NULL, not 0.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE e (x INTEGER);").unwrap();
        db.execute("INSERT INTO e VALUES (NULL), (NULL);").unwrap();
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e WHERE false;"), vec![vec![V::Null]]);
        assert_eq!(select(&mut db, "SELECT SUM(x + 1) FROM e;"), vec![vec![V::Null]]);
        // Non-empty sums still work.
        assert_eq!(select(&mut db, "SELECT SUM(x) FROM e WHERE x IS NULL;"), vec![vec![V::Null]]);
    }

    #[test]
    fn cross_type_join_matches() {
        // C4: INTEGER = BIGINT and INTEGER = DOUBLE join keys must match.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a (x INTEGER);").unwrap();
        db.execute("CREATE TABLE b (x BIGINT);").unwrap();
        db.execute("CREATE TABLE c (x DOUBLE);").unwrap();
        db.execute("INSERT INTO a VALUES (1), (2);").unwrap();
        db.execute("INSERT INTO b VALUES (1), (3);").unwrap();
        db.execute("INSERT INTO c VALUES (1.0), (4.0);").unwrap();
        assert_eq!(
            select(&mut db, "SELECT * FROM a JOIN b ON a.x = b.x;"),
            vec![vec![V::Integer(1), V::Bigint(1)]]
        );
        assert_eq!(
            select(&mut db, "SELECT * FROM a JOIN c ON a.x = c.x;"),
            vec![vec![V::Integer(1), V::Double(1.0)]]
        );
    }

    #[test]
    fn multirow_insert_is_atomic() {
        // M3: a bad row anywhere aborts the whole INSERT.
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE p (a INTEGER);").unwrap();
        db.execute("INSERT INTO p VALUES (1);").unwrap();
        assert!(db.execute("INSERT INTO p VALUES (2), ('bad');").is_err());
        assert_eq!(select(&mut db, "SELECT * FROM p;"), vec![vec![V::Integer(1)]]);
    }

    // --- phase 7: dates + timestamps ---

    fn date_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE ev (id INTEGER, d DATE, ts TIMESTAMP);").unwrap();
        db.execute(
            "INSERT INTO ev VALUES
             (1, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00'),
             (2, DATE '2024-02-20', TIMESTAMP '2024-02-20 23:59:59.5'),
             (3, NULL, NULL);",
        )
        .unwrap();
        db
    }

    #[test]
    fn dates_roundtrip_and_display() {
        let mut db = date_db();
        let rows = select(&mut db, "SELECT d, ts FROM ev ORDER BY id;");
        assert_eq!(rows[0][0].to_string(), "2024-01-15");
        assert_eq!(rows[0][1].to_string(), "2024-01-15 10:00:00");
        assert_eq!(rows[1][1].to_string(), "2024-02-20 23:59:59.500000");
        assert_eq!(rows[2], vec![V::Null, V::Null]);
    }

    #[test]
    fn date_arithmetic() {
        let mut db = date_db();
        let rows = select(&mut db, "SELECT d + 10, 1 + d, d - 5 FROM ev WHERE id = 1;");
        assert_eq!(rows[0][0].to_string(), "2024-01-25");
        assert_eq!(rows[0][1].to_string(), "2024-01-16");
        assert_eq!(rows[0][2].to_string(), "2024-01-10");
        let rows = select(&mut db, "SELECT DATE '2024-02-20' - DATE '2024-01-15';");
        assert_eq!(rows, vec![vec![V::Bigint(36)]]);
        // NULL propagates.
        let rows = select(&mut db, "SELECT d + 1 FROM ev WHERE id = 3;");
        assert_eq!(rows, vec![vec![V::Null]]);
        // Rejected: timestamp ± int, date ± date (non-minus), mixed compare.
        assert!(db.execute("SELECT ts + 1 FROM ev;").is_err());
        assert!(db.execute("SELECT d + d FROM ev;").is_err());
        assert!(db.execute("SELECT * FROM ev WHERE d = ts;").is_err());
        assert!(db.execute("SELECT * FROM ev WHERE d = '2024-01-15';").is_err());
    }

    #[test]
    fn date_functions() {
        let mut db = date_db();
        let rows = select(
            &mut db,
            "SELECT year(d), month(d), day(d), hour(ts), minute(ts), second(ts) FROM ev WHERE id = 2;",
        );
        assert_eq!(
            rows,
            vec![vec![
                V::Integer(2024),
                V::Integer(2),
                V::Integer(20),
                V::Integer(23),
                V::Integer(59),
                V::Integer(59)
            ]]
        );
        let rows = select(&mut db, "SELECT date_trunc('month', d), date_trunc('day', ts) FROM ev WHERE id = 1;");
        assert_eq!(rows[0][0].to_string(), "2024-01-01");
        assert_eq!(rows[0][1].to_string(), "2024-01-15 00:00:00");
        let rows = select(&mut db, "SELECT date_trunc('year', TIMESTAMP '2024-05-06 07:08:09');");
        assert_eq!(rows[0][0].to_string(), "2024-01-01 00:00:00");
        assert!(db.execute("SELECT hour(d) FROM ev;").is_err());
        assert!(db.execute("SELECT date_trunc('century', d) FROM ev;").is_err());
        assert!(db.execute("SELECT SUM(d) FROM ev;").is_err());
    }

    #[test]
    fn dates_group_order_join_minmax() {
        let mut db = date_db();
        let rows = select(&mut db, "SELECT MIN(d), MAX(d), COUNT(d) FROM ev;");
        assert_eq!(rows[0][0].to_string(), "2024-01-15");
        assert_eq!(rows[0][1].to_string(), "2024-02-20");
        assert_eq!(rows[0][2], V::Bigint(2));
        // date_trunc grouping (analytics workhorse).
        let rows = select(
            &mut db,
            "SELECT date_trunc('month', d) AS m, COUNT(*) FROM ev GROUP BY m ORDER BY m;",
        );
        assert_eq!(rows.len(), 3); // two months + NULL group
        assert_eq!(rows[2][0], V::Null);
        // Ordering + join on dates.
        db.execute("CREATE TABLE h (d DATE, note VARCHAR);").unwrap();
        db.execute("INSERT INTO h VALUES (DATE '2024-01-15', 'x');").unwrap();
        let rows = select(&mut db, "SELECT note FROM ev JOIN h ON ev.d = h.d ORDER BY id;");
        assert_eq!(rows, vec![vec![V::Varchar("x".to_string())]]);
        let rows = select(&mut db, "SELECT DISTINCT d FROM ev ORDER BY d;");
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
    }

    #[test]
    fn csv_inflects_dates() {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(
            &mut db,
            "c",
            "d,ts\n2024-01-15,2024-01-15 10:00:00\n,",
        )
        .unwrap();
        let t = db.table("c").unwrap();
        assert_eq!(
            t.schema(),
            vec![
                ("d".to_string(), DataType::Date),
                ("ts".to_string(), DataType::Timestamp),
            ]
        );
        assert_eq!(t.get(1, 0), Some(Value::Null));
        // Almost-dates stay VARCHAR.
        let mut db2 = PinkDb::new();
        crate::csv::import_csv_string(&mut db2, "c", "d\n2024-13-01\n2024-01-15\n").unwrap();
        assert_eq!(db2.table("c").unwrap().schema()[0].1, DataType::Varchar);
    }

    #[test]
    fn dates_persist() {
        let db = date_db();
        let path = std::env::temp_dir().join("pinkdb_dates.pink");
        db.save(path.to_str().unwrap()).unwrap();
        let back = PinkDb::open(path.to_str().unwrap()).unwrap();
        let (a, b) = (db.table("ev").unwrap(), back.table("ev").unwrap());
        assert_eq!(a.num_rows(), b.num_rows());
        for r in 0..a.num_rows() {
            for c in 0..3 {
                assert_eq!(a.get(r, c), b.get(r, c), "row {r} col {c}");
            }
        }
        std::fs::remove_file(&path).ok();
    }

    // --- phase 6: subqueries + CTEs ---

    fn sub_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "emp", "id,name,did\n1,ann,10\n2,bob,20\n3,cat,").unwrap();
        crate::csv::import_csv_string(&mut db, "dept", "did,dname\n10,eng\n20,ops\n").unwrap();
        db
    }

    #[test]
    fn cte_basic_and_chained() {
        let mut db = sub_db();
        let rows = select(
            &mut db,
            "WITH e AS (SELECT * FROM emp WHERE id > 1) SELECT name FROM e ORDER BY name;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Varchar("bob".to_string())], vec![V::Varchar("cat".to_string())]]
        );
        let rows = select(
            &mut db,
            "WITH e AS (SELECT * FROM emp WHERE id > 1),
                  n AS (SELECT name FROM e WHERE did IS NOT NULL)
             SELECT * FROM n;",
        );
        assert_eq!(rows, vec![vec![V::Varchar("bob".to_string())]]);
    }

    #[test]
    fn cte_with_agg_join_and_alias_cols() {
        let mut db = sub_db();
        let rows = select(
            &mut db,
            "WITH d(did, n) AS (SELECT did, COUNT(*) FROM emp GROUP BY did)
             SELECT did, n FROM d ORDER BY did;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Integer(10), V::Bigint(1)],
                vec![V::Integer(20), V::Bigint(1)],
                vec![V::Null, V::Bigint(1)],
            ]
        );
        let rows = select(
            &mut db,
            "WITH e AS (SELECT * FROM emp) SELECT dname FROM e JOIN dept ON e.did = dept.did ORDER BY 1;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Varchar("eng".to_string())], vec![V::Varchar("ops".to_string())]]
        );
    }

    #[test]
    fn derived_table_needs_alias() {
        let mut db = sub_db();
        assert!(db.execute("SELECT * FROM (SELECT * FROM emp);").is_err());
        let rows = select(&mut db, "SELECT name FROM (SELECT * FROM emp WHERE id = 2) AS s;");
        assert_eq!(rows, vec![vec![V::Varchar("bob".to_string())]]);
    }

    #[test]
    fn scalar_subquery() {
        let mut db = sub_db();
        let rows = select(&mut db, "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE dname = 'ops');");
        assert_eq!(rows, vec![vec![V::Varchar("bob".to_string())]]);
        // Empty → NULL → no rows.
        let rows = select(&mut db, "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE false);");
        assert!(rows.is_empty());
        // Multi-row → error.
        assert!(db.execute("SELECT (SELECT id FROM emp);").is_err());
        // Scalar over empty table in SELECT list.
        let rows = select(&mut db, "SELECT (SELECT COUNT(*) FROM emp WHERE false);");
        assert_eq!(rows, vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn in_subquery_and_list() {
        let mut db = sub_db();
        let rows = select(
            &mut db,
            "SELECT name FROM emp WHERE did IN (SELECT did FROM dept) ORDER BY name;",
        );
        assert_eq!(
            rows,
            vec![vec![V::Varchar("ann".to_string())], vec![V::Varchar("bob".to_string())]]
        );
        let rows = select(
            &mut db,
            "SELECT name FROM emp WHERE did NOT IN (SELECT did FROM dept) ORDER BY name;",
        );
        // cat has NULL did: NOT IN with no NULLs in set → TRUE... set has no
        // NULLs (dept.did all non-null) → cat (NULL did) → NULL → filtered.
        assert!(rows.is_empty());
        // List form.
        let rows = select(&mut db, "SELECT name FROM emp WHERE id IN (1, 3) ORDER BY name;");
        assert_eq!(
            rows,
            vec![vec![V::Varchar("ann".to_string())], vec![V::Varchar("cat".to_string())]]
        );
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE id NOT IN (2);");
        assert_eq!(rows, vec![vec![V::Bigint(2)]]);
        // NULL in list poisons non-matches.
        let rows = select(&mut db, "SELECT name FROM emp WHERE id IN (1, NULL);");
        assert_eq!(rows, vec![vec![V::Varchar("ann".to_string())]]);
    }

    #[test]
    fn uncorrelated_exists() {
        let mut db = sub_db();
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept);");
        assert_eq!(rows, vec![vec![V::Bigint(3)]]);
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE NOT EXISTS (SELECT * FROM dept WHERE false);");
        assert_eq!(rows, vec![vec![V::Bigint(3)]]);
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept WHERE false);");
        assert_eq!(rows, vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn correlated_is_rejected() {
        let mut db = sub_db();
        assert!(db
            .execute("SELECT dname FROM dept WHERE EXISTS (SELECT * FROM emp WHERE emp.did = dept.did);")
            .is_err());
    }

    fn join_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "emp", "id,name,did\n1,ann,10\n2,bob,20\n3,cat,").unwrap();
        crate::csv::import_csv_string(&mut db, "dept", "did,dname\n10,eng\n20,ops\n30,hr\n").unwrap();
        db
    }

    #[test]
    fn inner_join_basic() {
        let mut db = join_db();
        let rows = select(
            &mut db,
            "SELECT name, dname FROM emp JOIN dept ON emp.did = dept.did ORDER BY name;",
        );
        assert_eq!(
            rows,
            vec![
                vec![V::Varchar("ann".to_string()), V::Varchar("eng".to_string())],
                vec![V::Varchar("bob".to_string()), V::Varchar("ops".to_string())],
            ]
        );
    }

    #[test]
    fn join_null_keys_never_match() {
        let mut db = join_db();
        // cat has NULL did: inner join drops her.
        let rows = select(&mut db, "SELECT COUNT(*) FROM emp JOIN dept ON emp.did = dept.did;");
        assert_eq!(rows, vec![vec![V::Bigint(2)]]);
    }

    #[test]
    fn join_with_pushed_filters_and_agg() {
        let mut db = join_db();
        let rows = select(
            &mut db,
            "SELECT dname, COUNT(*) FROM emp JOIN dept ON emp.did = dept.did \
             WHERE emp.id > 0 AND dept.did > 5 GROUP BY dname ORDER BY dname;",
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], V::Varchar("eng".to_string()));
        assert_eq!(rows[0][1], V::Bigint(1));
    }
}
