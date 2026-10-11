//! Operators + statement execution.
//!
//! Each operator consumes `Vec<DataChunk>` and produces `Vec<DataChunk>`;
//! all computation is vector-at-a-time over ≤ [`CHUNK_SIZE`]-row chunks.
//! Sort / Distinct / Aggregate are blocking (they see all input); the rest
//! stream chunk-by-chunk. (A Volcano `next()` pull interface can replace
//! these `run_*` fns later without touching expression or storage code.)

pub mod aggregate;
pub mod expr;
pub(crate) mod window;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use aggregate::{aggregate, encode_row};
use expr::{coerce_to, eval_expr, select_true};
use window::eval_window;

use crate::plan::{LogicalPlan, agg_output_schema, compare_values, win_output_schema};
use crate::sql::binder::{BoundExpr, common_numeric};
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};

/// Result of one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Created,
    Inserted { count: usize },
    Updated { count: usize },
    Deleted { count: usize },
    Began,
    Committed,
    RolledBack,
    CreatedView { name: String },
    DroppedTable { name: String },
    DroppedView { name: String },
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
        LogicalPlan::Update { table, sets, predicate } => {
            // Phase 1: evaluate predicates + SET exprs over current chunks
            // (subqueries see pre-mutation state). Phase 2: write back.
            // SET exprs evaluate against original values (simultaneous).
            let mut per_chunk: Vec<Vec<(usize, usize, Value)>>;
            let mut count = 0usize;
            {
                let mut cx = Cx::new(db);
                let t = cx.db.table(table)?;
                per_chunk = vec![Vec::new(); t.num_chunks()];
                for (ci, writes) in per_chunk.iter_mut().enumerate() {
                    let chunk = t.scan_chunk(ci).ok_or_else(|| {
                        PinkDbError::Execution("storage changed during UPDATE".to_string())
                    })?;
                    let targets = match predicate {
                        Some(p) => select_true(&eval_expr(p, &chunk, &mut cx)?)?,
                        None => (0..chunk.num_rows()).collect(),
                    };
                    count += targets.len();
                    for (col, expr) in sets {
                        let vals = eval_expr(expr, &chunk, &mut cx)?;
                        for &r in &targets {
                            writes.push((*col, r, vals.get(r)));
                        }
                    }
                }
            }
            let t = db.table_mut(table)?;
            for (ci, writes) in per_chunk.iter().enumerate() {
                t.update_rows(ci, writes)?;
            }
            Ok(QueryResult::Updated { count })
        }
        LogicalPlan::Delete { table, predicate } => {
            // Phase 1: collect per-chunk drop lists. Phase 2: apply from the
            // highest chunk down (emptied chunks are removed, shifting lower
            // indices — descending order keeps them valid).
            let mut drops: Vec<Vec<usize>>;
            {
                let mut cx = Cx::new(db);
                let t = cx.db.table(table)?;
                drops = Vec::with_capacity(t.num_chunks());
                for ci in 0..t.num_chunks() {
                    let chunk = t.scan_chunk(ci).ok_or_else(|| {
                        PinkDbError::Execution("storage changed during DELETE".to_string())
                    })?;
                    drops.push(match predicate {
                        // Predicate TRUE ⇒ row is deleted (NULL ⇒ kept).
                        Some(p) => select_true(&eval_expr(p, &chunk, &mut cx)?)?,
                        None => (0..chunk.num_rows()).collect(),
                    });
                }
            }
            let mut count = 0usize;
            let t = db.table_mut(table)?;
            for ci in (0..drops.len()).rev() {
                count += t.delete_from_chunk(ci, &drops[ci])?;
            }
            Ok(QueryResult::Deleted { count })
        }
        LogicalPlan::Begin => {
            db.begin()?;
            Ok(QueryResult::Began)
        }        LogicalPlan::Commit => {
            db.commit()?;
            Ok(QueryResult::Committed)
        }
        LogicalPlan::Rollback => {
            db.rollback()?;
            Ok(QueryResult::RolledBack)
        }
        LogicalPlan::CreateView { name, cols, query } => {
            // Validated at bind time; store the AST for per-use rebinding.
            db.create_view(name, (**query).clone(), cols.clone())?;
            Ok(QueryResult::CreatedView { name: name.clone() })
        }
        LogicalPlan::DropTable { name } => {
            db.drop_table(name)?;
            Ok(QueryResult::DroppedTable { name: name.clone() })
        }
        LogicalPlan::DropView { name } => {
            db.drop_view(name)?;
            Ok(QueryResult::DroppedView { name: name.clone() })
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
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. } => Err(PinkDbError::Execution(
            "DDL/DML does not produce rows".to_string(),
        )),
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
        LogicalPlan::Window { input, windows, .. } => {
            let inner = eval(cx, input)?;
            let chunks = eval_window(cx, &inner.chunks, windows)?;
            let (names, types) = win_output_schema(windows);
            let mut all_names = inner.names.clone();
            all_names.extend(names);
            let mut all_types = inner.types.clone();
            all_types.extend(types);
            Ok(Output { chunks, names: all_names, types: all_types })
        }
        LogicalPlan::Union { left, right, distinct: is_distinct, types } => {
            let l = eval(cx, left)?;
            let r = eval(cx, right)?;
            // Coerce each side to the common column types, then concatenate.
            // Output names come from the left branch.
            let mut chunks = Vec::with_capacity(l.chunks.len() + r.chunks.len());
            for chunk in l.chunks.iter().chain(r.chunks.iter()) {
                let mut cols = Vec::with_capacity(types.len());
                for (col, t) in chunk.columns.iter().zip(types.iter()) {
                    cols.push(coerce_to(col, *t)?);
                }
                chunks.push(DataChunk::new(cols)?);
            }
            let chunks = if *is_distinct { distinct(&chunks)? } else { chunks };
            Ok(Output { chunks, names: l.names.clone(), types: types.clone() })
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

    // --- phase 8: window functions ---

    fn win_db() -> PinkDb {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(
            &mut db,
            "sales",
            "region,amount\nwest,10\nwest,20\neast,5\neast,5\n",
        )
        .unwrap();
        db
    }

    #[test]
    fn row_number_rank_dense_rank() {
        let mut db = win_db();
        let rows = select(
            &mut db,
            "SELECT region, amount,
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn,
                    RANK() OVER (PARTITION BY region ORDER BY amount) AS r,
                    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS d
             FROM sales ORDER BY region, amount;",
        );
        // east: (5,5) peers → rn 1,2; r 1,1; d 1,1. west: 10,20 → 1,2; 1,2; 1,2.
        assert_eq!(
            rows,
            vec![
                vec![
                    V::Varchar("east".to_string()),
                    V::Integer(5),
                    V::Bigint(1),
                    V::Bigint(1),
                    V::Bigint(1)
                ],
                vec![
                    V::Varchar("east".to_string()),
                    V::Integer(5),
                    V::Bigint(2),
                    V::Bigint(1),
                    V::Bigint(1)
                ],
                vec![
                    V::Varchar("west".to_string()),
                    V::Integer(10),
                    V::Bigint(1),
                    V::Bigint(1),
                    V::Bigint(1)
                ],
                vec![
                    V::Varchar("west".to_string()),
                    V::Integer(20),
                    V::Bigint(2),
                    V::Bigint(2),
                    V::Bigint(2)
                ],
            ]
        );
    }

    #[test]
    fn window_agg_frames() {
        let mut db = win_db();
        // Default cumulative frame with ORDER BY.
        let rows = select(
            &mut db,
            "SELECT amount, SUM(amount) OVER (ORDER BY amount) AS run FROM sales ORDER BY amount;",
        );
        let runs: Vec<i64> = rows
            .iter()
            .map(|r| match r[1] {
                V::Bigint(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(runs, vec![5, 10, 20, 40]);
        // Whole-partition default without ORDER BY.
        let rows = select(&mut db, "SELECT COUNT(*) OVER () AS c FROM sales;");
        assert!(rows.iter().all(|r| r[0] == V::Bigint(4)));
        // Explicit ROWS BETWEEN 1 PRECEDING AND CURRENT ROW.
        let rows = select(
            &mut db,
            "SELECT amount, SUM(amount) OVER (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w
             FROM sales ORDER BY amount;",
        );
        let got: Vec<i64> = rows
            .iter()
            .map(|r| match r[1] {
                V::Bigint(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![5, 10, 15, 30]);
    }

    #[test]
    fn window_over_agg_and_rejections() {        let mut db = win_db();
        let rows = select(
            &mut db,
            "SELECT region, SUM(amount) AS s, RANK() OVER (ORDER BY SUM(amount) DESC) AS r
             FROM sales GROUP BY region ORDER BY r;",
        );
        assert_eq!(rows[0][0], V::Varchar("west".to_string()));
        assert_eq!(rows[0][2], V::Bigint(1));
        // Windows forbidden in WHERE / GROUP BY / HAVING.
        assert!(db.execute("SELECT * FROM sales WHERE RANK() OVER () > 1;").is_err());
        assert!(db.execute("SELECT * FROM sales GROUP BY RANK() OVER ();").is_err());
        assert!(db
            .execute("SELECT SUM(amount) FROM sales HAVING RANK() OVER () > 1;")
            .is_err());
        // Nested windows rejected; unknown window rejected.
        assert!(db
            .execute("SELECT RANK() OVER (ORDER BY SUM(amount) OVER ()) FROM sales;")
            .is_err());
        assert!(db.execute("SELECT MEDIAN(amount) OVER () FROM sales;").is_err());
    }

    // --- phase 9: UPDATE / DELETE ---

    fn ud_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, NULL);").unwrap();
        db
    }

    fn updated(db: &mut PinkDb, sql: &str) -> usize {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Updated { count } => count,
            QueryResult::Deleted { count } => count,
            r => panic!("not a mutation result: {r:?}"),
        }
    }

    #[test]
    fn update_basic_and_where() {
        let mut db = ud_db();
        assert_eq!(updated(&mut db, "UPDATE t SET a = a + 10 WHERE a > 1;"), 2);
        assert_eq!(
            select(&mut db, "SELECT a, b FROM t ORDER BY a;"),
            vec![
                vec![V::Integer(1), V::Varchar("x".to_string())],
                vec![V::Integer(12), V::Varchar("y".to_string())],
                vec![V::Integer(13), V::Null],
            ]
        );
        // No WHERE updates everything.
        assert_eq!(updated(&mut db, "UPDATE t SET b = 'z';"), 3);
        // NULL assignment + expression to NULL.
        assert_eq!(updated(&mut db, "UPDATE t SET b = NULL WHERE a = 1;"), 1);
        assert_eq!(select(&mut db, "SELECT b FROM t WHERE a = 1;"), vec![vec![V::Null]]);
    }

    #[test]
    fn update_sets_are_simultaneous() {
        let mut db = ud_db();
        // a=b, b=a swaps (both read pre-update values).
        db.execute("CREATE TABLE s (a INTEGER, b INTEGER);").unwrap();
        db.execute("INSERT INTO s VALUES (1, 2);").unwrap();
        assert_eq!(updated(&mut db, "UPDATE s SET a = b, b = a;"), 1);
        assert_eq!(select(&mut db, "SELECT a, b FROM s;"), vec![vec![V::Integer(2), V::Integer(1)]]);
    }

    #[test]
    fn update_type_rules() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE n (i INTEGER, g BIGINT, d DOUBLE);").unwrap();
        db.execute("INSERT INTO n VALUES (1, 1, 1.0);").unwrap();
        // Widening allowed.
        assert_eq!(updated(&mut db, "UPDATE n SET g = i, d = g;"), 1);
        // Narrowing / wrong types rejected, table untouched.
        assert!(db.execute("UPDATE n SET i = g;").is_err());
        assert!(db.execute("UPDATE n SET i = 'x';").is_err());
        assert!(db.execute("UPDATE n SET missing = 1;").is_err());
        assert!(db.execute("UPDATE n SET i = 1, i = 2;").is_err());
        assert!(db.execute("UPDATE n SET i = SUM(i);").is_err());
        assert_eq!(
            select(&mut db, "SELECT i, g, d FROM n;"),
            vec![vec![V::Integer(1), V::Bigint(1), V::Double(1.0)]]
        );
    }

    #[test]
    fn delete_basic_and_all() {
        let mut db = ud_db();
        assert_eq!(updated(&mut db, "DELETE FROM t WHERE a >= 2;"), 2);
        assert_eq!(select(&mut db, "SELECT a FROM t;"), vec![vec![V::Integer(1)]]);
        // Delete everything, then reinsert (empty chunks are dropped cleanly).
        assert_eq!(updated(&mut db, "DELETE FROM t;"), 1);
        assert!(select(&mut db, "SELECT * FROM t;").is_empty());
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(0)]]);
        db.execute("INSERT INTO t VALUES (9, 'q');").unwrap();
        assert_eq!(select(&mut db, "SELECT a FROM t;"), vec![vec![V::Integer(9)]]);
        // Unknown table / bad predicate.
        assert!(db.execute("DELETE FROM missing;").is_err());
        assert!(db.execute("DELETE FROM t WHERE a;").is_err());
    }

    #[test]
    fn update_delete_with_subquery() {        let mut db = ud_db();
        assert_eq!(
            updated(&mut db, "UPDATE t SET b = 'top' WHERE a = (SELECT MAX(a) FROM t);"),
            1
        );
        assert_eq!(
            select(&mut db, "SELECT b FROM t WHERE a = 3;"),
            vec![vec![V::Varchar("top".to_string())]]
        );
        // Fresh DB: row 3 still has NULL b, so the IN-set is {3}.
        let mut db = ud_db();
        assert_eq!(updated(&mut db, "DELETE FROM t WHERE a IN (SELECT a FROM t WHERE b IS NULL);"), 1);
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(2)]]);
    }

    // --- phase 10: transactions ---

    #[test]
    fn txn_commit_and_rollback() {
        let mut db = ud_db();
        assert!(!db.in_transaction());
        db.execute("BEGIN;").unwrap();
        assert!(db.in_transaction());
        db.execute("UPDATE t SET a = 100 WHERE a = 1;").unwrap();
        db.execute("DELETE FROM t WHERE a = 2;").unwrap();
        db.execute("CREATE TABLE tmp (x INTEGER);").unwrap();
        // Inside: changes visible.
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(2)]]);
        db.execute("ROLLBACK;").unwrap();
        assert!(!db.in_transaction());
        // Outside: everything reverted, data and catalog alike.
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(3)]]);
        assert_eq!(select(&mut db, "SELECT a FROM t WHERE a = 1;"), vec![vec![V::Integer(1)]]);
        assert!(db.table("tmp").is_err());

        db.execute("BEGIN;").unwrap();
        db.execute("DELETE FROM t;").unwrap();
        db.execute("COMMIT;").unwrap();
        assert!(!db.in_transaction());
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(0)]]);
    }

    #[test]
    fn txn_errors() {
        let mut db = ud_db();
        assert!(db.execute("COMMIT;").is_err());
        assert!(db.execute("ROLLBACK;").is_err());
        db.execute("BEGIN;").unwrap();
        assert!(db.execute("BEGIN;").is_err());
        assert!(db.execute("BEGIN TRANSACTION;").is_err());
        db.execute("ROLLBACK;").unwrap();
        // Reads and writes work fine outside transactions too.
        assert_eq!(select(&mut db, "SELECT COUNT(*) FROM t;"), vec![vec![V::Bigint(3)]]);
    }

    // --- phase 11: UNION, views, CAST, COALESCE, LIKE, DROP ---

    fn uv_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE a (x INTEGER, s VARCHAR);").unwrap();
        db.execute("CREATE TABLE b (y BIGINT, t VARCHAR);").unwrap();
        db.execute("INSERT INTO a VALUES (1, 'one'), (2, 'two'), (NULL, 'nil');").unwrap();
        db.execute("INSERT INTO b VALUES (2, 'TWO'), (3, 'three');").unwrap();
        db
    }

    #[test]
    fn union_all_and_distinct() {
        let mut db = uv_db();
        // NOTE: ORDER BY/LIMIT attach to their own UNION branch in v1, so
        // union tests sort client-side.
        let mut rows = select(&mut db, "SELECT x FROM a WHERE x IS NOT NULL UNION ALL SELECT y FROM b;");
        rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
        assert_eq!(
            rows,
            vec![
                vec![V::Bigint(1)],
                vec![V::Bigint(2)],
                vec![V::Bigint(2)],
                vec![V::Bigint(3)],
            ]
        );
        // INTEGER unifies to BIGINT (left names win).
        let rows = select(&mut db, "SELECT x AS n FROM a WHERE x = 1 UNION SELECT y AS m FROM b WHERE y = 3;");
        assert_eq!(rows.len(), 2);
        // UNION (distinct) dedupes, NULLs dedupe together.
        let mut rows = select(&mut db, "SELECT x FROM a UNION SELECT y FROM b;");
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (V::Null, V::Null) => std::cmp::Ordering::Equal,
            (V::Null, _) => std::cmp::Ordering::Greater,
            (_, V::Null) => std::cmp::Ordering::Less,
            (x, y) => x.to_string().cmp(&y.to_string()),
        });
        assert_eq!(rows.len(), 4); // 1, 2, 3, NULL
        assert_eq!(rows.last().unwrap(), &vec![V::Null]);
        // Arity + type mismatches.
        assert!(db.execute("SELECT x FROM a UNION SELECT y, t FROM b;").is_err());
        assert!(db.execute("SELECT s FROM a UNION SELECT y FROM b;").is_err());
    }

    #[test]
    fn views_create_use_drop() {
        let mut db = uv_db();
        db.execute("CREATE VIEW evens AS SELECT x FROM a WHERE x % 2 = 0;").unwrap();
        assert_eq!(select(&mut db, "SELECT * FROM evens;"), vec![vec![V::Integer(2)]]);
        // Views compose with joins, CTEs, and aliases.
        let rows = select(
            &mut db,
            "WITH w AS (SELECT * FROM evens) SELECT x, y FROM w JOIN b ON w.x = b.y;",
        );
        assert_eq!(rows, vec![vec![V::Integer(2), V::Bigint(2)]]);
        // Column aliases.
        db.execute("CREATE VIEW named(n) AS SELECT x FROM a WHERE x = 1;").unwrap();
        assert_eq!(select(&mut db, "SELECT n FROM named;"), vec![vec![V::Integer(1)]]);
        // Name clashes + dupes rejected.
        assert!(db.execute("CREATE VIEW a AS SELECT 1;").is_err());
        assert!(db.execute("CREATE TABLE evens (x INTEGER);").is_err());
        assert!(db.execute("CREATE VIEW evens AS SELECT 2;").is_err());
        // Use-after-drop of the base table errors (rebound per use).
        db.execute("DROP TABLE b;").unwrap();
        assert!(db.execute("CREATE VIEW vb AS SELECT y FROM b;").is_err());
        db.execute("DROP VIEW evens;").unwrap();
        assert!(db.execute("SELECT * FROM evens;").is_err());
        assert!(db.execute("DROP VIEW evens;").is_err());
        assert!(db.execute("DROP TABLE missing;").is_err());
    }

    #[test]
    fn cast_coalesce_like() {
        let mut db = uv_db();
        let rows = select(
            &mut db,
            "SELECT CAST(x AS BIGINT), CAST(x AS DOUBLE) FROM a WHERE x = 1;",
        );
        assert_eq!(rows, vec![vec![V::Bigint(1), V::Double(1.0)]]);
        // 'one' does not parse as INTEGER.
        assert!(db.execute("SELECT CAST(s AS INTEGER) FROM a WHERE x = 1;").is_err());
        assert!(db.execute("SELECT CAST(x AS DATE) FROM a;").is_err());
        let rows = select(&mut db, "SELECT COALESCE(x, 99), COALESCE(s, 'dflt') FROM a ORDER BY 1;");
        assert_eq!(rows[0], vec![V::Integer(1), V::Varchar("one".to_string())]);
        assert_eq!(rows[2], vec![V::Integer(99), V::Varchar("nil".to_string())]);
        let rows = select(&mut db, "SELECT s FROM a WHERE s LIKE 'o%' ORDER BY s;");
        assert_eq!(
            rows,
            vec![vec![V::Varchar("one".to_string())]]
        );
        let rows = select(&mut db, "SELECT s FROM a WHERE s NOT LIKE '%o%' ORDER BY s;");
        assert_eq!(rows, vec![vec![V::Varchar("nil".to_string())]]);
        // LIKE is NULL-propagating; mixed types rejected.
        assert_eq!(select(&mut db, "SELECT CAST(NULL AS VARCHAR) LIKE 'a';"), vec![vec![V::Null]]);
        assert!(db.execute("SELECT x LIKE '1' FROM a;").is_err());
    }
}
