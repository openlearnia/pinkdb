//! Scores against the independent Python reference
//! (`tests/reference/bm25.py`, which regenerates both fixtures).
//!
//! The reference implements DuckDB's documented BM25 with its own tokenizer
//! and Porter stemmer; agreement here covers all three at once. Comparing
//! against a real DuckDB with fts installed is still outstanding — see
//! docs/wave-108a-plan.md.

use std::collections::BTreeMap;

use pinkdb::{PinkDb, QueryResult, Value};

const CORPUS: &str = include_str!("reference/bm25_corpus.tsv");
const QUERIES: &str = include_str!("reference/bm25_queries.tsv");

fn db() -> PinkDb {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| pinkdb_fts::ensure_registered().unwrap());
    let mut db = PinkDb::new();
    db.load_extension_package("fts").unwrap();
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup() -> PinkDb {
    let mut db = db();
    db.execute("CREATE TABLE docs (id INTEGER, title VARCHAR, body VARCHAR)")
        .unwrap();
    for line in CORPUS
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let mut parts = line.splitn(3, '\t');
        let id = parts.next().unwrap();
        let title = parts.next().unwrap().replace('\'', "''");
        let body = parts.next().unwrap().replace('\'', "''");
        db.execute(&format!(
            "INSERT INTO docs VALUES ({id}, '{title}', '{body}')"
        ))
        .unwrap();
    }
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    db
}

/// `match_bm25` for every document, skipping the NULL (no match) scores.
fn scores(
    db: &mut PinkDb,
    query: &str,
    fields: &str,
    k: &str,
    b: &str,
    conjunctive: &str,
) -> BTreeMap<i64, f64> {
    let fields = if fields.is_empty() {
        String::new()
    } else {
        format!(", fields := '{fields}'")
    };
    let sql = format!(
        "SELECT id, match_bm25('docs', id, '{query}'{fields}, k := {k}, b := {b}, \
         conjunctive := {conjunctive}) FROM docs"
    );
    rows(db, &sql)
        .into_iter()
        .filter_map(|row| match (&row[0], &row[1]) {
            (Value::Integer(id), Value::Double(score)) => Some((*id as i64, *score)),
            (_, Value::Null) => None,
            (id, score) => panic!("unexpected ({id:?}, {score:?})"),
        })
        .collect()
}

#[test]
fn scores_match_reference() {
    let mut db = setup();
    let mut checked = 0;
    for line in QUERIES
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let mut parts = line.splitn(6, '\t');
        let query = parts.next().unwrap();
        let fields = parts.next().unwrap();
        let k = parts.next().unwrap();
        let b = parts.next().unwrap();
        let conjunctive = parts.next().unwrap();
        let expected: BTreeMap<i64, f64> = parts
            .next()
            .unwrap()
            .split(';')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (id, score) = pair.split_once(':').unwrap();
                (id.parse().unwrap(), score.parse().unwrap())
            })
            .collect();

        let got = scores(&mut db, query, fields, k, b, conjunctive);
        assert_eq!(
            got.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "documents matching {query:?}"
        );
        for (id, want) in &expected {
            let got = got[id];
            assert!(
                (got - want).abs() <= 1e-9,
                "score of doc {id} for {query:?}: got {got}, reference {want}"
            );
        }
        // The ranking a caller would use: best score first, ties by id.
        let mut ranked: Vec<(i64, f64)> = got.iter().map(|(id, s)| (*id, *s)).collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut expected_ranked: Vec<(i64, f64)> =
            expected.iter().map(|(id, s)| (*id, *s)).collect();
        expected_ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        assert_eq!(
            ranked.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            expected_ranked
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            "ranking for {query:?}"
        );
        checked += 1;
    }
    assert_eq!(checked, 15);
}

#[test]
fn tokenizer_matches_reference() {
    // Lowercasing, accent stripping, the default ignore regex (punctuation and
    // digits separate) and the stopword list, observed through `match_bm25`.
    let mut db = db();
    db.execute(
        "CREATE TABLE t (id INTEGER, body VARCHAR); \
         INSERT INTO t VALUES (1, 'Café CRÈME brûlée'), (2, 'the 1984 report')",
    )
    .unwrap();
    db.execute("CALL create_fts_index('t', 'id', 'body')")
        .unwrap();
    let hit = |db: &mut PinkDb, id: i64, q: &str| {
        let rows = rows(
            db,
            &format!("SELECT match_bm25('t', id, '{q}') FROM t WHERE id = {id}"),
        );
        !matches!(rows[0][0], Value::Null)
    };
    assert!(hit(&mut db, 1, "cafe"), "lowercase + accent strip");
    assert!(hit(&mut db, 1, "creme"));
    assert!(hit(&mut db, 1, "brulee"));
    assert!(hit(&mut db, 1, "CAFÉ"), "the query is folded too");
    // A stopword-only query has no terms, and digits are separators.
    assert!(!hit(&mut db, 2, "the"));
    assert!(!hit(&mut db, 2, "1984"));
    assert!(!hit(&mut db, 1, "the"));
}
