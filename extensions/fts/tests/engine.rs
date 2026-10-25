//! Engine-level tests: the SQL surface against a real store.

use std::path::PathBuf;
use std::sync::Once;

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

fn register() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| pinkdb_fts::ensure_registered().unwrap());
}

fn db() -> PinkDb {
    register();
    let mut db = PinkDb::new();
    db.load_extension_package("fts").unwrap();
    db
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected a result set, got {other:?}"),
    }
}

fn score(db: &mut PinkDb, id: i64, query_text: &str) -> Option<f64> {
    score_with(db, "docs", id, query_text, "")
}

fn score_with(db: &mut PinkDb, index: &str, id: i64, query_text: &str, args: &str) -> Option<f64> {
    let rows = query(
        db,
        &format!(
            "SELECT match_bm25('{index}', id, '{query_text}'{args}) FROM docs WHERE id = {id}"
        ),
    );
    match rows.first().and_then(|row| row.first()) {
        Some(Value::Double(score)) => Some(*score),
        Some(Value::Null) | None => None,
        other => panic!("unexpected score {other:?}"),
    }
}

/// Every row of the index tables, so two builds can be compared byte for byte.
fn index_dump(db: &mut PinkDb) -> Vec<String> {
    let mut out = Vec::new();
    for table in pinkdb_fts::INDEX_TABLES {
        let rows = query(db, &format!("SELECT * FROM fts_main_docs.{table}"));
        for row in rows {
            out.push(format!("{table} {row:?}"));
        }
    }
    out
}

fn corpus() -> PinkDb {
    let mut db = db();
    db.execute(
        "CREATE TABLE docs (id INTEGER, title VARCHAR, body VARCHAR); \
         INSERT INTO docs VALUES \
           (1, 'Relational databases', 'A relational database stores rows in tables'), \
           (2, 'Search engines', 'Full text search ranks documents with BM25 over an inverted index'), \
           (3, 'Cooking', 'Bread needs flour water and time')",
    )
    .unwrap();
    db
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_fts_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn stem_and_rrf() {
    let mut db = db();
    let rows = query(
        &mut db,
        "SELECT stem('relational'), stem('databases'), stem('running'), stem('ponies')",
    );
    let row = &rows[0];
    assert_eq!(row[0], Value::Varchar("relat".to_string()));
    assert_eq!(row[1], Value::Varchar("databas".to_string()));
    assert_eq!(row[2], Value::Varchar("run".to_string()));
    assert_eq!(row[3], Value::Varchar("poni".to_string()));
    let rows = query(&mut db, "SELECT rrf(1), rrf(3, 60), rrf(0)");
    assert_eq!(rows[0][0], Value::Double(1.0 / 61.0));
    assert_eq!(rows[0][1], Value::Double(1.0 / 63.0));
    assert_eq!(rows[0][2], Value::Null);
}

#[test]
fn create_index_and_match() {
    let mut db = corpus();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    let hit = score(&mut db, 1, "relational database");
    assert!(hit.unwrap() > 0.0);
    assert_eq!(score(&mut db, 3, "relational database"), None);
    // Disjunctive (the default) ranks a document that matches either term;
    // conjunctive requires every term.
    assert!(score(&mut db, 2, "bm25 cooking").unwrap() > 0.0);
    assert_eq!(
        score_with(&mut db, "docs", 2, "bm25 cooking", ", conjunctive := 1"),
        None
    );
    assert!(score_with(&mut db, "docs", 3, "cooking time", ", conjunctive := 1").unwrap() > 0.0);
}

#[test]
fn fields_and_parameters() {
    let mut db = corpus();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    // "inverted" only occurs in doc 2's body, "cooking" only in doc 3's title.
    assert!(score_with(&mut db, "docs", 2, "inverted", ", fields := 'body'").is_some());
    assert_eq!(
        score_with(&mut db, "docs", 2, "inverted", ", fields := 'title'"),
        None
    );
    assert!(score_with(&mut db, "docs", 3, "cooking", ", fields := 'title'").is_some());
    // k and b change the score but not who matches.
    let default = score(&mut db, 1, "relational").unwrap();
    let tuned = score_with(&mut db, "docs", 1, "relational", ", k := 0.3, b := 0.0").unwrap();
    assert!(tuned > 0.0 && (tuned - default).abs() > 1e-6);
}

#[test]
fn stale_until_recreated() {
    let mut db = corpus();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    let before = score(&mut db, 1, "sourdough").is_none();
    assert!(before);
    db.execute("INSERT INTO docs VALUES (4, 'Sourdough', 'A sourdough starter needs flour')")
        .unwrap();
    // Static like DuckDB: the index still describes three documents.
    assert_eq!(score(&mut db, 4, "sourdough"), None);
    assert!(
        db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
            .is_err()
    );
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body', overwrite := 1)")
        .unwrap();
    assert!(score(&mut db, 4, "sourdough").unwrap() > 0.0);
}

#[test]
fn errors() {
    let mut db = corpus();
    // The table, its id column and its fields must exist and be text.
    for (sql, want) in [
        (
            "CALL create_fts_index('missing', 'id', 'title')",
            "table \"missing\" not found",
        ),
        (
            "CALL create_fts_index('docs', 'nope', 'title')",
            "has no id column \"nope\"",
        ),
        (
            "CALL create_fts_index('docs', 'id', 'nope')",
            "has no value column \"nope\"",
        ),
        (
            "CALL create_fts_index('docs', 'id')",
            "create_fts_index needs argument \"input_values\"",
        ),
        (
            "CALL create_fts_index('docs', 'id', 'title', stemmer := 'klingon')",
            "unsupported stemmer \"klingon\"",
        ),
        (
            "CALL create_fts_index('docs', 'id', 'title', ignore := '(')",
            "ignore regular expression",
        ),
        (
            "CALL create_fts_index('docs', 'id', 'title', overwrite := 2)",
            "overwrite takes 0 or 1",
        ),
    ] {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(err.contains(want), "{sql}: expected {want:?}, got {err:?}");
    }
    db.execute("CREATE TABLE nums (id INTEGER, n INTEGER); INSERT INTO nums VALUES (1, 2)")
        .unwrap();
    let err = db
        .execute("CALL create_fts_index('nums', 'id', 'n')")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("column \"n\" is INTEGER, not VARCHAR"),
        "{err}"
    );
    // Without an index the scorer says so; unknown field names are refused.
    let err = db
        .execute("SELECT match_bm25('docs', id, 'x') FROM docs")
        .unwrap_err();
    assert!(
        err.to_string().contains("no full-text index for \"docs\""),
        "{err}"
    );
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    let err = db
        .execute("SELECT match_bm25('docs', id, 'x', fields := 'nope') FROM docs")
        .unwrap_err();
    assert!(err.to_string().contains("no field \"nope\""), "{err}");
    let err = db
        .execute("SELECT match_bm25('docs', id, 'x', k := -1) FROM docs")
        .unwrap_err();
    assert!(err.to_string().contains("k must not be negative"), "{err}");
}

#[test]
fn save_and_open() {
    let dir = scratch("save");
    let path = dir.join("store.pinkdb");
    let path = path.to_str().unwrap();
    let mut db = corpus();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    let before = index_dump(&mut db);
    let score_before = score(&mut db, 1, "relational database").unwrap();
    db.save(path).unwrap();
    drop(db);

    let mut db = PinkDb::open(path).unwrap();
    db.load_extension_package("fts").unwrap();
    assert_eq!(index_dump(&mut db), before);
    assert_eq!(
        score(&mut db, 1, "relational database").unwrap(),
        score_before
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn wal_replay_is_deterministic() {
    let dir = scratch("wal");
    let path = dir.join("store.pinkdb");
    let path = path.to_str().unwrap();
    let mut db = PinkDb::open_durable_with(path, SyncPolicy::Sync).unwrap().0;
    db.load_extension_package("fts").unwrap();
    db.execute(
        "CREATE TABLE docs (id INTEGER, title VARCHAR, body VARCHAR); \
         INSERT INTO docs VALUES (1, 'Relational databases', 'A relational database stores rows')",
    )
    .unwrap();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    let before = index_dump(&mut db);
    let score_before = score(&mut db, 1, "relational database").unwrap();
    // Drop without a checkpoint: the next open replays the log, re-running
    // the `CALL` that built the index.
    drop(db);

    // Replay re-runs the logged `CALL` (prefixed by its `LOAD EXTENSION`), so
    // the package is already loaded.
    let mut db = PinkDb::open_durable_with(path, SyncPolicy::Sync).unwrap().0;
    assert_eq!(index_dump(&mut db), before, "replayed index differs");
    assert_eq!(
        score(&mut db, 1, "relational database").unwrap(),
        score_before
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn drop_fts_index() {
    let mut db = corpus();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    db.execute("CALL drop_fts_index('docs')").unwrap();
    assert!(db.execute("CALL drop_fts_index('docs')").is_err());
    assert!(
        db.execute("SELECT match_bm25('docs', id, 'x') FROM docs")
            .is_err()
    );
    // The base table is untouched.
    assert_eq!(
        query(&mut db, "SELECT count(*) FROM docs")[0][0],
        Value::Bigint(3)
    );
}
