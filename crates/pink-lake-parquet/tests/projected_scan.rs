//! Wave 133c: a lake scan hands the pipeline projected chunks built straight
//! from the decoded Parquet vectors — the logical width, zero-length
//! placeholders for the columns the query did not ask for, bulk-filled
//! defaults for columns an old file predates — and a plan node that drains a
//! source itself decodes on the pool while keeping morsel order.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, CommitSettings, Lake, WritableAttachment};
use pink_lake_parquet::{FORMAT_ID, register};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_projected_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn settings() -> CommitSettings {
    CommitSettings {
        retry_wait_ms: 1,
        ..CommitSettings::default()
    }
}

fn catalog(lake_dir: &Path) -> serde_json::Value {
    let url = format!("pink:{}", lake_dir.display());
    let lake = Lake::open(lake_dir).unwrap();
    let mut tables = serde_json::Map::new();
    for info in lake.table_info().unwrap() {
        if let Some(entry) = pink_lake::catalog_table_json(&url, &info.table).unwrap() {
            tables.insert(info.table.clone(), entry);
        }
    }
    serde_json::json!({ "tables": tables })
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// `t (i INTEGER, s VARCHAR)` with `rows` rows, `i = 0..rows`.
fn store(rows: i32) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        .unwrap();
    for i in 0..rows {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    db
}

/// A writable attachment's base tables in a session store, exactly like
/// `.lake attach … write` does (including the schema record's defaults).
fn materialize(wa: &WritableAttachment) -> PinkDb {
    let mut db = PinkDb::new();
    for (key, _) in wa.tables() {
        let (dbname, table) = key.split_once('.').unwrap();
        let base = wa.base_table(&key).unwrap();
        if !db.database_exists(dbname) {
            db.create_database(dbname).unwrap();
        }
        db.create_table_in(dbname, table, base.schema()).unwrap();
        let t = db.table_mut_in(dbname, table).unwrap();
        for (col, src) in t.columns.iter_mut().zip(&base.columns) {
            col.default = src.default.clone();
        }
        for c in 0..base.num_chunks() {
            let chunk = base.scan_chunk(c).unwrap().unwrap();
            t.append_shared_chunk(&chunk.columns).unwrap();
        }
    }
    db
}

fn appended(rows: &[(i32, &str)]) -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    for (i, s) in rows {
        table
            .append_row(vec![Value::Integer(*i), Value::Varchar((*s).to_string())])
            .unwrap();
    }
    Arc::new(table)
}

/// Files written before the `RENAME COLUMN` and the two `ADD COLUMN`s read
/// back the old data under the new name, the default of the added column and
/// NULL for the added column without one; the file written after them has all
/// four columns. The rows come back in morsel order, the inserted row last.
#[test]
fn old_files_read_the_added_defaults_and_the_renamed_column() {
    register().unwrap();
    let dir = scratch("evolve");
    let lake_dir = dir.join("lake");
    // More than one 64K-row Parquet row group in the first file.
    const ROWS: i32 = 70_000;

    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.write_from_store(&store(ROWS)).unwrap();

    let mut wa = WritableAttachment::attach(Lake::open(&lake_dir).unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 42;")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN q INTEGER;").unwrap();
    db.execute(&format!("INSERT INTO t (i, label) VALUES ({ROWS}, 'new');"))
        .unwrap();
    wa.commit(&db, &settings()).unwrap();

    let entry = catalog(&lake_dir);
    let files = entry["tables"]["main.t"]["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "{entry}");
    for file in files {
        let path = file["path"].as_str().unwrap();
        assert!(path.ends_with(".parquet"), "{path}");
    }

    let mut db = PinkDb::new();
    let attachment = Lake::open(&lake_dir).unwrap().attach(None).unwrap();
    db.attach_lake(attachment.lake).unwrap();

    let mut want: Vec<Vec<Value>> = (0..ROWS)
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::Varchar(format!("v{i}")),
                Value::Integer(42),
                Value::Null,
            ]
        })
        .collect();
    want.push(vec![
        Value::Integer(ROWS),
        Value::Varchar("new".to_string()),
        Value::Integer(42),
        Value::Null,
    ]);
    // Old file, then the file the commit wrote: morsel order, not the order
    // the workers finished in.
    assert_eq!(query(&mut db, "SELECT i, label, n, q FROM t;"), want);
    assert_eq!(
        query(&mut db, "SELECT * FROM t;"),
        want,
        "SELECT * reads every column"
    );

    // Only mapped columns: the renamed one, no default fills.
    let labels: Vec<Vec<Value>> = want.iter().map(|row| vec![row[1].clone()]).collect();
    assert_eq!(query(&mut db, "SELECT label FROM t;"), labels);

    // Only the added columns: the defaults are filled in bulk for the file
    // written before the ALTER.
    assert_eq!(
        query(
            &mut db,
            "SELECT COUNT(*), COUNT(n), MIN(n), MAX(n), COUNT(q) FROM t;"
        ),
        vec![vec![
            Value::Bigint(ROWS as i64 + 1),
            Value::Bigint(ROWS as i64 + 1),
            Value::Integer(42),
            Value::Integer(42),
            Value::Bigint(0),
        ]]
    );
    assert_eq!(
        query(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(ROWS as i64 + 1)]],
        "a count reads no column"
    );
    cleanup(&dir);
}

/// The same rows as a session table, appended in lake order: a store scan is
/// the serial reference, so a drained lake scan that decoded on several
/// workers and merged out of order would differ row by row.
#[test]
fn a_drained_lake_scan_and_join_match_a_store_copy() {
    register().unwrap();
    let dir = scratch("drain");
    let lake_dir = dir.join("lake");
    const ROWS: i32 = 70_000;

    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.write_from_store(&store(ROWS)).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append("main.t", appended(&[(ROWS, "new")]));
    lake.commit(&commit).unwrap();

    let mut db = PinkDb::new();
    let attachment = Lake::open(&lake_dir).unwrap().attach(None).unwrap();
    db.attach_lake(attachment.lake).unwrap();
    db.execute("CREATE TABLE copy (i INTEGER, s VARCHAR);")
        .unwrap();
    for i in 0..ROWS {
        db.table_mut("copy")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    db.table_mut("copy")
        .unwrap()
        .append_row(vec![
            Value::Integer(ROWS),
            Value::Varchar("new".to_string()),
        ])
        .unwrap();
    db.execute("CREATE TABLE k (k INTEGER);").unwrap();
    for k in (0..=ROWS).step_by(700) {
        db.table_mut("k")
            .unwrap()
            .append_row(vec![Value::Integer(k)])
            .unwrap();
    }

    assert_eq!(
        query(&mut db, "SELECT i, s FROM t;"),
        query(&mut db, "SELECT i, s FROM copy;"),
        "drained scan order"
    );
    // A join side the plan evaluates node by node (`scan_output`) drains the
    // lake itself, on the calling thread before wave 133c.
    assert_eq!(
        query(&mut db, "SELECT k.k, t.i, t.s FROM k JOIN t ON k.k = t.i;"),
        query(
            &mut db,
            "SELECT k.k, copy.i, copy.s FROM k JOIN copy ON k.k = copy.i;"
        ),
        "join with the lake on the build side"
    );
    assert_eq!(
        query(&mut db, "SELECT t.i, t.s, k.k FROM t JOIN k ON t.i = k.k;"),
        query(
            &mut db,
            "SELECT copy.i, copy.s, k.k FROM copy JOIN k ON copy.i = k.k;"
        ),
        "join with the lake on the probe side"
    );
    cleanup(&dir);
}
