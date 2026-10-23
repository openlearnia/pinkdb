//! Wave 103 T1a: DuckLake v1.1 catalog interop.
//!
//! Two directions:
//!
//! 1. A pinkdb Parquet lake whose catalog is SQLite attaches in DuckDB (the
//!    `ducklake` extension): `count(*)`, a filtered query and `AT VERSION`.
//!    Skipped, with a printed note, when the reference DuckDB binary is not
//!    present (`DUCKDB_BIN` overrides `/tmp/duckdb-2.0.0/duckdb`).
//! 2. A DuckDB-written DuckLake catalog (the checked-in fixture) opens in
//!    pinkdb with an explicit data location. Older versions read `t`
//!    (inlined) and `f` (Parquet); the latest `f` carries a DuckLake Parquet
//!    delete file, which this build names as unsupported rather than
//!    misreading.

#![cfg(feature = "catalog-sqlite")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, Lake};
use pink_lake_parquet::{FORMAT_ID, register};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_ducklake_{tag}_{}_{}",
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

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let source = entry.path();
        let target = to.join(entry.file_name());
        if source.is_dir() {
            copy_tree(&source, &target);
        } else {
            std::fs::copy(&source, &target).unwrap();
        }
    }
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn count(db: &mut PinkDb, sql: &str) -> i64 {
    match &query(db, sql)[0][0] {
        Value::Integer(v) => i64::from(*v),
        Value::Bigint(v) => *v,
        other => panic!("expected an integer count, got {other:?}"),
    }
}

fn duckdb_binary() -> Option<PathBuf> {
    let path = std::env::var_os("DUCKDB_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/duckdb-2.0.0/duckdb"));
    if path.is_file() {
        Some(path)
    } else {
        println!(
            "skip DuckDB interop test: {} is not present (set DUCKDB_BIN to run it)",
            path.display()
        );
        None
    }
}

/// Run one script and return the CSV rows (one line per SELECT).
fn run_duckdb(binary: &Path, script: &str) -> Result<Vec<String>, String> {
    let output = Command::new(binary)
        .args(["-no-agent", "-csv", "-noheader", "-c", script])
        .output()
        .map_err(|e| format!("running {}: {e}", binary.display()))?;
    if !output.status.success() {
        return Err(format!(
            "duckdb failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn parquet_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    for i in 0..3 {
        db.table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("row-{i}"))])
            .unwrap();
    }
    db
}

fn append_table() -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    for i in 3..5 {
        table
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("row-{i}"))])
            .unwrap();
    }
    Arc::new(table)
}

#[test]
fn a_pinkdb_parquet_lake_attaches_in_duckdb() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-to-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(&parquet_store()).unwrap(), 2);
    let mut commit = Commit::new();
    commit.append("main.t", append_table());
    assert_eq!(lake.commit(&commit).unwrap().version, 3);
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT count(*) FROM l.t;\n\
         SELECT count(*) FROM l.t WHERE i >= 3;\n\
         SELECT count(*) FROM l.t AT (VERSION => 1);\n",
        catalog_path.display(),
        data.display()
    );
    let rows = match run_duckdb(&binary, &script) {
        Ok(rows) => rows,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    };
    assert_eq!(
        rows,
        vec!["5".to_string(), "2".to_string(), "3".to_string()],
        "DuckDB attach/count/filter/AT VERSION over a pinkdb Parquet lake"
    );

    cleanup(&dir);
}

#[test]
fn duckdb_reads_pinkdb_inlined_rows_with_the_right_types() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-inlined-to-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(10);

    // Save only the schema, then append rows small enough to inline: they are
    // stored only in DuckLake's typed `ducklake_inlined_data_1_<n>` table.
    let mut empty = PinkDb::new();
    empty
        .execute(
            "CREATE TABLE main.t (
                 i INTEGER, big BIGINT, b BOOLEAN, d DOUBLE, v VARCHAR,
                 dt DATE, ts TIMESTAMP, dec DECIMAL(18,3)
             );",
        )
        .unwrap();
    lake.write_from_store(&empty).unwrap();

    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("big".to_string(), DataType::Bigint),
            ("b".to_string(), DataType::Boolean),
            ("d".to_string(), DataType::Double),
            ("v".to_string(), DataType::Varchar),
            ("dt".to_string(), DataType::Date),
            ("ts".to_string(), DataType::Timestamp),
            (
                "dec".to_string(),
                DataType::Decimal {
                    precision: 18,
                    scale: 3,
                },
            ),
        ],
    )
    .unwrap();
    table
        .append_row(vec![
            Value::Integer(1),
            Value::Bigint(9_000_000_000),
            Value::Boolean(true),
            Value::Double(-0.0),
            Value::Varchar("héllo".to_string()),
            Value::Date(0),
            Value::Timestamp(0),
            Value::Decimal(123_456, 3),
        ])
        .unwrap();
    table
        .append_row(vec![
            Value::Integer(-2),
            Value::Bigint(i64::MIN),
            Value::Boolean(false),
            Value::Double(1.5),
            Value::Varchar("x".to_string()),
            Value::Date(1),
            Value::Timestamp(1_000_000),
            Value::Decimal(0, 3),
        ])
        .unwrap();
    table
        .append_row(vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ])
        .unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(table));
    lake.commit(&commit).unwrap();
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT count(*) FROM l.t;\n\
         SELECT typeof(i), typeof(big), typeof(b), typeof(d), typeof(v), \
                typeof(dt), typeof(ts), typeof(dec) FROM l.t LIMIT 1;\n\
         SELECT i, big, b, d, v, dt, ts, dec FROM l.t ORDER BY i NULLS LAST;\n",
        catalog_path.display(),
        data.display()
    );
    let rows = match run_duckdb(&binary, &script) {
        Ok(rows) => rows,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    };
    assert_eq!(
        rows,
        vec![
            "3".to_string(),
            "INTEGER,BIGINT,BOOLEAN,DOUBLE,VARCHAR,DATE,TIMESTAMP,\"DECIMAL(18,3)\"".to_string(),
            "-2,-9223372036854775808,false,1.5,x,1970-01-02,1970-01-01 00:00:01,0.000".to_string(),
            "1,9000000000,true,-0.0,\"héllo\",1970-01-01,1970-01-01 00:00:00,123.456".to_string(),
            "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL".to_string(),
        ],
        "DuckDB must read pinkdb-inlined rows from the typed DuckLake tables"
    );

    cleanup(&dir);
}

#[test]
fn duckdb_reads_a_lake_after_a_pinkdb_flush() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-flush-to-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(10);
    let mut empty = PinkDb::new();
    empty
        .execute("CREATE TABLE main.t (i INTEGER, v VARCHAR);")
        .unwrap();
    lake.write_from_store(&empty).unwrap();
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("v".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    for i in 0..3 {
        table
            .append_row(vec![
                Value::Integer(i),
                Value::Varchar(format!("flushed-{i}")),
            ])
            .unwrap();
    }
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(table));
    assert_eq!(lake.commit(&commit).unwrap().version, 3);
    let report = lake.flush_inlined_data(Some("main.t")).unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.tables, vec![("main.t".to_string(), 3)]);
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT count(*) FROM l.t;\n\
         SELECT i, v FROM l.t ORDER BY i;\n",
        catalog_path.display(),
        data.display()
    );
    let rows = match run_duckdb(&binary, &script) {
        Ok(rows) => rows,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    };
    assert_eq!(
        rows,
        vec![
            "3".to_string(),
            "0,flushed-0".to_string(),
            "1,flushed-1".to_string(),
            "2,flushed-2".to_string(),
        ],
        "DuckDB must read the parquet files a pinkdb flush materialized"
    );

    cleanup(&dir);
}

#[test]
fn duckdb_refuses_an_encrypted_pinkdb_lake() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-encrypted-to-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let key = pink_lake::EncryptionKey::from_passphrase("duckdb-must-not-read-this");
    let mut lake =
        Lake::create_with_catalog_with_key(&catalog, data.to_str().unwrap(), key.clone()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&parquet_store()).unwrap();
    // Prove the file on disk really is sealed before DuckDB tries it.
    let data_file = std::fs::read_dir(data.join("data").join("main.t"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .expect("the save produced a .parquet data file");
    assert!(
        std::fs::read(&data_file).unwrap().starts_with(b"PNKLAKE1"),
        "the data file must be sealed"
    );
    drop(lake);

    // PNKLAKE1 is AES-GCM with raw per-file keys, not Parquet modular
    // encryption. DuckDB must fail (never return rows) rather than treat the
    // container as a Parquet file.
    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT i, s FROM l.t;\n",
        catalog_path.display(),
        data.display()
    );
    match run_duckdb(&binary, &script) {
        Ok(rows) => panic!("DuckDB must refuse an encrypted pinkdb lake, got {rows:?}"),
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
        }
        Err(message) => {
            assert!(
                !message.contains("-1\n") && !message.contains("row-0"),
                "never wrong rows: {message}"
            );
        }
    }

    cleanup(&dir);
}

/// Wave 103 T2: DuckDB attaches a pinkdb Parquet lake whose catalog is
/// Postgres. Skipped with a printed note when the server variable or DuckDB is
/// absent. Each run uses a fresh schema in the throwaway test database.
#[cfg(feature = "catalog-postgres")]
#[test]
fn a_pinkdb_postgres_catalog_lake_attaches_in_duckdb() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    let Ok(base) = std::env::var("PINKDB_TEST_POSTGRES_URL") else {
        println!("skip Postgres interop test: PINKDB_TEST_POSTGRES_URL is not set");
        return;
    };
    let base = base.trim();
    if base.is_empty() {
        println!("skip Postgres interop test: PINKDB_TEST_POSTGRES_URL is empty");
        return;
    }
    register().unwrap();

    let dir = scratch("pink-postgres-to-duckdb");
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    let schema = format!("w103_{}_{}", std::process::id(), sequence);
    let separator = if base.contains('?') { '&' } else { '?' };
    let catalog = format!("{base}{separator}schema={schema}");
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&parquet_store()).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", append_table());
    lake.commit(&commit).unwrap();
    drop(lake);

    // DuckDB's postgres connection string has no `schema` parameter; the
    // DuckLake extension selects the metadata schema with METADATA_SCHEMA.
    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:postgres:{base}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true, METADATA_SCHEMA '{schema}');\n\
         SELECT count(*) FROM l.t;\n\
         SELECT count(*) FROM l.t WHERE i >= 3;\n",
        data.display()
    );
    let rows = match run_duckdb(&binary, &script) {
        Ok(rows) => rows,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip Postgres interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    };
    assert_eq!(
        rows,
        vec!["5".to_string(), "2".to_string()],
        "DuckDB attach/count/filter over a pinkdb Parquet lake with a Postgres catalog"
    );

    cleanup(&dir);
}

#[test]
fn maintenance_round_trips_with_duckdb() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-maintenance-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    // v2: three rows in file A; v3: a second save replaces them with file B,
    // ending file A at snapshot 2.
    lake.write_from_store(&parquet_store()).unwrap();
    lake.write_from_store(&parquet_store()).unwrap();

    let report = lake
        .expire_snapshots(&pink_lake::ExpireSettings {
            versions: Some(vec![2]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(report.version_numbers(), vec![2]);
    let cleaned = lake
        .cleanup_old_files(&pink_lake::CleanupSettings {
            all: true,
            confirm: true,
            ..Default::default()
        })
        .unwrap();
    assert!(cleaned.deleted, "{cleaned:?}");
    drop(lake);

    // DuckDB still attaches the pruned catalog and answers the same rows.
    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT count(*) FROM l.t;\n\
         CALL ducklake_expire_snapshots('l', versions => [3]);\n\
         CALL ducklake_cleanup_old_files('l', cleanup_all => true);\n",
        catalog_path.display(),
        data.display()
    );
    match run_duckdb(&binary, &script) {
        Ok(rows) => assert_eq!(rows.first().map(String::as_str), Some("3"), "{rows:?}"),
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    }

    // Pink opens what DuckDB expired and cleaned.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(
        count(&mut lake.load(None).unwrap(), "SELECT count(*) FROM t"),
        3
    );
    cleanup(&dir);
}

#[test]
fn pinkdb_parquet_deletes_apply_in_duckdb() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-delete-to-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&parquet_store()).unwrap();

    let file = lake
        .list_files(Some("main.t"), None, false)
        .unwrap()
        .into_iter()
        .next()
        .expect("the saved table has one data file")
        .path;
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &file, &[1]);
    assert_eq!(lake.commit(&commit).unwrap().version, 3);
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT count(*) FROM l.t;\n\
         SELECT i, s FROM l.t ORDER BY i;\n",
        catalog_path.display(),
        data.display()
    );
    let rows = match run_duckdb(&binary, &script) {
        Ok(rows) => rows,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    };
    assert_eq!(
        rows,
        vec![
            "2".to_string(),
            "0,row-0".to_string(),
            "2,row-2".to_string(),
        ],
        "DuckDB must apply pinkdb's Parquet delete file"
    );

    cleanup(&dir);
}

#[test]
fn a_duckdb_catalog_with_parquet_deletes_opens() {
    register().unwrap();
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("ducklake-sqlite");
    let dir = scratch("duckdb-to-pink");
    let copied = dir.join("ducklake");
    copy_tree(&fixture, &copied);
    let catalog = format!("sqlite:{}", copied.join("ref.db").display());
    // DuckDB records `main/` as the schema path and `<copied>/data` as the
    // data path. Pink composes schema+table+file under `data/`, so the lake
    // root is the fixture directory itself.
    let data = &copied;

    let lake = Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    // Pink version 5 is DuckLake snapshot 4: `t`'s two inlined rows and `f`'s
    // 1000-row Parquet file, before the snapshot-5 delete.
    let mut before = lake.load(Some(5)).unwrap();
    assert_eq!(count(&mut before, "SELECT count(*) FROM t"), 2);
    assert_eq!(count(&mut before, "SELECT count(*) FROM f"), 1000);
    // `t`'s two rows were inlined by DuckDB into the typed table; pink reads
    // them (and their declared types) from there.
    let rows = query(&mut before, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Varchar("x".to_string()),],
            vec![Value::Integer(2), Value::Varchar("y".to_string()),],
        ]
    );

    // The head has a DuckLake Parquet delete file (format `parquet`): it is a
    // Parquet table of `(file_path, pos)`. Pink reads it and drops exactly
    // the ten deleted positions.
    let latest = lake.load(None).unwrap();
    assert_eq!(
        count(&mut lake.load(None).unwrap(), "SELECT count(*) FROM t"),
        2
    );
    assert_eq!(
        count(&mut lake.load(None).unwrap(), "SELECT count(*) FROM f"),
        991
    );
    assert_eq!(
        count(
            &mut lake.load(None).unwrap(),
            "SELECT count(*) FROM f WHERE a >= 10"
        ),
        991
    );
    drop(latest);

    cleanup(&dir);
}
