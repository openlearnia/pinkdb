//! Wave 103 T1a: DuckLake v1.1 catalog interop.
//!
//! Two directions:
//!
//! 1. A pinkdb Parquet lake whose catalog is SQLite attaches in DuckDB (the
//!    `ducklake` extension): `count(*)`, a filtered query and `AT VERSION`.
//!    Skipped, with a printed note, when the reference DuckDB binary is not
//!    present (`DUCKDB_BIN` overrides `~/.local/bin/duckdb`).
//! 2. A DuckDB-written DuckLake catalog (the checked-in fixture) opens in
//!    pinkdb with an explicit data location. Older versions read `t`
//!    (inlined) and `f` (Parquet); the latest `f` carries a DuckLake Parquet
//!    delete file, which this build names as unsupported rather than
//!    misreading.

#![cfg(feature = "catalog-sqlite")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, Lake};
use pink_lake_parquet::{FORMAT_ID, register};
use pinkdb::{ArrayElem, DataTable, DataType, PinkDb, QueryResult, Value};

// Wave 132b: QuackLab's attach sequence. The mock S3 is std-only and is
// included the same way pink-lake's own S3 tests include it.
#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

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
        // SUM returns HUGEINT; these tests only sum small values.
        Value::Hugeint(v) => i64::try_from(*v).expect("fits in i64"),
        other => panic!("expected an integer count, got {other:?}"),
    }
}

fn duckdb_binary() -> Option<PathBuf> {
    let path = std::env::var_os("DUCKDB_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".local/bin/duckdb")
        });
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
            Value::decimal(123_456, 3),
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
            Value::decimal(0, 3),
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
         SELECT i, s FROM l.t ORDER BY i;\n\
         SELECT count(*) FROM l.t AT (VERSION => 1);\n\
         SELECT i, s FROM l.t AT (VERSION => 1) ORDER BY i;\n\
         SELECT count(*) FROM l.t AT (VERSION => 2);\n\
         SELECT i, s FROM l.t AT (VERSION => 2) ORDER BY i;\n",
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
            "3".to_string(),
            "0,row-0".to_string(),
            "1,row-1".to_string(),
            "2,row-2".to_string(),
            "2".to_string(),
            "0,row-0".to_string(),
            "2,row-2".to_string(),
        ],
        "DuckDB must read both pinkdb snapshots and apply the delete in the later one"
    );

    cleanup(&dir);
}

#[test]
fn duckdb_reads_pinkdb_written_snappy_dictionary_files() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-compressed-to-duckdb");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut empty = PinkDb::new();
    empty
        .execute("CREATE TABLE main.t (i BIGINT, v VARCHAR);")
        .unwrap();
    lake.write_from_store(&empty).unwrap();
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Bigint),
            ("v".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    for i in 0..20_000i64 {
        table
            .append_row(vec![
                Value::Bigint(i),
                Value::Varchar(format!("region-{}", i % 16)),
            ])
            .unwrap();
    }
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(table));
    lake.commit(&commit).unwrap();
    let file = lake
        .list_files(Some("main.t"), None, false)
        .unwrap()
        .into_iter()
        .find(|file| file.rows == Some(20_000))
        .expect("the appended table has one 20,000-row data file")
        .path;
    drop(lake);
    // `list_files` paths are lake-relative, and the lake root is the data
    // directory the attachment was created with.
    let absolute = data.join(&file);
    let absolute = absolute.to_str().unwrap();

    // `parquet_metadata` reports what DuckDB itself parses out of the footer,
    // so this checks the file DuckDB sees, not pinkdb's own reader.
    let script = format!(
        "SELECT DISTINCT compression FROM parquet_metadata('{absolute}');\n\
         SELECT DISTINCT encodings FROM parquet_metadata('{absolute}') \
         WHERE path_in_schema = 'v';\n\
         SELECT count(*), sum(i) FROM read_parquet('{absolute}');\n"
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
    assert_eq!(rows[0], "SNAPPY", "pinkdb writes Snappy pages by default");
    assert!(
        rows[1].contains("DICTIONARY"),
        "the low-cardinality column must be dictionary-encoded: {}",
        rows[1]
    );
    assert_eq!(rows[2], "20000,199990000");
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
    // data path, so the lake root is the data directory itself.
    let data = copied.join("data");

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

/// A `DOUBLE[3]` column a Parquet lake holds survives DuckDB attaching the
/// lake: DuckLake has no fixed-size list type, so the catalog records the
/// column as a `list` plus a child `element` column (exactly what DuckDB
/// writes for its own `INTEGER[]`) and the Parquet file holds the three-level
/// list group.
///
/// DuckDB's DuckLake reader finds those columns *by field id* (`field_id` in
/// the Parquet schema; DuckDB's own lake files carry the `ducklake_column`
/// id there), so a pinkdb data file has to record them — see
/// `crate::metadata::store::planned_field_ids`. Without the ids DuckDB typed
/// the column `double[]` but read NULL for every element.
#[test]
fn duckdb_reads_pinkdb_array_columns_through_the_lake() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();

    let dir = scratch("pink-array");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);

    let mut store = PinkDb::new();
    store
        .execute("CREATE TABLE main.t (id INTEGER, v DOUBLE[3]);")
        .unwrap();
    for (id, array) in [
        (1, Some(vec![1.0, 2.0, 3.0])),
        (2, None),
        (3, Some(vec![-0.5, 0.0, 0.5])),
    ] {
        let value = match array {
            Some(values) => Value::Array(Arc::new(
                pinkdb::array::ArrayValue::from_values(
                    ArrayElem::Double,
                    3,
                    &values.into_iter().map(Value::Double).collect::<Vec<_>>(),
                )
                .unwrap(),
            )),
            None => Value::Null,
        };
        store
            .table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![Value::Integer(id), value])
            .unwrap();
    }
    assert_eq!(lake.write_from_store(&store).unwrap(), 2);
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT id, v IS NULL, CAST(v[1] AS DOUBLE), CAST(v[3] AS DOUBLE) FROM l.t ORDER BY id;\n\
         SELECT count(*) FROM l.t WHERE CAST(v[2] AS DOUBLE) = 0.0;\n",
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
            "1,false,1.0,3.0".to_string(),
            "2,true,NULL,NULL".to_string(),
            "3,false,-0.5,0.5".to_string(),
            "1".to_string(),
        ],
        "DuckDB reading a pinkdb ARRAY column through the lake"
    );

    cleanup(&dir);
}

/// The lake itself holds the vectors exactly: reopening the lake pinkdb wrote
/// rebuilds `DOUBLE[3]` (from the `pinkdb_column` side row, since the DuckLake
/// type name is only `list`) and reads every element back.
#[test]
fn a_pinkdb_lake_round_trips_array_columns() {
    let dir = scratch("pink-array-round-trip");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);

    let mut store = PinkDb::new();
    store
        .execute("CREATE TABLE main.t (id INTEGER, v DOUBLE[3]);")
        .unwrap();
    for (id, array) in [
        (1, Some(vec![1.0, 2.0, 3.0])),
        (2, None),
        (3, Some(vec![-0.5, 0.0, 0.5])),
    ] {
        let value = match array {
            Some(values) => Value::Array(Arc::new(
                pinkdb::array::ArrayValue::from_values(
                    ArrayElem::Double,
                    3,
                    &values.into_iter().map(Value::Double).collect::<Vec<_>>(),
                )
                .unwrap(),
            )),
            None => Value::Null,
        };
        store
            .table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![Value::Integer(id), value])
            .unwrap();
    }
    lake.write_from_store(&store).unwrap();
    drop(lake);

    let lake = Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT typeof(v) FROM t LIMIT 1")[0][0].to_string(),
        "DOUBLE[3]"
    );
    assert_eq!(count(&mut db, "SELECT sum(len(v)) FROM t"), 6);
    assert_eq!(
        query(&mut db, "SELECT v FROM t WHERE id = 3")[0][0].to_string(),
        "[-0.5, 0.0, 0.5]"
    );
    cleanup(&dir);
}

/// `field_id`s stay right after pinkdb alters the table: a column added later
/// takes the next catalog id (its ARRAY child one more), and a dropped one
/// leaves its id behind, so DuckDB still finds both array columns.
#[test]
fn duckdb_reads_arrays_after_pinkdb_added_and_dropped_columns() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("pink-array-alter");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);

    let doubles = |values: &[f64]| {
        Value::Array(Arc::new(
            pinkdb::array::ArrayValue::from_values(
                ArrayElem::Double,
                3,
                &values
                    .iter()
                    .copied()
                    .map(Value::Double)
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        ))
    };
    let floats = |values: &[f32]| {
        Value::Array(Arc::new(
            pinkdb::array::ArrayValue::from_values(
                ArrayElem::Float,
                3,
                &values
                    .iter()
                    .copied()
                    .map(|x| Value::Double(f64::from(x)))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        ))
    };
    let mut store = PinkDb::new();
    store
        .execute("CREATE TABLE main.t (id INTEGER, s VARCHAR, v DOUBLE[3]);")
        .unwrap();
    for (id, s, v) in [
        (1, Some("a"), Some([1.0, 2.0, 3.0])),
        (2, None, None),
        (3, Some("c"), Some([-0.5, 0.0, 0.5])),
    ] {
        store
            .table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![
                Value::Integer(id),
                s.map(|s| Value::Varchar(s.to_string()))
                    .unwrap_or(Value::Null),
                v.map(|v| doubles(&v)).unwrap_or(Value::Null),
            ])
            .unwrap();
    }
    lake.write_from_store(&store).unwrap();

    // A later version adds a 4-byte vector column and drops the VARCHAR one.
    store
        .execute("ALTER TABLE main.t ADD COLUMN f FLOAT[3];")
        .unwrap();
    store.execute("ALTER TABLE main.t DROP COLUMN s;").unwrap();
    store
        .table_mut_in("main", "t")
        .unwrap()
        .append_row(vec![
            Value::Integer(4),
            doubles(&[9.0, 8.0, 7.0]),
            floats(&[0.5, -0.5, 1.5]),
        ])
        .unwrap();
    lake.write_from_store(&store).unwrap();
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT id, CAST(v[1] AS DOUBLE), CAST(v[3] AS DOUBLE), f IS NULL, \
                CAST(f[2] AS DOUBLE) FROM l.t ORDER BY id;\n",
        catalog_path.display(),
        data.display()
    );
    let rows = match run_duckdb(&binary, &script) {
        Ok(rows) => rows,
        Err(message) if message.contains("xtension") => {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    };
    assert_eq!(
        rows,
        vec![
            "1,1.0,3.0,true,NULL".to_string(),
            "2,NULL,NULL,true,NULL".to_string(),
            "3,-0.5,0.5,true,NULL".to_string(),
            "4,9.0,7.0,false,-0.5".to_string(),
        ],
        "DuckDB after pinkdb added and dropped columns"
    );
    cleanup(&dir);
}

/// The other direction: a lake DuckDB wrote. DuckDB cannot create a
/// fixed-size ARRAY column in DuckLake at all ("unsupported type FLOAT[3]"),
/// so the closest it can write is a variable-length LIST. Pink attaches such
/// a lake but refuses to read the column, naming the wave that adds LIST
/// support — never a silently wrong ARRAY.
#[test]
fn a_duckdb_list_column_is_refused_with_the_followup_wave() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    let dir = scratch("duckdb-list");
    let catalog_path = dir.join("catalog.db");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS d (DATA_PATH '{}');\n\
         CREATE TABLE d.li (id INTEGER, v INTEGER[]);\n\
         INSERT INTO d.li VALUES (1, [1, 2, 3]), (2, NULL);\n",
        catalog_path.display(),
        data.display()
    );
    if let Err(message) = run_duckdb(&binary, &script) {
        if message.contains("extension") || message.contains("Extension") {
            println!("skip DuckDB interop test: {message}");
            cleanup(&dir);
            return;
        }
        panic!("{message}");
    }

    let catalog = format!("sqlite:{}", catalog_path.display());
    let lake = Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    let Err(error) = lake.load(None) else {
        panic!("a variable-length LIST column must not load as an ARRAY");
    };
    let text = error.to_string();
    assert!(
        text.contains("107a2"),
        "the refusal must name the follow-up wave: {text}"
    );

    cleanup(&dir);
}

// ===================== wave 132: DuckDB writes, pinkdb reads =================

/// The lake the reviewer's repro writes, plus a second table, a MERGE, an
/// added column with a default and later inserts. Each statement is one
/// snapshot, so the snapshot ids are 0..=13.
const INTEROP_STATEMENTS: &str = "\
CREATE TABLE l.t (a INTEGER, b VARCHAR);
INSERT INTO l.t VALUES (1, 'x'), (2, 'y');
INSERT INTO l.t SELECT CAST(range AS INTEGER), 'r' || range FROM range(10, 1010);
INSERT INTO l.t SELECT CAST(range AS INTEGER), 'r' || range FROM range(1010, 2010);
INSERT INTO l.t SELECT CAST(range AS INTEGER), 'r' || range FROM range(2010, 3010);
DELETE FROM l.t WHERE a % 100 = 0;
UPDATE l.t SET b = 'upd' WHERE a = 11;
CREATE TABLE l.u (id INTEGER, v VARCHAR);
INSERT INTO l.u VALUES (1, 'a'), (2, 'b');
MERGE INTO l.u USING (VALUES (2, 'b2'), (4, 'd')) AS s(id, v) ON l.u.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
ALTER TABLE l.t ADD COLUMN c INTEGER DEFAULT 7;
INSERT INTO l.t (a, b) SELECT CAST(range AS INTEGER), 'r' || range FROM range(3010, 3210);
INSERT INTO l.u VALUES (5, 'e');
";

/// (table, summed column, dumped columns, row order, snapshot it appears in)
const SNAPSHOT_TABLES: &[(&str, &str, &str, &str, i64)] =
    &[("t", "a", "a, b, c", "a", 1), ("u", "id", "id, v", "id", 8)];

fn write_interop_lake(binary: &Path, attach: &str, data: &Path) -> bool {
    let script = format!(
        "LOAD ducklake;\n\
         ATTACH '{attach}' AS l (DATA_PATH '{}');\n\
         {INTEROP_STATEMENTS}",
        data.display()
    );
    match run_duckdb(binary, &script) {
        Ok(_) => true,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            false
        }
        Err(message) => panic!("{message}"),
    }
}

/// DuckDB's own answers at one snapshot, one `count,<n>,<sum>` line per table
/// followed by one `row,…` line per dumped row.
fn duckdb_snapshot_lines(
    binary: &Path,
    setup: &str,
    attach: &str,
    data: &Path,
    snapshot: i64,
    tables: &[(&str, &str, &str, &str, i64)],
) -> Vec<String> {
    let mut selects = String::new();
    for (table, sum, columns, order, _) in tables {
        selects.push_str(&format!(
            "SELECT 'count', count(*), COALESCE(sum({sum}), 0) FROM l.{table} \
             AT (VERSION => {snapshot});\n\
             SELECT 'row', {columns} FROM l.{table} AT (VERSION => {snapshot}) \
             ORDER BY {order};\n"
        ));
    }
    let script = format!(
        "LOAD ducklake;\n\
         {setup}\
         ATTACH '{attach}' AS l (DATA_PATH '{}');\n\
         {selects}",
        data.display()
    );
    run_duckdb(binary, &script)
        .unwrap_or_else(|message| panic!("DuckDB at snapshot {snapshot}: {message}"))
        .into_iter()
        // `LOAD` echoes `true` on this DuckDB build; it is not a query row.
        .filter(|line| line != "true")
        .collect()
}

fn pink_snapshot_lines(
    lake: &Lake,
    version: u64,
    tables: &[(&str, &str, &str, &str, i64)],
) -> Vec<String> {
    let mut db = lake
        .load(Some(version))
        .unwrap_or_else(|e| panic!("pink load of version {version}: {e}"));
    let mut out = Vec::new();
    for (table, sum, columns, order, _) in tables {
        let totals = query(
            &mut db,
            &format!("SELECT count(*), COALESCE(sum({sum}), 0) FROM {table}"),
        );
        out.push(format!(
            "count,{},{}",
            csv_cell(&totals[0][0]),
            csv_cell(&totals[0][1])
        ));
        for row in query(
            &mut db,
            &format!("SELECT {columns} FROM {table} ORDER BY {order}"),
        ) {
            out.push(format!(
                "row,{}",
                row.iter().map(csv_cell).collect::<Vec<_>>().join(",")
            ));
        }
    }
    out
}

fn csv_cell(value: &Value) -> String {
    value.to_string()
}

/// Every DuckLake snapshot must agree with pinkdb: the row count, the sum of
/// one numeric column and the full ordered dump (which also covers inlined
/// rows, deletes, the UPDATE's re-inserted row and the added default column).
fn check_every_snapshot(binary: &Path, attach: &str, data: &Path, lake: &Lake) {
    let versions = lake.versions().unwrap();
    let script = format!(
        "LOAD ducklake;\n\
         ATTACH '{attach}' AS l (DATA_PATH '{}');\n\
         SELECT snapshot_id FROM ducklake_snapshots('l') ORDER BY 1;\n",
        data.display()
    );
    let ids: Vec<i64> = match run_duckdb(binary, &script) {
        Ok(lines) => lines.iter().map(|line| line.parse().unwrap()).collect(),
        Err(message) => {
            if message.contains("extension") || message.contains("Extension") {
                println!("skip DuckDB interop test: {message}");
                return;
            }
            panic!("{message}");
        }
    };
    assert_eq!(ids.len(), 14, "the script makes snapshots 0..=13: {ids:?}");
    assert_eq!(
        ids.len(),
        versions.len(),
        "pink versions {versions:?} vs DuckLake snapshots {ids:?}"
    );
    for (snapshot, version) in ids.iter().zip(&versions) {
        assert_eq!(
            *version,
            u64::try_from(*snapshot).unwrap() + 1,
            "pink version = snapshot_id + 1"
        );
        let active: Vec<_> = SNAPSHOT_TABLES
            .iter()
            .filter(|table| *snapshot >= table.4)
            // `c` only exists after the ALTER TABLE at snapshot 11.
            .map(|table| {
                if table.0 == "t" && *snapshot < 11 {
                    (table.0, table.1, "a, b", table.3, table.4)
                } else {
                    *table
                }
            })
            .collect();
        check_snapshot(binary, "", attach, data, lake, *snapshot, &active);
    }
}

/// One snapshot's rows from both sides, line for line: DuckDB's answer and
/// pinkdb's (pink version = snapshot id + 1).
fn check_snapshot(
    binary: &Path,
    setup: &str,
    attach: &str,
    data: &Path,
    lake: &Lake,
    snapshot: i64,
    tables: &[(&str, &str, &str, &str, i64)],
) {
    let version = u64::try_from(snapshot).unwrap() + 1;
    let expected = duckdb_snapshot_lines(binary, setup, attach, data, snapshot, tables);
    let actual = pink_snapshot_lines(lake, version, tables);
    if expected != actual {
        let first = expected
            .iter()
            .zip(&actual)
            .position(|(a, b)| a != b)
            .unwrap_or(expected.len().min(actual.len()));
        panic!(
            "snapshot {snapshot} (pink version {version}): first difference at line {first}\n\
             duckdb: {:?}\n\
             pinkdb: {:?}\n\
             lines: duckdb {}, pinkdb {}",
            expected.get(first),
            actual.get(first),
            expected.len(),
            actual.len()
        );
    }
}

#[cfg(feature = "catalog-postgres")]
/// Every snapshot of a lake a test wrote itself: each table's tuple carries the
/// snapshot that created it, and `snapshots` is how many the script makes.
fn check_snapshots(
    binary: &Path,
    setup: &str,
    attach: &str,
    data: &Path,
    lake: &Lake,
    tables: &[(&str, &str, &str, &str, i64)],
    snapshots: usize,
) {
    let script = format!(
        "LOAD ducklake;\n\
         {setup}\
         ATTACH '{attach}' AS l (DATA_PATH '{}');\n\
         SELECT snapshot_id FROM ducklake_snapshots('l') ORDER BY 1;\n",
        data.display()
    );
    let ids: Vec<i64> = run_duckdb(binary, &script)
        .unwrap_or_else(|message| panic!("DuckDB snapshot list: {message}"))
        .iter()
        // `LOAD` echoes `true` on this DuckDB build; it is not a query row.
        .filter(|line| line.as_str() != "true")
        .map(|line| line.parse().unwrap())
        .collect();
    assert_eq!(
        ids.len(),
        snapshots,
        "the script makes snapshots 0..={}: {ids:?}",
        snapshots - 1
    );
    let versions = lake.versions().unwrap();
    assert_eq!(
        ids.len(),
        versions.len(),
        "pink versions {versions:?} vs DuckLake snapshots {ids:?}"
    );
    for snapshot in ids {
        let active: Vec<_> = tables
            .iter()
            .filter(|table| snapshot >= table.4)
            .copied()
            .collect();
        check_snapshot(binary, setup, attach, data, lake, snapshot, &active);
    }
}

#[test]
fn a_duckdb_sqlite_lake_matches_pinkdb_at_every_snapshot() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("duckdb-sqlite-snapshots");
    let catalog_path = dir.join("cat.sqlite");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:sqlite:{}", catalog_path.display());
    if !write_interop_lake(&binary, &attach, &data) {
        cleanup(&dir);
        return;
    }

    let catalog = format!("sqlite:{}", catalog_path.display());
    let lake = Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    check_every_snapshot(&binary, &attach, &data, &lake);
    check_read_functions(&lake, &binary, &attach, &data);
    drop(lake);
    cleanup(&dir);
}

/// DuckDB records absolute file paths when its data path does not prefix-match
/// the normalized file path, as with a `//` in `DATA_PATH`. Those paths must
/// still resolve under the lake's data root.
#[test]
fn a_duckdb_lake_with_a_double_slash_data_path_reads_every_snapshot() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("duckdb-double-slash");
    let catalog_path = dir.join("cat.sqlite");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let data_arg = PathBuf::from(format!("{}//data/", dir.display()));
    let attach = format!("ducklake:sqlite:{}", catalog_path.display());
    if !write_interop_lake(&binary, &attach, &data_arg) {
        cleanup(&dir);
        return;
    }

    let catalog = format!("sqlite:{}", catalog_path.display());
    // No explicit DATA: the catalog's `//` data path is used as recorded.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    check_every_snapshot(&binary, &attach, &data_arg, &lake);
    drop(lake);
    cleanup(&dir);
}

#[test]
fn an_absolute_catalog_path_outside_the_data_root_is_refused() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    let Some(sqlite3) = binary_on_path("sqlite3") else {
        println!("skip outside-root test: sqlite3 is not on PATH");
        return;
    };
    register().unwrap();
    let dir = scratch("duckdb-outside-root");
    let catalog_path = dir.join("cat.sqlite");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:sqlite:{}", catalog_path.display());
    if !write_interop_lake(&binary, &attach, &data) {
        cleanup(&dir);
        return;
    }

    let edited = dir.join("outside.sqlite");
    std::fs::copy(&catalog_path, &edited).unwrap();
    let outside = dir.join("elsewhere").join("x.parquet");
    let update = format!(
        "UPDATE ducklake_data_file SET path = '{}', path_is_relative = 0 \
         WHERE data_file_id = (SELECT min(data_file_id) FROM ducklake_data_file);",
        outside.display()
    );
    let status = Command::new(sqlite3)
        .arg(&edited)
        .arg(update)
        .status()
        .unwrap();
    assert!(status.success(), "sqlite3 could not edit the catalog copy");

    let catalog = format!("sqlite:{}", edited.display());
    let lake = Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    let error = lake.load(None).expect_err("an outside path must fail");
    let text = error.to_string();
    assert!(
        text.contains("outside the lake's data path"),
        "the refusal must name the fence: {text}"
    );
    assert!(
        text.contains(&outside.display().to_string()),
        "the refusal must name the path: {text}"
    );
    assert!(
        text.contains(&data.display().to_string()),
        "the refusal must name the data root: {text}"
    );
    drop(lake);
    cleanup(&dir);
}

#[cfg(feature = "catalog-postgres")]
struct TestPostgres {
    dir: PathBuf,
}

/// One throwaway server at a time: the tests that start one share port 55433.
#[cfg(feature = "catalog-postgres")]
static POSTGRES: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(feature = "catalog-postgres")]
fn postgres_guard() -> std::sync::MutexGuard<'static, ()> {
    POSTGRES.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(feature = "catalog-postgres")]
impl TestPostgres {
    /// A throwaway local server: initdb under `$TMPDIR`, port 55433, trust on
    /// 127.0.0.1. Stopped and deleted by `Drop`.
    fn start() -> Option<Self> {
        let Some(initdb) = binary_on_path("initdb") else {
            println!("skip Postgres interop test: initdb is not on PATH");
            return None;
        };
        let dir = scratch("postgres-server");
        let output = Command::new(&initdb)
            .arg("-D")
            .arg(&dir)
            .args(["-U", "postgres", "--auth=trust", "--no-sync"])
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "initdb failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let pg_ctl = binary_on_path("pg_ctl").expect("pg_ctl is next to initdb");
        let log = dir.join("server.log");
        let output = Command::new(&pg_ctl)
            .arg("-D")
            .arg(&dir)
            .args(["-o", "-p 55433 -h 127.0.0.1", "-l"])
            .arg(&log)
            .args(["-w", "start"])
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        if !output.status.success() {
            let log = std::fs::read_to_string(&log).unwrap_or_default();
            cleanup(&dir);
            panic!(
                "pg_ctl start on port 55433 failed: {}\n{log}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let createdb = binary_on_path("createdb").expect("createdb is next to initdb");
        let output = Command::new(createdb)
            .args(["-h", "127.0.0.1", "-p", "55433", "-U", "postgres", "duck"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "createdb failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Some(Self { dir })
    }

    fn conninfo(&self) -> &'static str {
        "host=127.0.0.1 port=55433 user=postgres dbname=duck"
    }

    /// An additional empty database on the same server, for a test that needs a
    /// catalog of its own.
    #[cfg_attr(not(feature = "catalog-postgres"), allow(dead_code))]
    fn create_database(&self, name: &str) {
        let createdb = binary_on_path("createdb").expect("createdb is next to initdb");
        let output = Command::new(createdb)
            .args(["-h", "127.0.0.1", "-p", "55433", "-U", "postgres", name])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "createdb {name} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(feature = "catalog-postgres")]
impl Drop for TestPostgres {
    fn drop(&mut self) {
        if let Some(pg_ctl) = binary_on_path("pg_ctl") {
            let _ = Command::new(pg_ctl)
                .arg("-D")
                .arg(&self.dir)
                .args(["-m", "immediate", "stop"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        cleanup(&self.dir);
    }
}

fn binary_on_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(name))
            .find(|path| path.is_file())
    })
}

#[cfg(feature = "catalog-postgres")]
#[test]
fn a_duckdb_postgres_conninfo_lake_matches_pinkdb_at_every_snapshot() {
    let _postgres = postgres_guard();
    let Some(binary) = duckdb_binary() else {
        return;
    };
    let Some(server) = TestPostgres::start() else {
        return;
    };
    register().unwrap();
    let dir = scratch("duckdb-postgres-snapshots");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:postgres:{}", server.conninfo());
    if !write_interop_lake(&binary, &attach, &data) {
        cleanup(&dir);
        return;
    }

    // The catalog location is the libpq conninfo form, not a URL.
    let catalog = format!("postgres:{}", server.conninfo());
    let lake = Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    check_every_snapshot(&binary, &attach, &data, &lake);
    check_read_functions(&lake, &binary, &attach, &data);
    drop(lake);
    cleanup(&dir);
}

// ===== Wave 132c: DuckLake's functions as result tables =====================

use pink_lake::functions::{DuckLakeCall, FnArg, call_read, call_write, output_schema};

/// DuckDB's `-json` rows for the last statement of `script`, as text: `NULL`
/// is `"<null>"`, everything else is the value the SQL cast it to.
fn duckdb_json_rows(binary: &Path, script: &str, columns: &[&str]) -> Vec<Vec<String>> {
    let output = Command::new(binary)
        .args(["-no-agent", "-json", "-c", script])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "DuckDB failed: {}\n{script}",
        String::from_utf8_lossy(&output.stderr)
    );
    // One statement's rows arrive either as one JSON array or as one object
    // per line, depending on the DuckDB build.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let text = stdout.trim();
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = match serde_json::from_str(text) {
        Ok(rows) => rows,
        Err(error) => text
            .lines()
            .map(|line| line.trim().trim_end_matches([',', ']', '[', ' ']))
            .filter(|line| line.starts_with('{'))
            .map(|line| {
                serde_json::from_str(line).unwrap_or_else(|_| panic!("{error}: {line}\n{text}"))
            })
            .collect(),
    };
    rows.iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| match row.get(*column) {
                    None | Some(serde_json::Value::Null) => "<null>".to_string(),
                    Some(serde_json::Value::String(text)) => text.clone(),
                    Some(other) => other.to_string(),
                })
                .collect()
        })
        .collect()
}

#[cfg(feature = "catalog-postgres")]
fn pinkdb_sql_rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<String>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|value| match value {
                        Value::Null => "<null>".to_string(),
                        other => other.to_string(),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

#[cfg(feature = "catalog-postgres")]
/// Compare one SQL statement's rows between DuckDB (`l`) and pinkdb (`w`).
fn check_sql(
    db: &mut PinkDb,
    binary: &Path,
    prefix: &str,
    duck_sql: &str,
    pink_sql: &str,
    columns: &[&str],
    sorted: bool,
) {
    let mut theirs = duckdb_json_rows(binary, &format!("{prefix}{duck_sql}"), columns);
    let mut ours = pinkdb_sql_rows(db, pink_sql);
    if sorted {
        theirs.sort();
        ours.sort();
    }
    assert_eq!(ours, theirs, "{pink_sql}");
}

fn pinkdb_rows(table: &DataTable) -> Vec<Vec<String>> {
    (0..table.num_rows())
        .map(|row| {
            (0..table.schema().len())
                .map(
                    |column| match table.get(row, column).expect("decode a chunk") {
                        None | Some(Value::Null) => "<null>".to_string(),
                        Some(Value::Varchar(text)) => text,
                        Some(Value::Boolean(flag)) => flag.to_string(),
                        Some(Value::Integer(number)) => number.to_string(),
                        Some(Value::Bigint(number)) => number.to_string(),
                        Some(Value::Timestamp(micros)) => micros.to_string(),
                        Some(other) => panic!("unexpected value {other}"),
                    },
                )
                .collect()
        })
        .collect()
}

fn attach_script(attach: &str, data: &Path) -> String {
    format!(
        "LOAD ducklake;\n\
         ATTACH '{attach}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n",
        data.display()
    )
}

/// Runs `call` through both DuckDB (`script`) and pink-lake and compares every
/// value. The `CAST` in `script` gives DuckDB's own text for each column, so
/// the comparison also pins the declared schema to DuckDB's column names.
fn check_function(lake: &Lake, binary: &Path, script: &str, call: DuckLakeCall, sorted: bool) {
    let schema = output_schema(lake, &call)
        .unwrap_or_else(|error| panic!("{}: {error}", call.name))
        .unwrap_or_else(|| panic!("{} is not a DuckLake function", call.name));
    let columns: Vec<&str> = schema.iter().map(|(name, _)| name.as_str()).collect();
    let theirs = duckdb_json_rows(binary, script, &columns);
    let ours = call_read(lake, &call).unwrap_or_else(|error| panic!("{}: {error}", call.name));
    let mut ours = pinkdb_rows(&ours);
    let mut theirs = theirs;
    if sorted {
        ours.sort();
        theirs.sort();
    }
    assert_eq!(ours, theirs, "{}: rows", call.name);
}

fn call(name: &str, positional: Vec<FnArg>) -> DuckLakeCall {
    DuckLakeCall::new(name, positional)
}

fn text(value: &str) -> FnArg {
    FnArg::Text(value.to_string())
}

/// The three row-level change functions over every snapshot range, compared
/// value by value with DuckDB on the same lake. Ranges start at the first
/// snapshot that holds the table: a change read needs it at `end`. The table's
/// columns can change with the schema, so ranges are grouped by the output
/// columns pinkdb reports and each group is one DuckDB query.
fn check_change_functions(lake: &Lake, binary: &Path, attach: &str, data: &Path, table: &str) {
    let prefix = attach_script(attach, data);
    let versions = lake.versions().unwrap();
    let first = versions
        .iter()
        .copied()
        .find(|version| {
            lake.load(Some(*version))
                .is_ok_and(|mut db| db.execute(&format!("SELECT count(*) FROM {table}")).is_ok())
        })
        .unwrap_or(versions[0]);
    let ids: Vec<i64> = versions
        .iter()
        .filter(|version| **version >= first)
        .map(|version| *version as i64 - 1)
        .collect();
    for name in [
        "ducklake_table_changes",
        "ducklake_table_insertions",
        "ducklake_table_deletions",
    ] {
        // One output column set: our rows and the DuckDB selects for it.
        type Group = (Vec<Vec<String>>, Vec<String>);
        let mut groups: BTreeMap<Vec<String>, Group> = BTreeMap::new();
        for (index, start) in ids.iter().enumerate() {
            for end in ids[index..].iter() {
                let rows = call_read(
                    lake,
                    &call(
                        name,
                        vec![
                            text("l"),
                            text("main"),
                            text(table),
                            FnArg::Int(*start),
                            FnArg::Int(*end),
                        ],
                    ),
                )
                .unwrap_or_else(|error| panic!("{name} over {start}..={end}: {error}"));
                let columns: Vec<String> = rows
                    .schema()
                    .iter()
                    .map(|(column, _)| column.to_string())
                    .collect();
                let group = groups.entry(columns).or_default();
                for mut row in pinkdb_rows(&rows) {
                    row.insert(0, end.to_string());
                    row.insert(0, start.to_string());
                    group.0.push(row);
                }
                let casts = rows
                    .schema()
                    .iter()
                    .map(|(column, _)| format!("CAST({column} AS VARCHAR) AS {column}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                // A table function's arguments must be constants, so every
                // range is spelled out; the inner SELECT keeps an alias from
                // colliding with a column of the same name.
                group.1.push(format!(
                    "SELECT {start} AS start_snapshot, {end} AS end_snapshot, {casts} \
                     FROM (SELECT * FROM {name}('l', 'main', '{table}', {start}, {end}))"
                ));
            }
        }
        for (columns, (mut ours, selects)) in groups {
            ours.sort();
            let mut names = vec!["start_snapshot".to_string(), "end_snapshot".to_string()];
            names.extend(columns);
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            let mut theirs = duckdb_json_rows(
                binary,
                &format!("{prefix}{}", selects.join("\nUNION ALL\n")),
                &names,
            );
            theirs.sort();
            assert_eq!(
                ours, theirs,
                "{name}: every snapshot range of main.{table} over {names:?}"
            );
        }
    }
}

/// The lake every function is compared on: inlined rows, two flushed Parquet
/// files, a DELETE, UPDATEs, an added column, a second table and a commit
/// message with extra info.
const FUNCTION_STATEMENTS: &[&str] = &[
    "CALL ducklake_set_commit_message('l', 'duckdb-author', 'first commit');",
    "CREATE TABLE l.t (i INTEGER, s VARCHAR);",
    "INSERT INTO l.t VALUES (1, 'a'), (2, 'b');",
    "CALL ducklake_flush_inlined_data('l');",
    "INSERT INTO l.t VALUES (3, 'c'), (4, 'd');",
    "CALL ducklake_flush_inlined_data('l');",
    "DELETE FROM l.t WHERE i = 2;",
    "UPDATE l.t SET s = 'u' WHERE i = 3;",
    "ALTER TABLE l.t ADD COLUMN c INTEGER;",
    "UPDATE l.t SET c = i * 10;",
    "CREATE TABLE l.t2 (x BIGINT);",
    "INSERT INTO l.t2 VALUES (7), (8);",
    "INSERT INTO l.t VALUES (5, 'e', 50);",
    "CALL ducklake_set_commit_message('l', 'duckdb-author', 'last commit', extra_info := 'extra');",
    "INSERT INTO l.t VALUES (6, 'f', 60);",
];

/// A lake whose flushes merge rows of several snapshots: an inlined `UPDATE`
/// ends one inlined row and inserts another at a later snapshot, so the flush
/// writes a data file whose rows span two snapshots (`partial_max`) and a
/// delete file whose positions each carry their own snapshot id. DuckLake's
/// change queries then attribute every row and every position to its own
/// snapshot instead of the file's.
const PARTIAL_STATEMENTS: &[&str] = &[
    "CREATE TABLE l.t (i INTEGER, s VARCHAR);",
    "INSERT INTO l.t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    "UPDATE l.t SET s = 'B' WHERE i = 2;",
    "CALL ducklake_flush_inlined_data('l');",
    "UPDATE l.t SET s = 'C' WHERE i = 3;",
    "DELETE FROM l.t WHERE i = 1;",
    "INSERT INTO l.t VALUES (4, 'd');",
    "CALL ducklake_flush_inlined_data('l');",
];

fn write_duckdb_partial_lake(binary: &Path, prefix: &str) -> bool {
    let script = format!("{prefix}{}", PARTIAL_STATEMENTS.join("\n"));
    match run_duckdb(binary, &script) {
        Ok(_) => true,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            eprintln!("skipping: DuckDB has no ducklake extension: {message}");
            false
        }
        Err(message) => panic!("writing the partial-file lake: {message}"),
    }
}

fn write_duckdb_function_lake(binary: &Path, prefix: &str) -> bool {
    let script = format!("{prefix}{}", FUNCTION_STATEMENTS.join("\n"));
    match run_duckdb(binary, &script) {
        Ok(_) => true,
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB interop test: {message}");
            false
        }
        Err(message) => panic!("{message}"),
    }
}

/// Every read function and argument form, compared to DuckDB on the same lake.
fn check_read_functions(lake: &Lake, binary: &Path, attach: &str, data: &Path) {
    let prefix = attach_script(attach, data);
    // A snapshot id both tools can resolve, with data files behind it.
    let version = lake
        .snapshots()
        .map(|snapshots| snapshots.iter().map(|info| info.version).max().unwrap_or(4) as i64 - 3)
        .unwrap_or(1)
        .max(1);

    check_function(
        lake,
        binary,
        &format!(
            "{prefix}SELECT CAST(snapshot_id AS VARCHAR) AS snapshot_id, \
             CAST(epoch_us(snapshot_time) AS VARCHAR) AS snapshot_time, \
             CAST(schema_version AS VARCHAR) AS schema_version, \
             CAST(changes AS VARCHAR) AS changes, author, commit_message, \
             commit_extra_info FROM ducklake_snapshots('l')"
        ),
        call("ducklake_snapshots", vec![text("l")]),
        true,
    );
    check_function(
        lake,
        binary,
        &format!("{prefix}SELECT CAST(id AS VARCHAR) AS id FROM ducklake_current_snapshot('l')"),
        call("ducklake_current_snapshot", vec![text("l")]),
        false,
    );
    check_function(
        lake,
        binary,
        &format!(
            "{prefix}SELECT CAST(id AS VARCHAR) AS id FROM ducklake_last_committed_snapshot('l')"
        ),
        call("ducklake_last_committed_snapshot", vec![text("l")]),
        false,
    );
    check_function(
        lake,
        binary,
        &format!(
            "{prefix}SELECT table_name, CAST(schema_id AS VARCHAR) AS schema_id, \
             CAST(table_id AS VARCHAR) AS table_id, CAST(table_uuid AS VARCHAR) AS table_uuid, \
             CAST(file_count AS VARCHAR) AS file_count, \
             CAST(file_size_bytes AS VARCHAR) AS file_size_bytes, \
             CAST(delete_file_count AS VARCHAR) AS delete_file_count, \
             CAST(delete_file_size_bytes AS VARCHAR) AS delete_file_size_bytes \
             FROM ducklake_table_info('l') ORDER BY table_name"
        ),
        call("ducklake_table_info", vec![text("l")]),
        false,
    );
    let list_files = |extra: &str| {
        format!(
            "{prefix}SELECT data_file, CAST(data_file_size_bytes AS VARCHAR) AS data_file_size_bytes, \
             CAST(data_file_footer_size AS VARCHAR) AS data_file_footer_size, \
             CAST(data_file_encryption_key AS VARCHAR) AS data_file_encryption_key, \
             delete_file, CAST(delete_file_size_bytes AS VARCHAR) AS delete_file_size_bytes, \
             CAST(delete_file_footer_size AS VARCHAR) AS delete_file_footer_size, \
             CAST(delete_file_encryption_key AS VARCHAR) AS delete_file_encryption_key \
             FROM ducklake_list_files('l', 't'{extra}) ORDER BY data_file, delete_file"
        )
    };
    check_function(
        lake,
        binary,
        &list_files(""),
        call("ducklake_list_files", vec![text("l"), text("t")]),
        // DuckDB orders the rows by data file id; the manifest's order is its
        // own, so the comparison is order-insensitive.
        true,
    );
    let mut named = call("ducklake_list_files", vec![text("l"), text("t")]);
    named.named.push(("schema".to_string(), text("main")));
    named
        .named
        .push(("snapshot_version".to_string(), FnArg::Int(version)));
    check_function(
        lake,
        binary,
        &list_files(&format!(
            ", schema := 'main', snapshot_version := {version}"
        )),
        named,
        true,
    );
    check_function(
        lake,
        binary,
        &format!(
            "{prefix}SELECT option_name, description, value, scope, scope_entry \
             FROM ducklake_options('l') ORDER BY option_name"
        ),
        call("ducklake_options", vec![text("l")]),
        false,
    );

    // pinkdb reports its own extension version and its catalog backend's name,
    // so only the other columns can be compared value for value.
    let settings = call_read(lake, &call("ducklake_settings", vec![text("l")])).unwrap();
    let ours = pinkdb_rows(&settings);
    let theirs = duckdb_json_rows(
        binary,
        &format!(
            "{prefix}SELECT catalog_type, extension_version, data_path FROM ducklake_settings('l')"
        ),
        &["catalog_type", "extension_version", "data_path"],
    );
    assert_eq!(ours.len(), 1);
    assert_eq!(theirs.len(), 1);
    assert_eq!(ours[0][0], theirs[0][0], "catalog_type");
    assert_eq!(ours[0][2], theirs[0][2], "data_path");
    assert!(
        ours[0][1].contains('.'),
        "extension_version: {}",
        ours[0][1]
    );
}

#[test]
fn ducklake_functions_match_duckdb_on_a_duckdb_lake() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("functions-duckdb-lake");
    let catalog_path = dir.join("catalog.db");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:sqlite:{}", catalog_path.display());
    if !write_duckdb_function_lake(&binary, &attach_script(&attach, &data)) {
        cleanup(&dir);
        return;
    }
    let lake = Lake::open_with_catalog(
        &format!("sqlite:{}", catalog_path.display()),
        Some(data.to_str().unwrap()),
    )
    .unwrap();
    check_read_functions(&lake, &binary, &attach, &data);
    check_change_functions(&lake, &binary, &attach, &data, "t");
    drop(lake);
    cleanup(&dir);
}

/// A DuckDB-written lake whose flushed files carry per-row snapshot ids: the
/// change functions must report each row and each delete position at the
/// snapshot DuckDB does, over every range.
#[test]
fn ducklake_change_functions_match_duckdb_on_a_partial_file_lake() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("partial-lake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:sqlite:{}", dir.join("catalog.db").display());
    if !write_duckdb_partial_lake(&binary, &attach_script(&attach, &data)) {
        cleanup(&dir);
        return;
    }
    let lake = Lake::open_with_catalog(
        &format!("sqlite:{}", dir.join("catalog.db").display()),
        Some(data.to_str().unwrap()),
    )
    .unwrap();
    check_change_functions(&lake, &binary, &attach, &data, "t");
    drop(lake);
    cleanup(&dir);
}

/// The same function lake on a throwaway Postgres catalog: DuckDB's change
/// functions and pinkdb's must agree there too, flush snapshots included.
#[cfg(feature = "catalog-postgres")]
#[test]
fn ducklake_functions_match_duckdb_on_a_duckdb_postgres_lake() {
    let _postgres = postgres_guard();
    let Some(binary) = duckdb_binary() else {
        return;
    };
    let Some(server) = TestPostgres::start() else {
        return;
    };
    register().unwrap();
    let dir = scratch("functions-duckdb-postgres");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:postgres:{}", server.conninfo());
    if !write_duckdb_function_lake(&binary, &attach_script(&attach, &data)) {
        cleanup(&dir);
        return;
    }
    let lake = Lake::open_with_catalog(
        &format!("postgres:{}", server.conninfo()),
        Some(data.to_str().unwrap()),
    )
    .unwrap();
    check_read_functions(&lake, &binary, &attach, &data);
    check_change_functions(&lake, &binary, &attach, &data, "t");
    drop(lake);
    cleanup(&dir);
}

/// A pinkdb-written lake: inlined rows, a flushed Parquet file, a second table
/// and a commit message.
/// A lake with two tables and inlined rows only; `flush` decides whether the
/// first commit's rows go to a Parquet file (so a reader sees both a file and
/// inlined rows), which also keeps pinkdb's inlined bookkeeping out of a
/// later `ducklake_flush_inlined_data` comparison.
fn write_pinkdb_function_lake(dir: &Path, flush: bool) -> Lake {
    let catalog = format!("sqlite:{}", dir.join("catalog.db").display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(20);
    let mut empty = PinkDb::new();
    empty
        .execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    empty.execute("CREATE TABLE main.t2 (x BIGINT);").unwrap();
    lake.write_from_store(&empty).unwrap();

    let table_schema = || {
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ]
    };
    let mut first = DataTable::new("t".to_string(), table_schema()).unwrap();
    for i in 0..3 {
        first
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(first));
    assert!(lake.commit(&commit).unwrap().committed);
    if flush {
        assert!(lake.flush_inlined_data(Some("main.t")).unwrap().committed);
    }

    let mut second = DataTable::new("t".to_string(), table_schema()).unwrap();
    for i in 3..5 {
        second
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    let mut t2 =
        DataTable::new("t2".to_string(), vec![("x".to_string(), DataType::Bigint)]).unwrap();
    t2.append_row(vec![Value::Bigint(7)]).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(second));
    commit.append("main.t2", Arc::new(t2));
    assert!(lake.commit(&commit).unwrap().committed);

    let mut last =
        DataTable::new("t2".to_string(), vec![("x".to_string(), DataType::Bigint)]).unwrap();
    last.append_row(vec![Value::Bigint(8)]).unwrap();
    lake.set_commit_message(
        Some("pinkdb-author".to_string()),
        Some("pinkdb commit".to_string()),
    );
    let mut commit = Commit::new();
    commit.append("main.t2", Arc::new(last));
    assert!(lake.commit(&commit).unwrap().committed);
    lake
}

#[test]
fn ducklake_functions_match_duckdb_on_a_pinkdb_lake() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    // Unflushed: the rows sit in the inlined table, so the change functions
    // read them there on both sides.
    let dir = scratch("functions-pinkdb-lake");
    let lake = write_pinkdb_function_lake(&dir, false);
    let data = dir.join("data");
    let attach = format!("ducklake:sqlite:{}", dir.join("catalog.db").display());
    check_read_functions(&lake, &binary, &attach, &data);
    check_change_functions(&lake, &binary, &attach, &data, "t");
    drop(lake);
    cleanup(&dir);

    // Flushed: pinkdb closes the inlined rows with an end snapshot and writes
    // them into a Parquet file. DuckDB reads that catalog as deletions of the
    // inlined rows plus insertions of the file's rows, and pinkdb must agree.
    let dir = scratch("functions-pinkdb-flushed");
    let lake = write_pinkdb_function_lake(&dir, true);
    let data = dir.join("data");
    let attach = format!("ducklake:sqlite:{}", dir.join("catalog.db").display());
    check_read_functions(&lake, &binary, &attach, &data);
    check_change_functions(&lake, &binary, &attach, &data, "t");
    drop(lake);
    cleanup(&dir);
}

/// `flush_inlined_data` and `expire_snapshots` on two identical copies of one
/// lake: pinkdb's rows must equal DuckDB's, and DuckDB must read pinkdb's
/// flushed lake exactly as it reads its own copy.
#[test]
fn ducklake_write_functions_match_duckdb_on_copies() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("functions-write");
    let lake = write_pinkdb_function_lake(&dir, false);
    drop(lake);
    // The copies live outside `dir`: copying a tree into its own subdirectory
    // would make `copy_tree` recurse into what it is writing.
    let ours_dir = scratch("functions-write-ours");
    let theirs_dir = scratch("functions-write-theirs");
    copy_tree(&dir, &ours_dir);
    copy_tree(&dir, &theirs_dir);
    // The copies' catalogs still point at the original data directory, so the
    // lake is opened through its own catalog path with the copy's data.
    let catalog_of = |copy: &Path| format!("sqlite:{}", copy.join("catalog.db").display());

    let mut ours = Lake::open_with_catalog(
        &catalog_of(&ours_dir),
        Some(ours_dir.join("data").to_str().unwrap()),
    )
    .unwrap();
    let inlined = ours
        .table_info()
        .unwrap()
        .iter()
        .map(|info| info.inlined_rows)
        .sum::<u64>();
    assert!(inlined > 0, "the copy must start with inlined rows");
    let mut flush = call("ducklake_flush_inlined_data", vec![text("l")]);
    flush.named.push(("schema_name".to_string(), text("main")));
    flush.named.push(("table_name".to_string(), text("t")));
    let flushed = call_write(&mut ours, &flush).unwrap();
    assert!(flushed.num_rows() > 0, "{flushed:?}");
    drop(ours);

    let table = format!(
        "{prefix}SELECT CAST(rows_flushed AS VARCHAR) AS rows_flushed, schema_name, table_name \
         FROM ducklake_flush_inlined_data('l', schema_name := 'main', table_name := 't')",
        prefix = attach_script(
            &format!(
                "ducklake:sqlite:{}",
                theirs_dir.join("catalog.db").display()
            ),
            &theirs_dir.join("data")
        )
    );
    let mut theirs = duckdb_json_rows(
        &binary,
        &table,
        &["rows_flushed", "schema_name", "table_name"],
    );
    let ours_rows = pinkdb_rows(&flushed);
    let mut ours_rows: Vec<Vec<String>> = ours_rows
        .into_iter()
        .map(|row| vec![row[2].clone(), row[0].clone(), row[1].clone()])
        .collect();
    ours_rows.sort();
    theirs.sort();
    assert_eq!(ours_rows, theirs, "ducklake_flush_inlined_data rows");

    // Both lakes must now read the same, whichever tool flushed them.
    let read = |copy: &Path| {
        let script = format!(
            "{}SELECT i, s FROM l.t ORDER BY i",
            attach_script(
                &format!("ducklake:sqlite:{}", copy.join("catalog.db").display()),
                &copy.join("data")
            )
        );
        run_duckdb(&binary, &script).unwrap()
    };
    assert_eq!(read(&ours_dir), read(&theirs_dir), "flushed lake contents");

    // `expire_snapshots` reports the victims' snapshot rows; a dry run on
    // copies shows the same ids without mutating anything.
    let mut ours = Lake::open_with_catalog(
        &catalog_of(&ours_dir),
        Some(ours_dir.join("data").to_str().unwrap()),
    )
    .unwrap();
    let preview = call_write(&mut ours, &{
        let mut call = call("ducklake_expire_snapshots", vec![text("l")]);
        call.named
            .push(("older_than".to_string(), FnArg::Timestamp(i64::MAX / 4)));
        call.named.push(("dry_run".to_string(), FnArg::Bool(true)));
        call
    })
    .unwrap();
    let mut ours_ids: Vec<String> = pinkdb_rows(&preview)
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    ours_ids.sort();

    let their_script = format!(
        "{prefix}SELECT CAST(snapshot_id AS VARCHAR) AS snapshot_id FROM ducklake_expire_snapshots('l', older_than := TIMESTAMPTZ '2500-01-01 00:00:00+00', dry_run := true)",
        prefix = attach_script(
            &format!(
                "ducklake:sqlite:{}",
                theirs_dir.join("catalog.db").display()
            ),
            &theirs_dir.join("data")
        )
    );
    let mut theirs_ids: Vec<String> = duckdb_json_rows(&binary, &their_script, &["snapshot_id"])
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    theirs_ids.sort();
    assert_eq!(ours_ids, theirs_ids, "ducklake_expire_snapshots ids");

    // A real run over an explicit version on both sides leaves the same
    // snapshots behind.
    let expired = call_write(&mut ours, &{
        let mut call = call("ducklake_expire_snapshots", vec![text("l")]);
        call.named
            .push(("versions".to_string(), FnArg::IntList(vec![2])));
        call
    })
    .unwrap();
    assert_eq!(expired.num_rows(), 1, "{expired:?}");
    drop(ours);
    let their_script = format!(
        "{prefix}CALL ducklake_expire_snapshots('l', versions := [2]);",
        prefix = attach_script(
            &format!(
                "ducklake:sqlite:{}",
                theirs_dir.join("catalog.db").display()
            ),
            &theirs_dir.join("data")
        )
    );
    run_duckdb(&binary, &their_script).unwrap();
    let remaining = |copy: &Path| {
        let script = format!(
            "{}SELECT CAST(snapshot_id AS VARCHAR) AS snapshot_id FROM ducklake_snapshots('l') ORDER BY snapshot_id",
            attach_script(
                &format!("ducklake:sqlite:{}", copy.join("catalog.db").display()),
                &copy.join("data")
            )
        );
        let mut ids: Vec<String> = duckdb_json_rows(&binary, &script, &["snapshot_id"])
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(
        remaining(&ours_dir),
        remaining(&theirs_dir),
        "surviving snapshots"
    );

    cleanup(&ours_dir);
    cleanup(&theirs_dir);
    cleanup(&dir);
}

/// Wrong arity, an unknown named argument and a snapshot that does not exist
/// are refused in DuckDB's words.
#[test]
fn ducklake_function_argument_errors_are_duckdb_like() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("functions-errors");
    let lake = write_pinkdb_function_lake(&dir, false);

    let error = |call: &DuckLakeCall| {
        call_read(&lake, call)
            .err()
            .unwrap_or_else(|| panic!("{} should be refused", call.name))
            .to_string()
    };
    let message = error(&call(
        "ducklake_table_changes",
        vec![text("l"), text("main")],
    ));
    assert!(
        message.starts_with("ducklake_table_changes: needs 5 positional argument(s), got 2; expected ducklake_table_changes(catalog, schema, table, start_snapshot, end_snapshot)"),
        "{message}"
    );
    let mut unknown = call("ducklake_snapshots", vec![text("l")]);
    unknown.named.push(("snapshot".to_string(), FnArg::Int(1)));
    let message = error(&unknown);
    assert!(
        message.contains("unknown named argument \"snapshot\""),
        "{message}"
    );
    let message = error(&call("ducklake_nope", vec![text("l")]));
    assert!(message.contains("is not a DuckLake function"), "{message}");
    let message = error(&call(
        "ducklake_table_changes",
        vec![
            text("l"),
            text("main"),
            text("t"),
            FnArg::Int(0),
            FnArg::Int(99),
        ],
    ));
    assert_eq!(
        message,
        "ducklake_table_changes: No snapshot found at version 99; expected ducklake_table_changes(catalog, schema, table, start_snapshot, end_snapshot)"
    );

    // DuckDB's own words for the same bad snapshot, to show the message the
    // port copies.
    let script = format!(
        "{}SELECT * FROM ducklake_table_changes('l', 'main', 't', 0, 99)",
        attach_script(
            &format!("ducklake:sqlite:{}", dir.join("catalog.db").display()),
            &dir.join("data")
        )
    );
    let output = Command::new(&binary)
        .args(["-no-agent", "-csv", "-c", &script])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No snapshot found at version 99"),
        "{stderr}"
    );

    drop(lake);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Wave 132b: stock DuckDB's catalog shape, then QuackLab's attach sequence.
// ---------------------------------------------------------------------------

/// The catalog's `ducklake_*` columns as DuckDB reports them for the catalog
/// attached as `probe`: one `table,column,type` line per column in declaration
/// order. DuckDB's per-table inlined-data tables are not part of the shape.
fn ducklake_columns(binary: &Path, attach: &str) -> Vec<String> {
    run_duckdb(
        binary,
        &format!(
            "{attach}\n\
             SELECT table_name || ',' || column_name || ',' || data_type\n\
               FROM information_schema.columns\n\
              WHERE table_catalog = 'probe'\n\
                AND table_name LIKE 'ducklake%'\n\
                AND table_name NOT LIKE 'ducklake_inlined_data_%'\n\
              ORDER BY table_name, ordinal_position;\n"
        ),
    )
    .unwrap_or_else(|message| panic!("DuckDB column dump: {message}"))
}

/// A catalog's tables carry DuckLake's columns and types, not pinkdb's: stock
/// DuckDB's SQLite and Postgres catalogs are the reference. Postgres is where
/// `uuid`, `BOOLEAN` and `timestamptz` differ from SQLite's affinities.
#[test]
fn stock_duckdb_and_pinkdb_write_the_same_catalog_tables() {
    let Some(binary) = duckdb_binary() else {
        return;
    };
    register().unwrap();
    let dir = scratch("ddl-parity");

    // SQLite: DuckDB writes the reference catalog, pink-lake its own.
    let duck_catalog = dir.join("duck.sqlite");
    let duck_data = dir.join("duck-data");
    let pink_catalog = dir.join("pink.sqlite");
    let pink_data = dir.join("pink-data");
    for path in [&duck_data, &pink_data] {
        std::fs::create_dir_all(path).unwrap();
    }
    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}/');\n\
         CREATE SCHEMA staging;\n\
         CREATE TABLE staging.t (i INTEGER);\n\
         INSERT INTO staging.t VALUES (1);\n",
        duck_catalog.display(),
        duck_data.display()
    );
    match run_duckdb(&binary, &script) {
        Ok(_) => {}
        Err(message) if message.contains("extension") || message.contains("Extension") => {
            println!("skip DuckDB DDL parity test: {message}");
            cleanup(&dir);
            return;
        }
        Err(message) => panic!("{message}"),
    }
    Lake::create_with_catalog(
        &format!("sqlite:{}", pink_catalog.display()),
        pink_data.to_str().unwrap(),
    )
    .unwrap();
    let duckdb_side = ducklake_columns(
        &binary,
        &format!(
            "LOAD sqlite;\nATTACH '{}' AS probe (TYPE SQLITE, READ_ONLY);\n",
            duck_catalog.display()
        ),
    );
    let pinkdb_side = ducklake_columns(
        &binary,
        &format!(
            "LOAD sqlite;\nATTACH '{}' AS probe (TYPE SQLITE, READ_ONLY);\n",
            pink_catalog.display()
        ),
    );
    assert!(
        duckdb_side.len() > 150,
        "too few catalog columns for a real comparison: {}",
        duckdb_side.len()
    );
    assert_eq!(
        pinkdb_side, duckdb_side,
        "the SQLite catalog columns differ from stock DuckDB's"
    );

    // Postgres: the same comparison with a server that has real types (only
    // when the Postgres catalog backend is built).
    #[cfg(feature = "catalog-postgres")]
    {
        let _postgres = postgres_guard();
        let Some(server) = TestPostgres::start() else {
            cleanup(&dir);
            return;
        };
        server.create_database("ddl_duck");
        server.create_database("ddl_pink");
        let duck_conninfo = "host=127.0.0.1 port=55433 user=postgres dbname=ddl_duck";
        let pink_conninfo = "host=127.0.0.1 port=55433 user=postgres dbname=ddl_pink";
        let duck_data = dir.join("pg-duck-data");
        let pink_data = dir.join("pg-pink-data");
        for path in [&duck_data, &pink_data] {
            std::fs::create_dir_all(path).unwrap();
        }
        let script = format!(
            "LOAD ducklake;\n\
         LOAD postgres_scanner;\n\
         ATTACH 'ducklake:postgres:{duck_conninfo}' AS l (DATA_PATH '{}/');\n\
         CREATE SCHEMA staging;\n\
         CREATE TABLE staging.t (i INTEGER);\n\
         INSERT INTO staging.t VALUES (1);\n",
            duck_data.display()
        );
        run_duckdb(&binary, &script)
            .unwrap_or_else(|message| panic!("DuckDB Postgres catalog: {message}"));
        Lake::create_with_catalog(
            &format!("postgres:{pink_conninfo}"),
            pink_data.to_str().unwrap(),
        )
        .unwrap();
        let duckdb_side = ducklake_columns(
            &binary,
            &format!(
                "LOAD postgres_scanner;\nATTACH '{duck_conninfo}' AS probe (TYPE POSTGRES, READ_ONLY);\n"
            ),
        );
        let pinkdb_side = ducklake_columns(
            &binary,
            &format!(
                "LOAD postgres_scanner;\nATTACH '{pink_conninfo}' AS probe (TYPE POSTGRES, READ_ONLY);\n"
            ),
        );
        assert!(
            duckdb_side
                .iter()
                .any(|line| line.to_lowercase().ends_with(",uuid\"")),
            "the uuid columns are missing from DuckDB's Postgres catalog"
        );
        assert_eq!(
            pinkdb_side, duckdb_side,
            "the Postgres catalog columns differ from stock DuckDB's"
        );
    }
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Wave 132b: QuackLab's own attach sequence, end to end.
// ---------------------------------------------------------------------------

#[cfg(feature = "catalog-postgres")]
mod quacklab_attach {
    use super::*;

    use pink_lake::{LakeCredentials, S3Config};

    /// The loads and S3 secret a script needs to read the mock-bucket data:
    /// the endpoint has no scheme, as MinIO/R2 configs give it.
    fn s3_setup(endpoint: &str, key: &str, secret: &str) -> String {
        format!(
            "LOAD httpfs;\n\
             CREATE OR REPLACE SECRET quacklab_s3 (TYPE S3, KEY_ID '{key}', SECRET '{secret}', \
               ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false);\n"
        )
    }

    /// Run a script in DuckDB, creating the S3 secret with the mock's
    /// credentials.
    fn duckdb_with_lake(
        binary: &Path,
        endpoint: &str,
        key: &str,
        secret: &str,
        data: &str,
        uri: &str,
        tail: &str,
    ) -> Result<Vec<String>, String> {
        // `LOAD` echoes `true` on this DuckDB build; it is not a query row.
        run_duckdb(
            binary,
            &format!(
                "LOAD ducklake;\n\
                 LOAD postgres_scanner;\n\
                 {setup}\
                 ATTACH 'ducklake:postgres:{uri}' AS warehouse (DATA_PATH '{data}/');\n\
                 {tail}",
                setup = s3_setup(endpoint, key, secret)
            ),
        )
        .map(|rows| rows.into_iter().filter(|row| row != "true").collect())
    }

    /// The lake's object-store credentials for the mock endpoint: what the
    /// engine builds from the `r2_lake` secret, for the snapshot reads below
    /// that need a [`Lake`] handle.
    fn lake_credentials(
        data: &str,
        endpoint: &str,
        key: &str,
        secret: &str,
        password: &str,
    ) -> LakeCredentials {
        let path = data
            .strip_prefix("s3://")
            .unwrap_or_else(|| panic!("the mock S3 data location is not an s3:// path: {data}"));
        let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
        LakeCredentials {
            s3: Some(
                S3Config::new(bucket)
                    .with_prefix(prefix)
                    .with_region("auto")
                    .with_endpoint(format!("http://{endpoint}"))
                    .with_path_style(true)
                    .allow_http(true)
                    .with_credentials(key, secret),
            ),
            postgres_password: Some(password.to_string()),
        }
    }

    #[test]
    fn quacklabs_attach_sequence_round_trips_with_duckdb() {
        let Some(binary) = duckdb_binary() else {
            return;
        };
        let _guard = postgres_guard();
        let Some(pg) = TestPostgres::start() else {
            return;
        };
        let mock = mock_s3::MockS3::start();
        let data = mock.url("warehouse", "quacklab");
        let data = data.trim_end_matches('/').to_string();
        let endpoint = mock.endpoint().trim_start_matches("http://").to_string();
        let key = mock.access_key_id();
        let secret = mock.secret_access_key();
        let uri = "postgres://postgres:hunter2@127.0.0.1:55433/duck?sslmode=disable";

        // 1. DuckDB creates the catalog: two schemas, three tables, an inlined
        //    insert, Parquet inserts (past the inlining limit) and a delete.
        let rows = match duckdb_with_lake(
            &binary,
            &endpoint,
            &key,
            &secret,
            &data,
            uri,
            "USE warehouse;\n\
             CREATE SCHEMA staging;\n\
             CREATE SCHEMA mart;\n\
             CREATE TABLE staging.t (i INTEGER, s VARCHAR);\n\
             INSERT INTO staging.t VALUES (1, 'a'), (2, 'b'), (3, 'c');\n\
             CREATE TABLE staging.x (i INTEGER, s VARCHAR);\n\
             INSERT INTO staging.x SELECT i, 'x' || i FROM range(100) t(i);\n\
             CREATE TABLE mart.big (i INTEGER, s VARCHAR);\n\
             INSERT INTO mart.big SELECT i, 'v' || i FROM range(200) t(i);\n\
             DELETE FROM mart.big WHERE i % 10 = 0;\n\
             SELECT count(*) FROM staging.t;\n\
             SELECT count(*) FROM staging.x;\n\
             SELECT count(*) FROM mart.big;\n",
        ) {
            Ok(rows) => rows,
            Err(message) if message.contains("extension") || message.contains("Extension") => {
                println!("skip QuackLab interop test: {message}");
                return;
            }
            Err(message) => panic!("{message}"),
        };
        assert_eq!(
            rows,
            vec!["3".to_string(), "100".to_string(), "180".to_string()]
        );

        // 2. pinkdb runs the same sequence, password in the URI, and writes
        //    into the catalog DuckDB made: a schema and table of its own, two
        //    Parquet files, a delete, and an append to a DuckDB table.
        pink_lake::database::register();
        pink_lake_parquet::register().unwrap();
        let dir = scratch("quacklab-pinkdb");
        let store = dir.join("warehouse.pink");
        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        db.execute(&format!(
            "CREATE OR REPLACE PERSISTENT SECRET r2_lake (TYPE S3, KEY_ID '{key}', SECRET '{secret}', \
             ENDPOINT '{endpoint}', URL_STYLE 'path', REGION 'auto', USE_SSL false);"
        ))
        .unwrap();
        db.execute(&format!(
            "ATTACH 'ducklake:postgres:{uri}' AS warehouse (DATA_PATH '{data}/');"
        ))
        .unwrap();
        db.execute("USE warehouse;").unwrap();
        assert_eq!(count(&mut db, "SELECT count(*) FROM staging.t;"), 3);
        assert_eq!(count(&mut db, "SELECT count(*) FROM mart.big;"), 180);
        // The attach password became a scoped secret, not statement text.
        let secret_rows = query(&mut db, "SELECT name, type FROM duckdb_secrets();");
        assert!(
            secret_rows.iter().any(|row| {
                row[0] == Value::Varchar("__attach_warehouse".into())
                    && row[1] == Value::Varchar("postgres".into())
            }),
            "{secret_rows:?}"
        );
        db.execute("CREATE SCHEMA pinkmade;").unwrap();
        db.execute("CREATE TABLE pinkmade.t (i INTEGER, s VARCHAR);")
            .unwrap();
        db.execute(
            "INSERT INTO pinkmade.t SELECT CAST(i AS INTEGER), 'p' || CAST(i AS VARCHAR) \
             FROM range(0, 500) t(i);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO pinkmade.t SELECT CAST(i AS INTEGER), 'q' || CAST(i AS VARCHAR) \
             FROM range(500, 900) t(i);",
        )
        .unwrap();
        db.execute("DELETE FROM pinkmade.t WHERE i % 7 = 0;")
            .unwrap();
        db.execute("INSERT INTO staging.x VALUES (1000, 'pink');")
            .unwrap();
        // 900 rows less the 129 multiples of 7 in 0..=899; the appended row.
        assert_eq!(count(&mut db, "SELECT count(*) FROM pinkmade.t;"), 771);
        assert_eq!(count(&mut db, "SELECT count(*) FROM staging.x;"), 101);
        drop(db);

        // The catalog says the written files are Parquet, which is why DuckDB
        // reads them below; a `.pink` file would be unreadable there.
        let formats = run_duckdb(
            &binary,
            &format!(
                "LOAD postgres_scanner;\n\
                 ATTACH '{conninfo}' AS cat (TYPE POSTGRES, READ_ONLY);\n\
                 SELECT DISTINCT f.file_format FROM cat.ducklake_data_file f\n\
                   JOIN cat.ducklake_table t ON t.table_id = f.table_id\n\
                   JOIN cat.ducklake_schema s ON s.schema_id = t.schema_id\n\
                  WHERE t.table_name = 't' AND s.schema_name = 'pinkmade';\n\
                 SELECT DISTINCT d.format FROM cat.ducklake_delete_file d\n\
                   JOIN cat.ducklake_table t ON t.table_id = d.table_id\n\
                   JOIN cat.ducklake_schema s ON s.schema_id = t.schema_id\n\
                  WHERE t.table_name = 't' AND s.schema_name = 'pinkmade';\n",
                conninfo = pg.conninfo()
            ),
        )
        .unwrap();
        assert_eq!(
            formats,
            vec!["parquet".to_string(), "parquet".to_string()],
            "pinkdb wrote files of another format into DuckLake's catalog"
        );

        // 3. DuckDB reads the same catalog: every table and every snapshot
        //    equal to pinkdb's view, `pinkmade.t` included.
        // A `Lake` handle takes the catalog as a conninfo plus the password
        // beside it (the engine keeps that password in `__attach_warehouse`);
        // DuckDB's own location carries it in the URI.
        let credentials = lake_credentials(&data, &endpoint, &key, &secret, "hunter2");
        let catalog = "postgres:host=127.0.0.1 port=55433 user=postgres dbname=duck";
        let attach = format!("ducklake:postgres:{uri}");
        let lake = Lake::open_with_catalog_credentials(catalog, Some(&data), &credentials).unwrap();
        let tables: &[(&str, &str, &str, &str, i64)] = &[
            ("staging.t", "i", "i, s", "i", 3),
            ("staging.x", "i", "i, s", "i", 5),
            ("mart.big", "i", "i, s", "i", 7),
            ("pinkmade.t", "i", "i, s", "i", 11),
        ];
        let setup = s3_setup(&endpoint, &key, &secret);
        check_snapshots(
            &binary,
            &setup,
            &attach,
            Path::new(&data),
            &lake,
            tables,
            16,
        );
        drop(lake);

        // 4. DuckDB writes again; pinkdb (reopened, after WAL replay) reads
        //    the new snapshot equal.
        let rows = match duckdb_with_lake(
            &binary,
            &endpoint,
            &key,
            &secret,
            &data,
            uri,
            "USE warehouse;\n\
             INSERT INTO pinkmade.t VALUES (9000, 'duck');\n\
             DELETE FROM mart.big WHERE i = 1;\n\
             INSERT INTO staging.t VALUES (4, 'd');\n\
             SELECT count(*) FROM pinkmade.t;\n\
             SELECT count(*) FROM mart.big;\n\
             SELECT count(*) FROM staging.t;\n",
        ) {
            Ok(rows) => rows,
            Err(message) => panic!("{message}"),
        };
        assert_eq!(
            rows,
            vec!["772".to_string(), "179".to_string(), "4".to_string()]
        );

        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        db.execute("USE warehouse;").unwrap();
        assert_eq!(count(&mut db, "SELECT count(*) FROM pinkmade.t;"), 772);
        assert_eq!(count(&mut db, "SELECT count(*) FROM mart.big;"), 179);
        assert_eq!(count(&mut db, "SELECT count(*) FROM staging.t;"), 4);
        drop(db);

        let lake = Lake::open_with_catalog_credentials(catalog, Some(&data), &credentials).unwrap();
        assert_eq!(lake.versions().unwrap().len(), 19);
        for snapshot in 16..19 {
            check_snapshot(
                &binary,
                &setup,
                &attach,
                Path::new(&data),
                &lake,
                snapshot,
                tables,
            );
        }
        drop(lake);
        cleanup(&dir);
    }
}

/// Wave 132d: every read function through SQL on a DuckDB-written, two-schema
/// Postgres lake, plus QuackLab's `CALL warehouse.set_option(...)` sequence and
/// a flush whose result the same session then reads.
#[cfg(feature = "catalog-postgres")]
#[test]
fn ducklake_functions_run_through_sql_and_match_duckdb() {
    let _postgres = postgres_guard();
    let Some(binary) = duckdb_binary() else {
        return;
    };
    let Some(server) = TestPostgres::start() else {
        return;
    };
    register().unwrap();
    pink_lake::database::register();
    let dir = scratch("functions-sql");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let attach = format!("ducklake:postgres:{}", server.conninfo());
    // pinkdb casts a TIMESTAMPTZ to DATE in UTC while DuckDB uses the session
    // zone, so the snapshot dates would otherwise depend on the machine's zone.
    let prefix = format!("{}SET TimeZone = 'UTC';\n", attach_script(&attach, &data));
    let setup = format!(
        "{prefix}         CREATE SCHEMA l.s2;
         CREATE TABLE l.s2.u (x INTEGER);
         INSERT INTO l.s2.u VALUES (7);
         CREATE TABLE l.t (i INTEGER, s VARCHAR);
         INSERT INTO l.t VALUES (1, 'a'), (2, 'b');
         CALL l.set_option('parquet_compression', 'zstd');
         CALL l.set_option('parquet_version', '2');
         CALL l.set_option('parquet_row_group_size', '1000');
         CALL l.set_option('data_inlining_row_limit', '0');
         CALL l.set_option('expire_older_than', '1 week');
         CALL l.set_option('data_inlining_row_limit', '5', table_name := 't');
"
    );
    if let Err(message) = run_duckdb(&binary, &setup) {
        if message.contains("extension") || message.contains("Extension") {
            println!("skip DuckDB interop test: {message}");
            return;
        }
        panic!("writing the two-schema lake: {message}");
    }

    let mut db = PinkDb::new();
    db.execute(&format!(
        "ATTACH 'ducklake:postgres:{}' AS w;",
        server.conninfo()
    ))
    .unwrap();
    // QuackLab's catalog-scoped form, with the same five options DuckDB set.
    for statement in [
        "CALL w.set_option('parquet_compression', 'zstd');",
        "CALL w.set_option('parquet_version', '2');",
        "CALL w.set_option('parquet_row_group_size', '1000');",
        "CALL w.set_option('data_inlining_row_limit', '0');",
        "CALL w.set_option('expire_older_than', '1 week');",
        "CALL w.set_option('data_inlining_row_limit', '5', table_name := 't');",
    ] {
        db.execute(statement).unwrap();
    }

    check_sql(
        &mut db,
        &binary,
        &prefix,
        // pinkdb maps DuckLake's TIMESTAMPTZ to a UTC TIMESTAMP (functions.rs), so
        // DuckDB's side takes the UTC date too: a local-zone cast differs between
        // midnight local time and midnight UTC.
        "SELECT CAST(snapshot_id AS VARCHAR) AS snapshot_id, \
         CAST(timezone('UTC', snapshot_time) AS DATE) AS snapshot_time, \
         CAST(schema_version AS VARCHAR) AS schema_version, CAST(changes AS VARCHAR) AS changes, \
         author, commit_message, commit_extra_info \
         FROM ducklake_snapshots('l') ORDER BY snapshot_id;",
        "SELECT CAST(snapshot_id AS VARCHAR) AS snapshot_id, \
         CAST(snapshot_time AS DATE) AS snapshot_time, \
         CAST(schema_version AS VARCHAR) AS schema_version, CAST(changes AS VARCHAR) AS changes, \
         author, commit_message, commit_extra_info \
         FROM ducklake_snapshots('w') ORDER BY snapshot_id;",
        &[
            "snapshot_id",
            "snapshot_time",
            "schema_version",
            "changes",
            "author",
            "commit_message",
            "commit_extra_info",
        ],
        false,
    );
    check_sql(
        &mut db,
        &binary,
        &prefix,
        "SELECT CAST(id AS VARCHAR) AS id FROM ducklake_current_snapshot('l');",
        "SELECT CAST(id AS VARCHAR) AS id FROM ducklake_current_snapshot('w');",
        &["id"],
        false,
    );
    check_sql(
        &mut db,
        &binary,
        &prefix,
        "SELECT CAST(id AS VARCHAR) AS id FROM ducklake_last_committed_snapshot('l');",
        "SELECT CAST(id AS VARCHAR) AS id FROM ducklake_last_committed_snapshot('w');",
        &["id"],
        false,
    );
    check_sql(
        &mut db,
        &binary,
        &prefix,
        "SELECT table_name, CAST(schema_id AS VARCHAR) AS schema_id, \
         CAST(table_id AS VARCHAR) AS table_id, CAST(table_uuid AS VARCHAR) AS table_uuid, \
         CAST(file_count AS VARCHAR) AS file_count, \
         CAST(file_size_bytes AS VARCHAR) AS file_size_bytes, \
         CAST(delete_file_count AS VARCHAR) AS delete_file_count, \
         CAST(delete_file_size_bytes AS VARCHAR) AS delete_file_size_bytes \
         FROM ducklake_table_info('l') ORDER BY table_name;",
        "SELECT table_name, CAST(schema_id AS VARCHAR) AS schema_id, \
         CAST(table_id AS VARCHAR) AS table_id, CAST(table_uuid AS VARCHAR) AS table_uuid, \
         CAST(file_count AS VARCHAR) AS file_count, \
         CAST(file_size_bytes AS VARCHAR) AS file_size_bytes, \
         CAST(delete_file_count AS VARCHAR) AS delete_file_count, \
         CAST(delete_file_size_bytes AS VARCHAR) AS delete_file_size_bytes \
         FROM ducklake_table_info('w') ORDER BY table_name;",
        &[
            "table_name",
            "schema_id",
            "table_id",
            "table_uuid",
            "file_count",
            "file_size_bytes",
            "delete_file_count",
            "delete_file_size_bytes",
        ],
        false,
    );
    check_sql(
        &mut db,
        &binary,
        &prefix,
        "SELECT data_file, CAST(data_file_size_bytes AS VARCHAR) AS data_file_size_bytes, \
         CAST(data_file_footer_size AS VARCHAR) AS data_file_footer_size, \
         CAST(data_file_encryption_key AS VARCHAR) AS data_file_encryption_key, delete_file, \
         CAST(delete_file_size_bytes AS VARCHAR) AS delete_file_size_bytes, \
         CAST(delete_file_footer_size AS VARCHAR) AS delete_file_footer_size, \
         CAST(delete_file_encryption_key AS VARCHAR) AS delete_file_encryption_key \
         FROM ducklake_list_files('l', 't') ORDER BY data_file, delete_file;",
        "SELECT data_file, CAST(data_file_size_bytes AS VARCHAR) AS data_file_size_bytes, \
         CAST(data_file_footer_size AS VARCHAR) AS data_file_footer_size, \
         CAST(data_file_encryption_key AS VARCHAR) AS data_file_encryption_key, delete_file, \
         CAST(delete_file_size_bytes AS VARCHAR) AS delete_file_size_bytes, \
         CAST(delete_file_footer_size AS VARCHAR) AS delete_file_footer_size, \
         CAST(delete_file_encryption_key AS VARCHAR) AS delete_file_encryption_key \
         FROM ducklake_list_files('w', 't') ORDER BY data_file, delete_file;",
        &[
            "data_file",
            "data_file_size_bytes",
            "data_file_footer_size",
            "data_file_encryption_key",
            "delete_file",
            "delete_file_size_bytes",
            "delete_file_footer_size",
            "delete_file_encryption_key",
        ],
        false,
    );
    let options = "SELECT option_name, value, scope, scope_entry \
         FROM ducklake_options('{}') WHERE option_name IN (\
         'parquet_compression', 'parquet_version', 'parquet_row_group_size', \
         'data_inlining_row_limit', 'expire_older_than') \
         ORDER BY option_name, scope, scope_entry;";
    check_sql(
        &mut db,
        &binary,
        &prefix,
        &options.replace("{}", "l"),
        &options.replace("{}", "w"),
        &["option_name", "value", "scope", "scope_entry"],
        false,
    );

    check_sql(
        &mut db,
        &binary,
        &prefix,
        "SELECT i, s FROM l.main.t ORDER BY i;",
        "SELECT i, s FROM w.main.t ORDER BY i;",
        &["i", "s"],
        false,
    );
}
