//! Per-file-key encryption (wave 103): every data/delete file is a PNKLAKE1
//! container with its own key in the catalog; metadata rows stay plaintext.
#![cfg(feature = "encryption")]

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, CommitSettings, EncryptionKey, LakeError};
use pinkdb::{DataTable, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_enc_{tag}_{}_{}",
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

fn key() -> EncryptionKey {
    EncryptionKey::from_raw([7u8; 32])
}

fn store(n: i32) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        .unwrap();
    for i in 0..n {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![
                Value::Integer(100 + i),
                Value::Varchar(format!("v{i}")),
            ])
            .unwrap();
    }
    db
}

fn sub(n: i32, s: &str) -> Arc<DataTable> {
    let mut t = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
        ],
    )
    .unwrap();
    t.append_row(vec![Value::Integer(n), Value::Varchar(s.to_string())])
        .unwrap();
    Arc::new(t)
}

fn rows(db: &mut PinkDb) -> Vec<(i32, Option<String>)> {
    match db
        .execute("SELECT i, s FROM t ORDER BY i;")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| {
                let i = match r[0] {
                    Value::Integer(i) => i,
                    ref other => panic!("{other:?}"),
                };
                let s = match &r[1] {
                    Value::Varchar(s) => Some(s.clone()),
                    Value::Null => None,
                    other => panic!("{other:?}"),
                };
                (i, s)
            })
            .collect(),
        other => panic!("{other:?}"),
    }
}

fn data_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    walk(&data_dir(dir).join("data"), &mut out);
    out.sort();
    out
}

fn catalog_sql(dir: &Path, sql: &str) {
    let path = dir.join("catalog.pink");
    let mut db = PinkDb::open_durable_with_key(
        &path.display().to_string(),
        &pinkdb::EncryptionKey::from_raw([7u8; 32]),
    )
    .unwrap();
    db.execute(sql).unwrap();
}

#[test]
fn keyed_roundtrip_is_ciphertext_and_fails_closed_without_the_key() {
    let dir = scratch("roundtrip");
    let mut lake = create_lake_with_key(&dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(4)).unwrap();
    drop(lake);

    let files = data_files(&dir);
    assert!(!files.is_empty());
    for file in &files {
        assert_eq!(
            &std::fs::read(file).unwrap()[..8],
            b"PNKLAKE1",
            "{}",
            file.display()
        );
    }
    let keys = pink_lake::catalog_encryption_keys(&catalog_url(&dir), Some(key())).unwrap();
    assert_eq!(keys.len(), files.len(), "one key per file");
    for k in keys {
        assert_eq!(k.len(), 32);
    }

    let err = open_lake(&dir).unwrap_err().to_string();
    assert!(err.contains("encrypted"), "{err}");
    let lake = open_lake_with_key(&dir, key()).unwrap();
    assert_eq!(rows(&mut lake.load(None).unwrap()), rows(&mut store(4)));

    // The catalog key column is the only way in: clearing it fails closed.
    catalog_sql(&dir, "UPDATE ducklake_data_file SET encryption_key = NULL;");
    let lake = open_lake_with_key(&dir, key()).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("key") || err.contains("encrypted"), "{err}");
    cleanup(&dir);
}

#[test]
fn tampered_and_swapped_containers_are_authenticated_errors() {
    let dir = scratch("swap");
    let mut lake = create_lake_with_key(&dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE a (i INTEGER); INSERT INTO a VALUES (1);")
        .unwrap();
    db.execute("CREATE TABLE b (i INTEGER); INSERT INTO b VALUES (2);")
        .unwrap();
    lake.write_from_store(&db).unwrap();
    drop(lake);

    let files = data_files(&dir);
    assert_eq!(files.len(), 2);
    let (a, b) = (
        std::fs::read(&files[0]).unwrap(),
        std::fs::read(&files[1]).unwrap(),
    );
    std::fs::write(&files[0], &b).unwrap();
    std::fs::write(&files[1], &a).unwrap();
    let lake = open_lake_with_key(&dir, key()).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(
        err.contains("authenticat")
            || err.contains("bound to")
            || err.contains("wrong key")
            || err.contains("cannot be read as")
            || err.contains("decrypt"),
        "{err}"
    );
    cleanup(&dir);
}

#[test]
fn plaintext_lakes_are_unaffected() {
    let dir = scratch("plain");
    let mut lake = create_lake(&dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(3)).unwrap();
    let rendered = lake.load(None).unwrap();
    assert_eq!(rows(&mut { rendered }), rows(&mut store(3)));
    let err = open_lake_with_key(&dir, key()).unwrap_err().to_string();
    assert!(
        err.contains("not encrypted") || err.contains("without a key"),
        "{err}"
    );
    cleanup(&dir);
}

#[test]
fn stats_fast_paths_on_an_encrypted_lake_decode_no_data_file() {
    let dir = scratch("fast");
    let mut lake = create_lake_with_key(&dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(3)).unwrap();
    let report = lake.attach(None).unwrap();
    assert!(report.encrypted);
    assert_eq!(report.requests, 0, "attach reads no data file");
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();
    let QueryResult::Selected { rows, .. } = db
        .execute("SELECT COUNT(*), MIN(i), MAX(i) FROM t;")
        .unwrap()
        .pop()
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(
        rows,
        vec![vec![
            Value::Bigint(3),
            Value::Integer(100),
            Value::Integer(102)
        ]]
    );
    cleanup(&dir);
}

#[test]
fn encrypted_lake_maintenance_round_trip() {
    let dir = scratch("maint");
    let mut lake = create_lake_with_key(&dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = store(1);
    lake.write_from_store(&db).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub(101, "v1"));
    lake.commit(&commit).unwrap();
    commit = Commit::new();
    commit.append("main.t", sub(102, "v2"));
    lake.commit(&commit).unwrap();
    let report = lake
        .merge_adjacent_files(None, &pink_lake::MergeSettings::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    for file in data_files(&dir) {
        assert_eq!(&std::fs::read(&file).unwrap()[..8], b"PNKLAKE1");
    }
    let mut reopened = open_lake_with_key(&dir, key()).unwrap();
    assert_eq!(rows(&mut reopened.load(None).unwrap()).len(), 3);
    reopened
        .expire_snapshots(&pink_lake::ExpireSettings {
            retain_last: Some(1),
            ..pink_lake::ExpireSettings::default()
        })
        .unwrap();
    reopened
        .delete_orphaned_files(&pink_lake::CleanupSettings {
            all: true,
            confirm: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(rows(&mut reopened.load(None).unwrap()).len(), 3);
    db.execute("SELECT 1;").unwrap();
    cleanup(&dir);
}

#[test]
fn commits_deletes_and_time_travel_read_through_the_decorator() {
    let dir = scratch("cdc");
    let mut lake = create_lake_with_key(&dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(3)).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub(200, "new"));
    lake.commit(&commit).unwrap();
    let file = lake
        .list_files(Some("main.t"), None, false)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .path;
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &file, &[0]);
    lake.commit_with_settings(&commit, &CommitSettings::default())
        .unwrap();
    let reopened = open_lake_with_key(&dir, key()).unwrap();
    let head = rows(&mut reopened.load(None).unwrap());
    assert_eq!(head.len(), 3);
    assert!(!head.iter().any(|(i, _)| *i == 100));
    assert!(head.iter().any(|(i, _)| *i == 200));
    let old = rows(&mut reopened.load(Some(2)).unwrap());
    assert_eq!(old.len(), 3);
    assert!(old.iter().any(|(i, _)| *i == 100));
    let err = open_lake(&dir).unwrap_err();
    assert!(matches!(err, LakeError::Encrypted { .. }), "{err:?}");
    cleanup(&dir);
}
