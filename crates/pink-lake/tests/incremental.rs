//! Incremental lake commits (catalog `format_version` 5 with schema records;
//! partitioning and zone maps are additive): append-only data
//! files, positional delete files (merge-on-read), per-commit delta
//! manifests, the row-level diff behind `.lake commit`, multi-file readers,
//! and DuckLake's writer-retry settings.
//!
//! Format-agnostic on purpose: the suite runs in a default (lean `.pink`)
//! build and in a `vortexlake` build, and the data-file extension follows
//! the build. Everything else — the catalog shape, the manifest invariants,
//! the commit semantics — is identical.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pink_lake::{
    save_store, Commit, CommitSettings, Lake, LakeError, WritableAttachment, FORMAT_VERSION,
};
use pinkdb::{DataTable, PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "vortexlake")]
const EXT: &str = "vortex";
#[cfg(not(feature = "vortexlake"))]
const EXT: &str = "pink";

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_incremental_{tag}_{}_{}",
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

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let target = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &target);
        } else {
            std::fs::copy(e.path(), &target).unwrap();
        }
    }
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

/// One table starting at 100 with `s = "v<row>"`.
fn store(rows: i64) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        .unwrap();
    for r in 0..rows {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![
                Value::Integer(r as i32 + 100),
                Value::Varchar(format!("v{r}")),
            ])
            .unwrap();
    }
    db
}

/// A same-schema sub-table holding `rows` (the shape `Commit::append` takes).
fn sub_table(rows: &[(i32, &str)]) -> Arc<DataTable> {
    let mut t = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
        ],
    )
    .unwrap();
    for (i, s) in rows {
        t.append_row(vec![Value::Integer(*i), Value::Varchar(s.to_string())])
            .unwrap();
    }
    Arc::new(t)
}

fn rows_of(db: &PinkDb) -> Vec<(i32, String)> {
    let t = db.table("t").unwrap();
    (0..t.num_rows())
        .map(|r| {
            let i = match t.get(r, 0).unwrap() {
                Value::Integer(i) => i,
                other => panic!("row {r}: {other:?}"),
            };
            let s = match t.get(r, 1).unwrap() {
                Value::Varchar(s) => s,
                other => panic!("row {r}: {other:?}"),
            };
            (i, s)
        })
        .collect()
}

fn expected_rows(store: &PinkDb) -> Vec<(i32, String)> {
    rows_of(store)
}

fn assert_same_rows(a: &[(i32, String)], b: &[(i32, String)], what: &str) {
    assert_eq!(a.len(), b.len(), "{what}: row count");
    assert_eq!(a, b, "{what}");
}

/// Every file under `dir/data`, recursively, with bytes + mtime. The lake's
/// metadata (catalog, snapshots, commits) legitimately changes per commit;
/// the contract under test is that **data and delete files** are immutable.
fn files_with_meta(dir: &Path) -> Vec<(PathBuf, Vec<u8>, std::time::SystemTime)> {
    fn walk(dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>, std::time::SystemTime)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                let bytes = std::fs::read(&p).unwrap();
                let mtime = std::fs::metadata(&p).unwrap().modified().unwrap();
                out.push((p, bytes, mtime));
            }
        }
    }
    let mut out = Vec::new();
    walk(&dir.join("data"), &mut out);
    out.sort();
    out
}

fn data_files(lake_dir: &Path) -> Vec<PathBuf> {
    files_with_meta(lake_dir)
        .into_iter()
        .map(|(p, _, _)| p)
        .filter(|p| p.extension().unwrap() != "pinkdel")
        .collect()
}

/// The data-file names of every table (sorted) — for "no published file was
/// rewritten or replaced" assertions.
fn file_list(lake_dir: &Path, _table: &str) -> Vec<String> {
    data_files(lake_dir)
        .into_iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect()
}

/// One table's entry in `catalog.json`, raw (the crate's `TableEntry` is
/// private; the published JSON is the contract).
fn lake_entry(lake_dir: &Path, key: &str) -> serde_json::Value {
    read_json(&lake_dir.join("catalog.json"))["tables"][key].clone()
}

/// Run one query and return its rows.
fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

#[test]
fn commit_appends_without_rewriting_and_the_manifest_records_the_ranges() {
    let dir = scratch("append");
    let lake_dir = dir.join("lake");
    let base = store(1000);
    save_store(&base, &lake_dir).unwrap();

    // Snapshot the published bytes: data files, catalog, snapshot.
    let before = files_with_meta(&lake_dir);
    let old_data = data_files(&lake_dir);
    assert_eq!(old_data.len(), 1);
    assert_eq!(old_data[0].extension().unwrap(), EXT);
    let old_file = old_data[0]
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();

    // Append 5 rows as one new data file.
    // Wave 68: inlining is on by default (DuckLake's 10-row limit); this
    // test pins the file-append shape, so it opts out.
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let appended = sub_table(&[
        (1100, "a"),
        (1101, "b"),
        (1102, "c"),
        (1103, "d"),
        (1104, "e"),
    ]);
    let mut commit = Commit::new();
    commit.append("main.t", appended);
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.version, 3);
    assert_eq!(report.base_version, 2);
    assert_eq!(report.retries, 0);
    assert!(report.committed);
    assert_eq!(report.data_files, 1);
    assert_eq!(report.delete_files, 0);
    assert_eq!(report.rows_added, 5);
    assert_eq!(report.rows_deleted, 0);

    // Zero bytes of the old version were rewritten: same data files, same
    // bytes, same mtimes; only new files appeared.
    let after = files_with_meta(&lake_dir);
    for (path, bytes, mtime) in &before {
        let found = after
            .iter()
            .find(|(p, _, _)| p == path)
            .unwrap_or_else(|| panic!("{} disappeared", path.display()));
        assert_eq!(&found.1, bytes, "{} was rewritten", path.display());
        assert_eq!(&found.2, mtime, "{} was touched", path.display());
    }
    let new_files: Vec<PathBuf> = after
        .iter()
        .map(|(p, _, _)| p.clone())
        .filter(|p| !before.iter().any(|(b, _, _)| b == p))
        .collect();
    assert!(
        new_files
            .iter()
            .any(|p| p.extension().unwrap() == EXT && p.parent().unwrap().ends_with("main.t")),
        "a new data file appeared: {new_files:?}"
    );

    // The manifest lists both files with per-file row counts and the logical
    // sum; the new file is a second entry, not a replacement.
    let catalog = read_json(&lake_dir.join("catalog.json"));
    assert_eq!(catalog["format_version"], FORMAT_VERSION);
    let entry = &catalog["tables"]["main.t"];
    let files = entry["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "{entry}");
    assert_eq!(files[0]["path"], old_file.as_str(), "{entry}");
    assert_eq!(files[0]["rows"], 1000, "{entry}");
    assert_eq!(files[1]["rows"], 5, "{entry}");
    assert!(files[1]["path"]
        .as_str()
        .unwrap()
        .ends_with(&format!(".{EXT}")));
    assert_eq!(entry["rows"], 1005, "{entry}");
    assert!(entry["deletes"].is_null(), "no deletes yet: {entry}");

    // The per-commit delta manifest lists exactly what this commit added.
    assert_eq!(lake.commits().unwrap(), vec![2, 3]);
    let delta = read_json(&lake_dir.join("commits/3.json"));
    assert_eq!(delta["version"], 3, "{delta}");
    assert_eq!(delta["base_version"], 2, "{delta}");
    let t = &delta["tables"]["main.t"];
    assert_eq!(t["rows_added"], 5, "{t}");
    assert_eq!(t["rows_deleted"], 0, "{t}");
    assert_eq!(t["data_files"].as_array().unwrap().len(), 1, "{t}");
    assert_eq!(t["data_files"][0]["rows"], 5, "{t}");
    assert!(t["delete_files"].is_null(), "{t}");

    // The union reads in listed order: the 1,000 old rows then the 5 new.
    let loaded = lake.load(None).unwrap();
    let rows = rows_of(&loaded);
    assert_same_rows(&rows[..1000], &expected_rows(&base), "base rows");
    assert_eq!(rows[1000], (1100, "a".to_string()));
    assert_eq!(rows[1004], (1104, "e".to_string()));
    // And the catalog is still byte-identical to its snapshot.
    assert_eq!(
        std::fs::read(lake_dir.join("catalog.json")).unwrap(),
        std::fs::read(lake_dir.join("snapshots/3.json")).unwrap(),
        "catalog.json must equal its snapshot byte for byte"
    );
    cleanup(&dir);
}

#[test]
fn deletes_are_positional_files_applied_on_read() {
    let dir = scratch("deletes");
    let lake_dir = dir.join("lake");
    let base = store(10);
    save_store(&base, &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    let data_file = data_files(&lake_dir)[0]
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let before = files_with_meta(&lake_dir);

    // DELETE row 3 and row 7 (one positional delete file).
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[3, 7]);
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.rows_deleted, 2, "{report:?}");
    assert_eq!(report.delete_files, 1);
    assert_eq!(report.version, 3);

    // Nothing published before was rewritten.
    for (path, bytes, mtime) in &before {
        let found = files_with_meta(&lake_dir)
            .into_iter()
            .find(|(p, _, _)| p == path)
            .unwrap();
        assert_eq!(found.1, *bytes, "{} was rewritten", path.display());
        assert_eq!(found.2, *mtime, "{} was touched", path.display());
    }

    let catalog = read_json(&lake_dir.join("catalog.json"));
    let deletes = catalog["tables"]["main.t"]["deletes"].as_array().unwrap();
    assert_eq!(deletes.len(), 1, "{catalog}");
    assert_eq!(deletes[0]["file"], data_file.as_str());
    assert_eq!(deletes[0]["count"], 2);
    assert_eq!(catalog["tables"]["main.t"]["rows"], 8, "{catalog}");

    let loaded = lake.load(None).unwrap();
    let rows = rows_of(&loaded);
    let mut want = expected_rows(&base);
    want.remove(7);
    want.remove(3);
    assert_same_rows(&rows, &want, "merge-on-read delete");

    // UPDATE = delete the old position + append the new row. Old files stay.
    let before = files_with_meta(&lake_dir);
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[5]);
    commit.append("main.t", sub_table(&[(1005, "updated")]));
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.rows_added, 1);
    assert_eq!(report.rows_deleted, 1);
    let rows = rows_of(&lake.load(None).unwrap());
    want.retain(|(i, _)| *i != 105); // the position that was deleted
    want.push((1005, "updated".to_string()));
    assert_same_rows(&rows, &want, "update = delete + append");
    for (path, bytes, mtime) in &before {
        let found = files_with_meta(&lake_dir)
            .into_iter()
            .find(|(p, _, _)| p == path)
            .unwrap();
        assert_eq!(found.1, *bytes, "{} was rewritten", path.display());
        assert_eq!(found.2, *mtime, "{} was touched", path.display());
    }

    // Old versions stay loadable, with their own delete sets.
    assert_eq!(rows_of(&lake.load(Some(2)).unwrap()).len(), 10);
    assert_eq!(rows_of(&lake.load(Some(3)).unwrap()).len(), 8);
    cleanup(&dir);
}

#[test]
fn commit_from_tables_diffs_inserts_updates_and_deletes_exactly() {
    let dir = scratch("diff");
    let lake_dir = dir.join("lake");
    let base = store(6);
    save_store(&base, &lake_dir).unwrap();
    let before = files_with_meta(&lake_dir);

    // A writable attachment materializes the base; mutate the store as any
    // DML would and commit the difference. Wave 68: inlining is on by
    // default, so this test opts out to pin the file-based diff shape.
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    assert_eq!(wa.base_version(), 2);
    let mut db = base;
    // INSERT (append), UPDATE a middle row, DELETE the first row.
    db.execute("INSERT INTO t VALUES (200, 'x'), (201, 'y');")
        .unwrap();
    db.execute("UPDATE t SET s = 'U2' WHERE i = 102;").unwrap();
    db.execute("DELETE FROM t WHERE i = 100;").unwrap();
    let report = wa
        .commit(
            &db,
            &CommitSettings {
                max_retry_count: 10,
                retry_backoff: 1.5,
                retry_wait_ms: 1,
            },
        )
        .unwrap();
    assert_eq!(report.version, 3);
    assert_eq!(report.rows_added, 3, "2 inserts + 1 updated row");
    assert_eq!(report.rows_deleted, 2, "1 deleted + 1 updated-away row");
    assert!(report.committed);

    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let loaded = rows_of(&lake.load(None).unwrap());
    let want = expected_rows(&db);
    assert_eq!(loaded.len(), want.len(), "row count");
    // SQL row order is not guaranteed, and merge-on-read surfaces the updated
    // row where the commit appended it: compare as multisets, then pin the
    // deterministic merge-on-read order (old rows in order, deletes dropped,
    // then the appended rows in commit order).
    {
        let mut a = loaded.clone();
        let mut b = want.clone();
        a.sort();
        b.sort();
        assert_eq!(a, b, "the diff published exactly the store's content");
    }
    assert_eq!(
        loaded,
        vec![
            (101, "v1".to_string()),
            (103, "v3".to_string()),
            (104, "v4".to_string()),
            (105, "v5".to_string()),
            (102, "U2".to_string()),
            (200, "x".to_string()),
            (201, "y".to_string()),
        ],
        "merge-on-read order: base minus deletes, then appended rows"
    );

    // The data files that existed before the commit are byte- and
    // mtime-identical; the commit added one data file and one delete file.
    for (path, bytes, mtime) in &before {
        let found = files_with_meta(&lake_dir)
            .into_iter()
            .find(|(p, _, _)| p == path)
            .unwrap();
        assert_eq!(found.1, *bytes, "{} was rewritten", path.display());
        assert_eq!(found.2, *mtime, "{} was touched", path.display());
    }
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(entry["files"].as_array().unwrap().len(), 2);
    assert_eq!(entry["deletes"].as_array().unwrap().len(), 1);
    assert_eq!(entry["deletes"][0]["count"], 2);
    assert_eq!(entry["rows"], 6 - 2 + 3, "{entry}");

    // A re-run with no further changes is a no-op (this is what makes crash
    // recovery idempotent: replaying a recovered store commits nothing).
    let report = wa.commit(&db, &CommitSettings::default()).unwrap();
    assert!(!report.committed, "{report:?}");
    assert_eq!(report.version, 3);

    // Undo the update and the insert by hand: the diff now sees the updated
    // row gone and an original value back. This second diff is the one that
    // proves the base was advanced in *lake* order: with a store-order base
    // the positional mapping would delete the wrong row.

    db.execute("UPDATE t SET s = 'v2' WHERE i = 102;").unwrap();
    db.execute("DELETE FROM t WHERE i = 200;").unwrap();
    db.execute("DELETE FROM t WHERE i = 201;").unwrap();
    let report = wa
        .commit(
            &db,
            &CommitSettings {
                retry_wait_ms: 1,
                ..CommitSettings::default()
            },
        )
        .unwrap();
    assert_eq!(report.rows_added, 1, "the restored value");
    assert_eq!(report.rows_deleted, 3, "the updated row + 2 inserts");
    let lake = Lake::open(&lake_dir).unwrap();
    let loaded = rows_of(&lake.load(None).unwrap());
    let mut a = loaded.clone();
    let mut b = expected_rows(&db);
    a.sort();
    b.sort();
    assert_eq!(a, b, "the second diff converges (as a multiset)");
    // Every remaining row survives exactly once.
    {
        let mut sorted = loaded.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), loaded.len(), "no duplicate rows");
    }
    cleanup(&dir);
}

/// The diff must be exact for the adversarial types: duplicate rows,
/// `-0.0` vs `0.0`, NaN payloads, NULLs, long strings.
#[test]
fn diff_is_bit_exact_for_doubles_and_duplicates() {
    let dir = scratch("diff_bits");
    let lake_dir = dir.join("lake");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (d DOUBLE, s VARCHAR);").unwrap();
    let nan = Value::Double(f64::from_bits(0x7ff8_0000_dead_beef));
    for v in [
        Value::Double(-0.0),
        Value::Double(0.0),
        nan.clone(),
        Value::Double(1.5),
        Value::Double(-0.0),
        Value::Null,
    ] {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![v, Value::Varchar("dup".to_string())])
            .unwrap();
    }
    save_store(&db, &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();

    // Delete one of the duplicated `-0.0` rows: only that position may go.
    db.table_mut("t")
        .unwrap()
        .delete_from_chunk(0, &[4])
        .unwrap();
    let report = wa
        .commit(
            &db,
            &CommitSettings {
                retry_wait_ms: 1,
                ..CommitSettings::default()
            },
        )
        .unwrap();
    assert_eq!(report.rows_deleted, 1, "{report:?}");
    assert_eq!(report.rows_added, 0, "{report:?}");
    assert_eq!(report.delete_files, 1);
    let lake = Lake::open(&lake_dir).unwrap();
    let t = lake.load(None).unwrap();
    let t = t.table("t").unwrap();
    assert_eq!(t.num_rows(), 5);
    let bits: Vec<Option<u64>> = (0..t.num_rows())
        .map(|r| match t.get(r, 0).unwrap() {
            Value::Double(x) => Some(x.to_bits()),
            Value::Null => None,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        bits,
        vec![
            Some((-0.0f64).to_bits()),
            Some(0.0f64.to_bits()),
            Some(0x7ff8_0000_dead_beef),
            Some(1.5f64.to_bits()),
            None,
        ],
        "values stayed bit-exact and order-preserving"
    );
    cleanup(&dir);
}

#[test]
fn racing_commits_retry_and_rebase() {
    let dir = scratch("race");
    let lake_dir = dir.join("lake");
    save_store(&store(3), &lake_dir).unwrap();

    // Two handles opened at the same version. Wave 68: inlining is on by
    // default; this test pins file appends and rebase behavior, so both
    // handles opt out.
    let mut a = Lake::open(&lake_dir).unwrap();
    let mut b = Lake::open(&lake_dir).unwrap();
    a.set_data_inlining_row_limit(0);
    b.set_data_inlining_row_limit(0);
    let fast = CommitSettings {
        max_retry_count: 10,
        retry_backoff: 1.0,
        retry_wait_ms: 1,
    };

    let mut commit_a = Commit::new();
    commit_a.append("main.t", sub_table(&[(200, "a")]));
    assert_eq!(a.commit_with_settings(&commit_a, &fast).unwrap().version, 3);

    // B is stale now: its base is v2 but the head is v3. The commit rebases
    // (one retry) and lands on top of A's file.
    let mut commit_b = Commit::new();
    commit_b.append("main.t", sub_table(&[(300, "b")]));
    let report = b.commit_with_settings(&commit_b, &fast).unwrap();
    assert_eq!(report.version, 4);
    assert_eq!(report.retries, 1, "the stale base was retried, not refused");
    assert_eq!(
        report.base_version, 2,
        "the origin the change was computed on"
    );

    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let rows = rows_of(&lake.load(None).unwrap());
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[3], (200, "a".to_string()));
    assert_eq!(rows[4], (300, "b".to_string()));
    let entry = &read_json(&lake_dir.join("catalog.json"))["tables"]["main.t"];
    assert_eq!(entry["files"].as_array().unwrap().len(), 3, "{entry}");

    // A fresh handle at v4 publishes (400, "c") on top: v5.
    let mut c = Lake::open(&lake_dir).unwrap();
    let mut commit_c = Commit::new();
    commit_c.append("main.t", sub_table(&[(400, "c")]));
    assert_eq!(c.commit_with_settings(&commit_c, &fast).unwrap().version, 5);

    // d opens at v5; another writer then publishes v6.
    let mut d = Lake::open(&lake_dir).unwrap();
    assert_eq!(d.current_version(), 5);
    let mut a2 = Lake::open(&lake_dir).unwrap();
    let mut commit_a2 = Commit::new();
    commit_a2.append("main.t", sub_table(&[(500, "a2")]));
    assert_eq!(
        a2.commit_with_settings(&commit_a2, &fast).unwrap().version,
        6
    );

    // A bounded budget reports the conflict instead of retrying.
    let mut commit_d = Commit::new();
    commit_d.append("main.t", sub_table(&[(600, "d")]));
    let err = d
        .commit_with_settings(
            &commit_d,
            &CommitSettings {
                max_retry_count: 0,
                retry_backoff: 1.0,
                retry_wait_ms: 1,
            },
        )
        .unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    assert!(
        err.to_string().contains("another writer published"),
        "{err}"
    );
    // The lake is untouched by the refused commit.
    let lake = Lake::open(&lake_dir).unwrap();
    assert_eq!(lake.current_version(), 6);
    assert_eq!(rows_of(&lake.load(None).unwrap()).len(), 7);

    // One retry is enough: the stale handle rebases and publishes its row.
    let report = d
        .commit_with_settings(
            &commit_d,
            &CommitSettings {
                max_retry_count: 1,
                retry_backoff: 1.0,
                retry_wait_ms: 1,
            },
        )
        .unwrap();
    assert_eq!(report.version, 7);
    assert_eq!(report.retries, 1);
    let rows = rows_of(&lake.load(Some(7)).unwrap());
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[7], (600, "d".to_string()));
    cleanup(&dir);
}

#[test]
fn commit_settings_pin_ducklake_defaults() {
    let defaults = CommitSettings::default();
    assert_eq!(defaults.max_retry_count, 10);
    assert_eq!(defaults.retry_backoff, 1.5);
    assert_eq!(defaults.retry_wait_ms, 100);
    // The schedule: wait, wait*backoff, wait*backoff^2, …
    assert_eq!(
        defaults.backoff_for(0),
        std::time::Duration::from_millis(100)
    );
    assert_eq!(
        defaults.backoff_for(1),
        std::time::Duration::from_millis(100)
    );
    assert_eq!(
        defaults.backoff_for(2),
        std::time::Duration::from_millis(150)
    );
    assert_eq!(
        defaults.backoff_for(3),
        std::time::Duration::from_millis(225)
    );
    let no_backoff = CommitSettings {
        max_retry_count: 2,
        retry_backoff: 1.0,
        retry_wait_ms: 7,
    };
    assert_eq!(
        no_backoff.backoff_for(1),
        std::time::Duration::from_millis(7)
    );
    assert_eq!(
        no_backoff.backoff_for(2),
        std::time::Duration::from_millis(7)
    );
}

#[test]
fn wrong_deltas_are_refused_never_silently_applied() {
    let dir = scratch("wrong_delta");
    let lake_dir = dir.join("lake");
    save_store(&store(5), &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    let data_file = data_files(&lake_dir)[0]
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();

    // A position outside the file.
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[5]);
    let err = lake.commit(&commit).unwrap_err();
    assert!(err.to_string().contains("go up to 5"), "{err}");
    assert!(err.to_string().contains("has 5 rows"), "{err}");

    // A delete from a file the table does not have (after a rebase, a
    // replaced file is the same class of refual).
    let mut commit = Commit::new();
    commit.delete_positions("main.t", "data/main.t/doesnotexist.pink", &[0]);
    let err = lake.commit(&commit).unwrap_err();
    assert!(
        err.to_string().contains("no longer part of the table"),
        "{err}"
    );

    // A commit against a table that does not exist.
    let mut commit = Commit::new();
    commit.delete_positions("main.nope", &data_file, &[0]);
    let err = lake.commit(&commit).unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    // Deleting one position twice (the second commit overlaps the first).
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[1]);
    lake.commit(&commit).unwrap();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[1, 2]);
    let err = lake.commit(&commit).unwrap_err();
    assert!(err.to_string().contains("already deleted"), "{err}");
    assert!(err.to_string().contains("recompute the change"), "{err}");

    // The refused commits changed nothing.
    assert_eq!(rows_of(&lake.load(None).unwrap()).len(), 4);
    assert_eq!(lake.current_version(), 3);
    cleanup(&dir);
}

#[test]
fn tampered_delete_files_and_manifests_are_clear_errors() {
    let dir = scratch("tampered");
    let lake_dir = dir.join("lake");
    save_store(&store(6), &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    let data_file = data_files(&lake_dir)[0]
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[2]);
    lake.commit(&commit).unwrap();

    let del = files_with_meta(&lake_dir)
        .into_iter()
        .map(|(p, _, _)| p)
        .find(|p| p.extension().unwrap() == "pinkdel")
        .expect("a delete file");
    let original = std::fs::read(&del).unwrap();

    // A flipped magic byte is corrupt, never a silently empty delete set.
    let mut bytes = original.clone();
    bytes[0] = b'X';
    std::fs::write(&del, &bytes).unwrap();
    let err = Lake::open(&lake_dir)
        .unwrap()
        .load(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("PINKDL1"), "{err}");
    assert!(err.contains("corrupt"), "{err}");

    // A truncated list is corrupt too.
    std::fs::write(&del, &original[..original.len() - 1]).unwrap();
    let err = Lake::open(&lake_dir)
        .unwrap()
        .load(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("corrupt"), "{err}");
    std::fs::write(&del, &original).unwrap();
    assert_eq!(
        rows_of(&Lake::open(&lake_dir).unwrap().load(None).unwrap()).len(),
        5
    );

    // A manifest whose row counts do not add up is refused at open.
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    catalog["tables"]["main.t"]["rows"] = serde_json::json!(6);
    std::fs::write(
        lake_dir.join("catalog.json"),
        serde_json::to_vec_pretty(&catalog).unwrap(),
    )
    .unwrap();
    let err = Lake::open(&lake_dir).unwrap_err().to_string();
    assert!(err.contains("manifest disagrees with itself"), "{err}");
    cleanup(&dir);
}

#[test]
fn multi_file_attach_merges_stats_and_gates_on_deletes() {
    let dir = scratch("attach_multi");
    let lake_dir = dir.join("lake");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        .unwrap();
    for r in 0..4i32 {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(r), Value::Varchar(format!("v{r}"))])
            .unwrap();
    }
    save_store(&db, &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();

    // Append a file with new extremes and a NULL.
    let mut append = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
        ],
    )
    .unwrap();
    append
        .append_row(vec![Value::Integer(-100), Value::Null])
        .unwrap();
    append
        .append_row(vec![Value::Integer(9), Value::Varchar("z".to_string())])
        .unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(append));
    lake.commit(&commit).unwrap();

    // Attach: rows exact; merged statistics answer COUNT(*), COUNT(col) and
    // MIN/MAX without decoding a single column.
    let report = lake.attach(None).unwrap();
    assert_eq!(report.format_version, FORMAT_VERSION);
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    let query = |db: &mut PinkDb, sql: &str| -> Vec<Vec<Value>> {
        let mut results = db.execute(sql).unwrap();
        match results.pop().unwrap() {
            pinkdb::QueryResult::Selected { rows, .. } => rows,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(
        query(
            &mut attached,
            "SELECT COUNT(*), COUNT(s), MIN(i), MAX(i) FROM t;"
        ),
        vec![vec![
            Value::Bigint(6),
            Value::Bigint(5),
            Value::Integer(-100),
            Value::Integer(9),
        ]],
        "the merged statistics are exact"
    );
    assert_eq!(report.lake.decode_stats().0, 0, "no decode for stats-only");
    // A scan decodes the union in file order (with the NULL preserved).
    let rows = query(&mut attached, "SELECT i, s FROM t;");
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0][0], Value::Integer(0));
    assert_eq!(rows[4][0], Value::Integer(-100));
    assert_eq!(rows[4][1], Value::Null);
    assert_eq!(rows[5][1], Value::Varchar("z".to_string()));
    assert_eq!(report.lake.decode_stats().0, 1);
    let table = report
        .lake
        .tables()
        .iter()
        .find(|t| t.table == "t")
        .unwrap();
    assert_eq!(table.rows, 6);
    assert_eq!(table.stats[0].null_count, Some(0));
    assert_eq!(table.stats[1].null_count, Some(1));

    // Now a delete: rows stay exact, per-column facts become unknowable and
    // the affected queries fall back to a materialization.
    let data_file = read_json(&lake_dir.join("catalog.json"))["tables"]["main.t"]["files"][0]
        ["path"]
        .as_str()
        .unwrap()
        .to_string();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &data_file, &[0]);
    lake.commit(&commit).unwrap();
    let report = lake.attach(None).unwrap();
    let table = report
        .lake
        .tables()
        .iter()
        .find(|t| t.table == "t")
        .unwrap();
    assert_eq!(table.rows, 5, "rows stay exact through deletes");
    assert_eq!(table.stats[0].null_count, None, "null count is unknowable");
    assert_eq!(table.stats[0].min, None);
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        query(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(5)]]
    );
    assert_eq!(
        report.lake.decode_stats().0,
        0,
        "COUNT(*) stays metadata-only"
    );
    // MIN/MAX cannot be proven any more.
    let rows = query(&mut attached, "SELECT MIN(i), COUNT(s) FROM t;");
    assert_eq!(report.lake.decode_stats().0, 1, "MIN falls back");
    assert_eq!(rows[0][0], Value::Integer(-100));
    assert_eq!(rows[0][1], Value::Bigint(4), "the NULL from file two stays");
    cleanup(&dir);
}

#[test]
fn writable_attachment_drops_tables_and_publishes_schema_changes() {
    let dir = scratch("writable");
    let lake_dir = dir.join("lake");
    let mut db = store(4);
    db.execute("CREATE TABLE u (x INTEGER);").unwrap();
    db.table_mut("u")
        .unwrap()
        .append_row(vec![Value::Integer(1)])
        .unwrap();
    save_store(&db, &lake_dir).unwrap();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    // Wave 68: inlining is on by default; this test pins file writes.
    wa.set_data_inlining_row_limit(0);
    assert_eq!(wa.tables().len(), 2);
    assert!(wa.contains("main.t") && wa.contains("main.u"));

    // Dropping a table propagates as a catalog drop; the other table's files
    // stay untouched.
    db.execute("DROP TABLE u;").unwrap();
    let report = wa
        .commit(
            &db,
            &CommitSettings {
                retry_wait_ms: 1,
                ..CommitSettings::default()
            },
        )
        .unwrap();
    assert_eq!(report.tables_dropped, 1, "{report:?}");
    assert_eq!(report.tables_changed, 0, "{report:?}");
    let lake = Lake::open(&lake_dir).unwrap();
    let tables = lake.tables().unwrap();
    assert_eq!(tables.len(), 1, "{tables:?}");
    assert_eq!(tables[0].0, "main.t");

    // Wave 65: a schema change publishes a new schema record instead of
    // being refused. `ADD COLUMN … DEFAULT` is metadata-only for the old
    // files: they stay untouched and their rows read the default.
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    // Wave 68: the schema-change part pins file writes too.
    wa.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    let files_before = file_list(&lake_dir, "main.t");
    db.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7;")
        .unwrap();
    db.execute("INSERT INTO t (i, s) VALUES (50, 'late');")
        .unwrap();
    let report = wa
        .commit(
            &db,
            &CommitSettings {
                retry_wait_ms: 1,
                ..CommitSettings::default()
            },
        )
        .unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    assert_eq!(
        wa.changed_tables(&db).unwrap(),
        Vec::<String>::new(),
        "the schema change committed with the rows"
    );
    let lake = Lake::open(&lake_dir).unwrap();
    let mut reloaded = lake.load(None).unwrap();
    let rows = query(&mut reloaded, "SELECT i, extra FROM t ORDER BY i;");
    assert_eq!(rows.len(), 5, "{rows:?}");
    assert!(
        rows.iter().all(|r| r[1] == Value::Integer(7)),
        "old rows read the default, the new row writes it: {rows:?}"
    );
    // The old data file was never rewritten by the schema change.
    let files_after = file_list(&lake_dir, "main.t");
    assert!(
        files_before.iter().all(|f| files_after.contains(f)),
        "no published file may disappear: {files_before:?} vs {files_after:?}"
    );
    // The published entry carries the record: epoch 2, the column `added` at
    // epoch 2, and one file left at the base epoch.
    let entry = lake_entry(&lake_dir, "main.t");
    assert_eq!(entry["schema_version"], 2, "{entry}");
    let columns = entry["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 3, "{entry}");
    assert_eq!(columns[2]["name"], "extra");
    assert_eq!(columns[2]["added"], 2);
    let file_versions: Vec<u64> = entry["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["schema_version"].as_u64().unwrap())
        .collect();
    assert_eq!(
        file_versions,
        vec![1, 2],
        "the old file stays at epoch 1, the appended one is written at 2"
    );

    // changed_tables sees a later pending change and clears after a commit.
    assert_eq!(
        wa.changed_tables(&lake.load(None).unwrap()).unwrap(),
        Vec::<String>::new(),
        "no pending change"
    );
    let mut db2 = lake.load(None).unwrap();
    db2.execute("INSERT INTO t VALUES (999, 'late', 1);")
        .unwrap();
    assert_eq!(wa.changed_tables(&db2).unwrap(), vec!["main.t".to_string()]);
    wa.commit(
        &db2,
        &CommitSettings {
            retry_wait_ms: 1,
            ..CommitSettings::default()
        },
    )
    .unwrap();
    assert!(wa.changed_tables(&db2).unwrap().is_empty());
    cleanup(&dir);
}

#[test]
fn new_tables_and_empty_tables_commit() {
    let dir = scratch("newtables");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    let mut db = lake.load(None).unwrap();

    // A writable attachment scopes itself to the base version's tables: a
    // store table that is not part of it is ignored by `commit` (the
    // documented `.lake save` route publishes new tables), while the
    // lower-level API can create them.
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    db.execute("CREATE TABLE ignored (a INTEGER);").unwrap();
    let report = wa
        .commit(
            &db,
            &CommitSettings {
                retry_wait_ms: 1,
                ..CommitSettings::default()
            },
        )
        .unwrap();
    assert!(!report.committed, "an unrelated table is not committed");

    // A new empty table and a new non-empty one through the table API.
    db.execute("CREATE TABLE fresh (a INTEGER);").unwrap();
    db.execute("CREATE TABLE filled (a INTEGER);").unwrap();
    db.execute("INSERT INTO filled VALUES (7), (8);").unwrap();
    let base: Vec<(String, Arc<DataTable>)> = lake
        .tables()
        .unwrap()
        .into_iter()
        .map(|(key, _)| {
            let (dbname, table) = key.split_once('.').unwrap();
            let t = db.table_in(dbname, table).unwrap();
            (key, Arc::new(t.clone()))
        })
        .collect();
    let current: Vec<(String, Arc<DataTable>)> = vec![
        (
            "main.t".to_string(),
            Arc::new(db.table("t").unwrap().clone()),
        ),
        (
            "main.fresh".to_string(),
            Arc::new(db.table("fresh").unwrap().clone()),
        ),
        (
            "main.filled".to_string(),
            Arc::new(db.table("filled").unwrap().clone()),
        ),
    ];
    let report = lake
        .commit_from_tables(
            2,
            &base,
            &current,
            &CommitSettings {
                retry_wait_ms: 1,
                ..CommitSettings::default()
            },
        )
        .unwrap();
    assert_eq!(report.tables_changed, 2, "{report:?}");
    let lake = Lake::open(&lake_dir).unwrap();
    let mut names: Vec<(String, u64)> = lake.tables().unwrap();
    names.sort();
    assert_eq!(
        names,
        vec![
            ("main.filled".to_string(), 2),
            ("main.fresh".to_string(), 0),
            ("main.t".to_string(), 2),
        ]
    );
    let loaded = lake.load(None).unwrap();
    assert_eq!(loaded.table_in("main", "fresh").unwrap().num_rows(), 0);
    assert_eq!(loaded.table_in("main", "filled").unwrap().num_rows(), 2);
    // The empty table's entry still lists one (zero-row) data file.
    let entry = &read_json(&lake_dir.join("catalog.json"))["tables"]["main.fresh"];
    assert_eq!(entry["files"].as_array().unwrap().len(), 1, "{entry}");
    assert_eq!(entry["files"][0]["rows"], 0, "{entry}");
    cleanup(&dir);
}

/// A row-level diff commit refuses when the table it diffed against changed
/// concurrently (another writer's rows would be silently dropped from the
/// positional mapping) and succeeds after re-attaching, which recomputes
/// against the current version. Re-attaching adopts the session's content, so
/// the commit then reconciles the table to it — the documented content-sync
/// semantics of `WritableAttachment`, with the CLI printing a reconciliation
/// note when adopted content differs.
#[test]
fn diff_commits_refuse_concurrent_same_table_changes() {
    let dir = scratch("concurrent_diff");
    let lake_dir = dir.join("lake");
    save_store(&store(3), &lake_dir).unwrap(); // v2
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    let mut db = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    let settings = CommitSettings {
        retry_wait_ms: 1,
        ..CommitSettings::default()
    };

    // Another writer appends to the same table.
    let mut other = Lake::open(&lake_dir).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(300, "b")]));
    assert_eq!(other.commit(&commit).unwrap().version, 3);

    // The attachment's diff was computed against v2: refuse, publish nothing.
    db.execute("INSERT INTO t VALUES (200, 'a');").unwrap();
    let err = wa.commit(&db, &settings).unwrap_err();
    assert!(
        err.to_string()
            .contains("changed in the lake since this change was computed"),
        "{err}"
    );
    assert_eq!(
        Lake::open(&lake_dir).unwrap().current_version(),
        3,
        "the refused commit published nothing"
    );
    assert_eq!(
        rows_of(&Lake::open(&lake_dir).unwrap().load(None).unwrap()).len(),
        4
    );

    // Re-attach: recompute against v3. The store is the truth for the
    // attachment's tables, so the reconciliation appends (200, "a") and
    // deletes the other writer's (300, "b") — exactly what the CLI's
    // adoption note warns about, never a silent drop.
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    assert_eq!(wa.base_version(), 3);
    let report = wa.commit(&db, &settings).unwrap();
    assert_eq!(report.version, 4);
    assert_eq!(report.rows_added, 1);
    assert_eq!(
        report.rows_deleted, 1,
        "the concurrent row is reconciled away"
    );
    let rows = rows_of(&Lake::open(&lake_dir).unwrap().load(None).unwrap());
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[3], (200, "a".to_string()));

    // The base advanced in lake order: a further delete positions exactly.
    db.execute("DELETE FROM t WHERE i = 100;").unwrap();
    let report = wa.commit(&db, &settings).unwrap();
    assert_eq!(report.rows_deleted, 1);
    let rows = rows_of(&Lake::open(&lake_dir).unwrap().load(None).unwrap());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (101, "v1".to_string()));
    cleanup(&dir);
}

#[test]
fn commit_on_a_legacy_v1_lake_is_refused() {
    // A pre-incremental (format_version 1) head lists each table as one plain
    // data-file path with the row count only at the table level. Publishing a
    // v5 entry for it would write a catalog the reader rejects, so a commit is
    // refused with a migration hint and the lake stays readable.
    let dir = scratch("legacy-v1-commit");
    let lake_dir = dir.join("lake");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1"),
        &lake_dir,
    );
    let mut lake = Lake::open(&lake_dir).unwrap();
    assert_eq!(lake.format_version(), 1);

    let mut commit = Commit::new();
    commit.drop_table("analytics.u");
    let err = lake.commit(&commit).unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    assert!(err.to_string().contains("format_version 1"), "{err}");

    // Nothing was published: the lake still opens at v1.
    let reopened = Lake::open(&lake_dir).unwrap();
    assert_eq!(reopened.format_version(), 1);
    assert_eq!(reopened.current_version(), 2);
    cleanup(&dir);
}

#[test]
fn a_missing_predecessor_snapshot_does_not_reset_the_schema_version() {
    let dir = scratch("schema-monotonic");
    let mut db = store(2);
    save_store(&db, &dir).unwrap(); // create (v1) + save (v2), schema_version 1
    let catalog = read_json(&dir.join("catalog.json"));
    assert_eq!(catalog["schema_version"], 1, "{catalog}");

    let mut lake = Lake::open(&dir).unwrap();
    assert_eq!(lake.current_version(), 2);
    // Make the current version's snapshot unreadable; catalog.json is the
    // byte-identical head and must preserve the lake-wide counter.
    std::fs::remove_file(dir.join("snapshots/2.json")).unwrap();
    db.execute("CREATE TABLE u (i INTEGER);").unwrap();
    lake.write_from_store(&db).unwrap(); // v3, a schema change
    let catalog = read_json(&dir.join("catalog.json"));
    assert_eq!(
        catalog["schema_version"], 2,
        "the lake-wide schema counter must stay monotonic: {catalog}"
    );
    cleanup(&dir);
}
