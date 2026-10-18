//! Wave 67: lake maintenance — `merge_adjacent_files`, `rewrite_data_files`,
//! `expire_snapshots`, `cleanup_old_files` / `delete_orphaned_files`, and the
//! `list_files` / `table_info` / `options` / `settings` introspection.
//!
//! Format-agnostic on purpose: the suite runs in a default (lean `.pink`)
//! build and in a `vortexlake` build; the data-file extension follows the
//! build, everything else — the catalog shape, the compaction semantics, the
//! statistics/pruning contract — is identical. The S3-mock and encrypted
//! variants live in `tests/s3.rs` and `tests/encryption.rs`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use pink_lake::{
    CleanupSettings, Commit, CommitSettings, ExpireSettings, Lake, LakeError, MergeSettings,
    RewriteSettings,
};
use pinkdb::{PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "vortexlake")]
const EXT: &str = "vortex";
#[cfg(not(feature = "vortexlake"))]
const EXT: &str = "pink";

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_maint_{tag}_{}_{}",
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

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn catalog(dir: &Path) -> serde_json::Value {
    read_json(&dir.join("catalog.json"))
}

fn entry(dir: &Path, key: &str) -> serde_json::Value {
    catalog(dir)["tables"][key].clone()
}

/// The head manifest's data-file paths of one table, in order.
fn file_paths(dir: &Path, key: &str) -> Vec<String> {
    entry(dir, key)["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_string())
        .collect()
}

/// Every file under `<lake>/data`, keyed by its lake-relative path: content
/// and mtime — the "published files are never touched" evidence.
fn snapshot_data_files(dir: &Path) -> BTreeMap<String, (Vec<u8>, SystemTime)> {
    fn walk(base: &Path, current: &Path, out: &mut BTreeMap<String, (Vec<u8>, SystemTime)>) {
        for e in std::fs::read_dir(current).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                walk(base, &e.path(), out);
                continue;
            }
            let rel = e
                .path()
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let meta = e.metadata().unwrap();
            out.insert(
                rel,
                (std::fs::read(e.path()).unwrap(), meta.modified().unwrap()),
            );
        }
    }
    let mut out = BTreeMap::new();
    let data = dir.join("data");
    if data.exists() {
        walk(dir, &data, &mut out);
    }
    out
}

/// Assert that every path of `before` still exists with identical bytes and
/// mtime.
fn assert_untouched(before: &BTreeMap<String, (Vec<u8>, SystemTime)>, dir: &Path) {
    for (rel, (bytes, mtime)) in before {
        let path = dir.join(rel);
        let now = std::fs::read(&path).unwrap_or_else(|e| panic!("{rel} disappeared: {e}"));
        assert_eq!(&now, bytes, "{rel} bytes changed");
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(
            meta.modified().unwrap(),
            *mtime,
            "{rel} mtime changed (a published file was rewritten)"
        );
    }
}

/// `(id, s)` rows for `Commit::append`.
fn sub_table(rows: &[(i32, &str)]) -> Arc<pinkdb::DataTable> {
    let mut t = pinkdb::DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
        ],
    )
    .unwrap();
    for (id, s) in rows {
        t.append_row(vec![Value::Integer(*id), Value::Varchar(s.to_string())])
            .unwrap();
    }
    Arc::new(t)
}

/// A `(id, s)` store for `write_from_store`.
fn store(rows: &[(i32, &str)]) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, s VARCHAR);")
        .unwrap();
    for (id, s) in rows {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(*id), Value::Varchar(s.to_string())])
            .unwrap();
    }
    db
}

/// A lake whose table `main.t` is built from one appended file per chunk
/// (the first append creates the table).
fn appended_lake(dir: &Path, chunks: &[&[(i32, &str)]]) -> Lake {
    let mut lake = Lake::open_or_create(dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    for chunk in chunks {
        let mut commit = Commit::new();
        commit.append("main.t", sub_table(chunk));
        lake.commit(&commit).unwrap();
    }
    lake
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

/// The engine rows for `sql` against a fresh read-only attachment of
/// `lake_dir`, with the decode counters (`loads`, `columns`, `files`) the
/// query caused.
fn attached_query(lake_dir: &Path, sql: &str) -> (Vec<Vec<Value>>, (u64, u64, u64)) {
    let report = Lake::open(lake_dir).unwrap().attach(None).unwrap();
    let lake = Arc::clone(&report.lake);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let rows = query(&mut db, sql);
    (rows, lake.decode_stats())
}

fn ints(rows: Vec<Vec<Value>>) -> Vec<i32> {
    rows.into_iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            ref other => panic!("{other:?}"),
        })
        .collect()
}

/// A count-shaped value as `i64` (the engine may fold counts to either
/// integer width).
fn as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(i) => *i as i64,
        Value::Bigint(i) => *i,
        other => panic!("expected an integer, got {other:?}"),
    }
}

/// All rows of `main.t` in scan order (`(id, s)`).
fn scan_rows(lake_dir: &Path, version: Option<u64>) -> Vec<(i32, String)> {
    let db = Lake::open(lake_dir).unwrap().load(version).unwrap();
    let t = db.table("t").unwrap();
    (0..t.num_rows())
        .map(|r| {
            let id = match t.get(r, 0).unwrap() {
                Value::Integer(i) => i,
                other => panic!("{other:?}"),
            };
            let s = match t.get(r, 1).unwrap() {
                Value::Varchar(s) => s,
                other => panic!("{other:?}"),
            };
            (id, s)
        })
        .collect()
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let target = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &target);
        } else {
            std::fs::copy(e.path(), target).unwrap();
        }
    }
}

/// Backdate a file's mtime (the cleanup age-gate fixture).
fn backdate(path: &Path, secs_ago: u64) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_modified(SystemTime::now() - Duration::from_secs(secs_ago))
        .unwrap();
}

// ---------------------------------------------------------------------------
// merge_adjacent_files
// ---------------------------------------------------------------------------

#[test]
fn merge_consolidates_files_and_leaves_sources_byte_identical() {
    let dir = scratch("merge");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(
        &lake_dir,
        &[
            &[(1, "a"), (2, "b")],
            &[(3, "c"), (4, "d")],
            &[(5, "e"), (6, "f")],
        ],
    );
    let version_before = lake.current_version();
    let before = snapshot_data_files(&lake_dir);
    assert_eq!(file_paths(&lake_dir, "main.t").len(), 3);
    let rows_before = scan_rows(&lake_dir, None);
    assert_eq!(rows_before.len(), 6);

    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed, "3 same-group files must merge");
    assert_eq!(report.version, version_before + 1);
    assert_eq!(report.files_processed(), 3);
    assert_eq!(report.files_created(), 1);
    assert_eq!(report.tables.len(), 1);
    assert_eq!(report.tables[0].table, "main.t");

    // The new version lists one consolidated file with the exact row count;
    // every old file stays byte- and mtime-identical (never mutated).
    let files = file_paths(&lake_dir, "main.t");
    assert_eq!(files.len(), 1, "three files consolidate into one");
    assert!(files[0].ends_with(EXT), "{}", files[0]);
    let head = entry(&lake_dir, "main.t");
    assert_eq!(head["rows"], 6);
    assert_eq!(head["files"][0]["rows"], 6);
    assert_untouched(&before, &lake_dir);

    // Row order and content are exactly the concatenation of the sources.
    assert_eq!(scan_rows(&lake_dir, None), rows_before);
    // Every earlier version still loads, with its own (unmerged) file list.
    assert_eq!(scan_rows(&lake_dir, Some(version_before)), rows_before);

    // The commit delta records the audit trail: compacted sources + the new
    // file (never `replaced`).
    let delta = read_json(&lake_dir.join(format!("commits/{}.json", report.version)));
    let table = &delta["tables"]["main.t"];
    assert_eq!(table["compacted_files"].as_array().unwrap().len(), 3);
    assert_eq!(table["data_files"].as_array().unwrap().len(), 1);
    assert!(table.get("replaced").is_none(), "{table}");

    // A second merge finds nothing (one file per group).
    let again = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(!again.committed);
    assert_eq!(again.version, report.version);
    cleanup(&dir);
}

#[test]
fn merge_preserves_stats_and_pruning_on_the_merged_file() {
    let dir = scratch("merge-stats");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(
        &lake_dir,
        &[
            &[(1, "a"), (2, "b")],
            &[(3, "c"), (4, "d")],
            &[(5, "e"), (6, "f")],
        ],
    );
    // Zone-map pruning works before the merge: `id >= 5` reads one file.
    let (rows, (loads, _, files)) = attached_query(&lake_dir, "SELECT id FROM t WHERE id >= 5");
    assert_eq!(ints(rows), vec![5, 6]);
    assert_eq!(files, 1, "pruning must decode only the matching file");
    assert_eq!(loads, 1);

    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);

    // After the merge: COUNT/MIN/MAX are still answered from statistics
    // with zero decodes.
    let (rows, (loads, _, files)) =
        attached_query(&lake_dir, "SELECT count(*), min(id), max(id) FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(as_i64(&rows[0][0]), 6);
    assert_eq!(as_i64(&rows[0][1]), 1);
    assert_eq!(as_i64(&rows[0][2]), 6);
    assert_eq!(loads, 0, "a stats-only aggregate must not decode");
    assert_eq!(files, 0);

    // A filter that prunes to nothing decodes no file (the load itself is
    // still counted; no bytes are read).
    let (rows, (_, _, files)) = attached_query(&lake_dir, "SELECT id FROM t WHERE id > 100");
    assert!(rows.is_empty());
    assert_eq!(files, 0);

    // A filter that matches decodes the merged file once.
    let (rows, (loads, _, files)) = attached_query(&lake_dir, "SELECT id FROM t WHERE id = 3");
    assert_eq!(ints(rows), vec![3]);
    assert_eq!(loads, 1);
    assert_eq!(files, 1);
    cleanup(&dir);
}

#[test]
fn merge_skips_delete_bearing_files_and_rewrite_materializes_them() {
    let dir = scratch("merge-deletes");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(
        &lake_dir,
        &[
            &[(1, "a"), (2, "b")],
            &[(3, "c"), (4, "d")],
            &[(5, "e"), (6, "f")],
        ],
    );
    let version_before = lake.current_version();
    let files_before = file_paths(&lake_dir, "main.t");
    let delete_target = files_before[1].clone();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &delete_target, &[0]);
    lake.commit(&commit).unwrap();
    let before = snapshot_data_files(&lake_dir);

    // Merge: the delete-bearing file is skipped (DuckLake's rule); the other
    // two files merge. Its delete file stays valid, its bytes untouched.
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(
        report.files_processed(),
        2,
        "the file with deletes is skipped"
    );
    assert_eq!(report.files_created(), 1);
    let head = entry(&lake_dir, "main.t");
    let paths: Vec<String> = head["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(paths.len(), 2);
    assert!(paths.contains(&delete_target));
    let deletes = head["deletes"].as_array().unwrap();
    assert_eq!(
        deletes.len(),
        1,
        "the delete file is untouched: {deletes:?}"
    );
    assert_eq!(deletes[0]["file"], serde_json::json!(delete_target));
    assert_eq!(head["rows"], 5, "6 - 1 deleted");
    assert_untouched(&before, &lake_dir);
    // Content: the deleted position (id 3) is gone, everything else stays.
    // (Merging consolidates the group at its first member's position, so
    // scan order may differ; the row *set* is exact.)
    let mut ids: Vec<i32> = scan_rows(&lake_dir, None)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 4, 5, 6]);

    // rewrite_data_files at the default threshold (0.95) does nothing: the
    // file is only 1/2 deleted.
    let noop = lake
        .rewrite_data_files(None, &RewriteSettings::default())
        .unwrap();
    assert!(!noop.committed, "{noop:?}");

    // With delete_threshold = 0 the file is materialized: one new data
    // file, its delete file dropped, rows exact.
    let report = lake
        .rewrite_data_files(
            None,
            &RewriteSettings {
                delete_threshold: 0.0,
                max_compacted_files: None,
            },
        )
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files_processed(), 1);
    assert_eq!(report.files_created(), 1);
    let head = entry(&lake_dir, "main.t");
    assert!(
        head.get("deletes").is_none(),
        "no delete files left: {head}"
    );
    assert_eq!(head["files"].as_array().unwrap().len(), 2);
    assert_eq!(head["rows"], 5);
    let mut ids: Vec<i32> = scan_rows(&lake_dir, None)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 4, 5, 6]);
    // The old data + delete files stay on disk for earlier versions.
    let mut old_ids: Vec<i32> = scan_rows(&lake_dir, Some(version_before + 1))
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    old_ids.sort_unstable();
    assert_eq!(old_ids, vec![1, 2, 4, 5, 6]);
    let delta = read_json(&lake_dir.join(format!("commits/{}.json", report.version)));
    let table = &delta["tables"]["main.t"];
    assert_eq!(table["removed_delete_files"].as_array().unwrap().len(), 1);
    cleanup(&dir);
}

#[test]
fn merge_never_mixes_partition_values() {
    let dir = scratch("merge-partitions");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    // Partitioned table: one file per value set per commit; two commits per
    // value gives each value a two-file group.
    let mut commit = Commit::new();
    commit.set_partition("main.t", &["s"]).unwrap();
    commit.append("main.t", sub_table(&[(1, "east"), (2, "west")]));
    lake.commit(&commit).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(3, "east"), (4, "west")]));
    lake.commit(&commit).unwrap();
    assert_eq!(file_paths(&lake_dir, "main.t").len(), 4);
    let before = snapshot_data_files(&lake_dir);

    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files_processed(), 4, "two 2-file partition groups");
    assert_eq!(
        report.files_created(),
        2,
        "one file per partition value set"
    );

    // Each merged file carries exactly its partition value's rows: no file
    // ever mixes values.
    let head = entry(&lake_dir, "main.t");
    assert_eq!(head["rows"], 4);
    let mut values: Vec<String> = head["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["partition"]["s"].as_str().unwrap().to_string())
        .collect();
    values.sort();
    assert_eq!(values, vec!["east", "west"]);
    assert_eq!(head["files"].as_array().unwrap().len(), 2);
    for file in head["files"].as_array().unwrap() {
        let path = file["path"].as_str().unwrap();
        assert!(path.starts_with("data/main.t/"));
        assert!(path.ends_with(EXT));
    }
    assert_untouched(&before, &lake_dir);
    // The partition values prune: one merged file per value.
    let rows = scan_rows(&lake_dir, None);
    assert_eq!(rows.len(), 4);
    let (pruned, (loads, _, files)) =
        attached_query(&lake_dir, "SELECT id FROM t WHERE s = 'east' ORDER BY id");
    assert_eq!(ints(pruned), vec![1, 3]);
    assert_eq!(files, 1, "one merged file per partition value");
    assert_eq!(loads, 1);
    cleanup(&dir);
}

#[test]
fn merge_skips_across_schema_epochs() {
    use pink_lake::WritableAttachment;
    let dir = scratch("merge-epochs");
    let lake_dir = dir.join("lake");
    // Epoch 1: a saved table (one file, epoch 1).
    pink_lake::save_store(&store(&[(1, "a")]), &lake_dir).unwrap();

    // A writable attach runs `ALTER TABLE … ADD COLUMN n` (epoch 2); an
    // append right after carries epoch 2. Wave 68: inlining is on by
    // default; this fixture pins file epochs, so every handle opts out.
    {
        let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
        wa.set_data_inlining_row_limit(0);
        wa.set_data_inlining_row_limit(0);
        let mut db = materialize(&wa);
        db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 0;")
            .unwrap();
        wa.commit(&db, &CommitSettings::default()).unwrap();
    }
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append("main.t", three_col_table(2, "b"));
    lake.commit(&commit).unwrap();

    // Another ALTER (epoch 3) and two appends under it.
    {
        let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
        wa.set_data_inlining_row_limit(0);
        wa.set_data_inlining_row_limit(0);
        let mut db = materialize(&wa);
        db.execute("ALTER TABLE t ADD COLUMN m INTEGER DEFAULT 7;")
            .unwrap();
        wa.commit(&db, &CommitSettings::default()).unwrap();
    }
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.set_data_inlining_row_limit(0);
    for (id, s) in [(3, "c"), (4, "d")] {
        let mut commit = Commit::new();
        commit.append("main.t", four_col_table(id, s));
        lake.commit(&commit).unwrap();
    }

    let entry_json = entry(&lake_dir, "main.t");
    let epochs: Vec<u64> = entry_json["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["schema_version"].as_u64().unwrap())
        .collect();
    assert_eq!(epochs, vec![1, 2, 3, 3], "the fixture needs mixed epochs");

    let before = snapshot_data_files(&lake_dir);
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(
        report.files_processed(),
        2,
        "only the two same-epoch files may merge: {report:?}"
    );
    assert_eq!(report.files_created(), 1);

    // Each surviving file keeps its own epoch exactly; the single-epoch
    // groups were never touched.
    let entry_json = entry(&lake_dir, "main.t");
    let files = entry_json["files"].as_array().unwrap();
    assert_eq!(files.len(), 3);
    let epochs: Vec<u64> = files
        .iter()
        .map(|f| f["schema_version"].as_u64().unwrap())
        .collect();
    assert_eq!(epochs, vec![1, 2, 3]);
    assert_eq!(entry_json["rows"], 4);
    assert_untouched(&before, &lake_dir);
    // The logical content (through the schema mapping, defaults included)
    // is exact.
    let rows = scan_rows(&lake_dir, None);
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string())
        ]
    );
    cleanup(&dir);
}

/// `t (id INTEGER, s VARCHAR, n INTEGER)` with one row (epoch-2 appends).
fn three_col_table(id: i32, s: &str) -> Arc<pinkdb::DataTable> {
    let mut t = pinkdb::DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
            ("n".to_string(), pinkdb::DataType::Integer),
        ],
    )
    .unwrap();
    t.append_row(vec![
        Value::Integer(id),
        Value::Varchar(s.to_string()),
        Value::Integer(0),
    ])
    .unwrap();
    Arc::new(t)
}

/// `t (id INTEGER, s VARCHAR, n INTEGER, m INTEGER)` with one row (epoch-3
/// appends).
fn four_col_table(id: i32, s: &str) -> Arc<pinkdb::DataTable> {
    let mut t = pinkdb::DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
            ("n".to_string(), pinkdb::DataType::Integer),
            ("m".to_string(), pinkdb::DataType::Integer),
        ],
    )
    .unwrap();
    t.append_row(vec![
        Value::Integer(id),
        Value::Varchar(s.to_string()),
        Value::Integer(0),
        Value::Integer(7),
    ])
    .unwrap();
    Arc::new(t)
}

/// A writable attachment's base tables materialized into a session store,
/// exactly like `.lake attach … write` does (the helper `schema_evolution`
/// uses).
fn materialize(wa: &pink_lake::WritableAttachment) -> PinkDb {
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
            let chunk = base.scan_chunk(c).unwrap();
            t.append_shared_chunk(&chunk.columns).unwrap();
        }
    }
    db
}

#[test]
fn merge_respects_size_filters_and_max_compacted_files() {
    let dir = scratch("merge-limits");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]);

    // A min_file_size above every file filters all candidates out.
    let report = lake
        .merge_adjacent_files(
            None,
            &MergeSettings {
                min_file_size: Some(1_000_000),
                ..MergeSettings::default()
            },
        )
        .unwrap();
    assert!(!report.committed, "{report:?}");

    // max_file_size = 1 excludes every real file too.
    let report = lake
        .merge_adjacent_files(
            None,
            &MergeSettings {
                max_file_size: Some(1),
                ..MergeSettings::default()
            },
        )
        .unwrap();
    assert!(!report.committed, "{report:?}");

    // min_file_size = 0 includes them; max_compacted_files caps the groups
    // processed (here: one group, so 1 processes everything; the cap is per
    // group, and there is a single group).
    let report = lake
        .merge_adjacent_files(
            None,
            &MergeSettings {
                min_file_size: Some(0),
                max_compacted_files: Some(1),
                ..MergeSettings::default()
            },
        )
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files_processed(), 3);
    cleanup(&dir);

    // Two partition groups, max_compacted_files = 1: exactly one group this
    // call, the other untouched.
    let dir2 = scratch("merge-cap");
    let lake_dir2 = dir2.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir2).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.set_partition("main.t", &["s"]).unwrap();
    commit.append("main.t", sub_table(&[(1, "east"), (2, "west")]));
    lake.commit(&commit).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(3, "east"), (4, "west")]));
    lake.commit(&commit).unwrap();
    let report = lake
        .merge_adjacent_files(
            None,
            &MergeSettings {
                max_compacted_files: Some(1),
                ..MergeSettings::default()
            },
        )
        .unwrap();
    assert!(report.committed);
    assert_eq!(
        report.files_processed(),
        2,
        "one of the two groups: {report:?}"
    );
    assert_eq!(report.files_created(), 1);
    assert_eq!(file_paths(&lake_dir2, "main.t").len(), 3);
    cleanup(&dir2);
}

#[test]
fn merge_on_a_legacy_v1_lake_is_skipped_or_refused() {
    let dir = scratch("merge-legacy");
    let lake_dir = dir.join("lake");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1"),
        &lake_dir,
    );
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    // A scan skips every v1 entry (one path-only file, no per-file counts).
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(!report.committed, "{report:?}");
    // An explicit request is a clean error naming the reason.
    let err = lake
        .merge_adjacent_files(Some("main.fid"), &MergeSettings::default())
        .unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    assert!(err.to_string().contains("format_version < 4"), "{err}");
    // A missing table is a clean error too.
    let err = lake
        .merge_adjacent_files(Some("main.nope"), &MergeSettings::default())
        .unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    cleanup(&dir);
}

#[test]
fn merge_settings_validation() {
    let dir = scratch("merge-validate");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")], &[(2, "b")]]);
    let err = lake
        .merge_adjacent_files(
            None,
            &MergeSettings {
                min_file_size: Some(10),
                max_file_size: Some(10),
                max_compacted_files: None,
            },
        )
        .unwrap_err();
    assert!(matches!(err, LakeError::InvalidOption { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("min_file_size must be less than max_file_size"),
        "{err}"
    );
    let err = lake
        .rewrite_data_files(
            None,
            &RewriteSettings {
                delete_threshold: 1.5,
                max_compacted_files: None,
            },
        )
        .unwrap_err();
    assert!(matches!(err, LakeError::InvalidOption { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("delete_threshold option must be between 0 and 1"),
        "{err}"
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// expire_snapshots
// ---------------------------------------------------------------------------

#[test]
fn expire_snapshots_dry_run_then_versions_with_clean_time_travel_errors() {
    let dir = scratch("expire");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(
        &lake_dir,
        &[&[(1, "a")], &[(2, "b")], &[(3, "c")], &[(4, "d")]],
    );
    let current = lake.current_version();
    assert_eq!(current, 5, "create + 4 commits");

    // Dry run lists the victims and deletes nothing.
    let report = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![2, 3]),
            dry_run: true,
            ..ExpireSettings::default()
        })
        .unwrap();
    assert_eq!(report.version_numbers(), vec![2, 3]);
    assert!(report.dry_run);
    assert!(report.bytes > 0);
    assert!(lake_dir.join("snapshots/2.json").exists());
    assert!(lake_dir.join("commits/2.json").exists());
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3, 4, 5]);

    // Real run: the two snapshots and their commit deltas are gone.
    let report = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![2, 3]),
            dry_run: false,
            ..ExpireSettings::default()
        })
        .unwrap();
    assert_eq!(report.version_numbers(), vec![2, 3]);
    assert!(!lake_dir.join("snapshots/2.json").exists());
    assert!(!lake_dir.join("commits/2.json").exists());
    assert!(!lake_dir.join("snapshots/3.json").exists());
    assert!(!lake_dir.join("commits/3.json").exists());
    assert_eq!(lake.versions().unwrap(), vec![1, 4, 5]);
    // The create snapshot has no commit delta; the retained ones do.
    assert_eq!(lake.commits().unwrap(), vec![4, 5]);

    // Time travel to an expired version is the ordinary clean error; the
    // retained versions keep loading and time travel still works.
    let err = lake.load(Some(2)).unwrap_err();
    assert!(matches!(err, LakeError::VersionNotFound { .. }), "{err:?}");
    assert!(err.to_string().contains("no snapshot version 2"), "{err}");
    assert!(lake.load(Some(4)).is_ok());
    assert!(lake.load(None).is_ok());
    // The catalog is untouched (byte-for-byte the version-5 manifest).
    assert_eq!(catalog(&lake_dir)["version"], 5);

    // Expiring the same versions again is a clean no-op.
    let again = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![2, 3]),
            dry_run: false,
            ..ExpireSettings::default()
        })
        .unwrap();
    assert!(again.versions.is_empty());
    cleanup(&dir);
}

#[test]
fn expire_never_touches_the_current_version() {
    let dir = scratch("expire-current");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")], &[(2, "b")]]);
    let current = lake.current_version();
    // Naming the current version explicitly is a no-op (DuckLake's
    // `snapshot_id != MAX(snapshot_id)` filter).
    let report = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![current]),
            dry_run: false,
            ..ExpireSettings::default()
        })
        .unwrap();
    assert!(report.versions.is_empty());
    assert!(lake_dir.join(format!("snapshots/{current}.json")).exists());

    // retain_last alone protects the newest N; with N = 1 only the current
    // survives.
    let report = lake
        .expire_snapshots(&ExpireSettings {
            retain_last: Some(1),
            dry_run: false,
            ..ExpireSettings::default()
        })
        .unwrap();
    assert_eq!(report.version_numbers(), vec![1, 2]);
    assert_eq!(lake.versions().unwrap(), vec![current]);
    assert!(lake.load(None).is_ok());
    cleanup(&dir);
}

#[test]
fn expire_older_than_and_empty_rules() {
    let dir = scratch("expire-time");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")], &[(2, "b")]]);
    let current = lake.current_version();

    // No criterion: a silent no-op (DuckLake without `expire_older_than`).
    let report = lake.expire_snapshots(&ExpireSettings::default()).unwrap();
    assert!(report.versions.is_empty());
    // An explicitly empty version set expires nothing.
    let report = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![]),
            ..ExpireSettings::default()
        })
        .unwrap();
    assert!(report.versions.is_empty());

    // A cutoff before every snapshot expires nothing...
    let epoch_cutoff = pinkdb::types::parse_iso8601_utc("1970-01-01T00:00:01Z").unwrap();
    let report = lake
        .expire_snapshots(&ExpireSettings {
            older_than_micros: Some(epoch_cutoff),
            ..ExpireSettings::default()
        })
        .unwrap();
    assert!(report.versions.is_empty());
    // ...and a cutoff after every snapshot (but before "now") expires all
    // but the current version.
    let future = SystemTime::now() + Duration::from_secs(3600);
    let future = pinkdb::types::parse_iso8601_utc(&format_iso(future)).unwrap();
    let report = lake
        .expire_snapshots(&ExpireSettings {
            older_than_micros: Some(future),
            dry_run: false,
            ..ExpireSettings::default()
        })
        .unwrap();
    assert_eq!(report.version_numbers(), (1..current).collect::<Vec<u64>>());
    assert_eq!(lake.versions().unwrap(), vec![current]);
    cleanup(&dir);
}

/// `2026-10-17T12:00:00.000Z` — the same shape the manifests store, so the
/// engine's parser round-trips it.
fn format_iso(time: SystemTime) -> String {
    let since = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since.as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        since.subsec_millis(),
    )
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[test]
fn expire_rule_errors_match_ducklake() {
    let dir = scratch("expire-errors");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")]]);
    let err = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![1]),
            older_than_micros: Some(0),
            ..ExpireSettings::default()
        })
        .unwrap_err();
    assert!(matches!(err, LakeError::InvalidOption { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("cannot specify both 'versions' and 'older_than'"),
        "{err}"
    );
    // The rule was refused before anything was touched.
    assert!(lake_dir.join("snapshots/1.json").exists());
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// cleanup_old_files / delete_orphaned_files
// ---------------------------------------------------------------------------

#[test]
fn cleanup_dry_run_deletes_nothing_and_confirm_deletes_once() {
    let dir = scratch("cleanup");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(
        &lake_dir,
        &[&[(1, "a"), (2, "b")], &[(3, "c"), (4, "d")], &[(5, "e")]],
    );
    // Merge the first two files: they become unreferenced by the head, but
    // the older snapshots still reference them.
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    let head_files = file_paths(&lake_dir, "main.t");

    // While the old snapshots are retained, nothing is unreferenced.
    let report = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(report.paths.is_empty(), "{report:?}");

    // Expire everything but the current version: the merged-away files
    // become unreferenced. Dry run lists them and deletes nothing.
    lake.expire_snapshots(&ExpireSettings {
        retain_last: Some(1),
        dry_run: false,
        ..ExpireSettings::default()
    })
    .unwrap();
    let report = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(
        report.paths.len(),
        3,
        "the three merged-away files: {report:?}"
    );
    assert!(report.bytes > 0);
    for path in &report.paths {
        assert!(lake_dir.join(path).exists(), "dry run deleted {path}");
    }
    for path in &head_files {
        assert!(!report.paths.contains(path), "head file {path} listed");
    }

    // Neither dry-run nor confirm: refused, nothing deleted.
    let err = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            ..CleanupSettings::default()
        })
        .unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    assert!(err.to_string().contains("without confirmation"), "{err}");
    for path in &report.paths {
        assert!(lake_dir.join(path).exists(), "refusal deleted {path}");
    }

    // Confirmed: the unreferenced objects are gone; running twice is a
    // no-op.
    let report2 = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(report2.deleted);
    assert_eq!(report2.paths, report.paths);
    for path in &report2.paths {
        assert!(!lake_dir.join(path).exists(), "{path} survived");
    }
    // The head version (and the retained snapshot-less current version)
    // still loads, and its files are untouched.
    assert!(lake.load(None).is_ok());
    let again = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(again.paths.is_empty());
    // `delete_orphaned_files` runs the same scan.
    let orphans = lake
        .delete_orphaned_files(&CleanupSettings {
            all: true,
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(orphans.paths.is_empty());
    cleanup(&dir);
}

#[test]
fn cleanup_protects_referenced_objects_and_strays_outside_data() {
    let dir = scratch("cleanup-protect");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")], &[(2, "b")]]);
    let head_files = file_paths(&lake_dir, "main.t");

    // A crashed writer's leftovers: a stray inside data/ and one outside it.
    let stray = lake_dir.join("data/main.t/leftover.pink.tmp");
    std::fs::write(&stray, b"crashed half-written file").unwrap();
    backdate(&stray, 9 * 86_400);
    let outside = lake_dir.join("stray.bin");
    std::fs::write(&outside, b"not a data object").unwrap();
    backdate(&outside, 9 * 86_400);

    let report = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(report.paths, vec!["data/main.t/leftover.pink.tmp"]);
    // Nothing referenced is ever in the plan, even with `all`.
    for path in &head_files {
        assert!(!report.paths.contains(path), "{path} listed");
    }

    // The default age gate (2 days) protects a *young* unreferenced object;
    // an explicit cutoff in the past protects it too, a future cutoff
    // exposes it.
    let young = lake_dir.join("data/main.t/young.pink.tmp");
    std::fs::write(&young, b"fresh leftover").unwrap();
    let report = lake
        .cleanup_old_files(&CleanupSettings {
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(
        !report
            .paths
            .contains(&"data/main.t/young.pink.tmp".to_string()),
        "the 2-day default gate must protect a fresh object: {report:?}"
    );
    let epoch_cutoff = pinkdb::types::parse_iso8601_utc("1970-01-01T00:00:01Z").unwrap();
    let report = lake
        .cleanup_old_files(&CleanupSettings {
            older_than_micros: Some(epoch_cutoff),
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(
        !report
            .paths
            .contains(&"data/main.t/young.pink.tmp".to_string()),
        "a past cutoff protects it: {report:?}"
    );
    let future = format_iso(SystemTime::now() + Duration::from_secs(3600));
    let future = pinkdb::types::parse_iso8601_utc(&future).unwrap();
    let report = lake
        .cleanup_old_files(&CleanupSettings {
            older_than_micros: Some(future),
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(report
        .paths
        .contains(&"data/main.t/young.pink.tmp".to_string()));

    // The backdated stray *is* visible under the default gate.
    assert!(report
        .paths
        .contains(&"data/main.t/leftover.pink.tmp".to_string()));

    // Confirmed deletion removes exactly the in-data leftovers; the
    // outside-data stray and every referenced file stay.
    let report = lake
        .cleanup_old_files(&CleanupSettings {
            older_than_micros: Some(future),
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(report.deleted);
    assert!(!stray.exists());
    assert!(!young.exists());
    assert!(outside.exists(), "objects outside data/ are never touched");
    for path in &head_files {
        assert!(lake_dir.join(path).exists(), "{path} disappeared");
    }
    assert!(lake.load(None).is_ok());
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

#[test]
fn list_files_reports_rows_sizes_partitions_and_unreferenced_objects() {
    let dir = scratch("files");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(
        &lake_dir,
        &[&[(1, "a"), (2, "b")], &[(3, "c")], &[(4, "d")], &[(5, "e")]],
    );
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &file_paths(&lake_dir, "main.t")[0], &[0]);
    lake.commit(&commit).unwrap();

    let files = lake.list_files(None, None, false).unwrap();
    assert_eq!(
        files.len(),
        4,
        "three plain data files + one delete pairing"
    );
    let data: Vec<_> = files.iter().filter(|f| f.delete_file.is_none()).collect();
    let deletes: Vec<_> = files.iter().filter(|f| f.delete_file.is_some()).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(deletes.len(), 1);
    for info in &data {
        assert_eq!(info.table.as_deref(), Some("main.t"));
        assert!(info.referenced);
        assert!(info.rows.is_some());
        assert!(info.size_bytes.is_some_and(|s| s > 0));
        assert!(info.path.ends_with(EXT));
    }
    let rows: u64 = data.iter().filter_map(|f| f.rows).sum();
    assert_eq!(rows, 3, "1 + 1 + 1 data rows across the plain files");
    let delete = deletes[0];
    assert_eq!(delete.delete_file_rows, Some(1));
    assert_eq!(delete.table.as_deref(), Some("main.t"));
    assert!(delete.delete_file.as_deref().unwrap().ends_with(".pinkdel"));

    // The table filter matches the flat key and the bare table name; an
    // unknown table is a clean error.
    assert_eq!(
        lake.list_files(Some("main.t"), None, false).unwrap().len(),
        4
    );
    assert_eq!(lake.list_files(Some("t"), None, false).unwrap().len(), 4);
    let err = lake.list_files(Some("nope"), None, false).unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");

    // A specific version lists that version's own file set.
    assert_eq!(lake.list_files(None, Some(3), false).unwrap().len(), 2);

    // Merge: the delete-bearing file is skipped, the other three merge.
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files_processed(), 3);
    // While the old snapshots are retained, `all` adds nothing: the merged
    // sources are still referenced.
    let files = lake.list_files(None, None, true).unwrap();
    assert!(
        files.iter().all(|f| f.referenced),
        "old snapshots still reference the merged sources"
    );
    // After expiring them, `all` marks the three sources unreferenced.
    lake.expire_snapshots(&ExpireSettings {
        retain_last: Some(1),
        ..ExpireSettings::default()
    })
    .unwrap();
    let files = lake.list_files(None, None, true).unwrap();
    let unreferenced: Vec<_> = files.iter().filter(|f| !f.referenced).collect();
    assert_eq!(unreferenced.len(), 3, "{files:?}");
    for info in unreferenced {
        assert_eq!(info.table.as_deref(), Some("main.t"));
        assert!(info.rows.is_none());
        assert!(info.size_bytes.is_some_and(|s| s > 0));
        assert!(!info.path.is_empty());
    }
    cleanup(&dir);
}

#[test]
fn table_info_options_and_settings_report_exact_facts() {
    let dir = scratch("info");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.set_partition("main.t", &["s"]).unwrap();
    commit.append("main.t", sub_table(&[(1, "east"), (2, "west")]));
    lake.commit(&commit).unwrap();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &file_paths(&lake_dir, "main.t")[0], &[0]);
    lake.commit(&commit).unwrap();

    let info = lake.table_info().unwrap();
    assert_eq!(info.len(), 1);
    let table = &info[0];
    assert_eq!(table.table, "main.t");
    assert_eq!(table.rows, 1);
    assert_eq!(table.file_count, 2);
    assert_eq!(table.delete_file_count, 1);
    assert_eq!(table.partition_by, vec!["s".to_string()]);
    // Sizes are the stored objects' exact byte lengths.
    let on_disk: u64 = file_paths(&lake_dir, "main.t")
        .iter()
        .map(|p| std::fs::metadata(lake_dir.join(p)).unwrap().len())
        .sum();
    assert_eq!(table.file_size_bytes, Some(on_disk));
    assert!(table.delete_file_size_bytes.is_some_and(|s| s > 0));

    let options = lake.options().unwrap();
    let value = |name: &str| {
        options
            .iter()
            .find(|o| o.name == name)
            .map(|o| o.value.clone())
            .unwrap_or_else(|| panic!("no option {name}"))
    };
    assert_eq!(value("data_path"), lake_dir.display().to_string());
    assert_eq!(value("current_version"), "3");
    assert_eq!(value("encrypted"), "false");
    #[cfg(feature = "vortexlake")]
    assert_eq!(value("data_format"), "vortex");
    #[cfg(not(feature = "vortexlake"))]
    assert_eq!(value("data_format"), "pink");
    let settings = lake.settings();
    assert_eq!(settings.catalog_type, "json");
    assert_eq!(settings.data_path, lake_dir.display().to_string());
    assert_eq!(settings.extension_version, env!("CARGO_PKG_VERSION"));
    cleanup(&dir);
}

/// The compaction time-travel contract: a merge publishes one version, and
//. every earlier version still reads its own recorded files.
#[test]
fn merged_versions_time_travel_to_the_unmerged_past() {
    let dir = scratch("merge-travel");
    let lake_dir = dir.join("lake");
    let mut lake = appended_lake(&lake_dir, &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]);
    let before = lake.current_version();
    lake.merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    let version = lake.current_version();
    assert_eq!(
        lake.load(Some(before))
            .unwrap()
            .table("t")
            .unwrap()
            .num_rows(),
        3
    );
    assert_eq!(
        lake.load(Some(version))
            .unwrap()
            .table("t")
            .unwrap()
            .num_rows(),
        3
    );
    // A snapshot record for the maintenance version carries the change
    // summary: the table is rewritten, no rows moved.
    let records = lake.snapshots().unwrap();
    let latest = records.last().unwrap();
    assert_eq!(latest.version, version);
    let changes = latest.changes.as_ref().expect("a summary");
    assert_eq!(changes.tables_rewritten, vec!["main.t".to_string()]);
    assert!(changes.rows_added.is_empty());
    assert!(changes.rows_removed.is_empty());
    cleanup(&dir);
}
