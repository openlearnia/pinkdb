//! Lake partitioning and file pruning (wave 66): `PARTITIONED BY`-style
//! specs in the catalog (identity columns and `bucket(N, col)`), commits (and
//! saves) splitting rows into one data file per partition value set, per-file
//! zone maps, and pruned scans that decode only the files a simple `WHERE`
//! can still match.
//!
//! Format-agnostic on purpose: the suite runs in a default (lean `.pink`)
//! build and in a `vortexlake` build, and the data-file extension follows
//! the build. The catalog shape, the partition values, the zone maps and the
//! pruning semantics are identical.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, CommitSettings, Lake, LakeError};
use pinkdb::{PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "vortexlake")]
const EXT: &str = "vortex";
#[cfg(not(feature = "vortexlake"))]
const EXT: &str = "pink";

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_partition_{tag}_{}_{}",
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

/// `(id, region)` store; `region: None` is SQL NULL.
fn store(rows: &[(i32, Option<&str>)]) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, region VARCHAR, s VARCHAR);")
        .unwrap();
    for (i, region) in rows {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![
                Value::Integer(*i),
                match region {
                    Some(r) => Value::Varchar(r.to_string()),
                    None => Value::Null,
                },
                Value::Varchar(format!("v{i}")),
            ])
            .unwrap();
    }
    db
}

/// A same-schema sub-table for `Commit::append`.
fn sub_table(rows: &[(i32, Option<&str>)]) -> Arc<pinkdb::DataTable> {
    let mut t = pinkdb::DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("region".to_string(), pinkdb::DataType::Varchar),
            ("s".to_string(), pinkdb::DataType::Varchar),
        ],
    )
    .unwrap();
    for (i, region) in rows {
        t.append_row(vec![
            Value::Integer(*i),
            match region {
                Some(r) => Value::Varchar(r.to_string()),
                None => Value::Null,
            },
            Value::Varchar(format!("v{i}")),
        ])
        .unwrap();
    }
    Arc::new(t)
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

/// The engine rows for `sql` against a fresh read-only attachment of
/// `lake_dir`, with the data-file decode counters (`loads`, `columns`,
/// `files`) the query caused. Every call gets a fresh attachment, so the
/// counters are exactly that query's.
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

/// Compare two result sets ignoring row order: with partitioning, files are
/// in label order, not insert order, and a `SELECT` has no order guarantee.
fn assert_rows_eq(mut got: Vec<Vec<Value>>, mut want: Vec<Vec<Value>>, what: &str) {
    let key = |r: &Vec<Value>| format!("{r:?}");
    got.sort_by_key(key);
    want.sort_by_key(key);
    assert_eq!(got, want, "{what}");
}

fn part_values(catalog: &serde_json::Value, key: &str) -> Vec<Option<String>> {
    catalog["tables"]["main.t"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            f.get("partition")
                .and_then(|p| p.get(key))
                .and_then(|v| v.as_str().map(str::to_string))
        })
        .collect()
}

/// Commits split one append into one data file per partition value set, the
/// catalog records every file's values, a later commit appends another file
/// per value (never merging), and `.lake save` of a partitioned table
/// preserves the spec and re-splits.
#[test]
fn partitioned_commits_split_one_file_per_value_set() {
    let dir = scratch("split");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[
        (1, Some("east")),
        (2, Some("west")),
        (3, Some("east")),
    ]))
    .unwrap();

    let report = lake
        .set_partition_spec(
            "main.t",
            &["region".to_string()],
            &CommitSettings::default(),
        )
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.tables_repartitioned, 1);

    // The spec is a no-op when re-set unchanged.
    let again = lake
        .set_partition_spec(
            "main.t",
            &["region".to_string()],
            &CommitSettings::default(),
        )
        .unwrap();
    assert!(
        !again.committed,
        "re-setting the same spec publishes nothing"
    );

    // One commit appends rows spanning three values: one data file each.
    let mut commit = Commit::new();
    commit.append(
        "main.t",
        sub_table(&[
            (4, Some("east")),
            (5, Some("south")),
            (6, Some("west")),
            (7, Some("south")),
        ]),
    );
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.data_files, 3, "one file per distinct value set");
    assert_eq!(report.rows_added, 4);

    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(
        entry["partition_by"],
        serde_json::json!([{ "column": "region", "transform": "identity" }])
    );
    let files = entry["files"].as_array().unwrap();
    assert_eq!(files.len(), 4, "v2's file + 3 partitioned appends");
    // The pre-spec file carries no values; every appended file does.
    assert!(files[0].get("partition").is_none());
    let mut appended_values: Vec<String> = files[1..]
        .iter()
        .map(|f| f["partition"]["region"].as_str().unwrap().to_string())
        .collect();
    appended_values.sort();
    assert_eq!(appended_values, vec!["east", "south", "west"]);
    for f in &files[1..] {
        assert!(f["path"].as_str().unwrap().ends_with(EXT));
    }

    // The content is exactly the union, in file order.
    let loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    let ids: Vec<i32> = {
        let t = loaded.table("t").unwrap();
        (0..t.num_rows())
            .map(|r| match t.get(r, 0).unwrap() {
                Value::Integer(i) => i,
                other => panic!("{other:?}"),
            })
            .collect()
    };
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![1, 2, 3, 4, 5, 6, 7]);

    // A second commit for an existing value appends another file — commits
    // never merge (that is wave 67's `merge_adjacent_files`).
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(8, Some("south"))]));
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.data_files, 1);
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let values = part_values(&catalog, "region");
    assert_eq!(
        values
            .iter()
            .filter(|v| v.as_deref() == Some("south"))
            .count(),
        2
    );

    // A full save of a partitioned table preserves the spec and re-splits
    // every row by value.
    lake.write_from_store(&store(&[
        (10, Some("east")),
        (11, Some("west")),
        (12, Some("south")),
    ]))
    .unwrap();
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(
        entry["partition_by"],
        serde_json::json!([{ "column": "region", "transform": "identity" }])
    );
    let files = entry["files"].as_array().unwrap();
    assert_eq!(files.len(), 3, "the save re-splits into one file per value");
    let mut values: Vec<String> = files
        .iter()
        .map(|f| f["partition"]["region"].as_str().unwrap().to_string())
        .collect();
    values.sort();
    assert_eq!(values, vec!["east", "south", "west"]);
    assert_eq!(entry["rows"], 3);
    cleanup(&dir);
}

/// A selective predicate decodes only the files whose zone maps leave it
/// satisfiable; a fully pruned scan decodes nothing; an unprunable predicate
/// decodes everything; every answer equals the same rows in a plain store.
#[test]
fn zone_map_pruning_decodes_only_matching_files() {
    let dir = scratch("prune");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    // Three files with disjoint id and region ranges.
    lake.write_from_store(&store(&[(1, Some("a1")), (2, Some("a2")), (3, Some("a3"))]))
        .unwrap();
    let mut commit = Commit::new();
    commit.append(
        "main.t",
        sub_table(&[(10, Some("m1")), (11, Some("m2")), (12, Some("m3"))]),
    );
    let mut commit2 = Commit::new();
    commit2.append(
        "main.t",
        sub_table(&[(20, Some("z1")), (21, Some("z2")), (22, Some("z3"))]),
    );
    // Two appends in one commit: use one commit with both files' rows would
    // merge them; commit separately to keep the file ranges disjoint.
    lake.commit(&commit).unwrap();
    lake.commit(&commit2).unwrap();

    // Ground truth: the same rows in a plain store.
    let mut plain = store(&[
        (1, Some("a1")),
        (2, Some("a2")),
        (3, Some("a3")),
        (10, Some("m1")),
        (11, Some("m2")),
        (12, Some("m3")),
        (20, Some("z1")),
        (21, Some("z2")),
        (22, Some("z3")),
    ]);

    let cases: Vec<(&str, u64)> = vec![
        // id = 11: only the middle file can match.
        ("SELECT id FROM t WHERE id = 11;", 1),
        // id = 2: only the first file.
        ("SELECT id FROM t WHERE id = 2;", 1),
        // Fully pruned: no file's range can satisfy it.
        ("SELECT id FROM t WHERE id > 100;", 0),
        // region ranges are disjoint too: `>= 'm'` prunes the a-file; nothing
        // survives `> 'zz'` (the z-file's max is 'z3').
        ("SELECT id FROM t WHERE region >= 'm';", 2),
        ("SELECT id FROM t WHERE region > 'zz';", 0),
        // A range crossing two files decodes exactly those two.
        ("SELECT id FROM t WHERE id >= 10 AND id < 22;", 2),
        // NULL tests need null counts, which every file proves (0 NULLs).
        ("SELECT id FROM t WHERE region IS NULL;", 0),
        // NOT EQUAL cannot be proven from a range: all files decode.
        ("SELECT id FROM t WHERE id <> 11;", 3),
        // A LIKE is not pushed: all files decode.
        ("SELECT id FROM t WHERE region LIKE 'm%';", 3),
    ];
    for (sql, want_files) in cases {
        let (got, (_loads, _columns, files)) = attached_query(&lake_dir, sql);
        let want = query(&mut plain, sql);
        assert_rows_eq(got, want, sql);
        assert_eq!(files, want_files, "{sql}: data files decoded");
    }

    // The projection and the pruning compose: only the predicate's column and
    // the projected column are decoded, from the surviving file only.
    let (got, (loads, columns, files)) =
        attached_query(&lake_dir, "SELECT s FROM t WHERE id = 11;");
    assert_eq!(got, vec![vec![Value::Varchar("v11".to_string())]]);
    assert_eq!((loads, files), (1, 1), "one load, one file");
    #[cfg(feature = "vortexlake")]
    assert_eq!(columns, 2, "id (predicate) and s (projection) only");
    // A lean `.pink` projection decodes the whole (single-file) table.
    #[cfg(not(feature = "vortexlake"))]
    assert_eq!(columns, 3, "the `.pink` decode counts every column");
    cleanup(&dir);
}

/// Partition values prune identity keys exactly, NULL partition values
/// included, and every answer equals the plain-store ground truth.
#[test]
fn partition_values_prune_and_nulls_stay_exact() {
    let dir = scratch("partition_prune");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    // Save first (one file), then partition, then save again: every file of
    // the partitioned version carries its value.
    lake.write_from_store(&store(&[(1, Some("east"))])).unwrap();
    lake.set_partition_spec(
        "main.t",
        &["region".to_string()],
        &CommitSettings::default(),
    )
    .unwrap();
    lake.write_from_store(&store(&[
        (1, Some("east")),
        (2, Some("west")),
        (3, None),
        (4, Some("east")),
        (5, None),
    ]))
    .unwrap();

    let catalog = read_json(&lake_dir.join("catalog.json"));
    let values = part_values(&catalog, "region");
    assert_eq!(values.len(), 3, "east, west, NULL");
    assert_eq!(values.iter().filter(|v| v.is_none()).count(), 1);

    let mut plain = store(&[
        (1, Some("east")),
        (2, Some("west")),
        (3, None),
        (4, Some("east")),
        (5, None),
    ]);
    let cases: Vec<(&str, u64)> = vec![
        // Exactly the east file (4 rows? no: 2 rows) — one file.
        ("SELECT id FROM t WHERE region = 'east';", 1),
        ("SELECT id FROM t WHERE region = 'west';", 1),
        // `= 'north'` matches no partition at all: zero files.
        ("SELECT id FROM t WHERE region = 'north';", 0),
        // IS NULL serves the NULL file only.
        ("SELECT id FROM t WHERE region IS NULL;", 1),
        // IS NOT NULL prunes the NULL file (an exact null count).
        ("SELECT id FROM t WHERE region IS NOT NULL;", 2),
    ];
    for (sql, want_files) in cases {
        let (got, (_l, _c, files)) = attached_query(&lake_dir, sql);
        assert_rows_eq(got, query(&mut plain, sql), sql);
        assert_eq!(files, want_files, "{sql}: data files decoded");
    }
    cleanup(&dir);
}

/// Pruning with positional deletes keeps the documented honesty rule: the
/// table-level `MIN`/`MAX`/`NULL`-count facts fall back (a delete could have
/// removed the extreme), while file pruning keeps working — deleting rows
/// only shrinks a file's value set, so a pruned file could not have matched
/// before deletes either. Answers equal the plain-store ground truth.
#[test]
fn pruning_with_positional_deletes_is_correct_and_honest() {
    let dir = scratch("prune_deletes");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a1")), (2, Some("a2"))]))
        .unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(10, Some("m1")), (11, Some("m2"))]));
    lake.commit(&commit).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(20, Some("z1"))]));
    lake.commit(&commit).unwrap();

    // Delete the row with id = 11 (middle file, position 1).
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let middle = catalog["tables"]["main.t"]["files"][1]["path"]
        .as_str()
        .unwrap()
        .to_string();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &middle, &[1]);
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.rows_deleted, 1);

    // A fresh attachment: the row count stays exact, the per-column facts
    // are unknowable (the wave-63 rule)...
    let report = lake.attach(None).unwrap();
    let table = report
        .lake
        .tables()
        .iter()
        .find(|t| t.table == "t")
        .unwrap();
    assert_eq!(table.rows, 4);
    assert_eq!(table.stats[0].min, None, "MIN is unknowable after deletes");
    assert_eq!(table.stats[0].null_count, None);

    // ...and `MIN(id)` materializes (all three files) but answers exactly.
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        query(&mut db, "SELECT MIN(id), MAX(id), COUNT(*) FROM t;"),
        vec![vec![
            Value::Integer(1),
            Value::Integer(20),
            Value::Bigint(4)
        ]]
    );
    assert_eq!(report.lake.decode_stats().2, 3, "the fallback decodes all");

    // Pruning still applies with deletes: the middle file is decoded (its
    // zone [10, 11] covers the predicate) and the deleted position is
    // dropped; the other files are pruned away.
    let (got, (loads, _columns, files)) =
        attached_query(&lake_dir, "SELECT id FROM t WHERE id = 11;");
    assert!(got.is_empty(), "the only id = 11 row was deleted");
    assert_eq!((loads, files), (1, 1));
    // The first file is pruned for a predicate its range cannot satisfy.
    let (got, (_l, _c, files)) = attached_query(&lake_dir, "SELECT id FROM t WHERE id = 20;");
    assert_eq!(ints(got), vec![20]);
    assert_eq!(files, 1);
    cleanup(&dir);
}

/// Time travel prunes from **that version's** zone maps and file list: an
/// older version's single wide file is skipped for a predicate only the newer
/// version's files can satisfy, and the counters prove it.
#[test]
fn pruning_across_time_travel_uses_version_zone_maps() {
    let dir = scratch("prune_tt");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a")), (2, Some("b")), (3, Some("c"))]))
        .unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(10, Some("m")), (11, Some("n"))]));
    let commit_version = lake.commit(&commit).unwrap().version;

    // v2 has one file with id ∈ [1, 3]; the commit version has two, the
    // second with id ∈ [10, 11].
    let (got, (_l, _c, files)) = attached_query(
        &lake_dir,
        "SELECT COUNT(*) FROM t AT (VERSION => 2) WHERE id >= 10;",
    );
    assert_eq!(got, vec![vec![Value::Bigint(0)]]);
    assert_eq!(files, 0, "v2's file is pruned entirely");

    let (got, (_l, _c, files)) = attached_query(
        &lake_dir,
        &format!("SELECT id FROM t AT (VERSION => {commit_version}) WHERE id = 10;"),
    );
    assert_eq!(ints(got), vec![10]);
    assert_eq!(files, 1, "only the newer version's matching file decodes");

    let (got, (_l, _c, files)) = attached_query(
        &lake_dir,
        &format!("SELECT id FROM t AT (VERSION => {commit_version}) WHERE id = 2;"),
    );
    assert_eq!(ints(got), vec![2]);
    assert_eq!(files, 1, "and the older file for its own predicate");
    cleanup(&dir);
}

/// The pinned version is unaffected by time-travel pruning, and a version
/// attachment keeps its own counters (both are part of `decode_stats`).
#[test]
fn time_travel_pruning_leaves_the_pinned_version_alone() {
    let dir = scratch("prune_tt_pin");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();
    lake.set_partition_spec(
        "main.t",
        &["region".to_string()],
        &CommitSettings::default(),
    )
    .unwrap();
    lake.write_from_store(&store(&[(1, Some("a")), (2, Some("b"))]))
        .unwrap();

    let report = Lake::open(&lake_dir).unwrap().attach(None).unwrap();
    let lake = Arc::clone(&report.lake);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    // A pruned time-travel read of the pinned version itself.
    assert_eq!(
        query(&mut db, "SELECT id FROM t WHERE region = 'b';"),
        vec![vec![Value::Integer(2)]]
    );
    let (loads, _columns, files) = lake.decode_stats();
    assert_eq!((loads, files), (1, 1));
    // The newest version is intact afterwards.
    assert_eq!(
        query(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(2)]]
    );
    cleanup(&dir);
}

/// Bucket keys split rows by a deterministic label and record the label per
/// file; the recorded label is never treated as a column value, so queries
/// stay exact.
#[test]
fn bucket_partitioning_splits_and_stays_correct() {
    let dir = scratch("bucket");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();
    let report = lake
        .set_partition_spec(
            "main.t",
            &["bucket(2, id)".to_string()],
            &CommitSettings::default(),
        )
        .unwrap();
    assert!(report.committed);
    lake.write_from_store(&store(&[
        (1, Some("a")),
        (2, Some("b")),
        (3, Some("c")),
        (4, Some("d")),
        (5, Some("e")),
        (6, Some("f")),
    ]))
    .unwrap();

    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(
        entry["partition_by"],
        serde_json::json!([{ "column": "id", "transform": "bucket(2)" }])
    );
    let files = entry["files"].as_array().unwrap();
    assert!(
        (1..=2).contains(&files.len()),
        "bucket(2) produces at most two files, got {}",
        files.len()
    );
    // Every file carries its own bucket label (and the labels are the only
    // ones bucket(2) can produce).
    for f in files {
        let label = f["partition"]["bucket(2, id)"].as_str().unwrap();
        assert!(label == "0" || label == "1", "bucket label {label:?}");
    }

    // Content round-trips exactly, and a predicate stays correct (a bucket
    // label is not a column fact: pruning falls back to zone maps).
    let loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 6);
    let (got, (_l, _c, _f)) = attached_query(&lake_dir, "SELECT id FROM t WHERE id = 4;");
    assert_eq!(ints(got), vec![4]);
    let (got, (_l, _c, _f)) = attached_query(&lake_dir, "SELECT id FROM t WHERE id > 100;");
    assert!(got.is_empty());
    cleanup(&dir);
}

/// Clearing a spec publishes a version without one; old files keep their
/// recorded values (which are then not interpreted), new appends are
/// unpartitioned, and reads stay exact.
#[test]
fn clearing_the_spec_keeps_files_and_reads_exact() {
    let dir = scratch("clear");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();
    lake.set_partition_spec(
        "main.t",
        &["region".to_string()],
        &CommitSettings::default(),
    )
    .unwrap();
    let report = lake
        .clear_partition_spec("main.t", &CommitSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.tables_repartitioned, 1);
    let again = lake
        .clear_partition_spec("main.t", &CommitSettings::default())
        .unwrap();
    assert!(!again.committed, "clearing twice publishes nothing");

    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(2, Some("b")), (3, Some("c"))]));
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.data_files, 1, "unpartitioned appends stay one file");
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert!(entry.get("partition_by").is_none());
    assert!(entry["files"][1].get("partition").is_none());

    let loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 3);
    let (got, (_l, _c, _f)) = attached_query(&lake_dir, "SELECT id FROM t WHERE region = 'c';");
    assert_eq!(ints(got), vec![3]);
    cleanup(&dir);
}

/// The DuckLake-pinned partition DDL rules: unsupported transforms are
/// refused with DuckLake's own supported set in the message, duplicate keys
/// are refused like DuckLake's "matches an earlier partition key", and a
/// missing table/column is a clean error.
#[test]
fn partition_ddl_errors_match_ducklake() {
    let dir = scratch("ddl_errors");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();

    // DuckLake: "Unsupported partition function truncate - only year, month,
    // day, hour, epoch_year, epoch_month, epoch_day, epoch_hour, and bucket
    // are supported" — this build refuses with the same supported set named.
    for bad in ["truncate(2, region)", "year(dt)", "md5(id)"] {
        let mut commit = Commit::new();
        let err = commit
            .set_partition("main.t", &[bad])
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported partition key"), "{bad}: {err}");
        assert!(err.contains("year, month, day, hour"), "{bad}: {err}");
        assert!(err.contains("bucket"), "{bad}: {err}");
    }
    // DuckLake: duplicate partition key.
    let mut commit = Commit::new();
    let err = commit
        .set_partition("main.t", &["region", "region"])
        .unwrap_err()
        .to_string();
    assert!(err.contains("duplicate partition key"), "{err}");
    assert!(err.contains("earlier partition key"), "{err}");
    // DuckLake: unexpected partition key — column does not exist.
    let err = lake
        .set_partition_spec("main.t", &["nope".to_string()], &CommitSettings::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not exist"), "{err}");
    // Empty specs and unknown tables are clean errors too.
    let err = lake
        .set_partition_spec("main.t", &[], &CommitSettings::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("at least one key"), "{err}");
    let err = lake
        .set_partition_spec("main.ghost", &["x".to_string()], &CommitSettings::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not exist in the lake"), "{err}");
    // A partition key on a table the commit also creates is supported (the
    // DuckLake CREATE TABLE … PARTITIONED BY shape, in one commit).
    let mut fresh = Commit::new();
    fresh.append("main.fresh", Arc::clone(&sub_table(&[(7, Some("x"))])));
    fresh.set_partition("main.fresh", &["region"]).unwrap();
    // The table must not exist for this shape; it does not yet.
    let report = lake.commit(&fresh).unwrap();
    assert_eq!(report.tables_repartitioned, 1);
    let catalog = read_json(&lake_dir.join("catalog.json"));
    assert_eq!(
        catalog["tables"]["main.fresh"]["partition_by"],
        serde_json::json!([{ "column": "region", "transform": "identity" }])
    );
    // The created table's appended file was split under the new spec and
    // carries its partition value.
    assert_eq!(
        catalog["tables"]["main.fresh"]["files"][0]["partition"]["region"],
        "x"
    );
    cleanup(&dir);
}

/// A manifest whose partition value disagrees with the file's own zone map is
/// refused at attach (never a silently pruned matching file), and a swapped
/// in statistics block fails its CRC as before.
#[test]
fn tampered_partition_values_and_zone_maps_are_refused() {
    let dir = scratch("tamper");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();
    lake.set_partition_spec(
        "main.t",
        &["region".to_string()],
        &CommitSettings::default(),
    )
    .unwrap();
    lake.write_from_store(&store(&[(1, Some("east")), (2, Some("west"))]))
        .unwrap();

    // Swap one file's recorded partition value: the zone map still says the
    // file's region is a single value, so attach must refuse.
    let path = lake_dir.join("catalog.json");
    let original = read_json(&path);
    let mut catalog = original.clone();
    let files = catalog["tables"]["main.t"]["files"].as_array_mut().unwrap();
    for f in files.iter_mut() {
        if f["partition"]["region"] == "east" {
            f["partition"]["region"] = serde_json::json!("west");
        }
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
    let err = Lake::open(&lake_dir)
        .unwrap()
        .attach(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("partition check"), "{err}");

    // Restore the original, then flip a bit in the zone map: the CRC catches
    // it (partition values ride outside the block; zone maps ride inside).
    let mut catalog = original;
    catalog["tables"]["main.t"]["files"][0]["stats"]["columns"][0]["min"] =
        serde_json::json!("999");
    std::fs::write(&path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
    let err = Lake::open(&lake_dir)
        .unwrap()
        .attach(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("checksum"), "{err}");
    cleanup(&dir);
}

#[cfg(feature = "vortexlake")]
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredBlock {
    version: u32,
    table: String,
    file: String,
    rows: u64,
    columns: Vec<StoredColumn>,
}

#[cfg(feature = "vortexlake")]
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredColumn {
    name: String,
    dtype: String,
    nulls: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nan: Option<u64>,
}

#[cfg(feature = "vortexlake")]
#[test]
fn vortex_zone_maps_cross_check_against_the_footer() {
    // A catalog zone map that disagrees with the file footer is corrupt:
    // fabricate one by rewriting a block field *and* fixing its CRC (the CRC
    // only detects accidental corruption; the footer is the authority).
    let dir = scratch("vortex_cross");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a")), (2, Some("b"))]))
        .unwrap();

    let path = lake_dir.join("catalog.json");
    let mut catalog = read_json(&path);
    let stats = catalog["tables"]["main.t"]["files"][0]["stats"].clone();
    let mut block: StoredBlock = serde_json::from_value(stats).unwrap();
    block.columns[0].min = Some("0".to_string());
    // `StatsBlock::compute_crc` covers exactly `{version, table, file, rows,
    // columns}` in declaration order; this mirror serializes the same shape.
    let crc = pinkdb::wal::crc32(&serde_json::to_vec(&block).unwrap());
    let stats = &mut catalog["tables"]["main.t"]["files"][0]["stats"];
    stats["columns"][0]["min"] = serde_json::json!("0");
    stats["crc32"] = serde_json::json!(crc);
    std::fs::write(&path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
    let err = Lake::open(&lake_dir)
        .unwrap()
        .attach(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("disagrees with the file's footer"), "{err}");
    cleanup(&dir);
}

/// The commit-delta manifest records a spec change for the audit trail, and
/// the catalog/snapshot pair stays byte-identical.
#[test]
fn partition_changes_are_recorded_in_the_commit_manifest() {
    let dir = scratch("delta");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();
    let report = lake
        .set_partition_spec(
            "main.t",
            &["region".to_string()],
            &CommitSettings::default(),
        )
        .unwrap();
    let delta = read_json(&lake_dir.join(format!("commits/{}.json", report.version)));
    assert_eq!(delta["tables"]["main.t"]["partition_changed"], true);
    assert_eq!(
        delta["tables"]["main.t"]["partition_by"],
        serde_json::json!([{ "column": "region", "transform": "identity" }])
    );
    let catalog = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    let snapshot =
        std::fs::read(lake_dir.join(format!("snapshots/{}.json", report.version))).unwrap();
    assert_eq!(catalog, snapshot);
    cleanup(&dir);
}

/// `Lake::set_partition_spec` errors are `LakeError::NotRepresentable`, the
/// same class the rest of the lake's shape refusals use.
#[test]
fn partition_errors_are_not_representable() {
    let dir = scratch("err_class");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::open_or_create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&[(1, Some("a"))])).unwrap();
    let err = lake
        .set_partition_spec(
            "main.t",
            &["year(x)".to_string()],
            &CommitSettings::default(),
        )
        .unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    cleanup(&dir);
}
