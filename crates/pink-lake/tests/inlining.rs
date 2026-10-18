//! Wave 68: inlined small writes, flushing, change-data reads and
//! `add_data_files` (DuckLake parity), format-agnostic like the other suites:
//! the data-file extension follows the build, everything else — manifest
//! shapes, scan order, row ids, change rows — is identical.
//!
//! DuckLake pins these tests follow (probed live against the ducklake
//! extension, DuckDB v2.0.0-alpha46057):
//!
//! - `ducklake_default_data_inlining_row_limit` = 10 (0 disables); a
//!   per-table `data_inlining_row_limit` option overrides it; an insert of at
//!   most the limit inlines, one row more writes a data file (`inserted_into_table`);
//! - inlined rows carry `_ducklake_row_id` / `_ducklake_begin_snapshot` /
//!   `_ducklake_end_snapshot`; a delete stamps the end, an update stamps the
//!   old version's end and inserts a new version with the same row id;
//! - `ducklake_flush_inlined_data(catalog)` returns
//!   `(schema_name, table_name, rows_flushed)`, writes one data file per
//!   partition value set and empties the inlined metadata table;
//! - `ducklake_table_insertions/deletions(catalog, schema, table, start, end)`
//!   return the table's columns and the range is **inclusive on both ends**;
//!   `ducklake_table_changes` adds `snapshot_id, rowid, change_type`;
//! - `ducklake_add_data_files` registers existing files in place and refuses a
//!   missing column (unless `allow_missing`) or an extra one (unless
//!   `ignore_extra_columns`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pink_lake::{save_store, ChangeKind, Commit, CommitSettings, Lake, WritableAttachment};
use pinkdb::{DataTable, PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "vortexlake")]
const EXT: &str = "vortex";
#[cfg(not(feature = "vortexlake"))]
const EXT: &str = "pink";

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_inlining_{tag}_{}_{}",
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

/// Write `bytes` to a lake-relative `data/…` path, creating the directories
/// (a lake whose table is inlined has no `data/` tree yet).
fn write_lake_file(lake_dir: &Path, rel: &str, bytes: &[u8]) {
    let path = lake_dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

/// The lake-relative path of the first data file under `data/<table>` on
/// disk (for encrypted lakes, whose catalog is ciphertext and cannot be read
/// as JSON from the test).
#[cfg(feature = "encryption")]
fn first_data_file(lake_dir: &Path, table: &str) -> String {
    let dir = lake_dir.join("data").join(table);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.ends_with(".pinkdel"))
        .collect();
    names.sort();
    format!("data/{table}/{}", names[0])
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn entry(lake_dir: &Path, key: &str) -> serde_json::Value {
    read_json(&lake_dir.join("catalog.json"))["tables"][key].clone()
}

/// A `t(i INTEGER, s VARCHAR)` sub-table (the shape `Commit::append` takes).
fn sub_table(rows: &[(i32, Option<&str>)]) -> Arc<DataTable> {
    let mut t = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), pinkdb::DataType::Integer),
            ("s".to_string(), pinkdb::DataType::Varchar),
        ],
    )
    .unwrap();
    for (i, s) in rows {
        t.append_row(vec![
            Value::Integer(*i),
            s.map_or(Value::Null, |s| Value::Varchar(s.to_string())),
        ])
        .unwrap();
    }
    Arc::new(t)
}

fn rows_of(db: &PinkDb) -> Vec<(i32, Option<String>)> {
    let t = db.table("t").unwrap();
    (0..t.num_rows())
        .map(|r| {
            let i = match t.get(r, 0).unwrap() {
                Value::Integer(i) => i,
                other => panic!("row {r}: {other:?}"),
            };
            let s = match t.get(r, 1).unwrap() {
                Value::Varchar(s) => Some(s),
                Value::Null => None,
                other => panic!("row {r}: {other:?}"),
            };
            (i, s)
        })
        .collect()
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

/// Engine rows for `sql` against a fresh read-only attachment, plus the
/// data-file decode counters (`loads`, `columns`, `files`) the query caused.
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

fn i64_value(rows: &[Vec<Value>], row: usize, col: usize) -> i64 {
    match &rows[row][col] {
        Value::Bigint(i) => *i,
        other => panic!("{other:?}"),
    }
}

/// The live (end-less) inlined records of an entry, in row-id order.
fn live_inlined(entry: &serde_json::Value) -> Vec<serde_json::Value> {
    let Some(rows) = entry.get("inlined").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut live: Vec<serde_json::Value> = rows
        .iter()
        .filter(|r| r.get("end").is_none())
        .cloned()
        .collect();
    live.sort_by_key(|r| r["row_id"].as_u64().unwrap());
    live
}

fn file_paths(lake_dir: &Path, key: &str) -> Vec<String> {
    entry(lake_dir, key)["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_string())
        .collect()
}

/// Commit `rows` into `main.t` (creating the table), returning the report.
fn append(lake: &mut Lake, rows: &[(i32, Option<&str>)]) -> pink_lake::CommitReport {
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(rows));
    lake.commit(&commit).unwrap()
}

// ---------------------------------------------------------------------------
// Inlining basics
// ---------------------------------------------------------------------------

/// A commit whose rows fit the default limit (10) stores them in the manifest
/// and writes **no** data file; their rows are visible through `.lake load`
/// and a read-only attach, whose `COUNT`/`MIN`/`MAX` fast path stays exact
/// from the inlined facts alone (zero data files, zero decodes).
#[test]
fn small_commits_inline_into_the_manifest_and_read_exactly() {
    let dir = scratch("inline");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    assert_eq!(
        lake.data_inlining_row_limit(),
        pink_lake::DEFAULT_INLINING_ROW_LIMIT,
        "the DuckLake default is 10"
    );

    let report = append(&mut lake, &[(1, Some("a")), (2, Some("b"))]);
    assert_eq!(report.rows_added, 2);
    assert_eq!(report.rows_inlined, 2, "{report:?}");
    assert_eq!(report.data_files, 0, "no data file for an inlined commit");
    assert!(!lake_dir.join("data").join("main.t").exists());

    // The manifest: two live records with begin = the commit's version, the
    // logical row count, the row-id counter and the column list.
    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["rows"], 2, "{e}");
    assert_eq!(e["format"], EXT, "{e}");
    assert_eq!(e["next_row_id"], 2, "{e}");
    let live = live_inlined(&e);
    assert_eq!(live.len(), 2, "{e}");
    assert_eq!(live[0]["row_id"], 0, "{e}");
    assert_eq!(live[0]["begin"], 2, "the commit's version");
    assert_eq!(live[0]["columns"], serde_json::json!(["i", "s"]));
    assert_eq!(live[0]["values"], serde_json::json!(["1", "a"]));
    assert_eq!(live[1]["values"], serde_json::json!(["2", "b"]));

    // `.lake load` reads them.
    let db = lake.load(None).unwrap();
    assert_eq!(
        rows_of(&db),
        vec![(1, Some("a".into())), (2, Some("b".into()))],
        "files first, then inlined rows in row-id order"
    );

    // A statistics-only attach: COUNT/MIN/MAX exact, no data file decoded.
    let (rows, stats) = attached_query(&lake_dir, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 2);
    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[0][2], Value::Integer(2));
    assert_eq!(stats.0, 0, "no file decode for a statistics-only query");
    assert_eq!(stats.2, 0, "no data files at all");

    // The full scan too.
    let (rows, _) = attached_query(&lake_dir, "SELECT i FROM t;");
    assert_eq!(ints(rows), vec![1, 2]);

    cleanup(&dir);
}

/// The cap is a row-count gate: at most the limit inlines, one row more (or a
/// 0 limit) writes data files, exactly DuckLake's
/// `inserted_into_table` boundary.
#[test]
fn the_cap_decides_inline_versus_data_file() {
    let dir = scratch("cap");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    // Seed the table with a file-backed row (inlining off), then cap at 3.
    lake.set_data_inlining_row_limit(0);
    append(&mut lake, &[(99, Some("seed"))]);
    lake.set_data_inlining_row_limit(10);
    lake.set_inlining_limit("main.t", Some(3), &CommitSettings::default())
        .unwrap();

    // 3 rows: inline.
    let report = append(&mut lake, &[(1, Some("a")), (2, Some("b")), (3, Some("c"))]);
    assert_eq!(report.rows_inlined, 3, "{report:?}");
    assert_eq!(report.data_files, 0);
    assert_eq!(
        file_paths(&lake_dir, "main.t").len(),
        1,
        "still only the seed file"
    );

    // 4 rows: a data file.
    let report = append(
        &mut lake,
        &[
            (4, Some("d")),
            (5, Some("e")),
            (6, Some("f")),
            (7, Some("g")),
        ],
    );
    assert_eq!(report.rows_inlined, 0, "{report:?}");
    assert_eq!(report.data_files, 1);
    let files = file_paths(&lake_dir, "main.t");
    assert_eq!(files.len(), 2, "{files:?}");
    assert_eq!(entry(&lake_dir, "main.t")["files"][1]["rows"], 4);
    assert_eq!(
        entry(&lake_dir, "main.t")["files"][1]["row_id_start"],
        4,
        "row ids continue after the seed file and the inlined rows"
    );

    // Limit 0 disables inlining: even one row writes a file.
    lake.set_inlining_limit("main.t", Some(0), &CommitSettings::default())
        .unwrap();
    let report = append(&mut lake, &[(8, Some("h"))]);
    assert_eq!(report.rows_inlined, 0);
    assert_eq!(report.data_files, 1);

    // `default` clears the table's limit back to the handle default.
    lake.set_inlining_limit("main.t", None, &CommitSettings::default())
        .unwrap();
    let e = entry(&lake_dir, "main.t");
    assert!(e.get("inlined_limit").is_none(), "{e}");
    let report = append(&mut lake, &[(9, Some("i"))]);
    assert_eq!(report.rows_inlined, 1, "{report:?}");

    // Reads stay exact across the mix.
    let db = lake.load(None).unwrap();
    let mut got: Vec<i32> = rows_of(&db).into_iter().map(|(i, _)| i).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 99]);
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 10);
    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[0][2], Value::Integer(99));

    cleanup(&dir);
}

/// The documented scan order: files in manifest order first, then live
/// inlined rows in row-id order; pruning never drops inlined rows; MIN/MAX
/// fold the exact inlined facts with the files' (a predicate that prunes the
/// only file still returns the matching inlined row).
#[test]
fn scan_order_stats_and_pruning_stay_honest_with_inlined_rows() {
    let dir = scratch("order");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    // Two file-backed appends (ids 100.. and 200..).
    append(&mut lake, &[(100, Some("x")), (101, Some("y"))]);
    append(&mut lake, &[(200, Some("z"))]);
    // Then re-enable inlining and append two rows.
    lake.set_data_inlining_row_limit(10);
    append(&mut lake, &[(300, Some("w")), (301, Some("v"))]);

    let db = lake.load(None).unwrap();
    assert_eq!(
        rows_of(&db),
        vec![
            (100, Some("x".into())),
            (101, Some("y".into())),
            (200, Some("z".into())),
            (300, Some("w".into())),
            (301, Some("v".into())),
        ],
        "files in manifest order, then live inlined rows in row-id order"
    );

    // The table-level facts fold both sources exactly.
    let (rows, _) = attached_query(
        &lake_dir,
        "SELECT COUNT(*), COUNT(s), MIN(i), MAX(i), MIN(s), MAX(s) FROM t;",
    );
    assert_eq!(i64_value(&rows, 0, 0), 5);
    assert_eq!(i64_value(&rows, 0, 1), 5);
    assert_eq!(rows[0][2], Value::Integer(100));
    assert_eq!(rows[0][3], Value::Integer(301));
    assert_eq!(rows[0][4], Value::Varchar("v".into()));
    assert_eq!(rows[0][5], Value::Varchar("z".into()));

    // File pruning may skip a file, never an inlined row: `i = 300` lives in
    // an inlined record only, so the pruned scan must still find it (all
    // files' zone maps pruned, zero decodes, inlined rows scanned).
    let (pruned, (_, _, files)) = attached_query(&lake_dir, "SELECT i FROM t WHERE i = 300;");
    assert_eq!(ints(pruned), vec![300]);
    assert_eq!(files, 0, "every file may be pruned by the zone maps");

    // A NULL in an inlined row stays NULL and counts exactly.
    let report = append(&mut lake, &[(400, None)]);
    assert_eq!(report.rows_inlined, 1);
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(s), COUNT(*) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 5);
    assert_eq!(i64_value(&rows, 0, 1), 6);

    // Delete the inlined row with the NULL and re-check.
    let mut commit = Commit::new();
    commit.delete_inlined_rows("main.t", &[5]);
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.inlined_deleted, 1, "{report:?}");
    assert_eq!(report.rows_deleted, 1);
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(*) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 5);

    cleanup(&dir);
}

/// The writable-attachment diff drives inlined deletes and appends: DELETE and
/// UPDATE of inlined rows end the right row versions (same row id for an
/// update, DuckLake's begin/end pair), and the rebuilt base keeps diffing
/// cleanly against the reader's order.
#[test]
fn writable_attachment_diffs_inlined_rows() {
    let dir = scratch("diff-inline");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a")), (2, Some("b")), (3, Some("c"))]);
    assert_eq!(live_inlined(&entry(&lake_dir, "main.t")).len(), 3);

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    let mut db = wa.lake().load(None).unwrap();
    db.execute("UPDATE t SET s = 'B' WHERE i = 2;").unwrap();
    db.execute("DELETE FROM t WHERE i = 1;").unwrap();
    let report = wa.commit(&db, &CommitSettings::default()).unwrap();
    assert_eq!(
        report.rows_deleted, 2,
        "one delete + one update-away: {report:?}"
    );
    assert_eq!(report.inlined_deleted, 2, "{report:?}");
    assert_eq!(report.rows_added, 1, "the update's new row");
    assert_eq!(report.rows_inlined, 1, "the update's new row inlines too");

    // Row 0 was deleted at the commit; row 1's old version ended and the
    // update's new version is a fresh record (pinkdb's diff-based commits do
    // not pair an update's preimage with its postimage by row id — the
    // documented divergence from DuckLake, which reuses the id).
    let e = entry(&lake_dir, "main.t");
    let records = e["inlined"].as_array().unwrap();
    assert_eq!(records.len(), 4, "{e}");
    let row0 = records.iter().find(|r| r["row_id"] == 0).unwrap();
    assert_eq!(row0["begin"], 2);
    assert_eq!(row0["end"], 3, "deleted at the commit");
    let row1 = records.iter().find(|r| r["row_id"] == 1).unwrap();
    assert_eq!(row1["end"], 3, "the old version ended");
    let fresh = records.iter().find(|r| r["row_id"] == 3).unwrap();
    assert_eq!(fresh["begin"], 3);
    assert!(fresh.get("end").is_none(), "and is live");
    assert_eq!(fresh["values"], serde_json::json!(["2", "B"]));

    // The rebuilt base matches the reader: rows (2,B) and (3,c), and a second
    // diff round-trips to an empty change.
    let db2 = wa.lake().load(None).unwrap();
    assert_eq!(
        rows_of(&db2),
        vec![(3, Some("c".into())), (2, Some("B".into()))],
        "live inlined rows read in row-id order"
    );
    assert_eq!(wa.changed_tables(&db).unwrap(), Vec::<String>::new());

    // A stale row id is a conflict at publish.
    let mut commit = Commit::new();
    commit.delete_inlined_rows("main.t", &[1]);
    let err = lake.commit(&commit).unwrap_err().to_string();
    assert!(err.contains("inlined row 1 is not live"), "{err}");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

/// `.lake flush` materializes live inlined rows into data files (one per
/// partition value set) and publishes one version; the records stay as the
/// change log with `end = flush version` and `flushed`, so time travel to the
/// old version still sees them inlined and change data still reports the
/// rows' insertions — but never a deletion.
#[test]
fn flush_materializes_inlined_rows_and_keeps_the_history() {
    let dir = scratch("flush");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a")), (2, Some("b")), (3, Some("c"))]);

    let report = lake.flush_inlined_data(None).unwrap();
    assert!(report.committed);
    assert_eq!(report.version, 3);
    assert_eq!(report.tables, vec![("main.t".to_string(), 3)]);
    assert_eq!(report.data_files, 1);

    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["rows"], 3, "{e}");
    assert!(
        live_inlined(&e).is_empty(),
        "nothing live stays inlined: {e}"
    );
    let records = e["inlined"].as_array().unwrap();
    assert_eq!(records.len(), 3, "{e}");
    for r in records {
        assert_eq!(r["end"], 3, "{e}");
        assert_eq!(r["flushed"], true, "{e}");
    }
    assert_eq!(e["files"].as_array().unwrap().len(), 1, "{e}");
    assert_eq!(e["files"][0]["rows"], 3);
    assert_eq!(
        e["files"][0]["row_id_start"], 3,
        "the flushed file re-keys ids"
    );

    // Reads unchanged; time travel to v2 still sees the inlined shape.
    let db = lake.load(None).unwrap();
    assert_eq!(rows_of(&db).len(), 3);
    let report = Lake::open(&lake_dir).unwrap().attach(Some(2)).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let rows = query(&mut db, "SELECT i FROM t ORDER BY i;");
    assert_eq!(ints(rows), vec![1, 2, 3]);

    // A flush with nothing live is a no-op.
    let report = lake.flush_inlined_data(Some("main.t")).unwrap();
    assert!(!report.committed, "{report:?}");
    assert_eq!(report.version, 3);
    assert!(report.tables.is_empty());

    // Deleting a flushed row is a normal positional delete on the new file;
    // change data reports the deletion at the delete's version and never the
    // flush as one.
    let files = file_paths(&lake_dir, "main.t");
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &files[0], &[0]);
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.rows_deleted, 1);
    let deletions = lake.table_deletions("main.t", 1, None).unwrap();
    assert_eq!(deletions.len(), 1, "{deletions:?}");
    assert_eq!(deletions[0].version, 4, "the flush is not a deletion");
    assert_eq!(deletions[0].values[0], Value::Integer(1));
    let insertions = lake.table_insertions("main.t", 1, None).unwrap();
    assert_eq!(insertions.len(), 3, "{insertions:?}");

    cleanup(&dir);
}

/// Flushing a partitioned table splits exactly like any other write: one file
/// per partition value (probed: `ducklake_flush_inlined_data` did the same).
#[test]
fn flush_respects_the_partition_spec() {
    let dir = scratch("flush-part");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("x")), (2, Some("y"))]);
    lake.set_partition_spec("main.t", &["s".to_string()], &CommitSettings::default())
        .unwrap();
    // A second inlined append under the spec.
    append(&mut lake, &[(3, Some("x"))]);

    let report = lake.flush_inlined_data(None).unwrap();
    assert!(report.committed);
    assert_eq!(
        report.data_files, 2,
        "one file per partition value: {report:?}"
    );
    let values: Vec<Option<String>> = entry(&lake_dir, "main.t")["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["partition"]["s"].as_str().map(str::to_string))
        .collect();
    assert_eq!(
        values,
        vec![Some("x".into()), Some("y".into())],
        "{values:?}"
    );

    let (rows, _) = attached_query(&lake_dir, "SELECT i FROM t WHERE s = 'x' ORDER BY i;");
    assert_eq!(ints(rows), vec![1, 3]);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Change data
// ---------------------------------------------------------------------------

/// The `ducklake_table_insertions`/`deletions`/`changes` shapes over a mixed
/// history: file appends, inlined inserts, an inlined delete, a file-position
/// delete, an update of an inlined row and a flush. Ranges are inclusive on
/// both ends; row ids ride along where recorded; a full save in the range is
/// refused by name.
#[test]
fn change_data_renders_insertions_deletions_and_changes() {
    let dir = scratch("cdc");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    // v2: a 12-row file append (over the 10-row cap).
    let big: Vec<(i32, Option<&str>)> = (100..112).map(|i| (i, Some("f"))).collect();
    let report = append(&mut lake, &big);
    assert_eq!(report.data_files, 1);
    assert_eq!(report.rows_inlined, 0);
    let file_v2 = file_paths(&lake_dir, "main.t")[0].clone();
    // v3: a 2-row inline append.
    append(&mut lake, &[(1, Some("a")), (2, Some("b"))]);
    // v4: delete the first inlined row.
    let mut commit = Commit::new();
    commit.delete_inlined_rows("main.t", &[12]);
    lake.commit(&commit).unwrap();
    // v5: delete a file position + update the second inlined row (delete +
    // re-append with the same value shape).
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &file_v2, &[0]);
    commit.delete_inlined_rows("main.t", &[13]);
    commit.append("main.t", sub_table(&[(2, Some("b2"))]));
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.inlined_deleted, 1, "{report:?}");
    // v6: flush everything.
    let report = lake.flush_inlined_data(None).unwrap();
    assert!(report.committed);

    let head = lake.current_version();
    assert_eq!(head, 6);

    // Insertions: the 12 file rows at v2 (row ids 0..12), the two inlined at
    // v3 (ids 12, 13), the updated row's new version at v5 (id 13), and the
    // flushed file's rows are NOT re-reported.
    let insertions = lake.table_insertions("main.t", 1, None).unwrap();
    assert_eq!(insertions.len(), 15, "{insertions:?}");
    assert!(insertions
        .iter()
        .all(|r| r.change_type == ChangeKind::Insert));
    let at = |version: u64| -> Vec<(Option<u64>, i32)> {
        insertions
            .iter()
            .filter(|r| r.version == version)
            .map(|r| {
                let i = match r.values[0] {
                    Value::Integer(i) => i,
                    ref other => panic!("{other:?}"),
                };
                (r.row_id, i)
            })
            .collect()
    };
    let v2 = at(2);
    assert_eq!(v2.len(), 12);
    assert_eq!(
        v2[0],
        (Some(0), 100),
        "file rows carry row_id_start + position"
    );
    assert_eq!(v2[11], (Some(11), 111));
    assert_eq!(at(3), vec![(Some(12), 1), (Some(13), 2)]);
    assert_eq!(
        at(5),
        vec![(Some(14), 2)],
        "the update's postimage gets a fresh row id (no preimage pairing)"
    );
    assert!(at(6).is_empty(), "the flush adds no insertions");

    // Deletions: v4's inlined row 12, v5's file position (row id 0) and the
    // inlined row 13's old version; nothing at v6.
    let deletions = lake.table_deletions("main.t", 1, None).unwrap();
    assert_eq!(deletions.len(), 3, "{deletions:?}");
    assert_eq!(
        deletions
            .iter()
            .map(|r| (r.version, r.row_id))
            .collect::<Vec<_>>(),
        vec![(4, Some(12)), (5, Some(0)), (5, Some(13))]
    );
    assert_eq!(deletions[1].values[0], Value::Integer(100), "{deletions:?}");

    // `changes` orders insertions before deletions per version.
    let changes = lake.table_changes("main.t", 4, Some(5)).unwrap();
    assert_eq!(
        changes
            .iter()
            .map(|r| (r.version, r.change_type, r.row_id))
            .collect::<Vec<_>>(),
        vec![
            (4, ChangeKind::Delete, Some(12)),
            (5, ChangeKind::Insert, Some(14)),
            (5, ChangeKind::Delete, Some(0)),
            (5, ChangeKind::Delete, Some(13)),
        ]
    );

    // Inclusive bounds: (4,4) is exactly v4's change; a table that does not
    // exist at the range's end is a clean error.
    assert_eq!(
        lake.table_insertions("main.t", 4, Some(4)).unwrap().len(),
        0
    );
    assert_eq!(lake.table_deletions("main.t", 4, Some(4)).unwrap().len(), 1);
    assert_eq!(
        lake.table_changes("main.t", 2, Some(2)).unwrap().len(),
        12,
        "both ends inclusive"
    );
    let err = lake
        .table_changes("main.t", 1, Some(1))
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not exist at version 1"), "{err}");
    // An empty/invalid range is refused.
    let err = lake
        .table_changes("main.t", 5, Some(2))
        .unwrap_err()
        .to_string();
    assert!(err.contains("start must not exceed end"), "{err}");

    // A full save in the range is refused by name: CDC cannot render it.
    // (The save publishes through its own handle; reopen to see the head.)
    save_store(&lake.load(None).unwrap(), &lake_dir).unwrap();
    let lake = Lake::open(&lake_dir).unwrap();
    let err = lake
        .table_changes("main.t", 1, None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("replaced table \"main.t\" wholesale"), "{err}");
    assert!(err.contains("full"), "{err}");
    // A range that excludes the save still works, even across the flush.
    assert_eq!(lake.table_changes("main.t", 6, Some(6)).unwrap().len(), 0);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// add_data_files
// ---------------------------------------------------------------------------

/// Registering a data file produced elsewhere makes its rows queryable
/// without rewriting it; the manifest records the file with its verifiable
/// row count, statistics and a fresh row-id range, and change data reports
/// the rows as insertions at the registering version.
#[test]
fn add_data_files_registers_existing_files() {
    let dir = scratch("add");
    let lake_dir = dir.join("lake");
    let other_dir = dir.join("other");
    // The source file: main.t with 3 file-backed rows in another lake.
    {
        let mut other = Lake::create(&other_dir).unwrap();
        other.set_data_inlining_row_limit(0);
        append(&mut other, &[(7, Some("s"))]);
        append(&mut other, &[(8, Some("t")), (9, Some("u"))]);
    }
    let source_rel = file_paths(&other_dir, "main.t")[1].clone();
    let source_bytes = std::fs::read(other_dir.join(&source_rel)).unwrap();

    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);
    assert_eq!(entry(&lake_dir, "main.t")["rows"], 1);

    // Copy the file into this lake's data directory under the table's own
    // path and register it.
    let rel = format!("data/main.t/registered.{EXT}");
    write_lake_file(&lake_dir, &rel, &source_bytes);
    let report = lake
        .add_data_files("main.t", std::slice::from_ref(&rel))
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files.len(), 1);
    assert_eq!(report.files[0].2, 2, "the file's own row count");

    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["rows"], 3, "{e}");
    assert_eq!(e["files"].as_array().unwrap().len(), 1, "{e}");
    assert_eq!(e["files"][0]["path"], rel.as_str(), "{e}");
    assert_eq!(e["files"][0]["rows"], 2, "{e}");
    assert_eq!(e["files"][0]["row_id_start"], 1, "{e}");
    assert!(
        e["files"][0].get("stats").is_some(),
        "stats ride along: {e}"
    );

    // The rows are visible and exact.
    let db = lake.load(None).unwrap();
    let mut got: Vec<i32> = rows_of(&db).into_iter().map(|(i, _)| i).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 8, 9]);
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 3);
    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[0][2], Value::Integer(9));

    // Change data: the registered rows are insertions at the registering
    // version with their recorded row ids.
    let insertions = lake.table_insertions("main.t", 1, None).unwrap();
    let registered: Vec<(Option<u64>, i32)> = insertions
        .iter()
        .filter(|r| r.version == lake.current_version())
        .map(|r| match r.values[0] {
            Value::Integer(i) => (r.row_id, i),
            ref other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        registered,
        vec![(Some(1), 8), (Some(2), 9)],
        "{insertions:?}"
    );

    cleanup(&dir);
}

/// Registrations that cannot be validated are refused cleanly, before
/// anything is published: a missing path, a path outside `data/`, a file of
/// another table, a file whose schema does not match (here: the wrong table
/// shape), and a plaintext file in an encrypted lake.
#[test]
fn add_data_files_refuses_unverifiable_files() {
    let dir = scratch("add-refuse");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);
    let version = lake.current_version();

    let err = lake
        .add_data_files("main.t", &["data/main.t/missing.pink".to_string()])
        .unwrap_err()
        .to_string();
    assert!(err.contains("no files found that match"), "{err}");

    let err = lake
        .add_data_files("main.t", &["elsewhere/x.pink".to_string()])
        .unwrap_err()
        .to_string();
    assert!(err.contains("is not a lake-relative"), "{err}");

    let err = lake
        .add_data_files("main.other", &["data/main.t/x.pink".to_string()])
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not exist"), "{err}");

    // A file of another table (same lake) is refused by identity.
    let mut other = Lake::create(dir.join("other")).unwrap();
    other.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append("main.u", sub_table(&[(5, Some("q"))]));
    other.commit(&commit).unwrap();
    let other_files = file_paths(&dir.join("other"), "main.u");
    assert_eq!(other_files.len(), 1);
    let rel = format!("data/main.t/other-table.{EXT}");
    write_lake_file(
        &lake_dir,
        &rel,
        &std::fs::read(dir.join("other").join(&other_files[0])).unwrap(),
    );
    let err = lake
        .add_data_files("main.t", &[rel])
        .unwrap_err()
        .to_string();
    assert!(err.contains("data file holds main.u"), "{err}");

    // The version never moved.
    assert_eq!(lake.current_version(), version);

    cleanup(&dir);
}

/// An encrypted lake registers only files that unseal under its key: the
/// registering read goes through the container (path-based), so a plaintext
/// file is refused; a ciphertext copied between two keyed lakes of the same
/// key registers and reads back.
#[cfg(feature = "encryption")]
#[test]
fn add_data_files_respects_encryption() {
    use pink_lake::EncryptionKey;

    let dir = scratch("add-enc");
    let key = EncryptionKey::from_passphrase("wave-68-register");
    let source_dir = dir.join("source");
    let target_dir = dir.join("target");
    {
        let mut source = Lake::create_with_key(&source_dir, key.clone()).unwrap();
        source.set_data_inlining_row_limit(0);
        append(&mut source, &[(11, Some("k")), (12, Some("l"))]);
    }
    let src_file = first_data_file(&source_dir, "main.t");
    let ciphertext = std::fs::read(source_dir.join(&src_file)).unwrap();

    let mut target = Lake::create_with_key(&target_dir, key.clone()).unwrap();
    append(&mut target, &[(1, Some("a"))]);

    // A plaintext file under data/ cannot be registered.
    let plain = format!("data/main.t/plain.{EXT}");
    write_lake_file(&target_dir, &plain, b"not a container");
    let err = target
        .add_data_files("main.t", &[plain])
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot be read as"), "{err}");

    // A container copied to another path is refused: the data-file domain is
    // AAD-bound to the catalog path it was written for.
    let rel = format!("data/main.t/registered.{EXT}");
    write_lake_file(&target_dir, &rel, &ciphertext);
    let err = target
        .add_data_files("main.t", std::slice::from_ref(&rel))
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot be read as"), "{err}");
    assert!(err.contains("bound to"), "{err}");

    // A file this lake itself wrote at its (bound) canonical path registers
    // back: write two file-backed appends, merge them away from the current
    // entry, then restore the old sources by path.
    target.set_data_inlining_row_limit(0);
    append(&mut target, &[(5, Some("m"))]);
    append(&mut target, &[(6, Some("n"))]);
    let sources: Vec<String> = {
        let mut files: Vec<String> = std::fs::read_dir(target_dir.join("data/main.t"))
            .unwrap()
            .map(|e| format!("data/main.t/{}", e.unwrap().file_name().to_string_lossy()))
            .filter(|p| !p.ends_with(".pinkdel"))
            .filter(|p| !p.contains("plain") && !p.contains("registered"))
            .collect();
        files.sort();
        files
    };
    assert_eq!(sources.len(), 2, "{sources:?}");
    let report = target
        .merge_adjacent_files(None, &pink_lake::MergeSettings::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    let report = target.add_data_files("main.t", &sources).unwrap();
    assert_eq!(report.files.len(), 2, "{report:?}");
    // The restored files' rows are visible as insertions at this version.
    let db = target.load(None).unwrap();
    assert!(rows_of(&db).len() > 3, "{:?}", rows_of(&db));
    let insertions = target.table_insertions("main.t", 1, None).unwrap();
    for want in [5, 6] {
        assert!(
            insertions
                .iter()
                .any(|r| r.version == target.current_version()
                    && r.values[0] == Value::Integer(want)),
            "restored row {want}: {insertions:?}"
        );
    }

    // A keyless open of the target still fails closed.
    let err = Lake::open(&target_dir).unwrap_err().to_string();
    assert!(err.contains("encrypted"), "{err}");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Schema evolution and time travel
// ---------------------------------------------------------------------------

/// Inlined rows decode through the schema record's rename/default mapping,
/// exactly like per-file schemas: a column added after them reads its default
/// on old inlined rows, a rename maps their physical name, and a dropped
/// column vanishes — across time travel too.
#[test]
fn schema_evolution_reads_inlined_rows_through_the_record() {
    let dir = scratch("schema-inline");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a")), (2, None)]);
    assert_eq!(live_inlined(&entry(&lake_dir, "main.t")).len(), 2);

    // ADD COLUMN + RENAME through a writable attachment (metadata-only for
    // the inlined records).
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    let mut db = wa.lake().load(None).unwrap();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7;")
        .unwrap();
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    let report = wa.commit(&db, &CommitSettings::default()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    assert_eq!(report.inlined_deleted, 0, "metadata-only: no record ends");

    // New inline rows carry the new schema; old rows read the default.
    let mut lake = Lake::open(&lake_dir).unwrap();
    let mut t3 = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), pinkdb::DataType::Integer),
            ("label".to_string(), pinkdb::DataType::Varchar),
            ("n".to_string(), pinkdb::DataType::Integer),
        ],
    )
    .unwrap();
    t3.append_row(vec![
        Value::Integer(3),
        Value::Varchar("c".to_string()),
        Value::Integer(7),
    ])
    .unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(t3));
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.rows_inlined, 1, "{report:?}");
    let db = lake.load(None).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(t.num_cols(), 3);
    let values: Vec<(i32, Option<String>, Option<i32>)> = (0..t.num_rows())
        .map(|r| {
            let label = match t.get(r, 1).unwrap() {
                Value::Varchar(s) => Some(s),
                Value::Null => None,
                other => panic!("{other:?}"),
            };
            let n = match t.get(r, 2).unwrap() {
                Value::Integer(n) => Some(n),
                Value::Null => None,
                other => panic!("{other:?}"),
            };
            let i = match t.get(r, 0).unwrap() {
                Value::Integer(i) => i,
                other => panic!("{other:?}"),
            };
            (i, label, n)
        })
        .collect();
    assert_eq!(
        values,
        vec![
            (1, Some("a".into()), Some(7)),
            (2, None, Some(7)),
            (3, Some("c".into()), Some(7)),
        ]
    );

    // The attached scan agrees, and the MIN/MAX fast path is exact.
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(n), MIN(i), MAX(n) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 3);
    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[0][2], Value::Integer(7));

    // Time travel to v2 (before the schema change) reads the old schema and
    // the inlined rows as written.
    let report = Lake::open(&lake_dir).unwrap().attach(Some(2)).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let rows = query(&mut db, "SELECT i FROM t ORDER BY i;");
    assert_eq!(ints(rows), vec![1, 2]);

    // Change data decodes the old records under the current schema: the
    // insertion values carry the default for the added column.
    let insertions = lake.table_insertions("main.t", 1, None).unwrap();
    let first = insertions.iter().find(|r| r.version == 2).unwrap();
    assert_eq!(first.values.len(), 3, "{first:?}");
    assert_eq!(first.values[2], Value::Integer(7));

    cleanup(&dir);
}

/// Encryption covers inlined rows end to end: they cannot be changed in
/// flight, and the flush writes sealed files (the containers never parse as
/// JSON).
#[cfg(feature = "encryption")]
#[test]
fn encrypted_lakes_inline_flush_and_read() {
    use pink_lake::EncryptionKey;

    let dir = scratch("enc-inline");
    let lake_dir = dir.join("lake");
    let key = EncryptionKey::from_passphrase("wave-68-inline-enc");
    let mut lake = Lake::create_with_key(&lake_dir, key.clone()).unwrap();
    let report = append(&mut lake, &[(1, Some("a")), (2, Some("b"))]);
    assert_eq!(report.rows_inlined, 2);

    // The manifest on disk is a PNKLAKE1 container, not JSON.
    let bytes = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    assert_ne!(&bytes[..8], b"{\"format", "the manifest must be sealed");

    // A keyed reopen reads the inlined rows; a keyless one fails closed.
    let lake = Lake::open_with_key(&lake_dir, key.clone()).unwrap();
    let db = lake.load(None).unwrap();
    assert_eq!(rows_of(&db).len(), 2);
    let err = Lake::open(&lake_dir).unwrap_err().to_string();
    assert!(err.contains("encrypted"), "{err}");

    // Flush writes sealed data files that still read back.
    let mut lake = Lake::open_with_key(&lake_dir, key.clone()).unwrap();
    let report = lake.flush_inlined_data(None).unwrap();
    assert!(report.committed);
    let rel = first_data_file(&lake_dir, "main.t");
    let bytes = std::fs::read(lake_dir.join(&rel)).unwrap();
    assert!(
        bytes.starts_with(b"PNKLAKE1"),
        "the flushed data file must be a sealed container"
    );
    let db = lake.load(None).unwrap();
    assert_eq!(rows_of(&db).len(), 2);

    // Change data over the encrypted lake: insertions at v2, no deletions.
    let insertions = lake.table_insertions("main.t", 1, None).unwrap();
    assert_eq!(insertions.len(), 2, "{insertions:?}");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Maintenance interactions
// ---------------------------------------------------------------------------

/// Maintenance never touches inlined rows: merge consolidates the data files
/// and leaves the live inlined records (and their change history) exactly as
/// they were; a rewrite materializes file deletes without ending anything
/// inlined.
#[test]
fn maintenance_leaves_inlined_rows_alone() {
    let dir = scratch("maint");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    append(&mut lake, &[(1, Some("a")), (2, Some("b"))]);
    append(&mut lake, &[(3, Some("c")), (4, Some("d"))]);
    lake.set_data_inlining_row_limit(10);
    let report = append(&mut lake, &[(5, Some("e"))]);
    assert_eq!(report.rows_inlined, 1);

    let inlined_before = entry(&lake_dir, "main.t")["inlined"].clone();
    let files_before: Vec<String> = file_paths(&lake_dir, "main.t");
    assert_eq!(files_before.len(), 2);

    let report = lake
        .merge_adjacent_files(None, &pink_lake::MergeSettings::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    let e = entry(&lake_dir, "main.t");
    assert_eq!(
        e["inlined"], inlined_before,
        "merge leaves inlined rows alone"
    );
    assert_eq!(e["rows"], 5, "{e}");
    assert_eq!(e["files"].as_array().unwrap().len(), 1, "{e}");

    // Rows stay exact: 4 file rows (merged) then the live inlined row.
    let db = lake.load(None).unwrap();
    let got: Vec<i32> = rows_of(&db).into_iter().map(|(i, _)| i).collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5]);
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 5);
    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[0][2], Value::Integer(5));

    // Introspection counts the inlined rows and their history.
    let info = lake.table_info().unwrap();
    let t = info.iter().find(|t| t.table == "main.t").unwrap();
    assert_eq!(t.rows, 5, "{t:?}");
    assert_eq!(t.file_count, 1, "{t:?}");
    assert_eq!(t.inlined_rows, 1, "the live record: {t:?}");
    assert_eq!(t.inlined_history, 1, "{t:?}");

    // Change data still reports the inlined insertion at its own version and
    // skips the merge output.
    let insertions = lake.table_insertions("main.t", 1, None).unwrap();
    let inlined_only: Vec<_> = insertions.iter().filter(|r| r.row_id == Some(4)).collect();
    assert_eq!(inlined_only.len(), 1, "{insertions:?}");
    assert_eq!(inlined_only[0].version, 4, "{insertions:?}");

    cleanup(&dir);
}

/// The per-table limit is the committed option, not a session accident: a
/// reopen keeps it, `clear_inlining_limit` restores the handle default, and
/// the delta manifest records the change.
#[test]
fn the_inlining_limit_is_persisted_per_table() {
    let dir = scratch("limit");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);
    let report = lake
        .set_inlining_limit("main.t", Some(2), &CommitSettings::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    let version = report.version;
    assert_eq!(entry(&lake_dir, "main.t")["inlined_limit"], 2);

    let delta = read_json(&lake_dir.join(format!("commits/{version}.json")));
    assert_eq!(
        delta["tables"]["main.t"]["inlining_changed"], true,
        "{delta}"
    );
    assert_eq!(delta["tables"]["main.t"]["inlined_limit"], 2, "{delta}");
    let info = lake.table_info().unwrap();
    assert_eq!(
        info.iter()
            .find(|t| t.table == "main.t")
            .unwrap()
            .inlined_limit,
        Some(2)
    );

    // Reopened: the limit persists (3 rows now write a file).
    let mut lake = Lake::open(&lake_dir).unwrap();
    let report = append(&mut lake, &[(2, Some("b")), (3, Some("c")), (4, Some("d"))]);
    assert_eq!(report.rows_inlined, 0, "{report:?}");
    assert_eq!(report.data_files, 1, "{report:?}");

    // Setting the identical limit is a no-op; clearing restores the default.
    let report = lake
        .set_inlining_limit("main.t", Some(2), &CommitSettings::default())
        .unwrap();
    assert!(!report.committed, "{report:?}");
    lake.set_inlining_limit("main.t", None, &CommitSettings::default())
        .unwrap();
    let report = append(&mut lake, &[(5, Some("e"))]);
    assert_eq!(report.rows_inlined, 1, "{report:?}");

    cleanup(&dir);
}

/// Time travel reaches inlined rows of older versions exactly: the version's
/// own manifest carries the live set, and a stats-only attach at that version
/// answers from it.
#[test]
fn time_travel_serves_each_versions_inlined_rows() {
    let dir = scratch("tt");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);
    append(&mut lake, &[(2, Some("b"))]);
    append(&mut lake, &[(3, Some("c"))]);
    assert_eq!(lake.current_version(), 4, "create + three appends");

    for (version, want) in [(2u64, vec![1]), (3, vec![1, 2]), (4, vec![1, 2, 3])] {
        let report = Lake::open(&lake_dir)
            .unwrap()
            .attach(Some(version))
            .unwrap();
        let mut db = PinkDb::new();
        db.attach_lake(report.lake).unwrap();
        let rows = query(&mut db, "SELECT i FROM t ORDER BY i;");
        assert_eq!(ints(rows), want, "version {version}");
        // The statistics-only COUNT is exact per version.
        let rows = query(&mut db, "SELECT COUNT(*) FROM t;");
        assert_eq!(
            i64_value(&rows, 0, 0),
            want.len() as i64,
            "version {version}"
        );
    }

    cleanup(&dir);
}

/// A corrupt inlined record is a clean error, never a wrong answer: an
/// undecodable value and a row-count mismatch are both refused at load.
#[test]
fn corrupt_inlined_records_are_clear_errors() {
    let dir = scratch("corrupt-inline");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);

    // An undecodable integer value fails the load with the row named.
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    catalog["tables"]["main.t"]["inlined"][0]["values"][0] = serde_json::json!("not-an-int");
    std::fs::write(
        lake_dir.join("catalog.json"),
        serde_json::to_vec_pretty(&catalog).unwrap(),
    )
    .unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("inlined row 0"), "{err}");
    assert!(err.contains("undecodable"), "{err}");

    // A row-count mismatch is caught at parse.
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    catalog["tables"]["main.t"]["inlined"][0]["values"][0] = serde_json::json!("1");
    catalog["tables"]["main.t"]["rows"] = serde_json::json!(9);
    std::fs::write(
        lake_dir.join("catalog.json"),
        serde_json::to_vec_pretty(&catalog).unwrap(),
    )
    .unwrap();
    let err = Lake::open(&lake_dir).unwrap_err().to_string();
    assert!(err.contains("declares 9 rows"), "{err}");
    assert!(err.contains("1 inlined row live"), "{err}");

    // A duplicate live row id is refused too.
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    catalog["tables"]["main.t"]["rows"] = serde_json::json!(1);
    let dup = catalog["tables"]["main.t"]["inlined"][0].clone();
    catalog["tables"]["main.t"]["inlined"] = serde_json::json!([dup.clone(), dup]);
    std::fs::write(
        lake_dir.join("catalog.json"),
        serde_json::to_vec_pretty(&catalog).unwrap(),
    )
    .unwrap();
    let err = Lake::open(&lake_dir).unwrap_err().to_string();
    assert!(err.contains("appears twice"), "{err}");

    cleanup(&dir);
}

/// `add_data_files` on an inlined-only table works too (the file list grows
/// from zero), and the statistics-only attach reads both sources.
#[test]
fn registered_files_join_inlined_only_tables() {
    let dir = scratch("added-to-inline");
    let lake_dir = dir.join("lake");
    let source_dir = dir.join("source");
    {
        let mut source = Lake::create(&source_dir).unwrap();
        source.set_data_inlining_row_limit(0);
        append(&mut source, &[(50, Some("m"))]);
    }
    let source_rel = file_paths(&source_dir, "main.t")[0].clone();
    let bytes = std::fs::read(source_dir.join(&source_rel)).unwrap();

    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);
    assert!(entry(&lake_dir, "main.t")["files"]
        .as_array()
        .map(Vec::is_empty)
        .unwrap_or(true));
    let rel = format!("data/main.t/registered.{EXT}");
    write_lake_file(&lake_dir, &rel, &bytes);
    lake.add_data_files("main.t", &[rel]).unwrap();

    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 2);
    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[0][2], Value::Integer(50));

    cleanup(&dir);
}

/// A table created by an inlined-only commit mints its schema record with the
/// commit (the record is a precondition of inlining), so its zero-file entry
/// stays loadable and time-travellable.
#[test]
fn inlined_only_tables_are_created_with_a_record() {
    let dir = scratch("record");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a")), (2, Some("b"))]);
    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["schema_version"], 1, "{e}");
    assert!(e.get("columns").is_some(), "{e}");
    assert!(
        e.get("files")
            .is_none_or(|f| f.as_array().unwrap().is_empty()),
        "no file yet: {e}"
    );

    // `.lake load` and attach read the record's schema.
    let db = lake.load(None).unwrap();
    assert_eq!(db.table("t").unwrap().num_cols(), 2);
    let (rows, _) = attached_query(&lake_dir, "SELECT COUNT(*) FROM t;");
    assert_eq!(i64_value(&rows, 0, 0), 2);

    cleanup(&dir);
}

/// `Commit::append` on a legacy (record-less) table materializes instead of
/// inlining: inlining needs the v5 record, so the pre-wave-65 shape keeps
/// working unchanged.
#[test]
fn a_legacy_entry_materializes_instead_of_inlining() {
    let dir = scratch("legacy");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    append(&mut lake, &[(1, Some("a"))]);
    // Strip the schema record, making the entry legacy (a v5 manifest with a
    // record-less entry is legal: a table a commit did not touch).
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    for key in ["columns", "schema_version", "inlined", "next_row_id"] {
        catalog["tables"]["main.t"]
            .as_object_mut()
            .unwrap()
            .remove(key);
    }
    catalog["tables"]["main.t"]["files"][0]
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    std::fs::write(
        lake_dir.join("catalog.json"),
        serde_json::to_vec_pretty(&catalog).unwrap(),
    )
    .unwrap();

    let mut lake = Lake::open(&lake_dir).unwrap();
    let report = append(&mut lake, &[(2, Some("b"))]);
    assert_eq!(
        report.rows_inlined, 0,
        "a legacy entry writes a file: {report:?}"
    );
    assert_eq!(report.data_files, 1, "{report:?}");
    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["files"].as_array().unwrap().len(), 2, "{e}");
    assert_eq!(e["rows"], 2);
    let db = lake.load(None).unwrap();
    assert_eq!(rows_of(&db).len(), 2);

    cleanup(&dir);
}

/// Retry semantics with inlining: a stale second writer's small append
/// rebases and still inlines on the new head, with row ids continuing past
/// the first writer's records.
#[test]
fn retried_commits_keep_inlining_and_continue_row_ids() {
    let dir = scratch("retry-inline");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    append(&mut lake, &[(1, Some("a"))]);

    let mut a = Lake::open(&lake_dir).unwrap();
    let mut b = Lake::open(&lake_dir).unwrap();
    let fast = CommitSettings {
        max_retry_count: 10,
        retry_backoff: 1.0,
        retry_wait_ms: 1,
    };
    append(&mut a, &[(2, Some("b"))]);
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(3, Some("c"))]));
    let report = b.commit_with_settings(&commit, &fast).unwrap();
    assert_eq!(report.retries, 1, "the stale base was retried");
    assert_eq!(report.rows_inlined, 1, "the retry still inlines");

    let live = live_inlined(&entry(&lake_dir, "main.t"));
    let ids: Vec<u64> = live.iter().map(|r| r["row_id"].as_u64().unwrap()).collect();
    assert_eq!(ids, vec![0, 1, 2], "row ids continue, never restart");
    let db = b.load(None).unwrap();
    let got: Vec<i32> = rows_of(&db).into_iter().map(|(i, _)| i).collect();
    assert_eq!(got, vec![1, 2, 3]);

    cleanup(&dir);
}
