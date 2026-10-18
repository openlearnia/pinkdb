//! Wave 65: lake schema evolution — `ALTER TABLE` on writable attached
//! tables, per-version schema records (catalog `format_version` 5), and old
//! data files read through the schema mapping (rename chains, `ADD COLUMN …
//! DEFAULT` materialization, hidden dropped columns), with time travel
//! reading each version's own schema.
//!
//! Format-agnostic: runs in a default (lean `.pink`) build and in a
//! `vortex-format` build; the mapping semantics are the same either way.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{save_store, CommitSettings, Lake, WritableAttachment};
use pinkdb::{PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

const EXT: &str = if cfg!(feature = "vortex-format") {
    "vortex"
} else {
    "pink"
};

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_schema_{tag}_{}_{}",
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

/// A store: `t (i INTEGER, s VARCHAR)` with `rows` rows (`i = 100 + r`).
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

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

/// Materialize a writable attachment's base tables into a session store,
/// exactly like `.lake attach … write` does (including the schema record's
/// column defaults).
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
            let chunk = base.scan_chunk(c).unwrap();
            t.append_shared_chunk(&chunk.columns).unwrap();
        }
    }
    db
}

fn json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn entry(lake_dir: &Path, key: &str) -> serde_json::Value {
    json(&lake_dir.join("catalog.json"))["tables"][key].clone()
}

// ---------------------------------------------------------------------------
// ADD COLUMN with DEFAULT
// ---------------------------------------------------------------------------

#[test]
fn add_column_with_default_reads_back_on_old_files() {
    let dir = scratch("add");
    let lake_dir = dir.join("lake");
    save_store(&store(4), &lake_dir).unwrap();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    // The old rows keep their values; the added column is all-default in the
    // session (the engine materializes the ADD).
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 42;")
        .unwrap();
    // Update one row's added column: that is a real change and must publish.
    db.execute("UPDATE t SET n = 7 WHERE i = 101;").unwrap();
    // A later INSERT that omits the column writes the default.
    db.execute("INSERT INTO t (i, s) VALUES (200, 'new');")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    assert_eq!(
        report.rows_added, 2,
        "the UPDATE row and the INSERT: {report:?}"
    );
    assert_eq!(report.rows_deleted, 1, "the UPDATE is a delete + append");

    // Reload: old file rows read 42, the updated row 7, the new row 42.
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT i, n FROM t ORDER BY i;"),
        vec![
            vec![Value::Integer(100), Value::Integer(42)],
            vec![Value::Integer(101), Value::Integer(7)],
            vec![Value::Integer(102), Value::Integer(42)],
            vec![Value::Integer(103), Value::Integer(42)],
            vec![Value::Integer(200), Value::Integer(42)],
        ]
    );
    // The manifest record: epoch 2, `n` added at 2, one file at each epoch.
    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["schema_version"], 2, "{e}");
    assert_eq!(e["columns"][2]["name"], "n");
    assert_eq!(e["columns"][2]["default"], "42");
    assert_eq!(e["columns"][2]["added"], 2);
    let versions: Vec<u64> = e["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["schema_version"].as_u64().unwrap())
        .collect();
    assert_eq!(versions, vec![1, 2], "{e}");
    // The data-file extension is whatever this build writes.
    assert!(e["files"]
        .as_array()
        .unwrap()
        .iter()
        .all(|f| f["path"].as_str().unwrap().ends_with(&format!(".{EXT}"))));
    cleanup(&dir);
}

#[test]
fn add_column_without_default_stores_null_on_old_rows() {
    // DuckLake accepts `ADD COLUMN` without a DEFAULT (probed): old rows read
    // NULL and later inserts write NULL unless they name the column.
    let dir = scratch("add-null");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t ADD COLUMN m INTEGER;").unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT m FROM t ORDER BY i;"),
        vec![vec![Value::Null], vec![Value::Null]]
    );
    // Nullable column with no DEFAULT: the record's default is absent.
    let e = entry(&lake_dir, "main.t");
    assert!(e["columns"][2].get("default").is_none(), "{e}");
    // The attach descriptor proves the all-NULL column exactly.
    let report = lake.attach(None).unwrap();
    let t = report
        .lake
        .tables()
        .iter()
        .find(|t| t.table == "t")
        .unwrap();
    assert_eq!(t.schema[2].0, "m");
    assert_eq!(t.stats[2].null_count, Some(2));
    assert_eq!(t.stats[2].min, None);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// RENAME COLUMN
// ---------------------------------------------------------------------------

#[test]
fn rename_column_projects_old_files_and_time_travel_sees_the_old_name() {
    let dir = scratch("rename");
    let lake_dir = dir.join("lake");
    save_store(&store(3), &lake_dir).unwrap();
    let before = Lake::open(&lake_dir).unwrap().current_version();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    // The session sees the new name and the same values.
    assert_eq!(
        query(&mut db, "SELECT label FROM t ORDER BY i;"),
        vec![
            vec![Value::Varchar("v0".to_string())],
            vec![Value::Varchar("v1".to_string())],
            vec![Value::Varchar("v2".to_string())],
        ]
    );
    // A new row exercises a post-rename file (written under the new name).
    db.execute("INSERT INTO t VALUES (300, 'post');").unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);

    // Latest: the old file's `s` bytes are projected to `label`.
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT i, label FROM t ORDER BY i;"),
        vec![
            vec![Value::Integer(100), Value::Varchar("v0".to_string())],
            vec![Value::Integer(101), Value::Varchar("v1".to_string())],
            vec![Value::Integer(102), Value::Varchar("v2".to_string())],
            vec![Value::Integer(300), Value::Varchar("post".to_string())],
        ]
    );
    // The record: epoch 2, the rename logged, `s` gone from the live columns.
    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["schema_version"], 2, "{e}");
    assert_eq!(e["renames"][0]["version"], 2);
    assert_eq!(e["renames"][0]["from"], "s");
    assert_eq!(e["renames"][0]["to"], "label");
    assert!(e["columns"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["name"] != "s"));

    // Time travel: the old version still reads the old name.
    let report = lake.attach(None).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    let err = attached
        .execute(&format!(
            "SELECT label FROM t AT (VERSION => {before}) ORDER BY i;"
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("label"),
        "the old version has no `label`: {err}"
    );
    assert_eq!(
        query(
            &mut attached,
            &format!("SELECT s FROM t AT (VERSION => {before}) ORDER BY i;")
        ),
        vec![
            vec![Value::Varchar("v0".to_string())],
            vec![Value::Varchar("v1".to_string())],
            vec![Value::Varchar("v2".to_string())],
        ]
    );
    // And the old version's descriptor carries the old schema.
    let old = report
        .lake
        .table_at("main", "t", pinkdb::AtSpec::Version(before))
        .unwrap();
    assert!(old.schema.iter().any(|(n, _)| n == "s"));
    assert!(!old.schema.iter().any(|(n, _)| n == "label"));
    cleanup(&dir);
}

#[test]
fn chained_renames_map_every_generation() {
    let dir = scratch("rename-chain");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t RENAME COLUMN s TO mid;").unwrap();
    wa.commit(&db, &settings()).unwrap();
    // A second rename in a later commit.
    db.execute("ALTER TABLE t RENAME COLUMN mid TO final_name;")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT final_name FROM t ORDER BY i;"),
        vec![
            vec![Value::Varchar("v0".to_string())],
            vec![Value::Varchar("v1".to_string())],
        ],
        "the first generation's bytes project through both renames"
    );
    let e = entry(&lake_dir, "main.t");
    let renames = e["renames"].as_array().unwrap();
    assert_eq!(renames.len(), 2, "{e}");
    assert_eq!(renames[0]["from"], "s");
    assert_eq!(renames[0]["to"], "mid");
    assert_eq!(renames[1]["from"], "mid");
    assert_eq!(renames[1]["to"], "final_name");
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// DROP COLUMN
// ---------------------------------------------------------------------------

#[test]
fn drop_column_hides_it_now_and_old_versions_still_read_it() {
    let dir = scratch("drop");
    let lake_dir = dir.join("lake");
    save_store(&store(3), &lake_dir).unwrap();
    let before = Lake::open(&lake_dir).unwrap().current_version();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t DROP COLUMN s;").unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    assert_eq!(report.data_files, 0, "a drop writes no data file");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(3)]]
    );
    // The column is hidden: selecting it is a clean binder error.
    assert!(db.execute("SELECT s FROM t;").is_err());
    // DESCRIBE shows the surviving column only.
    assert_eq!(query(&mut db, "DESCRIBE t;").len(), 1);

    // Time travel to the version before the drop still reads it (the old
    // file kept its bytes).
    let report = lake.attach(None).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        query(
            &mut attached,
            &format!("SELECT i, s FROM t AT (VERSION => {before}) ORDER BY i;")
        ),
        vec![
            vec![Value::Integer(100), Value::Varchar("v0".to_string())],
            vec![Value::Integer(101), Value::Varchar("v1".to_string())],
            vec![Value::Integer(102), Value::Varchar("v2".to_string())],
        ]
    );
    let old = report
        .lake
        .table_at("main", "t", pinkdb::AtSpec::Version(before))
        .unwrap();
    assert_eq!(old.schema.len(), 2);
    let now = report
        .lake
        .table_at("main", "t", pinkdb::AtSpec::Version(lake.current_version()))
        .unwrap();
    assert_eq!(now.schema.len(), 1, "{:?}", now.schema);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Deletes across schema changes
// ---------------------------------------------------------------------------

#[test]
fn positional_deletes_stay_valid_across_schema_changes() {
    let dir = scratch("deletes");
    let lake_dir = dir.join("lake");
    save_store(&store(5), &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    // Delete a row from the old file, then evolve the schema, then delete
    // another row: positions are per file and must not shift.
    db.execute("DELETE FROM t WHERE i = 101;").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 9;")
        .unwrap();
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    db.execute("DELETE FROM t WHERE i = 103;").unwrap();
    db.execute("INSERT INTO t VALUES (400, 'x', 1);").unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "one commit, one schema record");
    assert_eq!(report.rows_deleted, 2, "{report:?}");
    assert_eq!(report.rows_added, 1, "{report:?}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT i, label, n FROM t ORDER BY i;"),
        vec![
            vec![
                Value::Integer(100),
                Value::Varchar("v0".to_string()),
                Value::Integer(9)
            ],
            vec![
                Value::Integer(102),
                Value::Varchar("v2".to_string()),
                Value::Integer(9)
            ],
            vec![
                Value::Integer(104),
                Value::Varchar("v4".to_string()),
                Value::Integer(9)
            ],
            vec![
                Value::Integer(400),
                Value::Varchar("x".to_string()),
                Value::Integer(1)
            ],
        ]
    );
    // Merged statistics gate on the deletes: no per-column fact is provable
    // for the surviving physical columns, but the added default column's
    // constant is still exact.
    let report = lake.attach(None).unwrap();
    let t = report
        .lake
        .tables()
        .iter()
        .find(|t| t.table == "t")
        .unwrap();
    assert_eq!(t.rows, 4);
    assert_eq!(
        t.stats[0].min, None,
        "a deleted position could hide any value"
    );
    // The added default column: the old file contributes the constant 9, the
    // appended file (no deletes) its exact 1 — the fold is exact.
    assert_eq!(t.stats[2].null_count, Some(0));
    assert_eq!(t.stats[2].min, Some(Value::Integer(1)));
    assert_eq!(t.stats[2].max, Some(Value::Integer(9)));
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Multiple schema changes across separate commits + reuse of names
// ---------------------------------------------------------------------------

#[test]
fn drop_and_readd_the_same_name_reads_the_default_not_old_bytes() {
    // DuckLake-pinned: dropping `a` and re-adding `a` is a fresh column —
    // old files read the new default, never the dropped column's bytes
    // (probed with `initial_default`).
    let dir = scratch("readd");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t DROP COLUMN s;").unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1);
    db.execute("ALTER TABLE t ADD COLUMN s VARCHAR DEFAULT 'fresh';")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1);
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT i, s FROM t ORDER BY i;"),
        vec![
            vec![Value::Integer(100), Value::Varchar("fresh".to_string())],
            vec![Value::Integer(101), Value::Varchar("fresh".to_string())],
        ],
        "the re-added column's first epoch is after the file: default wins"
    );
    let e = entry(&lake_dir, "main.t");
    assert_eq!(e["schema_version"], 3, "{e}");
    // The re-added column's `added` epoch is 3; the old bytes stay hidden.
    assert_eq!(e["columns"][1]["added"], 3);
    assert_eq!(e["columns"][1]["default"], "fresh");
    cleanup(&dir);
}

#[test]
fn legacy_v4_lake_upgrades_on_the_first_schema_change() {
    // A v4 lake (no schema records) stays readable; a schema change migrates
    // the entry to a v5 record, stamping the old files with epoch 1.
    let dir = scratch("legacy");
    let lake_dir = dir.join("lake");
    save_store(&store(3), &lake_dir).unwrap();
    // Degrade the manifest to v4 shape: strip the record and the file epoch.
    let catalog_path = lake_dir.join("catalog.json");
    let mut manifest = json(&catalog_path);
    manifest["format_version"] = serde_json::json!(4);
    for (_, e) in manifest["tables"].as_object_mut().unwrap() {
        e.as_object_mut().unwrap().remove("schema_version");
        e.as_object_mut().unwrap().remove("columns");
        e.as_object_mut().unwrap().remove("renames");
        for f in e["files"].as_array_mut().unwrap() {
            f.as_object_mut().unwrap().remove("schema_version");
        }
    }
    let snapshot_path = lake_dir
        .join("snapshots")
        .join(format!("{}.json", manifest["version"].as_u64().unwrap()));
    let bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    std::fs::write(&catalog_path, &bytes).unwrap();
    std::fs::write(&snapshot_path, &bytes).unwrap();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 5;")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        query(&mut db, "SELECT i, n FROM t ORDER BY i;"),
        vec![
            vec![Value::Integer(100), Value::Integer(5)],
            vec![Value::Integer(101), Value::Integer(5)],
            vec![Value::Integer(102), Value::Integer(5)],
        ]
    );
    let e = entry(&lake_dir, "main.t");
    assert_eq!(json(&catalog_path)["format_version"], 5);
    assert_eq!(e["schema_version"], 2, "the legacy base is epoch 1: {e}");
    assert_eq!(e["files"][0]["schema_version"], 1, "migrated file: {e}");
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Lake-wide schema versions
// ---------------------------------------------------------------------------

#[test]
fn snapshots_track_the_lake_schema_version() {
    let dir = scratch("schemaver");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let records = lake.snapshots().unwrap();
    assert_eq!(records[0].schema_version, 0, "the empty create");
    assert_eq!(
        records.last().unwrap().schema_version,
        1,
        "the save created the table"
    );
    // A data-only commit leaves the counter alone; a schema change bumps it.
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("INSERT INTO t (i, s) VALUES (500, 'x');")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 0, "{report:?}");
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1;")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    assert_eq!(report.schema_changes, 1, "{report:?}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let records = lake.snapshots().unwrap();
    let versions: Vec<u64> = records.iter().map(|r| r.schema_version).collect();
    assert_eq!(
        versions,
        vec![0, 1, 1, 2],
        "create, save, data-only commit, schema commit: {records:?}"
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Engine ALTER error cases (DuckLake/DuckDB-pinned)
// ---------------------------------------------------------------------------

#[test]
fn alter_error_cases_are_clean_and_duckdb_shaped() {
    let dir = scratch("errors");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);

    // A duplicate ADD.
    let err = db
        .execute("ALTER TABLE t ADD COLUMN i INTEGER;")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Column with name \"i\" already exists!"),
        "{err}"
    );
    // IF NOT EXISTS makes it a no-op.
    db.execute("ALTER TABLE t ADD COLUMN IF NOT EXISTS i INTEGER;")
        .unwrap();
    // RENAME to a taken name.
    let err = db
        .execute("ALTER TABLE t RENAME COLUMN s TO i;")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Column with name \"i\" already exists!"),
        "{err}"
    );
    // RENAME a missing column.
    let err = db
        .execute("ALTER TABLE t RENAME COLUMN zz TO q;")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Table \"t\" does not have a column with name \"zz\""),
        "{err}"
    );
    // DROP a missing column, with and without IF EXISTS.
    assert!(db.execute("ALTER TABLE t DROP COLUMN zz;").is_err());
    db.execute("ALTER TABLE t DROP COLUMN IF EXISTS zz;")
        .unwrap();
    // ADD COLUMN NOT NULL without a default on a non-empty table: DuckDB's
    // constraint error (an empty table accepts it).
    let err = db
        .execute("ALTER TABLE t ADD COLUMN nn INTEGER NOT NULL;")
        .unwrap_err()
        .to_string();
    assert!(err.contains("NOT NULL constraint failed: t.nn"), "{err}");
    db.execute("ALTER TABLE t ADD COLUMN ok INTEGER NOT NULL DEFAULT 3;")
        .unwrap();
    // RENAME TABLE onto an existing name.
    db.execute("CREATE TABLE u (x INTEGER);").unwrap();
    let err = db
        .execute("ALTER TABLE t RENAME TO u;")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "Could not rename \"t\" to \"u\": another entry with this name already exists!"
        ),
        "{err}"
    );
    // DROP a column a key constraint depends on.
    db.execute("CREATE TABLE k (a INTEGER PRIMARY KEY, b INTEGER);")
        .unwrap();
    let err = db
        .execute("ALTER TABLE k DROP COLUMN a;")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Cannot drop column \"a\" because there is a PRIMARY KEY constraint"),
        "{err}"
    );
    cleanup(&dir);
}

#[test]
fn rename_table_through_a_writable_attach_is_refused_with_a_hint() {
    let dir = scratch("renametable");
    let lake_dir = dir.join("lake");
    save_store(&store(2), &lake_dir).unwrap();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t RENAME TO t2;").unwrap();
    let err = wa.commit(&db, &settings()).unwrap_err().to_string();
    assert!(
        err.contains("RENAME TABLE is not published through .lake commit"),
        "{err}"
    );
    assert!(err.contains(".lake save"), "{err}");
    // Nothing was published: the lake still has one table at the same head
    // (`save_store` on a fresh location publishes the empty create + the save).
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.current_version(), 2);
    assert_eq!(lake.tables().unwrap().len(), 1);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Time travel across a schema change with deletes in between
// ---------------------------------------------------------------------------

#[test]
fn time_travel_reads_each_versions_own_schema_and_rows() {
    let dir = scratch("time-travel");
    let lake_dir = dir.join("lake");
    save_store(&store(3), &lake_dir).unwrap();
    let v1 = Lake::open(&lake_dir).unwrap().current_version();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1;")
        .unwrap();
    db.execute("UPDATE t SET n = 100 WHERE i = 100;").unwrap();
    wa.commit(&db, &settings()).unwrap();
    let v2 = Lake::open(&lake_dir).unwrap().current_version();

    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    let report = wa.commit(&db, &settings()).unwrap();
    let v3 = report.version;

    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let report = lake.attach(None).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    // v1: two columns, no `n`.
    assert_eq!(
        query(
            &mut attached,
            &format!("SELECT i, s FROM t AT (VERSION => {v1}) ORDER BY i;")
        ),
        vec![
            vec![Value::Integer(100), Value::Varchar("v0".to_string())],
            vec![Value::Integer(101), Value::Varchar("v1".to_string())],
            vec![Value::Integer(102), Value::Varchar("v2".to_string())],
        ]
    );
    // v2: `s` + `n`, the updated row's 100 reads back.
    assert_eq!(
        query(
            &mut attached,
            &format!("SELECT i, s, n FROM t AT (VERSION => {v2}) ORDER BY i;")
        ),
        vec![
            vec![
                Value::Integer(100),
                Value::Varchar("v0".to_string()),
                Value::Integer(100)
            ],
            vec![
                Value::Integer(101),
                Value::Varchar("v1".to_string()),
                Value::Integer(1)
            ],
            vec![
                Value::Integer(102),
                Value::Varchar("v2".to_string()),
                Value::Integer(1)
            ],
        ]
    );
    // v3: `label` + `n`.
    assert_eq!(
        query(
            &mut attached,
            &format!("SELECT i, label, n FROM t AT (VERSION => {v3}) ORDER BY i;")
        ),
        vec![
            vec![
                Value::Integer(100),
                Value::Varchar("v0".to_string()),
                Value::Integer(100)
            ],
            vec![
                Value::Integer(101),
                Value::Varchar("v1".to_string()),
                Value::Integer(1)
            ],
            vec![
                Value::Integer(102),
                Value::Varchar("v2".to_string()),
                Value::Integer(1)
            ],
        ]
    );
    // Columns that never existed at a version are still clean errors.
    assert!(attached
        .execute(&format!("SELECT n FROM t AT (VERSION => {v1});"))
        .is_err());
    cleanup(&dir);
}
