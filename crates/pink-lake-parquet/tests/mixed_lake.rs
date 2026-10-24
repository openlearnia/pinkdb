//! Wave 88: mixed lakes — one catalog holding tables in different data-file
//! formats.
//!
//! The dev-dependency enables pink-lake's `vortexlake` feature, so this test
//! builds with both the built-in Vortex provider and the Parquet provider in
//! one process. It covers the plan's mixed-lake contract: three tables in
//! `pink`, `vortex` and `parquet` round-trip through save/open; attach answers
//! the same COUNT/MIN/MAX facts as open for each table; appends keep each
//! table's format even when the handle's write format differs; a
//! whole-store save with a different handle default keeps every existing
//! table's format; a per-table choice selects a new table's format; a rewrite
//! converts one table and open/attach agree before and after; and an entry
//! naming an unknown format id refuses the lake.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, Lake, LakeError, MergeSettings};
use pink_lake_parquet::{FORMAT_ID, register};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_mixed_{tag}_{}_{}",
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

fn register_providers() {
    register().unwrap();
    assert!(
        pink_lake::format::lake_format("vortex").is_some(),
        "the mixed-lake test needs pink-lake's `vortexlake` feature (enabled by this crate's \
         dev-dependency)"
    );
    assert!(pink_lake::format::lake_format(FORMAT_ID).is_some());
}

fn schema() -> Vec<(String, DataType)> {
    vec![
        ("i".to_string(), DataType::Integer),
        ("s".to_string(), DataType::Varchar),
    ]
}

/// A table with rows `(i, s)` for `start..start + count`.
fn rows_table(start: i32, count: i32) -> Arc<DataTable> {
    let mut table = DataTable::new("t".to_string(), schema()).unwrap();
    for n in 0..count {
        let i = start + n;
        table
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("row-{i}"))])
            .unwrap();
    }
    Arc::new(table)
}

fn add_table(db: &mut PinkDb, name: &str, start: i32, count: i32) {
    db.create_table_in("main", name, schema()).unwrap();
    let table = db.table_mut_in("main", name).unwrap();
    for n in 0..count {
        let i = start + n;
        table
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("row-{i}"))])
            .unwrap();
    }
}

/// The source store of the mixed-lake test: one table per data-file format.
fn mixed_store() -> PinkDb {
    let mut db = PinkDb::new();
    add_table(&mut db, "as_pink", 0, 3);
    add_table(&mut db, "as_vortex", 100, 3);
    add_table(&mut db, "as_parquet", 200, 3);
    db
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn assert_rows_equal(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
    assert_eq!(a.len(), b.len(), "{what}: row count");
    for (r, (ra, rb)) in a.iter().zip(b).enumerate() {
        assert_eq!(ra, rb, "{what}: row {r}");
    }
}

fn assert_store_equal(a: &PinkDb, b: &PinkDb, what: &str) {
    assert_eq!(a.list_databases(), b.list_databases(), "{what}: databases");
    for db in a.list_databases() {
        assert_eq!(
            a.list_tables_in(&db),
            b.list_tables_in(&db),
            "{what}: tables of {db}"
        );
        for name in a.list_tables_in(&db) {
            let ta = a.table_in(&db, &name).unwrap();
            let tb = b.table_in(&db, &name).unwrap();
            assert_eq!(ta.schema(), tb.schema(), "{what}: {db}.{name} schema");
            assert_eq!(
                ta.num_rows(),
                tb.num_rows(),
                "{what}: {db}.{name} row count"
            );
            for r in 0..ta.num_rows() {
                for c in 0..ta.columns.len() {
                    assert_eq!(
                        ta.get(r, c).unwrap(),
                        tb.get(r, c).unwrap(),
                        "{what}: {db}.{name} row {r} column {}",
                        ta.columns[c].name
                    );
                }
            }
        }
    }
}

fn read_catalog(lake_dir: &Path) -> serde_json::Value {
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

fn entry_format(catalog: &serde_json::Value, key: &str) -> String {
    catalog["tables"][key]["format"]
        .as_str()
        .unwrap_or_else(|| panic!("table {key} has no format: {catalog}"))
        .to_string()
}

fn file_extensions(catalog: &serde_json::Value, key: &str) -> Vec<String> {
    catalog["tables"][key]["files"]
        .as_array()
        .unwrap_or_else(|| panic!("table {key} has no files: {catalog}"))
        .iter()
        .map(|file| {
            let path = Path::new(file["path"].as_str().unwrap());
            path.extension().unwrap().to_string_lossy().into_owned()
        })
        .collect()
}

fn assert_format(catalog: &serde_json::Value, key: &str, id: &str, extension: &str) {
    assert_eq!(entry_format(catalog, key), id, "{catalog}");
    for extension_seen in file_extensions(catalog, key) {
        assert_eq!(extension_seen, extension, "table {key}: {catalog}");
    }
}

/// Wave 102: formats change only explicitly, so the catalog records no pin.
fn assert_no_pin_field(catalog: &serde_json::Value) {
    for (key, entry) in catalog["tables"].as_object().unwrap() {
        assert!(
            entry.get("format_pinned").is_none(),
            "table {key}: {catalog}"
        );
    }
}

/// Every table of the mixed store answers the same COUNT/MIN/MAX facts from
/// `.lake attach` (manifest blocks + Vortex footers) as from `.lake open`.
fn assert_open_attach_agree(lake: &Lake, tables: &[&str]) {
    let attachment = lake.attach(None).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(attachment.lake).unwrap();
    let mut opened = lake.load(None).unwrap();
    for table in tables {
        for sql in [
            format!("SELECT COUNT(*) FROM {table};"),
            format!("SELECT MIN(i), MAX(i), MIN(s), MAX(s) FROM {table};"),
        ] {
            let want = query(&mut opened, &sql);
            let got = query(&mut attached, &sql);
            assert_rows_equal(&want, &got, &sql);
        }
    }
}

#[test]
fn mixed_formats_round_trip_appends_and_pins() {
    register_providers();
    let dir = scratch("mixed");
    let lake_dir = dir.join("lake");
    let source = mixed_store();

    // Save three tables, each with its own per-table choice. The handle's
    // write format stays at the build default (vortex in this test build), so
    // the pink and parquet choices must win.
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_table_format("main", "as_pink", "pink").unwrap();
    lake.set_table_format("main", "as_vortex", "vortex")
        .unwrap();
    lake.set_table_format("main", "as_parquet", FORMAT_ID)
        .unwrap();
    lake.write_from_store_report(&source).unwrap();

    let catalog = read_catalog(&lake_dir);
    assert_format(&catalog, "main.as_pink", "pink", "pink");
    assert_format(&catalog, "main.as_vortex", "vortex", "vortex");
    assert_format(&catalog, "main.as_parquet", FORMAT_ID, "parquet");
    assert_no_pin_field(&catalog);

    // Open equals the source; attach equals open for every table.
    let reopened = Lake::open(&lake_dir).unwrap();
    let opened = reopened.load(None).unwrap();
    assert_store_equal(&source, &opened, "mixed lake open");
    assert_open_attach_agree(&reopened, &["as_pink", "as_vortex", "as_parquet"]);

    // Appends keep each table's format, whatever the handle's write
    // format says (set here to parquet, which differs from pink and vortex).
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append("main.as_pink", rows_table(10, 2));
    commit.append("main.as_vortex", rows_table(110, 2));
    commit.append("main.as_parquet", rows_table(210, 2));
    let report = lake.commit(&commit).unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.data_files, 3, "{report:?}");

    let catalog = read_catalog(&lake_dir);
    for (key, id, extension) in [
        ("main.as_pink", "pink", "pink"),
        ("main.as_vortex", "vortex", "vortex"),
        ("main.as_parquet", FORMAT_ID, "parquet"),
    ] {
        assert_eq!(entry_format(&catalog, key), id, "{catalog}");
        let extensions = file_extensions(&catalog, key);
        assert_eq!(
            extensions,
            vec![extension.to_string(), extension.to_string()]
        );
    }
    let reopened = Lake::open(&lake_dir).unwrap();
    let appended = reopened.load(None).unwrap();
    for table in ["as_pink", "as_vortex", "as_parquet"] {
        assert_eq!(
            appended.table_in("main", table).unwrap().num_rows(),
            5,
            "{table}"
        );
    }
    assert_open_attach_agree(&reopened, &["as_pink", "as_vortex", "as_parquet"]);

    // A commit that creates a table honors the handle's per-table choice
    // (the handle-wide write format would have said parquet).
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.set_table_format("main", "committed", "pink").unwrap();
    let mut commit = Commit::new();
    commit.append("main.committed", rows_table(300, 2));
    lake.commit(&commit).unwrap();
    let catalog = read_catalog(&lake_dir);
    assert_format(&catalog, "main.committed", "pink", "pink");

    // A whole-store save with a different handle default keeps every existing
    // table's format (no silent re-encoding).
    let reopened = Lake::open(&lake_dir).unwrap();
    let store = reopened.load(None).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_write_format("pink").unwrap();
    lake.write_from_store_report(&store).unwrap();
    let catalog = read_catalog(&lake_dir);
    assert_format(&catalog, "main.as_pink", "pink", "pink");
    assert_format(&catalog, "main.as_vortex", "vortex", "vortex");
    assert_format(&catalog, "main.as_parquet", FORMAT_ID, "parquet");
    assert_format(&catalog, "main.committed", "pink", "pink");
    // The different handle default re-encoded none of them.
    assert_no_pin_field(&catalog);
    let reloaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    assert_store_equal(&store, &reloaded, "whole-store save");

    // A per-table choice selects a new table's format even when the handle's
    // write format says something else; existing tables keep their formats.
    let mut store = reloaded.clone();
    add_table(&mut store, "fresh", 500, 2);
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_table_format("main", "fresh", "pink").unwrap();
    lake.write_from_store_report(&store).unwrap();
    let catalog = read_catalog(&lake_dir);
    assert_format(&catalog, "main.fresh", "pink", "pink");
    assert_format(&catalog, "main.as_pink", "pink", "pink");
    assert_format(&catalog, "main.as_vortex", "vortex", "vortex");
    assert_format(&catalog, "main.as_parquet", FORMAT_ID, "parquet");
    assert_format(&catalog, "main.committed", "pink", "pink");
    assert_no_pin_field(&catalog);

    // An entry naming an unknown format id is a **per-table** refusal (wave
    // 88, rule 5): the lake opens, the other tables stay readable, the table
    // is listed as unavailable, and reads of it fail naming the id. (Done
    // last: the catalog is deliberately left with a broken entry.)
    pink_lake::catalog_set_table_format(
        &format!("pink:{}", lake_dir.display()),
        "main.as_vortex",
        "no-such-format",
    )
    .unwrap();
    let lake = Lake::open(&lake_dir).unwrap();
    let unavailable = lake.unavailable_tables().unwrap();
    let broken = unavailable
        .iter()
        .find(|u| u.table == "main.as_vortex")
        .unwrap_or_else(|| panic!("main.as_vortex must be unavailable: {unavailable:?}"));
    assert_eq!(broken.format, "no-such-format");
    assert!(
        matches!(broken.error, LakeError::UnknownFormat { .. }),
        "{:?}",
        broken.error
    );
    let loaded = lake.load(None).unwrap();
    assert!(
        !loaded.list_tables().contains(&"as_vortex".to_string()),
        "the unreadable table is skipped"
    );
    assert_eq!(
        loaded.table_in("main", "as_pink").unwrap().num_rows(),
        5,
        "the readable tables are still loaded"
    );
    let attachment = lake.attach(None).unwrap();
    let text = attachment
        .lake
        .load("main", "as_vortex")
        .unwrap_err()
        .to_string();
    assert!(text.contains("main.as_vortex"), "{text}");
    assert!(text.contains("no-such-format"), "{text}");
    let mut lake = Lake::open(&lake_dir).unwrap();
    let err = lake
        .merge_adjacent_files(Some("main.as_vortex"), &MergeSettings::default())
        .unwrap_err();
    assert!(matches!(err, LakeError::UnknownFormat { .. }), "{err:?}");

    cleanup(&dir);
}

#[test]
fn rewrite_converts_one_table_and_reads_agree() {
    register_providers();
    let dir = scratch("convert");
    let lake_dir = dir.join("lake");
    let mut source = PinkDb::new();
    add_table(&mut source, "t", 0, 4);
    add_table(&mut source, "u", 100, 4);

    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_table_format("main", "t", "vortex").unwrap();
    lake.set_table_format("main", "u", FORMAT_ID).unwrap();
    lake.write_from_store_report(&source).unwrap();
    let reopened = Lake::open(&lake_dir).unwrap();
    assert_format(&read_catalog(&lake_dir), "main.t", "vortex", "vortex");
    assert_open_attach_agree(&reopened, &["t", "u"]);

    // Give the table a second file and a positional delete, so the conversion
    // has to materialize deletion state as well as re-encode.
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append("main.t", rows_table(10, 2));
    lake.commit(&commit).unwrap();
    let catalog = read_catalog(&lake_dir);
    let appended = catalog["tables"]["main.t"]["files"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()["path"]
        .as_str()
        .unwrap()
        .to_string();
    let mut commit = Commit::new();
    commit.delete_positions("main.t", &appended, &[0]);
    lake.commit(&commit).unwrap();
    assert_eq!(
        lake.load(None)
            .unwrap()
            .table_in("main", "t")
            .unwrap()
            .num_rows(),
        5
    );

    // Convert `t` from vortex to parquet in one maintenance version.
    let report = lake.rewrite_table_format("main", "t", FORMAT_ID).unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.files_processed(), 2, "{report:?}");
    assert_eq!(report.files_created(), 1, "{report:?}");
    let catalog = read_catalog(&lake_dir);
    assert_format(&catalog, "main.t", FORMAT_ID, "parquet");
    assert_eq!(
        catalog["tables"]["main.t"]["deletes"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0),
        0,
        "{catalog}"
    );
    // The untouched table keeps its own format.
    assert_format(&catalog, "main.u", FORMAT_ID, "parquet");

    let mut opened = lake.load(None).unwrap();
    assert_eq!(
        query(&mut opened, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;"),
        vec![vec![
            Value::Bigint(5),
            Value::Integer(0),
            Value::Integer(11)
        ]]
    );
    assert_open_attach_agree(&lake, &["t", "u"]);

    // Converting into the format a table already has is a no-op.
    let version = lake.current_version();
    let report = lake.rewrite_table_format("main", "t", FORMAT_ID).unwrap();
    assert!(!report.committed, "{report:?}");
    assert_eq!(report.version, version);

    // An unknown id is refused before anything is published.
    let err = lake
        .rewrite_table_format("main", "t", "no-such-format")
        .unwrap_err();
    assert!(matches!(err, LakeError::UnknownFormat { .. }), "{err:?}");
    assert_eq!(lake.current_version(), version);

    cleanup(&dir);
}
