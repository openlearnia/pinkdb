//! End-to-end tests of the public `pink-lake` API: round-trips, versioning,
//! crash-atomicity leftovers, corrupt-metadata handling, and the v1 → v2
//! (`.pink` → Vortex) format history.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{save_store, validate_store, Lake, LakeError, FORMAT_VERSION};
use pinkdb::{DataTable, DataType, PinkDb, Value, Vector, CHUNK_SIZE};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_{tag}_{}_{}",
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

/// A store with all types, NULLs, NaN payloads, `-0.0`, a multi-chunk table,
/// an empty table, a second database, and a table name containing a dot.
fn fixture_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE main.t (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, \
         dt DATE, ts TIMESTAMP);",
    )
    .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        t.append_row(vec![
            Value::Boolean(true),
            Value::Integer(-5),
            Value::Bigint(9_999_999_999),
            Value::Double(-0.0),
            Value::Varchar("hi".to_string()),
            Value::Date(0),
            Value::Timestamp(-1),
        ])
        .unwrap();
        t.append_row(vec![Value::Null; 7]).unwrap();
        t.append_row(vec![
            Value::Boolean(false),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MIN),
            Value::Double(f64::from_bits(0x7ff8_0000_dead_beef)),
            Value::Varchar("héllo wörld 🌍".to_string()),
            Value::Date(19_000),
            Value::Timestamp(1_700_000_000_000_000),
        ])
        .unwrap();
    }
    db.execute("CREATE TABLE big (a INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("big").unwrap();
        for i in 0..(CHUNK_SIZE + 100) {
            t.append_row(vec![
                Value::Integer((i % 7) as i32 - 3),
                Value::Varchar(format!("region{}", i % 3)),
            ])
            .unwrap();
        }
    }
    db.execute("CREATE TABLE empty (x INTEGER, y VARCHAR);")
        .unwrap();
    db.execute("CREATE DATABASE analytics;").unwrap();
    db.execute("CREATE TABLE analytics.u (k BIGINT);").unwrap();
    db.execute("INSERT INTO analytics.u VALUES (7);").unwrap();
    db.execute("CREATE TABLE analytics.\"a.b\" (z DOUBLE);")
        .unwrap();
    db.execute("INSERT INTO analytics.\"a.b\" VALUES (1.5), (-0.0);")
        .unwrap();
    db
}

/// The engine's deterministic `.pink` bytes of `db`.
fn store_bytes(db: &PinkDb, scratch: &Path, tag: &str) -> Vec<u8> {
    let path = scratch.join(format!("{tag}.pink"));
    db.save(path.to_str().unwrap()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).ok();
    bytes
}

/// Compare two stores value by value, doubles by bit pattern (so NaN
/// payloads and `-0.0` are checked exactly).
fn assert_same_store(a: &PinkDb, b: &PinkDb) {
    assert_eq!(a.list_databases(), b.list_databases(), "databases differ");
    for dbname in a.list_databases() {
        assert_eq!(
            a.list_tables_in(&dbname),
            b.list_tables_in(&dbname),
            "tables differ in {dbname}"
        );
        for table in a.list_tables_in(&dbname) {
            let ta = a.table_in(&dbname, &table).unwrap();
            let tb = b.table_in(&dbname, &table).unwrap();
            assert_eq!(ta.schema(), tb.schema(), "{dbname}.{table} schema");
            assert_eq!(ta.num_rows(), tb.num_rows(), "{dbname}.{table} rows");
            for r in 0..ta.num_rows() {
                for c in 0..ta.num_cols() {
                    let (va, vb) = (ta.get(r, c).unwrap(), tb.get(r, c).unwrap());
                    match (&va, &vb) {
                        (Value::Double(x), Value::Double(y)) => assert_eq!(
                            x.to_bits(),
                            y.to_bits(),
                            "{dbname}.{table} r{r} c{c}: {:?} vs {:?}",
                            va,
                            vb
                        ),
                        _ => assert_eq!(va, vb, "{dbname}.{table} r{r} c{c}"),
                    }
                }
            }
        }
    }
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn write_json(path: &Path, value: &serde_json::Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

/// Every lake data file (`*.pink` or `*.vortex`) under `dir`, recursively.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if !dir.exists() {
        return out;
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(data_files(&path));
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("pink") | Some("vortex")
        ) {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// The path of the lake fixture checked in under `tests/fixtures/lake_v1/`
/// (written by the wave-34 writer, before Vortex).
fn v1_fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1")
}

fn v2_fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v2")
}

/// A format_version 2 lake (one Vortex block per engine chunk) still loads
/// value- and bit-exactly with the v3 reader, and saving migrates it to v3.
#[test]
fn v2_fixture_lake_still_loads() {
    let fixture = v2_fixture_dir();
    let catalog = read_json(&fixture.join("catalog.json"));
    assert_eq!(catalog["format_version"], 2, "fixture must stay v2");
    let expected = common::fidelity_store();

    let lake = Lake::open(&fixture).unwrap();
    assert_eq!(lake.format_version(), 2);
    let loaded = lake.load(None).unwrap();
    assert_same_store(&expected, &loaded);

    // Re-save into a scratch copy: a v3 snapshot that loads identically.
    let dir = scratch("v2_migrate");
    let copy = dir.join("lake");
    copy_dir(&fixture, &copy);
    let mut lake = Lake::open(&copy).unwrap();
    lake.write_from_store(&loaded).unwrap();
    assert_eq!(lake.format_version(), 3);
    assert_same_store(&expected, &Lake::open(&copy).unwrap().load(None).unwrap());
    // The old v2 snapshot stays loadable from the upgraded lake.
    assert_same_store(
        &expected,
        &Lake::open(&copy).unwrap().load(Some(2)).unwrap(),
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_roundtrip_is_bit_exact() {
    let dir = scratch("bit_exact");
    let db = fixture_store();
    let original_bytes = store_bytes(&db, &dir, "original");

    let mut lake = Lake::create(&dir).unwrap();
    assert_eq!(lake.current_version(), 1, "create publishes an empty v1");
    assert_eq!(lake.versions().unwrap(), vec![1]);
    let report = lake.write_from_store_report(&db).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.tables, 5);
    assert_eq!(report.rows, 3 + (CHUNK_SIZE as u64 + 100) + 1 + 2);
    assert!(report.bytes_written > 0);

    // Catalog shape: v3, one Vortex file per table, `format` declared.
    let catalog = read_json(&dir.join("catalog.json"));
    assert_eq!(catalog["format_version"], FORMAT_VERSION);
    assert_eq!(FORMAT_VERSION, 3, "v3 is the current Vortex layout");
    assert_eq!(catalog["version"], 2);
    let updated = catalog["updated"].as_str().unwrap();
    assert_eq!(updated.len(), 24, "ISO-8601 millis: {updated}");
    assert!(updated.ends_with('Z'), "{updated}");
    let tables = catalog["tables"].as_object().unwrap();
    let mut keys: Vec<&String> = tables.keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "analytics.a.b",
            "analytics.u",
            "main.big",
            "main.empty",
            "main.t"
        ]
    );
    for (key, entry) in tables {
        assert_eq!(entry["format"], "vortex", "{key}");
        let files = entry["files"].as_array().unwrap();
        assert_eq!(
            files.len(),
            1,
            "the writer emits one file per table ({key})"
        );
        let rel = files[0].as_str().unwrap();
        assert!(rel.starts_with("data/"), "{rel}");
        assert!(rel.ends_with(".vortex"), "{rel}");
        assert!(dir.join(rel).is_file(), "{rel} missing");
    }
    assert!(dir.join("snapshots/1.json").is_file());
    assert!(dir.join("snapshots/2.json").is_file());
    assert_eq!(data_files(&dir.join("data")).len(), 5);
    assert!(data_files(&dir.join("data"))
        .iter()
        .all(|p| p.extension().unwrap() == "vortex"));

    // Latest load is value- and byte-exact.
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);
    assert_eq!(store_bytes(&loaded, &dir, "loaded"), original_bytes);
    // NaN payload and -0.0 survive explicitly.
    let t = loaded.table("t").unwrap();
    match t.get(2, 3).unwrap() {
        Value::Double(x) => assert_eq!(x.to_bits(), 0x7ff8_0000_dead_beef),
        other => panic!("expected the NaN payload, got {other:?}"),
    }
    assert_eq!(t.get(0, 3), Some(Value::Double(-0.0)));
    assert!(matches!(
        t.get(0, 3),
        Some(Value::Double(x)) if x.to_bits() == (-0.0f64).to_bits()
    ));
    // The empty table keeps its schema and no data chunks.
    let empty = loaded.table("empty").unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(
        empty.schema(),
        vec![
            ("x".to_string(), DataType::Integer),
            ("y".to_string(), DataType::Varchar)
        ]
    );
    assert_eq!(empty.num_chunks(), db.table("empty").unwrap().num_chunks());
    // Multi-chunk table intact.
    assert_eq!(loaded.table("big").unwrap().num_rows(), CHUNK_SIZE + 100);
    assert_eq!(loaded.table("big").unwrap().num_chunks(), 2);
    // The dotted table name restored inside `analytics`.
    assert_eq!(
        loaded.table_in("analytics", "a.b").unwrap().get(1, 0),
        Some(Value::Double(-0.0))
    );

    // Version 1 is the create-time empty catalog.
    let v1 = lake.load(Some(1)).unwrap();
    assert_eq!(v1.list_databases(), vec!["main"]);
    assert!(v1.list_tables().is_empty());
    cleanup(&dir);
}

#[test]
fn versions_accumulate_and_old_versions_stay_loadable() {
    let dir = scratch("versions");
    let mut db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    let v2_bytes = store_bytes(&db, &dir, "v2");

    // Mutate: add a row, drop a table, add a database with a table.
    db.table_mut("t")
        .unwrap()
        .append_row(vec![
            Value::Boolean(true),
            Value::Integer(1),
            Value::Bigint(2),
            Value::Double(3.5),
            Value::Varchar("new".to_string()),
            Value::Date(1),
            Value::Timestamp(2),
        ])
        .unwrap();
    db.execute("DROP TABLE main.big;").unwrap();
    db.execute("CREATE DATABASE extra; CREATE TABLE extra.e (x INTEGER);")
        .unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);

    assert_eq!(lake.current_version(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    // v2 wrote 5 files, v3 wrote 5 more (main.t changed, main.big dropped,
    // extra.e added): old data files are never touched.
    assert_eq!(
        data_files(&dir.join("data")).len(),
        10,
        "old data files stay"
    );

    let latest = lake.load(None).unwrap();
    assert_same_store(&db, &latest);
    assert_eq!(
        store_bytes(&latest, &dir, "latest"),
        store_bytes(&db, &dir, "current")
    );

    let old = lake.load(Some(2)).unwrap();
    assert_eq!(store_bytes(&old, &dir, "old"), v2_bytes);
    assert_eq!(old.table("t").unwrap().num_rows(), 3);
    assert!(old.table("big").is_ok(), "v2 still has the dropped table");
    assert!(!old.database_exists("extra"));

    let missing = lake.load(Some(99)).unwrap_err();
    assert!(
        missing.to_string().contains("has no snapshot version 99"),
        "{missing}"
    );

    // Tables of the current version, sorted, with row counts.
    let tables = lake.tables().unwrap();
    assert_eq!(
        tables,
        vec![
            ("analytics.a.b".to_string(), 2),
            ("analytics.u".to_string(), 1),
            ("extra.e".to_string(), 0),
            ("main.empty".to_string(), 0),
            ("main.t".to_string(), 4),
        ]
    );
    cleanup(&dir);
}

#[test]
fn crash_leftovers_are_ignored() {
    let dir = scratch("stray");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);

    // A crashed save would leave exactly this kind of debris — none of it is
    // reachable from any manifest.
    std::fs::write(dir.join("data/main.t/deadbeef.pink"), b"junk").unwrap();
    std::fs::create_dir_all(dir.join("data/ghost/e/")).unwrap();
    std::fs::write(dir.join("data/ghost/e/stray.pink"), b"junk").unwrap();
    std::fs::write(dir.join("snapshots/2.json.tmp"), b"half-written").unwrap();
    std::fs::write(dir.join("catalog.json.tmp"), b"half-written").unwrap();
    // A snapshot above the catalog's version (interrupted publish).
    let mut stray: serde_json::Value = read_json(&dir.join("catalog.json"));
    stray["version"] = serde_json::json!(99);
    write_json(&dir.join("snapshots/99.json"), &stray);
    std::fs::write(dir.join("snapshots/garbage.json"), b"not json").unwrap();

    let lake = Lake::open(&dir).unwrap();
    assert_eq!(lake.current_version(), 2);
    assert_eq!(lake.versions().unwrap(), vec![1, 2], "99 is not published");
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);
    assert_eq!(lake.tables().unwrap().len(), 5);

    // The next save publishes version 3 (it never touches the stray 99).
    let mut lake = lake;
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    assert_eq!(read_json(&dir.join("snapshots/3.json"))["version"], 3);
    assert_same_store(&db, &lake.load(None).unwrap());
    cleanup(&dir);
}

#[test]
fn manifest_without_catalog_keeps_the_previous_version() {
    let dir = scratch("catalog_stale");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    let v2_bytes = store_bytes(&db, &dir, "v2");

    // Simulate a crash between "snapshot manifest written" and
    // "catalog.json replaced": v3's manifest exists, the catalog still
    // says v2.
    let mut v3: serde_json::Value = read_json(&dir.join("snapshots/2.json"));
    v3["version"] = serde_json::json!(3);
    write_json(&dir.join("snapshots/3.json"), &v3);

    let lake = Lake::open(&dir).unwrap();
    assert_eq!(lake.current_version(), 2, "the catalog is authoritative");
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
    assert_eq!(
        store_bytes(&lake.load(None).unwrap(), &dir, "prev"),
        v2_bytes
    );

    // The leftover manifest is self-contained (its data files were written
    // first), so an explicit load works — it is ignored, not corrupt.
    let explicit = lake.load(Some(3)).unwrap();
    assert_eq!(store_bytes(&explicit, &dir, "explicit"), v2_bytes);

    // A retry refuses to replace the (never published) stray: a save never
    // replaces an existing snapshots/<v>.json.
    let mut lake = lake;
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(&err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("snapshot 3 already exists"), "{msg}");
    assert!(msg.contains("refusing to replace"), "{msg}");
    assert!(msg.contains("never published"), "{msg}");
    assert_eq!(lake.current_version(), 2, "nothing was published");

    // Deleting the unpublished stray (the documented recovery) lets the
    // retry publish v3.
    std::fs::remove_file(dir.join("snapshots/3.json")).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    assert_eq!(read_json(&dir.join("snapshots/3.json"))["version"], 3);
    cleanup(&dir);
}

#[test]
fn corrupt_metadata_is_a_clear_error_not_a_panic() {
    let dir = scratch("corrupt");
    let db = fixture_store();

    // Not JSON at all.
    let mut lake = Lake::create(&dir).unwrap();
    std::fs::write(dir.join("catalog.json"), b"not json").unwrap();
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("catalog.json"), "{err}");
    // A live handle refuses to publish over the corrupted catalog (the
    // pre-save check reads the same file), instead of silently clobbering
    // whatever is there.
    let err = lake.write_from_store(&db).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    // Removing the unreadable catalog lets open_or_create heal the lake from
    // its empty v1 snapshot.
    std::fs::remove_file(dir.join("catalog.json")).unwrap();
    let mut lake = Lake::open_or_create(&dir).unwrap();
    assert_eq!(lake.current_version(), 1);

    // A valid catalog whose table references a path outside data/.
    lake.write_from_store(&db).unwrap();
    let mut evil = read_json(&dir.join("catalog.json"));
    evil["tables"]["main.t"]["files"] = serde_json::json!(["../evil.pink"]);
    write_json(&dir.join("catalog.json"), &evil);
    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("under data/"), "{err}");

    // An absolute path is refused too.
    let mut abs = read_json(&dir.join("catalog.json"));
    abs["tables"]["main.t"]["files"] = serde_json::json!(["/etc/passwd"]);
    write_json(&dir.join("catalog.json"), &abs);
    assert!(lake
        .load(None)
        .unwrap_err()
        .to_string()
        .contains("under data/"));

    // An unsupported format_version is named, not misparsed.
    let mut future = read_json(&dir.join("catalog.json"));
    future["format_version"] = serde_json::json!(4);
    write_json(&dir.join("catalog.json"), &future);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("unsupported lake format_version 4"), "{err}");
    assert!(err.contains("Vortex form"), "{err}");

    // A version 2 manifest must declare a format on every entry; a missing
    // one is corrupt v2 metadata, not a silent `.pink` downgrade.
    let mut no_format = read_json(&dir.join("catalog.json"));
    no_format["format_version"] = serde_json::json!(2);
    no_format["tables"]["main.t"]
        .as_object_mut()
        .unwrap()
        .remove("format");
    write_json(&dir.join("catalog.json"), &no_format);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("does not declare a data file format"), "{err}");

    // A version 1 manifest cannot reference a vortex data file.
    let mut v1_vortex = read_json(&dir.join("catalog.json"));
    v1_vortex["format_version"] = serde_json::json!(1);
    v1_vortex["tables"]["main.t"]["format"] = serde_json::json!("vortex");
    write_json(&dir.join("catalog.json"), &v1_vortex);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(
        err.contains("vortex data file in a format_version 1 manifest"),
        "{err}"
    );

    // A missing field is corrupt, not defaulted.
    write_json(
        &dir.join("catalog.json"),
        &serde_json::json!({"format_version": 1, "version": 2, "tables": {}}),
    );
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");

    // No data files for a table.
    write_json(
        &dir.join("catalog.json"),
        &serde_json::json!({
            "format_version": 1,
            "updated": "2026-10-14T00:00:00.000Z",
            "version": 2,
            "tables": {"main.t": {"files": [], "rows": 3}}
        }),
    );
    let lake = Lake::open(&dir).unwrap();
    assert!(lake
        .load(None)
        .unwrap_err()
        .to_string()
        .contains("no data files"));

    // A snapshot whose declared version disagrees with its file name.
    let mut wrong = read_json(&dir.join("catalog.json"));
    wrong["version"] = serde_json::json!(7);
    write_json(&dir.join("snapshots/5.json"), &wrong);
    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(Some(5)).unwrap_err().to_string();
    assert!(err.contains("declares version 7, expected 5"), "{err}");

    // A referenced data file that does not exist.
    write_json(
        &dir.join("catalog.json"),
        &serde_json::json!({
            "format_version": 1,
            "updated": "2026-10-14T00:00:00.000Z",
            "version": 2,
            "tables": {"main.t": {"files": ["data/main.t/missing.pink"], "rows": 3}}
        }),
    );
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("loading data file"), "{err}");
    cleanup(&dir);
}

#[test]
fn save_store_creates_open_or_create_and_errors_are_clear() {
    let dir = scratch("create");
    let missing = dir.join("nested/lake-here");
    let db = fixture_store();

    // A fresh (even missing-parent) directory becomes a lake.
    assert_eq!(save_store(&db, &missing).unwrap(), 2);
    assert_eq!(save_store(&db, &missing).unwrap(), 3);
    let lake = Lake::open(&missing).unwrap();
    assert_eq!(lake.current_version(), 3);
    assert_eq!(lake.load(None).unwrap().table("t").unwrap().num_rows(), 3);

    // create refuses an existing lake; open refuses a non-lake.
    let err = Lake::create(&missing).unwrap_err().to_string();
    assert!(err.contains("already a pink-lake directory"), "{err}");
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("not a pink-lake directory"), "{err}");
    assert!(err.contains("no catalog.json"), "{err}");
    cleanup(&dir);
}

#[test]
fn a_table_entry_can_list_several_data_files() {
    let dir = scratch("multifile");
    let other = scratch("multifile-src");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
        .unwrap();
    for i in 0..3 {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    // A second lake with the same schema; graft its data file into the first.
    let mut lake2 = Lake::create(&other).unwrap();
    let mut db2 = PinkDb::new();
    db2.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
        .unwrap();
    for i in 3..6 {
        db2.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    lake2.write_from_store(&db2).unwrap();
    let src = data_files(&other.join("data"));
    assert_eq!(src.len(), 1);
    let grafted_name = src[0].file_name().unwrap().to_str().unwrap().to_string();
    let files_dir = dir.join("data/main.t");
    std::fs::copy(&src[0], files_dir.join(&grafted_name)).unwrap();

    let mut catalog = read_json(&dir.join("catalog.json"));
    let existing = catalog["tables"]["main.t"]["files"][0]
        .as_str()
        .unwrap()
        .to_string();
    catalog["tables"]["main.t"]["files"] =
        serde_json::json!([existing, format!("data/main.t/{grafted_name}")]);
    catalog["tables"]["main.t"]["rows"] = serde_json::json!(6);
    write_json(&dir.join("catalog.json"), &catalog);
    write_json(&dir.join("snapshots/2.json"), &catalog);

    let lake = Lake::open(&dir).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 6);
    assert_eq!(
        loaded.table("t").unwrap().get(4, 1),
        Some(Value::Varchar("v4".into()))
    );

    // A row count that disagrees with the files is corrupt.
    catalog["tables"]["main.t"]["rows"] = serde_json::json!(999);
    write_json(&dir.join("catalog.json"), &catalog);
    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(
        err.contains("has 6 rows, but the catalog says 999"),
        "{err}"
    );

    cleanup(&dir);
    cleanup(&other);
}

#[test]
fn files_with_mismatched_table_names_are_rejected() {
    let dir = scratch("mismatch");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    // Point main.t's catalog entry at the data file of analytics.u.
    let mut catalog = read_json(&dir.join("catalog.json"));
    let u_file = catalog["tables"]["analytics.u"]["files"][0]
        .as_str()
        .unwrap()
        .to_string();
    catalog["tables"]["main.t"]["files"] = serde_json::json!([u_file]);
    write_json(&dir.join("catalog.json"), &catalog);

    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("data file holds analytics.u"), "{err}");
    cleanup(&dir);
}

#[test]
fn same_schema_equal_rows_swaps_are_detected_by_identity() {
    let dir = scratch("identity_swap");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE a (x INTEGER);").unwrap();
    db.execute("CREATE TABLE b (x INTEGER);").unwrap();
    db.execute("INSERT INTO a VALUES (1);").unwrap();
    db.execute("INSERT INTO b VALUES (2);").unwrap();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    // Point `main.a` at `main.b`'s data file: name, schema and row count all
    // match, so only the `pinkdb.table` identity can reject the swap.
    let mut catalog = read_json(&dir.join("catalog.json"));
    let b_file = catalog["tables"]["main.b"]["files"][0]
        .as_str()
        .unwrap()
        .to_string();
    catalog["tables"]["main.a"]["files"] = serde_json::json!([b_file]);
    write_json(&dir.join("catalog.json"), &catalog);

    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("data file holds main.b"), "{err}");
    assert!(err.contains("catalog says \"main.a\""), "{err}");
    cleanup(&dir);
}

#[test]
fn database_names_with_a_dot_are_refused_cleanly() {
    let dir = scratch("dotted_db");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER);").unwrap();
    db.execute("CREATE DATABASE \"a.b\";").unwrap();
    db.execute("CREATE TABLE \"a.b\".u (y INTEGER);").unwrap();

    let mut lake = Lake::create(&dir).unwrap();
    let err = lake.write_from_store(&db).unwrap_err().to_string();
    assert!(err.contains("contains \".\""), "{err}");
    assert!(err.contains("db.table"), "{err}");
    // The failed save published nothing.
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.versions().unwrap(), vec![1]);
    cleanup(&dir);
}

#[test]
fn table_names_stay_inside_the_data_directory() {
    let dir = scratch("names");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE \"odd name\" (x INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE \"a/b\" (x INTEGER);").unwrap();
    db.execute("CREATE TABLE \"..\" (x INTEGER);").unwrap();
    db.execute("INSERT INTO \"odd name\" VALUES (1);").unwrap();

    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);

    // Every data directory is a single safe segment under data/.
    let mut segments: Vec<String> = std::fs::read_dir(dir.join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
        .collect();
    segments.sort();
    assert_eq!(segments, vec!["main...", "main.a%2Fb", "main.odd%20name"]);
    assert_eq!(data_files(&dir.join("data")).len(), 3);
    cleanup(&dir);
}

#[test]
fn empty_databases_and_dropped_main_are_documented_v1_limits() {
    let dir = scratch("emptydb");
    let mut db = PinkDb::new();
    db.execute("CREATE DATABASE only;").unwrap();
    db.execute("CREATE TABLE only.t (x INTEGER);").unwrap();
    db.execute("INSERT INTO only.t VALUES (5);").unwrap();
    db.execute("DROP DATABASE main;").unwrap();
    db.execute("CREATE DATABASE empty;").unwrap();

    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    // `only` is rebuilt; a dropped `main` and an empty database are not.
    assert_eq!(loaded.list_databases(), vec!["main", "only"]);
    assert!(loaded.list_tables_in("main").is_empty());
    assert!(loaded.list_tables_in("empty").is_empty());
    assert!(!loaded.database_exists("empty"));
    assert_eq!(
        loaded.table_in("only", "t").unwrap().get(0, 0),
        Some(Value::Integer(5))
    );
    cleanup(&dir);
}

#[test]
fn tampered_versions_are_clean_errors_never_a_wrap() {
    let dir = scratch("version_range");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v2

    // `version: 0` and `version: u64::MAX` are corrupt, not wrapped or
    // panicked on. This must hold in debug (overflow checks on) and release
    // (wrap possible if unchecked) builds alike.
    for bad in [0u64, u64::MAX] {
        let mut cat = read_json(&dir.join("catalog.json"));
        cat["version"] = serde_json::json!(bad);
        write_json(&dir.join("catalog.json"), &cat);
        let err = Lake::open(&dir).unwrap_err().to_string();
        assert!(err.contains("corrupt lake metadata"), "{err}");
        assert!(err.contains("outside the supported range 1..="), "{err}");
        assert!(err.contains(&bad.to_string()), "{err}");
    }

    // The cap itself (`u64::MAX - 1`) still loads, but cannot be incremented.
    let ceiling = u64::MAX - 1;
    let mut cat = read_json(&dir.join("catalog.json"));
    cat["version"] = serde_json::json!(ceiling);
    write_json(&dir.join("catalog.json"), &cat);
    let mut lake = Lake::open(&dir).unwrap();
    assert_eq!(lake.current_version(), ceiling);
    let catalog_before = std::fs::read(dir.join("catalog.json")).unwrap();
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(&err, LakeError::Overflow { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("version overflow"), "{msg}");
    assert!(msg.contains("cannot be incremented"), "{msg}");
    assert_eq!(lake.current_version(), ceiling, "the handle stays put");
    assert_eq!(
        std::fs::read(dir.join("catalog.json")).unwrap(),
        catalog_before,
        "a refused save publishes nothing"
    );

    // Snapshot manifests are validated the same way...
    let mut snap = read_json(&dir.join("snapshots/2.json"));
    snap["version"] = serde_json::json!(u64::MAX);
    write_json(&dir.join("snapshots/2.json"), &snap);
    let err = lake.load(Some(2)).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("snapshots/2.json"), "{err}");
    // ...and the unwritable snapshot is not listed as published.
    assert_eq!(lake.versions().unwrap(), vec![1]);

    // `load(Some(v))` for a version above the current one is a clean
    // not-found, never a surprise snapshot read.
    let err = lake.load(Some(ceiling)).unwrap_err().to_string();
    assert!(err.contains("has no snapshot version"), "{err}");
    cleanup(&dir);
}

#[test]
fn stale_handles_conflict_instead_of_forking_the_version_chain() {
    let dir = scratch("stale_handle");
    let db = fixture_store();
    let mut a = Lake::create(&dir).unwrap();
    a.write_from_store(&db).unwrap(); // v2
    let mut b = Lake::open(&dir).unwrap(); // sees v2
    a.write_from_store(&db).unwrap(); // v3

    let data_files_before = data_files(&dir.join("data")).len();
    let snap3 = std::fs::read(dir.join("snapshots/3.json")).unwrap();
    let err = b.write_from_store(&db).unwrap_err();
    assert!(matches!(&err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("write conflict"), "{msg}");
    assert!(msg.contains("another writer published version 3"), "{msg}");
    assert!(msg.contains("reopen the lake"), "{msg}");
    assert_eq!(b.current_version(), 2, "the stale handle does not advance");
    assert_eq!(
        data_files(&dir.join("data")).len(),
        data_files_before,
        "a stale save writes no data files"
    );
    assert_eq!(
        std::fs::read(dir.join("snapshots/3.json")).unwrap(),
        snap3,
        "published snapshots stay byte-identical"
    );

    // Reopening picks up the published version and the chain continues.
    let mut b = Lake::open(&dir).unwrap();
    assert_eq!(b.current_version(), 3);
    assert_eq!(b.write_from_store(&db).unwrap(), 4);
    assert_eq!(a.load(Some(3)).unwrap().table("t").unwrap().num_rows(), 3);
    cleanup(&dir);
}

#[test]
fn rollback_resave_refuses_to_replace_a_published_snapshot() {
    let dir = scratch("rollback");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    let v2_catalog = std::fs::read(dir.join("catalog.json")).unwrap();
    lake.write_from_store(&db).unwrap(); // v3
    let v3_snapshot = std::fs::read(dir.join("snapshots/3.json")).unwrap();

    // "Roll back": restore the v2 catalog over the v3 one.
    std::fs::write(dir.join("catalog.json"), &v2_catalog).unwrap();
    let mut lake = Lake::open(&dir).unwrap();
    assert_eq!(lake.current_version(), 2);
    assert_eq!(lake.versions().unwrap(), vec![1, 2], "v3 left the chain");

    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(&err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("snapshot 3 already exists"), "{msg}");
    assert!(
        msg.contains("refusing to replace a published version"),
        "{msg}"
    );
    assert_eq!(
        std::fs::read(dir.join("snapshots/3.json")).unwrap(),
        v3_snapshot,
        "the published snapshot is untouched"
    );
    assert_eq!(
        std::fs::read(dir.join("catalog.json")).unwrap(),
        v2_catalog,
        "the catalog is untouched"
    );
    // The rolled-back-to version still loads; the orphaned snapshot remains
    // explicitly loadable.
    assert!(lake.load(None).is_ok());
    assert!(lake.load(Some(3)).is_ok());
    cleanup(&dir);
}

#[test]
fn a_catalog_without_tables_is_corrupt_not_an_empty_lake() {
    let dir = scratch("no_tables");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let good_catalog = std::fs::read(dir.join("catalog.json")).unwrap();

    let mut cat = read_json(&dir.join("catalog.json"));
    cat.as_object_mut().unwrap().remove("tables");
    write_json(&dir.join("catalog.json"), &cat);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("tables"), "{err}");

    // The same missing field in a snapshot is corrupt too.
    std::fs::write(dir.join("catalog.json"), &good_catalog).unwrap();
    let mut snap = read_json(&dir.join("snapshots/2.json"));
    snap.as_object_mut().unwrap().remove("tables");
    write_json(&dir.join("snapshots/2.json"), &snap);
    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(Some(2)).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("tables"), "{err}");
    cleanup(&dir);
}

#[test]
fn versions_lists_only_valid_canonical_snapshots() {
    let dir = scratch("versions_list");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v1, v2
    let one = std::fs::read(dir.join("snapshots/1.json")).unwrap();

    // Non-canonical names are not versions: `01.json` must not list as 1,
    // and `manifest(Some(1))` never reads it either.
    std::fs::write(dir.join("snapshots/01.json"), &one).unwrap();
    std::fs::write(dir.join("snapshots/1e1.json"), &one).unwrap();
    std::fs::write(dir.join("snapshots/1.json.bak"), &one).unwrap();
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);

    // A corrupt manifest is skipped, not listed (and load names the file).
    std::fs::write(dir.join("snapshots/1.json"), b"not json").unwrap();
    assert_eq!(lake.versions().unwrap(), vec![2]);
    let err = lake.load(Some(1)).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("snapshots/1.json"), "{err}");

    // A manifest declaring a different version than its name is skipped.
    std::fs::write(dir.join("snapshots/1.json"), &one).unwrap();
    let mut wrong = read_json(&dir.join("snapshots/2.json"));
    wrong["version"] = serde_json::json!(7);
    write_json(&dir.join("snapshots/2.json"), &wrong);
    assert_eq!(lake.versions().unwrap(), vec![1]);
    cleanup(&dir);
}

#[test]
fn save_rejects_unrepresentable_stores_before_creating_a_lake() {
    let dir = scratch("prevalidate");
    let lake_dir = dir.join("lake");

    // A dotted database name cannot be a flat `db.table` key.
    let mut dotted = PinkDb::new();
    dotted.execute("CREATE DATABASE \"a.b\";").unwrap();
    dotted
        .execute("CREATE TABLE \"a.b\".t (x INTEGER);")
        .unwrap();
    let err = validate_store(&dotted).unwrap_err().to_string();
    assert!(err.contains("contains \".\""), "{err}");
    let err = save_store(&dotted, &lake_dir).unwrap_err().to_string();
    assert!(err.contains("contains \".\""), "{err}");
    assert!(
        !lake_dir.join("catalog.json").exists(),
        "a rejected save leaves no partial lake"
    );

    // A table name whose encoded `data/` directory blows the filesystem
    // name limit is refused up front (the old behavior died with
    // ENAMETOOLONG after creating an empty v1 lake).
    let mut long = PinkDb::new();
    let name = "é".repeat(100); // 200 UTF-8 bytes -> 600 encoded bytes
    long.execute(&format!("CREATE TABLE \"{name}\" (x INTEGER);"))
        .unwrap();
    let err = validate_store(&long).unwrap_err().to_string();
    assert!(err.contains("over the 200-byte limit"), "{err}");
    let err = save_store(&long, &lake_dir).unwrap_err().to_string();
    assert!(err.contains("over the 200-byte limit"), "{err}");
    assert!(!lake_dir.join("catalog.json").exists());
    assert!(validate_store(&PinkDb::new()).is_ok());

    // A long-but-encodable name still saves: the check is not over-strict.
    let mut ok = PinkDb::new();
    let name = "a".repeat(180);
    ok.execute(&format!("CREATE TABLE \"{name}\" (x INTEGER);"))
        .unwrap();
    assert!(validate_store(&ok).is_ok());
    assert_eq!(save_store(&ok, &lake_dir).unwrap(), 2);
    let lake = Lake::open(&lake_dir).unwrap();
    assert_eq!(lake.load(None).unwrap().list_tables().len(), 1);
    cleanup(&dir);
}

#[cfg(unix)]
#[test]
fn a_failed_data_write_keeps_the_published_version() {
    use std::os::unix::fs::PermissionsExt;

    let dir = scratch("perm_fail");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    let catalog_before = std::fs::read(dir.join("catalog.json")).unwrap();

    // Make one data directory read-only so the next save's data-file write
    // fails mid-save. The rename/fsync machinery must leave v2 published.
    let t_dir = dir.join("data/main.t");
    std::fs::set_permissions(&t_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(t_dir.join("probe"), b"x").is_ok() {
        // Running as root (or on a filesystem ignoring mode bits): the
        // failure cannot be injected, so there is nothing to assert.
        std::fs::remove_file(t_dir.join("probe")).ok();
        std::fs::set_permissions(&t_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        cleanup(&dir);
        return;
    }
    let err = lake.write_from_store(&db).unwrap_err().to_string();
    assert!(err.contains("lake I/O error"), "{err}");
    assert_eq!(lake.current_version(), 2);
    assert_eq!(
        std::fs::read(dir.join("catalog.json")).unwrap(),
        catalog_before,
        "a failed save does not touch the catalog"
    );
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);

    // Restoring permissions recovers; v3 publishes and loads.
    std::fs::set_permissions(&t_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    assert_same_store(&db, &lake.load(None).unwrap());
    cleanup(&dir);
}

#[cfg(unix)]
#[test]
fn a_catalog_rename_failure_keeps_the_previous_catalog_authoritative() {
    use std::os::unix::fs::PermissionsExt;

    let dir = scratch("catalog_fail");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    let catalog_before = std::fs::read(dir.join("catalog.json")).unwrap();

    // The lake root read-only (subdirectories stay writable) fails exactly
    // the last step of a save: data files and the v3 snapshot publish, the
    // catalog replacement does not. This is the crash window between
    // "snapshot linked" and "catalog replaced", injected without a crash.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(dir.join("probe"), b"x").is_ok() {
        std::fs::remove_file(dir.join("probe")).ok();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        cleanup(&dir);
        return;
    }
    let err = lake.write_from_store(&db).unwrap_err().to_string();
    assert!(err.contains("lake I/O error"), "{err}");
    assert_eq!(lake.current_version(), 2);
    assert_eq!(
        std::fs::read(dir.join("catalog.json")).unwrap(),
        catalog_before,
        "the previous catalog stayed authoritative"
    );
    assert!(
        dir.join("snapshots/3.json").is_file(),
        "the stray manifest is there"
    );
    assert_eq!(
        lake.versions().unwrap(),
        vec![1, 2],
        "an unpublished stray is not listed"
    );
    assert!(
        lake.load(Some(3)).is_ok(),
        "the stray manifest is self-contained"
    );

    // The documented recovery: delete the unpublished stray, retry.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::remove_file(dir.join("snapshots/3.json")).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    assert_same_store(&db, &lake.load(None).unwrap());
    cleanup(&dir);
}

#[test]
fn a_directory_named_catalog_json_is_corrupt_not_io_noise() {
    let dir = scratch("catalog_dir");
    std::fs::create_dir_all(dir.join("catalog.json")).unwrap();
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("not a regular file"), "{err}");
    assert!(!err.contains("os error"), "{err}");
    cleanup(&dir);
}

#[test]
fn missing_data_files_are_reported_but_metadata_still_reads() {
    let dir = scratch("missing_files");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    assert!(lake.missing_data_files().unwrap().is_empty());

    let victim = data_files(&dir.join("data/main.t")).remove(0);
    std::fs::remove_file(&victim).unwrap();
    let missing = lake.missing_data_files().unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].0, "main.t");
    assert!(missing[0]
        .1
        .ends_with(victim.file_name().unwrap().to_str().unwrap()));
    // Metadata still reads; only a load fails, and it says why.
    assert!(lake.tables().unwrap().iter().any(|(k, _)| k == "main.t"));
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("loading data file"), "{err}");
    cleanup(&dir);
}

#[test]
fn error_paths_point_at_the_manifest_that_is_wrong() {
    let dir = scratch("error_paths");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    // A snapshot with a flat key that is not `db.table` names the snapshot.
    let mut snap = read_json(&dir.join("snapshots/2.json"));
    snap["tables"] =
        serde_json::json!({"nodot": {"files": ["data/x.vortex"], "rows": 1, "format": "vortex"}});
    write_json(&dir.join("snapshots/2.json"), &snap);
    let err = lake.load(Some(2)).unwrap_err().to_string();
    assert!(err.contains("snapshots/2.json"), "{err}");
    assert!(err.contains("not \"db.table\""), "{err}");

    // "no data files" in a snapshot names the snapshot, not the lake root.
    let mut snap = read_json(&dir.join("snapshots/2.json"));
    snap["tables"] = serde_json::json!({"main.t": {"files": [], "rows": 0, "format": "vortex"}});
    write_json(&dir.join("snapshots/2.json"), &snap);
    let err = lake.load(Some(2)).unwrap_err().to_string();
    assert!(err.contains("snapshots/2.json"), "{err}");
    assert!(err.contains("no data files"), "{err}");

    // A snapshot row-count mismatch names the snapshot too.
    let file = read_json(&dir.join("catalog.json"))["tables"]["main.t"]["files"][0].clone();
    let mut snap = read_json(&dir.join("snapshots/2.json"));
    snap["tables"]["main.t"] =
        serde_json::json!({"files": [file], "rows": 999, "format": "vortex"});
    write_json(&dir.join("snapshots/2.json"), &snap);
    let err = lake.load(Some(2)).unwrap_err().to_string();
    assert!(err.contains("snapshots/2.json"), "{err}");
    assert!(err.contains("catalog says 999"), "{err}");

    // The same catalog-level mismatch names catalog.json.
    let mut cat = read_json(&dir.join("catalog.json"));
    cat["tables"]["main.t"]["rows"] = serde_json::json!(999);
    write_json(&dir.join("catalog.json"), &cat);
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("catalog.json"), "{err}");
    assert!(err.contains("catalog says 999"), "{err}");
    cleanup(&dir);
}

#[test]
fn an_interrupted_create_is_healed_not_bricked() {
    let dir = scratch("interrupted_create");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v2

    // Simulate a crash between the v1 snapshot link and the catalog link,
    // on a lake whose title catalog was then removed: the empty v1 snapshot
    // is adopted, byte-identical, instead of rewritten or refused.
    let catalog = std::fs::read(dir.join("catalog.json")).unwrap();
    std::fs::remove_file(dir.join("catalog.json")).unwrap();
    let lake = Lake::open_or_create(&dir).unwrap();
    assert_eq!(lake.current_version(), 1);
    assert_eq!(
        std::fs::read(dir.join("catalog.json")).unwrap(),
        std::fs::read(dir.join("snapshots/1.json")).unwrap(),
        "catalog and its snapshot are byte-identical"
    );
    std::fs::write(dir.join("catalog.json"), &catalog).unwrap();

    // A snapshots/1.json that is not the empty v1 (tampered) is refused with
    // a clear conflict, never adopted.
    let bad = scratch("bad_adopt");
    std::fs::create_dir_all(bad.join("snapshots")).unwrap();
    let mut cat = read_json(&dir.join("catalog.json"));
    cat["tables"] = serde_json::json!({"main.t": {"files": ["data/main.t/x.pink"], "rows": 1}});
    write_json(&bad.join("snapshots/1.json"), &cat);
    let err = Lake::open_or_create(&bad).unwrap_err();
    assert!(matches!(&err, LakeError::Conflict { .. }), "{err:?}");
    assert!(err.to_string().contains("refusing to adopt"), "{err}");
    cleanup(&bad);
    cleanup(&dir);
}

/// Every data-file path declared by the current catalog, sorted.
fn catalog_data_files(dir: &Path) -> Vec<String> {
    let catalog = read_json(&dir.join("catalog.json"));
    let mut out = Vec::new();
    for entry in catalog["tables"].as_object().unwrap().values() {
        for file in entry["files"].as_array().unwrap() {
            out.push(file.as_str().unwrap().to_string());
        }
    }
    out.sort();
    out
}

#[test]
fn v1_fixture_lake_still_loads_bit_exactly() {
    let fixture = v1_fixture_dir();
    let scratch = scratch("v1_fixture_bits");
    // The fixture is a v1 lake: v1 manifests, engine `.pink` data files.
    let catalog = read_json(&fixture.join("catalog.json"));
    assert_eq!(catalog["format_version"], 1, "fixture must stay v1");
    let files = data_files(&fixture.join("data"));
    assert_eq!(files.len(), 6);
    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        assert!(
            bytes.starts_with(b"PINKDB0"),
            "{} is not engine `.pink` bytes",
            file.display()
        );
        assert_eq!(file.extension().unwrap(), "pink");
    }

    // It loads, and the loaded store is value- and bit-exact vs. a store
    // rebuilt from the same builder.
    let lake = Lake::open(&fixture).unwrap();
    assert_eq!(lake.format_version(), 1);
    assert_eq!(lake.current_version(), 2);
    let expected = common::fidelity_store();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&expected, &loaded);
    assert_eq!(
        store_bytes(&loaded, &scratch, "loaded"),
        store_bytes(&expected, &scratch, "expected"),
        "the loaded store re-saves byte-identically"
    );

    // Every data file is byte-exact: load the mini store and re-save it.
    for rel in catalog_data_files(&fixture) {
        let path = fixture.join(&rel);
        let mini = PinkDb::open(path.to_str().unwrap()).unwrap();
        let resaved = scratch.join("resaved.pink");
        mini.save(resaved.to_str().unwrap()).unwrap();
        // The engine now writes `.pink` v3 = the v2 payload with the
        // `PINKDB03` magic and a CRC-32 trailer; the payload must be
        // byte-identical to the fixture's v2 bytes.
        let resaved = std::fs::read(&resaved).unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(original.starts_with(b"PINKDB02"), "{rel}");
        assert!(resaved.starts_with(b"PINKDB03"), "{rel}");
        assert_eq!(
            &resaved[8..resaved.len() - 4],
            &original[8..],
            "{rel} does not round-trip byte-exactly"
        );
    }
    cleanup(&scratch);

    // Spot-check the extremes the fixture pins (bit level).
    let fid = loaded.table("fid").unwrap();
    assert_eq!(fid.num_rows(), 6);
    assert!(matches!(fid.get(0, 2), Some(Value::Bigint(i64::MIN))));
    assert!(matches!(fid.get(1, 1), Some(Value::Integer(i32::MAX))));
    assert!(matches!(
        fid.get(1, 3),
        Some(Value::Double(x)) if x.to_bits() == 0x7ff8_0000_dead_beef
    ));
    assert!(matches!(
        fid.get(4, 3),
        Some(Value::Double(x)) if x.to_bits() == 0xfff8_0000_0000_0001
    ));
    assert!(matches!(
        fid.get(0, 3),
        Some(Value::Double(x)) if x.to_bits() == (-0.0f64).to_bits()
    ));
    assert_eq!(
        fid.get(1, 4),
        Some(Value::Varchar("héllo wörld 🌍\0embedded".into()))
    );
    assert_eq!(fid.get(0, 4), Some(Value::Varchar(String::new())));
    assert_eq!(fid.get(2, 0), Some(Value::Null));
    assert!(matches!(fid.get(0, 5), Some(Value::Date(i32::MIN))));
    assert!(matches!(fid.get(1, 6), Some(Value::Timestamp(i64::MAX))));

    // The unicode table name survived its percent-encoded data directory.
    assert_eq!(
        loaded.table("héllo✓").unwrap().get(0, 0),
        Some(Value::Varchar("π≈3.14".into()))
    );
    // Version 1 (create-time empty) still loads.
    let v1 = lake.load(Some(1)).unwrap();
    assert!(v1.list_tables().is_empty());
    cleanup(&scratch);
}

#[test]
fn v1_lake_migrates_to_vortex_on_save() {
    let dir = scratch("v1_migrate");
    // Work on a copy: the checked-in fixture stays untouched.
    copy_dir(&v1_fixture_dir(), &dir);
    let expected = common::fidelity_store();

    let mut lake = Lake::open(&dir).unwrap();
    assert_eq!(lake.format_version(), 1, "still the old lake");
    let old = lake.load(None).unwrap();
    assert_same_store(&expected, &old);

    // Saving again publishes a v3 snapshot: a full rewrite in Vortex.
    assert_eq!(lake.write_from_store(&expected).unwrap(), 3);
    assert_eq!(lake.format_version(), 3, "the handle now reports v3");
    let catalog = read_json(&dir.join("catalog.json"));
    assert_eq!(catalog["format_version"], 3);
    for (key, entry) in catalog["tables"].as_object().unwrap() {
        assert_eq!(entry["format"], "vortex", "{key}");
        assert!(
            entry["files"][0].as_str().unwrap().ends_with(".vortex"),
            "{key}"
        );
    }
    let data = data_files(&dir.join("data"));
    assert_eq!(
        data.iter()
            .filter(|p| p.extension().unwrap() == "vortex")
            .count(),
        6
    );
    assert_eq!(
        data.iter()
            .filter(|p| p.extension().unwrap() == "pink")
            .count(),
        6,
        "the v1 files stay untouched"
    );

    // The migrated version is value- and bit-exact, and old versions still
    // load from their own (v1) files.
    let migrated = lake.load(None).unwrap();
    assert_same_store(&expected, &migrated);
    let v2 = lake.load(Some(2)).unwrap();
    assert_same_store(&expected, &v2);
    assert_eq!(lake.load(Some(1)).unwrap().list_tables().len(), 0);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);

    // The migrated chunking matches: the same `.pink` bytes come back out.
    assert_eq!(
        store_bytes(&migrated, &dir, "migrated"),
        store_bytes(&expected, &dir, "expected")
    );
    cleanup(&dir);
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[test]
fn vortex_roundtrip_preserves_every_bit_pattern() {
    let dir = scratch("vortex_bits");
    let db = common::fidelity_store();
    let original_bytes = store_bytes(&db, &dir, "original");

    let mut lake = Lake::create(&dir).unwrap();
    assert_eq!(lake.format_version(), 3, "create publishes a v3 lake");
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();

    assert_same_store(&db, &loaded);
    // Chunk boundaries (and thus the `.pink` bytes) round-trip exactly.
    assert_eq!(loaded.table("big").unwrap().num_chunks(), 2);
    assert_eq!(
        loaded.table("empty").unwrap().num_chunks(),
        db.table("empty").unwrap().num_chunks()
    );
    assert_eq!(
        loaded.table("empty").unwrap().schema(),
        vec![
            ("x".to_string(), DataType::Integer),
            ("y".to_string(), DataType::Varchar)
        ]
    );
    assert_eq!(store_bytes(&loaded, &dir, "loaded"), original_bytes);

    // The float extremes survive bit-exactly, including NaN payloads a
    // naive writer would canonicalize.
    let fid = loaded.table("fid").unwrap();
    let codes: Vec<u64> = (0..fid.num_rows())
        .filter_map(|r| match fid.get(r, 3) {
            Some(Value::Double(x)) => Some(x.to_bits()),
            _ => None,
        })
        .collect();
    assert!(codes.contains(&(-0.0f64).to_bits()));
    assert!(codes.contains(&0.0f64.to_bits()));
    assert!(
        codes.contains(&0x7ff8_0000_dead_beef),
        "positive NaN payload"
    );
    assert!(
        codes.contains(&0xfff8_0000_0000_0001),
        "negative NaN payload"
    );
    assert!(codes.contains(&f64::INFINITY.to_bits()));

    // DATE/TIMESTAMP extremes, i32/i64 extremes, NULLs, strings.
    assert!(matches!(fid.get(0, 5), Some(Value::Date(i32::MIN))));
    assert!(matches!(fid.get(1, 5), Some(Value::Date(i32::MAX))));
    assert!(matches!(fid.get(0, 6), Some(Value::Timestamp(i64::MIN))));
    assert!(matches!(fid.get(1, 6), Some(Value::Timestamp(i64::MAX))));
    assert!(matches!(fid.get(0, 2), Some(Value::Bigint(i64::MIN))));
    assert!(matches!(fid.get(1, 2), Some(Value::Bigint(i64::MAX))));
    assert_eq!(fid.get(2, 0), Some(Value::Null));
    assert_eq!(
        fid.get(1, 4),
        Some(Value::Varchar("héllo wörld 🌍\0embedded".into()))
    );
    assert_eq!(fid.get(5, 4), Some(Value::Varchar(String::new())));
    cleanup(&dir);
}

#[test]
fn a_corrupt_vortex_data_file_is_a_clear_error() {
    let dir = scratch("vortex_corrupt");
    let db = common::fidelity_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    let victim = data_files(&dir.join("data/main.fid"))
        .into_iter()
        .next()
        .expect("main.fid data file");
    // Truncate it to a prefix: not a Vortex file anymore.
    let bytes = std::fs::read(&victim).unwrap();
    std::fs::write(&victim, &bytes[..bytes.len() / 2]).unwrap();
    let lake = Lake::open(&dir).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("not a valid Vortex file"), "{err}");
    assert!(err.contains("main.fid"), "{err}");

    // Metadata still reads; only a load fails.
    assert!(lake.tables().unwrap().iter().any(|(k, _)| k == "main.fid"));
    cleanup(&dir);
}

#[test]
fn catalog_format_fields_are_validated() {
    let dir = scratch("format_fields");
    let db = fixture_store();
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let good = std::fs::read(dir.join("catalog.json")).unwrap();

    // `format_version` 0 and `u32::MAX` are unsupported, never guessed at.
    for bad in [0u32, u32::MAX] {
        let mut cat = read_json(&dir.join("catalog.json"));
        cat["format_version"] = serde_json::json!(bad);
        write_json(&dir.join("catalog.json"), &cat);
        let err = Lake::open(&dir).unwrap_err().to_string();
        assert!(
            err.contains(&format!("unsupported lake format_version {bad}")),
            "format_version {bad}: {err}"
        );
    }

    // An unknown `format` string is stopped by serde, not silently treated
    // as one of the known formats.
    std::fs::write(dir.join("catalog.json"), &good).unwrap();
    let mut cat = read_json(&dir.join("catalog.json"));
    cat["tables"]["main.t"]["format"] = serde_json::json!("parquet");
    write_json(&dir.join("catalog.json"), &cat);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");
    assert!(err.contains("parquet"), "{err}");

    // A v2 entry declaring `"pink"` is corrupt: the v2 writer emits Vortex
    // only, so this is tampered metadata, not a downgrade.
    let mut cat = read_json(&dir.join("catalog.json"));
    cat["tables"]["main.t"]["format"] = serde_json::json!("pink");
    write_json(&dir.join("catalog.json"), &cat);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(
        err.contains("declares a pink data file in a format_version 3 manifest"),
        "{err}"
    );
    assert!(err.contains("Vortex-era writers emit Vortex only"), "{err}");

    // A `format` of the wrong JSON type is corrupt too.
    let mut cat = read_json(&dir.join("catalog.json"));
    cat["tables"]["main.t"]["format"] = serde_json::json!(3);
    write_json(&dir.join("catalog.json"), &cat);
    let err = Lake::open(&dir).unwrap_err().to_string();
    assert!(err.contains("corrupt lake metadata"), "{err}");

    // The untouched catalog still opens (the tampering was on disk only).
    std::fs::write(dir.join("catalog.json"), &good).unwrap();
    assert_eq!(Lake::open(&dir).unwrap().current_version(), 2);
    cleanup(&dir);
}

#[test]
fn extreme_values_and_chunk_shapes_roundtrip() {
    let dir = scratch("extreme_values");
    let mut db = PinkDb::new();

    // Subnormals, infinities, signaling NaNs, f64::MAX/MIN, and a 1 MiB
    // string (DOUBLEs are compared by bit pattern throughout).
    db.execute("CREATE TABLE m (d DOUBLE, s VARCHAR);").unwrap();
    {
        let t = db.table_mut("m").unwrap();
        let doubles = [
            f64::from_bits(1),                     // min positive subnormal
            f64::from_bits(0x000f_ffff_ffff_ffff), // max subnormal
            f64::from_bits(0x8000_0000_0000_0001), // min negative subnormal
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN,                              // most negative finite
            f64::from_bits(0x7ff0_0000_0000_0001), // signaling NaN
            f64::from_bits(0xfff0_0000_0000_0001), // negative signaling NaN
        ];
        for (i, d) in doubles.into_iter().enumerate() {
            t.append_row(vec![Value::Double(d), Value::Varchar(format!("v{i}"))])
                .unwrap();
        }
        t.append_row(vec![
            Value::Double(1.0),
            Value::Varchar("x".repeat(1 << 20)),
        ])
        .unwrap();
    }

    // 0-, 1-, and >CHUNK_SIZE-row tables; the big one is built through the
    // `append_chunk` bulk path, which fills the tail chunk to CHUNK_SIZE
    // before opening a new one (2000 + 1049 rows land as [2048, 1001]).
    db.execute("CREATE TABLE zero (x INTEGER, y VARCHAR);")
        .unwrap();
    db.execute("CREATE TABLE one (x INTEGER);").unwrap();
    db.execute("INSERT INTO one VALUES (7);").unwrap();
    db.execute("CREATE TABLE many (x INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("many").unwrap();
        let build = |rows: usize, offset: i32| {
            let mut x = Vector::with_capacity(DataType::Integer, rows);
            let mut s = Vector::with_capacity(DataType::Varchar, rows);
            for i in 0..rows {
                x.push_value(Value::Integer(offset + i as i32)).unwrap();
                s.push_str(&format!("r{}", offset + i as i32)).unwrap();
            }
            vec![x, s]
        };
        t.append_chunk(build(2000, 0)).unwrap();
        t.append_chunk(build(1049, 2000)).unwrap();
    }

    // An all-NULL table with two full chunks (the all-NULL multi-chunk
    // shape), built through the same bulk path.
    db.execute("CREATE TABLE nulls (a INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("nulls").unwrap();
        for _ in 0..2 {
            let mut a = Vector::with_capacity(DataType::Integer, CHUNK_SIZE);
            let mut s = Vector::with_capacity(DataType::Varchar, CHUNK_SIZE);
            for _ in 0..CHUNK_SIZE {
                a.push_null().unwrap();
                s.push_null().unwrap();
            }
            t.append_chunk(vec![a, s]).unwrap();
        }
    }

    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);

    // The 1 MiB string came back byte for byte, and the signaling-NaN and
    // f64::MIN bits survived.
    let m = loaded.table("m").unwrap();
    match m.get(8, 1) {
        Some(Value::Varchar(s)) => {
            assert_eq!(s.len(), 1 << 20);
            assert!(s.bytes().all(|b| b == b'x'));
        }
        other => panic!("expected the 1 MiB string, got {other:?}"),
    }
    assert!(matches!(
        m.get(6, 0),
        Some(Value::Double(x)) if x.to_bits() == 0x7ff0_0000_0000_0001
    ));
    assert!(matches!(
        m.get(5, 0),
        Some(Value::Double(x)) if x.to_bits() == f64::MIN.to_bits()
    ));

    // Chunk boundaries survive: 0 (schema-only file), 1, 2 via the bulk
    // path, 2 all-NULL.
    let sizes = |t: &DataTable| -> Vec<usize> {
        (0..t.num_chunks())
            .map(|i| t.scan_chunk(i).unwrap().num_rows())
            .collect()
    };
    assert_eq!(loaded.table("zero").unwrap().num_rows(), 0);
    assert_eq!(
        loaded.table("zero").unwrap().num_chunks(),
        db.table("zero").unwrap().num_chunks()
    );
    assert_eq!(loaded.table("one").unwrap().num_rows(), 1);
    assert_eq!(sizes(loaded.table("many").unwrap()), vec![2048, 1001]);
    assert_eq!(sizes(loaded.table("nulls").unwrap()), vec![CHUNK_SIZE; 2]);
    let null_chunk = loaded.table("nulls").unwrap().scan_chunk(0).unwrap();
    assert!(
        (0..null_chunk.num_rows)
            .all(|r| (0..null_chunk.columns.len()).all(|c| null_chunk.columns[c].is_null(r))),
        "every value of the all-NULL chunk must stay NULL"
    );

    // The loaded store still re-saves to the same `.pink` bytes.
    assert_eq!(
        store_bytes(&loaded, &dir, "loaded"),
        store_bytes(&db, &dir, "orig")
    );
    cleanup(&dir);
}

#[test]
fn csv_import_chunk_boundaries_roundtrip() {
    let dir = scratch("csv_chunks");
    let mut db = PinkDb::new();
    let mut csv = String::from("id,name\n");
    for i in 0..(CHUNK_SIZE + 123) {
        csv.push_str(&format!("{i},row-{i}\n"));
    }
    let rows = pinkdb::csv::import_csv_string(&mut db, "t", &csv).unwrap();
    assert_eq!(rows, CHUNK_SIZE + 123);

    let sizes = |t: &DataTable| -> Vec<usize> {
        (0..t.num_chunks())
            .map(|i| t.scan_chunk(i).unwrap().num_rows())
            .collect()
    };
    assert_eq!(sizes(db.table("t").unwrap()), vec![CHUNK_SIZE, 123]);

    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);
    // The CSV loader's chunk boundary (full chunk + remainder) is the same
    // after a Vortex round-trip.
    assert_eq!(sizes(loaded.table("t").unwrap()), vec![CHUNK_SIZE, 123]);
    cleanup(&dir);
}

#[test]
fn v3_round_trip_accepts_scan_chunks_above_the_old_cap() {
    // Regression (wave 45 v3 loader): the Vortex reader sub-splits stored
    // layout chunks into scan ranges of at most its ideal split size (100,000
    // rows in Vortex 0.88), so a ~100,000-row table is handed to the loader as
    // one scan chunk of ~100,000 rows — larger than the loader's old 65,536
    // cap, which rejected the lake it had just written as "corrupt". Any lake
    // saved by the v3 writer with such a split was unreadable.
    let dir = scratch("v3_split_cap");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, region VARCHAR, total DOUBLE, active BOOLEAN);")
        .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        // 100,000 rows in engine chunks (each vector is at most CHUNK_SIZE).
        const ROWS: usize = 100_000;
        let mut offset = 0usize;
        while offset < ROWS {
            let rows = (ROWS - offset).min(CHUNK_SIZE);
            let mut id = Vector::with_capacity(DataType::Integer, rows);
            let mut region = Vector::with_capacity(DataType::Varchar, rows);
            let mut total = Vector::with_capacity(DataType::Double, rows);
            let mut active = Vector::with_capacity(DataType::Boolean, rows);
            for i in 0..rows {
                let n = offset + i;
                id.push_value(Value::Integer(n as i32)).unwrap();
                region
                    .push_str(["west", "east", "north", "south"][n % 4])
                    .unwrap();
                if n.is_multiple_of(97) {
                    total.push_null().unwrap();
                } else {
                    total.push_value(Value::Double(n as f64 / 8.0)).unwrap();
                }
                active
                    .push_value(Value::Boolean(!n.is_multiple_of(3)))
                    .unwrap();
            }
            t.append_chunk(vec![id, region, total, active]).unwrap();
            offset += rows;
        }
    }
    let mut lake = Lake::create(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);
    // The scan chunk was sliced back into engine chunks densely, and the
    // 100,000 rows survived exactly.
    assert_eq!(loaded.table("t").unwrap().num_rows(), 100_000);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// `.lake attach`: footer-only descriptors + lazy materialization.

/// Query `db` and return the rows bit-exactly (NaN payloads and `-0.0`
/// compared by bits, like [`assert_same_store`]).
fn select_rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn assert_rows_bit_same(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
    assert_eq!(a.len(), b.len(), "{what}: row count");
    for (ra, rb) in a.iter().zip(b) {
        assert_eq!(ra.len(), rb.len(), "{what}: column count");
        for (va, vb) in ra.iter().zip(rb) {
            match (va, vb) {
                (Value::Double(x), Value::Double(y)) => {
                    assert_eq!(x.to_bits(), y.to_bits(), "{what}: {va:?} vs {vb:?}")
                }
                _ => assert_eq!(va, vb, "{what}"),
            }
        }
    }
}

/// The whole fidelity fixture attaches: descriptors equal the decoded truth,
/// and a battery of COUNT/COUNT(col)/MIN/MAX (plus shapes that must fall
/// back) answers bit-identically to the `.lake open` store.
#[test]
fn attach_of_the_fidelity_store_matches_the_loaded_store() {
    let dir = scratch("attach_fidelity");
    let lake_dir = dir.join("lake");
    let db = common::fidelity_store();
    save_store(&db, &lake_dir).unwrap();
    let lake = Lake::open(&lake_dir).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.format_version, FORMAT_VERSION);
    assert!(!report.remote);

    // Descriptor-vs-decoded: every provable statistic must hold, for every
    // table of the fixture.
    let loaded = lake.load(None).unwrap();
    let mut attached_db = PinkDb::new();
    let tables = attached_db.attach_lake(report.lake.clone()).unwrap();
    // The attached catalog is exactly the loaded store's table set.
    let mut attached_keys: Vec<(String, String)> = tables
        .iter()
        .map(|t| (t.db.to_ascii_lowercase(), t.table.to_ascii_lowercase()))
        .collect();
    attached_keys.sort();
    let mut loaded_keys: Vec<(String, String)> = Vec::new();
    for dbname in loaded.list_databases() {
        for name in loaded.list_tables_in(&dbname) {
            loaded_keys.push((dbname.to_ascii_lowercase(), name.to_ascii_lowercase()));
        }
    }
    loaded_keys.sort();
    assert_eq!(attached_keys, loaded_keys);
    for table in &tables {
        let decoded = loaded.table_in(&table.db, &table.table).unwrap();
        assert_eq!(table.rows, decoded.num_rows() as u64);
        assert_eq!(table.schema, decoded.schema());
        for (i, (name, _)) in table.schema.iter().enumerate() {
            let mut nulls = 0u64;
            let (mut min, mut max): (Option<Value>, Option<Value>) = (None, None);
            for row in 0..decoded.num_rows() {
                let v = decoded.get(row, i).unwrap();
                if v.is_null() {
                    nulls += 1;
                    continue;
                }
                if min
                    .as_ref()
                    .is_none_or(|m| pinkdb::plan::compare_values(&v, m).is_lt())
                {
                    min = Some(v.clone());
                }
                if max
                    .as_ref()
                    .is_none_or(|m| pinkdb::plan::compare_values(&v, m).is_gt())
                {
                    max = Some(v);
                }
            }
            assert_eq!(table.stats[i].null_count, Some(nulls), "{name}");
            if let Some(stat) = &table.stats[i].min {
                assert_eq!(Some(stat), min.as_ref(), "{name} min");
            }
            if let Some(stat) = &table.stats[i].max {
                assert_eq!(Some(stat), max.as_ref(), "{name} max");
            }
        }
    }

    // The query battery: attached store vs loaded store, bit-exact.
    let fid_schema = attached_db
        .attached_table_in("main", "fid")
        .unwrap()
        .schema
        .clone();
    let mut mut_attached = attached_db;
    let mut mut_loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    let mut queries = vec![
        "SELECT COUNT(*) FROM main.fid;".to_string(),
        "SELECT COUNT(*) FROM big;".to_string(),
        "SELECT COUNT(*) FROM empty;".to_string(),
        "SELECT SUM(i), AVG(i) FROM fid;".to_string(),
        "SELECT COUNT(*) FROM fid WHERE i > 0;".to_string(),
        "SELECT b, COUNT(*) FROM fid GROUP BY b ORDER BY b;".to_string(),
        "SELECT i, g FROM fid ORDER BY i LIMIT 4;".to_string(),
        "SELECT MAX(i) FROM (SELECT i FROM fid) s;".to_string(),
        "SELECT MIN(i) + 1, MAX(g) - 1 FROM fid;".to_string(),
        "SELECT fid.i, big.a FROM fid JOIN big ON fid.i = big.a ORDER BY fid.i LIMIT 3;"
            .to_string(),
        "SELECT COUNT(*) FROM analytics.\"a.b\";".to_string(),
        "SELECT z FROM analytics.\"a.b\";".to_string(),
    ];
    // Every column: COUNT(col) and MIN/MAX (bit-exact comparison decides
    // whether the carrier is a fast path or a fallback).
    for (col, _) in fid_schema {
        queries.push(format!("SELECT COUNT({col}) FROM fid;"));
        queries.push(format!("SELECT MIN({col}), MAX({col}) FROM fid;"));
    }
    for sql in queries {
        let a = select_rows(&mut mut_attached, &sql);
        let b = select_rows(&mut mut_loaded, &sql);
        assert_rows_bit_same(&a, &b, &sql);
    }
    cleanup(&dir);
}

/// Attach reads the requested version only, and stays pinned to it however
/// the catalog moves.
#[test]
fn attach_is_pinned_to_the_requested_version() {
    let dir = scratch("attach_pinned");
    let lake_dir = dir.join("lake");
    let lake = Lake::open_or_create(&lake_dir).unwrap();
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    let mut lake = {
        drop(lake);
        Lake::open(&lake_dir).unwrap()
    };
    lake.write_from_store(&db).unwrap(); // version 2: 2 rows
    db.execute("INSERT INTO t VALUES (3);").unwrap();
    lake.write_from_store(&db).unwrap(); // version 3: 3 rows

    let older = Lake::open(&lake_dir).unwrap().attach(Some(2)).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(older.lake).unwrap();
    assert_eq!(
        select_rows(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(2)]],
        "the attachment is pinned to version 2"
    );
    let latest = Lake::open(&lake_dir).unwrap().attach(None).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(latest.lake).unwrap();
    assert_eq!(
        select_rows(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(3)]]
    );
    cleanup(&dir);
}

/// Attach refusals: not a lake, a v1 lake, a missing data file, a row-count
/// mismatch, and a two-file table.
#[test]
fn attach_refusals_are_clean_errors() {
    // Not a lake.
    let dir = scratch("attach_refuse");
    let missing = dir.join("nope");
    let err = Lake::open(&missing).unwrap_err().to_string();
    assert!(err.contains("is not a pink-lake directory"), "{err}");

    // A v1 lake: no statistics to read.
    let fixture = v1_fixture_dir();
    let lake = Lake::open(&fixture).unwrap();
    let err = lake.attach(None).unwrap_err().to_string();
    assert!(err.contains("format_version 1"), "{err}");
    assert!(err.contains("use .lake open"), "{err}");

    // A missing data file.
    let lake_dir = dir.join("lake");
    let db = common::fidelity_store();
    save_store(&db, &lake_dir).unwrap();
    for path in data_files(&lake_dir) {
        std::fs::remove_file(&path).unwrap();
    }
    let lake = Lake::open(&lake_dir).unwrap();
    let err = lake.attach(None).unwrap_err().to_string();
    assert!(
        err.contains("is missing") || err.contains("loading data file"),
        "{err}"
    );

    // A row-count mismatch (the manifest lies about the file).
    let lake_dir = dir.join("wrong_rows");
    save_store(&db, &lake_dir).unwrap();
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    let key = catalog["tables"]
        .as_object()
        .unwrap()
        .keys()
        .next()
        .unwrap()
        .clone();
    let rows = catalog["tables"][&key]["rows"].as_u64().unwrap();
    catalog["tables"][&key]["rows"] = serde_json::json!(rows + 1);
    write_json(&lake_dir.join("catalog.json"), &catalog);
    let err = Lake::open(&lake_dir)
        .unwrap()
        .attach(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("but the catalog says"), "{err}");

    // A table with two data files.
    let lake_dir = dir.join("two_files");
    save_store(&db, &lake_dir).unwrap();
    let files = data_files(&lake_dir);
    let data_dir = files[0].parent().unwrap().to_path_buf();
    let second = data_dir.join("second.vortex");
    std::fs::copy(&files[0], &second).unwrap();
    let rel_first = files[0]
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let rel_second = second
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut catalog = read_json(&lake_dir.join("catalog.json"));
    let key = catalog["tables"]
        .as_object()
        .unwrap()
        .keys()
        .next()
        .unwrap()
        .clone();
    catalog["tables"][&key]["files"] = serde_json::json!([rel_first, rel_second]);
    write_json(&lake_dir.join("catalog.json"), &catalog);
    let err = Lake::open(&lake_dir)
        .unwrap()
        .attach(None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("data files"), "{err}");
    cleanup(&dir);
}
