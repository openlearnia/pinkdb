//! Lean (`vortex-format`-less) lake tests: the v1 `.pink` data-file format is
//! the only one this build reads and writes, so every lake here is a v1 lake.
//!
//! `vortex-format` builds run `tests/lake.rs` instead; the two files pin the
//! same contract from opposite sides, and this one additionally opens the
//! checked-in `tests/fixtures/lake_v1_v4/` fixture — a v1 lake written by the
//! **lean writer** and read bit-exactly by the full build in `tests/lake.rs`.

#![cfg(not(feature = "vortex-format"))]

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{save_store, Lake, LakeError, FORMAT_VERSION};
use pinkdb::{PinkDb, Value};

#[cfg(feature = "s3")]
use pink_lake::S3Config;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_lite_{tag}_{}_{}",
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

/// Every data file under `dir`, recursively, sorted.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(data_files(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// The engine's deterministic `.pink` bytes of `db`.
fn store_bytes(db: &PinkDb, scratch: &Path, tag: &str) -> Vec<u8> {
    let path = scratch.join(format!("{tag}.pink"));
    db.save(path.to_str().unwrap()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).ok();
    bytes
}

/// The store's `(db, table)` pairs across every database (`list_tables()` is
/// current-database only).
fn all_tables(db: &PinkDb) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for dbname in db.list_databases() {
        for table in db.list_tables_in(&dbname) {
            out.push((dbname.clone(), table));
        }
    }
    out
}

/// Total rows across every table of every database.
fn total_rows(db: &PinkDb) -> u64 {
    all_tables(db)
        .into_iter()
        .map(|(dbname, table)| db.table_in(&dbname, &table).unwrap().num_rows() as u64)
        .sum()
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

/// The lean writer publishes a v1 lake: `format_version` 1 catalogs and
/// snapshots, entries with no `format` field, and `data/…/<uuid>.pink` data
/// files holding the engine's own bytes — then loads it back bit-exactly.
#[test]
fn lean_save_publishes_a_v1_lake_and_reloads_bit_exactly() {
    assert_eq!(FORMAT_VERSION, 1, "a lean build writes v1");
    let dir = scratch("roundtrip");
    let lake_dir = dir.join("lake");
    let expected = common::fidelity_store();
    let mut lake = Lake::create(&lake_dir).unwrap();
    assert_eq!(lake.format_version(), 1, "create publishes a v1 lake");
    let report = lake.write_from_store_report(&expected).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.tables, all_tables(&expected).len());
    assert_eq!(report.rows, total_rows(&expected));

    // The on-disk shape is exactly the wave-34 v1 layout.
    let catalog = read_json(&lake_dir.join("catalog.json"));
    assert_eq!(catalog["format_version"], 1, "{catalog}");
    assert_eq!(catalog["version"], 2, "{catalog}");
    for (key, entry) in catalog["tables"].as_object().unwrap() {
        assert!(
            entry.get("format").is_none(),
            "{key} must not declare a format: {entry}"
        );
        let file = entry["files"][0].as_str().unwrap();
        assert!(file.ends_with(".pink"), "{key}: {file}");
    }
    let files = data_files(&lake_dir.join("data"));
    assert_eq!(files.len(), all_tables(&expected).len());
    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        // The engine's current `.pink` magic: v4 since wave 45i's base commit.
        assert!(
            bytes.starts_with(b"PINKDB0"),
            "{} is not engine `.pink` bytes",
            file.display()
        );
        assert_eq!(file.extension().unwrap(), "pink");
    }

    // Values are bit-exact, the loaded store re-saves byte-identically, and
    // nested names survived the percent-encoded data directories.
    let loaded = lake.load(None).unwrap();
    assert_same_store(&expected, &loaded);
    assert_eq!(
        store_bytes(&loaded, &dir, "loaded"),
        store_bytes(&expected, &dir, "expected"),
        "the loaded store re-saves byte-identically"
    );
    assert_eq!(
        loaded.table("héllo✓").unwrap().get(0, 0),
        Some(Value::Varchar("π≈3.14".into()))
    );
    assert_eq!(
        loaded
            .table_in("analytics", "a.b")
            .unwrap()
            .get(1, 0)
            .unwrap(),
        Value::Double(-0.0)
    );

    // Version 1 (create-time empty) still loads; a second save bumps to 3.
    assert!(lake.load(Some(1)).unwrap().list_tables().is_empty());
    assert_eq!(lake.write_from_store(&expected).unwrap(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    assert_eq!(lake.tables().unwrap().len(), all_tables(&expected).len());
    assert!(lake.missing_data_files().unwrap().is_empty());
    cleanup(&dir);
}

/// Saving the same store twice (two fresh lakes) produces byte-identical data
/// files: the engine `.pink` encoder is deterministic, and the v1 data file
/// is just that image. (File *names* are fresh ids by design, so only the
/// contents are compared; the catalogs' `updated` timestamps differ too.)
#[test]
fn lean_save_is_deterministic_for_the_same_store() {
    let db = common::fidelity_store();
    let dir = scratch("determinism");
    let a = dir.join("a");
    let b = dir.join("b");
    save_store(&db, &a).unwrap();
    save_store(&db, &b).unwrap();

    let bytes_of = |lake: &Path| -> Vec<Vec<u8>> {
        data_files(&lake.join("data"))
            .iter()
            .map(|p| std::fs::read(p).unwrap())
            .collect()
    };
    let (fa, fb) = (bytes_of(&a), bytes_of(&b));
    assert_eq!(fa.len(), fb.len());
    assert_eq!(fa, fb, "the same store saves to identical data-file bytes");

    // And the same holds across two versions of one lake.
    let mut lake = Lake::open(&a).unwrap();
    lake.write_from_store(&db).unwrap();
    let catalog = read_json(&a.join("catalog.json"));
    assert_eq!(catalog["version"], 3);
    // Only the newest version's data files (version 2's files are untouched).
    let mut all = bytes_of(&a);
    all.sort();
    let mut first = fa.clone();
    first.sort();
    assert!(
        first.iter().all(|f| all.contains(f)),
        "a re-save keeps version 2's bytes intact"
    );
    cleanup(&dir);
}

/// A v2/v3 Vortex lake is refused at the very first manifest parse with a
/// clear feature error — never a corrupt-file error.
#[test]
fn lean_build_refuses_vortex_lakes_with_a_feature_error() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v2");
    let err = Lake::open(&fixture).unwrap_err();
    assert!(matches!(err, LakeError::FeatureMissing { .. }), "{err:?}");
    let text = err.to_string();
    assert!(
        text.contains("built without Vortex format support"),
        "{text}"
    );
    assert!(text.contains("--features lake"), "{text}");
    assert!(
        !text.contains("corrupt"),
        "must not read as corrupt metadata: {text}"
    );

    // The same holds for a v3 (current full-build) manifest and a snapshot at
    // a fetched version, not just the catalog.
    let dir = scratch("refuse_v3");
    let lake = dir.join("lake");
    std::fs::create_dir_all(lake.join("snapshots")).unwrap();
    let manifest = serde_json::json!({
        "format_version": 3,
        "updated": "2026-10-15T00:00:00.000Z",
        "version": 1,
        "tables": {},
    });
    let bytes = serde_json::to_vec(&manifest).unwrap();
    std::fs::write(lake.join("catalog.json"), &bytes).unwrap();
    std::fs::write(lake.join("snapshots/1.json"), &bytes).unwrap();
    let err = Lake::open(&lake).unwrap_err();
    assert!(matches!(err, LakeError::FeatureMissing { .. }), "{err:?}");
    assert!(err.to_string().contains("format_version 3"), "{err}");
    cleanup(&dir);
}

/// A `format_version` this codebase does not know at all (4+) is the same
/// [`LakeError::Unsupported`] as in a full build, not a feature error.
#[test]
fn unsupported_future_format_versions_are_named() {
    let dir = scratch("future");
    let lake = dir.join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    let manifest = serde_json::json!({
        "format_version": 4,
        "updated": "2026-10-15T00:00:00.000Z",
        "version": 1,
        "tables": {},
    });
    std::fs::write(
        lake.join("catalog.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let err = Lake::open(&lake).unwrap_err();
    assert!(
        matches!(err, LakeError::Unsupported { found: 4, .. }),
        "{err:?}"
    );
    assert!(
        err.to_string()
            .contains("unsupported lake format_version 4"),
        "{err}"
    );
    cleanup(&dir);
}

/// The wave-34 v1 fixture (engine v2 `.pink` bytes) still loads bit-exactly
/// in the lean build: the lean reader *is* the v1 reader.
#[test]
fn v1_fixture_written_by_wave34_still_loads_bit_exactly() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1");
    let scratch = scratch("v1_fixture");
    let catalog = read_json(&fixture.join("catalog.json"));
    assert_eq!(catalog["format_version"], 1, "fixture must stay v1");
    let lake = Lake::open(&fixture).unwrap();
    assert_eq!(lake.format_version(), 1);
    assert_eq!(lake.current_version(), 2);
    let loaded = lake.load(None).unwrap();
    assert_same_store(&common::fidelity_store(), &loaded);
    assert_eq!(
        store_bytes(&loaded, &scratch, "loaded"),
        store_bytes(&common::fidelity_store(), &scratch, "expected"),
    );
    cleanup(&scratch);
}

/// `tests/fixtures/lake_v1_v4/` is a v1 lake written by **this** (lean)
/// writer: engine v4 `.pink` data files under a v1 catalog. The lean build
/// reads it bit-exactly, and `tests/lake.rs` (the full build) opens the same
/// directory — that is the lean-writes / full-reads compatibility pin, and
/// the byte-identity of the re-save proves no value or chunking was lost.
#[test]
fn v1_v4_fixture_written_by_the_lean_writer_loads_bit_exactly() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1_v4");
    let scratch = scratch("v1_v4_fixture");

    let catalog = read_json(&fixture.join("catalog.json"));
    assert_eq!(catalog["format_version"], 1, "fixture must stay v1");
    for entry in catalog["tables"].as_object().unwrap().values() {
        assert!(entry.get("format").is_none(), "{entry}");
        assert!(entry["files"][0].as_str().unwrap().ends_with(".pink"));
    }
    for file in data_files(&fixture.join("data")) {
        assert!(
            std::fs::read(&file).unwrap().starts_with(b"PINKDB04"),
            "{} must be engine v4 bytes",
            file.display()
        );
    }

    let lake = Lake::open(&fixture).unwrap();
    assert_eq!(lake.format_version(), 1);
    let expected = common::fidelity_store();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&expected, &loaded);
    assert_eq!(
        store_bytes(&loaded, &scratch, "loaded"),
        store_bytes(&expected, &scratch, "expected"),
    );
    cleanup(&scratch);
}

/// A v1 lake over the mock S3 backend: the lean writer puts one `.pink`
/// object per table with the same catalog/snapshot contract, and reads it
/// back bit-exactly.
#[cfg(feature = "s3")]
#[test]
fn lean_s3_v1_roundtrip() {
    let mock = mock_s3::MockS3::start();
    let config = S3Config::new("lite-bucket")
        .with_prefix("lakes/lite")
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret");
    let expected = common::fidelity_store();
    let mut lake = Lake::create_s3(config).unwrap();
    assert_eq!(lake.format_version(), 1);
    lake.write_from_store(&expected).unwrap();

    let keys = mock.keys();
    assert!(
        keys.iter().any(|k| k.ends_with(".pink")),
        "v1 data files are `.pink` objects: {keys:?}"
    );
    assert!(
        !keys.iter().any(|k| k.ends_with(".vortex")),
        "a lean build writes no Vortex objects: {keys:?}"
    );

    lake.load(None).unwrap();
    let reopened = Lake::open_s3(
        S3Config::new("lite-bucket")
            .with_prefix("lakes/lite")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .with_credentials(mock.access_key_id(), "mock-secret"),
    )
    .unwrap();
    let loaded = reopened.load(None).unwrap();
    assert_same_store(&expected, &loaded);
}
