//! Lean (`vortex-format`-less) lake tests: the v1 `.pink` data-file format is
//! the only one this build reads and writes, so every lake here is a v1 lake.
//! The wave-45k statistics block is written by every save here and is what
//! `.lake attach` reads without opening a data file.
//!
//! `vortex-format` builds run `tests/lake.rs` instead; the two files pin the
//! same contract from opposite sides, and this one additionally opens the
//! checked-in `tests/fixtures/lake_v1_v4/` fixture (a v1 lake written by the
//! **lean writer**, read bit-exactly by the full build in `tests/lake.rs`)
//! and `tests/fixtures/lake_v1_v4_stats/` (the same store written with
//! statistics blocks: attached by both builds).

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

    // The on-disk shape is exactly the wave-34 v1 layout, plus the wave-45k
    // CRC-protected statistics block per entry.
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
        let stats = entry
            .get("stats")
            .unwrap_or_else(|| panic!("{key}: no stats"));
        assert_eq!(stats["table"], key.as_str(), "{key}");
        assert_eq!(stats["file"], file, "{key}");
        assert_eq!(stats["rows"], entry["rows"], "{key}");
        assert!(stats["crc32"].is_u64(), "{key}: no CRC");
    }
    // The single-writer invariant: `catalog.json` is byte-identical to the
    // snapshot of its version (statistics included).
    assert_eq!(
        std::fs::read(lake_dir.join("catalog.json")).unwrap(),
        std::fs::read(lake_dir.join("snapshots/2.json")).unwrap(),
        "catalog.json must equal snapshots/2.json byte for byte"
    );
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
/// files and byte-identical statistics blocks: the engine `.pink` encoder and
/// the `to_pink_bytes_with_stats` facts are deterministic. (File *names* are
/// fresh ids by design, and the catalogs' `updated` timestamps differ.)
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

    // The published statistics (file names differ; `table`/`file` inside the
    // block are name-bound, and the CRC covers them, so compare the facts).
    let stats_of = |lake: &Path| -> Vec<(String, serde_json::Value)> {
        let catalog = read_json(&lake.join("catalog.json"));
        let mut out: Vec<(String, serde_json::Value)> = catalog["tables"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, e)| {
                let mut s = e["stats"].clone();
                let obj = s.as_object_mut().unwrap();
                obj.remove("file");
                obj.remove("crc32");
                (k.clone(), s)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    };
    assert_eq!(
        stats_of(&a),
        stats_of(&b),
        "the same store publishes identical statistics"
    );

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

// ---------------------------------------------------------------------------
// `.lake attach` over the v1 statistics block (this file: the lean build; the
// same fixture is attached by the full build in `tests/lake.rs`).
// ---------------------------------------------------------------------------

/// Regenerate the checked-in `tests/fixtures/lake_v1_v4_stats/` fixture — a
/// v1 lake written by **this** lean writer, with the `.pink` v4 data files and
/// the CRC-protected statistics blocks `.lake attach` reads. It pins the
/// cross-build contract: the lean build attaches it bit-exactly and so does a
/// full (`vortex-format`) build (`tests/lake.rs`), which is the lean-writes /
/// full-attaches compatibility pin.
///
/// Run deliberately, only when the writer's stats-block shape is meant to
/// change (never as part of a normal test run):
///
/// ```text
/// cargo test -p pink-lake --test lake_lite -- --ignored generate_v1_v4_stats_fixture
/// ```
#[test]
#[ignore = "writes the checked-in tests/fixtures/lake_v1_v4_stats; run with --ignored"]
fn generate_v1_v4_stats_fixture() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1_v4_stats");
    std::fs::remove_dir_all(&fixture).ok();
    save_store(&common::fidelity_store(), &fixture).unwrap();
    let catalog = read_json(&fixture.join("catalog.json"));
    assert_eq!(catalog["format_version"], 1);
    let snapshot = read_json(&fixture.join("snapshots/2.json"));
    assert_eq!(catalog, snapshot, "catalog == snapshot byte invariant");
    for (key, entry) in catalog["tables"].as_object().unwrap() {
        assert!(entry["stats"].is_object(), "{key}: {entry}");
        assert!(entry["stats"]["crc32"].is_u64(), "{key}: {entry}");
    }
}

/// The `lake_v1_v4_stats` fixture: a v1 lake with statistics blocks loads
/// bit-exactly, attaches without touching a data file, answers the fast paths
/// exactly, and attaches with identical answers as a full build.
#[test]
fn v1_v4_stats_fixture_attaches_bit_exactly() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lake_v1_v4_stats");
    let dir = scratch("v1_v4_stats_fixture");
    let expected = common::fidelity_store();

    let lake = Lake::open(&fixture).unwrap();
    assert_eq!(lake.format_version(), 1);

    // The attached descriptors equal the decoded truth, and the query
    // battery answers bit-identically to the `.lake open` store.
    let report = lake.attach(None).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.format_version, 1);
    assert!(!report.remote, "v1 attach never reads a data file");
    let loaded = lake.load(None).unwrap();
    assert_same_store(&expected, &loaded);
    let mut attached = PinkDb::new();
    let tables = attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(tables.len(), all_tables(&expected).len());
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
            assert_eq!(table.stats[i].null_count, Some(nulls), "{name} nulls");
            if let Some(stat) = &table.stats[i].min {
                assert_eq!(Some(stat), min.as_ref(), "{name} min");
            }
            if let Some(stat) = &table.stats[i].max {
                assert_eq!(Some(stat), max.as_ref(), "{name} max");
            }
        }
    }
    let mut mut_loaded = lake.load(None).unwrap();
    let mut queries = vec![
        "SELECT COUNT(*) FROM main.fid;".to_string(),
        "SELECT COUNT(*) FROM big;".to_string(),
        "SELECT COUNT(*) FROM empty;".to_string(),
        "SELECT MIN(i), MAX(i), COUNT(v) FROM fid;".to_string(),
        "SELECT MIN(b), MAX(b), MIN(g), MAX(g) FROM fid;".to_string(),
        "SELECT MIN(d), MAX(d) FROM fid;".to_string(),
        "SELECT COUNT(x), MIN(x), MAX(y) FROM empty;".to_string(),
        "SELECT SUM(i), AVG(i) FROM fid;".to_string(),
        "SELECT COUNT(*) FROM big WHERE a > 0;".to_string(),
        "SELECT z FROM analytics.\"a.b\";".to_string(),
    ];
    for (col, _) in loaded.table_in("main", "fid").unwrap().schema() {
        queries.push(format!(
            "SELECT COUNT({col}), MIN({col}), MAX({col}) FROM fid;"
        ));
    }
    for sql in queries {
        let a = select_rows(&mut attached, &sql);
        let b = select_rows(&mut mut_loaded, &sql);
        assert_rows_bit_same(&a, &b, &sql);
    }

    // A pinned snapshot version reads the same statistics blocks (the
    // manifest is byte-identical to the catalog of its version).
    let pinned = Lake::open(&fixture).unwrap().attach(Some(2)).unwrap();
    assert_eq!(pinned.version, 2);
    let mut pinned_db = PinkDb::new();
    pinned_db.attach_lake(pinned.lake.clone()).unwrap();
    assert_rows_bit_same(
        &select_rows(
            &mut pinned_db,
            "SELECT COUNT(*), COUNT(s), MIN(a) FROM main.big;",
        ),
        &select_rows(
            &mut attached,
            "SELECT COUNT(*), COUNT(s), MIN(a) FROM main.big;",
        ),
        "pinned v1 attach",
    );

    // The fixture's own decodes: the fast paths above never decoded a table
    // except the fallbacks (SUM/AVG/filter/`d`'s NaN-gated MIN/MAX are all
    // gated to materialize), which cache.
    let (loads, _) = report.lake.decode_stats();
    assert!(loads >= 1, "the fallback queries must have materialized");
    cleanup(&dir);
}

/// Query `db` and return the rows bit-exactly (NaN payloads and `-0.0`
/// compared by bits).
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

/// An adversarial corpus for the statistics fast paths: NaN payloads, `-0.0`,
/// ±INF, extremes, an all-NULL column, an empty table, and a multi-chunk
/// table.
fn stats_corpus() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, z DOUBLE, inf DOUBLE, \
         v VARCHAR, b BOOLEAN, n INTEGER);",
    )
    .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        t.append_row(vec![
            Value::Integer(i32::MIN),
            Value::Bigint(i64::MIN),
            Value::Double(1.5),
            Value::Double(0.0),
            Value::Double(f64::NEG_INFINITY),
            Value::Varchar(String::new()),
            Value::Boolean(false),
            Value::Null,
        ])
        .unwrap();
        t.append_row(vec![
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MAX),
            Value::Double(f64::from_bits(0x7ff8_0000_cafe_0001)),
            Value::Double(-0.0),
            Value::Double(f64::INFINITY),
            Value::Varchar("héllo wörld 🌍\0".to_string()),
            Value::Boolean(true),
            Value::Null,
        ])
        .unwrap();
        t.append_row(vec![Value::Null; 8]).unwrap();
        // Multi-chunk: a second chunk with a NULL pattern.
        for i in 0..(pinkdb::CHUNK_SIZE + 100) {
            let null = i % 17 == 0;
            t.append_row(vec![
                if null {
                    Value::Null
                } else {
                    Value::Integer(i as i32 - 50)
                },
                Value::Bigint(-(i as i64) * 3),
                Value::Double((i as f64) / 4.0 - 25.0),
                if null {
                    Value::Null
                } else {
                    Value::Double(1.0 + i as f64)
                },
                Value::Double((i as f64) * 2.0),
                if null {
                    Value::Null
                } else {
                    Value::Varchar(format!("s{i:05}"))
                },
                Value::Boolean(i % 2 == 0),
                Value::Null,
            ])
            .unwrap();
        }
    }
    db.execute("CREATE TABLE empty (x INTEGER, y VARCHAR);")
        .unwrap();
    db
}

/// The lean attach fast paths decode nothing at all and answer exactly what
/// `.lake open` answers; every gate (`d`'s NaN, `z`'s ±0.0, the all-NULL
/// column, the empty table) either serves from statistics or falls back to
/// exactly one whole-table decode that then caches.
#[test]
fn lean_attach_fast_paths_decode_nothing_and_fallbacks_decode_once() {
    let dir = scratch("attach_counters");
    let lake_dir = dir.join("lake");
    let db = stats_corpus();
    save_store(&db, &lake_dir).unwrap();
    let lake = Lake::open(&lake_dir).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.format_version, 1);
    assert!(!report.remote, "a v1 attach reads no data file");
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    let mut opened = lake.load(None).unwrap();

    // Ordered: the statistics-only shapes first (zero decodes), then the
    // gated fallbacks (one decode, cached for everything after).
    let cases: &[(&str, u64)] = &[
        ("SELECT COUNT(*) FROM t;", 0),
        (
            "SELECT COUNT(i), COUNT(g), COUNT(d), COUNT(z), COUNT(inf), COUNT(v), \
             COUNT(b), COUNT(n) FROM t;",
            0,
        ),
        (
            "SELECT MIN(i), MAX(i), MIN(g), MAX(g), MIN(v), MAX(v), MIN(b), MAX(b) FROM t;",
            0,
        ),
        // ±INF is a normal value for the comparator: servable.
        ("SELECT MIN(inf), MAX(inf) FROM t;", 0),
        // An empty table and an all-NULL column answer without any statistic.
        ("SELECT COUNT(n), MIN(n), MAX(n) FROM t;", 0),
        (
            "SELECT COUNT(*), COUNT(x), COUNT(y), MIN(x), MAX(y) FROM empty;",
            0,
        ),
        // `d` holds a NaN and `z` a -0.0: those MIN/MAX cannot come from
        // statistics, so the whole query materializes once.
        ("SELECT MIN(d), MAX(d) FROM t;", 1),
        ("SELECT MIN(z), MAX(z) FROM t;", 1),
        ("SELECT SUM(i) FROM t;", 1),
        // Cached: the same answers, no second decode.
        ("SELECT MIN(i), MAX(i), COUNT(v) FROM t;", 1),
        ("SELECT COUNT(*) FROM t WHERE i > 0;", 1),
    ];
    for (sql, want_loads) in cases {
        let a = select_rows(&mut attached, sql);
        let b = select_rows(&mut opened, sql);
        assert_rows_bit_same(&a, &b, sql);
        assert_eq!(report.lake.decode_stats().0, *want_loads, "{sql}: decodes");
    }
    // Source-clean: the decoded store has a `.pink` v4 image that loads.
    let back = lake.load(None).unwrap();
    assert_same_store(&db, &back);
    cleanup(&dir);
}

/// Corrupted or absent statistics never produce a guess: the checksum
/// mismatch is refused, a stats-less v1 lake is refused with a clear pointer,
/// and `.lake open` (which decodes data files) stays available.
#[test]
fn lean_attach_refuses_corrupted_and_stats_less_lakes() {
    let dir = scratch("attach_refuse");
    let lake_dir = dir.join("lake");
    let db = stats_corpus();
    save_store(&db, &lake_dir).unwrap();

    // An edited statistic without a re-sealed CRC.
    let catalog_path = lake_dir.join("catalog.json");
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    let min = &catalog["tables"]["main.t"]["stats"]["columns"][0]["min"];
    assert!(min.is_string(), "{min}");
    catalog["tables"]["main.t"]["stats"]["columns"][0]["min"] = serde_json::json!("424242");
    std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
    let lake = Lake::open(&lake_dir).unwrap();
    let err = lake.attach(None).unwrap_err().to_string();
    assert!(err.contains("failed its checksum"), "{err}");
    assert!(err.contains("use .lake open"), "{err}");
    // `.lake open` never consults the statistics: the data is intact.
    assert_same_store(&db, &lake.load(None).unwrap());

    // The same lake with the block removed entirely: a clear stats-less
    // refusal (never an empty-statistics attach).
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    catalog["tables"]["main.t"]
        .as_object_mut()
        .unwrap()
        .remove("stats");
    std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
    let lake = Lake::open(&lake_dir).unwrap();
    let err = lake.attach(None).unwrap_err().to_string();
    assert!(err.contains("format_version 1"), "{err}");
    assert!(err.contains("without a statistics block"), "{err}");
    assert!(err.contains("use .lake open"), "{err}");
    assert_same_store(&db, &lake.load(None).unwrap());

    // The v1 fixtures written before statistics existed are refused the same
    // way (and still load bit-exactly).
    for name in ["lake_v1", "lake_v1_v4"] {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        let lake = Lake::open(&fixture).unwrap();
        let err = lake.attach(None).unwrap_err().to_string();
        assert!(err.contains("without a statistics block"), "{name}: {err}");
        assert!(err.contains("use .lake open"), "{name}: {err}");
    }
    cleanup(&dir);
}

/// A v1 lake over mock S3 attaches from its published statistics, still
/// reading no data file at attach; the lazy fallback downloads through
/// `local_copy` like `.lake open`.
#[cfg(feature = "s3")]
#[test]
fn lean_s3_attach_answers_from_statistics() {
    let mock = mock_s3::MockS3::start();
    let config = S3Config::new("lite-bucket")
        .with_prefix("lakes/attach")
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret");
    let db = stats_corpus();
    let expected_rows = db.table("t").unwrap().num_rows();
    let mut lake = Lake::create_s3(config.clone()).unwrap();
    lake.write_from_store(&db).unwrap();

    let reopened = Lake::open_s3(config).unwrap();
    let report = reopened.attach(None).unwrap();
    assert!(
        !report.remote,
        "a v1 attach reads no data file, so it is never a remote read"
    );
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        select_rows(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(expected_rows as i64)]]
    );
    assert_eq!(report.lake.decode_stats().0, 0, "fast paths must not load");
    // The fallback decodes the whole table once (via a whole-object download).
    let _ = select_rows(&mut attached, "SELECT SUM(i) FROM t;");
    assert_eq!(report.lake.decode_stats().0, 1);
    assert_eq!(
        report.lake.decode_stats().1 as usize,
        db.table("t").unwrap().num_cols()
    );
}

/// Time travel over the lean (v1 `.pink`) format: each version's data file
/// is decoded through its published statistics block, statistics-only reads
/// never open an old file, and the version boundaries are exactly the
/// saves' (wave 64).
#[test]
fn lean_time_travel_reads_old_versions_through_the_stats_blocks() {
    let dir = scratch("lean_time_travel");
    let lake_dir = dir.join("lake");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
        .unwrap();
    let mut lake = Lake::create(&lake_dir).unwrap();
    db.execute("INSERT INTO t VALUES (1, 'one');").unwrap();
    lake.write_from_store(&db).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.execute("INSERT INTO t VALUES (2, 'two');").unwrap();
    lake.write_from_store(&db).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.execute("DELETE FROM t WHERE x = 1;").unwrap();
    lake.write_from_store(&db).unwrap();

    let records = Lake::open(&lake_dir).unwrap().snapshots().unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!((records[3].tables, records[3].rows), (1, 1));
    // Change summaries come from the manifests' own counts, v1 included.
    let changes = records[3].changes.as_ref().unwrap();
    assert_eq!(changes.tables_rewritten, vec!["main.t"]);
    assert_eq!(changes.rows_removed.get("main.t"), Some(&1));

    let report = Lake::open(&lake_dir).unwrap().attach(None).unwrap();
    let lake = std::sync::Arc::clone(&report.lake);
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    // Statistics-only reads decode nothing — the current version and a
    // version target alike.
    assert_eq!(
        select_rows(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(1)]]
    );
    assert_eq!(
        select_rows(
            &mut attached,
            "SELECT COUNT(*), MIN(x), MAX(x) FROM t AT (VERSION => 3);"
        ),
        vec![vec![Value::Bigint(2), Value::Integer(1), Value::Integer(2)]]
    );
    assert_eq!(
        lake.decode_stats(),
        (0, 0),
        "v1 statistics blocks must answer AT without opening a data file"
    );
    // A scan of the old version opens exactly that version's file...
    assert_eq!(
        select_rows(
            &mut attached,
            "SELECT s FROM t AT (VERSION => 3) WHERE x = 2;"
        ),
        vec![vec![Value::Varchar("two".to_string())]]
    );
    assert_eq!(lake.decode_stats().0, 1);
    // ...v2's version file is a different decode, and the current version
    // decodes its own file once (then caches).
    assert_eq!(
        select_rows(&mut attached, "SELECT x FROM t AT (VERSION => 2);"),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        select_rows(&mut attached, "SELECT x FROM t;"),
        vec![vec![Value::Integer(2)]]
    );
    assert_eq!(lake.decode_stats().0, 3);
    assert_eq!(
        select_rows(&mut attached, "SELECT x FROM t AT (VERSION => 2);"),
        vec![vec![Value::Integer(1)]],
        "the version attachment caches"
    );
    assert_eq!(lake.decode_stats().0, 3);
    // v1 has no table at all, and unknown versions are clean errors.
    let err = attached
        .execute("SELECT * FROM t AT (VERSION => 1);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not exist at version 1"), "{err}");
    let err = attached
        .execute("SELECT * FROM t AT (VERSION => 9);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("no snapshot found at version 9"), "{err}");
    cleanup(&dir);
}
