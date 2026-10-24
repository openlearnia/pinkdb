//! Lean (`vortexlake`-less) lake tests: `.pink` (PINKDB06) is the data-file
//! format this build reads and writes. The statistics block is written by
//! every save here and is what `.lake attach` reads without opening a data
//! file; queries that need data read only the projected column chunks.
//!
//! `vortexlake` builds run `tests/lake.rs` instead; the two files pin the
//! same contract from opposite sides.

#![cfg(not(feature = "vortexlake"))]
#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::FORMAT_VERSION;
use pinkdb::{PinkDb, Value};

#[cfg(feature = "s3")]
use pink_lake::S3Config;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_pinklake_{tag}_{}_{}",
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

fn catalog_sql(lake_dir: &Path, sql: &str) {
    let path = lake_dir.join("catalog.pink");
    let mut db = PinkDb::open_durable(&path.display().to_string()).unwrap();
    db.execute(sql).unwrap();
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

/// The lean writer publishes a v6 lake: `format_version` 6 catalogs and
/// snapshots, entries with `"format": "pink"` and object-shaped `files` with
/// per-file row counts and CRC-protected statistics blocks, plus the
/// per-commit delta manifest — then loads it back bit-exactly.
#[test]
fn lean_save_publishes_a_v5_lake_and_reloads_bit_exactly() {
    assert_eq!(FORMAT_VERSION, 7, "a lean build writes v7");
    let dir = scratch("roundtrip");
    let lake_dir = dir.join("lake");
    let expected = common::fidelity_store();
    let mut lake = create_lake(&lake_dir).unwrap();
    assert_eq!(lake.format_version(), 7, "create publishes a v7 lake");
    let report = lake.write_from_store_report(&expected).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.tables, all_tables(&expected).len());
    assert_eq!(report.rows, total_rows(&expected));

    // The on-disk shape: a v4 entry per table, `.pink` files as objects with
    // their row counts and statistics blocks.
    let catalog = catalog_json(&lake_dir);
    assert_eq!(catalog["format_version"], 7, "{catalog}");
    assert_eq!(catalog["version"], 2, "{catalog}");
    for (key, entry) in catalog["tables"].as_object().unwrap() {
        assert_eq!(entry["format"], "pink", "{key}: {entry}");
        assert!(
            entry.get("stats").is_none(),
            "{key}: entry-level stats are v1"
        );
        let file = &entry["files"][0];
        let path = file["path"].as_str().unwrap();
        assert!(path.ends_with(".pink"), "{key}: {path}");
        assert_eq!(file["rows"], entry["rows"], "{key}");
        let stats = &file["stats"];
        assert_eq!(stats["table"], key.as_str(), "{key}");
        assert_eq!(stats["file"], path, "{key}");
        assert_eq!(stats["rows"], entry["rows"], "{key}");
        assert!(stats["crc32"].is_u64(), "{key}: no CRC");
    }
    let files = data_files(&lake_dir.join("data"));
    assert_eq!(files.len(), all_tables(&expected).len());
    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        // The engine's current `.pink` format: `PINKDB06`, whose magic lives
        // in the trailer (the last 20 bytes).
        assert!(
            pinkdb::persist::is_pinkdb06(&bytes),
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
    assert_eq!(lake.commits().unwrap(), vec![2, 3], "one delta per save");
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
    save_store_lake(&db, &a).unwrap();
    save_store_lake(&db, &b).unwrap();

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
        let catalog = catalog_json(lake);
        let mut out: Vec<(String, serde_json::Value)> = catalog["tables"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, e)| {
                let mut s = e["files"][0]["stats"].clone();
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
    let mut lake = open_lake(&a).unwrap();
    lake.write_from_store(&db).unwrap();
    let catalog = catalog_json(&a);
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

/// Wave 88 rule 5: a lake with a Vortex entry **opens** in a lean build; that
/// table is unavailable (every read/write of it is the feature error) and the
/// rest of the lake still works.
#[test]
fn a_vortex_entry_is_a_per_table_feature_error() {
    let dir = scratch("vortex_entry");
    let lake_dir = dir.join("lake");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();
    save_store_lake(&db, &lake_dir).unwrap();
    pink_lake::catalog_set_table_format(&catalog_url(&lake_dir), "main.t", "vortex").unwrap();
    let lake = open_lake(&lake_dir).unwrap();
    let unavailable = lake.unavailable_tables().unwrap();
    assert_eq!(unavailable.len(), 1, "{unavailable:?}");
    assert_eq!(unavailable[0].table, "main.t");
    assert!(
        unavailable[0].message().contains("vortex"),
        "{unavailable:?}"
    );
    cleanup(&dir);
}

#[cfg(feature = "s3")]
#[test]
fn lean_s3_v4_roundtrip() {
    let mock = mock_s3::MockS3::start();
    let config = S3Config::new("lite-bucket")
        .with_prefix("lakes/lite")
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret");
    let expected = common::fidelity_store();
    let mut lake = create_s3_lake(config).unwrap();
    assert_eq!(lake.format_version(), 7);
    lake.write_from_store(&expected).unwrap();

    let keys = mock.keys();
    assert!(
        keys.iter().any(|k| k.ends_with(".pink")),
        "lean data files are `.pink` objects: {keys:?}"
    );
    assert!(
        !keys.iter().any(|k| k.ends_with(".vortex")),
        "a lean build writes no Vortex objects: {keys:?}"
    );

    lake.load(None).unwrap();
    let reopened = open_s3_lake(
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
    save_store_lake(&db, &lake_dir).unwrap();
    let lake = open_lake(&lake_dir).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.format_version, 7);
    assert!(!report.remote, "a `.pink` attach reads no data file");
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
        // statistics. A `.pink` file decodes only the columns a query needs
        // (wave 102), so each new column is one projected load.
        ("SELECT MIN(d), MAX(d) FROM t;", 1),
        ("SELECT MIN(z), MAX(z) FROM t;", 2),
        ("SELECT SUM(i) FROM t;", 3),
        // Cached: the same column again, no further decode.
        ("SELECT MIN(i), MAX(i), COUNT(v) FROM t;", 3),
        // A filtered scan prunes row groups and is never cached: one load.
        ("SELECT COUNT(*) FROM t WHERE i > 0;", 4),
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
    let mut lake = create_s3_lake(config.clone()).unwrap();
    lake.write_from_store(&db).unwrap();

    let reopened = open_s3_lake(config).unwrap();
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
    // The fallback decodes only the needed column (wave 102: projected,
    // ranged `.pink` reads), not the whole table.
    let _ = select_rows(&mut attached, "SELECT SUM(i) FROM t;");
    assert_eq!(report.lake.decode_stats().0, 1);
    assert_eq!(report.lake.decode_stats().1, 1, "only column i is decoded");
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
    let mut lake = create_lake(&lake_dir).unwrap();
    db.execute("INSERT INTO t VALUES (1, 'one');").unwrap();
    lake.write_from_store(&db).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.execute("INSERT INTO t VALUES (2, 'two');").unwrap();
    lake.write_from_store(&db).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.execute("DELETE FROM t WHERE x = 1;").unwrap();
    lake.write_from_store(&db).unwrap();

    let records = open_lake(&lake_dir).unwrap().snapshots().unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!((records[3].tables, records[3].rows), (1, 1));
    // Change summaries are derived from the typed manifests, v1 included.
    let changes = records[3].changes.as_ref().unwrap();
    assert_eq!(changes.tables_rewritten, vec!["main.t"]);
    assert_eq!(changes.rows_removed.get("main.t"), Some(&1));

    let report = open_lake(&lake_dir).unwrap().attach(None).unwrap();
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
        (0, 0, 0),
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

#[test]
fn lean_attach_refuses_corrupted_and_stats_less_lakes() {
    let dir = scratch("attach_refuse");
    let lake_dir = dir.join("lake");
    let db = stats_corpus();
    save_store_lake(&db, &lake_dir).unwrap();
    let clean = open_lake(&lake_dir).unwrap();
    assert_same_store(&db, &clean.load(None).unwrap());
    drop(clean);

    // An undecodable statistic is corrupt rows: attach refuses with the
    // column named, and `.lake open` (which never consults statistics) still
    // reads the intact data.
    catalog_sql(
        &lake_dir,
        "UPDATE ducklake_file_column_stats SET min_value = 'not-an-int' \
         WHERE column_id = (SELECT MIN(column_id) FROM ducklake_column);",
    );
    let lake = open_lake(&lake_dir).unwrap();
    let err = lake.attach(None).unwrap_err().to_string();
    assert!(err.contains("does not decode"), "{err}");

    // Remove every statistics row: a clear stats-less refusal (never an
    // empty-statistics attach).
    catalog_sql(&lake_dir, "DELETE FROM ducklake_file_column_stats;");
    let lake = open_lake(&lake_dir).unwrap();
    let err = lake.attach(None).unwrap_err().to_string();
    assert!(err.contains("without a statistics block"), "{err}");
    assert_same_store(&db, &lake.load(None).unwrap());

    cleanup(&dir);
}
