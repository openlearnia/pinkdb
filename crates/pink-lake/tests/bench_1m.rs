//! Opt-in 1M-row measurement (not part of the default test run):
//! `bench/sales_1m.csv` imported into a store, saved as a v2/v3 (Vortex)
//! lake, and compared against the engine's own `.pink` bytes for the same
//! table.
//!
//! ```sh
//! cargo test --release -p pink-lake --features vortexlake --test bench_1m \
//!     -- --ignored --nocapture
//! ```
//!
//! This is a report, not a gate: it prints data-file bytes and save/load wall
//! time for both formats on the current machine. `vortexlake` builds only.

#![cfg(feature = "vortexlake")]

use std::path::PathBuf;
use std::time::Instant;

use pink_lake::Lake;
use pinkdb::PinkDb;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pink_lake_bench_{tag}_{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Every data file under `dir`, recursively; returns (count, bytes).
fn data_bytes(dir: &std::path::Path) -> (usize, u64) {
    let mut count = 0;
    let mut bytes = 0;
    if !dir.exists() {
        return (0, 0);
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            let (c, b) = data_bytes(&path);
            count += c;
            bytes += b;
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("pink") | Some("vortex")
        ) {
            count += 1;
            bytes += std::fs::metadata(&path).unwrap().len();
        }
    }
    (count, bytes)
}

#[test]
#[ignore = "1M-row measurement; run manually with --ignored --nocapture"]
fn measure_sales_1m_vortex_vs_pink() {
    let csv = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/sales_1m.csv");
    assert!(csv.is_file(), "missing {}", csv.display());

    let mut db = PinkDb::new();
    let t = Instant::now();
    let rows = pinkdb::csv::import_csv(&mut db, csv.to_str().unwrap(), None).unwrap();
    let import = t.elapsed();
    println!("import_csv: {rows} rows in {:.3}s", import.as_secs_f64());

    let dir = scratch("1m");
    let lake = dir.join("vortex_lake");
    let mut lake = Lake::create(&lake).unwrap();
    let t = Instant::now();
    let report = lake.write_from_store_report(&db).unwrap();
    let save_vortex = t.elapsed();
    let (files, vortex_bytes) = data_bytes(&dir.join("vortex_lake/data"));
    let t = Instant::now();
    let loaded = lake.load(None).unwrap();
    let load_vortex = t.elapsed();
    assert_eq!(loaded.table("sales_1m").unwrap().num_rows(), rows);

    // The engine's own `.pink` bytes for the same table (what a v1 lake's
    // single data file is).
    let pink_path = dir.join("sales_1m.pink");
    let t = Instant::now();
    db.save(pink_path.to_str().unwrap()).unwrap();
    let save_pink = t.elapsed();
    let pink_bytes = std::fs::metadata(&pink_path).unwrap().len();
    let t = Instant::now();
    let loaded_pink = PinkDb::open(pink_path.to_str().unwrap()).unwrap();
    let load_pink = t.elapsed();
    assert_eq!(loaded_pink.table("sales_1m").unwrap().num_rows(), rows);

    println!("--- 1M-row table: Vortex lake vs engine .pink ---");
    println!(
        "vortex: {files} data file(s), {vortex_bytes} bytes; save {:.3}s, load {:.3}s \
         (manifest+catalog {})",
        save_vortex.as_secs_f64(),
        load_vortex.as_secs_f64(),
        report.bytes_written
    );
    println!(
        "pink:   1 data file, {pink_bytes} bytes; save {:.3}s, load {:.3}s",
        save_pink.as_secs_f64(),
        load_pink.as_secs_f64()
    );
    println!(
        "vortex/pink size ratio: {:.3} ({} vs {} bytes)",
        vortex_bytes as f64 / pink_bytes as f64,
        vortex_bytes,
        pink_bytes
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The incremental-commit measurement (wave 63): a 1M-row lake, then a
/// writable attach with a handful of inserts, updates and a delete, published
/// as one incremental version. Reports the commit's bytes and asserts that
/// every previously published data file is byte- and mtime-identical — the
/// append rewrites **zero** bytes of the existing table.
#[test]
#[ignore = "1M-row measurement; run manually with --ignored --nocapture"]
fn measure_incremental_append_rewrites_zero_bytes() {
    use pink_lake::{CommitSettings, WritableAttachment};

    let csv = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/sales_1m.csv");
    assert!(csv.is_file(), "missing {}", csv.display());

    let mut db = PinkDb::new();
    let rows = pinkdb::csv::import_csv(&mut db, csv.to_str().unwrap(), None).unwrap();
    let dir = scratch("1m_append");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    let t = Instant::now();
    lake.write_from_store(&db).unwrap();
    let save = t.elapsed();
    let (files, bytes) = data_bytes(&lake_dir.join("data"));

    // Snapshot every published data file: bytes and mtime.
    let mut before: Vec<(PathBuf, Vec<u8>, std::time::SystemTime)> = Vec::new();
    {
        fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for e in entries {
                let p = e.unwrap().path();
                if p.is_dir() {
                    walk(&p, out);
                } else if matches!(
                    p.extension().and_then(|e| e.to_str()),
                    Some("pink") | Some("vortex")
                ) {
                    out.push(p);
                }
            }
        }
        let mut paths = Vec::new();
        walk(&lake_dir.join("data"), &mut paths);
        for p in paths {
            let bytes = std::fs::read(&p).unwrap();
            let mtime = std::fs::metadata(&p).unwrap().modified().unwrap();
            before.push((p, bytes, mtime));
        }
    }
    assert_eq!(before.len(), files);

    // Writable attach: materialize the pinned version (this decodes the 1M
    // rows once), then mutate the session's copy.
    let t = Instant::now();
    let mut wa = WritableAttachment::open(lake_dir.to_str().unwrap(), None).unwrap();
    let attach = t.elapsed();
    let t = Instant::now();
    db.execute(
        "INSERT INTO sales_1m VALUES (1000001, 'west', 42.5, true), \
         (1000002, 'east', 7.25, false), (1000003, 'north', NULL, true);",
    )
    .unwrap();
    db.execute("UPDATE sales_1m SET total = 0.01 WHERE id = 1;")
        .unwrap();
    db.execute("DELETE FROM sales_1m WHERE id = 2;").unwrap();
    let mutate = t.elapsed();

    let t = Instant::now();
    let report = wa.commit(&db, &CommitSettings::default()).unwrap();
    let commit = t.elapsed();
    assert!(report.committed);

    // The append rewrote nothing: every old data file is byte-identical and
    // untouched (mtime included).
    for (path, bytes, mtime) in &before {
        assert!(path.is_file(), "{} disappeared", path.display());
        assert_eq!(
            &std::fs::read(path).unwrap(),
            bytes,
            "{} was rewritten",
            path.display()
        );
        assert_eq!(
            std::fs::metadata(path).unwrap().modified().unwrap(),
            *mtime,
            "{} was touched",
            path.display()
        );
    }
    let (new_files, new_bytes) = data_bytes(&lake_dir.join("data"));
    let loaded = lake.load(None).unwrap();
    assert_eq!(loaded.table("sales_1m").unwrap().num_rows(), rows + 2);

    println!("--- 1M-row incremental commit (wave 63) ---");
    println!(
        "base:  {rows} rows, {files} data file(s), {bytes} bytes; full save {:.3}s",
        save.as_secs_f64()
    );
    println!(
        "attach: materialize {rows} rows in {:.3}s (adopted by the store)",
        attach.as_secs_f64()
    );
    println!(
        "mutate: 3 inserts + 1 update + 1 delete in {:.3}s",
        mutate.as_secs_f64()
    );
    println!(
        "commit: version {} in {:.3}s — {} data file(s) + {} delete file(s), \
         {} bytes written, +{} row(s), -{} row(s)",
        report.version,
        commit.as_secs_f64(),
        report.data_files,
        report.delete_files,
        report.bytes_written,
        report.rows_added,
        report.rows_deleted
    );
    println!(
        "rewritten: 0 bytes ({} published data file(s), now {new_files} file(s), \
         {new_bytes} bytes total)",
        before.len()
    );
    let ratio = report.bytes_written as f64 / bytes as f64;
    println!(
        "commit bytes / base bytes: {ratio:.6} ({:.1} KB for {rows} rows)",
        report.bytes_written as f64 / 1024.0
    );
    std::fs::remove_dir_all(&dir).ok();
}
