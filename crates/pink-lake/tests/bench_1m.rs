//! Opt-in 1M-row measurement (not part of the default test run):
//! `bench/sales_1m.csv` imported into a store, saved as a v2/v3 (Vortex)
//! lake, and compared against the engine's own `.pink` bytes for the same
//! table.
//!
//! ```sh
//! cargo test --release -p pink-lake --features vortex-format --test bench_1m \
//!     -- --ignored --nocapture
//! ```
//!
//! This is a report, not a gate: it prints data-file bytes and save/load wall
//! time for both formats on the current machine. `vortex-format` builds only.

#![cfg(feature = "vortex-format")]

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
