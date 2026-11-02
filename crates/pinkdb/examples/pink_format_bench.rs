//! `.pink` format measurement harness (wave 102).
//!
//! Loads the TPC-H SF1 CSVs of a directory into one in-memory store, then
//! measures the engine's `.pink` persistence format on it: the serialized
//! size and the save (encode) and open (decode) times.
//!
//! ```text
//! PINKDB_TPCH_DIR=/tmp/w102 cargo run --release -p pinkdb --example pink_format_bench
//! ```
//!
//! The directory must hold the TPC-H CSVs (`lineitem.csv`, `orders.csv`, …);
//! every `*.csv` in it is imported, in name order, as one table. The harness
//! works against **any** `.pink` version (the v5 baseline and `PINKDB07` alike):
//! it only uses the public [`PinkDb::to_pink_bytes`] /
//! [`PinkDb::from_pink_bytes`] API, so the same binary measures both.
//!
//! Environment knobs:
//!
//! - `PINKDB_TPCH_DIR` — the CSV directory (required).
//! - `PINKDB_TPCH_TABLES` — a comma-separated table subset (default: all).
//! - `PINKDB_BENCH_REPS` — how many save/open repetitions (default 3).

use std::time::Instant;

use pinkdb::PinkDb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("PINKDB_TPCH_DIR").unwrap_or_else(|_| {
        eprintln!("PINKDB_TPCH_DIR is not set (the directory with the TPC-H CSVs)");
        std::process::exit(2);
    });
    let reps: usize = std::env::var("PINKDB_BENCH_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let only: Option<Vec<String>> = std::env::var("PINKDB_TPCH_TABLES").ok().map(|v| {
        v.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });

    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "csv"))
        .collect();
    files.sort();
    if let Some(only) = &only {
        files.retain(|p| {
            let stem = p
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            only.contains(&stem)
        });
    }
    if files.is_empty() {
        return Err(format!("no CSV files in {dir}").into());
    }

    let mut db = PinkDb::new();
    let mut imported = Vec::new();
    for path in &files {
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        let t0 = Instant::now();
        let rows = pinkdb::csv::import_csv(&mut db, path.to_str().unwrap(), Some(&stem))?;
        let secs = t0.elapsed().as_secs_f64();
        imported.push((stem.clone(), rows, secs));
        println!("import {stem:<12} {rows:>10} rows in {secs:>7.2}s");
    }

    let total_rows: usize = imported.iter().map(|(_, r, _)| r).sum();
    println!("\nstore: {} tables, {total_rows} rows", imported.len());

    // --- save (encode) ---
    let mut best_save = f64::MAX;
    let mut bytes = Vec::new();
    for _ in 0..reps {
        let t0 = Instant::now();
        bytes = db.to_pink_bytes()?;
        best_save = best_save.min(t0.elapsed().as_secs_f64());
    }
    // The per-table breakdown of the serialized image.
    println!("\n=== .pink image ===");
    println!(
        "total bytes: {} ({:.1} MiB)",
        bytes.len(),
        bytes.len() as f64 / 1048576.0
    );
    println!("save (encode): {best_save:.3} s");

    // --- open (decode) ---
    let mut best_open = f64::MAX;
    for _ in 0..reps {
        let t0 = Instant::now();
        let back = PinkDb::from_pink_bytes(&bytes)?;
        best_open = best_open.min(t0.elapsed().as_secs_f64());
        std::hint::black_box(back);
    }
    println!("open (decode): {best_open:.3} s");

    // --- a durable round trip (checkpoint + reopen) ---
    let path = std::env::temp_dir().join(format!("pinkdb_bench_{}.pink", std::process::id()));
    let path = path.to_string_lossy().to_string();
    let t0 = Instant::now();
    db.save(&path)?;
    let save_secs = t0.elapsed().as_secs_f64();
    let on_disk = std::fs::metadata(&path)?.len();
    let t0 = Instant::now();
    let back = PinkDb::open(&path)?;
    let open_secs = t0.elapsed().as_secs_f64();
    println!("\n=== durable ===");
    println!(
        "file bytes: {on_disk} ({:.1} MiB)",
        on_disk as f64 / 1048576.0
    );
    println!("save to disk: {save_secs:.3} s");
    println!("open from disk: {open_secs:.3} s");
    for (name, rows, _) in &imported {
        let t = back.table(name)?;
        if t.num_rows() != *rows {
            return Err(format!("{name}: {} rows, want {rows}", t.num_rows()).into());
        }
    }
    std::fs::remove_file(&path).ok();
    println!("\nround trip verified: {} tables", imported.len());
    Ok(())
}
