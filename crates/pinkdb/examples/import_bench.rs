//! Timing harness for CSV import. Usage:
//!   cargo run --release -p pinkdb --example import_bench [file.csv] [runs]
//!
//! Reports best-of-N wall time and throughput (MB/s).

use std::time::Instant;

use pinkdb::PinkDb;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap_or_else(|| {
        eprintln!("usage: import_bench <file.csv> [runs]");
        std::process::exit(2);
    });
    let runs: usize = args.next().and_then(|r| r.parse().ok()).unwrap_or(5);
    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let mut best = f64::MAX;
    for run in 0..runs {
        let mut db = PinkDb::new();
        let t0 = Instant::now();
        let rows = pinkdb::csv::import_csv(&mut db, &path, Some("sales")).unwrap();
        let dt = t0.elapsed().as_secs_f64();
        best = best.min(dt);
        let mbps = (bytes as f64 / 1e6) / dt;
        println!("run {run}: {rows} rows in {dt:.3}s ({mbps:.0} MB/s)");
    }
    println!("best: {best:.3}s");
}
