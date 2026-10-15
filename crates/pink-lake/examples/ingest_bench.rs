//! Ingest/open timing for the lake path, best-of-N per phase:
//! CSV import → `.lake save` (Vortex encode + publish) → `.lake open`.
//!
//! ```sh
//! cargo run --release -p pink-lake --example ingest_bench -- --csv bench/sales_1m.csv --runs 5
//! ```
//!
//! The last run's lake is kept (path printed) so the data file bytes can be
//! compared across builds (`shasum`): byte-neutral optimizations must keep
//! them identical.

use std::time::{Duration, Instant};

use pink_lake::Lake;
use pinkdb::PinkDb;

fn main() {
    let mut csv = String::from("bench/sales_1m.csv");
    let mut runs = 5usize;
    // `--save-loop N`: import once, then save N times (a profiling target).
    let mut save_loop = 0usize;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--csv" => csv = args.next().expect("--csv PATH"),
            "--runs" => runs = args.next().expect("--runs N").parse().expect("number"),
            "--save-loop" => {
                save_loop = args.next().expect("--save-loop N").parse().expect("number")
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let scratch = std::env::temp_dir().join(format!("pink_ingest_bench_{}", std::process::id()));
    if save_loop > 0 {
        let mut db = PinkDb::new();
        pinkdb::csv::import_csv(&mut db, &csv, Some("sales")).unwrap();
        let mut best = Duration::MAX;
        for i in 0..save_loop {
            let dir = scratch.join(format!("loop{i}"));
            let t = Instant::now();
            Lake::create(&dir).unwrap().write_from_store(&db).unwrap();
            best = best.min(t.elapsed());
            std::fs::remove_dir_all(&dir).ok();
        }
        println!(
            "save x{save_loop} best {:>9.3} ms",
            best.as_secs_f64() * 1e3
        );
        return;
    }
    let (mut import, mut save, mut open) = (Duration::MAX, Duration::MAX, Duration::MAX);
    let mut last_lake = scratch.clone();
    for run in 0..runs {
        let mut db = PinkDb::new();
        let t = Instant::now();
        pinkdb::csv::import_csv(&mut db, &csv, Some("sales")).unwrap();
        import = import.min(t.elapsed());

        let dir = scratch.join(format!("lake{run}"));
        std::fs::remove_dir_all(&dir).ok();
        let t = Instant::now();
        let mut lake = Lake::create(&dir).unwrap();
        lake.write_from_store(&db).unwrap();
        save = save.min(t.elapsed());
        drop(db);

        let t = Instant::now();
        let lake = Lake::open(&dir).unwrap();
        let loaded = lake.load(None).unwrap();
        open = open.min(t.elapsed());
        drop(loaded);
        if run + 1 < runs {
            std::fs::remove_dir_all(&dir).ok();
        }
        last_lake = dir;
    }
    println!("ingest_bench: {csv}, runs={runs}");
    println!("import  {:>9.3} ms", import.as_secs_f64() * 1e3);
    println!("save    {:>9.3} ms", save.as_secs_f64() * 1e3);
    println!("open    {:>9.3} ms", open.as_secs_f64() * 1e3);
    println!("lake    {}", last_lake.display());
}
