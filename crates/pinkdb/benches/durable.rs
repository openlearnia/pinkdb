//! Single-process durable-path micro-benchmark (wave 45m, design §8.3):
//! 100k autocommit inserts (`SyncPolicy::Sync`), 100k point selects, and 20
//! checkpoints, all through the public `PinkDb` API on one process.
//!
//! Plain `Instant` timing with `harness = false` (no bench dependencies):
//! `cargo bench -p pinkdb --bench durable`. Every phase prints one
//! `phase=milliseconds` line; the measurement is the median of the runs the
//! caller performs (run it a few times and compare medians).

use std::time::Instant;

use pinkdb::{PinkDb, SyncPolicy};

const N: i64 = 100_000;
const CHECKPOINTS: usize = 20;

fn main() {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_bench_durable_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create bench scratch dir");
    let path = dir.join("bench.pink");
    let path = path.to_str().unwrap().to_string();

    let (mut db, _) = PinkDb::open_durable_with(&path, SyncPolicy::Sync).expect("open durable");
    db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
        .expect("create table");

    // Phase 1: 100k autocommit inserts, each a fsynced WAL record.
    let t0 = Instant::now();
    for i in 0..N {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'value-{i}');"))
            .expect("insert");
    }
    let inserts = t0.elapsed();

    // Phase 2: 100k point selects (served from memory; the durable path pays
    // only the freshness precheck).
    let t0 = Instant::now();
    let mut sum = 0i64;
    for i in 0..N {
        let out = db
            .execute(&format!("SELECT a FROM t WHERE a = {i};"))
            .expect("select");
        if let pinkdb::QueryResult::Selected { rows, .. } = &out[0] {
            sum += rows.len() as i64;
        }
    }
    let selects = t0.elapsed();
    assert_eq!(sum, N, "every point select must find its row");

    // Phase 3: 20 checkpoints of the full state.
    let t0 = Instant::now();
    for _ in 0..CHECKPOINTS {
        db.checkpoint().expect("checkpoint");
    }
    let checkpoints = t0.elapsed();

    println!("inserts_100k_ms={:.1}", inserts.as_secs_f64() * 1e3);
    println!("selects_100k_ms={:.1}", selects.as_secs_f64() * 1e3);
    println!("checkpoints_20_ms={:.1}", checkpoints.as_secs_f64() * 1e3);

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
