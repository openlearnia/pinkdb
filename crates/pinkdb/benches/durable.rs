//! Single-process durable-path micro-benchmark (wave 45m, design §8.3):
//! 100k autocommit inserts (`SyncPolicy::Sync`), 100k point selects, and 20
//! checkpoints, all through the public `PinkDb` API on one process.
//!
//! Plain `Instant` timing with `harness = false` (no bench dependencies):
//! `cargo bench -p pinkdb --bench durable`. Every phase prints one
//! `phase=milliseconds` line; the measurement is the median of the runs the
//! caller performs (run it a few times and compare medians).
//!
//! `PINKDB_BENCH_N` (default 100_000) and `PINKDB_BENCH_CHECKPOINTS`
//! (default 20) bound the run for a quick on-machine comparison; with the
//! defaults the printed labels are unchanged.

use std::time::Instant;

use pinkdb::{PinkDb, SyncPolicy};

const N: i64 = 100_000;
const CHECKPOINTS: usize = 20;

fn env_bound(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

fn main() {
    let n = env_bound("PINKDB_BENCH_N", N as usize) as i64;
    let n_ckpts = env_bound("PINKDB_BENCH_CHECKPOINTS", CHECKPOINTS);
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

    // Phase 1: `n` autocommit inserts, each a fsynced WAL record.
    let t0 = Instant::now();
    for i in 0..n {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'value-{i}');"))
            .expect("insert");
    }
    let inserts = t0.elapsed();

    // Phase 2: `n` point selects (served from memory; the durable path pays
    // only the freshness precheck).
    let t0 = Instant::now();
    let mut sum = 0i64;
    for i in 0..n {
        let out = db
            .execute(&format!("SELECT a FROM t WHERE a = {i};"))
            .expect("select");
        if let pinkdb::QueryResult::Selected { rows, .. } = &out[0] {
            sum += rows.len() as i64;
        }
    }
    let selects = t0.elapsed();
    assert_eq!(sum, n, "every point select must find its row");

    // Phase 3: `n_ckpts` checkpoints of the full state.
    let t0 = Instant::now();
    for _ in 0..n_ckpts {
        db.checkpoint().expect("checkpoint");
    }
    let checkpoints = t0.elapsed();

    if n == N && n_ckpts == CHECKPOINTS {
        println!("inserts_100k_ms={:.1}", inserts.as_secs_f64() * 1e3);
        println!("selects_100k_ms={:.1}", selects.as_secs_f64() * 1e3);
        println!("checkpoints_20_ms={:.1}", checkpoints.as_secs_f64() * 1e3);
    } else {
        println!("n={n} checkpoints={n_ckpts}");
        println!("inserts_ms={:.1}", inserts.as_secs_f64() * 1e3);
        println!("selects_ms={:.1}", selects.as_secs_f64() * 1e3);
        println!("checkpoints_ms={:.1}", checkpoints.as_secs_f64() * 1e3);
    }

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
