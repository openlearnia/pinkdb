//! What a streaming cursor costs over a materialized query: the store snapshot
//! (a `PinkDb` clone) plus the producer thread, interleaved in one process.
//!
//! Usage: cursor_overhead <store.pink> [queries.sql] [rounds]
//!
//! `queries.sql` holds one query per `;;`-separated block (the TPC-H set);
//! without it a few small queries run. Copy shared data to scratch first.

use std::time::{Duration, Instant};

use pinkdb::{PinkDb, QueryCursor};

fn drain(mut cursor: QueryCursor) -> u64 {
    while cursor.next_chunk().unwrap().is_some() {}
    cursor.rows_fetched()
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn materialized(db: &mut PinkDb, sql: &str) -> Duration {
    let t = Instant::now();
    db.execute(sql).unwrap();
    t.elapsed()
}

fn streamed(db: &mut PinkDb, sql: &str) -> Duration {
    let t = Instant::now();
    drain(db.query_cursor(sql).unwrap());
    t.elapsed()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut db = PinkDb::open_durable(&args[1]).unwrap();
    let queries: Vec<String> = match args.get(2) {
        Some(file) => std::fs::read_to_string(file)
            .unwrap()
            .split(";;")
            .map(|q| q.trim().trim_end_matches(';').to_string())
            .filter(|q| !q.is_empty())
            .collect(),
        None => vec![
            "SELECT 1".to_string(),
            "SELECT * FROM nation".to_string(),
            "SELECT count(*) FROM lineitem".to_string(),
        ],
    };
    let rounds: usize = args.get(3).map_or(200, |r| r.parse().unwrap());
    let mut log_ratio = 0.0;
    for (n, sql) in queries.iter().enumerate() {
        let (mut whole, mut stream) = (Vec::new(), Vec::new());
        // Warm both paths once.
        materialized(&mut db, sql);
        streamed(&mut db, sql);
        for round in 0..rounds {
            if round % 2 == 0 {
                whole.push(materialized(&mut db, sql));
                stream.push(streamed(&mut db, sql));
            } else {
                stream.push(streamed(&mut db, sql));
                whole.push(materialized(&mut db, sql));
            }
        }
        let (w, s) = (median(whole), median(stream));
        let ratio = s.as_secs_f64() / w.as_secs_f64();
        log_ratio += ratio.ln();
        println!(
            "q{:<2} materialized {:>10.3} ms  cursor {:>10.3} ms  cursor/materialized {ratio:.3}",
            n + 1,
            w.as_secs_f64() * 1e3,
            s.as_secs_f64() * 1e3
        );
    }
    println!(
        "geomean cursor/materialized {:.3}",
        (log_ratio / queries.len() as f64).exp()
    );
    let mut clones = Vec::new();
    for _ in 0..rounds {
        let t = Instant::now();
        let snapshot = db.clone();
        clones.push(t.elapsed());
        drop(snapshot);
    }
    println!("snapshot clone median {:?}", median(clones));
}
