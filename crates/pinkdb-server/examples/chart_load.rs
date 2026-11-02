//! Client-side load for the QuackLab chart widgets, used by
//! `bench/quacklab/run.py` to compare pinkdb-server with DuckDB's Quack
//! server. Run:
//!
//! ```sh
//! cargo run --release -p pinkdb-server --example chart_load -- \
//!     127.0.0.1:5433 4 10 queries.json 7
//! ```
//!
//! `queries.json` is `{name: sql}`. Every client opens its own connection,
//! warms up with one round, then runs `rounds` rounds of all queries in a
//! seeded shuffle. Each successful `execute` is timed with `Instant`; rows are
//! received and dropped, never printed. One JSON object goes to stdout:
//! `{"wall_s":…, "requests":[{"client":i,"name":…,"ms":…}, …]}`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Barrier, Mutex};
use std::time::Instant;

use pinkdb_server::client::Client;

/// One round of every query; not timed.
fn warm_up(client: &mut Client, queries: &[(String, String)]) -> Result<(), String> {
    for (_, sql) in queries {
        client.execute(sql).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// xorshift-free LCG: the same, tiny sequence on every platform.
fn next_seed(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn shuffle(order: &mut [usize], state: &mut u64) {
    for i in (1..order.len()).rev() {
        let j = (next_seed(state) % (i as u64 + 1)) as usize;
        order.swap(i, j);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 5 {
        eprintln!("usage: chart_load <addr> <clients> <rounds> <queries.json> <seed>");
        std::process::exit(2);
    }
    let addr = args[0].clone();
    let clients: usize = args[1].parse().expect("clients");
    let rounds: usize = args[2].parse().expect("rounds");
    let seed: u64 = args[4].parse().expect("seed");
    let text = std::fs::read_to_string(&args[3]).expect("read queries");
    let map: HashMap<String, String> = serde_json::from_str(&text).expect("queries.json");
    let mut queries: Vec<(String, String)> = map.into_iter().collect();
    // A stable order makes the per-client shuffle reproducible from <seed>
    // alone, whatever order the JSON object iterated in.
    queries.sort_by(|a, b| a.0.cmp(&b.0));

    let barrier = Barrier::new(clients + 1);
    let failed = AtomicBool::new(false);
    let error = Mutex::new(None::<String>);

    let (wall_s, output): (f64, Vec<(usize, String, f64)>) = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for id in 0..clients {
            let addr = addr.clone();
            let queries = queries.clone();
            let barrier = &barrier;
            let failed = &failed;
            let error = &error;
            handles.push(scope.spawn(move || {
                let mut client = match Client::connect(addr.as_str()) {
                    Ok(client) => client,
                    Err(e) => {
                        failed.store(true, Ordering::Relaxed);
                        *error.lock().unwrap() = Some(format!("client {id}: connect: {e}"));
                        barrier.wait();
                        return Vec::new();
                    }
                };
                if let Err(e) = warm_up(&mut client, &queries) {
                    failed.store(true, Ordering::Relaxed);
                    *error.lock().unwrap() = Some(format!("client {id}: warm-up: {e}"));
                    barrier.wait();
                    return Vec::new();
                }
                // All clients start their measured rounds together.
                barrier.wait();
                let mut state = seed ^ (id as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
                let mut order: Vec<usize> = (0..queries.len()).collect();
                let mut times = Vec::with_capacity(rounds * queries.len());
                for _ in 0..rounds {
                    shuffle(&mut order, &mut state);
                    for &i in &order {
                        if failed.load(Ordering::Relaxed) {
                            return times;
                        }
                        let (name, sql) = &queries[i];
                        let start = Instant::now();
                        match client.execute(sql) {
                            Ok(_) => {
                                times.push((id, name.clone(), start.elapsed().as_secs_f64() * 1e3))
                            }
                            Err(e) => {
                                failed.store(true, Ordering::Relaxed);
                                *error.lock().unwrap() = Some(format!("client {id}: {name}: {e}"));
                                return times;
                            }
                        }
                    }
                }
                times
            }));
        }

        barrier.wait();
        let start = Instant::now();
        let mut output = Vec::new();
        for handle in handles {
            if let Ok(times) = handle.join() {
                output.extend(times);
            }
        }
        (start.elapsed().as_secs_f64(), output)
    });

    if failed.load(Ordering::Relaxed) {
        if let Some(e) = error.lock().unwrap().as_deref() {
            eprintln!("{e}");
        }
        std::process::exit(1);
    }

    let requests: Vec<serde_json::Value> = output
        .iter()
        .map(|(client, name, ms)| serde_json::json!({"client": client, "name": name, "ms": ms}))
        .collect();
    println!(
        "{}",
        serde_json::json!({"wall_s": wall_s, "requests": requests})
    );
}
