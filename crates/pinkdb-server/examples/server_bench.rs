//! A/B benchmark: the blocking server vs the async (`async-server`) transport
//! under identical client load. Run:
//!
//! ```sh
//! cargo run --release -p pinkdb-server --features async-server \
//!     --example server_bench -- --clients 8 --requests 200
//! ```
//!
//! Both servers are started in-process on ephemeral ports, seeded with the
//! same table, and driven by the same public `Client`; the reported numbers are
//! per-request round-trip latencies (wall) plus overall throughput.

use std::time::{Duration, Instant};

use pinkdb::Database;
use pinkdb_server::Server;
use pinkdb_server::client::Client;

fn arg(name: &str, default: usize) -> usize {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == name
            && let Some(v) = it.next()
        {
            return v.parse().unwrap_or(default);
        }
    }
    default
}

const SETUP: &str = "CREATE TABLE t (a INTEGER); \
                     INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);";

fn seed(addr: std::net::SocketAddr) {
    let mut c = Client::connect(addr).expect("seed client");
    c.execute(SETUP).expect("seed");
}

struct Load {
    latencies: Vec<Duration>,
    wall: Duration,
}

fn drive(addr: std::net::SocketAddr, clients: usize, requests: usize) -> Load {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(clients);
    for _ in 0..clients {
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect(addr).expect("connect");
            // Warmup (connection, caches, protocol state) outside the samples.
            for _ in 0..5 {
                c.execute("SELECT COUNT(*) FROM t;").expect("warmup");
            }
            let mut out = Vec::with_capacity(requests);
            for _ in 0..requests {
                let t = Instant::now();
                c.execute("SELECT COUNT(*) FROM t;").expect("query");
                out.push(t.elapsed());
            }
            out
        }));
    }
    let mut latencies = Vec::with_capacity(clients * requests);
    for h in handles {
        latencies.extend(h.join().expect("client thread"));
    }
    Load {
        latencies,
        wall: start.elapsed(),
    }
}

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i].as_secs_f64() * 1e3
}

fn report(label: &str, l: &Load, clients: usize) {
    let total = l.latencies.len();
    let qps = total as f64 / l.wall.as_secs_f64();
    let mut sorted = l.latencies.clone();
    sorted.sort_unstable();
    let mean = l.latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / total as f64;
    println!(
        "| {label:<6} | {clients:>2} | {total:>6} | {:>8.0} | {:>7.2} | {:>7.2} | {:>7.2} |",
        qps,
        mean * 1e3,
        pct(&sorted, 0.50),
        pct(&sorted, 0.95),
    );
}

#[allow(unused_variables)]
fn main() {
    let clients = arg("--clients", 8);
    let requests = arg("--requests", 200);
    assert!(clients > 0 && requests > 0);

    println!(
        "server_bench: {clients} clients x {requests} requests, \
         `SELECT COUNT(*) FROM t` over the public client"
    );
    println!("| mode   | C  | reqs   | QPS      | mean ms | p50 ms  | p95 ms  |");

    // Blocking server.
    let sync = Server::bind("127.0.0.1:0", Database::new()).expect("bind sync");
    let sync_addr = sync.local_addr();
    let sync_handle = sync.spawn().expect("spawn sync");
    seed(sync_addr);
    let l = drive(sync_addr, clients, requests);
    report("sync", &l, clients);
    sync_handle.shutdown();

    // Async transport, same database shape and the same client code.
    #[cfg(feature = "async-server")]
    {
        use pinkdb_server::AsyncServer;
        let a = AsyncServer::bind("127.0.0.1:0", Database::new()).expect("bind async");
        let addr = a.local_addr();
        let handle = a.spawn().expect("spawn async");
        seed(addr);
        let l = drive(addr, clients, requests);
        report("async", &l, clients);
        handle.shutdown();
    }
    #[cfg(not(feature = "async-server"))]
    println!("| async  | (build with --features async-server)                       |");
}
