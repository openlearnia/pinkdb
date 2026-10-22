//! Client-side timings against a running `pinkdb-server` (used to compare
//! with DuckDB's Quack server on the same machine). Run:
//!
//! ```sh
//! cargo run --release -p pinkdb-server --example remote_client_bench -- \
//!     127.0.0.1:5433 [round-trips]
//! ```
//!
//! Measures, over one connection: `round-trips` sequential `SELECT 1`
//! (default 1000), then each query in [`QUERIES`] (rows are received and
//! counted, never printed). Prints one line per measurement.

use std::time::Instant;

use pinkdb::QueryResult;
use pinkdb_server::client::Client;

const QUERIES: &[(&str, &str)] = &[
    (
        "TPC-H Q6",
        "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' \
         AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;",
    ),
    (
        "TPC-H Q1",
        "SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), \
         sum(l_extendedprice * (1 - l_discount)), avg(l_quantity), avg(l_discount), count(*) \
         FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
         GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;",
    ),
    ("orders, all rows (1.5M x 9)", "SELECT * FROM orders;"),
    ("lineitem, 1M rows x 16", "SELECT * FROM lineitem LIMIT 1000000;"),
];

fn rows(results: &[QueryResult]) -> usize {
    results
        .iter()
        .map(|r| match r {
            QueryResult::Selected { rows, .. } => rows.len(),
            _ => 0,
        })
        .sum()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:5433".to_string());
    let round_trips: usize = args.next().and_then(|n| n.parse().ok()).unwrap_or(1000);
    let mut client = Client::connect(addr.as_str()).expect("connect");

    let start = Instant::now();
    for _ in 0..round_trips {
        client.execute("SELECT 1;").expect("SELECT 1");
    }
    let elapsed = start.elapsed();
    println!(
        "{round_trips} x SELECT 1: {:.3} s total, {:.1} us per round trip",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1e6 / round_trips as f64
    );

    for (label, sql) in QUERIES {
        let start = Instant::now();
        let results = client.execute(sql).unwrap_or_else(|e| panic!("{label}: {e}"));
        println!(
            "{label}: {:.3} s ({} rows received)",
            start.elapsed().as_secs_f64(),
            rows(&results)
        );
    }
}
