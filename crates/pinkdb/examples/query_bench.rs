//! Query benchmark harness for pinkdb. Usage:
//!   cargo run --release -p pinkdb --example query_bench -- \
//!     [--csv PATH] [--runs N] [--only SUBSTR]
//!
//! Times best-of-N CSV import, then best-of-N execution of a fixed query
//! battery over one fresh import. All reported numbers are milliseconds.
//! `--only SUBSTR` keeps queries whose name contains SUBSTR.

use std::time::Instant;

use pinkdb::{PinkDb, QueryResult};

/// One benchmark query.
struct Bench {
    name: &'static str,
    sql: &'static str,
}

/// Fixed query battery, in report order.
const BATTERY: &[Bench] = &[
    Bench {
        name: "count_star",
        sql: "SELECT COUNT(*) FROM sales;",
    },
    Bench {
        name: "filter_num",
        sql: "SELECT COUNT(*) FROM sales WHERE total > 500;",
    },
    Bench {
        name: "filter_str",
        sql: "SELECT COUNT(*) FROM sales WHERE region = 'west';",
    },
    Bench {
        name: "proj_func",
        sql: "SELECT COUNT(*) FROM (SELECT upper(region), length(region) FROM sales) AS s;",
    },
    Bench {
        name: "group_region",
        sql: "SELECT region, COUNT(*), AVG(total), SUM(total), MIN(total), MAX(total) FROM sales GROUP BY region;",
    },
    Bench {
        name: "group_id",
        sql: "SELECT COUNT(*) FROM (SELECT id FROM sales GROUP BY id) AS s;",
    },
    Bench {
        name: "order_num_topn",
        sql: "SELECT COUNT(*) FROM (SELECT id, total FROM sales ORDER BY total DESC LIMIT 10) AS s;",
    },
    Bench {
        name: "order_str",
        sql: "SELECT COUNT(*) FROM (SELECT region FROM sales ORDER BY region) AS s;",
    },
    Bench {
        name: "distinct_lowcard",
        sql: "SELECT COUNT(*) FROM (SELECT DISTINCT region, active FROM sales) AS s;",
    },
    Bench {
        name: "distinct_id",
        sql: "SELECT COUNT(*) FROM (SELECT DISTINCT id FROM sales) AS s;",
    },
    Bench {
        name: "window_row_number",
        sql: "SELECT COUNT(*) FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM sales) AS s;",
    },
    Bench {
        name: "window_count_star",
        sql: "SELECT COUNT(*) FROM (SELECT id, COUNT(*) OVER (PARTITION BY region) AS c FROM sales) AS s;",
    },
    Bench {
        name: "join_self",
        sql: "SELECT COUNT(*) FROM (SELECT a.region FROM sales a JOIN sales b ON a.id = b.id) AS s;",
    },
    // Fused join→GROUP BY shape: the inner aggregate over the join is fused.
    Bench {
        name: "join_group",
        sql: "SELECT COUNT(*) FROM (SELECT a.region FROM sales a JOIN sales b ON a.id = b.id GROUP BY a.region) AS s;",
    },
    // Mixed-side predicate (`a.total > b.total`) cannot be pushed below the
    // join, so the Filter stays between the aggregate and the join and the
    // shape does not fuse: isolates the materializing join with the integer
    // key path.
    Bench {
        name: "join_mat",
        sql: "SELECT COUNT(*) FROM (SELECT a.id, b.total FROM sales a JOIN sales b ON a.id = b.id WHERE a.total > b.total) AS s;",
    },
    // 1M probes against a 4-row string-key build side.
    Bench {
        name: "join_probe",
        sql: "SELECT COUNT(*) FROM (SELECT s.id FROM sales s JOIN (SELECT DISTINCT region FROM sales) r ON s.region = r.region) AS s;",
    },
];

/// Print usage and exit with status 2.
fn usage() -> ! {
    eprintln!("usage: query_bench [--csv PATH] [--runs N] [--only SUBSTR]");
    std::process::exit(2);
}

struct Args {
    csv: String,
    runs: usize,
    only: Option<String>,
}

impl Args {
    fn parse() -> Args {
        let mut args = Args {
            csv: "bench/sales_1m.csv".to_string(),
            runs: 5,
            only: None,
        };
        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            match flag.as_str() {
                "--csv" => args.csv = it.next().unwrap_or_else(|| usage()),
                "--runs" => {
                    args.runs = it
                        .next()
                        .and_then(|n| n.parse().ok())
                        .unwrap_or_else(|| usage())
                }
                "--only" => args.only = Some(it.next().unwrap_or_else(|| usage())),
                _ => usage(),
            }
        }
        if args.runs == 0 {
            usage();
        }
        args
    }
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

fn main() {
    let args = Args::parse();
    if !std::path::Path::new(&args.csv).is_file() {
        eprintln!("query_bench: no such file: {}", args.csv);
        usage();
    }

    let mut best_import = f64::MAX;
    for _ in 0..args.runs {
        let mut db = PinkDb::new();
        let t0 = Instant::now();
        pinkdb::csv::import_csv(&mut db, &args.csv, Some("sales")).unwrap();
        best_import = best_import.min(ms(t0));
    }
    println!("query_bench: {}, runs={}", args.csv, args.runs);
    println!("import  best of {}: {best_import:.3} ms", args.runs);

    let mut db = PinkDb::new();
    pinkdb::csv::import_csv(&mut db, &args.csv, Some("sales")).unwrap();

    let only = args.only.as_deref();
    let selected: Vec<&Bench> = BATTERY
        .iter()
        .filter(|b| only.is_none_or(|pat| b.name.contains(pat)))
        .collect();
    let name_w = BATTERY.iter().map(|b| b.name.len()).max().unwrap_or(4);

    println!();
    println!("{:<name_w$} {:>10} {:>6}", "name", "best ms", "rows");
    let mut total = 0.0;
    for bench in &selected {
        let mut best = f64::MAX;
        let mut row_count = 0;
        for _ in 0..args.runs {
            let t0 = Instant::now();
            let result = db.execute(bench.sql);
            best = best.min(ms(t0));
            match result.unwrap().as_slice() {
                [QueryResult::Selected { rows, .. }] => row_count = rows.len(),
                other => panic!(
                    "{}: expected one Selected result, got {other:?}",
                    bench.name
                ),
            }
        }
        total += best;
        println!("{:<name_w$} {best:>10.3} {row_count:>6}", bench.name);
    }
    println!("{:<name_w$} {total:>10.3}", "TOTAL");
}
