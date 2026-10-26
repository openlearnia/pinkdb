//! Measure an HNSW build and query on clustered synthetic vectors.
//!
//! Usage: `cargo run --release -p pinkdb-vss --example bench_vector -- <n> <dim> <clusters> <queries> <store>`
//!
//! The vectors are a Gaussian-like mixture: each row sits near one of
//! `clusters` centres, so nearest neighbours are meaningful (uniform noise
//! would make every recall number look the same at high dimensions).

use std::time::Instant;

use pinkdb::{PinkDb, QueryResult, Value};

fn vec_sql(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 10 + 32);
    s.push_str("CAST([");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{x:?}"));
    }
    s.push_str(&format!("] AS FLOAT[{}])", v.len()));
    s
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 30;
        self.0 = self.0.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x94d0_49bb_1331_11eb)
    }
    fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32
    }
    // Irwin–Hall: close enough to Gaussian for a mixture.
    fn gauss(&mut self) -> f32 {
        (0..6).map(|_| self.unit()).sum::<f32>() - 3.0
    }
}

fn centres(dim: usize, clusters: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    (0..clusters)
        .map(|_| (0..dim).map(|_| rng.gauss() * 3.0).collect())
        .collect()
}

fn sample(n: usize, centres: &[Vec<f32>], seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            let c = &centres[i % centres.len()];
            c.iter().map(|x| x + rng.gauss() * 0.35).collect()
        })
        .collect()
}

fn run(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn ids(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    run(db, sql)
        .into_iter()
        .map(|r| match r[0] {
            Value::Integer(v) => i64::from(v),
            Value::Bigint(v) => v,
            _ => panic!("bad id"),
        })
        .collect()
}

fn pct(xs: &mut [f64], p: f64) -> f64 {
    xs.sort_by(|a, b| a.total_cmp(b));
    let i = ((xs.len() - 1) as f64 * p).round() as usize;
    xs[i]
}

fn main() {
    pinkdb_vss::ensure_registered().unwrap();
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(100_000);
    let dim: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(768);
    let clusters: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(32);
    let nq: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(200);
    let store = args
        .get(5)
        .cloned()
        .unwrap_or_else(|| format!("/tmp/pinkdb_bench_{n}_{dim}.pink"));
    std::fs::remove_file(&store).ok();

    let centres = centres(dim, clusters, 0x107b);
    let rows = sample(n, &centres, 0x107b);
    let queries = sample(nq, &centres, 0x51ee);

    let mut db = PinkDb::new();
    db.load_extension_package("vss").unwrap();
    db.execute(&format!("CREATE TABLE items (id INTEGER, v FLOAT[{dim}]);"))
        .unwrap();

    let load = Instant::now();
    let batch = if dim >= 512 { 64 } else { 256 };
    for start in (0..n).step_by(batch) {
        let end = (start + batch).min(n);
        let mut sql = String::from("INSERT INTO items VALUES ");
        for (i, row) in rows.iter().enumerate().take(end).skip(start) {
            if i > start {
                sql.push(',');
            }
            sql.push_str(&format!("({i}, {})", vec_sql(row)));
        }
        sql.push(';');
        db.execute(&sql).unwrap();
    }
    let load = load.elapsed().as_secs_f64();

    // Brute force through the ordinary ORDER BY path (no index): the LIMIT
    // keeps only a bounded heap of ten rows.
    eprintln!("plain top-k...");
    let mut plain = Vec::new();
    for q in &queries {
        let sql = format!(
            "SELECT id FROM items ORDER BY array_distance(v, {}) LIMIT 10",
            vec_sql(q)
        );
        let t = Instant::now();
        ids(&mut db, &sql);
        plain.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    eprintln!("build...");
    let before = db.to_pink_bytes().unwrap().len();
    let build = Instant::now();
    db.execute("CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');")
        .unwrap();
    let build = build.elapsed().as_secs_f64();
    let after = db.to_pink_bytes().unwrap().len();
    let size = after - before;
    let plan = run(
        &mut db,
        &format!(
            "EXPLAIN SELECT id FROM items ORDER BY array_distance(v, {}) LIMIT 10",
            vec_sql(&queries[0])
        ),
    );
    println!("plan: {:?}", plan);

    // Exact answers, once, for recall.
    eprintln!("exact...");
    db.execute("SET VARIABLE hnsw_exact = true;").unwrap();
    let mut exact = Vec::with_capacity(nq);
    let mut exact_ms = Vec::new();
    for q in &queries {
        let sql = format!(
            "SELECT id FROM items ORDER BY array_distance(v, {}) LIMIT 10",
            vec_sql(q)
        );
        let t = Instant::now();
        exact.push(ids(&mut db, &sql));
        exact_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    db.execute("SET VARIABLE hnsw_exact = false;").unwrap();

    println!(
        "n={n} dim={dim} clusters={clusters} queries={nq} load={load:.2}s build={build:.2}s size={size} B"
    );
    println!(
        "brute-force (plain ORDER BY): p50={:.2}ms p95={:.2}ms",
        pct(&mut plain, 0.50),
        pct(&mut plain, 0.95)
    );
    println!(
        "brute-force (exact switch):   p50={:.2}ms p95={:.2}ms",
        pct(&mut exact_ms, 0.50),
        pct(&mut exact_ms, 0.95)
    );
    let stats = db
        .vector_index_stats_in("main", "items", "items_vec")
        .unwrap();
    println!("graph: {stats:?}");
    for ef in [16usize, 64, 128, 256] {
        eprintln!("ef {ef}...");
        db.execute(&format!("SET VARIABLE hnsw_ef_search = {ef};"))
            .unwrap();
        let mut hits = 0usize;
        let mut lat = Vec::new();
        for (q, truth) in queries.iter().zip(&exact) {
            let sql = format!(
                "SELECT id FROM items ORDER BY array_distance(v, {}) LIMIT 10",
                vec_sql(q)
            );
            let t = Instant::now();
            let got = ids(&mut db, &sql);
            lat.push(t.elapsed().as_secs_f64() * 1000.0);
            let truth: std::collections::HashSet<i64> = truth.iter().copied().collect();
            hits += got.iter().filter(|id| truth.contains(id)).count();
        }
        let recall = hits as f64 / (nq * 10) as f64;
        println!(
            "ef_search={ef:3} recall@10={recall:.3} p50={:.2}ms p95={:.2}ms",
            pct(&mut lat, 0.50),
            pct(&mut lat, 0.95)
        );
    }

    let save = Instant::now();
    db.save(&store).unwrap();
    println!(
        "saved {store} ({} B) in {:.2}s; index bytes {size}",
        std::fs::metadata(&store).unwrap().len(),
        save.elapsed().as_secs_f64()
    );
    drop(db);
    std::fs::remove_file(&store).ok();
    std::fs::remove_file(format!("{store}.wal")).ok();
    std::fs::remove_file(format!("{store}.lock")).ok();
}
