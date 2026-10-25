//! A 100k-document build and query benchmark. Run explicitly:
//!
//! ```text
//! cargo test -p pinkdb-fts --release --test scale -- --ignored --nocapture
//! ```

use std::time::Instant;

use pinkdb::{PinkDb, QueryResult, Value};

const DOCS: usize = 100_000;
const WORDS: [&str; 24] = [
    "relational",
    "database",
    "index",
    "search",
    "query",
    "ranks",
    "documents",
    "bm25",
    "inverted",
    "token",
    "phrase",
    "ranking",
    "corpus",
    "stemming",
    "stopword",
    "accent",
    "field",
    "score",
    "term",
    "vector",
    "storage",
    "engine",
    "table",
    "column",
];

/// Deterministic xorshift so two runs index the same corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn word(&mut self) -> &'static str {
        WORDS[(self.next() % WORDS.len() as u64) as usize]
    }
}

fn text(rng: &mut Rng, len: usize) -> String {
    let mut out = String::new();
    for i in 0..len {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(rng.word());
    }
    out
}

#[test]
#[ignore = "100k-document benchmark; run with --ignored --nocapture"]
fn hundred_thousand_documents() {
    pinkdb_fts::ensure_registered().unwrap();
    let mut db = PinkDb::new();
    db.load_extension_package("fts").unwrap();
    db.execute("CREATE TABLE docs (id INTEGER, title VARCHAR, body VARCHAR)")
        .unwrap();

    let mut rng = Rng(0x5eed_108a);
    let started = Instant::now();
    // Batched inserts keep the load in the seconds, not minutes.
    for batch in 0..(DOCS / 500) {
        let mut sql = String::from("INSERT INTO docs VALUES ");
        for i in 0..500 {
            if i > 0 {
                sql.push(',');
            }
            let id = batch * 500 + i;
            sql.push_str(&format!(
                "({id}, '{}', '{}')",
                text(&mut rng, 6),
                text(&mut rng, 120)
            ));
        }
        db.execute(&sql).unwrap();
    }
    let load = started.elapsed();

    let started = Instant::now();
    db.execute("CALL create_fts_index('docs', 'id', 'title', 'body')")
        .unwrap();
    let build = started.elapsed();
    let terms = match db
        .execute("SELECT count(*) FROM fts_main_docs.terms")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => match rows[0][0] {
            Value::Bigint(n) => n,
            ref other => panic!("unexpected {other:?}"),
        },
        other => panic!("unexpected {other:?}"),
    };

    let queries = [
        "relational database",
        "bm25 ranking",
        "inverted index token",
        "accent stopword stemming",
        "storage engine table column",
        "relational index search query",
        "vector score ranking",
        "phrase corpus field",
        "database database database",
        "relational database index search query ranking bm25",
    ];
    let mut total = std::time::Duration::ZERO;
    let mut slowest = std::time::Duration::ZERO;
    let runs = 5;
    for _ in 0..runs {
        for query in queries {
            let started = Instant::now();
            db.execute(&format!(
                "SELECT id, match_bm25('docs', id, '{query}') AS score FROM docs \
                 ORDER BY score DESC LIMIT 10"
            ))
            .unwrap();
            let took = started.elapsed();
            total += took;
            slowest = slowest.max(took);
        }
    }
    let mean = total / (runs * queries.len()) as u32;
    println!(
        "100k docs: load {load:?}, build {build:?}, {terms} postings; \
         query mean {mean:?}, slowest {slowest:?} (top-10 over a full scan)"
    );
}
