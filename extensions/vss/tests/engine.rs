//! Engine-level tests for `CREATE INDEX … USING HNSW` (wave 107b): the
//! optimizer rewrite, exact fallback, mutation visibility, persistence and
//! crash replay, compaction, refusals, and recall on a clustered set.

use std::path::PathBuf;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn register() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| pinkdb_vss::ensure_registered().unwrap());
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_vss_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn db() -> PinkDb {
    register();
    let mut db = PinkDb::new();
    db.load_extension_package("vss").unwrap();
    db
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected a result set from {sql}, got {other:?}"),
    }
}

fn exec(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap();
}

fn ids(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    query(db, sql)
        .into_iter()
        .map(|row| match row[0] {
            Value::Integer(v) => i64::from(v),
            Value::Bigint(v) => v,
            ref other => panic!("expected an id from {sql}, got {other:?}"),
        })
        .collect()
}

fn explain(db: &mut PinkDb, sql: &str) -> String {
    query(db, &format!("EXPLAIN {sql}"))
        .into_iter()
        .map(|row| match &row[0] {
            Value::Varchar(line) => line.clone(),
            other => panic!("expected plan text, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn vector_sql(v: &[f32]) -> String {
    let elems: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    format!("CAST([{}] AS FLOAT[{}])", elems.join(", "), v.len())
}

/// Deterministic clustered data: `clusters` centres far apart, a small
/// pseudo-random offset around each.
fn vector(i: usize, dim: usize, clusters: usize) -> Vec<f32> {
    let cluster = i % clusters;
    let mut state = (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..dim)
        .map(|d| {
            state ^= state >> 30;
            state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
            state ^= state >> 27;
            let noise = ((state >> 40) as f32 / (1u64 << 24) as f32 - 0.5) * 0.2;
            (cluster as f32 * 20.0) + (d as f32 * 0.5) + noise
        })
        .collect()
}

fn create_table(db: &mut PinkDb, name: &str, n: usize, dim: usize, clusters: usize) {
    exec(
        db,
        &format!("CREATE TABLE {name} (id INTEGER, v FLOAT[{dim}]);"),
    );
    insert_rows(db, name, 0, n, dim, clusters);
}

fn insert_rows(db: &mut PinkDb, name: &str, from: usize, to: usize, dim: usize, clusters: usize) {
    for start in (from..to).step_by(500) {
        let end = (start + 500).min(to);
        let values: Vec<String> = (start..end)
            .map(|i| format!("({i}, {})", vector_sql(&vector(i, dim, clusters))))
            .collect();
        exec(
            db,
            &format!("INSERT INTO {name} VALUES {};", values.join(", ")),
        );
    }
}

#[test]
fn explain_shows_the_index_scan_and_index_matches_exact() {
    let mut db = db();
    create_table(&mut db, "items", 300, 8, 8);
    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );
    let q = vector_sql(&vector(17, 8, 8));
    let sql = format!("SELECT id FROM items ORDER BY array_distance(v, {q}) LIMIT 10");
    let plan = explain(&mut db, &sql);
    assert!(plan.contains("VectorTopK"), "{plan}");
    let approximate = ids(&mut db, &sql);
    exec(&mut db, "SET VARIABLE hnsw_exact = true;");
    let exact = ids(&mut db, &sql);
    assert_eq!(approximate.len(), 10);
    assert_eq!(exact.len(), 10);
    // A 300-row clustered set is easy: the default ef_search finds the same
    // ten. Recall is measured properly below.
    assert_eq!(approximate, exact);
}

#[test]
fn inserts_updates_deletes_and_compaction_are_visible() {
    let mut db = db();
    create_table(&mut db, "items", 40, 6, 4);
    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );
    let q = vector_sql(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
    let sql = format!("SELECT id FROM items ORDER BY array_distance(v, {q}) LIMIT 3");

    // Insert a vector exactly at the query.
    exec(&mut db, &format!("INSERT INTO items VALUES (1000, {q});"));
    assert_eq!(ids(&mut db, &sql)[0], 1000);

    // Move it away: delete + insert under the hood.
    exec(
        &mut db,
        "UPDATE items SET v = CAST([1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0] AS FLOAT[6]) WHERE id = 1000;",
    );
    assert_ne!(ids(&mut db, &sql)[0], 1000);

    // Delete it, then compact: no tombstones stay.
    exec(&mut db, "DELETE FROM items WHERE id = 1000;");
    assert!(!ids(&mut db, &sql).contains(&1000));
    let before = db
        .vector_index_stats_in("main", "items", "items_vec")
        .unwrap();
    assert!(before.tombstones >= 1, "{before:?}");
    exec(&mut db, "CALL hnsw_compact_index('items', 'items_vec');");
    let after = db
        .vector_index_stats_in("main", "items", "items_vec")
        .unwrap();
    assert_eq!(after.tombstones, 0, "{after:?}");
    assert_eq!(after.live, 40, "{after:?}");
    assert!(!ids(&mut db, &sql).contains(&1000));
}

#[test]
fn remap_after_deleting_partial_and_dropped_chunks() {
    let mut db = db();
    // More than two CHUNK_SIZE (2048) chunks, so a delete in the first chunk
    // leaves the second chunk starting at a non-multiple offset.
    create_table(&mut db, "items", 4100, 4, 8);
    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );
    let q3000 = vector_sql(&vector(3000, 4, 8));
    let nearest = format!("SELECT id FROM items ORDER BY array_distance(v, {q3000}) LIMIT 1");
    assert_eq!(ids(&mut db, &nearest)[0], 3000);

    // One row gone from the first chunk: every later row shifts down by one.
    exec(&mut db, "DELETE FROM items WHERE id = 10;");
    assert_eq!(ids(&mut db, &nearest)[0], 3000);

    // Drop the now-partially-full second chunk completely. The third chunk
    // must shift by its pre-delete length, not be left pointing past the end.
    exec(&mut db, "DELETE FROM items WHERE id >= 2048 AND id < 4096;");
    let q4099 = vector_sql(&vector(4099, 4, 8));
    let last = format!("SELECT id FROM items ORDER BY array_distance(v, {q4099}) LIMIT 1");
    assert_eq!(ids(&mut db, &last), vec![4099]);
    exec(&mut db, "SET VARIABLE hnsw_exact = true;");
    assert_eq!(ids(&mut db, &last), vec![4099]);
    let stats = db
        .vector_index_stats_in("main", "items", "items_vec")
        .unwrap();
    assert_eq!(stats.live, 2051, "{stats:?}");
    assert_eq!(stats.tombstones, 2049, "{stats:?}");
}

#[test]
fn persistence_and_crash_replay_restore_the_same_graph() {
    register();
    let dir = scratch("persist");
    let path = dir.join("store.pink");

    // Build, save, reopen from the `.pink` section.
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    db.load_extension_package("vss").unwrap();
    create_table(&mut db, "items", 100, 8, 8);
    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'cosine');",
    );
    let q = vector_sql(&vector(11, 8, 8));
    let sql = format!("SELECT id FROM items ORDER BY array_cosine_distance(v, {q}) LIMIT 10");
    let expected = ids(&mut db, &sql);
    let graph = db.to_pink_bytes().unwrap();
    db.checkpoint().unwrap();
    drop(db);

    // The graph bytes are in the store; queries refuse until the extension
    // that owns them is loaded.
    let mut reopened = PinkDb::open(path.to_str().unwrap()).unwrap();
    // Until the owning extension is loaded the pending graph is ignored and
    // the query runs exactly; it never returns wrong rows or rebuilds.
    assert!(!explain(&mut reopened, &sql).contains("VectorTopK"));
    assert_eq!(ids(&mut reopened, &sql), expected);
    // A default build that cannot read the graph can still save the store.
    assert_eq!(reopened.to_pink_bytes().unwrap(), graph);
    reopened.load_extension_package("vss").unwrap();
    assert!(explain(&mut reopened, &sql).contains("VectorTopK"));
    assert_eq!(ids(&mut reopened, &sql), expected);
    assert_eq!(reopened.to_pink_bytes().unwrap(), graph);

    // Crash: insert without a checkpoint, then reopen and replay the WAL.
    let crash_path = dir.join("crash.pink");
    let mut live = PinkDb::open_durable(crash_path.to_str().unwrap()).unwrap();
    live.load_extension_package("vss").unwrap();
    create_table(&mut live, "items", 60, 6, 6);
    exec(
        &mut live,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );
    insert_rows(&mut live, "items", 60, 120, 6, 6);
    // WAL replay must apply deletes and vector updates (delete + insert), not
    // only appends.
    exec(&mut live, "DELETE FROM items WHERE id = 0;");
    exec(
        &mut live,
        &format!(
            "UPDATE items SET v = {} WHERE id = 1;",
            vector_sql(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        ),
    );
    let crash_sql = "SELECT id FROM items ORDER BY array_distance(v, CAST([0.0, 0.0, 0.0, 0.0, 0.0, 0.0] AS FLOAT[6])) LIMIT 10";
    let crash_expected = ids(&mut live, crash_sql);
    let crash_graph = live.to_pink_bytes().unwrap();
    drop(live);

    let mut replayed = PinkDb::open_durable(crash_path.to_str().unwrap()).unwrap();
    // The WAL prefix loaded `vss`, so the graph exists without an explicit
    // load here.
    assert_eq!(ids(&mut replayed, crash_sql), crash_expected);
    assert_eq!(replayed.to_pink_bytes().unwrap(), crash_graph);
}

#[test]
fn refusals_and_exact_fallbacks() {
    let mut db = db();
    exec(
        &mut db,
        "CREATE TABLE items (id INTEGER, v FLOAT[4], label VARCHAR);",
    );
    exec(
        &mut db,
        "INSERT INTO items VALUES (1, [1.0,0.0,0.0,0.0], 'a'), (2, [0.0,1.0,0.0,0.0], 'b');",
    );

    let non_array = db
        .execute("CREATE INDEX bad ON items USING HNSW (id)")
        .unwrap_err()
        .to_string();
    assert!(non_array.contains("ARRAY column"), "{non_array}");

    let missing = db
        .execute("CREATE INDEX bad ON items USING NOPE (v)")
        .unwrap_err()
        .to_string();
    assert!(missing.contains("not loaded"), "{missing}");

    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );

    // A different metric is not rewritten: exact brute force, not wrong rows.
    let q = vector_sql(&[1.0, 0.0, 0.0, 0.0]);
    let cosine = format!("SELECT id FROM items ORDER BY array_cosine_distance(v, {q}) LIMIT 1");
    let plan = explain(&mut db, &cosine);
    assert!(!plan.contains("VectorTopK"), "{plan}");
    assert_eq!(ids(&mut db, &cosine), vec![1]);

    // A WHERE clause also falls back to exact (DuckDB applies the filter
    // after the scan; pinkdb chooses the exact plan instead).
    let filtered =
        format!("SELECT id FROM items WHERE label <> 'a' ORDER BY array_distance(v, {q}) LIMIT 1");
    let plan = explain(&mut db, &filtered);
    assert!(!plan.contains("VectorTopK"), "{plan}");
    assert_eq!(ids(&mut db, &filtered), vec![2]);

    // A view is refused (only native tables can own a vector index).
    exec(&mut db, "CREATE VIEW v_items AS SELECT * FROM items;");
    let view = db
        .execute("CREATE INDEX bad ON v_items USING HNSW (v)")
        .unwrap_err()
        .to_string();
    assert!(view.contains("not found"), "{view}");
}

#[test]
fn recall_on_clustered_100k_is_at_least_95_percent() {
    let mut db = db();
    let n = 100_000;
    let dim = 8;
    let clusters = 16;
    create_table(&mut db, "items", n, dim, clusters);
    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );

    let mut hits = 0usize;
    let mut total = 0usize;
    for i in 0..200 {
        let q = vector_sql(&vector(i * 431 + 7, dim, clusters));
        let sql = format!("SELECT id FROM items ORDER BY array_distance(v, {q}) LIMIT 10");
        let approximate: std::collections::HashSet<i64> = ids(&mut db, &sql).into_iter().collect();
        exec(&mut db, "SET VARIABLE hnsw_exact = true;");
        let exact: Vec<i64> = ids(&mut db, &sql);
        exec(&mut db, "SET VARIABLE hnsw_exact = false;");
        hits += exact.iter().filter(|id| approximate.contains(id)).count();
        total += exact.len();
    }
    let recall = hits as f64 / total as f64;
    assert!(recall >= 0.95, "recall@{total} = {recall:.3}");
}

#[test]
fn parallel_build_matches_crash_replay() {
    register();
    let dir = scratch("bulkcrash");
    let path = dir.join("store.pink");
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    db.load_extension_package("vss").unwrap();
    // Past the 1,024-vector seed and several 256-vector batches, then a
    // multi-row INSERT into the indexed table (also the bulk path).
    create_table(&mut db, "items", 3000, 8, 8);
    db.execute("CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');")
        .unwrap();
    insert_rows(&mut db, "items", 3000, 4500, 8, 8);
    let q = vector_sql(&vector(4000, 8, 8));
    let sql = format!("SELECT id FROM items ORDER BY array_distance(v, {q}) LIMIT 10");
    let expected = ids(&mut db, &sql);
    let graph = db.to_pink_bytes().unwrap();
    drop(db);

    let mut replayed = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(ids(&mut replayed, &sql), expected);
    assert_eq!(replayed.to_pink_bytes().unwrap(), graph);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn wal_replay_after_a_checkpoint_applies_to_the_pending_graph() {
    register();
    let dir = scratch("walpending");
    let path = dir.join("store.pink");
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    db.load_extension_package("vss").unwrap();
    create_table(&mut db, "items", 40, 6, 6);
    exec(
        &mut db,
        "CREATE INDEX items_vec ON items USING HNSW (v) WITH (metric = 'l2sq');",
    );
    db.checkpoint().unwrap();
    insert_rows(&mut db, "items", 40, 60, 6, 6);
    let sql = "SELECT id FROM items ORDER BY array_distance(v, CAST([0.0,0.0,0.0,0.0,0.0,0.0] AS FLOAT[6])) LIMIT 5";
    let expected = ids(&mut db, sql);
    let graph = db.to_pink_bytes().unwrap();
    drop(db);

    let mut replayed = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(ids(&mut replayed, sql), expected);
    assert_eq!(replayed.to_pink_bytes().unwrap(), graph);
}
