//! Wave 139b: seeded volatile built-ins and the UUID type.
//!
//! A statement that calls `uuid()`, `gen_random_uuid()` or `random()` draws
//! from the seed of the WAL record it fills, recorded in that record's memo
//! beside the clock reading, and derives every call's value from
//! `(seed, statement ordinal, call site, chunk ordinal, row)`. Replay binds the
//! recorded seed, so a crash reproduces the values byte for byte, and the
//! number of worker threads does not matter.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_volatile_uuid_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("db.pink");
        Self { dir, file }
    }

    fn path(&self) -> &str {
        self.file.to_str().unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn open(scratch: &Scratch) -> PinkDb {
    PinkDb::open_durable_with(scratch.path(), SyncPolicy::Batch)
        .unwrap()
        .0
}

fn run(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn selected(db: &mut PinkDb, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    match db
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
    {
        Some(QueryResult::Selected { names, rows, .. }) => (names, rows),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// One column of one result, each cell as the text a cast to VARCHAR gives.
fn column(db: &mut PinkDb, sql: &str) -> Vec<String> {
    let (_, rows) = selected(db, sql);
    rows.into_iter()
        .map(|mut r| match r.remove(0) {
            Value::Null => "NULL".to_string(),
            v => v.to_string(),
        })
        .collect()
}

fn scalars(db: &mut PinkDb, sql: &str) -> Vec<String> {
    match selected(db, sql).1.pop() {
        Some(row) => row
            .into_iter()
            .map(|v| match v {
                Value::Null => "NULL".to_string(),
                v => v.to_string(),
            })
            .collect(),
        None => panic!("no row from {sql}"),
    }
}

fn assert_version_four(text: &str) {
    let b = text.as_bytes();
    assert_eq!(b.len(), 36, "{text} is not 8-4-4-4-12");
    for i in [8, 13, 18, 23] {
        assert_eq!(b[i], b'-', "{text}: no dash at byte {i}");
    }
    assert!(
        b.iter()
            .enumerate()
            .all(|(i, c)| [8, 13, 18, 23].contains(&i) || c.is_ascii_hexdigit()),
        "{text} is not lowercase hex"
    );
    assert_eq!(b[14], b'4', "{text} is not version 4");
    assert!(
        matches!(b[19], b'8' | b'9' | b'a' | b'b'),
        "{text} is not an RFC 4122 variant"
    );
}

fn copy_store(from: &Scratch, to: &Scratch) {
    for entry in std::fs::read_dir(&from.dir).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.dir.join(entry.file_name())).unwrap();
    }
}

/// Run `statements` on a durable store, read `read`, crash it (drop without a
/// checkpoint), reopen and demand byte-identical rows. Returns the rows.
fn assert_replays(tag: &str, statements: &[&str], read: &str) -> Vec<String> {
    assert_replays_with_memo(tag, statements, read, "@seed")
}

/// [`assert_replays`], for a record whose volatile value is a clock reading
/// rather than a seed.
fn assert_replays_with_memo(
    tag: &str,
    statements: &[&str],
    read: &str,
    marker: &str,
) -> Vec<String> {
    let scratch = Scratch::new(tag);
    let before = {
        let mut db = open(&scratch);
        for sql in statements {
            run(&mut db, sql);
        }
        column(&mut db, read)
    };
    let wal = std::fs::read(format!("{}.wal", scratch.path())).unwrap();
    assert!(
        wal.windows(marker.len()).any(|w| w == marker.as_bytes()),
        "{tag}: the WAL records no {marker}"
    );
    let after = {
        let mut db = open(&scratch);
        column(&mut db, read)
    };
    assert_eq!(before, after, "{tag}: the record did not replay exactly");
    before
}

fn assert_all_distinct(rows: &[String]) {
    let distinct = rows.iter().collect::<std::collections::HashSet<_>>().len();
    assert_eq!(rows.len(), distinct, "a statement reused another's values");
}

/// A `COMMIT` in a procedure body logs a segment; the statements after it are
/// logged by the next segment. Each must carry and replay its own values.
#[test]
fn a_procedure_that_commits_between_seeded_inserts_replays_each_segment() {
    let rows = assert_replays(
        "proc_segments",
        &[
            "CREATE TABLE t (id UUID);",
            "CREATE PROCEDURE batches() AS $$ BEGIN \
               INSERT INTO t SELECT uuid() FROM range(5); \
               COMMIT; \
               INSERT INTO t SELECT uuid() FROM range(7); \
               COMMIT; \
               INSERT INTO t SELECT uuid() FROM range(3); \
             END $$;",
            "CALL batches();",
        ],
        "SELECT id::VARCHAR FROM t ORDER BY id;",
    );
    assert_eq!(rows.len(), 15);
    assert_all_distinct(&rows);
}

/// A trigger action is a nested statement of the firing statement's record: it
/// draws from that record's seed at its own ordinal and replays by refiring.
#[test]
fn a_trigger_that_inserts_uuids_replays_from_one_record() {
    let rows = assert_replays(
        "trigger",
        &[
            "CREATE TABLE src (n INTEGER);",
            "CREATE TABLE log (id UUID);",
            "CREATE TRIGGER tr AFTER INSERT ON src FOR EACH ROW \
             EXECUTE INSERT INTO log VALUES (uuid());",
            "INSERT INTO src SELECT n FROM range(6) r(n);",
        ],
        "SELECT id::VARCHAR FROM log ORDER BY id;",
    );
    assert_eq!(rows.len(), 6);
    assert_all_distinct(&rows);
}

/// A procedure body's `now()` is bound inside the record that carries the
/// segment, so the replayed timestamps are the committed ones.
#[test]
fn a_procedure_body_clock_replays() {
    let rows = assert_replays_with_memo(
        "proc_clock",
        &[
            "CREATE TABLE t (id BIGINT, ts TIMESTAMP);",
            "CREATE PROCEDURE batches() AS $$ BEGIN \
               INSERT INTO t VALUES (1, now()); \
               COMMIT; \
               INSERT INTO t VALUES (2, now()); \
               COMMIT; \
             END $$;",
            "CALL batches();",
        ],
        "SELECT CAST(id AS VARCHAR) || '|' || CAST(ts AS VARCHAR) FROM t ORDER BY id;",
        "@clock",
    );
    assert_eq!(rows.len(), 2);
    assert!(rows[0] != rows[1], "{rows:?}");
}

/// A `DO` block is one record with several statements, like a transaction.
#[test]
fn a_do_block_with_seeded_inserts_replays() {
    let rows = assert_replays(
        "do_block",
        &[
            "CREATE TABLE t (id UUID);",
            "DO $$ BEGIN \
               INSERT INTO t SELECT uuid() FROM range(4); \
               INSERT INTO t SELECT uuid() FROM range(4); \
             END $$;",
        ],
        "SELECT id::VARCHAR FROM t ORDER BY id;",
    );
    assert_eq!(rows.len(), 8);
    assert_all_distinct(&rows);
}

/// An `ON COMMIT` refresh runs as its own logged statement, after the write
/// that dirtied the view: the table's per-row `uuid()` default and the view's
/// own `uuid()` call must both come back.
#[test]
fn an_on_commit_view_refresh_over_a_uuid_default_replays() {
    let rows = assert_replays(
        "on_commit_mv",
        &[
            "CREATE TABLE src (id UUID DEFAULT uuid(), n INTEGER);",
            "CREATE MATERIALIZED VIEW mv REFRESH ON COMMIT AS SELECT id, uuid() AS fresh FROM src;",
            "INSERT INTO src (n) VALUES (1), (2), (3);",
        ],
        "SELECT id::VARCHAR || '|' || fresh::VARCHAR FROM mv ORDER BY id;",
    );
    assert_eq!(rows.len(), 3);
    let ids = rows
        .iter()
        .map(|row| row.split('|').next().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_all_distinct(&ids);
}

/// 100000 rows, every one a distinct version-4 UUID.
#[test]
fn uuid_writes_are_distinct_version_four_values() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (u UUID);");
    run(&mut db, "INSERT INTO t SELECT uuid() FROM range(100000);");
    assert_eq!(
        scalars(&mut db, "SELECT count(*), count(DISTINCT u) FROM t;"),
        ["100000", "100000"]
    );
    let texts = column(&mut db, "SELECT u::VARCHAR FROM t;");
    assert_eq!(texts.len(), 100000);
    for text in &texts {
        assert_version_four(text);
    }
    assert_eq!(
        scalars(&mut db, "SELECT typeof(u), typeof(uuid()) FROM t LIMIT 1;"),
        ["UUID", "UUID"]
    );
}

/// A crash (no checkpoint) replays the statement from its recorded seed, so
/// the values are byte-identical, not merely distinct.
#[test]
fn a_crash_replays_the_same_uuids_byte_for_byte() {
    let scratch = Scratch::new("crash");
    let before = {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE t (n INTEGER, u UUID);");
        run(
            &mut db,
            "INSERT INTO t SELECT n, uuid() FROM range(5000) r(n);",
        );
        column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY n;")
    };
    // The seed rides in the record memo, so the statement text alone would not
    // be enough to reproduce these values.
    let wal = std::fs::read(format!("{}.wal", scratch.path())).unwrap();
    assert!(
        wal.windows(5).any(|w| w == b"@seed"),
        "the WAL memo records no seed"
    );
    // Dropping the handle leaves the WAL tail unreplayed; opening replays it.
    let after = {
        let mut db = open(&scratch);
        column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY n;")
    };
    assert_eq!(before.len(), 5000);
    assert_eq!(before, after);
}

/// The same recorded statement replayed by an eight-thread pool and by a
/// one-thread pool produces the same values.
#[test]
fn replay_agrees_between_one_and_eight_workers() {
    let source = Scratch::new("threads_src");
    let before = {
        let mut db = open(&source);
        run(&mut db, "CREATE TABLE t (n INTEGER, u UUID);");
        run(
            &mut db,
            "INSERT INTO t SELECT n, uuid() FROM range(20000) r(n);",
        );
        column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY n;")
    };
    // Two copies of the same unreplayed tail, so both replays bind the same
    // recorded seed and differ only in the pool they run on.
    let one = Scratch::new("threads_1");
    let eight = Scratch::new("threads_8");
    for scratch in [&one, &eight] {
        copy_store(&source, scratch);
    }
    let replay = |scratch: &Scratch, threads: usize| {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| {
            let mut db = open(scratch);
            column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY n;")
        })
    };
    let serial = replay(&one, 1);
    let parallel = replay(&eight, 8);
    assert_eq!(serial, parallel);
    assert_eq!(serial, before);
}

/// `random()` in an UPDATE replays identically too.
#[test]
fn random_in_an_update_replays_identically() {
    let scratch = Scratch::new("random");
    let before = {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE r (k INTEGER, x DOUBLE);");
        run(&mut db, "INSERT INTO r SELECT n, 0.0 FROM range(200) r(n);");
        run(&mut db, "UPDATE r SET x = random();");
        let texts = column(&mut db, "SELECT x::VARCHAR FROM r ORDER BY k;");
        assert_eq!(
            scalars(&mut db, "SELECT count(DISTINCT x) FROM r;"),
            ["200"]
        );
        texts
    };
    let after = {
        let mut db = open(&scratch);
        column(&mut db, "SELECT x::VARCHAR FROM r ORDER BY k;")
    };
    assert_eq!(before, after);
}

/// Two seeded statements in one transaction each record their own seed: a
/// shared one would make the second statement repeat the first's UUIDs.
#[test]
fn every_statement_of_a_transaction_records_its_own_seed() {
    let scratch = Scratch::new("txn");
    let before = {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE t (id UUID);");
        run(&mut db, "BEGIN;");
        run(&mut db, "INSERT INTO t SELECT uuid() FROM range(50);");
        // A read-only seeded statement is not recorded, and must not shift the
        // slot the next recorded statement uses.
        assert_eq!(
            scalars(&mut db, "SELECT count(DISTINCT uuid()) FROM range(50);"),
            ["50"]
        );
        run(&mut db, "INSERT INTO t SELECT uuid() FROM range(50);");
        run(&mut db, "COMMIT;");
        assert_eq!(
            scalars(&mut db, "SELECT count(*), count(DISTINCT id) FROM t;"),
            ["100", "100"]
        );
        column(&mut db, "SELECT id::VARCHAR FROM t ORDER BY id;")
    };
    let after = {
        let mut db = open(&scratch);
        column(&mut db, "SELECT id::VARCHAR FROM t ORDER BY id;")
    };
    assert_eq!(
        before, after,
        "the transaction did not replay byte for byte"
    );
}

/// `id UUID DEFAULT uuid()` gives every inserted row its own value, through
/// `INSERT … VALUES`, `INSERT … SELECT` and `ALTER TABLE … ADD COLUMN`.
#[test]
fn a_uuid_default_gives_every_row_its_own_value() {
    let scratch = Scratch::new("default");
    let before = {
        let mut db = open(&scratch);
        run(
            &mut db,
            "CREATE TABLE t (id UUID DEFAULT uuid(), n INTEGER);",
        );
        run(&mut db, "INSERT INTO t (n) VALUES (1),(2),(3);");
        run(&mut db, "INSERT INTO t VALUES (DEFAULT, 4);");
        run(&mut db, "INSERT INTO t (n) SELECT n FROM range(5) r(n);");
        run(&mut db, "CREATE TABLE a (n INTEGER);");
        run(&mut db, "INSERT INTO a VALUES (1),(2);");
        run(&mut db, "ALTER TABLE a ADD COLUMN id UUID DEFAULT uuid();");
        assert_eq!(
            scalars(&mut db, "SELECT count(*), count(DISTINCT id) FROM t;"),
            ["9", "9"]
        );
        assert_eq!(
            scalars(&mut db, "SELECT count(*), count(DISTINCT id) FROM a;"),
            ["2", "2"]
        );
        let mut texts = column(&mut db, "SELECT id::VARCHAR FROM t ORDER BY n;");
        texts.extend(column(&mut db, "SELECT id::VARCHAR FROM a ORDER BY n;"));
        texts
    };
    let after = {
        let mut db = open(&scratch);
        let mut texts = column(&mut db, "SELECT id::VARCHAR FROM t ORDER BY n;");
        texts.extend(column(&mut db, "SELECT id::VARCHAR FROM a ORDER BY n;"));
        texts
    };
    assert_eq!(before.len(), 11);
    assert_eq!(before, after);
}

/// Casts, ordering, GROUP BY, DISTINCT, joins and a checkpoint round trip.
#[test]
fn uuid_type_surface() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (u UUID);");
    run(
        &mut db,
        "INSERT INTO t VALUES
           ('550E8400-E29B-41D4-A716-446655440000'),
           ('{00000000-0000-0000-0000-000000000001}'),
           ('80000000000000000000000000000000'),
           ('7fffffffffffffffffffffffffffffff'),
           ('ffffffff-ffff-ffff-ffff-ffffffffffff'),
           ('550e8400-e29b-41d4-a716-446655440000');",
    );
    // The text form is canonical: lowercase, dashed, braces dropped.
    assert_eq!(
        column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY u;"),
        [
            "00000000-0000-0000-0000-000000000001",
            "550e8400-e29b-41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716-446655440000",
            "7fffffff-ffff-ffff-ffff-ffffffffffff",
            "80000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
    );
    // Unsigned 128-bit order, like DuckDB.
    assert_eq!(
        scalars(
            &mut db,
            "SELECT 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID
                  < '00000000-0000-0000-0000-000000000001'::UUID,
                    '80000000-0000-0000-0000-000000000000'::UUID
                  > '7fffffff-ffff-ffff-ffff-ffffffffffff'::UUID;",
        ),
        ["false", "true"]
    );
    assert_eq!(
        scalars(&mut db, "SELECT count(*), count(DISTINCT u) FROM t;",),
        ["6", "5"]
    );
    assert_eq!(
        scalars(
            &mut db,
            "SELECT count(*) FROM (SELECT u FROM t GROUP BY u) g;",
        ),
        ["5"]
    );
    // MIN/MAX are orderable on UUID (DuckDB allows them), and the catalog
    // reports DuckDB's type name and id.
    assert_eq!(
        scalars(&mut db, "SELECT typeof(min(u)), typeof(max(u)) FROM t;"),
        ["UUID", "UUID"]
    );
    assert_eq!(
        column(
            &mut db,
            "SELECT min(u)::VARCHAR FROM t UNION ALL SELECT max(u)::VARCHAR FROM t;",
        ),
        [
            "00000000-0000-0000-0000-000000000001",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
    );
    assert_eq!(
        column(
            &mut db,
            "SELECT data_type FROM duckdb_columns()
              WHERE table_name = 't' AND column_name = 'u';",
        ),
        ["UUID"]
    );
    // A string literal compared with a UUID is that UUID's text.
    assert_eq!(
        scalars(
            &mut db,
            "SELECT count(*) FROM t
              WHERE u = '550e8400-e29b-41d4-a716-446655440000';",
        ),
        ["2"]
    );
    run(&mut db, "CREATE TABLE s (u UUID);");
    run(
        &mut db,
        "INSERT INTO s VALUES ('550e8400-e29b-41d4-a716-446655440000');",
    );
    assert_eq!(
        scalars(&mut db, "SELECT count(*) FROM t JOIN s ON t.u = s.u;"),
        ["2"]
    );
    // BAD input is an error, not a zero UUID.
    assert!(
        db.execute("INSERT INTO s VALUES ('{550e8400e29b41d4a716446655440000}');")
            .is_err()
    );
}

/// Persist/restore: the UUID column survives a checkpoint and still reads back
/// as a UUID.
#[test]
fn uuid_survives_a_checkpoint() {
    let scratch = Scratch::new("checkpoint");
    let before = {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE t (n INTEGER, u UUID);");
        run(
            &mut db,
            "INSERT INTO t SELECT n, uuid() FROM range(3000) r(n);",
        );
        run(&mut db, "CHECKPOINT;");
        let texts = column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY n;");
        let types = scalars(&mut db, "SELECT typeof(u) FROM t LIMIT 1;");
        (texts, types)
    };
    let after = {
        let mut db = open(&scratch);
        let texts = column(&mut db, "SELECT u::VARCHAR FROM t ORDER BY n;");
        let types = scalars(&mut db, "SELECT typeof(u) FROM t LIMIT 1;");
        (texts, types)
    };
    assert_eq!(before.1, ["UUID"]);
    assert_eq!(before.0.len(), 3000);
    assert_eq!(before, after);
}

/// Unaliased expression names are the ones DuckDB gives.
#[test]
fn unaliased_expression_names_match_duckdb() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE t (x INTEGER, s VARCHAR);");
    run(&mut db, "INSERT INTO t VALUES (1, 'abc');");
    let (names, _) = selected(
        &mut db,
        "SELECT x IS NOT NULL, x IS NULL, NOT (x = 1), s NOT LIKE 'a%', -x,
                CAST(x AS INT), x::INT, substr('abc', 2, 1), x <> 1,
                COALESCE(x, 0), x NOT BETWEEN 1 AND 2
         FROM t;",
    );
    assert_eq!(
        names,
        [
            "(x IS NOT NULL)",
            "(x IS NULL)",
            "(NOT (x = 1))",
            r#"(s !~~ 'a%')"#,
            "(-(x))",
            "CAST(x AS INTEGER)",
            "CAST(x AS INTEGER)",
            "substr('abc', 2, 1)",
            "(x != 1)",
            "COALESCE(x, 0)",
            "(NOT (x BETWEEN 1 AND 2))",
        ]
    );
}
