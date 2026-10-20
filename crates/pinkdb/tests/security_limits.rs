//! Security remediation pass 5 (H-6 / M-2 / M-6 / M-8): login verification
//! outside the store lock, timing equalization, password work-factor policy,
//! and per-statement deadlines / materialization caps.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use pinkdb::{Database, ExecLimits, PinkDb, QueryResult};

fn exec(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn rows(mut results: Vec<QueryResult>) -> Vec<Vec<pinkdb::Value>> {
    assert_eq!(results.len(), 1);
    match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    }
}

fn phc(m: u32, t: u32, p: u32) -> String {
    format!(
        "$argon2id$v=19$m={m},t={t},p={p}$AAAAAAAAAAAAAAAAAAAAAA$\
         AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    )
}

// ---------- H-6: no store lock across Argon2id ----------

#[test]
fn login_verification_runs_without_the_store_read_lock() {
    let db = Database::new();
    {
        let mut conn = db.connect();
        conn.execute("CREATE TABLE t (i INTEGER); CREATE USER alice PASSWORD 'right';")
            .unwrap();
    }

    let writer_db = db.clone();
    let (tx, rx) = mpsc::channel();
    let mut conn = db.connect();
    // The hook runs after the user record is copied out and the lock is
    // dropped, before the ~19 MiB Argon2id derivation. If the read guard were
    // still alive, the writer would block on the store write lock while this
    // thread waits for it: the hook would time out instead of observing the
    // write. A timeout therefore fails deterministically, not by timing luck.
    let result = conn.login_with_verification_hook("alice", "wrong", || {
        let writer_db = writer_db.clone();
        let tx = tx.clone();
        std::thread::spawn(move || {
            let mut writer = writer_db.connect();
            let outcome = writer
                .execute("INSERT INTO t VALUES (1);")
                .map(|_| ())
                .map_err(|e| e.to_string());
            let _ = tx.send(outcome);
        });
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("concurrent write failed: {e}"),
            Err(e) => panic!(
                "concurrent write did not proceed while login verification was in \
                 flight (store read lock still held?): {e}"
            ),
        }
    });
    assert!(result.is_err(), "the wrong password must not authenticate");
    let mut check = db.connect();
    assert_eq!(
        rows(check.execute("SELECT COUNT(*) FROM t;").unwrap()).len(),
        1
    );
}

// ---------- M-2: unknown-user timing ----------

fn median_login(db: &Database, user: &str, password: &str) -> Duration {
    let mut samples = Vec::new();
    for _ in 0..3 {
        let mut conn = db.connect();
        let start = Instant::now();
        let _ = conn.login(user, password);
        samples.push(start.elapsed());
    }
    samples.sort();
    samples[1]
}

#[test]
fn unknown_user_and_wrong_password_have_comparable_login_timing() {
    let db = Database::new();
    {
        let mut conn = db.connect();
        conn.execute("CREATE USER alice PASSWORD 'right';").unwrap();
    }
    let known = median_login(&db, "alice", "wrong");
    let unknown = median_login(&db, "nobody", "wrong");
    // Both paths must run a real Argon2id derivation (the floor is ~20-35 ms;
    // 2 ms is a very generous lower bound for a noisy CI machine).
    assert!(
        unknown >= Duration::from_millis(2),
        "unknown-user login did not hash ({unknown:?})"
    );
    assert!(
        known >= Duration::from_millis(2),
        "known-user login did not hash ({known:?})"
    );
    // Coarse ratio bound: medians of three runs, 20x either way. This is far
    // wider than scheduler noise and still catches the pre-fix ~200-500x gap.
    let ratio = |a: Duration, b: Duration| a.as_secs_f64() / b.as_secs_f64().max(0.000_001);
    assert!(
        ratio(unknown, known) < 20.0 && ratio(known, unknown) < 20.0,
        "login timing differs too much: known={known:?}, unknown={unknown:?}"
    );
}

// ---------- M-8: password-hash work-factor policy ----------

#[test]
fn password_hash_policy_covers_create_and_alter_user() {
    let mut db = PinkDb::new();
    // Floor and a ceiling combination are accepted.
    exec(
        &mut db,
        &format!(
            "CREATE USER floor_user PASSWORD_HASH '{}';",
            phc(19456, 2, 1)
        ),
    );
    exec(
        &mut db,
        &format!(
            "CREATE USER ceiling_user PASSWORD_HASH '{}';",
            phc(77824, 4, 1)
        ),
    );
    // Crackable / malformed / oversized hashes are refused at bind time.
    for (hash, needle) in [
        (phc(8, 1, 1), "too low"),
        (phc(19456, 1, 1), "too low"),
        (phc(19456, 2, 0), "too low"),
        (phc(77825, 2, 1), "too high"),
        (phc(19456, 9, 1), "too high"),
        (phc(19456, 2, 5), "too high"),
        (phc(77824, 8, 1), "too high"), // m*t above the work cap
        (phc(4_000_000, 2, 1), "too high"),
        ("$argon2id$whatever".to_string(), "Argon2 version"),
        (
            "$argon2id$v=16$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$\
             AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                .to_string(),
            "version 19",
        ),
    ] {
        let e = db
            .execute(&format!("CREATE USER bad PASSWORD_HASH '{hash}';"))
            .expect_err("hash must be refused")
            .to_string();
        assert!(e.contains(needle), "hash {hash}: {e}");
        // The same policy applies to ALTER USER.
        let e = db
            .execute(&format!("ALTER USER floor_user PASSWORD_HASH '{hash}';"))
            .expect_err("hash must be refused")
            .to_string();
        assert!(e.contains(needle), "alter {hash}: {e}");
    }
    // The engine's own hashing output is inside the policy and authenticates.
    let hash = pinkdb::hash_password("s3cret").unwrap();
    pinkdb::validate_password_hash(&hash).unwrap();
    exec(
        &mut db,
        &format!("ALTER USER floor_user PASSWORD_HASH '{hash}';"),
    );
    assert!(db.authenticate("floor_user", "s3cret").is_ok());
}

// ---------- M-6: statement deadline ----------

fn small_table(sql_rows: &str) -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute(&format!(
        "CREATE TABLE t (i INTEGER); INSERT INTO t VALUES {sql_rows};"
    ))
    .unwrap();
    db
}

fn ints(n: usize) -> String {
    (0..n)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
fn statement_deadline_interrupts_a_cartesian_product_and_releases_the_lock() {
    let db = small_table(&ints(50));
    let mut conn = db.connect();
    conn.set_exec_limits(ExecLimits {
        statement_timeout: Some(Duration::from_millis(1)),
        max_result_rows: None,
        max_result_bytes: None,
    });
    let e = conn
        .execute("SELECT * FROM t a CROSS JOIN t b CROSS JOIN t c CROSS JOIN t d;")
        .expect_err("a 6.25M-row CROSS JOIN must hit the 1 ms deadline")
        .to_string();
    assert!(e.contains("statement timeout"), "{e}");

    // The timeout error path released the lock: a writer on another
    // connection proceeds normally (and no partial state is left behind).
    let mut writer = db.connect();
    writer.execute("INSERT INTO t VALUES (999);").unwrap();
    assert_eq!(
        rows(writer.execute("SELECT COUNT(*) FROM t;").unwrap()),
        vec![vec![pinkdb::Value::Bigint(51)]]
    );
}

// ---------- M-6: result caps ----------

#[test]
fn result_row_cap_bounds_cross_join_and_admits_exactly_the_limit() {
    let db = small_table(&ints(20));
    let mut conn = db.connect();
    conn.set_exec_limits(ExecLimits {
        statement_timeout: None,
        max_result_rows: Some(100),
        max_result_bytes: None,
    });
    // 20 x 20 = 400 output rows: the nested-loop builder fails once its
    // materialized output passes the cap.
    let e = conn
        .execute("SELECT * FROM t a CROSS JOIN t b;")
        .expect_err("400 rows must exceed a 100-row cap")
        .to_string();
    assert!(e.contains("result limit exceeded"), "{e}");

    // Exactly at the cap: a 10 x 10 CROSS JOIN (100 rows) is admitted. The
    // join stage and the final-result stage have separate counters, so the
    // same rows are not double charged.
    let small = small_table(&ints(10));
    let mut conn = small.connect();
    conn.set_exec_limits(ExecLimits {
        statement_timeout: None,
        max_result_rows: Some(100),
        max_result_bytes: None,
    });
    let out = rows(conn.execute("SELECT * FROM t a CROSS JOIN t b;").unwrap());
    assert_eq!(out.len(), 100);
}

#[test]
fn result_byte_cap_bounds_materialized_values() {
    let mut db = PinkDb::new();
    let long = "x".repeat(256);
    exec(&mut db, "CREATE TABLE s (v VARCHAR);");
    exec(
        &mut db,
        &format!("INSERT INTO s VALUES ('{long}'), ('{long}');"),
    );
    let db = Database::from_store(db);
    let mut conn = db.connect();
    conn.set_exec_limits(ExecLimits {
        statement_timeout: None,
        max_result_rows: None,
        max_result_bytes: Some(128),
    });
    let e = conn
        .execute("SELECT v FROM s;")
        .expect_err("two 256-byte values must exceed a 128-byte cap")
        .to_string();
    assert!(e.contains("result limit exceeded"), "{e}");

    // A generous byte cap admits the same query.
    conn.set_exec_limits(ExecLimits {
        statement_timeout: None,
        max_result_rows: None,
        max_result_bytes: Some(64 * 1024),
    });
    assert_eq!(rows(conn.execute("SELECT v FROM s;").unwrap()).len(), 2);
}
