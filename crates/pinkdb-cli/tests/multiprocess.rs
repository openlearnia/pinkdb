//! Multi-process access to one durable `.pink` database (wave 45m): REAL
//! spawned CLI processes (never threads) driven over stdin with marker-line
//! synchronization, plus fresh-process verification of the committed bytes.
//!
//! Determinism follows the crash-recovery harness: every ack marker is a
//! query result printed only after `SyncPolicy::Sync` made the statement
//! durable, so waiting for a marker is the only synchronization needed. The
//! one deliberate exception is the lease-timeout family, where a short
//! `PINKDB_BUSY_TIMEOUT_MS` bounds how long a process waits for another
//! holder before reporting the database busy.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_mp_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(ckpt: &Path) {
    if let Some(dir) = ckpt.parent() {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A live CLI process: SQL in over stdin, merged stdout+stderr out, marker
/// lines synchronized by polling the output stream.
struct Cli {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    lines: std::sync::mpsc::Receiver<String>,
    out: String,
}

impl Cli {
    fn spawn(cwd: &Path, busy_timeout_ms: Option<u64>) -> Cli {
        Cli::spawn_full(cwd, busy_timeout_ms, None)
    }

    /// [`Cli::spawn`] plus `PINKDB_TXN_RETRIES` (0 = report the first
    /// cross-process conflict instead of retrying the transaction).
    fn spawn_with_retries(cwd: &Path, retries: usize) -> Cli {
        Cli::spawn_full(cwd, None, Some(retries))
    }

    fn spawn_full(cwd: &Path, busy_timeout_ms: Option<u64>, retries: Option<usize>) -> Cli {
        let (reader, writer) = std::io::pipe().expect("create pipe");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
        cmd.current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(writer.try_clone().expect("clone pipe writer"))
            .stderr(writer);
        if let Some(ms) = busy_timeout_ms {
            cmd.env("PINKDB_BUSY_TIMEOUT_MS", ms.to_string());
        }
        if let Some(n) = retries {
            cmd.env("PINKDB_TXN_RETRIES", n.to_string());
        }
        let mut child = cmd.spawn().expect("spawn pinkdb CLI");
        let stdin = child.stdin.take().expect("child stdin");
        // A reader thread forwards merged stdout+stderr lines; `next_line`
        // applies a bounded wait, so a rare stall fails with the collected
        // output instead of hanging the suite forever.
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Cli {
            child,
            stdin: Some(stdin),
            lines,
            out: String::new(),
        }
    }

    fn open(&mut self, path: &str) {
        self.send(&format!(".open {path}\n"));
        self.wait_for("Opened");
    }

    fn send(&mut self, sql: &str) {
        let stdin = self
            .stdin
            .as_mut()
            .expect("stdin available (not handed to a feeder thread)");
        stdin.write_all(sql.as_bytes()).expect("write to CLI stdin");
        stdin.flush().expect("flush CLI stdin");
    }

    fn next_line(&mut self) -> String {
        use std::sync::mpsc::RecvTimeoutError;
        let line = match self.lines.recv_timeout(std::time::Duration::from_secs(120)) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => {
                // Drain anything already buffered so the evidence is complete.
                while let Ok(line) = self.lines.try_recv() {
                    self.out.push_str(&line);
                }
                panic!("no CLI output for 120s; collected output:\n{}", self.out);
            }
            Err(RecvTimeoutError::Disconnected) => panic!(
                "CLI exited before the expected output; collected output:\n{}",
                self.out
            ),
        };
        self.out.push_str(&line);
        line
    }

    /// Wait until a line contains `marker`.
    fn wait_for(&mut self, marker: &str) -> String {
        loop {
            let line = self.next_line();
            if line.contains(marker) {
                return line;
            }
        }
    }

    /// Run `sql` followed by a marker query, then wait for `marker`. An
    /// error line before the marker fails the test (the marker must prove
    /// the statement succeeded, not merely that it was attempted).
    fn marker(&mut self, sql: &str, marker: &str) -> String {
        self.send(&format!("{sql}\nSELECT '{marker}' AS m;\n"));
        loop {
            let line = self.next_line();
            if line.contains(marker) {
                return line;
            }
            assert!(
                !line.contains("Error"),
                "statement before {marker:?} failed: {line}\ncollected output:\n{}",
                self.out
            );
        }
    }

    /// Assert that `queries` (a conditional marker query) prints `marker`
    /// before the unconditional end marker, and consume the whole batch
    /// (through `__end__`) so the next wait starts on a clean stream.
    /// Deterministic and hang-free.
    fn expect_marker(&mut self, queries: &str, marker: &str) {
        self.send(&format!("{queries}\nSELECT '__end__' AS m;\n"));
        let mut seen = false;
        loop {
            let line = self.next_line();
            if line.contains(marker) {
                seen = true;
            }
            if line.contains("__end__") {
                assert!(
                    seen,
                    "expected {marker:?} before __end__; collected output:\n{}",
                    self.out
                );
                return;
            }
        }
    }

    /// Assert that `queries` does NOT print `marker` before the unconditional
    /// end marker.
    fn expect_absent(&mut self, queries: &str, marker: &str) {
        self.send(&format!("{queries}\nSELECT '__end__' AS m;\n"));
        loop {
            let line = self.next_line();
            if line.contains("__end__") {
                return;
            }
            if line.contains(marker) {
                panic!(
                    "did not expect {marker:?} before __end__; collected output:\n{}",
                    self.out
                );
            }
        }
    }

    /// Wait for an error line containing `needle`.
    fn expect_error(&mut self, needle: &str) -> String {
        loop {
            let line = self.next_line();
            if line.contains("Error") && line.contains(needle) {
                return line;
            }
        }
    }

    fn quit(mut self) {
        self.send(".quit\n");
        let _ = self.child.wait();
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Feed one `sql` line at a fixed cadence from a background thread until
    /// `stop` is set or the process dies, and hand back the stop flag and
    /// the thread handle. Used to keep a committer's conflicting frames
    /// landing inside another process's transaction window on any machine
    /// speed (a fixed burst can finish before that window even opens).
    fn feed_until_stopped(
        &mut self,
        sql: &'static str,
    ) -> (std::sync::Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let mut stdin = self.stdin.take().expect("stdin not already handed out");
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let thread_stop = std::sync::Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                // The child can die mid-feed (the test kills it once the
                // conflict is observed): a failed write simply ends the loop.
                if stdin.write_all(sql.as_bytes()).is_err() || stdin.flush().is_err() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });
        (stop, handle)
    }
}

/// Fresh-process verification of several `(table, expected column a)` pairs.
fn verify_tables(ckpt: &Path, tables: &[(&str, &[i64])]) {
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    for (table, expected) in tables {
        assert_eq!(
            ints(&mut db, &format!("SELECT a FROM {table} ORDER BY a;")),
            expected.to_vec(),
            "table {table}"
        );
    }
}

/// Fresh-process verification: open in-process, see exactly the committed
/// state.
fn verify(ckpt: &Path, expected: &[i64]) -> pinkdb::RecoveryInfo {
    let (mut db, info) =
        PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        expected,
        "fresh open must see exactly the committed rows"
    );
    info
}

fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i as i64,
                Value::Bigint(i) => *i,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Any `<ckpt>.wal.<gen>.roll` left next to the database?
fn has_gen_roll(ckpt: &Path) -> bool {
    let Some(name) = ckpt.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let prefix = format!("{name}.wal.");
    std::fs::read_dir(ckpt.parent().unwrap())
        .map(|rd| {
            rd.flatten().any(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.starts_with(&prefix) && n.ends_with(".roll")
            })
        })
        .unwrap_or(false)
}

// ---------- M1/M3 scenarios ----------

#[test]
fn two_processes_write_disjoint_tables() {
    let dir = scratch("disjoint");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker("CREATE TABLE a (x INTEGER);", "a-ready");
    p2.open("db.pink");
    p2.marker(
        "CREATE TABLE b (x INTEGER); INSERT INTO b VALUES (2);",
        "b-row",
    );
    p1.marker("INSERT INTO a VALUES (1);", "a-row");
    // Each process sees the other's commit after its next statement.
    p1.expect_marker("SELECT 'see-b' AS m FROM b WHERE x = 2;", "see-b");
    p2.expect_marker("SELECT 'see-a' AS m FROM a WHERE x = 1;", "see-a");
    p1.quit();
    p2.quit();
    cleanup(&ckpt);
}

#[test]
fn reader_sees_rows_while_writer_loads() {
    let dir = scratch("readerwriter");
    let ckpt = dir.join("db.pink");
    let mut reader = Cli::spawn(&dir, None);
    reader.open("db.pink");
    reader.marker("CREATE TABLE t (a INTEGER);", "created");
    let mut writer = Cli::spawn(&dir, None);
    writer.open("db.pink");
    // The writer loads rows in a loop; after each ack the reader must see at
    // least that many (an acked row is durable and published).
    for n in 1..=20i64 {
        writer.marker(&format!("INSERT INTO t VALUES ({n});"), &format!("w-{n}"));
        // Row `n` exists exactly when the nth insert was committed and the
        // reader's next statement saw it (statement-start refresh).
        reader.expect_marker(
            &format!("SELECT 'r-{n}' AS m FROM t WHERE a = {n};"),
            &format!("r-{n}"),
        );
    }
    writer.quit();
    reader.quit();
    verify(&ckpt, &(1..=20).collect::<Vec<_>>());
    cleanup(&ckpt);
}

#[test]
fn reader_not_blocked_by_open_transaction() {
    let dir = scratch("readertxn");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
        "setup",
    );
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    // P1 opens a transaction (wave 45o: it holds no lease while open — the
    // lease is taken only for the commit span).
    p1.marker("BEGIN; INSERT INTO t VALUES (99);", "txn-open");
    // P2's reads are served from memory: they must NOT block on the lease,
    // and must not see P1's uncommitted row.
    p2.expect_absent(
        "SELECT 'uncommitted' AS m FROM t WHERE a = 99;",
        "uncommitted",
    );
    p1.marker("COMMIT;", "p1-committed");
    // After the commit the reader refreshes (epoch changed) and sees it.
    p2.expect_marker("SELECT 'see-99' AS m FROM t WHERE a = 99;", "see-99");
    p1.quit();
    p2.quit();
    verify(&ckpt, &[1, 99]);
    cleanup(&ckpt);
}

#[test]
fn kill9_mid_insert_then_others_continue() {
    let dir = scratch("killwriter");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
        "p1-acked",
    );
    p1.kill(); // SIGKILL while "the insert loop" is between statements
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    p2.marker("INSERT INTO t VALUES (2);", "p2-acked");
    p2.quit();
    let info = verify(&ckpt, &[1, 2]);
    assert_eq!(
        info.replayed_records, 3,
        "CREATE + one insert each, no duplicates"
    );
    cleanup(&ckpt);
}

#[test]
fn checkpoint_by_p1_not_losing_p2_commits() {
    let dir = scratch("ckptnloss");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker("CREATE TABLE t (a INTEGER);", "ready");
    p2.open("db.pink");
    // P2 commits rows while P1 is idle (P1's view is stale).
    p2.marker(
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);",
        "p2-rows",
    );
    // P1 checkpoints without any intervening statement: the refresh-first
    // rule must fold P2's committed frames into the published checkpoint.
    p1.marker("checkpoint;", "p1-checkpointed");
    assert!(!has_gen_roll(&ckpt), "checkpoint completed cleanly");
    // P1 still sees the rows (its refreshed state) and can append on top.
    p1.expect_marker("SELECT 'p1-sees-2' AS m FROM t WHERE a = 2;", "p1-sees-2");
    p1.marker("INSERT INTO t VALUES (3);", "p1-appended");
    p1.quit();
    p2.quit();
    let info = verify(&ckpt, &[1, 2, 3]);
    assert!(
        info.replayed_records <= 1,
        "the checkpoint folded the pre-checkpoint rows; got {}",
        info.replayed_records
    );
    cleanup(&ckpt);
}

#[test]
fn generation_reload_is_transparent() {
    let dir = scratch("genreload");
    let ckpt = dir.join("db.pink");
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    p2.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
        "p2-ready",
    );
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker("INSERT INTO t VALUES (2); checkpoint;", "p1-ckpt");
    // P2 was paused across P1's checkpoint: its cached WAL fd is stale (the
    // generation rolled). Its next statement reloads checkpoint + WAL.
    p2.expect_marker("SELECT 'reload-ok' AS m FROM t WHERE a = 2;", "reload-ok");
    p2.marker("INSERT INTO t VALUES (3);", "p2-after");
    p1.quit();
    p2.quit();
    verify(&ckpt, &[1, 2, 3]);
    cleanup(&ckpt);
}

#[test]
fn stale_writer_detects_checkpoint() {
    let dir = scratch("stalewriter");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
        "p1-first",
    );
    p2.open("db.pink");
    // P2 commits and checkpoints: P1's cached WAL fd now points at the old
    // generation. Its next append must detect the replacement and land in
    // the new WAL (never in the roll).
    p2.marker("INSERT INTO t VALUES (2); checkpoint;", "p2-ckpt");
    p1.marker("INSERT INTO t VALUES (3);", "p1-second");
    p1.quit();
    p2.quit();
    let info = verify(&ckpt, &[1, 2, 3]);
    assert_eq!(
        info.replayed_records, 1,
        "only P1's post-checkpoint record is live"
    );
    assert!(!has_gen_roll(&ckpt), "the roll was removed");
    cleanup(&ckpt);
}

#[test]
fn lock_timeout_reports_busy() {
    let dir = scratch("busy");
    let ckpt = dir.join("db.pink");
    let lock = dir.join("db.pink.lock");
    let mut p2 = Cli::spawn(&dir, Some(100));
    p2.open("db.pink");
    p2.marker("CREATE TABLE t (a INTEGER);", "created");
    // A foreign process holds the write lease (the flock is per open file
    // description, exactly what a second process's lease looks like). Wave
    // 45o: only the short commit/checkpoint spans take the lease, so the
    // holder must be external — an open transaction no longer holds it.
    let foreign = {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock)
            .unwrap();
        f.try_lock().unwrap();
        f
    };
    // P2's write executes optimistically, waits 100 ms for the lease, then
    // reports the database busy; the uncommitted effect fail-stops the
    // handle (it must never be observable).
    p2.send("INSERT INTO t VALUES (20);\n");
    let line = p2.expect_error("busy");
    assert!(line.contains("older pinkdb build"), "{line}");
    p2.send("SELECT 'ghost' AS m FROM t WHERE a = 20;\n");
    p2.expect_error("stopped after a WAL write failure");
    // The holder exits; a reopened handle writes normally (no ghost row).
    drop(foreign);
    p2.open("db.pink");
    p2.marker("INSERT INTO t VALUES (20);", "p2-retry");
    p2.quit();
    verify(&ckpt, &[20]);
    cleanup(&ckpt);
}

/// Wave 45o headline: two transactions are open at the same time on
/// disjoint tables; neither waits for the other, and both commit.
#[test]
fn overlapping_transactions_on_disjoint_tables_both_commit() {
    let dir = scratch("optdisjoint");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, Some(100)); // a short busy timeout proves no wait
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); CREATE TABLE u (a INTEGER);",
        "ready",
    );
    p2.open("db.pink");
    // Both transactions are open at the same time.
    p1.marker("BEGIN; INSERT INTO t VALUES (1);", "p1-txn");
    p2.marker("BEGIN; INSERT INTO u VALUES (2);", "p2-txn");
    // P1 commits while P2's transaction is still open: no busy, no wait.
    p1.marker("COMMIT;", "p1-committed");
    // P2 sees P1's committed row on its next statement, inside its own
    // transaction (repeatable reads are per-transaction, not per-process).
    // (Its own snapshot predates P1's commit, so it does not see row 1 yet;
    // the commit below must still succeed: it never read t.)
    p2.marker("COMMIT;", "p2-committed");
    p2.expect_marker("SELECT 'see-t' AS m FROM t WHERE a = 1;", "see-t");
    p1.quit();
    p2.quit();
    verify_tables(&ckpt, &[("t", &[1]), ("u", &[2])]);
    cleanup(&ckpt);
}

/// Wave 45o: a same-table conflict surfaces with the documented error (the
/// table and the conflicting frame's LSN), the aborted transaction logs
/// nothing, and a retry on the refreshed state commits.
#[test]
fn same_table_transaction_conflict_then_retry_succeeds() {
    let dir = scratch("optsame");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker("CREATE TABLE t (a INTEGER);", "ready");
    p2.open("db.pink");
    p1.marker("BEGIN; INSERT INTO t VALUES (1);", "p1-txn");
    p2.marker("BEGIN; INSERT INTO t VALUES (2);", "p2-txn");
    p1.marker("COMMIT;", "p1-committed");
    // P2's COMMIT conflicts and the engine aborts the transaction; the CLI
    // re-runs the buffered transaction on the refreshed state (bounded) and
    // reports the (first) conflict error.
    p2.send("COMMIT;\n");
    let line = p2.expect_error("write conflict");
    assert!(line.contains("table \"main.t\""), "{line}");
    assert!(line.contains("lsn"), "{line}");
    assert!(line.contains("retry"), "{line}");
    p2.wait_for("Retrying the transaction");
    p2.wait_for("Transaction committed.");
    p1.quit();
    p2.quit();
    verify(&ckpt, &[1, 2]);
    cleanup(&ckpt);
}

/// Wave 45o: a whole transaction sent as ONE string (`BEGIN; …; COMMIT;`)
/// that conflicts is retried by the CLI. A background committer keeps
/// writing the table P2's transaction will write; when P2's first attempt
/// reports the conflict, the test stops the committer, so the automatic
/// retry of the whole call (with the pre-BEGIN session restored) commits.
#[test]
fn one_call_transaction_conflict_is_retried_by_the_cli() {
    let dir = scratch("optonecall");
    let ckpt = dir.join("db.pink");
    // A read table heavy enough that P2's one-call transaction stays open
    // long enough for the committer to land a conflicting frame.
    let csv = dir.join("read.csv");
    {
        use std::io::Write as _;
        let mut f = std::fs::File::create(&csv).unwrap();
        writeln!(f, "a").unwrap();
        for i in 0..100_000 {
            writeln!(f, "{i}").unwrap();
        }
    }
    let mut setup = Cli::spawn(&dir, None);
    setup.open("db.pink");
    setup.marker("CREATE TABLE t (a BIGINT);", "setup");
    setup.send(&format!(".import {} r\n", csv.display()));
    setup.wait_for("Imported");
    setup.quit();
    // The background committer: autocommit inserts into t, one per line,
    // fed continuously until the conflict is observed. A fixed burst can
    // finish before P2's transaction window even opens (P2 spawns/opens
    // after it), so the conflict would never happen; a continuous feed
    // guarantees overlapping frames on any machine speed.
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    let (stop, feeder) = p1.feed_until_stopped("INSERT INTO t VALUES (-1);\n");
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    // P2's whole transaction as one input line.
    let reads = "SELECT COUNT(*) FROM r AS x JOIN r AS y ON x.a = y.a + 1; ".repeat(30);
    p2.send(&format!(
        "BEGIN; {reads} INSERT INTO t VALUES (77); COMMIT;\n"
    ));
    // P2's COMMIT conflicts (the committer's frames touch t, its write set):
    // the CLI reports it and re-runs the whole buffered call.
    let line = p2.expect_error("write conflict");
    assert!(line.contains("main.t"), "{line}");
    // Stop the committer first: the retry (which re-runs the reads) then sees
    // a quiescent store and commits within the bounded budget. The conflict
    // line is printed before the first retry starts, so killing here leaves
    // at most that retry exposed to a straggler frame.
    stop.store(true, Ordering::SeqCst);
    p1.kill();
    let _ = feeder.join();
    p2.wait_for("Retrying the transaction");
    p2.wait_for("Transaction committed.");
    p2.quit();
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(
        ints(&mut db, "SELECT COUNT(*) FROM t WHERE a = 77;"),
        vec![1],
        "the retried transaction's row exists exactly once"
    );
    assert_eq!(
        ints(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![ints(&mut db, "SELECT COUNT(*) FROM t WHERE a = -1;")[0] + 1],
        "only the committer's sentinel rows and the retried row exist"
    );
    assert_eq!(
        ints(&mut db, "SELECT COUNT(*) FROM t WHERE a <> -1 AND a <> 77;"),
        vec![0]
    );
    drop(db);
    cleanup(&ckpt);
}

/// Wave 45o (c): read-set validation. P1's write is computed from a read of
/// `t`; P2 commits `t` before P1's COMMIT, so P1 must conflict (its WAL
/// record would replay against different data than the session saw).
#[test]
fn read_set_conflict_aborts_the_committer() {
    let dir = scratch("optread");
    let ckpt = dir.join("db.pink");
    // No CLI retry: the aborted transaction must be observable as-is (the
    // re-execution semantics get their own test below).
    let mut p1 = Cli::spawn_with_retries(&dir, 0);
    let mut p2 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); CREATE TABLE u (a INTEGER);",
        "ready",
    );
    p2.open("db.pink");
    p1.marker("BEGIN; UPDATE u SET a = (SELECT MAX(a) FROM t);", "p1-txn");
    p2.marker("INSERT INTO t VALUES (50);", "p2-inserted");
    p1.send("COMMIT;\n");
    let line = p1.expect_error("write conflict");
    assert!(line.contains("main.t"), "{line}");
    p1.quit();
    p2.quit();
    verify_tables(&ckpt, &[("t", &[1, 50]), ("u", &[])]);
    cleanup(&ckpt);
}

/// Wave 45o (e): write skew is prevented. T1 reads t and writes x; T2 reads
/// x and writes t. Whole-snapshot isolation would let both commit and break
/// `x.a = t.a`; the read half of the frame validation aborts the second one.
#[test]
fn write_skew_is_prevented_across_processes() {
    let dir = scratch("optskew");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); \
         CREATE TABLE x (a INTEGER); INSERT INTO x VALUES (1);",
        "ready",
    );
    p2.open("db.pink");
    p1.marker(
        "BEGIN; UPDATE x SET a = 2 WHERE (SELECT MAX(a) FROM t) = 1;",
        "p1-txn",
    );
    p2.marker(
        "BEGIN; UPDATE t SET a = 2 WHERE (SELECT MAX(a) FROM x) = 1;",
        "p2-txn",
    );
    p1.marker("COMMIT;", "p1-committed");
    // The conflict aborts T2; the CLI's retry re-executes T2's UPDATE on the
    // refreshed state, where its premise (`MAX(x.a) = 1`) no longer holds, so
    // the rebased write affects zero rows and commits.
    p2.send("COMMIT;\n");
    let line = p2.expect_error("write conflict");
    assert!(
        line.contains("main.x") || line.contains("main.t"),
        "the crossed tables must be named: {line}"
    );
    p2.wait_for("Retrying the transaction");
    p2.wait_for("Transaction committed.");
    p1.quit();
    p2.quit();
    // x.a = 2 and t.a = 1: the invariant x.a = t.a held (the skew's second
    // write became a no-op instead of breaking it).
    verify_tables(&ckpt, &[("t", &[1]), ("x", &[2])]);
    cleanup(&ckpt);
}

/// Wave 45o (d): autocommit hammering on disjoint tables stays exactly-once
/// (the optimistic fast path; the fallback covers any conflict).
#[test]
fn autocommit_hammering_on_disjoint_tables_is_exactly_once() {
    let dir = scratch("ophammerdisjoint");
    let ckpt = dir.join("db.pink");
    let n_procs: i64 = 4;
    let per_proc: i64 = 100;
    let mut p0 = Cli::spawn(&dir, None);
    p0.open("db.pink");
    let mut setup = String::new();
    for proc in 0..n_procs {
        setup.push_str(&format!("CREATE TABLE t{proc} (a BIGINT);\n"));
    }
    p0.marker(&setup, "created");
    p0.quit();
    let mut procs = Vec::new();
    for proc in 0..n_procs {
        let mut p = Cli::spawn(&dir, None);
        p.open("db.pink");
        let mut script = String::new();
        for i in 0..per_proc {
            let v = proc * 1_000_000 + i;
            script.push_str(&format!("INSERT INTO t{proc} VALUES ({v});\n"));
        }
        script.push_str(&format!("SELECT 'proc-{proc}-done' AS m;\n"));
        p.send(&script);
        procs.push((proc, p));
    }
    for (proc, p) in procs.iter_mut() {
        p.wait_for(&format!("proc-{proc}-done"));
    }
    for (_, p) in procs {
        p.quit();
    }
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    let expected_sum: i64 = per_proc * (per_proc - 1) / 2;
    for proc in 0..n_procs {
        assert_eq!(
            ints(&mut db, &format!("SELECT COUNT(*) FROM t{proc};")),
            vec![per_proc],
            "table t{proc} row count"
        );
        assert_eq!(
            ints(&mut db, &format!("SELECT SUM(a) FROM t{proc};")),
            vec![proc * 1_000_000 * per_proc + expected_sum],
            "table t{proc} sum"
        );
    }
    cleanup(&ckpt);
}

/// Wave 45o (d) cont.: autocommit hammering on the *same* table stays
/// exactly-once too — conflicts take the pessimistic fallback and never
/// duplicate or lose an effect.
#[test]
fn autocommit_hammering_on_one_table_is_exactly_once() {
    let dir = scratch("ophammersame");
    let ckpt = dir.join("db.pink");
    let n_procs: i64 = 3;
    let per_proc: i64 = 60;
    let mut p0 = Cli::spawn(&dir, None);
    p0.open("db.pink");
    p0.marker("CREATE TABLE t (a BIGINT);", "created");
    p0.quit();
    let mut procs = Vec::new();
    for proc in 0..n_procs {
        let mut p = Cli::spawn(&dir, None);
        p.open("db.pink");
        let mut script = String::new();
        for i in 0..per_proc {
            let v = proc * 1_000_000 + i;
            script.push_str(&format!("INSERT INTO t VALUES ({v});\n"));
        }
        script.push_str(&format!("SELECT 'proc-{proc}-done' AS m;\n"));
        p.send(&script);
        procs.push((proc, p));
    }
    for (proc, p) in procs.iter_mut() {
        p.wait_for(&format!("proc-{proc}-done"));
    }
    for (_, p) in procs {
        p.quit();
    }
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(
        ints(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![n_procs * per_proc]
    );
    let expected_sum: i64 = (0..n_procs)
        .map(|p| p * 1_000_000 * per_proc + per_proc * (per_proc - 1) / 2)
        .sum();
    assert_eq!(ints(&mut db, "SELECT SUM(a) FROM t;"), vec![expected_sum]);
    cleanup(&ckpt);
}

/// Wave 45o (f): kill -9 during an open optimistic transaction leaves no
/// partial effect (the frames were never appended — the effect exists only
/// if COMMIT appended them).
#[test]
fn kill9_during_an_optimistic_transaction_leaves_no_effect() {
    let dir = scratch("optkill");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
        "ready",
    );
    // The transaction is open and its insert is acked *in memory* only.
    p1.marker("BEGIN; INSERT INTO t VALUES (99);", "txn-open");
    p1.kill();
    // A second process writes and commits normally; the killed
    // transaction's row never existed.
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    p2.marker("INSERT INTO t VALUES (2);", "p2-acked");
    // A committed-then-killed transaction's effect exists exactly once.
    p2.marker("BEGIN; INSERT INTO t VALUES (3); COMMIT;", "p2-committed");
    p2.kill();
    let info = verify(&ckpt, &[1, 2, 3]);
    assert_eq!(info.replayed_records, 4, "CREATE, row 1, row 2, txn row 3");
    cleanup(&ckpt);
}

/// Wave 45o: a transaction and an autocommit statement from another process
/// overlap on disjoint tables — neither blocks the other.
#[test]
fn optimistic_transaction_and_autocommit_overlap_disjointly() {
    let dir = scratch("optoverlap");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, Some(100));
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); CREATE TABLE u (a INTEGER);",
        "ready",
    );
    p2.open("db.pink");
    p1.marker("BEGIN; INSERT INTO t VALUES (1);", "p1-txn");
    // P2's autocommit write runs while P1's transaction is open.
    p2.marker("INSERT INTO u VALUES (2);", "p2-acked");
    p1.marker("COMMIT;", "p1-committed");
    p1.quit();
    p2.quit();
    verify_tables(&ckpt, &[("t", &[1]), ("u", &[2])]);
    cleanup(&ckpt);
}

#[test]
fn nproc_stress() {
    let dir = scratch("stress");
    let ckpt = dir.join("db.pink");
    let n_procs: i64 = 4;
    let per_proc: i64 = 200;
    let mut p0 = Cli::spawn(&dir, None);
    p0.open("db.pink");
    p0.marker("CREATE TABLE t (a BIGINT);", "created");
    p0.quit();
    // Every other process appends its own disjoint, verifiable value range.
    let mut procs = Vec::new();
    for proc in 0..n_procs {
        let mut p = Cli::spawn(&dir, None);
        p.open("db.pink");
        let mut script = String::new();
        for i in 0..per_proc {
            let v = proc * 1_000_000 + i;
            script.push_str(&format!("INSERT INTO t VALUES ({v});\n"));
        }
        script.push_str(&format!("SELECT 'proc-{proc}-done' AS m;\n"));
        p.send(&script);
        procs.push((proc, p));
    }
    for (proc, p) in procs.iter_mut() {
        p.wait_for(&format!("proc-{proc}-done"));
    }
    for (_, p) in procs {
        p.quit();
    }
    // A fresh process must see exactly the committed effects: count and sum.
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(
        ints(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![n_procs * per_proc]
    );
    let expected_sum: i64 = (0..n_procs)
        .map(|p| {
            let base = p * 1_000_000;
            per_proc * (per_proc - 1) / 2 + base * per_proc
        })
        .sum();
    assert_eq!(ints(&mut db, "SELECT SUM(a) FROM t;"), vec![expected_sum]);
    cleanup(&ckpt);
}

#[test]
fn old_v2_wal_upgrades_cleanly() {
    let dir = scratch("v2upgrade");
    let ckpt = dir.join("db.pink");
    // A v2 checkpoint (written by the current engine) + a hand-built v2 WAL.
    {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (0);").unwrap();
        db.save(ckpt.to_str().unwrap()).unwrap();
    }
    let wal = dir.join("db.pink.wal");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"PINKWAL1");
    bytes.extend_from_slice(&2u32.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    for (lsn, sql) in [
        (1u64, "INSERT INTO t VALUES (1);"),
        (2, "INSERT INTO t VALUES (2);"),
    ] {
        let payload = {
            let mut v = Vec::new();
            v.extend_from_slice(&1u32.to_le_bytes());
            v.extend_from_slice(&(sql.len() as u32).to_le_bytes());
            v.extend_from_slice(sql.as_bytes());
            v
        };
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&lsn.to_le_bytes());
        bytes.extend_from_slice(&pinkdb::wal::crc32(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
    }
    std::fs::write(&wal, &bytes).unwrap();

    let mut cli = Cli::spawn(&dir, None);
    cli.open("db.pink");
    cli.expect_marker("SELECT 'v2-rows' AS m FROM t WHERE a = 2;", "v2-rows");
    cli.marker("INSERT INTO t VALUES (3);", "post-upgrade");
    cli.quit();

    // The WAL is v4 now: version 4, a nonzero wal_id, records preserved.
    let data = std::fs::read(&wal).unwrap();
    assert_eq!(&data[..8], b"PINKWAL1");
    assert_eq!(u32::from_le_bytes(data[8..12].try_into().unwrap()), 4);
    assert_ne!(&data[20..36], [0u8; 16], "fresh wal_id");
    verify(&ckpt, &[0, 1, 2, 3]);
    cleanup(&ckpt);
}

#[test]
fn mixed_old_build_is_busy_or_refuses() {
    let dir = scratch("oldbuild");
    let ckpt = dir.join("db.pink");
    // Create the database once so the lock file and WAL exist.
    {
        let mut db = PinkDb::open_durable(ckpt.to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    }
    // Simulate the old build's lifetime lock by holding the flock from this
    // process (flock is per open file description, exactly like a foreign
    // process).
    let foreign = {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(format!("{}.lock", ckpt.display()))
            .unwrap();
        f.try_lock().unwrap();
        f
    };
    let before = snapshot_dir(&dir);
    let mut cli = Cli::spawn(&dir, Some(100));
    cli.send(".open db.pink\n");
    cli.expect_error("busy");
    // Nothing may have been mutated while the lease could not be taken.
    assert_eq!(snapshot_dir(&dir), before, "the refused open mutated files");
    // After the old holder exits, the same CLI opens normally.
    drop(foreign);
    cli.open("db.pink");
    cli.quit();
    cleanup(&ckpt);
}

/// File-name + size map of one directory (mutation check).
fn snapshot_dir(dir: &Path) -> Vec<(String, u64)> {
    let mut out: Vec<(String, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| {
            (
                e.file_name().to_string_lossy().to_string(),
                e.metadata().map(|m| m.len()).unwrap_or(0),
            )
        })
        .collect();
    out.sort();
    out
}

// ---------- wave 45n: lock-file identity (real second process) ----------

/// Wave 45n: delete + recreate `<db>.pink.lock` while a real second process
/// holds its lease. The holding process must fail its next lease-taking
/// operation closed (clear error, zero WAL bytes); a fresh process that locks
/// the new file may proceed; the final state is exactly the union of
/// acknowledged commits of the process that held the valid lease.
#[test]
fn lock_file_replacement_fails_closed_across_processes() {
    let dir = scratch("lockreplace");
    let ckpt = dir.join("db.pink");
    let lock = dir.join("db.pink.lock");
    let wal = dir.join("db.pink.wal");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
        "p1-ready",
    );

    // A foreign process replaces the lock file (fresh inode); p1's fd still
    // holds the old, unlinked one.
    let wal_before = std::fs::read(&wal).unwrap();
    std::fs::remove_file(&lock).unwrap();
    std::fs::write(&lock, b"").unwrap();

    // Idle: the next write refuses before writing anything.
    p1.send("INSERT INTO t VALUES (2);\n");
    p1.expect_error("deleted or replaced");
    assert_eq!(
        std::fs::read(&wal).unwrap(),
        wal_before,
        "the refused write must not have touched the WAL"
    );

    // A fresh process locks the *new* lock file and continues cleanly.
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    p2.marker("INSERT INTO t VALUES (3);", "p2-acked");

    // p1 still fails closed on the stray inode (never a second writer).
    p1.send("INSERT INTO t VALUES (4);\n");
    p1.expect_error("deleted or replaced");
    p1.quit();
    p2.quit();

    // Exactly p1's pre-replacement commit and p2's post-replacement commit.
    let info = verify(&ckpt, &[1, 3]);
    assert_eq!(info.replayed_records, 3, "CREATE, row 1, row 3");
    cleanup(&ckpt);
}

/// Wave 45n/45o: replacement while p1 is mid-transaction. The transaction
/// holds no lease while open (wave 45o), but its COMMIT takes the lease and
/// must fail closed; the uncommitted row must never be durable, and the
/// process on the new lock file continues normally.
#[test]
fn lock_file_replacement_mid_transaction_fails_closed_across_processes() {
    let dir = scratch("lockreptxn");
    let ckpt = dir.join("db.pink");
    let lock = dir.join("db.pink.lock");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker("CREATE TABLE t (a INTEGER);", "ready");
    // p1's transaction is open with a buffered insert (no lease held).
    p1.marker("BEGIN; INSERT INTO t VALUES (99);", "p1-txn");
    std::fs::remove_file(&lock).unwrap();
    std::fs::write(&lock, b"").unwrap();
    // p2 locks the fresh file and commits normally.
    let mut p2 = Cli::spawn(&dir, None);
    p2.open("db.pink");
    p2.marker("INSERT INTO t VALUES (3);", "p2-acked");
    // p1's COMMIT refuses at the WAL mutation boundary.
    p1.send("COMMIT;\n");
    p1.expect_error("deleted or replaced");
    p1.quit();
    p2.quit();
    // The uncommitted 99 is gone; p2's commit is exactly once.
    let info = verify(&ckpt, &[3]);
    assert_eq!(info.replayed_records, 2, "CREATE + row 3");
    cleanup(&ckpt);
}

/// Wave 45n gap (d): `save_snapshot` refuses a target whose lease a REAL
/// second process holds — not a foreign flock from the test process.
/// Wave 45o: `.save <target>` while another process has an open transaction
/// proceeds (transactions hold no lease), and the *replaced checkpoint*
/// aborts the open transaction's COMMIT instead — a frame appended after the
/// snapshot would replay on top of a different database.
#[test]
fn save_snapshot_replacing_the_checkpoint_conflicts_an_open_transaction() {
    let dir = scratch("optsnap");
    let ckpt = dir.join("db.pink");
    // No CLI retry: the replaced checkpoint must surface as the conflict,
    // not be retried into a different database.
    let mut p1 = Cli::spawn_with_retries(&dir, 0);
    p1.open("db.pink");
    // Checkpoint first: the target WAL is empty, so the snapshot's live-WAL
    // guard is not what decides the outcome.
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); checkpoint;",
        "p1-ready",
    );
    p1.marker("BEGIN; INSERT INTO t VALUES (2);", "p1-txn");
    // P2 is bound to nothing: `.save db.pink` snapshots its (empty) store
    // over the target. It succeeds — P1's open transaction holds no lease.
    let mut p2 = Cli::spawn(&dir, None);
    p2.send(".save db.pink\n");
    p2.wait_for("Saved");
    // P1's COMMIT now conflicts: the checkpoint file was replaced.
    p1.send("COMMIT;\n");
    let line = p1.expect_error("write conflict");
    assert!(line.contains("checkpoint file"), "{line}");
    p1.quit();
    p2.quit();
    // The aborted transaction never appended: the durable pair is exactly
    // P2's snapshot (an empty `main`) — a fresh durable open replays cleanly
    // instead of hitting an insert against a vanished table.
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert!(
        db.execute("SELECT COUNT(*) FROM t;").is_err(),
        "the snapshot replaced the target with p2's empty store"
    );
    drop(db);
    cleanup(&ckpt);
}

/// Wave 45o: a real external lease holder (a foreign flock — the same
/// in-process simulation the wave-45n tests use for a second process) still
/// makes `.save <target>` refuse; the refusal is about the lease, not the
/// file state.
#[test]
fn save_snapshot_refuses_a_foreign_lease_holder() {
    let dir = scratch("snaplease");
    let ckpt = dir.join("db.pink");
    let lock = dir.join("db.pink.lock");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); checkpoint;",
        "p1-ready",
    );
    let foreign = {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock)
            .unwrap();
        f.try_lock().unwrap();
        f
    };
    let mut p2 = Cli::spawn(&dir, None);
    p2.send(".save db.pink\n");
    let line = p2.expect_error("in use by another handle or process");
    assert!(
        line.contains("in use by another handle or process"),
        "{line}"
    );
    // Once the holder exits, the same snapshot proceeds.
    drop(foreign);
    let before = std::fs::read(&ckpt).unwrap();
    p2.send(".save db.pink\n");
    p2.wait_for("Saved");
    assert_ne!(before, std::fs::read(&ckpt).unwrap());
    p2.quit();
    p1.kill();
    cleanup(&ckpt);
}

/// Compile `crates/pinkdb-ext-loader/tests/c/<name>.c` with the system `cc`
/// against the ABI header — the same real extension fixture the ext-loader
/// suite uses.
fn compile_test_extension(dir: &Path, name: &str) -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = manifest
        .join("../pinkdb-ext-loader/tests/c")
        .join(format!("{name}.c"));
    let include = manifest.join("../pinkdb-ext-loader/include");
    let out = dir.join(format!("{name}{}", std::env::consts::DLL_SUFFIX));
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let status = Command::new(&cc)
        .arg("-std=c99")
        .arg("-O1")
        .arg("-fPIC")
        .arg("-shared")
        .arg("-I")
        .arg(&include)
        .arg("-o")
        .arg(&out)
        .arg(&source)
        .status()
        .expect("run the C compiler (set CC?)");
    assert!(status.success(), "compiling {name}.c failed");
    out.to_str().expect("UTF-8 path").to_string()
}

/// Wave 45n gap (b), real processes + a REAL extension: refresh replay of a
/// `LOAD EXTENSION`-prefixed record must succeed even though this process
/// already has the extension mapped from its own earlier replay. This is the
/// cross-process H5 bug: before the fix, b's refresh died with "extension
/// \"test_ext\" is already loaded".
#[test]
fn refresh_replay_reloads_a_real_extension_across_processes() {
    let dir = scratch("extrefresh");
    let ext = compile_test_extension(&dir, "test_ext");
    let ckpt = dir.join("db.pink");
    // b opens first (empty database): its next statements must refresh.
    let mut b = Cli::spawn(&dir, None);
    b.open("db.pink");
    let mut a = Cli::spawn(&dir, None);
    a.open("db.pink");
    a.marker(
        &format!(
            "LOAD EXTENSION '{ext}'; CREATE TABLE t (a INTEGER); \
             INSERT INTO t VALUES (ext_double_it(1));"
        ),
        "a-loaded",
    );
    // b replays LOAD+CREATE+INSERT on its next statement: its process-local
    // registry now has test_ext.
    b.expect_marker("SELECT 'b-sees-2' AS m FROM t WHERE a = 2;", "b-sees-2");
    // a checkpoints (rolling the LOAD record away) and writes a record that
    // must be LOAD-prefixed again.
    a.marker("checkpoint;", "a-ckpt");
    a.marker("INSERT INTO t VALUES (ext_double_it(2));", "a-insert2");
    // b's refresh (full rebuild: new generation) replays the prefixed record
    // while test_ext is already loaded in b's process.
    b.expect_marker("SELECT 'b-sees-4' AS m FROM t WHERE a = 4;", "b-sees-4");
    b.marker("INSERT INTO t VALUES (ext_double_it(3));", "b-insert");
    a.quit();
    b.quit();
    // A fresh process replays the whole generation: the prefixed record loads
    // the extension exactly once; every row is present exactly once.
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![2, 4, 6]);
    cleanup(&ckpt);
}

/// Wave 45n gap (c), with the real pink-lake fixture: an attached lake
/// survives a full refresh rebuild triggered by another process's checkpoint
/// (H10: attachments are process-local and the rebuild must preserve them).
#[cfg(feature = "lake")]
#[test]
fn attached_lake_survives_a_full_rebuild() {
    let dir = scratch("attachrebuild");
    let lake = dir.join("lake");
    let ckpt = dir.join("db.pink");

    // Build a real lake fixture (catalog + data files) from an in-memory
    // store, in its own database named `lake`.
    let mut mk = Cli::spawn(&dir, None);
    mk.send(&format!(
        "CREATE DATABASE lake;\nUSE lake;\nCREATE TABLE lt (i INTEGER);\n\
         INSERT INTO lt VALUES (1), (2), (3);\n.lake save {}\n",
        lake.display()
    ));
    mk.wait_for("Saved lake");
    mk.quit();

    // The durable database those two processes share.
    let mut a = Cli::spawn(&dir, None);
    a.open("db.pink");
    a.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (10);",
        "a-ready",
    );

    // b opens before a's checkpoint and attaches the lake read-only.
    let mut b = Cli::spawn(&dir, None);
    b.open("db.pink");
    b.send(&format!(".lake attach {}\n", lake.display()));
    b.wait_for("Attached");
    b.expect_marker(
        "SELECT 'attached-3' AS m FROM lake.lt WHERE i = 3;",
        "attached-3",
    );

    // a commits and checkpoints: b's next statement refreshes with a full
    // rebuild (new WAL incarnation + new checkpoint identity).
    a.marker("INSERT INTO t VALUES (11); checkpoint;", "a-ckpt");
    b.expect_marker(
        "SELECT 'still-attached' AS m FROM lake.lt WHERE i = 3;",
        "still-attached",
    );
    // The attachment still answers every query shape, and the durable side
    // of the rebuild is right too.
    b.expect_marker(
        "SELECT 'attached-count' AS m FROM lake.lt HAVING COUNT(*) = 3;",
        "attached-count",
    );
    b.expect_marker("SELECT 'sees-11' AS m FROM t WHERE a = 11;", "sees-11");
    // `.lake attached` still lists it after the rebuild.
    b.send(".lake attached\n");
    b.wait_for("lake.lt");
    a.quit();
    b.quit();
    cleanup(&ckpt);
}

// ---------- M4 scenario: kill in the checkpoint window with a live writer ----------

#[test]
fn kill9_in_checkpoint_window_with_active_writer() {
    let dir = scratch("ckptwindow");
    let ckpt = dir.join("db.pink");
    let path = ckpt.to_str().unwrap().to_string();
    let big = "z".repeat(4_000_000);
    // Wider checkpoint window: a large committed state makes `.tmp` slow.
    let setup = format!(
        ".open {path}\n\
         CREATE TABLE t (a INTEGER, v VARCHAR);\n\
         INSERT INTO t VALUES (1, '{big}');\n\
         INSERT INTO t VALUES (2, '{big}');\n\
         SELECT 'setup-ok' AS m;\n"
    );
    let mut setup_cli = Cli::spawn(&dir, None);
    setup_cli.send(&setup);
    setup_cli.marker("SELECT 'setup-ok' AS m;", "setup-ok");
    setup_cli.quit();

    let mut captured = false;
    for _attempt in 0..10 {
        for f in std::fs::read_dir(&dir).unwrap() {
            let _ = std::fs::remove_file(f.unwrap().path());
        }
        let mut p1 = Cli::spawn(&dir, None);
        p1.send(&setup);
        p1.wait_for("setup-ok");
        // A live second process (with its own view) while P1 checkpoints.
        let mut p2 = Cli::spawn(&dir, None);
        p2.open("db.pink");
        p1.send("checkpoint;\n");
        // The dangerous image: `.tmp` complete, the WAL already rolled, the
        // publish not yet done.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut hit = false;
        while std::time::Instant::now() < deadline {
            let tmp = dir.join("db.pink.tmp");
            if tmp.exists() && has_gen_roll(&ckpt) && !ckpt.exists() {
                hit = true;
                break;
            }
            if p1.child.try_wait().unwrap().is_some() {
                break;
            }
        }
        if hit && dir.join("db.pink.tmp").exists() && has_gen_roll(&ckpt) && !ckpt.exists() {
            p1.kill();
            // The live writer continues: its write takes the lease (released
            // by the kernel when P1 died) after recovering the crash state.
            p2.marker("INSERT INTO t VALUES (3, 'after-crash');", "p2-after");
            p2.quit();
            captured = true;
            break;
        }
        p1.kill();
        p2.kill();
    }
    assert!(captured, "never caught the checkpoint publish window");

    // The live writer (P2) recovered the crash state under its own lease:
    // the `.tmp` was published, so the checkpoint holds rows 1+2 and only
    // row 3 lives in the WAL. A fresh open sees exactly that, and a second
    // open must be idempotent.
    assert!(ckpt.exists(), "the recovered checkpoint is durable");
    assert!(!dir.join("db.pink.tmp").exists(), ".tmp was consumed");
    assert!(!has_gen_roll(&ckpt), "the roll was removed");
    let (mut db, info) = PinkDb::open_durable_with(path.as_str(), SyncPolicy::Sync).unwrap();
    assert_eq!(info.checkpoints_completed, 0, "P2's recovery published it");
    assert_eq!(info.replayed_records, 1, "only row 3 was post-checkpoint");
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
    drop(db);
    let (mut again, info) = PinkDb::open_durable_with(path.as_str(), SyncPolicy::Sync).unwrap();
    assert_eq!(info.checkpoints_completed, 0);
    assert_eq!(info.replayed_records, 1);
    assert_eq!(
        ints(&mut again, "SELECT a FROM t ORDER BY a;"),
        vec![1, 2, 3]
    );
    drop(again);
    cleanup(&ckpt);
}
