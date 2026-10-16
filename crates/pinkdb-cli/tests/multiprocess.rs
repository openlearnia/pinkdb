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
use std::sync::atomic::{AtomicU64, Ordering};

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
    stdin: std::process::ChildStdin,
    reader: BufReader<std::io::PipeReader>,
    out: String,
}

impl Cli {
    fn spawn(cwd: &Path, busy_timeout_ms: Option<u64>) -> Cli {
        let (reader, writer) = std::io::pipe().expect("create pipe");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
        cmd.current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(writer.try_clone().expect("clone pipe writer"))
            .stderr(writer);
        if let Some(ms) = busy_timeout_ms {
            cmd.env("PINKDB_BUSY_TIMEOUT_MS", ms.to_string());
        }
        let mut child = cmd.spawn().expect("spawn pinkdb CLI");
        let stdin = child.stdin.take().expect("child stdin");
        Cli {
            child,
            stdin,
            reader: BufReader::new(reader),
            out: String::new(),
        }
    }

    fn open(&mut self, path: &str) {
        self.send(&format!(".open {path}\n"));
        self.wait_for("Opened");
    }

    fn send(&mut self, sql: &str) {
        self.stdin
            .write_all(sql.as_bytes())
            .expect("write to CLI stdin");
        self.stdin.flush().expect("flush CLI stdin");
    }

    fn next_line(&mut self) -> String {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).expect("read CLI output");
        assert!(
            n > 0,
            "CLI exited before the expected output; collected output:\n{}",
            self.out
        );
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
    // P1 opens a transaction (holds the lease, buffered inserts only).
    p1.wait_for_marker_after_begin();
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

impl Cli {
    /// `BEGIN; INSERT; marker` — leaves the process holding the lease.
    fn wait_for_marker_after_begin(&mut self) {
        self.marker("BEGIN; INSERT INTO t VALUES (99);", "txn-open");
    }
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
    let mut p2 = Cli::spawn(&dir, Some(100));
    p2.open("db.pink");
    p2.marker("CREATE TABLE t (a INTEGER);", "created");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    // P1 holds a transaction (and with it the write lease).
    p1.wait_for_marker_after_begin();
    // P2's write waits 100 ms, then reports the database busy.
    p2.send("INSERT INTO t VALUES (20);\n");
    p2.expect_error("busy");
    // P2's reads still work while the lease is held.
    p2.expect_absent("SELECT 'ghost' AS m FROM t WHERE a = 20;", "ghost");
    // P1 commits, releasing the lease; the retry succeeds.
    p1.marker("COMMIT;", "p1-committed");
    p2.marker("INSERT INTO t VALUES (20);", "p2-retry");
    p1.quit();
    p2.quit();
    verify(&ckpt, &[20, 99]);
    cleanup(&ckpt);
}

#[test]
fn long_transaction_serializes_fully() {
    let dir = scratch("longtxn");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    let mut p2 = Cli::spawn(&dir, Some(100));
    p1.open("db.pink");
    p1.marker("CREATE TABLE t (a INTEGER);", "ready");
    p2.open("db.pink");
    // P1 holds a transaction; P2's BEGIN cannot proceed (busy with the short
    // timeout) — proof the transaction serializes other writers.
    p1.marker("BEGIN; INSERT INTO t VALUES (1);", "p1-txn");
    p2.send("BEGIN; INSERT INTO t VALUES (2); COMMIT;\n");
    p2.expect_error("busy");
    // Commit P1; the retried transaction then runs and both effects apply
    // exactly once.
    p1.marker("COMMIT;", "p1-committed");
    p2.marker("BEGIN; INSERT INTO t VALUES (2); COMMIT;", "p2-committed");
    p1.quit();
    p2.quit();
    verify(&ckpt, &[1, 2]);
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

    // The WAL is v3 now: version 3, a nonzero wal_id, records preserved.
    let data = std::fs::read(&wal).unwrap();
    assert_eq!(&data[..8], b"PINKWAL1");
    assert_eq!(u32::from_le_bytes(data[8..12].try_into().unwrap()), 3);
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

/// Wave 45n: replacement while p1 is mid-transaction (holding the lease):
/// its COMMIT must fail closed, the uncommitted row must never be durable,
/// and the process on the new lock file continues normally.
#[test]
fn lock_file_replacement_mid_transaction_fails_closed_across_processes() {
    let dir = scratch("lockreptxn");
    let ckpt = dir.join("db.pink");
    let lock = dir.join("db.pink.lock");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    p1.marker("CREATE TABLE t (a INTEGER);", "ready");
    // p1 holds the lease (BEGIN) with a buffered insert.
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
#[test]
fn save_snapshot_refuses_a_lease_held_by_a_real_process() {
    let dir = scratch("snaplease");
    let ckpt = dir.join("db.pink");
    let mut p1 = Cli::spawn(&dir, None);
    p1.open("db.pink");
    // Checkpoint first so the target WAL is empty and the WAL-records guard
    // cannot mask the lease guard.
    p1.marker(
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); checkpoint;",
        "p1-ready",
    );
    // p1 holds the lease for a whole transaction.
    p1.marker("BEGIN; INSERT INTO t VALUES (2);", "p1-txn");

    // p2 is bound to nothing: `.save db.pink` is a snapshot at the target.
    let mut p2 = Cli::spawn(&dir, None);
    p2.send(".save db.pink\n");
    let line = p2.expect_error("in use by another handle or process");
    assert!(
        line.contains("in use by another handle or process"),
        "{line}"
    );
    let target_before = std::fs::read(&ckpt).unwrap();

    // Once the real holder dies, the kernel releases the flock: the same
    // snapshot now proceeds (the target WAL was checkpointed empty, so no
    // other guard masks the lease check) — proof the first error came from
    // the live lease, not from a static file state.
    p1.kill();
    p2.send(".save db.pink\n");
    p2.wait_for("Saved");
    let target_after = std::fs::read(&ckpt).unwrap();
    assert_ne!(
        target_before, target_after,
        "the released lease let the snapshot proceed"
    );
    // The snapshot is p2's in-memory store: an empty `main` database.
    p2.quit();
    let (mut db, _) = PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert!(
        db.execute("SELECT COUNT(*) FROM t;").is_err(),
        "the snapshot replaced the target with p2's empty store"
    );
    drop(db);
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
