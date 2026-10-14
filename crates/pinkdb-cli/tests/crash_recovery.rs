//! End-to-end crash recovery for the REPL: a real `pinkdb` process commits
//! rows (some folded into a `.save` checkpoint, some WAL-only), is SIGKILLed
//! as soon as it confirms the work, and a fresh open must see every committed
//! row — and nothing that was still uncommitted.
//!
//! Determinism: the SQL commands are fed over stdin and run in order; the
//! marker query is the last line, so observing its output proves every
//! earlier statement returned — and with `SyncPolicy::Sync` returned means
//! fsynced. Only then is the process killed.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_crash_{tag}_{}_{}",
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

/// Spawn the REPL, feed `cmds` on stdin, wait for `marker` on stdout, then
/// SIGKILL the child (stdin is held open so it cannot exit on EOF first).
fn run_until_marker_then_kill(cmds: &str, marker: &str) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn pinkdb CLI");
    let mut stdin = child.stdin.take().expect("child stdin");
    let stdout = child.stdout.take().expect("child stdout");
    stdin
        .write_all(cmds.as_bytes())
        .expect("write commands to CLI stdin");

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let mut all = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).expect("read CLI stdout");
        assert!(
            n > 0,
            "CLI exited before marker {marker:?}; output was:\n{all}"
        );
        all.push_str(&line);
        if line.contains(marker) {
            break;
        }
    }
    child.kill().expect("SIGKILL the CLI"); // SIGKILL on Unix
    let status = child.wait().expect("reap the CLI");
    assert!(
        !status.success(),
        "a SIGKILLed process must not exit cleanly"
    );
    drop(stdin);
}

/// Run the REPL to a clean `.quit`/EOF exit and return its stdout.
fn run_to_exit(cmds: &str) -> String {
    run_to_exit_in(None, cmds)
}

/// Like [`run_to_exit`], with an explicit working directory.
fn run_to_exit_in(cwd: Option<&Path>, cmds: &str) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let (out, status) = common::run_merged(cmd, cmds);
    common::assert_script_exit(status);
    out
}

/// Any `<ckpt>.wal.<gen>.roll` next to `ckpt`?
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

/// APFS/NTFS treat `Aa` and `AA` as one entry; ext4 does not.
fn case_insensitive(dir: &Path) -> bool {
    let probe = dir.join("pinkdb_case_probe");
    let upper = dir.join("PINKDB_CASE_PROBE");
    if std::fs::write(&probe, b"x").is_err() {
        return false;
    }
    let same = upper.exists();
    let _ = std::fs::remove_file(&probe);
    same
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

#[test]
fn sigkill_keeps_checkpointed_and_wal_committed_rows() {
    let dir = scratch("committed");
    let ckpt = dir.join("crash.pink");
    let path = ckpt.to_str().unwrap();
    assert!(
        !path.contains(char::is_whitespace),
        "dot-commands split on whitespace"
    );

    // Rows 1-2 (one autocommit, one BEGIN..COMMIT) are folded into a `.save`
    // checkpoint; row 3 lands in the fresh WAL generation after it.
    let cmds = format!(
        ".open {path}\n\
         CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         BEGIN;\n\
         INSERT INTO t VALUES (2);\n\
         COMMIT;\n\
         .save {path}\n\
         INSERT INTO t VALUES (3);\n\
         SELECT 'crash-marker-ok' AS m;\n"
    );
    run_until_marker_then_kill(&cmds, "crash-marker-ok");

    // A second CLI run must replay the sidecar WAL on `.open`: the message
    // reports the record, and the query sees checkpointed + replayed rows.
    let out = run_to_exit(&format!(
        ".open {path}\nSELECT a FROM t ORDER BY a;\n.quit\n"
    ));
    assert!(
        out.contains("Replayed 1 WAL record(s)"),
        "CLI must replay the WAL:\n{out}"
    );
    assert!(
        out.contains("3 rows"),
        "checkpointed + WAL rows must be visible:\n{out}"
    );

    // Reopen in-process: checkpoint rows plus the one post-checkpoint WAL
    // record. Exactly one replay also proves `.save` checkpointed (rather
    // than only snapshotting) and rolled the WAL.
    let (mut db, info) = PinkDb::open_durable_with(path, SyncPolicy::Sync).unwrap();
    assert_eq!(info.replayed_records, 1, "only the post-checkpoint insert");
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
    cleanup(&ckpt);
}

#[test]
fn sigkill_mid_transaction_drops_uncommitted_rows() {
    let dir = scratch("uncommitted");
    let ckpt = dir.join("crash.pink");
    let path = ckpt.to_str().unwrap();

    // Row 1 commits; row 99 is only in the open transaction's memory when
    // the process dies — it must not come back.
    let cmds = format!(
        ".open {path}\n\
         CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         BEGIN;\n\
         INSERT INTO t VALUES (99);\n\
         SELECT 'txn-marker-ok' AS m;\n"
    );
    run_until_marker_then_kill(&cmds, "txn-marker-ok");

    let (mut db, info) = PinkDb::open_durable_with(path, SyncPolicy::Sync).unwrap();
    assert_eq!(
        info.replayed_records, 2,
        "CREATE + the committed insert only"
    );
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1]);
    cleanup(&ckpt);
}

/// C1 end-to-end: SIGKILL in the checkpoint window after the WAL roll and
/// before the `.pink` publish. Recovery must publish the complete `.tmp`, and
/// a second, clean open must still see every row — the old recovery replayed
/// the roll into memory, deleted `.tmp`/`.roll`, and lost it all.
#[test]
fn sigkill_in_checkpoint_publish_window_survives_repeated_opens() {
    let dir = scratch("ckptwindow");
    let ckpt = dir.join("crash.pink");
    let path = ckpt.to_str().unwrap().to_string();
    let tmp = dir.join("crash.pink.tmp");
    let big = "z".repeat(8_000_000);
    let cmds = format!(
        ".open {path}\n\
         CREATE TABLE t (a INTEGER, v VARCHAR);\n\
         INSERT INTO t VALUES (1, '{big}');\n\
         INSERT INTO t VALUES (2, '{big}');\n\
         .save {path}\n\
         .quit\n"
    );

    let mut captured = false;
    for _attempt in 0..10 {
        for f in std::fs::read_dir(&dir).unwrap() {
            let _ = std::fs::remove_file(f.unwrap().path());
        }
        let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn pinkdb CLI");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(cmds.as_bytes())
            .unwrap();
        // The dangerous image: `.tmp` written, the WAL already rolled, the
        // publish not yet done.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut hit = false;
        while std::time::Instant::now() < deadline {
            if tmp.exists() && has_gen_roll(&ckpt) && !ckpt.exists() {
                hit = true;
                break;
            }
            if child.try_wait().unwrap().is_some() {
                break;
            }
        }
        let _ = child.kill();
        child.wait().unwrap();
        if hit && tmp.exists() && has_gen_roll(&ckpt) && !ckpt.exists() {
            captured = true;
            break;
        }
    }
    assert!(captured, "never caught the checkpoint publish window");

    // Open #1: recovery publishes the checkpoint from `.tmp`.
    let (mut db, info) = PinkDb::open_durable_with(&path, SyncPolicy::Sync).unwrap();
    assert_eq!(info.checkpoints_completed, 1, "recovery must publish .tmp");
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    drop(db);

    // Open #2 (no crash in between): the rows must not vanish — this is the
    // step the old recovery failed.
    let (mut again, info) = PinkDb::open_durable_with(&path, SyncPolicy::Sync).unwrap();
    assert_eq!(info.replayed_records, 0);
    assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    drop(again);

    // Open #3 via the REPL, from a different working directory: same rows.
    let out = run_to_exit(&format!(
        ".open {path}\nSELECT a FROM t ORDER BY a;\n.quit\n"
    ));
    assert!(out.contains("2 rows"), "checkpoint must be durable:\n{out}");
    cleanup(&ckpt);
}

/// M1: `.save ./x.pink` after `.open x.pink` is the same file and must
/// checkpoint (WAL roll) — a plain snapshot would let the WAL replay rows
/// that are already in the snapshot, silently duplicating them.
#[test]
fn dot_slash_save_path_checkpoints_the_bound_database() {
    let dir = scratch("savedot");
    let ckpt = dir.join("x.pink");
    let out = run_to_exit_in(
        Some(&dir),
        ".open x.pink\n\
         CREATE TABLE t (a INTEGER);\n\
         .save x.pink\n\
         INSERT INTO t VALUES (1);\n\
         .save ./x.pink\n\
         INSERT INTO t VALUES (2);\n\
         .quit\n",
    );
    assert!(!out.contains("Error"), "{out}");
    let (mut db, info) =
        PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(
        info.replayed_records, 1,
        "only the INSERT after the last save"
    );
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    cleanup(&ckpt);
}

/// M1: the same for an absolute path when the database was opened relative.
#[test]
fn absolute_save_path_checkpoints_the_bound_database() {
    let dir = scratch("saveabs");
    let ckpt = dir.join("x.pink");
    let abs = ckpt.to_str().unwrap().to_string();
    assert!(!abs.contains(char::is_whitespace));
    let cmds = format!(
        ".open x.pink\n\
         CREATE TABLE t (a INTEGER);\n\
         .save x.pink\n\
         INSERT INTO t VALUES (1);\n\
         .save {abs}\n\
         INSERT INTO t VALUES (2);\n\
         .quit\n"
    );
    let out = run_to_exit_in(Some(&dir), &cmds);
    assert!(!out.contains("Error"), "{out}");
    let (mut db, info) = PinkDb::open_durable_with(&abs, SyncPolicy::Sync).unwrap();
    assert_eq!(info.replayed_records, 1);
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    cleanup(&ckpt);
}

/// M1: a case-only difference is the same file on APFS — `.save x.pink`
/// after `.open X.pink` must checkpoint, not snapshot.
#[test]
fn case_only_save_path_checkpoints_on_case_insensitive_fs() {
    let dir = scratch("savecase");
    let ckpt = dir.join("x.pink");
    if !case_insensitive(&dir) {
        cleanup(&ckpt);
        return;
    }
    let out = run_to_exit_in(
        Some(&dir),
        ".open X.pink\n\
         CREATE TABLE t (a INTEGER);\n\
         .save X.pink\n\
         INSERT INTO t VALUES (1);\n\
         .save x.pink\n\
         INSERT INTO t VALUES (2);\n\
         .quit\n",
    );
    assert!(!out.contains("Error"), "{out}");
    let (mut db, info) =
        PinkDb::open_durable_with(ckpt.to_str().unwrap(), SyncPolicy::Sync).unwrap();
    assert_eq!(info.replayed_records, 1);
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    cleanup(&ckpt);
}

/// M1: saving over a *different* database whose WAL still holds committed
/// records must be refused — otherwise its next open would replay those
/// records onto this snapshot (duplicating rows) or brick.
#[test]
fn save_over_another_database_with_a_live_wal_is_refused() {
    let dir = scratch("saveother");
    // y has committed rows that live only in its WAL (no checkpoint yet).
    run_to_exit_in(
        Some(&dir),
        ".open y.pink\nCREATE TABLE y (a INTEGER);\nINSERT INTO y VALUES (10);\n.quit\n",
    );
    let y = dir.join("y.pink");
    assert!(!y.exists() && dir.join("y.pink.wal").exists());

    let out = run_to_exit_in(
        Some(&dir),
        ".open x.pink\n\
         CREATE TABLE x (a INTEGER);\n\
         INSERT INTO x VALUES (1);\n\
         .save y.pink\n\
         .quit\n",
    );
    assert!(
        out.contains("Error") && out.contains("committed WAL records"),
        "the save must be refused:\n{out}"
    );
    assert!(!y.exists(), "the refused save must not write y.pink");

    // y is intact: its WAL still replays and x was never written into it.
    let out = run_to_exit_in(Some(&dir), ".open y.pink\nSELECT a FROM y;\n.quit\n");
    assert!(out.contains("10"), "y's rows must survive:\n{out}");
    let out = run_to_exit_in(Some(&dir), ".open y.pink\nSELECT a FROM x;\n.quit\n");
    assert!(
        out.contains("not found"),
        "y must not grow x's tables:\n{out}"
    );
    cleanup(&y);
}

/// M2: a durable `.import` must survive a SIGKILL right after it acks. The
/// import has no SQL text to log, so it is made durable by a checkpoint.
#[test]
fn import_is_durable_across_sigkill_after_ack() {
    let dir = scratch("importkill");
    let csv = dir.join("data.csv");
    std::fs::write(&csv, "a\n1\n2\n3\n").unwrap();
    let ckpt = dir.join("i.pink");
    let path = ckpt.to_str().unwrap().to_string();

    let cmds = format!(
        ".open {path}\n.import {} t\nSELECT 'import-ack' AS m;\n",
        csv.display()
    );
    run_until_marker_then_kill(&cmds, "import-ack");

    let (mut db, _) = PinkDb::open_durable_with(&path, SyncPolicy::Sync).unwrap();
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
    drop(db);
    cleanup(&ckpt);
}

/// M2: import + later logged DML + SIGKILL must reopen cleanly (no "table
/// not found" brick) with both the imported rows and the DML applied once.
#[test]
fn import_then_logged_dml_survives_sigkill_without_bricking() {
    let dir = scratch("importdml");
    let csv = dir.join("data.csv");
    std::fs::write(&csv, "a\n1\n2\n3\n").unwrap();
    let ckpt = dir.join("i.pink");
    let path = ckpt.to_str().unwrap().to_string();

    let cmds = format!(
        ".open {path}\n\
         .import {} t\n\
         INSERT INTO t VALUES (100);\n\
         SELECT 'import-dml-ack' AS m;\n",
        csv.display()
    );
    run_until_marker_then_kill(&cmds, "import-dml-ack");

    let (mut db, info) = PinkDb::open_durable_with(&path, SyncPolicy::Sync).unwrap();
    assert_eq!(info.replayed_records, 1, "only the post-import INSERT");
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![1, 2, 3, 100]
    );
    drop(db);
    cleanup(&ckpt);
}
