//! CLI coverage for `.lake replicate …` (wave 89 T2): jobs between a source
//! lake (or the session store) and a writable destination lake, the manual
//! catch-up/status/tables surface, the session supervisor's start/stop, and
//! the error paths.
//!
//! This file only compiles when the `vortexlake` feature is ON (like
//! `tests/lake.rs`); the feature-less behavior is covered by
//! `tests/lake_disabled.rs`.

#![cfg(feature = "vortexlake")]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_replication_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn run(cmds: &str) -> String {
    let (out, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), cmds);
    common::assert_script_exit(status);
    out
}

#[test]
fn lake_source_create_catchup_status_tables_pause_resume_drop() {
    let dir = scratch("lake_source");
    let src = dir.join("src");
    let dest = dir.join("dest");

    let out = run(&format!(
        "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (100, 'a'), (101, 'b');\n\
         .lake save {src}\n\
         .quit\n",
        src = src.display()
    ));
    assert!(out.contains("Saved lake version"), "{out}");

    let out = run(&format!(
        ".lake replicate create {dest} from {src} every 1\n\
         .lake replicate catchup {dest} 1\n\
         .lake replicate status {dest}\n\
         .lake replicate tables {dest}\n\
         .lake replicate tables {dest} 1\n\
         .quit\n",
        dest = dest.display(),
        src = src.display()
    ));
    assert!(out.contains("Created replication job 1 on"), "{out}");
    assert!(
        out.contains(&format!("from lake:{}", src.display())),
        "the source location must be printed:\n{out}"
    );
    assert!(out.contains("(every 1s)"), "{out}");
    assert!(out.contains("Caught up replication job 1"), "{out}");
    assert!(out.contains("copied 1 tables / 2 rows"), "{out}");
    assert!(out.contains("status=created"), "{out}");
    assert!(out.contains("behind=0"), "{out}");
    assert!(
        out.contains("job 1  main.t -> main.t  strategy=snapshot"),
        "the per-table state must name the strategy:\n{out}"
    );
    assert!(out.contains("rows=2"), "{out}");

    // The destination is an ordinary lake: open and query it.
    let out = run(&format!(
        ".lake open {dest}\nSELECT i FROM t ORDER BY i;\n.quit\n",
        dest = dest.display()
    ));
    assert!(out.contains("Opened lake version"), "{out}");
    assert!(out.contains("100"), "{out}");
    assert!(out.contains("101"), "{out}");

    // A paused job refuses a manual catch-up; the status shows why.
    let out = run(&format!(
        ".lake replicate pause {dest} 1\n\
         .lake replicate status {dest}\n\
         .lake replicate catchup {dest} 1\n\
         .quit\n",
        dest = dest.display()
    ));
    assert!(out.contains("Paused replication job 1"), "{out}");
    assert!(out.contains("status=paused"), "{out}");
    assert!(out.contains("is paused - resume it"), "{out}");

    let out = run(&format!(
        ".lake replicate resume {dest} 1\n\
         .lake replicate catchup {dest} 1\n\
         .lake replicate resume-all {dest}\n\
         .lake replicate drop {dest} 1\n\
         .lake replicate status {dest}\n\
         .quit\n",
        dest = dest.display()
    ));
    assert!(out.contains("Resumed replication job 1"), "{out}");
    assert!(out.contains("Caught up replication job 1"), "{out}");
    assert!(out.contains("No paused or errored jobs"), "{out}");
    assert!(out.contains("Dropped replication job 1"), "{out}");
    assert!(out.contains("(no replication jobs)"), "{out}");
    cleanup(&dir);
}

#[test]
fn store_source_job_runs_through_catch_up_and_is_not_supervised() {
    let dir = scratch("store_source");
    let dest = dir.join("dest");

    let out = run(&format!(
        "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'a'), (2, 'b');\n\
         .lake replicate create {dest} from store:main every 1\n\
         .lake replicate catchup {dest} 1\n\
         .lake replicate status {dest}\n\
         .lake replicate tables {dest}\n\
         .lake replicate start {dest}\n\
         .lake replicate start {dest}\n\
         .lake replicate stop {dest}\n\
         .lake replicate pause {dest} 1\n\
         .lake replicate catchup {dest} 1\n\
         .lake replicate resume {dest} 1\n\
         .lake replicate catchup {dest} 1\n\
         .lake replicate resume-all {dest}\n\
         .lake replicate drop {dest} 1\n\
         .lake replicate status {dest}\n\
         .quit\n",
        dest = dest.display()
    ));
    assert!(out.contains("Created replication job 1 on"), "{out}");
    assert!(out.contains("from store:main"), "{out}");
    assert!(out.contains("Caught up replication job 1"), "{out}");
    assert!(out.contains("status=created"), "{out}");
    assert!(
        out.contains("behind=-"),
        "a store source has no version cursor:\n{out}"
    );
    assert!(
        out.contains("strategy=full"),
        "a store table without a primary key uses full:\n{out}"
    );
    assert!(out.contains("Started replication supervisor"), "{out}");
    assert!(
        out.contains("job 1 skipped: store source store:main is not supervised"),
        "start must report the store-source job as skipped:\n{out}"
    );
    assert!(out.contains("already running in this session"), "{out}");
    assert!(out.contains("Stopped replication supervisor"), "{out}");
    assert!(out.contains("Paused replication job 1"), "{out}");
    assert!(out.contains("is paused - resume it"), "{out}");
    assert!(out.contains("Resumed replication job 1"), "{out}");
    assert!(out.contains("No paused or errored jobs"), "{out}");
    assert!(out.contains("Dropped replication job 1"), "{out}");
    assert!(out.contains("(no replication jobs)"), "{out}");

    let out = run(&format!(
        ".lake open {dest}\nSELECT i FROM t ORDER BY i;\n.quit\n",
        dest = dest.display()
    ));
    assert!(out.contains("Opened lake version"), "{out}");
    assert!(out.contains("│ 1"), "{out}");
    assert!(out.contains("│ 2"), "{out}");
    assert!(out.contains("2 rows"), "{out}");
    cleanup(&dir);
}

#[test]
fn supervisor_start_stop_and_resume_all_lifecycle() {
    let dir = scratch("supervisor");
    let src = dir.join("src");
    let dest = dir.join("dest");

    let out = run(&format!(
        "CREATE TABLE t (i INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {src}\n\
         .lake replicate create {dest} from {src} every 3600\n\
         .quit\n",
        src = src.display(),
        dest = dest.display()
    ));
    assert!(out.contains("Created replication job 1"), "{out}");

    // `start` marks the job running; a second start is a note, not an error.
    // The supervisor is session-scoped, so start/status/stop share a process.
    let out = run(&format!(
        ".lake replicate start {dest}\n\
         .lake replicate start {dest}\n\
         .lake replicate status {dest}\n\
         .lake replicate stop {dest}\n\
         .lake replicate stop {dest}\n\
         .quit\n",
        dest = dest.display()
    ));
    assert!(out.contains("Started replication supervisor"), "{out}");
    assert!(out.contains("already running in this session"), "{out}");
    assert!(
        out.contains("status=running"),
        "start marks a lake-source job runnable:\n{out}"
    );
    assert!(out.contains("Stopped replication supervisor"), "{out}");
    assert!(
        out.contains("no replication supervisor"),
        "a second stop is a clean error:\n{out}"
    );
    cleanup(&dir);
}

#[test]
fn replicate_usage_errors_and_help_are_clear() {
    let dir = scratch("errors");
    let dest = dir.join("dest");

    let out = run(".lake replicate\n.quit\n");
    assert!(out.contains("Usage: .lake replicate create"), "{out}");
    assert!(out.contains(".lake replicate catchup <dest> <id>"), "{out}");

    let out = run(".lake replicate dunno\n.quit\n");
    assert!(
        out.contains("unknown .lake replicate subcommand \"dunno\""),
        "{out}"
    );

    // A create needs `from <source>`.
    let out = run(&format!(
        ".lake replicate create {dest}\n.quit\n",
        dest = dest.display()
    ));
    assert!(
        out.contains("Usage: .lake replicate create <dest> from"),
        "{out}"
    );

    // A destination cannot replicate from itself.
    let out = run(&format!(
        ".lake replicate create {dest} from {dest}\n.quit\n",
        dest = dest.display()
    ));
    assert!(
        out.contains("cannot create a replication job from"),
        "self-replication must be refused:\n{out}"
    );

    // Missing jobs are clean errors on every id-taking subcommand.
    run(&format!(
        ".lake save {dest}\n.quit\n",
        dest = dest.display()
    ));
    let out = run(&format!(
        ".lake replicate catchup {dest} 9\n\
         .lake replicate pause {dest} 9\n\
         .lake replicate resume {dest} 9\n\
         .lake replicate tables {dest} 9\n\
         .lake replicate drop {dest} 9\n\
         .quit\n",
        dest = dest.display()
    ));
    assert_eq!(
        out.matches("no replication job with id 9").count(),
        5,
        "catchup/pause/resume/tables/drop each name the missing job:\n{out}"
    );

    // Usage errors for malformed arguments.
    let out = run(&format!(
        ".lake replicate catchup {dest}\n\
         .lake replicate pause {dest} notanumber\n\
         .lake replicate tables {dest} notanumber\n\
         .lake replicate status {dest} extra\n\
         .quit\n",
        dest = dest.display()
    ));
    assert!(
        out.contains("Usage: .lake replicate catchup <dest> <id>"),
        "{out}"
    );
    assert!(
        out.contains("Usage: .lake replicate pause <dest> <id>"),
        "{out}"
    );
    assert!(
        out.contains("Usage: .lake replicate tables <dest> [<id>]"),
        "{out}"
    );
    assert!(
        out.contains("Usage: .lake replicate status <dest>"),
        "{out}"
    );

    // `.help` advertises the command in a lake build.
    let out = run(".help\n.quit\n");
    assert!(out.contains(".lake replicate create"), "{out}");
    assert!(out.contains(".lake replicate catchup|start|stop"), "{out}");
    cleanup(&dir);
}
