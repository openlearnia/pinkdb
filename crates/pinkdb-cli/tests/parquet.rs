//! The CLI's opt-in `parquet` feature (wave 70): `read_parquet`,
//! `parquet_scan`, the introspection functions, DuckDB's replacement scan and
//! the `.parquet` counter command, driven through the real binary. The
//! default build prints the not-built message instead (pinned by the
//! `parquet_disabled` test below, which runs without the feature).
//!
//! Fixtures are the committed DuckDB-written files under
//! `bench/parquet_fixtures/`; the CLI runs with the repository root as its
//! working directory, exactly like `bench/duckdiff.py` does.

use std::process::{Command, Stdio};

mod common;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

/// stdout, stderr, exit code — separately.
fn run_split(stdin: &str) -> (String, String, Option<i32>) {
    use std::io::Write;
    let mut child = cli()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code(),
    )
}

/// The repository root, which is also the CLI's working directory (used by
/// the feature-gated tests, so absent from the lean build's test binary).
#[cfg(feature = "parquet")]
fn root() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .to_string()
}

#[cfg(feature = "parquet")]
#[test]
fn read_parquet_works_without_a_load_statement() {
    let sql = format!(
        "SELECT id, name, dec FROM read_parquet('{}/bench/parquet_fixtures/people.parquet') \
         WHERE id >= 6 ORDER BY id;\n.quit\n",
        root()
    );
    let (out, err, code) = run_split(&sql);
    assert_eq!(code, Some(0), "stderr: {err}");
    assert!(out.contains("name6"), "{out}");
    assert!(out.contains("name7"), "{out}");
    assert!(out.contains("DECIMAL(9,2)"), "{out}");
}

#[cfg(feature = "parquet")]
#[test]
fn replacement_scans_introspection_and_dot_parquet() {
    // A replacement scan, a hive glob, an introspection function and the
    // pushdown counters in one session.
    let sql = format!(
        "SELECT count(*) FROM '{root}/bench/parquet_fixtures/gzip.parquet';\n\
         SELECT id, region FROM read_parquet('{root}/bench/parquet_fixtures/hive/**/*.parquet') \
         WHERE region = 'eu' ORDER BY id;\n\
         SELECT num_rows, num_row_groups FROM parquet_file_metadata('{root}/bench/parquet_fixtures/people.parquet');\n\
         .parquet\n\
         .parquet reset\n\
         .parquet\n\
         .quit\n",
        root = root()
    );
    let (out, err, code) = run_split(&sql);
    assert_eq!(code, Some(0), "stderr: {err}");
    assert!(out.contains("eu"), "{out}");
    // The counters line names the source and ends zeroed after `reset`.
    assert!(out.contains("parquet\n  scans:"), "{out}");
    assert!(out.contains("parquet counters reset"), "{out}");
    assert!(out.contains("scans: 0"), "{out}");
    // The replacement scan of the gzip fixture read all 8 rows.
    assert!(
        out.contains("| 8 ") || out.contains("│ 8 "),
        "expected the gzip count: {out}"
    );
}

#[cfg(feature = "parquet")]
#[test]
fn explain_shows_the_pushed_filters() {
    let sql = format!(
        "EXPLAIN SELECT name FROM read_parquet('{}/bench/parquet_fixtures/people.parquet') \
         WHERE id > 6;\n.quit\n",
        root()
    );
    let (out, err, code) = run_split(&sql);
    assert_eq!(code, Some(0), "stderr: {err}");
    assert!(
        out.contains("TableFunction parquet: read_parquet("),
        "{out}"
    );
    assert!(out.contains("filters=[id > 6]"), "{out}");
}

#[cfg(feature = "parquet")]
#[test]
fn durable_replay_re_reads_the_parquet_file() {
    // A logged statement that reads a parquet file replays by re-reading it
    // (DuckDB's rule): the CLI registers the source process-wide at startup,
    // so `open_durable`'s replay resolves it. A file deleted before replay is
    // a hard error (pinned by the arrow CLI test's shape; here the fixture is
    // committed, so this test covers the successful re-read).
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-cli-parquet-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = dir.join("s.pink");
    // A copy of the committed fixture: the test never mutates `bench/`.
    let fixture = dir.join("people.parquet");
    std::fs::copy(
        format!("{}/bench/parquet_fixtures/people.parquet", root()),
        &fixture,
    )
    .unwrap();
    let sql = format!(
        ".open {store}\n\
         SELECT count(*) FROM read_parquet('{fixture}');\n\
         CREATE TABLE t AS SELECT id, name FROM read_parquet('{fixture}') WHERE id >= 6;\n\
         SELECT id, name FROM t ORDER BY id;\n\
         .quit\n",
        store = store.display(),
        fixture = fixture.display()
    );
    let (out, err, code) = run_split(&sql);
    assert_eq!(code, Some(0), "stdout: {out}\nstderr: {err}");
    assert!(out.contains("name6"), "{out}");
    assert!(out.contains("name7"), "{out}");

    // Reopen (a second process): the logged CTAS replays by re-reading the
    // file, exactly as before.
    let reopen = format!(
        ".open {store}\nSELECT id, name FROM t ORDER BY id;\n.quit\n",
        store = store.display()
    );
    let (out, err, code) = run_split(&reopen);
    assert_eq!(code, Some(0), "stdout: {out}\nstderr: {err}");
    assert!(out.contains("name7"), "{out}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(not(feature = "parquet"))]
#[test]
fn parquet_disabled_message() {
    let (out, err, _) = run_split("SELECT * FROM read_parquet('x.parquet');\n.quit\n");
    let merged = format!("{out}{err}");
    assert!(merged.contains("read_parquet"), "{merged}");
    let (out, err, _) = run_split(".parquet\n.quit\n");
    let merged = format!("{out}{err}");
    assert!(merged.contains("without the parquet feature"), "{merged}");
}
