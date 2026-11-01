//! The CLI's default `math` feature (wave 54): the core math extension is
//! published in the linked registry at startup and loaded into every store
//! the session creates or opens, so `exp`/`log` work without a `LOAD
//! EXTENSION` statement. (`random()` became a core built-in in wave 139b, so
//! the extension no longer declares it.) `--no-default-features` prints the
//! not-built message instead (pinned by `--no-default-features` CI runs, not
//! here).
#![cfg(feature = "math")]

use std::process::{Command, Stdio};

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

/// stdout, stderr, exit code — separately.
fn run_split(stdin: &str) -> (String, String, Option<i32>) {
    run_split_args(&[], stdin)
}

/// [`run_split`] with command-line arguments.
fn run_split_args(args: &[&str], stdin: &str) -> (String, String, Option<i32>) {
    use std::io::Write;
    let mut child = cli()
        .args(args)
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

#[test]
fn math_functions_work_without_a_load_statement() {
    let (out, err, code) = run_split(
        "SELECT exp(1), log(100), log(2, 8), sin(0), atan2(1, 1), pi();\nSELECT sqrt(4), pow(2, 10);\n",
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("2.718281828459045"), "{out}");
    assert!(out.contains("3.0"), "{out}");
    assert!(out.contains("1024.0"), "{out}");
    // The statement is off by default: it loads and runs native code.
    let (_, err, code) = run_split("LOAD EXTENSION 'math';\n");
    assert_eq!(code, Some(1));
    assert!(err.contains("LOAD EXTENSION is disabled"), "{err}");
    // With the opt-in, loading the built-in extension explicitly is the
    // documented duplicate.
    let (_, err, code) = run_split_args(&["--allow-load-extension"], "LOAD EXTENSION 'math';\n");
    assert_eq!(code, Some(1));
    assert!(err.contains("already loaded"), "{err}");
}

/// The value row of a single-table, single-row SELECT (box format: header,
/// types, value).
fn single_row(stdout: &str) -> Vec<String> {
    let rows: Vec<Vec<String>> = stdout
        .lines()
        .filter(|l| l.trim_start().starts_with('│'))
        .map(|l| {
            let cells: Vec<&str> = l.split('│').collect();
            cells[1..cells.len() - 1]
                .iter()
                .map(|c| c.trim().to_string())
                .collect()
        })
        .collect();
    assert_eq!(rows.len(), 3, "expected one one-row table: {stdout}");
    rows[2].clone()
}

const RANDOM_SUMMARY: &str = "SELECT sum(x * 1000000007.0), count(DISTINCT x), \
     min(x) >= 0.0 AND max(x) < 1.0 FROM t;";

#[test]
fn volatile_random_is_readable_and_a_logged_write_replays() {
    let dir = std::env::temp_dir().join(format!("pinkdb_cli_random_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("db.pink");
    let file = file.to_str().unwrap();

    let (out, err, code) = run_split(&format!(
        ".open {file}\n\
         CREATE TABLE t (x DOUBLE);\n\
         INSERT INTO t VALUES (random()), (random()), (random());\n\
         {RANDOM_SUMMARY}\n"
    ));
    assert_eq!(code, Some(0), "{err}");
    let written = single_row(&out);
    assert_eq!(written[1], "3", "one value per row: {out}");
    assert_eq!(written[2], "true", "values stay in [0, 1): {out}");

    // A fresh process replays the WAL: the statement seed was recorded, so
    // the replayed values are bit-identical and the sum prints the same text.
    let (out, err, code) = run_split(&format!(".open {file}\n{RANDOM_SUMMARY}\n"));
    assert_eq!(code, Some(0), "{err}");
    assert_eq!(single_row(&out), written, "the replay changed the values");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_durable_store_replays_deterministic_math() {
    let dir = std::env::temp_dir().join(format!("pinkdb_cli_math_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("db.pink");
    let file = file.to_str().unwrap();

    let (_, err, code) = run_split(&format!(
        ".open {file}\n\
         CREATE TABLE t (a DOUBLE);\n\
         INSERT INTO t VALUES (exp(1)), (log(2, 8));\n\
         .save {file}\n"
    ));
    assert_eq!(code, Some(0), "{err}");
    // A fresh process reopens the file and replays the WAL: the extension is
    // re-published by the CLI startup step, then resolved from the registry.
    let (out, err, code) = run_split(&format!(".open {file}\nSELECT COUNT(*), MIN(a) FROM t;\n"));
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("2"), "{out}");
    assert!(out.contains("2.718281828459045"), "{out}");

    let _ = std::fs::remove_dir_all(&dir);
}
