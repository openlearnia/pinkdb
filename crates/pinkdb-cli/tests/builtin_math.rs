//! The CLI's default `math` feature (wave 54): the core math extension is
//! published in the linked registry at startup and loaded into every store
//! the session creates or opens, so `exp`/`log`/`random` work without a
//! `LOAD EXTENSION` statement — and the volatile refusal applies to the
//! default binary too. `--no-default-features` prints the not-built message
//! instead (pinned by `--no-default-features` CI runs, not here).
#![cfg(feature = "math")]

use std::process::{Command, Stdio};

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

#[test]
fn math_functions_work_without_a_load_statement() {
    let (out, err, code) = run_split(
        "SELECT exp(1), log(100), log(2, 8), sin(0), atan2(1, 1), pi();\nSELECT sqrt(4), pow(2, 10);\n",
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("2.718281828459045"), "{out}");
    assert!(out.contains("3.0"), "{out}");
    assert!(out.contains("1024.0"), "{out}");
    // Loading the built-in extension explicitly is the documented duplicate.
    let (_, err, code) = run_split("LOAD EXTENSION 'math';\n");
    assert_eq!(code, Some(1));
    assert!(err.contains("already loaded"), "{err}");
}

#[test]
fn volatile_random_is_readable_but_writes_are_refused() {
    let (out, err, code) = run_split(
        "CREATE TABLE t (x DOUBLE);\n\
         SELECT random() >= 0.0 AND random() < 1.0;\n\
         INSERT INTO t VALUES (random());\n\
         SELECT COUNT(*) FROM t;\n",
    );
    // The refused INSERT fails the script (exit 1) and reports the volatile
    // rule; the read ran and the table stayed empty.
    assert_eq!(code, Some(1), "{err}");
    assert!(out.contains("true"), "{out}");
    assert!(
        err.contains("random()")
            && err.contains("volatile (nondeterministic)")
            && err.contains("cannot be called in a statement the engine WAL-logs"),
        "{err}"
    );
    assert!(out.contains("0"), "{out}");
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
