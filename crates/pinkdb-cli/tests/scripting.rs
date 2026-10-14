//! Scripting conventions (sqlite3/duckdb style): errors on stderr, exit 1
//! when a non-interactive run had any error, `-c`, `.read`, end-of-input
//! statements, and statement ends that understand strings and comments.

use std::process::{Command, Stdio};

mod common;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

/// stdout, stderr, exit code — separately.
fn run_split(args: &[&str], stdin: &str) -> (String, String, Option<i32>) {
    use std::io::Write;
    let mut child = cli()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code(),
    )
}

#[test]
fn clean_scripts_exit_zero_quietly() {
    let (out, err, code) = run_split(&[], "SELECT 41 + 1 AS answer;\n");
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("42"), "{out}");
    // No banner, no timing in a non-interactive run.
    assert!(!out.contains("pinkdb v0.1.0"), "{out}");
    assert!(!out.contains("Run Time"), "{out}");
    assert!(err.is_empty(), "{err}");
}

#[test]
fn errors_go_to_stderr_and_fail_the_script() {
    let (out, err, code) = run_split(&[], "SELECT * FROM missing;\nSELECT 1;\n");
    assert_eq!(code, Some(1));
    assert!(err.contains("Error:") && err.contains("missing"), "{err}");
    assert!(!out.contains("Error:"), "{out}");
    // Later statements still run (no implicit bail).
    assert!(out.contains("1 row"), "{out}");
    // Dot-command failures count too.
    let (_, err, code) = run_split(&[], ".nosuchcommand\n");
    assert_eq!(code, Some(1));
    assert!(err.contains("unknown dot-command"), "{err}");
}

#[test]
fn statement_ends_respect_strings_and_comments() {
    let (out, err, code) = run_split(
        &[],
        "SELECT 1 AS a; -- trailing comment\n\
         SELECT 'x;y' AS s\n;\n\
         SELECT 2 /* ; inside */ AS b;\n",
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("x;y"), "{out}");
    assert_eq!(out.matches("1 row").count(), 3, "{out}");
}

#[test]
fn an_unterminated_last_statement_still_runs() {
    let (out, err, code) = run_split(&[], "CREATE TABLE t (a INTEGER);\nSELECT 7 AS seven");
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("7"), "{out}");
    // Only a comment left over is not a statement.
    let (_, err, code) = run_split(&[], "SELECT 1;\n-- just a note\n");
    assert_eq!(code, Some(0), "{err}");
}

#[test]
fn dash_c_runs_commands_and_reports_failure() {
    let (out, err, code) =
        run_split(&["-c", "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (5);", "-c", "SELECT a FROM t"], "");
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("5"), "{out}");
    let (_, err, code) = run_split(&["-c", "SELECT nope FROM nowhere"], "");
    assert_eq!(code, Some(1));
    assert!(err.contains("Error:"), "{err}");
    // Dot-commands work in -c too.
    let (out, _, code) = run_split(&["-c", ".databases"], "");
    assert_eq!(code, Some(0));
    assert!(out.contains("main"), "{out}");
}

#[test]
fn dot_read_runs_a_script_file_and_timer_is_opt_in() {
    let dir = std::env::temp_dir().join(format!("pinkdb_cli_read_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("setup.sql");
    std::fs::write(&script, "CREATE TABLE r (x INTEGER);\nINSERT INTO r VALUES (1), (2);\n").unwrap();
    let (out, status) = common::run_merged(
        cli(),
        &format!(".read {}\n.timer on\nSELECT COUNT(*) FROM r;\n", script.display()),
    );
    assert_eq!(status.code(), Some(0), "{out}");
    assert!(out.contains("Run Time (s): real"), "{out}");
    assert!(out.contains("│ 2"), "{out}");
    let (out, status) = common::run_merged(cli(), ".read /definitely/missing.sql\n");
    assert_eq!(status.code(), Some(1));
    assert!(out.contains("cannot read"), "{out}");
    let _ = std::fs::remove_dir_all(&dir);
}
