//! `.import` option plumbing: `key=value` tokens reach the shared CSV option
//! table, and the `errors=warn` rejects report is printed on stdout.

use std::process::{Command, Stdio};

mod common;

/// Run a script through the CLI; returns stdout, stderr and the exit code.
fn run(script: &str) -> (String, String, Option<i32>) {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pinkdb CLI");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code(),
    )
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pinkdb_cli_csv_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn delimiter_encoding_and_header_options_reach_the_importer() {
    let dir = scratch("delim");
    let path = dir.join("people.csv");
    // Semicolons, latin-1 "é", and a quoted field containing the delimiter.
    std::fs::write(&path, b"id;name\r\n1;Andr\xe9\r\n2;\"b;b\"\r\n").unwrap();
    let script = format!(
        ".import \"{}\" t delimiter=';' encoding=latin-1 header=true\n\
         SELECT id, name FROM t ORDER BY id;\n",
        path.display()
    );
    let (out, err, code) = run(&script);
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("Imported 2 rows"), "{out}");
    assert!(out.contains("André"), "{out}");
    assert!(out.contains("b;b"), "{out}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn warn_policy_prints_the_rejects_report() {
    let dir = scratch("warn");
    let path = dir.join("rows.csv");
    std::fs::write(&path, "a,b\n1,x\nbad,y\n4,z\n").unwrap();
    // The existing table fixes `a` as INTEGER, so the header match parses it
    // strictly and the middle row is rejected instead of widening the column.
    let script = format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         .import \"{}\" t errors=warn\n\
         SELECT SUM(a) FROM t;\n",
        path.display()
    );
    let (out, err, code) = run(&script);
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("Imported 2 rows"), "{out}");
    assert!(out.contains("1 row rejected:"), "{out}");
    assert!(
        out.contains("line 3, column \"a\": cannot parse \"bad\" as INTEGER"),
        "{out}"
    );
    assert!(out.contains('5'), "{out}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn strict_is_the_default_so_a_bad_row_fails_the_script() {
    let dir = scratch("strict");
    let path = dir.join("rows.csv");
    std::fs::write(&path, "a,b\n1,2\n3,4,5\n").unwrap();
    let script = format!(".import \"{}\" t\n", path.display());
    let (out, err, code) = run(&script);
    assert_eq!(code, Some(1));
    assert!(err.contains("3 fields"), "{err}");
    assert!(!out.contains("Imported"), "{out}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn bad_options_are_reported_not_ignored() {
    let dir = scratch("bad");
    let path = dir.join("x.csv");
    std::fs::write(&path, "a\n1\n").unwrap();
    let (out, err, code) = run(&format!(".import \"{}\" t bogus=1\n", path.display()));
    assert_eq!(code, Some(1));
    assert!(err.contains("unknown CSV option `bogus`"), "{err}");
    assert!(!out.contains("Imported"), "{out}");
    // A stray positional token after the options is a usage error.
    let (_, err, code) = run(&format!(
        ".import \"{}\" t header=true extra\n",
        path.display()
    ));
    assert_eq!(code, Some(0), "usage lines are not errors: {err}");
    std::fs::remove_dir_all(&dir).ok();
}
