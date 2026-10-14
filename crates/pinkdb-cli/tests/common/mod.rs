//! Shared CLI test plumbing.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::process::{Command, ExitStatus, Stdio};

/// Run `command` with `stdin` as its input and return its stdout and stderr
/// interleaved in write order (one pipe for both, like `2>&1`) plus the exit
/// status. Errors go to stderr since the CLI follows sqlite3/duckdb
/// conventions, so tests that look for `Error:` lines see them in place.
pub fn run_merged(mut command: Command, stdin: &str) -> (String, ExitStatus) {
    let (mut reader, writer) = std::io::pipe().expect("create pipe");
    command
        .stdin(Stdio::piped())
        .stdout(writer.try_clone().expect("clone pipe writer"))
        .stderr(writer);
    let mut child = command.spawn().expect("spawn pinkdb CLI");
    // The parent's copies of the write end live in `command`; drop them so
    // the reader sees EOF when the child exits.
    drop(command);
    let mut child_stdin = child.stdin.take().expect("child stdin");
    let input = stdin.to_string();
    let feeder = std::thread::spawn(move || {
        let _ = child_stdin.write_all(input.as_bytes());
    });
    let mut out = String::new();
    reader.read_to_string(&mut out).expect("read CLI output");
    feeder.join().expect("stdin feeder");
    let status = child.wait().expect("wait for the CLI");
    (out, status)
}

/// A clean script exit: 0 (no errors) or 1 (a statement or command failed);
/// never a signal or a panic exit code.
pub fn assert_script_exit(status: ExitStatus) {
    assert!(
        matches!(status.code(), Some(0) | Some(1)),
        "CLI did not exit cleanly: {status}"
    );
}
