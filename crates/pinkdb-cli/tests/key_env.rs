//! `PINKDB_KEY` handling in the CLI: a set-but-non-UTF-8 value must be a hard
//! error (never a silent plaintext save), and remote mode must call out that
//! the variable is inert there.

use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_keyenv_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Run the REPL with `PINKDB_KEY` set (or removed) and the given arguments.
fn run(cwd: &Path, key: Option<OsString>, args: &[&str], cmds: &str) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    cmd.args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(cwd);
    match key {
        Some(k) => {
            cmd.env("PINKDB_KEY", k);
        }
        None => {
            cmd.env_remove("PINKDB_KEY");
        }
    }
    let mut child = cmd.spawn().expect("spawn pinkdb CLI");
    // The child may exit before reading stdin (e.g. the invalid-key path).
    let _ = child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(cmds.as_bytes());
    child.wait_with_output().expect("wait for pinkdb CLI")
}

/// M2: a set-but-non-UTF-8 `PINKDB_KEY` must fail loudly instead of being
/// silently ignored (which would make `.save` write plaintext).
#[cfg(unix)]
#[test]
fn non_utf8_pinkdb_key_is_a_hard_error_not_a_plaintext_save() {
    use std::os::unix::ffi::OsStringExt;
    let dir = scratch("badutf8");
    let out = run(
        &dir,
        Some(OsString::from_vec(vec![0xFF, 0xFE])),
        &[],
        ".save x.pink\n.quit\n",
    );
    assert_eq!(out.status.code(), Some(2), "must exit with an error");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("PINKDB_KEY is not valid UTF-8"), "{text}");
    assert!(
        !dir.join("x.pink").exists(),
        "no plaintext file may be written for a set key"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// M6: remote mode does not use `PINKDB_KEY` (the server's own at-rest
/// configuration applies), so a set value must be called out instead of being
/// silently ignored. The warning fires before the connection is attempted, so
/// this holds with and without the `client` feature.
#[test]
fn remote_mode_warns_that_pinkdb_key_is_inert() {
    let dir = scratch("remotewarn");
    let out = run(
        &dir,
        Some(OsString::from("sesame")),
        &["--host", "127.0.0.1:1"],
        "",
    );
    let text = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        text.contains("PINKDB_KEY is set but is not used in remote mode"),
        "stderr was:\n{text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
