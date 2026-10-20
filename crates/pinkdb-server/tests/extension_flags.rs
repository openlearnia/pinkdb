//! The server binary's extension flags: `--allow-load-extension` turns the
//! statement on, `--extension-root` confines the paths a remote client may
//! name, and without the flag the statement stays refused.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb_server::Client;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_server_ext_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Spawn the real binary on an ephemeral port and wait for the readiness
/// ("listening") line; returns the child and the address it printed.
fn start_server(args: &[&str]) -> (Child, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"));
    cmd.args(["--host", "127.0.0.1", "--port", "0"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = cmd.spawn().expect("spawn pinkdb-server");
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .expect("read pinkdb-server stdout");
    assert!(
        n > 0,
        "pinkdb-server exited before printing its listening line: {line:?}"
    );
    let rest = line
        .split_once("listening on ")
        .unwrap_or_else(|| panic!("readiness line missing 'listening on': {line:?}"))
        .1;
    let addr = rest
        .split_whitespace()
        .next()
        .expect("address on readiness line");
    (child, addr.to_string())
}

#[test]
fn the_statement_is_refused_without_the_flag() {
    let dir = scratch("default");
    let (mut child, addr) = start_server(&[]);
    let mut client = Client::connect(&addr).expect("connect");
    let msg = client
        .execute("LOAD EXTENSION '/definitely/not/a/lib.dylib';")
        .expect_err("the statement must be refused by default")
        .to_string();
    assert!(msg.contains("LOAD EXTENSION is disabled"), "{msg}");
    let _ = child.kill();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_extension_root_confines_a_remote_client() {
    let dir = scratch("root");
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let outside = dir.join("outside.dylib");
    let inside = root.join("inside.dylib");

    let (mut child, addr) = start_server(&[
        "--allow-load-extension",
        "--extension-root",
        root.to_str().unwrap(),
    ]);
    let mut client = Client::connect(&addr).expect("connect");

    // A path outside the root is refused, whatever the file is.
    let msg = client
        .execute(&format!("LOAD EXTENSION '{}';", outside.display()))
        .expect_err("a path outside the root must be refused")
        .to_string();
    assert!(msg.contains("outside the extension root"), "{msg}");

    // A path inside the root passes the confinement and reaches the loader.
    let msg = client
        .execute(&format!("LOAD EXTENSION '{}';", inside.display()))
        .expect_err("the missing library still errors")
        .to_string();
    assert!(!msg.contains("outside the extension root"), "{msg}");
    assert!(msg.contains("cannot load extension"), "{msg}");

    let _ = child.kill();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_root_that_is_not_a_directory_is_refused_at_startup() {
    let dir = scratch("bad_root");
    let file = dir.join("a-file");
    std::fs::write(&file, b"x").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--extension-root",
            file.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .output()
        .expect("run pinkdb-server");
    assert_eq!(out.status.code(), Some(2));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("is not a directory"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}
