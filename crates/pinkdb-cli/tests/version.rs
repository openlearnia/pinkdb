//! `--version` / `-V`: release tooling identifies a binary without opening a
//! SQL session, and the output is stable (`pinkdb <semver>`).

use std::process::{Command, Output, Stdio};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb")
}

#[test]
fn version_flags_print_the_crate_version() {
    for flag in ["--version", "-V"] {
        let out = run(&[flag]);
        assert!(out.status.success(), "{flag} must succeed");
        let text = String::from_utf8_lossy(&out.stdout);
        assert_eq!(
            text.trim(),
            format!("pinkdb {}", env!("CARGO_PKG_VERSION")),
            "{flag} output was:\n{text}"
        );
        assert!(
            out.stderr.is_empty(),
            "{flag} wrote to stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
