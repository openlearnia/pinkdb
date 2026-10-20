//! The CLI's extension policy: `LOAD EXTENSION` is off by default, the
//! `--allow-load-extension` flag (or `PINKDB_ALLOW_LOAD_EXTENSION=1`) turns it
//! on, and `--extension-root` confines every load to one directory.

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

fn run(args: &[&str], env: &[(&str, &str)], stdin: &str) -> Output {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .args(args)
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pinkdb");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(stdin.as_bytes())
        .expect("write the script");
    child.wait_with_output().expect("wait for pinkdb")
}

fn output_text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A scratch directory that is removed when the guard drops.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_cli_ext_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        Scratch(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn load_extension_is_refused_without_the_opt_in() {
    let scratch = Scratch::new("default");
    let library = scratch.path("lib.dylib");
    let out = run(
        &[],
        &[],
        &format!("LOAD EXTENSION '{}';\n", library.display()),
    );
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(text.contains("LOAD EXTENSION is disabled"), "{text}");
}

#[test]
fn the_flag_and_the_environment_variable_turn_the_statement_on() {
    let scratch = Scratch::new("optin");
    let library = scratch.path("lib.dylib");
    let script = format!("LOAD EXTENSION '{}';\n", library.display());

    // The flag: the statement runs, and the loader's own error proves the
    // gate is no longer the blocker.
    let out = run(&["--allow-load-extension"], &[], &script);
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(!text.contains("LOAD EXTENSION is disabled"), "{text}");
    assert!(text.contains("cannot load extension"), "{text}");

    // The environment variable does the same.
    let out = run(&[], &[("PINKDB_ALLOW_LOAD_EXTENSION", "1")], &script);
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(!text.contains("LOAD EXTENSION is disabled"), "{text}");
    assert!(text.contains("cannot load extension"), "{text}");
}

#[test]
fn the_extension_root_confines_every_load() {
    let scratch = Scratch::new("root");
    let root = scratch.path("root");
    std::fs::create_dir_all(&root).unwrap();
    let inside = root.join("lib.dylib");
    let outside = scratch.path("outside.dylib");

    // A path outside the root is refused, even with the opt-in.
    let out = run(
        &[
            "--allow-load-extension",
            "--extension-root",
            root.to_str().unwrap(),
        ],
        &[],
        &format!("LOAD EXTENSION '{}';\n", outside.display()),
    );
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(text.contains("outside the extension root"), "{text}");

    // A path inside the root passes the confinement and reaches the loader
    // (which then reports the missing file).
    let out = run(
        &[
            "--allow-load-extension",
            "--extension-root",
            root.to_str().unwrap(),
        ],
        &[],
        &format!("LOAD EXTENSION '{}';\n", inside.display()),
    );
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(!text.contains("outside the extension root"), "{text}");
    assert!(text.contains("cannot load extension"), "{text}");

    // A root that is not a directory is refused at startup.
    let out = run(
        &[
            "--allow-load-extension",
            "--extension-root",
            outside.to_str().unwrap(),
        ],
        &[],
        "SELECT 1;\n",
    );
    assert_eq!(out.status.code(), Some(2));
    let text = output_text(&out);
    assert!(text.contains("is not a directory"), "{text}");
}

#[test]
fn the_help_lists_the_extension_flags() {
    let out = run(&["--help"], &[], "");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("--allow-load-extension"), "{text}");
    assert!(text.contains("--extension-root"), "{text}");
}
