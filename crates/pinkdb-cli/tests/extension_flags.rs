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
    assert!(text.contains("--extension-pin"), "{text}");
}

// SHA-256 of the three bytes "abc": a pinned hash the tests can spell without
// a hashing dependency. The library files below are deliberately not shared
// libraries — the integrity check runs before `dlopen`, so a plain file is
// enough to tell a pin refusal from a load failure.
const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

#[test]
fn a_bad_pin_is_a_startup_error() {
    for pin in ["nonsense", "a=/tmp/lib.so", "a=/tmp/lib.so=xyz"] {
        let out = run(&["--extension-pin", pin], &[], "SELECT 1;\n");
        assert_eq!(out.status.code(), Some(2), "pin {pin:?} must be refused");
        let text = output_text(&out);
        assert!(text.contains("invalid pin"), "{pin:?}: {text}");
    }
}

#[test]
fn an_allowlist_refuses_an_unpinned_library() {
    let scratch = Scratch::new("unpinned");
    let pinned = scratch.path("pinned.dylib");
    let unpinned = scratch.path("unpinned.dylib");
    std::fs::write(&pinned, b"abc").unwrap();
    std::fs::write(&unpinned, b"abc").unwrap();

    let pin = format!("{}={SHA256_ABC}", pinned.display());
    let out = run(
        &["--allow-load-extension", "--extension-pin", &pin],
        &[],
        &format!("LOAD EXTENSION '{}';\n", unpinned.display()),
    );
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(text.contains("is not pinned"), "{text}");
    assert!(
        text.contains("--extension-pin PATH=SHA256"),
        "the refusal names the flag: {text}"
    );
}

#[test]
fn a_library_whose_hash_differs_from_its_pin_is_refused() {
    let scratch = Scratch::new("tampered");
    let library = scratch.path("tampered.dylib");
    // The pin is the hash of "abc", the file holds "abcd".
    std::fs::write(&library, b"abcd").unwrap();
    let pin = format!("{}={SHA256_ABC}", library.display());

    let out = run(
        &["--allow-load-extension", "--extension-pin", &pin],
        &[],
        &format!("LOAD EXTENSION '{}';\n", library.display()),
    );
    assert_eq!(out.status.code(), Some(1));
    let text = output_text(&out);
    assert!(text.contains("does not match its pinned SHA-256"), "{text}");
    assert!(
        text.contains(SHA256_ABC),
        "the expected hash is named: {text}"
    );
}

#[test]
fn a_pin_uses_the_same_path_rules_as_a_load() {
    // A bare name would be resolved through the dynamic loader's search path,
    // so it cannot be pinned either.
    let out = run(&["--extension-pin", "libfoo.dylib=abc"], &[], "SELECT 1;\n");
    assert_eq!(out.status.code(), Some(2));
    let text = output_text(&out);
    assert!(!text.is_empty(), "{text}");
}
