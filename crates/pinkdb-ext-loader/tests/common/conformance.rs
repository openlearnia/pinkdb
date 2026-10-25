//! The four SDK conformance fixtures, built from source.
//!
//! `ext-sdk/conformance/README.md` is the contract; each SDK implements it
//! once (`ext-sdk/{c,cpp,go,rust}/conformance`). Building them lives here
//! because two test binaries need the paths: `conformance.rs` (the behaviour
//! matrix) and `conformance_ownership.rs` (the allocation counters, which need
//! a process of their own).
//!
//! A missing toolchain skips that fixture and says so, unless
//! `PINKDB_CONFORMANCE_STRICT=1` — CI's `sdk-smokes` job sets it, because that
//! job installs every toolchain.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// One SDK's build of the fixture, plus the few behaviours the fixture
/// advertises because not every SDK can produce them the same way.
pub struct Fixture {
    pub sdk: &'static str,
    pub library: PathBuf,
    /// What a panicking/`throw`ing callback comes back as. C has no
    /// unwinding, so its fixture returns 2 explicitly; the SDKs that contain
    /// real unwinding map it to their guard's code.
    pub unwind_code: i32,
    /// `pinkdb_conformance_caps` bit 1: `cf_unwind` really unwinds.
    pub unwind_is_real: bool,
    /// `pinkdb_conformance_caps` bit 0: `cf_fail` allocates before failing.
    pub fail_allocates: bool,
}

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the loader crate lives two levels below the repo root")
        .to_path_buf()
}

pub fn build_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("pinkdb_conformance_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the conformance build directory");
    dir
}

fn strict() -> bool {
    matches!(
        std::env::var("PINKDB_CONFORMANCE_STRICT").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Can `tool` be run at all (with `probe` as its version flag — `go version`,
/// `cc --version`)? A named-but-broken compiler is not "missing": it builds
/// and fails loudly below.
fn have(tool: &str, probe: &str) -> bool {
    Command::new(tool)
        .arg(probe)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Skip one fixture's build, or fail when the run is strict.
fn skip(sdk: &str, reason: &str) {
    if strict() {
        panic!("{sdk} conformance fixture cannot be built: {reason}");
    }
    eprintln!("SKIP the {sdk} conformance fixture: {reason}");
}

fn run(mut command: Command, what: &str) {
    let output = command
        .output()
        .unwrap_or_else(|e| panic!("cannot run {what}: {e}"));
    assert!(
        output.status.success(),
        "{what} failed ({})\n--- stdout\n{}\n--- stderr\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn library_path(name: &str) -> PathBuf {
    build_dir().join(format!("{name}{}", std::env::consts::DLL_SUFFIX))
}

fn build_c() -> Option<PathBuf> {
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    if !have(&cc, "--version") {
        skip("C", &format!("no `{cc}` compiler"));
        return None;
    }
    let root = repo_root().join("ext-sdk").join("c");
    let out = library_path("libconformance_c");
    let mut command = Command::new(&cc);
    command
        .args(["-std=c99", "-O1", "-fPIC", "-shared"])
        .arg("-I")
        .arg(root.join("include"))
        .arg(root.join("conformance").join("conformance_ext.c"))
        .arg("-o")
        .arg(&out);
    run(command, "the C conformance build");
    Some(out)
}

fn build_cpp() -> Option<PathBuf> {
    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".to_string());
    if !have(&cxx, "--version") {
        skip("C++", &format!("no `{cxx}` compiler"));
        return None;
    }
    let root = repo_root().join("ext-sdk").join("cpp");
    let out = library_path("libconformance_cpp");
    let mut command = Command::new(&cxx);
    command
        .args(["-std=c++17", "-O1", "-fPIC", "-shared"])
        .arg("-I")
        .arg(root.join("include"))
        .arg(root.join("conformance").join("conformance_ext.cpp"))
        .arg("-o")
        .arg(&out);
    run(command, "the C++ conformance build");
    Some(out)
}

fn build_go() -> Option<PathBuf> {
    let go = std::env::var("GO").unwrap_or_else(|_| "go".to_string());
    if !have(&go, "version") {
        skip("Go", &format!("no `{go}` toolchain"));
        return None;
    }
    let root = repo_root().join("ext-sdk").join("go");
    let out = library_path("libconformance_go");
    let mut command = Command::new(&go);
    command
        .current_dir(&root)
        .env("CGO_ENABLED", "1")
        .args(["build", "-buildmode=c-shared", "-o"])
        .arg(&out)
        .arg("./conformance");
    run(command, "the Go conformance build (c-shared)");
    Some(out)
}

/// The Rust fixture is a workspace member. Build it into a scratch target
/// directory: a nested `cargo` aimed at the main target directory would block
/// on the lock the outer `cargo test` holds.
fn build_rust() -> Option<PathBuf> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    if !have(&cargo, "--version") {
        skip("Rust", &format!("no `{cargo}`"));
        return None;
    }
    let target = build_dir().join("rust-target");
    let mut command = Command::new(&cargo);
    command
        .current_dir(repo_root())
        .args([
            "build",
            "-q",
            "-p",
            "pinkdb-ext-conformance",
            "--target-dir",
        ])
        .arg(&target);
    // The outer `cargo test` exports its own flags; they describe the test
    // build, not this one.
    command
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    run(command, "the Rust conformance build");
    let out = target.join("debug").join(format!(
        "libconformance_ext{}",
        std::env::consts::DLL_SUFFIX
    ));
    assert!(
        out.is_file(),
        "the Rust conformance build produced no cdylib at {}",
        out.display()
    );
    Some(out)
}

pub fn fixtures() -> &'static [Fixture] {
    static FIXTURES: OnceLock<Vec<Fixture>> = OnceLock::new();
    FIXTURES.get_or_init(|| {
        let mut built = Vec::new();
        if let Some(library) = build_c() {
            built.push(Fixture {
                sdk: "C",
                library,
                unwind_code: 2,
                unwind_is_real: false,
                fail_allocates: true,
            });
        }
        if let Some(library) = build_cpp() {
            built.push(Fixture {
                sdk: "C++",
                library,
                unwind_code: 3,
                unwind_is_real: true,
                fail_allocates: true,
            });
        }
        if let Some(library) = build_go() {
            built.push(Fixture {
                sdk: "Go",
                library,
                unwind_code: 3,
                unwind_is_real: true,
                fail_allocates: true,
            });
        }
        if let Some(library) = build_rust() {
            built.push(Fixture {
                sdk: "Rust",
                library,
                unwind_code: 2,
                unwind_is_real: true,
                // The typed SDK cannot publish text and then fail: `cf_fail`
                // is an `Err` before any allocation.
                fail_allocates: false,
            });
        }
        if strict() {
            assert_eq!(
                built.len(),
                4,
                "strict conformance requires all four SDK fixtures, built: {:?}",
                built.iter().map(|f| f.sdk).collect::<Vec<_>>()
            );
        }
        if built.is_empty() {
            eprintln!("SKIP the ABI conformance matrix: no SDK toolchain is available");
        }
        built
    })
}

pub fn path_string(path: &Path) -> String {
    path.to_str()
        .expect("the fixture paths are UTF-8")
        .to_string()
}
