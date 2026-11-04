//! Compile and run the C acceptance suite in `tests/c/` against the cdylib
//! cargo just built. The suite is plain C (libc + pthreads) so it exercises
//! the header exactly as an external caller would.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn profile_dir() -> PathBuf {
    // .../target/<profile>/deps/<test binary> -> .../target/<profile>/deps
    env::current_exe()
        .expect("test executable path")
        .parent()
        .expect("deps directory")
        .to_path_buf()
}

fn output_dir() -> PathBuf {
    env::var_os("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
}

fn build(name: &str, asan: bool) -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let deps = profile_dir();
    let binary = output_dir().join(name);
    let cc = env::var_os("CC").unwrap_or_else(|| "cc".into());
    let mut command = Command::new(cc);
    command
        .arg("-std=c11")
        .arg("-O1")
        .arg("-g")
        .arg("-pthread")
        .arg(format!("-I{}", manifest.join("include").display()))
        .arg(manifest.join("tests/c/api.c"))
        .arg(format!("-L{}", deps.display()))
        .arg("-lpinkdb_ffi")
        .arg(format!("-Wl,-rpath,{}", deps.display()));
    if asan {
        command
            .arg("-fsanitize=address")
            .arg("-fno-omit-frame-pointer");
    }
    let status = command
        .arg("-o")
        .arg(&binary)
        .status()
        .expect("run the C compiler");
    assert!(status.success(), "C suite did not compile");
    binary
}

fn run(binary: &Path, asan: bool) {
    let mut command = Command::new(binary);
    if asan {
        // LeakSanitizer is not available on macOS; ASan still checks
        // use-after-free, overflows and double frees in the C caller.
        command.env("ASAN_OPTIONS", "abort_on_error=1");
    }
    let status = command.status().expect("run the C suite");
    assert!(status.success(), "C suite failed");
}

#[test]
fn c_api() {
    run(&build("pinkdb-c-api", false), false);
}

/// Run with
/// `cargo test -q -p pinkdb-ffi --test c_api -- --ignored c_api_asan`.
#[test]
#[ignore = "separate AddressSanitizer command"]
fn c_api_asan() {
    run(&build("pinkdb-c-api-asan", true), true);
}
