//! Shared test scaffolding: compile the C test extensions with the system C
//! compiler into `CARGO_TARGET_TMPDIR` and hand out their paths.
//!
//! `cc` is invoked directly through [`std::process::Command`] — no build
//! scripts, no extra dependencies. Override the compiler with `$CC`.
//! Each test binary compiles into its own pid-suffixed directory, so
//! concurrently running binaries never race on the same output file.
//! The test extensions themselves are POSIX C (`pthread.h`); the ABI header
//! they build against is plain portable C99.

// Each integration-test file includes this whole module, so not every helper
// is used by every file.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

/// Compile `tests/c/<name>.c` (once per process) and return the path of the
/// resulting shared library.
pub fn extension(name: &str) -> PathBuf {
    static BUILT: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();
    let built = BUILT.get_or_init(|| Mutex::new(HashMap::new()));
    let mut built = built.lock().expect("extension build cache");
    if let Some(path) = built.get(name) {
        return path.clone();
    }

    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("ext_loader_tests_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create test extension directory");
    let library = dir.join(format!("{name}{}", std::env::consts::DLL_SUFFIX));
    compile(name, &library);

    built.insert(name.to_string(), library.clone());
    library
}

/// Compile `tests/c/<name>.c` into a fresh library at `output`.
///
/// Unlike copying an existing library, a fresh compile embeds `output` as the
/// library's install name (LC_ID_DYLIB). That matters for "the file was
/// deleted" tests on macOS: dyld resolves a `dlopen` by install name, so a
/// copy would alias the already-loaded original image whenever any other test
/// in the process still holds it — making the deleted-file behavior
/// non-deterministic.
pub fn compile_to(name: &str, output: &Path) {
    compile(name, output);
}

fn compile(name: &str, library: &Path) {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = manifest.join("tests/c").join(format!("{name}.c"));
    let include = manifest.join("include");
    assert!(
        source.is_file(),
        "missing test extension source: {}",
        source.display()
    );
    if let Some(parent) = library.parent() {
        std::fs::create_dir_all(parent).expect("create test extension directory");
    }

    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let output = Command::new(&cc)
        .arg("-std=c99") // the ABI header and the test extensions are C99
        .arg("-O1")
        .arg("-fPIC")
        .arg("-shared")
        .arg("-I")
        .arg(&include)
        .arg("-o")
        .arg(library)
        .arg(&source)
        .output()
        .unwrap_or_else(|e| panic!("could not run the C compiler `{cc}`: {e} (set CC?)"));
    assert!(
        output.status.success(),
        "compiling {name}.c failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// [`extension`] as a `&str`-friendly owned path string.
pub fn extension_path(name: &str) -> String {
    extension(name)
        .to_str()
        .expect("UTF-8 test path")
        .to_string()
}

/// Read one `pinkdb_test_*` introspection symbol returning `int64_t`.
///
/// These symbols are test-extension specific (not part of the ABI); the tests
/// open the library directly to observe call counts and thread use.
pub fn symbol_i64(library: &Path, name: &str) -> i64 {
    let lib = open(library);
    let symbol = resolve::<extern "C" fn() -> i64>(&lib, name);
    symbol()
}

/// Read one `pinkdb_test_*` introspection symbol returning `int32_t`.
pub fn symbol_i32(library: &Path, name: &str) -> i32 {
    let lib = open(library);
    let symbol = resolve::<extern "C" fn() -> i32>(&lib, name);
    symbol()
}

fn open(library: &Path) -> libloading::Library {
    // SAFETY: the path is a test extension this process compiled from source;
    // loading it is the whole point.
    unsafe { libloading::Library::new(library) }
        .unwrap_or_else(|e| panic!("cannot open test extension {}: {e}", library.display()))
}

fn resolve<'a, T>(lib: &'a libloading::Library, name: &str) -> libloading::Symbol<'a, T> {
    let mut symbol = name.as_bytes().to_vec();
    symbol.push(0); // dlsym needs a NUL-terminated name
    // SAFETY: the tests know each introspection symbol's C signature.
    unsafe { lib.get::<T>(&symbol) }
        .unwrap_or_else(|e| panic!("test extension does not export {name}: {e}"))
}
