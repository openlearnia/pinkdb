//! Local-extension integrity: the optional SHA-256 allowlist.
//!
//! The path rules (`path_confinement.rs`) decide *which* file is opened; these
//! tests cover the second half — refusing a library whose *contents* the host
//! did not pin, and refusing every load once an allowlist exists but the path
//! is missing from it.
//!
//! The allowlist is process-global (it has to be: a WAL replay may load
//! without going through the host that configured it), so these tests take a
//! file-wide lock and each test restores the "no allowlist" state it started
//! from.

mod common;

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use pinkdb_ext_loader::{
    allowlist_is_active, clear_allowlist, pinned_paths, set_pins, sha256_file,
};

/// Serialise the tests in this file (the allowlist is process-wide) and
/// restore the default "no allowlist" state even when a test panics.
struct Guard {
    _lock: MutexGuard<'static, ()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        clear_allowlist();
    }
}

fn lock() -> Guard {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _lock = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_allowlist();
    Guard { _lock }
}

fn path_string(path: &Path) -> String {
    path.to_str().expect("test paths are UTF-8").to_string()
}

/// A copy of a test extension in a scratch directory, so a test can mutate it
/// without disturbing the cached builds other tests share.
fn scratch_copy(tag: &str, name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "pinkdb_integrity_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create the scratch directory");
    let output = dir.join(format!("{name}{}", std::env::consts::DLL_SUFFIX));
    common::compile_to(name, &output);
    output
}

#[test]
fn without_an_allowlist_any_resolved_path_loads() {
    let _guard = lock();
    assert!(!allowlist_is_active());
    let ext = common::extension("test_ext");
    let loaded =
        pinkdb_ext_loader::load(&path_string(&ext)).expect("an unpinned host loads the extension");
    assert!(!loaded.functions().is_empty());
}

#[test]
fn a_pinned_path_loads_when_its_hash_matches() {
    let _guard = lock();
    let ext = common::extension("test_ext2");
    let hash = sha256_file(&ext).expect("hash the test extension");
    assert_eq!(hash.len(), 64);
    set_pins([format!("{}={hash}", ext.display())]).expect("install the pin");
    assert!(allowlist_is_active());
    assert_eq!(pinned_paths(), vec![ext.clone()]);

    let loaded = pinkdb_ext_loader::load(&path_string(&ext)).expect("the pinned hash matches");
    assert_eq!(loaded.canonical_path(), ext);
    clear_allowlist();
}

#[test]
fn a_pin_may_be_spelled_with_the_sha256_prefix_and_uppercase_hex() {
    let _guard = lock();
    let ext = scratch_copy("prefix", "test_ext");
    let hash = sha256_file(&ext).expect("hash the test extension");
    set_pins([format!("{}=sha256:{}", ext.display(), hash.to_uppercase())])
        .expect("the `sha256:` prefix and uppercase hex are the documented spellings");
    pinkdb_ext_loader::load(&path_string(&ext)).expect("the pinned hash matches");
    clear_allowlist();
}

#[test]
fn a_path_missing_from_the_allowlist_is_refused() {
    let _guard = lock();
    let pinned = common::extension("test_ext");
    // Not loaded by any other test in this file, so the registry proves the
    // refusal happened before the library was mapped.
    let other = common::extension("empty_ext");
    let hash = sha256_file(&pinned).expect("hash the test extension");
    set_pins([format!("{}={hash}", pinned.display())]).expect("install the pin");

    let error = pinkdb_ext_loader::load(&path_string(&other)).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("is not pinned"), "{message}");
    assert!(
        message.contains("--extension-pin PATH=SHA256"),
        "the error should name the flag that fixes it: {message}"
    );
    // Refused before any code ran: the extension is not in the registry.
    assert!(
        pinkdb_ext_loader::loaded_extension(&path_string(&other)).is_none(),
        "an unpinned library must not be mapped"
    );
    clear_allowlist();
}

#[test]
fn a_modified_library_no_longer_matches_its_pin() {
    let _guard = lock();
    let ext = scratch_copy("tamper", "test_ext2");
    let pinned = sha256_file(&ext).expect("hash the test extension");
    set_pins([format!("{}={pinned}", ext.display())]).expect("install the pin");

    // Same path, different bytes.
    let mut bytes = std::fs::read(&ext).expect("read the test extension");
    bytes.push(0);
    std::fs::write(&ext, &bytes).expect("rewrite the test extension");

    let error = pinkdb_ext_loader::load(&path_string(&ext)).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("does not match its pinned SHA-256"),
        "{message}"
    );
    assert!(message.contains(&pinned), "expected hash in: {message}");
    let actual = sha256_file(&ext).expect("hash the mutated file");
    assert!(
        message.contains(&actual),
        "actual hash {actual} in: {message}"
    );
    clear_allowlist();
}

#[test]
fn an_empty_allowlist_refuses_every_load() {
    let _guard = lock();
    // Documented: an empty iterator enables the allowlist with no entries,
    // which is the strictest configuration, not "off".
    set_pins(std::iter::empty::<String>()).expect("an empty allowlist is valid");
    assert!(allowlist_is_active());
    let ext = common::extension("test_ext");
    let error = pinkdb_ext_loader::load(&path_string(&ext)).unwrap_err();
    assert!(error.to_string().contains("is not pinned"), "{error}");
    clear_allowlist();
    assert!(!allowlist_is_active());
}

#[test]
fn clearing_the_allowlist_reenables_unpinned_loads() {
    let _guard = lock();
    let ext = common::extension("test_ext2");
    let hash = sha256_file(&ext).expect("hash the test extension");
    set_pins([format!("{}={hash}", ext.display())]).expect("install the pin");
    clear_allowlist();
    assert!(!allowlist_is_active());
    assert_eq!(pinned_paths(), Vec::<PathBuf>::new());
    pinkdb_ext_loader::load(&path_string(&ext)).expect("no allowlist, no content check");
}

#[test]
fn pins_must_name_loadable_paths_and_hashes() {
    let _guard = lock();
    let ext = common::extension("test_ext");
    let hash = sha256_file(&ext).expect("hash the test extension");

    // No `=` at all.
    let error = set_pins(["no-separator"]).unwrap_err();
    assert!(
        error.to_string().contains("expected PATH=SHA256"),
        "{error}"
    );

    // A malformed hash.
    let error = set_pins([format!("{}=not-a-hash", ext.display())]).unwrap_err();
    assert!(error.to_string().contains("invalid pin"), "{error}");

    // A bare name: the pin must obey the same path rules as a load, so a pin
    // can never name a file the loader would resolve through its search path.
    let error = set_pins([format!("libfoo.so={hash}")]).unwrap_err();
    assert!(
        error.to_string().contains("is not a loadable path"),
        "{error}"
    );

    // A `..` component.
    let error = set_pins([format!("../libfoo.so={hash}")]).unwrap_err();
    assert!(
        error.to_string().contains("is not a loadable path"),
        "{error}"
    );

    // A missing file.
    let missing = std::env::temp_dir().join("pinkdb-integrity-missing/libfoo.so");
    let error = set_pins([format!("{}={hash}", missing.display())]).unwrap_err();
    assert!(
        error.to_string().contains("cannot load extension"),
        "a pin for a missing file is not installable: {error}"
    );

    // Nothing was installed by the failed calls.
    assert!(!allowlist_is_active());
    pinkdb_ext_loader::load(&path_string(&ext)).expect("no allowlist was installed");
}

#[test]
fn conflicting_pins_for_one_path_are_refused() {
    let _guard = lock();
    let ext = common::extension("test_ext");
    let hash = sha256_file(&ext).expect("hash the test extension");
    let other = "0".repeat(64);

    let error = set_pins([
        format!("{}={hash}", ext.display()),
        format!("{}={other}", ext.display()),
    ])
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("already pinned with a different hash"),
        "{error}"
    );
    assert!(!allowlist_is_active());
}
