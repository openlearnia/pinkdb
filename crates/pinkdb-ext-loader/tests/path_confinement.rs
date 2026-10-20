//! H-7: the loader's path rules.
//!
//! Loading a library runs its code, so *which* file is opened matters as
//! much as the ABI handshake. Every native load goes through the rules
//! pinned here:
//!
//! - a bare name (no directory) is refused: the dynamic loader would
//!   resolve it through its own search path;
//! - a `..` component is refused;
//! - a symlinked library is refused;
//! - a path outside the configured extension root is refused;
//! - a regular file inside the root is accepted.

mod common;

use std::path::{Path, PathBuf};

use pinkdb_ext_loader::ExtError;

/// A scratch directory that is removed when the guard drops.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_confine_{tag}_{}_{}",
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

/// The error text of a refused load.
fn refusal(err: ExtError) -> String {
    assert!(matches!(err, ExtError::InvalidPath { .. }), "{err:?}");
    err.to_string()
}

#[test]
fn a_bare_name_is_refused() {
    // No directory: the dynamic loader would search its own path
    // (LD_LIBRARY_PATH, /usr/lib, ...) for it.
    let err = pinkdb_ext_loader::load("test_ext").unwrap_err();
    let text = refusal(err);
    assert!(text.contains("dynamic loader's search path"), "{text}");
    assert!(text.contains("test_ext"), "{text}");
}

#[test]
fn a_dot_dot_component_is_refused() {
    let path = common::extension_path("test_ext");
    let name = Path::new(&path)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let parent = Path::new(&path).parent().unwrap();
    // `<dir>/../<name>` — the classic escape.
    let escape = parent.join("..").join(&name);
    let err = pinkdb_ext_loader::load(escape.to_str().unwrap()).unwrap_err();
    let text = refusal(err);
    assert!(text.contains("`..` component"), "{text}");
}

#[test]
fn a_symlinked_library_is_refused() {
    let scratch = Scratch::new("symlink");
    let real = common::extension_path("test_ext");
    let link = scratch.path(&format!("link{}", std::env::consts::DLL_SUFFIX));
    #[cfg(unix)]
    std::os::unix::fs::symlink(&real, &link).expect("create the symlink");
    let err = pinkdb_ext_loader::load(link.to_str().unwrap()).unwrap_err();
    let text = refusal(err);
    assert!(text.contains("is a symlink"), "{text}");
}

#[test]
fn a_directory_is_refused() {
    let scratch = Scratch::new("dir");
    let dir = scratch.path("adir");
    std::fs::create_dir_all(&dir).unwrap();
    let err = pinkdb_ext_loader::load(dir.to_str().unwrap()).unwrap_err();
    let text = refusal(err);
    assert!(text.contains("not a regular file"), "{text}");
}

#[test]
fn a_path_outside_the_extension_root_is_refused() {
    let scratch = Scratch::new("outside");
    let root = scratch.path("root");
    std::fs::create_dir_all(&root).unwrap();
    let outside = common::extension_path("test_ext");

    let err = pinkdb_ext_loader::load_in_root(&root, &outside).unwrap_err();
    let text = refusal(err);
    assert!(text.contains("outside the extension root"), "{text}");

    // The root is checked first: a path outside it is refused whatever it
    // names (the error never depends on whether the file exists).
    let missing = scratch.path("nothing-here.dylib");
    let err = pinkdb_ext_loader::load_in_root(&root, missing.to_str().unwrap()).unwrap_err();
    let text = refusal(err);
    assert!(text.contains("outside the extension root"), "{text}");
}

#[test]
fn a_regular_file_inside_the_extension_root_is_accepted() {
    let scratch = Scratch::new("inside");
    let root = scratch.path("root");
    std::fs::create_dir_all(&root).unwrap();
    let library = root.join(format!("test_ext{}", std::env::consts::DLL_SUFFIX));
    common::compile_to("test_ext", &library);

    let ext = pinkdb_ext_loader::load_in_root(&root, library.to_str().unwrap())
        .expect("a regular file inside the root loads");
    assert_eq!(ext.name(), "test_ext");
    // The loaded path is the canonical one.
    assert_eq!(
        std::fs::canonicalize(&library).unwrap(),
        Path::new(ext.path()).to_path_buf()
    );
    assert!(!ext.functions().is_empty());
}

#[test]
fn the_process_registry_reports_the_loaded_paths() {
    let scratch = Scratch::new("registry");
    let library = scratch.path(&format!("test_ext{}", std::env::consts::DLL_SUFFIX));
    common::compile_to("test_ext", &library);
    let ext = pinkdb_ext_loader::load(library.to_str().unwrap()).expect("load");
    let canonical = std::fs::canonicalize(&library).unwrap();
    assert!(
        pinkdb_ext_loader::loaded_paths().contains(&canonical),
        "the registry must know the loaded library"
    );
    // The lookup finds the very same file — by canonical path, so another
    // spelling of it matches too.
    let again = pinkdb_ext_loader::loaded_extension(library.to_str().unwrap())
        .expect("the library is already loaded");
    assert_eq!(again.name(), ext.name());
    let spelled = scratch
        .path(".")
        .join(Path::new(&library).file_name().unwrap());
    assert!(
        pinkdb_ext_loader::loaded_extension(spelled.to_str().unwrap()).is_some(),
        "an equivalent spelling of the same file matches"
    );
    // A different file does not.
    assert!(
        pinkdb_ext_loader::loaded_extension(scratch.path("other.dylib").to_str().unwrap())
            .is_none()
    );
}
