//! The C/C++ , Go and Rust SDKs under `ext-sdk/` vendor `pinkdb_extension.h`.
//! This test keeps every vendored copy byte-identical to the loader crate's
//! copy, so editing one without re-vendoring the others fails `cargo test`.
//!
//! The loader header is the single source of truth; re-vendor with:
//!
//! ```sh
//! for sdk in c go rust; do
//!     cp crates/pinkdb-ext-loader/include/pinkdb_extension.h \
//!        ext-sdk/$sdk/include/pinkdb_extension.h
//! done
//! ```

use std::path::{Path, PathBuf};

/// The loader's copy (the source of truth) and every vendored SDK copy.
fn header_paths(manifest: &Path) -> (PathBuf, Vec<PathBuf>) {
    let source = manifest.join("include").join("pinkdb_extension.h");
    // `manifest` is `<repo>/crates/pinkdb-ext-loader`.
    let repo = manifest
        .parent()
        .and_then(Path::parent)
        .expect("the loader crate lives two levels below the repo root");
    let vendored = ["c", "go", "rust"]
        .iter()
        .map(|sdk| {
            repo.join("ext-sdk")
                .join(sdk)
                .join("include")
                .join("pinkdb_extension.h")
        })
        .collect();
    (source, vendored)
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn sdk_vendored_headers_match_the_loader_header() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (source_path, vendored_paths) = header_paths(manifest);
    let source = read(&source_path);

    for path in &vendored_paths {
        let vendored = read(path);
        if source == vendored {
            continue;
        }
        // Point at the first differing byte for a useful CI failure message.
        let detail = match source.iter().zip(vendored.iter()).position(|(a, b)| a != b) {
            Some(i) => format!("first difference at byte {i}"),
            None => "one copy is a strict prefix of the other".to_string(),
        };
        panic!(
            "the vendored SDK header {} has drifted from the loader header ({detail}); \
             re-vendor it with `cp {} {}`",
            path.display(),
            source_path.display(),
            path.display()
        );
    }
}

/// The header must stay C++-friendly: the SDK's C++ example relies on the
/// `extern "C"` guards, and dropping them would only fail at link time there.
#[test]
fn header_has_cpp_guards() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (source_path, _) = header_paths(manifest);
    let text = String::from_utf8(read(&source_path)).expect("the ABI header is UTF-8");
    assert!(
        text.contains("#ifdef __cplusplus") && text.contains("extern \"C\""),
        "the ABI header must keep its `#ifdef __cplusplus` / `extern \"C\"` guards"
    );
}
