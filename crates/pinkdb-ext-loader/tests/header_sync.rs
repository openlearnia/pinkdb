//! The C/C++ SDK under `ext-sdk/c/` vendors `pinkdb_extension.h`. This test
//! keeps the vendored copy byte-identical to the loader crate's copy, so
//! editing one without re-vendoring the other fails `cargo test`.
//!
//! The loader header is the single source of truth; re-vendor with:
//!
//! ```sh
//! cp crates/pinkdb-ext-loader/include/pinkdb_extension.h \
//!    ext-sdk/c/include/pinkdb_extension.h
//! ```

/// Both copies of the ABI header, as raw bytes.
fn header_bytes(manifest: &std::path::Path) -> (Vec<u8>, Vec<u8>) {
    let source = manifest.join("include").join("pinkdb_extension.h");
    let vendored = manifest
        .join("..")
        .join("..")
        .join("ext-sdk")
        .join("c")
        .join("include")
        .join("pinkdb_extension.h");
    let read = |path: &std::path::Path| {
        std::fs::read(path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
    };
    (read(&source), read(&vendored))
}

#[test]
fn sdk_vendored_header_matches_the_loader_header() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let (source, vendored) = header_bytes(manifest);

    if source == vendored {
        return;
    }
    // Point at the first differing byte for a useful CI failure message.
    let detail = match source.iter().zip(vendored.iter()).position(|(a, b)| a != b) {
        Some(i) => format!("first difference at byte {i}"),
        None => "one copy is a strict prefix of the other".to_string(),
    };
    panic!(
        "the vendored SDK header has drifted from the loader header ({detail}); \
         re-vendor it with `cp crates/pinkdb-ext-loader/include/pinkdb_extension.h \
         ext-sdk/c/include/pinkdb_extension.h`"
    );
}

/// The header must stay C++-friendly: the SDK's C++ example relies on the
/// `extern "C"` guards, and dropping them would only fail at link time there.
#[test]
fn header_has_cpp_guards() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let (source, _) = header_bytes(manifest);
    let text = String::from_utf8(source).expect("the ABI header is UTF-8");
    assert!(
        text.contains("#ifdef __cplusplus") && text.contains("extern \"C\""),
        "the ABI header must keep its `#ifdef __cplusplus` / `extern \"C\"` guards"
    );
}
