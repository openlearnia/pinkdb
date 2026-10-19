//! Remote Arrow IPC files (`remote` feature): a whole-object fetch over the
//! shared [`pinkdb_httpfs`] transport.
//!
//! IPC has no row-group/footer range protocol the way parquet does: the
//! footer is a length-prefixed trailer and record batches are not
//! independently addressable without reading the footer (and often the whole
//! stream). The reader therefore materializes the object with one
//! `fetch()`; there is intentionally **no** range read here.
//!
//! `https://` and `s3://` are served by the shared transport; plain `http://`
//! follows its default (refused unless `PINKDB_HTTPFS_ALLOW_HTTP=1`).

use std::path::Path;

use pinkdb::PinkDbError;
#[cfg(feature = "remote")]
use pinkdb::Result;

/// Whether `path` names a remote object (https, plain-http test endpoints, or
/// S3). Feature-independent so the parser can claim remote extensions and a
/// lean build can report the missing feature clearly.
pub fn is_remote(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("https://") || lower.starts_with("http://") || lower.starts_with("s3://")
}

/// The path component used for extension/table-name decisions (query and
/// fragment stripped); the transport still receives the full URL.
pub fn path_only(path: &str) -> &str {
    path.split(['?', '#']).next().unwrap_or(path)
}

/// The table name a remote path produces (its last component's stem).
pub fn table_name(path: &str) -> String {
    Path::new(path_only(path))
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("arrow")
        .to_string()
}

/// The clear lean-build error for a remote path.
pub fn missing_feature(path: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "cannot read remote arrow object \"{path}\": this build has no remote arrow support; \
         rebuild pinkdb-arrow with the `remote` feature (the CLI's `httpfs` feature enables \
         it together with `arrow`)"
    ))
}

/// Fetch a remote IPC file (whole object; see the module docs).
#[cfg(feature = "remote")]
pub(crate) fn fetch(path: &str) -> Result<Vec<u8>> {
    let fetcher = pinkdb_httpfs::open(path)
        .map_err(|e| PinkDbError::Execution(format!("cannot open \"{path}\": {e}")))?;
    fetcher
        .fetch()
        .map_err(|e| PinkDbError::Execution(format!("cannot read \"{path}\": {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_detection_paths_and_names() {
        assert!(is_remote("https://h/x.arrow"));
        assert!(is_remote("HTTP://h/x.arrow"));
        assert!(is_remote("s3://b/x.arrow"));
        assert!(!is_remote("x.arrow"));
        assert_eq!(
            path_only("https://h/a/x.arrow?token=1#f"),
            "https://h/a/x.arrow"
        );
        assert_eq!(table_name("https://h/a/x.arrow?token=1"), "x");
        assert_eq!(table_name("s3://b/x.feather"), "x");
        assert!(
            missing_feature("https://h/x.arrow")
                .to_string()
                .contains("remote")
        );
    }
}
