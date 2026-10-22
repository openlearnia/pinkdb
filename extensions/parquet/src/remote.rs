//! Remote parquet objects (`remote` feature): the [`RangeSource`] adapter over
//! the shared `pinkdb_httpfs` transport.
//!
//! Wave 98b moved the transport-agnostic ranged machinery (footer reads,
//! coalescing, bounded read-ahead, counters) into [`crate::ranged`]; this
//! module only adapts a [`Fetcher`] to it. The lake's Parquet provider uses
//! the same machinery with a lake `DataFile` as the source, so the two paths
//! cannot drift.
//!
//! `https://` and `s3://` are served by the shared transport; plain `http://`
//! follows its default (refused unless `PINKDB_HTTPFS_ALLOW_HTTP=1`).

#[cfg(feature = "remote")]
use std::sync::Arc;

use pinkdb::PinkDbError;
#[cfg(feature = "remote")]
use pinkdb::Result;
#[cfg(feature = "remote")]
use pinkdb_httpfs::Fetcher;

#[cfg(feature = "remote")]
use crate::ranged::{RangeSource, RemoteSpec};

/// Whether `path` names a remote object (https, plain-http test endpoints, or
/// S3). Feature-independent so the parsers can claim remote extensions and
/// give a clear "rebuild with `remote`" error in a lean build.
pub fn is_remote(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("https://") || lower.starts_with("http://") || lower.starts_with("s3://")
}

/// The path component used for extension/hive decisions (query/fragment
/// stripped). The transport still receives the full URL.
pub fn path_only(path: &str) -> &str {
    path.split(['?', '#']).next().unwrap_or(path)
}

/// The clear lean-build error for a remote path (used when the feature is
/// off; present in both builds so the parsers can name the fix).
#[allow(dead_code)]
pub fn missing_feature(path: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "cannot read remote parquet object \"{path}\": this build has no remote parquet support; \
         rebuild pinkdb-parquet with the `remote` feature (the CLI's `httpfs` feature enables \
         it together with `parquet`)"
    ))
}

/// [`RangeSource`] over one [`pinkdb_httpfs::Fetcher`]. The transport owns
/// retries and connection pooling; this adapter only forwards the window and
/// renders its error for the scan's context.
#[cfg(feature = "remote")]
#[derive(Debug)]
struct FetcherSource {
    fetcher: Arc<dyn Fetcher>,
    len: u64,
}

#[cfg(feature = "remote")]
impl RangeSource for FetcherSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_range(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, String> {
        self.fetcher
            .fetch_range(offset, len)
            .map_err(|e| e.to_string())
    }

    fn parallelism(&self) -> usize {
        pinkdb_httpfs::parallelism_from_env()
    }
}

/// Open one remote object. A known size keeps the reader ranged; an unknown
/// size is the documented whole-object fallback.
#[cfg(feature = "remote")]
pub(crate) fn open_spec(path: &str) -> Result<RemoteSpec> {
    let http_error = |error: pinkdb_httpfs::Error| {
        PinkDbError::Execution(format!("remote parquet read failed: {error}"))
    };
    let fetcher: Arc<dyn Fetcher> = Arc::from(pinkdb_httpfs::open(path).map_err(http_error)?);
    let parallelism = pinkdb_httpfs::parallelism_from_env();
    match fetcher.size().map_err(http_error)? {
        Some(len) => Ok(RemoteSpec {
            source: Arc::new(FetcherSource {
                fetcher: Arc::clone(&fetcher),
                len,
            }),
            len,
            whole: None,
            parallelism,
        }),
        None => {
            let bytes = fetcher.fetch().map_err(http_error)?;
            let len = bytes.len() as u64;
            Ok(RemoteSpec {
                source: Arc::new(FetcherSource { fetcher, len }),
                len,
                whole: Some(bytes::Bytes::from(bytes)),
                parallelism,
            })
        }
    }
}

/// Read one remote object's metadata (footer only; no data pages).
#[cfg(feature = "remote")]
pub(crate) fn read_meta(path: &str) -> Result<crate::file_meta::FileMeta> {
    let spec = open_spec(path)?;
    crate::file_meta::read_file_meta_remote(path, &spec)
}

/// Resolve one remote object: its metadata plus the transport handle the scan
/// reuses. Refuses globs (a remote path is one object).
#[cfg(feature = "remote")]
pub(crate) fn resolve_file(
    path: &str,
) -> Result<(crate::file_meta::FileMeta, crate::ranged::RemoteSpec)> {
    if has_glob(path) {
        return Err(PinkDbError::Execution(format!(
            "remote parquet paths are single objects, not globs: \"{path}\" contains a glob \
             metacharacter; list the objects yourself or read a single key"
        )));
    }
    let spec = open_spec(path)?;
    let meta = crate::file_meta::read_file_meta_remote(path, &spec)?;
    Ok((meta, spec))
}

/// Whether the path component carries a glob metacharacter (the query is
/// ignored, so a signed URL's `?` is not a glob).
#[cfg(feature = "remote")]
pub fn has_glob(path: &str) -> bool {
    path_only(path).contains(['*', '?', '['])
}

#[cfg(all(test, feature = "remote"))]
mod tests {
    use super::*;

    #[test]
    fn remote_detection_and_path_component() {
        assert!(is_remote("https://h/x.parquet"));
        assert!(is_remote("HTTP://h/x.parquet"));
        assert!(is_remote("s3://b/x.parquet"));
        assert!(!is_remote("x.parquet"));
        assert!(!is_remote("file:///x.parquet"));
        assert_eq!(
            path_only("https://h/x.parquet?token=1#frag"),
            "https://h/x.parquet"
        );
        assert!(!has_glob("https://h/x.parquet?token=*"));
        assert!(has_glob("https://h/*.parquet"));
    }
}
