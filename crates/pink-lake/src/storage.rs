//! The internal storage abstraction behind a lake.
//!
//! A lake needs six operations and nothing more: read one object, read with a
//! version tag, create-if-absent, replace, list the files under a prefix,
//! and delete. [`LocalStorage`](crate::local::LocalStorage) is the original
//! directory implementation (wave 34/35, unchanged files/atomicity/errors);
//! the feature-gated S3 backend implements the same operations over the S3
//! REST API (AWS S3, Cloudflare R2, MinIO, …).
//!
//! Two operations carry the single-writer contract:
//!
//! - [`Storage::put_new`] publishes a snapshot manifest **only if it does not
//!   exist yet** (`If-None-Match: *` over S3; a hard link locally), so two
//!   writers racing for one version cannot both win.
//! - [`Storage::put_if_match`] replaces `catalog.json` **only if it still
//!   carries the version tag read moments earlier** (`If-Match: <etag>` over
//!   S3, HTTP 412 → conflict; a file-identity re-check locally). The lake
//!   additionally re-parses the catalog right before this call, so a stale
//!   handle fails with the familiar "another writer published version N"
//!   error even on backends that cannot enforce conditional writes.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::{LakeError, Result};

/// What [`Storage::head`] found at a lake-relative path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Head {
    /// Nothing is there.
    Missing,
    /// A regular file (local) or an object (S3).
    File,
    /// Something exists but is not a regular file: a local directory, fifo,
    /// device, … Object storage never reports this (a `dir/` key is still an
    /// object).
    NotAFile,
}

/// An opaque storage version tag.
///
/// For S3 this is the object's `ETag` **including its quotes** (sent back
/// verbatim as `If-Match: <etag>`); for local files it is the file's identity
/// (byte length + modification time in nanoseconds). A tag is captured by
/// [`Storage::read_tagged`] when the lake reads a manifest and is required to
/// still match when the same manifest is replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionTag(pub(crate) String);

impl VersionTag {
    /// The tag as it goes on the wire (an S3 `If-Match` value).
    #[cfg(feature = "s3")]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// A local path to one lake object.
///
/// Local storage hands out the file itself; object storage downloads the
/// object into a unique temp file (engine `.pink` files must be opened by
/// path, and the v1 loader is the only user of this). The temp copy is
/// removed when the value is dropped.
#[derive(Debug)]
pub(crate) enum LocalCopy {
    /// The storage's own file; nothing to clean up.
    Path(PathBuf),
    /// A downloaded temp copy, removed on drop (only constructed by object
    /// storage, so it is dead code in a default build).
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    Temp(PathBuf),
}

impl LocalCopy {
    pub(crate) fn path(&self) -> &Path {
        match self {
            LocalCopy::Path(path) | LocalCopy::Temp(path) => path,
        }
    }
}

impl Drop for LocalCopy {
    fn drop(&mut self) {
        if let LocalCopy::Temp(path) = self {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// One backend of lake objects: a local directory or an S3 bucket+prefix.
///
/// All paths are lake-relative (`catalog.json`, `snapshots/<v>.json`,
/// `data/<table>/<uuid>.vortex`); [`Storage::abs`] turns one into the
/// backend's absolute form for error messages.
pub(crate) trait Storage: fmt::Debug {
    /// The absolute form of a lake-relative path: a filesystem path locally,
    /// an `s3://bucket/prefix/rel` URL for S3. `abs("")` is the lake root.
    fn abs(&self, rel: &str) -> PathBuf;

    /// What is at `rel` (missing / a regular file / something else).
    fn head(&self, rel: &str) -> Result<Head>;

    /// Read one object. `context` names the failed operation in the same
    /// style as the local backend (`reading <abs>`, `loading data file "…"`).
    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>>;

    /// [`Storage::read`] plus the version tag for a later conditional
    /// replace. The tag must come from the same read the caller reasons
    /// about.
    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)>;

    /// Create the directory `rel` (and parents) — a no-op for object
    /// storage, which has no directories.
    fn create_dir(&self, rel: &str) -> Result<()>;

    /// Atomically replace one object: locally a unique fsynced temp file
    /// renamed into place; over S3 a single `PUT` (S3 object writes are
    /// atomic — a reader sees the old or the new bytes, never a mix).
    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()>;

    /// Publish `bytes` **only if `rel` does not exist yet**: `Ok(false)` when
    /// it already existed (locally a hard link, so an existing file or
    /// planted symlink is never replaced; over S3 `If-None-Match: *` →
    /// HTTP 412).
    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool>;

    /// Replace `rel` **only if it still carries `expected`**: `Ok(false)`
    /// when the tag changed (over S3 `If-Match: <etag>`; locally the file
    /// identity is re-checked). A concurrent writer's change is never
    /// silently clobbered.
    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool>;

    /// File names directly under `rel`, sorted. A missing location reads as
    /// empty; subdirectories are not listed.
    fn list(&self, rel: &str) -> Result<Vec<String>>;

    /// Delete one object. The lake itself never deletes published data (every
    /// version stays loadable), so no lake code path calls this today; it is
    /// part of the storage surface for leftover cleanup and tests.
    #[allow(dead_code)]
    fn delete(&self, rel: &str) -> Result<()>;

    /// A local path for one object (see [`LocalCopy`]). Only the v1 `.pink`
    /// loader uses this: the engine opens files by path.
    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy>;

    /// Whether anything exists at `rel` (a directory counts, mirroring
    /// `Path::exists`).
    fn exists(&self, rel: &str) -> Result<bool> {
        Ok(!matches!(self.head(rel)?, Head::Missing))
    }
}

/// A parsed lake location: a local directory path or an `s3://` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Location {
    Local(PathBuf),
    S3 { bucket: String, prefix: String },
}

impl Location {
    /// Parse a lake location string: `s3://bucket[/prefix…]` is object
    /// storage; anything else is a local path (a directory literally named
    /// `s3://…` is not a thing).
    pub(crate) fn parse(location: &str) -> Result<Self> {
        match location.strip_prefix("s3://") {
            Some(rest) => Self::parse_s3(location, rest),
            None => Ok(Location::Local(PathBuf::from(location))),
        }
    }

    fn parse_s3(original: &str, rest: &str) -> Result<Self> {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix.trim_end_matches('/')),
            None => (rest, ""),
        };
        let valid_bucket = !bucket.is_empty()
            && bucket.len() <= 255
            && bucket.bytes().all(|b| {
                b.is_ascii_lowercase()
                    || b.is_ascii_uppercase()
                    || b.is_ascii_digit()
                    || b == b'-'
                    || b == b'.'
            });
        if !valid_bucket {
            return Err(LakeError::BadLocation {
                location: original.to_string(),
                message: "the bucket name must be non-empty and made of ASCII letters, digits, \
                          `.` and `-` (expected s3://bucket/prefix)"
                    .to_string(),
            });
        }
        if prefix.starts_with('/') {
            return Err(LakeError::BadLocation {
                location: original.to_string(),
                message: "the prefix must not start with `/`".to_string(),
            });
        }
        Ok(Location::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paths_pass_through() {
        assert_eq!(
            Location::parse("mylake").unwrap(),
            Location::Local(PathBuf::from("mylake"))
        );
        assert_eq!(
            Location::parse("/tmp/mylake").unwrap(),
            Location::Local(PathBuf::from("/tmp/mylake"))
        );
        // `s3:` without the full scheme is a local path.
        assert_eq!(
            Location::parse("s3:not-a-url").unwrap(),
            Location::Local(PathBuf::from("s3:not-a-url"))
        );
    }

    #[test]
    fn s3_urls_parse_bucket_and_prefix() {
        let cases = [
            ("s3://bucket", "bucket", ""),
            ("s3://bucket/", "bucket", ""),
            ("s3://bucket//", "bucket", ""),
            ("s3://bucket/prefix", "bucket", "prefix"),
            ("s3://bucket/a/b/c", "bucket", "a/b/c"),
            ("s3://bucket/a/b/c/", "bucket", "a/b/c"),
            ("s3://my.bucket-1/x", "my.bucket-1", "x"),
            ("s3://UPPER/x", "UPPER", "x"),
        ];
        for (url, bucket, prefix) in cases {
            assert_eq!(
                Location::parse(url).unwrap(),
                Location::S3 {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string()
                },
                "{url}"
            );
        }
    }

    #[test]
    fn bad_s3_urls_are_clear_errors() {
        for bad in ["s3://", "s3:///prefix", "s3://bucket//x", "s3://bad_bucket/x", "s3://b ck/x"] {
            let err = Location::parse(bad).unwrap_err();
            assert!(matches!(err, LakeError::BadLocation { .. }), "{bad}: {err:?}");
            assert!(err.to_string().contains("invalid lake location"), "{err}");
        }
    }
}
