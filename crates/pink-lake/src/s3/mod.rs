//! S3-compatible object storage (`s3` feature): [`S3Storage`] implements the
//! crate-internal [`Storage`] trait over the S3 REST API.
//!
//! Since wave 75 the **client** (SigV4 signing, endpoint addressing, retries,
//! conditional PUTs, listings, range reads) lives in the shared
//! `pinkdb-httpfs` crate; this module is the lake's storage adapter: it maps
//! paths to keys, maps the transport error model to [`LakeError`], and keeps
//! the lake-specific conditional-write policy (refuse by default when an
//! endpoint rejects `If-None-Match`/`If-Match`; the explicit single-writer
//! check-then-put fallback with a read-back byte compare).
//!
//! [`S3Config`] and [`DEFAULT_RETRIES`] are re-exported from the shared crate,
//! so the public configuration surface, the `AWS_*`/`PINKDB_S3_*`
//! conventions, and the retry semantics are unchanged by the extraction.
//! [`S3Config::resolve`] returns the shared `ConfigError`, which
//! [`S3Storage::new`] maps to the lake's `BadLocation`/`Auth` variants with
//! the historical messages.
//!
//! # Fast reads (wave 98b)
//!
//! [`Storage::read_range`] issues a signed `Range` GET (the shared client's
//! retries and timeouts apply); [`Storage::read`] downloads a whole object as
//! 8 MiB parts with a bounded concurrent pool
//! (`PINKDB_HTTPFS_PARALLELISM`, 0/1 = serial) and reassembles them in
//! offset order — a small object is still one request. All parts must carry
//! the same ETag; a mismatch (a concurrent replace) falls back to one atomic
//! GET, never a mixed body. [`Storage::stats`] exposes the client's request/
//! byte/wait counters.

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use pinkdb_httpfs::s3::{Condition, Get, ListPage, Put, ResolvedS3, S3Client};
use pinkdb_httpfs::{ConfigError, Error as HttpfsError};

use crate::storage::{Head, LocalCopy, ObjectInfo, Storage, StorageStats};
use crate::{LakeError, Result};

pub use pinkdb_httpfs::{DEFAULT_RETRIES, S3Config};

/// The most `ListObjectsV2` pages one `list` follows before giving up (a
/// broken endpoint repeating its continuation token must not loop forever).
const MAX_LIST_PAGES: usize = 10_000;

/// Whole-object downloads are split into parts of this size (wave 98b: 8 MiB,
/// inside the 8–16 MiB band the plan names). A small object stays one request;
/// a large one uses the link instead of one stream. Bounded by
/// `PINKDB_HTTPFS_PARALLELISM` (0/1 = one part at a time).
const DOWNLOAD_PART_BYTES: u64 = 8 * 1024 * 1024;

/// One S3 backend rooted at `s3://bucket/prefix`.
#[derive(Debug)]
pub(crate) struct S3Storage {
    client: S3Client,
    /// `s3://bucket[/prefix]`, what error paths and `Lake::path` show.
    root: PathBuf,
    /// The object-key prefix (no trailing slash; empty = bucket root).
    prefix: String,
    /// Set once a request has succeeded, so a later 404 does not have to
    /// probe whether the *bucket* is the missing thing.
    bucket_confirmed: AtomicBool,
    /// `ListObjectsV2` page cap (a safety valve; tests lower it).
    list_page_cap: usize,
}

impl S3Storage {
    /// Storage from explicit configuration (env fallbacks included).
    pub(crate) fn new(config: S3Config) -> Result<Self> {
        // Keep the lake's historical wire identity after the extraction.
        let resolved = config
            .with_user_agent(concat!("pink-lake/", env!("CARGO_PKG_VERSION")))
            .resolve()
            .map_err(map_config)?;
        let root = PathBuf::from(if resolved.prefix().is_empty() {
            format!("s3://{}", resolved.bucket())
        } else {
            format!("s3://{}/{}", resolved.bucket(), resolved.prefix())
        });
        Ok(Self::from_resolved(&resolved, root))
    }

    /// Storage for an `s3://bucket/prefix` URL: the same fields as
    /// [`S3Config::from_env`], credentials from the environment.
    pub(crate) fn from_url(bucket: &str, prefix: &str) -> Result<Self> {
        Self::new(S3Config::from_env(bucket).with_prefix(prefix))
    }

    fn from_resolved(resolved: &ResolvedS3, root: PathBuf) -> Self {
        Self {
            client: S3Client::from_resolved(resolved),
            prefix: resolved.prefix().to_string(),
            root,
            bucket_confirmed: AtomicBool::new(false),
            list_page_cap: MAX_LIST_PAGES,
        }
    }

    /// The full object key of a lake-relative path.
    fn key(&self, rel: &str) -> String {
        if self.prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}/{}", self.prefix, rel)
        }
    }

    /// Map one client failure to the lake's error type. `context` is the
    /// lake-level operation (the same shape the local backend reports, e.g.
    /// `loading data file "…"`); the client's own `METHOD URL` context is
    /// kept in the message.
    fn map(&self, context: &str, error: HttpfsError) -> LakeError {
        match error {
            HttpfsError::Transport {
                context: operation,
                source,
            } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::new(source.kind(), format!("{operation}: {source}")),
            },
            HttpfsError::Protocol {
                context: operation,
                message,
            } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!("{operation}: {message}")),
            },
            HttpfsError::Auth {
                context: operation,
                code,
                message,
            } => LakeError::Auth {
                path: self.root.clone(),
                message: format!("{operation}: S3 denied the request ({code}): {message}"),
            },
            HttpfsError::Status {
                context: operation,
                status,
                code,
                message,
            } => {
                if code == "NoSuchBucket" {
                    return LakeError::Io {
                        context: context.to_string(),
                        source: io::Error::other(format!(
                            "{operation}: the bucket does not exist (or is not visible to these \
                             credentials): S3 HTTP {status} NoSuchBucket: {message}"
                        )),
                    };
                }
                LakeError::Io {
                    context: context.to_string(),
                    source: io::Error::other(format!(
                        "{operation}: S3 HTTP {status} {code}: {message}"
                    )),
                }
            }
            HttpfsError::NotFound { context: operation } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{operation}: S3 object not found"),
                ),
            },
            HttpfsError::Unsupported {
                context: operation,
                message,
            } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!("{operation}: {message}")),
            },
        }
    }

    fn not_found(&self, rel: &str, context: &str) -> LakeError {
        LakeError::Io {
            context: context.to_string(),
            source: io::Error::new(
                io::ErrorKind::NotFound,
                format!("S3 object {} not found", self.abs(rel).display()),
            ),
        }
    }

    /// A 404 with no body (HEAD) can mean "the bucket does not exist" as
    /// easily as "the key does not exist". Once any request has succeeded
    /// the bucket is known; otherwise probe the bucket with one listing, so
    /// a missing bucket is a clear bucket error instead of "not a lake". A
    /// probe answered with 403 says nothing definitive (a denied
    /// `ListBucket` on a bucket that exists looks the same), so the original
    /// not-found stands rather than turning it into an auth error.
    fn confirm_bucket_after_404(&self, context: &str) -> Result<()> {
        if self.bucket_confirmed.load(Ordering::Relaxed) {
            return Ok(());
        }
        match self.client.list_objects("", "/", None) {
            Ok(_) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(HttpfsError::Auth { .. }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(self.map(context, e)),
        }
    }

    /// The whole object as concurrent range requests (wave 98b): 8 MiB parts,
    /// a bounded pool (`PINKDB_HTTPFS_PARALLELISM`), reassembled in order. A
    /// small object is still one request; all parts must report the same ETag,
    /// otherwise the object changed mid-read and the caller gets a fresh
    /// single GET (the pre-98b semantics: one atomic object).
    fn read_parallel(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Option<String>)> {
        let key = self.key(rel);
        let parallelism = pinkdb_httpfs::parallelism_from_env();
        if parallelism <= 1 {
            // The documented serial switch: one atomic GET, exactly the
            // pre-98b read path.
            return self.read_single_tagged(rel, context);
        }
        let first = match self.client.get_object_range(&key, 0, DOWNLOAD_PART_BYTES) {
            Ok(Get::Found {
                bytes,
                etag,
                content_range,
            }) => (bytes, etag, content_range),
            Ok(Get::NotFound) => {
                self.confirm_bucket_after_404(context)?;
                return Err(self.not_found(rel, context));
            }
            Err(HttpfsError::Status { status: 416, .. }) => {
                // An empty object: a range at 0 is unsatisfiable. Fall back to
                // the plain GET (which also carries the ETag).
                return self.read_single_tagged(rel, context);
            }
            Err(e) => return Err(self.map(context, e)),
        };
        self.bucket_confirmed.store(true, Ordering::Relaxed);
        let (first_bytes, first_etag, content_range) = first;
        let total = total_from_content_range(content_range.as_deref()).or(
            if first_bytes.len() as u64 == DOWNLOAD_PART_BYTES {
                self.stat(rel)?.map(|info| info.size)
            } else {
                Some(first_bytes.len() as u64)
            },
        );
        let Some(total) = total else {
            // No size from the endpoint: one whole GET is the only safe read.
            return self.read_single_tagged(rel, context);
        };
        if total <= first_bytes.len() as u64 {
            return Ok((first_bytes, first_etag));
        }
        let cap = self.client.max_object_bytes();
        if total > cap {
            return Err(LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!(
                    "the object is {total} bytes, over this client's {cap}-byte object cap \
                     (no multipart downloads)"
                )),
            });
        }
        // The remaining windows, fetched concurrently and reassembled in
        // offset order. `fetch` is the one counted/error-mapped call.
        let starts: Vec<u64> = (DOWNLOAD_PART_BYTES..total)
            .step_by(DOWNLOAD_PART_BYTES as usize)
            .collect();
        let parts = fetch_parts(self, &key, rel, &starts, total, parallelism, context)?;
        let mut etag = first_etag;
        let mut out = Vec::with_capacity(total.min(usize::MAX as u64) as usize);
        out.extend_from_slice(&first_bytes);
        for (start, bytes, part_etag) in parts {
            match (&etag, &part_etag) {
                (Some(a), Some(b)) if a != b => {
                    // The object changed between parts: a mixed body is never
                    // returned. One GET is atomic per S3 version.
                    return self.read_single_tagged(rel, context);
                }
                (None, Some(b)) => etag = Some(b.clone()),
                _ => {}
            }
            debug_assert_eq!(start, out.len() as u64);
            out.extend_from_slice(&bytes);
        }
        if out.len() as u64 != total {
            return Err(LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!(
                    "S3 parallel download of {} assembled {} of {total} bytes",
                    self.abs(rel).display(),
                    out.len()
                )),
            });
        }
        Ok((out, etag))
    }

    /// One unconditional GET (the pre-98b read path): an atomic object read
    /// and its ETag.
    fn read_single_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Option<String>)> {
        match self.client.get_object(&self.key(rel)) {
            Ok(Get::Found { bytes, etag, .. }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok((bytes, etag))
            }
            Ok(Get::NotFound) => {
                self.confirm_bucket_after_404(context)?;
                Err(self.not_found(rel, context))
            }
            Err(e) => Err(self.map(context, e)),
        }
    }
}

/// One downloaded part: its start offset, bytes and the object's ETag.
type DownloadPart = (u64, Vec<u8>, Option<String>);

/// Fetch the remaining windows of a parallel download with a bounded pool,
/// preserving offset order. Each part returns `(start, bytes, etag)`.
fn fetch_parts(
    storage: &S3Storage,
    key: &str,
    rel: &str,
    starts: &[u64],
    total: u64,
    parallelism: usize,
    context: &str,
) -> Result<Vec<DownloadPart>> {
    use std::sync::atomic::AtomicUsize;
    let results: Vec<Option<Result<DownloadPart>>> = (0..starts.len()).map(|_| None).collect();
    let results = std::sync::Mutex::new(results);
    let next = AtomicUsize::new(0);
    let workers = parallelism.clamp(1, starts.len().max(1));
    let fetch = |index: usize| -> Result<DownloadPart> {
        let start = starts[index];
        let len = (total - start).min(DOWNLOAD_PART_BYTES);
        match storage.client.get_object_range(key, start, len) {
            Ok(Get::Found { bytes, etag, .. }) if bytes.len() as u64 == len => {
                Ok((start, bytes, etag))
            }
            Ok(Get::Found { bytes, .. }) => Err(LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!(
                    "S3 range {start}..{} of {} returned {} bytes, expected {len}",
                    start + len,
                    storage.abs(rel).display(),
                    bytes.len()
                )),
            }),
            Ok(Get::NotFound) => Err(storage.not_found(rel, context)),
            Err(e) => Err(storage.map(context, e)),
        }
    };
    if workers == 1 {
        for index in 0..starts.len() {
            results.lock().unwrap()[index] = Some(fetch(index));
        }
    } else {
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= starts.len() {
                            break;
                        }
                        let part = fetch(index);
                        results.lock().unwrap()[index] = Some(part);
                    }
                });
            }
        });
    }
    let mut out = Vec::with_capacity(starts.len());
    for slot in results.into_inner().unwrap() {
        out.push(slot.expect("every download part is claimed")?);
    }
    Ok(out)
}

/// The total size in a `Content-Range: bytes start-end/total` value.
fn total_from_content_range(value: Option<&str>) -> Option<u64> {
    let after_slash = value?.rsplit_once('/')?.1.trim();
    if after_slash == "*" {
        None
    } else {
        after_slash.parse().ok()
    }
}

/// Map the shared config error to the lake's historical variants (same
/// wording, same paths).
fn map_config(error: ConfigError) -> LakeError {
    match error {
        ConfigError::BadLocation { location, message } => {
            LakeError::BadLocation { location, message }
        }
        ConfigError::Auth { path, message } => LakeError::Auth { path, message },
    }
}

impl Storage for S3Storage {
    fn abs(&self, rel: &str) -> PathBuf {
        if rel.is_empty() {
            self.root.clone()
        } else {
            self.root.join(rel)
        }
    }

    fn head(&self, rel: &str) -> Result<Head> {
        let context = format!("HEAD {}", self.abs(rel).display());
        match self.client.head_object(&self.key(rel)) {
            Ok(true) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(Head::File)
            }
            Ok(false) => {
                self.confirm_bucket_after_404(&context)?;
                Ok(Head::Missing)
            }
            Err(e) => Err(self.map(&context, e)),
        }
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        self.read_parallel(rel, context).map(|(bytes, _)| bytes)
    }

    fn read_optional(&self, rel: &str, context: &str) -> Result<Option<Vec<u8>>> {
        match self.client.get_object(&self.key(rel)) {
            Ok(Get::Found { bytes, .. }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(Some(bytes))
            }
            Ok(Get::NotFound) => {
                self.confirm_bucket_after_404(context)?;
                Ok(None)
            }
            Err(e) => Err(self.map(context, e)),
        }
    }

    fn read_range(&self, rel: &str, offset: u64, len: u64, context: &str) -> Result<Vec<u8>> {
        match self.client.get_object_range(&self.key(rel), offset, len) {
            Ok(Get::Found { bytes, .. }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                if bytes.len() as u64 != len {
                    return Err(LakeError::Io {
                        context: context.to_string(),
                        source: io::Error::other(format!(
                            "S3 range {offset}..{} of {} returned {} bytes, expected {len}",
                            offset.saturating_add(len),
                            self.abs(rel).display(),
                            bytes.len()
                        )),
                    });
                }
                Ok(bytes)
            }
            Ok(Get::NotFound) => {
                self.confirm_bucket_after_404(context)?;
                Err(self.not_found(rel, context))
            }
            Err(e) => Err(self.map(context, e)),
        }
    }

    fn ranged_reads(&self) -> bool {
        true
    }

    fn stats(&self) -> StorageStats {
        let stats = self.client.stats();
        StorageStats {
            requests: stats.requests,
            bytes: stats.bytes,
            wait_nanos: stats.wait_nanos,
        }
    }

    fn create_dir(&self, _rel: &str) -> Result<()> {
        // Object storage has no directories: a prefix is just key text.
        Ok(())
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let context = format!("PUT {}", self.abs(rel).display());
        match self
            .client
            .put_object(&self.key(rel), bytes, Condition::None)
        {
            Ok(Put::Written) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(())
            }
            Ok(_) => Err(self.map(
                &context,
                HttpfsError::Protocol {
                    context: context.clone(),
                    message: "an unconditional PUT reported a precondition failure".into(),
                },
            )),
            Err(e) => Err(self.map(&context, e)),
        }
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        let prefix = if rel.is_empty() {
            self.prefix.clone()
        } else {
            self.key(rel)
        };
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let context = format!("LIST {}", self.abs(rel).display());
        let mut names = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0usize;
        loop {
            pages += 1;
            if pages > self.list_page_cap {
                return Err(LakeError::Io {
                    context: context.clone(),
                    source: io::Error::other(format!(
                        "the listing did not finish after {} pages; the endpoint kept returning \
                         a continuation token (a broken ListObjectsV2 implementation?)",
                        self.list_page_cap
                    )),
                });
            }
            let page = self
                .client
                .list_objects(&prefix, "/", token.as_deref())
                .map_err(|e| self.map(&context, e))?;
            self.bucket_confirmed.store(true, Ordering::Relaxed);
            for key in page.keys {
                let Some(name) = key.strip_prefix(&prefix) else {
                    continue;
                };
                // Direct children only (a subdirectory is a `CommonPrefixes`
                // entry and never lands in `keys`); a `dir/` marker has no
                // name.
                if name.is_empty() || name.contains('/') {
                    continue;
                }
                names.push(name.to_string());
            }
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        names.sort();
        Ok(names)
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        let context = format!("HEAD {}", self.abs(rel).display());
        match self.client.stat_object(&self.key(rel)) {
            Ok(Some(meta)) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(Some(ObjectInfo {
                    size: meta.size,
                    modified_micros: meta.modified_micros,
                }))
            }
            Ok(None) => {
                self.confirm_bucket_after_404(&context)?;
                Ok(None)
            }
            Err(e) => Err(self.map(&context, e)),
        }
    }

    /// Every object under `rel`, recursively, relative to `rel`. One
    /// paginated ListObjectsV2 walk with no delimiter (the endpoint's own
    /// key ordering is not relied upon; the result is sorted).
    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        let prefix = if rel.is_empty() {
            self.prefix.clone()
        } else {
            self.key(rel)
        };
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let context = format!("LIST {}", self.abs(rel).display());
        let mut names = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0usize;
        loop {
            pages += 1;
            if pages > self.list_page_cap {
                return Err(LakeError::Io {
                    context: context.clone(),
                    source: io::Error::other(format!(
                        "the listing did not finish after {} pages; the endpoint kept returning \
                         a continuation token (a broken ListObjectsV2 implementation?)",
                        self.list_page_cap
                    )),
                });
            }
            let page: ListPage = self
                .client
                .list_objects(&prefix, "", token.as_deref())
                .map_err(|e| self.map(&context, e))?;
            self.bucket_confirmed.store(true, Ordering::Relaxed);
            for key in page.keys {
                let Some(name) = key.strip_prefix(&prefix) else {
                    continue;
                };
                if name.is_empty() || name.ends_with('/') {
                    // A zero-byte `dir/` marker is not a file.
                    continue;
                }
                names.push(name.to_string());
            }
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        let context = format!("DELETE {}", self.abs(rel).display());
        self.client
            .delete_object(&self.key(rel))
            .map_err(|e| self.map(&context, e))
    }

    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy> {
        let bytes = self.read(rel, context)?;
        // Stage into a private, owner-only directory: the copy is removed when
        // the `LocalCopy` drops, and other local users cannot read it while it
        // exists.
        let (dir, path) = crate::storage::private_temp_dir().map_err(|e| LakeError::Io {
            context: format!(
                "creating a private temp dir for {}",
                self.abs(rel).display()
            ),
            source: e,
        })?;
        if let Err(e) = crate::storage::write_private(&path, &bytes) {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(LakeError::Io {
                context: format!("writing the temp copy {}", path.display()),
                source: e,
            });
        }
        Ok(LocalCopy::Temp { file: path, dir })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(mock: &crate::mock_s3::MockS3, bucket: &str, prefix: &str) -> S3Storage {
        storage_with_secret(mock, bucket, prefix, "mock-secret")
    }

    fn storage_with_secret(
        mock: &crate::mock_s3::MockS3,
        bucket: &str,
        prefix: &str,
        secret: &str,
    ) -> S3Storage {
        S3Storage::new(
            S3Config::new(bucket)
                .with_prefix(prefix)
                .with_region("auto")
                .with_endpoint(mock.endpoint())
                .allow_http(true)
                .with_credentials(mock.access_key_id(), secret),
        )
        .unwrap()
    }

    #[test]
    fn debug_output_never_contains_credentials() {
        let config = S3Config::new("bucket")
            .with_credentials("AKIDVISIBLE", "TOP-SECRET-KEY")
            .with_session_token("SESSION-TOKEN-VALUE");
        let shown = format!("{config:?}");
        assert!(shown.contains("AKIDVISIBLE"), "{shown}");
        assert!(!shown.contains("TOP-SECRET-KEY"), "{shown}");
        assert!(!shown.contains("SESSION-TOKEN-VALUE"), "{shown}");
        // The resolved storage (and anything embedding it) redacts too.
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage_with_secret(&mock, "bucket", "p", "TOP-SECRET-KEY");
        let shown = format!("{storage:?}");
        assert!(!shown.contains("TOP-SECRET-KEY"), "{shown}");
    }

    /// The extraction maps the shared config errors to the lake's historical
    /// variants with the same messages.
    #[test]
    fn config_errors_keep_the_lake_shape() {
        let err = S3Storage::new(S3Config::new("bad_bucket").with_credentials("id", "secret"))
            .unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(
            err.to_string().contains("the bucket name must be 3"),
            "{err}"
        );

        let err = S3Storage::new(S3Config::new("bucket")).unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("no S3 credentials"), "{err}");

        let err = S3Storage::new(
            S3Config::new("bucket")
                .with_region("auto")
                .with_credentials("id", "secret"),
        )
        .unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("auto"), "{err}");
    }

    #[test]
    fn fabricated_signature_is_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage_with_secret(&mock, "ops-bucket", "ops", "not-the-secret");
        let err = storage
            .read("object.bin", "reading object.bin")
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("SignatureDoesNotMatch"), "{err}");
        assert!(mock.keys().is_empty(), "nothing was written");
        assert_eq!(mock.verified_requests(), 0, "no request verified");
    }

    /// A wrong body hash and a stale date are two distinct rejections.
    #[test]
    fn body_hash_and_date_skew_are_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage(&mock, "ops-bucket", "ops");

        mock.tamper_next_body_hash();
        let err = storage
            .read("object.bin", "reading object.bin")
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("x-amz-content-sha256"), "{err}");

        mock.set_clock_offset_secs(3600);
        let err = storage
            .read("object.bin", "reading object.bin")
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("RequestTimeTooSkewed"), "{err}");
        mock.set_clock_offset_secs(0);
        // Back in time: the same request verifies (and the mock records it).
        mock.put_object("ops/object.bin", b"{}");
        assert_eq!(storage.read("object.bin", "reading").unwrap(), b"{}");
        assert!(mock.verified_requests() >= 1);
    }

    #[test]
    fn whole_object_reads_use_concurrent_parts() {
        let mock = crate::mock_s3::MockS3::start();
        let size = 20 * 1024 * 1024 + 123;
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        mock.put_object("ops/big", &bytes);
        let storage = storage(&mock, "ops-bucket", "ops");

        let got = storage.read("big", "reading big").unwrap();
        assert_eq!(got.len(), bytes.len());
        assert_eq!(got, bytes, "the parts must reassemble in order");
        let all_ranges = mock.ranges();
        let mut ranges: Vec<String> = all_ranges
            .iter()
            .filter(|r| !r.is_empty())
            .cloned()
            .collect();
        ranges.sort();
        if pinkdb_httpfs::parallelism_from_env() > 1 {
            assert_eq!(ranges.len(), 3, "8 MiB parts: {ranges:?}");
            assert_eq!(
                ranges,
                vec![
                    "bytes=0-8388607".to_string(),
                    "bytes=16777216-20971642".to_string(),
                    "bytes=8388608-16777215".to_string()
                ]
            );
        } else {
            assert!(
                ranges.is_empty(),
                "PINKDB_HTTPFS_PARALLELISM=1 is one whole GET: {ranges:?}"
            );
        }

        // A small object is still one range request.
        mock.put_object("ops/small", b"tiny");
        let before = mock.ranges().iter().filter(|r| !r.is_empty()).count();
        assert_eq!(storage.read("small", "reading small").unwrap(), b"tiny");
        let after = mock.ranges().iter().filter(|r| !r.is_empty()).count();
        assert_eq!(after - before, 1, "a small object is one request");
    }

    /// A broken endpoint repeating its continuation token cannot loop
    /// forever: the paging cap stops `list` with a clear error.
    #[test]
    fn listing_paging_is_capped() {
        let mock = crate::mock_s3::MockS3::start();
        mock.set_always_truncated(true);
        let mut storage = storage(&mock, "ops-bucket", "ops");
        storage.list_page_cap = 3;
        let err = storage.list("snapshots").unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(
            err.to_string().contains("did not finish after 3 pages"),
            "{err}"
        );
        let pages = mock
            .requests()
            .iter()
            .filter(|r| r.contains("list-type=2"))
            .count();
        assert_eq!(pages, 3, "exactly the cap's pages went out");
    }

    /// A missing bucket is a clear bucket error on both a GET body and a
    /// bodiless HEAD probe (not "not a lake").
    #[test]
    fn no_such_bucket_is_reported_as_a_bucket_error() {
        let mock = crate::mock_s3::MockS3::start();
        mock.restrict_buckets(&["real-bucket"]);
        let ghost = storage(&mock, "ghost-bucket", "lake");

        let err = ghost.read("object.bin", "reading object.bin").unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(err.to_string().contains("bucket does not exist"), "{err}");

        let err = ghost.head("object.bin").unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(err.to_string().contains("bucket does not exist"), "{err}");

        // The existing bucket still reads as a lake-less prefix.
        let real = storage(&mock, "real-bucket", "lake");
        assert_eq!(real.head("object.bin").unwrap(), Head::Missing);
    }
}
