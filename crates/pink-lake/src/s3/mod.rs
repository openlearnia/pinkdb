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

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use pinkdb_httpfs::s3::{Condition, Get, ListPage, Put, ResolvedS3, S3Client};
use pinkdb_httpfs::{ConfigError, Error as HttpfsError};

use crate::storage::{Head, LocalCopy, ObjectInfo, Storage, VersionTag};
use crate::{LakeError, Result};

pub use pinkdb_httpfs::{DEFAULT_RETRIES, S3Config};

/// The most `ListObjectsV2` pages one `list` follows before giving up (a
/// broken endpoint repeating its continuation token must not loop forever).
const MAX_LIST_PAGES: usize = 10_000;

/// The message every refused degraded write carries. The task of the client
/// is to be safe by default: a check-then-put fallback cannot keep the
/// single-writer contract, so it needs an explicit opt-in.
const DEGRADED_HINT: &str = "this endpoint does not support conditional writes; concurrent-safe \
    saves are unavailable. Set PINKDB_S3_ALLOW_DEGRADED=1 (or S3Config::allow_degraded(true)) to \
    accept a SINGLE-WRITER guarantee with a small race window";

/// One S3 backend rooted at `s3://bucket/prefix`.
#[derive(Debug)]
pub(crate) struct S3Storage {
    client: S3Client,
    /// `s3://bucket[/prefix]`, what error paths and `Lake::path` show.
    root: PathBuf,
    /// The object-key prefix (no trailing slash; empty = bucket root).
    prefix: String,
    /// Set once the endpoint has proven it does not honor conditional
    /// headers. In the default mode later writes refuse
    /// ([`LakeError::UnsupportedEndpoint`]); with the explicit opt-in they
    /// use the single-writer check-then-put fallback.
    degraded: AtomicBool,
    /// Whether the degraded fallback is explicitly accepted.
    allow_degraded: bool,
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
            degraded: AtomicBool::new(false),
            allow_degraded: resolved.allow_degraded(),
            bucket_confirmed: AtomicBool::new(false),
            list_page_cap: MAX_LIST_PAGES,
        }
    }

    /// The refusal every write gets when the endpoint rejects conditionals
    /// and the degraded fallback was not explicitly accepted.
    fn unsupported_conditional_error(&self) -> LakeError {
        LakeError::UnsupportedEndpoint {
            path: self.root.clone(),
            message: DEGRADED_HINT.to_string(),
        }
    }

    /// Before a write: refuse when the endpoint is known to reject
    /// conditional writes and the fallback is not explicitly allowed.
    fn check_write_allowed(&self) -> Result<()> {
        if self.degraded.load(Ordering::Relaxed) && !self.allow_degraded {
            return Err(self.unsupported_conditional_error());
        }
        Ok(())
    }

    /// Note that the endpoint cannot do conditional writes; in the default
    /// mode that is a hard error, in opt-in mode it selects the fallback.
    fn note_conditional_unsupported(&self) -> Result<()> {
        self.degraded.store(true, Ordering::Relaxed);
        if self.allow_degraded {
            Ok(())
        } else {
            Err(self.unsupported_conditional_error())
        }
    }

    /// A degraded fallback write replaced/overwrote an object in a way the
    /// read-back could not confirm: the caller must treat it as a conflict.
    fn degraded_conflict(&self, rel: &str, detail: &str) -> LakeError {
        LakeError::Conflict {
            path: self.abs(rel),
            message: format!(
                "{detail}; the endpoint does not support conditional writes, so this fallback is \
                 only safe for a single writer (reopen the lake and check `.lake versions`)"
            ),
        }
    }

    /// After a degraded fallback PUT: read the object back and compare the
    /// bytes. A mismatch means another writer replaced it in the window — a
    /// conflict, never a silent success.
    fn verify_written(&self, key: &str, rel: &str, bytes: &[u8]) -> Result<()> {
        match self.client.get_object(key) {
            Ok(Get::Found { bytes: current, .. }) if current == bytes => Ok(()),
            Ok(Get::Found { .. }) => Err(self.degraded_conflict(
                rel,
                "the object changed again between this write and its read-back",
            )),
            Ok(Get::NotFound) => {
                Err(self.degraded_conflict(rel, "the object disappeared right after this write"))
            }
            Err(e) => Err(self.map(&format!("GET s3://{}/{key}", self.client.bucket()), e)),
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
        match self.client.get_object(&self.key(rel)) {
            Ok(Get::Found { bytes, .. }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(bytes)
            }
            Ok(Get::NotFound) => {
                self.confirm_bucket_after_404(context)?;
                Err(self.not_found(rel, context))
            }
            Err(e) => Err(self.map(context, e)),
        }
    }

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        match self.client.get_object(&self.key(rel)) {
            Ok(Get::Found {
                bytes,
                etag: Some(etag),
                ..
            }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok((bytes, VersionTag(etag)))
            }
            Ok(Get::Found { etag: None, .. }) => Err(self.map(
                context,
                HttpfsError::Protocol {
                    context: context.to_string(),
                    message: "response carried no ETag; conditional writes are impossible".into(),
                },
            )),
            Ok(Get::NotFound) => {
                self.confirm_bucket_after_404(context)?;
                Err(self.not_found(rel, context))
            }
            Err(e) => Err(self.map(context, e)),
        }
    }

    fn create_dir(&self, _rel: &str) -> Result<()> {
        // Object storage has no directories: a prefix is just key text.
        Ok(())
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        self.check_write_allowed()?;
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

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        self.check_write_allowed()?;
        let key = self.key(rel);
        let context = format!("PUT {}", self.abs(rel).display());
        if !self.degraded.load(Ordering::Relaxed) {
            match self.client.put_object(&key, bytes, Condition::IfNoneMatch) {
                Ok(Put::Written) => {
                    self.bucket_confirmed.store(true, Ordering::Relaxed);
                    return Ok(true);
                }
                Ok(Put::PreconditionFailed) => {
                    self.bucket_confirmed.store(true, Ordering::Relaxed);
                    return Ok(false);
                }
                Ok(Put::ConditionalUnsupported) => {
                    self.note_conditional_unsupported()?;
                }
                Err(e) => return Err(self.map(&context, e)),
            }
        }
        // Explicit single-writer fallback (the endpoint rejects
        // `If-None-Match: *`): check-then-put, then read back and
        // byte-compare, with the documented window.
        match self.client.head_object(&key) {
            Ok(true) => Ok(false),
            Ok(false) => match self.client.put_object(&key, bytes, Condition::None) {
                Ok(Put::Written) => {
                    self.verify_written(&key, rel, bytes)?;
                    Ok(true)
                }
                Ok(_) => Err(self.map(
                    &context,
                    HttpfsError::Protocol {
                        context: context.clone(),
                        message: "unconditional PUT was not written".into(),
                    },
                )),
                Err(e) => Err(self.map(&context, e)),
            },
            Err(e) => Err(self.map(&context, e)),
        }
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        self.check_write_allowed()?;
        let key = self.key(rel);
        let context = format!("PUT {}", self.abs(rel).display());
        if !self.degraded.load(Ordering::Relaxed) {
            match self
                .client
                .put_object(&key, bytes, Condition::IfMatch(expected.as_str()))
            {
                Ok(Put::Written) => {
                    self.bucket_confirmed.store(true, Ordering::Relaxed);
                    return Ok(true);
                }
                Ok(Put::PreconditionFailed) => {
                    self.bucket_confirmed.store(true, Ordering::Relaxed);
                    return Ok(false);
                }
                Ok(Put::ConditionalUnsupported) => {
                    self.note_conditional_unsupported()?;
                }
                Err(e) => return Err(self.map(&context, e)),
            }
        }
        // Explicit single-writer fallback: compare the current ETag, PUT,
        // then read back and byte-compare (documented check-then-put window).
        match self.client.get_object(&key) {
            Ok(Get::Found {
                etag: Some(current),
                ..
            }) => {
                if current != expected.as_str() {
                    return Ok(false);
                }
                match self.client.put_object(&key, bytes, Condition::None) {
                    Ok(Put::Written) => {
                        self.verify_written(&key, rel, bytes)?;
                        Ok(true)
                    }
                    Ok(_) => Err(self.map(
                        &context,
                        HttpfsError::Protocol {
                            context: context.clone(),
                            message: "unconditional PUT was not written".into(),
                        },
                    )),
                    Err(e) => Err(self.map(&context, e)),
                }
            }
            Ok(Get::Found { etag: None, .. }) => Err(self.map(
                &context,
                HttpfsError::Protocol {
                    context: context.clone(),
                    message: "response carried no ETag; conditional writes are impossible".into(),
                },
            )),
            // Gone since the read: the caller's catalog was replaced.
            Ok(Get::NotFound) => Ok(false),
            Err(e) => Err(self.map(&context, e)),
        }
    }

    fn uses_degraded_fallback(&self) -> bool {
        self.allow_degraded && self.degraded.load(Ordering::Relaxed)
    }

    /// The degraded-mode repair: catalog and snapshot are byte-identical by
    /// construction, so if a racing writer's snapshot replace slipped
    /// through, restore `snapshots/<version>.json` from the authoritative
    /// `catalog.json` bytes. A no-op on conditional-write endpoints and
    /// without the degraded opt-in.
    fn reconcile_snapshot(&self, version: u64, bytes: &[u8]) -> Result<()> {
        if !self.uses_degraded_fallback() {
            return Ok(());
        }
        let rel = format!("snapshots/{version}.json");
        let key = self.key(&rel);
        let context = format!("GET {}", self.abs(&rel).display());
        match self.client.get_object(&key) {
            Ok(Get::Found { bytes: current, .. }) if current == bytes => Ok(()),
            Ok(Get::Found { .. }) | Ok(Get::NotFound) => {
                let put_context = format!("PUT {}", self.abs(&rel).display());
                match self.client.put_object(&key, bytes, Condition::None) {
                    Ok(Put::Written) => Ok(()),
                    Ok(_) => Err(self.map(
                        &put_context,
                        HttpfsError::Protocol {
                            context: put_context.clone(),
                            message: "unconditional PUT was not written".into(),
                        },
                    )),
                    Err(e) => Err(self.map(&put_context, e)),
                }
            }
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
    use std::time::Duration;

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

    fn storage_allow_degraded(
        mock: &crate::mock_s3::MockS3,
        bucket: &str,
        prefix: &str,
    ) -> S3Storage {
        S3Storage::new(
            S3Config::new(bucket)
                .with_prefix(prefix)
                .with_region("auto")
                .with_endpoint(mock.endpoint())
                .allow_http(true)
                .allow_degraded(true)
                .with_credentials(mock.access_key_id(), "mock-secret"),
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

    /// Direct backend-level coverage (no `Lake`): the exact trait surface the
    /// lake relies on, including `delete` and `local_copy`.
    #[test]
    fn storage_trait_roundtrip_over_the_mock() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage(&mock, "ops-bucket", "ops");

        assert_eq!(storage.head("catalog.json").unwrap(), Head::Missing);
        storage.create_dir("").unwrap(); // a no-op over object storage
        assert_eq!(storage.abs(""), PathBuf::from("s3://ops-bucket/ops"));
        assert_eq!(
            storage.abs("snapshots/2.json"),
            PathBuf::from("s3://ops-bucket/ops/snapshots/2.json")
        );

        // put replaces; put_new is create-only, and the object key carries
        // the prefix.
        storage.put("snapshots/1.json", b"one").unwrap();
        assert_eq!(
            storage.read("snapshots/1.json", "reading one").unwrap(),
            b"one"
        );
        assert_eq!(mock.object("ops/snapshots/1.json").unwrap(), b"one");
        assert!(!storage.put_new("snapshots/1.json", b"two").unwrap());
        assert_eq!(
            storage.read("snapshots/1.json", "reading one").unwrap(),
            b"one",
            "put_new never replaces"
        );
        assert!(storage.put_new("snapshots/2.json", b"two").unwrap());

        // read_tagged + put_if_match: a stale tag is rejected, the fresh one
        // lands.
        storage.put("catalog.json", b"cat-1").unwrap();
        let (bytes, tag) = storage
            .read_tagged("catalog.json", "reading catalog.json")
            .unwrap();
        assert_eq!(bytes, b"cat-1");
        assert!(
            storage
                .put_if_match("catalog.json", b"cat-2", &tag)
                .unwrap()
        );
        assert_eq!(
            storage
                .read("catalog.json", "reading catalog.json")
                .unwrap(),
            b"cat-2"
        );
        assert!(
            !storage
                .put_if_match("catalog.json", b"cat-3", &tag)
                .unwrap()
        );
        assert_eq!(
            storage
                .read("catalog.json", "reading catalog.json")
                .unwrap(),
            b"cat-2",
            "a stale conditional replace does not land"
        );

        // head / list: files only, direct children only.
        assert_eq!(storage.head("catalog.json").unwrap(), Head::File);
        assert_eq!(
            storage.list("snapshots").unwrap(),
            vec!["1.json".to_string(), "2.json".to_string()]
        );
        assert_eq!(
            storage.list("").unwrap(),
            vec!["catalog.json".to_string()],
            "a directory is not a file name"
        );

        // local_copy materializes a temp file, removed on drop.
        let copy_path = {
            let copy = storage.local_copy("catalog.json", "reading").unwrap();
            let path = copy.path().to_path_buf();
            assert_eq!(std::fs::read(&path).unwrap(), b"cat-2");
            path
        };
        assert!(!copy_path.exists(), "the temp copy is removed on drop");

        // delete is idempotent (S3 DELETE of a missing key is success).
        storage.delete("snapshots/2.json").unwrap();
        assert!(!mock.keys().contains(&"ops/snapshots/2.json".to_string()));
        storage.delete("snapshots/2.json").unwrap();

        // 403 anywhere is the lake's Auth error.
        mock.set_deny(403);
        let err = storage.head("catalog.json").unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        let err = storage
            .read("catalog.json", "loading data file \"x\"")
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(
            err.to_string().contains("AccessDenied"),
            "the GET body carries the S3 code: {err}"
        );
        let err = storage.delete("catalog.json").unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
    }

    /// A fabricated signature (right access key, wrong secret) is rejected by
    /// the mock's HMAC recomputation, not accepted structurally.
    #[test]
    fn fabricated_signature_is_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage_with_secret(&mock, "ops-bucket", "ops", "not-the-secret");
        let err = storage
            .read("catalog.json", "reading catalog.json")
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
            .read("catalog.json", "reading catalog.json")
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("x-amz-content-sha256"), "{err}");

        mock.set_clock_offset_secs(3600);
        let err = storage
            .read("catalog.json", "reading catalog.json")
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("RequestTimeTooSkewed"), "{err}");
        mock.set_clock_offset_secs(0);
        // Back in time: the same request verifies (and the mock records it).
        mock.put_object("ops/catalog.json", b"{}");
        assert_eq!(storage.read("catalog.json", "reading").unwrap(), b"{}");
        assert!(mock.verified_requests() >= 1);
    }

    /// The read and write body cap refuses over-sized objects with a clear
    /// error (a small cap stands in for the 5 GiB single-PUT limit).
    #[test]
    fn object_size_cap_refuses_large_reads_and_writes() {
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("ops/big", &[7u8; 32]);
        let mut storage = storage(&mock, "ops-bucket", "ops");
        storage.client.set_max_object_bytes(16);

        let err = storage
            .read("big", "reading data file \"big\"")
            .unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(err.to_string().contains("16-byte object cap"), "{err}");

        let err = storage.put("small", &[0u8; 17]).unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(err.to_string().contains("capped at 16 bytes"), "{err}");
        assert!(mock.keys().iter().all(|k| k != "ops/small"));
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

        let err = ghost
            .read("catalog.json", "reading catalog.json")
            .unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(err.to_string().contains("bucket does not exist"), "{err}");

        let err = ghost.head("catalog.json").unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert!(err.to_string().contains("bucket does not exist"), "{err}");

        // The existing bucket still reads as a lake-less prefix.
        let real = storage(&mock, "real-bucket", "lake");
        assert_eq!(real.head("catalog.json").unwrap(), Head::Missing);
    }

    /// 400 `InvalidArgument` (not a conditional-support rejection) must not
    /// make degradation sticky; the sticky flag is only for 501 and 400
    /// `InvalidRequest`/`NotImplemented` on conditional requests.
    #[test]
    fn only_conditional_rejection_codes_degrade() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage(&mock, "ops-bucket", "ops");

        mock.set_conditional_rejection(400, "InvalidArgument");
        let err = storage.put_new("k1", b"v").unwrap_err();
        assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
        assert_eq!(mock.conditional_rejections(), 1);
        // Not sticky: the next write probes conditionals again and succeeds.
        mock.clear_conditional_rejection();
        assert!(storage.put_new("k1", b"v").unwrap());
        assert_eq!(mock.conditional_attempts(), 2, "it probed again");
    }

    /// Default refusal: 501/`InvalidRequest` on a conditional write refuses
    /// every later write with the opt-in hint, without touching the object.
    #[test]
    fn conditional_rejection_refuses_writes_by_default() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage(&mock, "ops-bucket", "ops");

        mock.set_conditional_rejection(400, "InvalidRequest");
        let err = storage.put_new("k1", b"v").unwrap_err();
        assert!(
            matches!(err, LakeError::UnsupportedEndpoint { .. }),
            "{err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("does not support conditional writes"), "{msg}");
        assert!(msg.contains("PINKDB_S3_ALLOW_DEGRADED"), "{msg}");
        assert!(msg.contains("SINGLE-WRITER"), "{msg}");
        assert!(mock.keys().is_empty(), "no object was written");

        // Sticky: even an unconditional write refuses without a new probe.
        let attempts = mock.conditional_attempts();
        let err = storage.put("k2", b"v").unwrap_err();
        assert!(
            matches!(err, LakeError::UnsupportedEndpoint { .. }),
            "{err:?}"
        );
        assert_eq!(mock.conditional_attempts(), attempts);
        assert_eq!(mock.conditional_rejections(), 1);
        assert!(mock.keys().is_empty());

        // The refusal is about writes: reads still work.
        mock.put_object("ops/catalog.json", b"cat");
        assert_eq!(storage.read("catalog.json", "reading").unwrap(), b"cat");
    }

    /// Opt-in fallback: check-then-put with the read-back byte compare, and
    /// the sticky degradation stops re-probing.
    #[test]
    fn degraded_opt_in_falls_back_and_verifies_read_back() {
        let mock = crate::mock_s3::MockS3::start();
        mock.reject_conditional_writes(true);
        let storage = storage_allow_degraded(&mock, "ops-bucket", "ops");

        assert!(storage.put_new("k1", b"v").unwrap());
        assert_eq!(mock.conditional_rejections(), 1, "one probe, then sticky");
        assert_eq!(storage.read("k1", "reading").unwrap(), b"v");
        assert!(
            !storage.put_new("k1", b"v2").unwrap(),
            "create-only still holds"
        );
        assert_eq!(mock.conditional_rejections(), 1, "no second probe");

        storage.put("catalog.json", b"cat-1").unwrap();
        let (_, tag) = storage.read_tagged("catalog.json", "reading").unwrap();
        assert!(
            storage
                .put_if_match("catalog.json", b"cat-2", &tag)
                .unwrap()
        );
        assert!(
            !storage
                .put_if_match("catalog.json", b"cat-3", &tag)
                .unwrap()
        );
        assert_eq!(storage.read("catalog.json", "reading").unwrap(), b"cat-2");
    }

    /// A replacement landing between a degraded PUT and its read-back is a
    /// conflict, never a silent success (the reviewer's interleaving at the
    /// storage level).
    #[test]
    fn degraded_read_back_catches_a_replacement() {
        let mock = crate::mock_s3::MockS3::start();
        mock.reject_conditional_writes(true);
        mock.hold_next_request("GET", "snapshots/9.json");
        let result = std::thread::scope(|scope| {
            let mock_ref = &mock;
            let writer = scope.spawn(move || {
                let storage = storage_allow_degraded(mock_ref, "ops-bucket", "ops");
                storage.put_new("snapshots/9.json", b"AAAA")
            });
            assert!(
                mock.wait_until_hold_reached(Duration::from_secs(10)),
                "the read-back did not arrive"
            );
            // Another writer replaces the object while the read-back is held.
            mock.put_object("ops/snapshots/9.json", b"BBBB");
            mock.release_hold();
            writer.join().unwrap()
        });
        let err = result.expect_err("the read-back must conflict");
        assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
        assert!(err.to_string().contains("read-back"), "{err}");
        assert_eq!(
            mock.object("ops/snapshots/9.json").unwrap(),
            b"BBBB",
            "the other writer's bytes stand"
        );
    }
}
