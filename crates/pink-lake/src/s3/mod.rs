//! S3-compatible object storage (`s3` feature): [`S3Storage`] implements the
//! crate-internal [`Storage`] trait over the S3 REST API, and [`S3Config`] is
//! the public configuration surface.
//!
//! # Design
//!
//! - **Blocking, dependency-light**: [`ureq`] over rustls, `hmac` + `sha2`
//!   for AWS SigV4 — no aws-sdk, no async runtime. Every lake operation is
//!   synchronous (local disk reads/writes), so the S3 path keeps that shape.
//! - **Path-style and virtual-hosted** addressing. With a custom endpoint
//!   (Cloudflare R2: `https://<account>.r2.cloudflarestorage.com`, region
//!   `auto`) path-style is the default (`https://<endpoint>/<bucket>/<key>`);
//!   AWS without an endpoint uses virtual-hosted
//!   (`https://<bucket>.s3.<region>.amazonaws.com/<key>`). Both are
//!   configurable via [`S3Config::path_style`].
//! - **Conditional writes**: `put_new` sends `If-None-Match: *` and
//!   `put_if_match` sends `If-Match: <etag>`. AWS S3 and Cloudflare R2 both
//!   evaluate these atomically with the write commit, so concurrent writers
//!   cannot both publish a snapshot and a catalog replace cannot land on a
//!   concurrent writer's catalog. An endpoint that rejects the conditional
//!   header (HTTP 501, or 400 `InvalidRequest`/`NotImplemented` on a
//!   conditional request) makes writes **refuse by default**
//!   ([`LakeError::UnsupportedEndpoint`]): a check-then-put fallback cannot
//!   keep the single-writer contract, so it is opt-in
//!   (`S3Config::allow_degraded(true)` / `PINKDB_S3_ALLOW_DEGRADED=1`).
//!   With the opt-in, the fallback reads the current ETag, compares, PUTs,
//!   **reads back and byte-compares** the written object (a mismatch is a
//!   conflict), and the lake re-checks the catalog; concurrent writers are
//!   then explicitly unsupported (a published snapshot can be transiently
//!   replaced by a racing writer and is restored from the authoritative
//!   catalog before the conflict is reported) — see the README.
//! - **Error mapping**: 404 → not found (the caller decides whether that is
//!   `NotALake`, `VersionNotFound`, or an I/O not-found for a data file); a
//!   404 *body* naming `NoSuchBucket` (and any 404 on a bucket the client
//!   has not yet seen succeed) is a clear "bucket does not exist" error
//!   instead of "not a lake"; 403 → [`LakeError::Auth`]; 412 → a
//!   precondition failure (`Ok(false)`, which the lake reports as a
//!   `Conflict`); everything else is an I/O error naming the operation and
//!   the S3 error code.
//!
//! # Timeouts, retries, addressing and size limits
//!
//! - Every request has one whole-request (connect + transfer) timeout of
//!   [`HTTP_TIMEOUT`] (120 s); large data objects must fit inside it.
//! - There is **no retry on 429/503** (`SlowDown`/`ServiceUnavailable`) or on
//!   a transport error: the operation fails with an I/O error naming the S3
//!   code, and the caller retries (reopening the lake first — a catalog PUT
//!   may have landed; see the README's recovery note). The one retry is the
//!   documented `PutObject` retry for a `409 Conflict` on a conditional
//!   write (a concurrent delete raced the write), bounded by
//!   [`CONFLICT_RETRIES`]. Redirects are not followed: a 301 (wrong
//!   region/endpoint) surfaces as an I/O error naming `PermanentRedirect`.
//! - Plain `http://` endpoints send credentials unencrypted: they are
//!   refused unless explicitly allowed (`S3Config::allow_http(true)` /
//!   `PINKDB_S3_ALLOW_HTTP=1`, for local test endpoints). Region `auto`
//!   without a custom endpoint is refused (it would resolve to
//!   `s3.auto.amazonaws.com`).
//! - Multipart uploads are not implemented: one `PutObject` is used, so one
//!   object must be ≤ 5 GiB (the S3 single-PUT limit). Larger PUTs are
//!   refused client-side with a clear error, and reads are capped at the
//!   same size. No bucket creation, no lifecycle rules, no server-side copy,
//!   no versioning support, no lake-side GC (old versions and snapshots
//!   stay, exactly like local). A dotted bucket name needs path-style
//!   addressing (virtual-hosted TLS certificates do not cover
//!   `dots.in.bucket.<endpoint>`); a custom endpoint defaults to path-style
//!   and `AWS_S3_FORCE_PATH_STYLE` forces it for AWS.

mod sigv4;

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::storage::{Head, LocalCopy, Storage, VersionTag};
use crate::{fresh_id, LakeError, Result};

use sigv4::{Credentials, SigningInput};

/// The SigV4 service name for both AWS S3 and R2.
const S3_SERVICE: &str = "s3";

/// Whole-request timeout (connect + transfer). Lake data files can be large;
/// this is generous enough for object storage over TLS.
const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

/// The largest object body this client reads or writes. One `PutObject` is
/// capped by S3 at 5 GiB (no multipart uploads — see the module docs); the
/// client refuses larger PUTs and larger responses with a clear error.
const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// The most `ListObjectsV2` pages one `list` follows before giving up (a
/// broken endpoint repeating its continuation token must not loop forever).
/// At 1000 keys per page this covers a million objects per directory.
const MAX_LIST_PAGES: usize = 10_000;

/// Retries for a retryable conditional-write race (`409 Conflict` when a
/// concurrent delete raced the write; AWS documents retrying `PutObject`).
const CONFLICT_RETRIES: usize = 3;

/// The message every refused degraded write carries. The task of the client
/// is to be safe by default: a check-then-put fallback cannot keep the
/// single-writer contract, so it needs an explicit opt-in.
const DEGRADED_HINT: &str = "this endpoint does not support conditional writes; concurrent-safe \
    saves are unavailable. Set PINKDB_S3_ALLOW_DEGRADED=1 (or S3Config::allow_degraded(true)) to \
    accept a SINGLE-WRITER guarantee with a small race window";

/// Explicit S3 configuration (the `s3://` URL path resolves the same fields
/// from the environment).
///
/// ```no_run
/// # #[cfg(feature = "s3")] {
/// use pink_lake::S3Config;
///
/// let config = S3Config::new("my-bucket")
///     .with_prefix("lakes/mine")
///     .with_region("auto")
///     .with_endpoint("https://<account>.r2.cloudflarestorage.com")
///     .with_credentials("access-key-id", "secret-access-key");
/// let lake = pink_lake::Lake::open_or_create_s3(config)?;
/// # }
/// # Ok::<(), pink_lake::LakeError>(())
/// ```
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Bucket name (required): 3–63 lowercase ASCII letters, digits, `.` and
    /// `-` (the same rule `Lake::open_url` applies to `s3://` URLs).
    pub bucket: String,
    /// Key prefix inside the bucket (no leading slash; empty = bucket root).
    pub prefix: String,
    /// SigV4 region. R2 uses `auto`; AWS defaults to `us-east-1` (or
    /// `AWS_REGION`/`AWS_DEFAULT_REGION` via [`S3Config::from_env`]). `auto`
    /// requires a custom endpoint.
    pub region: String,
    /// Custom endpoint URL (`https://host[:port]`), or `None` for AWS S3.
    /// A plain `http://` endpoint is refused unless [`S3Config::allow_http`]
    /// is set (test/local endpoints only — credentials travel unencrypted).
    pub endpoint: Option<String>,
    /// `true` path-style, `false` virtual-hosted; `None` picks path-style
    /// when a custom endpoint is set, virtual-hosted otherwise.
    pub path_style: Option<bool>,
    /// Credentials; when not set explicitly, `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` are used (the session
    /// token is only taken from the environment when the other two are too).
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// Explicitly allow an endpoint whose conditional writes are rejected to
    /// fall back to check-then-put. **Unsafe for concurrent writers**: the
    /// fallback cannot make a snapshot publish atomic, so a racing writer can
    /// transiently replace a published snapshot (it is restored from the
    /// catalog) — only opt in for a strictly single-writer lake.
    pub allow_degraded: bool,
    /// Explicitly allow a plain `http://` endpoint (local test endpoints).
    pub allow_http: bool,
}

impl S3Config {
    /// A configuration with no environment lookups (region `us-east-1`,
    /// no endpoint, conditional writes required; credentials still fall back
    /// to the environment at open time unless set explicitly).
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            region: "us-east-1".to_string(),
            endpoint: None,
            path_style: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            allow_degraded: false,
            allow_http: false,
        }
    }

    /// [`S3Config::new`] plus the usual environment variables: `AWS_REGION`
    /// (or `AWS_DEFAULT_REGION`), `AWS_ENDPOINT_URL_S3` (or
    /// `AWS_ENDPOINT_URL`), `AWS_S3_FORCE_PATH_STYLE`, and the explicit
    /// safety opt-outs `PINKDB_S3_ALLOW_DEGRADED` / `PINKDB_S3_ALLOW_HTTP`.
    /// Credentials always fall back to the environment when not set
    /// explicitly.
    pub fn from_env(bucket: impl Into<String>) -> Self {
        let mut config = Self::new(bucket);
        if let Some(region) = env_var("AWS_REGION").or_else(|| env_var("AWS_DEFAULT_REGION")) {
            config.region = region;
        }
        config.endpoint = env_var("AWS_ENDPOINT_URL_S3").or_else(|| env_var("AWS_ENDPOINT_URL"));
        config.path_style = env_truthy("AWS_S3_FORCE_PATH_STYLE");
        config.allow_degraded = env_truthy("PINKDB_S3_ALLOW_DEGRADED").unwrap_or(false);
        config.allow_http = env_truthy("PINKDB_S3_ALLOW_HTTP").unwrap_or(false);
        config
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = Some(path_style);
        self
    }

    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        self.session_token = Some(session_token.into());
        self
    }

    /// Accept a single-writer check-then-put fallback when the endpoint
    /// rejects conditional writes. See [`S3Config::allow_degraded`].
    pub fn allow_degraded(mut self, allow: bool) -> Self {
        self.allow_degraded = allow;
        self
    }

    /// Allow a plain `http://` endpoint (local tests). See
    /// [`S3Config::allow_http`].
    pub fn allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
        self
    }

    /// Resolve everything a client needs, or a clear error: a bad bucket /
    /// prefix is a [`LakeError::BadLocation`], missing credentials are
    /// [`LakeError::Auth`], a bad endpoint is [`LakeError::BadLocation`].
    fn resolve(self) -> Result<ResolvedConfig> {
        self.resolve_with(&|name| env_var(name))
    }

    fn resolve_with(self, env: &dyn Fn(&str) -> Option<String>) -> Result<ResolvedConfig> {
        let location = if self.prefix.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        };
        crate::storage::validate_bucket_name(&self.bucket)
            .map_err(|message| LakeError::BadLocation {
                location: location.clone(),
                message,
            })?;
        let prefix = self.prefix.trim_end_matches('/');
        crate::storage::validate_prefix(prefix, &location)?;
        // Credentials: explicit pair, else both from the environment. The
        // session token may be set explicitly always; the environment's
        // `AWS_SESSION_TOKEN` belongs to the environment's credentials and is
        // ignored with an explicit pair.
        let (access_key_id, secret_access_key, credentials_from_env) = match (
            self.access_key_id.filter(|v| !v.is_empty()),
            self.secret_access_key.filter(|v| !v.is_empty()),
        ) {
            (Some(id), Some(secret)) => (id, secret, false),
            (None, None) => match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
                (Some(id), Some(secret)) if !id.is_empty() && !secret.is_empty() => {
                    (id, secret, true)
                }
                _ => {
                    return Err(LakeError::Auth {
                        path: PathBuf::from(&location),
                        message: "no S3 credentials: set AWS_ACCESS_KEY_ID and \
                                  AWS_SECRET_ACCESS_KEY (plus AWS_SESSION_TOKEN for temporary \
                                  credentials), or configure S3Config::with_credentials"
                            .to_string(),
                    })
                }
            },
            _ => {
                return Err(LakeError::Auth {
                    path: PathBuf::from(&location),
                    message: "incomplete S3 credentials: set both the access key id and the \
                              secret access key"
                        .to_string(),
                })
            }
        };
        let session_token = self
            .session_token
            .filter(|t| !t.is_empty())
            .or_else(|| {
                if credentials_from_env {
                    env("AWS_SESSION_TOKEN")
                } else {
                    None
                }
            });
        if self.endpoint.is_none() && self.region == "auto" {
            return Err(LakeError::BadLocation {
                location: self.region.clone(),
                message: "region `auto` needs a custom endpoint (R2 uses `auto` with \
                          AWS_ENDPOINT_URL); without one, set a real AWS region"
                    .to_string(),
            });
        }
        let path_style = self.path_style.unwrap_or(self.endpoint.is_some());
        let endpoint = match &self.endpoint {
            Some(url) => {
                let endpoint =
                    Endpoint::parse(url, path_style).map_err(|message| LakeError::BadLocation {
                        location: url.clone(),
                        message: format!("invalid S3 endpoint: {message}"),
                    })?;
                if endpoint.scheme == "http" && !self.allow_http {
                    return Err(LakeError::BadLocation {
                        location: url.clone(),
                        message: "a plain http:// endpoint sends credentials unencrypted; use \
                                  https:// (or set S3Config::allow_http(true) / \
                                  PINKDB_S3_ALLOW_HTTP=1 for a local test endpoint)"
                            .to_string(),
                    });
                }
                endpoint
            }
            None => Endpoint::aws_default(&self.region, path_style),
        };
        Ok(ResolvedConfig {
            bucket: self.bucket,
            prefix: prefix.to_string(),
            region: self.region,
            endpoint,
            credentials: Credentials {
                access_key_id,
                secret_access_key,
                session_token,
            },
            allow_degraded: self.allow_degraded,
        })
    }
}

#[derive(Debug)]
struct ResolvedConfig {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Endpoint,
    credentials: Credentials,
    allow_degraded: bool,
}

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
        let resolved = config.resolve()?;
        let root = PathBuf::from(if resolved.prefix.is_empty() {
            format!("s3://{}", resolved.bucket)
        } else {
            format!("s3://{}/{}", resolved.bucket, resolved.prefix)
        });
        Ok(Self::from_resolved(resolved, root))
    }

    /// Storage for an `s3://bucket/prefix` URL: the same fields as
    /// [`S3Config::from_env`], credentials from the environment.
    pub(crate) fn from_url(bucket: &str, prefix: &str) -> Result<Self> {
        Self::new(S3Config::from_env(bucket).with_prefix(prefix))
    }

    fn from_resolved(resolved: ResolvedConfig, root: PathBuf) -> Self {
        let allow_degraded = resolved.allow_degraded;
        Self {
            client: S3Client {
                bucket: resolved.bucket,
                region: resolved.region,
                endpoint: resolved.endpoint,
                credentials: resolved.credentials,
                agent: ureq::Agent::config_builder()
                    .timeout_global(Some(HTTP_TIMEOUT))
                    // 4xx/5xx are data here: 404/412/403 have meanings.
                    .http_status_as_error(false)
                    // A 301 (wrong region/endpoint) must surface as an S3
                    // error, not be followed blindly.
                    .max_redirects(0)
                    .user_agent(concat!("pink-lake/", env!("CARGO_PKG_VERSION")))
                    .build()
                    .into(),
                max_object_bytes: MAX_OBJECT_BYTES,
            },
            prefix: resolved.prefix,
            root,
            degraded: AtomicBool::new(false),
            allow_degraded,
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
            Ok(Get::Found {
                bytes: current, ..
            }) if current == bytes => Ok(()),
            Ok(Get::Found { .. }) => Err(self.degraded_conflict(
                rel,
                "the object changed again between this write and its read-back",
            )),
            Ok(Get::NotFound) => Err(self.degraded_conflict(
                rel,
                "the object disappeared right after this write",
            )),
            Err(e) => Err(self.map(
                &format!("GET s3://{}/{key}", self.client.bucket),
                e,
            )),
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
    fn map(&self, context: &str, error: S3Error) -> LakeError {
        match error {
            S3Error::Transport {
                context: operation,
                source,
            } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::new(source.kind(), format!("{operation}: {source}")),
            },
            S3Error::Protocol {
                context: operation,
                message,
            } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!("{operation}: {message}")),
            },
            S3Error::Auth {
                context: operation,
                code,
                message,
            } => LakeError::Auth {
                path: self.root.clone(),
                message: format!("{operation}: S3 denied the request ({code}): {message}"),
            },
            S3Error::Status {
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
            Err(S3Error::Auth { .. }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(self.map(context, e)),
        }
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
            }) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok((bytes, VersionTag(etag)))
            }
            Ok(Get::Found { etag: None, .. }) => Err(self.map(
                context,
                S3Error::Protocol {
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
        match self.client.put_object(&self.key(rel), bytes, Condition::None) {
            Ok(Put::Written) => {
                self.bucket_confirmed.store(true, Ordering::Relaxed);
                Ok(())
            }
            Ok(_) => Err(self.map(
                &context,
                S3Error::Protocol {
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
            match self
                .client
                .put_object(&key, bytes, Condition::IfNoneMatch)
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
                    S3Error::Protocol {
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
                        S3Error::Protocol {
                            context: context.clone(),
                            message: "unconditional PUT was not written".into(),
                        },
                    )),
                    Err(e) => Err(self.map(&context, e)),
                }
            }
            Ok(Get::Found { etag: None, .. }) => Err(self.map(
                &context,
                S3Error::Protocol {
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
            Ok(Get::Found {
                bytes: current, ..
            }) if current == bytes => Ok(()),
            Ok(Get::Found { .. }) | Ok(Get::NotFound) => {
                let put_context = format!("PUT {}", self.abs(&rel).display());
                match self.client.put_object(&key, bytes, Condition::None) {
                    Ok(Put::Written) => Ok(()),
                    Ok(_) => Err(self.map(
                        &put_context,
                        S3Error::Protocol {
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

    fn delete(&self, rel: &str) -> Result<()> {
        let context = format!("DELETE {}", self.abs(rel).display());
        self.client
            .delete_object(&self.key(rel))
            .map_err(|e| self.map(&context, e))
    }

    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy> {
        let bytes = self.read(rel, context)?;
        let path = std::env::temp_dir().join(format!("pink-lake-s3-{}.pink", fresh_id()));
        // `create_new` so a planted symlink or an old temp file at the same
        // name is never followed or truncated; a failure removes the
        // partial file instead of leaving it behind.
        let written = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .and_then(|mut file| io::Write::write_all(&mut file, &bytes));
        if let Err(e) = written {
            let _ = std::fs::remove_file(&path);
            return Err(LakeError::Io {
                context: format!("writing the temp copy {}", path.display()),
                source: e,
            });
        }
        Ok(LocalCopy::Temp(path))
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// An environment flag: `1`, `true`, `yes`, `on` (case-insensitive).
fn env_truthy(name: &str) -> Option<bool> {
    env_var(name).map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

// ---------------------------------------------------------------------------
// The minimal blocking S3 REST client.

/// A parsed S3 endpoint: scheme, host, port, base path, addressing style.
#[derive(Debug, Clone)]
struct Endpoint {
    scheme: String,
    host: String,
    port: Option<u16>,
    /// `""` or `/base` (kept literal and encoded into the request URI).
    base_path: String,
    path_style: bool,
}

impl Endpoint {
    /// AWS's default endpoint for a region (virtual-hosted unless overridden).
    fn aws_default(region: &str, path_style: bool) -> Self {
        Self {
            scheme: "https".to_string(),
            host: format!("s3.{region}.amazonaws.com"),
            port: None,
            base_path: String::new(),
            path_style,
        }
    }

    /// Parse `scheme://host[:port][/base-path]`.
    fn parse(url: &str, path_style: bool) -> std::result::Result<Self, String> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| format!("{url:?} has no scheme (expected https://host)"))?;
        if scheme != "http" && scheme != "https" {
            return Err(format!("{url:?} must be http:// or https://"));
        }
        if rest.is_empty() || rest.contains([' ', '?', '#', '\t']) {
            return Err(format!("{url:?} is not a plain host[:port][/path] URL"));
        }
        let (authority, base_path) = match rest.split_once('/') {
            Some((authority, path)) => {
                let path = path.trim_end_matches('/');
                if path.is_empty() {
                    (authority, String::new())
                } else {
                    (authority, format!("/{path}"))
                }
            }
            None => (rest, String::new()),
        };
        let (host, port) = split_authority(authority)?;
        if host.is_empty() {
            return Err(format!("{url:?} has no host"));
        }
        if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
            return Err(format!(
                "{url:?} is not host[:port] (bracket IPv6 addresses as [host])"
            ));
        }
        Ok(Self {
            scheme: scheme.to_string(),
            host,
            port,
            base_path,
            path_style,
        })
    }

    /// The `host[:port]` header value of the endpoint itself.
    fn host_header(&self) -> String {
        match self.port {
            Some(port) => format!("{}:{port}", self.host),
            None => self.host.clone(),
        }
    }

    /// The request target (URL, canonical URI, `host` header) of one object
    /// key, encoded exactly as the signature canonicalizes it.
    fn target(&self, bucket: &str, key: &str, query: &str) -> Target {
        let encoded_key = uri_encode(key, false);
        let key_part = if encoded_key.is_empty() {
            String::new()
        } else {
            format!("/{encoded_key}")
        };
        let (host_header, mut canonical_uri) = if self.path_style {
            (
                self.host_header(),
                format!("{}/{bucket}{key_part}", self.base_path),
            )
        } else {
            let host = format!("{bucket}.{}", self.host);
            let host_header = match self.port {
                Some(port) => format!("{host}:{port}"),
                None => host,
            };
            (host_header, format!("{}{key_part}", self.base_path))
        };
        if canonical_uri.is_empty() {
            canonical_uri = "/".to_string();
        }
        let url = if query.is_empty() {
            format!("{}://{host_header}{canonical_uri}", self.scheme)
        } else {
            format!("{}://{host_header}{canonical_uri}?{query}", self.scheme)
        };
        Target {
            url,
            canonical_uri,
            host: host_header,
        }
    }
}

fn split_authority(authority: &str) -> std::result::Result<(String, Option<u16>), String> {
    if let Some(rest) = authority.strip_prefix('[') {
        // Bracketed IPv6: [::1] or [::1]:9000.
        let (host, tail) = rest
            .split_once(']')
            .ok_or_else(|| format!("{authority:?} has an unclosed IPv6 bracket"))?;
        let port = match tail {
            "" => None,
            tail => Some(
                tail.strip_prefix(':')
                    .ok_or_else(|| format!("{authority:?} has junk after the IPv6 host"))?
                    .parse()
                    .map_err(|_| format!("{authority:?} has an invalid port"))?,
            ),
        };
        return Ok((format!("[{host}]"), port));
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => Ok((
            host.to_string(),
            Some(
                port.parse()
                    .map_err(|_| format!("{authority:?} has an invalid port"))?,
            ),
        )),
        // A colon that is not a port: a bare IPv6 address needs brackets.
        Some(_) => Err(format!(
            "{authority:?} is not host[:port] (bracket IPv6 addresses as [host])"
        )),
        None => Ok((authority.to_string(), None)),
    }
}

/// One signed request's target.
#[derive(Debug)]
struct Target {
    url: String,
    canonical_uri: String,
    host: String,
}

/// RFC 3986 `uri-encode` with S3's rules: unreserved bytes
/// (`A-Z a-z 0-9 - _ . ~`) pass through; `/` passes through in paths but is
/// always encoded in query values; everything else (including `%`, so an
/// already-percent-encoded object key stays literal) becomes uppercase `%XX`.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        let unreserved = b.is_ascii_alphanumeric()
            || b == b'-'
            || b == b'_'
            || b == b'.'
            || b == b'~';
        if unreserved || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
    }
    out
}

/// Build a canonical query string: URI-encoded keys/values, sorted by key.
fn canonical_query(pairs: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// A conditional write precondition.
#[derive(Debug, Clone, Copy)]
enum Condition<'a> {
    None,
    /// `If-None-Match: *` — create only.
    IfNoneMatch,
    /// `If-Match: <etag>` — replace only an unchanged object.
    IfMatch(&'a str),
}

impl Condition<'_> {
    fn is_conditional(&self) -> bool {
        !matches!(self, Condition::None)
    }

    fn headers(&self) -> Vec<(&'static str, String)> {
        match self {
            Condition::None => Vec::new(),
            Condition::IfNoneMatch => vec![("if-none-match", "*".to_string())],
            Condition::IfMatch(etag) => vec![("if-match", (*etag).to_string())],
        }
    }
}

/// The result of a conditional `PutObject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Put {
    Written,
    /// HTTP 412 (or 404 for `If-Match` on a missing object).
    PreconditionFailed,
    /// The endpoint rejected the conditional header itself.
    ConditionalUnsupported,
}

#[derive(Debug)]
enum Get {
    Found {
        bytes: Vec<u8>,
        etag: Option<String>,
    },
    NotFound,
}

#[derive(Debug)]
struct ListPage {
    keys: Vec<String>,
    next_token: Option<String>,
}

/// Client failures before they are mapped to [`LakeError`].
#[derive(Debug)]
enum S3Error {
    Transport {
        context: String,
        source: io::Error,
    },
    Status {
        context: String,
        status: u16,
        code: String,
        message: String,
    },
    Auth {
        context: String,
        code: String,
        message: String,
    },
    Protocol {
        context: String,
        message: String,
    },
}

/// One S3 client (bucket + region + endpoint + credentials + connection pool).
#[derive(Debug)]
struct S3Client {
    bucket: String,
    region: String,
    endpoint: Endpoint,
    credentials: Credentials,
    agent: ureq::Agent,
    /// The read/write body cap (tests lower it to exercise the overflow path
    /// without transferring gigabytes).
    max_object_bytes: u64,
}

impl S3Client {
    /// One signed request. `extra` headers are sent and signed (conditional
    /// preconditions must not be tamperable).
    fn execute(
        &self,
        method: &str,
        key: &str,
        query_pairs: &[(&str, &str)],
        extra: &[(&'static str, String)],
        body: &[u8],
    ) -> std::result::Result<(u16, Option<String>, Vec<u8>), S3Error> {
        let query = canonical_query(query_pairs);
        let target = self.endpoint.target(&self.bucket, key, &query);
        let context = format!("{method} {}", target.url);

        let payload_hash = sigv4::payload_hash(body);
        let (amz_date, date_stamp) = sigv4::now_amz_date();

        let mut headers: Vec<(&str, String)> = Vec::new();
        headers.push(("host", target.host.clone()));
        headers.push(("x-amz-content-sha256", payload_hash.clone()));
        headers.push(("x-amz-date", amz_date.clone()));
        if let Some(token) = &self.credentials.session_token {
            headers.push(("x-amz-security-token", token.clone()));
        }
        for (name, value) in extra {
            headers.push((name, value.clone()));
        }
        let signed = sigv4::sign(
            &self.credentials,
            &SigningInput {
                method,
                canonical_uri: &target.canonical_uri,
                canonical_query: &query,
                headers: headers
                    .iter()
                    .map(|(name, value)| (*name, value.as_str()))
                    .collect(),
                payload_hash: &payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
                region: &self.region,
                service: S3_SERVICE,
            },
        );

        let mut request = ureq::http::Request::builder()
            .method(method)
            .uri(&target.url);
        for (name, value) in &headers {
            request = request.header(*name, value);
        }
        request = request.header("authorization", &signed.authorization);
        // Bodyless methods carry no body (and no `Content-Length: 0`).
        let build_error = |e: ureq::http::Error| S3Error::Protocol {
            context: context.clone(),
            message: format!("could not build the request: {e}"),
        };
        let mut response = if matches!(method, "GET" | "HEAD" | "DELETE") {
            self.agent.run(request.body(()).map_err(build_error)?)
        } else {
            self.agent.run(request.body(body).map_err(build_error)?)
        }
        .map_err(|e| match e {
            ureq::Error::Io(io) => S3Error::Transport {
                context: context.clone(),
                source: io,
            },
            other => S3Error::Transport {
                context: context.clone(),
                source: io::Error::other(other.to_string()),
            },
        })?;
        let status = response.status().as_u16();
        // A declared body over the cap is refused before any bytes are read
        // (the read below is limited too, for a lying Content-Length). HEAD
        // only reports the object's length, so it is exempt.
        if method != "HEAD" {
            if let Some(length) = response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
            {
                if length > self.max_object_bytes {
                    return Err(S3Error::Protocol {
                        context: context.clone(),
                        message: format!(
                            "the response is {length} bytes, over this client's {}-byte object cap \
                             (no multipart downloads)",
                            self.max_object_bytes
                        ),
                    });
                }
            }
        }
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let bytes = if method == "HEAD" {
            Vec::new()
        } else {
            response
                .body_mut()
                .with_config()
                .limit(self.max_object_bytes)
                .read_to_vec()
                .map_err(|e| S3Error::Transport {
                    context: context.clone(),
                    source: io::Error::other(e.to_string()),
                })?
        };
        Ok((status, etag, bytes))
    }

    /// The `s3://bucket/key` form, for error contexts.
    fn abs(&self, key: &str) -> String {
        if key.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{key}", self.bucket)
        }
    }

    /// Turn a non-2xx response into the right error, parsing the S3 error
    /// code/message from the XML body when there is one.
    fn status_error(&self, context: &str, status: u16, body: &[u8]) -> S3Error {
        let text = String::from_utf8_lossy(body);
        let code = xml_field(&text, "Code").unwrap_or_else(|| {
            // A HEAD error response has no body, and 403s are the access
            // family: report the canonical code instead of "UnknownError".
            if status == 403 {
                "AccessDenied".to_string()
            } else {
                "UnknownError".to_string()
            }
        });
        let message = xml_field(&text, "Message").unwrap_or_default();
        if status == 403 {
            return S3Error::Auth {
                context: context.to_string(),
                code,
                message,
            };
        }
        S3Error::Status {
            context: context.to_string(),
            status,
            code,
            message,
        }
    }

    fn get_object(&self, key: &str) -> std::result::Result<Get, S3Error> {
        let (status, etag, body) = self.execute("GET", key, &[], &[], b"")?;
        match status {
            200 => Ok(Get::Found { bytes: body, etag }),
            404 => Ok(Get::NotFound),
            other => Err(self.status_error(&format!("GET {}", self.abs(key)), other, &body)),
        }
    }

    fn head_object(&self, key: &str) -> std::result::Result<bool, S3Error> {
        let (status, _etag, body) = self.execute("HEAD", key, &[], &[], b"")?;
        match status {
            200 => Ok(true),
            404 => Ok(false),
            other => Err(self.status_error(&format!("HEAD {}", self.abs(key)), other, &body)),
        }
    }

    fn put_object(
        &self,
        key: &str,
        bytes: &[u8],
        condition: Condition<'_>,
    ) -> std::result::Result<Put, S3Error> {
        if bytes.len() as u64 > self.max_object_bytes {
            return Err(S3Error::Protocol {
                context: format!("PUT {}", self.abs(key)),
                message: format!(
                    "refusing to PUT {} bytes: one object is capped at {} bytes (a single \
                     PutObject is limited to 5 GiB by S3 and this client has no multipart \
                     uploads)",
                    bytes.len(),
                    self.max_object_bytes
                ),
            });
        }
        let extra = condition.headers();
        let context = format!("PUT {}", self.abs(key));
        let mut attempts = 0;
        loop {
            attempts += 1;
            let (status, _etag, body) = self.execute("PUT", key, &[], &extra, bytes)?;
            match status {
                200 => return Ok(Put::Written),
                // `If-Match` on a vanished object is a precondition failure.
                404 if matches!(condition, Condition::IfMatch(_)) => {
                    return Ok(Put::PreconditionFailed)
                }
                412 => return Ok(Put::PreconditionFailed),
                409 if condition.is_conditional() && attempts < CONFLICT_RETRIES => {
                    // A concurrent delete raced the write; AWS documents
                    // retrying PutObject.
                    std::thread::sleep(Duration::from_millis(50 * attempts as u64));
                    continue;
                }
                other => {
                    let error = self.status_error(&context, other, &body);
                    if condition.is_conditional() && is_unsupported_conditional(other, &error) {
                        return Ok(Put::ConditionalUnsupported);
                    }
                    return Err(error);
                }
            }
        }
    }

    fn delete_object(&self, key: &str) -> std::result::Result<(), S3Error> {
        let context = format!("DELETE {}", self.abs(key));
        let (status, _etag, body) = self.execute("DELETE", key, &[], &[], b"")?;
        match status {
            200 | 204 => Ok(()),
            // S3 DELETE is idempotent: a missing object is success.
            404 => Ok(()),
            other => Err(self.status_error(&context, other, &body)),
        }
    }

    fn list_objects(
        &self,
        prefix: &str,
        delimiter: &str,
        continuation: Option<&str>,
    ) -> std::result::Result<ListPage, S3Error> {
        let mut pairs: Vec<(&str, &str)> = vec![
            ("delimiter", delimiter),
            ("list-type", "2"),
            ("prefix", prefix),
        ];
        if let Some(token) = continuation {
            pairs.push(("continuation-token", token));
        }
        let context = format!("LIST s3://{}/{}", self.bucket, prefix);
        let (status, _etag, body) = self.execute("GET", "", &pairs, &[], b"")?;
        if status != 200 {
            return Err(self.status_error(&context, status, &body));
        }
        let text = String::from_utf8_lossy(&body).into_owned();
        let mut keys = Vec::new();
        for contents in xml_blocks(&text, "Contents") {
            if let Some(key) = xml_field(&contents, "Key") {
                keys.push(key);
            }
        }
        let truncated = xml_field(&text, "IsTruncated")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let next_token = if truncated {
            xml_field(&text, "NextContinuationToken")
        } else {
            None
        };
        Ok(ListPage { keys, next_token })
    }
}

/// Whether a failed *conditional* request looks like "this endpoint does not
/// support conditional writes" rather than a real error. The caller only
/// calls this for requests that actually carried a conditional header, and
/// only HTTP 501 or a 400 `NotImplemented`/`InvalidRequest` counts — an
/// unrelated 4xx (e.g. 400 `InvalidArgument`, 403) must never make
/// degradation sticky.
fn is_unsupported_conditional(status: u16, error: &S3Error) -> bool {
    if status == 501 {
        return true;
    }
    let code = match error {
        S3Error::Status { code, .. } | S3Error::Auth { code, .. } => code.as_str(),
        _ => return false,
    };
    status == 400 && matches!(code, "NotImplemented" | "InvalidRequest")
}

// ---------------------------------------------------------------------------
// Minimal XML helpers (ListObjectsV2 / error responses; std only).

/// The text of the first `<tag>…</tag>` element, entity-decoded.
fn xml_field(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(xml_unescape(&body[start..end]))
}

/// All `<tag>…</tag>` block bodies in order.
fn xml_blocks(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else {
            break;
        };
        out.push(after[..end].to_string());
        rest = &after[end + close.len()..];
    }
    out
}

/// Decode the five XML entities S3 keys/messages can carry.
fn xml_unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        let decoded = [
            ("&amp;", '&'),
            ("&lt;", '<'),
            ("&gt;", '>'),
            ("&quot;", '"'),
            ("&apos;", '\''),
        ]
        .iter()
        .find(|(entity, _)| tail.starts_with(entity));
        match decoded {
            Some((entity, ch)) => {
                out.push(*ch);
                rest = &tail[entity.len()..];
            }
            None => {
                out.push('&');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An `S3Storage` pointed at the mock, with explicit credentials. The
    /// mock's endpoint is plain `http://` (a local test server), hence the
    /// explicit `allow_http`.
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
        assert_eq!(storage.read("snapshots/1.json", "reading one").unwrap(), b"one");
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
        assert!(storage
            .put_if_match("catalog.json", b"cat-2", &tag)
            .unwrap());
        assert_eq!(
            storage.read("catalog.json", "reading catalog.json").unwrap(),
            b"cat-2"
        );
        assert!(!storage
            .put_if_match("catalog.json", b"cat-3", &tag)
            .unwrap());
        assert_eq!(
            storage.read("catalog.json", "reading catalog.json").unwrap(),
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

    #[test]
    fn uri_encoding_follows_the_s3_rules() {
        assert_eq!(uri_encode("plain/key-1_x.y~z", false), "plain/key-1_x.y~z");
        assert_eq!(uri_encode("a b", false), "a%20b");
        assert_eq!(uri_encode("a/b", false), "a/b");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("100%", false), "100%25");
        assert_eq!(uri_encode("main.a%2Fb", false), "main.a%252Fb");
        assert_eq!(uri_encode("héllo🌍", false), "h%C3%A9llo%F0%9F%8C%8D");
        assert_eq!(uri_encode("", false), "");
    }

    #[test]
    fn canonical_queries_sort_and_encode() {
        assert_eq!(
            canonical_query(&[("prefix", "a/b"), ("list-type", "2")]),
            "list-type=2&prefix=a%2Fb"
        );
        assert_eq!(
            canonical_query(&[("continuation-token", "1.json"), ("delimiter", "/")]),
            "continuation-token=1.json&delimiter=%2F"
        );
        assert_eq!(canonical_query(&[]), "");
    }

    #[test]
    fn endpoints_build_both_addressing_styles() {
        let path_style = Endpoint::parse("https://acc.r2.cloudflarestorage.com", true).unwrap();
        let target = path_style.target("mybucket", "data/main.t/x.vortex", "");
        assert_eq!(
            target.url,
            "https://acc.r2.cloudflarestorage.com/mybucket/data/main.t/x.vortex"
        );
        assert_eq!(target.canonical_uri, "/mybucket/data/main.t/x.vortex");
        assert_eq!(target.host, "acc.r2.cloudflarestorage.com");

        let virtual_hosted = Endpoint::parse("https://s3.us-east-1.amazonaws.com", false).unwrap();
        let target = virtual_hosted.target("mybucket", "lakes/mine/catalog.json", "");
        assert_eq!(
            target.url,
            "https://mybucket.s3.us-east-1.amazonaws.com/lakes/mine/catalog.json"
        );
        assert_eq!(target.canonical_uri, "/lakes/mine/catalog.json");
        assert_eq!(target.host, "mybucket.s3.us-east-1.amazonaws.com");

        // A port and a base path survive both styles.
        let with_port = Endpoint::parse("http://127.0.0.1:9000/base/path/", true).unwrap();
        let target = with_port.target("b", "k", "list-type=2");
        assert_eq!(target.url, "http://127.0.0.1:9000/base/path/b/k?list-type=2");
        assert_eq!(target.canonical_uri, "/base/path/b/k");
        assert_eq!(target.host, "127.0.0.1:9000");

        // An empty key (bucket-level ListObjectsV2) has no trailing slash.
        let target = path_style.target("mybucket", "", "list-type=2");
        assert_eq!(
            target.url,
            "https://acc.r2.cloudflarestorage.com/mybucket?list-type=2"
        );
        assert_eq!(target.canonical_uri, "/mybucket");

        // IPv6 endpoints.
        let ipv6 = Endpoint::parse("http://[::1]:9000", true).unwrap();
        let target = ipv6.target("b", "k", "");
        assert_eq!(target.url, "http://[::1]:9000/b/k");
        assert_eq!(target.host, "[::1]:9000");

        // The AWS default: virtual-hosted for the region.
        let aws = Endpoint::aws_default("eu-west-1", false);
        assert_eq!(
            aws.target("b", "k", "").url,
            "https://b.s3.eu-west-1.amazonaws.com/k"
        );
        let aws_path = Endpoint::aws_default("us-east-1", true);
        assert_eq!(
            aws_path.target("b", "k", "").url,
            "https://s3.us-east-1.amazonaws.com/b/k"
        );
    }

    #[test]
    fn bad_endpoints_are_rejected() {
        for bad in [
            "ftp://host",
            "host-without-scheme",
            "https://",
            "https://host?query=1",
            "https://[::1:9000",
            "https://host:notaport",
            "http://host:12x",
            "https://::1",
        ] {
            assert!(Endpoint::parse(bad, true).is_err(), "{bad}");
        }
    }

    #[test]
    fn config_resolution_maps_missing_credentials_to_auth() {
        let config = S3Config::new("bucket").with_prefix("a/b");
        let err = config.resolve_with(&|_| None).unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("no S3 credentials"), "{err}");

        let err = S3Config::new("bucket")
            .with_credentials("only-id", "")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");

        // Environment credentials fill the gap.
        let env = |name: &str| match name {
            "AWS_ACCESS_KEY_ID" => Some("env-id".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("env-secret".to_string()),
            "AWS_SESSION_TOKEN" => Some("env-token".to_string()),
            _ => None,
        };
        let resolved = S3Config::new("bucket").resolve_with(&env).unwrap();
        assert_eq!(resolved.credentials.access_key_id, "env-id");
        assert_eq!(
            resolved.credentials.session_token.as_deref(),
            Some("env-token")
        );
        // Explicit credentials do NOT pick up the environment's session
        // token: it belongs to the environment's credential pair.
        let resolved = S3Config::new("bucket")
            .with_credentials("id", "secret")
            .resolve_with(&env)
            .unwrap();
        assert_eq!(resolved.credentials.access_key_id, "id");
        assert_eq!(resolved.credentials.session_token, None);
        // An explicit session token is kept, with explicit or env creds.
        let resolved = S3Config::new("bucket")
            .with_credentials("id", "secret")
            .with_session_token("explicit-token")
            .resolve_with(&env)
            .unwrap();
        assert_eq!(
            resolved.credentials.session_token.as_deref(),
            Some("explicit-token")
        );
        // A custom endpoint defaults to path-style, AWS to virtual-hosted.
        let resolved = S3Config::new("bucket")
            .with_endpoint("https://acc.r2.cloudflarestorage.com")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap();
        assert!(resolved.endpoint.path_style);
        assert_eq!(resolved.endpoint.host, "acc.r2.cloudflarestorage.com");
        let resolved = S3Config::new("bucket")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap();
        assert!(!resolved.endpoint.path_style);
        assert_eq!(resolved.endpoint.host, "s3.us-east-1.amazonaws.com");
        // The safety opt-ins default to off and flow through resolution.
        assert!(!resolved.allow_degraded);
        let resolved = S3Config::new("bucket")
            .with_credentials("id", "secret")
            .allow_degraded(true)
            .resolve_with(&|_| None)
            .unwrap();
        assert!(resolved.allow_degraded);
    }

    #[test]
    fn config_refuses_plain_http_and_bare_auto_without_opt_in() {
        // Plain http sends credentials unencrypted: refused by default.
        let err = S3Config::new("bucket")
            .with_endpoint("http://127.0.0.1:9000")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        let message = err.to_string();
        assert!(message.contains("unencrypted"), "{message}");
        assert!(message.contains("allow_http"), "{message}");
        // The explicit opt-in (or PINKDB_S3_ALLOW_HTTP via from_env) allows it.
        let resolved = S3Config::new("bucket")
            .with_endpoint("http://127.0.0.1:9000")
            .with_credentials("id", "secret")
            .allow_http(true)
            .resolve_with(&|_| None)
            .unwrap();
        assert_eq!(resolved.endpoint.scheme, "http");

        // Region `auto` is R2-only: without a custom endpoint it would build
        // `s3.auto.amazonaws.com`.
        let err = S3Config::new("bucket")
            .with_region("auto")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("auto"), "{err}");
        // With an endpoint it is the documented R2 shape.
        let resolved = S3Config::new("bucket")
            .with_region("auto")
            .with_endpoint("https://acc.r2.cloudflarestorage.com")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap();
        assert_eq!(resolved.region, "auto");
    }

    #[test]
    fn config_rejects_bad_buckets_and_prefixes() {
        let long = "b".repeat(64);
        for bad in [
            "",
            "bad bucket",
            "bad/bucket",
            "bad_bucket",
            // Uppercase and the length bounds match `Location::parse`.
            "UPPER",
            "ab",
            long.as_str(),
        ] {
            let err = S3Config::new(bad)
                .with_credentials("id", "secret")
                .resolve_with(&|_| None)
                .unwrap_err();
            assert!(
                matches!(err, LakeError::BadLocation { .. }),
                "{bad}: {err:?}"
            );
        }
        // Both entry points reject the same shapes with the same wording.
        for bad in ["UPPER/x", "ab/x", "bad_bucket/x"] {
            let url = format!("s3://{bad}");
            let from_url = crate::storage::Location::parse(&url).unwrap_err();
            let from_config = S3Config::new(bad.split('/').next().unwrap())
                .with_credentials("id", "secret")
                .resolve_with(&|_| None)
                .unwrap_err();
            assert!(
                from_url.to_string().contains("the bucket name must be 3"),
                "{url}: {from_url}"
            );
            assert!(
                from_config.to_string().contains("the bucket name must be 3"),
                "{url}: {from_config}"
            );
        }
        let err = S3Config::new("bucket")
            .with_prefix("/lead")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        // Trailing slashes in a prefix are normalized away.
        let resolved = S3Config::new("bucket")
            .with_prefix("a/b//")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap();
        assert_eq!(resolved.prefix, "a/b");
        // A bad endpoint names the URL.
        let err = S3Config::new("bucket")
            .with_endpoint("ftp://host")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("ftp://host"), "{err}");
    }

    #[test]
    fn xml_helpers_parse_listings_and_errors() {
        let listing = "<?xml version=\"1.0\"?><ListBucketResult>\
            <IsTruncated>false</IsTruncated>\
            <Contents><Key>snapshots/1.json</Key><ETag>&quot;1&quot;</ETag><Size>5</Size></Contents>\
            <Contents><Key>snapshots/2&amp;3.json</Key></Contents>\
            </ListBucketResult>";
        let keys: Vec<String> = xml_blocks(listing, "Contents")
            .iter()
            .filter_map(|block| xml_field(block, "Key"))
            .collect();
        assert_eq!(keys, vec!["snapshots/1.json", "snapshots/2&3.json"]);
        assert_eq!(xml_field(listing, "IsTruncated").as_deref(), Some("false"));

        let error = "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
        assert_eq!(xml_field(error, "Code").as_deref(), Some("NoSuchKey"));
        assert!(xml_field(error, "Message")
            .unwrap()
            .contains("does not exist"));
        assert_eq!(xml_unescape("a&b<c&d"), "a&b<c&d");
        assert_eq!(xml_unescape("&amp;&lt;&gt;&quot;&apos;"), "&<>\"'");
        assert_eq!(xml_unescape("trailing &"), "trailing &");
    }

    #[test]
    fn conditional_headers_are_exact() {
        assert!(Condition::None.headers().is_empty());
        assert_eq!(
            Condition::IfNoneMatch.headers(),
            vec![("if-none-match", "*".to_string())]
        );
        assert_eq!(
            Condition::IfMatch("\"abc\"").headers(),
            vec![("if-match", "\"abc\"".to_string())]
        );
    }

    #[test]
    fn unsupported_conditional_detection() {
        let status = |status, code: &str| S3Error::Status {
            context: String::new(),
            status,
            code: code.to_string(),
            message: String::new(),
        };
        assert!(is_unsupported_conditional(501, &status(501, "NotImplemented")));
        assert!(is_unsupported_conditional(400, &status(400, "InvalidRequest")));
        assert!(!is_unsupported_conditional(400, &status(400, "InvalidArgument")));
        assert!(!is_unsupported_conditional(500, &status(500, "InternalError")));
        // 403 is auth, never "unsupported".
        assert!(!is_unsupported_conditional(
            403,
            &S3Error::Auth {
                context: String::new(),
                code: "AccessDenied".into(),
                message: String::new(),
            }
        ));
    }

    /// An `S3Storage` with the degraded fallback explicitly accepted.
    fn storage_allow_degraded(mock: &crate::mock_s3::MockS3, bucket: &str, prefix: &str) -> S3Storage {
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

    /// A fabricated signature (right access key, wrong secret) is rejected by
    /// the mock's HMAC recomputation, not accepted structurally.
    #[test]
    fn fabricated_signature_is_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage_with_secret(&mock, "ops-bucket", "ops", "not-the-secret");
        let err = storage.read("catalog.json", "reading catalog.json").unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("SignatureDoesNotMatch"), "{err}");
        assert!(mock.keys().is_empty(), "nothing was written");
        assert_eq!(mock.verified_requests(), 0, "no request verified");

        // A structurally perfect header with a random signature is rejected too.
        let response = mock.raw_request(
            "GET /ops-bucket/ops/catalog.json HTTP/1.1\r\nHost: 127.0.0.1\r\n\
             Authorization: AWS4-HMAC-SHA256 Credential=MOCKACCESSKEY/20260101/auto/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
             Signature=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef\r\n\
             x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\r\n\
             x-amz-date: 20260101T000000Z\r\nConnection: close\r\n\r\n",
        );
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(response.contains("SignatureDoesNotMatch"), "{response}");
    }

    /// A wrong body hash and a stale date are two distinct rejections.
    #[test]
    fn body_hash_and_date_skew_are_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let storage = storage(&mock, "ops-bucket", "ops");

        mock.tamper_next_body_hash();
        let err = storage.read("catalog.json", "reading catalog.json").unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("x-amz-content-sha256"), "{err}");

        mock.set_clock_offset_secs(3600);
        let err = storage.read("catalog.json", "reading catalog.json").unwrap_err();
        assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("RequestTimeTooSkewed"), "{err}");
        mock.set_clock_offset_secs(0);
        // Back in time: the same request verifies (and the mock records it).
        mock.put_object("ops/catalog.json", b"{}");
        assert_eq!(storage.read("catalog.json", "reading").unwrap(), b"{}");
        assert!(mock.verified_requests() >= 1);
    }

    /// Missing signatures and presigned-shaped requests are refused.
    #[test]
    fn unsigned_and_presigned_requests_are_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let unsigned = mock.raw_request(
            "GET /ops-bucket/ops/catalog.json HTTP/1.1\r\nHost: 127.0.0.1\r\n\
             Connection: close\r\n\r\n",
        );
        assert!(unsigned.starts_with("HTTP/1.1 403"), "{unsigned}");
        assert!(unsigned.contains("missing Authorization"), "{unsigned}");
        let presigned = mock.raw_request(
            "GET /ops-bucket/ops/catalog.json?X-Amz-Signature=deadbeef&X-Amz-Credential=K%2F20260101%2Fauto%2Fs3%2Faws4_request \
             HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        );
        assert!(presigned.starts_with("HTTP/1.1 403"), "{presigned}");
        assert!(presigned.contains("presigned"), "{presigned}");
    }

    /// The read and write body cap refuses over-sized objects with a clear
    /// error (a small cap stands in for the 5 GiB single-PUT limit).
    #[test]
    fn object_size_cap_refuses_large_reads_and_writes() {
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("ops/big", &[7u8; 32]);
        let mut storage = storage(&mock, "ops-bucket", "ops");
        storage.client.max_object_bytes = 16;

        let err = storage.read("big", "reading data file \"big\"").unwrap_err();
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
        assert!(err.to_string().contains("did not finish after 3 pages"), "{err}");
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

        let err = ghost.read("catalog.json", "reading catalog.json").unwrap_err();
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
        assert!(!storage.put_new("k1", b"v2").unwrap(), "create-only still holds");
        assert_eq!(mock.conditional_rejections(), 1, "no second probe");

        storage.put("catalog.json", b"cat-1").unwrap();
        let (_, tag) = storage.read_tagged("catalog.json", "reading").unwrap();
        assert!(storage.put_if_match("catalog.json", b"cat-2", &tag).unwrap());
        assert!(!storage.put_if_match("catalog.json", b"cat-3", &tag).unwrap());
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
