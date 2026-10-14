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
//!   header (HTTP 501 / `NotImplemented`) degrades once, with a documented
//!   check-then-put window (see the crate docs and the README).
//! - **Error mapping**: 404 → not found (the caller decides whether that is
//!   `NotALake`, `VersionNotFound`, or an I/O not-found for a data file);
//!   403 → [`LakeError::Auth`]; 412 → a precondition failure (`Ok(false)`,
//!   which the lake reports as a `Conflict`); everything else is an I/O error
//!   naming the operation and the S3 error code.
//!
//! # Not implemented (documented limits)
//!
//! Multipart uploads: a single `PutObject` is used, so one object must be
//! ≤ 5 GiB (the S3 single-PUT limit); larger tables fail with the server's
//! error. Reads are capped at the same limit. No bucket creation, no
//! lifecycle rules, no server-side copy, no versioning support, no lake-side
//! GC (old versions and snapshots stay, exactly like local).

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
/// capped by S3 at 5 GiB (no multipart uploads — see the module docs).
const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Retries for a retryable conditional-write race (`409 Conflict` when a
/// concurrent delete raced the write; AWS documents retrying `PutObject`).
const CONFLICT_RETRIES: usize = 3;

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
    /// Bucket name (required).
    pub bucket: String,
    /// Key prefix inside the bucket (no leading slash; empty = bucket root).
    pub prefix: String,
    /// SigV4 region. R2 uses `auto`; AWS defaults to `us-east-1` (or
    /// `AWS_REGION`/`AWS_DEFAULT_REGION` via [`S3Config::from_env`]).
    pub region: String,
    /// Custom endpoint URL (`https://host[:port]`), or `None` for AWS S3.
    pub endpoint: Option<String>,
    /// `true` path-style, `false` virtual-hosted; `None` picks path-style
    /// when a custom endpoint is set, virtual-hosted otherwise.
    pub path_style: Option<bool>,
    /// Credentials; when not set explicitly, `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` are used.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
}

impl S3Config {
    /// A configuration with no environment lookups (region `us-east-1`,
    /// no endpoint; credentials still fall back to the environment at open
    /// time unless set explicitly).
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
        }
    }

    /// [`S3Config::new`] plus the usual environment variables: `AWS_REGION`
    /// (or `AWS_DEFAULT_REGION`), `AWS_ENDPOINT_URL_S3` (or
    /// `AWS_ENDPOINT_URL`), `AWS_S3_FORCE_PATH_STYLE`. Credentials always
    /// fall back to the environment when not set explicitly.
    pub fn from_env(bucket: impl Into<String>) -> Self {
        let mut config = Self::new(bucket);
        if let Some(region) = env_var("AWS_REGION").or_else(|| env_var("AWS_DEFAULT_REGION")) {
            config.region = region;
        }
        config.endpoint = env_var("AWS_ENDPOINT_URL_S3").or_else(|| env_var("AWS_ENDPOINT_URL"));
        config.path_style = env_truthy("AWS_S3_FORCE_PATH_STYLE");
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
        if self.bucket.is_empty()
            || !self.bucket.bytes().all(|b| {
                b.is_ascii_lowercase()
                    || b.is_ascii_uppercase()
                    || b.is_ascii_digit()
                    || b == b'-'
                    || b == b'.'
            })
        {
            return Err(LakeError::BadLocation {
                location,
                message: "the bucket name must be non-empty and made of ASCII letters, digits, \
                          `.` and `-`"
                    .to_string(),
            });
        }
        let prefix = self.prefix.trim_end_matches('/');
        if prefix.starts_with('/') {
            return Err(LakeError::BadLocation {
                location,
                message: "the prefix must not start with `/`".to_string(),
            });
        }
        let (access_key_id, secret_access_key) = match (
            self.access_key_id.filter(|v| !v.is_empty()),
            self.secret_access_key.filter(|v| !v.is_empty()),
        ) {
            (Some(id), Some(secret)) => (id, secret),
            (None, None) => match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
                (Some(id), Some(secret)) if !id.is_empty() && !secret.is_empty() => (id, secret),
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
            .or_else(|| env("AWS_SESSION_TOKEN"));
        let path_style = self.path_style.unwrap_or(self.endpoint.is_some());
        let endpoint = match &self.endpoint {
            Some(url) => {
                Endpoint::parse(url, path_style).map_err(|message| LakeError::BadLocation {
                    location: url.clone(),
                    message: format!("invalid S3 endpoint: {message}"),
                })?
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
    /// headers; later writes use the documented check-then-put fallback.
    degraded: AtomicBool,
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
                    .user_agent(concat!("pink-lake/", env!("CARGO_PKG_VERSION")))
                    .build()
                    .into(),
            },
            prefix: resolved.prefix,
            root,
            degraded: AtomicBool::new(false),
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
            } => LakeError::Io {
                context: context.to_string(),
                source: io::Error::other(format!(
                    "{operation}: S3 HTTP {status} {code}: {message}"
                )),
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
            Ok(true) => Ok(Head::File),
            Ok(false) => Ok(Head::Missing),
            Err(e) => Err(self.map(&context, e)),
        }
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        match self.client.get_object(&self.key(rel)) {
            Ok(Get::Found { bytes, .. }) => Ok(bytes),
            Ok(Get::NotFound) => Err(self.not_found(rel, context)),
            Err(e) => Err(self.map(context, e)),
        }
    }

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        match self.client.get_object(&self.key(rel)) {
            Ok(Get::Found {
                bytes,
                etag: Some(etag),
            }) => Ok((bytes, VersionTag(etag))),
            Ok(Get::Found { etag: None, .. }) => Err(self.map(
                context,
                S3Error::Protocol {
                    context: context.to_string(),
                    message: "response carried no ETag; conditional writes are impossible".into(),
                },
            )),
            Ok(Get::NotFound) => Err(self.not_found(rel, context)),
            Err(e) => Err(self.map(context, e)),
        }
    }

    fn create_dir(&self, _rel: &str) -> Result<()> {
        // Object storage has no directories: a prefix is just key text.
        Ok(())
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let context = format!("PUT {}", self.abs(rel).display());
        match self.client.put_object(&self.key(rel), bytes, Condition::None) {
            Ok(Put::Written) => Ok(()),
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
        let key = self.key(rel);
        let context = format!("PUT {}", self.abs(rel).display());
        if !self.degraded.load(Ordering::Relaxed) {
            match self
                .client
                .put_object(&key, bytes, Condition::IfNoneMatch)
            {
                Ok(Put::Written) => return Ok(true),
                Ok(Put::PreconditionFailed) => return Ok(false),
                Ok(Put::ConditionalUnsupported) => self.degraded.store(true, Ordering::Relaxed),
                Err(e) => return Err(self.map(&context, e)),
            }
        }
        // Degraded path (the endpoint rejects `If-None-Match: *`):
        // check-then-put, with the documented window.
        match self.client.head_object(&key) {
            Ok(true) => Ok(false),
            Ok(false) => match self.client.put_object(&key, bytes, Condition::None) {
                Ok(Put::Written) => Ok(true),
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
        let key = self.key(rel);
        let context = format!("PUT {}", self.abs(rel).display());
        if !self.degraded.load(Ordering::Relaxed) {
            match self
                .client
                .put_object(&key, bytes, Condition::IfMatch(expected.as_str()))
            {
                Ok(Put::Written) => return Ok(true),
                Ok(Put::PreconditionFailed) => return Ok(false),
                Ok(Put::ConditionalUnsupported) => self.degraded.store(true, Ordering::Relaxed),
                Err(e) => return Err(self.map(&context, e)),
            }
        }
        // Degraded path: compare the current ETag, then PUT (documented
        // check-then-put window).
        match self.client.get_object(&key) {
            Ok(Get::Found {
                etag: Some(current),
                ..
            }) => {
                if current != expected.as_str() {
                    return Ok(false);
                }
                match self.client.put_object(&key, bytes, Condition::None) {
                    Ok(Put::Written) => Ok(true),
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
        loop {
            let page = self
                .client
                .list_objects(&prefix, "/", token.as_deref())
                .map_err(|e| self.map(&context, e))?;
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
        std::fs::write(&path, &bytes).map_err(|e| LakeError::Io {
            context: format!("writing the temp copy {}", path.display()),
            source: e,
        })?;
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
                .limit(MAX_OBJECT_BYTES)
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

/// Whether a failed conditional request looks like "this endpoint does not
/// support conditional writes" rather than a real error.
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

    /// An `S3Storage` pointed at the mock, with explicit credentials.
    fn storage(mock: &crate::mock_s3::MockS3, bucket: &str, prefix: &str) -> S3Storage {
        S3Storage::new(
            S3Config::new(bucket)
                .with_prefix(prefix)
                .with_region("auto")
                .with_endpoint(mock.endpoint())
                .with_credentials(mock.access_key_id(), "mock-secret"),
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
    }

    #[test]
    fn config_rejects_bad_buckets_and_prefixes() {
        for bad in ["", "bad bucket", "bad/bucket", "bad_bucket"] {
            let err = S3Config::new(bad)
                .with_credentials("id", "secret")
                .resolve_with(&|_| None)
                .unwrap_err();
            assert!(
                matches!(err, LakeError::BadLocation { .. }),
                "{bad}: {err:?}"
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
}
