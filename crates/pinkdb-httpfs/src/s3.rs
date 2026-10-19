//! The S3-compatible object client, shared by pink-lake and the httpfs table
//! sources (wave 75 extraction; no behavior change on the lake path).
//!
//! # Design
//!
//! - **Blocking, dependency-light**: [`ureq`] over rustls, `hmac` + `sha2`
//!   for AWS SigV4 — no aws-sdk, no async runtime. Every caller (the lake,
//!   `read_csv`, wave 72's parquet-over-object-storage) is synchronous.
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
//!   ([`Put::ConditionalUnsupported`]): a check-then-put fallback cannot keep
//!   the single-writer contract, so it is opt-in at the lake layer
//!   (`S3Config::allow_degraded(true)` / `PINKDB_S3_ALLOW_DEGRADED=1`).
//! - **Error mapping**: 404 → not found (the caller decides whether that is
//!   `NotALake`, `VersionNotFound`, or an I/O not-found for a data file); a
//!   404 *body* naming `NoSuchBucket` is a clear "bucket does not exist"
//!   error at the lake layer; 403 → [`Error::Auth`]; everything else is a
//!   [`Error::Status`] naming the S3 code.
//!
//! # Timeouts, retries, addressing and size limits
//!
//! - Every request has one whole-request (connect + transfer) timeout of
//!   `HTTP_TIMEOUT` (120 s); large data objects must fit inside it.
//! - **Retries** (exponential backoff with jitter, [`DEFAULT_RETRIES`]
//!   retries after the first attempt; `S3Config::with_retries` /
//!   `PINKDB_S3_RETRIES`): `429`/`503` (`SlowDown`/`ServiceUnavailable`,
//!   which S3 returns before applying a request) are retried for every
//!   request; `500`/`502`/`504` and transport errors are retried only for
//!   requests that are safe to repeat — reads, deletes, range reads, and
//!   unconditional PUTs (data files are written once under fresh names). A
//!   *conditional* PUT that failed ambiguously is never retried: if it
//!   landed, a retry would see its own write as a `412` conflict; the caller
//!   reopens the lake instead (see pink-lake's README). The `409 Conflict`
//!   retry for a conditional write is separate, bounded by
//!   `CONFLICT_RETRIES`.
//! - Redirects are not followed: a 301 (wrong region/endpoint) surfaces as
//!   an I/O error naming `PermanentRedirect`.
//! - Plain `http://` endpoints send credentials unencrypted: they are
//!   refused unless explicitly allowed (`S3Config::allow_http(true)` /
//!   `PINKDB_S3_ALLOW_HTTP=1`, for local test endpoints). Region `auto`
//!   without a custom endpoint is refused (it would resolve to
//!   `s3.auto.amazonaws.com`).
//! - Multipart uploads are not implemented: one `PutObject` is used, so one
//!   object must be ≤ 5 GiB (the S3 single-PUT limit). Larger PUTs are
//!   refused client-side with a clear error, and reads are capped at the
//!   same size. No bucket creation, no lifecycle rules, no server-side copy,
//!   no versioning support.
//! - **Range reads** ([`S3Client::get_object_range`]) send `Range:
//!   bytes=start-end` (signed), accept `206 Partial Content`, and tolerate a
//!   server that ignores the header by slicing a `200` body. This is the
//!   surface wave 72's parquet reader consumes through
//!   [`crate::Fetcher`].

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use crate::sigv4;
use crate::sigv4::{Credentials, SigningInput};
use crate::{ConfigError, DEFAULT_RETRIES, Error, HTTP_TIMEOUT, MAX_OBJECT_BYTES, backoff};

/// The SigV4 service name for both AWS S3 and R2.
const S3_SERVICE: &str = "s3";

/// Retries for a retryable conditional-write race (`409 Conflict` when a
/// concurrent delete raced the write; AWS documents retrying `PutObject`).
const CONFLICT_RETRIES: usize = 3;

/// Explicit S3 configuration (the `s3://` URL path resolves the same fields
/// from the environment).
///
/// ```no_run
/// # #[cfg(feature = "s3")] {
/// use pinkdb_httpfs::S3Config;
///
/// let config = S3Config::new("my-bucket")
///     .with_prefix("lakes/mine")
///     .with_region("auto")
///     .with_endpoint("https://<account>.r2.cloudflarestorage.com")
///     .with_credentials("access-key-id", "secret-access-key");
/// let client = pinkdb_httpfs::s3::S3Client::new(config)?;
/// # }
/// # Ok::<(), pinkdb_httpfs::ConfigError>(())
/// ```
/// `Debug` redacts the secret access key and the session token.
#[derive(Clone)]
pub struct S3Config {
    /// Bucket name (required): 3–63 lowercase ASCII letters, digits, `.` and
    /// `-`.
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
    /// Retries after a retryable failure (see the module docs).
    pub retries: u32,
    /// The HTTP `User-Agent` (defaults to this crate's name/version;
    /// pink-lake sets its own so its wire behavior is unchanged).
    pub user_agent: Option<String>,
}

impl std::fmt::Debug for S3Config {
    // Hand-written so `{:?}` on a config (or a `Lake` holding one) can never
    // print credentials into logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |v: &Option<String>| v.as_ref().map(|_| "<redacted>");
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("path_style", &self.path_style)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &redact(&self.secret_access_key))
            .field("session_token", &redact(&self.session_token))
            .field("allow_degraded", &self.allow_degraded)
            .field("allow_http", &self.allow_http)
            .field("retries", &self.retries)
            .field("user_agent", &self.user_agent)
            .finish()
    }
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
            retries: DEFAULT_RETRIES,
            user_agent: None,
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
        if let Some(retries) = env_var("PINKDB_S3_RETRIES").and_then(|v| v.parse().ok()) {
            config.retries = retries;
        }
        config
    }

    /// Retries after a retryable failure (0 disables retrying).
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
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

    /// Override the HTTP `User-Agent` (pink-lake keeps its historical one).
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Resolve everything a client needs, or a clear error: a bad bucket /
    /// prefix is [`ConfigError::BadLocation`], missing credentials are
    /// [`ConfigError::Auth`], a bad endpoint is
    /// [`ConfigError::BadLocation`].
    pub fn resolve(self) -> Result<ResolvedS3, ConfigError> {
        self.resolve_with(&env_var)
    }

    /// [`S3Config::resolve`] with an injected environment lookup (deterministic
    /// tests; public so downstream crates can pin their own environment
    /// shape).
    #[doc(hidden)]
    pub fn resolve_with(
        self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<ResolvedS3, ConfigError> {
        let location = if self.prefix.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        };
        validate_bucket_name(&self.bucket).map_err(|message| ConfigError::BadLocation {
            location: location.clone(),
            message,
        })?;
        let prefix = self.prefix.trim_end_matches('/');
        if prefix.starts_with('/') {
            return Err(ConfigError::BadLocation {
                location,
                message: "the prefix must not start with `/`".to_string(),
            });
        }
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
                    return Err(ConfigError::Auth {
                        path: PathBuf::from(&location),
                        message: "no S3 credentials: set AWS_ACCESS_KEY_ID and \
                                  AWS_SECRET_ACCESS_KEY (plus AWS_SESSION_TOKEN for temporary \
                                  credentials), or configure S3Config::with_credentials"
                            .to_string(),
                    });
                }
            },
            _ => {
                return Err(ConfigError::Auth {
                    path: PathBuf::from(&location),
                    message: "incomplete S3 credentials: set both the access key id and the \
                              secret access key"
                        .to_string(),
                });
            }
        };
        let session_token = self.session_token.filter(|t| !t.is_empty()).or_else(|| {
            if credentials_from_env {
                env("AWS_SESSION_TOKEN")
            } else {
                None
            }
        });
        if self.endpoint.is_none() && self.region == "auto" {
            return Err(ConfigError::BadLocation {
                location: self.region.clone(),
                message: "region `auto` needs a custom endpoint (R2 uses `auto` with \
                          AWS_ENDPOINT_URL); without one, set a real AWS region"
                    .to_string(),
            });
        }
        let path_style = self.path_style.unwrap_or(self.endpoint.is_some());
        let endpoint = match &self.endpoint {
            Some(url) => {
                let endpoint = Endpoint::parse(url, path_style).map_err(|message| {
                    ConfigError::BadLocation {
                        location: url.clone(),
                        message: format!("invalid S3 endpoint: {message}"),
                    }
                })?;
                if endpoint.scheme == "http" && !self.allow_http {
                    return Err(ConfigError::BadLocation {
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
        Ok(ResolvedS3 {
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
            retries: self.retries,
            user_agent: self.user_agent,
        })
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// An environment flag: `1`, `true`, `yes`, `on` (case-insensitive).
fn env_truthy(name: &str) -> Option<bool> {
    env_var(name).map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

/// The S3 bucket-name shape this client accepts: 3–63 bytes of lowercase
/// ASCII letters, digits, `.` and `-` (the DNS-compatible object-storage
/// rule; shared by pink-lake's `s3://` URL parser and `S3Config`, so a URL
/// and an explicit config reject exactly the same names).
fn validate_bucket_name(bucket: &str) -> std::result::Result<(), String> {
    let characters_ok = bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.');
    if (3..=63).contains(&bucket.len()) && characters_ok {
        Ok(())
    } else {
        Err(
            "the bucket name must be 3–63 characters of lowercase ASCII letters, digits, \
             `.` and `-` (expected s3://bucket/prefix)"
                .to_string(),
        )
    }
}

/// Resolved connection parameters. Fields are private; the lake reads the
/// bucket/prefix and hands the value back to [`S3Client::from_resolved`].
#[derive(Debug)]
pub struct ResolvedS3 {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Endpoint,
    credentials: Credentials,
    allow_degraded: bool,
    retries: u32,
    user_agent: Option<String>,
}

impl ResolvedS3 {
    /// The bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The key prefix (no trailing slash; empty = bucket root).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Whether the caller explicitly accepted the degraded write fallback.
    pub fn allow_degraded(&self) -> bool {
        self.allow_degraded
    }

    /// Retries after a retryable failure.
    pub fn retries(&self) -> u32 {
        self.retries
    }
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
        let unreserved =
            b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~';
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
pub enum Condition<'a> {
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

/// A conservative ETag: a short token of quoted/unquoted hex-ish characters.
/// Rejects control characters (notably CR/LF) and anything that could not be a
/// safe header value — the value is echoed back in an `If-Match`.
fn valid_etag(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 256
        && v.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'"' | b'-' | b'_' | b'.' | b'/' | b'+' | b'=')
        })
}

/// The result of a conditional `PutObject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Put {
    Written,
    /// HTTP 412 (or 404 for `If-Match` on a missing object).
    PreconditionFailed,
    /// The endpoint rejected the conditional header itself.
    ConditionalUnsupported,
}

/// One object read.
#[derive(Debug)]
pub enum Get {
    Found {
        bytes: Vec<u8>,
        etag: Option<String>,
        /// The `Content-Range` of a 206 answer (range reads only).
        content_range: Option<String>,
    },
    NotFound,
}

/// The facts a HEAD reports (the lake's `stat`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object size in bytes (`Content-Length`; 0 when the endpoint omits it).
    pub size: u64,
    /// The object's ETag, quotes included, when the endpoint sent one.
    pub etag: Option<String>,
    /// `Last-Modified` in microseconds since the Unix epoch, when parseable.
    pub modified_micros: Option<i64>,
}

/// One `ListObjectsV2` page.
#[derive(Debug)]
pub struct ListPage {
    pub keys: Vec<String>,
    pub next_token: Option<String>,
}

/// One raw HTTP response.
#[derive(Debug)]
struct HttpResponse {
    status: u16,
    etag: Option<String>,
    body: Vec<u8>,
    content_length: Option<u64>,
    content_range: Option<String>,
    last_modified: Option<String>,
}

/// One S3 client (bucket + region + endpoint + credentials + connection pool).
#[derive(Debug)]
pub struct S3Client {
    bucket: String,
    region: String,
    endpoint: Endpoint,
    credentials: Credentials,
    agent: ureq::Agent,
    /// The read/write body cap (tests lower it to exercise the overflow path
    /// without transferring gigabytes).
    max_object_bytes: u64,
    /// Retries after a retryable failure (see the module docs).
    retries: u32,
}

impl S3Client {
    /// Connect from explicit configuration (environment fallbacks included).
    pub fn new(config: S3Config) -> Result<Self, ConfigError> {
        let resolved = config.resolve()?;
        Ok(Self::from_resolved(&resolved))
    }

    /// Build a client from already-resolved configuration (pink-lake resolves
    /// once so it can read the bucket/prefix and map config errors itself).
    pub fn from_resolved(resolved: &ResolvedS3) -> Self {
        let user_agent = resolved
            .user_agent
            .clone()
            .unwrap_or_else(|| concat!("pinkdb-httpfs/", env!("CARGO_PKG_VERSION")).to_string());
        Self {
            bucket: resolved.bucket.clone(),
            region: resolved.region.clone(),
            endpoint: resolved.endpoint.clone(),
            credentials: resolved.credentials.clone(),
            agent: ureq::Agent::config_builder()
                .timeout_global(Some(HTTP_TIMEOUT))
                // 4xx/5xx are data here: 404/412/403 have meanings.
                .http_status_as_error(false)
                // A 301 (wrong region/endpoint) must surface as an S3
                // error, not be followed blindly.
                .max_redirects(0)
                .user_agent(user_agent)
                .build()
                .into(),
            max_object_bytes: MAX_OBJECT_BYTES,
            retries: resolved.retries,
        }
    }

    /// The bucket this client is rooted at.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Lower the read/write body cap (tests only; `#[doc(hidden)]` because it
    /// is not a production knob).
    #[doc(hidden)]
    pub fn set_max_object_bytes(&mut self, bytes: u64) {
        self.max_object_bytes = bytes;
    }

    /// One signed request with retries: `429`/`503` always, `500`/`502`/
    /// `504` and transport errors only when repeating the request is safe
    /// (not a conditional write — see the module docs). Each attempt is
    /// re-signed (fresh date).
    fn execute(
        &self,
        method: &str,
        key: &str,
        query_pairs: &[(&str, &str)],
        extra: &[(&'static str, String)],
        body: &[u8],
    ) -> Result<HttpResponse, Error> {
        let conditional = extra.iter().any(|(name, _)| name.starts_with("if-"));
        let mut attempt = 0u32;
        loop {
            let outcome = self.execute_once(method, key, query_pairs, extra, body);
            let retryable = match &outcome {
                // A conditional write must not be retried on a transient error:
                // the request may have committed, and a retry would see its own
                // object (a bogus 412/conflict). Same guard as 5xx/transport.
                Ok(response) if matches!(response.status, 429 | 503) => !conditional,
                Ok(response) if matches!(response.status, 500 | 502 | 504) => !conditional,
                Err(Error::Transport { .. }) => !conditional,
                _ => false,
            };
            if !retryable || attempt >= self.retries {
                return outcome;
            }
            attempt += 1;
            std::thread::sleep(backoff(attempt));
        }
    }

    /// One signed request. `extra` headers are sent and signed (conditional
    /// preconditions and a `Range` must not be tamperable).
    fn execute_once(
        &self,
        method: &str,
        key: &str,
        query_pairs: &[(&str, &str)],
        extra: &[(&'static str, String)],
        body: &[u8],
    ) -> Result<HttpResponse, Error> {
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
        let build_error = |e: ureq::http::Error| Error::Protocol {
            context: context.clone(),
            message: format!("could not build the request: {e}"),
        };
        let mut response = if matches!(method, "GET" | "HEAD" | "DELETE") {
            self.agent.run(request.body(()).map_err(build_error)?)
        } else {
            self.agent.run(request.body(body).map_err(build_error)?)
        }
        .map_err(|e| match e {
            ureq::Error::Io(io) => Error::Transport {
                context: context.clone(),
                source: io,
            },
            other => Error::Transport {
                context: context.clone(),
                source: io::Error::other(other.to_string()),
            },
        })?;
        let status = response.status().as_u16();
        // A declared body over the cap is refused before any bytes are read
        // (the read below is limited too, for a lying Content-Length). HEAD
        // only reports the object's length, so it is exempt.
        if method != "HEAD"
            && let Some(length) = response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
            && length > self.max_object_bytes
        {
            return Err(Error::Protocol {
                context: context.clone(),
                message: format!(
                    "the response is {length} bytes, over this client's {}-byte object cap \
                             (no multipart downloads)",
                    self.max_object_bytes
                ),
            });
        }
        // Server-provided ETags are echoed into an `If-Match` header; accept
        // only a conservative value so a hostile/broken server cannot inject
        // header syntax (CR/LF) or arbitrary bytes.
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .filter(|v| valid_etag(v))
            .map(str::to_string);
        let bytes = if method == "HEAD" {
            Vec::new()
        } else {
            response
                .body_mut()
                .with_config()
                .limit(self.max_object_bytes)
                .read_to_vec()
                .map_err(|e| Error::Transport {
                    context: context.clone(),
                    source: io::Error::other(e.to_string()),
                })?
        };
        Ok(HttpResponse {
            status,
            etag,
            body: bytes,
            content_length: response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok()),
            content_range: response
                .headers()
                .get("content-range")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            last_modified: response
                .headers()
                .get("last-modified")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
        })
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
    fn status_error(&self, context: &str, status: u16, body: &[u8]) -> Error {
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
        if status == 403 || status == 401 {
            return Error::Auth {
                context: context.to_string(),
                code,
                message,
            };
        }
        Error::Status {
            context: context.to_string(),
            status,
            code,
            message,
        }
    }

    /// Whole-object GET.
    pub fn get_object(&self, key: &str) -> Result<Get, Error> {
        let response = self.execute("GET", key, &[], &[], b"")?;
        match response.status {
            200 => Ok(Get::Found {
                bytes: response.body,
                etag: response.etag,
                content_range: None,
            }),
            404 => Ok(Get::NotFound),
            other => {
                Err(self.status_error(&format!("GET {}", self.abs(key)), other, &response.body))
            }
        }
    }

    /// Byte-range GET: `Range: bytes=offset-(offset+len-1)`. A `206` body is
    /// returned as-is; a server that ignores the header and answers `200` is
    /// sliced to the requested window; `416` is an [`Error::Status`].
    pub fn get_object_range(&self, key: &str, offset: u64, len: u64) -> Result<Get, Error> {
        if len == 0 {
            return Ok(Get::Found {
                bytes: Vec::new(),
                etag: None,
                content_range: None,
            });
        }
        let end = offset.saturating_add(len);
        let context = format!("GET {} (range {offset}..{end})", self.abs(key));
        let extra = vec![(
            "range" as &'static str,
            format!("bytes={offset}-{}", end.saturating_sub(1)),
        )];
        let response = self.execute("GET", key, &[], &extra, b"")?;
        match response.status {
            200 => {
                // The server ignored Range. Slice locally when the body is
                // long enough; otherwise the requested window is not there.
                let start = usize::try_from(offset).map_err(|_| Error::Protocol {
                    context: context.clone(),
                    message: "range offset does not fit this platform's usize".to_string(),
                })?;
                let stop = usize::try_from(end).map_err(|_| Error::Protocol {
                    context: context.clone(),
                    message: "range end does not fit this platform's usize".to_string(),
                })?;
                if response.body.len() < stop {
                    return Err(Error::Protocol {
                        context,
                        message: format!(
                            "the server ignored the Range header and returned {} bytes, \
                             shorter than the requested window",
                            response.body.len()
                        ),
                    });
                }
                Ok(Get::Found {
                    bytes: response.body[start..stop].to_vec(),
                    etag: response.etag,
                    content_range: None,
                })
            }
            206 => Ok(Get::Found {
                bytes: response.body,
                etag: response.etag,
                content_range: response.content_range,
            }),
            404 => Ok(Get::NotFound),
            other => Err(self.status_error(&context, other, &response.body)),
        }
    }

    pub fn head_object(&self, key: &str) -> Result<bool, Error> {
        let response = self.execute("HEAD", key, &[], &[], b"")?;
        match response.status {
            200 => Ok(true),
            404 => Ok(false),
            other => {
                Err(self.status_error(&format!("HEAD {}", self.abs(key)), other, &response.body))
            }
        }
    }

    /// A HEAD with the object's facts: `None` when it does not exist. An
    /// endpoint that omits `Content-Length`/`Last-Modified` answers `size` 0
    /// / `None` — the lake maintenance age gate then treats the object as
    /// ageless and only `cleanup_all` touches it.
    pub fn stat_object(&self, key: &str) -> Result<Option<ObjectMeta>, Error> {
        let response = self.execute("HEAD", key, &[], &[], b"")?;
        match response.status {
            200 => Ok(Some(ObjectMeta {
                size: response.content_length.unwrap_or(0),
                etag: response.etag,
                modified_micros: response
                    .last_modified
                    .as_deref()
                    .and_then(parse_http_date)
                    .map(|secs| secs.saturating_mul(1_000_000)),
            })),
            404 => Ok(None),
            other => {
                Err(self.status_error(&format!("HEAD {}", self.abs(key)), other, &response.body))
            }
        }
    }

    pub fn put_object(
        &self,
        key: &str,
        bytes: &[u8],
        condition: Condition<'_>,
    ) -> Result<Put, Error> {
        if bytes.len() as u64 > self.max_object_bytes {
            return Err(Error::Protocol {
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
            let response = self.execute("PUT", key, &[], &extra, bytes)?;
            match response.status {
                200 => return Ok(Put::Written),
                // `If-Match` on a vanished object is a precondition failure.
                404 if matches!(condition, Condition::IfMatch(_)) => {
                    return Ok(Put::PreconditionFailed);
                }
                412 => return Ok(Put::PreconditionFailed),
                409 if condition.is_conditional() && attempts < CONFLICT_RETRIES => {
                    // A concurrent delete raced the write; AWS documents
                    // retrying PutObject.
                    std::thread::sleep(Duration::from_millis(50 * attempts as u64));
                    continue;
                }
                other => {
                    let error = self.status_error(&context, other, &response.body);
                    if condition.is_conditional() && is_unsupported_conditional(other, &error) {
                        return Ok(Put::ConditionalUnsupported);
                    }
                    return Err(error);
                }
            }
        }
    }

    pub fn delete_object(&self, key: &str) -> Result<(), Error> {
        let context = format!("DELETE {}", self.abs(key));
        let response = self.execute("DELETE", key, &[], &[], b"")?;
        match response.status {
            200 | 204 => Ok(()),
            // S3 DELETE is idempotent: a missing object is success.
            404 => Ok(()),
            other => Err(self.status_error(&context, other, &response.body)),
        }
    }

    pub fn list_objects(
        &self,
        prefix: &str,
        delimiter: &str,
        continuation: Option<&str>,
    ) -> Result<ListPage, Error> {
        let mut pairs: Vec<(&str, &str)> = vec![
            ("delimiter", delimiter),
            ("list-type", "2"),
            ("prefix", prefix),
        ];
        if let Some(token) = continuation {
            pairs.push(("continuation-token", token));
        }
        let context = format!("LIST s3://{}/{}", self.bucket, prefix);
        let response = self.execute("GET", "", &pairs, &[], b"")?;
        if response.status != 200 {
            return Err(self.status_error(&context, response.status, &response.body));
        }
        let text = String::from_utf8_lossy(&response.body).into_owned();
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
fn is_unsupported_conditional(status: u16, error: &Error) -> bool {
    if status == 501 {
        return true;
    }
    let code = match error {
        Error::Status { code, .. } | Error::Auth { code, .. } => code.as_str(),
        _ => return false,
    };
    status == 400 && matches!(code, "NotImplemented" | "InvalidRequest")
}

// ---------------------------------------------------------------------------
// Minimal XML helpers (ListObjectsV2 / error responses; std only).

/// Parse an HTTP `Last-Modified`/`Date` header in the IMF-fixdate form S3
/// sends (`Sun, 06 Nov 1994 08:49:37 GMT`) into Unix seconds. `None` for
/// anything else — an unparsable header must never be guessed at, and the
/// maintenance age gate then treats the object as ageless.
fn parse_http_date(text: &str) -> Option<i64> {
    let rest = text.split_once(", ").map(|(_, rest)| rest).unwrap_or(text);
    let mut parts = rest.split(' ');
    let day: i64 = parts.next()?.parse().ok()?;
    let month: i64 = match parts.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts.next()?.parse().ok()?;
    let time = parts.next()?;
    if !parts.next()?.eq_ignore_ascii_case("GMT") {
        return None;
    }
    let mut clock = time.split(':');
    let hour: i64 = clock.next()?.parse().ok()?;
    let minute: i64 = clock.next()?.parse().ok()?;
    let second: i64 = clock.next()?.parse().ok()?;
    if clock.next().is_some()
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Days since the Unix epoch for a civil date (Howard Hinnant's
/// `days_from_civil`, the inverse of `time::civil_from_days`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

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

    fn client(mock: &crate::mock_s3::MockS3, bucket: &str) -> S3Client {
        S3Client::new(
            S3Config::new(bucket)
                .with_region("auto")
                .with_endpoint(mock.endpoint())
                .allow_http(true)
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
        assert_eq!(
            target.url,
            "http://127.0.0.1:9000/base/path/b/k?list-type=2"
        );
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
        assert!(matches!(err, ConfigError::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("no S3 credentials"), "{err}");

        let err = S3Config::new("bucket")
            .with_credentials("only-id", "")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, ConfigError::Auth { .. }), "{err:?}");

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
        assert!(matches!(err, ConfigError::BadLocation { .. }), "{err:?}");
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
        assert!(matches!(err, ConfigError::BadLocation { .. }), "{err:?}");
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
                matches!(err, ConfigError::BadLocation { .. }),
                "{bad}: {err:?}"
            );
        }
        let err = S3Config::new("bucket")
            .with_prefix("/lead")
            .with_credentials("id", "secret")
            .resolve_with(&|_| None)
            .unwrap_err();
        assert!(matches!(err, ConfigError::BadLocation { .. }), "{err:?}");
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
        assert!(matches!(err, ConfigError::BadLocation { .. }), "{err:?}");
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
        assert!(
            xml_field(error, "Message")
                .unwrap()
                .contains("does not exist")
        );
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
    fn etags_are_validated_before_being_echoed() {
        assert!(valid_etag("\"abc123\""));
        assert!(valid_etag("abc-1_2.3/4+5="));
        assert!(!valid_etag(""));
        assert!(!valid_etag("\"abc\r\nX-Evil: 1\""));
        assert!(!valid_etag(&"a".repeat(257)));
    }

    #[test]
    fn unsupported_conditional_detection() {
        let status = |status, code: &str| Error::Status {
            context: String::new(),
            status,
            code: code.to_string(),
            message: String::new(),
        };
        assert!(is_unsupported_conditional(
            501,
            &status(501, "NotImplemented")
        ));
        assert!(is_unsupported_conditional(
            400,
            &status(400, "InvalidRequest")
        ));
        assert!(!is_unsupported_conditional(
            400,
            &status(400, "InvalidArgument")
        ));
        assert!(!is_unsupported_conditional(
            500,
            &status(500, "InternalError")
        ));
        // 403 is auth, never "unsupported".
        assert!(!is_unsupported_conditional(
            403,
            &Error::Auth {
                context: String::new(),
                code: "AccessDenied".into(),
                message: String::new(),
            }
        ));
    }

    /// The mock verifies full SigV4; a wrong secret is rejected by the
    /// recomputation, not accepted structurally.
    #[test]
    fn fabricated_signature_is_rejected() {
        let mock = crate::mock_s3::MockS3::start();
        let client = S3Client::new(
            S3Config::new("ops-bucket")
                .with_region("auto")
                .with_endpoint(mock.endpoint())
                .allow_http(true)
                .with_credentials(mock.access_key_id(), "not-the-secret"),
        )
        .unwrap();
        let err = client.get_object("catalog.json").unwrap_err();
        assert!(matches!(err, Error::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("SignatureDoesNotMatch"), "{err}");
        assert_eq!(mock.verified_requests(), 0, "no request verified");
    }

    /// Whole-object and byte-range GETs over the S3 mock (the mock honors
    /// `Range` with a 206; the extraction must not change that).
    #[test]
    fn whole_and_range_gets_work() {
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("data/x.csv", b"one,two\n1,2\n3,4\n");
        let client = client(&mock, "ops-bucket");

        match client.get_object("data/x.csv").unwrap() {
            Get::Found { bytes, etag, .. } => {
                assert_eq!(bytes, b"one,two\n1,2\n3,4\n");
                assert!(etag.is_some());
            }
            other => panic!("expected bytes, got {other:?}"),
        }
        match client.get_object_range("data/x.csv", 8, 4).unwrap() {
            Get::Found {
                bytes,
                content_range,
                ..
            } => {
                assert_eq!(bytes, b"1,2\n");
                assert!(
                    content_range
                        .as_deref()
                        .unwrap_or("")
                        .starts_with("bytes 8-11/"),
                    "{content_range:?}"
                );
            }
            other => panic!("expected a range, got {other:?}"),
        }
        // The range request is a signed GET carrying the Range header.
        assert!(
            mock.requests()
                .iter()
                .any(|r| r == "GET /ops-bucket/data/x.csv"),
            "{:?}",
            mock.requests()
        );
        // Past the end is a 416 status, not a silent short read.
        let err = client.get_object_range("data/x.csv", 999, 10).unwrap_err();
        assert!(matches!(err, Error::Status { status: 416, .. }), "{err:?}");
        match client.get_object("missing") {
            Ok(Get::NotFound) => {}
            other => panic!("expected not-found, got {other:?}"),
        }
        assert_eq!(client.stat_object("data/x.csv").unwrap().unwrap().size, 16);
    }

    /// `Range` is signed (the mock rebuilds the canonical request and would
    /// reject an unsigned-but-listed header).
    #[test]
    fn range_requests_verify_against_the_mock() {
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("k", b"0123456789");
        let client = client(&mock, "ops-bucket");
        let _ = client.get_object_range("k", 2, 3).unwrap();
        assert!(mock.verified_requests() >= 1);
        let auth = mock.authorizations().pop().unwrap_or_default();
        assert!(auth.contains("range"), "{auth}");
    }

    /// The read and write body cap refuses over-sized objects with a clear
    /// error (a small cap stands in for the 5 GiB single-PUT limit).
    #[test]
    fn object_size_cap_refuses_large_reads_and_writes() {
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("big", &[7u8; 32]);
        let mut client = client(&mock, "ops-bucket");
        client.set_max_object_bytes(16);

        let err = client.get_object("big").unwrap_err();
        assert!(matches!(err, Error::Protocol { .. }), "{err:?}");
        assert!(err.to_string().contains("16-byte object cap"), "{err}");

        let err = client
            .put_object("small", &[0u8; 17], Condition::None)
            .unwrap_err();
        assert!(matches!(err, Error::Protocol { .. }), "{err:?}");
        assert!(err.to_string().contains("capped at 16 bytes"), "{err}");
    }

    /// Conditional writes are exact over the wire: create-only and
    /// if-match replace.
    #[test]
    fn conditional_put_roundtrip() {
        let mock = crate::mock_s3::MockS3::start();
        let client = client(&mock, "ops-bucket");
        assert_eq!(
            client
                .put_object("k", b"one", Condition::IfNoneMatch)
                .unwrap(),
            Put::Written
        );
        assert_eq!(
            client
                .put_object("k", b"two", Condition::IfNoneMatch)
                .unwrap(),
            Put::PreconditionFailed
        );
        let Get::Found {
            etag: Some(tag), ..
        } = client.get_object("k").unwrap()
        else {
            panic!("expected an etag");
        };
        assert_eq!(
            client
                .put_object("k", b"two", Condition::IfMatch(&tag))
                .unwrap(),
            Put::Written
        );
        assert_eq!(
            client
                .put_object("k", b"three", Condition::IfMatch(&tag))
                .unwrap(),
            Put::PreconditionFailed
        );
    }

    /// The user agent is configurable so pink-lake's wire behavior stays
    /// `pink-lake/<version>` after the extraction.
    #[test]
    fn user_agent_is_configurable() {
        let mock = crate::mock_s3::MockS3::start();
        let client = S3Client::new(
            S3Config::new("ops-bucket")
                .with_region("auto")
                .with_endpoint(mock.endpoint())
                .allow_http(true)
                .with_credentials(mock.access_key_id(), "mock-secret")
                .with_user_agent("pink-lake/test"),
        )
        .unwrap();
        let _ = client.head_object("missing");
        assert!(mock.verified_requests() >= 1);
    }
}
