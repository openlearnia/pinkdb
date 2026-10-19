//! pinkdb's **remote-file transport**: direct `https://` and `s3://` reads
//! without a lake (wave 75), the shared S3/SigV4 client pink-lake uses, and
//! the `read_csv('url')` table source.
//!
//! # What this crate is
//!
//! Two layers, deliberately small:
//!
//! 1. **Transport** ([`Fetcher`], [`open`]): a whole-object GET, a byte-range
//!    GET, and an optional size, on `https://` and `s3://` URLs. The S3 half
//!    was extracted verbatim from pink-lake (`AWS_*` / `PINKDB_S3_*`
//!    conventions and retry semantics unchanged); pink-lake now depends on
//!    this crate, so the lake and the engine share one client instead of two.
//!    `https://` is the default scheme; plain `http://` exists only for local
//!    test endpoints and needs an explicit opt-in ([`HttpfsConfig`]).
//! 2. **A CSV table source** ([`CsvSource`]): `read_csv('https://…/x.csv')`,
//!    `read_csv('s3://bucket/x.csv')`, `read_csv('local.csv')`, and DuckDB's
//!    replacement scan (`FROM 'https://…/x.csv'`). It reuses the engine's CSV
//!    parser ([`pinkdb::csv`]) and infers the same schema as `.import`, so
//!    the two paths agree by construction. A `.csv` path is claimed only by
//!    this source; an `https://`/`s3://` URL must *also* end in `.csv`, so
//!    another format's remote path (a future `read_parquet('https://…')`) is
//!    never swallowed.
//!
//! # Transport API (what wave 72 and other table sources call)
//!
//! ```no_run
//! # fn main() -> Result<(), pinkdb_httpfs::Error> {
//! let file = pinkdb_httpfs::open("s3://bucket/path/data.parquet")?;
//! let len = file.size()?.unwrap_or(0);          // HEAD; None when unknown
//! let head = file.fetch_range(0, len.min(4096))?; // Range: bytes=0-…
//! let all = file.fetch()?;                       // whole object
//! # let _ = (head, all);
//! # Ok(())
//! # }
//! ```
//!
//! [`open`] resolves credentials from the environment at open time:
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, plus
//! the lake's existing configuration conventions (`AWS_REGION` or
//! `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL`,
//! `AWS_S3_FORCE_PATH_STYLE`, `PINKDB_S3_RETRIES`, `PINKDB_S3_ALLOW_HTTP`).
//! For `https://`, transient failures (`429`, `500`, `502`, `503`, `504`, and
//! transport errors) are retried with the same exponential backoff + jitter
//! as the S3 client; a GET is safe to repeat.
//!
//! # Error model
//!
//! [`Error`] distinguishes the cases a table source branches on:
//!
//! - [`Error::NotFound`] — the object/URL does not exist (404),
//! - [`Error::Auth`] — 401/403 (S3 `AccessDenied`, `SignatureDoesNotMatch`,
//!   …); the S3 code is in `code`,
//! - [`Error::Status`] — any other non-2xx HTTP status (including
//!   `NoSuchBucket`, `RangeNotSatisfiable`), with the S3 XML code/message
//!   when the endpoint sent one,
//! - [`Error::Transport`] — I/O/TLS/connection failure,
//! - [`Error::Protocol`] — the peer answered something the client refuses to
//!   interpret (an over-size body, a malformed range response, a missing
//!   ETag where one was required),
//! - [`Error::Unsupported`] — the URL scheme is not served by this build
//!   (e.g. `s3://` without the `s3` feature).
//!
//! `context` always names the operation and URL (`"GET https://…"`), so a
//! caller can surface the error unchanged. Configuration failures are a
//! separate [`ConfigError`] (bad bucket/prefix/endpoint vs. missing
//! credentials) because pink-lake maps them to its own `BadLocation`/`Auth`
//! variants.
//!
//! # Honest limits
//!
//! - One object per read: no globs, no directory listing (the S3 client has
//!   the operations; [`Fetcher`] does not expose them).
//! - No compression, no gzip, no `COPY TO s3://` (follow-ups).
//! - `read_csv` is the default DuckDB dialect only (comma, double quote,
//!   header row, UTF-8); the option parser is the extension point wave 76
//!   fills in.
//! - A range read re-issues the request on a transient failure but does not
//!   resume a partial body: the whole requested window arrives or the call
//!   fails.
//! - `https://` does not verify a certificate pin and follows up to 10
//!   redirects; `s3://` does not follow redirects (a wrong region surfaces as
//!   an S3 error, exactly like the lake).

use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

mod fetch;
mod http;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
mod sigv4;
#[cfg(feature = "s3")]
mod time;

mod csv_source;

pub use csv_source::{
    Call, CsvOptions, CsvSchema, CsvSource, Encoding, SOURCE_NAME, UrlImportFetcher, claims_path,
    ensure_registered, ensure_registered_with, parse_call, register, source, source_with,
    url_import_fetcher,
};
#[cfg(feature = "s3")]
pub use fetch::open_s3_with;
pub use fetch::{Fetcher, HttpfsConfig, open, open_with};
pub use http::{ByteRange, HttpClient};
#[cfg(feature = "s3")]
pub use s3::S3Config;

/// Retries after a retryable failure (so up to 4 attempts), shared with
/// pink-lake's default (`DEFAULT_RETRIES`).
pub const DEFAULT_RETRIES: u32 = 3;

/// Whole-request timeout (connect + transfer) for both transports — the
/// lake's pinned 120 s.
pub(crate) const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

/// The largest body this crate reads. One `PutObject` is capped by S3 at
/// 5 GiB (no multipart uploads); remote CSV/parquet objects are expected to
/// fit. A range read requests only its window.
pub(crate) const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// The first retry delay (doubles per retry, plus up to 50 % jitter).
const RETRY_BASE: Duration = Duration::from_millis(100);

/// Exponential backoff for retry `attempt` (1-based): `RETRY_BASE * 2^(n-1)`
/// plus up to 50 % jitter (so racing writers do not retry in lockstep),
/// capped at 5 s.
pub(crate) fn backoff(attempt: u32) -> Duration {
    let base = RETRY_BASE.saturating_mul(1u32 << (attempt - 1).min(6));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter = base.mul_f64(f64::from(nanos % 500) / 1000.0);
    (base + jitter).min(Duration::from_secs(5))
}

/// The crate's result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// A transport failure, before a caller maps it to its own error type.
#[derive(Debug)]
pub enum Error {
    /// I/O, TLS or connection failure.
    Transport { context: String, source: io::Error },
    /// A non-2xx answer that is not auth/not-found.
    Status {
        context: String,
        status: u16,
        code: String,
        message: String,
    },
    /// 401/403; the S3 `Code` when the endpoint sent one.
    Auth {
        context: String,
        code: String,
        message: String,
    },
    /// 404 (or an S3 `Get` that answered not-found).
    NotFound { context: String },
    /// The peer's answer cannot be interpreted safely.
    Protocol { context: String, message: String },
    /// The URL scheme is not served by this build.
    Unsupported { context: String, message: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport { context, source } => {
                write!(f, "{context}: transport error: {source}")
            }
            Error::Status {
                context,
                status,
                code,
                message,
            } => write!(f, "{context}: HTTP {status} {code}: {message}"),
            Error::Auth {
                context,
                code,
                message,
            } => write!(f, "{context}: access denied ({code}): {message}"),
            Error::NotFound { context } => write!(f, "{context}: not found (HTTP 404)"),
            Error::Protocol { context, message } => {
                write!(f, "{context}: {message}")
            }
            Error::Unsupported { context, message } => {
                write!(f, "{context}: {message}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// A configuration failure, split the same way pink-lake reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A bad bucket/prefix/endpoint/region (the lake's `BadLocation`).
    BadLocation { location: String, message: String },
    /// Missing or incomplete credentials (the lake's `Auth`).
    Auth { path: PathBuf, message: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::BadLocation { location, message } => {
                write!(f, "invalid location \"{location}\": {message}")
            }
            ConfigError::Auth { path, message } => {
                write!(f, "authentication for \"{}\": {message}", path.display())
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        match e {
            ConfigError::BadLocation { location, message } => Error::Unsupported {
                context: location,
                message,
            },
            ConfigError::Auth { path, message } => Error::Auth {
                context: path.display().to_string(),
                code: "NoCredentials".to_string(),
                message,
            },
        }
    }
}

#[cfg(test)]
#[path = "test_http.rs"]
mod test_http;

#[cfg(all(test, feature = "s3"))]
#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;
