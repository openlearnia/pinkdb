//! The small reader abstraction other table sources consume, plus URL
//! resolution into one of the two transports.

use std::fmt;

use crate::http::HttpClient;
use crate::{Error, Result};

#[cfg(feature = "s3")]
use crate::s3::{Get, S3Client, S3Config};

/// Transport configuration for [`open_with`] and the CSV source.
///
/// The default is the roadmap's "https-only by default": a plain `http://`
/// URL (or S3 endpoint) is refused. The opt-in exists for local test
/// endpoints; the S3 side also honors the lake's `PINKDB_S3_ALLOW_HTTP`.
#[derive(Debug, Clone, Default)]
pub struct HttpfsConfig {
    /// Allow plain `http://` URLs and S3 endpoints (test/local only).
    pub allow_http: bool,
}

impl HttpfsConfig {
    /// Read the process environment: `PINKDB_HTTPFS_ALLOW_HTTP=1` enables
    /// plain `http://` for the https transport. (S3 keeps its own
    /// `PINKDB_S3_ALLOW_HTTP` convention; this flag is OR'd in for tests.)
    pub fn from_env() -> Self {
        let truthy = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .is_some_and(|v| {
                    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
                })
        };
        Self {
            allow_http: truthy("PINKDB_HTTPFS_ALLOW_HTTP"),
        }
    }
}

/// One remote file: size + whole-object and byte-range reads.
///
/// This is the surface a table source (the CSV source here; wave 72's
/// parquet-over-object-storage next) consumes. Implementations hold their
/// own connection pool and resolve credentials when constructed; every call
/// is blocking and safe to repeat.
pub trait Fetcher: fmt::Debug + Send + Sync {
    /// The URL as opened (`https://…` or `s3://bucket/key`).
    fn url(&self) -> &str;

    /// The object's size, when the server reports it (`HEAD`). `None` means
    /// "unknown", not "empty".
    fn size(&self) -> Result<Option<u64>>;

    /// The whole object's bytes.
    fn fetch(&self) -> Result<Vec<u8>>;

    /// `len` bytes starting at `offset` (`Range: bytes=offset-(offset+len-1)`).
    /// An empty request returns an empty vector without a network call.
    fn fetch_range(&self, offset: u64, len: u64) -> Result<Vec<u8>>;
}

/// Open `url` with the default (environment-derived) transport configuration.
pub fn open(url: &str) -> Result<Box<dyn Fetcher>> {
    open_with(url, &HttpfsConfig::from_env())
}

/// Open `url` with explicit configuration.
pub fn open_with(url: &str, config: &HttpfsConfig) -> Result<Box<dyn Fetcher>> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("https://") || lower.starts_with("http://") {
        return Ok(Box::new(HttpFetcher {
            url: url.to_string(),
            client: HttpClient::new(config),
        }));
    }
    if lower.starts_with("s3://") {
        #[cfg(feature = "s3")]
        {
            return open_s3(url, config);
        }
        #[cfg(not(feature = "s3"))]
        {
            return Err(Error::Unsupported {
                context: url.to_string(),
                message: "this build has no s3:// transport (the `s3` feature is off); \
                          use https:// or rebuild pinkdb-httpfs with `s3`"
                    .to_string(),
            });
        }
    }
    Err(Error::Unsupported {
        context: url.to_string(),
        message: "expected an https://, http:// (test opt-in) or s3:// URL".to_string(),
    })
}

#[cfg(feature = "s3")]
fn open_s3(url: &str, config: &HttpfsConfig) -> Result<Box<dyn Fetcher>> {
    let mut s3 = S3Config::from_env(parse_s3_bucket(url)?);
    if config.allow_http {
        s3 = s3.allow_http(true);
    }
    open_s3_with(url, s3)
}

/// Open an `s3://` URL with an explicit [`S3Config`] (the bucket and key are
/// read from the URL — they override any bucket/prefix set on the config;
/// the config supplies region/endpoint/credentials). Useful for embedders
/// and tests that do not want environment lookups.
#[cfg(feature = "s3")]
pub fn open_s3_with(url: &str, mut config: S3Config) -> Result<Box<dyn Fetcher>> {
    config.bucket = parse_s3_bucket(url)?.to_string();
    // The key is the whole object key; a prefix would be lake semantics.
    config.prefix.clear();
    let key = parse_s3_key(url)?.to_string();
    let client = S3Client::new(config)?;
    Ok(Box::new(S3Fetcher {
        url: url.to_string(),
        key,
        client,
    }))
}

#[cfg(feature = "s3")]
fn parse_s3_bucket(url: &str) -> Result<&str> {
    let rest = &url["s3://".len()..];
    let Some((bucket, _)) = rest.split_once('/') else {
        return Err(Error::Unsupported {
            context: url.to_string(),
            message: "expected s3://bucket/key".to_string(),
        });
    };
    Ok(bucket)
}

#[cfg(feature = "s3")]
fn parse_s3_key(url: &str) -> Result<&str> {
    let rest = &url["s3://".len()..];
    let Some((_, key)) = rest.split_once('/') else {
        return Err(Error::Unsupported {
            context: url.to_string(),
            message: "expected s3://bucket/key".to_string(),
        });
    };
    if key.is_empty() {
        return Err(Error::Unsupported {
            context: url.to_string(),
            message: "expected s3://bucket/key (an object key is required; this transport \
                      does not list prefixes)"
                .to_string(),
        });
    }
    Ok(key)
}

#[derive(Debug)]
struct HttpFetcher {
    url: String,
    client: HttpClient,
}

impl Fetcher for HttpFetcher {
    fn url(&self) -> &str {
        &self.url
    }

    fn size(&self) -> Result<Option<u64>> {
        self.client.size(&self.url)
    }

    fn fetch(&self) -> Result<Vec<u8>> {
        self.client.fetch_bytes(&self.url)
    }

    fn fetch_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.client.fetch_range(&self.url, offset, len)
    }
}

#[cfg(feature = "s3")]
#[derive(Debug)]
struct S3Fetcher {
    url: String,
    key: String,
    client: S3Client,
}

#[cfg(feature = "s3")]
impl Fetcher for S3Fetcher {
    fn url(&self) -> &str {
        &self.url
    }

    fn size(&self) -> Result<Option<u64>> {
        Ok(self.client.stat_object(&self.key)?.map(|meta| meta.size))
    }

    fn fetch(&self) -> Result<Vec<u8>> {
        match self.client.get_object(&self.key)? {
            Get::Found { bytes, .. } => Ok(bytes),
            Get::NotFound => Err(Error::NotFound {
                context: format!("GET {}", self.url),
            }),
        }
    }

    fn fetch_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        match self.client.get_object_range(&self.key, offset, len)? {
            Get::Found { bytes, .. } => Ok(bytes),
            Get::NotFound => Err(Error::NotFound {
                context: format!("GET {}", self.url),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_http::TestHttpServer;

    #[test]
    fn unsupported_schemes_are_clear() {
        for url in ["ftp://host/x.csv", "file:///x.csv", "not-a-url"] {
            let err = open_with(url, &HttpfsConfig::default()).unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{url}: {err:?}");
        }
        #[cfg(not(feature = "s3"))]
        {
            let err = open_with("s3://bucket/x.csv", &HttpfsConfig::default()).unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
            assert!(err.to_string().contains("s3"), "{err}");
        }
    }

    #[test]
    fn https_fetcher_implements_the_trait() {
        let server = TestHttpServer::start();
        server.put("/f.csv", b"x,y\n1,2\n");
        let config = HttpfsConfig { allow_http: true };
        let f = open_with(&server.url("/f.csv"), &config).unwrap();
        assert_eq!(f.url(), server.url("/f.csv"));
        assert_eq!(f.size().unwrap(), Some(8));
        assert_eq!(f.fetch().unwrap(), b"x,y\n1,2\n");
        assert_eq!(f.fetch_range(2, 3).unwrap(), b"y\n1");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_fetcher_reads_whole_objects_and_ranges() {
        let mock = crate::mock_s3::MockS3::start();
        mock.put_object("a/b.csv", b"col\n1\n2\n");
        let config = S3Config::new("bucket")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .with_credentials(mock.access_key_id(), "mock-secret");
        let url = "s3://bucket/a/b.csv".to_string();
        let f = open_s3_with(&url, config).unwrap();
        assert_eq!(f.url(), url);
        assert_eq!(f.size().unwrap(), Some(8));
        assert_eq!(f.fetch().unwrap(), b"col\n1\n2\n");
        assert_eq!(f.fetch_range(4, 3).unwrap(), b"1\n2");
        assert!(mock.verified_requests() >= 3);
    }
}
