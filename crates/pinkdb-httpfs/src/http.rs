//! Plain `https://` GET/HEAD with an optional byte range.
//!
//! This is the non-S3 half of [`crate::Fetcher`]: no signing, rustls
//! verification, up to 10 redirects, and the same 120 s whole-request
//! timeout and body cap as the S3 client. Plain `http://` is refused unless
//! [`HttpfsConfig::allow_http`](crate::HttpfsConfig) is set (local test
//! endpoints only).

use std::io;

use crate::{DEFAULT_RETRIES, Error, HTTP_TIMEOUT, HttpfsConfig, MAX_OBJECT_BYTES, backoff};

/// A byte-range request: `Range: bytes={offset}-{offset + len - 1}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    pub fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }
}

/// One HTTP response as the fetcher consumes it.
#[derive(Debug)]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
    pub(crate) content_length: Option<u64>,
    pub(crate) content_range: Option<String>,
}

/// A blocking HTTPS client (one connection pool).
#[derive(Debug)]
pub struct HttpClient {
    agent: ureq::Agent,
    allow_http: bool,
    retries: u32,
    max_object_bytes: u64,
}

impl HttpClient {
    pub fn new(config: &HttpfsConfig) -> Self {
        Self {
            agent: ureq::Agent::config_builder()
                .timeout_global(Some(HTTP_TIMEOUT))
                // 4xx/5xx are data here: 404/403 have meanings.
                .http_status_as_error(false)
                // A CDN/redirecting host is normal for https; 10 matches
                // ureq's default, stated explicitly.
                .max_redirects(10)
                .user_agent(concat!("pinkdb-httpfs/", env!("CARGO_PKG_VERSION")))
                .build()
                .into(),
            allow_http: config.allow_http,
            retries: DEFAULT_RETRIES,
            max_object_bytes: MAX_OBJECT_BYTES,
        }
    }

    /// The URL (`https://…`; `http://…` only with the explicit opt-in).
    fn check_scheme(&self, url: &str) -> Result<(), Error> {
        let lower = url.to_ascii_lowercase();
        if lower.starts_with("https://") {
            return Ok(());
        }
        if lower.starts_with("http://") {
            if self.allow_http {
                return Ok(());
            }
            return Err(Error::Unsupported {
                context: url.to_string(),
                message: "plain http:// sends credentials and data unencrypted; use https:// \
                          (or set PINKDB_HTTPFS_ALLOW_HTTP=1 for a local test endpoint)"
                    .to_string(),
            });
        }
        Err(Error::Unsupported {
            context: url.to_string(),
            message: "the https transport serves https:// URLs only".to_string(),
        })
    }

    /// Whole-object GET.
    pub fn fetch_bytes(&self, url: &str) -> Result<Vec<u8>, Error> {
        self.check_scheme(url)?;
        let response = self.get(url, None)?;
        match response.status {
            200 => Ok(response.body),
            404 => Err(Error::NotFound {
                context: format!("GET {url}"),
            }),
            206 => Err(Error::Protocol {
                context: format!("GET {url}"),
                message: "the server answered 206 to a request without Range".to_string(),
            }),
            other => Err(self.status_error(url, other, &response.body)),
        }
    }

    /// Byte-range GET. A server that ignores `Range` and answers `200` is
    /// sliced locally to the requested window; `416` is an error.
    pub fn fetch_range(&self, url: &str, offset: u64, len: u64) -> Result<Vec<u8>, Error> {
        self.check_scheme(url)?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(len);
        let response = self.get(url, Some(ByteRange::new(offset, len)))?;
        match response.status {
            206 => {
                // A 206 must actually describe the window we asked for.
                if let Some(content_range) = &response.content_range
                    && !content_range.starts_with(&format!("bytes {offset}-"))
                {
                    return Err(Error::Protocol {
                        context: format!("GET {url}"),
                        message: format!(
                            "the server answered Content-Range {content_range:?} for a \
                             request starting at byte {offset}"
                        ),
                    });
                }
                Ok(response.body)
            }
            200 => {
                let start = usize::try_from(offset).map_err(|_| Error::Protocol {
                    context: format!("GET {url}"),
                    message: "range offset does not fit this platform's usize".to_string(),
                })?;
                let stop = usize::try_from(end).map_err(|_| Error::Protocol {
                    context: format!("GET {url}"),
                    message: "range end does not fit this platform's usize".to_string(),
                })?;
                if response.body.len() < stop {
                    return Err(Error::Protocol {
                        context: format!("GET {url}"),
                        message: format!(
                            "the server ignored the Range header and returned {} bytes, \
                             shorter than the requested window",
                            response.body.len()
                        ),
                    });
                }
                Ok(response.body[start..stop].to_vec())
            }
            404 => Err(Error::NotFound {
                context: format!("GET {url}"),
            }),
            other => Err(self.status_error(url, other, &response.body)),
        }
    }

    /// The object size from a HEAD, or `None` when the server omits
    /// `Content-Length`/does not implement HEAD.
    pub fn size(&self, url: &str) -> Result<Option<u64>, Error> {
        self.check_scheme(url)?;
        let response = self.execute("HEAD", url, None)?;
        match response.status {
            200 => Ok(response.content_length),
            404 => Err(Error::NotFound {
                context: format!("HEAD {url}"),
            }),
            // Some static hosts answer 405/501 to HEAD; the size is simply
            // unknown then, not an error.
            405 | 501 => Ok(None),
            other => Err(self.status_error(url, other, &response.body)),
        }
    }

    /// One GET/HEAD with retries (safe to repeat).
    fn execute(
        &self,
        method: &str,
        url: &str,
        range: Option<ByteRange>,
    ) -> Result<HttpResponse, Error> {
        let mut attempt = 0u32;
        loop {
            let outcome = self.request(method, url, range);
            let retryable = match &outcome {
                Ok(response) if matches!(response.status, 429 | 503) => true,
                Ok(response) if matches!(response.status, 500 | 502 | 504) => true,
                Err(Error::Transport { .. }) => true,
                _ => false,
            };
            if !retryable || attempt >= self.retries {
                return outcome;
            }
            attempt += 1;
            std::thread::sleep(backoff(attempt));
        }
    }

    /// One GET with retries.
    fn get(&self, url: &str, range: Option<ByteRange>) -> Result<HttpResponse, Error> {
        self.execute("GET", url, range)
    }

    fn request(
        &self,
        method: &str,
        url: &str,
        range: Option<ByteRange>,
    ) -> Result<HttpResponse, Error> {
        let context = format!("{method} {url}");
        let mut request = ureq::http::Request::builder().method(method).uri(url);
        if let Some(range) = range {
            let end = range.offset.saturating_add(range.len).saturating_sub(1);
            request = request.header("range", format!("bytes={}-{end}", range.offset));
        }
        let mut response = self
            .agent
            .run(request.body(()).map_err(|e| Error::Protocol {
                context: context.clone(),
                message: format!("could not build the request: {e}"),
            })?)
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
                    "the response is {length} bytes, over this client's {}-byte object cap",
                    self.max_object_bytes
                ),
            });
        }
        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let content_range = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = if method == "HEAD" {
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
            body,
            content_length,
            content_range,
        })
    }

    fn status_error(&self, url: &str, status: u16, body: &[u8]) -> Error {
        let message = String::from_utf8_lossy(&body[..body.len().min(512)])
            .trim()
            .to_string();
        if status == 401 || status == 403 {
            let code = if status == 401 {
                "Unauthorized"
            } else {
                "AccessDenied"
            };
            return Error::Auth {
                context: format!("GET {url}"),
                code: code.to_string(),
                message,
            };
        }
        Error::Status {
            context: format!("GET {url}"),
            status,
            code: if status == 416 {
                "RangeNotSatisfiable".to_string()
            } else {
                "HttpStatus".to_string()
            },
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_http::TestHttpServer;

    fn client() -> HttpClient {
        HttpClient::new(&HttpfsConfig { allow_http: true })
    }

    #[test]
    fn whole_get_and_head() {
        let server = TestHttpServer::start();
        server.put("/data.csv", b"a,b\n1,2\n");
        let client = client();
        let url = server.url("/data.csv");
        assert_eq!(client.fetch_bytes(&url).unwrap(), b"a,b\n1,2\n");
        assert_eq!(client.size(&url).unwrap(), Some(8));
        assert_eq!(
            server
                .requests()
                .into_iter()
                .map(|r| r.split(' ').next().unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["GET".to_string(), "HEAD".to_string()]
        );
    }

    #[test]
    fn range_get_is_exact_and_slices_ignored_ranges() {
        let server = TestHttpServer::start();
        server.put("/r.bin", b"0123456789");
        let client = client();
        let url = server.url("/r.bin");
        assert_eq!(client.fetch_range(&url, 2, 4).unwrap(), b"2345");
        assert_eq!(client.fetch_range(&url, 8, 99).unwrap(), b"89");
        assert_eq!(client.fetch_range(&url, 0, 0).unwrap(), b"");
        assert!(
            server.requests().iter().all(|r| r.contains("/r.bin")),
            "{:?}",
            server.requests()
        );
        // A server that ignores Range still yields the requested window.
        server.set_range_enabled(false);
        assert_eq!(client.fetch_range(&url, 3, 3).unwrap(), b"345");
        server.set_range_enabled(true);
        let err = client.fetch_range(&url, 100, 2).unwrap_err();
        assert!(matches!(err, Error::Status { status: 416, .. }), "{err:?}");
    }

    #[test]
    fn errors_map_to_not_found_auth_and_status() {
        let server = TestHttpServer::start();
        let client = client();
        let err = client.fetch_bytes(&server.url("/missing")).unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "{err:?}");
        server.put_status("/denied", 403, b"no");
        let err = client.fetch_bytes(&server.url("/denied")).unwrap_err();
        assert!(matches!(err, Error::Auth { .. }), "{err:?}");
        assert!(err.to_string().contains("AccessDenied"), "{err}");
        server.put_status("/boom", 500, b"later");
        let err = client.fetch_bytes(&server.url("/boom")).unwrap_err();
        assert!(matches!(err, Error::Status { status: 500, .. }), "{err:?}");
    }

    #[test]
    fn plain_http_needs_the_opt_in() {
        let server = TestHttpServer::start();
        let strict = HttpClient::new(&HttpfsConfig::default());
        let err = strict.fetch_bytes(&server.url("/x")).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        assert!(
            err.to_string().contains("PINKDB_HTTPFS_ALLOW_HTTP"),
            "{err}"
        );
        assert!(server.requests().is_empty(), "no request went out");
    }

    #[test]
    fn retries_transient_statuses() {
        let server = TestHttpServer::start();
        server.fail_next("/flaky", 2, 503);
        server.put("/flaky", b"ok");
        assert_eq!(client().fetch_bytes(&server.url("/flaky")).unwrap(), b"ok");
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.contains("/flaky"))
                .count(),
            3
        );
    }
}
