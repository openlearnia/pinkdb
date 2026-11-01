//! The listener, its runtime thread, and the options the adapter runs with.

use std::fmt;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use pinkdb::Database;
use tokio::sync::watch;

use crate::app::{Shared, routes};
use crate::lease::{DEFAULT_DRAIN_TIMEOUT, LocalWarehouse, MIN_WRITE_LEASE_TTL, parse_go_duration};

/// DuckDB's default port, as QuackLab's service binds it.
pub const DEFAULT_BIND: &str = "127.0.0.1:1294";
/// The database every request runs in (`DUCKLAKE_ALIAS`).
pub const DEFAULT_WAREHOUSE: &str = "warehouse";
/// `GRAIN_SQL_RESULT_ROW_LIMIT` when it is unset or invalid.
pub const DEFAULT_ROW_LIMIT: usize = 5_000;
/// The ceiling `GRAIN_SQL_RESULT_ROW_LIMIT` is clamped to.
pub const MAX_ROW_LIMIT: usize = 100_000;

/// The adapter's options: `pinkdb-server --quacklab-bind` plus the
/// environment the DuckDB service reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuacklabOptions {
    /// `DUCKDB_SQL_TOKEN`: the bearer token `/sql` and `/recover` require.
    pub sql_token: String,
    /// `GRAIN_CR_SERVERLESS`: the platform authenticates the caller, so no
    /// token is asked for.
    pub iam_trust: bool,
    /// `DUCKDB_READ_ONLY`: only queries, EXPLAIN, DESCRIBE, SHOW and USE.
    pub read_only: bool,
    /// `GRAIN_SQL_RESULT_ROW_LIMIT`, already clamped by [`parse_row_limit`].
    pub row_limit: usize,
    /// `DUCKLAKE_ALIAS`: the database every request is positioned in.
    pub warehouse: String,
    /// `GRAIN_DUCKDB_SANDBOX`: refuse native-code loading from SQL (see
    /// [`crate::sandbox`] for what pinkdb can and cannot isolate).
    pub sandbox: bool,
    /// `GRAIN_WRITE_LEASE_DRAIN_TIMEOUT`: how long an acquire waits for
    /// in-flight SQL, in Go duration syntax; 5m when unset or invalid.
    pub lease_drain_timeout: Duration,
    /// The write-lease TTL clamp's floor: the Go service's 30s. A test lowers
    /// it so a lease can expire without a 30 second sleep.
    #[doc(hidden)]
    pub lease_ttl_floor: Duration,
}

impl Default for QuacklabOptions {
    fn default() -> Self {
        Self {
            sql_token: String::new(),
            iam_trust: false,
            read_only: false,
            row_limit: DEFAULT_ROW_LIMIT,
            warehouse: DEFAULT_WAREHOUSE.to_string(),
            sandbox: false,
            lease_drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            lease_ttl_floor: MIN_WRITE_LEASE_TTL,
        }
    }
}

impl QuacklabOptions {
    /// The options the DuckDB service's environment describes. The DuckDB-only
    /// knobs are the caller's to ignore: `pinkdb-server` logs them.
    pub fn from_env() -> Self {
        let token = std::env::var("DUCKDB_SQL_TOKEN").unwrap_or_default();
        let serverless = std::env::var("GRAIN_CR_SERVERLESS").ok();
        let read_only = std::env::var("DUCKDB_READ_ONLY").ok();
        let limit = std::env::var("GRAIN_SQL_RESULT_ROW_LIMIT").ok();
        let alias = std::env::var("DUCKLAKE_ALIAS").ok();
        let sandbox = std::env::var("GRAIN_DUCKDB_SANDBOX").ok();
        let drain = std::env::var("GRAIN_WRITE_LEASE_DRAIN_TIMEOUT").ok();
        Self {
            sql_token: token.trim().to_string(),
            iam_trust: truthy(serverless.as_deref()),
            read_only: truthy(read_only.as_deref()),
            row_limit: parse_row_limit(limit.as_deref()),
            warehouse: match alias.as_deref().map(str::trim) {
                Some(name) if !name.is_empty() => name.to_string(),
                _ => DEFAULT_WAREHOUSE.to_string(),
            },
            sandbox: truthy(sandbox.as_deref()),
            lease_drain_timeout: parse_go_duration(drain.as_deref().unwrap_or_default())
                .unwrap_or(DEFAULT_DRAIN_TIMEOUT),
            lease_ttl_floor: MIN_WRITE_LEASE_TTL,
        }
    }

    /// Go's `requireSQLToken`: IAM trust allows everything, so does an empty
    /// token; otherwise `Authorization: Bearer <token>`, compared in constant
    /// time.
    pub fn authorized(&self, authorization: Option<&str>) -> bool {
        if self.iam_trust {
            return true;
        }
        let expected = self.sql_token.trim();
        if expected.is_empty() {
            return true;
        }
        let Some(authorization) = authorization else {
            return false;
        };
        match authorization.trim().strip_prefix("Bearer ") {
            Some(given) => constant_time_eq(given.as_bytes(), expected.as_bytes()),
            None => false,
        }
    }
}

/// `GRAIN_SQL_RESULT_ROW_LIMIT`: DuckDB's rule — unset or empty is the
/// default, anything below 1 or unparseable is the default, and a larger value
/// is clamped to [`MAX_ROW_LIMIT`].
pub fn parse_row_limit(raw: Option<&str>) -> usize {
    match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
        Some(raw) => match raw.parse::<i64>() {
            Ok(limit) if limit >= 1 => (limit as u64).min(MAX_ROW_LIMIT as u64) as usize,
            _ => DEFAULT_ROW_LIMIT,
        },
        None => DEFAULT_ROW_LIMIT,
    }
}

/// `1|true|yes|on`, case-insensitive, as the DuckDB service reads its flags.
fn truthy(raw: Option<&str>) -> bool {
    let Some(raw) = raw else {
        return false;
    };
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Length-independent comparison: the fold touches every byte of both sides.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// A bound adapter that has not started serving yet.
pub struct QuacklabServer {
    listener: TcpListener,
    addr: SocketAddr,
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
}

impl QuacklabServer {
    /// Bind `addr` and ensure the warehouse database exists. Binding (not
    /// serving) means the caller can learn the port before the thread starts.
    pub fn bind(
        addr: impl ToSocketAddrs,
        db: Database,
        options: QuacklabOptions,
    ) -> Result<Self, QuacklabError> {
        let addr = addr
            .to_socket_addrs()
            .map_err(QuacklabError::Bind)?
            .next()
            .ok_or_else(|| {
                QuacklabError::Bind(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no socket address",
                ))
            })?;
        let listener = TcpListener::bind(addr).map_err(QuacklabError::Bind)?;
        listener
            .set_nonblocking(true)
            .map_err(QuacklabError::Bind)?;
        let addr = listener.local_addr().map_err(QuacklabError::Bind)?;
        let shared = Arc::new(Shared::new(db, options, Arc::new(LocalWarehouse)));
        shared
            .ensure_warehouse()
            .map_err(QuacklabError::Warehouse)?;
        let (stop, _) = watch::channel(false);
        Ok(Self {
            listener,
            addr,
            shared,
            stop,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn options(&self) -> &QuacklabOptions {
        &self.shared.options
    }

    /// Serve on a thread with its own runtime, the way the console does.
    pub fn spawn(self) -> std::io::Result<QuacklabHandle> {
        let QuacklabServer {
            listener,
            addr,
            shared,
            stop,
        } = self;
        let mut stopped = stop.subscribe();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("pinkdb-quacklab")
            .build()?;
        let thread = std::thread::Builder::new()
            .name("pinkdb-quacklab".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(listener) {
                        Ok(listener) => listener,
                        Err(e) => {
                            eprintln!("pinkdb-server: cannot listen for QuackLab: {e}");
                            return;
                        }
                    };
                    let served = axum::serve(
                        listener,
                        routes(shared).into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .with_graceful_shutdown(async move {
                        while !*stopped.borrow() {
                            if stopped.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await;
                    if let Err(e) = served {
                        eprintln!("pinkdb-server: the QuackLab adapter stopped: {e}");
                    }
                });
            })?;
        Ok(QuacklabHandle {
            addr,
            stop,
            thread: Some(thread),
        })
    }
}

/// A running adapter. Dropping it asks the server to stop, like the console's.
pub struct QuacklabHandle {
    addr: SocketAddr,
    stop: watch::Sender<bool>,
    thread: Option<JoinHandle<()>>,
}

impl QuacklabHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn shutdown(&self) {
        let _ = self.stop.send(true);
    }

    /// Stop accepting, then wait up to `grace` for in-flight requests to
    /// finish. Returns false when one is still running; the caller checkpoints
    /// anyway, because a platform kill would lose the checkpoint entirely.
    pub fn drain(&mut self, grace: Duration) -> bool {
        self.shutdown();
        let Some(thread) = self.thread.take() else {
            return true;
        };
        // A JoinHandle has no timeout: a watcher thread reports the join.
        let (finished, done) = std::sync::mpsc::channel();
        let watcher = std::thread::Builder::new()
            .name("pinkdb-quacklab-drain".to_string())
            .spawn(move || {
                let _ = thread.join();
                let _ = finished.send(());
            });
        watcher.is_ok() && done.recv_timeout(grace).is_ok()
    }

    pub fn join(&mut self) -> std::thread::Result<()> {
        match self.thread.take() {
            Some(thread) => thread.join(),
            None => Ok(()),
        }
    }
}

impl Drop for QuacklabHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Why the adapter did not start.
#[derive(Debug)]
pub enum QuacklabError {
    Bind(std::io::Error),
    Warehouse(String),
}

impl fmt::Display for QuacklabError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(e) => write!(f, "{e}"),
            Self::Warehouse(message) => write!(f, "the warehouse database: {message}"),
        }
    }
}

impl std::error::Error for QuacklabError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_row_limit_defaults_and_clamps() {
        assert_eq!(parse_row_limit(None), 5_000);
        assert_eq!(parse_row_limit(Some("")), 5_000);
        assert_eq!(parse_row_limit(Some(" 100 ")), 100);
        assert_eq!(parse_row_limit(Some("0")), 5_000);
        assert_eq!(parse_row_limit(Some("-1")), 5_000);
        assert_eq!(parse_row_limit(Some("abc")), 5_000);
        assert_eq!(parse_row_limit(Some("999999")), 100_000);
        assert_eq!(parse_row_limit(Some("100000")), 100_000);
    }

    #[test]
    fn draining_stops_the_listener() {
        let options = QuacklabOptions {
            sql_token: "tok".to_string(),
            ..QuacklabOptions::default()
        };
        let mut handle = QuacklabServer::bind("127.0.0.1:0", Database::new(), options)
            .expect("bind")
            .spawn()
            .expect("spawn");
        let addr = handle.local_addr();
        assert!(
            handle.drain(Duration::from_secs(5)),
            "nothing was in flight"
        );
        // The listener is closed once the drain returns.
        assert!(
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_err(),
            "a drained adapter still accepts connections"
        );
        assert!(
            handle.drain(Duration::from_secs(5)),
            "a second drain is a no-op"
        );
    }

    #[test]
    fn truthiness_matches_the_service() {
        for raw in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(truthy(Some(raw)), "{raw}");
        }
        for raw in ["", "0", "no", "off", "2"] {
            assert!(!truthy(Some(raw)), "{raw}");
        }
        assert!(!truthy(None));
    }

    #[test]
    fn the_token_gate() {
        let mut options = QuacklabOptions::default();
        assert!(options.authorized(None), "no configured token lets all in");
        options.sql_token = "secret".to_string();
        assert!(!options.authorized(None));
        assert!(!options.authorized(Some("Bearer nope")));
        assert!(!options.authorized(Some("Basic secret")));
        assert!(options.authorized(Some("Bearer secret")));
        assert!(options.authorized(Some("  Bearer secret  ")));
        options.iam_trust = true;
        assert!(options.authorized(None));
        assert!(options.authorized(Some("Bearer nope")));
    }
}
