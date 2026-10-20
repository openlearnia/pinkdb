//! TCP server: one std thread per connection, one pinkdb `Connection` per
//! client (so `USE` sessions are isolated per TCP client), zero async.
//!
//! ```no_run
//! use pinkdb::Database;
//! use pinkdb_server::Server;
//!
//! let server = Server::bind("127.0.0.1:0", Database::new())?;
//! let addr = server.local_addr();
//! let handle = server.spawn()?;          // accept loop on a background thread
//! // ... connect clients to `addr` ...
//! handle.join();                         // stop accepting and join
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Robustness
//!
//! - Connection threads run with a 16 MiB stack (the default 2 MiB is too
//!   close to the engine's worst-case recursive paths); the engine's depth
//!   guards reject adversarially deep statements before they can recurse.
//! - Accept errors (fd exhaustion, aborted connections, ...) are transient:
//!   the loop logs, backs off, and keeps serving.
//! - At most [`DEFAULT_MAX_CONNECTIONS`] connections are served at once;
//!   over the cap the server sends an `Error` frame and closes. The refusal
//!   is written on the accept loop with a short timeout (no thread per
//!   refusal, and a non-reading client is dropped).
//! - Each statement runs under [`DEFAULT_STATEMENT_TIMEOUT_MS`], a
//!   [`DEFAULT_MAX_RESULT_ROWS`] row cap and a [`DEFAULT_MAX_RESULT_BYTES`]
//!   byte cap (all configurable through [`Server`]'s builder methods; the
//!   embedded engine stays unlimited by default). A timed-out statement
//!   returns an error, releasing the store lock normally; a timed-out
//!   `UPDATE`/`DELETE` fails before its write-back, so no partial state is
//!   committed.
//! - A panic while executing one statement is caught, reported as an `Error`
//!   frame, and does not kill the server or the connection.
//! - Sockets carry an idle timeout ([`DEFAULT_IDLE_TIMEOUT`], configurable
//!   via [`Server::idle_timeout`]): a client that sends nothing (or reads
//!   nothing) for that long is disconnected, so stalled peers cannot pin the
//!   connection cap. Frame bodies are buffered as they arrive, never sized
//!   from the untrusted length prefix.
//! - SQL `LOAD EXTENSION` is refused unless the operator opts in with
//!   [`Server::allow_load_extension`]: letting a remote client `dlopen` a
//!   host library is remote code execution (and, since wave 48, the SQL
//!   statement also requires the `ADMIN` privilege on an authenticated
//!   session). [`Server::extension_root`] additionally confines the paths a
//!   client may name to one directory.
//! - **Authentication (wave 48).** The store's user catalog decides: while
//!   it has **no users**, a connection is trusted (embedded semantics) —
//!   the pre-wave-48 behavior, warned about loudly at startup. As soon as
//!   one user exists, every statement request of a connection without a
//!   successful [`Request::Login`](crate::protocol::Request::Login) is
//!   refused with an `Error` frame; after the login, each statement runs
//!   under that user's grants (the engine's per-statement check). The gate
//!   is re-evaluated between the statements of a multi-statement batch and
//!   at prepared-statement execution against the *current* catalog, so a
//!   batch that creates the first user cannot keep running unauthenticated.
//!   `Login` itself is answered with `LoggedIn` or the same
//!   `authentication failed: …` error for an unknown user and a wrong
//!   password. `Ping` never needs authentication. Login is resource-bounded
//!   (H-6): the Argon2id derivation runs without the store lock, unknown
//!   users still pay a dummy derivation (M-2), at most
//!   [`DEFAULT_MAX_CONCURRENT_LOGINS`] verifications run at once, a
//!   connection closes after [`DEFAULT_LOGIN_MAX_FAILURES`] failures with an
//!   increasing delay, and a bounded per-IP limiter refuses blocked
//!   addresses before any hashing.
//! - With the crate feature `tls`, [`Server::bind_tls`] serves the same
//!   protocol over rustls-encrypted TCP. The handshake runs on the
//!   connection's own thread (under the same idle timeout), so a failed or
//!   stalled handshake logs and drops that connection while the server keeps
//!   serving. A plaintext client on a TLS listener (or vice versa) fails its
//!   own connection and nothing else. TLS encrypts the transport only: it
//!   identifies the server, not the client (login is the client identity),
//!   and the connection cap, per-connection session isolation and idle
//!   timeout are unchanged.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pinkdb::{Connection, DataType, Database, ExecLimits, QueryResult, Value};

use crate::protocol::{self, DoneKind, PROTOCOL_VERSION, ProtocolError, Request, Response};

/// How often the nonblocking accept loop checks the shutdown flag.
const ACCEPT_POLL: Duration = Duration::from_millis(5);

/// First retry delay after a failed `accept()`; doubles up to the max.
const ACCEPT_ERROR_MIN_BACKOFF: Duration = Duration::from_millis(10);

/// Longest retry delay after a failed `accept()`.
const ACCEPT_ERROR_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Stack size for connection threads. The engine's AST-depth guard keeps
/// accepted statements far below this, but the default 2 MiB thread stack is
/// too close to the worst debug-build recursion depths for comfort. The async
/// transport applies the same size to its runtime's worker and blocking-pool
/// threads (engine calls run on those).
pub(crate) const CONNECTION_STACK_SIZE: usize = 16 * 1024 * 1024;

/// Concurrent connections served at once (fd/thread pressure bound).
pub const DEFAULT_MAX_CONNECTIONS: usize = 128;

/// Default idle timeout for client sockets (reads and writes).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Default per-statement wall-clock limit on the server (M-6). The embedded
/// engine has no limit; the server always sets one so a single statement
/// cannot pin a connection thread (and, for a write, the store's write lock)
/// forever. `--statement-timeout-ms` overrides it.
pub const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 30_000;

/// Default cap on rows materialized by one statement on the server. The
/// join-builder output (including a `CROSS JOIN`) and the final result set
/// are each capped at this value. The embedded default is unbounded;
/// `--max-result-rows` overrides it.
pub const DEFAULT_MAX_RESULT_ROWS: usize = 10_000_000;

/// Default cap on estimated bytes materialized by one statement on the
/// server (256 MiB); `--max-result-bytes` overrides it.
pub const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024 * 1024;

/// Default global cap on Argon2id verifications in flight at once. Each
/// verification allocates the work-factor's memory (19 MiB at the policy
/// floor, at most 76 MiB at the ceiling), so four bounds the transient
/// memory the connection cap can otherwise multiply to gigabytes. Excess
/// login requests get a clear "too many concurrent logins" error instead of
/// queueing; `--max-concurrent-logins` overrides it.
pub const DEFAULT_MAX_CONCURRENT_LOGINS: usize = 4;

/// Failed logins a single connection may make before it is closed (H-6).
/// Each failure is followed by an increasing delay (see
/// [`LoginThrottle::failure_delay`]).
pub const DEFAULT_LOGIN_MAX_FAILURES: u32 = 5;

/// Per-IP failed-login window / block policy. A client IP that accumulates
/// this many failures within [`LOGIN_IP_WINDOW`] is refused (without running
/// Argon2id) until [`LOGIN_IP_BLOCK`] expires.
pub(crate) const LOGIN_IP_MAX_FAILURES: u32 = 20;
pub(crate) const LOGIN_IP_WINDOW: Duration = Duration::from_secs(60);
pub(crate) const LOGIN_IP_BLOCK: Duration = Duration::from_secs(60);

/// Bound on the per-IP limiter's map. At most this many addresses are
/// remembered; expired entries are pruned first, then the oldest-observed
/// entry is evicted, so the map (and its memory) is bounded under a spoofed
/// or very large client population.
pub(crate) const LOGIN_IP_MAX_ENTRIES: usize = 4096;

/// Bound on a refusal write on the accept loop (M-11): a client that will
/// not read its error frame must not stall the accept loop for long.
pub(crate) const REJECT_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

/// Connection-local prepared statements a client may pin (shared by the
/// blocking and the `async-server` transports so the cap cannot drift).
pub(crate) const MAX_PREPARED: usize = 1024;

/// The per-server login controls shared by both transports: the global
/// in-flight-verification cap, the per-IP failure limiter, and the limits
/// each connection applies.
#[derive(Clone)]
pub(crate) struct ConnectionPolicy {
    pub(crate) limits: ExecLimits,
    pub(crate) gate: Arc<LoginGate>,
    pub(crate) ips: Arc<LoginIpLimiter>,
    pub(crate) peer_ip: IpAddr,
}

/// A non-queueing cap on concurrent password verifications.
pub(crate) struct LoginGate {
    active: AtomicUsize,
    max: usize,
}

impl LoginGate {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max: max.max(1),
        }
    }

    /// Try to reserve one verification slot; `None` when all are busy (the
    /// caller must report "too many concurrent logins", not wait).
    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<LoginPermit> {
        let mut current = self.active.load(Ordering::Acquire);
        loop {
            if current >= self.max {
                return None;
            }
            match self.active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(LoginPermit {
                        gate: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

/// Releases a [`LoginGate`] slot when the verification (or its panic unwind)
/// ends.
pub(crate) struct LoginPermit {
    gate: Arc<LoginGate>,
}

impl Drop for LoginPermit {
    fn drop(&mut self) {
        self.gate.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Per-connection failed-login throttle (H-6): after
/// [`DEFAULT_LOGIN_MAX_FAILURES`] failures the connection is closed, and each
/// failure sleeps for an exponentially increasing delay (25 ms, 50 ms, …,
/// capped at 500 ms) before the next request is read.
pub(crate) struct LoginThrottle {
    failures: u32,
}

impl LoginThrottle {
    pub(crate) fn new() -> Self {
        Self { failures: 0 }
    }

    pub(crate) fn closed(&self) -> bool {
        self.failures >= DEFAULT_LOGIN_MAX_FAILURES
    }

    pub(crate) fn reset(&mut self) {
        self.failures = 0;
    }

    /// Record one failed login; returns the delay to apply before reading the
    /// next request.
    pub(crate) fn failure_delay(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let exponent = self.failures.saturating_sub(1).min(4);
        Duration::from_millis(25 * (1u64 << exponent)).min(Duration::from_millis(500))
    }
}

struct IpEntry {
    window_start: Instant,
    last_seen: Instant,
    failures: u32,
    blocked_until: Option<Instant>,
}

/// Bounded per-IP failed-login limiter with expiry. Entries live for
/// [`LOGIN_IP_WINDOW`] after their last failure (or until a block expires),
/// after which they are pruned; the map holds at most
/// [`LOGIN_IP_MAX_ENTRIES`], evicting the oldest-observed entry if a full map
/// has no expired entries. Documented eviction bounds memory under churn but
/// means a very large rotating address set can dilute the limit (see the
/// residual-risk note in the remediation report).
pub(crate) struct LoginIpLimiter {
    entries: Mutex<HashMap<IpAddr, IpEntry>>,
    capacity: usize,
    window: Duration,
    block: Duration,
    max_failures: u32,
}

impl LoginIpLimiter {
    pub(crate) fn new() -> Self {
        Self::with_limits(
            LOGIN_IP_MAX_ENTRIES,
            LOGIN_IP_WINDOW,
            LOGIN_IP_BLOCK,
            LOGIN_IP_MAX_FAILURES,
        )
    }

    pub(crate) fn with_limits(
        capacity: usize,
        window: Duration,
        block: Duration,
        max_failures: u32,
    ) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: capacity.max(1),
            window,
            block,
            max_failures: max_failures.max(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<IpAddr, IpEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// How long this address is blocked for, if it is currently blocked.
    /// Expired blocks are dropped here (expiry is lazy, on access).
    pub(crate) fn retry_after(&self, ip: IpAddr) -> Option<Duration> {
        let now = Instant::now();
        let mut entries = self.lock();
        let entry = entries.get(&ip)?;
        match entry.blocked_until {
            Some(until) if until > now => Some(until - now),
            Some(_) => {
                entries.remove(&ip);
                None
            }
            None => {
                if now.duration_since(entry.window_start) > self.window {
                    entries.remove(&ip);
                }
                None
            }
        }
    }

    /// Record one failed login for `ip`; starts a block once the window's
    /// failure budget is exhausted.
    pub(crate) fn record_failure(&self, ip: IpAddr) {
        let now = Instant::now();
        let mut entries = self.lock();
        if entries.len() >= self.capacity {
            // Expiry first: a blocked entry is live, a recent window is live.
            entries.retain(|_, entry| {
                let blocked = entry.blocked_until.is_some_and(|until| until > now);
                let recent = now.duration_since(entry.window_start) <= self.window;
                blocked || recent
            });
            // Still full: evict the entry observed longest ago (bounded map;
            // see the type docs).
            if entries.len() >= self.capacity
                && let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_seen)
                    .map(|(ip, _)| *ip)
            {
                entries.remove(&oldest);
            }
        }
        let entry = entries.entry(ip).or_insert_with(|| IpEntry {
            window_start: now,
            last_seen: now,
            failures: 0,
            blocked_until: None,
        });
        if now.duration_since(entry.window_start) > self.window {
            entry.window_start = now;
            entry.failures = 0;
        }
        entry.last_seen = now;
        entry.failures = entry.failures.saturating_add(1);
        if entry.failures >= self.max_failures {
            entry.blocked_until = Some(now + self.block);
            entry.failures = 0;
        }
    }

    /// A successful login clears this address's failure history.
    pub(crate) fn record_success(&self, ip: IpAddr) {
        self.lock().remove(&ip);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }
}

/// Soft cap on the encoded size of one streamed `Rows` frame; batches are
/// flushed before they grow past it (the hard protocol cap is 64 MiB).
const ROWS_FRAME_BUDGET: usize = 8 * 1024 * 1024;

/// Test-only injection point (hidden from docs): when set, every connection
/// thread calls the hook with the statement SQL *inside* the panic guard,
/// before executing it. A panic in the hook is caught exactly like an engine
/// panic; the panic-recovery integration test uses it to exercise that path.
#[doc(hidden)]
pub static STATEMENT_HOOK: Mutex<Option<fn(&str)>> = Mutex::new(None);

/// Run the hidden statement hook, if one is installed.
pub(crate) fn run_statement_hook(sql: &str) {
    let hook = *STATEMENT_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(hook) = hook {
        hook(sql);
    }
}

/// Decrements the active-connection counter when the connection task ends,
/// including on unwind.
pub(crate) struct ActiveGuard(pub(crate) Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Encoded size of one row (tag byte + payload per value), used to batch
/// `Rows` frames without materializing them first.
fn row_encoded_len(row: &[Value]) -> usize {
    row.iter()
        .map(|value| {
            1 + match value {
                Value::Null => 0,
                Value::Boolean(_) => 1,
                Value::Integer(_) | Value::Date(_) => 4,
                Value::Bigint(_) | Value::Double(_) | Value::Timestamp(_) => 8,
                Value::Decimal(_, _) => 16 + 1,
                Value::Interval { .. } => 16,
                Value::Varchar(s) => 4 + s.len(),
            }
        })
        .sum()
}

/// Destination for encoded response frames.
///
/// The blocking transport writes every frame straight to the socket (`impl
/// FrameSink for W: Write` below); the `async-server` transport encodes each
/// frame into the bounded queue its connection task drains to the socket.
/// Both share [`send_result`]'s streaming and batching logic, so the wire
/// bytes cannot drift between the two transports.
pub(crate) trait FrameSink {
    /// Encode and deliver one response frame. An error ends the connection:
    /// the frame could not be delivered.
    fn send_frame(&mut self, response: &Response) -> Result<(), ProtocolError>;
}

impl<W: Write> FrameSink for W {
    fn send_frame(&mut self, response: &Response) -> Result<(), ProtocolError> {
        protocol::write_response(self, response)
    }
}

/// A bound, not-yet-running pinkdb server.
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    db: Database,
    shutdown: Arc<AtomicBool>,
    max_connections: usize,
    active: Arc<AtomicUsize>,
    idle_timeout: Option<Duration>,
    /// Per-statement engine limits applied to every accepted connection
    /// (M-6). The server always has safe defaults; the embedded engine does
    /// not.
    exec_limits: ExecLimits,
    /// Global cap on concurrent Argon2id login verifications (H-6).
    login_gate: Arc<LoginGate>,
    /// Bounded per-IP failed-login limiter (H-6).
    login_ips: Arc<LoginIpLimiter>,
    /// TLS identity when the server was bound via `bind_tls` (crate feature
    /// `tls`); `None` = plaintext TCP.
    #[cfg(feature = "tls")]
    tls: Option<Arc<rustls::ServerConfig>>,
}

impl Server {
    /// Bind `addr` and serve `db` with [`DEFAULT_MAX_CONNECTIONS`] concurrent
    /// connections. The listener is bound immediately (so `local_addr` is
    /// exact, including an ephemeral `:0` port).
    pub fn bind(addr: impl ToSocketAddrs, db: Database) -> std::io::Result<Server> {
        Self::bind_with_max_connections(addr, db, DEFAULT_MAX_CONNECTIONS)
    }

    /// Bind `addr` and serve `db` with an explicit concurrent-connection cap.
    /// Over the cap, `run` sends a best-effort `Error` frame and closes the
    /// accepted socket, so fd and thread pressure stay bounded.
    ///
    /// Binding turns SQL `LOAD EXTENSION` off on `db` (every clone of it,
    /// including embedded connections sharing the handle); see
    /// [`Server::allow_load_extension`].
    pub fn bind_with_max_connections(
        addr: impl ToSocketAddrs,
        db: Database,
        max_connections: usize,
    ) -> std::io::Result<Server> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        db.set_allow_sql_load_extension(false);
        Ok(Server {
            listener,
            local_addr,
            db,
            shutdown: Arc::new(AtomicBool::new(false)),
            max_connections,
            active: Arc::new(AtomicUsize::new(0)),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            // The server always bounds per-statement work; an embedder that
            // wants no limit uses the engine's `Connection` directly.
            exec_limits: ExecLimits {
                statement_timeout: Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS)),
                max_result_rows: Some(DEFAULT_MAX_RESULT_ROWS),
                max_result_bytes: Some(DEFAULT_MAX_RESULT_BYTES),
            },
            login_gate: Arc::new(LoginGate::new(DEFAULT_MAX_CONCURRENT_LOGINS)),
            login_ips: Arc::new(LoginIpLimiter::new()),
            #[cfg(feature = "tls")]
            tls: None,
        })
    }

    /// Bind `addr` and serve `db` over TLS (crate feature `tls`) with a PEM
    /// certificate chain and private key. See
    /// [`Server::bind_tls_with_max_connections`] for the certificate rules.
    #[cfg(feature = "tls")]
    pub fn bind_tls(
        addr: impl ToSocketAddrs,
        db: Database,
        cert_path: impl AsRef<std::path::Path>,
        key_path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Server> {
        Self::bind_tls_with_max_connections(addr, db, DEFAULT_MAX_CONNECTIONS, cert_path, key_path)
    }

    /// Bind `addr` and serve `db` over TLS with an explicit concurrent-
    /// connection cap. Everything but the transport behaves exactly like
    /// [`Server::bind_with_max_connections`].
    ///
    /// The certificate and key are loaded and validated *before* the listener
    /// binds: a missing or unreadable file, PEM that does not parse, an empty
    /// file, or a key that does not match the leaf certificate refuses
    /// startup with a message naming the file and the reason. Client
    /// certificates are not requested; TLS here is transport encryption and
    /// server identity, not client authentication.
    #[cfg(feature = "tls")]
    pub fn bind_tls_with_max_connections(
        addr: impl ToSocketAddrs,
        db: Database,
        max_connections: usize,
        cert_path: impl AsRef<std::path::Path>,
        key_path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Server> {
        let tls = crate::tls::server_config_from_pem(cert_path, key_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        let mut server = Self::bind_with_max_connections(addr, db, max_connections)?;
        server.tls = Some(tls);
        Ok(server)
    }

    /// Let remote clients run SQL `LOAD EXTENSION` (default: refused). Only
    /// for trusted networks: the statement makes the server process load and
    /// run a native library from its own filesystem.
    ///
    /// Combine it with [`Server::extension_root`] to confine the paths a
    /// remote client may name to one directory.
    pub fn allow_load_extension(self, allow: bool) -> Self {
        self.db.set_allow_sql_load_extension(allow);
        self
    }

    /// Confine every extension load of the served database to `root` (the
    /// directory that holds the extension libraries). `None` removes the
    /// confinement; the loader's own path rules (a directory, no `..`, a
    /// regular file, not a symlink) always apply.
    pub fn extension_root(self, root: Option<std::path::PathBuf>) -> Self {
        self.db.set_extension_root(root);
        self
    }

    /// Per-socket idle timeout for reads and writes (`None` = wait forever).
    /// A client that stays silent between requests, or stops reading results,
    /// for longer than this is disconnected. A zero duration is treated as
    /// `None` (the OS rejects a zero socket timeout).
    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout.filter(|t| !t.is_zero());
        self
    }

    /// Per-statement wall-clock deadline (M-6). Checked at chunk boundaries
    /// inside the executor, so a huge scan/join/`UPDATE` fails instead of
    /// pinning a connection (and the write lock) indefinitely. A zero
    /// duration makes every statement time out immediately; prefer the
    /// default unless the flag is explicitly set.
    pub fn statement_timeout(mut self, timeout: Duration) -> Self {
        self.exec_limits.statement_timeout = Some(timeout);
        self
    }

    /// Cap on rows materialized by one statement (join output + final
    /// result). A zero cap makes every row-producing statement fail.
    pub fn max_result_rows(mut self, rows: usize) -> Self {
        self.exec_limits.max_result_rows = Some(rows);
        self
    }

    /// Cap on estimated bytes materialized by one statement.
    pub fn max_result_bytes(mut self, bytes: usize) -> Self {
        self.exec_limits.max_result_bytes = Some(bytes);
        self
    }

    /// Global cap on Argon2id login verifications in flight (H-6). Excess
    /// requests get a "too many concurrent logins" error immediately.
    pub fn max_concurrent_logins(mut self, max: usize) -> Self {
        self.login_gate = Arc::new(LoginGate::new(max));
        self
    }

    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept connections until [`ServerHandle::shutdown`] (or the
    /// corresponding flag) is set; blocks the calling thread.
    ///
    /// Each accepted connection gets its own thread and its own
    /// [`pinkdb::Connection`]. Failed `accept()` calls are transient (fd
    /// exhaustion, an aborted handshake, ...): they are logged and retried
    /// with a short backoff instead of stopping the server.
    pub fn run(&self) -> std::io::Result<()> {
        warn_auth_mode("", &self.db);
        let mut backoff = ACCEPT_ERROR_MIN_BACKOFF;
        while !self.shutdown.load(Ordering::SeqCst) {
            match self.listener.accept() {
                Ok((mut stream, peer)) => {
                    backoff = ACCEPT_ERROR_MIN_BACKOFF;
                    if self.shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    if self.active.load(Ordering::SeqCst) >= self.max_connections {
                        eprintln!(
                            "pinkdb-server: connection limit ({}) reached; rejecting {peer}",
                            self.max_connections
                        );
                        #[cfg(feature = "tls")]
                        if self.tls.is_some() {
                            // A TLS listener has no plaintext Error frame to
                            // send before the handshake, and completing one
                            // just to report "busy" would spend the capacity
                            // the cap is defending. Dropping is the honest
                            // signal.
                            continue;
                        }
                        // M-11: write the refusal on the accept loop itself
                        // with a short write timeout. One refusal may cost a
                        // bounded 100 ms; it never creates an OS thread, and
                        // a client that will not read is simply dropped.
                        reject_over_limit(&mut stream, self.max_connections);
                        continue;
                    }
                    let guard = ActiveGuard(Arc::clone(&self.active));
                    self.active.fetch_add(1, Ordering::SeqCst);
                    let db = self.db.clone();
                    let idle_timeout = self.idle_timeout;
                    let policy = ConnectionPolicy {
                        limits: self.exec_limits,
                        gate: Arc::clone(&self.login_gate),
                        ips: Arc::clone(&self.login_ips),
                        peer_ip: peer.ip(),
                    };
                    // A clone kept only for the spawn-failure path: if the OS
                    // cannot create the connection thread, report it to the
                    // client (bounded write) and close, instead of silently
                    // dropping an accepted connection.
                    let spawn_error_stream = stream.try_clone().ok();
                    #[cfg(feature = "tls")]
                    let tls = self.tls.clone();
                    if let Err(e) = thread::Builder::new()
                        .name("pinkdb-server-conn".to_string())
                        .stack_size(CONNECTION_STACK_SIZE)
                        .spawn(move || {
                            let _guard = guard;
                            #[cfg(feature = "tls")]
                            if let Some(config) = tls {
                                serve_tls_connection(
                                    stream,
                                    peer,
                                    db,
                                    idle_timeout,
                                    policy,
                                    config,
                                );
                                return;
                            }
                            serve_connection(stream, db, idle_timeout, policy);
                        })
                    {
                        // On a spawn failure the closure (and with it the
                        // guard and the original stream) is dropped, so the
                        // active count stays exact and the connection closes.
                        eprintln!("pinkdb-server: cannot spawn connection thread: {e}");
                        if let Some(mut stream) = spawn_error_stream {
                            reject_with(
                                &mut stream,
                                &format!("server is out of connection capacity ({e}); close"),
                            );
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(ACCEPT_POLL);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    // EMFILE/ENFILE/ECONNABORTED/ENOBUFS/... are conditions
                    // that clear; exiting here would let one fd spike kill
                    // the whole server. Back off so a persistent condition
                    // does not spin.
                    eprintln!("pinkdb-server: accept failed ({e}); retrying");
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(ACCEPT_ERROR_MAX_BACKOFF);
                }
            }
        }
        Ok(())
    }

    /// Run the accept loop on a background thread; the returned handle stops
    /// it (on [`ServerHandle::shutdown`] or drop).
    pub fn spawn(self) -> std::io::Result<ServerHandle> {
        let shutdown = Arc::clone(&self.shutdown);
        let addr = self.local_addr;
        let thread = thread::Builder::new()
            .name("pinkdb-server".to_string())
            .spawn(move || {
                if let Err(e) = self.run() {
                    eprintln!("pinkdb-server: accept loop stopped: {e}");
                }
            })?;
        Ok(ServerHandle {
            addr,
            shutdown,
            thread: Some(thread),
        })
    }
}

/// Write one best-effort `Error` frame with a bounded write timeout, then
/// close. Used by the accept loop for a refusal (M-11) and by the
/// spawn-failure path, never from a thread spawned per connection.
pub(crate) fn reject_with(stream: &mut TcpStream, message: &str) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_write_timeout(Some(REJECT_WRITE_TIMEOUT));
    let _ = protocol::write_response(
        stream,
        &Response::Error {
            message: message.to_string(),
        },
    );
}

/// Best-effort `Error` frame for a connection refused by the cap, then close.
/// Runs **on the accept loop** with a short write timeout: a client that will
/// not read is dropped after at most [`REJECT_WRITE_TIMEOUT`], and no OS
/// thread is created per refusal (M-11).
fn reject_over_limit(stream: &mut TcpStream, max: usize) {
    reject_with(
        stream,
        &format!("connection limit reached (max {max}); try again later"),
    );
}

/// A running server's handle (see [`Server::spawn`]).
pub struct ServerHandle {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ServerHandle {
    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Ask the accept loop to stop. In-flight connection threads finish on
    /// their own (their clients' EOF or disconnect ends them).
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Shut down and join *the accept loop thread only*.
    ///
    /// `join` is deliberately shallow: it waits until the accept loop has
    /// stopped accepting, but in-flight connection threads are detached and
    /// finish when their clients disconnect (or when the process exits).
    /// They are bounded by the connection cap, so this cannot leak threads
    /// without bound. [`Drop`] behaves the same way.
    pub fn join(mut self) {
        self.shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Serve one plaintext client: configure the accepted socket, then run the
/// protocol.
fn serve_connection(
    stream: TcpStream,
    db: Database,
    idle_timeout: Option<Duration>,
    policy: ConnectionPolicy,
) {
    if !configure_socket(&stream, idle_timeout) {
        return;
    }
    serve_client(stream, db, policy);
}

/// Apply the connection-required socket options to a freshly accepted socket.
/// Returns `false` when a *fatal* option failed (the caller drops the socket).
fn configure_socket(stream: &TcpStream, idle_timeout: Option<Duration>) -> bool {
    // BSD-derived kernels (macOS) inherit the listener's O_NONBLOCK on
    // accepted sockets; the connection thread wants blocking IO.
    if let Err(e) = stream.set_nonblocking(false) {
        eprintln!("pinkdb-server: cannot switch an accepted socket to blocking mode: {e}");
    }
    if let Err(e) = stream
        .set_read_timeout(idle_timeout)
        .and_then(|()| stream.set_write_timeout(idle_timeout))
    {
        eprintln!("pinkdb-server: cannot set the idle timeout on an accepted socket: {e}");
        return false;
    }
    if let Err(e) = stream.set_nodelay(true) {
        eprintln!("pinkdb-server: cannot set TCP_NODELAY on an accepted socket: {e}");
    }
    true
}

/// Serve one TLS client (crate feature `tls`): complete the handshake on this
/// connection's own thread, then run the protocol over the encrypted stream.
///
/// The socket's idle timeout is applied before the handshake, so a peer that
/// connects and never speaks TLS is dropped like any other idle client. A
/// handshake failure (wrong protocol, untrusted client expectations, bad
/// record, timeout) is logged and drops only this connection.
#[cfg(feature = "tls")]
fn serve_tls_connection(
    stream: TcpStream,
    peer: SocketAddr,
    db: Database,
    idle_timeout: Option<Duration>,
    policy: ConnectionPolicy,
    config: Arc<rustls::ServerConfig>,
) {
    if !configure_socket(&stream, idle_timeout) {
        return;
    }
    let session = match rustls::ServerConnection::new(config) {
        Ok(session) => session,
        Err(e) => {
            eprintln!("pinkdb-server: cannot start a TLS session for {peer}: {e}");
            return;
        }
    };
    let mut tls = rustls::StreamOwned::new(session, stream);
    if let Err(e) = tls.conn.complete_io(&mut tls.sock) {
        eprintln!("pinkdb-server: TLS handshake failed for {peer}: {e}");
        return;
    }
    if tls.conn.is_handshaking() {
        // `complete_io` returns Ok when no further progress is possible
        // without more data: the peer stalled past the idle timeout.
        eprintln!("pinkdb-server: TLS handshake stalled for {peer}; dropping the connection");
        return;
    }
    serve_client(tls, db, policy);
}

/// Serve the pinkdb framed protocol on an established byte stream (plain TCP
/// or TLS): `Hello`, then request/response until EOF.
///
/// The connection owns a fresh `db.connect()`: its `USE` session, open
/// transaction and session state are private to this client.
///
/// Statement execution runs inside `catch_unwind`: an engine panic becomes an
/// `Error` frame and a stderr log instead of killing the connection (or, for
/// a panicking connection thread, the whole process).
///
/// Reads go through the `BufReader`; writes go through the *same* stream via
/// `BufReader::get_mut` (the transport is a single owned object, which is
/// what rustls' `StreamOwned` needs). Buffered read-ahead is untouched by
/// writes, and `write_frame` flushes, so a write failure is reported at the
/// frame that hit it.
fn serve_client<S: Read + Write>(stream: S, db: Database, policy: ConnectionPolicy) {
    let mut reader = BufReader::new(stream);
    let mut conn = db.connect();
    // Network session: the engine re-checks the store's user catalog before
    // every statement (so a batch cannot continue after its own CREATE USER).
    conn.require_login(true);
    conn.set_exec_limits(policy.limits);
    let mut throttle = LoginThrottle::new();

    // Handshake: the first request must be a version-matched Hello.
    let protocol_version = match protocol::read_request(&mut reader) {
        Ok(Request::Hello { protocol_version }) => protocol_version,
        Ok(other) => {
            let _ = send_error(
                reader.get_mut(),
                &format!("expected a Hello handshake first, got {}", other.kind()),
            );
            return;
        }
        Err(e) => {
            let _ = send_error(reader.get_mut(), &e.to_string());
            return;
        }
    };
    if protocol_version != PROTOCOL_VERSION {
        let _ = send_error(
            reader.get_mut(),
            &format!(
                "protocol version mismatch: server speaks v{PROTOCOL_VERSION}, \
                 client asked for v{protocol_version}"
            ),
        );
        return;
    }
    let hello_ok = Response::HelloOk {
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
    };
    if protocol::write_response(reader.get_mut(), &hello_ok).is_err() {
        return;
    }

    // v5: connection-local prepared statements (`?` placeholders). Handles are
    // handed out in order and die with this connection; the cap bounds the
    // per-connection metadata a client can pin.
    let mut prepared: std::collections::HashMap<u64, pinkdb::PreparedStatement> =
        std::collections::HashMap::new();
    let mut next_handle: u64 = 1;

    loop {
        match protocol::read_request(&mut reader) {
            // v9 (wave 48): verify credentials and adopt the principal. The
            // connection stays usable after a failure until the per-
            // connection failure budget is spent (H-6): five failures close
            // it, each after an increasing delay.
            Ok(Request::Login { user, password }) => {
                // Per-IP limit first: a blocked address costs no Argon2id.
                if let Some(retry) = policy.ips.retry_after(policy.peer_ip) {
                    if send_error(
                        reader.get_mut(),
                        &format!(
                            "too many failed login attempts from this address; retry in {}s",
                            retry.as_secs().max(1)
                        ),
                    )
                    .is_err()
                    {
                        return;
                    }
                    continue;
                }
                // Global in-flight cap: do not queue verifications (each one
                // allocates the work factor's memory).
                let Some(permit) = policy.gate.try_acquire() else {
                    if send_error(
                        reader.get_mut(),
                        "too many concurrent logins; retry shortly",
                    )
                    .is_err()
                    {
                        return;
                    }
                    continue;
                };
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    login_reply(reader.get_mut(), &mut conn, &user, &password)
                }));
                drop(permit);
                match outcome {
                    Ok(Ok(LoginAttempt::Succeeded)) => {
                        throttle.reset();
                        policy.ips.record_success(policy.peer_ip);
                    }
                    Ok(Ok(LoginAttempt::Failed)) => {
                        let delay = throttle.failure_delay();
                        policy.ips.record_failure(policy.peer_ip);
                        std::thread::sleep(delay);
                        if throttle.closed() {
                            // The auth-failed frame was already sent; EOF is
                            // the close signal (the next request fails).
                            return;
                        }
                    }
                    Ok(Err(_)) => return,
                    Err(payload) => {
                        let message = panic_message(payload.as_ref());
                        eprintln!("pinkdb-server: login panicked: {message}");
                        if send_error(
                            reader.get_mut(),
                            &format!("internal error: login panicked ({message})"),
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            Ok(Request::Execute { sql }) => {
                if auth_required(&db, &conn) {
                    if send_error(reader.get_mut(), AUTH_REQUIRED).is_err() {
                        return;
                    }
                    continue;
                }
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_statement_hook(&sql);
                    conn.execute(&sql)
                }));
                match outcome {
                    Ok(Ok(results)) => {
                        let statements = match u32::try_from(results.len()) {
                            Ok(n) => n,
                            Err(_) => {
                                let _ = send_error(
                                    reader.get_mut(),
                                    "too many statements in one Execute batch",
                                );
                                return;
                            }
                        };
                        for result in &results {
                            if let Err(e) = send_result(reader.get_mut(), result) {
                                // A streaming failure (e.g. a single value
                                // larger than the frame cap) is reported
                                // before closing; an IO failure just ends the
                                // thread.
                                if !matches!(e, ProtocolError::Io(_)) {
                                    let _ = send_error(reader.get_mut(), &e.to_string());
                                }
                                return;
                            }
                        }
                        // v2 batch terminator: the client reads result groups
                        // until this frame, so it never parses SQL to count
                        // statements.
                        if protocol::write_response(
                            reader.get_mut(),
                            &Response::BatchDone { statements },
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                    // A SQL error is a normal response: report it and keep the
                    // connection open (mirrors the embedded REPL). No
                    // BatchDone follows: the Error frame ends the batch.
                    Ok(Err(e)) => {
                        if send_error(reader.get_mut(), &e.to_string()).is_err() {
                            return;
                        }
                    }
                    // An engine panic is caught and reported; the connection
                    // (and every other client) stays alive. The store may need
                    // poisoned-lock recovery, which `db.rs` performs.
                    Err(payload) => {
                        let message = panic_message(payload.as_ref());
                        eprintln!("pinkdb-server: statement panicked: {message}");
                        if send_error(
                            reader.get_mut(),
                            &format!("internal error: statement panicked ({message})"),
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            Ok(Request::Ping) => {
                if protocol::write_response(reader.get_mut(), &Response::Pong).is_err() {
                    return;
                }
            }
            // v5: parse one statement with `?` placeholders into a
            // connection-local prepared statement. Pure parsing: nothing
            // executes and nothing is logged (but a connection that has not
            // logged in may not use the parser either).
            Ok(Request::Prepare { sql }) => {
                if auth_required(&db, &conn) {
                    if send_error(reader.get_mut(), AUTH_REQUIRED).is_err() {
                        return;
                    }
                    continue;
                }
                if prepared.len() >= MAX_PREPARED {
                    if send_error(
                        reader.get_mut(),
                        "too many prepared statements (per-connection limit 1024)",
                    )
                    .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| conn.prepare(&sql)));
                match outcome {
                    Ok(Ok(stmt)) => {
                        let Some(next) = next_handle.checked_add(1) else {
                            let _ = protocol::write_response(
                                reader.get_mut(),
                                &Response::Error {
                                    message: "prepared-statement handle space exhausted"
                                        .to_string(),
                                },
                            );
                            continue;
                        };
                        let handle = next_handle;
                        next_handle = next;
                        let parameter_count = stmt.parameter_count() as u32;
                        prepared.insert(handle, stmt);
                        if protocol::write_response(
                            reader.get_mut(),
                            &Response::Prepared {
                                handle,
                                parameter_count,
                            },
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                    // A parse error (or an unsupported shape) is a normal
                    // response: report it and keep the connection open.
                    Ok(Err(e)) => {
                        if send_error(reader.get_mut(), &e.to_string()).is_err() {
                            return;
                        }
                    }
                    Err(payload) => {
                        let message = panic_message(payload.as_ref());
                        eprintln!("pinkdb-server: prepare panicked: {message}");
                        if send_error(
                            reader.get_mut(),
                            &format!("internal error: prepare panicked ({message})"),
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            // v5: bind values and execute the prepared statement. Framing is
            // identical to a one-statement Execute: one result group, then
            // the BatchDone terminator. The hook sees the substituted SQL,
            // so statement-count instrumentation matches the embedded path.
            Ok(Request::ExecutePrepared { handle, values }) => {
                if auth_required(&db, &conn) {
                    if send_error(reader.get_mut(), AUTH_REQUIRED).is_err() {
                        return;
                    }
                    continue;
                }
                let Some(stmt) = prepared.get(&handle) else {
                    if send_error(
                        reader.get_mut(),
                        &format!("unknown prepared statement handle {handle}"),
                    )
                    .is_err()
                    {
                        return;
                    }
                    continue;
                };
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let bound = conn.bind(stmt, &values)?;
                    run_statement_hook(bound.sql());
                    conn.execute_bound(&bound)
                }));
                match outcome {
                    Ok(Ok(result)) => {
                        // A bound statement is exactly one statement: one
                        // result group, then the batch terminator.
                        if let Err(e) = send_result(reader.get_mut(), &result) {
                            if !matches!(e, ProtocolError::Io(_)) {
                                let _ = send_error(reader.get_mut(), &e.to_string());
                            }
                            return;
                        }
                        if protocol::write_response(
                            reader.get_mut(),
                            &Response::BatchDone { statements: 1 },
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                    // Bind/type/execution errors are normal responses; the
                    // connection stays open (mirrors Execute).
                    Ok(Err(e)) => {
                        if send_error(reader.get_mut(), &e.to_string()).is_err() {
                            return;
                        }
                    }
                    Err(payload) => {
                        let message = panic_message(payload.as_ref());
                        eprintln!("pinkdb-server: prepared execute panicked: {message}");
                        if send_error(
                            reader.get_mut(),
                            &format!("internal error: statement panicked ({message})"),
                        )
                        .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            Ok(Request::Hello { .. }) => {
                if send_error(reader.get_mut(), "duplicate Hello handshake").is_err() {
                    return;
                }
            }
            // EOF or an IO error: the client is gone; end this thread.
            Err(ProtocolError::Io(_)) => return,
            // A malformed or oversized frame cannot be resynchronized: report
            // it if possible, then close the connection.
            Err(e) => {
                let _ = send_error(reader.get_mut(), &e.to_string());
                return;
            }
        }
    }
}

/// Human-readable text from a caught panic payload.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Stream one statement's result: a `ResultSet` frame, chunked `Rows`
/// frames, and one `Done` frame.
///
/// Rows are batched by count *and* encoded size, so a result set of large
/// strings still streams in frames well under [`protocol::MAX_FRAME_SIZE`]
/// (a single value larger than the frame cap is unrepresentable and fails
/// with a clear error instead of silently dropping the connection).
///
/// Shared by both transports: the blocking server passes a frame sink that
/// writes to the socket, the async server one that queues frames to the
/// connection task (see [`FrameSink`]).
pub(crate) fn send_result<S: FrameSink + ?Sized>(
    w: &mut S,
    result: &QueryResult,
) -> Result<(), ProtocolError> {
    match result {
        QueryResult::Selected { names, types, rows } => {
            stream_selected(w, names, types, rows)?;
            done(w, DoneKind::Selected, 0, "")
        }
        QueryResult::CreatedProcedure { name } => done(w, DoneKind::Created, 0, name),
        QueryResult::DroppedProcedure { name } => done(w, DoneKind::DroppedTable, 0, name),
        QueryResult::CreatedMacro { name } => done(w, DoneKind::Created, 0, name),
        QueryResult::DroppedMacro { name } => done(w, DoneKind::DroppedTable, 0, name),
        QueryResult::Called {
            name,
            statements,
            result,
        } => {
            // The call's own result rides the frames before `Done(Called)` —
            // only a result set can (see `DoneKind::Called`).
            if let Some(QueryResult::Selected { names, types, rows }) = result.as_deref() {
                stream_selected(w, names, types, rows)?;
            }
            done(w, DoneKind::Called, *statements as u64, name)
        }
        QueryResult::Created => done(w, DoneKind::Created, 0, ""),
        QueryResult::Inserted { count } => done(w, DoneKind::Inserted, *count as u64, ""),
        QueryResult::Updated { count } => done(w, DoneKind::Updated, *count as u64, ""),
        QueryResult::Deleted { count } => done(w, DoneKind::Deleted, *count as u64, ""),
        QueryResult::Began => done(w, DoneKind::Began, 0, ""),
        QueryResult::Committed => done(w, DoneKind::Committed, 0, ""),
        QueryResult::RolledBack => done(w, DoneKind::RolledBack, 0, ""),
        QueryResult::CreatedView { name } => done(w, DoneKind::CreatedView, 0, name),
        QueryResult::DroppedTable { name } => done(w, DoneKind::DroppedTable, 0, name),
        QueryResult::DroppedView { name } => done(w, DoneKind::DroppedView, 0, name),
        QueryResult::CreatedDatabase { name } => done(w, DoneKind::CreatedDatabase, 0, name),
        QueryResult::DroppedDatabase { name } => done(w, DoneKind::DroppedDatabase, 0, name),
        QueryResult::Used { db } => done(w, DoneKind::Used, 0, db),
        QueryResult::LoadedExtension { name } => done(w, DoneKind::LoadedExtension, 0, name),
        QueryResult::Checkpointed => done(w, DoneKind::Checkpointed, 0, ""),
        QueryResult::AlteredTable { name } => done(w, DoneKind::AlteredTable, 0, name),
        QueryResult::CreatedIndex { name } => done(w, DoneKind::Created, 0, name),
        QueryResult::DroppedIndex { name } => done(w, DoneKind::DroppedTable, 0, name),
        QueryResult::CreatedMaterializedView { name } => done(w, DoneKind::Created, 0, name),
        QueryResult::RefreshedMaterializedView { name } => done(w, DoneKind::Created, 0, name),
        QueryResult::RefreshedMaterializedViews { .. } => done(w, DoneKind::Created, 0, ""),
        QueryResult::DroppedMaterializedView { name } => done(w, DoneKind::DroppedTable, 0, name),
        // v9 (wave 48): the RBAC statement outcomes carry their own kinds so
        // the remote client reconstructs exactly the embedded result.
        QueryResult::CreatedUser { name } => done(w, DoneKind::CreatedUser, 0, name),
        QueryResult::AlteredUser { name } => done(w, DoneKind::AlteredUser, 0, name),
        QueryResult::DroppedUser { name } => done(w, DoneKind::DroppedUser, 0, name),
        QueryResult::CreatedRole { name } => done(w, DoneKind::CreatedRole, 0, name),
        QueryResult::DroppedRole { name } => done(w, DoneKind::DroppedRole, 0, name),
        QueryResult::Granted { description } => done(w, DoneKind::Granted, 0, description),
        QueryResult::Revoked { description } => done(w, DoneKind::Revoked, 0, description),
    }
}

/// The refusal every statement request gets on a connection that has not
/// logged in while the store has users (wave 48). Shared by both transports,
/// so the two cannot drift.
pub(crate) const AUTH_REQUIRED: &str =
    "authentication required: this server has users defined; send a Login request first";

/// Does this request need a successful login? (`false` while the store has no
/// users: the documented empty-catalog policy, and `false` after a login.)
pub(crate) fn auth_required(db: &Database, conn: &Connection) -> bool {
    conn.principal().is_none() && db.has_users()
}

/// The result of one engine login attempt, reported to the transport so it
/// can apply the per-connection/per-IP backoff without re-parsing frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoginAttempt {
    Succeeded,
    Failed,
}

/// Answer one `Login` request: verify the credentials, adopt the principal,
/// and reply `LoggedIn` (or the identical `authentication failed` `Error`).
/// Shared by both transports. The caller holds a [`LoginPermit`] for the
/// duration and applies the failure delay/close policy from the returned
/// [`LoginAttempt`].
pub(crate) fn login_reply<S: FrameSink + ?Sized>(
    sink: &mut S,
    conn: &mut Connection,
    user: &str,
    password: &str,
) -> Result<LoginAttempt, ProtocolError> {
    match conn.login(user, password) {
        Ok(name) => {
            sink.send_frame(&Response::LoggedIn { user: name })?;
            Ok(LoginAttempt::Succeeded)
        }
        Err(e) => {
            send_error(sink, &e.to_string())?;
            Ok(LoginAttempt::Failed)
        }
    }
}

/// Announce the store's authentication mode at startup (wave 48): a server
/// with no users is fully trusted, and that must not be a silent surprise.
pub(crate) fn warn_auth_mode(which: &str, db: &Database) {
    if db.has_users() {
        eprintln!(
            "pinkdb-server{which}: authentication is on — connections must log in              (CREATE USER / GRANT ADMIN manage the users)"
        );
    } else {
        eprintln!(
            "pinkdb-server{which}: WARNING: no users defined — every connection is trusted              and can run any SQL. Create a user (CREATE USER <name> PASSWORD '<password>'              [ADMIN via GRANT ADMIN <name>]) to require login."
        );
    }
}

/// Stream one result set's `ResultSet`/`Rows` frames (batched by count and
/// encoded size, so a result set of large strings still streams well under
/// [`protocol::MAX_FRAME_SIZE`]; a single value larger than the frame cap is
/// unrepresentable and fails with a clear error instead of silently dropping
/// the connection). The caller sends the matching `Done`.
fn stream_selected<S: FrameSink + ?Sized>(
    w: &mut S,
    names: &[String],
    types: &[DataType],
    rows: &[Vec<Value>],
) -> Result<(), ProtocolError> {
    let result_set = Response::ResultSet {
        names: names.to_vec(),
        types: types.to_vec(),
    };
    w.send_frame(&result_set)?;
    let mut batch: Vec<Vec<Value>> = Vec::new();
    let mut batch_bytes = 0usize;
    for row in rows {
        let row_bytes = row_encoded_len(row);
        if !batch.is_empty()
            && (batch.len() >= protocol::ROWS_PER_FRAME
                || batch_bytes + row_bytes > ROWS_FRAME_BUDGET)
        {
            let frame = Response::Rows {
                rows: std::mem::take(&mut batch),
            };
            w.send_frame(&frame)?;
            batch_bytes = 0;
        }
        batch.push(row.clone());
        batch_bytes += row_bytes;
    }
    if !batch.is_empty() {
        w.send_frame(&Response::Rows { rows: batch })?;
    }
    Ok(())
}

fn done<S: FrameSink + ?Sized>(
    w: &mut S,
    kind: DoneKind,
    count: u64,
    name: &str,
) -> Result<(), ProtocolError> {
    w.send_frame(&Response::Done {
        kind,
        count,
        name: name.to_string(),
    })
}

/// Deliver one `Error` frame through a frame sink.
pub(crate) fn send_error<S: FrameSink + ?Sized>(
    w: &mut S,
    message: &str,
) -> Result<(), ProtocolError> {
    w.send_frame(&Response::Error {
        message: message.to_string(),
    })
}

#[cfg(test)]
mod login_limit_tests {
    use super::*;

    #[test]
    fn login_gate_refuses_instead_of_queueing() {
        let gate = Arc::new(LoginGate::new(1));
        let first = gate.try_acquire().expect("the first verification fits");
        assert!(
            gate.try_acquire().is_none(),
            "the cap must refuse, not queue"
        );
        assert_eq!(gate.in_flight(), 1);
        drop(first);
        assert_eq!(gate.in_flight(), 0);
        assert!(gate.try_acquire().is_some());
    }

    #[test]
    fn login_throttle_delays_increase_and_close_after_five() {
        let mut throttle = LoginThrottle::new();
        let delays: Vec<Duration> = (0..DEFAULT_LOGIN_MAX_FAILURES)
            .map(|_| throttle.failure_delay())
            .collect();
        assert!(delays_increase(&delays), "{delays:?}");
        assert!(throttle.closed());
        throttle.reset();
        assert!(!throttle.closed());
    }

    fn delays_increase(delays: &[Duration]) -> bool {
        delays.windows(2).all(|pair| pair[0] <= pair[1]) && delays.first() < delays.last()
    }

    #[test]
    fn ip_limiter_blocks_expires_and_stays_bounded() {
        let expiry =
            LoginIpLimiter::with_limits(4, Duration::from_secs(60), Duration::from_millis(40), 2);
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        expiry.record_failure(a);
        expiry.record_failure(a);
        assert!(
            expiry.retry_after(a).is_some(),
            "two failures must start the block"
        );
        // A block expires lazily on access, and a success clears the address.
        std::thread::sleep(Duration::from_millis(60));
        assert!(expiry.retry_after(a).is_none(), "the block must expire");
        expiry.record_failure(b);
        expiry.record_success(b);
        assert!(expiry.retry_after(b).is_none());

        // Capacity 2 with a one-failure block: a third address evicts the
        // oldest entry, so the map never grows past its documented bound.
        let bounded =
            LoginIpLimiter::with_limits(2, Duration::from_secs(60), Duration::from_secs(60), 1);
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let c: IpAddr = "10.0.0.3".parse().unwrap();
        bounded.record_failure(a);
        bounded.record_failure(b);
        bounded.record_failure(c);
        assert_eq!(bounded.len(), 2, "the per-IP map must stay bounded");
        assert!(
            bounded.retry_after(a).is_none(),
            "the oldest-observed entry is the one evicted"
        );
    }
}
