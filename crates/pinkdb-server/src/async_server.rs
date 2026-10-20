//! Async transport for the pinkdb network server (`async-server` feature).
//!
//! This is wave 47's **documented carve-out** to the workspace's zero-async
//! rule. It is deliberately the smallest slice that makes async real:
//!
//! # Where async lives — the carve-out
//!
//! - **Async is scoped to the I/O boundary of the server crate.** A tokio
//!   listener accepts sockets; one tokio task per connection reads request
//!   frames, applies the idle timeout, and writes response frames to the
//!   socket. Nothing else in the workspace is async: the executor, planner,
//!   binder, storage and WAL are synchronous, and the `Storage` trait's
//!   blocking contract is untouched. There is no async storage layer.
//! - **The engine stays synchronous.** Every `Connection` call runs on a
//!   blocking-pool thread via `spawn_blocking` (with the same 16 MiB stacks
//!   the blocking server gives connection threads, via
//!   [`Runtime`]'s `thread_stack_size`); the async task owns the connection
//!   between calls and moves it into the blocking action. The engine never
//!   holds an async lock and never awaits.
//! - **The protocol does not change.** This module reuses
//!   [`protocol`]'s codec and, through the shared
//!   `server::send_result` encoder, the blocking
//!   server's exact result-streaming/batching logic, so the wire bytes are
//!   identical to the blocking transport (pinned by a byte-parity test).
//! - **The default build gains no async runtime.** `tokio` is an optional
//!   dependency of this crate behind `async-server`; `cargo tree -p
//!   pinkdb-server` without the feature lists no tokio, and the default
//!   `pinkdb-server` binary is byte-for-byte the blocking server. Embedders
//!   that build with the feature but run the default transport pay nothing at
//!   runtime: a runtime is only created by [`AsyncServer::run_blocking`] /
//!   [`AsyncServer::spawn`].
//!
//! # No TLS on this transport
//!
//! This transport is plaintext-only. The binary refuses `--tls-cert`/
//! `--tls-key` when `--transport async` is selected (usage error, exit 2)
//! instead of accepting the flags and silently serving plaintext; TLS remains
//! a property of the blocking [`Server`](crate::Server) transport. Use the
//! default sync transport when transport encryption is required.
//!
//! # Robustness (same bar as the blocking server)
//!
//! - At most [`DEFAULT_MAX_CONNECTIONS`] connections are served at once
//!   (configurable); over the cap the server sends a best-effort `Error`
//!   frame and closes, and connections are admitted again as slots free.
//! - Sockets carry the same idle timeout ([`DEFAULT_IDLE_TIMEOUT`], reads and
//!   writes): a client that sends nothing, or stops reading results, for that
//!   long is disconnected. Frame bodies are buffered as they arrive, never
//!   sized from the untrusted length prefix.
//! - The same per-statement deadline/result caps and login controls as the
//!   blocking transport (`statement_timeout`, `max_result_rows`,
//!   `max_result_bytes`, `max_concurrent_logins`): engine work runs on the
//!   blocking pool under the executor's chunk-boundary deadline, and failed
//!   logins back off per connection and per IP.
//! - A panic while executing one statement is caught (`catch_unwind` on the
//!   blocking thread) and reported as an `Error` frame; the connection and
//!   every other client stay alive.
//! - Handshake errors (missing/duplicate/mismatched `Hello`, malformed
//!   frames) produce the same `Error` frames as the blocking server.
//! - [`AsyncServerHandle::join`] stops accepting and joins the accept loop;
//!   the owned runtime then ends in-flight connection tasks (their clients
//!   see EOF). Unlike the blocking server's detached connection threads,
//!   nothing outlives the handle.
//!
//! # Threading model
//!
//! One tokio multi-thread runtime: worker threads drive sockets and timers,
//! blocking-pool threads run engine calls. Concurrency is bounded by the
//! connection cap (each connection has at most one engine call in flight), so
//! the blocking pool is bounded by the same number — not tokio's default
//! 512-thread pool ceiling.
//!
//! ```no_run
//! # #[cfg(feature = "async-server")]
//! # fn main() -> std::io::Result<()> {
//! use pinkdb::Database;
//! use pinkdb_server::AsyncServer;
//!
//! let server = AsyncServer::bind("127.0.0.1:0", Database::new())?;
//! let addr = server.local_addr();
//! let handle = server.spawn()?;      // accept loop on the owned runtime
//! // ... connect existing pinkdb clients to `addr` ...
//! handle.join();                     // stop accepting and end in-flight tasks
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "async-server"))]
//! # fn main() {}
//! ```

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use pinkdb::{Connection, Database, ExecLimits, PreparedStatement};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;

use crate::protocol::{
    self, FRAME_READ_STEP, MAX_FRAME_SIZE, PROTOCOL_VERSION, ProtocolError, Request, Response,
};
use crate::server::{
    self, AUTH_REQUIRED, ActiveGuard, CONNECTION_STACK_SIZE, ConnectionPolicy,
    DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONCURRENT_LOGINS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS, DEFAULT_STATEMENT_TIMEOUT_MS, FrameSink,
    LoginAttempt, LoginGate, LoginIpLimiter, LoginThrottle, MAX_PREPARED, auth_required,
    panic_message, warn_auth_mode,
};

/// First retry delay after a failed `accept()`; doubles up to the max
/// (mirrors the blocking server).
const ACCEPT_ERROR_MIN_BACKOFF: Duration = Duration::from_millis(10);

/// Longest retry delay after a failed `accept()`.
const ACCEPT_ERROR_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// How often the accept loop checks the shutdown flag (mirrors the blocking
/// server's `ACCEPT_POLL`). `accept()` is cancel-safe, so a connection that
/// races the poll is still served on the next round.
const ACCEPT_POLL: Duration = Duration::from_millis(5);

/// Bound for the best-effort `Error` frame sent to a client refused by the
/// connection cap (mirrors the blocking server's 1 s write timeout).
const REJECT_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

/// Encoded response frames the blocking engine action may run ahead of the
/// socket writer. A full queue blocks the engine task (bounded memory:
/// ~[`FRAME_QUEUE`] frames, each under the 8 MiB streaming budget); a stalled
/// client is then disconnected by the write idle timeout.
const FRAME_QUEUE: usize = 4;

/// A bound, not-yet-running async pinkdb server.
///
/// Same configuration surface and semantics as [`Server`](crate::Server),
/// with a tokio transport underneath: `bind*`, `allow_load_extension`,
/// `idle_timeout`, `local_addr`, then either
/// `AsyncServer::run_blocking` (the binary's entry point) or
/// `AsyncServer::run` on an embedder's runtime.
pub struct AsyncServer {
    listener: std::net::TcpListener,
    local_addr: SocketAddr,
    db: Database,
    shutdown: Arc<AtomicBool>,
    max_connections: usize,
    active: Arc<AtomicUsize>,
    idle_timeout: Option<Duration>,
    exec_limits: ExecLimits,
    login_gate: Arc<LoginGate>,
    login_ips: Arc<LoginIpLimiter>,
}

impl AsyncServer {
    /// Bind `addr` and serve `db` with [`DEFAULT_MAX_CONNECTIONS`] concurrent
    /// connections. The listener is bound immediately (so `local_addr` is
    /// exact, including an ephemeral `:0` port).
    pub fn bind(addr: impl ToSocketAddrs, db: Database) -> io::Result<AsyncServer> {
        Self::bind_with_max_connections(addr, db, DEFAULT_MAX_CONNECTIONS)
    }

    /// Bind `addr` and serve `db` with an explicit concurrent-connection cap.
    /// Over the cap, the accept loop sends a best-effort `Error` frame and
    /// closes the accepted socket, so fd and task pressure stay bounded.
    ///
    /// Binding turns SQL `LOAD EXTENSION` off on `db` (every clone of it,
    /// including embedded connections sharing the handle); see
    /// [`AsyncServer::allow_load_extension`].
    pub fn bind_with_max_connections(
        addr: impl ToSocketAddrs,
        db: Database,
        max_connections: usize,
    ) -> io::Result<AsyncServer> {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        db.set_allow_sql_load_extension(false);
        Ok(AsyncServer {
            listener,
            local_addr,
            db,
            shutdown: Arc::new(AtomicBool::new(false)),
            max_connections,
            active: Arc::new(AtomicUsize::new(0)),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            exec_limits: ExecLimits {
                statement_timeout: Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS)),
                max_result_rows: Some(DEFAULT_MAX_RESULT_ROWS),
                max_result_bytes: Some(DEFAULT_MAX_RESULT_BYTES),
            },
            login_gate: Arc::new(LoginGate::new(DEFAULT_MAX_CONCURRENT_LOGINS)),
            login_ips: Arc::new(LoginIpLimiter::new()),
        })
    }

    /// Let remote clients run SQL `LOAD EXTENSION` (default: refused). Only
    /// for trusted networks: the statement makes the server process load and
    /// run a native library from its own filesystem.
    ///
    /// Combine it with [`AsyncServer::extension_root`] to confine the paths
    /// a remote client may name to one directory.
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
    /// A zero duration is treated as `None`, like the blocking server.
    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout.filter(|t| !t.is_zero());
        self
    }

    /// Per-statement wall-clock deadline (M-6), shared with the blocking
    /// transport's semantics: checked at chunk boundaries inside the
    /// executor.
    pub fn statement_timeout(mut self, timeout: Duration) -> Self {
        self.exec_limits.statement_timeout = Some(timeout);
        self
    }

    /// Cap on rows materialized by one statement (join output + final
    /// result).
    pub fn max_result_rows(mut self, rows: usize) -> Self {
        self.exec_limits.max_result_rows = Some(rows);
        self
    }

    /// Cap on estimated bytes materialized by one statement.
    pub fn max_result_bytes(mut self, bytes: usize) -> Self {
        self.exec_limits.max_result_bytes = Some(bytes);
        self
    }

    /// Global cap on Argon2id login verifications in flight (H-6).
    pub fn max_concurrent_logins(mut self, max: usize) -> Self {
        self.login_gate = Arc::new(LoginGate::new(max));
        self
    }

    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Build the runtime this transport runs on: multi-threaded workers plus
    /// a blocking pool, all with 16 MiB stacks so engine calls get the same
    /// stack the blocking server's connection threads do.
    pub fn runtime() -> io::Result<Runtime> {
        Builder::new_multi_thread()
            .thread_name("pinkdb-server-async")
            .thread_stack_size(CONNECTION_STACK_SIZE)
            .enable_all()
            .build()
    }

    /// Accept connections until `AsyncServerHandle::shutdown` is
    /// requested; runs on the caller's tokio runtime.
    ///
    /// Each accepted connection gets its own task and its own
    /// [`pinkdb::Connection`]. Failed `accept()` calls are logged and retried
    /// with a short backoff instead of stopping the server.
    pub async fn run(&self) -> io::Result<()> {
        warn_auth_mode("(async)", &self.db);
        // The std listener was bound (and set nonblocking) in `bind`; wrap it
        // for the reactor. `from_std` requires an IO-enabled runtime context.
        let listener = TcpListener::from_std(self.listener.try_clone()?)?;
        let mut backoff = ACCEPT_ERROR_MIN_BACKOFF;
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
            let accepted = match tokio::time::timeout(ACCEPT_POLL, listener.accept()).await {
                Ok(accepted) => accepted,
                // No connection this poll: check the shutdown flag again.
                Err(_) => continue,
            };
            match accepted {
                Ok((stream, peer)) => {
                    backoff = ACCEPT_ERROR_MIN_BACKOFF;
                    if self.shutdown.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    if self.active.load(Ordering::SeqCst) >= self.max_connections {
                        eprintln!(
                            "pinkdb-server(async): connection limit ({}) reached; rejecting {peer}",
                            self.max_connections
                        );
                        reject_over_limit(stream, self.max_connections);
                        continue;
                    }
                    // The guard moves into the task, so a panicking or
                    // cancelled connection task still frees its slot.
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
                    tokio::spawn(async move {
                        let _guard = guard;
                        serve_connection(stream, db, idle_timeout, policy).await;
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    // EMFILE/ENFILE/ECONNABORTED/ENOBUFS/... are conditions
                    // that clear; exiting here would let one fd spike kill
                    // the whole server.
                    eprintln!("pinkdb-server(async): accept failed ({e}); retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(ACCEPT_ERROR_MAX_BACKOFF);
                }
            }
        }
    }

    /// Run the accept loop on a freshly built multi-thread runtime, blocking
    /// the calling thread until `AsyncServerHandle::shutdown` — the binary's
    /// entry point.
    pub fn run_blocking(self) -> io::Result<()> {
        let runtime = Self::runtime()?;
        runtime.block_on(self.run())
    }

    /// Run the accept loop on a background task of an owned runtime; the
    /// returned handle stops it (on `AsyncServerHandle::shutdown` or drop).
    ///
    /// The handle owns the runtime, so it must be used from a non-async
    /// thread; embedders inside an async application drive
    /// [`run`](AsyncServer::run) on their own runtime instead.
    pub fn spawn(self) -> io::Result<AsyncServerHandle> {
        let runtime = Self::runtime()?;
        let addr = self.local_addr;
        let shutdown = Arc::clone(&self.shutdown);
        let accept = runtime.spawn(async move {
            if let Err(e) = self.run().await {
                eprintln!("pinkdb-server(async): accept loop stopped: {e}");
            }
        });
        Ok(AsyncServerHandle {
            addr,
            shutdown,
            runtime: Some(runtime),
            accept: Some(accept),
        })
    }
}

/// A running async server's handle (see [`AsyncServer::spawn`]).
pub struct AsyncServerHandle {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    runtime: Option<Runtime>,
    accept: Option<tokio::task::JoinHandle<()>>,
}

impl AsyncServerHandle {
    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Ask the accept loop to stop (the loop notices within 5 ms). In-flight connections end when the owned runtime is
    /// dropped in [`join`](AsyncServerHandle::join) (or when their clients
    /// disconnect first).
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Shut down, join the accept loop, and drop the runtime — which ends
    /// in-flight connection tasks (their clients see EOF) and waits for
    /// already-running blocking engine calls to leave the pool.
    ///
    /// Must be called from a non-async thread (it blocks on the owned
    /// runtime). [`Drop`] behaves the same way except that it does not wait
    /// for the accept loop.
    pub fn join(mut self) {
        self.shutdown();
        if let Some(runtime) = self.runtime.take()
            && let Some(accept) = self.accept.take()
        {
            let _ = runtime.block_on(accept);
        }
        // Dropping the runtime (at the end of this function) ends in-flight
        // connection tasks; their blocking engine calls (already running) are
        // awaited by the runtime shutdown, so no engine work is abandoned
        // mid-flight.
    }
}

impl Drop for AsyncServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Best-effort `Error` frame for a connection refused by the cap, then close.
/// Runs on its own task with a bounded write, so a client that never reads
/// cannot block the accept loop.
fn reject_over_limit(mut stream: TcpStream, max: usize) {
    tokio::spawn(async move {
        let message = format!("connection limit reached (max {max}); try again later");
        if let Ok(bytes) = protocol::encode_response(&Response::Error { message }) {
            let _ = tokio::time::timeout(REJECT_WRITE_TIMEOUT, stream.write_all(&bytes)).await;
        }
        // `stream` drops here: the connection is closed either way.
    });
}

/// Serve one client: handshake, then request/response until EOF.
///
/// Owns a fresh `db.connect()`, exactly like the blocking transport: the
/// `USE` session, open transaction and session state are private to this TCP
/// client. Engine calls run on blocking-pool threads (see [`run_blocking`]);
/// this task only does frame I/O, timers and the session bookkeeping the
/// blocking server keeps in its connection thread.
async fn serve_connection(
    stream: TcpStream,
    db: Database,
    idle_timeout: Option<Duration>,
    policy: ConnectionPolicy,
) {
    let mut stream = stream;
    if let Err(e) = stream.set_nodelay(true) {
        eprintln!("pinkdb-server(async): cannot set TCP_NODELAY on an accepted socket: {e}");
    }
    let mut conn = db.connect();
    // Network session: the engine re-checks the store's user catalog before
    // every statement (so a batch cannot continue after its own CREATE USER).
    conn.require_login(true);
    conn.set_exec_limits(policy.limits);
    let mut throttle = LoginThrottle::new();

    // Handshake: the first request must be a version-matched Hello.
    let protocol_version = match read_request(&mut stream, idle_timeout).await {
        Ok(Request::Hello { protocol_version }) => protocol_version,
        Ok(other) => {
            let _ = send_error(
                &mut stream,
                &format!("expected a Hello handshake first, got {}", other.kind()),
                idle_timeout,
            )
            .await;
            return;
        }
        Err(e) => {
            let _ = send_error(&mut stream, &e.to_string(), idle_timeout).await;
            return;
        }
    };
    if protocol_version != PROTOCOL_VERSION {
        let _ = send_error(
            &mut stream,
            &format!(
                "protocol version mismatch: server speaks v{PROTOCOL_VERSION}, \
                 client asked for v{protocol_version}"
            ),
            idle_timeout,
        )
        .await;
        return;
    }
    let hello_ok = Response::HelloOk {
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
    };
    if send_response(&mut stream, &hello_ok, idle_timeout)
        .await
        .is_err()
    {
        return;
    }

    // v5: connection-local prepared statements (`?` placeholders), exactly as
    // the blocking server: handles are handed out in order and die with this
    // connection; [`MAX_PREPARED`] bounds the metadata a client can pin.
    let mut prepared: HashMap<u64, PreparedStatement> = HashMap::new();
    let mut next_handle: u64 = 1;

    loop {
        let request = match read_request(&mut stream, idle_timeout).await {
            Ok(request) => request,
            // EOF or an IO error (including the idle timeout): the client is
            // gone; end this task.
            Err(ProtocolError::Io(_)) => return,
            // A malformed or oversized frame cannot be resynchronized: report
            // it if possible, then close the connection.
            Err(e) => {
                let _ = send_error(&mut stream, &e.to_string(), idle_timeout).await;
                return;
            }
        };
        match request {
            // v9 (wave 48): verify credentials on the blocking pool (an
            // Argon2id verification is CPU work) and adopt the principal.
            // The per-IP check costs no Argon2id, the global gate refuses
            // excess verifications immediately, and the per-connection
            // throttle closes after five failures with increasing delays.
            Request::Login { user, password } => {
                if let Some(retry) = policy.ips.retry_after(policy.peer_ip) {
                    if send_error(
                        &mut stream,
                        &format!(
                            "too many failed login attempts from this address; retry in {}s",
                            retry.as_secs().max(1)
                        ),
                        idle_timeout,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let Some(permit) = policy.gate.try_acquire() else {
                    if send_error(
                        &mut stream,
                        "too many concurrent logins; retry shortly",
                        idle_timeout,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    continue;
                };
                let ran = run_blocking(&mut stream, conn, idle_timeout, move |conn, sink| {
                    // `login_reply` is the blocking server's own code path:
                    // the frames cannot drift between the transports.
                    let attempt = server::login_reply(sink, conn, &user, &password).ok();
                    // Hold the verification slot until the derivation ends
                    // (including a frame-send failure path).
                    drop(permit);
                    Blocked {
                        close: attempt.is_none(),
                        prepared: None,
                        login: attempt,
                    }
                })
                .await;
                match ran {
                    Ran {
                        conn: Some(c),
                        blocked,
                    } => {
                        conn = *c;
                        if blocked.close {
                            return;
                        }
                        match blocked.login {
                            Some(LoginAttempt::Succeeded) => {
                                throttle.reset();
                                policy.ips.record_success(policy.peer_ip);
                            }
                            Some(LoginAttempt::Failed) => {
                                let delay = throttle.failure_delay();
                                policy.ips.record_failure(policy.peer_ip);
                                tokio::time::sleep(delay).await;
                                if throttle.closed() {
                                    return;
                                }
                            }
                            None => return,
                        }
                    }
                    Ran { conn: None, .. } => return,
                }
            }
            Request::Execute { sql } => {
                if auth_required(&db, &conn) {
                    if send_error(&mut stream, AUTH_REQUIRED, idle_timeout)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let ran = run_blocking(&mut stream, conn, idle_timeout, move |conn, sink| {
                    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        server::run_statement_hook(&sql);
                        conn.execute(&sql)
                    }));
                    let mut close = false;
                    match outcome {
                        Ok(Ok(results)) => match u32::try_from(results.len()) {
                            Ok(statements) => {
                                for result in &results {
                                    if let Err(e) = server::send_result(sink, result) {
                                        // A streaming failure (e.g. a single
                                        // value larger than the frame cap) is
                                        // reported before closing; a transport
                                        // failure just ends the connection.
                                        if !matches!(e, ProtocolError::Io(_)) {
                                            let _ = server::send_error(sink, &e.to_string());
                                        }
                                        close = true;
                                        break;
                                    }
                                }
                                // v2 batch terminator: the client reads result
                                // groups until this frame, so it never parses
                                // SQL to count statements.
                                if !close
                                    && sink
                                        .send_frame(&Response::BatchDone { statements })
                                        .is_err()
                                {
                                    close = true;
                                }
                            }
                            Err(_) => {
                                let _ = server::send_error(
                                    sink,
                                    "too many statements in one Execute batch",
                                );
                                close = true;
                            }
                        },
                        // A SQL error is a normal response: report it and keep
                        // the connection open. No BatchDone follows: the Error
                        // frame ends the batch.
                        Ok(Err(e)) => {
                            let _ = server::send_error(sink, &e.to_string());
                        }
                        // An engine panic is caught and reported; the
                        // connection (and every other client) stays alive.
                        Err(payload) => {
                            let message = panic_message(payload.as_ref());
                            eprintln!("pinkdb-server(async): statement panicked: {message}");
                            let _ = server::send_error(
                                sink,
                                &format!("internal error: statement panicked ({message})"),
                            );
                        }
                    }
                    Blocked {
                        close,
                        prepared: None,
                        login: None,
                    }
                })
                .await;
                match ran {
                    Ran {
                        conn: Some(c),
                        blocked,
                    } => {
                        conn = *c;
                        if blocked.close {
                            return;
                        }
                    }
                    Ran { conn: None, .. } => return,
                }
            }
            Request::Ping => {
                if send_response(&mut stream, &Response::Pong, idle_timeout)
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Request::Prepare { sql } => {
                if auth_required(&db, &conn) {
                    if send_error(&mut stream, AUTH_REQUIRED, idle_timeout)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                if prepared.len() >= MAX_PREPARED {
                    if send_error(
                        &mut stream,
                        &format!(
                            "too many prepared statements (per-connection limit {MAX_PREPARED})"
                        ),
                        idle_timeout,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let ran = run_blocking(&mut stream, conn, idle_timeout, move |conn, sink| {
                    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| conn.prepare(&sql)));
                    let mut parsed = None;
                    match outcome {
                        // Pure parsing: nothing executes and nothing is logged.
                        Ok(Ok(stmt)) => parsed = Some(stmt),
                        // A parse error (or an unsupported shape) is a normal
                        // response: report it and keep the connection open.
                        Ok(Err(e)) => {
                            let _ = server::send_error(sink, &e.to_string());
                        }
                        Err(payload) => {
                            let message = panic_message(payload.as_ref());
                            eprintln!("pinkdb-server(async): prepare panicked: {message}");
                            let _ = server::send_error(
                                sink,
                                &format!("internal error: prepare panicked ({message})"),
                            );
                        }
                    }
                    Blocked {
                        close: false,
                        prepared: parsed,
                        login: None,
                    }
                })
                .await;
                match ran {
                    Ran {
                        conn: Some(c),
                        blocked:
                            Blocked {
                                prepared: Some(stmt),
                                ..
                            },
                    } => {
                        conn = *c;
                        let Some(next) = next_handle.checked_add(1) else {
                            let _ = send_error(
                                &mut stream,
                                "prepared-statement handle space exhausted",
                                idle_timeout,
                            )
                            .await;
                            continue;
                        };
                        let handle = next_handle;
                        next_handle = next;
                        let parameter_count = stmt.parameter_count() as u32;
                        prepared.insert(handle, stmt);
                        if send_response(
                            &mut stream,
                            &Response::Prepared {
                                handle,
                                parameter_count,
                            },
                            idle_timeout,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                    Ran {
                        conn: Some(c),
                        blocked: Blocked { prepared: None, .. },
                    } => conn = *c,
                    Ran { conn: None, .. } => return,
                }
            }
            // v5: bind values and execute the prepared statement. Framing is
            // identical to a one-statement Execute: one result group, then a
            // BatchDone terminator. The hook sees the substituted SQL, so
            // statement-count instrumentation matches the embedded path.
            Request::ExecutePrepared { handle, values } => {
                if auth_required(&db, &conn) {
                    if send_error(&mut stream, AUTH_REQUIRED, idle_timeout)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let Some(stmt) = prepared.get(&handle).cloned() else {
                    if send_error(
                        &mut stream,
                        &format!("unknown prepared statement handle {handle}"),
                        idle_timeout,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    continue;
                };
                let ran = run_blocking(&mut stream, conn, idle_timeout, move |conn, sink| {
                    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        let bound = conn.bind(&stmt, &values)?;
                        server::run_statement_hook(bound.sql());
                        conn.execute_bound(&bound)
                    }));
                    let mut close = false;
                    match outcome {
                        Ok(Ok(result)) => {
                            // A bound statement is exactly one statement: one
                            // result group, then the batch terminator.
                            if let Err(e) = server::send_result(sink, &result) {
                                if !matches!(e, ProtocolError::Io(_)) {
                                    let _ = server::send_error(sink, &e.to_string());
                                }
                                close = true;
                            } else if sink
                                .send_frame(&Response::BatchDone { statements: 1 })
                                .is_err()
                            {
                                close = true;
                            }
                        }
                        // Bind/type/execution errors are normal responses; the
                        // connection stays open (mirrors Execute).
                        Ok(Err(e)) => {
                            let _ = server::send_error(sink, &e.to_string());
                        }
                        Err(payload) => {
                            let message = panic_message(payload.as_ref());
                            eprintln!("pinkdb-server(async): prepared execute panicked: {message}");
                            let _ = server::send_error(
                                sink,
                                &format!("internal error: statement panicked ({message})"),
                            );
                        }
                    }
                    Blocked {
                        close,
                        prepared: None,
                        login: None,
                    }
                })
                .await;
                match ran {
                    Ran {
                        conn: Some(c),
                        blocked,
                    } => {
                        conn = *c;
                        if blocked.close {
                            return;
                        }
                    }
                    Ran { conn: None, .. } => return,
                }
            }
            Request::Hello { .. } => {
                if send_error(&mut stream, "duplicate Hello handshake", idle_timeout)
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

/// What a blocking engine action reports back to the connection task.
struct Blocked {
    /// Close the connection after this action's frames have been delivered.
    close: bool,
    /// A parsed prepared statement to register on the connection task
    /// (`Request::Prepare`); `None` for every other action.
    prepared: Option<PreparedStatement>,
    /// The outcome of a `Request::Login` verification; `None` for every
    /// other action.
    login: Option<LoginAttempt>,
}

/// What [`run_blocking`] hands back to the connection loop.
struct Ran {
    /// The connection returned by the blocking action. Boxed because it is
    /// ~2 KiB and crosses this boundary on every request, and `Option`
    /// because the action can end the connection: delivery failed, the
    /// engine task panicked outside its `catch_unwind`, or the runtime is
    /// shutting down.
    conn: Option<Box<Connection>>,
    /// What the action reported (only meaningful when `conn` is `Some`).
    blocked: Blocked,
}

impl Ran {
    /// The connection is gone with its blocking action: close it.
    fn closed() -> Ran {
        Ran {
            conn: None,
            blocked: Blocked {
                close: true,
                prepared: None,
                login: None,
            },
        }
    }
}

/// Run one blocking engine action on the blocking pool while its encoded
/// response frames stream to the socket.
///
/// Frames travel through a bounded [`mpsc`] queue: the action (running on a
/// blocking-pool thread with a [`CONNECTION_STACK_SIZE`] stack) encodes each
/// frame and blocks when the queue is full, so memory stays bounded even for
/// huge result sets; this task drains the queue and writes to the socket
/// under the idle timeout. The action owns the `Connection` and returns it,
/// so the session survives across calls.
async fn run_blocking<F>(
    stream: &mut TcpStream,
    conn: Connection,
    idle_timeout: Option<Duration>,
    action: F,
) -> Ran
where
    F: FnOnce(&mut Connection, &mut ChannelSink) -> Blocked + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(FRAME_QUEUE);
    let engine = tokio::task::spawn_blocking(move || {
        let mut conn = conn;
        let mut sink = ChannelSink { tx };
        let blocked = action(&mut conn, &mut sink);
        (conn, blocked)
    });

    let mut write_failed = false;
    while let Some(frame) = rx.recv().await {
        if write_all(stream, &frame, idle_timeout).await.is_err() {
            write_failed = true;
            break;
        }
    }
    // Drop the receiver before awaiting the engine task: a task blocked on a
    // full queue (client stopped reading) wakes with a send error instead of
    // deadlocking the connection.
    drop(rx);

    match engine.await {
        // The connection comes back from the blocking action.
        Ok((conn, blocked)) if !write_failed => Ran {
            conn: Some(Box::new(conn)),
            blocked,
        },
        Ok(_) => Ran::closed(),
        Err(join) => {
            // The action panicked outside its own `catch_unwind`, or the
            // runtime is shutting down; the connection unwound with it.
            if join.is_panic() {
                let message = panic_message(join.into_panic().as_ref());
                eprintln!("pinkdb-server(async): engine task panicked: {message}");
                let _ = send_error(
                    stream,
                    &format!("internal error: statement panicked ({message})"),
                    idle_timeout,
                )
                .await;
            }
            Ran::closed()
        }
    }
}

/// Frame sink for the blocking engine action: each frame is encoded with the
/// shared codec and queued for the connection task. `blocking_send` is only
/// legal off the async workers — which is exactly where this runs
/// (`spawn_blocking`).
struct ChannelSink {
    tx: mpsc::Sender<Vec<u8>>,
}

impl FrameSink for ChannelSink {
    fn send_frame(&mut self, response: &Response) -> Result<(), ProtocolError> {
        let bytes = protocol::encode_response(response)?;
        self.tx.blocking_send(bytes).map_err(|_| {
            ProtocolError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the connection task is gone",
            ))
        })
    }
}

/// Read one [`Request`] frame. Mirrors `protocol::read_frame`: the length
/// prefix is validated against [`MAX_FRAME_SIZE`] and the body buffer grows
/// with the bytes that actually arrived (never pre-sized from the untrusted
/// prefix).
async fn read_request(
    stream: &mut TcpStream,
    idle_timeout: Option<Duration>,
) -> Result<Request, ProtocolError> {
    // The blocking transport reads the prefix from a socket with an idle
    // timeout: any failure here (EOF, reset, timeout) is an `Io` error, so
    // the caller ends the connection quietly — a client that vanishes
    // between frames is gone, not malformed.
    let mut prefix = [0u8; 4];
    let mut filled = 0;
    while filled < prefix.len() {
        let n = match with_idle(stream.read(&mut prefix[filled..]), idle_timeout).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(ProtocolError::Io(e)),
            Err(_) => return Err(idle_timeout_error()),
        };
        if n == 0 {
            return Err(ProtocolError::Io(io::Error::from(
                io::ErrorKind::UnexpectedEof,
            )));
        }
        filled += n;
    }
    let len = u32::from_le_bytes(prefix) as usize;
    if len == 0 {
        return Err(ProtocolError::Malformed(
            "empty frame (missing message type)".to_string(),
        ));
    }
    if len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            size: len,
            max: MAX_FRAME_SIZE,
        });
    }
    let mut tag = [0u8; 1];
    read_exact(stream, &mut tag, "frame type byte", idle_timeout).await?;
    let want = len - 1;
    let mut body = Vec::with_capacity(want.min(FRAME_READ_STEP));
    while body.len() < want {
        let start = body.len();
        let step = (want - start).min(FRAME_READ_STEP);
        body.resize(start + step, 0);
        read_exact(stream, &mut body[start..], "frame body", idle_timeout).await?;
    }
    Request::decode_body(tag[0], &body)
}

/// Fill `buf`, one read call at a time. Every call carries the idle timeout,
/// so the granularity matches a blocking socket with a read timeout: a peer
/// that keeps making progress keeps the connection, a silent one is
/// disconnected. An EOF before `buf` is filled truncates an announced frame.
async fn read_exact(
    stream: &mut TcpStream,
    buf: &mut [u8],
    what: &'static str,
    idle_timeout: Option<Duration>,
) -> Result<(), ProtocolError> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = match with_idle(stream.read(&mut buf[filled..]), idle_timeout).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(ProtocolError::Io(e)),
            Err(_) => return Err(idle_timeout_error()),
        };
        if n == 0 {
            return Err(ProtocolError::Truncated(what));
        }
        filled += n;
    }
    Ok(())
}

/// Write one encoded frame, one write call at a time. Each call carries the
/// idle timeout, the same per-syscall granularity the blocking transport's
/// socket write timeout has.
async fn write_all(
    stream: &mut TcpStream,
    bytes: &[u8],
    idle_timeout: Option<Duration>,
) -> Result<(), ProtocolError> {
    let mut written = 0;
    while written < bytes.len() {
        let n = match with_idle(stream.write(&bytes[written..]), idle_timeout).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(ProtocolError::Io(e)),
            Err(_) => return Err(idle_timeout_error()),
        };
        if n == 0 {
            return Err(ProtocolError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "write returned 0 bytes",
            )));
        }
        written += n;
    }
    Ok(())
}

/// Encode and write one [`Response`] frame.
async fn send_response(
    stream: &mut TcpStream,
    response: &Response,
    idle_timeout: Option<Duration>,
) -> Result<(), ProtocolError> {
    let bytes = protocol::encode_response(response)?;
    write_all(stream, &bytes, idle_timeout).await
}

/// Encode and write one `Error` frame.
async fn send_error(
    stream: &mut TcpStream,
    message: &str,
    idle_timeout: Option<Duration>,
) -> Result<(), ProtocolError> {
    send_response(
        stream,
        &Response::Error {
            message: message.to_string(),
        },
        idle_timeout,
    )
    .await
}

/// Apply the idle timeout to one socket operation (`None` waits forever).
async fn with_idle<F: std::future::Future>(
    fut: F,
    idle_timeout: Option<Duration>,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    match idle_timeout {
        Some(timeout) => tokio::time::timeout(timeout, fut).await,
        None => Ok(fut.await),
    }
}

/// The error an idle socket produces: an `Io` error, so the connection loop
/// treats it exactly like a blocking socket that hit its read/write timeout
/// (close quietly).
fn idle_timeout_error() -> ProtocolError {
    ProtocolError::Io(io::Error::new(io::ErrorKind::TimedOut, "idle timeout"))
}
