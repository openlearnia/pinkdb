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
//!   over the cap the server sends an `Error` frame and closes.
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
//!   under that user's grants (the engine's per-statement check). `Login`
//!   itself is answered with `LoggedIn` or the same
//!   `authentication failed: …` error for an unknown user and a wrong
//!   password. `Ping` never needs authentication.
//! - With the crate feature `tls`, [`Server::bind_tls`] serves the same
//!   protocol over rustls-encrypted TCP. The handshake runs on the
//!   connection's own thread (under the same idle timeout), so a failed or
//!   stalled handshake logs and drops that connection while the server keeps
//!   serving. A plaintext client on a TLS listener (or vice versa) fails its
//!   own connection and nothing else. TLS encrypts the transport only: there
//!   is still no authentication, and the connection cap, per-connection
//!   session isolation and idle timeout are unchanged.

use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pinkdb::{Connection, DataType, Database, QueryResult, Value};

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

/// Connection-local prepared statements a client may pin (shared by the
/// blocking and the `async-server` transports so the cap cannot drift).
pub(crate) const MAX_PREPARED: usize = 1024;

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
    /// certificates are not requested; TLS here is transport encryption, not
    /// authentication.
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
                Ok((stream, peer)) => {
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
                        reject_over_limit(stream, self.max_connections);
                        continue;
                    }
                    let guard = ActiveGuard(Arc::clone(&self.active));
                    self.active.fetch_add(1, Ordering::SeqCst);
                    let db = self.db.clone();
                    let idle_timeout = self.idle_timeout;
                    #[cfg(feature = "tls")]
                    let tls = self.tls.clone();
                    if let Err(e) = thread::Builder::new()
                        .name("pinkdb-server-conn".to_string())
                        .stack_size(CONNECTION_STACK_SIZE)
                        .spawn(move || {
                            let _guard = guard;
                            #[cfg(feature = "tls")]
                            if let Some(config) = tls {
                                serve_tls_connection(stream, peer, db, idle_timeout, config);
                                return;
                            }
                            serve_connection(stream, db, idle_timeout);
                        })
                    {
                        // On a spawn failure the closure (and with it the
                        // guard) is dropped, so the active count stays exact.
                        eprintln!("pinkdb-server: cannot spawn connection thread: {e}");
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

/// Best-effort `Error` frame for a connection refused by the cap, then close.
/// Runs on its own thread (with a bounded write) so a client that never reads
/// cannot block the accept loop.
fn reject_over_limit(mut stream: TcpStream, max: usize) {
    std::thread::spawn(move || {
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(1)));
        let message = format!("connection limit reached (max {max}); try again later");
        let _ = protocol::write_response(&mut stream, &Response::Error { message });
    });
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
fn serve_connection(stream: TcpStream, db: Database, idle_timeout: Option<Duration>) {
    if !configure_socket(&stream, idle_timeout) {
        return;
    }
    serve_client(stream, db);
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
    serve_client(tls, db);
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
fn serve_client<S: Read + Write>(stream: S, db: Database) {
    let mut reader = BufReader::new(stream);
    let mut conn = db.connect();

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
            // connection stays usable on a failure (a client may retry).
            Ok(Request::Login { user, password }) => {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    login_reply(reader.get_mut(), &mut conn, &user, &password)
                }));
                match outcome {
                    Ok(Ok(())) => {}
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

/// Answer one `Login` request: verify the credentials, adopt the principal,
/// and reply `LoggedIn` (or the identical `authentication failed` `Error`).
/// Shared by both transports.
pub(crate) fn login_reply<S: FrameSink + ?Sized>(
    sink: &mut S,
    conn: &mut Connection,
    user: &str,
    password: &str,
) -> Result<(), ProtocolError> {
    match conn.login(user, password) {
        Ok(name) => sink.send_frame(&Response::LoggedIn { user: name }),
        Err(e) => send_error(sink, &e.to_string()),
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
