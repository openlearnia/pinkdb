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
//!   [`Server::allow_load_extension`]: without authentication, letting a
//!   remote client `dlopen` a host library would be remote code execution.

use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pinkdb::{Database, QueryResult, Value};

use crate::protocol::{self, DoneKind, ProtocolError, Request, Response, PROTOCOL_VERSION};

/// How often the nonblocking accept loop checks the shutdown flag.
const ACCEPT_POLL: Duration = Duration::from_millis(5);

/// First retry delay after a failed `accept()`; doubles up to the max.
const ACCEPT_ERROR_MIN_BACKOFF: Duration = Duration::from_millis(10);

/// Longest retry delay after a failed `accept()`.
const ACCEPT_ERROR_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Stack size for connection threads. The engine's AST-depth guard keeps
/// accepted statements far below this, but the default 2 MiB thread stack is
/// too close to the worst debug-build recursion depths for comfort.
const CONNECTION_STACK_SIZE: usize = 16 * 1024 * 1024;

/// Concurrent connections served at once (fd/thread pressure bound).
pub const DEFAULT_MAX_CONNECTIONS: usize = 128;

/// Default idle timeout for client sockets (reads and writes).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

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
fn run_statement_hook(sql: &str) {
    let hook = *STATEMENT_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(hook) = hook {
        hook(sql);
    }
}

/// Decrements the active-connection counter when the connection thread ends,
/// including on unwind.
struct ActiveGuard(Arc<AtomicUsize>);

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

/// A bound, not-yet-running pinkdb server.
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    db: Database,
    shutdown: Arc<AtomicBool>,
    max_connections: usize,
    active: Arc<AtomicUsize>,
    idle_timeout: Option<Duration>,
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
        })
    }

    /// Let remote clients run SQL `LOAD EXTENSION` (default: refused). Only
    /// for trusted networks: the statement makes the server process load and
    /// run a native library from its own filesystem.
    pub fn allow_load_extension(self, allow: bool) -> Self {
        self.db.set_allow_sql_load_extension(allow);
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
                        reject_over_limit(stream, self.max_connections);
                        continue;
                    }
                    let guard = ActiveGuard(Arc::clone(&self.active));
                    self.active.fetch_add(1, Ordering::SeqCst);
                    let db = self.db.clone();
                    let idle_timeout = self.idle_timeout;
                    if let Err(e) = thread::Builder::new()
                        .name("pinkdb-server-conn".to_string())
                        .stack_size(CONNECTION_STACK_SIZE)
                        .spawn(move || {
                            let _guard = guard;
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

/// Serve one client: handshake, then request/response until EOF.
///
/// The connection owns a fresh `db.connect()`: its `USE` session, open
/// transaction and session state are private to this TCP client.
///
/// Statement execution runs inside `catch_unwind`: an engine panic becomes an
/// `Error` frame and a stderr log instead of killing the connection (or, for
/// a panicking connection thread, the whole process).
fn serve_connection(stream: TcpStream, db: Database, idle_timeout: Option<Duration>) {
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
        return;
    }
    if let Err(e) = stream.set_nodelay(true) {
        eprintln!("pinkdb-server: cannot set TCP_NODELAY on an accepted socket: {e}");
    }
    let Ok(mut writer) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut conn = db.connect();

    // Handshake: the first request must be a version-matched Hello.
    let protocol_version = match protocol::read_request(&mut reader) {
        Ok(Request::Hello { protocol_version }) => protocol_version,
        Ok(other) => {
            let _ = send_error(
                &mut writer,
                &format!("expected a Hello handshake first, got {}", other.kind()),
            );
            return;
        }
        Err(e) => {
            let _ = send_error(&mut writer, &e.to_string());
            return;
        }
    };
    if protocol_version != PROTOCOL_VERSION {
        let _ = send_error(
            &mut writer,
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
    if protocol::write_response(&mut writer, &hello_ok).is_err() {
        return;
    }

    // v5: connection-local prepared statements (`?` placeholders). Handles are
    // handed out in order and die with this connection; the cap bounds the
    // per-connection metadata a client can pin.
    const MAX_PREPARED: usize = 1024;
    let mut prepared: std::collections::HashMap<u64, pinkdb::PreparedStatement> =
        std::collections::HashMap::new();
    let mut next_handle: u64 = 1;

    loop {
        match protocol::read_request(&mut reader) {
            Ok(Request::Execute { sql }) => {
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
                                    &mut writer,
                                    "too many statements in one Execute batch",
                                );
                                return;
                            }
                        };
                        for result in &results {
                            if let Err(e) = send_result(&mut writer, result) {
                                // A streaming failure (e.g. a single value
                                // larger than the frame cap) is reported
                                // before closing; an IO failure just ends the
                                // thread.
                                if !matches!(e, ProtocolError::Io(_)) {
                                    let _ = send_error(&mut writer, &e.to_string());
                                }
                                return;
                            }
                        }
                        // v2 batch terminator: the client reads result groups
                        // until this frame, so it never parses SQL to count
                        // statements.
                        if protocol::write_response(
                            &mut writer,
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
                        if send_error(&mut writer, &e.to_string()).is_err() {
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
                            &mut writer,
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
                if protocol::write_response(&mut writer, &Response::Pong).is_err() {
                    return;
                }
            }
            // v5: parse one statement with `?` placeholders into a
            // connection-local prepared statement. Pure parsing: nothing
            // executes and nothing is logged.
            Ok(Request::Prepare { sql }) => {
                if prepared.len() >= MAX_PREPARED {
                    if send_error(
                        &mut writer,
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
                                &mut writer,
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
                            &mut writer,
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
                        if send_error(&mut writer, &e.to_string()).is_err() {
                            return;
                        }
                    }
                    Err(payload) => {
                        let message = panic_message(payload.as_ref());
                        eprintln!("pinkdb-server: prepare panicked: {message}");
                        if send_error(
                            &mut writer,
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
                let Some(stmt) = prepared.get(&handle) else {
                    if send_error(
                        &mut writer,
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
                        if let Err(e) = send_result(&mut writer, &result) {
                            if !matches!(e, ProtocolError::Io(_)) {
                                let _ = send_error(&mut writer, &e.to_string());
                            }
                            return;
                        }
                        if protocol::write_response(
                            &mut writer,
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
                        if send_error(&mut writer, &e.to_string()).is_err() {
                            return;
                        }
                    }
                    Err(payload) => {
                        let message = panic_message(payload.as_ref());
                        eprintln!("pinkdb-server: prepared execute panicked: {message}");
                        if send_error(
                            &mut writer,
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
                if send_error(&mut writer, "duplicate Hello handshake").is_err() {
                    return;
                }
            }
            // EOF or an IO error: the client is gone; end this thread.
            Err(ProtocolError::Io(_)) => return,
            // A malformed or oversized frame cannot be resynchronized: report
            // it if possible, then close the connection.
            Err(e) => {
                let _ = send_error(&mut writer, &e.to_string());
                return;
            }
        }
    }
}

/// Human-readable text from a caught panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
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
fn send_result<W: Write>(w: &mut W, result: &QueryResult) -> Result<(), ProtocolError> {
    match result {
        QueryResult::Selected { names, types, rows } => {
            let result_set = Response::ResultSet {
                names: names.clone(),
                types: types.clone(),
            };
            protocol::write_response(w, &result_set)?;
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
                    protocol::write_response(w, &frame)?;
                    batch_bytes = 0;
                }
                batch.push(row.clone());
                batch_bytes += row_bytes;
            }
            if !batch.is_empty() {
                protocol::write_response(w, &Response::Rows { rows: batch })?;
            }
            done(w, DoneKind::Selected, 0, "")
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
    }
}

fn done<W: Write>(w: &mut W, kind: DoneKind, count: u64, name: &str) -> Result<(), ProtocolError> {
    protocol::write_response(
        w,
        &Response::Done {
            kind,
            count,
            name: name.to_string(),
        },
    )
}

fn send_error<W: Write>(w: &mut W, message: &str) -> Result<(), ProtocolError> {
    protocol::write_response(
        w,
        &Response::Error {
            message: message.to_string(),
        },
    )
}
