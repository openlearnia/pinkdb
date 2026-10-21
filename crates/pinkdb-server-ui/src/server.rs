//! The console's HTTP server: the runtime, the accept loop, the limits and
//! the routing.
//!
//! # The worker model
//!
//! The console owns a **tokio runtime on its own thread** — it never
//! touches the server's transport (the blocking `sync` server and the tokio
//! `async-server` transport both work unchanged). The runtime is built with
//! an explicit worker-thread count and an explicit **blocking-pool bound**
//! (`max_blocking_threads`): pinkdb's engine is synchronous, so every store
//! read runs through `spawn_blocking`, and the pool is what keeps a burst of
//! console requests from spawning an unbounded number of OS threads.
//!
//! The accept loop is a plain `tokio::net::TcpListener` loop with a
//! **connection cap** (`max_connections`): over the cap the connection is
//! refused with a `503` and closed. There is no thread-per-connection: a
//! connection is a task, and the number of live connections is bounded.
//!
//! # The limits
//!
//! * **Header count and size**: hyper's HTTP/1.1 builder caps both
//!   ([`MAX_HEADERS`], [`MAX_HEADER_SIZE`]).
//! * **Header read timeout**: hyper's `header_read_timeout` ([`UiOptions`]
//!   `read_timeout`) — a slow client cannot hold a connection open forever.
//! * **Request deadline**: the whole connection is wrapped in a
//!   `tokio::time::timeout` (`request_timeout`), so a slow *body* or a slow
//!   reader cannot pin the connection either.
//! * **Body size**: axum's `DefaultBodyLimit` (`max_body_bytes`). The
//!   console reads no bodies at all (it is `GET`/`HEAD` only), so this is a
//!   belt-and-braces bound.
//! * **Statement limits**: the engine's own pass-5 limits
//!   ([`UiOptions::statement_timeout`], `max_result_rows`,
//!   `max_result_bytes`).

use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::extract::{ConnectInfo, DefaultBodyLimit};
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use pinkdb::{Database, ExecLimits};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tower_service::Service;

use crate::api;
use crate::assets;
use crate::auth::AuthState;
use crate::policy::{ConsoleTls, UiAuth, UiError, check_auth_policy, check_bind_policy};
use crate::store::Session;

/// Maximum number of request headers (hyper's HTTP/1.1 builder).
pub const MAX_HEADERS: usize = 64;

/// Maximum size of one request header (hyper's HTTP/1.1 builder).
pub const MAX_HEADER_SIZE: usize = 16 * 1024;

/// Stack size for the console's runtime thread. The engine's AST-depth
/// guard keeps accepted statements far below the default stack, but the
/// server crate uses 16 MiB for the same reason: the engine's worst-case
/// recursive paths are close to the default 2 MiB in a debug build.
const CONSOLE_STACK_SIZE: usize = 16 * 1024 * 1024;

/// The console's configuration.
#[derive(Clone, Debug)]
pub struct UiOptions {
    /// The tokio runtime's worker threads (the async reactor).
    pub workers: usize,
    /// The blocking pool's threads — the pool the **engine** runs on. Set
    /// explicitly: tokio's default is not accepted (it grows with the
    /// machine's core count and is not a bound the operator chose).
    pub max_blocking_threads: usize,
    /// The maximum number of connections served at once; over the cap a
    /// connection is refused with `503`.
    pub max_connections: usize,
    /// How long a client may take to send its request head (hyper's
    /// `header_read_timeout`).
    pub read_timeout: Duration,
    /// The deadline for one whole request (headers, body, response).
    pub request_timeout: Duration,
    /// The maximum request body size (the console reads no bodies; this is
    /// the belt-and-braces bound).
    pub max_body_bytes: usize,
    /// The maximum page size a row preview may ask for.
    pub max_page_size: usize,
    /// The per-statement engine deadline (the pass-5 limit).
    pub statement_timeout: Duration,
    /// The per-statement materialized-row cap (the pass-5 limit).
    pub max_result_rows: usize,
    /// The per-statement materialized-byte cap (the pass-5 limit).
    pub max_result_bytes: usize,
    /// How the console authenticates its requests.
    pub auth: UiAuth,
}

impl Default for UiOptions {
    fn default() -> Self {
        Self {
            workers: 4,
            max_blocking_threads: 8,
            max_connections: 64,
            read_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_body_bytes: 64 * 1024,
            max_page_size: 500,
            statement_timeout: Duration::from_secs(30),
            max_result_rows: 1_000,
            max_result_bytes: 16 * 1024 * 1024,
            auth: UiAuth::None,
        }
    }
}

impl UiOptions {
    /// The engine limits the console's sessions run under.
    fn exec_limits(&self) -> ExecLimits {
        ExecLimits {
            statement_timeout: Some(self.statement_timeout),
            max_result_rows: Some(self.max_result_rows),
            max_result_bytes: Some(self.max_result_bytes),
        }
    }
}

/// Test-only injection point (hidden from the docs): when set, every
/// request calls the hook **on the thread that runs the HTTP layer** (the
/// async runtime's worker). The console's tests use it to prove that the
/// engine never runs on that thread: the engine's statements run on the
/// blocking pool instead.
#[doc(hidden)]
pub static REACTOR_HOOK: std::sync::Mutex<Option<ReactorHook>> = std::sync::Mutex::new(None);

/// The reactor hook's type: a boxed closure the tests install.
#[doc(hidden)]
pub type ReactorHook = Box<dyn Fn() + Send + Sync>;

/// Run the hidden reactor hook, if one is installed.
fn run_reactor_hook() {
    let guard = REACTOR_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(hook) = guard.as_ref() {
        hook();
    }
}

/// The state every request shares.
pub(crate) struct Shared {
    /// The store the console browses.
    pub db: Database,
    /// The console's configuration.
    pub opts: UiOptions,
    /// The authentication state (mode, limiter, verification gate).
    pub auth: AuthState,
    /// The engine limits.
    exec_limits: ExecLimits,
    /// The pooled sessions (each remembers the database it browses).
    sessions: Mutex<Vec<Session>>,
}

impl Shared {
    fn new(db: Database, opts: UiOptions) -> Self {
        let exec_limits = opts.exec_limits();
        let auth = AuthState::new(opts.auth.clone());
        Self {
            db,
            opts,
            auth,
            exec_limits,
            sessions: Mutex::new(Vec::new()),
        }
    }

    /// Take a session from the pool (or open a new one).
    pub(crate) fn take_session(&self) -> Session {
        let mut pool = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pool.pop()
            .unwrap_or_else(|| Session::new(&self.db, self.exec_limits))
    }

    /// Return a session to the pool.
    ///
    /// The session's principal is dropped first: a pooled session must
    /// never keep the previous request's identity.
    pub(crate) fn put_session(&self, mut session: Session) {
        session.logout();
        let mut pool = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pool.len() < self.opts.max_blocking_threads {
            pool.push(session);
        }
    }
}

/// A bound, not-yet-running console.
pub struct UiServer {
    listener: std::net::TcpListener,
    addr: SocketAddr,
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
}

impl std::fmt::Debug for UiServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiServer")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl UiServer {
    /// Bind `addr` and serve `db` read-only.
    ///
    /// The listener is bound immediately, so [`UiServer::local_addr`] is
    /// exact even for an ephemeral port (`127.0.0.1:0`).
    ///
    /// The **exposure policy** is enforced here, before anything is served:
    ///
    /// * a non-loopback bind is refused (the console is a browser UI and
    ///   cannot use the server's TLS transport — see
    ///   [`crate::check_bind_policy`]);
    /// * a store that has users is refused when no authentication is
    ///   configured (see [`crate::check_auth_policy`]).
    pub fn bind(
        addr: impl ToSocketAddrs,
        db: Database,
        opts: UiOptions,
    ) -> Result<UiServer, UiError> {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        check_bind_policy(addr.ip(), ConsoleTls::Absent)?;
        check_auth_policy(db.has_users(), &opts.auth)?;
        let shared = Arc::new(Shared::new(db, opts));
        let (shutdown, _) = watch::channel(false);
        Ok(UiServer {
            listener,
            addr,
            shared,
            shutdown,
        })
    }

    /// The address the console listens on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the console on its own thread and runtime.
    ///
    /// The returned handle stops it ([`UiHandle::shutdown`]) and joins it.
    pub fn spawn(self) -> std::io::Result<UiHandle> {
        let addr = self.addr;
        let shared = Arc::clone(&self.shared);
        let shutdown = self.shutdown.clone();
        let listener = self.listener;
        let thread = std::thread::Builder::new()
            .name("pinkdb-server-ui".to_string())
            .stack_size(CONSOLE_STACK_SIZE)
            .spawn({
                let shared = Arc::clone(&shared);
                let shutdown = shutdown.clone();
                move || {
                    let runtime = match tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(shared.opts.workers.max(1))
                        .max_blocking_threads(shared.opts.max_blocking_threads.max(1))
                        .thread_name("pinkdb-ui")
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(err) => {
                            eprintln!("pinkdb-server-ui: cannot start the runtime: {err}");
                            return;
                        }
                    };
                    if let Err(err) = runtime.block_on(run(shared, listener, shutdown.clone())) {
                        eprintln!("pinkdb-server-ui: the console stopped: {err}");
                    }
                }
            })?;
        if matches!(shared.auth.mode(), UiAuth::None) {
            eprintln!(
                "pinkdb-server-ui: WARNING: the console on http://{addr} is served WITHOUT \
                 authentication. Every request can read the whole store. The store has no users \
                 (that is why the console may start), but any local process — and anything that \
                 can reach this port — can read every row. Keep the bind address on loopback \
                 and do not expose it."
            );
        }
        Ok(UiHandle {
            addr,
            shutdown,
            thread: Some(thread),
        })
    }
}

/// The accept loop.
async fn run(
    shared: Arc<Shared>,
    listener: std::net::TcpListener,
    shutdown: watch::Sender<bool>,
) -> Result<(), UiError> {
    let listener = TcpListener::from_std(listener)?;
    let app = build_router(Arc::clone(&shared));
    let permits = Arc::new(Semaphore::new(shared.opts.max_connections.max(1)));
    let mut stop = shutdown.subscribe();
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        // Transient accept errors (fd pressure, aborted
                        // connections) must not stop the console.
                        eprintln!("pinkdb-server-ui: accept failed: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        // Over the connection cap: refuse and close. No task
                        // is spawned for the refusal.
                        refuse(stream).await;
                        continue;
                    }
                };
                let app = app.clone();
                let opts = shared.opts.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    serve_connection(stream, peer, app, opts).await;
                });
            }
        }
    }
    Ok(())
}

/// Serve one connection: HTTP/1.1, one request, then close.
async fn serve_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    app: Router,
    opts: UiOptions,
) {
    let _ = stream.set_nodelay(true);
    let service = PeerService {
        router: app,
        peer,
        max_body_bytes: opts.max_body_bytes,
    };
    let mut builder = http1::Builder::new();
    builder.timer(TokioTimer::new());
    builder.header_read_timeout(opts.read_timeout);
    builder.max_headers(MAX_HEADERS);
    builder.max_header_size(MAX_HEADER_SIZE);
    // One request per connection (`Connection: close`).
    builder.keep_alive(false);
    let svc = TowerToHyperService::new(service);
    // The whole request (head, body, response) is bounded: a slow client
    // cannot pin the connection.
    let _ = tokio::time::timeout(
        opts.request_timeout,
        builder.serve_connection(TokioIo::new(stream), svc),
    )
    .await;
}

/// Refuse a connection that is over the cap.
async fn refuse(mut stream: tokio::net::TcpStream) {
    const REFUSAL: &[u8] =
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let _ = tokio::time::timeout(Duration::from_millis(100), stream.write_all(REFUSAL)).await;
}

/// Build the console's router.
///
/// Every route is `GET` (and, through axum, `HEAD`): **there is no mutating
/// route at all**. Anything else is a `405`.
fn build_router(shared: Arc<Shared>) -> Router {
    Router::new()
        .route("/", get(api::shell).fallback(method_not_allowed))
        .route("/preview", get(api::preview).fallback(method_not_allowed))
        .route("/assets/console.css", get(assets::css))
        .route("/assets/console.js", get(assets::script))
        .route(
            "/api/databases",
            get(api::databases).fallback(method_not_allowed),
        )
        .route(
            "/api/schemas",
            get(api::schemas).fallback(method_not_allowed),
        )
        .route("/api/tables", get(api::tables).fallback(method_not_allowed))
        .route("/api/rows", get(api::rows).fallback(method_not_allowed))
        .route("/api/stats", get(api::stats).fallback(method_not_allowed))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(shared.opts.max_body_bytes))
        .with_state(shared)
}

/// The response for a method the console does not implement.
async fn method_not_allowed() -> Response {
    let mut response = api::json_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "the console is read-only: only GET and HEAD are implemented",
    );
    response
        .headers_mut()
        .insert(header::ALLOW, header::HeaderValue::from_static("GET, HEAD"));
    response
}

/// The response for an unknown path.
async fn not_found() -> Response {
    api::json_error(StatusCode::NOT_FOUND, "not found")
}

/// A router wrapper that records the peer's address in the request, so the
/// handlers can apply the per-address login limiter.
#[derive(Clone)]
struct PeerService {
    router: Router,
    peer: SocketAddr,
    max_body_bytes: usize,
}

impl Service<hyper::Request<Incoming>> for PeerService {
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The router is always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: hyper::Request<Incoming>) -> Self::Future {
        run_reactor_hook();
        req.extensions_mut().insert(ConnectInfo(self.peer));
        // The console reads no body at all, but a request that *declares* one
        // over the limit is refused at the edge: the body is never buffered
        // and never read.
        if let Some(declared) = req
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            && declared > self.max_body_bytes
        {
            let limit = self.max_body_bytes;
            return Box::pin(async move {
                Ok(api::json_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("the request body is over the console's limit ({limit} bytes)"),
                ))
            });
        }
        let mut router = self.router.clone();
        Box::pin(async move { router.call(req).await })
    }
}

/// A running console's handle.
pub struct UiHandle {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl UiHandle {
    /// The address the console listens on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Ask the accept loop to stop.
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Stop the console and join its thread.
    pub fn join(mut self) {
        self.shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for UiHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
