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
use axum::middleware;
use axum::response::Response;
use axum::routing::{get, post};
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
use crate::ask;
use crate::assets;
use crate::auth::AuthState;
use crate::book::Book;
use crate::policy::{ConsoleTls, UiAuth, UiError, check_auth_policy, check_bind_policy};
use crate::security;
use crate::sessions::Sessions;
use crate::store::Session;
use crate::work;

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
    /// `--ui-read-only`: worksheets run only queries (`SELECT`, `EXPLAIN`,
    /// `DESCRIBE`, `SHOW`, `USE`) for everyone, whatever their grants, and
    /// the agent never writes.
    pub read_only: bool,
    /// The per-statement deadline of a worksheet statement.
    pub query_timeout: Duration,
    /// The bytes a worksheet statement may materialize (the engine's cap; a
    /// worksheet has no row cap, the grid shows [`Self::max_shown_rows`]).
    pub query_max_bytes: usize,
    /// The rows of one result the browser gets; the rest is counted, and
    /// [`Self::max_export_rows`] bounds a download.
    pub max_shown_rows: usize,
    /// The rows an export (CSV/JSON download) may hold.
    pub max_export_rows: usize,
    /// Worksheet runs one session may have running at once.
    pub session_queries: usize,
    /// A session ends after this long without a request.
    pub session_idle: Duration,
    /// A session ends this long after its login in any case.
    pub session_lifetime: Duration,
    /// The most live sessions; the least recently used one makes room.
    pub max_sessions: usize,
    /// The deadline of a connection that runs worksheet SQL or an agent run
    /// (the whole connection: request, work, response). Every other
    /// connection has [`Self::request_timeout`].
    pub long_request_timeout: Duration,
    /// Extra `Host` names the console answers to (a reverse proxy's name).
    /// Loopback names and the bind address are always allowed.
    pub allowed_hosts: Vec<String>,
    /// Mark the session cookie `Secure` (set when the console is served
    /// over TLS).
    pub secure_cookies: bool,
    /// Let worksheet statements call host scalars that reach an external
    /// service (wave 107c's `embed()`). Off by default.
    pub allow_external_services: bool,
    /// `--agent-allow-writes`: the agent's statements may write (still under
    /// the user's grants, never the transcript database, and never with
    /// [`Self::read_only`]).
    pub agent_allow_writes: bool,
}

impl Default for UiOptions {
    fn default() -> Self {
        Self {
            workers: 4,
            max_blocking_threads: 8,
            max_connections: 64,
            read_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_body_bytes: 1024 * 1024,
            max_page_size: 500,
            statement_timeout: Duration::from_secs(30),
            max_result_rows: 1_000,
            max_result_bytes: 16 * 1024 * 1024,
            auth: UiAuth::None,
            read_only: false,
            query_timeout: Duration::from_secs(300),
            query_max_bytes: 512 * 1024 * 1024,
            max_shown_rows: 10_000,
            max_export_rows: 1_000_000,
            session_queries: 2,
            session_idle: Duration::from_secs(30 * 60),
            session_lifetime: Duration::from_secs(12 * 60 * 60),
            max_sessions: 256,
            long_request_timeout: Duration::from_secs(15 * 60),
            allowed_hosts: Vec::new(),
            secure_cookies: false,
            allow_external_services: false,
            agent_allow_writes: false,
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
    /// The pooled browsing connections (each remembers the database it
    /// browses).
    pool: Mutex<Vec<Session>>,
    /// The login sessions (wave 121).
    pub sessions: Sessions,
    /// Worksheet and history storage.
    pub book: Book,
    /// The last provider list the agent answered (it cannot answer while a
    /// run holds it).
    pub agent_providers: Mutex<Option<serde_json::Value>>,
    /// The address the console is bound to (the `Host` check allows it).
    bind: SocketAddr,
}

impl Shared {
    fn new(db: Database, opts: UiOptions, bind: SocketAddr) -> Self {
        let exec_limits = opts.exec_limits();
        let auth = AuthState::new(opts.auth.clone());
        let sessions = Sessions::new(opts.session_idle, opts.session_lifetime, opts.max_sessions);
        let book = Book::new(db.clone());
        Self {
            db,
            opts,
            auth,
            exec_limits,
            pool: Mutex::new(Vec::new()),
            sessions,
            book,
            agent_providers: Mutex::new(None),
            bind,
        }
    }

    /// The IP address the console is bound to.
    pub(crate) fn bind_ip(&self) -> std::net::IpAddr {
        self.bind.ip()
    }

    /// Take a session from the pool (or open a new one).
    pub(crate) fn take_session(&self) -> Session {
        let mut pool = self
            .pool
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
            .pool
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
        let shared = Arc::new(Shared::new(db, opts, addr));
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
        request_timeout: opts.request_timeout,
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
    // cannot pin the connection. A connection that runs worksheet SQL or an
    // agent run gets the long deadline; [`PeerService`] holds every other
    // request to `request_timeout` itself.
    let _ = tokio::time::timeout(
        opts.request_timeout.max(opts.long_request_timeout),
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
/// The wave-78 browsing routes are `GET` (and, through axum, `HEAD`). The
/// worksheet routes (wave 121) that change anything — log in, run SQL, save a
/// worksheet, ask the agent — are `POST`s that need the session cookie, the
/// CSRF token and a same-origin `Origin` (see [`crate::security`]). Every
/// request's `Host` is checked and every response carries the security
/// headers. A method a path does not implement is a `405`.
fn build_router(shared: Arc<Shared>) -> Router {
    Router::new()
        .route(
            "/api/session",
            get(work::session).fallback(method_not_allowed),
        )
        .route("/api/login", post(work::login).fallback(method_not_allowed))
        .route(
            "/api/logout",
            post(work::logout).fallback(method_not_allowed),
        )
        .route("/api/query", post(work::query).fallback(method_not_allowed))
        .route(
            "/api/cancel",
            post(work::cancel).fallback(method_not_allowed),
        )
        .route(
            "/api/export",
            post(work::export).fallback(method_not_allowed),
        )
        .route(
            "/api/worksheets",
            get(work::worksheets)
                .post(work::save_worksheet)
                .fallback(method_not_allowed),
        )
        .route(
            "/api/worksheets/delete",
            post(work::delete_worksheet).fallback(method_not_allowed),
        )
        .route(
            "/api/history",
            get(work::history).fallback(method_not_allowed),
        )
        .route(
            "/api/catalog",
            get(work::catalog).fallback(method_not_allowed),
        )
        .route("/api/agent", get(ask::info).fallback(method_not_allowed))
        .route("/api/ask", post(ask::ask).fallback(method_not_allowed))
        .route("/", get(api::shell).fallback(method_not_allowed))
        .route("/preview", get(api::preview).fallback(method_not_allowed))
        .route("/assets/console.css", get(assets::css))
        .route("/assets/boot.js", get(assets::boot))
        .route("/assets/pinkdb-console.js", get(assets::app_js))
        .route("/assets/pinkdb-console_bg.wasm", get(assets::app_wasm))
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
        .layer(middleware::from_fn_with_state(
            Arc::clone(&shared),
            security::check_host,
        ))
        .layer(middleware::from_fn(security::security_headers))
        .with_state(shared)
}

/// The response for a method the console does not implement.
async fn method_not_allowed() -> Response {
    api::json_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "method not allowed on this path (state-changing requests are POSTs to /api/…)",
    )
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
    request_timeout: Duration,
}

/// The routes that may run longer than `request_timeout`: worksheet SQL, an
/// export and an agent run (each bounded by its own statement deadlines).
fn long_route(path: &str) -> bool {
    matches!(path, "/api/query" | "/api/export" | "/api/ask")
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
        if long_route(req.uri().path()) {
            return Box::pin(async move { router.call(req).await });
        }
        // Everything else answers within `request_timeout`, as in wave 78.
        let deadline = self.request_timeout;
        Box::pin(async move {
            match tokio::time::timeout(deadline, router.call(req)).await {
                Ok(response) => response,
                Err(_) => Ok(api::json_error(
                    StatusCode::REQUEST_TIMEOUT,
                    "the request took longer than the console's request timeout",
                )),
            }
        })
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
