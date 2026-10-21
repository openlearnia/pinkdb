//! pinkdb network server — a separate crate so the core stays lean.
//!
//! - [`protocol`]: hand-rolled binary framing + message codec (no serde);
//! - [`Server`]: TCP server over a pinkdb [`pinkdb::Database`], one std
//!   thread per connection, one `Connection` per client (per-connection
//!   `USE` sessions), zero async;
//! - [`Client`]: blocking client that reconstructs pinkdb's public
//!   `QueryResult`/`Value`/`DataType` values.
//!
//! No `unsafe`, no async in the default build. With the optional
//! `async-server` feature, `AsyncServer` is the same server on a tokio
//! transport (see the carve-out below).
//!
//! # Async carve-out (wave 47)
//!
//! The workspace's design rule is **zero async**: the engine, planner,
//! storage, and the default server are synchronous, and the default build
//! pulls no async runtime. Wave 47 carves out exactly one place, and this
//! crate documents it:
//!
//! 1. **Async lives at the I/O boundary of the server crate only** — and only
//!    in a build that opts in with the `async-server` cargo feature
//!    (`crates/pinkdb-server/src/async_server.rs`). A tokio listener accepts
//!    sockets, and one tokio task per connection does frame I/O and idle
//!    timeouts. Nothing else is async: there is no async storage layer and no
//!    async WAL, and pink-lake's blocking `Storage` trait contract is
//!    untouched (that crate is not involved at all).
//! 2. **The executor/engine stay synchronous.** Every engine call runs on a
//!    blocking-pool thread via `spawn_blocking` with the same 16 MiB stack
//!    size the blocking server gives connection threads; the connection task
//!    owns the `pinkdb::Connection` between calls and no engine code ever
//!    awaits or holds an async lock.
//! 3. **The default build gains no async runtime.** `tokio` is an *optional*
//!    dependency, enabled only by `async-server`:
//!
//!    ```text
//!    cargo tree -p pinkdb-server                    # no tokio
//!    cargo tree -p pinkdb-server --features async-server   # tokio appears
//!    ```
//!
//! 4. **The wire protocol is unchanged.** The async transport reuses the same
//!    codec and the same result-streaming code as the blocking server, and a
//!    byte-parity test (`tests/async_server.rs`) pins identical frames for
//!    identical requests. Existing clients connect to either transport
//!    unchanged.
//!
//! Scope limits, stated honestly: the async transport adds no async storage,
//! no `async fn` engine API, and no protocol change; it is a transport
//! variant, not an async rewrite.
//!
//! # Security posture
//!
//! **Authentication and authorization are opt-in per store (wave 48).** The
//! served [`pinkdb::Database`]'s user catalog is the switch:
//!
//! - **No users defined** — every connection is trusted and gets the full SQL
//!   surface of the served database (the pre-wave-48 behavior). The server
//!   prints a loud warning at startup in this state, and any client can
//!   create the first user over the wire (`CREATE USER … PASSWORD …`, then
//!   `GRANT ADMIN …` to it).
//! - **At least one user defined** — a connection must send a
//!   `Request::Login` (user name + password) before any statement request is
//!   accepted; an unauthenticated `Execute`/`Prepare`/`ExecutePrepared` gets
//!   an `Error` frame (`authentication required: …`), and `Ping` keeps
//!   working. After a successful login the connection runs as that user and
//!   the engine checks every statement against its grants (`permission
//!   denied: …` for a missing privilege). The gate is evaluated per
//!   statement — inside a multi-statement `Execute` batch and at prepared-
//!   statement execution, against the store's *current* user catalog — so a
//!   connection that creates the first user must log in before its next
//!   statement, even when both statements arrive in one batch. Emptying the
//!   catalog does not promote a session that logged in earlier: an
//!   authenticated principal is always checked and an empty catalog simply
//!   grants nothing.
//! - **The embedded API is trusted by construction.** `pinkdb::Database`
//!   handles, the C ABI crate and the wasm bindings carry no principal, so
//!   they run every statement unchecked — exactly as before this wave. Only
//!   [`pinkdb::Connection::login`] (through `Request::Login` here) puts a
//!   session under privilege checks; the server additionally arms
//!   [`pinkdb::Connection::require_login`] on every network connection, which
//!   is what re-evaluates the store's user catalog before each statement.
//!
//! Passwords are hashed with Argon2id when the DDL runs; neither the `.pink`
//! catalog nor the WAL ever holds the plaintext. Transport encryption is a
//! separate, also-optional concern — the `tls` feature (off by default) adds
//! a rustls-encrypted transport; it identifies the *server* to the client,
//! not the client to the server, so it complements the login rather than
//! replacing it. HTTP-style trust decisions (bind localhost, treat the port
//! as trusted) still apply; see the README's "Network server" section.
//! The async transport carries the same posture (and the same login path), but
//! it has no TLS path: the binary refuses `--tls-cert`/`--tls-key` together
//! with `--transport async` instead of serving plaintext.
//!
//! # The warehouse browsing UI (wave 78)
//!
//! With the `ui` cargo feature (off by default) the server can also serve a
//! **read-only web console** — a browser UI with the store's catalog, paged
//! row previews and statistics. The console lives in the sibling crate
//! [`pinkdb_server_ui`](https://docs.rs/pinkdb-server-ui) and is re-exported
//! by the [`ui`] module; the binary turns it on with `--enable-ui`.
//!
//! The console is **read-only by construction** (no DML, no DDL, no
//! mutating route), it binds **loopback only** (a non-loopback bind is
//! refused: the console has no TLS of its own), and a store that has users
//! is never served unauthenticated (the server refuses to start without
//! `--ui-auth basic` or `--ui-token`). It owns its own tokio runtime on its
//! own thread, so it works with either transport, and every synchronous
//! engine call runs on a bounded blocking pool. See `docs/server-ui.md`.
pub mod client;
pub mod protocol;
pub mod server;
#[cfg(feature = "tls")]
pub mod tls;

/// The warehouse browsing UI (wave 78): the read-only web console the
/// server can serve next to its binary protocol. Only in a build with the
/// `ui` cargo feature (off by default); see `docs/server-ui.md`.
#[cfg(feature = "ui")]
pub mod ui;

#[cfg(feature = "async-server")]
pub mod async_server;

pub use client::{Client, ClientError};
pub use protocol::{
    DoneKind, MAX_FRAME_SIZE, PROTOCOL_VERSION, ProtocolError, ROWS_PER_FRAME, Request, Response,
    decode_request, decode_response, encode_request, encode_response, read_request, read_response,
    write_request, write_response,
};
pub use server::{
    DEFAULT_IDLE_TIMEOUT, DEFAULT_LOGIN_MAX_FAILURES, DEFAULT_MAX_CONCURRENT_LOGINS,
    DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS,
    DEFAULT_STATEMENT_TIMEOUT_MS, Server, ServerHandle,
};

#[cfg(feature = "async-server")]
pub use async_server::{AsyncServer, AsyncServerHandle};
#[cfg(feature = "tls")]
pub use tls::{ClientTrust, TlsConfigError};
