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
//! **No authentication and no authorization: anyone who can open a TCP
//! connection to the port gets the full SQL surface of the served database.**
//! The optional `tls` feature (off by default) adds a rustls-encrypted
//! transport — see [`tls`] — without changing the protocol or the default
//! build: it encrypts the transport (and, with a trusted anchor, lets a
//! client verify *which* server it reached), but it does not identify or
//! restrict the client. The async transport carries the same posture.
//! Bind localhost (the default) and treat the port as fully trusted; see the
//! README's "Network server" section.
pub mod client;
pub mod protocol;
pub mod server;
#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "async-server")]
pub mod async_server;

pub use client::{Client, ClientError};
pub use protocol::{
    DoneKind, MAX_FRAME_SIZE, PROTOCOL_VERSION, ProtocolError, ROWS_PER_FRAME, Request, Response,
    decode_request, decode_response, encode_request, encode_response, read_request, read_response,
    write_request, write_response,
};
pub use server::{DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS, Server, ServerHandle};

#[cfg(feature = "async-server")]
pub use async_server::{AsyncServer, AsyncServerHandle};
#[cfg(feature = "tls")]
pub use tls::{ClientTrust, TlsConfigError};
