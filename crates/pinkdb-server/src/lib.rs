//! pinkdb network server — a separate crate so the core stays lean.
//!
//! - [`protocol`]: hand-rolled binary framing + message codec (no serde);
//! - [`Server`]: TCP server over a pinkdb [`pinkdb::Database`], one std
//!   thread per connection, one `Connection` per client (per-connection
//!   `USE` sessions), zero async;
//! - [`Client`]: blocking client that reconstructs pinkdb's public
//!   `QueryResult`/`Value`/`DataType` values.
//!
//! No external dependencies, no `unsafe`, no async.
//!
//! # Security posture
//!
//! **No authentication and no TLS in this wave.** Anyone who can open a TCP
//! connection to the port gets the full SQL surface of the served database.
//! Bind localhost (the default) and treat the port as fully trusted; see the
//! README's "Network server" section.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::{Client, ClientError};
pub use protocol::{
    decode_request, decode_response, encode_request, encode_response, read_request, read_response,
    write_request, write_response, DoneKind, ProtocolError, Request, Response, MAX_FRAME_SIZE,
    PROTOCOL_VERSION, ROWS_PER_FRAME,
};
pub use server::{Server, ServerHandle, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS};
