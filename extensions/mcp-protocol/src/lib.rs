//! The Model Context Protocol wire that pinkdb extensions serve (waves 105
//! and 109).
//!
//! A bridge crate supplies a [`ToolSet`] and a [`Host`]; this crate turns an
//! MCP client's JSON-RPC messages into [`ToolSet`] calls, runs the statements
//! they plan through the host, and answers with the result. Nothing here
//! decides what a client may run — the host's statement callbacks apply the
//! read-only policy, the role grants, the visible-row filter and the
//! time/row limits.
//!
//! - [`protocol`]: the dispatcher, the [`Host`] it calls and the [`ToolSet`]
//!   it serves.
//! - [`transport`]: stdio, or the loopback HTTP transport in [`http`].
//!
//! It is an rlib, never loaded as an extension: the MCP bridge and agent
//! memory each ship their own cdylib and share this code.

pub mod http;
pub mod protocol;
pub mod transport;

pub use http::{bind_address, is_loopback};
pub use protocol::{Host, Server, ToolReply, ToolSet};
