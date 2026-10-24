//! pinkdb's **MCP** extension (wave 105): expose a store to a Model Context
//! Protocol client — pi, Claude Code, fx, any MCP client.
//!
//! The client brings the reasoning; there is no model code here and none in
//! pinkdb. This crate speaks JSON-RPC 2.0, turns a tool call into one
//! statement, and hands the statement to the host, which runs it under its
//! own budgets and answers with `pinkdb::agent` Markdown (or a one-line JSON
//! error). Nothing in this crate decides what a client may run: the host's
//! statement callback applies the read-only policy, the role grants and the
//! time/row limits.
//!
//! # Loading
//!
//! ```text
//! LOAD EXTENSION '<install>/lib/libpinkdb_mcp.dylib';
//! .mcp serve stdio                -- serve one client on this terminal's pipes
//! .mcp serve http 7801            -- serve on 127.0.0.1:7801 only
//! .mcp serve stdio allow-writes   -- let the client change the store
//! ```
//!
//! `pinkdb --mcp store.pink` is the one-flag form the pi package
//! (`integrations/pi-pinkdb`) uses: it loads this extension from the install
//! directory and serves stdio.
//!
//! # Shape
//!
//! - [`protocol`]: the JSON-RPC/MCP dispatcher and the [`Host`] it calls.
//! - [`tools`]: the tool schemas a model reads and the statement each one
//!   sends.
//! - [`http`]: the localhost-only streamable-HTTP transport.
//! - [`transport`]: stdio or HTTP, chosen by the descriptor the host passed to
//!   `pinkdb_ext_serve` (`stdio` or `http:HOST:PORT`).

mod http;
mod protocol;
mod tools;
mod transport;

pub use http::{bind_address, is_loopback};
pub use protocol::{Host, Server};
pub use tools::tool_names;

use pinkdb_ext_sdk::HostSession;

pinkdb_ext_sdk::pinkdb_extension! {
    name = "mcp",
    functions = [],
}

/// The host's callback as this crate's [`Host`].
struct SessionHost<'a> {
    session: &'a mut HostSession,
}

impl Host for SessionHost<'_> {
    fn execute(&mut self, statement: &str) -> pinkdb_ext_sdk::StatementReply {
        self.session.execute_statement(statement)
    }
}

/// `pinkdb_ext_serve`: run the protocol loop until the client disconnects.
/// `options` is `allow-writes` when the host enabled writes — reported to the
/// client in `initialize`, never trusted for enforcement.
fn serve(session: &mut HostSession, transport: &str, options: &str) -> Result<(), String> {
    let enabled = |flag: &str| options.split_whitespace().any(|option| option == flag);
    let mut server = Server::new(
        SessionHost { session },
        enabled("allow-writes"),
        enabled("allow-files"),
    );
    transport::serve(&mut server, transport)
}

pinkdb_ext_sdk::pinkdb_abi_exports_with_serve!(serve);
