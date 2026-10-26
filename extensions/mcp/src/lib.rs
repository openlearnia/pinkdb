//! pinkdb's **MCP** extension (wave 105): expose a store to a Model Context
//! Protocol client — pi, Claude Code, fx, any MCP client.
//!
//! The client brings the reasoning; there is no model code here and none in
//! pinkdb. This crate wires the shared protocol ([`pinkdb_mcp_protocol`]) to
//! one host session, so a tool call becomes one statement for the host, which
//! runs it under its own budgets and answers with `pinkdb::agent` Markdown (or
//! a one-line JSON error). Nothing in this crate decides what a client may
//! run: the host's statement callback applies the read-only policy, the role
//! grants and the time/row limits.
//!
//! The wire (JSON-RPC 2.0, `tools/list`, `tools/call`, stdio and loopback
//! HTTP) and the tool table live in `pinkdb-mcp-protocol` and
//! `pinkdb-warehouse-tools`, because agent memory serves the same surface
//! from its own extension.
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

pub use pinkdb_mcp_protocol::{Host, Server, ToolReply, ToolSet, bind_address, is_loopback};
pub use pinkdb_warehouse_tools as tools;
pub use pinkdb_warehouse_tools::tool_names;

use pinkdb_ext_sdk::{HostSession, StatementReply};

pinkdb_ext_sdk::pinkdb_extension! {
    name = "mcp",
    functions = [],
}

/// The host's callback as this crate's [`Host`]. Only the public channel: the
/// MCP bridge has no bookkeeping of its own, so it also has no internal one.
struct SessionHost<'a> {
    session: &'a mut HostSession,
}

impl Host for SessionHost<'_> {
    fn execute(&mut self, statement: &str) -> StatementReply {
        self.session.execute_statement(statement)
    }
}

/// The dispatcher over `host` and the warehouse tool table, with the write
/// policy `initialize` reports (the host enforces it, not this crate).
pub fn server<H: Host>(host: H, allow_writes: bool, allow_files: bool) -> Server<H> {
    Server::new(
        host,
        Box::new(pinkdb_warehouse_tools::Warehouse),
        allow_writes,
        allow_files,
    )
}

/// `pinkdb_ext_serve`: run the protocol loop until the client disconnects.
/// `options` is `allow-writes` when the host enabled writes — reported to the
/// client in `initialize`, never trusted for enforcement.
fn serve(session: &mut HostSession, transport: &str, options: &str) -> Result<(), String> {
    let enabled = |flag: &str| options.split_whitespace().any(|option| option == flag);
    let mut server = server(
        SessionHost { session },
        enabled("allow-writes"),
        enabled("allow-files"),
    );
    pinkdb_mcp_protocol::transport::serve(&mut server, transport)
}

pinkdb_ext_sdk::pinkdb_abi_exports_with_serve!(serve);
