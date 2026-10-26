//! The MCP bridge: the shared protocol over one host session, serving the
//! warehouse tools and the memory tools.
//!
//! Two channels, two policies: a tool call's statements run on the public
//! channel (read-only unless the host enabled writes, and never able to read
//! `pinkdb_memory` directly), while memory's own statements run on the
//! internal channel, confined to `pinkdb_memory` by the host's policy. That
//! split is what makes the visibility filter unbypassable: the client can ask
//! memory, and cannot read it.

use pinkdb_ext_sdk::{HostSession, StatementReply};
use pinkdb_mcp_protocol::Server;

use crate::tools::Memory;

/// `pinkdb_ext_serve`: speak MCP until the client disconnects. `options` is
/// `allow-writes`, `allow-files` and `allow-memory`, as the host enabled them.
pub fn run(session: &mut HostSession, transport: &str, options: &str) -> Result<(), String> {
    let enabled = |flag: &str| options.split_whitespace().any(|option| option == flag);
    let mut server = Server::new(
        ServeHost { session },
        Box::new(Memory {
            allow_memory: enabled("allow-memory"),
        }),
        enabled("allow-writes"),
        enabled("allow-files"),
    );
    pinkdb_mcp_protocol::transport::serve(&mut server, transport)
}

/// The host's callback as the protocol's [`Host`]. Both channels exist here:
/// the internal one is the extension's own, and the host refuses it when the
/// policy has no internal channel (code 2).
///
/// [`Host`]: pinkdb_mcp_protocol::Host
struct ServeHost<'a> {
    session: &'a mut HostSession,
}

impl pinkdb_mcp_protocol::Host for ServeHost<'_> {
    fn execute(&mut self, statement: &str) -> StatementReply {
        self.session.execute_statement(statement)
    }

    fn execute_internal(&mut self, statement: &str) -> StatementReply {
        self.session.execute_internal(statement)
    }
}
