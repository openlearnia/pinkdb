//! pinkdb's **agent memory** extension (wave 109): long-term memory for an
//! agent, stored in pinkdb, in four layers.
//!
//! - **L0 conversation** — turns as they happen ([`store::add_turn`]).
//! - **L1 atoms** — small standalone facts distilled from L0, with a source,
//!   a confidence and supersession ([`store::add_atom`]).
//! - **L2 scenarios** — project knowledge grouping related atoms.
//! - **L3 persona** — one long-term profile per principal.
//!
//! Distillation (L0 → L1 → L2 → L3) is a queued model call through the shared
//! `pinkdb-llm` client; retrieval is layered and budgeted (persona and
//! scenarios first, then atoms and turns ranked by BM25 fused with vector
//! distance). Who may see a row is decided on every read from
//! `current_user()`/`current_roles()` — the engine's users and roles — so two
//! people sharing a store do not read each other's memory.
//!
//! # Surfaces
//!
//! ```text
//! LOAD EXTENSION '<install>/lib/libpinkdb_memory.dylib';
//! .memory add the warehouse is TPC-H SF1 at scale 1
//! .memory search how big is the warehouse
//! .memory distill                        -- needs a provider, like .agent
//! .memory persona
//! .memory show
//! .memory forget <atom>
//! .memory serve stdio allow-memory       -- MCP: warehouse + memory_* tools
//! ```
//!
//! There is no default provider: `distill` without one is one clear error,
//! while `add` and `search` keep working.
//!
//! # Why a served protocol and a command, and no SQL functions
//!
//! Row visibility is a filter the engine does not do (it has grants, not row
//! policies), and a scalar extension function never sees the store. So every
//! memory read runs through this extension on its internal channel, and the
//! tables are never granted to a non-admin: a direct `SELECT` is refused by
//! the engine. The dot command and the MCP bridge are the two surfaces; both
//! call the same [`ops`].

pub mod command;
pub mod distill;
pub mod host;
pub mod ops;
pub mod search;
pub mod serve;
pub mod store;
pub mod tools;

pub use command::USAGE;
pub use host::{ANSWER, Caller, DIAGNOSTIC, Host};
pub use store::DATABASE;
pub use tools::{Memory, READ_TOOLS, TOOLS, WRITE_TOOLS};

pinkdb_ext_sdk::pinkdb_extension! {
    name = "memory",
    functions = [],
}

fn command_entry(session: &mut pinkdb_ext_sdk::CommandSession, args: &str) -> i32 {
    command::run(session, args)
}

fn serve_entry(
    session: &mut pinkdb_ext_sdk::HostSession,
    transport: &str,
    options: &str,
) -> Result<(), String> {
    serve::run(session, transport, options)
}

pinkdb_ext_sdk::pinkdb_abi_exports_with_serve_and_command!(serve_entry, command_entry);
