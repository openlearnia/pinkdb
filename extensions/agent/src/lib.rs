//! The pinkdb data agent.
//!
//! An OpenAI-compatible model answers a question by running the warehouse
//! tool set against the store this session is connected to. Nothing of the
//! harness is linked into `pinkdb` or `pinkdb-server`: the model client, the
//! loop and the prompt live in this extension, which the CLI reaches through
//! the ABI's command entry point (`pinkdb_ext_command`, minor 4).
//!
//! Run state lives in the store, in the `pinkdb_agent` database: the
//! transcript, the tool calls and their replies, checkpoints and usage. That
//! is what makes a killed run resumable and a fork a copy instead of a guess.
//!
//! The model never gets the API key: the client holds it for the duration of
//! a run and no statement, table, log line or error carries it.

mod chat;
mod command;
mod host;
mod provider;
mod run;
mod sse;
mod store;

pub use chat::{Message, ToolCall, Turn, Usage};
pub use command::{DEFAULT_MAX_STEPS, call_tool};
pub use host::{ANSWER, DIAGNOSTIC, Host};
pub use provider::{Client, Config, DEFAULT_BASE_URL, DEFAULT_MODEL, KEY_VARIABLES, Provider};
pub use run::{Options, ask, continue_with, fork, resume, system_prompt};
pub use store::DATABASE;

pinkdb_ext_sdk::pinkdb_extension! {
    name = "agent",
    functions = [],
}

pinkdb_ext_sdk::pinkdb_abi_exports_with_command!(command_entry);

/// `pinkdb_ext_command`: `.agent <args>` from the host's statement session.
fn command_entry(session: &mut pinkdb_ext_sdk::CommandSession, args: &str) -> i32 {
    command::run(session, args)
}
