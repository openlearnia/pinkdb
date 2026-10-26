//! The pinkdb data agent.
//!
//! A model answers a question by running the warehouse tool set against the
//! store this session is connected to. Nothing of the harness is linked into
//! `pinkdb` or `pinkdb-server`: the model client, the loop and the prompt live
//! in this extension, which the CLI reaches through the ABI's command entry
//! point (`pinkdb_ext_command`, minor 4).
//!
//! The model client itself is `pinkdb-llm`: the provider table, the two wire
//! protocols (OpenAI chat completions and the Anthropic Messages API) and the
//! provider-neutral transcript. **There is no default provider** — every run
//! names one — and a key variable only ever reaches its own provider.
//!
//! Run state lives in the store, in the `pinkdb_agent` database: the
//! transcript, the tool calls and their replies, checkpoints and usage. That
//! is what makes a killed run resumable, a fork a copy instead of a guess, and
//! a fork onto a different provider a re-encode of the same transcript.
//!
//! The model never gets the API key: the client holds it for the duration of
//! a run and no statement, table, log line or error carries it.

mod command;
mod host;
mod run;
mod store;

pub use command::{DEFAULT_MAX_STEPS, call_tool};
pub use host::{ANSWER, DIAGNOSTIC, Host};
pub use pinkdb_llm::{
    Client, Config, Message, Plan, Protocol, Provider, ToolCall, Turn, Usage, registry,
};
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
