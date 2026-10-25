//! What the harness needs from the host, in the two channels the ABI gives it.
//!
//! * [`Host::internal`] is the extension's own bookkeeping channel: the host's
//!   internal policy confines its writes to the agent database and answers
//!   with machine-readable rows.
//! * [`Host::tool`] is where the model's tool calls run: the normal statement
//!   policy (read-only unless the run was started with `allow-writes`), with
//!   Markdown replies and the agent's own tables refused.
//! * [`Host::emit`] streams text to the user (channel 0) and diagnostics to
//!   stderr (channel 1).

pub use pinkdb_ext_sdk::CommandSession;

pub const ANSWER: i32 = 0;
pub const DIAGNOSTIC: i32 = 1;

pub trait Host {
    /// Run a statement batch on the internal channel (JSON rows).
    fn internal(&mut self, sql: &str) -> Result<String, String>;

    /// Run a statement on the model's channel (`(code, markdown)`).
    fn tool(&mut self, sql: &str) -> (i32, String);

    /// Send text to the user or to the diagnostic stream.
    fn emit(&mut self, channel: i32, text: &str);
}

impl Host for CommandSession {
    fn internal(&mut self, sql: &str) -> Result<String, String> {
        CommandSession::execute_internal(self, sql)
    }

    fn tool(&mut self, sql: &str) -> (i32, String) {
        let reply = pinkdb_ext_sdk::CommandSession::execute_statement(self, sql);
        (reply.code, reply.text)
    }

    fn emit(&mut self, channel: i32, text: &str) {
        CommandSession::emit(self, channel, text);
    }
}
