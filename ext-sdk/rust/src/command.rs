//! The command entry point (ABI minor 4) from the extension side.
//!
//! An extension that implements a host dot command (the agent, wave 106)
//! exports `pinkdb_ext_command`; the host calls it with the command's argument
//! text and an output sink. Two statement channels are live for the call:
//! [`CommandSession::execute_statement`] runs a model tool call under the
//! host's tool policy, and [`CommandSession::execute_internal`] runs the
//! extension's own bookkeeping under its internal policy. The extension never
//! holds a pointer to an engine structure, and neither channel lets it widen
//! the policy.

use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pinkdb_ext_loader::{PinkdbExtSinkFn, PinkdbHostApi};

use crate::serve::{StatementReply, call_statement};

/// The host callback bundle, valid for the duration of one command call.
/// The database a command extension keeps its own state in (ABI minor 4).
///
/// The host's internal policy confines `execute_internal` writes to it; the
/// engine's `pinkdb::serve::AGENT_DATABASE` is the same name on the host side.
pub const AGENT_DATABASE: &str = "pinkdb_agent";

pub struct CommandSession {
    api: PinkdbHostApi,
    ctx: *mut std::ffi::c_void,
    sink: Option<PinkdbExtSinkFn>,
    sink_ctx: *mut std::ffi::c_void,
}

impl CommandSession {
    /// Run one statement for a tool call (tool policy, Markdown answer).
    pub fn execute_statement(&mut self, sql: &str) -> StatementReply {
        call_statement(&self.api, self.ctx, sql, false)
    }

    /// Run one statement for this extension's own state (internal policy,
    /// JSON answer).
    ///
    /// `Err` is the host's one-line JSON error text, which is what the caller
    /// should report: it already names the refused statement.
    pub fn execute_internal(&mut self, sql: &str) -> Result<String, String> {
        let reply = call_statement(&self.api, self.ctx, sql, true);
        match reply.code {
            0 => Ok(reply.text),
            // 1 = the statement failed and the text is a JSON error; anything
            // else means the host has no session for this call.
            1 => Err(reply.text),
            _ => Err("the host has no session for this command".to_string()),
        }
    }

    /// Stream one chunk to the host. Channel 0 is the answer (stdout), 1 a
    /// diagnostic (stderr). A host that passed no sink ignores it.
    pub fn emit(&mut self, channel: i32, text: &str) {
        let Some(sink) = self.sink else {
            return;
        };
        sink(
            self.sink_ctx,
            channel,
            text.as_ptr().cast::<c_char>(),
            text.len(),
        );
    }
}

/// The body of a generated `pinkdb_ext_command` export.
///
/// # Safety
///
/// `api`/`host_ctx` must be the pair the host passed, `args` a NUL-terminated
/// string (or NULL), and `sink`/`sink_ctx` the host's output callback pair.
pub unsafe fn native_command<F>(
    api: *const PinkdbHostApi,
    host_ctx: *mut std::ffi::c_void,
    args: *const c_char,
    sink: Option<PinkdbExtSinkFn>,
    sink_ctx: *mut std::ffi::c_void,
    run: F,
) -> i32
where
    F: Fn(&mut CommandSession, &str) -> i32,
{
    if api.is_null() {
        return 2;
    }
    // SAFETY: the host passes a live, initialized struct for the call.
    let api = unsafe { *api };
    // SAFETY: NUL-terminated per the ABI; NULL maps to "".
    let args = unsafe { crate::serve::cstr(args) };
    let mut session = CommandSession {
        api,
        ctx: host_ctx,
        sink,
        sink_ctx,
    };
    match catch_unwind(AssertUnwindSafe(|| run(&mut session, args))) {
        Ok(code) => code,
        Err(_) => {
            eprintln!("pinkdb extension command panicked");
            2
        }
    }
}
