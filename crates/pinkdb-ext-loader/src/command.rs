//! The optional command entry point (ABI minor 4): a host dot command hands
//! control to an extension that streams text back.
//!
//! Like [`crate::serve`], only text crosses the boundary — a command string
//! in, streamed text out, plus two statement channels. The host keeps the
//! library mapped for the whole call and allows one call at a time per
//! library, so a command can never run against code another load replaced.
//!
//! The agent extension (wave 106) is the first user: `.agent ask "…"` is a
//! command, and the model's tool calls reach the store through
//! [`CommandHost::execute_statement`] (tool policy, Markdown) while the
//! harness's own session tables go through [`CommandHost::execute_internal`]
//! (its internal policy, JSON). Neither channel is a data path the extension
//! can widen: both are the host's callbacks.

use std::ffi::{CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Mutex, OnceLock};

use crate::{ExtError, LoadedExtension};

/// What a command extension can ask the host to do.
///
/// Implemented by the host (`pinkdb`'s CLI glue) so the read-only policy,
/// role checks and time/row budgets are applied on the host side: the
/// extension never decides whether a statement may run.
pub trait CommandHost {
    /// Run one statement for a *model tool call*. Returns `(0, markdown)` on
    /// success and `(1, error_json)` when the statement failed; any other
    /// code means the host has no live session.
    fn execute_statement(&mut self, sql: &str) -> (i32, String);

    /// Run one statement for the extension's *own bookkeeping*: the same
    /// policy checker, but writes are confined to the database the host keeps
    /// for this extension. Answers `(0, json)` on success, where `json` is an
    /// array of `{"columns":[…],"rows":[[…],…]}` results.
    fn execute_internal(&mut self, sql: &str) -> (i32, String);

    /// Stream one chunk of output. `channel` 0 is answer/progress text (the
    /// host's stdout), 1 a diagnostic (its stderr).
    fn emit(&mut self, channel: i32, text: &str);
}

struct CommandState<'a> {
    host: &'a mut dyn CommandHost,
}

/// Paths with a serve loop or a command running right now. One host-driven
/// call at a time per library: a second one would race the same code.
static ACTIVE: OnceLock<Mutex<std::collections::HashSet<std::path::PathBuf>>> = OnceLock::new();

fn active() -> &'static Mutex<std::collections::HashSet<std::path::PathBuf>> {
    ACTIVE.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

/// Mark `path` as running a host-driven call; `false` when it already is.
pub(crate) fn pin(path: &std::path::Path) -> bool {
    let mut guard = active().lock().unwrap_or_else(|p| p.into_inner());
    guard.insert(path.to_path_buf())
}

/// Clear the mark [`pin`] set.
pub(crate) fn unpin(path: &std::path::Path) {
    active()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(path);
}

/// Whether a serve loop or a command is running for the library at `path`.
pub fn is_active(path: &std::path::Path) -> bool {
    let guard = active().lock().unwrap_or_else(|p| p.into_inner());
    guard.contains(path)
}

/// Call `ext`'s `pinkdb_ext_command` and block until it returns.
///
/// `args` is the text after the command name (the extension parses it);
/// `host` receives every statement and every streamed chunk. The extension
/// reports its own failures through the sink, so a non-zero return code means
/// "the command failed" without further detail.
pub fn run_command(
    ext: &LoadedExtension,
    args: &str,
    host: &mut dyn CommandHost,
) -> Result<(), ExtError> {
    let path = ext.canonical_path();
    let command_fn = ext.command_fn().ok_or_else(|| ExtError::NoCommand {
        path: ext.path().to_string(),
    })?;
    if !pin(&path) {
        return Err(ExtError::Serving {
            path: ext.path().to_string(),
        });
    }
    let result = command_locked(ext, command_fn, args, host);
    unpin(&path);
    result
}

fn command_locked(
    ext: &LoadedExtension,
    command_fn: crate::abi::PinkdbExtCommandFn,
    args: &str,
    host: &mut dyn CommandHost,
) -> Result<(), ExtError> {
    let args = CString::new(args).map_err(|_| ExtError::BadTransport {
        message: "command arguments contain a NUL byte".to_string(),
    })?;
    let mut api = crate::host::host_api();
    api.execute_statement = statement_channel;
    api.execute_internal = internal_channel;
    let mut state = CommandState { host };
    let state_ptr = (&mut state as *mut CommandState<'_>).cast::<c_void>();
    // SAFETY (all of it): `api` is a fully initialized host struct with both
    // statement channels wired, `state` lives for the whole call and is what
    // the channels cast back, and `args` outlives the call.
    // `pinkdb_ext_command` must not unwind (its signature is `extern "C"`;
    // the SDK catches panics inside).
    let code = unsafe { command_fn(&api, state_ptr, args.as_ptr(), Some(emit_chunk), state_ptr) };
    if code == 0 {
        Ok(())
    } else {
        Err(ExtError::CommandFailed {
            path: ext.path().to_string(),
            code,
        })
    }
}

/// Read the `(sql, out)` arguments both statement channels share.
///
/// # Safety
///
/// The ABI requires `sql` to point at `sql_len` readable bytes and the two
/// out pointers to be writable (or NULL-checked here).
pub(crate) unsafe fn read_statement<'a>(
    sql: *const c_char,
    sql_len: usize,
    out_text: *mut *mut c_char,
    out_len: *mut usize,
) -> Option<Option<&'a str>> {
    if out_text.is_null() || out_len.is_null() {
        return None;
    }
    // SAFETY: both out pointers were just NULL-checked and point at the
    // caller's locals, per the ABI.
    unsafe {
        *out_text = std::ptr::null_mut();
        *out_len = 0;
    }
    if sql.is_null() {
        return Some(Some(""));
    }
    // SAFETY: `sql` points at `sql_len` readable bytes by contract.
    let bytes = unsafe { std::slice::from_raw_parts(sql.cast::<u8>(), sql_len) };
    Some(std::str::from_utf8(bytes).ok())
}

/// Write one channel's answer back across the ABI.
///
/// # Safety
///
/// `out_text`/`out_len` must be the pointers [`read_statement`] just cleared.
pub(crate) unsafe fn write_reply(
    (code, text): (i32, String),
    out_text: *mut *mut c_char,
    out_len: *mut usize,
) -> i32 {
    if text.is_empty() {
        return code;
    }
    let ptr = crate::host::alloc_text(text.as_bytes());
    if ptr.is_null() {
        return 2;
    }
    // SAFETY: the out pointers were NULL-checked at entry and the host wrote
    // nothing to them since.
    unsafe {
        *out_text = ptr;
        *out_len = text.len();
    }
    code
}

/// The model-tool channel: tool policy, Markdown results.
extern "C" fn statement_channel(
    ctx: *mut c_void,
    sql: *const c_char,
    sql_len: usize,
    out_text: *mut *mut c_char,
    out_len: *mut usize,
) -> i32 {
    // SAFETY: see `read_statement`.
    let Some(sql) = (unsafe { read_statement(sql, sql_len, out_text, out_len) }) else {
        return 2;
    };
    let Some(sql) = sql else {
        return 1;
    };
    // SAFETY: `ctx` is the `CommandState` this module passed to
    // `pinkdb_ext_command` (the only API instance that carries this callback).
    let Some(state) = (unsafe { ctx.cast::<CommandState<'_>>().as_mut() }) else {
        return 2;
    };
    let reply = catch_unwind(AssertUnwindSafe(|| state.host.execute_statement(sql)))
        .unwrap_or((2, String::new()));
    // SAFETY: the out pointers are the ones `read_statement` cleared.
    unsafe { write_reply(reply, out_text, out_len) }
}

/// The extension's own channel: its internal policy, JSON results.
extern "C" fn internal_channel(
    ctx: *mut c_void,
    sql: *const c_char,
    sql_len: usize,
    out_text: *mut *mut c_char,
    out_len: *mut usize,
) -> i32 {
    // SAFETY: see `read_statement`.
    let Some(sql) = (unsafe { read_statement(sql, sql_len, out_text, out_len) }) else {
        return 2;
    };
    let Some(sql) = sql else {
        return 1;
    };
    // SAFETY: `ctx` is the `CommandState` this module passed to
    // `pinkdb_ext_command`.
    let Some(state) = (unsafe { ctx.cast::<CommandState<'_>>().as_mut() }) else {
        return 2;
    };
    let reply = catch_unwind(AssertUnwindSafe(|| state.host.execute_internal(sql)))
        .unwrap_or((2, String::new()));
    // SAFETY: the out pointers are the ones `read_statement` cleared.
    unsafe { write_reply(reply, out_text, out_len) }
}

/// The output sink: one streamed chunk, never unwinding into the extension.
extern "C" fn emit_chunk(ctx: *mut c_void, channel: i32, text: *const c_char, len: usize) {
    if text.is_null() {
        return;
    }
    // SAFETY: `ctx` is the `CommandState` passed to `pinkdb_ext_command`.
    let Some(state) = (unsafe { ctx.cast::<CommandState<'_>>().as_mut() }) else {
        return;
    };
    // SAFETY: the ABI requires `text` to point at `len` readable bytes.
    let bytes = unsafe { std::slice::from_raw_parts(text.cast::<u8>(), len) };
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let _ = catch_unwind(AssertUnwindSafe(|| state.host.emit(channel, text)));
}
