//! The serve entry point (ABI minor 2) from the extension side.
//!
//! An extension that speaks a client protocol (MCP) exports
//! `pinkdb_ext_serve`; the host calls it and blocks until the loop returns.
//! Everything the extension may ask of the host goes through
//! [`HostSession::execute_statement`] — SQL text in, already-formatted text
//! out. The extension never holds a pointer to an engine structure.

use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pinkdb_ext_loader::{PinkdbExtExecuteStatementFn, PinkdbHostApi};

/// The host's answer to one [`HostSession::execute_statement`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementReply {
    /// `0` success, `1` statement error (the text is the `pinkdb::agent` JSON
    /// error line), any other code: no session.
    pub code: i32,
    /// Markdown result or JSON error line; empty when the host produced no
    /// text.
    pub text: String,
}

impl StatementReply {
    pub fn is_ok(&self) -> bool {
        self.code == 0
    }

    pub fn is_error(&self) -> bool {
        self.code == 1
    }
}

/// The host callback bundle, valid for the duration of one serve call.
pub struct HostSession {
    api: PinkdbHostApi,
    ctx: *mut std::ffi::c_void,
}

impl HostSession {
    /// Run one statement in the host (read-only policy, limits and roles
    /// applied there). Never panics: a host that cannot answer yields code 2
    /// with empty text.
    pub fn execute_statement(&mut self, sql: &str) -> StatementReply {
        // The field is appended; an older host's struct ends before it.
        let field_end = std::mem::offset_of!(PinkdbHostApi, execute_statement)
            + std::mem::size_of::<PinkdbExtExecuteStatementFn>();
        if (self.api.struct_size as usize) < field_end {
            return StatementReply {
                code: 2,
                text: String::new(),
            };
        }
        let mut out: *mut c_char = std::ptr::null_mut();
        let mut len = 0usize;
        let code = (self.api.execute_statement)(
            self.ctx,
            sql.as_ptr().cast::<c_char>(),
            sql.len(),
            &mut out,
            &mut len,
        );
        let text = if out.is_null() {
            String::new()
        } else {
            // SAFETY: the host returns `len` bytes it allocated; they are
            // copied out before the host owns the block again.
            let bytes = unsafe { std::slice::from_raw_parts(out.cast::<u8>(), len) };
            let text = String::from_utf8_lossy(bytes).into_owned();
            // SAFETY: alloc/free pairing per the ABI — the host allocated it.
            (self.api.free)(out.cast::<std::ffi::c_void>());
            text
        };
        StatementReply { code, text }
    }
}

/// The body of a generated `pinkdb_ext_serve` export.
///
/// # Safety
///
/// `api`/`host_ctx` must be the pair the host passed, and the two pointers
/// must be NUL-terminated strings (or NULL) per the ABI.
pub unsafe fn native_serve<F>(
    api: *const PinkdbHostApi,
    host_ctx: *mut std::ffi::c_void,
    transport: *const c_char,
    options: *const c_char,
    serve: F,
) -> i32
where
    F: Fn(&mut HostSession, &str, &str) -> Result<(), String>,
{
    if api.is_null() {
        return 2;
    }
    // SAFETY: the host passes a live, initialized struct for the call.
    let api = unsafe { *api };
    // SAFETY: both are NUL-terminated strings per the ABI; NULL maps to "".
    let transport = unsafe { cstr(transport) };
    let options = unsafe { cstr(options) };
    let mut session = HostSession { api, ctx: host_ctx };
    match catch_unwind(AssertUnwindSafe(|| serve(&mut session, transport, options))) {
        Ok(Ok(())) => 0,
        Ok(Err(message)) => {
            eprintln!("pinkdb extension serve failed: {message}");
            1
        }
        Err(_) => {
            eprintln!("pinkdb extension serve panicked");
            2
        }
    }
}

/// Decode one optional C string. The ABI guarantees NUL termination; invalid
/// UTF-8 is replaced rather than refused (the host chose the bytes).
unsafe fn cstr<'a>(ptr: *const c_char) -> &'a str {
    if ptr.is_null() {
        return "";
    }
    // SAFETY: the caller guarantees NUL termination.
    unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .unwrap_or("")
}
