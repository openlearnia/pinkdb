//! The optional serve entry point (ABI minor 2): the host hands control to an
//! extension that speaks a protocol (MCP) and answers its statement calls.
//!
//! Text crosses in both directions — SQL in, already-formatted text out — so
//! no engine structure is ever reachable from extension code. The host keeps
//! the library mapped for the whole call (the [`LoadedExtension`] holds the
//! code) and refuses to load or serve the same file twice concurrently: a
//! serve loop must never run against code another load could replace.

use std::ffi::{CString, c_char, c_void};
use std::path::Path;

use crate::{ExtError, LoadedExtension};

/// What the extension can ask the host to run.
///
/// Implemented by the host (`pinkdb`'s CLI/server glue) so the read-only
/// policy, role checks and time/row budgets are applied on the host side:
/// the extension never decides whether a statement may run.
pub trait StatementHost {
    /// Run one statement of `sql`. Returns `(0, markdown)` on success and
    /// `(1, error_json)` when the statement failed; a JSON error is still a
    /// complete answer the client can read and act on. Any other code means
    /// the host could not run a statement at all.
    fn execute_statement(&mut self, sql: &str) -> (i32, String);
}

struct ServeState<'a> {
    host: &'a mut dyn StatementHost,
}

/// Whether a serve loop (or a command) is running for the library at `path`.
pub fn is_serving(path: &Path) -> bool {
    crate::command::is_active(path)
}

/// The `execute_statement` implementation handed to a serving extension.
/// Never unwinds into extension code: a panicking host callback becomes a
/// "no session" code instead of a foreign unwind.
extern "C" fn execute_statement(
    ctx: *mut c_void,
    sql: *const c_char,
    sql_len: usize,
    out_text: *mut *mut c_char,
    out_len: *mut usize,
) -> i32 {
    if out_text.is_null() || out_len.is_null() {
        return 2;
    }
    // SAFETY: the out pointers were just NULL-checked and point at the
    // caller's locals, per the ABI.
    unsafe {
        *out_text = std::ptr::null_mut();
        *out_len = 0;
    }
    let sql = if sql.is_null() {
        ""
    } else {
        // SAFETY: the ABI requires `sql` to point at `sql_len` readable bytes;
        // a NULL pointer is handled above.
        let bytes = unsafe { std::slice::from_raw_parts(sql.cast::<u8>(), sql_len) };
        match std::str::from_utf8(bytes) {
            Ok(text) => text,
            // Statement text is UTF-8 by contract; report it instead of
            // producing a broken copy.
            Err(_) => return 1,
        }
    };
    // SAFETY: `ctx` is the `ServeState` this module passed to
    // `pinkdb_ext_serve` (the only API instance that carries this callback).
    let Some(state) = (unsafe { ctx.cast::<ServeState<'_>>().as_mut() }) else {
        return 2;
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        state.host.execute_statement(sql)
    }));
    let (code, text) = outcome.unwrap_or_else(|_| (2, String::new()));
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

/// Call `ext`'s `pinkdb_ext_serve` and block until it returns.
///
/// `transport` is the descriptor the extension understands (`stdio`, or
/// `http:HOST:PORT`); `options` is host-defined. The extension decides what
/// to do with them; the loader only checks that the library exports the
/// symbol and that this file is not already serving.
pub fn serve(
    ext: &LoadedExtension,
    transport: &str,
    options: &str,
    host: &mut dyn StatementHost,
) -> Result<(), ExtError> {
    // Reject a malformed descriptor here so the extension only ever parses
    // the two shapes it documents.
    parse_transport(transport)?;
    let path = ext.canonical_path();
    let serve_fn = ext.serve_fn().ok_or_else(|| ExtError::NoServe {
        path: ext.path().to_string(),
    })?;
    if !crate::command::pin(&path) {
        return Err(ExtError::Serving {
            path: ext.path().to_string(),
        });
    }
    let result = serve_locked(ext, serve_fn, transport, options, host);
    crate::command::unpin(&path);
    result
}

fn serve_locked(
    ext: &LoadedExtension,
    serve_fn: crate::abi::PinkdbExtServeFn,
    transport: &str,
    options: &str,
    host: &mut dyn StatementHost,
) -> Result<(), ExtError> {
    let transport = CString::new(transport).map_err(|_| ExtError::BadTransport {
        message: "transport contains a NUL byte".to_string(),
    })?;
    let options = CString::new(options).map_err(|_| ExtError::BadTransport {
        message: "options contain a NUL byte".to_string(),
    })?;
    let mut api = crate::host::host_api();
    api.execute_statement = execute_statement;
    let mut state = ServeState { host };
    // SAFETY (all of it): `api` is a fully initialized host struct (the same
    // one init gets, with the statement field wired), `state` lives for the
    // whole call and is what `execute_statement` casts back, and the two
    // strings outlive the call. `pinkdb_ext_serve` must not unwind (its
    // signature is `extern "C"`).
    let code = unsafe {
        serve_fn(
            &api,
            (&mut state as *mut ServeState<'_>).cast::<c_void>(),
            transport.as_ptr(),
            options.as_ptr(),
        )
    };
    if code == 0 {
        Ok(())
    } else {
        Err(ExtError::ServeFailed {
            path: ext.path().to_string(),
            code,
        })
    }
}

/// A transport descriptor an extension can be handed. The loader does not
/// interpret it; this is the grammar hosts and extensions share.
pub fn parse_transport(text: &str) -> Result<Transport, ExtError> {
    if text == "stdio" {
        return Ok(Transport::Stdio);
    }
    if let Some(addr) = text.strip_prefix("http:") {
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| ExtError::BadTransport {
                message: format!("expected http:HOST:PORT, got \"{text}\""),
            })?;
        let port: u16 = port.parse().map_err(|_| ExtError::BadTransport {
            message: format!("invalid port in \"{text}\""),
        })?;
        return Ok(Transport::Http {
            host: host.to_string(),
            port,
        });
    }
    Err(ExtError::BadTransport {
        message: format!("expected \"stdio\" or \"http:HOST:PORT\", got \"{text}\""),
    })
}

/// The transport a serve call is handed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    /// Newline-delimited JSON-RPC on stdin/stdout.
    Stdio,
    /// HTTP on `host:port`.
    Http { host: String, port: u16 },
}

impl Transport {
    /// The descriptor string handed to `pinkdb_ext_serve`.
    pub fn descriptor(&self) -> String {
        match self {
            Transport::Stdio => "stdio".to_string(),
            Transport::Http { host, port } => format!("http:{host}:{port}"),
        }
    }
}
