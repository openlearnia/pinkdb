//! One registered scalar function: the callback pointer, its metadata, and
//! the safe host-side invocation path.
//!
//! Everything dangerous about calling extension code is confined to
//! [`DynamicFunction::call`]: building the C argument array, invoking the
//! `extern "C"` callback, copying (and freeing) the result text, and turning
//! return codes into errors.

use std::ffi::c_void;
use std::fmt;
use std::slice;
use std::sync::Arc;

use crate::ExtError;
use crate::abi::{ExtArg, ExtType, ExtValue, PinkdbExtScalarFn, PinkdbExtValue};

/// One scalar function provided by a loaded extension.
///
/// `DynamicFunction` is `Clone` (a cheap handle: shared library handle plus
/// copied metadata), `Send`, and `Sync` — see the `SAFETY` notes below and
/// the thread-safety contract in `include/pinkdb_extension.h`.
pub struct DynamicFunction {
    /// Name as registered (case preserved for display; lookup is
    /// case-insensitive host-side).
    name: String,
    /// Path the extension was loaded from, exactly as passed to
    /// [`crate::load`]. The engine records it per function so a WAL record
    /// that calls this function can be prefixed with
    /// `LOAD EXTENSION '<path>'` when the current WAL generation has not
    /// loaded it yet.
    source_path: String,
    /// Declared arity.
    n_args: usize,
    /// Declared argument types, in order.
    arg_types: Vec<ExtType>,
    /// Declared return type.
    ret_type: ExtType,
    /// Declared volatile (nondeterministic): registered with
    /// `PINKDB_EXT_FLAG_VOLATILE`. The engine refuses to run such a call in
    /// any statement it WAL-logs (see the header's "Deterministic vs
    /// volatile functions" and `crate::linked`).
    volatile: bool,
    /// Extension code (valid while `library` is alive).
    callback: PinkdbExtScalarFn,
    /// Opaque extension-chosen context, passed back on every call.
    user_data: *mut c_void,
    /// Host allocator's matching free, used for result text.
    free_fn: extern "C" fn(*mut c_void),
    /// Keeps the shared library mapped while any function is registered.
    library: Arc<libloading::Library>,
}

// SAFETY (`Send`): the only non-Send field is `user_data`, a raw pointer the
// load-time `pinkdb_extension_init` call supplied. The ABI contract (see the
// header, "THREAD SAFETY") requires the extension — which owns both the
// callback and `user_data` — to be thread-safe, so moving the handle to
// another host thread is covered by that contract. `callback` (function
// pointer), `free_fn`, and `Arc<Library>` are all `Send` on their own.
unsafe impl Send for DynamicFunction {}

// SAFETY (`Sync`): shared `&DynamicFunction` access only exposes `call`,
// which touches immutable metadata and calls the extension's callback with
// its own `user_data`. The same ABI clause makes concurrent calls the
// documented contract, and `Arc<libloading::Library>` is `Sync`.
unsafe impl Sync for DynamicFunction {}

impl Clone for DynamicFunction {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            source_path: self.source_path.clone(),
            n_args: self.n_args,
            arg_types: self.arg_types.clone(),
            ret_type: self.ret_type,
            volatile: self.volatile,
            callback: self.callback,
            user_data: self.user_data,
            free_fn: self.free_fn,
            library: Arc::clone(&self.library),
        }
    }
}

impl fmt::Debug for DynamicFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Pointer values are deliberately not printed.
        f.debug_struct("DynamicFunction")
            .field("name", &self.name)
            .field("source_path", &self.source_path)
            .field("n_args", &self.n_args)
            .field("arg_types", &self.arg_types)
            .field("ret_type", &self.ret_type)
            .field("volatile", &self.volatile)
            .finish_non_exhaustive()
    }
}

/// Everything one registration call contributes to a function handle.
/// Mixed into [`DynamicFunction`] immediately, on the loading thread.
pub(crate) struct Registration {
    pub(crate) name: String,
    pub(crate) n_args: usize,
    pub(crate) arg_types: Vec<ExtType>,
    pub(crate) ret_type: ExtType,
    /// Decoded [`crate::abi::PINKDB_EXT_FLAG_VOLATILE`] bit.
    pub(crate) volatile: bool,
    pub(crate) callback: PinkdbExtScalarFn,
    pub(crate) user_data: *mut c_void,
}

impl DynamicFunction {
    /// Build a handle. Called by the loader while collecting registrations.
    pub(crate) fn new(
        reg: Registration,
        free_fn: extern "C" fn(*mut c_void),
        library: Arc<libloading::Library>,
        source_path: String,
    ) -> Self {
        Self {
            name: reg.name,
            source_path,
            n_args: reg.n_args,
            arg_types: reg.arg_types,
            ret_type: reg.ret_type,
            volatile: reg.volatile,
            callback: reg.callback,
            user_data: reg.user_data,
            free_fn,
            library,
        }
    }

    /// Registered name (original case).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Path this function's extension was loaded from, exactly as passed to
    /// [`crate::load`].
    pub fn source_path(&self) -> &str {
        &self.source_path
    }

    /// Declared argument count.
    pub fn n_args(&self) -> usize {
        self.n_args
    }

    /// Declared argument types, in order.
    pub fn arg_types(&self) -> &[ExtType] {
        &self.arg_types
    }

    /// Declared return type.
    pub fn ret_type(&self) -> ExtType {
        self.ret_type
    }

    /// Whether the extension declared this function volatile
    /// (nondeterministic) at registration
    /// ([`crate::abi::PINKDB_EXT_FLAG_VOLATILE`]). `false` — deterministic —
    /// is the default for every pre-flag registration and for every
    /// `register_scalar` call.
    ///
    /// The engine refuses to run a volatile call in any statement it
    /// WAL-logs: replaying logged SQL must land on identical data, and a
    /// nondeterministic callback cannot promise that.
    pub fn is_volatile(&self) -> bool {
        self.volatile
    }

    /// Invoke the callback once (one row) and return its result.
    ///
    /// `args` must have exactly [`Self::n_args`] entries and each entry's type
    /// must match the declared argument type; the host guarantees both at
    /// bind time, so a mismatch is reported as [`ExtError::BadResult`] rather
    /// than being undefined. A NULL argument is passed through with `is_null`
    /// set — the callback decides what it means.
    ///
    /// Text memory returned by the callback (non-NULL `text_ptr`) is copied
    /// into the result and immediately freed through the API's `free` — on
    /// every path, including NULL results and error returns.
    pub fn call(&self, args: &[ExtArg<'_>]) -> Result<ExtValue, ExtError> {
        if args.len() != self.n_args {
            return Err(ExtError::BadResult {
                function: self.name.clone(),
                message: format!("expected {} argument(s), got {}", self.n_args, args.len()),
            });
        }
        for (i, a) in args.iter().enumerate() {
            if a.ext_type() != self.arg_types[i] {
                return Err(ExtError::BadResult {
                    function: self.name.clone(),
                    message: format!(
                        "argument {} has type {}, registered as {}",
                        i + 1,
                        a.ext_type(),
                        self.arg_types[i]
                    ),
                });
            }
        }

        let abi_args: Vec<PinkdbExtValue> = args.iter().map(|a| a.to_abi()).collect();
        // Zero-argument callbacks receive a NULL array, per the header.
        let args_ptr = if abi_args.is_empty() {
            std::ptr::null()
        } else {
            abi_args.as_ptr()
        };
        let mut out = PinkdbExtValue::null(self.ret_type.tag());

        // SAFETY: `callback` is the function pointer the extension exported
        // for this registration (valid while `library` is mapped, i.e. at
        // least as long as this handle exists). `user_data` is the untouched
        // value the extension passed to `register_scalar`; `abi_args` is a
        // valid read-only array of `n_args` C structs (VARCHAR pointers borrow
        // their `ExtArg` slices, which outlive the call); `out` is a valid,
        // writable, host-owned struct. The callback may not unwind (the ABI
        // forbids panics across it), may not modify `args`, and returns 0 or a
        // non-zero error code we validate below.
        let code = unsafe { (self.callback)(self.user_data, args_ptr, abi_args.len(), &mut out) };

        // Freeing contract: a non-NULL text pointer was allocated with
        // `api->alloc`, and the host owns it — always release it, regardless
        // of is_null, of the return code, or of decode errors below.
        let free_text = !out.text_ptr.is_null();

        if code != 0 {
            // Error path: the result struct is not read (the callback may not
            // have finished writing it), but an allocated buffer is released.
            if free_text {
                (self.free_fn)(out.text_ptr as *mut c_void);
            }
            return Err(ExtError::Callback {
                function: self.name.clone(),
                code,
            });
        }

        // Decode before freeing: the VARCHAR copy reads the extension buffer.
        let decoded = self.decode_result(&out);
        if free_text {
            (self.free_fn)(out.text_ptr as *mut c_void);
        }
        decoded
    }

    /// Turn the host-owned `out` struct into an [`ExtValue`], copying text
    /// bytes out of extension memory.
    fn decode_result(&self, out: &PinkdbExtValue) -> Result<ExtValue, ExtError> {
        if out.is_null != 0 {
            // NULL: the tag is not checked (an extension may leave it 0).
            return Ok(ExtValue::Null);
        }
        let Some(tag) = ExtType::from_tag(out.type_tag) else {
            return Err(ExtError::BadResult {
                function: self.name.clone(),
                message: format!("returned unknown type tag {}", out.type_tag),
            });
        };
        if tag != self.ret_type {
            return Err(ExtError::BadResult {
                function: self.name.clone(),
                message: format!("returned {tag}, registered as {}", self.ret_type),
            });
        }
        Ok(match tag {
            ExtType::Boolean => ExtValue::Boolean(out.int64 != 0),
            ExtType::Integer => ExtValue::Integer(out.int64 as i32),
            ExtType::Bigint => ExtValue::Bigint(out.int64),
            ExtType::Double => ExtValue::Double(out.double),
            ExtType::Varchar => {
                let bytes = if out.text_len == 0 {
                    Vec::new()
                } else {
                    if out.text_ptr.is_null() {
                        return Err(ExtError::BadResult {
                            function: self.name.clone(),
                            message: format!(
                                "returned a NULL text pointer with text_len {}",
                                out.text_len
                            ),
                        });
                    }
                    // SAFETY: the ABI requires a non-NULL VARCHAR result to
                    // point at at least `text_len` readable bytes that stay
                    // valid until the callback returns (we are still inside
                    // that window) and to have been allocated with
                    // `api->alloc`. The copy is owned Rust memory; the
                    // extension buffer is freed by `call` right after.
                    unsafe { slice::from_raw_parts(out.text_ptr as *const u8, out.text_len) }
                        .to_vec()
                };
                ExtValue::Varchar(bytes)
            }
            ExtType::Date => ExtValue::Date(out.int64 as i32),
            ExtType::Timestamp => ExtValue::Timestamp(out.int64),
        })
    }
}
