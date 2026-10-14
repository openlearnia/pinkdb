//! The host half of the ABI: the [`PinkdbHostApi`] handed to an extension's
//! `pinkdb_extension_init`, with checked pointer handling and counted
//! malloc/free for result text.

use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::abi::{
    ExtType, PinkdbExtScalarFn, PinkdbHostApi, PINKDB_EXT_ABI_VERSION, PINKDB_EXT_MAX_ARGS,
};
use crate::function::{DynamicFunction, Registration};

// The C standard library allocator. Declared here instead of pulling in the
// `libc` crate: every Rust binary already links the C runtime, and the
// extension ABI is defined in terms of plain malloc/free. These are the only
// two foreign functions the loader calls.
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
}

/// State collected while an extension's `pinkdb_extension_init` runs.
///
/// Lives on the loading thread's stack only; the extension receives a
/// `*mut Collector` as the `host_ctx` argument and passes it back to
/// `register_scalar`. The ABI forbids calling `register_scalar` outside init.
pub(crate) struct Collector {
    /// Keeps the library mapped via every collected function.
    pub(crate) library: Arc<libloading::Library>,
    /// Path the library was loaded from; copied into every function so the
    /// engine can map a called function back to its extension.
    pub(crate) path: String,
    pub(crate) functions: Vec<DynamicFunction>,
    /// First rejected registration, if any: the load fails even when the
    /// extension's init returns 0 (a rejected function must never look
    /// registered).
    pub(crate) error: Option<String>,
}

/// Build the API struct for one `pinkdb_extension_init` call.
pub(crate) fn host_api() -> PinkdbHostApi {
    PinkdbHostApi {
        abi_version: PINKDB_EXT_ABI_VERSION,
        struct_size: std::mem::size_of::<PinkdbHostApi>() as u32,
        register_scalar,
        alloc: host_alloc,
        free: host_free,
    }
}

/// `register_scalar` implementation; never unwinds into extension code.
extern "C" fn register_scalar(
    ctx: *mut c_void,
    name: *const c_char,
    n_args: u32,
    arg_types: *const i32,
    ret_type: i32,
    func: Option<PinkdbExtScalarFn>,
    user_data: *mut c_void,
) -> i32 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        collect(ctx, name, n_args, arg_types, ret_type, func, user_data)
    }));
    match outcome {
        Ok(Ok(())) => 0,
        Ok(Err(message)) => {
            record_rejection(ctx, message);
            1
        }
        Err(_) => {
            record_rejection(ctx, "internal error: registration panicked".to_string());
            2
        }
    }
}

/// Remember the first rejected registration (later ones are not the cause).
fn record_rejection(ctx: *mut c_void, message: String) {
    // SAFETY: same contract as in `collect`: `ctx` points at the live
    // `Collector` for the duration of init; a NULL ctx is checked.
    if let Some(collector) = unsafe { ctx.cast::<Collector>().as_mut() } {
        if collector.error.is_none() {
            collector.error = Some(message);
        }
    }
}

/// Validate one registration and add it to the collector.
fn collect(
    ctx: *mut c_void,
    name: *const c_char,
    n_args: u32,
    arg_types: *const i32,
    ret_type: i32,
    func: Option<PinkdbExtScalarFn>,
    user_data: *mut c_void,
) -> Result<(), String> {
    // SAFETY: `ctx` is the pointer the loader passed to `pinkdb_extension_init`
    // and the ABI requires `register_scalar` to be called only from inside
    // init, on that thread, while the `Collector` is alive. A NULL ctx is
    // rejected below.
    let collector = unsafe { ctx.cast::<Collector>().as_mut() }
        .ok_or_else(|| "internal error: NULL host context".to_string())?;
    if let Some(first) = &collector.error {
        return Err(format!("a previous registration failed: {first}"));
    }
    if name.is_null() {
        return Err("function name is NULL".to_string());
    }
    // SAFETY: non-NULL and, per the ABI, NUL-terminated (the read stops at the
    // first NUL; garbage pointers are a contract violation like any FFI).
    let name = unsafe { CStr::from_ptr(name) }
        .to_str()
        .map_err(|_| "function name is not valid UTF-8".to_string())?;
    let (types, ret_type, func) = validate_registration(
        name,
        n_args,
        arg_types.is_null(),
        ret_type,
        // SAFETY: only called after the arity cap and the NULL-pointer check,
        // and the ABI requires `arg_types` to point at `n_args` tags.
        |i| unsafe { *arg_types.add(i) },
        func,
        &collector
            .functions
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>(),
    )?;
    collector.functions.push(DynamicFunction::new(
        Registration {
            name: name.to_string(),
            n_args: n_args as usize,
            arg_types: types,
            ret_type,
            callback: func,
            user_data,
        },
        host_free,
        Arc::clone(&collector.library),
        collector.path.clone(),
    ));
    Ok(())
}

/// Validate one registration's metadata (no pointer dereferencing beyond the
/// caller-provided tag reader) and return the decoded types plus the checked
/// non-NULL callback. Split out from [`collect`] so every rejection rule is
/// unit-testable without a live shared library.
fn validate_registration(
    name: &str,
    n_args: u32,
    arg_types_is_null: bool,
    ret_type: i32,
    mut arg_tag: impl FnMut(usize) -> i32,
    func: Option<PinkdbExtScalarFn>,
    existing_names: &[String],
) -> Result<(Vec<ExtType>, ExtType, PinkdbExtScalarFn), String> {
    if name.is_empty() {
        return Err("function name is empty".to_string());
    }
    if name.len() > 256 {
        let display: String = name.chars().take(40).collect();
        return Err(format!(
            "function name \"{display}…\" is longer than 256 bytes"
        ));
    }
    // The C signature takes a function pointer, which may be NULL; storing it
    // would turn the first call into a jump to address 0.
    let Some(func) = func else {
        return Err(format!("function \"{name}\": NULL callback"));
    };
    if n_args > PINKDB_EXT_MAX_ARGS {
        return Err(format!(
            "function \"{name}\": {n_args} arguments exceed the {PINKDB_EXT_MAX_ARGS} maximum"
        ));
    }
    if n_args > 0 && arg_types_is_null {
        return Err(format!(
            "function \"{name}\": NULL arg_types with {n_args} arguments"
        ));
    }
    let Some(ret_type) = ExtType::from_tag(ret_type) else {
        return Err(format!(
            "function \"{name}\": invalid return type tag {ret_type} (NULL is not a return type)"
        ));
    };
    let mut types = Vec::with_capacity(n_args as usize);
    for i in 0..n_args {
        let tag = arg_tag(i as usize);
        let Some(t) = ExtType::from_tag(tag) else {
            return Err(format!(
                "function \"{name}\": invalid type tag {tag} for argument {} (NULL is not an argument type)",
                i + 1
            ));
        };
        types.push(t);
    }
    if existing_names.iter().any(|n| n.eq_ignore_ascii_case(name)) {
        return Err(format!("function \"{name}\" is registered twice"));
    }
    Ok((types, ret_type, func))
}

/// Result-text allocations handed to extensions (process-wide counter).
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
/// Result-text frees handed back by the host (process-wide counter).
static FREE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Allocate result text for an extension (`api->alloc`).
extern "C" fn host_alloc(size: usize) -> *mut c_void {
    // SAFETY: plain C malloc; the returned pointer is either NULL or a fresh
    // heap block that only extension code sees.
    let ptr = unsafe { malloc(size) };
    if !ptr.is_null() {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    ptr
}

/// Release result text allocated by [`host_alloc`] (`api->free`).
extern "C" fn host_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    FREE_CALLS.fetch_add(1, Ordering::Relaxed);
    // SAFETY: the only pointers the host passes here come from `host_alloc`
    // (result text it just copied); an extension calling `api->free` on
    // anything else violates the ABI's alloc/free pairing rule.
    unsafe { free(ptr) };
}

/// Result-text allocation diagnostics: how many buffers the extension API
/// handed out, and how many the host (and extensions, through `api->free`)
/// handed back. Used by the leak tests; counts only, no byte accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextAllocStats {
    pub allocations: u64,
    pub frees: u64,
}

impl TextAllocStats {
    /// Out-standing buffers (0 when every allocation was freed).
    pub fn live(&self) -> u64 {
        self.allocations.saturating_sub(self.frees)
    }
}

/// Snapshot the process-wide result-text counters.
pub fn text_allocation_stats() -> TextAllocStats {
    TextAllocStats {
        allocations: ALLOC_CALLS.load(Ordering::Relaxed),
        frees: FREE_CALLS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::PINKDB_EXT_TYPE_INTEGER;

    extern "C" fn dummy(
        _user_data: *mut c_void,
        _args: *const crate::abi::PinkdbExtValue,
        _n_args: usize,
        _out: *mut crate::abi::PinkdbExtValue,
    ) -> i32 {
        0
    }

    fn some_fn() -> Option<PinkdbExtScalarFn> {
        Some(dummy as PinkdbExtScalarFn)
    }

    fn validate(
        name: &str,
        n_args: u32,
        null_types: bool,
        ret_type: i32,
        tags: &[i32],
    ) -> Result<(Vec<ExtType>, ExtType, PinkdbExtScalarFn), String> {
        validate_registration(
            name,
            n_args,
            null_types,
            ret_type,
            |i| tags[i],
            some_fn(),
            &[],
        )
    }

    #[test]
    fn registration_edges_are_rejected_with_clear_messages() {
        // Empty name.
        assert_eq!(
            validate("", 0, false, PINKDB_EXT_TYPE_INTEGER, &[]).unwrap_err(),
            "function name is empty"
        );
        // Longer than 256 bytes (the C header's cap).
        let long = "x".repeat(257);
        let e = validate(&long, 0, false, PINKDB_EXT_TYPE_INTEGER, &[]).unwrap_err();
        assert!(e.contains("is longer than 256 bytes"), "{e}");
        // Exactly 256 bytes is still accepted.
        let at_cap = "x".repeat(256);
        assert!(validate(&at_cap, 0, false, PINKDB_EXT_TYPE_INTEGER, &[]).is_ok());
        // NULL callback (the C side may pass one; it must never be stored).
        assert_eq!(
            validate_registration(
                "f",
                0,
                false,
                PINKDB_EXT_TYPE_INTEGER,
                |_| unreachable!(),
                None,
                &[]
            )
            .unwrap_err(),
            "function \"f\": NULL callback"
        );
        // NULL arg_types with arguments.
        assert_eq!(
            validate("f", 1, true, PINKDB_EXT_TYPE_INTEGER, &[]).unwrap_err(),
            "function \"f\": NULL arg_types with 1 arguments"
        );
        // Invalid argument tags: the NULL tag and an unknown tag.
        assert_eq!(
            validate("f", 1, false, PINKDB_EXT_TYPE_INTEGER, &[0]).unwrap_err(),
            "function \"f\": invalid type tag 0 for argument 1 (NULL is not an argument type)"
        );
        assert_eq!(
            validate(
                "f",
                2,
                false,
                PINKDB_EXT_TYPE_INTEGER,
                &[PINKDB_EXT_TYPE_INTEGER, 99]
            )
            .unwrap_err(),
            "function \"f\": invalid type tag 99 for argument 2 (NULL is not an argument type)"
        );
        // A duplicate name within one extension (case-insensitive): the
        // `existing_names` list is what `collect` builds from what it already
        // accepted.
        assert_eq!(
            validate_registration(
                "F",
                0,
                false,
                PINKDB_EXT_TYPE_INTEGER,
                |_| unreachable!(),
                some_fn(),
                &["f".to_string()],
            )
            .unwrap_err(),
            "function \"F\" is registered twice"
        );
        // A valid registration decodes the tags in order.
        let (types, ret, _) = validate(
            "ok",
            2,
            false,
            PINKDB_EXT_TYPE_INTEGER,
            &[PINKDB_EXT_TYPE_INTEGER, 3],
        )
        .unwrap();
        assert_eq!(types, vec![ExtType::Integer, ExtType::Bigint]);
        assert_eq!(ret, ExtType::Integer);
    }
}
