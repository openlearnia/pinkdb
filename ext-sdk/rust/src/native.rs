//! The native (cdylib) half of the SDK: the ABI v1 exports a registration
//! macro generates, and the single `extern "C"` trampoline that turns a
//! C-ABI call into a typed Rust call.
//!
//! Everything `unsafe` in this crate lives here (plus the small amount of
//! pointer handling in this module's helper functions), mirroring the loader
//! crate's stance for the host side: the ABI's contract is implemented once,
//! and extension authors never see an `unsafe` block.
//!
//! The trampoline's rules come straight from `pinkdb_extension.h`:
//! - arguments arrive as a read-only array of `pinkdb_ext_value`; VARCHAR
//!   bytes are borrowed for the duration of the call;
//! - the result struct is host-owned and pre-filled with NULL; a non-NULL
//!   `text_ptr` must come from the host's `alloc` (the host copies and frees
//!   it immediately), so the SDK allocates result text through the copied
//!   host API and never leaves a non-allocated pointer there;
//! - the callback must not unwind across the boundary: a panic in user code
//!   is caught and reported as an error code (with a diagnostic on stderr),
//!   and nothing is printed for a normal `Err` — the C ABI has no error-text
//!   channel (the linked model does).

use std::ffi::{CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use pinkdb_ext_loader::{
    ExtArg, ExtType, ExtValue, PINKDB_EXT_ABI_VERSION, PINKDB_EXT_FLAG_VOLATILE, PinkdbExtScalarFn,
    PinkdbExtValue, PinkdbHostApi,
};

use crate::ScalarSpec;

/// A copy of the host's API struct, valid for the process's lifetime (the
/// header documents the original as init-scoped). `alloc` is all callbacks
/// need from it; the registration pointers are used during init only.
static HOST_API: OnceLock<PinkdbHostApi> = OnceLock::new();

/// Error code for "the function itself returned `Err`".
pub const CODE_USER_ERROR: i32 = 1;
/// Error code for an SDK/ABI contract failure (a panic, a pointer the host
/// promised not to pass, an allocation failure). A diagnostic is printed to
/// stderr; `CODE_USER_ERROR` deliberately is not (a failing function is a
/// normal SQL error, and the ABI has no message channel).
pub const CODE_INTERNAL_ERROR: i32 = 2;
/// `pinkdb_extension_init`'s code when the host's ABI version differs.
const INIT_ABI_VERSION: i32 = 3;
/// `pinkdb_extension_init`'s code when the extension declares a volatile
/// function but the host predates the flag (an older host would silently
/// register it as deterministic, so the load must fail instead).
const INIT_NO_FLAGS_SUPPORT: i32 = 4;
/// `pinkdb_extension_init`'s code when the extension declares a
/// ranged-arity function (`optional(min)` / `variadic(min)`): the native ABI
/// v1 registers fixed-arity functions only, and registering a ranged one at
/// its maximum arity would silently refuse the shorter calls the declaration
/// promises. A diagnostic on stderr names the function and the linked
/// alternative.
const INIT_RANGED_ARITY: i32 = 5;

/// The end offset of the `register_scalar_flags` field in the host's struct,
/// as known to this build. An extension calls the field only when the host's
/// `struct_size` covers it (the field is appended last, so a bigger
/// `struct_size` means a newer host that has it).
const FLAGS_FIELD_END: u32 = (std::mem::offset_of!(PinkdbHostApi, register_scalar_flags)
    + std::mem::size_of::<usize>()) as u32;

/// Whether a host whose `struct_size` is `struct_size` provides the additive
/// `register_scalar_flags` entry point (see the header).
pub const fn flags_registration_available(struct_size: u32) -> bool {
    struct_size >= FLAGS_FIELD_END
}

/// A NULL result struct. The host pre-fills `out` with NULL, and a NULL
/// `text_ptr` is never freed, so filling first is always safe.
fn null_value(type_tag: i32) -> PinkdbExtValue {
    PinkdbExtValue {
        type_tag,
        is_null: 1,
        int64: 0,
        double: 0.0,
        text_ptr: std::ptr::null(),
        text_len: 0,
    }
}

/// Decode the host's argument array into the shared value model.
///
/// # Safety
///
/// `args` must be the callback's argument array: `n_args` valid
/// `pinkdb_ext_value` structs (or a NULL pointer when `n_args` is 0) whose
/// VARCHAR bytes stay readable for `'a`. The trampoline passes exactly that.
unsafe fn collect_args<'a>(
    args: *const PinkdbExtValue,
    n_args: usize,
) -> Result<Vec<ExtArg<'a>>, String> {
    let mut out = Vec::with_capacity(n_args);
    for i in 0..n_args {
        // SAFETY: the ABI promises `n_args` readable entries; checked non-NULL
        // before the first dereference by the caller contract.
        let value = unsafe { &*args.add(i) };
        if value.is_null != 0 {
            out.push(ExtArg::Null(
                ExtType::from_tag(value.type_tag).unwrap_or(ExtType::Varchar),
            ));
            continue;
        }
        out.push(match ExtType::from_tag(value.type_tag) {
            Some(ExtType::Boolean) => ExtArg::Boolean(value.int64 != 0),
            Some(ExtType::Integer) => ExtArg::Integer(value.int64 as i32),
            Some(ExtType::Bigint) => ExtArg::Bigint(value.int64),
            Some(ExtType::Double) => ExtArg::Double(value.double),
            Some(ExtType::Varchar) => {
                if value.text_len == 0 {
                    ExtArg::Varchar(&[])
                } else {
                    // SAFETY: the ABI requires a non-NULL pointer to at least
                    // `text_len` readable bytes for the duration of the call.
                    unsafe {
                        ExtArg::Varchar(std::slice::from_raw_parts(
                            value.text_ptr as *const u8,
                            value.text_len,
                        ))
                    }
                }
            }
            Some(ExtType::Date) => ExtArg::Date(value.int64 as i32),
            Some(ExtType::Timestamp) => ExtArg::Timestamp(value.int64),
            Some(ExtType::Interval) => {
                let (months, days, micros) =
                    pinkdb_ext_loader::unpack_interval(value.int64, value.double);
                ExtArg::Interval {
                    months,
                    days,
                    micros,
                }
            }
            // The host only passes declared, non-NULL tags (it rejects
            // PINKDB_EXT_TYPE_NULL at registration and bad results at call
            // time), so this is a contract violation, not something to
            // reinterpret.
            None => {
                return Err(format!(
                    "argument {} carries unknown type tag {}",
                    i + 1,
                    value.type_tag
                ));
            }
        });
    }
    Ok(out)
}

/// Write one result value into the host-owned struct, allocating VARCHAR
/// bytes with the host's `alloc` (the header's ownership rules).
fn write_result(value: ExtValue, out: &mut PinkdbExtValue) -> Result<(), String> {
    match value {
        ExtValue::Null => {
            *out = null_value(out.type_tag);
            Ok(())
        }
        ExtValue::Boolean(b) => {
            out.is_null = 0;
            out.int64 = i64::from(b);
            Ok(())
        }
        ExtValue::Integer(i) => {
            out.is_null = 0;
            out.int64 = i64::from(i);
            Ok(())
        }
        ExtValue::Bigint(i) => {
            out.is_null = 0;
            out.int64 = i;
            Ok(())
        }
        ExtValue::Double(x) => {
            out.is_null = 0;
            out.double = x;
            Ok(())
        }
        ExtValue::Date(days) => {
            out.is_null = 0;
            out.int64 = i64::from(days);
            Ok(())
        }
        ExtValue::Timestamp(micros) => {
            out.is_null = 0;
            out.int64 = micros;
            Ok(())
        }
        ExtValue::Interval {
            months,
            days,
            micros,
        } => {
            out.is_null = 0;
            let (int64, double) = pinkdb_ext_loader::pack_interval(months, days, micros);
            out.int64 = int64;
            out.double = double;
            Ok(())
        }
        ExtValue::Varchar(bytes) => {
            out.is_null = 0;
            if bytes.is_empty() {
                // A zero-length VARCHAR: NULL text_ptr, text_len 0 (never
                // freed by the host, and read as an empty string).
                return Ok(());
            }
            let Some(api) = HOST_API.get() else {
                return Err("the host API was never initialized".to_string());
            };
            let ptr = (api.alloc)(bytes.len());
            if ptr.is_null() {
                return Err(format!(
                    "the host could not allocate {} bytes for a VARCHAR result",
                    bytes.len()
                ));
            }
            // SAFETY: `alloc` returned a fresh block of `bytes.len()` bytes
            // owned by the host (`free` is its pair, and the host frees a
            // non-NULL `text_ptr` right after this call returns).
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.cast::<u8>(), bytes.len());
            }
            out.text_ptr = ptr as *const c_char;
            out.text_len = bytes.len();
            Ok(())
        }
    }
}

/// The panic payload as a string, for the stderr diagnostic.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// The one `extern "C"` callback every registered function shares: it looks
/// the spec up from `user_data`, decodes the arguments, calls the typed body
/// and writes the result. Never unwinds.
///
/// # Safety
///
/// Called by the host per the ABI: `user_data` is the spec pointer passed at
/// registration, `args` points at `n_args` valid argument structs and `out`
/// at a host-owned writable result struct.
unsafe extern "C" fn trampoline(
    user_data: *mut c_void,
    args: *const PinkdbExtValue,
    n_args: usize,
    out: *mut PinkdbExtValue,
) -> i32 {
    // SAFETY: `user_data` and `out` are the ABI's own pointers; both are
    // NULL-checked via `as_ref`/`as_mut` before use.
    let Some(spec) = (unsafe { user_data.cast::<ScalarSpec>().as_ref() }) else {
        eprintln!("pinkdb extension: callback invoked with a NULL user_data");
        return CODE_INTERNAL_ERROR;
    };
    let Some(out) = (unsafe { out.as_mut() }) else {
        eprintln!(
            "pinkdb extension {}: callback invoked with a NULL result",
            spec.name
        );
        return CODE_INTERNAL_ERROR;
    };
    *out = null_value(spec.ret.tag());
    if n_args < spec.min_args || n_args > spec.max_args() {
        eprintln!(
            "pinkdb extension {}: host passed {n_args} argument(s), this function accepts {}..{}",
            spec.name,
            spec.min_args,
            spec.max_args()
        );
        return CODE_INTERNAL_ERROR;
    }
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: `n_args` matches the registered arity and, per the ABI, the
        // pointer is NULL exactly when that arity is 0.
        if n_args == 0 {
            Ok(Vec::new())
        } else {
            unsafe { collect_args(args, n_args) }
        }
    }));
    let decoded = match decoded {
        Ok(Ok(args)) => args,
        Ok(Err(message)) => {
            eprintln!("pinkdb extension {}: {message}", spec.name);
            return CODE_INTERNAL_ERROR;
        }
        Err(_) => {
            eprintln!(
                "pinkdb extension {}: decoding the arguments panicked",
                spec.name
            );
            return CODE_INTERNAL_ERROR;
        }
    };
    if decoded.len() != n_args
        || decoded
            .iter()
            .enumerate()
            .any(|(i, arg)| arg.ext_type() != spec.arg_type_at(i))
    {
        eprintln!(
            "pinkdb extension {}: host passed unsupported argument values",
            spec.name
        );
        return CODE_INTERNAL_ERROR;
    }

    // A panic in user code must never cross the ABI boundary.
    match catch_unwind(AssertUnwindSafe(|| (spec.call)(&decoded))) {
        Ok(Ok(value)) => match write_result(value, out) {
            Ok(()) => 0,
            Err(message) => {
                eprintln!("pinkdb extension {}: {message}", spec.name);
                CODE_INTERNAL_ERROR
            }
        },
        // A normal extension error: the host reports the code (the C ABI v1
        // has no error-text channel; the linked model carries the message).
        Ok(Err(_)) => CODE_USER_ERROR,
        Err(payload) => {
            eprintln!(
                "pinkdb extension {}: callback panicked: {}",
                spec.name,
                panic_message(&*payload)
            );
            CODE_INTERNAL_ERROR
        }
    }
}

/// The generated `pinkdb_extension_init` body: store the host API, then
/// register every spec, volatile ones included.
///
/// # Safety
///
/// `api` must be the host's fully initialized `PinkdbHostApi` (the loader
/// passes exactly that) and `host_ctx` the opaque context to hand back.
pub unsafe fn native_init(
    api: *const PinkdbHostApi,
    host_ctx: *mut c_void,
    extension_name: &'static str,
    specs: &[ScalarSpec],
) -> i32 {
    // The loader already refuses a non-zero init with a load error; the catch
    // is the ABI's "never unwind" rule for `pinkdb_extension_init` itself.
    match catch_unwind(AssertUnwindSafe(|| {
        init_inner(api, host_ctx, extension_name, specs)
    })) {
        Ok(code) => code,
        Err(payload) => {
            eprintln!(
                "pinkdb extension {extension_name}: pinkdb_extension_init panicked: {}",
                panic_message(&*payload)
            );
            CODE_INTERNAL_ERROR
        }
    }
}

fn init_inner(
    api: *const PinkdbHostApi,
    host_ctx: *mut c_void,
    extension_name: &'static str,
    specs: &[ScalarSpec],
) -> i32 {
    // SAFETY: `api` is the pointer the loader passes to `pinkdb_extension_init`
    // and is valid for the duration of the call (the ABI documents it as
    // init-scoped; we copy it below).
    let Some(api) = (unsafe { api.as_ref() }) else {
        eprintln!("pinkdb extension {extension_name}: NULL host API");
        return CODE_INTERNAL_ERROR;
    };
    if api.abi_version != PINKDB_EXT_ABI_VERSION {
        eprintln!(
            "pinkdb extension {extension_name}: host ABI version {}, extension built for {}",
            api.abi_version, PINKDB_EXT_ABI_VERSION
        );
        return INIT_ABI_VERSION;
    }
    // Keep the first copy: a second load of the same image must not swap the
    // allocator out from under already-registered callbacks.
    let _ = HOST_API.set(*api);
    let flags_available = flags_registration_available(api.struct_size);
    if !flags_available && specs.iter().any(|spec| spec.volatile) {
        eprintln!(
            "pinkdb extension {extension_name}: this host predates the volatile-function flag \
             and cannot register one; upgrade pinkdb (refusing to register a volatile function \
             as deterministic)"
        );
        return INIT_NO_FLAGS_SUPPORT;
    }

    for spec in specs {
        // The native ABI v1 carries a fixed `n_args`, with no way to express
        // "at least n": registering a ranged declaration at its maximum arity
        // would silently refuse the shorter calls it promises, so the whole
        // load fails instead (atomically) with a diagnostic pointing at the
        // linked model.
        if spec.is_ranged() {
            eprintln!(
                "pinkdb extension {extension_name}: function {} declares a ranged arity \
                 ({}..={} arguments); the native ABI v1 registers fixed-arity functions only — \
                 publish this extension through the linked registry instead \
                 (pinkdb_register_linked(), then LOAD EXTENSION '{extension_name}')",
                spec.name,
                spec.min_args,
                spec.max_args()
            );
            return INIT_RANGED_ARITY;
        }
        // The host copies the name during the call, so a temporary C string is
        // enough (and keeps the spec free of a duplicate NUL-terminated
        // spelling).
        let Ok(name) = CString::new(spec.name) else {
            eprintln!(
                "pinkdb extension {extension_name}: function name {:?} contains a NUL",
                spec.name
            );
            return CODE_INTERNAL_ERROR;
        };
        let tags: Vec<i32> = spec.args.iter().map(|t| t.tag()).collect();
        let arg_ptr = if tags.is_empty() {
            std::ptr::null()
        } else {
            tags.as_ptr()
        };
        let callback: Option<PinkdbExtScalarFn> = Some(trampoline);
        let user_data = std::ptr::from_ref(spec).cast_mut().cast::<c_void>();
        let name = name.as_ptr();
        let n_args = spec.args.len() as u32;
        let ret = spec.ret.tag();
        // Every argument is per the ABI: `host_ctx` is passed back unchanged,
        // `name` is NUL-terminated and copied by the host, `tags` holds the
        // declared non-NULL type tags, `trampoline` is a valid callback and
        // `user_data` points at a `'static` spec. Registration is init-only,
        // which is where we are.
        let code = if flags_available {
            (api.register_scalar_flags)(
                host_ctx,
                name,
                n_args,
                arg_ptr,
                ret,
                if spec.volatile {
                    PINKDB_EXT_FLAG_VOLATILE
                } else {
                    0
                },
                callback,
                user_data,
            )
        } else {
            (api.register_scalar)(host_ctx, name, n_args, arg_ptr, ret, callback, user_data)
        };
        if code != 0 {
            // The loader already recorded the reason (it is reported as the
            // load error); this non-zero return only fails the load too.
            return 1;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    extern "C" fn dummy_register(
        _ctx: *mut c_void,
        _name: *const c_char,
        _n_args: u32,
        _arg_types: *const i32,
        _ret_type: i32,
        _func: Option<PinkdbExtScalarFn>,
        _user_data: *mut c_void,
    ) -> i32 {
        0
    }

    extern "C" fn dummy_register_flags(
        _ctx: *mut c_void,
        _name: *const c_char,
        _n_args: u32,
        _arg_types: *const i32,
        _ret_type: i32,
        _flags: u32,
        _func: Option<PinkdbExtScalarFn>,
        _user_data: *mut c_void,
    ) -> i32 {
        0
    }

    extern "C" fn test_alloc(size: usize) -> *mut c_void {
        // The ABI's allocator is plain malloc; the host frees with `free`.
        unsafe extern "C" {
            fn malloc(size: usize) -> *mut c_void;
        }
        // SAFETY: plain C malloc.
        unsafe { malloc(size) }
    }

    extern "C" fn test_free(ptr: *mut c_void) {
        unsafe extern "C" {
            fn free(ptr: *mut c_void);
        }
        if !ptr.is_null() {
            // SAFETY: pairs with `test_alloc`.
            unsafe { free(ptr) };
        }
    }

    /// Give the process a host API so `write_result` can allocate text (the
    /// unit-test binary never loads a real extension).
    fn install_test_host_api() {
        let _ = HOST_API.set(PinkdbHostApi {
            abi_version: PINKDB_EXT_ABI_VERSION,
            struct_size: std::mem::size_of::<PinkdbHostApi>() as u32,
            register_scalar: dummy_register,
            alloc: test_alloc,
            free: test_free,
            register_scalar_flags: dummy_register_flags,
        });
    }

    /// The flag entry point exists exactly when the host's `struct_size`
    /// covers the appended field — the check the header documents.
    #[test]
    fn flags_registration_is_gated_on_struct_size() {
        assert!(!flags_registration_available(0));
        // The pre-flag host API: 32 bytes on a 64-bit target.
        assert!(!flags_registration_available(FLAGS_FIELD_END - 1));
        assert!(flags_registration_available(FLAGS_FIELD_END));
        assert!(flags_registration_available(FLAGS_FIELD_END + 8));
    }

    /// Encoding a result follows the header's ownership rules: non-VARCHAR
    /// results leave `text_ptr` NULL, an empty VARCHAR is NULL-pointer +
    /// length 0, and a non-empty one points at host-`alloc` memory.
    #[test]
    fn results_encode_per_the_abi() {
        install_test_host_api();
        let mut out = null_value(ExtType::Varchar.tag());

        write_result(ExtValue::Null, &mut out).unwrap();
        assert_eq!(out.is_null, 1);
        assert!(out.text_ptr.is_null());

        write_result(ExtValue::Integer(-3), &mut out).unwrap();
        assert_eq!(out.is_null, 0);
        assert_eq!(out.int64, -3);
        assert!(out.text_ptr.is_null());

        write_result(ExtValue::Boolean(true), &mut out).unwrap();
        assert_eq!(out.int64, 1);
        write_result(ExtValue::Double(1.5), &mut out).unwrap();
        assert_eq!(out.double, 1.5);
        write_result(ExtValue::Date(9), &mut out).unwrap();
        assert_eq!(out.int64, 9);
        write_result(ExtValue::Timestamp(-1), &mut out).unwrap();
        assert_eq!(out.int64, -1);
        write_result(
            ExtValue::Interval {
                months: 13,
                days: -2,
                micros: 7,
            },
            &mut out,
        )
        .unwrap();
        assert_eq!(
            pinkdb_ext_loader::unpack_interval(out.int64, out.double),
            (13, -2, 7)
        );
        assert!(out.text_ptr.is_null());

        write_result(ExtValue::Varchar(Vec::new()), &mut out).unwrap();
        assert_eq!(out.is_null, 0);
        assert_eq!(out.text_len, 0);
        assert!(out.text_ptr.is_null(), "an empty result needs no buffer");

        write_result(ExtValue::Varchar(b"hey".to_vec()), &mut out).unwrap();
        assert_eq!(out.text_len, 3);
        assert!(!out.text_ptr.is_null());
        // SAFETY: just allocated with `test_alloc`, `text_len` bytes long.
        let bytes = unsafe { std::slice::from_raw_parts(out.text_ptr as *const u8, out.text_len) };
        assert_eq!(bytes, b"hey");
        (test_free)(out.text_ptr as *mut c_void);
    }

    /// Decoding reads every ABI value shape, including NULL and empty text.
    #[test]
    fn arguments_decode_from_the_abi() {
        let values = [
            PinkdbExtValue {
                type_tag: ExtType::Boolean.tag(),
                is_null: 0,
                int64: 1,
                double: 0.0,
                text_ptr: std::ptr::null(),
                text_len: 0,
            },
            PinkdbExtValue {
                type_tag: ExtType::Bigint.tag(),
                is_null: 0,
                int64: -7,
                double: 0.0,
                text_ptr: std::ptr::null(),
                text_len: 0,
            },
            PinkdbExtValue {
                type_tag: ExtType::Varchar.tag(),
                is_null: 1,
                int64: 0,
                double: 0.0,
                text_ptr: std::ptr::null(),
                text_len: 0,
            },
            PinkdbExtValue {
                type_tag: ExtType::Varchar.tag(),
                is_null: 0,
                int64: 0,
                double: 0.0,
                text_ptr: b"hi".as_ptr() as *const c_char,
                text_len: 2,
            },
            {
                let (int64, double) = pinkdb_ext_loader::pack_interval(1, 2, -3);
                PinkdbExtValue {
                    type_tag: ExtType::Interval.tag(),
                    is_null: 0,
                    int64,
                    double,
                    text_ptr: std::ptr::null(),
                    text_len: 0,
                }
            },
        ];
        // SAFETY: the array holds five valid structs whose text borrows live
        // for this scope.
        let decoded = unsafe { collect_args(values.as_ptr(), values.len()) }.unwrap();
        assert_eq!(decoded[0], ExtArg::Boolean(true));
        assert_eq!(decoded[1], ExtArg::Bigint(-7));
        assert_eq!(decoded[2], ExtArg::Null(ExtType::Varchar));
        assert_eq!(decoded[3], ExtArg::Varchar(b"hi"));
        assert_eq!(
            decoded[4],
            ExtArg::Interval {
                months: 1,
                days: 2,
                micros: -3
            }
        );
        // SAFETY: as above, with a NULL array (the zero-argument case).
        assert!(
            unsafe { collect_args(std::ptr::null(), 0) }
                .unwrap()
                .is_empty()
        );

        let unknown = [PinkdbExtValue {
            type_tag: 99,
            is_null: 0,
            int64: 0,
            double: 0.0,
            text_ptr: std::ptr::null(),
            text_len: 0,
        }];
        // SAFETY: one valid struct.
        let err = unsafe { collect_args(unknown.as_ptr(), 1) }.unwrap_err();
        assert!(err.contains("unknown type tag 99"), "{err}");
    }
}
