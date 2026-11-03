//! `conformance_ext` — the Rust implementation of the pinkdb extension ABI
//! conformance fixture (`ext-sdk/conformance/README.md`).
//!
//! One contract, four SDKs: this library registers the fixture's functions
//! (`cf_echo` … `cf_thread`) through the safe Rust SDK, so the loader's
//! conformance tests can hold the Rust SDK to the same assertions as the C,
//! C++ and Go fixtures. It is a cdylib, built by `cargo build -p
//! pinkdb-ext-conformance` (or `make -C ext-sdk/rust conformance`).
//!
//! Two things are not the SDK's default path, and both are the fixture's
//! point:
//!
//! - the three ABI entry points are written out by hand instead of coming
//!   from `pinkdb_abi_exports!()`, because the fixture must record **the
//!   thread `pinkdb_extension_init` ran on** to count foreign-thread callbacks
//!   (`pinkdb_conformance_foreign_threads`). Everything else about them is
//!   the macro's code, unchanged: the version constant, the pinned name and
//!   `__private::native_init`;
//! - `cf_fail` is an ordinary SDK error (`Err`), so it returns code 1 without
//!   publishing text: the typed SDK cannot allocate a VARCHAR result and then
//!   fail. That is exactly what the capability word reports — bit 0 (cf_fail
//!   allocates before failing) is **clear**, bit 1 (cf_unwind really panics)
//!   is **set**, so `pinkdb_conformance_caps()` returns 2.
//!
//! Three more exports are introspection, not part of the ABI:
//!
//! ```c
//! int64_t pinkdb_conformance_calls(void);            /* callbacks invoked */
//! int64_t pinkdb_conformance_foreign_threads(void);  /* ... on another thread */
//! int64_t pinkdb_conformance_caps(void);             /* behaviour bit set */
//! ```

// `cf_all` takes the fixture's eight arguments, and `pinkdb_extension!`
// accepts doc comments only on a declared function — not an attribute.
#![allow(clippy::too_many_arguments)]

use std::ffi::{c_char, c_void};
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::ThreadId;

use pinkdb_ext_sdk::{Date, Interval, Timestamp, Variant, pinkdb_extension};

/// The fixture's capability word (see the module docs): bit 0 clear, bit 1
/// set.
const CAPS: i64 = 2;

/// The ABI's own limit on a VARCHAR result; `cf_fill` refuses past it instead
/// of asking the host to reject the pointer.
const MAX_FILL: i64 = 64 * 1024 * 1024;

/// Callbacks invoked, and those of them that ran on a thread other than the
/// init thread. Atomics because callbacks run concurrently on host threads.
static CALLS: AtomicI64 = AtomicI64::new(0);
static FOREIGN_THREADS: AtomicI64 = AtomicI64::new(0);

/// The OS thread `pinkdb_extension_init` ran on — set there, read by every
/// callback.
static INIT_THREAD: OnceLock<ThreadId> = OnceLock::new();

/// Count one callback. Every fixture function starts with this, so
/// `pinkdb_conformance_calls` counts callbacks rather than rows.
fn tally() {
    CALLS.fetch_add(1, Ordering::Relaxed);
    let current = std::thread::current().id();
    if INIT_THREAD.get().is_some_and(|init| *init != current) {
        FOREIGN_THREADS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Render `<prefix><value>` into `out`, or `<prefix>null` for a NULL argument
/// — the field format `cf_all` is defined by.
fn field<T: std::fmt::Display>(out: &mut String, prefix: &str, value: Option<T>) {
    out.push_str(prefix);
    match value {
        Some(value) => {
            let _ = write!(out, "{value}");
        }
        None => out.push_str("null"),
    }
}

pinkdb_extension! {
    name = "conformance_ext",
    functions = [
        /// The argument's bytes, verbatim: embedded NULs, multibyte text and
        /// the empty string all survive, because the ABI carries a length and
        /// never a terminator.
        fn cf_echo(input: Option<&str>) -> Option<String> {
            tally();
            input.map(str::to_owned)
        },

        fn cf_bool(value: Option<bool>) -> Option<bool> {
            tally();
            value
        },

        fn cf_int(value: Option<i32>) -> Option<i32> {
            tally();
            value
        },

        fn cf_bigint(value: Option<i64>) -> Option<i64> {
            tally();
            value
        },

        fn cf_sum_bigint(value: Option<i64>) -> Option<i64> {
            tally();
            value
        },

        fn cf_hugeint(value: Option<i128>) -> Option<i128> {
            tally();
            value
        },

        fn cf_uhugeint(value: Option<u128>) -> Option<u128> {
            tally();
            value
        },

        /// Bit for bit: the payload travels as a double, so NaN payloads and
        /// ±inf are the host's bits, not a re-formatted decimal.
        fn cf_double(value: Option<f64>) -> Option<f64> {
            tally();
            value
        },

        fn cf_date(value: Option<Date>) -> Option<Date> {
            tally();
            value
        },

        fn cf_timestamp(value: Option<Timestamp>) -> Option<Timestamp> {
            tally();
            value
        },

        /// The three calendar fields round-trip exactly; the SDK's `Interval`
        /// is the (months, days, micros) triple, so the ABI's bit packing
        /// never appears here.
        fn cf_interval(value: Option<Interval>) -> Option<Interval> {
            tally();
            value
        },

        /// Opaque VARIANT bytes on the text channel: the SDK copies them out
        /// of host memory and the host decodes what comes back.
        fn cf_variant(value: Option<Variant>) -> Option<Variant> {
            tally();
            value
        },

        /// The canonical one-line summary the fixture defines:
        ///
        /// ```text
        /// b=<bool>,i=<int>,big=<bigint>,d=<double bits>,v=<varchar>,date=<days>,ts=<micros>,iv=<months>/<days>/<micros>
        /// ```
        ///
        /// Doubles render as their 16-digit hex bit pattern so four
        /// languages' float formatting never enters the contract; a NULL
        /// argument renders as `null`.
        fn cf_all(
            b: Option<bool>,
            i: Option<i32>,
            big: Option<i64>,
            d: Option<f64>,
            v: Option<&str>,
            date: Option<Date>,
            ts: Option<Timestamp>,
            iv: Option<Interval>,
        ) -> String {
            tally();
            let mut out = String::new();
            out.push_str("b=");
            match b {
                Some(true) => out.push('1'),
                Some(false) => out.push('0'),
                None => out.push_str("null"),
            }
            field(&mut out, ",i=", i);
            field(&mut out, ",big=", big);
            out.push_str(",d=");
            match d {
                Some(x) => {
                    let _ = write!(out, "0x{:016x}", x.to_bits());
                }
                None => out.push_str("null"),
            }
            out.push_str(",v=");
            match v {
                Some(v) => out.push_str(v),
                None => out.push_str("null"),
            }
            field(&mut out, ",date=", date.map(|d| d.0));
            field(&mut out, ",ts=", ts.map(|t| t.0));
            out.push_str(",iv=");
            match iv {
                Some(iv) => {
                    let _ = write!(out, "{}/{}/{}", iv.months, iv.days, iv.micros);
                }
                None => out.push_str("null"),
            }
            out
        },

        /// A zero-length, non-NULL VARCHAR: no buffer, `text_len` 0, which the
        /// host reads as the empty string.
        fn cf_empty() -> String {
            tally();
            String::new()
        },

        /// `n` bytes of `'x'` (host-allocated through the SDK's owning text
        /// type); a negative `n` is a NULL result, not an error. `n` past the
        /// ABI's 64 MiB VARCHAR limit is refused here, before the allocation.
        fn cf_fill(n: Option<i64>) -> Result<Option<String>, String> {
            tally();
            match n {
                None | Some(..0) => Ok(None),
                Some(n) if n > MAX_FILL => {
                    Err(format!("cf_fill({n}) exceeds the 64 MiB VARCHAR limit"))
                }
                Some(n) => Ok(Some("x".repeat(n as usize))),
            }
        },

        /// Fails with code 1. The fixture's richer shape — allocate 4 bytes of
        /// `"oops"`, publish them on `out`, then return 1 — is outside the
        /// typed SDK, which is why capability bit 0 is clear.
        fn cf_fail(n: Option<i64>) -> Result<String, String> {
            tally();
            Err(format!(
                "cf_fail fails on purpose (argument {})",
                n.map(|n| n.to_string()).unwrap_or_default()
            ))
        },

        /// The SDK's worst case: a real panic inside the callback body. The
        /// SDK's trampoline contains it and answers code 2 (capability bit 1).
        fn cf_unwind() -> i64 {
            tally();
            panic!("conformance_ext: cf_unwind panics on purpose")
        },

        /// A plain callback; the counters in `tally` are what the test reads.
        fn cf_thread() -> i64 {
            tally();
            1
        },
    ],
    volatile = [
        /// Declared through the SDK's volatile registration: a function that is
        /// not a pure function of its arguments, declared honestly (it is here
        /// to prove the SDK sets `PINKDB_EXT_FLAG_VOLATILE`, and fails the load
        /// on a host that cannot accept the flag).
        fn cf_volatile() -> i64 {
            tally();
            42
        },
    ],
}

// ---------------------------------------------------------------------------
// ABI entry points
//
// The macro's `pinkdb_abi_exports!()` is the normal path; the fixture writes
// the same three exports by hand for one reason — recording the init thread,
// which no hook in the SDK exposes. The bodies are the declared constants and
// the SDK's own init, so behaviour is identical.
// ---------------------------------------------------------------------------

/// `pinkdb_extension_abi_version` (ABI v1).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_extension_abi_version() -> u32 {
    pinkdb_ext_sdk::__private::ABI_VERSION
}

/// `pinkdb_extension_name` (ABI v1): NUL-terminated UTF-8, copied by the host
/// immediately.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_extension_name() -> *const c_char {
    PINKDB_EXTENSION_NAME_C.as_ptr().cast::<c_char>()
}

/// `pinkdb_extension_init` (ABI v1): record the init thread, then register
/// every declared function through the host API. Never unwinds.
///
/// # Safety
///
/// The loader passes a live, initialized host API and the opaque context to
/// hand back; `native_init` documents both.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pinkdb_extension_init(
    api: *const pinkdb_ext_sdk::__private::HostApi,
    host_ctx: *mut c_void,
) -> i32 {
    // Before registering: every callback on another thread is what
    // `pinkdb_conformance_foreign_threads` reports.
    let _ = INIT_THREAD.set(std::thread::current().id());
    // SAFETY: the loader's contract for `pinkdb_extension_init`.
    unsafe {
        pinkdb_ext_sdk::__private::native_init(
            api,
            host_ctx,
            PINKDB_EXTENSION_NAME,
            pinkdb_scalar_specs(),
        )
    }
}

// ---------------------------------------------------------------------------
// Introspection (not part of the ABI; the loader tests read these directly)
// ---------------------------------------------------------------------------

/// Callbacks invoked since the library was loaded.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_conformance_calls() -> i64 {
    CALLS.load(Ordering::Relaxed)
}

/// Callbacks that ran on a thread other than the init thread.
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_conformance_foreign_threads() -> i64 {
    FOREIGN_THREADS.load(Ordering::Relaxed)
}

/// The capability word: which of the fixture's two awkward behaviours this SDK
/// really performs (see the module docs).
#[unsafe(no_mangle)]
pub extern "C" fn pinkdb_conformance_caps() -> i64 {
    CAPS
}
