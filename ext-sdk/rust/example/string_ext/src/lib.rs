//! `rust_string_ext` — the worked example for the pinkdb Rust extension SDK.
//!
//! One declaration defines the extension: its SQL name and four typed
//! functions. The SDK generates the linked registration
//! (`pinkdb_register_linked()`), and the explicit `pinkdb_abi_exports!()`
//! call below emits the ABI v1 exports this cdylib needs to be loaded by
//! `LOAD EXTENSION '<path>'`.
//!
//! Build and try it:
//!
//! ```sh
//! cargo build --release -p pinkdb-ext-sdk-example
//! # then, in the pinkdb CLI:
//! #   LOAD EXTENSION '<repo>/target/release/librust_string_ext.dylib';
//! #   SELECT ext_rs_repeat('ab', 3);          -- ababab
//! #   SELECT ext_rs_len('héllo');             -- 6
//! #   SELECT ext_rs_next_day(DATE '2024-02-28'); -- 2024-02-29
//! #   SELECT ext_rs_tick();                   -- a fresh counter value
//! #   INSERT INTO t VALUES (ext_rs_tick());   -- refused: volatile
//! ```
//!
//! `./ext-sdk/rust/smoke.sh` runs exactly that session end to end.

use std::sync::atomic::{AtomicI64, Ordering};

use pinkdb_ext_sdk::{Date, pinkdb_abi_exports, pinkdb_extension};

/// A process-local counter behind [`ext_rs_tick`]. Extension callbacks run
/// concurrently on the host's rayon workers, so shared state must be
/// synchronized — an atomic is enough here.
static TICKS: AtomicI64 = AtomicI64::new(0);

pinkdb_extension! {
    name = "rust_string_ext",
    functions = [
        /// Repeat `input`. A NULL count means "repeat once"; a NULL input
        /// returns NULL (the `Option` parameters make that explicit — a NULL
        /// reaching a plain parameter would be an error instead).
        fn ext_rs_repeat(input: Option<&str>, count: Option<i64>) -> Result<Option<String>, String> {
            let Some(text) = input else {
                return Ok(None);
            };
            let times = count.unwrap_or(1);
            if !(0..=4096).contains(&times) {
                return Err(format!("repeat count {times} is outside 0..=4096"));
            }
            Ok(Some(text.repeat(times as usize)))
        },

        /// The length of `input` in **bytes** (the ABI's VARCHAR is bytes, not
        /// characters; `length()` is the engine's character count).
        fn ext_rs_len(input: Option<&str>) -> Option<i64> {
            input.map(|s| s.len() as i64)
        },

        /// Clamp `value` into `low..=high`. Plain (non-`Option`) parameters:
        /// a NULL reaching one fails the statement (declare `Option<T>` to
        /// receive NULLs explicitly).
        fn ext_rs_clamp(value: i64, low: i64, high: i64) -> Result<i64, String> {
            if low > high {
                return Err(format!("low ({low}) is greater than high ({high})"));
            }
            Ok(value.clamp(low, high))
        },

        /// The day after `day` — the docs' example of the DATE type, whose
        /// representation is days since 1970-01-01.
        fn ext_rs_next_day(day: Option<Date>) -> Option<Date> {
            day.map(|d| Date(d.0 + 1))
        },
    ],
    volatile = [
        /// A clock-like counter: every call returns a fresh value, so it is
        /// **volatile** and must never run in a statement the engine
        /// WAL-logs (an `INSERT` calling it is refused before it runs).
        fn ext_rs_tick() -> i64 {
            TICKS.fetch_add(1, Ordering::Relaxed)
        },
    ],
}

// This crate is cdylib-only, so it opts in to the ABI v1 `#[no_mangle]`
// exports that `LOAD EXTENSION '<path>'` looks up. A linked-only (rlib)
// extension must *not* emit them: two rlibs carrying the same globals in one
// binary fail to link on Linux (`duplicate symbol: pinkdb_extension_init`).
pinkdb_abi_exports!();
