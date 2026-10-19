//! `my_extension` — a pinkdb extension skeleton (ABI v1, Rust SDK).
//!
//! Copy this directory, rename the crate and the extension, and replace the
//! function bodies. Build it:
//!
//! ```sh
//! make            # or: cargo build --release   (from this directory)
//! ```
//!
//! Then load it from a pinkdb REPL (release CLI):
//!
//! ```sql
//! LOAD EXTENSION '/abs/path/to/target/release/libmy_extension.dylib';
//! SELECT my_ext_greet('world');        -- Hello, world!
//! SELECT my_ext_greet(NULL) IS NULL;   -- true
//! SELECT my_ext_greet(1);              -- Error: my_ext_greet argument 1 must be VARCHAR ...
//! ```
//!
//! The contract lives in `../../include/pinkdb_extension.h` (byte-identical to
//! the loader crate's copy). The SDK-specific rules that matter most:
//!
//!  1. Types are the ABI v1 types, exactly as written: `bool`, `i32`, `i64`,
//!     `f64`, `&str`/`String`, `Date`, `Timestamp`. Wrap a parameter in
//!     `Option<T>` to accept NULL; a NULL reaching a plain parameter fails
//!     the statement instead of being silently reinterpreted.
//!  2. A deterministic function (listed under `functions`) must be a pure
//!     function of its arguments: the engine may WAL-log a statement that
//!     calls it and re-execute that statement on recovery. A function that
//!     reads a clock, a random source or ambient state belongs under
//!     `volatile`, and the engine then refuses to run it in any WAL-logged
//!     statement. Deterministic is the default.
//!  3. `Err(String)` fails the enclosing SQL statement. On native targets the
//!     C ABI carries an error code, not text: a returned `Err` surfaces as
//!     `extension function <name>() failed with code 1`. (Under the linked
//!     model the message reaches SQL too.)
//!  4. Callbacks run concurrently on the host's rayon worker threads: guard
//!     any shared mutable state (atomics, locks) and never assume init and a
//!     callback run on the same thread. The SDK catches panics at the ABI
//!     boundary, but they still fail the statement — return `Err` instead.
//!  5. One declaration per crate: the macro defines the extension's exported
//!     symbols. Use one module per extension if you need to organize code.

use pinkdb_ext_sdk::pinkdb_extension;

pinkdb_extension! {
    name = "my_extension",
    functions = [
        /// Greet `name`: `my_ext_greet('world')` -> `Hello, world!`.
        ///
        /// `Option<&str>` accepts NULL (returned as SQL NULL here). A
        /// `&str` parameter would borrow the host's bytes for the call;
        /// `String` copies them if the value must outlive the call.
        fn my_ext_greet(name: Option<&str>) -> Result<Option<String>, String> {
            match name {
                Some(name) => Ok(Some(format!("Hello, {name}!"))),
                None => Ok(None),
            }
        },
    ],
    // Nondeterministic functions go here; the engine refuses to run them in a
    // statement it WAL-logs (INSERT/UPDATE/DELETE/DDL/CALL). For example:
    //
    // volatile = [
    //     fn my_ext_now_micros() -> i64 { /* read a clock */ }
    // ],
}
