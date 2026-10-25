//! `my_extension` — a pinkdb extension skeleton (ABI v1, Rust SDK).
//!
//! Copy this directory, rename the crate and the extension, and replace the
//! function bodies. Build it:
//!
//! ```sh
//! make            # or: cargo build --release   (from this directory)
//! ```
//!
//! Loading is opt-in: start the CLI (or server) with `--allow-load-extension`
//! or set `PINKDB_ALLOW_LOAD_EXTENSION=1`, or the `LOAD EXTENSION` above is
//! refused. `--extension-root DIR` confines loads to DIR (a bare name, a `..`
//! component and a symlink are always refused), and `--extension-pin
//! PATH=SHA256` (repeatable) pins the bytes. A local library is unsigned —
//! Ed25519 covers `.install` packages only — and runs with the loader's
//! privileges.
//!
//! Then load it from a pinkdb REPL (release CLI):
//!
//! ```sql
//! LOAD EXTENSION '/abs/path/to/target/release/libmy_extension.dylib';
//! SELECT my_ext_greet('world');        -- Hello, world!
//! SELECT my_ext_greet(NULL) IS NULL;   -- true
//! SELECT my_ext_greet(1);              -- Error: my_ext_greet argument 1 must be VARCHAR ...
//! SELECT my_ext_tick();                -- 0, 1, 2, ...   (volatile: reads allowed)
//! INSERT INTO t VALUES (my_ext_tick()); -- Error: ... is volatile (nondeterministic) ...
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
//!  5. One declaration per crate, and one module per extension. The
//!     declaration generates the shared function list and the linked
//!     registration; the ABI v1 exports are an explicit opt-in because only a
//!     cdylib may carry them (see the `pinkdb_abi_exports!` call below).
//!  6. Ranged arities (`optional(min)` / `variadic(min)`, documented in the
//!     SDK README) are **linked-only**: the native ABI v1 registers a fixed
//!     argument count, so a declaration that uses one fails a native load.
//!     This skeleton stays fixed-arity so it builds a loadable cdylib.
//!  7. `pinkdb_abi_exports!()` below is the whole native entry-point
//!     boilerplate: it pins the ABI version (`pinkdb_extension_abi_version`),
//!     the name (`pinkdb_extension_name`) and runs
//!     `pinkdb_extension_init`, which negotiates `struct_size` with the host
//!     and registers every declared function. There is nothing to write by
//!     hand unless the extension *serves* instead of returning scalars — see
//!     `pinkdb_abi_exports_with_serve!` at the bottom.

use std::sync::atomic::{AtomicI64, Ordering};

use pinkdb_ext_sdk::{pinkdb_abi_exports, pinkdb_extension};

/// Bumped by `my_ext_tick`: a volatile function may read ambient state, so
/// this is the skeleton's worked example of shared state across callbacks
/// (they run concurrently on host worker threads).
static TICKS: AtomicI64 = AtomicI64::new(0);

pinkdb_extension! {
    name = "my_extension",
    functions = [
        /// Greet `name`: `my_ext_greet('world')` -> `Hello, world!`.
        ///
        /// `Option<&str>` accepts NULL (returned as SQL NULL here). A
        /// `&str` parameter would borrow the host's bytes for the call;
        /// `String` copies them if the value must outlive the call. The
        /// returned `String` is the SDK's owning text type: the host
        /// allocates the result's bytes and copies them before the call
        /// returns, so no lifetime escapes this body.
        fn my_ext_greet(name: Option<&str>) -> Result<Option<String>, String> {
            match name {
                Some(name) => Ok(Some(format!("Hello, {name}!"))),
                None => Ok(None),
            }
        },
    ],
    // Everything nondeterministic goes here. The engine then refuses to run it
    // in a statement it WAL-logs (INSERT/UPDATE/DELETE/DDL/CALL): replaying
    // the statement could produce a different value. A plain `SELECT` may
    // still call it. Move a function to this list the moment it reads a
    // clock, a random source or other ambient state.
    volatile = [
        /// A read-only counter: `SELECT my_ext_tick();` works, `INSERT ...
        /// my_ext_tick()` is refused.
        fn my_ext_tick() -> i64 {
            TICKS.fetch_add(1, Ordering::Relaxed)
        },
    ],
}

// A skeleton that ships a shared library: opt in to the ABI v1
// `#[no_mangle]` exports (`pinkdb_extension_abi_version`, `_name`, `_init`)
// that `LOAD EXTENSION '<path>'` looks up. A crate that is only ever linked
// into a host as an rlib must not call this — two linked extensions carrying
// the same globals fail to link on Linux with
// `duplicate symbol: pinkdb_extension_init`.
pinkdb_abi_exports!();

// An extension that *serves* a protocol (speaks to the client itself instead
// of registering scalar functions) swaps the line above for this one, which
// also exports `pinkdb_ext_serve` (ABI minor 2):
//
// ```rust
// fn serve(
//     session: &mut pinkdb_ext_sdk::HostSession,
//     transport: &str,
//     options: &str,
// ) -> Result<(), String> {
//     // Ask the host through `session.execute_statement`; do not touch the
//     // client after this returns.
//     let _ = (session, transport, options);
//     Ok(())
// }
// pinkdb_ext_sdk::pinkdb_abi_exports_with_serve!(serve);
// ```
//
// A serve extension usually declares `functions = []`: the host blocks in
// `pinkdb_ext_serve` for as long as the session lasts.
