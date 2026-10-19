//! Safe Rust SDK for pinkdb extensions (ABI v1).
//!
//! An extension author writes plain Rust — typed parameters, typed return
//! values, `Result<_, String>` for errors, no `unsafe` anywhere — and a
//! single [`pinkdb_extension!`] declaration generates the boilerplate:
//! value marshalling, text ownership, NULL handling, the ABI v1 exports, and
//! the wave-49 **linked** registration (the wasm loading model, which native
//! targets share). The same declaration therefore builds a native cdylib and
//! a linked/wasm extension from one source.
//!
//! ```no_run
//! pinkdb_ext_sdk::pinkdb_extension! {
//!     name = "string_ext",
//!     functions = [
//!         fn ext_repeat(input: &str, count: Option<i64>) -> Result<Option<String>, String> {
//!             Ok(count.map(|n| input.repeat(n.max(0) as usize)))
//!         },
//!     ]
//! }
//!
//! // Native: `cargo build --release` a cdylib, then
//! //   LOAD EXTENSION '/path/to/libstring_ext.dylib';
//! // Linked (every target, including wasm): publish at host bootstrap
//! //   pinkdb_register_linked()?;
//! // then `LOAD EXTENSION 'string_ext';`
//! # Ok::<(), pinkdb_ext_sdk::ExtError>(())
//! ```
//!
//! # Types
//!
//! Parameters and return values are the ABI v1 types, wrapped in the Rust
//! spelling the author prefers:
//!
//! | SQL type | parameter type | return type |
//! |---|---|---|
//! | `BOOLEAN` | `bool` | `bool` |
//! | `INTEGER` | `i32` | `i32` |
//! | `BIGINT` | `i64` | `i64` |
//! | `DOUBLE` | `f64` | `f64` |
//! | `VARCHAR` | `&str` (borrowed) or `String` (owned) | `String`, `&'static str`, or `Option<_>` of those |
//! | `DATE` | [`Date`] | [`Date`] |
//! | `TIMESTAMP` | [`Timestamp`] | [`Timestamp`] |
//!
//! `Option<T>` around any of them means "NULL is a normal value": a NULL
//! argument decodes to `None`, and `None` encodes to SQL NULL. A NULL that
//! reaches a non-`Option` parameter is a clear error (per the header, the
//! callback decides what NULL means — the SDK makes that decision explicit).
//!
//! # Deterministic by default; volatile functions are declared separately
//!
//! **A function listed under `functions` is assumed deterministic.** A
//! deterministic function's result depends only on its arguments, so the
//! engine may WAL-log a statement that calls it and re-execute that statement
//! on recovery. A function that reads the clock, a random source, or other
//! ambient state is *not* deterministic and is declared under `volatile`
//! instead:
//!
//! ```no_run
//! # pinkdb_ext_sdk::pinkdb_extension! {
//! #     name = "clocky",
//! #     functions = [],
//! volatile = [
//! fn ext_now_micros() -> Result<i64, String> {
//!     // `SystemTime::now()` would make replay diverge; declaring the function
//!     // volatile makes the engine refuse the call wherever that could happen.
//!     Ok(0)
//! }
//! #     ]
//! # }
//! ```
//!
//! The engine then **refuses to execute a volatile call in any statement it
//! WAL-logs** (`INSERT`, `UPDATE`, `DELETE`, DDL, `CALL`, ...) with a clear
//! error — replay re-executes logged SQL, and a nondeterministic callback
//! could produce a different value there. Read-only statements (a plain
//! `SELECT`) never reach the WAL and may call volatile functions freely. The
//! declaration is the author's contract: list a function under `volatile`
//! whenever it is not a pure function of its arguments. Forgetting to is a
//! bug that can make a durable store replay divergent data; listing a pure
//! function as volatile only costs it the right to run in a WAL-logged
//! statement.
//!
//! # Errors
//!
//! `Err(message)` fails the enclosing SQL statement, as does an argument the
//! declared signature does not accept (a NULL reaching a plain, non-`Option`
//! parameter; VARCHAR bytes that are not UTF-8). On native targets the C ABI
//! carries an error *code*, not text (the header's contract), so those are
//! reported as `extension function <name>() failed with code 1` — the message
//! is deliberately not printed anywhere (a failing function is a normal SQL
//! error). SDK/ABI contract failures (a panic in user code, an allocation
//! failure, a host that violates the ABI) return code 2 and print a
//! diagnostic to stderr. Under the **linked** model the message itself
//! reaches SQL as `extension function <name>() failed: <message>`.
//!
//! # One declaration, two loading models
//!
//! [`pinkdb_scalar_specs`](pinkdb_extension)`()` is the generated list both
//! models consume; [`pinkdb_linked_extension`](pinkdb_extension)`()` and
//! [`pinkdb_register_linked`](pinkdb_extension)`()` expose the wave-49
//! registration, and the three `pinkdb_extension_*` exports (native only)
//! are what `LOAD EXTENSION '<path>'` drives. See `ext-sdk/rust/README.md`
//! for the layout, the template, and the sharp edges.

mod macros;
#[cfg(not(target_arch = "wasm32"))]
mod native;
mod spec;
mod value;

pub use pinkdb_ext_loader::{
    ExtArg, ExtError, ExtType, ExtValue, LinkedExtension, LinkedFunction, linked,
};
pub use spec::ScalarSpec;
pub use value::{Date, ExtArgType, FromExtArg, IntoExtValue, Timestamp};

/// Publish a linked extension in the process-global registry (wave 49);
/// the generated [`pinkdb_register_linked`](crate::pinkdb_extension) wraps
/// this for the declared extension.
pub fn register_linked_extension(ext: LinkedExtension) -> Result<(), ExtError> {
    linked::register(ext)
}

/// Items the generated code uses. Not a stable API.
#[doc(hidden)]
pub mod __private {
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::native::native_init;
    #[cfg(not(target_arch = "wasm32"))]
    pub use pinkdb_ext_loader::PinkdbHostApi as HostApi;

    /// [`crate::ExtType::tag`]-compatible: the ABI version the generated
    /// `pinkdb_extension_abi_version` returns.
    #[cfg(not(target_arch = "wasm32"))]
    pub const ABI_VERSION: u32 = pinkdb_ext_loader::PINKDB_EXT_ABI_VERSION;
}
