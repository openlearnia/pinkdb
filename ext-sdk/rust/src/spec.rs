//! The target-independent description of one registered function.
//!
//! The registration macro emits one [`ScalarSpec`] per declared function and
//! both loading models consume the same list: the native exports register
//! each spec through the host API (one `extern "C"` trampoline serves them
//! all, keyed by the spec pointer it receives as `user_data`), and
//! [`crate::LinkedExtension`]'s registration turns each spec into a
//! [`LinkedFunction`](pinkdb_ext_loader::LinkedFunction). One definition
//! therefore covers a native cdylib and a linked (wasm) target.

use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

/// One function a `pinkdb_extension!` declaration contributes.
pub struct ScalarSpec {
    /// SQL name, as registered (the engine matches it case-insensitively).
    pub name: &'static str,
    /// Declared argument types, in declaration order.
    pub args: &'static [ExtType],
    /// Declared return type (`PINKDB_EXT_TYPE_*`).
    pub ret: ExtType,
    /// Volatile (nondeterministic) declaration: the engine refuses to run
    /// this function in any statement it WAL-logs.
    pub volatile: bool,
    /// The typed body: called once per row with the row's decoded arguments.
    /// `Err` fails the enclosing SQL statement.
    pub call: fn(&[ExtArg<'_>]) -> Result<ExtValue, String>,
}
