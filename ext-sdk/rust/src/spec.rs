//! The target-independent description of one registered function.
//!
//! The registration macro emits one [`ScalarSpec`] per declared function and
//! both loading models consume the same list: the native exports register
//! each spec through the host API (one `extern "C"` trampoline serves them
//! all, keyed by the spec pointer it receives as `user_data`), and
//! [`crate::LinkedExtension`]'s registration turns each spec into a
//! [`LinkedFunction`](pinkdb_ext_loader::LinkedFunction). One definition
//! therefore covers a native cdylib and a linked (wasm) target.

use pinkdb_ext_loader::{ExtArg, ExtError, ExtType, ExtValue, LinkedFunction};

/// One function a `pinkdb_extension!` declaration contributes.
pub struct ScalarSpec {
    /// SQL name, as registered (the engine matches it case-insensitively).
    pub name: &'static str,
    /// Declared argument types, in declaration order. For a variadic function
    /// this is the declared prefix (argument types past it repeat the last
    /// entry — see [`Self::variadic`]).
    pub args: &'static [ExtType],
    /// Smallest accepted argument count. Equal to `args.len()` for a fixed
    /// function; lower for an `optional(min)`/`variadic(min)` declaration.
    pub min_args: usize,
    /// Whether arguments past [`Self::args`] repeat its last type (a
    /// `variadic(min)` declaration).
    pub variadic: bool,
    /// Declared return type (`PINKDB_EXT_TYPE_*`).
    pub ret: ExtType,
    /// Volatile (nondeterministic) declaration: the engine refuses to run
    /// this function in any statement it WAL-logs.
    pub volatile: bool,
    /// The typed body: called once per row with the row's decoded arguments.
    /// `Err` fails the enclosing SQL statement.
    pub call: fn(&[ExtArg<'_>]) -> Result<ExtValue, String>,
}

impl ScalarSpec {
    /// Largest accepted argument count: the declared count, or the ABI's
    /// arity limit for a variadic declaration.
    pub fn max_args(&self) -> usize {
        if self.variadic {
            pinkdb_ext_loader::PINKDB_EXT_MAX_ARGS as usize
        } else {
            self.args.len()
        }
    }

    /// The declared type of argument `index` (0-based): the positional type,
    /// or the repeated last type of a variadic declaration. The index is
    /// clamped to the declared list, so this is defined for every index of a
    /// declaration that has at least one argument type.
    pub fn arg_type_at(&self, index: usize) -> ExtType {
        debug_assert!(
            !self.args.is_empty(),
            "a zero-argument declaration has no argument type"
        );
        self.args[index.min(self.args.len() - 1)]
    }

    /// Whether this declaration expresses an **arity range** (a shorter call
    /// is legal), i.e. `optional(min)` / `variadic(min)`.
    ///
    /// The linked loading model registers these through
    /// [`LinkedFunction::new_optional`] / [`LinkedFunction::new_variadic`].
    /// The native ABI v1 has no ranged-arity descriptor (a registration
    /// carries exactly `n_args` types), so a native load refuses such a
    /// specification instead of registering it as fixed-arity — see the
    /// crate docs' "Ranged arity" section.
    pub fn is_ranged(&self) -> bool {
        self.variadic || self.min_args < self.args.len()
    }

    /// This specification as a [`LinkedFunction`] — the wave-49 registration
    /// the generated `pinkdb_linked_extension()` publishes. Fixed, ranged and
    /// variadic declarations all go through here, so one declaration covers
    /// every arity the linked model accepts.
    pub fn linked_function(&self) -> Result<LinkedFunction, ExtError> {
        let mut function = if self.variadic {
            LinkedFunction::new_variadic(
                self.name,
                self.args.to_vec(),
                self.min_args,
                self.ret,
                self.call,
            )?
        } else if self.min_args < self.args.len() {
            LinkedFunction::new_optional(
                self.name,
                self.args.to_vec(),
                self.min_args,
                self.ret,
                self.call,
            )?
        } else {
            LinkedFunction::new(self.name, self.args.to_vec(), self.ret, self.call)?
        };
        if self.volatile {
            function = function.volatile();
        }
        Ok(function)
    }
}
