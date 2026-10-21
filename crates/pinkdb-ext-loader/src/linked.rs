//! The **linked** extension model: the wasm-compatible loading path.
//!
//! Native extensions arrive as a shared library on disk and are opened with
//! `dlopen` (see [`crate::load`] and `include/pinkdb_extension.h`). A wasm
//! sandbox has no dynamic linker and no filesystem, so there an extension is
//! *linked into the host* — an rlib dependency of the embedder, or code the
//! embedder itself wrote — and published to a process-global **registry**
//! through the safe Rust API of this module. Nothing is opened at run time:
//! `LOAD EXTENSION '<name>'` resolves the name against the registry and
//! registers the functions the extension declared.
//!
//! The two models share [`ExtType`], [`ExtArg`], [`ExtValue`] and the arity
//! limit, but not the calling convention:
//!
//! | | `dlopen` (ABI v1, [`crate::load`]) | linked registry (this module) |
//! |---|---|---|
//! | how the code arrives | shared library on disk | linked into the host binary |
//! | how it is addressed | filesystem path | registry name |
//! | how functions register | `pinkdb_extension_init` calls `api->register_scalar` | [`LinkedExtension::new`] at host bootstrap |
//! | callback | `extern "C"` over `pinkdb_ext_value` structs | [`LinkedScalar`]: safe Rust over [`ExtArg`] / [`ExtValue`] |
//! | text ownership | extension `alloc`s, host copies then `free`s | values by value, no ownership transfer |
//! | errors | `i32` return code | `Result<_, String>` |
//! | panics | forbidden (undefined behavior) | caught at the boundary, reported as a statement error |
//! | thread safety | promised by the extension | required by the type system (`Send + Sync`) |
//! | volatility | `PINKDB_EXT_FLAG_VOLATILE` at registration (default: deterministic) | [`LinkedFunction::volatile`] (default: deterministic) |
//! | version check | `PINKDB_EXT_ABI_VERSION` compared at load | none: the Rust type system checks the ABI at compile time |
//! | lifecycle | `dlopen`; image stays mapped | process-lifetime registration; no unload |
//!
//! Whichever model declared a function, the engine applies one rule: a
//! *volatile* (nondeterministic) function may not run in a statement the
//! engine WAL-logs, because replaying that statement's SQL could produce a
//! different value than the session that wrote it saw.
//!
//! # Loading rules
//!
//! * **wasm targets** have no dynamic loader: `LOAD EXTENSION '<name>'` (and
//!   `PinkDb::load_extension`) resolve `name` against this registry; nothing
//!   on disk is ever consulted, and an unregistered name is a clear error
//!   ([`ExtError::NotRegistered`]).
//! * **native targets** keep the filesystem model: `LOAD EXTENSION '<path>'`
//!   `dlopen`s the library, and the registry is consulted only when nothing
//!   at that exact string is loadable *and* a linked extension is registered
//!   under it. That fallback is what makes a replayed WAL record
//!   (`LOAD EXTENSION '<name>'`, written when a linked function was called)
//!   resolve on native targets too.
//!
//! Registration is one-shot: publishing a name twice is an error, and an
//! extension stays registered for the life of the process — there is no
//! unload, exactly like `dlopen`'s mapped images. Registration is a
//! *host-side* API: SQL can only activate what the host published, it can
//! never inject code.
//!
//! # Publishing an extension
//!
//! ```no_run
//! use pinkdb_ext_loader::linked::{self, LinkedExtension, LinkedFunction};
//! use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};
//!
//! # fn main() -> Result<(), pinkdb_ext_loader::ExtError> {
//! let ext = LinkedExtension::new(
//!     "math",
//!     vec![
//!         LinkedFunction::new(
//!             "double_it",
//!             vec![ExtType::Integer],
//!             ExtType::Integer,
//!             |args: &[ExtArg<'_>]| match args[0] {
//!                 ExtArg::Integer(i) => Ok(ExtValue::Integer(i * 2)),
//!                 ExtArg::Null(_) => Ok(ExtValue::Null),
//!                 other => Err(format!("unexpected argument {other:?}")),
//!             },
//!         )?,
//!     ],
//! )?;
//! linked::register(ext)?; // host bootstrap; once per process
//! # Ok(())
//! # }
//! ```
//!
//! A wasm-bindgen embedder does the same from its start function, which is
//! how the JavaScript surface below is reached:
//!
//! ```text
//! #[wasm_bindgen(start)]
//! fn start() -> Result<(), JsValue> {
//!     pinkdb_ext_loader::linked::register(js_math()?)?;   // js_math: the shim's LinkedExtension
//!     Ok(())
//! }
//! ```
//!
//! # The JavaScript host surface (contract)
//!
//! A wasm-bindgen embedder (the follow-up `pinkdb-wasm` package) exposes the
//! registry to JavaScript as exactly [`JS_HOST_SURFACE`], with values mapped
//! per [`JS_TYPE_MAPPINGS`]:
//!
//! ```text
//! pinkdb.registerExtension({ name, functions: [{ name, args, ret, fn }] })
//! pinkdb.listExtensions() -> [{ name, functions: [{ name, args, ret }] }]
//! ```
//!
//! `args` / `ret` are the ABI type names (`'INTEGER'`, `'VARCHAR'`, …); `fn`
//! is called once per row with exactly that row's argument values in
//! JavaScript form and returns the row's result (`null` for SQL NULL). The
//! shim registers the extension through [`register`], so from the engine's
//! side a JavaScript function is an ordinary linked function — same binder,
//! same executor, same WAL prefixing. [`JS_SANDBOX_RULES`] states what the
//! shim and its scripts must guarantee; this crate ships and pins the
//! contract, the binding itself is the recorded follow-up (no `wasm-bindgen`
//! dependency is added to the default graph).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::ExtError;
use crate::abi::{ExtArg, ExtType, ExtValue, PINKDB_EXT_MAX_ARGS};

/// The host-side implementation of one linked scalar function.
///
/// Implementations are plain safe Rust: the engine marshals one row into
/// [`ExtArg`]s, calls [`LinkedScalar::call`], and converts the returned
/// [`ExtValue`] back into a column value. This is the linked model's analogue
/// of the C ABI callback without the `extern "C"` and ownership machinery; a
/// blanket impl makes any matching closure an implementor.
///
/// `Send + Sync` is required because the executor calls registered functions
/// from its worker threads on native targets (on wasm there is a single
/// thread; the bound is kept so one extension is portable between targets).
///
/// A closure that implements this trait must annotate its parameter type —
/// `|args: &[ExtArg<'_>]| …` — because the argument types are higher-ranked
/// over two lifetimes and Rust cannot infer that for an unannotated closure;
/// function items need no annotation. Registering a struct with `#[derive]`d
/// state is the other option: implement this trait for it directly.
pub trait LinkedScalar: Send + Sync + 'static {
    /// Compute one row's result.
    ///
    /// `Err(message)` fails the enclosing statement with
    /// `extension function <name>() failed: <message>`; a panic is caught at
    /// the boundary and reported the same way (`… panicked: <message>`), so a
    /// buggy callback fails its statement instead of the engine.
    fn call(&self, args: &[ExtArg<'_>]) -> Result<ExtValue, String>;
}

impl<F> LinkedScalar for F
where
    F: Fn(&[ExtArg<'_>]) -> Result<ExtValue, String> + Send + Sync + 'static,
{
    fn call(&self, args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
        (self)(args)
    }
}

/// One scalar function an extension publishes through the registry: its SQL
/// name, its declared argument types and return type, and the callback.
///
/// A `LinkedFunction` is a cheap `Arc`-shared handle (`Clone`); the shared
/// callback must be `Send + Sync` (see [`LinkedScalar`]). Its metadata is
/// validated once, at [`LinkedFunction::new`], so a handle in hand is always
/// well-formed; [`LinkedFunction::call`] additionally checks every call
/// against the metadata, exactly like the C ABI path checks a callback's
/// result.
#[derive(Clone)]
pub struct LinkedFunction {
    /// Name as registered (case preserved for display; lookup is
    /// case-insensitive host-side).
    name: String,
    /// Declared argument types, in order.
    arg_types: Vec<ExtType>,
    /// Smallest accepted argument count. Equal to the declared count for a
    /// fixed function; lower for [`LinkedFunction::new_optional`] and
    /// [`LinkedFunction::new_variadic`].
    min_args: usize,
    /// Whether arguments past the declared list repeat the last declared
    /// type (a variadic function, capped at [`PINKDB_EXT_MAX_ARGS`]).
    repeats: bool,
    /// Declared return type.
    ret_type: ExtType,
    /// Volatile (nondeterministic) declaration: the engine refuses to run a
    /// volatile call in any statement it WAL-logs. `false` — deterministic —
    /// is the default, matching the native ABI.
    volatile: bool,
    /// Host code.
    callback: Arc<dyn LinkedScalar>,
}

impl std::fmt::Debug for LinkedFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkedFunction")
            .field("name", &self.name)
            .field("n_args", &self.arg_types.len())
            .field("arg_types", &self.arg_types)
            .field("ret_type", &self.ret_type)
            .field("volatile", &self.volatile)
            .finish_non_exhaustive()
    }
}

impl LinkedFunction {
    /// Declare one function with a fixed argument list.
    ///
    /// Fails when `name` is empty or when the arity exceeds
    /// [`PINKDB_EXT_MAX_ARGS`] (the C ABI's limit, kept identical so the two
    /// models accept the same extensions).
    pub fn new(
        name: impl Into<String>,
        arg_types: Vec<ExtType>,
        ret_type: ExtType,
        callback: impl LinkedScalar,
    ) -> Result<Self, ExtError> {
        let min_args = arg_types.len();
        Self::build(name, arg_types, min_args, false, ret_type, callback)
    }

    /// Declare one function whose arity is a **bounded range**: the declared
    /// argument types, but a call may omit trailing arguments down to
    /// `min_args` (`0..=arg_types.len()`). The declared types stay
    /// positional — there is no repetition — so this is how
    /// `json_array_length(json)` / `json_array_length(json, path)` is
    /// declared. `min_args` must not exceed the declared count.
    pub fn new_optional(
        name: impl Into<String>,
        arg_types: Vec<ExtType>,
        min_args: usize,
        ret_type: ExtType,
        callback: impl LinkedScalar,
    ) -> Result<Self, ExtError> {
        Self::build(name, arg_types, min_args, false, ret_type, callback)
    }

    /// Declare one **variadic** function: it accepts at least `min_args`
    /// arguments, and every argument past the declared list repeats the
    /// *last* declared type — `json_array` (VARCHAR values, `min_args` 0) and
    /// `json_object` (VARCHAR keys and values, `min_args` 0, checked for
    /// parity by the callback) are declared this way. The declared list must
    /// not be empty (it names the repeated type) and `min_args` must not
    /// exceed it. A call is capped at [`PINKDB_EXT_MAX_ARGS`] arguments, the
    /// ABI's arity limit.
    pub fn new_variadic(
        name: impl Into<String>,
        arg_types: Vec<ExtType>,
        min_args: usize,
        ret_type: ExtType,
        callback: impl LinkedScalar,
    ) -> Result<Self, ExtError> {
        Self::build(name, arg_types, min_args, true, ret_type, callback)
    }

    /// Shared validation and construction for all three arities.
    fn build(
        name: impl Into<String>,
        arg_types: Vec<ExtType>,
        min_args: usize,
        repeats: bool,
        ret_type: ExtType,
        callback: impl LinkedScalar,
    ) -> Result<Self, ExtError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ExtError::registration("a function name must not be empty"));
        }
        if arg_types.len() > PINKDB_EXT_MAX_ARGS as usize {
            return Err(ExtError::registration(format!(
                "function \"{name}\" declares {} arguments (the maximum is {PINKDB_EXT_MAX_ARGS})",
                arg_types.len()
            )));
        }
        if repeats && arg_types.is_empty() {
            return Err(ExtError::registration(format!(
                "function \"{name}\" is variadic but declares no argument type to repeat"
            )));
        }
        if min_args > arg_types.len() {
            return Err(ExtError::registration(format!(
                "function \"{name}\" declares {min_args} minimum argument(s) but only {} type(s)",
                arg_types.len()
            )));
        }
        Ok(Self {
            name,
            arg_types,
            min_args,
            repeats,
            ret_type,
            volatile: false,
            callback: Arc::new(callback),
        })
    }

    /// Declare this function volatile (nondeterministic): it may read the
    /// clock, a random source, ambient state, ... The engine then refuses to
    /// run it in any statement it WAL-logs (INSERT, UPDATE, DELETE, DDL,
    /// `CALL`, ...) — replaying logged SQL could otherwise produce a
    /// different value than the session that wrote it saw. Read-only
    /// statements may call it.
    ///
    /// Deterministic (unmarked) is the default, exactly like the native
    /// ABI's flag-0 registration: mark a function volatile whenever it is
    /// not a pure function of its arguments.
    ///
    /// ```
    /// # use pinkdb_ext_loader::linked::LinkedFunction;
    /// # use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};
    /// let f = LinkedFunction::new("tick", vec![], ExtType::Integer,
    ///     |_args: &[ExtArg<'_>]| Ok(ExtValue::Integer(1)))?
    ///     .volatile();
    /// assert!(f.is_volatile());
    /// # Ok::<(), pinkdb_ext_loader::ExtError>(())
    /// ```
    pub fn volatile(mut self) -> Self {
        self.volatile = true;
        self
    }

    /// Whether this function is declared volatile (see [`Self::volatile`]).
    pub fn is_volatile(&self) -> bool {
        self.volatile
    }

    /// Registered name (original case).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Declared argument count (the length of [`Self::arg_types`], which is
    /// the *upper* bound of a bounded-range arity and the declared prefix of
    /// a variadic one).
    pub fn n_args(&self) -> usize {
        self.arg_types.len()
    }

    /// Declared argument types, in order.
    pub fn arg_types(&self) -> &[ExtType] {
        &self.arg_types
    }

    /// Smallest accepted argument count (equal to [`Self::n_args`] for a
    /// fixed function).
    pub fn min_args(&self) -> usize {
        self.min_args
    }

    /// Largest accepted argument count: the declared count, or
    /// [`PINKDB_EXT_MAX_ARGS`] for a variadic function.
    pub fn max_args(&self) -> usize {
        if self.repeats {
            PINKDB_EXT_MAX_ARGS as usize
        } else {
            self.arg_types.len()
        }
    }

    /// Whether arguments past the declared list repeat the last declared
    /// type (see [`Self::new_variadic`]).
    pub fn is_variadic(&self) -> bool {
        self.repeats
    }

    /// The declared type of argument `index` (0-based): the positional type,
    /// or the last declared type for a variadic argument past the declared
    /// list. `None` for a zero-argument function: there is no declared type,
    /// and `len - 1` must never underflow (it is OOB in release builds).
    pub fn arg_type_at(&self, index: usize) -> Option<ExtType> {
        let i = index.min(self.arg_types.len().checked_sub(1)?);
        Some(self.arg_types[i])
    }

    /// Declared return type.
    pub fn ret_type(&self) -> ExtType {
        self.ret_type
    }

    /// Invoke the callback once (one row) and return its result.
    ///
    /// The host guarantees at bind time that `args` matches the declared
    /// arity — exactly [`Self::n_args`] for a fixed function, within
    /// `min_args..=max_args` for the ranged arities — with each argument of
    /// [`Self::arg_type_at`]'s type; a mismatch reaching here is reported as
    /// [`ExtError::BadResult`] rather than being undefined. A NULL argument
    /// is passed through as [`ExtArg::Null`] — the callback decides what NULL
    /// means. The result must be NULL or carry the registered return type.
    pub fn call(&self, args: &[ExtArg<'_>]) -> Result<ExtValue, ExtError> {
        if args.len() < self.min_args || args.len() > self.max_args() {
            let message = if self.min_args == self.max_args() {
                format!("expected {} argument(s), got {}", self.min_args, args.len())
            } else if args.len() < self.min_args {
                if self.repeats {
                    format!(
                        "expected at least {} argument(s), got {}",
                        self.min_args,
                        args.len()
                    )
                } else {
                    format!(
                        "expected between {} and {} argument(s), got {}",
                        self.min_args,
                        self.max_args(),
                        args.len()
                    )
                }
            } else {
                format!(
                    "expected at most {} argument(s), got {}",
                    self.max_args(),
                    args.len()
                )
            };
            return Err(ExtError::BadResult {
                function: self.name.clone(),
                message,
            });
        }
        for (i, a) in args.iter().enumerate() {
            let Some(expected) = self.arg_type_at(i) else {
                return Err(ExtError::BadResult {
                    function: self.name.clone(),
                    message: format!(
                        "argument {} was supplied, but the function declares no arguments",
                        i + 1
                    ),
                });
            };
            if a.ext_type() != expected {
                return Err(ExtError::BadResult {
                    function: self.name.clone(),
                    message: format!(
                        "argument {} has type {}, registered as {}",
                        i + 1,
                        a.ext_type(),
                        expected
                    ),
                });
            }
        }

        // A linked callback is ordinary Rust and may panic; the C ABI forbids
        // unwinding across the boundary, here the boundary catches it instead.
        // (Under `panic = "abort"` — a common wasm setting — there is nothing
        // to catch and the configured panic strategy wins.)
        let value = match catch_unwind(AssertUnwindSafe(|| self.callback.call(args))) {
            Ok(Ok(value)) => value,
            Ok(Err(message)) => {
                return Err(ExtError::Failed {
                    function: self.name.clone(),
                    message,
                });
            }
            Err(payload) => {
                return Err(ExtError::Panicked {
                    function: self.name.clone(),
                    message: panic_message(&payload),
                });
            }
        };

        if let Some(got) = value.ext_type()
            && got != self.ret_type
        {
            return Err(ExtError::BadResult {
                function: self.name.clone(),
                message: format!("returned {got}, registered as {}", self.ret_type),
            });
        }
        Ok(value)
    }
}

/// The message of a caught panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// One extension as published by the host: a name and the functions it
/// declares.
///
/// Cheap to clone (the functions are `Arc`-shared handles). The name is the
/// registry key and — on wasm — the string `LOAD EXTENSION` takes; it is
/// matched case-insensitively, like SQL function names.
#[derive(Clone, Debug)]
pub struct LinkedExtension {
    name: String,
    functions: Vec<LinkedFunction>,
}

impl LinkedExtension {
    /// Build an extension record.
    ///
    /// Fails when `name` is empty or when one function name occurs twice
    /// (such a record could never register: the engine keys functions by
    /// lowercased name).
    pub fn new(name: impl Into<String>, functions: Vec<LinkedFunction>) -> Result<Self, ExtError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ExtError::registration(
                "an extension name must not be empty",
            ));
        }
        let mut seen: Vec<String> = Vec::with_capacity(functions.len());
        for f in &functions {
            let key = f.name().to_ascii_lowercase();
            if seen.contains(&key) {
                return Err(ExtError::registration(format!(
                    "function \"{}\" is declared twice",
                    f.name()
                )));
            }
            seen.push(key);
        }
        Ok(Self { name, functions })
    }

    /// The registry key / declared name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared functions, in declaration order.
    pub fn functions(&self) -> &[LinkedFunction] {
        &self.functions
    }
}

/// A host that can accept a linked extension's functions.
///
/// Implementations must be atomic: on error nothing may stay registered.
pub trait LinkedExtHost {
    /// Register every function of `ext`; `Err` leaves the host unchanged.
    fn register_linked_extension(&mut self, ext: LinkedExtension) -> Result<(), ExtError>;
}

/// The process-global linked registry: the wasm analogue of the filesystem
/// native extensions are loaded from.
static REGISTRY: OnceLock<Mutex<Vec<LinkedExtension>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<LinkedExtension>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Lock the registry, recovering from poisoning: a panic while holding it can
/// only come from a test or an embedder, and refusing every later
/// registration would be worse than continuing (the same stance as the
/// engine's store locks).
fn lock() -> MutexGuard<'static, Vec<LinkedExtension>> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Publish `ext` in the process-global registry so a store can load it by
/// name ([`load`], `PinkDb::load_linked_extension`, and on wasm
/// `LOAD EXTENSION '<name>'`).
///
/// Fails when an extension is already registered under `ext`'s name
/// (case-insensitively): registration is a host-side bootstrap step that must
/// run exactly once, so a second registration is a host bug, not a reload.
/// Extensions are never unregistered — the functions a store already holds
/// stay valid for the life of the process, exactly like a mapped library.
pub fn register(ext: LinkedExtension) -> Result<(), ExtError> {
    let mut extensions = lock();
    if let Some(existing) = extensions
        .iter()
        .find(|e| e.name().eq_ignore_ascii_case(ext.name()))
    {
        return Err(ExtError::registration(format!(
            "extension \"{}\" is already registered",
            existing.name()
        )));
    }
    extensions.push(ext);
    Ok(())
}

/// The extension registered under `name` (case-insensitive), if any.
pub fn get(name: &str) -> Option<LinkedExtension> {
    lock()
        .iter()
        .find(|e| e.name().eq_ignore_ascii_case(name))
        .cloned()
}

/// Whether an extension is registered under `name` (case-insensitive).
pub fn is_registered(name: &str) -> bool {
    lock().iter().any(|e| e.name().eq_ignore_ascii_case(name))
}

/// Every registered name, in registration order.
pub fn names() -> Vec<String> {
    lock().iter().map(|e| e.name().to_string()).collect()
}

/// Load the extension registered under `name` into `host`; returns the
/// extension's declared name.
///
/// This is the linked model's [`crate::load_extension`]: no library is opened,
/// the registry entry (which the host published at bootstrap) is handed to
/// `host` verbatim. Fails — without registering anything — when `name` is not
/// registered ([`ExtError::NotRegistered`]) or when `host` rejects the
/// registration.
pub fn load<H: LinkedExtHost + ?Sized>(host: &mut H, name: &str) -> Result<String, ExtError> {
    let ext = get(name).ok_or_else(|| ExtError::NotRegistered {
        name: name.to_string(),
    })?;
    let name = ext.name().to_string();
    host.register_linked_extension(ext)?;
    Ok(name)
}

/// One function of the JavaScript host surface, with its documented
/// signature. Mirrors `pinkdb-js`'s contract style: the surface is shipped
/// and pinned by tests, the binding itself is the recorded follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsHostFunction {
    pub name: &'static str,
    pub signature: &'static str,
    pub docs: &'static str,
}

/// The complete JavaScript surface a wasm-bindgen embedder exposes.
pub const JS_HOST_SURFACE: &[JsHostFunction] = &[
    JsHostFunction {
        name: "pinkdb.registerExtension",
        signature: "pinkdb.registerExtension({ name, functions: [{ name, args, ret, fn, volatile }] })",
        docs: "publish an extension in the linked registry (host bootstrap); throws on invalid \
               metadata or a duplicate name. `name` is the registry key `LOAD EXTENSION '<name>'` \
               takes, `args`/`ret` are ABI type names, `volatile` (optional, default false) \
               declares a nondeterministic function that the engine then refuses to run in any \
               WAL-logged statement, and `fn` is called once per row with that row's argument \
               values and returns the row's result",
    },
    JsHostFunction {
        name: "pinkdb.listExtensions",
        signature: "pinkdb.listExtensions() -> [{ name, functions: [{ name, args, ret }] }]",
        docs: "introspect the registry (registration order); metadata only, never the callbacks",
    },
];

/// One row of the JavaScript value-mapping table: an ABI type name, the
/// JavaScript value a callback receives and returns for it, and the notes a
/// shim must honour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsTypeMapping {
    /// The ABI type name — `ExtType::name()`, the string `args`/`ret` use.
    pub sql_type: &'static str,
    /// The JavaScript value on both sides of the callback.
    pub js_value: &'static str,
    pub notes: &'static str,
}

/// The complete JavaScript value mapping (contract; see the module docs).
///
/// SQL NULL crosses as `null` in either direction and is valid for any
/// declared type; DATE and TIMESTAMP cross as their ABI representation
/// (day / microsecond counts since the epoch) so no parsing is involved.
pub const JS_TYPE_MAPPINGS: &[JsTypeMapping] = &[
    JsTypeMapping {
        sql_type: "BOOLEAN",
        js_value: "boolean",
        notes: "true / false",
    },
    JsTypeMapping {
        sql_type: "INTEGER",
        js_value: "number",
        notes: "integral, 32-bit range; anything else is an error naming the function",
    },
    JsTypeMapping {
        sql_type: "BIGINT",
        js_value: "number",
        notes: "integral, exactly representable in a JavaScript number (|n| <= 2^53); \
                wider results must be cast host-side",
    },
    JsTypeMapping {
        sql_type: "DOUBLE",
        js_value: "number",
        notes: "any finite value; NaN / ±Infinity cross as SQL NULL",
    },
    JsTypeMapping {
        sql_type: "VARCHAR",
        js_value: "string",
        notes: "UTF-8; unpaired surrogates are an error naming the function",
    },
    JsTypeMapping {
        sql_type: "DATE",
        js_value: "number",
        notes: "days since 1970-01-01 (the ABI's i32); render with `new Date(days * 86400000)`",
    },
    JsTypeMapping {
        sql_type: "TIMESTAMP",
        js_value: "number",
        notes: "microseconds since 1970-01-01 00:00:00 (the ABI's i64); \
                `new Date(us / 1000)` loses sub-millisecond precision",
    },
    JsTypeMapping {
        sql_type: "NULL",
        js_value: "null",
        notes: "valid for any declared type, on both sides",
    },
];

/// What a JavaScript (or any host-language) shim and its scripts must
/// guarantee. These are requirements, not enforcement: unlike `pinkdb-js`,
/// which removes the intrinsics that break determinism, the shim cannot
/// sandbox the embedder's JavaScript engine.
pub const JS_SANDBOX_RULES: &[&str] = &[
    "the callback sees exactly its declared arguments: the shim must pass no database handle, \
     no clock, no I/O and no ambient state",
    "a durable store replays committed statements, so a callback must be a pure function of its \
     arguments unless it is registered with `volatile: true` — then the engine refuses to run it \
     in any WAL-logged statement instead; a script that reads Date.now() or Math.random() without \
     declaring itself volatile makes replay diverge and is the embedder's bug, not the engine's",
    "a throw fails the enclosing statement (`extension function <name>() failed: <message>`); \
     it must not leave host state half-updated if the statement is retried",
    "the registry is the only extension source on wasm: `LOAD EXTENSION '<name>'` activates \
     functions the host already published and can never inject code",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// One function with a single `INTEGER` argument, for the metadata tests.
    fn int_function(name: &str) -> LinkedFunction {
        LinkedFunction::new(
            name,
            vec![ExtType::Integer],
            ExtType::Integer,
            |args: &[ExtArg<'_>]| match args[0] {
                ExtArg::Integer(i) => Ok(ExtValue::Integer(i)),
                _ => Ok(ExtValue::Null),
            },
        )
        .unwrap()
    }

    #[test]
    fn function_metadata_is_validated_once() {
        let err = LinkedFunction::new("", vec![], ExtType::Integer, |_args: &[ExtArg<'_>]| {
            Ok(ExtValue::Integer(0))
        })
        .unwrap_err();
        assert!(err.to_string().contains("name must not be empty"), "{err}");

        let too_many = vec![ExtType::Integer; PINKDB_EXT_MAX_ARGS as usize + 1];
        let err = LinkedFunction::new(
            "wide",
            too_many,
            ExtType::Integer,
            |_args: &[ExtArg<'_>]| Ok(ExtValue::Null),
        )
        .unwrap_err();
        assert!(err.to_string().contains("the maximum is 64"), "{err}");

        let err = LinkedExtension::new("", vec![]).unwrap_err();
        assert!(
            err.to_string().contains("extension name must not be empty"),
            "{err}"
        );

        let dup =
            LinkedExtension::new("dupes", vec![int_function("f"), int_function("F")]).unwrap_err();
        assert!(dup.to_string().contains("\"F\" is declared twice"), "{dup}");

        // An extension with no functions is legal (it pins its name and does
        // nothing else), like the native zero-function case.
        assert!(LinkedExtension::new("empty", vec![]).is_ok());
    }

    #[test]
    fn calls_are_checked_against_the_metadata() {
        let f = int_function("checked");
        assert_eq!(f.call(&[ExtArg::Integer(3)]).unwrap(), ExtValue::Integer(3));
        // Declared arity.
        let err = f.call(&[]).unwrap_err();
        assert!(
            err.to_string().contains("expected 1 argument(s), got 0"),
            "{err}"
        );
        let err = f
            .call(&[ExtArg::Integer(1), ExtArg::Integer(2)])
            .unwrap_err();
        assert!(
            err.to_string().contains("expected 1 argument(s), got 2"),
            "{err}"
        );
        // Declared types (the engine checks at bind time; this is the
        // defense-in-depth check for a direct host call).
        let err = f.call(&[ExtArg::Varchar(b"x")]).unwrap_err();
        assert!(
            err.to_string()
                .contains("argument 1 has type VARCHAR, registered as INTEGER"),
            "{err}"
        );
        // A NULL argument carries the declared type.
        assert_eq!(
            f.call(&[ExtArg::Null(ExtType::Integer)]).unwrap(),
            ExtValue::Null
        );
    }

    #[test]
    fn result_types_are_checked_and_errors_reported() {
        let wrong = LinkedFunction::new(
            "wrong",
            vec![],
            ExtType::Integer,
            |_args: &[ExtArg<'_>]| Ok(ExtValue::Varchar(b"nope".to_vec())),
        )
        .unwrap();
        // L-6: a zero-argument function has no argument type; querying one
        // must return `None`, not underflow `len - 1`.
        assert_eq!(wrong.arg_type_at(0), None);
        assert_eq!(wrong.arg_type_at(usize::MAX), None);
        let err = wrong.call(&[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("returned VARCHAR, registered as INTEGER"),
            "{err}"
        );

        let failing = LinkedFunction::new(
            "failing",
            vec![],
            ExtType::Integer,
            |_args: &[ExtArg<'_>]| Err("no input".to_string()),
        )
        .unwrap();
        let err = failing.call(&[]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "extension function failing() failed: no input"
        );

        let panicking = LinkedFunction::new(
            "panicking",
            vec![],
            ExtType::Integer,
            |_args: &[ExtArg<'_>]| {
                panic!("boom");
            },
        )
        .unwrap();
        let err = panicking.call(&[]).unwrap_err();
        assert!(
            matches!(err, ExtError::Panicked { .. }),
            "a panic must be caught at the boundary: {err:?}"
        );
        assert!(err.to_string().contains("panicked: boom"), "{err}");
    }

    #[test]
    fn ranged_and_variadic_arities_are_checked() {
        // A bounded range (`new_optional`): 1 or 2 declared VARCHARs.
        let optional = LinkedFunction::new_optional(
            "maybe_two",
            vec![ExtType::Varchar, ExtType::Varchar],
            1,
            ExtType::Integer,
            |args: &[ExtArg<'_>]| Ok(ExtValue::Integer(args.len() as i32)),
        )
        .unwrap();
        assert_eq!(optional.min_args(), 1);
        assert_eq!(optional.max_args(), 2);
        assert!(!optional.is_variadic());
        assert_eq!(
            optional.call(&[ExtArg::Varchar(b"a")]).unwrap(),
            ExtValue::Integer(1)
        );
        assert_eq!(
            optional
                .call(&[ExtArg::Varchar(b"a"), ExtArg::Varchar(b"b")])
                .unwrap(),
            ExtValue::Integer(2)
        );
        let err = optional.call(&[]).unwrap_err().to_string();
        assert!(
            err.contains("expected between 1 and 2 argument(s), got 0"),
            "{err}"
        );
        let err = optional
            .call(&[
                ExtArg::Varchar(b"a"),
                ExtArg::Varchar(b"b"),
                ExtArg::Varchar(b"c"),
            ])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("expected at most 2 argument(s), got 3"),
            "{err}"
        );

        // A variadic function (`new_variadic`): every argument past the
        // declared list repeats the last declared type.
        let variadic = LinkedFunction::new_variadic(
            "many",
            vec![ExtType::Integer, ExtType::Varchar],
            0,
            ExtType::Integer,
            |args: &[ExtArg<'_>]| Ok(ExtValue::Integer(args.len() as i32)),
        )
        .unwrap();
        assert_eq!(variadic.min_args(), 0);
        assert_eq!(variadic.max_args(), PINKDB_EXT_MAX_ARGS as usize);
        assert!(variadic.is_variadic());
        assert_eq!(variadic.arg_type_at(0), Some(ExtType::Integer));
        assert_eq!(variadic.arg_type_at(1), Some(ExtType::Varchar));
        assert_eq!(variadic.arg_type_at(9), Some(ExtType::Varchar));
        assert_eq!(
            variadic
                .call(&[
                    ExtArg::Integer(1),
                    ExtArg::Varchar(b"x"),
                    ExtArg::Varchar(b"y"),
                    ExtArg::Varchar(b"z"),
                ])
                .unwrap(),
            ExtValue::Integer(4)
        );
        // A variadic argument of the wrong type is refused…
        let err = variadic
            .call(&[ExtArg::Integer(1), ExtArg::Integer(2)])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("argument 2 has type INTEGER, registered as VARCHAR"),
            "{err}"
        );
        // …and the ABI's arity cap still applies.
        let too_many = vec![ExtArg::Varchar(b"x"); PINKDB_EXT_MAX_ARGS as usize + 1];
        let err = variadic.call(&too_many).unwrap_err().to_string();
        assert!(
            err.contains("expected at most 64 argument(s), got 65"),
            "{err}"
        );

        // Metadata validation: a minimum above the declared list, or a
        // variadic function with no type to repeat, is refused at build time.
        let err = LinkedFunction::new_optional(
            "bad_min",
            vec![ExtType::Varchar],
            2,
            ExtType::Integer,
            |_args: &[ExtArg<'_>]| Ok(ExtValue::Null),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("2 minimum argument(s) but only 1 type(s)"),
            "{err}"
        );
        let err = LinkedFunction::new_variadic(
            "bad_variadic",
            vec![],
            0,
            ExtType::Integer,
            |_args: &[ExtArg<'_>]| Ok(ExtValue::Null),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("declares no argument type to repeat"), "{err}");
    }

    #[test]
    fn closures_can_carry_thread_safe_state() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let counter = LinkedFunction::new(
            "counter",
            vec![],
            ExtType::Integer,
            move |_args: &[ExtArg<'_>]| {
                let n = seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(ExtValue::Integer(n as i32))
            },
        )
        .unwrap();
        assert_eq!(counter.call(&[]).unwrap(), ExtValue::Integer(0));
        assert_eq!(counter.call(&[]).unwrap(), ExtValue::Integer(1));
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    /// Deterministic is the default (matching the native ABI's flag 0) and
    /// `volatile()` is the only way to declare otherwise.
    #[test]
    fn functions_are_deterministic_by_default_and_can_declare_volatility() {
        let det = int_function("lnk_det");
        assert!(!det.is_volatile(), "the default must be deterministic");
        // The builder does not lose the rest of the metadata.
        let vol = int_function("lnk_vol").volatile();
        assert!(vol.is_volatile());
        assert_eq!(vol.n_args(), 1);
        assert_eq!(vol.arg_types(), [ExtType::Integer]);
        assert_eq!(vol.ret_type(), ExtType::Integer);
        assert_eq!(
            vol.call(&[ExtArg::Integer(2)]).unwrap(),
            ExtValue::Integer(2)
        );
    }

    #[test]
    fn registry_is_one_shot_and_case_insensitive() {
        // These names are unique to this test: the registry is process-wide.
        assert!(!is_registered("wave49_unit_registry"));
        register(LinkedExtension::new("wave49_unit_registry", vec![]).unwrap()).unwrap();
        assert!(is_registered("WAVE49_UNIT_REGISTRY"));
        let got = get("Wave49_Unit_Registry").expect("registered");
        assert_eq!(got.name(), "wave49_unit_registry");

        let err =
            register(LinkedExtension::new("WAVE49_Unit_Registry", vec![]).unwrap()).unwrap_err();
        assert!(err.to_string().contains("is already registered"), "{err}");

        // Registration order is preserved (other tests may have registered
        // names too, so assert about this name's position, not the whole
        // list).
        let names = names();
        let i = names
            .iter()
            .position(|n| n == "wave49_unit_registry")
            .expect("registered name listed");
        assert!(names[..i].iter().all(|n| n != "wave49_unit_registry"));

        assert!(get("wave49_unit_absent").is_none());
        assert!(!is_registered("wave49_unit_absent"));
    }

    #[test]
    fn load_reports_an_unregistered_name_cleanly() {
        struct Sink;
        impl LinkedExtHost for Sink {
            fn register_linked_extension(&mut self, _ext: LinkedExtension) -> Result<(), ExtError> {
                Ok(())
            }
        }
        let err = load(&mut Sink, "wave49_unit_absent").unwrap_err();
        assert!(matches!(err, ExtError::NotRegistered { .. }), "{err:?}");
        let message = err.to_string();
        assert!(message.contains("not registered"), "{message}");
        assert!(message.contains("wave49_unit_absent"), "{message}");
    }

    /// The JavaScript contract is pinned: exactly two functions, every row of
    /// the value table documented, every ABI type mapped, and the rules
    /// naming purity and the registry-as-only-source stance.
    #[test]
    fn js_host_surface_is_pinned() {
        let names: Vec<&str> = JS_HOST_SURFACE.iter().map(|f| f.name).collect();
        assert_eq!(
            names,
            vec!["pinkdb.registerExtension", "pinkdb.listExtensions"]
        );
        assert!(JS_HOST_SURFACE.iter().all(|f| !f.docs.is_empty()));

        for t in [
            ExtType::Boolean,
            ExtType::Integer,
            ExtType::Bigint,
            ExtType::Double,
            ExtType::Varchar,
            ExtType::Date,
            ExtType::Timestamp,
        ] {
            assert!(
                JS_TYPE_MAPPINGS.iter().any(|m| m.sql_type == t.name()),
                "{} must be mapped",
                t.name()
            );
        }
        assert!(JS_TYPE_MAPPINGS.iter().any(|m| m.sql_type == "NULL"));
        assert!(
            JS_TYPE_MAPPINGS
                .iter()
                .all(|m| !m.js_value.is_empty() && !m.notes.is_empty())
        );

        let rules = JS_SANDBOX_RULES.join(" ");
        assert!(rules.contains("pure function"), "{rules}");
        assert!(rules.contains("never inject code"), "{rules}");
    }
}
