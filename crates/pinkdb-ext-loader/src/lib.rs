//! pinkdb extension loading — two models, one value ABI.
//!
//! **Native targets** load a shared library (`.dylib` / `.so` / `.dll`) that
//! exports the versioned C ABI documented in
//! [`include/pinkdb_extension.h`], collect the scalar functions it registers,
//! and hand them to a host through the [`ExtHost`] trait. [`load_extension`]
//! is the complete load-and-register entry point; the engine's
//! `PinkDb::load_extension` is a thin wrapper over it.
//!
//! ```no_run
//! # fn main() -> Result<(), pinkdb_ext_loader::ExtError> {
//! # let mut host = ();
//! # struct H; impl pinkdb_ext_loader::ExtHost for H {
//! #   fn register_extension(&mut self, _e: pinkdb_ext_loader::LoadedExtension) -> Result<(), pinkdb_ext_loader::ExtError> { Ok(()) }
//! # }
//! # let mut host = H;
//! let name = pinkdb_ext_loader::load_extension(&mut host, "path/to/ext.dylib")?;
//! # let _ = name;
//! # Ok(())
//! # }
//! ```
//!
//! **wasm targets** have no dynamic linker and no filesystem, so a wasm build
//! links no `dlopen` at all: extensions are linked into the host and
//! published to a process-global registry through the safe Rust API of
//! [`linked`] (the *linked* model — see its module docs for the full
//! comparison, the loading rules and the JavaScript host-surface contract).
//! `LOAD EXTENSION '<name>'` there resolves the name against that registry.
//! The linked model is available on native targets too, where it is the
//! host-function registration path for embedders (and the fallback that lets
//! a WAL record naming a linked extension replay).
//!
//! Both models share the value model ([`ExtType`], [`ExtArg`], [`ExtValue`]
//! and the arity limit) and the engine integration: the binder resolves
//! unknown function names case-insensitively, the executor marshals one row
//! at a time into ABI values, and [`ExtFunction`] / [`ExtHandle`] are what
//! the engine's registry stores whichever model produced them. Both models
//! also share the volatility rule: a function is deterministic unless it was
//! declared volatile (the native ABI's `PINKDB_EXT_FLAG_VOLATILE` flag, or
//! [`LinkedFunction::volatile`]), and the engine refuses to run a volatile
//! call in any statement it WAL-logs — replaying logged SQL must never
//! diverge.
//!
//! The ABI contract (symbol names, value layout, text ownership, and the
//! thread-safety requirement on callbacks) lives in the header; this crate is
//! its single host-side implementation. All `unsafe` in the workspace's
//! runtime crates (outside `pinkdb-ffi`) is contained here and is native-only:
//! `dlopen`/`dlsym` (via `libloading`), invoking extension callbacks, and
//! reading/freeing result text. The one other crate with `unsafe` is the Rust
//! extension SDK (`ext-sdk/rust`), which implements the same ABI from the
//! *extension* side and is never a dependency of any pinkdb binary.
//!
//! [`include/pinkdb_extension.h`]: https://github.com/openlearnia/pinkdb/blob/main/crates/pinkdb-ext-loader/include/pinkdb_extension.h

// Raw-pointer arguments are NULL-checked before every dereference and the
// pointer-taking signatures are dictated by the C ABI, so the lint that wants
// `unsafe fn` here does not apply (same rationale as `pinkdb-ffi`).
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod abi;
#[cfg(not(target_arch = "wasm32"))]
mod function;
#[cfg(not(target_arch = "wasm32"))]
mod host;
pub mod linked;

#[cfg(not(target_arch = "wasm32"))]
use std::ffi::{CStr, c_void};
use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

pub use abi::{
    ExtArg, ExtType, ExtValue, PINKDB_EXT_MAX_ARGS, PINKDB_EXT_TYPE_BIGINT,
    PINKDB_EXT_TYPE_BOOLEAN, PINKDB_EXT_TYPE_DATE, PINKDB_EXT_TYPE_DOUBLE, PINKDB_EXT_TYPE_INTEGER,
    PINKDB_EXT_TYPE_NULL, PINKDB_EXT_TYPE_TIMESTAMP, PINKDB_EXT_TYPE_VARCHAR,
};
#[cfg(not(target_arch = "wasm32"))]
pub use abi::{
    PINKDB_EXT_ABI_VERSION, PINKDB_EXT_FLAG_KNOWN, PINKDB_EXT_FLAG_VOLATILE, PinkdbExtAbiVersionFn,
    PinkdbExtInitFn, PinkdbExtNameFn, PinkdbExtScalarFn, PinkdbExtValue, PinkdbHostApi,
};
#[cfg(not(target_arch = "wasm32"))]
pub use function::DynamicFunction;
#[cfg(not(target_arch = "wasm32"))]
pub use host::{TextAllocStats, text_allocation_stats};
pub use linked::{LinkedExtension, LinkedFunction, LinkedScalar};

/// One callable a host can register, whichever loading model produced it.
///
/// The engine's registry stores these (keyed by lowercased name); the binder
/// and executor reach callbacks only through this type, so a `dlopen`
/// function and a linked one behave identically from SQL.
#[derive(Clone, Debug)]
pub enum ExtFunction {
    /// A native `dlopen` extension's callback (ABI v1).
    #[cfg(not(target_arch = "wasm32"))]
    Dynamic(DynamicFunction),
    /// The linked model: a Rust callback published by an embedder. `source`
    /// is the publishing extension's registry name — the string
    /// `LOAD EXTENSION` takes on wasm (and the WAL records as the function's
    /// origin).
    Linked {
        source: String,
        func: LinkedFunction,
    },
}

impl ExtFunction {
    /// Registered name (original case).
    pub fn name(&self) -> &str {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.name(),
            ExtFunction::Linked { func, .. } => func.name(),
        }
    }

    /// Where this function came from: the path it was loaded from
    /// ([`ExtFunction::Dynamic`]) or the registry name its extension was
    /// published under ([`ExtFunction::Linked`]). The engine prefixes a WAL
    /// record that calls the function with `LOAD EXTENSION '<source>'`.
    pub fn source(&self) -> &str {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.source_path(),
            ExtFunction::Linked { source, .. } => source,
        }
    }

    /// Declared argument count.
    pub fn n_args(&self) -> usize {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.n_args(),
            ExtFunction::Linked { func, .. } => func.n_args(),
        }
    }

    /// Declared argument types, in order.
    pub fn arg_types(&self) -> &[ExtType] {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.arg_types(),
            ExtFunction::Linked { func, .. } => func.arg_types(),
        }
    }

    /// Declared return type.
    pub fn ret_type(&self) -> ExtType {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.ret_type(),
            ExtFunction::Linked { func, .. } => func.ret_type(),
        }
    }

    /// Whether the function was declared volatile (nondeterministic) — the
    /// native ABI's `PINKDB_EXT_FLAG_VOLATILE` or the linked model's
    /// [`LinkedFunction::volatile`]. `false` (deterministic) is the default
    /// in both models, so every pre-flag extension and every existing
    /// registration keeps today's semantics.
    ///
    /// The engine refuses to run a volatile call in any statement it
    /// WAL-logs: the logged SQL text is re-executed on recovery, and a
    /// nondeterministic callback could produce a different value there.
    /// Read-only statements never reach the WAL and may call volatile
    /// functions.
    pub fn is_volatile(&self) -> bool {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.is_volatile(),
            ExtFunction::Linked { func, .. } => func.is_volatile(),
        }
    }

    /// Invoke the callback once (one row) and return its result.
    ///
    /// `args` must have exactly [`Self::n_args`] entries of the declared
    /// types; a violation is reported as [`ExtError::BadResult`] by both
    /// models rather than being undefined.
    pub fn call(&self, args: &[ExtArg<'_>]) -> Result<ExtValue, ExtError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.call(args),
            ExtFunction::Linked { func, .. } => func.call(args),
        }
    }
}

/// One extension's handle: keeps it alive and named (and, natively, its
/// library mapped), whichever loading model produced it.
#[derive(Clone, Debug)]
pub enum ExtHandle {
    /// A `dlopen`ed library, held through [`LoadedExtension`].
    #[cfg(not(target_arch = "wasm32"))]
    Dynamic(LoadedExtension),
    /// A linked extension published by the host.
    Linked(LinkedExtension),
}

impl ExtHandle {
    /// The declared extension name.
    pub fn name(&self) -> &str {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtHandle::Dynamic(e) => e.name(),
            ExtHandle::Linked(e) => e.name(),
        }
    }

    /// The extension's source: the path it was loaded from
    /// ([`ExtHandle::Dynamic`]) or its registry name
    /// ([`ExtHandle::Linked`]) — the string `LOAD EXTENSION` takes on that
    /// target.
    pub fn source(&self) -> &str {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtHandle::Dynamic(e) => e.path(),
            ExtHandle::Linked(e) => e.name(),
        }
    }

    /// The extension's functions, as engine-registry entries.
    pub fn functions(&self) -> Vec<ExtFunction> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtHandle::Dynamic(e) => e
                .functions()
                .iter()
                .map(|f| ExtFunction::Dynamic(f.clone()))
                .collect(),
            ExtHandle::Linked(e) => e
                .functions()
                .iter()
                .map(|f| ExtFunction::Linked {
                    source: e.name().to_string(),
                    func: f.clone(),
                })
                .collect(),
        }
    }
}

/// Everything that can go wrong loading or calling an extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtError {
    /// The shared library could not be opened (`dlopen` failed).
    Load { path: String, message: String },
    /// The library opened but does not export a required ABI symbol.
    MissingSymbol { path: String, symbol: String },
    /// The extension was built against a different ABI version.
    AbiVersion {
        path: String,
        extension: u32,
        host: u32,
    },
    /// `pinkdb_extension_name` was NULL, empty, or not UTF-8.
    InvalidName { path: String, message: String },
    /// `pinkdb_extension_init` returned a non-zero code.
    InitFailed { path: String, code: i32 },
    /// A registration was rejected by the loader or by the host. Nothing from
    /// the extension was registered (registration is atomic).
    Registration { message: String },
    /// A callback returned a non-zero error code.
    Callback { function: String, code: i32 },
    /// A callback broke the ABI contract (wrong return type, bad pointer...).
    BadResult { function: String, message: String },
    /// A linked extension name is not in the process-global registry
    /// ([`linked::load`]; on wasm that is what `LOAD EXTENSION` resolves
    /// against).
    NotRegistered { name: String },
    /// A linked function's implementation returned an error (a Rust `Err`; a
    /// wasm-bindgen shim reports a JavaScript exception this way, like the
    /// C ABI's non-zero callback code).
    Failed { function: String, message: String },
    /// A linked function panicked. Caught at the boundary — the linked model
    /// has no "must not unwind" rule like the C ABI, so a panic is a normal
    /// statement error there.
    Panicked { function: String, message: String },
}

impl ExtError {
    /// Build a [`ExtError::Registration`] (used by [`ExtHost`] implementations).
    pub fn registration(message: impl Into<String>) -> Self {
        ExtError::Registration {
            message: message.into(),
        }
    }
}

impl fmt::Display for ExtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtError::Load { path, message } => {
                write!(f, "cannot load extension \"{path}\": {message}")
            }
            ExtError::MissingSymbol { path, symbol } => write!(
                f,
                "extension \"{path}\" does not export \"{symbol}\" (not a pinkdb extension, or built against another ABI)"
            ),
            ExtError::AbiVersion {
                path,
                extension,
                host,
            } => write!(
                f,
                "extension \"{path}\" uses ABI version {extension}, host supports version {host}"
            ),
            ExtError::InvalidName { path, message } => {
                write!(f, "extension \"{path}\" has an invalid name: {message}")
            }
            ExtError::InitFailed { path, code } => write!(
                f,
                "extension \"{path}\" failed to initialize (pinkdb_extension_init returned {code})"
            ),
            ExtError::Registration { message } => {
                write!(f, "extension registration failed: {message}")
            }
            ExtError::Callback { function, code } => {
                write!(f, "extension function {function}() failed with code {code}")
            }
            ExtError::BadResult { function, message } => write!(
                f,
                "extension function {function}() returned an invalid result: {message}"
            ),
            ExtError::NotRegistered { name } => write!(
                f,
                "extension \"{name}\" is not registered (register it at host bootstrap with \
                 pinkdb_ext_loader::linked::register; wasm targets have no dynamic loader)"
            ),
            ExtError::Failed { function, message } => {
                write!(f, "extension function {function}() failed: {message}")
            }
            ExtError::Panicked { function, message } => {
                write!(f, "extension function {function}() panicked: {message}")
            }
        }
    }
}

impl std::error::Error for ExtError {}

/// A loaded extension: its declared name, the path it was loaded from, and
/// every scalar function it registered, in registration order.
///
/// The handle owns an [`Arc`] of the shared library, so the code stays mapped
/// as long as the host keeps the `LoadedExtension` — even when the extension
/// registered no functions at all.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct LoadedExtension {
    name: String,
    path: String,
    functions: Vec<DynamicFunction>,
    /// Keeps the library mapped independently of the functions (the
    /// zero-function case: without this, dropping the handle would unload the
    /// code immediately after a successful load). Never read: holding the
    /// `Arc` is the whole point.
    _library: Arc<libloading::Library>,
}

#[cfg(not(target_arch = "wasm32"))]
impl LoadedExtension {
    /// The name `pinkdb_extension_name` returned.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The path passed to [`load`] / [`load_extension`].
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Registered functions.
    pub fn functions(&self) -> &[DynamicFunction] {
        &self.functions
    }

    /// Consume the handle, yielding the functions (and with them the shared
    /// library reference: the code stays mapped while any survives).
    pub fn into_functions(self) -> Vec<DynamicFunction> {
        self.functions
    }
}

/// A host that can accept a loaded extension's functions.
///
/// Implementations must be atomic: on error nothing may stay registered.
#[cfg(not(target_arch = "wasm32"))]
pub trait ExtHost {
    /// Register every function of `ext`; `Err` leaves the host unchanged.
    fn register_extension(&mut self, ext: LoadedExtension) -> Result<(), ExtError>;
}

/// Load `path` and register its functions on `db`; returns the extension's
/// declared name.
///
/// Fails — without registering anything — when the library does not open, is
/// missing an ABI symbol, reports a different ABI version, has an invalid
/// name, fails `pinkdb_extension_init`, or when `db` rejects a registration.
#[cfg(not(target_arch = "wasm32"))]
pub fn load_extension<H: ExtHost + ?Sized>(db: &mut H, path: &str) -> Result<String, ExtError> {
    let ext = load(path)?;
    let name = ext.name().to_string();
    db.register_extension(ext)?;
    Ok(name)
}

/// Open `path` and run its ABI v1 handshake, without registering anything.
///
/// Symbol checks happen in a fixed order: wrong ABI version is reported before
/// a missing init symbol, so a foreign library built for a future ABI gets the
/// version error rather than a confusing symbol error.
#[cfg(not(target_arch = "wasm32"))]
pub fn load(path: &str) -> Result<LoadedExtension, ExtError> {
    if path.is_empty() {
        return Err(ExtError::Load {
            path: path.to_string(),
            message: "empty path".to_string(),
        });
    }
    // SAFETY: dlopen runs the library's initializers and maps its code; that
    // is exactly what loading an extension means, and the caller asked for
    // this path. The handle is kept alive by the returned functions.
    let library = unsafe { libloading::Library::new(path) }.map_err(|e| ExtError::Load {
        path: path.to_string(),
        message: e.to_string(),
    })?;
    let library = Arc::new(library);

    // SAFETY (all symbol lookups below): `get` is unsafe because a symbol of
    // the same name with a different type is undefined behavior. Each symbol
    // is looked up under the exact name and signature this ABI defines; a
    // mismatched extension is outside the contract (the version check below
    // is the guard that the library speaks this ABI).
    let abi_fn = unsafe { library.get::<PinkdbExtAbiVersionFn>(b"pinkdb_extension_abi_version\0") }
        .map_err(|_| ExtError::MissingSymbol {
            path: path.to_string(),
            symbol: "pinkdb_extension_abi_version".to_string(),
        })?;
    // SAFETY: called per the ABI: no arguments, no unwinding.
    let extension_version = unsafe { abi_fn() };
    if extension_version != PINKDB_EXT_ABI_VERSION {
        return Err(ExtError::AbiVersion {
            path: path.to_string(),
            extension: extension_version,
            host: PINKDB_EXT_ABI_VERSION,
        });
    }

    let name_fn =
        unsafe { library.get::<PinkdbExtNameFn>(b"pinkdb_extension_name\0") }.map_err(|_| {
            ExtError::MissingSymbol {
                path: path.to_string(),
                symbol: "pinkdb_extension_name".to_string(),
            }
        })?;
    // SAFETY: called per the ABI; returns a NUL-terminated UTF-8 string with
    // static lifetime. Copied immediately below.
    let name_ptr = unsafe { name_fn() };
    if name_ptr.is_null() {
        return Err(ExtError::InvalidName {
            path: path.to_string(),
            message: "pinkdb_extension_name returned NULL".to_string(),
        });
    }
    // SAFETY: non-NULL NUL-terminated pointer per the ABI.
    let name = unsafe { CStr::from_ptr(name_ptr) }
        .to_str()
        .map_err(|_| ExtError::InvalidName {
            path: path.to_string(),
            message: "name is not valid UTF-8".to_string(),
        })?
        .to_string();
    if name.is_empty() {
        return Err(ExtError::InvalidName {
            path: path.to_string(),
            message: "name is empty".to_string(),
        });
    }

    let init_fn =
        unsafe { library.get::<PinkdbExtInitFn>(b"pinkdb_extension_init\0") }.map_err(|_| {
            ExtError::MissingSymbol {
                path: path.to_string(),
                symbol: "pinkdb_extension_init".to_string(),
            }
        })?;

    let mut collector = host::Collector {
        library: Arc::clone(&library),
        path: path.to_string(),
        functions: Vec::new(),
        error: None,
    };
    let api = host::host_api();
    // SAFETY: `api` is a fully initialized host-owned struct, valid for the
    // duration of the call; `&mut collector` is passed as the opaque ctx the
    // ABI routes back to our `register_scalar`. The extension must not store
    // `api` beyond the call (documented) and must not unwind.
    let code = unsafe { init_fn(&api, std::ptr::from_mut(&mut collector).cast::<c_void>()) };
    if let Some(first_error) = collector.error.take() {
        // A rejected registration fails the load even if init returned 0.
        return Err(ExtError::Registration {
            message: first_error,
        });
    }
    if code != 0 {
        return Err(ExtError::InitFailed {
            path: path.to_string(),
            code,
        });
    }

    Ok(LoadedExtension {
        name,
        path: path.to_string(),
        functions: collector.functions,
        _library: library,
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// A tiny host sink used for the loader-level tests (no engine needed).
    #[derive(Default)]
    struct Sink {
        extensions: Vec<String>,
        functions: Vec<String>,
        fail: Option<String>,
    }

    impl ExtHost for Sink {
        fn register_extension(&mut self, ext: LoadedExtension) -> Result<(), ExtError> {
            if let Some(message) = &self.fail {
                return Err(ExtError::registration(message.clone()));
            }
            self.extensions.push(ext.name().to_string());
            for f in ext.functions() {
                self.functions.push(f.name().to_string());
            }
            Ok(())
        }
    }

    #[test]
    fn missing_file_is_a_clear_error() {
        let err = load("/nonexistent/pinkdb/does_not_exist.dylib").unwrap_err();
        assert!(matches!(err, ExtError::Load { .. }), "{err:?}");
        assert!(err.to_string().contains("cannot load extension"), "{err}");
    }

    #[test]
    fn empty_path_is_a_clear_error() {
        let err = load("").unwrap_err();
        assert!(matches!(err, ExtError::Load { .. }), "{err:?}");
    }

    #[test]
    fn host_rejection_propagates_and_registers_nothing() {
        let mut host = Sink {
            fail: Some("nope".to_string()),
            ..Sink::default()
        };
        let err = load_extension(&mut host, "/nonexistent").unwrap_err();
        // The load fails first (no such library) — the host is only reached on
        // a successful load, which the integration tests cover end to end.
        assert!(matches!(err, ExtError::Load { .. }), "{err:?}");
        assert!(host.extensions.is_empty() && host.functions.is_empty());
    }
}
