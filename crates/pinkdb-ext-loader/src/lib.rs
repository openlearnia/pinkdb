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
//! ### Path rules (native targets)
//!
//! Loading a library runs its code, so *which* file is opened matters as
//! much as the ABI handshake. Every native load goes through
//! [`resolve_path`]:
//!
//! - the path must contain a directory: a bare name (`libfoo.so`) would be
//!   resolved through the *dynamic loader's* search path
//!   (`LD_LIBRARY_PATH`, `/usr/lib`, …) — a file the caller never chose;
//! - the path must not contain a `..` component;
//! - the path must resolve to a regular file whose last component is not a
//!   symlink (the canonical path must be the very file that was named);
//! - [`load_in_root`] additionally requires the resolved path to be inside a
//!   configured extension root.
//!
//! The loader also keeps a process-wide registry of the libraries it has
//! loaded ([`loaded_paths`], [`loaded_extension`]): a library stays mapped for
//! the lifetime of the process, and a host can ask whether a path is already
//! loaded *without* touching the filesystem. The engine's WAL replay uses it
//! to answer exactly that question before it may load anything.
//!
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
use std::path::{Component, Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, Mutex, OnceLock};

pub use abi::{
    ExtArg, ExtType, ExtValue, PINKDB_EXT_MAX_ARGS, PINKDB_EXT_TYPE_BIGINT,
    PINKDB_EXT_TYPE_BOOLEAN, PINKDB_EXT_TYPE_DATE, PINKDB_EXT_TYPE_DOUBLE, PINKDB_EXT_TYPE_INTEGER,
    PINKDB_EXT_TYPE_INTERVAL, PINKDB_EXT_TYPE_NULL, PINKDB_EXT_TYPE_TIMESTAMP,
    PINKDB_EXT_TYPE_VARCHAR, pack_interval, unpack_interval,
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
    ///
    /// For a variadic linked function this is the declared prefix: arguments
    /// past it repeat the last type (see [`Self::arg_type_at`]).
    pub fn arg_types(&self) -> &[ExtType] {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.arg_types(),
            ExtFunction::Linked { func, .. } => func.arg_types(),
        }
    }

    /// Smallest accepted argument count (the declared count for every
    /// `dlopen` function and for a fixed linked function).
    pub fn min_args(&self) -> usize {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.n_args(),
            ExtFunction::Linked { func, .. } => func.min_args(),
        }
    }

    /// Largest accepted argument count (`PINKDB_EXT_MAX_ARGS` for a variadic
    /// linked function, the declared count otherwise).
    pub fn max_args(&self) -> usize {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => f.n_args(),
            ExtFunction::Linked { func, .. } => func.max_args(),
        }
    }

    /// Whether arguments past the declared list repeat the last declared type
    /// (`false` for every `dlopen` function).
    pub fn is_variadic(&self) -> bool {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(_) => false,
            ExtFunction::Linked { func, .. } => func.is_variadic(),
        }
    }

    /// The declared type of argument `index` (0-based): the positional type,
    /// or the repeated last type for a variadic argument past the declared
    /// list. `None` for a zero-argument function (there is no declared type
    /// to clamp to) — callers must have checked the arity first.
    pub fn arg_type_at(&self, index: usize) -> Option<ExtType> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            ExtFunction::Dynamic(f) => {
                let n = f.n_args();
                if n == 0 {
                    None
                } else {
                    Some(f.arg_types()[index.min(n - 1)])
                }
            }
            ExtFunction::Linked { func, .. } => func.arg_type_at(index),
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
    /// `args` must match the registered arity — exactly [`Self::n_args`] for
    /// a fixed function, within `min_args..=max_args` for a ranged or
    /// variadic one — with each argument of [`Self::arg_type_at`]'s type; a
    /// violation is reported as [`ExtError::BadResult`] by both models rather
    /// than being undefined.
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
    /// The path is not an acceptable extension path: empty, a bare name
    /// (which the dynamic loader would resolve through its search path), a
    /// `..` component, a symlink, not a regular file, or outside the
    /// configured extension root. See [`resolve_path`].
    InvalidPath { path: String, message: String },
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
            ExtError::InvalidPath { path, message } => {
                write!(f, "extension \"{path}\" is not a loadable path: {message}")
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
/// registered no functions at all. The loader's process-wide registry keeps
/// one more handle, so a loaded library stays mapped for the lifetime of the
/// process even after every store that used it is dropped.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct LoadedExtension {
    name: String,
    path: String,
    /// The canonical absolute path of the library: the resolved directory
    /// plus the file name (the file itself is not a symlink — see
    /// [`resolve_path`]). This is the identity the loader's registry and the
    /// engine's WAL replay compare.
    canonical_path: PathBuf,
    /// L-8: the heap-allocated init collector, retained for the library's
    /// lifetime. It owns the registered functions *and* keeps the host
    /// context pointer valid, so a late `register_scalar` reaches the
    /// `init_complete` flag instead of a dead stack frame.
    collector: Arc<host::Collector>,
}

#[cfg(not(target_arch = "wasm32"))]
impl LoadedExtension {
    /// The name `pinkdb_extension_name` returned.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The canonical absolute path of the library this extension was loaded
    /// from.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The canonical absolute path of the library this extension was loaded
    /// from, as a [`PathBuf`] (the identity the registry compares).
    pub fn canonical_path(&self) -> PathBuf {
        self.canonical_path.clone()
    }

    /// Registered functions.
    pub fn functions(&self) -> &[DynamicFunction] {
        &self.collector.functions
    }

    /// Consume the handle, yielding the functions (and with them the shared
    /// library reference: the code stays mapped while any survives).
    pub fn into_functions(self) -> Vec<DynamicFunction> {
        // The registry may hold a second handle for the same library; then
        // the collector stays shared and its functions are cloned.
        match Arc::try_unwrap(self.collector) {
            Ok(collector) => collector.functions,
            Err(shared) => shared.functions.clone(),
        }
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

// ---------- path resolution (the loader's path rules) ----------

/// Resolve `path` to the canonical absolute path of a loadable extension
/// file, applying the loader's path rules. Every native load goes through
/// it, so the rules hold for the SQL statement, the Rust API and the WAL
/// replay alike:
///
/// - the path must not be empty and must contain a path separator: a bare
///   name (`libfoo.so`) would be resolved through the *dynamic loader's*
///   search path (`LD_LIBRARY_PATH`, `/usr/lib`, …) — a file the caller
///   never chose;
/// - the path must not contain a `..` component: it is an escape vector,
///   and `canonicalize` would resolve it silently;
/// - the path must resolve to an existing **regular file**;
/// - the last component must not be a **symlink**: the canonical path
///   (which follows symlinks) must be the very same path as the resolved
///   directory joined with the file name. A symlinked library is a
///   redirection the caller cannot see.
///
/// The returned path is absolute and canonical: it is what the loader
/// `dlopen`s, what [`LoadedExtension::path`] reports and what the engine
/// records in the WAL.
#[cfg(not(target_arch = "wasm32"))]
pub fn resolve_path(path: &str) -> Result<PathBuf, ExtError> {
    let key = path_key_checked(path)?;
    check_regular_file(path, &key)
}

/// [`resolve_path`] plus root confinement: the resolved path must be inside
/// `root` (the root itself is resolved the same way, so a symlinked root
/// directory is fine — only the *library* must not be a symlink).
///
/// This is the entry point a host uses when it configures an extension
/// directory: a path that escapes the root (through `..`, through a
/// symlinked directory, or by naming a file outside it) is refused. The root
/// is checked *first*: a path outside it is refused whatever it names, so
/// the error never depends on whether the file exists.
#[cfg(not(target_arch = "wasm32"))]
pub fn resolve_path_in_root(root: &Path, path: &str) -> Result<PathBuf, ExtError> {
    let key = path_key_checked(path)?;
    let root_canonical = std::fs::canonicalize(root).map_err(|e| {
        invalid_path(
            path,
            &format!(
                "the extension root {} cannot be resolved: {e}",
                root.display()
            ),
        )
    })?;
    if !root_canonical.is_dir() {
        return Err(invalid_path(
            path,
            &format!(
                "the extension root {} is not a directory",
                root_canonical.display()
            ),
        ));
    }
    if !key.starts_with(&root_canonical) {
        return Err(invalid_path(
            path,
            &format!(
                "the path is outside the extension root {}",
                root_canonical.display()
            ),
        ));
    }
    check_regular_file(path, &key)
}

/// The loader's path rules, up to the file's own checks: the path must not
/// be empty, must name a directory (a bare name would be resolved through
/// the dynamic loader's search path), must not contain a `..` component, and
/// must have a resolvable parent directory.
///
/// Returns the *key* of the path: its parent directory resolved, plus the
/// file name (which is left unresolved — the file itself is never a symlink).
#[cfg(not(target_arch = "wasm32"))]
fn path_key_checked(path: &str) -> Result<PathBuf, ExtError> {
    if path.is_empty() {
        return Err(ExtError::Load {
            path: path.to_string(),
            message: "empty path".to_string(),
        });
    }
    let raw = Path::new(path);
    if !path.contains('/') && !path.contains(std::path::MAIN_SEPARATOR) {
        return Err(invalid_path(
            path,
            "a bare name would be resolved through the dynamic loader's search path; \
             give the path of the library (with a directory)",
        ));
    }
    if raw.components().any(|c| c == Component::ParentDir) {
        return Err(invalid_path(path, "the path contains a `..` component"));
    }
    path_key(raw).ok_or_else(|| invalid_path(path, "the path has no file name"))
}

/// The file's own rules: it must exist, be a regular file, and not be a
/// symlink (the canonical path — which follows symlinks — must be the very
/// file that was named).
#[cfg(not(target_arch = "wasm32"))]
fn check_regular_file(path: &str, key: &Path) -> Result<PathBuf, ExtError> {
    // `symlink_metadata` does not follow the final symlink.
    let meta = std::fs::symlink_metadata(key).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            // A missing file is a load failure, not a malformed path.
            ExtError::Load {
                path: path.to_string(),
                message: e.to_string(),
            }
        } else {
            invalid_path(path, &format!("cannot stat the path: {e}"))
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(invalid_path(
            path,
            "the path is a symlink; load the real file instead",
        ));
    }
    if !meta.is_file() {
        return Err(invalid_path(path, "the path is not a regular file"));
    }
    let canonical = std::fs::canonicalize(key)
        .map_err(|e| invalid_path(path, &format!("cannot resolve the path: {e}")))?;
    if canonical != key {
        return Err(invalid_path(
            path,
            "the path resolves to a different file than the one named",
        ));
    }
    Ok(canonical)
}

/// The canonical key of a path: its parent directory resolved (symlinks in
/// the directory are followed — a *directory* symlink is a normal way to
/// publish an extension tree) plus the file name, which is left unresolved.
/// The parent is left unresolved when it cannot be resolved (it does not
/// exist), so the caller reports the accurate "no such file" error.
#[cfg(not(target_arch = "wasm32"))]
fn path_key(path: &Path) -> Option<PathBuf> {
    let file_name = path.file_name()?;
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let parent = match std::fs::canonicalize(parent) {
        Ok(p) => p,
        Err(_) => parent.to_path_buf(),
    };
    Some(parent.join(file_name))
}

/// An [`ExtError::InvalidPath`] for `path`.
#[cfg(not(target_arch = "wasm32"))]
fn invalid_path(path: &str, message: &str) -> ExtError {
    ExtError::InvalidPath {
        path: path.to_string(),
        message: message.to_string(),
    }
}

// ---------- the process-wide registry of loaded libraries ----------

/// Every extension library this process has loaded, with its canonical
/// path. The registry keeps the library mapped for the lifetime of the
/// process (the [`Arc`] inside each [`LoadedExtension`]), so a host can ask
/// "is this library already loaded?" without touching the filesystem — the
/// question WAL replay has to answer before it may load anything.
#[cfg(not(target_arch = "wasm32"))]
static LOADED: OnceLock<Mutex<Vec<LoadedExtension>>> = OnceLock::new();

#[cfg(not(target_arch = "wasm32"))]
fn loaded_registry() -> &'static Mutex<Vec<LoadedExtension>> {
    LOADED.get_or_init(|| Mutex::new(Vec::new()))
}

/// The canonical paths of every extension library this process has loaded
/// through [`load`] / [`load_in_root`], in load order.
#[cfg(not(target_arch = "wasm32"))]
pub fn loaded_paths() -> Vec<PathBuf> {
    let guard = loaded_registry().lock().unwrap_or_else(|p| p.into_inner());
    guard.iter().map(|e| e.canonical_path()).collect()
}

/// The canonical key of `path`: its parent directory resolved, plus the file
/// name (the file itself is not resolved — a symlinked library is refused
/// elsewhere). `None` when the path names no file.
///
/// This is the identity the loader's registry compares: two spellings of the
/// same file (`/tmp/x/../lib.dylib`, `/tmp/./lib.dylib`) have the same key.
#[cfg(not(target_arch = "wasm32"))]
pub fn canonical_key(path: &str) -> Option<PathBuf> {
    path_key(Path::new(path))
}

/// The already-loaded extension whose canonical path is `path`'s, if this
/// process has loaded that very file.
///
/// This is the lookup WAL replay uses before it may load anything: a
/// recorded path that names a library this process already has mapped is
/// *not* loaded again — the mapped image (and its functions) is reused, so
/// no constructor and no `init` runs again. The comparison is on canonical
/// paths, not on the spelling of the path: `/tmp/x/../lib.dylib` and
/// `/tmp/lib.dylib` are the same file, `/tmp/other/lib.dylib` is not.
#[cfg(not(target_arch = "wasm32"))]
pub fn loaded_extension(path: &str) -> Option<LoadedExtension> {
    let key = path_key(Path::new(path))?;
    let guard = loaded_registry().lock().unwrap_or_else(|p| p.into_inner());
    guard.iter().find(|e| e.canonical_path() == key).cloned()
}

/// Load `path` and register its functions on `db`; returns the extension's
/// declared name.
///
/// Fails — without registering anything — when the library does not open, is
/// missing an ABI symbol, reports a different ABI version, has an invalid
/// name, fails `pinkdb_extension_init`, or when `db` rejects a registration.
///
/// The path goes through [`resolve_path`] first: a bare name, a `..`
/// component, a symlink or anything that is not a regular file is refused
/// before the library is opened. Use [`load_in_root`] to confine the load to
/// a directory.
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
///
/// The path is resolved first ([`resolve_path`]): only an absolute, canonical
/// path to a regular file that is not a symlink is ever opened.
#[cfg(not(target_arch = "wasm32"))]
pub fn load(path: &str) -> Result<LoadedExtension, ExtError> {
    let canonical = resolve_path(path)?;
    load_resolved(&canonical, path)
}

/// [`load`], confined to `root`: the resolved path must be inside `root`
/// (see [`resolve_path_in_root`]). This is the entry point for a host that
/// configures an extension directory — the server's `--extension-root`, the
/// CLI's `--extension-root`, the engine's `extension_root` open option.
#[cfg(not(target_arch = "wasm32"))]
pub fn load_in_root(root: &Path, path: &str) -> Result<LoadedExtension, ExtError> {
    let canonical = resolve_path_in_root(root, path)?;
    load_resolved(&canonical, path)
}

/// Open the already-resolved `canonical` path and run the ABI v1 handshake.
/// `display` is the path the caller wrote (used in the error messages).
#[cfg(not(target_arch = "wasm32"))]
fn load_resolved(canonical: &Path, display: &str) -> Result<LoadedExtension, ExtError> {
    let path = display.to_string();
    // SAFETY: dlopen runs the library's initializers and maps its code; that
    // is exactly what loading an extension means, and the caller asked for
    // this path. The handle is kept alive by the returned functions.
    let library = unsafe { libloading::Library::new(canonical) }.map_err(|e| ExtError::Load {
        path: path.clone(),
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

    // L-8: heap-allocate the collector and retain it with the handle (and the
    // registry). Its address is stable for the library's lifetime, so the
    // `host_ctx` an extension may have kept is never a dead stack frame; the
    // flag makes a post-init registration a clean error.
    let mut collector = Arc::new(host::Collector {
        library: Arc::clone(&library),
        // The canonical path, like the handle's: the engine records it in
        // the WAL (`LOAD EXTENSION '<path>'`), and a canonical path resolves
        // the same way in every process that replays it.
        path: canonical.to_string_lossy().into_owned(),
        init_complete: std::sync::atomic::AtomicBool::new(false),
        functions: Vec::new(),
        error: None,
    });
    let api = host::host_api();
    // SAFETY: `api` is a fully initialized host-owned struct, valid for the
    // duration of the call; `Arc::as_ptr(&collector)` is a live, stable
    // `Collector` address passed as the opaque ctx the ABI routes back to our
    // `register_scalar`. The extension must not store `api` beyond the call
    // (documented) and must not unwind.
    let code = unsafe { init_fn(&api, Arc::as_ptr(&collector).cast_mut().cast::<c_void>()) };
    // Close registration before evaluating the outcome: init is over.
    collector
        .init_complete
        .store(true, std::sync::atomic::Ordering::Release);
    if let Some(first_error) = Arc::get_mut(&mut collector).and_then(|c| c.error.take()) {
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
        // The canonical path, not the spelling the caller used: it is what
        // the engine records in the WAL, and a canonical path resolves the
        // same way in every process that replays it.
        path: canonical.to_string_lossy().into_owned(),
        canonical_path: canonical.to_path_buf(),
        collector,
    })
    .inspect(|ext| {
        // The process-wide registry: the library stays mapped for the
        // lifetime of the process, and a later WAL replay can find this very
        // file by its canonical path (and reuse it instead of loading it
        // again).
        let mut guard = loaded_registry().lock().unwrap_or_else(|p| p.into_inner());
        guard.push(ext.clone());
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
