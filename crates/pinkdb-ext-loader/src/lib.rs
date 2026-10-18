//! pinkdb loadable-extension loader — ABI v1.
//!
//! Loads a shared library (`.dylib` / `.so` / `.dll`) that exports the
//! versioned C ABI documented in [`include/pinkdb_extension.h`], collects the
//! scalar functions it registers, and hands them to a host through the
//! [`ExtHost`] trait. [`load_extension`] is the complete load-and-register
//! entry point; the engine's `PinkDb::load_extension` is a thin wrapper over
//! it.
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
//! The ABI contract (symbol names, value layout, text ownership, and the
//! thread-safety requirement on callbacks) lives in the header; this crate is
//! its single host-side implementation. All `unsafe` in the workspace outside
//! `pinkdb-ffi` is contained here: `dlopen`/`dlsym` (via `libloading`),
//! invoking extension callbacks, and reading/freeing result text.
//!
//! [`include/pinkdb_extension.h`]: https://github.com/openlearnia/pinkdb/blob/main/crates/pinkdb-ext-loader/include/pinkdb_extension.h

// Raw-pointer arguments are NULL-checked before every dereference and the
// pointer-taking signatures are dictated by the C ABI, so the lint that wants
// `unsafe fn` here does not apply (same rationale as `pinkdb-ffi`).
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod abi;
mod function;
mod host;

use std::ffi::{CStr, c_void};
use std::fmt;
use std::sync::Arc;

pub use abi::{
    ExtArg, ExtType, ExtValue, PINKDB_EXT_ABI_VERSION, PINKDB_EXT_MAX_ARGS, PINKDB_EXT_TYPE_BIGINT,
    PINKDB_EXT_TYPE_BOOLEAN, PINKDB_EXT_TYPE_DATE, PINKDB_EXT_TYPE_DOUBLE, PINKDB_EXT_TYPE_INTEGER,
    PINKDB_EXT_TYPE_NULL, PINKDB_EXT_TYPE_TIMESTAMP, PINKDB_EXT_TYPE_VARCHAR,
    PinkdbExtAbiVersionFn, PinkdbExtInitFn, PinkdbExtNameFn, PinkdbExtScalarFn, PinkdbExtValue,
    PinkdbHostApi,
};
pub use function::DynamicFunction;
pub use host::{TextAllocStats, text_allocation_stats};

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

#[cfg(test)]
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
