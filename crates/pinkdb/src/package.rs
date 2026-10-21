//! **Extension packages** (wave 86): one host-published registry entry that
//! carries a whole extension surface — optional linked scalar functions plus
//! [`TableSource`]s (table functions and replacement scans) and
//! [`FileFormat`] COPY sinks.
//!
//! The loader's two existing models ([`crate::ext`]) carry scalar functions
//! only: a dlopen'd library registers ABI v1 callbacks, and the linked
//! registry in `pinkdb_ext_loader::linked` carries safe-Rust callbacks. A
//! table source needs a schema at bind time, a projection and pushdown
//! hints, and whole chunks of output; a sink consumes one materialized
//! table. Neither shape crosses the C ABI, so a *package* is the host-level
//! bundling point: plain Rust values the embedder compiled in, registered
//! here and activated per store by `LOAD EXTENSION '<name>'`.
//!
//! ```text
//! register_extension_package(ExtensionPackage::new("lake")
//!     .with_scalars(linked_extension)          // optional, linked model
//!     .with_table_source(Arc::new(ReadLake))   // table functions + scans
//!     .with_file_format(Arc::new(LakeSink)));  // COPY … (FORMAT lake)
//! ```
//!
//! ## Registration and activation
//!
//! Registration is **process-global** and by case-insensitive name; a
//! duplicate name is an error (a package is a host bootstrap step, exactly
//! like [`pinkdb_ext_loader::linked::register`]). Nothing is active on any
//! store until it is loaded, and loading gives the package to **that store
//! only** — another store is unaffected even though the package is
//! registered process-wide.
//!
//! Activation is **atomic**. Before anything is installed the package's
//! scalar names, table-source names and format names are checked against
//! the built-in function registry, the process-global source/format
//! registries and everything already active on the store. A collision — or
//! any other failure — leaves the store exactly as it was. On success the
//! scalars register like a linked extension and the sources/formats are
//! appended to this store's own lists. Loading the same package twice is an
//! error, like loading any other extension twice.
//!
//! The SQL gate is unchanged: `LOAD EXTENSION` is off by default
//! ([`PinkDb::set_allow_sql_load_extension`]); the host API
//! [`PinkDb::load_extension_package`] is an explicit embedder choice and is
//! not gated.
//!
//! ## Resolution and durability
//!
//! [`PinkDb::load_extension`] resolves, in order: a loadable library
//! (native `dlopen`, unchanged) → a registered package → the scalar-only
//! linked registry (unchanged). On wasm the order is a package first, then
//! the linked registry (there is no dynamic loader). A replayed
//! `LOAD EXTENSION '<name>'` resolves a registered package too, so the
//! package must be registered **before** the store is opened — the same rule
//! the linked registry follows.
//!
//! A logged statement that reads one of the package's table sources or
//! writes through one of its sinks records the package in the WAL's
//! touched-extension set, exactly like a statement calling a dynamic
//! function records its extension. When the current WAL generation has not
//! loaded the package yet, the record is prefixed with `LOAD EXTENSION
//! '<package>'`; a checkpoint resets that coverage, so the first record
//! afterwards is prefixed again.
//!
//! ## wasm
//!
//! A package is plain Rust, so it works on wasm32 exactly like the linked
//! scalar model. Nothing here opens a file or calls `dlopen`.

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use pinkdb_ext_loader::{ExtHandle, LinkedExtension};

use crate::filefmt::FileFormat;
use crate::storage::PinkDb;
use crate::table_source::TableSource;
use crate::types::{PinkDbError, Result};

/// A host-published bundle of an extension's surfaces: optional scalar
/// functions, any number of table sources, and any number of COPY sinks.
///
/// Built with the `with_*` methods and published once with
/// [`register_extension_package`]. Cheap to clone (every surface is an
/// `Arc`-shared trait object or an `Arc`-shared linked extension).
#[derive(Clone, Debug)]
pub struct ExtensionPackage {
    name: String,
    scalars: Option<LinkedExtension>,
    table_sources: Vec<Arc<dyn TableSource>>,
    file_formats: Vec<Arc<dyn FileFormat>>,
}

impl ExtensionPackage {
    /// Start a package with `name` — the process-global registry key and the
    /// string `LOAD EXTENSION '<name>'` takes.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            scalars: None,
            table_sources: Vec::new(),
            file_formats: Vec::new(),
        }
    }

    /// Attach the package's scalar functions (the linked model). They
    /// register as one linked extension when the package is loaded.
    pub fn with_scalars(mut self, ext: LinkedExtension) -> Self {
        self.scalars = Some(ext);
        self
    }

    /// Attach a table source: its functions answer `FROM <fn>(...)` and its
    /// replacement scan answers `FROM 'path'`, as documented on
    /// [`TableSource`].
    pub fn with_table_source(mut self, source: Arc<dyn TableSource>) -> Self {
        self.table_sources.push(source);
        self
    }

    /// Attach a COPY write sink: it resolves by `FORMAT <name>` and by its
    /// declared file extensions.
    pub fn with_file_format(mut self, format: Arc<dyn FileFormat>) -> Self {
        self.file_formats.push(format);
        self
    }

    /// The package's registry key, as registered (original case).
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn scalars(&self) -> Option<&LinkedExtension> {
        self.scalars.as_ref()
    }

    pub(crate) fn table_sources(&self) -> &[Arc<dyn TableSource>] {
        &self.table_sources
    }

    pub(crate) fn file_formats(&self) -> &[Arc<dyn FileFormat>] {
        &self.file_formats
    }
}

fn registry() -> &'static Mutex<Vec<ExtensionPackage>> {
    static REGISTRY: OnceLock<Mutex<Vec<ExtensionPackage>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Lock the registry, recovering from poisoning (the same stance as every
/// other process-global registry in the engine: refusing every later
/// registration would be worse).
fn lock_registry() -> MutexGuard<'static, Vec<ExtensionPackage>> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Publish `pkg` in the process-global registry so a store can activate it
/// by name (`LOAD EXTENSION '<name>'`, [`PinkDb::load_extension`],
/// [`PinkDb::load_extension_package`]).
///
/// Fails when another package is registered under the same name
/// (case-insensitively), when the name is empty, or when the package
/// declares two surfaces of the same kind under one name — a registry entry
/// must be unambiguous. Registration is a host bootstrap step: a second
/// registration is a host bug, not a reload.
pub fn register_extension_package(pkg: ExtensionPackage) -> Result<()> {
    if pkg.name.trim().is_empty() {
        return Err(PinkDbError::Execution(
            "an extension package name must not be empty".to_string(),
        ));
    }
    let mut seen_sources: Vec<String> = Vec::new();
    for source in pkg.table_sources() {
        let key = source.name().to_ascii_lowercase();
        if seen_sources.contains(&key) {
            return Err(PinkDbError::Execution(format!(
                "extension package \"{}\" declares table source \"{}\" twice",
                pkg.name(),
                source.name()
            )));
        }
        seen_sources.push(key);
    }
    let mut seen_formats: Vec<String> = Vec::new();
    for format in pkg.file_formats() {
        let key = format.name().to_ascii_lowercase();
        if seen_formats.contains(&key) {
            return Err(PinkDbError::Execution(format!(
                "extension package \"{}\" declares file format \"{}\" twice",
                pkg.name(),
                format.name()
            )));
        }
        seen_formats.push(key);
    }
    let mut packages = lock_registry();
    if packages
        .iter()
        .any(|p| p.name().eq_ignore_ascii_case(pkg.name()))
    {
        return Err(PinkDbError::Execution(format!(
            "extension package \"{}\" is already registered",
            pkg.name()
        )));
    }
    packages.push(pkg);
    Ok(())
}

/// Every registered package name, in registration order.
pub fn extension_package_names() -> Vec<String> {
    lock_registry()
        .iter()
        .map(|p| p.name().to_string())
        .collect()
}

/// Whether a package is registered under `name` (case-insensitive).
pub(crate) fn is_registered_extension_package(name: &str) -> bool {
    lock_registry()
        .iter()
        .any(|p| p.name().eq_ignore_ascii_case(name))
}

/// The package registered under `name` (case-insensitive), cloned.
pub(crate) fn registered_extension_package(name: &str) -> Option<ExtensionPackage> {
    lock_registry()
        .iter()
        .find(|p| p.name().eq_ignore_ascii_case(name))
        .cloned()
}

impl PinkDb {
    /// Activate the registered extension package `name` on this store.
    /// Returns the package's declared name.
    ///
    /// The package must already be published with
    /// [`register_extension_package`]. Activation is atomic: every scalar,
    /// table-source and format name is checked against the built-ins, the
    /// process-global registries and this store's active surfaces before
    /// anything is installed; a refusal leaves the store unchanged. On
    /// success the package's scalars register like a linked extension and its
    /// sources and sinks belong to **this store only** — a different store is
    /// unaffected, and nothing is persisted (reopen the store and load the
    /// package again, or let WAL replay's `LOAD EXTENSION` prefix do it).
    ///
    /// Loading the same package twice is an error, like a second load of any
    /// other extension.
    pub fn load_extension_package(&mut self, name: &str) -> Result<String> {
        let key = name.to_ascii_lowercase();
        if self.loaded_packages.contains_key(&key) {
            return Err(PinkDbError::Execution(format!(
                "extension package \"{name}\" is already loaded"
            )));
        }
        if self.loaded_extensions.contains_key(&key) {
            return Err(PinkDbError::Execution(format!(
                "extension \"{name}\" is already loaded"
            )));
        }
        let pkg = registered_extension_package(name).ok_or_else(|| {
            PinkDbError::Execution(format!(
                "extension package \"{name}\" is not registered in this process (publish it with \
                 register_extension_package before loading; a durable store needs it registered \
                 before the store is opened)"
            ))
        })?;
        // Validate everything before installing anything: a rejected package
        // must not leave half of its surface behind.
        if let Some(scalars) = pkg.scalars() {
            self.validate_handle(&ExtHandle::Linked(scalars.clone()))
                .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        }
        for source in pkg.table_sources() {
            let source_key = source.name().to_ascii_lowercase();
            if self.table_source(&source_key).is_some()
                || crate::table_source::is_global_table_source(&source_key)
            {
                return Err(PinkDbError::Execution(format!(
                    "extension package \"{}\" cannot be loaded: table source \"{}\" is already \
                     registered",
                    pkg.name(),
                    source.name()
                )));
            }
        }
        for format in pkg.file_formats() {
            let format_key = format.name().to_ascii_lowercase();
            if self.file_format(&format_key).is_some()
                || crate::filefmt::is_global_file_format(&format_key)
            {
                return Err(PinkDbError::Execution(format!(
                    "extension package \"{}\" cannot be loaded: file format \"{}\" is already \
                     registered",
                    pkg.name(),
                    format.name()
                )));
            }
        }
        // Install. Nothing below can fail.
        let declared = pkg.name().to_string();
        if let Some(scalars) = pkg.scalars() {
            self.register_handle(ExtHandle::Linked(scalars.clone()))
                .map_err(|e| PinkDbError::Execution(e.to_string()))?;
            for function in scalars.functions() {
                self.package_dynamic_functions
                    .insert(function.name().to_ascii_lowercase(), declared.clone());
            }
        }
        self.loaded_packages.insert(key, declared.clone());
        for source in pkg.table_sources() {
            self.package_table_sources
                .insert(source.name().to_ascii_lowercase(), declared.clone());
            self.table_sources.push(Arc::clone(source));
        }
        for format in pkg.file_formats() {
            self.package_file_formats
                .insert(format.name().to_ascii_lowercase(), declared.clone());
            self.file_formats.push(Arc::clone(format));
        }
        Ok(declared)
    }

    /// The package that installed the active table source `name`, if any
    /// (case-insensitive). The binder records it so a logged statement that
    /// reads the source is WAL-prefixed with the package's `LOAD EXTENSION`.
    pub(crate) fn package_of_table_source(&self, name: &str) -> Option<&str> {
        self.package_table_sources
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// The package that installed the active file format `name`, if any
    /// (case-insensitive).
    pub(crate) fn package_of_file_format(&self, name: &str) -> Option<&str> {
        self.package_file_formats
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}
