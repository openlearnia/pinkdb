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

use crate::exec::QueryResult;
use crate::filefmt::FileFormat;
use crate::storage::PinkDb;
use crate::table_source::TableSource;
use crate::types::{PinkDbError, Result, Value};

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
    host_scalars: Vec<HostScalar>,
    host_statements: Vec<HostStatement>,
    vector_provider: Option<Arc<dyn crate::vector_index::VectorIndexProvider>>,
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
            host_scalars: Vec::new(),
            host_statements: Vec::new(),
            vector_provider: None,
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

    /// Attach a host scalar: a per-row function that also receives the
    /// store, for a function that reads the catalog or a table (wave 108a).
    /// It binds like a linked scalar, with named arguments allowed.
    pub fn with_host_scalar(mut self, scalar: HostScalar) -> Self {
        self.host_scalars.push(scalar);
        self
    }

    /// Attach a host statement: `CALL <name>(…)` runs it when no stored
    /// procedure of that name exists. It may read and write the store
    /// through [`HostCtx`]; the CALL stays one statement, so a durable store
    /// logs and replays it once.
    pub fn with_host_statement(mut self, statement: HostStatement) -> Self {
        self.host_statements.push(statement);
        self
    }

    /// Attach a vector index provider (wave 107b): `CREATE INDEX … USING
    /// <name>` resolves it while the package is loaded, and loading the
    /// package restores every serialized graph that names it.
    pub fn with_vector_index_provider(
        mut self,
        provider: Arc<dyn crate::vector_index::VectorIndexProvider>,
    ) -> Self {
        self.vector_provider = Some(provider);
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

    pub(crate) fn host_scalars(&self) -> &[HostScalar] {
        &self.host_scalars
    }

    pub(crate) fn host_statements(&self) -> &[HostStatement] {
        &self.host_statements
    }

    pub(crate) fn vector_provider(
        &self,
    ) -> Option<&Arc<dyn crate::vector_index::VectorIndexProvider>> {
        self.vector_provider.as_ref()
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
    let mut seen: Vec<String> = Vec::new();
    for name in pkg
        .host_scalars()
        .iter()
        .map(HostScalar::name)
        .chain(pkg.host_statements().iter().map(HostStatement::name))
    {
        let key = name.to_ascii_lowercase();
        if seen.contains(&key) {
            return Err(PinkDbError::Execution(format!(
                "extension package \"{}\" declares host function \"{}\" twice",
                pkg.name(),
                name
            )));
        }
        seen.push(key);
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

/// The registered package that owns vector-index provider `provider`.
pub(crate) fn registered_vector_provider_package(provider: &str) -> Option<String> {
    lock_registry()
        .iter()
        .find(|p| {
            p.vector_provider()
                .is_some_and(|v| v.name().eq_ignore_ascii_case(provider))
        })
        .map(|p| p.name().to_string())
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
        // A host scalar shares the scalar namespace with built-ins and every
        // extension; a collision is refused before anything is installed.
        for scalar in pkg.host_scalars() {
            let key = scalar.name().to_ascii_lowercase();
            if crate::sql::binder::ScalarFunc::from_name(&key).is_some()
                || crate::sql::binder::AggFunc::from_name(&key).is_some()
                || self.dynamic_scalar(&key).is_some()
                || self.host_scalars.contains_key(&key)
            {
                return Err(PinkDbError::Execution(format!(
                    "extension package \"{}\" cannot be loaded: function \"{}\" is already \
                     registered",
                    pkg.name(),
                    scalar.name()
                )));
            }
        }
        let hydration = match pkg.vector_provider() {
            Some(provider) => {
                if self.vector_provider(provider.name()).is_some() {
                    return Err(PinkDbError::Execution(format!(
                        "extension package \"{}\" cannot be loaded: vector index provider \"{}\" is \
                         already registered",
                        pkg.name(),
                        provider.name()
                    )));
                }
                // A graph is restored from bytes only; deserialize now so a
                // corrupt or foreign section rejects the whole load before any
                // surface is installed, then install the parsed graphs without
                // parsing them a second time.
                Some((
                    Arc::clone(provider),
                    self.take_hydrated_vector_indexes(provider.as_ref())?,
                ))
            }
            None => None,
        };
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
        for mut scalar in pkg.host_scalars().iter().cloned() {
            scalar.set_package(&declared);
            self.host_scalars
                .insert(scalar.name().to_ascii_lowercase(), scalar);
        }
        for mut statement in pkg.host_statements().iter().cloned() {
            statement.set_package(&declared);
            self.host_statements
                .insert(statement.name().to_ascii_lowercase(), statement);
        }
        if let Some((provider, ready)) = hydration {
            self.package_vector_providers
                .insert(provider.name().to_ascii_lowercase(), declared.clone());
            self.vector_providers.push(provider);
            self.install_hydrated_vector_indexes(ready);
        }
        Ok(declared)
    }

    /// The active host scalar `name` (case-insensitive), if any.
    pub(crate) fn host_scalar(&self, name: &str) -> Option<&HostScalar> {
        self.host_scalars.get(&name.to_ascii_lowercase())
    }

    /// The active host statement `name` (case-insensitive), if any.
    pub(crate) fn host_statement(&self, name: &str) -> Option<&HostStatement> {
        self.host_statements.get(&name.to_ascii_lowercase())
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

    /// The package that installed the active vector index provider `name`,
    /// if any (case-insensitive).
    pub(crate) fn package_of_vector_provider(&self, name: &str) -> Option<&str> {
        self.package_vector_providers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

// ---------------------------------------------------------------------------
// Host functions and host statements (wave 108a)
// ---------------------------------------------------------------------------
//
// A linked scalar function sees values only. An extension whose functions are
// *about the store* — a search index that reads a table, a statement that
// builds one — needs the store itself. These two surfaces are that door, and
// they are deliberately narrow:
//
// - a **host scalar** receives `&PinkDb` and one row's arguments; it reads
//   the catalog and tables and cannot write, so it stays a pure function of
//   the store and a statement that calls it is still logged as read-only SQL;
// - a **host statement** receives a [`HostCtx`], which executes internal SQL
//   through the same statement path a procedure body uses (so the outer
//   `CALL` remains one logged statement) and appends rows through the
//   ordinary insert funnel (keys, triggers, constraints, versions).
//
// Both bind through the existing `DynamicScalar` / `Call` plan nodes, so no
// new statement kind, plan node or parser rule exists for them.

/// One named parameter of a [`HostScalar`] or [`HostStatement`].
#[derive(Clone, Debug)]
pub struct HostParam {
    /// The name SQL writes (matched case-insensitively).
    pub name: &'static str,
    /// The declared type. Arguments are checked like ABI v1 arguments: exact
    /// type, with the numeric→DOUBLE and VARIANT→VARCHAR widenings.
    pub ty: crate::ext::ExtType,
    /// The value an omitted parameter takes; `None` makes it required.
    pub default: Option<Value>,
    /// The last parameter repeats for every extra positional argument
    /// (DuckDB's `*input_values`). Only meaningful on the last parameter.
    pub variadic: bool,
}

impl HostParam {
    /// A required positional-or-named parameter.
    pub fn new(name: &'static str, ty: crate::ext::ExtType) -> Self {
        Self {
            name,
            ty,
            default: None,
            variadic: false,
        }
    }

    /// Give the parameter a value used when the call omits it.
    pub fn default_value(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    /// Let the parameter repeat for every extra argument.
    pub fn variadic(mut self) -> Self {
        self.variadic = true;
        self
    }
}

/// A host scalar's callback: reads the store, returns one value per row.
type ScalarFn = Arc<dyn Fn(&PinkDb, &[Value]) -> Result<Value> + Send + Sync>;

/// A host scalar's chunk callback: gets every argument vector for the chunk
/// and returns the whole result column. See [`HostScalar::batched`].
type BatchFn =
    Arc<dyn Fn(&PinkDb, &[crate::vector::Vector]) -> Result<crate::vector::Vector> + Send + Sync>;

/// The tables a host scalar's call reads, given its arguments (`None` when an
/// argument is not constant) and the statement's database. See
/// [`HostScalar::reads_tables`].
type ReadsFn = fn(&[Option<&Value>], &str) -> Result<Vec<(String, String)>>;

/// A host statement's callback: runs SQL and inserts rows through the store.
type StatementFn = Arc<dyn Fn(&mut HostCtx<'_>, &[Value]) -> Result<QueryResult> + Send + Sync>;

/// What a call to an external service (a host scalar declared
/// [`HostScalar::external_io`]) gets while [`ServePolicy::
/// allow_external_services`] is off. Its own switch, not the files one: a
/// remote call can cost money and leaks the text it is given, while a file
/// read leaks whatever the path names. The operator's `allow-files` sets both.
/// It lives here, next to the marker, because the executor refuses the call on
/// `wasm32` too (where `serve` is absent).
pub const SERVICES_HINT: &str = "external services are disabled: this session reads tables only \
                             (start the server with allow-files to permit a call to an \
                             external service, such as embed())";

/// A function that also sees the store (see the module section on host
/// functions). Bound and evaluated like a linked scalar: per row
/// ([`HostScalar::new`]) or over a whole chunk ([`HostScalar::batched`]).
#[derive(Clone)]
pub struct HostScalar {
    name: String,
    package: String,
    params: Vec<HostParam>,
    ret: crate::ext::ExtType,
    eval: Option<ScalarFn>,
    batch: Option<BatchFn>,
    reads: Option<ReadsFn>,
    external_io: bool,
}

impl std::fmt::Debug for HostScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostScalar")
            .field("name", &self.name)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl HostScalar {
    /// `name`, its parameters (positional or `name := value`), the declared
    /// return type, and the callback.
    pub fn new(
        name: impl Into<String>,
        params: Vec<HostParam>,
        ret: crate::ext::ExtType,
        eval: impl Fn(&PinkDb, &[Value]) -> Result<Value> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            package: String::new(),
            params,
            ret,
            eval: Some(Arc::new(eval)),
            batch: None,
            reads: None,
            external_io: false,
        }
    }

    /// `name`, its parameters, the declared return type, and a callback that
    /// sees **every row of the chunk at once**: each argument arrives as its
    /// evaluated [`Vector`](crate::vector::Vector) (an invalid cell is a
    /// NULL, and the callback decides what that means), and the return value
    /// is the result column for the same rows, with the declared type and
    /// length. This is the shape for work whose cost is one round trip per
    /// call — an HTTP request for an embedding, say — so it can batch,
    /// deduplicate and cache across the chunk instead of paying per row.
    /// There is no per-row fallback: a batched scalar is only ever called
    /// this way.
    pub fn batched(
        name: impl Into<String>,
        params: Vec<HostParam>,
        ret: crate::ext::ExtType,
        eval: impl Fn(&PinkDb, &[crate::vector::Vector]) -> Result<crate::vector::Vector>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            package: String::new(),
            params,
            ret,
            eval: None,
            batch: Some(Arc::new(eval)),
            reads: None,
            external_io: false,
        }
    }

    /// Declare that a call leaves the process — a network service, possibly a
    /// paid one. `serve`'s read-only policy refuses such a call unless
    /// [`ServePolicy::allow_external_services`] is on; it is its own switch,
    /// not the files one, because a remote call leaks the text it is given
    /// while a file read leaks whatever the path names (the operator's
    /// `allow-files` sets both).
    pub fn external_io(mut self) -> Self {
        self.external_io = true;
        self
    }

    /// Declare the tables a call reads.
    ///
    /// The binder records them as the statement's reads, exactly as if the
    /// plan held a `Scan`, so RBAC asks for `SELECT` on them and the WAL's
    /// frame sets see them. The callback runs once, at bind time, so it must
    /// reject an argument it cannot decide from (an index chosen per row
    /// would make the check unenforceable). Return `(database, table)` pairs;
    /// the default database is passed in for an unqualified name.
    pub fn reads_tables(mut self, reads: ReadsFn) -> Self {
        self.reads = Some(reads);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn params(&self) -> &[HostParam] {
        &self.params
    }

    pub fn ret(&self) -> crate::ext::ExtType {
        self.ret
    }

    /// The package that installed the scalar (the WAL's `LOAD EXTENSION`
    /// prefix names it).
    pub fn package(&self) -> &str {
        &self.package
    }

    pub(crate) fn set_package(&mut self, package: &str) {
        self.package = package.to_string();
    }

    pub(crate) fn call(&self, db: &PinkDb, args: &[Value]) -> Result<Value> {
        let eval = self.eval.as_ref().ok_or_else(|| {
            PinkDbError::Execution(format!(
                "host function {}() is evaluated a chunk at a time",
                self.name
            ))
        })?;
        eval(db, args)
    }

    pub(crate) fn batch(&self) -> Option<&BatchFn> {
        self.batch.as_ref()
    }

    /// Whether a call leaves the process (a paid service), which the agent
    /// policy gates. Only the native `serve` module reads it.
    pub(crate) fn is_external_io(&self) -> bool {
        self.external_io
    }

    pub(crate) fn reads(&self) -> Option<ReadsFn> {
        self.reads
    }
}

/// The store handle a [`HostStatement`] runs with.
///
/// `execute` parses and runs **one** statement through the same path a
/// procedure body uses: it is not logged separately, so the `CALL` that
/// triggered it stays the store's one WAL record, and a replayed `CALL`
/// re-runs the same work deterministically.
pub struct HostCtx<'a> {
    db: &'a mut PinkDb,
    db_name: String,
    statement: String,
}

impl<'a> HostCtx<'a> {
    pub(crate) fn new(db: &'a mut PinkDb, db_name: String, statement: String) -> Self {
        Self {
            db,
            db_name,
            statement,
        }
    }

    /// The store, read-only.
    pub fn db(&self) -> &PinkDb {
        self.db
    }

    /// The store, mutable. A host statement that maintains an engine-side
    /// structure (a vector index's graph) uses this; ordinary catalog/data
    /// changes should go through [`Self::execute`] so they keep their usual
    /// validation.
    pub fn db_mut(&mut self) -> &mut PinkDb {
        self.db
    }

    /// The session database the `CALL` ran in.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Run one SQL statement against the store, without a WAL record of its
    /// own. Errors name the host statement that issued it.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let mut statements = crate::sql::parse_statements(sql)
            .map_err(|e| PinkDbError::Parse(format!("{}: {e}", self.statement)))?;
        if statements.len() != 1 {
            return Err(PinkDbError::Execution(format!(
                "{}: execute() takes exactly one statement, got {}",
                self.statement,
                statements.len()
            )));
        }
        let stmt = statements.remove(0);
        let name = self.statement.clone();
        let (result, _wrote) = crate::procedure::run_call_statement(
            self.db,
            &self.db_name,
            crate::sql::binder::ParamSlots::new(Vec::new()),
            &stmt,
            None,
            move |m| PinkDbError::Execution(format!("{name}: {m}")),
        )?;
        Ok(result)
    }

    /// Append `rows` to `db.table` through the ordinary insert funnel, so
    /// keys, constraints, triggers and the table version behave exactly like
    /// `INSERT`.
    pub fn insert_rows(&mut self, db: &str, table: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let budget = Arc::new(crate::exec::ExecBudget::unbounded());
        crate::trigger::insert_rows(self.db, db, table, rows, &budget)
    }
}

/// A `CALL <name>(…)` that a package provides (see the module section on host
/// statements). A stored procedure of the same name shadows it.
#[derive(Clone)]
pub struct HostStatement {
    name: String,
    package: String,
    params: Vec<HostParam>,
    run: StatementFn,
}

impl std::fmt::Debug for HostStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostStatement")
            .field("name", &self.name)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl HostStatement {
    pub fn new(
        name: impl Into<String>,
        params: Vec<HostParam>,
        run: impl Fn(&mut HostCtx<'_>, &[Value]) -> Result<QueryResult> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            package: String::new(),
            params,
            run: Arc::new(run),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn params(&self) -> &[HostParam] {
        &self.params
    }

    pub fn package(&self) -> &str {
        &self.package
    }

    pub(crate) fn set_package(&mut self, package: &str) {
        self.package = package.to_string();
    }

    pub(crate) fn call(&self, ctx: &mut HostCtx<'_>, args: &[Value]) -> Result<QueryResult> {
        (self.run)(ctx, args)
    }
}
