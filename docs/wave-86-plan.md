# Wave 86 — extension capability: table functions + COPY sinks (plan)

Started 2026-10-21; **done** the same day. ROADMAP row 86: grow the extension model so extensions
can expose scans/table functions (`read_*`, replacement scans) and COPY
sinks — linked model first; the native C ABI for scans is a later,
separate step.

## Where things stand

- Scans and sinks already exist as **host** traits in the engine:
  `pinkdb::table_source::TableSource` (bind-time schema, projection and
  filter pushdown, chunked scans; `FROM read_x(...)` and `FROM 'file.x'`)
  and `pinkdb::filefmt::FileFormat` (`COPY … TO … (FORMAT x)`). First-party
  readers (parquet, arrow) register them **process-globally** at startup.
- The **extension** model (`LOAD EXTENSION`, wave 29 dlopen + wave 49
  linked registry in `pinkdb-ext-loader`) only carries scalar functions.
  The loader crate cannot name the engine traits (the engine depends on it).

## Design: extension packages (engine-level, linked)

```rust
// crates/pinkdb/src/package.rs (new, re-exported from the crate root)
pub struct ExtensionPackage { /* name, scalars, table_sources, file_formats */ }
impl ExtensionPackage {
    pub fn new(name: impl Into<String>) -> Self;
    pub fn with_scalars(self, ext: pinkdb_ext_loader::LinkedExtension) -> Self; // optional
    pub fn with_table_source(self, s: Arc<dyn TableSource>) -> Self;
    pub fn with_file_format(self, f: Arc<dyn FileFormat>) -> Self;
    pub fn name(&self) -> &str;
}
pub fn register_extension_package(pkg: ExtensionPackage) -> Result<()>; // process-global
pub fn extension_package_names() -> Vec<String>;
```

- **Registration** is process-global and by case-insensitive name; a
  duplicate name is an error. Nothing is active on any store until it is
  loaded.
- **Activation:** `LOAD EXTENSION '<name>'` (and `PinkDb::load_extension`)
  resolves, in order: a loadable library (native dlopen, unchanged) → a
  registered **package** → the scalar-only linked registry (unchanged). A
  package activation is **atomic**: its scalars, table-source names and
  format names are checked for collisions (with built-ins, globally
  registered sources/formats, and earlier loads) before anything is
  installed; then the scalars register like a linked extension and the
  sources/formats are added to **this store only**. Loading the same
  package twice is an error, like other extensions.
- **Gate:** unchanged — `LOAD EXTENSION` stays off by default
  (`--allow-load-extension`, `Database::set_allow_sql_load_extension`).
  Loosening it for compiled-in packages is a recorded follow-up.
- **WAL:** `LOAD EXTENSION '<name>'` is logged as today. Replay resolves a
  registered package name (it must be registered before the store opens,
  exactly like linked scalars). A logged statement that reads a package's
  table source or writes through its sink gets the same
  `LOAD EXTENSION`-prefix treatment as a statement calling a dynamic
  function when the current WAL generation has not loaded it yet.
- **RBAC / policies:** unchanged — table functions need ADMIN in
  authenticated sessions and COPY sinks keep the COPY path policy (pass 6).
- **wasm:** packages are plain Rust, so they work on wasm32 (the linked
  model).

## Tasks

- **T1 (engine):** `package.rs`, activation in `ext.rs`/`exec`, store-local
  source/format lists consulted together with the global ones (binder,
  executor, COPY), replay resolution, WAL prefixing, docs, and
  `crates/pinkdb/tests/packages.rs`: a test package with a table function
  (`pkg_range(n)` returning n rows, honoring projection) and a sink
  (`FORMAT pkgtsv`) — not active before LOAD; active after on that store
  only; second store unaffected; atomic refusal on a colliding name;
  double load refused; gate refusal when not allowed; durable store:
  `CREATE TABLE t AS SELECT * FROM pkg_range(5)` replays after reopen when
  the package is registered, and fails with a clear error when it is not;
  checkpoint then a new statement using the source gets a LOAD prefix.
- **T2 (SDK + docs):** `ext-sdk/rust` (wave 51) gains a helper/example for
  building a package; `docs/` extension guide section; README.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files your task
names. Run the task's checks and report what changed and the results.

## How it went

- T1 (engine packages) ran on CommandCode and was accepted after review:
  package names ride the binder's existing "extensions used" list, so the
  WAL's `LOAD EXTENSION` prefixing covers package sources and sinks with no
  new mechanism.
- T2 changed scope: the SDK crate depends only on the loader, so the
  package form went to an engine-dependent extension instead —
  `pinkdb_arrow::package()` / `register_package()` — plus the extensions
  guide and README. OpenCode hung (20 min, no output, `--standalone` too);
  the same brief completed on CommandCode.
- Not done here (follow-ups): a native C ABI for scans/sinks; making the
  first-party parquet/arrow readers opt-in packages by default (a
  behavior change); a trust distinction letting compiled-in packages
  bypass the `LOAD EXTENSION` opt-in.
