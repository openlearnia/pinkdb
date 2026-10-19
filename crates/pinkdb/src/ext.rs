//! Extension host integration — the scalar-function registry and the two
//! loading models that feed it.
//!
//! Two models, one registry (waves 29A / 49):
//!
//! - **native `dlopen`** (ABI v1): [`PinkDb::load_extension`] loads a cdylib
//!   through `pinkdb-ext-loader`, which owns every `unsafe` (dlopen, symbol
//!   lookup, callback invocation, result-text ownership). A `LOAD EXTENSION
//!   '<path>'` statement is the SQL face of the same call.
//! - **linked registration** (wave 49): a wasm sandbox has no dynamic linker,
//!   so there an extension is linked into the host and published to the
//!   process-global registry in `pinkdb_ext_loader::linked` (safe Rust
//!   metadata + callbacks). [`PinkDb::load_linked_extension`] resolves a
//!   registered name; on wasm, `LOAD EXTENSION '<name>'` does exactly that.
//!   The linked model is available natively too: it is the host-function
//!   registration path for embedders, and the reason a WAL record naming a
//!   linked extension can replay natively (the dlopen attempt fails, the
//!   registry lookup succeeds).
//!
//! Whichever model registered a function, the rest of the engine sees the
//! same thing — a [`pinkdb_ext_loader::ExtFunction`] in
//! [`PinkDb`](crate::PinkDb)'s registry, keyed by lowercased name:
//!
//! - the binder resolves unknown function names against that registry
//!   (case-insensitively) into
//!   [`BoundExpr::DynamicScalar`](crate::sql::binder::BoundExpr::DynamicScalar);
//! - the executor marshals argument vectors per row into the ABI values,
//!   calls the callback, and converts the result back;
//! - the optimizer never constant-folds an expression tree containing a
//!   dynamic call (and never pushes a predicate containing one through a
//!   join/union), because a callback is opaque in both models: it may be
//!   impure, non-deterministic, or fail;
//! - a statement the engine WAL-logs may not call a *volatile*
//!   (nondeterministic) extension function: the binder refuses it before it
//!   runs (replay re-executes the logged SQL text, and a volatile callback
//!   could produce a different value there). Deterministic is the default in
//!   both loading models; a function declares volatility at registration
//!   (the ABI's `PINKDB_EXT_FLAG_VOLATILE`, or
//!   [`pinkdb_ext_loader::LinkedFunction::volatile`]).
//!
//! Extensions are process-level: they are not part of a database snapshot.
//! A `LOAD EXTENSION` statement is WAL-logged, and every logged record that
//! *touches* a dynamic function is prefixed with `LOAD EXTENSION '<source>'`
//! (the library path, or the registry name for a linked function) when the
//! current WAL generation has not loaded that extension yet (see
//! [`crate::wal::Wal`]). Replay therefore re-loads exactly the extensions the
//! replayed records need. A checkpoint rolls the WAL and never persists an
//! extension: after a checkpoint the next record touching one is prefixed
//! again, and an extension whose source is gone at replay time is a hard
//! recovery error (never a silent drop). Extensions loaded before the last
//! checkpoint are gone after a reopen — re-load them (for a linked extension
//! that means: re-register it before opening the store).

use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value};

use pinkdb_ext_loader::linked::LinkedExtHost;
use pinkdb_ext_loader::{ExtArg, ExtError, ExtFunction, ExtHandle, ExtValue, LinkedExtension};

/// Re-exported so the binder and executor can name registered ABI types.
pub(crate) use pinkdb_ext_loader::ExtType;

#[cfg(not(target_arch = "wasm32"))]
use pinkdb_ext_loader::{ExtHost, LoadedExtension};

use crate::sql::binder::{AggFunc, ScalarFunc, WindowFunc};
use crate::vector::{Vector, VectorData};

/// Map an engine type to its ABI tag.
pub(crate) fn ext_type_of(dtype: DataType) -> Option<ExtType> {
    match dtype {
        DataType::Boolean => Some(ExtType::Boolean),
        DataType::Integer => Some(ExtType::Integer),
        DataType::Bigint => Some(ExtType::Bigint),
        DataType::Double => Some(ExtType::Double),
        DataType::Varchar => Some(ExtType::Varchar),
        DataType::Date => Some(ExtType::Date),
        DataType::Timestamp => Some(ExtType::Timestamp),
        // INTERVAL is an additive ABI v1 type (tag 9, the pinkdb C ABI's
        // number): a function may declare it as an argument or return type.
        // DECIMAL still has no ABI type; the binder's exact-type check
        // rejects such arguments with its usual message.
        DataType::Interval => Some(ExtType::Interval),
        DataType::Decimal { .. } => None,
    }
}

/// Map an ABI tag back to an engine type.
pub(crate) fn dtype_of_ext(dtype: ExtType) -> DataType {
    match dtype {
        ExtType::Boolean => DataType::Boolean,
        ExtType::Integer => DataType::Integer,
        ExtType::Bigint => DataType::Bigint,
        ExtType::Double => DataType::Double,
        ExtType::Varchar => DataType::Varchar,
        ExtType::Date => DataType::Date,
        ExtType::Timestamp => DataType::Timestamp,
        ExtType::Interval => DataType::Interval,
    }
}

impl PinkDb {
    /// Load a loadable extension from `path` (a `.dylib` / `.so` / `.dll`
    /// exporting ABI v1 — see `crates/pinkdb-ext-loader/include/pinkdb_extension.h`)
    /// and register its scalar functions. Returns the extension's declared
    /// name.
    ///
    /// Atomic: on any error (library, ABI version, init, or a conflicting
    /// function name) nothing is registered. Names are matched
    /// case-insensitively and may not collide with a built-in function or an
    /// already-loaded extension function; an extension name may not be loaded
    /// twice. Extensions are process-level — not persisted, not part of any
    /// snapshot — and stay loaded (the library stays mapped) as long as the
    /// store exists, even when an extension registers no functions. A
    /// `LOAD EXTENSION` statement is WAL-logged, and records that call its
    /// functions are `LOAD EXTENSION`-prefixed when the current WAL
    /// generation has not loaded it yet, so a durable reopen re-loads exactly
    /// what replay needs (a missing file at replay time is a hard recovery
    /// error, never a silent drop).
    ///
    /// Native only. When `path` is not a loadable library but a *linked*
    /// extension is registered under exactly that name, the linked extension
    /// is loaded instead ([`PinkDb::load_linked_extension`]): that is how a
    /// replayed `LOAD EXTENSION '<name>'` record — written when a linked
    /// function was called — resolves here. Anything else keeps the dlopen
    /// error, unchanged.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_extension(&mut self, path: &str) -> Result<String> {
        match pinkdb_ext_loader::load_extension(self, path) {
            Ok(name) => Ok(name),
            Err(dlopen_error) => {
                if pinkdb_ext_loader::linked::is_registered(path) {
                    self.load_linked_extension(path)
                } else {
                    Err(PinkDbError::Execution(dlopen_error.to_string()))
                }
            }
        }
    }

    /// wasm targets have no dynamic linker, so the argument is a **registry
    /// name**, not a path: the linked registry
    /// (`pinkdb_ext_loader::linked::register`) is this target's only extension
    /// source. `LOAD EXTENSION '<name>'` is the SQL face of this call.
    #[cfg(target_arch = "wasm32")]
    pub fn load_extension(&mut self, name: &str) -> Result<String> {
        self.load_linked_extension(name)
    }

    /// Load a *linked* extension by its registry name (the wasm loading model;
    /// available on every target). Looks the name up in the process-global
    /// linked registry — case-insensitively — and registers its functions.
    /// Returns the extension's declared name.
    ///
    /// Extensions are published at host bootstrap
    /// (`pinkdb_ext_loader::linked::register`); nothing is opened from disk,
    /// and an unregistered name fails with a clear error naming the registry.
    /// The registration rules are the native ones: atomic, case-insensitive
    /// names, no collision with a built-in or with an already-registered
    /// function, and an extension name may not be loaded twice. A durable
    /// reopen that replays a record calling a linked function needs the name
    /// registered *before* the store is opened.
    pub fn load_linked_extension(&mut self, name: &str) -> Result<String> {
        pinkdb_ext_loader::linked::load(self, name)
            .map_err(|e| PinkDbError::Execution(e.to_string()))
    }

    /// Registered dynamic scalar function by case-insensitive name — either
    /// loading model.
    pub(crate) fn dynamic_scalar(&self, name: &str) -> Option<&ExtFunction> {
        self.dynamic_functions.get(&name.to_ascii_lowercase())
    }

    /// Sources of the extensions providing `names` (lowercased registry
    /// keys), deduped in first-use order. Unknown names are skipped: they were
    /// bound against this registry moments earlier and extensions are never
    /// unloaded, so this can only skip a name that was never registered.
    pub(crate) fn dynamic_function_paths(&self, names: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for name in names {
            if let Some(f) = self.dynamic_functions.get(&name.to_ascii_lowercase()) {
                let source = f.source();
                if !out.iter().any(|p| p == source) {
                    out.push(source.to_string());
                }
            }
        }
        out
    }

    /// The first volatile (nondeterministic) dynamic function among `names`
    /// (lowercased registry keys), by original name, in first-use order.
    /// Unknown names are skipped, like [`Self::dynamic_function_paths`].
    ///
    /// This is the WAL-divergence guard's lookup: the binder calls it for
    /// every statement whose SQL text the engine would log, and refuses the
    /// statement when it finds one. A function is volatile only when it was
    /// declared so at registration (the native ABI's
    /// `PINKDB_EXT_FLAG_VOLATILE`, or the linked model's
    /// `LinkedFunction::volatile`); deterministic is the default and keeps
    /// every pre-flag extension exactly as it was.
    pub(crate) fn first_volatile_dynamic_function(&self, names: &[String]) -> Option<String> {
        names.iter().find_map(|name| {
            self.dynamic_functions
                .get(&name.to_ascii_lowercase())
                .filter(|f| f.is_volatile())
                .map(|f| f.name().to_string())
        })
    }

    /// Distinct sources of every loaded extension (including ones that
    /// registered no functions), for seeding the WAL's tracked set after
    /// recovery: the library path for a `dlopen` extension, the registry name
    /// for a linked one.
    pub(crate) fn loaded_extension_paths(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for ext in self.loaded_extensions.values() {
            if !out.iter().any(|p| p == ext.source()) {
                out.push(ext.source().to_string());
            }
        }
        out
    }

    /// The declared name of the extension whose *source* is exactly `path`,
    /// if any. Used by WAL replay (hazard H5): a replayed `LOAD EXTENSION
    /// '<source>'` whose extension is already loaded in this process is
    /// satisfied by the existing registration instead of re-running `init`
    /// (which the duplicate-load guard would refuse) — or, for a linked
    /// extension, instead of consulting the registry again. A different source
    /// that claims the same extension *name* is still a hard replay error —
    /// running with the wrong library would silently change query results.
    pub(crate) fn loaded_extension_name_for_path(&self, path: &str) -> Option<String> {
        self.loaded_extensions
            .values()
            .find(|ext| ext.source() == path)
            .map(|ext| ext.name().to_string())
    }

    /// Whether an extension is loaded in this store under `name` (the
    /// extension's *declared* name, matched case-insensitively) — the
    /// question a host that auto-loads its own first-party extensions asks
    /// before running `LOAD EXTENSION '<name>'`, whose second load is a
    /// documented error.
    pub fn is_extension_loaded(&self, name: &str) -> bool {
        self.loaded_extensions
            .contains_key(&name.to_ascii_lowercase())
    }

    /// Register one extension handle — the shared half of both loading
    /// models: validate everything, then insert everything.
    fn register_handle(&mut self, handle: ExtHandle) -> std::result::Result<(), ExtError> {
        // Pin duplicate loads by name: a second load of the same extension
        // must fail even if it were to register a disjoint set of functions
        // (init would run twice against shared state).
        let name_key = handle.name().to_ascii_lowercase();
        if self.loaded_extensions.contains_key(&name_key) {
            return Err(ExtError::registration(format!(
                "extension \"{}\" is already loaded",
                handle.name()
            )));
        }
        // Validate everything before inserting anything: a rejected extension
        // must not leave half its functions behind.
        let functions = handle.functions();
        let mut keys: Vec<String> = Vec::with_capacity(functions.len());
        for f in &functions {
            let key = f.name().to_ascii_lowercase();
            if ScalarFunc::from_name(&key).is_some()
                || AggFunc::from_name(&key).is_some()
                || WindowFunc::from_name(&key).is_some()
            {
                return Err(ExtError::registration(format!(
                    "function \"{}\" conflicts with a built-in function",
                    f.name()
                )));
            }
            if keys.contains(&key) || self.dynamic_functions.contains_key(&key) {
                return Err(ExtError::registration(format!(
                    "function \"{}\" is already registered",
                    f.name()
                )));
            }
            keys.push(key);
        }
        for (f, key) in functions.into_iter().zip(keys) {
            self.dynamic_functions.insert(key, f);
        }
        // Keep the handle: the shared library stays mapped (native) and the
        // linked functions stay referenced while the store holds the
        // registration — even for an extension that registered no functions —
        // and the name stays pinned against a duplicate load.
        self.loaded_extensions.insert(name_key, handle);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ExtHost for PinkDb {
    fn register_extension(&mut self, ext: LoadedExtension) -> std::result::Result<(), ExtError> {
        self.register_handle(ExtHandle::Dynamic(ext))
    }
}

impl LinkedExtHost for PinkDb {
    fn register_linked_extension(
        &mut self,
        ext: LinkedExtension,
    ) -> std::result::Result<(), ExtError> {
        self.register_handle(ExtHandle::Linked(ext))
    }
}

/// Append one row of `vector` as an ABI argument, borrowing VARCHAR bytes.
///
/// `declared` is the registered argument type. A NULL cell becomes
/// [`ExtArg::Null`] carrying it — the host always invokes the callback and
/// lets it decide what NULL means (per the header's NULL-handling rule), and
/// the tag comes from the registration rather than the vector (a typeless
/// `NULL` literal evaluates to an all-NULL INTEGER vector).
pub(crate) fn push_ext_arg<'a>(
    out: &mut Vec<ExtArg<'a>>,
    vector: &'a Vector,
    row: usize,
    declared: ExtType,
) {
    if !vector.is_valid(row) {
        out.push(ExtArg::Null(declared));
        return;
    }
    out.push(match vector.data() {
        VectorData::Boolean(v) => ExtArg::Boolean(v[row]),
        VectorData::Int32(v) => ExtArg::Integer(v[row]),
        VectorData::Int64(v) => ExtArg::Bigint(v[row]),
        VectorData::Float64(v) => ExtArg::Double(v[row]),
        VectorData::VarChar(v) => ExtArg::Varchar(v[row].as_bytes()),
        VectorData::Date32(v) => ExtArg::Date(v[row]),
        VectorData::TimestampMicros(v) => ExtArg::Timestamp(v[row]),
        VectorData::Interval(v) => ExtArg::Interval {
            months: v[row].months,
            days: v[row].days,
            micros: v[row].micros,
        },
        // Unreachable: `ext_type_of` rejects DECIMAL in the binder's
        // exact-type check before any callback can run.
        VectorData::Decimal { .. } => {
            unreachable!("extensions v1 do not accept DECIMAL arguments")
        }
    });
}

/// Convert one ABI result into an engine value, checking it against the
/// bound result type. VARCHAR bytes become a Rust string (the native loader
/// already copied and freed the extension's buffer; a linked callback
/// returned owned bytes).
pub(crate) fn ext_value_to_value(
    function: &str,
    dtype: DataType,
    value: ExtValue,
) -> Result<Value> {
    let mismatch = |got: ExtType| {
        PinkDbError::Execution(format!(
            "extension function {function}() returned {got}, expected {dtype}"
        ))
    };
    Ok(match value {
        ExtValue::Null => Value::Null,
        ExtValue::Boolean(b) => {
            if dtype != DataType::Boolean {
                return Err(mismatch(ExtType::Boolean));
            }
            Value::Boolean(b)
        }
        ExtValue::Integer(i) => {
            if dtype != DataType::Integer {
                return Err(mismatch(ExtType::Integer));
            }
            Value::Integer(i)
        }
        ExtValue::Bigint(i) => {
            if dtype != DataType::Bigint {
                return Err(mismatch(ExtType::Bigint));
            }
            Value::Bigint(i)
        }
        ExtValue::Double(x) => {
            if dtype != DataType::Double {
                return Err(mismatch(ExtType::Double));
            }
            Value::Double(x)
        }
        ExtValue::Varchar(bytes) => {
            if dtype != DataType::Varchar {
                return Err(mismatch(ExtType::Varchar));
            }
            let s = String::from_utf8(bytes).map_err(|_| {
                PinkDbError::Execution(format!(
                    "extension function {function}() returned invalid UTF-8"
                ))
            })?;
            Value::Varchar(s)
        }
        ExtValue::Date(d) => {
            if dtype != DataType::Date {
                return Err(mismatch(ExtType::Date));
            }
            Value::Date(d)
        }
        ExtValue::Timestamp(us) => {
            if dtype != DataType::Timestamp {
                return Err(mismatch(ExtType::Timestamp));
            }
            Value::Timestamp(us)
        }
        ExtValue::Interval {
            months,
            days,
            micros,
        } => {
            if dtype != DataType::Interval {
                return Err(mismatch(ExtType::Interval));
            }
            Value::Interval {
                months,
                days,
                micros,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping_is_bijective() {
        for dtype in [
            DataType::Boolean,
            DataType::Integer,
            DataType::Bigint,
            DataType::Double,
            DataType::Varchar,
            DataType::Date,
            DataType::Timestamp,
            DataType::Interval,
        ] {
            assert_eq!(dtype_of_ext(ext_type_of(dtype).unwrap()), dtype);
        }
        assert_eq!(
            ext_type_of(DataType::Decimal {
                precision: 4,
                scale: 2
            }),
            None
        );
    }

    #[test]
    fn result_conversion_accepts_an_interval_only_for_an_interval() {
        let ok = ext_value_to_value(
            "f",
            DataType::Interval,
            ExtValue::Interval {
                months: 1,
                days: 2,
                micros: 3,
            },
        )
        .unwrap();
        assert_eq!(
            ok,
            Value::Interval {
                months: 1,
                days: 2,
                micros: 3
            }
        );
        let err = ext_value_to_value(
            "f",
            DataType::Varchar,
            ExtValue::Interval {
                months: 0,
                days: 0,
                micros: 0,
            },
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("returned INTERVAL, expected VARCHAR"),
            "{err}"
        );
    }

    #[test]
    fn stock_store_has_no_dynamic_functions() {
        let db = PinkDb::new();
        assert!(db.dynamic_functions.is_empty());
        assert!(db.loaded_extensions.is_empty());
        assert!(db.dynamic_scalar("ext_double_it").is_none());
        assert!(
            db.dynamic_function_paths(&["ext_double_it".to_string()])
                .is_empty()
        );
        assert!(db.loaded_extension_paths().is_empty());
        // Built-ins are not in the dynamic registry either.
        assert!(db.dynamic_scalar("upper").is_none());
    }

    /// An unregistered linked name is a clean, documented error on every
    /// target (on wasm this is what `LOAD EXTENSION` reports).
    #[test]
    fn loading_an_unregistered_linked_extension_is_a_clear_error() {
        let mut db = PinkDb::new();
        let err = db
            .load_linked_extension("wave49_not_registered")
            .unwrap_err()
            .to_string();
        assert!(err.contains("is not registered"), "{err}");
        assert!(err.contains("linked::register"), "{err}");
        assert!(db.loaded_extensions.is_empty() && db.dynamic_functions.is_empty());
    }

    #[test]
    fn result_conversion_checks_the_type() {
        let ok = ext_value_to_value("f", DataType::Integer, ExtValue::Integer(7)).unwrap();
        assert_eq!(ok, Value::Integer(7));
        let null = ext_value_to_value("f", DataType::Varchar, ExtValue::Null).unwrap();
        assert_eq!(null, Value::Null);
        let text =
            ext_value_to_value("f", DataType::Varchar, ExtValue::Varchar(b"hi".to_vec())).unwrap();
        assert_eq!(text, Value::Varchar("hi".to_string()));
        // A VARCHAR result where INTEGER was bound (and registered) is a
        // contract violation, not a silent coercion.
        let err = ext_value_to_value("shout", DataType::Integer, ExtValue::Varchar(b"x".to_vec()))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("returned VARCHAR, expected INTEGER"),
            "{err}"
        );
        // Invalid UTF-8 gets its own clear error.
        let err = ext_value_to_value(
            "shout",
            DataType::Varchar,
            ExtValue::Varchar(vec![0xff, 0xfe]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid UTF-8"), "{err}");
    }
}
