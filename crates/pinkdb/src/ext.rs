//! Loadable-extension host integration (ABI v1) — the dynamic scalar registry.
//!
//! The `pinkdb-ext-loader` crate owns everything unsafe
//! (dlopen, calling extension callbacks, result-text ownership). This module
//! is the safe engine side:
//!
//! - [`PinkDb::load_extension`] loads a cdylib and registers its scalar
//!   functions into [`PinkDb::dynamic_functions`](crate::PinkDb) (the
//!   [`ExtHost`] implementation below, atomic: a rejected registration
//!   registers nothing);
//! - the binder resolves unknown function names against that registry
//!   (case-insensitively) into [`BoundExpr::DynamicScalar`](crate::sql::binder::BoundExpr::DynamicScalar);
//! - the executor marshals argument vectors per row into the ABI values,
//!   calls the callback, and converts the result back;
//! - the optimizer never constant-folds an expression tree containing a
//!   dynamic call (and never pushes a predicate containing one through a
//!   join/union), because a callback is opaque: it may be impure,
//!   non-deterministic, or fail.
//!
//! Extensions are process-level: they are not part of a database snapshot.
//! A `LOAD EXTENSION` statement is WAL-logged, and every logged record that
//! *touches* a dynamic function is prefixed with `LOAD EXTENSION '<path>'`
//! when the current WAL generation has not loaded that extension yet (see
//! [`crate::wal::Wal`]). Replay therefore re-loads exactly the extensions the
//! replayed records need. A checkpoint rolls the WAL and never persists an
//! extension: after a checkpoint the next record touching one is prefixed
//! again, and an extension whose file is gone at replay time is a hard
//! recovery error (never a silent drop). Extensions loaded before the last
//! checkpoint are gone after a reopen — re-load them.
//!
//! Native-only: the loader links `libloading` (dlopen), which does not build
//! for wasm32. On wasm targets everything above is cfg'd out and
//! [`PinkDb::load_extension`] is a stub returning a clear "extensions are not
//! supported on wasm targets" error (the `LOAD EXTENSION` statement fails the
//! same way).

use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result};

#[cfg(not(target_arch = "wasm32"))]
use crate::types::{DataType, Value};

#[cfg(not(target_arch = "wasm32"))]
use pinkdb_ext_loader::{
    DynamicFunction, ExtArg, ExtError, ExtHost, ExtValue, LoadedExtension,
};

/// Re-exported so the binder and executor can name registered ABI types.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use pinkdb_ext_loader::ExtType;

#[cfg(not(target_arch = "wasm32"))]
use crate::sql::binder::{AggFunc, ScalarFunc, WindowFunc};
#[cfg(not(target_arch = "wasm32"))]
use crate::vector::{Vector, VectorData};

/// Map an engine type to its ABI tag.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn ext_type_of(dtype: DataType) -> ExtType {
    match dtype {
        DataType::Boolean => ExtType::Boolean,
        DataType::Integer => ExtType::Integer,
        DataType::Bigint => ExtType::Bigint,
        DataType::Double => ExtType::Double,
        DataType::Varchar => ExtType::Varchar,
        DataType::Date => ExtType::Date,
        DataType::Timestamp => ExtType::Timestamp,
    }
}

/// Map an ABI tag back to an engine type.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn dtype_of_ext(dtype: ExtType) -> DataType {
    match dtype {
        ExtType::Boolean => DataType::Boolean,
        ExtType::Integer => DataType::Integer,
        ExtType::Bigint => DataType::Bigint,
        ExtType::Double => DataType::Double,
        ExtType::Varchar => DataType::Varchar,
        ExtType::Date => DataType::Date,
        ExtType::Timestamp => DataType::Timestamp,
    }
}

#[cfg(not(target_arch = "wasm32"))]
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
    pub fn load_extension(&mut self, path: &str) -> Result<String> {
        pinkdb_ext_loader::load_extension(self, path)
            .map_err(|e| PinkDbError::Execution(e.to_string()))
    }

    /// Registered dynamic scalar function by case-insensitive name.
    pub(crate) fn dynamic_scalar(&self, name: &str) -> Option<&DynamicFunction> {
        self.dynamic_functions.get(&name.to_ascii_lowercase())
    }

    /// Source paths of the extensions providing `names` (lowercased registry
    /// keys), deduped in first-use order. Unknown names are skipped: they were
    /// bound against this registry moments earlier and extensions are never
    /// unloaded, so this can only skip a name that was never registered.
    pub(crate) fn dynamic_function_paths(&self, names: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for name in names {
            if let Some(f) = self.dynamic_functions.get(&name.to_ascii_lowercase()) {
                let path = f.source_path();
                if !out.iter().any(|p| p == path) {
                    out.push(path.to_string());
                }
            }
        }
        out
    }

    /// Distinct paths of every loaded extension (including ones that
    /// registered no functions), for seeding the WAL's tracked set after
    /// recovery.
    pub(crate) fn loaded_extension_paths(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for ext in self.loaded_extensions.values() {
            if !out.iter().any(|p| p == ext.path()) {
                out.push(ext.path().to_string());
            }
        }
        out
    }
}

/// wasm32 stub: the loader is not linked there, so loading is a clear error.
/// (`LOAD EXTENSION` surfaces exactly this message.) The WAL bookkeeping
/// helpers are stubs too: no dynamic function can ever be registered, so no
/// path can ever be touched.
#[cfg(target_arch = "wasm32")]
impl PinkDb {
    pub fn load_extension(&mut self, path: &str) -> Result<String> {
        let _ = path;
        Err(PinkDbError::Execution(
            "extensions are not supported on wasm targets".to_string(),
        ))
    }

    pub(crate) fn dynamic_function_paths(&self, _names: &[String]) -> Vec<String> {
        Vec::new()
    }

    pub(crate) fn loaded_extension_paths(&self) -> Vec<String> {
        Vec::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ExtHost for PinkDb {
    fn register_extension(
        &mut self,
        ext: LoadedExtension,
    ) -> std::result::Result<(), ExtError> {
        // Pin duplicate loads by name: a second load of the same extension
        // must fail even if it were to register a disjoint set of functions
        // (init would run twice against shared state).
        let name_key = ext.name().to_ascii_lowercase();
        if self.loaded_extensions.contains_key(&name_key) {
            return Err(ExtError::registration(format!(
                "extension \"{}\" is already loaded",
                ext.name()
            )));
        }
        // Validate everything before inserting anything: a rejected extension
        // must not leave half its functions behind.
        let functions = ext.functions().to_vec();
        let mut keys: Vec<String> = Vec::with_capacity(functions.len());
        for f in &functions {
            let key = f.name().to_ascii_lowercase();
            if ScalarFunc::from_name(&key).is_some()
                || AggFunc::from_name(&key).is_some()
                || WindowFunc::ranking(&key).is_some()
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
        // Keep the handle: the shared library stays mapped while the store
        // holds the registration — even for an extension that registered no
        // functions — and the name stays pinned against a duplicate load.
        self.loaded_extensions.insert(name_key, ext);
        Ok(())
    }
}

/// Append one row of `vector` as an ABI argument, borrowing VARCHAR bytes.
///
/// `declared` is the registered argument type. A NULL cell becomes
/// [`ExtArg::Null`] carrying it — the host always invokes the callback and
/// lets it decide what NULL means (per the header's NULL-handling rule), and
/// the tag comes from the registration rather than the vector (a typeless
/// `NULL` literal evaluates to an all-NULL INTEGER vector).
#[cfg(not(target_arch = "wasm32"))]
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
    });
}

/// Convert one ABI result into an engine value, checking it against the
/// bound result type. VARCHAR bytes become a Rust string (the extension's
/// buffer was already copied and freed by `DynamicFunction::call`).
#[cfg(not(target_arch = "wasm32"))]
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
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
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
        ] {
            assert_eq!(dtype_of_ext(ext_type_of(dtype)), dtype);
        }
    }

    #[test]
    fn stock_store_has_no_dynamic_functions() {
        let db = PinkDb::new();
        assert!(db.dynamic_functions.is_empty());
        assert!(db.loaded_extensions.is_empty());
        assert!(db.dynamic_scalar("ext_double_it").is_none());
        assert!(db.dynamic_function_paths(&["ext_double_it".to_string()]).is_empty());
        assert!(db.loaded_extension_paths().is_empty());
        // Built-ins are not in the dynamic registry either.
        assert!(db.dynamic_scalar("upper").is_none());
    }

    #[test]
    fn result_conversion_checks_the_type() {
        let ok = ext_value_to_value("f", DataType::Integer, ExtValue::Integer(7)).unwrap();
        assert_eq!(ok, Value::Integer(7));
        let null = ext_value_to_value("f", DataType::Varchar, ExtValue::Null).unwrap();
        assert_eq!(null, Value::Null);
        let text =
            ext_value_to_value("f", DataType::Varchar, ExtValue::Varchar(b"hi".to_vec()))
                .unwrap();
        assert_eq!(text, Value::Varchar("hi".to_string()));
        // A VARCHAR result where INTEGER was bound (and registered) is a
        // contract violation, not a silent coercion.
        let err = ext_value_to_value("shout", DataType::Integer, ExtValue::Varchar(b"x".to_vec()))
            .unwrap_err();
        assert!(err.to_string().contains("returned VARCHAR, expected INTEGER"), "{err}");
        // Invalid UTF-8 gets its own clear error.
        let err =
            ext_value_to_value("shout", DataType::Varchar, ExtValue::Varchar(vec![0xff, 0xfe]))
                .unwrap_err();
        assert!(err.to_string().contains("invalid UTF-8"), "{err}");
    }
}
