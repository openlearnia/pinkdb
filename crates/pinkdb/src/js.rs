//! The `js` cargo feature's bridge to the [`pinkdb_js`] sandbox crate.
//!
//! Kept to two calls on purpose: the engine binding, the host implementation
//! (`db.exec`/`db.query` over the executor) and the value conversions live in
//! `pinkdb-js`; this module only decides what this build can do, so that
//! `pinkdb`'s SQL surface never grows a second engine path.

/// Can this build evaluate JavaScript procedure bodies? `Err` is the
/// documented refusal (`pinkdb-js`'s engine binding is the recorded
/// follow-up).
pub(crate) fn engine_available() -> Result<(), String> {
    pinkdb_js::engine_available()
}
