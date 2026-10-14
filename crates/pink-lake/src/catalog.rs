//! The lake catalog: the JSON metadata of one version.
//!
//! `catalog.json` and `snapshots/<version>.json` have the same shape:
//!
//! ```json
//! {
//!   "format_version": 1,
//!   "updated": "<iso8601>",
//!   "version": 3,
//!   "tables": { "main.t": { "files": ["data/main.t/<uuid>.pink"], "rows": 5 } }
//! }
//! ```
//!
//! Schema and typing live inside the data files, never here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One version of a lake. Keys of `tables` are flat `db.table` names; a
/// `BTreeMap` keeps the JSON deterministic (sorted).
///
/// Every field is required: a reader that defaulted a missing `tables` would
/// silently load a missing-tables (or hand-truncated) catalog as an *empty*
/// lake, so `tables` is deliberately not `#[serde(default)]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Catalog {
    pub(crate) format_version: u32,
    pub(crate) updated: String,
    pub(crate) version: u64,
    pub(crate) tables: BTreeMap<String, TableEntry>,
}

/// One table of one version: the immutable data files it is made of and the
/// total row count (checked on load). A v1 writer always emits exactly one
/// file per table; the loader appends multiple in order (forward-compatible).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub(crate) files: Vec<String>,
    pub(crate) rows: u64,
}
