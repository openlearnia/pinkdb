//! The lake catalog: the JSON metadata of one version.
//!
//! `catalog.json` and `snapshots/<version>.json` have the same shape:
//!
//! ```json
//! {
//!   "format_version": 2,
//!   "updated": "<iso8601>",
//!   "version": 3,
//!   "tables": {
//!     "main.t": { "files": ["data/main.t/<uuid>.vortex"], "rows": 5, "format": "vortex" }
//!   }
//! }
//! ```
//!
//! `format_version` 2 is the Vortex default: every table entry declares
//! `"format": "vortex"` (the v2 writer emits Vortex only, so a `"pink"` entry
//! in a v2 manifest is corrupt). A `format_version` 1 manifest — written by
//! the wave-34 writer — has no `format` field and implies the engine's
//! `.pink` single-table bytes.
//!
//! Schema and typing live inside the data files, never here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The data-file format of one table entry.
///
/// `Pink` is the engine's own `.pink` single-table persistence bytes (the v1
/// lake format: a `format_version` 1 manifest has no `format` field at all,
/// so it loads as `Pink`); `Vortex` is a Vortex file (the v2 default and the
/// only format a v2 manifest may declare). Serialized as a lowercase string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FileFormat {
    /// The engine's `.pink` single-table persistence bytes (format_version 1).
    Pink,
    /// A Vortex file (format_version 2, the default for new writes).
    Vortex,
}

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
/// total row count (checked on load).
///
/// A v2 writer always emits exactly one `.vortex` file per table; a v1
/// manifest's entries have no `format` (they are engine `.pink` bytes), and
/// the loader appends multiple files in order (forward-compatible).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub(crate) files: Vec<String>,
    pub(crate) rows: u64,
    /// The data-file format; required in a `format_version` 2 manifest,
    /// absent (⇒ [`FileFormat::Pink`]) in a `format_version` 1 manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) format: Option<FileFormat>,
}

impl TableEntry {
    /// The format to load this entry's files with: v1 entries without a
    /// `format` field are engine `.pink` bytes.
    pub(crate) fn format(&self) -> FileFormat {
        self.format.unwrap_or(FileFormat::Pink)
    }
}
