//! The lake catalog: the JSON metadata of one version.
//!
//! `catalog.json` and `snapshots/<version>.json` have the same shape:
//!
//! ```json
//! {
//!   "format_version": 3,
//!   "updated": "<iso8601>",
//!   "version": 3,
//!   "tables": {
//!     "main.t": { "files": ["data/main.t/<uuid>.vortex"], "rows": 5, "format": "vortex" }
//!   }
//! }
//! ```
//!
//! `format_version` 2 and 3 are Vortex lakes (3: larger row blocks, the
//! default for new writes): every table entry declares `"format": "vortex"`
//! (those writers emit Vortex only, so a `"pink"` entry there is corrupt). A `format_version` 1 manifest — written by
//! the wave-34 writer — has no `format` field and implies the engine's
//! `.pink` single-table bytes.
//!
//! Schema and typing live inside the data files, never here.
//!
//! Wave 64 adds the optional **snapshot record** to a manifest:
//! `"author"`, `"commit_message"` and `"changes"` (see
//! [`crate::snapshot::ChangeSummary`]). The record is additive — an older
//! reader ignores the fields (serde does not deny unknown fields), and every
//! field is optional, so the manifest shape of a save that recorded nothing
//! is byte-identical to the pre-wave-64 shape. `"version"` and `"updated"`
//! (the version's publish time, millisecond ISO-8601 UTC) were already there;
//! together they are the per-version record `.lake snapshots` lists.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::snapshot::ChangeSummary;

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
    /// A Vortex file (format_version 2 and 3; 3 is the default for new writes).
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
    /// The save that published this version: the author it recorded
    /// (`ducklake_set_commit_message`'s `author`), if any. Optional and
    /// additive; absent in manifests saved without a message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) author: Option<String>,
    /// The save's commit message, if any (DuckLake's `commit_message`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) commit_message: Option<String>,
    /// What this version changed relative to the one before it, computed at
    /// save time from the two manifests (see [`ChangeSummary`]). Absent for
    /// the empty `create` manifest and when the previous manifest was
    /// unavailable (e.g. expired snapshots).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) changes: Option<ChangeSummary>,
}

/// One table of one version: the immutable data files it is made of and the
/// total row count (checked on load).
///
/// A v2 writer always emits exactly one `.vortex` file per table; a v1
/// manifest's entries have no `format` (they are engine `.pink` bytes), and
/// the loader appends multiple files in order (forward-compatible).
///
/// A v1 entry may also carry a `StatsBlock` (see the `stats` module): the
/// exact per-column statistics `.lake attach` answers
/// `COUNT`/`COUNT(col)`/`MIN`/`MAX` from without opening the data file. The
/// block is published atomically with the entry (catalog and snapshot are
/// byte-identical) and CRC-protected; a v1 entry without one is a lake saved
/// before statistics existed, and attach refuses it with a clear message. A
/// v2/v3 (Vortex) entry must not carry one — Vortex statistics live in the
/// file footer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub(crate) files: Vec<String>,
    pub(crate) rows: u64,
    /// The data-file format; required in a `format_version` 2 manifest,
    /// absent (⇒ [`FileFormat::Pink`]) in a `format_version` 1 manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) format: Option<FileFormat>,
    /// The v1 `.pink` statistics block (see the struct docs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stats: Option<crate::stats::StatsBlock>,
}

impl TableEntry {
    /// The format to load this entry's files with: v1 entries without a
    /// `format` field are engine `.pink` bytes.
    pub(crate) fn format(&self) -> FileFormat {
        self.format.unwrap_or(FileFormat::Pink)
    }
}
