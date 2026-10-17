//! Per-version snapshot records: what each published version changed, and
//! what `.lake snapshots` lists.
//!
//! DuckLake keeps a `snapshot` row per commit —
//! `snapshot_id`, `snapshot_time`, `schema_version`, `changes` (a map),
//! `author`, `commit_message` — and pinkdb's lake mirrors the parts its
//! whole-store snapshot model has: every manifest already carries `version`
//! and `updated` (the publish time, millisecond ISO-8601 UTC), and wave 64
//! adds the optional `author`, `commit_message` and `changes` record to the
//! manifest itself (see [`ChangeSummary`]). Reading any of them is a plain
//! JSON read of one manifest — no data file is touched.
//!
//! A pinkdb save publishes the *whole store*, so its change summary is the
//! diff of two manifests, computed at save time (the previous manifest may
//! later be expired away — then the summary is simply absent):
//!
//! - `tables_created` / `tables_dropped`: keys that appeared or disappeared
//!   (their rows are the tables themselves — no separate row delta);
//! - `tables_rewritten`: keys present in both whose data files changed (a
//!   whole-store save writes fresh data files for every table, so this is
//!   every surviving table — the honest signal that a table's *contents* may
//!   have changed even when its row count did not);
//! - `rows_added` / `rows_removed`: the net row-count deltas per *surviving*
//!   table (value-only edits do not move a count, hence the separate
//!   `tables_rewritten` list).
//!
//! This is deliberately *not* DuckLake's change map (`insertions` /
//! `deletions` counts per table id, `inlined_insert`, ...): pinkdb has no
//! per-row change log, and inventing one in the read/catalog layer would be
//! a lie. The shapes are pinned by tests and by the `.lake snapshots` output.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalog::{Catalog, TableEntry};

/// What one save changed relative to the version it was published on top of.
///
/// Every list is sorted (the manifest's `tables` is a `BTreeMap`) and every
/// map is keyed by flat `db.table`. An empty summary means the two manifests
/// were byte-identical in the way this summary measures.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChangeSummary {
    /// Tables that did not exist in the previous version.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_created: Vec<String>,
    /// Tables that existed in the previous version and no longer do.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_dropped: Vec<String>,
    /// Tables present in both versions whose data files changed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_rewritten: Vec<String>,
    /// Net rows gained per table present in both versions
    /// (`after.rows - before.rows`, when positive).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rows_added: BTreeMap<String, u64>,
    /// Net rows lost per table present in both versions
    /// (`before.rows - after.rows`, when positive).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rows_removed: BTreeMap<String, u64>,
}

impl ChangeSummary {
    /// True when nothing measurable changed between the two versions.
    pub fn is_empty(&self) -> bool {
        self.tables_created.is_empty()
            && self.tables_dropped.is_empty()
            && self.tables_rewritten.is_empty()
            && self.rows_added.is_empty()
            && self.rows_removed.is_empty()
    }

    /// The diff of two manifests' tables: see the module docs for the rules.
    pub(crate) fn diff(
        before: &BTreeMap<String, TableEntry>,
        after: &BTreeMap<String, TableEntry>,
    ) -> Self {
        let mut summary = Self::default();
        for (key, entry) in after {
            match before.get(key) {
                None => summary.tables_created.push(key.clone()),
                Some(previous) => {
                    if previous.files != entry.files {
                        summary.tables_rewritten.push(key.clone());
                    }
                    if entry.rows > previous.rows {
                        summary
                            .rows_added
                            .insert(key.clone(), entry.rows - previous.rows);
                    } else if previous.rows > entry.rows {
                        summary
                            .rows_removed
                            .insert(key.clone(), previous.rows - entry.rows);
                    }
                }
            }
        }
        for key in before.keys() {
            if !after.contains_key(key) {
                summary.tables_dropped.push(key.clone());
            }
        }
        summary
    }
}

/// One published version's record, as `Lake::snapshots` returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotInfo {
    /// The published version (`1..=MAX_VERSION`); pinkdb's versions start at
    /// the empty catalog `create` publishes as 1 (DuckLake's snapshot ids
    /// start at 0 for the catalog creation — a deliberate 1-off).
    pub version: u64,
    /// The version's publish time, exactly as the manifest's `updated`
    /// field stores it (millisecond ISO-8601 UTC, e.g.
    /// `2026-10-17T12:34:56.789Z`).
    pub time: String,
    /// [`SnapshotInfo::time`] as microseconds since the Unix epoch, when it
    /// parses (always, for manifests this writer published; `None` for a
    /// tampered manifest with a malformed time).
    pub time_micros: Option<i64>,
    /// The catalog `format_version` of this version's manifest.
    pub format_version: u32,
    /// The author the save recorded, if any.
    pub author: Option<String>,
    /// The save's commit message, if any.
    pub message: Option<String>,
    /// Number of tables in this version.
    pub tables: usize,
    /// Total rows across those tables (the manifest's own counts).
    pub rows: u64,
    /// What changed relative to the previous version, if the save recorded it
    /// (absent for the initial `create` and when the previous manifest was
    /// unavailable).
    pub changes: Option<ChangeSummary>,
}

impl SnapshotInfo {
    /// The record of one manifest, plus [`SnapshotInfo::time_micros`].
    pub(crate) fn of(manifest: &Catalog, time_micros: Option<i64>) -> Self {
        Self {
            version: manifest.version,
            time: manifest.updated.clone(),
            time_micros,
            format_version: manifest.format_version,
            author: manifest.author.clone(),
            message: manifest.commit_message.clone(),
            tables: manifest.tables.len(),
            rows: manifest.tables.values().map(|e| e.rows).sum(),
            changes: manifest.changes.clone(),
        }
    }
}
