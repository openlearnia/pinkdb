//! Change-data reads (wave 68): a version range's row-level changes, the
//! pinkdb counterpart of DuckLake's `ducklake_table_insertions` /
//! `ducklake_table_deletions` / `ducklake_table_changes` table functions
//! (pinkdb has no SQL table functions yet, so this is the library surface the
//! CLI's `.lake changes` renders; the same functions become table functions
//! when that path lands).
//!
//! # Shape and semantics (pinned against the ducklake extension)
//!
//! `ducklake_table_insertions('catalog', 'schema', 'table', start, end)`
//! returns the table's columns; `ducklake_table_deletions` the same;
//! `ducklake_table_changes` (a macro over the two) adds
//! `snapshot_id BIGINT, rowid BIGINT, change_type VARCHAR`. The version range
//! is **inclusive on both ends** (`insertions(2,2)` returns snapshot 2's
//! rows). [`ChangeRow`] mirrors exactly that: `version`, `row_id`
//! (`Option<u64>` — the reference's non-nullable `rowid`; pinkdb leaves it
//! `NULL` for rows whose file has no recorded row-id range, see below),
//! `change_type` and the values in the read version's logical schema order.
//!
//! A row is an **insertion** at the version that added its row version and a
//! **deletion** at the version that ended it (an UPDATE is a deletion plus an
//! insertion, DuckLake's `update_preimage`/`update_postimage` pair — the
//! reference pairs them by row id; pinkdb's commit deltas do not record that
//! pairing, so a change row set reports plain `insert`/`delete`, documented).
//!
//! # Where the rows come from
//!
//! - **Inlined rows** ([`crate::catalog::InlinedRow`]): exact — `begin`/`end`
//!   are the change versions and the values were stored with the row. A
//!   `flushed` end is a materialization, not a deletion, and is never
//!   reported (the row's insertion stays reported at its begin version; the
//!   flushed file's rows are skipped as insertions so nothing is counted
//!   twice).
//! - **File rows**: a version's plain appended files (`commits/<v>.json`
//!   `data_files` without compaction/flush markers) are insertions; positional
//!   delete files added by a version are deletions of the target file's rows
//!   at the recorded positions. Values are read from the (immutable) file and
//!   projected through the read version's schema record, exactly like a scan.
//! - **Full saves** (`replaced` deltas) are the one representation this build
//!   cannot turn into row-level changes: a save rewrites every file, and the
//!   delta records only counts. A range that includes a save version is
//!   refused with a clean error naming the version (a table *created* by a
//!   save is fine — every row was inserted there). Maintenance outputs
//!   (`compacted_files`/`removed_delete_files`) are skipped: their rows were
//!   already reported when their sources were written.
//!
//! # Row ids
//!
//! `row_id` is the table's per-row identity: an inlined record's `row_id`, or
//! `file.row_id_start + position` for a file row. Files published by a full
//! save's predecessors or by maintenance have no recorded range, so their
//! rows read `row_id = None`; the values stay exact. Unlike DuckLake (which
//! persists `_ducklake_internal_row_id` columns inside its data files), a
//! flush re-keys the flushed rows' file range, so a row id is stable only
//! while the row's physical file survives — the values are the documented
//! join key across a flush.
//!
//! # Expired history
//!
//! The reads depend on the retained objects: a missing `commits/<v>.json`
//! (expired by `expire_snapshots`) or a cleaned delete/data file makes the
//! read a clean error naming the object, never a silently partial answer.

use std::path::Path;

use pinkdb::{DataTable, Value};

use crate::catalog::{CommitManifest, DeleteRef, TableEntry};
use crate::local::CATALOG_FILE;
use crate::schema::TableSchema;
use crate::storage::Storage;
use crate::{Lake, LakeError, Result, only_table, split_key, validate_rel_path};

/// What one [`ChangeRow`] is: the row version was added (`Insert`) or ended
/// (`Delete`) at its version. Serialized lowercase, matching DuckLake's
/// `change_type` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    /// The version added the row version (`ducklake_table_changes`'s
    /// `insert`).
    Insert,
    /// The version ended the row version (deleted or superseded; the
    /// reference's `delete`/`update_preimage`).
    Delete,
}

impl ChangeKind {
    /// The DuckLake `change_type` spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeKind::Insert => "insert",
            ChangeKind::Delete => "delete",
        }
    }
}

impl std::fmt::Display for ChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One row version of a table's change data: the version that changed it, its
/// row id (when the build recorded one), what happened, and the values in the
/// read version's logical schema order.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeRow {
    /// The version that added (`Insert`) or ended (`Delete`) the row version.
    pub version: u64,
    /// The table's row identity; `None` when the row's file carries no
    /// recorded row-id range.
    pub row_id: Option<u64>,
    /// The change.
    pub change_type: ChangeKind,
    /// The row's values, in the read version's schema order.
    pub values: Vec<Value>,
}

impl Lake {
    /// Every change row of `table` over the inclusive version range
    /// `[start, end]` (`end` = the handle's current version when `None`), in
    /// `(version, insert-before-delete, row id)` order — the
    /// `ducklake_table_changes` shape. See the module docs for the semantics
    /// and the documented differences.
    pub fn table_changes(
        &self,
        table: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<ChangeRow>> {
        self.changes(table, start, end, true, true)
    }

    /// The **insertions** of `table` over `[start, end]` — the
    /// `ducklake_table_insertions` shape (the reference returns the table's
    /// columns only; the [`ChangeRow`] metadata rides along and the CLI drops
    /// it for this read).
    pub fn table_insertions(
        &self,
        table: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<ChangeRow>> {
        self.changes(table, start, end, true, false)
    }

    /// The **deletions** of `table` over `[start, end]` — the
    /// `ducklake_table_deletions` shape.
    pub fn table_deletions(
        &self,
        table: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<ChangeRow>> {
        self.changes(table, start, end, false, true)
    }

    fn changes(
        &self,
        table: &str,
        start: u64,
        end: Option<u64>,
        insertions: bool,
        deletions: bool,
    ) -> Result<Vec<ChangeRow>> {
        let (_, _) = split_key(table, &self.root)?;
        let end = end.unwrap_or(self.version);
        if start < 1 || end > self.version || start > end {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "invalid change-data range {start}..={end}: versions are 1..={} and start \
                     must not exceed end (DuckLake's range is inclusive on both ends)",
                    self.version
                ),
            });
        }
        let (manifest, manifest_path) = self.manifest(Some(end))?;
        let entry = manifest
            .tables
            .get(table)
            .ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!("table \"{table}\" does not exist at version {end}"),
            })?;
        let record = TableSchema::from_entry(table, entry, &manifest_path)?;
        let mut inserted: Vec<ChangeRow> = Vec::new();
        let mut deleted: Vec<ChangeRow> = Vec::new();
        // The inlined history (cumulative, so the read version's manifest
        // holds every record a `[start, end]` read can see).
        if insertions || deletions {
            for row in &entry.inlined {
                if insertions && row.begin >= start && row.begin <= end {
                    let values = crate::inlined::decode_row(
                        record.as_ref().ok_or_else(|| LakeError::Corrupt {
                            path: manifest_path.clone(),
                            message: format!(
                                "table \"{table}\" carries inlined rows but no schema record"
                            ),
                        })?,
                        row,
                        table,
                        &manifest_path,
                    )?;
                    inserted.push(ChangeRow {
                        version: row.begin,
                        row_id: Some(row.row_id),
                        change_type: ChangeKind::Insert,
                        values,
                    });
                }
                if deletions
                    && !row.flushed
                    && let Some(end_version) = row.end
                    && end_version >= start
                    && end_version <= end
                {
                    let values = crate::inlined::decode_row(
                        record.as_ref().ok_or_else(|| LakeError::Corrupt {
                            path: manifest_path.clone(),
                            message: format!(
                                "table \"{table}\" carries inlined rows but no schema \
                                         record"
                            ),
                        })?,
                        row,
                        table,
                        &manifest_path,
                    )?;
                    deleted.push(ChangeRow {
                        version: end_version,
                        row_id: Some(row.row_id),
                        change_type: ChangeKind::Delete,
                        values,
                    });
                }
            }
        }
        // The per-version audit trail: `commits/<v>.json`.
        for version in start..=end {
            let Some(delta) = read_commit_manifest(&*self.storage, &self.root, version)? else {
                // Version 1 is the empty create manifest and never carries a
                // delta; anything else missing was expired or cleaned.
                if version == 1 {
                    continue;
                }
                return Err(LakeError::Corrupt {
                    path: self.storage.abs(&format!("commits/{version}.json")),
                    message: format!(
                        "commit manifest for version {version} is missing (expired or cleaned); \
                         row-level changes over a range that includes it are not available"
                    ),
                });
            };
            let Some(table_delta) = delta.tables.get(table) else {
                continue;
            };
            let replay_all_rows = if table_delta.replaced {
                // A full save rewrites the table wholesale; its rows are
                // insertions only when the table itself was created by it.
                if self.table_existed_before(table, version)? {
                    return Err(LakeError::NotRepresentable {
                        message: format!(
                            "version {version} replaced table \"{table}\" wholesale (a full \
                             `.lake save`); per-row change data is not recorded for full \
                             saves — narrow the range past it or use `.lake snapshots` for \
                             the summary"
                        ),
                    });
                }
                true
            } else {
                false
            };
            if insertions {
                let plain_append = table_delta.inlined_flushed == 0
                    && table_delta.compacted_files.is_empty()
                    && table_delta.removed_delete_files.is_empty();
                if plain_append || replay_all_rows {
                    for file in &table_delta.data_files {
                        let rows = self.read_file_rows(
                            table,
                            entry,
                            record.as_ref(),
                            file.path(),
                            file.rows(),
                            None,
                        )?;
                        let start_id = file.row_id_start();
                        for (pos, values) in rows.into_iter().enumerate() {
                            inserted.push(ChangeRow {
                                version,
                                row_id: start_id.map(|s| s.saturating_add(pos as u64)),
                                change_type: ChangeKind::Insert,
                                values,
                            });
                        }
                    }
                }
            }
            if deletions {
                for delete in &table_delta.delete_files {
                    let rows = self.read_delete_rows(table, entry, record.as_ref(), delete)?;
                    for (row_id, values) in rows {
                        deleted.push(ChangeRow {
                            version,
                            row_id,
                            change_type: ChangeKind::Delete,
                            values,
                        });
                    }
                }
            }
        }
        let mut out = Vec::new();
        if insertions {
            out.extend(inserted);
        }
        if deletions {
            out.extend(deleted);
        }
        out.sort_by(|a, b| {
            a.version
                .cmp(&b.version)
                .then_with(|| kind_rank(a.change_type).cmp(&kind_rank(b.change_type)))
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        Ok(out)
    }

    /// The rows one positional delete file removes: `(row id, values)` per
    /// position, the values read from the target file and projected onto
    /// `record` (the read version's logical schema).
    fn read_delete_rows(
        &self,
        table: &str,
        entry: &TableEntry,
        record: Option<&TableSchema>,
        delete: &DeleteRef,
    ) -> Result<Vec<(Option<u64>, Vec<Value>)>> {
        // The target may have been compacted away by a later version: the
        // delete file names it as of its own version, so a missing entry
        // simply means its row ids are not recorded (values stay exact).
        let target_start = entry
            .files
            .iter()
            .find(|f| f.path() == delete.file)
            .and_then(|f| f.row_id_start());
        let path = self.storage.abs(&delete.path);
        let bytes = self
            .storage
            .read(&delete.path, &format!("reading {}", path.display()))?;
        let positions = crate::delete_file::decode(&bytes, &path)?;
        if positions.len() as u64 != delete.count {
            return Err(LakeError::Corrupt {
                path,
                message: format!(
                    "table \"{table}\": delete file declares {} positions, holds {}",
                    delete.count,
                    positions.len()
                ),
            });
        }
        let rows = self.read_file_rows(
            table,
            entry,
            record,
            delete.file.as_str(),
            None,
            Some(&positions),
        )?;
        Ok(rows
            .into_iter()
            .enumerate()
            .map(|(i, values)| (target_start.map(|s| s.saturating_add(positions[i])), values))
            .collect())
    }

    /// Read rows from one data file named by a commit delta, projected onto
    /// `record` (or as written, for a legacy entry): every row, or the given
    /// ascending positions. The file must be reachable by its path — it may
    /// have been compacted away by a *later* version (the delta names it as
    /// of its own version), so membership in the read version's file list is
    /// not required; its identity and row count are checked against the bytes.
    fn read_file_rows(
        &self,
        table: &str,
        entry: &TableEntry,
        record: Option<&TableSchema>,
        rel: &str,
        expect_rows: Option<u64>,
        positions: Option<&[u64]>,
    ) -> Result<Vec<Vec<Value>>> {
        let (dbname, table_name) = split_key(table, &self.storage.abs(CATALOG_FILE))?;
        // The file's own metadata is the authority: a delete-file target or a
        // compaction source is named by path, and every value read is checked
        // against the decoded file.
        let mini = self.open_data_file(rel, entry.format())?;
        let (file_db, file_table) = only_table(&mini, rel, &self.root)?;
        if !file_db.eq_ignore_ascii_case(dbname)
            || !file_table.name.eq_ignore_ascii_case(table_name)
        {
            return Err(LakeError::Corrupt {
                path: self.storage.abs(rel),
                message: format!(
                    "data file holds {file_db}.{}, but the catalog says \"{table}\"",
                    file_table.name
                ),
            });
        }
        let rows = file_table.num_rows() as u64;
        if let Some(expect_rows) = expect_rows
            && expect_rows != rows
        {
            return Err(LakeError::Corrupt {
                path: self.storage.abs(CATALOG_FILE),
                message: format!(
                    "table \"{table}\": commit delta says \"{rel}\" holds {expect_rows} \
                         rows, but the file holds {rows}"
                ),
            });
        }
        // A delete position is per the file's physical rows; `.pink` files
        // decode whole and project in memory, Vortex files too on this path.
        let file_epoch = entry
            .files
            .iter()
            .find(|f| f.path() == rel)
            .and_then(|f| f.schema_version());
        let file_table: DataTable = match record {
            Some(record) => {
                let physical = file_table.schema();
                let file_version = file_epoch.unwrap_or(record.version);
                let mapping = record.map_file(file_version, &physical, &self.storage.abs(rel))?;
                let all = vec![true; physical.len()];
                record
                    .project_table(&mapping, file_table, &all, file_table.name.as_str())?
                    .0
            }
            None => file_table.clone(),
        };
        let indices: Vec<usize> = match positions {
            None => (0..file_table.num_rows()).collect(),
            Some(positions) => {
                let mut out = Vec::with_capacity(positions.len());
                for pos in positions {
                    if *pos >= file_table.num_rows() as u64 {
                        return Err(LakeError::Corrupt {
                            path: self.storage.abs(rel),
                            message: format!(
                                "table \"{table}\": delete position {pos} is out of bounds for \
                                 \"{rel}\" ({} rows)",
                                file_table.num_rows()
                            ),
                        });
                    }
                    out.push(*pos as usize);
                }
                out
            }
        };
        let mut rows_out = Vec::with_capacity(indices.len());
        for row in indices {
            let mut values = Vec::with_capacity(file_table.num_cols());
            for col in 0..file_table.num_cols() {
                values.push(file_table.get(row, col).unwrap_or(Value::Null));
            }
            rows_out.push(values);
        }
        Ok(rows_out)
    }

    /// Whether `table` existed at the version before `version`: read the
    /// predecessor manifest. Version 1 has no predecessor (nothing existed).
    fn table_existed_before(&self, table: &str, version: u64) -> Result<bool> {
        if version <= 1 {
            return Ok(false);
        }
        match crate::manifest_for(&*self.storage, &self.root, Some(version - 1)) {
            Ok((manifest, _)) => Ok(manifest.tables.contains_key(table)),
            Err(LakeError::VersionNotFound { .. }) | Err(LakeError::Io { .. }) => {
                Err(LakeError::Corrupt {
                    path: self.storage.abs(&format!("snapshots/{}.json", version - 1)),
                    message: format!(
                        "version {}'s predecessor is missing (expired or cleaned); change \
                         data over a range that includes a full save needs it",
                        version
                    ),
                })
            }
            Err(e) => Err(e),
        }
    }
}

/// The insertion/deletion order inside one version: insertions first
/// (DuckLake's `changes` puts insertions and the update postimage before the
/// deletions).
fn kind_rank(kind: ChangeKind) -> u8 {
    match kind {
        ChangeKind::Insert => 0,
        ChangeKind::Delete => 1,
    }
}

/// Every path a commit delta names must be a plain lake-relative `data/…`
/// path, exactly like the main manifest's `files` list. A tampered
/// `commits/<v>.json` must fail here, before any `abs`/read, so an absolute
/// path or `..` can never reach another location on disk.
fn validate_commit_paths(manifest: &CommitManifest) -> Result<()> {
    for delta in manifest.tables.values() {
        for file in &delta.data_files {
            validate_rel_path(file.path())?;
        }
        for delete in &delta.delete_files {
            validate_rel_path(&delete.path)?;
            validate_rel_path(&delete.file)?;
        }
        for rel in &delta.compacted_files {
            validate_rel_path(rel)?;
        }
        for rel in &delta.removed_delete_files {
            validate_rel_path(rel)?;
        }
    }
    Ok(())
}

/// Read one `commits/<v>.json`; `None` when the object does not exist.
fn read_commit_manifest(
    storage: &dyn Storage,
    _root: &Path,
    version: u64,
) -> Result<Option<CommitManifest>> {
    let rel = format!("commits/{version}.json");
    let path = storage.abs(&rel);
    let bytes = match storage.read(&rel, &format!("reading {}", path.display())) {
        Ok(bytes) => bytes,
        Err(LakeError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let manifest: CommitManifest =
        serde_json::from_slice(&bytes).map_err(|e| LakeError::Corrupt {
            path: path.clone(),
            message: e.to_string(),
        })?;
    if manifest.version != version {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "commit manifest declares version {}, but its name says {version}",
                manifest.version
            ),
        });
    }
    validate_commit_paths(&manifest)?;
    Ok(Some(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{FileRef, TableDelta};
    use std::collections::BTreeMap;

    fn manifest_with_data_file(path: &str) -> CommitManifest {
        let mut tables = BTreeMap::new();
        tables.insert(
            "main.t".to_string(),
            TableDelta {
                data_files: vec![FileRef::Path(path.to_string())],
                rows_added: 1,
                ..TableDelta::default()
            },
        );
        CommitManifest {
            format_version: 5,
            version: 7,
            base_version: 6,
            updated: "2026-10-19T00:00:00Z".to_string(),
            tables,
        }
    }

    #[test]
    fn commit_delta_paths_are_validated_before_any_read() {
        assert!(validate_commit_paths(&manifest_with_data_file("data/main.t/x.pink")).is_ok());
        for hostile in ["../escape", "/etc/x", "data/../../escape"] {
            let err = validate_commit_paths(&manifest_with_data_file(hostile)).unwrap_err();
            assert!(
                err.to_string()
                    .contains("data file path must be relative and under data/"),
                "{hostile}: {err}"
            );
        }
    }

    #[test]
    fn commit_delta_delete_file_paths_are_validated_too() {
        let mut tables = BTreeMap::new();
        tables.insert(
            "main.t".to_string(),
            TableDelta {
                delete_files: vec![DeleteRef {
                    path: "data/main.t/x.pinkdel".to_string(),
                    file: "../escape".to_string(),
                    count: 1,
                }],
                rows_deleted: 1,
                ..TableDelta::default()
            },
        );
        let manifest = CommitManifest {
            format_version: 5,
            version: 7,
            base_version: 6,
            updated: "2026-10-19T00:00:00Z".to_string(),
            tables,
        };
        let err = validate_commit_paths(&manifest).unwrap_err();
        assert!(
            err.to_string()
                .contains("data file path must be relative and under data/"),
            "{err}"
        );
    }
}
