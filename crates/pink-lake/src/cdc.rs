//! Change-data reads (wave 68): a version range's row-level changes, the
//! pinkdb counterpart of DuckLake's `ducklake_table_insertions` /
//! `ducklake_table_deletions` / `ducklake_table_changes` table functions
//! (the library surface behind those functions in [`crate::functions`] and the
//! CLI's `.lake changes`).
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
//! A row version is **inserted** at the version that created it and
//! **deleted** at the version that ended it. An update keeps the row id, so
//! `ducklake_table_changes` reports its delete and insert at one snapshot as
//! `update_preimage` / `update_postimage`; a plain delete or insert keeps
//! `delete` / `insert`.
//!
//! # Where the rows come from
//!
//! The derivation is DuckLake's own, read from the catalog rather than from
//! pinkdb's commit history, so it answers the same on a lake any DuckLake
//! writer produced: `GetTableInsertions` / `GetTableDeletions` in
//! `ducklake_metadata_manager.cpp`, the scan-side change logic in
//! `ducklake_delete_filter.cpp` (`DuckLakeDeleteFilter::Initialize`), and the
//! `ducklake_table_changes` macro in `ducklake_table_changes.cpp`.
//!
//! - **Insertions**: data files whose `begin_snapshot` is in the range, plus
//!   inlined rows whose `begin_snapshot` is in it. A file that merged rows of
//!   several snapshots (`ducklake_data_file.partial_max`) carries each row's
//!   snapshot id in `_ducklake_internal_snapshot_id`; rows whose id is out of
//!   the range are skipped. Later deletes do not reduce insertions (DuckLake's
//!   insertion query joins no delete file), so a row inserted *and* deleted
//!   inside the range appears once as an insertion and once as a deletion.
//! - **Deletions**, DuckLake's three flavors: positions of delete files the
//!   catalog lists at `end` — each position's snapshot comes from the file's
//!   `_ducklake_internal_snapshot_id` when it has one, else from the delete
//!   file's `begin_snapshot`, and positions dated before the range (a
//!   cumulative delete file's earlier deletes) are not re-reported; every row
//!   of a data file whose `end_snapshot` is in the range (minus rows deleted
//!   before the drop); and per-position inlined deletes from
//!   `ducklake_inlined_delete_<table_id>`, which apply only to files with no
//!   delete file and whose lifetime lies outside the range. An inlined row's
//!   `end_snapshot` in the range is a deletion too — a flush closes the flushed
//!   rows that way, so a flush reads as a deletion of each inlined row plus an
//!   insertion of the file that carries it, exactly what a DuckLake reader gets
//!   from the same catalog.
//! - **Values** are read from the (immutable) data file and projected through
//!   the read version's schema record, exactly like a scan.
//!
//! # Row ids
//!
//! `row_id` is the table's per-row identity: an inlined record's `row_id`; a
//! file row's `_ducklake_internal_row_id` when the file carries one (DuckLake's
//! flush writes it), else `file.row_id_start + position`. A file published
//! before row ids were recorded (a full save's predecessor, a maintenance
//! output) reads `row_id = None`; the values stay exact. Pairing an update
//! needs the id, so a file with no recorded range reports plain
//! `insert`/`delete`.
//!
//! # Expired history
//!
//! The reads depend on the retained catalog history: a data or delete file
//! `expire_snapshots` removed makes the read a clean error naming the object,
//! never a silently partial answer.

use std::collections::{BTreeMap, BTreeSet};

use pinkdb::{DataTable, Value};

use crate::catalog::{InlinedRow, TableEntry};
use crate::schema::TableSchema;
use crate::{Lake, LakeError, Result, only_table, split_key, validate_rel_path};

/// The hidden column DuckLake's flush-written files carry each row's
/// snapshot in (`ducklake_insert.cpp`'s `WriteSnapshotId`).
const SNAPSHOT_COLUMN: &str = "_ducklake_internal_snapshot_id";
/// The hidden column those files carry each row's row id in.
const ROW_ID_COLUMN: &str = "_ducklake_internal_row_id";

/// A DuckLake snapshot id as a pinkdb version: they differ by one
/// (`metadata::pink_version`), and the ids read from files are non-negative.
fn change_version(snapshot_id: u64) -> u64 {
    snapshot_id.saturating_add(1)
}

/// What one [`ChangeRow`] is: the row version was added (`Insert`) or ended
/// (`Delete`) at its version, and whether it is one half of an update. The
/// spellings match DuckLake's `change_type` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    /// The version added the row version (`ducklake_table_changes`'s
    /// `insert`).
    Insert,
    /// The version ended the row version with no replacement in the same
    /// snapshot (`ducklake_table_changes`'s `delete`).
    Delete,
    /// The version replaced the row version: the deletion half of an update
    /// (`ducklake_table_changes`'s `update_preimage`).
    UpdatePreimage,
    /// The version replaced the row version: the insertion half of an update
    /// (`ducklake_table_changes`'s `update_postimage`).
    UpdatePostimage,
}

impl ChangeKind {
    /// The DuckLake `change_type` spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeKind::Insert => "insert",
            ChangeKind::Delete => "delete",
            ChangeKind::UpdatePreimage => "update_preimage",
            ChangeKind::UpdatePostimage => "update_postimage",
        }
    }

    /// Whether the row version was added (`insert` or an update's postimage),
    /// i.e. a `ducklake_table_insertions` row.
    pub fn is_insertion(&self) -> bool {
        matches!(self, ChangeKind::Insert | ChangeKind::UpdatePostimage)
    }

    /// Whether the row version was ended (`delete` or an update's preimage),
    /// i.e. a `ducklake_table_deletions` row.
    pub fn is_deletion(&self) -> bool {
        matches!(self, ChangeKind::Delete | ChangeKind::UpdatePreimage)
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

/// A data file a change read must open, with the catalog facts the change
/// queries read for it.
#[derive(Debug, Clone)]
pub(crate) struct ChangeFile {
    /// Lake-relative path.
    pub(crate) rel: String,
    /// `ducklake_data_file.record_count`.
    pub(crate) rows: u64,
    /// `ducklake_data_file.row_id_start`.
    pub(crate) row_id_start: Option<u64>,
    /// The schema record the file was written with (`pinkdb_data_file`, else
    /// the schema version whose `begin_snapshot` covers it).
    pub(crate) schema_version: Option<u64>,
    /// `ducklake_data_file.partial_max`: the file merges rows from several
    /// snapshots and carries their ids in the hidden columns above.
    pub(crate) partial: Option<u64>,
    /// The version this fact is about: the file's `begin_snapshot` when it
    /// holds insertions, its `end_snapshot` when it was dropped whole.
    pub(crate) snapshot: u64,
}

/// One delete file the catalog lists, and the data file it deletes from.
#[derive(Debug, Clone)]
pub(crate) struct ChangeDeleteFile {
    /// Lake-relative path of the delete file.
    pub(crate) path: String,
    /// The delete file's format id (the provider that reads it).
    pub(crate) format: String,
    /// `ducklake_delete_file.begin_snapshot`.
    pub(crate) snapshot: u64,
    /// `ducklake_delete_file.partial_max`: each position's snapshot is in the
    /// file's `_ducklake_internal_snapshot_id` column.
    pub(crate) partial: Option<u64>,
    /// `ducklake_delete_file.delete_count`.
    pub(crate) count: u64,
    /// The file the positions apply to.
    pub(crate) target: ChangeFile,
}

/// `ducklake_inlined_delete_<table_id>` positions, grouped by target file.
#[derive(Debug, Clone)]
pub(crate) struct ChangeInlinedDeletes {
    /// The file the positions apply to.
    pub(crate) target: ChangeFile,
    /// `(physical position, deleting snapshot)`, already inside the range.
    pub(crate) positions: Vec<(u64, u64)>,
}

/// The catalog state DuckLake's three change queries read for one table over
/// an inclusive version range. See the module docs for how each list is used.
#[derive(Debug, Default)]
pub(crate) struct ChangeFacts {
    /// Data files with rows inserted in the range (`begin_snapshot` in it, or
    /// a `partial_max` that reaches it).
    pub(crate) inserted_files: Vec<ChangeFile>,
    /// Data files dropped whole in the range (`end_snapshot` in it).
    pub(crate) dropped_files: Vec<ChangeFile>,
    /// Every delete file the catalog lists at `end`, including ones written
    /// before `start`: their positions carry the snapshot that deleted the
    /// row, and the earlier ones cancel positions a cumulative file repeats.
    pub(crate) delete_files: Vec<ChangeDeleteFile>,
    /// Inlined rows whose `begin_snapshot` is in the range.
    pub(crate) inserted_rows: Vec<InlinedRow>,
    /// Inlined rows whose `end_snapshot` is in the range.
    pub(crate) deleted_rows: Vec<InlinedRow>,
    /// Per-position inlined deletes whose `begin_snapshot` is in the range.
    pub(crate) inlined_deletes: Vec<ChangeInlinedDeletes>,
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
        let (_, _) = split_key(table, self.catalog_path())?;
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
        let facts =
            self.store
                .change_facts(table, start, end)?
                .ok_or_else(|| LakeError::Corrupt {
                    path: manifest_path.clone(),
                    message: format!("table \"{table}\" does not exist at version {end}"),
                })?;
        validate_change_paths(&facts)?;
        let mut reader = ChangeReader {
            lake: self,
            table,
            entry,
            record: record.as_ref(),
            files: BTreeMap::new(),
        };
        let mut inserted: Vec<ChangeRow> = Vec::new();
        let mut deleted: Vec<ChangeRow> = Vec::new();
        if insertions {
            for file in &facts.inserted_files {
                let read = reader.file(file)?;
                for (pos, values) in read.values.iter().enumerate() {
                    let snapshot = read.snapshot(pos, file.snapshot);
                    if snapshot < start || snapshot > end {
                        continue;
                    }
                    inserted.push(ChangeRow {
                        version: snapshot,
                        row_id: read.row_id(pos, file.row_id_start),
                        change_type: ChangeKind::Insert,
                        values: values.clone(),
                    });
                }
            }
            for row in &facts.inserted_rows {
                inserted.push(ChangeRow {
                    version: row.begin,
                    row_id: Some(row.row_id),
                    change_type: ChangeKind::Insert,
                    values: crate::inlined::decode_row(
                        required_record(record.as_ref(), table, &manifest_path)?,
                        row,
                        table,
                        &manifest_path,
                    )?,
                });
            }
        }
        if deletions {
            // Flavor 1: positions of the delete files the catalog lists. A
            // delete file written before the range still lists positions the
            // range must not report, so each position carries its own
            // snapshot; a file that does not record them holds deletes of one
            // snapshot (its `begin_snapshot`) and repeats any earlier file's
            // positions, which are removed here.
            for delete in &facts.delete_files {
                let positions = self.read_delete_positions(delete)?;
                let earlier: BTreeSet<u64> = if delete.partial.is_some() {
                    BTreeSet::new()
                } else {
                    self.earlier_delete_positions(
                        &facts.delete_files,
                        &delete.target,
                        delete.snapshot,
                    )?
                };
                let read = reader.file(&delete.target)?;
                for (pos, recorded) in positions {
                    let snapshot = recorded.unwrap_or(delete.snapshot);
                    if snapshot < start || snapshot > end || earlier.contains(&pos) {
                        continue;
                    }
                    deleted.push(ChangeRow {
                        version: snapshot,
                        row_id: read.row_id(pos as usize, delete.target.row_id_start),
                        change_type: ChangeKind::Delete,
                        values: read.row_at(pos, table, &delete.target)?,
                    });
                }
            }
            // Flavor 2: a file dropped whole — every row still live at the
            // drop is a deletion then (`previous_delete` in DuckLake's query:
            // a delete file written before the drop already reported its
            // positions).
            for file in &facts.dropped_files {
                let earlier =
                    self.earlier_delete_positions(&facts.delete_files, file, file.snapshot)?;
                let read = reader.file(file)?;
                for pos in 0..read.values.len() {
                    if earlier.contains(&(pos as u64)) {
                        continue;
                    }
                    deleted.push(ChangeRow {
                        version: file.snapshot,
                        row_id: read.row_id(pos, file.row_id_start),
                        change_type: ChangeKind::Delete,
                        values: read.row_at(pos as u64, table, file)?,
                    });
                }
            }
            // Flavor 3: positions DuckLake keeps inline instead of in a delete
            // file (only for files with no delete file, and each position
            // carries its own snapshot).
            for group in &facts.inlined_deletes {
                let read = reader.file(&group.target)?;
                for (pos, snapshot) in &group.positions {
                    deleted.push(ChangeRow {
                        version: *snapshot,
                        row_id: read.row_id(*pos as usize, group.target.row_id_start),
                        change_type: ChangeKind::Delete,
                        values: read.row_at(*pos, table, &group.target)?,
                    });
                }
            }
            // Inlined rows the range ended.
            for row in &facts.deleted_rows {
                let Some(end_version) = row.end else {
                    continue;
                };
                deleted.push(ChangeRow {
                    version: end_version,
                    row_id: Some(row.row_id),
                    change_type: ChangeKind::Delete,
                    values: crate::inlined::decode_row(
                        required_record(record.as_ref(), table, &manifest_path)?,
                        row,
                        table,
                        &manifest_path,
                    )?,
                });
            }
        }
        if insertions && deletions {
            pair_updates(&mut inserted, &mut deleted);
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

    /// The `(position, snapshot)` pairs one delete file lists. A file
    /// DuckLake rewrote for several snapshots names each position's snapshot
    /// in its third column; the format provider decodes it like any other
    /// table, so the pairs come straight from the bytes.
    fn read_delete_positions(&self, delete: &ChangeDeleteFile) -> Result<Vec<(u64, Option<u64>)>> {
        let path = self.storage.abs(&delete.path);
        let bytes = self
            .storage
            .read(&delete.path, &format!("reading {}", path.display()))?;
        if delete.partial.is_none() {
            let positions = crate::delete_file::read_positions(
                &bytes,
                &delete.format,
                &delete.target.rel,
                &path,
            )?;
            check_delete_count(delete, positions.len())?;
            return Ok(positions.into_iter().map(|pos| (pos, None)).collect());
        }
        let provider = crate::format::format_for(&delete.format, &path)?;
        let file =
            crate::format::DataFile::new(&path, &delete.path, std::sync::Arc::clone(&self.storage));
        let mini = provider.read(&file)?;
        let (_, decoded) = only_table(&mini, &delete.path, &self.root)?;
        let physical = decoded.schema();
        let column = |name: &str| physical.iter().position(|(column, _)| column == name);
        let (Some(file_path), Some(pos)) = (column("file_path"), column("pos")) else {
            return Err(LakeError::Corrupt {
                path,
                message: "a parquet delete file needs (file_path, pos)".to_string(),
            });
        };
        let snapshots = column(SNAPSHOT_COLUMN);
        let wanted = delete
            .target
            .rel
            .rsplit('/')
            .next()
            .unwrap_or(&delete.target.rel);
        let mut out = Vec::new();
        for row in 0..decoded.num_rows() {
            let names_target = match decoded.get(row, file_path) {
                Some(Value::Varchar(file)) => {
                    file == delete.target.rel
                        || file.ends_with(&format!("/{}", delete.target.rel))
                        || file.ends_with(&format!("/{wanted}"))
                        || file == wanted
                }
                other => {
                    return Err(LakeError::Corrupt {
                        path,
                        message: format!("parquet delete file_path is {other:?}, not VARCHAR"),
                    });
                }
            };
            if !names_target {
                continue;
            }
            let Some(position) = unsigned(&decoded.get(row, pos)) else {
                return Err(LakeError::Corrupt {
                    path,
                    message: "parquet delete pos is not a non-negative integer".to_string(),
                });
            };
            let snapshot = snapshots
                .and_then(|index| unsigned(&decoded.get(row, index)))
                .map(change_version);
            out.push((position, snapshot));
        }
        check_delete_count(delete, out.len())?;
        Ok(out)
    }

    /// Positions the delete files for `target` wrote before `snapshot`: a
    /// delete file that does not record per-position snapshots repeats the
    /// earlier deletes, so DuckLake subtracts them (`previous_delete_file` in
    /// `DuckLakeDeleteFilter::Initialize`).
    fn earlier_delete_positions(
        &self,
        deletes: &[ChangeDeleteFile],
        target: &ChangeFile,
        snapshot: u64,
    ) -> Result<BTreeSet<u64>> {
        let mut out = BTreeSet::new();
        for earlier in deletes {
            if earlier.target.rel != target.rel
                || earlier.snapshot >= snapshot
                || earlier.partial.is_some()
            {
                continue;
            }
            for (pos, _) in self.read_delete_positions(earlier)? {
                out.insert(pos);
            }
        }
        Ok(out)
    }
}

/// One data file's rows plus the hidden columns a change read addresses them
/// by.
struct FileChange {
    values: Vec<Vec<Value>>,
    /// `_ducklake_internal_snapshot_id` per row (only on a partial file).
    snapshots: Option<Vec<Option<u64>>>,
    /// `_ducklake_internal_row_id` per row (only on a flush-written file).
    row_ids: Option<Vec<Option<u64>>>,
}

impl FileChange {
    /// The snapshot that created row `pos`, falling back to `fallback` when
    /// the file records no ids.
    fn snapshot(&self, pos: usize, fallback: u64) -> u64 {
        self.snapshots
            .as_ref()
            .and_then(|ids| ids.get(pos).copied().flatten())
            .unwrap_or(fallback)
    }

    /// The row id of row `pos`: the file's embedded id, else
    /// `row_id_start + position`, else none.
    fn row_id(&self, pos: usize, start: Option<u64>) -> Option<u64> {
        self.row_ids
            .as_ref()
            .and_then(|ids| ids.get(pos).copied().flatten())
            .or_else(|| start.map(|start| start.saturating_add(pos as u64)))
    }

    /// The values of `pos`, checked against the decoded file.
    fn row_at(&self, pos: u64, table: &str, file: &ChangeFile) -> Result<Vec<Value>> {
        self.values
            .get(pos as usize)
            .cloned()
            .ok_or_else(|| LakeError::Corrupt {
                path: std::path::PathBuf::from(&file.rel),
                message: format!(
                    "table \"{table}\": delete position {pos} is out of bounds for \"{}\" \
                     ({} rows)",
                    file.rel,
                    self.values.len()
                ),
            })
    }
}

/// Reads (and caches) the data files one change read touches: a delete file
/// and the file it deletes from name the same target, and a dropped file is
/// read once for its values and once for its row count, so a cache keeps a
/// wide range from decoding a file once per delete.
struct ChangeReader<'a> {
    lake: &'a Lake,
    table: &'a str,
    entry: &'a TableEntry,
    record: Option<&'a TableSchema>,
    files: BTreeMap<String, FileChange>,
}

impl ChangeReader<'_> {
    fn file(&mut self, file: &ChangeFile) -> Result<&FileChange> {
        if !self.files.contains_key(&file.rel) {
            let read = self
                .lake
                .read_change_file(self.table, self.entry, self.record, file)?;
            self.files.insert(file.rel.clone(), read);
        }
        Ok(&self.files[&file.rel])
    }
}

impl Lake {
    /// Decode one data file a change read names, projected onto `record` (or
    /// as written, for a legacy entry), plus its hidden snapshot/row-id
    /// columns. The file's own metadata is the authority: it may have been
    /// dropped or compacted away by a later version, so membership in the
    /// read version's file list is not required, but its identity and row
    /// count are checked against the bytes.
    fn read_change_file(
        &self,
        table: &str,
        entry: &TableEntry,
        record: Option<&TableSchema>,
        file: &ChangeFile,
    ) -> Result<FileChange> {
        let (dbname, table_name) = split_key(table, self.catalog_path())?;
        let mini = self.open_data_file(&file.rel, entry.format())?;
        let (file_db, decoded) = only_table(&mini, &file.rel, &self.root)?;
        if !file_db.eq_ignore_ascii_case(dbname) || !decoded.name.eq_ignore_ascii_case(table_name) {
            return Err(LakeError::Corrupt {
                path: self.storage.abs(&file.rel),
                message: format!(
                    "data file holds {file_db}.{}, but the catalog says \"{table}\"",
                    decoded.name
                ),
            });
        }
        let rows = decoded.num_rows() as u64;
        if file.rows != rows {
            return Err(LakeError::Corrupt {
                path: self.catalog_path().to_path_buf(),
                message: format!(
                    "table \"{table}\": the catalog says \"{}\" holds {} rows, but the file \
                     holds {rows}",
                    file.rel, file.rows
                ),
            });
        }
        let physical = decoded.schema();
        // The hidden snapshot column holds DuckLake snapshot ids; change rows
        // are reported by pinkdb version.
        let hidden = |name: &str, version: bool| -> Option<Vec<Option<u64>>> {
            let index = physical.iter().position(|(column, _)| column == name)?;
            Some(
                (0..decoded.num_rows())
                    .map(|row| {
                        unsigned(&decoded.get(row, index))
                            .map(|id| if version { change_version(id) } else { id })
                    })
                    .collect(),
            )
        };
        let snapshots = hidden(SNAPSHOT_COLUMN, true);
        if file.partial.is_some() && snapshots.is_none() {
            // DuckLake's own readers refuse a partial file that cannot be
            // filtered by snapshot: without the ids every row would read as a
            // row of the file's first snapshot.
            return Err(LakeError::Corrupt {
                path: self.storage.abs(&file.rel),
                message: format!(
                    "the catalog says \"{}\" merges rows of several snapshots, but the file \
                     carries no {SNAPSHOT_COLUMN} column",
                    file.rel
                ),
            });
        }
        let row_ids = hidden(ROW_ID_COLUMN, false);
        let projected: DataTable = match record {
            Some(record) => {
                let file_version = file.schema_version.unwrap_or(record.version);
                let mapping =
                    record.map_file(file_version, &physical, &self.storage.abs(&file.rel))?;
                let all = vec![true; physical.len()];
                record
                    .project_table(&mapping, decoded, &all, decoded.name.as_str())?
                    .0
            }
            None => decoded.clone(),
        };
        let mut values = Vec::with_capacity(projected.num_rows());
        for row in 0..projected.num_rows() {
            let mut row_values = Vec::with_capacity(projected.num_cols());
            for col in 0..projected.num_cols() {
                row_values.push(projected.get(row, col).unwrap_or(Value::Null));
            }
            values.push(row_values);
        }
        Ok(FileChange {
            values,
            snapshots,
            row_ids,
        })
    }
}

/// Mark the insertion and deletion halves of an update: DuckLake's `changes`
/// macro pairs rows that share `(snapshot_id, rowid)`, so an update (one
/// snapshot, one row id, two row versions) reads as
/// `update_postimage`/`update_preimage`.
fn pair_updates(inserted: &mut [ChangeRow], deleted: &mut [ChangeRow]) {
    let ids = |rows: &[ChangeRow]| -> BTreeSet<(u64, u64)> {
        rows.iter()
            .filter_map(|row| row.row_id.map(|id| (row.version, id)))
            .collect()
    };
    let (inserted_ids, deleted_ids) = (ids(inserted), ids(deleted));
    for row in inserted.iter_mut() {
        if row
            .row_id
            .is_some_and(|id| deleted_ids.contains(&(row.version, id)))
        {
            row.change_type = ChangeKind::UpdatePostimage;
        }
    }
    for row in deleted.iter_mut() {
        if row
            .row_id
            .is_some_and(|id| inserted_ids.contains(&(row.version, id)))
        {
            row.change_type = ChangeKind::UpdatePreimage;
        }
    }
}

/// The insertion/deletion order inside one version: insertions first
/// (DuckLake's `changes` puts insertions and the update postimage before the
/// deletions).
fn kind_rank(kind: ChangeKind) -> u8 {
    match kind {
        ChangeKind::Insert | ChangeKind::UpdatePostimage => 0,
        ChangeKind::Delete | ChangeKind::UpdatePreimage => 1,
    }
}

/// A positive integer stored in one of the value types the hidden columns and
/// delete-file positions use.
fn unsigned(value: &Option<Value>) -> Option<u64> {
    match value {
        Some(Value::Bigint(v)) if *v >= 0 => Some(*v as u64),
        Some(Value::Integer(v)) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}

fn required_record<'a>(
    record: Option<&'a TableSchema>,
    table: &str,
    path: &std::path::Path,
) -> Result<&'a TableSchema> {
    record.ok_or_else(|| LakeError::Corrupt {
        path: path.to_path_buf(),
        message: format!("table \"{table}\" carries inlined rows but no schema record"),
    })
}

/// A delete file's `delete_count` counts every position in it; one that
/// merges deletes for several data files holds fewer for one target, but never
/// more than the catalog says.
fn check_delete_count(delete: &ChangeDeleteFile, positions: usize) -> Result<()> {
    if positions as u64 > delete.count {
        return Err(LakeError::Corrupt {
            path: std::path::PathBuf::from(&delete.path),
            message: format!(
                "delete file declares {} positions, holds {positions}",
                delete.count
            ),
        });
    }
    Ok(())
}

/// Every path the catalog's change facts name must be a plain lake-relative
/// `data/…` path, exactly like the main manifest's `files` list. A tampered
/// catalog must fail here, before any `abs`/read, so an absolute path or `..`
/// can never reach another location on disk.
fn validate_change_paths(facts: &ChangeFacts) -> Result<()> {
    for file in facts.inserted_files.iter().chain(&facts.dropped_files) {
        validate_rel_path(&file.rel)?;
    }
    for delete in &facts.delete_files {
        validate_rel_path(&delete.path)?;
        validate_rel_path(&delete.target.rel)?;
    }
    for group in &facts.inlined_deletes {
        validate_rel_path(&group.target.rel)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts_with(path: &str) -> ChangeFacts {
        ChangeFacts {
            inserted_files: vec![ChangeFile {
                rel: path.to_string(),
                rows: 1,
                row_id_start: Some(0),
                schema_version: None,
                partial: None,
                snapshot: 2,
            }],
            ..ChangeFacts::default()
        }
    }

    #[test]
    fn change_fact_paths_are_validated_before_any_read() {
        assert!(validate_change_paths(&facts_with("data/main.t/x.pink")).is_ok());
        for hostile in ["../escape", "/etc/x", "data/../../escape"] {
            let err = validate_change_paths(&facts_with(hostile)).unwrap_err();
            assert!(
                err.to_string().contains("data file path must be relative"),
                "{hostile}: {err}"
            );
        }
    }

    #[test]
    fn change_fact_delete_and_target_paths_are_validated_too() {
        let mut facts = ChangeFacts::default();
        facts.delete_files.push(ChangeDeleteFile {
            path: "data/main.t/x.pinkdel".to_string(),
            format: "pink".to_string(),
            snapshot: 3,
            partial: None,
            count: 1,
            target: ChangeFile {
                rel: "../escape".to_string(),
                rows: 1,
                row_id_start: None,
                schema_version: None,
                partial: None,
                snapshot: 2,
            },
        });
        let err = validate_change_paths(&facts).unwrap_err();
        assert!(
            err.to_string().contains("data file path must be relative"),
            "{err}"
        );
    }

    #[test]
    fn updates_are_paired_by_snapshot_and_row_id() {
        let row = |version: u64, id: Option<u64>, kind: ChangeKind| ChangeRow {
            version,
            row_id: id,
            change_type: kind,
            values: Vec::new(),
        };
        let mut deleted = vec![
            row(4, Some(1), ChangeKind::Delete),
            row(5, Some(2), ChangeKind::Delete),
        ];
        let mut inserted = vec![
            row(4, Some(1), ChangeKind::Insert),
            row(5, Some(3), ChangeKind::Insert),
        ];
        pair_updates(&mut inserted, &mut deleted);
        assert_eq!(inserted[0].change_type, ChangeKind::UpdatePostimage);
        assert_eq!(deleted[0].change_type, ChangeKind::UpdatePreimage);
        assert_eq!(inserted[1].change_type, ChangeKind::Insert);
        assert_eq!(deleted[1].change_type, ChangeKind::Delete);
        // An insertion and a deletion in different snapshots stay separate.
        let mut inserted = vec![row(4, Some(2), ChangeKind::Insert)];
        let mut deleted = vec![row(5, Some(2), ChangeKind::Delete)];
        pair_updates(&mut inserted, &mut deleted);
        assert_eq!(inserted[0].change_type, ChangeKind::Insert);
        assert_eq!(deleted[0].change_type, ChangeKind::Delete);
        // A row without an id cannot be paired (NULL never equals NULL).
        let mut inserted = vec![row(4, None, ChangeKind::Insert)];
        let mut deleted = vec![row(4, None, ChangeKind::Delete)];
        pair_updates(&mut inserted, &mut deleted);
        assert_eq!(inserted[0].change_type, ChangeKind::Insert);
        assert_eq!(deleted[0].change_type, ChangeKind::Delete);
    }
}
