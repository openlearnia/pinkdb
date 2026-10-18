//! Table schema records (catalog `format_version` 5, wave 65): the logical
//! schema of one table at one version, and the mapping that keeps old data
//! files readable after `ALTER TABLE`.
//!
//! # The record
//!
//! A v5 [`TableEntry`](crate::catalog::TableEntry) carries
//!
//! - `schema_version`: the table's schema **epoch**, starting at 1 when the
//!   entry is created and incremented by one per published schema change
//!   (add / drop / rename column);
//! - `columns`: every live column, in scan order, with its type, the DEFAULT
//!   an old file's missing value materializes as, and `added` — the epoch at
//!   which the column appeared;
//! - `renames`: for each rename, the epoch it produced and the `from`/`to`
//!   names (same column identity, so its `added` never changes);
//! - every data file carries `schema_version`: the epoch it was written
//!   under. Files written before this wave (and files of a legacy entry) do
//!   not carry one.
//!
//! Each manifest embeds the record as of its own version, so time travel
//! (`AT (VERSION => n)`) reads the schema that version had — the same way
//! DuckLake's per-snapshot `schema_version` resolves column names through the
//! `ducklake_column` history. `catalog.schema_version` is the lake-wide
//! counter `.lake snapshots` prints (DuckLake's `snapshot.schema_version`).
//!
//! # Reading an old file
//!
//! A file written at epoch `f` holds the column names *as they were at `f`*
//! and does not hold columns that appeared later. To read it against the
//! entry's current record, each logical column resolves to its physical name
//! like this ([`TableSchema::physical_name`]):
//!
//! 1. `added > f` — the column did not exist yet: the file contributes the
//!    column's DEFAULT for every row (metadata-only `ADD COLUMN`, DuckLake's
//!    `initial_default` rule). A dropped-and-re-added column has a fresh
//!    identity whose `added` is after the file, so old bytes are never
//!    mis-read even when the name matches.
//! 2. otherwise walk the rename log backwards (newest first): a rename with
//!    `version > f` still post-dates the file, so its `to` name is replaced
//!    by its `from`; walking stops at the first rename at or before `f`.
//!
//! A `DROP COLUMN` publishes a new record without the column: old files keep
//! its bytes (positions are unchanged, so positional delete files stay
//! valid), the reader simply never asks for them. Unmapped physical columns
//! are ignored; a mapped one whose type disagrees with the record is a
//! [`LakeError::Corrupt`] — type changes are not part of this wave.

use std::path::{Path, PathBuf};

use pinkdb::{DataType, Value};

use crate::catalog::{ColumnEntry, RenameEntry, TableEntry};
use crate::stats::decode_value;
use crate::{LakeError, Result};

/// One live column, resolved: name, type, default (decoded) and the epoch it
/// appeared.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SchemaColumn {
    pub(crate) name: String,
    pub(crate) dtype: DataType,
    /// `None` = NULL (no DEFAULT, or `DEFAULT NULL`).
    pub(crate) default: Option<Value>,
    /// The table schema epoch at which the column appeared.
    pub(crate) added: u64,
}

impl SchemaColumn {
    /// Build a record entry for this column.
    pub(crate) fn entry(&self) -> Result<ColumnEntry> {
        ColumnEntry::new(&self.name, self.dtype, self.default.as_ref(), self.added)
    }
}

/// One rename: at epoch `version`, column identity `from` became `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rename {
    pub(crate) version: u64,
    pub(crate) from: String,
    pub(crate) to: String,
}

/// A table's recorded logical schema at one version.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableSchema {
    /// The table's schema epoch at this version.
    pub(crate) version: u64,
    pub(crate) columns: Vec<SchemaColumn>,
    pub(crate) renames: Vec<Rename>,
}

/// How one data file's columns map onto the logical schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileMapping {
    /// Per logical column: the physical index in the file, or `None` when the
    /// file predates the column (its default materializes).
    pub(crate) physical: Vec<Option<usize>>,
}

impl TableSchema {
    /// The initial record (epoch 1) of a table whose schema is `table`'s:
    /// every live column at `added: 1`, no rename history — what a commit
    /// publishes for a table it creates.
    pub(crate) fn initial(table: &pinkdb::DataTable) -> Self {
        Self {
            version: 1,
            columns: table
                .columns
                .iter()
                .map(|c| SchemaColumn {
                    name: c.name.clone(),
                    dtype: c.dtype,
                    default: c.default.clone().filter(|v| !v.is_null()),
                    added: 1,
                })
                .collect(),
            renames: Vec::new(),
        }
    }

    /// Parse a v5 table entry's record. `None` for a legacy entry (no
    /// `columns`); every parse failure is a [`LakeError::Corrupt`] naming the
    /// manifest.
    pub(crate) fn from_entry(
        key: &str,
        entry: &TableEntry,
        manifest_path: &Path,
    ) -> Result<Option<TableSchema>> {
        let Some(columns) = &entry.columns else {
            // A renames log without a columns record is malformed: it can
            // never be resolved.
            if !entry.renames.is_empty() {
                return Err(LakeError::Corrupt {
                    path: manifest_path.to_path_buf(),
                    message: format!(
                        "table \"{key}\" lists renames without a column schema record"
                    ),
                });
            }
            return Ok(None);
        };
        let corrupt = |message: String| LakeError::Corrupt {
            path: manifest_path.to_path_buf(),
            message: format!("table \"{key}\": {message}"),
        };
        if columns.is_empty() {
            return Err(corrupt("the column schema record is empty".to_string()));
        }
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::with_capacity(columns.len());
        for col in columns {
            let dtype = col.dtype().ok_or_else(|| {
                corrupt(format!(
                    "column \"{}\" has the unknown type \"{}\"",
                    col.name, col.dtype
                ))
            })?;
            if !seen.insert(col.name.to_ascii_lowercase()) {
                return Err(corrupt(format!(
                    "column \"{}\" appears twice in the schema record",
                    col.name
                )));
            }
            let default = match &col.default {
                None => None,
                Some(s) => Some(decode_value(dtype, s).ok_or_else(|| {
                    corrupt(format!(
                        "the default \"{s}\" of column \"{}\" does not decode as {dtype:?}",
                        col.name
                    ))
                })?),
            };
            out.push(SchemaColumn {
                name: col.name.clone(),
                dtype,
                default,
                added: col.added,
            });
        }
        let mut renames = Vec::with_capacity(entry.renames.len());
        let mut previous = 0u64;
        for r in &entry.renames {
            if r.version <= previous {
                return Err(corrupt(
                    "the renames log must be ordered by version".to_string(),
                ));
            }
            previous = r.version;
            renames.push(Rename {
                version: r.version,
                from: r.from.clone(),
                to: r.to.clone(),
            });
        }
        Ok(Some(TableSchema {
            version: entry.schema_version,
            columns: out,
            renames,
        }))
    }

    /// The logical `(name, type)` pairs in scan order.
    pub(crate) fn schema_pairs(&self) -> Vec<(String, DataType)> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.dtype))
            .collect()
    }

    /// Serialize the record back into a table entry's fields.
    pub(crate) fn to_entry_fields(&self) -> Result<(u64, Vec<ColumnEntry>, Vec<RenameEntry>)> {
        let columns = self
            .columns
            .iter()
            .map(SchemaColumn::entry)
            .collect::<Result<_>>()?;
        let renames = self
            .renames
            .iter()
            .map(|r| RenameEntry {
                version: r.version,
                from: r.from.clone(),
                to: r.to.clone(),
            })
            .collect();
        Ok((self.version, columns, renames))
    }

    /// The physical name of logical column `index` in a file written at epoch
    /// `file_version`: `None` when the column did not exist yet (its default
    /// materializes for every row).
    pub(crate) fn physical_name(&self, index: usize, file_version: u64) -> Option<&str> {
        let col = self.columns.get(index)?;
        if col.added > file_version {
            return None;
        }
        let mut name = col.name.as_str();
        for r in self.renames.iter().rev() {
            if r.version <= file_version {
                // The rename happened before (or at) the file's write: the
                // walked name already reflects it.
                break;
            }
            if r.to == name {
                name = r.from.as_str();
            }
        }
        Some(name)
    }

    /// Map one data file's schema onto this logical schema, validating every
    /// mapped column's type. Unmapped physical columns (dropped, or simply
    /// absent from the record) are ignored; a mapped name missing from the
    /// file is an internal error (the caller computed the mapping).
    pub(crate) fn map_file(
        &self,
        file_version: u64,
        file_schema: &[(String, DataType)],
        file_path: &Path,
    ) -> Result<FileMapping> {
        let mut physical = Vec::with_capacity(self.columns.len());
        for (index, col) in self.columns.iter().enumerate() {
            let Some(want) = self.physical_name(index, file_version) else {
                physical.push(None);
                continue;
            };
            let found = file_schema
                .iter()
                .position(|(n, _)| n.eq_ignore_ascii_case(want));
            match found {
                Some(i) if file_schema[i].1 == col.dtype => physical.push(Some(i)),
                Some(i) => {
                    return Err(LakeError::Corrupt {
                        path: file_path.to_path_buf(),
                        message: format!(
                            "data file holds column \"{}\" as {:?}, but the table's schema \
                             record says {:?} (type changes are not part of this wave)",
                            want, file_schema[i].1, col.dtype
                        ),
                    })
                }
                None => {
                    return Err(LakeError::Corrupt {
                        path: file_path.to_path_buf(),
                        message: format!(
                            "data file does not hold column \"{want}\", which the table's \
                             schema record declares at schema version {file_version}"
                        ),
                    })
                }
            }
        }
        Ok(FileMapping { physical })
    }

    /// Project a decoded file (full schema, with `present[i]` marking which
    /// of its columns hold decoded values) onto this logical schema: returns
    /// the logical-schema table (chunk-aligned, values shared by `Arc`) plus
    /// the logical `present` flags. Default-filled columns are complete
    /// constants and always count as present — no decode is needed for them.
    pub(crate) fn project_table(
        &self,
        mapping: &FileMapping,
        file_table: &pinkdb::DataTable,
        file_present: &[bool],
        table_name: &str,
    ) -> Result<(pinkdb::DataTable, Vec<bool>)> {
        let num_rows = file_table.num_rows();
        let mut columns: Vec<pinkdb::storage::Column> = Vec::with_capacity(self.columns.len());
        let mut present = Vec::with_capacity(self.columns.len());
        for (index, col) in self.columns.iter().enumerate() {
            match mapping.physical[index] {
                Some(pi) => {
                    let mut c = file_table.columns[pi].clone();
                    // The logical name may differ from the physical one
                    // (rename); the data is shared, never copied. The record
                    // is authoritative for the DEFAULT too.
                    c.name = col.name.clone();
                    c.default = col.default.clone();
                    present.push(file_present.get(pi).copied().unwrap_or(true));
                    columns.push(c);
                }
                None => {
                    columns.push(constant_column(
                        &col.name,
                        col.dtype,
                        col.default.as_ref(),
                        file_table,
                    )?);
                    present.push(true);
                }
            }
        }
        Ok((
            pinkdb::DataTable::from_columns(table_name.to_string(), columns, num_rows),
            present,
        ))
    }

    /// The stats contribution of one *unmapped* (default-filled) logical
    /// column: every surviving row holds the default. A NULL default makes
    /// the column all-NULL; a non-NULL one proves min = max = default.
    pub(crate) fn default_stats(&self, index: usize, surviving_rows: u64) -> pinkdb::ColumnStats {
        match self.columns.get(index).and_then(|c| c.default.as_ref()) {
            Some(v) if !v.is_null() => pinkdb::ColumnStats {
                null_count: Some(0),
                min: Some(v.clone()),
                max: Some(v.clone()),
            },
            _ => pinkdb::ColumnStats {
                null_count: Some(surviving_rows),
                min: None,
                max: None,
            },
        }
    }
}

/// A column filled entirely with `default` (or NULL), chunk-aligned with
/// `file_table` (every chunk gets a constant vector of the same length).
pub(crate) fn constant_column(
    name: &str,
    dtype: DataType,
    default: Option<&Value>,
    file_table: &pinkdb::DataTable,
) -> Result<pinkdb::storage::Column> {
    let mut chunks = Vec::with_capacity(file_table.columns[0].chunks.len());
    for chunk in &file_table.columns[0].chunks {
        let mut v = pinkdb::Vector::with_capacity(dtype, chunk.len());
        for _ in 0..chunk.len() {
            match default {
                Some(d) if !d.is_null() => {
                    v.push_valid(d.clone()).map_err(|e| LakeError::Internal {
                        message: format!("filling default of column \"{name}\": {e}"),
                    })?
                }
                _ => v.push_null().map_err(|e| LakeError::Internal {
                    message: format!("filling NULL default of column \"{name}\": {e}"),
                })?,
            }
        }
        chunks.push(std::sync::Arc::new(v));
    }
    Ok(pinkdb::storage::Column {
        name: name.to_string(),
        dtype,
        default: default.cloned(),
        chunks,
    })
}

/// How each *current* column reads from a base-version row (the write-side
/// twin of [`TableSchema::physical_name`], wave 65's incremental commits).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DiffCol {
    /// The value comes from the base table's column `index` (an exact match
    /// or a rename of the same identity).
    Base(usize),
    /// The column did not exist at the base version: every base row reads
    /// this default (or NULL).
    Added(Option<Value>),
}

/// The planned schema evolution of one table between the version a writable
/// attachment pinned (`base_entry` + `base_table`, its materialized logical
/// content) and the store's current table.
#[derive(Debug, Clone)]
pub(crate) struct SchemaDelta {
    /// The record to publish, when the schema changed (`None` = the base
    /// entry's record, or its absence, stays as it is).
    pub(crate) record: Option<TableSchema>,
    /// Per current column: where a base-version row reads it from.
    pub(crate) view: Vec<DiffCol>,
}

/// Plan the schema delta (wave 65). The matching rules:
///
/// - an exact column (same name, ignoring case, same type) is the same
///   identity;
/// - a name change at the same effective position with the same type is a
///   **rename** (the identity and its `added` epoch survive; old files map
///   their physical name through the rename log);
/// - a same-name type change, a drop and an add of the same shape, or
///   anything else is a **drop + add**: the new column is a fresh identity
///   (old files read its default, DuckLake's `initial_default` rule — even
///   when the physical name matches);
/// - a dropped identity's rename history is pruned.
///
/// The new record's epoch is `base + 1`; added columns, renames and the
/// cleared history are stamped with it. A legacy base entry (no record, or a
/// v4 table) is materialized as epoch 1 with `added: 1` for every column, so
/// its files migrate cleanly.
pub(crate) fn plan_schema_delta(
    key: &str,
    base_entry: &crate::catalog::TableEntry,
    base_table: &pinkdb::DataTable,
    current: &pinkdb::DataTable,
) -> Result<SchemaDelta> {
    let base = match TableSchema::from_entry(key, base_entry, &manifest_path())? {
        Some(record) => record,
        None => TableSchema {
            version: 1,
            columns: base_table
                .columns
                .iter()
                .map(|c| SchemaColumn {
                    name: c.name.clone(),
                    dtype: c.dtype,
                    default: c.default.clone(),
                    added: 1,
                })
                .collect(),
            renames: Vec::new(),
        },
    };
    // Pass 1: exact matches by name (case-insensitive) and type.
    let mut base_taken = vec![false; base.columns.len()];
    let mut matched: Vec<Option<usize>> = vec![None; current.num_cols()];
    for (ci, c) in current.columns.iter().enumerate() {
        if let Some(bi) = base.columns.iter().enumerate().position(|(bi, b)| {
            !base_taken[bi] && b.name.eq_ignore_ascii_case(&c.name) && b.dtype == c.dtype
        }) {
            base_taken[bi] = true;
            matched[ci] = Some(bi);
        }
    }
    // Pass 2: renames. For each unmatched current column, a rename candidate
    // is an unused unmatched base column with the same type sitting at the
    // same effective position (the count of *matched* columns before each
    // agrees) — the k-slot alignment a drop/rename/add sequence preserves.
    // Several candidates at one gap are inherently ambiguous (drop + rename
    // in one commit); the closest to the end of the base list wins (the
    // `DROP a; RENAME b TO c` reading). No candidate = the column is a fresh
    // identity (drop + add): old files read its default, never old bytes.
    let unmatched_base: Vec<usize> = (0..base.columns.len())
        .filter(|&bi| !base_taken[bi])
        .collect();
    let unmatched_cur: Vec<usize> = (0..current.num_cols())
        .filter(|&ci| matched[ci].is_none())
        .collect();
    let anchors_base = |bi: usize| base_taken[..bi].iter().filter(|t| **t).count();
    let anchors_cur = |ci: usize| matched[..ci].iter().filter(|m| m.is_some()).count();
    let mut used_base = vec![false; base.columns.len()];
    let mut pair_of: Vec<Option<usize>> = vec![None; current.num_cols()];
    for &ci in &unmatched_cur {
        let mut candidate: Option<usize> = None;
        for &ub in &unmatched_base {
            if used_base[ub] {
                continue;
            }
            if base.columns[ub].dtype == current.columns[ci].dtype
                && base.columns[ub].name != current.columns[ci].name
                && anchors_base(ub) == anchors_cur(ci)
            {
                candidate = Some(ub);
            }
        }
        if let Some(ub) = candidate {
            used_base[ub] = true;
            pair_of[ci] = Some(ub);
        }
    }
    let renamed: Vec<(usize, usize)> = pair_of
        .iter()
        .enumerate()
        .filter_map(|(ci, ub)| ub.map(|ub| (ub, ci)))
        .collect();
    // Exact matches whose casing changed are recorded as renames too (the
    // identity is the same; only the display name moved).
    let case_pairs: Vec<(usize, usize)> = matched
        .iter()
        .enumerate()
        .filter_map(|(ci, m)| {
            m.filter(|&bi| base.columns[bi].name != current.columns[ci].name)
                .map(|bi| (bi, ci))
        })
        .collect();
    // The view: how each current column reads from a base row.
    let mut view = Vec::with_capacity(current.num_cols());
    for ci in 0..current.num_cols() {
        match matched[ci] {
            Some(bi) => view.push(DiffCol::Base(bi)),
            None => match pair_of[ci] {
                Some(bi) => view.push(DiffCol::Base(bi)),
                None => view.push(DiffCol::Added(
                    current.columns[ci].default.clone().filter(|v| !v.is_null()),
                )),
            },
        }
    }
    let added: Vec<usize> = unmatched_cur
        .iter()
        .copied()
        .filter(|ci| pair_of[*ci].is_none())
        .collect();
    let dropped: Vec<usize> = unmatched_base
        .iter()
        .copied()
        .filter(|bi| !used_base[*bi])
        .collect();
    let changed =
        !added.is_empty() || !dropped.is_empty() || !renamed.is_empty() || !case_pairs.is_empty();
    if !changed {
        return Ok(SchemaDelta { record: None, view });
    }
    let version = base.version + 1;
    // Prune the rename histories of dropped identities: walk each dropped
    // column's name backwards through the log collecting the entries.
    let mut removed: Vec<usize> = Vec::new();
    for bi in &dropped {
        let mut name = base.columns[*bi].name.clone();
        for (ri, r) in base.renames.iter().enumerate().rev() {
            if r.to == name {
                if !removed.contains(&ri) {
                    removed.push(ri);
                }
                name = r.from.clone();
            }
        }
    }
    let mut renames: Vec<Rename> = base
        .renames
        .iter()
        .enumerate()
        .filter(|(i, _)| !removed.contains(i))
        .map(|(_, r)| r.clone())
        .collect();
    for (ub, uc) in renamed.iter().chain(case_pairs.iter()) {
        if base.columns[*ub].name != current.columns[*uc].name {
            renames.push(Rename {
                version,
                from: base.columns[*ub].name.clone(),
                to: current.columns[*uc].name.clone(),
            });
        }
    }
    // The new live-column list, in the current table's order: surviving
    // identities keep their `added` epoch and take the current name/default.
    let mut columns = Vec::with_capacity(current.num_cols());
    for (ci, c) in current.columns.iter().enumerate() {
        let added_at = match matched[ci] {
            Some(bi) => base.columns[bi].added,
            None => match renamed.iter().find(|(_, uc)| *uc == ci) {
                Some((ub, _)) => base.columns[*ub].added,
                None => version,
            },
        };
        columns.push(SchemaColumn {
            name: c.name.clone(),
            dtype: c.dtype,
            default: c.default.clone().filter(|v| !v.is_null()),
            added: added_at,
        });
    }
    Ok(SchemaDelta {
        record: Some(TableSchema {
            version,
            columns,
            renames,
        }),
        view,
    })
}

/// A manifest path for internal decode errors (schema deltas are planned
/// from in-memory manifests; real read paths carry their own path).
pub(crate) fn manifest_path() -> PathBuf {
    PathBuf::from("catalog.json")
}
