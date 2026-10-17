//! The lake catalog: the JSON metadata of one version.
//!
//! `catalog.json` and `snapshots/<version>.json` have the same shape:
//!
//! ```json
//! {
//!   "format_version": 5,
//!   "updated": "<iso8601>",
//!   "version": 3,
//!   "tables": {
//!     "main.t": {
//!       "files": [
//!         { "path": "data/main.t/<uuid>.vortex", "rows": 5 },
//!         { "path": "data/main.t/<uuid>.vortex", "rows": 3 }
//!       ],
//!       "deletes": [
//!         { "path": "data/main.t/<uuid>.pinkdel", "file": "data/main.t/<a-uuid>.vortex",
//!           "count": 1 }
//!       ],
//!       "rows": 7,
//!       "format": "vortex"
//!     }
//!   }
//! }
//! ```
//!
//! `format_version` 4 (the incremental format) lists each data file with its
//! own exact row count and may carry **positional delete files**: every delete
//! file names the one data file whose positions it drops (`file`) and how many
//! it drops (`count`); `rows` is the logical row count (data rows minus
//! deleted positions) and must equal `sum(files.rows) - sum(deletes.count)`.
//! Every entry declares its `format`: `"vortex"` in a `vortex-format` build,
//! `"pink"` (the engine's single-table `.pink` bytes, with a per-file
//! CRC-protected statistics block) in a lean one.
//!
//! `format_version` 2 and 3 are single-file Vortex lakes (3: larger row
//! blocks), readable but not written any more; a v1 manifest — written by the
//! wave-34 lean writer — has no `format` field, implies the engine's `.pink`
//! bytes, and carries one entry-level statistics block. In v1–v3 every entry
//! lists exactly one data file, as a plain string path.
//!
//! Schema and typing live inside the data files, never here (a v1/v4 lean
//! entry's statistics block duplicates the schema so attach can serve it
//! without a data-file read; the data file stays authoritative for loading).
//!
//! Wave 64 adds the optional **snapshot record** to a manifest:
//! `"author"`, `"commit_message"` and `"changes"` (see
//! [`crate::snapshot::ChangeSummary`]). The record is additive — an older
//! reader ignores the fields (serde does not deny unknown fields), and every
//! field is optional, so the manifest shape of a save that recorded nothing
//! is byte-identical to the pre-wave-64 shape. `"version"` and `"updated"`
//! (the version's publish time, millisecond ISO-8601 UTC) were already there;
//! together they are the per-version record `.lake snapshots` lists.
//!
//! Wave 66 adds two additive fields for partitioning:
//!
//! ```json
//! "main.t": {
//!   "files": [
//!     { "path": "data/main.t/<uuid>.vortex", "rows": 2,
//!       "partition": { "b": "x" },
//!       "stats": { …zone map… } }
//!   ],
//!   "rows": 2,
//!   "format": "vortex",
//!   "partition_by": [ { "column": "b", "transform": "identity" } ]
//! }
//! ```
//!
//! `partition_by` is the table's current partition specification (a vector of
//! [`PartitionKey`]s, mirroring DuckLake's `ducklake_partition_column` rows:
//! the column plus its transform, DuckLake's own `"identity"` / `"bucket(4)"`
//! spelling). `partition` on a data file is the value the file was written
//! under, keyed by the partition key's [`PartitionKey::key_text`] (`"b"` for
//! an identity key, `"bucket(4, b)"` for a bucket key); a JSON `null` value is
//! a SQL NULL. Both fields are optional and additive, so an unpartitioned
//! lake's manifest shape is unchanged, and both are validated structurally
//! when a manifest is read (a table must not partition on the same key twice,
//! every file partition key must be a parseable key, and a file partition
//! value must decode under its column's type at attach time, where the schema
//! is known).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::snapshot::ChangeSummary;
use crate::{LakeError, Result};

/// One partition key of a table's partition specification (wave 66), the
/// maker-side twin of DuckLake's `ducklake_partition_column` row: the key
/// column plus its transform.
///
/// `transform` uses DuckLake's own spelling: `"identity"` for a bare column,
/// `"bucket(N)"` for `bucket(N, column)`. Only identity is written and pruned
/// by this build; `bucket(N)` is accepted structurally (a newer writer's
/// lake stays readable) but its values are never used for pruning — see
/// [`PartitionKey::is_identity`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PartitionKey {
    /// The column the key partitions on, as the spec wrote it (matched
    /// case-insensitively against the table's schema).
    pub(crate) column: String,
    /// The partition transform, DuckLake-spelled: `"identity"` or
    /// `"bucket(N)"`.
    pub(crate) transform: String,
}

/// Why a partition key text did not parse (the DDL error text).
pub(crate) fn partition_key_error(text: &str) -> String {
    format!(
        "invalid partition key \"{text}\": expected a column name or \
         bucket(N, column)"
    )
}

impl PartitionKey {
    /// Parse one partition-key text: a bare column name (identity), or
    /// `bucket(N, column)` with `N >= 1`. Anything else — including the
    /// DuckLake transforms this build does not implement (`year`, `month`,
    /// `day`, `hour`, `epoch_*`, `truncate`) — is refused; DuckLake itself
    /// supports `year, month, day, hour, epoch_year, epoch_month, epoch_day,
    /// epoch_hour, and bucket` and reports anything else as an unsupported
    /// partition function.
    pub(crate) fn parse(text: &str) -> std::result::Result<Self, String> {
        let trimmed = text.trim();
        if let Some(rest) = trimmed.strip_suffix(')') {
            if let Some(inner) = rest.strip_prefix("bucket(") {
                let Some((n, column)) = inner.split_once(',') else {
                    return Err(partition_key_error(text));
                };
                let column = column.trim();
                let ok = n.trim().parse::<u32>().is_ok_and(|n| n >= 1)
                    && !column.is_empty()
                    && !column.contains('(')
                    && !column.contains(')');
                if !ok {
                    return Err(partition_key_error(text));
                }
                return Ok(PartitionKey {
                    column: column.to_string(),
                    transform: format!("bucket({})", n.trim()),
                });
            }
            return Err(partition_key_error(text));
        }
        if trimmed.is_empty()
            || trimmed.contains('(')
            || trimmed.contains(',')
            || trimmed.contains(char::is_whitespace)
        {
            return Err(partition_key_error(text));
        }
        Ok(PartitionKey {
            column: trimmed.to_string(),
            transform: "identity".to_string(),
        })
    }

    /// True for an identity key — the only transform this build writes,
    /// records file values for, and prunes by.
    pub(crate) fn is_identity(&self) -> bool {
        self.transform == "identity"
    }

    /// Structurally valid: a non-empty column and a transform this build
    /// understands (`identity`, or `bucket(N)` with `N >= 1`).
    pub(crate) fn is_valid(&self) -> bool {
        !self.column.is_empty()
            && !self.column.contains('(')
            && !self.column.contains(',')
            && (self.is_identity() || matches!(self.bucket(), Some(n) if n >= 1))
    }

    /// The canonical key text a file's `partition` map is keyed by:
    /// the bare column for identity, `bucket(N, column)` otherwise.
    pub(crate) fn key_text(&self) -> String {
        if self.is_identity() {
            self.column.clone()
        } else if self.transform.starts_with("bucket(") {
            format!("bucket({}, {})", bucket_n(&self.transform), self.column)
        } else {
            format!("{}({})", self.transform, self.column)
        }
    }

    /// The `bucket` modulus of a `bucket(N)` transform; `None` otherwise.
    pub(crate) fn bucket(&self) -> Option<u32> {
        self.transform
            .strip_prefix("bucket(")?
            .strip_suffix(')')?
            .parse()
            .ok()
    }
}

/// The raw `N` of a `bucket(N)` transform (a string, so a malformed
/// transform keeps its own spelling in error messages).
fn bucket_n(transform: &str) -> &str {
    transform
        .strip_prefix("bucket(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or(transform)
}

/// The data-file format of one table entry.
///
/// `Pink` is the engine's own `.pink` single-table persistence bytes (the v1
/// lake format, and the v4 lean format); `Vortex` is a Vortex file (the v2/v3
/// format and the v4 `vortex-format` format). Serialized as a lowercase
/// string. Unambiguous in every version: a v1 entry has no `format` field at
/// all (it loads as `Pink`), and a v4 entry must declare one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FileFormat {
    /// The engine's `.pink` single-table persistence bytes (format_version 1,
    /// or 4 in a lean build).
    Pink,
    /// A Vortex file (format_version 2/3, or 4 in a `vortex-format` build).
    Vortex,
}

/// One data file of a table entry.
///
/// A v1–v3 manifest lists a plain string path (one file per entry, no row
/// information); a v4 manifest lists an object with the file's exact row
/// count, and — for `.pink` files — the CRC-protected statistics block that
/// describes it, so `.lake attach` can merge per-file statistics across an
/// append-only file list without opening any file.
///
/// Wave 66 adds two optional fields to the v4 object: `stats` may also carry
/// the CRC-protected zone map of a **Vortex** file (published at write time
/// from the same facts the Vortex footer stores; attach cross-checks the two
/// and prefers the block, which can be more informative for truncated string
/// statistics), and `partition` is the file's partition value map (one entry
/// per partition key, keyed by [`PartitionKey::key_text`]; `None` is a SQL
/// NULL). Both are additive: a manifest without them keeps its old shape.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum FileRef {
    /// v1–v3: a plain lake-relative path (the entry's single data file).
    Path(String),
    /// v4+: the path, the file's exact row count, and (`.pink` files only)
    /// its statistics block.
    Entry {
        path: String,
        rows: u64,
        /// v5: the table schema version this file was written under (see
        /// [`TableEntry::columns`]). Absent in v4 manifests and on files of a
        /// legacy entry; attach then reads them as written under the entry's
        /// current schema (no rename/default mapping). Written on every file
        /// of a v5-recorded entry.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_version: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stats: Option<crate::stats::StatsBlock>,
        /// The file's written partition values, keyed by the partition key's
        /// [`PartitionKey::key_text`]; a `None` value is SQL NULL. Absent for
        /// an unpartitioned file (every pre-wave-66 file included) and for a
        /// file written before the table's spec was set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partition: Option<BTreeMap<String, Option<String>>>,
    },
}

/// Hand-written so a malformed v4 file object gets a precise message (the
/// derived untagged deserializer reports only "data did not match any
/// variant", which hides that a row count is missing).
impl<'de> Deserialize<'de> for FileRef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Path(String),
            Entry {
                path: String,
                rows: Option<u64>,
                schema_version: Option<u64>,
                stats: Option<crate::stats::StatsBlock>,
                partition: Option<BTreeMap<String, Option<String>>>,
            },
        }
        match Repr::deserialize(deserializer)? {
            Repr::Path(path) => Ok(FileRef::Path(path)),
            Repr::Entry {
                path,
                rows,
                schema_version,
                stats,
                partition,
            } => {
                let Some(rows) = rows else {
                    return Err(D::Error::custom(format!(
                        "data file \"{path}\" is listed without its row count (a \
                         format_version 4 entry lists files as \
                         {{\"path\": \"…\", \"rows\": n}} objects)"
                    )));
                };
                Ok(FileRef::Entry {
                    path,
                    rows,
                    schema_version,
                    stats,
                    partition,
                })
            }
        }
    }
}

impl FileRef {
    /// The data file's lake-relative path.
    pub(crate) fn path(&self) -> &str {
        match self {
            FileRef::Path(path) => path,
            FileRef::Entry { path, .. } => path,
        }
    }

    /// The file's exact row count; `None` for a v1–v3 entry (which carries no
    /// per-file row information).
    pub(crate) fn rows(&self) -> Option<u64> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { rows, .. } => Some(*rows),
        }
    }

    /// The file's statistics block: the v4 `.pink` per-file block, or the
    /// catalog-published Vortex zone map (both v4). `None` for v1–v3 paths
    /// (the v1 entry-level block lives on the entry) and for a file published
    /// without one.
    pub(crate) fn stats(&self) -> Option<&crate::stats::StatsBlock> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { stats, .. } => stats.as_ref(),
        }
    }

    /// The file's partition value map (v4), if it was written under a
    /// partition spec.
    pub(crate) fn partition(&self) -> Option<&BTreeMap<String, Option<String>>> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { partition, .. } => partition.as_ref(),
        }
    }

    /// Set this file's partition value map (a no-op on a v1–v3 [`FileRef::Path`],
    /// which never carries one).
    pub(crate) fn with_partition(mut self, partition: BTreeMap<String, Option<String>>) -> Self {
        if let FileRef::Entry { partition: p, .. } = &mut self {
            *p = Some(partition);
        }
        self
    }

    /// The table schema version this file was written under (v5 files of a
    /// recorded entry); `None` for v1–v4 files and legacy entries.
    pub(crate) fn schema_version(&self) -> Option<u64> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { schema_version, .. } => *schema_version,
        }
    }

    /// [`FileRef::schema_version`] set to a new value (verbatim variant kept;
    /// an entry's partition values are carried through unchanged).
    pub(crate) fn with_schema_version(&self, version: Option<u64>) -> FileRef {
        match self {
            FileRef::Path(path) => FileRef::Path(path.clone()),
            FileRef::Entry {
                path,
                rows,
                stats,
                partition,
                ..
            } => FileRef::Entry {
                path: path.clone(),
                rows: *rows,
                schema_version: version,
                stats: stats.clone(),
                partition: partition.clone(),
            },
        }
    }
}

/// One column of a v5 table entry's schema record (wave 65): the logical
/// name, its canonical type name (`DataType::name()`), the optional DEFAULT
/// as a canonical value string (the same encoding the statistics block uses;
/// decoded with the column's type), and the table schema version at which the
/// column appeared (`added`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ColumnEntry {
    pub(crate) name: String,
    /// `INTEGER`, `DECIMAL(10,2)`, … ([`pinkdb::DataType::name`]).
    #[serde(rename = "type")]
    pub(crate) dtype: String,
    /// The column's DEFAULT, canonically encoded; absent = NULL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default: Option<String>,
    /// The table schema version at which this column appeared.
    pub(crate) added: u64,
}

impl ColumnEntry {
    /// Build the record for one live column.
    pub(crate) fn new(
        name: &str,
        dtype: pinkdb::DataType,
        default: Option<&pinkdb::Value>,
        added: u64,
    ) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            dtype: dtype.name(),
            default: match default {
                Some(v) if !v.is_null() => {
                    Some(crate::stats::encode_value(dtype, v).ok_or_else(|| {
                        LakeError::Internal {
                            message: format!(
                            "column \"{name}\": default {v:?} does not match its {dtype:?} type"
                        ),
                        }
                    })?)
                }
                _ => None,
            },
            added,
        })
    }

    /// The column's type, parsed back.
    pub(crate) fn dtype(&self) -> Option<pinkdb::DataType> {
        parse_dtype_name(&self.dtype)
    }
}

/// Parse a canonical type name back to [`pinkdb::DataType`]
/// (`DECIMAL(p,s)` included).
pub(crate) fn parse_dtype_name(s: &str) -> Option<pinkdb::DataType> {
    let upper = s.to_ascii_uppercase();
    if let Some(rest) = upper.strip_prefix("DECIMAL(") {
        let inner = rest.strip_suffix(')')?;
        let (p, sc) = inner.split_once(',')?;
        let precision = p.trim().parse::<u8>().ok()?;
        let scale = sc.trim().parse::<u8>().ok()?;
        return pinkdb::types::decimal_type(precision, scale);
    }
    pinkdb::DataType::from_sql(&upper)
}

/// One rename of a table's schema history (wave 65): at table schema version
/// `version` the column last known as `from` became `to`. Files written
/// before `version` hold the old name physically; the reader walks the log
/// backwards to map a logical column to a physical one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RenameEntry {
    /// The table schema version the rename produced.
    pub(crate) version: u64,
    pub(crate) from: String,
    pub(crate) to: String,
}

/// One positional delete file of a v4 table entry: the rows it deletes from
/// one data file, by 0-based position within that file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeleteRef {
    /// The delete file's lake-relative path.
    pub(crate) path: String,
    /// The data file (lake-relative path) whose positions it drops. Must name
    /// one of the entry's `files`; positions are per that file, unaffected by
    /// any other delete file or data file.
    pub(crate) file: String,
    /// Number of positions in the delete file (its exact row count), checked
    /// against the file's contents when it is read.
    pub(crate) count: u64,
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
    /// The lake-wide schema version at this version (wave 65; DuckLake's
    /// `snapshot.schema_version`). Starts at 0 with the empty `create`
    /// manifest and increments by one on every published version that
    /// created, dropped or altered a table's schema — a pure-data version
    /// leaves it unchanged. Absent (0) in v4 manifests.
    #[serde(default)]
    pub(crate) schema_version: u64,
}

/// One table of one version: the immutable data files it is made of, the
/// positional delete files applied on top of them, and the logical row count
/// (checked on load).
///
/// A v4 writer appends: a table's entry grows by new data files (and delete
/// files) from commit to commit, and never rewrites a file it published
/// before. A v1–v3 entry has exactly one `.pink`/`.vortex` data file (as a
/// plain string path, no `deletes`), and a v1 entry may carry one entry-level
/// `stats` block (see the `stats` module); v4 `.pink` entries carry one
/// per-file stats block inside each `files` object instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub(crate) files: Vec<FileRef>,
    /// Logical rows: data rows minus the positions every delete file drops.
    pub(crate) rows: u64,
    /// The data-file format; required in a `format_version` 2+ manifest,
    /// absent (⇒ [`FileFormat::Pink`]) in a `format_version` 1 manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) format: Option<FileFormat>,
    /// The v1 `.pink` statistics block (see the struct docs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stats: Option<crate::stats::StatsBlock>,
    /// v4 positional delete files (see [`DeleteRef`]); empty for v1–v3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) deletes: Vec<DeleteRef>,
    /// The table's partition specification (wave 66; see [`PartitionKey`]).
    /// Files written while it is set carry their [`FileRef::partition`]
    /// values; files written earlier (or before `.lake partition`) carry
    /// none and simply cannot be pruned by partition. Absent on an
    /// unpartitioned table — the pre-wave-66 shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) partition_by: Option<Vec<PartitionKey>>,
    /// v5 (wave 65): the table's schema epoch at this version. `0` = a legacy
    /// entry without a schema record (the files carry the schema).
    #[serde(default)]
    pub(crate) schema_version: u64,
    /// v5: the logical schema record — every live column with its type, the
    /// optional DEFAULT an old file's missing value materializes as, and the
    /// epoch it appeared. Absent on legacy entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) columns: Option<Vec<ColumnEntry>>,
    /// v5: the rename history of the live columns (see [`RenameEntry`]),
    /// ordered by version.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) renames: Vec<RenameEntry>,
}

impl TableEntry {
    /// The format to load this entry's files with: v1 entries without a
    /// `format` field are engine `.pink` bytes.
    pub(crate) fn format(&self) -> FileFormat {
        self.format.unwrap_or(FileFormat::Pink)
    }

    /// The entry's partition keys, or an empty slice when unpartitioned.
    pub(crate) fn partition_keys(&self) -> &[PartitionKey] {
        self.partition_by.as_deref().unwrap_or(&[])
    }

    /// Sum of the data files' row counts, when every file carries one (v4).
    pub(crate) fn data_rows(&self) -> Option<u64> {
        let mut total = 0u64;
        for file in &self.files {
            total = total.checked_add(file.rows()?)?;
        }
        Some(total)
    }

    /// Sum of the delete files' counts.
    pub(crate) fn deleted_rows(&self) -> Option<u64> {
        let mut total = 0u64;
        for d in &self.deletes {
            total = total.checked_add(d.count)?;
        }
        Some(total)
    }
}

/// One commit's delta manifest (`commits/<version>.json`): the data and
/// delete files this commit added, per table, plus the logical row movement.
///
/// Written append-only (create-only, like a snapshot) *before* the snapshot
/// manifest and the catalog that reference the same files, so the audit trail
/// is never ahead of the authoritative state. A full snapshot (`.lake save`)
/// also writes one: `replaced` marks a table whose whole file list this commit
/// replaced (a full rewrite), as opposed to an append/delete delta on top of
/// the base version's files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CommitManifest {
    pub(crate) format_version: u32,
    /// The version this commit published.
    pub(crate) version: u64,
    /// The version the commit is based on (`version - 1` for a conflict-free
    /// publish; a retried commit rebases onto the head it finally lands on).
    pub(crate) base_version: u64,
    pub(crate) updated: String,
    pub(crate) tables: BTreeMap<String, TableDelta>,
}

/// One table's change inside a [`CommitManifest`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableDelta {
    /// Data files this commit added.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) data_files: Vec<FileRef>,
    /// Delete files this commit added.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) delete_files: Vec<DeleteRef>,
    /// Rows the commit appended.
    pub(crate) rows_added: u64,
    /// Rows the commit deleted (by position).
    pub(crate) rows_deleted: u64,
    /// True when this commit replaced the table's whole file list (a full
    /// snapshot of that table, not an append/delete delta).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) replaced: bool,
    /// True when this commit changed the table's partition specification.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) partition_changed: bool,
    /// The table's partition specification **after** this commit, when it has
    /// one (a spec change to `None` — cleared — carries only
    /// `partition_changed`). Absent when the commit did not change it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) partition_by: Option<Vec<PartitionKey>>,
}
