//! The lake catalog: the metadata of one version.
//!
//! A manifest is stored as typed DuckLake rows (see `metadata`); this is its
//! logical shape:
//!
//! ```json
//! {
//!   "format_version": 7,
//!   "lake_id": "<32 lowercase hex>",
//!   "updated": "<iso8601>",
//!   "version": 3,
//!   "tables": {
//!     "main.t": {
//!       "files": [
//!         { "path": "data/main.t/<uuid>.pink", "rows": 5 },
//!         { "path": "data/main.t/<uuid>.pink", "rows": 3 }
//!       ],
//!       "deletes": [
//!         { "path": "data/main.t/<uuid>.pinkdel", "file": "data/main.t/<a-uuid>.pink",
//!           "count": 1 }
//!       ],
//!       "rows": 7,
//!       "format": "pink"
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
//! Every entry declares its `format`: `"pink"` (the engine's single-table
//! `.pink` bytes, with a per-file CRC-protected statistics block) is the
//! built-in. The field is a **format id**, resolved through the
//! process-global provider registry ([`crate::format`]), and an extension may
//! register more. An id with no registered provider is a clean
//! [`LakeError::UnknownFormat`] when that table is read or written — never a
//! panic or a silent fallback; a lake still opens, and
//! [`crate::Lake::unavailable_tables`] names the tables this build cannot
//! read (wave 88, rule 5). The catalog stores only the id string, so existing
//! manifests stay byte-identical.
//!
//! Wave 89 adds the optional per-table `"table_id"` — a random 128-bit
//! identity (32 lowercase hex characters), assigned when a table is created
//! and preserved for the same catalog key across later versions:
//!
//! ```json
//! "main.t": { "files": [ … ], "rows": 7, "format": "pink",
//!             "table_id": "0123456789abcdef0123456789abcdef" }
//! ```
//!
//! The field is additive: absent on entries written by older builds (and
//! omitted while it is absent, so old manifests stay byte-identical), ignored
//! by older readers. Every version this build publishes assigns an id to
//! every entry that lacks one, so an old lake acquires ids on its next write.
//! A table dropped and recreated gets a **new** id; replication
//! (`crate::replication`) compares the source entry's id with the one it
//! recorded and reseeds when it changed.
//!
//! Every entry declares its `format`, and a table keeps it across saves and
//! appends; changing it is always explicit (`Lake::set_table_format`,
//! `Lake::rewrite_table_format`). Wave 102 (`format_version` 7, the only
//! version this build reads) adds each `.pink` data file's footer location
//! to its file object (`footer_offset`, `footer_len`, `footer_crc32c`).
//!
//! Only `format_version` 7 is read: wave 102 dropped every older catalog.
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
//!     { "path": "data/main.t/<uuid>.pink", "rows": 2,
//!       "partition": { "b": "x" },
//!       "stats": { …zone map… } }
//!   ],
//!   "rows": 2,
//!   "format": "pink",
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
//!
//! Wave 68 adds **inlined rows** — small commits whose appended rows are
//! stored in the manifest instead of a data file (DuckLake's
//! `data_inlining_row_limit`, whose default is 10 rows per table):
//!
//! ```json
//! "main.t": {
//!   "files": [ … ],
//!   "rows": 3,
//!   "format": "pink",
//!   "schema_version": 1,
//!   "columns": [ … ],
//!   "inlined": [
//!     { "row_id": 0, "begin": 2, "schema_version": 1,
//!       "columns": ["a", "b"], "values": ["1", "x"] },
//!     { "row_id": 3, "begin": 3, "end": 4, "flushed": true,
//!       "schema_version": 1, "columns": ["a", "b"], "values": ["2", "y"] }
//!   ],
//!   "inlined_limit": 5,
//!   "next_row_id": 4
//! }
//! ```
//!
//! The list is the table's **inlined row history**: `begin` is the version
//! that added the row version, `end` (optional) the version that deleted or
//! superseded it (an update is an end plus a new row version with the same
//! `row_id`, DuckLake's `_ducklake_begin_snapshot`/`_ducklake_end_snapshot`
//! pair), and a row is **live** exactly while `end` is absent. `flushed`
//! marks an `end` written by `.lake flush` — the row was *materialized into a
//! data file*, not deleted; the record stays as the change log. `values` are
//! canonically encoded like the statistics block ([`crate::stats`]), `null`
//! for SQL NULL, positional against `columns` — the column names **as the row
//! was written** (they decode through the schema record's rename map, like
//! per-file schemas; a column added later reads its DEFAULT and a dropped one
//! is skipped). `inlined_limit` is the table's `data_inlining_row_limit`
//! (absent = the lake handle's default), and `next_row_id` is the per-table
//! row-id counter shared by inlined rows and data-file row ranges
//! ([`FileRef`]'s `row_id_start`).
//!
//! Every field is optional and additive: a table without inlined rows keeps
//! the pre-wave-68 shape. `rows` stays the table's **logical** row count —
//! `sum(files.rows) - sum(deletes.count) + live inlined rows` — and the
//! manifest reader enforces exactly that invariant, so a stats-only
//! `COUNT(*)` stays exact with inlined rows present.

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

/// The data-file format id of one table entry.
///
/// A thin handle over the catalog's lowercase `format` string, resolved
/// through the process-global provider registry ([`crate::format`]):
///
/// - `"pink"` is the engine's own `.pink` single-table persistence bytes (the
///   v1 lake format, and the v4 lean format); it is registered by every
///   build;
/// - extensions may register further ids (the Parquet provider does).
///
/// Serialized exactly as that string (`#[serde(transparent)]`), so existing
/// catalogs stay byte-identical. The type deliberately accepts any string on
/// deserialization: an unknown id is refused later, by the provider registry,
/// with a clean error naming it. Unambiguous in every version: a v1 entry has
/// no `format` field at all (it loads as `"pink"`), and a v4 entry must
/// declare one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct FileFormat(String);

impl FileFormat {
    /// A handle for the given catalog id.
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The built-in `"pink"` format (the v1 default).
    pub(crate) fn pink() -> Self {
        Self(crate::format::PINK_FORMAT_ID.to_string())
    }

    /// The catalog id string.
    pub(crate) fn id(&self) -> &str {
        &self.0
    }
}

/// One data file of a table entry.
///
/// A v1–v3 manifest lists a plain string path (one file per entry, no row
/// information); a v4 manifest lists an object with the file's exact row
/// count, and — for `.pink` files — the CRC-protected statistics block that
/// describes it, so `.lake attach` can merge per-file statistics across an
/// append-only file list without opening any file.
///
/// Wave 66 adds two optional fields to the file object: `stats` carries the
/// CRC-protected zone map of the file, and `partition` is the file's
/// partition value map (one entry per partition key, keyed by
/// [`PartitionKey::key_text`]; `None` is a SQL NULL). Both are additive: a
/// manifest without them keeps its old shape.
///
/// Wave 68 adds `row_id_start`: the first row id of the file's rows, assigned
/// by the commit that published it (the per-table [`TableEntry::next_row_id`]
/// counter; DuckLake's `ducklake_data_file.row_id_start`). Change-data reads
/// address a file row as `row_id_start + position`. Absent on a file published
/// by a full save, a maintenance output (merge/rewrite), or a pre-wave-68
/// file — its rows then have no recorded row id (`NULL` in a change row); the
/// values stay exact either way.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
// The entry variant is the hot path (one per data file); boxing it would add
// an allocation to every attach and commit for no runtime benefit.
#[allow(clippy::large_enum_variant)]
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
        /// Wave 68: the row id of the file's first row (`None` = not
        /// recorded; see the struct docs).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        row_id_start: Option<u64>,
        /// Wave 102 (`format_version` 7): where the file's footer is, so a
        /// reader fetches it **without** probing the file's 20-byte trailer —
        /// one ranged read instead of two. Recorded for the engine's `.pink`
        /// data files (the only format whose footer the lake locates this
        /// way); absent for every other format, and for a `.pink` file
        /// written by an older build.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        footer_offset: Option<u64>,
        /// The footer's length in bytes (with [`FileRef::Entry::footer_offset`]
        ///).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        footer_len: Option<u64>,
        /// The footer's CRC32C, as the file's trailer records it. The reader
        /// verifies the footer against it before decoding a byte of it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        footer_crc32c: Option<u32>,
        /// True when `stats` was built and sealed from the catalog rows in
        /// this process (the SQL store's `load_files`). Attach may then skip
        /// re-hashing the block. Never serialized: a manifest read from disk
        /// must verify.
        #[serde(skip)]
        stats_sealed: bool,
    },
}

impl PartialEq for FileRef {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FileRef::Path(a), FileRef::Path(b)) => a == b,
            (
                FileRef::Entry {
                    path,
                    rows,
                    schema_version,
                    stats,
                    partition,
                    row_id_start,
                    footer_offset,
                    footer_len,
                    footer_crc32c,
                    ..
                },
                FileRef::Entry {
                    path: other_path,
                    rows: other_rows,
                    schema_version: other_schema,
                    stats: other_stats,
                    partition: other_partition,
                    row_id_start: other_row_id,
                    footer_offset: other_offset,
                    footer_len: other_len,
                    footer_crc32c: other_crc,
                    ..
                },
            ) => {
                path == other_path
                    && rows == other_rows
                    && schema_version == other_schema
                    && stats == other_stats
                    && partition == other_partition
                    && row_id_start == other_row_id
                    && footer_offset == other_offset
                    && footer_len == other_len
                    && footer_crc32c == other_crc
            }
            _ => false,
        }
    }
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
        // A short-lived deserialization helper: the size difference between
        // the bare-path and object forms does not matter here.
        #[allow(clippy::large_enum_variant)]
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
                row_id_start: Option<u64>,
                footer_offset: Option<u64>,
                footer_len: Option<u64>,
                footer_crc32c: Option<u32>,
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
                row_id_start,
                footer_offset,
                footer_len,
                footer_crc32c,
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
                    row_id_start,
                    footer_offset,
                    footer_len,
                    footer_crc32c,
                    stats_sealed: false,
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

    /// The file's per-file `.pink` statistics block. `None` for a file
    /// published without one.
    pub(crate) fn stats(&self) -> Option<&crate::stats::StatsBlock> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { stats, .. } => stats.as_ref(),
        }
    }

    /// True when [`FileRef::stats`] was built and sealed from catalog rows in
    /// this process; attach can skip its CRC re-hash.
    pub(crate) fn stats_sealed(&self) -> bool {
        match self {
            FileRef::Path(_) => false,
            FileRef::Entry { stats_sealed, .. } => *stats_sealed,
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

    /// Wave 68: the row id of the file's first row, when the file's row ids
    /// were recorded (`row_id_start + position` addresses one of its rows).
    pub(crate) fn row_id_start(&self) -> Option<u64> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { row_id_start, .. } => *row_id_start,
        }
    }

    /// [`FileRef::row_id_start`] set to a new value (a no-op on a v1–v3
    /// [`FileRef::Path`], which never carries one).
    pub(crate) fn with_row_id_start(&self, start: Option<u64>) -> FileRef {
        match self {
            FileRef::Path(path) => FileRef::Path(path.clone()),
            FileRef::Entry {
                path,
                rows,
                schema_version,
                stats,
                partition,
                row_id_start: _,
                footer_offset,
                footer_len,
                footer_crc32c,
                stats_sealed,
            } => FileRef::Entry {
                path: path.clone(),
                rows: *rows,
                schema_version: *schema_version,
                stats: stats.clone(),
                partition: partition.clone(),
                row_id_start: start,
                footer_offset: *footer_offset,
                footer_len: *footer_len,
                footer_crc32c: *footer_crc32c,
                stats_sealed: *stats_sealed,
            },
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
    /// an entry's partition values and row-id start are carried through
    /// unchanged).
    pub(crate) fn with_schema_version(&self, version: Option<u64>) -> FileRef {
        match self {
            FileRef::Path(path) => FileRef::Path(path.clone()),
            FileRef::Entry {
                path,
                rows,
                stats,
                partition,
                row_id_start,
                footer_offset,
                footer_len,
                footer_crc32c,
                stats_sealed,
                ..
            } => FileRef::Entry {
                path: path.clone(),
                rows: *rows,
                schema_version: version,
                stats: stats.clone(),
                partition: partition.clone(),
                row_id_start: *row_id_start,
                footer_offset: *footer_offset,
                footer_len: *footer_len,
                footer_crc32c: *footer_crc32c,
                stats_sealed: *stats_sealed,
            },
        }
    }

    /// The file's recorded footer location (wave 102): where its footer is,
    /// how long it is and the CRC32C the file's trailer records for it.
    /// `None` for a format whose footer the lake does not locate this way.
    /// The recorded footer offset (formats that publish one).
    pub(crate) fn footer_offset(&self) -> Option<u64> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { footer_offset, .. } => *footer_offset,
        }
    }

    /// The recorded footer length (formats that publish one).
    pub(crate) fn footer_len(&self) -> Option<u64> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry { footer_len, .. } => *footer_len,
        }
    }

    pub(crate) fn footer_location(&self) -> Option<crate::format::FooterLocation> {
        match self {
            FileRef::Path(_) => None,
            FileRef::Entry {
                footer_offset,
                footer_len,
                footer_crc32c,
                ..
            } => Some(crate::format::FooterLocation {
                offset: (*footer_offset)?,
                length: (*footer_len)?,
                crc32c: (*footer_crc32c)?,
            }),
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
    /// The DuckLake delete-file format (`"pink"`, `"parquet"` or `"inlined"`).
    /// Old JSON manifests omit it; they always held pink's container.
    #[serde(default = "pink_delete_format")]
    pub(crate) format: String,
    /// Positions when the catalog stores them inline (DuckLake's
    /// `ducklake_inlined_delete_<table_id>` rows) instead of in `path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) positions: Option<Vec<u64>>,
}

fn pink_delete_format() -> String {
    crate::format::PINK_FORMAT_ID.to_string()
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
    /// The lake's random 128-bit identity, hex (M-7; format_version 6).
    /// Absent in v1–v5 manifests. Every v6 manifest must carry it; `Lake`
    /// checks it against `lake.json` on open and against the container
    /// binding when encrypted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) lake_id: Option<String>,
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

/// One inlined row version (wave 68): a row stored in the manifest instead of
/// a data file (see the module docs). See [`InlinedRow::live`],
/// [`TableEntry::live_inlined`] and the `crate::inlined` module for the
/// decode/materialize rules.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct InlinedRow {
    /// The table's per-row identity (`ducklake_inlined_data`'s
    /// `_ducklake_row_id`): unique within the table, never reused, monotonic
    /// in `begin` order. An update ends the old version and inserts the new
    /// one under the same id.
    pub(crate) row_id: u64,
    /// The version that added this row version (`_ducklake_begin_snapshot`).
    pub(crate) begin: u64,
    /// The version that deleted or superseded this row version; absent while
    /// the row is live (`_ducklake_end_snapshot`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) end: Option<u64>,
    /// True when `end` was written by `.lake flush` (the row was materialized
    /// into a data file, not deleted). Absent (false) on a real deletion.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) flushed: bool,
    /// The table schema epoch the row was written under (like
    /// [`FileRef`]'s `schema_version`; used for the rename/default mapping).
    pub(crate) schema_version: u64,
    /// The column names as written, in write order (positional against
    /// `values`).
    pub(crate) columns: Vec<String>,
    /// The row's values, canonically encoded ([`crate::stats::encode_value`]),
    /// positional against `columns`; `null` is a SQL NULL.
    pub(crate) values: Vec<Option<String>>,
}

impl InlinedRow {
    /// True while this row version has not been deleted or superseded
    /// (`end` is absent): exactly the rows a scan unions with the data files.
    pub(crate) fn live(&self) -> bool {
        self.end.is_none()
    }
}

/// Zero-check for `skip_serializing_if` on a counter.
fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// One table of one version: the immutable data files it is made of, the
/// positional delete files applied on top of them, the inlined row history,
/// and the logical row count (checked on load).
///
/// A v4 writer appends: a table's entry grows by new data files (and delete
/// files) from commit to commit, and never rewrites a file it published
/// before. An entry carries one per-file stats block inside each `files`
/// object (see the `stats` module).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub(crate) files: Vec<FileRef>,
    /// Logical rows: data rows minus the positions every delete file drops.
    pub(crate) rows: u64,
    /// Wave 89: the table's random 128-bit identity (32 lowercase hex), a
    /// stable name-independent identity for replication. Assigned when the
    /// table is created and preserved for the same catalog key across later
    /// save/commit/maintenance versions; a drop + recreate mints a new one.
    /// Absent on entries written by older builds; any version this build
    /// writes assigns ids to every entry that lacks one. Omitted from JSON
    /// while absent, so older manifests stay byte-identical and older builds
    /// ignore the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) table_id: Option<String>,
    /// The data-file format; required in a `format_version` 2+ manifest,
    /// absent (⇒ `"pink"`) in a `format_version` 1 manifest.
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
    /// Wave 68: the table's inlined row history (see [`InlinedRow`]) — live
    /// rows and every ended/superseded version this table ever inlined.
    /// Absent (empty) for a table that never inlined.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) inlined: Vec<InlinedRow>,
    /// Wave 68: the table's `data_inlining_row_limit` (0 disables inlining);
    /// absent = the lake handle's default. Persisted like DuckLake's
    /// per-table option of the same name (`ducklake_metadata` scope `table`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) inlined_limit: Option<u64>,
    /// Wave 68: the next per-table row id to assign — shared by inlined rows
    /// and the `row_id_start` of every file an incremental commit adds
    /// (DuckLake's row-id counter). Absent (0) on a table that predates wave
    /// 68.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) next_row_id: u64,
}

impl TableEntry {
    /// The format to load this entry's files with: v1 entries without a
    /// `format` field are engine `.pink` bytes.
    pub(crate) fn format(&self) -> FileFormat {
        self.format.clone().unwrap_or_else(FileFormat::pink)
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

    /// The live inlined row versions (`end` absent), in row-id order — the
    /// rows a scan unions with the data files, after every file.
    pub(crate) fn live_inlined(&self) -> Vec<&InlinedRow> {
        let mut live: Vec<&InlinedRow> = self.inlined.iter().filter(|r| r.live()).collect();
        live.sort_by_key(|r| r.row_id);
        live
    }

    /// The number of live inlined row versions.
    pub(crate) fn live_inlined_rows(&self) -> u64 {
        self.inlined.iter().filter(|r| r.live()).count() as u64
    }

    /// The highest row id the table ever assigned (`None` when it never
    /// assigned one): the floor a writer's next `next_row_id` must exceed.
    pub(crate) fn max_row_id(&self) -> Option<u64> {
        self.inlined
            .iter()
            .map(|r| r.row_id)
            .max()
            .into_iter()
            .chain(
                self.files
                    .iter()
                    .filter_map(|f| f.row_id_start().zip(f.rows()))
                    .filter(|(_, rows)| *rows > 0)
                    .map(|(start, rows)| start.saturating_add(rows - 1)),
            )
            .max()
    }
}

/// Assign a fresh random 128-bit id to every table entry that has none
/// (wave 89). Every publishing path calls this right before building its
/// manifest, so one version written by this build upgrades the whole catalog
/// — old lakes acquire ids on their next write, and entries that already
/// carry one (a full save copies it from the previous entry; commits and
/// maintenance clone their entries) keep it.
pub(crate) fn assign_missing_table_ids(tables: &mut BTreeMap<String, TableEntry>) -> Result<()> {
    for entry in tables.values_mut() {
        if entry.table_id.is_none() {
            entry.table_id = Some(crate::fresh_lake_id()?);
        }
    }
    Ok(())
}

/// One commit's delta manifest: the data and delete files this commit added,
/// per table, plus the logical row movement.
///
/// Published in the same catalog transaction as the snapshot record it
/// belongs to. A full snapshot (`.lake save`) also writes one: `replaced`
/// marks a table whose whole file list this commit replaced (a full rewrite),
/// as opposed to an append/delete delta on top of the base version's files.
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
    /// The runtime-only part of a delta: the target table facts and the
    /// inlined row versions, which the SQL catalog derives back from its rows.
    /// Skipped by serde (it is not part of the stored delta).
    #[serde(skip)]
    pub(crate) runtime: TableRuntime,
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
    /// Maintenance (wave 67): the data files this commit **consolidated
    /// from** (`merge_adjacent_files` / `rewrite_data_files`). Their rows now
    /// live in the listed `data_files`; the source files stay on disk for the
    /// versions that reference them. Absent on ordinary commits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) compacted_files: Vec<String>,
    /// Maintenance (wave 67): the delete files this commit **materialized
    /// and dropped** (`rewrite_data_files`); their positions are baked into
    /// the rewritten data files. Absent on ordinary commits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) removed_delete_files: Vec<String>,
    /// Wave 68: rows this commit **inlined** into the manifest instead of
    /// writing a data file. The rows themselves live in the version's
    /// `inlined` list (`begin == commit version`); this count is the audit
    /// marker a change-data read uses to tell an inlined append from a plain
    /// one.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) rows_inlined: u64,
    /// Wave 68: inlined row versions this commit ended (deleted or
    /// superseded). Their `end` is the commit's version.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) inlined_ended: u64,
    /// Wave 68: inlined rows this commit **materialized into data files**
    /// (`.lake flush`). The listed `data_files` hold the flushed rows, whose
    /// change history stays in the version's `inlined` list; a change-data
    /// read must not report those files' rows as insertions.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) inlined_flushed: u64,
    /// Wave 68: the table's `data_inlining_row_limit` after this commit when
    /// the commit changed it (a clear — back to the default — carries only
    /// `inlining_changed`). Absent when the commit did not change it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) inlined_limit: Option<u64>,
    /// True when this commit set or cleared the table's inlining limit.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) inlining_changed: bool,
}

/// The target facts of one table a commit touched (wave 103).
///
/// A SQL catalog applies a delta to its rows directly, so it needs the
/// table's *after* facts — its identity, format, schema record, partition
/// spec, inlining limit and row-id counter — without rebuilding (or even
/// loading) the table's whole file list. This is the typed form of exactly
/// those facts.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TableMeta {
    pub(crate) table_id: Option<String>,
    pub(crate) format: Option<FileFormat>,
    pub(crate) inlined_limit: Option<u64>,
    pub(crate) partition_by: Option<Vec<PartitionKey>>,
    pub(crate) schema_version: u64,
    pub(crate) columns: Option<Vec<ColumnEntry>>,
    pub(crate) renames: Vec<RenameEntry>,
    pub(crate) next_row_id: u64,
}

impl TableMeta {
    pub(crate) fn from_entry(entry: &TableEntry) -> Self {
        Self {
            table_id: entry.table_id.clone(),
            format: entry.format.clone(),
            inlined_limit: entry.inlined_limit,
            partition_by: entry.partition_by.clone(),
            schema_version: entry.schema_version,
            columns: entry.columns.clone(),
            renames: entry.renames.clone(),
            next_row_id: entry.next_row_id,
        }
    }
}

/// What kind of publication produced a [`TableDelta`]. The SQL catalog keeps
/// this as a narrow integer per changed table: it is the one fact the rows
/// cannot tell apart (an appended file and a rewritten file both begin at the
/// commit's snapshot, but their `rows_added` semantics differ).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum DeltaKind {
    /// An ordinary incremental commit.
    #[default]
    Commit,
    /// A full save (`replaced` is also true).
    Save,
    /// A maintenance rewrite (`compacted_files` / `removed_delete_files`).
    Compaction,
    /// A `.lake flush` of inlined rows into data files.
    Flush,
}

/// The runtime-only part of a [`TableDelta`].
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TableRuntime {
    /// The table's facts after the commit.
    pub(crate) meta: Option<TableMeta>,
    /// The table did not exist in the base version.
    pub(crate) created: bool,
    /// The table disappeared in this commit.
    pub(crate) dropped: bool,
    pub(crate) kind: DeltaKind,
    /// Inlined row versions this commit began (their `begin` is the commit
    /// version).
    pub(crate) inlined_rows: Vec<InlinedRow>,
    /// Inlined row versions this commit ended (their `end` is the commit
    /// version; `flushed` distinguishes a materialization).
    pub(crate) inlined_ends: Vec<InlinedRow>,
}
