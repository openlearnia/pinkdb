//! Typed views over Iceberg's Avro manifests (wave 72).
//!
//! Two files make up a snapshot's file list: the **manifest list** names the
//! manifests (with per-manifest row and partition summaries) and each
//! **manifest** names the data and delete files of one commit. Both are Avro;
//! [`crate::avro`] decodes the bytes and this module gives the records their
//! names and refused-by-name edge cases. Fields this reader does not need
//! (`column_sizes`, `split_offsets`, `key_metadata`, sort orders) stay decoded
//! but unused: projecting an Avro record costs more code than it saves.

use std::io::Read;

use pinkdb::{PinkDbError, Result};

use crate::avro::{self, Avro};

/// A manifest's content: data files or delete files. v1 manifests have no
/// `content` field and are always data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Content {
    Data,
    Deletes,
}

impl Content {
    pub fn name(self) -> &'static str {
        match self {
            Content::Data => "DATA",
            Content::Deletes => "DELETES",
        }
    }
}

/// A manifest entry's content. Iceberg v2's two delete flavours; only
/// positional deletes are applied (an equality delete is refused, see
/// [`crate::scan`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileContent {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

impl FileContent {
    pub fn name(self) -> &'static str {
        match self {
            FileContent::Data => "DATA",
            FileContent::PositionDeletes => "POSITION_DELETES",
            FileContent::EqualityDeletes => "EQUALITY_DELETES",
        }
    }
}

/// A manifest entry's status: a file added by this commit, still live from an
/// earlier one, or removed by this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Existing,
    Added,
    Deleted,
}

impl Status {
    pub fn name(self) -> &'static str {
        match self {
            Status::Existing => "EXISTING",
            Status::Added => "ADDED",
            Status::Deleted => "DELETED",
        }
    }
}

/// One manifest's partition summary: is there a null partition, and the
/// lower/upper bound of each partition field across the manifest's files
/// (Iceberg's single-value encoding per partition type).
///
/// `contains_nan` is parsed but not kept: a NaN compares false against every
/// bound, so the bounds alone already prune soundly.
#[derive(Debug, Clone)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
}

/// One entry of a snapshot's manifest list.
#[derive(Debug, Clone)]
pub struct ManifestFile {
    /// As written (relative to the table root, usually).
    pub path: String,
    pub spec_id: Option<i64>,
    pub content: Content,
    pub sequence_number: i64,
    /// One summary per partition field, in the spec's order (empty for an
    /// unpartitioned table).
    pub partitions: Vec<FieldSummary>,
}

/// The physical file one manifest entry points at.
#[derive(Debug, Clone)]
pub struct DataFile {
    pub content: FileContent,
    /// As written (relative to the table root, usually).
    pub path: String,
    pub format: String,
    pub record_count: i64,
    /// Per-column statistics, keyed by Iceberg field id.
    pub null_counts: Vec<(i64, i64)>,
    pub lower_bounds: Vec<(i64, Vec<u8>)>,
    pub upper_bounds: Vec<(i64, Vec<u8>)>,
}

/// One record of a manifest.
#[derive(Debug, Clone)]
pub struct Entry {
    pub status: Status,
    /// Iceberg v2 inheritance: a null entry sequence number inherits the
    /// manifest's.
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub file: DataFile,
}

/// Read a manifest list.
pub fn read_manifest_list(r: &mut impl Read) -> Result<Vec<ManifestFile>> {
    let records = avro::read_records(r).map_err(manifest_error)?;
    records
        .iter()
        .map(|record| {
            Ok(ManifestFile {
                path: text(record.field("manifest_path")),
                spec_id: opt_long(record.field("partition_spec_id")),
                content: match long(record.field("content")) {
                    0 => Content::Data,
                    1 => Content::Deletes,
                    other => {
                        return Err(manifest_error(format!(
                            "a manifest with content {other} (0 = data, 1 = deletes)"
                        )));
                    }
                },
                sequence_number: long(record.field("sequence_number")),
                partitions: record
                    .field("partitions")
                    .as_array()
                    .map(|summaries| {
                        summaries
                            .iter()
                            .map(|summary| FieldSummary {
                                contains_null: summary
                                    .field("contains_null")
                                    .as_bool()
                                    .unwrap_or(false),
                                lower: summary.field("lower_bound").as_bytes().map(<[u8]>::to_vec),
                                upper: summary.field("upper_bound").as_bytes().map(<[u8]>::to_vec),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// Read a manifest's entries.
pub fn read_manifest(r: &mut impl Read, inherited_sequence: i64) -> Result<Vec<Entry>> {
    let records = avro::read_records(r).map_err(manifest_error)?;
    records
        .iter()
        .map(|record| parse_entry(record, inherited_sequence))
        .collect()
}

/// `inherited_sequence` is the manifest's own sequence number: a v2 entry that
/// leaves its `sequence_number` null inherits it, and a v1 manifest has no
/// such field at all.
fn parse_entry(record: &Avro, inherited_sequence: i64) -> Result<Entry> {
    let status = match long(record.field("status")) {
        0 => Status::Existing,
        1 => Status::Added,
        2 => Status::Deleted,
        other => {
            return Err(manifest_error(format!(
                "a manifest entry with status {other} (0 = existing, 1 = added, 2 = deleted)"
            )));
        }
    };
    let file = record.field("data_file");
    let content = match long(file.field("content")) {
        0 => FileContent::Data,
        1 => FileContent::PositionDeletes,
        2 => FileContent::EqualityDeletes,
        other => {
            return Err(manifest_error(format!(
                "a data file with content {other} (0 = data, 1 = position deletes, \
                 2 = equality deletes)"
            )));
        }
    };
    Ok(Entry {
        status,
        sequence_number: Some(
            opt_long(record.field("sequence_number")).unwrap_or(inherited_sequence),
        ),
        file_sequence_number: Some(
            opt_long(record.field("file_sequence_number")).unwrap_or(inherited_sequence),
        ),
        file: DataFile {
            content,
            path: text(file.field("file_path")),
            format: text(file.field("file_format")),
            record_count: long(file.field("record_count")),
            null_counts: counts(file.field("null_value_counts")),
            lower_bounds: bounds(file.field("lower_bounds")),
            upper_bounds: bounds(file.field("upper_bounds")),
        },
    })
}

/// Iceberg writes these as maps; `apache-avro` (and this decoder) see them as
/// arrays of `{key, value}` records.
fn counts(value: &Avro) -> Vec<(i64, i64)> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| (long(entry.field("key")), long(entry.field("value"))))
                .collect()
        })
        .unwrap_or_default()
}

fn bounds(value: &Avro) -> Vec<(i64, Vec<u8>)> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    Some((
                        long(entry.field("key")),
                        entry.field("value").as_bytes()?.to_vec(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn text(value: &Avro) -> String {
    value.as_str().unwrap_or_default().to_string()
}

fn long(value: &Avro) -> i64 {
    value.as_long().unwrap_or(0)
}

fn opt_long(value: &Avro) -> Option<i64> {
    value.as_long()
}

fn manifest_error(message: impl Into<String>) -> PinkDbError {
    PinkDbError::Execution(format!("iceberg: {} in an Avro manifest", message.into()))
}
