//! Per-file metadata: the flat column map, row counts, per-row-group
//! statistics (for pruning and for `parquet_metadata`), and the footer facts
//! the introspection functions expose.
//!
//! Reading metadata never decodes a data page: `SerializedFileReader::new`
//! reads the footer, and everything here comes from that.

use std::sync::Arc;

use parquet::basic::Type as PhysicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::schema::types::Type;
use pinkdb::{DataType, PinkDbError, Result, Value};

use crate::schema_map::{FileColumn, fixed_decimal_value, int_decimal_value, map_columns};

/// One column chunk's statistics, projected into engine values. `min`/`max`
/// are `None` when the file carries no usable min/max (or the column's
/// ordering is not the engine's: unsigned integers, INT96).
#[derive(Debug, Clone, PartialEq)]
pub struct ColStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: Option<u64>,
    /// Whether the stored min/max are exact (α's `min_is_exact`/`max_is_exact`).
    pub min_exact: bool,
    pub max_exact: bool,
}

/// One row group: its row count, per-leaf-column statistics (by leaf index
/// into [`FileMeta::columns`]) and the column chunks' metadata.
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub rows: u64,
    /// The row group's uncompressed byte size (`total_byte_size`).
    pub bytes: i64,
    /// The row group's compressed byte size (the sum of its chunks').
    pub compressed_bytes: i64,
    pub stats: Vec<Option<ColStats>>,
    pub chunks: Vec<ChunkInfo>,
}

/// The facts `parquet_metadata` renders per column chunk (α's column set for
/// the shared columns).
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub file_offset: i64,
    pub num_values: i64,
    pub compression: String,
    pub encodings: Vec<String>,
    pub index_page_offset: Option<i64>,
    pub dictionary_page_offset: Option<i64>,
    pub data_page_offset: i64,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub distinct_count: Option<u64>,
    pub bloom_filter_offset: Option<i64>,
    pub bloom_filter_length: Option<i32>,
}

/// One resolved file.
#[derive(Debug)]
pub struct FileMeta {
    /// The path as the scan displays it (`filename`).
    pub path: String,
    pub columns: Vec<FileColumn>,
    pub rows: u64,
    pub row_groups: Vec<RowGroupMeta>,
    pub created_by: Option<String>,
    pub version: i32,
    /// Key-value metadata in file order.
    pub kv: Vec<(String, Vec<u8>)>,
    /// The thrift metadata length recorded in the trailer (α's
    /// `footer_size`: the 630 of a 954-byte file, not 638).
    pub footer_size: u64,
    pub file_size: u64,
    /// The schema root, for `parquet_schema` (shared, cheap to clone).
    pub schema_root: Arc<Type>,
}

impl FileMeta {
    /// The row group's rows as an engine `u64`; a negative count means the
    /// file is corrupt.
    fn rows_of(rows: i64, path: &str) -> Result<u64> {
        u64::try_from(rows).map_err(|_| {
            PinkDbError::Execution(format!(
                "parquet file \"{path}\" declares a negative row count"
            ))
        })
    }

    /// The column leaf index of the first column named `name`
    /// (case-insensitively, like every SQL name).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }
}

/// Read one file's metadata (no data pages).
pub fn read_file_meta(path: &str) -> Result<FileMeta> {
    let file = std::fs::File::open(path).map_err(|e| {
        PinkDbError::Io(std::io::Error::new(
            e.kind(),
            format!("cannot open \"{path}\": {e}"),
        ))
    })?;
    let file_size = file.metadata().map(|m| m.len()).map_err(PinkDbError::Io)?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| PinkDbError::Execution(format!("cannot read parquet file \"{path}\": {e}")))?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let schema_root: Arc<Type> = file_metadata.schema_descr().root_schema_ptr();
    let columns = map_columns(schema_root.as_ref(), file_metadata.schema_descr())?;
    let rows = FileMeta::rows_of(file_metadata.num_rows(), path)?;

    let mut row_groups = Vec::with_capacity(metadata.num_row_groups());
    for i in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(i);
        let rg_rows = FileMeta::rows_of(rg.num_rows(), path)?;
        let mut stats = Vec::with_capacity(columns.len());
        let mut chunks = Vec::with_capacity(columns.len());
        let mut compressed_bytes = 0i64;
        for (leaf, column) in columns.iter().enumerate() {
            let chunk = rg.column(leaf);
            stats.push(column_stats(chunk.statistics(), column, path)?);
            compressed_bytes += chunk.compressed_size();
            chunks.push(ChunkInfo {
                file_offset: chunk.file_offset(),
                num_values: chunk.num_values(),
                compression: format!("{:?}", chunk.compression()),
                encodings: chunk.encodings().map(|e| format!("{e:?}")).collect(),
                index_page_offset: chunk.index_page_offset(),
                dictionary_page_offset: chunk.dictionary_page_offset(),
                data_page_offset: chunk.data_page_offset(),
                compressed_size: chunk.compressed_size(),
                uncompressed_size: chunk.uncompressed_size(),
                distinct_count: chunk.statistics().and_then(|s| s.distinct_count_opt()),
                bloom_filter_offset: chunk.bloom_filter_offset(),
                bloom_filter_length: chunk.bloom_filter_length(),
            });
        }
        row_groups.push(RowGroupMeta {
            rows: rg_rows,
            bytes: rg.total_byte_size(),
            compressed_bytes,
            stats,
            chunks,
        });
    }

    // The footer size: the thrift metadata length recorded in the trailer
    // (α's `footer_size` — the trailer's 8 bytes are not included).
    let footer_size = read_footer_size(path, file_size);
    let kv = file_metadata
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| {
                    (
                        kv.key.clone(),
                        kv.value.clone().unwrap_or_default().into_bytes(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(FileMeta {
        path: path.to_string(),
        columns,
        rows,
        row_groups,
        created_by: file_metadata.created_by().map(str::to_string),
        version: file_metadata.version(),
        kv,
        footer_size,
        file_size,
        schema_root,
    })
}

/// `metadata_len` from the parquet trailer, or 0 when the trailer is
/// unreadable (the file still read fine, so this is informational).
fn read_footer_size(path: &str, file_size: u64) -> u64 {
    use std::io::{Read, Seek, SeekFrom};
    if file_size < 12 {
        return 0;
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        return 0;
    };
    if file.seek(SeekFrom::End(-8)).is_err() {
        return 0;
    }
    let mut buf = [0u8; 4];
    if file.read_exact(&mut buf).is_err() {
        return 0;
    }
    u64::from(u32::from_le_bytes(buf))
}

/// Project one column chunk's statistics into engine values, or `None` when
/// there is nothing usable.
fn column_stats(
    stats: Option<&Statistics>,
    column: &FileColumn,
    path: &str,
) -> Result<Option<ColStats>> {
    let Some(stats) = stats else {
        return Ok(None);
    };
    let null_count = stats.null_count_opt();
    let min = stat_value(stats, column, true);
    let max = stat_value(stats, column, false);
    let _ = path;
    Ok(Some(ColStats {
        min,
        max,
        null_count,
        min_exact: stats.min_is_exact(),
        max_exact: stats.max_is_exact(),
    }))
}

/// One side of the statistics, in the engine's type for the column.
fn stat_value(stats: &Statistics, column: &FileColumn, min: bool) -> Option<Value> {
    if !prunable_column(column) {
        return None;
    }
    let scale = match column.ty {
        DataType::Decimal { scale, .. } => scale,
        _ => 0,
    };
    match stats {
        Statistics::Boolean(s) => {
            let v = if min { s.min_opt() } else { s.max_opt() }?;
            Some(Value::Boolean(*v))
        }
        Statistics::Int32(s) => {
            let v = *if min { s.min_opt() } else { s.max_opt() }?;
            Some(match column.ty {
                DataType::Integer => Value::Integer(v),
                DataType::Date => Value::Date(v),
                DataType::Decimal { .. } => int_decimal_value(v as i64, scale),
                _ => return None,
            })
        }
        Statistics::Int64(s) => {
            let v = *if min { s.min_opt() } else { s.max_opt() }?;
            Some(match column.ty {
                DataType::Bigint => Value::Bigint(v),
                DataType::Timestamp => Value::Timestamp(v),
                DataType::Decimal { .. } => int_decimal_value(v, scale),
                _ => return None,
            })
        }
        Statistics::Float(s) => {
            let v = *if min { s.min_opt() } else { s.max_opt() }?;
            Some(Value::Double(v as f64))
        }
        Statistics::Double(s) => {
            let v = *if min { s.min_opt() } else { s.max_opt() }?;
            Some(Value::Double(v))
        }
        Statistics::ByteArray(s) => {
            let v = if min { s.min_opt() } else { s.max_opt() }?;
            let text = std::str::from_utf8(v.data()).ok()?;
            Some(Value::Varchar(text.to_string()))
        }
        Statistics::FixedLenByteArray(s) => {
            let v = if min { s.min_opt() } else { s.max_opt() }?;
            match column.ty {
                DataType::Decimal { .. } => {
                    Some(fixed_decimal_value(v.data())).map(|u| Value::Decimal(u, scale))
                }
                DataType::Interval => None,
                _ => None,
            }
        }
        Statistics::Int96(_) => None,
    }
}

/// Whether min/max statistics for this column may be used for pruning at all.
///
/// Unsigned integer columns are excluded: the deprecated `min`/`max` fields
/// (and parquet-rs' folded view of them) carry a signed interpretation, and an
/// unsigned column's true order can disagree — a wrong bound would silently
/// drop rows. INT96 has no usable statistics.
pub fn prunable_column(column: &FileColumn) -> bool {
    column.physical != PhysicalType::INT96 && !column.unsigned
}
