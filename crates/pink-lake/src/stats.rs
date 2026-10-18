//! The v1 `.pink` statistics block, and the one exactness gate both lake
//! formats share.
//!
//! A lean (`vortexlake`-less) save writes one engine `.pink` data file per
//! table. Those files carry no statistics section (the engine format is
//! frozen), so the writer publishes the exact per-column statistics it
//! collected during the encode ([`pinkdb::PinkDb::to_pink_bytes_with_stats`])
//! inside the manifest's table entry — `catalog.json` and
//! `snapshots/<v>.json` are byte-identical by construction, so the statistics
//! are published atomically with the data-file reference. `.lake attach`
//! reads the block and answers exact `COUNT`/`COUNT(col)`/`MIN`/`MAX` from it
//! without opening a data file at all.
//!
//! # Integrity
//!
//! The block carries a CRC-32 (IEEE, the engine's own
//! [`pinkdb::wal::crc32`]) over its canonical JSON, *including* the catalog
//! key, the data-file path, and the row count it describes. Attach verifies
//! it before trusting anything and refuses a mismatch: a corrupted, edited,
//! or stale block can never yield a silently wrong answer. Cross-checks at
//! attach additionally require the block's `table`/`file`/`rows` to equal the
//! entry's key/path/`rows`, and the decoded data file is re-checked against
//! the schema (identity + schema + row count) the first time a table is
//! materialized.
//!
//! # Exactness
//!
//! [`serve_column_stats`] is the single place a raw proven statistic becomes
//! an attachable [`ColumnStats`], used by both this block and the Vortex
//! footer path (`vortex_file::column_stats`): a DOUBLE min/max needs an exact
//! `NaNCount == 0` (the engine's comparator treats NaN as equal to
//! everything and keeps the first-seen value) and a nonzero value
//! (`-0.0 == 0.0` in the same comparator, so the sign of a first-seen zero is
//! not recoverable); everything else passes through exactly as proven.
//!
//! # Value encoding
//!
//! Min/max are stored as canonical strings, one form per column type:
//! `"false"`/`"true"` for BOOLEAN; decimal for INTEGER/DATE (i32) and
//! BIGINT/TIMESTAMP (i64); 16 lowercase hex digits of the raw bit pattern for
//! DOUBLE (JSON cannot carry NaN/±INF, and bit patterns keep `-0.0` and NaN
//! payloads exact); the string itself for VARCHAR (JSON-escaped, NULs and
//! unicode included).

use std::path::Path;

use serde::{Deserialize, Serialize};

use pinkdb::persist::SavedTableStats;
use pinkdb::{ColumnStats, DataType, Value};

use crate::{LakeError, Result};

/// The statistics block version this build writes and reads.
pub(crate) const STATS_VERSION: u32 = 1;

/// The statistics one v1 `.pink` table entry publishes: its schema and
/// per-column exact facts, bound to the catalog key, the data file, and the
/// row count by a CRC-32.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StatsBlock {
    /// Block format version ([`STATS_VERSION`]).
    pub(crate) version: u32,
    /// The flat `db.table` catalog key this block belongs to.
    pub(crate) table: String,
    /// The single data file (lake-relative path) this block describes.
    pub(crate) file: String,
    /// Exact row count of that data file.
    pub(crate) rows: u64,
    /// Per-column schema + statistics, in data-file (scan) order.
    pub(crate) columns: Vec<StatsColumn>,
    /// CRC-32 (IEEE) over the canonical JSON of every other field.
    pub(crate) crc32: u32,
}

/// One column's schema + exact statistics inside a [`StatsBlock`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StatsColumn {
    /// Column name, as the data file stores it.
    pub(crate) name: String,
    /// Lowercase type name: `boolean`, `integer`, `bigint`, `double`,
    /// `varchar`, `date`, `timestamp`.
    pub(crate) dtype: String,
    /// Exact number of NULL rows.
    pub(crate) nulls: u64,
    /// Exact min of the non-NULL values, canonically encoded; `None` when the
    /// column has no non-NULL value (empty table or all-NULL column).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) min: Option<String>,
    /// Exact max of the non-NULL values; `None` with `min`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max: Option<String>,
    /// Exact NaN count (DOUBLE columns; always present for them, absent for
    /// every other type).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) nan: Option<u64>,
}

/// The block's schema and gated statistics: `(schema, stats)` in column
/// order, exactly the shape [`pinkdb::AttachedTable`] wants.
type SchemaAndStats = (Vec<(String, DataType)>, Vec<ColumnStats>);

impl StatsBlock {
    /// Build the block for one saved table from the facts the engine
    /// collected during the encode. `key` is the flat `db.table` catalog key,
    /// `file` the lake-relative path of the one data file.
    ///
    /// Every build writes blocks now: a `vortexlake` build still appends
    /// `.pink` files to a pinklake lake's v4 entries, and those carry per-file
    /// blocks.
    pub(crate) fn from_saved(key: &str, file: &str, saved: &SavedTableStats) -> Result<Self> {
        let mut columns = Vec::with_capacity(saved.columns.len());
        for col in &saved.columns {
            let encode =
                |v: &Option<Value>| match v {
                    None => Ok(None),
                    Some(v) => encode_value(col.dtype, v).map(Some).ok_or_else(|| {
                        LakeError::Internal {
                            message: format!(
                                "statistics for \"{key}\".{}: {v:?} does not match its {dtype:?} \
                             column",
                                col.name,
                                dtype = col.dtype
                            ),
                        }
                    }),
                };
            columns.push(StatsColumn {
                name: col.name.clone(),
                dtype: dtype_name(col.dtype),
                nulls: col.null_count,
                min: encode(&col.min)?,
                max: encode(&col.max)?,
                nan: (col.dtype == DataType::Double).then_some(col.nan_count),
            });
        }
        let mut block = Self {
            version: STATS_VERSION,
            table: key.to_string(),
            file: file.to_string(),
            rows: saved.rows,
            columns,
            crc32: 0,
        };
        block.seal();
        Ok(block)
    }

    /// The CRC-32 over the canonical compact JSON of every field but
    /// `crc32` itself.
    fn compute_crc(&self) -> u32 {
        #[derive(Serialize)]
        struct CrcView<'a> {
            version: u32,
            table: &'a str,
            file: &'a str,
            rows: u64,
            columns: &'a [StatsColumn],
        }
        let view = CrcView {
            version: self.version,
            table: &self.table,
            file: &self.file,
            rows: self.rows,
            columns: &self.columns,
        };
        let bytes = serde_json::to_vec(&view).expect("statistics block serializes");
        pinkdb::wal::crc32(&bytes)
    }

    /// Fill in the CRC after all fields are set (the writer path; see
    /// [`StatsBlock::from_saved`]).
    pub(crate) fn seal(&mut self) {
        self.crc32 = self.compute_crc();
    }

    /// Whether the block's CRC matches its contents.
    pub(crate) fn verify_crc(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    /// The block's schema and gated [`ColumnStats`], for the attach
    /// descriptors. `manifest_path` names the manifest the block came from in
    /// every error. Every check is fail-closed: a block that does not verify,
    /// is not version 1, disagrees with its own schema, or carries an
    /// undecodable value is [`LakeError::Corrupt`], never a guess.
    pub(crate) fn schema_and_stats(&self, manifest_path: &Path) -> Result<SchemaAndStats> {
        let corrupt = |message: String| LakeError::Corrupt {
            path: manifest_path.to_path_buf(),
            message,
        };
        if !self.verify_crc() {
            return Err(corrupt(format!(
                "table \"{}\": the statistics block failed its checksum (corrupted or edited \
                 metadata); refusing to attach from it — use .lake open, or re-save the store \
                 to publish fresh statistics",
                self.table
            )));
        }
        if self.version != STATS_VERSION {
            return Err(corrupt(format!(
                "table \"{}\": statistics block version {} is not readable by this build \
                 (it reads version {STATS_VERSION}); use .lake open",
                self.table, self.version
            )));
        }
        let mut schema = Vec::with_capacity(self.columns.len());
        let mut stats = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let dtype = dtype_from_name(&col.dtype).ok_or_else(|| {
                corrupt(format!(
                    "table \"{}\": column \"{}\" declares unknown type \"{}\"",
                    self.table, col.name, col.dtype
                ))
            })?;
            if col.nulls > self.rows {
                return Err(corrupt(format!(
                    "table \"{}\": column \"{}\" has {} NULLs in {} rows",
                    self.table, col.name, col.nulls, self.rows
                )));
            }
            if col.min.is_some() != col.max.is_some() {
                return Err(corrupt(format!(
                    "table \"{}\": column \"{}\" carries only one of min/max",
                    self.table, col.name
                )));
            }
            let min = decode_option(dtype, &col.min, manifest_path, &self.table, &col.name)?;
            let max = decode_option(dtype, &col.max, manifest_path, &self.table, &col.name)?;
            schema.push((col.name.clone(), dtype));
            stats.push(serve_column_stats(
                dtype,
                Some(col.nulls),
                min,
                max,
                col.nan,
            ));
        }
        Ok((schema, stats))
    }
}

fn decode_option(
    dtype: DataType,
    s: &Option<String>,
    manifest_path: &Path,
    table: &str,
    column: &str,
) -> Result<Option<Value>> {
    match s {
        None => Ok(None),
        Some(s) => decode_value(dtype, s)
            .map(Some)
            .ok_or_else(|| LakeError::Corrupt {
                path: manifest_path.to_path_buf(),
                message: format!(
                    "table \"{table}\": column \"{column}\" carries an undecodable {dtype:?} \
                 statistic {s:?}"
                ),
            }),
    }
}

/// The one exactness gate: given what a format proved about one column,
/// decide which parts the executor may serve. Shared by the `.pink` stats
/// block and the Vortex footer path so the two cannot drift.
///
/// - `null_count` passes through (the format layer only supplies an exact
///   count);
/// - a DOUBLE min/max is served only when the format proved an exact
///   `NaNCount == 0` *and* the statistic is nonzero (refusing `-0.0` too:
///   `-0.0 == 0.0` in the engine's comparator, which keeps the first-seen
///   zero, so its sign is not recoverable from a min/max alone);
/// - every other type's min/max passes through as proven (a truncated or
///   inexact statistic must already be `None` at the format layer).
pub(crate) fn serve_column_stats(
    dtype: DataType,
    null_count: Option<u64>,
    min: Option<Value>,
    max: Option<Value>,
    nan_count: Option<u64>,
) -> ColumnStats {
    let (min, max) = if dtype == DataType::Double {
        if nan_count == Some(0) {
            (nonzero_double(min), nonzero_double(max))
        } else {
            (None, None)
        }
    } else {
        (min, max)
    };
    ColumnStats {
        null_count,
        min,
        max,
    }
}

/// A DOUBLE statistic that can be served: exact, non-NaN, nonzero (catches
/// `-0.0` too).
fn nonzero_double(v: Option<Value>) -> Option<Value> {
    match v {
        Some(Value::Double(x)) if x != 0.0 && !x.is_nan() => Some(Value::Double(x)),
        _ => None,
    }
}

/// The lowercase type name a [`StatsBlock`] stores.
pub(crate) fn dtype_name(dtype: DataType) -> String {
    if let DataType::Decimal { precision, scale } = dtype {
        return format!("decimal({precision},{scale})");
    }
    match dtype {
        DataType::Boolean => "boolean",
        DataType::Integer => "integer",
        DataType::Bigint => "bigint",
        DataType::Double => "double",
        DataType::Varchar => "varchar",
        DataType::Date => "date",
        DataType::Timestamp => "timestamp",
        DataType::Interval => "interval",
        DataType::Decimal { .. } => unreachable!("handled above"),
    }
    .to_string()
}

/// [`dtype_name`] in reverse; `None` for an unknown name.
pub(crate) fn dtype_from_name(name: &str) -> Option<DataType> {
    if let Some(rest) = name.strip_prefix("decimal(") {
        let (p, s) = rest.strip_suffix(')')?.split_once(',')?;
        let precision: u8 = p.parse().ok()?;
        let scale: u8 = s.parse().ok()?;
        return pinkdb::types::decimal_type(precision, scale);
    }
    Some(match name {
        "boolean" => DataType::Boolean,
        "integer" => DataType::Integer,
        "bigint" => DataType::Bigint,
        "double" => DataType::Double,
        "varchar" => DataType::Varchar,
        "date" => DataType::Date,
        "timestamp" => DataType::Timestamp,
        "interval" => DataType::Interval,
        _ => return None,
    })
}

/// Canonical textual encoding of one min/max statistic (see the module
/// docs). `None` when the value's shape does not match `dtype` — a writer bug
/// that must never become a wrong answer.
pub(crate) fn encode_value(dtype: DataType, v: &Value) -> Option<String> {
    Some(match (dtype, v) {
        (DataType::Boolean, Value::Boolean(b)) => b.to_string(),
        (DataType::Integer, Value::Integer(i)) => i.to_string(),
        (DataType::Bigint, Value::Bigint(i)) => i.to_string(),
        (DataType::Date, Value::Date(d)) => d.to_string(),
        (DataType::Timestamp, Value::Timestamp(t)) => t.to_string(),
        // Bit-exact: JSON has no NaN/±INF and `-0.0` must stay distinguishable.
        (DataType::Double, Value::Double(x)) => format!("{:016x}", x.to_bits()),
        (DataType::Varchar, Value::Varchar(s)) => s.clone(),
        (DataType::Decimal { scale, .. }, Value::Decimal(x, s)) if *s == scale => {
            pinkdb::types::format_decimal(*x, *s)
        }
        (
            DataType::Interval,
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => pinkdb::types::format_interval(*months, *days, *micros),
        _ => return None,
    })
}

/// [`encode_value`] in reverse; `None` for anything malformed.
pub(crate) fn decode_value(dtype: DataType, s: &str) -> Option<Value> {
    Some(match dtype {
        DataType::Boolean => match s {
            "false" => Value::Boolean(false),
            "true" => Value::Boolean(true),
            _ => return None,
        },
        DataType::Integer => Value::Integer(s.parse().ok()?),
        DataType::Bigint => Value::Bigint(s.parse().ok()?),
        DataType::Date => Value::Date(s.parse().ok()?),
        DataType::Timestamp => Value::Timestamp(s.parse().ok()?),
        DataType::Double => {
            if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
                return None;
            }
            Value::Double(f64::from_bits(u64::from_str_radix(s, 16).ok()?))
        }
        DataType::Varchar => Value::Varchar(s.to_string()),
        DataType::Decimal { precision, scale } => Value::Decimal(
            pinkdb::types::parse_decimal_str(s, precision, scale).ok()?,
            scale,
        ),
        DataType::Interval => {
            let iv = pinkdb::types::parse_interval(s)?;
            Value::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn saved(dtype: DataType, min: Option<Value>, max: Option<Value>) -> SavedTableStats {
        SavedTableStats {
            db: "main".to_string(),
            table: "t".to_string(),
            rows: min.iter().chain(max.iter()).count() as u64,
            columns: vec![pinkdb::persist::SavedColumnStats {
                name: "c".to_string(),
                dtype,
                null_count: 0,
                min,
                max,
                nan_count: 0,
            }],
        }
    }

    /// Every type's min/max survives the block encoding bit-exactly,
    /// NaN payloads and all.
    #[test]
    fn value_encoding_roundtrips_every_type() {
        let cases: Vec<(DataType, Value)> = vec![
            (DataType::Boolean, Value::Boolean(false)),
            (DataType::Boolean, Value::Boolean(true)),
            (DataType::Integer, Value::Integer(i32::MIN)),
            (DataType::Integer, Value::Integer(i32::MAX)),
            (DataType::Bigint, Value::Bigint(i64::MIN)),
            (DataType::Bigint, Value::Bigint(i64::MAX)),
            (DataType::Date, Value::Date(-19_000)),
            (DataType::Timestamp, Value::Timestamp(i64::MAX)),
            (DataType::Double, Value::Double(-0.0)),
            (DataType::Double, Value::Double(f64::INFINITY)),
            (DataType::Double, Value::Double(f64::NEG_INFINITY)),
            (
                DataType::Double,
                Value::Double(f64::from_bits(0x7ff8_0000_dead_beef)),
            ),
            (
                DataType::Double,
                Value::Double(f64::from_bits(0xfff8_0000_0000_0001)),
            ),
            (DataType::Varchar, Value::Varchar(String::new())),
            (
                DataType::Varchar,
                Value::Varchar("héllo 🌍\0.. %2E".to_string()),
            ),
        ];
        for (dtype, v) in cases {
            let s = encode_value(dtype, &v).unwrap();
            let back = decode_value(dtype, &s).unwrap();
            match (&v, &back) {
                (Value::Double(x), Value::Double(y)) => {
                    assert_eq!(x.to_bits(), y.to_bits(), "{dtype:?} {s}")
                }
                _ => assert_eq!(v, back, "{dtype:?} {s}"),
            }
        }
        // Shape mismatches are refused, never coerced.
        assert_eq!(encode_value(DataType::Integer, &Value::Bigint(1)), None);
        assert_eq!(decode_value(DataType::Boolean, "1"), None);
        assert_eq!(decode_value(DataType::Double, "0xg"), None);
        assert_eq!(decode_value(DataType::Integer, "1.5"), None);
    }

    /// The gate: doubles need an exact zero NaN count and a nonzero
    /// statistic; everything else passes through.
    #[test]
    fn gate_refuses_nan_and_zero_doubles_only() {
        let v = |x: f64| Some(Value::Double(x));
        let s = serve_column_stats(DataType::Double, Some(1), v(1.5), v(3.25), Some(0));
        assert_eq!(s.min, v(1.5));
        assert_eq!(s.max, v(3.25));
        // Any NaN refuses both statistics.
        let s = serve_column_stats(DataType::Double, Some(1), v(1.5), v(3.25), Some(1));
        assert_eq!(s.min, None);
        assert_eq!(s.max, None);
        // A missing NaN count is unproven.
        let s = serve_column_stats(DataType::Double, Some(1), v(1.5), v(3.25), None);
        assert_eq!(s.min, None);
        // Zero statistics are refused (-0.0 by the same check).
        let s = serve_column_stats(DataType::Double, Some(1), v(-0.0), v(3.25), Some(0));
        assert_eq!(s.min, None);
        assert_eq!(s.max, v(3.25));
        let s = serve_column_stats(DataType::Double, Some(1), v(-1.0), v(0.0), Some(0));
        assert_eq!(s.min, v(-1.0));
        assert_eq!(s.max, None);
        // ±INF is a normal value for the comparator.
        let s = serve_column_stats(
            DataType::Double,
            Some(1),
            v(f64::NEG_INFINITY),
            v(f64::INFINITY),
            Some(0),
        );
        assert_eq!(s.min, v(f64::NEG_INFINITY));
        assert_eq!(s.max, v(f64::INFINITY));
        // Non-double types pass through as proven.
        let s = serve_column_stats(
            DataType::Integer,
            Some(1),
            Some(Value::Integer(-5)),
            Some(Value::Integer(9)),
            None,
        );
        assert_eq!(s.min, Some(Value::Integer(-5)));
        assert_eq!(s.max, Some(Value::Integer(9)));
    }

    /// The block round-trips through JSON, seals, and verifies; any edit to
    /// a covered field breaks the CRC.
    #[test]
    fn block_crc_detects_every_edit() {
        let mut block = StatsBlock::from_saved(
            "main.t",
            "data/main.t/x.pink",
            &saved(
                DataType::Integer,
                Some(Value::Integer(-5)),
                Some(Value::Integer(9)),
            ),
        )
        .unwrap();
        assert!(block.verify_crc());
        let json = serde_json::to_string(&block).unwrap();
        let parsed: StatsBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, block);
        assert!(parsed.verify_crc());

        let (schema, stats) = block
            .schema_and_stats(std::path::Path::new("catalog.json"))
            .unwrap();
        assert_eq!(schema, vec![("c".to_string(), DataType::Integer)]);
        assert_eq!(stats[0].min, Some(Value::Integer(-5)));
        assert_eq!(stats[0].max, Some(Value::Integer(9)));

        // Every covered field is protected.
        let mut tampered = block.clone();
        tampered.columns[0].min = Some("0".to_string());
        assert!(!tampered.verify_crc());
        let mut tampered = block.clone();
        tampered.table = "main.other".to_string();
        assert!(!tampered.verify_crc());
        let mut tampered = block.clone();
        tampered.file = "data/main.t/y.pink".to_string();
        assert!(!tampered.verify_crc());
        let mut tampered = block.clone();
        tampered.rows += 1;
        assert!(!tampered.verify_crc());
        let mut tampered = block.clone();
        tampered.columns[0].name = "other".to_string();
        assert!(!tampered.verify_crc());
        // Whitespace-only edits (the manifest is re-parsed) do not matter.
        let respaced = json.replace(':', ": ");
        let parsed: StatsBlock = serde_json::from_str(&respaced).unwrap();
        assert!(parsed.verify_crc());

        // A corrupted block is refused with a clear message.
        block.crc32 ^= 1;
        let err = block
            .schema_and_stats(std::path::Path::new("catalog.json"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("failed its checksum"), "{err}");
        assert!(err.contains(".lake open"), "{err}");
    }

    /// Malformed block contents are clean errors; a future block version is
    /// refused rather than half-read.
    #[test]
    fn malformed_blocks_are_refused() {
        let make = || {
            StatsBlock::from_saved(
                "main.t",
                "data/main.t/x.pink",
                &saved(
                    DataType::Varchar,
                    Some(Value::Varchar("a".to_string())),
                    Some(Value::Varchar("z".to_string())),
                ),
            )
            .unwrap()
        };
        let path = std::path::Path::new("snapshots/2.json");

        let mut b = make();
        b.nulls_break();
        let err = b.schema_and_stats(path).unwrap_err().to_string();
        assert!(err.contains("NULLs in"), "{err}");

        let mut b = make();
        b.columns[0].max = None;
        b.seal();
        let err = b.schema_and_stats(path).unwrap_err().to_string();
        assert!(err.contains("only one of min/max"), "{err}");

        let mut b = make();
        b.columns[0].dtype = "clob".to_string();
        b.seal();
        let err = b.schema_and_stats(path).unwrap_err().to_string();
        assert!(err.contains("unknown type"), "{err}");

        let mut b = make();
        b.columns[0].min = Some("not-an-int".to_string());
        b.columns[0].dtype = "integer".to_string();
        b.seal();
        let err = b.schema_and_stats(path).unwrap_err().to_string();
        assert!(err.contains("undecodable"), "{err}");

        let mut b = make();
        b.version = 99;
        b.seal();
        let err = b.schema_and_stats(path).unwrap_err().to_string();
        assert!(err.contains("statistics block version 99"), "{err}");
        assert!(err.contains(".lake open"), "{err}");
    }

    impl StatsBlock {
        /// Test helper: make `nulls` exceed `rows` (then reseal).
        fn nulls_break(&mut self) {
            self.rows = 1;
            self.columns[0].nulls = 2;
            self.seal();
        }
    }
}
