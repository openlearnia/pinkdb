//! Per-file zone maps and file pruning (wave 66; the deferred lake-scan
//! stage 3).
//!
//! Every data file a v4 lake publishes carries a **zone map**: the exact
//! per-column `(null count, min, max)` facts of the rows it holds. For a
//! `.pink` file those facts ride in the CRC-protected per-file statistics
//! block the manifest already publishes; for a Vortex file they are
//! published in the same catalog block (cross-checked against the file's own
//! footer at attach). A file written before wave 66 simply has no catalog
//! zone map and can never be pruned — never a guess.
//!
//! A scan with simple `WHERE` conjuncts may then **skip whole data files
//! without decoding them**: a file is pruned when the zone map proves no row
//! can satisfy a conjunct. That proof is one-directional and conservative: a
//! statistic that is unknown (a gated DOUBLE min/max, an absent NULL count, a
//! file without a zone map) never prunes; a file is only skipped when every
//! value the map allows fails the predicate.
//!
//! # Positional deletes
//!
//! The wave-63 exactness rule still holds for the **table-level** statistics
//! that answer `COUNT`/`MIN`/`MAX`: once a table has positional delete files
//! its per-column facts become unknown and those aggregates materialize. A
//! file's zone map, however, stays a valid **over-approximation** after
//! deletes, because deleting rows only shrinks the value set: if no
//! pre-delete row could satisfy a predicate, no surviving row can. Pruning
//! therefore keeps using a file's zone map (and its partition values) even
//! when the file has delete files; the aggregates keep falling back. See
//! [`file_prunes`].
//!
//! # Partitions
//!
//! A file written under an identity partition key has an exact scalar fact
//! for that column — every row holds the same value (or NULL) — which is
//! stronger than its min/max zone. [`zone_for_predicate`] prefers that fact
//! when the file records one, and [`cross_check_partition_value`] verifies at
//! attach that the recorded value agrees with the file's zone map.

use std::collections::BTreeMap;

use pinkdb::{ColumnStats, DataType, PruneOp, PrunePredicate, Value};

use crate::catalog::PartitionKey;
use crate::stats;

/// Everything pruning needs to know about one data file.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FileFacts<'a> {
    /// The file's exact row count (pre-delete).
    pub(crate) rows: u64,
    /// The file's per-column zone map, in schema order, when it has one.
    pub(crate) zone: Option<&'a [ColumnStats]>,
    /// The file's recorded partition values, keyed by
    /// [`PartitionKey::key_text`]; `None` (the map value) is SQL NULL.
    pub(crate) partition: Option<&'a BTreeMap<String, Option<String>>>,
}

/// The partition label of one row value under one key: the identity value
/// itself, or the bucket number of a `bucket(N, column)` key. `Some(None)` is
/// SQL NULL; `None` means the value cannot be labeled (a shape mismatch —
/// the writer refuses the split rather than guessing).
pub(crate) fn partition_label(
    key: &PartitionKey,
    dtype: DataType,
    v: &Value,
) -> Option<Option<String>> {
    if key.is_identity() {
        return partition_value(dtype, v);
    }
    let n = key.bucket()?;
    if v.is_null() {
        return Some(None);
    }
    let text = stats::encode_value(dtype, v)?;
    Some(Some((fnv1a64(text.as_bytes()) % n as u64).to_string()))
}

/// FNV-1a, 64-bit: the stable hash behind `bucket(N, column)` labels.
///
/// Deliberately pinkdb's own function, not DuckDB's: DuckLake records the
/// bucket *number* in its metadata, while this lake records it per file too —
/// the numbers must be stable across builds of pinkdb (a re-opened lake is
/// pruned/split by the same function), which a specified FNV hash guarantees.
/// The bucket transform is only written by the writer that split the rows, so
/// no other engine needs to reproduce the number.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

/// The canonical partition value of one row value under `dtype`: `Some(None)`
/// for SQL NULL, `Some(Some(text))` for a value the statistics encoding can
/// represent, `None` when the value's shape does not match its column (a
/// writer bug that must never become a wrong pruning fact).
pub(crate) fn partition_value(dtype: DataType, v: &Value) -> Option<Option<String>> {
    if v.is_null() {
        return Some(None);
    }
    stats::encode_value(dtype, v).map(Some)
}

/// The exact zone map a partition value proves for one column of `rows`
/// rows: all rows hold that value, or all are NULL. `None` when the recorded
/// text does not decode as the column's type (never a guess).
pub(crate) fn partition_zone(
    rows: u64,
    value: &Option<String>,
    dtype: DataType,
) -> Option<ColumnStats> {
    match value {
        None => Some(ColumnStats {
            null_count: Some(rows),
            min: None,
            max: None,
        }),
        Some(text) => {
            let v = stats::decode_value(dtype, text)?;
            Some(ColumnStats {
                null_count: Some(0),
                min: Some(v.clone()),
                max: Some(v),
            })
        }
    }
}

/// The schema column index whose name matches `name` case-insensitively
/// (the engine's catalog rules).
pub(crate) fn schema_column_index(schema: &[(String, DataType)], name: &str) -> Option<usize> {
    schema
        .iter()
        .position(|(n, _)| n.eq_ignore_ascii_case(name))
}

/// The zone a predicate on `column` should be evaluated against: the exact
/// scalar a matching **identity** partition value proves when the file
/// records one (stronger than min/max), else the file's zone map column.
/// `None` when neither is available — no pruning.
pub(crate) fn zone_for_predicate(
    facts: FileFacts<'_>,
    keys: &[PartitionKey],
    schema: &[(String, DataType)],
    column: usize,
) -> Option<ColumnStats> {
    let (name, dtype) = schema.get(column)?;
    if let Some(partition) = facts.partition {
        for key in keys.iter().filter(|k| k.is_identity()) {
            if !key.column.eq_ignore_ascii_case(name) {
                continue;
            }
            if let Some(value) = partition.get(&key.key_text())
                && let Some(zone) = partition_zone(facts.rows, value, *dtype)
            {
                return Some(zone);
            }
        }
    }
    facts.zone.and_then(|zone| zone.get(column)).cloned()
}

/// True when the file's facts prove **no** row of the file can satisfy
/// `predicate`; the scan may then skip the whole file, delete files included
/// (deletes only remove rows, so a pre-delete proof stays valid — unlike the
/// table-level aggregates, which fall back).
///
/// Conservative by construction: an unknown zone, a column/dtype mismatch, a
/// malformed partition value, or a statistic the predicate cannot use all
/// return `false` (decode the file).
pub(crate) fn predicate_prunes(
    facts: FileFacts<'_>,
    keys: &[PartitionKey],
    schema: &[(String, DataType)],
    predicate: &PrunePredicate,
) -> bool {
    let Some((_, column_dtype)) = schema.get(predicate.column) else {
        return false;
    };
    if let Some(value) = &predicate.value {
        match value.data_type() {
            Some(literal_dtype) if comparable(*column_dtype, literal_dtype) => {}
            _ => return false,
        }
    }
    if facts.rows == 0 {
        return true;
    }
    let Some(zone) = zone_for_predicate(facts, keys, schema, predicate.column) else {
        return false;
    };
    zone_prunes(&zone, facts.rows, predicate.op, predicate.value.as_ref())
}

/// True when **any** predicate proves the file cannot contribute a matching
/// row (the `WHERE` keeps only rows satisfying every conjunct, so one
/// unsatisfiable conjunct empties the file).
pub(crate) fn file_prunes(
    facts: FileFacts<'_>,
    keys: &[PartitionKey],
    schema: &[(String, DataType)],
    predicates: &[PrunePredicate],
) -> bool {
    predicates
        .iter()
        .any(|p| predicate_prunes(facts, keys, schema, p))
}

/// The core zone-map test for one column: given what the file proved about a
/// column, decide whether `col op literal` can match no row.
///
/// `rows` is the file's exact row count; `zone.null_count == Some(rows)`
/// means the column is all-NULL (so only `IS NULL` can match) and an unknown
/// NULL count only disables the NULL-dependent tests. Comparisons mirror the
/// executor's own comparator (`pinkdb::plan::compare_values`), so a pruned
/// file's rows could only have failed the filter anyway.
pub(crate) fn zone_prunes(
    zone: &ColumnStats,
    rows: u64,
    op: PruneOp,
    literal: Option<&Value>,
) -> bool {
    let all_null = zone.null_count == Some(rows);
    match op {
        PruneOp::IsNull => zone.null_count == Some(0),
        PruneOp::IsNotNull => all_null,
        PruneOp::Eq => {
            let Some(literal) = literal else {
                return false;
            };
            if all_null {
                return true;
            }
            if let Some(min) = &zone.min
                && pinkdb::plan::compare_values(literal, min).is_lt()
            {
                return true;
            }
            if let Some(max) = &zone.max
                && pinkdb::plan::compare_values(literal, max).is_gt()
            {
                return true;
            }
            false
        }
        PruneOp::NotEq => {
            let Some(literal) = literal else {
                return false;
            };
            // Only an exact single-value file (min == max == literal) proves
            // no row differs; rows with NULL satisfy nothing either.
            match (&zone.min, &zone.max) {
                (Some(min), Some(max)) => {
                    pinkdb::plan::compare_values(min, literal).is_eq()
                        && pinkdb::plan::compare_values(max, literal).is_eq()
                }
                _ => false,
            }
        }
        PruneOp::Lt => {
            let Some(literal) = literal else {
                return false;
            };
            if all_null {
                return true;
            }
            zone.min
                .as_ref()
                .is_some_and(|min| !pinkdb::plan::compare_values(min, literal).is_lt())
        }
        PruneOp::LtEq => {
            let Some(literal) = literal else {
                return false;
            };
            if all_null {
                return true;
            }
            zone.min
                .as_ref()
                .is_some_and(|min| pinkdb::plan::compare_values(min, literal).is_gt())
        }
        PruneOp::Gt => {
            let Some(literal) = literal else {
                return false;
            };
            if all_null {
                return true;
            }
            zone.max
                .as_ref()
                .is_some_and(|max| !pinkdb::plan::compare_values(max, literal).is_gt())
        }
        PruneOp::GtEq => {
            let Some(literal) = literal else {
                return false;
            };
            if all_null {
                return true;
            }
            zone.max
                .as_ref()
                .is_some_and(|max| pinkdb::plan::compare_values(max, literal).is_lt())
        }
    }
}

/// [`crate::plan::prunable_types`]'s lake-side twin: the literal must be
/// comparable to the column's statistics under `compare_values`'s semantics
/// (exact type, or the numeric family).
fn comparable(column_dtype: DataType, literal_dtype: DataType) -> bool {
    if column_dtype == literal_dtype {
        return true;
    }
    let numeric = |t: DataType| {
        matches!(
            t,
            DataType::Integer | DataType::Bigint | DataType::Double | DataType::Decimal { .. }
        )
    };
    numeric(column_dtype) && numeric(literal_dtype)
}

/// Verify at attach that a file's recorded identity partition value agrees
/// with its zone map: `Err(message)` when it cannot (a misplaced file, or
/// tampered metadata). `Ok(())` when either fact is unknown — this is a
/// consistency check, not a re-derivation.
pub(crate) fn cross_check_partition_value(
    rows: u64,
    zone: Option<&ColumnStats>,
    value: &Option<String>,
    dtype: DataType,
) -> std::result::Result<(), String> {
    let Some(zone) = zone else {
        return Ok(());
    };
    match value {
        None => {
            // Every row must be NULL.
            if let Some(nulls) = zone.null_count
                && nulls != rows
            {
                return Err(format!(
                    "the file is recorded as all-NULL under its partition value, but its \
                         zone map proves {nulls} NULLs in {rows} rows"
                ));
            }
            if zone.min.is_some() || zone.max.is_some() {
                return Err(
                    "the file is recorded as all-NULL under its partition value, but its \
                     zone map carries a min/max"
                        .to_string(),
                );
            }
            Ok(())
        }
        Some(text) => {
            let Some(v) = stats::decode_value(dtype, text) else {
                return Err(format!(
                    "the recorded partition value {text:?} does not decode as {dtype}"
                ));
            };
            if let Some(nulls) = zone.null_count
                && nulls != 0
            {
                return Err(format!(
                    "the file is recorded under partition value {text:?}, but its zone \
                         map proves {nulls} NULLs"
                ));
            }
            for (what, stat) in [("min", &zone.min), ("max", &zone.max)] {
                if let Some(stat) = stat
                    && !pinkdb::plan::compare_values(stat, &v).is_eq()
                {
                    return Err(format!(
                        "the file is recorded under partition value {text:?}, but its \
                             zone map {what} is {stat:?}"
                    ));
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::PrunePredicate;

    fn zone(nulls: Option<u64>, min: Option<Value>, max: Option<Value>) -> ColumnStats {
        ColumnStats {
            null_count: nulls,
            min,
            max,
        }
    }

    fn keys(texts: &[&str]) -> Vec<PartitionKey> {
        texts
            .iter()
            .map(|t| PartitionKey::parse(t).unwrap())
            .collect()
    }

    fn schema() -> Vec<(String, DataType)> {
        vec![
            ("i".to_string(), DataType::Integer),
            ("b".to_string(), DataType::Varchar),
        ]
    }

    #[test]
    fn zone_tests_prune_only_what_they_prove() {
        let z = zone(Some(0), Some(Value::Integer(10)), Some(Value::Integer(20)));
        let lit = |i: i32| Value::Integer(i);
        // Eq outside [min, max] prunes; inside does not.
        assert!(zone_prunes(&z, 5, PruneOp::Eq, Some(&lit(9))));
        assert!(zone_prunes(&z, 5, PruneOp::Eq, Some(&lit(21))));
        assert!(!zone_prunes(&z, 5, PruneOp::Eq, Some(&lit(10))));
        assert!(!zone_prunes(&z, 5, PruneOp::Eq, Some(&lit(15))));
        // Ranges.
        assert!(zone_prunes(&z, 5, PruneOp::Lt, Some(&lit(10))));
        assert!(!zone_prunes(&z, 5, PruneOp::Lt, Some(&lit(11))));
        assert!(zone_prunes(&z, 5, PruneOp::LtEq, Some(&lit(9))));
        assert!(zone_prunes(&z, 5, PruneOp::Gt, Some(&lit(20))));
        assert!(!zone_prunes(&z, 5, PruneOp::Gt, Some(&lit(19))));
        assert!(zone_prunes(&z, 5, PruneOp::GtEq, Some(&lit(21))));
        // NotEq prunes only an exact single-value file.
        assert!(!zone_prunes(&z, 5, PruneOp::NotEq, Some(&lit(10))));
        let exact = zone(Some(0), Some(Value::Integer(10)), Some(Value::Integer(10)));
        assert!(zone_prunes(&exact, 5, PruneOp::NotEq, Some(&lit(10))));
        assert!(!zone_prunes(&exact, 5, PruneOp::NotEq, Some(&lit(11))));
        // IS NULL / IS NOT NULL need an exact NULL count.
        assert!(zone_prunes(&z, 5, PruneOp::IsNull, None));
        assert!(!zone_prunes(&z, 5, PruneOp::IsNotNull, None));
        let unknown = ColumnStats::unknown();
        assert!(!zone_prunes(&unknown, 5, PruneOp::IsNull, None));
        assert!(!zone_prunes(&unknown, 5, PruneOp::IsNotNull, None));
        assert!(!zone_prunes(&unknown, 5, PruneOp::Eq, Some(&lit(1))));
        // An all-NULL file: only IS NULL can match.
        let all_null = zone(Some(5), None, None);
        assert!(zone_prunes(&all_null, 5, PruneOp::IsNotNull, None));
        assert!(zone_prunes(&all_null, 5, PruneOp::Eq, Some(&lit(1))));
        assert!(zone_prunes(&all_null, 5, PruneOp::Lt, Some(&lit(1))));
        assert!(!zone_prunes(&all_null, 5, PruneOp::IsNull, None));
        // A value only bounded on one side prunes only from that side.
        let half = zone(Some(0), None, Some(Value::Integer(20)));
        assert!(zone_prunes(&half, 5, PruneOp::Gt, Some(&lit(20))));
        assert!(!zone_prunes(&half, 5, PruneOp::Lt, Some(&lit(1))));
        // Zero rows: an empty file can never match (the caller checks it).
        let empty = FileFacts {
            rows: 0,
            zone: None,
            partition: None,
        };
        assert!(predicate_prunes(
            empty,
            &[],
            &schema(),
            &PrunePredicate::compare(0, PruneOp::Eq, lit(1)),
        ));
    }

    #[test]
    fn partition_zones_are_exact_scalars() {
        let b = DataType::Varchar;
        let z = partition_zone(3, &Some("x".to_string()), b).unwrap();
        assert_eq!(z.null_count, Some(0));
        assert_eq!(z.min, Some(Value::Varchar("x".to_string())));
        assert_eq!(z.max, Some(Value::Varchar("x".to_string())));
        let z = partition_zone(3, &None, b).unwrap();
        assert_eq!(z.null_count, Some(3));
        assert_eq!(z.min, None);
        // An undecodable value is no fact at all.
        assert!(partition_zone(3, &Some("not-an-int".to_string()), DataType::Integer).is_none());
    }

    #[test]
    fn partition_values_prune_by_identity_and_survive_unknown_zones() {
        let s = schema();
        let k = keys(&["b"]);
        let mut partition = BTreeMap::new();
        partition.insert("b".to_string(), Some("x".to_string()));
        let facts = FileFacts {
            rows: 4,
            zone: None, // no zone map at all: the partition value still prunes
            partition: Some(&partition),
        };
        let eq = PrunePredicate::compare(1, PruneOp::Eq, Value::Varchar("x".to_string()));
        assert!(!predicate_prunes(facts, &k, &s, &eq));
        let other = PrunePredicate::compare(1, PruneOp::Eq, Value::Varchar("y".to_string()));
        assert!(predicate_prunes(facts, &k, &s, &other));
        let is_null = PrunePredicate::null_test(1, false);
        assert!(predicate_prunes(facts, &k, &s, &is_null));
        let not_null = PrunePredicate::null_test(1, true);
        assert!(!predicate_prunes(facts, &k, &s, &not_null));
        // A predicate on a column the partition does not cover uses no zone.
        let on_i = PrunePredicate::compare(0, PruneOp::Eq, Value::Integer(1));
        assert!(!predicate_prunes(facts, &k, &s, &on_i));
        // Without the spec (cleared), the recorded value is not interpreted.
        assert!(!predicate_prunes(facts, &[], &s, &other));
        // A NULL partition value serves IS NULL only.
        let mut null_partition = BTreeMap::new();
        null_partition.insert("b".to_string(), None);
        let facts = FileFacts {
            rows: 4,
            zone: None,
            partition: Some(&null_partition),
        };
        assert!(predicate_prunes(facts, &k, &s, &not_null));
        assert!(!predicate_prunes(facts, &k, &s, &is_null));
    }

    #[test]
    fn cross_check_finds_misplaced_partition_values() {
        let b = DataType::Varchar;
        let v = || Some("x".to_string());
        // Consistent: min = max = "x", no NULLs.
        let z = zone(
            Some(0),
            Some(Value::Varchar("x".to_string())),
            Some(Value::Varchar("x".to_string())),
        );
        assert!(cross_check_partition_value(2, Some(&z), &v(), b).is_ok());
        // Wrong value.
        let z = zone(
            Some(0),
            Some(Value::Varchar("y".to_string())),
            Some(Value::Varchar("y".to_string())),
        );
        assert!(cross_check_partition_value(2, Some(&z), &v(), b).is_err());
        // NULLs under a non-NULL partition value.
        let z = zone(Some(1), Some(Value::Varchar("x".to_string())), None);
        assert!(cross_check_partition_value(2, Some(&z), &v(), b).is_err());
        // All-NULL file with a NULL partition value.
        let z = zone(Some(2), None, None);
        assert!(cross_check_partition_value(2, Some(&z), &None, b).is_ok());
        assert!(cross_check_partition_value(2, Some(&z), &v(), b).is_err());
        // No zone map (or unknown NULL count): nothing to check.
        assert!(cross_check_partition_value(2, None, &v(), b).is_ok());
        let z = zone(None, None, None);
        assert!(cross_check_partition_value(2, Some(&z), &v(), b).is_ok());
        // Undecodable recorded value.
        assert!(
            cross_check_partition_value(2, Some(&z), &Some("nope".to_string()), DataType::Integer)
                .is_err()
        );
    }
}
