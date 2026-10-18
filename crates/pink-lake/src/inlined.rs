//! Inlined small writes (wave 68, catalog additive to `format_version` 4/5):
//! the write-side and read-side of [`crate::catalog::InlinedRow`].
//!
//! # What inlining is
//!
//! A commit whose appended row count is at most the table's
//! `data_inlining_row_limit` ([`crate::catalog::TableEntry::inlined_limit`],
//! else the lake handle's default — DuckLake's
//! `ducklake_default_data_inlining_row_limit`, 10 rows; 0 disables) stores the
//! rows **in the catalog manifest** instead of writing a data file, exactly
//! like DuckLake's `ducklake_inlined_data_<table>` metadata table. Every scan
//! of such a table reads its data files (in manifest order, minus positional
//! deletes) and then unions the **live** inlined rows (row-id order) after
//! them — this module owns that union and everything that decodes an inlined
//! record.
//!
//! # Row order
//!
//! The documented scan order of a wave-68 table is: **data files in manifest
//! order first, then live inlined rows in row-id order**. Both readers (attach
//! and `.lake open`) produce it, and the writable attachment's diff base
//! reproduces it, so every reader of one version agrees on row positions.
//! Inlined rows are never pruned by the zone-map/partition filter — they
//! always scan (or their small in-memory predicates are evaluated by the
//! executor); file pruning is unaffected because inlined rows are not files.
//!
//! # Values
//!
//! A record's `values` are canonically encoded with [`crate::stats`]
//! (`encode_value`/`decode_value`: bit-exact doubles, exact decimals and
//! intervals, JSON strings for VARCHAR) and positional against the record's
//! `columns` — the logical column names **as the row was written**. Decoding
//! against a later schema uses the same mapping per-file schemas use
//! ([`crate::schema::TableSchema::physical_name`]): a column added after the
//! row reads its DEFAULT, a rename walks back to the physical name, and a
//! dropped-then-re-added column (a fresh identity) never mis-reads old bytes.
//!
//! # Statistics
//!
//! Inlined rows are fully known, so they contribute **exact** facts: NULL
//! counts add up and MIN/MAX fold with the file statistics (an all-NULL
//! contribution adds no extreme, it does not make the fold unknown). The
//! table-level gate stays exactly where it was: any positional delete over a
//! *file* still makes that file's physical facts unknown, and a `.pink` block
//! without a NaN count still refuses DOUBLE extremes. `rows` (logical) stays
//! `files - deletes + live inlined`, so the statistics-only `COUNT(*)` fast
//! path is exact with inlined rows present.

use std::sync::Arc;

use pinkdb::{DataTable, DataType, Value};

use crate::catalog::InlinedRow;
use crate::schema::TableSchema;
use crate::stats::{decode_value, encode_value};
use crate::{LakeError, Result};

/// The encoded values of one table row, positional against the table's
/// columns: `None` is a SQL NULL.
pub(crate) fn encode_row(
    row: &[Value],
    dtypes: &[DataType],
    key: &str,
) -> Result<Vec<Option<String>>> {
    if row.len() != dtypes.len() {
        return Err(LakeError::Internal {
            message: format!(
                "table \"{key}\": a row holds {} values for {} columns",
                row.len(),
                dtypes.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(row.len());
    for (value, dtype) in row.iter().zip(dtypes) {
        out.push(match value {
            Value::Null => None,
            value => Some(
                encode_value(*dtype, value).ok_or_else(|| LakeError::Internal {
                    message: format!(
                        "table \"{key}\": value {value:?} does not match its {dtype:?} column"
                    ),
                })?,
            ),
        });
    }
    Ok(out)
}

/// Build the inlined records for `rows` appended to one table, assigning row
/// ids from `first_row_id`. `columns` are the names as written (the appended
/// table's schema), `schema_version` the epoch they were written under and
/// `begin` the version publishing them.
pub(crate) fn records_for_append(
    rows: &DataTable,
    columns: &[String],
    dtypes: &[DataType],
    first_row_id: u64,
    begin: u64,
    schema_version: u64,
    key: &str,
) -> Result<Vec<InlinedRow>> {
    let mut out = Vec::with_capacity(rows.num_rows());
    let mut values = vec![Value::Null; columns.len()];
    for r in 0..rows.num_rows() {
        for (c, slot) in values.iter_mut().enumerate() {
            *slot = rows.get(r, c).unwrap_or(Value::Null);
        }
        out.push(InlinedRow {
            row_id: first_row_id.checked_add(out.len() as u64).ok_or_else(|| {
                LakeError::Internal {
                    message: format!("table \"{key}\": row ids overflow u64"),
                }
            })?,
            begin,
            end: None,
            flushed: false,
            schema_version,
            columns: columns.to_vec(),
            values: encode_row(&values, dtypes, key)?,
        });
    }
    Ok(out)
}

/// Decode one inlined row version against `record` — the table's logical
/// schema at the version being read. Columns the row predates (or a fresh
/// identity that replaced a dropped one) read their DEFAULT (or NULL).
pub(crate) fn decode_row(
    record: &TableSchema,
    row: &InlinedRow,
    key: &str,
    manifest_path: &std::path::Path,
) -> Result<Vec<Value>> {
    if row.values.len() != row.columns.len() {
        return Err(LakeError::Corrupt {
            path: manifest_path.to_path_buf(),
            message: format!(
                "table \"{key}\": inlined row {} holds {} values for {} columns",
                row.row_id,
                row.values.len(),
                row.columns.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(record.columns.len());
    for (i, col) in record.columns.iter().enumerate() {
        let value = match record.physical_name(i, row.schema_version) {
            None => None,
            Some(physical) => match row
                .columns
                .iter()
                .position(|name| name.eq_ignore_ascii_case(physical))
            {
                Some(p) => match &row.values[p] {
                    None => None,
                    Some(encoded) => Some(decode_value(col.dtype, encoded).ok_or_else(|| {
                        LakeError::Corrupt {
                            path: manifest_path.to_path_buf(),
                            message: format!(
                                "table \"{key}\": inlined row {} column \"{}\" carries an \
                                 undecodable {} value {encoded:?}",
                                row.row_id, col.name, col.dtype
                            ),
                        }
                    })?),
                },
                // The row was written before the (identity of the) column:
                // its default materializes, never old bytes.
                None => None,
            },
        };
        out.push(value.unwrap_or_else(|| col.default.clone().unwrap_or(Value::Null)));
    }
    Ok(out)
}

/// Materialize the entry's **live** inlined rows as one table in `record`'s
/// logical schema, unioned as one extra block after the data files.
///
/// `wanted` marks the requested logical columns (`None` = all): a column
/// outside the request is filled with NULLs and reported `present = false`,
/// chunk-aligned with the decoded ones — exactly the placeholder contract
/// `AttachedLake::load_projected` gives a projected data-file decode, so a
/// later per-column decode can merge over it.
pub(crate) fn materialize_live(
    live: &[InlinedRow],
    record: &TableSchema,
    key: &str,
    table_name: &str,
    wanted: Option<&[bool]>,
    manifest_path: &std::path::Path,
) -> Result<(DataTable, Vec<bool>)> {
    let wanted: Vec<bool> = match wanted {
        Some(w) if w.len() == record.columns.len() => w.to_vec(),
        _ => vec![true; record.columns.len()],
    };
    let mut decoded: Vec<Vec<Value>> = Vec::with_capacity(live.len());
    for row in live {
        decoded.push(decode_row(record, row, key, manifest_path)?);
    }
    let mut columns = Vec::with_capacity(record.columns.len());
    let mut present = Vec::with_capacity(record.columns.len());
    for (i, col) in record.columns.iter().enumerate() {
        let mut vector = pinkdb::Vector::with_capacity(col.dtype, decoded.len());
        for values in &decoded {
            let value = &values[i];
            if wanted[i] {
                match value {
                    Value::Null => vector.push_null().map_err(|e| LakeError::Internal {
                        message: format!("materializing inlined NULL of \"{key}\": {e}"),
                    })?,
                    value => vector
                        .push_valid(value.clone())
                        .map_err(|e| LakeError::Internal {
                            message: format!("materializing inlined value of \"{key}\": {e}"),
                        })?,
                }
            } else {
                vector.push_null().map_err(|e| LakeError::Internal {
                    message: format!("materializing inlined placeholder of \"{key}\": {e}"),
                })?;
            }
        }
        columns.push(pinkdb::storage::Column {
            name: col.name.clone(),
            dtype: col.dtype,
            default: col.default.clone(),
            chunks: vec![Arc::new(vector)],
        });
        present.push(wanted[i]);
    }
    let table = DataTable::from_columns(table_name.to_string(), columns, decoded.len());
    Ok((table, present))
}

/// One column's exact contribution of the live inlined rows: NULL count, the
/// fold of its non-NULL values, and whether it had any non-NULL value at all
/// (an all-NULL contribution adds no extreme — it must not make the file fold
/// unknown).
#[derive(Debug, Clone)]
pub(crate) struct InlinedFact {
    /// Live inlined rows this fact observes.
    pub(crate) rows: u64,
    pub(crate) nulls: u64,
    pub(crate) min: Option<Value>,
    pub(crate) max: Option<Value>,
    pub(crate) has_value: bool,
    /// DOUBLE NaNs among the live values (0 for other types).
    pub(crate) nans: u64,
}

/// The exact facts of the given live inlined rows, one per logical column.
pub(crate) fn live_facts(
    live: &[InlinedRow],
    record: &TableSchema,
    key: &str,
    manifest_path: &std::path::Path,
) -> Result<Vec<InlinedFact>> {
    let mut facts: Vec<InlinedFact> = record
        .columns
        .iter()
        .map(|_| InlinedFact {
            rows: live.len() as u64,
            nulls: 0,
            min: None,
            max: None,
            has_value: false,
            nans: 0,
        })
        .collect();
    for row in live {
        let values = decode_row(record, row, key, manifest_path)?;
        for (fact, value) in facts.iter_mut().zip(&values) {
            match value {
                Value::Null => fact.nulls += 1,
                value => {
                    fact.has_value = true;
                    if matches!(value, Value::Double(x) if x.is_nan()) {
                        fact.nans += 1;
                    }
                    match &fact.min {
                        None => fact.min = Some(value.clone()),
                        Some(current) => {
                            if pinkdb::plan::compare_values(value, current).is_lt() {
                                fact.min = Some(value.clone());
                            }
                        }
                    }
                    match &fact.max {
                        None => fact.max = Some(value.clone()),
                        Some(current) => {
                            if pinkdb::plan::compare_values(value, current).is_gt() {
                                fact.max = Some(value.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(facts)
}

/// Fold one live-inlined column fact into an in-progress table-level fold:
/// NULL counts add up, and a column with at least one non-NULL inlined value
/// contributes its exact extremes. The DOUBLE gate is the shared one (an
/// exact zero NaN count and a nonzero extreme) applied to the inlined
/// contribution alone. `*_known` are the fold's provenance flags — a refused
/// (or NaN-poisoned) inlined extreme clears the flag exactly like a file whose
/// physical facts a positional delete made unknown, so the executor falls
/// back to materializing, never to a wrong answer.
pub(crate) fn apply_fact(
    fact: &InlinedFact,
    nulls: &mut Option<u64>,
    min: &mut Option<Value>,
    min_known: &mut bool,
    max: &mut Option<Value>,
    max_known: &mut bool,
) {
    if let Some(n) = *nulls {
        *nulls = n.checked_add(fact.nulls);
    }
    if !fact.has_value {
        return;
    }
    let is_double = fact
        .min
        .as_ref()
        .or(fact.max.as_ref())
        .is_some_and(|v| matches!(v, Value::Double(_)));
    let (imin, imax) = if is_double {
        if fact.nans == 0 {
            (
                nonzero_double(fact.min.clone()),
                nonzero_double(fact.max.clone()),
            )
        } else {
            (None, None)
        }
    } else {
        (fact.min.clone(), fact.max.clone())
    };
    match imin {
        Some(v) => match min {
            None => *min = Some(v),
            Some(current) => {
                if pinkdb::plan::compare_values(&v, current).is_lt() {
                    *current = v;
                }
            }
        },
        None => *min_known = false,
    }
    match imax {
        Some(v) => match max {
            None => *max = Some(v),
            Some(current) => {
                if pinkdb::plan::compare_values(&v, current).is_gt() {
                    *current = v;
                }
            }
        },
        None => *max_known = false,
    }
}

/// A DOUBLE extreme that can be served: nonzero and not NaN (the
/// `-0.0 == 0.0` rule of [`crate::stats::serve_column_stats`]).
fn nonzero_double(v: Option<Value>) -> Option<Value> {
    match v {
        Some(Value::Double(x)) if x != 0.0 && !x.is_nan() => Some(Value::Double(x)),
        _ => None,
    }
}
