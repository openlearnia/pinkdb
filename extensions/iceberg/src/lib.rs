//! pinkdb's **iceberg** reader: `iceberg_scan`, `iceberg_snapshots` and
//! `iceberg_metadata` over an Apache Iceberg table. Wave 72 of the roadmap.
//!
//! # What it reads
//!
//! A table is a directory (or an object prefix) whose `metadata/` holds one
//! JSON metadata file per commit, `version-hint.text` naming the newest, and
//! an Avro **manifest list** per snapshot. [`iceberg_scan`](IcebergSource)
//! resolves the current metadata, picks a snapshot, reads the manifest list,
//! then each manifest, then the live data files — Parquet, through
//! [`pinkdb_parquet::scan_source`], i.e. the exact ranged/projected/pruning
//! path `read_parquet` uses. Remote tables therefore inherit read-ahead and
//! the opt-in range cache instead of growing a second transport.
//!
//! ```text
//! iceberg_scan('warehouse/db/table')                     -- current snapshot
//! iceberg_scan('warehouse/db/table/metadata/v3.metadata.json')
//! iceberg_scan('s3://bucket/table', snapshot_from_id := 4091)
//! iceberg_scan('table', snapshot_from_timestamp := TIMESTAMP '2024-05-01 00:00:00')
//! ```
//!
//! Paths may be local, `https://` or `s3://` (the `remote` feature, which the
//! CLI's `httpfs` feature turns on). Relative object paths inside the
//! metadata resolve against the table root, so a copied table directory reads
//! without rewriting its metadata (DuckDB's `allow_moved_paths`, but always
//! on: a table that stores absolute paths still opens them as written).
//!
//! # Semantics
//!
//! - **Snapshots**: the metadata's `current-snapshot-id` by default, or
//!   `snapshot_from_id` / `snapshot_from_timestamp` (DuckDB's parameter
//!   names; the timestamp picks the newest snapshot at or before it).
//!   `iceberg_snapshots` lists them all, `iceberg_metadata` lists the current
//!   snapshot's manifest entries — both without touching a data file.
//! - **Deletes**: positional delete files (`content = 1`) are applied by row
//!   position; equality deletes (`content = 2`) are refused with an error
//!   naming the file, because applying them means comparing delete-file values
//!   per row (a later wave).
//! - **Schema evolution by field id**: a data file's Parquet column is
//!   matched to the current schema's field id first (renames follow their id)
//!   and by name only when the file carries no ids. A column added after a
//!   file was written reads NULL.
//! - **Pruning** is an optimization, never a correctness rule — the engine
//!   re-applies every predicate. Manifest-list partition summaries prune whole
//!   manifests (identity transforms only), and a manifest entry's
//!   `lower_bounds`/`upper_bounds`/`null_value_counts` prune its data file.
//! - **`COUNT(*)`** is the sum of the live files' manifest record counts minus
//!   the positional deletes that apply: no data page is read.
//!
//! # Avro
//!
//! Manifests and manifest lists are Avro object container files. This crate
//! carries its own decoder ([`mod avro`](avro)) instead of a dependency:
//! Iceberg uses a fixed, small slice of Avro — records, unions, arrays,
//! `int`/`long`-keyed maps encoded as arrays, `bytes`/`string`, `null` and
//! `deflate` blocks — and that slice is a few hundred lines. The schema is
//! read from each file's own header, so a newer writer's extra fields are
//! skipped, not rejected. `snappy` and `zstd` blocks are refused by name
//! (they need a C or a much larger pure-Rust codec; Iceberg's default is
//! `deflate`).
//!
//! # Honest limits
//!
//! - Equality deletes are refused (above).
//! - A remote table needs `metadata/version-hint.text`; without it the newest
//!   metadata cannot be discovered without listing, so pass the metadata file
//!   itself.
//! - Only flat schemas: a nested or list-typed Iceberg column, and any type
//!   the engine has no column for (UUID, fixed, time), is refused at bind with
//!   the column and the Iceberg type named.
//! - `iceberg_snapshots` has no `summary` MAP column (pinkdb has no map type).
//! - Writing, `ALTER`, and the REST/Glue catalogues are out of scope: a table
//!   location is given as a path.

mod avro;
mod introspect;
mod manifest;
mod obj;
mod scan;
mod table;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::attached::PrunePredicate;
use pinkdb::table_source::{
    TableFunctionCall, TableScanOutput, TableSource, TableSourceStats, register_global_table_source,
};
use pinkdb::{DataChunk, DataType, PinkDbError, Result};

use crate::table::{Selector, Table};

/// The registry name this source claims (`EXPLAIN`, stats, `table_source()`).
pub const SOURCE_NAME: &str = "iceberg";

/// The functions this source serves.
const SCAN: &str = "iceberg_scan";
const SNAPSHOTS: &str = "iceberg_snapshots";
const METADATA: &str = "iceberg_metadata";

/// A parsed call: which function, which table, which snapshot.
#[derive(Debug, Clone, PartialEq)]
enum Call {
    Scan { path: String, selector: Selector },
    Snapshots { path: String },
    Metadata { path: String },
}

/// The iceberg table source. Register it on a store
/// ([`pinkdb::PinkDb::register_table_source`]) or process-wide
/// ([`ensure_registered`]).
#[derive(Debug)]
pub struct IcebergSource {
    scans: AtomicU64,
    rows: AtomicU64,
    files: AtomicU64,
    manifests: AtomicU64,
    manifests_pruned: AtomicU64,
    columns: AtomicU64,
}

impl Default for IcebergSource {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergSource {
    pub fn new() -> Self {
        Self {
            scans: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            files: AtomicU64::new(0),
            manifests: AtomicU64::new(0),
            manifests_pruned: AtomicU64::new(0),
            columns: AtomicU64::new(0),
        }
    }

    /// Interpret a call. `Ok(None)` means "not mine": the binder then asks the
    /// next source, and a bare path is only claimed when it is an Iceberg
    /// metadata file (a directory stays parquet's to claim).
    fn parse(call: &TableFunctionCall) -> Result<Option<Call>> {
        let (name, args) = match call {
            TableFunctionCall::Path(path) => {
                if !path.ends_with(".metadata.json") {
                    return Ok(None);
                }
                (SCAN.to_string(), Vec::new())
            }
            TableFunctionCall::Function { name, args } => (name.to_ascii_lowercase(), args.clone()),
        };
        if ![SCAN, SNAPSHOTS, METADATA].contains(&name.as_str()) {
            return Ok(None);
        }
        let mut path: Option<String> = None;
        let mut from_id: Option<i64> = None;
        let mut from_timestamp: Option<i64> = None;
        for arg in &args {
            let Some(arg_name) = arg.name_lower() else {
                if path.is_some() {
                    return Err(PinkDbError::Execution(format!(
                        "iceberg: {name} takes one path argument (got a second, {arg})"
                    )));
                }
                let pinkdb::Value::Varchar(value) = &arg.value else {
                    return Err(PinkDbError::Execution(format!(
                        "iceberg: {name} takes a path string, not {arg}"
                    )));
                };
                path = Some(value.clone());
                continue;
            };
            if name != SCAN {
                return Err(PinkDbError::Execution(format!(
                    "iceberg: {name} takes no named arguments (got {arg_name})"
                )));
            }
            match arg_name.as_str() {
                "snapshot_from_id" => {
                    let id = match &arg.value {
                        pinkdb::Value::Bigint(value) => *value,
                        pinkdb::Value::Integer(value) => *value as i64,
                        _ => {
                            return Err(PinkDbError::Execution(format!(
                                "iceberg: snapshot_from_id takes a snapshot id (a BIGINT), \
                                 not {arg}"
                            )));
                        }
                    };
                    from_id = Some(id);
                }
                "snapshot_from_timestamp" => {
                    let pinkdb::Value::Timestamp(value) = &arg.value else {
                        return Err(PinkDbError::Execution(format!(
                            "iceberg: snapshot_from_timestamp takes a TIMESTAMP, not {arg} \
                             (a BIGINT of milliseconds is not a timestamp — write \
                             TIMESTAMP '<iso 8601>')"
                        )));
                    };
                    from_timestamp = Some(*value);
                }
                other => {
                    return Err(PinkDbError::Execution(format!(
                        "iceberg: {SCAN} does not take `{other}`; it takes a path, \
                         snapshot_from_id := <id> and snapshot_from_timestamp := <TIMESTAMP>"
                    )));
                }
            }
        }
        let path = match (call, path) {
            (TableFunctionCall::Path(path), None) => path.clone(),
            (_, Some(path)) => path,
            (_, None) => {
                return Err(PinkDbError::Execution(format!(
                    "iceberg: {name} needs a table location: a table directory, its \
                     metadata directory, or a …/metadata/vN.metadata.json file"
                )));
            }
        };
        let selector = match (from_id, from_timestamp) {
            (Some(_), Some(_)) => {
                return Err(PinkDbError::Execution(
                    "iceberg: pass at most one of snapshot_from_id and \
                     snapshot_from_timestamp"
                        .to_string(),
                ));
            }
            (Some(id), None) => Selector::Id(id),
            (None, Some(micros)) => Selector::Timestamp(micros),
            (None, None) => Selector::Current,
        };
        Ok(Some(match name.as_str() {
            SNAPSHOTS => Call::Snapshots { path },
            METADATA => Call::Metadata { path },
            _ => Call::Scan { path, selector },
        }))
    }

    fn record(&self, output: &TableScanOutput, rows: u64) {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.files.fetch_add(output.files, Ordering::Relaxed);
        self.manifests
            .fetch_add(output.row_groups, Ordering::Relaxed);
        self.manifests_pruned
            .fetch_add(output.row_groups_pruned, Ordering::Relaxed);
        self.columns
            .fetch_add(output.columns_decoded, Ordering::Relaxed);
    }
}

impl TableSource for IcebergSource {
    fn name(&self) -> &str {
        SOURCE_NAME
    }

    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
        let Some(parsed) = Self::parse(call)? else {
            return Ok(None);
        };
        let schema = match &parsed {
            Call::Scan { path, selector } => {
                // Bind resolves the metadata and the schema only: a bad path or
                // a bad snapshot id fails here, before any data file is read.
                let table = Table::open(path)?;
                let _ = table.select(*selector)?;
                table.current_schema()?
            }
            Call::Snapshots { path } => {
                let _ = Table::open(path)?;
                let types = introspect::snapshot_types();
                return Ok(Some(types));
            }
            Call::Metadata { path } => {
                let _ = Table::open(path)?;
                let types = introspect::manifest_types();
                return Ok(Some(types));
            }
        };
        let mut columns = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let ty = field.ty.data_type();
            if matches!(field.ty, table::IceType::Other(_)) {
                return Err(PinkDbError::Execution(format!(
                    "iceberg: column `{}` has the type {}; pinkdb reads flat \
                     booleans, integers, longs, floats, doubles, decimals, dates, \
                     timestamps, strings and binary — everything else is out of scope",
                    field.name,
                    field.ty.name()
                )));
            }
            columns.push((field.name.clone(), ty));
        }
        Ok(Some(columns))
    }

    fn scan(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        filters: &[PrunePredicate],
    ) -> Result<TableScanOutput> {
        let Some(parsed) = Self::parse(call)? else {
            return Err(PinkDbError::Execution(format!(
                "the iceberg source does not serve {call:?}"
            )));
        };
        let output = match parsed {
            Call::Scan { path, selector } => {
                let plan = scan::plan(&path, selector, filters)?;
                let (chunks, columns) = scan::scan(&plan, projection)?;
                TableScanOutput {
                    files: plan.files.len() as u64,
                    row_groups: plan.manifests,
                    row_groups_pruned: plan.manifests_pruned,
                    columns_decoded: columns,
                    chunks,
                }
            }
            Call::Snapshots { path } => TableScanOutput {
                chunks: project(introspect::snapshots(&path)?, projection),
                ..Default::default()
            },
            Call::Metadata { path } => TableScanOutput {
                chunks: project(introspect::metadata(&path)?, projection),
                ..Default::default()
            },
        };
        let rows: u64 = output.chunks.iter().map(|c| c.num_rows() as u64).sum();
        self.record(&output, rows);
        Ok(output)
    }

    fn stats(&self) -> TableSourceStats {
        TableSourceStats {
            name: SOURCE_NAME.to_string(),
            scans: self.scans.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            files: self.files.load(Ordering::Relaxed),
            row_groups: self.manifests.load(Ordering::Relaxed),
            row_groups_pruned: self.manifests_pruned.load(Ordering::Relaxed),
            columns_decoded: self.columns.load(Ordering::Relaxed),
        }
    }

    fn reset_stats(&self) {
        for counter in [
            &self.scans,
            &self.rows,
            &self.files,
            &self.manifests,
            &self.manifests_pruned,
            &self.columns,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

/// The source, for `db.register_table_source(pinkdb_iceberg::source())`.
pub fn source() -> Arc<dyn TableSource> {
    Arc::new(IcebergSource::new())
}

/// Register on one store (embedder path). Idempotent.
pub fn register(db: &mut pinkdb::PinkDb) {
    db.register_table_source(source());
}

/// Register **process-wide**, for every store created afterwards — the
/// bootstrap a durable store needs, because recovery replays a logged
/// `CREATE TABLE AS SELECT … FROM iceberg_scan(…)` before the caller holds
/// the store. The CLI (with its `iceberg` feature) calls this once at
/// startup. Idempotent.
pub fn ensure_registered() {
    register_global_table_source(source());
}

/// Apply a statement's projection to already-built chunks: columns the
/// statement did not name become the zero-length placeholders the scan
/// contract specifies.
fn project(chunks: Vec<DataChunk>, projection: Option<&[usize]>) -> Vec<DataChunk> {
    let Some(projection) = projection else {
        return chunks;
    };
    chunks
        .into_iter()
        .map(|chunk| {
            let rows = chunk.num_rows();
            let columns = (0..chunk.num_cols())
                .map(|column| {
                    if projection.contains(&column) {
                        Arc::new(chunk.column(column).clone())
                    } else {
                        Arc::new(pinkdb::Vector::new_empty(chunk.column(column).dtype()))
                    }
                })
                .collect();
            DataChunk::projected(columns, rows)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::Value;
    use pinkdb::table_source::TableFunctionArg;

    fn function(name: &str, args: Vec<(&str, Value)>) -> TableFunctionCall {
        TableFunctionCall::Function {
            name: name.to_string(),
            args: args
                .into_iter()
                .map(|(name, value)| TableFunctionArg {
                    name: if name.is_empty() {
                        None
                    } else {
                        Some(name.to_string())
                    },
                    value,
                })
                .collect(),
        }
    }

    #[test]
    fn parses_the_three_functions_and_the_two_snapshot_selectors() {
        let path = Value::Varchar("t".to_string());
        assert_eq!(
            IcebergSource::parse(&function(SCAN, vec![("", path.clone())])).unwrap(),
            Some(Call::Scan {
                path: "t".to_string(),
                selector: Selector::Current
            })
        );
        assert_eq!(
            IcebergSource::parse(&function(SNAPSHOTS, vec![("", path.clone())])).unwrap(),
            Some(Call::Snapshots {
                path: "t".to_string()
            })
        );
        assert_eq!(
            IcebergSource::parse(&function(
                SCAN,
                vec![("", path.clone()), ("snapshot_from_id", Value::Bigint(7))]
            ))
            .unwrap(),
            Some(Call::Scan {
                path: "t".to_string(),
                selector: Selector::Id(7)
            })
        );
        assert_eq!(
            IcebergSource::parse(&function(
                SCAN,
                vec![
                    ("", path),
                    (
                        "snapshot_from_timestamp",
                        Value::Timestamp(1_700_000_000_000_000)
                    )
                ]
            ))
            .unwrap(),
            Some(Call::Scan {
                path: "t".to_string(),
                selector: Selector::Timestamp(1_700_000_000_000_000)
            })
        );
    }

    #[test]
    fn refuses_what_it_cannot_honour_by_name() {
        let path = Value::Varchar("t".to_string());
        let error = IcebergSource::parse(&function(
            SCAN,
            vec![
                ("", path.clone()),
                ("snapshot_from_id", Value::Bigint(1)),
                ("snapshot_from_timestamp", Value::Timestamp(0)),
            ],
        ))
        .unwrap_err()
        .to_string();
        assert!(error.contains("at most one"), "{error}");
        let error = IcebergSource::parse(&function(
            SCAN,
            vec![
                ("", path.clone()),
                ("snapshot_from_id", Value::Varchar("x".into())),
            ],
        ))
        .unwrap_err()
        .to_string();
        assert!(error.contains("BIGINT"), "{error}");
        let error = IcebergSource::parse(&function(
            SCAN,
            vec![("", path), ("snapshot_as_of", Value::Bigint(1))],
        ))
        .unwrap_err()
        .to_string();
        assert!(error.contains("snapshot_as_of"), "{error}");
        assert_eq!(
            IcebergSource::parse(&function("iceberg_append", vec![])).unwrap(),
            None
        );
        assert_eq!(
            IcebergSource::parse(&TableFunctionCall::Path("dir".to_string())).unwrap(),
            None
        );
    }
}
