//! `.lake attach`: a session-local, read-only, footer-only view of one lake.
//!
//! Attaching reads `catalog.json` + the chosen manifest (exactly what
//! [`Lake::load`] reads first), then opens every data file **footer-only**:
//! the Vortex footer, the `pinkdb.table` identity/schema segment and the
//! stored statistics — no data segment is read and no column is decoded.
//! Locally that is a positioned read of the postscript + footer; over object
//! storage (which has no ranged reads in this build) the whole object is read
//! into memory and opened in place, still skipping the decode.
//!
//! The result is a [`pinkdb::AttachedLake`]: descriptors the engine can
//! answer exact `COUNT`/`MIN`/`MAX` from, and a per-table loader that decodes
//! a table on first use through the very same path [`Lake::load`] uses, so
//! materialized values are identical to `.lake open`.
//!
//! Every check `.lake open` performs at load time runs at attach time
//! instead, where it can: the metadata segment parses, the Vortex dtype
//! agrees with the declared schema, the file's `db.table` identity matches
//! its catalog key, and the file's row count matches the manifest's. A lake
//! this build cannot attach (format_version 1, a table with several data
//! files) is refused with a clear error instead of a guess.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pinkdb::attached::AttachedScan;
use pinkdb::{AttachedLake, AttachedTable, DataTable, PinkDbError};

use crate::storage::{Head, Storage};
use crate::{only_table, split_key, validate_rel_path, vortex_file, Lake, LakeError, Result};

/// What [`Lake::attach`] read and pinned.
#[derive(Debug)]
pub struct AttachmentReport {
    /// The manifest version the attachment is pinned to (the latest, or the
    /// one asked for).
    pub version: u64,
    /// Number of attached tables.
    pub tables: usize,
    /// The lake's catalog `format_version` (2 or 3 in this build).
    pub format_version: u32,
    /// True when the data files live on object storage: attach read whole
    /// objects (no ranged reads in this build), still without decoding.
    pub remote: bool,
    /// The read-only handle to register with the engine
    /// ([`pinkdb::PinkDb::attach_lake`]).
    pub lake: Arc<dyn AttachedLake>,
}

/// One attached lake, as the engine sees it.
#[derive(Debug)]
pub(crate) struct LakeAttachment {
    location: String,
    storage: Arc<dyn Storage>,
    root: std::path::PathBuf,
    /// lowercased `(db, table)` → `(display db, display table, data rel)`.
    files: HashMap<(String, String), (String, String, String)>,
    tables: Vec<AttachedTable>,
    /// Materialized columns per table, decoded lazily and cached.
    cache: Mutex<HashMap<(String, String), CachedTable>>,
    /// Decodes performed (tests: a stats-only query must not decode).
    loads: AtomicU64,
    /// Columns decoded in total (diagnostics and tests: projection pushdown
    /// must decode only the referenced columns).
    columns_decoded: AtomicU64,
}

/// One table's materialized state: the columns decoded so far, plus NULL
/// placeholders (same chunking) for the ones not decoded yet. Every column of
/// `table` is chunk-aligned with every other, so a column decoded later can
/// be swapped in without touching the rest.
#[derive(Debug)]
struct CachedTable {
    table: Arc<DataTable>,
    /// One flag per column: true = decoded (its chunks hold values).
    present: Vec<bool>,
}

impl LakeAttachment {
    #[cfg(test)]
    pub(crate) fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn columns_decoded(&self) -> u64 {
        self.columns_decoded.load(Ordering::Relaxed)
    }

    /// The table descriptor for a lowercased `(db, table)` key.
    fn descriptor(&self, key: &(String, String)) -> Option<&AttachedTable> {
        let (db, table) = key;
        self.tables
            .iter()
            .find(|t| t.db.eq_ignore_ascii_case(db) && t.table.eq_ignore_ascii_case(table))
    }

    /// The columns a call must decode: `requested` marked per index. A
    /// `None`/out-of-range/unprovable projection marks every column (the
    /// always-correct fallback), and an empty one marks column 0 (the table's
    /// chunk shape has to come from somewhere).
    fn requested_columns(&self, key: &(String, String), columns: Option<&[usize]>) -> Vec<bool> {
        let n_cols = self.descriptor(key).map(|t| t.schema.len()).unwrap_or(0);
        let mut requested = match columns {
            Some(cols) if cols.iter().all(|&i| i < n_cols) => {
                let mut wanted = vec![false; n_cols];
                for &i in cols {
                    wanted[i] = true;
                }
                wanted
            }
            _ => vec![true; n_cols],
        };
        if requested.iter().all(|&w| !w) {
            if let Some(first) = requested.first_mut() {
                *first = true;
            }
        }
        requested
    }
}

impl AttachedLake for LakeAttachment {
    fn location(&self) -> &str {
        &self.location
    }

    fn tables(&self) -> &[AttachedTable] {
        &self.tables
    }

    fn load(&self, db: &str, table: &str) -> std::result::Result<Arc<DataTable>, PinkDbError> {
        Ok(self.load_projected(db, table, None)?.table)
    }

    fn load_projected(
        &self,
        db: &str,
        table: &str,
        columns: Option<&[usize]>,
    ) -> std::result::Result<AttachedScan, PinkDbError> {
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        // One decode per table at a time: the lock is held across the decode
        // so concurrent readers of one store materialize each column once.
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        let Some((dbname, tablename, rel)) = self.files.get(&key).cloned() else {
            return Err(PinkDbError::Catalog(format!(
                "lake {} has no table \"{db}.{table}\"",
                self.location
            )));
        };
        let requested = self.requested_columns(&key, columns);
        if requested.is_empty() {
            return Err(PinkDbError::Catalog(format!(
                "lake {} table \"{db}.{table}\" has no columns",
                self.location
            )));
        }
        match cache.get_mut(&key) {
            Some(entry) => {
                let missing: Vec<usize> = (0..requested.len())
                    .filter(|&i| requested[i] && !entry.present[i])
                    .collect();
                if missing.is_empty() {
                    return Ok(AttachedScan {
                        table: Arc::clone(&entry.table),
                        present: entry.present.clone(),
                    });
                }
                self.loads.fetch_add(1, Ordering::Relaxed);
                self.columns_decoded
                    .fetch_add(missing.len() as u64, Ordering::Relaxed);
                let (fresh, _fresh_present) = load_projected_table(
                    &*self.storage,
                    &self.root,
                    &rel,
                    &dbname,
                    &tablename,
                    &missing,
                )
                .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                let merged = merge_projected(&entry.table, &fresh, &missing, &tablename)
                    .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                for &i in &missing {
                    entry.present[i] = true;
                }
                entry.table = Arc::new(merged);
                Ok(AttachedScan {
                    table: Arc::clone(&entry.table),
                    present: entry.present.clone(),
                })
            }
            None => {
                // A full read takes the very decode `.lake open` performs
                // (pinned value- and chunking-identical by the wave-D tests);
                // a projection decodes only its columns.
                self.loads.fetch_add(1, Ordering::Relaxed);
                let decoded: Vec<usize> = (0..requested.len()).filter(|&i| requested[i]).collect();
                self.columns_decoded
                    .fetch_add(decoded.len() as u64, Ordering::Relaxed);
                let (table, present) = if requested.iter().all(|&w| w) {
                    let table = load_table(&*self.storage, &self.root, &rel, &dbname, &tablename)
                        .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                    let present = vec![true; table.num_cols()];
                    (table, present)
                } else {
                    let (table, present) = load_projected_table(
                        &*self.storage,
                        &self.root,
                        &rel,
                        &dbname,
                        &tablename,
                        &decoded,
                    )
                    .map_err(|e| PinkDbError::Execution(e.to_string()))?;
                    (table, present)
                };
                let table = Arc::new(table);
                cache.insert(
                    key,
                    CachedTable {
                        table: Arc::clone(&table),
                        present: present.clone(),
                    },
                );
                Ok(AttachedScan { table, present })
            }
        }
    }

    fn decode_stats(&self) -> (u64, u64) {
        (
            self.loads.load(Ordering::Relaxed),
            self.columns_decoded.load(Ordering::Relaxed),
        )
    }
}

/// [`Lake::load`]'s per-file decode for one table: read the file, decode it
/// into a one-table store, check the file's identity against its catalog key,
/// and hand the table back (chunks shared by `Arc`, never copied twice).
fn load_table(
    storage: &dyn Storage,
    root: &std::path::Path,
    rel: &str,
    dbname: &str,
    tablename: &str,
) -> Result<DataTable> {
    let path = storage.abs(rel);
    let bytes = storage.read(rel, &format!("loading data file \"{rel}\""))?;
    let mini = vortex_file::decode_table_owned(&path, bytes)?;
    let (file_db, file_table) = only_table(&mini, rel, root)?;
    if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(tablename) {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{dbname}.{tablename}\"",
                file_table.name
            ),
        });
    }
    Ok(file_table.clone())
}

/// [`vortex_file::decode_projected`]'s per-file read for one table: decode
/// only `columns`, check the file's identity against its catalog key, and
/// hand the full-schema table (decoded columns + NULL placeholders) back.
fn load_projected_table(
    storage: &dyn Storage,
    root: &std::path::Path,
    rel: &str,
    dbname: &str,
    tablename: &str,
    columns: &[usize],
) -> Result<(DataTable, Vec<bool>)> {
    let path = storage.abs(rel);
    let (mini, present) = vortex_file::decode_projected(&path, storage, rel, columns)?;
    let (file_db, file_table) = only_table(&mini, rel, root)?;
    if !file_db.eq_ignore_ascii_case(dbname) || !file_table.name.eq_ignore_ascii_case(tablename) {
        return Err(LakeError::Corrupt {
            path,
            message: format!(
                "data file holds {file_db}.{}, but the catalog says \"{dbname}.{tablename}\"",
                file_table.name
            ),
        });
    }
    Ok((file_table.clone(), present))
}

/// Swap freshly decoded columns into an already materialized table. The two
/// tables' chunkings must agree exactly (both come from the same slicing of
/// the same file, so they do); a mismatch is an internal error, never a
/// silently misaligned column.
fn merge_projected(
    existing: &DataTable,
    fresh: &DataTable,
    columns: &[usize],
    tablename: &str,
) -> Result<DataTable> {
    let rows: Vec<usize> = existing.columns[0].chunks.iter().map(|c| c.len()).collect();
    let mut merged = existing.columns.clone();
    for &i in columns {
        let lens: Vec<usize> = fresh.columns[i].chunks.iter().map(|c| c.len()).collect();
        if lens != rows {
            return Err(LakeError::Internal {
                message: format!(
                    "projected decode of \"{tablename}\" column {i} produced a different \
                     chunking ({} vs {} chunks)",
                    lens.len(),
                    rows.len()
                ),
            });
        }
        merged[i] = fresh.columns[i].clone();
    }
    Ok(DataTable::from_columns(
        existing.name.clone(),
        merged,
        existing.num_rows,
    ))
}

/// Footer-only open of one manifest's tables (see the module docs). `version`
/// is the caller's snapshot choice; the manifest (not the catalog) is read so
/// an attached version stays pinned however the catalog moves.
pub(crate) fn build(
    lake: &Lake,
    version: Option<u64>,
) -> Result<(Arc<LakeAttachment>, AttachmentReport)> {
    let (manifest, manifest_path) = lake.manifest(version)?;
    let root = lake.path().to_path_buf();
    if manifest.format_version < 2 {
        return Err(LakeError::NotRepresentable {
            message: format!(
                "lake {} is format_version {} (its `.pink` data files carry no statistics); \
                 .lake attach needs the Vortex format — use .lake open, or save the store \
                 again to migrate the lake to format_version {}",
                root.display(),
                manifest.format_version,
                crate::FORMAT_VERSION
            ),
        });
    }
    let mut tables: Vec<AttachedTable> = Vec::with_capacity(manifest.tables.len());
    let mut files: HashMap<(String, String), (String, String, String)> = HashMap::new();
    let mut remote = false;
    for (key, entry) in &manifest.tables {
        let (dbname, table) = split_key(key, &manifest_path)?;
        if entry.files.len() != 1 {
            return Err(LakeError::NotRepresentable {
                message: format!(
                    "table \"{key}\" has {} data files; .lake attach supports one data file \
                     per table in this build — use .lake open",
                    entry.files.len()
                ),
            });
        }
        let rel = &entry.files[0];
        validate_rel_path(rel)?;
        let path = lake.storage.abs(rel);
        match lake.storage.head(rel)? {
            Head::File => {}
            Head::Missing => {
                return Err(LakeError::Io {
                    context: format!("loading data file \"{rel}\""),
                    source: std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("{} is missing", path.display()),
                    ),
                })
            }
            Head::NotAFile => {
                return Err(LakeError::Corrupt {
                    path,
                    message: "data file is not a regular file".to_string(),
                })
            }
        }
        let local = lake.storage.local_path(rel).is_some();
        remote |= !local;
        let footer = vortex_file::read_footer(&path, &*lake.storage, rel)?;
        if !footer.db.eq_ignore_ascii_case(dbname) || !footer.table.eq_ignore_ascii_case(table) {
            return Err(LakeError::Corrupt {
                path,
                message: format!(
                    "data file holds {}.{}, but the catalog says \"{key}\"",
                    footer.db, footer.table
                ),
            });
        }
        if footer.rows != entry.rows {
            return Err(LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!(
                    "table \"{key}\" has {} rows, but the catalog says {}",
                    footer.rows, entry.rows
                ),
            });
        }
        let lkey = (dbname.to_ascii_lowercase(), table.to_ascii_lowercase());
        if files.contains_key(&lkey) {
            return Err(LakeError::Corrupt {
                path: manifest_path.clone(),
                message: format!(
                    "the manifest names \"{key}\" twice under one case-insensitive name; \
                     refusing to attach an ambiguous table"
                ),
            });
        }
        files.insert(lkey, (dbname.to_string(), table.to_string(), rel.clone()));
        tables.push(AttachedTable {
            db: dbname.to_string(),
            table: table.to_string(),
            rows: footer.rows,
            schema: footer.schema,
            stats: footer.stats,
        });
    }
    let attachment = Arc::new(LakeAttachment {
        location: root.display().to_string(),
        storage: Arc::clone(&lake.storage),
        root,
        files,
        tables,
        cache: Mutex::new(HashMap::new()),
        loads: AtomicU64::new(0),
        columns_decoded: AtomicU64::new(0),
    });
    let report = AttachmentReport {
        version: manifest.version,
        tables: attachment.tables.len(),
        format_version: manifest.format_version,
        remote,
        lake: Arc::clone(&attachment) as Arc<dyn AttachedLake>,
    };
    Ok((attachment, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::{PinkDb, Value};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicU64;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_attach_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A store with an all-types table (NULLs included) plus an empty table.
    fn fixture_store() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE TABLE t (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, \
             dt DATE, ts TIMESTAMP);",
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            t.append_row(vec![
                Value::Boolean(true),
                Value::Integer(-5),
                Value::Bigint(9_999_999_999),
                Value::Double(-2.5),
                Value::Varchar("b".to_string()),
                Value::Date(0),
                Value::Timestamp(-1),
            ])
            .unwrap();
            t.append_row(vec![Value::Null; 7]).unwrap();
            t.append_row(vec![
                Value::Boolean(false),
                Value::Integer(9),
                Value::Bigint(i64::MIN),
                Value::Double(1.25),
                Value::Varchar("a".to_string()),
                Value::Date(19_000),
                Value::Timestamp(1_700_000_000_000_000),
            ])
            .unwrap();
        }
        db.execute("CREATE TABLE empty (x INTEGER);").unwrap();
        db
    }

    fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        let mut results = db.execute(sql).unwrap();
        match results.pop().unwrap() {
            pinkdb::QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Stats-only queries must not decode a data file; anything else must
    /// decode once and then cache.
    #[test]
    fn stats_queries_do_not_decode_and_others_decode_once() {
        let dir = scratch("stats");
        let lake_dir = dir.join("lake");
        crate::save_store(&fixture_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, report) = build(&lake, None).unwrap();
        assert_eq!(report.tables, 2);
        let db = Arc::clone(&attachment);

        let mut store = PinkDb::new();
        store
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        // COUNT(*) over the attached table: footer rows, no decode.
        assert_eq!(
            query(&mut store, "SELECT COUNT(*) FROM t;"),
            vec![vec![Value::Bigint(3)]]
        );
        assert_eq!(db.loads(), 0, "COUNT(*) must not decode the data file");
        // MIN/MAX over the integer column: exact stats, no decode.
        assert_eq!(
            query(&mut store, "SELECT MIN(i), MAX(i) FROM t;"),
            vec![vec![Value::Integer(-5), Value::Integer(9)]]
        );
        assert_eq!(db.loads(), 0, "MIN/MAX must not decode the data file");
        // COUNT(v) skips the one NULL row: exact NULL count, no decode.
        assert_eq!(
            query(&mut store, "SELECT COUNT(v) FROM t;"),
            vec![vec![Value::Bigint(2)]]
        );
        assert_eq!(db.loads(), 0, "COUNT(col) must not decode the data file");
        // A filtered scan materializes the table exactly once.
        let rows = query(&mut store, "SELECT COUNT(*) FROM t WHERE i > 0;");
        assert_eq!(db.loads(), 1);
        assert_eq!(rows, vec![vec![Value::Bigint(1)]]);
        let _ = query(&mut store, "SELECT SUM(i) FROM t;");
        assert_eq!(db.loads(), 1, "materialized tables are reused");
        // The empty table answers counts and NULL min/max without decoding.
        assert_eq!(
            query(
                &mut store,
                "SELECT COUNT(*), COUNT(x), MIN(x), MAX(x) FROM empty;"
            ),
            vec![vec![
                Value::Bigint(0),
                Value::Bigint(0),
                Value::Null,
                Value::Null
            ]]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The descriptors' statistics are exactly what the decoded table would
    /// answer, for every column of an adversarial fixture.
    #[test]
    fn footer_stats_match_decoded_answers() {
        let dir = scratch("equiv");
        let lake_dir = dir.join("lake");
        crate::save_store(&fixture_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();
        let mut store = PinkDb::new();
        store
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        let tables = attachment.tables().to_vec();
        for table in tables {
            // Decode once (this is the ground truth), then compare every
            // provable statistic against the decoded table.
            let decoded = attachment.load(&table.db, &table.table).unwrap();
            assert_eq!(
                decoded.num_rows() as u64,
                table.rows,
                "{}.{}",
                table.db,
                table.table
            );
            for (i, (name, dtype)) in table.schema.iter().enumerate() {
                let stats = &table.stats[i];
                let mut nulls = 0u64;
                let mut min: Option<Value> = None;
                let mut max: Option<Value> = None;
                for row in 0..decoded.num_rows() {
                    let v = decoded.get(row, i).unwrap();
                    if v.is_null() {
                        nulls += 1;
                        continue;
                    }
                    let v = v.clone();
                    if min.as_ref().is_none_or(|m| compare(&v, m).is_lt()) {
                        min = Some(v.clone());
                    }
                    if max.as_ref().is_none_or(|m| compare(&v, m).is_gt()) {
                        max = Some(v);
                    }
                }
                if let Some(n) = stats.null_count {
                    assert_eq!(n, nulls, "{}.{name} NULL count", table.table);
                }
                if decoded.num_rows() == 0 || nulls as usize == decoded.num_rows() {
                    // The executor derives NULL without the statistic; a
                    // statistic must not disagree.
                    if let Some(v) = &stats.min {
                        assert!(v.is_null(), "{}.{name} min", table.table);
                    }
                    if let Some(v) = &stats.max {
                        assert!(v.is_null(), "{}.{name} max", table.table);
                    }
                } else {
                    if let Some(v) = &stats.min {
                        assert_eq!(
                            Some(v),
                            min.as_ref(),
                            "{}.{name} ({dtype}) min",
                            table.table
                        );
                    }
                    if let Some(v) = &stats.max {
                        assert_eq!(
                            Some(v),
                            max.as_ref(),
                            "{}.{name} ({dtype}) max",
                            table.table
                        );
                    }
                }
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
        pinkdb::plan::compare_values(a, b)
    }

    /// A multi-block, all-types table for the projection tests: 20,000 rows
    /// (ten engine chunks over several stored blocks, so decodes exercise
    /// ragged block tails and multi-block merges), NULLs, NaN payloads,
    /// `-0.0`, extremes and long strings.
    fn projection_store() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, b BOOLEAN);")
            .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            let mut next = move || {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                state
            };
            for row in 0..20_000i64 {
                let r = next();
                let d = if row % 599 == 0 {
                    Value::Double(f64::from_bits(0x7ff8_0000_cafe_0001))
                } else if row == 3 {
                    Value::Double(-0.0)
                } else {
                    Value::Double((r % 4000) as f64 / 8.0 - 250.0)
                };
                t.append_row(vec![
                    if r % 23 == 0 {
                        Value::Null
                    } else {
                        Value::Integer((r % 500) as i32 - 250)
                    },
                    Value::Bigint((r as i64).wrapping_sub(i64::MAX / 2)),
                    d,
                    if r % 31 == 0 {
                        Value::Varchar("z".repeat((r % 500) as usize))
                    } else {
                        Value::Varchar(format!("k{}-{}", r % 100, row % 7))
                    },
                    Value::Boolean(r % 2 == 0),
                ])
                .unwrap();
            }
        }
        db
    }

    /// Row-for-row equality with doubles compared bit-exactly.
    fn assert_same_rows(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
        assert_eq!(a.len(), b.len(), "{what}: row count");
        for (ra, rb) in a.iter().zip(b) {
            assert_eq!(ra.len(), rb.len(), "{what}: column count");
            for (va, vb) in ra.iter().zip(rb) {
                match (va, vb) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "{what}: {va:?} vs {vb:?}")
                    }
                    _ => assert_eq!(va, vb, "{what}"),
                }
            }
        }
    }

    /// A projected read decodes exactly the requested columns, is
    /// value-identical (doubles by bit pattern) to the full decode for those
    /// columns, keeps the full decode's chunking in every column, and shares
    /// already-decoded columns across later projections.
    #[test]
    fn projected_reads_equal_full_reads_and_decode_only_requested_columns() {
        let dir = scratch("projection_values");
        let lake_dir = dir.join("lake");
        crate::save_store(&projection_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();

        // Ground truth: the full decode (what `.lake open` materializes).
        let full = attachment.load("main", "t").unwrap();
        assert_eq!(full.num_rows(), 20_000);
        assert!(full.num_chunks() > 1, "the fixture must be multi-chunk");

        // A sparse projection after the full load: nothing new to decode.
        let projected = attachment
            .load_projected("main", "t", Some(&[0, 3]))
            .unwrap();
        assert_eq!(attachment.loads(), 1, "the full load already decoded");
        assert_eq!(attachment.columns_decoded(), 5);
        assert_eq!(projected.present, vec![true; 5]);
        assert_eq!(projected.table.schema(), full.schema());
        assert_eq!(projected.table.num_rows(), full.num_rows());

        // A fresh attachment, so the first decode is the projection itself:
        // exactly columns 1 and 4, with every other column a NULL
        // placeholder of the same chunk lengths.
        let (solo, _) = build(&lake, None).unwrap();
        let sparse = solo.load_projected("main", "t", Some(&[1, 4])).unwrap();
        assert_eq!(sparse.present, vec![false, true, false, false, true]);
        assert_eq!(solo.loads(), 1);
        assert_eq!(solo.columns_decoded(), 2, "only columns 1 and 4 decoded");
        assert_eq!(sparse.table.num_rows(), full.num_rows());
        assert_eq!(sparse.table.num_chunks(), full.num_chunks());
        for j in 0..full.num_chunks() {
            for i in 0..full.num_cols() {
                assert_eq!(
                    sparse.table.columns[i].chunks[j].len(),
                    full.columns[i].chunks[j].len(),
                    "chunk {j} column {i} length"
                );
            }
        }
        for (i, decoded) in [
            (0usize, false),
            (1, true),
            (2, false),
            (3, false),
            (4, true),
        ] {
            for row in 0..full.num_rows() {
                let (a, b) = (sparse.table.get(row, i).unwrap(), full.get(row, i).unwrap());
                if decoded {
                    match (&a, &b) {
                        (Value::Double(x), Value::Double(y)) => assert_eq!(
                            x.to_bits(),
                            y.to_bits(),
                            "column {i} row {row}: {a:?} vs {b:?}"
                        ),
                        _ => assert_eq!(a, b, "column {i} row {row}"),
                    }
                } else {
                    assert!(a.is_null(), "an undecoded column is a NULL placeholder");
                }
            }
        }

        // Extending decodes only the missing columns and keeps the decoded
        // ones shared (no re-decode, no copy).
        let extended = solo
            .load_projected("main", "t", Some(&[0, 1, 2, 3, 4]))
            .unwrap();
        assert_eq!(solo.loads(), 2);
        assert_eq!(solo.columns_decoded(), 5);
        assert_eq!(extended.present, vec![true; 5]);
        for i in [1usize, 4] {
            for j in 0..full.num_chunks() {
                assert!(
                    Arc::ptr_eq(
                        &sparse.table.columns[i].chunks[j],
                        &extended.table.columns[i].chunks[j]
                    ),
                    "column {i} chunk {j} must be shared across projections"
                );
            }
        }
        // Every value now equals the full decode bit for bit.
        for i in 0..full.num_cols() {
            for row in 0..full.num_rows() {
                let (a, b) = (
                    extended.table.get(row, i).unwrap(),
                    full.get(row, i).unwrap(),
                );
                match (&a, &b) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "column {i} row {row}")
                    }
                    _ => assert_eq!(a, b, "column {i} row {row}"),
                }
            }
        }

        // An empty projection decodes column 0 only (the chunk shape has to
        // come from somewhere) and never panics.
        let (empty, _) = build(&lake, None).unwrap();
        let none = empty.load_projected("main", "t", Some(&[])).unwrap();
        assert_eq!(none.present, vec![true, false, false, false, false]);
        assert_eq!(empty.columns_decoded(), 1);
        assert_eq!(none.table.num_rows(), 20_000);
        assert_eq!(none.table.num_chunks(), full.num_chunks());

        // An out-of-range projection is unprovable: every column decodes.
        let (oob, _) = build(&lake, None).unwrap();
        let all = oob.load_projected("main", "t", Some(&[9])).unwrap();
        assert_eq!(all.present, vec![true; 5]);
        assert_eq!(oob.columns_decoded(), 5);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The engine's attached scan path drives `load_projected`: each query
    /// decodes its referenced columns only, extensions are incremental, the
    /// fast paths decode nothing at all, and every answer equals the
    /// in-memory execution over the same rows.
    #[test]
    fn queries_decode_only_their_referenced_columns() {
        let dir = scratch("projection_queries");
        let lake_dir = dir.join("lake");
        crate::save_store(&projection_store(), &lake_dir).unwrap();
        let lake = Lake::open(&lake_dir).unwrap();
        let (attachment, _) = build(&lake, None).unwrap();
        let mut store = PinkDb::new();
        store
            .attach_lake(Arc::clone(&attachment) as Arc<dyn AttachedLake>)
            .unwrap();
        // The same rows in an ordinary store: the ground-truth answers.
        let mut plain = projection_store();

        let mut check = |store: &mut PinkDb, sql: &str, decoded_after: u64| {
            let (attached, plain) = (query(store, sql), query(&mut plain, sql));
            assert_eq!(
                attachment.columns_decoded(),
                decoded_after,
                "{sql}: columns decoded"
            );
            assert_same_rows(&attached, &plain, sql);
        };

        // Fast paths: statistics only, zero columns decoded.
        check(&mut store, "SELECT COUNT(*) FROM t;", 0);
        check(&mut store, "SELECT MIN(i), MAX(i), COUNT(v) FROM t;", 0);
        check(
            &mut store,
            "SELECT MIN(b), MAX(b), MIN(g), MAX(g) FROM t;",
            0,
        );
        // `d` holds NaN payloads: its MIN/MAX cannot come from statistics,
        // so the whole query materializes — only `d`'s column.
        check(&mut store, "SELECT MIN(d), MAX(d) FROM t;", 1);
        // Each fallback decodes exactly the referenced column; repeats of an
        // already-decoded column decode nothing.
        check(&mut store, "SELECT SUM(i) FROM t;", 2);
        check(&mut store, "SELECT AVG(i) FROM t;", 2);
        // A filter decodes the predicate's column (already decoded here).
        check(&mut store, "SELECT COUNT(*) FROM t WHERE i > 0;", 2);
        // Order-by + limit: the sort key and the projected column.
        check(&mut store, "SELECT g FROM t ORDER BY g DESC LIMIT 3;", 3);
        check(&mut store, "SELECT v FROM t ORDER BY v LIMIT 5;", 4);
        // GROUP BY aggregate: the key.
        check(
            &mut store,
            "SELECT b, COUNT(*) FROM t GROUP BY b ORDER BY b;",
            5,
        );
        check(&mut store, "SELECT SUM(i), MIN(g), COUNT(v) FROM t;", 5);
        // A window over projected columns.
        check(
            &mut store,
            "SELECT i, ROW_NUMBER() OVER (PARTITION BY b ORDER BY i) FROM t LIMIT 5;",
            5,
        );
        // `SELECT *` and expressions over several columns: every column is
        // already in, so nothing decodes.
        check(&mut store, "SELECT * FROM t ORDER BY i LIMIT 2;", 5);
        check(&mut store, "SELECT i + g + d FROM t LIMIT 2;", 5);
        // The counter only ever grows; the decode count stops growing once
        // every column is materialized.
        assert_eq!(
            attachment.loads(),
            5,
            "one decode per newly referenced column set"
        );
        let loads = attachment.loads();
        check(&mut store, "SELECT SUM(i), SUM(d) FROM t;", 5);
        assert_eq!(
            attachment.loads(),
            loads,
            "no decode once every column is in"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A missing data file fails attach with a clean error.
    #[test]
    fn missing_data_file_fails_attach() {
        let dir = scratch("missing");
        let lake_dir = dir.join("lake");
        crate::save_store(&fixture_store(), &lake_dir).unwrap();
        // Remove every data file the manifest references.
        for entry in walk(&lake_dir.join("data")) {
            std::fs::remove_file(&entry).unwrap();
        }
        let lake = Lake::open(&lake_dir).unwrap();
        let err = build(&lake, None).unwrap_err().to_string();
        assert!(
            err.contains("is missing") || err.contains("loading data file"),
            "{err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
        out
    }
}
