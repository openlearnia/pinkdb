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
    /// Materialized tables, decoded once on first use.
    cache: Mutex<HashMap<(String, String), Arc<DataTable>>>,
    /// Decodes performed (tests: a stats-only query must not decode).
    loads: AtomicU64,
}

impl LakeAttachment {
    #[cfg(test)]
    pub(crate) fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
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
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        // One decode per table: the lock is held across the decode so
        // concurrent readers of one store materialize a table once.
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(t) = cache.get(&key) {
            return Ok(Arc::clone(t));
        }
        let (dbname, tablename, rel) = self.files.get(&key).ok_or_else(|| {
            PinkDbError::Catalog(format!(
                "lake {} has no table \"{db}.{table}\"",
                self.location
            ))
        })?;
        self.loads.fetch_add(1, Ordering::Relaxed);
        let table = load_table(&*self.storage, &self.root, rel, dbname, tablename)
            .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        let table = Arc::new(table);
        cache.insert(key, Arc::clone(&table));
        Ok(table)
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
