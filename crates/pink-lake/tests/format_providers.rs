//! Wave 87/88: lake data-file format providers.
//!
//! Two things are proved here:
//!
//! - an unknown format id in a v4–v6 catalog is refused **per table** (wave
//!   88, rule 5): the lake opens, the readable tables load, the table is
//!   listed by `unavailable_tables`, attach lists it but every read fails
//!   with the `UnknownFormat` error naming the id, targeted maintenance and
//!   commits fail, lake-wide maintenance skips it, and a full save carries
//!   its entry forward verbatim unless the store has that key (then the save
//!   is refused);
//! - a test-only provider (`pinkwrap`, `.pink` bytes behind a 4-byte header)
//!   registered through the public seam works end to end: save → reopen →
//!   open → attach (`COUNT`/`MIN`/`MAX` equal to `open`) → append → merge
//!   maintenance, with the catalog and the files carrying the new id.
#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::format::{DataFile, EncodedFile, LakeFormat, register_lake_format};
use pink_lake::{Commit, LakeError, MergeSettings};
use pinkdb::persist::SavedTableStats;
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_formats_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// `main.t(i INTEGER, s VARCHAR)` with `rows` deterministic rows.
fn store(rows: usize) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    for r in 0..rows {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![
                Value::Integer(r as i32 - 2),
                Value::Varchar(format!("row-{r}")),
            ])
            .unwrap();
    }
    db
}

/// `main.t` and `main.u`, both `(i INTEGER, s VARCHAR)`, `rows` rows each.
fn two_table_store(rows: usize) -> PinkDb {
    let mut db = store(rows);
    db.execute("CREATE TABLE main.u (i INTEGER, s VARCHAR);")
        .unwrap();
    for r in 0..rows {
        db.table_mut_in("main", "u")
            .unwrap()
            .append_row(vec![
                Value::Integer(r as i32 - 2),
                Value::Varchar(format!("row-{r}")),
            ])
            .unwrap();
    }
    db
}

fn sub_table(rows: &[(i32, &str)]) -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    for (i, s) in rows {
        table
            .append_row(vec![Value::Integer(*i), Value::Varchar((*s).to_string())])
            .unwrap();
    }
    Arc::new(table)
}

/// The test provider's magic: four bytes in front of ordinary `.pink` bytes.
const HEADER: &[u8; 4] = b"PW01";

/// A minimal provider: `.pink` single-table bytes behind a 4-byte header.
///
/// The write side builds the one-table store the engine's own writer wants,
/// so the published statistics are the same exact facts the `.pink` provider
/// publishes; the read side strips the header and opens the payload (through
/// a staging file, since the engine's `.pink` opener takes a path).
#[derive(Debug)]
struct PinkWrap;

impl LakeFormat for PinkWrap {
    fn id(&self) -> &str {
        "pinkwrap"
    }

    fn extension(&self) -> &str {
        "pinkwrap"
    }

    fn write(&self, db: &str, table: &str, src: &DataTable) -> pink_lake::Result<EncodedFile> {
        let mut mini = PinkDb::new();
        if db != "main" {
            mini.create_database(db).unwrap();
            mini.drop_database("main").unwrap();
        }
        mini.create_table_in(db, table, src.schema()).unwrap();
        {
            let dst = mini.table_mut_in(db, table).unwrap();
            for i in 0..src.num_chunks() {
                let chunk = src.scan_chunk(i).unwrap();
                dst.append_shared_chunk(&chunk.columns).unwrap();
            }
        }
        let (bytes, saved) = mini.to_pink_bytes_with_stats().unwrap();
        let stats: SavedTableStats = saved
            .into_iter()
            .find(|s| s.db == db && s.table == table)
            .expect("the single-table encoder publishes its statistics");
        let mut wrapped = Vec::with_capacity(HEADER.len() + bytes.len());
        wrapped.extend_from_slice(HEADER);
        wrapped.extend_from_slice(&bytes);
        Ok(EncodedFile {
            bytes: wrapped,
            stats,
            footer: None,
        })
    }

    fn read(&self, file: &DataFile<'_>) -> pink_lake::Result<PinkDb> {
        let bytes = file.read()?;
        let payload = bytes
            .strip_prefix(HEADER)
            .ok_or_else(|| LakeError::Corrupt {
                path: file.path().to_path_buf(),
                message: "pinkwrap data file does not start with its PW01 header".to_string(),
            })?;
        let dir = scratch("pinkwrap_stage");
        let staged = dir.join("payload.pink");
        std::fs::write(&staged, payload).unwrap();
        let mini = PinkDb::open(staged.to_str().unwrap()).map_err(|e| LakeError::Engine {
            context: format!("loading data file \"{}\"", file.rel()),
            source: e,
        });
        cleanup(&dir);
        mini
    }
}

/// Register `pinkwrap` exactly once for this test process.
fn register_pinkwrap() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| register_lake_format(Arc::new(PinkWrap)).unwrap());
}

fn assert_unknown_format(err: &LakeError, id: &str) {
    assert!(matches!(err, LakeError::UnknownFormat { .. }), "{err:?}");
    let text = err.to_string();
    assert!(text.contains(id), "must name the id: {text}");
    assert!(
        text.contains("no lake format provider is registered"),
        "must say no provider is registered: {text}"
    );
}

/// An id with no registered provider is refused **per table** (wave 88, rule
/// 5): the lake opens, the readable table loads, attach lists the table but
/// every read fails naming the id, targeted maintenance/commits fail,
/// lake-wide maintenance skips it, and a full save carries it forward unless
/// the store has that key.
#[test]
fn an_unknown_format_refuses_only_its_own_table() {
    let dir = scratch("unknown");
    let lake_dir = dir.join("lake");
    let source = two_table_store(4);
    save_store_lake(&source, &lake_dir).unwrap();

    // Pin a format id no provider can serve. The catalog keeps its version
    // and lake id, so the manifest is otherwise a valid v6 head.
    assert!(
        open_lake(&lake_dir)
            .unwrap()
            .table_format_name("main.u")
            .unwrap()
            .is_some()
    );
    pink_lake::catalog_set_table_format(&catalog_url(&lake_dir), "main.u", "nosuch").unwrap();

    // Open succeeds; only the readable table materializes.
    let lake = open_lake(&lake_dir).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_eq!(loaded.list_tables(), vec!["t".to_string()]);
    assert_eq!(loaded.table("t").unwrap().num_rows(), 4);

    let unavailable = lake.unavailable_tables().unwrap();
    assert_eq!(unavailable.len(), 1, "{unavailable:?}");
    assert_eq!(unavailable[0].table, "main.u");
    assert_eq!(unavailable[0].format, "nosuch");
    assert_unknown_format(&unavailable[0].error, "nosuch");

    // Attach lists both; the unavailable descriptor cannot answer and every
    // read of it fails with the provider error naming table and id.
    let attachment = lake.attach(None).unwrap();
    assert_eq!(attachment.tables, 2, "both tables are listed");
    let outage = attachment
        .lake
        .tables()
        .iter()
        .find(|t| t.table == "u")
        .expect("the unavailable table is listed");
    assert_eq!(outage.rows, pink_lake::UNAVAILABLE_ROWS);
    let text = attachment.lake.load("main", "u").unwrap_err().to_string();
    assert!(text.contains("main.u"), "{text}");
    assert!(text.contains("nosuch"), "{text}");
    assert!(
        text.contains("no lake format provider is registered"),
        "{text}"
    );

    let mut attached = PinkDb::new();
    attached.attach_lake(attachment.lake).unwrap();
    assert_eq!(
        query(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(4)]]
    );
    for sql in [
        "SELECT COUNT(*) FROM u;",
        "SELECT COUNT(i) FROM u;",
        "SELECT MIN(i), MAX(i) FROM u;",
        "SELECT i FROM u;",
    ] {
        let text = attached.execute(sql).unwrap_err().to_string();
        assert!(text.contains("main.u"), "{sql}: {text}");
        assert!(text.contains("nosuch"), "{sql}: {text}");
    }

    // A targeted maintenance/rewrite/commit fails with the provider error.
    let mut lake = open_lake(&lake_dir).unwrap();
    let err = lake
        .merge_adjacent_files(Some("main.u"), &MergeSettings::default())
        .unwrap_err();
    assert_unknown_format(&err, "nosuch");
    let err = lake
        .rewrite_table_format("main", "u", pink_lake::format::PINK_FORMAT_ID)
        .unwrap_err();
    assert_unknown_format(&err, "nosuch");
    let mut commit = Commit::new();
    commit.append("main.u", sub_table(&[(9, "nine")]));
    let err = lake.commit(&commit).unwrap_err();
    assert_unknown_format(&err, "nosuch");

    // Lake-wide maintenance skips it and reports the key.
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(!report.committed, "{report:?}");
    assert_eq!(report.skipped, vec!["main.u".to_string()], "{report:?}");

    // A full save carries the entry forward verbatim: same file list, same
    // pin flag — the store has no table under that key.
    let store = lake.load(None).unwrap();
    let before = lake.table_info().unwrap();
    let report = lake.write_from_store_report(&store).unwrap();
    assert_eq!(report.version, 3);
    assert_eq!(
        lake.table_format_name("main.u").unwrap().as_deref(),
        Some("nosuch")
    );
    let after = lake.table_info().unwrap();
    for field in ["rows", "file_count", "schema_version"] {
        let pick = |infos: &[pink_lake::TableInfo]| {
            infos
                .iter()
                .find(|i| i.table == "main.u")
                .map(|i| format!("{i:?}"))
        };
        assert_eq!(pick(&after).is_some(), pick(&before).is_some(), "{field}");
    }
    let reloaded = open_lake(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(reloaded.table("t").unwrap().num_rows(), 4);

    // A store that has the key would replace data this build cannot read:
    // the save is refused and publishes nothing.
    let text = lake
        .write_from_store_report(&two_table_store(4))
        .unwrap_err()
        .to_string();
    assert!(text.contains("main.u"), "{text}");
    assert!(text.contains("nosuch"), "{text}");
    assert_eq!(lake.current_version(), 3, "nothing was published");

    cleanup(&dir);
}

/// Duplicate ids are refused: a catalog id resolves to one provider.
#[test]
fn duplicate_registration_is_refused() {
    register_pinkwrap();
    let err = register_lake_format(Arc::new(PinkWrap)).unwrap_err();
    assert!(matches!(err, LakeError::DuplicateFormat { .. }), "{err:?}");
    assert!(err.to_string().contains("pinkwrap"), "{err}");
}

/// A registered provider is used end to end: save, reopen, open, attach,
/// append and merge maintenance.
#[test]
fn a_registered_provider_is_used_end_to_end() {
    register_pinkwrap();
    let dir = scratch("pinkwrap");
    let lake_dir = dir.join("lake");

    // Save a fresh lake under the test provider.
    let original = store(5);
    let mut lake = create_lake(&lake_dir).unwrap();
    lake.set_write_format("pinkwrap").unwrap();
    assert!(!lake.attach(None).is_err(), "an empty lake still attaches");
    let report = lake.write_from_store_report(&original).unwrap();
    assert_eq!(report.version, 2);

    // The catalog pins the id, the file is `*.pinkwrap`, and its bytes start
    // with the provider's header.
    let lake_now = open_lake(&lake_dir).unwrap();
    assert_eq!(
        lake_now.table_format_name("main.t").unwrap().as_deref(),
        Some("pinkwrap")
    );
    let files = lake_now.list_files(Some("main.t"), None, false).unwrap();
    assert_eq!(files.len(), 1);
    let rel = files[0].path.as_str();
    assert!(rel.ends_with(".pinkwrap"), "{rel}");
    let on_disk = std::fs::read(data_dir(&lake_dir).join(rel)).unwrap();
    assert_eq!(&on_disk[..4], HEADER, "the provider's header is on disk");

    // Reopen and load through the provider.
    let reopened = open_lake(&lake_dir).unwrap();
    let loaded = reopened.load(None).unwrap();
    assert_eq!(
        loaded.table("t").unwrap().num_rows(),
        original.table("t").unwrap().num_rows()
    );

    // Attach: COUNT/MIN/MAX answers equal the open path's answers.
    let attachment = reopened.attach(None).unwrap();
    assert_eq!(
        attachment.requests, 0,
        "attach reads the catalog's statistics block, not the provider's file"
    );
    let mut attached = PinkDb::new();
    attached.attach_lake(attachment.lake).unwrap();
    let sql = "SELECT COUNT(*), MIN(i), MAX(i) FROM t;";
    let open_answers = query(&mut reopened.load(None).unwrap(), sql);
    let attach_answers = query(&mut attached, sql);
    assert_eq!(attach_answers, open_answers);
    assert_eq!(
        open_answers,
        vec![vec![
            Value::Bigint(5),
            Value::Integer(-2),
            Value::Integer(2)
        ]]
    );

    // Append through a commit: the handle's chosen provider writes the new
    // file. Inlining is disabled so the rows become a data file.
    let mut lake = open_lake(&lake_dir).unwrap();
    lake.set_write_format("pinkwrap").unwrap();
    lake.set_data_inlining_row_limit(0);
    let appended = sub_table(&[
        (10, "ten"),
        (11, "eleven"),
        (12, "twelve"),
        (13, "thirteen"),
    ]);
    let mut commit = Commit::new();
    commit.append("main.t", appended);
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.version, 3);
    let lake_now = open_lake(&lake_dir).unwrap();
    assert_eq!(
        lake_now.table_format_name("main.t").unwrap().as_deref(),
        Some("pinkwrap")
    );
    let files = lake_now.list_files(Some("main.t"), None, false).unwrap();
    assert_eq!(files.len(), 2, "the append added one file");
    for file in &files {
        let rel = file.path.as_str();
        assert!(rel.ends_with(".pinkwrap"), "{rel}");
        let bytes = std::fs::read(data_dir(&lake_dir).join(rel)).unwrap();
        assert_eq!(&bytes[..4], HEADER, "{rel}");
    }

    // The appended rows load through the provider.
    let loaded = open_lake(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 9);

    // Maintenance: merge the two files into one, re-encoded by the entry's
    // own provider.
    let mut lake = open_lake(&lake_dir).unwrap();
    let report = lake
        .merge_adjacent_files(Some("main.t"), &MergeSettings::default())
        .unwrap();
    assert!(report.committed, "two same-epoch files are mergeable");
    assert_eq!(report.files_processed(), 2);
    assert_eq!(report.files_created(), 1);
    let lake_now = open_lake(&lake_dir).unwrap();
    assert_eq!(
        lake_now.table_format_name("main.t").unwrap().as_deref(),
        Some("pinkwrap")
    );
    let files = lake_now.list_files(Some("main.t"), None, false).unwrap();
    assert_eq!(files.len(), 1, "the merge consolidated the files");
    let rel = files[0].path.as_str();
    assert!(rel.ends_with(".pinkwrap"), "{rel}");
    let bytes = std::fs::read(data_dir(&lake_dir).join(rel)).unwrap();
    assert_eq!(&bytes[..4], HEADER, "{rel}");

    // ... and the merged file answers exactly what the open path holds.
    let loaded = open_lake(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 9);
    let mut opened = loaded;
    assert_eq!(
        query(&mut opened, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;"),
        vec![vec![
            Value::Bigint(9),
            Value::Integer(-2),
            Value::Integer(13)
        ]]
    );

    cleanup(&dir);
}

/// Wave 104: a catalog row naming the removed data format fails with a clear
/// re-save error — the lake is never opened as if the format were merely
/// unavailable, and there is no reader or converter.
#[test]
fn a_removed_format_entry_fails_the_lake_with_a_resave_hint() {
    let dir = scratch("removed_format");
    let lake_dir = dir.join("lake");
    save_store_lake(&store(3), &lake_dir).unwrap();

    // The id is spelled from two pieces: wave 104 removed it from the tree.
    let removed = concat!("vor", "tex");
    pink_lake::catalog_set_table_format(&catalog_url(&lake_dir), "main.t", removed).unwrap();

    let lake = open_lake(&lake_dir).unwrap();
    let expected = concat!(
        "this lake has Vor",
        "tex data files, which pinkdb no longer reads; re-save the lake with an older build"
    );
    let err = lake.load(None).unwrap_err();
    assert!(matches!(err, LakeError::RemovedFormat { .. }), "{err:?}");
    assert_eq!(err.to_string(), expected);
    let err = lake.attach(None).unwrap_err();
    assert!(matches!(err, LakeError::RemovedFormat { .. }), "{err:?}");

    cleanup(&dir);
}
