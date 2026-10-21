//! Wave 87: lake data-file format providers.
//!
//! Two things are proved here:
//!
//! - an unknown format id in a catalog is refused cleanly — naming the id and
//!   saying no provider is registered — on open, read (`load`), attach and
//!   maintenance (never a panic, never a fallback);
//! - a test-only provider (`pinkwrap`, `.pink` bytes behind a 4-byte header)
//!   registered through the public seam works end to end: save → reopen →
//!   open → attach (`COUNT`/`MIN`/`MAX` equal to `open`) → append → merge
//!   maintenance, with the catalog and the files carrying the new id.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::format::{DataFile, EncodedFile, LakeFormat, register_lake_format};
use pink_lake::{Commit, Lake, LakeError, MergeSettings, save_store};
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

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
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

/// An id with no registered provider refuses the catalog at open, read,
/// attach and maintenance — naming the id and saying no provider exists.
#[test]
fn an_unknown_format_id_is_refused_cleanly_on_open_attach_read_and_maintenance() {
    let dir = scratch("unknown");
    let lake_dir = dir.join("lake");
    save_store(&store(4), &lake_dir).unwrap();
    let mut lake = Lake::open(&lake_dir).unwrap();

    // Pin a format id no provider can serve. The catalog keeps its version
    // and lake id, so the manifest is otherwise a valid v6 head.
    let catalog_path = lake_dir.join("catalog.json");
    let mut catalog = read_json(&catalog_path);
    assert!(
        catalog["tables"]["main.t"]["format"].is_string(),
        "{catalog}"
    );
    catalog["tables"]["main.t"]["format"] = serde_json::json!("nosuch");
    std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();

    let err = Lake::open(&lake_dir).unwrap_err();
    assert_unknown_format(&err, "nosuch");

    // The handle opened before the edit reads the catalog again for every
    // operation, so each path sees the unknown id.
    let err = lake.load(None).unwrap_err();
    assert_unknown_format(&err, "nosuch");
    let err = lake.attach(None).unwrap_err();
    assert_unknown_format(&err, "nosuch");
    let err = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap_err();
    assert_unknown_format(&err, "nosuch");

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
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format("pinkwrap").unwrap();
    assert!(!lake.attach(None).is_err(), "an empty lake still attaches");
    let report = lake.write_from_store_report(&original).unwrap();
    assert_eq!(report.version, 2);

    // The catalog pins the id, the file is `*.pinkwrap`, and its bytes start
    // with the provider's header.
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(entry["format"], "pinkwrap", "{entry}");
    let files = entry["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{entry}");
    let rel = files[0]["path"].as_str().unwrap();
    assert!(rel.ends_with(".pinkwrap"), "{rel}");
    let on_disk = std::fs::read(lake_dir.join(rel)).unwrap();
    assert_eq!(&on_disk[..4], HEADER, "the provider's header is on disk");

    // Reopen and load through the provider.
    let reopened = Lake::open(&lake_dir).unwrap();
    let loaded = reopened.load(None).unwrap();
    assert_eq!(
        loaded.table("t").unwrap().num_rows(),
        original.table("t").unwrap().num_rows()
    );

    // Attach: COUNT/MIN/MAX answers equal the open path's answers.
    let attachment = reopened.attach(None).unwrap();
    assert!(
        attachment.statistics_only,
        "a provider without footer statistics attaches from the manifest block"
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
    let mut lake = Lake::open(&lake_dir).unwrap();
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
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(entry["format"], "pinkwrap", "{entry}");
    let files = entry["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "the append added one file: {entry}");
    for file in files {
        let rel = file["path"].as_str().unwrap();
        assert!(rel.ends_with(".pinkwrap"), "{rel}");
        let bytes = std::fs::read(lake_dir.join(rel)).unwrap();
        assert_eq!(&bytes[..4], HEADER, "{rel}");
    }

    // The appended rows load through the provider.
    let loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 9);

    // Maintenance: merge the two files into one, re-encoded by the entry's
    // own provider.
    let mut lake = Lake::open(&lake_dir).unwrap();
    let report = lake
        .merge_adjacent_files(Some("main.t"), &MergeSettings::default())
        .unwrap();
    assert!(report.committed, "two same-epoch files are mergeable");
    assert_eq!(report.files_processed(), 2);
    assert_eq!(report.files_created(), 1);
    let catalog = read_json(&lake_dir.join("catalog.json"));
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(entry["format"], "pinkwrap", "{entry}");
    let files = entry["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "the merge consolidated the files: {entry}");
    let rel = files[0]["path"].as_str().unwrap();
    assert!(rel.ends_with(".pinkwrap"), "{rel}");
    let bytes = std::fs::read(lake_dir.join(rel)).unwrap();
    assert_eq!(&bytes[..4], HEADER, "{rel}");

    // ... and the merged file answers exactly what the open path holds.
    let loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
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
