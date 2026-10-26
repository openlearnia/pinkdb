//! End-to-end tests of the public `pink-lake` API: round-trips, versioning,
//! crash-atomicity leftovers, and corrupt-metadata handling.

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{FORMAT_VERSION, LakeError, validate_store};
use pinkdb::{CHUNK_SIZE, DataTable, DataType, PinkDb, Value, Vector};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test, removed when the guard drops —
/// including when the test panics, so a failure leaves no litter behind.
struct ScratchDir(PathBuf);

impl std::ops::Deref for ScratchDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for ScratchDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> ScratchDir {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    ScratchDir(dir)
}

/// The helper catalog's `pink:` database path.
fn catalog_path(lake_dir: &Path) -> PathBuf {
    lake_dir.join("catalog.pink")
}

/// Execute a catalog-row mutation directly (corruption tests).
fn catalog_sql(lake_dir: &Path, sql: &str) {
    let mut db = PinkDb::open_durable(&catalog_path(lake_dir).display().to_string()).unwrap();
    db.execute(sql).unwrap();
}

/// A store with all types, NULLs, NaN payloads, `-0.0`, a multi-chunk table,
/// an empty table, a second database, and a table name containing a dot.
fn fixture_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE main.t (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, \
         dt DATE, ts TIMESTAMP);",
    )
    .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        t.append_row(vec![
            Value::Boolean(true),
            Value::Integer(-5),
            Value::Bigint(9_999_999_999),
            Value::Double(-0.0),
            Value::Varchar("hi".to_string()),
            Value::Date(0),
            Value::Timestamp(-1),
        ])
        .unwrap();
        t.append_row(vec![Value::Null; 7]).unwrap();
        t.append_row(vec![
            Value::Boolean(false),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MIN),
            Value::Double(f64::from_bits(0x7ff8_0000_dead_beef)),
            Value::Varchar("héllo wörld 🌍".to_string()),
            Value::Date(19_000),
            Value::Timestamp(1_700_000_000_000_000),
        ])
        .unwrap();
    }
    db.execute("CREATE TABLE big (a INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("big").unwrap();
        for i in 0..(CHUNK_SIZE + 100) {
            t.append_row(vec![
                Value::Integer((i % 7) as i32 - 3),
                Value::Varchar(format!("region{}", i % 3)),
            ])
            .unwrap();
        }
    }
    db.execute("CREATE TABLE empty (x INTEGER, y VARCHAR);")
        .unwrap();
    db.execute("CREATE DATABASE analytics;").unwrap();
    db.execute("CREATE TABLE analytics.u (k BIGINT);").unwrap();
    db.execute("INSERT INTO analytics.u VALUES (7);").unwrap();
    db.execute("CREATE TABLE analytics.\"a.b\" (z DOUBLE);")
        .unwrap();
    db.execute("INSERT INTO analytics.\"a.b\" VALUES (1.5), (-0.0);")
        .unwrap();
    db
}

/// The engine's deterministic `.pink` bytes of `db`.
fn store_bytes(db: &PinkDb, scratch: &Path, tag: &str) -> Vec<u8> {
    let path = scratch.join(format!("{tag}.pink"));
    db.save(path.to_str().unwrap()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).ok();
    bytes
}

/// Compare two stores value by value, doubles by bit pattern (so NaN
/// payloads and `-0.0` are checked exactly).
fn assert_same_store(a: &PinkDb, b: &PinkDb) {
    assert_eq!(a.list_databases(), b.list_databases(), "databases differ");
    for dbname in a.list_databases() {
        assert_eq!(
            a.list_tables_in(&dbname),
            b.list_tables_in(&dbname),
            "tables differ in {dbname}"
        );
        for table in a.list_tables_in(&dbname) {
            let ta = a.table_in(&dbname, &table).unwrap();
            let tb = b.table_in(&dbname, &table).unwrap();
            assert_eq!(ta.schema(), tb.schema(), "{dbname}.{table} schema");
            assert_eq!(ta.num_rows(), tb.num_rows(), "{dbname}.{table} rows");
            for r in 0..ta.num_rows() {
                for c in 0..ta.num_cols() {
                    let (va, vb) = (ta.get(r, c).unwrap(), tb.get(r, c).unwrap());
                    match (&va, &vb) {
                        (Value::Double(x), Value::Double(y)) => assert_eq!(
                            x.to_bits(),
                            y.to_bits(),
                            "{dbname}.{table} r{r} c{c}: {:?} vs {:?}",
                            va,
                            vb
                        ),
                        _ => assert_eq!(va, vb, "{dbname}.{table} r{r} c{c}"),
                    }
                }
            }
        }
    }
}

/// Every lake data file (`*.pink`) under `dir`, recursively.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if !dir.exists() {
        return out;
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(data_files(&path));
        } else if matches!(path.extension().and_then(|e| e.to_str()), Some("pink")) {
            out.push(path);
        }
    }
    out.sort();
    out
}

#[test]
fn versions_accumulate_and_old_versions_stay_loadable() {
    let dir = scratch("versions");
    let mut db = fixture_store();
    let mut lake = create_lake(&dir).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    let v2_bytes = store_bytes(&db, &dir, "v2");

    // Mutate: add a row, drop a table, add a database with a table.
    db.table_mut("t")
        .unwrap()
        .append_row(vec![
            Value::Boolean(true),
            Value::Integer(1),
            Value::Bigint(2),
            Value::Double(3.5),
            Value::Varchar("new".to_string()),
            Value::Date(1),
            Value::Timestamp(2),
        ])
        .unwrap();
    db.execute("DROP TABLE main.big;").unwrap();
    db.execute("CREATE DATABASE extra; CREATE TABLE extra.e (x INTEGER);")
        .unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);

    assert_eq!(lake.current_version(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    // v2 wrote 5 files, v3 wrote 5 more (main.t changed, main.big dropped,
    // extra.e added): old data files are never touched.
    assert_eq!(
        data_files(&dir.join("data")).len(),
        10,
        "old data files stay"
    );

    let latest = lake.load(None).unwrap();
    assert_same_store(&db, &latest);
    assert_eq!(
        store_bytes(&latest, &dir, "latest"),
        store_bytes(&db, &dir, "current")
    );

    let old = lake.load(Some(2)).unwrap();
    assert_eq!(store_bytes(&old, &dir, "old"), v2_bytes);
    assert_eq!(old.table("t").unwrap().num_rows(), 3);
    assert!(old.table("big").is_ok(), "v2 still has the dropped table");
    assert!(!old.database_exists("extra"));

    let missing = lake.load(Some(99)).unwrap_err();
    assert!(
        missing.to_string().contains("has no snapshot version 99"),
        "{missing}"
    );

    // Tables of the current version, sorted, with row counts.
    let tables = lake.tables().unwrap();
    assert_eq!(
        tables,
        vec![
            ("analytics.a.b".to_string(), 2),
            ("analytics.u".to_string(), 1),
            ("extra.e".to_string(), 0),
            ("main.empty".to_string(), 0),
            ("main.t".to_string(), 4),
        ]
    );
}

#[test]
fn database_names_with_a_dot_are_refused_cleanly() {
    let dir = scratch("dotted_db");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER);").unwrap();
    db.execute("CREATE DATABASE \"a.b\";").unwrap();
    db.execute("CREATE TABLE \"a.b\".u (y INTEGER);").unwrap();

    let mut lake = create_lake(&dir).unwrap();
    let err = lake.write_from_store(&db).unwrap_err().to_string();
    assert!(err.contains("contains \".\""), "{err}");
    assert!(err.contains("db.table"), "{err}");
    // The failed save published nothing.
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.versions().unwrap(), vec![1]);
}

#[test]
fn table_names_stay_inside_the_data_directory() {
    let dir = scratch("names");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE \"odd name\" (x INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE \"a/b\" (x INTEGER);").unwrap();
    db.execute("CREATE TABLE \"..\" (x INTEGER);").unwrap();
    db.execute("INSERT INTO \"odd name\" VALUES (1);").unwrap();

    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);

    // Every data directory is a single safe segment under data/.
    let mut segments: Vec<String> = std::fs::read_dir(dir.join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
        .collect();
    segments.sort();
    assert_eq!(segments, vec!["main...", "main.a%2Fb", "main.odd%20name"]);
    assert_eq!(data_files(&dir.join("data")).len(), 3);
}

#[test]
fn empty_databases_and_dropped_main_are_documented_v1_limits() {
    let dir = scratch("emptydb");
    let mut db = PinkDb::new();
    db.execute("CREATE DATABASE only;").unwrap();
    db.execute("CREATE TABLE only.t (x INTEGER);").unwrap();
    db.execute("INSERT INTO only.t VALUES (5);").unwrap();
    db.execute("DROP DATABASE main;").unwrap();
    db.execute("CREATE DATABASE empty;").unwrap();

    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    // `only` is rebuilt; a dropped `main` and an empty database are not.
    assert_eq!(loaded.list_databases(), vec!["main", "only"]);
    assert!(loaded.list_tables_in("main").is_empty());
    assert!(loaded.list_tables_in("empty").is_empty());
    assert!(!loaded.database_exists("empty"));
    assert_eq!(
        loaded.table_in("only", "t").unwrap().get(0, 0),
        Some(Value::Integer(5))
    );
}

#[test]
fn save_rejects_unrepresentable_stores_before_creating_a_lake() {
    let dir = scratch("prevalidate");
    let lake_dir = dir.join("lake");

    // A dotted database name cannot be a flat `db.table` key.
    let mut dotted = PinkDb::new();
    dotted.execute("CREATE DATABASE \"a.b\";").unwrap();
    dotted
        .execute("CREATE TABLE \"a.b\".t (x INTEGER);")
        .unwrap();
    let err = validate_store(&dotted).unwrap_err().to_string();
    assert!(err.contains("contains \".\""), "{err}");
    let err = save_store_lake(&dotted, &lake_dir).unwrap_err().to_string();
    assert!(err.contains("contains \".\""), "{err}");
    assert!(
        !lake_dir.join("data").exists(),
        "a rejected save writes no data"
    );

    // A table name whose encoded `data/` directory blows the filesystem
    // name limit is refused up front (the old behavior died with
    // ENAMETOOLONG after creating an empty v1 lake).
    let mut long = PinkDb::new();
    let name = "é".repeat(100); // 200 UTF-8 bytes -> 600 encoded bytes
    long.execute(&format!("CREATE TABLE \"{name}\" (x INTEGER);"))
        .unwrap();
    let err = validate_store(&long).unwrap_err().to_string();
    assert!(err.contains("over the 200-byte limit"), "{err}");
    let err = save_store_lake(&long, &lake_dir).unwrap_err().to_string();
    assert!(err.contains("over the 200-byte limit"), "{err}");
    assert!(!lake_dir.join("data").exists());
    assert!(validate_store(&PinkDb::new()).is_ok());

    // A long-but-encodable name still saves: the check is not over-strict.
    let mut ok = PinkDb::new();
    let name = "a".repeat(180);
    ok.execute(&format!("CREATE TABLE \"{name}\" (x INTEGER);"))
        .unwrap();
    assert!(validate_store(&ok).is_ok());
    assert_eq!(save_store_lake(&ok, &lake_dir).unwrap(), 2);
    let lake = open_lake(&lake_dir).unwrap();
    assert_eq!(lake.load(None).unwrap().list_tables().len(), 1);
}

#[cfg(unix)]
#[cfg(unix)]
#[test]
fn missing_data_files_are_reported_but_metadata_still_reads() {
    let dir = scratch("missing_files");
    let db = fixture_store();
    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    assert!(lake.missing_data_files().unwrap().is_empty());

    let victim = data_files(&dir.join("data/main.t")).remove(0);
    std::fs::remove_file(&victim).unwrap();
    let missing = lake.missing_data_files().unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].0, "main.t");
    assert!(
        missing[0]
            .1
            .ends_with(victim.file_name().unwrap().to_str().unwrap())
    );
    // Metadata still reads; only a load fails, and it says why.
    assert!(lake.tables().unwrap().iter().any(|(k, _)| k == "main.t"));
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("loading data file"), "{err}");
}

#[test]
fn pink_roundtrip_preserves_every_bit_pattern() {
    let dir = scratch("pink_bits");
    let db = common::fidelity_store();
    let original_bytes = store_bytes(&db, &dir, "original");

    let mut lake = create_lake(&dir).unwrap();
    assert_eq!(lake.format_version(), 7, "create publishes a v7 lake");
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();

    assert_same_store(&db, &loaded);
    // Chunk boundaries (and thus the `.pink` bytes) round-trip exactly.
    assert_eq!(loaded.table("big").unwrap().num_chunks(), 2);
    assert_eq!(
        loaded.table("empty").unwrap().num_chunks(),
        db.table("empty").unwrap().num_chunks()
    );
    assert_eq!(
        loaded.table("empty").unwrap().schema(),
        vec![
            ("x".to_string(), DataType::Integer),
            ("y".to_string(), DataType::Varchar)
        ]
    );
    assert_eq!(store_bytes(&loaded, &dir, "loaded"), original_bytes);

    // The float extremes survive bit-exactly, including NaN payloads a
    // naive writer would canonicalize.
    let fid = loaded.table("fid").unwrap();
    let codes: Vec<u64> = (0..fid.num_rows())
        .filter_map(|r| match fid.get(r, 3) {
            Some(Value::Double(x)) => Some(x.to_bits()),
            _ => None,
        })
        .collect();
    assert!(codes.contains(&(-0.0f64).to_bits()));
    assert!(codes.contains(&0.0f64.to_bits()));
    assert!(
        codes.contains(&0x7ff8_0000_dead_beef),
        "positive NaN payload"
    );
    assert!(
        codes.contains(&0xfff8_0000_0000_0001),
        "negative NaN payload"
    );
    assert!(codes.contains(&f64::INFINITY.to_bits()));

    // DATE/TIMESTAMP extremes, i32/i64 extremes, NULLs, strings.
    assert!(matches!(fid.get(0, 5), Some(Value::Date(i32::MIN))));
    assert!(matches!(fid.get(1, 5), Some(Value::Date(i32::MAX))));
    assert!(matches!(fid.get(0, 6), Some(Value::Timestamp(i64::MIN))));
    assert!(matches!(fid.get(1, 6), Some(Value::Timestamp(i64::MAX))));
    assert!(matches!(fid.get(0, 2), Some(Value::Bigint(i64::MIN))));
    assert!(matches!(fid.get(1, 2), Some(Value::Bigint(i64::MAX))));
    assert_eq!(fid.get(2, 0), Some(Value::Null));
    assert_eq!(
        fid.get(1, 4),
        Some(Value::Varchar("héllo wörld 🌍\0embedded".into()))
    );
    assert_eq!(fid.get(5, 4), Some(Value::Varchar(String::new())));
}

#[test]
fn a_corrupt_pink_data_file_is_a_clear_error() {
    let dir = scratch("pink_corrupt");
    let db = common::fidelity_store();
    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    let victim = data_files(&dir.join("data/main.fid"))
        .into_iter()
        .next()
        .expect("main.fid data file");
    // Truncate it to a prefix: no longer a decodable `.pink` file.
    let bytes = std::fs::read(&victim).unwrap();
    std::fs::write(&victim, &bytes[..bytes.len() / 2]).unwrap();
    let lake = open_lake(&dir).unwrap();
    let err = lake.load(None).unwrap_err().to_string();
    assert!(err.contains("main.fid"), "{err}");

    // Metadata still reads; only a load fails.
    assert!(lake.tables().unwrap().iter().any(|(k, _)| k == "main.fid"));
}

#[test]
fn extreme_values_and_chunk_shapes_roundtrip() {
    let dir = scratch("extreme_values");
    let mut db = PinkDb::new();

    // Subnormals, infinities, signaling NaNs, f64::MAX/MIN, and a 1 MiB
    // string (DOUBLEs are compared by bit pattern throughout).
    db.execute("CREATE TABLE m (d DOUBLE, s VARCHAR);").unwrap();
    {
        let t = db.table_mut("m").unwrap();
        let doubles = [
            f64::from_bits(1),                     // min positive subnormal
            f64::from_bits(0x000f_ffff_ffff_ffff), // max subnormal
            f64::from_bits(0x8000_0000_0000_0001), // min negative subnormal
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN,                              // most negative finite
            f64::from_bits(0x7ff0_0000_0000_0001), // signaling NaN
            f64::from_bits(0xfff0_0000_0000_0001), // negative signaling NaN
        ];
        for (i, d) in doubles.into_iter().enumerate() {
            t.append_row(vec![Value::Double(d), Value::Varchar(format!("v{i}"))])
                .unwrap();
        }
        t.append_row(vec![
            Value::Double(1.0),
            Value::Varchar("x".repeat(1 << 20)),
        ])
        .unwrap();
    }

    // 0-, 1-, and >CHUNK_SIZE-row tables; the big one is built through the
    // `append_chunk` bulk path, which fills the tail chunk to CHUNK_SIZE
    // before opening a new one (2000 + 1049 rows land as [2048, 1001]).
    db.execute("CREATE TABLE zero (x INTEGER, y VARCHAR);")
        .unwrap();
    db.execute("CREATE TABLE one (x INTEGER);").unwrap();
    db.execute("INSERT INTO one VALUES (7);").unwrap();
    db.execute("CREATE TABLE many (x INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("many").unwrap();
        let build = |rows: usize, offset: i32| {
            let mut x = Vector::with_capacity(DataType::Integer, rows);
            let mut s = Vector::with_capacity(DataType::Varchar, rows);
            for i in 0..rows {
                x.push_value(Value::Integer(offset + i as i32)).unwrap();
                s.push_str(&format!("r{}", offset + i as i32)).unwrap();
            }
            vec![x, s]
        };
        t.append_chunk(build(2000, 0)).unwrap();
        t.append_chunk(build(1049, 2000)).unwrap();
    }

    // An all-NULL table with two full chunks (the all-NULL multi-chunk
    // shape), built through the same bulk path.
    db.execute("CREATE TABLE nulls (a INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("nulls").unwrap();
        for _ in 0..2 {
            let mut a = Vector::with_capacity(DataType::Integer, CHUNK_SIZE);
            let mut s = Vector::with_capacity(DataType::Varchar, CHUNK_SIZE);
            for _ in 0..CHUNK_SIZE {
                a.push_null().unwrap();
                s.push_null().unwrap();
            }
            t.append_chunk(vec![a, s]).unwrap();
        }
    }

    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);

    // The 1 MiB string came back byte for byte, and the signaling-NaN and
    // f64::MIN bits survived.
    let m = loaded.table("m").unwrap();
    match m.get(8, 1) {
        Some(Value::Varchar(s)) => {
            assert_eq!(s.len(), 1 << 20);
            assert!(s.bytes().all(|b| b == b'x'));
        }
        other => panic!("expected the 1 MiB string, got {other:?}"),
    }
    assert!(matches!(
        m.get(6, 0),
        Some(Value::Double(x)) if x.to_bits() == 0x7ff0_0000_0000_0001
    ));
    assert!(matches!(
        m.get(5, 0),
        Some(Value::Double(x)) if x.to_bits() == f64::MIN.to_bits()
    ));

    // Chunk boundaries survive: 0 (schema-only file), 1, 2 via the bulk
    // path, 2 all-NULL.
    let sizes = |t: &DataTable| -> Vec<usize> {
        (0..t.num_chunks())
            .map(|i| t.scan_chunk(i).unwrap().num_rows())
            .collect()
    };
    assert_eq!(loaded.table("zero").unwrap().num_rows(), 0);
    assert_eq!(
        loaded.table("zero").unwrap().num_chunks(),
        db.table("zero").unwrap().num_chunks()
    );
    assert_eq!(loaded.table("one").unwrap().num_rows(), 1);
    assert_eq!(sizes(loaded.table("many").unwrap()), vec![2048, 1001]);
    assert_eq!(sizes(loaded.table("nulls").unwrap()), vec![CHUNK_SIZE; 2]);
    let null_chunk = loaded.table("nulls").unwrap().scan_chunk(0).unwrap();
    assert!(
        (0..null_chunk.num_rows)
            .all(|r| (0..null_chunk.columns.len()).all(|c| null_chunk.columns[c].is_null(r))),
        "every value of the all-NULL chunk must stay NULL"
    );

    // The loaded store still re-saves to the same `.pink` bytes.
    assert_eq!(
        store_bytes(&loaded, &dir, "loaded"),
        store_bytes(&db, &dir, "orig")
    );
}

#[test]
fn csv_import_chunk_boundaries_roundtrip() {
    let dir = scratch("csv_chunks");
    let mut db = PinkDb::new();
    let mut csv = String::from("id,name\n");
    for i in 0..(CHUNK_SIZE + 123) {
        csv.push_str(&format!("{i},row-{i}\n"));
    }
    let rows = pinkdb::csv::import_csv_string(&mut db, "t", &csv).unwrap();
    assert_eq!(rows, CHUNK_SIZE + 123);

    let sizes = |t: &DataTable| -> Vec<usize> {
        (0..t.num_chunks())
            .map(|i| t.scan_chunk(i).unwrap().num_rows())
            .collect()
    };
    assert_eq!(sizes(db.table("t").unwrap()), vec![CHUNK_SIZE, 123]);

    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);
    // The CSV loader's chunk boundary (full chunk + remainder) is the same
    // after a `.pink` round-trip.
    assert_eq!(sizes(loaded.table("t").unwrap()), vec![CHUNK_SIZE, 123]);
}

/// Query `db` and return the rows bit-exactly (NaN payloads and `-0.0`
/// compared by bits, like [`assert_same_store`]).
fn select_rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn assert_rows_bit_same(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
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

#[test]
fn attach_of_the_fidelity_store_matches_the_loaded_store() {
    let dir = scratch("attach_fidelity");
    let lake_dir = dir.join("lake");
    let db = common::fidelity_store();
    save_store_lake(&db, &lake_dir).unwrap();
    let lake = open_lake(&lake_dir).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.format_version, FORMAT_VERSION);

    // Descriptor-vs-decoded: every provable statistic must hold, for every
    // table of the fixture.
    let loaded = lake.load(None).unwrap();
    let mut attached_db = PinkDb::new();
    let tables = attached_db.attach_lake(report.lake.clone()).unwrap();
    // The attached catalog is exactly the loaded store's table set.
    let mut attached_keys: Vec<(String, String)> = tables
        .iter()
        .map(|t| (t.db.to_ascii_lowercase(), t.table.to_ascii_lowercase()))
        .collect();
    attached_keys.sort();
    let mut loaded_keys: Vec<(String, String)> = Vec::new();
    for dbname in loaded.list_databases() {
        for name in loaded.list_tables_in(&dbname) {
            loaded_keys.push((dbname.to_ascii_lowercase(), name.to_ascii_lowercase()));
        }
    }
    loaded_keys.sort();
    assert_eq!(attached_keys, loaded_keys);
    for table in &tables {
        let decoded = loaded.table_in(&table.db, &table.table).unwrap();
        assert_eq!(table.rows, decoded.num_rows() as u64);
        assert_eq!(table.schema, decoded.schema());
        for (i, (name, _)) in table.schema.iter().enumerate() {
            let mut nulls = 0u64;
            let (mut min, mut max): (Option<Value>, Option<Value>) = (None, None);
            for row in 0..decoded.num_rows() {
                let v = decoded.get(row, i).unwrap();
                if v.is_null() {
                    nulls += 1;
                    continue;
                }
                if min
                    .as_ref()
                    .is_none_or(|m| pinkdb::plan::compare_values(&v, m).is_lt())
                {
                    min = Some(v.clone());
                }
                if max
                    .as_ref()
                    .is_none_or(|m| pinkdb::plan::compare_values(&v, m).is_gt())
                {
                    max = Some(v);
                }
            }
            assert_eq!(table.stats[i].null_count, Some(nulls), "{name}");
            if let Some(stat) = &table.stats[i].min {
                assert_eq!(Some(stat), min.as_ref(), "{name} min");
            }
            if let Some(stat) = &table.stats[i].max {
                assert_eq!(Some(stat), max.as_ref(), "{name} max");
            }
        }
    }

    // The query battery: attached store vs loaded store, bit-exact.
    let fid_schema = attached_db
        .attached_table_in("main", "fid")
        .unwrap()
        .schema
        .clone();
    let mut mut_attached = attached_db;
    let mut mut_loaded = open_lake(&lake_dir).unwrap().load(None).unwrap();
    let mut queries = vec![
        "SELECT COUNT(*) FROM main.fid;".to_string(),
        "SELECT COUNT(*) FROM big;".to_string(),
        "SELECT COUNT(*) FROM empty;".to_string(),
        "SELECT SUM(i), AVG(i) FROM fid;".to_string(),
        "SELECT COUNT(*) FROM fid WHERE i > 0;".to_string(),
        "SELECT b, COUNT(*) FROM fid GROUP BY b ORDER BY b;".to_string(),
        "SELECT i, g FROM fid ORDER BY i LIMIT 4;".to_string(),
        "SELECT MAX(i) FROM (SELECT i FROM fid) s;".to_string(),
        "SELECT MIN(i) + 1, MAX(g) - 1 FROM fid;".to_string(),
        "SELECT fid.i, big.a FROM fid JOIN big ON fid.i = big.a ORDER BY fid.i LIMIT 3;"
            .to_string(),
        "SELECT COUNT(*) FROM analytics.\"a.b\";".to_string(),
        "SELECT z FROM analytics.\"a.b\";".to_string(),
    ];
    // Every column: COUNT(col) and MIN/MAX (bit-exact comparison decides
    // whether the carrier is a fast path or a fallback).
    for (col, _) in fid_schema {
        queries.push(format!("SELECT COUNT({col}) FROM fid;"));
        queries.push(format!("SELECT MIN({col}), MAX({col}) FROM fid;"));
    }
    for sql in queries {
        let a = select_rows(&mut mut_attached, &sql);
        let b = select_rows(&mut mut_loaded, &sql);
        assert_rows_bit_same(&a, &b, &sql);
    }
}

/// Attach reads the requested version only, and stays pinned to it however
/// the catalog moves.
#[test]
fn attach_is_pinned_to_the_requested_version() {
    let dir = scratch("attach_pinned");
    let lake_dir = dir.join("lake");
    let lake = open_or_create_lake(&lake_dir).unwrap();
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    let mut lake = {
        drop(lake);
        open_lake(&lake_dir).unwrap()
    };
    lake.write_from_store(&db).unwrap(); // version 2: 2 rows
    db.execute("INSERT INTO t VALUES (3);").unwrap();
    lake.write_from_store(&db).unwrap(); // version 3: 3 rows

    let older = open_lake(&lake_dir).unwrap().attach(Some(2)).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(older.lake).unwrap();
    assert_eq!(
        select_rows(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(2)]],
        "the attachment is pinned to version 2"
    );
    let latest = open_lake(&lake_dir).unwrap().attach(None).unwrap();
    let mut attached = PinkDb::new();
    attached.attach_lake(latest.lake).unwrap();
    assert_eq!(
        select_rows(&mut attached, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(3)]]
    );
}

// ---------- time travel and snapshot records (wave 64) ----------

/// A three-version lake: v2 holds `t` with two rows, v3 adds a row, v4 adds
/// another. Returns `(scratch, lake_dir, versions)` where `versions` is the
/// per-version record list read through [`Lake::snapshots`].
fn time_travel_lake(tag: &str) -> (ScratchDir, PathBuf, Vec<pink_lake::SnapshotInfo>) {
    let dir = scratch(tag);
    let lake_dir = dir.join("lake");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
        .unwrap();
    let mut lake = open_or_create_lake(&lake_dir).unwrap();
    db.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two');")
        .unwrap();
    lake.write_from_store(&db).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.execute("INSERT INTO t VALUES (3, 'three');").unwrap();
    lake.write_from_store(&db).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.execute("INSERT INTO t VALUES (4, 'four');").unwrap();
    lake.write_from_store(&db).unwrap();
    let records = open_lake(&lake_dir).unwrap().snapshots().unwrap();
    (dir, lake_dir, records)
}

/// The engine's rows for `sql` on an attached lake, or the error text.
fn try_rows(db: &mut PinkDb, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    match db.execute(sql) {
        Ok(mut results) => match results.pop().unwrap() {
            pinkdb::QueryResult::Selected { rows, .. } => Ok(rows),
            other => panic!("expected rows, got {other:?}"),
        },
        Err(e) => Err(e.to_string()),
    }
}

/// Reading an older version returns exactly that version's rows, and the
/// current version is unchanged by any number of time-travel reads.
#[test]
fn time_travel_reads_exact_old_versions() {
    let (_dir, lake_dir, records) = time_travel_lake("tt_versions");
    assert_eq!(
        records.iter().map(|r| r.version).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    let report = open_lake(&lake_dir).unwrap().attach(None).unwrap();
    assert_eq!(report.version, 4);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();

    // Every version reads back its own rows, in file order.
    assert_eq!(
        try_rows(&mut db, "SELECT x, s FROM t AT (VERSION => 2) ORDER BY x;").unwrap(),
        vec![
            vec![Value::Integer(1), Value::Varchar("one".to_string())],
            vec![Value::Integer(2), Value::Varchar("two".to_string())],
        ]
    );
    assert_eq!(
        try_rows(&mut db, "SELECT x FROM t AT (VERSION => 3) ORDER BY x;").unwrap(),
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    );
    // v1 is the empty lake: the table did not exist yet.
    let err = try_rows(&mut db, "SELECT * FROM t AT (VERSION => 1);").unwrap_err();
    assert!(
        err.contains("does not exist at version 1"),
        "unexpected error: {err}"
    );
    // The current version answers the same after all those reads.
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t;").unwrap(),
        vec![vec![Value::Bigint(4)]]
    );
    // Window clauses, joins, aggregates and aliases all work on a version.
    assert_eq!(
        try_rows(
            &mut db,
            "SELECT COUNT(*), MIN(x), MAX(x) FROM t AS a AT (VERSION => 2);"
        )
        .unwrap(),
        vec![vec![Value::Bigint(2), Value::Integer(1), Value::Integer(2)]]
    );
    assert_eq!(
        try_rows(
            &mut db,
            "SELECT old.x, new.x FROM t old AT (VERSION => 2) \
             JOIN t new AT (VERSION => 4) ON old.x = new.x ORDER BY old.x;"
        )
        .unwrap(),
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2)],
        ]
    );
    // The engine refuses time travel on a store table (DuckLake: "catalog
    // type does not support time travel") and on views; both are clean
    // errors, and the session keeps working.
    db.execute("CREATE TABLE plain (x INTEGER);").unwrap();
    let err = try_rows(&mut db, "SELECT * FROM plain AT (VERSION => 2);").unwrap_err();
    assert!(err.contains("has no snapshot history"), "{err}");
    db.execute("CREATE VIEW v AS SELECT * FROM t;").unwrap();
    let err = try_rows(&mut db, "SELECT * FROM v AT (VERSION => 2);").unwrap_err();
    assert!(err.contains("not available on the view"), "{err}");
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t;").unwrap(),
        vec![vec![Value::Bigint(4)]]
    );
}

/// `AT (TIMESTAMP => …)` resolves to the newest snapshot at or before the
/// timestamp, inclusively; before the first snapshot it is a clean error.
#[test]
fn time_travel_timestamp_resolves_inclusively() {
    let (_dir, lake_dir, records) = time_travel_lake("tt_timestamp");
    // The writer's millisecond times must be strictly increasing across the
    // three saves for this test to have a single right answer; the 5 ms
    // sleeps make that true, and a failure here is a real (not flaky-time)
    // signal.
    for pair in records.windows(2) {
        assert!(
            pair[0].time < pair[1].time,
            "snapshot times must increase: {} then {}",
            pair[0].time,
            pair[1].time
        );
        assert!(pair[0].time_micros.unwrap() < pair[1].time_micros.unwrap());
    }
    let ts = |v: u64| {
        records
            .iter()
            .find(|r| r.version == v)
            .unwrap()
            .time
            .clone()
    };
    let report = open_lake(&lake_dir).unwrap().attach(None).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();

    // The exact publish time of v3 is inside v3 (inclusive), and 1 ms before
    // v4's publish time is still v3.
    assert_eq!(
        try_rows(
            &mut db,
            &format!(
                "SELECT COUNT(*) FROM t AT (TIMESTAMP => TIMESTAMP '{}');",
                ts(3)
            )
        )
        .unwrap(),
        vec![vec![Value::Bigint(3)]]
    );
    let before_v4 = records
        .iter()
        .find(|r| r.version == 4)
        .unwrap()
        .time_micros
        .unwrap()
        - 1_000;
    let before_v4 = pinkdb::types::format_timestamp(before_v4);
    assert_eq!(
        try_rows(
            &mut db,
            &format!("SELECT COUNT(*) FROM t AT (TIMESTAMP => TIMESTAMP '{before_v4}');")
        )
        .unwrap(),
        vec![vec![Value::Bigint(3)]]
    );
    // A bare string literal is accepted too (DuckLake casts it).
    assert_eq!(
        try_rows(
            &mut db,
            &format!("SELECT COUNT(*) FROM t AT (TIMESTAMP => '{}');", ts(2))
        )
        .unwrap(),
        vec![vec![Value::Bigint(2)]]
    );
    // Before the first snapshot: clean error naming the timestamp.
    let before_v1 = records
        .iter()
        .find(|r| r.version == 1)
        .unwrap()
        .time_micros
        .unwrap()
        - 1_000;
    let before_v1 = pinkdb::types::format_timestamp(before_v1);
    let err = try_rows(
        &mut db,
        &format!("SELECT * FROM t AT (TIMESTAMP => TIMESTAMP '{before_v1}');"),
    )
    .unwrap_err();
    assert!(
        err.contains("no snapshot found at timestamp") && err.contains(&before_v1),
        "unexpected error: {err}"
    );
    // A malformed timestamp is a parse error, never a guessed instant.
    let err = try_rows(
        &mut db,
        "SELECT * FROM t AT (TIMESTAMP => TIMESTAMP 'not a time');",
    )
    .unwrap_err();
    assert!(err.contains("parse error"), "{err}");
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t;").unwrap(),
        vec![vec![Value::Bigint(4)]]
    );
}

/// Unknown, unpublished and out-of-range versions are clean errors, and the
/// session keeps answering afterwards.
#[test]
fn time_travel_unknown_versions_are_clean_errors() {
    let (_dir, lake_dir, _) = time_travel_lake("tt_unknown");
    let report = open_lake(&lake_dir).unwrap().attach(None).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();
    for sql in [
        "SELECT * FROM t AT (VERSION => 0);",
        "SELECT * FROM t AT (VERSION => 99);",
        "SELECT * FROM t AT (VERSION => 18446744073709551615);",
    ] {
        let err = try_rows(&mut db, sql).unwrap_err();
        assert!(err.contains("no snapshot found at version"), "{sql}: {err}");
        assert!(err.contains("lake"), "{sql}: {err}");
    }
    // Parse errors: the clause takes exactly the two documented literal
    // forms; everything else fails the parse without disturbing the session.
    for sql in [
        "SELECT * FROM t AT (VERSION 3);",
        "SELECT * FROM t AT VERSION 3;",
        "SELECT * FROM t AT (FOO => 3);",
        "SELECT * FROM t AT ();",
        "SELECT * FROM t AT (VERSION => );",
        "SELECT * FROM t AT (VERSION => 'x');",
        "SELECT * FROM t AT (VERSION => 1.5);",
        "SELECT * FROM t AT (VERSION => 3) AS z;",
    ] {
        let err = try_rows(&mut db, sql).unwrap_err();
        assert!(err.contains("parse error"), "{sql}: {err}");
    }
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t;").unwrap(),
        vec![vec![Value::Bigint(4)]]
    );
    // A table that did not exist at the target version is named in the error.
    let mut db2 = PinkDb::new();
    db2.execute("CREATE TABLE later (y INTEGER);").unwrap();
    let mut lake = open_lake(&lake_dir).unwrap();
    lake.write_from_store(&db2).unwrap();
    let report = lake.attach(None).unwrap();
    let mut db3 = PinkDb::new();
    db3.attach_lake(report.lake.clone()).unwrap();
    let err = try_rows(&mut db3, "SELECT * FROM later AT (VERSION => 2);").unwrap_err();
    assert!(
        err.contains("later") && err.contains("does not exist at version 2"),
        "{err}"
    );
}

/// Statistics fast paths work per version: `COUNT(*)`/`MIN`/`MAX` over
/// `AT (VERSION => n)` answer from that version's footer without decoding,
/// while a filtered read decodes that version's file and caches it.
#[test]
fn time_travel_stats_fast_paths_do_not_decode() {
    let (_dir, lake_dir, _) = time_travel_lake("tt_stats");
    let report = open_lake(&lake_dir).unwrap().attach(None).unwrap();
    let lake = Arc::clone(&report.lake);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t AT (VERSION => 2);").unwrap(),
        vec![vec![Value::Bigint(2)]]
    );
    assert_eq!(
        try_rows(
            &mut db,
            "SELECT MIN(x), MAX(x), COUNT(s) FROM t AT (VERSION => 3);"
        )
        .unwrap(),
        vec![vec![Value::Integer(1), Value::Integer(3), Value::Bigint(3)]]
    );
    let (loads, columns, _files) = lake.decode_stats();
    assert_eq!(
        loads, 0,
        "statistics answers must not decode; {columns} cols"
    );
    // A predicate read materializes v2 only, and a second pass reuses it.
    assert_eq!(
        try_rows(
            &mut db,
            "SELECT COUNT(*) FROM t AT (VERSION => 2) WHERE x > 1;"
        )
        .unwrap(),
        vec![vec![Value::Bigint(1)]]
    );
    assert_eq!(lake.decode_stats().0, 1);
    let _ = try_rows(&mut db, "SELECT SUM(x) FROM t AT (VERSION => 2);").unwrap();
    assert_eq!(
        lake.decode_stats().0,
        2,
        "a pruned load is uncached; the full load then materializes the version"
    );
    // The pinned current version is unaffected (no decode either).
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t;").unwrap(),
        vec![vec![Value::Bigint(4)]]
    );
    assert_eq!(lake.decode_stats().0, 2);
}

/// Time travel never reaches past the version an attachment is pinned to,
/// and the pinned version is served by the attachment itself.
#[test]
fn time_travel_is_capped_at_the_attached_version() {
    let (_dir, lake_dir, _) = time_travel_lake("tt_capped");
    let report = open_lake(&lake_dir).unwrap().attach(Some(2)).unwrap();
    assert_eq!(report.version, 2);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();
    // The pinned tables are v2's; v3/v4 are not reachable from this handle.
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t;").unwrap(),
        vec![vec![Value::Bigint(2)]]
    );
    for sql in [
        "SELECT * FROM t AT (VERSION => 3);",
        "SELECT * FROM t AT (VERSION => 4);",
    ] {
        let err = try_rows(&mut db, sql).unwrap_err();
        assert!(err.contains("no snapshot found at version"), "{sql}: {err}");
    }
    // v1 is reachable (the table did not exist), v2 is the pinned version.
    let err = try_rows(&mut db, "SELECT * FROM t AT (VERSION => 1);").unwrap_err();
    assert!(err.contains("does not exist at version 1"), "{err}");
    assert_eq!(
        try_rows(&mut db, "SELECT COUNT(*) FROM t AT (VERSION => 2);").unwrap(),
        vec![vec![Value::Bigint(2)]]
    );
}

#[test]
fn save_store_creates_open_or_create_and_errors_are_clear() {
    let dir = scratch("create");
    let missing = dir.join("nested/lake-here");
    let db = fixture_store();

    // A fresh (even missing-parent) directory becomes a lake.
    assert_eq!(save_store_lake(&db, &missing).unwrap(), 2);
    assert_eq!(save_store_lake(&db, &missing).unwrap(), 3);
    let lake = open_lake(&missing).unwrap();
    assert_eq!(lake.current_version(), 3);
    assert_eq!(lake.load(None).unwrap().table("t").unwrap().num_rows(), 3);

    // create refuses an existing lake; open refuses a non-lake.
    let err = create_lake(&missing).unwrap_err().to_string();
    assert!(err.contains("already a pink lake"), "{err}");
    let err = open_lake(&dir).unwrap_err().to_string();
    assert!(err.contains("not a pink lake"), "{err}");
}

#[test]
fn a_table_entry_can_list_several_data_files() {
    let dir = scratch("multifile");
    let other = scratch("multifile-src");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
        .unwrap();
    for i in 0..3 {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap();

    // A second lake with the same schema; graft its data file into the first.
    let mut lake2 = create_lake(&other).unwrap();
    let mut db2 = PinkDb::new();
    db2.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
        .unwrap();
    for i in 3..6 {
        db2.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
    }
    lake2.write_from_store(&db2).unwrap();
    let src = data_files(&other.join("data"));
    assert_eq!(src.len(), 1);
    let grafted_name = src[0].file_name().unwrap().to_str().unwrap().to_string();
    let files_dir = dir.join("data/main.t");
    std::fs::copy(&src[0], files_dir.join(&grafted_name)).unwrap();

    // Register the copied file through the public seam: the entry now lists
    // two data files and reads their union in order.
    let rel = format!("data/main.t/{grafted_name}");
    let mut lake = open_lake(&dir).unwrap();
    lake.add_data_files("main.t", &[rel]).unwrap();
    let loaded = lake.load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 6);
    assert_eq!(
        loaded.table("t").unwrap().get(4, 1),
        Some(Value::Varchar("v4".into()))
    );
}

#[test]
fn stale_handles_conflict_instead_of_forking_the_version_chain() {
    let dir = scratch("stale_handle");
    let db = fixture_store();
    let mut a = create_lake(&dir).unwrap();
    a.write_from_store(&db).unwrap(); // v2
    let mut b = open_lake(&dir).unwrap(); // sees v2
    a.write_from_store(&db).unwrap(); // v3

    let data_files_before = data_files(&dir.join("data")).len();
    let err = b.write_from_store(&db).unwrap_err();
    assert!(matches!(&err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("write conflict"), "{msg}");
    assert!(msg.contains("another writer published version 3"), "{msg}");
    assert!(msg.contains("reopen the lake"), "{msg}");
    assert_eq!(b.current_version(), 2, "the stale handle does not advance");
    assert_eq!(
        data_files(&dir.join("data")).len(),
        data_files_before,
        "a stale save writes no data files"
    );

    // Reopening picks up the published version and the chain continues.
    let mut b = open_lake(&dir).unwrap();
    assert_eq!(b.current_version(), 3);
    assert_eq!(b.write_from_store(&db).unwrap(), 4);
    assert_eq!(a.load(Some(3)).unwrap().table("t").unwrap().num_rows(), 3);
}

#[cfg(unix)]
#[test]
fn a_failed_data_write_keeps_the_published_version() {
    use std::os::unix::fs::PermissionsExt;

    let dir = scratch("perm_fail");
    let db = fixture_store();
    let mut lake = create_lake(&dir).unwrap();
    lake.write_from_store(&db).unwrap(); // v2

    // Make one data directory read-only so the next save's data-file write
    // fails mid-save. The rename/fsync machinery must leave v2 published.
    let t_dir = dir.join("data/main.t");
    std::fs::set_permissions(&t_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(t_dir.join("probe"), b"x").is_ok() {
        // Running as root (or on a filesystem ignoring mode bits): the
        // failure cannot be injected, so there is nothing to assert.
        std::fs::remove_file(t_dir.join("probe")).ok();
        std::fs::set_permissions(&t_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    let err = lake.write_from_store(&db).unwrap_err().to_string();
    assert!(err.contains("lake I/O error"), "{err}");
    assert_eq!(lake.current_version(), 2);
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);

    // Restoring permissions recovers; v3 publishes and loads.
    std::fs::set_permissions(&t_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    assert_same_store(&db, &lake.load(None).unwrap());
}

/// Commit messages and change summaries are recorded per version, cleared
/// after the save that publishes them, and survive a reopen.
#[test]
fn snapshot_records_carry_messages_and_change_summaries() {
    let dir = scratch("snapshot_records");
    let lake_dir = dir.join("lake");
    let mut db = PinkDb::new();
    let mut lake = open_or_create_lake(&lake_dir).unwrap();

    // Version 2: two tables, an author and a message.
    db.execute("CREATE TABLE t (x INTEGER);").unwrap();
    db.execute("CREATE TABLE u (y INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    db.execute("INSERT INTO u VALUES (9);").unwrap();
    lake.set_commit_message(Some("alice".to_string()), Some("add t and u".to_string()));
    lake.write_from_store(&db).unwrap();

    // Version 3: rows added, no message set — the pending record must not be
    // repeated.
    db.execute("INSERT INTO t VALUES (3), (4), (5);").unwrap();
    lake.write_from_store(&db).unwrap();

    // Version 4: a message only, "bob" as the author.
    db.execute("DROP TABLE u;").unwrap();
    db.execute("UPDATE t SET x = 0 WHERE x = 1;").unwrap();
    lake.set_commit_message(Some("bob".to_string()), Some("drop u".to_string()));
    lake.write_from_store(&db).unwrap();

    // Reopened from disk, the records are exactly what each save recorded.
    let records = open_lake(&lake_dir).unwrap().snapshots().unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].version, 1);
    assert_eq!(records[0].author, None);
    assert_eq!(records[0].message, None);
    assert_eq!(records[0].changes, None, "create records no change summary");
    assert_eq!((records[0].tables, records[0].rows), (0, 0));

    assert_eq!(records[1].author.as_deref(), Some("alice"));
    assert_eq!(records[1].message.as_deref(), Some("add t and u"));
    assert_eq!((records[1].tables, records[1].rows), (2, 3));
    let changes = records[1].changes.as_ref().unwrap();
    assert_eq!(changes.tables_created, vec!["main.t", "main.u"]);
    assert!(changes.tables_dropped.is_empty());
    assert!(changes.tables_rewritten.is_empty());
    // A created table's rows are the table itself: no row delta is recorded.
    assert!(changes.rows_added.is_empty());

    assert_eq!(records[2].author, None, "the message must not repeat");
    assert_eq!(records[2].message, None);
    let changes = records[2].changes.as_ref().unwrap();
    assert!(changes.tables_created.is_empty());
    // A whole-store save writes fresh files for every surviving table, so
    // both are "rewritten" even though only `t`'s rows moved.
    assert_eq!(changes.tables_rewritten, vec!["main.t", "main.u"]);
    assert_eq!(changes.rows_added.get("main.t"), Some(&3));
    assert_eq!(records[2].tables, 2);

    assert_eq!(records[3].author.as_deref(), Some("bob"));
    assert_eq!(records[3].message.as_deref(), Some("drop u"));
    let changes = records[3].changes.as_ref().unwrap();
    assert_eq!(changes.tables_dropped, vec!["main.u"]);
    assert_eq!(changes.tables_rewritten, vec!["main.t"]);
    assert!(
        changes.rows_added.is_empty(),
        "x=1 became x=0, no row moved"
    );
    assert!((records[3].tables, records[3].rows) == (1, 5));
    for record in &records {
        assert!(record.time_micros.is_some(), "{}", record.time);
    }

    // A save that fails keeps the pending record for the retry — and
    // publishes nothing.
    let mut stale = open_lake(&lake_dir).unwrap();
    open_lake(&lake_dir).unwrap().write_from_store(&db).unwrap();
    stale.set_commit_message(Some("carol".to_string()), Some("retry me".to_string()));
    assert!(
        stale.write_from_store(&db).is_err(),
        "stale handle conflicts"
    );
    let records_after = open_lake(&lake_dir).unwrap().snapshots().unwrap();
    assert_eq!(records_after.len(), 5);
    assert_eq!(records_after[3].message.as_deref(), Some("drop u"));
    assert_eq!(records_after[4].message, None, "the winner records nothing");
    assert!(
        records_after
            .iter()
            .all(|r| r.message.as_deref() != Some("retry me")),
        "the failed save's pending record must not publish"
    );
}

/// Attach refusals: not a lake, a v1 lake, a missing data file, a row-count
/// mismatch, and a two-file table.
#[test]
fn attach_refusals_are_clean_errors() {
    // Not a lake.
    let dir = scratch("attach_refuse");
    let missing = dir.join("nope");
    let err = open_lake(&missing).unwrap_err().to_string();
    assert!(err.contains("not a pink lake"), "{err}");

    // A missing data file.
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store_lake(&db, &lake_dir).unwrap();
    for path in data_files(&lake_dir.join("data")) {
        std::fs::remove_file(&path).unwrap();
    }
    // Attach itself never probes a data file (statistics-only); the first
    // read is what reports the missing file.
    let lake = open_lake(&lake_dir).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.tables, 5, "the descriptors still list every table");
    let err = lake.load(None).unwrap_err().to_string();
    assert!(
        err.contains("is missing") || err.contains("loading data file"),
        "{err}"
    );

    // A row-count mismatch: the catalog's record_count is negative (corrupt
    // rows are a clean error, never a panic). A lifted file is detected by
    // the loader when the file's actual rows disagree.
    let lake_dir = dir.join("wrong_rows");
    save_store_lake(&db, &lake_dir).unwrap();
    catalog_sql(
        &lake_dir,
        "UPDATE ducklake_data_file SET record_count = -1;",
    );
    let err = open_lake(&lake_dir)
        .and_then(|lake| lake.attach(None))
        .unwrap_err()
        .to_string();
    assert!(err.contains("corrupt") || err.contains("record"), "{err}");

    // A two-file table attaches: the descriptors merge the files' statistics,
    // and materialization unions them in order.
    let lake_dir = dir.join("two_files");
    save_store_lake(&db, &lake_dir).unwrap();
    let files: Vec<PathBuf> = data_files(&lake_dir.join("data"))
        .into_iter()
        .filter(|p| p.parent().is_some_and(|d| d.ends_with("main.t")))
        .collect();
    assert_eq!(files.len(), 1);
    let mut lake = open_lake(&lake_dir).unwrap();
    let data_dir = files[0].parent().unwrap().to_path_buf();
    let ext = files[0]
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("pink");
    let second = data_dir.join(format!("second.{ext}"));
    std::fs::copy(&files[0], &second).unwrap();
    let rel_second = second
        .strip_prefix(&lake_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let key = "main.t".to_string();
    let original = lake
        .load(None)
        .unwrap()
        .table_in("main", "t")
        .unwrap()
        .num_rows() as u64;
    lake.add_data_files(&key, &[rel_second]).unwrap();
    let report = lake.attach(None).unwrap();
    let attached = report.lake;
    let table = attached
        .tables()
        .iter()
        .find(|t| format!("{}.{}", t.db, t.table) == key)
        .unwrap();
    let first_rows = files.len() as u64; // one file per original table
    assert_eq!(table.rows, original * 2, "both files are listed");
    let union_rows = attached.load(&table.db, &table.table).unwrap();
    assert_eq!(union_rows.num_rows() as u64, original * 2);
    let _ = first_rows;
}

#[test]
fn an_interrupted_create_is_healed_not_bricked() {
    let dir = scratch("interrupted_create");
    let db = fixture_store();
    // A create that died after the catalog database file appeared but before
    // its first transaction leaves an empty `pink:` DB. Opening it as a lake
    // says "not a lake"; open-or-create initializes it instead of bricking.
    let catalog = catalog_path(&dir);
    std::fs::create_dir_all(&*dir).unwrap();
    drop(PinkDb::open_durable(&catalog.display().to_string()).unwrap());
    let mut lake = open_or_create_lake(&dir).unwrap();
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    assert_eq!(lake.load(None).unwrap().table("t").unwrap().num_rows(), 3);
}

/// M-5: every lake artifact a save publishes is owner-only (0600), and an
/// artifact a pre-fix build left world-readable is narrowed on the next open.
#[cfg(unix)]
#[test]
fn lake_artifacts_are_owner_only_and_existing_ones_are_tightened() {
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files_under(&path, out);
            } else {
                out.push(path);
            }
        }
    }

    let dir = scratch("artifact_modes");
    let db = fixture_store();
    save_store_lake(&db, &dir).unwrap();

    let mut files = Vec::new();
    files_under(&dir, &mut files);
    assert!(!files.is_empty());
    for path in &files {
        assert_eq!(
            mode(path),
            0o600,
            "lake artifact {} is world/group accessible",
            path.display()
        );
    }

    // A pre-fix lake: chmod the catalog and one data file to 0644, then open.
    let catalog = {
        let wal = PathBuf::from(format!("{}.wal", catalog_path(&dir).display()));
        if wal.exists() {
            wal
        } else {
            catalog_path(&dir)
        }
    };
    let data = data_files(&dir.join("data")).into_iter().next().unwrap();
    std::fs::set_permissions(&catalog, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o644)).unwrap();
    let _lake = open_lake(&dir).unwrap();
    assert_eq!(mode(&catalog), 0o600, "catalog was not tightened on open");
    assert_eq!(mode(&data), 0o600, "data file was not tightened on open");
}

#[test]
fn corrupt_metadata_is_a_clear_error_not_a_panic() {
    let db = fixture_store();

    // A negative record count is corrupt rows, not a saturating zero.
    let dir = scratch("corrupt_rows");
    save_store_lake(&db, &dir).unwrap();
    catalog_sql(&dir, "UPDATE ducklake_data_file SET record_count = -1;");
    let err = open_lake(&dir)
        .and_then(|lake| lake.load(None))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("corrupt") && err.contains("record_count"),
        "{err}"
    );

    // A data-file path that escapes the table directory is refused.
    let dir = scratch("corrupt_escape");
    save_store_lake(&db, &dir).unwrap();
    catalog_sql(&dir, "UPDATE ducklake_data_file SET path = '../evil.pink';");
    let err = open_lake(&dir)
        .and_then(|lake| lake.attach(None))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("corrupt") || err.contains("evil.pink"),
        "{err}"
    );

    // An unknown file format is a per-table refusal, never a panic.
    let dir = scratch("corrupt_format");
    save_store_lake(&db, &dir).unwrap();
    catalog_sql(&dir, "UPDATE pinkdb_table SET format = 'no-such-format';");
    let lake = open_lake(&dir).unwrap();
    let unavailable = lake.unavailable_tables().unwrap();
    assert!(
        unavailable.iter().any(|t| t.format == "no-such-format"),
        "{unavailable:?}"
    );

    // A referenced data file that does not exist is a clean load error.
    let dir = scratch("corrupt_missing");
    save_store_lake(&db, &dir).unwrap();
    catalog_sql(&dir, "UPDATE ducklake_data_file SET path = 'missing.pink';");
    let err = open_lake(&dir)
        .and_then(|lake| lake.load(None))
        .unwrap_err()
        .to_string();
    assert!(err.contains("missing.pink"), "{err}");
}
