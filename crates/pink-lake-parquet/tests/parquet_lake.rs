//! Wave 87 / task T2: the Parquet lake format provider, end to end.
//!
//! A store holding every supported type (multi-table, multi-database, an
//! empty table, all-NULL columns, doubles with NaN/-0.0/±inf, a 70,000-row
//! table) is saved as a parquet lake. It must reopen/open identically,
//! attach answer the same `COUNT`/`MIN`/`MAX` facts as `open`, append through
//! a commit, survive merge maintenance, and refuse an INTERVAL table without
//! publishing anything.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::format::LakeFormat;
use pink_lake::{Commit, Lake, LakeError, MergeSettings};
use pink_lake_parquet::{FORMAT_ID, ParquetLakeFormat, register};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_parquet_{tag}_{}_{}",
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

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn assert_rows_equal(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
    assert_eq!(a.len(), b.len(), "{what}: row count");
    for (r, (ra, rb)) in a.iter().zip(b).enumerate() {
        assert_eq!(ra.len(), rb.len(), "{what}: row {r} width");
        for (c, (va, vb)) in ra.iter().zip(rb).enumerate() {
            assert!(
                values_equal(va, vb),
                "{what}: row {r} column {c}: {va:?} vs {vb:?}"
            );
        }
    }
}

fn assert_store_equal(a: &PinkDb, b: &PinkDb, what: &str) {
    assert_eq!(a.list_databases(), b.list_databases(), "{what}: databases");
    for db in a.list_databases() {
        assert_eq!(
            a.list_tables_in(&db),
            b.list_tables_in(&db),
            "{what}: tables of {db}"
        );
        for name in a.list_tables_in(&db) {
            let ta = a.table_in(&db, &name).unwrap();
            let tb = b.table_in(&db, &name).unwrap();
            assert_eq!(ta.schema(), tb.schema(), "{what}: {db}.{name} schema");
            assert_eq!(
                ta.num_rows(),
                tb.num_rows(),
                "{what}: {db}.{name} row count"
            );
            for r in 0..ta.num_rows() {
                for c in 0..ta.columns.len() {
                    let va = ta.get(r, c).unwrap();
                    let vb = tb.get(r, c).unwrap();
                    assert!(
                        values_equal(&va, &vb),
                        "{what}: {db}.{name} row {r} column {}: {va:?} vs {vb:?}",
                        ta.columns[c].name
                    );
                }
            }
        }
    }
}

fn all_types_schema() -> Vec<(String, DataType)> {
    vec![
        ("b".to_string(), DataType::Boolean),
        ("i".to_string(), DataType::Integer),
        ("g".to_string(), DataType::Bigint),
        ("d".to_string(), DataType::Double),
        ("s".to_string(), DataType::Varchar),
        ("dt".to_string(), DataType::Date),
        ("ts".to_string(), DataType::Timestamp),
        (
            "dc9".to_string(),
            DataType::Decimal {
                precision: 9,
                scale: 2,
            },
        ),
        (
            "dc18".to_string(),
            DataType::Decimal {
                precision: 18,
                scale: 4,
            },
        ),
        (
            "dc38".to_string(),
            DataType::Decimal {
                precision: 38,
                scale: 6,
            },
        ),
        ("nulls".to_string(), DataType::Integer),
    ]
}

fn all_types_rows() -> Vec<Vec<Value>> {
    let max38 = 10i128.pow(38) - 1;
    vec![
        vec![
            Value::Boolean(true),
            Value::Integer(i32::MIN),
            Value::Bigint(i64::MIN),
            Value::Double(f64::NAN),
            Value::Varchar(String::new()),
            Value::Date(i32::MIN),
            Value::Timestamp(i64::MIN),
            Value::Decimal(-999_999_999, 2),
            Value::Decimal(-999_999_999_999_999_999, 4),
            Value::Decimal(-max38, 6),
            Value::Null,
        ],
        vec![
            Value::Boolean(false),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MAX),
            Value::Double(-0.0),
            Value::Varchar("héllo 世界 🚀".to_string()),
            Value::Date(i32::MAX),
            Value::Timestamp(i64::MAX),
            Value::Decimal(999_999_999, 2),
            Value::Decimal(999_999_999_999_999_999, 4),
            Value::Decimal(max38, 6),
            Value::Null,
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Double(f64::INFINITY),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            Value::Boolean(true),
            Value::Integer(0),
            Value::Bigint(0),
            Value::Double(f64::NEG_INFINITY),
            Value::Varchar("x".repeat(5_000)),
            Value::Date(0),
            Value::Timestamp(0),
            Value::Decimal(0, 2),
            Value::Decimal(0, 4),
            Value::Decimal(0, 6),
            Value::Null,
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    ]
}

fn append_rows(target: &mut DataTable, src: &DataTable) {
    for c in 0..src.num_chunks() {
        target
            .append_shared_chunk(&src.scan_chunk(c).unwrap().columns)
            .unwrap();
    }
}

/// The corpus store: every type, two databases, an empty table, an all-NULL
/// column, and a 70,000-row table (multiple parquet row groups).
fn corpus() -> PinkDb {
    let mut db = PinkDb::new();
    db.create_table_in("main", "t", all_types_schema()).unwrap();
    {
        let table = db.table_mut_in("main", "t").unwrap();
        for row in all_types_rows() {
            table.append_row(row).unwrap();
        }
    }
    db.create_table_in(
        "main",
        "empty",
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    db.create_database("analytics").unwrap();
    db.create_table_in(
        "analytics",
        "u",
        vec![
            ("k".to_string(), DataType::Bigint),
            ("v".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    {
        let table = db.table_mut_in("analytics", "u").unwrap();
        for (k, v) in [(1i64, "one"), (2, "two"), (3, "three")] {
            table
                .append_row(vec![Value::Bigint(k), Value::Varchar(v.to_string())])
                .unwrap();
        }
    }
    // 70,000 rows: more than one 64K-row parquet row group.
    let big_schema = vec![
        ("i".to_string(), DataType::Integer),
        ("s".to_string(), DataType::Varchar),
    ];
    let mut big = DataTable::new("big".to_string(), big_schema.clone()).unwrap();
    for i in 0..70_000i32 {
        big.append_row(vec![Value::Integer(i), Value::Varchar(format!("row-{i}"))])
            .unwrap();
    }
    db.create_table_in("main", "big", big_schema).unwrap();
    append_rows(db.table_mut_in("main", "big").unwrap(), &big);
    db
}

fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&dir.join("data"), &mut out);
    out.sort();
    out
}

#[test]
fn register_is_idempotent() {
    register().unwrap();
    register().unwrap();
    assert!(pink_lake::format::lake_format(FORMAT_ID).is_some());
    let provider = ParquetLakeFormat::new();
    assert_eq!(provider.id(), FORMAT_ID);
    assert_eq!(provider.extension(), "parquet");
}

#[test]
fn save_reopen_open_and_attach_agree_for_every_type() {
    register().unwrap();
    let dir = scratch("roundtrip");
    let lake_dir = dir.join("lake");
    let source = corpus();

    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    let report = lake.write_from_store_report(&source).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.tables, 4);

    // The catalog pins the provider on every table and every data file is a
    // `.parquet` file.
    let catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(lake_dir.join("catalog.json")).unwrap()).unwrap();
    for key in ["main.t", "main.empty", "main.big", "analytics.u"] {
        assert_eq!(catalog["tables"][key]["format"], FORMAT_ID, "{catalog}");
        let files = catalog["tables"][key]["files"].as_array().unwrap();
        assert_eq!(files.len(), 1, "{key}: {catalog}");
        let rel = files[0]["path"].as_str().unwrap();
        assert!(rel.ends_with(".parquet"), "{rel}");
    }
    let files = parquet_files(&lake_dir);
    assert_eq!(files.len(), 4, "{files:?}");

    // Reopen → load equals the source, type for type and value for value.
    let reopened = Lake::open(&lake_dir).unwrap();
    let opened = reopened.load(None).unwrap();
    assert_store_equal(&source, &opened, "lake open");

    // Attach answers the same COUNT/MIN/MAX facts as the open path.
    let attachment = reopened.attach(None).unwrap();
    assert!(
        attachment.statistics_only,
        "a format without footer statistics attaches from the manifest block"
    );
    let mut attached = PinkDb::new();
    attached.attach_lake(attachment.lake).unwrap();

    let mut opened = reopened.load(None).unwrap();
    let mut stats_queries = vec![
        "SELECT COUNT(*), COUNT(nulls), MIN(i), MAX(i), MIN(g), MAX(g), \
         MIN(s), MAX(s), MIN(dt), MAX(dt), MIN(ts), MAX(ts), \
         MIN(dc9), MAX(dc9), MIN(dc18), MAX(dc18), MIN(dc38), MAX(dc38), \
         MIN(b), MAX(b), MIN(d), MAX(d) FROM t;"
            .to_string(),
        "SELECT COUNT(*), COUNT(i), MIN(i), MAX(i) FROM empty;".to_string(),
        "SELECT COUNT(*), MIN(k), MAX(k), MIN(v), MAX(v) FROM analytics.u;".to_string(),
        "SELECT COUNT(*), MIN(i), MAX(i), MIN(s), MAX(s) FROM big;".to_string(),
    ];
    for sql in stats_queries.drain(..) {
        let want = query(&mut opened, &sql);
        let got = query(&mut attached, &sql);
        assert_rows_equal(&want, &got, &sql);
    }
    // The NaN-bearing DOUBLE column's MIN/MAX fell back to the decoded copy;
    // its answers still match, and the sanity facts are the source's.
    assert_eq!(
        query(&mut opened, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;"),
        vec![vec![
            Value::Bigint(5),
            Value::Integer(i32::MIN),
            Value::Integer(i32::MAX),
        ]]
    );

    cleanup(&dir);
}

/// A light store with a single table for the append/merge test.
fn light_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.create_table_in(
        "main",
        "t",
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    {
        let table = db.table_mut_in("main", "t").unwrap();
        for i in 0..4i32 {
            table
                .append_row(vec![Value::Integer(i), Value::Varchar(format!("r{i}"))])
                .unwrap();
        }
    }
    db
}

fn appended_table(rows: &[(i32, &str)]) -> Arc<DataTable> {
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

#[test]
fn commit_append_and_merge_maintenance_stay_parquet() {
    register().unwrap();
    let dir = scratch("append");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.write_from_store_report(&light_store()).unwrap();

    // Append through a commit: the handle's provider writes the new file.
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append(
        "main.t",
        appended_table(&[(10, "ten"), (11, "eleven"), (12, "twelve")]),
    );
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.version, 3);
    let catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(lake_dir.join("catalog.json")).unwrap()).unwrap();
    assert_eq!(
        catalog["tables"]["main.t"]["format"], FORMAT_ID,
        "{catalog}"
    );
    let files = catalog["tables"]["main.t"]["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "{catalog}");
    for file in files {
        assert!(
            file["path"].as_str().unwrap().ends_with(".parquet"),
            "{file}"
        );
    }

    let loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(loaded.table_in("main", "t").unwrap().num_rows(), 7);

    // Merge the two files: maintenance re-encodes under the entry's format.
    let mut lake = Lake::open(&lake_dir).unwrap();
    let report = lake
        .merge_adjacent_files(Some("main.t"), &MergeSettings::default())
        .unwrap();
    assert!(report.committed, "two same-epoch files are mergeable");
    assert_eq!(report.files_processed(), 2);
    assert_eq!(report.files_created(), 1);

    let catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(lake_dir.join("catalog.json")).unwrap()).unwrap();
    assert_eq!(
        catalog["tables"]["main.t"]["format"], FORMAT_ID,
        "{catalog}"
    );
    let files = catalog["tables"]["main.t"]["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{catalog}");
    assert!(
        files[0]["path"].as_str().unwrap().ends_with(".parquet"),
        "{catalog}"
    );

    let mut loaded = Lake::open(&lake_dir).unwrap().load(None).unwrap();
    assert_eq!(
        query(
            &mut loaded,
            "SELECT COUNT(*), MIN(i), MAX(i), MIN(s), MAX(s) FROM t;"
        ),
        vec![vec![
            Value::Bigint(7),
            Value::Integer(0),
            Value::Integer(12),
            Value::Varchar("eleven".to_string()),
            Value::Varchar("twelve".to_string()),
        ]]
    );
    cleanup(&dir);
}

#[test]
fn an_interval_table_is_refused_and_nothing_is_published() {
    register().unwrap();
    let dir = scratch("interval");
    let lake_dir = dir.join("lake");
    let mut store = PinkDb::new();
    store
        .create_table_in(
            "main",
            "gaps",
            vec![
                ("id".to_string(), DataType::Integer),
                ("took".to_string(), DataType::Interval),
            ],
        )
        .unwrap();
    {
        let table = store.table_mut_in("main", "gaps").unwrap();
        table
            .append_row(vec![
                Value::Integer(1),
                Value::Interval {
                    months: 1,
                    days: 2,
                    micros: 3,
                },
            ])
            .unwrap();
    }

    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    let err = lake.write_from_store_report(&store).unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    let text = err.to_string();
    assert!(text.contains("took"), "must name the column: {text}");
    assert!(
        text.to_lowercase().contains("interval"),
        "must name the type: {text}"
    );

    // Nothing was published: the lake is still at its empty version 1 and
    // holds no data files.
    assert_eq!(lake.current_version(), 1);
    let loaded = lake.load(None).unwrap();
    for db in loaded.list_databases() {
        assert!(
            loaded.list_tables_in(&db).is_empty(),
            "nothing may be published, found {db}"
        );
    }
    assert!(parquet_files(&lake_dir).is_empty());
    cleanup(&dir);
}
