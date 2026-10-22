//! Wave 88, plan rule 7: **one format contract** for every lake data-file
//! provider.
//!
//! [`run_contract`] drives one provider through the promises the
//! [`pink_lake::format::LakeFormat`] seam makes to the lake:
//!
//! 1. every engine column type round-trips value-for-value — NULLs in every
//!    column, NaN, `-0.0`, `+inf`/`-inf`, the `i32`/`i64` extremes, unicode
//!    and empty strings, an empty table, and a 70,000-row table (several
//!    engine chunks, and two Parquet row groups) — through `save` → a fresh
//!    `Lake::open` → `load`;
//! 2. a type listed in [`ContractOptions::refused_types`] fails the save with
//!    an error and leaves the lake at its previous version (never a lossy
//!    write);
//! 3. `attach` answers exactly the same `COUNT(*)`/`COUNT`/`MIN`/`MAX` facts
//!    as the loaded store, column by column;
//! 4. an append keeps the entry's format (catalog `format`, data-file
//!    extensions) and its data;
//! 5. `rewrite_table_format` pink → provider → pink preserves the data
//!    exactly (for `pink` itself, the same-format rewrite is the documented
//!    no-op);
//! 6. an explicit per-table format choice pins the catalog entry
//!    (`"format_pinned": true`).
//!
//! Every assertion names the format id under test, so one failing provider is
//! unambiguous when the same body runs for `pink`, `vortex` and `parquet` in
//! one test binary.
//!
//! This file is included through `#[path]` by
//! `crates/pink-lake/tests/format_contract.rs` and
//! `crates/pink-lake-parquet/tests/format_contract.rs`. It deliberately does
//! not live in `common/mod.rs`: the other test binaries that declare
//! `mod common;` would then compile (and warn about) this module without
//! using it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::format::{PINK_FORMAT_ID, lake_format};
use pink_lake::{Commit, Lake};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

/// What one provider is allowed to refuse.
#[derive(Debug, Clone, Default)]
pub struct ContractOptions {
    /// Column types the provider may refuse to store. The round-trip corpus
    /// leaves them out; the refusal check requires a save holding one of them
    /// to fail cleanly and publish nothing.
    pub refused_types: Vec<DataType>,
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(format_id: &str, tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_contract_{format_id}_{tag}_{}_{}",
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

/// The provider's data-file extension; also proves the id has a provider.
fn provider_extension(format_id: &str) -> String {
    lake_format(format_id)
        .unwrap_or_else(|| {
            panic!("{format_id}: no lake format provider is registered in this process")
        })
        .extension()
        .to_string()
}

fn schema_with_types(entries: &[(&str, DataType)]) -> Vec<(String, DataType)> {
    entries
        .iter()
        .map(|(name, dtype)| ((*name).to_string(), *dtype))
        .collect()
}

/// Every column type the engine supports.
fn engine_types() -> Vec<(String, DataType)> {
    schema_with_types(&[
        ("b", DataType::Boolean),
        ("i", DataType::Integer),
        ("g", DataType::Bigint),
        ("d", DataType::Double),
        ("v", DataType::Varchar),
        ("dt", DataType::Date),
        ("ts", DataType::Timestamp),
        (
            "dc9",
            DataType::Decimal {
                precision: 9,
                scale: 2,
            },
        ),
        (
            "dc18",
            DataType::Decimal {
                precision: 18,
                scale: 4,
            },
        ),
        (
            "dc38",
            DataType::Decimal {
                precision: 38,
                scale: 6,
            },
        ),
        ("iv", DataType::Interval),
    ])
}

fn supported_schema(opts: &ContractOptions) -> Vec<(String, DataType)> {
    engine_types()
        .into_iter()
        .filter(|(_, dtype)| !opts.refused_types.contains(dtype))
        .collect()
}

/// Eight values per type: every column gets NULLs (rows 2 and 5) and the
/// interesting extremes.
fn special_values(dtype: DataType) -> Vec<Value> {
    match dtype {
        DataType::Boolean => vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
        ],
        DataType::Integer => vec![
            Value::Integer(i32::MIN),
            Value::Integer(i32::MAX),
            Value::Null,
            Value::Integer(0),
            Value::Integer(-1),
            Value::Null,
            Value::Integer(1_234_567),
            Value::Integer(-42),
        ],
        DataType::Bigint => vec![
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Null,
            Value::Bigint(0),
            Value::Bigint(-1),
            Value::Null,
            Value::Bigint(9_007_199_254_740_993),
            Value::Bigint(-42),
        ],
        DataType::Double => vec![
            Value::Double(f64::NAN),
            Value::Double(-0.0),
            Value::Null,
            Value::Double(f64::INFINITY),
            Value::Double(f64::NEG_INFINITY),
            Value::Null,
            Value::Double(1.5),
            Value::Double(-2.25),
        ],
        DataType::Varchar => vec![
            Value::Varchar(String::new()),
            Value::Varchar("héllo 世界 🚀".to_string()),
            Value::Null,
            Value::Varchar("plain ascii".to_string()),
            Value::Varchar("tab\tnewline\nquote\"".to_string()),
            Value::Null,
            Value::Varchar("x".repeat(1_000)),
            Value::Varchar("snake_case-42".to_string()),
        ],
        DataType::Date => vec![
            Value::Date(i32::MIN),
            Value::Date(i32::MAX),
            Value::Null,
            Value::Date(0),
            Value::Date(-1),
            Value::Null,
            Value::Date(19_000),
            Value::Date(-19_000),
        ],
        DataType::Timestamp => vec![
            Value::Timestamp(i64::MIN),
            Value::Timestamp(i64::MAX),
            Value::Null,
            Value::Timestamp(0),
            Value::Timestamp(-1),
            Value::Null,
            Value::Timestamp(1_700_000_000_000_000),
            Value::Timestamp(-1_000_000),
        ],
        DataType::Decimal { precision, scale } => {
            let max = 10i128.pow(u32::from(precision)) - 1;
            vec![
                Value::Decimal(-max, scale),
                Value::Decimal(max, scale),
                Value::Null,
                Value::Decimal(0, scale),
                Value::Decimal(-1, scale),
                Value::Null,
                Value::Decimal(max / 3, scale),
                Value::Decimal(-42, scale),
            ]
        }
        DataType::Interval => vec![
            Value::Interval {
                months: -12,
                days: -30,
                micros: i64::MIN,
            },
            Value::Interval {
                months: 12,
                days: 30,
                micros: i64::MAX,
            },
            Value::Null,
            Value::Interval {
                months: 0,
                days: 0,
                micros: 0,
            },
            Value::Interval {
                months: 1,
                days: 2,
                micros: 3,
            },
            Value::Null,
            Value::Interval {
                months: -1,
                days: -2,
                micros: -3,
            },
            Value::Interval {
                months: 24,
                days: 7,
                micros: 1,
            },
        ],
    }
}

/// The all-types row set.
fn all_types_rows(schema: &[(String, DataType)]) -> Vec<Vec<Value>> {
    let columns: Vec<Vec<Value>> = schema
        .iter()
        .map(|(_, dtype)| special_values(*dtype))
        .collect();
    let rows = columns.first().map_or(0, Vec::len);
    (0..rows)
        .map(|row| columns.iter().map(|values| values[row].clone()).collect())
        .collect()
}

fn add_table(db: &mut PinkDb, name: &str, schema: &[(String, DataType)], rows: &[Vec<Value>]) {
    db.create_table_in("main", name, schema.to_vec()).unwrap();
    let table = db.table_mut_in("main", name).unwrap();
    for row in rows {
        table.append_row(row.clone()).unwrap();
    }
}

/// Rows appended by step 4; also folded into the expected store.
fn append_rows() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Integer(10),
            Value::Varchar("ten ✓".to_string()),
            Value::Double(-0.0),
        ],
        vec![Value::Null, Value::Null, Value::Null],
        vec![
            Value::Integer(i32::MAX),
            Value::Varchar(String::new()),
            Value::Double(f64::NAN),
        ],
    ]
}

fn append_table() -> Arc<DataTable> {
    let schema = schema_with_types(&[
        ("i", DataType::Integer),
        ("s", DataType::Varchar),
        ("d", DataType::Double),
    ]);
    let mut table = DataTable::new("appended".to_string(), schema).unwrap();
    for row in append_rows() {
        table.append_row(row).unwrap();
    }
    Arc::new(table)
}

/// A non-NULL value of `dtype`, for the refusal store.
fn sample_value(dtype: DataType) -> Value {
    match dtype {
        DataType::Boolean => Value::Boolean(true),
        DataType::Integer => Value::Integer(1),
        DataType::Bigint => Value::Bigint(1),
        DataType::Double => Value::Double(1.5),
        DataType::Varchar => Value::Varchar("x".to_string()),
        DataType::Date => Value::Date(1),
        DataType::Timestamp => Value::Timestamp(1),
        DataType::Decimal { scale, .. } => Value::Decimal(1, scale),
        DataType::Interval => Value::Interval {
            months: 1,
            days: 2,
            micros: 3,
        },
    }
}

fn contract_store(opts: &ContractOptions) -> PinkDb {
    let mut db = PinkDb::new();
    let schema = supported_schema(opts);
    add_table(&mut db, "types", &schema, &all_types_rows(&schema));

    let simple = schema_with_types(&[("i", DataType::Integer), ("s", DataType::Varchar)]);
    // The empty table carries every supported type, so a provider's empty-file
    // schema is exercised, not just an empty two-column one.
    add_table(&mut db, "empty", &schema, &[]);
    add_table(
        &mut db,
        "nulls",
        &simple,
        &[
            vec![Value::Null, Value::Null],
            vec![Value::Null, Value::Null],
            vec![Value::Null, Value::Null],
        ],
    );

    let appended_schema = schema_with_types(&[
        ("i", DataType::Integer),
        ("s", DataType::Varchar),
        ("d", DataType::Double),
    ]);
    add_table(
        &mut db,
        "appended",
        &appended_schema,
        &[
            vec![
                Value::Integer(1),
                Value::Varchar("one".to_string()),
                Value::Double(1.5),
            ],
            vec![Value::Null, Value::Null, Value::Null],
            vec![
                Value::Integer(3),
                Value::Varchar("three".to_string()),
                Value::Double(f64::NEG_INFINITY),
            ],
        ],
    );

    // 70,000 rows: 35 engine chunks (CHUNK_SIZE is 2,048) and two Parquet
    // row groups (64K rows each), so every provider spans several batches.
    let big_schema = schema_with_types(&[("i", DataType::Integer), ("s", DataType::Varchar)]);
    db.create_table_in("main", "big", big_schema).unwrap();
    {
        let table = db.table_mut_in("main", "big").unwrap();
        for i in 0..70_000i32 {
            table
                .append_row(vec![
                    if i % 10_000 == 0 {
                        Value::Null
                    } else {
                        Value::Integer(i)
                    },
                    Value::Varchar(format!("row-{i}")),
                ])
                .unwrap();
        }
    }
    db
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // NaN equals NaN (a provider may canonicalize the payload); otherwise
        // bit-exact, so -0.0 keeps its sign and is distinct from 0.0.
        (Value::Double(x), Value::Double(y)) => {
            (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits()
        }
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

fn assert_rows_equal(format_id: &str, what: &str, want: &[Vec<Value>], got: &[Vec<Value>]) {
    assert_eq!(
        want.len(),
        got.len(),
        "{format_id}: {what}: row count: {want:?} vs {got:?}"
    );
    for (row, (a, b)) in want.iter().zip(got).enumerate() {
        assert_eq!(
            a.len(),
            b.len(),
            "{format_id}: {what}: row {row} width: {a:?} vs {b:?}"
        );
        for (column, (x, y)) in a.iter().zip(b).enumerate() {
            assert!(
                values_equal(x, y),
                "{format_id}: {what}: row {row} column {column}: {x:?} vs {y:?}"
            );
        }
    }
}

fn assert_table_equal(format_id: &str, what: &str, want: &DataTable, got: &DataTable) {
    assert_eq!(want.schema(), got.schema(), "{format_id}: {what}: schema");
    assert_eq!(
        want.num_rows(),
        got.num_rows(),
        "{format_id}: {what}: row count"
    );
    for row in 0..want.num_rows() {
        for (column, definition) in want.columns.iter().enumerate() {
            let a = want.get(row, column).unwrap();
            let b = got.get(row, column).unwrap();
            assert!(
                values_equal(&a, &b),
                "{format_id}: {what}: row {row} column {:?}: {a:?} vs {b:?}",
                definition.name
            );
        }
    }
}

fn assert_store_equal(format_id: &str, what: &str, want: &PinkDb, got: &PinkDb) {
    assert_eq!(
        want.list_databases(),
        got.list_databases(),
        "{format_id}: {what}: databases"
    );
    for db in want.list_databases() {
        assert_eq!(
            want.list_tables_in(&db),
            got.list_tables_in(&db),
            "{format_id}: {what}: tables in {db}"
        );
        for name in want.list_tables_in(&db) {
            let a = want.table_in(&db, &name).unwrap();
            let b = got.table_in(&db, &name).unwrap();
            assert_table_equal(format_id, &format!("{what}: {db}.{name}"), a, b);
        }
    }
}

fn read_catalog(lake_dir: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(lake_dir.join("catalog.json")).unwrap()).unwrap()
}

fn assert_entry_format(label: &str, catalog: &serde_json::Value, key: &str, id: &str) {
    let entry = &catalog["tables"][key];
    assert_eq!(
        entry["format"], id,
        "{label}: catalog entry {key}: {catalog}"
    );
    let extension = provider_extension(id);
    let files = entry["files"]
        .as_array()
        .unwrap_or_else(|| panic!("{label}: catalog entry {key} has no files: {catalog}"));
    assert!(
        !files.is_empty(),
        "{label}: catalog entry {key} lists no data files: {catalog}"
    );
    for file in files {
        let path = file["path"]
            .as_str()
            .unwrap_or_else(|| panic!("{label}: catalog entry {key}: bad file: {file}"));
        let seen = Path::new(path)
            .extension()
            .map(|ext| ext.to_string_lossy())
            .unwrap_or_default();
        assert_eq!(
            seen.as_ref(),
            extension.as_str(),
            "{label}: catalog entry {key} data file {path} is not a .{extension} file: \
             {catalog}"
        );
    }
}

/// Step 2: a type the provider cannot store fails the save, and the lake stays
/// at its previous version with its previous data.
fn refusal_check(format_id: &str, opts: &ContractOptions, dir: &Path) {
    if opts.refused_types.is_empty() {
        return;
    }
    let lake_dir = dir.join("refusal_lake");
    let mut good = PinkDb::new();
    add_table(
        &mut good,
        "kept",
        &schema_with_types(&[("i", DataType::Integer)]),
        &[
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ],
    );
    let mut lake =
        Lake::create(&lake_dir).unwrap_or_else(|e| panic!("{format_id}: refusal lake create: {e}"));
    lake.set_write_format(format_id)
        .unwrap_or_else(|e| panic!("{format_id}: refusal lake set_write_format: {e}"));
    lake.write_from_store_report(&good)
        .unwrap_or_else(|e| panic!("{format_id}: refusal lake baseline save: {e}"));
    let before = lake.current_version();

    for (index, refused) in opts.refused_types.iter().enumerate() {
        let table = format!("refused_{index}");
        let mut bad = PinkDb::new();
        add_table(
            &mut bad,
            &table,
            &schema_with_types(&[("id", DataType::Integer), ("blocked", *refused)]),
            &[vec![Value::Integer(1), sample_value(*refused)]],
        );
        let err = lake.write_from_store_report(&bad).err().unwrap_or_else(|| {
            panic!(
                "{format_id}: saving a {refused:?} column must fail cleanly, not publish a \
                 lossy write"
            )
        });
        let text = err.to_string();
        let needle = refused.name().to_lowercase();
        assert!(
            text.to_lowercase().contains(&needle),
            "{format_id}: the refusal of {refused:?} must name the type: {text}"
        );
        assert_eq!(
            lake.current_version(),
            before,
            "{format_id}: a refused {refused:?} save must leave the lake at version {before}"
        );
        let reloaded = Lake::open(&lake_dir)
            .unwrap_or_else(|e| panic!("{format_id}: reopen after a refused save: {e}"))
            .load(None)
            .unwrap_or_else(|e| panic!("{format_id}: load after a refused save: {e}"));
        assert_store_equal(
            format_id,
            &format!("refusal of {refused:?}"),
            &good,
            &reloaded,
        );
    }
}

/// Step 4: a commit append keeps the entry's format and its data.
fn append_check(format_id: &str, source: &PinkDb, lake_dir: &Path) {
    let mut lake = Lake::open(lake_dir).unwrap_or_else(|e| panic!("{format_id}: append-open: {e}"));
    if format_id != PINK_FORMAT_ID {
        // A different handle default must not change an existing entry: rule 3
        // says an append always uses the entry's format.
        lake.set_write_format(PINK_FORMAT_ID)
            .unwrap_or_else(|e| panic!("{format_id}: append set_write_format(pink): {e}"));
    }
    lake.set_data_inlining_row_limit(0);
    let mut commit = Commit::new();
    commit.append("main.appended", append_table());
    let report = lake
        .commit(&commit)
        .unwrap_or_else(|e| panic!("{format_id}: append commit: {e}"));
    assert!(
        report.committed,
        "{format_id}: the append must publish a version: {report:?}"
    );

    let catalog = read_catalog(lake_dir);
    assert_entry_format(format_id, &catalog, "main.appended", format_id);
    let files = catalog["tables"]["main.appended"]["files"]
        .as_array()
        .unwrap_or_else(|| panic!("{format_id}: appended entry has no files: {catalog}"));
    assert_eq!(
        files.len(),
        2,
        "{format_id}: one append must add exactly one data file: {catalog}"
    );

    let mut expected = source.clone();
    for row in append_rows() {
        expected
            .table_mut_in("main", "appended")
            .unwrap()
            .append_row(row)
            .unwrap();
    }
    let loaded = Lake::open(lake_dir)
        .unwrap_or_else(|e| panic!("{format_id}: reopen after append: {e}"))
        .load(None)
        .unwrap_or_else(|e| panic!("{format_id}: load after append: {e}"));
    assert_store_equal(format_id, "append", &expected, &loaded);
}

/// Step 5: rewriting pink → this provider → pink preserves the data exactly.
fn rewrite_check(format_id: &str, opts: &ContractOptions, dir: &Path) {
    let lake_dir = dir.join("rewrite_lake");
    let mut source = PinkDb::new();
    let schema = supported_schema(opts);
    // The rewrite path never attaches, so it keeps the full extremes (for
    // `pink`; every other provider refuses INTERVAL).
    add_table(&mut source, "types", &schema, &all_types_rows(&schema));

    let mut lake =
        Lake::create(&lake_dir).unwrap_or_else(|e| panic!("{format_id}: rewrite lake create: {e}"));
    lake.set_write_format(PINK_FORMAT_ID)
        .unwrap_or_else(|e| panic!("{format_id}: rewrite lake set_write_format(pink): {e}"));
    lake.write_from_store_report(&source)
        .unwrap_or_else(|e| panic!("{format_id}: rewrite lake baseline save: {e}"));
    let before = lake.current_version();

    if format_id == PINK_FORMAT_ID {
        let report = lake
            .rewrite_table_format("main", "types", PINK_FORMAT_ID)
            .unwrap_or_else(|e| panic!("{format_id}: pink rewrite: {e}"));
        assert!(
            !report.committed,
            "{format_id}: rewriting pink to pink must be the documented no-op: {report:?}"
        );
        assert_eq!(
            report.version, before,
            "{format_id}: the no-op rewrite must not publish a version: {report:?}"
        );
        assert_eq!(
            lake.current_version(),
            before,
            "{format_id}: the no-op rewrite must keep the lake at version {before}"
        );
    } else {
        let report = lake
            .rewrite_table_format("main", "types", format_id)
            .unwrap_or_else(|e| panic!("{format_id}: rewrite pink -> {format_id}: {e}"));
        assert!(
            report.committed,
            "{format_id}: pink -> {format_id} must publish a version: {report:?}"
        );
        let catalog = read_catalog(&lake_dir);
        assert_entry_format(format_id, &catalog, "main.types", format_id);
        let loaded = lake
            .load(None)
            .unwrap_or_else(|e| panic!("{format_id}: load after pink -> {format_id}: {e}"));
        assert_table_equal(
            format_id,
            &format!("rewrite pink -> {format_id}"),
            source.table_in("main", "types").unwrap(),
            loaded.table_in("main", "types").unwrap(),
        );

        let report = lake
            .rewrite_table_format("main", "types", PINK_FORMAT_ID)
            .unwrap_or_else(|e| panic!("{format_id}: rewrite {format_id} -> pink: {e}"));
        assert!(
            report.committed,
            "{format_id}: {format_id} -> pink must publish a version: {report:?}"
        );
        let catalog = read_catalog(&lake_dir);
        assert_entry_format(format_id, &catalog, "main.types", PINK_FORMAT_ID);
        let loaded = lake
            .load(None)
            .unwrap_or_else(|e| panic!("{format_id}: load after {format_id} -> pink: {e}"));
        assert_table_equal(
            format_id,
            &format!("rewrite {format_id} -> pink"),
            source.table_in("main", "types").unwrap(),
            loaded.table_in("main", "types").unwrap(),
        );
    }
}

/// Step 6: an explicit per-table choice records `"format_pinned": true`.
fn pinned_check(format_id: &str, dir: &Path) {
    let lake_dir = dir.join("pinned_lake");
    let mut lake =
        Lake::create(&lake_dir).unwrap_or_else(|e| panic!("{format_id}: pinned lake create: {e}"));
    lake.set_table_format("main", "t", format_id)
        .unwrap_or_else(|e| panic!("{format_id}: set_table_format: {e}"));
    let mut store = PinkDb::new();
    add_table(
        &mut store,
        "t",
        &schema_with_types(&[("i", DataType::Integer)]),
        &[vec![Value::Integer(7)]],
    );
    lake.write_from_store_report(&store)
        .unwrap_or_else(|e| panic!("{format_id}: pinned lake save: {e}"));
    let catalog = read_catalog(&lake_dir);
    let entry = &catalog["tables"]["main.t"];
    assert_eq!(
        entry["format"], format_id,
        "{format_id}: an explicit per-table choice must select the entry's format: {catalog}"
    );
    assert_eq!(
        entry["format_pinned"],
        serde_json::json!(true),
        "{format_id}: an explicit per-table choice must record \"format_pinned\": true: {catalog}"
    );
}

/// Step 3: `attach` answers exactly the same per-column COUNT/MIN/MAX facts as
/// the loaded store.
fn attach_check(format_id: &str, lake: &Lake, source: &PinkDb, opened: &mut PinkDb) {
    let attachment = lake
        .attach(None)
        .unwrap_or_else(|e| panic!("{format_id}: attach: {e}"));
    let mut attached = PinkDb::new();
    attached
        .attach_lake(attachment.lake)
        .unwrap_or_else(|e| panic!("{format_id}: attach_lake: {e}"));
    for db in source.list_databases() {
        for table in source.list_tables_in(&db) {
            let columns = source.table_in(&db, &table).unwrap().schema();
            for (column, _) in &columns {
                let sql = format!(
                    "SELECT COUNT(*), COUNT({column}), MIN({column}), MAX({column}) \
                     FROM {db}.{table};"
                );
                let want = query(opened, &sql);
                let got = query(&mut attached, &sql);
                assert_rows_equal(format_id, &format!("attach: {sql}"), &want, &got);
            }
        }
    }
}

/// Run the whole wave-88 format contract against `format_id`.
///
/// `format_id` must have a provider registered in this process (`pink` and,
/// with `vortexlake`, `vortex` always do; `parquet` is registered by its
/// crate). Every assertion names the id.
pub fn run_contract(format_id: &str, opts: ContractOptions) {
    // Fail before doing any work when the binary has no such provider.
    provider_extension(format_id);

    let dir = scratch(format_id, "contract");
    let lake_dir = dir.join("lake");
    let source = contract_store(&opts);

    // 1. save -> fresh open -> load equals the source, value for value.
    let mut lake =
        Lake::create(&lake_dir).unwrap_or_else(|e| panic!("{format_id}: create lake: {e}"));
    lake.set_write_format(format_id)
        .unwrap_or_else(|e| panic!("{format_id}: set_write_format: {e}"));
    let report = lake
        .write_from_store_report(&source)
        .unwrap_or_else(|e| panic!("{format_id}: initial save: {e}"));
    assert!(
        report.version >= 2,
        "{format_id}: the initial save must publish a version: {report:?}"
    );
    // The explicit write format must be what every entry and data file carry,
    // not the build default.
    let catalog = read_catalog(&lake_dir);
    for key in [
        "main.types",
        "main.empty",
        "main.nulls",
        "main.appended",
        "main.big",
    ] {
        assert_entry_format(format_id, &catalog, key, format_id);
    }
    let reopened = Lake::open(&lake_dir).unwrap_or_else(|e| panic!("{format_id}: fresh open: {e}"));
    let mut opened = reopened
        .load(None)
        .unwrap_or_else(|e| panic!("{format_id}: load: {e}"));
    assert_store_equal(format_id, "save -> open", &source, &opened);

    // 2. a refused type fails cleanly and publishes nothing.
    refusal_check(format_id, &opts, &dir);

    // 3. attach answers the same COUNT(*)/COUNT/MIN/MAX facts as open.
    attach_check(format_id, &reopened, &source, &mut opened);

    // 4. an append keeps the entry's format and its data.
    append_check(format_id, &source, &lake_dir);

    // 5. rewrite pink -> provider -> pink preserves the data.
    rewrite_check(format_id, &opts, &dir);

    // 6. an explicit per-table choice pins the entry.
    pinned_check(format_id, &dir);

    cleanup(&dir);
}
