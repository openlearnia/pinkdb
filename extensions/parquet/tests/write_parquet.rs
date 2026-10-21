//! Round-trip tests for the wave-87 parquet **writer**: every supported
//! engine type is encoded with `pinkdb_parquet::write::write_table` and read
//! back through the crate's **existing** reader (the `read_parquet` table
//! source), with edge values compared bit-exactly.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parquet::file::reader::{FileReader, SerializedFileReader};
use pinkdb::{DataTable, DataType, TableFunctionArg, TableFunctionCall, Value};
use pinkdb_parquet::write::{WriteError, read_identity, write_table, write_table_with_identity};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_parquet_write_{tag}_{}_{}",
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

fn table(name: &str, schema: Vec<(String, DataType)>, rows: Vec<Vec<Value>>) -> DataTable {
    let mut out = DataTable::new(name.to_string(), schema).unwrap();
    for row in rows {
        out.append_row(row).unwrap();
    }
    out
}

/// Write `src` to a file and decode it through the existing reader,
/// returning the reader's schema and rows.
fn round_trip(src: &DataTable, dir: &Path) -> (Vec<(String, DataType)>, Vec<Vec<Value>>) {
    let bytes = write_table(src).unwrap();
    let path = dir.join("data.parquet");
    std::fs::write(&path, &bytes).unwrap();
    read_rows(&path, src.schema())
}

fn read_rows(
    path: &Path,
    expected_schema: Vec<(String, DataType)>,
) -> (Vec<(String, DataType)>, Vec<Vec<Value>>) {
    let source = pinkdb_parquet::source();
    let call = TableFunctionCall::Function {
        name: "read_parquet".to_string(),
        args: vec![TableFunctionArg {
            name: None,
            value: Value::Varchar(path.display().to_string()),
        }],
    };
    let schema = source
        .bind(&call)
        .unwrap()
        .expect("the parquet source claims read_parquet");
    assert_eq!(schema, expected_schema, "reader schema");
    let output = source.scan(&call, None, &[]).unwrap();
    let mut rows = Vec::new();
    for chunk in &output.chunks {
        for r in 0..chunk.num_rows {
            rows.push((0..schema.len()).map(|c| chunk.columns[c].get(r)).collect());
        }
    }
    (schema, rows)
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn assert_rows_equal(expected: &[Vec<Value>], actual: &[Vec<Value>], what: &str) {
    assert_eq!(actual.len(), expected.len(), "{what}: row count");
    for (r, (want, got)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(got.len(), want.len(), "{what}: row {r} width");
        for (c, (want, got)) in want.iter().zip(got).enumerate() {
            assert!(
                values_equal(want, got),
                "{what}: row {r} column {c}: expected {want:?}, got {got:?}"
            );
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

#[test]
fn every_supported_type_round_trips_through_the_existing_reader() {
    let dir = scratch("types");
    let src = table("t", all_types_schema(), all_types_rows());
    let (schema, rows) = round_trip(&src, &dir);
    assert_eq!(schema, all_types_schema());
    assert_rows_equal(&all_types_rows(), &rows, "all types");
    cleanup(&dir);
}

#[test]
fn empty_tables_and_null_only_columns_round_trip() {
    let dir = scratch("empty");
    // A schema-only file: zero row groups, the schema still binds.
    let empty = table("e", all_types_schema(), Vec::new());
    let (schema, rows) = round_trip(&empty, &dir);
    assert_eq!(schema, all_types_schema());
    assert!(rows.is_empty(), "{rows:?}");

    // A NULL-only column in a non-empty table keeps its count.
    let nulls = table(
        "n",
        vec![
            ("a".to_string(), DataType::Integer),
            ("only_null".to_string(), DataType::Bigint),
        ],
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Null],
        ],
    );
    let (_, rows) = round_trip(&nulls, &dir);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Null],
        ]
    );
    cleanup(&dir);
}

#[test]
fn a_large_table_writes_multiple_row_groups_with_statistics() {
    let dir = scratch("rowgroups");
    let schema = vec![
        ("i".to_string(), DataType::Integer),
        ("s".to_string(), DataType::Varchar),
    ];
    let rows: Vec<Vec<Value>> = (0..70_000i32)
        .map(|i| vec![Value::Integer(i), Value::Varchar(format!("row-{i}"))])
        .collect();
    let src = table("big", schema.clone(), rows.clone());
    let (read_schema, read) = round_trip(&src, &dir);
    assert_eq!(read_schema, schema);
    assert_rows_equal(&rows, &read, "large table");

    let path = dir.join("data.parquet");
    let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    let metadata = reader.metadata();
    assert!(
        metadata.num_row_groups() >= 2,
        "70,000 rows must span multiple row groups, got {}",
        metadata.num_row_groups()
    );
    let mut total = 0;
    for group in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(group);
        total += rg.num_rows();
        for column in 0..rg.num_columns() {
            let stats = rg
                .column(column)
                .statistics()
                .unwrap_or_else(|| panic!("row group {group} column {column} has no statistics"));
            assert!(
                stats.null_count_opt().is_some(),
                "row group {group} column {column} has no NULL count"
            );
        }
        assert!(
            rg.column(0).statistics().unwrap().min_bytes_opt().is_some(),
            "row group {group} column 0 has no min statistic"
        );
        assert!(
            rg.column(0).statistics().unwrap().max_bytes_opt().is_some(),
            "row group {group} column 0 has no max statistic"
        );
    }
    assert_eq!(total, 70_000);

    // The all-NULL case reports the exact NULL count in the file statistics.
    let nulls = table(
        "nulls",
        vec![("x".to_string(), DataType::Integer)],
        vec![vec![Value::Null]; 3],
    );
    let (_, _) = round_trip(&nulls, &dir);
    let reader =
        SerializedFileReader::new(std::fs::File::open(dir.join("data.parquet")).unwrap()).unwrap();
    let stats = reader
        .metadata()
        .row_group(0)
        .column(0)
        .statistics()
        .unwrap();
    assert_eq!(stats.null_count_opt(), Some(3));
    cleanup(&dir);
}

#[test]
fn interval_is_refused_naming_the_column() {
    let dir = scratch("interval");
    let src = table(
        "iv",
        vec![
            ("ok".to_string(), DataType::Integer),
            ("gap".to_string(), DataType::Interval),
        ],
        vec![vec![
            Value::Integer(1),
            Value::Interval {
                months: 1,
                days: 2,
                micros: 3,
            },
        ]],
    );
    let err = write_table(&src).unwrap_err();
    assert!(matches!(err, WriteError::UnsupportedType { .. }), "{err:?}");
    let text = err.to_string();
    assert!(text.contains("gap"), "must name the column: {text}");
    assert!(
        text.to_lowercase().contains("interval"),
        "must say why: {text}"
    );
    assert!(
        text.contains("microsecond") && text.contains("millisecond"),
        "must explain the precision loss: {text}"
    );
    assert!(!dir.join("data.parquet").exists());
    cleanup(&dir);
}

#[test]
fn lake_identity_is_written_to_the_footer() {
    let dir = scratch("identity");
    let src = table(
        "t",
        vec![("i".to_string(), DataType::Integer)],
        vec![vec![Value::Integer(7)]],
    );
    let bytes = write_table_with_identity(&src, "analytics", "events").unwrap();
    let path = dir.join("data.parquet");
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(
        read_identity(&path).unwrap(),
        Some(("analytics".to_string(), "events".to_string()))
    );
    // A plain `write_table` file carries no identity.
    let plain = write_table(&src).unwrap();
    std::fs::write(&path, plain).unwrap();
    assert_eq!(read_identity(&path).unwrap(), None);
    cleanup(&dir);
}
