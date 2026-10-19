//! End-to-end tests for the parquet table source: fixtures are written by the
//! same `parquet` crate's low-level writer (so the reader is exercised against
//! a second implementation's encodings), and the committed DuckDB-written
//! fixtures under `bench/parquet_fixtures` cross-check the read path against
//! files the reference engine produced.

use std::sync::Arc;

use parquet::basic::{Compression, ConvertedType, Repetition, Type as PhysicalType};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type as SchemaType;
use pinkdb::{CHUNK_SIZE, DataType, PinkDb, Value};

/// A column of an in-test parquet file: the schema element plus its values
/// (`None` = NULL).
enum Column {
    Int32(&'static str, Vec<Option<i32>>),
    Int64(&'static str, Vec<Option<i64>>),
    Double(&'static str, Vec<Option<f64>>),
    Str(&'static str, Vec<Option<&'static str>>),
    Date(&'static str, Vec<Option<i32>>),
    TimestampMicros(&'static str, Vec<Option<i64>>),
    Int32Decimal {
        name: &'static str,
        precision: i32,
        scale: i32,
        values: Vec<Option<i32>>,
    },
}

impl Column {
    fn name(&self) -> &'static str {
        match self {
            Column::Int32(n, _)
            | Column::Int64(n, _)
            | Column::Double(n, _)
            | Column::Str(n, _)
            | Column::Date(n, _)
            | Column::TimestampMicros(n, _) => n,
            Column::Int32Decimal { name, .. } => name,
        }
    }

    fn nullable(&self) -> bool {
        match self {
            Column::Int32(_, v) => v.iter().any(Option::is_none),
            Column::Int64(_, v) => v.iter().any(Option::is_none),
            Column::Double(_, v) => v.iter().any(Option::is_none),
            Column::Str(_, v) => v.iter().any(Option::is_none),
            Column::Date(_, v) => v.iter().any(Option::is_none),
            Column::TimestampMicros(_, v) => v.iter().any(Option::is_none),
            Column::Int32Decimal { values, .. } => values.iter().any(Option::is_none),
        }
    }

    fn schema(&self) -> SchemaType {
        let repetition = if self.nullable() {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        };
        let builder = |physical| {
            let mut b = SchemaType::primitive_type_builder(self.name(), physical)
                .with_repetition(repetition);
            if let Column::Date(..) = self {
                b = b.with_converted_type(ConvertedType::DATE);
            }
            if let Column::TimestampMicros(..) = self {
                b = b.with_converted_type(ConvertedType::TIMESTAMP_MICROS);
            }
            if let Column::Int32Decimal {
                precision, scale, ..
            } = self
            {
                b = b
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_precision(*precision)
                    .with_scale(*scale);
            }
            b
        };
        let physical = match self {
            Column::Int32(..) | Column::Date(..) | Column::Int32Decimal { .. } => {
                PhysicalType::INT32
            }
            Column::Int64(..) | Column::TimestampMicros(..) => PhysicalType::INT64,
            Column::Double(..) => PhysicalType::DOUBLE,
            Column::Str(..) => PhysicalType::BYTE_ARRAY,
        };
        builder(physical).build().unwrap()
    }

    fn rows(&self) -> usize {
        match self {
            Column::Int32(_, v) => v.len(),
            Column::Int64(_, v) => v.len(),
            Column::Double(_, v) => v.len(),
            Column::Str(_, v) => v.len(),
            Column::Date(_, v) => v.len(),
            Column::TimestampMicros(_, v) => v.len(),
            Column::Int32Decimal { values, .. } => values.len(),
        }
    }

    /// The values for rows `[start, start + len)` as a write payload.
    fn slice(&self, start: usize, len: usize) -> Payload {
        match self {
            Column::Int32(_, v) => {
                let window = &v[start..start + len];
                Payload::Int32(window.iter().flatten().copied().collect(), levels(window))
            }
            Column::Date(_, v) => {
                let window = &v[start..start + len];
                Payload::Int32(window.iter().flatten().copied().collect(), levels(window))
            }
            Column::Int32Decimal { values, .. } => {
                let window = &values[start..start + len];
                Payload::Int32(window.iter().flatten().copied().collect(), levels(window))
            }
            Column::Int64(_, v) | Column::TimestampMicros(_, v) => {
                let window = &v[start..start + len];
                Payload::Int64(window.iter().flatten().copied().collect(), levels(window))
            }
            Column::Double(_, v) => {
                let window = &v[start..start + len];
                Payload::Double(window.iter().flatten().copied().collect(), levels(window))
            }
            Column::Str(_, v) => {
                let window = &v[start..start + len];
                Payload::Str(
                    window
                        .iter()
                        .flatten()
                        .map(|s| ByteArray::from(s.as_bytes().to_vec()))
                        .collect(),
                    levels(window),
                )
            }
        }
    }
}

enum Payload {
    Int32(Vec<i32>, Option<Vec<i16>>),
    Int64(Vec<i64>, Option<Vec<i16>>),
    Double(Vec<f64>, Option<Vec<i16>>),
    Str(Vec<ByteArray>, Option<Vec<i16>>),
}

fn levels<T>(values: &[Option<T>]) -> Option<Vec<i16>> {
    if values.iter().all(Option::is_some) {
        // A required column needs no definition levels.
        None
    } else {
        Some(
            values
                .iter()
                .map(|v| if v.is_some() { 1 } else { 0 })
                .collect(),
        )
    }
}

/// Write one flat parquet file, splitting rows into row groups of
/// `rows_per_group` (`0` = one group for everything).
fn write_file(
    path: &str,
    columns: &[Column],
    rows_per_group: usize,
    compression: Compression,
    kv: Vec<(&str, &str)>,
) {
    let rows = columns.first().map(Column::rows).unwrap_or(0);
    assert!(columns.iter().all(|c| c.rows() == rows));
    let schema = Arc::new(
        SchemaType::group_type_builder("pinkdb_schema")
            .with_fields(columns.iter().map(|c| Arc::new(c.schema())).collect())
            .build()
            .unwrap(),
    );
    let mut props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(if rows_per_group == 0 {
            rows.max(1)
        } else {
            rows_per_group
        }));
    if !kv.is_empty() {
        props = props.set_key_value_metadata(Some(
            kv.into_iter()
                .map(|(k, v)| {
                    parquet::file::metadata::KeyValue::new(k.to_string(), Some(v.to_string()))
                })
                .collect(),
        ));
    }
    let props = Arc::new(props.build());
    let file = std::fs::File::create(path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut written = 0usize;
    while written < rows {
        let n = if rows_per_group == 0 {
            rows - written
        } else {
            rows_per_group.min(rows - written)
        };
        let mut row_group = writer.next_row_group().unwrap();
        for column in columns {
            let mut col_writer = row_group.next_column().unwrap().unwrap();
            match column.slice(written, n) {
                Payload::Int32(values, def) => {
                    col_writer
                        .typed::<Int32Type>()
                        .write_batch(&values, def.as_deref(), None)
                        .unwrap();
                }
                Payload::Int64(values, def) => {
                    col_writer
                        .typed::<Int64Type>()
                        .write_batch(&values, def.as_deref(), None)
                        .unwrap();
                }
                Payload::Double(values, def) => {
                    col_writer
                        .typed::<DoubleType>()
                        .write_batch(&values, def.as_deref(), None)
                        .unwrap();
                }
                Payload::Str(values, def) => {
                    col_writer
                        .typed::<ByteArrayType>()
                        .write_batch(&values, def.as_deref(), None)
                        .unwrap();
                }
            }
            col_writer.close().unwrap();
        }
        row_group.close().unwrap();
        written += n;
    }
    writer.close().unwrap();
}

/// A fresh store with the parquet source registered.
fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.register_table_source(pinkdb_parquet::source());
    db
}

fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).expect("query");
    match out.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The parquet source's counters after the last statement.
fn stats(db: &PinkDb) -> pinkdb::TableSourceStats {
    db.table_source_stats()
        .pop()
        .expect("the source is registered")
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-parquet-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The repository's committed DuckDB-written fixtures.
fn bench_fixture(relative: &str) -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    root.join("bench/parquet_fixtures")
        .join(relative)
        .to_string_lossy()
        .to_string()
}

#[test]
fn reads_types_nulls_and_projection() {
    let dir = temp_dir("types");
    let path = dir.join("types.parquet");
    write_file(
        path.to_str().unwrap(),
        &[
            Column::Int32("i", vec![Some(1), Some(-2), None]),
            Column::Int64("b", vec![Some(10), None, Some(30)]),
            Column::Double("d", vec![Some(1.5), Some(-0.25), None]),
            Column::Str("s", vec![Some("hello"), None, Some("héllo")]),
            Column::Date("dt", vec![Some(0), Some(19723), None]),
            Column::TimestampMicros("ts", vec![Some(1_000_000), None, Some(-1)]),
            Column::Int32Decimal {
                name: "dec",
                precision: 9,
                scale: 2,
                values: vec![Some(125), None, Some(-3)],
            },
        ],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let mut db = store();
    let rows = select(
        &mut db,
        &format!(
            "SELECT i, b, d, s, dt, ts, dec FROM '{}' ORDER BY i NULLS LAST;",
            path.display()
        ),
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(-2));
    assert_eq!(rows[1][0], Value::Integer(1));
    assert_eq!(rows[1][4], Value::Date(0));
    assert_eq!(rows[1][5], Value::Timestamp(1_000_000));
    assert_eq!(rows[1][6], Value::Decimal(125, 2));
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[2][0], Value::Null);
    assert_eq!(rows[2][2], Value::Null);

    // Types are pinned through a typed comparison and CAST.
    let rows = select(
        &mut db,
        &format!(
            "SELECT i + 1, CAST(dec AS DOUBLE) FROM '{}' WHERE i = 1;",
            path.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[0][1], Value::Double(1.25));

    // Projection pushdown: one column decoded.
    db.table_sources();
    let fresh = store();
    let mut db = fresh;
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT) FROM '{}' WHERE i IS NOT NULL;",
            path.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(2));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn projection_and_filter_pruning_are_visible_in_counters() {
    let dir = temp_dir("prune");
    let path = dir.join("rg.parquet");
    let values: Vec<Option<i32>> = (0..8192).map(Some).collect();
    write_file(
        path.to_str().unwrap(),
        &[
            Column::Int32("id", values.clone()),
            Column::Str("label", (0..8192).map(|_| Some("x")).collect()),
        ],
        2048,
        Compression::SNAPPY,
        vec![],
    );
    let mut db = store();
    // Everything: 4 row groups visited, none pruned, both columns decoded.
    let rows = select(
        &mut db,
        &format!("SELECT CAST(count(*) AS BIGINT) FROM '{}';", path.display()),
    );
    assert_eq!(rows[0][0], Value::Bigint(8192));
    let s = stats(&db);
    assert_eq!((s.files, s.row_groups, s.row_groups_pruned), (1, 4, 0));

    // A range filter prunes three of four row groups.
    db.execute("SELECT 1;").unwrap();
    let before = stats(&db);
    let rows = select(
        &mut db,
        &format!(
            "SELECT id FROM '{}' WHERE id >= 7000 ORDER BY id;",
            path.display()
        ),
    );
    assert_eq!(rows.len(), 1192);
    assert_eq!(rows[0][0], Value::Integer(7000));
    let after = stats(&db);
    assert_eq!(after.row_groups - before.row_groups, 1);
    assert_eq!(after.row_groups_pruned - before.row_groups_pruned, 3);
    // Only the selected column was decoded.
    assert_eq!(after.columns_decoded - before.columns_decoded, 1);

    // An unsatisfiable filter prunes every row group.
    db.execute("SELECT 1;").unwrap();
    let before = stats(&db);
    let rows = select(
        &mut db,
        &format!("SELECT id FROM '{}' WHERE id > 100000;", path.display()),
    );
    assert!(rows.is_empty());
    let after = stats(&db);
    assert_eq!(after.row_groups - before.row_groups, 0);
    assert_eq!(after.row_groups_pruned - before.row_groups_pruned, 4);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn globs_multi_file_and_filename_columns() {
    let dir = temp_dir("glob");
    std::fs::create_dir_all(dir.join("b")).unwrap();
    std::fs::create_dir_all(dir.join("a")).unwrap();
    write_file(
        dir.join("b/one.parquet").to_str().unwrap(),
        &[Column::Int32("id", vec![Some(2)])],
        0,
        Compression::SNAPPY,
        vec![],
    );
    write_file(
        dir.join("a/two.parquet").to_str().unwrap(),
        &[Column::Int32("id", vec![Some(1)])],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let mut db = store();
    // Sorted by path: a/two before b/one.
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, filename, file_row_number FROM read_parquet('{}/*/*.parquet', filename := TRUE, file_row_number := TRUE);",
            dir.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Integer(1));
    assert!(matches!(&rows[0][1], Value::Varchar(p) if p.ends_with("a/two.parquet")));
    assert_eq!(rows[0][2], Value::Bigint(0));
    assert_eq!(rows[1][0], Value::Integer(2));

    // A directory path reads every parquet file under it, recursively.
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT) FROM read_parquet('{}');",
            dir.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(2));

    // A glob that matches nothing is α's error; `allow_empty` is empty.
    let err = db
        .execute(&format!(
            "SELECT * FROM read_parquet('{}/*.nope');",
            dir.display()
        ))
        .unwrap_err();
    assert!(err.to_string().contains("No files found"), "{err}");
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT) FROM read_parquet('{}/*.nope', allow_empty := TRUE);",
            dir.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(0));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn union_by_name_and_schema_mismatch() {
    let dir = temp_dir("union");
    write_file(
        dir.join("a.parquet").to_str().unwrap(),
        &[
            Column::Int32("id", vec![Some(1)]),
            Column::Str("name", vec![Some("a")]),
            Column::Double("score", vec![Some(1.5)]),
        ],
        0,
        Compression::SNAPPY,
        vec![],
    );
    write_file(
        dir.join("b.parquet").to_str().unwrap(),
        &[
            Column::Str("name", vec![Some("b")]),
            Column::Int32("id", vec![Some(2)]),
            Column::Double("score", vec![Some(2.5)]),
            Column::Int64("extra", vec![Some(7)]),
        ],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let mut db = store();
    // Without union_by_name the first file's schema is the reference: columns
    // are resolved by name, an extra column is ignored, and a later file's
    // type is cast into the reference's (α's rules — `extra` is not in the
    // output and id=2 reads from the reordered b.parquet).
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, name, score FROM read_parquet('{}/*.parquet') ORDER BY id;",
            dir.display()
        ),
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[1][1], Value::Varchar("b".to_string()));

    // A BIGINT column casts into the reference's INTEGER (range-checked).
    write_file(
        dir.join("c.parquet").to_str().unwrap(),
        &[
            Column::Int64("id", vec![Some(3)]),
            Column::Str("name", vec![Some("c")]),
            Column::Double("score", vec![Some(3.5)]),
        ],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let rows = select(
        &mut db,
        &format!(
            "SELECT id FROM read_parquet('{}/*.parquet') ORDER BY id;",
            dir.display()
        ),
    );
    assert_eq!(rows.last().unwrap()[0], Value::Integer(3));

    // A *missing* column in a later file is α's schema-mismatch error.
    let missing = dir.join("missing");
    std::fs::create_dir_all(&missing).unwrap();
    write_file(
        missing.join("a.parquet").to_str().unwrap(),
        &[Column::Int32("id", vec![Some(1)])],
        0,
        Compression::SNAPPY,
        vec![],
    );
    write_file(
        missing.join("b.parquet").to_str().unwrap(),
        &[Column::Int32("other", vec![Some(2)])],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let err = db
        .execute(&format!(
            "SELECT * FROM read_parquet('{}/*.parquet');",
            missing.display()
        ))
        .unwrap_err();
    assert!(err.to_string().contains("schema mismatch"), "{err}");
    assert!(err.to_string().contains("union_by_name"), "{err}");

    // union_by_name: the union schema, a missing column reading NULL, and the
    // extra column appearing.
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, name, score, extra FROM read_parquet('{}/*.parquet', union_by_name := TRUE) ORDER BY id;",
            dir.display()
        ),
    );
    assert_eq!(rows.len(), 3);
    // id is INTEGER in a.parquet and BIGINT in c.parquet: the union widens it.
    assert_eq!(rows[0][0], Value::Bigint(1));
    assert_eq!(rows[0][3], Value::Null);
    assert_eq!(rows[1][3], Value::Bigint(7));
    assert_eq!(rows[2][0], Value::Bigint(3));

    // Incompatible types fall back to VARCHAR with rendered values (α's rule).
    write_file(
        dir.join("d.parquet").to_str().unwrap(),
        &[
            Column::Int32("id", vec![Some(4)]),
            Column::Str("name", vec![Some("d")]),
            Column::Str("score", vec![Some("n/a")]),
        ],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let rows = select(
        &mut db,
        &format!(
            "SELECT score FROM read_parquet('{}/*.parquet', union_by_name := TRUE) ORDER BY score;",
            dir.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Varchar("1.5".to_string()));
    assert_eq!(rows[3][0], Value::Varchar("n/a".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hive_columns_types_and_pruning() {
    let dir = temp_dir("hive");
    for (region, id) in [("us", 1), ("eu", 2)] {
        let sub = dir.join(format!("part/region={region}"));
        std::fs::create_dir_all(&sub).unwrap();
        write_file(
            sub.join("data.parquet").to_str().unwrap(),
            &[Column::Int32("id", vec![Some(id)])],
            0,
            Compression::SNAPPY,
            vec![],
        );
    }
    // A `year=2024` partition: BIGINT by α's round-trip rule, `01` stays text.
    let sub = dir.join("part/region=us/year=01");
    std::fs::create_dir_all(&sub).unwrap();
    write_file(
        sub.join("more.parquet").to_str().unwrap(),
        &[Column::Int32("id", vec![Some(3), Some(4)])],
        0,
        Compression::SNAPPY,
        vec![],
    );
    let mut db = store();
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, region, year FROM read_parquet('{}/part/**/*.parquet') ORDER BY id;",
            dir.display()
        ),
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Varchar("us".to_string()));
    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[1][1], Value::Varchar("eu".to_string()));
    assert_eq!(rows[0][2], Value::Null);
    assert_eq!(rows[2][2], Value::Varchar("01".to_string()));
    assert_eq!(rows[3][2], Value::Varchar("01".to_string()));

    // hive_partitioning := FALSE drops them.
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT) FROM read_parquet('{}/part/**/*.parquet', hive_partitioning := FALSE);",
            dir.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(4));
    let err = db
        .execute(&format!(
            "SELECT region FROM read_parquet('{}/part/**/*.parquet', hive_partitioning := FALSE);",
            dir.display()
        ))
        .unwrap_err();
    assert!(err.to_string().contains("region"), "{err}");

    // A predicate over the hive column prunes whole files before any decode.
    db.execute("SELECT 1;").unwrap();
    let before = stats(&db);
    let rows = select(
        &mut db,
        &format!(
            "SELECT id FROM read_parquet('{}/part/**/*.parquet') WHERE region = 'us' ORDER BY id;",
            dir.display()
        ),
    );
    assert_eq!(rows.len(), 3);
    let after = stats(&db);
    assert_eq!(after.files - before.files, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn metadata_functions_report_alpha_shapes() {
    let mut db = store();
    let people = bench_fixture("people.parquet");

    let rows = select(
        &mut db,
        &format!(
            "SELECT name, type, repetition_type, converted_type, duckdb_type FROM parquet_schema('{people}');"
        ),
    );
    assert_eq!(rows.len(), 7); // root + 6 columns

    let names: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            v => format!("{v:?}"),
        })
        .collect();
    assert_eq!(
        names,
        vec![
            "duckdb_schema",
            "id",
            "name",
            "score",
            "hired",
            "flag",
            "dec"
        ]
    );
    assert_eq!(rows[1][1], Value::Varchar("INT64".to_string()));
    assert_eq!(rows[1][2], Value::Varchar("REQUIRED".to_string()));
    assert_eq!(rows[1][4], Value::Varchar("BIGINT".to_string()));
    assert_eq!(rows[4][3], Value::Varchar("DATE".to_string()));

    let rows = select(
        &mut db,
        &format!(
            "SELECT row_group_id, row_group_num_rows, path_in_schema, type, stats_min, stats_max, stats_null_count, compression, encodings FROM parquet_metadata('{people}');"
        ),
    );
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0][2], Value::Varchar("id".to_string()));
    assert_eq!(rows[0][4], Value::Varchar("0".to_string()));
    assert_eq!(rows[0][5], Value::Varchar("7".to_string()));
    assert_eq!(rows[2][4], Value::Varchar("0.0".to_string()));
    assert_eq!(rows[0][7], Value::Varchar("SNAPPY".to_string()));
    assert_eq!(rows[0][8], Value::Varchar("PLAIN".to_string()));

    let rows = select(
        &mut db,
        &format!(
            "SELECT num_rows, num_row_groups, format_version, file_size_bytes, footer_size FROM parquet_file_metadata('{people}');"
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(8));
    assert_eq!(rows[0][1], Value::Bigint(1));
    assert_eq!(rows[0][2], Value::Bigint(1));

    let kv = bench_fixture("kv.parquet");
    let rows = select(
        &mut db,
        &format!("SELECT key, value FROM parquet_kv_metadata('{kv}') ORDER BY key;"),
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Varchar("owner".to_string()));
    assert_eq!(rows[0][1], Value::Varchar("pinkdb".to_string()));

    // The rg_stats fixture's four row groups show disjoint id bounds.
    let rg = bench_fixture("rg_stats.parquet");
    let rows = select(
        &mut db,
        &format!(
            "SELECT row_group_id, stats_min, stats_max FROM parquet_metadata('{rg}') WHERE path_in_schema = 'id' ORDER BY row_group_id;"
        ),
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][1], Value::Varchar("0".to_string()));
    assert_eq!(rows[0][2], Value::Varchar("2047".to_string()));
    assert_eq!(rows[3][1], Value::Varchar("6144".to_string()));
    assert_eq!(rows[3][2], Value::Varchar("8191".to_string()));
}

#[test]
fn reads_the_committed_duckdb_fixtures() {
    let mut db = store();
    let people = bench_fixture("people.parquet");
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, name, score, hired, flag, dec FROM read_parquet('{people}') ORDER BY id;"
        ),
    );
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[0][0], Value::Bigint(0));
    assert_eq!(rows[3][1], Value::Null);
    assert_eq!(rows[5][2], Value::Null);
    assert_eq!(rows[0][3], Value::Date(19723));
    assert_eq!(rows[0][4], Value::Boolean(true));
    assert_eq!(rows[0][5], Value::Decimal(0, 2));
    assert_eq!(rows[7][5], Value::Decimal(7, 2));

    // gzip decodes through the same path.
    let gzip = bench_fixture("gzip.parquet");
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT), CAST(sum(id) AS BIGINT) FROM read_parquet('{gzip}');"
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(8));
    assert_eq!(rows[0][1], Value::Bigint(28));

    // The four-row-group fixture prunes to one row group for a range filter.
    let rg = bench_fixture("rg_stats.parquet");
    db.execute("SELECT 1;").unwrap();
    let before = stats(&db);
    let rows = select(
        &mut db,
        &format!("SELECT CAST(count(*) AS BIGINT) FROM read_parquet('{rg}') WHERE id >= 7000;"),
    );
    assert_eq!(rows[0][0], Value::Bigint(1192));
    let after = stats(&db);
    assert_eq!(after.row_groups_pruned - before.row_groups_pruned, 3);

    // union_by_name over the two different-schema files.
    let multi = bench_fixture("multi/*.parquet");
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, name, score, extra FROM read_parquet('{multi}', union_by_name := TRUE) ORDER BY id;"
        ),
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][3], Value::Null);
    assert_eq!(rows[1][3], Value::Integer(7));

    // Hive partitions.
    let hive = bench_fixture("hive/**/*.parquet");
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, note, region FROM read_parquet('{hive}') WHERE region = 'eu' ORDER BY id;"
        ),
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][2], Value::Varchar("eu".to_string()));
}

#[test]
fn the_replacement_scan_claims_parquet_paths_only() {
    // The unified read surface has one source per format: the parquet source
    // must leave another format's extensions to that source (the binder's
    // error names the sources when none claims the path).
    let source = pinkdb_parquet::source();
    for path in [
        "x.parquet",
        "dir/y.PARQUET",
        "x.parq",
        "x.pq",
        "part=*/u.parquet",
    ] {
        let call = pinkdb::TableFunctionCall::Path(path.to_string());
        // Claimed: the bind gets as far as the file (which does not exist).
        let err = source
            .bind(&call)
            .expect_err(&format!("the parquet source claims {path}"))
            .to_string();
        assert!(err.contains("No files found"), "{path}: {err}");
    }
    for path in ["x.arrow", "x.ipc", "x.feather", "x.csv", "x", "dir/**"] {
        let call = pinkdb::TableFunctionCall::Path(path.to_string());
        assert_eq!(
            source.bind(&call).ok().flatten(),
            None,
            "the parquet source must not claim {path}"
        );
    }
    // A `read_parquet(...)` call is unambiguous and always claimed.
    let call = pinkdb::TableFunctionCall::Function {
        name: "read_parquet".to_string(),
        args: vec![pinkdb::TableFunctionArg {
            name: None,
            value: Value::Varchar("whatever.dat".to_string()),
        }],
    };
    // (The bind fails on the missing file, which proves it was claimed.)
    let err = source.bind(&call).unwrap_err().to_string();
    assert!(err.contains("No files found"), "{err}");
}

#[test]
fn replacement_scan_reads_a_bare_path() {
    let mut db = store();
    let people = bench_fixture("people.parquet");
    let rows = select(
        &mut db,
        &format!("SELECT CAST(count(*) AS BIGINT) FROM '{people}';"),
    );
    assert_eq!(rows[0][0], Value::Bigint(8));
    // Qualified references use the file stem.
    let rows = select(
        &mut db,
        &format!("SELECT people.id FROM '{people}' LIMIT 1;"),
    );
    assert_eq!(rows[0][0], Value::Bigint(0));
    // A path no source claims is a clean error: register nothing.
    let mut bare = PinkDb::new();
    let err = bare.execute("SELECT * FROM 'people.parquet';").unwrap_err();
    assert!(err.to_string().contains("people.parquet"), "{err}");
}

#[test]
fn unsupported_types_options_and_arguments_are_refused() {
    let dir = temp_dir("errors");
    // A TIME column (INT64 TIME_MICROS) is refused by name.
    let time_file = dir.join("time.parquet");
    let mut schema = SchemaType::group_type_builder("s");
    schema = schema.with_fields(vec![Arc::new(
        SchemaType::primitive_type_builder("t", PhysicalType::INT64)
            .with_converted_type(ConvertedType::TIME_MICROS)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    )]);
    let schema = Arc::new(schema.build().unwrap());
    let file = std::fs::File::create(&time_file).unwrap();
    let mut writer =
        SerializedFileWriter::new(file, schema, Arc::new(WriterProperties::builder().build()))
            .unwrap();
    let mut rg = writer.next_row_group().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&[1i64], None, None)
        .unwrap();
    col.close().unwrap();
    rg.close().unwrap();
    writer.close().unwrap();

    let mut db = store();
    let err = db
        .execute(&format!(
            "SELECT * FROM read_parquet('{}');",
            time_file.display()
        ))
        .unwrap_err();
    assert!(err.to_string().contains("TIME"), "{err}");

    // Options and argument shapes.
    let people = bench_fixture("people.parquet");
    for sql in [
        format!("SELECT * FROM read_parquet('{people}', schema := 'x');"),
        format!("SELECT * FROM read_parquet('{people}', nope := TRUE);"),
        format!("SELECT * FROM read_parquet('{people}', 'other.parquet');"),
        format!("SELECT * FROM read_parquet('{people}' || '');"),
        "SELECT * FROM read_parquet();".to_string(),
    ] {
        assert!(db.execute(&sql).is_err(), "expected an error for {sql}");
    }
    // A missing file is an error, never an empty result.
    let err = db
        .execute("SELECT * FROM read_parquet('/nonexistent/x.parquet');")
        .unwrap_err();
    assert!(err.to_string().contains("No files found"), "{err}");
    // An unknown table function is the engine's own error.
    let err = db
        .execute("SELECT * FROM not_a_function('x');")
        .unwrap_err();
    assert!(err.to_string().contains("not_a_function"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn writer_round_trip_decodes_every_supported_encoding_path() {
    // A one-row-group file written by parquet-rs (PAGE v1, PLAIN, snappy) with
    // a VARCHAR column wide enough to cross a page boundary.
    let dir = temp_dir("roundtrip");
    let path = dir.join("wide.parquet");
    let values: Vec<Option<i32>> = (0..(CHUNK_SIZE as i32 * 3)).map(Some).collect();
    write_file(
        path.to_str().unwrap(),
        &[
            Column::Int32("id", values),
            Column::Str(
                "label",
                (0..CHUNK_SIZE as i32 * 3)
                    .map(|_| Some("repeated-label-for-compression"))
                    .collect(),
            ),
        ],
        0,
        Compression::SNAPPY,
        vec![("pinkdb.test", "round-trip")],
    );
    let mut db = store();
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT), CAST(min(id) AS BIGINT), CAST(max(id) AS BIGINT) FROM read_parquet('{}');",
            path.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(CHUNK_SIZE as i64 * 3));
    assert_eq!(rows[0][1], Value::Bigint(0));
    // Three engine chunks: the reader's chunking crosses row-group boundaries.
    let rows = select(
        &mut db,
        &format!(
            "SELECT CAST(count(*) AS BIGINT) FROM read_parquet('{}') WHERE id >= 2048;",
            path.display()
        ),
    );
    assert_eq!(rows[0][0], Value::Bigint(CHUNK_SIZE as i64 * 2));
    // The footer's key-value metadata round-trips.
    let rows = select(
        &mut db,
        &format!("SELECT key FROM parquet_kv_metadata('{}');", path.display()),
    );
    assert_eq!(rows[0][0], Value::Varchar("pinkdb.test".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_reader_agrees_with_duckdb_on_the_committed_people_fixture() {
    // A direct structural cross-check of the reader against DuckDB's own
    // reading of the committed file, no test-only rewriting in between.
    let people = bench_fixture("people.parquet");
    let mut db = store();
    let rows = select(
        &mut db,
        &format!(
            "SELECT id, name, score, hired, flag, dec FROM read_parquet('{people}') ORDER BY id;"
        ),
    );
    assert_eq!(rows.len(), 8);
    // Row 4's rendered text: id 4, name 'name4', score 6.0, date +5 days.
    assert_eq!(rows[4][0], Value::Bigint(4));
    assert_eq!(rows[4][1], Value::Varchar("name4".to_string()));

    let reader = SerializedFileReader::new(std::fs::File::open(&people).unwrap()).unwrap();
    let file_rows = reader.metadata().file_metadata().num_rows();
    assert_eq!(file_rows, 8);
    let source = pinkdb_parquet::source();
    assert_eq!(source.name(), "parquet");
    let schema = source
        .bind(&pinkdb::TableFunctionCall::Function {
            name: "read_parquet".to_string(),
            args: vec![pinkdb::TableFunctionArg {
                name: None,
                value: Value::Varchar(people.clone()),
            }],
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        schema,
        vec![
            ("id".to_string(), DataType::Bigint),
            ("name".to_string(), DataType::Varchar),
            ("score".to_string(), DataType::Double),
            ("hired".to_string(), DataType::Date),
            ("flag".to_string(), DataType::Boolean),
            (
                "dec".to_string(),
                DataType::Decimal {
                    precision: 9,
                    scale: 2
                }
            ),
        ]
    );
}
