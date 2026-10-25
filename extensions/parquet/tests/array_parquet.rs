//! Fixed-size `ARRAY` in Parquet (wave 107a): parquet has no fixed-size list
//! type, so DuckDB 2.0 writes an `ARRAY` as the three-level `LIST` group and
//! keeps the length only in the number of element slots per row. pinkdb
//! writes and reads that same shape, DuckDB reads what pinkdb wrote, and
//! pinkdb reads what DuckDB wrote — including a NULL array (whose slot count
//! still carries the length), a NULL element, and a file with no rows at all.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parquet::file::reader::FileReader;
use pinkdb::{
    ArrayElem, DataTable, DataType, PinkDb, QueryResult, TableFunctionArg, TableFunctionCall, Value,
};
use pinkdb_parquet::write::write_table;

/// The DuckDB reference binary (see the standing rules); the interop tests
/// skip when it is absent.
const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_parquet_array_{tag}_{}_{}",
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

fn doubles(items: &[Option<f64>]) -> Value {
    let values: Vec<Value> = items
        .iter()
        .map(|item| item.map(Value::Double).unwrap_or(Value::Null))
        .collect();
    array(ArrayElem::Double, &values)
}

fn array(elem: ArrayElem, items: &[Value]) -> Value {
    Value::Array(Arc::new(
        pinkdb::array::ArrayValue::from_values(elem, items.len() as u32, items).unwrap(),
    ))
}

/// The test rows: plain arrays, a NULL array, a NULL element, and negatives.
fn rows() -> Vec<Vec<Value>> {
    let mut out: Vec<Vec<Value>> = Vec::new();
    for i in 0..40 {
        let base = i as f64;
        out.push(vec![
            Value::Integer(i),
            doubles(&[Some(base), Some(base + 0.5), Some(-base)]),
        ]);
    }
    out.push(vec![Value::Integer(100), Value::Null]);
    out.push(vec![
        Value::Integer(101),
        doubles(&[Some(1.0), None, Some(3.0)]),
    ]);
    out.push(vec![
        Value::Integer(102),
        doubles(&[Some(f64::NAN), Some(f64::INFINITY), Some(-0.0)]),
    ]);
    out
}

fn table(rows: Vec<Vec<Value>>) -> DataTable {
    let mut out = DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), DataType::Integer),
            (
                "v".to_string(),
                DataType::Array {
                    elem: ArrayElem::Double,
                    len: 3,
                },
            ),
        ],
    )
    .unwrap();
    for row in rows {
        out.append_row(row).unwrap();
    }
    out
}

fn floats32(items: &[Option<f32>]) -> Value {
    let values: Vec<Value> = items
        .iter()
        .map(|item| {
            item.map(|x| Value::Double(f64::from(x)))
                .unwrap_or(Value::Null)
        })
        .collect();
    array(ArrayElem::Float, &values)
}

/// The schema and rows of a parquet file through pinkdb's own reader.
fn read_file(path: &Path) -> (Vec<(String, DataType)>, Vec<Vec<Value>>) {
    let source = pinkdb_parquet::source();
    let call = TableFunctionCall::Function {
        name: "read_parquet".to_string(),
        args: vec![TableFunctionArg {
            name: None,
            value: Value::Varchar(path.display().to_string()),
        }],
    };
    let schema = source.bind(&call).unwrap().expect("read_parquet binds");
    let output = source.scan(&call, None, &[]).unwrap();
    let mut rows = Vec::new();
    for chunk in &output.chunks {
        for r in 0..chunk.num_rows {
            rows.push((0..schema.len()).map(|c| chunk.columns[c].get(r)).collect());
        }
    }
    (schema, rows)
}

/// A `FLOAT[3]` column keeps 4-byte elements in the file (DuckDB writes the
/// element leaf as parquet `FLOAT`), and DuckDB reads the values back.
#[test]
fn float_arrays_round_trip_and_duckdb_reads_them() {
    let dir = scratch("float");
    let path = dir.join("float.parquet");
    let mut out = DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), DataType::Integer),
            (
                "v".to_string(),
                DataType::Array {
                    elem: ArrayElem::Float,
                    len: 3,
                },
            ),
        ],
    )
    .unwrap();
    out.append_row(vec![
        Value::Integer(1),
        floats32(&[Some(1.5), Some(-2.25), Some(3.0)]),
    ])
    .unwrap();
    out.append_row(vec![Value::Integer(2), Value::Null])
        .unwrap();
    out.append_row(vec![
        Value::Integer(3),
        floats32(&[Some(0.1), None, Some(f32::INFINITY)]),
    ])
    .unwrap();
    std::fs::write(&path, write_table(&out).unwrap()).unwrap();

    let (schema, rows) = read_file(&path);
    assert_eq!(
        schema[1].1,
        DataType::Array {
            elem: ArrayElem::Float,
            len: 3
        },
        "the 4-byte element kind survives our own file"
    );
    let want = [
        "[1.5, -2.25, 3.0]",
        "NULL",
        "[0.10000000149011612, NULL, inf]",
    ];
    for (row, want) in rows.iter().zip(&want) {
        assert_eq!(row[1].to_string(), *want);
    }

    // A standalone file records no field ids (DuckDB's COPY does not either);
    // only the lake stamps the catalog's column ids.
    if let Some(ids) = duck(&format!(
        "SELECT count(*) FROM parquet_schema('{}') WHERE field_id IS NOT NULL",
        path.display()
    )) {
        assert_eq!(ids, vec![vec!["0".to_string()]], "no field ids standalone");
    }
    // parquet_metadata reports the element leaf's physical type.
    if let Some(meta) = duck(&format!(
        "SELECT type FROM parquet_metadata('{}') WHERE path_in_schema LIKE '%element%'",
        path.display()
    )) {
        assert_eq!(meta, vec![vec!["FLOAT".to_string()]], "element leaf");
    }
    if let Some(got) = duck(&format!(
        "SELECT id, v IS NULL, CAST(v[1] AS DOUBLE), CAST(v[3] AS DOUBLE) \
         FROM read_parquet('{}') ORDER BY id",
        path.display()
    )) {
        assert_eq!(
            got[1],
            vec!["2".to_string(), "true".into(), "NULL".into(), "NULL".into()]
        );
        assert_eq!(got[2][3], "inf", "the f32 infinity survives");
        assert_eq!(
            got[2][2], "0.10000000149011612",
            "f32 bits, not the f64 literal"
        );
        assert_eq!(got[0][3], "3.0");
    }
    cleanup(&dir);
}

/// `Some(rows)` when the DuckDB reference binary exists: each row is the
/// tab-separated cells of the statement's result.
fn duck(query: &str) -> Option<Vec<Vec<String>>> {
    if !Path::new(DUCKDB).exists() {
        return None;
    }
    let mut child = Command::new(DUCKDB)
        .args(["-no-agent", "-noheader", "-list", "-separator", "\t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().unwrap();
        writeln!(stdin, "{query};").unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "duckdb refused {query}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Some(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                line.split('\t')
                    .map(|cell| cell.trim_end().to_string())
                    .collect()
            })
            .collect(),
    )
}

/// A write/read round trip through pinkdb keeps every row exactly, and the
/// file really is the three-level list group (a reader that only understands
/// arithmetic leaves must still see `list`/`element`).
#[test]
fn array_round_trips_exactly() {
    let dir = scratch("roundtrip");
    let path = dir.join("a.parquet");
    let want = rows();
    std::fs::write(&path, write_table(&table(want.clone())).unwrap()).unwrap();

    let (schema, got) = read_file(&path);
    assert_eq!(
        schema[1].1,
        DataType::Array {
            elem: ArrayElem::Double,
            len: 3
        },
        "the ARRAY type survives: {schema:?}"
    );
    assert_eq!(got.len(), want.len(), "row count");
    for (r, (want, got)) in want.iter().zip(&got).enumerate() {
        // Compared by display: a NaN element is never equal to itself, and
        // `[1.0, NaN, -0.0]` still has to round-trip exactly.
        let want: Vec<String> = want.iter().map(|value| value.to_string()).collect();
        let got: Vec<String> = got.iter().map(|value| value.to_string()).collect();
        assert_eq!(want, got, "row {r}");
    }

    let reader =
        parquet::file::reader::SerializedFileReader::new(std::fs::File::open(&path).unwrap())
            .unwrap();
    let text = format!("{:?}", reader.metadata().file_metadata().schema());
    assert!(text.contains("list"), "no list group: {text}");
    assert!(text.contains("element"), "no element leaf: {text}");
    assert!(text.contains("DOUBLE"), "wrong element type: {text}");
    cleanup(&dir);
}

/// A file with no rows still carries the ARRAY's length (parquet's list group
/// cannot express it, so the writer records it in the key-value metadata).
#[test]
fn empty_array_table_keeps_its_length() {
    let dir = scratch("empty");
    let path = dir.join("a.parquet");
    std::fs::write(&path, write_table(&table(Vec::new())).unwrap()).unwrap();
    let (schema, rows) = read_file(&path);
    assert!(rows.is_empty(), "a schema-only file has no rows");
    assert_eq!(
        schema[1].1,
        DataType::Array {
            elem: ArrayElem::Double,
            len: 3
        },
        "schema {schema:?}"
    );
    cleanup(&dir);
}

/// DuckDB 2.0 reads a pinkdb-written ARRAY column, including the NULL array
/// (which keeps its three slots) and the NULL element.
#[test]
fn duckdb_reads_pinkdb_array() {
    let dir = scratch("duckdb_reads");
    let path = dir.join("a.parquet");
    let want = rows();
    std::fs::write(&path, write_table(&table(want.clone())).unwrap()).unwrap();
    let Some(got) = duck(&format!(
        "SELECT id, CAST(v[1] AS DOUBLE), CAST(v[2] AS DOUBLE), CAST(v[3] AS DOUBLE), \
         v IS NULL FROM read_parquet('{}') ORDER BY id",
        path.display()
    )) else {
        cleanup(&dir);
        return;
    };
    assert_eq!(got.len(), want.len(), "row count");
    let number = |cell: &str| -> Option<f64> {
        match cell {
            "NULL" => None,
            "nan" => Some(f64::NAN),
            "inf" => Some(f64::INFINITY),
            "-inf" => Some(f64::NEG_INFINITY),
            other => Some(other.parse().unwrap_or_else(|_| panic!("{other:?}"))),
        }
    };
    for (row, cells) in want.iter().zip(&got) {
        let id = match &row[0] {
            Value::Integer(id) => id.to_string(),
            other => panic!("unexpected id {other:?}"),
        };
        assert_eq!(cells[0], id, "row {row:?} id");
        assert_eq!(cells[4], (row[1] == Value::Null).to_string(), "row {row:?}");
        match &row[1] {
            Value::Null => {
                for cell in &cells[1..4] {
                    assert_eq!(*cell, "NULL", "row {row:?}: a NULL array has no elements");
                }
            }
            Value::Array(items) => {
                for (i, cell) in cells[1..4].iter().enumerate() {
                    match items.elem_value(i) {
                        Value::Null => assert_eq!(*cell, "NULL", "row {row:?} element {i}"),
                        Value::Double(want) => {
                            let got = number(cell).expect("a number");
                            // A FLOAT element is f32; every test value is
                            // exact in both widths, and NaN/inf compare by
                            // class.
                            if want.is_nan() {
                                assert!(got.is_nan(), "row {row:?} element {i}: {cell}");
                            } else {
                                assert_eq!(got, want, "row {row:?} element {i}");
                            }
                        }
                        other => panic!("unexpected {other:?}"),
                    }
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    cleanup(&dir);
}

/// pinkdb reads a DuckDB-written ARRAY column: DuckDB stores it as a list, so
/// the length is inferred from the row group's `num_values`, and FLOAT becomes
/// pinkdb's DOUBLE.
/// One row of the DuckDB-written file: id, the INTEGER[3] elements and the
/// FLOAT[3] elements (empty when the whole array is NULL).
type ExpectedRow = (i32, Vec<Option<i64>>, Vec<Option<f64>>);

#[test]
fn pinkdb_reads_duckdb_array() {
    let dir = scratch("duckdb_writes");
    let path = dir.join("duckdb.parquet");
    let Some(_) = duck(&format!(
        "COPY (SELECT * FROM (VALUES \
         (1, [1, 2, 3]::INTEGER[3], [1.0, 2.0, 3.0]::FLOAT[3]), \
         (2, NULL, NULL), \
         (3, [4, NULL, 6]::INTEGER[3], [4.0, NULL, 6.0]::FLOAT[3]), \
         (4, [-7, 8, 9]::INTEGER[3], [-7.5, 8.5, 9.5]::FLOAT[3]) \
         ) AS t(id, vi, vf)) TO '{}' (FORMAT parquet)",
        path.display()
    )) else {
        cleanup(&dir);
        return;
    };
    let (schema, rows) = read_file(&path);
    assert_eq!(
        schema[1].1,
        DataType::Array {
            elem: ArrayElem::Integer,
            len: 3
        },
        "INTEGER[3] is inferred from the row group: {schema:?}"
    );
    assert_eq!(
        schema[2].1,
        DataType::Array {
            elem: ArrayElem::Float,
            len: 3
        },
        "a FLOAT leaf is a 4-byte element kind: {schema:?}"
    );
    assert_eq!(rows.len(), 4, "row count");
    let expect: Vec<ExpectedRow> = vec![
        (
            1,
            vec![Some(1), Some(2), Some(3)],
            vec![Some(1.0), Some(2.0), Some(3.0)],
        ),
        (2, vec![], vec![]),
        (
            3,
            vec![Some(4), None, Some(6)],
            vec![Some(4.0), None, Some(6.0)],
        ),
        (
            4,
            vec![Some(-7), Some(8), Some(9)],
            vec![Some(-7.5), Some(8.5), Some(9.5)],
        ),
    ];
    for (row, (id, vi, vf)) in rows.iter().zip(&expect) {
        assert_eq!(row[0], Value::Integer(*id));
        let ints: Vec<Option<f64>> = vi.iter().map(|v| v.map(|v| v as f64)).collect();
        for (cell, want) in [(&row[1], &ints), (&row[2], vf)] {
            if want.is_empty() {
                assert_eq!(*cell, Value::Null, "row {id}: a NULL array");
                continue;
            }
            let Value::Array(items) = cell else {
                panic!("row {id}: expected an array, found {cell:?}")
            };
            assert_eq!(items.len(), want.len(), "row {id}: width");
            for (i, want) in want.iter().enumerate() {
                let got = items.elem_value(i);
                match want {
                    None => assert_eq!(got, Value::Null, "row {id} element {i}"),
                    Some(want) => {
                        let got = match got {
                            Value::Integer(x) => x as f64,
                            Value::Double(x) => x,
                            other => panic!("row {id} element {i}: unexpected {other:?}"),
                        };
                        assert_eq!(got, *want, "row {id} element {i}");
                    }
                }
            }
        }
    }

    // The same rows answer the same SQL through the engine.
    let mut db = PinkDb::new();
    pinkdb_parquet::register(&mut db);
    let mut results = db
        .execute(&format!(
            "SELECT id, len(vi), vi[2], vf[1] FROM read_parquet('{}') ORDER BY id",
            path.display()
        ))
        .unwrap();
    let QueryResult::Selected { rows, .. } = results.pop().unwrap() else {
        panic!("expected rows")
    };
    assert_eq!(
        rows[1],
        vec![Value::Integer(2), Value::Null, Value::Null, Value::Null],
        "a NULL array has no length and no elements"
    );
    assert_eq!(rows[2][2], Value::Null, "a NULL element");
    assert_eq!(rows[2][3], Value::Double(4.0));
    cleanup(&dir);
}
