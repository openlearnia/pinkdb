//! The parquet `Variant` logical type (wave 101b): pinkdb writes shredded
//! VARIANT columns and reads them back exactly, DuckDB 2.0 reads what pinkdb
//! wrote, and pinkdb reads what DuckDB wrote.
//!
//! DuckDB shreds `VARIANT` by default too, so the two directions exercise
//! different schemas: pinkdb's policy (an object's common keys, typed) and
//! DuckDB's (nested objects and arrays as well).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parquet::file::reader::FileReader;
use pinkdb::{
    DataTable, DataType, PinkDb, QueryResult, TableFunctionArg, TableFunctionCall, Value,
};
use pinkdb_parquet::write::write_table;

/// The DuckDB reference binary (see the standing rules); the interop tests
/// skip when it is absent.
const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_parquet_variant_{tag}_{}_{}",
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

fn variant(json: &str) -> Value {
    Value::Variant(Arc::new(
        pinkdb::variant::from_json_str(json).expect("valid test JSON"),
    ))
}

/// The JSON texts of the test rows: objects the policy types (`a`, `tag`) and
/// shapes it keeps whole — a nested object, an array, scalars, explicit null.
fn json_rows() -> Vec<String> {
    let mut rows: Vec<String> = (0..64)
        .map(|i| format!("{{\"a\": {i}, \"tag\": \"t{i}\"}}"))
        .collect();
    rows.extend(
        [
            "{\"a\": 100, \"tag\": \"t\", \"deep\": {\"x\": [1, 2, 3]}}",
            "[1, \"two\", {\"three\": 3}]",
            "\"a scalar\"",
            "12",
            "true",
            "null",
        ]
        .iter()
        .map(|s| s.to_string()),
    );
    rows
}

/// `(id, variant)` rows, the last one a SQL NULL.
fn rows() -> Vec<Vec<Value>> {
    let mut rows: Vec<Vec<Value>> = json_rows()
        .iter()
        .enumerate()
        .map(|(at, json)| vec![Value::Integer(at as i32), variant(json)])
        .collect();
    let at = rows.len() as i32;
    rows.push(vec![Value::Integer(at), Value::Null]);
    rows
}

fn table() -> DataTable {
    let mut out = DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), DataType::Integer),
            ("v".to_string(), DataType::Variant),
        ],
    )
    .unwrap();
    for row in rows() {
        out.append_row(row).unwrap();
    }
    out
}

/// The rows of a parquet file through pinkdb's own reader.
fn read_rows(path: &Path) -> Vec<Vec<Value>> {
    let source = pinkdb_parquet::source();
    let call = TableFunctionCall::Function {
        name: "read_parquet".to_string(),
        args: vec![TableFunctionArg {
            name: None,
            value: Value::Varchar(path.display().to_string()),
            literal: None,
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
    rows
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

/// The CLI's text for one extracted variant value.
fn plain(value: &pinkdb::variant::Variant) -> String {
    use pinkdb::variant::Variant;
    match value {
        Variant::Null => "NULL".to_string(),
        Variant::Bool(b) => b.to_string(),
        Variant::Int(i) => i.to_string(),
        Variant::Double(d) => format!("{d}"),
        Variant::Str(s) => s.to_string(),
        other => other.to_json(),
    }
}

/// Every value survives a pinkdb write/read exactly, and the shredder really
/// shredded (the file's schema carries the typed leaves).
#[test]
fn variant_round_trips_exactly() {
    let dir = scratch("roundtrip");
    let path = dir.join("v.parquet");
    std::fs::write(&path, write_table(&table()).unwrap()).unwrap();
    let got = read_rows(&path);
    let want = rows();
    assert_eq!(got.len(), want.len(), "row count");
    for (r, (want, got)) in want.iter().zip(&got).enumerate() {
        assert_eq!(want, got, "row {r}");
    }
    let reader =
        parquet::file::reader::SerializedFileReader::new(std::fs::File::open(&path).unwrap())
            .unwrap();
    let text = format!("{:?}", reader.metadata().file_metadata().schema());
    assert!(text.contains("typed_value"), "no shredded column: {text}");
    assert!(text.contains("Variant"), "no variant annotation: {text}");
    cleanup(&dir);
}

/// DuckDB 2.0 reads a pinkdb-written VARIANT column and agrees on every row's
/// NULL-ness and on the shredded paths' values.
#[test]
fn duckdb_reads_pinkdb_variant() {
    let dir = scratch("duckdb_reads");
    let path = dir.join("v.parquet");
    std::fs::write(&path, write_table(&table()).unwrap()).unwrap();
    let Some(got) = duck(&format!(
        "SELECT id, v IS NULL, coalesce(variant_extract(v, 'a')::VARCHAR, '<none>'), \
         coalesce(variant_extract(v, 'tag')::VARCHAR, '<none>') \
         FROM read_parquet('{}') ORDER BY id",
        path.display()
    )) else {
        cleanup(&dir);
        return;
    };
    let want = rows();
    assert_eq!(got.len(), want.len(), "row count");
    for (row, cells) in want.iter().zip(&got) {
        let id = match &row[0] {
            Value::Integer(id) => id.to_string(),
            other => panic!("unexpected id {other:?}"),
        };
        assert_eq!(cells[0], id, "row {row:?} id");
        let (null, fields) = match &row[1] {
            // DuckDB collapses a VARIANT holding null to SQL NULL (pinkdb
            // keeps the two apart, wave 101a), so its `IS NULL` is true here.
            Value::Variant(value) if **value == pinkdb::variant::Variant::Null => {
                ("true", ["<none>".to_string(), "<none>".to_string()])
            }
            Value::Variant(value) => (
                "false",
                ["a", "tag"].map(|name| {
                    value
                        .object_get(name)
                        .map(plain)
                        .unwrap_or_else(|| "<none>".to_string())
                }),
            ),
            Value::Null => ("true", ["<none>".to_string(), "<none>".to_string()]),
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(cells[1], null, "row {row:?} IS NULL");
        assert_eq!(cells[2], fields[0], "row {row:?} v['a']");
        assert_eq!(cells[3], fields[1], "row {row:?} v['tag']");
    }
    cleanup(&dir);
}

/// pinkdb reads a DuckDB-written VARIANT column, including DuckDB's own
/// shredding of nested objects and arrays.
#[test]
fn pinkdb_reads_duckdb_variant() {
    let dir = scratch("duckdb_writes");
    let path = dir.join("duckdb.parquet");
    let source = json_rows();
    // A VALUES list, not a CSV: CSV parsing would strip the quotes of a JSON
    // string value.
    let values: Vec<String> = source
        .iter()
        .enumerate()
        .map(|(at, json)| format!("({at}, '{}')", json.replace('\'', "''")))
        .collect();
    let Some(_) = duck(&format!(
        "COPY (SELECT i, json(j)::VARIANT AS v FROM (VALUES {}) AS t(i, j)) \
         TO '{}' (FORMAT parquet)",
        values.join(", "),
        path.display()
    )) else {
        cleanup(&dir);
        return;
    };
    let got = read_rows(&path);
    assert_eq!(got.len(), source.len(), "row count");
    for (row, json) in source.iter().enumerate() {
        // DuckDB writes a VARIANT holding null as a SQL NULL row, so that is
        // what any reader sees; every other value must survive exactly.
        let want = match variant(json) {
            Value::Variant(value) if *value == pinkdb::variant::Variant::Null => Value::Null,
            other => other,
        };
        assert_eq!(got[row][1], want, "row {row}: {json}");
    }
    cleanup(&dir);
}

/// `COPY … TO '….parquet'` and `read_parquet` agree with the table they came
/// from, which is the SQL spelling a user actually types.
#[test]
fn copy_and_read_parquet_agree() {
    let dir = scratch("copy");
    let path = dir.join("copy.parquet");
    let mut db = PinkDb::new();
    pinkdb_parquet::register(&mut db);
    db.execute("CREATE TABLE t (id INTEGER, v VARIANT);")
        .unwrap();
    for row in rows() {
        db.table_mut("t").unwrap().append_row(row).unwrap();
    }
    db.execute(&format!("COPY t TO '{}' (FORMAT parquet);", path.display()))
        .unwrap();
    let mut results = db
        .execute(&format!(
            "SELECT id, v FROM read_parquet('{}') ORDER BY id;",
            path.display()
        ))
        .unwrap();
    let got = match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    };
    let want = rows();
    assert_eq!(got.len(), want.len(), "row count");
    for (want, got) in want.iter().zip(&got) {
        assert_eq!(want, got);
    }
    cleanup(&dir);
}
