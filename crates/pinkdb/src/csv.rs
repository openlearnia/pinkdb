//! CSV import with schema inference.
//!
//! Inference rule (per column, over all non-empty cells):
//! all i32-parseable → INTEGER, all i64 → BIGINT, all f64 → DOUBLE,
//! all true/false (case-insensitive) → BOOLEAN, else VARCHAR.
//! Empty cell → NULL. An all-empty column → VARCHAR.

use std::path::Path;

use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value, parse_date, parse_timestamp};

/// Streaming inference state: folded per cell, no row storage.
/// Same rule as before (all-non-empty-cells agree), computed in one pass.
#[derive(Clone)]
struct InferState {
    non_empty: usize,
    is_bool: bool,
    is_i32: bool,
    is_i64: bool,
    is_f64: bool,
    is_date: bool,
    is_ts: bool,
}

impl InferState {
    fn new() -> Self {
        Self {
            non_empty: 0,
            is_bool: true,
            is_i32: true,
            is_i64: true,
            is_f64: true,
            is_date: true,
            is_ts: true,
        }
    }

    fn feed(&mut self, cell: &str) {
        let c = cell.trim();
        if c.is_empty() {
            return;
        }
        self.non_empty += 1;
        // Case-insensitive compare without allocating (hot path: every cell).
        if !(c.eq_ignore_ascii_case("true") || c.eq_ignore_ascii_case("false")) {
            self.is_bool = false;
        }
        if c.parse::<i32>().is_err() {
            self.is_i32 = false;
        }
        if c.parse::<i64>().is_err() {
            self.is_i64 = false;
        }
        if c.parse::<f64>().is_err() {
            self.is_f64 = false;
        }
        if parse_date(c).is_none() {
            self.is_date = false;
        }
        if parse_timestamp(c).is_none() {
            self.is_ts = false;
        }
    }

    fn dtype(&self) -> DataType {
        if self.non_empty == 0 {
            DataType::Varchar
        } else if self.is_bool {
            DataType::Boolean
        } else if self.is_i32 {
            DataType::Integer
        } else if self.is_i64 {
            DataType::Bigint
        } else if self.is_f64 {
            DataType::Double
        } else if self.is_date {
            DataType::Date
        } else if self.is_ts {
            DataType::Timestamp
        } else {
            DataType::Varchar
        }
    }
}

/// Infer one column's type from its non-empty cells.
fn infer_column(cells: &[&str]) -> DataType {
    let mut st = InferState::new();
    for c in cells {
        st.feed(c);
    }
    st.dtype()
}

/// Infer a full schema from header + string records.
pub fn infer_schema(headers: &[String], records: &[Vec<String>]) -> Vec<DataType> {
    (0..headers.len())
        .map(|col| {
            let cells: Vec<&str> = records
                .iter()
                .map(|row| row.get(col).map(String::as_str).unwrap_or(""))
                .collect();
            infer_column(&cells)
        })
        .collect()
}

/// Parse one cell into a `Value` of the inferred type. Empty → NULL.
fn parse_cell(cell: &str, dtype: DataType) -> Result<Value> {
    let c = cell.trim();
    if c.is_empty() {
        return Ok(Value::Null);
    }
    match dtype {
        DataType::Boolean => match c {
            c if c.eq_ignore_ascii_case("true") => Ok(Value::Boolean(true)),
            c if c.eq_ignore_ascii_case("false") => Ok(Value::Boolean(false)),
            _ => Err(PinkDbError::Type(format!(
                "cannot parse \"{c}\" as BOOLEAN"
            ))),
        },
        DataType::Integer => c.parse::<i32>().map(Value::Integer).map_err(|_| {
            PinkDbError::Type(format!("cannot parse \"{c}\" as INTEGER"))
        }),
        DataType::Bigint => c.parse::<i64>().map(Value::Bigint).map_err(|_| {
            PinkDbError::Type(format!("cannot parse \"{c}\" as BIGINT"))
        }),
        DataType::Double => c.parse::<f64>().map(Value::Double).map_err(|_| {
            PinkDbError::Type(format!("cannot parse \"{c}\" as DOUBLE"))
        }),
        DataType::Varchar => Ok(Value::Varchar(c.to_string())),
        DataType::Date => parse_date(c)
            .map(Value::Date)
            .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{c}\" as DATE"))),
        DataType::Timestamp => parse_timestamp(c)
            .map(Value::Timestamp)
            .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{c}\" as TIMESTAMP"))),
    }
}

/// Default table name = file stem (e.g. `sales.csv` → `sales`).
fn default_table_name(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("imported")
        .to_string()
}

fn open_reader(data: &[u8]) -> csv::Reader<&[u8]> {
    csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(csv::Trim::None)
        .from_reader(data)
}

fn read_headers(reader: &mut csv::Reader<&[u8]>) -> Result<Vec<String>> {
    let headers: Vec<String> = reader
        .byte_headers()
        .map_err(PinkDbError::Csv)?
        .iter()
        .map(|h| String::from_utf8_lossy(h).into_owned())
        .collect();
    if headers.is_empty() {
        return Err(PinkDbError::Catalog("CSV has no header row".to_string()));
    }
    Ok(headers)
}

/// Borrowed field as &str (invalid UTF-8 errors instead of panicking).
fn field_str(rec: &csv::ByteRecord, i: usize) -> Result<&str> {
    match rec.get(i) {
        Some(b) => std::str::from_utf8(b)
            .map_err(|_| PinkDbError::Execution("CSV contains invalid UTF-8".to_string())),
        None => Ok(""),
    }
}

/// Load CSV bytes into a fresh table (shared by file/string import).
/// Two streaming passes over the input (page-cache resident): inference
/// folds per-cell state with no row storage, then rows parse straight into
/// typed columns. All-or-nothing: a failure drops the created table.
fn load_bytes(db: &mut PinkDb, table: &str, data: &[u8]) -> Result<usize> {
    // Pass 1: headers + inference (record buffer reused across rows).
    let mut reader = open_reader(data);
    let headers = read_headers(&mut reader)?;
    let mut states = vec![InferState::new(); headers.len()];
    let mut rec = csv::ByteRecord::new();
    while reader.read_byte_record(&mut rec).map_err(PinkDbError::Csv)? {
        for (i, st) in states.iter_mut().enumerate() {
            st.feed(field_str(&rec, i)?);
        }
    }
    let schema: Vec<(String, DataType)> = headers
        .into_iter()
        .zip(states.iter().map(|st| st.dtype()))
        .collect();
    let parsed_types: Vec<DataType> = schema.iter().map(|(_, t)| *t).collect();
    db.create_table(table, schema)?;
    // Pass 2: parse + append (record buffer reused).
    let result = (|| {
        let mut reader = open_reader(data);
        let _ = read_headers(&mut reader)?;
        let tbl = db.table_mut(table)?;
        let mut count = 0usize;
        let mut rec = csv::ByteRecord::new();
        while reader.read_byte_record(&mut rec).map_err(PinkDbError::Csv)? {
            if rec.len() > parsed_types.len() {
                return Err(PinkDbError::Catalog(format!(
                    "CSV row has {} fields but the header has {}",
                    rec.len(),
                    parsed_types.len()
                )));
            }
            let mut values = Vec::with_capacity(parsed_types.len());
            for (i, dtype) in parsed_types.iter().enumerate() {
                values.push(parse_cell(field_str(&rec, i)?, *dtype)?);
            }
            tbl.append_row(values)?;
            count += 1;
        }
        Ok(count)
    })();
    if result.is_err() {
        db.drop_table(table).ok();
    }
    result
}

/// Import a CSV file into `table` (default: file stem). Returns rows loaded.
pub fn import_csv(db: &mut PinkDb, path: &str, table: Option<&str>) -> Result<usize> {
    let data = std::fs::read(path)?;
    let owned: String;
    let table_name: &str = match table {
        Some(t) => t,
        None => {
            owned = default_table_name(path);
            &owned
        }
    };
    load_bytes(db, table_name, &data)
}

/// Import CSV text (header + rows) — used by tests and the REPL.
pub fn import_csv_string(
    db: &mut PinkDb,
    table: &str,
    csv_text: &str,
) -> Result<usize> {
    load_bytes(db, table, csv_text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SALES: &str = "id,region,total,active\n\
                         1,west,10.5,true\n\
                         2,east,,FALSE\n\
                         3,west,7,True\n";

    #[test]
    fn inference_picks_expected_types() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "sales", SALES).unwrap();
        let t = db.table("sales").unwrap();
        assert_eq!(
            t.schema(),
            vec![
                ("id".to_string(), DataType::Integer),
                ("region".to_string(), DataType::Varchar),
                ("total".to_string(), DataType::Double),
                ("active".to_string(), DataType::Boolean),
            ]
        );
        assert_eq!(t.num_rows(), 3);
        // Empty cell → NULL.
        assert_eq!(t.get(1, 2), Some(Value::Null));
        assert_eq!(t.get(0, 3), Some(Value::Boolean(true)));
        assert_eq!(t.get(1, 3), Some(Value::Boolean(false)));
    }

    #[test]
    fn bigint_and_all_empty_column() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "b", "big,note\n9999999999,\n-5,\n").unwrap();
        let t = db.table("b").unwrap();
        assert_eq!(
            t.schema(),
            vec![
                ("big".to_string(), DataType::Bigint),
                ("note".to_string(), DataType::Varchar),
            ]
        );
        assert_eq!(t.get(0, 0), Some(Value::Bigint(9_999_999_999)));
        assert_eq!(t.get(0, 1), Some(Value::Null));
    }

    #[test]
    fn import_csv_file_defaults_to_stem() {
        let dir = std::env::temp_dir();
        let path = dir.join("pinkdb_test_sales.csv");
        std::fs::write(&path, SALES).unwrap();
        let mut db = PinkDb::new();
        let n = import_csv(&mut db, path.to_str().unwrap(), None).unwrap();
        assert_eq!(n, 3);
        assert!(db.table("pinkdb_test_sales").is_ok());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn ragged_rows_treat_missing_as_null() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "r", "a,b\n1\n2,3\n").unwrap();
        let t = db.table("r").unwrap();
        assert_eq!(t.get(0, 1), Some(Value::Null));
        assert_eq!(t.get(1, 1), Some(Value::Integer(3)));
    }

    #[test]
    fn ragged_rows_reject_extra_fields() {
        // M2: extra fields error instead of silently truncating…
        let mut db = PinkDb::new();
        assert!(import_csv_string(&mut db, "r", "a,b\n1,2,3\n").is_err());
        // …and leave no partial table behind (m5).
        assert!(db.table("r").is_err());
    }
}
