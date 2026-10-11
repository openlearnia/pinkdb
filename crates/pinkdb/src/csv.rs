//! CSV import with schema inference.
//!
//! Inference rule (per column, over all non-empty cells):
//! all i32-parseable → INTEGER, all i64 → BIGINT, all f64 → DOUBLE,
//! all true/false (case-insensitive) → BOOLEAN, else VARCHAR.
//! Empty cell → NULL. An all-empty column → VARCHAR.

use std::path::Path;

use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value, parse_date, parse_timestamp};

/// Infer one column's type from its non-empty cells.
fn infer_column(cells: &[&str]) -> DataType {
    let mut non_empty = 0;
    let mut is_bool = true;
    let mut is_i32 = true;
    let mut is_i64 = true;
    let mut is_f64 = true;
    let mut is_date = true;
    let mut is_ts = true;

    for c in cells {
        let c = c.trim();
        if c.is_empty() {
            continue;
        }
        non_empty += 1;
        let lower = c.to_ascii_lowercase();
        if lower != "true" && lower != "false" {
            is_bool = false;
        }
        if c.parse::<i32>().is_err() {
            is_i32 = false;
        }
        if c.parse::<i64>().is_err() {
            is_i64 = false;
        }
        if c.parse::<f64>().is_err() {
            is_f64 = false;
        }
        if parse_date(c).is_none() {
            is_date = false;
        }
        if parse_timestamp(c).is_none() {
            is_ts = false;
        }
    }

    if non_empty == 0 {
        DataType::Varchar
    } else if is_bool {
        DataType::Boolean
    } else if is_i32 {
        DataType::Integer
    } else if is_i64 {
        DataType::Bigint
    } else if is_f64 {
        DataType::Double
    } else if is_date {
        DataType::Date
    } else if is_ts {
        DataType::Timestamp
    } else {
        DataType::Varchar
    }
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
        DataType::Boolean => match c.to_ascii_lowercase().as_str() {
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
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

/// Read CSV bytes (with header row) into `(headers, records)`.
fn read_records(data: &[u8]) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(csv::Trim::None)
        .from_reader(data);
    let headers: Vec<String> = reader
        .headers()
        .map_err(PinkDbError::Csv)?
        .iter()
        .map(|h| h.to_string())
        .collect();
    if headers.is_empty() {
        return Err(PinkDbError::Catalog("CSV has no header row".to_string()));
    }
    // Note: constructing a csv::Error for an empty-header case without IO
    // backing is awkward; use a Catalog error instead.
    let mut records = Vec::new();
    for row in reader.records() {
        let row = row.map_err(PinkDbError::Csv)?;
        records.push(row.iter().map(|f| f.to_string()).collect());
    }
    Ok((headers, records))
}

/// Load header + records into a fresh table (shared by file/string import).
/// All-or-nothing: a failure drops the freshly created table.
fn load_into(
    db: &mut PinkDb,
    table: &str,
    headers: Vec<String>,
    records: Vec<Vec<String>>,
) -> Result<usize> {
    let schema_types = infer_schema(&headers, &records);
    let schema: Vec<(String, DataType)> =
        headers.into_iter().zip(schema_types.iter().copied()).collect();
    let parsed_types: Vec<DataType> = schema.iter().map(|(_, t)| *t).collect();
    db.create_table(table, schema)?;
    let result = (|| {
        let tbl = db.table_mut(table)?;
        for row in &records {
            if row.len() > parsed_types.len() {
                return Err(PinkDbError::Catalog(format!(
                    "CSV row has {} fields but the header has {}",
                    row.len(),
                    parsed_types.len()
                )));
            }
            let mut values = Vec::with_capacity(parsed_types.len());
            for (i, dtype) in parsed_types.iter().enumerate() {
                let cell = row.get(i).map(String::as_str).unwrap_or("");
                values.push(parse_cell(cell, *dtype)?);
            }
            tbl.append_row(values)?;
        }
        Ok(records.len())
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
    let (headers, records) = read_records(&data)?;
    if headers.is_empty() {
        return Err(PinkDbError::Catalog("CSV has no header row".to_string()));
    }
    load_into(db, table_name, headers, records)
}

/// Import CSV text (header + rows) — used by tests and the REPL.
pub fn import_csv_string(
    db: &mut PinkDb,
    table: &str,
    csv_text: &str,
) -> Result<usize> {
    let (headers, records) = read_records(csv_text.as_bytes())?;
    if headers.is_empty() {
        return Err(PinkDbError::Catalog("CSV has no header row".to_string()));
    }
    load_into(db, table, headers, records)
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
