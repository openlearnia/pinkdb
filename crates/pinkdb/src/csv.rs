//! CSV import with schema inference.
//!
//! Inference rule (per column, over all non-empty cells):
//! all i32-parseable → INTEGER, all i64 → BIGINT, all f64 → DOUBLE,
//! all true/false (case-insensitive) → BOOLEAN, else VARCHAR.
//! Empty cell → NULL. An all-empty column → VARCHAR.
//!
//! Import runs a fast path first: infer from a head sample
//! ([`INFER_SAMPLE_ROWS`] rows, like DuckDB), then one parse pass — on the
//! rayon pool for files with a "clean" byte dialect (no `"` quoting, no
//! `\r`), sequentially through the `csv` crate otherwise. Whenever the sample
//! can't prove the final schema — a later row violates the sampled type, or a
//! column is empty across the sample while rows continue — the import is
//! retried with exact whole-file inference (two passes): same result, just
//! slower. See [`load_bytes`].

use std::path::Path;

use rayon::prelude::*;

use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value, parse_date, parse_timestamp};
use crate::vector::{Vector, CHUNK_SIZE};

/// Rows inspected for head-sample inference (DuckDB's documented default).
const INFER_SAMPLE_ROWS: usize = 20_480;
/// Byte blocks scanned per rayon task when locating newlines.
const SCAN_BLOCK: usize = 1 << 20;
/// Lines per batch in the byte fast path (≈ one output chunk).
const BATCH_LINES: usize = CHUNK_SIZE;

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
        let c = trim_cell(cell);
        if c.is_empty() {
            return;
        }
        self.non_empty += 1;
        if !(self.is_bool || self.is_i32 || self.is_i64 || self.is_f64 || self.is_date || self.is_ts)
        {
            return; // Every flag is dead: the column is VARCHAR already.
        }
        // Case-insensitive compare without allocating (hot path: every cell).
        if self.is_bool && !(c.eq_ignore_ascii_case("true") || c.eq_ignore_ascii_case("false")) {
            self.is_bool = false;
        }
        // An i32 parse implies i64 and f64; an i64 parse implies f64, so the
        // wider parses are skipped while the narrower flag holds.
        if self.is_i32 && c.parse::<i32>().is_ok() {
            // i32 ⇒ i64 ⇒ f64 (flags stay true).
        } else {
            self.is_i32 = false;
            if self.is_i64 && c.parse::<i64>().is_ok() {
                // i64 ⇒ f64 (flag stays true).
            } else {
                self.is_i64 = false;
                if self.is_f64 && c.parse::<f64>().is_err() {
                    self.is_f64 = false;
                }
            }
        }
        if self.is_date && parse_date(c).is_none() {
            self.is_date = false;
        }
        if self.is_ts && parse_timestamp(c).is_none() {
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

/// `str::trim` with a fast path: most CSV cells carry no surrounding
/// whitespace, so the Unicode-aware scan is skipped unless a boundary byte
/// could be whitespace (ASCII whitespace or non-ASCII).
#[inline]
fn trim_cell(cell: &str) -> &str {
    const fn is_ascii_ws(b: u8) -> bool {
        matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
    }
    let clean = match (cell.as_bytes().first(), cell.as_bytes().last()) {
        (Some(&f), Some(&l)) => f.is_ascii() && l.is_ascii() && !is_ascii_ws(f) && !is_ascii_ws(l),
        _ => true, // empty: nothing to trim
    };
    if clean {
        cell
    } else {
        cell.trim()
    }
}

/// Parse one raw CSV field into `col` (empty → NULL).
fn push_field(col: &mut Vector, field: &str, dtype: DataType) -> Result<()> {
    let c = trim_cell(field);
    if c.is_empty() {
        return col.push_null();
    }
    match dtype {
        DataType::Boolean => match c {
            c if c.eq_ignore_ascii_case("true") => col.push_valid(Value::Boolean(true)),
            c if c.eq_ignore_ascii_case("false") => col.push_valid(Value::Boolean(false)),
            _ => Err(PinkDbError::Type(format!(
                "cannot parse \"{c}\" as BOOLEAN"
            ))),
        },
        DataType::Integer => c.parse::<i32>().map(Value::Integer).map_err(|_| {
            PinkDbError::Type(format!("cannot parse \"{c}\" as INTEGER"))
        }).and_then(|v| col.push_valid(v)),
        DataType::Bigint => c.parse::<i64>().map(Value::Bigint).map_err(|_| {
            PinkDbError::Type(format!("cannot parse \"{c}\" as BIGINT"))
        }).and_then(|v| col.push_valid(v)),
        DataType::Double => c.parse::<f64>().map(Value::Double).map_err(|_| {
            PinkDbError::Type(format!("cannot parse \"{c}\" as DOUBLE"))
        }).and_then(|v| col.push_valid(v)),
        DataType::Varchar => col.push_str(c),
        DataType::Date => parse_date(c)
            .map(Value::Date)
            .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{c}\" as DATE")))
            .and_then(|v| col.push_valid(v)),
        DataType::Timestamp => parse_timestamp(c)
            .map(Value::Timestamp)
            .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{c}\" as TIMESTAMP")))
            .and_then(|v| col.push_valid(v)),
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

/// Fresh per-column builders for one output chunk.
fn new_chunk(types: &[DataType]) -> Vec<Vector> {
    types
        .iter()
        .map(|t| Vector::with_capacity(*t, CHUNK_SIZE))
        .collect()
}

/// Parse all CSV rows into `types`-shaped vectors, appending them to `table`
/// in [`CHUNK_SIZE`]-row batches. `table` must already exist.
fn parse_rows(db: &mut PinkDb, table: &str, data: &[u8], types: &[DataType]) -> Result<usize> {
    let mut reader = open_reader(data);
    let _ = read_headers(&mut reader)?;
    let tbl = db.table_mut(table)?;
    let mut chunk = new_chunk(types);
    let mut in_chunk = 0usize;
    let mut count = 0usize;
    let mut rec = csv::ByteRecord::new();
    while reader.read_byte_record(&mut rec).map_err(PinkDbError::Csv)? {
        if rec.len() > types.len() {
            return Err(PinkDbError::Catalog(format!(
                "CSV row has {} fields but the header has {}",
                rec.len(),
                types.len()
            )));
        }
        for (i, dtype) in types.iter().enumerate() {
            push_field(&mut chunk[i], field_str(&rec, i)?, *dtype)?;
        }
        count += 1;
        in_chunk += 1;
        if in_chunk == CHUNK_SIZE {
            tbl.append_chunk(std::mem::replace(&mut chunk, new_chunk(types)))?;
            in_chunk = 0;
        }
    }
    if in_chunk > 0 {
        tbl.append_chunk(chunk)?;
    }
    Ok(count)
}

/// Failure inside one fast-path batch.
#[derive(Debug)]
enum BatchError {
    /// A row has more fields than the header (structural — never retried).
    Fields { count: usize },
    /// A field failed to parse as the sampled type (retries whole-file
    /// inference) or the data is not valid UTF-8 (fatal, like `field_str`).
    Db(PinkDbError),
}

/// Outcome of the clean-file fast path.
#[derive(Debug)]
enum CleanError {
    /// The file has features the byte splitter doesn't model (quoting, `\r`,
    /// or >4 GiB): the caller must use the `csv`-crate pass.
    NotClean,
    Db(PinkDbError),
}

/// Parse one line's fields into `chunk`. Empty lines are skipped (csv crate
/// dialect). Missing fields read as NULL; extra fields error.
fn push_line(
    chunk: &mut [Vector],
    line: &[u8],
    types: &[DataType],
) -> std::result::Result<(), BatchError> {
    if line.is_empty() {
        return Ok(());
    }
    let mut fields = line.split(|&b| b == b',');
    for (col, dtype) in chunk.iter_mut().zip(types) {
        let field = fields.next().unwrap_or(b"");
        let s = std::str::from_utf8(field).map_err(|_| {
            BatchError::Db(PinkDbError::Execution(
                "CSV contains invalid UTF-8".to_string(),
            ))
        })?;
        push_field(col, s, *dtype).map_err(BatchError::Db)?;
    }
    if fields.next().is_some() {
        return Err(BatchError::Fields {
            count: line.iter().filter(|&&b| b == b',').count() + 1,
        });
    }
    Ok(())
}

/// Parse `data[start..end]` (whole lines) into one chunk of vectors.
fn parse_batch(
    data: &[u8],
    start: usize,
    end: usize,
    types: &[DataType],
) -> std::result::Result<Vec<Vector>, BatchError> {
    let mut chunk = new_chunk(types);
    let mut line_start = start;
    for i in start..end {
        if data[i] == b'\n' {
            push_line(&mut chunk, &data[line_start..i], types)?;
            line_start = i + 1;
        }
    }
    if line_start < end {
        // Final line without a trailing newline (last batch of the file).
        push_line(&mut chunk, &data[line_start..end], types)?;
    }
    Ok(chunk)
}

/// Rayon-parallel parse of a "clean" file: split the data region into
/// ~[`BATCH_LINES`]-line batches (all on newline boundaries), parse each batch
/// on the pool, then move the chunks into `table` in file order. Clean means
/// no `"` and no `\r` anywhere — then line splitting provably matches the
/// `csv` crate's dialect (see `push_line`), and the file is parsed exactly
/// once. Returns [`CleanError::NotClean`] before touching the table when the
/// dialect isn't provable, so the caller can fall back to the `csv` crate.
fn parse_clean(
    db: &mut PinkDb,
    table: &str,
    data: &[u8],
    types: &[DataType],
) -> std::result::Result<usize, CleanError> {
    if data.len() > u32::MAX as usize {
        return Err(CleanError::NotClean);
    }
    let header_end = match data.iter().position(|&b| b == b'\n') {
        Some(i) => i + 1,
        None => return Ok(0), // header only, no rows
    };
    // 1. Locate every newline; bail on quoting/CR (in parallel, ~1 MiB tasks).
    let scans: Vec<Option<Vec<u32>>> = data
        .par_chunks(SCAN_BLOCK)
        .enumerate()
        .map(|(blk, bytes)| {
            let base = (blk * SCAN_BLOCK) as u32;
            let mut out = Vec::new();
            for (i, &b) in bytes.iter().enumerate() {
                match b {
                    b'"' | b'\r' => return None,
                    b'\n' => out.push(base + i as u32),
                    _ => {}
                }
            }
            Some(out)
        })
        .collect();
    let mut newlines: Vec<u32> = Vec::with_capacity(data.len() / 24 + 16);
    for scan in scans {
        match scan {
            Some(nl) => newlines.extend(nl),
            None => return Err(CleanError::NotClean),
        }
    }
    // 2. Cut the data region into line batches (line counts, not row counts:
    //    empty lines are skipped later, chunk sizes may vary a little).
    let mut batches: Vec<(usize, usize)> = Vec::new();
    let mut start = header_end;
    let mut j = BATCH_LINES; // newlines[0] ends the header
    while j < newlines.len() {
        let end = newlines[j] as usize + 1;
        if end > start {
            batches.push((start, end));
            start = end;
        }
        j += BATCH_LINES;
    }
    if start < data.len() {
        batches.push((start, data.len()));
    }
    // 3. Parse batches in parallel (order-preserving collect).
    let parsed: Vec<std::result::Result<Vec<Vector>, BatchError>> = batches
        .par_iter()
        .map(|&(s, e)| parse_batch(data, s, e, types))
        .collect();
    // 4. Append chunks in file order; the first error (in file order) wins,
    //    matching the sequential pass. On error the caller drops the table.
    let tbl = db.table_mut(table).map_err(CleanError::Db)?;
    let mut count = 0usize;
    for chunk in parsed {
        match chunk {
            Ok(chunk) => {
                count += chunk.first().map(|c| c.len()).unwrap_or(0);
                tbl.append_chunk(chunk).map_err(CleanError::Db)?;
            }
            Err(BatchError::Fields { count: fields }) => {
                return Err(CleanError::Db(PinkDbError::Catalog(format!(
                    "CSV row has {fields} fields but the header has {}",
                    types.len()
                ))));
            }
            Err(BatchError::Db(e)) => return Err(CleanError::Db(e)),
        }
    }
    Ok(count)
}

/// Head-sample inference outcome.
enum Sampled {
    /// Import finished; payload is the row count.
    Done(usize),
    /// The sample can't prove the final schema: rerun with whole-file inference.
    NeedsFull,
}

/// Fast path: infer from the first [`INFER_SAMPLE_ROWS`] rows, then parse the
/// file once. The sampled schema equals the whole-file schema unless a later
/// row breaks one of its flags (a `Type` error from the parse pass — retried)
/// or a column is empty across the sample while rows continue (`NeedsFull`
/// then also goes through whole-file inference). Drops the table on failure
/// so the retry starts clean.
fn load_sampled(db: &mut PinkDb, table: &str, data: &[u8]) -> Result<Sampled> {
    let mut reader = open_reader(data);
    let headers = read_headers(&mut reader)?;
    let mut states = vec![InferState::new(); headers.len()];
    let mut rec = csv::ByteRecord::new();
    let mut sampled = 0usize;
    while sampled < INFER_SAMPLE_ROWS && reader.read_byte_record(&mut rec).map_err(PinkDbError::Csv)?
    {
        for (i, st) in states.iter_mut().enumerate() {
            st.feed(field_str(&rec, i)?);
        }
        sampled += 1;
    }
    // Does the file continue past the sample?
    let more_rows = if sampled == INFER_SAMPLE_ROWS {
        reader.read_byte_record(&mut rec).map_err(PinkDbError::Csv)?
    } else {
        false
    };
    // An all-empty sample makes the dtype VARCHAR, but later non-empty cells
    // would infer differently: defer to whole-file inference.
    if more_rows && states.iter().any(|st| st.non_empty == 0) {
        return Ok(Sampled::NeedsFull);
    }
    let schema: Vec<(String, DataType)> = headers
        .into_iter()
        .zip(states.iter().map(|st| st.dtype()))
        .collect();
    let types: Vec<DataType> = schema.iter().map(|(_, t)| *t).collect();
    db.create_table(table, schema)?;
    let result = match parse_clean(db, table, data, &types) {
        Ok(n) => Ok(n),
        Err(CleanError::NotClean) => parse_rows(db, table, data, &types),
        Err(CleanError::Db(e)) => Err(e),
    };
    match result {
        Ok(n) => Ok(Sampled::Done(n)),
        Err(e) => {
            db.drop_table(table).ok();
            Err(e)
        }
    }
}

/// Exact two-pass path: infer over every cell, then parse. Same result as the
/// fast path; used when the sample can't be trusted.
fn load_full(db: &mut PinkDb, table: &str, data: &[u8]) -> Result<usize> {
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
    let types: Vec<DataType> = schema.iter().map(|(_, t)| *t).collect();
    db.create_table(table, schema)?;
    // Pass 2: parse + append (record buffer reused).
    match parse_rows(db, table, data, &types) {
        Ok(n) => Ok(n),
        Err(e) => {
            db.drop_table(table).ok();
            Err(e)
        }
    }
}

/// Load CSV bytes into a fresh table (shared by file/string import).
/// All-or-nothing: a failure drops the created table.
fn load_bytes(db: &mut PinkDb, table: &str, data: &[u8]) -> Result<usize> {
    match load_sampled(db, table, data) {
        Ok(Sampled::Done(n)) => Ok(n),
        Ok(Sampled::NeedsFull) => load_full(db, table, data),
        // A row beyond the sample violated the sampled type (the table is
        // already dropped): rerun with exact whole-file inference.
        Err(PinkDbError::Type(_)) => load_full(db, table, data),
        Err(e) => Err(e),
    }
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

    #[test]
    fn quoted_fields_survive_chunking() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "q", "a,b\n\"x,y\",2\n\"say \"\"hi\"\"\",3\n").unwrap();
        let t = db.table("q").unwrap();
        assert_eq!(t.get(0, 0), Some(Value::Varchar("x,y".to_string())));
        assert_eq!(t.get(1, 0), Some(Value::Varchar("say \"hi\"".to_string())));
    }

    #[test]
    fn crlf_header_only_and_unicode() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "c", "a,b\r\n1,2\r\n3,4\r\n").unwrap();
        assert_eq!(db.table("c").unwrap().num_rows(), 2);
        import_csv_string(&mut db, "h", "a,b\n").unwrap();
        assert_eq!(db.table("h").unwrap().num_rows(), 0);
        import_csv_string(&mut db, "u", "a\n Héllo \n").unwrap();
        assert_eq!(
            db.table("u").unwrap().get(0, 0),
            Some(Value::Varchar("Héllo".to_string()))
        );
    }

    #[test]
    fn invalid_utf8_errors_cleanly() {
        let mut db = PinkDb::new();
        let bytes = b"a,b\n\xff\xfe,2\n";
        // load_bytes is private; exercise through a temp file.
        let path = std::env::temp_dir().join("pinkdb_badutf.csv");
        std::fs::write(&path, bytes).unwrap();
        assert!(import_csv(&mut db, path.to_str().unwrap(), None).is_err());
        assert!(db.list_tables().is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn inference_flag_cascade_matches_full_rule() {
        let cases: &[(&[&str], DataType)] = &[
            (&["1", "2", "-3"], DataType::Integer),
            (&["1", "9999999999"], DataType::Bigint),
            (&["1", "2.5"], DataType::Double),
            (&["1.5", "2"], DataType::Double),
            (&["true", "FALSE"], DataType::Boolean),
            (&["1", "true"], DataType::Varchar),
            (&["1", "abc"], DataType::Varchar),
            (&["1", "2024-01-01"], DataType::Varchar),
            (&["2024-01-01", "2024-02-29"], DataType::Date),
            (&["2024-01-01", "2024-01-02 10:00:00"], DataType::Varchar),
            (&["2024-01-01 10:00:00", "2024-01-02T00:00:00.5"], DataType::Timestamp),
            (&["", ""], DataType::Varchar),
            (&["1e5", "2"], DataType::Double),
        ];
        for (cells, want) in cases {
            assert_eq!(infer_column(cells), *want, "cells: {cells:?}");
        }
    }

    #[test]
    fn tail_violation_falls_back_to_whole_file_inference() {
        // More rows than the sample window, then a row that breaks the type.
        let mut text = String::from("id,note\n");
        for i in 1..=INFER_SAMPLE_ROWS + 2 {
            text.push_str(&format!("{i},x\n"));
        }
        text.push_str("oops,late\n");
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "t", &text).unwrap();
        let t = db.table("t").unwrap();
        assert_eq!(t.num_rows(), INFER_SAMPLE_ROWS + 3);
        // Whole-file inference: the tail cell makes the column VARCHAR.
        assert_eq!(t.schema()[0], ("id".to_string(), DataType::Varchar));
        assert_eq!(t.get(INFER_SAMPLE_ROWS + 2, 0), Some(Value::Varchar("oops".to_string())));
    }

    #[test]
    fn empty_in_sample_column_falls_back() {
        // `b` is empty across the whole sample but has data afterwards, so
        // the head sample alone would wrongly call it VARCHAR.
        let mut text = String::from("a,b\n");
        for i in 1..=INFER_SAMPLE_ROWS + 5 {
            text.push_str(&format!("{i},\n"));
        }
        text.push_str("x,5\n");
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "t", &text).unwrap();
        let t = db.table("t").unwrap();
        assert_eq!(t.schema()[1], ("b".to_string(), DataType::Integer));
        assert_eq!(t.get(INFER_SAMPLE_ROWS + 5, 1), Some(Value::Integer(5)));
        assert_eq!(t.get(INFER_SAMPLE_ROWS + 5, 0), Some(Value::Varchar("x".to_string())));
    }

    #[test]
    fn fallback_drops_table_on_structural_error() {
        // Too many fields beyond the sample: no retry, no partial table.
        let mut text = String::from("a,b\n");
        for i in 1..=INFER_SAMPLE_ROWS + 2 {
            text.push_str(&format!("{i},2\n"));
        }
        text.push_str("1,2,3\n");
        let mut db = PinkDb::new();
        let err = import_csv_string(&mut db, "t", &text).unwrap_err();
        assert!(matches!(err, PinkDbError::Catalog(_)));
        assert!(db.table("t").is_err());
    }

    /// Error class (variant) of a `PinkDbError`, for outcome comparison.
    fn err_kind(e: &PinkDbError) -> &'static str {
        match e {
            PinkDbError::Catalog(_) => "catalog",
            PinkDbError::Type(_) => "type",
            PinkDbError::Parse(_) => "parse",
            PinkDbError::Execution(_) => "execution",
            PinkDbError::Io(_) => "io",
            PinkDbError::Csv(_) => "csv",
        }
    }

    /// The fast path (head sample + parallel byte parse with all fallbacks)
    /// must be indistinguishable from the exact two-pass `csv`-crate path.
    #[test]
    fn fast_path_matches_exact_two_pass() {
        let mut corpus: Vec<String> = vec![
            SALES.to_string(),
            "a,b\n1,2\n\n3,4\n".to_string(),   // empty line
            "a,b\n1,2\n   \n3,4\n".to_string(), // whitespace-only line
            "a,b\n\n".to_string(),
            "a,b\n".to_string(),
            "a,b\n \n".to_string(),
            "a,b\n1,2".to_string(),            // no trailing newline
            "a,b\n1,2\n3".to_string(),         // ragged last row
            "a,b\n1,2\n,\n".to_string(),
            "a,b\n1,2\n\n\n".to_string(),
            "a\n1\n\n".to_string(),
            "a,b\r\n1,2\r\n".to_string(),      // CRLF → csv path
            "a,b\n\"x\",\"y\"\n".to_string(),  // quoted → csv path
            "a,b\n\"x\ny\",2\n".to_string(),   // embedded newline → csv path
            "a,b\n1\n,2\n".to_string(),
            "a,b\n1,2,3\n".to_string(),        // structural error
            "a,b\n,5\nx,\n".to_string(),
            "d,note\n2024-02-29,ok\n2024-03-01,\n".to_string(),
            "t,big\n2024-02-29 10:00:00.25,9999999999\n2024-03-01 00:00:00,-5\n".to_string(),
        ];
        // Several 2048-line batches, empty lines mixed in, values that hit
        // batch boundaries.
        let mut big = String::from("id,region,total,active\n");
        for i in 1..=5000i64 {
            if i % 97 == 0 {
                big.push('\n');
            }
            let total = if i % 11 == 0 {
                String::new()
            } else {
                format!("{i}.5")
            };
            big.push_str(&format!(
                "{i},r{},{total},{}\n",
                i % 3,
                if i % 2 == 0 { "true" } else { "false" }
            ));
        }
        corpus.push(big);

        for text in &corpus {
            let mut exact = PinkDb::new();
            let exact_res = load_full(&mut exact, "t", text.as_bytes());
            let mut fast = PinkDb::new();
            let fast_res = load_bytes(&mut fast, "t", text.as_bytes());
            match (exact_res, fast_res) {
                (Ok(n0), Ok(n1)) => {
                    assert_eq!(n0, n1, "row count differs for {text:?}");
                    let t0 = exact.table("t").unwrap();
                    let t1 = fast.table("t").unwrap();
                    assert_eq!(t0.schema(), t1.schema(), "schema differs for {text:?}");
                    for r in 0..n0 {
                        for c in 0..t0.num_cols() {
                            assert_eq!(
                                t0.get(r, c),
                                t1.get(r, c),
                                "cell ({r},{c}) differs for {text:?}"
                            );
                        }
                    }
                }
                (Err(e0), Err(e1)) => {
                    assert_eq!(
                        err_kind(&e0),
                        err_kind(&e1),
                        "error kind differs for {text:?}: {e0:?} vs {e1:?}"
                    );
                    assert!(exact.table("t").is_err(), "exact left a table for {text:?}");
                    assert!(fast.table("t").is_err(), "fast left a table for {text:?}");
                }
                (a, b) => panic!("divergent outcomes for {text:?}: {a:?} vs {b:?}"),
            }
        }
    }

    #[test]
    fn clean_parse_guards_and_batching() {
        let types = vec![DataType::Integer, DataType::Varchar];
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        // Quoting and CR are not modeled: NotClean, table untouched.
        assert!(matches!(
            parse_clean(&mut db, "t", b"a,b\n1,\"x\"\n", &types),
            Err(CleanError::NotClean)
        ));
        assert!(matches!(
            parse_clean(&mut db, "t", b"a,b\n1,x\r\n", &types),
            Err(CleanError::NotClean)
        ));
        // Clean input parses (empty lines skipped, missing fields NULL).
        assert_eq!(
            parse_clean(&mut db, "t", b"a,b\n1,x\n\n2\n", &types).unwrap(),
            2
        );
        // More than one batch (BATCH_LINES + 3 data lines, one empty).
        let mut text = b"a,b\n".to_vec();
        for i in 0..BATCH_LINES + 3 {
            text.extend_from_slice(format!("{i},v{i}\n").as_bytes());
        }
        let mut db = PinkDb::new();
        db.create_table(
            "t2",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        let n = parse_clean(&mut db, "t2", &text, &types).unwrap();
        assert_eq!(n, BATCH_LINES + 3);
        let t = db.table("t2").unwrap();
        for &i in &[0usize, BATCH_LINES - 1, BATCH_LINES, BATCH_LINES + 2] {
            assert_eq!(t.get(i, 0), Some(Value::Integer(i as i32)));
            assert_eq!(t.get(i, 1), Some(Value::Varchar(format!("v{i}"))));
        }
    }
}
