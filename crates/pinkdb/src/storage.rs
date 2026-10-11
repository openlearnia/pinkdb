//! In-memory columnar storage + catalog.
//!
//! `DataTable` = one [`Vector`] chunk list per column (each ≤ [`CHUNK_SIZE`]
//! rows). [`PinkDb`] = map of table name → table. Single connection, single
//! thread. No persistence in v1.

use std::collections::HashMap;

use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, CHUNK_SIZE};

/// One column: its schema + its chunk list.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
    pub chunks: Vec<Vector>,
}

impl Column {
    fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            chunks: vec![Vector::new_empty(dtype)],
        }
    }
}

/// One table: a set of same-length columns.
#[derive(Debug, Clone)]
pub struct DataTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub num_rows: usize,
}

impl DataTable {
    pub fn new(name: String, schema: Vec<(String, DataType)>) -> Result<Self> {
        if schema.is_empty() {
            return Err(PinkDbError::Catalog(
                "cannot create table with no columns".to_string(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for (col, _) in &schema {
            let key = col.to_ascii_lowercase();
            if !seen.insert(key) {
                return Err(PinkDbError::Catalog(format!(
                    "duplicate column name \"{col}\""
                )));
            }
        }
        Ok(Self {
            name,
            columns: schema
                .into_iter()
                .map(|(name, dtype)| Column::new(name, dtype))
                .collect(),
            num_rows: 0,
        })
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    pub fn schema(&self) -> Vec<(String, DataType)> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.dtype))
            .collect()
    }

    /// Case-insensitive column lookup (SQL identifiers are case-insensitive).
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                PinkDbError::Catalog(format!(
                    "table \"{}\" has no column \"{}\"",
                    self.name, name
                ))
            })
    }

    /// Widening coercion for INSERT/CSV: INTEGER → BIGINT → DOUBLE.
    /// NULL passes through for any target.
    pub(crate) fn coerce(value: Value, target: DataType) -> Result<Value> {
        match (value, target) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Boolean(b), DataType::Boolean) => Ok(Value::Boolean(b)),
            (Value::Integer(i), DataType::Integer) => Ok(Value::Integer(i)),
            (Value::Integer(i), DataType::Bigint) => Ok(Value::Bigint(i as i64)),
            (Value::Integer(i), DataType::Double) => Ok(Value::Double(i as f64)),
            (Value::Bigint(i), DataType::Bigint) => Ok(Value::Bigint(i)),
            (Value::Bigint(i), DataType::Double) => Ok(Value::Double(i as f64)),
            (Value::Double(x), DataType::Double) => Ok(Value::Double(x)),
            (Value::Varchar(s), DataType::Varchar) => Ok(Value::Varchar(s)),
            (Value::Date(d), DataType::Date) => Ok(Value::Date(d)),
            (Value::Timestamp(us), DataType::Timestamp) => Ok(Value::Timestamp(us)),
            (v, t) => Err(PinkDbError::Type(format!(
                "cannot coerce {v} to {t}"
            ))),
        }
    }

    /// Append one row, opening a fresh chunk in every column when the tail
    /// chunk is full.
    pub fn append_row(&mut self, values: Vec<Value>) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(PinkDbError::Execution(format!(
                "row has {} values but table \"{}\" has {} columns",
                values.len(),
                self.name,
                self.columns.len()
            )));
        }
        // Open a new tail chunk everywhere if the tail is full, so columns
        // stay aligned even at chunk boundaries.
        let tail_full = self.columns[0]
            .chunks
            .last()
            .map(|c| c.len() >= CHUNK_SIZE)
            .unwrap_or(true);
        if tail_full {
            for col in &mut self.columns {
                col.chunks.push(Vector::new_empty(col.dtype));
            }
        }
        for (col, val) in self.columns.iter_mut().zip(values) {
            let coerced = Self::coerce(val, col.dtype)?;
            col.chunks
                .last_mut()
                .expect("tail chunk exists")
                .push_value(coerced)?;
        }
        self.num_rows += 1;
        Ok(())
    }

    /// Append a whole `DataChunk` (used by tests / future INSERT paths).
    /// Number of stored chunks (all columns share the same count).
    pub fn num_chunks(&self) -> usize {
        self.columns.first().map(|c| c.chunks.len()).unwrap_or(0)
    }

    /// Read chunk `idx` as a `DataChunk` (column chunks cloned).
    pub fn scan_chunk(&self, idx: usize) -> Option<DataChunk> {
        if idx >= self.num_chunks() {
            return None;
        }
        let cols = self.columns.iter().map(|c| c.chunks[idx].clone()).collect();
        DataChunk::new(cols).ok()
    }

    /// Value at (row, col).
    pub fn get(&self, row: usize, col: usize) -> Option<Value> {
        if row >= self.num_rows || col >= self.columns.len() {
            return None;
        }
        let mut base = row;
        for chunk in &self.columns[col].chunks {
            if base < chunk.len() {
                return Some(chunk.get(base));
            }
            base -= chunk.len();
        }
        None
    }
}

/// The catalog + database. Owns all tables.
#[derive(Debug, Default)]
pub struct PinkDb {
    tables: HashMap<String, DataTable>,
}

impl PinkDb {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    pub fn create_table(
        &mut self,
        name: &str,
        schema: Vec<(String, DataType)>,
    ) -> Result<()> {
        let key = Self::key(name);
        if self.tables.contains_key(&key) {
            return Err(PinkDbError::Catalog(format!(
                "table \"{name}\" already exists"
            )));
        }
        self.tables
            .insert(key, DataTable::new(name.to_string(), schema)?);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.tables
            .remove(&Self::key(name))
            .map(|_| ())
            .ok_or_else(|| PinkDbError::Catalog(format!("table \"{name}\" not found")))
    }

    pub fn table(&self, name: &str) -> Result<&DataTable> {
        self.tables.get(&Self::key(name)).ok_or_else(|| {
            PinkDbError::Catalog(format!("table \"{name}\" not found"))
        })
    }

    pub fn table_mut(&mut self, name: &str) -> Result<&mut DataTable> {
        self.tables.get_mut(&Self::key(name)).ok_or_else(|| {
            PinkDbError::Catalog(format!("table \"{name}\" not found"))
        })
    }

    /// Sorted table names (stable output for `.tables`).
    pub fn list_tables(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.values().map(|t| t.name.clone()).collect();
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DataTable {
        let mut t = DataTable::new(
            "t".to_string(),
            vec![
                ("id".to_string(), DataType::Bigint),
                ("name".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        t.append_row(vec![
            Value::Bigint(1),
            Value::Varchar("ann".to_string()),
        ])
        .unwrap();
        // Integer widens to Bigint.
        t.append_row(vec![Value::Integer(2), Value::Null]).unwrap();
        t
    }

    #[test]
    fn create_and_lookup_case_insensitive() {
        let mut db = PinkDb::new();
        db.create_table(
            "Sales",
            vec![("id".to_string(), DataType::Integer)],
        )
        .unwrap();
        assert!(db.table("sales").is_ok());
        assert!(db.table("SALES").is_ok());
        assert_eq!(db.list_tables(), vec!["Sales".to_string()]);
        assert!(db.create_table("sales", vec![]).is_err());
        assert!(db.table("missing").is_err());
    }

    #[test]
    fn append_and_get_with_coercion() {
        let t = sample();
        assert_eq!(t.num_rows(), 2);
        assert_eq!(t.get(0, 0), Some(Value::Bigint(1)));
        assert_eq!(t.get(1, 0), Some(Value::Bigint(2)));
        assert_eq!(t.get(1, 1), Some(Value::Null));
        assert_eq!(t.get(9, 0), None);
    }

    #[test]
    fn coercion_rejects_bad_types() {
        let mut t = sample();
        let err = t
            .append_row(vec![
                Value::Varchar("x".to_string()),
                Value::Varchar("y".to_string()),
            ])
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Type(_)));
        let err = t.append_row(vec![Value::Null]).unwrap_err();
        assert!(matches!(err, PinkDbError::Execution(_)));
    }

    #[test]
    fn chunk_boundary_stays_aligned() {
        let mut t = DataTable::new(
            "big".to_string(),
            vec![("a".to_string(), DataType::Integer)],
        )
        .unwrap();
        for i in 0..(CHUNK_SIZE + 5) {
            t.append_row(vec![Value::Integer(i as i32)]).unwrap();
        }
        assert_eq!(t.num_chunks(), 2);
        assert_eq!(t.scan_chunk(0).unwrap().num_rows(), CHUNK_SIZE);
        assert_eq!(t.scan_chunk(1).unwrap().num_rows(), 5);
        assert!(t.scan_chunk(2).is_none());
        assert_eq!(t.get(CHUNK_SIZE, 0), Some(Value::Integer(CHUNK_SIZE as i32)));
    }

    #[test]
    fn duplicate_columns_rejected() {
        let err = DataTable::new(
            "d".to_string(),
            vec![
                ("a".to_string(), DataType::Integer),
                ("A".to_string(), DataType::Integer),
            ],
        )
        .unwrap_err();
        assert!(matches!(err, PinkDbError::Catalog(_)));
    }
}
