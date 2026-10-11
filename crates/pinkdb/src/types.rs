//! Core types: [`DataType`], [`Value`], [`PinkDbError`].
//!
//! v1 types: BOOLEAN, INTEGER (i32), BIGINT (i64), DOUBLE (f64), VARCHAR.

use std::fmt;

/// Column data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
}

impl DataType {
    /// DuckDB-style display name.
    pub fn name(self) -> &'static str {
        match self {
            DataType::Boolean => "BOOLEAN",
            DataType::Integer => "INTEGER",
            DataType::Bigint => "BIGINT",
            DataType::Double => "DOUBLE",
            DataType::Varchar => "VARCHAR",
        }
    }

    /// Parse a type name from `CREATE TABLE` (case-insensitive).
    /// Aliases: INT → INTEGER, TEXT/STRING → VARCHAR, FLOAT → DOUBLE.
    pub fn from_sql(s: &str) -> Option<DataType> {
        match s.to_ascii_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => Some(DataType::Boolean),
            "INTEGER" | "INT" => Some(DataType::Integer),
            "BIGINT" | "LONG" => Some(DataType::Bigint),
            "DOUBLE" | "FLOAT" | "REAL" => Some(DataType::Double),
            "VARCHAR" | "TEXT" | "STRING" => Some(DataType::Varchar),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A single scalar value. `Null` carries no type; the column does.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i32),
    Bigint(i64),
    Double(f64),
    Varchar(String),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::Bigint(_) => Some(DataType::Bigint),
            Value::Double(_) => Some(DataType::Double),
            Value::Varchar(_) => Some(DataType::Varchar),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Bigint(i) => write!(f, "{i}"),
            Value::Double(x) => write!(f, "{x}"),
            Value::Varchar(s) => write!(f, "{s}"),
        }
    }
}

/// All engine errors. Lean: a few string-carrying variants + transparent
/// IO/CSV wrappers. No extra deps (no thiserror).
#[derive(Debug)]
pub enum PinkDbError {
    Catalog(String),
    Type(String),
    Parse(String),
    Execution(String),
    Io(std::io::Error),
    Csv(csv::Error),
}

impl fmt::Display for PinkDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinkDbError::Catalog(msg)
            | PinkDbError::Type(msg)
            | PinkDbError::Parse(msg)
            | PinkDbError::Execution(msg) => write!(f, "{msg}"),
            PinkDbError::Io(e) => write!(f, "IO error: {e}"),
            PinkDbError::Csv(e) => write!(f, "CSV error: {e}"),
        }
    }
}

impl std::error::Error for PinkDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PinkDbError::Io(e) => Some(e),
            PinkDbError::Csv(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PinkDbError {
    fn from(e: std::io::Error) -> Self {
        PinkDbError::Io(e)
    }
}

impl From<csv::Error> for PinkDbError {
    fn from(e: csv::Error) -> Self {
        PinkDbError::Csv(e)
    }
}

pub type Result<T> = std::result::Result<T, PinkDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_roundtrip() {
        assert_eq!(DataType::from_sql("integer"), Some(DataType::Integer));
        assert_eq!(DataType::from_sql("INT"), Some(DataType::Integer));
        assert_eq!(DataType::from_sql("bigint"), Some(DataType::Bigint));
        assert_eq!(DataType::from_sql("double"), Some(DataType::Double));
        assert_eq!(DataType::from_sql("varchar"), Some(DataType::Varchar));
        assert_eq!(DataType::from_sql("text"), Some(DataType::Varchar));
        assert_eq!(DataType::from_sql("boolean"), Some(DataType::Boolean));
        assert_eq!(DataType::from_sql("nope"), None);
        assert_eq!(DataType::Bigint.name(), "BIGINT");
    }

    #[test]
    fn value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Integer(-3).to_string(), "-3");
        assert_eq!(Value::Varchar("hi".to_string()).to_string(), "hi");
    }
}
