//! The small connection surface the SQL catalog core is written against
//! (wave 103).
//!
//! [`store`](super::store) owns every statement, every DuckLake row and the
//! whole commit loop; it never names a backend. A backend supplies exactly
//! this: execute one bound statement, run a query and materialize its rows,
//! run a DDL batch, begin/commit/rollback a write transaction, and report its
//! [`Dialect`]. SQLite and pinkdb are the two implementations; T2's Postgres
//! adds a third.
//!
//! Values are a tiny tagged union rather than either backend's type: the core
//! only needs the four storage classes both databases share (NULL, integer,
//! real, text) plus bytes for the opaque side-object table. Placeholders stay
//! numbered (`?1`, `?2`, …) in the core's SQL, because one parameter is
//! legitimately repeated by several predicates; an adapter that only accepts
//! positional `?` expands the repeats while binding (pinkdb's prepared
//! statements are strictly positional).

use std::path::PathBuf;

use super::dialect::Dialect;
use crate::{LakeError, Result};

/// One SQL storage value. This is the whole type surface the catalog core
/// binds and reads; adapters convert to and from their own value types.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    /// Binary only reaches the opaque `pinkdb_object.bytes` column.
    Blob(Vec<u8>),
}

/// A value that can be bound as one placeholder.
pub(crate) trait ToSqlValue {
    fn to_sql(&self) -> SqlValue;
}

impl ToSqlValue for i64 {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Integer(*self)
    }
}

impl ToSqlValue for i32 {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Integer(i64::from(*self))
    }
}

impl ToSqlValue for u32 {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Integer(i64::from(*self))
    }
}

// `u64`/`usize` are deliberately not bindable: a value above `i64::MAX` has
// no SQL integer representation, and silently clamping it (the old behaviour)
// would corrupt an id or row count. Callers convert through `i64_from_u64`,
// which is where the overflow policy belongs.

impl ToSqlValue for f64 {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Real(*self)
    }
}

impl ToSqlValue for f32 {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Real(f64::from(*self))
    }
}

impl ToSqlValue for bool {
    fn to_sql(&self) -> SqlValue {
        // Both backends store booleans as 0/1 BIGINT in DuckLake's schema.
        SqlValue::Integer(i64::from(*self))
    }
}

impl ToSqlValue for str {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Text(self.to_string())
    }
}

impl ToSqlValue for String {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Text(self.clone())
    }
}

impl ToSqlValue for [u8] {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Blob(self.to_vec())
    }
}

impl ToSqlValue for Vec<u8> {
    fn to_sql(&self) -> SqlValue {
        SqlValue::Blob(self.clone())
    }
}

impl<T: ToSqlValue> ToSqlValue for Option<T> {
    fn to_sql(&self) -> SqlValue {
        match self {
            Some(value) => value.to_sql(),
            None => SqlValue::Null,
        }
    }
}

impl<T: ToSqlValue + ?Sized> ToSqlValue for &T {
    fn to_sql(&self) -> SqlValue {
        (*self).to_sql()
    }
}

/// Bind `value` (the macro below always passes a reference so a field of a
/// borrowed struct is never moved).
pub(crate) fn to_value<T: ToSqlValue + ?Sized>(value: &T) -> SqlValue {
    value.to_sql()
}

/// Build the `&[SqlValue]` a statement binds. Numbered placeholders are in
/// the order they appear as `?1` … `?N`.
macro_rules! params {
    () => {
        &[] as &[SqlValue]
    };
    ($($value:expr),* $(,)?) => {
        &[$($crate::metadata::conn::to_value(&$value)),*] as &[SqlValue]
    };
}

pub(crate) use params;

/// One result row, materialized as [`SqlValue`]s so the adapter's cursor can
/// be closed before the core inspects it.
#[derive(Debug, Clone)]
pub(crate) struct SqlRow {
    values: Vec<SqlValue>,
}

impl SqlRow {
    pub(crate) fn new(values: Vec<SqlValue>) -> Self {
        Self { values }
    }

    /// Read column `index` as `T`. The error names the column, never a
    /// backend's cursor type. The extra index generic mirrors rusqlite's
    /// `row.get::<_, T>(i)` so the SQL core's call sites stayed unchanged.
    pub(crate) fn get<I: SqlIndex, T: FromSqlValue>(&self, index: I) -> Result<T> {
        let index = index.index();
        let value = self.values.get(index).ok_or_else(|| LakeError::Internal {
            message: format!(
                "the catalog query returned {} column(s); column {} was requested",
                self.values.len(),
                index + 1
            ),
        })?;
        T::from_sql(value)
    }
}

/// A column index: a literal `0` is inferred as `i32`, an `index + 1` as
/// `usize`.
pub(crate) trait SqlIndex {
    fn index(self) -> usize;
}

impl SqlIndex for usize {
    fn index(self) -> usize {
        self
    }
}

impl SqlIndex for i32 {
    fn index(self) -> usize {
        usize::try_from(self).unwrap_or(usize::MAX)
    }
}

impl SqlIndex for u32 {
    fn index(self) -> usize {
        self as usize
    }
}

/// Conversion from a [`SqlValue`] to a Rust field. Mirrors the handful of
/// rusqlite conversions the SQL core used before the split.
pub(crate) trait FromSqlValue: Sized {
    fn from_sql(value: &SqlValue) -> Result<Self>;
}

fn type_error(expected: &str, value: &SqlValue) -> LakeError {
    LakeError::Corrupt {
        path: PathBuf::from("catalog"),
        message: format!("the catalog row holds {value:?}, not {expected}"),
    }
}

impl FromSqlValue for i64 {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Integer(v) => Ok(*v),
            other => Err(type_error("an integer", other)),
        }
    }
}

impl FromSqlValue for i32 {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        let v = i64::from_sql(value)?;
        i32::try_from(v).map_err(|_| LakeError::Corrupt {
            path: PathBuf::from("catalog"),
            message: format!("catalog value {v} does not fit a 32-bit integer"),
        })
    }
}

impl FromSqlValue for u64 {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        let v = i64::from_sql(value)?;
        Ok(u64::try_from(v).unwrap_or(0))
    }
}

impl FromSqlValue for f64 {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Real(v) => Ok(*v),
            SqlValue::Integer(v) => Ok(*v as f64),
            other => Err(type_error("a real", other)),
        }
    }
}

impl FromSqlValue for bool {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        Ok(i64::from_sql(value)? != 0)
    }
}

impl FromSqlValue for String {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Text(v) => Ok(v.clone()),
            other => Err(type_error("text", other)),
        }
    }
}

impl FromSqlValue for Vec<u8> {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Blob(v) => Ok(v.clone()),
            // pinkdb has no BLOB type: the opaque side-table bytes are stored
            // as lower-case hex text, so a text value is the encoded form.
            SqlValue::Text(v) => Ok(decode_hex(v).unwrap_or_else(|| v.as_bytes().to_vec())),
            other => Err(type_error("bytes", other)),
        }
    }
}

impl FromSqlValue for SqlValue {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        Ok(value.clone())
    }
}

impl<T: FromSqlValue> FromSqlValue for Option<T> {
    fn from_sql(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Null => Ok(None),
            other => T::from_sql(other).map(Some),
        }
    }
}

/// Lower-case hex (pinkdb's `VARCHAR` stand-in for the BLOB column).
pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    for pair in bytes.chunks(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        out.push(high * 16 + low);
    }
    Some(out)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// One SQL connection. Implementations serialize access internally (the lake
/// handle shares the store across threads), so every method takes `&self`.
///
/// A transaction is a *handle*: [`SqlConnection::begin`] returns a connection
/// whose statements all run inside that transaction, and
/// [`SqlConnection::commit`]/[`SqlConnection::rollback`] finish it. SQLite
/// opens a second `Connection` (its documented multi-connection model);
/// pinkdb shares the durable store and serializes transaction owners on a
/// gate, because one `PinkDb` carries one transaction state.
pub(crate) trait SqlConnection: Send + Sync + std::fmt::Debug {
    fn dialect(&self) -> Dialect;

    /// Execute one statement (DDL or counted DML) and return the number of
    /// changed rows.
    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<usize>;

    /// Run a query and materialize every row.
    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>>;

    /// Run a semicolon-separated DDL batch (schema creation only).
    fn execute_batch(&self, sql: &str) -> Result<()>;

    /// Start a write transaction and return its handle.
    fn begin(&self) -> Result<Box<dyn SqlConnection>>;

    /// Commit the transaction owned by this handle.
    fn commit(&self) -> Result<()>;

    /// Roll the transaction owned by this handle back.
    fn rollback(&self) -> Result<()>;

    /// Rows changed since this connection was opened (the scaling
    /// diagnostic; only SQLite can answer it exactly).
    fn total_changes(&self) -> u64;
}

/// Statement helpers written against `&dyn SqlConnection`, so a store method
/// can call `conn.query_row(...)` / `conn.prepare(...)` exactly where it used
/// to call rusqlite.
pub(crate) trait SqlConnectionExt {
    /// Run a query and map its first row, or `None` when it returned no rows.
    fn query_row<T>(
        &self,
        sql: &str,
        params: impl AsRef<[SqlValue]>,
        f: impl FnOnce(&SqlRow) -> Result<T>,
    ) -> Result<Option<T>>;

    /// A prepared statement (the adapter prepares on first use). The `Result`
    /// shape keeps every call site's context mapping; preparation itself
    /// happens when the rows are requested.
    fn prepare<'a>(&'a self, sql: &'a str) -> Result<Prepared<'a>>;
}

impl SqlConnectionExt for dyn SqlConnection + '_ {
    fn query_row<T>(
        &self,
        sql: &str,
        params: impl AsRef<[SqlValue]>,
        f: impl FnOnce(&SqlRow) -> Result<T>,
    ) -> Result<Option<T>> {
        let rows = self.query(sql, params.as_ref())?;
        rows.first().map(f).transpose()
    }

    fn prepare<'a>(&'a self, sql: &'a str) -> Result<Prepared<'a>> {
        Ok(Prepared { conn: self, sql })
    }
}

/// A statement that always returns exactly one row (an aggregate such as
/// `MAX(…)`/`COUNT(*)`, or a lookup by primary key the commit just wrote).
/// A missing row is a corrupt catalog, not an optional value.
pub(crate) fn query_one<T>(
    conn: &dyn SqlConnection,
    sql: &str,
    params: impl AsRef<[SqlValue]>,
    f: impl FnOnce(&SqlRow) -> Result<T>,
) -> Result<T> {
    conn.query_row(sql, params, f)?
        .ok_or_else(|| LakeError::Corrupt {
            path: std::path::PathBuf::from("catalog"),
            message: format!("the catalog query returned no row: {sql}"),
        })
}

/// A statement about to run. `query_map` materializes the rows: the closure's
/// `Result` is kept per row so existing call sites can keep reporting the
/// query context on either the statement or a conversion failure.
pub(crate) struct Prepared<'a> {
    conn: &'a dyn SqlConnection,
    sql: &'a str,
}

impl Prepared<'_> {
    pub(crate) fn query_map<T>(
        &self,
        params: impl AsRef<[SqlValue]>,
        f: impl FnMut(&SqlRow) -> Result<T>,
    ) -> Result<Vec<Result<T>>> {
        let rows = self.conn.query(self.sql, params.as_ref())?;
        Ok(rows.iter().map(f).collect())
    }
}
