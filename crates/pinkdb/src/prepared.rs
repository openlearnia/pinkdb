//! Prepared statements: `?` placeholders, bind-time type inference, and
//! parameter substitution for execution.
//!
//! ```no_run
//! use pinkdb::{PinkDb, Value};
//!
//! let mut db = PinkDb::new();
//! db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
//! let insert = db.prepare("INSERT INTO t VALUES (?, ?);").unwrap();
//! db.execute_prepared(&insert, &[Value::Integer(1), Value::Varchar("x".into())]).unwrap();
//! let select = db.prepare("SELECT b FROM t WHERE a = ?;").unwrap();
//! let result = db.execute_prepared(&select, &[Value::Integer(1)]).unwrap();
//! # let _ = result;
//! ```
//!
//! # Semantics
//!
//! `?` is a positional placeholder; placeholders are numbered in the order
//! they appear in the SQL text (multi-statement strings are refused — one
//! prepared statement is exactly one statement). `$1`-style named
//! parameters are **not** accepted: `$` is not part of the SQL surface, and
//! each `?` is a distinct slot (repeat the value for a repeated use).
//!
//! [`PinkDb::prepare`] parses only: it validates the statement shape (one
//! statement, no placeholder in a clause that cannot bind — `ORDER BY`,
//! `GROUP BY`, `CREATE VIEW`) and counts the placeholders. Types are resolved
//! at bind time, against the current catalog, by
//! [`PreparedStatement::bind`]/[`PinkDb::execute_prepared`]:
//!
//! 1. **Context inference.** A placeholder is typed by the first context that
//!    needs a type: the other side of a comparison/arithmetic operator
//!    (`a > ?` and `? + 1`), the target column of `INSERT`/`UPDATE`, and the
//!    target of `CAST(? AS t)` (which pins `t` outright). `LIKE`/`||` pin
//!    VARCHAR, `AND`/`OR` pin BOOLEAN, `date ± ?` pins INTEGER days.
//! 2. **Value fallback.** A placeholder no context constrains takes its
//!    value's own type.
//! 3. **Conversion.** The bound value converts to the resolved type with the
//!    engine's `CAST` rules; an impossible conversion is a clear bind-time
//!    error (`cannot bind parameter 2 as INTEGER: ...`). A `NULL` value keeps
//!    its typeless form.
//! 4. **Unresolved.** A `NULL` value with no context and no `CAST` cannot be
//!    typed: bind fails with
//!    `could not determine data type of parameter N: ...`. (DuckDB defaults
//!    such a parameter to INTEGER; this engine asks for the explicit type
//!    instead — a mistyped statement then fails at bind, never later.)
//! 5. **Reuse.** Binding is per execute: the same [`PreparedStatement`] can
//!    run repeatedly, with fresh catalog/type resolution every time, inside
//!    open transactions included.
//!
//! # Durability
//!
//! Executing a prepared DML statement on a durable database logs the
//! **fully substituted** SQL to the WAL (each `?` is replaced by a literal:
//! numbers, quoted/doubled-quote strings, `NULL`, `DATE '...'`,
//! `TIMESTAMP '...'`), so the normal replay path sees an ordinary statement.
//! Execution goes through the same paths as hand-written SQL — autocommit,
//! optimistic commits, transaction buffering, session `USE` all behave
//! identically; only the SQL text the WAL records differs (it carries the
//! values, not the placeholders).

use crate::QueryResult;
use crate::sql::ast;
use crate::sql::binder::{ParamSlots, bind_tracked_with};
use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value};

/// A parsed, reusable single SQL statement with `?` placeholders.
///
/// Produced by [`PinkDb::prepare`] / [`Connection::prepare`](crate::Connection::prepare).
/// Cheap to clone; binding never mutates it, so one prepared statement may be
/// shared freely between binds (each bind takes its own values).
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedStatement {
    /// The original SQL text (still holding `?`s).
    sql: String,
    /// The parsed statement.
    ast: ast::Statement,
    /// Byte offset of every `?` in `sql`, in textual order.
    offsets: Vec<usize>,
}

impl PreparedStatement {
    /// Parse `sql` as a prepared statement: exactly one statement.
    pub fn parse(sql: &str) -> Result<PreparedStatement> {
        let (mut stmts, offsets) = crate::sql::parse_prepared(sql)?;
        if stmts.len() != 1 {
            return Err(PinkDbError::Parse(format!(
                "a prepared statement must contain exactly one statement (found {})",
                stmts.len()
            )));
        }
        // The parser records each `?`'s byte offset; the raw-text scan must
        // agree (strings, quoted identifiers and comments skipped on both
        // sides), or substitution would splice into the wrong bytes.
        let scanned = crate::sql::parameter_offsets(sql);
        if scanned != offsets {
            return Err(PinkDbError::Parameter(
                "internal error: parser and text scan disagree on placeholder positions"
                    .to_string(),
            ));
        }
        Ok(PreparedStatement {
            sql: sql.to_string(),
            ast: stmts.pop().expect("checked one statement"),
            offsets,
        })
    }

    /// The original SQL text (`?` placeholders included).
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Number of `?` placeholders: the exact number of values a bind takes.
    pub fn parameter_count(&self) -> usize {
        self.offsets.len()
    }

    /// Bind `params` (one value per placeholder, in textual order) against
    /// `db`, returning the substituted SQL text plus the inferred parameter
    /// types.
    ///
    /// This is the type-inference/validation step: it dry-binds the statement
    /// against the current catalog, so every bind-time error (unknown column,
    /// incompatible value, untypable `NULL`) surfaces here, before anything
    /// executes. Does not mutate `db`.
    pub fn bind(&self, db: &PinkDb, default_db: &str, params: &[Value]) -> Result<BoundStatement> {
        check_arity(self.offsets.len(), params.len())?;
        let slots = ParamSlots::new(params.to_vec());
        // Dry bind: inference, conversion, and every bind-time check happen
        // here; the plan itself is discarded (execution re-binds the
        // substituted text through the statement's normal path).
        bind_tracked_with(db, &self.ast, default_db, Some(slots.clone()))?;
        let mut values = Vec::with_capacity(params.len());
        let mut types = Vec::with_capacity(params.len());
        for i in 0..params.len() {
            match (slots.resolved_value(i), slots.slot_type(i)) {
                (Some(value), Some(dtype)) => {
                    values.push(value);
                    types.push(dtype);
                }
                _ => {
                    return Err(PinkDbError::Parameter(format!(
                        "internal error: parameter {} did not bind",
                        i + 1
                    )));
                }
            }
        }
        let sql = substitute(&self.sql, &self.offsets, &values)?;
        Ok(BoundStatement {
            sql,
            parameter_types: types,
        })
    }
}

/// A [`PreparedStatement`] bound to concrete values: the executed SQL text
/// with every placeholder replaced by a literal, plus the inferred types.
///
/// Produced by [`PreparedStatement::bind`] (also via
/// [`PinkDb::execute_prepared`], which binds and runs in one call). Execute
/// it with [`PinkDb::execute_bound`] /
/// [`Connection::execute_bound`](crate::Connection::execute_bound).
#[derive(Debug, Clone, PartialEq)]
pub struct BoundStatement {
    sql: String,
    parameter_types: Vec<DataType>,
}

impl BoundStatement {
    /// The fully substituted SQL text (no placeholders left).
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The type inferred for each parameter, in textual order.
    pub fn parameter_types(&self) -> &[DataType] {
        &self.parameter_types
    }
}

impl PinkDb {
    /// Parse `sql` into a reusable [`PreparedStatement`] (exactly one
    /// statement; `?` placeholders allowed). No catalog access, no type
    /// checking: those happen at [`PreparedStatement::bind`].
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        PreparedStatement::parse(sql)
    }

    /// Bind `params` and execute the prepared statement like a normal
    /// statement of this session (fresh state per execute; inside an open
    /// transaction it runs within it).
    pub fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let bound = stmt.bind(self, self.current_db(), params)?;
        self.execute_bound(&bound)
    }

    /// Execute a statement already bound to values.
    pub fn execute_bound(&mut self, bound: &BoundStatement) -> Result<QueryResult> {
        let mut results = self.execute(bound.sql())?;
        debug_assert_eq!(results.len(), 1, "a bound statement is one statement");
        results.pop().ok_or_else(|| {
            PinkDbError::Execution("internal error: bound statement produced no result".to_string())
        })
    }
}

/// Parameter-count mismatch: the message names the missing/excess positions
/// (1-based), like DuckDB's bind errors do.
pub(crate) fn check_arity(expected: usize, got: usize) -> Result<()> {
    if expected == got {
        return Ok(());
    }
    if got < expected {
        let missing: Vec<String> = (got + 1..=expected).map(|i| i.to_string()).collect();
        Err(PinkDbError::Parameter(format!(
            "values were not provided for prepared statement parameters: {}",
            missing.join(", ")
        )))
    } else {
        let excess: Vec<String> = (expected + 1..=got).map(|i| i.to_string()).collect();
        Err(PinkDbError::Parameter(format!(
            "parameter count mismatch: the prepared statement takes {expected}, \
             but values were provided for: {}",
            excess.join(", ")
        )))
    }
}

/// Replace every `?` at `offsets[i]` with the SQL literal for `values[i]`.
///
/// `offsets` is ascending and points at `?` bytes; anything else is treated
/// as an internal-state error (the offsets are validated at parse time and
/// the statement text is immutable).
fn substitute(sql: &str, offsets: &[usize], values: &[Value]) -> Result<String> {
    if offsets.len() != values.len() {
        return Err(PinkDbError::Parameter(
            "internal error: placeholder/value count mismatch".to_string(),
        ));
    }
    let mut out = String::with_capacity(sql.len() + 24 * values.len());
    let mut last = 0;
    for (i, &off) in offsets.iter().enumerate() {
        if off < last || sql.as_bytes().get(off) != Some(&b'?') {
            return Err(PinkDbError::Parameter(
                "internal error: placeholder offsets out of sync with the statement text"
                    .to_string(),
            ));
        }
        out.push_str(&sql[last..off]);
        let literal = value_literal(&values[i])
            .map_err(|e| PinkDbError::Parameter(format!("cannot bind parameter {}: {e}", i + 1)))?;
        out.push_str(&literal);
        last = off + 1;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// First day whose 4-digit-year literal form (`DATE 'YYYY-MM-DD'`) parses.
const MIN_LITERAL_DAY: i32 = -719_528; // 0000-01-01
/// Last day whose 4-digit-year literal form parses.
const MAX_LITERAL_DAY: i32 = 2_932_896; // 9999-12-31

/// Render one bound value back to SQL text that re-parses to exactly the same
/// value (or a clear error for the few values the literal surface cannot
/// express). This is the text a durable database logs, so exactness here is
/// what makes prepared DML replay.
pub(crate) fn value_literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        // The negative extremes cannot be written as plain literals (`-2147483648`
        // parses as BIGINT negation, `-9223372036854775808` overflows the
        // positive i64 literal), so pin them through a CAST of their text.
        Value::Integer(i) if *i == i32::MIN => {
            format!("CAST('{}' AS INTEGER)", i)
        }
        Value::Integer(i) => i.to_string(),
        // A BIGINT in i32 range would re-parse as INTEGER; pin it so the
        // substituted text keeps the parameter's exact type.
        Value::Bigint(i) if (i32::MIN as i64..=i32::MAX as i64).contains(i) => {
            format!("CAST({i} AS BIGINT)")
        }
        Value::Bigint(i) if *i == i64::MIN => {
            format!("CAST('{}' AS BIGINT)", i)
        }
        Value::Bigint(i) => i.to_string(),
        // `{:?}` is the shortest form that round-trips an f64, with a `.0`
        // for integral values (so the literal stays DOUBLE, never INTEGER).
        Value::Double(x) if x.is_finite() => format!("{x:?}"),
        Value::Double(x) if x.is_nan() => "CAST('NaN' AS DOUBLE)".to_string(),
        Value::Double(x) if x.is_sign_negative() => "CAST('-inf' AS DOUBLE)".to_string(),
        Value::Double(_) => "CAST('inf' AS DOUBLE)".to_string(),
        Value::Varchar(s) => format!("'{}'", s.replace('\'', "''")),
        // DECIMAL keeps its exact unscaled value: pin through the widest
        // precision, which every engine-produced DECIMAL(p ≤ 38) fits.
        Value::Decimal(x, scale) => format!(
            "CAST('{}' AS DECIMAL(38,{scale}))",
            crate::types::format_decimal(*x, *scale)
        ),
        // INTERVAL's canonical text round-trips through its literal form.
        Value::Interval {
            months,
            days,
            micros,
        } => format!(
            "INTERVAL '{}'",
            crate::types::format_interval(*months, *days, *micros)
        ),
        Value::Date(days) if (MIN_LITERAL_DAY..=MAX_LITERAL_DAY).contains(days) => {
            format!("DATE '{}'", crate::types::format_date(*days))
        }
        // Out of the 4-digit-year literal range: rebuild through day
        // arithmetic (exact, and never overflows: one operand is 0).
        Value::Date(days) => format!("(DATE '1970-01-01' + CAST('{days}' AS INTEGER))"),
        Value::Timestamp(us) => {
            let (days, ..) = crate::types::split_timestamp(*us);
            if (MIN_LITERAL_DAY..=MAX_LITERAL_DAY).contains(&days) {
                format!("TIMESTAMP '{}'", crate::types::format_timestamp(*us))
            } else {
                return Err(PinkDbError::Parameter(format!(
                    "TIMESTAMP value {us} is outside the SQL literal range \
                     (years 0000-01-01 to 9999-12-31)"
                )));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_round_trips_through_the_parser() {
        let corpus = [
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Integer(0),
            Value::Integer(-1),
            Value::Integer(i32::MIN),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Bigint(0),
            Value::Double(0.0),
            Value::Double(-0.0),
            Value::Double(2.5),
            Value::Double(-1.0e300),
            Value::Double(f64::MIN_POSITIVE),
            Value::Double(f64::MAX),
            Value::Double(1.0 / 3.0),
            Value::Double(f64::NAN),
            Value::Double(f64::INFINITY),
            Value::Double(f64::NEG_INFINITY),
            Value::Varchar(String::new()),
            Value::Varchar("plain".to_string()),
            Value::Varchar("it's".to_string()),
            Value::Varchar("a''b; -- not a comment".to_string()),
            Value::Varchar("%_\\".to_string()),
            Value::Date(0),
            Value::Date(-1),
            Value::Date(i32::MIN),
            Value::Date(i32::MAX),
            Value::Timestamp(0),
            Value::Timestamp(-1),
            Value::Timestamp(1_234_567_890_123_456),
            Value::Timestamp(-1_234_567_890_123_456),
        ];
        for value in corpus {
            let lit = value_literal(&value).unwrap();
            let sql = format!("SELECT {lit};");
            let mut db = PinkDb::new();
            let out = db
                .execute(&sql)
                .unwrap_or_else(|e| panic!("literal {lit:?} for {value:?} failed: {e}"));
            match &out[0] {
                QueryResult::Selected { rows, .. } => {
                    assert_eq!(rows.len(), 1, "{sql}");
                    assert_eq!(rows[0].len(), 1, "{sql}");
                    let got = &rows[0][0];
                    if value.is_null() {
                        assert!(got.is_null(), "{sql}");
                    } else if matches!(value, Value::Double(x) if x.is_nan()) {
                        assert!(matches!(got, Value::Double(x) if x.is_nan()), "{sql}");
                    } else if let (Value::Double(a), Value::Double(b)) = (got, &value) {
                        // Bit-exact: NaN aside, the shortest round-trip form
                        // must reproduce the exact f64 (sign of zero included).
                        assert_eq!(a.to_bits(), b.to_bits(), "literal {lit:?} round-trip");
                    } else {
                        assert_eq!(*got, value, "literal {lit:?} round-trip");
                    }
                }
                other => panic!("expected a SELECT result, got {other:?}"),
            }
        }
    }

    #[test]
    fn value_literal_escapes_quotes() {
        assert_eq!(
            value_literal(&Value::Varchar("it's".to_string())).unwrap(),
            "'it''s'"
        );
        assert_eq!(value_literal(&Value::Null).unwrap(), "NULL");
        assert_eq!(value_literal(&Value::Boolean(false)).unwrap(), "false");
        assert_eq!(value_literal(&Value::Integer(7)).unwrap(), "7");
        assert_eq!(
            value_literal(&Value::Bigint(i64::MIN)).unwrap(),
            "CAST('-9223372036854775808' AS BIGINT)"
        );
        assert_eq!(
            value_literal(&Value::Bigint(0)).unwrap(),
            "CAST(0 AS BIGINT)"
        );
        assert_eq!(value_literal(&Value::Double(1.0)).unwrap(), "1.0");
        assert_eq!(value_literal(&Value::Date(0)).unwrap(), "DATE '1970-01-01'");
        assert_eq!(
            value_literal(&Value::Timestamp(0)).unwrap(),
            "TIMESTAMP '1970-01-01 00:00:00'"
        );
        // An out-of-literal-range DATE rebuilds through day arithmetic and
        // still round-trips.
        let extreme = Value::Date(i32::MAX);
        let lit = value_literal(&extreme).unwrap();
        assert!(lit.contains("1970-01-01"), "{lit}");
        let mut db = PinkDb::new();
        let out = db.execute(&format!("SELECT {lit};")).unwrap();
        match &out[0] {
            QueryResult::Selected { rows, .. } => assert_eq!(rows[0][0], extreme),
            other => panic!("expected a SELECT result, got {other:?}"),
        }
    }

    #[test]
    fn substitute_replaces_only_marked_offsets() {
        let sql = "SELECT ?, '?' FROM t -- ?\nWHERE a = ?";
        // Offsets of the three raw `?` bytes; only the parser's two (the
        // placeholder and the WHERE one) are substituted — the string and
        // comment ones are skipped by the scan that produced the offsets.
        let offsets = vec![7, 36];
        assert_eq!(sql.as_bytes()[7], b'?');
        assert_eq!(sql.as_bytes()[36], b'?');
        let out = substitute(
            sql,
            &offsets,
            &[Value::Integer(1), Value::Varchar("x".into())],
        )
        .unwrap();
        assert_eq!(out, "SELECT 1, '?' FROM t -- ?\nWHERE a = 'x'");
        // A mismatch in the offset stream is caught, not spliced blindly.
        assert!(substitute("SELECT ?", &[3], &[Value::Integer(1)]).is_err());
        assert!(substitute("SELECT ?", &[8], &[Value::Integer(1)]).is_err());
        assert!(substitute("SELECT ?", &[7, 7], &[Value::Integer(1), Value::Integer(1)]).is_err());
    }

    #[test]
    fn arity_messages_name_positions() {
        let missing = check_arity(3, 1).unwrap_err().to_string();
        assert!(missing.contains("2, 3"), "{missing}");
        let excess = check_arity(1, 3).unwrap_err().to_string();
        assert!(excess.contains("2, 3"), "{excess}");
        assert!(check_arity(2, 2).is_ok());
    }
}
