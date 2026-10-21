//! Read-only store access for the console.
//!
//! The console reaches the store through the **engine's own SQL surface**:
//! a [`pinkdb::Connection`] on the same [`pinkdb::Database`] handle the
//! server serves. That keeps one source of truth (the console sees exactly
//! what the server sees, including a durable store's WAL state) and it makes
//! the console's read-only guarantee structural: the statements below are
//! the only statements the console can ever run.
//!
//! # The statements
//!
//! | Purpose            | Statement                                  |
//! |--------------------|--------------------------------------------|
//! | list databases     | `SHOW DATABASES`                           |
//! | list relations     | `SHOW TABLES` (session database)           |
//! | relation columns   | `DESCRIBE "db"."table"`                    |
//! | row count          | `SELECT COUNT(*) FROM "db"."table"`        |
//! | a page of rows     | `SELECT * FROM "db"."table" LIMIT n OFFSET m` |
//!
//! There is no DML and no DDL here. The one session statement the console
//! uses is `USE` (to browse a database other than the session's current
//! one): it changes **no** table, catalog or row — on a durable store the
//! engine appends a *session record* to the WAL so a replay reproduces the
//! same session, which is the engine's documented behavior for `USE`.
//! Sessions are pooled, so the switch happens once per pooled session and
//! database, not once per request.
//!
//! # Locking
//!
//! Every call below runs one statement under the engine's own locks and
//! returns materialized rows. The store lock is **released** before the
//! caller does any HTTP work (JSON/HTML rendering happens after the
//! blocking task returned).

use pinkdb::{Connection, DataType, Database, ExecLimits, QueryResult, Value};

/// One column of a relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnInfo {
    /// The column name.
    pub name: String,
    /// The DuckDB-style type name (`INTEGER`, `VARCHAR`, `DECIMAL(18,3)`, …).
    pub dtype: String,
    /// The column accepts NULLs (`DESCRIBE`'s `null` column says `YES`).
    pub nullable: bool,
}

/// One relation (a table or a view) of a database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationInfo {
    /// The relation name.
    pub name: String,
    /// The relation's columns.
    pub columns: Vec<ColumnInfo>,
}

/// One page of rows.
#[derive(Clone, Debug)]
pub struct Page {
    /// The columns of the previewed relation.
    pub columns: Vec<ColumnInfo>,
    /// The page's rows (raw engine values; the preview formatter renders
    /// them).
    pub rows: Vec<Vec<Value>>,
}

/// Test-only injection point (hidden from the docs): when set, every
/// statement the console runs calls the hook with the statement text, on the
/// thread that runs it. The console's tests use it to prove that the
/// engine runs on the **blocking pool** (never on an async runtime thread)
/// and that the pool's size is the configured bound.
#[doc(hidden)]
pub static ENGINE_HOOK: std::sync::Mutex<Option<EngineHook>> = std::sync::Mutex::new(None);

/// The hook's type: a boxed closure the tests install.
#[doc(hidden)]
pub type EngineHook = Box<dyn Fn(&str) + Send + Sync>;

/// Run the hidden engine hook, if one is installed.
pub fn run_engine_hook(sql: &str) {
    // The lock is held while the hook runs: the hook must not install
    // itself (that would deadlock), and the hook is a test seam only.
    let guard = ENGINE_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(hook) = guard.as_ref() {
        hook(sql);
    }
}

/// Quote an identifier for SQL: double quotes, with an embedded quote
/// doubled (the engine's own escaping — see `quote_ident` in the engine's
/// `rbac` module).
///
/// The console always quotes, even for names that would parse as bare
/// identifiers: a quoted name is unambiguous, and a name that *is* a
/// keyword (`"select"`) still resolves to the object.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A console session: one engine connection plus the database it browses.
///
/// A session is reused across requests (the server pools them), so the
/// session's database is remembered and `USE` is issued only when the
/// browsed database actually changes.
pub struct Session {
    conn: Connection,
    db: String,
}

impl Session {
    /// Open a session on `db` with the console's statement limits.
    pub fn new(db: &Database, limits: ExecLimits) -> Self {
        let mut conn = db.connect();
        conn.set_exec_limits(limits);
        let current = conn.current_db().to_string();
        Self { conn, db: current }
    }

    /// The database this session currently browses.
    pub fn current_db(&self) -> &str {
        &self.db
    }

    /// Verify a password through the engine's own path and adopt the user as
    /// this session's principal: every later statement of the session then
    /// runs under that user's grants (the engine's per-statement RBAC
    /// check).
    pub fn login(&mut self, user: &str, password: &str) -> Result<(), String> {
        self.conn
            .login(user, password)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    /// Forget the session's principal.
    ///
    /// The console calls this before a pooled session is handed to another
    /// request: a session must never keep a previous request's identity.
    pub fn logout(&mut self) {
        self.conn.logout();
    }

    /// Every database of the store.
    pub fn databases(&mut self) -> Result<Vec<String>, String> {
        let selected = selected(self.run("SHOW DATABASES")?)?;
        Ok(selected
            .rows
            .into_iter()
            .map(|row| row.first().map(text_of).unwrap_or_default())
            .collect())
    }

    /// The relations (tables and views) of `db`, with their columns.
    pub fn relations(&mut self, db: &str) -> Result<Vec<RelationInfo>, String> {
        self.ensure_db(db)?;
        let names = self.show_tables()?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let columns = self.describe(db, &name)?;
            out.push(RelationInfo { name, columns });
        }
        Ok(out)
    }

    /// The exact number of rows of `db`.`table`.
    pub fn row_count(&mut self, db: &str, table: &str) -> Result<u64, String> {
        let sql = format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote_ident(db),
            quote_ident(table)
        );
        let selected = selected(self.run(&sql)?)?;
        Ok(selected
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| match value {
                Value::Bigint(n) => u64::try_from(*n).ok(),
                Value::Integer(n) => u64::try_from(*n).ok(),
                _ => None,
            })
            .unwrap_or(0))
    }

    /// One page of rows of `db`.`table` (`limit` rows, skipping `offset`).
    pub fn rows(
        &mut self,
        db: &str,
        table: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Page, String> {
        let columns = self.describe(db, table)?;
        let sql = format!(
            "SELECT * FROM {}.{} LIMIT {limit} OFFSET {offset}",
            quote_ident(db),
            quote_ident(table)
        );
        let selected = selected(self.run(&sql)?)?;
        Ok(Page {
            columns,
            rows: selected.rows,
        })
    }

    /// Switch the session to `db` (a session statement; it changes no data).
    fn ensure_db(&mut self, db: &str) -> Result<(), String> {
        if self.db.eq_ignore_ascii_case(db) {
            return Ok(());
        }
        let sql = format!("USE {}", quote_ident(db));
        let results = self.run(&sql)?;
        match results.into_iter().next() {
            Some(QueryResult::Used { db }) => {
                self.db = db;
                Ok(())
            }
            _ => Err(format!("cannot switch to database {db:?}")),
        }
    }

    /// The relations of the session's current database.
    fn show_tables(&mut self) -> Result<Vec<String>, String> {
        let selected = selected(self.run("SHOW TABLES")?)?;
        Ok(selected
            .rows
            .into_iter()
            .map(|row| row.first().map(text_of).unwrap_or_default())
            .collect())
    }

    /// The columns of `db`.`table` (`DESCRIBE`).
    fn describe(&mut self, db: &str, table: &str) -> Result<Vec<ColumnInfo>, String> {
        let sql = format!("DESCRIBE {}.{}", quote_ident(db), quote_ident(table));
        let selected = selected(self.run(&sql)?)?;
        let mut out = Vec::with_capacity(selected.rows.len());
        for row in selected.rows {
            let name = row.first().map(text_of).unwrap_or_default();
            let dtype = row.get(1).map(text_of).unwrap_or_default();
            // `DESCRIBE` reports `null` as `NO` for a NOT NULL / PRIMARY KEY
            // column and `YES` otherwise.
            let nullable = row
                .get(2)
                .map(text_of)
                .map(|flag| flag.eq_ignore_ascii_case("YES"))
                .unwrap_or(true);
            out.push(ColumnInfo {
                name,
                dtype,
                nullable,
            });
        }
        Ok(out)
    }

    /// Run one statement, mapping an engine error to a message.
    fn run(&mut self, sql: &str) -> Result<Vec<QueryResult>, String> {
        run_engine_hook(sql);
        self.conn.execute(sql).map_err(|err| err.to_string())
    }
}

/// The rows of one `SELECT`.
struct Selected {
    #[allow(dead_code)]
    names: Vec<String>,
    #[allow(dead_code)]
    types: Vec<DataType>,
    rows: Vec<Vec<Value>>,
}

/// Take the rows of the first result of a statement.
fn selected(results: Vec<QueryResult>) -> Result<Selected, String> {
    match results.into_iter().next() {
        Some(QueryResult::Selected { names, types, rows }) => Ok(Selected { names, types, rows }),
        Some(_) => Err("statement did not return rows".to_string()),
        None => Err("statement returned no result".to_string()),
    }
}

/// The text of a value (the catalog columns are VARCHARs).
fn text_of(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_ident("t"), "\"t\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quote_ident(""), "\"\"");
    }
}
