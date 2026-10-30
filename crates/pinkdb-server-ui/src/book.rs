//! Worksheet and history storage (wave 121), in the host's state store.
//!
//! The console keeps its own state in the `pinkdb_worksheets` database of
//! the **state store** (wave 121e), a store separate from the warehouse: no
//! statement a user runs can list, read or drop it.
//!
//! ```sql
//! worksheets(id, owner, title, sql, created_at, updated_at, shared_with)
//! history(id, owner, worksheet_id, sql, started_at, elapsed_ms, status, row_count, error)
//! ```
//!
//! These statements are the console's **bookkeeping**: they run on a trusted
//! connection, with every value bound as a parameter (never spliced into the
//! text), and the console — not RBAC — decides whose rows a request sees: a
//! worksheet is visible to its owner and to the users its `shared_with`
//! names (`*` for everyone), and only its owner may change or delete it.
//! Timestamps are bound values too, so a WAL replay of the bookkeeping
//! reproduces the same rows.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pinkdb::{Connection, Database, ExecLimits, QueryResult, Value};
pub use pinkdb_server_ui_api::{HistoryEntry, Worksheet};

/// The console's database.
pub const DATABASE: &str = "pinkdb_worksheets";

/// The longest SQL text a history row keeps.
const HISTORY_SQL_CHARS: usize = 16 * 1024;

/// How many history rows one request returns.
pub const HISTORY_ROWS: usize = 200;

/// A finished run, for [`Book::record_run`].
pub struct RunRecord<'a> {
    pub owner: &'a str,
    pub worksheet_id: &'a str,
    pub sql: &'a str,
    pub started: SystemTime,
    pub elapsed: Duration,
    pub status: &'a str,
    pub rows: u64,
    pub error: &'a str,
}

/// The bookkeeping store.
pub struct Book {
    db: Database,
    ready: AtomicBool,
}

impl Book {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            ready: AtomicBool::new(false),
        }
    }

    /// A trusted connection with a short deadline.
    fn conn(&self) -> Connection {
        let mut conn = self.db.connect();
        conn.set_exec_limits(ExecLimits {
            statement_timeout: Some(Duration::from_secs(10)),
            max_result_rows: Some(100_000),
            max_result_bytes: Some(64 * 1024 * 1024),
        });
        conn
    }

    /// Create the database and tables once.
    fn ensure(&self, conn: &mut Connection) -> Result<(), String> {
        if self.ready.load(Ordering::Acquire) {
            return Ok(());
        }
        conn.execute(&format!(
            "CREATE DATABASE IF NOT EXISTS {DATABASE}; \
             CREATE TABLE IF NOT EXISTS {DATABASE}.worksheets (id VARCHAR, owner VARCHAR, \
             title VARCHAR, sql VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, \
             shared_with VARCHAR); \
             CREATE TABLE IF NOT EXISTS {DATABASE}.history (id VARCHAR, owner VARCHAR, \
             worksheet_id VARCHAR, sql VARCHAR, started_at TIMESTAMP, elapsed_ms BIGINT, \
             status VARCHAR, row_count BIGINT, error VARCHAR);"
        ))
        .map_err(|e| format!("cannot create the worksheet store: {e}"))?;
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Run one parameterized statement.
    fn run(&self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut conn = self.conn();
        self.ensure(&mut conn)?;
        let stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        conn.execute_prepared(&stmt, params)
            .map_err(|e| e.to_string())
    }

    /// The worksheets `user` may see, newest first.
    pub fn list(&self, user: &str) -> Result<Vec<Worksheet>, String> {
        let result = self.run(
            &format!(
                "SELECT id, owner, title, sql, created_at, updated_at, shared_with \
                 FROM {DATABASE}.worksheets ORDER BY updated_at DESC"
            ),
            &[],
        )?;
        Ok(rows(result)
            .into_iter()
            .map(|row| worksheet(&row, user))
            .filter(|sheet| sheet.mine || visible(&sheet.shared_with, user))
            .collect())
    }

    /// One worksheet, if `user` may see it.
    pub fn get(&self, user: &str, id: &str) -> Result<Option<Worksheet>, String> {
        let result = self.run(
            &format!(
                "SELECT id, owner, title, sql, created_at, updated_at, shared_with \
                 FROM {DATABASE}.worksheets WHERE id = ?"
            ),
            &[text(id)],
        )?;
        Ok(rows(result)
            .into_iter()
            .map(|row| worksheet(&row, user))
            .find(|sheet| sheet.mine || visible(&sheet.shared_with, user)))
    }

    /// Create a worksheet owned by `user`.
    pub fn create(
        &self,
        user: &str,
        title: &str,
        sql: &str,
        shared_with: &str,
    ) -> Result<Worksheet, String> {
        let id = crate::security::random_token()[..16].to_string();
        let now = now_value();
        self.run(
            &format!("INSERT INTO {DATABASE}.worksheets VALUES (?, ?, ?, ?, ?, ?, ?)"),
            &[
                text(&id),
                text(user),
                text(title),
                text(sql),
                now.clone(),
                now,
                text(&normalize_shared(shared_with)),
            ],
        )?;
        self.get(user, &id)?
            .ok_or_else(|| "the new worksheet was not stored".to_string())
    }

    /// Change a worksheet; only its owner may. `Ok(None)` when it does not
    /// exist or is not the caller's.
    pub fn update(
        &self,
        user: &str,
        id: &str,
        title: &str,
        sql: &str,
        shared_with: &str,
    ) -> Result<Option<Worksheet>, String> {
        match self.get(user, id)? {
            Some(sheet) if sheet.mine => {}
            _ => return Ok(None),
        }
        self.run(
            &format!(
                "UPDATE {DATABASE}.worksheets SET title = ?, sql = ?, updated_at = ?, \
                 shared_with = ? WHERE id = ? AND owner = ?"
            ),
            &[
                text(title),
                text(sql),
                now_value(),
                text(&normalize_shared(shared_with)),
                text(id),
                text(user),
            ],
        )?;
        self.get(user, id)
    }

    /// Delete a worksheet; only its owner may. `false` when nothing was
    /// deleted.
    pub fn delete(&self, user: &str, id: &str) -> Result<bool, String> {
        let result = self.run(
            &format!("DELETE FROM {DATABASE}.worksheets WHERE id = ? AND owner = ?"),
            &[text(id), text(user)],
        )?;
        Ok(matches!(result, QueryResult::Deleted { count } if count > 0))
    }

    /// Add a run to `owner`'s history (best effort: a store that refuses the
    /// write — a read-only replica, say — does not fail the run).
    pub fn record_run(&self, run: &RunRecord<'_>) {
        let sql: String = run.sql.chars().take(HISTORY_SQL_CHARS).collect();
        let error: String = run.error.chars().take(4096).collect();
        let _ = self.run(
            &format!("INSERT INTO {DATABASE}.history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            &[
                text(&crate::security::random_token()[..16]),
                text(run.owner),
                text(run.worksheet_id),
                text(&sql),
                time_value(run.started),
                Value::Bigint(i64::try_from(run.elapsed.as_millis()).unwrap_or(i64::MAX)),
                text(run.status),
                Value::Bigint(i64::try_from(run.rows).unwrap_or(i64::MAX)),
                text(&error),
            ],
        );
    }

    /// `owner`'s latest runs, newest first.
    pub fn history(&self, owner: &str) -> Result<Vec<HistoryEntry>, String> {
        let result = self.run(
            &format!(
                "SELECT id, worksheet_id, sql, started_at, elapsed_ms, status, row_count, error \
                 FROM {DATABASE}.history WHERE owner = ? ORDER BY started_at DESC \
                 LIMIT {HISTORY_ROWS}"
            ),
            &[text(owner)],
        )?;
        Ok(rows(result)
            .into_iter()
            .map(|row| HistoryEntry {
                id: cell(&row, 0),
                worksheet_id: cell(&row, 1),
                sql: cell(&row, 2),
                started_at: cell(&row, 3),
                elapsed_ms: int(&row, 4),
                status: cell(&row, 5),
                rows: int(&row, 6),
                error: cell(&row, 7),
            })
            .collect())
    }
}

/// Is a worksheet shared with `user`?
fn visible(shared_with: &str, user: &str) -> bool {
    shared_with
        .split(',')
        .map(str::trim)
        .any(|name| name == "*" || name.eq_ignore_ascii_case(user))
}

/// `a, B ,c` → `a,b,c` (names are case-insensitive, like the catalog's).
fn normalize_shared(shared_with: &str) -> String {
    shared_with
        .split(',')
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

fn worksheet(row: &[Value], user: &str) -> Worksheet {
    let owner = cell(row, 1);
    Worksheet {
        id: cell(row, 0),
        mine: owner.eq_ignore_ascii_case(user),
        owner,
        title: cell(row, 2),
        sql: cell(row, 3),
        created_at: cell(row, 4),
        updated_at: cell(row, 5),
        shared_with: cell(row, 6),
    }
}

fn rows(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Selected { rows, .. } => rows,
        _ => Vec::new(),
    }
}

fn cell(row: &[Value], i: usize) -> String {
    match row.get(i) {
        None | Some(Value::Null) => String::new(),
        Some(Value::Varchar(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn int(row: &[Value], i: usize) -> i64 {
    match row.get(i) {
        Some(Value::Bigint(n)) => *n,
        Some(Value::Integer(n)) => i64::from(*n),
        _ => 0,
    }
}

fn text(s: &str) -> Value {
    Value::Varchar(s.to_string())
}

fn time_value(at: SystemTime) -> Value {
    let micros = at
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    Value::Timestamp(micros)
}

fn now_value() -> Value {
    time_value(SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sharing_is_by_name_or_star() {
        assert!(visible("a,b", "B"));
        assert!(visible("*", "anyone"));
        assert!(!visible("", "a"));
        assert!(!visible("ab", "a"));
        assert_eq!(normalize_shared(" A, ,b "), "a,b");
    }

    #[test]
    fn worksheets_round_trip_and_stay_private() {
        let book = Book::new(Database::new());
        let sheet = book
            .create("alice", "it's mine", "SELECT 'x''y'", "")
            .unwrap();
        assert_eq!(sheet.title, "it's mine");
        assert_eq!(sheet.sql, "SELECT 'x''y'");
        assert!(sheet.mine);
        assert!(book.list("bob").unwrap().is_empty());
        assert!(book.get("bob", &sheet.id).unwrap().is_none());
        assert!(
            book.update("bob", &sheet.id, "t", "s", "")
                .unwrap()
                .is_none()
        );
        assert!(!book.delete("bob", &sheet.id).unwrap());
        let shared = book
            .update("alice", &sheet.id, "t", "SELECT 2", "Bob")
            .unwrap()
            .unwrap();
        assert_eq!(shared.shared_with, "bob");
        let seen = book.list("bob").unwrap();
        assert_eq!(seen.len(), 1);
        assert!(!seen[0].mine);
        assert!(book.delete("alice", &sheet.id).unwrap());
        assert!(book.list("alice").unwrap().is_empty());
    }

    #[test]
    fn history_is_per_owner() {
        let book = Book::new(Database::new());
        for owner in ["alice", "bob"] {
            book.record_run(&RunRecord {
                owner,
                worksheet_id: "w",
                sql: "SELECT 1",
                started: SystemTime::now(),
                elapsed: Duration::from_millis(3),
                status: "ok",
                rows: 1,
                error: "",
            });
        }
        let alice = book.history("alice").unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0].sql, "SELECT 1");
        assert_eq!(alice[0].elapsed_ms, 3);
    }
}
