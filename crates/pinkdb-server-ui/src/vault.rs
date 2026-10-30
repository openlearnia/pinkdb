//! The console's credential store (wave 121d), next to the worksheets in
//! the `pinkdb_worksheets` database of the host's state store (wave 121e):
//!
//! ```sql
//! access_tokens(id, owner, name, token_hash, hint, created_at, expires_at, last_used_at)
//! totp(owner, secret, enabled_at, last_step)
//! recovery_codes(owner, code_hash, used_at)
//! ```
//!
//! Times are Unix seconds (`expires_at` 0 = never, `used_at` 0 = unused).
//! Tokens and recovery codes are kept as their SHA-256 only; a TOTP secret
//! must be readable to check a code, so it is kept as is — in the state
//! store, which no statement on the warehouse can reach (not even an
//! administrator's). Like the worksheet store, every statement runs on a trusted
//! connection with bound parameters, and the console decides whose rows a
//! request touches. The checks that must hold once — a TOTP step, a
//! recovery code — are single `UPDATE`s whose row count says whether this
//! request won.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pinkdb::{Connection, Database, ExecLimits, QueryResult, Value};

use crate::book::DATABASE;
use crate::factors::{new_recovery_codes, new_token, normalize_code, secret_hash, token_hint};

/// The most access tokens one user may hold.
pub const MAX_TOKENS: usize = 50;

/// How often a token's last-use time is written (a script that calls the API
/// in a loop must not write a row per request).
const LAST_USED_EVERY: u64 = 60;

/// A personal access token, without its secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenRecord {
    pub id: String,
    pub name: String,
    pub hint: String,
    pub created_at: u64,
    /// 0 = never.
    pub expires_at: u64,
    /// 0 = never used.
    pub last_used_at: u64,
}

/// Unix seconds now.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn int(row: &[Value], i: usize) -> u64 {
    match row.get(i) {
        Some(Value::Bigint(n)) => u64::try_from(*n).unwrap_or(0),
        Some(Value::Integer(n)) => u64::try_from(*n).unwrap_or(0),
        _ => 0,
    }
}

fn cell(row: &[Value], i: usize) -> String {
    match row.get(i) {
        None | Some(Value::Null) => String::new(),
        Some(Value::Varchar(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn text(s: &str) -> Value {
    Value::Varchar(s.to_string())
}

fn big(n: u64) -> Value {
    Value::Bigint(i64::try_from(n).unwrap_or(i64::MAX))
}

/// The credential store.
pub struct Vault {
    db: Database,
    ready: AtomicBool,
}

impl Vault {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            ready: AtomicBool::new(false),
        }
    }

    fn conn(&self) -> Connection {
        let mut conn = self.db.connect();
        conn.set_exec_limits(ExecLimits {
            statement_timeout: Some(Duration::from_secs(10)),
            max_result_rows: Some(100_000),
            max_result_bytes: Some(16 * 1024 * 1024),
        });
        conn
    }

    fn ensure(&self, conn: &mut Connection) -> Result<(), String> {
        if self.ready.load(Ordering::Acquire) {
            return Ok(());
        }
        conn.execute(&format!(
            "CREATE DATABASE IF NOT EXISTS {DATABASE}; \
             CREATE TABLE IF NOT EXISTS {DATABASE}.access_tokens (id VARCHAR, owner VARCHAR, \
             name VARCHAR, token_hash VARCHAR, hint VARCHAR, created_at BIGINT, \
             expires_at BIGINT, last_used_at BIGINT); \
             CREATE TABLE IF NOT EXISTS {DATABASE}.totp (owner VARCHAR, secret VARCHAR, \
             enabled_at BIGINT, last_step BIGINT); \
             CREATE TABLE IF NOT EXISTS {DATABASE}.recovery_codes (owner VARCHAR, \
             code_hash VARCHAR, used_at BIGINT);"
        ))
        .map_err(|e| format!("cannot create the credential store: {e}"))?;
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    fn run(&self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut conn = self.conn();
        self.ensure(&mut conn)?;
        crate::store::run_engine_hook(sql);
        let stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        conn.execute_prepared(&stmt, params)
            .map_err(|e| e.to_string())
    }

    fn select(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, String> {
        Ok(match self.run(sql, params)? {
            QueryResult::Selected { rows, .. } => rows,
            _ => Vec::new(),
        })
    }

    fn changed(&self, sql: &str, params: &[Value]) -> Result<usize, String> {
        Ok(match self.run(sql, params)? {
            QueryResult::Updated { count }
            | QueryResult::Deleted { count }
            | QueryResult::Inserted { count } => count,
            _ => 0,
        })
    }

    // ---------- personal access tokens ----------

    /// A user's tokens, newest first.
    pub fn tokens(&self, owner: &str) -> Result<Vec<TokenRecord>, String> {
        Ok(self
            .select(
                &format!(
                    "SELECT id, name, hint, created_at, expires_at, last_used_at \
                     FROM {DATABASE}.access_tokens WHERE owner = ? ORDER BY created_at DESC"
                ),
                &[text(owner)],
            )?
            .into_iter()
            .map(|r| TokenRecord {
                id: cell(&r, 0),
                name: cell(&r, 1),
                hint: cell(&r, 2),
                created_at: int(&r, 3),
                expires_at: int(&r, 4),
                last_used_at: int(&r, 5),
            })
            .collect())
    }

    /// Create a token; the secret is returned once and never stored.
    pub fn create_token(
        &self,
        owner: &str,
        name: &str,
        expires_at: u64,
    ) -> Result<(TokenRecord, String), String> {
        if self.tokens(owner)?.len() >= MAX_TOKENS {
            return Err(format!(
                "a user may hold {MAX_TOKENS} access tokens; revoke one first"
            ));
        }
        let secret = new_token();
        let record = TokenRecord {
            id: crate::security::random_token().chars().take(16).collect(),
            name: name.to_string(),
            hint: token_hint(&secret),
            created_at: now_secs(),
            expires_at,
            last_used_at: 0,
        };
        self.run(
            &format!("INSERT INTO {DATABASE}.access_tokens VALUES (?, ?, ?, ?, ?, ?, ?, 0)"),
            &[
                text(&record.id),
                text(owner),
                text(name),
                text(&secret_hash(&secret)),
                text(&record.hint),
                big(record.created_at),
                big(expires_at),
            ],
        )?;
        Ok((record, secret))
    }

    /// Revoke one of a user's tokens.
    pub fn revoke_token(&self, owner: &str, id: &str) -> Result<bool, String> {
        Ok(self.changed(
            &format!("DELETE FROM {DATABASE}.access_tokens WHERE owner = ? AND id = ?"),
            &[text(owner), text(id)],
        )? > 0)
    }

    /// The owner of a live (unexpired) token; refreshes its last use.
    pub fn token_owner(&self, token: &str) -> Result<Option<String>, String> {
        let rows = self.select(
            &format!(
                "SELECT id, owner, expires_at, last_used_at FROM {DATABASE}.access_tokens \
                 WHERE token_hash = ?"
            ),
            &[text(&secret_hash(token))],
        )?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let now = now_secs();
        let expires = int(row, 2);
        if expires != 0 && expires <= now {
            return Ok(None);
        }
        if now.saturating_sub(int(row, 3)) >= LAST_USED_EVERY {
            self.run(
                &format!("UPDATE {DATABASE}.access_tokens SET last_used_at = ? WHERE id = ?"),
                &[big(now), text(&cell(row, 0))],
            )?;
        }
        Ok(Some(cell(row, 1)))
    }

    // ---------- TOTP ----------

    /// A user's enabled TOTP secret and the last step it accepted.
    pub fn totp(&self, owner: &str) -> Result<Option<(String, Option<u64>)>, String> {
        let rows = self.select(
            &format!("SELECT secret, last_step FROM {DATABASE}.totp WHERE owner = ?"),
            &[text(owner)],
        )?;
        Ok(rows.first().map(|r| {
            let last = int(r, 1);
            (cell(r, 0), (last > 0).then_some(last))
        }))
    }

    /// The users with TOTP on.
    pub fn totp_users(&self) -> Result<HashSet<String>, String> {
        Ok(self
            .select(&format!("SELECT owner FROM {DATABASE}.totp"), &[])?
            .iter()
            .map(|r| cell(r, 0))
            .collect())
    }

    /// Turn TOTP on with a confirmed secret (the confirming code's step is
    /// spent) and return new recovery codes (shown once).
    pub fn enable_totp(&self, owner: &str, secret: &str, step: u64) -> Result<Vec<String>, String> {
        self.disable_totp(owner)?;
        self.run(
            &format!("INSERT INTO {DATABASE}.totp VALUES (?, ?, ?, ?)"),
            &[text(owner), text(secret), big(now_secs()), big(step)],
        )?;
        self.new_recovery_codes(owner)
    }

    /// Replace a user's recovery codes; returns the new ones (shown once).
    pub fn new_recovery_codes(&self, owner: &str) -> Result<Vec<String>, String> {
        self.run(
            &format!("DELETE FROM {DATABASE}.recovery_codes WHERE owner = ?"),
            &[text(owner)],
        )?;
        let codes = new_recovery_codes();
        for code in &codes {
            self.run(
                &format!("INSERT INTO {DATABASE}.recovery_codes VALUES (?, ?, 0)"),
                &[text(owner), text(&secret_hash(&normalize_code(code)))],
            )?;
        }
        Ok(codes)
    }

    /// Turn TOTP off and drop the recovery codes.
    pub fn disable_totp(&self, owner: &str) -> Result<bool, String> {
        let removed = self.changed(
            &format!("DELETE FROM {DATABASE}.totp WHERE owner = ?"),
            &[text(owner)],
        )?;
        self.run(
            &format!("DELETE FROM {DATABASE}.recovery_codes WHERE owner = ?"),
            &[text(owner)],
        )?;
        Ok(removed > 0)
    }

    /// Spend a TOTP step: `true` only for the one request that moves the
    /// user's last step forward to `step`.
    pub fn spend_step(&self, owner: &str, step: u64) -> Result<bool, String> {
        Ok(self.changed(
            &format!("UPDATE {DATABASE}.totp SET last_step = ? WHERE owner = ? AND last_step < ?"),
            &[big(step), text(owner), big(step)],
        )? == 1)
    }

    /// Spend a recovery code: `true` only once per code.
    pub fn use_recovery_code(&self, owner: &str, code: &str) -> Result<bool, String> {
        Ok(self.changed(
            &format!(
                "UPDATE {DATABASE}.recovery_codes SET used_at = ? \
                 WHERE owner = ? AND code_hash = ? AND used_at = 0"
            ),
            &[
                big(now_secs().max(1)),
                text(owner),
                text(&secret_hash(&normalize_code(code))),
            ],
        )? == 1)
    }

    /// How many unused recovery codes a user has.
    pub fn recovery_left(&self, owner: &str) -> Result<usize, String> {
        Ok(self
            .select(
                &format!(
                    "SELECT count(*) FROM {DATABASE}.recovery_codes WHERE owner = ? AND used_at = 0"
                ),
                &[text(owner)],
            )?
            .first()
            .map_or(0, |r| int(r, 0) as usize))
    }

    /// Forget the credentials of every owner that is not one of `users`
    /// (lower-case names): a dropped user's tokens and second factor must not
    /// pass to a later user of the same name. Returns how many owners went.
    pub fn sweep(&self, users: &[String]) -> Result<usize, String> {
        // Read-only until there is something to forget: a sweep must never
        // be the statement that creates the tables (a write that would queue
        // behind a long query and hold up every request behind it).
        let mut owners: HashSet<String> = HashSet::new();
        let mut conn = self.conn();
        for table in ["access_tokens", "totp", "recovery_codes"] {
            let sql = format!("SELECT DISTINCT owner FROM {DATABASE}.{table}");
            crate::store::run_engine_hook(&sql);
            match conn.execute(&sql).map(|mut r| r.pop()) {
                Ok(Some(QueryResult::Selected { rows, .. })) => {
                    owners.extend(rows.iter().map(|r| cell(r, 0)));
                }
                // No credential store yet: nothing to sweep.
                Ok(_) | Err(_) => return Ok(0),
            }
        }
        let mut gone = 0;
        for owner in owners {
            if !users.iter().any(|u| u.eq_ignore_ascii_case(&owner)) {
                self.forget(&owner)?;
                gone += 1;
            }
        }
        Ok(gone)
    }

    /// Forget every credential of a user (a dropped user).
    pub fn forget(&self, owner: &str) -> Result<(), String> {
        self.disable_totp(owner)?;
        self.run(
            &format!("DELETE FROM {DATABASE}.access_tokens WHERE owner = ?"),
            &[text(owner)],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factors::{new_totp_secret, totp_at};

    #[test]
    fn tokens_resolve_until_revoked_or_expired() {
        let vault = Vault::new(Database::new());
        let (record, secret) = vault.create_token("ana", "ci", 0).unwrap();
        assert_eq!(vault.token_owner(&secret).unwrap().as_deref(), Some("ana"));
        assert_eq!(vault.token_owner("pdbt_wrong").unwrap(), None);
        assert_eq!(vault.tokens("ana").unwrap().len(), 1);
        assert!(vault.tokens("bob").unwrap().is_empty());
        assert!(!vault.revoke_token("bob", &record.id).unwrap(), "not bob's");
        assert!(vault.revoke_token("ana", &record.id).unwrap());
        assert_eq!(vault.token_owner(&secret).unwrap(), None);
        let (_, old) = vault.create_token("ana", "old", 1).unwrap();
        assert_eq!(vault.token_owner(&old).unwrap(), None, "expired");
    }

    #[test]
    fn steps_and_recovery_codes_are_spent_once() {
        let vault = Vault::new(Database::new());
        let secret = new_totp_secret();
        assert!(totp_at(&secret, 1).is_some());
        let codes = vault.enable_totp("ana", &secret, 100).unwrap();
        assert_eq!(
            vault.totp("ana").unwrap(),
            Some((secret.clone(), Some(100)))
        );
        assert!(!vault.spend_step("ana", 100).unwrap(), "already spent");
        assert!(vault.spend_step("ana", 101).unwrap());
        assert!(!vault.spend_step("ana", 101).unwrap(), "replayed");
        assert_eq!(vault.recovery_left("ana").unwrap(), 10);
        assert!(
            vault
                .use_recovery_code("ana", &codes[0].to_lowercase())
                .unwrap()
        );
        assert!(!vault.use_recovery_code("ana", &codes[0]).unwrap(), "used");
        assert!(
            !vault.use_recovery_code("bob", &codes[1]).unwrap(),
            "not bob's"
        );
        assert_eq!(vault.recovery_left("ana").unwrap(), 9);
        assert!(vault.totp_users().unwrap().contains("ana"));
        vault.create_token("bob", "t", 0).unwrap();
        assert_eq!(vault.sweep(&["bob".to_string()]).unwrap(), 1, "ana is gone");
        assert_eq!(vault.totp("ana").unwrap(), None);
        assert_eq!(vault.tokens("bob").unwrap().len(), 1);
        vault.forget("ana").unwrap();
        assert_eq!(vault.totp("ana").unwrap(), None);
        assert_eq!(vault.recovery_left("ana").unwrap(), 0);
    }
}
