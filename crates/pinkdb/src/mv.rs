//! Materialized-view refresh policies (wave 80): manual completion, scheduled
//! (`REFRESH EVERY`) and commit-triggered (`REFRESH ON COMMIT`) recomputation,
//! daemonless.
//!
//! # Semantics
//!
//! A materialized view is a cached query result stored as a regular table plus
//! a catalog definition: the bound defining query, its text (persisted in
//! `.pink`), and a [`RefreshPolicy`]. Wave 43 shipped only the manual form;
//! this module adds the policies and the boundary enforcement.
//!
//! ```sql
//! CREATE MATERIALIZED VIEW v REFRESH EVERY INTERVAL '5 minutes' AS SELECT ...;
//! CREATE MATERIALIZED VIEW v REFRESH ON COMMIT AS SELECT ...;
//! CREATE MATERIALIZED VIEW v REFRESH MANUAL AS SELECT ...;   -- default
//! REFRESH MATERIALIZED VIEW v;      -- one view, reports errors
//! REFRESH MATERIALIZED VIEWS;       -- every view of the session database
//! SHOW MATERIALIZED VIEWS;          -- schedule, last refresh, staleness
//! ```
//!
//! The policy clause is written *before* `AS` (the position Postgres uses for
//! storage options): after the query the word `REFRESH` could be read as a
//! from-item alias (`FROM t refresh`).
//!
//! ## The freshness guarantee
//!
//! Enforcement is daemonless — no background threads, no timers. There is one
//! hook at the **statement boundary** of the embedded `PinkDb::execute` loop
//! and of every `Connection` statement (so `pinkdb-server` gets it for free),
//! and one hook where committed units record what they wrote:
//!
//! * `EVERY i`: a view is *due* once `i` elapsed since its last refresh
//!   **attempt** (success or failure — a broken view retries at most once per
//!   interval, never on every statement). The first statement boundary after
//!   the view became due recomputes it **before that statement binds or
//!   executes**: every statement issued after the due moment observes the
//!   refreshed table. A session that issues no further statement leaves the
//!   view unrecomputed — accepted and stated: there is no clock-driven work
//!   without a statement.
//!
//! * `ON COMMIT`: every committed unit (autocommit statement, `COMMIT`,
//!   replayed WAL record) marks the tables it wrote. The first statement
//!   boundary after such a commit recomputes every view whose base tables are
//!   dirty, before the next statement runs; writes to unrelated tables do not
//!   touch it. So the statement *following* the commit sees fresh data, but
//!   the refresh itself is a boundary action, not a step inside the commit —
//!   the commit statement's own row count/result is unaffected, and a session
//!   that stops after the write leaves the view stale until its next
//!   statement.
//!
//! * Neither policy ever fires **inside an open transaction**: `BEGIN`..`COMMIT`
//!   statements see the view as of the last boundary, and the pass resumes at
//!   the first boundary after `COMMIT`/`ROLLBACK`. Write MV statements
//!   (`CREATE`/`DROP`/`REFRESH`) are refused inside a transaction outright
//!   (`plan::refused_in_transaction`): the definition metadata does not ride
//!   the per-table transaction merge correctly, and deferred maintenance makes
//!   in-transaction refreshes meaningless.
//!
//! * Multiple handles on one durable store: every handle sees a view exactly
//!   as the owner's boundary pass logged it — a second process replays the
//!   `REFRESH` records through the ordinary WAL refresh protocol, so both
//!   handles agree on the data and the counters. `SHOW MATERIALIZED VIEWS`'s
//!   `stale` is a per-handle observation: it reports *this* handle's pending
//!   base-table writes and recorded errors, so a write another process has not
//!   yet refreshed (it has no statement boundary of its own yet) does not mark
//!   the view stale here.
//!
//! ## Determinism and the WAL
//!
//! Every automatic refresh is an explicit, WAL-logged statement
//! (`REFRESH MATERIALIZED VIEW <db>.<view>`); one boundary pass appends one
//! record with the batch, in deterministic order (database, then lowercased
//! view name, refresh-at-most-once per pass). Replay is not allowed to trigger
//! auto-refresh (`PinkDb::replaying` suppresses the pass): the logged
//! statements reproduce the live data and the refresh counters exactly, in the
//! same order. Refresh timestamps (`last_refreshed`, `last_attempt`) are
//! wall-clock values, not WAL payload: a refresh that replayed after an
//! unclean shutdown reports the replay moment, while one covered by a
//! checkpoint keeps its original timestamp. Data and counts never depend on
//! the clock.
//!
//! Chain semantics: a refreshed view counts as dirty for views built on it, so
//! one pass propagates through `v1 → v2` chains (each view refreshes at most
//! once per pass; a name-ordered sweep repeats until it makes no progress).
//!
//! ## Failures
//!
//! An automatic refresh failure never fails the user's statement and never
//! wedges the engine: the data stays as of the last successful refresh, the
//! failure is recorded in the definition (`last_error`, shown by `SHOW
//! MATERIALIZED VIEWS`), and the view retries on its next due boundary (or the
//! next commit touching its base tables). A manual `REFRESH` reports the raw
//! error instead and leaves `last_error` alone. Reading a stale view is always
//! allowed: it returns the last successfully computed rows. The usual cause is
//! a dropped/renamed base table; recreating it heals the view automatically
//! (the definition is rebound from its persisted SQL when needed), and
//! `DROP MATERIALIZED VIEW` always removes a broken view.

use std::collections::HashSet;

use crate::plan::LogicalPlan;
use crate::storage::{MaterializedViewDef, PinkDb};
use crate::types::{PinkDbError, RefreshPolicy, Result};
use crate::wal::FrameSets;

/// Wall-clock now, in milliseconds since the Unix epoch (saturating for
/// pre-epoch clocks).
pub(crate) fn now_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

/// The SQL text that logs (and re-runs) one view's refresh. Fully qualified and
/// fully quoted, so the record is session-independent and reparseable for any
/// view name.
pub(crate) fn refresh_sql(db: &str, name: &str) -> String {
    format!(
        "REFRESH MATERIALIZED VIEW {}.{}",
        quote_ident(db),
        quote_ident(name)
    )
}

/// A double-quoted SQL identifier (`"` doubled inside).
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// What one boundary pass did: the executed refresh statements, in execution
/// order, and the frame set to WAL-log them with (each view's base-table reads
/// plus its own backing table).
#[derive(Default)]
pub(crate) struct AutoRefresh {
    pub(crate) stmts: Vec<String>,
    pub(crate) sets: FrameSets,
}

/// One row of `SHOW MATERIALIZED VIEWS`.
pub(crate) struct MvStatus {
    pub(crate) name: String,
    pub(crate) schedule: String,
    pub(crate) last_refreshed: String,
    pub(crate) refresh_count: u64,
    pub(crate) stale: bool,
    pub(crate) last_error: Option<String>,
}

impl PinkDb {
    /// Does this store hold any materialized view at all? (Cheap gate for the
    /// per-statement boundary hook: MV-free stores pay one scan over the
    /// databases.)
    pub(crate) fn has_materialized_views(&self) -> bool {
        self.databases
            .values()
            .any(|c| !c.materialized_views.is_empty())
    }

    /// Every materialized view of the store as `(db key, lowercased name)`, in
    /// deterministic order: database key, then name. (Names only: the
    /// definitions carry a bound plan, so the boundary hot path never clones
    /// one.)
    fn all_materialized_views(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (db, cat) in &self.databases {
            for key in cat.materialized_views.keys() {
                out.push((db.clone(), key.clone()));
            }
        }
        out.sort();
        out
    }

    /// Record a committed unit's write set for the `ON COMMIT` trigger.
    /// Never called during WAL replay (replayed refreshes are logged
    /// statements; re-deriving triggers would double them).
    pub(crate) fn mark_mv_writes(&mut self, writes: &HashSet<(String, String)>) {
        if writes.is_empty() || self.replaying {
            return;
        }
        if !self.has_materialized_views() {
            return;
        }
        self.mv_pending.extend(writes.iter().cloned());
    }

    /// The `(db, table)` writes of a bound plan: the plan's own writes plus the
    /// backing tables a `REFRESH MATERIALIZED VIEWS` names only by database.
    /// `record_writes` already covers single-view statements.
    pub(crate) fn plan_writes_of(
        &self,
        plan: &LogicalPlan,
        default_db: &str,
    ) -> HashSet<(String, String)> {
        let mut out = HashSet::new();
        crate::db::record_writes(plan, default_db, &mut out, &mut HashSet::new());
        if let LogicalPlan::RefreshMaterializedViews { db } = plan {
            let target = db.as_deref().unwrap_or(default_db);
            for name in self.materialized_view_names_in(target) {
                out.insert((target.to_ascii_lowercase(), name.to_ascii_lowercase()));
            }
        }
        out
    }

    /// The `(db, table)` *reads* a materialized-view statement performs on top
    /// of what the plan itself names: a refresh re-evaluates the view's
    /// defining query, so it reads the query's base tables.
    pub(crate) fn mv_reads_of(
        &self,
        plan: &LogicalPlan,
        default_db: &str,
    ) -> Vec<(String, String)> {
        let mut out = Vec::new();
        match plan {
            LogicalPlan::RefreshMaterializedView { db, name } => {
                let target = db.as_deref().unwrap_or(default_db);
                out.extend(self.materialized_view_deps(target, name));
            }
            LogicalPlan::RefreshMaterializedViews { db } => {
                let target = db.as_deref().unwrap_or(default_db);
                for name in self.materialized_view_names_in(target) {
                    out.extend(self.materialized_view_deps(target, &name));
                }
            }
            _ => {}
        }
        out
    }

    /// Mark a committed statement's writes for the `ON COMMIT` trigger. A
    /// no-op for MV-free stores and for statements that write nothing (so
    /// ordinary workloads pay one `has_materialized_views` scan). Inside a
    /// transaction nothing is committed yet: `PinkDb::commit` moves the
    /// transaction's writes here.
    pub(crate) fn mark_committed_mv_writes(&mut self, plan: &LogicalPlan, default_db: &str) {
        if self.in_transaction() || !crate::plan::writes_tables(plan) {
            return;
        }
        if !self.has_materialized_views() {
            return;
        }
        let writes = self.plan_writes_of(plan, default_db);
        self.mark_mv_writes(&writes);
    }

    /// The plan's touched set for a WAL frame (wave 45o), store-aware: MV
    /// statements depend on objects the plan alone cannot name.
    pub(crate) fn plan_sets_store(&self, plan: &LogicalPlan, default_db: &str) -> FrameSets {
        let mut sets = FrameSets::default();
        crate::plan::plan_sets(plan, default_db, &mut sets);
        for (db, table) in self.mv_reads_of(plan, default_db) {
            sets.add_table(&db, &table);
        }
        if let LogicalPlan::RefreshMaterializedViews { db } = plan {
            let target = db.as_deref().unwrap_or(default_db);
            for name in self.materialized_view_names_in(target) {
                sets.add_table(target, &name);
            }
        }
        sets
    }

    /// Is a boundary pass worth running at `now`? (Cheap and non-mutating:
    /// used to avoid taking the write lease when nothing is due.)
    pub(crate) fn mv_boundary_has_work(&self, now: i64) -> bool {
        if !self.has_materialized_views() {
            return false;
        }
        for (db, key) in self.all_materialized_views() {
            let Ok(def) = self.materialized_view_in(&db, &key) else {
                continue;
            };
            match def.policy {
                RefreshPolicy::Manual => {}
                RefreshPolicy::Every { .. } => {
                    if def.is_due(now) {
                        return true;
                    }
                }
                RefreshPolicy::OnCommit => {
                    if !self.mv_pending.is_empty()
                        && !self
                            .materialized_view_deps(&db, &def.name)
                            .is_disjoint(&self.mv_pending)
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// The boundary pass itself (no WAL, no lease — callers wrap it).
    /// Recomputes, in deterministic order and at most once each, every `EVERY`
    /// view that is due and every `ON COMMIT` view whose base tables are
    /// dirty (a refreshed view dirties its own table for views built on it,
    /// repeated to a fixpoint). Per-view failures are recorded in the
    /// definition, never returned. The pending dirty set is consumed.
    pub(crate) fn refresh_due_auto_views(&mut self, now: i64) -> AutoRefresh {
        let mut report = AutoRefresh::default();
        let mut dirty: HashSet<(String, String)> = std::mem::take(&mut self.mv_pending);
        if !self.has_materialized_views() {
            return report;
        }
        let mut done: HashSet<(String, String)> = HashSet::new();
        loop {
            let mut progressed = false;
            for (db, key) in self.all_materialized_views() {
                if done.contains(&(db.clone(), key.clone())) {
                    continue;
                }
                let trigger = {
                    let Ok(def) = self.materialized_view_in(&db, &key) else {
                        continue;
                    };
                    match def.policy {
                        RefreshPolicy::Manual => false,
                        RefreshPolicy::Every { .. } => def.is_due(now),
                        RefreshPolicy::OnCommit => !self
                            .materialized_view_deps(&db, &def.name)
                            .is_disjoint(&dirty),
                    }
                };
                if !trigger {
                    continue;
                }
                done.insert((db.clone(), key.clone()));
                progressed = true;
                let name = key.clone();
                match crate::exec::refresh_materialized_view(self, &db, &name) {
                    Ok(()) => {
                        // The fresh table is itself a base table for views
                        // built on this one.
                        dirty.insert((db.to_ascii_lowercase(), key.clone()));
                        report.sets.add_table(&db, &name);
                        for (dep_db, dep_table) in self.materialized_view_deps(&db, &name) {
                            report.sets.add_table(&dep_db, &dep_table);
                        }
                        report.stmts.push(refresh_sql(&db, &name));
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.record_auto_refresh_error(&db, &name, now, &msg);
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        report
    }

    /// One statement-boundary pass for the embedded session: takes the durable
    /// lease when the store is durable, refreshes the store first (the WAL
    /// append below must land after every earlier commit), runs the pass, and
    /// logs the executed refreshes as one record.
    pub(crate) fn boundary_auto_refresh(&mut self) -> Result<()> {
        if !self.has_materialized_views() {
            // A store whose last views were dropped keeps no pending marks.
            self.mv_pending.clear();
            return Ok(());
        }
        let now = now_ms();
        if !self.mv_boundary_has_work(now) {
            self.mv_pending.clear();
            return Ok(());
        }
        if !self.is_durable() {
            let _ = self.refresh_due_auto_views(now);
            return Ok(());
        }
        let lease = self.acquire_lease()?;
        if let Some(g) = &lease {
            // Another process's commits may carry changed definitions.
            self.refresh_under_lease(g)?;
        }
        // The refresh may have brought in a newer catalog: decide again.
        if !self.mv_boundary_has_work(now) {
            self.mv_pending.clear();
        } else {
            let report = self.refresh_due_auto_views(now);
            if !report.stmts.is_empty() {
                self.log_auto_refresh(&report)?;
            }
        }
        self.maybe_auto_checkpoint(lease.as_ref());
        drop(lease);
        Ok(())
    }

    /// Append one boundary pass's refreshes as a single WAL record (the lease
    /// must be held by the caller for durable stores).
    pub(crate) fn log_auto_refresh(&self, report: &AutoRefresh) -> Result<()> {
        let db = self.current_db.clone();
        self.wal_append_session(&db, &db, &report.stmts, &[], &[], Some(&report.sets))
    }

    /// Mark a successful refresh: the wall-clock stamp, the attempt stamp and
    /// the counter, clearing any recorded error.
    pub(crate) fn mark_refreshed(&mut self, db: &str, name: &str, now: i64) -> Result<()> {
        let def = self.materialized_view_mut_in(db, name)?;
        def.last_refreshed_ms = now;
        def.last_attempt_ms = now;
        def.refresh_count = def.refresh_count.saturating_add(1);
        def.last_error = None;
        Ok(())
    }

    /// Record a failed *automatic* refresh without disturbing the data.
    fn record_auto_refresh_error(&mut self, db: &str, name: &str, now: i64, error: &str) {
        if let Ok(def) = self.materialized_view_mut_in(db, name) {
            def.last_attempt_ms = now;
            def.last_error = Some(error.to_string());
        }
    }

    /// The `SHOW MATERIALIZED VIEWS` rows of `db`, sorted by name.
    pub(crate) fn materialized_view_status_in(&self, db: &str, now: i64) -> Vec<MvStatus> {
        let mut out: Vec<MvStatus> = self
            .materialized_views_in(db)
            .into_iter()
            .map(|(_, def)| {
                let stale = def.last_error.is_some()
                    || match def.policy {
                        RefreshPolicy::Manual => false,
                        RefreshPolicy::Every { .. } => def.is_due(now),
                        RefreshPolicy::OnCommit => !self
                            .materialized_view_deps(db, &def.name)
                            .is_disjoint(&self.mv_pending),
                    };
                MvStatus {
                    name: def.name.clone(),
                    schedule: def.policy.to_string(),
                    last_refreshed: crate::types::format_timestamp(
                        def.last_refreshed_ms.saturating_mul(1000),
                    ),
                    refresh_count: def.refresh_count,
                    stale,
                    last_error: def.last_error.clone(),
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

impl MaterializedViewDef {
    /// Is this view due for an automatic refresh at `now`? An `EVERY` view
    /// measures from the last *attempt* (so a failing view backs off a full
    /// interval instead of paying a failing evaluation per statement).
    pub(crate) fn is_due(&self, now: i64) -> bool {
        let since = self.last_refreshed_ms.max(self.last_attempt_ms);
        let elapsed_us = (now.saturating_sub(since) as i128).saturating_mul(1000);
        self.policy.due(elapsed_us)
    }
}

/// Rebind a materialized view's defining statement text (the persisted
/// `CREATE MATERIALIZED VIEW ...`) against `db_name`, returning the defining
/// query. Used when the definition has no bound plan — a file whose base
/// tables were missing at load time — and by the loader itself.
pub(crate) fn rebind_query(
    db: &PinkDb,
    db_name: &str,
    query_sql: &str,
) -> Result<Box<LogicalPlan>> {
    let stmts = crate::sql::parse_statements(query_sql)?;
    match stmts.into_iter().next() {
        Some(crate::sql::ast::Statement::CreateMaterializedView { query, .. }) => {
            Ok(Box::new(crate::sql::bind_mv_query(db, db_name, &query)?))
        }
        _ => Err(PinkDbError::Catalog(format!(
            "the persisted definition of a materialized view is not a CREATE \
             MATERIALIZED VIEW statement: {query_sql}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use crate::wal::SyncPolicy;
    use crate::QueryResult;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Per-test scratch directory (removed on drop, panics included), so no
    /// `.pink`/`.wal`/lock artifact ever lands in the tree.
    struct Scratch {
        dir: PathBuf,
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "pinkdb_mv_{tag}_{}_{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self { dir }
        }

        fn path(&self) -> String {
            self.dir.join("db.pink").to_string_lossy().to_string()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn durable(s: &Scratch) -> PinkDb {
        PinkDb::open_durable_with(&s.path(), SyncPolicy::Batch)
            .unwrap()
            .0
    }

    fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows from {sql}, got {other:?}"),
        }
    }

    fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
        rows(db, sql)
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i as i64,
                Value::Bigint(i) => *i,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect()
    }

    /// One `SHOW MATERIALIZED VIEWS` row by name:
    /// (name, schedule, last_refreshed, refresh_count, stale, last_error).
    #[derive(Debug, PartialEq)]
    struct MvRow {
        schedule: String,
        last_refreshed: String,
        refresh_count: i64,
        stale: bool,
        last_error: Option<String>,
    }

    fn show(db: &mut PinkDb, name: &str) -> MvRow {
        for r in rows(db, "SHOW MATERIALIZED VIEWS;") {
            if r[0] == Value::Varchar(name.to_string()) {
                return MvRow {
                    schedule: match &r[1] {
                        Value::Varchar(s) => s.clone(),
                        other => panic!("schedule: {other:?}"),
                    },
                    last_refreshed: match &r[2] {
                        Value::Varchar(s) => s.clone(),
                        other => panic!("last_refreshed: {other:?}"),
                    },
                    refresh_count: match &r[3] {
                        Value::Bigint(i) => *i,
                        other => panic!("refresh_count: {other:?}"),
                    },
                    stale: match &r[4] {
                        Value::Boolean(b) => *b,
                        other => panic!("stale: {other:?}"),
                    },
                    last_error: match &r[5] {
                        Value::Varchar(e) => Some(e.clone()),
                        Value::Null => None,
                        other => panic!("last_error: {other:?}"),
                    },
                };
            }
        }
        panic!("no materialized view named {name}");
    }

    fn c_ints(c: &mut crate::Connection, sql: &str) -> Vec<i64> {
        match c.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i as i64,
                    Value::Bigint(i) => *i,
                    other => panic!("expected an integer, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// The definition's refresh counter, read without running a statement (a
    /// SQL read would itself cross a boundary).
    fn count(db: &PinkDb, name: &str) -> i64 {
        db.materialized_view_in("main", name).unwrap().refresh_count as i64
    }

    fn seeded() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y');")
            .unwrap();
        db
    }

    /// Manual `REFRESH`: contents and the refresh stamps move; the count is
    /// exactly one successful refresh.
    #[test]
    fn manual_refresh_updates_contents_and_stamps() {
        let mut db = seeded();
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT a FROM t;")
            .unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2]);
        let before = show(&mut db, "mv");
        assert_eq!(before.schedule, "manual");
        assert_eq!(before.refresh_count, 0, "creation is not a refresh");
        assert!(!before.stale);
        db.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        // Manual views never refresh themselves.
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2]);
        std::thread::sleep(std::time::Duration::from_millis(5));
        db.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2, 3]);
        let after = show(&mut db, "mv");
        assert_eq!(after.refresh_count, 1);
        assert!(
            after.last_refreshed > before.last_refreshed,
            "timestamps must move: {} -> {}",
            before.last_refreshed,
            after.last_refreshed
        );
        // The `REFRESH <name>` shorthand works too.
        db.execute("REFRESH mv;").unwrap();
        assert_eq!(show(&mut db, "mv").refresh_count, 2);
        // REFRESH of an unknown view is a clear error.
        let err = db.execute("REFRESH MATERIALIZED VIEW nope;").unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    /// `REFRESH MATERIALIZED VIEWS` recomputes every view of the session
    /// database, all-or-nothing, and leaves other databases alone.
    #[test]
    fn refresh_all_views_is_per_database() {
        let mut db = seeded();
        db.execute("CREATE MATERIALIZED VIEW v1 AS SELECT a FROM t;")
            .unwrap();
        db.execute("CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) AS n FROM t;")
            .unwrap();
        db.execute("CREATE DATABASE other; USE other;").unwrap();
        db.execute("CREATE TABLE u (x INTEGER);").unwrap();
        db.execute("INSERT INTO u VALUES (7);").unwrap();
        db.execute("CREATE MATERIALIZED VIEW v3 AS SELECT x FROM u;")
            .unwrap();
        // Refresh everything in `other`: only its own view moves.
        db.execute("INSERT INTO u VALUES (8);").unwrap();
        db.execute("REFRESH MATERIALIZED VIEWS;").unwrap();
        assert_eq!(ints(&mut db, "SELECT x FROM v3;"), vec![7, 8]);
        assert_eq!(show(&mut db, "v3").refresh_count, 1);
        // `main`'s views are untouched by that session's refresh...
        db.execute("USE main;").unwrap();
        assert_eq!(show(&mut db, "v1").refresh_count, 0);
        assert_eq!(ints(&mut db, "SELECT a FROM v1;"), vec![1, 2]);
        // ... and refreshing `main` does not touch `other`'s view.
        db.execute("INSERT INTO t VALUES (9, 'w');").unwrap();
        db.execute("REFRESH MATERIALIZED VIEWS;").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM v1;"), vec![1, 2, 9]);
        assert_eq!(ints(&mut db, "SELECT n FROM v2;"), vec![3]);
        assert_eq!(show(&mut db, "v1").refresh_count, 1);
        assert_eq!(show(&mut db, "v2").refresh_count, 1);
        db.execute("USE other;").unwrap();
        assert_eq!(show(&mut db, "v3").refresh_count, 1, "still the first");
        // All-or-nothing: one broken view fails the whole statement and
        // nothing is written (the evaluation happens before any write).
        db.execute("USE main; DROP TABLE t;").unwrap();
        let err = db.execute("REFRESH MATERIALIZED VIEWS;").unwrap_err();
        assert!(err.to_string().contains("t"), "{err}");
        assert_eq!(ints(&mut db, "SELECT a FROM v1;"), vec![1, 2, 9]);
        assert_eq!(show(&mut db, "v1").refresh_count, 1, "nothing applied");
        // An empty database is a clean no-op.
        db.execute("CREATE DATABASE empty_db; USE empty_db;")
            .unwrap();
        assert_eq!(
            db.execute("REFRESH MATERIALIZED VIEWS;").unwrap().pop(),
            Some(QueryResult::RefreshedMaterializedViews { names: Vec::new() })
        );
    }

    /// An `EVERY` view refreshes at the first statement boundary after it is
    /// due, before that statement runs; an undue interval does nothing.
    #[test]
    fn every_due_refreshes_at_the_next_boundary() {
        let mut db = seeded();
        db.execute(
            "CREATE MATERIALIZED VIEW mv REFRESH EVERY INTERVAL '0 seconds' AS SELECT a FROM t;",
        )
        .unwrap();
        // Creation itself computed the data (a refresh count of zero); every
        // later statement boundary sees the view due (interval zero) and
        // recomputes *before* that statement runs.
        assert_eq!(count(&db, "mv"), 0, "creation is not a refresh");
        // The INSERT's own boundary refreshes: exactly one refresh.
        db.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(count(&db, "mv"), 1);
        // The next boundary refreshes again, before the SELECT runs — so the
        // row its own commit wrote is visible here.
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2, 3]);
        assert_eq!(count(&db, "mv"), 2);
        db.execute("SELECT 1;").unwrap();
        assert_eq!(count(&db, "mv"), 3, "every boundary refreshes");
        // A real interval is not due immediately: an hour-long view never
        // moves on its own.
        db.execute(
            "CREATE MATERIALIZED VIEW slow REFRESH EVERY INTERVAL '1 hour' AS SELECT a FROM t;",
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (4, 'w');").unwrap();
        db.execute("SELECT 1;").unwrap();
        assert_eq!(show(&mut db, "slow").refresh_count, 0);
        assert_eq!(ints(&mut db, "SELECT a FROM slow;"), vec![1, 2, 3], "stale");
        // ... and a short interval fires once it has really elapsed.
        db.execute(
            "CREATE MATERIALIZED VIEW timed REFRESH EVERY INTERVAL '80 milliseconds' AS SELECT a FROM t;",
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(140));
        // The boundary of this statement recomputes the view before it runs.
        assert_eq!(ints(&mut db, "SELECT a FROM timed;"), vec![1, 2, 3, 4]);
        assert_eq!(count(&db, "timed"), 1);
    }

    /// `ON COMMIT` refreshes after a commit that wrote a base table and not
    /// after unrelated writes; transactions defer to the boundary after
    /// COMMIT.
    #[test]
    fn on_commit_tracks_base_tables_and_defers_in_transactions() {
        let mut db = seeded();
        db.execute("CREATE TABLE u (x INTEGER);").unwrap();
        db.execute("INSERT INTO u VALUES (7);").unwrap();
        db.execute("CREATE MATERIALIZED VIEW mv REFRESH ON COMMIT AS SELECT a FROM t;")
            .unwrap();
        // A write to an unrelated table does not refresh.
        db.execute("INSERT INTO u VALUES (8);").unwrap();
        db.execute("SELECT 1;").unwrap();
        assert_eq!(show(&mut db, "mv").refresh_count, 0);
        // A write to the base table refreshes at the next boundary.
        db.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2, 3]);
        assert_eq!(show(&mut db, "mv").refresh_count, 1);
        // Inside a transaction the view stays as of the last boundary; the
        // refresh lands after COMMIT.
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'w');").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM mv;"),
            vec![1, 2, 3],
            "deferred"
        );
        assert_eq!(show(&mut db, "mv").refresh_count, 1);
        db.execute("COMMIT;").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2, 3, 4]);
        assert_eq!(show(&mut db, "mv").refresh_count, 2);
        // A rolled-back write triggers nothing.
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (5, 'q');").unwrap();
        db.execute("ROLLBACK;").unwrap();
        db.execute("SELECT 1;").unwrap();
        assert_eq!(show(&mut db, "mv").refresh_count, 2);
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2, 3, 4]);
        // DDL on a base table counts too: DROP TABLE makes the refresh fail
        // (recorded, never fatal).
        db.execute("DROP TABLE t;").unwrap();
        let row = show(&mut db, "mv");
        assert!(row.last_error.is_some(), "{row:?}");
        assert!(row.stale);
        // The data computed before the drop is still there.
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2, 3, 4]);
        // Recreating the base table heals the view automatically.
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (6, 'h');").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![6]);
        assert_eq!(show(&mut db, "mv").last_error, None);
        // `CREATE TABLE t` healed the view (empty) at the next boundary, and
        // the insert's dirty mark refreshed it again: two further successes
        // on top of the two before the drop.
        assert_eq!(show(&mut db, "mv").refresh_count, 4);
    }

    /// Chained views: one write to the base propagates through the chain in
    /// the same boundary pass.
    #[test]
    fn on_commit_propagates_through_view_chains() {
        let mut db = seeded();
        db.execute("CREATE MATERIALIZED VIEW v1 REFRESH ON COMMIT AS SELECT a FROM t;")
            .unwrap();
        db.execute(
            "CREATE MATERIALIZED VIEW v2 REFRESH ON COMMIT AS SELECT a * 10 AS a10 FROM v1;",
        )
        .unwrap();
        db.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(ints(&mut db, "SELECT a10 FROM v2;"), vec![10, 20, 30]);
        assert_eq!(show(&mut db, "v1").refresh_count, 1);
        assert_eq!(show(&mut db, "v2").refresh_count, 1);
    }

    /// A dropped base table: the automatic pass records the failure without
    /// failing the user's statement, manual `REFRESH` reports it, and the
    /// data (and count) stay as of the last success.
    #[test]
    fn dropped_base_table_is_reported_not_fatal() {
        let mut db = seeded();
        db.execute(
            "CREATE MATERIALIZED VIEW mv REFRESH EVERY INTERVAL '0 seconds' AS SELECT a FROM t;",
        )
        .unwrap();
        db.execute("DROP TABLE t;").unwrap();
        // The drop itself succeeded; the next boundary tried and recorded.
        let row = show(&mut db, "mv");
        assert!(row.stale);
        assert!(
            row.last_error.as_deref().unwrap_or("").contains("\"t\""),
            "{row:?}"
        );
        assert_eq!(row.refresh_count, 1, "the pre-drop boundary succeeded");
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2], "data kept");
        assert_eq!(count(&db, "mv"), 1, "the failing pass did not count");
        // Manual refresh reports the errojr directly.
        let err = db.execute("REFRESH MATERIALIZED VIEW mv;").unwrap_err();
        assert!(err.to_string().contains("\"t\""), "{err}");
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![1, 2]);
        // Recreating the table heals the view (the definition is rebound).
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (42);").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM mv;"), vec![42]);
        assert_eq!(show(&mut db, "mv").last_error, None);
    }

    /// Materialized-view statements are refused inside transactions (both the
    /// embedded and the connection flavor): the definition metadata does not
    /// ride the per-table transaction merge.
    #[test]
    fn mv_statements_are_refused_in_transactions() {
        let mut db = seeded();
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT a FROM t;")
            .unwrap();
        for (sql, what) in [
            (
                "BEGIN; REFRESH MATERIALIZED VIEW mv;",
                "REFRESH MATERIALIZED VIEW",
            ),
            (
                "BEGIN; REFRESH MATERIALIZED VIEWS;",
                "REFRESH MATERIALIZED VIEWS",
            ),
            (
                "BEGIN; CREATE MATERIALIZED VIEW m2 AS SELECT a FROM t;",
                "CREATE MATERIALIZED VIEW",
            ),
            (
                "BEGIN; DROP MATERIALIZED VIEW mv;",
                "DROP MATERIALIZED VIEW",
            ),
        ] {
            let err = db.execute(sql).unwrap_err().to_string();
            assert!(
                err.contains(what) && err.contains("not allowed"),
                "{sql}: {err}"
            );
            db.execute("ROLLBACK;").unwrap();
        }
        // A plain base-table write inside a transaction still works.
        db.execute("BEGIN; INSERT INTO t VALUES (3, 'z'); COMMIT;")
            .unwrap();
        // ... and the connection flavor refuses the same statements.
        let database = crate::Database::from_store(db);
        let mut c = database.connect();
        let err = c
            .execute("BEGIN; REFRESH MATERIALIZED VIEW mv;")
            .unwrap_err()
            .to_string();
        assert!(err.contains("REFRESH MATERIALIZED VIEW"), "{err}");
        c.execute("ROLLBACK;").unwrap();
    }

    /// `SHOW MATERIALIZED VIEWS` reports the schedule, stamps, staleness and
    /// the recorded error, sorted by name; only the session database's views.
    #[test]
    fn show_reports_schedule_stamps_and_staleness() {
        let mut db = seeded();
        db.execute("CREATE MATERIALIZED VIEW z_manual AS SELECT a FROM t;")
            .unwrap();
        db.execute("CREATE MATERIALIZED VIEW a_every REFRESH EVERY INTERVAL '5 minutes' AS SELECT a FROM t;")
            .unwrap();
        db.execute("CREATE MATERIALIZED VIEW m_commit REFRESH ON COMMIT AS SELECT a FROM t;")
            .unwrap();
        let names: Vec<String> = rows(&mut db, "SHOW MATERIALIZED VIEWS;")
            .into_iter()
            .map(|r| match &r[0] {
                Value::Varchar(n) => n.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(names, vec!["a_every", "m_commit", "z_manual"], "sorted");
        assert_eq!(show(&mut db, "a_every").schedule, "every 00:05:00");
        assert_eq!(show(&mut db, "m_commit").schedule, "on commit");
        assert_eq!(show(&mut db, "z_manual").schedule, "manual");
        // Staleness: an ON COMMIT view is dirty exactly between the commit
        // that wrote a base table and the next boundary pass (which is why
        // `SHOW` itself already sees it fresh — its own boundary ran first).
        // Inspect the store between statements to pin the mechanism.
        db.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(
            db.mv_pending,
            std::collections::HashSet::from([("main".to_string(), "t".to_string())])
        );
        let stale_now = db.materialized_view_status_in("main", crate::mv::now_ms());
        assert!(
            stale_now
                .iter()
                .find(|r| r.name == "m_commit")
                .unwrap()
                .stale
        );
        assert!(
            !stale_now
                .iter()
                .find(|r| r.name == "z_manual")
                .unwrap()
                .stale
        );
        assert!(
            !stale_now
                .iter()
                .find(|r| r.name == "a_every")
                .unwrap()
                .stale
        );
        db.execute("SELECT 1;").unwrap();
        assert!(db.mv_pending.is_empty(), "the pass consumed the dirty set");
        assert!(!show(&mut db, "m_commit").stale);
        assert_eq!(show(&mut db, "m_commit").refresh_count, 1);
        // A failing view stays stale and reports the error.
        db.execute("DROP TABLE t;").unwrap();
        let row = show(&mut db, "m_commit");
        assert!(row.stale);
        assert!(row.last_error.is_some());
        // Another database's views do not leak into `SHOW`.
        db.execute("CREATE DATABASE other; USE other;").unwrap();
        assert!(rows(&mut db, "SHOW MATERIALIZED VIEWS;").is_empty());
    }

    /// `DROP MATERIALIZED VIEW` removes the definition and the backing table
    /// (also when a `DROP TABLE` already removed the table), and the name is
    /// reusable.
    #[test]
    fn drop_cleans_definition_and_table() {
        let mut db = seeded();
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT a FROM t;")
            .unwrap();
        db.execute("DROP MATERIALIZED VIEW mv;").unwrap();
        assert!(rows(&mut db, "SHOW MATERIALIZED VIEWS;").is_empty());
        assert!(db.table_in("main", "mv").is_err());
        assert!(db.execute("SELECT * FROM mv;").is_err());
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT b FROM t;")
            .unwrap();
        assert_eq!(
            rows(&mut db, "SELECT b FROM mv;"),
            vec![
                vec![Value::Varchar("x".to_string())],
                vec![Value::Varchar("y".to_string())]
            ]
        );
        // A dangling definition (its table dropped) still drops cleanly.
        db.execute("DROP TABLE mv;").unwrap();
        assert!(db.materialized_view_in("main", "mv").is_ok());
        db.execute("DROP MATERIALIZED VIEW mv;").unwrap();
        assert!(db.materialized_view_in("main", "mv").is_err());
        // IF EXISTS tolerates a missing view.
        db.execute("DROP MATERIALIZED VIEW IF EXISTS mv;").unwrap();
        let err = db.execute("DROP MATERIALIZED VIEW mv;").unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }

    /// A durable store: automatic refreshes are logged as explicit `REFRESH`
    /// statements in the boundary record, so a reopen replays the data, the
    /// counters and the definition exactly — including through a checkpoint.
    #[test]
    fn durable_auto_refresh_replays_exactly() {
        let s = Scratch::new("replay");
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute(
            "CREATE MATERIALIZED VIEW every_mv REFRESH EVERY INTERVAL '0 seconds' AS SELECT a FROM t;",
        )
        .unwrap();
        db.execute("CREATE MATERIALIZED VIEW commit_mv REFRESH ON COMMIT AS SELECT a FROM t;")
            .unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        // The next statement's boundary refreshes both (every: due;
        // commit: dirty) and logs one record.
        assert_eq!(ints(&mut db, "SELECT a FROM every_mv;"), vec![1, 2]);
        assert_eq!(ints(&mut db, "SELECT a FROM commit_mv;"), vec![1, 2]);
        db.execute("INSERT INTO t VALUES (3);").unwrap();
        db.execute("SELECT 1;").unwrap();
        // A checkpoint folds the state (the backing tables *and* the
        // materialized-view section) into the `.pink` file and rolls the WAL.
        db.execute("CHECKPOINT;").unwrap();
        db.execute("INSERT INTO t VALUES (4);").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM commit_mv;"), vec![1, 2, 3, 4]);
        // Read the counters without running a statement: an `EVERY 0` view
        // refreshes at *every* boundary, including the ones the assertions
        // themselves would cross.
        let counts = |db: &PinkDb| -> Vec<i64> {
            ["every_mv", "commit_mv"]
                .iter()
                .map(|n| db.materialized_view_in("main", n).unwrap().refresh_count as i64)
                .collect()
        };
        let before = counts(&db);
        drop(db);
        // Reopen: the checkpoint carries the definition (and its stamps), the
        // WAL replays the refreshes that followed it. Data and counters must
        // land exactly where the live store was.
        let mut db = durable(&s);
        assert_eq!(counts(&db), before, "counts survive replay exactly");
        assert_eq!(ints(&mut db, "SELECT a FROM every_mv;"), vec![1, 2, 3, 4]);
        assert_eq!(ints(&mut db, "SELECT a FROM commit_mv;"), vec![1, 2, 3, 4]);
        // (Timestamps are wall-clock and deliberately not WAL payload: a
        // refresh that replayed after an unclean shutdown reports the replay
        // moment, one covered by a checkpoint keeps its stamp.)
        // The replayed definitions are live: a further write still refreshes.
        db.execute("INSERT INTO t VALUES (5);").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM commit_mv;"),
            vec![1, 2, 3, 4, 5]
        );
    }

    /// Two processes on one durable store: one's automatic refresh reaches the
    /// other through the ordinary WAL refresh protocol (no extra machinery).
    #[test]
    fn a_second_open_replays_the_auto_refreshes() {
        let s = Scratch::new("twoopens");
        let mut a = durable(&s);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        a.execute("INSERT INTO t VALUES (1);").unwrap();
        let mut b = durable(&s); // hmm: opening twice in one process shares the lease
        a.execute("CREATE MATERIALIZED VIEW mv REFRESH ON COMMIT AS SELECT a FROM t;")
            .unwrap();
        a.execute("INSERT INTO t VALUES (2);").unwrap();
        // a's next boundary refreshes its view and logs the `REFRESH`, so the
        // WAL now describes the fresh state...
        a.execute("SELECT 1;").unwrap();
        assert_eq!(ints(&mut a, "SELECT a FROM mv;"), vec![1, 2]);
        // ... and b's boundary replays it: definition, data and count.
        assert_eq!(ints(&mut b, "SELECT a FROM mv;"), vec![1, 2]);
        assert_eq!(count(&b, "mv"), 1);
        // Before that logged refresh existed, b agreed with a: both were stale
        // until the owning session's next boundary.
        // b's own write refreshes its view and lands in the WAL; a sees it.
        b.execute("INSERT INTO t VALUES (3);").unwrap();
        assert_eq!(ints(&mut b, "SELECT a FROM mv;"), vec![1, 2, 3]);
        assert_eq!(ints(&mut a, "SELECT a FROM mv;"), vec![1, 2, 3]);
        assert_eq!(count(&a, "mv"), 2);
    }

    /// The `Connection`/server path gets the same boundary enforcement, also
    /// for multi-statement strings and explicit transactions.
    #[test]
    fn connections_get_boundary_refresh() {
        let database = crate::Database::new();
        {
            let mut c = database.connect();
            c.execute("CREATE TABLE t (a INTEGER);").unwrap();
            c.execute("CREATE MATERIALIZED VIEW mv REFRESH ON COMMIT AS SELECT a FROM t;")
                .unwrap();
            c.execute("INSERT INTO t VALUES (1);").unwrap();
        }
        // A different connection's first statement observes the refresh.
        let mut c2 = database.connect();
        assert_eq!(c_ints(&mut c2, "SELECT a FROM mv;"), vec![1]);
        // Inside a transaction the view stays as of the last boundary.
        c2.execute("BEGIN;").unwrap();
        c2.execute("INSERT INTO t VALUES (2);").unwrap();
        assert_eq!(c_ints(&mut c2, "SELECT a FROM mv;"), vec![1], "deferred");
        c2.execute("COMMIT;").unwrap();
        assert_eq!(c_ints(&mut c2, "SELECT a FROM mv;"), vec![1, 2]);
        // A transaction with no base-table write triggers nothing.
        let before = c2.execute("SHOW MATERIALIZED VIEWS;").unwrap();
        assert_eq!(before.len(), 1);
        c2.execute("BEGIN;").unwrap();
        c2.execute("CREATE TABLE unrelated (x INTEGER);").unwrap();
        c2.execute("INSERT INTO unrelated VALUES (1);").unwrap();
        c2.execute("COMMIT;").unwrap();
        assert_eq!(c_ints(&mut c2, "SELECT a FROM mv;"), vec![1, 2]);
        // Manual refresh through a connection; a third connection sees it.
        c2.execute("INSERT INTO t VALUES (3);").unwrap();
        c2.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
        let mut c3 = database.connect();
        assert_eq!(c_ints(&mut c3, "SELECT a FROM mv;"), vec![1, 2, 3]);
    }

    /// Identifiers holding quotes/spaces survive the auto-generated refresh
    /// statement text (it is WAL-logged and reparsed).
    #[test]
    fn quoted_identifiers_round_trip_through_the_log() {
        let s = Scratch::new("quoted");
        let mut db = durable(&s);
        db.execute("CREATE TABLE \"odd table\" (a INTEGER);")
            .unwrap();
        db.execute("INSERT INTO \"odd table\" VALUES (1);").unwrap();
        db.execute(
            "CREATE MATERIALIZED VIEW \"we\"\"ird mv\" REFRESH ON COMMIT AS SELECT a FROM \"odd table\";",
        )
        .unwrap();
        db.execute("INSERT INTO \"odd table\" VALUES (2);").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM \"we\"\"ird mv\";"),
            vec![1, 2],
            "the auto refresh appended and re-executed its own statement"
        );
        drop(db);
        let mut db = durable(&s);
        assert_eq!(ints(&mut db, "SELECT a FROM \"we\"\"ird mv\";"), vec![1, 2]);
        assert_eq!(show(&mut db, "we\"ird mv").refresh_count, 1);
    }
}
