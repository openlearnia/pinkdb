//! Lake replication jobs (wave 89, T1): **table-level** synchronization from
//! one source into a writable destination lake.
//!
//! # The model
//!
//! A **destination** is an ordinary writable [`Lake`]. It may hold the user's
//! own tables next to replicated ones, and any number of jobs. A **job**
//! replicates the tables of one **source** into the destination under the
//! same `db.table` names:
//!
//! - [`Source::Lake`] — another lake (a local directory or `s3://` prefix).
//!   Its encryption key is never stored in the state: the caller supplies it
//!   to [`catch_up`] on every run.
//! - [`Source::Store`] — one database of the caller's live [`PinkDb`] store
//!   (the CLI's session store), passed to [`catch_up`] as `Some(store)`.
//!
//! Jobs and their per-table state live **in the destination lake**
//! (`replication/state.json`), so any process that opens the destination sees
//! them. Every destination write is an ordinary incremental lake commit
//! ([`Lake::commit_from_tables`] with only that table in `base`/`current`):
//! a replicated version is a normal version (time travel, attach, CDC and
//! maintenance all apply), and the existing concurrent-writer rebase/retry/
//! conflict rules govern races with other writers.
//!
//! # State (`replication/state.json`)
//!
//! ```json
//! {
//!   "replication_format": 1,
//!   "next_id": 3,
//!   "jobs": [{
//!     "id": 1,
//!     "source": { "kind": "lake", "location": "/data/primary" },
//!     "status": "created",
//!     "interval_ms": 60000,
//!     "include": "main.*", "exclude": "", "watermark_columns": "main.events=ts",
//!     "created": "…", "last_run": "…", "last_ok": "…", "last_error": null,
//!     "tables": [{
//!       "source": "main.events", "dest": "main.events",
//!       "strategy": "watermark", "watermark_column": "ts", "pk_columns": null,
//!       "source_table_id": "…", "last_watermark": "…",
//!       "last_source_version": 41, "last_full_sync_version": 12,
//!       "rows_dest": 1234, "last_error": null, "synced": "…"
//!     }]
//!   }]
//! }
//! ```
//!
//! The file is read and replaced through the [`Storage`](crate::storage)
//! layer with conditional writes (`put_new` / `put_if_match`); a lost race
//! re-reads and retries (bounded), never overwriting a concurrent change.
//! Through the destination's storage decorator it is sealed like every other
//! artifact when the destination is keyed. Lake maintenance never touches
//! `replication/` (cleanup only scans `data/`).
//!
//! # Strategies
//!
//! The strategy is chosen per table, in this order (the ducklake-vortex
//! order):
//!
//! 1. **watermark** — configured in [`JobSpec::watermark_columns`] as
//!    `db.table=column`; the column must exist, and a NULL value in it
//!    refuses the table before any destination change. The cursor is
//!    append-only: each cycle deletes destination rows with
//!    `col >= last_watermark`, then inserts source rows with
//!    `last_watermark <= col <= high` (replaying the boundary catches rows
//!    that arrived with the previous maximum). `high < last_watermark` (the
//!    source was recreated or its maximum row deleted) reseeds. Works for
//!    both source kinds.
//! 2. **snapshot** — lake sources. The cursor is `last_source_version`;
//!    [`Lake::table_changes`] over `low+1 ..= high` is applied per version
//!    in order: inserts deduplicated by full-row match against the
//!    destination, then deletes by full-row match. The result is verified
//!    against the source at `high` as a multiset (both directions); any
//!    difference reseeds. A missing/`None` source identity, a changed
//!    identity, a CDC refusal (a full-save version in the range, expired
//!    history) or a cursor below the oldest retained version all reseed.
//! 3. **merge** — store sources whose table declares a `PRIMARY KEY`: delete
//!    destination rows whose key is missing in the source or whose non-key
//!    values changed, then insert source rows whose key is missing in the
//!    (post-delete) destination. Unchanged rows keep their positions, so the
//!    commit is minimal.
//! 4. **full** — everything else: destination := source.
//!
//! Row matching is NULL-safe. Primary keys match with the engine's
//! `IS NOT DISTINCT FROM` equality (NULL equals NULL, every NaN equals every
//! other NaN, `-0.0` equals `0.0`; see [`value_eq`]), because that is what
//! makes two source keys the same key. Everything that decides whether the
//! replica holds the source's **values** — snapshot insert de-duplication and
//! delete matching, the merge's changed-row test, and the post-apply
//! verification — is bit-exact (`-0.0` differs from `0.0`, doubles compare
//! by their bits), so the replica never silently drops a sign or a NaN
//! payload. Watermarks order with [`value_cmp`].
//!
//! # One catch-up cycle
//!
//! [`catch_up`] refuses a paused job, takes an in-process cycle guard keyed by
//! `(destination, job id)`, lists the source tables (minus the include/exclude
//! globs), drops destinations whose source table vanished or became excluded,
//! and then syncs each remaining table in isolation — one failure never aborts
//! the cycle. A source that cannot be listed fails the whole cycle (it is
//! never treated as empty, which would drop every replica table). Ownership is
//! checked before every write: a destination key another job's state already
//! claims, or an existing destination table this job has no state row for, is
//! an error and writes no state row. The claim (a state row with no cursor) is
//! written before the first data commit, so a crash between the data commit
//! and the cursor update converges on the next cycle.
//!
//! Job status: success leaves the job `running` when a live supervisor owns
//! the destination, else `created` — a concurrent `paused` always wins. Any
//! table failure leaves `error` with an `"N table(s) failed: …"` message.
//! `last_run` is always updated and `last_ok` on success.
//!
//! # The supervisor
//!
//! [`Supervisor`] runs one background thread per destination in this process.
//! [`Supervisor::start`] marks every runnable lake-source job (anything not
//! `paused`) `running`, then runs each due `running`/`error` job on its
//! interval (minimum [`MIN_CYCLE_INTERVAL_MS`]). A failed cycle backs off
//! exponentially (2×, 4×, … up to [`MAX_BACKOFF_MS`]); a successful cycle
//! resets the backoff. Cycles never overlap ([`CycleGuard`]). The thread
//! sleeps on a condition variable, so [`Supervisor::stop`],
//! [`Supervisor::notify`] (run a job now), [`Supervisor::pause`] and
//! [`Supervisor::resume`] wake it immediately. A stop aborts an in-flight
//! cycle at the next table boundary, so shutdown never waits out a long copy.
//! Dropping a [`Supervisor`] stops it.
//!
//! Store-source jobs are **not** supervised in this wave: the caller's
//! [`PinkDb`] store is single-threaded. [`Supervisor::start`] reports them as
//! skipped ([`Supervisor::skipped`]) and they run through [`catch_up`] from
//! the process that owns the store.
//!
//! A process-global registry keyed by destination backs [`supervisor_owns`],
//! so [`catch_up`] leaves a successful job `running` while a live supervisor
//! owns its destination and `created` otherwise.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use pinkdb::{AttachedLake, DataTable, DataType, PinkDb, Value};
use serde::{Deserialize, Serialize};

use crate::incremental::{Commit, CommitReport, CommitSettings};
use crate::storage::{Location, VersionTag};
use crate::{EncryptionKey, Lake, LakeError, Result};

/// The lake-relative path of the replication state file (in the destination).
pub const STATE_PATH: &str = "replication/state.json";

/// The replication state format this build writes and reads.
pub const REPLICATION_FORMAT: u32 = 1;

/// Bounded retries for a conditional state write whose file changed in
/// between (another job or another process moved it).
const STATE_WRITE_RETRIES: u32 = 16;

/// Serializes in-process read-modify-write state updates. The conditional
/// write detects another *process*, but two threads of one process (the
/// supervisor's cycle and a `pause`, say) can pass the tag check in the same
/// instant; serializing them makes the in-process last writer win instead of
/// a stale snapshot overwriting a pause.
static STATE_WRITE_LOCK: Mutex<()> = Mutex::new(());

/// One table's synchronization strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    /// Append-only cursor on a configured column (see the module docs).
    Watermark,
    /// Lake source: replay per-version change data and verify.
    Snapshot,
    /// Store source with a PRIMARY KEY: key-level diff.
    Merge,
    /// Everything else: destination := source.
    #[default]
    Full,
}

impl Strategy {
    /// The state-file spelling (`"watermark"`, `"snapshot"`, `"merge"`,
    /// `"full"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Strategy::Watermark => "watermark",
            Strategy::Snapshot => "snapshot",
            Strategy::Merge => "merge",
            Strategy::Full => "full",
        }
    }
}

impl fmt::Display for Strategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a job's `status` field means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// Never supervised (a manual job); a successful manual cycle leaves it
    /// here.
    #[default]
    Created,
    /// Runnable under the supervisor.
    Running,
    /// Refused by [`catch_up`] until resumed.
    Paused,
    /// The last cycle had a failure; the next cycle retries.
    Error,
}

impl JobStatus {
    /// The state-file spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Created => "created",
            JobStatus::Running => "running",
            JobStatus::Paused => "paused",
            JobStatus::Error => "error",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A job's source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Source {
    /// Another lake, local or `s3://`. The encryption key is supplied to
    /// [`catch_up`] per run and never stored.
    Lake {
        /// A directory path or an `s3://bucket/prefix` URL.
        location: String,
    },
    /// One database of the caller's live [`PinkDb`] store.
    Store {
        /// The database name in the store.
        database: String,
    },
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Lake { location } => write!(f, "lake:{location}"),
            Source::Store { database } => write!(f, "store:{database}"),
        }
    }
}

/// Everything [`create`] needs to record a job.
#[derive(Debug, Clone)]
pub struct JobSpec {
    /// The source to replicate from.
    pub source: Source,
    /// The desired cycle interval (the supervisor's floor is 250 ms).
    pub interval: Duration,
    /// Comma-separated globs over `db.table`; empty means every table.
    pub include: String,
    /// Comma-separated globs over `db.table`; empty means none.
    pub exclude: String,
    /// `db.table=column` entries selecting the watermark strategy.
    pub watermark_columns: String,
}

impl JobSpec {
    /// A spec for `source` with the default 60 s interval and no filters.
    pub fn new(source: Source) -> Self {
        Self {
            source,
            interval: Duration::from_secs(60),
            include: String::new(),
            exclude: String::new(),
            watermark_columns: String::new(),
        }
    }
}

/// One job, exactly as `replication/state.json` records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    /// The job id (unique in the destination; never reused).
    pub id: u64,
    /// The job's source.
    pub source: Source,
    /// The job's status.
    #[serde(default)]
    pub status: JobStatus,
    /// The desired interval, in milliseconds (the supervisor applies a
    /// 250 ms floor).
    pub interval_ms: u64,
    /// Include globs over `db.table` (comma-separated).
    #[serde(default)]
    pub include: String,
    /// Exclude globs over `db.table` (comma-separated).
    #[serde(default)]
    pub exclude: String,
    /// `db.table=column` watermark configuration.
    #[serde(default)]
    pub watermark_columns: String,
    /// When the job was created (ISO-8601 UTC, milliseconds).
    pub created: String,
    /// The last cycle's start/finish time.
    #[serde(default)]
    pub last_run: Option<String>,
    /// The last successful cycle's time.
    #[serde(default)]
    pub last_ok: Option<String>,
    /// The last cycle's error, if it failed.
    #[serde(default)]
    pub last_error: Option<String>,
    /// Per-table state, in first-seen order.
    #[serde(default)]
    pub tables: Vec<TableState>,
}

/// One table's replication state inside a job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableState {
    /// The source table's flat `db.table` key.
    pub source: String,
    /// The destination table's flat `db.table` key (the same name).
    pub dest: String,
    /// The strategy currently used.
    #[serde(default)]
    pub strategy: Strategy,
    /// The watermark column (watermark strategy).
    #[serde(default)]
    pub watermark_column: Option<String>,
    /// The primary-key columns, comma-joined (merge strategy).
    #[serde(default)]
    pub pk_columns: Option<String>,
    /// The source catalog entry's identity at the last sync (lake sources;
    /// `None` for a store source and for a pre-wave-89 source entry).
    #[serde(default)]
    pub source_table_id: Option<String>,
    /// The last watermark value, in the catalog's canonical value encoding
    /// (`crate::stats::encode_value`, decoded with the column's type on the
    /// next cycle — robust for doubles, timestamps and decimals alike).
    #[serde(default)]
    pub last_watermark: Option<String>,
    /// The source lake version the last sync covered (lake sources).
    #[serde(default)]
    pub last_source_version: Option<u64>,
    /// The source lake version a full reseed last ran at (lake sources).
    #[serde(default)]
    pub last_full_sync_version: Option<u64>,
    /// The destination row count after the last sync.
    #[serde(default)]
    pub rows_dest: Option<u64>,
    /// The table's last failure; cleared by a success.
    #[serde(default)]
    pub last_error: Option<String>,
    /// When this row last synced (success or failure).
    #[serde(default)]
    pub synced: Option<String>,
}

impl TableState {
    fn new(source: &str, dest: &str) -> Self {
        Self {
            source: source.to_string(),
            dest: dest.to_string(),
            strategy: Strategy::Full,
            watermark_column: None,
            pk_columns: None,
            source_table_id: None,
            last_watermark: None,
            last_source_version: None,
            last_full_sync_version: None,
            rows_dest: None,
            last_error: None,
            synced: None,
        }
    }
}

/// What one [`catch_up`] cycle did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpReport {
    /// Source tables synced (including no-op cycles that changed nothing).
    pub tables_synced: usize,
    /// Source tables that failed (isolated; the rest still committed).
    pub tables_failed: usize,
    /// Stale destination tables dropped because their source vanished or is
    /// now excluded.
    pub tables_dropped: usize,
    /// Rows the cycle added to destination tables (the sum of the commits'
    /// `rows_added`).
    pub rows_written: u64,
    /// A human-readable summary.
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicationState {
    replication_format: u32,
    next_id: u64,
    #[serde(default)]
    jobs: Vec<Job>,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self {
            replication_format: REPLICATION_FORMAT,
            next_id: 1,
            jobs: Vec::new(),
        }
    }
}

/// One in-process catch-up cycle marker, keyed by `(destination, job id)`.
///
/// A second concurrent cycle for the same job in this process is refused with
/// [`LakeError::InvalidOption`]. The guard is released when it drops, so a
/// panicking or failing cycle never wedges the job. It is process-local: the
/// state file's conditional writes (and the destination lake's own
/// single-writer rules) govern other processes.
#[derive(Debug)]
pub struct CycleGuard {
    key: (String, u64),
}

static ACTIVE_CYCLES: LazyLock<Mutex<BTreeSet<(String, u64)>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

impl CycleGuard {
    /// Mark a cycle for `job_id` on `destination` as running. Fails when one
    /// is already running for the same key.
    pub fn acquire(destination: impl AsRef<Path>, job_id: u64) -> Result<Self> {
        let key = (destination.as_ref().display().to_string(), job_id);
        let mut active = ACTIVE_CYCLES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(key.clone()) {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "a replication cycle for job {job_id} on {} is already running in this \
                     process",
                    key.0
                ),
            });
        }
        Ok(Self { key })
    }

    /// Whether a cycle for `job_id` on `destination` is running in this
    /// process.
    pub fn is_active(destination: impl AsRef<Path>, job_id: u64) -> bool {
        let key = (destination.as_ref().display().to_string(), job_id);
        ACTIVE_CYCLES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&key)
    }
}

impl Drop for CycleGuard {
    fn drop(&mut self) {
        ACTIVE_CYCLES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
    }
}

/// The supervisor's minimum cycle interval, in milliseconds (the same floor
/// as DuckLake's `MIN_CYCLE_INTERVAL_MS`).
pub const MIN_CYCLE_INTERVAL_MS: u64 = 250;

/// The supervisor's backoff ceiling, in milliseconds, after failed cycles
/// (DuckLake's `MAX_BACKOFF_MS`).
pub const MAX_BACKOFF_MS: u64 = 60_000;

/// The consecutive-failure exponent stops growing here (2^8 × the interval,
/// then capped at [`MAX_BACKOFF_MS`]).
const MAX_BACKOFF_EXPONENT: u32 = 8;

/// The destinations with a live supervisor in this process. [`supervisor_owns`]
/// is the read side used by [`catch_up`] when it writes the post-cycle status;
/// [`Supervisor::stop`] removes the entry after the thread has joined.
static LIVE_SUPERVISORS: LazyLock<Mutex<BTreeSet<String>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

/// Whether a live supervisor in this process owns `job_id` on `destination`.
///
/// [`catch_up`] consults this before it writes the post-cycle status: a
/// successful cycle leaves the job `running` while a supervisor owns the
/// destination and `created` otherwise (a concurrent `paused` always wins).
pub(crate) fn supervisor_owns(destination: &str, _job_id: u64) -> bool {
    LIVE_SUPERVISORS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(destination)
}

/// The result of the last cycle a [`Supervisor`] ran for one job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CycleOutcome {
    /// [`catch_up`] returned a report; `tables_failed == 0` means the cycle
    /// succeeded (per-table failures are isolated, so the job is in `error`).
    Completed {
        /// Source tables synced.
        tables_synced: usize,
        /// Source tables that failed.
        tables_failed: usize,
        /// Stale destination tables dropped.
        tables_dropped: usize,
        /// Rows the cycle added to destination tables.
        rows_written: u64,
        /// The cycle's human-readable summary.
        message: String,
    },
    /// The cycle failed before the per-table work (the job is `error` and the
    /// supervisor retries with backoff).
    Failed(String),
    /// The job is not supervised in this process; it runs through [`catch_up`]
    /// from the process that owns its store.
    Skipped(String),
    /// A stop request interrupted the cycle at a table boundary.
    Aborted,
}

/// One job's live state in a [`Supervisor`] (see [`Supervisor::status`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorStatus {
    /// The job id.
    pub job_id: u64,
    /// The last completed cycle's result, or `None` before the first cycle.
    pub last_cycle: Option<CycleOutcome>,
    /// When the supervisor will next run the job, or `None` when it will not
    /// (paused, not runnable, or skipped).
    pub next_due: Option<SystemTime>,
    /// Consecutive failed cycles (the backoff exponent).
    pub consecutive_failures: u32,
}

/// One background replication supervisor: a thread per destination that runs
/// due `running`/`error` lake-source jobs.
///
/// See the module docs for the scheduling rules. [`Supervisor::drop`] stops
/// the thread, so a coupled lifetime is all the cleanup a caller needs.
pub struct Supervisor {
    inner: Arc<SupervisorInner>,
    thread: Option<JoinHandle<()>>,
}

struct SupervisorInner {
    destination: String,
    key: Option<EncryptionKey>,
    schedule: Mutex<BTreeMap<u64, ScheduleEntry>>,
    cv: Condvar,
    stop: AtomicBool,
    abort: AtomicBool,
}

#[derive(Debug, Clone, Default)]
struct ScheduleEntry {
    next_due: Option<Instant>,
    failures: u32,
    last: Option<CycleOutcome>,
    skipped: Option<String>,
}

impl Supervisor {
    /// Start the supervisor for the destination lake at `destination`.
    ///
    /// Marks every lake-source job that is not `paused` `running` (so a
    /// `created`/`error` job becomes runnable) and starts the background
    /// thread. Store-source jobs are left alone and reported through
    /// [`Supervisor::skipped`]. `key` is the destination's at-rest encryption
    /// key, if it has one.
    ///
    /// Fails when a supervisor for the same destination is already running in
    /// this process, or when the destination cannot be opened.
    pub fn start(destination: &str, key: Option<EncryptionKey>) -> Result<Supervisor> {
        let mut lake = open_destination(destination, key.as_ref())?;
        let resolved = lake.path().display().to_string();
        let current = jobs(&lake)?;
        {
            let mut live = LIVE_SUPERVISORS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !live.insert(resolved.clone()) {
                return Err(LakeError::InvalidOption {
                    message: format!(
                        "a replication supervisor for {resolved} is already running in this \
                         process"
                    ),
                });
            }
        }
        let mut schedule: BTreeMap<u64, ScheduleEntry> = BTreeMap::new();
        for job in &current {
            match &job.source {
                Source::Store { database } => {
                    let reason = store_source_skip(database);
                    schedule.insert(
                        job.id,
                        ScheduleEntry {
                            skipped: Some(reason.clone()),
                            last: Some(CycleOutcome::Skipped(reason)),
                            ..Default::default()
                        },
                    );
                }
                Source::Lake { .. } => {
                    if job.status == JobStatus::Paused {
                        continue;
                    }
                    if let Err(e) = resume(&mut lake, job.id) {
                        LIVE_SUPERVISORS
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(&resolved);
                        return Err(e);
                    }
                    schedule.insert(
                        job.id,
                        ScheduleEntry {
                            next_due: Some(Instant::now()),
                            ..Default::default()
                        },
                    );
                }
            }
        }
        let inner = Arc::new(SupervisorInner {
            destination: resolved.clone(),
            key,
            schedule: Mutex::new(schedule),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
            abort: AtomicBool::new(false),
        });
        let for_thread = Arc::clone(&inner);
        let thread = match std::thread::Builder::new()
            .name(format!("pink-lake-replication:{resolved}"))
            .spawn(move || supervisor_loop(for_thread))
        {
            Ok(thread) => thread,
            Err(e) => {
                LIVE_SUPERVISORS
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&resolved);
                return Err(LakeError::Internal {
                    message: format!("could not start the replication supervisor thread: {e}"),
                });
            }
        };
        Ok(Self {
            inner,
            thread: Some(thread),
        })
    }

    /// The resolved destination location this supervisor runs.
    pub fn destination(&self) -> &str {
        &self.inner.destination
    }

    /// Each scheduled job's last cycle result, next due time and backoff
    /// count, in job-id order. Store-source jobs appear with
    /// [`CycleOutcome::Skipped`] and no due time.
    pub fn status(&self) -> Vec<SupervisorStatus> {
        let schedule = self
            .inner
            .schedule
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        schedule
            .iter()
            .map(|(job_id, entry)| SupervisorStatus {
                job_id: *job_id,
                last_cycle: entry.last.clone(),
                next_due: entry
                    .next_due
                    .map(|due| SystemTime::now() + due.saturating_duration_since(Instant::now())),
                consecutive_failures: entry.failures,
            })
            .collect()
    }

    /// The jobs this supervisor will not run, with the reason: store-source
    /// jobs (the store is single-threaded; run [`catch_up`] from the process
    /// that owns it).
    pub fn skipped(&self) -> Vec<(u64, String)> {
        let schedule = self
            .inner
            .schedule
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        schedule
            .iter()
            .filter_map(|(job_id, entry)| entry.skipped.clone().map(|reason| (*job_id, reason)))
            .collect()
    }

    /// Ask the supervisor to run `job_id` at the next opportunity instead of
    /// waiting for its interval.
    pub fn notify(&self, job_id: u64) {
        {
            let mut schedule = self
                .inner
                .schedule
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let entry = schedule.entry(job_id).or_default();
            if entry.skipped.is_none() {
                entry.next_due = Some(Instant::now());
            }
        }
        self.inner.cv.notify_all();
    }

    /// Pause `job_id` (a state-file write) and wake the thread.
    pub fn pause(&self, job_id: u64) -> Result<()> {
        self.update(|lake| pause(lake, job_id))?;
        self.inner
            .schedule
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&job_id);
        self.inner.cv.notify_all();
        Ok(())
    }

    /// Resume `job_id` (a state-file write), schedule it now and wake the
    /// thread.
    pub fn resume(&self, job_id: u64) -> Result<()> {
        self.update(|lake| resume(lake, job_id))?;
        self.wake(job_id);
        Ok(())
    }

    /// Resume every paused/errored job and schedule each now; returns their
    /// ids.
    pub fn resume_all(&self) -> Result<Vec<u64>> {
        let resumed = self.update(resume_all)?;
        for job_id in &resumed {
            self.wake(*job_id);
        }
        Ok(resumed)
    }

    /// Stop the thread, abort an in-flight cycle at its next table boundary,
    /// and join. Dropping the supervisor does the same.
    pub fn stop(mut self) {
        self.stop_inner();
    }

    fn update<T>(&self, write: impl FnOnce(&mut Lake) -> Result<T>) -> Result<T> {
        let mut lake = self.inner.open()?;
        write(&mut lake)
    }

    fn wake(&self, job_id: u64) {
        {
            let mut schedule = self
                .inner
                .schedule
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let entry = schedule.entry(job_id).or_default();
            if entry.skipped.is_none() {
                entry.next_due = Some(Instant::now());
            }
        }
        self.inner.cv.notify_all();
    }

    fn stop_inner(&mut self) {
        self.inner.stop.store(true, AtomicOrdering::Release);
        self.inner.abort.store(true, AtomicOrdering::Release);
        self.inner.cv.notify_all();
        if let Some(thread) = self.thread.take()
            && thread.thread().id() != std::thread::current().id()
        {
            let _ = thread.join();
        }
        // Only after the join: an in-flight `finish_cycle` must still see the
        // registry entry and leave the job `running`.
        LIVE_SUPERVISORS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.inner.destination);
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.stop_inner();
    }
}

impl SupervisorInner {
    fn open(&self) -> Result<Lake> {
        open_destination(&self.destination, self.key.as_ref())
    }
}

fn open_destination(location: &str, key: Option<&EncryptionKey>) -> Result<Lake> {
    match key {
        Some(key) => Lake::open_url_with_key(location, key.clone()),
        None => Lake::open_url(location),
    }
}

fn store_source_skip(database: &str) -> String {
    format!(
        "store source store:{database} is not supervised (the caller's store is single-threaded); \
         run a catch-up from the process that owns the store"
    )
}

/// The supervisor thread: run a pass, then sleep on the condvar until the
/// earliest due job (never longer than a second, so a newly added job is
/// noticed promptly).
fn supervisor_loop(inner: Arc<SupervisorInner>) {
    while !inner.stop.load(AtomicOrdering::Acquire) {
        run_pass(&inner);
        let schedule = inner
            .schedule
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.stop.load(AtomicOrdering::Acquire) {
            break;
        }
        let now = Instant::now();
        let wake = schedule
            .values()
            .filter_map(|entry| entry.next_due)
            .min()
            .unwrap_or(now + Duration::from_secs(1))
            .min(now + Duration::from_secs(1));
        let wait = wake
            .saturating_duration_since(now)
            .max(Duration::from_millis(1));
        let _ = inner.cv.wait_timeout(schedule, wait);
    }
}

/// One scheduling pass: re-read the destination's jobs, update the schedule,
/// then run every due job sequentially (never overlapping: [`catch_up`]
/// holds the [`CycleGuard`]).
fn run_pass(inner: &Arc<SupervisorInner>) {
    let lake = match inner.open() {
        Ok(lake) => lake,
        Err(_) => return,
    };
    let current = match jobs(&lake) {
        Ok(jobs) => jobs,
        Err(_) => return,
    };
    let present: BTreeSet<u64> = current.iter().map(|job| job.id).collect();
    {
        let mut schedule = inner
            .schedule
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        schedule.retain(|job_id, _| present.contains(job_id));
        for job in &current {
            match &job.source {
                Source::Store { database } => {
                    let reason = store_source_skip(database);
                    let entry = schedule.entry(job.id).or_default();
                    entry.next_due = None;
                    entry.failures = 0;
                    entry.skipped = Some(reason.clone());
                    entry.last = Some(CycleOutcome::Skipped(reason));
                }
                Source::Lake { .. } => {
                    if !matches!(job.status, JobStatus::Running | JobStatus::Error) {
                        schedule.remove(&job.id);
                        continue;
                    }
                    let entry = schedule.entry(job.id).or_default();
                    entry.skipped = None;
                    if entry.next_due.is_none() {
                        entry.next_due = Some(Instant::now());
                    }
                }
            }
        }
    }
    for job in &current {
        if !matches!(job.source, Source::Lake { .. })
            || !matches!(job.status, JobStatus::Running | JobStatus::Error)
        {
            continue;
        }
        let due = {
            let schedule = inner
                .schedule
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            schedule
                .get(&job.id)
                .and_then(|entry| entry.next_due)
                .is_some_and(|due| due <= Instant::now())
        };
        if !due {
            continue;
        }
        if inner.stop.load(AtomicOrdering::Acquire) || inner.abort.load(AtomicOrdering::Acquire) {
            return;
        }
        let outcome = match inner.open() {
            Ok(mut cycle_lake) => match catch_up_abortable(
                &mut cycle_lake,
                job.id,
                None,
                inner.key.clone(),
                Some(&inner.abort),
            ) {
                Ok(report) => CycleOutcome::Completed {
                    tables_synced: report.tables_synced,
                    tables_failed: report.tables_failed,
                    tables_dropped: report.tables_dropped,
                    rows_written: report.rows_written,
                    message: report.message,
                },
                Err(_) if inner.abort.load(AtomicOrdering::Acquire) => CycleOutcome::Aborted,
                Err(e) => CycleOutcome::Failed(e.to_string()),
            },
            Err(e) => CycleOutcome::Failed(e.to_string()),
        };
        if inner.abort.load(AtomicOrdering::Acquire) {
            let mut schedule = inner
                .schedule
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = schedule.get_mut(&job.id) {
                entry.last = Some(CycleOutcome::Aborted);
                entry.next_due = None;
            }
            return;
        }
        record_cycle(inner, job, outcome);
    }
}

/// Record `outcome` and schedule the next attempt: the interval after a
/// success, an exponential (capped) backoff after a failure.
fn record_cycle(inner: &SupervisorInner, job: &Job, outcome: CycleOutcome) {
    let interval = job.interval_ms.max(MIN_CYCLE_INTERVAL_MS);
    let success = matches!(
        &outcome,
        CycleOutcome::Completed {
            tables_failed: 0,
            ..
        }
    );
    let mut schedule = inner
        .schedule
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = schedule.entry(job.id).or_default();
    if success {
        entry.failures = 0;
        entry.next_due = Some(Instant::now() + Duration::from_millis(interval));
    } else {
        entry.failures = (entry.failures + 1).min(MAX_BACKOFF_EXPONENT);
        let backoff = interval
            .saturating_mul(1u64 << entry.failures)
            .min(MAX_BACKOFF_MS);
        entry.next_due = Some(Instant::now() + Duration::from_millis(backoff));
    }
    entry.last = Some(outcome);
}

/// Create a job in `dest` and return its id.
///
/// Refuses a lake source whose location resolves to the destination itself,
/// and a malformed `watermark_columns` entry (the column's existence is
/// checked per cycle, against the live source schema).
pub fn create(dest: &mut Lake, spec: JobSpec) -> Result<u64> {
    create_with(dest, spec)
}

fn create_with(dest: &mut Lake, spec: JobSpec) -> Result<u64> {
    if let Source::Lake { location } = &spec.source {
        let parsed = Location::parse(location)?;
        if same_lake_location(dest.path(), &parsed) {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "cannot create a replication job from {} into itself",
                    dest.path().display()
                ),
            });
        }
    }
    // Validate the syntax now (the column is checked per cycle, where the
    // source schema is known).
    parse_watermark_spec(&spec.watermark_columns)?;
    let interval_ms = u64::try_from(spec.interval.as_millis()).unwrap_or(u64::MAX);
    let created = now();
    mutate_state(dest, |state| {
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| LakeError::Internal {
                message: "replication job ids overflow u64".to_string(),
            })?;
        state.jobs.push(Job {
            id,
            source: spec.source.clone(),
            status: JobStatus::Created,
            interval_ms,
            include: spec.include.clone(),
            exclude: spec.exclude.clone(),
            watermark_columns: spec.watermark_columns.clone(),
            created: created.clone(),
            last_run: None,
            last_ok: None,
            last_error: None,
            tables: Vec::new(),
        });
        Ok(id)
    })
}

/// Remove a job's state from `dest`. Replicated tables stay (drop them like
/// any other destination table).
pub fn drop(dest: &mut Lake, job_id: u64) -> Result<()> {
    mutate_state(dest, |state| {
        let before = state.jobs.len();
        state.jobs.retain(|job| job.id != job_id);
        if state.jobs.len() == before {
            return Err(no_such_job(job_id));
        }
        Ok(())
    })
}

/// Every job recorded in `dest`, in creation order.
pub fn jobs(dest: &Lake) -> Result<Vec<Job>> {
    Ok(read_state(dest)?.0.jobs)
}

/// One job's per-table state.
pub fn tables(dest: &Lake, job_id: u64) -> Result<Vec<TableState>> {
    let state = read_state(dest)?.0;
    let job = state
        .jobs
        .iter()
        .find(|job| job.id == job_id)
        .ok_or_else(|| no_such_job(job_id))?;
    Ok(job.tables.clone())
}

/// Pause a job. [`catch_up`] refuses it until [`resume`].
pub fn pause(dest: &mut Lake, job_id: u64) -> Result<()> {
    mutate_state(dest, |state| {
        let job = state
            .jobs
            .iter_mut()
            .find(|job| job.id == job_id)
            .ok_or_else(|| no_such_job(job_id))?;
        job.status = JobStatus::Paused;
        Ok(())
    })
}

/// Resume one paused/errored job: it becomes `running` (the supervisor's
/// runnable state; a manual [`catch_up`] sets it back to `created` when no
/// supervisor owns it).
pub fn resume(dest: &mut Lake, job_id: u64) -> Result<()> {
    mutate_state(dest, |state| {
        let job = state
            .jobs
            .iter_mut()
            .find(|job| job.id == job_id)
            .ok_or_else(|| no_such_job(job_id))?;
        job.status = JobStatus::Running;
        Ok(())
    })
}

/// Resume every paused or errored job; returns their ids.
pub fn resume_all(dest: &mut Lake) -> Result<Vec<u64>> {
    mutate_state(dest, |state| {
        let mut resumed = Vec::new();
        for job in &mut state.jobs {
            if matches!(job.status, JobStatus::Paused | JobStatus::Error) {
                job.status = JobStatus::Running;
                resumed.push(job.id);
            }
        }
        Ok(resumed)
    })
}

/// How many source versions the job's tables are behind.
///
/// Lake sources: the source lake's head version minus the minimum recorded
/// `last_source_version` (a table that never synced counts as version 0), or
/// `0` when the job has no table rows. Store sources have no version cursor:
/// `None`.
///
/// The source is opened **without a key** here (the job API has no key
/// parameter), so an encrypted source fails with the usual keyed-lake error;
/// use the source's key with a normal [`Lake::open_with_key`] if you need the
/// number out of band.
pub fn versions_behind(dest: &Lake, job_id: u64) -> Result<Option<u64>> {
    let state = read_state(dest)?.0;
    let job = state
        .jobs
        .iter()
        .find(|job| job.id == job_id)
        .ok_or_else(|| no_such_job(job_id))?;
    let Source::Lake { location } = &job.source else {
        return Ok(None);
    };
    let source = Lake::open_url(location)?;
    let head = source.current_version();
    let Some(minimum) = job
        .tables
        .iter()
        .map(|table| table.last_source_version.unwrap_or(0))
        .min()
    else {
        return Ok(Some(0));
    };
    Ok(Some(head.saturating_sub(minimum)))
}

/// Run one replication cycle for `job_id` on `dest`.
///
/// `store` is required for a [`Source::Store`] job (the caller's live store)
/// and ignored for a lake source. `source_key` is the source lake's
/// encryption key, if it has one; the destination's own key is whatever
/// `dest` was opened with.
///
/// The cycle is described in the module docs. It returns the per-table
/// outcome; per-table failures are isolated (recorded in the state and in
/// `tables_failed`), while a whole-cycle failure (missing/paused job, an
/// unlistable source, a state write that cannot be made) returns `Err` after
/// marking the job `error`.
pub fn catch_up(
    dest: &mut Lake,
    job_id: u64,
    store: Option<&PinkDb>,
    source_key: Option<EncryptionKey>,
) -> Result<CatchUpReport> {
    catch_up_abortable(dest, job_id, store, source_key, None)
}

/// [`catch_up`] with an abort flag.
///
/// The supervisor sets the flag on [`Supervisor::stop`]: the cycle then stops
/// at the next table boundary without marking the job `error` (the job stays
/// `running` for the next process). A normal caller passes `None` and gets
/// exactly [`catch_up`].
pub(crate) fn catch_up_abortable(
    dest: &mut Lake,
    job_id: u64,
    store: Option<&PinkDb>,
    source_key: Option<EncryptionKey>,
    abort: Option<&AtomicBool>,
) -> Result<CatchUpReport> {
    let aborted = || abort.is_some_and(|flag| flag.load(AtomicOrdering::Acquire));
    let state = read_state(dest)?.0;
    let job = state
        .jobs
        .iter()
        .find(|job| job.id == job_id)
        .cloned()
        .ok_or_else(|| no_such_job(job_id))?;
    if job.status == JobStatus::Paused {
        return Err(LakeError::InvalidOption {
            message: format!("replication job {job_id} is paused - resume it before catching up"),
        });
    }
    let _guard = CycleGuard::acquire(dest.path(), job_id)?;

    // The source must be listable; anything else fails the whole cycle (an
    // empty list would drop every replica table).
    let source = match open_source(&job.source, store, source_key) {
        Ok(source) => source,
        Err(e) => return Err(cycle_failure(dest, job_id, e)),
    };
    let source_keys: Vec<String> = match source.keys() {
        Ok(keys) => keys
            .into_iter()
            .filter(|key| wanted(key, &job.include, &job.exclude))
            .collect(),
        Err(e) => return Err(cycle_failure(dest, job_id, e)),
    };
    let wanted_set: BTreeSet<String> = source_keys
        .iter()
        .map(|key| key.to_ascii_lowercase())
        .collect();

    let mut report = CatchUpReport {
        tables_synced: 0,
        tables_failed: 0,
        tables_dropped: 0,
        rows_written: 0,
        message: String::new(),
    };
    let mut failures: Vec<String> = Vec::new();

    // Stale tables: a source table that vanished or is now excluded.
    let recorded = state
        .jobs
        .iter()
        .find(|job| job.id == job_id)
        .map(|job| job.tables.clone())
        .unwrap_or_default();
    for table in recorded {
        if aborted() {
            report.message = aborted_message(&report);
            return Ok(report);
        }
        if wanted_set.contains(&table.source.to_ascii_lowercase()) {
            continue;
        }
        let label = table.source.clone();
        match drop_destination_table(dest, &table.dest) {
            Ok(()) => match forget_table(dest, job_id, &table.source) {
                Ok(()) => report.tables_dropped += 1,
                Err(e) => {
                    report.tables_failed += 1;
                    failures.push(format!("{label}: removing the state row failed: {e}"));
                }
            },
            Err(e) => {
                report.tables_failed += 1;
                failures.push(format!("{label}: {e}"));
                if let Err(state_error) = record_table_error(dest, job_id, &label, &e.to_string()) {
                    failures.push(format!(
                        "{label}: recording the error failed: {state_error}"
                    ));
                }
            }
        }
    }

    for key in &source_keys {
        if aborted() {
            report.message = aborted_message(&report);
            return Ok(report);
        }
        match sync_table(dest, &job, &source, key) {
            Ok((_published, rows)) => {
                report.tables_synced += 1;
                report.rows_written += rows;
            }
            Err(e) => {
                report.tables_failed += 1;
                failures.push(format!("{key}: {e}"));
                if let Err(state_error) = record_table_error(dest, job_id, key, &e.to_string()) {
                    failures.push(format!("{key}: recording the error failed: {state_error}"));
                }
            }
        }
    }

    if aborted() {
        report.message = aborted_message(&report);
        return Ok(report);
    }

    let success = failures.is_empty();
    let detail = (!success).then(|| {
        format!(
            "{} table(s) failed: {}",
            failures.len(),
            failures.join("; ")
        )
    });
    let status_written = finish_cycle(dest, job_id, success, detail.clone());
    if let Err(e) = status_written {
        return Err(cycle_failure(dest, job_id, e));
    }

    if success {
        report.message = if source_keys.is_empty() && report.tables_dropped == 0 {
            "no source tables matched include/exclude patterns".to_string()
        } else {
            format!(
                "copied {} tables / {} rows",
                report.tables_synced, report.rows_written
            )
        };
        if report.tables_dropped > 0 {
            report.message.push_str(&format!(
                ", dropped {} stale table(s)",
                report.tables_dropped
            ));
        }
    } else if source_keys.is_empty() && report.tables_dropped == 0 {
        report.message = format!(
            "no source tables matched include/exclude patterns; {}",
            detail.unwrap_or_default()
        );
    } else {
        report.message = format!(
            "copied {} tables / {} rows",
            report.tables_synced, report.rows_written
        );
        if report.tables_dropped > 0 {
            report.message.push_str(&format!(
                ", dropped {} stale table(s)",
                report.tables_dropped
            ));
        }
        if let Some(detail) = detail {
            report.message.push_str("; ");
            report.message.push_str(&detail);
        }
    }
    Ok(report)
}

// ---------------------------------------------------------------------------
// Source access
// ---------------------------------------------------------------------------

/// The source tables, materializable one at a time (per-table failures stay
/// isolated).
enum SourceReader<'a> {
    Lake {
        lake: Box<Lake>,
        attachment: Arc<dyn AttachedLake>,
        ids: BTreeMap<String, Option<String>>,
        head: u64,
        oldest: Option<u64>,
    },
    Store {
        store: &'a PinkDb,
        database: String,
        tables: Vec<String>,
    },
}

impl SourceReader<'_> {
    fn keys(&self) -> Result<Vec<String>> {
        Ok(match self {
            SourceReader::Lake { attachment, .. } => attachment
                .tables()
                .iter()
                .map(|table| format!("{}.{}", table.db, table.table))
                .collect(),
            SourceReader::Store {
                database, tables, ..
            } => tables
                .iter()
                .map(|table| format!("{database}.{table}"))
                .collect(),
        })
    }

    fn load(&self, key: &str) -> Result<DataTable> {
        match self {
            SourceReader::Lake { attachment, .. } => {
                let (db, table) = split_key(key)?;
                attachment
                    .load(db, table)
                    .map(|table| (*table).clone())
                    .map_err(|e| LakeError::Engine {
                        context: format!("materializing source table \"{key}\""),
                        source: e,
                    })
            }
            SourceReader::Store {
                store, database, ..
            } => {
                let (_, table) = split_key(key)?;
                store
                    .table_in(database, table)
                    .cloned()
                    .map_err(|e| LakeError::Engine {
                        context: format!("reading store source table \"{key}\""),
                        source: e,
                    })
            }
        }
    }

    fn table_id(&self, key: &str) -> Option<String> {
        match self {
            SourceReader::Lake { ids, .. } => ids
                .iter()
                .find(|(source, _)| source.eq_ignore_ascii_case(key))
                .and_then(|(_, id)| id.clone()),
            SourceReader::Store { .. } => None,
        }
    }

    fn head_version(&self) -> Option<u64> {
        match self {
            SourceReader::Lake { head, .. } => Some(*head),
            SourceReader::Store { .. } => None,
        }
    }

    fn oldest_version(&self) -> Option<u64> {
        match self {
            SourceReader::Lake { oldest, .. } => *oldest,
            SourceReader::Store { .. } => None,
        }
    }

    fn changes(&self, key: &str, low: u64, high: u64) -> Result<Vec<crate::cdc::ChangeRow>> {
        match self {
            SourceReader::Lake { lake, .. } => lake.table_changes(key, low, Some(high)),
            SourceReader::Store { .. } => Err(LakeError::Internal {
                message: "store sources have no change history".to_string(),
            }),
        }
    }

    /// A store table's declared PRIMARY KEY columns, in declaration order.
    fn pk_columns(&self, key: &str) -> Option<Vec<String>> {
        let SourceReader::Store {
            store, database, ..
        } = self
        else {
            return None;
        };
        let (_, table) = split_key(key).ok()?;
        let source = store.table_in(database, table).ok()?;
        let key = source.constraints.primary_key()?;
        key.columns
            .iter()
            .map(|&index| source.columns.get(index).map(|c| c.name.clone()))
            .collect()
    }
}

fn open_source<'a>(
    source: &Source,
    store: Option<&'a PinkDb>,
    key: Option<EncryptionKey>,
) -> Result<SourceReader<'a>> {
    match source {
        Source::Lake { location } => {
            Location::parse(location)?;
            let lake = match key {
                Some(key) => Lake::open_url_with_key(location, key)?,
                None => Lake::open_url(location)?,
            };
            let (attachment, _) = crate::attach::build(&lake, None)?;
            let ids: BTreeMap<String, Option<String>> = lake.table_ids()?.into_iter().collect();
            let head = lake.current_version();
            // Version 1 is the empty create manifest and carries no change
            // history; the oldest *usable* retained version is the first one
            // after it (a snapshot cursor below that has expired history).
            let oldest = lake.versions()?.into_iter().find(|version| *version > 1);
            Ok(SourceReader::Lake {
                lake: Box::new(lake),
                attachment,
                ids,
                head,
                oldest,
            })
        }
        Source::Store { database } => {
            let Some(store) = store else {
                return Err(LakeError::InvalidOption {
                    message: format!(
                        "replication source {source} needs the caller's store (catch_up's \
                         `store` argument)"
                    ),
                });
            };
            if !store.database_exists(database) {
                return Err(LakeError::InvalidOption {
                    message: format!(
                        "replication source store database \"{database}\" does not exist"
                    ),
                });
            }
            Ok(SourceReader::Store {
                store,
                database: database.clone(),
                tables: store.list_tables_in(database),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// One table
// ---------------------------------------------------------------------------

/// What a destination table's schema allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaFit {
    /// Every column agrees (name ignoring case, exact type).
    Same,
    /// The destination is a strict prefix of the source: new columns can be
    /// added in place.
    Extend,
    /// Anything else: recreate (which reseeds).
    Recreate,
}

/// Sync one source table. Returns `(whether a data version was published,
/// rows_added)`.
fn sync_table(
    dest: &mut Lake,
    job: &Job,
    source: &SourceReader<'_>,
    key: &str,
) -> Result<(bool, u64)> {
    // Ownership: another job's state row for this destination key.
    let state = read_state(dest)?.0;
    if let Some(owner) = state
        .jobs
        .iter()
        .filter(|other| other.id != job.id)
        .find_map(|other| {
            other
                .tables
                .iter()
                .find(|table| table.dest.eq_ignore_ascii_case(key))
                .map(|_| other.id)
        })
    {
        return Err(LakeError::InvalidOption {
            message: format!(
                "destination table \"{key}\" is already managed by replication job {owner}"
            ),
        });
    }
    let prior = state
        .jobs
        .iter()
        .find(|other| other.id == job.id)
        .and_then(|other| {
            other
                .tables
                .iter()
                .find(|table| table.source.eq_ignore_ascii_case(key))
                .cloned()
        });

    let (manifest, manifest_path) = dest.manifest(None)?;
    let base_version = manifest.version;
    let dest_entry = manifest.tables.get(key).cloned();
    if dest_entry.is_some() && prior.is_none() {
        return Err(LakeError::InvalidOption {
            message: format!(
                "destination table \"{key}\" already exists and is not managed by this \
                 replication job"
            ),
        });
    }
    let base = match &dest_entry {
        Some(_) => {
            let (attachment, _) = crate::attach::build(dest, None)?;
            let (db, table) = split_key(key)?;
            Some(Arc::new(
                attachment
                    .load(db, table)
                    .map(|table| (*table).clone())
                    .map_err(|e| LakeError::Engine {
                        context: format!(
                            "materializing destination table \"{key}\" at version \
                             {base_version} ({})",
                            manifest_path.display()
                        ),
                        source: e,
                    })?,
            ))
        }
        None => None,
    };

    // The source table's live content and schema.
    let source_data = source.load(key)?;
    let source_table_id = source.table_id(key);
    let source_version = source.head_version();

    // Strategy and its configuration.
    let source_pk = source.pk_columns(key);
    let (strategy, watermark_column, pk_columns) =
        choose_strategy(job, key, &source_data, source_pk.as_deref())?;

    // Claim the destination before the first data commit (a state row with no
    // cursor). The ownership checks run again against the freshly-read state
    // inside the mutator, so a concurrent claim cannot slip in.
    if prior.is_none() {
        claim_table(
            dest,
            job.id,
            key,
            strategy,
            watermark_column.as_deref(),
            pk_columns.as_deref(),
            source_table_id.as_deref(),
        )?;
    }

    let identity_changed = prior
        .as_ref()
        .is_none_or(|prior| prior.source_table_id != source_table_id);

    let dest_schema = base.as_ref().map(|table| table.schema());
    let source_schema = source_data.schema();
    let fit = match dest_schema.as_deref() {
        Some(schema) => schema_fit(schema, &source_schema),
        None => SchemaFit::Recreate,
    };

    // A recreate replaces the whole table content (which reseeds every
    // strategy). An extension is applied in place only for merge (its diff
    // backfills the new columns); watermark/snapshot reseed on an extension,
    // exactly like the reference.
    let mut reseed_from_scratch = base.is_none() || fit == SchemaFit::Recreate;
    let extended = fit == SchemaFit::Extend;
    if extended && strategy != Strategy::Merge {
        reseed_from_scratch = true;
    }

    let mut content: DataTable;
    let mut last_watermark: Option<String> = prior.as_ref().and_then(|p| p.last_watermark.clone());
    let last_source_version: Option<u64>;
    let mut last_full_sync_version = prior.as_ref().and_then(|p| p.last_full_sync_version);

    match strategy {
        Strategy::Watermark => {
            let column = watermark_column
                .as_deref()
                .ok_or_else(|| LakeError::Internal {
                    message: format!("table \"{key}\": watermark strategy without a column"),
                })?;
            let index = source_data
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| LakeError::InvalidOption {
                    message: format!(
                        "watermark column \"{column}\" not found on source table \"{key}\""
                    ),
                })?;
            let dtype = source_data.columns[index].dtype;
            let rows = table_rows(&source_data);
            let high = watermark_high(key, column, dtype, &rows, index)?;
            let last = match (
                prior.as_ref().filter(|p| p.strategy == Strategy::Watermark),
                prior
                    .as_ref()
                    .and_then(|p| p.watermark_column.as_deref())
                    .filter(|prior_column| prior_column.eq_ignore_ascii_case(column)),
                last_watermark.as_deref(),
            ) {
                (Some(_), Some(_), Some(encoded)) => {
                    crate::stats::decode_value(dtype, encoded).filter(|v| !v.is_null())
                }
                _ => None,
            };
            let reseed = reseed_from_scratch
                || prior.is_none()
                || prior
                    .as_ref()
                    .filter(|p| p.strategy == Strategy::Watermark)
                    .is_none()
                || prior
                    .as_ref()
                    .and_then(|p| p.watermark_column.as_deref())
                    .is_none_or(|prior_column| !prior_column.eq_ignore_ascii_case(column))
                || last.is_none()
                || identity_changed
                || match (&high, &last) {
                    (Some(high), Some(last)) => value_cmp(high, last) == Ordering::Less,
                    _ => true,
                };
            if reseed {
                content = table_from_rows(key, &source_data, rows)?;
                last_full_sync_version = source_version;
            } else {
                let last = last.expect("checked above");
                let mut kept: Vec<Vec<Value>> = table_rows(
                    base.as_ref()
                        .expect("a watermark table with a cursor has a base")
                        .as_ref(),
                );
                // Keep exactly the rows below the cursor (a NULL never
                // matches `col >= last` in SQL, so it is kept).
                kept.retain(|row| match row.get(index) {
                    Some(Value::Null) | None => true,
                    Some(value) => value_cmp(value, &last) == Ordering::Less,
                });
                for row in rows {
                    let value = row.get(index).ok_or_else(|| LakeError::Internal {
                        message: format!(
                            "table \"{key}\": a materialized row is missing the watermark \
                                 column \"{column}\""
                        ),
                    })?;
                    if value_cmp(value, &last) != Ordering::Less
                        && high
                            .as_ref()
                            .is_none_or(|high| value_cmp(value, high) != Ordering::Greater)
                    {
                        kept.push(row);
                    }
                }
                content = table_from_rows(key, &source_data, kept)?;
            }
            last_watermark = high
                .as_ref()
                .and_then(|value| crate::stats::encode_value(dtype, value));
            last_source_version = source_version;
        }
        Strategy::Snapshot => {
            let high = source_version.ok_or_else(|| LakeError::Internal {
                message: format!("table \"{key}\": snapshot strategy on a versionless source"),
            })?;
            let low = prior.as_ref().and_then(|p| p.last_source_version);
            // A source entry without an identity cannot be trusted across
            // cycles: reseed until the source writes again and assigns one.
            let mut reseed = reseed_from_scratch
                || low.is_none()
                || identity_changed
                || source_table_id.is_none()
                || low.is_some_and(|low| low > high);
            if !reseed
                && let Some(low) = low
                && source.oldest_version().is_some_and(|oldest| low < oldest)
            {
                reseed = true;
            }
            if !reseed
                && let Some(low) = low
                && low < high
            {
                match source.changes(key, low + 1, high) {
                    Ok(changes) => {
                        let mut rows = table_rows(
                            base.as_ref()
                                .expect("an incremental snapshot table has a base")
                                .as_ref(),
                        );
                        apply_snapshot_changes(&mut rows, &changes);
                        content = table_from_rows(key, &source_data, rows)?;
                    }
                    // A full save inside the range, or expired history: the
                    // reference falls back to a reseed rather than failing.
                    Err(_) => {
                        reseed = true;
                        content = source_data.clone();
                    }
                }
            } else if reseed {
                content = source_data.clone();
            } else {
                content = base
                    .as_ref()
                    .expect("a snapshot table with a cursor has a base")
                    .as_ref()
                    .clone();
            }
            // Verify the destination equals the source at `high` as a
            // multiset, both directions; any difference reseeds.
            if !reseed && !multiset_equal(&content, &source_data) {
                reseed = true;
                content = source_data.clone();
            }
            if reseed {
                last_full_sync_version = Some(high);
            }
            last_source_version = Some(high);
            last_watermark = None;
        }
        Strategy::Merge => {
            let names = pk_columns.as_deref().ok_or_else(|| LakeError::Internal {
                message: format!("table \"{key}\": merge strategy without a primary key"),
            })?;
            let pk_names: Vec<&str> = names.split(',').map(str::trim).collect();
            let mut pk_indices = Vec::with_capacity(pk_names.len());
            for name in &pk_names {
                let index = source_data
                    .columns
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| LakeError::InvalidOption {
                        message: format!(
                            "primary-key column \"{name}\" not found on source table \"{key}\""
                        ),
                    })?;
                pk_indices.push(index);
            }
            if reseed_from_scratch {
                content = source_data.clone();
            } else {
                let mut working = if extended {
                    extended_base(
                        base.as_ref()
                            .expect("an extended merge table has a base")
                            .as_ref(),
                        &source_data,
                    )?
                } else {
                    base.as_ref()
                        .expect("a merge table with a state row has a base")
                        .as_ref()
                        .clone()
                };
                merge_content(&mut working, &source_data, &pk_indices)?;
                content = working;
            }
            last_watermark = None;
            last_source_version = source_version;
        }
        Strategy::Full => {
            content = source_data.clone();
            last_watermark = None;
            last_source_version = source_version;
            last_full_sync_version = source_version;
        }
    }

    let table_rows_out = content.num_rows() as u64;
    let report = commit_content(dest, key, base_version, base, &content)?;

    record_success(
        dest,
        job.id,
        key,
        strategy,
        watermark_column.as_deref(),
        pk_columns.as_deref(),
        source_table_id.as_deref(),
        last_watermark,
        last_source_version,
        last_full_sync_version,
        table_rows_out,
    )?;
    Ok((report.committed, report.rows_added))
}

fn choose_strategy(
    job: &Job,
    key: &str,
    source: &DataTable,
    source_pk: Option<&[String]>,
) -> Result<(Strategy, Option<String>, Option<String>)> {
    let spec = parse_watermark_spec(&job.watermark_columns)?;
    if let Some(column) = spec
        .iter()
        .find(|(table, _)| table.eq_ignore_ascii_case(key))
        .map(|(_, column)| column.clone())
    {
        let actual = source
            .columns
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&column))
            .map(|candidate| candidate.name.clone())
            .ok_or_else(|| LakeError::InvalidOption {
                message: format!(
                    "watermark column \"{column}\" not found on source table \"{key}\""
                ),
            })?;
        return Ok((Strategy::Watermark, Some(actual), None));
    }
    if matches!(job.source, Source::Lake { .. }) {
        return Ok((Strategy::Snapshot, None, None));
    }
    match source_pk {
        Some(columns) if !columns.is_empty() => {
            Ok((Strategy::Merge, None, Some(columns.join(","))))
        }
        _ => Ok((Strategy::Full, None, None)),
    }
}

/// Parse `db.table=column` entries (comma-separated; empty is valid).
fn parse_watermark_spec(text: &str) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for piece in text.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let Some((table, column)) = piece.split_once('=') else {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "invalid watermark_columns entry {piece:?} - expected \"db.table=column\""
                ),
            });
        };
        let table = table.trim();
        let column = column.trim();
        if table.is_empty() || !table.contains('.') || column.is_empty() {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "invalid watermark_columns entry {piece:?} - expected \"db.table=column\""
                ),
            });
        }
        out.insert(table.to_string(), column.to_string());
    }
    Ok(out)
}

/// The maximum watermark value, refusing NULL values before any destination
/// change (the cursor cannot order them).
fn watermark_high(
    key: &str,
    column: &str,
    dtype: DataType,
    rows: &[Vec<Value>],
    index: usize,
) -> Result<Option<Value>> {
    let mut high: Option<Value> = None;
    for row in rows {
        let value = &row[index];
        if value.is_null() {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "watermark column \"{column}\" of source table \"{key}\" contains NULL \
                     values; use full replication or remove the NULL rows"
                ),
            });
        }
        if crate::stats::encode_value(dtype, value).is_none() {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "watermark column \"{column}\" of source table \"{key}\" holds a value \
                     that does not encode as {dtype:?}"
                ),
            });
        }
        match &high {
            None => high = Some(value.clone()),
            Some(current) if value_cmp(value, current) == Ordering::Greater => {
                high = Some(value.clone());
            }
            Some(_) => {}
        }
    }
    Ok(high)
}

/// Apply one snapshot CDC range: per version in order, deduplicated inserts
/// by full-row match, then deletes by full-row match. A delete whose row is
/// value-identical to an insert in the same version is a no-op update and is
/// skipped (the change data does not pair update pre/post images).
fn apply_snapshot_changes(rows: &mut Vec<Vec<Value>>, changes: &[crate::cdc::ChangeRow]) {
    // Rows are matched bit-exactly (`exact_key`): replication must keep
    // `-0.0` and NaN payloads, so the engine's `-0.0 = 0.0` equality is the
    // wrong identity here. Positions are indexed by key so one cycle is
    // linear in the destination size plus the changes, not their product.
    let width = rows.first().map_or(0, Vec::len);
    let all: Vec<usize> = (0..width).collect();
    let mut alive: Vec<bool> = vec![true; rows.len()];
    let mut positions: HashMap<Vec<u8>, VecDeque<usize>> = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        positions
            .entry(exact_key(row, &all))
            .or_default()
            .push_back(index);
    }
    let mut start = 0usize;
    while start < changes.len() {
        let version = changes[start].version;
        let mut end = start;
        while end < changes.len() && changes[end].version == version {
            end += 1;
        }
        let batch = &changes[start..end];
        let mut inserted: HashSet<Vec<u8>> = HashSet::new();
        for change in batch {
            if change.change_type == crate::cdc::ChangeKind::Insert {
                let columns: Vec<usize> = (0..change.values.len()).collect();
                let key = exact_key(&change.values, &columns);
                let slot = positions.entry(key.clone()).or_default();
                if slot.is_empty() {
                    slot.push_back(rows.len());
                    rows.push(change.values.clone());
                    alive.push(true);
                }
                inserted.insert(key);
            }
        }
        for change in batch {
            if change.change_type != crate::cdc::ChangeKind::Delete {
                continue;
            }
            let columns: Vec<usize> = (0..change.values.len()).collect();
            let key = exact_key(&change.values, &columns);
            if inserted.contains(&key) {
                continue;
            }
            if let Some(index) = positions.get_mut(&key).and_then(VecDeque::pop_front) {
                alive[index] = false;
            }
        }
        start = end;
    }
    let mut keep = alive.into_iter();
    rows.retain(|_| keep.next().unwrap_or(true));
}

/// The primary-key merge: delete destination rows whose key is missing in the
/// source or whose non-key values changed, then insert source rows whose key
/// is missing in the remaining destination. Unchanged rows keep their order.
fn merge_content(base: &mut DataTable, source: &DataTable, pk_indices: &[usize]) -> Result<()> {
    let base_rows = table_rows(base);
    let source_rows = table_rows(source);
    let pk_set: HashSet<usize> = pk_indices.iter().copied().collect();
    let non_key: Vec<usize> = (0..source.num_cols())
        .filter(|index| !pk_set.contains(index))
        .collect();
    let mut source_by_key: HashMap<Vec<u8>, usize> = HashMap::new();
    for (index, row) in source_rows.iter().enumerate() {
        source_by_key
            .entry(row_key(row, pk_indices))
            .or_insert(index);
    }
    let mut kept: Vec<Vec<Value>> = Vec::new();
    let mut present: HashSet<Vec<u8>> = HashSet::new();
    for row in base_rows {
        let key = row_key(&row, pk_indices);
        let Some(&source_index) = source_by_key.get(&key) else {
            continue;
        };
        let source_row = &source_rows[source_index];
        if non_key
            .iter()
            .all(|&index| value_identical(&row[index], &source_row[index]))
        {
            kept.push(row);
            present.insert(key);
        }
    }
    let mut out = kept;
    for row in source_rows {
        let key = row_key(&row, pk_indices);
        if present.insert(key) {
            out.push(row);
        }
    }
    *base = table_from_rows(&base.name.clone(), source, out)?;
    Ok(())
}

/// Build a table with the source's schema, filling columns the base predates
/// with their declared default (what an `ADD COLUMN` does to old rows).
fn extended_base(base: &DataTable, source: &DataTable) -> Result<DataTable> {
    let mut columns = base.columns.clone();
    for column in source.columns.iter().skip(base.num_cols()) {
        columns.push(crate::schema::constant_column(
            &column.name,
            column.dtype,
            column.default.as_ref(),
            base,
        )?);
    }
    Ok(DataTable::from_columns(
        base.name.clone(),
        columns,
        base.num_rows(),
    ))
}

/// How the destination schema relates to the source schema.
fn schema_fit(dest: &[(String, DataType)], source: &[(String, DataType)]) -> SchemaFit {
    let agrees = |a: &(String, DataType), b: &(String, DataType)| {
        a.0.eq_ignore_ascii_case(&b.0) && a.1 == b.1
    };
    if dest.len() == source.len() && dest.iter().zip(source).all(|(a, b)| agrees(a, b)) {
        return SchemaFit::Same;
    }
    if dest.len() < source.len() && dest.iter().zip(source.iter()).all(|(a, b)| agrees(a, b)) {
        return SchemaFit::Extend;
    }
    SchemaFit::Recreate
}

// ---------------------------------------------------------------------------
// Destination commits
// ---------------------------------------------------------------------------

/// Publish `content` as the destination table's new logical content, with
/// only that table in `base`/`current` (one ordinary incremental commit). An
/// identical content publishes nothing.
fn commit_content(
    dest: &mut Lake,
    key: &str,
    base_version: u64,
    base: Option<Arc<DataTable>>,
    content: &DataTable,
) -> Result<CommitReport> {
    let base: Vec<(String, Arc<DataTable>)> = match base {
        Some(table) => vec![(key.to_string(), table)],
        None => Vec::new(),
    };
    let current = vec![(key.to_string(), Arc::new(content.clone()))];
    dest.commit_from_tables(base_version, &base, &current, &CommitSettings::default())
}

/// Drop one destination table through an ordinary commit. A table that is
/// already gone is a successful no-op.
fn drop_destination_table(dest: &mut Lake, key: &str) -> Result<()> {
    let (manifest, _) = dest.manifest(None)?;
    if !manifest.tables.contains_key(key) {
        return Ok(());
    }
    let mut commit = Commit::new();
    commit.drop_table(key);
    dest.commit(&commit)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// State file
// ---------------------------------------------------------------------------

fn read_state(dest: &Lake) -> Result<(ReplicationState, Option<VersionTag>)> {
    let path = dest.storage.abs(STATE_PATH);
    let context = format!("reading {}", path.display());
    match dest.storage.read_tagged(STATE_PATH, &context) {
        Ok((bytes, tag)) => {
            let state: ReplicationState =
                serde_json::from_slice(&bytes).map_err(|e| LakeError::Corrupt {
                    path: path.clone(),
                    message: format!("replication state does not parse: {e}"),
                })?;
            if state.replication_format != REPLICATION_FORMAT {
                return Err(LakeError::Unsupported {
                    path,
                    found: state.replication_format,
                });
            }
            Ok((state, Some(tag)))
        }
        Err(LakeError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok((ReplicationState::default(), None))
        }
        Err(e) => Err(e),
    }
}

/// Read-modify-write the state with conditional writes and bounded retries.
/// In-process writers are serialized first ([`STATE_WRITE_LOCK`]); another
/// process is caught by the conditional write. The closure re-runs against a
/// freshly-read state after a lost race, so a concurrent change is never
/// overwritten; returning `Err` aborts without writing.
fn mutate_state<T>(
    dest: &Lake,
    mut change: impl FnMut(&mut ReplicationState) -> Result<T>,
) -> Result<T> {
    let _serialized = STATE_WRITE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let path = dest.storage.abs(STATE_PATH);
    for _ in 0..STATE_WRITE_RETRIES {
        let (mut state, tag) = read_state(dest)?;
        let value = change(&mut state)?;
        let bytes = state_json(&state, &path)?;
        let written = match &tag {
            None => dest.storage.put_new(STATE_PATH, &bytes)?,
            Some(tag) => dest.storage.put_if_match(STATE_PATH, &bytes, tag)?,
        };
        if written {
            return Ok(value);
        }
    }
    Err(LakeError::Conflict {
        path,
        message: format!(
            "replication state changed concurrently {STATE_WRITE_RETRIES} times; retry the \
             operation"
        ),
    })
}

fn state_json(state: &ReplicationState, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(state).map_err(|e| LakeError::Internal {
        message: format!("serializing replication state for {}: {e}", path.display()),
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn claim_table(
    dest: &Lake,
    job_id: u64,
    key: &str,
    strategy: Strategy,
    watermark_column: Option<&str>,
    pk_columns: Option<&str>,
    source_table_id: Option<&str>,
) -> Result<()> {
    mutate_state(dest, |state| {
        if !state.jobs.iter().any(|job| job.id == job_id) {
            return Err(no_such_job(job_id));
        }
        if let Some(owner) = other_owner(state, job_id, key) {
            return Err(LakeError::InvalidOption {
                message: format!(
                    "destination table \"{key}\" is already managed by replication job {owner}"
                ),
            });
        }
        let job = state
            .jobs
            .iter_mut()
            .find(|job| job.id == job_id)
            .expect("checked above");
        if let Some(row) = job
            .tables
            .iter_mut()
            .find(|table| table.source.eq_ignore_ascii_case(key))
        {
            row.strategy = strategy;
            row.watermark_column = watermark_column.map(str::to_string);
            row.pk_columns = pk_columns.map(str::to_string);
            row.source_table_id = source_table_id.map(str::to_string);
            return Ok(());
        }
        let mut row = TableState::new(key, key);
        row.strategy = strategy;
        row.watermark_column = watermark_column.map(str::to_string);
        row.pk_columns = pk_columns.map(str::to_string);
        row.source_table_id = source_table_id.map(str::to_string);
        job.tables.push(row);
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
fn record_success(
    dest: &Lake,
    job_id: u64,
    key: &str,
    strategy: Strategy,
    watermark_column: Option<&str>,
    pk_columns: Option<&str>,
    source_table_id: Option<&str>,
    last_watermark: Option<String>,
    last_source_version: Option<u64>,
    last_full_sync_version: Option<u64>,
    rows_dest: u64,
) -> Result<()> {
    let synced = now();
    mutate_state(dest, |state| {
        let Some(job) = state.jobs.iter_mut().find(|job| job.id == job_id) else {
            return Ok(());
        };
        let Some(row) = job
            .tables
            .iter_mut()
            .find(|table| table.source.eq_ignore_ascii_case(key))
        else {
            return Ok(());
        };
        row.strategy = strategy;
        row.watermark_column = watermark_column.map(str::to_string);
        row.pk_columns = pk_columns.map(str::to_string);
        row.source_table_id = source_table_id.map(str::to_string);
        row.last_watermark = last_watermark.clone();
        row.last_source_version = last_source_version;
        row.last_full_sync_version = last_full_sync_version;
        row.rows_dest = Some(rows_dest);
        row.last_error = None;
        row.synced = Some(synced.clone());
        Ok(())
    })
}

/// Record a per-table failure. A row this job already owns is updated; with
/// no row, one is claimed only when no other job owns the destination key and
/// the destination table does not exist (an unmanaged existing table is not
/// this job's to record).
fn record_table_error(dest: &Lake, job_id: u64, key: &str, message: &str) -> Result<()> {
    let (manifest, _) = dest.manifest(None)?;
    let dest_exists = manifest.tables.contains_key(key);
    let synced = now();
    let message = message.to_string();
    mutate_state(dest, |state| {
        if !state.jobs.iter().any(|job| job.id == job_id) {
            return Ok(());
        }
        if let Some(row) = state
            .jobs
            .iter_mut()
            .find(|job| job.id == job_id)
            .and_then(|job| {
                job.tables
                    .iter_mut()
                    .find(|table| table.source.eq_ignore_ascii_case(key))
            })
        {
            row.last_error = Some(message.clone());
            row.synced = Some(synced.clone());
            return Ok(());
        }
        if other_owner(state, job_id, key).is_some() || dest_exists {
            return Ok(());
        }
        let job = state
            .jobs
            .iter_mut()
            .find(|job| job.id == job_id)
            .expect("checked above");
        let mut row = TableState::new(key, key);
        row.last_error = Some(message.clone());
        row.synced = Some(synced.clone());
        job.tables.push(row);
        Ok(())
    })
}

/// Remove one table's state row (a dropped stale destination).
fn forget_table(dest: &Lake, job_id: u64, key: &str) -> Result<()> {
    mutate_state(dest, |state| {
        let Some(job) = state.jobs.iter_mut().find(|job| job.id == job_id) else {
            return Ok(());
        };
        job.tables
            .retain(|table| !table.source.eq_ignore_ascii_case(key));
        Ok(())
    })
}

/// Apply the post-cycle job transition: `last_run` always, `last_ok` on
/// success, `error` + the detail on a table failure. A concurrent pause wins
/// the status.
fn finish_cycle(dest: &Lake, job_id: u64, success: bool, detail: Option<String>) -> Result<()> {
    let location = dest.path().display().to_string();
    let supervised = supervisor_owns(&location, job_id);
    let at = now();
    mutate_state(dest, |state| {
        let Some(job) = state.jobs.iter_mut().find(|job| job.id == job_id) else {
            return Ok(());
        };
        if job.status != JobStatus::Paused {
            job.status = if success {
                if supervised && matches!(job.status, JobStatus::Running | JobStatus::Error) {
                    JobStatus::Running
                } else {
                    JobStatus::Created
                }
            } else {
                JobStatus::Error
            };
        }
        job.last_run = Some(at.clone());
        if success {
            job.last_ok = Some(at.clone());
            job.last_error = None;
        } else {
            job.last_error = detail.clone();
        }
        Ok(())
    })
}

fn mark_job_failure(dest: &Lake, job_id: u64, message: &str) -> Result<()> {
    let at = now();
    mutate_state(dest, |state| {
        let Some(job) = state.jobs.iter_mut().find(|job| job.id == job_id) else {
            return Ok(());
        };
        if job.status != JobStatus::Paused {
            job.status = JobStatus::Error;
        }
        job.last_run = Some(at.clone());
        job.last_error = Some(message.to_string());
        Ok(())
    })
}

fn cycle_failure(dest: &Lake, job_id: u64, error: LakeError) -> LakeError {
    if let Err(state_error) = mark_job_failure(dest, job_id, &error.to_string()) {
        return LakeError::Internal {
            message: format!("{error} (and recording the job error failed: {state_error})"),
        };
    }
    error
}

fn other_owner(state: &ReplicationState, job_id: u64, key: &str) -> Option<u64> {
    state
        .jobs
        .iter()
        .filter(|job| job.id != job_id)
        .find_map(|job| {
            job.tables
                .iter()
                .find(|table| table.dest.eq_ignore_ascii_case(key))
                .map(|_| job.id)
        })
}

fn no_such_job(job_id: u64) -> LakeError {
    LakeError::InvalidOption {
        message: format!("no replication job with id {job_id} in this lake"),
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn now() -> String {
    crate::time::iso8601_utc(SystemTime::now())
}

/// The summary a cycle returns when a supervisor stop interrupts it at a
/// table boundary.
fn aborted_message(report: &CatchUpReport) -> String {
    format!(
        "cycle aborted at a table boundary after {} table(s)",
        report.tables_synced
    )
}

fn split_key(key: &str) -> Result<(&str, &str)> {
    key.split_once('.').ok_or_else(|| LakeError::Corrupt {
        path: std::path::PathBuf::from(key),
        message: format!("table key \"{key}\" is not \"db.table\""),
    })
}

/// Whether a lake source location is the destination itself.
fn same_lake_location(destination: &Path, source: &Location) -> bool {
    let Ok(destination) = Location::parse(&destination.to_string_lossy()) else {
        return false;
    };
    match (&destination, source) {
        (
            Location::S3 {
                bucket: a,
                prefix: pa,
            },
            Location::S3 {
                bucket: b,
                prefix: pb,
            },
        ) => a == b && pa == pb,
        (Location::Local(a), Location::Local(b)) => {
            if a == b {
                return true;
            }
            match (a.canonicalize(), b.canonicalize()) {
                (Ok(a), Ok(b)) => a == b,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Include/exclude filtering over flat `db.table` keys: comma-separated
/// globs with `*` and `?`, matched case-insensitively. An empty include list
/// matches all; any matching exclude wins.
fn wanted(key: &str, include: &str, exclude: &str) -> bool {
    let patterns = |text: &str| -> Vec<String> {
        text.split(',')
            .map(str::trim)
            .filter(|pattern| !pattern.is_empty())
            .map(str::to_string)
            .collect()
    };
    let include = patterns(include);
    let includes = include.is_empty()
        || include
            .iter()
            .any(|pattern| pattern == "*" || pattern == "*.*" || glob_match(pattern, key));
    if !includes {
        return false;
    }
    !patterns(exclude)
        .iter()
        .any(|pattern| pattern == "*" || pattern == "*.*" || glob_match(pattern, key))
}

/// Case-insensitive `*`/`?` glob matching.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.to_lowercase().chars().collect();
    let text: Vec<char> = text.to_lowercase().chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut restart = 0usize;
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some(p);
            restart = t;
            p += 1;
        } else if let Some(star_at) = star {
            p = star_at + 1;
            restart += 1;
            t = restart;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

fn table_rows(table: &DataTable) -> Vec<Vec<Value>> {
    let mut rows = Vec::with_capacity(table.num_rows());
    for row in 0..table.num_rows() {
        let mut values = Vec::with_capacity(table.num_cols());
        for column in 0..table.num_cols() {
            values.push(table.get(row, column).unwrap_or(Value::Null));
        }
        rows.push(values);
    }
    rows
}

fn table_from_rows(name: &str, template: &DataTable, rows: Vec<Vec<Value>>) -> Result<DataTable> {
    let mut table =
        DataTable::new(name.to_string(), template.schema()).map_err(|e| LakeError::Engine {
            context: format!("building destination content for \"{name}\""),
            source: e,
        })?;
    for (index, column) in template.columns.iter().enumerate() {
        table.columns[index].default = column.default.clone();
    }
    for row in rows {
        table.append_row(row).map_err(|e| LakeError::Engine {
            context: format!("building destination content for \"{name}\""),
            source: e,
        })?;
    }
    Ok(table)
}

/// The engine's `IS NOT DISTINCT FROM` value equality: NULL matches NULL,
/// every NaN matches every other NaN, and `-0.0` matches `0.0`.
pub(crate) fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Double(x), Value::Double(y)) => (x.is_nan() && y.is_nan()) || x == y,
        _ => a == b,
    }
}

/// The engine's numeric total order (see `pinkdb`'s `num_cmp`): `-0.0 ==
/// 0.0`, every NaN equals every other NaN and sorts above every other double.
pub(crate) fn value_cmp(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Double(x), Value::Double(y)) => x
            .partial_cmp(y)
            .unwrap_or_else(|| x.is_nan().cmp(&y.is_nan())),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Bigint(x), Value::Bigint(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Varchar(x), Value::Varchar(y)) => x.cmp(y),
        (Value::Decimal(x, sx), Value::Decimal(y, sy)) => {
            let scale = (*sx).max(*sy);
            let raise = |value: i128, from: u8| -> Option<i128> {
                value.checked_mul(10i128.checked_pow(u32::from(scale - from))?)
            };
            match (raise(*x, *sx), raise(*y, *sy)) {
                (Some(x), Some(y)) => x.cmp(&y),
                _ => x.cmp(y),
            }
        }
        (
            Value::Interval {
                months: am,
                days: ad,
                micros: au,
            },
            Value::Interval {
                months: bm,
                days: bd,
                micros: bu,
            },
        ) => (*am, *ad, *au).cmp(&(*bm, *bd, *bu)),
        // A typed column never mixes variants; a fallback keeps this total.
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// A canonical byte key for the engine's equality: values that `value_eq`
/// considers equal encode identically (all NaNs one key, `-0.0`/`0.0` one
/// key, NULL one key), so hash-based row matching agrees with it.
fn row_key(row: &[Value], columns: &[usize]) -> Vec<u8> {
    let mut out = Vec::new();
    for &index in columns {
        encode_key(&row[index], &mut out);
    }
    out
}

/// Bit-exact value identity (replication fidelity): like [`value_eq`] but
/// `-0.0` differs from `0.0` and doubles compare by their bits.
fn value_identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        _ => value_eq(a, b),
    }
}

/// A bit-exact row key ([`value_identical`]): doubles keep their raw bits.
fn exact_key(row: &[Value], columns: &[usize]) -> Vec<u8> {
    let mut out = Vec::new();
    for &index in columns {
        match &row[index] {
            Value::Double(value) => {
                out.push(10);
                out.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            other => encode_key(other, &mut out),
        }
    }
    out
}

fn encode_key(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(0),
        Value::Boolean(value) => {
            out.push(1);
            out.push(u8::from(*value));
        }
        Value::Integer(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Bigint(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Double(value) => {
            out.push(4);
            let bits = if value.is_nan() {
                f64::NAN.to_bits()
            } else if *value == 0.0 {
                0.0f64.to_bits()
            } else {
                value.to_bits()
            };
            out.extend_from_slice(&bits.to_le_bytes());
        }
        Value::Varchar(value) => {
            out.push(5);
            out.extend_from_slice(&(value.len() as u64).to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        Value::Date(value) => {
            out.push(6);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Timestamp(value) => {
            out.push(7);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Decimal(value, scale) => {
            out.push(8);
            out.extend_from_slice(&value.to_le_bytes());
            out.push(*scale);
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            out.push(9);
            out.extend_from_slice(&months.to_le_bytes());
            out.extend_from_slice(&days.to_le_bytes());
            out.extend_from_slice(&micros.to_le_bytes());
        }
    }
}

/// Multiset equality over every column, bit-exact (NULL-safe; `-0.0` and
/// `0.0` differ, NaN payloads compare by bits): the replica must hold the
/// source's exact values.
fn multiset_equal(a: &DataTable, b: &DataTable) -> bool {
    if a.num_rows() != b.num_rows() || a.num_cols() != b.num_cols() {
        return false;
    }
    let left = a.schema();
    let right = b.schema();
    if left.len() != right.len()
        || !left
            .iter()
            .zip(&right)
            .all(|(a, b)| a.0.eq_ignore_ascii_case(&b.0) && a.1 == b.1)
    {
        return false;
    }
    let mut counts: HashMap<Vec<u8>, i64> = HashMap::new();
    let columns: Vec<usize> = (0..a.num_cols()).collect();
    for row in table_rows(a) {
        *counts.entry(exact_key(&row, &columns)).or_insert(0) += 1;
    }
    for row in table_rows(b) {
        let count = counts.entry(exact_key(&row, &columns)).or_insert(0);
        *count -= 1;
        if *count < 0 {
            return false;
        }
    }
    counts.values().all(|count| *count == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs_are_case_insensitive_and_support_star_question() {
        assert!(glob_match("main.*", "MAIN.events"));
        assert!(glob_match("m?in.events", "main.events"));
        assert!(!glob_match("m?in.events", "mainn.events"));
        assert!(glob_match("*", "anything.at.all"));
        assert!(wanted("main.t", "", ""));
        assert!(wanted("main.t", "main.*", ""));
        assert!(!wanted("other.t", "main.*", ""));
        assert!(!wanted("main.t", "", "main.*"));
        assert!(wanted("main.t", "*.*", "main.other"));
    }

    #[test]
    fn value_matching_is_null_safe_and_nan_correct() {
        assert!(value_eq(&Value::Null, &Value::Null));
        assert!(!value_eq(&Value::Null, &Value::Integer(0)));
        assert!(value_eq(
            &Value::Double(f64::from_bits(0x7ff8_0000_0000_0001)),
            &Value::Double(f64::from_bits(0xfff8_0000_0000_0042))
        ));
        assert!(value_eq(&Value::Double(-0.0), &Value::Double(0.0)));
        assert_eq!(
            value_cmp(
                &Value::Double(f64::from_bits(0x7ff8_0000_0000_0001)),
                &Value::Double(f64::INFINITY)
            ),
            Ordering::Greater
        );
        assert_eq!(
            value_cmp(&Value::Double(-0.0), &Value::Double(0.0)),
            Ordering::Equal
        );
    }

    #[test]
    fn replica_value_matching_is_bit_exact() {
        let columns = [0];
        let neg = [Value::Double(-0.0)];
        let pos = [Value::Double(0.0)];
        assert_ne!(exact_key(&neg, &columns), exact_key(&pos, &columns));
        assert!(!value_identical(&neg[0], &pos[0]));
        assert!(value_identical(&Value::Null, &Value::Null));
        // Snapshot apply: a `-0.0` insert next to an existing `0.0` row is a
        // new row, and a delete of `0.0` never removes the `-0.0` one.
        let mut rows = vec![vec![Value::Double(0.0)]];
        let change = |version, kind, value: f64| crate::cdc::ChangeRow {
            version,
            row_id: None,
            change_type: kind,
            values: vec![Value::Double(value)],
        };
        apply_snapshot_changes(
            &mut rows,
            &[
                change(2, crate::cdc::ChangeKind::Insert, -0.0),
                change(3, crate::cdc::ChangeKind::Delete, 0.0),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], Value::Double(x) if x.to_bits() == (-0.0f64).to_bits()));
    }

    #[test]
    fn snapshot_apply_handles_duplicates_by_count() {
        let row = || vec![Value::Integer(7)];
        let mut rows = vec![row(), row(), vec![Value::Integer(8)]];
        let delete = crate::cdc::ChangeRow {
            version: 2,
            row_id: None,
            change_type: crate::cdc::ChangeKind::Delete,
            values: row(),
        };
        apply_snapshot_changes(&mut rows, std::slice::from_ref(&delete));
        // One delete removes exactly one of the two identical rows.
        assert_eq!(rows, vec![row(), vec![Value::Integer(8)]]);
    }

    #[test]
    fn row_keys_fold_equal_values() {
        let columns = [0, 1];
        let a = [Value::Null, Value::Double(-0.0)];
        let b = [Value::Null, Value::Double(0.0)];
        let c = [Value::Null, Value::Double(f64::NAN)];
        let d = [
            Value::Null,
            Value::Double(f64::from_bits(0xfff8_0000_0000_0042)),
        ];
        assert_eq!(row_key(&a, &columns), row_key(&b, &columns));
        assert_eq!(row_key(&c, &columns), row_key(&d, &columns));
        assert_ne!(row_key(&a, &columns), row_key(&c, &columns));
    }

    #[test]
    fn watermark_spec_parses_and_rejects_malformed_entries() {
        let parsed = parse_watermark_spec(" main.events = ts , main.other=c2").unwrap();
        assert_eq!(parsed.get("main.events").map(String::as_str), Some("ts"));
        assert_eq!(parsed.get("main.other").map(String::as_str), Some("c2"));
        assert!(parse_watermark_spec("").unwrap().is_empty());
        for bad in ["main.events", "main.events=", "=ts", "events=ts"] {
            assert!(parse_watermark_spec(bad).is_err(), "{bad}");
        }
    }
}
