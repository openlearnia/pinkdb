//! The agent-memory distillation worker (wave 121; wave 109's deferral).
//!
//! Wave 109's memory extension never calls a model on the path of an add: a
//! turn is appended, and `.memory distill` later turns pending turns into
//! atoms, scenarios and a persona. On a server nobody types `.memory
//! distill`, so this optional worker does it on an interval
//! (`--memory-distill-every SECS`, off by default).
//!
//! Each tick finds the owners with pending queue jobs or undistilled turns and
//! runs `.memory distill` **as each owner** (a connection impersonating the
//! user, so the extension's per-caller filtering sees exactly that user's
//! rows), under [`pinkdb::serve::memory_policies`]. A store without users has
//! one implicit owner, served by a trusted connection. The provider comes
//! from the server's environment, as for the CLI; with none configured every
//! tick reports the same clear error once and the queue waits.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use pinkdb::serve::{CommandEvents, ServePolicy, memory_policies};
use pinkdb::{Database, ExecLimits, QueryResult, Value};

/// A running worker; dropping the handle does not stop it, [`MemoryWorker::stop`] does.
pub struct MemoryWorker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MemoryWorker {
    /// Start draining every `every` (the first drain happens after one
    /// interval).
    pub fn spawn(db: Database, every: Duration) -> std::io::Result<MemoryWorker> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("pinkdb-memory-distill".to_string())
            .spawn(move || {
                let mut last_error = String::new();
                loop {
                    // Sleep in small steps so a stop is prompt.
                    let mut slept = Duration::ZERO;
                    while slept < every {
                        if flag.load(Ordering::Relaxed) {
                            return;
                        }
                        let step = Duration::from_millis(200).min(every - slept);
                        std::thread::sleep(step);
                        slept += step;
                    }
                    match drain_once(&db) {
                        Ok(lines) => {
                            last_error.clear();
                            for line in lines {
                                eprintln!("pinkdb-server: memory: {line}");
                            }
                        }
                        Err(message) => {
                            if message != last_error {
                                eprintln!("pinkdb-server: memory distillation: {message}");
                                last_error = message;
                            }
                        }
                    }
                }
            })?;
        Ok(MemoryWorker {
            stop,
            thread: Some(thread),
        })
    }

    /// Stop the worker and wait for its current drain to finish.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Collects a command's output.
#[derive(Default)]
struct Collect {
    answer: String,
    diagnostic: String,
}

impl CommandEvents for Collect {
    fn emit(&mut self, channel: i32, text: &str) {
        if channel == 0 {
            self.answer.push_str(text);
        } else {
            self.diagnostic.push_str(text);
        }
    }
}

/// The owners with work waiting. Memory's layers live in the host's state
/// store when one is attached (wave 121e), else in the store itself.
fn owners(db: &Database) -> Result<Vec<String>, String> {
    let mut conn = db.state_store().unwrap_or_else(|| db.clone()).connect();
    let sql = format!(
        "SELECT DISTINCT owner FROM {m}.distill_queue WHERE state = 'pending' \
         UNION SELECT DISTINCT owner FROM {m}.l0_turns WHERE distilled = false",
        m = pinkdb::serve::MEMORY_DATABASE
    );
    match conn.execute(&sql) {
        Ok(mut results) => match results.pop() {
            Some(QueryResult::Selected { rows, .. }) => Ok(rows
                .into_iter()
                .filter_map(|row| match row.into_iter().next() {
                    Some(Value::Varchar(owner)) if !owner.is_empty() => Some(owner),
                    _ => None,
                })
                .collect()),
            _ => Ok(Vec::new()),
        },
        // No memory tables yet: nothing has been added.
        Err(e) if e.to_string().contains("not found") => Ok(Vec::new()),
        Err(e) => Err(e.to_string()),
    }
}

/// One drain: every owner with work, in turn. Returns the report lines.
pub fn drain_once(db: &Database) -> Result<Vec<String>, String> {
    if !db
        .command_extensions()
        .iter()
        .any(|name| name.eq_ignore_ascii_case("memory"))
    {
        return Err("the memory extension is not loaded (--memory-extension)".to_string());
    }
    let base = ServePolicy {
        limits: ExecLimits {
            statement_timeout: Some(Duration::from_secs(60)),
            max_result_rows: Some(1_000_000),
            max_result_bytes: None,
        },
        ..ServePolicy::default()
    };
    let (tool, internal) = memory_policies(&base);
    let mut lines = Vec::new();
    for owner in owners(db)? {
        let mut conn = db.connect();
        if db.has_users() {
            // An owner that is no longer a user (dropped, or the implicit
            // name from before the store had users) is left alone.
            if conn.impersonate(&owner).is_err() {
                continue;
            }
            conn.require_login(true);
        }
        let mut out = Collect::default();
        let result = conn.run_command("memory", "distill", &tool, &internal, &mut out);
        match result {
            Ok(()) => {
                let report = out.answer.trim();
                if !report.is_empty() {
                    lines.push(format!("{owner}: {report}"));
                }
            }
            Err(_) => {
                let reason = out.diagnostic.trim();
                return Err(if reason.is_empty() {
                    "the distill command failed".to_string()
                } else {
                    reason.to_string()
                });
            }
        }
    }
    Ok(lines)
}
