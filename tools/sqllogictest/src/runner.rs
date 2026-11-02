//! Executes a parsed sqllogictest file against pinkdb.
//!
//! One `Database` per file (in memory unless the file `load`s a path), a
//! connection per connection label, and one statement at a time. Each
//! statement carries a per-statement deadline, and the file as a whole a
//! wall-clock deadline; a file that blows the latter is killed from the
//! outside (`run_file`'s worker channel) even if a statement ignores its
//! deadline.

use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once, mpsc};
use std::time::{Duration, Instant};

use pinkdb::{Connection, DataType, Database, ExecLimits, PinkDb, QueryResult, Value};

use crate::compare::{
    compare_result, format_value_typed, is_hash_result, load_result_file, prepare_values,
    result_hash,
};
use crate::parser::{
    CondOp, Condition, Node, NodeKind, ParseError, Query, Statement, StatementExpect, parse_script,
};
use crate::report::{FileResult, FileStatus, FirstFailure, Records};

/// The `STANDARD_VECTOR_SIZE` the corpus' `require vector_size` checks against.
const STANDARD_VECTOR_SIZE: usize = 2048;

#[derive(Debug, Clone)]
pub struct Config {
    pub statement_timeout: Duration,
    pub file_timeout: Duration,
    /// One directory below this is created per test file.
    pub temp_root: PathBuf,
    /// The duckdb checkout root; `data/...` paths, `{DATA_DIR}` and
    /// `{BUILD_DIRECTORY}` resolve here.
    pub duckdb_root: PathBuf,
    /// `{RUN_ID}`: stable for one invocation, like DuckDB's run id.
    pub run_id: String,
    /// Set by `PINKDB_SLT_TEST_HOOK`: makes the fixture's crash SQL panic in
    /// the runner (a runner test hook, not a pinkdb bug).
    pub test_hook: bool,
}

#[derive(Default)]
struct Progress {
    records: Records,
    first_failure: Option<FirstFailure>,
}

/// Run one file on a worker thread so the caller can enforce a hard file
/// timeout. The worker sends its finished `FileResult`; progress is shared so
/// a timed-out file still reports the records it got through.
pub fn run_file(cfg: &Config, path: &Path, rel: &str) -> FileResult {
    let progress = Arc::new(Mutex::new(Progress::default()));
    let worker_progress = Arc::clone(&progress);
    let panic_progress = Arc::clone(&progress);
    let (sender, receiver) = mpsc::channel();
    let worker_cfg = cfg.clone();
    let worker_path = path.to_path_buf();
    let worker_rel = rel.to_string();
    let handle = match std::thread::Builder::new()
        .name(format!("slt-{rel}"))
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_file_inner(&worker_cfg, &worker_path, &worker_rel, worker_progress)
            }));
            let result = match result {
                Ok(result) => result,
                Err(payload) => {
                    let progress = panic_progress.lock().expect("progress lock");
                    FileResult {
                        path: worker_rel,
                        status: FileStatus::Crash,
                        reason: Some(format!("panic: {}", panic_message(payload))),
                        records: progress.records,
                        first_failure: progress.first_failure.clone(),
                        notes: Vec::new(),
                        duration_ms: 0,
                    }
                }
            };
            let _ = sender.send(result);
        }) {
        Ok(handle) => handle,
        Err(error) => {
            return FileResult {
                path: rel.to_string(),
                status: FileStatus::Fail,
                reason: Some(format!("could not start file worker: {error}")),
                records: Records::default(),
                first_failure: None,
                notes: Vec::new(),
                duration_ms: 0,
            };
        }
    };

    let hard_timeout = cfg.file_timeout + Duration::from_secs(2);
    match receiver.recv_timeout(hard_timeout) {
        Ok(result) => {
            let _ = handle.join();
            result
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let progress = progress.lock().expect("progress lock");
            FileResult {
                path: rel.to_string(),
                status: FileStatus::Timeout,
                reason: Some(format!(
                    "file timeout: exceeded {} ms",
                    cfg.file_timeout.as_millis()
                )),
                records: progress.records,
                first_failure: progress.first_failure.clone(),
                notes: vec!["file worker did not stop within the deadline".to_string()],
                duration_ms: hard_timeout.as_millis() as u64,
            }
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => FileResult {
            path: rel.to_string(),
            status: FileStatus::Crash,
            reason: Some("file worker died without a result".to_string()),
            records: progress.lock().expect("progress lock").records,
            first_failure: None,
            notes: Vec::new(),
            duration_ms: 0,
        },
    }
}

fn run_file_inner(
    cfg: &Config,
    path: &Path,
    rel: &str,
    progress: Arc<Mutex<Progress>>,
) -> FileResult {
    let started = Instant::now();
    let temp_dir = cfg.temp_root.join(temp_dir_name(rel));
    if let Err(error) = fs::create_dir_all(&temp_dir) {
        return FileResult {
            path: rel.to_string(),
            status: FileStatus::Fail,
            reason: Some(format!("could not create {}: {error}", temp_dir.display())),
            records: Records::default(),
            first_failure: None,
            notes: Vec::new(),
            duration_ms: started.elapsed().as_millis() as u64,
        };
    }
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) => {
            return FileResult {
                path: rel.to_string(),
                status: FileStatus::Fail,
                reason: Some(format!("could not read {}: {error}", path.display())),
                records: Records::default(),
                first_failure: None,
                notes: Vec::new(),
                duration_ms: started.elapsed().as_millis() as u64,
            };
        }
    };

    let mut result = match parse_script(&source) {
        Ok(nodes) => {
            let mut runner = match Runner::new(cfg, rel, temp_dir.clone(), progress) {
                Ok(runner) => runner,
                Err(reason) => {
                    let _ = fs::remove_dir_all(&temp_dir);
                    return FileResult {
                        path: rel.to_string(),
                        status: FileStatus::Fail,
                        reason: Some(reason),
                        records: Records::default(),
                        first_failure: None,
                        notes: Vec::new(),
                        duration_ms: started.elapsed().as_millis() as u64,
                    };
                }
            };
            let stop = runner.execute(&nodes);
            runner.finish(stop)
        }
        Err(ParseError::Unsupported(_)) => FileResult {
            path: rel.to_string(),
            status: FileStatus::Skip,
            reason: Some(parse_reason(&source)),
            records: Records::default(),
            first_failure: None,
            notes: Vec::new(),
            duration_ms: 0,
        },
        Err(error @ ParseError::Syntax { .. }) => FileResult {
            path: rel.to_string(),
            status: FileStatus::Fail,
            reason: Some(error.to_string()),
            records: Records::default(),
            first_failure: None,
            notes: Vec::new(),
            duration_ms: 0,
        },
    };
    result.duration_ms = started.elapsed().as_millis() as u64;
    let _ = fs::remove_dir_all(&temp_dir);
    result
}

fn parse_reason(source: &str) -> String {
    parse_script(source)
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default()
}

fn temp_dir_name(rel: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rel.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Run an inline script (unit tests); same path as a file, minus the read.
#[cfg(test)]
fn run_text(cfg: &Config, rel: &str, source: &str) -> FileResult {
    let temp_dir = cfg.temp_root.join(temp_dir_name(rel));
    fs::create_dir_all(&temp_dir).expect("create temp dir");
    let progress = Arc::new(Mutex::new(Progress::default()));
    match parse_script(source) {
        Ok(nodes) => {
            let mut runner = Runner::new(cfg, rel, temp_dir, progress).expect("runner");
            let stop = runner.execute(&nodes);
            runner.finish(stop)
        }
        Err(error) => FileResult {
            path: rel.to_string(),
            status: match error {
                ParseError::Unsupported(_) => FileStatus::Skip,
                ParseError::Syntax { .. } => FileStatus::Fail,
            },
            reason: Some(error.to_string()),
            records: Records::default(),
            first_failure: None,
            notes: Vec::new(),
            duration_ms: 0,
        },
    }
}

#[derive(Debug)]
enum Stop {
    /// `halt`.
    Halt,
    /// The whole file is skipped (missing `require`, unknown directive).
    Skip(String),
    Fail {
        reason: String,
    },
    Timeout(String),
    Crash(String),
}

struct Runner<'a> {
    cfg: &'a Config,
    rel: &'a str,
    temp_dir: PathBuf,
    deadline: Instant,
    uuid: String,
    db: Option<Database>,
    db_path: Option<PathBuf>,
    connections: HashMap<String, Connection>,
    /// Loop variables and `set variable` values.
    vars: HashMap<String, String>,
    /// `require-env` values, used by `{NAME}` and `${NAME}` substitution.
    env: HashMap<String, String>,
    /// Query label -> first result hash.
    labels: HashMap<String, String>,
    skip_level: usize,
    /// Line of the record being executed, for the failure report.
    current_line: usize,
    records: Records,
    first: Option<FirstFailure>,
    notes: Vec<String>,
    progress: Arc<Mutex<Progress>>,
}

impl<'a> Runner<'a> {
    fn new(
        cfg: &'a Config,
        rel: &'a str,
        temp_dir: PathBuf,
        progress: Arc<Mutex<Progress>>,
    ) -> Result<Self, String> {
        let db = open_store(None)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Ok(Self {
            cfg,
            rel,
            temp_dir,
            deadline: Instant::now() + cfg.file_timeout,
            uuid: format!("{:016x}-{:016x}", now.as_nanos() as u64, std::process::id()),
            db: Some(db),
            db_path: None,
            connections: HashMap::new(),
            vars: HashMap::new(),
            env: HashMap::new(),
            labels: HashMap::new(),
            skip_level: 0,
            current_line: 0,
            records: Records::default(),
            first: None,
            notes: Vec::new(),
            progress,
        })
    }

    fn finish(mut self, stop: Stop) -> FileResult {
        // `failure`, `crash` and the timeout paths already counted the record.
        let (status, reason) = match stop {
            Stop::Halt => (FileStatus::Pass, None),
            Stop::Skip(reason) => (FileStatus::Skip, Some(reason)),
            Stop::Fail { reason } => (FileStatus::Fail, Some(reason)),
            Stop::Timeout(reason) => (FileStatus::Timeout, Some(reason)),
            Stop::Crash(reason) => (FileStatus::Crash, Some(reason)),
        };
        FileResult {
            path: self.rel.to_string(),
            status,
            reason,
            records: self.records,
            first_failure: self.first,
            notes: std::mem::take(&mut self.notes),
            duration_ms: 0,
        }
    }

    fn execute(&mut self, nodes: &[Node]) -> Stop {
        for node in nodes {
            self.current_line = node.line;
            if self.remaining().is_zero() {
                return Stop::Timeout(format!(
                    "file timeout: exceeded {} ms",
                    self.cfg.file_timeout.as_millis()
                ));
            }
            if self.conditions_skip(&node.conditions) {
                if matches!(node.kind, NodeKind::Statement(_) | NodeKind::Query(_)) {
                    self.records.skip += 1;
                    self.sync();
                }
                continue;
            }
            if self.skip_level > 0
                && !matches!(node.kind, NodeKind::ModeSkip { .. } | NodeKind::ModeUnskip)
            {
                if matches!(node.kind, NodeKind::Statement(_) | NodeKind::Query(_)) {
                    self.records.skip += 1;
                    self.sync();
                }
                continue;
            }
            match &node.kind {
                NodeKind::ModeSkip { .. } => self.skip_level += 1,
                NodeKind::ModeUnskip => {
                    self.skip_level = self.skip_level.saturating_sub(1);
                }
                NodeKind::Halt => return Stop::Halt,
                NodeKind::Require { name, arg } => {
                    if !require_present(name, arg.as_deref()) {
                        // DuckDB's reason is `require <name>`; keep the
                        // argument out so the summary groups by extension.
                        return Stop::Skip(format!("require {}", name.to_ascii_lowercase()));
                    }
                }
                NodeKind::RequireEnv { name, value } => match std::env::var(name) {
                    Err(_) => return Stop::Skip(format!("require-env {name}")),
                    Ok(actual) => {
                        if let Some(expected) = value
                            && &actual != expected
                        {
                            return Stop::Skip(format!("require-env {name} {expected}"));
                        }
                        self.env.insert(name.clone(), actual);
                    }
                },
                NodeKind::Sleep(duration) => {
                    let remaining = self.remaining();
                    if *duration >= remaining {
                        std::thread::sleep(remaining);
                        return Stop::Timeout(format!(
                            "file timeout: exceeded {} ms",
                            self.cfg.file_timeout.as_millis()
                        ));
                    }
                    std::thread::sleep(*duration);
                }
                NodeKind::Load { path, read_only } => {
                    if let Err(reason) = self.load(path, *read_only) {
                        let got = reason.clone();
                        return self.failure(
                            node.line,
                            &format!("load {}", node.line),
                            "",
                            &got,
                            reason,
                        );
                    }
                }
                NodeKind::Restart => {
                    if let Err(reason) = self.restart() {
                        let got = reason.clone();
                        return self.failure(node.line, "restart", "", &got, reason);
                    }
                }
                NodeKind::SetVariable { name, value } => {
                    let value = self.replace_keywords(value, false);
                    self.vars.insert(name.clone(), value);
                }
                NodeKind::ResetLabel { name } => {
                    self.labels.remove(name);
                }
                NodeKind::Statement(statement) => {
                    match catch_unwind(AssertUnwindSafe(|| self.run_statement(statement))) {
                        Ok(Ok(())) => {}
                        Ok(Err(stop)) => return stop,
                        Err(payload) => return self.crash(node.line, &statement.sql, payload),
                    }
                }
                NodeKind::Query(query) => {
                    match catch_unwind(AssertUnwindSafe(|| self.run_query(query))) {
                        Ok(Ok(())) => {}
                        Ok(Err(stop)) => return stop,
                        Err(payload) => return self.crash(node.line, &query.sql, payload),
                    }
                }
                NodeKind::Loop {
                    var,
                    values,
                    concurrent,
                    body,
                } => {
                    if *concurrent {
                        self.note("concurrentloop ran serially");
                    }
                    for value in values {
                        let names: Vec<&str> = var.split(',').map(str::trim).collect();
                        let replacements: Vec<&str> = value.split(',').collect();
                        let bindings: Vec<(String, String)> = if names.len() == 1 {
                            vec![(names[0].to_string(), value.clone())]
                        } else if names.len() == replacements.len() {
                            names
                                .iter()
                                .zip(replacements)
                                .map(|(name, value)| (name.to_string(), value.to_string()))
                                .collect()
                        } else {
                            return self.failure(
                                node.line,
                                var,
                                &format!("{} values", names.len()),
                                value,
                                format!(
                                    "foreach iterator {var} has {} names but {value:?} has {} values",
                                    names.len(),
                                    replacements.len()
                                ),
                            );
                        };
                        let saved: Vec<(String, Option<String>)> = bindings
                            .iter()
                            .map(|(name, _)| (name.clone(), self.vars.get(name).cloned()))
                            .collect();
                        for (name, value) in bindings {
                            self.vars.insert(name, value);
                        }
                        let stop = self.execute(body);
                        for (name, old) in saved {
                            match old {
                                Some(value) => {
                                    self.vars.insert(name, value);
                                }
                                None => {
                                    self.vars.remove(&name);
                                }
                            }
                        }
                        if !matches!(stop, Stop::Halt)
                            && let Err(stop) = stop_result(stop)
                        {
                            return stop;
                        }
                    }
                }
            }
        }
        Stop::Halt
    }

    fn conditions_skip(&self, conditions: &[Condition]) -> bool {
        for condition in conditions {
            let matched = match condition.op {
                None => {
                    condition.key.eq_ignore_ascii_case("duckdb")
                        || condition.key.eq_ignore_ascii_case("pinkdb")
                }
                Some(op) => {
                    let actual = self
                        .vars
                        .get(&condition.key)
                        .map(String::as_str)
                        .unwrap_or("");
                    compare_condition(actual, &condition.value, op)
                }
            };
            if matched == condition.skip_if {
                return true;
            }
        }
        false
    }

    fn run_statement(&mut self, statement: &Statement) -> Result<(), Stop> {
        let sql = self.replace_keywords(&statement.sql, true);
        if self.cfg.test_hook && sql.contains("pinkdb_slt_crash") {
            panic!("pinkdb-slt crash test hook");
        }
        let budget = self.statement_budget()?;
        let expected_raw = statement.expected_error.clone();
        let expected = self.replace_keywords(&expected_raw, false);
        let result = {
            let connection = self.connection(statement.connection.as_deref());
            connection.set_exec_limits(ExecLimits {
                statement_timeout: Some(budget),
                ..ExecLimits::default()
            });
            let result = connection.execute(&sql);
            connection.clear_exec_limits();
            result
        };
        match result {
            Ok(_) => {
                if statement.expect == StatementExpect::Error {
                    return self.fail_record(
                        &sql,
                        &quoted(&expected),
                        "the statement succeeded",
                        format!("expected an error containing {}", quoted(&expected)),
                    );
                }
                self.records.pass += 1;
                self.sync();
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                if is_timeout(&message) {
                    self.records.fail += 1;
                    self.sync();
                    return Err(Stop::Timeout(format!(
                        "statement timeout: exceeded {} ms",
                        budget.as_millis()
                    )));
                }
                match statement.expect {
                    StatementExpect::Ok => self.fail_record(
                        &sql,
                        "statement ok",
                        &short(&message),
                        format!("statement failed: {}", short(&message)),
                    ),
                    StatementExpect::Maybe => {
                        self.records.pass += 1;
                        self.sync();
                        Ok(())
                    }
                    StatementExpect::Error => {
                        if error_matches(&expected_raw, &expected, &message) {
                            self.records.pass += 1;
                            self.sync();
                            Ok(())
                        } else {
                            self.fail_record(
                                &sql,
                                &quoted(&expected),
                                &short(&message),
                                format!(
                                    "expected error containing {}, got {}",
                                    quoted(&expected),
                                    quoted(&short(&message))
                                ),
                            )
                        }
                    }
                }
            }
        }
    }

    fn run_query(&mut self, query: &Query) -> Result<(), Stop> {
        let sql = self.replace_keywords(&query.sql, true);
        let expected: Vec<String> = query
            .expected
            .iter()
            .map(|line| self.replace_keywords(line, false))
            .collect();
        if self.cfg.test_hook && sql.contains("pinkdb_slt_crash") {
            panic!("pinkdb-slt crash test hook");
        }
        let budget = self.statement_budget()?;
        let result = {
            let connection = self.connection(query.connection.as_deref());
            connection.set_exec_limits(ExecLimits {
                statement_timeout: Some(budget),
                ..ExecLimits::default()
            });
            let result = connection.execute(&sql);
            connection.clear_exec_limits();
            result
        };
        let results = match result {
            Ok(results) => results,
            Err(error) => {
                let message = error.to_string();
                if is_timeout(&message) {
                    self.records.fail += 1;
                    self.sync();
                    return Err(Stop::Timeout(format!(
                        "statement timeout: exceeded {} ms",
                        budget.as_millis()
                    )));
                }
                return self.fail_record(
                    &sql,
                    &expected.join("\n"),
                    &short(&message),
                    format!("query failed: {}", short(&message)),
                );
            }
        };
        let selected = results.into_iter().rev().find_map(|result| match result {
            QueryResult::Selected { types, rows, .. } => Some((types, rows)),
            // DuckDB's `query` directive also accepts a DML statement and
            // compares its changed-row count as one integer value.
            QueryResult::Inserted { count }
            | QueryResult::Updated { count }
            | QueryResult::Deleted { count } => Some((
                vec![DataType::Bigint],
                vec![vec![Value::Bigint(count as i64)]],
            )),
            _ => None,
        });
        let Some((types, rows)) = selected else {
            return self.fail_record(
                &sql,
                &expected.join("\n"),
                "no result set",
                "query returned no result set".to_string(),
            );
        };
        let formatted: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(index, value)| {
                        format_value_typed(value, types.get(index).unwrap_or(&DataType::Varchar))
                    })
                    .collect()
            })
            .collect();
        let got = render(&formatted);
        let values = prepare_values(formatted, query.sort);
        let hash = result_hash(&values);

        let mut hash_mode = query.label.is_some();
        let expect_hash = expected.len() == 1 && is_hash_result(&expected[0]);
        if expect_hash {
            hash_mode = true;
        }
        if hash_mode {
            if let Some(label) = &query.label {
                match self.labels.get(label) {
                    None => {
                        self.labels.insert(label.clone(), hash.clone());
                    }
                    Some(cached) if cached != &hash => {
                        let cached = cached.clone();
                        return self.fail_record(
                            &sql,
                            &cached,
                            &hash,
                            format!("label {label:?} hash changed"),
                        );
                    }
                    Some(_) => {}
                }
            }
            if expect_hash && expected[0] != hash {
                return self.fail_record(
                    &sql,
                    &expected[0],
                    &hash,
                    "result hash mismatch".to_string(),
                );
            }
        } else if let Some(file) = expected
            .first()
            .and_then(|line| line.strip_prefix("<FILE>:"))
        {
            if expected.len() != 1 {
                return self.fail_record(
                    &sql,
                    &expected.join("\n"),
                    &got,
                    "<FILE>: must be the only expected result line".to_string(),
                );
            }
            match self.expected_file(file) {
                Ok(file_values) => {
                    if let Err(reason) = compare_result(&file_values, &values, &types) {
                        return self.fail_record(&sql, &expected[0], &got, reason);
                    }
                }
                Err(reason) => return self.fail_record(&sql, &expected[0], &got, reason),
            }
        } else if let Err(reason) = compare_result(&expected, &values, &types) {
            return self.fail_record(&sql, &expected.join("\n"), &got, reason);
        }
        self.records.pass += 1;
        self.sync();
        Ok(())
    }

    /// Read a `<FILE>:` expected-result CSV. DuckDB's runner resolves these
    /// against its working directory, which is the duckdb checkout root.
    fn expected_file(&self, file: &str) -> Result<Vec<String>, String> {
        let path = PathBuf::from(file);
        let path = if path.is_absolute() {
            path
        } else {
            self.cfg.duckdb_root.join(path)
        };
        load_result_file(&path).map(|(values, _columns)| values)
    }

    fn statement_budget(&self) -> Result<Duration, Stop> {
        let remaining = self.remaining();
        let budget = self.cfg.statement_timeout.min(remaining);
        if budget.is_zero() {
            return Err(Stop::Timeout(format!(
                "file timeout: exceeded {} ms",
                self.cfg.file_timeout.as_millis()
            )));
        }
        Ok(budget)
    }

    fn connection(&mut self, name: Option<&str>) -> &mut Connection {
        let key = name.unwrap_or("").to_string();
        if !self.connections.contains_key(&key) {
            let connection = self.db.as_ref().expect("database is open").connect();
            self.connections.insert(key.clone(), connection);
        }
        self.connections.get_mut(&key).expect("just inserted")
    }

    fn load(&mut self, raw_path: &str, read_only: bool) -> Result<(), String> {
        let path = self.replace_keywords(raw_path, true);
        if read_only {
            self.note("load ... readonly opened read-write (pinkdb has no read-only mode)");
        }
        let resolved = if path.is_empty() {
            None
        } else {
            let raw = PathBuf::from(&path);
            Some(if raw.is_absolute() {
                raw
            } else {
                self.temp_dir.join(raw)
            })
        };
        if !read_only && let Some(path) = &resolved {
            delete_store_files(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("could not create {}: {error}", parent.display()))?;
            }
        }
        self.db_path = resolved;
        self.restart()
    }

    fn restart(&mut self) -> Result<(), String> {
        self.connections.clear();
        self.db = None;
        let db = open_store(self.db_path.as_deref())?;
        self.db = Some(db);
        Ok(())
    }

    /// DuckDB's `ReplaceKeywords`: loop variables, the path/environment
    /// keyword set, then environment variables. For SQL, relative `data/`
    /// paths resolve against the duckdb checkout (the cwd DuckDB's runner
    /// uses), because the corpus is read-only and we do not chdir.
    fn replace_keywords(&self, text: &str, sql: bool) -> String {
        let mut out = text.to_string();
        for (name, value) in &self.vars {
            out = out
                .replace(&format!("${{{name}}}"), value)
                .replace(&format!("{{{name}}}"), value);
        }
        let temp = self.temp_dir.to_string_lossy().into_owned();
        let root = self.cfg.duckdb_root.to_string_lossy().into_owned();
        let temp_root = self.cfg.temp_root.to_string_lossy().into_owned();
        let data_dir = format!("{root}/data");
        let build_dir = format!("{root}/build/release");
        let name = format!("test/sql/{}", self.rel.trim_start_matches("./"));
        let no_slash = name.replace('/', "_");
        let test_id: String = name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                    ch
                } else {
                    '_'
                }
            })
            .collect();
        let catalog_dir = format!("{temp}/{}", self.uuid);
        for (name, value) in [
            ("BUILD_DIR", build_dir.as_str()),
            ("BUILD_DIRECTORY", build_dir.as_str()),
            ("CATALOG_DIR", catalog_dir.as_str()),
            ("DATA_DIR", data_dir.as_str()),
            ("LOCAL_DATA_DIR", data_dir.as_str()),
            ("LOCAL_TEMP_DIR", temp.as_str()),
            ("RUN_ID", self.cfg.run_id.as_str()),
            ("TEMP_DIR", temp.as_str()),
            ("TEMP_DIR_ABSOLUTE", temp.as_str()),
            ("TEMP_DIR_ROOT", temp_root.as_str()),
            ("TEST_DIR", temp.as_str()),
            ("TEST_ID", test_id.as_str()),
            ("TEST_NAME", name.as_str()),
            ("TEST_NAME__NO_SLASH", no_slash.as_str()),
            ("TEST_UUID", self.uuid.as_str()),
            ("UUID", self.uuid.as_str()),
            ("WORKING_DIR", root.as_str()),
            ("WORKING_DIRECTORY", root.as_str()),
            ("BASE_TEST_NAME", no_slash.as_str()),
            ("__TEST_DIR__", temp.as_str()),
            ("__WORKING_DIRECTORY__", root.as_str()),
            ("__BUILD_DIRECTORY__", build_dir.as_str()),
        ] {
            out = out
                .replace(&format!("${{{name}}}"), value)
                .replace(&format!("{{{name}}}"), value);
        }
        for (name, value) in &self.env {
            out = out
                .replace(&format!("${{{name}}}"), value)
                .replace(&format!("{{{name}}}"), value);
        }
        out = replace_env_vars(&out);
        if sql {
            out = out
                .replace("'data/", &format!("'{root}/data/"))
                .replace("\"data/", &format!("\"{root}/data/"));
        }
        out
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    fn note(&mut self, note: &str) {
        if !self.notes.iter().any(|existing| existing == note) {
            self.notes.push(note.to_string());
        }
    }

    fn sync(&self) {
        let mut progress = self.progress.lock().expect("progress lock");
        progress.records = self.records;
        progress.first_failure = self.first.clone();
    }

    fn crash(&mut self, line: usize, sql: &str, payload: Box<dyn std::any::Any + Send>) -> Stop {
        let message = panic_message(payload);
        self.records.fail += 1;
        let first = FirstFailure {
            line,
            sql: short(sql),
            expected: "no panic".to_string(),
            got: message.clone(),
        };
        self.first = Some(first);
        self.sync();
        Stop::Crash(format!("panic: {message}"))
    }

    fn failure(
        &mut self,
        line: usize,
        sql: &str,
        expected: &str,
        got: &str,
        reason: impl Into<String>,
    ) -> Stop {
        self.records.fail += 1;
        let first = FirstFailure {
            line,
            sql: short(sql),
            expected: short(expected),
            got: short(got),
        };
        self.first = Some(first);
        self.sync();
        Stop::Fail {
            reason: reason.into(),
        }
    }

    fn fail_record(
        &mut self,
        sql: &str,
        expected: &str,
        got: &str,
        reason: impl Into<String>,
    ) -> Result<(), Stop> {
        Err(self.failure(self.current_line, sql, expected, got, reason))
    }
}

/// Loop bodies return `Stop::Halt` for a normal end (the same sentinel the
/// top level maps to pass); turn that into `Ok` and keep other stops.
fn stop_result(stop: Stop) -> Result<(), Stop> {
    match stop {
        Stop::Halt => Ok(()),
        other => Err(other),
    }
}

fn open_store(path: Option<&Path>) -> Result<Database, String> {
    prepare_extensions();
    let mut db = match path {
        Some(path) => {
            let path = path
                .to_str()
                .ok_or_else(|| format!("database path {} is not UTF-8", path.display()))?;
            PinkDb::open_durable(path).map_err(|error| error.to_string())?
        }
        None => PinkDb::new(),
    };
    #[cfg(feature = "json")]
    let _ = db.load_linked_extension("json");
    #[cfg(feature = "datetime")]
    let _ = db.load_linked_extension("datetime");
    #[cfg(feature = "math")]
    let _ = db.load_linked_extension("math");
    #[cfg(feature = "string")]
    let _ = db.load_linked_extension("string");
    #[cfg(feature = "parquet")]
    let _ = db.load_linked_extension("parquet");
    Ok(Database::from_store(db))
}

/// Register the linked extension packages exactly once per process; the
/// per-store load above is what makes their functions visible to SQL.
fn prepare_extensions() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        #[cfg(feature = "json")]
        let _ = pinkdb_json::ensure_registered();
        #[cfg(feature = "datetime")]
        let _ = pinkdb_datetime::ensure_registered();
        #[cfg(feature = "math")]
        let _ = pinkdb_math::ensure_registered();
        #[cfg(feature = "string")]
        let _ = string_ext::ensure_registered();
        #[cfg(feature = "parquet")]
        pinkdb_parquet::ensure_registered();
    });
}

fn require_present(name: &str, arg: Option<&str>) -> bool {
    match name.to_ascii_lowercase().as_str() {
        "json" => cfg!(feature = "json"),
        "parquet" => cfg!(feature = "parquet"),
        "datetime" => cfg!(feature = "datetime"),
        "math" => cfg!(feature = "math"),
        "string" => cfg!(feature = "string"),
        // DuckDB settings/build properties that are true of a normal build.
        "skip_reload"
        | "notwindows"
        | "notmingw"
        | "notmusl"
        | "64bit"
        | "noforcestorage"
        | "no_force_storage"
        | "no_alternative_verify"
        | "no_extension_autoloading"
        | "no_vector_verification"
        | "no_latest_storage"
        | "notthreadsan"
        | "strinline"
        | "core_functions" => true,
        "vector_size" => arg
            .and_then(|arg| arg.parse::<usize>().ok())
            .is_some_and(|size| size <= STANDARD_VECTOR_SIZE),
        "exact_vector_size" => arg
            .and_then(|arg| arg.parse::<usize>().ok())
            .is_some_and(|size| size == STANDARD_VECTOR_SIZE),
        // `block_size`, `ram`, `disk_space`, and every extension we do not
        // link (icu, httpfs, sqlite_scanner, ...) are missing.
        _ => false,
    }
}

fn compare_condition(actual: &str, expected: &str, op: CondOp) -> bool {
    if let (Ok(actual), Ok(expected)) = (actual.parse::<i64>(), expected.parse::<i64>()) {
        return match op {
            CondOp::Eq => actual == expected,
            CondOp::Ne => actual != expected,
            CondOp::Gt => actual > expected,
            CondOp::Ge => actual >= expected,
            CondOp::Lt => actual < expected,
            CondOp::Le => actual <= expected,
        };
    }
    match op {
        CondOp::Eq => actual == expected,
        CondOp::Ne => actual != expected,
        CondOp::Gt => actual > expected,
        CondOp::Ge => actual >= expected,
        CondOp::Lt => actual < expected,
        CondOp::Le => actual <= expected,
    }
}

fn replace_env_vars(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(['{', '$']) {
        let (prefix, candidate) = rest.split_at(start);
        out.push_str(prefix);
        let (name, consumed) = if let Some(after) = candidate.strip_prefix("${") {
            match after.find('}') {
                Some(end) => (&after[..end], end + 3),
                None => ("", 0),
            }
        } else if let Some(after) = candidate.strip_prefix('{') {
            match after.find('}') {
                Some(end) => (&after[..end], end + 2),
                None => ("", 0),
            }
        } else {
            ("", 0)
        };
        let valid = !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if valid && let Ok(value) = std::env::var(name) {
            out.push_str(&value);
            rest = &rest[consumed..];
        } else {
            let ch = candidate.chars().next().expect("a brace is one byte");
            out.push(ch);
            rest = &candidate[ch.len_utf8()..];
        }
    }
    out.push_str(rest);
    out
}

fn is_timeout(message: &str) -> bool {
    message.contains("statement timeout: exceeded")
}

fn error_matches(expected_raw: &str, expected: &str, message: &str) -> bool {
    if expected_raw.is_empty() {
        return true;
    }
    if message.contains(expected_raw) || message.contains(expected) {
        return true;
    }
    let (want, pattern) = if let Some(pattern) = expected_raw.strip_prefix("<REGEX>:") {
        (true, pattern)
    } else if let Some(pattern) = expected_raw.strip_prefix("<!REGEX>:") {
        (false, pattern)
    } else {
        return false;
    };
    let Ok(regex) = regex::Regex::new(&format!("(?s)^(?:{pattern})$")) else {
        return false;
    };
    regex.is_match(message) == want
}

fn delete_store_files(path: &Path) {
    for suffix in ["", ".wal", ".lock", ".tmp"] {
        let candidate = if suffix.is_empty() {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = fs::remove_file(candidate);
    }
}

fn render(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn quoted(text: &str) -> String {
    format!("{text:?}")
}

fn short(text: &str) -> String {
    let first_line = text.lines().next().unwrap_or(text).trim();
    if first_line.chars().count() <= 300 {
        first_line.to_string()
    } else {
        format!("{}…", first_line.chars().take(300).collect::<String>())
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_config() -> Config {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("pinkdb-slt-unit-{}-{id}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        Config {
            statement_timeout: Duration::from_secs(10),
            file_timeout: Duration::from_secs(30),
            temp_root: root,
            duckdb_root: std::env::temp_dir(),
            run_id: "unit-run".to_string(),
            test_hook: false,
        }
    }

    fn run(source: &str) -> FileResult {
        let cfg = test_config();
        let result = run_text(&cfg, "inline.test", source);
        let _ = fs::remove_dir_all(&cfg.temp_root);
        result
    }

    #[test]
    fn placeholders_resolve_the_duckdb_environment() {
        let cfg = test_config();
        let data = format!("{}/data", cfg.duckdb_root.display());
        let root = cfg.temp_root.display();
        let source = format!(
            "query TTTTT\nSELECT '{{DATA_DIR}}', '{{TEST_NAME}}', '{{TEST_ID}}', '{{TEMP_DIR_ROOT}}', '{{TEST_DIR}}'\n----\n{data}\ttest/sql/inline.test\ttest_sql_inline_test\t{root}\t{{TEMP_DIR}}\n"
        );
        let result = run_text(&cfg, "inline.test", &source);
        let _ = fs::remove_dir_all(&cfg.temp_root);
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
    }

    #[test]
    fn expected_results_can_come_from_a_csv_file() {
        let cfg = test_config();
        let file = cfg
            .duckdb_root
            .join(format!("pinkdb-slt-file-result-{}.csv", std::process::id()));
        fs::write(&file, "\"a\"|\"b\"\n1|2\n3|4\n").unwrap();
        let source = format!(
            "query II rowsort\nSELECT * FROM (VALUES (1, 2), (3, 4)) t(a, b)\n----\n<FILE>:{}\n",
            file.display()
        );
        let result = run_text(&cfg, "inline.test", &source);
        let _ = fs::remove_file(&file);
        let _ = fs::remove_dir_all(&cfg.temp_root);
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
    }

    #[test]
    fn query_accepts_dml_changed_row_counts() {
        let result = run("statement ok\nCREATE TABLE t(i INTEGER)\n\n\
             query I\nINSERT INTO t VALUES (1), (2), (3)\n----\n3\n\n\
             query I\nUPDATE t SET i = i + 1\n----\n3\n\n\
             query I\nDELETE FROM t WHERE i > 2\n----\n2\n");
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.pass, 4);
    }

    #[test]
    fn runs_statements_and_queries() {
        let result = run("statement ok\nCREATE TABLE t(i INTEGER)\n\n\
             statement ok\nINSERT INTO t VALUES (1), (2)\n\n\
             query I rowsort\nSELECT i FROM t\n----\n1\n2\n\n\
             statement error not found\nSELECT * FROM missing\n----\nnot found\n");
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.pass, 4);
    }

    #[test]
    fn statement_maybe_accepts_both_outcomes() {
        let result = run("statement maybe\nSELECT * FROM missing\n----\nanything\n\n\
             statement maybe\nSELECT 1\n----\nanything\n");
        assert_eq!(result.status, FileStatus::Pass);
        assert_eq!(result.records.pass, 2);
    }

    #[test]
    fn loops_substitute_variables() {
        let result = run(
            "loop i 1 3\nstatement ok\nCREATE TABLE t${i}(x INTEGER)\n\nendloop\n\n\
             query I\nSELECT count(*) FROM t3\n----\n0\n",
        );
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.pass, 4);
    }

    #[test]
    fn labels_compare_hashes_and_reset() {
        let result = run(
            "query I rowsort a\nSELECT 2 UNION ALL SELECT 1\n----\n1\n2\n\n\
             query I rowsort a\nSELECT 1 UNION ALL SELECT 2\n----\nignored\n\n\
             reset label a\n\n\
             query I rowsort a\nSELECT 1 UNION ALL SELECT 2\n----\nignored\n",
        );
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.pass, 3);
    }

    #[test]
    fn label_mismatch_is_a_failure() {
        let result = run("query I rowsort a\nSELECT 1\n----\n1\n\n\
             query I rowsort a\nSELECT 2\n----\n2\n");
        assert_eq!(result.status, FileStatus::Fail);
        assert!(result.reason.unwrap().contains("label"));
    }

    #[test]
    fn explicit_hash_results_are_compared() {
        let result = run(
            "query I\nSELECT 2 UNION ALL SELECT 1\n----\n2 values hashing to 3ff46cfc7e84a3d0c8a2e9e7f6a0c7d3\n",
        );
        assert_eq!(result.status, FileStatus::Fail);
        assert!(result.reason.unwrap().contains("hash"));
    }

    #[test]
    fn error_substring_and_regex_matching() {
        let result = run(
            "statement error not found\nSELECT * FROM missing\n----\nnot found\n\n\
             statement error <REGEX>:.*not found.*\nSELECT * FROM missing\n----\n<REGEX>:.*not found.*\n\n\
             statement error <!REGEX>:.*parser.*\nSELECT * FROM missing\n----\n<!REGEX>:.*parser.*\n",
        );
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.pass, 3);
    }

    #[test]
    fn mode_skip_counts_records_and_set_variable_substitutes() {
        let result = run("set variable t 7\n\n\
             mode skip skipme\nstatement ok\nSELECT ${t}\n\n\
             query I\nSELECT ${t}\n----\n7\n\n\
             mode unskip\n\n\
             query I\nSELECT ${t}\n----\n7\n");
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.skip, 2);
        assert_eq!(result.records.pass, 1);
    }

    #[test]
    fn require_missing_skips_the_file_and_concurrentloop_is_serial() {
        let result = run("require icu\nstatement ok\nSELECT 1\n");
        assert_eq!(result.status, FileStatus::Skip);
        assert_eq!(result.reason.as_deref(), Some("require icu"));

        let result = run("concurrentloop i 1 2\nstatement ok\nSELECT ${i}\n\nendloop\n");
        assert_eq!(result.status, FileStatus::Pass);
        assert!(result.notes.iter().any(|note| note.contains("serially")));
    }

    #[test]
    fn load_and_restart_persist_a_file_backed_store() {
        let result = run(
            "load {TEST_DIR}/store.db\nstatement ok\nCREATE TABLE t(i INTEGER)\n\n\
             statement ok\nINSERT INTO t VALUES (1)\n\n\
             restart\n\n\
             query I\nSELECT count(*) FROM t\n----\n1\n",
        );
        assert_eq!(result.status, FileStatus::Pass, "{:?}", result.reason);
        assert_eq!(result.records.pass, 3);
    }

    #[test]
    fn timeout_is_reported_not_hung() {
        let mut cfg = test_config();
        cfg.file_timeout = Duration::from_millis(50);
        let result = run_text(&cfg, "sleep.test", "sleep 5 seconds\n");
        let _ = fs::remove_dir_all(&cfg.temp_root);
        assert_eq!(result.status, FileStatus::Timeout);
        assert!(result.duration_ms < 2_000);
    }
}
