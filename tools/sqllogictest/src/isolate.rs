//! Process isolation for the file runner.
//!
//! `runner::run_file` contains a panic, but not a stack overflow or an abort:
//! those end the process, and with it every other file's result. So each file
//! runs in a child of this binary — the hidden `--run-batch` mode, one job on
//! stdin, one `FileResult` JSON line per file on stdout — and the parent
//! records a child that dies without a result as `crash`, with its exit
//! status and stderr tail.
//!
//! One child per file: measured startup is ~7 ms (see
//! `docs/sqllogictest-baseline.md`), so batching would save little and lose
//! the direct mapping from a dead child to the file that killed it.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::report::{FileResult, FileStatus, Records};
use crate::runner::{Config, run_file};

/// The hidden child mode; not in `--help`, it is this module's protocol.
pub const CHILD_FLAG: &str = "--run-batch";

/// The child's stdin: the shared config plus the files to run.
#[derive(Serialize, Deserialize)]
struct Job {
    config: Config,
    /// `(corpus-relative path, absolute path)`.
    files: Vec<(String, PathBuf)>,
}

/// Run the job on stdin, one `FileResult` JSON line per file on stdout.
pub fn run_child() -> std::process::ExitCode {
    let mut input = String::new();
    if let Err(error) = std::io::stdin().read_to_string(&mut input) {
        eprintln!("pinkdb-slt: could not read the job: {error}");
        return std::process::ExitCode::from(2);
    }
    let job: Job = match serde_json::from_str(&input) {
        Ok(job) => job,
        Err(error) => {
            eprintln!("pinkdb-slt: could not parse the job: {error}");
            return std::process::ExitCode::from(2);
        }
    };
    for (rel, path) in &job.files {
        let result = run_file(&job.config, path, rel);
        let line = match serde_json::to_string(&result) {
            Ok(line) => line,
            Err(error) => {
                eprintln!("pinkdb-slt: could not encode the result: {error}");
                return std::process::ExitCode::from(2);
            }
        };
        let mut stdout = std::io::stdout().lock();
        // A failed write means the parent killed us or is gone; stop early,
        // the parent has already recorded this file.
        if writeln!(stdout, "{line}")
            .and_then(|()| stdout.flush())
            .is_err()
        {
            return std::process::ExitCode::FAILURE;
        }
    }
    std::process::ExitCode::SUCCESS
}

/// Run one file in a child process. The child's result is passed through
/// unchanged; a child that dies without one is a `crash`, and one that is
/// still running at the file deadline is killed and is a `timeout`.
pub fn run_file_isolated(cfg: &Config, path: &Path, rel: &str) -> FileResult {
    let started = Instant::now();
    // The child gets no cutoff of its own: the parent's kill is what bounds a
    // file that ignores its own deadline, and it stops the process rather than
    // leaving a worker thread behind.
    let mut child_cfg = cfg.clone();
    child_cfg.hard_timeout = Duration::from_secs(24 * 60 * 60);
    let job = Job {
        config: child_cfg,
        files: vec![(rel.to_string(), path.to_path_buf())],
    };
    let job = match serde_json::to_string(&job) {
        Ok(job) => job,
        Err(error) => return failure(rel, format!("could not encode the job: {error}")),
    };
    let binary = match std::env::current_exe() {
        Ok(binary) => binary,
        Err(error) => return failure(rel, format!("could not find this binary: {error}")),
    };
    let mut child = match Command::new(binary)
        .arg(CHILD_FLAG)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => return failure(rel, format!("could not start the file runner: {error}")),
    };
    if let Some(mut stdin) = child.stdin.take() {
        // A broken pipe means the child already died; its status says how.
        let _ = stdin.write_all(job.as_bytes());
    }
    let stderr = child.stderr.take();
    let stderr = std::thread::spawn(move || {
        let mut text = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut text);
        }
        text
    });
    let (sender, receiver) = mpsc::channel();
    let stdout = child.stdout.take();
    let reader = std::thread::spawn(move || {
        if let Some(stdout) = stdout {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        }
    });

    let deadline = cfg.file_timeout + Duration::from_secs(2);
    match receiver.recv_timeout(deadline) {
        Ok(line) => {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            let _ = stderr.join();
            match serde_json::from_str::<FileResult>(&line) {
                Ok(result) => result,
                Err(error) => failure(rel, format!("could not read the child's result: {error}")),
            }
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            let _ = stderr.join();
            FileResult {
                path: rel.to_string(),
                status: FileStatus::Timeout,
                reason: Some(format!(
                    "file timeout: exceeded {} ms",
                    cfg.file_timeout.as_millis()
                )),
                records: Records::default(),
                first_failure: None,
                notes: vec!["child process killed at the file deadline".to_string()],
                duration_ms: started.elapsed().as_millis() as u64,
            }
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let status = child.wait();
            let tail = stderr.join().ok().and_then(|text| stderr_tail(&text));
            let _ = reader.join();
            let status = match status {
                Ok(status) => status.to_string(),
                Err(error) => format!("unwaitable child: {error}"),
            };
            let reason = match tail {
                Some(tail) => format!("child process ended with {status}: {tail}"),
                None => format!("child process ended with {status}"),
            };
            FileResult {
                path: rel.to_string(),
                status: FileStatus::Crash,
                reason: Some(reason),
                records: Records::default(),
                first_failure: None,
                notes: Vec::new(),
                duration_ms: started.elapsed().as_millis() as u64,
            }
        }
    }
}

/// The last few non-empty stderr lines: where the runtime puts the cause
/// (`fatal runtime error: stack overflow, aborting`) and its note.
fn stderr_tail(stderr: &str) -> Option<String> {
    let lines: Vec<&str> = stderr
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let tail = lines[lines.len().saturating_sub(3)..].join(" | ");
    if tail.is_empty() {
        None
    } else {
        Some(tail.chars().take(300).collect())
    }
}

fn failure(rel: &str, reason: String) -> FileResult {
    FileResult {
        path: rel.to_string(),
        status: FileStatus::Fail,
        reason: Some(reason),
        records: Records::default(),
        first_failure: None,
        notes: Vec::new(),
        duration_ms: 0,
    }
}
