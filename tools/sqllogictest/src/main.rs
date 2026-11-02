//! `pinkdb-slt`: run DuckDB sqllogictest files against pinkdb and report the
//! pass rate.
//!
//! ```text
//! pinkdb-slt --corpus <dir> [--filter <glob>] [--jobs N] --out <file.json>
//! pinkdb-slt summary <file.json>
//! pinkdb-slt check <file.json> --baseline <baseline.json>
//! ```

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use pinkdb_sqllogictest::report::{FileStatus, Report, check, summary};
use pinkdb_sqllogictest::runner::{Config, run_file};

fn main() -> ExitCode {
    // A pinkdb panic is a per-file `crash` result, not a reason to dump a
    // backtrace per record; the message is captured in the report.
    std::panic::set_hook(Box::new(|_| {}));
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("summary") => summary_command(&args[1..]),
        Some("check") => check_command(&args[1..]),
        Some("--help" | "-h" | "help") => {
            print_help();
            ExitCode::SUCCESS
        }
        _ => run_command(&args),
    }
}

struct RunArgs {
    corpus: PathBuf,
    filter: Option<String>,
    jobs: usize,
    out: Option<PathBuf>,
    baseline_out: Option<PathBuf>,
    minimal: bool,
    slow: bool,
    statement_timeout: Duration,
    file_timeout: Duration,
    list: bool,
}

fn parse_run_args(args: &[String]) -> Result<RunArgs, String> {
    let mut parsed = RunArgs {
        corpus: PathBuf::new(),
        filter: None,
        jobs: std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1),
        out: None,
        baseline_out: None,
        minimal: false,
        slow: false,
        statement_timeout: Duration::from_secs(10),
        file_timeout: Duration::from_secs(60),
        list: false,
    };
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        let mut value = |name: &str| -> Result<String, String> {
            index += 1;
            args.get(index)
                .cloned()
                .ok_or_else(|| format!("{name} needs a value"))
        };
        match arg.as_str() {
            "--corpus" => parsed.corpus = PathBuf::from(value("--corpus")?),
            "--filter" => parsed.filter = Some(value("--filter")?),
            "--jobs" => {
                parsed.jobs = value("--jobs")?
                    .parse()
                    .map_err(|_| "--jobs needs a number".to_string())?;
            }
            "--out" => parsed.out = Some(PathBuf::from(value("--out")?)),
            "--baseline-out" => parsed.baseline_out = Some(PathBuf::from(value("--baseline-out")?)),
            "--statement-timeout" => {
                parsed.statement_timeout =
                    millis(&value("--statement-timeout")?, "--statement-timeout")?;
            }
            "--file-timeout" => {
                parsed.file_timeout = millis(&value("--file-timeout")?, "--file-timeout")?;
            }
            "--minimal" => parsed.minimal = true,
            "--slow" => parsed.slow = true,
            "--list" => parsed.list = true,
            other => return Err(format!("unknown argument {other}")),
        }
        index += 1;
    }
    if parsed.corpus.as_os_str().is_empty() {
        return Err("--corpus is required".to_string());
    }
    if parsed.jobs == 0 {
        return Err("--jobs must be at least 1".to_string());
    }
    Ok(parsed)
}

fn millis(value: &str, name: &str) -> Result<Duration, String> {
    value
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| format!("{name} needs milliseconds"))
}

fn run_command(args: &[String]) -> ExitCode {
    let args = match parse_run_args(args) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("pinkdb-slt: {error}");
            return ExitCode::from(2);
        }
    };
    let files = match collect_files(&args) {
        Ok(files) => files,
        Err(error) => {
            eprintln!("pinkdb-slt: {error}");
            return ExitCode::from(2);
        }
    };
    if args.list {
        for (rel, _) in &files {
            println!("{rel}");
        }
        return ExitCode::SUCCESS;
    }
    // `corpus` is `.../duckdb/test/sql`; DuckDB's runner uses the checkout
    // root as its cwd, which is where `data/...` lives.
    let duckdb_root = args
        .corpus
        .parent()
        .and_then(Path::parent)
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let temp_root = std::env::temp_dir().join(format!("pinkdb-slt-{}", std::process::id()));
    let config = Config {
        statement_timeout: args.statement_timeout,
        file_timeout: args.file_timeout,
        temp_root: temp_root.clone(),
        duckdb_root,
        run_id: format!("{}-{}", std::process::id(), temp_root.display()),
        test_hook: std::env::var_os("PINKDB_SLT_TEST_HOOK").is_some(),
    };
    if files.is_empty() {
        eprintln!("pinkdb-slt: no test files matched");
        return ExitCode::from(2);
    }

    let started = Instant::now();
    // pinkdb's memory limit is process-wide (wave 136a), while DuckDB's is per
    // database: a file that sets it would squeeze every file running beside
    // it. Those files run one at a time after the parallel phase, each
    // followed by a reset of the limit.
    let (serial, parallel): (Vec<usize>, Vec<usize>) =
        (0..files.len()).partition(|&i| sets_process_limit(&files[i].1));
    let next = AtomicUsize::new(0);
    let (sender, receiver) = mpsc::channel();
    let mut results = std::thread::scope(|scope| {
        for _ in 0..args.jobs.min(parallel.len()) {
            let sender = sender.clone();
            let files = &files;
            let parallel = &parallel;
            let config = &config;
            let next = &next;
            scope.spawn(move || {
                loop {
                    let slot = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&index) = parallel.get(slot) else {
                        break;
                    };
                    let (rel, path) = &files[index];
                    let result = run_file(config, path, rel);
                    if sender.send((index, result)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(sender);
        let mut results = Vec::new();
        for result in receiver {
            results.push(result);
        }
        results
    });
    for index in serial {
        let (rel, path) = &files[index];
        results.push((index, run_file(&config, path, rel)));
        pinkdb::memory::reset_limit();
    }
    let wall_ms = started.elapsed().as_millis() as u64;
    let mut results: Vec<_> = results.into_iter().collect();
    results.sort_by_key(|(index, _)| *index);
    let report = Report {
        statement_timeout_ms: config.statement_timeout.as_millis() as u64,
        file_timeout_ms: config.file_timeout.as_millis() as u64,
        wall_ms,
        files: results.into_iter().map(|(_, result)| result).collect(),
    };

    if let Some(out) = &args.out {
        if let Err(error) = report.write(out, args.minimal) {
            eprintln!("pinkdb-slt: could not write {}: {error}", out.display());
            return ExitCode::from(2);
        }
    } else if args.baseline_out.is_none() {
        print!("{}", summary(&report));
    }
    if let Some(baseline_out) = &args.baseline_out
        && let Err(error) = report.write(baseline_out, true)
    {
        eprintln!(
            "pinkdb-slt: could not write {}: {error}",
            baseline_out.display()
        );
        return ExitCode::from(2);
    }
    let _ = std::fs::remove_dir_all(&temp_root);

    let mut counts = std::collections::BTreeMap::new();
    for file in &report.files {
        *counts.entry(file.status).or_insert(0usize) += 1;
    }
    let count = |status: FileStatus| counts.get(&status).copied().unwrap_or(0);
    eprintln!(
        "pinkdb-slt: {} files in {:.1}s ({} pass, {} fail, {} skip, {} crash, {} timeout)",
        report.files.len(),
        wall_ms as f64 / 1000.0,
        count(FileStatus::Pass),
        count(FileStatus::Fail),
        count(FileStatus::Skip),
        count(FileStatus::Crash),
        count(FileStatus::Timeout),
    );
    ExitCode::SUCCESS
}

fn collect_files(args: &RunArgs) -> Result<Vec<(String, PathBuf)>, String> {
    let corpus = args
        .corpus
        .canonicalize()
        .map_err(|error| format!("could not open {}: {error}", args.corpus.display()))?;
    let filter = match &args.filter {
        Some(filter) => Some(
            glob::Pattern::new(filter)
                .map_err(|error| format!("bad --filter {filter}: {error}"))?,
        ),
        None => None,
    };
    let mut files = Vec::new();
    walk(&corpus, &corpus, &mut files)?;
    files.retain(|(rel, _)| {
        let is_slow = rel.ends_with(".test_slow");
        let is_test = rel.ends_with(".test") || is_slow;
        is_test
            && (args.slow || !is_slow)
            && filter.as_ref().is_none_or(|filter| filter.matches(rel))
    });
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(files)
}

fn walk(root: &Path, dir: &Path, files: &mut Vec<(String, PathBuf)>) -> Result<(), String> {
    for entry in std::fs::read_dir(dir)
        .map_err(|error| format!("could not read {}: {error}", dir.display()))?
    {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, files)?;
        } else {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            files.push((rel, path));
        }
    }
    Ok(())
}

fn summary_command(args: &[String]) -> ExitCode {
    let Some(path) = args.first() else {
        eprintln!("usage: pinkdb-slt summary <file.json>");
        return ExitCode::from(2);
    };
    match Report::load(Path::new(path)) {
        Ok(report) => {
            print!("{}", summary(&report));
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("pinkdb-slt: could not read {path}: {error}");
            ExitCode::from(2)
        }
    }
}

fn check_command(args: &[String]) -> ExitCode {
    let mut current = None;
    let mut baseline = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--baseline" => {
                index += 1;
                baseline = args.get(index).map(PathBuf::from);
            }
            path => current = Some(PathBuf::from(path)),
        }
        index += 1;
    }
    let (Some(current), Some(baseline)) = (current, baseline) else {
        eprintln!("usage: pinkdb-slt check <file.json> --baseline <baseline.json>");
        return ExitCode::from(2);
    };
    let load = |path: &Path| match Report::load(path) {
        Ok(report) => Some(report),
        Err(error) => {
            eprintln!("pinkdb-slt: could not read {}: {error}", path.display());
            None
        }
    };
    let (Some(current), Some(baseline)) = (load(&current), load(&baseline)) else {
        return ExitCode::from(2);
    };
    let outcome = check(&baseline, &current);
    for regression in &outcome.regressions {
        println!("regressed: {regression}");
    }
    for file in &outcome.newly_passing {
        println!("newly passing: {file}");
    }
    eprintln!(
        "pinkdb-slt: {} files compared, {} regressions, {} newly passing",
        outcome.compared,
        outcome.regressions.len(),
        outcome.newly_passing.len()
    );
    if outcome.regressions.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn print_help() {
    println!(
        "pinkdb-slt: run DuckDB sqllogictest files against pinkdb\n\n\
         usage:\n  \
         pinkdb-slt --corpus <dir> [--filter <glob>] [--jobs N] [--out <file.json>] [--slow]\n           \
         [--minimal] [--baseline-out <file.json>] [--statement-timeout <ms>] [--file-timeout <ms>]\n  \
         pinkdb-slt summary <file.json>\n  \
         pinkdb-slt check <file.json> --baseline <baseline.json>\n\n\
         --slow includes .test_slow files. --minimal writes status and skip reason only\n\
         (the baseline form). --list prints the selected files instead of running them."
    );
}

/// Whether a test file sets pinkdb's process-wide memory limit
/// (`memory_limit` or its alias `max_memory`).
fn sets_process_limit(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|text| {
        let text = text.to_ascii_lowercase();
        text.contains("memory_limit") || text.contains("max_memory")
    })
}
