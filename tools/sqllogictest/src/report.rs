//! The JSON report, its Markdown summary, and the baseline check.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileStatus {
    Pass,
    Fail,
    Skip,
    Crash,
    Timeout,
}

impl FileStatus {
    pub fn is_pass(self) -> bool {
        self == Self::Pass
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
            Self::Crash => "crash",
            Self::Timeout => "timeout",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Records {
    pub pass: u64,
    pub fail: u64,
    pub skip: u64,
}

impl Records {
    pub fn total(self) -> u64 {
        self.pass + self.fail + self.skip
    }

    pub fn add(&mut self, other: Self) {
        self.pass += other.pass;
        self.fail += other.fail;
        self.skip += other.skip;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstFailure {
    pub line: usize,
    pub sql: String,
    pub expected: String,
    pub got: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileResult {
    /// Relative to the corpus root, `/`-separated.
    pub path: String,
    pub status: FileStatus,
    /// Skip reason, or the first failure's reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub records: Records,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failure: Option<FirstFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    #[serde(default)]
    pub duration_ms: u64,
}

impl FileResult {
    /// The baseline form: path, status and the first failure/skip reason only.
    pub fn minimal(&self) -> Self {
        Self {
            path: self.path.clone(),
            status: self.status,
            reason: self.reason.clone(),
            records: Records::default(),
            first_failure: None,
            notes: Vec::new(),
            duration_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub statement_timeout_ms: u64,
    pub file_timeout_ms: u64,
    #[serde(default)]
    pub wall_ms: u64,
    pub files: Vec<FileResult>,
}

impl Report {
    pub fn write(&self, path: &Path, minimal: bool) -> std::io::Result<()> {
        let report = if minimal {
            Report {
                statement_timeout_ms: self.statement_timeout_ms,
                file_timeout_ms: self.file_timeout_ms,
                wall_ms: self.wall_ms,
                files: self.files.iter().map(FileResult::minimal).collect(),
            }
        } else {
            self.clone()
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(path)?;
        serde_json::to_writer(file, &report).map_err(std::io::Error::other)
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
        serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
    }
}

/// Counters for one group of files.
#[derive(Debug, Default)]
struct Area {
    files: u64,
    pass: u64,
    fail: u64,
    skip: u64,
    crash: u64,
    timeout: u64,
    records: Records,
}

impl Area {
    fn add(&mut self, file: &FileResult) {
        self.files += 1;
        self.records.add(file.records);
        match file.status {
            FileStatus::Pass => self.pass += 1,
            FileStatus::Fail => self.fail += 1,
            FileStatus::Skip => self.skip += 1,
            FileStatus::Crash => self.crash += 1,
            FileStatus::Timeout => self.timeout += 1,
        }
    }

    fn file_rate(&self) -> f64 {
        percent(self.pass, self.files)
    }

    fn run_file_rate(&self) -> f64 {
        percent(self.pass, self.files - self.skip)
    }

    fn record_rate(&self) -> f64 {
        percent(self.records.pass, self.records.pass + self.records.fail)
    }
}

fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * part as f64 / total as f64
    }
}

fn area_keys(path: &str) -> (String, Option<String>) {
    let mut parts = path.split('/');
    let first = parts.next().unwrap_or("(root)");
    match parts.next() {
        Some(second) => (first.to_string(), Some(format!("{first}/{second}"))),
        None => (first.to_string(), None),
    }
}

/// The Markdown summary: pass rates per directory, grouped failure and skip
/// reasons. Record rates exclude skipped records; file rates are shown both
/// over every file and over the files that actually ran.
pub fn summary(report: &Report) -> String {
    let mut overall = Area::default();
    let mut top: BTreeMap<String, Area> = BTreeMap::new();
    let mut second: BTreeMap<String, Area> = BTreeMap::new();
    let mut failures: HashMap<String, u64> = HashMap::new();
    let mut skips: HashMap<String, u64> = HashMap::new();

    for file in &report.files {
        overall.add(file);
        let (top_key, second_key) = area_keys(&file.path);
        top.entry(top_key).or_default().add(file);
        if let Some(second_key) = second_key {
            second.entry(second_key).or_default().add(file);
        }
        match file.status {
            FileStatus::Skip => {
                *skips
                    .entry(file.reason.clone().unwrap_or_default())
                    .or_default() += 1
            }
            FileStatus::Fail | FileStatus::Crash | FileStatus::Timeout => {
                *failures
                    .entry(normalize_reason(
                        file.reason.as_deref().unwrap_or("unknown failure"),
                    ))
                    .or_default() += 1
            }
            FileStatus::Pass => {}
        }
    }

    let mut out = String::new();
    out.push_str(&format!(
        "Overall: {} files, {} pass, {} fail, {} skip, {} crash, {} timeout; \
         {:.1}% of files pass ({:.1}% of files that ran).\n\n",
        overall.files,
        overall.pass,
        overall.fail,
        overall.skip,
        overall.crash,
        overall.timeout,
        overall.file_rate(),
        overall.run_file_rate(),
    ));
    out.push_str(&format!(
        "Records: {} pass, {} fail, {} skip ({:.1}% of non-skipped records pass). \
         Wall time: {:.1} s.\n\n",
        overall.records.pass,
        overall.records.fail,
        overall.records.skip,
        overall.record_rate(),
        report.wall_ms as f64 / 1000.0,
    ));

    out.push_str("## Pass rate per area\n\n| area | files | pass | fail | skip | crash | timeout | files pass | run files pass | records pass | records fail | record pass |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for (name, area) in &top {
        out.push_str(&area_row(name, area));
    }
    out.push('\n');
    out.push_str("### Second-level areas\n\n| area | files | pass | fail | skip | crash | timeout | files pass | run files pass | records pass | records fail | record pass |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for (name, area) in &second {
        out.push_str(&area_row(name, area));
    }

    out.push_str("\n## Top failure reasons\n\n");
    if failures.is_empty() {
        out.push_str("None.\n");
    } else {
        out.push_str("| files | reason |\n|---:|---|\n");
        let mut failures: Vec<(String, u64)> = failures.into_iter().collect();
        failures.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (reason, count) in failures.into_iter().take(30) {
            out.push_str(&format!("| {count} | {} |\n", markdown_escape(&reason)));
        }
    }

    out.push_str("\n## Top skip reasons\n\n");
    if skips.is_empty() {
        out.push_str("None.\n");
    } else {
        out.push_str("| files | reason |\n|---:|---|\n");
        let mut skips: Vec<(String, u64)> = skips.into_iter().collect();
        skips.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (reason, count) in skips.into_iter().take(20) {
            out.push_str(&format!("| {count} | {} |\n", markdown_escape(&reason)));
        }
    }
    out
}

fn area_row(name: &str, area: &Area) -> String {
    format!(
        "| {} | {} | {} | {} | {} | {} | {} | {:.1}% | {:.1}% | {} | {} | {:.1}% |\n",
        markdown_escape(name),
        area.files,
        area.pass,
        area.fail,
        area.skip,
        area.crash,
        area.timeout,
        area.file_rate(),
        area.run_file_rate(),
        area.records.pass,
        area.records.fail,
        area.record_rate(),
    )
}

/// Collapse the variable parts of an error message so the same cause groups
/// together: first line, digit runs replaced by `N`, whitespace collapsed.
pub fn normalize_reason(reason: &str) -> String {
    let first_line = reason.lines().next().unwrap_or(reason).trim();
    let mut out = String::with_capacity(first_line.len());
    let mut in_digits = false;
    let mut last_space = false;
    for ch in first_line.chars() {
        if ch.is_ascii_digit() {
            if !in_digits {
                out.push('N');
                in_digits = true;
            }
            last_space = false;
        } else if ch.is_whitespace() {
            if !last_space && !out.is_empty() {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(ch);
            in_digits = false;
            last_space = false;
        }
    }
    let out = out.trim_end().to_string();
    let mut chars = out.chars();
    let truncated: String = chars.by_ref().take(160).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn markdown_escape(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

/// The result of comparing a report against a baseline.
pub struct CheckOutcome {
    pub regressions: Vec<String>,
    pub newly_passing: Vec<String>,
    pub compared: usize,
}

impl CheckOutcome {
    pub fn is_ok(&self) -> bool {
        self.regressions.is_empty()
    }
}

pub fn check(baseline: &Report, current: &Report) -> CheckOutcome {
    let current: HashMap<&str, &FileResult> = current
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    let mut outcome = CheckOutcome {
        regressions: Vec::new(),
        newly_passing: Vec::new(),
        compared: baseline.files.len(),
    };
    for file in &baseline.files {
        let Some(now) = current.get(file.path.as_str()) else {
            if file.status.is_pass() {
                outcome.regressions.push(format!(
                    "{}: passed in the baseline, missing from this report",
                    file.path
                ));
            }
            continue;
        };
        match (file.status.is_pass(), now.status.is_pass()) {
            (true, false) => outcome.regressions.push(format!(
                "{}: pass -> {} ({})",
                file.path,
                status_name(now.status),
                now.reason.as_deref().unwrap_or("no reason")
            )),
            (false, true) => outcome.newly_passing.push(file.path.clone()),
            _ => {}
        }
    }
    outcome
}

fn status_name(status: FileStatus) -> &'static str {
    match status {
        FileStatus::Pass => "pass",
        FileStatus::Fail => "fail",
        FileStatus::Skip => "skip",
        FileStatus::Crash => "crash",
        FileStatus::Timeout => "timeout",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, status: FileStatus, reason: &str) -> FileResult {
        FileResult {
            path: path.to_string(),
            status,
            reason: Some(reason.to_string()),
            records: Records {
                pass: 3,
                fail: u64::from(!status.is_pass()),
                skip: 0,
            },
            first_failure: None,
            notes: Vec::new(),
            duration_ms: 1,
        }
    }

    fn report(files: Vec<FileResult>) -> Report {
        Report {
            statement_timeout_ms: 10_000,
            file_timeout_ms: 60_000,
            wall_ms: 1234,
            files,
        }
    }

    #[test]
    fn normalizes_variable_error_text() {
        assert_eq!(
            normalize_reason("value 12 differs: expected \"5\", got \"4\""),
            "value N differs: expected \"N\", got \"N\""
        );
        assert_eq!(
            normalize_reason("statement failed: Catalog Error: 42\nsecond line"),
            "statement failed: Catalog Error: N"
        );
    }

    #[test]
    fn summary_groups_areas_and_reasons() {
        let report = report(vec![
            file("a/x.test", FileStatus::Pass, ""),
            file("a/fail.test", FileStatus::Fail, "value 1 differs: x"),
            file("b/deep/y.test", FileStatus::Skip, "require parquet"),
        ]);
        let text = summary(&report);
        assert!(text.contains("Overall: 3 files, 1 pass, 1 fail, 1 skip"));
        assert!(text.contains("| a | 2 | 1 | 1 | 0 | 0 | 0 | 50.0% | 50.0% |"));
        assert!(text.contains("| b/deep | 1 | 0 | 0 | 1 | 0 | 0 |"));
        assert!(text.contains("value N differs: x"));
        assert!(text.contains("require parquet"));
    }

    #[test]
    fn check_detects_regressions_and_new_passes() {
        let baseline = report(vec![
            file("kept.test", FileStatus::Pass, ""),
            file("broken.test", FileStatus::Pass, ""),
            file("new.test", FileStatus::Skip, "require json"),
            file("gone.test", FileStatus::Pass, ""),
        ]);
        let current = report(vec![
            file("kept.test", FileStatus::Pass, ""),
            file("broken.test", FileStatus::Fail, "query failed: boom"),
            file("new.test", FileStatus::Pass, ""),
        ]);
        let outcome = check(&baseline, &current);
        assert_eq!(outcome.regressions.len(), 2);
        assert_eq!(outcome.newly_passing, vec!["new.test"]);
        assert!(!outcome.is_ok());
    }
}
