//! End-to-end check of the fixture corpus: one file per status.

use std::path::{Path, PathBuf};
use std::process::Command;

use pinkdb_sqllogictest::report::{FileResult, FileStatus, Report};
use pinkdb_sqllogictest::runner::{Config, run_file};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn run(directory: &Path, filter: &str, out: &Path) -> Report {
    let output = Command::new(env!("CARGO_BIN_EXE_pinkdb-slt"))
        .arg("--corpus")
        .arg(directory)
        .arg("--filter")
        .arg(filter)
        .arg("--jobs")
        .arg("1")
        .arg("--file-timeout")
        .arg("300")
        .arg("--out")
        .arg(out)
        .env("PINKDB_SLT_TEST_HOOK", "1")
        .output()
        .expect("run pinkdb-slt");
    assert!(
        output.status.success(),
        "pinkdb-slt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Report::load(out).expect("report")
}

fn file<'a>(report: &'a Report, name: &str) -> &'a FileResult {
    report
        .files
        .iter()
        .find(|file| file.path == name)
        .unwrap_or_else(|| panic!("{name} is missing from the report"))
}

/// The runner crate's own fixture corpus goes through the real library path
/// (not the CLI's file walker) so the statuses are checked at the API too.
#[test]
fn fixtures_cover_every_status() {
    // The crash fixture panics on purpose; its message is the report's job.
    std::panic::set_hook(Box::new(|_| {}));
    let directory = fixtures();
    let temp = std::env::temp_dir().join(format!("pinkdb-slt-fixtures-{}", std::process::id()));
    let config = Config {
        statement_timeout: std::time::Duration::from_secs(10),
        file_timeout: std::time::Duration::from_millis(300),
        temp_root: temp.clone(),
        duckdb_root: directory
            .parent()
            .and_then(Path::parent)
            .unwrap_or(Path::new("."))
            .to_path_buf(),
        test_hook: true,
        run_id: "fixtures".to_string(),
    };
    let mut statuses = Vec::new();
    for name in [
        "pass.test",
        "fail.test",
        "skip_unknown.test",
        "skip_require.test",
        "crash.test",
        "timeout.test",
    ] {
        let path = directory.join(name);
        let result = run_file(&config, &path, name);
        assert!(
            result.duration_ms < 2_500,
            "{name} did not respect the file timeout: {} ms",
            result.duration_ms
        );
        statuses.push((name, result));
    }
    let _ = std::fs::remove_dir_all(&temp);

    let status = |name: &str| {
        statuses
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, result)| result.status)
            .unwrap()
    };
    assert_eq!(status("pass.test"), FileStatus::Pass);
    assert_eq!(status("fail.test"), FileStatus::Fail);
    assert_eq!(status("skip_unknown.test"), FileStatus::Skip);
    assert_eq!(status("skip_require.test"), FileStatus::Skip);
    assert_eq!(status("crash.test"), FileStatus::Crash);
    assert_eq!(status("timeout.test"), FileStatus::Timeout);

    let pass = &statuses[0].1;
    assert_eq!(pass.records.pass, 4);
    assert_eq!(pass.records.fail, 0);
    let fail = &statuses[1].1;
    assert_eq!((fail.records.pass, fail.records.fail), (1, 1));
    let first = fail.first_failure.as_ref().unwrap();
    assert_eq!(first.expected, "41");
    assert_eq!(first.got, "42");
    let unknown = statuses[2].1.reason.as_deref().unwrap_or_default();
    assert_eq!(unknown, "unsupported directive: test-env");
    let require = statuses[3].1.reason.as_deref().unwrap_or_default();
    assert_eq!(require, "require nonexistent_extension");
    assert!(
        statuses[4]
            .1
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("hook")
    );
}

#[test]
fn cli_report_and_baseline_check() {
    let directory = fixtures();
    let temp = std::env::temp_dir().join(format!("pinkdb-slt-cli-{}", std::process::id()));
    let all = temp.join("all.json");
    let report = run(&directory, "*.test", &all);
    assert_eq!(report.files.len(), 6);
    assert_eq!(file(&report, "pass.test").status, FileStatus::Pass);

    // Flip one file's status to exercise both directions of `check` with an
    // otherwise identical file set.
    let mut regressed_report = Report::load(&all).unwrap();
    let pass = regressed_report
        .files
        .iter_mut()
        .find(|file| file.path == "pass.test")
        .unwrap();
    pass.status = FileStatus::Pass;
    let baseline = temp.join("baseline.json");
    regressed_report.write(&baseline, true).unwrap();

    let mut failed = Report::load(&all).unwrap();
    let pass = failed
        .files
        .iter_mut()
        .find(|file| file.path == "pass.test")
        .unwrap();
    pass.status = FileStatus::Fail;
    pass.reason = Some("query result mismatch".to_string());
    let current_regressed = temp.join("current-regressed.json");
    failed.write(&current_regressed, false).unwrap();

    let check = |current: &Path, baseline: &Path| {
        Command::new(env!("CARGO_BIN_EXE_pinkdb-slt"))
            .arg("check")
            .arg(current)
            .arg("--baseline")
            .arg(baseline)
            .output()
            .expect("check")
    };
    let regressed = check(&current_regressed, &baseline);
    assert_eq!(regressed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&regressed.stdout).contains("regressed: pass.test"));

    let failed_baseline = temp.join("failed-baseline.json");
    failed.write(&failed_baseline, true).unwrap();
    let improved = check(&all, &failed_baseline);
    assert_eq!(improved.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&improved.stdout).contains("newly passing: pass.test"));

    let _ = std::fs::remove_dir_all(&temp);
}
