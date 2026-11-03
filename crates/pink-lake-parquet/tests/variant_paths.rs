//! VARIANT in a Parquet lake (wave 101b): the data files carry the `Variant`
//! logical type (shredded), the published statistics hold path-qualified
//! bounds, a path filter prunes whole files, and DuckDB reads the same file
//! and agrees on the values.
//!
//! Pruning is an optimization with a correctness obligation, so every case
//! also compares the lake's answer with the same rows in a plain store.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, Lake};
use pink_lake_parquet::{FORMAT_ID, register};
use pinkdb::{DataTable, PinkDb, QueryResult, Value};

/// The DuckDB reference binary, found like the other interop tests do:
/// `DUCKDB_BIN`, else `~/.local/bin/duckdb`. The interop tests skip when it is
/// absent (and the gate fails a run that skipped them).
fn duckdb_path() -> std::path::PathBuf {
    std::env::var_os("DUCKDB_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/bin/duckdb")
        })
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_parquet_variant_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn variant(json: &str) -> Value {
    Value::Variant(Arc::new(
        pinkdb::variant::from_json_str(json).expect("valid test JSON"),
    ))
}

fn row(id: i32, a: i32) -> Vec<Value> {
    vec![
        Value::Integer(id),
        variant(&format!("{{\"a\": {a}, \"tag\": \"t{id}\"}}")),
    ]
}

fn store(rows: &[(i32, i32)]) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, v VARIANT);")
        .unwrap();
    for (id, a) in rows {
        db.table_mut("t").unwrap().append_row(row(*id, *a)).unwrap();
    }
    db
}

fn sub_table(rows: &[(i32, i32)]) -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("v".to_string(), pinkdb::DataType::Variant),
        ],
    )
    .unwrap();
    for (id, a) in rows {
        table.append_row(row(*id, *a)).unwrap();
    }
    Arc::new(table)
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop() {
        Some(QueryResult::Selected { rows, .. }) => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn ids(rows: Vec<Vec<Value>>) -> Vec<i32> {
    let mut ids: Vec<i32> = rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Integer(id) => id,
            ref other => panic!("{other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Run a DuckDB script; `None` when the reference binary is absent. Returns
/// `(success, stdout, stderr)`.
fn duckdb(script: &str) -> Option<(bool, String, String)> {
    if !duckdb_path().exists() {
        return None;
    }
    let output = Command::new(duckdb_path())
        .args(["-no-agent", "-noheader", "-list", "-separator", "\t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            writeln!(child.stdin.as_mut().unwrap(), "{script}")?;
            child.wait_with_output()
        })
        .unwrap();
    Some((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    ))
}

/// One `ducklake_file_variant_stats` row, without the engine-specific
/// columns (`data_file_id`, `column_size_bytes`, …).
type StatsRow = (String, String, i64, i64, Option<String>, Option<String>);

/// The variant-statistic rows of a SQLite catalog, ordered by path and bounds.
/// DuckDB's SQLite scanner reads the catalog directly, so the test reads what
/// DuckDB would read in the same session.
fn stats_rows(catalog: &Path) -> Vec<StatsRow> {
    let script = format!(
        "INSTALL sqlite; LOAD sqlite;\n\
         ATTACH '{}' AS raw (TYPE SQLITE, READ_ONLY);\n\
         SELECT variant_path, shredded_type, value_count, null_count, min_value, max_value\n\
         FROM raw.ducklake_file_variant_stats\n\
         ORDER BY variant_path, min_value, max_value;\n",
        catalog.display()
    );
    let Some((ok, out, err)) = duckdb(&script) else {
        panic!("the interop tests need {}", duckdb_path().display());
    };
    assert!(ok, "reading {catalog:?} failed: {err}");
    out.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let cells: Vec<&str> = line.split('\t').collect();
            assert_eq!(cells.len(), 6, "{line}");
            let bound = |cell: &str| (cell != "NULL").then(|| cell.to_string());
            (
                cells[0].to_string(),
                cells[1].to_string(),
                cells[2].parse().unwrap(),
                cells[3].parse().unwrap(),
                bound(cells[4]),
                bound(cells[5]),
            )
        })
        .collect()
}

/// A `(id, v)` store whose VARIANT column holds the given JSON texts.
fn json_store(rows: &[(i32, String)]) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, v VARIANT);")
        .unwrap();
    for (id, json) in rows {
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(*id), variant(json)])
            .unwrap();
    }
    db
}

/// The JSON text of the parity fixture row: a string, a boolean and an
/// integer path, all present in every row.
fn parity_json(i: i32) -> String {
    format!("{{\"i\": {i}, \"s\": \"v{i}\", \"flag\": {}}}", i % 2 == 0)
}

/// The `(id, v)` DataTable form of [`parity_json`] rows.
fn parity_table(rows: &[i32]) -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("v".to_string(), pinkdb::DataType::Variant),
        ],
    )
    .unwrap();
    for i in rows {
        table
            .append_row(vec![Value::Integer(*i), variant(&parity_json(*i))])
            .unwrap();
    }
    Arc::new(table)
}

/// The rows of one attached query plus the lake's decoded data-file count.
fn attached_query(lake_dir: &Path, sql: &str) -> (Vec<Vec<Value>>, u64) {
    let report = Lake::open(lake_dir).unwrap().attach(None).unwrap();
    let lake = Arc::clone(&report.lake);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let rows = query(&mut db, sql);
    (rows, lake.decode_stats().2)
}

/// Three files whose `a` ranges are disjoint, so a path filter decodes only
/// the files that can hold the value.
#[test]
fn parquet_lake_prunes_paths() {
    register().unwrap();
    let dir = scratch("prune");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    // The shredder types a path only when it is common (`MIN_OCCURRENCES = 32`
    // and half the rows), so each file holds enough rows to qualify.
    let file = |base: i32| -> Vec<(i32, i32)> { (0..40).map(|i| (base + i, base + i)).collect() };
    let first = file(0);
    let second = file(100);
    let third = file(200);
    lake.write_from_store(&store(&first)).unwrap();
    for rows in [&second, &third] {
        let mut commit = Commit::new();
        commit.append("main.t", sub_table(rows));
        lake.commit(&commit).unwrap();
    }

    let all: Vec<(i32, i32)> = first
        .iter()
        .chain(second.iter())
        .chain(third.iter())
        .copied()
        .collect();
    let mut plain = store(&all);

    let cases: Vec<(&str, u64)> = vec![
        ("SELECT id FROM t WHERE v['a'] = 105;", 1),
        ("SELECT id FROM t WHERE variant_extract(v, '$.a') = 105;", 1),
        ("SELECT id FROM t WHERE v['a'] = 1000;", 0),
        ("SELECT id FROM t WHERE v['a'] >= 190;", 1),
        ("SELECT id FROM t WHERE v['a'] >= 130 AND v['a'] < 210;", 2),
        ("SELECT id FROM t WHERE v['a'] < 0;", 0),
        ("SELECT id FROM t WHERE v['a'] IS NULL;", 0),
        (
            "SELECT id FROM t WHERE variant_extract(v, '$.a') IS NOT NULL;",
            3,
        ),
        ("SELECT id FROM t WHERE v['a'] <> 105;", 3),
        ("SELECT id FROM t WHERE id = 105;", 1),
    ];
    for (sql, want_files) in cases {
        let (got, files) = attached_query(&lake_dir, sql);
        assert_eq!(
            ids(got),
            ids(query(&mut plain, sql)),
            "{sql}: the pruned answer must equal the unpruned one"
        );
        assert_eq!(files, want_files, "{sql}: decoded data files");
    }
    cleanup(&dir);
}

/// DuckDB's own DuckLake reader attaches the same lake's SQLite catalog and
/// agrees on the values: the path statistics live in DuckLake's
/// `ducklake_file_variant_stats` table, the table DuckDB itself writes.
#[test]
fn duckdb_attaches_a_lake_with_path_statistics() {
    if !duckdb_path().exists() {
        return;
    }
    register().unwrap();
    let dir = scratch("ducklake");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store(&(0..40).map(|i| (i, i)).collect::<Vec<_>>()))
        .unwrap();
    let mut commit = Commit::new();
    commit.append(
        "main.t",
        sub_table(&(100..140).map(|i| (i, i)).collect::<Vec<_>>()),
    );
    lake.commit(&commit).unwrap();
    drop(lake);

    let script = format!(
        "LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         SELECT count(*), sum(variant_extract(v, 'a')::INTEGER) FROM l.t;\n\
         SELECT count(*) FROM l.t WHERE variant_extract(v, 'a') = 105;\n",
        catalog_path.display(),
        data.display()
    );
    let output = Command::new(duckdb_path())
        .args(["-no-agent", "-noheader", "-list", "-separator", "\t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            writeln!(child.stdin.as_mut().unwrap(), "{script}")?;
            child.wait_with_output()
        })
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("extension") || stderr.contains("Extension") {
        println!("skip DuckDB DuckLake test: {stderr}");
        cleanup(&dir);
        return;
    }
    assert!(output.status.success(), "duckdb refused the lake: {stderr}");
    let text = String::from_utf8_lossy(&output.stdout);
    // 80 rows, `a` = 0..39 then 100..139 sums to 5560, and 105 appears once.
    assert_eq!(text.trim(), "80\t5560\n1", "{text}");
    cleanup(&dir);
}

/// The statistics pinkdb publishes are the rows DuckDB itself publishes for
/// the paths it shreds: one `ducklake_file_variant_stats` row per path per
/// data file, DuckDB's path spelling, DuckDB's type names, plain text bounds.
#[test]
fn variant_statistics_match_duckdbs_own_rows() {
    register().unwrap();
    let dir = scratch("parity");
    let duck_dir = dir.join("duck");
    std::fs::create_dir_all(&duck_dir).unwrap();
    let duck_catalog = duck_dir.join("cat.db");
    let mut script = format!(
        "INSTALL ducklake; LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         CREATE TABLE l.t (id INTEGER, v VARIANT);\n\
         CALL ducklake_set_option('l', 'data_inlining_row_limit', 0);\n",
        duck_catalog.display(),
        duck_dir.join("data").display()
    );
    for base in [0, 100, 200] {
        script.push_str(&format!(
            "INSERT INTO l.t SELECT i, json_object('i', i, 's', 'v' || i, \
             'flag', i % 2 = 0)::VARIANT FROM range({}, {}) t(i);\n",
            base,
            base + 40
        ));
    }
    let Some((ok, out, err)) = duckdb(&script) else {
        println!("skip parity test: DuckDB is not present");
        cleanup(&dir);
        return;
    };
    if err.contains("extension") || err.contains("Extension") {
        println!("skip parity test: DuckDB has no ducklake extension: {err}");
        cleanup(&dir);
        return;
    }
    assert!(ok, "duckdb refused to write the lake: {err}");
    assert!(out.trim().is_empty(), "{out}");

    // The same fixture rows, written by pinkdb.
    let pink_dir = dir.join("pink");
    std::fs::create_dir_all(&pink_dir).unwrap();
    let pink_catalog = format!("sqlite:{}", pink_dir.join("cat.db").display());
    let mut lake =
        Lake::create_with_catalog(&pink_catalog, pink_dir.join("data").to_str().unwrap()).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.set_data_inlining_row_limit(0);
    let first: Vec<i32> = (0..40).collect();
    lake.write_from_store(&json_store(
        &first
            .iter()
            .map(|i| (*i, parity_json(*i)))
            .collect::<Vec<_>>(),
    ))
    .unwrap();
    for base in [100, 200] {
        let mut commit = Commit::new();
        commit.append(
            "main.t",
            parity_table(&(base..base + 40).collect::<Vec<_>>()),
        );
        lake.commit(&commit).unwrap();
    }
    drop(lake);

    let duck_rows = stats_rows(&duck_catalog);
    let pink_rows = stats_rows(&pink_dir.join("cat.db"));
    // `flag` and `s` agree exactly, file by file: 3 files × 2 paths.
    let shared = |rows: &[StatsRow]| -> Vec<StatsRow> {
        rows.iter()
            .filter(|row| row.0 == "\"flag\"" || row.0 == "\"s\"")
            .cloned()
            .collect()
    };
    assert_eq!(duck_rows.len(), 6, "DuckDB's own rows: {duck_rows:#?}");
    assert_eq!(
        shared(&duck_rows),
        shared(&pink_rows),
        "DuckDB: {duck_rows:#?}"
    );
    assert!(
        duck_rows
            .iter()
            .any(|row| row.0 == "\"s\"" && row.1 == "varchar" && row.4.as_deref() == Some("v0")),
        "{duck_rows:#?}"
    );
    // The integer path is pinkdb's alone: DuckDB shreds it into a typed leaf
    // but publishes no row for it, and ignores the row pinkdb adds.
    assert!(
        !duck_rows.iter().any(|row| row.0 == "\"i\""),
        "{duck_rows:#?}"
    );
    let ints: Vec<&StatsRow> = pink_rows.iter().filter(|row| row.1 == "int64").collect();
    assert_eq!(ints.len(), 3, "{pink_rows:#?}");
    assert_eq!(ints[0].4.as_deref(), Some("0"));
    assert_eq!(ints[0].5.as_deref(), Some("39"));
    cleanup(&dir);
}

/// pinkdb prunes files using path statistics **DuckDB** wrote: a lake built by
/// DuckDB's own DuckLake writer re-attaches here, and a filter on the string
/// path decodes only the file whose bounds can hold the literal.
#[test]
fn pinkdb_prunes_files_duckdb_wrote() {
    register().unwrap();
    let dir = scratch("duckdb-stats");
    let duck_dir = dir.join("duck");
    std::fs::create_dir_all(&duck_dir).unwrap();
    let duck_catalog = duck_dir.join("cat.db");
    let mut script = format!(
        "INSTALL ducklake; LOAD ducklake;\n\
         ATTACH 'ducklake:sqlite:{}' AS l (DATA_PATH '{}', OVERRIDE_DATA_PATH true);\n\
         CREATE TABLE l.t (id INTEGER, v VARIANT);\n\
         CALL ducklake_set_option('l', 'data_inlining_row_limit', 0);\n",
        duck_catalog.display(),
        duck_dir.join("data").display()
    );
    for base in [0, 100, 200] {
        script.push_str(&format!(
            // Zero-padded so lexicographic bounds order like the numbers do.
            "INSERT INTO l.t SELECT i, json_object('s', 'v' || printf('%03d', i), \
             'flag', true)::VARIANT FROM range({}, {}) t(i);\n",
            base,
            base + 40
        ));
    }
    let Some((ok, _, err)) = duckdb(&script) else {
        println!("skip DuckDB-authored lake test: DuckDB is not present");
        cleanup(&dir);
        return;
    };
    if err.contains("extension") || err.contains("Extension") {
        println!("skip DuckDB-authored lake test: no ducklake extension: {err}");
        cleanup(&dir);
        return;
    }
    assert!(ok, "duckdb refused to write the lake: {err}");
    // DuckDB published the string path; without those rows pinkdb has nothing
    // to prune on.
    let rows = stats_rows(&duck_catalog);
    assert!(
        rows.iter()
            .any(|row| row.0 == "\"s\"" && row.1 == "varchar"),
        "{rows:#?}"
    );

    // The lake opens at DuckLake's recorded `data_path` (`duck/data/`), and
    // each file resolves under it through its schema and table paths, as
    // DuckDB wrote it (`main/t/…`).
    let url = format!("sqlite:{}", duck_catalog.display());
    let lake = Lake::open_with_catalog(&url, None).unwrap();
    // A fresh attachment per query, so the decode counter is per query.
    let run = |sql: &str| -> (Vec<Vec<Value>>, u64) {
        let report = lake.attach(None).unwrap();
        let attached = Arc::clone(&report.lake);
        let mut db = PinkDb::new();
        db.attach_lake(report.lake).unwrap();
        (query(&mut db, sql), attached.decode_stats().2)
    };
    let (queried, files) = run("SELECT id FROM t WHERE v['s'] = 'v105';");
    assert_eq!(ids(queried), vec![105], "the pruned answer");
    assert_eq!(files, 1, "one file decoded");
    let (queried, files) = run("SELECT count(*) FROM t WHERE v['s'] = 'v9999';");
    assert_eq!(queried[0][0], Value::Bigint(0), "nothing holds v9999");
    assert_eq!(files, 0, "every file pruned");
    cleanup(&dir);
}

/// DuckDB reads a VARIANT column out of a parquet *lake* file — the same
/// footer, shredded schema and dictionary a `COPY … TO '….parquet'` writes.
#[test]
fn duckdb_reads_a_lake_file() {
    if !duckdb_path().exists() {
        return;
    }
    register().unwrap();
    let dir = scratch("duckdb");
    let lake_dir = dir.join("lake");
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.write_from_store(&store(&(0..40).map(|i| (i, i * 2)).collect::<Vec<_>>()))
        .unwrap();
    // The catalog file is `<lake>`, its data lives in the `<lake>.files` sidecar.
    let file = std::fs::read_dir(
        PathBuf::from(format!("{}.files", lake_dir.display())).join("data/main.t"),
    )
    .unwrap()
    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
    .min()
    .expect("a parquet data file");
    let output = Command::new(duckdb_path())
        .args(["-no-agent", "-noheader", "-list", "-separator", "\t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            writeln!(
                child.stdin.as_mut().unwrap(),
                "SELECT count(*), sum(variant_extract(v, 'a')::INTEGER), \
                 count(variant_extract(v, 'tag')) FROM read_parquet('{}');",
                file.display()
            )?;
            child.wait_with_output()
        })
        .unwrap();
    assert!(
        output.status.success(),
        "duckdb refused the lake file: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    // 40 rows, `a` = 0, 2, …, 78 sums to 1560, every row has a `tag`.
    assert_eq!(text.trim(), "40\t1560\t40", "{text}");
    cleanup(&dir);
}
