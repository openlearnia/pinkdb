//! VARIANT path statistics (wave 101b): the `.pink` lake publishes exact
//! min/max/null-count facts for every path its shredder typed, and a filter
//! on such a path (`v['a'] = 12`, `variant_extract(v, '$.a') = 12`) skips the
//! files whose facts exclude the literal.
//!
//! Pruning is an optimization with a correctness obligation: every case here
//! also compares the lake's answer with the same rows in a plain store.

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::Commit;
use pinkdb::{PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_variant_paths_{tag}_{}_{}",
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

/// A `(id, v)` store whose VARIANT column carries a common `a` path.
fn store(rows: &[(i32, i32)]) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, v VARIANT);")
        .unwrap();
    for (id, a) in rows {
        db.table_mut("t").unwrap().append_row(row(*id, *a)).unwrap();
    }
    db
}

fn sub_table(rows: &[(i32, i32)]) -> Arc<pinkdb::DataTable> {
    let mut t = pinkdb::DataTable::new(
        "t".to_string(),
        vec![
            ("id".to_string(), pinkdb::DataType::Integer),
            ("v".to_string(), pinkdb::DataType::Variant),
        ],
    )
    .unwrap();
    for (id, a) in rows {
        t.append_row(row(*id, *a)).unwrap();
    }
    Arc::new(t)
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

/// Rows and data-file decode counters of one query against a fresh attachment.
fn attached_query(lake_dir: &Path, sql: &str) -> (Vec<Vec<Value>>, u64) {
    let report = open_lake(lake_dir).unwrap().attach(None).unwrap();
    let lake = Arc::clone(&report.lake);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let rows = query(&mut db, sql);
    (rows, lake.decode_stats().2)
}

fn ids(rows: Vec<Vec<Value>>) -> Vec<i32> {
    let mut ids: Vec<i32> = rows
        .into_iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            ref other => panic!("{other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Three files whose `a` ranges are disjoint, so a path filter decodes only
/// the files that can hold the value.
fn three_files(lake_dir: &Path) -> Vec<(i32, i32)> {
    let mut lake = open_or_create_lake(lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    // The shredder only types a path that is common (`MIN_OCCURRENCES = 32`
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
    first
        .iter()
        .chain(second.iter())
        .chain(third.iter())
        .copied()
        .collect()
}

#[test]
fn path_statistics_prune_files() {
    let dir = scratch("prune");
    let lake_dir = dir.join("lake");
    let all = three_files(&lake_dir);
    let mut plain = store(&all);

    let cases: Vec<(&str, u64)> = vec![
        // One file's range holds 105; the other two are pruned.
        ("SELECT id FROM t WHERE v['a'] = 105;", 1),
        ("SELECT id FROM t WHERE variant_extract(v, '$.a') = 105;", 1),
        // Nothing holds 1000: every file is pruned.
        ("SELECT id FROM t WHERE v['a'] = 1000;", 0),
        // Above the first two files' maxima: only the third decodes.
        ("SELECT id FROM t WHERE v['a'] >= 190;", 1),
        // A range crossing two files decodes exactly those two.
        ("SELECT id FROM t WHERE v['a'] >= 130 AND v['a'] < 210;", 2),
        // Smaller than every file's minimum.
        ("SELECT id FROM t WHERE v['a'] < 0;", 0),
        // No row's `a` is missing or null, so IS NULL decodes nothing.
        ("SELECT id FROM t WHERE v['a'] IS NULL;", 0),
        (
            "SELECT id FROM t WHERE variant_extract(v, '$.a') IS NOT NULL;",
            3,
        ),
        // `<>` cannot be proven from a min/max range: every file decodes.
        ("SELECT id FROM t WHERE v['a'] <> 105;", 3),
        // A filter on a plain column is unaffected by path statistics.
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

/// A SQL catalog stores the facts in DuckLake's own
/// `ducklake_file_variant_stats` table, one row per path per data file, so a
/// lake reopened from the catalog still prunes.
#[cfg(feature = "catalog-sqlite")]
#[test]
fn path_statistics_survive_a_sql_catalog() {
    use rusqlite::Connection;

    let dir = scratch("catalog");
    let lake_dir = dir.join("lake");
    with_backend("sqlite", || {
        three_files(&lake_dir);
        // A fresh handle: nothing in memory knows the facts any more.
        let (rows, files) = attached_query(&lake_dir, "SELECT id FROM t WHERE v['a'] = 105;");
        assert_eq!(ids(rows), vec![105], "the pruned answer");
        assert_eq!(files, 1, "one file decoded from the SQL catalog's facts");
        let (rows, files) = attached_query(&lake_dir, "SELECT id FROM t WHERE v['a'] = 1000;");
        assert!(rows.is_empty(), "nothing holds 1000");
        assert_eq!(files, 0, "every file pruned");
        // DuckLake's own table carries one row per path per file, with the
        // file's exact bounds as text.
        let conn = Connection::open(lake_dir.join("catalog.sqlite")).unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT variant_path, shredded_type, value_count, null_count, min_value, max_value
                 FROM ducklake_file_variant_stats ORDER BY variant_path, min_value",
            )
            .unwrap();
        let rows: Vec<(String, String, i64, i64, String, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(rows.len(), 6, "two paths in each of three files: {rows:#?}");
        assert_eq!(
            rows[0],
            (
                "\"a\"".to_string(),
                "int64".to_string(),
                40,
                0,
                "0".to_string(),
                "39".to_string()
            ),
            "the first file's `a` bounds"
        );
        assert_eq!(rows[1].4, "100", "the second file's minimum");
        assert!(
            rows.iter()
                .all(|row| row.1 == "int64" || row.1 == "varchar")
        );
        assert!(
            rows.iter()
                .any(|row| row.0 == "\"tag\"" && row.1 == "varchar" && row.4 == "t0"),
            "the string path is published too: {rows:#?}"
        );
        assert_eq!(stmt.finalize(), Ok(()));
    });
    cleanup(&dir);
}

/// DuckLake's own table is the only place the facts live: expiring a snapshot
/// drops the variant-stat rows of the files it retires, exactly like the
/// column-stat rows.
#[cfg(feature = "catalog-sqlite")]
#[test]
fn expiring_a_snapshot_drops_its_variant_statistics() {
    use rusqlite::Connection;

    let dir = scratch("expire");
    let lake_dir = dir.join("lake");
    with_backend("sqlite", || {
        three_files(&lake_dir);
        let catalog = lake_dir.join("catalog.sqlite");
        let count = |sql: &str| -> i64 {
            let conn = Connection::open(&catalog).unwrap();
            conn.query_row(sql, [], |row| row.get(0)).unwrap()
        };
        assert_eq!(count("SELECT count(*) FROM ducklake_data_file"), 3);
        assert_eq!(count("SELECT count(*) FROM ducklake_file_variant_stats"), 6);
        // Compaction rewrites the three files into one and ends the old rows:
        // their facts stay behind until the snapshots that could see them go.
        let mut lake = open_lake(&lake_dir).unwrap();
        lake.merge_adjacent_files(
            None,
            &pink_lake::MergeSettings {
                min_file_size: Some(0),
                ..pink_lake::MergeSettings::default()
            },
        )
        .unwrap();
        assert_eq!(
            count("SELECT count(*) FROM ducklake_data_file WHERE end_snapshot IS NULL"),
            1,
            "the merged file is the only live one"
        );
        let stale = count("SELECT count(*) FROM ducklake_file_variant_stats");
        assert!(
            stale > 2,
            "the retired files still own their facts: {stale}"
        );
        // Retiring every version but the head drops the ended files' rows.
        let head = lake.current_version();
        let victims: Vec<u64> = lake
            .versions()
            .unwrap()
            .into_iter()
            .filter(|version| *version < head)
            .collect();
        assert!(!victims.is_empty(), "there is something to expire");
        lake.expire_snapshots(&pink_lake::ExpireSettings {
            versions: Some(victims),
            ..pink_lake::ExpireSettings::default()
        })
        .unwrap();
        drop(lake);
        assert_eq!(count("SELECT count(*) FROM ducklake_data_file"), 1);
        assert_eq!(
            count("SELECT count(*) FROM ducklake_file_variant_stats"),
            2,
            "only the live file's two paths remain"
        );
        assert_eq!(
            count("SELECT count(*) FROM ducklake_file_column_stats"),
            2,
            "the column statistics follow the same rule"
        );
    });
    cleanup(&dir);
}

/// The same facts round trip through a Postgres catalog: DuckLake's own table
/// carries them, a fresh attach prunes with them, and expire drops them.
#[cfg(feature = "catalog-postgres")]
#[test]
fn path_statistics_round_trip_through_a_postgres_catalog() {
    let Ok(base) = std::env::var("PINKDB_TEST_POSTGRES_URL") else {
        println!("skip Postgres variant-path test: PINKDB_TEST_POSTGRES_URL is not set");
        return;
    };
    let base = base.trim();
    if base.is_empty() {
        println!("skip Postgres variant-path test: PINKDB_TEST_POSTGRES_URL is empty");
        return;
    }
    let dir = scratch("postgres");
    let data = dir.join("data");
    let schema = format!(
        "w101b_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let separator = if base.contains('?') { '&' } else { '?' };
    let catalog = format!("{base}{separator}schema={schema}");

    let mut lake = pink_lake::Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let file = |base: i32| -> Vec<(i32, i32)> { (0..40).map(|i| (base + i, base + i)).collect() };
    lake.write_from_store(&store(&file(0))).unwrap();
    for base in [100, 200] {
        let mut commit = Commit::new();
        commit.append("main.t", sub_table(&file(base)));
        lake.commit(&commit).unwrap();
    }
    drop(lake);

    // A client on the catalog's schema, straight from the test database.
    let plain = base.split('?').next().unwrap().to_string();
    let mut client = plain
        .parse::<postgres::Config>()
        .unwrap()
        .connect(postgres::NoTls)
        .unwrap();
    client
        .batch_execute(&format!("SET search_path TO \"{schema}\""))
        .unwrap();
    let rows: Vec<StatsRow> = client
        .query(
            "SELECT variant_path, shredded_type, value_count, null_count, min_value, max_value
             FROM ducklake_file_variant_stats ORDER BY variant_path, min_value",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            )
        })
        .collect();
    assert_eq!(rows.len(), 6, "{rows:#?}");
    assert_eq!(
        rows[0],
        (
            "\"a\"".to_string(),
            "int64".to_string(),
            40,
            0,
            Some("0".to_string()),
            Some("39".to_string())
        )
    );
    assert_eq!(rows[1].4.as_deref(), Some("100"));

    // A fresh handle prunes from those rows.
    let report = pink_lake::Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap()))
        .unwrap()
        .attach(None)
        .unwrap();
    let live = Arc::clone(&report.lake);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let got = query(&mut db, "SELECT id FROM t WHERE v['a'] = 105;");
    assert_eq!(ids(got), vec![105]);
    assert_eq!(live.decode_stats().2, 1, "one file decoded");

    // Compaction ends the old files; expiring their snapshots drops the rows.
    let mut lake =
        pink_lake::Lake::open_with_catalog(&catalog, Some(data.to_str().unwrap())).unwrap();
    lake.merge_adjacent_files(
        None,
        &pink_lake::MergeSettings {
            min_file_size: Some(0),
            ..pink_lake::MergeSettings::default()
        },
    )
    .unwrap();
    let head = lake.current_version();
    let victims: Vec<u64> = lake
        .versions()
        .unwrap()
        .into_iter()
        .filter(|version| *version < head)
        .collect();
    lake.expire_snapshots(&pink_lake::ExpireSettings {
        versions: Some(victims),
        ..pink_lake::ExpireSettings::default()
    })
    .unwrap();
    drop(lake);
    let left: i64 = client
        .query_one("SELECT count(*) FROM ducklake_file_variant_stats", &[])
        .unwrap()
        .get(0);
    assert_eq!(left, 2, "only the merged file's paths remain");
    // Leave the test database as it was found.
    client
        .batch_execute(&format!(
            "RESET search_path; DROP SCHEMA \"{schema}\" CASCADE"
        ))
        .unwrap();
    cleanup(&dir);
}

/// One `ducklake_file_variant_stats` row, as the Postgres catalog stores it.
#[cfg(feature = "catalog-postgres")]
type StatsRow = (String, String, i64, i64, Option<String>, Option<String>);

/// The facts a file publishes in DuckLake's table are what a fresh attach
/// prunes with — including after the catalog rewrote the block (the `sqlite:`
/// backend stores no `.pink` footer at all).
#[cfg(feature = "catalog-sqlite")]
#[test]
fn a_sql_catalog_lake_round_trips_without_the_embedded_block() {
    let dir = scratch("no-block");
    let lake_dir = dir.join("lake");
    with_backend("sqlite", || {
        let rows = three_files(&lake_dir);
        let mut plain = store(&rows);
        for sql in [
            "SELECT id FROM t WHERE v['a'] = 205;",
            "SELECT id FROM t WHERE v['a'] > 39;",
            "SELECT id FROM t WHERE v['tag'] = 't100';",
            "SELECT id FROM t WHERE v['tag'] <> 't100';",
        ] {
            let (got, _) = attached_query(&lake_dir, sql);
            assert_eq!(ids(got), ids(query(&mut plain, sql)), "{sql}");
        }
    });
    cleanup(&dir);
}
