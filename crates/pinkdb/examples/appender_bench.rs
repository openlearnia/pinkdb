//! Temporary 112d measurement: appender vs prepared insert vs INSERT … SELECT.
//!
//! Usage: appender_bench <source-store.pink | lineitem.csv> [scratch-dir]
//!
//! A `.csv` source is loaded into a scratch store first. With
//! `APPENDER_FLUSH_SWEEP=4096,204800` only the durable appender cases run, once
//! per auto-flush size and interleaved, to choose the default.
//! The scratch directory defaults to
//! `~/Desktop/projects/pinkdb-wt/scratch-a/appender-bench` and must stay under
//! `~/Desktop/projects/pinkdb-wt/scratch-a`; `/tmp` is never used. The source
//! is copied to scratch before opening, and every target store is deleted
//! after its round.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pinkdb::exec::ChunkedResult;
use pinkdb::{Connection, DataType, Database, QueryResult, Value};

fn ok(conn: &mut Connection, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn count(conn: &mut Connection, sql: &str) -> i64 {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => match &rows[0][0] {
            Value::Bigint(n) => *n,
            Value::Integer(n) => *n as i64,
            Value::Hugeint(n) => i64::try_from(*n).unwrap(),
            other => panic!("expected an integer, got {other:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

fn remove_store(path: &Path) {
    for suffix in ["", ".wal", ".lock", ".open"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(p));
    }
}

fn copy_store(source: &Path, target: &Path) {
    remove_store(target);
    for suffix in ["", ".wal"] {
        let mut from = source.as_os_str().to_owned();
        from.push(suffix);
        let from = PathBuf::from(from);
        if from.exists() {
            let mut to = target.as_os_str().to_owned();
            to.push(suffix);
            std::fs::copy(&from, PathBuf::from(to)).unwrap();
        }
    }
}

fn create_table(conn: &mut Connection, table: &str, names: &[String], types: &[DataType]) {
    let columns = names
        .iter()
        .zip(types)
        .map(|(name, dtype)| format!("\"{name}\" {dtype}"))
        .collect::<Vec<_>>()
        .join(", ");
    ok(conn, &format!("CREATE TABLE \"{table}\" ({columns})"));
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort();
    values[values.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let source = PathBuf::from(&args[1]);
    let scratch_root = PathBuf::from(std::env::var("HOME").expect("HOME"))
        .join("Desktop/projects/pinkdb-wt/scratch-a");
    let scratch = match args.get(2) {
        Some(arg) => {
            let path = PathBuf::from(arg);
            assert!(
                path.starts_with(&scratch_root),
                "scratch must be under {}",
                scratch_root.display()
            );
            path
        }
        None => scratch_root.join("appender-bench"),
    };
    std::fs::create_dir_all(&scratch).unwrap();
    let source_copy = scratch.join("source.pink");
    let source = if source.extension().is_some_and(|e| e == "csv") {
        remove_store(&source_copy);
        // The engine alone has no `read_csv`; its CSV importer infers types.
        let mut db = pinkdb::PinkDb::open_durable(source_copy.to_str().unwrap()).unwrap();
        pinkdb::csv::import_csv(&mut db, source.to_str().unwrap(), Some("lineitem")).unwrap();
        db.execute("CHECKPOINT;").unwrap();
        drop(db);
        source_copy.clone()
    } else {
        copy_store(&source, &source_copy);
        source
    };

    let source_db = Database::open(source_copy.to_str().unwrap()).unwrap();
    let mut source_conn = source_db.connect();
    let select = "SELECT * FROM lineitem";
    let head = match source_conn
        .execute_chunked(&format!("{select} LIMIT 0"))
        .unwrap()
        .pop()
        .unwrap()
    {
        ChunkedResult::Selected { names, types, .. } => (names, types),
        other => panic!(
            "expected columns, got {}",
            other.as_query_result().is_some()
        ),
    };
    let (names, types) = head;
    let total = count(&mut source_conn, "SELECT count(*) FROM lineitem");
    let chunks = match source_conn.execute_chunked(select).unwrap().pop().unwrap() {
        ChunkedResult::Selected { chunks, .. } => chunks,
        other => panic!("expected chunks, got {}", other.as_query_result().is_some()),
    };
    println!(
        "source rows={total} columns={} chunks={}",
        names.len(),
        chunks.len()
    );
    drop(source_conn);
    drop(source_db);

    if let Ok(sweep) = std::env::var("APPENDER_FLUSH_SWEEP") {
        let sizes: Vec<usize> = sweep.split(',').map(|s| s.trim().parse().unwrap()).collect();
        let mut times = vec![(Vec::new(), Vec::new()); sizes.len()];
        for round in 0..3 {
            for (i, &size) in sizes.iter().enumerate() {
                for chunked in [false, true] {
                    let path = scratch.join(format!("sweep-{round}-{size}-{chunked}.pink"));
                    remove_store(&path);
                    let db = Database::open(path.to_str().unwrap()).unwrap();
                    let mut conn = db.connect();
                    create_table(&mut conn, "lineitem", &names, &types);
                    let start = Instant::now();
                    let mut app = conn.appender(None, "lineitem").unwrap();
                    app.set_auto_flush(size);
                    for chunk in &chunks {
                        if chunked {
                            app.append_shared_chunk(&mut conn, &chunk.columns).unwrap();
                        } else {
                            for row in 0..chunk.num_rows {
                                let values =
                                    chunk.columns.iter().map(|column| column.get(row)).collect();
                                app.append_row(&mut conn, values).unwrap();
                            }
                        }
                    }
                    app.flush(&mut conn).unwrap();
                    let elapsed = start.elapsed();
                    assert_eq!(count(&mut conn, "SELECT count(*) FROM lineitem"), total);
                    drop(conn);
                    drop(db);
                    remove_store(&path);
                    if chunked {
                        times[i].1.push(elapsed);
                    } else {
                        times[i].0.push(elapsed);
                    }
                }
            }
        }
        for (size, (row, chunk)) in sizes.iter().zip(times) {
            let (row, chunk) = (median(row), median(chunk));
            println!(
                "auto-flush {size}: durable row {:.3}s ({:.0} rows/s), durable chunk {:.3}s ({:.0} rows/s)",
                row.as_secs_f64(),
                total as f64 / row.as_secs_f64(),
                chunk.as_secs_f64(),
                total as f64 / chunk.as_secs_f64()
            );
        }
        remove_store(&source_copy);
        return;
    }

    let mut durable_row = Vec::new();
    let mut durable_chunk = Vec::new();
    let mut memory_row = Vec::new();
    let mut memory_chunk = Vec::new();
    let mut prepared_times = Vec::new();
    let mut select_times = Vec::new();
    let mut wal_bytes = 0u64;

    for round in 0..3 {
        // (a) durable row-wise
        let path = scratch.join(format!("row-{round}.pink"));
        remove_store(&path);
        let db = Database::open(path.to_str().unwrap()).unwrap();
        let mut conn = db.connect();
        create_table(&mut conn, "lineitem", &names, &types);
        let start = Instant::now();
        {
            let mut app = conn.appender(None, "lineitem").unwrap();
            for chunk in &chunks {
                for row in 0..chunk.num_rows {
                    let values = chunk.columns.iter().map(|column| column.get(row)).collect();
                    app.append_row(&mut conn, values).unwrap();
                }
            }
            app.flush(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        assert_eq!(count(&mut conn, "SELECT count(*) FROM lineitem"), total);
        let wal = std::fs::metadata(scratch.join(format!("row-{round}.pink.wal")))
            .map(|m| m.len())
            .unwrap_or(0);
        wal_bytes = wal_bytes.max(wal);
        drop(conn);
        drop(db);
        remove_store(&path);
        durable_row.push(elapsed);

        // (b) durable chunk-wise
        let path = scratch.join(format!("chunk-{round}.pink"));
        remove_store(&path);
        let db = Database::open(path.to_str().unwrap()).unwrap();
        let mut conn = db.connect();
        create_table(&mut conn, "lineitem", &names, &types);
        let start = Instant::now();
        {
            let mut app = conn.appender(None, "lineitem").unwrap();
            for chunk in &chunks {
                app.append_shared_chunk(&mut conn, &chunk.columns).unwrap();
            }
            app.flush(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        assert_eq!(count(&mut conn, "SELECT count(*) FROM lineitem"), total);
        drop(conn);
        drop(db);
        remove_store(&path);
        durable_chunk.push(elapsed);

        // (a') in-memory row-wise
        let db = Database::new();
        let mut conn = db.connect();
        create_table(&mut conn, "lineitem", &names, &types);
        let start = Instant::now();
        {
            let mut app = conn.appender(None, "lineitem").unwrap();
            for chunk in &chunks {
                for row in 0..chunk.num_rows {
                    let values = chunk.columns.iter().map(|column| column.get(row)).collect();
                    app.append_row(&mut conn, values).unwrap();
                }
            }
            app.flush(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        assert_eq!(count(&mut conn, "SELECT count(*) FROM lineitem"), total);
        memory_row.push(elapsed);

        // (b') in-memory chunk-wise
        let db = Database::new();
        let mut conn = db.connect();
        create_table(&mut conn, "lineitem", &names, &types);
        let start = Instant::now();
        {
            let mut app = conn.appender(None, "lineitem").unwrap();
            for chunk in &chunks {
                app.append_shared_chunk(&mut conn, &chunk.columns).unwrap();
            }
            app.flush(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        assert_eq!(count(&mut conn, "SELECT count(*) FROM lineitem"), total);
        memory_chunk.push(elapsed);

        // (c) prepared INSERT … VALUES per row, 200k rows
        let path = scratch.join(format!("prepared-{round}.pink"));
        remove_store(&path);
        let db = Database::open(path.to_str().unwrap()).unwrap();
        let mut conn = db.connect();
        create_table(&mut conn, "lineitem", &names, &types);
        let placeholders = std::iter::repeat_n("?", names.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO lineitem VALUES ({placeholders})");
        let prepared = conn.prepare(&sql).unwrap();
        let start = Instant::now();
        let mut inserted = 0i64;
        'rows: for chunk in &chunks {
            for row in 0..chunk.num_rows {
                if inserted >= 200_000 {
                    break 'rows;
                }
                let values: Vec<Value> =
                    chunk.columns.iter().map(|column| column.get(row)).collect();
                conn.execute_prepared(&prepared, &values).unwrap();
                inserted += 1;
            }
        }
        let elapsed = start.elapsed();
        assert_eq!(count(&mut conn, "SELECT count(*) FROM lineitem"), inserted);
        drop(conn);
        drop(db);
        remove_store(&path);
        prepared_times.push(elapsed);

        // (d) INSERT … SELECT inside the source store
        let path = scratch.join(format!("select-{round}.pink"));
        copy_store(&source, &path);
        let db = Database::open(path.to_str().unwrap()).unwrap();
        let mut conn = db.connect();
        create_table(&mut conn, "bench_lineitem", &names, &types);
        let start = Instant::now();
        ok(
            &mut conn,
            "INSERT INTO bench_lineitem SELECT * FROM lineitem",
        );
        let elapsed = start.elapsed();
        assert_eq!(
            count(&mut conn, "SELECT count(*) FROM bench_lineitem"),
            total
        );
        drop(conn);
        drop(db);
        remove_store(&path);
        select_times.push(elapsed);
    }

    let rows = total as f64;
    let row_durable = median(durable_row.clone());
    let chunk_durable = median(durable_chunk.clone());
    let row_memory = median(memory_row.clone());
    let chunk_memory = median(memory_chunk.clone());
    let text_row =
        (row_durable.as_secs_f64() - row_memory.as_secs_f64()) / row_durable.as_secs_f64() * 100.0;
    let text_chunk = (chunk_durable.as_secs_f64() - chunk_memory.as_secs_f64())
        / chunk_durable.as_secs_f64()
        * 100.0;
    for (name, times) in [
        ("appender-row-durable", durable_row),
        ("appender-chunk-durable", durable_chunk),
        ("appender-row-memory", memory_row),
        ("appender-chunk-memory", memory_chunk),
    ] {
        let med = median(times.clone());
        println!(
            "{name}: median {:.3}s {:.0} rows/s rounds={:?}",
            med.as_secs_f64(),
            rows / med.as_secs_f64(),
            times
                .iter()
                .map(|t| format!("{:.3}", t.as_secs_f64()))
                .collect::<Vec<_>>()
        );
    }
    println!(
        "text-cost-of-durable-row: {:.1}% of durable append time",
        text_row
    );
    println!(
        "text-cost-of-durable-chunk: {:.1}% of durable append time",
        text_chunk
    );
    let prepared_med = median(prepared_times.clone());
    println!(
        "prepared-row-durable-200k: median {:.3}s {:.0} rows/s rounds={:?}",
        prepared_med.as_secs_f64(),
        200_000.0 / prepared_med.as_secs_f64(),
        prepared_times
            .iter()
            .map(|t| format!("{:.3}", t.as_secs_f64()))
            .collect::<Vec<_>>()
    );
    let select_med = median(select_times.clone());
    println!(
        "insert-select-durable: median {:.3}s {:.0} rows/s rounds={:?}",
        select_med.as_secs_f64(),
        rows / select_med.as_secs_f64(),
        select_times
            .iter()
            .map(|t| format!("{:.3}", t.as_secs_f64()))
            .collect::<Vec<_>>()
    );
    println!("max-wal-bytes={wal_bytes}");
}
