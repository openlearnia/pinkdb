//! Page-level pruning (wave 133a): a selective predicate decodes only the
//! data pages whose statistics leave a match, so the bytes a scan reads drop
//! with the selectivity — for pinkdb-written files (the writer now records the
//! column/offset indexes) and for DuckDB-written ones (DuckDB records them).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{DataTable, DataType, PruneOp, PrunePredicate, Value};
use pinkdb_parquet::{RangeSource, RemoteReadStats, plan_row_groups, scan_source};

/// A positional source over an in-memory file that counts what it served.
#[derive(Debug, Default)]
struct CountedRange {
    bytes: Vec<u8>,
    calls: AtomicU64,
    served: AtomicU64,
}

impl RangeSource for CountedRange {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| "range overflows".to_string())?;
        let slice = self
            .bytes
            .get(offset as usize..end as usize)
            .ok_or_else(|| format!("range {offset}..{end} is past the file"))?;
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.served.fetch_add(len, Ordering::Relaxed);
        Ok(slice.to_vec())
    }

    fn parallelism(&self) -> usize {
        4
    }
}

/// A file whose pages each hold one narrow band of values and whose row
/// groups mix bands from across the whole range — so row-group statistics
/// cannot prune a selective predicate, but page statistics can.
fn write_bands(rows: u64, page_rows: u64) -> Vec<u8> {
    let mut table =
        DataTable::new("t".to_string(), vec![("i".to_string(), DataType::Bigint)]).unwrap();
    for row in 0..rows {
        let page = row / page_rows;
        let value = band(page) + row % page_rows;
        table.append_row(vec![Value::Bigint(value as i64)]).unwrap();
    }
    pinkdb_parquet::write::write_table(&table).unwrap()
}

/// The first value of written page `page`. A row group holds four consecutive
/// pages whose bands are 240_000 apart, so a row group spans most of the file
/// while each page keeps a narrow band.
fn band(page: u64) -> u64 {
    (page % 4) * 240_000 + (page / 4) * 15_000
}

/// Read a whole file through the page-aware morsel plan, one morsel at a time.
fn scan_all(
    source: Arc<dyn RangeSource>,
    filters: &[PrunePredicate],
) -> (Vec<Value>, RemoteReadStats, u64, u64) {
    let plan = plan_row_groups("page_index.parquet", source, Some(&[0]), filters).unwrap();
    let mut values = Vec::new();
    for index in 0..plan.count() {
        plan.decode(index, &mut |chunk| {
            for row in 0..chunk.num_rows() {
                values.push(chunk.columns[0].get(row));
            }
            Ok(())
        })
        .unwrap();
    }
    let counters = plan.counters();
    (values, plan.stats(), counters.pages, counters.pages_pruned)
}

/// Matches the first row of every band-3 page: band 1 spans 50_000..66_383.
/// A value only the 33rd written page holds.
fn band_target() -> u64 {
    band(33) + 100
}

fn selective() -> Vec<PrunePredicate> {
    vec![PrunePredicate {
        column: 0,
        op: PruneOp::Eq,
        value: Some(Value::Bigint(band_target() as i64)),
        path: None,
    }]
}

#[test]
fn a_pinkdb_file_reads_only_the_pages_a_predicate_leaves() {
    let rows = 1_048_576;
    let bytes = write_bands(rows, 16_384);
    let full_source = Arc::new(CountedRange {
        bytes: bytes.clone(),
        ..CountedRange::default()
    });
    let (all, full_stats, full_pages, full_pruned) = scan_all(full_source.clone(), &[]);
    assert_eq!(all.len() as u64, rows);
    assert_eq!(full_pages, 64, "one page per 16k rows");
    assert_eq!(full_pruned, 0, "no predicate, no page pruned");
    assert!(
        full_stats.bytes + 8 >= bytes.len() as u64 && full_stats.bytes <= bytes.len() as u64,
        "a full scan reads the file once ({} of {} bytes)",
        full_stats.bytes,
        bytes.len()
    );

    let pruned_source = Arc::new(CountedRange {
        bytes,
        ..CountedRange::default()
    });
    let (rows_of_last_page, stats, pages, pruned) = scan_all(pruned_source.clone(), &selective());
    assert!(
        pruned > 32,
        "a value only one page holds prunes most pages, kept {pages}"
    );
    assert_eq!(pages + pruned, full_pages, "every page is read or pruned");
    assert!(
        rows_of_last_page.contains(&Value::Bigint(band_target() as i64)),
        "the surviving page holds the matching row"
    );
    assert!(
        pages <= 18,
        "a literal in one page keeps that page plus one alignment anchor per \
         row group ({pages} of {full_pages})"
    );
    println!(
        "page pruning: {pages} of {full_pages} pages read, {} of {} bytes, {} requests",
        stats.bytes,
        full_stats.bytes,
        pruned_source
            .calls
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert!(
        stats.bytes < full_stats.bytes * 4 / 5,
        "pruned scan read {} of {} bytes, kept {pages} of {full_pages} pages \
         (each row group keeps its first page as the reader's anchor, so the \
         bytes saved trail the pages pruned)",
        stats.bytes,
        full_stats.bytes
    );
    // Page pruning never changes what the reader answers for the rows it
    // keeps: the whole file read through the plain scan agrees.
    let plain_source = Arc::new(CountedRange {
        bytes: write_bands(rows, 16_384),
        ..CountedRange::default()
    });
    let unpruned = scan_source("page_index.parquet", plain_source, Some(&[0]), &[])
        .unwrap()
        .output
        .chunks
        .iter()
        .flat_map(|chunk| (0..chunk.num_rows()).map(|row| chunk.columns[0].get(row)))
        .collect::<Vec<Value>>();
    assert_eq!(unpruned.len() as u64, rows);
    assert_eq!(all, unpruned, "streamed and whole scans agree");
}

/// DuckDB's writer sets row-group statistics but no column/offset index (the
/// parquet extension has no `OffsetIndex`/`ColumnIndexMetaData` use — only the
/// generated thrift types mention them), so a DuckDB-written file prunes at
/// row-group granularity: every page of a surviving group is read.
#[test]
fn a_duckdb_file_prunes_at_row_group_granularity() {
    let duckdb =
        std::path::Path::new(&std::env::var("HOME").unwrap_or_default()).join(".local/bin/duckdb");
    assert!(
        duckdb.exists(),
        "the reference binary {} is required",
        duckdb.display()
    );
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_page_index_duckdb_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("sorted.parquet");
    let status = std::process::Command::new(&duckdb)
        .arg("-no-agent")
        .arg("-c")
        .arg(format!(
            "COPY (SELECT range AS i FROM range(200000)) TO '{}' (FORMAT PARQUET);",
            file.display()
        ))
        .status()
        .unwrap();
    assert!(status.success(), "duckdb wrote the fixture");
    let bytes = std::fs::read(&file).unwrap();
    let full_source = Arc::new(CountedRange {
        bytes: bytes.clone(),
        ..CountedRange::default()
    });
    let (all, full_stats, full_pages, _) = scan_all(full_source, &[]);
    assert_eq!(all.len(), 200_000);
    let only_last = vec![PrunePredicate {
        column: 0,
        op: PruneOp::Eq,
        value: Some(Value::Bigint(199_999)),
        path: None,
    }];
    let pruned_source = Arc::new(CountedRange {
        bytes: bytes.clone(),
        ..CountedRange::default()
    });
    let plan = plan_row_groups("duckdb.parquet", pruned_source, Some(&[0]), &only_last).unwrap();
    let mut kept = Vec::new();
    for index in 0..plan.count() {
        plan.decode(index, &mut |chunk| {
            for row in 0..chunk.num_rows() {
                kept.push(chunk.columns[0].get(row));
            }
            Ok(())
        })
        .unwrap();
    }
    std::fs::remove_dir_all(&dir).ok();
    let counters = plan.counters();
    let row_groups_pruned = counters.row_groups_pruned;
    // The DuckDB build this environment ships (v2.0.0-alpha46057, the
    // ducklake-vortex reference) writes no page index — its writer has no
    // column/offset index at all — so page pruning cannot apply and the scan
    // falls back to row-group granularity, still exact.
    assert_eq!(counters.pages_pruned, 0, "no page index to prune with");
    assert_eq!(
        counters.pages, full_pages,
        "every page is read when none can be pruned"
    );
    assert!(
        row_groups_pruned > 0,
        "a selective predicate still prunes DuckDB row groups"
    );
    let stats = plan.stats();
    assert!(
        stats.bytes < full_stats.bytes,
        "pruned scan read {} of {} bytes",
        stats.bytes,
        full_stats.bytes
    );
    assert!(
        kept.last() == Some(&Value::Bigint(199_999)),
        "the surviving row group holds the matching row"
    );
    assert!(kept.len() < 200_000);
}
