//! Streaming scan sources for the pipeline executor (wave 133b).
//!
//! The pipeline's source is an [`crate::exec::Output`] (every chunk
//! evaluated first, then folded in parallel) or a [`MorselSource`], which
//! instead hands out morsels one at a time: [`MorselSource::next_morsel`] claims a cheap
//! descriptor (a data file, a Parquet row group or page range index) from
//! shared state, and the worker that claimed it decodes that morsel itself.
//! Partials merge by morsel index, so the answer is the materialized path's
//! answer while memory is the morsels in flight, not the whole input.
//!
//! Decoding on the claiming worker is what keeps a streamed scan from
//! deadlocking a thread pool: the pool's workers are the only threads waiting
//! for morsels, so nothing they wait on may need a worker itself. A source
//! that pushed decoded chunks to its consumers had to decode on another
//! thread, and a decode that used the same pool (pink-lake's per-row-group
//! reads do) then starved the workers waiting for its rows.
//!
//! Nothing here knows what a morsel is: a run of a store table's chunks, a
//! Parquet row group or page range, a data file, a table function's next batch
//! all arrive through the same interface. [`ScanIo`] is the generic
//! read-vs-pruned report behind `EXPLAIN ANALYZE` and `pinkdb_scan_io()`, for
//! the sources that have object-storage reads to report.
//!
//! The source state is shared, like DuckDB's global source state
//! (`src/execution/operator/scan/physical_table_scan.cpp`, `GetData`): every
//! worker asks the same source for the next morsel, so the work distributes
//! without a per-worker split, and at most one morsel per worker is in
//! flight.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::DataTable;
use crate::types::{DataType, Result};
use crate::vector::DataChunk;

/// One read-vs-pruned counter a source reports. `unit` is the plural noun
/// `EXPLAIN ANALYZE` prints ("files", "row groups", "pages").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanUnit {
    pub unit: &'static str,
    pub read: u64,
    pub pruned: u64,
}

/// What one scan source read, pruned and kept in flight.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanIo {
    /// Coarse-to-fine units the source prunes, in the order it prunes them.
    /// Empty for a source with no such units; it still reports requests and
    /// bytes.
    pub units: Vec<ScanUnit>,
    /// Object-storage or filesystem requests the scan issued.
    pub requests: u64,
    /// Bytes those requests read.
    pub bytes: u64,
    /// The most morsels decoded and not yet folded (filled in by the
    /// pipeline; a drained source reports the workers that decoded).
    pub in_flight_peak: u64,
}

impl ScanIo {
    /// The one-line form `EXPLAIN ANALYZE` prints.
    pub fn describe(&self) -> String {
        let mut parts: Vec<String> = self
            .units
            .iter()
            .map(|u| format!("{} {} read / {} pruned", u.unit, u.read, u.pruned))
            .collect();
        parts.push(format!("{} requests", self.requests));
        parts.push(format!("{} bytes", self.bytes));
        parts.push(format!("{} in flight", self.in_flight_peak));
        parts.join(", ")
    }
}

/// One completed scan of the last statement, as `EXPLAIN ANALYZE` and
/// `pinkdb_scan_io()` report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRecord {
    /// [`MorselSource::label`]: which scan this was.
    pub source: String,
    pub io: ScanIo,
}

/// A scan source of the pipeline executor: morsels claimed by index and
/// decoded by the worker that claimed them.
pub trait MorselSource: std::fmt::Debug + Send + Sync {
    /// The source's output names, in chunk order.
    fn names(&self) -> &[String];
    /// Their types.
    fn types(&self) -> &[DataType];
    /// Short source description for `EXPLAIN ANALYZE`/`pinkdb_scan_io()`.
    fn label(&self) -> &str;
    /// Claim the next morsel, or `None` once every morsel is claimed. Cheap
    /// and callable from any worker: a descriptor from shared state, no
    /// decode, and no lock held past the claim.
    fn next_morsel(&self) -> Result<Option<u64>>;
    /// Decode the claimed morsel, handing each chunk to `sink` in source
    /// order. Runs on the claiming worker outside every lock the source
    /// holds, so it may use the thread pool it runs on.
    fn decode(&self, morsel: u64, sink: &mut dyn FnMut(DataChunk) -> Result<()>) -> Result<()>;
    /// What the scan read; complete once [`MorselSource::next_morsel`] has
    /// returned `None` and every claimed morsel was decoded.
    fn io(&self) -> ScanIo;
    /// The rows the source will hand out, when it knows them without reading
    /// (a store scan's footer). The pipeline uses it as a bucket-count hint,
    /// never as a result; `None` means the source must run to know.
    fn rows(&self) -> Option<usize> {
        None
    }
    /// Whether [`MorselSource::io`] is worth a [`ScanRecord`]: a source with
    /// no object-storage reads (a store scan) has nothing to report.
    fn reports_io(&self) -> bool {
        true
    }
}

/// The shared claim counter of a [`MorselSource`]: morsels numbered `0..len`
/// are handed out in order, one claim per worker.
#[derive(Debug)]
pub struct MorselCursor {
    len: u64,
    next: AtomicU64,
}

impl MorselCursor {
    pub fn new(len: u64) -> MorselCursor {
        MorselCursor {
            len,
            next: AtomicU64::new(0),
        }
    }

    /// The next unclaimed index, or `None` when all are claimed.
    pub fn claim(&self) -> Option<u64> {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        (index < self.len).then_some(index)
    }
}

/// Drain a source into one table: what a caller that needs the whole table
/// (copy paths, tests) reads a streamed scan into.
pub fn materialize(source: &dyn MorselSource, name: &str) -> Result<DataTable> {
    let schema: Vec<(String, DataType)> = source
        .names()
        .iter()
        .cloned()
        .zip(source.types().iter().copied())
        .collect();
    let mut table = DataTable::new(name.to_string(), schema)?;
    while let Some(morsel) = source.next_morsel()? {
        source.decode(morsel, &mut |chunk| {
            table.append_shared_chunk(&chunk.columns)
        })?;
    }
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A source of `len` morsels of `per_morsel` rows that counts decodes.
    #[derive(Debug)]
    struct Rows {
        names: Vec<String>,
        types: Vec<DataType>,
        cursor: MorselCursor,
        per_morsel: u64,
        decoded: AtomicU64,
    }

    impl Rows {
        fn new(len: u64, per_morsel: u64) -> Rows {
            Rows {
                names: vec!["x".to_string()],
                types: vec![DataType::Integer],
                cursor: MorselCursor::new(len),
                per_morsel,
                decoded: AtomicU64::new(0),
            }
        }
    }

    impl MorselSource for Rows {
        fn names(&self) -> &[String] {
            &self.names
        }

        fn types(&self) -> &[DataType] {
            &self.types
        }

        fn label(&self) -> &str {
            "test"
        }

        fn next_morsel(&self) -> Result<Option<u64>> {
            Ok(self.cursor.claim())
        }

        fn decode(&self, morsel: u64, sink: &mut dyn FnMut(DataChunk) -> Result<()>) -> Result<()> {
            self.decoded.fetch_add(1, Ordering::Relaxed);
            for row in 0..self.per_morsel {
                let mut column = crate::Vector::with_capacity(DataType::Integer, 1);
                column.push_valid(crate::Value::Integer(
                    (morsel * self.per_morsel + row) as i32,
                ))?;
                sink(DataChunk::new(vec![column])?)?;
            }
            Ok(())
        }

        fn io(&self) -> ScanIo {
            ScanIo::default()
        }
    }

    /// Four workers claim and decode the same source: every morsel is handed
    /// out once and decoded once, and no claim ever blocks.
    #[test]
    fn concurrent_workers_claim_every_morsel_once() {
        let source = Arc::new(Rows::new(64, 2));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let source = Arc::clone(&source);
                std::thread::spawn(move || {
                    let mut rows = 0u64;
                    while let Some(morsel) = source.next_morsel().unwrap() {
                        source
                            .decode(morsel, &mut |chunk| {
                                rows += chunk.num_rows() as u64;
                                Ok(())
                            })
                            .unwrap();
                    }
                    rows
                })
            })
            .collect();
        let rows: u64 = workers.into_iter().map(|w| w.join().unwrap()).sum();
        assert_eq!(rows, 64 * 2);
        assert_eq!(source.decoded.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn materialize_reads_every_row() {
        let table = materialize(&Rows::new(3, 2), "t").unwrap();
        assert_eq!(table.num_rows(), 6);
    }

    #[test]
    fn the_cursor_stops_at_its_length() {
        let cursor = MorselCursor::new(2);
        assert_eq!(
            (cursor.claim(), cursor.claim(), cursor.claim()),
            (Some(0), Some(1), None)
        );
    }
}
