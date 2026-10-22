//! The lake's ranged-scan core (wave 102).
//!
//! A projected lake read fetches exactly the byte ranges it needs: the
//! footer of a data file and the column chunks of the row groups that
//! survive the scan's filters. This module turns that wish into requests:
//!
//! - [`coalesce_spans`] merges overlapping or near-adjacent byte ranges into
//!   as few requests as possible (adjacent chunks always merge; a gap up to
//!   [`COALESCE_GAP`] merges when the result stays under
//!   [`MAX_REQUEST_BYTES`]);
//! - [`fetch_spans`] fetches the merged ranges with at most
//!   `PINKDB_HTTPFS_PARALLELISM` requests in flight, preserving the order.
//!
//! Both are the **same rules** wave 98b gave the parquet scan
//! (`extensions/parquet/src/ranged.rs`): the two crates cannot share one
//! implementation without inverting the layering (the engine's parquet
//! extension must not depend on the lake crate), so the rules are mirrored
//! here and pinned against the parquet extension's own coalescer by
//! `crates/pink-lake-parquet/tests/fast_reads.rs`.
//!
//! The transport itself is not duplicated: the lake's positional reader
//! ([`crate::format::RangedFile`], the wave-98b seam) does the I/O, and the
//! bounded concurrency is the lake's own [`crate::map_bounded`].

use crate::Result;
use crate::format::RangedFile;

/// Coalesce projected column chunks whose gap is at most this many bytes
/// into one range request. Adjacent chunks always coalesce.
pub(crate) const COALESCE_GAP: u64 = 1024 * 1024;

/// The largest single coalesced range request.
pub(crate) const MAX_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

/// One byte range a scan wants to fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteSpan {
    /// The range's first byte.
    pub(crate) start: u64,
    /// The range's length in bytes.
    pub(crate) len: u64,
}

impl ByteSpan {
    /// The byte after the range's last one.
    pub(crate) fn end(&self) -> u64 {
        self.start.saturating_add(self.len)
    }
}

/// Coalesce overlapping or near-adjacent spans into at most
/// [`MAX_REQUEST_BYTES`]-sized requests (adjacent chunks always merge; gaps
/// up to [`COALESCE_GAP`] merge when the result stays under the cap). The
/// output is sorted by offset.
pub(crate) fn coalesce_spans(mut spans: Vec<ByteSpan>) -> Vec<ByteSpan> {
    spans.retain(|span| span.len > 0);
    spans.sort_by_key(|span| span.start);
    let mut out: Vec<ByteSpan> = Vec::with_capacity(spans.len());
    for span in spans {
        if let Some(last) = out.last_mut() {
            let last_end = last.end();
            let end = span.end().max(last_end);
            let merged_len = end - last.start;
            if span.start <= last_end.saturating_add(COALESCE_GAP)
                && merged_len <= MAX_REQUEST_BYTES
            {
                *last = ByteSpan {
                    start: last.start,
                    len: merged_len,
                };
                continue;
            }
        }
        out.push(span);
    }
    out
}

/// Fetch `spans` with at most `limit` concurrent requests (one request per
/// span), preserving the result order. The first error in span order is the
/// one the caller reports.
pub(crate) fn fetch_spans(
    source: &dyn RangedFile,
    spans: &[ByteSpan],
    limit: usize,
) -> Vec<Result<Vec<u8>>> {
    if spans.is_empty() {
        return Vec::new();
    }
    crate::map_bounded(spans, limit, |span| source.read_range(span.start, span.len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescing_merges_adjacent_and_small_gaps_only() {
        let merged = coalesce_spans(vec![
            ByteSpan { start: 10, len: 5 },
            ByteSpan { start: 15, len: 5 },
            ByteSpan {
                start: 20 + COALESCE_GAP,
                len: 7,
            },
        ]);
        assert_eq!(
            merged,
            vec![ByteSpan {
                start: 10,
                len: COALESCE_GAP + 17
            }]
        );
        let separate = coalesce_spans(vec![
            ByteSpan { start: 0, len: 1 },
            ByteSpan {
                start: COALESCE_GAP + 2,
                len: 1,
            },
        ]);
        assert_eq!(separate.len(), 2);
        // Sorted input, no duplicate fetch (the small gap also coalesces).
        let sorted = coalesce_spans(vec![
            ByteSpan { start: 100, len: 4 },
            ByteSpan { start: 100, len: 4 },
            ByteSpan { start: 0, len: 2 },
        ]);
        assert_eq!(sorted, vec![ByteSpan { start: 0, len: 104 }]);
        // A merged range over the cap is never merged (the cap is the point).
        let big = coalesce_spans(vec![
            ByteSpan {
                start: 0,
                len: MAX_REQUEST_BYTES,
            },
            ByteSpan {
                start: MAX_REQUEST_BYTES,
                len: 1,
            },
        ]);
        assert_eq!(big.len(), 2);
    }
}
