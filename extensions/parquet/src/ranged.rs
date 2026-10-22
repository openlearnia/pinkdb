//! Ranged parquet reads over any positional source (wave 98a, generalized by
//! wave 98b).
//!
//! [`RangeSource`] is the small seam between parquet's [`ChunkReader`] and a
//! byte-range backend: the shared `pinkdb-httpfs` transport supplies one
//! implementation for remote objects, and pink-lake's `DataFile` supplies
//! another for lake data files (object storage or a local plaintext file).
//! Everything below is transport-agnostic:
//!
//! - the **footer** is read as parquet asks (trailer, thrift metadata), as
//!   range reads;
//! - for each surviving row group the projected column chunks' byte ranges
//!   are coalesced ([`COALESCE_GAP`], capped at [`MAX_REQUEST_BYTES`]) and
//!   fetched with a bounded pool ([`fetch_spans`]); earlier row groups decode
//!   while later ones are still in flight;
//! - results are decoded and assembled strictly in row-group order, so output
//!   order and values are unchanged;
//! - [`TransportCounters`] records requests, response bytes and network wait.

use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

/// The maximum bytes one lazy read fetches ahead on the serial path.
pub(crate) const READ_BLOCK: u64 = 128 * 1024;

/// Coalesce projected column chunks whose gap is at most this many bytes into
/// one range request. Adjacent chunks always coalesce.
pub(crate) const COALESCE_GAP: u64 = 1024 * 1024;

/// The largest single coalesced range request.
pub(crate) const MAX_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

/// The most compressed bytes one read-ahead window buffers at once. A row
/// group larger than this is still fetched alone.
pub(crate) const MAX_PREFETCH_BYTES: u64 = 128 * 1024 * 1024;

/// Per-scan transport counters: requests issued, bytes received, and wall
/// time spent waiting on the network.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemoteReadStats {
    /// Transport calls made by the scan.
    pub requests: u64,
    /// Response bytes received across those calls.
    pub bytes: u64,
    /// Wall-clock nanoseconds spent inside transport calls.
    pub wait_nanos: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TransportCounters {
    pub(crate) requests: AtomicU64,
    pub(crate) bytes: AtomicU64,
    pub(crate) wait_nanos: AtomicU64,
}

impl TransportCounters {
    pub(crate) fn snapshot(&self) -> RemoteReadStats {
        RemoteReadStats {
            requests: self.requests.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            wait_nanos: self.wait_nanos.load(Ordering::Relaxed),
        }
    }
}

/// A positional, ranged byte source: the one thing a scan needs from a
/// transport. Implementations must be safe to call from several threads.
pub trait RangeSource: std::fmt::Debug + Send + Sync {
    /// The object's total byte length.
    fn len(&self) -> u64;
    /// Whether the object is zero bytes (paired with [`RangeSource::len`]).
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// `len` bytes starting at `offset`. An error is a human-readable message
    /// (the scan prefixes it with its own context).
    fn read_range(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, String>;
    /// The desired in-flight request limit (0/1 = serial).
    fn parallelism(&self) -> usize {
        8
    }
}

/// A resolved ranged object: its source, size, and the documented
/// unknown-size whole-object fallback (never used for a lake data file, whose
/// size comes from `stat`).
#[derive(Debug, Clone)]
pub struct RemoteSpec {
    pub(crate) source: Arc<dyn RangeSource>,
    pub(crate) len: u64,
    pub(crate) whole: Option<Bytes>,
    pub(crate) parallelism: usize,
}

impl RemoteSpec {
    pub(crate) fn from_source(source: Arc<dyn RangeSource>) -> Self {
        let len = source.len();
        let parallelism = source.parallelism();
        Self {
            source,
            len,
            whole: None,
            parallelism,
        }
    }

    /// The object size the scan should use (the whole-body fallback when the
    /// transport had no size).
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the object is zero bytes.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// One counted range request.
pub(crate) fn counted_range(
    source: &dyn RangeSource,
    start: u64,
    len: u64,
    stats: &TransportCounters,
) -> std::result::Result<Vec<u8>, String> {
    stats.requests.fetch_add(1, Ordering::Relaxed);
    let begin = Instant::now();
    let result = source.read_range(start, len);
    stats
        .wait_nanos
        .fetch_add(begin.elapsed().as_nanos() as u64, Ordering::Relaxed);
    if let Ok(bytes) = &result {
        stats.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }
    result
}

/// `metadata_len` from the parquet trailer (`get_bytes(len-8, 8)`), read from
/// the cache or with one range request.
pub(crate) fn footer_size(spec: &RemoteSpec) -> std::result::Result<u64, String> {
    if spec.len < 12 {
        return Ok(0);
    }
    let trailer = match &spec.whole {
        Some(bytes) => bytes.slice(bytes.len() - 8..).to_vec(),
        None => spec.source.read_range(spec.len - 8, 8)?,
    };
    if trailer.len() < 4 {
        return Ok(0);
    }
    Ok(u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as u64)
}

/// One byte range a scan wants to fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteSpan {
    pub(crate) start: u64,
    pub(crate) len: u64,
}

impl ByteSpan {
    pub(crate) fn end(&self) -> u64 {
        self.start.saturating_add(self.len)
    }
}

/// The byte range of one column chunk a page decoder will read: from the
/// dictionary page offset (when present) or the data page offset, for the
/// chunk's compressed size.
pub(crate) fn chunk_span(chunk: &crate::file_meta::ChunkInfo) -> Option<ByteSpan> {
    if chunk.compressed_size <= 0 {
        return None;
    }
    let start = chunk
        .dictionary_page_offset
        .filter(|offset| *offset >= 0)
        .unwrap_or(chunk.data_page_offset);
    if start < 0 {
        return None;
    }
    Some(ByteSpan {
        start: start as u64,
        len: chunk.compressed_size as u64,
    })
}

/// Coalesce overlapping or near-adjacent spans into at most
/// [`MAX_REQUEST_BYTES`]-sized requests (adjacent chunks always merge; gaps up
/// to [`COALESCE_GAP`] merge when the result stays under the cap). The output
/// is sorted by offset.
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

/// One cached row-group byte window, keyed by its start offset.
#[derive(Debug)]
struct CachedRange {
    start: u64,
    end: u64,
    bytes: Bytes,
}

/// The read-ahead cache shared by a scan's `RemoteFile` and its prefetch
/// requests. Sequential decoding still happens through the same
/// `SerializedFileReader`; a populated range is served from memory.
#[derive(Debug, Default)]
pub(crate) struct RangeCache {
    ranges: Mutex<Vec<CachedRange>>,
}

impl RangeCache {
    /// Install one fetched range (its key is the span start).
    pub(crate) fn insert(&self, start: u64, bytes: Vec<u8>) {
        let len = bytes.len() as u64;
        self.ranges.lock().unwrap().push(CachedRange {
            start,
            end: start.saturating_add(len),
            bytes: Bytes::from(bytes),
        });
    }

    /// Serve a read at `start` from the cached prefix (to the end of its
    /// range). `None` means the read is not prefetched.
    fn read_from(&self, start: u64) -> Option<Bytes> {
        let ranges = self.ranges.lock().unwrap();
        ranges
            .iter()
            .find(|range| range.start <= start && start < range.end)
            .map(|range| range.bytes.slice((start - range.start) as usize..))
    }

    /// Serve `start..start+len` only when one cached range fully covers it.
    fn exact(&self, start: u64, len: u64) -> Option<Bytes> {
        let end = start.checked_add(len)?;
        let ranges = self.ranges.lock().unwrap();
        ranges
            .iter()
            .find(|range| range.start <= start && end <= range.end)
            .map(|range| {
                let from = (start - range.start) as usize;
                range.bytes.slice(from..from + len as usize)
            })
    }

    /// Drop the ranges a decoded row group used (their `Read` handles own a
    /// `Bytes` slice, so an active decoder is unaffected).
    pub(crate) fn remove(&self, starts: &[u64]) {
        if starts.is_empty() {
            return;
        }
        self.ranges
            .lock()
            .unwrap()
            .retain(|range| !starts.contains(&range.start));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.ranges.lock().unwrap().len()
    }
}

/// Fetch `spans` with at most `parallelism` concurrent requests (one request
/// per span), preserving result order.
pub(crate) fn fetch_spans(
    source: &dyn RangeSource,
    spans: &[ByteSpan],
    parallelism: usize,
    stats: &TransportCounters,
) -> Vec<std::result::Result<Vec<u8>, String>> {
    if spans.is_empty() {
        return Vec::new();
    }
    let results: Vec<Option<std::result::Result<Vec<u8>, String>>> =
        (0..spans.len()).map(|_| None).collect();
    let next = AtomicUsize::new(0);
    let results = Mutex::new(results);
    let workers = parallelism.clamp(1, spans.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(span) = spans.get(index) else {
                        break;
                    };
                    let result = counted_range(source, span.start, span.len, stats);
                    results.lock().unwrap()[index] = Some(result);
                }
            });
        }
    });
    results
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|result| result.expect("every fetch span was claimed by a worker"))
        .collect()
}

/// Parquet's [`ChunkReader`] over a ranged source.
#[derive(Debug)]
pub(crate) struct RemoteFile {
    source: Arc<dyn RangeSource>,
    len: u64,
    whole: Option<Bytes>,
    stats: Arc<TransportCounters>,
    cache: Arc<RangeCache>,
}

impl RemoteFile {
    pub(crate) fn from_spec(spec: &RemoteSpec) -> Self {
        Self::with_counters(spec, Arc::new(TransportCounters::default()))
    }

    pub(crate) fn with_counters(spec: &RemoteSpec, stats: Arc<TransportCounters>) -> Self {
        Self {
            source: Arc::clone(&spec.source),
            len: spec.len,
            whole: spec.whole.clone(),
            stats,
            cache: Arc::new(RangeCache::default()),
        }
    }

    /// The cache a prefetching scan populates.
    pub(crate) fn cache(&self) -> Arc<RangeCache> {
        Arc::clone(&self.cache)
    }
}

impl Length for RemoteFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for RemoteFile {
    type T = RemoteRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len {
            return Err(ParquetError::General(format!(
                "remote parquet read at offset {start} is past the {}-byte object",
                self.len
            )));
        }
        Ok(match &self.whole {
            Some(bytes) => {
                let start = usize::try_from(start).map_err(|_| {
                    ParquetError::General(format!(
                        "remote parquet offset {start} does not fit this platform's usize"
                    ))
                })?;
                RemoteRead::Whole(Cursor::new(bytes.slice(start..)))
            }
            None => match self.cache.read_from(start) {
                Some(bytes) => RemoteRead::Cached(CachedRead {
                    buf: bytes,
                    offset: 0,
                }),
                None => RemoteRead::Ranged(RangedRead {
                    source: Arc::clone(&self.source),
                    pos: start,
                    end: self.len,
                    buf: Vec::new(),
                    offset: 0,
                    stats: Arc::clone(&self.stats),
                }),
            },
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::General("remote parquet range overflows u64".into()))?;
        if end > self.len {
            return Err(ParquetError::General(format!(
                "remote parquet read {start}..{end} is past the {}-byte object",
                self.len
            )));
        }
        match &self.whole {
            Some(bytes) => {
                let start = usize::try_from(start).map_err(|_| {
                    ParquetError::General(format!(
                        "remote parquet offset {start} does not fit this platform's usize"
                    ))
                })?;
                let end = usize::try_from(end).map_err(|_| {
                    ParquetError::General(format!(
                        "remote parquet offset {end} does not fit this platform's usize"
                    ))
                })?;
                Ok(bytes.slice(start..end))
            }
            None => match self.cache.exact(start, length as u64) {
                Some(bytes) => Ok(bytes),
                None => counted_range(self.source.as_ref(), start, length as u64, &self.stats)
                    .map(Bytes::from)
                    .map_err(range_error),
            },
        }
    }
}

fn range_error(message: String) -> ParquetError {
    ParquetError::General(format!("remote parquet read failed: {message}"))
}

/// The `Read` handle parquet's page decoder consumes.
#[derive(Debug)]
pub(crate) enum RemoteRead {
    /// Unknown-size fallback: an in-memory cursor (cheap `Bytes` clones).
    Whole(Cursor<Bytes>),
    /// A prefetched row group: serve from memory.
    Cached(CachedRead),
    /// Lazy ranged reads (serial path / non-prefetched reads).
    Ranged(RangedRead),
}

impl Read for RemoteRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            RemoteRead::Whole(cursor) => cursor.read(buf),
            RemoteRead::Cached(read) => read.read(buf),
            RemoteRead::Ranged(read) => read.read(buf),
        }
    }
}

/// A `Read` over one prefetched byte range. A short response (or a decoder
/// that reads past the fetched chunk) surfaces as EOF, never as a silent
/// second fetch — a truncated range must fail the scan.
#[derive(Debug)]
pub(crate) struct CachedRead {
    buf: Bytes,
    offset: usize,
}

impl Read for CachedRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.offset >= self.buf.len() {
            return Ok(0);
        }
        let n = out.len().min(self.buf.len() - self.offset);
        out[..n].copy_from_slice(&self.buf[self.offset..self.offset + n]);
        self.offset += n;
        Ok(n)
    }
}

/// A lazy `Read` over one object: fetches `min(READ_BLOCK, remaining)` bytes
/// at the current offset on demand and serves the caller from that buffer.
#[derive(Debug)]
pub(crate) struct RangedRead {
    source: Arc<dyn RangeSource>,
    pos: u64,
    end: u64,
    buf: Vec<u8>,
    offset: usize,
    stats: Arc<TransportCounters>,
}

impl Read for RangedRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.pos >= self.end {
            return Ok(0);
        }
        if self.offset >= self.buf.len() {
            let want = (self.end - self.pos).min(READ_BLOCK.max(out.len() as u64));
            let bytes = counted_range(self.source.as_ref(), self.pos, want, &self.stats)
                .map_err(io::Error::other)?;
            if bytes.is_empty() {
                return Ok(0);
            }
            self.buf = bytes;
            self.offset = 0;
        }
        let n = out.len().min(self.buf.len() - self.offset);
        out[..n].copy_from_slice(&self.buf[self.offset..self.offset + n]);
        self.offset += n;
        self.pos += n as u64;
        Ok(n)
    }
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
            },]
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
    }

    #[test]
    fn a_cached_range_serves_reads_and_is_removed() {
        let cache = RangeCache::default();
        cache.insert(100, (0u8..50).collect());
        assert_eq!(cache.read_from(100).unwrap().len(), 50);
        assert_eq!(&cache.read_from(120).unwrap()[..3], &[20, 21, 22]);
        assert_eq!(&cache.exact(110, 4).unwrap()[..], &[10, 11, 12, 13]);
        assert!(cache.exact(140, 20).is_none());
        assert_eq!(cache.len(), 1);
        cache.remove(&[100]);
        assert_eq!(cache.len(), 0);
    }
}
