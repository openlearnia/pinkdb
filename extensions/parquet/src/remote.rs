//! Remote parquet objects (`remote` feature): parquet's [`ChunkReader`] over
//! [`pinkdb_httpfs::Fetcher`].
//!
//! # What is ranged and what is not
//!
//! - The **footer** is read as parquet asks: the 8-byte trailer
//!   (`get_bytes(len-8, 8)`), then the thrift metadata bytes, then the
//!   footer-size probe — all `Range` requests, never a whole-object GET.
//! - **Row groups / column chunks** are read through [`RemoteRead`]. The
//!   default path (wave 98a) computes each projected column chunk's byte
//!   range from the footer, coalesces adjacent chunks with small gaps, and
//!   fetches every range of the next row groups with a bounded pool of
//!   concurrent requests (one request per chunk/run, decoded from memory).
//!   `PINKDB_HTTPFS_PARALLELISM=1` (or `0`) restores the legacy serial path:
//!   a lazy `Read` that issues a `Range` request for at most [`READ_BLOCK`]
//!   bytes at a time as the decoder consumes page headers/pages.
//! - **Unknown size** (a `HEAD` that answers without `Content-Length` or
//!   with 405/501) is the only whole-object fallback: the object is fetched
//!   once, the footer is read from memory, and the same bytes serve the scan
//!   (`RemoteSpec::whole`). A backend that reports a size never downloads the
//!   whole object.
//! - Single objects only: remote globs/directories are refused, and the
//!   path is the exact key/URL (`?query`/`#fragment` are kept for the
//!   transport but ignored for extension/hive decisions).
//!
//! `https://` and `s3://` are served by the shared transport; plain `http://`
//! follows its default (refused unless `PINKDB_HTTPFS_ALLOW_HTTP=1`).
//!
//! # Remote reads (wave 98a)
//!
//! A remote scan used to pay one round trip per 128 KiB of every projected
//! column chunk, serially. Wave 98a adds:
//!
//! - **Counters**: every transport call a scan makes is counted
//!   ([`TransportCounters`] → [`RemoteReadStats`]); the source exposes the
//!   per-session totals through `ParquetSource::remote_stats`.
//! - **Request per byte range**: for each surviving row group the projected
//!   chunks' `[dictionary/data-page offset, compressed size]` ranges are
//!   coalesced across gaps up to [`COALESCE_GAP`] (bounded at
//!   [`MAX_REQUEST_BYTES`]) and fetched one request per run.
//! - **Read-ahead**: a bounded `std::thread` pool (`PINKDB_HTTPFS_PARALLELISM`,
//!   default [`pinkdb_httpfs::DEFAULT_PARALLELISM`], 0/1 = serial) fetches the
//!   next row groups while earlier ones decode; per-window bytes are bounded
//!   by [`MAX_PREFETCH_BYTES`]. Results are decoded and assembled in
//!   row-group order, so output order and values are unchanged.
//! - **Metadata-only counts**: a scan whose projection needs no file column
//!   (`COUNT(*)`) emits its chunks from the footer's row counts and issues no
//!   data-page request at all (local files open no reader either).
//! - **Connection reuse**: the scan uses one `Fetcher` (one `ureq::Agent`
//!   pool); the transport sizes its idle-connection pool to at least the
//!   in-flight limit, so no request pays a fresh TLS setup.
//! - **Errors/retries**: each range request still goes through the
//!   transport's retry policy and maps to the same `remote parquet read
//!   failed: …` error shape.

#[cfg(feature = "remote")]
use std::io::{self, Cursor, Read};
#[cfg(feature = "remote")]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(feature = "remote")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "remote")]
use std::time::Instant;

#[cfg(feature = "remote")]
use bytes::Bytes;
#[cfg(feature = "remote")]
use parquet::errors::{ParquetError, Result as ParquetResult};
#[cfg(feature = "remote")]
use parquet::file::reader::{ChunkReader, Length};
use pinkdb::PinkDbError;
#[cfg(feature = "remote")]
use pinkdb::Result;
#[cfg(feature = "remote")]
use pinkdb_httpfs::Fetcher;

/// The maximum bytes one lazy read fetches ahead on the serial path. Page
/// headers are tiny; 128 KiB keeps the request count low without turning a
/// scan into a whole-column-chunk download.
#[cfg(feature = "remote")]
pub(crate) const READ_BLOCK: u64 = 128 * 1024;

/// Coalesce projected column chunks whose gap is at most this many bytes into
/// one range request (wave 98a). Adjacent chunks always coalesce.
#[cfg(feature = "remote")]
pub(crate) const COALESCE_GAP: u64 = 1024 * 1024;

/// The largest single coalesced range request (wave 98a).
#[cfg(feature = "remote")]
pub(crate) const MAX_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

/// The most compressed bytes one read-ahead window buffers at once (wave
/// 98a). A row group larger than this is still fetched alone.
#[cfg(feature = "remote")]
pub(crate) const MAX_PREFETCH_BYTES: u64 = 128 * 1024 * 1024;

/// Per-scan transport counters (wave 98a): requests issued, bytes received,
/// and wall time spent waiting on the network. Requests are the footer/data
/// range GETs the scan itself issues (the bind-time HEAD/footer resolution is
/// not part of a scan).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemoteReadStats {
    /// Transport calls (footer and data range GETs) made by the scan.
    pub requests: u64,
    /// Response bytes received across those calls.
    pub bytes: u64,
    /// Wall-clock nanoseconds spent inside transport calls.
    pub wait_nanos: u64,
}

#[cfg(feature = "remote")]
#[derive(Debug, Default)]
pub(crate) struct TransportCounters {
    pub(crate) requests: AtomicU64,
    pub(crate) bytes: AtomicU64,
    pub(crate) wait_nanos: AtomicU64,
}

#[cfg(feature = "remote")]
impl TransportCounters {
    pub(crate) fn snapshot(&self) -> RemoteReadStats {
        RemoteReadStats {
            requests: self.requests.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            wait_nanos: self.wait_nanos.load(Ordering::Relaxed),
        }
    }
}

/// Whether `path` names a remote object (https, plain-http test endpoints, or
/// S3). Feature-independent so the parsers can claim remote extensions and
/// give a clear "rebuild with `remote`" error in a lean build.
pub fn is_remote(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("https://") || lower.starts_with("http://") || lower.starts_with("s3://")
}

/// The path component used for extension/hive decisions (query/fragment
/// stripped). The transport still receives the full URL.
pub fn path_only(path: &str) -> &str {
    path.split(['?', '#']).next().unwrap_or(path)
}

/// The clear lean-build error for a remote path (used when the feature is
/// off; present in both builds so the parsers can name the fix).
#[allow(dead_code)]
pub fn missing_feature(path: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "cannot read remote parquet object \"{path}\": this build has no remote parquet support; \
         rebuild pinkdb-parquet with the `remote` feature (the CLI's `httpfs` feature enables \
         it together with `parquet`)"
    ))
}

/// A remote file's transport + size, cached in the resolved scan so a
/// bind→scan pair reuses one connection pool and one size lookup.
#[cfg(feature = "remote")]
#[derive(Debug, Clone)]
pub(crate) struct RemoteSpec {
    pub(crate) fetcher: Arc<dyn Fetcher>,
    /// The object size (from HEAD, or the whole body when the size was
    /// unknown).
    pub(crate) len: u64,
    /// `Some` only for the unknown-size fallback: the whole object, fetched
    /// once and reused for metadata + scan.
    pub(crate) whole: Option<Bytes>,
    /// In-flight read-ahead limit for scans of this object (wave 98a):
    /// `PINKDB_HTTPFS_PARALLELISM`, 0/1 = the legacy serial path.
    pub(crate) parallelism: usize,
}

#[cfg(feature = "remote")]
fn http_error(error: pinkdb_httpfs::Error) -> PinkDbError {
    PinkDbError::Execution(format!("remote parquet read failed: {error}"))
}

#[cfg(feature = "remote")]
fn parquet_http_error(error: pinkdb_httpfs::Error) -> ParquetError {
    ParquetError::General(format!("remote parquet read failed: {error}"))
}

/// One counted range request: the transport's own retry policy still applies
/// (this only observes the call).
#[cfg(feature = "remote")]
fn counted_range(
    fetcher: &dyn Fetcher,
    start: u64,
    len: u64,
    stats: &TransportCounters,
) -> std::result::Result<Vec<u8>, pinkdb_httpfs::Error> {
    stats.requests.fetch_add(1, Ordering::Relaxed);
    let begin = Instant::now();
    let result = fetcher.fetch_range(start, len);
    stats
        .wait_nanos
        .fetch_add(begin.elapsed().as_nanos() as u64, Ordering::Relaxed);
    if let Ok(bytes) = &result {
        stats.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }
    result
}

/// Open one remote object. A known size keeps the reader ranged; an unknown
/// size is the documented whole-object fallback.
#[cfg(feature = "remote")]
pub(crate) fn open_spec(path: &str) -> Result<RemoteSpec> {
    let fetcher: Arc<dyn Fetcher> = Arc::from(pinkdb_httpfs::open(path).map_err(http_error)?);
    let parallelism = pinkdb_httpfs::parallelism_from_env();
    match fetcher.size().map_err(http_error)? {
        Some(len) => Ok(RemoteSpec {
            fetcher,
            len,
            whole: None,
            parallelism,
        }),
        None => {
            let bytes = fetcher.fetch().map_err(http_error)?;
            Ok(RemoteSpec {
                fetcher,
                len: bytes.len() as u64,
                whole: Some(Bytes::from(bytes)),
                parallelism,
            })
        }
    }
}

/// Read one remote object's metadata (footer only; no data pages).
#[cfg(feature = "remote")]
pub(crate) fn read_meta(path: &str) -> Result<crate::file_meta::FileMeta> {
    let spec = open_spec(path)?;
    crate::file_meta::read_file_meta_remote(path, &spec)
}

/// Resolve one remote object: its metadata plus the transport handle the scan
/// reuses. Refuses globs (a remote path is one object).
#[cfg(feature = "remote")]
pub(crate) fn resolve_file(path: &str) -> Result<(crate::file_meta::FileMeta, RemoteSpec)> {
    if has_glob(path) {
        return Err(PinkDbError::Execution(format!(
            "remote parquet paths are single objects, not globs: \"{path}\" contains a glob \
             metacharacter; list the objects yourself or read a single key"
        )));
    }
    let spec = open_spec(path)?;
    let meta = crate::file_meta::read_file_meta_remote(path, &spec)?;
    Ok((meta, spec))
}

/// Whether the path component carries a glob metacharacter (the query is
/// ignored, so a signed URL's `?` is not a glob).
#[cfg(feature = "remote")]
pub fn has_glob(path: &str) -> bool {
    path_only(path).contains(['*', '?', '['])
}

/// `metadata_len` from the parquet trailer (`get_bytes(len-8, 8)`), read from
/// the cache or with one range request.
#[cfg(feature = "remote")]
pub(crate) fn footer_size(spec: &RemoteSpec) -> Result<u64> {
    if spec.len < 12 {
        return Ok(0);
    }
    let trailer = match &spec.whole {
        Some(bytes) => bytes.slice(bytes.len() - 8..).to_vec(),
        None => spec
            .fetcher
            .fetch_range(spec.len - 8, 8)
            .map_err(http_error)?,
    };
    if trailer.len() < 4 {
        return Ok(0);
    }
    Ok(u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as u64)
}

/// One byte range a scan wants to fetch.
#[cfg(feature = "remote")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteSpan {
    pub(crate) start: u64,
    pub(crate) len: u64,
}

#[cfg(feature = "remote")]
impl ByteSpan {
    pub(crate) fn end(&self) -> u64 {
        self.start.saturating_add(self.len)
    }
}

/// The byte range of one column chunk a page decoder will read: from the
/// dictionary page offset (when present) or the data page offset, for the
/// chunk's compressed size.
#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
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

/// The compact row-group byte window a prefetched fetch serves, keyed by its
/// start offset.
#[cfg(feature = "remote")]
#[derive(Debug)]
struct CachedRange {
    start: u64,
    end: u64,
    bytes: Bytes,
}

/// The read-ahead cache shared by a scan's `RemoteFile` and its prefetch
/// requests. Sequential decoding still happens through the same
/// `SerializedFileReader`; a populated range is served from memory.
#[cfg(feature = "remote")]
#[derive(Debug, Default)]
pub(crate) struct RangeCache {
    ranges: Mutex<Vec<CachedRange>>,
}

#[cfg(feature = "remote")]
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
/// per span), preserving result order. The caller bounds the total bytes in a
/// window; this bounds the concurrency.
#[cfg(feature = "remote")]
pub(crate) fn fetch_spans(
    fetcher: &dyn Fetcher,
    spans: &[ByteSpan],
    parallelism: usize,
    stats: &TransportCounters,
) -> Vec<std::result::Result<Vec<u8>, pinkdb_httpfs::Error>> {
    let results: Vec<Option<std::result::Result<Vec<u8>, pinkdb_httpfs::Error>>> =
        (0..spans.len()).map(|_| None).collect();
    if spans.is_empty() {
        return Vec::new();
    }
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
                    let result = counted_range(fetcher, span.start, span.len, stats);
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

/// Parquet's [`ChunkReader`] over a remote object.
#[cfg(feature = "remote")]
#[derive(Debug)]
pub(crate) struct RemoteFile {
    fetcher: Arc<dyn Fetcher>,
    len: u64,
    /// `Some` for the unknown-size fallback; the whole object serves every
    /// read without further requests.
    whole: Option<Bytes>,
    /// Per-scan transport counters (wave 98a).
    stats: Arc<TransportCounters>,
    /// Read-ahead ranges installed by the scan before decoding a row group.
    cache: Arc<RangeCache>,
}

#[cfg(feature = "remote")]
impl RemoteFile {
    pub(crate) fn from_spec(spec: &RemoteSpec) -> Self {
        Self::with_counters(spec, Arc::new(TransportCounters::default()))
    }

    pub(crate) fn with_counters(spec: &RemoteSpec, stats: Arc<TransportCounters>) -> Self {
        Self {
            fetcher: Arc::clone(&spec.fetcher),
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

#[cfg(feature = "remote")]
impl Length for RemoteFile {
    fn len(&self) -> u64 {
        self.len
    }
}

#[cfg(feature = "remote")]
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
                    fetcher: Arc::clone(&self.fetcher),
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
                None => counted_range(self.fetcher.as_ref(), start, length as u64, &self.stats)
                    .map(Bytes::from)
                    .map_err(parquet_http_error),
            },
        }
    }
}

/// The `Read` handle parquet's page decoder consumes.
#[cfg(feature = "remote")]
pub(crate) enum RemoteRead {
    /// Unknown-size fallback: an in-memory cursor (cheap `Bytes` clones).
    Whole(Cursor<Bytes>),
    /// A prefetched row group (wave 98a): serve from memory, falling back to
    /// a ranged read only if the decoder runs past the fetched window.
    Cached(CachedRead),
    /// Lazy ranged reads (serial path / non-prefetched reads).
    Ranged(RangedRead),
}

#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
#[derive(Debug)]
pub(crate) struct CachedRead {
    buf: Bytes,
    offset: usize,
}

#[cfg(feature = "remote")]
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
#[cfg(feature = "remote")]
#[derive(Debug)]
pub(crate) struct RangedRead {
    fetcher: Arc<dyn Fetcher>,
    pos: u64,
    end: u64,
    buf: Vec<u8>,
    offset: usize,
    stats: Arc<TransportCounters>,
}

#[cfg(feature = "remote")]
impl Read for RangedRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.pos >= self.end {
            return Ok(0);
        }
        if self.offset >= self.buf.len() {
            let want = (self.end - self.pos).min(READ_BLOCK.max(out.len() as u64));
            let bytes = counted_range(self.fetcher.as_ref(), self.pos, want, &self.stats)
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

#[cfg(all(test, feature = "remote"))]
mod tests {
    use super::*;

    #[test]
    fn remote_detection_and_path_component() {
        assert!(is_remote("https://h/x.parquet"));
        assert!(is_remote("HTTP://h/x.parquet"));
        assert!(is_remote("s3://b/x.parquet"));
        assert!(!is_remote("x.parquet"));
        assert!(!is_remote("file:///x.parquet"));
        assert_eq!(
            path_only("https://h/x.parquet?token=1#frag"),
            "https://h/x.parquet"
        );
        assert!(!has_glob("https://h/x.parquet?token=*"));
        assert!(has_glob("https://h/*.parquet"));
    }

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
