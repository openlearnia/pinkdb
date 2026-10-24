//! Bounded read-ahead for a reader that consumes a remote object in order
//! (wave 98c): one thread issues sequential ranged GETs a window ahead of the
//! parser, so a CSV scan overlaps download and parse and holds at most the
//! window's bytes. The same model as the Parquet read-ahead in wave 98a — a
//! bounded thread, no async runtime.

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use crate::fetch::Fetcher;

/// Bytes per ranged GET.
pub const DEFAULT_CHUNK_BYTES: u64 = 1024 * 1024;

/// Bytes downloaded but not yet parsed (the in-flight bound). The reader also
/// holds the chunk being fetched, so the peak is one chunk more.
pub const DEFAULT_WINDOW_BYTES: u64 = 8 * 1024 * 1024;

/// Counters for one [`Prefetch`], readable while it runs and after the scan
/// dropped it (the tests assert the window bound through them).
#[derive(Debug, Default)]
pub struct PrefetchStats {
    requests: AtomicU64,
    bytes: AtomicU64,
    peak_buffered_bytes: AtomicU64,
}

impl PrefetchStats {
    /// Ranged GETs issued (at least one per chunk; a retried chunk counts
    /// once, since the transport retries inside the call).
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Bytes downloaded.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// High-water mark of downloaded-but-unparsed bytes.
    pub fn peak_buffered_bytes(&self) -> u64 {
        self.peak_buffered_bytes.load(Ordering::Relaxed)
    }
}

/// A [`Read`] over a remote object that downloads ranged GETs ahead of the
/// consumer. Sequential by design: one request in flight, `window / chunk`
/// chunks queued, so the parser never waits for the network with a full
/// window in hand.
pub struct Prefetch {
    receiver: Receiver<Result<Vec<u8>, String>>,
    /// The chunk being consumed and how much of it was handed out.
    current: Vec<u8>,
    pos: usize,
    done: bool,
    stats: Arc<PrefetchStats>,
    /// Downloaded, not yet parsed — shared with the pump thread.
    buffered: Arc<AtomicU64>,
}

impl Prefetch {
    /// Start reading `fetcher` from byte 0 with the default chunk and window.
    pub fn open(fetcher: Box<dyn Fetcher>) -> Self {
        Self::open_with(fetcher, DEFAULT_CHUNK_BYTES, DEFAULT_WINDOW_BYTES)
    }

    /// Start after `head`, the file's already-fetched beginning (the CSV
    /// bind's schema probe): it is the reader's first chunk and the pump
    /// resumes after it, so no byte is downloaded twice.
    pub fn open_after_head(fetcher: Box<dyn Fetcher>, head: Vec<u8>) -> Self {
        Self::start(fetcher, DEFAULT_CHUNK_BYTES, DEFAULT_WINDOW_BYTES, head)
    }

    /// Start reading with an explicit chunk size and in-flight window.
    pub fn open_with(fetcher: Box<dyn Fetcher>, chunk: u64, window: u64) -> Self {
        Self::start(fetcher, chunk, window, Vec::new())
    }

    fn start(mut fetcher: Box<dyn Fetcher>, chunk: u64, window: u64, head: Vec<u8>) -> Self {
        let chunk = chunk.max(1);
        // One chunk is fetched while `capacity` chunks wait in the queue.
        let capacity = (window / chunk).max(1) as usize;
        let (sender, receiver) = sync_channel(capacity);
        let stats = Arc::new(PrefetchStats::default());
        let buffered = Arc::new(AtomicU64::new(0));
        let size = fetcher.size().ok().flatten();
        let start = head.len() as u64;
        if !head.is_empty() {
            buffered.fetch_add(start, Ordering::Relaxed);
            // Capacity is at least one chunk, so the first send never blocks.
            let _ = sender.send(Ok(head));
        }
        let pump_stats = Arc::clone(&stats);
        let pump_buffered = Arc::clone(&buffered);
        thread::spawn(move || {
            pump(
                &mut *fetcher,
                size,
                chunk,
                start,
                &sender,
                &pump_stats,
                &pump_buffered,
            )
        });
        Self {
            receiver,
            current: Vec::new(),
            pos: 0,
            done: false,
            stats,
            buffered,
        }
    }

    pub fn stats(&self) -> Arc<PrefetchStats> {
        Arc::clone(&self.stats)
    }
}

impl Read for Prefetch {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            if self.pos < self.current.len() {
                let n = (self.current.len() - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
                self.pos += n;
                self.buffered.fetch_sub(n as u64, Ordering::Relaxed);
                return Ok(n);
            }
            if self.done {
                return Ok(0);
            }
            self.current.clear();
            self.pos = 0;
            match self.receiver.recv() {
                Ok(Ok(bytes)) if bytes.is_empty() => self.done = true,
                Ok(Ok(bytes)) => self.current = bytes,
                Ok(Err(message)) => {
                    self.done = true;
                    return Err(io::Error::other(message));
                }
                // The pump ended without an error: the object is exhausted.
                Err(_) => self.done = true,
            }
        }
    }
}

/// Download `size` bytes as sequential ranges, blocking on the queue. The
/// transport retries a failed range itself; an error that survives the retries
/// is handed to the reader once and ends the pump.
fn pump(
    fetcher: &mut dyn Fetcher,
    size: Option<u64>,
    chunk: u64,
    start: u64,
    sender: &SyncSender<Result<Vec<u8>, String>>,
    stats: &PrefetchStats,
    buffered: &AtomicU64,
) {
    let Some(size) = size else {
        // An unknown size rules out ranged GETs: fetch once and hand it over.
        let _ = sender.send(
            fetcher
                .fetch()
                .map_err(|e| e.to_string())
                .inspect(|bytes| count(bytes, stats, buffered)),
        );
        return;
    };
    let mut offset = start;
    while offset < size {
        let len = (size - offset).min(chunk);
        match fetcher.fetch_range(offset, len) {
            Ok(bytes) => {
                count(&bytes, stats, buffered);
                if bytes.is_empty() {
                    return;
                }
                offset += bytes.len() as u64;
                if sender.send(Ok(bytes)).is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = sender.send(Err(e.to_string()));
                return;
            }
        }
    }
}

fn count(bytes: &[u8], stats: &PrefetchStats, buffered: &AtomicU64) {
    let n = bytes.len() as u64;
    stats.requests.fetch_add(1, Ordering::Relaxed);
    stats.bytes.fetch_add(n, Ordering::Relaxed);
    let held = buffered.fetch_add(n, Ordering::Relaxed) + n;
    stats.peak_buffered_bytes.fetch_max(held, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::fetch::{HttpfsConfig, open_with};
    use crate::test_http::TestHttpServer;

    const CHUNK: u64 = 1000;
    const WINDOW: u64 = 4000;

    fn open(server: &TestHttpServer, path: &str) -> Prefetch {
        let config = HttpfsConfig {
            allow_http: true,
            ..Default::default()
        };
        Prefetch::open_with(
            open_with(&server.url(path), &config).unwrap(),
            CHUNK,
            WINDOW,
        )
    }

    fn body(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn reads_a_file_in_bounded_windows() {
        let server = TestHttpServer::start();
        let bytes = body(10_000);
        server.put("/big.bin", &bytes);
        let mut reader = open(&server, "/big.bin");
        let mut got = Vec::new();
        reader.read_to_end(&mut got).unwrap();
        assert_eq!(got, bytes);

        let stats = reader.stats();
        let requests = stats.requests.load(Ordering::Relaxed);
        let peak = stats.peak_buffered_bytes.load(Ordering::Relaxed);
        assert_eq!(requests, 10, "one request per chunk");
        // Bounded by the in-flight window plus the chunk being fetched, never
        // by the file.
        assert!(peak <= WINDOW + CHUNK, "peak buffered {peak} bytes");

        let mut ranges: Vec<String> = server
            .ranges()
            .into_iter()
            .filter(|range| range.starts_with("bytes="))
            .collect();
        ranges.sort_by_key(|range| {
            range
                .strip_prefix("bytes=")
                .and_then(|r| r.split_once('-'))
                .and_then(|(start, _)| start.parse::<u64>().ok())
                .unwrap_or(u64::MAX)
        });
        assert_eq!(
            ranges,
            (0..10)
                .map(|i| format!("bytes={}-{}", i * CHUNK, (i + 1) * CHUNK - 1))
                .collect::<Vec<_>>()
        );
    }

    /// A range that fails mid-file is retried by the transport; the reader
    /// sees the byte sequence, not the error. This is what lets the CSV parser
    /// overlap its work with the download.
    #[test]
    fn a_mid_file_error_is_retried() {
        let server = TestHttpServer::start();
        let bytes = body(5000);
        server.put("/flaky.bin", &bytes);
        // Request 4 is the third ranged GET (the HEAD counts as the first), a
        // chunk in the middle of the file: it fails once, the transport
        // retries it, and the reader never sees the error.
        server.fail_at("/flaky.bin", 4, 500);
        let mut reader = open(&server, "/flaky.bin");
        let mut got = Vec::new();
        reader.read_to_end(&mut got).unwrap();
        assert_eq!(got, bytes);

        // Every byte was fetched, once, in order; only the failed range was
        // requested again.
        let ranges = server.ranges();
        let mut spans: Vec<(u64, u64)> = ranges
            .iter()
            .filter_map(|range| range.strip_prefix("bytes="))
            .map(|range| {
                let (start, end) = range.split_once('-').unwrap();
                (start.parse().unwrap(), end.parse().unwrap())
            })
            .collect();
        spans.sort_unstable();
        spans.dedup();
        assert_eq!(spans.first(), Some(&(0, CHUNK - 1)));
        assert_eq!(
            spans.last().map(|(_, end)| *end + 1),
            Some(bytes.len() as u64)
        );
        for pair in spans.windows(2) {
            assert_eq!(pair[0].1 + 1, pair[1].0, "a gap or overlap: {spans:?}");
        }
        let ranged = ranges.iter().filter(|r| r.starts_with("bytes=")).count();
        assert_eq!(ranged, spans.len() + 1, "no retry happened: {ranges:?}");
    }

    /// A prefix handed in (the CSV schema probe) is not fetched again.
    #[test]
    fn a_known_head_is_not_downloaded_twice() {
        let server = TestHttpServer::start();
        let bytes = body(3000);
        server.put("/head.bin", &bytes);
        let config = HttpfsConfig {
            allow_http: true,
            ..Default::default()
        };
        let fetcher = open_with(&server.url("/head.bin"), &config).unwrap();
        let head = fetcher.fetch_range(0, CHUNK).unwrap();
        let mut reader = Prefetch::open_after_head(fetcher, head);
        let mut got = Vec::new();
        reader.read_to_end(&mut got).unwrap();
        assert_eq!(got, bytes);
        let ranges = server.ranges();
        assert_eq!(
            ranges.iter().filter(|r| r.starts_with("bytes=0-")).count(),
            1,
            "the handed-in head was fetched again: {ranges:?}"
        );
    }
}
