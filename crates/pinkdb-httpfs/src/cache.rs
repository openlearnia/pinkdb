//! An optional on-disk cache of remote byte ranges (wave 98c).
//!
//! Off unless [`CacheOptions::from_env`] finds `PINKDB_EXTERNAL_FILE_CACHE`
//! set; [`crate::fetch::open_with`] then wraps every fetcher, so the parquet
//! read-ahead, the CSV read-ahead and the lake's ranged reads all hit it.
//!
//! Entries are keyed by URL and ETag. A range is served only when the object's
//! ETag still matches the one its entries were written for, so a rewritten
//! object invalidates the whole URL at once and never yields another
//! version's bytes. A corrupt or half-written entry is a miss, never an error.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::Result;
use crate::fetch::Fetcher;

/// The most bytes the cache directory may hold before LRU eviction runs.
/// One GiB holds a TPC-H SF1 Parquet scan with room to spare.
pub const DEFAULT_CACHE_BYTES: u64 = 1 << 30;

/// Where the cache lives and how big it may get.
#[derive(Debug, Clone)]
pub struct CacheOptions {
    pub dir: PathBuf,
    pub capacity: u64,
}

impl CacheOptions {
    /// The cache is **off** unless `PINKDB_EXTERNAL_FILE_CACHE` is truthy
    /// (`1`/`on`/`true`/`yes`). The directory is
    /// `PINKDB_EXTERNAL_FILE_CACHE_DIR` (default: a subdirectory of the
    /// system temporary directory) and the cap is
    /// `PINKDB_EXTERNAL_FILE_CACHE_BYTES` (default 1 GiB).
    pub fn from_env() -> Option<CacheOptions> {
        if !enabled() {
            return None;
        }
        let dir = std::env::var_os("PINKDB_EXTERNAL_FILE_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("pinkdb-external-file-cache"));
        let capacity = std::env::var("PINKDB_EXTERNAL_FILE_CACHE_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_CACHE_BYTES);
        Some(CacheOptions { dir, capacity })
    }
}

/// Whether the environment asks for the cache.
pub fn enabled() -> bool {
    std::env::var("PINKDB_EXTERNAL_FILE_CACHE").is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "on" | "true" | "yes"
        )
    })
}

/// The magic every entry starts with; a different leading byte means the file
/// is not ours and the entry is discarded.
const MAGIC: &[u8; 8] = b"PKDBRC1\n";
/// The ETag of the object a directory's entries are for.
const ETAG_FILE: &str = "etag";

/// Wrap `inner` so ranged reads go through the cache.
pub(crate) fn wrap(
    inner: Box<dyn Fetcher>,
    options: Option<&CacheOptions>,
) -> Result<Box<dyn Fetcher>> {
    let Some(options) = options else {
        return Ok(inner);
    };
    // Without an ETag there is no key that can tell two versions apart, so the
    // fetcher is left uncached rather than risking stale bytes.
    let etag = inner.etag().unwrap_or_default();
    let Some(etag) = etag else {
        return Ok(inner);
    };
    let cache = DiskCache::open(options)?;
    let url = inner.url().to_string();
    Ok(Box::new(Cached {
        inner,
        cache,
        url,
        etag,
    }))
}

/// A fetcher whose ranged reads are served from (and written to) the cache.
/// The ETag is pinned when the file is opened: one query sees one version.
#[derive(Debug)]
struct Cached {
    inner: Box<dyn Fetcher>,
    cache: Arc<DiskCache>,
    url: String,
    etag: String,
}

impl Fetcher for Cached {
    fn url(&self) -> &str {
        &self.url
    }

    fn size(&self) -> Result<Option<u64>> {
        self.inner.size()
    }

    fn etag(&self) -> Result<Option<String>> {
        Ok(Some(self.etag.clone()))
    }

    fn fetch(&self) -> Result<Vec<u8>> {
        // Whole-object reads are small by construction (read-ahead windows are
        // ranged), and caching them would let them crowd out range entries.
        self.inner.fetch()
    }

    fn fetch_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if let Some(bytes) = self.cache.get(&self.url, &self.etag, offset, len) {
            return Ok(bytes);
        }
        let bytes = self.inner.fetch_range(offset, len)?;
        self.cache.put(&self.url, &self.etag, offset, len, &bytes);
        Ok(bytes)
    }
}

#[derive(Debug)]
struct DiskCache {
    root: PathBuf,
    capacity: u64,
    /// Serializes writes and eviction; readers take no lock.
    write: Mutex<()>,
}

impl DiskCache {
    fn open(options: &CacheOptions) -> Result<Arc<DiskCache>> {
        fs::create_dir_all(&options.dir).map_err(|e| io_error(&options.dir, e))?;
        set_mode(&options.dir, 0o700);
        Ok(Arc::new(DiskCache {
            root: options.dir.clone(),
            capacity: options.capacity,
            write: Mutex::new(()),
        }))
    }

    /// One entry's payload, or `None` for a miss (missing, other ETag, or
    /// corrupt — corrupt entries are removed so they cannot be a hit twice).
    fn get(&self, url: &str, etag: &str, offset: u64, len: u64) -> Option<Vec<u8>> {
        let dir = self.root.join(hash(url));
        if read_text(&dir.join(ETAG_FILE)).as_deref() != Some(etag) {
            return None;
        }
        let path = dir.join(entry_name(offset, len));
        let entry = fs::read(&path).ok()?;
        let payload = match split_entry(&entry, url, etag, offset, len) {
            Some(payload) => payload.to_vec(),
            None => {
                let _ = fs::remove_file(&path);
                return None;
            }
        };
        touch(&path);
        Some(payload)
    }

    fn put(&self, url: &str, etag: &str, offset: u64, len: u64, bytes: &[u8]) {
        let _guard = self.write.lock().unwrap_or_else(|p| p.into_inner());
        let dir = self.root.join(hash(url));
        if read_text(&dir.join(ETAG_FILE)).as_deref() != Some(etag) {
            // A new version of the object: drop every range of the old one.
            let _ = fs::remove_dir_all(&dir);
            if fs::create_dir_all(&dir).is_err() {
                return;
            }
            set_mode(&dir, 0o700);
            if write_file(&dir.join(ETAG_FILE), etag.as_bytes()).is_err() {
                return;
            }
        }
        let path = dir.join(entry_name(offset, len));
        if write_file(&path, &encode_entry(url, etag, offset, len, bytes)).is_ok() {
            self.evict(&path);
        }
    }

    /// Drop the least recently written-or-read entries until under the cap.
    /// `keep` is the entry that was just written: it is the newest one, and
    /// file timestamps can tie, so it is skipped by name.
    fn evict(&self, keep: &Path) {
        let entries = self.entries();
        let mut total: u64 = entries.iter().map(|(_, len, _)| *len).sum();
        for (_, len, path) in entries {
            if total <= self.capacity {
                break;
            }
            if path == keep {
                continue;
            }
            if fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(len);
            }
        }
    }

    /// Every entry file, oldest first.
    fn entries(&self) -> Vec<(SystemTime, u64, PathBuf)> {
        let mut entries = Vec::new();
        let Ok(urls) = fs::read_dir(&self.root) else {
            return entries;
        };
        for url in urls.flatten() {
            let Ok(files) = fs::read_dir(url.path()) else {
                continue;
            };
            for file in files.flatten() {
                let path = file.path();
                if path.extension().and_then(|e| e.to_str()) != Some("range") {
                    continue;
                }
                let Ok(meta) = file.metadata() else { continue };
                let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                entries.push((mtime, meta.len(), path));
            }
        }
        entries.sort_by_key(|(mtime, _, _)| *mtime);
        entries
    }
}

/// A stable directory name for `url`. FNV-1a keeps entries usable after a
/// rebuilt binary (unlike `DefaultHasher`); a collision is harmless because
/// the entry header carries the URL and a mismatch is a miss.
fn hash(key: &str) -> String {
    let mut value: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.as_bytes() {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{value:016x}")
}

fn entry_name(offset: u64, len: u64) -> String {
    format!("{offset}-{len}.range")
}

fn encode_entry(url: &str, etag: &str, offset: u64, len: u64, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 64 + url.len() + etag.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(url.len() as u32).to_le_bytes());
    out.extend_from_slice(url.as_bytes());
    out.extend_from_slice(&(etag.len() as u32).to_le_bytes());
    out.extend_from_slice(etag.as_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
    out
}

/// The payload of `entry`, when its header says it is exactly the range asked
/// for on the same URL and ETag.
fn split_entry<'a>(
    entry: &'a [u8],
    url: &str,
    etag: &str,
    offset: u64,
    len: u64,
) -> Option<&'a [u8]> {
    let mut rest = entry.strip_prefix(MAGIC)?;
    let url_len = store_u32(&mut rest)?;
    let stored_url = std::str::from_utf8(take(&mut rest, url_len)?).ok()?;
    let etag_len = store_u32(&mut rest)?;
    let stored_etag = std::str::from_utf8(take(&mut rest, etag_len)?).ok()?;
    let stored_offset = u64::from_le_bytes(take(&mut rest, 8)?.try_into().ok()?);
    let stored_len = u64::from_le_bytes(take(&mut rest, 8)?.try_into().ok()?);
    let payload_len = u64::from_le_bytes(take(&mut rest, 8)?.try_into().ok()?);
    if stored_url != url
        || stored_etag != etag
        || stored_offset != offset
        || stored_len != len
        || payload_len != rest.len() as u64
    {
        return None;
    }
    Some(rest)
}

fn store_u32(bytes: &mut &[u8]) -> Option<usize> {
    Some(u32::from_le_bytes(take(bytes, 4)?.try_into().ok()?) as usize)
}

fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    if bytes.len() < n {
        return None;
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Some(head)
}

fn read_text(path: &Path) -> Option<String> {
    fs::read_to_string(path).ok()
}

/// Write `bytes` to `path` atomically, readable by its owner only.
fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    set_mode(&tmp, 0o600);
    file.write_all(bytes)?;
    file.sync_data()?;
    fs::rename(&tmp, path)
}

/// Mark an entry as recently used, so LRU eviction leaves it alone.
fn touch(path: &Path) {
    if let Ok(file) = File::options().write(true).open(path) {
        let _ = file.set_modified(SystemTime::now());
    }
}

fn set_mode(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
}

fn io_error(path: &Path, error: std::io::Error) -> crate::Error {
    crate::Error::Transport {
        context: format!("cache directory {}", path.display()),
        source: error,
    }
}

#[cfg(all(test, feature = "s3"))]
mod tests {
    use super::*;
    use crate::fetch::open_s3_cached;
    use crate::mock_s3::MockS3;
    use crate::s3::S3Config;

    /// A fresh cache directory, removed by the test when it finishes.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Scratch {
            let nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::env::temp_dir()
                .join(format!("pinkdb-cache-{tag}-{}-{nanos}", std::process::id()));
            Scratch(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn open(mock: &MockS3, key: &str, dir: &Path, capacity: u64) -> Box<dyn Fetcher> {
        let config = S3Config::new("bucket")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .with_credentials(mock.access_key_id(), "mock-secret");
        let options = CacheOptions {
            dir: dir.to_path_buf(),
            capacity,
        };
        open_s3_cached(&format!("s3://bucket/{key}"), config, Some(&options)).unwrap()
    }

    fn gets(mock: &MockS3) -> usize {
        mock.ranges().len()
    }

    fn entry_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let Ok(urls) = fs::read_dir(dir) else {
            return files;
        };
        for url in urls.flatten() {
            for file in fs::read_dir(url.path()).into_iter().flatten().flatten() {
                if file.path().extension().and_then(|e| e.to_str()) == Some("range") {
                    files.push(file.path());
                }
            }
        }
        files
    }

    #[test]
    fn the_cache_is_off_by_default() {
        assert!(crate::fetch::HttpfsConfig::default().cache.is_none());
        assert_eq!(
            super::enabled(),
            std::env::var_os("PINKDB_EXTERNAL_FILE_CACHE").is_some_and(|v| v != "0")
        );
    }

    #[test]
    fn a_second_read_of_a_range_costs_no_request() {
        let mock = MockS3::start();
        mock.put_object(
            "data/f.bin",
            &(0..1000u32).flat_map(u32::to_le_bytes).collect::<Vec<u8>>(),
        );
        let scratch = Scratch::new("hit");
        let fetch = open(&mock, "data/f.bin", &scratch.0, DEFAULT_CACHE_BYTES);
        let first = fetch.fetch_range(100, 200).unwrap();
        let after_first = gets(&mock);
        let second = fetch.fetch_range(100, 200).unwrap();
        assert_eq!(first, second);
        assert_eq!(gets(&mock), after_first, "a cached range was refetched");
        // A different range is still a miss.
        fetch.fetch_range(300, 200).unwrap();
        assert_eq!(gets(&mock), after_first + 1);
        // The bytes are the object's, not a neighbour's.
        let whole = (0..1000u32).flat_map(u32::to_le_bytes).collect::<Vec<u8>>();
        assert_eq!(second, whole[100..300]);
    }

    #[test]
    fn a_new_etag_invalidates_every_range() {
        let mock = MockS3::start();
        mock.put_object("data/f.bin", &[1u8; 500]);
        let scratch = Scratch::new("etag");
        let fetch = open(&mock, "data/f.bin", &scratch.0, DEFAULT_CACHE_BYTES);
        assert_eq!(fetch.fetch_range(0, 100).unwrap(), vec![1u8; 100]);

        // A rewrite replaces the object and its ETag; a fresh open must not
        // serve the old version's bytes.
        mock.put_object("data/f.bin", &[2u8; 500]);
        let fetch = open(&mock, "data/f.bin", &scratch.0, DEFAULT_CACHE_BYTES);
        assert_eq!(fetch.fetch_range(0, 100).unwrap(), vec![2u8; 100]);
        // The old generation is gone: one entry for the new one.
        assert_eq!(entry_files(&scratch.0).len(), 1);
    }

    #[test]
    fn the_directory_is_capped_by_lru_eviction() {
        let mock = MockS3::start();
        mock.put_object("data/f.bin", &[7u8; 1000]);
        let scratch = Scratch::new("evict");
        // Two entries (100-byte payloads plus a small header) fit in 400
        // bytes; three do not.
        let fetch = open(&mock, "data/f.bin", &scratch.0, 400);
        for offset in 0..5 {
            fetch.fetch_range(offset * 100, 100).unwrap();
        }
        let files = entry_files(&scratch.0);
        assert_eq!(files.len(), 2, "{files:?}");
        // The survivors are still hits.
        let before = gets(&mock);
        for offset in 0..5 {
            fetch.fetch_range(offset * 100, 100).unwrap();
        }
        assert!(gets(&mock) > before);
        assert!(entry_files(&scratch.0).len() <= 2);
    }

    #[test]
    fn a_corrupt_entry_is_a_miss_not_an_error() {
        let mock = MockS3::start();
        mock.put_object("data/f.bin", &[3u8; 400]);
        let scratch = Scratch::new("corrupt");
        let fetch = open(&mock, "data/f.bin", &scratch.0, DEFAULT_CACHE_BYTES);
        let first = fetch.fetch_range(10, 50).unwrap();
        for file in entry_files(&scratch.0) {
            fs::write(&file, b"not a cache entry").unwrap();
        }
        let before = gets(&mock);
        let second = fetch.fetch_range(10, 50).unwrap();
        assert_eq!(first, second, "a corrupt entry was served");
        assert_eq!(gets(&mock), before + 1, "a corrupt entry was not refetched");
    }

    #[test]
    fn entries_and_directories_are_private_to_the_user() {
        use std::os::unix::fs::PermissionsExt;
        let mock = MockS3::start();
        mock.put_object("data/f.bin", &[5u8; 100]);
        let scratch = Scratch::new("perms");
        let fetch = open(&mock, "data/f.bin", &scratch.0, DEFAULT_CACHE_BYTES);
        fetch.fetch_range(0, 100).unwrap();
        let root = fs::metadata(&scratch.0).unwrap().permissions().mode();
        assert_eq!(root & 0o777, 0o700, "{root:o}");
        for file in entry_files(&scratch.0) {
            let mode = fs::metadata(&file).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "{}: {mode:o}", file.display());
        }
    }
}
