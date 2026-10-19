//! Remote parquet objects (`remote` feature): parquet's [`ChunkReader`] over
//! [`pinkdb_httpfs::Fetcher`].
//!
//! # What is ranged and what is not
//!
//! - The **footer** is read as parquet asks: the 8-byte trailer
//!   (`get_bytes(len-8, 8)`), then the thrift metadata bytes, then the
//!   footer-size probe — all `Range` requests, never a whole-object GET.
//! - **Row groups / column chunks** are read through [`RemoteRead`], a lazy
//!   `Read` that issues a `Range` request for at most [`READ_BLOCK`] bytes at
//!   a time as the decoder consumes page headers/pages. A scan decodes only
//!   the projected columns of the row groups that survive pruning, so a
//!   remote scan transfers roughly the same bytes a local scan reads.
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

#[cfg(feature = "remote")]
use std::io::{self, Cursor, Read};
#[cfg(feature = "remote")]
use std::sync::Arc;

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

/// The maximum bytes one lazy read fetches ahead. Page headers are tiny;
/// 128 KiB keeps the request count low without turning a scan into a
/// whole-column-chunk download.
#[cfg(feature = "remote")]
pub(crate) const READ_BLOCK: u64 = 128 * 1024;

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
}

#[cfg(feature = "remote")]
fn http_error(error: pinkdb_httpfs::Error) -> PinkDbError {
    PinkDbError::Execution(format!("remote parquet read failed: {error}"))
}

#[cfg(feature = "remote")]
fn parquet_http_error(error: pinkdb_httpfs::Error) -> ParquetError {
    ParquetError::General(format!("remote parquet read failed: {error}"))
}

/// Open one remote object. A known size keeps the reader ranged; an unknown
/// size is the documented whole-object fallback.
#[cfg(feature = "remote")]
pub(crate) fn open_spec(path: &str) -> Result<RemoteSpec> {
    let fetcher: Arc<dyn Fetcher> = Arc::from(pinkdb_httpfs::open(path).map_err(http_error)?);
    match fetcher.size().map_err(http_error)? {
        Some(len) => Ok(RemoteSpec {
            fetcher,
            len,
            whole: None,
        }),
        None => {
            let bytes = fetcher.fetch().map_err(http_error)?;
            Ok(RemoteSpec {
                fetcher,
                len: bytes.len() as u64,
                whole: Some(Bytes::from(bytes)),
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

/// Parquet's [`ChunkReader`] over a remote object.
#[cfg(feature = "remote")]
#[derive(Debug)]
pub(crate) struct RemoteFile {
    fetcher: Arc<dyn Fetcher>,
    len: u64,
    /// `Some` for the unknown-size fallback; the whole object serves every
    /// read without further requests.
    whole: Option<Bytes>,
}

#[cfg(feature = "remote")]
impl RemoteFile {
    pub(crate) fn from_spec(spec: &RemoteSpec) -> Self {
        Self {
            fetcher: Arc::clone(&spec.fetcher),
            len: spec.len,
            whole: spec.whole.clone(),
        }
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
            None => RemoteRead::Ranged(RangedRead {
                fetcher: Arc::clone(&self.fetcher),
                pos: start,
                end: self.len,
                buf: Vec::new(),
                offset: 0,
            }),
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
            None => self
                .fetcher
                .fetch_range(start, length as u64)
                .map(Bytes::from)
                .map_err(parquet_http_error),
        }
    }
}

/// The `Read` handle parquet's page decoder consumes.
#[cfg(feature = "remote")]
pub(crate) enum RemoteRead {
    /// Unknown-size fallback: an in-memory cursor (cheap `Bytes` clones).
    Whole(Cursor<Bytes>),
    /// Lazy ranged reads.
    Ranged(RangedRead),
}

#[cfg(feature = "remote")]
impl Read for RemoteRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            RemoteRead::Whole(cursor) => cursor.read(buf),
            RemoteRead::Ranged(read) => read.read(buf),
        }
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
}

#[cfg(feature = "remote")]
impl Read for RangedRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.pos >= self.end {
            return Ok(0);
        }
        if self.offset >= self.buf.len() {
            let want = (self.end - self.pos).min(READ_BLOCK.max(out.len() as u64));
            let bytes = self
                .fetcher
                .fetch_range(self.pos, want)
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
}
