//! Ranged object access for Iceberg tables (wave 72).
//!
//! Every byte an Iceberg read touches — the metadata JSON, the Avro manifest
//! list and manifests, the Parquet data and delete files — comes through
//! [`Object`], so a local table and an `https://`/`s3://` one share one code
//! path. Remote objects ride the shared `pinkdb-httpfs` transport that
//! `read_parquet` uses too, which is where the read-ahead and the opt-in range
//! cache (`PINKDB_EXTERNAL_FILE_CACHE`) live; the Parquet reads then go
//! through [`pinkdb_parquet::RangeSource`] implemented here.
//!
//! Reads are **sequential per object** and windowed ([`WINDOW`] bytes a
//! request) with a pure read-ahead cache: the Avro decoder streams a block at
//! a time instead of materializing a whole manifest, and a range request never
//! asks for bytes twice.

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

use pinkdb::{PinkDbError, Result};
use pinkdb_parquet::RangeSource;

/// The bytes one read-ahead window of a sequential read requests. Small
/// enough that a metadata-only read does not fetch a manifest whole, large
/// enough that a manifest usually takes one request.
const WINDOW: u64 = 256 * 1024;

/// A resolved object: a local file, or a remote URL plus its transport.
pub struct Object {
    path: String,
    kind: Kind,
}

enum Kind {
    Local {
        len: u64,
    },
    #[cfg(feature = "remote")]
    Remote {
        fetcher: Arc<dyn pinkdb_httpfs::Fetcher>,
        len: u64,
        /// The whole-object fallback for a backend that reports no size (the
        /// same shape parquet's remote reader uses).
        whole: Option<Vec<u8>>,
    },
}

/// Whether `path` names a remote object rather than a local file.
pub fn is_remote(path: &str) -> bool {
    path.contains("://")
}

impl fmt::Debug for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Object").field("path", &self.path).finish()
    }
}

impl Object {
    /// Resolve `path` (a local path or an `https://`/`s3://` URL) and its
    /// length.
    pub fn open(path: &str) -> Result<Object> {
        let path = path.to_string();
        if is_remote(&path) {
            return RemoteOpen::open(path);
        }
        let len = std::fs::metadata(&path)
            .map_err(|e| PinkDbError::Execution(format!("iceberg: cannot open {path}: {e}")))?
            .len();
        Ok(Object {
            path,
            kind: Kind::Local { len },
        })
    }

    /// The object's total length in bytes.
    pub fn len(&self) -> u64 {
        match &self.kind {
            Kind::Local { len } => *len,
            #[cfg(feature = "remote")]
            Kind::Remote { len, .. } => *len,
        }
    }

    /// The whole object. Metadata JSON is small enough to parse in one piece.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        match &self.kind {
            Kind::Local { .. } => std::fs::read(&self.path).map_err(|e| {
                PinkDbError::Execution(format!("iceberg: cannot read {}: {e}", self.path))
            }),
            #[cfg(feature = "remote")]
            Kind::Remote { fetcher, whole, .. } => match whole {
                Some(bytes) => Ok(bytes.clone()),
                None => fetcher.fetch().map_err(|e| {
                    PinkDbError::Execution(format!("iceberg: cannot read {}: {e}", self.path))
                }),
            },
        }
    }

    /// `len` bytes from `offset`, clamped to the object's end.
    pub fn read(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, String> {
        let len = len.min(self.len().saturating_sub(offset));
        if len == 0 {
            return Ok(Vec::new());
        }
        match &self.kind {
            // Opened per read: `read_range` takes `&self` and the Parquet
            // reader calls it from several threads, so a shared handle would
            // need a lock around a seek anyway.
            Kind::Local { .. } => {
                let mut file = File::open(&self.path)
                    .map_err(|e| format!("cannot open {}: {e}", self.path))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|e| format!("cannot seek {}: {e}", self.path))?;
                let mut buf = vec![0u8; len as usize];
                file.read_exact(&mut buf)
                    .map_err(|e| format!("cannot read {} at {offset}: {e}", self.path))?;
                Ok(buf)
            }
            #[cfg(feature = "remote")]
            Kind::Remote { fetcher, whole, .. } => match whole {
                Some(bytes) => Ok(bytes[offset as usize..(offset + len) as usize].to_vec()),
                None => fetcher
                    .fetch_range(offset, len)
                    .map_err(|e| format!("cannot read {} at {offset}: {e}", self.path)),
            },
        }
    }

    /// A sequential, windowed reader over this object.
    pub fn reader(self: &Arc<Self>) -> Reader {
        Reader {
            object: Arc::clone(self),
            buf: Vec::new(),
            pos: 0,
            next: 0,
        }
    }
}

/// The remote open, split out so the no-`remote` build still compiles the
/// local half without `cfg` noise in `open`.
struct RemoteOpen;

impl RemoteOpen {
    fn open(path: String) -> Result<Object> {
        #[cfg(feature = "remote")]
        {
            let fetcher = pinkdb_httpfs::open(&path)
                .map_err(|e| PinkDbError::Execution(format!("iceberg: cannot open {path}: {e}")))?;
            let (len, whole) = match fetcher
                .size()
                .map_err(|e| PinkDbError::Execution(format!("iceberg: cannot stat {path}: {e}")))?
            {
                Some(len) => (len, None),
                None => {
                    let bytes = fetcher.fetch().map_err(|e| {
                        PinkDbError::Execution(format!("iceberg: cannot read {path}: {e}"))
                    })?;
                    (bytes.len() as u64, Some(bytes))
                }
            };
            Ok(Object {
                path,
                kind: Kind::Remote {
                    fetcher: Arc::from(fetcher),
                    len,
                    whole,
                },
            })
        }
        #[cfg(not(feature = "remote"))]
        {
            Err(PinkDbError::Execution(format!(
                "iceberg: {path} is a remote table and this build has no remote \
                 transport; rebuild with the `remote` feature (the CLI's `httpfs` \
                 feature enables it)"
            )))
        }
    }
}

/// A sequential reader over an [`Object`], which is all the Avro decoder
/// needs. Seeking backwards restarts the window at the new offset; nothing
/// here seeks.
pub struct Reader {
    object: Arc<Object>,
    buf: Vec<u8>,
    pos: usize,
    next: u64,
}

impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.buf.len() {
            let remaining = self.object.len().saturating_sub(self.next);
            if remaining == 0 {
                return Ok(0);
            }
            self.buf = self
                .object
                .read(self.next, WINDOW.min(remaining))
                .map_err(io::Error::other)?;
            self.next += self.buf.len() as u64;
            self.pos = 0;
            if self.buf.is_empty() {
                return Ok(0);
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl RangeSource for Object {
    fn len(&self) -> u64 {
        Object::len(self)
    }

    fn read_range(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, String> {
        self.read(offset, len)
    }
}
