//! The engine catalog image (wave 96b): a store's metadata as one blob.
//!
//! A stateless `pinkdb-server` keeps table rows in a pink-lake lake and every
//! engine-side catalog object — databases, tables (schema, constraints,
//! column defaults, index definitions), views, macros, materialized views,
//! procedures, triggers, RBAC and the loaded extension paths — in the lake
//! catalog. The catalog stores that metadata as **one opaque image** rather
//! than one relational row per object: the objects reference each other, the
//! `PINKDB06` footer already encodes them, and the only other consumer of the
//! catalog database (DuckDB interop) reads `ducklake_*` and never these
//! tables. Splitting the image into rows would duplicate the footer encoder
//! without a reader that wants the parts.
//!
//! The image is a `PINKDB06` metadata image (every table's schema, no rows)
//! plus the sections that format does not carry:
//!
//! ```text
//! "PINKCAT1"                       magic
//! u32                              format version (1)
//! u64, u8[len]                     PINKDB06 metadata image
//! u32, (u32, u8[len])*             loaded extension sources
//! u32, (u32, u8; u32, u8; u32, u8)* views: database, name, CREATE VIEW text
//! u32                              CRC32C of every preceding byte
//! ```
//!
//! Rows never travel in the image: they are the lake's data files. A load
//! therefore pairs the image with the lake's materialized tables and refuses
//! to start when the two disagree — that is the invariant a stateless server
//! relies on, since both are published by one catalog transaction.

use std::sync::Arc;

use crate::persist::crc32c;
use crate::plan::LogicalPlan;
use crate::storage::{DataTable, PinkDb};
use crate::{PinkDbError, Result};

/// `PINKCAT1`, the image magic (a `.pink` file starts with `PINKDB`).
const MAGIC: &[u8; 8] = b"PINKCAT1";
/// The image format this build writes and reads.
const FORMAT: u32 = 1;

/// One lake table as the load expects it: database, table, rows.
pub type LakeRows = (String, String, Arc<DataTable>);

impl PinkDb {
    /// Serialize the engine catalog image: every catalog object above, with
    /// each table's metadata but **no rows**. Fails for a view created
    /// without a defining statement (an embedder calling `create_view_in`
    /// directly), which could never be rebuilt.
    pub fn to_catalog_image(&self) -> Result<Vec<u8>> {
        let metadata = self.to_metadata_bytes()?;
        let mut out = Vec::with_capacity(metadata.len() + 256);
        out.extend_from_slice(MAGIC);
        put_u32(&mut out, FORMAT);
        put_u64(&mut out, metadata.len() as u64);
        out.extend_from_slice(&metadata);
        let extensions = self.loaded_extension_paths();
        put_u32(&mut out, extensions.len() as u32);
        for source in &extensions {
            put_str(&mut out, source);
        }
        let mut views: Vec<(String, String, String)> = Vec::new();
        for dbname in self.list_databases() {
            for name in self.list_views_in(&dbname) {
                let view = self.view_in(&dbname, &name)?;
                if view.sql.is_empty() {
                    return Err(PinkDbError::Execution(format!(
                        "view \"{dbname}.{name}\" has no defining statement text to \
                         persist in the engine catalog"
                    )));
                }
                views.push((dbname.clone(), name, view.sql.clone()));
            }
        }
        put_u32(&mut out, views.len() as u32);
        for (dbname, name, sql) in views {
            put_str(&mut out, &dbname);
            put_str(&mut out, &name);
            put_str(&mut out, &sql);
        }
        let crc = crc32c(&out);
        put_u32(&mut out, crc);
        Ok(out)
    }

    /// Rebuild a store from its catalog image plus the lake's materialized
    /// tables. The image is authoritative for metadata and the lake for rows;
    /// a table present in only one of them is a hard error (both are
    /// published by one catalog transaction, so a difference means the
    /// catalog and the data files were not written together).
    ///
    /// The caller sets the store's extension policy before this call: the
    /// image's extension sources are replayed through
    /// [`PinkDb::resolve_replay_extension`], which refuses a source the policy
    /// does not allow.
    pub fn load_catalog_image(image: &[u8], tables: &[LakeRows]) -> Result<PinkDb> {
        let decoded = Image::decode(image)?;
        let mut db = PinkDb::from_pink_bytes(decoded.metadata)?;
        let mut seen: Vec<(String, String)> = Vec::with_capacity(tables.len());
        for (dbname, name, rows) in tables {
            let table = db.table_in(dbname, name).map_err(|_| {
                PinkDbError::Execution(format!(
                    "lake table \"{dbname}.{name}\" has no engine catalog entry: the \
                     catalog image and the data files were written by different versions"
                ))
            })?;
            if table.schema() != rows.schema() {
                return Err(PinkDbError::Execution(format!(
                    "table \"{dbname}.{name}\" disagrees between the engine catalog and \
                     the lake data files (column names or types differ)"
                )));
            }
            seen.push((dbname.clone(), name.clone()));
            db.attach_rows_in(dbname, name, rows)?;
        }
        for dbname in db.list_databases() {
            for name in db.list_tables_in(&dbname) {
                if !seen.iter().any(|(d, t)| *d == dbname && *t == name) {
                    return Err(PinkDbError::Execution(format!(
                        "engine catalog table \"{dbname}.{name}\" has no lake table: the \
                         catalog image and the data files were written by different versions"
                    )));
                }
            }
        }
        for source in decoded.extensions {
            db.resolve_replay_extension(source)?;
        }
        for (dbname, sql) in decoded.views {
            db.apply_view_sql(&dbname, sql)?;
        }
        Ok(db)
    }

    /// Append a lake table's materialized rows to its (empty) catalog-image
    /// table. The hash indexes and the primary-key cache are marked stale:
    /// rows arrived behind their back, exactly like a bulk load.
    fn attach_rows_in(&mut self, dbname: &str, name: &str, rows: &DataTable) -> Result<()> {
        let table = self.table_mut_in(dbname, name)?;
        for chunk_idx in 0..rows.num_chunks() {
            let chunk = rows.scan_chunk(chunk_idx).ok_or_else(|| {
                PinkDbError::Execution("lake table changed while it was being loaded".to_string())
            })?;
            table.append_shared_chunk(&chunk.columns)?;
        }
        if !table.indexes.is_empty() {
            table.indexes_stale = true;
        }
        table.key_index = None;
        Ok(())
    }

    /// Rebuild one view from its stored `CREATE VIEW` statement.
    fn apply_view_sql(&mut self, dbname: &str, sql: &str) -> Result<()> {
        let mut statements = crate::sql::parse_statements(sql)?;
        if statements.len() != 1 {
            return Err(PinkDbError::Execution(format!(
                "a stored view definition is not one statement: {sql}"
            )));
        }
        let statement = statements.remove(0);
        let (plan, _) = crate::sql::bind_tracked(self, &statement, dbname)?;
        match crate::plan::with_mv_source(plan, Some(sql)) {
            LogicalPlan::CreateView {
                db: target,
                name,
                cols,
                query,
                source,
            } => {
                let target = target.unwrap_or_else(|| dbname.to_string());
                self.create_view_in(&target, &name, *query, cols)?;
                self.set_view_source_in(&target, &name, &source)
            }
            _ => Err(PinkDbError::Execution(format!(
                "a stored view definition is not a CREATE VIEW: {sql}"
            ))),
        }
    }
}

/// A decoded image: borrowed sections plus owned view texts.
struct Image<'a> {
    metadata: &'a [u8],
    extensions: Vec<&'a str>,
    views: Vec<(String, &'a str)>,
}

impl<'a> Image<'a> {
    fn decode(bytes: &'a [u8]) -> Result<Image<'a>> {
        let (body, _crc) = split_crc(bytes)?;
        let mut r = Reader::new(body);
        if r.take(8)? != MAGIC {
            return Err(PinkDbError::Execution(
                "not an engine catalog image (bad magic)".to_string(),
            ));
        }
        let format = r.u32()?;
        if format != FORMAT {
            return Err(PinkDbError::Execution(format!(
                "engine catalog image format {format} is not supported by this \
                 build (it writes and reads {FORMAT})"
            )));
        }
        let metadata_len = r.u64()? as usize;
        let metadata = r.take(metadata_len)?;
        let mut extensions = Vec::new();
        for _ in 0..r.u32()? {
            extensions.push(r.str()?);
        }
        let mut views = Vec::new();
        for _ in 0..r.u32()? {
            let dbname = r.str()?.to_string();
            let _name = r.str()?;
            let sql = r.str()?;
            views.push((dbname, sql));
        }
        if !r.rest().is_empty() {
            return Err(PinkDbError::Execution(
                "engine catalog image has trailing bytes".to_string(),
            ));
        }
        Ok(Image {
            metadata,
            extensions,
            views,
        })
    }
}

fn split_crc(bytes: &[u8]) -> Result<(&[u8], u32)> {
    let Some((body, crc)) = bytes.split_at_checked(bytes.len().saturating_sub(4)) else {
        return Err(PinkDbError::Execution(
            "engine catalog image is truncated".to_string(),
        ));
    };
    let crc = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
    if crc32c(body) != crc {
        return Err(PinkDbError::Execution(
            "engine catalog image checksum mismatch".to_string(),
        ));
    }
    Ok((body, crc))
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
}

/// A bounds-checked reader: a truncated image is an error, never a panic.
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len());
        let Some(end) = end else {
            return Err(PinkDbError::Execution(
                "engine catalog image is truncated".to_string(),
            ));
        };
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        let mut word = [0u8; 8];
        word.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(word))
    }

    fn str(&mut self) -> Result<&'a str> {
        let len = self.u32()? as usize;
        std::str::from_utf8(self.take(len)?).map_err(|_| {
            PinkDbError::Execution("engine catalog image holds invalid UTF-8".to_string())
        })
    }

    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
}
