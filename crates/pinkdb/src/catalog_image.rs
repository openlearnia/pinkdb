//! The database image (wave 124, from wave 96b's catalog image): one
//! database's engine metadata as one blob.
//!
//! A writable lake database keeps its rows in the lake's data files and every
//! engine-side object of the database — tables (schema, constraints, column
//! defaults, index definitions), macros, materialized views, procedures and
//! triggers — in the lake catalog, as **one opaque image** rather than one
//! relational row per object: the objects reference each other, the `.pink`
//! footer already encodes them, and the only other consumer of the catalog
//! database (DuckDB interop) reads `ducklake_*` and never this blob. Views are
//! session-local and never persisted; users, roles and grants stay in the
//! server's own store (a grant names the database).
//!
//! The image is a `.pink` metadata image of a store holding only that
//! database (every table's schema, no rows):
//!
//! ```text
//! "PINKDBI1"                       magic
//! u32                              format version (1)
//! u64, u8[len]                     .pink metadata image
//! u32                              CRC32C of every preceding byte
//! ```
//!
//! Rows never travel in the image: they are the lake's data files. A load
//! pairs the image with the lake's materialized tables. A table the image
//! describes with the lake's schema gets the image's metadata; a lake table
//! the image does not know (written by another tool) loads as a plain table,
//! and an image table the lake no longer has is dropped — the lake's tables
//! are the truth.

use std::sync::Arc;

use crate::lake_db::DbStorage;
use crate::persist::crc32c;
use crate::storage::{DataTable, DbCatalog, PinkDb};
use crate::{PinkDbError, Result};

/// `PINKDBI1`, the image magic (a `.pink` file ends with `PINKDB06`).
const MAGIC: &[u8; 8] = b"PINKDBI1";
/// The image format this build writes and reads.
const FORMAT: u32 = 1;

impl PinkDb {
    /// The image of database `db`: its catalog objects with each table's
    /// metadata but **no rows**, no RBAC and no views.
    pub fn to_database_image(&self, db: &str) -> Result<Vec<u8>> {
        let mut scratch = PinkDb::with_databases(Vec::<String>::new());
        let key = db.to_ascii_lowercase();
        let mut cat = self
            .databases
            .get(&key)
            .cloned()
            .ok_or_else(|| PinkDbError::Catalog(format!("database \"{db}\" not found")))?;
        cat.storage = DbStorage::Local;
        scratch.databases.insert(key, cat);
        scratch.metadata_image()
    }

    /// The image of this whole (scratch) store: the database image's body.
    pub(crate) fn metadata_image(&self) -> Result<Vec<u8>> {
        let metadata = self.to_metadata_bytes()?;
        let mut out = Vec::with_capacity(metadata.len() + 24);
        out.extend_from_slice(MAGIC);
        put_u32(&mut out, FORMAT);
        put_u64(&mut out, metadata.len() as u64);
        out.extend_from_slice(&metadata);
        let crc = crc32c(&out);
        put_u32(&mut out, crc);
        Ok(out)
    }

    /// Append a lake table's materialized rows to its (empty) image table.
    /// The hash indexes and the primary-key cache are marked stale: rows
    /// arrived behind their back, exactly like a bulk load.
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
}

/// Build database `name`'s catalog from its lake: the image (if the lake
/// carries one) for metadata and `tables` for rows. See the module docs for
/// how the two are reconciled.
pub(crate) fn load_database_image(
    name: &str,
    storage: DbStorage,
    image: Option<&[u8]>,
    tables: &[(String, Arc<DataTable>)],
) -> Result<DbCatalog> {
    let mut scratch = match image {
        Some(image) => PinkDb::from_pink_bytes(decode(image)?)?,
        None => PinkDb::with_databases(Vec::<String>::new()),
    };
    let image_db = match scratch.list_databases().as_slice() {
        [] => None,
        [one] => Some(one.clone()),
        many => {
            return Err(PinkDbError::Execution(format!(
                "a database image holds {} databases; it must hold one",
                many.len()
            )));
        }
    };
    let db = match &image_db {
        Some(db) => db.clone(),
        None => {
            scratch.create_database(name)?;
            name.to_string()
        }
    };
    let image_tables = scratch.list_tables_in(&db);
    // Image tables the lake no longer has are gone.
    for t in &image_tables {
        if !tables.iter().any(|(n, _)| n.eq_ignore_ascii_case(t)) {
            scratch.drop_table_in(&db, t)?;
        }
    }
    for (tname, rows) in tables {
        let described = scratch
            .table_in(&db, tname)
            .is_ok_and(|t| t.schema() == rows.schema());
        if described {
            scratch.attach_rows_in(&db, tname, rows)?;
        } else {
            if scratch.table_in(&db, tname).is_ok() {
                scratch.drop_table_in(&db, tname)?;
            }
            let mut table = (**rows).clone();
            table.name = tname.clone();
            scratch.tables_insert_in(&db, tname.to_ascii_lowercase(), table)?;
        }
    }
    let mut cat = scratch
        .databases
        .remove(&db.to_ascii_lowercase())
        .expect("the database was found above");
    if !cat.name.eq_ignore_ascii_case(name) {
        // Attached under another name: the definitions follow the database.
        for trigger in &mut cat.triggers {
            trigger.db = name.to_string();
        }
        for mv in cat.materialized_views.values_mut() {
            mv.query = None;
        }
    }
    cat.name = name.to_string();
    cat.storage = storage;
    Ok(cat)
}

/// The `.pink` metadata image inside a database image, checked.
fn decode(bytes: &[u8]) -> Result<&[u8]> {
    let (body, _crc) = split_crc(bytes)?;
    let mut r = Reader::new(body);
    if r.take(8)? != MAGIC {
        return Err(PinkDbError::Execution(
            "not a database image (bad magic)".to_string(),
        ));
    }
    let format = r.u32()?;
    if format != FORMAT {
        return Err(PinkDbError::Execution(format!(
            "database image format {format} is not supported by this build (it writes and \
             reads {FORMAT})"
        )));
    }
    let metadata_len = r.u64()? as usize;
    let metadata = r.take(metadata_len)?;
    if !r.rest().is_empty() {
        return Err(PinkDbError::Execution(
            "database image has trailing bytes".to_string(),
        ));
    }
    Ok(metadata)
}

fn split_crc(bytes: &[u8]) -> Result<(&[u8], u32)> {
    let Some((body, crc)) = bytes.split_at_checked(bytes.len().saturating_sub(4)) else {
        return Err(PinkDbError::Execution(
            "database image is truncated".to_string(),
        ));
    };
    let crc = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
    if crc32c(body) != crc {
        return Err(PinkDbError::Execution(
            "database image checksum mismatch".to_string(),
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
                "database image is truncated".to_string(),
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

    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
}
