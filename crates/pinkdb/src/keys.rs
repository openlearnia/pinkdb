//! `PRIMARY KEY` / `UNIQUE` enforcement (wave 95b).
//!
//! pinkdb kept keys as catalog metadata only, and `ON CONFLICT` on top of that
//! is incoherent (it could never fire). Every SQL write therefore checks the
//! declared keys against a statement-local hash map: built by one scan when
//! the statement starts, dropped with it. A table without keys pays nothing,
//! and a pure append (the bulk `INSERT … SELECT` case) stays O(rows) because
//! each new key is one hash lookup.
//!
//! Keys are the same encoding the `CREATE INDEX` maps use, one length-prefixed
//! encoded value per column, so `NULL`s are never equal to anything (a
//! `UNIQUE` key admits any number of them; a `PRIMARY KEY` column is `NOT
//! NULL` and is rejected instead). Bulk ingestion that bypasses
//! [`crate::trigger`] (`COPY FROM`, CSV import) still loads unvalidated rows;
//! the next SQL write sees them as existing duplicates and keeps the first.

use std::collections::HashMap;

use crate::exec::aggregate::encode_value;
use crate::storage::{DataTable, KeyKind};
use crate::types::{PinkDbError, Result, Value};

/// One declared key, columns in constraint order.
#[derive(Clone, Debug)]
pub(crate) struct KeySpec {
    pub kind: KeyKind,
    pub columns: Vec<usize>,
}

/// A row's identity inside one table: chunk index and row index within it.
/// Updates never move rows and appends never shift them, so it is stable while
/// a statement runs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct RowId {
    pub chunk: usize,
    pub row: usize,
}

impl RowId {
    pub(crate) fn new(chunk: usize, row: usize) -> Self {
        RowId { chunk, row }
    }
}

/// The declared keys of one table plus the live occupant of each key value.
#[derive(Clone, Default, Debug)]
pub(crate) struct KeySpace {
    specs: Vec<KeySpec>,
    map: HashMap<(usize, Vec<u8>), RowId>,
    /// The table version the map describes: any write this process did not
    /// route through `store` bumps it, so the cache rebuilds instead of
    /// answering from stale occupancy (another writer, a delete, a move).
    version: u64,
}

/// The table's cached key map, built on first use and kept in
/// [`DataTable::key_index`] afterwards. `None` means "the table declares no
/// key", so a keyless table never pays for a map.
pub(crate) fn take_or_build(
    db: &mut crate::storage::PinkDb,
    db_name: &str,
    table: &str,
) -> Result<Option<KeySpace>> {
    let t = db.table_mut_in(db_name, table)?;
    if t.constraints.keys.is_empty() {
        return Ok(None);
    }
    if let Some(keys) = t.key_index.take()
        && keys.version == t.version
    {
        return Ok(Some(keys));
    }
    // The table is scanned once per process: the cache lives in memory only,
    // so a reopen pays one O(rows) build before its first keyed write.
    // shortcut: the map is not persisted; persist it beside the checkpoint
    // when reopening a large store gets hot.
    Ok(Some(KeySpace::build(t)))
}

/// Put a map (built, taken or already empty) back into the table.
pub(crate) fn store(
    db: &mut crate::storage::PinkDb,
    db_name: &str,
    table: &str,
    keys: Option<KeySpace>,
) {
    if let Ok(t) = db.table_mut_in(db_name, table) {
        t.key_index = keys.map(|mut k| {
            k.version = t.version;
            k
        });
    }
}

/// Drop the cache after a mutation that moved rows (deletes shift indices).
pub(crate) fn invalidate(db: &mut crate::storage::PinkDb, db_name: &str, table: &str) {
    if let Ok(t) = db.table_mut_in(db_name, table) {
        t.key_index = None;
    }
}

impl KeySpace {
    /// An empty map with this table's key definitions: the occupancy a
    /// statement's own staged rows live in while the table's map stays
    /// read-only.
    pub(crate) fn empty_like(&self) -> KeySpace {
        KeySpace {
            specs: self.specs.clone(),
            map: HashMap::new(),
            version: self.version,
        }
    }

    /// Build the key map from the table's current rows.
    pub(crate) fn build(t: &DataTable) -> KeySpace {
        let specs: Vec<KeySpec> = t
            .constraints
            .keys
            .iter()
            .map(|k| KeySpec {
                kind: k.kind,
                columns: k.columns.clone(),
            })
            .collect();
        let mut space = KeySpace {
            specs,
            map: HashMap::new(),
            version: t.version,
        };
        if space.specs.is_empty() {
            return space;
        }
        for ci in 0..t.num_chunks() {
            let Some(chunk) = t.scan_chunk(ci) else {
                continue;
            };
            for row in 0..chunk.num_rows() {
                for (si, spec) in space.specs.iter().enumerate() {
                    let key: Vec<Value> = spec
                        .columns
                        .iter()
                        .map(|&c| chunk.columns[c].get(row))
                        .collect();
                    if let Some(enc) = encode_key(&key) {
                        // A table written before enforcement existed may
                        // already hold duplicates: the first row owns the key.
                        space
                            .map
                            .entry((si, enc))
                            .or_insert(RowId { chunk: ci, row });
                    }
                }
            }
        }
        space
    }

    /// The row a new image would collide with: the declared key named by
    /// `target` (or any key when `None`), in declaration order.
    pub(crate) fn find(&self, values: &[Value], target: Option<&[usize]>) -> Option<RowId> {
        for (si, spec) in self.specs.iter().enumerate() {
            if let Some(cols) = target
                && !same_columns(&spec.columns, cols)
            {
                continue;
            }
            let key: Vec<Value> = spec.columns.iter().map(|&c| values[c].clone()).collect();
            let Some(enc) = encode_key(&key) else {
                continue;
            };
            if let Some(id) = self.map.get(&(si, enc)) {
                return Some(*id);
            }
        }
        None
    }

    /// Reject a new image that duplicates a declared key. `skip` is the row
    /// the image belongs to (an `UPDATE` keeps its own key).
    pub(crate) fn check(&self, values: &[Value], t: &DataTable, skip: Option<RowId>) -> Result<()> {
        for (si, spec) in self.specs.iter().enumerate() {
            let key: Vec<Value> = spec.columns.iter().map(|&c| values[c].clone()).collect();
            if let Some(null_col) = spec
                .columns
                .iter()
                .zip(key.iter())
                .find(|(_, v)| matches!(v, Value::Null))
                .map(|(c, _)| *c)
            {
                if spec.kind == KeyKind::Primary {
                    return Err(PinkDbError::Execution(format!(
                        "NOT NULL constraint failed: {}.{}",
                        t.name, t.columns[null_col].name
                    )));
                }
                continue;
            }
            let Some(enc) = encode_key(&key) else {
                continue;
            };
            if let Some(id) = self.map.get(&(si, enc))
                && Some(*id) != skip
            {
                return Err(duplicate_error(t, spec, &key));
            }
        }
        Ok(())
    }

    /// Record `values` as the occupant of its keys at `id`.
    pub(crate) fn reserve(&mut self, values: &[Value], id: RowId) {
        for (si, spec) in self.specs.iter().enumerate() {
            let key: Vec<Value> = spec.columns.iter().map(|&c| values[c].clone()).collect();
            if let Some(enc) = encode_key(&key) {
                self.map.insert((si, enc), id);
            }
        }
    }

    /// Free the keys `values` occupied (the row was updated or deleted).
    pub(crate) fn release(&mut self, values: &[Value]) {
        for (si, spec) in self.specs.iter().enumerate() {
            let key: Vec<Value> = spec.columns.iter().map(|&c| values[c].clone()).collect();
            if let Some(enc) = encode_key(&key) {
                self.map.remove(&(si, enc));
            }
        }
    }
}

fn same_columns(a: &[usize], b: &[usize]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

fn encode_key(values: &[Value]) -> Option<Vec<u8>> {
    // NULL never equals anything, so a key holding one conflicts with nothing.
    if values.iter().any(|v| matches!(v, Value::Null)) {
        return None;
    }
    let mut buf = Vec::new();
    for v in values {
        let mut part = Vec::new();
        encode_value(&mut part, v);
        buf.extend((part.len() as u32).to_le_bytes());
        buf.extend(part);
    }
    Some(buf)
}

fn duplicate_error(t: &DataTable, spec: &KeySpec, key: &[Value]) -> PinkDbError {
    if spec.kind == KeyKind::Primary {
        let shown: Vec<String> = key.iter().map(|v| v.to_string()).collect();
        return PinkDbError::Execution(format!(
            "PRIMARY KEY or UNIQUE constraint violation: duplicate key \"{}\"",
            shown.join(", ")
        ));
    }
    let shown: Vec<String> = spec
        .columns
        .iter()
        .zip(key.iter())
        .map(|(c, v)| format!("{}: {v}", t.columns[*c].name))
        .collect();
    PinkDbError::Execution(format!(
        "Duplicate key \"{}\" violates unique constraint.",
        shown.join(", ")
    ))
}
