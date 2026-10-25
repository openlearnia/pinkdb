//! Planning and reading one Iceberg snapshot (wave 72).
//!
//! A scan is three reads deep: the snapshot's **manifest list** says which
//! **manifests** belong to it, each manifest says which **data files** are live
//! (`ADDED`/`EXISTING`; `DELETED` is dropped) and which **delete files** remove
//! rows from them, and the data files are Parquet read by
//! [`pinkdb_parquet::scan_source`] — the same ranged, projected, row-group-
//! pruning path `read_parquet` uses.
//!
//! Iceberg's guarantees are what make the metadata enough for most queries:
//!
//! - **Pruning is only an optimization.** The engine re-applies every
//!   predicate above the scan. Manifest-list partition summaries prune whole
//!   manifests (identity transforms only — a bucket or truncate summary says
//!   nothing comparable about a column value), and a manifest entry's
//!   `lower_bounds`/`upper_bounds`/`null_value_counts` prune its file. Bounds
//!   are Iceberg's single-value encoding (little-endian primitives); a
//!   truncated bound is still a sound bound in Iceberg, so using it never
//!   drops a matching file.
//! - **`COUNT(*)` needs no data page.** Without delete files the row count is
//!   the sum of the live files' manifest `record_count`s; with them it is that
//!   sum minus the positional deletes that apply. Nothing is decoded.
//! - **Positional deletes** (`content = 1`) name `(file_path, pos)` rows of a
//!   data file. The reader decodes a file's chunks in file order, so the
//!   absolute row offset of every chunk is known and a delete set is a
//!   selection vector over each chunk. A file with deletes is read **without**
//!   filters, because a filtered chunk would no longer start at the row offset
//!   the positions refer to.
//! - **Equality deletes** (`content = 2`) are refused by name: applying them
//!   correctly means reading the delete file's values and comparing them per
//!   row, which is a later wave (`iceberg_scan` never guesses).
//! - **Schema evolution by field id**: a Parquet column is matched to a schema
//!   field by its Iceberg field id (the Parquet footer's basic info), by name
//!   only when the file carries no ids. A renamed column therefore follows its
//!   id, and a column added after a file was written reads NULL.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use pinkdb::attached::{PruneOp, PrunePredicate};
use pinkdb::{CHUNK_SIZE, DataChunk, DataType, PinkDbError, Result, Value, Vector};
use pinkdb_parquet::{FileMeta, read_meta_source, scan_source};

use crate::manifest::{self, Content, DataFile, FileContent, ManifestFile, Status};
use crate::obj::Object;
use crate::table::{Field, IceType, PartitionSpec, Schema, Selector, Snapshot, Table};

/// One live data file of a snapshot, with the row positions the snapshot's
/// positional deletes remove from it.
#[derive(Debug, Clone)]
pub struct PlannedFile {
    /// Resolved (openable) path.
    pub path: String,
    /// As the manifest wrote it: delete files name the data file this way.
    pub raw_path: String,
    /// The data sequence number the position-delete rule compares against.
    pub sequence_number: i64,
    pub record_count: i64,
    /// Sorted, deduplicated row positions, empty for most files.
    pub deletes: Vec<i64>,
}

/// A resolved plan: what a scan of one snapshot will read.
#[derive(Debug)]
pub struct Plan {
    pub table: Table,
    pub snapshot: Option<Snapshot>,
    pub schema: Schema,
    /// The bound column types, in schema order.
    pub types: Vec<DataType>,
    pub files: Vec<PlannedFile>,
    /// Positional deletes that apply to `files`.
    pub delete_rows: u64,
    pub manifests: u64,
    /// Data manifests pruned from their partition summaries.
    pub manifests_pruned: u64,
    /// Data files pruned from their manifest-entry statistics.
    pub files_pruned: u64,
    pub delete_files: u64,
}

impl Plan {
    /// The metadata-only row count: every live row minus the positional
    /// deletes. Exact when nothing was pruned.
    pub fn count_rows(&self) -> u64 {
        let rows: i64 = self.files.iter().map(|file| file.record_count).sum();
        (rows - self.delete_rows as i64).max(0) as u64
    }
}

/// Resolve a table, pick the snapshot and collect the live files (pruning
/// what the predicates rule out).
pub fn plan(path: &str, selector: Selector, filters: &[PrunePredicate]) -> Result<Plan> {
    let table = Table::open(path)?;
    let schema = table.current_schema()?;
    let types = schema
        .fields
        .iter()
        .map(|field| field.ty.data_type())
        .collect::<Vec<_>>();
    let snapshot = table.select(selector)?;
    let mut plan = Plan {
        table,
        snapshot: None,
        schema,
        types,
        files: Vec::new(),
        delete_rows: 0,
        manifests: 0,
        manifests_pruned: 0,
        files_pruned: 0,
        delete_files: 0,
    };
    let Some(snapshot) = snapshot else {
        return Ok(plan);
    };
    let manifest_list = plan.table.resolve_object(&snapshot.manifest_list);
    let object = Arc::new(Object::open(&manifest_list)?);
    let manifests = manifest::read_manifest_list(&mut object.reader())?;
    plan.manifests = manifests.len() as u64;

    let mut delete_files: Vec<(String, i64)> = Vec::new();
    for manifest in &manifests {
        match manifest.content {
            Content::Data => {
                if !manifest_may_match(&plan.table, &plan.schema, filters, manifest) {
                    plan.manifests_pruned += 1;
                    continue;
                }
                for entry in read_entries(&plan.table, manifest)? {
                    if entry.status == Status::Deleted {
                        continue;
                    }
                    match entry.file.content {
                        FileContent::Data => {
                            if filters.is_empty()
                                || file_may_match(&entry.file, &plan.schema, filters)
                            {
                                plan.files.push(PlannedFile {
                                    path: plan.table.resolve_object(&entry.file.path),
                                    raw_path: entry.file.path.clone(),
                                    sequence_number: sequence(&entry),
                                    record_count: entry.file.record_count,
                                    deletes: Vec::new(),
                                });
                            } else {
                                plan.files_pruned += 1;
                            }
                        }
                        FileContent::PositionDeletes => {
                            let sequence = sequence(&entry);
                            delete_files.push((entry.file.path, sequence));
                        }
                        FileContent::EqualityDeletes => {
                            return Err(PinkDbError::Execution(format!(
                                "iceberg: snapshot {} of {} has equality delete files \
                                 (e.g. {}); pinkdb applies positional deletes only — pass \
                                 snapshot_from_id := <an older snapshot> or rewrite the \
                                 table without equality deletes",
                                snapshot.id, plan.table.path, entry.file.path
                            )));
                        }
                    }
                }
            }
            Content::Deletes => {
                // A delete manifest ships one entry per delete file; a
                // DELETED entry is a delete file a later commit removed.
                for entry in read_entries(&plan.table, manifest)? {
                    if entry.status == Status::Deleted {
                        continue;
                    }
                    match entry.file.content {
                        FileContent::PositionDeletes => {
                            let sequence = sequence(&entry);
                            delete_files.push((entry.file.path, sequence));
                        }
                        FileContent::EqualityDeletes => {
                            return Err(PinkDbError::Execution(format!(
                                "iceberg: snapshot {} of {} has equality delete files \
                                 (e.g. {}); pinkdb applies positional deletes only",
                                snapshot.id, plan.table.path, entry.file.path
                            )));
                        }
                        FileContent::Data => {
                            return Err(PinkDbError::Execution(format!(
                                "iceberg: the delete manifest {}/{} lists a data file; \
                                 the table's metadata is inconsistent",
                                plan.table.metadata_path, manifest.path
                            )));
                        }
                    }
                }
            }
        }
    }
    plan.delete_files = delete_files.len() as u64;
    if !delete_files.is_empty() {
        // Iceberg's rule: a position delete applies to a data file whose data
        // sequence number is at or before the delete's. A file rewritten under
        // the same path by a later commit therefore keeps its rows.
        let mut positions: HashMap<String, BTreeSet<i64>> = HashMap::new();
        {
            let data_sequence: HashMap<&str, i64> = plan
                .files
                .iter()
                .map(|file| (file.raw_path.as_str(), file.sequence_number))
                .collect();
            for (raw_path, delete_sequence) in &delete_files {
                for (file, pos) in read_positions(&plan.table, raw_path)? {
                    let applies = data_sequence
                        .get(file.as_str())
                        .is_some_and(|sequence| sequence <= delete_sequence);
                    if applies {
                        positions.entry(file).or_default().insert(pos);
                    }
                }
            }
        }
        for file in &mut plan.files {
            if let Some(set) = positions.get(&file.raw_path) {
                file.deletes = set.iter().copied().collect();
                plan.delete_rows += file.deletes.len() as u64;
            }
        }
    }
    plan.snapshot = Some(snapshot);
    Ok(plan)
}

fn read_entries(table: &Table, manifest: &ManifestFile) -> Result<Vec<manifest::Entry>> {
    let path = table.resolve_object(&manifest.path);
    let object = Arc::new(Object::open(&path)?);
    manifest::read_manifest(&mut object.reader(), manifest.sequence_number)
}

fn sequence(entry: &manifest::Entry) -> i64 {
    entry.sequence_number.unwrap_or(0)
}

/// The positional deletes one delete file holds, as `(data file, position)`
/// pairs in the data file's own path spelling.
fn read_positions(table: &Table, delete_path: &str) -> Result<Vec<(String, i64)>> {
    let path = table.resolve_object(delete_path);
    let object = Arc::new(Object::open(&path)?);
    let meta = read_meta_source(&path, object.clone())?;
    let file_column = column_index(&meta, "file_path").ok_or_else(|| {
        PinkDbError::Execution(format!(
            "iceberg: the delete file {path} has no `file_path` column"
        ))
    })?;
    let pos_column = column_index(&meta, "pos").ok_or_else(|| {
        PinkDbError::Execution(format!(
            "iceberg: the delete file {path} has no `pos` column"
        ))
    })?;
    let mut columns = vec![file_column, pos_column];
    columns.sort_unstable();
    let scan = scan_source(&path, object, Some(&columns), &[])?;
    let mut out = Vec::new();
    for chunk in &scan.output.chunks {
        let file_at = columns.iter().position(|c| *c == file_column).unwrap_or(0);
        let pos_at = columns.iter().position(|c| *c == pos_column).unwrap_or(1);
        for row in 0..chunk.num_rows() {
            let Value::Varchar(file) = chunk.column(file_at).get(row) else {
                return Err(PinkDbError::Execution(format!(
                    "iceberg: the delete file {path} has a null or non-text `file_path`"
                )));
            };
            let pos = match chunk.column(pos_at).get(row) {
                Value::Bigint(pos) => pos,
                Value::Integer(pos) => pos as i64,
                other => {
                    return Err(PinkDbError::Execution(format!(
                        "iceberg: the delete file {path} has a `pos` that is not an \
                         integer: {other}"
                    )));
                }
            };
            out.push((file, pos));
        }
    }
    Ok(out)
}

fn column_index(meta: &FileMeta, name: &str) -> Option<usize> {
    meta.columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.leaf)
}

/// Read the planned snapshot and emit chunks in the bound schema's shape, with
/// the number of column chunks decoded.
pub fn scan(plan: &Plan, projection: Option<&[usize]>) -> Result<(Vec<DataChunk>, u64)> {
    let width = plan.schema.fields.len();
    let wanted: Vec<usize> = match projection {
        Some(projection) => projection.to_vec(),
        None => (0..width).collect(),
    };
    // COUNT(*): the manifest record counts already answer it, and no data
    // page is touched. The chunk still has the bound schema's width — the
    // scan contract wants one (empty) vector per column — but no page is read.
    if wanted.is_empty() {
        let rows = plan.count_rows();
        if rows == 0 {
            return Ok((Vec::new(), 0));
        }
        let columns: Vec<Arc<Vector>> = plan
            .types
            .iter()
            .map(|ty| Arc::new(Vector::new_empty(*ty)))
            .collect();
        let rows = rows as usize;
        let chunks = (0..rows)
            .step_by(CHUNK_SIZE)
            .map(|start| DataChunk::projected(columns.clone(), (rows - start).min(CHUNK_SIZE)))
            .collect();
        return Ok((chunks, 0));
    }
    let exclusion: Vec<bool> = (0..width).map(|i| !wanted.contains(&i)).collect();
    let mut chunks = Vec::new();
    let mut decoded = 0;
    for file in &plan.files {
        let object = Arc::new(Object::open(&file.path)?);
        let meta = read_meta_source(&file.path, object.clone())?;
        let mut slots: Vec<Slot> = exclusion
            .iter()
            .enumerate()
            .map(|(column, excluded)| {
                if *excluded {
                    Slot::Excluded
                } else {
                    match leaf_for(&meta, &plan.schema.fields[column]) {
                        Some(leaf) => Slot::Leaf(leaf),
                        None => Slot::Absent,
                    }
                }
            })
            .collect();
        // Decode each present column once, even if two schema fields resolve
        // to the same Parquet column.
        let mut leaves: Vec<usize> = Vec::new();
        for slot in &mut slots {
            if let Slot::Leaf(leaf) = slot {
                if let Some(index) = leaves.iter().position(|candidate| candidate == leaf) {
                    *slot = Slot::Leaf(index);
                } else {
                    leaves.push(*leaf);
                    *slot = Slot::Leaf(leaves.len() - 1);
                }
            }
        }
        // A file with positional deletes must keep its row offsets: no
        // filter, so the chunk rows line up with the delete positions.
        let filters: Vec<PrunePredicate> = Vec::new();
        let scan = scan_source(&file.path, object, Some(&leaves), &filters)?;
        decoded += scan.output.columns_decoded;
        let mut base: i64 = 0;
        for chunk in scan.output.chunks {
            let rows = chunk.num_rows() as i64;
            let kept = if file.deletes.is_empty() {
                None
            } else {
                Some(selection(&file.deletes, base, rows as usize))
            };
            base += rows;
            let assembled = assemble(&chunk, &slots, &plan.types);
            let out = match kept {
                Some(kept) => assembled.gather(&kept),
                None => assembled,
            };
            if out.num_rows() > 0 {
                chunks.push(out);
            }
        }
    }
    Ok((chunks, decoded))
}

/// Where one bound column's values come from for one data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// The query did not project it: a zero-length placeholder, per the scan
    /// contract.
    Excluded,
    /// Projected, but this file has no such column (written before it was
    /// added): NULL.
    Absent,
    /// Decoded from the file, as output column `index` of the Parquet scan.
    Leaf(usize),
}

fn assemble(chunk: &DataChunk, slots: &[Slot], types: &[DataType]) -> DataChunk {
    let rows = chunk.num_rows();
    let columns = slots
        .iter()
        .enumerate()
        .map(|(column, slot)| match slot {
            Slot::Excluded => Arc::new(Vector::new_empty(types[column])),
            Slot::Absent => Arc::new(nulls(&types[column], rows)),
            Slot::Leaf(index)
                if *index < chunk.num_cols() && chunk.column(*index).len() == rows =>
            {
                Arc::new(chunk.column(*index).clone())
            }
            Slot::Leaf(_) => Arc::new(nulls(&types[column], rows)),
        })
        .collect();
    DataChunk::projected(columns, rows)
}

fn nulls(ty: &DataType, rows: usize) -> Vector {
    let mut vector = Vector::with_capacity(*ty, rows);
    for _ in 0..rows {
        vector.push_null().expect("a NULL fits every type");
    }
    vector
}

/// The rows of `[base, base + rows)` that no delete position names.
fn selection(deletes: &[i64], base: i64, rows: usize) -> Vec<usize> {
    let mut kept = Vec::with_capacity(rows);
    let mut next = deletes.partition_point(|position| *position < base);
    for row in 0..rows as i64 {
        let position = base + row;
        while next < deletes.len() && deletes[next] < position {
            next += 1;
        }
        if next >= deletes.len() || deletes[next] != position {
            kept.push(row as usize);
        }
    }
    kept
}

/// The Parquet leaf a schema field reads from: by Iceberg field id first (the
/// identity a rename preserves), by name second (a file written without ids).
fn leaf_for(meta: &FileMeta, field: &Field) -> Option<usize> {
    let id = i32::try_from(field.id).ok()?;
    for ty in meta.schema_root.get_fields() {
        let info = ty.get_basic_info();
        if info.has_id() && info.id() == id {
            // Only flat schemas are read, so a top-level field's index is its
            // leaf index.
            return meta
                .columns
                .iter()
                .position(|column| column.name == ty.name());
        }
    }
    meta.columns
        .iter()
        .find(|column| column.name == field.name)
        .map(|column| column.leaf)
}

// --------------------------------------------------------------------------
// Pruning
// --------------------------------------------------------------------------

/// Whether a manifest can hold a file matching any predicate, decided from the
/// manifest list's partition summaries (identity transforms only).
fn manifest_may_match(
    table: &Table,
    schema: &Schema,
    filters: &[PrunePredicate],
    manifest: &ManifestFile,
) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Some(spec) = table.partition_spec(manifest.spec_id) else {
        return true;
    };
    filters.iter().all(|predicate| {
        let Some(field) = schema.fields.get(predicate.column) else {
            return true;
        };
        let Some(position) = partition_position(&spec, field.id) else {
            return true;
        };
        let Some(summary) = manifest.partitions.get(position) else {
            return true;
        };
        match predicate.op {
            PruneOp::IsNull => summary.contains_null,
            PruneOp::IsNotNull => true,
            _ => bounds_may_match(
                summary.lower.as_deref(),
                summary.upper.as_deref(),
                &field.ty,
                predicate,
            ),
        }
    })
}

/// The position of the spec field that partitions on `source_id` with the
/// identity transform (the only transform whose value *is* the column value).
fn partition_position(spec: &PartitionSpec, source_id: i64) -> Option<usize> {
    spec.fields
        .iter()
        .position(|field| field.source_id == source_id && field.transform == "identity")
}

/// Whether one data file can hold a row matching any predicate, decided from
/// its manifest-entry statistics.
fn file_may_match(file: &DataFile, schema: &Schema, filters: &[PrunePredicate]) -> bool {
    filters.iter().all(|predicate| {
        let Some(field) = schema.fields.get(predicate.column) else {
            return true;
        };
        let nulls = lookup(&file.null_counts, field.id);
        match predicate.op {
            PruneOp::IsNull => nulls != Some(0),
            PruneOp::IsNotNull => nulls != Some(file.record_count),
            _ => bounds_may_match(
                lookup_bytes(&file.lower_bounds, field.id),
                lookup_bytes(&file.upper_bounds, field.id),
                &field.ty,
                predicate,
            ),
        }
    })
}

fn lookup(entries: &[(i64, i64)], field_id: i64) -> Option<i64> {
    entries
        .iter()
        .find(|(id, _)| *id == field_id)
        .map(|(_, value)| *value)
}

fn lookup_bytes(entries: &[(i64, Vec<u8>)], field_id: i64) -> Option<&[u8]> {
    entries
        .iter()
        .find(|(id, _)| *id == field_id)
        .map(|(_, value)| value.as_slice())
}

/// The value a bound decodes to, for comparing against a predicate.
#[derive(Debug, Clone, PartialEq)]
enum Bound {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

fn bounds_may_match(
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    ty: &IceType,
    predicate: &PrunePredicate,
) -> bool {
    let Some(value) = predicate.value.as_ref() else {
        return true;
    };
    let Some(value) = predicate_bound(value, ty) else {
        return true;
    };
    let lower = lower.and_then(|bytes| decode_bound(bytes, ty));
    let upper = upper.and_then(|bytes| decode_bound(bytes, ty));
    // A missing or undecodable bound proves nothing; a bound that cannot be
    // compared with the literal does either.
    match predicate.op {
        PruneOp::Eq => {
            let below = lower
                .as_ref()
                .and_then(|lower| cmp(&value, lower))
                .is_some_and(|order| order.is_lt());
            let above = upper
                .as_ref()
                .and_then(|upper| cmp(&value, upper))
                .is_some_and(|order| order.is_gt());
            !(below || above)
        }
        PruneOp::Lt => !matches!(
            lower.as_ref().and_then(|lower| cmp(lower, &value)),
            Some(std::cmp::Ordering::Less)
        ),
        PruneOp::LtEq => !matches!(
            lower.as_ref().and_then(|lower| cmp(lower, &value)),
            Some(std::cmp::Ordering::Greater)
        ),
        PruneOp::Gt => !matches!(
            upper.as_ref().and_then(|upper| cmp(upper, &value)),
            Some(std::cmp::Ordering::Greater)
        ),
        PruneOp::GtEq => !matches!(
            upper.as_ref().and_then(|upper| cmp(upper, &value)),
            Some(std::cmp::Ordering::Less)
        ),
        PruneOp::NotEq | PruneOp::IsNull | PruneOp::IsNotNull => true,
    }
}

/// Decode Iceberg's single-value encoding of a bound.
fn decode_bound(bytes: &[u8], ty: &IceType) -> Option<Bound> {
    match ty {
        IceType::Int | IceType::Date => {
            Some(Bound::Int(i32::from_le_bytes(bytes.try_into().ok()?) as i64))
        }
        IceType::Long | IceType::Timestamp | IceType::TimestampTz => {
            Some(Bound::Int(i64::from_le_bytes(bytes.try_into().ok()?)))
        }
        IceType::Float => Some(Bound::Float(
            f32::from_le_bytes(bytes.try_into().ok()?) as f64
        )),
        IceType::Double => Some(Bound::Float(f64::from_le_bytes(bytes.try_into().ok()?))),
        IceType::String => Some(Bound::Text(String::from_utf8(bytes.to_vec()).ok()?)),
        IceType::Boolean => Some(Bound::Bool(*bytes.first()? != 0)),
        _ => None,
    }
}

fn predicate_bound(value: &Value, ty: &IceType) -> Option<Bound> {
    // An integer literal arrives as whichever of the two widths the parser
    // chose, so both compare against every integer-typed bound.
    let integer = matches!(
        ty,
        IceType::Int
            | IceType::Date
            | IceType::Long
            | IceType::Timestamp
            | IceType::TimestampTz
            | IceType::Decimal { .. }
    );
    match (value, ty) {
        (Value::Integer(value), _) if integer => Some(Bound::Int(*value as i64)),
        (Value::Bigint(value), _) if integer => Some(Bound::Int(*value)),
        (Value::Varchar(value), IceType::String) => Some(Bound::Text(value.clone())),
        (Value::Double(value), IceType::Float | IceType::Double) => Some(Bound::Float(*value)),
        (Value::Boolean(value), IceType::Boolean) => Some(Bound::Bool(*value)),
        _ => None,
    }
}

fn cmp(a: &Bound, b: &Bound) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Bound::Int(a), Bound::Int(b)) => Some(a.cmp(b)),
        (Bound::Float(a), Bound::Float(b)) => a.partial_cmp(b),
        (Bound::Text(a), Bound::Text(b)) => Some(a.as_str().cmp(b.as_str())),
        (Bound::Bool(a), Bound::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_drops_the_deleted_positions() {
        assert_eq!(selection(&[1, 3], 0, 5), vec![0, 2, 4]);
        assert_eq!(selection(&[1, 3], 2, 3), vec![0, 2]);
        assert_eq!(selection(&[], 0, 2), vec![0, 1]);
        assert_eq!(selection(&[10], 0, 3), vec![0, 1, 2]);
    }

    #[test]
    fn bounds_decode_little_endian_and_compare() {
        let lower = 1i64.to_le_bytes();
        let upper = 2i64.to_le_bytes();
        assert_eq!(decode_bound(&lower, &IceType::Long), Some(Bound::Int(1)));
        let predicate = PrunePredicate {
            column: 0,
            op: PruneOp::Eq,
            value: Some(Value::Bigint(5)),
            path: None,
        };
        assert!(!bounds_may_match(
            Some(&lower),
            Some(&upper),
            &IceType::Long,
            &predicate
        ));
        let inside = PrunePredicate {
            column: 0,
            op: PruneOp::Eq,
            value: Some(Value::Bigint(2)),
            path: None,
        };
        assert!(bounds_may_match(
            Some(&lower),
            Some(&upper),
            &IceType::Long,
            &inside
        ));
    }
}
