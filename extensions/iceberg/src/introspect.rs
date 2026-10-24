//! `iceberg_snapshots` and `iceberg_metadata`: the table's own log read back
//! as rows. Neither touches a data file — both are useful on their own when a
//! query returns nothing, because they show what the metadata actually says.
//!
//! The shapes follow DuckDB's iceberg extension so the same queries work on
//! pinkdb. One deliberate difference: [`snapshots`] has no `summary` MAP
//! column (pinkdb has no map type). Use `iceberg_metadata` for per-file
//! detail.

use pinkdb::{CHUNK_SIZE, DataChunk, DataType, PinkDbError, Result, Value, Vector};

use crate::manifest;
use crate::obj::Object;
use crate::table::Table;

/// Avro timestamps are milliseconds; pinkdb's `TIMESTAMP` is microseconds.
const MILLIS: i64 = 1_000;

pub fn snapshot_types() -> Vec<(String, DataType)> {
    vec![
        ("sequence_number".to_string(), DataType::Bigint),
        ("snapshot_id".to_string(), DataType::Bigint),
        ("parent_id".to_string(), DataType::Bigint),
        ("timestamp_ms".to_string(), DataType::Timestamp),
        ("manifest_list".to_string(), DataType::Varchar),
        ("operation".to_string(), DataType::Varchar),
    ]
}

pub fn manifest_types() -> Vec<(String, DataType)> {
    [
        ("manifest_path", DataType::Varchar),
        ("manifest_sequence_number", DataType::Bigint),
        ("manifest_content", DataType::Varchar),
        ("status", DataType::Varchar),
        ("content", DataType::Varchar),
        ("file_path", DataType::Varchar),
        ("file_format", DataType::Varchar),
        ("record_count", DataType::Bigint),
        ("data_sequence_number", DataType::Bigint),
        ("file_sequence_number", DataType::Bigint),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// Every snapshot the table's metadata remembers, in sequence order.
pub fn snapshots(path: &str) -> Result<Vec<DataChunk>> {
    let table = Table::open(path)?;
    let rows = table
        .snapshots()
        .into_iter()
        .map(|snapshot| {
            vec![
                Value::Bigint(snapshot.sequence_number),
                Value::Bigint(snapshot.id),
                snapshot.parent_id.map_or(Value::Null, Value::Bigint),
                Value::Timestamp(snapshot.timestamp_ms.saturating_mul(MILLIS)),
                Value::Varchar(snapshot.manifest_list),
                snapshot.operation.map_or(Value::Null, Value::Varchar),
            ]
        })
        .collect();
    emit(&snapshot_types(), rows)
}

/// One row per manifest entry of the current snapshot: which file each
/// manifest names, whether it is a data or delete file, and how many rows it
/// holds. `DELETED` entries mean the file is no longer part of the table.
pub fn metadata(path: &str) -> Result<Vec<DataChunk>> {
    let table = Table::open(path)?;
    let mut rows = Vec::new();
    if let Some(snapshot) = table.current_snapshot() {
        let manifest_list = table.resolve_object(&snapshot.manifest_list);
        let object = std::sync::Arc::new(Object::open(&manifest_list)?);
        let manifests = manifest::read_manifest_list(&mut object.reader())?;
        for manifest in &manifests {
            let entries = {
                let path = table.resolve_object(&manifest.path);
                let object = std::sync::Arc::new(Object::open(&path)?);
                manifest::read_manifest(&mut object.reader(), manifest.sequence_number)?
            };
            for entry in entries {
                rows.push(vec![
                    Value::Varchar(manifest.path.clone()),
                    Value::Bigint(manifest.sequence_number),
                    Value::Varchar(manifest.content.name().to_string()),
                    Value::Varchar(entry.status.name().to_string()),
                    Value::Varchar(entry.file.content.name().to_string()),
                    Value::Varchar(entry.file.path.clone()),
                    Value::Varchar(entry.file.format.clone()),
                    Value::Bigint(entry.file.record_count),
                    entry.sequence_number.map_or(Value::Null, Value::Bigint),
                    entry
                        .file_sequence_number
                        .map_or(Value::Null, Value::Bigint),
                ]);
            }
        }
    }
    emit(&manifest_types(), rows)
}

/// Split rows into chunks of the bound column types.
fn emit(spec: &[(String, DataType)], rows: Vec<Vec<Value>>) -> Result<Vec<DataChunk>> {
    let mut chunks = Vec::new();
    for group in rows.chunks(CHUNK_SIZE) {
        let mut columns = Vec::with_capacity(spec.len());
        for (column, (name, ty)) in spec.iter().enumerate() {
            let mut vector = Vector::with_capacity(*ty, group.len());
            for row in group {
                let value = row[column].clone();
                if matches!(value, Value::Null) {
                    vector.push_null()?;
                } else {
                    vector.push_valid(value).map_err(|error| {
                        PinkDbError::Execution(format!("iceberg: {name} does not hold {error}"))
                    })?;
                }
            }
            columns.push(std::sync::Arc::new(vector));
        }
        chunks.push(DataChunk::projected(columns, group.len()));
    }
    Ok(chunks)
}
