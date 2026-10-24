//! Iceberg table metadata (wave 72).
//!
//! One job: turn a table location — a table directory, a `…/metadata` directory
//! or a metadata JSON file, local or remote — into the current metadata
//! document, and read the parts a scan needs out of it: the format version, the
//! schema (by id), the partition specs and the snapshot a query selected.
//!
//! Metadata resolution is Iceberg's: `metadata/version-hint.text` names the
//! current metadata file. When it is absent (a local table, typically a copy
//! that lost it) the newest `metadata/vN.metadata.json` wins. A remote
//! directory has no listing in this transport, so it needs the hint or an
//! explicit metadata path — refused by name, never guessed.

use serde_json::Value as Json;

use pinkdb::{DataType, PinkDbError, Result};

use crate::obj::Object;

/// The Iceberg primitive and logical types a scan can bind.
#[derive(Debug, Clone, PartialEq)]
pub enum IceType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Date,
    Timestamp,
    TimestampTz,
    Binary,
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// Anything else: `uuid`, `time`, `fixed[n]`, `struct`, `list`, `map`.
    /// Read as text through the Parquet reader's own mapping.
    Other(String),
}

impl IceType {
    fn parse(ty: &Json) -> IceType {
        match ty {
            Json::String(name) => match name.as_str() {
                "boolean" => IceType::Boolean,
                "int" => IceType::Int,
                "long" => IceType::Long,
                "float" => IceType::Float,
                "double" => IceType::Double,
                "string" => IceType::String,
                "date" => IceType::Date,
                "timestamp" => IceType::Timestamp,
                "timestamptz" => IceType::TimestampTz,
                "binary" => IceType::Binary,
                other => IceType::Other(other.to_string()),
            },
            Json::Object(obj) => {
                let kind = obj.get("type").and_then(Json::as_str).unwrap_or("unknown");
                match kind {
                    "decimal" => IceType::Decimal {
                        precision: obj.get("precision").and_then(Json::as_u64).unwrap_or(38) as u8,
                        scale: obj.get("scale").and_then(Json::as_u64).unwrap_or(0) as u8,
                    },
                    other => IceType::Other(other.to_string()),
                }
            }
            other => IceType::Other(other.to_string()),
        }
    }

    /// The Iceberg spelling, for error messages.
    pub fn name(&self) -> String {
        match self {
            IceType::Boolean => "boolean".to_string(),
            IceType::Int => "int".to_string(),
            IceType::Long => "long".to_string(),
            IceType::Float => "float".to_string(),
            IceType::Double => "double".to_string(),
            IceType::String => "string".to_string(),
            IceType::Date => "date".to_string(),
            IceType::Timestamp => "timestamp".to_string(),
            IceType::TimestampTz => "timestamptz".to_string(),
            IceType::Binary => "binary".to_string(),
            IceType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
            IceType::Other(name) => name.clone(),
        }
    }

    /// The engine column type a scan binds for this field.
    pub fn data_type(&self) -> DataType {
        match self {
            IceType::Boolean => DataType::Boolean,
            IceType::Int => DataType::Integer,
            IceType::Long => DataType::Bigint,
            // pinkdb has no FLOAT column type; a float is carried as a double.
            IceType::Float | IceType::Double => DataType::Double,
            IceType::String | IceType::Binary | IceType::Other(_) => DataType::Varchar,
            IceType::Date => DataType::Date,
            IceType::Timestamp | IceType::TimestampTz => DataType::Timestamp,
            IceType::Decimal { precision, scale } => DataType::Decimal {
                precision: *precision,
                scale: *scale,
            },
        }
    }
}

/// One field of a table schema. `id` is the identity that survives a rename:
/// Parquet files are matched to schema fields by id first, by name second.
#[derive(Debug, Clone)]
pub struct Field {
    pub id: i64,
    pub name: String,
    pub ty: IceType,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    /// As written in the metadata (absolute, or relative to the table root).
    pub manifest_list: String,
    pub operation: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PartitionField {
    pub source_id: i64,
    pub transform: String,
}

#[derive(Debug, Clone)]
pub struct PartitionSpec {
    pub id: i64,
    pub fields: Vec<PartitionField>,
}

/// Which snapshot a query asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selector {
    Current,
    Id(i64),
    /// A `TIMESTAMP` value, in microseconds since the Unix epoch.
    Timestamp(i64),
}

/// A resolved table: the metadata document plus the paths its objects resolve
/// against.
#[derive(Debug)]
pub struct Table {
    /// The path the query gave.
    pub path: String,
    /// The metadata JSON actually read.
    pub metadata_path: String,
    /// `<table>`: relative object paths in the metadata resolve against it.
    pub table_root: String,
    doc: Json,
}

impl Table {
    /// Resolve `path` (a table directory, a `…/metadata` directory or a
    /// metadata JSON file; local or `https://`/`s3://`) to a table.
    pub fn open(path: &str) -> Result<Table> {
        let given = path.trim().trim_end_matches('/');
        if given.is_empty() {
            return Err(PinkDbError::Execution(
                "iceberg: an empty table path".to_string(),
            ));
        }
        let metadata_path = if given.ends_with(".json") {
            given.to_string()
        } else if last_segment(given) == "metadata" {
            resolve_metadata(parent(given), given.to_string())?
        } else {
            resolve_metadata(given.to_string(), join(given, "metadata"))?
        };
        let doc = parse_json(&Object::open(&metadata_path)?.read_all()?, &metadata_path)?;
        let table_root = parent(&parent(&metadata_path));
        let table = Table {
            path: given.to_string(),
            metadata_path,
            table_root,
            doc,
        };
        let version = table.format_version();
        if version != 2 {
            return Err(PinkDbError::Execution(format!(
                "iceberg: {} is format version {version}; pinkdb reads Iceberg v2 \
                 tables (v1 metadata and equality deletes are follow-ups)",
                table.metadata_path
            )));
        }
        Ok(table)
    }

    pub fn format_version(&self) -> i64 {
        self.doc
            .get("format-version")
            .and_then(Json::as_i64)
            .unwrap_or(1)
    }

    fn list(&self, key: &str) -> &[Json] {
        self.doc
            .get(key)
            .and_then(Json::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Every snapshot in the metadata's own order (oldest first in practice).
    pub fn snapshots(&self) -> Vec<Snapshot> {
        self.list("snapshots")
            .iter()
            .filter_map(parse_snapshot)
            .collect()
    }

    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.doc.get("current-snapshot-id").and_then(Json::as_i64)
    }

    pub fn current_snapshot(&self) -> Option<Snapshot> {
        let id = self.current_snapshot_id()?;
        self.snapshot(id)
    }

    pub fn snapshot(&self, id: i64) -> Option<Snapshot> {
        self.snapshots().into_iter().find(|s| s.id == id)
    }

    /// The snapshot a query asked for. Time travel:
    ///
    /// - `Id`: that snapshot, or an error naming the snapshots that exist;
    /// - `Timestamp`: the newest snapshot at or before it;
    /// - `Current`: the table's `current-snapshot-id` (`None` for a table that
    ///   has never committed — an empty table, not an error).
    pub fn select(&self, selector: Selector) -> Result<Option<Snapshot>> {
        let snapshots = self.snapshots();
        match selector {
            Selector::Current => match self.current_snapshot_id() {
                Some(id) => snapshots
                    .into_iter()
                    .find(|s| s.id == id)
                    .map(Some)
                    .ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "iceberg: the metadata of {} names the current snapshot {id} but \
                             does not list it",
                            self.path
                        ))
                    }),
                None => Ok(None),
            },
            Selector::Id(id) => snapshots
                .iter()
                .find(|s| s.id == id)
                .cloned()
                .map(Some)
                .ok_or_else(|| {
                    PinkDbError::Execution(format!(
                        "iceberg: snapshot {id} is not in {}'s metadata (snapshots: {})",
                        self.path,
                        list_ids(&snapshots)
                    ))
                }),
            Selector::Timestamp(micros) => {
                let ms = micros.div_euclid(1000);
                let mut best: Option<&Snapshot> = None;
                for snapshot in &snapshots {
                    if snapshot.timestamp_ms <= ms
                        && best.is_none_or(|b| snapshot.timestamp_ms >= b.timestamp_ms)
                    {
                        best = Some(snapshot);
                    }
                }
                best.cloned().map(Some).ok_or_else(|| {
                    PinkDbError::Execution(format!(
                        "iceberg: no snapshot of {} at or before {ms} ms since the epoch \
                         (snapshots: {})",
                        self.path,
                        list_ids(&snapshots)
                    ))
                })
            }
        }
    }

    /// The schema a scan binds: the table's current one (`current-schema-id`),
    /// falling back to the newest in the list. A time-travelled snapshot reads
    /// through the current schema, so a renamed column keeps its new name and a
    /// dropped one is not resurrected.
    pub fn current_schema(&self) -> Result<Schema> {
        let schemas = self.list("schemas");
        let current = self.doc.get("current-schema-id").and_then(Json::as_i64);
        let picked = current
            .and_then(|id| {
                schemas
                    .iter()
                    .find(|s| s.get("schema-id").and_then(Json::as_i64) == Some(id))
            })
            .or_else(|| schemas.last());
        let schema = picked.ok_or_else(|| {
            PinkDbError::Execution(format!("iceberg: {}'s metadata has no schema", self.path))
        })?;
        parse_schema(schema)
    }

    pub fn partition_specs(&self) -> Vec<PartitionSpec> {
        self.list("partition-specs")
            .iter()
            .map(parse_spec)
            .collect()
    }

    /// The spec a manifest belongs to (`partition_spec_id` on the manifest
    /// file), falling back to the table's default spec.
    pub fn partition_spec(&self, id: Option<i64>) -> Option<PartitionSpec> {
        let specs = self.partition_specs();
        let wanted = id
            .or_else(|| self.doc.get("default-spec-id").and_then(Json::as_i64))
            .unwrap_or(0);
        specs.into_iter().find(|s| s.id == wanted)
    }

    /// An object path from the metadata, resolved to something openable:
    /// absolute paths and URLs pass through, everything else joins the table
    /// root (which is how Iceberg writers store paths, and what DuckDB's
    /// `allow_moved_paths` resolves).
    pub fn resolve_object(&self, path: &str) -> String {
        let path = path.strip_prefix("file://").unwrap_or(path);
        if path.contains("://") || path.starts_with('/') {
            path.to_string()
        } else {
            join(&self.table_root, path)
        }
    }
}

fn list_ids(snapshots: &[Snapshot]) -> String {
    if snapshots.is_empty() {
        return "none".to_string();
    }
    snapshots
        .iter()
        .map(|s| s.id.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_schema(schema: &Json) -> Result<Schema> {
    let mut fields = Vec::new();
    for field in schema
        .get("fields")
        .and_then(Json::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
    {
        fields.push(Field {
            id: field.get("id").and_then(Json::as_i64).unwrap_or(0),
            name: field
                .get("name")
                .and_then(Json::as_str)
                .unwrap_or("")
                .to_string(),
            ty: field
                .get("type")
                .map(IceType::parse)
                .unwrap_or_else(|| IceType::Other("unknown".to_string())),
        });
    }
    if fields.is_empty() {
        return Err(PinkDbError::Execution(
            "iceberg: a table schema with no fields".to_string(),
        ));
    }
    Ok(Schema { fields })
}

fn parse_snapshot(snapshot: &Json) -> Option<Snapshot> {
    Some(Snapshot {
        id: snapshot.get("snapshot-id").and_then(Json::as_i64)?,
        parent_id: snapshot.get("parent-snapshot-id").and_then(Json::as_i64),
        sequence_number: snapshot
            .get("sequence-number")
            .and_then(Json::as_i64)
            .unwrap_or(0),
        timestamp_ms: snapshot
            .get("timestamp-ms")
            .and_then(Json::as_i64)
            .unwrap_or(0),
        manifest_list: snapshot.get("manifest-list")?.as_str()?.to_string(),
        operation: snapshot
            .get("summary")
            .and_then(|s| s.get("operation"))
            .and_then(Json::as_str)
            .map(str::to_string),
    })
}

fn parse_spec(spec: &Json) -> PartitionSpec {
    PartitionSpec {
        id: spec.get("spec-id").and_then(Json::as_i64).unwrap_or(0),
        fields: spec
            .get("fields")
            .and_then(Json::as_array)
            .map(|fields| {
                fields
                    .iter()
                    .map(|field| PartitionField {
                        source_id: field.get("source-id").and_then(Json::as_i64).unwrap_or(0),
                        transform: field
                            .get("transform")
                            .and_then(Json::as_str)
                            .unwrap_or("identity")
                            .to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Find the table's current metadata file below `metadata_dir`.
fn resolve_metadata(table_root: String, metadata_dir: String) -> Result<String> {
    let hint = join(&metadata_dir, "version-hint.text");
    match Object::open(&hint).and_then(|object| object.read_all()) {
        Ok(bytes) => {
            let version = String::from_utf8_lossy(&bytes).trim().to_string();
            if version.is_empty() {
                return Err(PinkDbError::Execution(format!(
                    "iceberg: {hint} is empty; it must hold the current metadata version"
                )));
            }
            Ok(join(&metadata_dir, &format!("v{version}.metadata.json")))
        }
        Err(hint_error) => {
            if crate::obj::is_remote(&metadata_dir) {
                return Err(PinkDbError::Execution(format!(
                    "iceberg: {table_root} is a remote table with no \
                     metadata/version-hint.text, and a remote directory cannot be \
                     listed; pass the metadata file (…/metadata/vN.metadata.json) \
                     instead ({})",
                    hint_error
                )));
            }
            newest_metadata(&metadata_dir).ok_or_else(|| {
                PinkDbError::Execution(format!(
                    "iceberg: {table_root} has neither metadata/version-hint.text nor \
                     metadata/vN.metadata.json"
                ))
            })
        }
    }
}

/// The highest-numbered `metadata/vN.metadata.json`, for a local table.
fn newest_metadata(metadata_dir: &str) -> Option<String> {
    let mut best: Option<(u64, String)> = None;
    for entry in std::fs::read_dir(metadata_dir).ok()?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(version) = name
            .strip_prefix('v')
            .and_then(|rest| rest.strip_suffix(".metadata.json"))
            .and_then(|digits| digits.parse::<u64>().ok())
        else {
            continue;
        };
        let path = join(metadata_dir, &name);
        if best.as_ref().is_none_or(|(current, _)| version > *current) {
            best = Some((version, path));
        }
    }
    best.map(|(_, path)| path)
}

fn parse_json(bytes: &[u8], path: &str) -> Result<Json> {
    serde_json::from_slice(bytes).map_err(|e| {
        PinkDbError::Execution(format!(
            "iceberg: {path} is not a metadata JSON document: {e}"
        ))
    })
}

/// The last path component, for both local paths and URLs.
fn last_segment(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// The parent of a local path or a URL (`s3://bucket/a/b` → `s3://bucket/a`).
pub fn parent(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(index) if index >= scheme_end(trimmed) => trimmed[..index].to_string(),
        _ => trimmed.to_string(),
    }
}

/// `s3://` is five bytes: a `/` before that belongs to the scheme.
fn scheme_end(path: &str) -> usize {
    path.find("://").map_or(0, |index| index + 3)
}

/// Join `rel` onto `base`, leaving absolute paths and URLs alone.
pub fn join(base: &str, rel: &str) -> String {
    if rel.contains("://") || rel.starts_with('/') {
        return rel.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        rel.trim_start_matches("./").trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_and_takes_parents_of_paths_and_urls() {
        assert_eq!(
            join("s3://b/lake/t", "data/x.parquet"),
            "s3://b/lake/t/data/x.parquet"
        );
        assert_eq!(join("/tmp/t/", "./data/x.parquet"), "/tmp/t/data/x.parquet");
        assert_eq!(join("/tmp/t", "s3://b/x"), "s3://b/x");
        assert_eq!(parent("s3://b/lake/t/metadata"), "s3://b/lake/t");
        assert_eq!(parent("s3://b"), "s3://b");
        assert_eq!(
            parent("tests/fixtures/simple/metadata"),
            "tests/fixtures/simple"
        );
        assert_eq!(parent("/tmp/x"), "/tmp");
        assert_eq!(last_segment("s3://b/lake/t/metadata"), "metadata");
    }

    #[test]
    fn refuses_v1_metadata_by_name() {
        // A v1 table would need the `schema`/`schema-id` fields this reader
        // does not look at; refusing it is honest until a fixture pins it.
        let dir = std::env::temp_dir().join(format!("pinkdb-iceberg-v1-{}", std::process::id()));
        let metadata = dir.join("metadata");
        std::fs::create_dir_all(&metadata).unwrap();
        let path = metadata.join("v1.metadata.json");
        std::fs::write(
            &path,
            r#"{"format-version": 1, "location": "/tmp/t", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "snapshots": []}"#,
        )
        .unwrap();
        let err = Table::open(dir.to_str().unwrap()).unwrap_err().to_string();
        assert!(err.contains("format version 1"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reads_the_fixture_table_and_its_snapshots() {
        let table = Table::open("tests/fixtures/simple").unwrap();
        assert_eq!(table.format_version(), 2);
        assert_eq!(table.snapshots().len(), 2);
        assert_eq!(table.select(Selector::Current).unwrap().unwrap().id, 1002);
        assert_eq!(table.select(Selector::Id(1001)).unwrap().unwrap().id, 1001);
        // 1700000050000 ms sits between the two commits.
        let micros = 1_700_000_050_000_000;
        assert_eq!(
            table
                .select(Selector::Timestamp(micros))
                .unwrap()
                .unwrap()
                .id,
            1001
        );
        let schema = table.current_schema().unwrap();
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[0].ty, IceType::Long);
        assert_eq!(
            table.resolve_object("data/data-1.parquet"),
            "tests/fixtures/simple/data/data-1.parquet"
        );
    }
}
