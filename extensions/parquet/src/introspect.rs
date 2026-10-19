//! The introspection table functions: `parquet_schema`, `parquet_metadata`,
//! `parquet_file_metadata`, `parquet_kv_metadata`.
//!
//! Column names, order and value rendering follow `duckdb`
//! v2.0.0-alpha46057 with two documented divergences:
//!
//! - α's columns that need nested/MAP types are **not emitted**: α's
//!   `parquet_metadata` carries `key_value_metadata MAP(BLOB, BLOB)`,
//!   `encoding_stats STRUCT(…)[…]`, `geo_bbox STRUCT(…)` and `geo_types
//!   VARCHAR[]`; α's `parquet_file_metadata` carries `column_orders
//!   VARCHAR[]`. pinkdb has no MAP/LIST/STRUCT types, so those five are
//!   omitted instead of faked. Every remaining column keeps α's relative
//!   order, so a probe projecting the shared columns compares equal.
//! - α renders `key`/`value` of `parquet_kv_metadata` as BLOB; pinkdb renders
//!   them as VARCHAR (no BLOB type). Metadata bytes that are not UTF-8 are
//!   refused with a clear error.
//!
//! `stats_min`/`stats_max` and `stats_min_value`/`stats_max_value` carry the
//! *same* rendered value: parquet-rs exposes the deprecated `min`/`max` and
//! the modern `min_value`/`max_value` as one folded view, so the reader cannot
//! tell which pair a writer stored. α prints NULL for the pair the writer did
//! not use (for DuckDB-written files both pairs are set, so the two agree).

use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use parquet::schema::types::Type;
use pinkdb::{CHUNK_SIZE, DataChunk, DataType, PinkDbError, Result, Value, Vector};

use crate::file_meta::{FileMeta, read_file_meta};
use crate::remote;
use crate::schema_map::{alpha_type_name, render_stat};

/// A table being built row by row, chunked at [`CHUNK_SIZE`].
struct Rows {
    schema: Vec<(String, DataType)>,
    /// Finished chunks.
    chunks: Vec<Vec<Vector>>,
    /// The chunk under construction (one vector per column, ≤ CHUNK_SIZE).
    current: Vec<Vector>,
    current_rows: usize,
}

impl Rows {
    fn new(schema: Vec<(String, DataType)>) -> Self {
        let current = schema
            .iter()
            .map(|(_, ty)| Vector::with_capacity(*ty, CHUNK_SIZE))
            .collect();
        Self {
            schema,
            chunks: Vec::new(),
            current,
            current_rows: 0,
        }
    }

    /// Append one row; the value count must match the schema.
    fn push(&mut self, values: Vec<Value>) -> Result<()> {
        if values.len() != self.schema.len() {
            return Err(PinkDbError::Execution(format!(
                "internal: introspection row has {} values for {} columns",
                values.len(),
                self.schema.len()
            )));
        }
        if self.current_rows == CHUNK_SIZE {
            let done = std::mem::replace(
                &mut self.current,
                self.schema
                    .iter()
                    .map(|(_, ty)| Vector::with_capacity(*ty, CHUNK_SIZE))
                    .collect(),
            );
            self.chunks.push(done);
            self.current_rows = 0;
        }
        for (column, value) in self.current.iter_mut().zip(values) {
            column.push_value(value)?;
        }
        self.current_rows += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<DataChunk>> {
        if self.current_rows > 0 {
            let done = std::mem::take(&mut self.current);
            self.chunks.push(done);
        }
        let mut out = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks {
            let n = chunk.first().map(|c| c.len()).unwrap_or(0);
            out.push(DataChunk::new(chunk)?);
            let _ = n;
        }
        Ok(out)
    }
}

fn varchar(name: &str) -> (String, DataType) {
    (name.to_string(), DataType::Varchar)
}

fn bigint(name: &str) -> (String, DataType) {
    (name.to_string(), DataType::Bigint)
}

fn boolean(name: &str) -> (String, DataType) {
    (name.to_string(), DataType::Boolean)
}

/// The schema of one introspection function (α's column list minus the
/// nested-only columns; see the module docs).
pub fn bind(function: &str) -> Vec<(String, DataType)> {
    match function {
        "parquet_schema" => vec![
            varchar("file_name"),
            varchar("name"),
            varchar("type"),
            varchar("type_length"),
            varchar("repetition_type"),
            bigint("num_children"),
            varchar("converted_type"),
            bigint("scale"),
            bigint("precision"),
            bigint("field_id"),
            varchar("logical_type"),
            varchar("duckdb_type"),
            bigint("column_id"),
        ],
        "parquet_metadata" => vec![
            varchar("file_name"),
            bigint("row_group_id"),
            bigint("row_group_num_rows"),
            bigint("row_group_num_columns"),
            bigint("row_group_bytes"),
            bigint("column_id"),
            bigint("file_offset"),
            bigint("num_values"),
            varchar("path_in_schema"),
            varchar("type"),
            varchar("stats_min"),
            varchar("stats_max"),
            bigint("stats_null_count"),
            bigint("stats_distinct_count"),
            varchar("stats_min_value"),
            varchar("stats_max_value"),
            varchar("compression"),
            varchar("encodings"),
            bigint("index_page_offset"),
            bigint("dictionary_page_offset"),
            bigint("data_page_offset"),
            bigint("total_compressed_size"),
            bigint("total_uncompressed_size"),
            // α: key_value_metadata (MAP) omitted here.
            bigint("bloom_filter_offset"),
            bigint("bloom_filter_length"),
            boolean("min_is_exact"),
            boolean("max_is_exact"),
            bigint("row_group_compressed_bytes"),
            // α: geo_bbox, geo_types, encoding_stats omitted here.
            bigint("stats_nan_count"),
        ],
        "parquet_file_metadata" => vec![
            varchar("file_name"),
            varchar("created_by"),
            bigint("num_rows"),
            bigint("num_row_groups"),
            bigint("format_version"),
            varchar("encryption_algorithm"),
            varchar("footer_signing_key_metadata"),
            bigint("file_size_bytes"),
            bigint("footer_size"),
            // α: column_orders VARCHAR[] omitted here.
        ],
        "parquet_kv_metadata" => vec![varchar("file_name"), varchar("key"), varchar("value")],
        _ => Vec::new(),
    }
}

/// Run one introspection function over one path (glob-expanded, α's rule).
pub fn run(function: &str, path: &str) -> Result<Vec<DataChunk>> {
    let mut metas = Vec::new();
    if remote::is_remote(path) {
        #[cfg(feature = "remote")]
        metas.push(remote::read_meta(path)?);
        #[cfg(not(feature = "remote"))]
        return Err(remote::missing_feature(path));
    } else {
        let paths = crate::glob::expand(path).map_err(|e| {
            PinkDbError::Io(std::io::Error::other(format!(
                "cannot expand the parquet path \"{path}\": {e}"
            )))
        })?;
        if paths.is_empty() {
            return Err(PinkDbError::Io(std::io::Error::other(format!(
                "No files found that match the pattern \"{path}\""
            ))));
        }
        metas.reserve(paths.len());
        for path in paths {
            metas.push(read_file_meta(&path)?);
        }
    }
    match function {
        "parquet_schema" => schema_rows(&metas),
        "parquet_metadata" => metadata_rows(&metas),
        "parquet_file_metadata" => file_metadata_rows(&metas),
        "parquet_kv_metadata" => kv_rows(&metas),
        other => Err(PinkDbError::Execution(format!(
            "unknown parquet metadata function \"{other}\""
        ))),
    }
}

/// One file's schema tree, depth-first from the root (α's schema-element
/// order), one row per schema element.
fn schema_rows(metas: &[FileMeta]) -> Result<Vec<DataChunk>> {
    let mut rows = Rows::new(bind("parquet_schema"));
    for meta in metas {
        let mut leaf = 0usize;
        let mut index: i64 = 0;
        push_schema_element(
            &mut rows,
            meta,
            &meta.schema_root,
            &mut index,
            &mut leaf,
            true,
        )?;
    }
    rows.finish()
}

fn push_schema_element(
    rows: &mut Rows,
    meta: &FileMeta,
    element: &Type,
    index: &mut i64,
    leaf: &mut usize,
    root: bool,
) -> Result<()> {
    let info = element.get_basic_info();
    let primitive = element.is_primitive();
    let physical = if primitive {
        Some(element.get_physical_type())
    } else {
        None
    };
    let decimal = matches!(info.converted_type(), ConvertedType::DECIMAL)
        || matches!(info.logical_type_ref(), Some(LogicalType::Decimal(_)));
    let engine_type = if primitive {
        let ty = meta.columns.get(*leaf).map(|c| c.ty);
        *leaf += 1;
        ty
    } else {
        None
    };
    let values = vec![
        Value::Varchar(meta.path.clone()),
        Value::Varchar(element.name().to_string()),
        match physical {
            Some(p) => Value::Varchar(format!("{p:?}")),
            None => Value::Null,
        },
        match physical {
            Some(PhysicalType::FIXED_LEN_BYTE_ARRAY) => {
                // Flat schemas only: the leaf ordinal is the element order.
                let length = meta.columns.get(*leaf).map(|c| c.type_length).unwrap_or(0);
                Value::Varchar(length.to_string())
            }
            _ => Value::Null,
        },
        if info.has_repetition() {
            Value::Varchar(format!("{:?}", info.repetition()))
        } else if root {
            // The root element's repetition is optional in the thrift
            // `SchemaElement` and implicitly REQUIRED; α prints REQUIRED.
            Value::Varchar("REQUIRED".to_string())
        } else {
            Value::Null
        },
        if element.is_group() {
            Value::Bigint(element.get_fields().len() as i64)
        } else {
            Value::Null
        },
        if info.converted_type() == ConvertedType::NONE {
            Value::Null
        } else {
            Value::Varchar(format!("{:?}", info.converted_type()))
        },
        if decimal {
            Value::Bigint(element.get_scale() as i64)
        } else {
            Value::Null
        },
        if decimal {
            Value::Bigint(element.get_precision() as i64)
        } else {
            Value::Null
        },
        if info.has_id() {
            Value::Bigint(info.id() as i64)
        } else {
            Value::Null
        },
        match info.logical_type_ref() {
            Some(logical_type) => Value::Varchar(format!("{logical_type:?}")),
            None => Value::Null,
        },
        match engine_type.and_then(alpha_type_name) {
            Some(text) => Value::Varchar(text),
            None => Value::Null,
        },
        Value::Bigint(*index),
    ];
    *index += 1;
    rows.push(values)?;
    if element.is_group() {
        for child in element.get_fields() {
            push_schema_element(rows, meta, child, index, leaf, false)?;
        }
    }
    Ok(())
}

/// One row per column chunk (α's column order, minus the nested-only ones).
fn metadata_rows(metas: &[FileMeta]) -> Result<Vec<DataChunk>> {
    let mut rows = Rows::new(bind("parquet_metadata"));
    for meta in metas {
        for (rg_index, rg) in meta.row_groups.iter().enumerate() {
            for (leaf, column) in meta.columns.iter().enumerate() {
                let chunk = &rg.chunks[leaf];
                let stats = rg.stats.get(leaf).and_then(|s| s.as_ref());
                let (min, max) = match stats {
                    Some(stats) => (
                        stats.min.as_ref().map(render_stat),
                        stats.max.as_ref().map(render_stat),
                    ),
                    None => (None, None),
                };
                let values = vec![
                    Value::Varchar(meta.path.clone()),
                    Value::Bigint(rg_index as i64),
                    Value::Bigint(rg.rows as i64),
                    Value::Bigint(meta.columns.len() as i64),
                    Value::Bigint(rg.bytes),
                    Value::Bigint(leaf as i64),
                    Value::Bigint(chunk.file_offset),
                    Value::Bigint(chunk.num_values),
                    Value::Varchar(column.name.clone()),
                    Value::Varchar(format!("{:?}", column.physical)),
                    match &min {
                        Some(text) => Value::Varchar(text.clone()),
                        None => Value::Null,
                    },
                    match &max {
                        Some(text) => Value::Varchar(text.clone()),
                        None => Value::Null,
                    },
                    match stats.and_then(|s| s.null_count) {
                        Some(count) => Value::Bigint(count as i64),
                        None => Value::Null,
                    },
                    match chunk.distinct_count {
                        Some(count) => Value::Bigint(count as i64),
                        None => Value::Null,
                    },
                    // α's min_value/max_value pair: the same folded value (see
                    // the module docs).
                    match &min {
                        Some(text) => Value::Varchar(text.clone()),
                        None => Value::Null,
                    },
                    match &max {
                        Some(text) => Value::Varchar(text.clone()),
                        None => Value::Null,
                    },
                    Value::Varchar(chunk.compression.clone()),
                    Value::Varchar(chunk.encodings.join(",")),
                    match chunk.index_page_offset {
                        Some(offset) => Value::Bigint(offset),
                        None => Value::Null,
                    },
                    match chunk.dictionary_page_offset {
                        Some(offset) => Value::Bigint(offset),
                        None => Value::Null,
                    },
                    Value::Bigint(chunk.data_page_offset),
                    Value::Bigint(chunk.compressed_size),
                    Value::Bigint(chunk.uncompressed_size),
                    match chunk.bloom_filter_offset {
                        Some(offset) => Value::Bigint(offset),
                        None => Value::Null,
                    },
                    match chunk.bloom_filter_length {
                        Some(length) => Value::Bigint(length as i64),
                        None => Value::Null,
                    },
                    match stats {
                        Some(stats) => Value::Boolean(stats.min_exact),
                        None => Value::Null,
                    },
                    match stats {
                        Some(stats) => Value::Boolean(stats.max_exact),
                        None => Value::Null,
                    },
                    Value::Bigint(rg.compressed_bytes),
                    Value::Null,
                ];
                rows.push(values)?;
            }
        }
    }
    rows.finish()
}

/// One row per file.
fn file_metadata_rows(metas: &[FileMeta]) -> Result<Vec<DataChunk>> {
    let mut rows = Rows::new(bind("parquet_file_metadata"));
    for meta in metas {
        let values = vec![
            Value::Varchar(meta.path.clone()),
            match &meta.created_by {
                Some(created_by) => Value::Varchar(created_by.clone()),
                None => Value::Null,
            },
            Value::Bigint(meta.rows as i64),
            Value::Bigint(meta.row_groups.len() as i64),
            Value::Bigint(meta.version as i64),
            // Encryption is not supported by this reader: an encrypted file
            // fails while reading the footer, so a readable file never has an
            // algorithm to report.
            Value::Null,
            Value::Null,
            Value::Bigint(meta.file_size as i64),
            Value::Bigint(meta.footer_size as i64),
        ];
        rows.push(values)?;
    }
    rows.finish()
}

/// One row per key-value pair.
fn kv_rows(metas: &[FileMeta]) -> Result<Vec<DataChunk>> {
    let mut rows = Rows::new(bind("parquet_kv_metadata"));
    for meta in metas {
        for (key, value) in &meta.kv {
            let text = std::str::from_utf8(value).map_err(|_| {
                PinkDbError::Execution(format!(
                    "parquet_kv_metadata: the value of key \"{key}\" in \"{}\" is not UTF-8 \
                     (pinkdb has no BLOB type)",
                    meta.path
                ))
            })?;
            rows.push(vec![
                Value::Varchar(meta.path.clone()),
                Value::Varchar(key.clone()),
                Value::Varchar(text.to_string()),
            ])?;
        }
    }
    rows.finish()
}
