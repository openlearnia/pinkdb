//! Argument parsing, file resolution and the union schema.
//!
//! `read_parquet` / `parquet_scan` take one positional path plus DuckDB's
//! named options. The supported set is deliberately narrow and documented;
//! everything else is refused by name (never silently ignored):
//!
//! | option | supported | note |
//! |---|---|---|
//! | `filename` | yes | virtual VARCHAR column, path as matched |
//! | `hive_partitioning` | yes | default `auto` (detect `key=value` dirs) |
//! | `union_by_name` | yes | name-matched union; mismatched types unify to VARCHAR |
//! | `file_row_number` | yes | virtual BIGINT column, 0-based within the file |
//! | `allow_empty` | yes | a glob matching nothing yields an empty result |
//! | `binary_as_string` | yes | a no-op: unannotated BYTE_ARRAY already reads as VARCHAR |
//! | `hive_types_autocast` | yes | `false` keeps every hive column VARCHAR |
//! | `hive_types`, `schema`, `column_statistics`, `compression`, `explicit_cardinality`, `encryption_config`, `parquet_version`, `can_have_nan`, `prefetch_strategy`, `utf8_validation`, `debug_use_openssl` | refused | each needs a type (MAP/LIST) or a surface pinkdb does not have |
//!
//! Multi-file reads use a glob (α refuses `read_parquet('a','b')` too: its
//! signature takes one `path` or an `ANY[]` pinkdb has no type for).

use std::collections::HashMap;
use std::sync::Arc;

use pinkdb::{DataType, PinkDbError, Result, TableFunctionArg, TableFunctionCall, Value};

use crate::file_meta::{FileMeta, read_file_meta};
use crate::glob;
use crate::hive::{HiveColumn, partition_pairs, value_of};
use crate::parse::parse_bool;

/// The parsed call: `read_parquet`/`parquet_scan` or a metadata function.
#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    /// `read_parquet('path', …)` / `parquet_scan('path', …)`.
    Scan { path: String, options: Options },
    /// `parquet_schema('path')` / `parquet_metadata` / `parquet_file_metadata`
    /// / `parquet_kv_metadata`.
    Metadata { function: String, path: String },
}

/// The supported options, with DuckDB's defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    pub filename: bool,
    /// `None` = α's default `auto`: hive columns when the paths look hive
    /// partitioned.
    pub hive_partitioning: Option<bool>,
    pub union_by_name: bool,
    pub file_row_number: bool,
    pub allow_empty: bool,
    pub hive_types_autocast: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            filename: false,
            hive_partitioning: None,
            union_by_name: false,
            file_row_number: false,
            allow_empty: false,
            hive_types_autocast: true,
        }
    }
}

/// Whether a bare FROM path is a parquet source: a `.parquet`/`.parq`/`.pq`
/// extension, or an extension-less existing directory. Glob metacharacters are
/// fine as long as the last component carries a parquet extension.
fn claims_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let last = lower.rsplit('/').next().unwrap_or(&lower);
    match last.rsplit_once('.').map(|(_, ext)| ext) {
        Some("parquet" | "parq" | "pq") => true,
        Some(_) => false,
        None => {
            !last.contains('*')
                && !last.contains('?')
                && !last.contains('[')
                && std::path::Path::new(path).is_dir()
        }
    }
}

/// The metadata function names this source serves.
pub const METADATA_FUNCTIONS: [&str; 4] = [
    "parquet_schema",
    "parquet_metadata",
    "parquet_file_metadata",
    "parquet_kv_metadata",
];

/// Parse a bound call, or `Ok(None)` when this source does not serve the
/// function name.
pub fn parse_call(call: &TableFunctionCall) -> Result<Option<Call>> {
    match call {
        TableFunctionCall::Path(path) => {
            // The replacement scan claims only paths this source can serve, so
            // another registered source (e.g. arrow) keeps its own extensions:
            // `.parquet`/`.parq`/`.pq`, or an extension-less existing directory
            // (DuckDB's directory form). A `read_parquet(...)` call is
            // unambiguous and always claimed.
            if !claims_path(path) {
                return Ok(None);
            }
            Ok(Some(Call::Scan {
                path: path.clone(),
                options: Options::default(),
            }))
        }
        TableFunctionCall::Function { name, args } => {
            let lower = name.to_ascii_lowercase();
            let is_scan = lower == "read_parquet" || lower == "parquet_scan";
            let is_metadata = METADATA_FUNCTIONS.contains(&lower.as_str());
            if !is_scan && !is_metadata {
                return Ok(None);
            }
            let mut positional: Vec<&TableFunctionArg> = Vec::new();
            let mut named: HashMap<String, &TableFunctionArg> = HashMap::new();
            for arg in args {
                match arg.name_lower() {
                    Some(name) => {
                        if named.insert(name.clone(), arg).is_some() {
                            return Err(PinkDbError::Type(format!(
                                "{lower}: the argument \"{name}\" is given twice"
                            )));
                        }
                    }
                    None => positional.push(arg),
                }
            }
            let path = one_path(&lower, &positional)?;
            if is_metadata {
                if let Some((name, _)) = named.iter().next() {
                    return Err(PinkDbError::Type(format!(
                        "{lower} takes only a path; \"{name}\" is not a supported argument"
                    )));
                }
                return Ok(Some(Call::Metadata {
                    function: lower,
                    path,
                }));
            }
            let options = parse_options(&lower, &named)?;
            Ok(Some(Call::Scan { path, options }))
        }
    }
}

/// The single positional path argument (`path VARCHAR` in α's signature).
fn one_path(function: &str, positional: &[&TableFunctionArg]) -> Result<String> {
    match positional {
        [] => Err(PinkDbError::Type(format!(
            "{function}: a path argument is required, e.g. {function}('data.parquet')"
        ))),
        [arg] => match &arg.value {
            Value::Varchar(path) => Ok(path.clone()),
            other => Err(PinkDbError::Type(format!(
                "{function}: the path must be a string literal (got {other}); \
                 pinkdb has no list type, so read several files with a glob"
            ))),
        },
        extra => Err(PinkDbError::Type(format!(
            "{function}: expected one path argument, got {} (pinkdb has no list type; \
             read several files with a glob like 'dir/**/*.parquet')",
            extra.len()
        ))),
    }
}

/// The supported named options; every unsupported name is refused by name.
fn parse_options(function: &str, named: &HashMap<String, &TableFunctionArg>) -> Result<Options> {
    let mut options = Options::default();
    for (name, arg) in named {
        match name.as_str() {
            "filename" => options.filename = flag(function, name, &arg.value)?,
            "hive_partitioning" => {
                options.hive_partitioning = Some(flag(function, name, &arg.value)?)
            }
            "union_by_name" => options.union_by_name = flag(function, name, &arg.value)?,
            "file_row_number" => options.file_row_number = flag(function, name, &arg.value)?,
            "allow_empty" => options.allow_empty = flag(function, name, &arg.value)?,
            "hive_types_autocast" => {
                options.hive_types_autocast = flag(function, name, &arg.value)?
            }
            // α's `binary_as_string=true` shape is this reader's only shape
            // (pinkdb has no BLOB); the flag is accepted for source
            // compatibility and changes nothing.
            "binary_as_string" => {
                let _ = flag(function, name, &arg.value)?;
            }
            other => {
                return Err(PinkDbError::Type(format!(
                    "{function}: the argument \"{other}\" is not supported by pinkdb's \
                     parquet reader"
                )));
            }
        }
    }
    Ok(options)
}

fn flag(function: &str, name: &str, value: &Value) -> Result<bool> {
    match value {
        Value::Boolean(b) => Ok(*b),
        Value::Varchar(s) => parse_bool(s).ok_or_else(|| {
            PinkDbError::Type(format!(
                "{function}: \"{name}\" expects TRUE or FALSE (got '{s}')"
            ))
        }),
        other => Err(PinkDbError::Type(format!(
            "{function}: \"{name}\" expects TRUE or FALSE (got {other})"
        ))),
    }
}

/// How one output column is produced.
#[derive(Debug, Clone, PartialEq)]
pub enum OutKind {
    /// The file's column at this leaf index (per-file mapping decided by
    /// [`FileResolved::out_to_file`]).
    FileColumn,
    /// `file_row_number`: 0-based row index within the file.
    RowNumber,
    /// `filename`: the path as matched.
    FileName,
    /// The hive column at this index into [`Resolved::hive`].
    Hive(usize),
}

/// One file of a resolved scan.
#[derive(Debug)]
pub struct FileResolved {
    pub meta: FileMeta,
    /// Per output column: the file's leaf index, when the output column is a
    /// file column present in this file.
    pub out_to_file: Vec<Option<usize>>,
    /// Per hive column (aligned with [`Resolved::hive`]): this file's value,
    /// `None` when the path has no such key.
    pub hive: Vec<Option<String>>,
}

/// A resolved `read_parquet` call: the files, the output schema and where
/// each output column comes from.
#[derive(Debug)]
pub struct Resolved {
    pub files: Vec<FileResolved>,
    pub schema: Vec<(String, DataType)>,
    pub kind: Vec<OutKind>,
    pub hive: Vec<HiveColumn>,
    pub options: Options,
}

impl Resolved {
    /// The row groups a scan of this call will visit (`None` when the call
    /// resolved to no files at all).
    pub fn total_rows(&self) -> u64 {
        self.files.iter().map(|f| f.meta.rows).sum()
    }
}

/// Resolve a scan call: expand the path, read every file's footer, and build
/// the union schema.
pub fn resolve(path: &str, options: &Options) -> Result<Resolved> {
    let paths = glob::expand(path).map_err(|e| {
        PinkDbError::Io(std::io::Error::other(format!(
            "cannot expand the parquet path \"{path}\": {e}"
        )))
    })?;
    if paths.is_empty() {
        if options.allow_empty {
            // α: an empty result. pinkdb's zero-column output shape is
            // representable (names/types empty, no chunks).
            return Ok(Resolved {
                files: Vec::new(),
                schema: Vec::new(),
                kind: Vec::new(),
                hive: Vec::new(),
                options: options.clone(),
            });
        }
        return Err(PinkDbError::Io(std::io::Error::other(format!(
            "No files found that match the pattern \"{path}\""
        ))));
    }
    let mut metas = Vec::with_capacity(paths.len());
    for path in &paths {
        metas.push(read_file_meta(path)?);
    }

    // Hive columns: the union of the keys across files, alphabetical.
    let detect_hive = options.hive_partitioning.unwrap_or(true);
    let per_file_hive: Vec<Vec<(String, String)>> = metas
        .iter()
        .map(|m| {
            if detect_hive {
                partition_pairs(&m.path)
            } else {
                Vec::new()
            }
        })
        .collect();
    let hive_keys: Vec<String> = per_file_hive
        .iter()
        .flatten()
        .map(|(k, _)| k.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let hive: Vec<HiveColumn> = hive_keys
        .iter()
        .map(|key| HiveColumn {
            key: key.clone(),
            ty: if options.hive_types_autocast {
                crate::hive::infer_type(
                    per_file_hive
                        .iter()
                        .flatten()
                        .filter(|(k, _)| k == key)
                        .map(|(_, v)| v.clone()),
                )
            } else {
                DataType::Varchar
            },
        })
        .collect();

    // The union schema of the file columns. `leaf_to_out[i]` maps file `i`'s
    // leaf columns to output columns (`None` for a leaf the union dropped,
    // which cannot happen: every leaf of every file is kept, either matching
    // an existing output column by name or adding one).
    let mut schema: Vec<(String, DataType)> = Vec::new();
    // Per file, per leaf column: the output column it feeds (`None` for a
    // leaf the reference schema does not name — α ignores extra columns).
    let mut leaf_to_out: Vec<Vec<Option<usize>>> = Vec::with_capacity(metas.len());
    if options.union_by_name {
        for meta in &metas {
            let mut map = Vec::with_capacity(meta.columns.len());
            for column in &meta.columns {
                let found = schema
                    .iter()
                    .position(|(name, _)| name.eq_ignore_ascii_case(&column.name));
                match found {
                    Some(index) => {
                        let unified = unify_type(schema[index].1, column.ty).ok_or_else(|| {
                            PinkDbError::Type(format!(
                                "union_by_name: no common type for column \"{}\" \
                                 ({} and {})",
                                column.name, schema[index].1, column.ty
                            ))
                        })?;
                        schema[index].1 = unified;
                        map.push(Some(index));
                    }
                    None => {
                        schema.push((column.name.clone(), column.ty));
                        map.push(Some(schema.len() - 1));
                    }
                }
            }
            leaf_to_out.push(map);
        }
    } else {
        let reference = &metas[0];
        for column in &reference.columns {
            schema.push((column.name.clone(), column.ty));
        }
        leaf_to_out.push((0..reference.columns.len()).map(Some).collect());
        for meta in &metas[1..] {
            let mut map = Vec::with_capacity(meta.columns.len());
            for column in &meta.columns {
                // α resolves every file against the *first* file's schema and
                // ignores a column the reference does not have.
                let Some(index) = reference
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&column.name))
                else {
                    map.push(None);
                    continue;
                };
                let target = reference.columns[index].ty;
                if column.ty != target && !castable(column.ty, target) {
                    return Err(schema_mismatch(
                        &reference.path,
                        &meta.path,
                        &column.name,
                        &format!(
                            "has type {} where the original file has {} (pinkdb only casts \
                             the numeric-widening, range-checked integer-narrowing, \
                             DATE→TIMESTAMP and text cases; set union_by_name := TRUE to \
                             unify the schemas instead)",
                            column.ty, target
                        ),
                        &meta
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect::<Vec<_>>(),
                    ));
                }
                map.push(Some(index));
            }
            // Every column of the reference schema must exist in this file —
            // a missing one is α's schema-mismatch error, never a NULL column
            // silently standing in for data the file does not have.
            for (index, reference_column) in reference.columns.iter().enumerate() {
                if !meta
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(&reference_column.name))
                {
                    return Err(schema_mismatch(
                        &reference.path,
                        &meta.path,
                        &reference_column.name,
                        "could not be found",
                        &meta
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect::<Vec<_>>(),
                    ));
                }
                let _ = index;
            }
            leaf_to_out.push(map);
        }
    }

    // Output columns: file columns, then the virtual columns, then hive.
    let mut kind: Vec<OutKind> = Vec::new();
    for _ in &schema {
        kind.push(OutKind::FileColumn);
    }
    if options.file_row_number {
        schema.push(("file_row_number".to_string(), DataType::Bigint));
        kind.push(OutKind::RowNumber);
    }
    if options.filename {
        schema.push(("filename".to_string(), DataType::Varchar));
        kind.push(OutKind::FileName);
    }
    for (index, column) in hive.iter().enumerate() {
        schema.push((column.key.clone(), column.ty));
        kind.push(OutKind::Hive(index));
    }

    // Per file: output column -> leaf, plus its hive values.
    let files: Vec<FileResolved> = metas
        .into_iter()
        .enumerate()
        .map(|(i, meta)| {
            let map = &leaf_to_out[i];
            let out_to_file = (0..schema.len())
                .map(|out| {
                    if kind[out] != OutKind::FileColumn {
                        return None;
                    }
                    map.iter()
                        .enumerate()
                        .find(|(_, target)| **target == Some(out))
                        .map(|(leaf, _)| leaf)
                })
                .collect();
            let hive_values = hive_keys
                .iter()
                .map(|key| {
                    per_file_hive[i]
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                })
                .collect();
            FileResolved {
                meta,
                out_to_file,
                hive: hive_values,
            }
        })
        .collect();

    Ok(Resolved {
        files,
        schema,
        kind,
        hive,
        options: options.clone(),
    })
}

/// α's glob schema-mismatch error shape.
fn schema_mismatch(
    reference: &str,
    file: &str,
    column: &str,
    what: &str,
    candidates: &[String],
) -> PinkDbError {
    PinkDbError::Type(format!(
        "schema mismatch in glob: column \"{column}\" was read from the original file \
         \"{reference}\", but {what} in file \"{file}\". Candidate names: {}. If you are \
         trying to read files with different schemas, try setting union_by_name=true",
        candidates.join(", ")
    ))
}

/// The type unification `union_by_name` uses, α-shaped: identical types stay,
/// numerics unify in the engine's numeric tower, DATE/TIMESTAMP unify to
/// TIMESTAMP, and anything else falls back to VARCHAR (α's rule — the values
/// are rendered as text).
pub fn unify_type(a: DataType, b: DataType) -> Option<DataType> {
    if a == b {
        return Some(a);
    }
    if a.is_numeric() && b.is_numeric() {
        return Some(numeric_common(a, b));
    }
    if matches!(
        (a, b),
        (DataType::Date, DataType::Timestamp) | (DataType::Timestamp, DataType::Date)
    ) {
        return Some(DataType::Timestamp);
    }
    Some(DataType::Varchar)
}

/// INTEGER → BIGINT → DOUBLE with DECIMAL as a first-class member (the
/// engine's `common_numeric` rule).
fn numeric_common(a: DataType, b: DataType) -> DataType {
    match (a, b) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Decimal { .. }, _) | (_, DataType::Decimal { .. }) => {
            let (pa, sa) = decimal_width(a);
            let (pb, sb) = decimal_width(b);
            let scale = sa.max(sb);
            let int_digits = (pa - sa).max(pb - sb) as i32;
            DataType::Decimal {
                precision: (int_digits + scale as i32).clamp(scale as i32, 38) as u8,
                scale,
            }
        }
        (DataType::Bigint, _) | (_, DataType::Bigint) => DataType::Bigint,
        _ => DataType::Integer,
    }
}

fn decimal_width(t: DataType) -> (u8, u8) {
    match t {
        DataType::Decimal { precision, scale } => (precision, scale),
        DataType::Integer => (10, 0),
        DataType::Bigint => (19, 0),
        _ => (38, 0),
    }
}

/// Whether a decoded value of type `from` can be converted to `to` — the set
/// of casts this reader performs when a file's column has to fit the scan's
/// output type (`union_by_name`, or α's "the first file's schema wins" rule).
///
/// Everything here is lossless or range-checked; a narrowing that could lose
/// data (DOUBLE → INTEGER rounding, DECIMAL scale changes) is refused at bind
/// time, where α would cast at read time.
pub fn castable(from: DataType, to: DataType) -> bool {
    if from == to {
        return true;
    }
    matches!(
        (from, to),
        (DataType::Integer, DataType::Bigint)
            | (DataType::Integer, DataType::Double)
            | (DataType::Bigint, DataType::Double)
            | (DataType::Integer, DataType::Decimal { .. })
            | (DataType::Bigint, DataType::Decimal { .. })
            | (DataType::Bigint, DataType::Integer)
            | (DataType::Date, DataType::Timestamp)
            | (DataType::Decimal { .. }, DataType::Double)
    ) || to == DataType::Varchar
        // Same-scale decimals differ only in precision, which is exact.
        || matches!(
            (from, to),
            (
                DataType::Decimal { scale: s1, .. },
                DataType::Decimal { scale: s2, .. }
            ) if s1 == s2
        )
}

/// Convert one decoded value into an output column's type (see [`castable`]).
/// `Err` carries the reason (a value that does not fit), which the scan
/// reports with its column name.
pub fn convert_value(value: Value, to: DataType) -> std::result::Result<Value, String> {
    if value.is_null() {
        return Ok(value);
    }
    if value.data_type() == Some(to) {
        return Ok(value);
    }
    Ok(match (value, to) {
        (Value::Integer(n), DataType::Bigint) => Value::Bigint(n as i64),
        (Value::Integer(n), DataType::Double) => Value::Double(n as f64),
        (Value::Bigint(n), DataType::Double) => Value::Double(n as f64),
        (Value::Bigint(n), DataType::Integer) => {
            Value::Integer(i32::try_from(n).map_err(|_| format!("{n} does not fit in INTEGER"))?)
        }
        (Value::Date(d), DataType::Timestamp) => Value::Timestamp(d as i64 * 86_400_000_000),
        (Value::Decimal(v, s), DataType::Double) => {
            Value::Double(pinkdb::types::decimal_to_f64(v, s))
        }
        (Value::Integer(n), DataType::Decimal { scale, .. }) => {
            Value::Decimal(n as i128 * 10i128.pow(scale as u32), scale)
        }
        (Value::Bigint(n), DataType::Decimal { scale, .. }) => {
            Value::Decimal(n as i128 * 10i128.pow(scale as u32), scale)
        }
        (Value::Decimal(v, s), DataType::Decimal { scale, .. }) if s == scale => {
            Value::Decimal(v, scale)
        }
        (v, DataType::Varchar) => Value::Varchar(format!("{v}")),
        (v, to) => {
            return Err(format!(
                "cannot convert {} to {to}",
                v.data_type().map(|t| t.to_string()).unwrap_or_default()
            ));
        }
    })
}

/// A one-entry lookup helper for the command key of a resolved call (used by
/// the source's cache).
pub fn cache_key(call: &TableFunctionCall) -> String {
    match call {
        TableFunctionCall::Path(path) => format!("path:{path}"),
        TableFunctionCall::Function { name, args } => {
            let mut out = format!("fn:{}", name.to_ascii_lowercase());
            for arg in args {
                match &arg.name_lower() {
                    Some(n) => out.push_str(&format!(";{n}={}", arg.value)),
                    None => out.push_str(&format!(";{}", arg.value)),
                }
            }
            out
        }
    }
}

/// The predicates over an output column that are *constant per file* (hive
/// and `filename` columns), which prune whole files before any row group is
/// touched.
pub fn file_constant_value(resolved: &Resolved, file: &FileResolved, out: usize) -> Option<Value> {
    match resolved.kind.get(out)? {
        OutKind::FileName => Some(Value::Varchar(file.meta.path.clone())),
        OutKind::Hive(index) => {
            let column = resolved.hive.get(*index)?;
            Some(match file.hive.get(*index).and_then(|v| v.as_ref()) {
                Some(text) => value_of(column.ty, text),
                None => Value::Null,
            })
        }
        _ => None,
    }
}

/// Shared cache of resolved calls (a statement binds, then scans; the cache
/// keeps the bind work from repeating — `bind` always refreshes the entry, so
/// a changed file never yields a stale schema).
#[derive(Debug, Default)]
pub struct ResolutionCache {
    entries: std::sync::Mutex<HashMap<String, Arc<Resolved>>>,
}

impl ResolutionCache {
    /// Insert (or replace) the entry for `key`.
    pub fn put(&self, key: String, resolved: Arc<Resolved>) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= 32 && !entries.contains_key(&key) {
            entries.clear();
        }
        entries.insert(key, resolved);
    }

    /// The entry for `key`, if the most recent bind put one there.
    pub fn get(&self, key: &str) -> Option<Arc<Resolved>> {
        self.entries.lock().unwrap().get(key).cloned()
    }
}
