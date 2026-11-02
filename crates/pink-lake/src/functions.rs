//! DuckLake's table functions, answered for one [`Lake`] (wave 132c).
//!
//! The engine exposes these as `SELECT * FROM ducklake_snapshots('db')` and
//! `CALL ducklake_flush_inlined_data('db', table_name := 't')`; this layer is
//! what those call once the arguments are evaluated, so it shares nothing with
//! DuckDB's binder but the signatures and the output columns. The shape is
//! DuckDB's: [`output_schema`] answers a binder without running anything,
//! [`call_read`] runs a read function on a borrowed lake, [`call_write`] runs a
//! `CALL` function on an exclusive one, and [`is_mutating`] tells the two
//! apart.
//!
//! A call's first argument is the lake's catalog name. The engine resolved it
//! before binding (the name selects *which* lake), so this layer accepts any
//! text there and ignores it.
//!
//! # Types pinkdb does not have yet
//!
//! DuckDB's return types name MAP, LIST, UUID, UBIGINT, BLOB and TIMESTAMPTZ;
//! pinkdb has none of them (a later wave adds LIST and MAP). Until then this
//! module's mapping stands in for those, in one place ([`FnArg`] on the way in,
//! the value helpers here on the way out):
//!
//! | DuckDB | here |
//! | --- | --- |
//! | `MAP(VARCHAR, VARCHAR[])` | `VARCHAR` holding DuckDB's text form, e.g. `{tables_created=[main.t]}` |
//! | `VARCHAR[]` | `VARCHAR`, DuckDB's text form |
//! | `UUID` | `VARCHAR`, canonical lowercase |
//! | `UBIGINT` | `BIGINT` |
//! | `BLOB` | `VARCHAR`: `NULL` for no key, hex otherwise |
//! | `TIMESTAMPTZ` | `TIMESTAMP` (microseconds since the epoch, UTC) |
//!
//! # Differences from DuckDB that stay
//!
//! - Snapshot ids are DuckLake's 0-based ids: pinkdb version `v` is snapshot
//!   `v - 1` (pinkdb's version 1 is DuckLake's snapshot 0, the
//!   `created_schema:"main"` snapshot). The named arguments that take a
//!   snapshot (`ducklake_list_files(snapshot_version := …)`) take the same id.
//! - `ducklake_last_committed_snapshot` is answered by the engine's session:
//!   it is the last snapshot *this connection* committed to the lake. The
//!   handle itself records no commits, so this function's `call_read` reports
//!   `NULL` for a bare handle.
//! - `ducklake_list_files` reports no encryption keys or delete-file footer
//!   size: pinkdb's catalog keeps a delete file's path and row count but not
//!   its footer, and the storage facade does not hand out per-file keys. Those
//!   three columns are `NULL`.
//! - `ducklake_table_changes` reports `update_preimage`/`update_postimage` for
//!   an `UPDATE` exactly when DuckDB does: an insertion and a deletion with the
//!   same `(snapshot_id, rowid)`. The three row-level functions read the
//!   catalog (see [`crate::cdc`]), not a commit of pinkdb's own, so they answer
//!   for any DuckLake, including one only DuckDB wrote.
//! - A row whose file carries no recorded row-id range reads `rowid = NULL`,
//!   where DuckDB's is `row_id_start + position`; a NULL row id never pairs
//!   (DuckDB's join on `rowid` behaves the same), so such a range reports
//!   plain `insert`/`delete`.
//! - `ducklake_options` reports the catalog's own `ducklake_metadata` rows,
//!   exactly as DuckDB does, so a pinkdb-written lake shows pinkdb's option set
//!   (`data_format`, `current_version`, …) and a DuckDB-written lake shows
//!   DuckDB's; an option name this build does not know keeps a `NULL`
//!   description, as in DuckDB.
//! - A flush keeps the flushed rows in the inlined table, closing them with an
//!   end snapshot instead of deleting them, which is what lets a later snapshot
//!   still read them at an older version. The row-level change functions read
//!   that closing as a deletion of each flushed row plus an insertion of the
//!   file that carries it — what DuckDB computes from the same catalog, where
//!   its own flush instead deletes the inlined rows. DuckDB's own
//!   `ducklake_flush_inlined_data` would flush floating rows a second time, so
//!   a lake pinkdb flushed is flushed by pinkdb (a lake DuckDB flushed reads
//!   the same either way — the rows are gone from the inlined table).
//! - `ducklake_set_option` stores any option name in `ducklake_metadata`
//!   with DuckDB's scope rows (global, `schema :=`, `table_name :=`) and
//!   `ducklake_options` reports them. `data_inlining_row_limit` is also
//!   applied to the handle (global) or the table's manifest (table scope);
//!   `expire_older_than` and `delete_older_than` become the defaults of the
//!   expire/cleanup functions. The Parquet writer settings are stored and
//!   reported but not applied: pinkdb's writer takes no such options.
//! - `ducklake_settings.catalog_type` and `extension_version` name the
//!   metadata backend and the version of *this* build (`pinkdb` for pinkdb's
//!   own `.pink` catalog), where DuckDB names itself.
//! - `ducklake_expire_snapshots(force := true)` and
//!   `ducklake_merge_adjacent_files(newer_than := …)` are refused: pinkdb keeps
//!   neither snapshot protection nor per-file age, so accepting them would
//!   silently drop a guarantee the caller asked for.
//! - `ducklake_set_option` validates DuckLake's two interval options and
//!   `skip_stats_columns`; every other name is stored verbatim.
//! - `ducklake_add_data_files` takes a path under the lake's data directory
//!   (DuckDB also expands globs) and refuses `allow_missing`,
//!   `ignore_extra_columns` and `hive_partitioning := true`: pinkdb copies no
//!   file it cannot read and maps no partition columns from a path.

use std::collections::BTreeMap;

use pinkdb::{DataTable, DataType, Value};

use crate::metadata::snapshot_id;
use crate::schema::TableSchema;
use crate::snapshot::SnapshotInfo;
use crate::time::parse_iso8601_utc;
use crate::{
    CleanupSettings, ExpireSettings, Lake, LakeError, LakeFileInfo, MergeSettings, Result,
    RewriteSettings,
};

/// One DuckLake function argument, in the shape the engine evaluates it to.
#[derive(Debug, Clone, PartialEq)]
pub enum FnArg {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    Text(String),
    /// Microseconds since the epoch, UTC (DuckDB's `TIMESTAMPTZ`).
    Timestamp(i64),
    TextList(Vec<String>),
    IntList(Vec<i64>),
}

/// One call of a DuckLake function: its name, positional arguments in order,
/// and named arguments as `(name, value)`. Argument names are matched
/// case-insensitively, like DuckDB's.
#[derive(Debug, Clone, PartialEq)]
pub struct DuckLakeCall {
    pub name: String,
    pub positional: Vec<FnArg>,
    pub named: Vec<(String, FnArg)>,
}

impl DuckLakeCall {
    /// A call with positional arguments only.
    pub fn new(name: impl Into<String>, positional: Vec<FnArg>) -> Self {
        Self {
            name: name.into(),
            positional,
            named: Vec::new(),
        }
    }
}

/// The output schema of `call` without running it (`None` when its name is not
/// a DuckLake function).
///
/// The lake is needed for the table functions that return a table's own
/// columns (`ducklake_table_changes` and its two siblings): which columns those
/// are is a property of the lake, not of the call.
pub fn output_schema(lake: &Lake, call: &DuckLakeCall) -> Result<Option<Vec<(String, DataType)>>> {
    let Some(bound) = bind(call)? else {
        return Ok(None);
    };
    Ok(Some(schema(lake, &bound)?))
}

/// Whether `name` writes the lake, i.e. it is `CALL`-only and needs an
/// exclusive handle.
pub fn is_mutating(name: &str) -> bool {
    spec(name).is_some_and(|spec| spec.write)
}

/// Whether `name` is one of DuckLake's functions.
pub fn is_known(name: &str) -> bool {
    spec(name).is_some()
}

/// Run a read function.
pub fn call_read(lake: &Lake, call: &DuckLakeCall) -> Result<DataTable> {
    let bound = bind(call)?.ok_or_else(|| unknown_function(&call.name))?;
    if bound.spec.write {
        return Err(invalid(
            bound.spec,
            "writes the lake and must be called as CALL",
        ));
    }
    run_read(lake, &bound)
}

/// Run a write (`CALL`) function.
pub fn call_write(lake: &mut Lake, call: &DuckLakeCall) -> Result<DataTable> {
    let bound = bind(call)?.ok_or_else(|| unknown_function(&call.name))?;
    if !bound.spec.write {
        return Err(invalid(
            bound.spec,
            "only reads the lake and has no CALL form",
        ));
    }
    run_write(lake, &bound)
}

// ============================ signatures ===================================

/// One function's shape: how many positional arguments it needs, how many it
/// takes at most, which named arguments it knows, and its signature text (used
/// verbatim in every error this module raises).
struct Spec {
    name: &'static str,
    write: bool,
    positional: usize,
    max_positional: usize,
    named: &'static [&'static str],
    signature: &'static str,
}

const SPECS: &[Spec] = &[
    Spec {
        name: "ducklake_snapshots",
        write: false,
        positional: 1,
        max_positional: 1,
        named: &[],
        signature: "ducklake_snapshots(catalog)",
    },
    Spec {
        name: "ducklake_current_snapshot",
        write: false,
        positional: 1,
        max_positional: 1,
        named: &[],
        signature: "ducklake_current_snapshot(catalog)",
    },
    Spec {
        name: "ducklake_last_committed_snapshot",
        write: false,
        positional: 1,
        max_positional: 1,
        named: &[],
        signature: "ducklake_last_committed_snapshot(catalog)",
    },
    Spec {
        name: "ducklake_table_info",
        write: false,
        positional: 1,
        max_positional: 1,
        named: &[],
        signature: "ducklake_table_info(catalog)",
    },
    Spec {
        name: "ducklake_list_files",
        write: false,
        positional: 2,
        max_positional: 2,
        named: &["schema", "snapshot_version", "snapshot_time"],
        signature: "ducklake_list_files(catalog, table, schema := VARCHAR, \
                    snapshot_version := BIGINT, snapshot_time := TIMESTAMPTZ)",
    },
    Spec {
        name: "ducklake_table_changes",
        write: false,
        positional: 5,
        max_positional: 5,
        named: &[],
        signature: "ducklake_table_changes(catalog, schema, table, start_snapshot, end_snapshot)",
    },
    Spec {
        name: "ducklake_table_insertions",
        write: false,
        positional: 5,
        max_positional: 5,
        named: &[],
        signature: "ducklake_table_insertions(catalog, schema, table, start_snapshot, end_snapshot)",
    },
    Spec {
        name: "ducklake_table_deletions",
        write: false,
        positional: 5,
        max_positional: 5,
        named: &[],
        signature: "ducklake_table_deletions(catalog, schema, table, start_snapshot, end_snapshot)",
    },
    Spec {
        name: "ducklake_options",
        write: false,
        positional: 1,
        max_positional: 1,
        named: &[],
        signature: "ducklake_options(catalog)",
    },
    Spec {
        name: "ducklake_settings",
        write: false,
        positional: 1,
        max_positional: 1,
        named: &[],
        signature: "ducklake_settings(catalog)",
    },
    Spec {
        name: "ducklake_flush_inlined_data",
        write: true,
        positional: 1,
        max_positional: 1,
        named: &["schema_name", "table_name"],
        signature: "ducklake_flush_inlined_data(catalog, schema_name := VARCHAR, \
                    table_name := VARCHAR)",
    },
    Spec {
        name: "ducklake_expire_snapshots",
        write: true,
        positional: 1,
        max_positional: 1,
        named: &["older_than", "versions", "dry_run", "force"],
        signature: "ducklake_expire_snapshots(catalog, older_than := TIMESTAMPTZ, \
                    versions := BIGINT[], dry_run := BOOLEAN, force := BOOLEAN)",
    },
    Spec {
        name: "ducklake_merge_adjacent_files",
        write: true,
        positional: 1,
        max_positional: 2,
        named: &[
            "schema",
            "min_file_size",
            "max_file_size",
            "max_compacted_files",
            "newer_than",
        ],
        signature: "ducklake_merge_adjacent_files(catalog, table, schema := VARCHAR, \
                    min_file_size := BIGINT, max_file_size := BIGINT, \
                    max_compacted_files := BIGINT, newer_than := TIMESTAMPTZ)",
    },
    Spec {
        name: "ducklake_rewrite_data_files",
        write: true,
        positional: 1,
        max_positional: 2,
        named: &["schema", "delete_threshold", "max_compacted_files"],
        signature: "ducklake_rewrite_data_files(catalog, table, schema := VARCHAR, \
                    delete_threshold := DOUBLE, max_compacted_files := BIGINT)",
    },
    Spec {
        name: "ducklake_cleanup_old_files",
        write: true,
        positional: 1,
        max_positional: 1,
        named: &["older_than", "cleanup_all", "dry_run"],
        signature: "ducklake_cleanup_old_files(catalog, older_than := TIMESTAMPTZ, \
                    cleanup_all := BOOLEAN, dry_run := BOOLEAN)",
    },
    Spec {
        name: "ducklake_delete_orphaned_files",
        write: true,
        positional: 1,
        max_positional: 1,
        named: &["older_than", "cleanup_all", "dry_run"],
        signature: "ducklake_delete_orphaned_files(catalog, older_than := TIMESTAMPTZ, \
                    cleanup_all := BOOLEAN, dry_run := BOOLEAN)",
    },
    Spec {
        name: "ducklake_set_option",
        write: true,
        positional: 3,
        max_positional: 3,
        named: &["table_name", "schema"],
        signature: "ducklake_set_option(catalog, option, value, table_name := VARCHAR, \
                    schema := VARCHAR)",
    },
    Spec {
        name: "ducklake_set_commit_message",
        write: true,
        positional: 3,
        max_positional: 3,
        named: &["extra_info"],
        signature: "ducklake_set_commit_message(catalog, author, commit_message, \
                    extra_info := VARCHAR)",
    },
    Spec {
        name: "ducklake_add_data_files",
        write: true,
        positional: 3,
        max_positional: 3,
        named: &[
            "schema",
            "allow_missing",
            "ignore_extra_columns",
            "hive_partitioning",
        ],
        signature: "ducklake_add_data_files(catalog, table, path, schema := VARCHAR, \
                    allow_missing := BOOLEAN, ignore_extra_columns := BOOLEAN, \
                    hive_partitioning := BOOLEAN)",
    },
];

fn spec(name: &str) -> Option<&'static Spec> {
    SPECS
        .iter()
        .find(|spec| spec.name.eq_ignore_ascii_case(name))
}

/// A call whose positional and named arguments already match its [`Spec`].
struct Bound<'a> {
    spec: &'static Spec,
    positional: Vec<&'a FnArg>,
    named: Vec<(&'a str, &'a FnArg)>,
}

impl Bound<'_> {
    fn positional(&self, index: usize) -> &FnArg {
        self.positional[index]
    }

    /// The second positional argument, where a `CALL` function takes its
    /// table.
    fn positional_table(&self) -> Option<&FnArg> {
        self.positional.get(1).copied()
    }

    fn named(&self, name: &str) -> Option<&FnArg> {
        self.named
            .iter()
            .find(|(arg, _)| arg.eq_ignore_ascii_case(name))
            .map(|(_, value)| *value)
    }
}

fn bind(call: &DuckLakeCall) -> Result<Option<Bound<'_>>> {
    let Some(spec) = spec(&call.name) else {
        return Ok(None);
    };
    if call.positional.len() < spec.positional {
        return Err(invalid(
            spec,
            &format!(
                "needs {} positional argument(s), got {}",
                spec.positional,
                call.positional.len()
            ),
        ));
    }
    if call.positional.len() > spec.max_positional {
        return Err(invalid(
            spec,
            &format!(
                "takes at most {} positional argument(s), got {}",
                spec.max_positional,
                call.positional.len()
            ),
        ));
    }
    for (name, _) in &call.named {
        if !spec
            .named
            .iter()
            .any(|known| known.eq_ignore_ascii_case(name))
        {
            return Err(invalid(spec, &format!("unknown named argument \"{name}\"")));
        }
        if call
            .named
            .iter()
            .filter(|(other, _)| other.eq_ignore_ascii_case(name))
            .count()
            > 1
        {
            return Err(invalid(
                spec,
                &format!("named argument \"{name}\" given twice"),
            ));
        }
    }
    // The catalog argument names the lake the engine already resolved; any
    // text is accepted here.
    as_text(spec, &call.positional[0], "argument 1 (catalog)")?;
    Ok(Some(Bound {
        spec,
        positional: call.positional.iter().collect(),
        named: call.named.iter().map(|(n, v)| (n.as_str(), v)).collect(),
    }))
}

// ============================== schemas ====================================

/// The DuckDB output schema of `bound` (see [`output_schema`]).
fn schema(lake: &Lake, bound: &Bound<'_>) -> Result<Vec<(String, DataType)>> {
    Ok(match bound.spec.name {
        "ducklake_snapshots" => columns(&[
            ("snapshot_id", DataType::Bigint),
            ("snapshot_time", DataType::Timestamp),
            ("schema_version", DataType::Bigint),
            ("changes", DataType::Varchar),
            ("author", DataType::Varchar),
            ("commit_message", DataType::Varchar),
            ("commit_extra_info", DataType::Varchar),
        ]),
        "ducklake_current_snapshot" | "ducklake_last_committed_snapshot" => {
            columns(&[("id", DataType::Bigint)])
        }
        "ducklake_table_info" => columns(&[
            ("table_name", DataType::Varchar),
            ("schema_id", DataType::Bigint),
            ("table_id", DataType::Bigint),
            ("table_uuid", DataType::Varchar),
            ("file_count", DataType::Bigint),
            ("file_size_bytes", DataType::Bigint),
            ("delete_file_count", DataType::Bigint),
            ("delete_file_size_bytes", DataType::Bigint),
        ]),
        "ducklake_list_files" => columns(&[
            ("data_file", DataType::Varchar),
            ("data_file_size_bytes", DataType::Bigint),
            ("data_file_footer_size", DataType::Bigint),
            ("data_file_encryption_key", DataType::Varchar),
            ("delete_file", DataType::Varchar),
            ("delete_file_size_bytes", DataType::Bigint),
            ("delete_file_footer_size", DataType::Bigint),
            ("delete_file_encryption_key", DataType::Varchar),
        ]),
        "ducklake_options" => columns(&[
            ("option_name", DataType::Varchar),
            ("description", DataType::Varchar),
            ("value", DataType::Varchar),
            ("scope", DataType::Varchar),
            ("scope_entry", DataType::Varchar),
        ]),
        "ducklake_settings" => columns(&[
            ("catalog_type", DataType::Varchar),
            ("extension_version", DataType::Varchar),
            ("data_path", DataType::Varchar),
        ]),
        "ducklake_flush_inlined_data" => columns(&[
            ("schema_name", DataType::Varchar),
            ("table_name", DataType::Varchar),
            ("rows_flushed", DataType::Bigint),
        ]),
        "ducklake_expire_snapshots" => snapshot_columns(),
        "ducklake_merge_adjacent_files" | "ducklake_rewrite_data_files" => columns(&[
            ("schema_name", DataType::Varchar),
            ("table_name", DataType::Varchar),
            ("files_processed", DataType::Bigint),
            ("files_created", DataType::Bigint),
        ]),
        "ducklake_cleanup_old_files" | "ducklake_delete_orphaned_files" => {
            columns(&[("path", DataType::Varchar)])
        }
        "ducklake_set_option" | "ducklake_set_commit_message" => {
            columns(&[("Success", DataType::Boolean)])
        }
        "ducklake_add_data_files" => columns(&[("filename", DataType::Varchar)]),
        _ => change_columns(lake, bound)?,
    })
}

fn snapshot_columns() -> Vec<(String, DataType)> {
    columns(&[
        ("snapshot_id", DataType::Bigint),
        ("snapshot_time", DataType::Timestamp),
        ("schema_version", DataType::Bigint),
        ("changes", DataType::Varchar),
        ("author", DataType::Varchar),
        ("commit_message", DataType::Varchar),
        ("commit_extra_info", DataType::Varchar),
    ])
}

/// `ducklake_table_changes` and its two siblings: the table's own columns at
/// the call's read snapshot, prefixed by the change columns for
/// `ducklake_table_changes`.
fn change_columns(lake: &Lake, bound: &Bound<'_>) -> Result<Vec<(String, DataType)>> {
    let with_change = bound.spec.name == "ducklake_table_changes";
    let mut out = Vec::new();
    if with_change {
        out.extend(columns(&[
            ("snapshot_id", DataType::Bigint),
            ("rowid", DataType::Bigint),
            ("change_type", DataType::Varchar),
        ]));
    }
    out.extend(read_snapshot(lake, bound)?.schema);
    Ok(out)
}

fn columns(spec: &[(&str, DataType)]) -> Vec<(String, DataType)> {
    spec.iter()
        .map(|(name, dtype)| ((*name).to_string(), *dtype))
        .collect()
}

// =============================== readers ===================================

fn run_read(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    match bound.spec.name {
        "ducklake_snapshots" => snapshots_table(lake, bound),
        "ducklake_current_snapshot" => {
            let mut table = table_for(lake, bound)?;
            push(
                &mut table,
                vec![Value::Bigint(snapshot_id(lake.current_version())?)],
            )?;
            Ok(table)
        }
        "ducklake_last_committed_snapshot" => {
            let mut table = table_for(lake, bound)?;
            push(&mut table, vec![Value::Null])?;
            Ok(table)
        }
        "ducklake_table_info" => table_info(lake, bound),
        "ducklake_list_files" => list_files(lake, bound),
        "ducklake_options" => options(lake, bound),
        "ducklake_settings" => settings(lake, bound),
        _ => table_changes(lake, bound),
    }
}

fn table_for(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let schema = schema(lake, bound)?;
    DataTable::new(bound.spec.name.to_string(), schema).map_err(|error| {
        LakeError::NotRepresentable {
            message: format!("building {}'s result: {error}", bound.spec.name),
        }
    })
}

fn snapshots_table(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut table = table_for(lake, bound)?;
    for info in lake.snapshots()? {
        push(&mut table, snapshot_values(lake, &info)?)?;
    }
    Ok(table)
}

/// One `ducklake_snapshots` row: the version's commit facts and its change map,
/// both read from the snapshot's own `ducklake_snapshot_changes` row.
fn snapshot_values(lake: &Lake, info: &SnapshotInfo) -> Result<Vec<Value>> {
    let changes = lake.store.snapshot_changes(info.version)?;
    let text = |value: Option<String>| value.map_or(Value::Null, Value::Varchar);
    // The catalog's own text is the only time a catalog written by another
    // tool has; a manifest's `updated` is the fallback.
    let time = changes
        .as_ref()
        .and_then(|changes| changes.snapshot_time.as_deref())
        .and_then(parse_lake_time)
        .or_else(|| snapshot_micros(info));
    Ok(vec![
        Value::Bigint(snapshot_id(info.version)?),
        time.map_or(Value::Null, Value::Timestamp),
        Value::Bigint(i64::try_from(info.schema_version).unwrap_or(i64::MAX)),
        Value::Varchar(changes_text(
            changes
                .as_ref()
                .and_then(|changes| changes.changes_made.as_deref()),
        )),
        text(changes.as_ref().and_then(|changes| changes.author.clone())),
        text(
            changes
                .as_ref()
                .and_then(|changes| changes.commit_message.clone()),
        ),
        text(
            changes
                .as_ref()
                .and_then(|changes| changes.commit_extra_info.clone()),
        ),
    ])
}

fn table_info(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut table = table_for(lake, bound)?;
    let identities: BTreeMap<String, (i64, i64, Option<String>)> = lake
        .store
        .catalog_table_ids()?
        .into_iter()
        .map(|identity| {
            (
                identity.key,
                (identity.schema_id, identity.table_id, identity.uuid),
            )
        })
        .collect();
    // `Lake::table_info` counts every delete reference, inlined ones included;
    // DuckDB counts the delete files the catalog holds.
    let deletes: BTreeMap<i64, (u64, Option<u64>)> = lake
        .store
        .table_delete_files()?
        .into_iter()
        .map(|(table_id, count, bytes)| (table_id, (count, bytes)))
        .collect();
    for info in lake.table_info()? {
        let ids = identities.get(&info.table);
        let counts = ids.and_then(|(_, table_id, _)| deletes.get(table_id));
        let name = info
            .table
            .split_once('.')
            .map_or(info.table.as_str(), |(_, name)| name);
        push(
            &mut table,
            vec![
                Value::Varchar(name.to_string()),
                ids.map_or(Value::Null, |(schema_id, _, _)| Value::Bigint(*schema_id)),
                ids.map_or(Value::Null, |(_, table_id, _)| Value::Bigint(*table_id)),
                ids.and_then(|(_, _, uuid)| uuid.as_deref())
                    .map_or(Value::Null, |uuid| {
                        Value::Varchar(uuid.to_ascii_lowercase())
                    }),
                unsigned(Some(info.file_count)),
                unsigned(info.file_size_bytes),
                unsigned(Some(counts.map_or(0, |(count, _)| *count))),
                // DuckDB reports a size whenever it reports a count.
                unsigned(Some(counts.and_then(|(_, bytes)| *bytes).unwrap_or(0))),
            ],
        )?;
    }
    Ok(table)
}

fn list_files(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let table = as_text(bound.spec, bound.positional(1), "argument 2 (table)")?;
    let schema = bound
        .named("schema")
        .map(|arg| as_text(bound.spec, arg, "schema"))
        .transpose()?;
    let version = listed_version(lake, bound)?;
    let key = table_key(lake, bound.spec, schema.as_deref(), &table)?;
    let mut out = table_for(lake, bound)?;
    // A DuckDB-written catalog keeps each Parquet footer's size in
    // `ducklake_data_file`; pinkdb's manifest keeps its own ranged-read
    // offsets instead.
    let footers: BTreeMap<String, u64> = lake.store.data_file_footers()?.into_iter().collect();
    for info in lake.list_files(Some(&key), Some(version), false)? {
        let name = info.path.rsplit('/').next().unwrap_or(&info.path);
        let delete_file = delete_file_of(&info);
        let delete_size = match delete_file {
            Some(path) => lake.storage.stat(path)?.map(|stat| stat.size),
            None => None,
        };
        push(
            &mut out,
            vec![
                Value::Varchar(lake.storage.abs(&info.path).display().to_string()),
                unsigned(info.size_bytes),
                unsigned(footers.get(name).copied().or(info.footer_len)),
                Value::Null,
                delete_file.map_or(Value::Null, |path| {
                    Value::Varchar(lake.storage.abs(path).display().to_string())
                }),
                unsigned(delete_size),
                Value::Null,
                Value::Null,
            ],
        )?;
    }
    Ok(out)
}

/// The pinkdb version `ducklake_list_files` lists: the named snapshot (by id or
/// by time) or the head.
fn listed_version(lake: &Lake, bound: &Bound<'_>) -> Result<u64> {
    let by_time = bound.named("snapshot_time");
    let by_version = bound.named("snapshot_version");
    if by_time.is_some() && by_version.is_some() {
        return Err(invalid(
            bound.spec,
            "takes either snapshot_version or snapshot_time, not both",
        ));
    }
    if let Some(arg) = by_version {
        let id = as_int(bound.spec, arg, "snapshot_version")?;
        return check_snapshot(lake, bound.spec, id);
    }
    if let Some(arg) = by_time {
        let time = as_timestamp(bound.spec, arg, "snapshot_time")?;
        return snapshot_at(lake, bound.spec, time);
    }
    Ok(lake.current_version())
}

fn table_changes(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let read = read_snapshot(lake, bound)?;
    let rows = lake.table_changes(&read.key, read.start, Some(read.end))?;
    let with_change = bound.spec.name == "ducklake_table_changes";
    let wanted = if bound.spec.name == "ducklake_table_insertions" {
        Some(true)
    } else if bound.spec.name == "ducklake_table_deletions" {
        Some(false)
    } else {
        None
    };
    let mut out = table_for(lake, bound)?;
    for row in rows {
        if wanted.is_some_and(|insertions| insertions != row.change_type.is_insertion()) {
            continue;
        }
        let mut values = Vec::new();
        if with_change {
            values.push(Value::Bigint(snapshot_id(row.version)?));
            values.push(unsigned(row.row_id));
            values.push(Value::Varchar(row.change_type.as_str().to_string()));
        }
        values.extend(row.values);
        push(&mut out, values)?;
    }
    Ok(out)
}

/// A resolved change-range call: which table, which versions, and that table's
/// columns at the range's end.
struct ReadSnapshot {
    key: String,
    start: u64,
    end: u64,
    schema: Vec<(String, DataType)>,
}

fn read_snapshot(lake: &Lake, bound: &Bound<'_>) -> Result<ReadSnapshot> {
    let schema = as_text(bound.spec, bound.positional(1), "argument 2 (schema)")?;
    let table = as_text(bound.spec, bound.positional(2), "argument 3 (table)")?;
    let start = as_snapshot(lake, bound.spec, bound.positional(3), "start_snapshot")?;
    let end = as_snapshot(lake, bound.spec, bound.positional(4), "end_snapshot")?;
    // DuckDB reports a bad bound before it resolves the table, so check them
    // first.
    let start = check_snapshot(lake, bound.spec, start)?;
    let end = check_snapshot(lake, bound.spec, end)?;
    let key = table_key(lake, bound.spec, Some(&schema), &table)?;
    let (manifest, path) = lake.manifest(Some(end))?;
    let entry = manifest.tables.get(&key).ok_or_else(|| {
        invalid(
            bound.spec,
            &format!("table \"{table}\" does not exist in snapshot {}", end - 1),
        )
    })?;
    let schema = TableSchema::from_entry(&key, entry, &path)?.ok_or_else(|| {
        LakeError::NotRepresentable {
            message: format!("table \"{key}\" has no column record"),
        }
    })?;
    Ok(ReadSnapshot {
        key,
        start,
        end,
        schema: schema.schema_pairs(),
    })
}

fn options(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    // DuckLake stores schema- and table-scoped options in the same table, with
    // the object's id; DuckDB names the object in `scope_entry`.
    let tables = lake.store.catalog_table_ids()?;
    let by_table: BTreeMap<i64, &str> = tables
        .iter()
        .map(|identity| (identity.table_id, identity.key.as_str()))
        .collect();
    let schemas = lake.store.catalog_schema_ids()?;
    let by_schema: BTreeMap<i64, &str> = schemas
        .iter()
        .map(|(schema, id)| (*id, schema.as_str()))
        .collect();
    for row in lake.store.metadata_options()? {
        let (scope, entry) = match row.scope.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("schema") => (
                "SCHEMA",
                row.scope_id
                    .and_then(|id| by_schema.get(&id))
                    .map(|schema| schema.to_string()),
            ),
            Some("table") => (
                "TABLE",
                row.scope_id
                    .and_then(|id| by_table.get(&id))
                    .map(|key| key.to_string()),
            ),
            _ => ("GLOBAL", None),
        };
        push(
            &mut out,
            vec![
                Value::Varchar(row.key.clone()),
                option_description(&row.key).map_or(Value::Null, Value::Varchar),
                Value::Varchar(row.value),
                Value::Varchar(scope.to_string()),
                entry.map_or(Value::Null, Value::Varchar),
            ],
        )?;
    }
    Ok(out)
}

fn settings(lake: &Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let data_path = lake.store.metadata_options()?.into_iter().find_map(|row| {
        (row.scope.is_none() && row.key.eq_ignore_ascii_case("data_path")).then_some(row.value)
    });
    push(
        &mut out,
        vec![
            Value::Varchar(lake.store.catalog_type().to_string()),
            Value::Varchar(env!("CARGO_PKG_VERSION").to_string()),
            Value::Varchar(
                data_path.unwrap_or_else(|| lake.storage.abs("data").display().to_string()),
            ),
        ],
    )?;
    Ok(out)
}

// =============================== writers ===================================

fn run_write(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    match bound.spec.name {
        "ducklake_flush_inlined_data" => flush_inlined_data(lake, bound),
        "ducklake_expire_snapshots" => expire_snapshots(lake, bound),
        "ducklake_merge_adjacent_files" => merge_adjacent_files(lake, bound),
        "ducklake_rewrite_data_files" => rewrite_data_files(lake, bound),
        "ducklake_cleanup_old_files" | "ducklake_delete_orphaned_files" => {
            cleanup_files(lake, bound)
        }
        "ducklake_set_option" => set_option(lake, bound),
        "ducklake_set_commit_message" => set_commit_message(lake, bound),
        _ => add_data_files(lake, bound),
    }
}

fn flush_inlined_data(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    // `schema_name` only narrows which table `table_name` names; none of
    // pinkdb's own lakes has a second schema, so a call that names no table
    // flushes the lake as DuckDB's does when no table matches.
    let schema = bound
        .named("schema_name")
        .map(|arg| as_text(bound.spec, arg, "schema_name"))
        .transpose()?;
    let table = match bound.named("table_name") {
        Some(arg) => {
            let table = as_text(bound.spec, arg, "table_name")?;
            Some(if table.contains('.') {
                table
            } else {
                table_key(lake, bound.spec, schema.as_deref(), &table)?
            })
        }
        None => None,
    };
    for (key, rows) in &lake.flush_inlined_data(table.as_deref())?.tables {
        let (schema_name, table_name) = split_key(key);
        push(
            &mut out,
            vec![
                Value::Varchar(schema_name),
                Value::Varchar(table_name),
                unsigned(Some(*rows)),
            ],
        )?;
    }
    Ok(out)
}

fn expire_snapshots(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let older_than = bound
        .named("older_than")
        .map(|arg| as_timestamp(bound.spec, arg, "older_than"))
        .transpose()?;
    let versions = bound
        .named("versions")
        .map(|arg| as_int_list(bound.spec, arg, "versions"))
        .transpose()?
        .map(|ids| {
            ids.into_iter()
                .map(|id| check_snapshot(lake, bound.spec, id))
                .collect::<Result<Vec<u64>>>()
        })
        .transpose()?;
    let dry_run = bound
        .named("dry_run")
        .map(|arg| as_bool(bound.spec, arg, "dry_run"))
        .transpose()?
        .unwrap_or(false);
    if bound.named("force") == Some(&FnArg::Bool(true)) {
        return Err(invalid(
            bound.spec,
            "force := true is not supported (pinkdb keeps no snapshot protection)",
        ));
    }
    if older_than.is_some() && versions.is_some() {
        return Err(invalid(
            bound.spec,
            "takes either older_than or versions, not both",
        ));
    }
    let older_than = match (older_than, versions.is_some()) {
        (None, false) => option_cutoff(lake, bound.spec, "expire_older_than")?,
        (older_than, _) => older_than,
    };
    let settings = |dry_run| ExpireSettings {
        versions: versions.clone(),
        older_than_micros: older_than,
        retain_last: None,
        dry_run,
    };
    // The victims' own change rows only exist until the real run deletes them,
    // so the preview is what reports them.
    let preview = lake.expire_snapshots(&settings(true))?;
    for info in &preview.versions {
        push(&mut out, snapshot_values(lake, info)?)?;
    }
    if !dry_run {
        lake.expire_snapshots(&settings(false))?;
    }
    Ok(out)
}

fn merge_adjacent_files(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    if bound.named("newer_than").is_some() {
        return Err(invalid(
            bound.spec,
            "newer_than is not supported (pinkdb records no per-file age)",
        ));
    }
    let table = call_table(lake, bound, "")?;
    let settings = MergeSettings {
        min_file_size: optional_unsigned(bound, "min_file_size"),
        max_file_size: optional_unsigned(bound, "max_file_size"),
        max_compacted_files: optional_unsigned(bound, "max_compacted_files"),
    };
    for entry in lake
        .merge_adjacent_files(table.as_deref(), &settings)?
        .tables
    {
        append_compaction(&mut out, &entry)?;
    }
    Ok(out)
}

fn rewrite_data_files(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let table = call_table(lake, bound, "")?;
    let mut settings = RewriteSettings::default();
    if let Some(arg) = bound.named("delete_threshold") {
        settings.delete_threshold = as_double(bound.spec, arg, "delete_threshold")?;
    }
    settings.max_compacted_files = optional_unsigned(bound, "max_compacted_files");
    for entry in lake.rewrite_data_files(table.as_deref(), &settings)?.tables {
        append_compaction(&mut out, &entry)?;
    }
    Ok(out)
}

fn append_compaction(out: &mut DataTable, entry: &crate::TableCompaction) -> Result<()> {
    let (schema_name, table_name) = split_key(&entry.table);
    push(
        out,
        vec![
            Value::Varchar(schema_name),
            Value::Varchar(table_name),
            unsigned(Some(entry.files_processed)),
            unsigned(Some(entry.files_created)),
        ],
    )
}

fn cleanup_files(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let older_than = bound
        .named("older_than")
        .map(|arg| as_timestamp(bound.spec, arg, "older_than"))
        .transpose()?;
    let all = bound
        .named("cleanup_all")
        .map(|arg| as_bool(bound.spec, arg, "cleanup_all"))
        .transpose()?
        .unwrap_or(false);
    let dry_run = bound
        .named("dry_run")
        .map(|arg| as_bool(bound.spec, arg, "dry_run"))
        .transpose()?
        .unwrap_or(false);
    let older_than = if older_than.is_none() && !all {
        option_cutoff(lake, bound.spec, "delete_older_than")?
    } else {
        older_than
    };
    if !dry_run && older_than.is_none() && !all {
        return Err(invalid(
            bound.spec,
            "either cleanup_all OR older_than must be specified. You can also set a default \
             value for file deletion via e.g., CALL ducklake_set_option('delete_older_than', \
             '1 week')",
        ));
    }
    let settings = CleanupSettings {
        older_than_micros: older_than,
        all,
        dry_run,
        // A non-dry run is the caller's confirmation: this layer only runs on
        // an exclusive handle, which is DuckDB's CALL requirement too.
        confirm: true,
    };
    let report = if bound.spec.name == "ducklake_cleanup_old_files" {
        lake.cleanup_old_files(&settings)?
    } else {
        lake.delete_orphaned_files(&settings)?
    };
    for path in &report.paths {
        push(
            &mut out,
            vec![Value::Varchar(lake.storage.abs(path).display().to_string())],
        )?;
    }
    Ok(out)
}

fn set_option(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let option = as_text(bound.spec, bound.positional(1), "argument 2 (option)")?;
    let value = option_text(bound.spec, bound.positional(2), "argument 3 (value)")?;
    let schema = bound
        .named("schema")
        .map(|arg| as_text(bound.spec, arg, "schema"))
        .transpose()?;
    let table = match bound.named("table_name") {
        Some(arg) => {
            let table = as_text(bound.spec, arg, "table_name")?;
            if table.contains('.') {
                Some(table)
            } else {
                Some(table_key(lake, bound.spec, schema.as_deref(), &table)?)
            }
        }
        None => None,
    };
    let tables = lake.store.catalog_table_ids()?;
    let (scope, scope_id) = if let Some(key) = &table {
        let id = tables
            .iter()
            .find(|identity| identity.key.eq_ignore_ascii_case(key))
            .map(|identity| identity.table_id)
            .ok_or_else(|| invalid(bound.spec, &format!("table with name {key} does not exist")))?;
        (Some("table"), Some(id))
    } else if let Some(schema) = &schema {
        let schemas = lake.store.catalog_schema_ids()?;
        let id = schemas
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(schema))
            .map(|(_, id)| *id)
            .ok_or_else(|| {
                invalid(
                    bound.spec,
                    &format!("schema with name {schema} does not exist"),
                )
            })?;
        (Some("schema"), Some(id))
    } else {
        (None, None)
    };
    let lower = option.to_ascii_lowercase();
    let mut limit = None;
    match lower.as_str() {
        "expire_older_than" | "delete_older_than" => {
            if pinkdb::types::parse_interval(&value).is_none() {
                return Err(invalid(
                    bound.spec,
                    &format!("{option} is not a valid interval value."),
                ));
            }
        }
        "skip_stats_columns" => {
            validate_skip_stats_columns(lake, bound, table.as_deref(), &value)?;
        }
        "data_inlining_row_limit" => {
            limit = match value.trim().to_ascii_lowercase().as_str() {
                "" | "none" | "null" => None,
                digits => Some(digits.parse::<u64>().map_err(|_| {
                    invalid(
                        bound.spec,
                        &format!("data_inlining_row_limit must be a whole number, got \"{value}\""),
                    )
                })?),
            };
        }
        _ => {}
    }
    // DuckDB normalizes the options it interprets; store the same text so
    // `ducklake_options` agrees with it after the same calls.
    let stored = match lower.as_str() {
        "parquet_compression" | "data_file_format" => value.to_ascii_lowercase(),
        "parquet_version" => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "v1" => "V1".to_string(),
            "2" | "v2" => "V2".to_string(),
            _ => value.clone(),
        },
        "data_inlining_row_limit" => limit
            .map(|limit| limit.to_string())
            .unwrap_or_else(|| "none".to_string()),
        _ => value.clone(),
    };
    lake.store.set_option(&lower, &stored, scope, scope_id)?;
    if lower == "data_inlining_row_limit" {
        if let Some(key) = &table {
            // DuckLake stores this option without publishing a snapshot.
            lake.set_table_inlining_limit(key, limit);
        } else if let Some(limit) = limit {
            lake.set_data_inlining_row_limit(limit);
        }
    }
    push(&mut out, vec![Value::Boolean(true)])?;
    Ok(out)
}

/// The cutoff a stored `expire_older_than` / `delete_older_than` option names.
fn option_cutoff(lake: &Lake, spec: &Spec, name: &str) -> Result<Option<i64>> {
    let Some(row) = lake
        .store
        .metadata_options()?
        .into_iter()
        .find(|row| row.scope.is_none() && row.key.eq_ignore_ascii_case(name))
    else {
        return Ok(None);
    };
    let interval = pinkdb::types::parse_interval(&row.value)
        .ok_or_else(|| invalid(spec, &format!("{name} is not a valid interval value.")))?;
    let ago = pinkdb::types::Interval {
        months: -interval.months,
        days: -interval.days,
        micros: -interval.micros,
    };
    Ok(pinkdb::types::timestamp_add_interval(
        crate::maintenance::now_micros(),
        ago,
    ))
}

fn validate_skip_stats_columns(
    lake: &Lake,
    bound: &Bound<'_>,
    table: Option<&str>,
    value: &str,
) -> Result<()> {
    let keys: Vec<String> = match table {
        Some(key) => vec![key.to_string()],
        None => lake.tables()?.into_iter().map(|(key, _)| key).collect(),
    };
    let mut columns = std::collections::BTreeSet::new();
    for key in keys {
        if let Some(entry) = lake.store.table_layout(&key, None)? {
            for column in entry.columns.unwrap_or_default() {
                columns.insert(column.name.to_ascii_lowercase());
            }
        }
    }
    for column in value.split(',').map(str::trim).filter(|c| !c.is_empty()) {
        if !columns.contains(&column.to_ascii_lowercase()) {
            return Err(invalid(
                bound.spec,
                &format!("column with name \"{column}\" does not exist"),
            ));
        }
    }
    Ok(())
}

fn set_commit_message(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let author = as_text(bound.spec, bound.positional(1), "argument 2 (author)")?;
    let message = as_text(
        bound.spec,
        bound.positional(2),
        "argument 3 (commit_message)",
    )?;
    let extra_info = bound
        .named("extra_info")
        .map(|arg| as_text(bound.spec, arg, "extra_info"))
        .transpose()?;
    lake.set_commit_message_with_extra(Some(author), Some(message), extra_info);
    push(&mut out, vec![Value::Boolean(true)])?;
    Ok(out)
}

fn add_data_files(lake: &mut Lake, bound: &Bound<'_>) -> Result<DataTable> {
    let mut out = table_for(lake, bound)?;
    let paths = match bound.positional(2) {
        FnArg::Text(path) => vec![path.clone()],
        FnArg::TextList(paths) => paths.clone(),
        other => {
            return Err(invalid(
                bound.spec,
                &format!(
                    "argument 3 (path) must be VARCHAR or VARCHAR[], got {}",
                    kind(other)
                ),
            ));
        }
    };
    for name in ["allow_missing", "ignore_extra_columns", "hive_partitioning"] {
        if bound.named(name) == Some(&FnArg::Bool(true)) {
            return Err(invalid(
                bound.spec,
                &format!("{name} := true is not supported"),
            ));
        }
    }
    let table = call_table(lake, bound, "")?
        .ok_or_else(|| invalid(bound.spec, "argument 2 (table) selects the table to add to"))?;
    let root = lake.storage.abs("data").display().to_string();
    let relative: Vec<String> = paths
        .iter()
        .map(|path| lake_relative(&root, path))
        .collect::<Result<Vec<String>>>()?;
    for (_, path, _) in &lake.add_data_files(&table, &relative)?.files {
        push(
            &mut out,
            vec![Value::Varchar(lake.storage.abs(path).display().to_string())],
        )?;
    }
    Ok(out)
}

// =============================== helpers ===================================

/// The pinkdb version of DuckLake snapshot `id`, checked against the lake's
/// published versions.
fn check_snapshot(lake: &Lake, spec: &Spec, id: i64) -> Result<u64> {
    u64::try_from(id)
        .ok()
        .and_then(|id| id.checked_add(1))
        .filter(|version| {
            lake.versions()
                .is_ok_and(|versions| versions.contains(version))
        })
        .ok_or_else(|| invalid(spec, &format!("No snapshot found at version {id}")))
}

/// The snapshot at or before `time`, DuckLake's `snapshot_time` rule.
fn snapshot_at(lake: &Lake, spec: &Spec, time: i64) -> Result<u64> {
    lake.snapshots()?
        .into_iter()
        .filter(|info| snapshot_micros(info).is_some_and(|at| at <= time))
        .map(|info| info.version)
        .max()
        .ok_or_else(|| invalid(spec, &format!("No snapshot found at time {time}")))
}

/// A change-range bound: DuckDB takes a snapshot id (`BIGINT`) or a time
/// (`TIMESTAMPTZ`).
fn as_snapshot(lake: &Lake, spec: &Spec, arg: &FnArg, label: &str) -> Result<i64> {
    match arg {
        FnArg::Int(id) => Ok(*id),
        FnArg::Timestamp(time) => snapshot_id(snapshot_at(lake, spec, *time)?),
        other => Err(invalid(
            spec,
            &format!("{label} must be BIGINT or TIMESTAMPTZ, got {}", kind(other)),
        )),
    }
}

/// The flat `db.table` key `table` names in `schema` (DuckLake defaults to the
/// `main` schema).
fn table_key(lake: &Lake, spec: &Spec, schema: Option<&str>, table: &str) -> Result<String> {
    let matches = |key: &str| match key.split_once('.') {
        Some((db, name)) => {
            name.eq_ignore_ascii_case(table)
                && schema.map_or(db.eq_ignore_ascii_case("main"), |wanted| {
                    db.eq_ignore_ascii_case(wanted)
                })
        }
        None => false,
    };
    let keys: Vec<String> = lake
        .tables()?
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| matches(key))
        .collect();
    match keys.as_slice() {
        [key] => Ok(key.clone()),
        [] => Err(invalid(
            spec,
            &format!("table with name {table} does not exist"),
        )),
        _ => Err(invalid(
            spec,
            &format!("table with name {table} is not unique: {}", keys.join(", ")),
        )),
    }
}

/// The table a `CALL` function names, if it names one: the second positional
/// argument or the `named` argument (`""` when the function has no such named
/// argument).
fn call_table(lake: &Lake, bound: &Bound<'_>, named: &str) -> Result<Option<String>> {
    let schema = bound
        .named("schema")
        .map(|arg| as_text(bound.spec, arg, "schema"))
        .transpose()?;
    let table = match bound.positional_table() {
        Some(arg) => Some(as_text(bound.spec, arg, "argument 2 (table)")?),
        None if named.is_empty() => None,
        None => match bound.named(named) {
            Some(arg) => Some(as_text(bound.spec, arg, named)?),
            None => None,
        },
    };
    match table {
        // A flat `db.table` key already names the table.
        Some(table) if table.contains('.') => Ok(Some(table)),
        Some(table) => table_key(lake, bound.spec, schema.as_deref(), &table).map(Some),
        None => Ok(None),
    }
}

/// `schema.table` → `(schema, table)`.
fn split_key(key: &str) -> (String, String) {
    match key.split_once('.') {
        Some((schema, table)) => (schema.to_string(), table.to_string()),
        None => ("main".to_string(), key.to_string()),
    }
}

/// `path` as a lake-relative `data/…` path, the form
/// [`Lake::add_data_files`] takes.
fn lake_relative(root: &str, path: &str) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .ok_or_else(|| LakeError::NotRepresentable {
            message: format!("data file \"{path}\" is not under the lake's data path ({root})"),
        })?
        .trim_start_matches('/');
    if relative.is_empty() {
        return Err(LakeError::NotRepresentable {
            message: format!("data file \"{path}\" is the data path itself, not a file"),
        });
    }
    Ok(format!("data/{relative}"))
}

/// Add one result row, naming pinkdb's error as what it is.
fn push(table: &mut DataTable, values: Vec<Value>) -> Result<()> {
    table
        .append_row(values)
        .map_err(|error| LakeError::NotRepresentable {
            message: format!("building a function's result row: {error}"),
        })
}

fn unsigned(value: Option<u64>) -> Value {
    value.map_or(Value::Null, |value| {
        Value::Bigint(i64::try_from(value).unwrap_or(i64::MAX))
    })
}

fn optional_unsigned(bound: &Bound<'_>, name: &str) -> Option<u64> {
    bound.named(name).and_then(|arg| match arg {
        FnArg::Int(value) => u64::try_from(*value).ok(),
        _ => None,
    })
}

fn invalid(spec: &Spec, message: &str) -> LakeError {
    LakeError::InvalidOption {
        message: format!("{}: {message}; expected {}", spec.name, spec.signature),
    }
}

fn unknown_function(name: &str) -> LakeError {
    LakeError::InvalidOption {
        message: format!(
            "{name} is not a DuckLake function; expected one of {}",
            SPECS
                .iter()
                .map(|spec| spec.name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn kind(arg: &FnArg) -> &'static str {
    match arg {
        FnArg::Null => "NULL",
        FnArg::Bool(_) => "BOOLEAN",
        FnArg::Int(_) => "BIGINT",
        FnArg::Double(_) => "DOUBLE",
        FnArg::Text(_) => "VARCHAR",
        FnArg::Timestamp(_) => "TIMESTAMPTZ",
        FnArg::TextList(_) => "VARCHAR[]",
        FnArg::IntList(_) => "BIGINT[]",
    }
}

fn wrong_type(spec: &Spec, arg: &FnArg, label: &str, expected: &str) -> LakeError {
    invalid(
        spec,
        &format!("{label} must be {expected}, got {}", kind(arg)),
    )
}

fn as_text(spec: &Spec, arg: &FnArg, label: &str) -> Result<String> {
    match arg {
        FnArg::Text(text) => Ok(text.clone()),
        other => Err(wrong_type(spec, other, label, "VARCHAR")),
    }
}

/// An option value as DuckDB stores it: the text form of a scalar.
fn option_text(spec: &Spec, arg: &FnArg, label: &str) -> Result<String> {
    match arg {
        FnArg::Text(text) => Ok(text.clone()),
        FnArg::Int(value) => Ok(value.to_string()),
        FnArg::Double(value) => Ok(value.to_string()),
        FnArg::Bool(value) => Ok(value.to_string()),
        other => Err(wrong_type(spec, other, label, "a scalar")),
    }
}

fn as_int(spec: &Spec, arg: &FnArg, label: &str) -> Result<i64> {
    match arg {
        FnArg::Int(value) => Ok(*value),
        other => Err(wrong_type(spec, other, label, "BIGINT")),
    }
}

fn as_bool(spec: &Spec, arg: &FnArg, label: &str) -> Result<bool> {
    match arg {
        FnArg::Bool(value) => Ok(*value),
        other => Err(wrong_type(spec, other, label, "BOOLEAN")),
    }
}

fn as_double(spec: &Spec, arg: &FnArg, label: &str) -> Result<f64> {
    match arg {
        FnArg::Double(value) => Ok(*value),
        other => Err(wrong_type(spec, other, label, "DOUBLE")),
    }
}

fn as_timestamp(spec: &Spec, arg: &FnArg, label: &str) -> Result<i64> {
    match arg {
        FnArg::Timestamp(value) => Ok(*value),
        other => Err(wrong_type(spec, other, label, "TIMESTAMPTZ")),
    }
}

fn as_int_list(spec: &Spec, arg: &FnArg, label: &str) -> Result<Vec<i64>> {
    match arg {
        FnArg::IntList(values) => Ok(values.clone()),
        other => Err(wrong_type(spec, other, label, "BIGINT[]")),
    }
}

// ============================ change text ==================================

/// DuckLake's `changes` map, rendered as DuckDB prints it
/// (`{tables_created=[main.t], inlined_insert=[1]}`).
///
/// The raw text is the snapshot's `ducklake_snapshot_changes.changes_made`,
/// which every writer (DuckDB, pinkdb) fills with DuckLake's own grammar
/// (`created_table:"main"."t"`, `inlined_insert:1`, …). Rendering from that
/// text is what makes this column equal to DuckDB's for a lake either tool
/// wrote; the engine-facing [`crate::ChangeSummary`] answers a different
/// question and cannot.
fn changes_text(changes_made: Option<&str>) -> String {
    let mut buckets: BTreeMap<&'static str, Bucket> = BTreeMap::new();
    // Iterating `CHANGE_KEYS` below prints the map in DuckDB's order, so the
    // collected keys' own order is irrelevant.
    for item in split_items(changes_made.unwrap_or_default()) {
        let Some((kind, value)) = item.split_once(':') else {
            continue;
        };
        let Some((key, named)) = change_key(kind.trim()) else {
            continue;
        };
        let value = value.trim();
        let names = || Bucket::Names(Vec::new());
        let ids = || Bucket::Ids(Vec::new());
        match buckets
            .entry(key)
            .or_insert_with(if named { names } else { ids })
        {
            Bucket::Names(names) => names.push(unquote(value)),
            Bucket::Ids(ids) => {
                if let Ok(id) = value.parse::<i64>() {
                    ids.push(id);
                }
            }
        }
    }
    let entries: Vec<String> = CHANGE_KEYS
        .iter()
        .filter_map(|key| {
            let values = buckets.remove(key)?.render();
            (!values.is_empty()).then(|| format!("{key}=[{values}]"))
        })
        .collect();
    if entries.is_empty() {
        return "{}".to_string();
    }
    format!("{{{}}}", entries.join(", "))
}

/// The `changes` map's keys in the order DuckDB prints them
/// (`ducklake_snapshots.cpp`).
const CHANGE_KEYS: &[&str] = &[
    "schemas_created",
    "tables_created",
    "views_created",
    "scalar_macros_created",
    "table_macros_created",
    "procedures_created",
    "schemas_dropped",
    "tables_dropped",
    "tables_altered",
    "tables_inserted_into",
    "tables_deleted_from",
    "views_dropped",
    "scalar_macros_dropped",
    "table_macros_dropped",
    "procedures_dropped",
    "views_altered",
    "inlined_insert",
    "inlined_delete",
    "flushed_inlined",
    "merge_adjacent",
    "rewrite_delete",
];

/// One `changes` map entry: names keep their quoted form's text, ids sort.
enum Bucket {
    Names(Vec<String>),
    Ids(Vec<i64>),
}

impl Bucket {
    fn render(self) -> String {
        match self {
            Bucket::Ids(mut ids) => {
                ids.sort_unstable();
                ids.dedup();
                ids.iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            Bucket::Names(mut names) => {
                names.sort_by_key(|name| name.to_lowercase());
                names.dedup();
                names.join(", ")
            }
        }
    }
}

/// `changes_made`'s kinds in `ducklake_snapshots`' `changes` map, in DuckDB's
/// key order (`ducklake_snapshots.cpp`); the flag says whether the values are
/// names or ids.
fn change_key(kind: &str) -> Option<(&'static str, bool)> {
    Some(match kind {
        "created_schema" => ("schemas_created", true),
        "created_table" => ("tables_created", true),
        "created_view" => ("views_created", true),
        "created_scalar_macro" => ("scalar_macros_created", true),
        "created_table_macro" => ("table_macros_created", true),
        "created_procedure" => ("procedures_created", true),
        "dropped_schema" => ("schemas_dropped", false),
        "dropped_table" => ("tables_dropped", false),
        "dropped_view" => ("views_dropped", false),
        "dropped_scalar_macro" => ("scalar_macros_dropped", false),
        "dropped_table_macro" => ("table_macros_dropped", false),
        "dropped_procedure" => ("procedures_dropped", false),
        "altered_table" => ("tables_altered", false),
        "altered_view" => ("views_altered", false),
        "inserted_into_table" => ("tables_inserted_into", false),
        "deleted_from_table" => ("tables_deleted_from", false),
        "inlined_insert" => ("inlined_insert", false),
        "inlined_delete" => ("inlined_delete", false),
        "flushed_inlined" | "inline_flush" => ("flushed_inlined", false),
        "merge_adjacent" => ("merge_adjacent", false),
        "rewrite_delete" => ("rewrite_delete", false),
        _ => return None,
    })
}

/// `changes_made`'s comma-separated items; a comma inside a quoted identifier
/// does not end one.
fn split_items(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    for (index, character) in text.char_indices() {
        match character {
            '"' => quoted = !quoted,
            ',' if !quoted => {
                out.push(&text[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    out.push(&text[start..]);
    out
}

/// `"main"."my table"` → `main.my table` (DuckLake's unquoted display form).
fn unquote(value: &str) -> String {
    let mut out = String::new();
    let mut quoted = false;
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '"' if !quoted => quoted = true,
            '"' => {
                if characters.peek() == Some(&'"') {
                    characters.next();
                    out.push('"');
                } else {
                    quoted = false;
                }
            }
            other => out.push(other),
        }
    }
    out
}

/// A lake file's delete file, when it has one: pinkdb's manifest also carries
/// inlined deletes as a reference to the data file itself, which DuckDB never
/// shows as a delete file.
fn delete_file_of(info: &LakeFileInfo) -> Option<&str> {
    info.delete_file
        .as_deref()
        .filter(|delete| *delete != info.path)
}

/// A snapshot's time in microseconds since the epoch.
///
/// `SnapshotInfo::time_micros` is only filled when the catalog holds the
/// engine's own ISO-8601 shape; a DuckLake catalog holds
/// `2026-10-31 22:54:07.5545+05:30` and a pinkdb-written SQL catalog holds
/// `2026-10-23 11:09:19.127+00:00`, so both are parsed here.
fn snapshot_micros(info: &SnapshotInfo) -> Option<i64> {
    info.time_micros.or_else(|| parse_lake_time(&info.time))
}

pub(crate) fn parse_lake_time(text: &str) -> Option<i64> {
    let text = text.trim();
    let (text, offset) = match text.strip_suffix('Z') {
        Some(text) => (text, 0),
        None => split_utc_offset(text),
    };
    let (date, time) = text.split_once(['T', ' '])?;
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (time, ""),
    };
    if frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let micros = parse_iso8601_utc(&format!("{date}T{hms}.{frac:0<6}Z"))?;
    Some(micros - offset * 1_000_000)
}

/// `2026-10-31 22:54:07.5545+05:30` → (`2026-10-31 22:54:07.5545`,
/// 19_800).
fn split_utc_offset(text: &str) -> (&str, i64) {
    let bytes = text.as_bytes();
    if bytes.len() < 6 {
        return (text, 0);
    }
    let start = bytes.len() - 6;
    let sign = match bytes[start] {
        b'+' => 1,
        b'-' => -1,
        _ => return (text, 0),
    };
    let (hours, minutes) = (&text[start + 1..start + 3], &text[start + 4..]);
    if bytes[start + 3] != b':' {
        return (text, 0);
    }
    match (hours.parse::<i64>(), minutes.parse::<i64>()) {
        (Ok(hours), Ok(minutes)) => (&text[..start], sign * (hours * 3_600 + minutes * 60)),
        _ => (text, 0),
    }
}

/// DuckDB's `ducklake_options` descriptions, by option name
/// (`ducklake_options.cpp`). An option this build does not know keeps a `NULL`
/// description, exactly as DuckDB prints one.
fn option_description(name: &str) -> Option<String> {
    let description = match name.to_ascii_lowercase().as_str() {
        "data_inlining_row_limit" => "Maximum amount of rows to inline in a single insert",
        "data_file_format" => "Managed data-file format (parquet or vortex)",
        "parquet_compression" => {
            "Compression algorithm for Parquet files (uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw)"
        }
        "parquet_version" => "Parquet format version (1 or 2)",
        "parquet_compression_level" => "Compression level for Parquet files",
        "parquet_row_group_size" => "Number of rows per row group in Parquet files",
        "parquet_row_group_size_bytes" => "Number of bytes per row group in Parquet files",
        "hive_file_pattern" => {
            "If partitioned data should be written in a hive-like folder structure"
        }
        "target_file_size" => "The target data file size for insertion and compaction operations",
        "version" => "DuckLake format version",
        "created_by" => "Tool used to write the DuckLake",
        "data_path" => "Path to data files",
        "require_commit_message" => {
            "If an explicit commit message is required for a snapshot commit."
        }
        "rewrite_delete_threshold" => {
            "A threshold that determines the minimum amount of data that must be removed from a file before a rewrite is warranted. From 0 - 1."
        }
        "delete_older_than" => {
            "How old unused files must be to be removed by the 'ducklake_delete_orphaned_files' and 'ducklake_cleanup_old_files' cleanup functions."
        }
        "expire_older_than" => {
            "How old snapshots must be, by default, to be expired by: 'ducklake_expire_snapshots'"
        }
        "auto_compact" => {
            "Pre-defined schema used as a default value for the following compaction functions 'ducklake_flush_inlined_data','ducklake_merge_adjacent_files', 'ducklake_rewrite_data_files', 'ducklake_delete_orphaned_files'"
        }
        "encrypted" => "Whether or not to encrypt Parquet files written to the data path",
        "per_thread_output" => {
            "Whether to create separate output files per thread during parallel insertion"
        }
        "write_deletion_vectors" => {
            "[EXPERIMENTAL - do not use outside testing] Whether to write Iceberg V3 deletion vectors (puffin) instead of positional delete files (parquet)"
        }
        "sort_on_insert" => {
            "Whether to sort data on INSERT according to SET SORTED BY (default: true)"
        }
        "skip_stats_columns" => {
            "Columns for which min/max bounds are not recorded (counts are still recorded)"
        }
        _ => return None,
    };
    Some(description.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, positional: Vec<FnArg>) -> DuckLakeCall {
        DuckLakeCall::new(name, positional)
    }

    fn text(value: &str) -> FnArg {
        FnArg::Text(value.to_string())
    }

    fn error(call: &DuckLakeCall) -> String {
        bind(call)
            .err()
            .expect("the call should be refused")
            .to_string()
    }

    #[test]
    fn changes_render_as_duckdbs_map() {
        assert_eq!(changes_text(None), "{}");
        assert_eq!(changes_text(Some("nothing")), "{}");
        assert_eq!(
            changes_text(Some("created_schema:\"main\"")),
            "{schemas_created=[main]}"
        );
        assert_eq!(
            changes_text(Some("created_table:\"main\".\"t\"")),
            "{tables_created=[main.t]}"
        );
        // DuckDB prints the map's own key order, not the order it was stored.
        assert_eq!(
            changes_text(Some("inlined_delete:1,inlined_insert:1")),
            "{inlined_insert=[1], inlined_delete=[1]}"
        );
        // Ids sort numerically; a kind listed twice is printed once.
        assert_eq!(
            changes_text(Some(
                "inserted_into_table:10,inserted_into_table:2,altered_table:1"
            )),
            "{tables_altered=[1], tables_inserted_into=[2, 10]}"
        );
        assert_eq!(
            changes_text(Some("flushed_inlined:2,inline_flush:2")),
            "{flushed_inlined=[2]}"
        );
    }

    #[test]
    fn snapshot_times_parse_in_both_catalog_shapes() {
        // DuckDB writes a local-offset timestamp with up to six fraction digits.
        assert_eq!(
            parse_lake_time("2026-10-31 22:54:07.5545+05:30"),
            parse_lake_time("2026-10-31 17:24:07.554500Z")
        );
        // pinkdb's SQL catalogs hold UTC with a `+00:00` suffix.
        assert_eq!(
            parse_lake_time("2026-10-23 11:09:19.127+00:00"),
            parse_lake_time("2026-10-23 11:09:19.127Z")
        );
        assert_eq!(
            parse_lake_time("2026-10-23 11:09:19-07:00"),
            parse_lake_time("2026-10-23 18:09:19Z")
        );
        assert_eq!(parse_lake_time("yesterday"), None);
    }

    #[test]
    fn change_items_split_outside_quotes() {
        assert_eq!(split_items("a:1,b:2"), vec!["a:1", "b:2"]);
        assert_eq!(
            split_items("created_table:\"a,b\".\"c\""),
            vec!["created_table:\"a,b\".\"c\""]
        );
        assert_eq!(unquote("\"a,b\".\"c\"\"d\""), "a,b.c\"d");
    }

    #[test]
    fn mutating_functions_are_the_call_ones() {
        assert!(!is_mutating("ducklake_snapshots"));
        assert!(is_mutating("ducklake_flush_inlined_data"));
        assert!(is_mutating("DUCKLAKE_EXPIRE_SNAPSHOTS"));
        assert!(!is_mutating("ducklake_nope"));
    }

    #[test]
    fn binding_reports_the_expected_signature() {
        // An unknown name is not a binding error: `output_schema` answers
        // `None` for it and the runners refuse it.
        assert!(bind(&call("ducklake_nope", vec![])).unwrap().is_none());
        let message = unknown_function("ducklake_nope").to_string();
        assert!(
            message.contains("ducklake_nope is not a DuckLake function"),
            "{message}"
        );
        assert!(message.contains("ducklake_snapshots"), "{message}");

        let message = error(&call("ducklake_table_changes", vec![text("l")]));
        assert!(
            message.starts_with(
                "ducklake_table_changes: needs 5 positional argument(s), got 1; expected "
            ),
            "{message}"
        );

        let message = error(&call("ducklake_snapshots", vec![text("l"), text("x")]));
        assert!(
            message.contains("takes at most 1 positional argument(s), got 2"),
            "{message}"
        );

        let mut named = call("ducklake_snapshots", vec![text("l")]);
        named.named.push(("drayn".to_string(), FnArg::Bool(true)));
        let message = error(&named);
        assert!(
            message.contains("unknown named argument \"drayn\""),
            "{message}"
        );

        let mut twice = call("ducklake_expire_snapshots", vec![text("l")]);
        twice.named.push(("dry_run".to_string(), FnArg::Bool(true)));
        twice
            .named
            .push(("DRY_RUN".to_string(), FnArg::Bool(false)));
        let message = error(&twice);
        assert!(message.contains("given twice"), "{message}");

        let message = error(&call("ducklake_snapshots", vec![FnArg::Int(7)]));
        assert!(
            message.contains("argument 1 (catalog) must be VARCHAR, got BIGINT"),
            "{message}"
        );
    }

    #[test]
    fn named_arguments_match_whatever_case_the_caller_used() {
        let mut call = call("ducklake_expire_snapshots", vec![text("l")]);
        call.named.push(("Dry_Run".to_string(), FnArg::Bool(true)));
        let bound = bind(&call).unwrap().unwrap();
        assert!(matches!(bound.named("dry_run"), Some(FnArg::Bool(true))));
        assert!(bound.named("DRY_RUN").is_some());
        assert!(bound.named("versions").is_none());
    }
}
