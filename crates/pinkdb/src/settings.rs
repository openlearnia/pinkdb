//! DuckDB's session settings.
//!
//! The sqllogictest corpus tunes DuckDB's engine far more than it queries it:
//! `SET`/`PRAGMA x = v` on bench knobs, profiler switches and debug checks.
//! pinkdb keeps two of those settings meaningful ([`OrderDefaults`]) and
//! accepts the rest of DuckDB's catalog as stored no-ops, so a file that
//! tunes DuckDB keeps going. The list below is the contract — a name outside
//! it stays the Catalog Error DuckDB raises for an unknown setting, and a
//! corpus test that expects that error keeps passing.
//!
//! Settings live in the session (`Session::setting`), not in the store: like
//! variables they never reach the WAL or a snapshot.
use crate::PinkDbError;
use crate::storage::Session;
use crate::types::Value;

/// Every name `duckdb_settings()` lists (v2.0.0-alpha46057), lowercased.
/// `SET` and `PRAGMA x = v` accept all of them; the ones pinkdb does not act
/// on still answer `current_setting`, so a test that only sets and reads one
/// back passes.
const DUCKDB_SETTINGS: &[&str] = &[
    "access_mode",
    "active_grammar_extensions",
    "allocator_background_threads",
    "allocator_bulk_deallocation_flush_threshold",
    "allocator_flush_threshold",
    "allow_community_extensions",
    "allow_extension_repositories",
    "allow_extensions_metadata_mismatch",
    "allow_parser_override_extension",
    "allow_persistent_secrets",
    "allow_unredacted_secrets",
    "allow_unsigned_extensions",
    "allowed_configs",
    "allowed_directories",
    "allowed_paths",
    "approximate_join_order_threshold",
    "arrow_large_buffer_size",
    "arrow_lossless_conversion",
    "arrow_output_list_view",
    "arrow_output_version",
    "asof_loop_join_threshold",
    "async_threads",
    "auto_checkpoint_skip_wal_threshold",
    "autoinstall_extension_repository",
    "autoinstall_known_extensions",
    "autoload_known_extensions",
    "binary_as_string",
    "block_allocator_memory",
    "cache_local_files",
    "calendar",
    "catalog_error_max_schemas",
    "checkpoint_on_detach",
    "checkpoint_threshold",
    "current_dialect",
    "current_transaction_invalidation_policy",
    "custom_extension_repository",
    "custom_user_agent",
    "debug_checkpoint_scan_sleep_ms",
    "default_block_size",
    "default_collation",
    "default_null_order",
    "default_order",
    "default_secret_storage",
    "default_transaction_invalidation_policy",
    "delim_join_as_cte",
    "dialect_compatibility_mode",
    "disable_database_invalidation",
    "disable_parquet_prefetching",
    "disable_timestamptz_casts",
    "disabled_compression_methods",
    "disabled_filesystems",
    "disabled_log_types",
    "disabled_optimizers",
    "duckdb_api",
    "dynamic_or_filter_threshold",
    "enable_external_access",
    "enable_external_file_cache",
    "enable_fsst_vectors",
    "enable_geoparquet_conversion",
    "enable_http_metadata_cache",
    "enable_logging",
    "enable_macro_dependencies",
    "enable_optimistic_write",
    "enable_optimizer",
    "enable_profiling",
    "enable_progress_bar",
    "enable_progress_bar_print",
    "enable_view_dependencies",
    "enabled_log_types",
    "error_on_division_by_zero",
    "errors_as_json",
    "experimental_metadata_reuse",
    "explain_output",
    "extension_directories",
    "extension_repository_directory",
    "external_file_cache_local_max_block_size",
    "external_file_cache_remote_max_block_size",
    "external_file_cache_remote_min_block_size",
    "external_file_cache_request_sizing",
    "external_file_cache_spill",
    "external_threads",
    "file_search_path",
    "force_column_metadata_reuse",
    "force_compression",
    "fsync_mode",
    "geometry_minimum_shredding_size",
    "home_directory",
    "http_client_pool_capacity",
    "http_proxy",
    "http_proxy_password",
    "http_proxy_username",
    "ieee_floating_point_ops",
    "ignore_unknown_crs",
    "immediate_transaction_mode",
    "index_scan_max_count",
    "index_scan_percentage",
    "initial_column_segment_size",
    "integer_division",
    "json_geometry_format",
    "late_materialization_max_rows",
    "legacy_disable_null_type",
    "lock_configuration",
    "log_query_path",
    "logging_level",
    "logging_mode",
    "logging_storage",
    "max_execution_time",
    "max_expression_depth",
    "max_memory",
    "max_streaming_buffer_size",
    "max_temp_directory_size",
    "max_vacuum_tasks",
    // Process-wide, wave 136a's memory pool: `SET`/`RESET` move the pool's
    // limit (exec `set_setting`), `current_setting` reads it.
    "memory_limit",
    "merge_join_threshold",
    "nested_loop_join_threshold",
    "null_order",
    "operator_memory_limit",
    "order_by_non_integer_literal",
    "ordered_aggregate_threshold",
    "parquet_metadata_cache",
    "parquet_prefetch_column_gap",
    "partitioned_write_flush_threshold",
    "partitioned_write_max_open_files",
    "password",
    "perfect_ht_threshold",
    "pin_threads",
    "pivot_filter_threshold",
    "pivot_limit",
    "prefer_range_joins",
    "prefetch_all_parquet_files",
    "preserve_identifier_case",
    "preserve_insertion_order",
    "profile_output",
    "profiling_coverage",
    "profiling_output",
    "profiling_renderer_settings",
    "progress_bar_time",
    // DuckDB still accepts these two deprecated spellings; `profiling_mode`
    // prints a deprecation warning and is otherwise ignored.
    "profile_output",
    "profiling_mode",
    // DuckDB registers these only in debug and extension builds (the corpus
    // files that set them are written for those builds). Accepting them keeps
    // the tuning statements of those files running; pinkdb stores them.
    "debug_disable_optimizer",
    "debug_force_commit_failure",
    "debug_force_commit_revert_failure",
    "debug_force_external",
    "debug_force_fetch_row",
    "debug_force_wal_fsync_failure",
    "debug_fs_delay_mean_ms",
    "debug_fs_delay_stddev_ms",
    "debug_fs_random_seed",
    "debug_local_file_system_delay_ms",
    "debug_physical_table_scan_execution_strategy",
    "debug_skip_checkpoint_on_commit",
    "debug_verify_blocks",
    "debug_verify_column_bindings",
    "debug_verify_statement",
    "enable_caching_operators",
    "extension_directory",
    "external_file_cache_local_block_size",
    "external_file_cache_remote_block_size",
    "force_bitpacking_mode",
    "force_mbedtls_unsafe",
    "force_update_to_del_and_insert",
    "force_variant_shredding",
    "http_retries",
    "lambda_syntax",
    "legacy_metrics_format",
    "local_infile",
    "match_recognize_max_states",
    "materialized_view_stale_read",
    "old_implicit_casting",
    "sqlite_all_varchar",
    "union_by_name",
    "read_ahead_depth",
    "redact_http_logs",
    "regex_match_operator_semantics",
    "scalar_subquery_error_on_multiple_rows",
    "scheduler_process_partial",
    "schema",
    "search_path",
    "secret_directory",
    "show_behavior",
    "standard_vector_size",
    "storage_block_prefetch",
    "storage_compatibility_version",
    "streaming_buffer_size",
    "table_function_identifier_conversion",
    "temp_directory",
    "temp_file_encryption",
    "threads",
    "timezone",
    "tracked_metrics",
    "user",
    "username",
    "validate_external_file_cache",
    "variant_minimum_shredding_size",
    "wal_autocheckpoint",
    "wal_autocheckpoint_entries",
    "warnings_as_errors",
    "worker_threads",
    "write_buffer_row_group_count",
    "write_buffer_row_group_memory_limit",
    "zstd_min_string_length",
];

/// The canonical (lowercase) spelling of a DuckDB setting, if pinkdb knows
/// the name.
pub(crate) fn canonical(name: &str) -> Option<&'static str> {
    let lower = name.to_ascii_lowercase();
    DUCKDB_SETTINGS
        .iter()
        .chain(PINKDB_SETTINGS)
        .copied()
        .find(|known| *known == lower)
}

/// pinkdb's own switches. They are settings rather than `SET VARIABLE`
/// variables so that `SET name = value` means one thing, and they have no
/// DuckDB counterpart: `join_filter_pushdown`, `pipeline_execution` (wave 135:
/// `false` forces the materialized path) and the two HNSW search knobs are
/// debugging switches here.
const PINKDB_SETTINGS: &[&str] = &[
    "hnsw_ef_search",
    "hnsw_exact",
    "join_filter_pushdown",
    "pipeline_execution",
];

/// The same, as the error path wants it: an unknown name is DuckDB's Catalog
/// Error, which is what the corpus' `statement error` tests expect.
/// The catalog name of a setting, or a test knob that is also settable
/// (`SET debug_checkpoint_abort = 'before'`): DuckDB's test knobs are pragmas
/// that take a value too, and pinkdb stores them like the rest of the no-op
/// list.
pub(crate) fn resolve_settable(name: &str) -> Result<&str, PinkDbError> {
    match canonical(name) {
        Some(canonical) => Ok(canonical),
        None if is_noop_pragma(name) => Ok(name),
        None => Err(unknown_setting(name)),
    }
}

fn unknown_setting(name: &str) -> PinkDbError {
    PinkDbError::Catalog(format!("unrecognized configuration parameter \"{name}\""))
}

pub(crate) fn resolve(name: &str) -> Result<&'static str, PinkDbError> {
    canonical(name).ok_or_else(|| unknown_setting(name))
}

/// DuckDB's statement pragmas that only tune or check the engine: controlled
/// by the corpus far more often than any real feature (`PRAGMA
/// disable_checkpoint_on_shutdown` alone heads ~115 failing files). pinkdb
/// accepts them and does nothing — this list is the whole contract, and a
/// name outside it stays DuckDB's Catalog Error.
const PRAGMA_NOOPS: &[&str] = &[
    "asof_loop_join_threshold",
    "checkpoint_on_shutdown",
    "debug_asof_iejoin",
    "debug_checkpoint_abort",
    "debug_force_external",
    "debug_force_no_cross_product",
    "debug_print_bind_context",
    "debug_print_optimized_plan",
    "debug_print_plans",
    "debug_set_forced_table_scan_info",
    "debug_skip_checkpoint_on_commit",
    "debug_verify_serializer",
    "debug_verify_vector",
    "debug_window_mode",
    "disable_checkpoint_on_shutdown",
    "disable_logging",
    "disable_object_cache",
    "disable_optimizer",
    "disable_print_progress_bar",
    "disable_profiling",
    "disable_progress_bar",
    "disable_verification",
    "disable_verify_parallelism",
    "enable_checkpoint_on_shutdown",
    "enable_logging",
    "enable_object_cache",
    "enable_optimizer",
    "enable_print_progress_bar",
    "enable_profiling",
    "enable_progress_bar",
    "enable_verification",
    "force_checkpoint",
    "force_compression",
    "verify_external",
    "verify_parallelism",
    "verify_serializer",
];

/// Is `name` one of the pragmas pinkdb takes as a no-op?
pub(crate) fn is_noop_pragma(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    PRAGMA_NOOPS.contains(&lower.as_str())
}

/// The value `current_setting` reports for a setting the session never set.
/// Only the handful the corpus reads is filled in: DuckDB's other defaults
/// are engine-specific (sizes, paths, time zones) and never drive a query.
fn default_value(name: &str) -> Option<Value> {
    match name {
        "threads" => Some(Value::Bigint(threads())),
        "default_null_order" => Some(Value::Varchar(NullOrder::default().canonical().to_string())),
        "default_order" => Some(Value::Varchar("ASC".to_string())),
        "max_execution_time" => Some(Value::Bigint(0)),
        "catalog_error_max_schemas" => Some(Value::Bigint(100)),
        "search_path" => Some(Value::Varchar(String::new())),
        _ => None,
    }
}

/// The session's value for `name`, or its default. `None` when neither is
/// known (DuckDB reports NULL for an unset, defaultless setting too).
pub(crate) fn current(session: &Session, name: &str) -> Option<Value> {
    session.setting(name).or_else(|| default_value(name))
}

/// Check a value before it is stored: the order settings must name a mode,
/// `threads` a positive integer. Returns the value to store (the canonical
/// spelling for the order settings), or DuckDB's error for a bad one.
pub(crate) fn validate(name: &str, value: Value) -> Result<Value, PinkDbError> {
    match name {
        "default_null_order" => {
            let text = text(&value).ok_or_else(|| bad_null_order(&format!("{value:?}")))?;
            let order = NullOrder::parse(text).ok_or_else(|| bad_null_order(text))?;
            Ok(Value::Varchar(order.canonical().to_string()))
        }
        "default_order" => {
            let text = text(&value).ok_or_else(|| bad_default_order(&format!("{value:?}")))?;
            let desc = match text.to_ascii_uppercase().as_str() {
                "ASC" | "ASCENDING" => false,
                "DESC" | "DESCENDING" => true,
                _ => return Err(bad_default_order(text)),
            };
            Ok(Value::Varchar(
                if desc { "DESC" } else { "ASC" }.to_string(),
            ))
        }
        "threads" => match value {
            Value::Integer(n) if n > 0 => Ok(Value::Bigint(i64::from(n))),
            Value::Bigint(n) if n > 0 => Ok(Value::Bigint(n)),
            _ => Err(PinkDbError::Type(
                "threads must be a positive integer".to_string(),
            )),
        },
        _ => Ok(value),
    }
}

fn bad_null_order(value: &str) -> PinkDbError {
    PinkDbError::Parse(format!(
        "Unrecognized parameter for option NULL_ORDER \"{value}\", expected either \
         NULLS FIRST, NULLS LAST, SQLite, MySQL or Postgres"
    ))
}

fn bad_default_order(value: &str) -> PinkDbError {
    PinkDbError::Type(format!(
        "Unrecognized parameter for option DEFAULT_ORDER \"{value}\". Expected ASC or DESC."
    ))
}

fn text(value: &Value) -> Option<&str> {
    match value {
        Value::Varchar(s) => Some(s),
        _ => None,
    }
}

/// How many threads the machine has: DuckDB's `threads` default, and what
/// pinkdb reports for the setting.
fn threads() -> i64 {
    std::thread::available_parallelism()
        .map(|n| i64::try_from(n.get()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}

/// DuckDB's `default_null_order`: where NULLs sort when an order item does
/// not say. The four modes are DuckDB's own; its SQLite/MySQL/Postgres
/// compat spellings resolve to one of them (measured on v2.0.0-alpha46057).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullOrder {
    First,
    /// DuckDB's default, in both directions.
    #[default]
    Last,
    FirstOnAscLastOnDesc,
    LastOnAscFirstOnDesc,
}

impl NullOrder {
    pub(crate) fn parse(text: &str) -> Option<NullOrder> {
        let normal = text.to_ascii_uppercase().replace([' ', '-'], "_");
        Some(match normal.as_str() {
            "NULLS_FIRST" | "NULL_FIRST" => NullOrder::First,
            "NULLS_LAST" | "NULL_LAST" => NullOrder::Last,
            "NULLS_FIRST_ON_ASC_LAST_ON_DESC" | "SQLITE" | "MYSQL" => {
                NullOrder::FirstOnAscLastOnDesc
            }
            "NULLS_LAST_ON_ASC_FIRST_ON_DESC" | "POSTGRES" => NullOrder::LastOnAscFirstOnDesc,
            _ => return None,
        })
    }

    pub(crate) fn canonical(self) -> &'static str {
        match self {
            NullOrder::First => "NULLS_FIRST",
            NullOrder::Last => "NULLS_LAST",
            NullOrder::FirstOnAscLastOnDesc => "NULLS_FIRST_ON_ASC_LAST_ON_DESC",
            NullOrder::LastOnAscFirstOnDesc => "NULLS_LAST_ON_ASC_FIRST_ON_DESC",
        }
    }

    /// Does an order with this mode sort NULLs first when it is `desc`?
    pub(crate) fn nulls_first(self, desc: bool) -> bool {
        match self {
            NullOrder::First => true,
            NullOrder::Last => false,
            NullOrder::FirstOnAscLastOnDesc => !desc,
            NullOrder::LastOnAscFirstOnDesc => desc,
        }
    }
}

/// The session's `default_order` / `default_null_order`, resolved once per
/// ORDER BY (an `OrderItem` that says neither inherits both).
#[derive(Debug, Clone, Copy)]
pub struct OrderDefaults {
    pub desc: bool,
    pub null_order: NullOrder,
}

impl OrderDefaults {
    pub(crate) fn from_session(session: &Session) -> OrderDefaults {
        let setting_text = |name: &str| match current(session, name) {
            Some(Value::Varchar(s)) => Some(s),
            _ => None,
        };
        let desc = matches!(
            setting_text("default_order").as_deref(),
            Some("DESC" | "desc")
        );
        let null_order = setting_text("default_null_order")
            .and_then(|s| NullOrder::parse(&s))
            .unwrap_or_default();
        OrderDefaults { desc, null_order }
    }
}
