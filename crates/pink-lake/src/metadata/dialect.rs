//! One SQL layer, one dialect enum (wave 103).
//!
//! The DuckLake v1.1 schema and every statement the catalog issues are written
//! once here. [`Dialect`] names the small syntactic differences a backend has:
//! placeholder style, how a write transaction begins and the few DDL spellings
//! a backend lacks. SQLite and pinkdb are the implementations; T2's Postgres
//! reuses the same text with another variant.
//!
//! Values are always bound, never interpolated. The only text this module
//! builds by concatenation is DDL that names an inlined table
//! (`ducklake_inlined_data_<table_id>_<schema_version>`), and that name is
//! derived from two integers the store allocated, never from user input.

/// The DuckLake metadata format this build writes and reads.
pub(crate) const DUCKLAKE_VERSION: &str = "1.1";

/// A SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dialect {
    /// SQLite (the `catalog-sqlite` feature).
    Sqlite,
    /// The durable pinkdb `.pink` database (`pink:<path>`), always built.
    Pink,
}

impl Dialect {
    /// One bound-parameter placeholder (`?1` for SQLite, `?` for pinkdb,
    /// whose prepared statements are strictly positional).
    pub(crate) fn ph(&self, index: usize) -> String {
        match self {
            Self::Sqlite => format!("?{index}"),
            Self::Pink => "?".to_string(),
        }
    }

    /// Start one write transaction. SQLite takes its write lock up front so
    /// two writers serialize instead of deadlocking on an upgrade; pinkdb's
    /// `BEGIN` is its documented optimistic snapshot, and `COMMIT` validates
    /// the read/write set under the write lease (a moved head is the same
    /// retryable conflict).
    pub(crate) fn begin_write(&self) -> &'static str {
        match self {
            Self::Sqlite => "BEGIN IMMEDIATE",
            Self::Pink => "BEGIN",
        }
    }

    /// Start a read transaction (not used by either backend today; the
    /// trait reads are single statements).
    #[allow(dead_code)]
    pub(crate) fn begin_read(&self) -> &'static str {
        match self {
            Self::Sqlite | Self::Pink => "BEGIN",
        }
    }

    /// The binary column type. pinkdb has no BLOB: the opaque side objects
    /// (JSON bytes only) are stored as lower-case hex text there, which the
    /// [`super::conn`] value layer encodes and decodes.
    pub(crate) fn blob_type(&self) -> &'static str {
        match self {
            Self::Sqlite => "BLOB",
            Self::Pink => "VARCHAR",
        }
    }

    /// The pinkdb side tables. One list, with the two backend differences
    /// applied here: the BLOB spelling, and no `DEFAULT` clause (pinkdb's
    /// `CREATE TABLE` grammar has none; every insert supplies the column).
    pub(crate) fn side_ddl(&self) -> String {
        SIDE_DDL.replace("BLOB", self.blob_type())
    }

    /// The exact DuckLake v1.1 catalog tables. Column names, order and types
    /// match `/tmp/w96a/ref/ducklake-v1.1-schema.sql`; `IF NOT EXISTS` is the
    /// only addition, so opening an existing DuckLake-created catalog is
    /// idempotent.
    pub(crate) fn ducklake_ddl(&self) -> String {
        let mut out = String::new();
        for statement in DUCKLAKE_TABLES {
            out.push_str("CREATE TABLE IF NOT EXISTS ");
            out.push_str(statement);
            out.push_str(";\n");
        }
        out
    }
}

/// The DuckLake v1.1 tables, exactly as DuckLake creates them. Kept as a
/// constant list so a test can compare the generated DDL against the
/// reference file.
pub(crate) const DUCKLAKE_TABLES: &[&str] = &[
    r#"ducklake_metadata("key" VARCHAR NOT NULL, "value" VARCHAR NOT NULL, "scope" VARCHAR, scope_id BIGINT)"#,
    "ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time VARCHAR, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)",
    "ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)",
    "ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BIGINT, parent_schema_id BIGINT)",
    "ducklake_table(table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BIGINT)",
    "ducklake_view(view_id BIGINT, view_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, \"sql\" VARCHAR, column_aliases VARCHAR)",
    "ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BIGINT, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT, row_group_count BIGINT)",
    "ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BIGINT, extra_stats VARCHAR, min_is_exact BIGINT, max_is_exact BIGINT)",
    "ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BIGINT, extra_stats VARCHAR)",
    "ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT, row_group_count BIGINT)",
    "ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BIGINT, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)",
    "ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)",
    "ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BIGINT, contains_nan BIGINT, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR, min_is_exact BIGINT, max_is_exact BIGINT)",
    "ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, \"transform\" VARCHAR)",
    "ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)",
    "ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, schedule_start VARCHAR)",
    "ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)",
    "ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, \"type\" VARCHAR)",
    "ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BIGINT)",
    "ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT)",
    "ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, \"sql\" VARCHAR, \"type\" VARCHAR)",
    "ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR)",
    "ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR)",
    "ducklake_view_column_tag(view_id BIGINT, column_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
];

/// The pinkdb-only side tables. They carry facts DuckLake has no column for
/// (pink's table/schema epoch, footer offsets, typed per-commit facts and
/// opaque side objects) and are keyed by DuckLake's own ids. Statistics
/// blocks and commit manifests are derived from DuckLake's rows, so they have
/// no JSON column here. The indexes are a SQLite access-path detail for those
/// derived reads; DuckLake never looks at them.
pub(crate) const SIDE_DDL: &str = "
CREATE TABLE IF NOT EXISTS pinkdb_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pinkdb_lake (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pinkdb_snapshot (
    snapshot_id    BIGINT PRIMARY KEY,
    updated        TEXT,
    format_version BIGINT,
    lake_id        TEXT
);
CREATE TABLE IF NOT EXISTS pinkdb_table (
    table_id             BIGINT PRIMARY KEY,
    catalog_key          TEXT NOT NULL,
    table_id_hex         TEXT,
    format               TEXT,
    inlined_limit        BIGINT,
    table_schema_version BIGINT
);
CREATE TABLE IF NOT EXISTS pinkdb_column (
    table_id      BIGINT NOT NULL,
    column_id     BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    dtype         TEXT,
    default_value TEXT,
    added         BIGINT,
    PRIMARY KEY (table_id, column_id, begin_snapshot)
);
CREATE TABLE IF NOT EXISTS pinkdb_rename (
    table_id    BIGINT NOT NULL,
    version     BIGINT NOT NULL,
    from_name   TEXT NOT NULL,
    to_name     TEXT NOT NULL,
    PRIMARY KEY (table_id, version, from_name)
);
CREATE TABLE IF NOT EXISTS pinkdb_data_file (
    data_file_id  BIGINT PRIMARY KEY,
    schema_version BIGINT,
    footer_offset BIGINT,
    footer_len    BIGINT,
    footer_crc32  BIGINT
);
-- The typed `ducklake_inlined_data_<table_id>_<suffix>` tables are the only
-- copy of an inlined row's values. DuckLake names that table after the
-- snapshot schema version (`suffix`), but pinkdb's decode path needs the
-- table schema epoch the row was written under (for rename/default mapping),
-- and DuckLake has nowhere to store that. One row per (table, suffix) — not
-- one per inlined row — carries only that mapping.
CREATE TABLE IF NOT EXISTS pinkdb_inlined_schema (
    table_id             BIGINT NOT NULL,
    suffix               BIGINT NOT NULL,
    table_schema_version BIGINT NOT NULL,
    PRIMARY KEY (table_id, suffix)
);
CREATE TABLE IF NOT EXISTS pinkdb_table_change (
    snapshot_id       BIGINT NOT NULL,
    table_id          BIGINT NOT NULL,
    kind              BIGINT NOT NULL,
    partition_changed BIGINT NOT NULL,
    inlining_changed  BIGINT NOT NULL,
    inlined_limit     BIGINT,
    PRIMARY KEY (snapshot_id, table_id)
);
CREATE TABLE IF NOT EXISTS pinkdb_object (
    path  TEXT PRIMARY KEY,
    bytes BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS pinkdb_file_stats_data_file
    ON ducklake_file_column_stats(data_file_id);
CREATE INDEX IF NOT EXISTS pinkdb_data_file_table
    ON ducklake_data_file(table_id);
CREATE INDEX IF NOT EXISTS pinkdb_delete_file_table
    ON ducklake_delete_file(table_id);
CREATE INDEX IF NOT EXISTS pinkdb_table_column_stats_table
    ON ducklake_table_column_stats(table_id);
CREATE INDEX IF NOT EXISTS pinkdb_table_change_snapshot
    ON pinkdb_table_change(snapshot_id);
";
