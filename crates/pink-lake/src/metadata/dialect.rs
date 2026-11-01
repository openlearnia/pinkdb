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

/// The DuckLake metadata format this build writes. Stock DuckDB 2.x reports
/// `1.1-dev1` for its own catalogs, so writing the same string lets DuckDB
/// attach a pinkdb-written lake without a version override.
pub(crate) const DUCKLAKE_VERSION: &str = "1.1-dev1";

/// The DuckLake versions this build reads. They share the v1.1 table shape;
/// later versions only add tables pinkdb does not use.
pub(crate) const SUPPORTED_DUCKLAKE_VERSIONS: &[&str] = &["1.1-dev1", "1.1", "1.2", "1.3"];

/// The supported range, for the refusal message.
pub(crate) const SUPPORTED_DUCKLAKE_RANGE: &str = "1.1-dev1 through 1.3";

/// A SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dialect {
    /// SQLite (the `catalog-sqlite` feature).
    Sqlite,
    /// The durable pinkdb `.pink` database (`pink:<path>`), always built.
    Pink,
    /// Postgres (`catalog-postgres`): numbered `$n` placeholders, `BYTEA`
    /// binary columns and a plain `BEGIN` (a snapshot-id unique violation is
    /// the retryable conflict; no advisory lock is needed).
    Postgres,
}

impl Dialect {
    /// One bound-parameter placeholder (`?1` for SQLite, `?` for pinkdb,
    /// whose prepared statements are strictly positional).
    pub(crate) fn ph(&self, index: usize) -> String {
        match self {
            Self::Sqlite => format!("?{index}"),
            Self::Pink => "?".to_string(),
            Self::Postgres => format!("${index}"),
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
            Self::Pink | Self::Postgres => "BEGIN",
        }
    }

    /// Start a read transaction (not used by any backend today; the trait
    /// reads are single statements).
    #[allow(dead_code)]
    pub(crate) fn begin_read(&self) -> &'static str {
        match self {
            Self::Sqlite | Self::Pink | Self::Postgres => "BEGIN",
        }
    }

    /// The binary column type. pinkdb has no BLOB: the opaque side objects
    /// (JSON bytes only) are stored as lower-case hex text there, which the
    /// [`super::conn`] value layer encodes and decodes.
    pub(crate) fn blob_type(&self) -> &'static str {
        match self {
            Self::Sqlite => "BLOB",
            Self::Pink => "VARCHAR",
            Self::Postgres => "BYTEA",
        }
    }

    /// The pinkdb side tables. One list, with the two backend differences
    /// applied here: the BLOB spelling, and no `DEFAULT` clause (pinkdb's
    /// `CREATE TABLE` grammar has none; every insert supplies the column).
    pub(crate) fn side_ddl(&self) -> String {
        let tables = SIDE_DDL.replace("BLOB", self.blob_type());
        // One index list for all three backends. The engine extends index
        // maps incrementally on append, so an index on a per-commit table no
        // longer makes a commit O(files).
        format!("{tables}{INDEX_DDL}")
    }

    /// The exact DuckLake v1.1 catalog tables. Column names and order match
    /// what DuckDB's own DuckLake extension creates; the three column types a
    /// backend spells differently are [`Dialect::ducklake_column_type`], and
    /// `IF NOT EXISTS` is the only other addition, so opening an existing
    /// DuckLake-created catalog is idempotent.
    pub(crate) fn ducklake_ddl(&self) -> String {
        let mut out = String::new();
        for statement in DUCKLAKE_TABLES {
            out.push_str("CREATE TABLE IF NOT EXISTS ");
            out.push_str(&self.ducklake_table(statement));
            out.push_str(";\n");
        }
        out
    }

    /// One table's text with this backend's spelling of the three DuckLake
    /// column types DuckDB does not write the same way everywhere.
    ///
    /// DuckDB creates a boolean as `BIGINT` in a SQLite catalog and as
    /// `BOOLEAN` in Postgres, a uuid as `VARCHAR`/`UUID`, and a timestamp as
    /// `VARCHAR`/`TIMESTAMP WITH TIME ZONE`. A pinkdb catalog keeps the SQLite
    /// spellings at the engine level (it has no boolean or uuid type), and a
    /// DuckDB reader is told the catalog's type by `ducklake_metadata.version`,
    /// never by the column text.
    fn ducklake_table(&self, statement: &str) -> String {
        let mut sql = statement.to_string();
        // The tokens are upper-case type names and no DuckLake column name
        // spells one, so a plain replace cannot hit data.
        for (token, written) in [
            ("BOOL", self.ducklake_column_type(DuckLakeType::Bool)),
            ("UUID", self.ducklake_column_type(DuckLakeType::Uuid)),
            (
                "TIMESTAMP",
                self.ducklake_column_type(DuckLakeType::Timestamp),
            ),
        ] {
            sql = sql.replace(token, written);
        }
        sql
    }

    fn ducklake_column_type(&self, ducklake_type: DuckLakeType) -> &'static str {
        match (self, ducklake_type) {
            (Self::Postgres, DuckLakeType::Bool) => "BOOLEAN",
            (Self::Postgres, DuckLakeType::Uuid) => "UUID",
            (Self::Postgres, DuckLakeType::Timestamp) => "TIMESTAMP WITH TIME ZONE",
            (_, DuckLakeType::Bool) => "BIGINT",
            (_, DuckLakeType::Uuid | DuckLakeType::Timestamp) => "VARCHAR",
        }
    }
}

/// The three DuckLake column types a backend spells differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DuckLakeType {
    Bool,
    Uuid,
    Timestamp,
}

/// The DuckLake v1.1 tables, exactly as DuckLake creates them. A column whose
/// type a backend spells differently carries the token [`DuckLakeType`] names
/// (`BOOL`, `UUID`, `TIMESTAMP`); [`Dialect::ducklake_ddl`] resolves it. Kept as
/// a constant list so the parity test can compare it with a catalog DuckDB
/// created.
pub(crate) const DUCKLAKE_TABLES: &[&str] = &[
    r#"ducklake_metadata("key" VARCHAR NOT NULL, "value" VARCHAR NOT NULL, "scope" VARCHAR, scope_id BIGINT)"#,
    "ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMP, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)",
    "ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)",
    "ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOL)",
    "ducklake_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOL)",
    "ducklake_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, \"sql\" VARCHAR, column_aliases VARCHAR)",
    "ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
    "ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOL, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT, row_group_count BIGINT)",
    "ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOL, extra_stats VARCHAR, min_is_exact BOOL, max_is_exact BOOL)",
    "ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOL, extra_stats VARCHAR)",
    "ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOL, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT, row_group_count BIGINT)",
    "ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOL, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)",
    "ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)",
    "ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BOOL, contains_nan BOOL, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR, min_is_exact BOOL, max_is_exact BOOL)",
    "ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
    "ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, \"transform\" VARCHAR)",
    "ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)",
    "ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BOOL, schedule_start TIMESTAMP)",
    "ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)",
    "ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, \"type\" VARCHAR)",
    "ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOL)",
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
-- The engine catalog image (wave 96b): a stateless `pinkdb-server` keeps every
-- engine-side catalog object in the catalog database instead of a local
-- `.pink` + WAL. One row, one opaque blob, keyed by the revision it was
-- published with: the `ducklake_*` schema is fixed by DuckDB interop, so this
-- lives beside it under a pinkdb name. See `crates/pinkdb/src/catalog_image.rs`
-- for the blob's layout.
CREATE TABLE IF NOT EXISTS pinkdb_engine_catalog (
    id       INTEGER PRIMARY KEY,
    revision BIGINT NOT NULL,
    encoded  BLOB NOT NULL
);
-- Replication state (wave 103 T2): every field the old `replication/state.json`
-- carried has its own typed column. Tables are keyed by DuckLake's table id
-- where one exists (`dest_table_id`); `source`/`dest` stay the flat catalog
-- keys because a duckdb-created catalog may not have the destination table
-- yet.
CREATE TABLE IF NOT EXISTS pinkdb_replication_state (
    id                 BIGINT PRIMARY KEY,
    replication_format BIGINT NOT NULL,
    next_id            BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS pinkdb_replication_job (
    job_id            BIGINT PRIMARY KEY,
    source_kind       TEXT NOT NULL,
    source_location   TEXT,
    source_database   TEXT,
    status            TEXT NOT NULL,
    interval_ms       BIGINT NOT NULL,
    \"include\"         TEXT NOT NULL,
    \"exclude\"         TEXT NOT NULL,
    watermark_columns TEXT NOT NULL,
    created           TEXT NOT NULL,
    last_run          TEXT,
    last_ok           TEXT,
    last_error        TEXT
);
CREATE TABLE IF NOT EXISTS pinkdb_replication_table (
    job_id                 BIGINT NOT NULL,
    source                 TEXT NOT NULL,
    dest                   TEXT NOT NULL,
    dest_table_id          BIGINT,
    strategy               TEXT NOT NULL,
    watermark_column       TEXT,
    pk_columns             TEXT,
    source_table_id        TEXT,
    last_watermark         TEXT,
    last_source_version    BIGINT,
    last_full_sync_version BIGINT,
    rows_dest              BIGINT,
    last_error             TEXT,
    synced                 TEXT,
    PRIMARY KEY (job_id, source)
);
";

/// The access-path indexes SQLite and Postgres get (pink rebuilds its whole
/// index map on every write, which would make a per-commit row O(files)).
pub(crate) const INDEX_DDL: &str = "
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
