//! `GRAIN_DUCKDB_SANDBOX`: keep tenant SQL from loading native code.
//!
//! The Go service's sandbox is DuckDB's own configuration: an allow-list of
//! file prefixes (`allowed_directories`), external access off, autoinstall
//! off, and `lock_configuration` so SQL cannot undo any of it.
//!
//! pinkdb has one of those policies, the store-wide `LOAD`/`INSTALL` gate
//! (`Database::set_allow_sql_load_extension`). It has no file-read roots —
//! `serve::ServePolicy::allow_files` is a bool, and `fsutil::CopyToPolicy`
//! confines `COPY … TO` to a single root, neither a prefix allow-list for
//! `read_csv`/`read_parquet`/`COPY … FROM` — and no configuration lock. An
//! adapter-level path check would re-implement the engine's file resolution
//! and still miss a view or a scalar that reads on its own, so the switch
//! applies the gate and nothing else. The gap is reported, not papered over.

use pinkdb::Database;

/// Apply the switch to the sessions this adapter opens. `LOAD`/`INSTALL` are
/// refused by the engine before it looks at a path.
pub(crate) fn apply(db: &Database, enabled: bool) {
    if !enabled {
        eprintln!(
            "warn: GRAIN_DUCKDB_SANDBOX is off — tenant SQL can read local files and load extensions"
        );
        return;
    }
    db.set_allow_sql_load_extension(false);
    eprintln!("pinkdb-server: QuackLab sandbox on: LOAD/INSTALL refused");
}
