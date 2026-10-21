//! Registered **write sinks** — the `COPY … (FORMAT <name>)` half of the
//! first-party file extensions (wave 71; scoped to writes in the wave-70/71
//! unification).
//!
//! ### What this surface is (and is not)
//!
//! Reading a file — `FROM 'x.arrow'`, `FROM read_arrow('x')`, parquet globs —
//! is the host **table-source** surface ([`crate::table_source::TableSource`]);
//! one trait serves every file format. This module keeps the *write* half
//! only: `COPY <table> TO '<path>' [(FORMAT <name>)]` needs a name (or an
//! extension) to resolve and a function that turns an engine table into
//! bytes. The two halves are separate traits because their consumers differ:
//! a scan answers chunks with a projection and pushdown hints, a sink
//! consumes one materialized table.
//!
//! ```text
//! pinkdb (engine)         pinkdb-arrow / pinkdb-parquet        pinkdb-cli
//!   trait FileSink     ←──  impl FileSink (write)         ←──  `arrow` feature:
//!   PinkDb registry         impl TableSource (read)             register(db)
//! ```
//!
//! Registration is **process/store-level**, exactly like loadable
//! extensions: not part of a database snapshot, never persisted, and a store
//! clone (transaction snapshot, multi-writer copy) shares the registered
//! sinks. A build without the implementation has no sink registered, and
//! `COPY` fails with a clear "no file format …" error naming the feature —
//! never a silent no-op.
//!
//! One asymmetry with reads: a **store-level** registration is enough for a
//! sink. `COPY … TO` is never WAL-logged (it has no store effect), so
//! recovery never needs a sink; the process-global registration exists only
//! so a host can bootstrap every store it creates with one call (the same
//! call that registers the read side).
//!
//! ### Extension packages (wave 86)
//!
//! A sink can ship inside a [`crate::package::ExtensionPackage`] together
//! with scalar functions and table sources. `LOAD EXTENSION '<package>'`
//! appends the package's sinks to **this store's** format list (the same
//! list `register_file_format` fills, consulted together with the
//! process-global seed), atomically: a format-name collision with a global
//! registration or an already-active sink refuses the whole package before
//! anything is installed. Another store sees nothing until it loads the
//! package itself.
//!
//! ### Durability
//!
//! A file read is an **external input**, not session-local state: a logged
//! statement that reads a file (`INSERT INTO t SELECT * FROM 'f.arrow'`)
//! replays by re-reading that file, exactly like DuckDB — see
//! [`crate::table_source`]. A file deleted before replay is a hard recovery
//! error, and a file *changed* before replay replays its new content (the
//! documented, DuckDB-matching caveat). `COPY … TO` has no store effect at
//! all and is never WAL-logged.

use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use crate::storage::{DataTable, PinkDb};
use crate::types::{PinkDbError, Result};

/// Register a write sink **process-wide** (idempotent: same-name
/// registration replaces).
///
/// Every store *constructed after* this call starts with the sink in its own
/// registry. The host-bootstrap step `pinkdb_arrow::ensure_registered()`
/// performs this (and the matching global table-source registration) for the
/// CLI, so a build with the feature has both the read and the write surface
/// in every store it creates or opens. A store's own
/// [`PinkDb::register_file_format`] adds or replaces sinks for that store
/// alone.
pub fn register_global_file_format(format: Arc<dyn FileFormat>) {
    let mut formats = global_formats()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = format.name().to_ascii_lowercase();
    formats.retain(|f| f.name().to_ascii_lowercase() != key);
    formats.push(format);
}

/// Whether a sink is registered process-globally under `name`
/// (case-insensitive). Package activation consults this before installing a
/// format: a name a global registration already owns stays out of reach,
/// even on a store constructed before that registration.
pub(crate) fn is_global_file_format(name: &str) -> bool {
    global_formats()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .any(|f| f.name().eq_ignore_ascii_case(name))
}

/// The process-global formats a store starts with (see
/// [`register_global_file_format`]).
pub(crate) fn seeded_global_formats() -> Vec<Arc<dyn FileFormat>> {
    global_formats()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn global_formats() -> &'static Mutex<Vec<Arc<dyn FileFormat>>> {
    static GLOBAL: OnceLock<Mutex<Vec<Arc<dyn FileFormat>>>> = OnceLock::new();
    GLOBAL.get_or_init(|| Mutex::new(Vec::new()))
}

/// A registered `COPY … (FORMAT <name>)` **write sink**: Arrow IPC, parquet, …
///
/// Every method takes the path as written in SQL (never canonicalized): the
/// implementation resolves it against the process working directory, exactly
/// like `.import`. Reads do **not** go through this trait — see
/// [`crate::table_source::TableSource`].
pub trait FileFormat: fmt::Debug + Send + Sync {
    /// Canonical lowercased name (`"arrow"`), as `FORMAT <name>` spells it.
    fn name(&self) -> &str;

    /// Lowercased file extensions (no dot) an extension-less `COPY … TO
    /// 'x.<ext>'` resolves by (DuckDB's inference rule).
    fn extensions(&self) -> &[&str];

    /// Write one engine table to `path` (created or truncated).
    fn write(&self, path: &Path, table: &DataTable) -> Result<()>;
}

impl PinkDb {
    /// Register (or replace) a file format — process/store-level, like a
    /// loadable extension: not part of any snapshot, shared by store clones.
    ///
    /// Registering a format under a name that is already present replaces
    /// the previous implementation (the registry is consulted at bind time;
    /// a replacement never affects a running statement).
    pub fn register_file_format(&mut self, format: Arc<dyn FileFormat>) {
        let key = format.name().to_ascii_lowercase();
        // A host registration replaces whatever occupied the name, including
        // a package's sink: drop the package mapping with it.
        self.package_file_formats.remove(&key);
        self.file_formats
            .retain(|f| f.name().to_ascii_lowercase() != key);
        self.file_formats.push(format);
    }

    /// The registered format named `name` (case-insensitive), if any.
    pub fn file_format(&self, name: &str) -> Option<&dyn FileFormat> {
        let key = name.to_ascii_lowercase();
        self.file_formats
            .iter()
            .find(|f| f.name().to_ascii_lowercase() == key)
            .map(|f| &**f)
    }

    /// A human-readable list of the registered format names, for errors
    /// (`arrow`; `arrow, parquet`; `none`).
    pub fn file_format_names(&self) -> String {
        if self.file_formats.is_empty() {
            return "none".to_string();
        }
        let mut names: Vec<&str> = self.file_formats.iter().map(|f| f.name()).collect();
        names.sort_unstable();
        names.join(", ")
    }

    /// The registered sink a `COPY … TO '<path>'` without `FORMAT` resolves to
    /// by the path's extension (DuckDB's inference rule), or a bind error
    /// naming the registered formats.
    pub fn file_format_for_path(&self, path: &str) -> Result<&dyn FileFormat> {
        let ext = Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase());
        match ext.as_deref().and_then(|e| {
            self.file_formats
                .iter()
                .find(|f| f.extensions().contains(&e))
        }) {
            Some(f) => Ok(&**f),
            None => Err(PinkDbError::Catalog(format!(
                "no file format is registered for \"{path}\" (registered formats: {}); \
                 the format is chosen by the file extension — a build with the \
                 matching first-party extension (e.g. `arrow`) registers it",
                self.file_format_names()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::QueryResult;
    use crate::types::DataType;
    use crate::types::Value;

    /// A unique scratch directory per call (pid + a process-wide counter), the
    /// engine's usual pattern for file-backed tests.
    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_filefmt_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A fake **write sink**: line 1 is `name:TYPE,…`, every following line is
    /// one row of `|`-separated cells (an empty cell is NULL — the fake has no
    /// way to spell an empty string; the arrow crate's round-trip tests do).
    /// It exists to pin the engine's COPY surfaces (format resolution by name
    /// and by extension, the `Count` result, error paths) without any real
    /// file-format dependency. Reads are the table-source surface
    /// ([`crate::table_source`]) and are pinned there.
    #[derive(Debug)]
    struct TestFormat {
        name: &'static str,
        ext: &'static str,
    }

    impl TestFormat {
        fn main() -> Self {
            Self {
                name: "testfmt",
                ext: "testfmt",
            }
        }
        fn clonefmt() -> Self {
            Self {
                name: "clonefmt",
                ext: "clonefmt",
            }
        }
    }

    impl FileFormat for TestFormat {
        fn name(&self) -> &str {
            self.name
        }
        fn extensions(&self) -> &[&str] {
            std::slice::from_ref(&self.ext)
        }
        fn write(&self, path: &Path, table: &DataTable) -> Result<()> {
            let mut out = String::new();
            let schema = table.schema();
            out.push_str(
                &schema
                    .iter()
                    .map(|(n, t)| format!("{n}:{}", t.name()))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            out.push('\n');
            for row in 0..table.num_rows {
                let cells: Vec<String> = (0..table.num_cols())
                    .map(|c| match table.get(row, c).unwrap() {
                        Value::Null => String::new(),
                        Value::Integer(i) => i.to_string(),
                        Value::Bigint(i) => i.to_string(),
                        Value::Double(x) => x.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        Value::Varchar(s) => s,
                        other => format!("{other:?}"),
                    })
                    .collect();
                out.push_str(&cells.join("|"));
                out.push('\n');
            }
            use std::io::Write as _;
            crate::fsutil::write_atomic_private(path, |f| {
                f.write_all(out.as_bytes())?;
                Ok(())
            })
        }
    }

    /// A stock engine has no sinks: every COPY surface fails with the
    /// documented error, never a silent no-op.
    #[test]
    fn stock_store_refuses_every_copy_surface() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let err = db
            .execute("COPY t TO 'x.testfmt' (FORMAT testfmt);")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no file format \"testfmt\" is registered"),
            "{err}"
        );
        assert!(db.file_format("testfmt").is_none());
        assert!(db.file_format_names().contains("none"));
    }

    /// A registered sink drives `COPY … TO … (FORMAT …)`, including extension
    /// inference when FORMAT is omitted, plus the `Count` result shape.
    #[test]
    fn registered_sink_writes_and_infers_by_extension() {
        let dir = scratch_dir("surfaces");
        let file = dir.join("data.testfmt");
        let file2 = dir.join("inferred.testfmt");
        let mut db = PinkDb::new();
        db.register_file_format(Arc::new(TestFormat::main()));

        db.execute("CREATE TABLE t (a INTEGER, s VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (NULL, ''), (3, NULL);")
            .unwrap();
        let result = db
            .execute(&format!("COPY t TO '{}' (FORMAT testfmt);", file.display()))
            .unwrap()
            .pop()
            .unwrap();
        match result {
            QueryResult::Selected { names, types, rows } => {
                assert_eq!(names, vec!["Count".to_string()]);
                assert_eq!(types, vec![DataType::Bigint]);
                assert_eq!(rows, vec![vec![Value::Bigint(3)]]);
            }
            other => panic!("expected the Count result, got {other:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "a:INTEGER,s:VARCHAR\n1|x\n|\n3|\n"
        );
        // M-3/M-5: even the embedded (trusted) path writes owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "COPY destination must be 0600");
        }

        // Extension inference when FORMAT is omitted.
        db.execute(&format!("COPY t TO '{}';", file2.display()))
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&file2).unwrap(),
            std::fs::read_to_string(&file).unwrap()
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Bind-time refusals: unknown format name, a missing source table, a
    /// format with no sink registered, a view source, and unsupported COPY
    /// options.
    #[test]
    fn copy_surfaces_refuse_cleanly() {
        let dir = scratch_dir("refusals");
        let mut db = PinkDb::new();
        db.register_file_format(Arc::new(TestFormat::main()));
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();

        let err = db
            .execute("COPY nope TO 'x.testfmt' (FORMAT testfmt);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("COPY source table \"nope\""), "{err}");

        let err = db
            .execute("COPY t TO 'x.testfmt' (FORMAT other);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no file format \"other\""), "{err}");

        let err = db
            .execute("COPY t TO 'x.testfmt' (FORMAT testfmt, COMPRESSION zstd);")
            .unwrap_err()
            .to_string();
        assert!(!err.is_empty());

        // COPY of a view is refused with the materialize hint.
        db.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        let err = db
            .execute("COPY v TO 'x.testfmt' (FORMAT testfmt);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("materialize"), "{err}");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Re-registering replaces the implementation and store clones keep it.
    #[test]
    fn registration_is_store_level_and_shared_by_clones() {
        let mut db = PinkDb::new();
        db.register_file_format(Arc::new(TestFormat::clonefmt()));
        db.register_file_format(Arc::new(TestFormat::clonefmt()));
        assert!(db.file_format_names().contains("clonefmt"));
        let clone = db.clone();
        assert!(clone.file_format("clonefmt").is_some());
        // A fresh store starts without it (sinks are never persisted; this
        // name is not registered process-globally).
        assert!(PinkDb::new().file_format("clonefmt").is_none());
    }
}
