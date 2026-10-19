//! Registered file formats — the engine-adjacent half of the first-party
//! `arrow` extension (wave 71).
//!
//! ### Why this exists (the integration decision)
//!
//! The scalar extension ABI (waves 29A/49) registers *scalar functions*
//! only: it cannot express a table source (`FROM 'x.arrow'`), a table
//! function (`read_arrow`), or a write statement (`COPY … (FORMAT arrow)`).
//! The engine therefore exposes a second, deliberately tiny extension
//! surface for **file formats**, modeled on the attached-lake trait
//! (`crate::attached::AttachedLake`): the engine defines the trait and owns
//! the registry, an engine-adjacent first-party crate implements it
//! (`extensions/arrow` = `pinkdb-arrow`, which depends on `pinkdb`, never
//! the other way around), and the host (the CLI, or an embedder) registers
//! the implementation at store creation:
//!
//! ```text
//! pinkdb (engine)          pinkdb-arrow (crate)         pinkdb-cli
//!   trait FileFormat   ←──   impl FileFormat      ←──   `arrow` feature:
//!   PinkDb registry          arrow-rs IPC                 register(db)
//! ```
//!
//! Registration is **process/store-level**, exactly like loadable
//! extensions: not part of a database snapshot, never persisted, and a store
//! clone (transaction snapshot, multi-writer copy) shares the registered
//! formats. A build without the implementation simply has no format
//! registered, and every SQL surface that would need one fails with a clear
//! "no file format registered for …" error naming the feature — never a
//! silent no-op and never a half-answer.
//!
//! ### Durability
//!
//! A file is an **external input**, not session-local state. A logged
//! statement that reads a file (`INSERT INTO t SELECT * FROM 'f.arrow'`)
//! replays by re-reading that file, exactly like DuckDB; a replay build
//! without the needed format fails hard at bind (never a silent drop), and a
//! file deleted or changed between the original execution and replay is a
//! hard replay error (or a different, *detectable* schema). `COPY … TO` has
//! no store effect at all and is never WAL-logged.

use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result};

/// One column of a file's schema: the engine type a scan of that column
/// produces, in file order.
pub type FileSchema = Vec<(String, DataType)>;

/// Register a file format **process-wide** (idempotent: same-name
/// registration replaces).
///
/// Every store *constructed after* this call starts with the format in its
/// own registry. That timing is what makes a durable store replay: recovery
/// runs inside [`PinkDb::open_durable`](crate::PinkDb::open_durable), before
/// the caller holds the store, so a host that will replay file-reading
/// records must register here first — the host-bootstrap step
/// `pinkdb_arrow::ensure_registered()` performs for the CLI (the same shape
/// as `pinkdb_json::ensure_registered()` for a linked scalar extension). A
/// store's own [`PinkDb::register_file_format`] then adds or replaces
/// formats for that store alone.
pub fn register_global_file_format(format: Arc<dyn FileFormat>) {
    let mut formats = global_formats()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = format.name().to_ascii_lowercase();
    formats.retain(|f| f.name().to_ascii_lowercase() != key);
    formats.push(format);
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

/// A registered file format: Arrow IPC, parquet, …
///
/// Every method takes the path as written in SQL (never canonicalized): the
/// implementation resolves it against the process working directory, exactly
/// like `.import`.
pub trait FileFormat: fmt::Debug + Send + Sync {
    /// Canonical lowercased name (`"arrow"`), as `FORMAT <name>` spells it.
    fn name(&self) -> &str;

    /// Lowercased file extensions (no dot) `FROM 'x.<ext>'` resolves by.
    fn extensions(&self) -> &[&str];

    /// Lowercased `read_<name>(…)` table-function names this format answers
    /// to (`["read_arrow"]`).
    fn read_functions(&self) -> &[&str];

    /// The file's schema, read from metadata only — the binder calls this to
    /// resolve `FROM 'x.<ext>'` without decoding any column data.
    fn schema(&self, path: &Path) -> Result<FileSchema>;

    /// Read the whole file as an engine table (chunks ≤ `CHUNK_SIZE`, columns
    /// in file order). Called once per scan.
    fn read(&self, path: &Path) -> Result<DataTable>;

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

    /// The registered format whose extension list contains `ext` (lowercased,
    /// no dot).
    pub fn file_format_for_extension(&self, ext: &str) -> Option<&dyn FileFormat> {
        self.file_formats
            .iter()
            .find(|f| f.extensions().contains(&ext))
            .map(|f| &**f)
    }

    /// The registered format answering the `read_*` table function `fname`
    /// (lowercased).
    pub fn file_format_for_function(&self, fname: &str) -> Option<&dyn FileFormat> {
        self.file_formats
            .iter()
            .find(|f| f.read_functions().contains(&fname))
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

    /// The format a `FROM '<path>'` source resolves to by extension, or a
    /// bind error naming the registered formats.
    pub fn file_format_for_path(&self, path: &str) -> Result<&dyn FileFormat> {
        let ext = Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase());
        match ext
            .as_deref()
            .and_then(|e| self.file_format_for_extension(e))
        {
            Some(f) => Ok(f),
            None => Err(PinkDbError::Catalog(format!(
                "no file format is registered for \"{path}\" (registered formats: {}); \
                 the extension is chosen by the file extension — a build with the \
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

    /// A fake registered format: line 1 is `name:TYPE,…`, every following
    /// line is one row of `|`-separated cells (**empty cell = NULL** — the
    /// fake format has no way to spell an empty string, so the surface tests
    /// do not exercise that distinction; the arrow crate does). It exists to
    /// pin the *engine* surfaces (replacement scan, `read_*` function, COPY,
    /// schema validation, errors) without any real file-format dependency —
    /// the arrow crate exercises the same trait with a real format.
    ///
    /// Each surface test uses its own *name* so the one test that touches the
    /// process-global registry (the durability test) cannot leak into the
    /// others' "not registered" assertions.
    #[derive(Debug)]
    struct TestFormat {
        name: &'static str,
        ext: &'static str,
        read_fn: &'static str,
    }

    impl TestFormat {
        /// The store-level surface tests' format.
        fn main() -> Self {
            Self {
                name: "testfmt",
                ext: "testfmt",
                read_fn: "read_testfmt",
            }
        }
        /// The durable-replay test's format (registered process-globally).
        fn wal() -> Self {
            Self {
                name: "walfmt",
                ext: "walfmt",
                read_fn: "read_walfmt",
            }
        }
        /// The clone/replacement test's format (never global).
        fn clonefmt() -> Self {
            Self {
                name: "clonefmt",
                ext: "clonefmt",
                read_fn: "read_clonefmt",
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
        fn read_functions(&self) -> &[&str] {
            std::slice::from_ref(&self.read_fn)
        }
        fn schema(&self, path: &Path) -> Result<FileSchema> {
            Ok(Self::parse(path)?.0)
        }
        fn read(&self, path: &Path) -> Result<DataTable> {
            let (schema, rows) = Self::parse(path)?;
            let mut t = DataTable::new(
                path.file_stem().unwrap().to_string_lossy().to_string(),
                schema,
            )?;
            for row in rows {
                t.append_row(row)?;
            }
            Ok(t)
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
            std::fs::write(path, out).map_err(|e| {
                PinkDbError::Execution(format!("cannot write \"{}\": {e}", path.display()))
            })
        }
    }

    impl TestFormat {
        fn parse(path: &Path) -> Result<(FileSchema, Vec<Vec<Value>>)> {
            let text = std::fs::read_to_string(path).map_err(|e| {
                PinkDbError::Execution(format!("cannot read \"{}\": {e}", path.display()))
            })?;
            let mut lines = text.lines();
            let header = lines.next().ok_or_else(|| {
                PinkDbError::Execution(format!("\"{}\" is empty", path.display()))
            })?;
            let schema: FileSchema = header
                .split(',')
                .map(|spec| {
                    let (name, ty) = spec.split_once(':').expect("name:TYPE");
                    (
                        name.to_string(),
                        DataType::from_sql_params(ty, &[]).expect("testfmt TYPE"),
                    )
                })
                .collect();
            let mut rows = Vec::new();
            for line in lines {
                let mut row = Vec::with_capacity(schema.len());
                for (cell, (_, ty)) in line.split('|').zip(schema.iter()) {
                    row.push(if cell.is_empty() {
                        Value::Null
                    } else {
                        match ty {
                            DataType::Integer => Value::Integer(cell.parse().unwrap()),
                            DataType::Varchar => Value::Varchar(cell.to_string()),
                            other => panic!("TestFormat does not parse {other:?}"),
                        }
                    });
                }
                rows.push(row);
            }
            Ok((schema, rows))
        }
    }

    fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected a result set, got {other:?}"),
        }
    }

    /// A stock engine has no formats: every file surface fails with the
    /// documented error, never a silent no-op.
    #[test]
    fn stock_store_refuses_every_file_surface() {
        let mut db = PinkDb::new();
        let err = db
            .execute("SELECT * FROM 'x.testfmt';")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no file format is registered"), "{err}");
        let err = db
            .execute("SELECT * FROM read_arrow('x.arrow');")
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown table function"), "{err}");
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let err = db
            .execute("COPY t TO 'x.testfmt' (FORMAT testfmt);")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no file format \"testfmt\" is registered"),
            "{err}"
        );
        // A fresh store has only whatever the process-global registry seeded.
        assert!(db.file_format("testfmt").is_none());
    }

    /// One registered format drives all three surfaces: the extension
    /// replacement scan, the `read_*` table function, and `COPY … TO …
    /// (FORMAT …)` (with extension inference when FORMAT is omitted).
    #[test]
    fn registered_format_drives_scan_function_and_copy() {
        let dir = scratch_dir("surfaces");
        let file = dir.join("data.testfmt");
        let file2 = dir.join("inferred.testfmt");
        let mut db = PinkDb::new();
        db.register_file_format(Arc::new(TestFormat::main()));

        // COPY writes the file and reports DuckDB's Count shape.
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

        // Replacement scan: the bare path, with an alias, projected, filtered.
        let rows = query(
            &mut db,
            &format!(
                "SELECT a, s FROM '{}' ORDER BY a NULLS LAST;",
                file.display()
            ),
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Varchar("x".to_string())],
                vec![Value::Integer(3), Value::Null],
                // The fake format spells every empty cell as NULL.
                vec![Value::Null, Value::Null],
            ]
        );
        let rows = query(
            &mut db,
            &format!(
                "SELECT f.a FROM '{}' AS f WHERE f.a IS NOT NULL ORDER BY f.a;",
                file.display()
            ),
        );
        assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
        // Aggregates and joins over a file source work like any scan.
        let rows = query(
            &mut db,
            &format!("SELECT COUNT(*), COUNT(a) FROM '{}';", file.display()),
        );
        assert_eq!(rows, vec![vec![Value::Bigint(3), Value::Bigint(2)]]);

        // Table-function spelling, with the same schema.
        let rows = query(
            &mut db,
            &format!("SELECT COUNT(*) FROM read_testfmt('{}');", file.display()),
        );
        assert_eq!(rows[0][0], Value::Bigint(3));

        // Extension inference when FORMAT is omitted.
        db.execute(&format!("COPY t TO '{}';", file2.display()))
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&file2).unwrap(),
            std::fs::read_to_string(&file).unwrap()
        );

        // EXPLAIN names the file scan and the format.
        let rows = query(
            &mut db,
            &format!("EXPLAIN SELECT * FROM '{}';", file.display()),
        );
        let plan: Vec<String> = rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Varchar(s) => s.clone(),
                other => panic!("expected VARCHAR, got {other:?}"),
            })
            .collect();
        assert!(
            plan.iter().any(|l| l.contains("FileScan testfmt")),
            "{plan:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Bind-time refusals: unknown format name, unknown reader function, a
    /// file that does not exist, a missing table, and `AT` on a file source.
    #[test]
    fn file_surfaces_refuse_cleanly() {
        let dir = scratch_dir("refusals");
        let missing = dir.join("missing.testfmt");
        let mut db = PinkDb::new();
        db.register_file_format(Arc::new(TestFormat::main()));
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();

        let err = db
            .execute(&format!("SELECT * FROM '{}';", missing.display()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read"), "{err}");
        assert!(err.contains("testfmt"), "{err}");

        let err = db
            .execute("SELECT * FROM read_nope('x.testfmt');")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unknown table function \"read_nope\""),
            "{err}"
        );
        assert!(err.contains("testfmt"), "{err}");

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
            .execute(&format!(
                "SELECT * FROM '{}' AT (VERSION => 1);",
                missing.display()
            ))
            .unwrap_err()
            .to_string();
        assert!(err.contains("AT is only valid after a table name"), "{err}");

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

    /// Durability: a logged statement that reads a file replays by
    /// re-reading it (DuckDB's rule); a file that vanished before replay is
    /// a hard recovery error, never a silent drop. `COPY` itself is never
    /// logged (it publishes no store change).
    ///
    /// The format must be registered *before* the store is constructed:
    /// recovery replays inside `open_durable`. This test is the only one in
    /// the engine binary that touches the process-global registry, so the
    /// store-level tests above stay order-independent.
    #[test]
    fn durable_stores_replay_file_reads_by_re_reading() {
        let dir = scratch_dir("durable");
        let file = dir.join("src.walfmt");
        let store = dir.join("store.pink");
        crate::filefmt::register_global_file_format(Arc::new(TestFormat::wal()));
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE src (a INTEGER, s VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y');")
            .unwrap();
        db.execute(&format!(
            "COPY src TO '{}' (FORMAT walfmt);",
            file.display()
        ))
        .unwrap();
        db.save(store.to_str().unwrap()).unwrap();
        drop(db);

        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE dst (a INTEGER);").unwrap();
        db.execute(&format!(
            "INSERT INTO dst SELECT a FROM '{}' WHERE a > 1;",
            file.display()
        ))
        .unwrap();
        assert_eq!(
            query(&mut db, "SELECT a FROM dst;"),
            vec![vec![Value::Integer(2)]]
        );

        // Replay re-reads the file: the reopened store has the same row, and
        // the re-opened store got the process-global format seeded at
        // construction (which is what makes this replay possible).
        drop(db);
        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        assert_eq!(
            query(&mut db, "SELECT a FROM dst;"),
            vec![vec![Value::Integer(2)]]
        );
        drop(db);

        // A file that vanished before replay is a hard error, never a silent
        // drop (the same shape a build without the format reports: its
        // binder has no registered format).
        std::fs::remove_file(&file).unwrap();
        let err = PinkDb::open_durable(store.to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read"), "{err}");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Re-registering replaces the implementation and store clones keep it.
    #[test]
    fn registration_is_store_level_and_shared_by_clones() {
        let mut db = PinkDb::new();
        db.register_file_format(Arc::new(TestFormat::clonefmt()));
        db.register_file_format(Arc::new(TestFormat::clonefmt()));
        // (Compared by `contains`: another test in this binary registers a
        // different format process-globally, which every later store seeds.)
        assert!(db.file_format_names().contains("clonefmt"));
        let clone = db.clone();
        assert!(clone.file_format("clonefmt").is_some());
        // A fresh store starts without it (store formats are never persisted;
        // this name is never registered process-globally).
        assert!(PinkDb::new().file_format("clonefmt").is_none());
    }
}
