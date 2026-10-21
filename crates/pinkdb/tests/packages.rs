//! Wave 86 T1: extension packages — table functions and COPY sinks for
//! linked extensions.
//!
//! The package registry is process-global, so every test registers its own
//! uniquely named package. The durable tests create scratch stores under the
//! system temp directory; the "package is not registered" case runs the
//! store creation in a child process, because a process-global registration
//! cannot be undone.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use pinkdb::{
    DataChunk, DataTable, DataType, ExtensionPackage, FileFormat, PinkDb, PrunePredicate,
    QueryResult, Result, TableFunctionCall, TableScanOutput, TableSource, Value, Vector,
    extension_package_names, register_extension_package,
};
use pinkdb_ext_loader::linked::{LinkedExtension, LinkedFunction};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

/// Unique package names: the registry is process-global and permanently
/// append-only, so two tests must never share one.
const PKG_MAIN: &str = "wave86_pkg_main";
const PKG_ATOMIC_SOURCE: &str = "wave86_pkg_atomic_source";
const PKG_ATOMIC_FORMAT: &str = "wave86_pkg_atomic_format";
const PKG_ATOMIC_SCALAR: &str = "wave86_pkg_atomic_scalar";
const PKG_GLOBAL_SOURCE: &str = "wave86_pkg_global_source";
const PKG_GLOBAL_FORMAT: &str = "wave86_pkg_global_format";
const PKG_DURABLE: &str = "wave86_pkg_durable";
const PKG_CHILD: &str = "wave86_pkg_child";
const PKG_CHECKPOINT: &str = "wave86_pkg_checkpoint";
const PKG_SOURCELESS: &str = "wave86_pkg_sourceless";
const PKG_REGISTER: &str = "wave86_pkg_register";
const PKG_RENAMED: &str = "wave86_pkg_renamed";
const RENAMED_SCALARS: &str = "wave86_pkg_renamed_scalars";

const CHILD_STORE_ENV: &str = "PINKDB_TEST_PACKAGE_CHILD_STORE";

// ---------- helpers ----------

/// A scratch directory that is removed when the guard drops.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_packages_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        Scratch { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(out.len(), 1, "{sql} must be one statement");
    match out.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Whether `haystack` contains `needle` (the WAL payload is length-prefixed
/// but statement text appears verbatim).
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// The test table source: `pkg_range(n)` yields one BIGINT column `n` with
/// `0..n`, honoring the projection (an absent column becomes the chunk's
/// zero-length placeholder). It records every scan's projection so a test
/// can see what the engine asked for.
#[derive(Debug)]
struct PkgRangeSource {
    name: &'static str,
    scans: Mutex<Vec<Option<Vec<usize>>>>,
}

impl PkgRangeSource {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            scans: Mutex::new(Vec::new()),
        }
    }

    fn projections(&self) -> Vec<Option<Vec<usize>>> {
        self.scans.lock().unwrap().clone()
    }
}

impl TableSource for PkgRangeSource {
    fn name(&self) -> &str {
        self.name
    }

    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
        // Claim only this source's own function name: the process-global
        // registry may hold other test sources while the suite runs, and they
        // must not answer for each other.
        match call {
            TableFunctionCall::Function { name, .. } if name.eq_ignore_ascii_case(self.name) => {
                Ok(Some(vec![("n".to_string(), DataType::Bigint)]))
            }
            _ => Ok(None),
        }
    }

    fn scan(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        _filters: &[PrunePredicate],
    ) -> Result<TableScanOutput> {
        let n = match call {
            TableFunctionCall::Function { args, .. } => match args.first().map(|a| &a.value) {
                Some(Value::Bigint(n)) => *n,
                Some(Value::Integer(n)) => i64::from(*n),
                other => {
                    return Err(pinkdb::PinkDbError::Type(format!(
                        "pkg_range expects one integer argument, got {other:?}"
                    )));
                }
            },
            TableFunctionCall::Path(path) => {
                return Err(pinkdb::PinkDbError::Type(format!(
                    "pkg_range does not claim the path \"{path}\""
                )));
            }
        };
        if !(0..=1_000_000).contains(&n) {
            return Err(pinkdb::PinkDbError::Type(format!(
                "pkg_range argument {n} is out of range"
            )));
        }
        let row_count = n as usize;
        self.scans
            .lock()
            .unwrap()
            .push(projection.map(|p| p.to_vec()));
        let wanted = projection.is_none_or(|p| p.contains(&0));
        let chunk = if wanted {
            let mut column = Vector::with_capacity(DataType::Bigint, row_count);
            for i in 0..row_count {
                column.push_valid(Value::Bigint(i as i64))?;
            }
            DataChunk::new(vec![column])?
        } else {
            DataChunk::projected(
                vec![Arc::new(Vector::new_empty(DataType::Bigint))],
                row_count,
            )
        };
        Ok(TableScanOutput {
            chunks: vec![chunk],
            files: 0,
            row_groups: 0,
            row_groups_pruned: 0,
            columns_decoded: wanted as u64,
        })
    }
}

/// The test COPY sink: one row per line, cells separated by a TAB (NULL is
/// an empty cell).
#[derive(Debug)]
struct TsvFormat {
    name: &'static str,
    ext: &'static str,
}

impl TsvFormat {
    fn pkg() -> Self {
        Self {
            name: "pkgtsv",
            ext: "pkgtsv",
        }
    }
}

impl FileFormat for TsvFormat {
    fn name(&self) -> &str {
        self.name
    }

    fn extensions(&self) -> &[&str] {
        std::slice::from_ref(&self.ext)
    }

    fn write(&self, path: &Path, table: &DataTable) -> Result<()> {
        let mut out = String::new();
        for row in 0..table.num_rows() {
            let cells: Vec<String> = (0..table.num_cols())
                .map(|c| match table.get(row, c).unwrap_or(Value::Null) {
                    Value::Null => String::new(),
                    Value::Bigint(v) => v.to_string(),
                    Value::Integer(v) => v.to_string(),
                    Value::Varchar(s) => s,
                    other => format!("{other:?}"),
                })
                .collect();
            out.push_str(&cells.join("\t"));
            out.push('\n');
        }
        std::fs::write(path, out).map_err(|e| {
            pinkdb::PinkDbError::Execution(format!("cannot write \"{}\": {e}", path.display()))
        })
    }
}

/// One linked scalar extension named `name` with a single `INTEGER → INTEGER`
/// doubling function, `<name>_double`.
fn scalar_extension(name: &str) -> LinkedExtension {
    let double = LinkedFunction::new(
        format!("{name}_double"),
        vec![ExtType::Integer],
        ExtType::Integer,
        |args: &[ExtArg<'_>]| match args.first() {
            Some(ExtArg::Integer(v)) => Ok(ExtValue::Integer(v * 2)),
            Some(ExtArg::Null(_)) => Ok(ExtValue::Null),
            _ => Err("expected one INTEGER argument".to_string()),
        },
    )
    .unwrap();
    LinkedExtension::new(name, vec![double]).unwrap()
}

/// Publish `package` carrying the `pkg_range` source and the `pkgtsv` sink
/// (plus one scalar function) and return the source so a test can inspect
/// the projections the engine pushed. Each call must use a package name that
/// has never been registered in this process.
fn register_range_package(package: &'static str, source_name: &'static str) -> Arc<PkgRangeSource> {
    let source = Arc::new(PkgRangeSource::new(source_name));
    let as_source: Arc<dyn TableSource> = Arc::clone(&source) as Arc<dyn TableSource>;
    let pkg = ExtensionPackage::new(package)
        .with_scalars(scalar_extension(package))
        .with_table_source(as_source)
        .with_file_format(Arc::new(TsvFormat::pkg()));
    register_extension_package(pkg).expect("register the extension package");
    source
}

// ---------- the tests ----------

/// Registration is by case-insensitive name and one-shot; the listing is in
/// registration order. Registration alone activates nothing.
#[test]
fn package_registration_is_case_insensitive_and_one_shot() {
    let err = register_extension_package(ExtensionPackage::new("  ")).unwrap_err();
    assert!(err.to_string().contains("must not be empty"), "{err}");

    register_extension_package(ExtensionPackage::new(PKG_REGISTER)).unwrap();
    let names = extension_package_names();
    let index = names
        .iter()
        .position(|n| n == PKG_REGISTER)
        .expect("the registered package is listed");
    assert!(
        names[..index]
            .iter()
            .all(|n| !n.eq_ignore_ascii_case(PKG_REGISTER)),
        "the listing is in registration order"
    );
    let err = register_extension_package(ExtensionPackage::new(PKG_REGISTER.to_uppercase()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("already registered"), "{err}");

    let db = PinkDb::new();
    assert!(!db.is_extension_loaded(PKG_REGISTER));
    assert!(db.table_source("pkg_range").is_none());
}

/// The scalar half is optional: a package with only a table source loads,
/// pins its name against a second load, and leaves the sink registry alone.
#[test]
fn a_package_without_scalars_still_loads_and_pins_its_name() {
    let source: Arc<dyn TableSource> = Arc::new(PkgRangeSource::new("pkg_range"));
    register_extension_package(
        ExtensionPackage::new(PKG_SOURCELESS).with_table_source(Arc::clone(&source)),
    )
    .unwrap();

    let mut db = PinkDb::new();
    db.load_extension_package(PKG_SOURCELESS).unwrap();
    assert!(db.is_extension_loaded(PKG_SOURCELESS));
    assert_eq!(
        rows(&mut db, "SELECT n FROM pkg_range(2) ORDER BY n;"),
        vec![vec![Value::Bigint(0)], vec![Value::Bigint(1)]]
    );
    assert!(db.file_format("pkgtsv").is_none());
    let err = db
        .load_extension_package(PKG_SOURCELESS)
        .unwrap_err()
        .to_string();
    assert!(err.contains("already loaded"), "{err}");
}

/// A package's scalars are installed as a linked extension, but the WAL
/// prefix names the **package** (the name replay can resolve), even when the
/// linked extension's own name differs.
#[test]
fn package_scalars_are_prefixed_by_the_package_name() {
    let scratch = Scratch::new("renamed");
    let store = scratch.path("db.pink");
    register_extension_package(
        ExtensionPackage::new(PKG_RENAMED).with_scalars(scalar_extension(RENAMED_SCALARS)),
    )
    .unwrap();

    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    db.load_extension_package(PKG_RENAMED).unwrap();
    db.execute("CREATE TABLE t (x INTEGER);").unwrap();
    db.execute(&format!(
        "INSERT INTO t VALUES ({RENAMED_SCALARS}_double(21));"
    ))
    .unwrap();
    db.flush_wal().unwrap();
    let wal = std::fs::read(format!("{}.wal", store.display())).unwrap();
    let package_prefix = format!("LOAD EXTENSION '{PKG_RENAMED}'");
    assert!(
        contains(&wal, package_prefix.as_bytes()),
        "the record must name the package"
    );
    let scalar_prefix = format!("LOAD EXTENSION '{RENAMED_SCALARS}'");
    assert!(
        !contains(&wal, scalar_prefix.as_bytes()),
        "the record must not name the package's inner linked extension"
    );
    drop(db);

    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT x FROM t;"),
        vec![vec![Value::Integer(42)]]
    );
}

/// The headline: not active before `LOAD`, active on that store afterwards
/// (table function, scalar and COPY sink), unaffected on another store, and
/// a second load is refused. Projection pushdown reaches the package source.
#[test]
fn packages_activate_on_load_for_one_store_only() {
    let scratch = Scratch::new("main");
    let source = register_range_package(PKG_MAIN, "pkg_range");
    assert!(
        extension_package_names()
            .iter()
            .any(|n| n.eq_ignore_ascii_case(PKG_MAIN)),
        "the package must be listed after registration"
    );

    let mut db = PinkDb::new();
    // Not active before LOAD: no source, no sink, and the SQL surfaces fail.
    assert!(db.table_source("pkg_range").is_none());
    assert!(db.file_format("pkgtsv").is_none());
    assert!(!db.is_extension_loaded(PKG_MAIN));
    let err = db
        .execute("SELECT n FROM pkg_range(3);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown table function"), "{err}");
    db.execute("CREATE TABLE t (n BIGINT);").unwrap();
    let err = db
        .execute(&format!(
            "COPY t TO '{}' (FORMAT pkgtsv);",
            scratch.path("before.pkgtsv").display()
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("no file format"), "{err}");

    // The SQL gate is unchanged.
    let err = db
        .execute(&format!("LOAD EXTENSION '{PKG_MAIN}';"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LOAD EXTENSION is disabled"), "{err}");

    // The documented opt-in activates the package.
    db.set_allow_sql_load_extension(true);
    let result = db
        .execute(&format!("LOAD EXTENSION '{PKG_MAIN}';"))
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        matches!(&result, QueryResult::LoadedExtension { name } if name == PKG_MAIN),
        "{result:?}"
    );
    assert!(db.is_extension_loaded(PKG_MAIN));
    assert!(db.table_source("pkg_range").is_some());
    assert!(db.file_format("pkgtsv").is_some());
    // A store clone (transaction snapshot) shares the active surface.
    assert!(db.clone().table_source("pkg_range").is_some());

    // The table function binds and scans, and the projection reaches it.
    assert_eq!(
        rows(&mut db, "SELECT n FROM pkg_range(3) ORDER BY n;"),
        vec![
            vec![Value::Bigint(0)],
            vec![Value::Bigint(1)],
            vec![Value::Bigint(2)]
        ]
    );
    assert_eq!(source.projections().last(), Some(&Some(vec![0])));
    // An empty projection (COUNT(*) reads no column) still yields every row.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM pkg_range(4);"),
        vec![vec![Value::Bigint(4)]]
    );
    // The package's scalar function is installed like a linked extension's.
    assert_eq!(
        rows(&mut db, &format!("SELECT {PKG_MAIN}_double(21);")),
        vec![vec![Value::Integer(42)]]
    );

    // The COPY sink resolves by FORMAT and, when omitted, by extension.
    db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    let by_name = scratch.path("by_name.pkgtsv");
    db.execute(&format!(
        "COPY t TO '{}' (FORMAT pkgtsv);",
        by_name.display()
    ))
    .unwrap();
    assert_eq!(std::fs::read_to_string(&by_name).unwrap(), "1\n2\n");
    let by_ext = scratch.path("by_ext.pkgtsv");
    db.execute(&format!("COPY t TO '{}';", by_ext.display()))
        .unwrap();
    assert_eq!(std::fs::read_to_string(&by_ext).unwrap(), "1\n2\n");

    // Another store sees nothing: activation is store-local.
    let mut other = PinkDb::new();
    let err = other
        .execute("SELECT n FROM pkg_range(1);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown table function"), "{err}");
    assert!(other.file_format("pkgtsv").is_none());
    assert!(!other.is_extension_loaded(PKG_MAIN));

    // A second load is refused, through SQL and through the host API.
    let err = db
        .execute(&format!("LOAD EXTENSION '{PKG_MAIN}';"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("already loaded"), "{err}");
    let err = db.load_extension_package(PKG_MAIN).unwrap_err().to_string();
    assert!(err.contains("already loaded"), "{err}");
    assert!(db.table_source("pkg_range").is_some());
}

/// Activation is atomic: a colliding table-source name, format name or
/// scalar function name refuses the whole package and installs nothing.
#[test]
fn package_activation_is_atomic() {
    // (1) A store-local source already owns the package's source name.
    let mut db = PinkDb::new();
    let direct: Arc<dyn TableSource> = Arc::new(PkgRangeSource::new("pkg_range"));
    db.register_table_source(Arc::clone(&direct));
    register_extension_package(
        ExtensionPackage::new(PKG_ATOMIC_SOURCE)
            .with_scalars(scalar_extension(PKG_ATOMIC_SOURCE))
            .with_table_source(Arc::new(PkgRangeSource::new("pkg_range")))
            .with_file_format(Arc::new(TsvFormat::pkg())),
    )
    .unwrap();
    let err = db
        .load_extension_package(PKG_ATOMIC_SOURCE)
        .unwrap_err()
        .to_string();
    assert!(err.contains("pkg_range"), "{err}");
    assert!(!db.is_extension_loaded(PKG_ATOMIC_SOURCE));
    assert!(
        db.file_format("pkgtsv").is_none(),
        "the sink must not be installed after a refusal"
    );
    let err = db
        .execute(&format!("SELECT {PKG_ATOMIC_SOURCE}_double(1);"))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unknown function"),
        "the scalar must not be installed after a refusal: {err}"
    );
    assert!(
        Arc::ptr_eq(db.table_source("pkg_range").unwrap(), &direct),
        "the existing source must not be replaced"
    );

    // (2) A store-local sink already owns the package's format name.
    let mut db = PinkDb::new();
    db.register_file_format(Arc::new(TsvFormat::pkg()));
    register_extension_package(
        ExtensionPackage::new(PKG_ATOMIC_FORMAT)
            .with_scalars(scalar_extension(PKG_ATOMIC_FORMAT))
            .with_table_source(Arc::new(PkgRangeSource::new("pkg_range_atomic")))
            .with_file_format(Arc::new(TsvFormat::pkg())),
    )
    .unwrap();
    let err = db
        .load_extension_package(PKG_ATOMIC_FORMAT)
        .unwrap_err()
        .to_string();
    assert!(err.contains("pkgtsv"), "{err}");
    assert!(!db.is_extension_loaded(PKG_ATOMIC_FORMAT));
    assert!(
        db.table_source("pkg_range_atomic").is_none(),
        "the source must not be installed after a refusal"
    );

    // (3) A scalar function name is already taken by a loaded extension.
    let owner = "wave86_pkg_scalar_owner";
    pinkdb_ext_loader::linked::register(scalar_extension(owner)).unwrap();
    let mut db = PinkDb::new();
    db.load_linked_extension(owner).unwrap();
    let colliding = LinkedFunction::new(
        format!("{owner}_double"),
        vec![ExtType::Integer],
        ExtType::Integer,
        |args: &[ExtArg<'_>]| match args.first() {
            Some(ExtArg::Integer(v)) => Ok(ExtValue::Integer(*v)),
            _ => Ok(ExtValue::Null),
        },
    )
    .unwrap();
    register_extension_package(
        ExtensionPackage::new(PKG_ATOMIC_SCALAR)
            .with_scalars(LinkedExtension::new(PKG_ATOMIC_SCALAR, vec![colliding]).unwrap())
            .with_table_source(Arc::new(PkgRangeSource::new("pkg_range_scalar")))
            .with_file_format(Arc::new(TsvFormat::pkg())),
    )
    .unwrap();
    let err = db
        .load_extension_package(PKG_ATOMIC_SCALAR)
        .unwrap_err()
        .to_string();
    assert!(err.contains("already registered"), "{err}");
    assert!(!db.is_extension_loaded(PKG_ATOMIC_SCALAR));
    assert!(db.table_source("pkg_range_scalar").is_none());
    assert!(db.file_format("pkgtsv").is_none());
}

/// A name a process-global registration already owns is out of reach, even
/// on a store constructed before that registration.
#[test]
fn activation_refuses_globally_registered_names() {
    pinkdb::table_source::register_global_table_source(Arc::new(PkgRangeSource::new(
        "pkg_global_source",
    )));
    pinkdb::filefmt::register_global_file_format(Arc::new(TsvFormat {
        name: "pkgglobalfmt",
        ext: "pkgglobalfmt",
    }));

    register_extension_package(
        ExtensionPackage::new(PKG_GLOBAL_SOURCE)
            .with_table_source(Arc::new(PkgRangeSource::new("pkg_global_source")))
            .with_file_format(Arc::new(TsvFormat {
                name: "pkg_global_ok",
                ext: "pkg_global_ok",
            })),
    )
    .unwrap();
    let mut db = PinkDb::new();
    let err = db
        .load_extension_package(PKG_GLOBAL_SOURCE)
        .unwrap_err()
        .to_string();
    assert!(err.contains("pkg_global_source"), "{err}");
    assert!(db.file_format("pkg_global_ok").is_none());

    register_extension_package(
        ExtensionPackage::new(PKG_GLOBAL_FORMAT)
            .with_table_source(Arc::new(PkgRangeSource::new("pkg_global_source_2")))
            .with_file_format(Arc::new(TsvFormat {
                name: "pkgglobalfmt",
                ext: "pkgglobalfmt",
            })),
    )
    .unwrap();
    let mut db = PinkDb::new();
    let err = db
        .load_extension_package(PKG_GLOBAL_FORMAT)
        .unwrap_err()
        .to_string();
    assert!(err.contains("pkgglobalfmt"), "{err}");
    assert!(db.table_source("pkg_global_source_2").is_none());
}

/// A durable store replays `CREATE TABLE … AS SELECT … FROM pkg_range(…)` by
/// re-reading the source, and the record carries the package's
/// `LOAD EXTENSION` prefix.
#[test]
fn durable_stores_replay_package_sources() {
    let scratch = Scratch::new("durable");
    let store = scratch.path("db.pink");
    register_range_package(PKG_DURABLE, "pkg_range");

    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    db.load_extension_package(PKG_DURABLE).unwrap();
    db.execute("CREATE TABLE t AS SELECT n FROM pkg_range(5);")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n FROM t ORDER BY n;"),
        (0..5).map(|n| vec![Value::Bigint(n)]).collect::<Vec<_>>()
    );
    db.flush_wal().unwrap();
    let wal = std::fs::read(format!("{}.wal", store.display())).unwrap();
    let prefix = format!("LOAD EXTENSION '{PKG_DURABLE}'");
    assert!(
        contains(&wal, prefix.as_bytes()),
        "a logged statement reading the package source must be prefixed with {prefix:?}"
    );
    drop(db);

    // Reopening replays the prefix (the package is still registered in this
    // process), which reactivates the source before the CTAS rebinds.
    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n FROM t ORDER BY n;"),
        (0..5).map(|n| vec![Value::Bigint(n)]).collect::<Vec<_>>()
    );
}

/// A checkpoint rolls the WAL and never persists an extension: the next
/// record touching the package source is prefixed with `LOAD EXTENSION`.
#[test]
fn a_checkpoint_makes_the_next_package_statement_reload_it() {
    let scratch = Scratch::new("checkpoint");
    let store = scratch.path("db.pink");
    register_range_package(PKG_CHECKPOINT, "pkg_range");

    let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
    db.set_allow_sql_load_extension(true);
    db.execute(&format!(
        "LOAD EXTENSION '{PKG_CHECKPOINT}'; CREATE TABLE t (n BIGINT);"
    ))
    .unwrap();
    db.execute("CHECKPOINT;").unwrap();
    db.execute("INSERT INTO t SELECT n FROM pkg_range(3);")
        .unwrap();
    db.flush_wal().unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n FROM t ORDER BY n;"),
        vec![
            vec![Value::Bigint(0)],
            vec![Value::Bigint(1)],
            vec![Value::Bigint(2)]
        ]
    );

    // The WAL is the fresh generation: only the post-checkpoint record is in
    // it, and that record must re-load the package.
    let wal = std::fs::read(format!("{}.wal", store.display())).unwrap();
    let prefix = format!("LOAD EXTENSION '{PKG_CHECKPOINT}'");
    assert!(
        contains(&wal, prefix.as_bytes()),
        "the first record after a checkpoint must re-load the package"
    );
}

/// The helper child: registers the package (this process never does) and
/// creates the durable store that used its source.
#[test]
#[ignore = "helper: run as a child process by an_unregistered_package_makes_replay_fail_cleanly"]
fn child_creates_a_package_store() {
    let Ok(store) = std::env::var(CHILD_STORE_ENV) else {
        return;
    };
    register_range_package(PKG_CHILD, "pkg_range");
    let mut db = PinkDb::open_durable(&store).expect("open the durable store");
    db.load_extension_package(PKG_CHILD)
        .expect("load the package");
    db.execute("CREATE TABLE t AS SELECT n FROM pkg_range(5);")
        .expect("create the table through the package source");
    db.flush_wal().expect("flush the WAL");
}

/// Without a registration in this process, a replayed package prefix fails
/// with the documented replay error; registering the package before the next
/// open makes the same file replay.
#[test]
fn an_unregistered_package_makes_replay_fail_cleanly() {
    let scratch = Scratch::new("unregistered");
    let store = scratch.path("db.pink");
    let exe = std::env::current_exe().expect("the test binary's own path");
    let output = Command::new(exe)
        .arg("--exact")
        .arg("child_creates_a_package_store")
        .arg("--ignored")
        .arg("--nocapture")
        .env(CHILD_STORE_ENV, &store)
        .output()
        .expect("run the helper child process");
    assert!(
        output.status.success(),
        "the helper child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let err = PinkDb::open_durable(store.to_str().unwrap())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("WAL replay refused to load the extension"),
        "{err}"
    );
    assert!(err.contains(PKG_CHILD), "{err}");

    // Registration before the next open is the documented fix.
    register_range_package(PKG_CHILD, "pkg_range");
    let mut db = PinkDb::open_durable(store.to_str().unwrap())
        .expect("a registered package makes the record replay");
    assert_eq!(
        rows(&mut db, "SELECT n FROM t ORDER BY n;"),
        (0..5).map(|n| vec![Value::Bigint(n)]).collect::<Vec<_>>()
    );
}
