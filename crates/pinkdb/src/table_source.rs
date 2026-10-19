//! Host-registered table sources: the integration point for file readers
//! (`read_parquet`, `FROM 'file.parquet'`, and the `read_csv('url')` /
//! arrow work waves 71/75 extend).
//!
//! ## Why this is not an extension
//!
//! The extension host (`crate::ext`, ABI v1) registers **scalar functions**:
//! one callback per row over ABI values. A file reader is none of that — it
//! needs a schema at *bind* time, a row count, a projection, pushdown
//! predicates, and it answers with whole chunks. The two surfaces are
//! disjoint by design, so a table source is an ordinary Rust trait object the
//! **host** registers on a store, exactly like an attached lake
//! ([`crate::attached`]): the engine stays dependency-free, and the reader
//! (e.g. `pinkdb-parquet`) depends on the engine, never the other way
//! around.
//!
//! ## What the engine does with a source
//!
//! - The parser accepts two new FROM shapes: a table-function call
//!   (`FROM read_parquet('x.parquet', union_by_name := true)`) and a bare
//!   string literal (`FROM 'x.parquet'`, DuckDB's *replacement scan*). The
//!   binder offers the call to every registered source
//!   ([`TableSource::bind`]); the first that answers with a schema owns the
//!   node.
//! - The plan carries the source's registry **name** plus the call's
//!   arguments ([`crate::plan::LogicalPlan::TableFunction`]) — not the trait
//!   object — so plans stay `Clone + PartialEq + Debug` like every other
//!   node. Execution resolves the name again through
//!   [`PinkDb::table_source`].
//! - Projection pushdown and filter pushdown reach the source the same way
//!   they reach an attached lake: the statement's projection and the simple
//!   `WHERE` conjuncts travel as `Option<&[usize]>` and
//!   [`PrunePredicate`](crate::attached::PrunePredicate)s. Both are *hints*:
//!   the engine still executes the full filter above the scan, so a source
//!   may ignore either one (it just decodes more).
//! - [`TableSource::scan`] returns engine chunks plus the counters the
//!   engine accumulates into [`PinkDb::table_source_stats`] (files read,
//!   row groups read/pruned, columns decoded) — the visible evidence of
//!   pushdown, the same shape `.lake attached` reports for lakes.
//!
//! Sources are process-level, like extensions and attached lakes: they are
//! not part of a snapshot, not persisted, and not re-established by
//! reopening — register them again (or use
//! [`register_global_table_source`], the host-bootstrap step a durable store
//! needs; see the durability note below).
//!
//! ### Durability: file reads replay by **re-reading** (DuckDB's rule)
//!
//! A file is an external input, not session-local state. A logged statement
//! that reads one (`CREATE TABLE t AS SELECT * FROM read_parquet('f')`,
//! `INSERT INTO t SELECT * FROM 'f.arrow'`) replays by re-reading that file,
//! exactly like DuckDB:
//!
//! - recovery runs inside [`PinkDb::open_durable`](crate::PinkDb::open_durable),
//!   before the caller holds the store, so the source must already be in the
//!   store's registry: [`register_global_table_source`] seeds every store
//!   constructed afterwards (the `pinkdb_parquet::ensure_registered()` /
//!   `pinkdb_arrow::ensure_registered()` bootstrap the CLI performs at
//!   startup);
//! - a replay build **without** the source fails hard at bind (unknown table
//!   function / no source claims the path) — never a silent drop;
//! - a file **deleted** between the original run and replay is a hard
//!   recovery error; a file that **changed** replays its new content — the
//!   documented caveat of the re-read policy, matching DuckDB.

use std::fmt;
use std::sync::Arc;

use crate::attached::PrunePredicate;
use crate::storage::PinkDb;
use crate::types::{DataType, Result, Value};
use crate::vector::DataChunk;

/// One argument of a table-function call: a literal value, optionally named
/// (`read_parquet('x', union_by_name := TRUE)` → the second argument's `name`
/// is `Some("union_by_name")`).
///
/// Only literals reach a source — the binder evaluates the argument
/// expressions at bind time and refuses anything that is not a constant
/// (a placeholder, a column, an arithmetic expression). Named arguments are
/// matched by the source; unknown names are the source's to refuse.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFunctionArg {
    /// `Some(name)` for `name := value` / `name => value`, `None` for a
    /// positional argument.
    pub name: Option<String>,
    pub value: Value,
}

impl TableFunctionArg {
    /// The argument's name, lowercased (named arguments are matched
    /// case-insensitively like every other SQL name).
    pub fn name_lower(&self) -> Option<String> {
        self.name.as_ref().map(|n| n.to_ascii_lowercase())
    }
}

impl fmt::Display for TableFunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.name {
            Some(name) => write!(f, "{name} := {}", display_value(&self.value)),
            None => write!(f, "{}", display_value(&self.value)),
        }
    }
}

/// Render one argument value for `EXPLAIN` and error messages. Strings are
/// quoted; everything else uses the engine's own display shape.
fn display_value(value: &Value) -> String {
    match value {
        Value::Varchar(s) => format!("'{s}'"),
        other => format!("{other}"),
    }
}

/// What one [`TableSource::scan`] did, for the pushdown counters and for the
/// chunks the engine executes.
///
/// `chunks` follow the scan contract every leaf scan implements: full-width
/// columns in the *bound schema's* order, chunk-aligned, with a zero-length
/// placeholder ([`DataChunk::projected`]) for every column the projection did
/// not decode. A source may decode more than the projection asked for; it
/// must never decode fewer columns than the projection named.
#[derive(Debug, Default)]
pub struct TableScanOutput {
    /// The decoded chunks, in scan order.
    pub chunks: Vec<DataChunk>,
    /// Data files read (after globbing and any file-level pruning).
    pub files: u64,
    /// Row groups visited (surviving files' row groups).
    pub row_groups: u64,
    /// Row groups skipped through statistics.
    pub row_groups_pruned: u64,
    /// Column chunks decoded.
    pub columns_decoded: u64,
}

/// Per-source scan counters, reported by the source itself
/// ([`TableSource::stats`]) across a session's statements
/// ([`PinkDb::table_source_stats`]).
///
/// The source owns its counters because the executor holds a shared
/// `&PinkDb` and the store is cloned for transaction snapshots — a counter
/// field on the store would fork per snapshot. A source may leave every
/// counter zero when it tracks nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSourceStats {
    /// The source's registry name.
    pub name: String,
    /// `scan` calls the engine executed.
    pub scans: u64,
    /// Rows returned across those scans.
    pub rows: u64,
    pub files: u64,
    pub row_groups: u64,
    pub row_groups_pruned: u64,
    pub columns_decoded: u64,
}

/// The FROM-position call a [`TableSource`] binds and executes.
#[derive(Debug, Clone, PartialEq)]
pub enum TableFunctionCall {
    /// A table-function call: `read_parquet('x.parquet', union_by_name := TRUE)`.
    Function {
        /// The name as written (matched case-insensitively by sources).
        name: String,
        args: Vec<TableFunctionArg>,
    },
    /// DuckDB's *replacement scan*: a bare string literal in FROM position
    /// (`FROM 'file.parquet'`).
    Path(String),
}

impl TableFunctionCall {
    /// The call's function name, lowercased; `None` for a replacement scan.
    pub fn function_name(&self) -> Option<String> {
        match self {
            TableFunctionCall::Function { name, .. } => Some(name.to_ascii_lowercase()),
            TableFunctionCall::Path(_) => None,
        }
    }

    /// The name a `EXPLAIN`/error message shows: the function name (as
    /// written) or the path.
    pub fn display_name(&self) -> String {
        match self {
            TableFunctionCall::Function { name, .. } => name.clone(),
            TableFunctionCall::Path(path) => format!("'{path}'"),
        }
    }
}

/// A host-registered table source: a set of FROM-position functions plus the
/// replacement-scan hook, and the scan they execute.
///
/// The engine never interprets a call's arguments — the source owns its
/// dialect. It must be deterministic in one direction: `scan` must produce
/// exactly the columns `bind` promised (the engine checks column count and
/// dtypes and refuses a mismatch).
pub trait TableSource: fmt::Debug + Send + Sync {
    /// The registry name (`"parquet"`), matched case-insensitively by
    /// [`PinkDb::register_table_source`] and shown in `EXPLAIN`.
    fn name(&self) -> &str;

    /// Bind `call`: `Ok(Some(schema))` when this source serves it, `Ok(None)`
    /// when it does not (the binder then asks the next source and finally
    /// reports the engine's own unknown-function error), `Err` for a call
    /// the source serves but refuses (a bad argument, a missing file, an
    /// unsupported option).
    ///
    /// The returned schema is the full output schema — including any virtual
    /// columns the call requested — in output order.
    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>>;

    /// Execute a bound call.
    ///
    /// `projection` is `Some(columns)` when the statement's referenced-column
    /// analysis bounded what is read (ascending indices into the bound
    /// schema), `None` when every column is needed. `filters` are the simple
    /// conjuncts the planner proved over the scan; a source may use them for
    /// row-group/file pruning and must ignore the ones it cannot interpret —
    /// the engine applies the full predicate above the scan regardless.
    fn scan(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        filters: &[PrunePredicate],
    ) -> Result<TableScanOutput>;

    /// The source's scan counters so far (files read, row groups read and
    /// pruned, columns decoded, rows returned). The default reports zeros: a
    /// source may track nothing. Read though
    /// [`PinkDb::table_source_stats`], which stamps the registry name.
    fn stats(&self) -> TableSourceStats {
        TableSourceStats::default()
    }

    /// Zero the counters (the CLI's `.parquet reset`; a source that tracks
    /// nothing may ignore it).
    fn reset_stats(&self) {}
}

/// Register a table source **process-wide** (idempotent: same-name
/// registration replaces).
///
/// Every store *constructed after* this call starts with the source in its
/// own registry. That timing is what makes a durable store replay: recovery
/// runs inside [`PinkDb::open_durable`](crate::PinkDb::open_durable), before
/// the caller holds the store, so a host that will replay file-reading
/// records must register here first — the host-bootstrap step
/// `pinkdb_parquet::ensure_registered()` / `pinkdb_arrow::ensure_registered()`
/// performs for the CLI (the same shape as a linked scalar extension's
/// registration). A store's own [`PinkDb::register_table_source`] then adds
/// or replaces sources for that store alone.
pub fn register_global_table_source(source: Arc<dyn TableSource>) {
    let mut sources = global_sources()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = source.name().to_ascii_lowercase();
    sources.retain(|s| s.name().to_ascii_lowercase() != key);
    sources.push(source);
}

/// The process-global sources a store starts with (see
/// [`register_global_table_source`]).
pub(crate) fn seeded_global_table_sources() -> Vec<Arc<dyn TableSource>> {
    global_sources()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn global_sources() -> &'static std::sync::Mutex<Vec<Arc<dyn TableSource>>> {
    static GLOBAL: std::sync::OnceLock<std::sync::Mutex<Vec<Arc<dyn TableSource>>>> =
        std::sync::OnceLock::new();
    GLOBAL.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

impl PinkDb {
    /// Register a table source (idempotent by name: registering a second
    /// source under a loaded name replaces it, so a host can refresh a
    /// source's configuration). Sources are process-level: never persisted,
    /// never part of a snapshot, and gone after a reopen — register them
    /// again.
    pub fn register_table_source(&mut self, source: Arc<dyn TableSource>) {
        let name = source.name().to_ascii_lowercase();
        if let Some(existing) = self
            .table_sources
            .iter_mut()
            .find(|s| s.name().eq_ignore_ascii_case(&name))
        {
            *existing = source;
            return;
        }
        self.table_sources.push(source);
    }

    /// Remove every registered table source (and their counters). A
    /// statement that later names one fails at bind time with the engine's
    /// unknown-function error.
    pub fn clear_table_sources(&mut self) {
        self.table_sources.clear();
    }

    /// The registered table sources, in registration order.
    pub fn table_sources(&self) -> Vec<&Arc<dyn TableSource>> {
        self.table_sources.iter().collect()
    }

    /// The source registered under `name`, case-insensitively.
    pub fn table_source(&self, name: &str) -> Option<&Arc<dyn TableSource>> {
        self.table_sources
            .iter()
            .find(|s| s.name().eq_ignore_ascii_case(name))
    }

    /// Per-source scan counters, in registration order, each stamped with the
    /// source's registry name — the visible evidence of projection and filter
    /// pushdown (`pinkdb-cli` prints them with `.parquet`).
    pub fn table_source_stats(&self) -> Vec<TableSourceStats> {
        self.table_sources
            .iter()
            .map(|s| TableSourceStats {
                name: s.name().to_string(),
                ..s.stats()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryResult;
    use crate::types::PinkDbError;
    use crate::vector::Vector;

    /// The rows of one `SELECT` result.
    fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        let mut out = db.execute(sql).expect("query");
        assert_eq!(out.len(), 1);
        match out.pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// A tiny in-memory source: `values(n)` returns one INTEGER column with
    /// `0..n`, `fake:x` a replacement scan that yields the path text. Every
    /// scan records what it was asked for, and the source keeps its own
    /// counters like a real one.
    #[derive(Debug, Default)]
    struct FakeSource {
        scans: std::sync::Mutex<Vec<(Option<Vec<usize>>, usize)>>,
        scans_count: std::sync::atomic::AtomicU64,
        rows_count: std::sync::atomic::AtomicU64,
    }

    fn int_arg(args: &[TableFunctionArg], idx: usize) -> Result<i64> {
        match args.get(idx).map(|a| &a.value) {
            Some(Value::Bigint(n)) => Ok(*n),
            Some(Value::Integer(n)) => Ok(*n as i64),
            other => Err(PinkDbError::Type(format!("bad argument {other:?}"))),
        }
    }

    impl TableSource for FakeSource {
        fn name(&self) -> &str {
            "fake"
        }

        fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
            let schema = match call {
                TableFunctionCall::Function { name, .. } if name.eq_ignore_ascii_case("fvals") => {
                    vec![("v".to_string(), DataType::Integer)]
                }
                TableFunctionCall::Path(path) if path.starts_with("fake:") => {
                    vec![("p".to_string(), DataType::Varchar)]
                }
                _ => return Ok(None),
            };
            Ok(Some(schema))
        }

        fn scan(
            &self,
            call: &TableFunctionCall,
            projection: Option<&[usize]>,
            _filters: &[PrunePredicate],
        ) -> Result<TableScanOutput> {
            let mut col = Vector::with_capacity(DataType::Integer, 8);
            match call {
                TableFunctionCall::Path(path) => {
                    col = Vector::with_capacity(DataType::Varchar, 1);
                    col.push_valid(Value::Varchar(path.clone()))?;
                }
                TableFunctionCall::Function { args, .. } => {
                    let n = int_arg(args, 0)?;
                    self.scans
                        .lock()
                        .unwrap()
                        .push((projection.map(|p| p.to_vec()), n as usize));
                    for i in 0..n {
                        col.push_valid(Value::Integer(i as i32))?;
                    }
                }
            }
            self.scans_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.rows_count.fetch_add(
                match call {
                    TableFunctionCall::Path(_) => 1,
                    TableFunctionCall::Function { args, .. } => int_arg(args, 0)? as u64,
                },
                std::sync::atomic::Ordering::Relaxed,
            );
            Ok(TableScanOutput {
                chunks: vec![DataChunk::new(vec![col])?],
                files: 1,
                row_groups: 1,
                row_groups_pruned: 0,
                columns_decoded: 1,
            })
        }

        fn stats(&self) -> TableSourceStats {
            TableSourceStats {
                name: String::new(),
                scans: self.scans_count.load(std::sync::atomic::Ordering::Relaxed),
                rows: self.rows_count.load(std::sync::atomic::Ordering::Relaxed),
                files: 1,
                row_groups: 1,
                row_groups_pruned: 0,
                columns_decoded: 1,
            }
        }

        fn reset_stats(&self) {
            self.scans_count
                .store(0, std::sync::atomic::Ordering::Relaxed);
            self.rows_count
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn store_with_fake() -> (PinkDb, Arc<FakeSource>) {
        let mut db = PinkDb::new();
        let fake = Arc::new(FakeSource::default());
        db.register_table_source(fake.clone());
        (db, fake)
    }

    /// The rows of one `SELECT` result.
    fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        let mut out = db.execute(sql).expect("query");
        assert_eq!(out.len(), 1);
        match out.pop().unwrap() {
            crate::exec::QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn table_function_binds_and_scans() {
        let (mut db, _) = store_with_fake();
        let got = rows(&mut db, "SELECT v FROM fvals(3) ORDER BY v;");
        assert_eq!(got.len(), 3);
        assert_eq!(got[0][0], Value::Integer(0));

        let stats = db.table_source_stats();
        let fake = stats
            .iter()
            .find(|s| s.name == "fake")
            .expect("the fake source is registered");
        assert_eq!(fake.scans, 1);
        assert_eq!(fake.rows, 3);
        assert_eq!(fake.files, 1);
    }

    #[test]
    fn table_function_sees_projection_and_filters() {
        let (mut db, fake) = store_with_fake();
        rows(&mut db, "SELECT v FROM fvals(5) WHERE v > 1;");
        let scans = fake.scans.lock().unwrap().clone();
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].0.as_deref(), Some(&[0usize][..]));
    }

    #[test]
    fn replacement_scan_binds_paths() {
        let (mut db, _) = store_with_fake();
        let got = rows(&mut db, "SELECT p FROM 'fake:hello';");
        assert_eq!(got[0][0], Value::Varchar("fake:hello".to_string()));
        // Qualified references use the scope name the binder derives.
        let got = rows(&mut db, "SELECT fake.p FROM 'fake:hello' AS fake;");
        assert_eq!(got[0][0], Value::Varchar("fake:hello".to_string()));
        // `AT` is only valid after a table name (the landed file-source rule).
        let err = db
            .execute("SELECT * FROM 'fake:hello' AT (VERSION => 1);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("AT is only valid after a table name"), "{err}");
    }

    #[test]
    fn unknown_function_is_the_engines_error() {
        let (mut db, _) = store_with_fake();
        let err = db.execute("SELECT * FROM nosuch(1);").unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("nosuch"),
            "error should name the function: {text}"
        );
    }

    #[test]
    fn unclaimed_path_is_a_clean_error() {
        let (mut db, _) = store_with_fake();
        let err = db.execute("SELECT * FROM 'other:thing';").unwrap_err();
        let text = err.to_string();
        assert!(text.contains("other:thing"), "{text}");
    }

    #[test]
    fn named_arguments_reach_the_source() {
        let (mut db, _) = store_with_fake();
        // The fake ignores the option; what matters is that a named literal
        // argument binds and reaches the source without an error.
        let got = rows(&mut db, "SELECT v FROM fvals(2, show_more := TRUE);");
        assert_eq!(got.len(), 2);
        let got = rows(&mut db, "SELECT v FROM fvals(2, show_more => TRUE);");
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn non_literal_arguments_are_refused() {
        let (mut db, _) = store_with_fake();
        let err = db.execute("SELECT v FROM fvals(1 + 1);").unwrap_err();
        assert!(err.to_string().contains("literal"), "{err}");
        let err = db.execute("SELECT v FROM fvals(1, x := v);").unwrap_err();
        assert!(err.to_string().contains("literal"), "{err}");
    }

    #[test]
    fn explain_names_the_source_and_options() {
        let (mut db, _) = store_with_fake();
        let lines = rows(&mut db, "EXPLAIN SELECT v FROM fvals(3) WHERE v > 1;");
        let text: String = lines
            .iter()
            .map(|r| match &r[0] {
                Value::Varchar(s) => s.clone(),
                v => format!("{v:?}"),
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("fvals(3)"), "{text}");
        assert!(text.contains("fake"), "{text}");
    }

    /// Durability: a logged statement that reads a table source replays by
    /// **re-reading** the file (DuckDB's rule). The source must be registered
    /// process-globally before the store is constructed — recovery replays
    /// inside `open_durable`. A file deleted before replay is a hard recovery
    /// error, never a silent drop. This is the only test in the engine binary
    /// that touches the process-global source registry; its name is unique so
    /// the store-level tests above stay order-independent.
    #[test]
    fn durable_stores_replay_file_reads_by_re_reading() {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-table-source-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("src.walsrc");
        let store = dir.join("store.pink");
        // The source reads `line1:line2:…` as one INTEGER column, the values
        // being the line numbers, so a re-read is observable.
        crate::table_source::register_global_table_source(Arc::new(FileFakeSource));
        std::fs::write(&file, "a\nb\nc").unwrap();

        // A durable store (it has a WAL from the start, so the file-reading
        // INSERT is logged) — the *reopen* has to replay it, re-reading the
        // file.
        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE dst (a INTEGER);").unwrap();
        db.execute(&format!(
            "INSERT INTO dst SELECT v FROM walsrc('{}');",
            file.display()
        ))
        .unwrap();
        assert_eq!(
            query(&mut db, "SELECT a FROM dst ORDER BY a;"),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(3)]
            ]
        );
        drop(db);

        // Recovery re-reads the file (the store was constructed after the
        // global registration, so replay resolves the source).
        let mut db = PinkDb::open_durable(store.to_str().unwrap()).unwrap();
        assert_eq!(
            query(&mut db, "SELECT a FROM dst ORDER BY a;"),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(3)]
            ]
        );
        drop(db);

        // A file that vanished before replay is a hard error, never a silent
        // drop.
        std::fs::remove_file(&file).unwrap();
        let err = PinkDb::open_durable(store.to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("walsrc") || err.contains("cannot open"),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A line-counting source registered process-globally by the durability
    /// test: `walsrc('file')` yields one INTEGER column, one row per line.
    #[derive(Debug)]
    struct FileFakeSource;

    impl TableSource for FileFakeSource {
        fn name(&self) -> &str {
            "walsrc"
        }

        fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
            match call.function_name().as_deref() {
                Some("walsrc") => Ok(Some(vec![("v".to_string(), DataType::Integer)])),
                _ => Ok(None),
            }
        }

        fn scan(
            &self,
            call: &TableFunctionCall,
            _projection: Option<&[usize]>,
            _filters: &[crate::attached::PrunePredicate],
        ) -> Result<TableScanOutput> {
            let path = match call {
                TableFunctionCall::Function { args, .. } => match args.first().map(|a| &a.value) {
                    Some(Value::Varchar(p)) => p.clone(),
                    other => {
                        return Err(PinkDbError::Type(format!("bad argument {other:?}")));
                    }
                },
                TableFunctionCall::Path(p) => p.clone(),
            };
            let text = std::fs::read_to_string(&path)
                .map_err(|e| PinkDbError::Execution(format!("cannot open \"{path}\": {e}")))?;
            let lines = text.lines().count().max(1);
            let mut col = Vector::with_capacity(DataType::Integer, lines);
            for i in 0..lines {
                col.push_valid(Value::Integer(i as i32 + 1))?;
            }
            Ok(TableScanOutput {
                chunks: vec![DataChunk::new(vec![col])?],
                files: 1,
                row_groups: 0,
                row_groups_pruned: 0,
                columns_decoded: 1,
            })
        }
    }
}
