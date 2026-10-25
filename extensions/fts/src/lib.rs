//! Full-text search: a DuckDB-fts-compatible BM25 index (wave 108a).
//!
//! The surface is DuckDB's, with one difference in spelling: pinkdb has no
//! `PRAGMA` and its functions are never schema-qualified, so the index name is
//! the first argument instead of the schema.
//!
//! ```sql
//! CALL create_fts_index('docs', 'id', 'title', 'body',
//!                       stemmer := 'porter', stopwords := 'english');
//! SELECT id, match_bm25('docs', id, 'relational database') AS score
//!   FROM docs ORDER BY score DESC NULLS LAST;
//! CALL drop_fts_index('docs');
//! ```
//!
//! `create_fts_index` builds a database `fts_main_<table>` of ordinary tables
//! (`meta`, `dict`, `terms`, `term_stats`, `field_stats`, `docs`) and
//! `match_bm25` scores from them, so an index saves, loads and replays with
//! the store. Like DuckDB's, the index is **static**: after an `INSERT`,
//! `UPDATE` or `DELETE` on the base table a query sees the old index until it
//! is re-created (`overwrite := 1`). Re-creating is deterministic — the same
//! rows, tokenizer and stemmer produce the same tables — which is what makes
//! WAL replay of the `CALL` reproduce the same index.
//!
//! The scoring is DuckDB's BM25: `idf(t) = ln(1 + (N - df + 0.5)/(df + 0.5))`
//! with `df` the number of documents containing the term in any field, and
//! `score(d) = Σ_t idf(t) · Σ_f tf·(k+1) / (tf + k·(1 - b + b·len/avg))`.
//! `k` defaults to 1.2 and `b` to 0.75. A document that matches no query term
//! scores NULL (DuckDB's macro sums only matching rows), so order with
//! `ORDER BY score DESC NULLS LAST`.
//!
//! `stem(word, stemmer := 'porter')` is the index's stemmer on its own, and
//! `rrf(rank, k := 60)` is Reciprocal Rank Fusion (`1/(k + rank)`), the
//! building block for fusing two ranked lists: `SUM(rrf(rank))` per row over
//! a `UNION ALL` of the per-list rankings.

mod index;
mod index_store;
pub mod porter;
mod text;

pub use index::{INDEX_DB_PREFIX, INDEX_TABLES, IndexData};
pub use text::{DEFAULT_IGNORE, ENGLISH_STOPWORDS, TextOptions, Tokenizer};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pinkdb::package::{ExtensionPackage, HostParam, HostScalar, HostStatement};
use pinkdb::{ExtType, PinkDb, PinkDbError, Result, Value};

use index_store::{index_db_for, load_index};

/// The package (and linked-registry) name: `LOAD EXTENSION 'fts'`.
pub const EXTENSION_NAME: &str = "fts";

/// The stemmer an index was built with (`stemmer := 'porter'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stemmer {
    /// Porter (1980), DuckDB's default.
    Porter,
    /// No stemming.
    None,
}

impl Stemmer {
    pub fn parse(name: &str) -> std::result::Result<Self, String> {
        match name.to_ascii_lowercase().as_str() {
            "porter" => Ok(Self::Porter),
            "none" | "" => Ok(Self::None),
            other => Err(format!(
                "unsupported stemmer \"{other}\": this build supports 'porter' and 'none'"
            )),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Porter => "porter",
            Self::None => "none",
        }
    }

    pub fn stem(self, word: &str) -> String {
        match self {
            Self::Porter => porter::stem(word),
            Self::None => word.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// The package
// ---------------------------------------------------------------------------

/// This extension as an [`ExtensionPackage`]: `stem` and `rrf` as host
/// scalars, `match_bm25` as a host scalar that reads the index, and
/// `create_fts_index` / `drop_fts_index` as host statements.
pub fn package() -> ExtensionPackage {
    ExtensionPackage::new(EXTENSION_NAME)
        .with_host_scalar(stem_scalar())
        .with_host_scalar(rrf_scalar())
        .with_host_scalar(bm25_scalar())
        .with_host_statement(create_statement())
        .with_host_statement(drop_statement())
}

/// Publish the package process-wide — the host bootstrap step a durable
/// reopen's `LOAD EXTENSION 'fts'` replay resolves against.
pub fn register_package() -> Result<()> {
    pinkdb::register_extension_package(package())
}

/// Register the package if it is not registered yet; errors are the
/// registry's.
pub fn ensure_registered() -> Result<()> {
    register_package()
}

fn stem_scalar() -> HostScalar {
    HostScalar::new(
        "stem",
        vec![
            HostParam::new("word", ExtType::Varchar),
            HostParam::new("stemmer", ExtType::Varchar)
                .default_value(Value::Varchar("porter".to_string())),
        ],
        ExtType::Varchar,
        |_db, args| {
            let word = varchar(&args[0], "stem")?;
            let stemmer = Stemmer::parse(varchar(&args[1], "stem")?)
                .map_err(|e| PinkDbError::Execution(format!("stem: {e}")))?;
            Ok(Value::Varchar(stemmer.stem(word)))
        },
    )
}

fn rrf_scalar() -> HostScalar {
    HostScalar::new(
        "rrf",
        vec![
            HostParam::new("rank", ExtType::Bigint),
            HostParam::new("k", ExtType::Bigint).default_value(Value::Bigint(60)),
        ],
        ExtType::Double,
        |_db, args| {
            let rank = bigint(&args[0], "rrf")?;
            let k = bigint(&args[1], "rrf")?;
            // Ranks are 1-based; a row with no rank has nothing to fuse.
            if rank < 1 || k < 0 {
                return Ok(Value::Null);
            }
            Ok(Value::Double(1.0 / (k as f64 + rank as f64)))
        },
    )
}

fn bm25_scalar() -> HostScalar {
    HostScalar::new(
        "match_bm25",
        vec![
            HostParam::new("index", ExtType::Varchar),
            HostParam::new("id", ExtType::Bigint),
            HostParam::new("query", ExtType::Varchar),
            HostParam::new("fields", ExtType::Varchar).default_value(Value::Null),
            HostParam::new("k", ExtType::Double).default_value(Value::Double(1.2)),
            HostParam::new("b", ExtType::Double).default_value(Value::Double(0.75)),
            HostParam::new("conjunctive", ExtType::Integer).default_value(Value::Integer(0)),
        ],
        ExtType::Double,
        match_bm25,
    )
    .reads_tables(bm25_reads)
}

/// The table `match_bm25` reads: the base table the index was built from.
///
/// The index database holds a copy of that table's text, so scoring against
/// it must need the same `SELECT` a direct read would. The binder turns this
/// into the statement's read set, which is why the index name has to be a
/// literal — the check happens once, before execution.
fn bm25_reads(args: &[Option<&Value>], default_db: &str) -> Result<Vec<(String, String)>> {
    let index = match args.first().copied().flatten() {
        Some(Value::Varchar(index)) => index.as_str(),
        _ => {
            return Err(PinkDbError::Parse(
                "match_bm25 needs a literal index name: the table it reads is checked \
                 when the statement binds"
                    .to_string(),
            ));
        }
    };
    let (db, name) = index_store::split_table(default_db, index);
    Ok(vec![(db, index_store::base_table(&name).to_string())])
}

fn match_bm25(db: &PinkDb, args: &[Value]) -> Result<Value> {
    let index = varchar(&args[0], "match_bm25")?;
    let id = bigint(&args[1], "match_bm25")?;
    let query = varchar(&args[2], "match_bm25")?;
    let fields = match &args[3] {
        Value::Null => None,
        Value::Varchar(fields) => Some(fields.as_str()),
        other => return Err(type_error("match_bm25", "fields", "VARCHAR", other)),
    };
    let k = double(&args[4], "match_bm25")?;
    let b = double(&args[5], "match_bm25")?;
    let conjunctive = integer(&args[6], "match_bm25")? != 0;
    if k < 0.0 {
        return Err(PinkDbError::Execution(
            "match_bm25: k must not be negative".to_string(),
        ));
    }
    if !(0.0..=1.0).contains(&b) {
        return Err(PinkDbError::Execution(
            "match_bm25: b must be between 0 and 1".to_string(),
        ));
    }
    let loaded = load_index(db, &index_db_for(index))?;
    let mask = loaded.field_mask(fields)?;
    let scores = loaded.scores(query, mask.as_deref(), k, b, conjunctive);
    Ok(match scores.get(&id) {
        Some(score) => Value::Double(*score),
        None => Value::Null,
    })
}

fn create_statement() -> HostStatement {
    HostStatement::new(
        "create_fts_index",
        vec![
            HostParam::new("input_table", ExtType::Varchar),
            HostParam::new("input_id", ExtType::Varchar),
            HostParam::new("input_values", ExtType::Varchar).variadic(),
            HostParam::new("stemmer", ExtType::Varchar)
                .default_value(Value::Varchar("porter".to_string())),
            HostParam::new("stopwords", ExtType::Varchar)
                .default_value(Value::Varchar("english".to_string())),
            HostParam::new("ignore", ExtType::Varchar)
                .default_value(Value::Varchar(DEFAULT_IGNORE.to_string())),
            HostParam::new("strip_accents", ExtType::Integer).default_value(Value::Integer(1)),
            HostParam::new("lower", ExtType::Integer).default_value(Value::Integer(1)),
            HostParam::new("overwrite", ExtType::Integer).default_value(Value::Integer(0)),
        ],
        index_store::create,
    )
}

fn drop_statement() -> HostStatement {
    HostStatement::new(
        "drop_fts_index",
        vec![HostParam::new("input_table", ExtType::Varchar)],
        index_store::drop_index,
    )
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

pub(crate) fn varchar<'a>(value: &'a Value, what: &str) -> Result<&'a str> {
    match value {
        Value::Varchar(text) => Ok(text),
        other => Err(type_error(what, "argument", "VARCHAR", other)),
    }
}

fn bigint(value: &Value, what: &str) -> Result<i64> {
    match value {
        Value::Bigint(v) => Ok(*v),
        other => Err(type_error(what, "argument", "BIGINT", other)),
    }
}

pub(crate) fn integer(value: &Value, what: &str) -> Result<i32> {
    match value {
        Value::Integer(v) => Ok(*v),
        other => Err(type_error(what, "argument", "INTEGER", other)),
    }
}

fn double(value: &Value, what: &str) -> Result<f64> {
    match value {
        Value::Double(v) => Ok(*v),
        other => Err(type_error(what, "argument", "DOUBLE", other)),
    }
}

fn type_error(what: &str, argument: &str, want: &str, got: &Value) -> PinkDbError {
    PinkDbError::Type(format!("{what} {argument} must be {want}, got {got:?}"))
}

// ---------------------------------------------------------------------------
// The loaded index and its cache
// ---------------------------------------------------------------------------

/// A query's cached per-document scores, keyed so a repeated query reuses them.
type Memo = Mutex<Option<(QueryKey, Arc<HashMap<i64, f64>>)>>;

/// One loaded index, plus the versions of its tables at load time.
pub(crate) struct LoadedIndex {
    pub(crate) versions: Vec<u64>,
    pub(crate) stemmer: Stemmer,
    pub(crate) tokenizer: Tokenizer,
    pub(crate) data: index::IndexData,
    /// The last query's scores: `match_bm25` is evaluated once per row, and a
    /// scan asks the same query N times.
    pub(crate) memo: Memo,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct QueryKey {
    query: String,
    fields: Option<Vec<usize>>,
    k: u64,
    b: u64,
    conjunctive: bool,
}

impl LoadedIndex {
    /// The `fields` filter as a per-field mask, validating the names.
    pub(crate) fn field_mask(&self, fields: Option<&str>) -> Result<Option<Vec<bool>>> {
        let Some(fields) = fields else {
            return Ok(None);
        };
        let mut mask = vec![false; self.data.fields.len()];
        for name in fields.split(',') {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let at = self
                .data
                .fields
                .iter()
                .position(|field| field.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    PinkDbError::Catalog(format!(
                        "match_bm25: the index has no field \"{name}\": {}",
                        self.data.fields.join(", ")
                    ))
                })?;
            mask[at] = true;
        }
        Ok(Some(mask))
    }

    /// The BM25 scores of `query`, memoized for the scan that is asking.
    pub(crate) fn scores(
        &self,
        query: &str,
        mask: Option<&[bool]>,
        k: f64,
        b: f64,
        conjunctive: bool,
    ) -> Arc<HashMap<i64, f64>> {
        let key = QueryKey {
            query: query.to_string(),
            fields: mask.map(|mask| {
                (0..mask.len())
                    .filter(|&field| mask[field])
                    .collect::<Vec<usize>>()
            }),
            k: k.to_bits(),
            b: b.to_bits(),
            conjunctive,
        };
        let mut memo = self
            .memo
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((cached, scores)) = memo.as_ref()
            && *cached == key
        {
            return scores.clone();
        }
        // The query goes through the index's tokenizer and stemmer: DuckDB
        // tokenizes the query terms with the index's settings too.
        let tokens = self.tokenizer.indexed_tokens(query, self.stemmer);
        let scores = Arc::new(self.data.score(&tokens, mask, k, b, conjunctive));
        *memo = Some((key, scores.clone()));
        scores
    }
}
