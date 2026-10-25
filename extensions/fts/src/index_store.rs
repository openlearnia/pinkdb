//! Building, dropping and loading the index tables.
//!
//! The tables are ordinary pinkdb tables, written through the store's insert
//! funnel: an index therefore saves, loads and replays with the store, and a
//! replayed `CALL create_fts_index(…)` re-creates it from the same base rows
//! in the same order (term ids in first-seen order, postings in scan order,
//! documents by ascending id).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use pinkdb::exec::QueryResult;
use pinkdb::package::HostCtx;
use pinkdb::{DataChunk, DataTable, DataType, PinkDb, PinkDbError, Result, Value};

use crate::index::{
    DICT, DOCS, DocInput, FIELD_STATS, FORMAT, INDEX_DB_PREFIX, INDEX_TABLES, IndexData, META,
    TERM_STATS, TERMS,
};
use crate::text::{ENGLISH_STOPWORDS, TextOptions, Tokenizer};
use crate::{LoadedIndex, Stemmer, integer, varchar};

/// Rows per `INSERT`: large enough to be fast, small enough that a query's
/// postings never sit in both the index and a batch of rows.
const INSERT_BATCH: usize = 8192;

/// The index database a table's index lives in.
pub(crate) fn index_db_for(table: &str) -> String {
    if table.starts_with(INDEX_DB_PREFIX) {
        table.to_string()
    } else {
        format!("{INDEX_DB_PREFIX}{table}")
    }
}

pub(crate) fn base_table(index_db: &str) -> &str {
    index_db.strip_prefix(INDEX_DB_PREFIX).unwrap_or(index_db)
}

// ---------------------------------------------------------------------------
// create_fts_index / drop_fts_index
// ---------------------------------------------------------------------------

pub(crate) fn create(ctx: &mut HostCtx<'_>, args: &[Value]) -> Result<QueryResult> {
    // Table, id, the variadic value columns, then the six options.
    if args.len() < 9 {
        return Err(PinkDbError::Execution(
            "create_fts_index needs at least one input column: \
             CALL create_fts_index(table, id, column, …)"
                .to_string(),
        ));
    }
    let options_at = args.len() - 6;
    let table_arg = varchar(&args[0], "create_fts_index")?.to_string();
    let id_name = varchar(&args[1], "create_fts_index")?.to_string();
    let mut value_names = Vec::with_capacity(options_at - 2);
    for value in &args[2..options_at] {
        value_names.push(varchar(value, "create_fts_index")?.to_string());
    }
    let stemmer = Stemmer::parse(varchar(&args[options_at], "create_fts_index")?)
        .map_err(|e| PinkDbError::Execution(format!("create_fts_index: {e}")))?;
    let stopwords = parse_stopwords(varchar(&args[options_at + 1], "create_fts_index")?)?;
    let ignore = varchar(&args[options_at + 2], "create_fts_index")?.to_string();
    let strip_accents = integer(&args[options_at + 3], "create_fts_index")? != 0;
    let lower = integer(&args[options_at + 4], "create_fts_index")? != 0;
    let overwrite = match integer(&args[options_at + 5], "create_fts_index")? {
        0 => false,
        1 => true,
        other => {
            return Err(PinkDbError::Type(format!(
                "create_fts_index: overwrite takes 0 or 1, got {other}"
            )));
        }
    };
    let options = TextOptions {
        ignore,
        lower,
        strip_accents,
        stopwords,
    };
    let tokenizer = Tokenizer::new(&options)
        .map_err(|e| PinkDbError::Execution(format!("create_fts_index: {e}")))?;

    let (base_db, base) = split_table(ctx.db_name(), &table_arg);
    let table = ctx.db().table_in(&base_db, &base)?;
    let id_at = column_at(table, &id_name, "id")?;
    match table.columns[id_at].dtype {
        DataType::Integer | DataType::Bigint => {}
        other => {
            return Err(PinkDbError::Type(format!(
                "create_fts_index: id column \"{id_name}\" is {}, not an integer",
                other.name()
            )));
        }
    }
    let mut value_at = Vec::with_capacity(value_names.len());
    for name in &value_names {
        let at = column_at(table, name, "value")?;
        if table.columns[at].dtype != DataType::Varchar {
            return Err(PinkDbError::Type(format!(
                "create_fts_index: column \"{name}\" is {}, not VARCHAR; a full-text index \
                 needs text columns",
                table.columns[at].dtype.name()
            )));
        }
        value_at.push(at);
    }
    let docs = read_docs(table, id_at, &value_at)?;
    let data = IndexData::build(&value_names, &docs, &tokenizer, stemmer)
        .map_err(|e| PinkDbError::Execution(format!("create_fts_index: {e}")))?;

    let index_db = index_db_for(&base);
    let exists = ctx.db().list_databases().iter().any(|db| db == &index_db);
    if exists && !overwrite {
        return Err(PinkDbError::Catalog(format!(
            "a full-text index for \"{base}\" already exists ({index_db}); \
             pass overwrite := 1 to re-create it"
        )));
    }
    if exists {
        ctx.execute(&format!("DROP DATABASE {}", ident(&index_db)))?;
    }
    let mut statements = 0;
    for sql in schema(&index_db) {
        ctx.execute(&sql)?;
        statements += 1;
    }
    statements += write(ctx, &index_db, &data, &options, stemmer)?;
    invalidate(ctx.db().store_id(), &index_db);
    Ok(QueryResult::Called {
        name: "create_fts_index".to_string(),
        statements,
        result: None,
        output: Vec::new(),
    })
}

pub(crate) fn drop_index(ctx: &mut HostCtx<'_>, args: &[Value]) -> Result<QueryResult> {
    let table_arg = varchar(&args[0], "drop_fts_index")?;
    let (_, base) = split_table(ctx.db_name(), table_arg);
    let index_db = index_db_for(&base);
    if !ctx.db().list_databases().iter().any(|db| db == &index_db) {
        return Err(missing_index(&index_db));
    }
    ctx.execute(&format!("DROP DATABASE {}", ident(&index_db)))?;
    invalidate(ctx.db().store_id(), &index_db);
    Ok(QueryResult::Called {
        name: "drop_fts_index".to_string(),
        statements: 1,
        result: None,
        output: Vec::new(),
    })
}

fn parse_stopwords(name: &str) -> Result<Option<&'static [&'static str]>> {
    match name.to_ascii_lowercase().as_str() {
        "english" => Ok(Some(ENGLISH_STOPWORDS)),
        "none" | "" => Ok(None),
        other => Err(PinkDbError::Execution(format!(
            "create_fts_index: unsupported stopword list \"{other}\": \
             this build ships 'english' and 'none'"
        ))),
    }
}

fn schema(index_db: &str) -> Vec<String> {
    vec![
        format!("CREATE DATABASE {}", ident(index_db)),
        format!(
            "CREATE TABLE {}.{META} (key VARCHAR, value VARCHAR)",
            ident(index_db)
        ),
        format!(
            "CREATE TABLE {}.{DICT} (term_id INTEGER, term VARCHAR)",
            ident(index_db)
        ),
        format!(
            "CREATE TABLE {}.{TERMS} (term_id INTEGER, doc_id BIGINT, field_id INTEGER, tf BIGINT)",
            ident(index_db)
        ),
        format!(
            "CREATE TABLE {}.{TERM_STATS} (term_id INTEGER, df BIGINT, idf DOUBLE)",
            ident(index_db)
        ),
        format!(
            "CREATE TABLE {}.{FIELD_STATS} (field_id INTEGER, name VARCHAR, avg_len DOUBLE)",
            ident(index_db)
        ),
        format!(
            "CREATE TABLE {}.{DOCS} (doc_id BIGINT, field_id INTEGER, len BIGINT)",
            ident(index_db)
        ),
    ]
}

/// Write the index rows; returns the number of statements it ran.
fn write(
    ctx: &mut HostCtx<'_>,
    index_db: &str,
    data: &IndexData,
    options: &TextOptions,
    stemmer: Stemmer,
) -> Result<usize> {
    let mut statements = 0;
    let meta: Vec<Vec<Value>> = [
        ("format", FORMAT),
        ("stemmer", stemmer.name()),
        (
            "stopwords",
            if options.stopwords.is_some() {
                "english"
            } else {
                "none"
            },
        ),
        ("ignore", options.ignore.as_str()),
        ("lower", if options.lower { "1" } else { "0" }),
        (
            "strip_accents",
            if options.strip_accents { "1" } else { "0" },
        ),
    ]
    .into_iter()
    .map(|(key, value)| {
        vec![
            Value::Varchar(key.to_string()),
            Value::Varchar(value.to_string()),
        ]
    })
    .collect();
    push(ctx, index_db, META, meta, &mut statements)?;

    let mut field_rows = Vec::with_capacity(data.fields.len());
    for (field, name) in data.fields.iter().enumerate() {
        field_rows.push(vec![
            Value::Integer(field as i32),
            Value::Varchar(name.clone()),
            Value::Double(data.avg_len[field]),
        ]);
    }
    push(ctx, index_db, FIELD_STATS, field_rows, &mut statements)?;

    let mut dict_rows = Vec::with_capacity(data.terms.len());
    let mut stat_rows = Vec::with_capacity(data.terms.len());
    for (term, text) in data.terms.iter().enumerate() {
        dict_rows.push(vec![
            Value::Integer(term as i32),
            Value::Varchar(text.clone()),
        ]);
        let df = data.postings[term]
            .iter()
            .map(|p| p.doc)
            .collect::<Vec<_>>();
        // `df` was counted per document at build time; recount from the
        // postings so the table is what a reader would derive anyway.
        let mut docs: Vec<i64> = df;
        docs.sort_unstable();
        docs.dedup();
        stat_rows.push(vec![
            Value::Integer(term as i32),
            Value::Bigint(docs.len() as i64),
            Value::Double(data.idf[term]),
        ]);
    }
    push(ctx, index_db, DICT, dict_rows, &mut statements)?;
    push(ctx, index_db, TERM_STATS, stat_rows, &mut statements)?;

    let mut doc_ids: Vec<&i64> = data.doc_lens.keys().collect();
    doc_ids.sort_unstable();
    let mut doc_rows = Vec::new();
    for doc in doc_ids {
        let lens = &data.doc_lens[doc];
        for (field, len) in lens.iter().enumerate() {
            doc_rows.push(vec![
                Value::Bigint(*doc),
                Value::Integer(field as i32),
                Value::Bigint(*len as i64),
            ]);
            maybe_flush(ctx, index_db, DOCS, &mut doc_rows, &mut statements)?;
        }
    }
    push(ctx, index_db, DOCS, doc_rows, &mut statements)?;

    let mut rows = Vec::new();
    for (term, postings) in data.postings.iter().enumerate() {
        for posting in postings {
            rows.push(vec![
                Value::Integer(term as i32),
                Value::Bigint(posting.doc),
                Value::Integer(posting.field as i32),
                Value::Bigint(posting.tf as i64),
            ]);
            maybe_flush(ctx, index_db, TERMS, &mut rows, &mut statements)?;
        }
    }
    push(ctx, index_db, TERMS, rows, &mut statements)?;
    Ok(statements)
}

fn maybe_flush(
    ctx: &mut HostCtx<'_>,
    index_db: &str,
    table: &str,
    rows: &mut Vec<Vec<Value>>,
    statements: &mut usize,
) -> Result<()> {
    if rows.len() >= INSERT_BATCH {
        push(ctx, index_db, table, std::mem::take(rows), statements)?;
    }
    Ok(())
}

fn push(
    ctx: &mut HostCtx<'_>,
    index_db: &str,
    table: &str,
    rows: Vec<Vec<Value>>,
    statements: &mut usize,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    ctx.insert_rows(index_db, table, rows)?;
    *statements += 1;
    Ok(())
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

type Cache = Mutex<HashMap<(u64, String), Arc<LoadedIndex>>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock() -> std::sync::MutexGuard<'static, HashMap<(u64, String), Arc<LoadedIndex>>> {
    cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The loaded index for `index_db`, from the cache when no index table changed
/// since it was built.
pub(crate) fn load_index(db: &PinkDb, index_db: &str) -> Result<Arc<LoadedIndex>> {
    let mut versions = Vec::with_capacity(INDEX_TABLES.len());
    for table in INDEX_TABLES {
        match db.table_version_in(index_db, table) {
            Ok(version) => versions.push(version),
            Err(_) => return Err(missing_index(index_db)),
        }
    }
    let key = (db.store_id(), index_db.to_string());
    if let Some(hit) = lock().get(&key).cloned()
        && hit.versions == versions
    {
        return Ok(hit);
    }
    let loaded = Arc::new(read(db, index_db, versions)?);
    let mut cache = lock();
    // The cache is keyed by store and invalidated by table versions, so the
    // only way it grows is many distinct stores in one process (a test suite,
    // a server reopening files). A cap keeps that bounded.
    if cache.len() >= 64 {
        cache.clear();
    }
    cache.insert(key, loaded.clone());
    Ok(loaded)
}

/// Forget `index_db`'s entry (create/drop changed the tables).
pub(crate) fn invalidate(store_id: u64, index_db: &str) {
    lock().remove(&(store_id, index_db.to_string()));
}

fn read(db: &PinkDb, index_db: &str, versions: Vec<u64>) -> Result<LoadedIndex> {
    let mut meta: HashMap<String, String> = HashMap::new();
    scan(db, index_db, META, |chunk, row| {
        let key = text(chunk, 0, row)?;
        let value = text(chunk, 1, row)?;
        meta.insert(key, value);
        Ok(())
    })?;
    match meta.get("format") {
        Some(format) if format == FORMAT => {}
        Some(other) => {
            return Err(PinkDbError::Catalog(format!(
                "full-text index {index_db} has format \"{other}\"; this build writes {FORMAT} \
                 — re-create it with CALL create_fts_index(…, overwrite := 1)"
            )));
        }
        None => {
            return Err(PinkDbError::Catalog(format!(
                "full-text index {index_db} has no meta.format row; re-create it"
            )));
        }
    }
    let stemmer = Stemmer::parse(meta.get("stemmer").map(String::as_str).unwrap_or("porter"))
        .map_err(|e| PinkDbError::Catalog(format!("full-text index {index_db}: {e}")))?;
    let options = TextOptions {
        ignore: meta.get("ignore").cloned().unwrap_or_default(),
        lower: meta.get("lower").map(String::as_str) != Some("0"),
        strip_accents: meta.get("strip_accents").map(String::as_str) != Some("0"),
        stopwords: match meta.get("stopwords").map(String::as_str) {
            Some("none") => None,
            _ => Some(ENGLISH_STOPWORDS),
        },
    };
    let tokenizer = Tokenizer::new(&options)
        .map_err(|e| PinkDbError::Catalog(format!("full-text index {index_db}: {e}")))?;

    let mut fields: Vec<String> = Vec::new();
    let mut avg_len: Vec<f64> = Vec::new();
    scan(db, index_db, FIELD_STATS, |chunk, row| {
        let field = int(chunk, 0, row)?;
        let name = text(chunk, 1, row)?;
        if field as usize != fields.len() {
            return Err(corrupt(index_db, "field_stats is not in field order"));
        }
        fields.push(name);
        avg_len.push(double(chunk, 2, row)?);
        Ok(())
    })?;

    let mut doc_lens: HashMap<i64, Vec<u64>> = HashMap::new();
    scan(db, index_db, DOCS, |chunk, row| {
        let doc = int(chunk, 0, row)?;
        let field = int(chunk, 1, row)? as usize;
        let len = int(chunk, 2, row)?;
        if field >= fields.len() || len < 0 {
            return Err(corrupt(index_db, "docs has a field or length out of range"));
        }
        let lens = doc_lens.entry(doc).or_insert_with(|| vec![0; fields.len()]);
        lens[field] = len as u64;
        Ok(())
    })?;

    let mut terms: Vec<String> = Vec::new();
    scan(db, index_db, DICT, |chunk, row| {
        let term = int(chunk, 0, row)?;
        if term as usize != terms.len() {
            return Err(corrupt(index_db, "dict is not in term-id order"));
        }
        terms.push(text(chunk, 1, row)?);
        Ok(())
    })?;
    let mut term_ids: HashMap<String, u32> = HashMap::with_capacity(terms.len());
    for (term, text) in terms.iter().enumerate() {
        term_ids.insert(text.clone(), term as u32);
    }

    let mut idf = vec![0.0f64; terms.len()];
    scan(db, index_db, TERM_STATS, |chunk, row| {
        let term = int(chunk, 0, row)? as usize;
        if term >= idf.len() {
            return Err(corrupt(index_db, "term_stats names an unknown term"));
        }
        idf[term] = double(chunk, 2, row)?;
        Ok(())
    })?;

    let mut postings: Vec<Vec<crate::index::Posting>> = vec![Vec::new(); terms.len()];
    scan(db, index_db, TERMS, |chunk, row| {
        let term = int(chunk, 0, row)? as usize;
        let doc = int(chunk, 1, row)?;
        let field = int(chunk, 2, row)? as usize;
        let tf = int(chunk, 3, row)?;
        if term >= postings.len() || field >= fields.len() || tf < 0 {
            return Err(corrupt(
                index_db,
                "terms has a term, field or count out of range",
            ));
        }
        postings[term].push(crate::index::Posting {
            doc,
            field: field as u32,
            tf: tf as u64,
        });
        Ok(())
    })?;

    Ok(LoadedIndex {
        versions,
        stemmer,
        tokenizer,
        memo: Mutex::new(None),
        data: IndexData {
            fields,
            num_docs: doc_lens.len() as u64,
            avg_len,
            terms,
            term_ids,
            idf,
            postings,
            doc_lens,
        },
    })
}

fn corrupt(index_db: &str, what: &str) -> PinkDbError {
    PinkDbError::Catalog(format!(
        "full-text index {index_db} is corrupt: {what} — re-create it with \
         CALL create_fts_index(…, overwrite := 1)"
    ))
}

fn missing_index(index_db: &str) -> PinkDbError {
    PinkDbError::Catalog(format!(
        "no full-text index for \"{}\" ({index_db}); create one with \
         CALL create_fts_index('<table>', '<id>', '<column>', …)",
        base_table(index_db)
    ))
}

// ---------------------------------------------------------------------------
// Table reading
// ---------------------------------------------------------------------------

pub(crate) fn split_table(default_db: &str, name: &str) -> (String, String) {
    match name.rsplit_once('.') {
        Some((db, table)) => (db.to_string(), table.to_string()),
        None => (default_db.to_string(), name.to_string()),
    }
}

fn column_at(table: &DataTable, name: &str, what: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| {
            PinkDbError::Catalog(format!(
                "create_fts_index: table \"{}\" has no {what} column \"{name}\"",
                table.name
            ))
        })
}

fn read_docs(table: &DataTable, id_at: usize, value_at: &[usize]) -> Result<Vec<DocInput>> {
    let mut docs = Vec::with_capacity(table.num_rows);
    let mut chunk = 0;
    while let Some(rows) = table.scan_chunk(chunk) {
        for row in 0..rows.num_rows {
            let id = match rows.columns[id_at].get(row) {
                Value::Integer(id) => id as i64,
                Value::Bigint(id) => id,
                other => {
                    return Err(PinkDbError::Type(format!(
                        "create_fts_index: id column \"{}\" holds {other:?}, not an integer",
                        table.columns[id_at].name
                    )));
                }
            };
            let mut texts = Vec::with_capacity(value_at.len());
            for &at in value_at {
                texts.push(match rows.columns[at].get(row) {
                    Value::Varchar(text) => Some(text),
                    _ => None,
                });
            }
            docs.push(DocInput { id, texts });
        }
        chunk += 1;
    }
    Ok(docs)
}

fn scan(
    db: &PinkDb,
    index_db: &str,
    table: &str,
    mut row: impl FnMut(&DataChunk, usize) -> Result<()>,
) -> Result<()> {
    let table = db
        .table_in(index_db, table)
        .map_err(|_| missing_index(index_db))?;
    let mut chunk = 0;
    while let Some(rows) = table.scan_chunk(chunk) {
        for index in 0..rows.num_rows {
            row(&rows, index)?;
        }
        chunk += 1;
    }
    Ok(())
}

fn value(chunk: &DataChunk, column: usize, row: usize) -> Value {
    chunk
        .columns
        .get(column)
        .map(|vector| vector.get(row))
        .unwrap_or(Value::Null)
}

fn int(chunk: &DataChunk, column: usize, row: usize) -> Result<i64> {
    match value(chunk, column, row) {
        Value::Integer(v) => Ok(v as i64),
        Value::Bigint(v) => Ok(v),
        other => Err(PinkDbError::Type(format!(
            "index column {column} holds {other:?}, not an integer"
        ))),
    }
}

fn double(chunk: &DataChunk, column: usize, row: usize) -> Result<f64> {
    match value(chunk, column, row) {
        Value::Double(v) => Ok(v),
        other => Err(PinkDbError::Type(format!(
            "index column {column} holds {other:?}, not a DOUBLE"
        ))),
    }
}

fn text(chunk: &DataChunk, column: usize, row: usize) -> Result<String> {
    match value(chunk, column, row) {
        Value::Varchar(v) => Ok(v),
        other => Err(PinkDbError::Type(format!(
            "index column {column} holds {other:?}, not VARCHAR"
        ))),
    }
}

/// Quote an identifier only when it needs it, with `"` doubling.
fn ident(name: &str) -> String {
    let simple = !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if simple {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
