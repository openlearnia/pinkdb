//! The index: build, on-disk shape, load and BM25 scoring.
//!
//! An index is a pinkdb **database** (`fts_main_<table>`) holding ordinary
//! tables, DuckDB's shape: `dict`, `terms`, `term_stats`, `field_stats`,
//! `docs` and a `meta` table this implementation adds for the text options
//! and the field order. Every table is written with `INSERT` through the
//! store's ordinary insert path, so it saves, loads and replays with the
//! store; nothing here is engine state.

use std::collections::HashMap;

use crate::Stemmer;
use crate::text::Tokenizer;

/// Every index database starts with this prefix.
pub const INDEX_DB_PREFIX: &str = "fts_main_";
/// `meta`: the text options and the field names, one row per key.
pub const META: &str = "meta";
/// `dict(term_id, term)`.
pub const DICT: &str = "dict";
/// `terms(term_id, doc_id, field_id, tf)` — one row per (term, doc, field).
pub const TERMS: &str = "terms";
/// `term_stats(term_id, df, idf)`.
pub const TERM_STATS: &str = "term_stats";
/// `field_stats(field_id, name, avg_len)`.
pub const FIELD_STATS: &str = "field_stats";
/// `docs(doc_id, field_id, len)` — the document lengths BM25 normalizes by.
pub const DOCS: &str = "docs";
/// The table names [`load`] and [`crate::versions`] consult.
pub const INDEX_TABLES: [&str; 6] = [META, DOCS, DICT, TERMS, TERM_STATS, FIELD_STATS];
/// The `meta.format` value this build writes and reads.
pub const FORMAT: &str = "fts-108a";

/// One stored posting.
#[derive(Clone, Copy, Debug)]
pub struct Posting {
    pub doc: i64,
    pub field: u32,
    pub tf: u64,
}

/// A document read from the base table.
pub struct DocInput {
    pub id: i64,
    /// One entry per indexed field; `None` is SQL NULL (an empty field).
    pub texts: Vec<Option<String>>,
}

/// The loaded index: everything a query needs, in memory.
#[derive(Debug)]
pub struct IndexData {
    pub fields: Vec<String>,
    pub num_docs: u64,
    pub avg_len: Vec<f64>,
    pub terms: Vec<String>,
    pub term_ids: HashMap<String, u32>,
    pub idf: Vec<f64>,
    pub postings: Vec<Vec<Posting>>,
    pub doc_lens: HashMap<i64, Vec<u64>>,
}

impl IndexData {
    /// Tokenize, stem and count `docs` into a fresh index. Deterministic: the
    /// same input, tokenizer and stemmer produce byte-identical tables (term
    /// ids in first-seen order, postings in input order).
    pub fn build(
        fields: &[String],
        docs: &[DocInput],
        tokenizer: &Tokenizer,
        stemmer: Stemmer,
    ) -> Result<Self, String> {
        let width = fields.len();
        let mut doc_lens = HashMap::with_capacity(docs.len());
        let mut terms: Vec<String> = Vec::new();
        let mut term_ids: HashMap<String, u32> = HashMap::new();
        let mut postings: Vec<Vec<Posting>> = Vec::new();
        let mut df: Vec<u64> = Vec::new();
        let mut lengths = vec![0u64; width];
        for doc in docs {
            if doc_lens.contains_key(&doc.id) {
                return Err(format!(
                    "duplicate document id {}: the id column must be unique",
                    doc.id
                ));
            }
            lengths.iter_mut().for_each(|l| *l = 0);
            for (field, text) in doc.texts.iter().enumerate() {
                let tokens: Vec<String> = match text {
                    Some(text) => tokenizer.indexed_tokens(text, stemmer),
                    None => Vec::new(),
                };
                lengths[field] = tokens.len() as u64;
                // Term frequency within this (doc, field) pair.
                let mut tf: HashMap<u32, u64> = HashMap::new();
                for token in tokens {
                    let id = match term_ids.get(&token) {
                        Some(id) => *id,
                        None => {
                            let id = terms.len() as u32;
                            terms.push(token.clone());
                            term_ids.insert(token, id);
                            postings.push(Vec::new());
                            df.push(0);
                            id
                        }
                    };
                    *tf.entry(id).or_default() += 1;
                }
                let mut pairs: Vec<(u32, u64)> = tf.into_iter().collect();
                pairs.sort_unstable();
                for (term, count) in pairs {
                    // `df` counts documents, not occurrences: a term that
                    // repeats across fields of one document bumps it once.
                    if postings[term as usize]
                        .last()
                        .is_none_or(|p| p.doc != doc.id)
                    {
                        df[term as usize] += 1;
                    }
                    postings[term as usize].push(Posting {
                        doc: doc.id,
                        field: field as u32,
                        tf: count,
                    });
                }
            }
            doc_lens.insert(doc.id, lengths.clone());
        }
        let n = docs.len() as f64;
        let avg_len = (0..width)
            .map(|f| {
                if n == 0.0 {
                    0.0
                } else {
                    doc_lens.values().map(|l| l[f] as f64).sum::<f64>() / n
                }
            })
            .collect();
        let idf = df
            .iter()
            .map(|&df| {
                let df = df as f64;
                (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
            })
            .collect();
        Ok(Self {
            fields: fields.to_vec(),
            num_docs: docs.len() as u64,
            avg_len,
            terms,
            term_ids,
            idf,
            postings,
            doc_lens,
        })
    }

    /// BM25: `idf(t) · Σ_f tf·(k+1) / (tf + k·(1 - b + b·len(f)/avg(f)))`.
    ///
    /// `query` is already tokenized and stemmed. A repeated query term is
    /// scored once per occurrence; with `conjunctive` a document must contain
    /// every *distinct* term in at least one of the searched fields.
    pub fn score(
        &self,
        query: &[String],
        field_mask: Option<&[bool]>,
        k: f64,
        b: f64,
        conjunctive: bool,
    ) -> HashMap<i64, f64> {
        let mut unique: Vec<u32> = Vec::with_capacity(query.len());
        for term in query {
            if let Some(id) = self.term_ids.get(term) {
                unique.push(*id);
            }
        }
        unique.sort_unstable();
        unique.dedup();
        let mut scores: HashMap<i64, f64> = HashMap::new();
        let mut hits: HashMap<i64, u32> = HashMap::new();
        for id in &unique {
            let id = *id as usize;
            let idf = self.idf[id];
            // `hits` counts *terms* a document matched (for `conjunctive`),
            // so a term found in two of its fields counts once.
            let mut last_doc = None;
            for posting in &self.postings[id] {
                if let Some(mask) = field_mask
                    && !mask[posting.field as usize]
                {
                    continue;
                }
                let len = self.doc_lens[&posting.doc][posting.field as usize] as f64;
                let avg = self.avg_len[posting.field as usize];
                // An all-empty field has no length to normalize by; every
                // document in it is "average".
                let norm = if avg > 0.0 {
                    1.0 - b + b * len / avg
                } else {
                    1.0
                };
                let tf = posting.tf as f64;
                *scores.entry(posting.doc).or_default() += idf * tf * (k + 1.0) / (tf + k * norm);
                if last_doc != Some(posting.doc) {
                    *hits.entry(posting.doc).or_default() += 1;
                    last_doc = Some(posting.doc);
                }
            }
        }
        if conjunctive {
            let needed = unique.len() as u32;
            scores.retain(|doc, _| hits.get(doc).copied() == Some(needed));
        }
        scores
    }
}
