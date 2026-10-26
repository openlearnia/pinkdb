//! Layered, budgeted retrieval: the persona first, then the best-matching
//! scenarios (a fast bootstrap), then atoms and turns ranked by BM25 fused
//! with vector distance.
//!
//! The fusion is reciprocal-rank fusion (`1/(60+rank)`), the same rule the
//! fts extension's `rrf` scalar uses (wave 108a) — computed here rather than
//! in SQL because the BM25 corpus is the caller's *visible* rows, which only
//! this crate knows how to filter, and because a 108a index is static (it
//! would be stale after every `memory_add`).
//!
//! Vectors are optional and additive: when the embeddings extension is loaded
//! and a provider is configured (`embed()` binds), atoms are embedded into
//! `atom_vectors` on first use and ranked by `array_distance`; otherwise the
//! answer is BM25-only and says so.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::host::{Caller, Host, cell_str, query};
use crate::store::{self, Atom, DATABASE, Scenario, text};

/// BM25 constants, the values the fts extension uses.
const K1: f64 = 1.2;
const B: f64 = 0.75;
/// The reciprocal-rank-fusion constant: rank 1 scores more than rank 0.
const RRF: f64 = 60.0;
/// How many scenarios a search reports: the bootstrap, not a list.
const SCENARIOS: usize = 3;
/// How many vector hits the fused ranking considers.
const VECTOR_CANDIDATES: usize = 200;

/// One search call's budget. Every field is per call, so an agent can ask for
/// a cheap search and an operator for a large one.
#[derive(Clone, Debug)]
pub struct Request {
    pub query: String,
    pub max_items: usize,
    pub max_chars: usize,
    pub timeout: Duration,
}

impl Default for Request {
    fn default() -> Self {
        Request {
            query: String::new(),
            max_items: 5,
            max_chars: 4000,
            timeout: Duration::from_secs(5),
        }
    }
}

/// Where a hit's rank came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Bm25,
    Vector,
    Both,
}

impl Source {
    pub fn name(self) -> &'static str {
        match self {
            Source::Bm25 => "bm25",
            Source::Vector => "vector",
            Source::Both => "bm25+vector",
        }
    }
}

/// One result. `layer` is `L0` or `L1`; the bootstrap persona and scenarios
/// are reported separately, because they are context, not hits.
#[derive(Clone, Debug)]
pub struct Hit {
    pub layer: &'static str,
    pub id: String,
    pub score: f64,
    pub source: Source,
    pub text: String,
    pub visibility: String,
}

/// Which half of the ranking ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Text only.
    Bm25,
    /// BM25 fused with vector distance.
    Hybrid,
}

/// Why a budget stopped the result list, if one did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Budget {
    Items,
    Characters,
    Timeout,
}

impl Budget {
    pub fn name(self) -> &'static str {
        match self {
            Budget::Items => "items",
            Budget::Characters => "characters",
            Budget::Timeout => "timeout",
        }
    }
}

/// A search answer.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    pub mode: Option<Mode>,
    /// Why the vector half did not run, when it did not.
    pub note: String,
    pub persona: Option<String>,
    pub scenarios: Vec<Scenario>,
    pub hits: Vec<Hit>,
    pub stopped: Option<Budget>,
    /// How many rows were ranked before the budget applied.
    pub considered: usize,
}

/// Search the memory `caller` may see.
pub fn search(host: &mut dyn Host, caller: &Caller, request: &Request) -> Result<Outcome, String> {
    let started = Instant::now();
    // 1. The bootstrap: the caller's persona and the best-matching scenarios.
    //    One row each, so this keeps the latency of a small answer.
    let mut outcome = Outcome {
        persona: store::persona(host, caller)?.filter(|text| !text.is_empty()),
        ..Outcome::default()
    };
    let terms = tokens(&request.query);
    let mut scenarios = store::scenarios(host, caller)?;
    scenarios.sort_by(|left, right| {
        scenario_score(right, &terms)
            .partial_cmp(&scenario_score(left, &terms))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    outcome.scenarios = scenarios
        .into_iter()
        .filter(|scenario| scenario_score(scenario, &terms) > 0.0)
        .take(SCENARIOS)
        .collect();
    if started.elapsed() > request.timeout {
        outcome.stopped = Some(Budget::Timeout);
        outcome.mode = Some(Mode::Bm25);
        return Ok(outcome);
    }

    // 2. The ranked half: atoms and turns the caller may see.
    let atoms = store::atoms(host, caller, true)?;
    let turns = turns(host, caller)?;
    outcome.considered = atoms.len() + turns.len();
    let mut docs: Vec<Doc> = atoms
        .iter()
        .map(|atom| Doc {
            layer: "L1",
            id: atom.id.clone(),
            text: atom.text.clone(),
            visibility: atom.visibility.clone(),
        })
        .collect();
    docs.extend(turns);

    let bm25 = bm25(&docs, &terms);
    let (vector, mode, note) =
        vectors::rank(host, caller, &request.query, &atoms, started, request)?;
    let scored = fuse(&bm25, &vector, &docs);

    // 3. The budget: items, characters and elapsed time, each reported.
    let mut chars = 0_usize;
    for (score, index, source) in scored {
        if outcome.hits.len() >= request.max_items {
            outcome.stopped = Some(Budget::Items);
            break;
        }
        if started.elapsed() > request.timeout {
            outcome.stopped = Some(Budget::Timeout);
            break;
        }
        let doc = &docs[index];
        if chars + doc.text.chars().count() > request.max_chars {
            outcome.stopped = Some(Budget::Characters);
            break;
        }
        chars += doc.text.chars().count();
        outcome.hits.push(Hit {
            layer: doc.layer,
            id: doc.id.clone(),
            score,
            source,
            text: doc.text.clone(),
            visibility: doc.visibility.clone(),
        });
    }
    outcome.mode = Some(mode);
    outcome.note = note;
    Ok(outcome)
}

/// One rankable document.
#[derive(Clone, Debug)]
pub struct Doc {
    pub layer: &'static str,
    pub id: String,
    pub text: String,
    pub visibility: String,
}

/// Every L0 turn the caller may see, as rankable documents.
fn turns(host: &mut dyn Host, caller: &Caller) -> Result<Vec<Doc>, String> {
    let sql = format!(
        "SELECT id, content, visibility FROM {DATABASE}.l0_turns AS turns WHERE {}",
        store::visible("turns", caller)
    );
    Ok(query(host, &sql)?
        .iter()
        .map(|row| Doc {
            layer: "L0",
            id: cell_str(row, 0),
            text: cell_str(row, 1),
            visibility: cell_str(row, 2),
        })
        .collect())
}

/// Lowercase alphanumeric runs. Punctuation carries no memory signal, and a
/// query and a document must tokenize the same way for BM25 to mean anything.
pub fn tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if character.is_alphanumeric() {
            current.extend(character.to_lowercase());
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Reciprocal-rank fusion: a document scores `1/(60+rank)` once per list it
/// appears in, so agreeing lists beat a single confident one. Only atoms can
/// have a vector, so a turn's fused score is its BM25 rank alone — and a
/// vector hit that BM25 never ranked still enters at its vector rank.
pub fn fuse(
    bm25: &[(usize, f64)],
    vector: &HashMap<String, usize>,
    docs: &[Doc],
) -> Vec<(f64, usize, Source)> {
    let mut scored: Vec<(f64, usize, Source)> = Vec::new();
    for (rank, (index, _)) in bm25.iter().enumerate() {
        let mut source = Source::Bm25;
        let mut score = 1.0 / (RRF + rank as f64);
        if let Some(vector_rank) = vector.get(&docs[*index].id) {
            score += 1.0 / (RRF + *vector_rank as f64);
            source = Source::Both;
        }
        scored.push((score, *index, source));
    }
    for (id, rank) in vector {
        if let Some(index) = docs.iter().position(|doc| doc.id == *id)
            && !scored.iter().any(|(_, i, _)| *i == index)
        {
            scored.push((1.0 / (RRF + *rank as f64), index, Source::Vector));
        }
    }
    scored.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored
}

/// BM25 ranking of `docs` for `terms`, best first: `(index, score)`.
///
/// The formula is the fts extension's (`k1 = 1.2`, `b = 0.75`,
/// `idf = ln(1 + (N - df + 0.5)/(df + 0.5))`), so a term that appears in every
/// document contributes nothing and a rare one dominates.
pub fn bm25(docs: &[Doc], terms: &[String]) -> Vec<(usize, f64)> {
    if docs.is_empty() || terms.is_empty() {
        return Vec::new();
    }
    let corpus: Vec<Vec<String>> = docs.iter().map(|doc| tokens(&doc.text)).collect();
    let total = corpus.len() as f64;
    let average = corpus.iter().map(Vec::len).sum::<usize>() as f64 / total;
    let mut scored: Vec<(usize, f64)> = Vec::new();
    for (index, words) in corpus.iter().enumerate() {
        let length = words.len() as f64;
        let mut score = 0.0;
        for term in terms {
            let tf = words.iter().filter(|word| *word == term).count();
            if tf == 0 {
                continue;
            }
            let df = corpus
                .iter()
                .filter(|words| words.iter().any(|word| word == term))
                .count() as f64;
            let idf = (1.0 + (total - df + 0.5) / (df + 0.5)).ln();
            let tf = tf as f64;
            score += idf * tf * (K1 + 1.0) / (tf + K1 * (1.0 - B + B * length / average));
        }
        if score > 0.0 {
            scored.push((index, score));
        }
    }
    // Deterministic: equal scores keep document order, so a test can compare.
    scored.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
}

fn scenario_score(scenario: &Scenario, terms: &[String]) -> f64 {
    let haystack = tokens(&format!("{} {}", scenario.title, scenario.summary));
    let mut score = 0.0;
    for term in terms {
        if haystack.iter().any(|word| word == term) {
            score += 1.0;
        }
    }
    score
}

/// The optional vector half.
mod vectors {
    use super::*;

    /// The `FLOAT[N]` elements of one cell.
    fn dimension(cell: &Value) -> Option<usize> {
        let text = match cell {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let rest = text.strip_prefix("FLOAT[")?;
        rest.strip_suffix(']')?.parse().ok()
    }

    /// Rank the caller's atoms by distance to the query's embedding, if the
    /// store can embed at all. `Ok((ranks, mode, note))`.
    pub(super) fn rank(
        host: &mut dyn Host,
        _caller: &Caller,
        query_text: &str,
        atoms: &[Atom],
        started: Instant,
        request: &Request,
    ) -> Result<(HashMap<String, usize>, Mode, String), String> {
        if started.elapsed() > request.timeout {
            return Ok((
                HashMap::new(),
                Mode::Bm25,
                "the timeout budget was spent before vectors".to_string(),
            ));
        }
        if atoms.iter().all(|atom| atom.text.is_empty()) {
            return Ok((HashMap::new(), Mode::Bm25, "no atoms to embed".to_string()));
        }
        let probe = match query(host, "SELECT typeof(embed('probe'))") {
            Ok(rows) => rows,
            Err(error) => {
                return Ok((
                    HashMap::new(),
                    Mode::Bm25,
                    format!("bm25 only: embeddings are not available ({error})"),
                ));
            }
        };
        let Some(n) = probe.first().and_then(|row| dimension(&row[0])) else {
            return Ok((
                HashMap::new(),
                Mode::Bm25,
                "bm25 only: the embeddings extension returned no FLOAT[N]".to_string(),
            ));
        };
        if let Err(error) = ensure_table(host, n) {
            return Ok((HashMap::new(), Mode::Bm25, format!("bm25 only: {error}")));
        }
        if let Err(error) = fill(host) {
            return Ok((HashMap::new(), Mode::Bm25, format!("bm25 only: {error}")));
        }
        let sql = format!(
            "SELECT vectors.atom, array_distance(vectors.v, embed({})) AS distance FROM \
             {DATABASE}.atom_vectors AS vectors JOIN {DATABASE}.atoms AS atoms ON atoms.id = \
             vectors.atom WHERE atoms.status = 'active' AND {} ORDER BY distance LIMIT \
             {VECTOR_CANDIDATES}",
            text(query_text),
            store::visible("atoms", _caller)
        );
        let ranks = match query(host, &sql) {
            Ok(rows) => rows,
            Err(error) => {
                return Ok((HashMap::new(), Mode::Bm25, format!("bm25 only: {error}")));
            }
        };
        let ranked = ranks
            .iter()
            .enumerate()
            .map(|(rank, row)| (cell_str(row, 0), rank))
            .collect::<BTreeMap<String, usize>>();
        Ok((
            ranked.into_iter().collect(),
            Mode::Hybrid,
            format!("hybrid: bm25 and {n}-dimension vectors"),
        ))
    }

    fn ensure_table(host: &mut dyn Host, n: usize) -> Result<(), String> {
        host.internal(&format!(
            "CREATE TABLE IF NOT EXISTS {DATABASE}.atom_vectors(atom VARCHAR, v FLOAT[{n}])"
        ))
        .map(|_| ())
    }

    /// Embed every visible active atom that has no vector yet. One statement,
    /// so the embed call is evaluated by the executor, not one row per round
    /// trip.
    fn fill(host: &mut dyn Host) -> Result<(), String> {
        host.internal(&format!(
            "INSERT INTO {DATABASE}.atom_vectors(atom, v) SELECT id, embed(text) FROM \
             {DATABASE}.atoms WHERE status = 'active' AND id NOT IN (SELECT atom FROM \
             {DATABASE}.atom_vectors)"
        ))
        .map(|_| ())
    }
}
