//! A deterministic HNSW graph (Malkov & Yashunin) over f32 vectors.
//!
//! No dependency: the level generator is a seeded SplitMix64, and every
//! heap/candidate tie breaks on node id, so for a given insert order a build
//! (or a WAL replay, or a reopen) produces the same graph. Distances call
//! [`pinkdb::distance::metric_f32`], whose chunked loops LLVM vectorizes.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};

use pinkdb::distance::{Metric, metric_f32};
use pinkdb::vector_index::{DeleteShift, VectorIndex, VectorIndexStats};
use pinkdb::{PinkDbError, Result};

/// The serialized graph starts with this magic and version.
const MAGIC: &[u8; 8] = b"PHNSW01\0";
/// A hard ceiling on levels: 2^32 vectors cannot need more under m >= 2, and
/// a corrupt file must not allocate an enormous link table.
const MAX_LEVEL: usize = 32;

/// Parallel build (wave 107d): the first `SEED` vectors of a graph go in
/// sequentially, exactly as before, so small graphs are unchanged. The rest
/// go in batches of `BATCH`: levels drawn in insert order, a parallel
/// read-only search against the graph frozen at the batch start (plus an
/// exact pass over the batch itself), then links applied sequentially in
/// insert order. Both constants are fixed — never derived from the thread
/// count — so the graph depends only on the data and the insert order.
const SEED: usize = 1024;
const BATCH: usize = 256;

/// HNSW parameters, named as DuckDB's vss extension names them.
#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    /// SplitMix64 seed. Fixed so builds are reproducible; serialized so
    /// inserts after a reopen continue the same level stream.
    pub seed: u64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 128,
            ef_search: 64,
            seed: 0x5eed_107b,
        }
    }
}

impl Params {
    fn level_cap(&self, level: usize) -> usize {
        if level == 0 { self.m0 } else { self.m }
    }
}

#[derive(Clone, Debug)]
struct Node {
    row: u64,
    level: u8,
    deleted: bool,
    vector: Vec<f32>,
    /// `links[l]` is the neighbour list on level `l` (0-based).
    links: Vec<Vec<u32>>,
}

/// One candidate in a distance heap; ties break on node id so the walk is
/// stable.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Cand {
    dist: f32,
    node: u32,
}

impl Eq for Cand {}

impl Ord for Cand {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist
            .total_cmp(&other.dist)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for Cand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct Hnsw {
    dim: usize,
    metric: Metric,
    params: Params,
    rng: u64,
    nodes: Vec<Node>,
    row_to_node: HashMap<u64, u32>,
    entry: Option<u32>,
    max_level: usize,
    live: usize,
}

impl Hnsw {
    pub fn new(dim: usize, metric: Metric, params: Params) -> Self {
        Self {
            dim,
            metric,
            params,
            rng: params.seed,
            nodes: Vec::new(),
            row_to_node: HashMap::new(),
            entry: None,
            max_level: 0,
            live: 0,
        }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64.
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn random_level(&mut self) -> usize {
        // P(level >= l) = exp(-l / mL), mL = 1 / ln(m), as in the paper.
        let u = ((self.next_u64() >> 11) as f64 + 1.0) / ((1u64 << 53) as f64 + 1.0);
        let ml = 1.0 / (self.params.m as f64).ln();
        ((-u.ln()) * ml).floor().min(MAX_LEVEL as f64) as usize
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        metric_f32(self.metric, a, b)
    }

    fn node_frame(&self, node: u32) -> &Node {
        &self.nodes[node as usize]
    }

    fn search_layer(
        &self,
        query: &[f32],
        entries: &[u32],
        ef: usize,
        level: usize,
    ) -> Vec<(f32, u32)> {
        let ef = ef.max(1);
        let mut visited: HashSet<u32> =
            HashSet::with_capacity((ef.saturating_mul(self.params.m)).min(1 << 16));
        let mut candidates: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        for &entry in entries {
            if self.nodes.get(entry as usize).is_none_or(|n| n.deleted) {
                continue;
            }
            if !visited.insert(entry) {
                continue;
            }
            let d = self.dist(query, &self.node_frame(entry).vector);
            candidates.push(Reverse(Cand {
                dist: d,
                node: entry,
            }));
            results.push(Cand {
                dist: d,
                node: entry,
            });
            if results.len() > ef {
                results.pop();
            }
        }
        while let Some(Reverse(current)) = candidates.pop() {
            if results.len() >= ef
                && results
                    .peek()
                    .is_some_and(|worst| current.dist > worst.dist)
            {
                break;
            }
            let frame = self.node_frame(current.node);
            let Some(links) = frame.links.get(level) else {
                continue;
            };
            for &next in links {
                if !visited.insert(next) {
                    continue;
                }
                let Some(candidate) = self.nodes.get(next as usize) else {
                    continue;
                };
                if candidate.deleted {
                    continue;
                }
                let d = self.dist(query, &candidate.vector);
                if results.len() < ef || results.peek().is_some_and(|worst| d < worst.dist) {
                    candidates.push(Reverse(Cand {
                        dist: d,
                        node: next,
                    }));
                    results.push(Cand {
                        dist: d,
                        node: next,
                    });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut out: Vec<(f32, u32)> = results.into_iter().map(|c| (c.dist, c.node)).collect();
        out.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        out
    }

    fn greedy(&self, query: &[f32], mut entry: u32, level: usize) -> u32 {
        let mut best = self.dist(query, &self.node_frame(entry).vector);
        loop {
            let mut improved = false;
            if let Some(links) = self.node_frame(entry).links.get(level) {
                for &next in links {
                    let Some(node) = self.nodes.get(next as usize) else {
                        continue;
                    };
                    if node.deleted {
                        continue;
                    }
                    let d = self.dist(query, &node.vector);
                    if d < best {
                        best = d;
                        entry = next;
                        improved = true;
                    }
                }
            }
            if !improved {
                return entry;
            }
        }
    }

    /// Paper algorithm 4: keep a candidate only when it is closer to the base
    /// than to every already-kept neighbour. Diversifies links, which is what
    /// keeps recall high on clustered data.
    fn select(&self, mut candidates: Vec<(f32, u32)>, m: usize) -> Vec<u32> {
        candidates.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let mut out: Vec<u32> = Vec::with_capacity(m);
        for (d, node) in candidates {
            if out.len() >= m {
                break;
            }
            let diverse = out.iter().all(|&kept| {
                self.dist(&self.node_frame(node).vector, &self.node_frame(kept).vector) > d
            });
            if diverse {
                out.push(node);
            }
        }
        out
    }

    fn link(&mut self, node: u32, level: usize, neighbours: Vec<u32>) {
        let keep = self.pruned(node, level, neighbours);
        self.nodes[node as usize].links[level] = keep;
    }

    /// The neighbour list `node` should keep on `level` out of
    /// `neighbours`: pure (reads vectors only), so the batch commit can run
    /// it in parallel and assign the results afterwards.
    fn pruned(&self, node: u32, level: usize, neighbours: Vec<u32>) -> Vec<u32> {
        let cap = self.params.level_cap(level);
        let base = self.nodes[node as usize].vector.clone();
        let cands: Vec<(f32, u32)> = neighbours
            .into_iter()
            .map(|n| (self.dist(&base, &self.nodes[n as usize].vector), n))
            .collect();
        self.select(cands, cap)
    }

    fn insert_vector(&mut self, row: u64, vector: &[f32]) {
        if let Some(old) = self.row_to_node.remove(&row) {
            let node = &mut self.nodes[old as usize];
            if !node.deleted {
                node.deleted = true;
                self.live -= 1;
            }
        }
        let level = self.random_level();
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            row,
            level: level as u8,
            deleted: false,
            vector: vector.to_vec(),
            links: vec![Vec::new(); level + 1],
        });
        self.live += 1;
        self.row_to_node.insert(row, id);
        let Some(entry) = self.entry else {
            self.entry = Some(id);
            self.max_level = level;
            return;
        };
        let query = vector.to_vec();
        let mut nearest = entry;
        for lc in ((level + 1)..=self.max_level).rev() {
            nearest = self.greedy(&query, nearest, lc);
        }
        let mut entries = vec![nearest];
        for lc in (0..=level.min(self.max_level)).rev() {
            let found = self.search_layer(&query, &entries, self.params.ef_construction, lc);
            let selected = self.select(found.clone(), self.params.level_cap(lc));
            self.connect(id, lc, selected);
            entries = found.into_iter().map(|(_, n)| n).collect();
            if entries.is_empty() {
                entries.push(nearest);
            }
        }
        if level > self.max_level {
            self.entry = Some(id);
            self.max_level = level;
        }
    }

    /// Set `id`'s links on `level` to `selected`, then add the reverse link
    /// to each neighbour, pruning an over-full neighbour list with the same
    /// heuristic. Shared by the sequential insert and the batch commit.
    fn connect(&mut self, id: u32, level: usize, selected: Vec<u32>) {
        self.nodes[id as usize].links[level] = selected.clone();
        for &neighbour in &selected {
            let links = &mut self.nodes[neighbour as usize].links[level];
            if links.len() < self.params.level_cap(level) {
                links.push(id);
            } else {
                let mut cands = links.clone();
                cands.push(id);
                self.link(neighbour, level, cands);
            }
        }
    }

    /// Batched build: seed sequentially, then parallel-search /
    /// sequential-commit batches (see `SEED`). Deterministic for a given
    /// row order at any thread count, including the sequential fallback.
    fn insert_bulk(&mut self, rows: &[(u64, &[f32])]) {
        let mut pos = 0;
        while pos < rows.len() && self.nodes.len() < SEED {
            self.insert_vector(rows[pos].0, rows[pos].1);
            pos += 1;
        }
        while pos < rows.len() {
            let end = (pos + BATCH).min(rows.len());
            self.insert_one_batch(&rows[pos..end]);
            pos = end;
        }
    }

    fn insert_one_batch(&mut self, batch: &[(u64, &[f32])]) {
        // Levels first, in insert order, from the one RNG stream: batching
        // must not change the level (or the serialized RNG state) of any row.
        let levels: Vec<usize> = batch.iter().map(|_| self.random_level()).collect();
        // Replacements tombstone the frozen node before the search, as a
        // sequential insert would; a row repeated in the batch supersedes
        // its earlier copy, which is pushed below as a linkless tombstone.
        let mut superseded = vec![false; batch.len()];
        let mut seen: HashMap<u64, usize> = HashMap::new();
        for (i, &(row, _)) in batch.iter().enumerate() {
            if let Some(&prev) = seen.get(&row) {
                superseded[prev] = true;
            }
            seen.insert(row, i);
            if let Some(old) = self.row_to_node.remove(&row) {
                let node = &mut self.nodes[old as usize];
                if !node.deleted {
                    node.deleted = true;
                    self.live -= 1;
                }
            }
        }
        let base = self.nodes.len();
        let frozen_entry = self.entry;
        let frozen_max = self.max_level;
        // Push every node first (selection reads peer vectors). The batch
        // nodes have no links yet and no frozen link points at them, so the
        // searches below still see exactly the frozen graph.
        for (i, &(row, vector)) in batch.iter().enumerate() {
            self.nodes.push(Node {
                row,
                level: levels[i] as u8,
                deleted: superseded[i],
                vector: vector.to_vec(),
                links: vec![Vec::new(); levels[i] + 1],
            });
            if !superseded[i] {
                self.live += 1;
                self.row_to_node.insert(row, (base + i) as u32);
            }
        }
        // Parallel phase: read-only against the frozen graph, ending in
        // each node's selected neighbours per level (selection is pure:
        // it reads vectors only). Every task writes only its own slot and
        // `collect` keeps insert order, so the selections cannot depend
        // on scheduling or thread count.
        let search = |i: usize| -> Vec<Vec<u32>> {
            let query = batch[i].1;
            let level = levels[i];
            let mut frozen: Vec<Vec<(f32, u32)>> = vec![Vec::new(); level + 1];
            let mut peers: Vec<Vec<(f32, u32)>> = vec![Vec::new(); level + 1];
            if let Some(entry) = frozen_entry {
                let mut nearest = entry;
                for lc in ((level + 1)..=frozen_max).rev() {
                    nearest = self.greedy(query, nearest, lc);
                }
                let mut entries = vec![nearest];
                for lc in (0..=level.min(frozen_max)).rev() {
                    let found = self.search_layer(query, &entries, self.params.ef_construction, lc);
                    entries = found.iter().map(|&(_, n)| n).collect();
                    if entries.is_empty() {
                        entries.push(nearest);
                    }
                    frozen[lc] = found;
                }
            }
            // Batch peers are not in the frozen graph yet: an exact pass so
            // near-duplicates in one batch still find each other. A peer is
            // only a candidate on levels it actually has.
            for (j, &(_, peer)) in batch.iter().enumerate() {
                if j == i || superseded[j] {
                    continue;
                }
                let d = self.dist(query, peer);
                for cand in peers.iter_mut().take(level.min(levels[j]) + 1) {
                    cand.push((d, (base + j) as u32));
                }
            }
            // Select from peers and from the frozen graph separately, then
            // merge (peers first) and cap. One joint selection would let
            // near peers diversity-reject every frozen candidate — exact
            // duplicates do exactly that — leaving the batch as an island
            // with no edges to the old graph.
            (0..=level)
                .map(|lc| {
                    let cap = self.params.level_cap(lc);
                    let mut selected = self.select(peers[lc].clone(), cap);
                    for node in self.select(frozen[lc].clone(), cap) {
                        if !selected.contains(&node) {
                            selected.push(node);
                        }
                    }
                    selected.truncate(cap);
                    selected
                })
                .collect()
        };
        let empty = |i: usize| vec![Vec::new(); levels[i] + 1];
        #[cfg(not(target_arch = "wasm32"))]
        let selections: Vec<Vec<Vec<u32>>> = {
            use rayon::prelude::*;
            (0..batch.len())
                .into_par_iter()
                .map(|i| if superseded[i] { empty(i) } else { search(i) })
                .collect()
        };
        #[cfg(target_arch = "wasm32")]
        let selections: Vec<Vec<Vec<u32>>> = (0..batch.len())
            .map(|i| if superseded[i] { empty(i) } else { search(i) })
            .collect();
        // Commit, in insert order: set each node's own links, remember the
        // reverse additions per (neighbour, level) in insert order, and
        // update the entry point. No graph search happens here.
        let mut reverse: HashMap<(u32, usize), Vec<u32>> = HashMap::new();
        for (i, _) in batch.iter().enumerate() {
            if superseded[i] {
                continue;
            }
            let id = (base + i) as u32;
            for (lc, selected) in selections[i].iter().enumerate() {
                for &neighbour in selected {
                    reverse.entry((neighbour, lc)).or_default().push(id);
                }
                self.nodes[id as usize].links[lc] = selected.clone();
            }
            if self.entry.is_none() || levels[i] > self.max_level {
                self.entry = Some(id);
                self.max_level = levels[i];
            }
        }
        // Apply the reverse additions. Each (neighbour, level) list is
        // independent and its additions are already in insert order, so
        // this phase is parallel too; the sequential replay of one list is
        // exactly the push-or-prune a sequential commit would have done.
        let additions: Vec<((u32, usize), Vec<u32>)> = reverse.into_iter().collect();
        let apply = |((node, lc), adds): &((u32, usize), Vec<u32>)| {
            let cap = self.params.level_cap(*lc);
            let mut list = self.nodes[*node as usize].links[*lc].clone();
            for &id in adds {
                if list.len() < cap {
                    list.push(id);
                } else {
                    let mut cands = list.clone();
                    cands.push(id);
                    list = self.pruned(*node, *lc, cands);
                }
            }
            ((*node, *lc), list)
        };
        #[cfg(not(target_arch = "wasm32"))]
        let applied: Vec<((u32, usize), Vec<u32>)> = {
            use rayon::prelude::*;
            additions.par_iter().map(apply).collect()
        };
        #[cfg(target_arch = "wasm32")]
        let applied: Vec<((u32, usize), Vec<u32>)> = additions.iter().map(apply).collect();
        for ((node, lc), list) in applied {
            self.nodes[node as usize].links[lc] = list;
        }
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn params(&self) -> Params {
        self.params
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(64 + self.nodes.len() * (self.dim * 4 + 16 + self.params.m0 * 4));
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(self.dim as u32).to_le_bytes());
        out.extend_from_slice(&[match self.metric {
            Metric::L2Sq => 0,
            Metric::Cosine => 1,
            Metric::InnerProduct => 2,
        }]);
        out.extend_from_slice(&(self.params.m as u32).to_le_bytes());
        out.extend_from_slice(&(self.params.m0 as u32).to_le_bytes());
        out.extend_from_slice(&(self.params.ef_construction as u32).to_le_bytes());
        out.extend_from_slice(&(self.params.ef_search as u32).to_le_bytes());
        out.extend_from_slice(&self.params.seed.to_le_bytes());
        out.extend_from_slice(&self.rng.to_le_bytes());
        out.extend_from_slice(&self.entry.unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(&(self.max_level as u32).to_le_bytes());
        out.extend_from_slice(&(self.nodes.len() as u64).to_le_bytes());
        for node in &self.nodes {
            out.extend_from_slice(&node.row.to_le_bytes());
            out.push(node.level);
            out.push(u8::from(node.deleted));
            for value in &node.vector {
                out.extend_from_slice(&value.to_le_bytes());
            }
            for links in &node.links {
                out.extend_from_slice(&(links.len() as u32).to_le_bytes());
                for &link in links {
                    out.extend_from_slice(&link.to_le_bytes());
                }
            }
        }
        out
    }

    pub fn from_serialized(bytes: &[u8]) -> Result<Self> {
        let mut r = Cursor {
            data: bytes,
            pos: 0,
        };
        if r.take(8)? != MAGIC {
            return Err(corrupt("bad vector index magic"));
        }
        let dim = r.u32()? as usize;
        if dim == 0 || dim > 65_536 {
            return Err(corrupt("bad vector index dimension"));
        }
        let metric = match r.byte()? {
            0 => Metric::L2Sq,
            1 => Metric::Cosine,
            2 => Metric::InnerProduct,
            _ => return Err(corrupt("bad vector index metric")),
        };
        let m = r.u32()? as usize;
        let m0 = r.u32()? as usize;
        if m == 0 || m > 4096 || m0 < m || m0 > 8192 {
            return Err(corrupt("bad vector index M/M0"));
        }
        let params = Params {
            m,
            m0,
            ef_construction: r.u32()? as usize,
            ef_search: r.u32()? as usize,
            seed: r.u64()?,
        };
        if params.ef_construction == 0
            || params.ef_search == 0
            || params.ef_construction > 65_536
            || params.ef_search > 65_536
        {
            return Err(corrupt("bad vector index ef parameters"));
        }
        let rng = r.u64()?;
        let entry_raw = r.u32()?;
        let max_level = r.u32()? as usize;
        if max_level > MAX_LEVEL {
            return Err(corrupt("bad vector index max level"));
        }
        let count = r.u64()?;
        // Every node carries at least its vector plus a link count; this
        // bounds the allocation by the bytes actually present.
        if count > (bytes.len() as u64) / (dim as u64 * 4 + 14) {
            return Err(corrupt("vector index node count exceeds file size"));
        }
        let mut nodes = Vec::with_capacity(count as usize);
        let mut row_to_node = HashMap::with_capacity(count as usize);
        let mut live = 0usize;
        for id in 0..count {
            let row = r.u64()?;
            let level = r.byte()? as usize;
            if level > MAX_LEVEL {
                return Err(corrupt("bad vector index node level"));
            }
            let deleted = r.byte()? != 0;
            let mut vector = Vec::with_capacity(dim);
            for _ in 0..dim {
                vector.push(f32::from_le_bytes(r.array4()?));
            }
            let mut links = Vec::with_capacity(level + 1);
            for lc in 0..=level {
                let n = r.u32()? as usize;
                if n > m0.max(m) {
                    return Err(corrupt("vector index link count exceeds M0"));
                }
                let mut list = Vec::with_capacity(n);
                for _ in 0..n {
                    let target = r.u32()?;
                    if target as u64 >= count {
                        return Err(corrupt("vector index link out of range"));
                    }
                    let _ = lc;
                    list.push(target);
                }
                links.push(list);
            }
            if !deleted {
                live += 1;
                if row_to_node.insert(row, id as u32).is_some() {
                    return Err(corrupt("duplicate row id in vector index"));
                }
            }
            nodes.push(Node {
                row,
                level: level as u8,
                deleted,
                vector,
                links,
            });
        }
        let entry = match entry_raw {
            u32::MAX => None,
            id if (id as u64) < count => Some(id),
            _ => return Err(corrupt("vector index entry out of range")),
        };
        let mut index = Self {
            dim,
            metric,
            params,
            rng,
            nodes,
            row_to_node,
            entry,
            max_level,
            live,
        };
        if index.entry.is_some_and(|e| index.nodes[e as usize].deleted) {
            index.refresh_entry();
        }
        Ok(index)
    }
    /// Pick a live entry point after the current one was tombstoned; the
    /// highest-level live node keeps the descent short.
    fn refresh_entry(&mut self) {
        let best = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| !n.deleted)
            .max_by_key(|(_, n)| n.level)
            .map(|(id, n)| (id as u32, n.level as usize));
        match best {
            Some((id, level)) => {
                self.entry = Some(id);
                self.max_level = level;
            }
            None => {
                self.entry = None;
                self.max_level = 0;
            }
        }
    }
}

fn corrupt(what: &str) -> PinkDbError {
    PinkDbError::Execution(format!("corrupt vector index: {what}"))
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| corrupt("offset overflow"))?;
        if end > self.data.len() {
            return Err(corrupt("truncated"));
        }
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn array4(&mut self) -> Result<[u8; 4]> {
        let mut out = [0u8; 4];
        out.copy_from_slice(self.take(4)?);
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array4()?))
    }

    fn u64(&mut self) -> Result<u64> {
        let mut out = [0u8; 8];
        out.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(out))
    }
}

impl VectorIndex for Hnsw {
    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn insert(&mut self, row: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dim {
            return Err(PinkDbError::Execution(format!(
                "vector index expects {} dimensions, got {}",
                self.dim,
                vector.len()
            )));
        }
        self.insert_vector(row, vector);
        Ok(())
    }

    fn insert_batch(&mut self, rows: &[(u64, &[f32])]) -> Result<()> {
        // Validate everything before mutating, so a bad row cannot leave a
        // half-applied batch behind.
        for (_, vector) in rows {
            if vector.len() != self.dim {
                return Err(PinkDbError::Execution(format!(
                    "vector index expects {} dimensions, got {}",
                    self.dim,
                    vector.len()
                )));
            }
        }
        self.insert_bulk(rows);
        Ok(())
    }

    fn delete(&mut self, row: u64) {
        if let Some(node) = self.row_to_node.remove(&row) {
            let n = &mut self.nodes[node as usize];
            if !n.deleted {
                n.deleted = true;
                self.live -= 1;
            }
        }
        if self.entry.is_some_and(|e| self.nodes[e as usize].deleted) {
            self.refresh_entry();
        }
    }

    fn remap_after_delete(&mut self, shift: &DeleteShift) {
        let mut deleted: Vec<u64> = shift
            .deleted
            .iter()
            .map(|&d| shift.chunk_start + d as u64)
            .collect();
        deleted.sort_unstable();
        debug_assert!(shift.chunk_dropped || shift.deleted.len() < shift.chunk_len as usize);
        // Interior chunks stay shorter than CHUNK_SIZE after a partial delete,
        // so every row after a removed row shifts, across chunk boundaries — not
        // only rows inside the affected chunk.
        let mut map = HashMap::with_capacity(self.row_to_node.len());
        for (id, node) in self.nodes.iter_mut().enumerate() {
            if node.deleted {
                continue;
            }
            let row = node.row;
            if row >= shift.chunk_start
                && row < shift.chunk_start + shift.chunk_len
                && deleted.binary_search(&row).is_ok()
            {
                node.deleted = true;
                self.live -= 1;
                continue;
            }
            let new_row = if row < shift.chunk_start {
                row
            } else {
                row - deleted.partition_point(|&d| d < row) as u64
            };
            node.row = new_row;
            map.insert(new_row, id as u32);
        }
        self.row_to_node = map;
    }

    fn search(&self, query: &[f32], k: usize, ef_search: Option<usize>) -> Result<Vec<(f32, u64)>> {
        if query.len() != self.dim {
            return Err(PinkDbError::Execution(format!(
                "vector index expects {} dimensions, got {}",
                self.dim,
                query.len()
            )));
        }
        let Some(entry) = self.entry else {
            return Ok(Vec::new());
        };
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut nearest = entry;
        for lc in (1..=self.max_level).rev() {
            nearest = self.greedy(query, nearest, lc);
        }
        let ef = ef_search
            .unwrap_or(self.params.ef_search)
            .max(k)
            .min(self.nodes.len());
        let found = self.search_layer(query, &[nearest], ef, 0);
        Ok(found
            .into_iter()
            .filter(|(_, node)| !self.nodes[*node as usize].deleted)
            .take(k)
            .map(|(dist, node)| (dist, self.nodes[node as usize].row))
            .collect())
    }

    /// Rebuild the graph from its live nodes (drop tombstones). Levels are
    /// drawn again from the same seed in node order, so replaying the same
    /// `CALL` reproduces the same graph.
    fn compact(&mut self) {
        let mut live: Vec<(u64, Vec<f32>)> = self
            .nodes
            .iter()
            .filter(|n| !n.deleted)
            .map(|n| (n.row, n.vector.clone()))
            .collect();
        live.sort_by_key(|(row, _)| *row);
        let mut fresh = Hnsw::new(self.dim, self.metric, self.params);
        for (row, vector) in live {
            fresh.insert_vector(row, &vector);
        }
        *self = fresh;
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        Ok(self.to_bytes())
    }

    fn stats(&self) -> VectorIndexStats {
        VectorIndexStats {
            nodes: self.nodes.len(),
            live: self.live,
            tombstones: self.nodes.iter().filter(|n| n.deleted).count(),
            max_level: self.max_level as u8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clustered(n: usize, dim: usize) -> Vec<Vec<f32>> {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let cluster = (i % 8) as f32;
            let mut v = Vec::with_capacity(dim);
            for d in 0..dim {
                let x = ((i * 31 + d * 17) % 97) as f32 / 97.0;
                v.push(cluster * 2.0 + x);
            }
            out.push(v);
        }
        out
    }

    #[test]
    fn search_finds_the_nearest_of_a_small_set() {
        let mut index = Hnsw::new(4, Metric::L2Sq, Params::default());
        let data = clustered(200, 4);
        for (row, v) in data.iter().enumerate() {
            index.insert(row as u64, v).unwrap();
        }
        let hits = index.search(&data[42], 5, None).unwrap();
        assert_eq!(hits[0].1, 42);
        assert!(hits.len() == 5);
    }

    #[test]
    fn build_is_deterministic_and_survives_serialization() {
        let data = clustered(300, 6);
        let build = || {
            let mut index = Hnsw::new(6, Metric::Cosine, Params::default());
            for (row, v) in data.iter().enumerate() {
                index.insert(row as u64, v).unwrap();
            }
            index
        };
        let a = build();
        let b = build();
        assert_eq!(a.serialize().unwrap(), b.serialize().unwrap());
        let bytes = a.serialize().unwrap();
        let c = Hnsw::from_serialized(&bytes).unwrap();
        assert_eq!(bytes, c.serialize().unwrap());
        let q = &data[7];
        assert_eq!(
            a.search(q, 10, None).unwrap(),
            c.search(q, 10, None).unwrap()
        );
    }

    fn bulk_build(data: &[Vec<f32>]) -> Hnsw {
        let mut index = Hnsw::new(32, Metric::L2Sq, Params::default());
        let rows: Vec<(u64, &[f32])> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.as_slice()))
            .collect();
        index.insert_batch(&rows).unwrap();
        index
    }

    #[test]
    fn batch_below_seed_matches_sequential_build() {
        let data = clustered(500, 6);
        let mut sequential = Hnsw::new(6, Metric::Cosine, Params::default());
        for (row, v) in data.iter().enumerate() {
            sequential.insert(row as u64, v).unwrap();
        }
        let mut bulk = Hnsw::new(6, Metric::Cosine, Params::default());
        let rows: Vec<(u64, &[f32])> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.as_slice()))
            .collect();
        bulk.insert_batch(&rows).unwrap();
        assert_eq!(sequential.serialize().unwrap(), bulk.serialize().unwrap());
    }

    #[test]
    fn batch_build_is_thread_count_independent() {
        // 20k x 32 clustered set, built under pools of 1, 2, 4 and 8.
        let data: Vec<Vec<f32>> = (0..20_000)
            .map(|i| {
                let cluster = (i % 16) as f32;
                (0..32)
                    .map(|d| cluster * 4.0 + (((i * 31 + d * 17) % 89) as f32 / 89.0))
                    .collect()
            })
            .collect();
        let mut reference: Option<Vec<u8>> = None;
        for threads in [1usize, 2, 4, 8] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let bytes = pool.install(|| bulk_build(&data).serialize().unwrap());
            match &reference {
                None => reference = Some(bytes),
                Some(expected) => assert_eq!(*expected, bytes, "{threads} threads"),
            }
        }
    }

    #[test]
    fn batch_boundary_cases_round_trip() {
        // n < SEED, n = SEED, n = SEED + 1, several batches.
        for n in [17usize, SEED, SEED + 1, SEED + 3 * BATCH + 5] {
            let data = clustered(n, 4);
            let mut index = Hnsw::new(4, Metric::L2Sq, Params::default());
            // Rebuild at this dim: `bulk_build` is fixed at 32.
            let rows: Vec<(u64, &[f32])> = data
                .iter()
                .enumerate()
                .map(|(i, v)| (i as u64, v.as_slice()))
                .collect();
            index.insert_batch(&rows).unwrap();
            assert_eq!(index.stats().live, n, "n={n}");
            let bytes = index.serialize().unwrap();
            let back = Hnsw::from_serialized(&bytes).unwrap();
            assert_eq!(bytes, back.serialize().unwrap(), "n={n}");
            // `clustered` repeats vectors with period 776, so the top hit
            // need not be the last row itself — only an identical vector.
            let (dist, row) = back.search(&data[n - 1], 1, None).unwrap()[0];
            assert_eq!(dist, 0.0, "n={n}");
            assert_eq!(data[row as usize], data[n - 1], "n={n}");
        }
    }

    #[test]
    fn batch_of_exact_duplicates_all_stay_live_and_searchable() {
        let mut data = clustered(SEED, 4);
        let dup = vec![42.0, 42.0, 42.0, 42.0];
        data.extend((0..BATCH).map(|_| dup.clone()));
        let mut index = Hnsw::new(4, Metric::L2Sq, Params::default());
        let rows: Vec<(u64, &[f32])> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.as_slice()))
            .collect();
        index.insert_batch(&rows).unwrap();
        assert_eq!(index.stats().live, SEED + BATCH);
        let hits = index.search(&dup, 10, None).unwrap();
        assert_eq!(hits.len(), 10);
        assert!(hits.iter().all(|(_, row)| *row >= SEED as u64), "{hits:?}");
    }

    #[test]
    fn a_batch_can_raise_max_level() {
        // White-box: one batch on an empty graph (the seed phase bypassed),
        // so the entry point and max level come from the batch commit.
        // M = 2 draws high levels often; the fixed seed makes this stable.
        let data = clustered(BATCH, 4);
        let params = Params {
            m: 2,
            m0: 4,
            ..Params::default()
        };
        let mut index = Hnsw::new(4, Metric::L2Sq, params);
        let rows: Vec<(u64, &[f32])> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.as_slice()))
            .collect();
        index.insert_one_batch(&rows);
        let entry = index.entry.expect("batch sets an entry point");
        assert!(index.max_level >= 2, "max_level={}", index.max_level);
        assert_eq!(
            index.nodes[entry as usize].level as usize, index.max_level,
            "the entry point sits at the max level"
        );
        let bytes = index.serialize().unwrap();
        assert_eq!(
            bytes,
            Hnsw::from_serialized(&bytes).unwrap().serialize().unwrap()
        );
    }

    #[test]
    fn deletes_are_not_returned_and_compaction_removes_them() {
        let data = clustered(100, 5);
        let mut index = Hnsw::new(5, Metric::L2Sq, Params::default());
        for (row, v) in data.iter().enumerate() {
            index.insert(row as u64, v).unwrap();
        }
        index.delete(3);
        assert_eq!(index.stats().tombstones, 1);
        assert!(
            index
                .search(&data[3], 10, None)
                .unwrap()
                .iter()
                .all(|(_, r)| *r != 3)
        );
        index.compact();
        assert_eq!(index.stats().tombstones, 0);
        assert!(
            index
                .search(&data[3], 10, None)
                .unwrap()
                .iter()
                .all(|(_, r)| *r != 3)
        );
    }

    #[test]
    fn remap_shifts_rows_in_later_chunks() {
        let data = clustered(8, 3);
        let mut index = Hnsw::new(3, Metric::L2Sq, Params::default());
        for (row, v) in data.iter().enumerate() {
            index.insert(row as u64, v).unwrap();
        }
        index.remap_after_delete(&DeleteShift {
            chunk_start: 2,
            chunk_len: 3,
            deleted: vec![0],
            chunk_dropped: false,
        });
        // Row 2 is tombstoned; rows 3..7 shift down by one, including rows
        // outside the affected chunk.
        assert_eq!(index.stats().tombstones, 1);
        assert_eq!(index.row_to_node.get(&0), Some(&0));
        assert_eq!(index.row_to_node.get(&1), Some(&1));
        for node in 3..8 {
            assert_eq!(index.row_to_node.get(&(node - 1)), Some(&(node as u32)));
        }
        assert_eq!(index.row_to_node.get(&7), None);
    }

    #[test]
    fn remap_follows_chunk_compaction() {
        let data = clustered(10, 3);
        let mut index = Hnsw::new(3, Metric::L2Sq, Params::default());
        for (row, v) in data.iter().enumerate() {
            index.insert(row as u64, v).unwrap();
        }
        // Delete row 2, then row 6: row 7..9 shift down by two.
        index.delete(2);
        index.delete(6);
        index.remap_after_delete(&DeleteShift {
            chunk_start: 0,
            chunk_len: 10,
            deleted: vec![2, 6],
            chunk_dropped: false,
        });
        index.compact();
        let mut rows: Vec<u64> = index
            .search(&data[9], 100, None)
            .unwrap()
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
