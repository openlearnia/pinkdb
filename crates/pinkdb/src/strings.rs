//! VARCHAR column storage as 16-byte string views (wave 128a).
//!
//! A VARCHAR vector used to be `Vec<Arc<str>>`: every copy of a cell (a
//! join's gather, a filter's take, a group's key) was an atomic reference
//! count update on a shared heap object, and every compare chased a pointer
//! to it. [`StrVec`] stores each cell as a 16-byte [`StrView`] — the layout
//! of DuckDB's `string_t` (`src/include/duckdb/common/types/string_type.hpp`)
//! and Umbra's "German strings": the length, then either up to 12 bytes
//! inline or a 4-byte prefix plus the location of the bytes. Short strings
//! (TPC-H's flags, nation names, dates as text) live entirely in the view;
//! a copy is a 16-byte copy and two equal short strings have equal views.
//!
//! Long strings live in byte buffers the vector owns: its own growable
//! buffer, plus immutable shared buffers (a decoded store chunk's bytes,
//! a dictionary) that copies of the vector share by `Arc` — one reference
//! count per vector, never per cell.

use std::sync::Arc;

/// Strings up to this many bytes are stored inside their view.
const INLINE: usize = 12;

/// The heap index of a vector's own (growable) buffer.
const OWN: u32 = u32::MAX;

/// One string cell. `len <= 12`: `data[..len]` is the string. Otherwise
/// `data[0..4]` is its first four bytes, `data[4..8]` the buffer (`OWN` or
/// an index into the vector's shared buffers) and `data[8..12]` the offset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub(crate) struct StrView {
    len: u32,
    data: [u8; 12],
}

impl StrView {
    #[inline]
    fn inline(s: &[u8]) -> StrView {
        debug_assert!(s.len() <= INLINE);
        let mut data = [0u8; 12];
        data[..s.len()].copy_from_slice(s);
        StrView {
            len: s.len() as u32,
            data,
        }
    }

    #[inline]
    fn long(s: &[u8], heap: u32, offset: u32) -> StrView {
        let mut data = [0u8; 12];
        data[..4].copy_from_slice(&s[..4]);
        data[4..8].copy_from_slice(&heap.to_le_bytes());
        data[8..12].copy_from_slice(&offset.to_le_bytes());
        StrView {
            len: s.len() as u32,
            data,
        }
    }

    #[inline]
    fn is_inline(&self) -> bool {
        self.len as usize <= INLINE
    }

    #[inline]
    fn heap(&self) -> u32 {
        u32::from_le_bytes([self.data[4], self.data[5], self.data[6], self.data[7]])
    }

    #[inline]
    fn offset(&self) -> usize {
        u32::from_le_bytes([self.data[8], self.data[9], self.data[10], self.data[11]]) as usize
    }

    #[inline]
    fn with_location(self, heap: u32, offset: u32) -> StrView {
        let mut v = self;
        v.data[4..8].copy_from_slice(&heap.to_le_bytes());
        v.data[8..12].copy_from_slice(&offset.to_le_bytes());
        v
    }

    /// The view as one 128-bit key: equal keys within one vector mean equal
    /// strings (inline bytes, or the same bytes in the same buffer).
    #[inline]
    pub(crate) fn key(&self) -> u128 {
        let mut b = [0u8; 16];
        b[..4].copy_from_slice(&self.len.to_le_bytes());
        b[4..].copy_from_slice(&self.data);
        u128::from_le_bytes(b)
    }

    /// An inline view's key: with the padding zero it is the string's content
    /// and nothing else, so equal keys ⇔ equal strings anywhere.
    #[inline]
    fn inline_key(&self) -> u128 {
        debug_assert!(self.is_inline(), "inline_key on a long view");
        debug_assert!(
            self.data[self.len as usize..].iter().all(|&b| b == 0),
            "inline views zero their padding: their key must be the content"
        );
        self.key()
    }
}

/// MurmurHash3's finalizer (`fmix64`, DuckDB's `MurmurHash64`), the crate's
/// one 64-bit mix (`exec::mix64` and `stats::mix` are this function).
#[inline]
pub(crate) fn mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// Eight bytes at a time, then mixed (`exec::hash_bytes_key` is this
/// function): the crate's one hash over string content.
#[inline]
pub(crate) fn word_hash(key: &[u8]) -> u64 {
    let mut h = 0x9e37_79b9_7f4a_7c15u64 ^ key.len() as u64;
    let (words, rest) = key.as_chunks::<8>();
    for w in words {
        let x = u64::from_le_bytes(*w);
        h = (h ^ x).wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(31);
    }
    let mut tail = [0u8; 8];
    tail[..rest.len()].copy_from_slice(rest);
    h ^= u64::from_le_bytes(tail);
    mix(h)
}

/// Hash of cell `i` as a key: a short string hashes its two view words (no
/// byte access, no buffer), a long one its bytes. Equal strings hash alike
/// whichever vector, chunk or buffer they came from — the build and probe
/// sides of a join hash the same content twice.
#[inline]
pub(crate) fn view_hash(v: &StrVec, i: usize) -> u64 {
    let view = v.views[i];
    if view.is_inline() {
        let k = view.inline_key();
        mix(
            (k as u64 ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ (k >> 64) as u64,
        )
    } else {
        word_hash(v.bytes_of(&view))
    }
}

/// Whether cells `i` of `a` and `j` of `b` are the same string, views first:
/// two inline views decide it without touching a byte, a long string compares
/// its bytes (its view carries a location, not the content).
#[inline]
pub(crate) fn views_eq(a: &StrVec, i: usize, b: &StrVec, j: usize) -> bool {
    let (x, y) = (a.views[i], b.views[j]);
    match (x.is_inline(), y.is_inline()) {
        (true, true) => x.inline_key() == y.inline_key(),
        // A short string is at most 12 bytes, a long one more.
        (true, false) | (false, true) => false,
        (false, false) => a.bytes_of(&x) == b.bytes_of(&y),
    }
}

/// A vector's shared buffers: one list behind one `Arc` (copying the
/// vector costs one reference count, however many buffers a join's gather
/// left it pointing into), and no allocation at all while empty.
#[derive(Clone, Default)]
struct SharedList(Option<Arc<Vec<Arc<[u8]>>>>);

impl SharedList {
    fn of(list: Vec<Arc<[u8]>>) -> SharedList {
        SharedList(Some(Arc::new(list)))
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |l| l.len())
    }

    fn iter(&self) -> impl Iterator<Item = &Arc<[u8]>> {
        self.0.iter().flat_map(|l| l.iter())
    }

    /// Append `buffer`, returning its index.
    fn push(&mut self, buffer: &Arc<[u8]>) -> u32 {
        let list = Arc::make_mut(self.0.get_or_insert_with(Arc::default));
        list.push(Arc::clone(buffer));
        (list.len() - 1) as u32
    }
}

impl std::ops::Index<usize> for SharedList {
    type Output = Arc<[u8]>;

    #[inline]
    fn index(&self, i: usize) -> &Arc<[u8]> {
        &self
            .0
            .as_ref()
            .expect("a shared buffer index into an empty list")[i]
    }
}

/// A VARCHAR vector's cells. Reads look like a `Vec<String>` (`v[i]` is a
/// `str`); writes append.
#[derive(Clone, Default)]
pub struct StrVec {
    views: Vec<StrView>,
    own: Vec<u8>,
    /// The shared buffers, as one list behind one `Arc`: copying the vector
    /// (a filter's take) costs one reference count, however many buffers a
    /// join's gather left it pointing into.
    shared: SharedList,
}

impl StrVec {
    pub fn new() -> StrVec {
        StrVec::default()
    }

    pub fn with_capacity(cap: usize) -> StrVec {
        StrVec {
            views: Vec::with_capacity(cap),
            own: Vec::new(),
            shared: SharedList::default(),
        }
    }

    /// `n` copies of `s`, sharing one buffer.
    pub fn repeat(s: &str, n: usize) -> StrVec {
        let b = s.as_bytes();
        if b.len() <= INLINE {
            return StrVec {
                views: vec![StrView::inline(b); n],
                own: Vec::new(),
                shared: SharedList::default(),
            };
        }
        let view = StrView::long(b, 0, 0);
        StrVec {
            views: vec![view; n],
            own: Vec::new(),
            shared: SharedList::of(vec![Arc::from(b)]),
        }
    }

    /// Cells over one shared buffer: `ranges` are `(offset, len)` of UTF-8
    /// strings inside `buffer` (a decoded column chunk's bytes). The bytes
    /// are not copied; short strings are inlined.
    pub(crate) fn over_buffer(
        buffer: Arc<[u8]>,
        ranges: impl ExactSizeIterator<Item = (usize, usize)>,
    ) -> StrVec {
        let mut views = Vec::with_capacity(ranges.len());
        let mut any_long = false;
        for (offset, len) in ranges {
            let s = &buffer[offset..offset + len];
            debug_assert!(std::str::from_utf8(s).is_ok());
            if len <= INLINE {
                views.push(StrView::inline(s));
            } else {
                any_long = true;
                views.push(StrView::long(s, 0, offset as u32));
            }
        }
        StrVec {
            views,
            own: Vec::new(),
            shared: if any_long {
                SharedList::of(vec![buffer])
            } else {
                SharedList::default()
            },
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.views.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    #[inline]
    pub(crate) fn view(&self, i: usize) -> StrView {
        self.views[i]
    }

    #[inline]
    fn bytes_of<'a>(&'a self, v: &'a StrView) -> &'a [u8] {
        let len = v.len as usize;
        if len <= INLINE {
            &v.data[..len]
        } else {
            let at = v.offset();
            match v.heap() {
                OWN => &self.own[at..at + len],
                h => &self.shared[h as usize][at..at + len],
            }
        }
    }

    /// Cell `i`.
    #[inline]
    pub fn get(&self, i: usize) -> &str {
        let b = self.bytes_of(&self.views[i]);
        // SAFETY: every view was built from a `&str` (or, for
        // `over_buffer`, a range the decoder validated as UTF-8), and
        // buffers are never mutated under an existing view.
        unsafe { std::str::from_utf8_unchecked(b) }
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &str> + '_ {
        (0..self.views.len()).map(move |i| self.get(i))
    }

    /// Append `s` (its bytes are copied once if longer than 12).
    #[inline]
    pub fn push(&mut self, s: &str) {
        let b = s.as_bytes();
        if b.len() <= INLINE {
            self.views.push(StrView::inline(b));
        } else {
            let offset = self.own_offset(b.len());
            self.own.extend_from_slice(b);
            self.views.push(StrView::long(b, OWN, offset));
        }
    }

    fn own_offset(&self, extra: usize) -> u32 {
        let offset = self.own.len();
        assert!(
            offset + extra <= u32::MAX as usize,
            "a string vector holds at most 4 GiB of string bytes"
        );
        offset as u32
    }

    /// Replace cell `i` (the old bytes, if owned, stay unused).
    pub fn set(&mut self, i: usize, s: &str) {
        let b = s.as_bytes();
        self.views[i] = if b.len() <= INLINE {
            StrView::inline(b)
        } else {
            let offset = self.own_offset(b.len());
            self.own.extend_from_slice(b);
            StrView::long(b, OWN, offset)
        };
    }

    /// The index in `self.shared` of `buffer`, adding it when new.
    fn shared_index(&mut self, buffer: &Arc<[u8]>) -> u32 {
        if let Some(i) = self.shared.iter().position(|b| Arc::ptr_eq(b, buffer)) {
            return i as u32;
        }
        self.shared.push(buffer)
    }

    /// Append cell `i` of `src`: a shared long string keeps pointing into its
    /// buffer, an owned one is copied.
    #[inline]
    fn push_view_of(&mut self, src: &StrVec, map: &mut [Option<u32>], i: usize) {
        let v = src.views[i];
        if v.is_inline() {
            self.views.push(v);
            return;
        }
        match v.heap() {
            OWN => {
                let len = v.len as usize;
                let at = v.offset();
                let offset = self.own_offset(len);
                self.own.extend_from_slice(&src.own[at..at + len]);
                self.views.push(v.with_location(OWN, offset));
            }
            h => {
                let h = h as usize;
                let mine = match map[h] {
                    Some(m) => m,
                    None => {
                        let m = self.shared_index(&src.shared[h]);
                        map[h] = Some(m);
                        m
                    }
                };
                self.views.push(v.with_location(mine, v.offset() as u32));
            }
        }
    }

    /// Append cell `i` of `src`.
    pub fn push_from(&mut self, src: &StrVec, i: usize) {
        let mut map = vec![None; src.shared.len()];
        self.push_view_of(src, &mut map, i);
    }

    /// Append cells `start..start + len` of `src`.
    pub fn extend_from(&mut self, src: &StrVec, start: usize, len: usize) {
        let mut map = vec![None; src.shared.len()];
        self.views.reserve(len);
        for i in start..start + len {
            self.push_view_of(src, &mut map, i);
        }
    }

    /// The cells at `rows` (any order, repeats allowed). Only the shared
    /// buffers a taken cell points into come along (a vector gathered from a
    /// join's build side can reference a buffer per build chunk).
    pub fn take(&self, rows: impl IntoIterator<Item = usize>) -> StrVec {
        let rows = rows.into_iter();
        if self.own.is_empty() {
            // Every long string lives in a shared buffer: the cells are view
            // copies and the buffers come along by reference.
            return StrVec {
                views: rows.map(|i| self.views[i]).collect(),
                own: Vec::new(),
                shared: self.shared.clone(),
            };
        }
        let mut out = StrVec::with_capacity(rows.size_hint().0);
        let mut map = vec![None; self.shared.len()];
        for i in rows {
            self.push_view_into(&mut out, &mut map, i);
        }
        out
    }

    /// Append cell `i` of `self` to `out`, mapping shared buffers through
    /// `map` (`self`'s buffer index → `out`'s).
    #[inline]
    fn push_view_into(&self, out: &mut StrVec, map: &mut [Option<u32>], i: usize) {
        out.push_view_of(self, map, i);
    }

    /// One vector gathered from several: cell `k` is `sources[c][r]` for
    /// `picks[k] = (c, r)`, or `""` where `c` is `null` (the caller marks
    /// those rows NULL). A join gathers from every build chunk, so shared
    /// buffers are mapped by address as they are met, not per source.
    pub(crate) fn gather(sources: &[&StrVec], picks: &[(u32, u32)], null: u32) -> StrVec {
        let mut out = StrVec::with_capacity(picks.len());
        let mut index: crate::types::FastMap<usize, u32> = crate::types::FastMap::default();
        // The last (source, buffer) mapped: consecutive picks mostly repeat it.
        let mut last: Option<(u32, u32, u32)> = None;
        let mut first = vec![u32::MAX; sources.len()];
        for &(c, r) in picks {
            if c == null {
                out.views.push(StrView::default());
                continue;
            }
            let src = sources[c as usize];
            let v = src.views[r as usize];
            if v.is_inline() {
                out.views.push(v);
                continue;
            }
            match v.heap() {
                OWN => {
                    let len = v.len as usize;
                    let at = v.offset();
                    let offset = out.own_offset(len);
                    out.own.extend_from_slice(&src.own[at..at + len]);
                    out.views.push(v.with_location(OWN, offset));
                }
                h => {
                    // Most sources (a decoded store chunk) hold one buffer:
                    // its mapping is a direct slot per source.
                    let single = src.shared.len() == 1;
                    let mine = match last {
                        Some((lc, lh, m)) if lc == c && lh == h => m,
                        _ if single && first[c as usize] != u32::MAX => first[c as usize],
                        _ => {
                            let buffer = &src.shared[h as usize];
                            let key = buffer.as_ptr() as usize;
                            let m = *index.entry(key).or_insert_with(|| out.shared.push(buffer));
                            if single {
                                first[c as usize] = m;
                            }
                            last = Some((c, h, m));
                            m
                        }
                    };
                    out.views.push(v.with_location(mine, v.offset() as u32));
                }
            }
        }
        out
    }
}

impl std::ops::Index<usize> for StrVec {
    type Output = str;

    #[inline]
    fn index(&self, i: usize) -> &str {
        self.get(i)
    }
}

impl PartialEq for StrVec {
    fn eq(&self, other: &StrVec) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl std::fmt::Debug for StrVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<S: AsRef<str>> FromIterator<S> for StrVec {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> StrVec {
        let iter = iter.into_iter();
        let mut out = StrVec::with_capacity(iter.size_hint().0);
        for s in iter {
            out.push(s.as_ref());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_and_long_strings_round_trip_through_every_copy() {
        let words = [
            "",
            "R",
            "SAUDI ARABIA",
            "a thirteen ch",
            "Customer#000000001",
            "é∂ unicode ✓",
        ];
        let v: StrVec = words.iter().collect();
        assert_eq!(v.iter().collect::<Vec<_>>(), words);
        let t = v.take([5, 0, 4, 4, 2]);
        assert_eq!(
            t.iter().collect::<Vec<_>>(),
            [
                "é∂ unicode ✓",
                "",
                "Customer#000000001",
                "Customer#000000001",
                "SAUDI ARABIA"
            ]
        );
        let mut e = StrVec::new();
        e.extend_from(&v, 2, 3);
        e.push_from(&t, 0);
        assert_eq!(
            e.iter().collect::<Vec<_>>(),
            [
                "SAUDI ARABIA",
                "a thirteen ch",
                "Customer#000000001",
                "é∂ unicode ✓"
            ]
        );
        let mut s = v.clone();
        s.set(1, "now a much longer string");
        assert_eq!(&s[1], "now a much longer string");
        assert_eq!(&s[4], "Customer#000000001");
    }

    #[test]
    fn shared_buffers_are_shared_not_copied() {
        let bytes: Arc<[u8]> = Arc::from(&b"shortlong string number onelong string number two"[..]);
        let v = StrVec::over_buffer(Arc::clone(&bytes), [(0, 5), (5, 22), (27, 22)].into_iter());
        assert_eq!(
            v.iter().collect::<Vec<_>>(),
            ["short", "long string number one", "long string number two"]
        );
        let t = v.take([2, 1, 1]);
        assert!(t.own.is_empty(), "shared long strings are not copied");
        let other: StrVec = ["x", "another long string here"].iter().collect();
        let g = StrVec::gather(
            &[&t, &other],
            &[(1, 1), (0, 0), (u32::MAX, 0), (1, 0)],
            u32::MAX,
        );
        assert_eq!(
            g.iter().collect::<Vec<_>>(),
            [
                "another long string here",
                "long string number two",
                "",
                "x"
            ]
        );
        assert_eq!(g.shared.len(), 1);
        let r = StrVec::repeat("a constant long enough", 3);
        assert_eq!(r.iter().collect::<Vec<_>>(), ["a constant long enough"; 3]);
        assert_eq!(v.view(1).key(), t.view(1).key());
    }
}
